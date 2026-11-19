// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use super::{ErasedHookFn, HookRegistration, TaskObservers, TaskSpawn, WorkerInit};

pub(crate) struct ClearedHooks {
    _task_spawn: Arc<Vec<Arc<ErasedHookFn<TaskSpawn>>>>,
    _worker_init: Arc<Vec<Arc<ErasedHookFn<WorkerInit>>>>,
}

#[derive(derive_more::Debug)]
pub(crate) struct RetainedHooks {
    registration: Mutex<()>,
    closed: AtomicBool,
    has_task_spawn: AtomicBool,
    task_spawn: ArcSwap<Vec<Arc<ErasedHookFn<TaskSpawn>>>>,
    worker_init: ArcSwap<Vec<Arc<ErasedHookFn<WorkerInit>>>>,
}

impl RetainedHooks {
    pub(crate) fn new(hooks: Vec<HookRegistration>) -> Self {
        let mut task_spawn = Vec::new();
        let mut worker_init = Vec::new();
        for hook in hooks {
            match hook {
                HookRegistration::TaskSpawn(hook) => task_spawn.push(Arc::new(hook)),
                HookRegistration::WorkerInit(hook) => worker_init.push(Arc::new(hook)),
                HookRegistration::Panic(_) => {}
            }
        }
        Self {
            registration: Mutex::new(()),
            closed: AtomicBool::new(false),
            has_task_spawn: AtomicBool::new(!task_spawn.is_empty()),
            task_spawn: ArcSwap::from_pointee(task_spawn),
            worker_init: ArcSwap::from_pointee(worker_init),
        }
    }

    pub(crate) fn push(&self, hook: HookRegistration) -> bool {
        let _registration = self.registration.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        match hook {
            HookRegistration::TaskSpawn(hook) => {
                Self::push_to(&self.task_spawn, hook);
                self.has_task_spawn.store(true, Ordering::Release);
            }
            HookRegistration::WorkerInit(hook) => Self::push_to(&self.worker_init, hook),
            HookRegistration::Panic(_) => {}
        }
        true
    }

    fn push_to<T>(hooks: &ArcSwap<Vec<Arc<T>>>, hook: T) {
        let hook = Arc::new(hook);
        hooks.rcu(move |current| {
            let mut updated = Vec::with_capacity(current.len().saturating_add(1));
            updated.extend(current.iter().cloned());
            updated.push(Arc::clone(&hook));
            Arc::new(updated)
        });
    }

    pub(crate) fn execute_task_spawn(&self, create_event: impl FnOnce() -> TaskSpawn) -> Option<TaskObservers> {
        if !self.has_task_spawn.load(Ordering::Acquire) {
            return None;
        }
        let hooks = self.task_spawn.load();
        if hooks.is_empty() {
            return None;
        }
        let mut event = create_event();
        for hook in hooks.iter() {
            hook.execute(&mut event);
        }
        event.into_observers()
    }

    pub(crate) fn clear(&self) -> ClearedHooks {
        let _registration = self.registration.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.closed.store(true, Ordering::Release);
        self.has_task_spawn.store(false, Ordering::Release);
        ClearedHooks {
            _task_spawn: self.task_spawn.swap(Arc::new(Vec::new())),
            _worker_init: self.worker_init.swap(Arc::new(Vec::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.task_spawn.load().len().saturating_add(self.worker_init.load().len())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::AtomicUsize;

    use thread_aware::{Thread, ThreadAware};

    use super::*;
    use crate::runtime::hook::{HookFn, Panic, registration};

    #[derive(Clone)]
    struct Calls(Arc<AtomicUsize>);

    impl ThreadAware for Calls {
        fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
    }

    #[test]
    fn publishes_executes_and_clears_snapshots() {
        let spawn_calls = Arc::new(AtomicUsize::new(0));
        let worker_init_calls = Arc::new(AtomicUsize::new(0));
        let hooks = RetainedHooks::new(Vec::new());
        let created = Cell::new(false);
        drop(hooks.execute_task_spawn(|| {
            created.set(true);
            TaskSpawn::new(None)
        }));
        assert!(!created.get());

        hooks.has_task_spawn.store(true, Ordering::Release);
        assert!(
            hooks
                .execute_task_spawn(|| {
                    created.set(true);
                    TaskSpawn::new(None)
                })
                .is_none()
        );
        assert!(!created.get());
        hooks.has_task_spawn.store(false, Ordering::Release);

        let panic_calls = Arc::new(AtomicUsize::new(0));
        let panic_hook = registration(HookFn::<Panic, _>::new(Calls(Arc::clone(&panic_calls)), |_event, calls| {
            calls.0.fetch_add(1, Ordering::Relaxed);
        }));
        let panic_only = RetainedHooks::new(vec![panic_hook.clone()]);
        assert_eq!(panic_only.len(), 0);
        assert!(hooks.push(panic_hook));
        assert_eq!(hooks.len(), 0);

        assert!(hooks.push(registration(HookFn::<TaskSpawn, _>::new(
            Calls(Arc::clone(&spawn_calls)),
            |_event, calls| {
                calls.0.fetch_add(1, Ordering::Relaxed);
            },
        ))));
        assert!(hooks.push(registration(HookFn::<WorkerInit, _>::new(
            Calls(Arc::clone(&worker_init_calls)),
            |_event, calls| {
                calls.0.fetch_add(1, Ordering::Relaxed);
            },
        ))));
        drop(hooks.execute_task_spawn(|| TaskSpawn::new(None)));
        drop(hooks.execute_task_spawn(|| TaskSpawn::new(None)));
        assert_eq!(hooks.len(), 2);
        drop(hooks.clear());
        assert!(!hooks.push(registration(HookFn::<WorkerInit, _>::new(
            Calls(Arc::clone(&worker_init_calls)),
            |_event, _calls| {},
        ))));
        assert_eq!(hooks.len(), 0);
        drop(hooks.execute_task_spawn(|| {
            created.set(true);
            TaskSpawn::new(None)
        }));

        assert_eq!(spawn_calls.load(Ordering::Relaxed), 2);
        assert_eq!(worker_init_calls.load(Ordering::Relaxed), 0);
        assert_eq!(panic_calls.load(Ordering::Relaxed), 0);
        assert!(!created.get());
    }
}
