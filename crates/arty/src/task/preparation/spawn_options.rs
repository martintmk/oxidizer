// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::marker::PhantomData;

use arty_executor::{TaskCompletion, TaskInstrumentation, TaskSet};

use crate::core::Thread;
use crate::runtime::hook::TaskObservers;
/// Captures submission metadata before a task crosses threads.
pub(crate) struct TaskSpawnOptions<Body, S = ()> {
    #[cfg(any(test, feature = "monitor"))]
    parent: Option<seismograph::recorder::runtime::TaskId>,
    #[cfg(any(test, feature = "monitor"))]
    body_size: S,
    marker: PhantomData<fn() -> (Body, S)>,
    observers: Option<TaskObservers>,
}

impl<Body: 'static> TaskSpawnOptions<Body> {
    pub(crate) fn capture() -> TaskSpawnOptions<Body, impl Fn() -> usize + Send> {
        // A function item stores no bytes; only erased scoped futures carry a size.
        TaskSpawnOptions::new(size_of::<Body>)
    }

    pub(crate) fn with_body_size(body_size: usize) -> TaskSpawnOptions<Body, impl Fn() -> usize + Send> {
        TaskSpawnOptions::new(move || body_size)
    }
}

impl<Body, S> TaskSpawnOptions<Body, S> {
    fn new(body_size: S) -> Self {
        #[cfg(not(any(test, feature = "monitor")))]
        drop(body_size);
        Self {
            #[cfg(any(test, feature = "monitor"))]
            parent: crate::runtime::monitoring::current_task(),
            #[cfg(any(test, feature = "monitor"))]
            body_size,
            marker: PhantomData,
            observers: None,
        }
    }
}

impl<Body, S> TaskSpawnOptions<Body, S>
where
    Body: 'static,
    S: Fn() -> usize + 'static,
{
    #[cfg_attr(
        not(any(test, feature = "monitor")),
        expect(clippy::unused_self, reason = "Submission metadata is empty when monitoring is compiled out.")
    )]
    pub(crate) fn observed_by(mut self, observers: Option<TaskObservers>) -> Self {
        self.observers = observers;
        self
    }

    pub(crate) fn register<F, R, T>(mut self, destination: &Thread, tasks: &TaskSet, future: F, completion: T)
    where
        F: IntoFuture<Output = R> + 'static,
        R: 'static,
        T: TaskCompletion<R>,
    {
        let Some(observers) = self.observers.take() else {
            self.register_with(tasks, future, completion, ());
            return;
        };
        let observers = observers.relocate_to(destination);
        match observers.into_single() {
            Ok(observer) => self.register_with(tasks, future, completion, observer),
            Err(observers) => self.register_with(tasks, future, completion, observers),
        }
    }

    fn register_with<F, R, T, I>(self, tasks: &TaskSet, future: F, completion: T, instrumentation: I)
    where
        F: IntoFuture<Output = R> + 'static,
        R: 'static,
        T: TaskCompletion<R>,
        I: TaskInstrumentation<F::IntoFuture>,
    {
        #[cfg(any(test, feature = "monitor"))]
        crate::runtime::monitoring::register_monitored::<Body, _, _, _, _, _>(
            tasks,
            future,
            completion,
            instrumentation,
            self.parent,
            self.body_size,
        );
        // #[gamma::skip(stmt.delete_call, reason = "This branch is absent from Gamma's all-features builds; runtime-only integration tests exercise it.")]
        #[cfg(not(any(test, feature = "monitor")))]
        tasks.task(future).instrument(instrumentation).register(completion);
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::pin::Pin;

    use arty_executor::{Executor, InspectionStatus};
    use seismograph_runtime::RuntimeMetadata;
    use seismograph_runtime::worker::{WorkerMetadata, WorkerRole};
    use thread_aware::ThreadBuilder;

    use super::*;
    use crate::runtime::Shutdown;
    use crate::runtime::monitoring::{Description, enter_current, set_events};
    use crate::task::handle::JoinHandlePool;

    #[test]
    fn erased_submission_preserves_its_original_size_and_submission_parent() {
        type ErasedFuture = Pin<Box<dyn Future<Output = ()>>>;

        let _monitoring = crate::runtime::monitoring::TEST_LOCK.lock().unwrap();
        let runtime = seismograph_runtime::register_runtime(RuntimeMetadata::new("submission-metadata", 1));
        let worker = runtime.register_worker(WorkerMetadata::new(WorkerRole::Core));
        let parent = seismograph_runtime::allocate_task_id();
        let options = {
            let _parent = enter_current(parent);
            TaskSpawnOptions::<ErasedFuture>::with_body_size(37)
        };
        let _other_parent = enter_current(seismograph_runtime::allocate_task_id());
        set_events(Some(worker.handle().task_events()));
        let _events = scopeguard::guard((), |()| set_events(None));
        let mut executor = Executor::builder().build();
        let results = JoinHandlePool::new(Shutdown::default());
        let (_handle, sender) = results.handle();
        let future: ErasedFuture = Box::pin(std::future::pending());
        let destination = ThreadBuilder::default().build(std::thread::current().id());
        options.register(&destination, &executor.tasks(), future, sender);
        let mut snapshots = Vec::new();

        let status = executor
            .inspect(1, |description: &Description| {
                if let Description::Snapshot(task) = description {
                    snapshots.push(task.clone());
                }
            })
            .unwrap();

        assert_eq!(status, InspectionStatus::Complete);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].parent, Some(parent));
        assert_eq!(snapshots[0].future_size_bytes, Some(37));
    }
}
