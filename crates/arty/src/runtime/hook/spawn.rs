// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::task::Poll;

use smallvec::SmallVec;
use thread_aware::{Thread, ThreadAware};

use super::hook_fn::ErasedHookFn;

/// Observes each top-level poll of an async task.
///
/// The handler runs on one async worker. Arty relocates it to that worker
/// before the task can run.
///
/// The scope includes the first poll, wake-driven polls, completion, and task
/// panics. It excludes blocking functions and nested future polls inside one
/// top-level task poll. Implementations must not panic. Arty does not catch
/// panics from these methods.
pub trait TaskObserver: ThreadAware + 'static {
    /// Runs immediately before Arty polls the task.
    fn poll_start(&mut self);

    /// Runs immediately after Arty polls the task.
    ///
    /// `outcome` is `Poll::Pending` if the task remains pending. It is
    /// `Poll::Ready(())` if the task completes or panics.
    fn poll_end(&mut self, outcome: Poll<()>);
}

/// Reports that Arty is submitting an async task.
///
/// Arty creates this event before it sends the task to its selected worker.
/// On a runtime worker, Arty uses that worker's hook instance. Outside runtime
/// workers, Arty uses a worker-neutral hook instance. The event covers tasks submitted through
/// [`RuntimeScheduler`](crate::task::RuntimeScheduler) and
/// [`Scheduler`](crate::task::Scheduler), including worker-specific and
/// relocating submissions. Arty does not create this event for a task that it
/// rejects before registration. The callback must not panic.
#[derive(derive_more::Debug)]
pub struct TaskSpawn {
    pub(super) thread: Option<Thread>,
    #[debug(ignore)]
    pub(super) observers: Option<TaskObservers>,
}

impl TaskSpawn {
    pub(crate) fn new(thread: Option<Thread>) -> Self {
        Self { observers: None, thread }
    }

    pub(crate) fn into_observers(self) -> Option<TaskObservers> {
        self.observers
    }

    /// Returns the worker that submitted the task.
    ///
    /// Returns `None` when the submission originated outside a runtime worker.
    #[must_use]
    pub fn thread(&self) -> Option<&Thread> {
        self.thread.as_ref()
    }

    /// Adds a poll observer to the task.
    ///
    /// The first observer is the outermost observer. Arty relocates `observer`
    /// on the selected worker before it registers the task. The first two
    /// observers allocate only their boxed trait objects. They do not allocate
    /// a separate collection buffer.
    pub fn observe<O: TaskObserver>(&mut self, observer: O) {
        let observer = Box::new(observer) as Box<dyn TaskObserver>;
        self.observers = Some(match self.observers.take() {
            Some(observers) => observers.push(observer),
            None => TaskObservers::new(self.thread.clone(), observer),
        });
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct TaskObservers {
    source: Option<Thread>,
    #[debug(ignore)]
    observers: SmallVec<[Box<dyn TaskObserver>; 2]>,
}

impl TaskObservers {
    fn new(source: Option<Thread>, observer: Box<dyn TaskObserver>) -> Self {
        let mut observers = SmallVec::new();
        observers.push(observer);
        Self { source, observers }
    }

    fn push(mut self, observer: Box<dyn TaskObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    pub(crate) fn into_single(mut self) -> Result<Box<dyn TaskObserver>, Self> {
        if self.observers.len() == 1 {
            return Ok(self.observers.pop().expect("observer count was exactly one"));
        }
        Err(self)
    }

    pub(crate) fn relocate_to(&mut self, destination: &Thread) {
        for observer in &mut self.observers {
            observer.relocate(self.source.as_ref(), destination);
        }
        self.source = Some(destination.clone());
    }

    pub(crate) fn poll_start(&mut self) {
        for observer in &mut self.observers {
            observer.poll_start();
        }
    }

    pub(crate) fn poll_end(&mut self, outcome: Poll<()>) {
        for observer in self.observers.iter_mut().rev() {
            observer.poll_end(outcome);
        }
    }
}

impl arty_executor::TaskObserver for Box<dyn TaskObserver> {
    fn poll_start(&mut self) {
        TaskObserver::poll_start(self.as_mut());
    }

    fn poll_end(&mut self, outcome: Poll<()>) {
        TaskObserver::poll_end(self.as_mut(), outcome);
    }
}

impl arty_executor::TaskObserver for TaskObservers {
    fn poll_start(&mut self) {
        Self::poll_start(self);
    }

    fn poll_end(&mut self, outcome: Poll<()>) {
        Self::poll_end(self, outcome);
    }
}

impl super::Hook for TaskSpawn {}

impl super::private::Sealed for TaskSpawn {
    type Erased<D>
        = ErasedHookFn<Self>
    where
        D: ThreadAware + Clone + Sync + 'static;
}

#[cfg(all(test, not(miri)))]
mod tests {
    use std::alloc::System;
    use std::hint::black_box;

    use alloc_tracker::{Allocator, Session};
    use thread_aware::ThreadBuilder;

    use super::*;

    #[global_allocator]
    static ALLOCATOR: Allocator<System> = Allocator::system();

    const MEASURED_OPERATIONS: usize = 1_000;

    struct OneBytePoll(u8);

    impl ThreadAware for OneBytePoll {
        fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
    }

    impl TaskObserver for OneBytePoll {
        fn poll_start(&mut self) {
            black_box(self.0);
        }

        fn poll_end(&mut self, _outcome: Poll<()>) {}
    }

    #[test]
    fn one_observer_uses_one_allocation() {
        assert_eq!(observer_allocations(1), MEASURED_OPERATIONS as u64);
    }

    #[test]
    fn two_observers_use_two_allocations() {
        assert_eq!(observer_allocations(2), (MEASURED_OPERATIONS * 2) as u64);
    }

    #[test]
    fn third_observer_allocates_collection_buffer() {
        assert_eq!(observer_allocations(3), (MEASURED_OPERATIONS * 4) as u64);
    }

    #[test]
    fn into_single_returns_only_observer() {
        let mut event = TaskSpawn::new(None);
        event.observe(OneBytePoll(1));

        assert!(event.into_observers().unwrap().into_single().is_ok());
    }

    #[test]
    fn into_single_returns_original_collection_for_multiple_observers() {
        let mut event = TaskSpawn::new(None);
        event.observe(OneBytePoll(1));
        event.observe(OneBytePoll(2));
        let observers = event.into_observers().unwrap();

        let Err(observers) = observers.into_single() else {
            panic!("multiple observers cannot become one observer");
        };
        assert_eq!(observers.observers.len(), 2);
    }

    fn observer_allocations(observer_count: usize) -> u64 {
        let worker = ThreadBuilder::default().build(std::thread::current().id());
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("observers");
        {
            let _span = operation.measure_thread().iterations(MEASURED_OPERATIONS as u64);
            for value in 0..MEASURED_OPERATIONS {
                let mut event = TaskSpawn::new(Some(worker.clone()));
                for observer in 0..observer_count {
                    event.observe(OneBytePoll((value + observer) as u8));
                }
                black_box(event.into_observers());
            }
        }

        session
            .to_report()
            .operations()
            .find_map(|(name, operation)| (name == "observers").then(|| operation.total_allocations_count()))
            .unwrap()
    }
}
