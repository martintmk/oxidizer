// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::inspection::Inspector;
use crate::{ExecutorShared, TaskCompletion, TaskObserver};

/// Configures a task before registering it with an executor.
///
/// Create this builder with [`TaskSet::task`](crate::TaskSet::task). Configuration
/// does not convert or register the future. Dropping the builder drops its inputs
/// without registering a task.
#[derive(derive_more::Debug)]
#[must_use = "call register to submit the task"]
pub struct TaskBuilder<'a, F, I = (), O = ()> {
    shared: &'a ExecutorShared,
    #[debug(skip)]
    future: F,
    #[debug(skip)]
    inspector: I,
    #[debug(skip)]
    observer: O,
}

impl<'a, F> TaskBuilder<'a, F> {
    pub(crate) fn new(shared: &'a ExecutorShared, future: F) -> Self {
        Self {
            shared,
            future,
            inspector: (),
            observer: (),
        }
    }
}

impl<'a, F, I, O> TaskBuilder<'a, F, I, O> {
    /// Sets a callback that describes the converted future during explicit inspection.
    ///
    /// The callback is stored with the future without a separate task allocation.
    /// Ordinary execution cycles do not call it. Calling this method again replaces
    /// the previous callback.
    #[cfg(feature = "inspection")]
    pub fn inspect<D, J>(self, inspect: J) -> TaskBuilder<'a, F, impl Inspector<F::IntoFuture> + use<F, I, O, D, J>, O>
    where
        F: IntoFuture,
        D: 'static,
        J: Fn(&F::IntoFuture, crate::TaskState) -> Option<D> + 'static,
    {
        TaskBuilder {
            shared: self.shared,
            future: self.future,
            inspector: crate::inspection::InspectWith::new(inspect),
            observer: self.observer,
        }
    }

    /// Sets the value that observes each top-level task poll.
    ///
    /// The observer runs on the executor thread. It must always return normally.
    /// Calling this method again replaces the previous observer.
    pub fn observe<Q: TaskObserver>(self, observer: Q) -> TaskBuilder<'a, F, I, Q> {
        TaskBuilder {
            shared: self.shared,
            future: self.future,
            inspector: self.inspector,
            observer,
        }
    }
}

impl<F, I, O> TaskBuilder<'_, F, I, O>
where
    F: IntoFuture + 'static,
    F::Output: 'static,
    I: Inspector<F::IntoFuture>,
    O: TaskObserver,
{
    /// Registers the task and its completion target.
    ///
    /// The executor polls the future until it completes. Shutdown can cancel and
    /// drop the future before completion. If shutdown already started, registration
    /// reports cancellation without converting the future.
    ///
    /// Registration does not wake the executor owner. The caller must arrange an
    /// [`execute_cycle`](crate::Executor::execute_cycle) after registration.
    pub fn register(self, completion: impl TaskCompletion<F::Output>) {
        self.shared.add_task(self.future, self.observer, completion, self.inspector);
    }
}
