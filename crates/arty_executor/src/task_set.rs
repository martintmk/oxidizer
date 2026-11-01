// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::panic::Location;

use crate::{ExecutorShared, TaskCompletion, TaskOptions};

/// A handle that registers tasks with an executor.
///
/// You can clone this handle and use each clone on the executor thread. A task
/// future can use a clone to register another task on the same executor.
#[derive(Clone, Debug)]
pub struct TaskSet {
    shared: ExecutorShared,
}

impl TaskSet {
    pub(crate) fn new(core: &ExecutorShared) -> Self {
        Self { shared: core.clone() }
    }

    /// Registers a future and its runtime integration options as a new task.
    ///
    /// The executor polls the future until it completes. Executor shutdown can
    /// cancel and drop the future before completion. Use [`TaskOptions::default()`]
    /// when the task needs no observed context or placement metadata.
    ///
    /// Records the caller location unless [`TaskOptions::location`] supplies
    /// an earlier submission location.
    #[track_caller]
    pub fn add<F, R, T>(&self, future: F, mut options: TaskOptions, completion: T)
    where
        F: IntoFuture<Output = R> + 'static,
        R: 'static,
        T: TaskCompletion<R>,
    {
        options.location.get_or_insert(Location::caller());
        self.shared.add_task(future, options, completion);
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::assert_not_impl_any;

    use super::*;

    #[test]
    fn thread_safety() {
        assert_not_impl_any!(TaskSet: Send, Sync);
    }
}
