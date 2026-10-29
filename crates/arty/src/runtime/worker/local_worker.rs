// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::runtime::worker::{blocking, task};

/// Borrowed view of the worker state attached to the current thread.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LocalWorker<'a> {
    Blocking(&'a blocking::LocalWorker),
    Task(&'a task::LocalWorker),
}

impl LocalWorker<'_> {
    pub(crate) fn is_worker_thread() -> bool {
        Self::with_current(|current| match current {
            Some(LocalWorker::Blocking(_blocking)) => true,
            Some(LocalWorker::Task(_task)) => true,
            None => false,
        })
    }

    fn with_current<R>(f: impl for<'a> FnOnce(Option<LocalWorker<'a>>) -> R) -> R {
        task::LocalWorker::with_current(|task| match task {
            Some(task) => f(Some(LocalWorker::Task(task))),
            None => blocking::LocalWorker::with_current(|blocking| f(blocking.map(LocalWorker::Blocking))),
        })
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding is not runtime behavior.
mod tests {
    use performables::arc::Arc;

    use super::*;

    #[test]
    fn local_worker_attaches_to_the_current_thread() {
        let identity = Arc::new(());

        blocking::LocalWorker::attach(Arc::clone(&identity));

        assert!(LocalWorker::is_worker_thread());
        assert!(blocking::LocalWorker::is_current_pool(&identity));
    }
}
