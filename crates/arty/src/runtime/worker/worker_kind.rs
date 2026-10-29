// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::cell::Cell;

thread_local! {
    static CURRENT: Cell<Option<WorkerKind>> = const { Cell::new(None) };
}

/// Identifies the worker kind attached to the current thread.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WorkerKind {
    Blocking,
    Task,
}

impl WorkerKind {
    #[cfg_attr(test, mutants::skip)] // Removing worker classification lets worker-side runtime shutdown self-join.
    pub(crate) fn attach(worker: Self) {
        assert!(
            CURRENT.replace(Some(worker)).is_none(),
            "a local worker is already attached to this thread"
        );
    }

    #[cfg_attr(test, mutants::skip)] // Returning false lets worker-side runtime shutdown self-join.
    pub(crate) fn is_current() -> bool {
        CURRENT.get().is_some()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding is not runtime behavior.
mod tests {
    use super::*;
    use crate::runtime::worker::asynchronous;

    thread_local! {
        static OWNER: Owner = const { Owner };
    }

    struct Owner;

    impl Drop for Owner {
        fn drop(&mut self) {
            assert!(WorkerKind::is_current());
        }
    }

    #[test]
    fn worker_kind_attaches_to_the_current_thread() {
        WorkerKind::attach(WorkerKind::Task);
        assert!(WorkerKind::is_current());
    }

    #[test]
    fn marker_remains_accessible_after_destructible_worker_state() {
        std::thread::spawn(|| {
            OWNER.with(|_| {});
            asynchronous::LocalWorker::attach();
        })
        .join()
        .unwrap();
    }
}
