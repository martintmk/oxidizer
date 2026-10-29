// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::cell::RefCell;

use performables::arc::Arc;

use crate::runtime::worker;
use crate::runtime::worker::blocking::PoolIdentity;

thread_local! {
    static CURRENT: RefCell<Option<LocalWorker>> = const { RefCell::new(None) };
}

/// Blocking pool identity attached to each pool thread.
#[derive(Debug)]
pub(crate) struct LocalWorker {
    pool: PoolIdentity,
}

impl LocalWorker {
    pub(crate) fn attach(pool: PoolIdentity) {
        worker::WorkerKind::attach(worker::WorkerKind::Blocking);
        CURRENT.with_borrow_mut(|current| {
            assert!(current.is_none(), "a blocking local worker is already attached to this thread");
            *current = Some(Self { pool });
        });
    }

    fn owns_pool(&self, pool: &PoolIdentity) -> bool {
        Arc::ptr_eq(&self.pool, pool)
    }

    pub(crate) fn is_current_pool(pool: &PoolIdentity) -> bool {
        Self::with_current(|current| current.is_some_and(|current| current.owns_pool(pool)))
    }

    pub(crate) fn with_current<R>(f: impl FnOnce(Option<&Self>) -> R) -> R {
        CURRENT.with_borrow(|current| f(current.as_ref()))
    }
}
