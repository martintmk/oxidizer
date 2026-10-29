// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::cell::RefCell;

use arty_executor::{Shutdown as ExecutorShutdown, ShutdownCause, TaskSet};
use thread_aware::Thread;

use crate::io::RuntimeDriverHost;
use crate::runtime::dispatch::DispatcherClient;
use crate::runtime::worker;
use crate::task::Builtins;
use crate::task::handle::JoinHandlePool;

thread_local! {
    static CURRENT: RefCell<Option<LocalWorker>> = const { RefCell::new(None) };
}

/// Task worker state that remains attached throughout thread teardown.
#[derive(Debug)]
pub(crate) struct LocalWorker {
    scheduler: Option<SchedulerContext>,
    driver_host: Option<RuntimeDriverHost>,
}

impl LocalWorker {
    fn new() -> Self {
        Self {
            scheduler: None,
            driver_host: None,
        }
    }

    pub(crate) fn attach() {
        worker::WorkerKind::attach(worker::WorkerKind::Task);
        CURRENT.with_borrow_mut(|current| {
            assert!(current.is_none(), "a task local worker is already attached to this thread");
            *current = Some(Self::new());
        });
    }

    pub(super) fn register_scheduler(builtins: Builtins, tasks: TaskSet, executor_shutdown: ExecutorShutdown) {
        let results = JoinHandlePool::new(builtins.dispatcher().shutdown().clone());
        let scheduler = SchedulerContext {
            thread: builtins.thread().clone(),
            builtins,
            tasks,
            executor_shutdown,
            results,
        };
        Self::with_current_mut(|current| {
            let current = current.expect("task worker startup attaches its local worker before registering the scheduler");
            assert!(current.scheduler.is_none(), "a scheduler is already registered on this task worker");
            current.scheduler = Some(scheduler);
        });
    }

    pub(crate) fn is_current() -> bool {
        Self::with_current(|current| current.is_some())
    }

    pub(crate) fn current_thread() -> Option<Thread> {
        Self::with_scheduler(|current| current.map(|current| current.thread().clone()))
    }

    pub(crate) fn with_scheduler<R>(f: impl FnOnce(Option<&SchedulerContext>) -> R) -> R {
        Self::with_current(|current| f(current.and_then(|current| current.scheduler.as_ref())))
    }

    pub(crate) fn begin_executor_shutdown(dispatcher: &DispatcherClient, cause: ShutdownCause) {
        Self::with_scheduler(|current| {
            let Some(current) = current else {
                return;
            };
            if dispatcher.owns(current.thread()) {
                current.executor_shutdown.trigger(cause);
            }
        });
    }

    pub(super) fn clear_scheduler() {
        let scheduler = Self::with_current_mut(|current| current.and_then(|current| current.scheduler.take()));
        drop(scheduler);
    }

    pub(crate) fn install_driver_host(driver_host: RuntimeDriverHost) {
        Self::with_current_mut(|current| {
            let current = current.expect("task worker startup attaches its local worker before installing the I/O driver host");
            assert!(
                current.driver_host.is_none(),
                "an I/O driver host is already installed on this task worker"
            );
            current.driver_host = Some(driver_host);
        });
    }

    pub(crate) fn uninstall_driver_host() -> RuntimeDriverHost {
        Self::with_current_mut(|current| {
            let current = current.expect("task worker teardown retains its local worker while uninstalling the I/O driver host");
            current
                .driver_host
                .take()
                .expect("the task worker must uninstall the I/O driver host that it installed")
        })
    }

    pub(crate) fn with_driver_host<R>(f: impl FnOnce(&RuntimeDriverHost) -> R) -> R {
        Self::try_with_driver_host(f).expect("the task worker must install its I/O driver host before running")
    }

    pub(crate) fn try_with_driver_host<R>(f: impl FnOnce(&RuntimeDriverHost) -> R) -> Option<R> {
        Self::with_current(|current| current.and_then(|current| current.driver_host.as_ref().map(f)))
    }

    #[cfg(test)]
    pub(crate) fn has_scheduler_context() -> bool {
        Self::with_scheduler(|current| current.is_some())
    }

    pub(crate) fn with_current<R>(f: impl FnOnce(Option<&Self>) -> R) -> R {
        CURRENT.with_borrow(|current| f(current.as_ref()))
    }

    fn with_current_mut<R>(f: impl FnOnce(Option<&mut Self>) -> R) -> R {
        CURRENT.with_borrow_mut(|current| f(current.as_mut()))
    }
}

/// Task worker state available while direct task registration is safe.
#[derive(Debug)]
pub(crate) struct SchedulerContext {
    thread: Thread,
    builtins: Builtins,
    tasks: TaskSet,
    executor_shutdown: ExecutorShutdown,
    results: JoinHandlePool,
}

impl SchedulerContext {
    pub(crate) fn thread(&self) -> &Thread {
        &self.thread
    }

    pub(crate) fn builtins(&self) -> &Builtins {
        &self.builtins
    }

    pub(crate) fn tasks(&self) -> &TaskSet {
        &self.tasks
    }

    pub(crate) fn results(&self) -> &JoinHandlePool {
        &self.results
    }
}
