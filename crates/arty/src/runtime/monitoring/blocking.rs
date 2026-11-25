// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use seismograph_runtime::task::{BlockingPool, BlockingTask as MonitoredBlockingTask, BlockingTaskExecution};

use super::{current_task, descriptor};
use crate::runtime::shutdown::{Shutdown, ShutdownState};
use crate::task::preparation::BlockingTask;

pub(crate) enum BlockingJob {
    Plain(Box<dyn FnOnce() + Send + 'static>),
    Monitored(std::sync::Arc<dyn MonitoredBlockingJob>),
}

impl BlockingJob {
    #[cfg(test)]
    pub(crate) fn run(self) {
        self.run_with(None);
    }

    fn run_with(self, execution: Option<&BlockingTaskExecution<'_>>) {
        match self {
            Self::Plain(body) => body(),
            Self::Monitored(body) => body.run(execution.expect("monitored blocking work has an execution token")),
        }
    }

    pub(crate) fn monitoring(&self) -> Option<&std::sync::Arc<dyn MonitoredBlockingJob>> {
        match self {
            Self::Plain(_) => None,
            Self::Monitored(body) => Some(body),
        }
    }
}

pub(crate) trait MonitoredBlockingJob: Send + Sync {
    fn info(&self) -> &MonitoredBlockingTask;
    fn run(&self, execution: &BlockingTaskExecution<'_>);
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BlockingPoolMonitoring {
    pool: std::sync::Arc<std::sync::OnceLock<BlockingPool>>,
}

impl BlockingPoolMonitoring {
    pub(crate) fn task<F: 'static>(&self, shutdown: &Shutdown) -> BlockingTaskMonitoring {
        BlockingTaskMonitoring {
            info: matches!(shutdown.state(), ShutdownState::Running)
                .then(|| self.pool().map(|pool| pool.task(descriptor::<F>, current_task(), size_of::<F>())))
                .flatten(),
        }
    }

    pub(crate) fn enable(&self, pool: BlockingPool) {
        self.pool.set(pool).expect("monitoring is enabled once under the queue lock");
    }

    pub(crate) fn pool(&self) -> Option<BlockingPool> {
        self.pool.get().copied()
    }
}

#[derive(Default)]
pub(crate) struct BlockingTaskMonitoring {
    info: Option<MonitoredBlockingTask>,
}

impl BlockingTaskMonitoring {
    pub(crate) fn prepare<F, R>(self, task: BlockingTask<F, R>) -> BlockingJob
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Some(info) = self.info {
            return BlockingJob::Monitored(std::sync::Arc::new(MonitoredTask {
                task: std::sync::Mutex::new(task),
                info,
            }));
        }
        BlockingJob::Plain(Box::new(move || task.run(BlockingTaskEvents::default())))
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct BlockingTaskEvents<'execution, 'task> {
    execution: Option<&'execution BlockingTaskExecution<'task>>,
}

impl BlockingTaskEvents<'_, '_> {
    pub(crate) fn completed(self) {
        if let Some(execution) = self.execution {
            execution.completed();
        }
    }

    pub(crate) fn panicked(self) {
        if let Some(execution) = self.execution {
            execution.panicked();
        }
    }
}

// Inspection shares this allocation with execution. Only taking the callback
// needs the mutex; neither the callback nor its destructor runs under the lock.
struct MonitoredTask<F, R: Send + 'static> {
    task: std::sync::Mutex<BlockingTask<F, R>>,
    info: MonitoredBlockingTask,
}

impl<F, R> MonitoredBlockingJob for MonitoredTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn info(&self) -> &MonitoredBlockingTask {
        &self.info
    }

    fn run(&self, execution: &BlockingTaskExecution<'_>) {
        let task = std::mem::replace(
            &mut *self.task.lock().expect("taking a blocking callback does not execute user code"),
            BlockingTask { pending: None },
        );
        task.run(BlockingTaskEvents {
            execution: Some(execution),
        });
    }
}

impl<F, R: Send + 'static> Drop for MonitoredTask<F, R> {
    fn drop(&mut self) {
        if self
            .task
            .get_mut()
            .expect("taking a blocking callback does not execute user code")
            .pending
            .is_some()
        {
            self.info.canceled();
        }
    }
}

#[derive(Default)]
pub(crate) struct BlockingWorkerMonitoring {
    pub(crate) handle: Option<seismograph_runtime::worker::WorkerHandle>,
}

impl BlockingWorkerMonitoring {
    pub(crate) fn parked(&self) {
        if let Some(worker) = &self.handle {
            worker.parked();
        }
    }

    pub(crate) fn unparked(&self) {
        if let Some(worker) = &self.handle {
            worker.unparked();
        }
    }

    pub(crate) fn run(&self, body: BlockingJob, canceled: bool) -> std::thread::Result<()> {
        let monitoring = body.monitoring().cloned();
        let execution = self
            .handle
            .as_ref()
            .filter(|_| !canceled)
            .and_then(|worker| monitoring.as_ref().map(|body| body.info().started(worker)));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _current = execution
                .as_ref()
                .and_then(BlockingTaskExecution::observed_task_id)
                .map(super::task::enter_current);
            if canceled {
                drop(body);
            } else {
                body.run_with(execution.as_ref());
            }
        }));
        drop(execution);
        outcome
    }
}

#[cfg(all(test, feature = "test-util"))]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn a_monitored_callback_runs_and_delivers_its_result() {
        let _monitoring = crate::runtime::monitoring::TEST_LOCK.lock().unwrap();
        let runtime = seismograph_runtime::register_runtime(seismograph_runtime::RuntimeMetadata::new("blocking-callback-result", 1));
        let worker = runtime.register_worker(seismograph_runtime::worker::WorkerMetadata::new(
            seismograph_runtime::worker::WorkerRole::Blocking,
        ));
        let monitoring = BlockingPoolMonitoring::default();
        monitoring.enable(runtime.handle().blocking_pool());
        let shutdown = Shutdown::default();
        let pool = crate::task::handle::JoinHandlePool::new(shutdown.clone());
        let (body, handle) = crate::task::preparation::prepare_blocking(&pool, || 42, monitoring.task::<()>(&shutdown));

        let monitored = body.monitoring().unwrap().clone();
        monitored.info().submitted(1);
        let worker_handle = worker.handle();
        let execution = monitored.info().started(&worker_handle);
        body.run_with(Some(&execution));

        assert_eq!(handle.join().unwrap(), 42);
    }
}
