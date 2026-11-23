// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(any(test, feature = "monitor"))]
use super::TaskInfo;
use crate::runtime::shutdown::Shutdown;
#[cfg(any(test, feature = "monitor"))]
use crate::runtime::shutdown::ShutdownState;
use crate::task::preparation::BlockingTask;

pub(crate) enum BlockingJob {
    Plain(Box<dyn FnOnce() + Send + 'static>),
    #[cfg(any(test, feature = "monitor"))]
    Monitored(std::sync::Arc<dyn MonitoredBlockingJob>),
}

impl BlockingJob {
    pub(crate) fn run(self) -> Option<BlockingTaskOutcome> {
        match self {
            Self::Plain(body) => {
                body();
                None
            }
            #[cfg(any(test, feature = "monitor"))]
            Self::Monitored(body) => Some(body.run()),
        }
    }

    #[cfg(any(test, feature = "monitor"))]
    pub(crate) fn monitoring(&self) -> Option<&std::sync::Arc<dyn MonitoredBlockingJob>> {
        match self {
            Self::Plain(_) => None,
            Self::Monitored(body) => Some(body),
        }
    }
}

#[cfg(any(test, feature = "monitor"))]
pub(crate) trait MonitoredBlockingJob: Send + Sync {
    fn info(&self) -> &TaskInfo;
    fn run(&self) -> BlockingTaskOutcome;
}

#[derive(Clone, Copy)]
pub(crate) enum BlockingTaskOutcome {
    Completed,
    Panicked,
}

impl BlockingTaskOutcome {
    fn publish(self, info: &TaskInfo) {
        match self {
            Self::Completed => info.completed(),
            Self::Panicked => info.panicked(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BlockingPoolMonitoring {
    #[cfg(any(test, feature = "monitor"))]
    events: std::sync::Arc<std::sync::OnceLock<seismograph_runtime::task::EventContext>>,
}

impl BlockingPoolMonitoring {
    #[cfg_attr(
        not(any(test, feature = "monitor")),
        expect(
            clippy::unused_self,
            clippy::extra_unused_type_parameters,
            reason = "The monitoring interface is unchanged when monitoring is compiled out."
        )
    )]
    pub(crate) fn task<F: 'static>(&self, shutdown: &Shutdown) -> BlockingTaskMonitoring {
        #[cfg(not(any(test, feature = "monitor")))]
        let _ = shutdown;
        BlockingTaskMonitoring {
            #[cfg(any(test, feature = "monitor"))]
            info: matches!(shutdown.state(), ShutdownState::Running)
                .then(|| self.events().map(TaskInfo::new::<F>))
                .flatten(),
        }
    }

    #[cfg(any(test, feature = "monitor"))]
    pub(crate) fn enable(&self, events: seismograph_runtime::task::EventContext) {
        self.events.set(events).expect("monitoring is enabled once under the queue lock");
    }

    #[cfg(any(test, feature = "monitor"))]
    pub(crate) fn events(&self) -> Option<seismograph_runtime::task::EventContext> {
        self.events.get().copied()
    }
}

#[derive(Default)]
pub(crate) struct BlockingTaskMonitoring {
    #[cfg(any(test, feature = "monitor"))]
    info: Option<TaskInfo>,
}

impl BlockingTaskMonitoring {
    #[cfg_attr(
        not(any(test, feature = "monitor")),
        expect(clippy::unused_self, reason = "Task monitoring is empty when monitoring is compiled out.")
    )]
    pub(crate) fn prepare<F, R>(self, task: BlockingTask<F, R>) -> BlockingJob
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        #[cfg(any(test, feature = "monitor"))]
        if let Some(info) = self.info {
            return BlockingJob::Monitored(std::sync::Arc::new(MonitoredTask {
                task: std::sync::Mutex::new(task),
                info,
            }));
        }
        BlockingJob::Plain(Box::new(move || {
            let _outcome = task.run();
        }))
    }
}

// Inspection shares this allocation with execution. Only taking the callback
// needs the mutex; neither the callback nor its destructor runs under the lock.
#[cfg(any(test, feature = "monitor"))]
struct MonitoredTask<F, R: Send + 'static> {
    task: std::sync::Mutex<BlockingTask<F, R>>,
    info: TaskInfo,
}

#[cfg(any(test, feature = "monitor"))]
impl<F, R> MonitoredBlockingJob for MonitoredTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn info(&self) -> &TaskInfo {
        &self.info
    }

    fn run(&self) -> BlockingTaskOutcome {
        let task = std::mem::replace(
            &mut *self.task.lock().expect("taking a blocking callback does not execute user code"),
            BlockingTask { pending: None },
        );
        task.run()
    }
}

#[cfg(any(test, feature = "monitor"))]
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
    #[cfg(any(test, feature = "monitor"))]
    pub(crate) handle: Option<seismograph_runtime::worker::WorkerHandle>,
}

#[cfg_attr(
    not(any(test, feature = "monitor")),
    expect(clippy::unused_self, reason = "Worker monitoring is empty when monitoring is compiled out.")
)]
impl BlockingWorkerMonitoring {
    pub(crate) fn parked(&self) {
        #[cfg(any(test, feature = "monitor"))]
        if let Some(worker) = &self.handle {
            worker.parked();
        }
    }

    pub(crate) fn unparked(&self) {
        #[cfg(any(test, feature = "monitor"))]
        if let Some(worker) = &self.handle {
            worker.unparked();
        }
    }

    pub(crate) fn run(&self, body: BlockingJob, canceled: bool) -> std::thread::Result<()> {
        #[cfg(any(test, feature = "monitor"))]
        let observation = self.handle.as_ref().filter(|_| !canceled).and_then(|worker| {
            body.monitoring().cloned().map(|body| {
                let info = body.info();
                let queued_at = info.started_on(worker.task_events());
                let poll = worker.is_observed().then(|| worker.blocking_task_started(info.id(), queued_at));
                (worker, poll, body)
            })
        });
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            #[cfg(any(test, feature = "monitor"))]
            let _current = observation
                .as_ref()
                .and_then(|(_, poll, body)| poll.as_ref().map(|_| body))
                .map(|body| super::task::enter_current(body.info().id()));
            if canceled {
                drop(body);
                None
            } else {
                body.run()
            }
        }));
        #[cfg(any(test, feature = "monitor"))]
        if let Some((worker, poll, body)) = observation {
            if let Some(poll) = poll {
                worker.blocking_task_finished(poll);
            }
            return match outcome {
                Ok(Some(outcome)) => {
                    outcome.publish(body.info());
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(panic) => Err(panic),
            };
        }
        outcome.map(|_| ())
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
        monitoring.enable(worker.handle().task_events());
        let shutdown = Shutdown::default();
        let pool = crate::task::handle::JoinHandlePool::new(shutdown.clone());
        let (body, handle) = crate::task::preparation::prepare_blocking(&pool, || 42, monitoring.task::<()>(&shutdown));

        let _outcome = body.run();

        assert_eq!(handle.join().unwrap(), 42);
    }
}
