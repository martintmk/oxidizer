// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Native-storage requests, task lifecycle events, and observed poll tokens.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, mpsc};

use seismograph::recorder::event::{BacktraceCapture, EventClass, EventKind, EventTimestamp, Record};
use seismograph::recorder::runtime::{RuntimeEvent, RuntimeId, TaskId, TypeDescriptorId, WorkerId};

use crate::snapshot::{Task, TaskActivityState, TaskCollection, TaskMetrics};
use crate::worker::WorkerHandle;
use crate::{WorkerControl, collection, lock};

/// Task-event identities and backtrace policy that implement [`Copy`].
///
/// Obtain this from [`WorkerHandle::task_events`](crate::worker::WorkerHandle::task_events).
/// It owns no registration, counter, task state, or shared handle.
#[derive(Clone, Copy, Debug)]
pub struct EventContext {
    runtime: RuntimeId,
    worker: Option<WorkerId>,
    backtrace: BacktraceCapture,
}

/// Identity and timestamp assigned while building a task creation event.
///
/// Sampling or a concurrent recording change can omit the event after this
/// metadata is assigned. Keep the identity for inspection and later outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskSpawn {
    /// Process-wide task identity.
    pub id: TaskId,
    /// Concrete task type identity used by the event.
    pub descriptor: TypeDescriptorId,
    /// Timestamp used by the task creation event.
    pub timestamp: EventTimestamp,
}

/// Worker-neutral instrumentation for one blocking work queue.
///
/// Obtain this from [`RuntimeHandle::blocking_pool`](crate::RuntimeHandle::blocking_pool).
/// Create a [`BlockingTask`] before adding work to the queue, then call
/// [`BlockingTask::submitted`] with the queue depth after a successful enqueue.
#[derive(Clone, Copy, Debug)]
pub struct BlockingPool {
    events: EventContext,
}

impl BlockingPool {
    pub(crate) const fn new(events: EventContext) -> Self {
        Self { events }
    }

    /// Creates metadata for a blocking task before its queue accepts it.
    ///
    /// `body_size` is the concrete synchronous callback size before runtime
    /// wrapping or type erasure. Call [`BlockingTask::submitted`] after the
    /// queue accepts the task.
    #[must_use]
    pub fn task(&self, descriptor: fn() -> TypeDescriptorId, parent: Option<TaskId>, body_size: usize) -> BlockingTask {
        BlockingTask::new(self.events, descriptor, parent, body_size)
    }

    /// Creates metadata and records a successful queue submission.
    ///
    /// `queue_depth` is the number of tasks waiting after this task enters the
    /// queue.
    #[must_use]
    pub fn submitted(
        &self,
        descriptor: fn() -> TypeDescriptorId,
        parent: Option<TaskId>,
        body_size: usize,
        queue_depth: usize,
    ) -> BlockingTask {
        let task = self.task(descriptor, parent, body_size);
        task.submitted(queue_depth);
        task
    }
}

#[derive(Clone, Copy, Debug)]
struct BlockingTaskIdentity {
    id: TaskId,
    descriptor: TypeDescriptorId,
}

/// Lifecycle and inspection metadata for one queued blocking task.
#[derive(Debug)]
pub struct BlockingTask {
    events: EventContext,
    descriptor: fn() -> TypeDescriptorId,
    parent: Option<TaskId>,
    body_size: usize,
    spawned_at: Option<EventTimestamp>,
    identity: OnceLock<BlockingTaskIdentity>,
    queued_at: OnceLock<Option<EventTimestamp>>,
    started: AtomicBool,
}

impl BlockingTask {
    fn new(events: EventContext, descriptor: fn() -> TypeDescriptorId, parent: Option<TaskId>, body_size: usize) -> Self {
        let spawn = events.spawned_with(descriptor, parent, body_size);
        Self {
            events,
            descriptor,
            parent,
            body_size,
            spawned_at: spawn.map(|spawn| spawn.timestamp),
            identity: spawn.map_or_else(OnceLock::new, |spawn| {
                OnceLock::from(BlockingTaskIdentity {
                    id: spawn.id,
                    descriptor: spawn.descriptor,
                })
            }),
            queued_at: OnceLock::new(),
            started: AtomicBool::new(false),
        }
    }

    /// Records that the queue accepted this task.
    ///
    /// `queue_depth` is the number of tasks waiting after this task enters the
    /// queue.
    ///
    /// # Panics
    ///
    /// Panics if this task was already submitted or if the queue depth does not
    /// fit in `u64`.
    pub fn submitted(&self, queue_depth: usize) {
        let queue_depth = u64::try_from(queue_depth).expect("a queue depth fits in u64");
        let mut queued_at = None;
        seismograph::record(EventClass::RuntimeTask, || {
            let timestamp = EventTimestamp::now();
            queued_at = Some(timestamp);
            Some(Record::runtime(
                timestamp,
                EventKind::TaskEnqueued,
                RuntimeEvent {
                    runtime_id: self.events.runtime,
                    worker_id: None,
                    subject_id: self.id().get(),
                    related_id: 0,
                    value_0: queue_depth,
                    value_1: 0,
                },
                BacktraceCapture::Never,
            ))
        });
        self.queued_at
            .set(queued_at)
            .expect("a blocking task is submitted to its queue exactly once");
    }

    /// Returns the task identity, allocating it on first inspection if needed.
    #[must_use]
    pub fn id(&self) -> TaskId {
        self.identity().id
    }

    /// Describes the task in its queue or execution slot.
    ///
    /// # Panics
    ///
    /// Panics if the queue did not accept the task.
    #[must_use]
    pub fn describe(&self, state: TaskActivityState) -> Task {
        let identity = self.identity();
        let mut task = Task::new(identity.id, identity.descriptor, state);
        task.parent = self.parent;
        task.spawned_at = self.spawned_at;
        task.future_size_bytes = Some(u64::try_from(self.body_size).expect("a task body fits the platform address space"));
        let queued_at = *self.queued_at.get().expect("describe a blocking task only after queue submission");
        if matches!(state, TaskActivityState::Ready)
            && let Some(activity) = &mut task.activity
        {
            activity.ready_since = queued_at;
            activity.queued_since = queued_at;
        }
        task
    }

    /// Binds this task to the blocking worker that starts it.
    ///
    /// Dropping the returned token finishes any observed poll.
    ///
    /// # Panics
    ///
    /// Panics if the task was not submitted or already started.
    pub fn started<'a>(&'a self, worker: &'a WorkerHandle) -> BlockingTaskExecution<'a> {
        assert!(self.queued_at.get().is_some(), "start a blocking task only after queue submission");
        assert!(
            self.started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "a blocking task starts on exactly one worker"
        );
        let poll = worker.is_observed().then(|| worker.task_poll_started(self.id()));
        BlockingTaskExecution { task: self, worker, poll }
    }

    /// Records cancellation before execution starts.
    pub fn canceled(&self) {
        if let Some(id) = self.terminal_id() {
            self.events.canceled(id);
        }
    }

    fn identity(&self) -> BlockingTaskIdentity {
        *self.identity.get_or_init(|| BlockingTaskIdentity {
            id: crate::allocate_task_id(),
            descriptor: (self.descriptor)(),
        })
    }

    fn terminal_id(&self) -> Option<TaskId> {
        self.identity
            .get()
            .map(|identity| identity.id)
            .or_else(|| seismograph::recorder::is_enabled(EventClass::RuntimeTask).then(|| self.id()))
    }
}

/// One blocking task bound to its execution worker.
#[derive(Debug)]
#[must_use = "keep the execution token until the blocking callback finishes"]
pub struct BlockingTaskExecution<'a> {
    task: &'a BlockingTask,
    worker: &'a WorkerHandle,
    poll: Option<TaskPoll>,
}

impl BlockingTaskExecution<'_> {
    /// Returns the task identity when this execution is observed.
    #[must_use]
    pub fn observed_task_id(&self) -> Option<TaskId> {
        self.poll.as_ref().map(|poll| poll.task)
    }

    /// Records successful completion on the execution worker.
    pub fn completed(&self) {
        if let Some(id) = self.task.terminal_id() {
            self.worker.task_events().completed(id);
        }
    }

    /// Records panic completion on the execution worker.
    pub fn panicked(&self) {
        if let Some(id) = self.task.terminal_id() {
            self.worker.task_events().panicked(id);
        }
    }
}

impl Drop for BlockingTaskExecution<'_> {
    fn drop(&mut self) {
        if let Some(poll) = self.poll.take() {
            self.worker.task_poll_finished(poll);
        }
    }
}

impl EventContext {
    pub(crate) const fn new(runtime: RuntimeId, worker: Option<WorkerId>, backtrace: BacktraceCapture) -> Self {
        Self {
            runtime,
            worker,
            backtrace,
        }
    }

    /// Assigns an identity and emits task creation only when recording is enabled.
    ///
    /// Returns `None` when the recorder does not run the event builder. Sampling
    /// or a concurrent policy change can still omit the event after assigning
    /// an identity. Keep that identity for native inspection and later outcomes.
    /// Measure `body_size` before runtime wrapping or type erasure.
    ///
    /// # Panics
    ///
    /// Panics if process task identities are exhausted or `body_size` cannot be
    /// encoded as a value smaller than `u64::MAX`.
    #[must_use]
    #[inline]
    pub fn spawned(&self, descriptor: TypeDescriptorId, parent: Option<TaskId>, body_size: usize) -> Option<TaskId> {
        self.spawned_with(|| descriptor, parent, body_size).map(|spawn| spawn.id)
    }

    /// Builds a task creation event and returns its identity and timestamp.
    ///
    /// Calls `descriptor` only when the recorder runs the event builder.
    /// Disabled or suppressed recording does not call it. Sampling and
    /// concurrent recording changes can still omit the event after it runs.
    /// Reuse the returned timestamp in native task metadata to avoid another
    /// clock read. Measure `body_size` before wrapping or type erasure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `descriptor`. Panics if process task identities
    /// are exhausted or `body_size` cannot be encoded below `u64::MAX`.
    #[must_use]
    pub fn spawned_with(
        &self,
        descriptor: impl FnOnce() -> TypeDescriptorId,
        parent: Option<TaskId>,
        body_size: usize,
    ) -> Option<TaskSpawn> {
        let mut assigned = None;
        seismograph::record(EventClass::RuntimeTask, || {
            let size = u64::try_from(body_size)
                .ok()
                .and_then(|size| size.checked_add(1))
                .expect("a concrete task body must be smaller than u64::MAX");
            let id = crate::allocate_task_id();
            let descriptor = descriptor();
            let timestamp = EventTimestamp::now();
            assigned = Some(TaskSpawn { id, descriptor, timestamp });
            Some(Record::runtime(
                timestamp,
                EventKind::TaskSpawned,
                RuntimeEvent {
                    runtime_id: self.runtime,
                    worker_id: self.worker,
                    subject_id: id.get(),
                    related_id: parent.map_or(0, TaskId::get),
                    value_0: descriptor.get(),
                    value_1: size,
                },
                self.backtrace,
            ))
        });
        assigned
    }

    /// Emits a real successful outcome without retaining task state.
    #[inline]
    pub fn completed(&self, task: TaskId) {
        self.outcome(task, EventKind::TaskCompleted, BacktraceCapture::Never);
    }

    /// Emits a real task panic with the configured backtrace policy.
    #[inline]
    pub fn panicked(&self, task: TaskId) {
        self.outcome(task, EventKind::TaskPanicked, self.backtrace);
    }

    /// Emits cancellation without retaining task state.
    #[inline]
    pub fn canceled(&self, task: TaskId) {
        self.outcome(task, EventKind::TaskCanceled, BacktraceCapture::Never);
    }

    fn outcome(&self, task: TaskId, kind: EventKind, backtrace: BacktraceCapture) {
        seismograph::record(EventClass::RuntimeTask, || {
            Some(Record::runtime(
                EventTimestamp::now(),
                kind,
                RuntimeEvent {
                    runtime_id: self.runtime,
                    worker_id: self.worker,
                    subject_id: task.get(),
                    related_id: 0,
                    value_0: 0,
                    value_1: 0,
                },
                backtrace,
            ))
        });
    }
}

/// An owned request that a provider moves to its worker and answers once.
///
/// Dropping an unanswered request reports collection failure and releases the
/// worker's request slot. Canceled requests do not send a response.
/// No references to native task storage can escape in the response.
#[derive(Debug)]
pub struct TaskRequest {
    pub(crate) worker: Arc<WorkerControl>,
    pub(crate) sender: mpsc::SyncSender<collection::Batch>,
    pub(crate) canceled: Arc<AtomicBool>,
    pub(crate) limit: usize,
    pub(crate) window: u64,
    pub(crate) answered: bool,
}

impl TaskRequest {
    /// Maximum native tasks to inspect, including tasks without descriptions.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Whether the diagnostic caller stopped waiting for this request.
    #[must_use]
    pub fn is_canceled(&self) -> bool {
        // Acquire observes the collector's Release when it stops receiving.
        self.canceled.load(Ordering::Acquire)
    }

    /// Sends owned descriptions without waiting for the diagnostic caller.
    ///
    /// Excess descriptions are truncated and marked incomplete. A canceled
    /// request discards its response. Measurement data comes from observed polls,
    /// not from values supplied by the provider.
    pub fn respond(mut self, mut tasks: Vec<Task>, mut collection: TaskCollection) {
        if self.is_canceled() {
            return;
        }
        if tasks.len() > self.limit {
            tasks.truncate(self.limit);
            collection = TaskCollection::Truncated;
        }
        let measurements = lock(&self.worker.measurements);
        for task in &mut tasks {
            task.last_worker_id = Some(self.worker.id);
            task.metrics = TaskMetrics::default();
            task.metrics_scope = crate::snapshot::TaskMetricsScope::Uncollected;
            if self.window != 0
                && measurements.window == self.window
                && let Some(measured) = measurements.tasks.get(&task.id)
            {
                task.metrics = measured.metrics;
                task.metrics_scope = crate::snapshot::TaskMetricsScope::PollsSince(measured.since);
            }
        }
        drop(measurements);
        self.answered = true;
        self.send(collection::Batch {
            worker: self.worker.id,
            tasks,
            collection,
        });
    }

    fn send(&self, batch: collection::Batch) {
        match self.sender.try_send(batch) {
            Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {}
            Err(mpsc::TrySendError::Full(_)) => unreachable!("one response per worker fits the collection channel"),
        }
    }
}

impl Drop for TaskRequest {
    fn drop(&mut self) {
        if !self.answered && !self.is_canceled() {
            self.send(collection::Batch {
                worker: self.worker.id,
                tasks: Vec::new(),
                collection: TaskCollection::Failed,
            });
        }
        // Release transfers this single pending slot to the next dispatch.
        self.worker.request_pending.store(false, Ordering::Release);
    }
}

/// Token pairing a poll observed by one worker.
///
/// Event-recording changes do not reset requested poll measurements.
/// Dropping an unfinished token clears its worker slot without publishing a duration.
#[derive(Debug)]
#[must_use = "finish the observed poll with WorkerHandle::task_poll_finished, or drop it to cancel observation"]
pub struct TaskPoll {
    pub(crate) task: TaskId,
    pub(crate) worker: Arc<WorkerControl>,
    pub(crate) started_at: Option<EventTimestamp>,
    pub(crate) window: u64,
    pub(crate) active: bool,
}

impl Drop for TaskPoll {
    fn drop(&mut self) {
        if self.active {
            let _previous = self
                .worker
                .current_task
                .compare_exchange(self.task.get(), 0, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}
