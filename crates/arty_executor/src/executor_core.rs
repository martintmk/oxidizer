// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use std::{mem, task};

use nm::{Event, Magnitude, MetricsPusher, Push};
use observed::Sink;
use plurality::MultiPool;
use tick::SimpleClock;

use crate::remote::Sender as RemoteSender;
use crate::wake::WakeShared;
use crate::{
    BuildPointerHasher, CycleOutcome, ERR_POISONED_LOCK, JoinEventLake, JoinHandle, Task, TaskId, TaskOptions, TaskRef, TypeErasedTask,
    WakeSignal,
};

/// The real implementation of the executor, shared by different public "client" API surfaces.
///
/// * `Executor` is the public API used by the owner of the executor, the one who creates it,
///   controls its operation and is responsible for its shutdown orchestration.
/// * `TaskSet` is the public API used by anyone on the same thread to add tasks to the executor.
///   It can be freely cloned and shared, with its useful lifetime bounded by the lifetime of
///   `Executor` (after `Executor` is dropped, new submissions return a shutdown join error).
#[derive(Debug)]
pub(crate) struct ExecutorCore {
    reentrancy_safe: RefCell<ReentrancySafeState>,
    exclusive: RefCell<ExclusiveState>,
    wake: Arc<WakeShared>,
    sink: Sink,

    _single_threaded: PhantomData<*const ()>,
}

/// This portion of the executor state is reentrancy-safe - during `execute_cycle()`, external code
/// may call back into the executor and call methods that depend on this state. Essentially, this is
/// the data set required to enqueue new tasks.
///
/// This entire struct is `RefCell`-guarded and borrowed for the duration of each individual
/// non-overlapping time span during which its contents may be used. In other words, whenever we are
/// calling into external code in `execute_cycle()` we must release the borrow.
#[derive(Debug)]
struct ReentrancySafeState {
    /// The new set contains all the tasks that have been created but not processed as part of
    /// any `execute_cycle()` call yet.
    ///
    /// This is a `VecDeque` because we do not require set characteristics and a deque is faster.
    new_tasks: VecDeque<TaskRef>,

    /// Storage for all the tasks registered with the executor. Once a task has been registered,
    /// the executor only sees it as a [`TypeErasedTask`][crate::TypeErasedTask], with only the
    /// task itself knowing the specific type of the future and the type of the result it produces.
    task_storage: MultiPool,

    tasks: HashMap<TaskId, TaskRef>,
    last_task_id: TaskId,

    /// Storage for result channels. Managed endpoints retain detached pool slots independently,
    /// allowing join handles to outlive the executor safely.
    join_events: JoinEventLake,

    /// In shutdown mode, all tasks are cancelled and retired. New submissions produce a
    /// shutdown join error without registering a task.
    shutting_down: bool,
}

/// This portion of the executor state is exclusive to its internal use - even if something calls
/// back into the executor during `execute_cycle()`, this state is never touched.
///
/// This entire struct is `RefCell`-guarded and borrowed for the duration of `execute_cycle()`.
#[derive(Debug)]
struct ExclusiveState {
    /// All the tasks we want to poll.
    ///
    /// This is a `VecDeque` because we do not require set characteristics and a deque is faster.
    active: VecDeque<TaskRef>,

    /// The inactive set contains all the tasks that we consider to be in a state where polling
    /// will not make any progress. We will move them back to the active set after a waker notifies
    /// us that a future needs to be polled again because the poll may do something useful now.
    ///
    /// Note that the wake-up may arrive during the time the task is being polled. This does not
    /// mean it gets polled multiple times per cycle, though - we explicitly poll every task at
    /// most once per cycle, to avoid getting stuck on a hyperactive task.
    ///
    /// We use a `PointerHasher` for efficient hashing because `TaskRef` hashes only its task pointer.
    inactive: HashSet<TaskRef, BuildPointerHasher>,

    /// Tasks that completed during this cycle and can be released after their independently pooled
    /// wake state has been retired.
    completed: VecDeque<TaskRef>,

    /// Used to measure the time spent during/between executor cycle processing and during shutdown.
    /// This is a very fast clock designed to be cheap to poll many times each millisecond (if we
    /// need to).
    clock: SimpleClock,

    // Used to report interval between cycles.
    last_cycle_ended: Option<Instant>,
}

#[expect(clippy::unused_self, reason = "semantically correct, even if not always necessary")]
impl ExecutorCore {
    /// Creates a new async task executor.
    ///
    /// The `owner_waker` is used by the executor to wake up its owner when more work has
    /// arrived for the executor and calling `execute_cycle()` is desirable.
    #[must_use]
    pub(crate) fn new(owner_waker: task::Waker, sink: Sink) -> Self {
        Self {
            reentrancy_safe: RefCell::new(ReentrancySafeState {
                new_tasks: VecDeque::new(),
                task_storage: MultiPool::new(),
                tasks: HashMap::new(),
                last_task_id: TaskId::default(),
                join_events: JoinEventLake::new(),
                shutting_down: false,
            }),
            exclusive: RefCell::new(ExclusiveState {
                active: VecDeque::new(),
                inactive: HashSet::with_hasher(BuildPointerHasher::default()),
                completed: VecDeque::new(),
                clock: SimpleClock::new_system().with_fast_instant(true),
                last_cycle_ended: None,
            }),
            wake: Arc::new(WakeShared::new(owner_waker)),
            sink,
            _single_threaded: PhantomData,
        }
    }

    pub(crate) fn add_task<F, R>(&self, future: F, options: TaskOptions) -> JoinHandle<R>
    where
        F: IntoFuture<Output = R> + 'static,
        R: 'static,
    {
        if let Some(join_handle) = self.rejected_submission() {
            return join_handle;
        }
        let future = future.into_future();
        self.register_task(
            move |join_events, sink| {
                let (result_tx, join_handle) = join_events.rent();
                (Task::new_with_options(future, result_tx, options, sink), join_handle)
            },
            JoinEventLake::disconnected,
        )
    }

    pub(crate) fn add_remote_task<F, R>(&self, future: F, options: TaskOptions, remote_result_tx: RemoteSender<R>)
    where
        F: IntoFuture<Output = R> + 'static,
        R: Send + 'static,
    {
        if self.reentrancy_safe.borrow().shutting_down {
            return;
        }
        let future = future.into_future();
        self.register_task(
            move |_join_events, sink| (Task::new_remote(future, options, sink, remote_result_tx), ()),
            |_join_events| (),
        );
    }

    fn rejected_submission<R: 'static>(&self) -> Option<JoinHandle<R>> {
        let state = self.reentrancy_safe.borrow();
        state.shutting_down.then(|| state.join_events.disconnected())
    }

    fn register_task<O, T>(&self, create: impl FnOnce(&JoinEventLake, Sink) -> (T, O), reject: impl FnOnce(&JoinEventLake) -> O) -> O
    where
        T: TypeErasedTask + 'static,
    {
        let (reporter, output) = {
            let mut state = self.reentrancy_safe.borrow_mut();
            if state.shutting_down {
                return reject(&state.join_events);
            }
            let (task, output) = create(&state.join_events, self.sink.clone());

            let task_id = state.last_task_id.next();
            let task_ref = TaskRef::new(state.task_storage.alloc_box(task), task_id);
            state.tasks.insert(task_id, task_ref);

            let wake_signal = WakeSignal::new(&self.wake, task_id);

            // SAFETY: The task is alive (we own it and just created it) and we are on the thread
            // where it was created (because we just created it). The executor is the only thing that
            // creates references to the tasks and it only ever creates temporary non-overlapping
            // references narrowly bounded to individual code blocks, ensuring that aliasing rules
            // are upheld. Anything outside `ExecutorCore` only passes `TaskRef` by value, never
            // dereferencing it. Reentrant logic for registering new tasks cannot touch existing tasks.
            let task = unsafe { task_ref.as_task() };

            task.initialize(wake_signal);
            let reporter = task.created_reporter();
            state.new_tasks.push_back(task_ref);
            (reporter, output)
        };

        // Telemetry processors are external code and may submit another task. The state borrow
        // must be released before this call.
        reporter.report();
        output
    }

    #[must_use]
    pub(crate) fn join_event_lake(&self) -> JoinEventLake {
        self.reentrancy_safe.borrow().join_events.clone()
    }

    #[must_use]
    pub(crate) fn has_tasks(&self) -> bool {
        !self.reentrancy_safe.borrow().task_storage.is_empty()
    }

    #[must_use]
    pub(crate) fn execute_cycle(&self) -> CycleOutcome {
        let mut state_exclusive = self.exclusive.borrow_mut();

        let cycle_start_timestamp = state_exclusive.clock.instant();

        if let Some(last_cycle_ended) = state_exclusive.last_cycle_ended {
            let cycle_gap = cycle_start_timestamp.saturating_duration_since(last_cycle_ended);
            CYCLE_GAP_MILLIS.with(|x| x.observe_millis(cycle_gap));
        }

        {
            let mut state_reentrant = self.reentrancy_safe.borrow_mut();

            self.accept_new_tasks(&mut state_exclusive, &mut state_reentrant);
            self.activate_awakened_tasks(&mut state_exclusive, &state_reentrant.tasks);
        }

        self.poll_active_tasks(&mut state_exclusive);

        if self.reentrancy_safe.borrow().shutting_down {
            let aborted_count = {
                let mut state_reentrant = self.reentrancy_safe.borrow_mut();
                Self::prepare_task_abort(&self.wake, &mut state_exclusive, &mut state_reentrant)
            };
            Self::abort_prepared_tasks(&state_exclusive, aborted_count);
        }

        {
            let mut state_reentrant = self.reentrancy_safe.borrow_mut();

            Self::drop_completed_tasks(&mut state_exclusive, &mut state_reentrant.tasks, true);

            let outcome = if self.evaluate_shutdown_completion(&state_exclusive, &state_reentrant) {
                CycleOutcome::Shutdown
            } else if self.has_work_to_do(&state_exclusive, &state_reentrant) {
                // We want to be immediately called again because we may have more work to do.
                CYCLE_OUTCOME_CONTINUE.with(Event::observe_once);
                CycleOutcome::Continue
            } else {
                // We have no work to do, feel free to take a while before coming back to us.
                // We will try to trigger the owner's waker if more work arrives, though this is
                // not guaranteed.
                CYCLE_OUTCOME_SUSPEND.with(Event::observe_once);
                CycleOutcome::Suspend
            };

            let cycle_end_timestamp = state_exclusive.clock.instant();

            let cycle_duration = cycle_end_timestamp.saturating_duration_since(cycle_start_timestamp);
            state_exclusive.last_cycle_ended = Some(cycle_end_timestamp);
            CYCLE_DURATION_MILLIS.with(|x| x.observe_millis(cycle_duration));

            // Publish metrics from this cycle.
            EXECUTOR_PUSHER.with(MetricsPusher::push);

            outcome
        }
    }

    /// Accepts new tasks that have been registered with the executor since the last call to this
    /// method.
    fn accept_new_tasks(&self, state_exclusive: &mut ExclusiveState, state_reentrant: &mut ReentrancySafeState) {
        if state_reentrant.new_tasks.is_empty() {
            // Nothing to do, no new tasks.
            return;
        }

        TASKS_ACCEPTED.with(|x| x.observe(state_reentrant.new_tasks.len()));

        // Every cycle drains the active queue. Swap its reusable buffer with the new-task queue
        // instead of copying each task, leaving a separate queue for reentrant additions.
        debug_assert!(
            state_exclusive.active.is_empty(),
            "active tasks are drained at the end of each cycle"
        );
        mem::swap(&mut state_exclusive.active, &mut state_reentrant.new_tasks);
    }

    fn activate_awakened_tasks(&self, state_exclusive: &mut ExclusiveState, tasks: &HashMap<TaskId, TaskRef>) {
        let mut awakened = self.wake.awakened.lock().expect(ERR_POISONED_LOCK);
        for task_id in awakened.drain(..) {
            let Some(&task_ref) = tasks.get(&task_id) else {
                TASKS_ACTIVATED_SPURIOUS.with(Event::observe_once);
                continue;
            };
            // SAFETY: The owner-thread map retains only live tasks. Entries are removed before
            // releasing task storage, and IDs are never reused.
            unsafe { task_ref.as_task() }.clear_queued_notification();
            if state_exclusive.inactive.remove(&task_ref) {
                state_exclusive.active.push_back(task_ref);
                TASKS_ACTIVATED_VIA_AWAKENED_SET.with(Event::observe_once);
            } else {
                TASKS_ACTIVATED_SPURIOUS.with(Event::observe_once);
            }
        }
    }

    fn poll_active_tasks(&self, state_exclusive: &mut ExclusiveState) {
        let completed_before = state_exclusive.completed.len();
        let inactive_before = state_exclusive.inactive.len();

        while let Some(task_ref) = state_exclusive.active.pop_front() {
            // SAFETY: The task is alive (we own it and just created it) and we are on the thread
            // where it was created (because we just created it). The executor is the only thing that
            // creates references to the tasks and it only ever creates temporary non-overlapping
            // references narrowly bounded to individual code blocks, ensuring that aliasing rules
            // are upheld. Anything outside `ExecutorCore` only passes `TaskRef` by value, never
            // dereferencing it. Reentrant logic for registering new tasks cannot touch existing tasks.
            let task = unsafe { task_ref.as_task() };

            if self.reentrancy_safe.borrow().shutting_down {
                task.abort();
                state_exclusive.completed.push_back(task_ref);
                continue;
            }

            match task.poll() {
                task::Poll::Ready(()) => {
                    // The task has completed and retired its independently pooled wake state, so it
                    // can move to the completed list for release at the end of this cycle.
                    state_exclusive.completed.push_back(task_ref);
                }
                task::Poll::Pending => {
                    // The task is still pending, so we move it to the inactive set.
                    // It will return to the active set once something jiggles its waker.
                    state_exclusive.inactive.insert(task_ref);
                }
            }
        }

        let completed_diff = state_exclusive
            .completed
            .len()
            .checked_sub(completed_before)
            .expect("completed tasks set cannot decrease when completing more tasks");
        let inactive_diff = state_exclusive
            .inactive
            .len()
            .checked_sub(inactive_before)
            .expect("inactive tasks set cannot decrease when moving more tasks to inactive state");

        self.record_poll_metrics(inactive_diff, completed_diff);
    }

    #[cfg_attr(test, mutants::skip)] // Just metrics reporting, not practical to test.
    fn record_poll_metrics(&self, inactive_diff: usize, completed_diff: usize) {
        if inactive_diff != 0 {
            TASKS_INACTIVATED.with(|x| x.observe(inactive_diff));
        }

        if completed_diff != 0 {
            TASKS_COMPLETED.with(|x| x.observe(completed_diff));
        }
    }

    #[cfg_attr(test, mutants::skip)] // If tasks are not dropped, executor will never shut down, leading to infinite loop.
    fn drop_completed_tasks(state_exclusive: &mut ExclusiveState, tasks: &mut HashMap<TaskId, TaskRef>, record_cycle_metric: bool) {
        let completed_before = state_exclusive.completed.len();

        // We drop all completed tasks after their completion/cancellation path has retired any
        // independently held wake metadata.
        for task_ref in state_exclusive.completed.drain(..) {
            assert!(tasks.remove(&task_ref.id()).is_some(), "completed tasks must still be registered");
            // SAFETY: Draining removes each completed task exactly once, after its wake metadata
            // has been retired by completion or cancellation.
            unsafe { task_ref.release() };
        }

        if record_cycle_metric && completed_before > 0 {
            TASKS_DROPPED.with(|x| x.observe(completed_before));
        }
    }

    /// Whether an immediately performed `execute_cycle()` would do any useful work.
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // Mutation can make the executor spin without useful work.
    fn has_work_to_do(&self, state_exclusive: &ExclusiveState, state_reentrant: &ReentrancySafeState) -> bool {
        // Work for us means there is a task that we can poll (either because it is new
        // or because it has been awakened).

        // As it stands at time of writing, there does not seem to be a way for a task to enter
        // the "active" list during an execution cycle, so this should never happen. We assert it
        // here just in case we missed something and it does happen, or starts to happen in the
        // future after some refactoring (as we would then need to react to it).
        debug_assert!(
            state_exclusive.active.is_empty(),
            "Active tasks should not be present at this point"
        );

        !state_reentrant.new_tasks.is_empty() || !self.wake.awakened.lock().expect(ERR_POISONED_LOCK).is_empty()
    }

    #[cfg_attr(test, mutants::skip)] // Mutation can lead to deadlocked executor as it never shuts down.
    pub(crate) fn begin_shutdown(&self) {
        {
            let mut state = self.reentrancy_safe.borrow_mut();
            assert!(
                !state.shutting_down,
                "Cannot start shutdown process when it has already been started"
            );
            state.shutting_down = true;
        }
        let mut state_exclusive = self.exclusive.borrow_mut();
        let mut state_reentrant = self.reentrancy_safe.borrow_mut();

        let aborted_count = Self::prepare_task_abort(&self.wake, &mut state_exclusive, &mut state_reentrant);
        drop(state_reentrant);
        Self::abort_prepared_tasks(&state_exclusive, aborted_count);
    }

    pub(crate) fn request_shutdown(&self) {
        self.reentrancy_safe.borrow_mut().shutting_down = true;
    }

    pub(crate) fn owner_dropped(&self) {
        self.request_shutdown();

        let Ok(mut state_exclusive) = self.exclusive.try_borrow_mut() else {
            // An active cycle observes `shutting_down` before it returns and performs cancellation.
            return;
        };
        let mut state_reentrant = self.reentrancy_safe.borrow_mut();
        let aborted_count = Self::prepare_task_abort(&self.wake, &mut state_exclusive, &mut state_reentrant);
        drop(state_reentrant);
        Self::abort_prepared_tasks(&state_exclusive, aborted_count);
        Self::drop_completed_tasks(&mut state_exclusive, &mut self.reentrancy_safe.borrow_mut().tasks, false);
        if EXECUTOR_PUSHER.try_with(MetricsPusher::push).is_err() {
            // Executor cleanup must remain safe after metrics TLS has been destroyed.
        }
    }

    fn prepare_task_abort(wake: &WakeShared, state_exclusive: &mut ExclusiveState, state_reentrant: &mut ReentrancySafeState) -> usize {
        let mut awakened = wake.awakened.lock().expect(ERR_POISONED_LOCK);
        awakened.clear();
        drop(awakened);

        // Needed for split borrowing.
        let state_exclusive_real: &mut ExclusiveState = state_exclusive;

        let new = &mut state_reentrant.new_tasks;
        let active = &mut state_exclusive_real.active;
        let inactive = &mut state_exclusive_real.inactive;
        let completed = &mut state_exclusive_real.completed;

        let aborted_count = new.len().saturating_add(active.len().saturating_add(inactive.len()));
        match EXECUTOR_PUSHER.try_with(|_pusher| TASKS_ABORTED_ON_SHUTDOWN.try_with(|event| event.batch(aborted_count).observe_once())) {
            Ok(Ok(())) => {}
            Ok(Err(_access_error)) | Err(_access_error) => {
                // Metrics TLS may already be destroyed when an executor itself lives in TLS.
            }
        }

        for task_ref in active.drain(..).chain(inactive.drain()).chain(new.drain(..)) {
            completed.push_back(task_ref);
        }

        aborted_count
    }

    fn abort_prepared_tasks(state_exclusive: &ExclusiveState, aborted_count: usize) {
        let abort_start = state_exclusive
            .completed
            .len()
            .checked_sub(aborted_count)
            .expect("prepared task count cannot exceed completed task count");
        for &task_ref in state_exclusive.completed.range(abort_start..) {
            // SAFETY: The task is alive (we own it and just created it) and we are on the thread
            // where it was created (because we just created it). The executor is the only thing that
            // creates references to the tasks and it only ever creates temporary non-overlapping
            // references narrowly bounded to individual code blocks, ensuring that aliasing rules
            // are upheld. Anything outside `ExecutorCore` only passes `TaskRef` by value, never
            // dereferencing it. Reentrant logic for registering new tasks cannot touch existing tasks.
            let task = unsafe { task_ref.as_task() };

            task.abort();
        }
    }

    /// Returns whether we are at the end of a successful shutdown process.
    #[cfg_attr(test, mutants::skip)] // Mutation can lead to deadlocked executor as it never shuts down.
    #[must_use]
    fn evaluate_shutdown_completion(&self, state_exclusive: &ExclusiveState, state_reentrant: &ReentrancySafeState) -> bool {
        if !state_reentrant.shutting_down {
            // We are not in a shutdown process.
            return false;
        }

        // During shutdown, tasks can only exist in the "completed" state.
        debug_assert!(state_exclusive.active.is_empty());
        debug_assert!(state_reentrant.new_tasks.is_empty());
        debug_assert!(state_exclusive.inactive.is_empty());

        state_exclusive.completed.is_empty() && state_reentrant.task_storage.is_empty()
    }
}

#[cfg_attr(test, mutants::skip)] // Executor::drop tests the same cleanup through owner_dropped().
impl Drop for ExecutorCore {
    fn drop(&mut self) {
        let state_exclusive = self.exclusive.get_mut();
        let state_reentrant = self.reentrancy_safe.get_mut();

        state_reentrant.shutting_down = true;
        let aborted_count = Self::prepare_task_abort(&self.wake, state_exclusive, state_reentrant);
        Self::abort_prepared_tasks(state_exclusive, aborted_count);

        Self::drop_completed_tasks(state_exclusive, &mut state_reentrant.tasks, false);
        match EXECUTOR_PUSHER.try_with(MetricsPusher::push) {
            Ok(()) => {}
            Err(_access_error) => {
                // Executor cleanup must remain safe after metrics TLS has been destroyed.
            }
        }
        debug_assert!(state_reentrant.task_storage.is_empty());
    }
}

const MILLIS_BUCKETS: &[Magnitude] = &[0, 1, 20, 50, 100, 1_000, 10_000];
const TASKS_TOUCHED_PER_CYCLE_BUCKETS: &[Magnitude] = &[0, 1, 10, 100, 1_000];
const TASKS_MANAGED_PER_CYCLE_BUCKETS: &[Magnitude] = &[0, 1, 10, 100, 1_000];

thread_local! {
    static EXECUTOR_PUSHER: MetricsPusher = MetricsPusher::new();

    static TASKS_ABORTED_ON_SHUTDOWN: Event<Push> = Event::builder()
        .name("ae_tasks_aborted_on_shutdown")
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static TASKS_ACCEPTED: Event<Push> = Event::builder()
        .name("ae_tasks_accepted_per_cycle")
        .histogram(TASKS_MANAGED_PER_CYCLE_BUCKETS)
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static TASKS_COMPLETED: Event<Push> = Event::builder()
        .name("ae_tasks_completed_per_cycle")
        .histogram(TASKS_TOUCHED_PER_CYCLE_BUCKETS)
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static TASKS_INACTIVATED: Event<Push> = Event::builder()
        .name("ae_tasks_inactivated_per_cycle")
        .histogram(TASKS_TOUCHED_PER_CYCLE_BUCKETS)
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static TASKS_DROPPED: Event<Push> = Event::builder()
        .name("ae_tasks_dropped_per_cycle")
        .histogram(TASKS_MANAGED_PER_CYCLE_BUCKETS)
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static TASKS_ACTIVATED_VIA_AWAKENED_SET: Event<Push> = Event::builder()
        .name("ae_tasks_activated_via_awakened_set")
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static TASKS_ACTIVATED_SPURIOUS: Event<Push> = Event::builder()
        .name("ae_tasks_activated_spurious")
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static CYCLE_DURATION_MILLIS: Event<Push> = Event::builder()
        .name("ae_cycle_duration_millis")
        .histogram(MILLIS_BUCKETS)
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static CYCLE_GAP_MILLIS: Event<Push> = Event::builder()
        .name("ae_cycle_gap_millis")
        .histogram(MILLIS_BUCKETS)
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static CYCLE_OUTCOME_CONTINUE: Event<Push> = Event::builder()
        .name("ae_cycle_outcome_continue")
        .pusher_local(&EXECUTOR_PUSHER)
        .build();

    static CYCLE_OUTCOME_SUSPEND: Event<Push> = Event::builder()
        .name("ae_cycle_outcome_suspend")
        .pusher_local(&EXECUTOR_PUSHER)
        .build();
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Poll;

    use static_assertions::assert_not_impl_any;

    use super::*;

    #[test]
    fn thread_safety() {
        assert_not_impl_any!(ExecutorCore: Send, Sync);
    }

    #[test]
    fn stale_wake_identity_cannot_activate_a_replacement_task() {
        #[expect(clippy::future_not_send, reason = "the executor intentionally supports thread-local futures")]
        fn counting_task(polls: Rc<Cell<usize>>, ready: bool) -> impl Future<Output = ()> {
            std::future::poll_fn(move |_| {
                polls.set(polls.get() + 1);
                if ready { Poll::Ready(()) } else { Poll::Pending }
            })
        }

        let core = ExecutorCore::new(task::Waker::noop().clone(), Sink::noop());
        let polls = Rc::new(Cell::new(0));
        let first_handle = core.add_task(counting_task(Rc::clone(&polls), true), TaskOptions::default());
        let first = *core.reentrancy_safe.borrow().new_tasks.front().unwrap();
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);

        let second_handle = core.add_task(counting_task(Rc::clone(&polls), false), TaskOptions::default());
        let second = *core.reentrancy_safe.borrow().new_tasks.front().unwrap();
        assert_eq!(first, second, "the pool reuses the completed task's storage");
        assert_ne!(first.id(), second.id());
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);
        assert_eq!(polls.get(), 2);

        core.wake.awakened.lock().unwrap().extend([first.id(), first.id()]);
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);
        assert_eq!(polls.get(), 2);

        core.wake.awakened.lock().unwrap().extend([first.id(), second.id(), second.id()]);
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);
        assert_eq!(polls.get(), 3);

        core.begin_shutdown();
        assert_eq!(core.execute_cycle(), CycleOutcome::Shutdown);
        drop((first_handle, second_handle));
    }

    #[test]
    fn shutdown_ignores_stale_wake_identities() {
        let core = ExecutorCore::new(task::Waker::noop().clone(), Sink::noop());
        let completed = core.add_task(async {}, TaskOptions::default());
        let stale_id = core.reentrancy_safe.borrow().new_tasks.front().unwrap().id();
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);

        let pending = core.add_task(std::future::pending::<()>(), TaskOptions::default());
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);
        core.wake.awakened.lock().unwrap().push_back(stale_id);

        core.begin_shutdown();
        assert!(core.wake.awakened.lock().unwrap().is_empty());
        assert_eq!(core.execute_cycle(), CycleOutcome::Shutdown);
        drop((completed, pending));
    }

    #[test]
    fn wake_after_activation_is_preserved_for_the_next_cycle() {
        let core = ExecutorCore::new(task::Waker::noop().clone(), Sink::noop());
        let captured = Rc::new(Cell::new(None));
        let polls = Rc::new(Cell::new(0));
        let handle = core.add_task(
            std::future::poll_fn({
                let captured = Rc::clone(&captured);
                let polls = Rc::clone(&polls);
                move |cx| {
                    polls.set(polls.get() + 1);
                    captured.set(Some(cx.waker().clone()));
                    Poll::<()>::Pending
                }
            }),
            TaskOptions::default(),
        );
        assert_eq!(core.execute_cycle(), CycleOutcome::Suspend);
        let waker = captured.take().unwrap();
        waker.wake_by_ref();

        let mut state = core.exclusive.borrow_mut();
        core.activate_awakened_tasks(&mut state, &core.reentrancy_safe.borrow().tasks);
        std::thread::spawn(move || waker.wake()).join().unwrap();

        core.poll_active_tasks(&mut state);

        assert!(core.has_work_to_do(&state, &core.reentrancy_safe.borrow()));
        core.activate_awakened_tasks(&mut state, &core.reentrancy_safe.borrow().tasks);
        core.poll_active_tasks(&mut state);
        assert_eq!(polls.get(), 3);
        drop(state);
        drop(handle);
        core.begin_shutdown();
        assert_eq!(core.execute_cycle(), CycleOutcome::Shutdown);
    }
}
