// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::VecDeque;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{self, AtomicBool, AtomicPtr, AtomicUsize};
use std::sync::{Arc, Mutex, Weak};
use std::task::{RawWaker, RawWakerVTable, Waker};

use pin_project::{pin_project, pinned_drop};
use plurality::{Box as PoolBox, Pool};

use crate::TaskId;

const MAX_WAKER_COUNT: usize = usize::MAX / 2;

pub(crate) type AwakenedQueue = Arc<Mutex<VecDeque<TaskId>>>;

#[derive(Debug)]
pub(crate) struct WakeShared {
    pub(crate) awakened: AwakenedQueue,
    pub(crate) probe_embedded_wake_signals: Arc<AtomicBool>,
    parent_waker: Waker,
    waker_states: Mutex<Pool<WakerState>>,
}

impl WakeShared {
    pub(crate) fn new(awakened: AwakenedQueue, probe_embedded_wake_signals: Arc<AtomicBool>, parent_waker: Waker) -> Self {
        Self {
            awakened,
            probe_embedded_wake_signals,
            parent_waker,
            waker_states: Mutex::new(Pool::new()),
        }
    }
}

/// Task-owned access to independently pooled wake metadata.
///
/// The wake signal supplies the waker that can be used to set the signal and enables the owner
/// to inspect the awakened state. It also integrates with the executor's shared state to signal
/// the awakened state directly to the executor, eliminating the requirement to access the
/// task to detect that it has awakened.
///
/// # Ownership
///
/// The type uses interior mutability because its state is accessed concurrently from multiple
/// threads - by the executor (and/or task) that owns it on one hand, and by any number of
/// awaited async futures on any number of threads on the other hand (via wakers).
///
/// The borrowed polling waker is scoped to the poll. Cloning it retains only the pooled
/// [`WakerState`], not task storage. Completion and cancellation retire that state before the
/// task is released, so retained wakers remain valid across shutdown but become inert no-ops.
///
/// # Thread safety
///
/// The type itself is single-threaded, although the `std::task::Waker` instances obtained
/// from it are thread-safe as required by the waker API contract.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub(crate) struct WakeSignal {
    task_id: TaskId,
    shared: Arc<WakeShared>,
    state: AtomicPtr<WakerState>,
    awakened: AtomicBool,
    detached_unreported: AtomicBool,

    /// The type is single threaded... as far as other modules are concerned.
    ///
    /// We are secretly thread-safe internally but that is just a side-effect of
    /// the same type also masquerading as zero or more thread-safe `Waker` instances.
    ///
    /// When we act under the personality of a `Waker` within this module, Rust does not
    /// see the cross-thread access so will not complain about this marker.
    _single_threaded: PhantomData<*const ()>,

    /// This type cannot be unpinned once it has been pinned (latest when calling `waker_ref()`).
    _requires_pin: PhantomPinned,
}

impl WakeSignal {
    pub(crate) fn new(shared: Arc<WakeShared>, task_id: TaskId) -> Self {
        Self {
            task_id,
            shared,
            state: AtomicPtr::new(std::ptr::null_mut()),
            awakened: AtomicBool::new(false),
            detached_unreported: AtomicBool::new(false),
            _single_threaded: PhantomData,
            _requires_pin: PhantomPinned,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_parts(
        awakened_queue: AwakenedQueue,
        probe_embedded_wake_signals: Arc<AtomicBool>,
        parent_waker: Waker,
        task_id: TaskId,
    ) -> Self {
        let shared = Arc::new(WakeShared::new(awakened_queue, probe_embedded_wake_signals, parent_waker));
        Self::new(shared, task_id)
    }

    /// Creates a fake wake signal that is not connected to an executor. This can be useful
    /// for testing, where you need a wake signal but do not care about what it actually does.
    #[cfg(test)]
    pub(crate) fn fake() -> Self {
        Self::new_with_parts(
            Arc::new(Mutex::new(VecDeque::new())),
            Arc::new(AtomicBool::new(false)),
            Waker::noop().clone(),
            TaskId::fake(),
        )
    }

    fn state(&self) -> &WakerState {
        let mut state = self.state.load(atomic::Ordering::Acquire);
        if state.is_null() {
            state = self.install_state(self.allocate_state());
        }
        // SAFETY: The atomic pointer owns one intrusive reference until `WakeSignal::drop`.
        unsafe { state.as_ref() }.expect("initialized wake state pointer is never null")
    }

    fn allocate_state(&self) -> WakerStateRef {
        let value = WakerState::new(Arc::downgrade(&self.shared), self.task_id);
        let candidate = self
            .shared
            .waker_states
            .lock()
            .expect("waker state pool is never held while invoking user code")
            .alloc_box(value);
        WakerStateRef::from_pool_box(candidate)
    }

    fn install_state(&self, candidate: WakerStateRef) -> *mut WakerState {
        let candidate_ptr = candidate.as_ptr().cast_mut();
        // Release publishes the fully initialized pooled state to Acquire readers. On failure,
        // Acquire observes the state installed by the winning contender before its pointer is used.
        match self.state.compare_exchange(
            std::ptr::null_mut(),
            candidate_ptr,
            atomic::Ordering::AcqRel,
            atomic::Ordering::Acquire,
        ) {
            Ok(_) => {
                let installed = candidate.into_raw().as_ptr();
                self.detached_unreported.store(true, atomic::Ordering::Release);
                installed
            }
            Err(existing) => {
                drop(candidate);
                existing
            }
        }
    }

    fn state_if_initialized(&self) -> Option<&WakerState> {
        // SAFETY: A non-null atomic pointer owns one intrusive reference until `WakeSignal::drop`.
        unsafe { self.state.load(atomic::Ordering::Acquire).as_ref() }
    }

    fn state_ptr(&self) -> *mut WakerState {
        _ = self.state();
        self.state.load(atomic::Ordering::Acquire)
    }

    pub(crate) fn retire(&self) {
        let state = self.state.load(atomic::Ordering::Acquire);
        // SAFETY: A non-null atomic pointer owns one intrusive reference until `WakeSignal::drop`.
        if let Some(state) = unsafe { state.as_ref() } {
            state.retire();
        }
    }

    /// Returns whether the signal has received a wake-up notification.
    ///
    /// If it has, resets the signal to a not-awakened state.
    pub(crate) fn consume_awakened(&self) -> bool {
        // Most of the time, the flag will be false so we at first probe it with Relaxed ordering.
        // If it is false, we can return early. If it is true, we need to ensure that we see all
        // memory operations that happened before the flag was set (i.e. the state changes that
        // led to the task being awakened). An Acquire fence sequenced after the relaxed RMW that
        // observes the Release store establishes the required synchronization.
        let inline = self.awakened.swap(false, atomic::Ordering::Relaxed);
        let independent = self.state_if_initialized().is_some_and(WakerState::consume_awakened);
        if inline || independent {
            atomic::fence(atomic::Ordering::Acquire);
            true
        } else {
            false
        }
    }

    pub(crate) fn is_awakened(&self) -> bool {
        self.awakened.load(atomic::Ordering::Acquire) || self.state_if_initialized().is_some_and(WakerState::is_awakened)
    }

    pub(crate) fn take_detached_unreported(&self) -> bool {
        self.detached_unreported.swap(false, atomic::Ordering::Acquire)
    }

    /// Borrows a waker associated with this signal without incrementing its reference count.
    ///
    /// # Safety
    ///
    /// The signal must remain pinned for the returned borrow. Any clones own independent pooled
    /// state and do not extend this borrow.
    pub(crate) unsafe fn waker_ref(self: Pin<&Self>) -> impl Deref<Target = Waker> + '_ {
        let signal_ptr: *const Self = self.get_ref();

        // Like a borrowed Arc waker, this borrow keeps the signal alive without an owned count.
        // Only Waker::clone creates an owned reference. Hide ManuallyDrop behind Deref so callers
        // cannot extract or consume the uncounted Waker; its lifetime stays tied to this borrow.
        // SAFETY: We are required to correctly implement the waker API contract, which we do.
        // This includes being thread-safe, etc. The `WakeSignal` is thread-safe and all the
        // methods use interior mutability, so we are only using shared references, thereby
        // ensuring we do not violate Rust aliasing rules (as long as the owner of the `WakeSignal`
        // does not create any mutable references - though given that all methods are `&self` that
        // might still be relatively harmless and anyway likely detected by Miri as a bug.
        ManuallyDrop::new(unsafe { Waker::from_raw(RawWaker::new(signal_ptr.cast(), &BORROWED_WAKER_VTABLE)) })
    }

    /// Creates an owned waker for tests of the reference-counting protocol.
    ///
    /// # Safety
    ///
    /// The signal must remain pinned until this call returns.
    #[cfg(test)]
    pub(crate) unsafe fn waker(self: Pin<&Self>) -> Waker {
        // SAFETY: The caller keeps the signal alive until all owned wakers are released.
        unsafe { self.waker_ref() }.clone()
    }

    #[cfg(test)]
    fn external_waker_count(&self) -> usize {
        self.state_if_initialized().map_or(0, WakerState::external_waker_count)
    }

    fn wake_inline(&self) {
        if enqueue_wake(&self.shared.awakened, self.task_id, &self.shared.parent_waker, || true) {
            return;
        }
        self.awakened.store(true, atomic::Ordering::Release);
        self.shared.probe_embedded_wake_signals.store(true, atomic::Ordering::Release);
        self.shared.parent_waker.wake_by_ref();
    }
}

#[pinned_drop]
impl PinnedDrop for WakeSignal {
    fn drop(self: Pin<&mut Self>) {
        self.retire();
        let state = self.state.swap(std::ptr::null_mut(), atomic::Ordering::AcqRel);
        if let Some(state) = NonNull::new(state) {
            // SAFETY: The atomic pointer owns exactly one intrusive reference.
            drop(unsafe { WakerStateRef::from_non_null(state) });
        }
    }
}

pub(crate) struct WakerState {
    task_id: TaskId,
    shared: Weak<WakeShared>,
    ref_count: AtomicUsize,
    awakened: AtomicBool,
    active: AtomicBool,
}

impl WakerState {
    pub(crate) fn new(shared: Weak<WakeShared>, task_id: TaskId) -> Self {
        Self {
            task_id,
            shared,
            ref_count: AtomicUsize::new(1),
            awakened: AtomicBool::new(false),
            active: AtomicBool::new(true),
        }
    }

    fn wake(&self) {
        if !self.active.load(atomic::Ordering::Acquire) {
            return;
        }

        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        if enqueue_wake(&shared.awakened, self.task_id, &shared.parent_waker, || {
            self.active.load(atomic::Ordering::Acquire)
        }) {
            return;
        }

        if !self.active.load(atomic::Ordering::Acquire) {
            return;
        }
        self.awakened.store(true, atomic::Ordering::Release);
        shared.probe_embedded_wake_signals.store(true, atomic::Ordering::Release);
        shared.parent_waker.wake_by_ref();
    }

    fn retire(&self) {
        // Stale queued IDs are safe lookup misses, so retirement only needs to make future wakes
        // inert. A wake already past this check may still enqueue the retired ID.
        self.active.store(false, atomic::Ordering::Release);
    }

    fn consume_awakened(&self) -> bool {
        self.awakened.swap(false, atomic::Ordering::Relaxed)
    }

    fn is_awakened(&self) -> bool {
        self.awakened.load(atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    fn external_waker_count(&self) -> usize {
        self.ref_count.load(atomic::Ordering::Acquire) - 1
    }
}

impl std::fmt::Debug for WakerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("active", &self.active.load(atomic::Ordering::Relaxed))
            .field("ref_count", &self.ref_count.load(atomic::Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

fn enqueue_wake(queue: &AwakenedQueue, task_id: TaskId, parent_waker: &Waker, is_active: impl FnOnce() -> bool) -> bool {
    let Ok(mut queue) = queue.try_lock() else {
        return false;
    };
    if !is_active() {
        return true;
    }
    if queue.len() >= queue.capacity() {
        return false;
    }
    queue.push_back(task_id);
    drop(queue);
    parent_waker.wake_by_ref();
    true
}

struct WakerStateRef(NonNull<WakerState>);

// SAFETY: The pointee is `Send + Sync`, uses an atomic reference count, and remains allocated
// until the final reference returns its slot to the pool.
unsafe impl Send for WakerStateRef {}
// SAFETY: Same invariant as `Send`.
unsafe impl Sync for WakerStateRef {}

impl WakerStateRef {
    fn from_pool_box(state: PoolBox<WakerState>) -> Self {
        Self(PoolBox::into_raw(state))
    }

    unsafe fn from_raw(ptr: *const ()) -> Self {
        Self(NonNull::new(ptr.cast_mut().cast()).expect("raw waker state pointer is never null"))
    }

    unsafe fn from_non_null(ptr: NonNull<WakerState>) -> Self {
        Self(ptr)
    }

    fn as_ptr(&self) -> *const WakerState {
        self.0.as_ptr()
    }

    fn into_raw(self) -> NonNull<WakerState> {
        let ptr = self.0;
        std::mem::forget(self);
        ptr
    }
}

impl Deref for WakerStateRef {
    type Target = WakerState;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This reference contributes to the intrusive count, so the pooled value remains
        // alive for the returned borrow.
        unsafe { self.0.as_ref() }
    }
}

impl Drop for WakerStateRef {
    fn drop(&mut self) {
        // Each owner publishes its prior accesses with Release. The final owner acquires those
        // publications before reclaiming the pooled slot.
        if self.ref_count.fetch_sub(1, atomic::Ordering::Release) != 1 {
            return;
        }
        atomic::fence(atomic::Ordering::Acquire);
        // SAFETY: The count reached zero, so this is the unique final reconstruction of the exact
        // pointer produced by `PoolBox::into_raw`.
        drop(unsafe { PoolBox::<WakerState>::from_raw(self.0) });
    }
}

impl std::fmt::Debug for WakerStateRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

static BORROWED_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(borrowed_clone, borrowed_consume, borrowed_wake_by_ref, borrowed_drop);
static OWNED_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(waker_clone_waker, waker_wake, waker_wake_by_ref, waker_drop_waker);

fn borrowed_clone(ptr: *const ()) -> RawWaker {
    let signal = resurrect_signal_ref(ptr);
    let state_ptr = signal.state_ptr();
    // SAFETY: `state_ptr` owns one intrusive reference until `WakeSignal::drop`.
    let state = unsafe { state_ptr.as_ref() }.expect("initialized wake state pointer is never null");
    increment_reference_count(&state.ref_count, std::process::abort);
    RawWaker::new(state_ptr.cast_const().cast(), &OWNED_WAKER_VTABLE)
}

fn borrowed_wake_by_ref(ptr: *const ()) {
    resurrect_signal_ref(ptr).wake_inline();
}

#[cfg_attr(coverage_nightly, coverage(off))] // Unreachable through safe public Waker APIs.
#[cfg_attr(test, mutants::skip)] // Safe Waker APIs never consume the borrowed polling waker.
fn borrowed_consume(_: *const ()) {
    unreachable!("a borrowed polling waker cannot be consumed");
}

#[cfg_attr(coverage_nightly, coverage(off))] // Unreachable through safe public Waker APIs.
#[cfg_attr(test, mutants::skip)] // Safe Waker APIs never drop the borrowed polling waker.
fn borrowed_drop(_: *const ()) {
    unreachable!("a borrowed polling waker cannot be dropped");
}

fn waker_clone_waker(ptr: *const ()) -> RawWaker {
    let state = resurrect_state_ref(ptr);
    increment_reference_count(&state.ref_count, std::process::abort);

    RawWaker::new(ptr, &OWNED_WAKER_VTABLE)
}

fn increment_reference_count(ref_count: &AtomicUsize, on_overflow: fn() -> !) {
    // Cloning only reserves another owner; it does not publish or consume state, so Relaxed is
    // sufficient. Final Release/Acquire reclamation synchronizes prior owners before reuse.
    let previous = ref_count.fetch_add(1, atomic::Ordering::Relaxed);
    if previous >= MAX_WAKER_COUNT {
        on_overflow();
    }
}

#[cfg_attr(test, mutants::skip)] // If tasks do not wake up, tests tend to infinite loop.
fn waker_wake(ptr: *const ()) {
    // SAFETY: Consuming wake transfers this raw waker's counted ownership into the guard. Its
    // destructor releases that ownership even if the configured owner waker panics.
    let state = unsafe { WakerStateRef::from_raw(ptr) };
    state.wake();
}

#[cfg_attr(test, mutants::skip)] // If tasks do not wake up, tests tend to infinite loop.
fn waker_wake_by_ref(ptr: *const ()) {
    resurrect_state_ref(ptr).wake();
}

#[cfg_attr(test, mutants::skip)] // It's well tested, but causes ocasional test timeouts
fn waker_drop_waker(ptr: *const ()) {
    // SAFETY: Dropping transfers this raw waker's counted ownership into the guard.
    drop(unsafe { WakerStateRef::from_raw(ptr) });
}

/// Resurrects the pooled state reference that hides behind the waker's state pointer.
///
/// We return it with `'static` because there is no Rust lifetime that corresponds to
/// the waker reference's real lifetime. Just do not use it after the waker vtable methods.
#[cfg_attr(coverage_nightly, coverage(off))] // A null pointer would violate this module's RawWaker invariant.
fn resurrect_state_ref(ptr: *const ()) -> &'static WakerState {
    // SAFETY: Every raw waker pointer comes from a live pooled state retained by either the task
    // owner or the raw waker's own counted reference.
    let state = unsafe { ptr.cast::<WakerState>().as_ref() };

    let Some(state) = state else {
        unreachable!("waker has a null pointer for its inner state - impossible")
    };

    state
}

fn resurrect_signal_ref(ptr: *const ()) -> &'static WakeSignal {
    // SAFETY: Borrowed polling wakers exist only for the poll scope while their pinned signal
    // remains alive.
    let signal = unsafe { ptr.cast::<WakeSignal>().as_ref() };

    let Some(signal) = signal else {
        unreachable!("borrowed waker has a null pointer for its wake signal - impossible")
    };

    signal
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::Barrier;
    use std::thread;

    use super::*;
    use crate::testing::TestWaker;

    #[test]
    fn never_used() {
        let fake_task_id = TaskId::fake();

        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(10)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));

        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Waker::noop().clone(),
            fake_task_id
        ));

        assert!(signal.state_if_initialized().is_none());
    }

    #[test]
    fn borrowed_waker_reference_count() {
        let signal = pin!(WakeSignal::fake());
        let signal = signal.as_ref();

        {
            // SAFETY: The signal stays pinned until the borrowed waker and its clone are dropped.
            let waker = unsafe { signal.waker_ref() };
            assert_eq!(signal.external_waker_count(), 0);

            waker.wake_by_ref();
            assert!(signal.consume_awakened());
            assert_eq!(signal.external_waker_count(), 0);

            let clone = waker.clone();
            assert_eq!(signal.external_waker_count(), 1);
            drop(clone);
            assert_eq!(signal.external_waker_count(), 0);
        }

        assert_eq!(signal.external_waker_count(), 0);
    }

    #[test]
    fn borrowed_wake_uses_inline_queue_without_allocating_state() {
        let fake_task_id = TaskId::fake();
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(1)));
        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::new(AtomicBool::new(false)),
            Waker::noop().clone(),
            fake_task_id
        ));
        let signal = signal.as_ref();

        // SAFETY: The signal remains pinned for the borrowed wake.
        unsafe { signal.waker_ref() }.wake_by_ref();

        assert!(signal.state_if_initialized().is_none());
        assert_eq!(awakened_queue.lock().unwrap().len(), 1);
    }

    #[test]
    fn duplicate_state_installation_releases_the_unused_candidate() {
        let signal = pin!(WakeSignal::fake());
        let signal = signal.as_ref();
        let installed = std::ptr::from_ref(signal.state()).cast_mut();

        let candidate = signal.allocate_state();
        assert!(format!("{candidate:?}").contains(std::any::type_name::<WakerState>()));
        assert_eq!(signal.install_state(candidate), installed);
    }

    #[test]
    fn retirement_leaves_stale_ids_safe_and_makes_owned_wakers_inert() {
        let fake_task_id = TaskId::fake();
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(1)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));
        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Waker::noop().clone(),
            fake_task_id
        ));
        let signal = signal.as_ref();
        // SAFETY: The signal remains pinned through retirement.
        let waker = unsafe { signal.waker() };

        waker.wake_by_ref();
        assert_eq!(awakened_queue.lock().unwrap().len(), 1);

        signal.retire();
        assert_eq!(awakened_queue.lock().unwrap().len(), 1);
        waker.wake_by_ref();
        assert_eq!(awakened_queue.lock().unwrap().len(), 1);
        assert!(!probe_embedded_wake_signals.load(atomic::Ordering::Relaxed));
    }

    #[test]
    fn retirement_without_state_does_not_touch_stale_notifications() {
        let fake_task_id = TaskId::fake();
        let awakened_queue = Arc::new(Mutex::new(VecDeque::from([fake_task_id])));
        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::new(AtomicBool::new(false)),
            Waker::noop().clone(),
            fake_task_id,
        ));

        signal.retire();

        assert_eq!(awakened_queue.lock().unwrap().as_slices().0, [fake_task_id]);
    }

    #[test]
    fn waker_state_retirement_tolerates_dropped_shared_state() {
        let state = WakerState::new(Weak::new(), TaskId::fake());

        state.retire();

        assert!(!state.active.load(atomic::Ordering::Acquire));
    }

    #[test]
    fn retirement_does_not_wait_for_an_in_progress_enqueue() {
        let fake_task_id = TaskId::fake();
        let shared = Arc::new(WakeShared::new(
            Arc::new(Mutex::new(VecDeque::with_capacity(1))),
            Arc::new(AtomicBool::new(false)),
            Waker::noop().clone(),
        ));
        let state = WakerState::new(Arc::downgrade(&shared), fake_task_id);
        let active_checked = Barrier::new(2);
        let resume_enqueue = Barrier::new(2);

        thread::scope(|scope| {
            let wake_state = &state;
            let wake_task_id = fake_task_id;
            let active_checked = &active_checked;
            let resume_enqueue = &resume_enqueue;
            let wake = scope.spawn({
                let shared = Arc::clone(&shared);
                move || {
                    enqueue_wake(&shared.awakened, wake_task_id, Waker::noop(), || {
                        let active = wake_state.active.load(atomic::Ordering::Acquire);
                        active_checked.wait();
                        resume_enqueue.wait();
                        active
                    })
                }
            });

            active_checked.wait();
            state.retire();
            assert!(!state.active.load(atomic::Ordering::Acquire));
            resume_enqueue.wait();

            assert!(wake.join().unwrap());
        });

        assert_eq!(shared.awakened.lock().unwrap().as_slices().0, [fake_task_id]);
    }

    #[test]
    fn dropping_an_active_signal_retires_retained_wakers() {
        let fake_task_id = TaskId::fake();
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(1)));
        let shared = Arc::new(WakeShared {
            awakened: Arc::clone(&awakened_queue),
            probe_embedded_wake_signals: Arc::new(AtomicBool::new(false)),
            parent_waker: Waker::noop().clone(),
            waker_states: Mutex::new(Pool::builder().chunk_size(1).max_chunks(1).build()),
        });
        let retained = {
            let signal = pin!(WakeSignal::new(Arc::clone(&shared), fake_task_id));
            // SAFETY: The signal remains pinned until the clone owns independent state.
            unsafe { signal.as_ref().waker() }
        };

        retained.wake_by_ref();
        assert!(awakened_queue.lock().unwrap().is_empty());
        drop(retained);

        let state = WakerState::new(Arc::downgrade(&shared), fake_task_id);
        drop(shared.waker_states.lock().unwrap().try_alloc_box(state).unwrap());
    }

    #[test]
    fn final_state_reference_returns_its_pool_slot() {
        let pool = Pool::builder().chunk_size(1).max_chunks(1).build();
        let shared = Arc::new(WakeShared::new(
            Arc::new(Mutex::new(VecDeque::new())),
            Arc::new(AtomicBool::new(false)),
            Waker::noop().clone(),
        ));
        let fake_task_id = TaskId::fake();
        let state = || WakerState::new(Arc::downgrade(&shared), fake_task_id);

        drop(WakerStateRef::from_pool_box(pool.alloc_box(state())));

        drop(pool.try_alloc_box(state()).unwrap());
    }

    #[test]
    fn wake_state_debug_reports_lifecycle_counts() {
        let signal = pin!(WakeSignal::fake());
        let debug = format!("{:?}", signal.state());

        assert!(debug.contains("active"));
        assert!(debug.contains("ref_count"));
    }

    #[test]
    fn inactive_notification_is_not_queued() {
        let fake_task_id = TaskId::fake();
        let queue = Arc::new(Mutex::new(VecDeque::with_capacity(1)));

        assert!(enqueue_wake(&queue, fake_task_id, Waker::noop(), || false));
        assert!(queue.lock().unwrap().is_empty());
    }

    #[test]
    fn reference_count_overflow_is_rejected() {
        let count = AtomicUsize::new(MAX_WAKER_COUNT);

        testing_aids::assert_panic!(increment_reference_count(&count, || panic!("reference count overflow")));

        assert_eq!(count.load(atomic::Ordering::Relaxed), MAX_WAKER_COUNT + 1);
    }

    #[test]
    fn null_borrowed_signal_pointer_is_rejected() {
        testing_aids::assert_panic!(resurrect_signal_ref(std::ptr::null()));
    }

    #[test]
    fn awaken_via_embedded_signal() {
        let fake_task_id = TaskId::fake();

        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(10)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));
        let parent_waker = Arc::new(TestWaker::new());

        // We hold the lock - the signal cannot use the set.
        let awakened_set_lock_guard = awakened_queue.lock().unwrap();

        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Arc::clone(&parent_waker).into(),
            fake_task_id
        ));

        // WakeSignal is only meant to be consumed via shared references.
        let signal = signal.as_ref();

        // SAFETY: The signal remains alive through the end of the test.
        // We expect that the test leaves us in this state and rely on assertions to verify it.
        let waker = unsafe { signal.waker() };

        let initially_awakened = signal.consume_awakened();
        let parent_initially_awakened = parent_waker.awakened.load(atomic::Ordering::Relaxed);

        waker.wake_by_ref();
        let embedded_probe_awakened = probe_embedded_wake_signals.load(atomic::Ordering::Relaxed);
        let awakened = signal.consume_awakened();
        let parent_awakened = parent_waker.awakened.load(atomic::Ordering::Relaxed);

        // Verify that it is now consumed.
        let awakened_after_consumption = signal.consume_awakened();
        drop(awakened_set_lock_guard);

        assert!(!initially_awakened);
        assert!(!parent_initially_awakened);
        assert!(embedded_probe_awakened);
        assert!(awakened);
        assert!(parent_awakened);
        assert!(!awakened_after_consumption);
    }

    #[test]
    fn awaken_via_awakened_set() {
        let fake_task_id = TaskId::fake();

        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(10)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));
        let parent_waker = Arc::new(TestWaker::new());

        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Arc::clone(&parent_waker).into(),
            fake_task_id
        ));

        // WakeSignal is only meant to be consumed via shared references.
        let signal = signal.as_ref();

        // SAFETY: The signal remains alive through the end of the test.
        // We expect that the test leaves us in this state and rely on assertions to verify it.
        let waker = unsafe { signal.waker() };

        assert!(!signal.consume_awakened());
        assert!(!parent_waker.awakened.load(atomic::Ordering::Relaxed));

        waker.wake_by_ref();
        // It should not have set the embedded signal here because we use the awakened set.
        assert!(!probe_embedded_wake_signals.load(atomic::Ordering::Relaxed));
        assert!(!signal.consume_awakened());
        assert!(!awakened_queue.lock().unwrap().is_empty());

        // But it should always signal the parent waker.
        assert!(parent_waker.awakened.load(atomic::Ordering::Relaxed));
    }

    #[test]
    fn awaken_via_full_awakened_set() {
        let fake_task_id = TaskId::fake();

        // Capacity is 0 so the queue is not allowed to allocate (== is never used).
        let awakened_queue = Arc::new(Mutex::new(VecDeque::new()));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));
        let parent_waker = Arc::new(TestWaker::new());

        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Arc::clone(&parent_waker).into(),
            fake_task_id
        ));

        // WakeSignal is only meant to be consumed via shared references.
        let signal = signal.as_ref();

        // SAFETY: The signal remains alive through the end of the test.
        // We expect that the test leaves us in this state and rely on assertions to verify it.
        let waker = unsafe { signal.waker() };

        assert!(!signal.consume_awakened());
        assert!(!parent_waker.awakened.load(atomic::Ordering::Relaxed));

        waker.wake_by_ref();
        // Even though it could lock the set, it could not use it because it was at capacity.
        assert!(probe_embedded_wake_signals.load(atomic::Ordering::Relaxed));
        assert!(signal.consume_awakened());
        assert!(parent_waker.awakened.load(atomic::Ordering::Relaxed));
    }

    #[test]
    fn detached_reference_count_tracks_owned_wakers() {
        let fake_task_id = TaskId::fake();

        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(10)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));

        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Waker::noop().clone(),
            fake_task_id
        ));

        // WakeSignal is only meant to be consumed via shared references.
        let signal = signal.as_ref();

        // SAFETY: The signal remains alive through the end of the test.
        // We expect that the test leaves us in this state and rely on assertions to verify it.
        let waker = unsafe { signal.waker() };

        let waker_clone = waker.clone();

        assert_eq!(signal.external_waker_count(), 2);

        // Drop the wakers and we are inert again.
        drop(waker);
        drop(waker_clone);

        assert_eq!(signal.external_waker_count(), 0);

        // And back to having wakers!

        // SAFETY: The signal remains alive through the end of the test.
        // We expect that the test leaves us in this state and rely on assertions to verify it.
        let waker = unsafe { signal.waker() };

        let waker_clone = waker.clone();

        assert_eq!(signal.external_waker_count(), 2);

        // Drop the wakers and we are inert again.
        drop(waker);
        drop(waker_clone);

        assert_eq!(signal.external_waker_count(), 0);
    }

    #[test]
    fn consuming_waker_wakes_signal() {
        let fake_task_id = TaskId::fake();

        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(1)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));

        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
            Waker::noop().clone(),
            fake_task_id
        ));
        let signal = signal.as_ref();

        // SAFETY: The consuming wake releases the only waker before the signal is dropped.
        let waker = unsafe { signal.waker() };
        waker.wake();

        assert_eq!(signal.external_waker_count(), 0);
        assert!(!awakened_queue.lock().unwrap().is_empty());
    }

    #[test]
    fn consuming_waker_releases_ownership_when_owner_panics() {
        struct PanicWake;

        impl std::task::Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("owner wake panic");
            }
        }

        let fake_task_id = TaskId::fake();
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(1)));
        let signal = pin!(WakeSignal::new_with_parts(
            Arc::clone(&awakened_queue),
            Arc::new(AtomicBool::new(false)),
            Waker::from(Arc::new(PanicWake)),
            fake_task_id
        ));
        let signal = signal.as_ref();

        // SAFETY: The signal remains pinned through the consuming wake.
        let waker = unsafe { signal.waker() };
        testing_aids::assert_panic!(waker.wake());

        assert_eq!(signal.external_waker_count(), 0);
    }
}
