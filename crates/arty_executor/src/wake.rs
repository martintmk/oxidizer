// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::VecDeque;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{RawWaker, RawWakerVTable, Waker};

use plurality::{Arc as PoolArc, Pool};

use crate::{ERR_POISONED_LOCK, TaskId};

/// Only IDs cross threads; the task map and task storage stay on the executor's thread.
#[derive(Debug)]
pub(crate) struct WakeShared {
    pub(crate) awakened: Mutex<VecDeque<TaskId>>,
    parent_waker: Waker,
    states: Mutex<Pool<WakeState>>,
}

impl WakeShared {
    pub(crate) fn new(parent_waker: Waker) -> Self {
        Self {
            awakened: Mutex::new(VecDeque::new()),
            parent_waker,
            states: Mutex::new(Pool::new()),
        }
    }
}

/// Owns a pooled wake token independently of task storage. Retained wakers become inert on
/// retirement and cannot keep the executor alive.
#[derive(Debug)]
pub(crate) struct WakeSignal {
    state: PoolArc<WakeState>,
    waker: Waker,
}

impl WakeSignal {
    pub(crate) fn new(shared: &Arc<WakeShared>, task_id: TaskId) -> Self {
        let state = shared.states.lock().expect(ERR_POISONED_LOCK).alloc_arc(WakeState {
            shared: Arc::downgrade(shared),
            task_id,
            active: AtomicBool::new(true),
            queued: AtomicBool::new(false),
        });
        let waker = into_waker(PoolArc::clone(&state));
        Self { state, waker }
    }

    #[cfg(test)]
    pub(crate) fn fake() -> Self {
        Self::new(&Arc::new(WakeShared::new(Waker::noop().clone())), TaskId::fake())
    }

    pub(crate) fn waker(&self) -> &Waker {
        &self.waker
    }

    pub(crate) fn retire(&self) {
        self.state.active.store(false, Ordering::Relaxed);
    }

    /// Called while holding the queue mutex, before the task is polled again.
    pub(crate) fn clear_queued_notification(&self) {
        self.state.queued.store(false, Ordering::Relaxed);
    }
}

impl Drop for WakeSignal {
    fn drop(&mut self) {
        self.retire();
    }
}

#[derive(Debug)]
struct WakeState {
    shared: Weak<WakeShared>,
    task_id: TaskId,
    active: AtomicBool,
    queued: AtomicBool,
}

impl WakeState {
    fn wake(&self) {
        if !self.active.load(Ordering::Relaxed) {
            return;
        }
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        {
            let mut queue = shared.awakened.lock().expect(ERR_POISONED_LOCK);
            if !self.active.load(Ordering::Relaxed) {
                return;
            }
            // Serialize even coalesced wakes with queue draining to publish the caller's writes.
            if !self.queued.load(Ordering::Relaxed) {
                queue.push_back(self.task_id);
                self.queued.store(true, Ordering::Relaxed);
            }
        }
        // Parent wakers may run user code, reenter the executor, or panic.
        shared.parent_waker.wake_by_ref();
    }
}

// std::task::Wake requires std::sync::Arc, which would allocate per task. This adapter transfers
// pooled Arc owners instead; reference counting and reclamation remain entirely in plurality.
static WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

fn into_waker(state: PoolArc<WakeState>) -> Waker {
    let ptr = PoolArc::into_raw(state);
    // SAFETY: Each raw waker owns one pooled Arc. The vtable clones, borrows, or consumes that
    // owner, and WakeState is Send + Sync without access to task storage.
    unsafe { Waker::from_raw(RawWaker::new(ptr.as_ptr().cast(), &WAKER_VTABLE)) }
}

unsafe fn take_owner(ptr: *const ()) -> PoolArc<WakeState> {
    let ptr = NonNull::new(ptr.cast_mut().cast()).expect("pooled Arc raw pointers are non-null");
    // SAFETY: The caller transfers one outstanding owner created by PoolArc::into_raw.
    unsafe { PoolArc::from_raw(ptr) }
}

fn clone_waker(ptr: *const ()) -> RawWaker {
    // SAFETY: The caller's raw waker retains ownership; ManuallyDrop prevents consuming it.
    let owner = ManuallyDrop::new(unsafe { take_owner(ptr) });
    let clone = PoolArc::into_raw(PoolArc::clone(&owner));
    RawWaker::new(clone.as_ptr().cast(), &WAKER_VTABLE)
}

fn wake(ptr: *const ()) {
    // SAFETY: Consuming wake transfers this waker's owner, also releasing it if the parent panics.
    let owner = unsafe { take_owner(ptr) };
    owner.wake();
}

fn wake_by_ref(ptr: *const ()) {
    // SAFETY: The caller retains ownership throughout this borrow, including on unwind.
    let owner = ManuallyDrop::new(unsafe { take_owner(ptr) });
    owner.wake();
}

fn drop_waker(ptr: *const ()) {
    // SAFETY: Dropping the raw waker consumes exactly its outstanding owner.
    drop(unsafe { take_owner(ptr) });
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::task::Wake;
    use std::thread;

    use super::*;
    use crate::testing::TestWaker;

    #[test]
    fn repeated_and_concurrent_wakes_are_coalesced() {
        let parent = Arc::new(TestWaker::new());
        let shared = Arc::new(WakeShared::new(Arc::clone(&parent).into()));
        let signal = WakeSignal::new(&shared, TaskId::fake());
        thread::scope(|scope| {
            for _ in 0..4 {
                let waker = signal.waker().clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        waker.wake_by_ref();
                    }
                    waker.wake();
                });
            }
        });
        assert_eq!(*shared.awakened.lock().unwrap(), VecDeque::from([TaskId::fake()]));
        assert!(parent.awakened.load(Ordering::Relaxed));
        {
            let mut queue = shared.awakened.lock().unwrap();
            queue.clear();
            signal.clear_queued_notification();
        }
        signal.waker().wake_by_ref();
        assert_eq!(shared.awakened.lock().unwrap().len(), 1);
    }

    #[test]
    fn wake_waits_for_queue_contention_instead_of_losing_notification() {
        let shared = Arc::new(WakeShared::new(Waker::noop().clone()));
        let signal = WakeSignal::new(&shared, TaskId::fake());
        let barrier = Barrier::new(2);
        thread::scope(|scope| {
            let queue = shared.awakened.lock().unwrap();
            scope.spawn(|| {
                barrier.wait();
                signal.waker().wake_by_ref();
            });
            barrier.wait();
            drop(queue);
        });
        assert_eq!(shared.awakened.lock().unwrap().len(), 1);
    }

    #[test]
    #[expect(clippy::waker_clone_wake, reason = "exercise consuming wake on an independently retained owner")]
    fn retired_and_dropped_signals_leave_retained_wakers_inert() {
        let shared = Arc::new(WakeShared::new(Waker::noop().clone()));
        let signal = WakeSignal::new(&shared, TaskId::fake());
        let retained = signal.waker().clone();
        signal.retire();
        retained.wake_by_ref();
        assert!(shared.awakened.lock().unwrap().is_empty());
        drop(signal);
        retained.clone().wake();
        assert!(shared.awakened.lock().unwrap().is_empty());
        drop(shared);
        thread::spawn(move || retained.wake()).join().unwrap();
    }

    #[test]
    fn dropping_active_signal_retires_retained_wakers() {
        let shared = Arc::new(WakeShared::new(Waker::noop().clone()));
        let signal = WakeSignal::new(&shared, TaskId::fake());
        let retained = signal.waker().clone();
        drop(signal);
        retained.wake();
        assert!(shared.awakened.lock().unwrap().is_empty());
        assert!(shared.states.lock().unwrap().is_empty());
    }

    #[test]
    fn retained_waker_tolerates_shared_state_dropped_first() {
        let shared = Arc::new(WakeShared::new(Waker::noop().clone()));
        let signal = WakeSignal::new(&shared, TaskId::fake());
        let retained = signal.waker().clone();
        drop(shared);
        retained.wake_by_ref();
        drop(signal);
        retained.wake();
    }

    #[test]
    #[expect(clippy::waker_clone_wake, reason = "exercise the consuming-wake callback")]
    fn parent_runs_without_holding_queue_lock() {
        #[derive(Debug)]
        struct ReentrantParent(Mutex<Weak<WakeShared>>);
        impl Wake for ReentrantParent {
            fn wake(self: Arc<Self>) {
                let shared = self.0.lock().unwrap().upgrade().unwrap();
                assert_eq!(shared.awakened.try_lock().unwrap().len(), 1);
            }
        }
        let parent = Arc::new(ReentrantParent(Mutex::new(Weak::new())));
        let shared = Arc::new(WakeShared::new(Arc::clone(&parent).into()));
        *parent.0.lock().unwrap() = Arc::downgrade(&shared);
        let signal = WakeSignal::new(&shared, TaskId::fake());
        signal.waker().clone().wake();
    }

    #[test]
    fn parent_panic_preserves_queue_and_releases_consumed_owner() {
        struct PanicParent;
        impl Wake for PanicParent {
            fn wake(self: Arc<Self>) {
                panic!("parent wake panic");
            }
        }
        let shared = Arc::new(WakeShared::new(Arc::new(PanicParent).into()));
        let signal = WakeSignal::new(&shared, TaskId::fake());
        let waker = signal.waker().clone();
        testing_aids::assert_panic!(waker.wake());
        assert_eq!(shared.awakened.lock().unwrap().len(), 1);
        testing_aids::assert_panic!(signal.waker().wake_by_ref());
        drop(signal);
        assert!(shared.states.lock().unwrap().is_empty());
    }
}
