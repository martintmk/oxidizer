// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Public executor contracts for retained and panicking task wakers.

use std::cell::RefCell;
use std::future::poll_fn;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Wake, Waker};

use arty_executor::{CycleOutcome, Executor, TaskOptions};

#[test]
fn retained_waker_is_noop_after_shutdown() {
    let executor = Executor::builder().build();
    let retained = Rc::new(RefCell::new(None));
    let task = executor.tasks().add(
        poll_fn({
            let retained = Rc::clone(&retained);
            move |cx| {
                *retained.borrow_mut() = Some(cx.waker().clone());
                Poll::Ready(())
            }
        }),
        TaskOptions::default(),
    );

    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    let retained = retained.borrow_mut().take().unwrap();
    drop(task);
    executor.begin_shutdown();
    assert_eq!(executor.execute_cycle(), CycleOutcome::Shutdown);
    drop(executor);

    std::thread::spawn(move || {
        retained.wake_by_ref();
        retained.wake();
    })
    .join()
    .unwrap();
}

#[test]
fn owner_waker_panic_does_not_prevent_shutdown() {
    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("owner wake panic");
        }

        fn wake_by_ref(self: &Arc<Self>) {
            panic!("owner wake panic");
        }
    }

    let executor = Executor::builder().owner_waker(Waker::from(Arc::new(PanicWake))).build();
    let retained = Rc::new(RefCell::new(None));
    let task = executor.tasks().add(
        poll_fn({
            let retained = Rc::clone(&retained);
            move |cx| {
                retained.borrow_mut().get_or_insert_with(|| cx.waker().clone());
                Poll::<()>::Pending
            }
        }),
        TaskOptions::default(),
    );
    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    let retained = retained.borrow_mut().take().unwrap();

    assert!(catch_unwind(AssertUnwindSafe(|| retained.wake())).is_err());
    drop(task);
    executor.begin_shutdown();
    assert_eq!(executor.execute_cycle(), CycleOutcome::Shutdown);
}

#[test]
fn retired_waker_cannot_wake_a_later_task() {
    let executor = Executor::builder().build();
    let tasks = executor.tasks();
    let retained = Rc::new(RefCell::new(None));
    _ = tasks.add(
        poll_fn({
            let retained = Rc::clone(&retained);
            move |cx| {
                *retained.borrow_mut() = Some(cx.waker().clone());
                Poll::Ready(())
            }
        }),
        TaskOptions::default(),
    );
    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    let retained = retained.borrow_mut().take().unwrap();

    let later_polls = Rc::new(std::cell::Cell::new(0));
    let later = tasks.add(
        poll_fn({
            let later_polls = Rc::clone(&later_polls);
            move |_| {
                later_polls.set(later_polls.get() + 1);
                Poll::<()>::Pending
            }
        }),
        TaskOptions::default(),
    );
    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    assert_eq!(later_polls.get(), 1);

    retained.wake_by_ref();

    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    assert_eq!(later_polls.get(), 1);
    drop(later);
    executor.begin_shutdown();
    assert_eq!(executor.execute_cycle(), CycleOutcome::Shutdown);
}

#[test]
fn borrowed_waker_can_be_cloned_concurrently_during_poll() {
    const CLONES: usize = 8;

    let executor = Executor::builder().build();
    let retained = Arc::new(Mutex::new(Vec::<Waker>::new()));
    let cloned = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut task = executor.tasks().add(
        poll_fn({
            let retained = Arc::clone(&retained);
            let cloned = Arc::clone(&cloned);
            let ready = Arc::clone(&ready);
            let polls = Arc::clone(&polls);
            move |cx| {
                polls.fetch_add(1, Ordering::Relaxed);
                if !cloned.swap(true, Ordering::Relaxed) {
                    std::thread::scope(|scope| {
                        for _ in 0..CLONES {
                            let retained = Arc::clone(&retained);
                            let waker = cx.waker();
                            scope.spawn(move || retained.lock().unwrap().push(waker.clone()));
                        }
                    });
                }
                if ready.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }),
        TaskOptions::default(),
    );

    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    assert_eq!(retained.lock().unwrap().len(), CLONES);

    ready.store(true, Ordering::Release);
    let retained = std::mem::take(&mut *retained.lock().unwrap());
    std::thread::scope(|scope| {
        for waker in retained {
            scope.spawn(move || waker.wake());
        }
    });

    assert_eq!(executor.execute_cycle(), CycleOutcome::Suspend);
    let mut cx = std::task::Context::from_waker(Waker::noop());
    assert!(std::pin::Pin::new(&mut task).poll(&mut cx).is_ready());
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    executor.begin_shutdown();
    assert_eq!(executor.execute_cycle(), CycleOutcome::Shutdown);
}
