// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::error::Error as _;
use std::panic::panic_any;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use arty_executor::{CycleOutcome, Executor, TaskPlacement};
use futures::executor::block_on;
use observed::Sink;

use super::*;
use crate::runtime::{Runtime, Shutdown, WorkersPolicy};
use crate::task::Builtins;
use crate::task::handle::JoinHandlePool;

fn test_builtins() -> (Runtime, Builtins) {
    let runtime = Runtime::builder().workers(WorkersPolicy::exactly(1)).build().unwrap();
    let builtins = runtime.scheduler().block_on(async |builtins| builtins).unwrap();
    (runtime, builtins)
}

#[test]
fn remote_factory_remains_deferred_until_poll() {
    let invoked = Arc::new(AtomicBool::new(false));
    let factory_invoked = Arc::clone(&invoked);
    let sink = Sink::noop();
    let result_pool = JoinHandlePool::new(Shutdown::default());
    let (factory, handle) = prepare_remote(
        &result_pool,
        move |_| {
            // This test polls and observes the factory on one thread; no synchronization is needed.
            factory_invoked.store(true, Ordering::Relaxed);
            async {
                // Retain an Rc across await so the factory-produced future remains non-Send.
                let value = Rc::new(42);
                std::future::ready(()).await;
                *value
            }
        },
        sink.transfer_context(),
        TaskPlacement::Any,
    );

    let mut executor = Executor::builder().owner_waker(Waker::noop().clone()).build();
    let (_runtime, builtins) = test_builtins();
    let _registered = factory.register(builtins, &executor.tasks());
    let before_poll = invoked.load(Ordering::Relaxed);
    run_to_completion(&mut executor);
    let result = block_on(handle).unwrap();

    assert_eq!((before_poll, invoked.load(Ordering::Relaxed), result), (false, true, 42),);
}

#[test]
fn shutdown_cancelled_deferred_task_stays_pending_without_invoking_factory() {
    let (runtime, builtins) = test_builtins();
    let invoked = Arc::new(AtomicBool::new(false));
    let factory_invoked = Arc::clone(&invoked);
    builtins.control().request_stop();
    let mut task = pin!(DeferredTask::new(
        move |_| {
            factory_invoked.store(true, Ordering::Release);
            std::future::ready(())
        },
        builtins,
    ));
    let mut cx = Context::from_waker(Waker::noop());

    assert!(task.as_mut().poll(&mut cx).is_pending());
    assert!(task.as_mut().poll(&mut cx).is_pending());
    assert!(!invoked.load(Ordering::Acquire));
    runtime.stop().unwrap();
}

/// Drives the executor until it reports that no further progress can be made.
fn run_to_completion(executor: &mut Executor) {
    while matches!(executor.execute_cycle(), CycleOutcome::Continue) {}
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("join receiver notification");
    }
}

#[test]
fn blocking_task_delivers_its_result() {
    let result_pool = JoinHandlePool::new(Shutdown::default());
    let (task, handle) = prepare_blocking(&result_pool, || 42);
    task();

    assert_eq!(block_on(handle).unwrap(), 42);
}

#[test]
fn blocking_panic_is_delivered_to_joiner() {
    let result_pool = JoinHandlePool::new(Shutdown::default());
    let (task, handle) = prepare_blocking(&result_pool, || panic!("blocking task"));
    task();

    assert!(block_on(handle).unwrap_err().is_panic());
}

#[test]
fn remote_factory_panic_is_delivered_to_joiner() {
    #[derive(Debug, PartialEq)]
    struct Payload(u32);

    let sink = Sink::noop();
    let result_pool = JoinHandlePool::new(Shutdown::default());
    let (factory, handle) = prepare_remote(
        &result_pool,
        |_| -> std::future::Ready<()> { panic_any(Payload(42)) },
        sink.transfer_context(),
        TaskPlacement::Any,
    );
    let mut executor = Executor::builder().owner_waker(Waker::noop().clone()).build();
    let (_runtime, builtins) = test_builtins();
    let _registered = factory.register(builtins, &executor.tasks());
    run_to_completion(&mut executor);
    assert!(block_on(handle).unwrap_err().is_panic());
}

#[test]
fn discarded_preparation_reports_the_shutdown_cause() {
    let shutdown = Shutdown::default();
    shutdown.begin(arty_executor::ShutdownError::new(std::io::Error::other("blocking shutdown")).into());
    let result_pool = JoinHandlePool::new(shutdown);
    let (task, handle) = prepare_blocking(&result_pool, || 42);
    drop(task);
    let mut handle = pin!(handle);
    let mut context = Context::from_waker(Waker::noop());

    let Poll::Ready(Err(error)) = handle.as_mut().poll(&mut context) else {
        panic!("discarded work must report shutdown");
    };
    assert!(error.is_shutdown());
    assert!(error.source().is_some());
}

#[test]
fn discarded_remote_factory_reports_the_shutdown_cause() {
    let shutdown = Shutdown::default();
    shutdown.begin(arty_executor::ShutdownError::new(std::io::Error::other("queued shutdown")).into());
    let result_pool = JoinHandlePool::new(shutdown);
    let (factory, handle) = prepare_remote(&result_pool, |_| async { 42 }, Sink::noop().transfer_context(), TaskPlacement::Any);

    drop(factory);

    let error = block_on(handle).unwrap_err();
    assert!(error.is_shutdown());
    assert!(error.source().is_some());
}

#[test]
fn discarded_blocking_task_contains_body_destructor_panic() {
    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("discarded blocking capture");
        }
    }

    let capture = PanicOnDrop;
    let result_pool = JoinHandlePool::new(Shutdown::default());
    let (task, handle) = prepare_blocking(&result_pool, move || drop(capture));
    catch_unwind(AssertUnwindSafe(|| drop(task))).unwrap();
    assert!(block_on(handle).unwrap_err().is_shutdown());
}

#[test]
fn discarded_blocking_task_contains_join_waker_panic() {
    let result_pool = JoinHandlePool::new(Shutdown::default());
    let (task, handle) = prepare_blocking(&result_pool, || 42);
    let mut handle = pin!(handle);
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(handle.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    catch_unwind(AssertUnwindSafe(|| drop(task))).unwrap();

    let Poll::Ready(Err(error)) = handle.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("discarded blocking work must report shutdown");
    };
    assert!(error.is_shutdown());
}
