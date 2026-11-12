// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Tests ownership and shutdown through the public compatibility API.

#![expect(clippy::unwrap_used, reason = "test failures use unwrap for concise backtraces")]

mod support;

use std::future::pending;
use std::sync::{Arc, mpsc};

use arty::runtime::{Runtime, WorkersPolicy};
use arty_tokio::TokioSupportExt as _;
use support::{TEST_TIMEOUT, stop_bounded};
use tokio::runtime::Builder;

#[test]
fn external_runtime_is_not_retained_when_it_shuts_down_first() {
    let tokio = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
    let weak = Arc::downgrade(&tokio);
    let runtime = Runtime::builder()
        .workers(WorkersPolicy::exactly(1))
        .tokio_support(tokio.handle())
        .build()
        .unwrap();

    let (dropped, completion) = mpsc::sync_channel(1);
    let drop_runtime = std::thread::spawn(move || {
        drop(tokio);
        dropped.send(()).unwrap();
    });
    completion.recv_timeout(TEST_TIMEOUT).unwrap();
    drop_runtime.join().unwrap();
    assert!(weak.upgrade().is_none());

    let still_running = runtime.scheduler().block_on(async |_| 42).unwrap();
    assert_eq!(still_running, 42);
    stop_bounded(runtime);
}

#[test]
fn cross_runtime_ownership_cycle_is_broken_during_shutdown() {
    let tokio = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
    let weak = Arc::downgrade(&tokio);
    let runtime = Runtime::builder()
        .workers(WorkersPolicy::exactly(1))
        .tokio_support(tokio.handle())
        .build()
        .unwrap();
    let (started, ready) = mpsc::sync_channel(1);

    runtime
        .scheduler()
        .block_on(async |cx| {
            let never_completes = cx.scheduler().spawn(async |_| pending::<()>().await);
            drop(tokio::spawn(async move {
                started.send(()).unwrap();
                let () = never_completes.await.unwrap();
            }));
            ready.recv_timeout(TEST_TIMEOUT).unwrap();
        })
        .unwrap();

    drop(tokio);
    assert!(weak.upgrade().is_none());
    stop_bounded(runtime);
}
