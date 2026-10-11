// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Worker clocks advance timers and remain usable after relocation.

#![cfg(all(feature = "rt", feature = "macros"))]

testing_aids::init_tracing!();

use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, mpsc};
use std::task::{Context, Wake, Waker};
use std::thread;
use std::time::Duration;

use arty::runtime::{Runtime, WorkersPolicy};
use arty::task::Builtins;
use tick::FutureExt;

const TIMER_COUNT: usize = 8;

struct TimerWake(mpsc::Sender<()>);

impl Wake for TimerWake {
    fn wake(self: Arc<Self>) {
        _ = self.0.send(());
    }
}

#[test]
fn earlier_timer_registration_interrupts_parked_worker() {
    let runtime = Runtime::builder().workers(WorkersPolicy::exactly(1)).build().unwrap();
    let clock = runtime.scheduler().block_on(async |builtins| builtins.clock().clone()).unwrap();

    let mut long_delay = pin!(clock.delay(Duration::from_secs(5)));
    assert!(long_delay.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());

    // Give the worker time to consume the registration wake and park against the long deadline.
    thread::sleep(Duration::from_millis(50));

    let (timer_fired, fired) = mpsc::channel();
    let timer_waker = Waker::from(Arc::new(TimerWake(timer_fired)));
    let mut short_delay = pin!(clock.delay(Duration::from_millis(20)));
    assert!(short_delay.as_mut().poll(&mut Context::from_waker(&timer_waker)).is_pending());

    // This is shorter than the worker's one-second fail-safe poll. The timer can fire in time only
    // if registering the earlier deadline interrupts the existing five-second park.
    fired.recv_timeout(Duration::from_millis(750)).unwrap();
    assert!(short_delay.as_mut().poll(&mut Context::from_waker(&timer_waker)).is_ready());

    drop(short_delay);
    drop(long_delay);
    drop(clock);
    runtime.stop().unwrap();
}

#[arty::test]
async fn many_timers_with_relocation_ensure_advanced(builtins: Builtins) {
    let scheduler = builtins.scheduler();
    // Ensure each relocated task keeps access to the runtime clock.
    let handles: Vec<_> = (0..TIMER_COUNT)
        .map(|_| {
            scheduler.spawn_anywhere(builtins.clone(), move |builtins| async move {
                println!("delay(pending) - clock: {:?}, thread: {:?}", builtins.clock(), builtins.thread());

                let watch = builtins.clock().stopwatch();
                builtins.clock().delay(Duration::from_millis(50)).await;

                println!("delay(done) - clock: {:?}, thread: {:?}", builtins.clock(), builtins.thread());

                assert!(watch.elapsed().as_millis() >= 50);
            })
        })
        .collect();

    for handle in handles {
        handle.timeout(builtins.clock(), Duration::from_secs(10)).await.unwrap().unwrap();
    }
}

#[arty::test]
async fn many_timers_ensure_advanced(builtins: Builtins) {
    let handles: Vec<_> = (0..TIMER_COUNT)
        .map(|_| {
            builtins
                .scheduler()
                .spawn_anywhere(builtins.clone(), |builtins| builtins.clock().delay(Duration::from_millis(1)))
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}

#[arty::test]
async fn timer_with_relocated_builtins(builtins: Builtins) {
    builtins
        .scheduler()
        .spawn_anywhere(builtins.clone(), |builtins| builtins.clock().delay(Duration::from_millis(10)))
        .await
        .unwrap();
}
