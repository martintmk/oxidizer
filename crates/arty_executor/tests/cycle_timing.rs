// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Cycle outcomes carry the completion time from the configured clock.

use std::future::poll_fn;
use std::task::Poll;
use std::time::{Duration, Instant};

use arty_executor::{CycleOutcome, Executor, ShutdownCause, TaskCompletion, TaskOptions, TaskPanicPayload, discard_task_panic_payload};
use tick::ClockControl;

#[derive(Debug)]
struct DiscardCompletion;

impl TaskCompletion<()> for DiscardCompletion {
    fn completed(self, _result: ()) {}

    fn panicked(self, panic: TaskPanicPayload, _cause: Option<ShutdownCause>) {
        discard_task_panic_payload(panic);
    }

    fn cancelled(self, _cause: ShutdownCause) {}
}

#[test]
fn cycle_completion_uses_configured_time_for_every_outcome() {
    let control = ClockControl::new();
    let clock = control.to_simple_clock();
    let start = clock.instant();
    let mut executor = Executor::builder().clock(clock.clone()).build();
    let mut polls = 0;
    let task_control = control.clone();
    executor.tasks().add(
        poll_fn(move |cx| {
            task_control.advance(Duration::from_secs(7));
            polls += 1;
            if polls == 1 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }),
        TaskOptions::default(),
        DiscardCompletion,
    );

    let (outcome, finished) = executor.execute_cycle();
    assert!(matches!(outcome, CycleOutcome::Continue));
    assert_eq!(finished, start + Duration::from_secs(7));

    let (outcome, finished) = executor.execute_cycle();
    assert!(matches!(outcome, CycleOutcome::Suspend));
    assert_eq!(finished, start + Duration::from_secs(14));

    let (outcome, finished) = executor.execute_cycle();
    assert!(matches!(outcome, CycleOutcome::Suspend));
    assert_eq!(finished, clock.instant());

    control.advance(Duration::from_secs(3));
    executor.shutdown().trigger(ShutdownCause::default());
    let (outcome, finished) = executor.execute_cycle();
    assert!(matches!(outcome, CycleOutcome::Shutdown(_)));
    assert_eq!(finished, start + Duration::from_secs(17));

    let (outcome, finished) = executor.execute_cycle();
    assert!(matches!(outcome, CycleOutcome::Shutdown(_)));
    assert_eq!(finished, clock.instant());
}

#[test]
fn completion_timestamp_reuses_the_cycle_end_sample() {
    let control = ClockControl::new();
    let clock = control.to_simple_clock();
    let start = clock.instant();
    let control = control.auto_advance(Duration::from_secs(1));
    let mut executor = Executor::builder().clock(clock.clone()).build();

    let result = executor.execute_cycle();
    let _control = control.auto_advance(Duration::ZERO);

    assert!(matches!(result.0, CycleOutcome::Suspend));
    assert_eq!(result.1, start + Duration::from_secs(1));
    assert_eq!(clock.instant(), start + Duration::from_secs(2));
}

#[test]
fn default_clock_returns_a_system_timestamp() {
    let mut executor = Executor::builder().build();
    let before = Instant::now();
    let (outcome, now) = executor.execute_cycle();
    let after = Instant::now();

    assert!(matches!(outcome, CycleOutcome::Suspend));
    // The default clock can be coarse on Linux and Windows.
    let tolerance = Duration::from_millis(100);
    assert!(now >= before.checked_sub(tolerance).unwrap());
    assert!(now <= after + tolerance);
}
