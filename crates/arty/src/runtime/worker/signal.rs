// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc as StdArc;
use std::task::{Wake, Waker};
use std::time::Duration;

use performables::arc::Arc;
use performables::sync::condition::Condvar;
use performables::sync::mutex::Mutex;

/// A coalescing notification shared by task wakers, timer registration, and the command dispatcher.
#[derive(Debug, Default)]
pub(crate) struct WorkerSignal {
    state: Mutex<SignalState>,
    ready: Condvar,
}

#[derive(Debug, Default)]
struct SignalState {
    notified: bool,
    timer_registration_armed: bool,
}

#[derive(Debug)]
struct TimerRegistrationSignal {
    signal: Arc<WorkerSignal>,
}

fn should_finish_wait(timed_out: bool, elapsed: Duration, timeout: Duration) -> bool {
    timed_out && elapsed >= timeout
}

fn wait_until_deadline<S>(
    state: &mut S,
    timeout: Duration,
    mut waiting: impl FnMut(&S) -> bool,
    mut elapsed: impl FnMut(&S) -> Duration,
    mut timed_wait: impl FnMut(&mut S, Duration) -> bool,
) {
    while waiting(state) {
        let remaining = timeout.saturating_sub(elapsed(state));
        if remaining.is_zero() {
            break;
        }
        let timed_out = timed_wait(state, remaining);
        if should_finish_wait(timed_out, elapsed(state), timeout) {
            break;
        }
    }
}

impl WorkerSignal {
    pub(crate) fn wait(&self, timeout: Duration) -> bool {
        self.wait_before_park(timeout, || {})
    }

    fn wait_before_park(&self, timeout: Duration, mut before_park: impl FnMut()) -> bool {
        let start = std::time::Instant::now();
        let mut state = Some(self.state.lock());
        wait_until_deadline(
            &mut state,
            timeout,
            |state| {
                !state
                    .as_ref()
                    .expect("the wait always retains or immediately replaces its notification guard")
                    .notified
            },
            |_| start.elapsed(),
            |state, remaining| {
                before_park();
                let (guard, elapsed) = self.ready.wait_timeout(
                    state
                        .take()
                        .expect("the previous timed wait always replaced its notification guard"),
                    remaining,
                );
                *state = Some(guard);
                elapsed.timed_out()
            },
        );
        let mut state = state.expect("the final timed wait always replaced its notification guard");
        let was_notified = state.notified;
        state.notified = false;
        state.timer_registration_armed = false;
        was_notified
    }

    pub(crate) fn waker(signal: &Arc<Self>) -> Waker {
        // Wake requires a standard Arc; both handles share the same allocation.
        Waker::from(Arc::into_std_arc(Arc::clone(signal)))
    }

    pub(crate) fn timer_registration_waker(signal: &Arc<Self>) -> Waker {
        Waker::from(StdArc::new(TimerRegistrationSignal {
            signal: Arc::clone(signal),
        }))
    }

    pub(crate) fn arm_timer_registration(&self) {
        self.state.lock().timer_registration_armed = true;
    }

    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn is_notified(&self) -> bool {
        self.state.lock().notified
    }

    fn notify(&self) {
        // A waiter checks and consumes this flag under the same mutex before parking, so only the
        // transition from clear to set needs a native notification.
        if self.mark_notified() {
            self.wake_waiter();
        }
    }

    fn notify_timer_registration(&self) {
        if self.mark_timer_registration_notified() {
            self.wake_waiter();
        }
    }

    fn mark_notified(&self) -> bool {
        let mut state = self.state.lock();
        !std::mem::replace(&mut state.notified, true)
    }

    fn mark_timer_registration_notified(&self) -> bool {
        let mut state = self.state.lock();
        if !state.timer_registration_armed {
            return false;
        }
        !std::mem::replace(&mut state.notified, true)
    }

    #[cfg_attr(test, mutants::skip)] // Removing the native notification strands the smoke test; flag handling is tested separately.
    fn wake_waiter(&self) {
        self.ready.notify_one();
    }
}

impl Wake for WorkerSignal {
    fn wake(self: StdArc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.notify();
    }
}

impl Wake for TimerRegistrationSignal {
    fn wake(self: StdArc<Self>) {
        self.signal.notify_timer_registration();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.signal.notify_timer_registration();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding is not runtime behavior.
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;
    use std::thread;

    use testing_aids::{TEST_TIMEOUT, is_mutation_testing};

    use super::*;

    #[test]
    fn notification_before_wait_is_retained_and_consumed() {
        let signal = Arc::new(WorkerSignal::default());
        let waker = WorkerSignal::waker(&signal);
        waker.wake_by_ref();
        assert!(signal.state.lock().notified);
        assert!(signal.wait(Duration::ZERO));
        assert!(!signal.state.lock().notified);
        waker.wake();
        assert!(signal.state.lock().notified);
        assert!(signal.wait(Duration::ZERO));
        assert!(!signal.state.lock().notified);
    }

    #[test]
    fn duplicate_notifications_are_coalesced_until_consumed() {
        let signal = WorkerSignal::default();

        assert!(signal.mark_notified());
        assert!(!signal.mark_notified());
        assert!(signal.wait(Duration::ZERO));
        assert!(signal.mark_notified());
    }

    #[test]
    fn timer_registration_notifications_are_retained_only_while_armed() {
        let signal = Arc::new(WorkerSignal::default());
        let waker = WorkerSignal::timer_registration_waker(&signal);

        waker.wake_by_ref();
        assert!(!signal.is_notified());

        signal.arm_timer_registration();
        waker.wake_by_ref();
        assert!(signal.is_notified());
        assert!(signal.wait(Duration::ZERO));

        waker.wake();
        assert!(!signal.is_notified());
    }

    #[test]
    fn armed_timer_registration_wake_is_retained_through_the_atomic_park_transition() {
        if is_mutation_testing() {
            return;
        }
        let signal = Arc::new(WorkerSignal::default());
        signal.arm_timer_registration();
        let (preparing_tx, preparing_rx) = mpsc::channel();
        let (park_tx, park_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = Arc::clone(&signal);
        let worker = thread::spawn(move || {
            let mut park_rx = Some(park_rx);
            let notified = waiter.wait_before_park(TEST_TIMEOUT.saturating_mul(2), || {
                if let Some(park_rx) = park_rx.take() {
                    preparing_tx.send(()).unwrap();
                    park_rx.recv().unwrap();
                }
            });
            done_tx.send(notified).unwrap();
        });
        preparing_rx.recv_timeout(TEST_TIMEOUT).unwrap();

        let waker = WorkerSignal::timer_registration_waker(&signal);
        let notifier = thread::spawn(move || waker.wake());
        park_tx.send(()).unwrap();

        assert!(done_rx.recv_timeout(TEST_TIMEOUT).unwrap());
        worker.join().unwrap();
        notifier.join().unwrap();
        assert!(!signal.is_notified());
    }

    #[test]
    fn remote_wake_releases_a_waiter() {
        if is_mutation_testing() {
            return;
        }
        let signal = Arc::new(WorkerSignal::default());
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = Arc::clone(&signal);
        let worker = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            waiter.wait(TEST_TIMEOUT.saturating_mul(2));
            done_tx.send(()).unwrap();
        });
        ready_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        WorkerSignal::waker(&signal).wake_by_ref();
        done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn wake_while_preparing_to_wait_is_retained() {
        if is_mutation_testing() {
            return;
        }
        let signal = Arc::new(WorkerSignal::default());
        let (preparing_tx, preparing_rx) = mpsc::channel();
        let (park_tx, park_rx) = mpsc::channel();
        let (wake_started_tx, wake_started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = Arc::clone(&signal);
        let worker = thread::spawn(move || {
            let mut rendezvous = Some((preparing_tx, park_rx));
            waiter.wait_before_park(TEST_TIMEOUT.saturating_mul(2), || {
                if let Some((preparing_tx, park_rx)) = rendezvous.take() {
                    preparing_tx.send(()).unwrap();
                    park_rx.recv().unwrap();
                }
            });
            done_tx.send(()).unwrap();
        });
        preparing_rx.recv_timeout(TEST_TIMEOUT).unwrap();

        let waker = WorkerSignal::waker(&signal);
        let notifier = thread::spawn(move || {
            wake_started_tx.send(()).unwrap();
            waker.wake();
        });
        wake_started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        park_tx.send(()).unwrap();

        done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        worker.join().unwrap();
        notifier.join().unwrap();
        assert!(!signal.state.lock().notified);
    }

    #[test]
    fn an_unnotified_wait_can_time_out() {
        if is_mutation_testing() {
            return;
        }
        let signal = WorkerSignal::default();
        let timeout = Duration::from_millis(20);
        let start = std::time::Instant::now();
        assert!(!signal.wait(timeout));
        assert!(start.elapsed() >= timeout);
        assert!(!signal.state.lock().notified);
    }

    #[test]
    fn wait_timeout_decision_requires_both_expiration_and_deadline() {
        let timeout = Duration::from_millis(20);
        assert!(!should_finish_wait(false, timeout, timeout));
        assert!(!should_finish_wait(true, timeout.saturating_sub(Duration::from_millis(1)), timeout));
        assert!(should_finish_wait(true, timeout, timeout));
    }

    #[test]
    fn spurious_wakes_reduce_the_remaining_wait_budget() {
        struct Script {
            elapsed: Duration,
            elapsed_after_wait: VecDeque<Duration>,
            wait_budgets: Vec<Duration>,
        }

        let timeout = Duration::from_millis(10);
        let mut script = Script {
            elapsed: Duration::from_millis(4),
            elapsed_after_wait: VecDeque::from([Duration::from_millis(7), Duration::from_millis(11)]),
            wait_budgets: Vec::new(),
        };

        wait_until_deadline(
            &mut script,
            timeout,
            |_| true,
            |script| script.elapsed,
            |script, budget| {
                script.wait_budgets.push(budget);
                script.elapsed = script.elapsed_after_wait.pop_front().unwrap();
                false
            },
        );

        assert_eq!(script.wait_budgets, [Duration::from_millis(6), Duration::from_millis(3)]);
        assert_eq!(script.elapsed, Duration::from_millis(11));
        assert!(script.elapsed_after_wait.is_empty());
    }
}
