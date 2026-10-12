// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::{Arc as StdArc, OnceLock};
use std::task::{Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use performables::arc::Arc;

/// A coalescing notification shared by task wakers and the command dispatcher.
#[derive(Debug)]
pub(crate) struct WorkerSignal {
    owner: Thread,
    primary_waker: OnceLock<Waker>,
}

impl WorkerSignal {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            owner: thread::current(),
            primary_waker: OnceLock::new(),
        })
    }

    pub(crate) fn wait(&self, timeout: Duration) {
        debug_assert_eq!(
            thread::current().id(),
            self.owner.id(),
            "a worker signal may only park its owning thread"
        );

        thread::park_timeout(timeout);
    }

    pub(crate) fn waker(signal: &Arc<Self>) -> Waker {
        // Wake requires a standard Arc; both handles share the same allocation.
        Waker::from(Arc::into_std_arc(Arc::clone(signal)))
    }

    pub(crate) fn set_primary_waker(&self, waker: Waker) {
        self.primary_waker
            .set(waker)
            .expect("a worker initializes at most one primary I/O driver");
    }

    fn notify(&self) {
        if let Some(primary_waker) = self.primary_waker.get() {
            primary_waker.wake_by_ref();
        }
        self.wake_waiter();
    }

    #[cfg_attr(test, mutants::skip)] // Removing the native notification strands the smoke test.
    fn wake_waiter(&self) {
        self.owner.unpark();
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

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding is not runtime behavior.
mod tests {
    use std::sync::mpsc;

    use testing_aids::{TEST_TIMEOUT, is_mutation_testing};

    use super::*;

    #[test]
    fn notification_before_wait_is_retained_and_consumed() {
        let signal = WorkerSignal::new();
        let waker = WorkerSignal::waker(&signal);
        waker.wake_by_ref();
        let start = std::time::Instant::now();
        signal.wait(TEST_TIMEOUT);
        assert!(start.elapsed() < TEST_TIMEOUT);

        waker.wake();
        let start = std::time::Instant::now();
        signal.wait(TEST_TIMEOUT);
        assert!(start.elapsed() < TEST_TIMEOUT);
    }

    #[test]
    fn remote_wake_releases_a_waiter() {
        if is_mutation_testing() {
            return;
        }
        let (signal_tx, signal_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let signal = WorkerSignal::new();
            signal_tx.send(Arc::clone(&signal)).unwrap();
            signal.wait(TEST_TIMEOUT.saturating_mul(2));
            done_tx.send(()).unwrap();
        });
        let signal = signal_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        WorkerSignal::waker(&signal).wake_by_ref();
        done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn an_unnotified_wait_can_time_out() {
        if is_mutation_testing() {
            return;
        }
        let signal = WorkerSignal::new();
        let timeout = Duration::from_millis(20);
        let start = std::time::Instant::now();
        signal.wait(timeout);
        assert!(start.elapsed() >= timeout);
    }
}
