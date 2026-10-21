// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::atomic::{AtomicBool, Ordering};

use performables::arc::Arc;

/// Shared, monotonic shutdown request without an associated data payload.
///
/// Channels, locks, and thread joins synchronize runtime state independently.
/// This flag only controls admission and elects the first shutdown requester.
#[derive(Debug, Clone, Default)]
pub(crate) struct ShutdownSignal(Arc<AtomicBool>);

impl ShutdownSignal {
    #[must_use]
    pub(crate) fn shutdown_requested(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Returns `true` only for the first request. Does not wake workers.
    pub(crate) fn request_shutdown(&self) -> bool {
        !self.0.swap(true, Ordering::Relaxed)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn requests_are_shared_and_cannot_be_reset() {
        let signal = ShutdownSignal::default();
        let other = signal.clone();
        assert!(!signal.shutdown_requested());

        assert!(std::thread::spawn(move || other.request_shutdown()).join().unwrap());

        assert!(signal.shutdown_requested());
        assert!(!signal.request_shutdown());
        assert!(signal.shutdown_requested());
    }

    #[test]
    fn concurrent_requests_elect_one_shutdown_owner() {
        let signal = ShutdownSignal::default();
        let threads: [_; 8] = std::array::from_fn(|_| {
            let signal = signal.clone();
            std::thread::spawn(move || signal.request_shutdown())
        });

        let owners = threads.into_iter().map(|thread| usize::from(thread.join().unwrap())).sum::<usize>();

        assert_eq!(owners, 1);
        assert!(signal.shutdown_requested());
    }
}
