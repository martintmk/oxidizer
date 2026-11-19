// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::task::Poll;

/// Observes each top-level poll of one task.
///
/// The executor calls `poll_start` and `poll_end` as a pair. Implementations
/// must always return normally. The executor does not contain observer panics.
pub trait TaskObserver: 'static {
    /// Runs immediately before the executor polls the task.
    fn poll_start(&mut self);

    /// Runs immediately after the executor polls the task.
    ///
    /// `outcome` is `Poll::Pending` if the task remains pending. It is
    /// `Poll::Ready(())` if the task completes or panics.
    fn poll_end(&mut self, outcome: Poll<()>);
}

impl TaskObserver for () {
    #[inline]
    fn poll_start(&mut self) {}

    #[inline]
    fn poll_end(&mut self, _outcome: Poll<()>) {}
}

impl<O: TaskObserver> TaskObserver for Option<O> {
    fn poll_start(&mut self) {
        if let Some(observer) = self {
            observer.poll_start();
        }
    }

    fn poll_end(&mut self, outcome: Poll<()>) {
        if let Some(observer) = self {
            observer.poll_end(outcome);
        }
    }
}
