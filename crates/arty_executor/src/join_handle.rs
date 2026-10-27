// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::pin::Pin;
use std::task;

use events_once::{LocalEventLake, PooledLocalReceiver};

use crate::{Shutdown, ShutdownCause};

/// A task failed to return its result.
///
/// A shutdown error is available through [`Error::source`] when shutdown started
/// because of an error.
pub enum JoinError {
    /// Executor shutdown cancelled the task or rejected its submission.
    Shutdown(ShutdownCause),
}

impl PartialEq for JoinError {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Shutdown(_), Self::Shutdown(_)))
    }
}

impl Eq for JoinError {}

impl JoinError {
    #[must_use]
    pub(crate) fn shutdown(cause: ShutdownCause) -> Self {
        Self::Shutdown(cause)
    }
}

impl Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("runtime is shutting down")
    }
}

impl Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("kind", &"shutdown")
            .finish_non_exhaustive()
    }
}

impl Error for JoinError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let Self::Shutdown(cause) = self;
        cause.as_error().map(|error| error as &(dyn Error + 'static))
    }
}

/// A handle that can be used to await the completion of a task and to receive its result `R`.
///
/// Completion produces `Ok(result)`. If executor shutdown cancels the task or rejects its
/// submission, awaiting the handle produces [`JoinError`].
///
/// A join handle may outlive its executor. Dropping the handle does not cancel the task.
///
/// # Panics
///
/// Panics if polled again after returning `Ready`.
#[derive(Debug)]
pub struct JoinHandle<R: 'static> {
    rx: PooledLocalReceiver<R>,
    shutdown: Shutdown,
}

impl<R: 'static> JoinHandle<R> {
    pub(crate) fn new(rx: PooledLocalReceiver<R>, shutdown: Shutdown) -> Self {
        Self { rx, shutdown }
    }

    pub(crate) fn disconnected(events: &LocalEventLake, shutdown: Shutdown) -> Self {
        let (tx, rx) = events.rent();
        drop(tx);
        Self::new(rx, shutdown)
    }
}

impl<R: 'static> Future for JoinHandle<R> {
    type Output = Result<R, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();
        if this.shutdown.is_triggered() {
            return task::Poll::Ready(Err(JoinError::shutdown(this.shutdown.cause())));
        }
        Pin::new(&mut this.rx)
            .poll(cx)
            .map(|result| result.map_err(|_disconnected| JoinError::shutdown(this.shutdown.cause())))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;

    // We expect these join handles to be single-threaded.
    assert_not_impl_any!(JoinHandle<u8>: Send, Sync);

    // We do not expect there to be anything that requires pinning the join handle.
    assert_impl_all!(JoinHandle<u8>: RefUnwindSafe, Unpin, UnwindSafe);

    #[test]
    fn shutdown_error_contract() {
        assert_impl_all!(JoinError: Error, Send, Sync, UnwindSafe, RefUnwindSafe);

        let error = JoinError::shutdown(ShutdownCause::default());

        assert!(matches!(&error, JoinError::Shutdown(cause) if cause.as_error().is_none()));
        assert_eq!(error.to_string(), "runtime is shutting down");
        assert!(format!("{error:?}").contains("shutdown"));
        assert!(error.source().is_none());
    }

    #[test]
    fn join_errors_compare_as_shutdown_failures() {
        let shutdown = JoinError::shutdown(ShutdownCause::default());
        let another_shutdown = JoinError::shutdown(ShutdownCause::default());

        assert_eq!(shutdown, another_shutdown);
    }
}
