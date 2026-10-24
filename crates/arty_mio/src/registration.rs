// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::{fmt, io};

use mio::event::Source;
use mio::{Interest, Token};

use crate::readiness::{Readiness, RegistrationClosed};
use crate::worker::WorkerState;

struct RegistryOperation<'a>(&'a WorkerState);

impl Drop for RegistryOperation<'_> {
    fn drop(&mut self) {
        self.0.finish_registry_operation();
    }
}

pub(crate) struct RegistrationState {
    inner: Mutex<RegistrationInner>,
    closed: AtomicBool,
}

impl Default for RegistrationState {
    fn default() -> Self {
        Self {
            inner: Mutex::new(RegistrationInner::default()),
            closed: AtomicBool::new(false),
        }
    }
}

pub(crate) struct RegistrationEntry {
    state: Arc<RegistrationState>,
}

impl RegistrationEntry {
    fn new(state: Arc<RegistrationState>) -> Self {
        Self { state }
    }

    pub(crate) fn notify(&self, event: &mio::event::Event) {
        self.state.notify(event);
    }

    pub(crate) fn close(&self) {
        self.state.close();
    }
}

#[derive(Default)]
struct RegistrationInner {
    readable: Readiness,
    writable: Readiness,
    read_waiter: Option<Waker>,
    write_waiter: Option<Waker>,
    closed: bool,
}

impl RegistrationState {
    pub(crate) fn notify(&self, event: &mio::event::Event) {
        let (read_waiter, write_waiter) = {
            let mut inner = self
                .inner
                .lock()
                .expect("readiness notification does not call code that can panic while the readiness lock is held");
            if inner.closed {
                return;
            }
            let wake_read = event.is_readable()
                || event.is_error()
                || event.is_read_closed()
                || event.is_write_closed()
                || event.is_priority()
                || event.is_aio()
                || event.is_lio();
            let wake_write = event.is_writable()
                || event.is_error()
                || event.is_read_closed()
                || event.is_write_closed()
                || event.is_aio()
                || event.is_lio();
            if wake_read {
                inner.readable.merge(event);
            }
            if wake_write {
                inner.writable.merge(event);
            }
            (
                wake_read.then(|| inner.read_waiter.take()).flatten(),
                wake_write.then(|| inner.write_waiter.take()).flatten(),
            )
        };
        if let Some(waiter) = read_waiter {
            waiter.wake();
        }
        if let Some(waiter) = write_waiter {
            waiter.wake();
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let (read_waiter, write_waiter) = {
            let mut inner = self
                .inner
                .lock()
                .expect("registration close does not call code that can panic while the readiness lock is held");
            inner.closed = true;
            (inner.read_waiter.take(), inner.write_waiter.take())
        };
        if let Some(waiter) = read_waiter {
            waiter.wake();
        }
        if let Some(waiter) = write_waiter {
            waiter.wake();
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn poll_read(&self, waker: &Waker) -> Poll<Result<Readiness, RegistrationClosed>> {
        let mut inner = self
            .inner
            .lock()
            .expect("read readiness polling does not call code that can panic while the readiness lock is held");
        let readiness = std::mem::take(&mut inner.readable);
        if !readiness.is_empty() {
            return Poll::Ready(Ok(readiness));
        }
        if inner.closed {
            return Poll::Ready(Err(RegistrationClosed));
        }
        if inner.read_waiter.as_ref().is_none_or(|waiter| !waiter.will_wake(waker)) {
            inner.read_waiter = Some(waker.clone());
        }
        Poll::Pending
    }

    fn poll_write(&self, waker: &Waker) -> Poll<Result<Readiness, RegistrationClosed>> {
        let mut inner = self
            .inner
            .lock()
            .expect("write readiness polling does not call code that can panic while the readiness lock is held");
        let readiness = std::mem::take(&mut inner.writable);
        if !readiness.is_empty() {
            return Poll::Ready(Ok(readiness));
        }
        if inner.closed {
            return Poll::Ready(Err(RegistrationClosed));
        }
        if inner.write_waiter.as_ref().is_none_or(|waiter| !waiter.will_wake(waker)) {
            inner.write_waiter = Some(waker.clone());
        }
        Poll::Pending
    }
}

/// An owned Mio source registered with one Arty Mio driver.
///
/// This type gives its owner direct source access without a lock. Driver
/// shutdown closes readiness and the native poller. The owner keeps the source
/// until this value drops.
///
/// While the driver is active, dropping this value removes the source
/// registration through a thread-safe clone of the native registry. The source
/// then drops on the caller thread. Mio binds a source to one `Poll` for its
/// registered lifetime, so this type does not expose an operation that moves
/// the source to another registry.
pub struct Registered<S>
where
    S: Source + Send + 'static,
{
    source: S,
    worker: Arc<WorkerState>,
    token: Token,
    entry: Arc<RegistrationEntry>,
}

impl<S> fmt::Debug for Registered<S>
where
    S: Source + Send + fmt::Debug + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registered").field("token", &self.token).finish_non_exhaustive()
    }
}

impl<S> Registered<S>
where
    S: Source + Send + 'static,
{
    fn new(source: S, worker: Arc<WorkerState>, token: Token, entry: Arc<RegistrationEntry>) -> Self {
        Self {
            source,
            worker,
            token,
            entry,
        }
    }

    /// Returns the Mio token assigned to this source.
    #[must_use]
    pub const fn token(&self) -> Token {
        self.token
    }

    /// Returns a shared reference to the registered source.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if the driver is closed.
    pub fn source(&self) -> Result<&S, RegistrationClosed> {
        if self.entry.state.is_closed() {
            Err(RegistrationClosed)
        } else {
            Ok(&self.source)
        }
    }

    /// Returns an exclusive reference to the registered source.
    ///
    /// Read or write until the operation returns
    /// [`WouldBlock`](io::ErrorKind::WouldBlock). Then wait for new readiness.
    ///
    /// A source operation that starts before driver shutdown can finish after
    /// shutdown starts. The driver never accesses or drops the source.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if the driver is closed.
    pub fn source_mut(&mut self) -> Result<&mut S, RegistrationClosed> {
        if self.entry.state.is_closed() {
            Err(RegistrationClosed)
        } else {
            Ok(&mut self.source)
        }
    }

    /// Polls for read readiness or driver shutdown.
    ///
    /// Call this method only after a read operation returns
    /// [`WouldBlock`](io::ErrorKind::WouldBlock). Readiness is a hint and can
    /// be spurious. This method consumes the accumulated read readiness. It
    /// does not consume write readiness.
    ///
    /// The registration stores only the latest read waker. A later poll can
    /// replace the waker from an earlier poll.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if the driver closes before it reports
    /// more read readiness.
    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "the poll API follows Future::poll and accepts the caller's task context"
    )]
    pub fn poll_read_ready(&self, context: &mut Context<'_>) -> Poll<Result<Readiness, RegistrationClosed>> {
        self.entry.state.poll_read(context.waker())
    }

    /// Polls for write readiness or driver shutdown.
    ///
    /// Call this method only after a write operation returns
    /// [`WouldBlock`](io::ErrorKind::WouldBlock). Readiness is a hint and can
    /// be spurious. This method consumes the accumulated write readiness. It
    /// does not consume read readiness.
    ///
    /// The registration stores only the latest write waker. A later poll can
    /// replace the waker from an earlier poll.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if the driver closes before it reports
    /// more write readiness.
    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "the poll API follows Future::poll and accepts the caller's task context"
    )]
    pub fn poll_write_ready(&self, context: &mut Context<'_>) -> Poll<Result<Readiness, RegistrationClosed>> {
        self.entry.state.poll_write(context.waker())
    }
}

impl<S> Drop for Registered<S>
where
    S: Source + Send + 'static,
{
    fn drop(&mut self) {
        if !self.worker.begin_deregistration(self.token, &self.entry) {
            self.entry.close();
            return;
        }
        let _operation = RegistryOperation(&self.worker);
        if let Err(error) = self.worker.registry().deregister(&mut self.source) {
            self.worker.record_failure(error);
        }
        self.entry.close();
    }
}

pub(crate) fn register<S>(mut source: S, interest: Interest, worker: Arc<WorkerState>) -> io::Result<Registered<S>>
where
    S: Source + Send + 'static,
{
    let token = worker.allocate_token()?;
    let state = Arc::new(RegistrationState::default());
    let entry = Arc::new(RegistrationEntry::new(state));
    worker.begin_registration(token, Arc::clone(&entry))?;
    let operation = RegistryOperation(&worker);
    if let Err(error) = worker.registry().register(&mut source, token, interest) {
        worker.finish_registration(token, &entry, false);
        entry.close();
        return Err(error);
    }
    if !worker.finish_registration(token, &entry, true) {
        if let Err(error) = worker.registry().deregister(&mut source) {
            worker.record_failure(error);
        }
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "the Arty Mio driver closed while it registered the source",
        ));
    }
    drop(operation);
    Ok(Registered::new(source, worker, token, entry))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::task::Waker;

    use mio::event::Source;
    use mio::{Interest, Poll, Registry, Token};

    use super::register;
    use crate::worker::WorkerState;

    #[test]
    fn shutdown_wins_against_an_in_flight_registration() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (deregister_tx, deregister_rx) = mpsc::channel();
        let poll = Poll::new().unwrap();
        let worker = Arc::new(WorkerState::new(
            std::thread::current().id(),
            poll.registry().try_clone().unwrap(),
            Waker::noop().clone(),
        ));
        let registration_worker = Arc::clone(&worker);
        let registration_thread = std::thread::spawn(move || {
            register(
                BlockingSource {
                    entered_tx,
                    release_rx,
                    deregister_tx,
                },
                Interest::READABLE,
                registration_worker,
            )
        });
        let registration_thread_id = registration_thread.thread().id();

        entered_rx.recv().unwrap();
        let shutdown_worker = Arc::clone(&worker);
        let shutdown_thread = std::thread::spawn(move || shutdown_worker.close_registrations());
        while !worker.registrations_are_closed() {
            std::thread::yield_now();
        }
        release_tx.send(()).unwrap();

        assert_eq!(
            registration_thread.join().unwrap().err().unwrap().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        shutdown_thread.join().unwrap();
        assert_eq!(deregister_rx.recv().unwrap(), registration_thread_id);
    }

    struct BlockingSource {
        entered_tx: mpsc::Sender<()>,
        release_rx: mpsc::Receiver<()>,
        deregister_tx: mpsc::Sender<std::thread::ThreadId>,
    }

    impl Source for BlockingSource {
        fn register(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> std::io::Result<()> {
            self.entered_tx.send(()).unwrap();
            self.release_rx.recv().unwrap();
            Ok(())
        }

        fn reregister(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> std::io::Result<()> {
            Ok(())
        }

        fn deregister(&mut self, _registry: &Registry) -> std::io::Result<()> {
            self.deregister_tx.send(std::thread::current().id()).unwrap();
            Ok(())
        }
    }
}
