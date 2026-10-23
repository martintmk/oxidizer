// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::future::poll_fn;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::{fmt, io};

use mio::event::Source;
use mio::{Interest, Registry, Token};
use parking_lot::{Mutex as SourceMutex, MutexGuard as SourceMutexGuard};

use crate::readiness::{Readiness, RegistrationClosed};
use crate::worker::WorkerState;

struct RegistryOperation<'a>(&'a WorkerState);

impl Drop for RegistryOperation<'_> {
    fn drop(&mut self) {
        self.0.finish_registry_operation();
    }
}

#[derive(Default)]
pub(crate) struct RegistrationState {
    inner: Mutex<RegistrationInner>,
}

pub(crate) struct RegistrationEntry {
    state: Arc<RegistrationState>,
    source: Arc<dyn NativeRegistration>,
}

impl RegistrationEntry {
    fn new(state: Arc<RegistrationState>, source: Arc<dyn NativeRegistration>) -> Self {
        Self { state, source }
    }

    pub(crate) fn notify(&self, event: &mio::event::Event) {
        self.state.notify(event);
    }

    pub(crate) fn close(&self) {
        self.state.close();
    }

    pub(crate) fn deregister(&self, registry: &Registry) -> io::Result<()> {
        let result = self.source.deregister(registry);
        self.state.close();
        result
    }

    pub(crate) fn try_deregister(&self, registry: &Registry) -> io::Result<()> {
        self.source.try_deregister(registry)?;
        self.state.close();
        Ok(())
    }
}

trait NativeRegistration: Send + Sync {
    fn deregister(&self, registry: &Registry) -> io::Result<()>;
    fn try_deregister(&self, registry: &Registry) -> io::Result<()>;
}

struct SourceCell<S> {
    source: SourceMutex<Option<S>>,
}

impl<S> SourceCell<S> {
    fn new(source: S) -> Self {
        Self {
            source: SourceMutex::new(Some(source)),
        }
    }
}

impl<S> NativeRegistration for SourceCell<S>
where
    S: Source + Send + 'static,
{
    fn deregister(&self, registry: &Registry) -> io::Result<()> {
        let source = self.source.lock().take();
        if let Some(mut source) = source {
            registry.deregister(&mut source)?;
        }
        Ok(())
    }

    fn try_deregister(&self, registry: &Registry) -> io::Result<()> {
        let mut source = self
            .source
            .try_lock()
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "the registered source is in use during driver shutdown"))?;
        if let Some(mut source) = source.take() {
            registry.deregister(&mut source)?;
        }
        Ok(())
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

    fn poll(&self, interest: Interest, waker: &Waker) -> Poll<Result<Readiness, RegistrationClosed>> {
        let mut inner = self
            .inner
            .lock()
            .expect("readiness polling does not call code that can panic while the readiness lock is held");
        let mut readiness = Readiness::default();
        if interest.is_readable() {
            readiness.combine(std::mem::take(&mut inner.readable));
        }
        if interest.is_writable() {
            readiness.combine(std::mem::take(&mut inner.writable));
        }
        if !readiness.is_empty() {
            return Poll::Ready(Ok(readiness));
        }
        if inner.closed {
            return Poll::Ready(Err(RegistrationClosed));
        }
        if interest.is_readable() && inner.read_waiter.as_ref().is_none_or(|waiter| !waiter.will_wake(waker)) {
            inner.read_waiter = Some(waker.clone());
        }
        if interest.is_writable() && inner.write_waiter.as_ref().is_none_or(|waiter| !waiter.will_wake(waker)) {
            inner.write_waiter = Some(waker.clone());
        }
        Poll::Pending
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
/// Dropping this value removes the source registration through a thread-safe
/// clone of the native registry. The source then drops on the caller thread.
/// Mio binds a source to one `Poll` for its registered lifetime, so this type
/// does not expose an operation that moves the source to another registry.
pub struct Registered<S>
where
    S: Source + Send + 'static,
{
    source: Arc<SourceCell<S>>,
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
    fn new(source: Arc<SourceCell<S>>, worker: Arc<WorkerState>, token: Token, entry: Arc<RegistrationEntry>) -> Self {
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
    /// Drop the returned guard before an await point. The guard prevents
    /// driver shutdown from releasing the source.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if driver shutdown already released the
    /// source.
    pub fn source(&self) -> Result<SourceRef<'_, S>, RegistrationClosed> {
        SourceRef::new(self.source.source.lock())
    }

    /// Returns a mutable reference to the registered source.
    ///
    /// Read or write until the operation returns
    /// [`WouldBlock`](io::ErrorKind::WouldBlock). Then wait for new readiness.
    /// Drop the returned guard before an await point. The guard prevents
    /// driver shutdown from releasing the source.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if driver shutdown already released the
    /// source.
    pub fn source_mut(&self) -> Result<SourceMut<'_, S>, RegistrationClosed> {
        SourceMut::new(self.source.source.lock())
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

    /// Waits for the requested readiness or driver shutdown.
    ///
    /// Readiness is a hint and can be spurious. Multiple events accumulate
    /// until this future returns them. Reading readiness and writing readiness
    /// have separate latest-waker slots. Taking one direction does not clear
    /// the other direction.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationClosed`] if the driver closes before it reports
    /// the requested readiness.
    pub fn readiness(&self, interest: Interest) -> impl Future<Output = Result<Readiness, RegistrationClosed>> + Send + 'static {
        let state = Arc::clone(&self.entry.state);
        async move { poll_fn(|context| state.poll(interest, context.waker())).await }
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
        if let Err(error) = self.entry.deregister(self.worker.registry()) {
            self.worker.record_failure(error);
        }
    }
}

/// Shared access to a registered Mio source.
pub struct SourceRef<'a, S> {
    guard: SourceMutexGuard<'a, Option<S>>,
}

impl<S> fmt::Debug for SourceRef<'_, S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<'a, S> SourceRef<'a, S> {
    fn new(guard: SourceMutexGuard<'a, Option<S>>) -> Result<Self, RegistrationClosed> {
        if guard.is_some() {
            Ok(Self { guard })
        } else {
            Err(RegistrationClosed)
        }
    }
}

impl<S> Deref for SourceRef<'_, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("SourceRef is constructed only while the source is present")
    }
}

/// Exclusive access to a registered Mio source.
pub struct SourceMut<'a, S> {
    guard: SourceMutexGuard<'a, Option<S>>,
}

impl<S> fmt::Debug for SourceMut<'_, S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<'a, S> SourceMut<'a, S> {
    fn new(guard: SourceMutexGuard<'a, Option<S>>) -> Result<Self, RegistrationClosed> {
        if guard.is_some() {
            Ok(Self { guard })
        } else {
            Err(RegistrationClosed)
        }
    }
}

impl<S> Deref for SourceMut<'_, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("SourceMut is constructed only while the source is present")
    }
}

impl<S> DerefMut for SourceMut<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .as_mut()
            .expect("SourceMut is constructed only while the source is present")
    }
}

pub(crate) fn register<S>(source: S, interest: Interest, worker: Arc<WorkerState>) -> io::Result<Registered<S>>
where
    S: Source + Send + 'static,
{
    let token = worker.allocate_token()?;
    let state = Arc::new(RegistrationState::default());
    let source = Arc::new(SourceCell::new(source));
    let entry = Arc::new(RegistrationEntry::new(
        Arc::clone(&state),
        Arc::clone(&source) as Arc<dyn NativeRegistration>,
    ));
    worker.begin_registration(token, Arc::clone(&entry))?;
    let operation = RegistryOperation(&worker);
    let registration = {
        let mut source = source.source.lock();
        worker.registry().register(
            source.as_mut().expect("the source is present before native registration"),
            token,
            interest,
        )
    };
    if let Err(error) = registration {
        worker.finish_registration(token, &entry, false);
        entry.close();
        return Err(error);
    }
    if !worker.finish_registration(token, &entry, true) {
        if let Err(error) = entry.deregister(worker.registry()) {
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
