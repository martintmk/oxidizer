// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::future::poll_fn;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::{fmt, io};

use mio::event::Source;
use mio::{Interest, Registry, Token};
use parking_lot::{Mutex as SourceMutex, MutexGuard as SourceMutexGuard};

use crate::readiness::{Readiness, RegistrationClosed};
use crate::worker::{ControlCommand, DriverCommand, WorkerState};

pub(crate) struct Response<T> {
    inner: Mutex<ResponseState<T>>,
}

enum ResponseState<T> {
    Pending(Option<Waker>),
    Ready(T),
}

impl<T> Default for Response<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(ResponseState::Pending(None)),
        }
    }
}

impl<T> Response<T> {
    pub(crate) fn complete(&self, value: T) {
        let waiter = {
            let mut state = self
                .inner
                .lock()
                .expect("response completion does not call code that can panic while the response lock is held");
            match std::mem::replace(&mut *state, ResponseState::Ready(value)) {
                ResponseState::Pending(waiter) => waiter,
                ResponseState::Ready(_) => {
                    unreachable!("a driver command completes its response exactly once")
                }
            }
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    pub(crate) async fn wait(&self) -> T {
        poll_fn(|context| {
            let mut state = self
                .inner
                .lock()
                .expect("response polling does not call code that can panic while the response lock is held");
            match &mut *state {
                ResponseState::Ready(_) => {
                    let ResponseState::Ready(value) = std::mem::replace(&mut *state, ResponseState::Pending(None)) else {
                        unreachable!("the response state was matched as ready")
                    };
                    Poll::Ready(value)
                }
                ResponseState::Pending(waiter) => {
                    if waiter.as_ref().is_none_or(|waiter| !waiter.will_wake(context.waker())) {
                        *waiter = Some(context.waker().clone());
                    }
                    Poll::Pending
                }
            }
        })
        .await
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
}

/// An owned Mio source registered with one Arty Mio driver.
///
/// Dropping this value sends deregistration to the native poll owner. The
/// source drops on that thread after the deregistration attempt. Mio binds a
/// source to one `Poll` for its registered lifetime, so this type does not
/// expose an operation that moves the source to another registry.
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
        self.worker.enqueue_control(Box::new(DeregisterCommand {
            token: self.token,
            entry: Arc::clone(&self.entry),
        }));
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

pub(crate) struct RegisterCommand<S>
where
    S: Source + Send + 'static,
{
    source: S,
    interest: Interest,
    response: Arc<Response<io::Result<Registered<S>>>>,
}

impl<S> RegisterCommand<S>
where
    S: Source + Send + 'static,
{
    pub(crate) fn new(source: S, interest: Interest, response: Arc<Response<io::Result<Registered<S>>>>) -> Self {
        Self {
            source,
            interest,
            response,
        }
    }
}

impl<S> DriverCommand for RegisterCommand<S>
where
    S: Source + Send + 'static,
{
    fn execute(self: Box<Self>, registry: &Registry, worker: &Arc<WorkerState>) {
        let token = match worker.allocate_token() {
            Ok(token) => token,
            Err(error) => {
                self.response.complete(Err(error));
                return;
            }
        };
        let state = Arc::new(RegistrationState::default());
        let source = Arc::new(SourceCell::new(self.source));
        let entry = Arc::new(RegistrationEntry::new(
            Arc::clone(&state),
            Arc::clone(&source) as Arc<dyn NativeRegistration>,
        ));
        if let Err(error) = worker.add_registration(token, Arc::clone(&entry)) {
            self.response.complete(Err(error));
            return;
        }
        let registration = {
            let mut source = source.source.lock();
            registry.register(
                source.as_mut().expect("the source is present before native registration"),
                token,
                self.interest,
            )
        };
        if let Err(error) = registration {
            worker.remove_registration(token);
            entry.close();
            self.response.complete(Err(error));
            return;
        }
        if !worker.registration_is_active(token, &entry) {
            if let Err(error) = entry.deregister(registry) {
                worker.record_failure(error);
            }
            self.response.complete(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the Arty Mio driver closed while it registered the source",
            )));
            return;
        }
        self.response
            .complete(Ok(Registered::new(source, Arc::clone(worker), token, entry)));
    }

    fn cancel(self: Box<Self>) {
        self.response.complete(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "the Arty Mio driver closed before it registered the source",
        )));
    }
}

struct DeregisterCommand {
    token: Token,
    entry: Arc<RegistrationEntry>,
}

impl ControlCommand for DeregisterCommand {
    fn execute(self: Box<Self>, registry: &Registry, worker: &Arc<WorkerState>) {
        worker.remove_registration(self.token);
        if let Err(error) = self.entry.deregister(registry) {
            worker.record_failure(error);
        }
    }

    fn cancel(self: Box<Self>) {
        self.entry.close();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::task::Waker;

    use mio::event::Source;
    use mio::{Interest, Poll, Registry, Token};

    use super::{RegisterCommand, Response};
    use crate::driver::PollWake;
    use crate::worker::{DriverCommand, WorkerState};

    #[test]
    fn shutdown_wins_against_an_in_flight_registration() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (deregister_tx, deregister_rx) = mpsc::channel();
        let response = Arc::new(Response::default());
        let command = RegisterCommand::new(
            BlockingSource {
                entered_tx,
                release_rx,
                deregister_tx,
            },
            Interest::READABLE,
            Arc::clone(&response),
        );
        let (worker_tx, worker_rx) = mpsc::channel();

        let poll_thread = std::thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let poll_waker = Arc::new(PollWake::new(poll.registry()).unwrap());
            let worker = Arc::new(WorkerState::new(std::thread::current().id(), poll_waker, Waker::noop().clone()));
            worker_tx.send(Arc::clone(&worker)).unwrap();
            Box::new(command).execute(poll.registry(), &worker);
        });

        let worker = worker_rx.recv().unwrap();
        entered_rx.recv().unwrap();
        worker.begin_close();
        worker.close_registrations();
        release_tx.send(()).unwrap();
        poll_thread.join().unwrap();

        assert_eq!(
            futures::executor::block_on(response.wait()).err().unwrap().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert_eq!(deregister_rx.recv().unwrap(), worker.thread_id);
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
