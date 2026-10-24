// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Wake, Waker};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{fmt, io};

use arty_io_core::{Cycle, Driver, DriverError, DriverOptions, DriverRole};
use mio::event::Event;
use mio::{Events, Poll, Registry};

use crate::registration::RegistrationEntry;
use crate::worker::{ContextState, DRIVER_WAKE_TOKEN, WorkerState};

const COMPLETION_BUDGET: usize = 256;
const EVENT_CAPACITY: usize = 1_024;
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(1);

/// A Mio poll driver for one Arty worker.
///
/// This driver owns the [`Poll`] lifecycle and all driver-side registration
/// state. A primary driver polls on the Arty worker. A secondary driver owns a
/// helper thread that waits for Mio events.
///
/// Shutdown closes readiness for each live registration. It does not take or
/// drop the registered sources.
pub struct MioDriver {
    inner: Option<DriverInner>,
}

impl fmt::Debug for MioDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MioDriver")
            .field("active", &self.inner.is_some())
            .finish_non_exhaustive()
    }
}

impl MioDriver {
    pub(crate) fn new(state: Arc<ContextState>, options: &DriverOptions) -> Result<(Self, Arc<WorkerState>), DriverError> {
        if options.thread().id() != thread::current().id() {
            return Err(DriverError::from_message(
                "the caller must create an Arty Mio driver on the worker named by DriverOptions",
            ));
        }

        let (mode, poll_waker, registry) = match options.role() {
            DriverRole::Primary => {
                let poll = Poll::new().map_err(DriverError::from_source)?;
                let poll_waker = Arc::new(PollWake::new(poll.registry()).map_err(DriverError::from_source)?);
                let registry = poll.registry().try_clone().map_err(DriverError::from_source)?;
                (DriverMode::Primary(PrimaryDriver::new(poll)), poll_waker, registry)
            }
            DriverRole::Secondary => {
                let (driver, poll_waker, registry) = SecondaryDriver::new().map_err(DriverError::from_source)?;
                (DriverMode::Secondary(driver), poll_waker, registry)
            }
        };

        let worker = Arc::new(WorkerState::new(options.thread().id(), registry, options.owner_waker().clone()));
        if let DriverMode::Secondary(driver) = &mode {
            driver.attach(Arc::clone(&worker));
        }
        if let Err(error) = state.install(Arc::clone(&worker)) {
            worker.close_registrations();
            let cleanup = match mode {
                DriverMode::Primary(_) => Ok(()),
                DriverMode::Secondary(driver) => driver.shutdown(&poll_waker, &worker),
            };
            poll_waker.deactivate();
            cleanup?;
            return Err(DriverError::from_source(error));
        }

        let driver = Self {
            inner: Some(DriverInner {
                context: state,
                worker: Arc::clone(&worker),
                poll_waker,
                mode,
            }),
        };
        Ok((driver, worker))
    }

    fn inner(&self) -> &DriverInner {
        self.inner
            .as_ref()
            .expect("Driver::shutdown consumes the Mio driver before its inner state is removed")
    }

    fn inner_mut(&mut self) -> &mut DriverInner {
        self.inner
            .as_mut()
            .expect("Driver::shutdown consumes the Mio driver before its inner state is removed")
    }
}

impl Driver for MioDriver {
    fn waker(&self) -> Waker {
        Waker::from(Arc::clone(&self.inner().poll_waker))
    }

    fn execute_cycle(&mut self, cycle: &mut Cycle) -> Result<(), DriverError> {
        self.inner_mut().execute_cycle(cycle)
    }

    fn shutdown(mut self) -> Result<(), DriverError> {
        self.inner
            .take()
            .expect("the Driver trait calls shutdown at most once because shutdown consumes self")
            .shutdown()
    }
}

impl Drop for MioDriver {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.shutdown();
        }
    }
}

struct DriverInner {
    context: Arc<ContextState>,
    worker: Arc<WorkerState>,
    poll_waker: Arc<PollWake>,
    mode: DriverMode,
}

impl DriverInner {
    fn execute_cycle(&mut self, cycle: &Cycle) -> Result<(), DriverError> {
        match &mut self.mode {
            DriverMode::Primary(driver) => driver.execute_cycle(&self.worker, cycle.max_wait()),
            DriverMode::Secondary(driver) => driver.execute_cycle(&self.worker),
        }
    }

    fn shutdown(self) -> Result<(), DriverError> {
        self.context.remove(&self.worker);
        self.worker.close_registrations();
        match self.mode {
            DriverMode::Primary(driver) => {
                let control_error = self.worker.take_failure().map(DriverError::from_source);
                self.poll_waker.deactivate();
                drop(driver);
                match control_error {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }
            DriverMode::Secondary(driver) => {
                let result = driver.shutdown(&self.poll_waker, &self.worker);
                self.poll_waker.deactivate();
                result
            }
        }
    }
}

enum DriverMode {
    Primary(PrimaryDriver),
    Secondary(SecondaryDriver),
}

struct PrimaryDriver {
    poll: Poll,
    events: Events,
    pending: VecDeque<Event>,
    batch: Vec<Event>,
    resolved: Vec<Option<Arc<RegistrationEntry>>>,
}

impl PrimaryDriver {
    fn new(poll: Poll) -> Self {
        Self {
            poll,
            events: Events::with_capacity(EVENT_CAPACITY),
            pending: VecDeque::new(),
            batch: Vec::with_capacity(COMPLETION_BUDGET),
            resolved: Vec::with_capacity(COMPLETION_BUDGET),
        }
    }

    fn execute_cycle(&mut self, worker: &Arc<WorkerState>, max_wait: Duration) -> Result<(), DriverError> {
        if let Some(error) = worker.take_failure() {
            return Err(DriverError::from_source(error));
        }

        let processed_pending = self.dispatch_pending(worker, COMPLETION_BUDGET);
        if !self.pending.is_empty() {
            worker.wake_owner();
            return Ok(());
        }

        let timeout = if processed_pending != 0 {
            Some(Duration::ZERO)
        } else if max_wait == Duration::MAX {
            None
        } else {
            Some(max_wait)
        };
        match self.poll.poll(&mut self.events, timeout) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(()),
            Err(error) => return Err(DriverError::from_source(error)),
        }

        self.pending
            .extend(self.events.iter().filter(|event| event.token() != DRIVER_WAKE_TOKEN).cloned());
        self.dispatch_pending(worker, COMPLETION_BUDGET);
        if !self.pending.is_empty() || self.events.iter().count() == self.events.capacity() {
            worker.wake_owner();
        }
        Ok(())
    }

    fn dispatch_pending(&mut self, worker: &WorkerState, budget: usize) -> usize {
        let count = budget.min(self.pending.len());
        if count != 0 {
            self.batch.clear();
            self.batch.extend(self.pending.drain(..count));
            worker.dispatch_batch(&self.batch, &mut self.resolved);
        }
        count
    }
}

struct SecondaryDriver {
    shared: Arc<SecondaryShared>,
    finished: std::sync::mpsc::Receiver<()>,
    thread: JoinHandle<()>,
    events: Vec<Event>,
    resolved: Vec<Option<Arc<RegistrationEntry>>>,
}

impl SecondaryDriver {
    fn new() -> io::Result<(Self, Arc<PollWake>, Registry)> {
        let shared = Arc::new(SecondaryShared::default());
        let thread_shared = Arc::clone(&shared);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (finished_tx, finished) = std::sync::mpsc::channel();
        let thread = thread::Builder::new().name("arty-mio-secondary".to_owned()).spawn(move || {
            let result = (|| -> io::Result<()> {
                let mut poll = Poll::new()?;
                let poll_waker = Arc::new(PollWake::new(poll.registry())?);
                let registry = poll.registry().try_clone()?;
                started_tx
                    .send(Ok((Arc::clone(&poll_waker), registry)))
                    .map_err(|_send_error| io::Error::other("the Arty worker stopped secondary driver creation"))?;
                let mut events = Events::with_capacity(EVENT_CAPACITY);
                loop {
                    thread_shared.worker()?;
                    if thread_shared.shutdown_requested() {
                        break;
                    }

                    match poll.poll(&mut events, None) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => return Err(error),
                    }

                    let mut state = thread_shared.state();
                    state
                        .events
                        .extend(events.iter().filter(|event| event.token() != DRIVER_WAKE_TOKEN).cloned());
                    let has_events = !state.events.is_empty();
                    state.paused = has_events;
                    drop(state);
                    if has_events {
                        thread_shared.worker()?.wake_owner();
                        thread_shared.wait_until_resumed();
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                thread_shared.fail(error);
                if let Some(worker) = thread_shared.try_worker() {
                    worker.wake_owner();
                }
            }
            let _ = finished_tx.send(());
        })?;

        let (poll_waker, registry) = match started_rx.recv() {
            Ok(Ok(started)) => started,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(io::Error::other("the secondary Arty Mio poll thread stopped during creation")),
        };
        Ok((
            Self {
                shared,
                finished,
                thread,
                events: Vec::with_capacity(COMPLETION_BUDGET),
                resolved: Vec::with_capacity(COMPLETION_BUDGET),
            },
            poll_waker,
            registry,
        ))
    }

    fn attach(&self, worker: Arc<WorkerState>) {
        self.shared.set_worker(worker);
    }

    fn execute_cycle(&mut self, worker: &WorkerState) -> Result<(), DriverError> {
        let (failure, resume, has_events) = drain_secondary_events(&self.shared, &mut self.events);
        if resume {
            self.shared.ready.notify_one();
        }
        if let Some(error) = failure {
            return Err(DriverError::from_source(error));
        }
        if let Some(error) = worker.take_failure() {
            return Err(DriverError::from_source(error));
        }
        worker.dispatch_batch(&self.events, &mut self.resolved);
        if has_events {
            worker.wake_owner();
        }
        Ok(())
    }

    fn shutdown(self, poll_waker: &PollWake, worker: &WorkerState) -> Result<(), DriverError> {
        self.shared.set_shutdown();
        poll_waker.wake_poll();
        let result = match self.finished.recv_timeout(SHUTDOWN_LIMIT) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => self
                .thread
                .join()
                .map_err(|_panic| DriverError::from_message("the secondary Arty Mio poll thread panicked")),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(DriverError::from_message(
                "the secondary Arty Mio poll thread did not stop within one second",
            )),
        };
        result?;
        if let Some(error) = worker.take_failure() {
            return Err(DriverError::from_source(error));
        }
        match self.shared.take_failure() {
            Some(error) => Err(DriverError::from_source(error)),
            None => Ok(()),
        }
    }
}

fn drain_secondary_events(shared: &SecondaryShared, events: &mut Vec<Event>) -> (Option<io::Error>, bool, bool) {
    events.clear();
    let mut state = shared.state();
    let count = COMPLETION_BUDGET.min(state.events.len());
    events.extend(state.events.drain(..count));
    let failure = state.failure.take();
    let has_events = !state.events.is_empty();
    let resume = !has_events;
    if resume {
        state.paused = false;
    }
    (failure, resume, has_events)
}

#[derive(Default)]
struct SecondaryShared {
    inner: Mutex<SecondaryState>,
    ready: Condvar,
}

impl SecondaryShared {
    fn state(&self) -> std::sync::MutexGuard<'_, SecondaryState> {
        self.inner
            .lock()
            .expect("secondary poller state changes do not call user code while the state lock is held")
    }

    fn set_worker(&self, worker: Arc<WorkerState>) {
        let mut state = self.state();
        state.worker = Some(worker);
        self.ready.notify_all();
    }

    fn worker(&self) -> io::Result<Arc<WorkerState>> {
        let mut state = self.state();
        while state.worker.is_none() && !state.shutdown {
            state = self
                .ready
                .wait(state)
                .expect("only the secondary poller waits for its worker attachment");
        }
        state
            .worker
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "the secondary Arty Mio driver is closed"))
    }

    fn try_worker(&self) -> Option<Arc<WorkerState>> {
        self.state().worker.clone()
    }

    fn wait_until_resumed(&self) {
        let mut state = self.state();
        while state.paused && !state.shutdown {
            state = self
                .ready
                .wait(state)
                .expect("only the secondary poller waits while queued events are pending");
        }
    }

    fn fail(&self, error: io::Error) {
        let mut state = self.state();
        if state.failure.is_none() {
            state.failure = Some(error);
        }
    }

    fn take_failure(&self) -> Option<io::Error> {
        self.state().failure.take()
    }

    fn set_shutdown(&self) {
        let mut state = self.state();
        state.shutdown = true;
        self.ready.notify_all();
    }

    fn shutdown_requested(&self) -> bool {
        self.state().shutdown
    }
}

#[derive(Default)]
struct SecondaryState {
    worker: Option<Arc<WorkerState>>,
    events: VecDeque<Event>,
    failure: Option<io::Error>,
    paused: bool,
    shutdown: bool,
}

pub(crate) struct PollWake {
    inner: Mutex<Option<mio::Waker>>,
}

impl PollWake {
    pub(crate) fn new(registry: &Registry) -> io::Result<Self> {
        mio::Waker::new(registry, DRIVER_WAKE_TOKEN).map(|inner| Self {
            inner: Mutex::new(Some(inner)),
        })
    }

    pub(crate) fn wake_poll(&self) {
        if let Some(waker) = self
            .inner
            .lock()
            .expect("poll wake does not call user code while the native waker lock is held")
            .as_ref()
        {
            let _ = waker.wake();
        }
    }

    fn deactivate(&self) {
        self.inner
            .lock()
            .expect("poll wake deactivation only removes the native waker")
            .take();
    }
}

impl Wake for PollWake {
    fn wake(self: Arc<Self>) {
        self.wake_poll();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wake_poll();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};
    use std::time::Duration;

    use mio::{Events, Poll, Token};

    use super::{COMPLETION_BUDGET, EVENT_CAPACITY, PrimaryDriver, SecondaryDriver, SecondaryShared, drain_secondary_events};
    use crate::worker::WorkerState;

    #[test]
    fn secondary_background_failure_becomes_driver_error() {
        let (mut driver, poll_waker, registry) = SecondaryDriver::new().unwrap();
        let worker = Arc::new(WorkerState::new(std::thread::current().id(), registry, Waker::noop().clone()));
        driver.attach(Arc::clone(&worker));
        driver.shared.fail(std::io::Error::other("scripted poll failure"));

        let error = driver.execute_cycle(&worker).unwrap_err();
        assert_eq!(error.to_string(), "scripted poll failure");

        worker.close_registrations();
        driver.shutdown(&poll_waker, &worker).unwrap();
        poll_waker.deactivate();
    }

    #[test]
    fn primary_full_capacity_batch_yields_after_each_budget() {
        let poll = Poll::new().unwrap();
        let owner = Arc::new(WakeCounter::default());
        let worker = Arc::new(WorkerState::new(
            std::thread::current().id(),
            poll.registry().try_clone().unwrap(),
            Waker::from(Arc::clone(&owner)),
        ));
        let mut driver = PrimaryDriver::new(poll);
        driver.pending.extend(std::iter::repeat_n(wake_event(), EVENT_CAPACITY));

        for remaining in [768, 512, 256, 0] {
            driver.execute_cycle(&worker, Duration::ZERO).unwrap();
            assert_eq!(driver.pending.len(), remaining);
        }

        assert_eq!(owner.count(), 3);
    }

    #[test]
    fn secondary_full_capacity_batch_resumes_after_the_last_budget() {
        let shared = SecondaryShared::default();
        {
            let mut state = shared.state();
            state.events.extend(std::iter::repeat_n(wake_event(), EVENT_CAPACITY));
            state.paused = true;
        }
        let mut events = Vec::with_capacity(COMPLETION_BUDGET);

        for remaining in [768, 512, 256] {
            let (_failure, resume, has_events) = drain_secondary_events(&shared, &mut events);
            assert_eq!(events.len(), COMPLETION_BUDGET);
            assert!(has_events);
            assert!(!resume);
            assert_eq!(shared.state().events.len(), remaining);
        }

        let (_failure, resume, has_events) = drain_secondary_events(&shared, &mut events);
        assert_eq!(events.len(), COMPLETION_BUDGET);
        assert!(!has_events);
        assert!(resume);
        assert!(!shared.state().paused);
    }

    fn wake_event() -> mio::event::Event {
        let mut poll = Poll::new().unwrap();
        let waker = mio::Waker::new(poll.registry(), Token(0)).unwrap();
        let mut events = Events::with_capacity(1);
        waker.wake().unwrap();
        poll.poll(&mut events, Some(Duration::ZERO)).unwrap();
        events.iter().next().unwrap().clone()
    }

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl WakeCounter {
        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}
