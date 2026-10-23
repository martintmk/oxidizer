// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Public behavior for Mio registration, readiness, roles, and lifecycle.

#![allow(clippy::unwrap_used, reason = "test code")]
#![expect(clippy::panic, reason = "the polling helper reports a failed test precondition")]

use std::io;
use std::io::Write as _;
use std::net::{TcpListener, TcpStream as StdTcpStream, UdpSocket as StdUdpSocket};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::ThreadId;
use std::time::Duration;

use arty_io_core::{Cycle, Driver, DriverOptions, DriverRole, IoContext};
use arty_mio::{MioContext, MioDriver, MioOptions, Readiness, Registered, RegistrationClosed};
use mio::event::Source;
use mio::net::{TcpStream, UdpSocket};
use mio::{Interest, Registry, Token};
use static_assertions::{assert_impl_all, assert_not_impl_any};
use thread_aware::ThreadBuilder;
use thread_aware_core::ThreadAware;

assert_impl_all!(MioContext: Clone, Send, Sync, ThreadAware);
assert_impl_all!(MioOptions: Clone, Send, Sync, Default, std::fmt::Debug, Eq);
assert_not_impl_any!(MioOptions: Copy);
assert_impl_all!(MioDriver: Driver);
assert_impl_all!(Readiness: Copy, Send, Sync, std::fmt::Debug, Eq);
assert_impl_all!(RegistrationClosed: Send, Sync, std::fmt::Debug, std::fmt::Display, std::error::Error);
assert_impl_all!(Registered<TcpStream>: Send, Unpin);

#[test]
fn primary_driver_registers_and_reports_repeat_readiness() {
    let (io, mut driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let (receiver, sender) = udp_pair();
    let registration = register_primary(&io, &mut driver, receiver);

    sender.send(&[1]).unwrap();
    driver.execute_cycle(&mut Cycle::new(Duration::from_secs(1))).unwrap();
    assert!(
        expect_ready(poll_once(registration.readiness(Interest::READABLE), Waker::noop(),))
            .unwrap()
            .is_readable()
    );

    let mut buffer = [0; 8];
    assert_eq!(registration.source_mut().unwrap().recv(&mut buffer).unwrap(), 1);
    assert_eq!(
        registration.source_mut().unwrap().recv(&mut buffer).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(poll_once(registration.readiness(Interest::READABLE), Waker::noop(),).is_pending());

    sender.send(&[2]).unwrap();
    driver.execute_cycle(&mut Cycle::new(Duration::from_secs(1))).unwrap();
    assert!(
        expect_ready(poll_once(registration.readiness(Interest::READABLE), Waker::noop(),))
            .unwrap()
            .is_readable()
    );

    drop(registration);
    driver.shutdown().unwrap();
}

#[test]
fn readiness_before_first_poll_and_waiter_replacement_are_lossless() {
    let (io, mut driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let (receiver, sender) = udp_pair();
    let registration = register_primary(&io, &mut driver, receiver);

    sender.send(&[1]).unwrap();
    driver.execute_cycle(&mut Cycle::new(Duration::from_secs(1))).unwrap();
    assert!(
        expect_ready(poll_once(registration.readiness(Interest::READABLE), Waker::noop(),))
            .unwrap()
            .is_readable()
    );
    let mut buffer = [0; 8];
    assert_eq!(registration.source_mut().unwrap().recv(&mut buffer).unwrap(), 1);
    assert_eq!(
        registration.source_mut().unwrap().recv(&mut buffer).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );

    let first = Arc::new(WakeCounter::default());
    let second = Arc::new(WakeCounter::default());
    assert!(poll_once(registration.readiness(Interest::READABLE), &Waker::from(Arc::clone(&first)),).is_pending());
    assert!(poll_once(registration.readiness(Interest::READABLE), &Waker::from(Arc::clone(&second)),).is_pending());

    sender.send(&[2]).unwrap();
    driver.execute_cycle(&mut Cycle::new(Duration::from_secs(1))).unwrap();
    assert_eq!((first.count(), second.count()), (0, 1));

    drop(registration);
    driver.shutdown().unwrap();
}

#[test]
fn readable_and_writable_waiters_are_independent() {
    let (io, mut driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let (stream, mut peer) = tcp_pair();
    let mut future = pin!(io.register(stream, Interest::READABLE | Interest::WRITABLE));
    assert!(poll_pinned(future.as_mut(), Waker::noop()).is_pending());
    driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    let registration = expect_ready(poll_pinned(future.as_mut(), Waker::noop())).unwrap();
    let read_wake = Arc::new(WakeCounter::default());
    let write_wake = Arc::new(WakeCounter::default());

    assert!(poll_once(registration.readiness(Interest::READABLE), &Waker::from(Arc::clone(&read_wake)),).is_pending());
    assert!(
        expect_ready(poll_once(registration.readiness(Interest::WRITABLE), Waker::noop(),))
            .unwrap()
            .is_writable()
    );
    assert!(poll_once(registration.readiness(Interest::WRITABLE), &Waker::from(Arc::clone(&write_wake)),).is_pending());

    peer.write_all(&[1]).unwrap();
    driver.execute_cycle(&mut Cycle::new(Duration::from_secs(1))).unwrap();

    assert_eq!((read_wake.count(), write_wake.count()), (1, 0));
    assert!(
        expect_ready(poll_once(registration.readiness(Interest::READABLE), Waker::noop(),))
            .unwrap()
            .is_readable()
    );
    assert!(poll_once(registration.readiness(Interest::WRITABLE), Waker::noop(),).is_pending());

    drop(registration);
    driver.shutdown().unwrap();
}

#[test]
fn secondary_driver_registers_on_its_poller_and_wakes_the_owner() {
    let (owner_tx, owner_rx) = mpsc::channel();
    let owner_waker = Waker::from(Arc::new(ChannelWake(owner_tx)));
    let (io, mut driver) = driver(DriverRole::Secondary, owner_waker);
    let (receiver, sender) = udp_pair();
    let (response_tx, response_rx) = mpsc::channel();
    let mut future = pin!(io.register(receiver, Interest::READABLE));

    assert!(poll_pinned(future.as_mut(), &Waker::from(Arc::new(ChannelWake(response_tx))),).is_pending());
    response_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let registration = expect_ready(poll_pinned(future.as_mut(), Waker::noop())).unwrap();
    while owner_rx.try_recv().is_ok() {}

    driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    sender.send(&[1]).unwrap();
    owner_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();

    assert!(
        expect_ready(poll_once(registration.readiness(Interest::READABLE), Waker::noop(),))
            .unwrap()
            .is_readable()
    );
    drop(registration);
    driver.shutdown().unwrap();
}

#[test]
fn registry_work_runs_on_the_native_poller_thread_for_each_role() {
    let current = std::thread::current().id();
    let (primary_io, mut primary_driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let primary_source = recording_source();
    let (primary_tx, primary_rx) = mpsc::channel();
    let mut primary_future = pin!(primary_io.register(primary_source.with_reporter(primary_tx), Interest::READABLE,));
    assert!(poll_pinned(primary_future.as_mut(), Waker::noop()).is_pending());
    primary_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    let primary_registration = expect_ready(poll_pinned(primary_future.as_mut(), Waker::noop())).unwrap();
    assert_eq!(primary_rx.recv().unwrap(), current);

    let (secondary_io, secondary_driver) = driver(DriverRole::Secondary, Waker::noop().clone());
    let secondary_source = recording_source();
    let (secondary_tx, secondary_rx) = mpsc::channel();
    let secondary_registration =
        futures::executor::block_on(secondary_io.register(secondary_source.with_reporter(secondary_tx), Interest::READABLE)).unwrap();
    assert_ne!(secondary_rx.recv().unwrap(), current);

    drop(primary_registration);
    drop(secondary_registration);
    primary_driver.shutdown().unwrap();
    secondary_driver.shutdown().unwrap();
}

#[test]
fn drop_deregisters_on_the_native_poller_thread_for_each_role() {
    let current = std::thread::current().id();
    let (primary_io, mut primary_driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let primary_source = recording_source();
    let (primary_tx, primary_rx) = mpsc::channel();
    let mut primary_future = pin!(primary_io.register(primary_source.with_deregister_reporter(primary_tx), Interest::READABLE,));
    assert!(poll_pinned(primary_future.as_mut(), Waker::noop()).is_pending());
    primary_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    let primary_registration = expect_ready(poll_pinned(primary_future.as_mut(), Waker::noop())).unwrap();

    drop(primary_registration);
    primary_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    assert_eq!(primary_rx.recv().unwrap(), current);

    let (secondary_io, secondary_driver) = driver(DriverRole::Secondary, Waker::noop().clone());
    let secondary_source = recording_source();
    let (secondary_tx, secondary_rx) = mpsc::channel();
    let secondary_registration =
        futures::executor::block_on(secondary_io.register(secondary_source.with_deregister_reporter(secondary_tx), Interest::READABLE))
            .unwrap();

    drop(secondary_registration);
    assert_ne!(secondary_rx.recv().unwrap(), current);

    primary_driver.shutdown().unwrap();
    secondary_driver.shutdown().unwrap();
}

#[test]
fn context_clone_submits_from_another_thread_and_relocation_selects_destination_worker() {
    let (owner_tx, owner_rx) = mpsc::channel();
    let (mut io, mut driver) = driver(DriverRole::Primary, Waker::from(Arc::new(ChannelWake(owner_tx))));
    let external = io.clone();
    let source = recording_source();

    let registration = std::thread::scope(|scope| {
        let task = scope.spawn(move || futures::executor::block_on(external.register(source, Interest::READABLE)));
        owner_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
        task.join().unwrap().unwrap()
    });
    drop(registration);

    std::thread::scope(|scope| {
        let mut relocated = io.clone();
        scope
            .spawn(move || {
                let destination = ThreadBuilder::default().build(std::thread::current().id());
                relocated.relocate(None, &destination);
                let mut destination_driver = relocated
                    .driver(DriverOptions::new(destination, Waker::noop().clone(), DriverRole::Primary))
                    .unwrap();
                let source = recording_source();
                let (thread_tx, thread_rx) = mpsc::channel();
                let mut future = pin!(relocated.register(source.with_reporter(thread_tx), Interest::READABLE,));
                assert!(poll_pinned(future.as_mut(), Waker::noop()).is_pending());
                destination_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
                let registration = expect_ready(poll_pinned(future.as_mut(), Waker::noop())).unwrap();
                assert_eq!(thread_rx.recv().unwrap(), std::thread::current().id());
                drop(registration);
                destination_driver.shutdown().unwrap();
            })
            .join()
            .unwrap();
    });

    io.relocate(None, &ThreadBuilder::default().build(std::thread::current().id()));
    driver.shutdown().unwrap();
}

#[test]
fn relocation_keeps_existing_sources_on_their_original_poller() {
    let (io, mut driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let (old_source, old_sender) = udp_pair();
    let old_registration = register_primary(&io, &mut driver, old_source);
    assert!(poll_once(old_registration.readiness(Interest::READABLE), Waker::noop(),).is_pending());
    let (destination_ready_tx, destination_ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    std::thread::scope(|scope| {
        let mut relocated = io.clone();
        let destination = scope.spawn(move || {
            let thread = ThreadBuilder::default().build(std::thread::current().id());
            relocated.relocate(None, &thread);
            let mut destination_driver = relocated
                .driver(DriverOptions::new(thread, Waker::noop().clone(), DriverRole::Primary))
                .unwrap();
            let source = recording_source();
            let (thread_tx, thread_rx) = mpsc::channel();
            let mut future = pin!(relocated.register(source.with_reporter(thread_tx), Interest::READABLE,));
            assert!(poll_pinned(future.as_mut(), Waker::noop()).is_pending());
            destination_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
            let registration = expect_ready(poll_pinned(future.as_mut(), Waker::noop())).unwrap();
            assert_eq!(thread_rx.recv().unwrap(), std::thread::current().id());
            destination_ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(registration);
            destination_driver.shutdown().unwrap();
        });

        destination_ready_rx.recv().unwrap();
        old_sender.send(&[1]).unwrap();
        driver.execute_cycle(&mut Cycle::new(Duration::from_secs(1))).unwrap();
        assert!(
            expect_ready(poll_once(old_registration.readiness(Interest::READABLE), Waker::noop(),))
                .unwrap()
                .is_readable()
        );
        release_tx.send(()).unwrap();
        destination.join().unwrap();
    });

    drop(old_registration);
    driver.shutdown().unwrap();
}

#[test]
fn secondary_shutdown_deregisters_live_sources_on_the_poller_thread() {
    let current = std::thread::current().id();
    let (io, driver) = driver(DriverRole::Secondary, Waker::noop().clone());
    let (deregister_tx, deregister_rx) = mpsc::channel();
    let registration =
        futures::executor::block_on(io.register(recording_source().with_deregister_reporter(deregister_tx), Interest::READABLE)).unwrap();

    driver.shutdown().unwrap();

    assert_ne!(deregister_rx.recv().unwrap(), current);
    assert_eq!(registration.source().err().unwrap(), RegistrationClosed);
}

#[test]
fn shutdown_reports_a_source_guard_that_is_still_in_use() {
    let (io, mut driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let registration = register_primary(&io, &mut driver, recording_source());
    let (deregister_tx, deregister_rx) = mpsc::channel();
    let second = register_primary(&io, &mut driver, recording_source().with_deregister_reporter(deregister_tx));
    let source = registration.source().unwrap();

    let error = driver.shutdown().unwrap_err();

    assert_eq!(error.to_string(), "the registered source is in use during driver shutdown");
    assert_eq!(deregister_rx.recv().unwrap(), std::thread::current().id());
    drop(source);
    drop(registration);
    drop(second);
}

#[test]
fn drop_deregistration_failure_is_reported_by_each_driver_role() {
    let (primary_io, mut primary_driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let primary = register_primary(&primary_io, &mut primary_driver, FailingDeregisterSource);
    drop(primary);

    let error = primary_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap_err();
    assert_eq!(error.to_string(), "scripted deregistration failure");
    primary_driver.shutdown().unwrap();

    let (owner_tx, owner_rx) = mpsc::channel();
    let (secondary_io, mut secondary_driver) = driver(DriverRole::Secondary, Waker::from(Arc::new(ChannelWake(owner_tx))));
    let secondary = futures::executor::block_on(secondary_io.register(FailingDeregisterSource, Interest::READABLE)).unwrap();
    while owner_rx.try_recv().is_ok() {}
    drop(secondary);
    owner_rx.recv().unwrap();

    let error = secondary_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap_err();
    assert_eq!(error.to_string(), "scripted deregistration failure");
    secondary_driver.shutdown().unwrap();
}

#[test]
fn out_of_band_deregistration_bypasses_a_full_queue_for_each_role() {
    let current = std::thread::current().id();
    let (primary_io, mut primary_driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let (primary_tx, primary_rx) = mpsc::channel();
    let primary = register_primary(
        &primary_io,
        &mut primary_driver,
        recording_source().with_deregister_reporter(primary_tx),
    );
    let mut primary_queued = fill_registration_queue(&primary_io);
    assert!(!primary_queued.is_empty());

    drop(primary);
    primary_driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    assert_eq!(primary_rx.recv().unwrap(), current);
    primary_driver.shutdown().unwrap();
    primary_queued.clear();

    let (secondary_io, secondary_driver) = driver(DriverRole::Secondary, Waker::noop().clone());
    let (secondary_tx, secondary_rx) = mpsc::channel();
    let secondary =
        futures::executor::block_on(secondary_io.register(recording_source().with_deregister_reporter(secondary_tx), Interest::READABLE))
            .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut blocker = Box::pin(secondary_io.register(PausingRegistrationSource { entered_tx, release_rx }, Interest::READABLE));
    assert!(poll_pinned(blocker.as_mut(), Waker::noop()).is_pending());
    entered_rx.recv().unwrap();
    let mut secondary_queued = fill_registration_queue(&secondary_io);
    assert!(!secondary_queued.is_empty());

    drop(secondary);
    release_tx.send(()).unwrap();
    assert_ne!(secondary_rx.recv().unwrap(), current);
    secondary_driver.shutdown().unwrap();
    secondary_queued.clear();
    drop(blocker);
}

#[test]
fn full_submission_queue_rejects_work_and_shutdown_cancels_the_rest() {
    let (io, driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let mut queued = Vec::new();
    let queue_full = loop {
        let mut future = Box::pin(io.register(NeverRegisteredSource, Interest::READABLE));
        match poll_pinned(future.as_mut(), Waker::noop()) {
            Poll::Pending => queued.push(future),
            Poll::Ready(result) => break result.err().unwrap(),
        }
    };
    assert_eq!(queue_full.kind(), io::ErrorKind::WouldBlock);

    driver.shutdown().unwrap();

    for mut future in queued {
        assert_eq!(
            expect_ready(poll_pinned(future.as_mut(), Waker::noop())).err().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

#[test]
fn driver_waker_is_latched_distinct_coalescing_and_inert_after_drop() {
    let owner_waker = Waker::from(Arc::new(WakeCounter::default()));
    let (_io, mut driver) = driver(DriverRole::Primary, owner_waker.clone());
    let driver_waker = driver.waker();

    assert!(!driver_waker.will_wake(&owner_waker));
    driver_waker.wake_by_ref();
    driver_waker.wake_by_ref();
    driver.execute_cycle(&mut Cycle::new(Duration::MAX)).unwrap();

    driver.shutdown().unwrap();
    driver_waker.wake_by_ref();
}

#[test]
fn shutdown_cancels_queued_and_live_registration_futures() {
    let (io, mut driver) = driver(DriverRole::Primary, Waker::noop().clone());
    let (deregister_tx, deregister_rx) = mpsc::channel();
    let live_source = recording_source().with_deregister_reporter(deregister_tx);
    let live = register_primary(&io, &mut driver, live_source);
    let live_wake = Arc::new(WakeCounter::default());
    assert!(poll_once(live.readiness(Interest::READABLE), &Waker::from(Arc::clone(&live_wake)),).is_pending());

    let queued_source = recording_source();
    let mut queued = pin!(io.register(queued_source, Interest::READABLE));
    let queued_wake = Arc::new(WakeCounter::default());
    assert!(poll_pinned(queued.as_mut(), &Waker::from(Arc::clone(&queued_wake))).is_pending());

    driver.shutdown().unwrap();

    assert_eq!(deregister_rx.recv().unwrap(), std::thread::current().id());
    assert_eq!(live.source_mut().err().unwrap(), RegistrationClosed);
    assert_eq!((live_wake.count(), queued_wake.count()), (1, 1));
    assert_eq!(
        expect_ready(poll_once(live.readiness(Interest::READABLE), Waker::noop(),)),
        Err(RegistrationClosed)
    );
    assert_eq!(
        expect_ready(poll_pinned(queued.as_mut(), Waker::noop())).err().unwrap().kind(),
        io::ErrorKind::BrokenPipe
    );

    let later_source = recording_source();
    let error = futures::executor::block_on(io.register(later_source, Interest::READABLE))
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

fn driver(role: DriverRole, owner_waker: Waker) -> (MioContext, MioDriver) {
    let mut io = MioContext::context(MioOptions::default());
    let worker = ThreadBuilder::default().build(std::thread::current().id());
    io.relocate(None, &worker);
    let driver = io.driver(DriverOptions::new(worker, owner_waker, role)).unwrap();
    (io, driver)
}

fn register_primary<S>(io: &MioContext, driver: &mut MioDriver, source: S) -> Registered<S>
where
    S: Source + Send + 'static,
{
    let mut future = pin!(io.register(source, Interest::READABLE));
    assert!(poll_pinned(future.as_mut(), Waker::noop()).is_pending());
    driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
    expect_ready(poll_pinned(future.as_mut(), Waker::noop())).unwrap()
}

fn udp_pair() -> (UdpSocket, StdUdpSocket) {
    let receiver = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let sender = StdUdpSocket::bind("127.0.0.1:0").unwrap();
    sender.connect(receiver.local_addr().unwrap()).unwrap();
    (receiver, sender)
}

fn tcp_pair() -> (TcpStream, StdTcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (stream, _) = listener.accept().unwrap();
    stream.set_nonblocking(true).unwrap();
    (TcpStream::from_std(stream), peer)
}

fn recording_source() -> RecordingSource {
    RecordingSource {
        reporter: None,
        deregister_reporter: None,
    }
}

type QueueFuture<'a> = Pin<Box<dyn Future<Output = io::Result<Registered<QueueSource>>> + 'a>>;

fn fill_registration_queue(io: &MioContext) -> Vec<QueueFuture<'_>> {
    let mut queued = Vec::new();
    loop {
        let mut future: QueueFuture<'_> = Box::pin(io.register(QueueSource, Interest::READABLE));
        match poll_pinned(future.as_mut(), Waker::noop()) {
            Poll::Pending => queued.push(future),
            Poll::Ready(result) => {
                assert_eq!(result.err().unwrap().kind(), io::ErrorKind::WouldBlock);
                return queued;
            }
        }
    }
}

fn poll_once<F>(future: F, waker: &Waker) -> Poll<F::Output>
where
    F: Future,
{
    let mut future = pin!(future);
    poll_pinned(future.as_mut(), waker)
}

fn poll_pinned<F>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output>
where
    F: Future + ?Sized,
{
    future.poll(&mut Context::from_waker(waker))
}

fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("the future must be ready"),
    }
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
}

struct ChannelWake(mpsc::Sender<()>);

impl Wake for ChannelWake {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
}

struct RecordingSource {
    reporter: Option<mpsc::Sender<ThreadId>>,
    deregister_reporter: Option<mpsc::Sender<ThreadId>>,
}

impl RecordingSource {
    fn with_reporter(mut self, reporter: mpsc::Sender<ThreadId>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    fn with_deregister_reporter(mut self, reporter: mpsc::Sender<ThreadId>) -> Self {
        self.deregister_reporter = Some(reporter);
        self
    }
}

impl Source for RecordingSource {
    fn register(&mut self, registry: &Registry, token: Token, interests: Interest) -> io::Result<()> {
        if let Some(reporter) = &self.reporter {
            let _ = reporter.send(std::thread::current().id());
        }
        let _ = (registry, token, interests);
        Ok(())
    }

    fn reregister(&mut self, registry: &Registry, token: Token, interests: Interest) -> io::Result<()> {
        let _ = (registry, token, interests);
        Ok(())
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        if let Some(reporter) = &self.deregister_reporter {
            let _ = reporter.send(std::thread::current().id());
        }
        let _ = registry;
        Ok(())
    }
}

struct NeverRegisteredSource;

impl Source for NeverRegisteredSource {
    fn register(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        unreachable!("the test never executes queued registration commands")
    }

    fn reregister(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        unreachable!("the test never registers this source")
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        unreachable!("the test never registers this source")
    }
}

struct QueueSource;

impl Source for QueueSource {
    fn register(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        Ok(())
    }

    fn reregister(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        Ok(())
    }
}

struct FailingDeregisterSource;

impl Source for FailingDeregisterSource {
    fn register(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        Ok(())
    }

    fn reregister(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        Err(io::Error::other("scripted deregistration failure"))
    }
}

struct PausingRegistrationSource {
    entered_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
}

impl Source for PausingRegistrationSource {
    fn register(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        self.entered_tx.send(()).unwrap();
        self.release_rx.recv().unwrap();
        Ok(())
    }

    fn reregister(&mut self, _registry: &Registry, _token: Token, _interests: Interest) -> io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        Ok(())
    }
}
