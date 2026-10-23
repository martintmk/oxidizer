// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::future::poll_fn;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use arty::task::Scheduler;
use arty_mio::{MioContext, Registered, RegistrationClosed};
use fetch_options::SocketOptions;
use futures::future::{Either, select};
use http::uri::Scheme;
use http_extensions::{HttpError, Result};
use layered::Service;
use mio::Interest;
use mio::net::TcpStream;
use seatbelt::RecoveryInfo;
use socket2::SockRef;
use templated_uri::BaseUri;
use tick::Clock;

use crate::error::{
    AttemptFailure, AttemptPhase, LABEL_DNS, LABEL_RUNTIME, LABEL_SCHEME_NOT_ALLOWED, connect_failures, io_error, unavailable,
};
use crate::{MioConnectionInfo, MioStream};

const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(300);

type AttemptFuture = Pin<Box<dyn Future<Output = std::result::Result<MioStream, AttemptFailure>> + Send + 'static>>;
type ResolveFuture = Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>>> + Send + 'static>>;

trait Resolver {
    fn resolve(&self, host: String, port: u16) -> ResolveFuture;
}

trait Dialer {
    type Stream;

    fn connect(
        &self,
        address: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<Self::Stream, AttemptFailure>> + Send + 'static>>;
}

/// Configuration for [`MioConnector`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct MioConnectorOptions {
    /// Socket settings for every outbound TCP connection.
    pub socket: SocketOptions,
}

impl MioConnectorOptions {
    /// Sets the socket settings for every outbound TCP connection.
    #[must_use]
    pub fn socket(mut self, socket: SocketOptions) -> Self {
        self.socket = socket;
        self
    }
}

/// A cloneable service that opens Arty Mio TCP streams for Hyper.
///
/// Create this value on its target Arty worker. Clones keep the same worker
/// association. Fetch creates one connector for each worker.
#[derive(Clone, Debug)]
pub struct MioConnector {
    context: MioContext,
    scheduler: Scheduler,
    clock: Clock,
    options: MioConnectorOptions,
}

impl MioConnector {
    /// Creates a connector for one Arty worker.
    #[must_use]
    pub const fn new(context: MioContext, scheduler: Scheduler, clock: Clock, options: MioConnectorOptions) -> Self {
        Self {
            context,
            scheduler,
            clock,
            options,
        }
    }
}

impl Service<BaseUri> for MioConnector {
    type Out = Result<MioStream>;

    async fn execute(&self, input: BaseUri) -> Self::Out {
        validate_scheme(&input)?;
        let port = input.try_effective_port()?;
        let addresses = SystemResolver {
            scheduler: self.scheduler.clone(),
        }
        .resolve(input.host().to_owned(), port)
        .await?;
        race_connections(
            &self.clock,
            addresses,
            &MioDialer {
                context: self.context.clone(),
                options: self.options.socket,
            },
        )
        .await
    }
}

fn validate_scheme(input: &BaseUri) -> Result<()> {
    let scheme = input.scheme();
    if scheme == &Scheme::HTTP || scheme == &Scheme::HTTPS {
        return Ok(());
    }

    Err(HttpError::other(
        format!("the connector does not support the '{scheme}' scheme; only http and https are supported"),
        RecoveryInfo::never(),
        LABEL_SCHEME_NOT_ALLOWED,
    ))
}

#[derive(Clone)]
struct SystemResolver {
    scheduler: Scheduler,
}

impl Resolver for SystemResolver {
    fn resolve(&self, host: String, port: u16) -> ResolveFuture {
        let scheduler = self.scheduler.clone();
        Box::pin(async move {
            let result = scheduler
                .spawn_blocking(move || {
                    let addresses = (host.as_str(), port).to_socket_addrs()?.collect::<Vec<_>>();
                    if addresses.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrNotAvailable,
                            "DNS resolution returned no socket addresses",
                        ));
                    }
                    Ok(order_addresses(addresses))
                })
                .await
                .map_err(|error| unavailable(error, LABEL_RUNTIME))?;

            result.map_err(|error| io_error(error, LABEL_DNS))
        })
    }
}

fn order_addresses(addresses: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut unique = Vec::with_capacity(addresses.len());
    for address in addresses {
        if !unique.contains(&address) {
            unique.push(address);
        }
    }

    let prefer_ipv6 = unique.first().is_some_and(SocketAddr::is_ipv6);
    let mut ipv4 = unique.iter().copied().filter(SocketAddr::is_ipv4);
    let mut ipv6 = unique.iter().copied().filter(SocketAddr::is_ipv6);
    let mut ordered = Vec::with_capacity(unique.len());

    loop {
        let first = if prefer_ipv6 { ipv6.next() } else { ipv4.next() };
        let second = if prefer_ipv6 { ipv4.next() } else { ipv6.next() };
        if first.is_none() && second.is_none() {
            break;
        }
        ordered.extend(first);
        ordered.extend(second);
    }

    ordered
}

async fn race_connections<D>(clock: &Clock, addresses: Vec<SocketAddr>, dialer: &D) -> Result<D::Stream>
where
    D: Dialer + Sync,
    D::Stream: Send,
{
    if addresses.is_empty() {
        return Err(io_error(
            io::Error::new(io::ErrorKind::AddrNotAvailable, "DNS resolution returned no socket addresses"),
            LABEL_DNS,
        ));
    }

    let mut attempts = Attempts::new(addresses.len());
    let mut failures = std::iter::repeat_with(|| None)
        .take(addresses.len())
        .collect::<Vec<Option<AttemptFailure>>>();
    let mut next = 0;

    launch(&mut attempts, next, addresses[next], dialer);
    next += 1;
    let mut delay = Box::pin(clock.delay(HAPPY_EYEBALLS_DELAY));

    loop {
        if next == addresses.len() {
            let Some((index, result)) = attempts.next().await else {
                return Err(finish_failures(failures));
            };
            match result {
                Ok(stream) => return Ok(stream),
                Err(error) => failures[index] = Some(error),
            }
            continue;
        }

        let (attempt, remaining_delay) = {
            match select(Box::pin(attempts.next()), delay).await {
                Either::Left((attempt, remaining_delay)) => (Some(attempt), Some(remaining_delay)),
                Either::Right(((), pending_attempt)) => {
                    drop(pending_attempt);
                    (None, None)
                }
            }
        };

        if let Some(Some((index, result))) = attempt {
            match result {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    failures[index] = Some(error);
                    launch(&mut attempts, next, addresses[next], dialer);
                    next += 1;
                    drop(remaining_delay);
                    delay = Box::pin(clock.delay(HAPPY_EYEBALLS_DELAY));
                }
            }
        } else {
            launch(&mut attempts, next, addresses[next], dialer);
            next += 1;
            delay = Box::pin(clock.delay(HAPPY_EYEBALLS_DELAY));
        }
    }
}

fn launch<D>(attempts: &mut Attempts<D::Stream>, index: usize, address: SocketAddr, dialer: &D)
where
    D: Dialer,
{
    attempts.insert(index, dialer.connect(address));
}

#[derive(Clone)]
struct MioDialer {
    context: MioContext,
    options: SocketOptions,
}

impl Dialer for MioDialer {
    type Stream = MioStream;

    fn connect(&self, address: SocketAddr) -> AttemptFuture {
        let context = self.context.clone();
        let options = self.options;
        Box::pin(async move {
            let stream = TcpStream::connect(address).map_err(|error| AttemptFailure::new(address, AttemptPhase::Connect, error))?;
            apply_buffer_options(&stream, options).map_err(|error| AttemptFailure::new(address, AttemptPhase::SocketOption, error))?;

            let registration = context
                .register(stream, Interest::READABLE | Interest::WRITABLE)
                .await
                .map_err(|error| AttemptFailure::new(address, AttemptPhase::Registration, error))?;

            let info = complete_connect(&registration)
                .await
                .map_err(|error| AttemptFailure::new(address, AttemptPhase::Connect, error))?;

            if let Some(no_delay) = options.no_delay {
                registration
                    .source()
                    .map_err(registration_closed)
                    .and_then(|source| source.set_nodelay(no_delay))
                    .map_err(|error| AttemptFailure::new(address, AttemptPhase::SocketOption, error))?;
            }

            Ok(MioStream::new(registration, info))
        })
    }
}

fn apply_buffer_options(stream: &TcpStream, options: SocketOptions) -> io::Result<()> {
    let socket = SockRef::from(stream);
    if let Some(size) = options.receive_buffer_size {
        socket.set_recv_buffer_size(size as usize)?;
    }
    if let Some(size) = options.send_buffer_size {
        socket.set_send_buffer_size(size as usize)?;
    }
    Ok(())
}

async fn complete_connect(registration: &Registered<TcpStream>) -> io::Result<MioConnectionInfo> {
    loop {
        registration.readiness(Interest::WRITABLE).await.map_err(registration_closed)?;

        loop {
            let source = registration.source().map_err(registration_closed)?;
            let connect_error = match source.take_error() {
                Ok(None) => None,
                Ok(Some(error)) | Err(error) => Some(error),
            };
            if let Some(error) = connect_error {
                match connect_error_action(&error) {
                    ConnectErrorAction::Retry => continue,
                    ConnectErrorAction::Wait => break,
                    ConnectErrorAction::Fail => return Err(error),
                }
            }

            match source.peer_addr() {
                Ok(peer_addr) => {
                    let local_addr = source.local_addr()?;
                    return Ok(MioConnectionInfo::new(local_addr, peer_addr));
                }
                Err(error) => match connect_error_action(&error) {
                    ConnectErrorAction::Retry => {}
                    ConnectErrorAction::Wait => break,
                    ConnectErrorAction::Fail => return Err(error),
                },
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ConnectErrorAction {
    Retry,
    Wait,
    Fail,
}

fn connect_error_action(error: &io::Error) -> ConnectErrorAction {
    if error.kind() == io::ErrorKind::Interrupted {
        ConnectErrorAction::Retry
    } else if is_connect_pending(error) {
        ConnectErrorAction::Wait
    } else {
        ConnectErrorAction::Fail
    }
}

fn is_connect_pending(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::NotConnected) || is_in_progress(error)
}

#[cfg(unix)]
fn is_in_progress(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::EINPROGRESS)
}

#[cfg(windows)]
fn is_in_progress(error: &io::Error) -> bool {
    const WSAEINPROGRESS: i32 = 10_036;
    error.raw_os_error() == Some(WSAEINPROGRESS)
}

#[cfg(not(any(unix, windows)))]
const fn is_in_progress(_error: &io::Error) -> bool {
    false
}

fn registration_closed(error: RegistrationClosed) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error)
}

fn finish_failures(failures: Vec<Option<AttemptFailure>>) -> HttpError {
    connect_failures(failures.into_iter().flatten().collect())
}

type BoxAttempt<T> = Pin<Box<dyn Future<Output = std::result::Result<T, AttemptFailure>> + Send + 'static>>;

struct Attempts<T> {
    slots: Vec<Option<BoxAttempt<T>>>,
    active: usize,
}

impl<T> Attempts<T> {
    fn new(capacity: usize) -> Self {
        Self {
            slots: std::iter::repeat_with(|| None).take(capacity).collect(),
            active: 0,
        }
    }

    fn insert(&mut self, index: usize, future: BoxAttempt<T>) {
        self.slots[index] = Some(future);
        self.active += 1;
    }

    async fn next(&mut self) -> Option<(usize, std::result::Result<T, AttemptFailure>)> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, std::result::Result<T, AttemptFailure>)>> {
        if self.active == 0 {
            return Poll::Ready(None);
        }

        for (index, slot) in self.slots.iter_mut().enumerate() {
            let Some(future) = slot else {
                continue;
            };
            if let Poll::Ready(result) = future.as_mut().poll(cx) {
                *slot = None;
                self.active -= 1;
                return Poll::Ready(Some((index, result)));
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::task::Waker;

    use future_assert::FutureTestExt as _;
    use ohno::Labeled as _;
    use seatbelt::{Recovery as _, RecoveryKind};
    use tick::ClockControl;

    use super::*;

    #[test]
    fn address_order_interleaves_families_and_removes_duplicates() {
        let v4_a = "127.0.0.1:80".parse().unwrap();
        let v4_b = "127.0.0.2:80".parse().unwrap();
        let v6_a = "[::1]:80".parse().unwrap();
        let v6_b = "[::2]:80".parse().unwrap();

        assert_eq!(order_addresses(vec![v6_a, v6_b, v4_a, v4_b, v6_a]), vec![v6_a, v4_a, v6_b, v4_b]);
        assert_eq!(order_addresses(vec![v4_a, v4_b, v6_a, v6_b, v4_a]), vec![v4_a, v6_a, v4_b, v6_b]);
    }

    #[test]
    fn connect_pending_recognizes_platform_in_progress() {
        #[cfg(unix)]
        let error = io::Error::from_raw_os_error(libc::EINPROGRESS);
        #[cfg(windows)]
        let error = io::Error::from_raw_os_error(10_036);
        #[cfg(not(any(unix, windows)))]
        let error = io::Error::from(io::ErrorKind::WouldBlock);

        assert_eq!(connect_error_action(&error), ConnectErrorAction::Wait);
        assert_eq!(
            connect_error_action(&io::Error::from(io::ErrorKind::NotConnected)),
            ConnectErrorAction::Wait
        );
        assert_eq!(
            connect_error_action(&io::Error::from(io::ErrorKind::Interrupted)),
            ConnectErrorAction::Retry
        );
        assert_eq!(
            connect_error_action(&io::Error::from(io::ErrorKind::ConnectionRefused)),
            ConnectErrorAction::Fail
        );
    }

    #[test]
    fn resolver_seam_returns_scripted_addresses_without_dns() {
        let expected = vec!["192.0.2.1:443".parse().unwrap(), "[2001:db8::1]:443".parse().unwrap()];
        let resolver = ScriptedResolver::success(expected.clone());

        assert_eq!(resolver.resolve("unused.invalid".to_owned(), 443).unwrap_ready().unwrap(), expected);
        assert_eq!(resolver.calls(), vec![("unused.invalid".to_owned(), 443)]);
    }

    #[test]
    fn happy_eyeballs_launches_second_address_after_300_ms() {
        let first = ManualAttempt::new(AttemptPhase::Connect);
        let dialer = ScriptedDialer::new(vec![AttemptScript::Pending(first.future()), AttemptScript::Success("second")]);
        let control = ClockControl::new();
        let clock = control.to_clock();
        let addresses = addresses();
        let mut future = std::pin::pin!(race_connections(&clock, addresses.clone(), &dialer));

        assert!(poll(future.as_mut()).is_pending());
        assert_eq!(dialer.launches(), vec![addresses[0]]);

        control.advance(Duration::from_millis(299));
        assert!(poll(future.as_mut()).is_pending());
        assert_eq!(dialer.launches(), vec![addresses[0]]);

        control.advance(Duration::from_millis(1));
        assert_eq!(poll(future.as_mut()).unwrap_ready().unwrap(), "second");
        assert_eq!(dialer.launches(), addresses);
        assert_eq!(first.dropped(), 1, "the losing attempt must be cancelled");
    }

    #[test]
    fn fast_failure_launches_next_address_without_waiting() {
        let dialer = ScriptedDialer::new(vec![
            AttemptScript::Failure(AttemptPhase::Connect, io::ErrorKind::ConnectionRefused),
            AttemptScript::Success("second"),
        ]);
        let clock = ClockControl::new().to_clock();

        assert_eq!(race_connections(&clock, addresses(), &dialer).unwrap_ready().unwrap(), "second");
        assert_eq!(dialer.launches().len(), 2);
    }

    #[test]
    fn all_address_failures_keep_last_recovery_and_connect_label() {
        let dialer = ScriptedDialer::new(vec![
            AttemptScript::Failure(AttemptPhase::Registration, io::ErrorKind::BrokenPipe),
            AttemptScript::Failure(AttemptPhase::Connect, io::ErrorKind::NetworkUnreachable),
        ]);
        let clock = ClockControl::new().to_clock();
        let error = race_connections(&clock, addresses(), &dialer).unwrap_ready().unwrap_err();

        assert_eq!(error.label().as_str(), "connect");
        assert_eq!(error.recovery().kind(), RecoveryKind::Unavailable);
        assert!(error.to_string().contains("all 2 connection attempts failed"));
    }

    #[test]
    fn dropping_registration_and_connect_attempts_cleans_them_up() {
        for phase in [AttemptPhase::Registration, AttemptPhase::Connect] {
            let attempt = ManualAttempt::new(phase.clone());
            let dialer = ScriptedDialer::new(vec![AttemptScript::Pending(attempt.future())]);
            let clock = ClockControl::new().to_clock();
            let mut future = Box::pin(race_connections(&clock, vec![addresses()[0]], &dialer));

            assert!(poll(future.as_mut()).is_pending());
            drop(future);
            assert_eq!(attempt.dropped_phases(), vec![phase]);
        }
    }

    fn addresses() -> Vec<SocketAddr> {
        vec!["192.0.2.1:443".parse().unwrap(), "192.0.2.2:443".parse().unwrap()]
    }

    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    trait PollReadyExt<T> {
        fn unwrap_ready(self) -> T;
    }

    impl<T> PollReadyExt<T> for Poll<T> {
        fn unwrap_ready(self) -> T {
            match self {
                Self::Ready(value) => value,
                Self::Pending => panic!("expected scripted future to be ready"),
            }
        }
    }

    #[derive(Clone)]
    struct ScriptedResolver {
        result: Arc<Mutex<Option<Result<Vec<SocketAddr>>>>>,
        calls: Arc<Mutex<Vec<(String, u16)>>>,
    }

    impl ScriptedResolver {
        fn success(addresses: Vec<SocketAddr>) -> Self {
            Self {
                result: Arc::new(Mutex::new(Some(Ok(addresses)))),
                calls: Arc::default(),
            }
        }

        fn calls(&self) -> Vec<(String, u16)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Resolver for ScriptedResolver {
        fn resolve(&self, host: String, port: u16) -> ResolveFuture {
            self.calls.lock().unwrap().push((host, port));
            let result = self.result.lock().unwrap().take().unwrap();
            Box::pin(std::future::ready(result))
        }
    }

    enum AttemptScript {
        Pending(ManualAttemptFuture),
        Success(&'static str),
        Failure(AttemptPhase, io::ErrorKind),
    }

    #[derive(Clone)]
    struct ScriptedDialer {
        scripts: Arc<Mutex<VecDeque<AttemptScript>>>,
        launches: Arc<Mutex<Vec<SocketAddr>>>,
    }

    impl ScriptedDialer {
        fn new(scripts: Vec<AttemptScript>) -> Self {
            Self {
                scripts: Arc::new(Mutex::new(scripts.into())),
                launches: Arc::default(),
            }
        }

        fn launches(&self) -> Vec<SocketAddr> {
            self.launches.lock().unwrap().clone()
        }
    }

    impl Dialer for ScriptedDialer {
        type Stream = &'static str;

        fn connect(
            &self,
            address: SocketAddr,
        ) -> Pin<Box<dyn Future<Output = std::result::Result<Self::Stream, AttemptFailure>> + Send + 'static>> {
            self.launches.lock().unwrap().push(address);
            let script = self.scripts.lock().unwrap().pop_front().unwrap();
            match script {
                AttemptScript::Pending(future) => Box::pin(future),
                AttemptScript::Success(value) => Box::pin(std::future::ready(Ok(value))),
                AttemptScript::Failure(phase, kind) => {
                    Box::pin(std::future::ready(Err(AttemptFailure::new(address, phase, io::Error::from(kind)))))
                }
            }
        }
    }

    struct ManualAttempt {
        phase: AttemptPhase,
        dropped_phases: Arc<Mutex<Vec<AttemptPhase>>>,
    }

    impl ManualAttempt {
        fn new(phase: AttemptPhase) -> Self {
            Self {
                phase,
                dropped_phases: Arc::default(),
            }
        }

        fn future(&self) -> ManualAttemptFuture {
            ManualAttemptFuture {
                phase: self.phase.clone(),
                dropped_phases: Arc::clone(&self.dropped_phases),
            }
        }

        fn dropped(&self) -> usize {
            self.dropped_phases.lock().unwrap().len()
        }

        fn dropped_phases(&self) -> Vec<AttemptPhase> {
            self.dropped_phases.lock().unwrap().clone()
        }
    }

    struct ManualAttemptFuture {
        phase: AttemptPhase,
        dropped_phases: Arc<Mutex<Vec<AttemptPhase>>>,
    }

    impl Future for ManualAttemptFuture {
        type Output = std::result::Result<&'static str, AttemptFailure>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for ManualAttemptFuture {
        fn drop(&mut self) {
            self.dropped_phases.lock().unwrap().push(self.phase.clone());
        }
    }
}
