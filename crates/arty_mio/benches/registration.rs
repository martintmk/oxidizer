// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Measures registration on the primary driver owner thread.

#![allow(missing_docs, reason = "benchmark code")]
#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "benchmark fixture failures provide sufficient context"
)]

use std::hint::black_box;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use arty_io_core::{Cycle, Driver, DriverOptions, DriverRole, IoContext};
use arty_mio::{MioContext, MioDriver, MioOptions, Registered};
use criterion::{BatchSize, Criterion};
use mio::Interest;
use mio::net::UdpSocket;
use thread_aware::ThreadBuilder;
use thread_aware_core::ThreadAware;

const GROUP: &str = "arty_mio/registration";

struct RegistrationState {
    io: MioContext,
    driver: MioDriver,
    source: Option<UdpSocket>,
}

fn registration_state() -> RegistrationState {
    let mut io = MioContext::context(MioOptions::default());
    let thread = ThreadBuilder::default().build(std::thread::current().id());
    io.relocate(None, &thread);
    let driver = io
        .driver(DriverOptions::new(thread, Waker::noop().clone(), DriverRole::Primary))
        .unwrap();
    let source = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    RegistrationState {
        io,
        driver,
        source: Some(source),
    }
}

#[metabench::benchmark(REGISTER, GROUP, "register")]
#[bench::primary_owner(&mut registration_state())]
fn register(state: &mut RegistrationState) -> Registered<UdpSocket> {
    let source = state.source.take().unwrap();
    let mut future = pin!(state.io.register(source, Interest::READABLE));
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => {
            state.driver.execute_cycle(&mut Cycle::new(Duration::ZERO)).unwrap();
            match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(result) => result.unwrap(),
                Poll::Pending => panic!("the primary driver must complete registration in one cycle"),
            }
        }
    }
}

fn criterion_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group(GROUP);
    group.bench_function(REGISTER.benchmark_name(), |bencher| {
        bencher.iter_batched_ref(registration_state, |state| register(black_box(state)), BatchSize::PerIteration);
    });
    group.finish();
}

metabench::main!(criterion = criterion_benchmarks, benchmarks = [REGISTER],);
