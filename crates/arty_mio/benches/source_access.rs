// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Compares guarded source access with exclusive direct source access.

#![allow(missing_docs, reason = "benchmark code")]
#![expect(clippy::unwrap_used, reason = "benchmark fixture failures provide sufficient context")]

use std::hint::black_box;
use std::io::{self, IoSlice};
use std::task::Waker;

use arty_io_core::{DriverOptions, DriverRole, IoContext};
use arty_mio::{ExclusiveRegistered, MioContext, MioDriver, MioOptions, Registered};
use criterion::Criterion;
use mio::event::Source;
use mio::{Interest, Registry, Token};
use thread_aware::ThreadBuilder;
use thread_aware_core::ThreadAware;

const READ_GROUP: &str = "arty_mio/source_access/read";
const WRITE_GROUP: &str = "arty_mio/source_access/write";
const VECTORED_WRITE_GROUP: &str = "arty_mio/source_access/write_vectored";
const BUFFER: [u8; 64] = [42; 64];

struct LockedState {
    registration: Registered<BenchSource>,
    _driver: MioDriver,
    destination: [u8; BUFFER.len()],
}

struct DirectState {
    registration: ExclusiveRegistered<BenchSource>,
    _driver: MioDriver,
    destination: [u8; BUFFER.len()],
}

#[derive(Default)]
struct BenchSource {
    bytes: usize,
}

impl BenchSource {
    fn read(&mut self, destination: &mut [u8]) -> usize {
        destination.copy_from_slice(black_box(&BUFFER));
        self.bytes = destination.len();
        self.bytes
    }

    fn write(&mut self, source: &[u8]) -> usize {
        self.bytes = black_box(source).len();
        self.bytes
    }

    fn write_vectored(&mut self, sources: &[IoSlice<'_>]) -> usize {
        self.bytes = black_box(sources).iter().map(|source| source.len()).sum();
        self.bytes
    }
}

impl Source for BenchSource {
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

fn context() -> (MioContext, MioDriver) {
    let mut io = MioContext::context(MioOptions::default());
    let thread = ThreadBuilder::default().build(std::thread::current().id());
    io.relocate(None, &thread);
    let driver = io
        .driver(DriverOptions::new(thread, Waker::noop().clone(), DriverRole::Primary))
        .unwrap();
    (io, driver)
}

fn locked_state() -> LockedState {
    let (io, driver) = context();
    let registration = io.register(BenchSource::default(), Interest::READABLE).unwrap();
    LockedState {
        registration,
        _driver: driver,
        destination: [0; BUFFER.len()],
    }
}

fn direct_state() -> DirectState {
    let (io, driver) = context();
    let registration = io.register_exclusive(BenchSource::default(), Interest::READABLE).unwrap();
    DirectState {
        registration,
        _driver: driver,
        destination: [0; BUFFER.len()],
    }
}

#[metabench::benchmark(READ_LOCKED, READ_GROUP, "locked")]
#[bench::source(&mut locked_state())]
fn read_locked(state: &mut LockedState) -> usize {
    state.registration.source_mut().unwrap().read(black_box(&mut state.destination))
}

#[metabench::benchmark(READ_DIRECT, READ_GROUP, "direct")]
#[bench::source(&mut direct_state())]
fn read_direct(state: &mut DirectState) -> usize {
    state.registration.source_mut().unwrap().read(black_box(&mut state.destination))
}

#[metabench::benchmark(WRITE_LOCKED, WRITE_GROUP, "locked")]
#[bench::source(&locked_state())]
fn write_locked(state: &LockedState) -> usize {
    state.registration.source_mut().unwrap().write(black_box(&BUFFER))
}

#[metabench::benchmark(WRITE_DIRECT, WRITE_GROUP, "direct")]
#[bench::source(&mut direct_state())]
fn write_direct(state: &mut DirectState) -> usize {
    state.registration.source_mut().unwrap().write(black_box(&BUFFER))
}

#[metabench::benchmark(VECTORED_WRITE_LOCKED, VECTORED_WRITE_GROUP, "locked")]
#[bench::source(&locked_state())]
fn write_vectored_locked(state: &LockedState) -> usize {
    let buffers = [IoSlice::new(&BUFFER[..16]), IoSlice::new(&BUFFER[16..])];
    state.registration.source_mut().unwrap().write_vectored(black_box(&buffers))
}

#[metabench::benchmark(VECTORED_WRITE_DIRECT, VECTORED_WRITE_GROUP, "direct")]
#[bench::source(&mut direct_state())]
fn write_vectored_direct(state: &mut DirectState) -> usize {
    let buffers = [IoSlice::new(&BUFFER[..16]), IoSlice::new(&BUFFER[16..])];
    state.registration.source_mut().unwrap().write_vectored(black_box(&buffers))
}

fn criterion_benchmarks(criterion: &mut Criterion) {
    let mut read = criterion.benchmark_group(READ_GROUP);
    let mut locked = locked_state();
    read.bench_function(READ_LOCKED.benchmark_name(), |bencher| {
        bencher.iter(|| black_box(read_locked(black_box(&mut locked))));
    });
    let mut direct = direct_state();
    read.bench_function(READ_DIRECT.benchmark_name(), |bencher| {
        bencher.iter(|| black_box(read_direct(black_box(&mut direct))));
    });
    read.finish();

    let mut write = criterion.benchmark_group(WRITE_GROUP);
    let locked = locked_state();
    write.bench_function(WRITE_LOCKED.benchmark_name(), |bencher| {
        bencher.iter(|| black_box(write_locked(black_box(&locked))));
    });
    let mut direct = direct_state();
    write.bench_function(WRITE_DIRECT.benchmark_name(), |bencher| {
        bencher.iter(|| black_box(write_direct(black_box(&mut direct))));
    });
    write.finish();

    let mut vectored = criterion.benchmark_group(VECTORED_WRITE_GROUP);
    let locked = locked_state();
    vectored.bench_function(VECTORED_WRITE_LOCKED.benchmark_name(), |bencher| {
        bencher.iter(|| black_box(write_vectored_locked(black_box(&locked))));
    });
    let mut direct = direct_state();
    vectored.bench_function(VECTORED_WRITE_DIRECT.benchmark_name(), |bencher| {
        bencher.iter(|| black_box(write_vectored_direct(black_box(&mut direct))));
    });
    vectored.finish();
}

metabench::main!(
    criterion = criterion_benchmarks,
    benchmarks = [
        READ_LOCKED,
        READ_DIRECT,
        WRITE_LOCKED,
        WRITE_DIRECT,
        VECTORED_WRITE_LOCKED,
        VECTORED_WRITE_DIRECT
    ],
    allocator = std::alloc::System,
);
