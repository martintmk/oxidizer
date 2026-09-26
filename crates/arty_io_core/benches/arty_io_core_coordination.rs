// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Work registration and completion notification overhead.
//!
//! A runtime fixture counts notifications through the coordination trait. The workloads
//! measure the shared contract, not runtime synchronization or native waiting.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use arty_io_core::{Cycle, PendingWork, PendingWorkTracker};
use criterion::{BatchSize, BenchmarkId, Criterion};

const OPERATION_GROUP: &str = "arty_io_core_coordination/operation";

#[derive(Default)]
struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct Input {
    completed: Waker,
    interrupt: Waker,
    work: Option<PendingWork>,
    already_interrupted: bool,
    started_at: Instant,
}

impl Input {
    fn new(already_interrupted: bool) -> Self {
        Self {
            completed: Waker::from(Arc::new(Counter::default())),
            interrupt: Waker::from(Arc::new(Counter::default())),
            work: None,
            already_interrupted,
            started_at: Instant::now(),
        }
    }

    fn pending() -> Self {
        let mut input = Self::new(false);
        input.work = Some(PendingWork::new(input.completed.clone()));
        input
    }
}

impl PendingWorkTracker for Input {
    fn start_work(&mut self, interrupt: Waker) -> PendingWork {
        if self.already_interrupted {
            interrupt.wake();
        }
        PendingWork::new(self.completed.clone())
    }
}

#[metabench::benchmark(CREATE_WORK, OPERATION_GROUP, "create_work")]
#[bench::default(Input::new(false))]
fn create_work(mut input: Input) -> Input {
    input.work = Some(PendingWork::new(black_box(input.completed.clone())));
    input
}

#[metabench::benchmark(START_WORK, OPERATION_GROUP, "start_work")]
#[bench::idle(Input::new(false))]
#[bench::interrupted(Input::new(true))]
fn start_work(mut input: Input) -> Input {
    let interrupt = black_box(input.interrupt.clone());
    let started_at = black_box(input.started_at);
    let tracker: &mut dyn PendingWorkTracker = &mut input;
    let mut cycle = Cycle::new(started_at, black_box(Duration::MAX), true, black_box(tracker));
    let work = cycle.start_work(interrupt);
    input.work = Some(work);
    input
}

#[metabench::benchmark(DROP_WORK, OPERATION_GROUP, "drop_work")]
#[bench::default(Input::pending())]
fn drop_work(mut input: Input) -> Input {
    drop(input.work.take());
    input
}

#[metabench::benchmark(COMPLETE_WORK, OPERATION_GROUP, "complete_work")]
#[bench::default(Input::pending())]
fn complete_work(mut input: Input) -> Input {
    input.work.take().expect("setup creates pending work for this operation").complete();
    input
}

fn criterion_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group(OPERATION_GROUP);
    group.bench_function(CREATE_WORK.benchmark_name(), |bencher| {
        bencher.iter_batched(|| Input::new(false), create_work, BatchSize::SmallInput);
    });
    for (case, interrupted) in [("idle", false), ("interrupted", true)] {
        group.bench_with_input(
            BenchmarkId::new(START_WORK.benchmark_name(), case),
            &interrupted,
            |bencher, &raised| {
                bencher.iter_batched(|| Input::new(raised), start_work, BatchSize::SmallInput);
            },
        );
    }
    group.bench_function(DROP_WORK.benchmark_name(), |bencher| {
        bencher.iter_batched(Input::pending, drop_work, BatchSize::SmallInput);
    });
    group.bench_function(COMPLETE_WORK.benchmark_name(), |bencher| {
        bencher.iter_batched(Input::pending, complete_work, BatchSize::SmallInput);
    });
    group.finish();
}

metabench::main!(
    criterion = criterion_benchmarks,
    benchmarks = [CREATE_WORK, START_WORK, DROP_WORK, COMPLETE_WORK],
);
