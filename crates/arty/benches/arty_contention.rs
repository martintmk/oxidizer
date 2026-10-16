// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! A focused Arty/Tokio contention workload using a relocated cache payload.
//!
//! Each case submits one outer task and fans out ten inner operations. The
//! cache touch is black-boxed so the benchmark measures the scheduling work
//! around an observable operation rather than an optimized-away future.
//!
//! The cache is a mutex behind a per-thread `performables::arc::Arc`. Arty
//! relocates that payload before the outer task starts; Tokio uses the same
//! cache type without an equivalent relocation hook.
//!
//! Arty's inner tasks stay on the worker selected for the relocated cache,
//! while Tokio may move its child tasks between workers. The cases are
//! comparable in cache work and task count at one and eight workers, but this
//! is one coordination workload rather than a claim about general runtime
//! performance.
//!
//! Callgrind cannot model the relocated-cache scheduling and mutex contention
//! faithfully. The retained-waker case is included because its deterministic
//! three-poll transition isolates cloned-waker wake and rescheduling overhead.

use std::hint::black_box;
use std::pin::Pin;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use arty::runtime::{Runtime, WorkersPolicy};
use arty::task::{Builtins, JoinHandle};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
#[cfg(target_os = "linux")]
use gungraun::{Callgrind, CallgrindMetrics, LibraryBenchmarkConfig};
use metabench::benchmark;
use performables::arc::{Arc, PerThread};
use performables::sync::mutex::Mutex;
use thread_aware::{Thread, ThreadAware, Unaware};

const INNER_OPERATIONS: usize = 10;
const CONCURRENCY: usize = 10;
const CACHE_ENTRIES: usize = 8;
const WORKERS: [usize; 2] = [1, 8];
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct CacheState {
    entries: [u64; CACHE_ENTRIES],
}

#[derive(Clone, Debug)]
struct Cache {
    inner: Arc<Mutex<CacheState>, PerThread>,
}

impl Cache {
    fn new() -> Self {
        Self {
            inner: Arc::<Mutex<CacheState>, PerThread>::new_with(new_cache_mutex),
        }
    }

    fn touch(&self, operation: usize) -> u64 {
        let value = {
            let mut state = self.inner.lock_result().expect("benchmark cache mutex is never poisoned");
            let index = operation % CACHE_ENTRIES;
            let value = state.entries[index].wrapping_add(u64::try_from(operation).expect("benchmark operation fits in u64") + 1);
            state.entries[index] = value;
            value
        };
        black_box(value)
    }
}

impl ThreadAware for Cache {
    fn relocate(&mut self, source: Option<&Thread>, destination: &Thread) {
        self.inner.relocate(source, destination);
    }
}

fn new_cache_mutex() -> Mutex<CacheState> {
    Mutex::new(CacheState::default())
}

#[derive(Debug)]
struct ArtyCase {
    runtime: Runtime,
    cache: Cache,
}

impl ArtyCase {
    fn new(workers: usize) -> Self {
        let case = Self {
            runtime: Runtime::builder()
                .workers(WorkersPolicy::exactly(workers))
                .build()
                .expect("benchmark runtime construction must succeed"),
            cache: Cache::new(),
        };
        for _ in 0..workers {
            black_box(case.run(CONCURRENCY));
        }
        case
    }

    fn run(&self, concurrency: usize) -> Duration {
        assert_eq!(concurrency, CONCURRENCY, "benchmark uses its fixed concurrency");
        let start = Instant::now();
        let scheduler = self.runtime.scheduler();
        let mut handles = std::array::from_fn::<_, CONCURRENCY, _>(|_| None);
        for handle in &mut handles {
            *handle = Some(scheduler.spawn_anywhere(self.cache.clone(), arty_outer));
        }
        futures::executor::block_on(async move {
            for handle in handles {
                let Some(handle) = handle else {
                    unreachable!("all benchmark outer task slots are filled");
                };
                handle.await.expect("benchmark outer task finishes before shutdown");
            }
        });
        start.elapsed()
    }
}

async fn arty_outer(cx: Builtins, cache: Cache) {
    let scheduler = cx.scheduler().clone();
    let mut handles = std::array::from_fn::<_, INNER_OPERATIONS, _>(|_| None);
    for (operation, handle) in handles.iter_mut().enumerate() {
        let cache = cache.clone();
        *handle = Some(scheduler.spawn(move |_| async move { cache.touch(operation) }));
    }

    for handle in handles {
        let Some(handle) = handle else {
            unreachable!("all benchmark inner task slots are filled");
        };
        handle.await.expect("benchmark inner task finishes before shutdown");
    }
}

#[derive(Debug)]
struct TokioCase {
    runtime: tokio::runtime::Runtime,
    cache: Cache,
}

impl TokioCase {
    fn new(workers: usize) -> Self {
        let case = Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .build()
                .expect("benchmark runtime construction must succeed"),
            cache: Cache::new(),
        };
        for _ in 0..workers {
            black_box(case.run(CONCURRENCY));
        }
        case
    }

    fn run(&self, concurrency: usize) -> Duration {
        assert_eq!(concurrency, CONCURRENCY, "benchmark uses its fixed concurrency");
        let start = Instant::now();
        let mut handles = std::array::from_fn::<_, CONCURRENCY, _>(|_| None);
        for handle in &mut handles {
            *handle = Some(self.runtime.spawn(tokio_outer(self.cache.clone())));
        }
        futures::executor::block_on(async move {
            for handle in handles {
                let Some(handle) = handle else {
                    unreachable!("all benchmark outer task slots are filled");
                };
                handle.await.expect("benchmark outer task finishes before shutdown");
            }
        });
        start.elapsed()
    }
}

async fn tokio_outer(cache: Cache) {
    let mut handles = std::array::from_fn::<_, INNER_OPERATIONS, _>(|_| None);
    for (operation, handle) in handles.iter_mut().enumerate() {
        let cache = cache.clone();
        *handle = Some(tokio::spawn(async move { cache.touch(operation) }));
    }

    for handle in handles {
        let Some(handle) = handle else {
            unreachable!("all benchmark inner task slots are filled");
        };
        handle.await.expect("benchmark inner task finishes before shutdown");
    }
}

#[derive(Debug)]
struct ArtyRetainedWakeCase {
    _runtime: Runtime,
    retained_waker: Option<Waker>,
    handle: Option<JoinHandle<()>>,
}

impl ArtyRetainedWakeCase {
    fn new() -> Self {
        let runtime = Runtime::builder()
            .workers(WorkersPolicy::exactly(1))
            .build()
            .expect("benchmark runtime construction must succeed");
        let (waker_tx, waker_rx) = sync_channel(1);
        let handle = runtime.scheduler().spawn_anywhere(Unaware(waker_tx), retained_io_driver_task);
        let retained_waker = waker_rx
            .recv_timeout(SETUP_TIMEOUT)
            .expect("benchmark task must reach its initial pending state");
        Self {
            _runtime: runtime,
            retained_waker: Some(retained_waker),
            handle: Some(handle),
        }
    }

    fn wake_and_join(&mut self) {
        self.retained_waker
            .take()
            .expect("benchmark task retained its polling waker")
            .wake();
        let handle = self.handle.take().expect("benchmark case runs exactly once");
        futures::executor::block_on(handle).expect("benchmark task completes after its retained wake");
    }
}

#[derive(Debug)]
struct RetainedWakeFuture {
    poll_count: u8,
    retained_waker: Option<Waker>,
    first_waker: Option<SyncSender<Waker>>,
}

impl Future for RetainedWakeFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_count = this.poll_count.saturating_add(1);
        match this.poll_count {
            1 => {
                let retained_waker = cx.waker().clone();
                this.first_waker
                    .take()
                    .expect("benchmark future sends its first waker once")
                    .send(retained_waker.clone())
                    .expect("benchmark setup receives the retained waker");
                this.retained_waker = Some(retained_waker);
                Poll::Pending
            }
            2 => {
                this.retained_waker
                    .as_ref()
                    .expect("benchmark future retained its first polling waker")
                    .wake_by_ref();
                Poll::Pending
            }
            _ => Poll::Ready(()),
        }
    }
}

fn retained_io_driver_task(_: Builtins, Unaware(first_waker): Unaware<SyncSender<Waker>>) -> RetainedWakeFuture {
    RetainedWakeFuture {
        poll_count: 0,
        retained_waker: None,
        first_waker: Some(first_waker),
    }
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .sample_size(50)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
}

fn criterion_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group(ARTY.group_name());
    group.throughput(Throughput::Elements(
        u64::try_from(INNER_OPERATIONS * CONCURRENCY).expect("benchmark operation count fits in u64"),
    ));
    for workers in WORKERS {
        let name = format!("concurrency_w{workers}");
        group.bench_function(BenchmarkId::new(ARTY.benchmark_name(), &name), |bencher| {
            let case = ArtyCase::new(workers);
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    elapsed += case.run(CONCURRENCY);
                }
                elapsed
            });
        });
        group.bench_function(BenchmarkId::new(TOKIO.benchmark_name(), &name), |bencher| {
            let case = TokioCase::new(workers);
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    elapsed += case.run(CONCURRENCY);
                }
                elapsed
            });
        });
    }
    group.finish();

    let mut retained_waker = criterion.benchmark_group(ARTY_RETAINED_WAKE.group_name());
    retained_waker.bench_function(BenchmarkId::new(ARTY_RETAINED_WAKE.benchmark_name(), "io_driver"), |bencher| {
        bencher.iter_batched_ref(
            ArtyRetainedWakeCase::new,
            ArtyRetainedWakeCase::wake_and_join,
            BatchSize::PerIteration,
        );
    });
    retained_waker.finish();
}

#[benchmark(ARTY, "arty_contention/relocated_cache", "Arty")]
#[bench::concurrency_w1(&ArtyCase::new(1), 10)]
#[bench::concurrency_w8(&ArtyCase::new(8), 10)]
fn arty_workload(case: &ArtyCase, concurrency: u64) -> Duration {
    case.run(usize::try_from(concurrency).expect("benchmark concurrency fits in usize"))
}

#[benchmark(TOKIO, "arty_contention/relocated_cache", "Tokio")]
#[bench::concurrency_w1(&TokioCase::new(1), 10)]
#[bench::concurrency_w8(&TokioCase::new(8), 10)]
fn tokio_workload(case: &TokioCase, concurrency: u64) -> Duration {
    case.run(usize::try_from(concurrency).expect("benchmark concurrency fits in usize"))
}

#[benchmark(ARTY_RETAINED_WAKE, "arty_contention/retained_waker", "Arty")]
#[bench::io_driver(&mut ArtyRetainedWakeCase::new())]
fn arty_retained_wake(case: &mut ArtyRetainedWakeCase) {
    case.wake_and_join();
}

#[cfg(target_os = "linux")]
metabench::main!(
    criterion = {
        factory = configured_criterion,
        benchmarks = criterion_benchmarks,
        unit = "ns",
    },
    gungraun = {
        config = LibraryBenchmarkConfig::default().tool(
            Callgrind::default()
                .args(["--branch-sim=yes"])
                .format([CallgrindMetrics::Default, CallgrindMetrics::BranchSim]),
        );
    },
    benchmarks = [ARTY, TOKIO, ARTY_RETAINED_WAKE],
);

#[cfg(not(target_os = "linux"))]
metabench::main!(
    criterion = {
        factory = configured_criterion,
        benchmarks = criterion_benchmarks,
        unit = "ns",
    },
    benchmarks = [ARTY, TOKIO, ARTY_RETAINED_WAKE],
);
