// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Runtime worker and matched request driver.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use arty::runtime::{BlockingPoolPolicy, Runtime, WorkersPolicy};
use bytesbuf::BytesView;
use fetch::options::{ConnectionPoolOptions, HttpBodyOptions, SocketOptions};
use fetch::{HttpClient, Version};
use futures::channel::oneshot;
use futures::future::join_all;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::{
    Accounting, Cell, FailureKey, HistogramBucket, LatencySummary, LoadModel, ProcessResources, Protocol, RuntimeKind, ServerWindow,
    Workload, http, platform,
};

const BLOCKING_THREADS: usize = 4;
const HISTOGRAM_LOWEST_US: u64 = 1;
const HISTOGRAM_HIGHEST_US: u64 = 60_000_000;
const HISTOGRAM_DIGITS: u8 = 3;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Input for one runtime worker process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkerSpec {
    /// Sample ordinal.
    pub sample_index: usize,
    /// Cell repetition.
    pub repetition: usize,
    /// Runtime under test.
    pub runtime: RuntimeKind,
    /// Matched workload cell.
    pub cell: Cell,
    /// Load model.
    pub load_model: LoadModel,
    /// Server port.
    pub port: u16,
    /// Server process ID.
    pub server_pid: u32,
    /// Warm-up duration.
    pub warmup: Duration,
    /// Measurement duration.
    pub duration: Duration,
    /// Optional client process affinity.
    pub affinity: Option<u64>,
}

/// Worker output before coordinator rejection rules.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkerOutput {
    /// Actual client duration.
    pub client_duration_secs: f64,
    /// Completed requests per second.
    pub throughput_per_sec: f64,
    /// Exact accounting.
    pub accounting: Accounting,
    /// Latency distribution.
    pub latency: LatencySummary,
    /// Client resource data.
    pub client_resources: ProcessResources,
    /// Server control report.
    pub server: ServerWindow,
    /// Exact raw `/control/start` HTTP response.
    pub control_start_raw: Vec<u8>,
    /// Exact raw `/control/end` HTTP response.
    pub control_end_raw: Vec<u8>,
    /// Client process ID.
    pub client_pid: u32,
}

#[derive(Debug)]
struct GroupData {
    spec: WorkerSpec,
    lanes: usize,
    ready: mpsc::Sender<Result<(), String>>,
    start: oneshot::Receiver<Instant>,
}

#[derive(Debug)]
struct GroupResult {
    accounting: Accounting,
    histogram: Histogram<u64>,
    underflow: u64,
    overflow: u64,
}

impl GroupResult {
    fn empty() -> Self {
        Self {
            accounting: Accounting::default(),
            histogram: Histogram::new_with_bounds(HISTOGRAM_LOWEST_US, HISTOGRAM_HIGHEST_US, HISTOGRAM_DIGITS)
                .expect("the fixed histogram bounds are valid"),
            underflow: 0,
            overflow: 0,
        }
    }

    fn merge(&mut self, other: &Self) {
        self.accounting.merge(&other.accounting);
        self.histogram
            .add(&other.histogram)
            .expect("all worker histograms use identical bounds");
        self.underflow = self.underflow.saturating_add(other.underflow);
        self.overflow = self.overflow.saturating_add(other.overflow);
    }
}

/// Runs one worker process.
///
/// # Errors
///
/// Returns an error when runtime startup, affinity, control traffic, or a request group fails.
pub fn run(spec: &WorkerSpec) -> Result<WorkerOutput, Box<dyn std::error::Error + Send + Sync>> {
    platform::apply_current_affinity(spec.affinity)?;
    match spec.runtime {
        RuntimeKind::Arty => run_arty(spec),
        RuntimeKind::Tokio => run_tokio(spec),
    }
}

fn run_arty(spec: &WorkerSpec) -> Result<WorkerOutput, Box<dyn std::error::Error + Send + Sync>> {
    let runtime = Runtime::builder()
        .workers(WorkersPolicy::exactly(spec.cell.workers))
        .blocking_pool(BlockingPoolPolicy::shared().max(BLOCKING_THREADS))
        .build()?;
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut starts = Vec::with_capacity(spec.cell.workers);
    let mut handles = Vec::with_capacity(spec.cell.workers);
    for lanes in distribute_lanes(spec.cell.concurrency, spec.cell.workers) {
        let (start_tx, start_rx) = oneshot::channel();
        starts.push(start_tx);
        handles.push(runtime.scheduler().spawn_anywhere(
            thread_aware::Unaware(GroupData {
                spec: spec.clone(),
                lanes,
                ready: ready_tx.clone(),
                start: start_rx,
            }),
            arty_group,
        ));
    }
    drop(ready_tx);
    let output = coordinate(spec, &ready_rx, starts, || {
        let joined = futures::executor::block_on(join_all(handles));
        joined.into_iter().map(|result| result.map_err(|error| error.to_string())).collect()
    });
    runtime.stop()?;
    output
}

async fn arty_group(builtins: arty::task::Builtins, thread_aware::Unaware(data): thread_aware::Unaware<GroupData>) -> GroupResult {
    let client = configure_client(HttpClient::builder_arty(&builtins), &data.spec.cell.protocol);
    run_group(client, data).await
}

fn run_tokio(spec: &WorkerSpec) -> Result<WorkerOutput, Box<dyn std::error::Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(spec.cell.workers)
        .max_blocking_threads(BLOCKING_THREADS)
        .enable_all()
        .build()?;
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut starts = Vec::with_capacity(spec.cell.workers);
    let mut handles = Vec::with_capacity(spec.cell.workers);
    for lanes in distribute_lanes(spec.cell.concurrency, spec.cell.workers) {
        let (start_tx, start_rx) = oneshot::channel();
        starts.push(start_tx);
        let data = GroupData {
            spec: spec.clone(),
            lanes,
            ready: ready_tx.clone(),
            start: start_rx,
        };
        handles.push(runtime.spawn(async move {
            let client = configure_client(
                HttpClient::builder_tokio_with_options(
                    fetch::tokio::TokioDeps::default(),
                    fetch::tokio::TokioTransportOptions::default().socket(SocketOptions::default().no_delay(true)),
                ),
                &data.spec.cell.protocol,
            );
            run_group(client, data).await
        }));
    }
    drop(ready_tx);
    coordinate(spec, &ready_rx, starts, || {
        runtime.block_on(async {
            join_all(handles)
                .await
                .into_iter()
                .map(|result| result.map_err(|error| error.to_string()))
                .collect()
        })
    })
}

fn configure_client(builder: fetch::HttpClientBuilder, protocol: &Protocol) -> HttpClient {
    let version = match protocol {
        Protocol::Http1 => Version::HTTP_11,
        Protocol::Http2 => Version::HTTP_2,
    };
    builder
        .supported_http_versions(&[version])
        .insecure_allow_http()
        .minimal_pipeline()
        .response_body_options(HttpBodyOptions::default().buffer_limit(32 * 1024))
        .connection_pool_options(
            ConnectionPoolOptions::default()
                .connection_idle_timeout(None)
                .max_connections(usize::MAX),
        )
        .build()
}

async fn run_group(client: HttpClient, data: GroupData) -> GroupResult {
    let body = BytesView::copied_from_slice(&vec![0_u8; data.spec.cell.workload.request_bytes()], client.as_ref());
    let warmup_deadline = Instant::now() + data.spec.warmup;
    let warmup = run_lanes(
        &client,
        &data.spec.cell.workload,
        data.spec.port,
        body.clone(),
        data.lanes,
        warmup_deadline,
        false,
    )
    .await;
    if warmup.accounting.failed != 0 || warmup.accounting.successful == 0 {
        let _ = data.ready.send(Err(format!(
            "warmup completed {} requests and failed {} requests: {:?}",
            warmup.accounting.successful, warmup.accounting.failed, warmup.accounting.failures
        )));
        return warmup;
    }
    if data.ready.send(Ok(())).is_err() {
        return GroupResult::empty();
    }
    let Ok(deadline) = data.start.await else {
        return GroupResult::empty();
    };
    run_lanes(&client, &data.spec.cell.workload, data.spec.port, body, data.lanes, deadline, true).await
}

async fn run_lanes(
    client: &HttpClient,
    workload: &Workload,
    port: u16,
    body: BytesView,
    lanes: usize,
    deadline: Instant,
    record: bool,
) -> GroupResult {
    let url = format!("http://127.0.0.1:{port}{}", workload.path());
    let futures = (0..lanes).map(|_| lane(client.clone(), workload, url.clone(), body.clone(), deadline, record));
    let mut aggregate = GroupResult::empty();
    for result in join_all(futures).await {
        aggregate.merge(&result);
    }
    aggregate
}

async fn lane(client: HttpClient, workload: &Workload, url: String, body: BytesView, deadline: Instant, record: bool) -> GroupResult {
    let mut result = GroupResult::empty();
    while Instant::now() < deadline {
        let started = Instant::now();
        result.accounting.attempted = result.accounting.attempted.saturating_add(1);
        result.accounting.physical_attempts = result.accounting.physical_attempts.saturating_add(1);
        match execute(&client, workload, &url, body.clone()).await {
            Ok(()) => {
                result.accounting.successful = result.accounting.successful.saturating_add(1);
                if record {
                    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                    let value = micros.max(HISTOGRAM_LOWEST_US);
                    if value > HISTOGRAM_HIGHEST_US || result.histogram.record(value).is_err() {
                        result.overflow = result.overflow.saturating_add(1);
                    }
                }
            }
            Err((stage, detail)) => {
                result.accounting.failed = result.accounting.failed.saturating_add(1);
                *result.accounting.failures.entry(FailureKey { stage, detail }).or_default() += 1;
            }
        }
    }
    result
}

async fn execute(client: &HttpClient, workload: &Workload, url: &str, body: BytesView) -> Result<(), (String, String)> {
    let request = match workload {
        Workload::HelloWorld => client.get(url),
        Workload::RequestResponse => client.post(url).bytes(body),
    }
    .timeout(REQUEST_TIMEOUT);
    let response = request.fetch().await.map_err(|error| ("fetch".to_owned(), error.to_string()))?;
    if response.status() != fetch::StatusCode::OK {
        return Err(("status".to_owned(), format!("unexpected status {}", response.status())));
    }
    let bytes = response
        .into_body()
        .into_bytes()
        .await
        .map_err(|error| ("body".to_owned(), error.to_string()))?;
    match workload {
        Workload::HelloWorld if bytes != b"Hello World!" => Err(("validation".to_owned(), "hello response bytes differ".to_owned())),
        Workload::RequestResponse
            if bytes.len() != Workload::RequestResponse.response_bytes()
                || bytes.slices().any(|(slice, _metadata)| slice.iter().any(|byte| *byte != 0)) =>
        {
            Err(("validation".to_owned(), "request-response bytes differ".to_owned()))
        }
        _ => Ok(()),
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "request counts and worker counts are small load-test values"
)]
fn coordinate(
    spec: &WorkerSpec,
    ready: &mpsc::Receiver<Result<(), String>>,
    starts: Vec<oneshot::Sender<Instant>>,
    join: impl FnOnce() -> Vec<Result<GroupResult, String>>,
) -> Result<WorkerOutput, Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..starts.len() {
        ready.recv_timeout(spec.warmup + Duration::from_secs(30))??;
    }

    let control_start = http::request(spec.port, "POST", "/control/start")?;
    if control_start.status != 200 {
        return Err(format!("control start returned HTTP {}", control_start.status).into());
    }
    let _: serde_json::Value = serde_json::from_slice(&control_start.body)?;

    let resources_start = platform::capture();
    let client_started = Instant::now();
    let deadline = client_started + spec.duration;
    for start in starts {
        start
            .send(deadline)
            .map_err(|_send_error| "runtime group stopped before measurement")?;
    }
    let results = join();
    let client_duration = client_started.elapsed();
    let resources_end = platform::capture();

    let control_end = http::request(spec.port, "POST", "/control/end")?;
    if control_end.status != 200 {
        return Err(format!("control end returned HTTP {}", control_end.status).into());
    }
    let server: ServerWindow = serde_json::from_slice(&control_end.body)?;

    let mut aggregate = GroupResult::empty();
    for result in results {
        aggregate.merge(&result.map_err(|error| format!("runtime group failed: {error}"))?);
    }
    let latency = summarize_histogram(&aggregate.histogram, aggregate.underflow, aggregate.overflow);
    let throughput = if client_duration.is_zero() {
        0.0
    } else {
        aggregate.accounting.successful as f64 / client_duration.as_secs_f64()
    };
    let processor_time = resources_end.processor_time.saturating_sub(resources_start.processor_time);
    let available = client_duration.as_secs_f64() * spec.cell.workers as f64;
    let client_resources = ProcessResources {
        processor_time_secs: processor_time.as_secs_f64(),
        cycles: resources_end
            .cycles
            .zip(resources_start.cycles)
            .map(|(end, start)| end.saturating_sub(start)),
        processor_usage_pct: if available > 0.0 {
            processor_time.as_secs_f64() / available * 100.0
        } else {
            0.0
        },
        end_working_set_bytes: resources_end.working_set_bytes,
        peak_working_set_bytes: resources_end.peak_working_set_bytes,
        end_private_bytes: resources_end.private_bytes,
    };

    Ok(WorkerOutput {
        client_duration_secs: client_duration.as_secs_f64(),
        throughput_per_sec: throughput,
        accounting: aggregate.accounting,
        latency,
        client_resources,
        server,
        control_start_raw: control_start.raw,
        control_end_raw: control_end.raw,
        client_pid: std::process::id(),
    })
}

fn summarize_histogram(histogram: &Histogram<u64>, underflow: u64, overflow: u64) -> LatencySummary {
    let buckets = histogram
        .iter_recorded()
        .map(|value| HistogramBucket {
            value_us: value.value_iterated_to(),
            count: value.count_since_last_iteration(),
        })
        .collect();
    LatencySummary {
        significant_digits: HISTOGRAM_DIGITS,
        lowest_us: HISTOGRAM_LOWEST_US,
        highest_us: HISTOGRAM_HIGHEST_US,
        underflow,
        overflow,
        count: histogram.len(),
        min_us: histogram.min(),
        p50_us: histogram.value_at_quantile(0.50),
        p90_us: histogram.value_at_quantile(0.90),
        p95_us: histogram.value_at_quantile(0.95),
        p99_us: histogram.value_at_quantile(0.99),
        max_us: histogram.max(),
        buckets,
    }
}

fn distribute_lanes(concurrency: usize, workers: usize) -> Vec<usize> {
    let groups = concurrency.min(workers);
    let base = concurrency / groups;
    let remainder = concurrency % groups;
    (0..groups).map(|index| base + usize::from(index < remainder)).collect()
}
