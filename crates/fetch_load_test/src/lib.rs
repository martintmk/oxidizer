// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Data model and report helpers for the local Fetch runtime load comparison.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Loopback control-plane HTTP client.
pub mod http;
/// Operating-system process controls and resource counters.
pub mod platform;
/// Runtime worker implementation.
pub mod worker;

/// The runtime integration under test.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeKind {
    /// Fetch with Arty and Mio.
    Arty,
    /// Fetch with Tokio.
    Tokio,
}

impl RuntimeKind {
    /// Returns the stable command and report name.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Arty => "arty",
            Self::Tokio => "tokio",
        }
    }

    /// Returns the other runtime.
    #[must_use]
    pub const fn other(&self) -> Self {
        match self {
            Self::Arty => Self::Tokio,
            Self::Tokio => Self::Arty,
        }
    }
}

/// One HTTP workload.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    /// `GET /hello-world`.
    HelloWorld,
    /// `POST /request-response`.
    RequestResponse,
}

impl Workload {
    /// Returns the server path.
    #[must_use]
    pub const fn path(&self) -> &'static str {
        match self {
            Self::HelloWorld => "/hello-world",
            Self::RequestResponse => "/request-response",
        }
    }

    /// Returns the request body length.
    #[must_use]
    pub const fn request_bytes(&self) -> usize {
        match self {
            Self::HelloWorld => 0,
            Self::RequestResponse => 10_240,
        }
    }

    /// Returns the response body length.
    #[must_use]
    pub const fn response_bytes(&self) -> usize {
        match self {
            Self::HelloWorld => 12,
            Self::RequestResponse => 9_216,
        }
    }
}

/// The HTTP protocol selected for a cell.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// Clear-text HTTP/1.1.
    Http1,
    /// Clear-text HTTP/2 prior knowledge.
    Http2,
}

impl Protocol {
    /// Returns the stable command and report name.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
        }
    }
}

/// The offered-load model.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LoadModel {
    /// A lane starts its next request after its prior response completes.
    ClosedLoop,
    /// A future fixed-rate phase with a shared offered rate.
    FixedRate {
        /// Offered requests per second.
        requests_per_second: f64,
    },
}

/// One matched workload cell.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Cell {
    /// Workload endpoint.
    pub workload: Workload,
    /// HTTP protocol.
    pub protocol: Protocol,
    /// Runtime worker count.
    pub workers: usize,
    /// Concurrent request lanes.
    pub concurrency: usize,
}

impl Cell {
    /// Returns a stable file and report identifier.
    #[must_use]
    pub fn id(&self) -> String {
        format!(
            "{}-{}-w{}-c{}",
            match &self.workload {
                Workload::HelloWorld => "hello",
                Workload::RequestResponse => "request-response",
            },
            self.protocol.as_str(),
            self.workers,
            self.concurrency
        )
    }
}

/// One request failure category.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FailureKey {
    /// Request stage.
    pub stage: String,
    /// Stable error detail when available.
    pub detail: String,
}

/// Request accounting for one sample.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Accounting {
    /// Logical requests started.
    pub attempted: u64,
    /// Requests with a valid response.
    pub successful: u64,
    /// Requests that failed.
    pub failed: u64,
    /// Requests cancelled before completion.
    pub cancelled: u64,
    /// Physical transport attempts.
    pub physical_attempts: u64,
    /// Failure counts by category.
    pub failures: BTreeMap<FailureKey, u64>,
}

impl Accounting {
    /// Returns whether the accounting identity holds.
    #[must_use]
    pub const fn is_balanced(&self) -> bool {
        self.attempted == self.successful + self.failed + self.cancelled
    }

    /// Adds another lane or worker result.
    pub fn merge(&mut self, other: &Self) {
        self.attempted = self.attempted.saturating_add(other.attempted);
        self.successful = self.successful.saturating_add(other.successful);
        self.failed = self.failed.saturating_add(other.failed);
        self.cancelled = self.cancelled.saturating_add(other.cancelled);
        self.physical_attempts = self.physical_attempts.saturating_add(other.physical_attempts);
        for (key, count) in &other.failures {
            *self.failures.entry(key.clone()).or_default() = self.failures.get(key).copied().unwrap_or_default().saturating_add(*count);
        }
    }
}

/// Latency distribution summary for one sample.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LatencySummary {
    /// Histogram significant digits.
    pub significant_digits: u8,
    /// Lowest latency that the histogram can record, in microseconds.
    pub lowest_us: u64,
    /// Highest latency that the histogram can record, in microseconds.
    pub highest_us: u64,
    /// Values below the lower bound.
    pub underflow: u64,
    /// Values above the upper bound.
    pub overflow: u64,
    /// Number of recorded requests.
    pub count: u64,
    /// Minimum latency in microseconds.
    pub min_us: u64,
    /// Median latency in microseconds.
    pub p50_us: u64,
    /// 90th percentile latency in microseconds.
    pub p90_us: u64,
    /// 95th percentile latency in microseconds.
    pub p95_us: u64,
    /// 99th percentile latency in microseconds.
    pub p99_us: u64,
    /// Maximum latency in microseconds.
    pub max_us: u64,
    /// Recorded histogram buckets.
    pub buckets: Vec<HistogramBucket>,
}

/// One recorded histogram bucket.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistogramBucket {
    /// Equivalent bucket value in microseconds.
    pub value_us: u64,
    /// Values recorded in the bucket.
    pub count: u64,
}

/// A process resource delta for one measured window.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ProcessResources {
    /// User and kernel processor time in seconds.
    pub processor_time_secs: f64,
    /// Processor cycles when available.
    pub cycles: Option<u64>,
    /// Processor utilization relative to the selected worker budget.
    pub processor_usage_pct: f64,
    /// Working set at the end of the window.
    pub end_working_set_bytes: u64,
    /// Peak working set since process start.
    pub peak_working_set_bytes: u64,
    /// Private committed bytes at the end of the window.
    pub end_private_bytes: u64,
}

/// The load server report returned by `/control/end`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ServerWindow {
    /// Server implementation name.
    pub server_configuration: String,
    /// Completed measured requests.
    pub requests: u64,
    /// Window duration.
    pub duration_secs: f64,
    /// Configured server processors.
    pub processors: usize,
    /// Processors that completed responses.
    pub completion_processors_used: usize,
    /// Share completed by the busiest processor.
    pub busiest_completion_processor_pct: f64,
    /// Processor time in seconds.
    pub processor_time_secs: f64,
    /// Processor utilization against the processor budget.
    pub processor_usage_pct: f64,
    /// Processor microseconds per request.
    pub us_per_req: f64,
    /// Processor cycles.
    pub cycles: Option<u64>,
    /// Processor cycles per request.
    pub cycles_per_req: Option<u64>,
    /// Transport reads per request when available.
    pub reads_per_req: Option<f64>,
    /// Bytes per transport read when available.
    pub bytes_per_read: Option<f64>,
    /// Transport writes per request when available.
    pub writes_per_req: Option<f64>,
    /// Bytes per transport write when available.
    pub bytes_per_write: Option<f64>,
    /// Working set at the end of the window.
    pub end_working_set_bytes: u64,
    /// Peak working set since process start.
    pub peak_working_set_bytes: u64,
    /// Private committed bytes at the end of the window.
    pub end_private_bytes: u64,
}

/// One complete runtime sample.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[expect(clippy::struct_field_names, reason = "sample_index is the stable artifact key")]
pub struct Sample {
    /// Stable sample ordinal.
    pub sample_index: usize,
    /// Repetition number inside the cell.
    pub repetition: usize,
    /// Runtime under test.
    pub runtime: RuntimeKind,
    /// Matched workload cell.
    pub cell: Cell,
    /// Load model.
    pub load_model: LoadModel,
    /// Warm-up duration.
    pub warmup_secs: f64,
    /// Requested measurement duration.
    pub requested_duration_secs: f64,
    /// Actual client measurement duration.
    pub client_duration_secs: f64,
    /// Completed requests per client second.
    pub throughput_per_sec: f64,
    /// Exact request accounting.
    pub accounting: Accounting,
    /// Latency distribution.
    pub latency: LatencySummary,
    /// Client process resource data.
    pub client_resources: ProcessResources,
    /// Server control data.
    pub server: ServerWindow,
    /// Process ID of the load server.
    pub server_pid: u32,
    /// Fresh loopback server port.
    pub server_port: u16,
    /// Exact server argument list for this sample.
    pub server_arguments: Vec<String>,
    /// Process ID of the runtime worker.
    pub client_pid: u32,
    /// Rejection reasons. An empty list means the sample is accepted.
    pub rejection_reasons: Vec<String>,
}

impl Sample {
    /// Returns whether the sample can support a comparison claim.
    #[must_use]
    pub fn accepted(&self) -> bool {
        self.rejection_reasons.is_empty()
    }
}

/// Immutable run metadata.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[expect(clippy::struct_excessive_bools, reason = "the metadata records independent effective settings")]
pub struct RunMetadata {
    /// Oxidizer commit under test.
    pub oxidizer_commit: String,
    /// Whether the worktree was dirty.
    pub oxidizer_dirty: bool,
    /// ox-sdk revision used to build the server.
    pub ox_sdk_revision: String,
    /// Server binary path.
    pub server_binary: PathBuf,
    /// Server binary SHA-256.
    pub server_sha256: String,
    /// `SymCrypt` DLL path.
    pub symcrypt_dll: PathBuf,
    /// `SymCrypt` DLL SHA-256.
    pub symcrypt_sha256: String,
    /// Exact server argument list.
    pub server_arguments: Vec<String>,
    /// Server serving layer.
    pub server_serving_layer: String,
    /// Server transport mode.
    pub server_transport: String,
    /// Injected logical latency in milliseconds.
    pub server_logical_latency_ms: Option<u64>,
    /// Whether file logging was enabled.
    pub server_file_logging: bool,
    /// Fixed response chunk count for the representative workload.
    pub response_chunk_count: usize,
    /// Fixed response chunk size for the representative workload.
    pub response_chunk_bytes: usize,
    /// Fetch pipeline name.
    pub client_pipeline: String,
    /// Whether retries were enabled.
    pub client_retries: bool,
    /// Whether decompression was enabled.
    pub client_decompression: bool,
    /// Connection-pool description.
    pub client_pool: String,
    /// Whether connection reuse was proven by an independent identity or server counter.
    pub connection_reuse_verified: bool,
    /// Connection-reuse evidence or limitation.
    pub connection_reuse_evidence: String,
    /// Maximum accepted server processor utilization.
    pub max_server_processor_usage_pct: f64,
    /// Server processor count.
    pub server_processors: usize,
    /// Optional server affinity mask.
    pub server_affinity: Option<u64>,
    /// Optional client affinity mask.
    pub client_affinity: Option<u64>,
    /// Rust compiler description.
    pub rustc: String,
    /// Cargo description.
    pub cargo: String,
    /// Operating system description.
    pub operating_system: String,
    /// CPU description.
    pub cpu: String,
    /// Active power plan.
    pub power_plan: String,
    /// Run-order seed.
    pub seed: u64,
}

/// Aggregated statistics for one runtime and cell.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CellStatistics {
    /// Matched workload cell.
    pub cell: Cell,
    /// Runtime under test.
    pub runtime: RuntimeKind,
    /// Accepted sample count.
    pub samples: usize,
    /// Median throughput.
    pub median_throughput_per_sec: f64,
    /// Minimum throughput.
    pub min_throughput_per_sec: f64,
    /// Maximum throughput.
    pub max_throughput_per_sec: f64,
}

/// Complete machine-readable run summary.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunSummary {
    /// Immutable run metadata.
    pub metadata: RunMetadata,
    /// Cell-level accepted-sample statistics.
    pub cells: Vec<CellStatistics>,
    /// All accepted and rejected samples.
    pub samples: Vec<Sample>,
}

/// Returns the balanced runtime order for one repetition.
#[must_use]
pub fn runtime_order(seed: u64, repetition: usize) -> [RuntimeKind; 2] {
    let first = if (seed.wrapping_add(repetition as u64) & 1) == 0 {
        RuntimeKind::Tokio
    } else {
        RuntimeKind::Arty
    };
    [first.clone(), first.other()]
}

/// Builds the deterministic cross-product matrix.
#[must_use]
pub fn build_matrix(workloads: &[Workload], protocols: &[Protocol], workers: &[usize], concurrency: &[usize]) -> Vec<Cell> {
    let mut cells = Vec::new();
    for workload in workloads {
        for protocol in protocols {
            for &worker_count in workers {
                for &concurrency in concurrency {
                    cells.push(Cell {
                        workload: workload.clone(),
                        protocol: protocol.clone(),
                        workers: worker_count,
                        concurrency,
                    });
                }
            }
        }
    }
    cells
}

/// Applies the objective sample rejection rules.
#[must_use]
pub fn rejection_reasons(sample: &Sample, max_server_processor_usage_pct: f64) -> Vec<String> {
    let mut reasons = Vec::new();
    if !sample.accounting.is_balanced() {
        reasons.push("request accounting is not balanced".to_owned());
    }
    if sample.accounting.failed != 0 || sample.accounting.cancelled != 0 {
        reasons.push("requests failed or were cancelled".to_owned());
    }
    if sample.accounting.physical_attempts != sample.accounting.attempted {
        reasons.push("physical attempts differ from logical attempts".to_owned());
    }
    if sample.server.requests != sample.accounting.successful {
        reasons.push("client and server request counts differ".to_owned());
    }
    if sample.latency.overflow != 0 {
        reasons.push("latency histogram overflowed".to_owned());
    }
    if sample.server.processor_usage_pct >= max_server_processor_usage_pct {
        reasons.push(format!(
            "server processor usage {:.2}% reached the configured {:.2}% limit",
            sample.server.processor_usage_pct, max_server_processor_usage_pct
        ));
    }
    reasons
}

/// Calculates cell-level statistics from accepted samples.
#[must_use]
pub fn summarize_cells(samples: &[Sample]) -> Vec<CellStatistics> {
    let mut groups: BTreeMap<(Cell, RuntimeKind), Vec<&Sample>> = BTreeMap::new();
    for sample in samples.iter().filter(|sample| sample.accepted()) {
        groups
            .entry((sample.cell.clone(), sample.runtime.clone()))
            .or_default()
            .push(sample);
    }

    groups
        .into_iter()
        .map(|((cell, runtime), samples)| {
            let mut throughput: Vec<f64> = samples.iter().map(|sample| sample.throughput_per_sec).collect();
            throughput.sort_by(f64::total_cmp);
            let throughput_median = median(&throughput);
            CellStatistics {
                cell,
                runtime,
                samples: samples.len(),
                median_throughput_per_sec: throughput_median,
                min_throughput_per_sec: throughput.first().copied().unwrap_or_default(),
                max_throughput_per_sec: throughput.last().copied().unwrap_or_default(),
            }
        })
        .collect()
}

fn median(values: &[f64]) -> f64 {
    match values.len() {
        0 => 0.0,
        length if length % 2 == 1 => values[length / 2],
        length => f64::midpoint(values[length / 2 - 1], values[length / 2]),
    }
}

/// Writes the local Markdown report.
#[must_use]
#[expect(
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::unwrap_used,
    reason = "writing formatted text to a String is infallible and the report stays linear"
)]
pub fn render_report(metadata: &RunMetadata, samples: &[Sample]) -> String {
    let accepted = samples.iter().filter(|sample| sample.accepted()).count();
    let rejected = samples.len().saturating_sub(accepted);
    let mut output = String::new();
    writeln!(output, "# Fetch runtime load comparison").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "This report compares the Fetch Tokio and Arty runtime integrations against one local Axum server."
    )
    .unwrap();
    writeln!(
        output,
        "The server uses Tokio and Hyper. This server design can favor related client behavior."
    )
    .unwrap();
    writeln!(
        output,
        "The closed-loop load model does not measure coordinated omission. Do not use these latency values as overload latency."
    )
    .unwrap();
    writeln!(output).unwrap();
    writeln!(output, "## Run context").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "| Field | Value |").unwrap();
    writeln!(output, "|---|---|").unwrap();
    writeln!(output, "| Oxidizer commit | `{}` |", metadata.oxidizer_commit).unwrap();
    writeln!(output, "| Dirty worktree | `{}` |", metadata.oxidizer_dirty).unwrap();
    writeln!(output, "| ox-sdk revision | `{}` |", metadata.ox_sdk_revision).unwrap();
    writeln!(output, "| Server binary | `{}` |", metadata.server_binary.display()).unwrap();
    writeln!(output, "| Server SHA-256 | `{}` |", metadata.server_sha256).unwrap();
    writeln!(output, "| SymCrypt SHA-256 | `{}` |", metadata.symcrypt_sha256).unwrap();
    writeln!(output, "| Server serving layer | `{}` |", metadata.server_serving_layer).unwrap();
    writeln!(output, "| Server transport | `{}` |", metadata.server_transport).unwrap();
    writeln!(output, "| Server arguments | `{}` |", metadata.server_arguments.join(" ")).unwrap();
    writeln!(output, "| Logical latency | `{:?}` ms |", metadata.server_logical_latency_ms).unwrap();
    writeln!(output, "| File logging | `{}` |", metadata.server_file_logging).unwrap();
    writeln!(output, "| Server processors | `{}` |", metadata.server_processors).unwrap();
    writeln!(output, "| Server affinity | `{:?}` |", metadata.server_affinity).unwrap();
    writeln!(output, "| Client affinity | `{:?}` |", metadata.client_affinity).unwrap();
    writeln!(output, "| Rust | `{}` |", metadata.rustc.replace('\n', " ")).unwrap();
    writeln!(output, "| Cargo | `{}` |", metadata.cargo.replace('\n', " ")).unwrap();
    writeln!(output, "| Operating system | `{}` |", metadata.operating_system.replace('\n', " ")).unwrap();
    writeln!(output, "| CPU | `{}` |", metadata.cpu.replace('\n', " ")).unwrap();
    writeln!(output, "| Power plan | `{}` |", metadata.power_plan.replace('\n', " ")).unwrap();
    writeln!(output, "| Order seed | `{}` |", metadata.seed).unwrap();
    writeln!(
        output,
        "| Server CPU rejection limit | `{:.2}%` |",
        metadata.max_server_processor_usage_pct
    )
    .unwrap();
    writeln!(output).unwrap();
    writeln!(output, "## Method").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "The client used the `{}` Fetch pipeline with retries set to `{}` and decompression set to `{}`.",
        metadata.client_pipeline, metadata.client_retries, metadata.client_decompression
    )
    .unwrap();
    writeln!(
        output,
        "The request-response workload sent 10,240 bytes and consumed 9,216 bytes in {} server chunks of {} bytes.",
        metadata.response_chunk_count, metadata.response_chunk_bytes
    )
    .unwrap();
    writeln!(
        output,
        "Each request was timed from submission through complete response body consumption."
    )
    .unwrap();
    writeln!(
        output,
        "The server receives `--logical-latency 0`. This selects its zero-duration latency hook for both client runtimes."
    )
    .unwrap();
    writeln!(
        output,
        "Connection reuse was independently verified: `{}`. {}",
        metadata.connection_reuse_verified, metadata.connection_reuse_evidence
    )
    .unwrap();
    writeln!(output).unwrap();
    writeln!(output, "## Cell summary").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "| Cell | Runtime | Samples | Median requests/s | Minimum requests/s | Maximum requests/s |"
    )
    .unwrap();
    writeln!(output, "|---|---:|---:|---:|---:|---:|").unwrap();
    for summary in summarize_cells(samples) {
        writeln!(
            output,
            "| {} | {} | {} | {:.2} | {:.2} | {:.2} |",
            summary.cell.id(),
            summary.runtime.as_str(),
            summary.samples,
            summary.median_throughput_per_sec,
            summary.min_throughput_per_sec,
            summary.max_throughput_per_sec
        )
        .unwrap();
    }
    writeln!(output).unwrap();
    writeln!(output, "## Sample status").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "The run produced {} accepted samples and {} rejected samples.",
        accepted, rejected
    )
    .unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "| Cell | Runtime | Repetition | Requests/s | Completed | Failed | Server CPU % | Status |"
    )
    .unwrap();
    writeln!(output, "|---|---:|---:|---:|---:|---:|---:|---|").unwrap();
    for sample in samples {
        let status = if sample.accepted() {
            "accepted".to_owned()
        } else {
            format!("rejected: {}", sample.rejection_reasons.join(", "))
        };
        writeln!(
            output,
            "| {} | {} | {} | {:.2} | {} | {} | {:.2} | {} |",
            sample.cell.id(),
            sample.runtime.as_str(),
            sample.repetition,
            sample.throughput_per_sec,
            sample.accounting.successful,
            sample.accounting.failed,
            sample.server.processor_usage_pct,
            status
        )
        .unwrap();
    }
    writeln!(output).unwrap();
    writeln!(output, "## Interpretation limits").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "Each result applies only to its endpoint, protocol, worker count, concurrency, machine state, and server budget."
    )
    .unwrap();
    writeln!(
        output,
        "A rejected sample cannot support a runtime ranking. The raw files retain its values and rejection reasons."
    )
    .unwrap();
    output
}
