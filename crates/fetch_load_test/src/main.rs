// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Local Fetch runtime load-test executable.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use argh::FromArgs;
use fetch_load_test::{
    Cell, LoadModel, Protocol, RunMetadata, RunSummary, RuntimeKind, Sample, Workload, build_matrix, http, platform, rejection_reasons,
    render_report, runtime_order, summarize_cells, worker,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use worker::{WorkerOutput, WorkerSpec};

const DEFAULT_OX_SDK_REVISION: &str = "d3982012a647a9c483f128c449c9a4d2edce6d2c";
const DEFAULT_SERVER_SHA256: &str = "D39286133678F72DF5047A5AF7D92C121318D09127767D16AA4DD98ECB8C0A36";
const DEFAULT_SYMCRYPT_SHA256: &str = "738BB2E4B5BA996CAC7CAB8C976A1B07A267EDF53C3F68F809D70728DBF51FC5";

type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Compares Fetch runtime integrations with a local HTTP load server.
#[derive(Debug, FromArgs)]
struct Args {
    /// command to run
    #[argh(subcommand)]
    command: Subcommand,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand)]
enum Subcommand {
    Measure(Measure),
    Validate(Validate),
    Worker(Worker),
}

/// Runs the configured sample matrix and writes local artifacts.
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "measure")]
struct Measure {
    /// path to the approved `http_load_server` executable
    #[argh(option)]
    server_bin: Option<PathBuf>,

    /// directory containing symcrypt.dll
    #[argh(option)]
    symcrypt_dir: Option<PathBuf>,

    /// ox-sdk revision used to build the server
    #[argh(option, default = "DEFAULT_OX_SDK_REVISION.to_owned()")]
    ox_sdk_revision: String,

    /// expected server executable SHA-256
    #[argh(option, default = "DEFAULT_SERVER_SHA256.to_owned()")]
    expected_server_sha256: String,

    /// expected symcrypt.dll SHA-256
    #[argh(option, default = "DEFAULT_SYMCRYPT_SHA256.to_owned()")]
    expected_symcrypt_sha256: String,

    /// server processor count
    #[argh(option)]
    server_processors: usize,

    /// optional hexadecimal server affinity mask
    #[argh(option)]
    server_affinity: Option<AffinityMask>,

    /// optional hexadecimal client affinity mask
    #[argh(option)]
    client_affinity: Option<AffinityMask>,

    /// comma-separated runtime worker counts
    #[argh(option, default = "\"1,4\".to_owned()")]
    workers: String,

    /// comma-separated concurrency levels
    #[argh(option, default = "\"1,32,128\".to_owned()")]
    concurrency: String,

    /// comma-separated protocols: http1 or http2
    #[argh(option, default = "\"http1\".to_owned()")]
    protocol: String,

    /// warm-up duration
    #[argh(option, default = "DurationArg(Duration::from_secs(5))")]
    warmup: DurationArg,

    /// measured duration
    #[argh(option, default = "DurationArg(Duration::from_secs(15))")]
    duration: DurationArg,

    /// repetitions for each runtime and cell
    #[argh(option, default = "5")]
    repetitions: usize,

    /// deterministic runtime-order seed
    #[argh(option, default = "0x5eed")]
    seed: u64,

    /// maximum accepted server processor utilization percentage
    #[argh(option, default = "80.0")]
    max_server_processor_usage_pct: f64,

    /// output directory for raw data and report files
    #[argh(option)]
    output: PathBuf,
}

/// Runs focused correctness smoke coverage without the final matrix.
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "validate")]
struct Validate {
    /// path to the approved `http_load_server` executable
    #[argh(option)]
    server_bin: Option<PathBuf>,

    /// directory containing symcrypt.dll
    #[argh(option)]
    symcrypt_dir: Option<PathBuf>,

    /// ox-sdk revision used to build the server
    #[argh(option, default = "DEFAULT_OX_SDK_REVISION.to_owned()")]
    ox_sdk_revision: String,

    /// expected server executable SHA-256
    #[argh(option, default = "DEFAULT_SERVER_SHA256.to_owned()")]
    expected_server_sha256: String,

    /// expected symcrypt.dll SHA-256
    #[argh(option, default = "DEFAULT_SYMCRYPT_SHA256.to_owned()")]
    expected_symcrypt_sha256: String,

    /// server processor count
    #[argh(option)]
    server_processors: usize,

    /// optional hexadecimal server affinity mask
    #[argh(option)]
    server_affinity: Option<AffinityMask>,

    /// optional hexadecimal client affinity mask
    #[argh(option)]
    client_affinity: Option<AffinityMask>,
}

/// Runs one internal runtime sample.
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "worker")]
struct Worker {
    /// JSON worker specification path
    #[argh(option)]
    spec: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct AffinityMask(u64);

impl FromStr for AffinityMask {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.strip_prefix("0x").unwrap_or(value);
        let mask = u64::from_str_radix(value, 16).map_err(|error| error.to_string())?;
        if mask == 0 {
            return Err("an affinity mask must select at least one processor".to_owned());
        }
        Ok(Self(mask))
    }
}

#[derive(Clone, Copy, Debug)]
struct DurationArg(Duration);

impl FromStr for DurationArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
            (value, 1_u64)
        } else if let Some(value) = value.strip_suffix('s') {
            (value, 1_000)
        } else if let Some(value) = value.strip_suffix('m') {
            (value, 60_000)
        } else {
            return Err("duration must use ms, s, or m".to_owned());
        };
        let count = number.parse::<u64>().map_err(|error| error.to_string())?;
        Ok(Self(Duration::from_millis(count.saturating_mul(multiplier))))
    }
}

#[derive(Clone, Debug)]
struct Fixture {
    server_bin: PathBuf,
    symcrypt_dir: PathBuf,
    symcrypt_dll: PathBuf,
    ox_sdk_revision: String,
    server_sha256: String,
    symcrypt_sha256: String,
    processors: usize,
    server_affinity: Option<u64>,
    client_affinity: Option<u64>,
}

impl Fixture {
    fn server_arguments(&self, port: u16) -> Vec<String> {
        vec![
            "--serving-layer".to_owned(),
            "axum".to_owned(),
            "--port".to_owned(),
            port.to_string(),
            "--processors".to_owned(),
            self.processors.to_string(),
            "--logical-latency".to_owned(),
            "0".to_owned(),
        ]
    }

    fn server_argument_template(&self) -> Vec<String> {
        let mut arguments = self.server_arguments(0);
        "<fresh-port>".clone_into(&mut arguments[3]);
        arguments
    }
}

#[derive(Debug)]
struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn create(path: PathBuf) -> Result<Self, DynError> {
        fs::create_dir_all(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
struct ServerProcess {
    child: Child,
    group: platform::ChildGroup,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    stdout_thread: Option<thread::JoinHandle<()>>,
    stderr_thread: Option<thread::JoinHandle<()>>,
    port: u16,
    arguments: Vec<String>,
    stopped: bool,
}

#[derive(Debug)]
struct CompletedSample {
    sample: Sample,
    control_start_raw: Vec<u8>,
    control_end_raw: Vec<u8>,
}

impl ServerProcess {
    fn start(fixture: &Fixture) -> Result<Self, DynError> {
        let port = fresh_port()?;
        let path = prepend_path(&fixture.symcrypt_dir)?;
        let arguments = fixture.server_arguments(port);
        let mut child = Command::new(&fixture.server_bin)
            .args(&arguments)
            .env("PATH", path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let assignment = platform::ChildGroup::assign(&child, fixture.server_affinity);
        let group = cleanup_failed_setup(assignment, || kill_and_wait(&mut child))?;
        let stdout = Arc::new(Mutex::new(String::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let mut server = Self {
            child,
            group,
            stdout: Arc::clone(&stdout),
            stderr: Arc::clone(&stderr),
            stdout_thread: None,
            stderr_thread: None,
            port,
            arguments,
            stopped: false,
        };
        let (ready_tx, ready_rx) = mpsc::channel();

        let child_stdout = server.child.stdout.take().ok_or("server stdout was not piped")?;
        let stdout_copy = Arc::clone(&stdout);
        server.stdout_thread = Some(thread::spawn(move || {
            let reader = BufReader::new(child_stdout);
            let mut announced = false;
            for line in reader.lines().map_while(Result::ok) {
                if !announced && line.contains("Listening on ") {
                    let _ = ready_tx.send(());
                    announced = true;
                }
                let mut output = stdout_copy.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                output.push_str(&line);
                output.push('\n');
            }
        }));

        let child_stderr = server.child.stderr.take().ok_or("server stderr was not piped")?;
        let stderr_copy = Arc::clone(&stderr);
        server.stderr_thread = Some(thread::spawn(move || {
            let mut reader = BufReader::new(child_stderr);
            let mut buffer = String::new();
            let _ = reader.read_to_string(&mut buffer);
            *stderr_copy.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = buffer;
        }));

        if ready_rx.recv_timeout(Duration::from_secs(30)).is_err() {
            return Err("server did not announce readiness".into());
        }
        let readiness = http::request(port, "GET", "/hello-world")?;
        if readiness.status != 200 || readiness.body != b"Hello World!" {
            return Err("server readiness response was invalid".into());
        }
        let startup = stdout.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        validate_server_effective_config(&startup, fixture.processors)?;

        Ok(server)
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn stop(mut self) -> Result<(), DynError> {
        self.group.terminate()?;
        let _ = self.child.wait()?;
        if let Some(thread) = self.stdout_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr_thread.take() {
            let _ = thread.join();
        }
        self.stopped = true;
        Ok(())
    }

    fn diagnostics(&self) -> (String, String) {
        (
            self.stdout.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
            self.stderr.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
        )
    }

    fn arguments(&self) -> &[String] {
        &self.arguments
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        let _ = self.group.terminate();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr_thread.take() {
            let _ = thread.join();
        }
    }
}

fn main() -> Result<(), DynError> {
    let args: Args = argh::from_env();
    match args.command {
        Subcommand::Measure(command) => measure(command),
        Subcommand::Validate(command) => validate(command),
        Subcommand::Worker(command) => run_worker(&command.spec),
    }
}

fn measure(command: Measure) -> Result<(), DynError> {
    if command.repetitions == 0 {
        return Err("--repetitions must be greater than zero".into());
    }
    validate_server_cpu_limit(command.max_server_processor_usage_pct)?;
    let fixture = fixture(
        command.server_bin,
        command.symcrypt_dir,
        command.ox_sdk_revision,
        command.expected_server_sha256,
        command.expected_symcrypt_sha256,
        command.server_processors,
        command.server_affinity.map(|mask| mask.0),
        command.client_affinity.map(|mask| mask.0),
    )?;
    let workers = parse_usize_list(&command.workers)?;
    let concurrency = parse_usize_list(&command.concurrency)?;
    let protocols = parse_protocols(&command.protocol)?;
    let matrix = build_matrix(
        &[Workload::HelloWorld, Workload::RequestResponse],
        &protocols,
        &workers,
        &concurrency,
    );
    fs::create_dir_all(command.output.join("control"))?;
    fs::create_dir_all(command.output.join("histograms"))?;
    fs::create_dir_all(command.output.join("raw"))?;
    fs::create_dir_all(command.output.join("worker-specs"))?;
    let metadata = metadata(&fixture, command.seed, command.max_server_processor_usage_pct)?;
    write_json(command.output.join("metadata.json"), &metadata)?;
    write_json(command.output.join("matrix.json"), &matrix)?;
    let samples_path = command.output.join("samples.jsonl");
    let raw_samples_path = command.output.join("raw").join("samples.jsonl");
    if samples_path.exists() {
        fs::remove_file(&samples_path)?;
    }
    if raw_samples_path.exists() {
        fs::remove_file(&raw_samples_path)?;
    }

    let mut sample_index = 0;
    for cell in matrix {
        for repetition in 0..command.repetitions {
            for runtime in runtime_order(command.seed, repetition) {
                let completed = run_and_persist_sample(
                    || {
                        run_sample(
                            &fixture,
                            &command.output,
                            sample_index,
                            repetition,
                            runtime,
                            cell.clone(),
                            command.warmup.0,
                            command.duration.0,
                            command.max_server_processor_usage_pct,
                        )
                    },
                    |completed| append_json_line(&raw_samples_path, &completed.sample),
                )?;
                fs::write(
                    command.output.join("control").join(format!("{sample_index:04}-start.http")),
                    &completed.control_start_raw,
                )?;
                fs::write(
                    command.output.join("control").join(format!("{sample_index:04}-end.http")),
                    &completed.control_end_raw,
                )?;
                write_json(
                    command.output.join("control").join(format!("{sample_index:04}-parsed.json")),
                    &completed.sample.server,
                )?;
                write_json(
                    command.output.join("histograms").join(format!("{sample_index:04}.json")),
                    &completed.sample.latency,
                )?;
                sample_index += 1;
            }
        }
    }
    let samples: Vec<Sample> = read_json_lines(&raw_samples_path)?;
    write_json_lines(&samples_path, &samples)?;
    write_summary_files(&command.output, &metadata, &samples)?;
    Ok(())
}

fn validate(command: Validate) -> Result<(), DynError> {
    let fixture = fixture(
        command.server_bin,
        command.symcrypt_dir,
        command.ox_sdk_revision,
        command.expected_server_sha256,
        command.expected_symcrypt_sha256,
        command.server_processors,
        command.server_affinity.map(|mask| mask.0),
        command.client_affinity.map(|mask| mask.0),
    )?;
    let max_server_processor_usage_pct = 80.0;
    let output = TemporaryDirectory::create(std::env::temp_dir().join(format!("fetch-load-validate-{}", std::process::id())))?;
    fs::create_dir_all(output.path().join("worker-specs"))?;
    let mut sample_index = 0;
    for workload in [Workload::HelloWorld, Workload::RequestResponse] {
        for runtime in [RuntimeKind::Tokio, RuntimeKind::Arty] {
            let completed = run_sample(
                &fixture,
                output.path(),
                sample_index,
                0,
                runtime,
                Cell {
                    workload: workload.clone(),
                    protocol: Protocol::Http1,
                    workers: 1,
                    concurrency: 1,
                },
                Duration::from_secs(1),
                Duration::from_secs(1),
                max_server_processor_usage_pct,
            )?;
            let sample = completed.sample;
            if !sample.accepted() {
                return Err(format!(
                    "validation sample {} was rejected: {}",
                    sample.cell.id(),
                    sample.rejection_reasons.join(", ")
                )
                .into());
            }
            println!(
                "validated {} {} with {} requests",
                sample.runtime.as_str(),
                sample.cell.id(),
                sample.accounting.successful
            );
            sample_index += 1;
        }
    }
    Ok(())
}

#[expect(clippy::too_many_arguments, reason = "each sample coordinate is independent")]
fn run_sample(
    fixture: &Fixture,
    output: &Path,
    sample_index: usize,
    repetition: usize,
    runtime: RuntimeKind,
    cell: Cell,
    warmup: Duration,
    duration: Duration,
    max_server_processor_usage_pct: f64,
) -> Result<CompletedSample, DynError> {
    let server = ServerProcess::start(fixture)?;
    let spec = WorkerSpec {
        sample_index,
        repetition,
        runtime: runtime.clone(),
        cell: cell.clone(),
        load_model: LoadModel::ClosedLoop,
        port: server.port,
        server_pid: server.pid(),
        warmup,
        duration,
        affinity: fixture.client_affinity,
    };
    let spec_path = output.join("worker-specs").join(format!("{sample_index:04}.json"));
    write_json(&spec_path, &spec)?;
    let executable = std::env::current_exe()?;
    let mut child = Command::new(executable)
        .arg("worker")
        .arg("--spec")
        .arg(&spec_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let child_pid = child.id();
    let assignment = platform::ChildGroup::assign(&child, fixture.client_affinity);
    let client_group = cleanup_failed_setup(assignment, || kill_and_wait(&mut child))?;
    let output_result = child.wait_with_output();
    let output_result = match output_result {
        Ok(output) => output,
        Err(error) => {
            let _ = client_group.terminate();
            return Err(error.into());
        }
    };
    if !output_result.status.success() {
        let (server_stdout, server_stderr) = server.diagnostics();
        return Err(format!(
            "worker failed with {}\nworker stderr:\n{}\nserver stdout:\n{}\nserver stderr:\n{}",
            output_result.status,
            String::from_utf8_lossy(&output_result.stderr),
            server_stdout,
            server_stderr
        )
        .into());
    }
    let worker: WorkerOutput = serde_json::from_slice(&output_result.stdout)?;
    let mut sample = Sample {
        sample_index,
        repetition,
        runtime,
        cell,
        load_model: LoadModel::ClosedLoop,
        warmup_secs: warmup.as_secs_f64(),
        requested_duration_secs: duration.as_secs_f64(),
        client_duration_secs: worker.client_duration_secs,
        throughput_per_sec: worker.throughput_per_sec,
        accounting: worker.accounting,
        latency: worker.latency,
        client_resources: worker.client_resources,
        server: worker.server,
        server_pid: server.pid(),
        server_port: server.port,
        server_arguments: server.arguments().to_vec(),
        client_pid: child_pid,
        rejection_reasons: Vec::new(),
    };
    sample.rejection_reasons = rejection_reasons(&sample, max_server_processor_usage_pct);
    let completed = CompletedSample {
        sample,
        control_start_raw: worker.control_start_raw,
        control_end_raw: worker.control_end_raw,
    };
    server.stop()?;
    Ok(completed)
}

fn run_worker(path: &Path) -> Result<(), DynError> {
    let spec: WorkerSpec = serde_json::from_slice(&fs::read(path)?)?;
    let output = worker::run(&spec)?;
    serde_json::to_writer(std::io::stdout().lock(), &output)?;
    Ok(())
}

#[expect(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    reason = "the command transfers owned fixture identity values into one validated fixture"
)]
fn fixture(
    server_bin: Option<PathBuf>,
    symcrypt_dir: Option<PathBuf>,
    ox_sdk_revision: String,
    expected_server_sha256: String,
    expected_symcrypt_sha256: String,
    processors: usize,
    server_affinity: Option<u64>,
    client_affinity: Option<u64>,
) -> Result<Fixture, DynError> {
    if processors == 0 {
        return Err("--server-processors must be greater than zero".into());
    }
    let server_bin = server_bin
        .or_else(|| std::env::var_os("HTTP_LOAD_SERVER_BIN").map(PathBuf::from))
        .ok_or("--server-bin or HTTP_LOAD_SERVER_BIN is required")?;
    let symcrypt_dir = symcrypt_dir
        .or_else(|| std::env::var_os("SYMCRYPT_DIR").map(PathBuf::from))
        .ok_or("--symcrypt-dir or SYMCRYPT_DIR is required")?;
    let symcrypt_dll = symcrypt_dir.join("symcrypt.dll");
    let server_sha256 = file_sha256(&server_bin)?;
    let symcrypt_sha256 = file_sha256(&symcrypt_dll)?;
    if !server_sha256.eq_ignore_ascii_case(&expected_server_sha256) {
        return Err(format!("server SHA-256 mismatch: expected {expected_server_sha256}, got {server_sha256}").into());
    }
    if !symcrypt_sha256.eq_ignore_ascii_case(&expected_symcrypt_sha256) {
        return Err(format!("symcrypt.dll SHA-256 mismatch: expected {expected_symcrypt_sha256}, got {symcrypt_sha256}").into());
    }
    if let (Some(server), Some(client)) = (server_affinity, client_affinity)
        && server & client != 0
    {
        return Err("server and client affinity masks overlap".into());
    }
    Ok(Fixture {
        server_bin,
        symcrypt_dir,
        symcrypt_dll,
        ox_sdk_revision,
        server_sha256,
        symcrypt_sha256,
        processors,
        server_affinity,
        client_affinity,
    })
}

fn metadata(fixture: &Fixture, seed: u64, max_server_processor_usage_pct: f64) -> Result<RunMetadata, DynError> {
    let server_arguments = fixture.server_argument_template();
    Ok(RunMetadata {
        oxidizer_commit: command_output("git", &["rev-parse", "HEAD"])?,
        oxidizer_dirty: !command_output("git", &["status", "--porcelain"])?.is_empty(),
        ox_sdk_revision: fixture.ox_sdk_revision.clone(),
        server_binary: fixture.server_bin.clone(),
        server_sha256: fixture.server_sha256.clone(),
        symcrypt_dll: fixture.symcrypt_dll.clone(),
        symcrypt_sha256: fixture.symcrypt_sha256.clone(),
        server_arguments: server_arguments.clone(),
        server_serving_layer: argument_value(&server_arguments, "--serving-layer")
            .ok_or("server argument template has no serving layer")?
            .to_owned(),
        server_transport: if server_arguments.iter().any(|argument| argument == "--cert") {
            "TLS".to_owned()
        } else {
            "cleartext HTTP".to_owned()
        },
        server_logical_latency_ms: argument_value(&server_arguments, "--logical-latency")
            .map(str::parse)
            .transpose()?,
        server_file_logging: server_arguments.iter().any(|argument| argument == "--log-to-file"),
        response_chunk_count: 3,
        response_chunk_bytes: 3_072,
        client_pipeline: "minimal".to_owned(),
        client_retries: false,
        client_decompression: false,
        client_pool: "one Fetch client per active worker group, unlimited idle HTTP connections, no idle eviction".to_owned(),
        connection_reuse_verified: false,
        connection_reuse_evidence: "The public connection metadata has no stable connection identity, and this server control plane has no accepted-connection counter. The harness configures persistent pools and does not send `Connection: close`, but it does not use connection age as identity proof.".to_owned(),
        max_server_processor_usage_pct,
        server_processors: fixture.processors,
        server_affinity: fixture.server_affinity,
        client_affinity: fixture.client_affinity,
        rustc: command_output("rustc", &["-Vv"]).unwrap_or_else(|error| error.to_string()),
        cargo: command_output("cargo", &["-V"]).unwrap_or_else(|error| error.to_string()),
        operating_system: command_output("cmd", &["/c", "ver"]).unwrap_or_else(|error| error.to_string()),
        cpu: command_output(
            "powershell",
            &[
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name) -join '; '",
            ],
        )
        .unwrap_or_else(|error| error.to_string()),
        power_plan: command_output("powercfg", &["/getactivescheme"]).unwrap_or_else(|error| error.to_string()),
        seed,
    })
}

fn validate_server_cpu_limit(max_server_processor_usage_pct: f64) -> Result<(), DynError> {
    if !max_server_processor_usage_pct.is_finite() || !(0.0..=100.0).contains(&max_server_processor_usage_pct) {
        return Err("server processor usage threshold must be between 0 and 100".into());
    }
    Ok(())
}

fn argument_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1))
        .map(String::as_str)
}

fn validate_server_effective_config(startup: &str, processors: usize) -> Result<(), DynError> {
    let compact: String = startup.chars().filter(|character| !character.is_whitespace()).collect();
    let required = [
        "serving_layer:Axum",
        "cert:None",
        "key:None",
        "log_to_file:false",
        "logical_latency:Some(0,",
    ];
    for setting in required {
        if !compact.contains(setting) {
            return Err(format!("server effective configuration did not confirm `{setting}`").into());
        }
    }
    if !compact.contains(&format!("processors:Some({processors},")) {
        return Err("server effective configuration did not confirm the processor count".into());
    }
    Ok(())
}

fn write_summary_files(output: &Path, metadata: &RunMetadata, samples: &[Sample]) -> Result<(), DynError> {
    write_json(
        output.join("summary.json"),
        &RunSummary {
            metadata: metadata.clone(),
            cells: summarize_cells(samples),
            samples: samples.to_vec(),
        },
    )?;
    fs::write(output.join("report.md"), render_report(metadata, samples))?;
    let mut csv = String::from(
        "sample_index,repetition,runtime,cell,throughput_per_sec,completed,failed,server_processor_usage_pct,accepted,rejection_reasons\n",
    );
    for sample in samples {
        writeln!(
            csv,
            "{},{},{},{},{:.6},{},{},{:.6},{},{}",
            sample.sample_index,
            sample.repetition,
            sample.runtime.as_str(),
            sample.cell.id(),
            sample.throughput_per_sec,
            sample.accounting.successful,
            sample.accounting.failed,
            sample.server.processor_usage_pct,
            sample.accepted(),
            csv_field(&sample.rejection_reasons.join("; "))
        )?;
    }
    fs::write(output.join("samples.csv"), csv)?;
    Ok(())
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String, DynError> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(format!("{program} exited with {}", output.status).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn cleanup_failed_setup<T>(setup: Result<T, DynError>, cleanup: impl FnOnce() -> Result<(), DynError>) -> Result<T, DynError> {
    match setup {
        Ok(value) => Ok(value),
        Err(setup_error) => {
            cleanup().map_err(|cleanup_error| -> DynError {
                format!("child setup failed: {setup_error}; exact child cleanup also failed: {cleanup_error}").into()
            })?;
            Err(setup_error)
        }
    }
}

fn kill_and_wait(child: &mut Child) -> Result<(), DynError> {
    let kill = child.kill();
    let wait = child.wait();
    match (kill, wait) {
        (Ok(()), Ok(_status)) => Ok(()),
        (Err(kill_error), Ok(_status)) => Err(format!("failed to kill exact child before wait: {kill_error}").into()),
        (Ok(()), Err(wait_error)) => Err(format!("failed to wait for exact child after kill: {wait_error}").into()),
        (Err(kill_error), Err(wait_error)) => {
            Err(format!("failed to kill and wait for exact child: kill={kill_error}; wait={wait_error}").into())
        }
    }
}

fn file_sha256(path: &Path) -> Result<String, DynError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:X}", hasher.finalize()))
}

fn prepend_path(directory: &Path) -> Result<OsString, DynError> {
    let current = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![directory.to_path_buf()];
    paths.extend(std::env::split_paths(&current));
    Ok(std::env::join_paths(paths)?)
}

fn fresh_port() -> Result<u16, DynError> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

fn parse_usize_list(value: &str) -> Result<Vec<usize>, DynError> {
    let values = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<usize>)
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.contains(&0) {
        return Err("list values must be greater than zero".into());
    }
    Ok(values)
}

fn parse_protocols(value: &str) -> Result<Vec<Protocol>, DynError> {
    value
        .split(',')
        .map(str::trim)
        .map(|value| match value {
            "http1" => Ok(Protocol::Http1),
            "http2" => Ok(Protocol::Http2),
            _ => Err(format!("unknown protocol {value}").into()),
        })
        .collect()
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> Result<(), DynError> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn run_and_persist_sample<T, E>(run: impl FnOnce() -> Result<T, E>, persist: impl FnOnce(&T) -> Result<(), E>) -> Result<T, E> {
    let value = run()?;
    persist(&value)?;
    Ok(value)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> Result<(), DynError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn parse_json_lines<T: DeserializeOwned>(content: &str) -> Result<Vec<T>, DynError> {
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(Into::into))
        .collect()
}

fn read_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, DynError> {
    parse_json_lines(&fs::read_to_string(path)?)
}

fn write_json_lines(path: &Path, values: &[impl Serialize]) -> Result<(), DynError> {
    let mut file = File::create(path)?;
    for value in values {
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
