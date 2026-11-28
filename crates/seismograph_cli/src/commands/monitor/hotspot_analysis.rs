// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Reusable, evidence-backed performance findings for one retained snapshot.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::data::{
    AllocationSnapshot, AllocationStackFilter, CacheMonitorSnapshot, IoMonitorSnapshot, PrimitiveSnapshot, RuntimeMonitorSnapshot,
};
use super::formatting::{format_bytes, format_count, format_prose_duration};

const LARGE_ALLOCATION_BYTES: u64 = 256 * 1024;
const VERY_LARGE_ALLOCATION_BYTES: u64 = 1024 * 1024;
const HIGH_ALLOCATION_COUNT: u64 = 1_000;
const HIGH_ALLOCATED_BYTES: u64 = 8 * 1024 * 1024;
const CONTENTION_EVENTS: u64 = 50;
const LONG_ASYNC_POLL_NANOS: u64 = 10_000_000;
const VERY_LONG_ASYNC_POLL_NANOS: u64 = 50_000_000;
const SLOW_TASK_POLL_NANOS: u64 = 500_000;
const SLOW_TASK_POLL_PERCENT: u64 = 10;
const LONG_BLOCKING_QUEUE_NANOS: u64 = 50_000_000;
const VERY_LONG_BLOCKING_QUEUE_NANOS: u64 = 500_000_000;
const LONG_BLOCKING_EXECUTION_NANOS: u64 = 250_000_000;
const CACHE_LOOKUPS: u64 = 100;
const CACHE_MISS_PERCENT: u64 = 50;
const MAX_CATEGORY_FINDINGS: usize = 8;
const MAX_SLOW_POLL_TASK_FINDINGS: usize = 5;
const MAX_FINDINGS: usize = 20;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Severity {
    Medium,
    High,
}

impl Severity {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Medium => "Medium",
            Self::High => "High",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Category {
    Allocation,
    Contention,
    AsyncRuntime,
    BlockingRuntime,
    Cache,
    Io,
}

impl Category {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Allocation => "Allocation",
            Self::Contention => "Contention",
            Self::AsyncRuntime => "Async runtime",
            Self::BlockingRuntime => "Blocking runtime",
            Self::Cache => "Cache",
            Self::Io => "I/O",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Finding {
    pub(super) severity: Severity,
    pub(super) category: Category,
    pub(super) title: String,
    pub(super) evidence: String,
    pub(super) location: String,
    pub(super) explanation: String,
    pub(super) agent_instruction: String,
    pub(super) score: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Report {
    pub(super) findings: Vec<Finding>,
    pub(super) capture_warning: Option<String>,
}

pub(super) fn analyze(
    allocations: Option<&AllocationSnapshot>,
    primitives: &PrimitiveSnapshot,
    runtime: &RuntimeMonitorSnapshot,
    io: &IoMonitorSnapshot,
    cache: &CacheMonitorSnapshot,
) -> Report {
    let mut findings = Vec::new();
    if let Some(allocations) = allocations {
        allocation_findings(allocations, &mut findings);
    }
    contention_findings(primitives, &mut findings);
    runtime_findings(runtime, &mut findings);
    cache_findings(cache, &mut findings);
    io_findings(io, &mut findings);
    findings.sort_unstable_by(compare_findings);
    findings.dedup_by(|left, right| left.category == right.category && left.title == right.title && left.location == right.location);
    findings.truncate(MAX_FINDINGS);

    let (total_events, lost_events) = allocations.map_or((primitives.total_events, primitives.lost_events), |allocations| {
        (allocations.total_events, allocations.lost_events)
    });
    let capture_warning = capture_warning(total_events, lost_events);
    Report { findings, capture_warning }
}

fn allocation_findings(allocations: &AllocationSnapshot, findings: &mut Vec<Finding>) {
    let mut candidates = allocations
        .hotspots
        .iter()
        .filter_map(|hotspot| {
            let location = hotspot.location(AllocationStackFilter::Application);
            let large = hotspot.max_allocation_bytes >= LARGE_ALLOCATION_BYTES;
            let churn = hotspot.allocations >= HIGH_ALLOCATION_COUNT && hotspot.allocated_bytes >= HIGH_ALLOCATED_BYTES;
            (location != "Backtraces disabled" && (large || churn)).then_some((
                hotspot,
                location,
                large,
                churn,
                hotspot.allocated_bytes.max(hotspot.max_allocation_bytes),
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable_by_key(|(_, _, _, _, score)| std::cmp::Reverse(*score));
    for (hotspot, location, large, churn, score) in candidates.into_iter().take(MAX_CATEGORY_FINDINGS) {
        let severity = if hotspot.max_allocation_bytes >= VERY_LARGE_ALLOCATION_BYTES
            || hotspot.allocated_bytes >= HIGH_ALLOCATED_BYTES.saturating_mul(4)
        {
            Severity::High
        } else {
            Severity::Medium
        };
        let title = match (large, churn) {
            (true, true) => "Large repeated allocations",
            (true, false) => "Large allocation",
            (false, true) => "High allocation churn",
            (false, false) => unreachable!("guarded above"),
        };
        let evidence = format!(
            "{} allocations, {} total, {} maximum",
            format_count(hotspot.allocations),
            format_bytes(hotspot.allocated_bytes),
            format_bytes(hotspot.max_allocation_bytes)
        );
        let fix_direction = match (large, churn) {
            (true, true) => "Reuse storage across iterations and stream or chunk the largest temporary values.",
            (true, false) => "Reuse the large buffer when possible, or stream and process the value in bounded chunks.",
            (false, true) => "Reserve predictable capacity and reuse short-lived buffers or collections across iterations.",
            (false, false) => unreachable!("guarded above"),
        };
        findings.push(Finding {
            severity,
            category: Category::Allocation,
            title: title.into(),
            evidence: evidence.clone(),
            location: location.into(),
            explanation: "Repeated or large temporary allocations increase allocator work, memory traffic, and peak working set.".into(),
            agent_instruction: format!(
                "At {location}, reduce this allocation hotspot ({evidence}). {fix_direction} Preserve behavior and defensive checks. Add a \
                 focused test or benchmark, then compare allocation count, total bytes, and maximum allocation with the same workload."
            ),
            score,
        });
    }
}

fn contention_findings(primitives: &PrimitiveSnapshot, findings: &mut Vec<Finding>) {
    for group in &primitives.groups {
        for operation in group.operations.iter().filter(|operation| actionable_contention(operation.kind)) {
            if operation.events < CONTENTION_EVENTS {
                continue;
            }

            let Some(hotspot) = operation.hotspots.first() else {
                continue;
            };
            let location = hotspot.location(AllocationStackFilter::Application);
            if location == "Backtraces disabled" {
                continue;
            }
            let severity = if operation.events >= CONTENTION_EVENTS.saturating_mul(10) {
                Severity::High
            } else {
                Severity::Medium
            };
            let evidence = format!(
                "{} retained {} events across {} objects and {} threads; top stack {} events",
                format_count(operation.events),
                operation.kind.label(),
                format_count(operation.objects),
                format_count(operation.threads),
                format_count(hotspot.count)
            );
            findings.push(Finding {
                severity,
                category: Category::Contention,
                title: format!("{} contention", group.kind.label()),
                evidence: evidence.clone(),
                location: location.into(),
                explanation: "Frequent contention can serialize work and increase tail latency. Counts are retained events, not wait duration."
                    .into(),
                agent_instruction: format!(
                    "At {location}, reduce the observed {} ({evidence}). Shorten the critical section, move unrelated work outside the \
                     synchronization boundary, or partition shared state. Preserve synchronization semantics. Add a deterministic concurrency \
                     test and compare retained contention events with the same workload.",
                    operation.kind.label()
                ),
                score: operation.events,
            });
        }
    }
}

const fn actionable_contention(kind: super::data::PrimitiveOperationKind) -> bool {
    use super::data::PrimitiveOperationKind;
    matches!(
        kind,
        PrimitiveOperationKind::MutexContention
            | PrimitiveOperationKind::RwLockReadContention
            | PrimitiveOperationKind::RwLockWriteContention
            | PrimitiveOperationKind::OnceContention
            | PrimitiveOperationKind::ChannelSendContention
    )
}

fn runtime_findings(runtime: &RuntimeMonitorSnapshot, findings: &mut Vec<Finding>) {
    slow_poll_task_findings(runtime, findings);
    scheduling_delay_findings(runtime, findings);

    let mut asynchronous = runtime
        .asynchronous_groups
        .iter()
        .filter_map(|group| {
            let max_poll = group.poll_time.max_nanos?;
            let location = group.spawned_at.as_deref()?;
            long_async_poll(max_poll, group.poll_samples.len()).then_some((group, location, max_poll))
        })
        .collect::<Vec<_>>();
    asynchronous.sort_unstable_by_key(|(_, _, max_poll)| std::cmp::Reverse(*max_poll));
    for (group, location, max_poll) in asynchronous.into_iter().take(MAX_CATEGORY_FINDINGS) {
        let severity = if max_poll >= VERY_LONG_ASYNC_POLL_NANOS {
            Severity::High
        } else {
            Severity::Medium
        };
        let evidence = format!(
            "{} polls, {} average, {} maximum",
            format_count(group.polls),
            group.poll_time.mean_nanos().map_or_else(|| "-".into(), format_prose_duration),
            format_prose_duration(max_poll)
        );
        findings.push(Finding {
            severity,
            category: Category::AsyncRuntime,
            title: "Long async task polls".into(),
            evidence: evidence.clone(),
            location: location.into(),
            explanation: "A long poll occupies an async worker and delays unrelated ready tasks on that worker.".into(),
            agent_instruction: format!(
                "At {location}, shorten async poll execution ({evidence}). Move blocking or CPU-heavy work to the blocking pool, or split bounded \
                 work into smaller state-machine steps. Do not add unconditional yield loops that keep the same blocking work. Add a focused \
                 runtime test or benchmark and compare maximum poll duration."
            ),
            score: max_poll,
        });
    }

    let mut blocking = runtime
        .blocking_groups
        .iter()
        .filter_map(|group| {
            let max_queue = group.queue_time.max_nanos?;
            let location = group.spawned_at.as_deref()?;
            long_blocking_queue(max_queue, group.executions).then_some((group, location, max_queue))
        })
        .collect::<Vec<_>>();
    blocking.sort_unstable_by_key(|(_, _, max_queue)| std::cmp::Reverse(*max_queue));
    for (group, location, max_queue) in blocking.into_iter().take(MAX_CATEGORY_FINDINGS) {
        let max_execution = group.execution_time.max_nanos.unwrap_or_default();
        let severity = if max_queue >= VERY_LONG_BLOCKING_QUEUE_NANOS || max_execution >= 1_000_000_000 {
            Severity::High
        } else {
            Severity::Medium
        };
        let evidence = format!(
            "{} runs, {} average queue, {} maximum queue, {} maximum execution",
            format_count(group.executions),
            group.queue_time.mean_nanos().map_or_else(|| "-".into(), format_prose_duration),
            format_prose_duration(max_queue),
            group.execution_time.max_nanos.map_or_else(|| "-".into(), format_prose_duration)
        );
        let execution_advice = if long_blocking_execution(max_execution) {
            " Reduce callback duration or split large batches before increasing pool size."
        } else {
            " Reduce burst concurrency or batch submissions before increasing pool size."
        };
        findings.push(Finding {
            severity,
            category: Category::BlockingRuntime,
            title: "Blocking pool queue delay".into(),
            evidence: evidence.clone(),
            location: location.into(),
            explanation: "Blocking tasks waited before execution, which can delay every caller that depends on the shared pool.".into(),
            agent_instruction: format!(
                "At {location}, reduce blocking pool queue delay ({evidence}).{execution_advice} Preserve cancellation and error behavior. Add a \
                 focused load test and compare average and maximum queue duration."
            ),
            score: max_queue,
        });
    }
}

fn scheduling_delay_findings(runtime: &RuntimeMonitorSnapshot, findings: &mut Vec<Finding>) {
    let mut scheduling = runtime
        .asynchronous_groups
        .iter()
        .filter_map(|group| {
            let max_delay = group.ready_time.max_nanos?;
            let location = group.spawned_at.as_deref()?;
            long_scheduling_delay(max_delay, group.ready_samples.len()).then_some((group, location, max_delay))
        })
        .collect::<Vec<_>>();
    scheduling.sort_unstable_by_key(|(_, _, max_delay)| std::cmp::Reverse(*max_delay));
    for (group, location, max_delay) in scheduling.into_iter().take(MAX_CATEGORY_FINDINGS) {
        let severity = if max_delay >= VERY_LONG_ASYNC_POLL_NANOS {
            Severity::High
        } else {
            Severity::Medium
        };
        let evidence = format!(
            "{} samples, {} average, {} maximum",
            format_count(u64::try_from(group.ready_samples.len()).unwrap_or(u64::MAX)),
            group.ready_time.mean_nanos().map_or_else(|| "-".into(), format_prose_duration),
            format_prose_duration(max_delay)
        );
        findings.push(Finding {
            severity,
            category: Category::AsyncRuntime,
            title: "Long async scheduling delays".into(),
            evidence: evidence.clone(),
            location: location.into(),
            explanation: "A ready task waited too long before an async worker started its next poll.".into(),
            agent_instruction: format!(
                "At {location}, reduce async scheduling delay ({evidence}). Look for long polls, blocking work, or worker saturation. Preserve task \
                 behavior and cancellation. Add a focused runtime test and compare maximum scheduling delay."
            ),
            score: max_delay,
        });
    }
}

#[derive(Default)]
struct SlowPollTask {
    runtime_id: u64,
    task_id: u64,
    location: Option<String>,
    tracked_polls: u64,
    slow_polls: u64,
    max_poll_nanos: u64,
}

fn slow_poll_task_findings(runtime: &RuntimeMonitorSnapshot, findings: &mut Vec<Finding>) {
    let mut tasks = BTreeMap::<(u64, u64), SlowPollTask>::new();
    for task in runtime.workers.iter().flat_map(|worker| &worker.tasks) {
        let candidate = tasks.entry((task.runtime_id, task.task_id)).or_insert_with(|| SlowPollTask {
            runtime_id: task.runtime_id,
            task_id: task.task_id,
            location: task.spawned_at.clone(),
            ..SlowPollTask::default()
        });
        if candidate.location.is_none() {
            candidate.location.clone_from(&task.spawned_at);
        }
        candidate.tracked_polls = candidate
            .tracked_polls
            .saturating_add(u64::try_from(task.metrics.poll_samples.len()).unwrap_or(u64::MAX));
        candidate.slow_polls = candidate.slow_polls.saturating_add(
            u64::try_from(
                task.metrics
                    .poll_samples
                    .iter()
                    .filter(|duration| **duration > SLOW_TASK_POLL_NANOS)
                    .count(),
            )
            .unwrap_or(u64::MAX),
        );
        candidate.max_poll_nanos = candidate
            .max_poll_nanos
            .max(task.metrics.poll_samples.iter().copied().max().unwrap_or_default());
    }
    let mut candidates = tasks
        .into_values()
        .filter(|task| frequent_slow_polls(task.slow_polls, task.tracked_polls))
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|left, right| {
        (u128::from(right.slow_polls) * u128::from(left.tracked_polls))
            .cmp(&(u128::from(left.slow_polls) * u128::from(right.tracked_polls)))
            .then_with(|| right.slow_polls.cmp(&left.slow_polls))
            .then_with(|| right.max_poll_nanos.cmp(&left.max_poll_nanos))
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    for task in candidates.into_iter().take(MAX_SLOW_POLL_TASK_FINDINGS) {
        let tenths = u128::from(task.slow_polls).saturating_mul(1_000) / u128::from(task.tracked_polls);
        let evidence = format!(
            "{} of {} tracked polls ({}.{:01}%) exceeded {}; maximum {}",
            format_count(task.slow_polls),
            format_count(task.tracked_polls),
            tenths / 10,
            tenths % 10,
            format_prose_duration(SLOW_TASK_POLL_NANOS),
            format_prose_duration(task.max_poll_nanos),
        );
        let location = task
            .location
            .unwrap_or_else(|| format!("runtime #{} task #{}", task.runtime_id, task.task_id));
        let severity = if u128::from(task.slow_polls) * 2 >= u128::from(task.tracked_polls) && task.max_poll_nanos >= LONG_ASYNC_POLL_NANOS
        {
            Severity::High
        } else {
            Severity::Medium
        };
        let ratio_score = u64::try_from(tenths).unwrap_or(u64::MAX);
        findings.push(Finding {
            severity,
            category: Category::AsyncRuntime,
            title: format!("Async task #{} has frequent slow polls", task.task_id),
            evidence: evidence.clone(),
            location: location.clone(),
            explanation: "Frequent long polls occupy an async worker and can delay unrelated ready tasks.".into(),
            agent_instruction: format!(
                "At {location}, reduce slow polls for async task #{} ({evidence}). Move blocking or CPU-heavy work to the blocking pool, or split \
                 bounded work into smaller state-machine steps. Preserve task behavior and cancellation. Add a focused runtime test or benchmark \
                 and compare the percentage of polls above {}.",
                task.task_id,
                format_prose_duration(SLOW_TASK_POLL_NANOS),
            ),
            score: ratio_score
                .saturating_mul(1_000_000_000)
                .saturating_add(task.max_poll_nanos),
        });
    }
}

pub(super) fn frequent_slow_poll_samples(samples: &[u64]) -> bool {
    let tracked = u64::try_from(samples.len()).unwrap_or(u64::MAX);
    let slow = u64::try_from(samples.iter().filter(|duration| **duration > SLOW_TASK_POLL_NANOS).count()).unwrap_or(u64::MAX);
    frequent_slow_polls(slow, tracked)
}

fn frequent_slow_polls(slow: u64, tracked: u64) -> bool {
    tracked != 0 && u128::from(slow) * 100 > u128::from(tracked) * u128::from(SLOW_TASK_POLL_PERCENT)
}

pub(super) fn long_async_poll(max_poll_nanos: u64, samples: usize) -> bool {
    max_poll_nanos >= LONG_ASYNC_POLL_NANOS && (samples >= 3 || max_poll_nanos >= VERY_LONG_ASYNC_POLL_NANOS)
}

pub(super) fn long_scheduling_delay(max_delay_nanos: u64, samples: usize) -> bool {
    long_async_poll(max_delay_nanos, samples)
}

pub(super) fn long_blocking_queue(max_queue_nanos: u64, executions: u64) -> bool {
    max_queue_nanos >= LONG_BLOCKING_QUEUE_NANOS && executions >= 3
}

pub(super) const fn long_blocking_execution(execution_nanos: u64) -> bool {
    execution_nanos >= LONG_BLOCKING_EXECUTION_NANOS
}

fn cache_findings(cache: &CacheMonitorSnapshot, findings: &mut Vec<Finding>) {
    let mut tiers = cache
        .tiers
        .iter()
        .filter(|tier| {
            let lookups = tier.hits.saturating_add(tier.misses).saturating_add(tier.errors);
            tier.errors != 0 || (lookups >= CACHE_LOOKUPS && tier.misses.saturating_mul(100) >= lookups.saturating_mul(CACHE_MISS_PERCENT))
        })
        .collect::<Vec<_>>();
    tiers.sort_unstable_by_key(|tier| std::cmp::Reverse((tier.errors, tier.misses)));
    for tier in tiers.into_iter().take(MAX_CATEGORY_FINDINGS) {
        let lookups = tier.hits.saturating_add(tier.misses).saturating_add(tier.errors);
        if tier.errors != 0 {
            findings.push(Finding {
                severity: Severity::High,
                category: Category::Cache,
                title: "Cache operation errors".into(),
                evidence: format!(
                    "{} errors across {} lookup outcomes",
                    format_count(tier.errors),
                    format_count(lookups)
                ),
                location: format!("cache tier 0x{:x}", tier.tier_id),
                explanation: "Cache errors can turn an optimization into request failures or repeated fallback work.".into(),
                agent_instruction: format!(
                    "Find the cache instrumentation for tier 0x{:x}. Fix the {} retained cache errors without hiding them or converting them to \
                     hits. Preserve fallback behavior. Add deterministic error and recovery tests, then compare the error count.",
                    tier.tier_id, tier.errors
                ),
                score: tier.errors,
            });
        }
        if lookups < CACHE_LOOKUPS || tier.misses.saturating_mul(100) < lookups.saturating_mul(CACHE_MISS_PERCENT) {
            continue;
        }
        let miss_percent = u128::from(tier.misses).saturating_mul(100) / u128::from(lookups);
        findings.push(Finding {
            severity: Severity::Medium,
            category: Category::Cache,
            title: "Low cache hit rate".into(),
            evidence: format!(
                "{} hits, {} misses, {} errors ({}% misses)",
                format_count(tier.hits),
                format_count(tier.misses),
                format_count(tier.errors),
                miss_percent
            ),
            location: format!("cache tier 0x{:x}", tier.tier_id),
            explanation: "A low hit rate adds compute, allocation, or I/O work while retaining cache maintenance overhead.".into(),
            agent_instruction: format!(
                "Find the cache instrumentation for tier 0x{:x}. Improve the observed hit rate by checking key normalization, capacity, lifetime, \
                 and invalidation behavior. Do not extend lifetime beyond correctness requirements. Add deterministic hit, miss, and eviction tests, \
                 then compare the same workload.",
                tier.tier_id
            ),
            score: tier.misses,
        });
    }
}

fn io_findings(io: &IoMonitorSnapshot, findings: &mut Vec<Finding>) {
    let mut resources = io.resources.iter().filter(|resource| resource.errors != 0).collect::<Vec<_>>();
    resources.sort_unstable_by_key(|resource| std::cmp::Reverse(resource.errors));
    for resource in resources.into_iter().take(MAX_CATEGORY_FINDINGS) {
        findings.push(Finding {
            severity: Severity::High,
            category: Category::Io,
            title: "I/O failures".into(),
            evidence: format!(
                "{} errors across {} reads and {} writes",
                format_count(resource.errors),
                format_count(resource.reads),
                format_count(resource.writes)
            ),
            location: format!("I/O resource #{}", resource.resource_id),
            explanation: "Failed I/O can trigger retries, partial work, and latency amplification.".into(),
            agent_instruction: format!(
                "Find the instrumentation for I/O resource #{}. Handle the {} retained errors explicitly. Retry only idempotent operations with \
                 bounded policy, preserve the original error, and add success, partial-completion, and failure tests.",
                resource.resource_id, resource.errors
            ),
            score: resource.errors,
        });
    }
}

fn capture_warning(total_events: u64, lost_events: u64) -> Option<String> {
    if total_events == 0 || u128::from(lost_events) * 100 < u128::from(total_events) * 5 {
        return None;
    }
    let tenths = u128::from(lost_events).saturating_mul(1_000) / u128::from(total_events);
    Some(format!(
        "{}.{:01}% of accepted telemetry was overwritten. Findings are retained lower-bound evidence.",
        tenths / 10,
        tenths % 10
    ))
}

fn compare_findings(left: &Finding, right: &Finding) -> Ordering {
    right
        .severity
        .cmp(&left.severity)
        .then_with(|| right.score.cmp(&left.score))
        .then_with(|| left.category.cmp(&right.category))
        .then_with(|| left.location.cmp(&right.location))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::monitor::data::{CacheTierSummary, IoResourceSummary};
    use crate::commands::monitor::runtime::{
        AsynchronousTaskGroupSummary, BlockingTaskGroupSummary, DurationSummary, RuntimeTaskSummary, RuntimeWorkerSummary,
    };
    use crate::commands::monitor::runtime_timeline::ExecutionMetrics;

    fn primitives(total_events: u64, lost_events: u64) -> PrimitiveSnapshot {
        PrimitiveSnapshot {
            total_events,
            lost_events,
            groups: Vec::new(),
        }
    }

    #[test]
    fn blocking_queue_and_cache_miss_findings_include_agent_instructions() {
        let runtime = RuntimeMonitorSnapshot {
            blocking_groups: vec![BlockingTaskGroupSummary {
                spawned_at: Some("src/service.rs:42:9".into()),
                executions: 8,
                queue_time: DurationSummary {
                    samples: 8,
                    total_nanos: 4_000_000_000,
                    max_nanos: Some(900_000_000),
                },
                execution_time: DurationSummary {
                    samples: 8,
                    total_nanos: 8_000_000_000,
                    max_nanos: Some(1_200_000_000),
                },
                ..BlockingTaskGroupSummary::default()
            }],
            ..RuntimeMonitorSnapshot::default()
        };
        let cache = CacheMonitorSnapshot {
            total_events: 200,
            retained_events: 200,
            lost_events: 0,
            tiers: vec![CacheTierSummary {
                tier_id: 7,
                fallback: false,
                events: 200,
                hits: 40,
                misses: 160,
                errors: 0,
                operations: Vec::new(),
            }],
        };

        let report = analyze(None, &primitives(200, 0), &runtime, &IoMonitorSnapshot::default(), &cache);

        assert_eq!(report.findings.len(), 2);
        assert!(report.findings[0].agent_instruction.contains("focused load test"));
        assert!(report.findings[1].agent_instruction.contains("key normalization"));
        assert!(report.findings.iter().all(|finding| !finding.location.is_empty()));
    }

    #[test]
    fn scheduling_delay_findings_use_ready_to_poll_samples() {
        let runtime = RuntimeMonitorSnapshot {
            asynchronous_groups: vec![AsynchronousTaskGroupSummary {
                spawned_at: Some("src/tasks.rs:12:3".into()),
                ready_time: DurationSummary {
                    samples: 3,
                    total_nanos: 30_000_000,
                    max_nanos: Some(10_000_000),
                },
                ready_samples: vec![10_000_000; 3],
                ..AsynchronousTaskGroupSummary::default()
            }],
            ..RuntimeMonitorSnapshot::default()
        };

        let report = analyze(
            None,
            &primitives(3, 0),
            &runtime,
            &IoMonitorSnapshot::default(),
            &CacheMonitorSnapshot::default(),
        );

        let finding = report
            .findings
            .iter()
            .find(|finding| finding.title == "Long async scheduling delays")
            .unwrap();
        assert_eq!(finding.location, "src/tasks.rs:12:3");
        assert_eq!(finding.score, 10_000_000);
        assert!(finding.evidence.contains("3 samples"));
    }

    #[test]
    fn frequent_slow_poll_findings_are_strict_and_limited_to_top_five_tasks() {
        let task = |task_id: u64, slow_polls: usize| RuntimeTaskSummary {
            runtime_id: 1,
            task_id,
            spawned_at: Some(format!("src/tasks.rs:{task_id}:1")),
            metrics: ExecutionMetrics {
                poll_samples: (0..10).map(|index| if index < slow_polls { 600_000 } else { 500_000 }).collect(),
                ..ExecutionMetrics::default()
            },
            ..RuntimeTaskSummary::default()
        };
        let mut tasks = (1..=7)
            .map(|task_id| task(task_id, usize::try_from(10 - task_id).unwrap()))
            .collect::<Vec<_>>();
        tasks.extend([task(8, 1), task(9, 0)]);
        let runtime = RuntimeMonitorSnapshot {
            workers: vec![RuntimeWorkerSummary {
                runtime_id: 1,
                worker_id: Some(1),
                tasks,
                ..RuntimeWorkerSummary::default()
            }],
            ..RuntimeMonitorSnapshot::default()
        };

        let report = analyze(
            None,
            &primitives(70, 0),
            &runtime,
            &IoMonitorSnapshot::default(),
            &CacheMonitorSnapshot::default(),
        );
        let findings = report
            .findings
            .iter()
            .filter(|finding| finding.title.contains("frequent slow polls"))
            .collect::<Vec<_>>();

        assert_eq!(findings.len(), 5);
        assert_eq!(
            findings.iter().map(|finding| finding.title.as_str()).collect::<Vec<_>>(),
            [
                "Async task #1 has frequent slow polls",
                "Async task #2 has frequent slow polls",
                "Async task #3 has frequent slow polls",
                "Async task #4 has frequent slow polls",
                "Async task #5 has frequent slow polls",
            ]
        );
        assert!(findings[0].evidence.contains("9 of 10 tracked polls (90.0%)"));
        assert!(findings.iter().all(|finding| !finding.title.contains("#8")));
    }

    #[test]
    fn capture_warning_does_not_turn_loss_into_an_application_hotspot() {
        let report = analyze(
            None,
            &primitives(100, 25),
            &RuntimeMonitorSnapshot::default(),
            &IoMonitorSnapshot::default(),
            &CacheMonitorSnapshot::default(),
        );

        assert!(report.findings.is_empty());
        assert_eq!(
            report.capture_warning.as_deref(),
            Some("25.0% of accepted telemetry was overwritten. Findings are retained lower-bound evidence.")
        );
    }

    #[test]
    fn normal_coordination_waits_are_not_actionable_contention() {
        use crate::commands::monitor::data::PrimitiveOperationKind;

        assert!(!actionable_contention(PrimitiveOperationKind::NotifyContention));
        assert!(!actionable_contention(PrimitiveOperationKind::CondvarContention));
        assert!(actionable_contention(PrimitiveOperationKind::MutexContention));
    }

    #[test]
    fn canceled_io_without_errors_is_not_reported_as_a_hotspot() {
        let io = IoMonitorSnapshot {
            resources: vec![IoResourceSummary {
                resource_id: 1,
                kind: seismograph::recorder::io::IoResourceKind::File,
                events: 2,
                reads: 1,
                writes: 0,
                completed: 1,
                errors: 0,
                canceled: 1,
                requested_bytes: 10,
                completed_bytes: 0,
                operations: Vec::new(),
            }],
            ..IoMonitorSnapshot::default()
        };

        let report = analyze(
            None,
            &primitives(2, 0),
            &RuntimeMonitorSnapshot::default(),
            &io,
            &CacheMonitorSnapshot::default(),
        );

        assert!(report.findings.is_empty());
    }
}
