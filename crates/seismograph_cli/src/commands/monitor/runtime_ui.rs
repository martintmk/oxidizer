// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Runtime workers and category-specific worker activity.

use std::cmp::Ordering;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Bar, BarChart, BarGroup, Cell, Paragraph, Row, Table, TableState};

use super::super::app::{RuntimeSectionFocus, RuntimeViewState, RuntimeWorkerSort, RuntimeWorkersFocus};
use super::super::data::{RuntimeMonitorSnapshot, RuntimeWorkerSummary};
use super::super::formatting::{format_count, format_histogram_duration, format_runtime_duration};
use super::super::mouse::{ListTarget, MouseRows};
use super::super::runtime_timeline::histogram;
use super::super::table_ui;
use super::draw_empty_panel_with_message;
use super::runtime_group_ui::aligned_details;

pub(super) fn draw(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    [workers, activity]: [Rect; 2],
    runtime: Option<&RuntimeMonitorSnapshot>,
    unavailable: Option<&str>,
    view: RuntimeViewState,
) {
    let Some(runtime) = runtime else {
        draw_empty_panel_with_message(frame, workers, " Runtime Workers ", unavailable);
        draw_empty_panel_with_message(frame, activity, " Worker Activity ", unavailable);
        return;
    };
    let selected_workers = workers_for(runtime, view);
    draw_workers(frame, mouse, workers, runtime, &selected_workers, view);
    let selected = selected_index(view).min(selected_workers.len().saturating_sub(1));
    draw_worker_activity(frame, activity, selected_workers.get(selected).copied(), view.focus);
}

fn workers_for(runtime: &RuntimeMonitorSnapshot, view: RuntimeViewState) -> Vec<&RuntimeWorkerSummary> {
    let runtime_id = runtime.selected_runtime(view.runtime_selected).map(|runtime| runtime.runtime_id);
    let mut workers = runtime
        .workers
        .iter()
        .filter(|worker| Some(worker.runtime_id) == runtime_id)
        .filter(|worker| worker.worker_id.is_some())
        .filter(|worker| match view.focus {
            RuntimeWorkersFocus::Asynchronous => !is_blocking(worker),
            RuntimeWorkersFocus::Blocking => is_blocking(worker),
        })
        .collect::<Vec<_>>();
    let (sort, descending) = match view.focus {
        RuntimeWorkersFocus::Asynchronous => (view.asynchronous_worker_sort, view.asynchronous_worker_descending),
        RuntimeWorkersFocus::Blocking => (view.blocking_worker_sort, view.blocking_worker_descending),
    };
    workers.sort_unstable_by(|left, right| compare_workers(left, right, sort, descending));
    workers
}

fn optional(left: Option<u64>, right: Option<u64>, descending: bool) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => table_ui::directed(left.cmp(&right), descending),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_workers(left: &RuntimeWorkerSummary, right: &RuntimeWorkerSummary, sort: RuntimeWorkerSort, descending: bool) -> Ordering {
    let ordering = match sort {
        RuntimeWorkerSort::WorkerId => table_ui::directed(left.worker_id.cmp(&right.worker_id), descending),
        RuntimeWorkerSort::State => table_ui::directed(left.state.cmp(&right.state), descending),
        RuntimeWorkerSort::Types => table_ui::directed(left.task_types.cmp(&right.task_types), descending),
        RuntimeWorkerSort::Tasks => table_ui::directed(left.observed_tasks.cmp(&right.observed_tasks), descending),
        RuntimeWorkerSort::Polls => table_ui::directed(left.metrics.poll_count.cmp(&right.metrics.poll_count), descending),
        RuntimeWorkerSort::Parks => table_ui::directed(left.park_events.cmp(&right.park_events), descending),
        RuntimeWorkerSort::Unparks => table_ui::directed(left.unpark_events.cmp(&right.unpark_events), descending),
        RuntimeWorkerSort::MedianPoll => optional(left.metrics.median_poll_nanos, right.metrics.median_poll_nanos, descending),
        RuntimeWorkerSort::MaxPoll => optional(left.metrics.max_poll_nanos, right.metrics.max_poll_nanos, descending),
        RuntimeWorkerSort::AverageScheduling => optional(left.scheduling_time.mean_nanos(), right.scheduling_time.mean_nanos(), descending),
        RuntimeWorkerSort::MaximumScheduling => optional(left.scheduling_time.max_nanos, right.scheduling_time.max_nanos, descending),
        RuntimeWorkerSort::AverageReady => optional(left.ready_time.mean_nanos(), right.ready_time.mean_nanos(), descending),
        RuntimeWorkerSort::MaximumReady => optional(left.ready_time.max_nanos, right.ready_time.max_nanos, descending),
        RuntimeWorkerSort::Runs => table_ui::directed(left.blocking.executions.cmp(&right.blocking.executions), descending),
        RuntimeWorkerSort::Succeeded => table_ui::directed(left.blocking.succeeded.cmp(&right.blocking.succeeded), descending),
        RuntimeWorkerSort::Canceled => table_ui::directed(left.blocking.canceled.cmp(&right.blocking.canceled), descending),
        RuntimeWorkerSort::Panicked => table_ui::directed(left.blocking.panicked.cmp(&right.blocking.panicked), descending),
        RuntimeWorkerSort::AverageQueue => optional(
            left.blocking.queue_time.mean_nanos(),
            right.blocking.queue_time.mean_nanos(),
            descending,
        ),
        RuntimeWorkerSort::MaximumQueue => optional(left.blocking.queue_time.max_nanos, right.blocking.queue_time.max_nanos, descending),
        RuntimeWorkerSort::AverageExecution => optional(
            left.blocking.execution_time.mean_nanos(),
            right.blocking.execution_time.mean_nanos(),
            descending,
        ),
        RuntimeWorkerSort::MaximumExecution => optional(
            left.blocking.execution_time.max_nanos,
            right.blocking.execution_time.max_nanos,
            descending,
        ),
    };
    ordering.then_with(|| left.worker_id.cmp(&right.worker_id))
}

fn is_blocking(worker: &RuntimeWorkerSummary) -> bool {
    worker.role == "Blocking" || worker.blocking.executions != 0
}

const fn selected_index(view: RuntimeViewState) -> usize {
    match view.focus {
        RuntimeWorkersFocus::Asynchronous => view.asynchronous_selected,
        RuntimeWorkersFocus::Blocking => view.blocking_selected,
    }
}

fn worker_name(worker: &RuntimeWorkerSummary) -> String {
    worker.worker_id.map_or_else(|| "Unknown".into(), |id| format!("w{id}"))
}

fn duration(value: Option<u64>) -> String {
    value.map_or_else(|| "-".into(), format_runtime_duration)
}

fn count_label(count: usize, singular: &str, plural: &str) -> String {
    format!("{count} {}", if count == 1 { singular } else { plural })
}

fn task_count_label(count: u64) -> String {
    format!("{} live {}", format_count(count), if count == 1 { "task" } else { "tasks" })
}

#[expect(
    clippy::too_many_lines,
    reason = "worker tabs and the responsive sortable table share one selection and mouse-target layout"
)]
fn draw_workers(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    area: Rect,
    runtime: &RuntimeMonitorSnapshot,
    workers: &[&RuntimeWorkerSummary],
    view: RuntimeViewState,
) {
    if area.is_empty() {
        return;
    }
    let [tabs, table] = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);
    let runtime_summary = runtime.selected_runtime(view.runtime_selected);
    let runtime_label = runtime_summary.map_or_else(
        || "Runtime: unavailable".into(),
        |runtime_summary| {
            let position = view.runtime_selected.min(runtime.runtimes.len().saturating_sub(1)) + 1;
            if runtime.runtimes.len() > 1 {
                format!(
                    "Runtime: {} (#{}) [{position}/{}] · [/] select",
                    runtime_summary.name,
                    runtime_summary.runtime_id,
                    runtime.runtimes.len()
                )
            } else {
                format!("Runtime: {} (#{})", runtime_summary.name, runtime_summary.runtime_id)
            }
        },
    );
    let tab_offset = u16::try_from(runtime_label.chars().count().saturating_add(3)).unwrap_or(u16::MAX);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw(runtime_label),
            Span::raw(" | "),
            Span::styled(
                "Async workers",
                table_ui::selection_style(view.focus == RuntimeWorkersFocus::Asynchronous),
            ),
            Span::raw(" | "),
            Span::styled(
                "Blocking workers",
                table_ui::selection_style(view.focus == RuntimeWorkersFocus::Blocking),
            ),
            Span::raw(" · a/b · Enter task types"),
        ])),
        tabs,
    );
    mouse.register_tab(
        Rect::new(tabs.x.saturating_add(tab_offset), tabs.y, 13, tabs.height).intersection(tabs),
        ListTarget::AsynchronousWorkersTab,
    );
    mouse.register_tab(
        Rect::new(tabs.x.saturating_add(tab_offset).saturating_add(16), tabs.y, 16, tabs.height).intersection(tabs),
        ListTarget::BlockingWorkersTab,
    );

    let selected = selected_index(view).min(workers.len().saturating_sub(1));
    let focused = view.section == RuntimeSectionFocus::Workers;
    let visible = usize::from(table.height.saturating_sub(3));
    let first = selected.saturating_sub(visible.saturating_sub(1));
    let live_tasks = match view.focus {
        RuntimeWorkersFocus::Asynchronous => runtime
            .asynchronous_groups
            .iter()
            .filter(|group| Some(group.runtime_id) == runtime_summary.map(|runtime| runtime.runtime_id))
            .fold(0_u64, |total, group| total.saturating_add(group.outcomes.active)),
        RuntimeWorkersFocus::Blocking => runtime
            .blocking_groups
            .iter()
            .filter(|group| Some(group.runtime_id) == runtime_summary.map(|runtime| runtime.runtime_id))
            .fold(0_u64, |total, group| total.saturating_add(group.outcomes.active)),
    };
    let title = format!(
        " {} · {} · {} ",
        match view.focus {
            RuntimeWorkersFocus::Asynchronous => "Async Workers",
            RuntimeWorkersFocus::Blocking => "Blocking Workers",
        },
        count_label(workers.len(), "worker", "workers"),
        task_count_label(live_tasks),
    );
    if workers.is_empty() {
        frame.render_widget(
            Paragraph::new(match view.focus {
                RuntimeWorkersFocus::Asynchronous => "No asynchronous workers are present in this capture.",
                RuntimeWorkersFocus::Blocking => "No blocking workers are present in this capture.",
            })
            .style(Style::default().fg(Color::DarkGray))
            .block(table_ui::focused_block(title, focused)),
            table,
        );
        return;
    }
    let wide = table.width >= 145;
    let rows = workers
        .iter()
        .skip(first)
        .take(visible)
        .map(|worker| worker_row(worker, view.focus, wide, table.width));
    let (headers, sorts, widths) = worker_columns(view.focus, wide, table.width);
    let targets = sorts
        .iter()
        .copied()
        .map(|sort| ListTarget::RuntimeWorkerHeader(view.focus, sort))
        .collect::<Vec<_>>();
    table_ui::register_headers(mouse, table, &widths, &targets);
    let (selected_sort, descending) = match view.focus {
        RuntimeWorkersFocus::Asynchronous => (view.asynchronous_worker_sort, view.asynchronous_worker_descending),
        RuntimeWorkersFocus::Blocking => (view.blocking_worker_sort, view.blocking_worker_descending),
    };
    let widget = Table::new(rows, widths)
        .header(
            Row::new(table_ui::sort_headers(headers, &sorts, selected_sort, descending))
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .column_spacing(1)
        .row_highlight_style(table_ui::selection_style(focused))
        .block(table_ui::focused_block(title, focused));
    let mut state = TableState::default().with_selected(Some(selected.saturating_sub(first)));
    frame.render_stateful_widget(widget, table, &mut state);
    mouse.register(
        table,
        1,
        first.saturating_add(state.offset()),
        workers.len(),
        match view.focus {
            RuntimeWorkersFocus::Asynchronous => ListTarget::AsynchronousWorkers,
            RuntimeWorkersFocus::Blocking => ListTarget::BlockingWorkers,
        },
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "responsive worker columns keep their sort targets and widths aligned"
)]
fn worker_columns(focus: RuntimeWorkersFocus, wide: bool, width: u16) -> (Vec<&'static str>, Vec<RuntimeWorkerSort>, Vec<Constraint>) {
    match (focus, wide) {
        (RuntimeWorkersFocus::Asynchronous, true) => (
            vec![
                "Worker ID",
                "State",
                "Types",
                "Tasks",
                "Polls",
                "Parks",
                "Unparks",
                "Median poll",
                "Max poll",
                "Avg sched",
                "Max sched",
                "Avg ready",
                "Max ready",
            ],
            vec![
                RuntimeWorkerSort::WorkerId,
                RuntimeWorkerSort::State,
                RuntimeWorkerSort::Types,
                RuntimeWorkerSort::Tasks,
                RuntimeWorkerSort::Polls,
                RuntimeWorkerSort::Parks,
                RuntimeWorkerSort::Unparks,
                RuntimeWorkerSort::MedianPoll,
                RuntimeWorkerSort::MaxPoll,
                RuntimeWorkerSort::AverageScheduling,
                RuntimeWorkerSort::MaximumScheduling,
                RuntimeWorkerSort::AverageReady,
                RuntimeWorkerSort::MaximumReady,
            ],
            vec![
                Constraint::Length(10),
                Constraint::Length(9),
                Constraint::Length(5),
                Constraint::Length(6),
                Constraint::Length(8),
                Constraint::Length(7),
                Constraint::Length(9),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
            ],
        ),
        (RuntimeWorkersFocus::Asynchronous, false) if width >= 95 => (
            vec![
                "Worker ID",
                "State",
                "Types",
                "Tasks",
                "Polls",
                "Max poll",
                "Avg sched",
                "Max sched",
                "Avg ready",
                "Max ready",
            ],
            vec![
                RuntimeWorkerSort::WorkerId,
                RuntimeWorkerSort::State,
                RuntimeWorkerSort::Types,
                RuntimeWorkerSort::Tasks,
                RuntimeWorkerSort::Polls,
                RuntimeWorkerSort::MaxPoll,
                RuntimeWorkerSort::AverageScheduling,
                RuntimeWorkerSort::MaximumScheduling,
                RuntimeWorkerSort::AverageReady,
                RuntimeWorkerSort::MaximumReady,
            ],
            vec![8, 8, 4, 5, 6, 10, 10, 10, 10, 10]
                .into_iter()
                .map(Constraint::Length)
                .collect(),
        ),
        (RuntimeWorkersFocus::Asynchronous, false) => (
            vec!["Worker ID", "Polls", "Avg sched", "Max ready"],
            vec![
                RuntimeWorkerSort::WorkerId,
                RuntimeWorkerSort::Polls,
                RuntimeWorkerSort::AverageScheduling,
                RuntimeWorkerSort::MaximumReady,
            ],
            vec![
                Constraint::Min(8),
                Constraint::Length(6),
                Constraint::Length(10),
                Constraint::Length(10),
            ],
        ),
        (RuntimeWorkersFocus::Blocking, true) => (
            vec![
                "Worker ID",
                "State",
                "Types",
                "Runs",
                "Succeeded",
                "Canceled",
                "Panicked",
                "Parks",
                "Unparks",
                "Avg queue",
                "Max queue",
                "Avg exec",
                "Max exec",
            ],
            vec![
                RuntimeWorkerSort::WorkerId,
                RuntimeWorkerSort::State,
                RuntimeWorkerSort::Types,
                RuntimeWorkerSort::Runs,
                RuntimeWorkerSort::Succeeded,
                RuntimeWorkerSort::Canceled,
                RuntimeWorkerSort::Panicked,
                RuntimeWorkerSort::Parks,
                RuntimeWorkerSort::Unparks,
                RuntimeWorkerSort::AverageQueue,
                RuntimeWorkerSort::MaximumQueue,
                RuntimeWorkerSort::AverageExecution,
                RuntimeWorkerSort::MaximumExecution,
            ],
            vec![10, 9, 5, 6, 9, 8, 8, 7, 9, 12, 12, 12, 12]
                .into_iter()
                .map(Constraint::Length)
                .collect(),
        ),
        (RuntimeWorkersFocus::Blocking, false) => (
            vec!["Worker ID", "Types", "Runs", "Succeeded", "Max queue", "Max exec"],
            vec![
                RuntimeWorkerSort::WorkerId,
                RuntimeWorkerSort::Types,
                RuntimeWorkerSort::Runs,
                RuntimeWorkerSort::Succeeded,
                RuntimeWorkerSort::MaximumQueue,
                RuntimeWorkerSort::MaximumExecution,
            ],
            vec![
                Constraint::Min(18),
                Constraint::Length(7),
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Length(12),
                Constraint::Length(12),
            ],
        ),
    }
}

fn worker_row(worker: &RuntimeWorkerSummary, focus: RuntimeWorkersFocus, wide: bool, width: u16) -> Row<'static> {
    let mut cells = vec![worker_name(worker)];
    match (focus, wide) {
        (RuntimeWorkersFocus::Asynchronous, true) => {
            cells.extend([
                worker.state.clone(),
                worker.task_types.to_string(),
                worker.observed_tasks.to_string(),
                format_count(worker.metrics.poll_count),
                format_count(worker.park_events),
                format_count(worker.unpark_events),
                duration(worker.metrics.median_poll_nanos),
                duration(worker.metrics.max_poll_nanos),
                duration(worker.scheduling_time.mean_nanos()),
                duration(worker.scheduling_time.max_nanos),
                duration(worker.ready_time.mean_nanos()),
                duration(worker.ready_time.max_nanos),
            ]);
        }
        (RuntimeWorkersFocus::Asynchronous, false) if width >= 95 => {
            cells.extend([
                worker.state.clone(),
                worker.task_types.to_string(),
                worker.observed_tasks.to_string(),
                format_count(worker.metrics.poll_count),
                duration(worker.metrics.max_poll_nanos),
                duration(worker.scheduling_time.mean_nanos()),
                duration(worker.scheduling_time.max_nanos),
                duration(worker.ready_time.mean_nanos()),
                duration(worker.ready_time.max_nanos),
            ]);
        }
        (RuntimeWorkersFocus::Asynchronous, false) => {
            cells.extend([
                format_count(worker.metrics.poll_count),
                duration(worker.scheduling_time.mean_nanos()),
                duration(worker.ready_time.max_nanos),
            ]);
        }
        (RuntimeWorkersFocus::Blocking, true) => {
            cells.extend([
                worker.state.clone(),
                worker.task_types.to_string(),
                format_count(worker.blocking.executions),
                format_count(worker.blocking.succeeded),
                format_count(worker.blocking.canceled),
                format_count(worker.blocking.panicked),
                format_count(worker.park_events),
                format_count(worker.unpark_events),
                duration(worker.blocking.queue_time.mean_nanos()),
                duration(worker.blocking.queue_time.max_nanos),
                duration(worker.blocking.execution_time.mean_nanos()),
                duration(worker.blocking.execution_time.max_nanos),
            ]);
        }
        (RuntimeWorkersFocus::Blocking, false) => {
            cells.extend([
                worker.task_types.to_string(),
                format_count(worker.blocking.executions),
                format_count(worker.blocking.succeeded),
                duration(worker.blocking.queue_time.max_nanos),
                duration(worker.blocking.execution_time.max_nanos),
            ]);
        }
    }
    Row::new(cells.into_iter().enumerate().map(|(index, value)| {
        let line = Line::from(value);
        Cell::from(if index >= 1 { line.right_aligned() } else { line })
    }))
}

fn draw_worker_activity(frame: &mut ratatui::Frame<'_>, area: Rect, worker: Option<&RuntimeWorkerSummary>, focus: RuntimeWorkersFocus) {
    if area.is_empty() {
        return;
    }
    let title = match focus {
        RuntimeWorkersFocus::Asynchronous => " Async Worker Activity ",
        RuntimeWorkersFocus::Blocking => " Blocking Worker Activity ",
    };
    let Some(worker) = worker else {
        draw_empty_panel_with_message(frame, area, title, Some("Select a worker."));
        return;
    };
    if focus == RuntimeWorkersFocus::Asynchronous {
        let [summary, chart] = Layout::vertical([Constraint::Length(8), Constraint::Min(0)]).areas(area);
        let lines = vec![
            Line::from(format!(
                "Thread: {}",
                worker.thread_id.map_or_else(|| "-".into(), |id| id.to_string())
            )),
            Line::from(format!(
                "Current task: {}",
                worker.current_task.map_or_else(|| "-".into(), |id| format!("#{id}"))
            )),
            Line::from(format!("Scheduling samples: {}", format_count(worker.scheduling_time.samples))),
            Line::from(format!("Ready wait samples: {}", format_count(worker.ready_time.samples))),
            Line::from(format!("Park events: {}", format_count(worker.park_events))),
            Line::from(format!("Unpark events: {}", format_count(worker.unpark_events))),
        ];
        frame.render_widget(
            Paragraph::new(aligned_details(lines)).block(table_ui::focused_block(title, false)),
            summary,
        );
        draw_poll_histogram(frame, chart, &worker.metrics.poll_samples);
        return;
    }
    let lines = vec![
        Line::from(format!(
            "Thread: {}",
            worker.thread_id.map_or_else(|| "-".into(), |id| id.to_string())
        )),
        Line::from(format!(
            "Current task: {}",
            worker.current_task.map_or_else(|| "-".into(), |id| format!("#{id}"))
        )),
        Line::from(format!("Queue samples: {}", format_count(worker.blocking.queue_time.samples))),
        Line::from(format!(
            "Total queue wait: {}",
            duration((worker.blocking.queue_time.samples != 0).then_some(worker.blocking.queue_time.total_nanos))
        )),
        Line::from(format!(
            "Execution samples: {}",
            format_count(worker.blocking.execution_time.samples)
        )),
        Line::from(format!(
            "Total execution: {}",
            duration((worker.blocking.execution_time.samples != 0).then_some(worker.blocking.execution_time.total_nanos))
        )),
    ];
    frame.render_widget(
        Paragraph::new(aligned_details(lines)).block(table_ui::focused_block(title, false)),
        area,
    );
}

fn draw_poll_histogram(frame: &mut ratatui::Frame<'_>, area: Rect, samples: &[u64]) {
    if samples.is_empty() {
        draw_empty_panel_with_message(frame, area, " Poll Duration ", Some("No completed poll samples."));
        return;
    }
    let count = usize::from(area.width.saturating_sub(2) / 9).clamp(1, 12);
    let bins = histogram(samples, count);
    let bars = bins
        .iter()
        .map(|bin| {
            Bar::default()
                .value(bin.count)
                .label(Line::from(format_histogram_duration(bin.lower)))
                .style(Style::default().fg(Color::Cyan))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        BarChart::default()
            .block(table_ui::focused_block(
                format!(
                    " Poll Duration · n={} · max {} ",
                    samples.len(),
                    duration(samples.iter().copied().max())
                ),
                false,
            ))
            .data(BarGroup::default().bars(&bars))
            .bar_width(5)
            .bar_gap(1)
            .max(bins.iter().map(|bin| bin.count).max().unwrap_or(1)),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::monitor::runtime::{BlockingWorkerSummary, DurationSummary, RuntimeSummary};
    use crate::commands::monitor::runtime_timeline::ExecutionMetrics;

    #[test]
    fn histogram_duration_labels_fit_fixed_width_bars() {
        for (nanos, expected) in [
            (999, "999ns"),
            (1_000, "1µs"),
            (999_999, "999µs"),
            (1_000_000, "1ms"),
            (999_999_999, "999ms"),
            (1_000_000_000, "1s"),
            (10_000_000_000, "10s"),
        ] {
            assert_eq!(format_histogram_duration(nanos), expected);
            assert!(expected.chars().count() <= 5);
        }
    }

    fn runtime() -> RuntimeMonitorSnapshot {
        RuntimeMonitorSnapshot {
            runtimes: vec![RuntimeSummary {
                runtime_id: 1,
                name: "demo".into(),
            }],
            workers: vec![
                RuntimeWorkerSummary {
                    runtime_id: 1,
                    runtime_name: "demo".into(),
                    worker_id: Some(1),
                    role: "Core".into(),
                    state: "Running".into(),
                    park_events: 5,
                    unpark_events: 6,
                    observed_tasks: 3,
                    task_types: 2,
                    scheduling_time: DurationSummary {
                        samples: 2,
                        total_nanos: 80,
                        max_nanos: Some(60),
                    },
                    ready_time: DurationSummary {
                        samples: 3,
                        total_nanos: 150,
                        max_nanos: Some(90),
                    },
                    metrics: ExecutionMetrics {
                        poll_count: 7,
                        median_poll_nanos: Some(20),
                        max_poll_nanos: Some(50),
                        ..ExecutionMetrics::default()
                    },
                    ..RuntimeWorkerSummary::default()
                },
                RuntimeWorkerSummary {
                    runtime_id: 1,
                    runtime_name: "demo".into(),
                    worker_id: Some(2),
                    role: "Blocking".into(),
                    state: "Running".into(),
                    park_events: 7,
                    unpark_events: 8,
                    task_types: 1,
                    blocking: BlockingWorkerSummary {
                        executions: 4,
                        succeeded: 3,
                        canceled: 1,
                        queue_time: DurationSummary {
                            samples: 4,
                            total_nanos: 80,
                            max_nanos: Some(40),
                        },
                        execution_time: DurationSummary {
                            samples: 4,
                            total_nanos: 400,
                            max_nanos: Some(200),
                        },
                        ..BlockingWorkerSummary::default()
                    },
                    ..RuntimeWorkerSummary::default()
                },
            ],
            ..RuntimeMonitorSnapshot::default()
        }
    }

    #[test]
    fn runtime_worker_grid_shows_scheduling_and_ready_delays_at_each_width() {
        let runtime = runtime();
        let view = RuntimeViewState::new();
        let workers = workers_for(&runtime, view);
        let mouse = MouseRows::default();
        for width in [80, 105, 150] {
            let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, 8)).unwrap();
            terminal
                .draw(|frame| {
                    mouse.begin(frame.area());
                    draw_workers(frame, &mouse, frame.area(), &runtime, &workers, view);
                })
                .unwrap();
            let text = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(ratatui::buffer::Cell::symbol)
                .collect::<String>();
            assert!(text.contains("Avg sched"));
            assert!(text.contains("Max ready"));
            assert!(text.contains("40ns"));
            assert!(text.contains("90ns"));
            assert!(!text.contains("Poll timeline"));
            if width >= 95 {
                assert!(text.contains("Max sched"));
                assert!(text.contains("Avg ready"));
                assert!(text.contains("60ns"));
                assert!(text.contains("50ns"));
            }
            if width >= 125 {
                assert!(text.contains("Median poll"));
                assert!(text.contains("20ns"));
            }
            if width >= 145 {
                assert!(text.contains("Parks"));
                assert!(text.contains("Unparks"));
            }
        }
        let mut other = runtime.workers[0].clone();
        other.worker_id = Some(3);
        other.scheduling_time.max_nanos = Some(30);
        assert_eq!(
            compare_workers(&runtime.workers[0], &other, RuntimeWorkerSort::MaximumScheduling, true),
            Ordering::Less
        );
        assert_eq!(
            compare_workers(&runtime.workers[0], &other, RuntimeWorkerSort::MaximumScheduling, false),
            Ordering::Greater
        );
    }

    #[test]
    fn worker_tabs_show_category_specific_metrics_without_observed_percentage() {
        let runtime = runtime();
        let mouse = MouseRows::default();
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(150, 16)).unwrap();
        for focus in [RuntimeWorkersFocus::Asynchronous, RuntimeWorkersFocus::Blocking] {
            terminal
                .draw(|frame| {
                    mouse.begin(frame.area());
                    let [workers, activity] = Layout::horizontal([Constraint::Percentage(65), Constraint::Min(0)]).areas(frame.area());
                    draw(
                        frame,
                        &mouse,
                        [workers, activity],
                        Some(&runtime),
                        None,
                        RuntimeViewState {
                            focus,
                            ..RuntimeViewState::new()
                        },
                    );
                })
                .unwrap();
            let text = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(ratatui::buffer::Cell::symbol)
                .collect::<String>();
            assert!(!text.contains("Observed %"));
            match focus {
                RuntimeWorkersFocus::Asynchronous => {
                    assert!(text.contains("Async Workers"));
                    assert!(text.contains("Async Worker Activity"));
                    assert!(text.contains("50ns"));
                    let details = text.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
                    assert!(details.contains("Park events: 5"), "{details}");
                    assert!(details.contains("Unpark events: 6"), "{details}");
                }
                RuntimeWorkersFocus::Blocking => {
                    assert!(text.contains("Blocking Workers"));
                    assert!(text.contains("Blocking Worker Activity"));
                    assert!(text.contains("200ns"));
                    let details = text.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
                    for expected in [
                        "Thread: -",
                        "Current task: -",
                        "Queue samples: 4",
                        "Total queue wait: 80ns",
                        "Execution samples: 4",
                        "Total execution: 400ns",
                    ] {
                        assert!(details.contains(expected), "{expected}: {details}");
                    }
                    for duplicate in [
                        "Worker:",
                        "Task types:",
                        "Runs:",
                        "Succeeded:",
                        "Maximum queue:",
                        "Maximum execution:",
                    ] {
                        assert!(!details.contains(duplicate), "{duplicate}: {details}");
                    }
                }
            }
        }
    }
}
