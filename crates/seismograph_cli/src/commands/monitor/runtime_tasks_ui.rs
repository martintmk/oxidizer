// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Descriptor-level asynchronous and blocking task summaries.

use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};

use super::super::app::{
    RuntimeSectionFocus, RuntimeTaskTypeSort, RuntimeViewState, RuntimeWorkersFocus, selected_runtime_worker_id, sort_runtime_async_groups,
    sort_runtime_blocking_groups,
};
use super::super::data::{AsynchronousTaskGroupSummary, BlockingTaskGroupSummary, RuntimeMonitorSnapshot};
use super::super::formatting::{format_count, format_runtime_duration};
use super::super::hotspot_analysis::{long_async_poll, long_blocking_execution, long_blocking_queue};
use super::super::mouse::{ListTarget, MouseRows};
use super::super::table_ui;
use super::draw_empty_panel_with_message;

pub(super) fn draw(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    area: Rect,
    runtime: Option<&RuntimeMonitorSnapshot>,
    unavailable: Option<&str>,
    view: RuntimeViewState,
) {
    let Some(runtime) = runtime else {
        draw_empty_panel_with_message(frame, area, " Task Types ", unavailable);
        return;
    };
    match view.focus {
        RuntimeWorkersFocus::Asynchronous => draw_asynchronous(frame, mouse, area, runtime, view),
        RuntimeWorkersFocus::Blocking => draw_blocking(frame, mouse, area, runtime, view),
    }
}

fn descriptor(id: Option<u64>) -> String {
    id.map_or_else(|| "not retained".into(), |id| format!("#{id}"))
}

fn task_type(spawned_at: Option<&str>, id: Option<u64>) -> String {
    spawned_at.map_or_else(
        || descriptor(id),
        |location| {
            let location = location.replace('\\', "/");
            let count = location.chars().count();
            if count <= 44 {
                location
            } else {
                format!("…{}", location.chars().skip(count - 43).collect::<String>())
            }
        },
    )
}

fn duration(value: Option<u64>) -> String {
    value.map_or_else(|| "-".into(), format_runtime_duration)
}

fn draw_empty(frame: &mut ratatui::Frame<'_>, area: Rect, title: String, message: &'static str, focused: bool) {
    frame.render_widget(
        Paragraph::new(message)
            .style(Style::default().fg(Color::DarkGray))
            .block(table_ui::focused_block(title, focused)),
        area,
    );
}

fn outcome_totals(groups: impl Iterator<Item = (u64, u64, u64, u64, u64)>) -> (u64, u64, u64, u64, u64) {
    groups.fold((0, 0, 0, 0, 0), |total, group| {
        (
            total.0.saturating_add(group.0),
            total.1.saturating_add(group.1),
            total.2.saturating_add(group.2),
            total.3.saturating_add(group.3),
            total.4.saturating_add(group.4),
        )
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "the responsive async task-type table keeps columns, sorting, selection, and totals aligned"
)]
fn draw_asynchronous(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    area: Rect,
    runtime: &RuntimeMonitorSnapshot,
    view: RuntimeViewState,
) {
    if area.is_empty() {
        return;
    }
    let runtime_id = runtime.selected_runtime(view.runtime_selected).map(|runtime| runtime.runtime_id);
    let worker_id = selected_runtime_worker_id(runtime, &view);
    let mut groups = runtime
        .asynchronous_groups
        .iter()
        .filter(|group| Some(group.runtime_id) == runtime_id)
        .filter(|group| group.worker_id == worker_id)
        .collect::<Vec<_>>();
    sort_runtime_async_groups(&mut groups, view);
    let focused = view.section == RuntimeSectionFocus::TaskTypes;
    let selected = view.asynchronous_task_selected.min(groups.len().saturating_sub(1));
    let visible = usize::from(area.height.saturating_sub(3));
    let first = selected.saturating_sub(visible.saturating_sub(1));
    let (tasks, active, _, _, _) = outcome_totals(groups.iter().map(|group| {
        (
            group.outcomes.tasks,
            group.outcomes.active,
            group.outcomes.completed,
            group.outcomes.canceled,
            group.outcomes.panicked,
        )
    }));
    let polls = groups.iter().fold(0_u64, |total, group| total.saturating_add(group.polls));
    let title = format!(
        " Async Task Types · {} · {} types · {} spawned · {} live · {} polls ",
        worker_id.map_or_else(|| "unassigned".into(), |worker| format!("#{worker}")),
        groups.len(),
        format_count(tasks),
        format_count(active),
        format_count(polls),
    );
    if groups.is_empty() {
        draw_empty(
            frame,
            area,
            title,
            "No asynchronous task polls are retained in this capture.",
            focused,
        );
        return;
    }
    let wide = area.width >= 145;
    let rows = groups.iter().skip(first).take(visible).map(|group| asynchronous_row(group, wide));
    let (headers, widths, sorts): (Vec<_>, Vec<_>, Vec<_>) = if wide {
        (
            vec![
                "Spawned at",
                "Future B",
                "Total",
                "Live",
                "Polls",
                "Avg ready",
                "Max ready",
                "Avg poll",
                "Max poll",
            ],
            vec![42, 10, 8, 8, 10, 12, 12, 12, 12].into_iter().map(Constraint::Length).collect(),
            vec![
                RuntimeTaskTypeSort::SpawnedAt,
                RuntimeTaskTypeSort::BodySize,
                RuntimeTaskTypeSort::Total,
                RuntimeTaskTypeSort::Live,
                RuntimeTaskTypeSort::PollsOrRuns,
                RuntimeTaskTypeSort::AverageReadyOrQueue,
                RuntimeTaskTypeSort::MaximumReadyOrQueue,
                RuntimeTaskTypeSort::AverageDuration,
                RuntimeTaskTypeSort::MaximumDuration,
            ],
        )
    } else {
        (
            vec!["Spawned at", "Future B", "Total", "Live", "Polls", "Max poll"],
            vec![
                Constraint::Min(24),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Length(12),
            ],
            vec![
                RuntimeTaskTypeSort::SpawnedAt,
                RuntimeTaskTypeSort::BodySize,
                RuntimeTaskTypeSort::Total,
                RuntimeTaskTypeSort::Live,
                RuntimeTaskTypeSort::PollsOrRuns,
                RuntimeTaskTypeSort::MaximumDuration,
            ],
        )
    };
    let targets = sorts.iter().copied().map(ListTarget::RuntimeTaskTypeHeader).collect::<Vec<_>>();
    table_ui::register_headers(mouse, area, &widths, &targets);
    let table = Table::new(rows, widths)
        .header(
            Row::new(table_ui::sort_headers(
                headers,
                &sorts,
                view.task_type_sort,
                view.task_type_sort_descending,
            ))
            .add_modifier(Modifier::BOLD),
        )
        .column_spacing(1)
        .row_highlight_style(table_ui::selection_style(focused))
        .block(table_ui::focused_block(title, focused));
    let mut state = TableState::default().with_selected(Some(selected.saturating_sub(first)));
    frame.render_stateful_widget(table, area, &mut state);
    mouse.register(
        area,
        1,
        first.saturating_add(state.offset()),
        groups.len(),
        ListTarget::AsynchronousTaskGroups,
    );
}

fn asynchronous_row(group: &AsynchronousTaskGroupSummary, wide: bool) -> Row<'static> {
    let mut cells = vec![
        task_type(group.spawned_at.as_deref(), group.type_descriptor_id),
        group.body_size_bytes.map_or_else(|| "-".into(), |size| size.to_string()),
        format_count(group.outcomes.tasks),
    ];
    if wide {
        cells.extend([
            format_count(group.outcomes.active),
            format_count(group.polls),
            duration(group.ready_time.mean_nanos()),
            duration(group.ready_time.max_nanos),
            duration(group.poll_time.mean_nanos()),
            duration(group.poll_time.max_nanos),
        ]);
    } else {
        cells.extend([
            format_count(group.outcomes.active),
            format_count(group.polls),
            duration(group.poll_time.max_nanos),
        ]);
    }
    let max_poll_index = if wide { 8 } else { 5 };
    let poll_hotspot = group
        .poll_time
        .max_nanos
        .is_some_and(|maximum| long_async_poll(maximum, group.poll_samples.len()));
    Row::new(cells.into_iter().enumerate().map(|(index, value)| {
        let line = Line::from(value);
        let cell = Cell::from(if index == 0 { line } else { line.right_aligned() });
        if index == max_poll_index && poll_hotspot {
            cell.style(Style::default().fg(Color::Yellow))
        } else {
            cell
        }
    }))
}

#[expect(
    clippy::too_many_lines,
    reason = "the responsive blocking task-type table keeps columns, sorting, selection, and totals aligned"
)]
fn draw_blocking(frame: &mut ratatui::Frame<'_>, mouse: &MouseRows, area: Rect, runtime: &RuntimeMonitorSnapshot, view: RuntimeViewState) {
    if area.is_empty() {
        return;
    }
    let runtime_id = runtime.selected_runtime(view.runtime_selected).map(|runtime| runtime.runtime_id);
    let worker_id = selected_runtime_worker_id(runtime, &view);
    let mut groups = runtime
        .blocking_groups
        .iter()
        .filter(|group| Some(group.runtime_id) == runtime_id)
        .filter(|group| group.worker_id == worker_id)
        .collect::<Vec<_>>();
    sort_runtime_blocking_groups(&mut groups, view);
    let focused = view.section == RuntimeSectionFocus::TaskTypes;
    let selected = view.blocking_task_selected.min(groups.len().saturating_sub(1));
    let visible = usize::from(area.height.saturating_sub(3));
    let first = selected.saturating_sub(visible.saturating_sub(1));
    let (tasks, active, _, _, _) = outcome_totals(groups.iter().map(|group| {
        (
            group.outcomes.tasks,
            group.outcomes.active,
            group.outcomes.completed,
            group.outcomes.canceled,
            group.outcomes.panicked,
        )
    }));
    let executions = groups.iter().fold(0_u64, |total, group| total.saturating_add(group.executions));
    let title = format!(
        " Blocking Task Types · {} · {} types · {} spawned · {} runs · {} live · {} unclassified ",
        worker_id.map_or_else(|| "unassigned".into(), |worker| format!("#{worker}")),
        groups.len(),
        format_count(tasks),
        format_count(executions),
        format_count(active),
        format_count(runtime.unclassified_tasks),
    );
    if groups.is_empty() {
        draw_empty(
            frame,
            area,
            title,
            "No blocking task executions are retained in this capture.",
            focused,
        );
        return;
    }
    let wide = area.width >= 145;
    let rows = groups.iter().skip(first).take(visible).map(|group| blocking_row(group, wide));
    let (headers, widths, sorts): (Vec<_>, Vec<_>, Vec<_>) = if wide {
        (
            vec![
                "Spawned at",
                "Closure B",
                "Total",
                "Runs",
                "Live",
                "Avg queue",
                "Max queue",
                "Avg exec",
                "Max exec",
            ],
            vec![42, 10, 8, 8, 8, 12, 12, 12, 12].into_iter().map(Constraint::Length).collect(),
            vec![
                RuntimeTaskTypeSort::SpawnedAt,
                RuntimeTaskTypeSort::BodySize,
                RuntimeTaskTypeSort::Total,
                RuntimeTaskTypeSort::PollsOrRuns,
                RuntimeTaskTypeSort::Live,
                RuntimeTaskTypeSort::AverageReadyOrQueue,
                RuntimeTaskTypeSort::MaximumReadyOrQueue,
                RuntimeTaskTypeSort::AverageDuration,
                RuntimeTaskTypeSort::MaximumDuration,
            ],
        )
    } else {
        (
            vec!["Spawned at", "Closure B", "Total", "Runs", "Live", "Max queue", "Max exec"],
            vec![
                Constraint::Min(24),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(12),
                Constraint::Length(12),
            ],
            vec![
                RuntimeTaskTypeSort::SpawnedAt,
                RuntimeTaskTypeSort::BodySize,
                RuntimeTaskTypeSort::Total,
                RuntimeTaskTypeSort::PollsOrRuns,
                RuntimeTaskTypeSort::Live,
                RuntimeTaskTypeSort::MaximumReadyOrQueue,
                RuntimeTaskTypeSort::MaximumDuration,
            ],
        )
    };
    let targets = sorts.iter().copied().map(ListTarget::RuntimeTaskTypeHeader).collect::<Vec<_>>();
    table_ui::register_headers(mouse, area, &widths, &targets);
    let table = Table::new(rows, widths)
        .header(
            Row::new(table_ui::sort_headers(
                headers,
                &sorts,
                view.task_type_sort,
                view.task_type_sort_descending,
            ))
            .add_modifier(Modifier::BOLD),
        )
        .column_spacing(1)
        .row_highlight_style(table_ui::selection_style(focused))
        .block(table_ui::focused_block(title, focused));
    let mut state = TableState::default().with_selected(Some(selected.saturating_sub(first)));
    frame.render_stateful_widget(table, area, &mut state);
    mouse.register(
        area,
        1,
        first.saturating_add(state.offset()),
        groups.len(),
        ListTarget::BlockingTaskGroups,
    );
}

fn blocking_row(group: &BlockingTaskGroupSummary, wide: bool) -> Row<'static> {
    let mut cells = vec![
        task_type(group.spawned_at.as_deref(), group.type_descriptor_id),
        group.body_size_bytes.map_or_else(|| "-".into(), |size| size.to_string()),
        format_count(group.outcomes.tasks),
        format_count(group.executions),
    ];
    if wide {
        cells.extend([
            format_count(group.outcomes.active),
            duration(group.queue_time.mean_nanos()),
            duration(group.queue_time.max_nanos),
            duration(group.execution_time.mean_nanos()),
            duration(group.execution_time.max_nanos),
        ]);
    } else {
        cells.extend([
            format_count(group.outcomes.active),
            duration(group.queue_time.max_nanos),
            duration(group.execution_time.max_nanos),
        ]);
    }
    let (max_queue_index, max_execution_index) = if wide { (6, 8) } else { (5, 6) };
    let queue_hotspot = group
        .queue_time
        .max_nanos
        .is_some_and(|maximum| long_blocking_queue(maximum, group.executions));
    let execution_hotspot = group.execution_time.max_nanos.is_some_and(long_blocking_execution);
    Row::new(cells.into_iter().enumerate().map(|(index, value)| {
        let line = Line::from(value);
        let cell = Cell::from(if index == 0 { line } else { line.right_aligned() });
        if (index == max_queue_index && queue_hotspot) || (index == max_execution_index && execution_hotspot) {
            cell.style(Style::default().fg(Color::Yellow))
        } else {
            cell
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::monitor::runtime::{DurationSummary, RuntimeSummary, TaskOutcomeSummary};

    #[test]
    fn task_group_tables_show_category_specific_statistics() {
        let runtime = RuntimeMonitorSnapshot {
            runtimes: vec![RuntimeSummary {
                runtime_id: 1,
                name: "test".into(),
            }],
            asynchronous_groups: vec![AsynchronousTaskGroupSummary {
                runtime_id: 1,
                worker_id: None,
                type_descriptor_id: Some(42),
                spawned_at: Some("src/async.rs:1:1".into()),
                body_size_bytes: Some(128),
                outcomes: TaskOutcomeSummary {
                    tasks: 3,
                    active: 1,
                    completed: 1,
                    canceled: 1,
                    workers: 2,
                    ..TaskOutcomeSummary::default()
                },
                polls: 9,
                ready_time: DurationSummary::default(),
                poll_time: DurationSummary {
                    samples: 3,
                    total_nanos: 30_000_000,
                    max_nanos: Some(10_000_000),
                },
                ready_samples: Vec::new(),
                poll_samples: vec![10_000_000; 3],
            }],
            blocking_groups: vec![BlockingTaskGroupSummary {
                runtime_id: 1,
                worker_id: None,
                type_descriptor_id: Some(77),
                spawned_at: Some("src/blocking.rs:2:1".into()),
                body_size_bytes: Some(64),
                outcomes: TaskOutcomeSummary {
                    tasks: 3,
                    completed: 1,
                    canceled: 1,
                    workers: 1,
                    ..TaskOutcomeSummary::default()
                },
                executions: 3,
                queue_time: DurationSummary {
                    samples: 3,
                    total_nanos: 150_000_000,
                    max_nanos: Some(50_000_000),
                },
                execution_time: DurationSummary {
                    samples: 3,
                    total_nanos: 750_000_000,
                    max_nanos: Some(300_000_000),
                },
                queue_samples: vec![50_000_000; 3],
                execution_samples: vec![250_000_000; 3],
            }],
            unclassified_tasks: 1,
            ..RuntimeMonitorSnapshot::default()
        };
        let mouse = MouseRows::default();
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(180, 18)).unwrap();
        for (focus, expected) in [
            (RuntimeWorkersFocus::Asynchronous, "src/async.rs:1:1"),
            (RuntimeWorkersFocus::Blocking, "src/blocking.rs:2:1"),
        ] {
            let mut view = RuntimeViewState::new();
            view.focus = focus;
            terminal
                .draw(|frame| {
                    mouse.begin(frame.area());
                    draw(frame, &mouse, frame.area(), Some(&runtime), None, view);
                })
                .unwrap();
            let text = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(ratatui::buffer::Cell::symbol)
                .collect::<String>();
            assert!(text.contains(expected));
            assert!(text.contains("Total"));
            assert!(
                terminal.backend().buffer().content.iter().any(|cell| cell.fg == Color::Yellow),
                "{focus:?}"
            );
            for moved in ["Succeeded", "Canceled", "Panicked", "Workers"] {
                assert!(!text.contains(moved), "{focus:?}: {moved}");
            }
        }
    }
}
