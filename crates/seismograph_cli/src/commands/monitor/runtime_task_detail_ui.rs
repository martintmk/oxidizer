// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Rich statistics for one task selected from a task-type group.

#[cfg(test)]
use ratatui::layout::Layout;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Row, Table, TableState};

use super::super::app::{RuntimeDetailMode, RuntimeViewState, TaskPollSort};
use super::super::data::{RuntimeMonitorSnapshot, RuntimeTaskSummary};
use super::super::formatting::format_runtime_duration;
use super::super::mouse::{ListTarget, MouseRows};
use super::super::runtime::RuntimeTaskKind;
use super::super::table_ui;
use super::draw_empty_panel_with_message;
use super::runtime_group_ui::{draw_details, draw_histogram, draw_histogram_pair, draw_summary, statistics_heading};

pub(super) fn draw(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    task: Option<&RuntimeTaskSummary>,
    runtime: Option<&RuntimeMonitorSnapshot>,
    view: RuntimeViewState,
) {
    let Some(task) = task else {
        draw_empty_panel_with_message(frame, area, " Task Details ", Some("Select a task."));
        return;
    };
    let lines = detail_lines(task, runtime);
    let histogram = match task.kind {
        RuntimeTaskKind::Asynchronous => Some((
            ("Poll duration", task.metrics.poll_samples.as_slice()),
            ("Scheduling delay", task.metrics.ready_samples.as_ref()),
        )),
        RuntimeTaskKind::Blocking | RuntimeTaskKind::Unknown => None,
    };
    let title = format!(" Task Details · #{} ", task.task_id);
    let focused = view.detail_mode == RuntimeDetailMode::Task;
    if let Some((polls, scheduling)) = histogram {
        let chart = draw_summary(frame, area, lines, title, focused, view.activity_scroll);
        draw_histogram_pair(frame, chart, polls, scheduling);
    } else {
        draw_details(frame, area, lines, title, focused, view.activity_scroll);
    }
}

pub(super) fn draw_resizable(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    areas: [Rect; 4],
    task: &RuntimeTaskSummary,
    runtime: Option<&RuntimeMonitorSnapshot>,
    view: RuntimeViewState,
) {
    let title = format!(" Task Details · #{} ", task.task_id);
    let focused = view.detail_mode == RuntimeDetailMode::Task;
    draw_details(frame, areas[0], detail_lines(task, runtime), title, focused, view.activity_scroll);
    if task.kind != RuntimeTaskKind::Asynchronous {
        draw_empty_panel_with_message(frame, areas[1], " Polls ", Some("This is not an asynchronous task."));
        draw_empty_panel_with_message(frame, areas[2], " Poll duration ", Some("This is not an asynchronous task."));
        draw_empty_panel_with_message(frame, areas[3], " Scheduling delay ", Some("This is not an asynchronous task."));
        return;
    }
    draw_polls(frame, mouse, areas[1], task, view, view.detail_mode == RuntimeDetailMode::Polls);
    draw_histogram(frame, areas[2], "Poll duration", &task.metrics.poll_samples);
    draw_histogram(frame, areas[3], "Scheduling delay", &task.metrics.ready_samples);
}

fn detail_lines(task: &RuntimeTaskSummary, runtime: Option<&RuntimeMonitorSnapshot>) -> Vec<Line<'static>> {
    let mut lines = vec![statistics_heading("Spawn hierarchy")];
    lines.extend(spawn_hierarchy(task, runtime).into_iter().map(Line::from));
    if task.kind == RuntimeTaskKind::Blocking {
        lines.push(statistics_heading("Lifecycle"));
        lines.push(Line::from(format!("Started: {}", if task.blocking.started { "yes" } else { "no" })));
        lines.push(statistics_heading("Timing"));
        lines.push(Line::from(format!("Queue wait: {}", duration(task.blocking.queue_duration_nanos))));
        lines.push(Line::from(format!(
            "Execution duration: {}",
            duration(task.blocking.execution_duration_nanos)
        )));
    }
    lines
}

fn draw_polls(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    area: Rect,
    task: &RuntimeTaskSummary,
    view: RuntimeViewState,
    focused: bool,
) {
    let polls = sorted_polls(task, view);
    if polls.is_empty() {
        draw_empty_panel_with_message(frame, area, " Polls ", Some("No completed polls."));
        return;
    }
    let selected_order = view.selected_poll(task.runtime_id, task.task_id);
    let selected = selected_order.and_then(|selected| polls.iter().position(|(order, _)| *order == selected));
    let visible = usize::from(area.height.saturating_sub(3));
    let first = selected.unwrap_or(0).saturating_sub(visible.saturating_sub(1));
    let visible_polls = polls.iter().skip(first).take(visible).collect::<Vec<_>>();
    let rows = visible_polls.iter().map(|(order, poll)| {
        Row::new([
            (*order + 1).to_string(),
            format_runtime_duration(poll.interval.end.saturating_sub(poll.interval.start)),
            duration(poll.scheduling_delay_nanos),
        ])
    });
    let widths = [Constraint::Length(7), Constraint::Length(10), Constraint::Min(14)];
    let sorts = [TaskPollSort::Order, TaskPollSort::Duration, TaskPollSort::SchedulingDelay];
    let targets = sorts.map(ListTarget::RuntimeTaskPollHeader);
    table_ui::register_headers(mouse, area, &widths, &targets);
    let table = Table::new(rows, widths)
        .header(
            Row::new(table_ui::sort_headers(
                vec!["Order", "Duration", "Scheduling delay"],
                &sorts,
                view.poll_sort,
                view.poll_descending,
            ))
            .add_modifier(Modifier::BOLD),
        )
        .column_spacing(1)
        .row_highlight_style(table_ui::selection_style(focused))
        .block(table_ui::focused_block(" Polls ", focused));
    let mut state = TableState::default().with_selected(selected.map(|selected| selected.saturating_sub(first)));
    frame.render_stateful_widget(table, area, &mut state);
    let inner = Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    );
    for (offset, (order, _)) in visible_polls.into_iter().enumerate() {
        mouse.register_item(
            Rect::new(
                inner.x,
                inner.y.saturating_add(1).saturating_add(u16::try_from(offset).unwrap_or(u16::MAX)),
                inner.width,
                1,
            ),
            ListTarget::RuntimeTaskPolls(task.runtime_id, task.task_id),
            *order,
        );
    }
}

fn sorted_polls(task: &RuntimeTaskSummary, view: RuntimeViewState) -> Vec<(usize, &super::super::runtime_timeline::PollExecution)> {
    super::super::app::sorted_task_poll_orders(task, view)
        .into_iter()
        .filter_map(|order| task.metrics.poll_executions.get(order).map(|poll| (order, poll)))
        .collect()
}

fn spawn_hierarchy(task: &RuntimeTaskSummary, runtime: Option<&RuntimeMonitorSnapshot>) -> Vec<String> {
    let mut visited = vec![task.task_id];
    let mut chain = vec![format!("#{}  {}  [selected]", task.task_id, spawn_location(task))];
    let mut parent = task.parent_id;
    while let Some(id) = parent {
        if visited.contains(&id) {
            chain.push(format!("#{id}  cycle detected"));
            break;
        }
        visited.push(id);
        let Some(ancestor) = runtime
            .into_iter()
            .flat_map(|runtime| &runtime.workers)
            .filter(|worker| worker.runtime_id == task.runtime_id)
            .flat_map(|worker| &worker.tasks)
            .find(|task| task.task_id == id)
        else {
            chain.push(format!("#{id}  task not retained"));
            break;
        };
        chain.push(format!("#{id}  {}", spawn_location(ancestor)));
        parent = ancestor.parent_id;
    }
    chain.reverse();
    chain
        .into_iter()
        .enumerate()
        .map(|(index, task)| format!("{index:>3}  {task}"))
        .collect()
}

fn spawn_location(task: &RuntimeTaskSummary) -> String {
    task.spawned_at.clone().unwrap_or_else(|| {
        task.spawn_stack.first().cloned().unwrap_or_else(|| {
            task.type_descriptor_id.map_or_else(
                || "location not retained".into(),
                |descriptor| format!("descriptor #{descriptor}; location not retained"),
            )
        })
    })
}

fn duration(value: Option<u64>) -> String {
    value.map_or_else(|| "-".into(), format_runtime_duration)
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::commands::monitor::runtime::RuntimeWorkerSummary;

    fn resizable_areas(area: Rect) -> [Rect; 4] {
        let [summary, metrics] = Layout::vertical([Constraint::Percentage(45), Constraint::Min(0)]).areas(area);
        let [polls, durations] = Layout::horizontal([Constraint::Length(42), Constraint::Min(0)]).areas(metrics);
        let [poll_duration, scheduling_delay] = Layout::horizontal([Constraint::Percentage(50), Constraint::Min(0)]).areas(durations);
        [summary, polls, poll_duration, scheduling_delay]
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one render fixture covers hierarchy, sortable polls, and duration charts"
    )]
    fn runtime_details_show_ancestors_across_workers_without_table_totals() {
        let task = RuntimeTaskSummary {
            runtime_id: 1,
            task_id: 3,
            parent_id: Some(2),
            kind: RuntimeTaskKind::Asynchronous,
            state: "Running".into(),
            origin_worker_id: Some(4),
            worker_ids: vec![4, 5],
            metrics: super::super::super::runtime_timeline::ExecutionMetrics {
                poll_count: 3,
                poll_samples: vec![8, 1, 4],
                poll_executions: vec![
                    super::super::super::runtime_timeline::PollExecution {
                        interval: super::super::super::runtime_timeline::Interval { start: 10, end: 18 },
                        scheduling_delay_nanos: Some(5),
                    },
                    super::super::super::runtime_timeline::PollExecution {
                        interval: super::super::super::runtime_timeline::Interval { start: 20, end: 21 },
                        scheduling_delay_nanos: Some(2),
                    },
                    super::super::super::runtime_timeline::PollExecution {
                        interval: super::super::super::runtime_timeline::Interval { start: 30, end: 34 },
                        scheduling_delay_nanos: None,
                    },
                ],
                ready_samples: vec![2, 4].into(),
                wake_samples: vec![0].into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let runtime = RuntimeMonitorSnapshot {
            workers: vec![
                RuntimeWorkerSummary {
                    runtime_id: 9,
                    tasks: vec![RuntimeTaskSummary {
                        task_id: 2,
                        spawned_at: Some("wrong.rs:1:1".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                RuntimeWorkerSummary {
                    runtime_id: 1,
                    worker_id: Some(7),
                    tasks: vec![RuntimeTaskSummary {
                        runtime_id: 1,
                        task_id: 2,
                        parent_id: Some(1),
                        spawned_at: Some("parent.rs:2:1".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                RuntimeWorkerSummary {
                    runtime_id: 1,
                    tasks: vec![RuntimeTaskSummary {
                        runtime_id: 1,
                        task_id: 1,
                        spawned_at: Some("root.rs:1:1".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(180, 30)).unwrap();
        let mouse = MouseRows::default();
        terminal
            .draw(|frame| {
                mouse.begin(frame.area());
                draw_resizable(
                    frame,
                    &mouse,
                    resizable_areas(frame.area()),
                    &task,
                    Some(&runtime),
                    RuntimeViewState::new(),
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
        let text = text.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
        assert_eq!(RuntimeViewState::new().selected_poll(1, 3), None);
        let duration_order = sorted_polls(
            &task,
            RuntimeViewState {
                poll_sort: TaskPollSort::Duration,
                ..RuntimeViewState::new()
            },
        )
        .into_iter()
        .map(|(order, _)| order)
        .collect::<Vec<_>>();
        assert_eq!(duration_order, [1, 2, 0]);
        let scheduling_order = sorted_polls(
            &task,
            RuntimeViewState {
                poll_sort: TaskPollSort::SchedulingDelay,
                ..RuntimeViewState::new()
            },
        )
        .into_iter()
        .map(|(order, _)| order)
        .collect::<Vec<_>>();
        assert_eq!(scheduling_order, [1, 0, 2]);
        assert!(text.contains("Spawn hierarchy"));
        assert!(text.contains("Order"));
        assert!(text.contains("Duration"));
        assert!(text.contains("Scheduling delay"));
        assert!(text.contains("1 8ns"));
        assert!(text.contains("0 #1 root.rs:1:1"), "{text}");
        assert!(text.contains("1 #2 parent.rs:2:1"), "{text}");
        assert!(text.contains("2 #3 location not retained [selected]"), "{text}");
        assert!(!text.contains("Execution workers:"));
        assert!(text.contains("Poll duration"));
        assert!(text.contains("n=3 · max 8ns"));
        assert!(text.contains("Scheduling delay"));
        assert!(text.contains("n=2 · max 4ns"));
        for duplicate in [
            "Timing",
            "Count:",
            "Total:",
            "Median:",
            "Wake latency",
            "Running",
            "Spawn origin:",
            "Poll samples:",
            "Poll timeline:",
            "wrong.rs",
        ] {
            assert!(!text.contains(duplicate), "{duplicate}");
        }

        terminal
            .draw(|frame| {
                mouse.begin(frame.area());
                draw_resizable(
                    frame,
                    &mouse,
                    resizable_areas(frame.area()),
                    &task,
                    Some(&runtime),
                    RuntimeViewState {
                        poll_sort: TaskPollSort::Duration,
                        ..RuntimeViewState::new()
                    },
                );
            })
            .unwrap();
        let area = terminal.backend().buffer().area;
        let first_poll = (0..area.height)
            .flat_map(|row| (0..area.width).map(move |column| (column, row)))
            .find_map(|(column, row)| {
                mouse
                    .at(area, column, row)
                    .filter(|(target, _)| *target == ListTarget::RuntimeTaskPolls(1, 3))
            })
            .unwrap();
        assert_eq!(first_poll.1, 1);
    }

    #[test]
    fn runtime_details_mark_missing_parents_and_stop_cycles() {
        let mut task = RuntimeTaskSummary {
            runtime_id: 1,
            task_id: 3,
            parent_id: Some(2),
            ..Default::default()
        };
        assert_eq!(
            spawn_hierarchy(&task, None),
            ["  0  #2  task not retained", "  1  #3  location not retained  [selected]"]
        );
        let runtime = RuntimeMonitorSnapshot {
            workers: vec![RuntimeWorkerSummary {
                runtime_id: 1,
                tasks: vec![RuntimeTaskSummary {
                    runtime_id: 1,
                    task_id: 2,
                    parent_id: Some(3),
                    type_descriptor_id: Some(12),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            spawn_hierarchy(&task, Some(&runtime)),
            [
                "  0  #3  cycle detected",
                "  1  #2  descriptor #12; location not retained",
                "  2  #3  location not retained  [selected]"
            ]
        );
        task.parent_id = None;
        assert_eq!(
            spawn_hierarchy(&task, Some(&runtime)),
            ["  0  #3  location not retained  [selected]"]
        );
    }

    #[test]
    fn blocking_task_details_use_aligned_values_without_a_histogram() {
        let task = RuntimeTaskSummary {
            runtime_id: 1,
            task_id: 4,
            kind: RuntimeTaskKind::Blocking,
            blocking: super::super::super::runtime::BlockingTaskSummary {
                started: true,
                queue_duration_nanos: Some(12),
                execution_duration_nanos: Some(34),
            },
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(80, 16)).unwrap();
        terminal
            .draw(|frame| draw(frame, frame.area(), Some(&task), None, RuntimeViewState::new()))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        let text = text.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
        assert!(text.contains("Spawn hierarchy"));
        assert!(text.contains("Lifecycle"));
        assert!(text.contains("Timing"));
        assert!(text.contains("Started: yes"), "{text}");
        assert!(text.contains("Queue wait: 12ns"), "{text}");
        assert!(text.contains("Execution duration: 34ns"), "{text}");
        assert!(!text.contains("histogram"), "{text}");
        assert_eq!(text.matches("Execution duration:").count(), 1);
        assert_eq!(terminal.backend().buffer()[(0, 15)].symbol(), "└");
    }
}
