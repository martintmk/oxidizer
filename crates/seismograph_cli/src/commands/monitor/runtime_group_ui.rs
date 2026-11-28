// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Accumulated task-type statistics and the tasks in the selected type.

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Bar, BarChart, BarGroup, Cell, Paragraph, Row, Table, TableState, Wrap};

use super::super::app::{
    GroupHistogram, RuntimeDetailMode, RuntimeSectionFocus, RuntimeTaskRowSort, RuntimeViewState, RuntimeWorkersFocus,
    selected_runtime_group, sort_runtime_tasks,
};
use super::super::data::{RuntimeMonitorSnapshot, RuntimeTaskSummary};
use super::super::formatting::{format_count, format_histogram_duration, format_runtime_duration};
use super::super::hotspot_analysis::{frequent_slow_poll_samples, long_blocking_execution, long_blocking_queue};
use super::super::mouse::{ListTarget, MouseRows};
use super::super::runtime::{AsynchronousTaskGroupSummary, BlockingTaskGroupSummary};
use super::super::runtime_timeline::{median_nanos, populated_histogram};
use super::super::table_ui;
use super::draw_empty_panel_with_message;

pub(super) fn draw_statistics(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    runtime: Option<&RuntimeMonitorSnapshot>,
    unavailable: Option<&str>,
    view: RuntimeViewState,
) {
    let Some(runtime) = runtime else {
        draw_empty_panel_with_message(frame, area, " Task Type Details ", unavailable);
        return;
    };
    let Some((runtime_id, worker_id, descriptor, _kind)) = selected_runtime_group(runtime, &view) else {
        draw_empty_panel_with_message(frame, area, " Task Type Details ", Some("Select a task type."));
        return;
    };
    match view.focus {
        RuntimeWorkersFocus::Asynchronous => {
            let group = runtime
                .asynchronous_groups
                .iter()
                .filter(|group| group.runtime_id == runtime_id)
                .filter(|group| group.worker_id == worker_id)
                .find(|group| group.type_descriptor_id == descriptor);
            draw_async_statistics(
                frame,
                area,
                group,
                view.detail_mode == RuntimeDetailMode::Group,
                view.activity_scroll,
            );
        }
        RuntimeWorkersFocus::Blocking => {
            let group = runtime
                .blocking_groups
                .iter()
                .filter(|group| group.runtime_id == runtime_id)
                .filter(|group| group.worker_id == worker_id)
                .find(|group| group.type_descriptor_id == descriptor);
            draw_blocking_statistics(
                frame,
                area,
                group,
                view.group_histogram,
                view.detail_mode == RuntimeDetailMode::Group,
                view.activity_scroll,
            );
        }
    }
}

pub(super) fn draw_task_list<'a>(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    area: Rect,
    runtime: Option<&'a RuntimeMonitorSnapshot>,
    unavailable: Option<&str>,
    view: RuntimeViewState,
) -> Option<&'a RuntimeTaskSummary> {
    let Some(runtime) = runtime else {
        draw_empty_panel_with_message(frame, area, " Tasks ", unavailable);
        return None;
    };
    let Some((runtime_id, worker_id, descriptor, kind)) = selected_runtime_group(runtime, &view) else {
        draw_empty_panel_with_message(frame, area, " Tasks ", Some("Select a task type."));
        return None;
    };
    let tasks = runtime.tasks_for_group(runtime_id, worker_id, descriptor, kind);
    draw_tasks(frame, mouse, area, &tasks, view)
}

fn duration(value: Option<u64>) -> String {
    value.map_or_else(|| "-".into(), format_runtime_duration)
}

fn task_spawn(task: &RuntimeTaskSummary) -> String {
    match (task.origin_worker_id, task.worker_ids.first().copied()) {
        (None, _) => "external".into(),
        (Some(origin), Some(worker)) if origin == worker => format!("local #{worker}"),
        (Some(origin), Some(worker)) => format!("#{origin} -> #{worker}"),
        (Some(origin), None) => format!("#{origin} -> ?"),
    }
}

fn draw_async_statistics(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    group: Option<&AsynchronousTaskGroupSummary>,
    focused: bool,
    scroll: usize,
) {
    let Some(group) = group else {
        draw_empty_panel_with_message(frame, area, " Async Task Type ", Some("Task type data is unavailable."));
        return;
    };
    let lines = vec![
        statistics_heading("Identity"),
        Line::from(format!(
            "Spawned at: {}",
            task_type_location(group.spawned_at.as_deref(), group.type_descriptor_id)
        )),
        statistics_heading("Outcomes"),
        Line::from(format!("Succeeded: {}", format_count(group.outcomes.completed))),
        Line::from(format!("Canceled: {}", format_count(group.outcomes.canceled))),
        Line::from(format!("Panicked: {}", format_count(group.outcomes.panicked))),
    ];
    let chart = draw_summary(frame, area, lines, " Task Type Details ".into(), focused, scroll);
    draw_histogram_pair(
        frame,
        chart,
        ("Poll duration", &group.poll_samples),
        ("Scheduling delay", &group.ready_samples),
    );
}

fn draw_blocking_statistics(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    group: Option<&BlockingTaskGroupSummary>,
    histogram_kind: GroupHistogram,
    focused: bool,
    scroll: usize,
) {
    let Some(group) = group else {
        draw_empty_panel_with_message(frame, area, " Blocking Task Type ", Some("Task type data is unavailable."));
        return;
    };
    let (label, samples) = match histogram_kind {
        GroupHistogram::Primary => ("Execution duration", group.execution_samples.as_slice()),
        GroupHistogram::Secondary => ("Queue wait", group.queue_samples.as_slice()),
    };
    let mut lines = vec![
        statistics_heading("Identity"),
        Line::from(format!(
            "Spawned at: {}",
            task_type_location(group.spawned_at.as_deref(), group.type_descriptor_id)
        )),
        statistics_heading("Outcomes"),
        Line::from(format!("Succeeded: {}", format_count(group.outcomes.completed))),
        Line::from(format!("Canceled: {}", format_count(group.outcomes.canceled))),
        Line::from(format!("Panicked: {}", format_count(group.outcomes.panicked))),
        statistics_heading("Execution"),
        Line::from(format!("Workers: {}", format_count(group.outcomes.workers))),
        Line::from(format!("Runs: {}", format_count(group.executions))),
    ];
    lines.extend(sample_details("Execution", &group.execution_samples));
    lines.extend(sample_details("Queue wait", &group.queue_samples));
    let chart = draw_summary(frame, area, lines, " Task Type Details ".into(), focused, scroll);
    draw_histogram(frame, chart, label, samples);
}

fn task_type_location(location: Option<&str>, descriptor: Option<u64>) -> String {
    location.map_or_else(
        || descriptor.map_or_else(|| "not retained".into(), |descriptor| format!("descriptor #{descriptor}")),
        str::to_owned,
    )
}

pub(super) fn statistics_heading(label: impl Into<String>) -> Line<'static> {
    Line::from(ratatui::text::Span::styled(
        label.into(),
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    ))
}

pub(super) fn sample_details(label: &str, samples: &[u64]) -> Vec<Line<'static>> {
    vec![
        Line::from(format!(
            "{label} samples: {}",
            format_count(u64::try_from(samples.len()).unwrap_or(u64::MAX))
        )),
        Line::from(format!("{label} total: {}", duration(sample_total(samples)))),
        Line::from(format!("{label} median: {}", duration(sample_median(samples)))),
    ]
}

fn sample_total(samples: &[u64]) -> Option<u64> {
    (!samples.is_empty()).then(|| samples.iter().copied().fold(0_u64, u64::saturating_add))
}

fn sample_median(samples: &[u64]) -> Option<u64> {
    median_nanos(&mut samples.to_vec())
}

pub(super) fn aligned_details(lines: Vec<Line<'static>>) -> Vec<Line<'static>> {
    let caption_width = lines
        .iter()
        .filter_map(|line| {
            let text = line.to_string();
            (!line.spans.iter().any(|span| span.style.add_modifier.contains(Modifier::BOLD))
                && !text.chars().next().is_some_and(char::is_whitespace))
            .then(|| text.find(':'))
            .flatten()
        })
        .max()
        .unwrap_or(0);
    lines
        .into_iter()
        .map(|line| {
            let text = line.to_string();
            if line.spans.iter().any(|span| span.style.add_modifier.contains(Modifier::BOLD))
                || text.chars().next().is_some_and(char::is_whitespace)
            {
                return line;
            }
            match text.split_once(':') {
                Some((caption, value)) => Line::from(format!("{caption:caption_width$}:{value}")),
                None => line,
            }
        })
        .collect()
}

pub(super) fn draw_summary(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    lines: Vec<Line<'static>>,
    title: String,
    focused: bool,
    scroll: usize,
) -> Rect {
    draw_detail_lines(frame, area, lines, title, focused, scroll, (area.height / 3).min(5))
}

pub(super) fn draw_details(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    lines: Vec<Line<'static>>,
    title: String,
    focused: bool,
    scroll: usize,
) {
    draw_detail_lines(frame, area, lines, title, focused, scroll, 0);
}

fn draw_detail_lines(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    lines: Vec<Line<'static>>,
    title: String,
    focused: bool,
    scroll: usize,
    chart_height: u16,
) -> Rect {
    let paragraph = Paragraph::new(aligned_details(lines)).wrap(Wrap { trim: false });
    let line_count = paragraph.line_count(area.width.saturating_sub(2));
    let height = u16::try_from(line_count).unwrap_or(u16::MAX).saturating_add(2);
    let summary_height = if chart_height == 0 {
        area.height
    } else {
        height.min(area.height.saturating_sub(chart_height))
    };
    let [summary, chart] = Layout::vertical([Constraint::Length(summary_height), Constraint::Min(0)]).areas(area);
    let visible = usize::from(summary.height.saturating_sub(2));
    let scroll = scroll.min(line_count.saturating_sub(visible));
    let mut block = table_ui::focused_block(title, focused);
    if line_count > visible {
        block = block.title_bottom(format!(" line {}/{} ", scroll + 1, line_count));
    }
    frame.render_widget(
        paragraph.scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0)).block(block),
        summary,
    );
    chart
}

pub(super) fn draw_histogram_pair(frame: &mut ratatui::Frame<'_>, area: Rect, left: (&str, &[u64]), right: (&str, &[u64])) {
    let [left_area, right_area] = Layout::horizontal([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)]).areas(area);
    draw_histogram(frame, left_area, left.0, left.1);
    draw_histogram(frame, right_area, right.0, right.1);
}

pub(super) fn draw_histogram(frame: &mut ratatui::Frame<'_>, area: Rect, label: &str, samples: &[u64]) {
    if area.is_empty() {
        return;
    }
    let metadata = format!(
        " n={} · max {} ",
        format_count(u64::try_from(samples.len()).unwrap_or(u64::MAX)),
        duration(samples.iter().copied().max())
    );
    if samples.is_empty() {
        frame.render_widget(
            Paragraph::new("No completed duration samples.")
                .style(Style::default().fg(Color::DarkGray))
                .block(table_ui::focused_block(format!(" {label} "), false).title_bottom(metadata)),
            area,
        );
        return;
    }
    let count = usize::from(area.width.saturating_sub(2) / 9).clamp(1, 12);
    let bins = populated_histogram(samples, count);
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
            .block(table_ui::focused_block(format!(" {label} "), false).title_bottom(metadata))
            .data(BarGroup::default().bars(&bars))
            .bar_width(5)
            .bar_gap(1)
            .max(bins.iter().map(|bin| bin.count).max().unwrap_or(1)),
        area,
    );
}

fn draw_tasks<'a>(
    frame: &mut ratatui::Frame<'_>,
    mouse: &MouseRows,
    area: Rect,
    tasks: &[&'a RuntimeTaskSummary],
    view: RuntimeViewState,
) -> Option<&'a RuntimeTaskSummary> {
    if area.is_empty() {
        return None;
    }
    let mut tasks = tasks.to_vec();
    sort_runtime_tasks(&mut tasks, view);
    let selected = view.task_selected.min(tasks.len().saturating_sub(1));
    let visible = usize::from(area.height.saturating_sub(3));
    let first = selected.saturating_sub(visible.saturating_sub(1));
    let wide = area.width >= 82;
    let rows = tasks.iter().skip(first).take(visible).map(|task| task_row(task, view.focus));
    let (headers, widths, sorts) = match (view.focus, wide) {
        (RuntimeWorkersFocus::Asynchronous, true) => (
            vec!["Task", "State", "Spawn", "Ops", "Polls", "Max poll"],
            vec![8, 11, 16, 7, 8, 12].into_iter().map(Constraint::Length).collect::<Vec<_>>(),
            vec![
                RuntimeTaskRowSort::Task,
                RuntimeTaskRowSort::State,
                RuntimeTaskRowSort::Spawn,
                RuntimeTaskRowSort::Operations,
                RuntimeTaskRowSort::Polls,
                RuntimeTaskRowSort::MaximumPoll,
            ],
        ),
        (RuntimeWorkersFocus::Asynchronous, false) => (
            vec!["Task", "State", "Spawn", "Ops", "Polls", "Max poll"],
            vec![
                Constraint::Length(8),
                Constraint::Length(9),
                Constraint::Min(10),
                Constraint::Length(6),
                Constraint::Length(7),
                Constraint::Length(11),
            ],
            vec![
                RuntimeTaskRowSort::Task,
                RuntimeTaskRowSort::State,
                RuntimeTaskRowSort::Spawn,
                RuntimeTaskRowSort::Operations,
                RuntimeTaskRowSort::Polls,
                RuntimeTaskRowSort::MaximumPoll,
            ],
        ),
        (RuntimeWorkersFocus::Blocking, true) => (
            vec!["Task", "State", "Spawn", "Ops", "Queue", "Execution"],
            vec![8, 11, 16, 7, 12, 12].into_iter().map(Constraint::Length).collect::<Vec<_>>(),
            vec![
                RuntimeTaskRowSort::Task,
                RuntimeTaskRowSort::State,
                RuntimeTaskRowSort::Spawn,
                RuntimeTaskRowSort::Operations,
                RuntimeTaskRowSort::Queue,
                RuntimeTaskRowSort::Execution,
            ],
        ),
        (RuntimeWorkersFocus::Blocking, false) => (
            vec!["Task", "State", "Spawn", "Ops", "Queue", "Execution"],
            vec![
                Constraint::Length(8),
                Constraint::Length(9),
                Constraint::Min(10),
                Constraint::Length(6),
                Constraint::Length(11),
                Constraint::Length(11),
            ],
            vec![
                RuntimeTaskRowSort::Task,
                RuntimeTaskRowSort::State,
                RuntimeTaskRowSort::Spawn,
                RuntimeTaskRowSort::Operations,
                RuntimeTaskRowSort::Queue,
                RuntimeTaskRowSort::Execution,
            ],
        ),
    };
    let targets = sorts.iter().copied().map(ListTarget::RuntimeTaskHeader).collect::<Vec<_>>();
    table_ui::register_headers(mouse, area, &widths, &targets);
    let table = Table::new(rows, widths)
        .header(
            Row::new(table_ui::sort_headers(headers, &sorts, view.task_sort, view.task_sort_descending))
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .column_spacing(1)
        .row_highlight_style(table_ui::selection_style(view.section == RuntimeSectionFocus::GroupTasks))
        .block(table_ui::focused_block(
            format!(" Tasks · {} ", tasks.len()),
            view.section == RuntimeSectionFocus::GroupTasks,
        ));
    let mut state = TableState::default().with_selected((!tasks.is_empty()).then_some(selected.saturating_sub(first)));
    frame.render_stateful_widget(table, area, &mut state);
    mouse.register(
        area,
        1,
        first.saturating_add(state.offset()),
        tasks.len(),
        ListTarget::RuntimeGroupTasks,
    );
    tasks.get(selected).copied()
}

fn task_row(task: &RuntimeTaskSummary, focus: RuntimeWorkersFocus) -> Row<'static> {
    let spawn = task_spawn(task);
    let mut cells = vec![format!("#{}", task.task_id), task.state.clone(), spawn];
    match focus {
        RuntimeWorkersFocus::Asynchronous => cells.extend([
            format_count(task.operations),
            format_count(task.metrics.poll_count),
            duration(task.metrics.max_poll_nanos),
        ]),
        RuntimeWorkersFocus::Blocking => cells.extend([
            format_count(task.operations),
            duration(task.blocking.queue_duration_nanos),
            duration(task.blocking.execution_duration_nanos),
        ]),
    }
    let hotspots = match focus {
        RuntimeWorkersFocus::Asynchronous => [(5, frequent_slow_poll_samples(&task.metrics.poll_samples)), (usize::MAX, false)],
        RuntimeWorkersFocus::Blocking => [
            (
                4,
                task.blocking
                    .queue_duration_nanos
                    .is_some_and(|duration| long_blocking_queue(duration, 3)),
            ),
            (5, task.blocking.execution_duration_nanos.is_some_and(long_blocking_execution)),
        ],
    };
    Row::new(cells.into_iter().enumerate().map(|(index, value)| {
        let line = Line::from(value);
        let cell = Cell::from(if index >= 3 { line.right_aligned() } else { line });
        if hotspots.iter().any(|(hotspot_index, active)| *active && index == *hotspot_index) {
            cell.style(Style::default().fg(Color::Yellow))
        } else {
            cell
        }
    }))
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    #[test]
    fn runtime_details_replace_group_table_totals_with_completed_samples() {
        let group = AsynchronousTaskGroupSummary {
            spawned_at: Some("src/tasks.rs:42:5".into()),
            type_descriptor_id: Some(8),
            outcomes: super::super::super::runtime::TaskOutcomeSummary {
                tasks: 10,
                completed: 7,
                canceled: 2,
                panicked: 1,
                workers: 3,
                ..Default::default()
            },
            polls: 9,
            poll_samples: vec![8, 1, 4],
            ready_samples: vec![0],
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal
            .draw(|frame| draw_async_statistics(frame, frame.area(), Some(&group), true, 0))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        let text = text.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
        for detail in [
            "Identity",
            "Spawned at: src/tasks.rs:42:5",
            "Outcomes",
            "Succeeded: 7",
            "Canceled: 2",
            "Panicked: 1",
            "Poll duration",
            "n=3 · max 8ns",
            "Scheduling delay",
            "n=1 · max 0ns",
        ] {
            assert!(text.contains(detail), "{detail}: {text}");
        }
        for duplicate in [
            "Execution",
            "Workers:",
            "Count:",
            "Total:",
            "Median:",
            "spawned ·",
            "live ·",
            "workers ·",
            "avg",
            "Poll:",
        ] {
            assert!(!text.contains(duplicate), "{duplicate}");
        }
    }

    #[test]
    fn runtime_details_wrap_locations_and_scroll_to_missing_samples() {
        let group = BlockingTaskGroupSummary {
            spawned_at: Some("src/a/long/spawn/location/that/needs/several/lines/tasks.rs:42:5".into()),
            execution_samples: vec![0],
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(40, 12)).unwrap();
        terminal
            .draw(|frame| draw_blocking_statistics(frame, frame.area(), Some(&group), GroupHistogram::Primary, true, 0))
            .unwrap();
        let initial = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        let normalized = initial
            .chars()
            .filter(|character| character.is_ascii() && !character.is_ascii_whitespace())
            .collect::<String>();
        assert!(
            normalized.contains("Spawnedat:src/a/long/spawn/location/that/needs/several/lines/tasks.rs:42:5"),
            "{initial}"
        );
        assert!(!initial.contains("Queue wait median:"));
        terminal
            .draw(|frame| {
                draw_blocking_statistics(frame, frame.area(), Some(&group), GroupHistogram::Primary, true, usize::MAX);
            })
            .unwrap();
        let scrolled = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        let scrolled = scrolled.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
        assert!(scrolled.contains("Execution total: 0ns"));
        assert!(scrolled.contains("Queue wait samples: 0"));
        assert!(scrolled.contains("Queue wait total: -"));
        assert!(scrolled.contains("Queue wait median: -"));
    }

    #[test]
    fn runtime_detail_captions_share_a_colon_column() {
        let mut terminal = Terminal::new(TestBackend::new(50, 10)).unwrap();
        terminal
            .draw(|frame| {
                draw_summary(
                    frame,
                    frame.area(),
                    vec![Line::from("A: first"), Line::from("Long caption: second")],
                    " Details ".into(),
                    true,
                    0,
                );
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let first = (0..50).find(|&x| buffer[(x, 1)].symbol() == ":").unwrap();
        let second = (0..50).find(|&x| buffer[(x, 2)].symbol() == ":").unwrap();
        assert_eq!(first, second);
        assert_eq!(buffer[(first + 2, 1)].symbol(), "f");
        assert_eq!(buffer[(second + 2, 2)].symbol(), "s");
    }

    #[test]
    fn task_type_location_uses_descriptor_only_as_a_fallback() {
        assert_eq!(task_type_location(Some("src/task.rs:1:2"), Some(8)), "src/task.rs:1:2");
        assert_eq!(task_type_location(None, Some(8)), "descriptor #8");
        assert_eq!(task_type_location(None, None), "not retained");
    }
}
