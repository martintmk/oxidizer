// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Rich statistics for one task selected from a task-type group.

use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Bar, BarChart, BarGroup, Paragraph};

use super::super::app::{RuntimeDetailMode, RuntimeViewState, TaskHistogram};
use super::super::data::{RuntimeMonitorSnapshot, RuntimeTaskSummary};
use super::super::formatting::{format_histogram_duration, format_runtime_duration};
use super::super::runtime::RuntimeTaskKind;
use super::super::runtime_timeline::histogram;
use super::super::table_ui;
use super::draw_empty_panel_with_message;
use super::runtime_group_ui::{draw_details, draw_summary, sample_details};

pub(super) fn draw(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    task: Option<&RuntimeTaskSummary>,
    runtime: Option<&RuntimeMonitorSnapshot>,
    view: RuntimeViewState,
) {
    let Some(task) = task else {
        draw_empty_panel_with_message(frame, area, " Task Statistics ", Some("Select a task."));
        return;
    };
    let mut lines = vec![Line::from("Spawn hierarchy:")];
    lines.extend(spawn_hierarchy(task, runtime).into_iter().map(Line::from));
    lines.extend([
        Line::from(format!(
            "Descriptor: {}",
            task.type_descriptor_id
                .map_or_else(|| "not retained".into(), |descriptor| format!("#{descriptor}")),
        )),
        Line::from(format!(
            "Execution workers: {}",
            if task.worker_ids.is_empty() {
                "-".into()
            } else {
                task.worker_ids
                    .iter()
                    .map(|worker| format!("w{worker}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        )),
    ]);
    let histogram = match task.kind {
        RuntimeTaskKind::Asynchronous => {
            lines.push(Line::from(format!("Scheduling delay: {}", duration(task.scheduling_delay_nanos))));
            lines.extend(sample_details("Poll", &task.metrics.poll_samples).into_iter().skip(1));
            lines.extend(sample_details("Ready wait", &task.metrics.ready_samples));
            lines.push(Line::from(format!(
                "Ready wait max: {}",
                duration(task.metrics.ready_samples.iter().copied().max())
            )));
            if !task.metrics.wake_samples.is_empty() {
                lines.extend(sample_details("Wake latency", &task.metrics.wake_samples));
                lines.push(Line::from(format!(
                    "Wake latency max: {}",
                    duration(task.metrics.wake_samples.iter().copied().max())
                )));
            }
            Some(match view.task_histogram {
                TaskHistogram::Poll => ("Poll duration", task.metrics.poll_samples.as_slice()),
                TaskHistogram::Ready => ("Ready wait", task.metrics.ready_samples.as_ref()),
            })
        }
        RuntimeTaskKind::Blocking => {
            lines.push(Line::from(format!("Started: {}", if task.blocking.started { "yes" } else { "no" })));
            lines.push(Line::from(format!("Queue wait: {}", duration(task.blocking.queue_duration_nanos))));
            lines.push(Line::from(format!(
                "Execution duration: {}",
                duration(task.blocking.execution_duration_nanos)
            )));
            None
        }
        RuntimeTaskKind::Unknown => None,
    };
    let title = format!(" Task Statistics · #{} ", task.task_id);
    let focused = view.detail_mode == RuntimeDetailMode::Task;
    if let Some((label, samples)) = histogram {
        let chart = draw_summary(frame, area, lines, title, focused, view.activity_scroll);
        draw_histogram(frame, chart, label, samples);
    } else {
        draw_details(frame, area, lines, title, focused, view.activity_scroll);
    }
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

fn draw_histogram(frame: &mut ratatui::Frame<'_>, area: Rect, label: &str, samples: &[u64]) {
    if samples.is_empty() {
        frame.render_widget(
            Paragraph::new("No completed duration samples.")
                .style(Style::default().fg(Color::DarkGray))
                .block(table_ui::focused_block(format!(" {label} "), false)),
            area,
        );
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
            .block(table_ui::focused_block(format!(" {label} "), false))
            .data(BarGroup::default().bars(&bars))
            .bar_width(5)
            .bar_gap(1)
            .max(bins.iter().map(|bin| bin.count).max().unwrap_or(1)),
        area,
    );
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::commands::monitor::runtime::RuntimeWorkerSummary;

    #[test]
    fn runtime_details_show_ancestors_across_workers_without_table_totals() {
        let task = RuntimeTaskSummary {
            runtime_id: 1,
            task_id: 3,
            parent_id: Some(2),
            kind: RuntimeTaskKind::Asynchronous,
            state: "Running".into(),
            origin_worker_id: Some(4),
            scheduling_delay_nanos: Some(11),
            worker_ids: vec![4, 5],
            metrics: super::super::super::runtime_timeline::ExecutionMetrics {
                poll_count: 3,
                poll_samples: vec![8, 1, 4],
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
        terminal
            .draw(|frame| draw(frame, frame.area(), Some(&task), Some(&runtime), RuntimeViewState::new()))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        let text = text.split_whitespace().collect::<Vec<_>>().join(" ").replace(" : ", ": ");
        assert!(text.contains("Spawn hierarchy:"));
        assert!(text.contains("0 #1 root.rs:1:1"), "{text}");
        assert!(text.contains("1 #2 parent.rs:2:1"), "{text}");
        assert!(text.contains("2 #3 location not retained [selected]"), "{text}");
        assert!(text.contains("Execution workers: w4, w5"));
        assert!(text.contains("Scheduling delay: 11ns"));
        assert!(text.contains("Poll total: 13ns"));
        assert!(text.contains("Poll median: 4ns"));
        assert!(text.contains("Ready wait samples: 2"));
        assert!(text.contains("Ready wait max: 4ns"));
        assert!(text.contains("Wake latency median: 0ns"));
        for duplicate in ["Running", "Spawn origin:", "Poll samples:", "Poll timeline:", "wrong.rs"] {
            assert!(!text.contains(duplicate), "{duplicate}");
        }
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
        assert!(text.contains("Started: yes"), "{text}");
        assert!(text.contains("Queue wait: 12ns"), "{text}");
        assert!(text.contains("Execution duration: 34ns"), "{text}");
        assert!(!text.contains("histogram"), "{text}");
        assert_eq!(text.matches("Execution duration:").count(), 1);
        assert_eq!(terminal.backend().buffer()[(0, 15)].symbol(), "└");
    }
}
