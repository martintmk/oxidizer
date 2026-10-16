// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use data_privacy::classified;
use observed::{Sink, emit, event};

use crate::TaskPlacement;

static SYSTEM_METADATA: data_privacy::DataClass = data_privacy::DataClass::new("arty", "SystemMetadata");

#[classified(SYSTEM_METADATA)]
#[derive(Clone, Copy)]
struct PlacementLabel(&'static str);

pub(crate) fn report_created(sink: &Sink, placement: TaskPlacement) {
    let placement = match placement {
        TaskPlacement::Any => "any",
        TaskPlacement::SameThread => "same_thread",
    };
    emit!(
        sink,
        TaskCreated {
            placement: PlacementLabel(placement)
        }
    );
}

pub(crate) fn report_succeeded(sink: &Sink, polls: u64) {
    emit!(sink, TaskSucceeded { polls });
}

pub(crate) fn report_panicked(sink: &Sink, polls: u64) {
    emit!(sink, TaskPanicked { polls });
}

pub(crate) fn report_waker_detached(sink: &Sink) {
    emit!(sink, WakerDetached);
}

#[event("arty.rt.task.spawned")]
#[counter(name = "arty.rt.task.spawned")]
struct TaskCreated {
    #[dimension(metric = "placement")]
    placement: PlacementLabel,
}

#[event("arty.rt.task.succeeded")]
#[counter(name = "arty.rt.task.succeeded")]
#[histogram(polls, name = "arty.rt.task.succeeded.polls", unit = "{poll}")]
struct TaskSucceeded {
    #[unredacted]
    polls: u64,
}

#[event("arty.rt.task.panicked")]
#[error("task panicked")]
#[counter(name = "arty.rt.task.panicked")]
#[histogram(polls, name = "arty.rt.task.panicked.polls", unit = "{poll}")]
struct TaskPanicked {
    #[unredacted]
    polls: u64,
}

/// Counts tasks that create independently owned wake state.
///
/// This records the first owned clone of a task's borrowed polling waker. It does not imply that
/// the owned waker crossed a thread or outlived the poll.
#[event("arty.rt.task.waker.detached")]
#[counter(name = "arty.rt.task.waker.detached")]
struct WakerDetached;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use observed_testing::{TEST_ID, test_emitter};

    use super::*;

    #[test]
    fn lifecycle_events_include_placement_and_poll_count() {
        let (sink, processor) = test_emitter(TEST_ID);

        report_created(&sink, TaskPlacement::SameThread);
        report_succeeded(&sink, 3);
        report_waker_detached(&sink);

        let events = processor.events();
        assert_eq!(events[0].name(), "arty.rt.task.spawned");
        assert_eq!(events[0].dimensions()[0], ("placement".to_owned(), "same_thread".into()));
        assert_eq!(events[1].name(), "arty.rt.task.succeeded");
        assert_eq!(events[1].field_metrics()[0].field_key(), "polls");
        assert_eq!(events[1].field_metrics()[0].metric().unit(), "{poll}");
        assert!((events[1].field_metrics()[0].value() - 3.0).abs() < f64::EPSILON);
        assert_eq!(events[2].name(), "arty.rt.task.waker.detached");
        assert!(events[2].dimensions().is_empty());
    }
}
