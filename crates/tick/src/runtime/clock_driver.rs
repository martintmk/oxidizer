// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::time::Instant;

use crate::runtime::ClockGone;
use crate::state::ClockState;

/// Drives timer advancement for the clock.
///
/// The `ClockDriver` is responsible for advancing and firing timers associated with
/// the clock. The runtime must call [`ClockDriver::advance_timers`] periodically to
/// ensure timers fire at the correct time.
///
/// In thread-isolated runtime architectures using one coordinate owner, each
/// worker owns its own `ClockDriver` that advances only the timers registered
/// on that thread's [`Clock`][crate::Clock]. See the [`runtime`][crate::runtime]
/// module for the setup pattern.
#[derive(Debug)]
pub struct ClockDriver {
    pub(crate) state: ClockState,
}

impl ClockDriver {
    pub(super) const fn new(state: ClockState) -> Self {
        Self { state }
    }

    /// Advances and fires all timers scheduled to execute by the given time.
    ///
    /// This method processes all timers scheduled to fire at or before the
    /// specified `now` time, waking their associated tasks.
    ///
    /// Returns the next system-clock deadline suitable for parking. Controlled clocks return
    /// `None` because their time advances externally rather than with [`Instant`].
    ///
    /// # Errors
    ///
    /// Returns `Err(ClockGone)` if the all clocks are gone, all timers are fired and
    /// advancing the clock is no longer necessary.
    #[cfg_attr(test, mutants::skip)] // Causes test timeout.
    #[expect(clippy::needless_pass_by_ref_mut, reason = "the mut forces exclusive ownership of the driver")]
    pub fn advance_timers(&mut self, now: Instant) -> Result<Option<Instant>, ClockGone> {
        let next_timer = match &self.state {
            ClockState::System(timers) => timers.try_advance_timers(now),
            #[cfg(any(feature = "test-util", test))]
            ClockState::ClockControl(_) => None,
        };

        match next_timer {
            Some(next) => Ok(Some(next)),
            // Check if this is the last reference to the clock state
            None if self.state.is_unique() => Err(ClockGone::new()),
            None => Ok(None),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Waker};
    use std::time::Duration;

    use futures::FutureExt;

    use super::*;
    use crate::clock_control::ClockControl;
    use crate::runtime::InactiveClock;
    use crate::state::SynchronizedTimers;

    struct CountingWake(AtomicUsize);

    impl std::task::Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn assert_types() {
        static_assertions::assert_impl_all!(ClockDriver: Send, Sync);
        static_assertions::assert_not_impl_all!(ClockDriver: Clone);
    }

    #[test]
    fn advance_timers_ok() {
        let timers = SynchronizedTimers::new_isolated();
        let when = Instant::now();
        timers.with_timers::<_, ()>(|timers| {
            timers.register(when, Waker::noop().clone());
        });

        let clock_state = ClockState::System(timers);
        let mut driver = ClockDriver::new(clock_state.clone());

        _ = driver.advance_timers(Instant::now() - Duration::from_secs(1));
        assert_eq!(clock_state.timers_len(), 1);

        _ = driver.advance_timers(Instant::now() + Duration::from_secs(1));
        assert_eq!(clock_state.timers_len(), 0);
    }

    #[test]
    fn clock_gone_error_reported() {
        let (clock, mut driver) = InactiveClock::default().activate(Waker::noop().clone());

        driver.advance_timers(Instant::now()).unwrap();
        drop(clock);
        let error = driver.advance_timers(Instant::now()).unwrap_err();

        assert_eq!(error.to_string(), "all clock owners have been dropped");
    }

    #[test]
    fn clock_gone_but_timers_left_not_dropped() {
        let now = Instant::now();
        let (clock, mut driver) = InactiveClock::default().activate(Waker::noop().clone());
        driver.advance_timers(now).unwrap();
        let mut future = Box::pin(clock.delay(Duration::from_secs(1)));
        let mut context = Context::from_waker(Waker::noop());
        _ = future.poll_unpin(&mut context);

        drop(clock);

        // still timers left
        driver.advance_timers(now).unwrap();

        // advance pending timers
        driver.advance_timers(now + Duration::from_secs(123)).unwrap();
        _ = future.poll_unpin(&mut context);
        drop(future);

        // no more timers left
        driver.advance_timers(now + Duration::from_secs(123)).unwrap_err();
    }

    #[test]
    fn advance_timers_with_clock_control_has_no_system_deadline() {
        let control = ClockControl::new();
        let clock_state = ClockState::ClockControl(control.clone());
        let when = control.instant() + Duration::from_secs(1);

        control.register_timer(when, Waker::noop().clone());

        let mut driver = ClockDriver::new(clock_state);

        // Calling advance_timers should not advance timers when using ClockControl
        let next = driver.advance_timers(control.instant() + Duration::from_secs(2)).unwrap();

        // Verify timers are not advanced (still registered)
        assert_eq!(control.timers_len(), 1);
        // Verify next timer time is returned
        assert_eq!(next, None);
    }

    #[test]
    fn earlier_system_timer_wakes_driver() {
        let wake_count = Arc::new(CountingWake(AtomicUsize::new(0)));
        let driver_waker = Waker::from(Arc::clone(&wake_count));
        let (clock, _driver) = InactiveClock::default().activate(driver_waker);
        let now = Instant::now();

        clock.register_timer(now + Duration::from_secs(2), Waker::noop().clone());
        clock.register_timer(now + Duration::from_secs(3), Waker::noop().clone());
        clock.register_timer(now + Duration::from_secs(1), Waker::noop().clone());

        assert_eq!(wake_count.0.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn parking_deadline_uses_only_system_time() {
        let now = Instant::now();
        let (clock, mut system_driver) = InactiveClock::default().activate(Waker::noop().clone());
        clock.register_timer(now + Duration::from_secs(1), Waker::noop().clone());
        assert_eq!(system_driver.advance_timers(now).unwrap(), Some(now + Duration::from_secs(1)));

        let control = ClockControl::new();
        let controlled_clock = control.to_clock();
        controlled_clock.register_timer(control.instant() + Duration::from_secs(1), Waker::noop().clone());
        let mut controlled_driver = ClockDriver::new(ClockState::ClockControl(control));
        assert_eq!(controlled_driver.advance_timers(now).unwrap(), None);
    }
}
