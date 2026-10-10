// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Timing for one driver invocation.
pub struct Cycle {
    started_at: Instant,
    max_wait: Duration,
    _not_send: PhantomData<Rc<()>>,
}

impl Cycle {
    /// Creates the inputs for one driver invocation.
    ///
    /// The runtime must use [`Duration::ZERO`] for `max_wait` when no waiting is allowed,
    /// including during initialization.
    #[must_use]
    pub const fn new(started_at: Instant, max_wait: Duration) -> Self {
        Self {
            started_at,
            max_wait,
            _not_send: PhantomData,
        }
    }

    /// Returns the time snapshot shared by all drivers in this runtime cycle.
    #[must_use]
    pub const fn started_at(&self) -> Instant {
        self.started_at
    }

    /// Returns the maximum wait duration.
    ///
    /// A [primary driver](crate::DriverRole::Primary) may wait on its worker for up to this
    /// duration. A [secondary driver](crate::DriverRole::Secondary) must not wait on its worker.
    /// [`Duration::ZERO`] means no waiting.
    #[must_use]
    pub const fn max_wait(&self) -> Duration {
        self.max_wait
    }
}

impl fmt::Debug for Cycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cycle")
            .field("started_at", &self.started_at)
            .field("max_wait", &self.max_wait)
            .finish_non_exhaustive()
    }
}
