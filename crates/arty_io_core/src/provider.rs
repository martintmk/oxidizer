// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use thread_aware_core::ThreadAware;

use crate::{Driver, DriverError, DriverOptions, IoContext};

/// A factory for per-worker driver and context pairs.
///
/// The runtime clones and relocates the provider to each worker, then consumes the clone to
/// create its pair. Shared driver state remains private to the provider.
pub trait DriverProvider: Clone + ThreadAware + Sized + 'static {
    /// Whether drivers created by this provider may receive [`DriverRole::Primary`](crate::DriverRole::Primary).
    ///
    /// The runtime may assign primary only when this flag is `true` and the worker has no
    /// primary. Read the assignment through [`DriverOptions::role`].
    const CAN_BE_PRIMARY: bool;

    /// The context type associated with this provider.
    type Context: IoContext<Provider = Self>;

    /// The driver type created by this provider.
    type Driver: Driver;

    /// Creates a driver and context for the worker described by `options`.
    ///
    /// Runs on the owning worker. Return promptly without waiting for another runtime worker.
    /// The context may outlive the driver and must reject operations after admission closes.
    ///
    /// Earlier peers are available through [`DriverOptions::drivers`]. Their borrowed handles
    /// may be used to clone independently owned state.
    ///
    /// Do not publish the context. The runtime first completes a zero-wait
    /// [`Driver::execute_cycle`] to establish notification and finish initialization.
    ///
    /// # Errors
    ///
    /// Returns initialization failures. Partial state must be safe to drop, with no context
    /// published; the runtime rolls back the pair.
    fn create(self, options: DriverOptions<'_>) -> Result<(Self::Driver, Self::Context), DriverError>;
}
