// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use thread_aware_core::ThreadAware;

use crate::{ContextOptions, Driver, DriverError, DriverInstance, DriverOptions};

/// A consumer-facing I/O handle that creates worker-local drivers.
///
/// The runtime registers each concrete context type once. On its first request, it creates the
/// context, clones and relocates it to every active worker, creates a driver on each worker, and
/// completes each driver's zero-wait initialization cycle before publishing the context. Later
/// requests reuse the registration.
///
/// See the [single-worker runtime example] for registration with drivers that perform no I/O.
///
/// [single-worker runtime example]: https://github.com/microsoft/oxidizer/tree/main/crates/arty_io_core/examples/single_thread_runtime
pub trait IoContext: Clone + ThreadAware + 'static {
    /// The driver type created by this context.
    type Driver: Driver;

    /// Creates the context for a registration.
    ///
    /// The runtime calls this method at most once per registered context type, passing
    /// `options`. Repeated calls should return equivalent contexts.
    #[must_use]
    fn create_context(options: ContextOptions) -> Self;

    /// Creates a driver for the worker described by `options`.
    ///
    /// The runtime calls this method on the owning worker after cloning and relocating the
    /// context. Implementations must return promptly without waiting for another runtime worker.
    /// The context may outlive the driver and must reject operations after admission closes.
    ///
    /// The context must not be published yet. The runtime first completes a zero-wait
    /// [`Driver::execute_cycle`] to establish notification and finish initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails. Partial state must be safe to drop, with no
    /// context published; the runtime rolls back the driver.
    fn create_driver(&mut self, options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError>;
}
