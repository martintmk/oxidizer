// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use thread_aware_core::ThreadAware;

use crate::{Driver, DriverError, DriverInstance, DriverOptions};

/// A consumer-facing I/O handle that creates worker-local drivers.
///
/// The runtime creates one registration for each concrete context type. The first context
/// retrieval creates and centrally stores a thread-neutral context. Each async worker clones and
/// relocates that context from no source thread and creates its local driver when it first
/// retrieves the context. Later retrievals return a clone of the existing worker-local context.
pub trait IoContext: Clone + ThreadAware + 'static {
    /// Configuration used to create the context.
    type ContextOptions;

    /// The driver type created by this context.
    type Driver: Driver;

    /// Creates the thread-neutral context for the first retrieval of this concrete context type.
    ///
    /// The runtime calls this method once and passes the options supplied by the caller. It stores
    /// the returned value without relocating it, then relocates clones from no source thread to
    /// each worker. Implementations must not retrieve another I/O context from this method.
    #[must_use]
    fn context(options: Self::ContextOptions) -> Self;

    /// Creates a driver for the async worker described by `options`.
    ///
    /// The runtime calls this method on each async worker after cloning and relocating the context.
    /// Implementations must finish all driver setup before returning and must not wait for work to
    /// execute on another async worker. The context may outlive the driver and must reject new
    /// operations after [`Driver::shutdown`] closes it.
    ///
    /// A successful instance must select a role listed by [`DriverOptions::allowed_roles`].
    /// One context registration must select the same role on every async worker. Return an error
    /// when the driver supports none of the allowed roles.
    ///
    /// # Errors
    ///
    /// Returns an error if driver creation fails. The implementation must clean up partial
    /// construction before returning. The runtime treats the failure as fatal and starts runtime
    /// shutdown. The neutral context may already be registered, but no further retrieval succeeds
    /// after shutdown begins. The runtime calls [`Driver::shutdown`] on every driver that was
    /// created successfully.
    fn driver(&mut self, options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError>;
}
