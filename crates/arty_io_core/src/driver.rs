// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::{Cycle, DriverError, DriverHandle, ShutdownError};

/// A worker-local I/O driver.
///
/// The runtime calls every method on the owning worker. Drivers need not be [`Send`] or
/// [`Sync`]; native observers, queue sharing, routing, and cancellation are driver-owned.
///
/// Dropping a driver must always be memory-safe. State reachable through contexts, callbacks,
/// observers, or wakers must remain valid independently of the driver.
pub trait Driver: 'static {
    /// Returns a borrowed handle for peer discovery.
    ///
    /// Peers may clone independently owned state from the handle, but cannot retain its borrow.
    #[must_use]
    fn handle(&self) -> DriverHandle<'_>;

    /// Notifies this driver of a newly registered peer before its context is published.
    ///
    /// # Panics
    ///
    /// Panics if the peer cannot be integrated; the runtime cannot continue a partially
    /// connected registration. Report native initialization failures during creation or the
    /// initial cycle instead.
    fn on_peer_registered(&mut self, peer: DriverHandle<'_>);

    /// Processes submissions and completions, optionally waiting for I/O.
    ///
    /// The runtime invokes secondaries before the primary, sharing [`Cycle::started_at`] and
    /// [`Cycle::max_wait`]. Only an invocation with [`Cycle::can_block`] set to `true` may block
    /// the worker. Secondaries may arm background waits but must not wait for them to finish.
    ///
    /// Before publishing a context or notifying peers, the runtime runs a zero-wait cycle with
    /// `can_block` set to `false`. Establish native notification and recheck work queued during
    /// construction in this initial call.
    ///
    /// Register each native wait with [`Cycle::start_work`] before entering or scheduling it.
    /// Keep its [`PendingWork`](crate::PendingWork) alive until the work ends. The runtime
    /// interrupts remaining waits after the primary returns and waits for all handles before
    /// starting the next cycle. An interrupted wait still requires completion processing.
    ///
    /// Process a bounded batch. If serviceable work remains, complete a pending-work handle
    /// before returning. In-flight operations alone do not indicate serviceable work.
    ///
    /// # Errors
    ///
    /// Returns infrastructure failures, not individual I/O results. The runtime rolls back an
    /// unpublished driver/context pair on initialization failure; during normal operation it
    /// reports the error and shuts down the worker's drivers.
    fn execute_cycle(&mut self, cycle: &mut Cycle<'_>) -> Result<(), DriverError>;

    /// Closes admission and blocks until driver resources have drained.
    ///
    /// Drain active operations, callbacks, and observers, or return an error. Context handles
    /// remain valid as closed handles and do not themselves delay shutdown.
    ///
    /// Bound the wait and make progress locally or on independent threads. Normal cycles have
    /// stopped: do not depend on cycle coordination or another driver on the same worker.
    ///
    /// Dropping must remain memory-safe after either result. Cancellation alone does not prove
    /// that native code has stopped accessing operation storage.
    ///
    /// The runtime keeps [`SystemTaskSpawner`](crate::SystemTaskSpawner) available until all
    /// shutdown calls return and attempts remaining drivers after an error. It may shut down
    /// secondaries before the primary.
    ///
    /// # Errors
    ///
    /// Returns an error if graceful cleanup cannot be completed.
    fn shutdown(self) -> Result<(), ShutdownError>
    where
        Self: Sized;
}
