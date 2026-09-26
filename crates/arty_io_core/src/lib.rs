// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(missing_docs)]
#![cfg_attr(all(coverage_nightly, test), feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_logo_url = "https://media.githubusercontent.com/media/microsoft/oxidizer/refs/heads/main/crates/arty_io_core/logo.png")]
#![doc(html_favicon_url = "https://media.githubusercontent.com/media/microsoft/oxidizer/refs/heads/main/crates/arty_io_core/favicon.ico")]

//! Contracts for integrating I/O drivers with an async runtime.
//!
//! Shared types for runtimes and independently versioned drivers. This crate provides neither
//! a runtime nor an I/O implementation.
//!
//! # Core types
//!
//! - [`IoContext`] selects a [`DriverProvider`], which creates a [`Driver`] and context per worker.
//! - [`DriverRole`] identifies an optional worker-blocking primary and non-blocking secondaries.
//! - [`Cycle`] supplies timing, blocking permission, and work registration through [`PendingWorkTracker`].
//! - [`PendingWork`] holds one participation in the runtime's completion barrier.
//! - [`DriverOptions`] supplies per-worker construction facilities and peer handles.
//! - [`SystemTaskSpawner`] runs blocking system work outside async workers.
//! - [`DriverError`] and [`ShutdownError`] report infrastructure and cleanup failures.
//!
//! # Registration
//!
//! The runtime registers each context type once, initializing a driver/context pair on every
//! active worker. It assigns a role and completes a zero-wait cycle before publishing the
//! context. Peers discover each other through [`DriverOptions::drivers`] and
//! [`Driver::on_peer_registered`].
//!
//! # Driving I/O
//!
//! Secondaries run before the primary, with a shared time snapshot and wait bound. Only an
//! invocation with [`Cycle::can_block`] set to `true` may block the worker.
//!
//! Drivers register native waits through [`Cycle::start_work`]. They keep the handle until
//! work ends and publish any results before completing or dropping it; both notify the runtime.
//! After the primary returns, the runtime interrupts remaining waits and waits for all handles
//! before advancing.
//!
//! The runtime implements [`PendingWorkTracker`], including interruption state, counters,
//! and parking. The registration example uses a no-op tracker and performs no I/O.
//!
//! # Shutdown
//!
//! [`Driver::shutdown`] consumes the driver and drains its resources within a bounded wait.
//! Contexts remain valid as closed handles. Shutdown must progress independently of other
//! drivers on the same worker.
//!
//! # Project documents
//!
//! - [Requirements](https://github.com/microsoft/oxidizer/blob/main/crates/arty_io_core/docs/REQUIREMENTS.md)
//! - [Design](https://github.com/microsoft/oxidizer/blob/main/crates/arty_io_core/docs/DESIGN.md)
//! - [Completion coordination (exploratory)](https://github.com/microsoft/oxidizer/blob/main/crates/arty_io_core/docs/COMPLETION_COORDINATION.md)

mod cycle;
mod driver;
mod driver_error;
mod driver_handle;
mod driver_options;
mod driver_role;
mod io_context;
mod pending_work;
mod pending_work_tracker;
mod provider;
mod provider_options;
mod shutdown_error;
mod system_task_spawner;

pub use cycle::Cycle;
pub use driver::Driver;
pub use driver_error::DriverError;
pub use driver_handle::DriverHandle;
pub use driver_options::DriverOptions;
pub use driver_role::DriverRole;
pub use io_context::IoContext;
pub use pending_work::PendingWork;
pub use pending_work_tracker::PendingWorkTracker;
pub use provider::DriverProvider;
pub use provider_options::ProviderOptions;
pub use shutdown_error::ShutdownError;
pub use system_task_spawner::{SystemTask, SystemTaskSpawner};
