// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Lets [`arty`] tasks use Tokio-based libraries.
//!
//! Use [`TokioSupportExt::tokio_support`] from an async main function to enter
//! the same Tokio runtime context on every live Arty worker:
//!
//! ```
//! use arty::task::Context;
//! use arty_tokio::TokioSupportExt as _;
//! use tokio::runtime::Builder;
//!
//! # #[arty::main]
//! # async fn main(cx: Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let tokio = Builder::new_multi_thread().enable_all().build()?;
//! cx.control().tokio_support(tokio.handle());
//! let value = tokio::spawn(async { 42 }).await?;
//! assert_eq!(value, 42);
//! # Ok(())
//! # }
//! ```
//!
//! Use [`TokioSupportExt::tokio_support`] to configure workers before the
//! runtime starts:
//!
//! ```
//! use arty::runtime::{Runtime, WorkersPolicy};
//! use arty_tokio::TokioSupportExt as _;
//! use tokio::runtime::Builder;
//!
//! let tokio = Builder::new_multi_thread().enable_all().build()?;
//! let runtime = Runtime::builder()
//!     .workers(WorkersPolicy::exactly(2))
//!     .tokio_support(tokio.handle())
//!     .build()?;
//! assert_eq!(runtime.scheduler().block_on(async |_| 42)?, 42);
//! runtime.stop()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The application owns the Tokio runtime. Keep it alive while Arty tasks use
//! Tokio. This crate stores a clone of its handle on each Arty worker.
//! Current-thread Tokio runtimes are not supported.
//! Use [`TokioHandle::default`] when the application does not supply a runtime.
//! The returned handle owns a default multi-thread Tokio runtime.
//!
//! After setup, Arty tasks can use Tokio timers, networking, I/O resources,
//! task spawning, and libraries that require a Tokio runtime context.
//! Tokio tasks run on Tokio workers. They do not receive
//! [`arty::task::Context`] or keep Arty thread-aware state.
//!
//! Do not call `tokio::task::block_in_place`, `Runtime::block_on`, or
//! `Handle::block_on` from an Arty worker. `LocalSet` and `spawn_local` are not
//! integrated with Arty.

use std::sync::Arc;

use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

mod extensions;

pub use extensions::TokioSupportExt;

/// Selects the Tokio runtime that Arty workers enter.
///
/// Convert an owned or borrowed Tokio [`Handle`] from an application-owned
/// multi-thread runtime. The value is opaque and its [`Debug`] output does not
/// expose runtime details.
/// [`Default`] creates and owns a new runtime through [`Runtime::new`].
///
/// Convert an owned runtime to let the handle keep it alive:
///
/// ```
/// use arty_tokio::TokioHandle;
///
/// let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
/// let _handle = TokioHandle::from(runtime);
/// ```
#[derive(derive_more::Debug)]
pub struct TokioHandle {
    #[debug(ignore)]
    handle: Handle,
    #[debug(ignore)]
    owned_runtime: Option<Arc<Runtime>>,
}

impl TokioHandle {
    fn assert_supported(&self) {
        assert!(
            matches!(self.handle.runtime_flavor(), RuntimeFlavor::MultiThread),
            "Tokio support requires a multi-thread Tokio runtime"
        );
    }
}

impl Default for TokioHandle {
    fn default() -> Self {
        Runtime::new()
            .unwrap_or_else(|error| panic!("failed to create the default Tokio runtime: {error}"))
            .into()
    }
}

impl From<Runtime> for TokioHandle {
    fn from(runtime: Runtime) -> Self {
        let runtime = Arc::new(runtime);
        Self {
            handle: runtime.handle().clone(),
            owned_runtime: Some(runtime),
        }
    }
}

impl From<Handle> for TokioHandle {
    fn from(handle: Handle) -> Self {
        Self {
            handle,
            owned_runtime: None,
        }
    }
}

impl From<&Handle> for TokioHandle {
    fn from(handle: &Handle) -> Self {
        handle.clone().into()
    }
}
