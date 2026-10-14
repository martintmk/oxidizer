// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! I/O drivers for Arty runtimes.
//!
//! Arty hosts I/O drivers but does not implement I/O itself. An I/O provider
//! supplies an [`IoContext`](arty_io_core::IoContext) and
//! [`Driver`](arty_io_core::Driver); Arty runs the driver and gives tasks access
//! to the context. Providers remain free to use any protocol, library, or
//! operating-system facility.
//!
//! # Example
//!
//! Obtain an I/O context from the task's built-in services, then use the
//! operations exposed by that context:
//!
//! ```
//! # use std::task::Waker;
//! # use arty::core::{Thread, ThreadAware};
//! # use arty::task::Builtins;
//! # use arty_io_core::{Cycle, Driver, DriverError, DriverInstance, DriverOptions, DriverRole, IoContext};
//! # #[derive(Clone)]
//! # struct ExampleIo;
//! # impl ExampleIo {
//! #     fn submit(&self, _request: &str) {}
//! # }
//! # impl ThreadAware for ExampleIo {
//! #     fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
//! # }
//! # impl IoContext for ExampleIo {
//! #     type ContextOptions = ();
//! #     type Driver = ExampleDriver;
//! #
//! #     fn context((): Self::ContextOptions) -> Self {
//! #         Self
//! #     }
//! #
//! #     fn driver(&mut self, options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError> {
//! #         let role = options.allowed_roles().first().copied().ok_or_else(|| {
//! #             DriverError::from_message("the example driver requires an available role")
//! #         })?;
//! #         Ok(DriverInstance::new(ExampleDriver, role))
//! #     }
//! # }
//! # struct ExampleDriver;
//! # impl Driver for ExampleDriver {
//! #     fn waker(&self) -> Waker {
//! #         Waker::noop().clone()
//! #     }
//! #
//! #     fn execute_cycle(&mut self, _cycle: &mut Cycle) -> Result<(), DriverError> {
//! #         Ok(())
//! #     }
//! #
//! #     fn shutdown(self) -> Result<(), DriverError> {
//! #         Ok(())
//! #     }
//! # }
//! #[arty::main]
//! async fn main(cx: Builtins) {
//!     let io = cx.io().context::<ExampleIo>(&());
//!     io.submit("request");
//! }
//! ```
//!
//! `ExampleIo` stands in for a provider's context; real contexts expose their
//! own operations.
//!
//! # Using multiple providers
//!
//! A runtime can host multiple context types, with one driver of each type per
//! worker. One may be the blocking
//! [`Primary`](arty_io_core::DriverRole::Primary); the others run as non-blocking
//! [`Secondary`](arty_io_core::DriverRole::Secondary) drivers.
//!
//! Request a context with [`Io::context`]. Each concrete context type is
//! registered once per runtime and reused on later requests.

mod driver_host;
mod runtime_state;

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arty_io_core::DriverError;
pub(crate) use driver_host::DriverHost;
use performables::sync::channel;
pub(crate) use runtime_state::IoRuntime;
use thread_aware::{Thread, ThreadAware};

use crate::runtime::Error;
use crate::runtime::dispatch::{DispatcherClient, WorkerIndex};
use crate::task::{Builtins, Scheduler};

pub(crate) type DriverCommand = Box<dyn FnOnce() + Send>;

/// Access to I/O contexts hosted by an Arty runtime.
///
/// Obtain this handle from [`Builtins::io`](crate::task::Builtins::io). Clones
/// remain associated with the same worker until their runtime relocates them.
/// They can be used from ordinary threads; such access synchronously dispatches
/// to the associated worker and is slower than worker-local access.
///
/// A context type is registered once per runtime. The first request creates,
/// clones, and relocates the context, then initializes one driver on every
/// worker before returning it to application code.
#[derive(Clone)]
pub struct Io {
    runtime: Arc<IoRuntime>,
    dispatcher: DispatcherClient,
    thread: Thread,
}

impl Io {
    pub(crate) fn new(runtime: Arc<IoRuntime>, dispatcher: DispatcherClient, thread: Thread) -> Self {
        Self {
            runtime,
            dispatcher,
            thread,
        }
    }

    /// Returns the runtime's context of type `C`, registering it if necessary.
    ///
    /// `options` must be initialized before this call. It is cloned and
    /// [relocated](ThreadAware::relocate) only when this call performs the
    /// runtime-wide registration for `C`.
    ///
    /// Each concrete context type is initialized once per runtime. The first
    /// successful call fixes the context created from `options`; later calls
    /// return clones of that context and ignore any different options supplied.
    ///
    /// # Panics
    ///
    /// Panics if the driver cannot be initialized, the runtime is shutting
    /// down, the handle is no longer bound to an active worker, or the caller
    /// is another Arty worker. Driver initialization failure also requests
    /// runtime shutdown.
    #[must_use]
    pub fn context<C>(&self, options: &C::ContextOptions) -> C
    where
        C: arty_io_core::IoContext,
        C::ContextOptions: Clone + ThreadAware,
    {
        match self.try_context(options) {
            Ok(context) => context,
            Err(error) => panic_context(&error),
        }
    }

    /// Returns the runtime's context of type `C`, registering it if necessary.
    ///
    /// `options` must be initialized before this call. It is cloned and
    /// [relocated](ThreadAware::relocate) only when this call performs the
    /// runtime-wide registration for `C`.
    ///
    /// Each concrete context type is initialized once per runtime. The first
    /// successful call fixes the context created from `options`; later calls
    /// return clones of that context and ignore any different options supplied.
    /// A newly registered context is initialized on every worker before this
    /// method returns.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the worker-local driver cannot be initialized,
    /// the runtime is shutting down, the handle is no longer bound to an active
    /// worker, or the caller is another Arty worker. Driver initialization
    /// failure also requests runtime shutdown.
    pub(crate) fn try_context<C>(&self, options: &C::ContextOptions) -> Result<C, Error>
    where
        C: arty_io_core::IoContext,
        C::ContextOptions: Clone + ThreadAware,
    {
        if let Some(error) = self.runtime_error() {
            return Err(error);
        }

        if DriverHost::is_current(&self.runtime, &self.thread) {
            let destination = self.thread.clone();
            let result = DriverHost::try_context_current(&self.runtime, &self.thread, || {
                let mut options = options.clone();
                options.relocate(None, &destination);
                options
            });
            let result = result?;
            return self.finish_context(result);
        }
        if Scheduler::is_current_worker_thread() {
            return Err(Error::io_outside_worker());
        }
        let Some(worker_index) = self.dispatcher.worker_index(self.thread.id()) else {
            return Err(Error::io_outside_worker());
        };
        let options = (!self.runtime.is_registered::<C>()).then(|| options.clone());
        self.try_context_from_external_thread(worker_index, options)
    }

    fn try_context_from_external_thread<C>(&self, worker_index: WorkerIndex, options: Option<C::ContextOptions>) -> Result<C, Error>
    where
        C: arty_io_core::IoContext,
        C::ContextOptions: Clone + ThreadAware,
    {
        let (result_tx, result_rx) = channel::unbounded();
        let io = self.clone();
        let access = Box::new(move |_: &Builtins| {
            let destination = io.thread.clone();
            let result = catch_unwind(AssertUnwindSafe(|| {
                io.try_context_on_worker(|| {
                    let mut options = options.expect("options are present when the context is not registered");
                    options.relocate(None, &destination);
                    options
                })
            }));
            _ = result_tx.send(result);
        });
        if !self.dispatcher.access_io(worker_index, access) {
            return Err(self.runtime_error().unwrap_or_else(Error::runtime_shutting_down));
        }
        match result_rx
            .recv()
            .map_err(|disconnected| self.runtime_error().unwrap_or_else(|| Error::new(disconnected)))?
        {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn try_context_on_worker<C>(&self, options: impl FnOnce() -> C::ContextOptions) -> Result<C, Error>
    where
        C: arty_io_core::IoContext,
        C::ContextOptions: Clone + ThreadAware,
    {
        let result = DriverHost::try_context_current(&self.runtime, &self.thread, options)?;
        self.finish_context(result)
    }

    fn finish_context<C>(&self, (context, registered): (C, bool)) -> Result<C, Error>
    where
        C: arty_io_core::IoContext,
    {
        if let Some(error) = self.runtime_error() {
            return Err(error);
        }
        if registered {
            self.initialize_everywhere::<C>()?;
        }
        DriverHost::wait_until_ready_current::<C>(&self.runtime, &self.thread)?;
        self.runtime_error().map_or(Ok(context), Err)
    }

    fn initialize_everywhere<C>(&self) -> Result<(), Error>
    where
        C: arty_io_core::IoContext,
    {
        if self.dispatcher.prepare_io_everywhere::<C>() {
            return Ok(());
        }
        if let Some(error) = self.runtime_error() {
            return Err(error);
        }
        let error = self.runtime.record_failure(DriverError::from_message(
            "runtime worker stopped before I/O registration was delivered",
        ));
        self.dispatcher.stop();
        Err(Error::io_failure(error))
    }

    fn runtime_error(&self) -> Option<Error> {
        self.runtime
            .failure()
            .or_else(|| self.dispatcher.is_shutting_down().then(Error::runtime_shutting_down))
    }
}

#[cold]
#[inline(never)]
#[expect(clippy::panic, reason = "get intentionally converts returned runtime errors into panics")]
fn panic_context(error: &Error) -> ! {
    panic!("failed to access Arty I/O context: {error}");
}

impl fmt::Debug for Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Io").field("thread", &self.thread).finish_non_exhaustive()
    }
}

impl ThreadAware for Io {
    fn relocate(&mut self, _source: Option<&Thread>, destination: &Thread) {
        if self.dispatcher.owns(destination) {
            self.thread = destination.clone();
        }
    }
}
