// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;
use std::task::Poll;
use std::{fmt, io};

use arty_io_core::{DriverError, DriverOptions, IoContext};
use mio::Interest;
use mio::event::Source;
use thread_aware_core::{Thread, ThreadAware};

use crate::driver::MioDriver;
use crate::options::MioOptions;
use crate::registration::{Registered, register};
use crate::worker::{ContextState, WorkerState};

/// A thread-aware handle that submits sources to a worker-local Mio driver.
///
/// New registrations use the current worker when it has a driver. Otherwise,
/// they use the last driver selected for this clone. Relocation selects an
/// available destination driver. Existing registrations stay on their
/// original poller.
#[derive(Clone)]
pub struct MioContext {
    state: Arc<ContextState>,
    worker: Option<Arc<WorkerState>>,
}

impl fmt::Debug for MioContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MioContext")
            .field("attached", &self.worker.is_some())
            .finish_non_exhaustive()
    }
}

impl MioContext {
    /// Registers an owned Mio source with this context's driver.
    ///
    /// The future registers the source through a thread-safe Mio registry
    /// handle.
    ///
    /// # Errors
    ///
    /// Returns an error if this context is not attached to a driver, the
    /// driver is closed, or Mio rejects the source.
    pub fn register<S>(&self, source: S, interest: Interest) -> impl Future<Output = io::Result<Registered<S>>> + Send + '_
    where
        S: Source + Send + 'static,
    {
        let mut source = Some(source);
        std::future::poll_fn(move |_context| {
            let source = source
                .take()
                .ok_or_else(|| io::Error::other("a completed MioContext::register future was polled again"))?;
            let current = std::thread::current().id();
            let worker = self
                .worker
                .as_ref()
                .filter(|worker| worker.thread_id == current)
                .cloned()
                .or_else(|| self.state.worker(current))
                .or_else(|| self.worker.clone())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotConnected,
                        "the Arty Mio context is not attached to a worker driver",
                    )
                })?;
            Poll::Ready(register(source, interest, worker))
        })
    }
}

impl ThreadAware for MioContext {
    fn relocate(&mut self, _source: Option<&Thread>, destination: &Thread) {
        // A missing destination route leaves the previous route usable.
        if let Some(worker) = self.state.try_worker(destination.id()) {
            self.worker = Some(worker);
        }
    }
}

impl IoContext for MioContext {
    type ContextOptions = MioOptions;
    type Driver = MioDriver;

    fn context(_options: Self::ContextOptions) -> Self {
        Self {
            state: Arc::new(ContextState::default()),
            worker: None,
        }
    }

    fn driver(&mut self, options: DriverOptions) -> Result<Self::Driver, DriverError> {
        let (driver, worker) = MioDriver::new(Arc::clone(&self.state), &options)?;
        self.worker = Some(worker);
        Ok(driver)
    }
}
