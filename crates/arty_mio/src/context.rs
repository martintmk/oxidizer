// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;
use std::{fmt, io};

use arty_io_core::{DriverError, DriverOptions, IoContext};
use mio::Interest;
use mio::event::Source;
use thread_aware_core::{Thread, ThreadAware};

use crate::driver::MioDriver;
use crate::options::MioOptions;
use crate::registration::{RegisterCommand, Registered, Response};
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
    /// The future submits the source through a thread-safe queue. The driver
    /// performs the Mio registry operation on its poll thread.
    ///
    /// # Errors
    ///
    /// Returns an error if this context is not attached to a driver, the
    /// submission queue is full, the driver is closed, or Mio rejects the
    /// source.
    pub async fn register<S>(&self, source: S, interest: Interest) -> io::Result<Registered<S>>
    where
        S: Source + Send + 'static,
    {
        let worker = self
            .state
            .worker(std::thread::current().id())
            .or_else(|| self.worker.clone())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "the Arty Mio context is not attached to a worker driver",
                )
            })?;
        let response = Arc::new(Response::default());
        worker.enqueue(Box::new(RegisterCommand::new(source, interest, Arc::clone(&response))))?;
        response.wait().await
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
