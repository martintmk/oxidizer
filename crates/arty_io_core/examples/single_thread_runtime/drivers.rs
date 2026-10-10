// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::task::Waker;

use arty_io_core::{ContextOptions, Cycle, Driver, DriverError, DriverInstance, DriverOptions, DriverRole, IoContext, ShutdownError};
use thread_aware_core::{Thread, ThreadAware};

#[derive(Clone)]
pub(super) struct SampleContext;

impl ThreadAware for SampleContext {
    fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
}

impl IoContext for SampleContext {
    type Driver = SampleDriver;

    fn create_context(_options: ContextOptions) -> Self {
        Self
    }

    fn create_driver(&mut self, options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError> {
        let role = if options.allowed_roles().contains(&DriverRole::Primary) {
            DriverRole::Primary
        } else {
            DriverRole::Secondary
        };
        println!("initializing sample driver as {role:?}");
        Ok(DriverInstance::new(SampleDriver { role }, role))
    }
}

pub(super) struct SampleDriver {
    role: DriverRole,
}

impl Driver for SampleDriver {
    fn waker(&self) -> Waker {
        Waker::noop().clone()
    }

    fn execute_cycle(&mut self, _cycle: &mut Cycle) -> Result<(), DriverError> {
        let _ = self.role;
        Ok(())
    }

    fn shutdown(self) -> Result<(), ShutdownError> {
        println!("shutting down sample driver");
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct EchoContext;

impl ThreadAware for EchoContext {
    fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
}

impl IoContext for EchoContext {
    type Driver = EchoDriver;

    fn create_context(_options: ContextOptions) -> Self {
        Self
    }

    fn create_driver(&mut self, _options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError> {
        let role = DriverRole::Secondary;
        println!("initializing echo driver as {role:?}");
        Ok(DriverInstance::new(EchoDriver { role }, role))
    }
}

pub(super) struct EchoDriver {
    role: DriverRole,
}

impl Driver for EchoDriver {
    fn waker(&self) -> Waker {
        Waker::noop().clone()
    }

    fn execute_cycle(&mut self, _cycle: &mut Cycle) -> Result<(), DriverError> {
        let _ = self.role;
        Ok(())
    }

    fn shutdown(self) -> Result<(), ShutdownError> {
        println!("shutting down echo driver");
        Ok(())
    }
}
