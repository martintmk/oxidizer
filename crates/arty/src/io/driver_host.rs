// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, type_name};
use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::{Rc, Weak};
use std::time::Duration;

use arty_io_core::{Cycle, Driver, DriverError, DriverOptions, DriverRole, IoContext};
use observed::{Sink, emit};
use performables::arc::Arc;
use thread_aware::{Thread, ThreadAware};

use super::runtime_state::Resolution;
use super::{IoRuntime, fail_runtime, runtime_error};
use crate::runtime::dispatch::DispatcherClient;
use crate::runtime::telemetry::events::{
    IoContextName, IoDriverCycleFailed, IoDriverErrorMessage, IoDriverInitializationFailed, IoDriverInitialized, IoDriverRoleLabel,
    IoDriverShutdownFailed,
};
use crate::runtime::{Error, WorkerSignal};

thread_local! {
    static CURRENT: RefCell<Option<Weak<DriverHost>>> = const { RefCell::new(None) };
}

trait ErasedDriver {
    fn execute_cycle(&mut self, cycle: &mut Cycle) -> Result<(), DriverError>;
    fn shutdown(self: Box<Self>) -> Result<(), DriverError>;
}

impl<D> ErasedDriver for D
where
    D: Driver,
{
    fn execute_cycle(&mut self, cycle: &mut Cycle) -> Result<(), DriverError> {
        Driver::execute_cycle(self, cycle)
    }

    fn shutdown(self: Box<Self>) -> Result<(), DriverError> {
        Driver::shutdown(*self)
    }
}

struct LocalContext {
    context: Box<dyn Any>,
    driver: Box<dyn ErasedDriver>,
    role: DriverRole,
    context_name: &'static str,
}

struct InitializationGuard<'a>(&'a Cell<bool>);

impl Drop for InitializationGuard<'_> {
    fn drop(&mut self) {
        assert!(self.0.replace(false), "worker-local I/O initialization guard must be active");
    }
}

pub(crate) struct DriverHost {
    runtime: IoRuntime,
    dispatcher: DispatcherClient,
    thread: Thread,
    signal: Arc<WorkerSignal>,
    sink: Sink,
    primary: RefCell<Option<LocalContext>>,
    secondaries: RefCell<Vec<LocalContext>>,
    initializing: Cell<bool>,
}

impl fmt::Debug for DriverHost {
    #[cfg_attr(coverage_nightly, coverage(off))] // Opaque diagnostic formatting only.
    #[cfg_attr(test, mutants::skip)] // Debug formatting has no behavioral contract.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverHost").field("thread", &self.thread).finish_non_exhaustive()
    }
}

impl DriverHost {
    pub(crate) fn new(runtime: IoRuntime, dispatcher: DispatcherClient, thread: Thread, signal: Arc<WorkerSignal>, sink: Sink) -> Rc<Self> {
        let host = Rc::new(Self {
            runtime,
            dispatcher,
            thread,
            signal,
            sink,
            primary: RefCell::new(None),
            secondaries: RefCell::new(Vec::new()),
            initializing: Cell::new(false),
        });
        CURRENT.with_borrow_mut(|current| {
            assert!(
                current.as_ref().and_then(Weak::upgrade).is_none(),
                "an I/O driver host is already installed on this worker"
            );
            *current = Some(Rc::downgrade(&host));
        });
        host
    }

    pub(crate) fn try_context_current<C>(runtime: &IoRuntime, thread: &Thread, options: &C::ContextOptions) -> Option<Result<C, Error>>
    where
        C: IoContext,
        C::ContextOptions: Clone + Send + 'static,
    {
        Self::current(runtime, thread).map(|host| host.try_context(options))
    }

    fn current(runtime: &IoRuntime, thread: &Thread) -> Option<Rc<Self>> {
        CURRENT.with_borrow(|current| {
            current
                .as_ref()
                .and_then(Weak::upgrade)
                .filter(|host| host.runtime.eq(runtime) && host.thread == *thread)
        })
    }

    fn try_context<C>(&self, options: &C::ContextOptions) -> Result<C, Error>
    where
        C: IoContext,
        C::ContextOptions: Clone + Send + 'static,
    {
        if let Some(error) = runtime_error(&self.dispatcher) {
            return Err(error);
        }
        if let Some(context) = self
            .primary
            .borrow()
            .as_ref()
            .and_then(Self::try_clone_context::<C>)
            .or_else(|| self.secondaries.borrow().iter().find_map(Self::try_clone_context::<C>))
        {
            return runtime_error(&self.dispatcher).map_or(Ok(context), Err);
        }
        if self.initializing.replace(true) {
            return Err(Error::nested_io_context());
        }
        let initialization = InitializationGuard(&self.initializing);
        let Resolution { mut context, role } = self.runtime.get_context::<C>(options);
        context.relocate(None, &self.thread);
        let local = self.create_local_driver::<C>(context, role)?;
        let context = Self::clone_context::<C>(&local);
        match local.role {
            DriverRole::Primary => {
                assert!(
                    self.primary.borrow_mut().replace(local).is_none(),
                    "a worker has at most one primary I/O driver"
                );
            }
            DriverRole::Secondary => self.secondaries.borrow_mut().push(local),
        }
        drop(initialization);
        runtime_error(&self.dispatcher).map_or(Ok(context), Err)
    }

    fn create_local_driver<C>(&self, mut context: C, role: DriverRole) -> Result<LocalContext, Error>
    where
        C: IoContext,
    {
        let options = DriverOptions::new(self.thread.clone(), WorkerSignal::waker(&self.signal), role);
        let driver = context
            .driver(options)
            .inspect_err(|error| {
                emit!(
                    &self.sink,
                    IoDriverInitializationFailed {
                        context: IoContextName(type_name::<C>()),
                        error: IoDriverErrorMessage::new(error),
                    }
                );
            })
            .map_err(|error| self.fail(error))?;
        if role == DriverRole::Primary {
            self.signal.set_primary_waker(driver.waker());
        }
        emit!(
            &self.sink,
            IoDriverInitialized {
                context: IoContextName(type_name::<C>()),
                role: IoDriverRoleLabel::from(role),
            }
        );
        Ok(LocalContext {
            context: Box::new(context),
            driver: Box::new(driver),
            role,
            context_name: type_name::<C>(),
        })
    }

    fn clone_context<C>(local: &LocalContext) -> C
    where
        C: IoContext,
    {
        local
            .context
            .downcast_ref::<C>()
            .expect("worker-local I/O contexts are stored under their own TypeId")
            .clone()
    }

    fn try_clone_context<C>(local: &LocalContext) -> Option<C>
    where
        C: IoContext,
    {
        local.context.downcast_ref::<C>().cloned()
    }

    pub(crate) fn execute_cycle(&self, max_wait: Duration) -> Result<(), Error> {
        for local in self.secondaries.borrow_mut().iter_mut() {
            Self::execute_driver(local, Duration::ZERO, &self.sink).map_err(|error| self.fail(error))?;
        }
        if let Some(primary) = self.primary.borrow_mut().as_mut() {
            Self::execute_driver(primary, max_wait, &self.sink).map_err(|error| self.fail(error))?;
        }
        Ok(())
    }

    fn execute_driver(local: &mut LocalContext, max_wait: Duration, sink: &Sink) -> Result<(), DriverError> {
        local.driver.execute_cycle(&mut Cycle::new(max_wait)).inspect_err(|error| {
            emit!(
                sink,
                IoDriverCycleFailed {
                    context: IoContextName(local.context_name),
                    role: IoDriverRoleLabel::from(local.role),
                    error: IoDriverErrorMessage::new(error),
                }
            );
        })
    }

    fn fail(&self, error: DriverError) -> Error {
        fail_runtime(&self.dispatcher, error)
    }

    pub(crate) fn shutdown(&self) {
        self.signal.clear_primary_waker();

        let primary = self.primary.borrow_mut().take();
        let secondaries = std::mem::take(&mut *self.secondaries.borrow_mut());
        for local in secondaries.into_iter().chain(primary) {
            if let Err(error) = local.driver.shutdown() {
                emit!(
                    &self.sink,
                    IoDriverShutdownFailed {
                        context: IoContextName(local.context_name),
                        error: IoDriverErrorMessage::new(&error),
                    }
                );
                self.dispatcher.stop_with_error(Error::io_failure(Arc::new(error)));
            }
        }
    }
}
