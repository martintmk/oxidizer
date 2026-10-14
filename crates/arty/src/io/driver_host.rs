// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, TypeId, type_name};
use std::cell::RefCell;
use std::fmt;
use std::rc::{Rc, Weak};
use std::task::Waker;
use std::time::Duration;

use arty_io_core::{Cycle, Driver, DriverError, DriverInstance, DriverOptions, DriverRole, IoContext};
use foldhash::{HashMap, HashSet};
use observed::{Sink, emit};
use performables::arc::Arc;
use performables::sync::channel;
use thread_aware::Thread;

use super::runtime_state::Resolution;
use super::{DriverCommand, IoRuntime, fail_runtime, runtime_error};
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

struct LocalInitialization<'a> {
    initializing: &'a RefCell<HashSet<TypeId>>,
    context_type: TypeId,
}

impl Drop for LocalInitialization<'_> {
    fn drop(&mut self) {
        assert!(
            self.initializing.borrow_mut().remove(&self.context_type),
            "worker-local I/O initialization guard must own its context type"
        );
    }
}

struct LocalContext {
    context: Box<dyn Any>,
    driver: Box<dyn ErasedDriver>,
    role: DriverRole,
    context_name: &'static str,
}

pub(crate) struct DriverHost {
    runtime: Arc<IoRuntime>,
    dispatcher: DispatcherClient,
    thread: Thread,
    owner_waker: Waker,
    signal: Arc<WorkerSignal>,
    command_rx: channel::Receiver<DriverCommand>,
    sink: Sink,
    contexts: RefCell<HashMap<TypeId, LocalContext>>,
    initializing: RefCell<HashSet<TypeId>>,
}

impl fmt::Debug for DriverHost {
    #[cfg_attr(coverage_nightly, coverage(off))] // Opaque diagnostic formatting only.
    #[cfg_attr(test, mutants::skip)] // Debug formatting has no behavioral contract.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverHost").field("thread", &self.thread).finish_non_exhaustive()
    }
}

impl DriverHost {
    pub(crate) fn new(
        runtime: Arc<IoRuntime>,
        dispatcher: DispatcherClient,
        thread: Thread,
        signal: Arc<WorkerSignal>,
        command_rx: channel::Receiver<DriverCommand>,
        sink: Sink,
    ) -> Rc<Self> {
        let owner_waker = WorkerSignal::waker(&signal);
        let host = Rc::new(Self {
            runtime,
            dispatcher,
            thread,
            owner_waker,
            signal,
            command_rx,
            sink,
            contexts: RefCell::new(HashMap::default()),
            initializing: RefCell::new(HashSet::default()),
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

    pub(crate) fn try_context_current<C>(
        runtime: &Arc<IoRuntime>,
        thread: &Thread,
        options: impl FnOnce() -> C::ContextOptions,
    ) -> Option<Result<C, Error>>
    where
        C: IoContext,
        C::ContextOptions: Clone,
    {
        Self::current(runtime, thread).map(|host| host.try_context(options))
    }

    fn current(runtime: &Arc<IoRuntime>, thread: &Thread) -> Option<Rc<Self>> {
        CURRENT.with_borrow(|current| {
            current
                .as_ref()
                .and_then(Weak::upgrade)
                .filter(|host| Arc::ptr_eq(&host.runtime, runtime) && host.thread == *thread)
        })
    }

    fn try_context<C>(&self, options: impl FnOnce() -> C::ContextOptions) -> Result<C, Error>
    where
        C: IoContext,
        C::ContextOptions: Clone,
    {
        if let Some(error) = runtime_error(&self.runtime, &self.dispatcher) {
            return Err(error);
        }
        let context_type = TypeId::of::<C>();
        assert!(
            self.initializing.borrow_mut().insert(context_type),
            "I/O context recursively requested its own registration on one worker"
        );
        let initialization = LocalInitialization {
            initializing: &self.initializing,
            context_type,
        };
        let local = self.contexts.borrow_mut().remove(&context_type);
        let (context, registered) = if let Some(local) = local {
            let context = Self::clone_context::<C>(&local);
            assert!(
                self.contexts.borrow_mut().insert(context_type, local).is_none(),
                "worker-local I/O initialization cannot replace an existing context"
            );
            (context, false)
        } else {
            let (mut context, allowed_roles, registered) = match self.runtime.resolve::<C>(&self.thread) {
                Resolution::Existing { context, allowed_roles } => (context, allowed_roles, false),
                Resolution::Register(reservation) => {
                    let mut context = C::context(options());
                    context.relocate(None, &self.thread);
                    let allowed_roles = reservation.publish(&context, &self.thread);
                    (context, allowed_roles, true)
                }
            };
            let (driver, role) = self
                .initialize_driver::<C>(&mut context, allowed_roles)
                .map_err(|error| self.fail(error))?;
            self.insert_local::<C>(context.clone(), driver, role);
            (context, registered)
        };
        drop(initialization);
        if registered {
            self.initialize_everywhere::<C>()?;
        }
        self.wait_until_ready::<C>()?;
        runtime_error(&self.runtime, &self.dispatcher).map_or(Ok(context), Err)
    }

    pub(crate) fn prepare<C>(&self)
    where
        C: IoContext,
    {
        if runtime_error(&self.runtime, &self.dispatcher).is_some() {
            return;
        }
        if self.contexts.borrow().contains_key(&TypeId::of::<C>()) {
            return;
        }
        let Some((mut context, allowed_roles)) = self.runtime.prepared::<C>(&self.thread) else {
            return;
        };
        match self.initialize_driver::<C>(&mut context, allowed_roles) {
            Ok((driver, role)) => self.insert_local::<C>(context, driver, role),
            Err(error) => {
                self.fail(error);
            }
        }
    }

    pub(crate) fn process_commands(&self) {
        while let Ok(command) = self.command_rx.try_recv() {
            command(self);
        }
    }

    fn initialize_everywhere<C>(&self) -> Result<(), Error>
    where
        C: IoContext,
    {
        if self.dispatcher.prepare_io_everywhere::<C>() {
            return Ok(());
        }
        if let Some(error) = runtime_error(&self.runtime, &self.dispatcher) {
            return Err(error);
        }
        Err(self.fail(DriverError::from_message(
            "runtime worker stopped before I/O registration was delivered",
        )))
    }

    fn wait_until_ready<C>(&self) -> Result<(), Error>
    where
        C: IoContext,
    {
        loop {
            if self.runtime.is_ready::<C>()? {
                return Ok(());
            }
            if self.dispatcher.is_shutting_down() {
                return Err(runtime_error(&self.runtime, &self.dispatcher).unwrap_or_else(Error::runtime_shutting_down));
            }
            match self.command_rx.recv_timeout(Duration::from_millis(1)) {
                Ok(command) => command(self),
                Err(error) if error.is_timeout() => {}
                Err(_) => {
                    return Err(runtime_error(&self.runtime, &self.dispatcher).unwrap_or_else(Error::runtime_shutting_down));
                }
            }
        }
    }

    fn initialize_driver<C>(
        &self,
        context: &mut C,
        allowed_roles: &[DriverRole],
    ) -> Result<(Box<dyn ErasedDriver>, DriverRole), DriverError>
    where
        C: IoContext,
    {
        let options = DriverOptions::new(self.thread.clone(), self.owner_waker.clone(), allowed_roles.to_vec());
        let DriverInstance { driver, role, .. } = context.driver(options).inspect_err(|error| {
            emit!(
                &self.sink,
                IoDriverInitializationFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(error),
                }
            );
        })?;
        if !allowed_roles.contains(&role) {
            let error = DriverError::from_message(format!("I/O driver selected disallowed role {role:?}"));
            emit!(
                &self.sink,
                IoDriverInitializationFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(&error),
                }
            );
            Self::shutdown_driver::<C>(Box::new(driver), &self.sink);
            return Err(error);
        }
        if let Err(error) = self.runtime.complete_driver::<C>(self.thread.id(), role) {
            emit!(
                &self.sink,
                IoDriverInitializationFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(&error),
                }
            );
            Self::shutdown_driver::<C>(Box::new(driver), &self.sink);
            return Err(error);
        }
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
        Ok((Box::new(driver), role))
    }

    fn insert_local<C>(&self, context: C, driver: Box<dyn ErasedDriver>, role: DriverRole)
    where
        C: IoContext,
    {
        assert!(
            self.contexts
                .borrow_mut()
                .insert(
                    TypeId::of::<C>(),
                    LocalContext {
                        context: Box::new(context),
                        driver,
                        role,
                        context_name: type_name::<C>(),
                    },
                )
                .is_none(),
            "worker-local I/O initialization cannot replace an existing context"
        );
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

    pub(crate) fn execute_cycle(&self, max_wait: Duration) -> Result<(), Error> {
        let mut contexts = self.contexts.borrow_mut();
        for local in contexts.values_mut().filter(|local| local.role == DriverRole::Secondary) {
            Self::execute_driver(local, Duration::ZERO, &self.sink).map_err(|error| self.fail(error))?;
        }
        if let Some(primary) = contexts.values_mut().find(|local| local.role == DriverRole::Primary) {
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
        fail_runtime(&self.runtime, &self.dispatcher, error)
    }

    pub(crate) fn shutdown(&self) {
        self.signal.clear_primary_waker();

        let mut primary = None;
        let mut secondaries = Vec::new();
        for (_, local) in self.contexts.borrow_mut().drain() {
            match local.role {
                DriverRole::Primary => {
                    assert!(
                        primary.replace((local.driver, local.context_name)).is_none(),
                        "a worker has at most one primary I/O driver"
                    );
                }
                DriverRole::Secondary => secondaries.push((local.driver, local.context_name)),
            }
        }

        for (driver, context_name) in secondaries.into_iter().chain(primary) {
            if let Err(error) = driver.shutdown() {
                emit!(
                    &self.sink,
                    IoDriverShutdownFailed {
                        context: IoContextName(context_name),
                        error: IoDriverErrorMessage::new(&error),
                    }
                );
                self.runtime.record_failure(error);
            }
        }
    }

    fn shutdown_driver<C>(driver: Box<dyn ErasedDriver>, sink: &Sink)
    where
        C: IoContext,
    {
        if let Err(error) = driver.shutdown() {
            emit!(
                sink,
                IoDriverShutdownFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(&error),
                }
            );
        }
    }
}
