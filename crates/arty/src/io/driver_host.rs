// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, TypeId, type_name};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use arty_io_core::{Cycle, Driver, DriverError, DriverInstance, DriverOptions, DriverRole, IoContext};
use foldhash::{HashMap, HashSet};
use observed::{Sink, emit};
use performables::sync::channel;
use thread_aware::Thread;

use super::runtime_state::Resolution;
use super::{DriverCommand, IoRuntime};
use crate::runtime::dispatch::DispatcherClient;
use crate::runtime::telemetry::events::{
    IoContextName, IoDriverCycleFailed, IoDriverErrorMessage, IoDriverInitializationFailed, IoDriverInitialized, IoDriverRoleLabel,
    IoDriverShutdownFailed,
};
use crate::runtime::{Error, WorkerSignal};

thread_local! {
    static CURRENT: RefCell<Option<Rc<DriverHost>>> = const { RefCell::new(None) };
    static DRIVERS_ACTIVE: Cell<bool> = const { Cell::new(false) };
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
    driver: Option<Box<dyn ErasedDriver>>,
    role: DriverRole,
    context_name: &'static str,
}

pub(crate) struct DriverHost {
    runtime: Arc<IoRuntime>,
    dispatcher: DispatcherClient,
    thread: Thread,
    owner_waker: Waker,
    signal: performables::arc::Arc<WorkerSignal>,
    command_rx: channel::Receiver<DriverCommand>,
    sink: Sink,
    contexts: RefCell<HashMap<TypeId, LocalContext>>,
    initializing: RefCell<HashSet<TypeId>>,
}

impl DriverHost {
    pub(crate) fn install(
        runtime: Arc<IoRuntime>,
        dispatcher: DispatcherClient,
        thread: Thread,
        signal: performables::arc::Arc<WorkerSignal>,
        command_rx: channel::Receiver<DriverCommand>,
        sink: Sink,
    ) {
        let owner_waker = WorkerSignal::waker(&signal);
        CURRENT.with_borrow_mut(|current| {
            assert!(current.is_none(), "an I/O driver host is already installed on this worker");
            *current = Some(Rc::new(Self {
                runtime,
                dispatcher,
                thread,
                owner_waker,
                signal,
                command_rx,
                sink,
                contexts: RefCell::new(HashMap::default()),
                initializing: RefCell::new(HashSet::default()),
            }));
        });
    }

    pub(crate) fn try_context_current<C>(
        runtime: &Arc<IoRuntime>,
        thread: &Thread,
        options: impl FnOnce() -> C::ContextOptions,
    ) -> Option<Result<(C, bool), Error>>
    where
        C: IoContext,
        C::ContextOptions: Clone,
    {
        CURRENT.with_borrow(|current| {
            let host = current.as_ref()?;
            if !Arc::ptr_eq(&host.runtime, runtime) || host.thread != *thread {
                return None;
            }
            Some(host.try_context(options))
        })
    }

    pub(crate) fn is_current(runtime: &Arc<IoRuntime>, thread: &Thread) -> bool {
        CURRENT.with_borrow(|current| {
            current
                .as_ref()
                .is_some_and(|host| Arc::ptr_eq(&host.runtime, runtime) && host.thread == *thread)
        })
    }

    pub(crate) fn prepare_current<C>(runtime: &Arc<IoRuntime>, thread: &Thread)
    where
        C: IoContext,
    {
        CURRENT.with_borrow(|current| {
            let Some(host) = current.as_ref() else {
                return;
            };
            if !Arc::ptr_eq(&host.runtime, runtime) || host.thread != *thread {
                return;
            }
            host.prepare::<C>();
        });
    }

    pub(crate) fn process_current_commands() {
        CURRENT.with_borrow(|current| {
            if let Some(host) = current {
                host.process_commands();
            }
        });
    }

    pub(crate) fn wait_until_ready_current<C>(runtime: &Arc<IoRuntime>, thread: &Thread) -> Result<(), Error>
    where
        C: IoContext,
    {
        CURRENT.with_borrow(|current| {
            let host = current.as_ref().ok_or_else(Error::io_outside_worker)?;
            if !Arc::ptr_eq(&host.runtime, runtime) || host.thread != *thread {
                return Err(Error::io_outside_worker());
            }
            host.wait_until_ready::<C>()
        })
    }

    pub(crate) fn execute_current(max_wait: Duration) -> Result<(), ()> {
        CURRENT.with_borrow(|current| {
            let Some(host) = current.as_ref() else {
                return Ok(());
            };
            host.execute_cycle(max_wait).map_err(|error| {
                host.fail(error);
            })
        })
    }

    pub(crate) fn drivers_active() -> bool {
        DRIVERS_ACTIVE.get()
    }

    pub(crate) fn shutdown_current() {
        let host = CURRENT.with_borrow_mut(Option::take);
        if let Some(host) = host {
            host.shutdown();
        }
    }

    fn try_context<C>(&self, options: impl FnOnce() -> C::ContextOptions) -> Result<(C, bool), Error>
    where
        C: IoContext,
        C::ContextOptions: Clone,
    {
        if let Some(error) = Self::runtime_error(&self.runtime, &self.dispatcher) {
            return Err(error);
        }
        let context_type = TypeId::of::<C>();
        assert!(
            self.initializing.borrow_mut().insert(context_type),
            "I/O context recursively requested its own registration on one worker"
        );
        let _initialization = LocalInitialization {
            initializing: &self.initializing,
            context_type,
        };
        let local = self.contexts.borrow_mut().remove(&context_type);
        if let Some(local) = local {
            let context = Self::clone_context::<C>(&local);
            assert!(
                self.contexts.borrow_mut().insert(context_type, local).is_none(),
                "worker-local I/O initialization cannot replace an existing context"
            );
            return Ok((context, false));
        }

        match self.runtime.resolve::<C>(&self.thread) {
            Resolution::Existing {
                mut context,
                allowed_roles,
            } => {
                let (driver, role) = self
                    .initialize_driver::<C>(&mut context, &allowed_roles)
                    .map_err(|error| self.fail(error))?;
                self.insert_local::<C>(context.clone(), driver, role);
                Ok((context, false))
            }
            Resolution::Register {
                allowed_roles,
                reservation,
            } => {
                let mut context = C::context(options());
                context.relocate(None, &self.thread);
                reservation.publish(&context, &self.thread);
                let initialization = self.initialize_driver::<C>(&mut context, &allowed_roles);
                let (driver, role) = match initialization {
                    Ok(initialization) => initialization,
                    Err(error) => return Err(self.fail(error)),
                };
                self.insert_local::<C>(context.clone(), driver, role);
                Ok((context, true))
            }
        }
    }

    fn prepare<C>(&self)
    where
        C: IoContext,
    {
        if Self::runtime_error(&self.runtime, &self.dispatcher).is_some() {
            return;
        }
        if self.contexts.borrow().contains_key(&TypeId::of::<C>()) {
            return;
        }
        let Some((mut context, allowed_roles)) = self.runtime.prepared::<C>(&self.thread) else {
            return;
        };
        match self.initialize_driver::<C>(&mut context, &allowed_roles) {
            Ok((driver, role)) => self.insert_local::<C>(context, driver, role),
            Err(error) => {
                self.fail(error);
            }
        }
    }

    fn process_commands(&self) {
        while let Ok(command) = self.command_rx.try_recv() {
            command();
        }
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
                return Err(self.runtime.failure().unwrap_or_else(Error::runtime_shutting_down));
            }
            match self.command_rx.recv_timeout(Duration::from_millis(1)) {
                Ok(command) => command(),
                Err(error) if error.is_timeout() => {}
                Err(_) => return Err(self.runtime.failure().unwrap_or_else(Error::runtime_shutting_down)),
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
        if let Err(error) = self.runtime.complete_driver::<C>(thread::current().id(), role) {
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
        DRIVERS_ACTIVE.set(true);
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
                        driver: Some(driver),
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

    fn execute_cycle(&self, max_wait: Duration) -> Result<(), DriverError> {
        let mut contexts = self.contexts.borrow_mut();
        for local in contexts.values_mut().filter(|local| local.role == DriverRole::Secondary) {
            Self::execute_driver(local, Duration::ZERO, &self.sink)?;
        }
        if let Some(primary) = contexts.values_mut().find(|local| local.role == DriverRole::Primary) {
            Self::execute_driver(primary, max_wait, &self.sink)?;
        }
        Ok(())
    }

    fn execute_driver(local: &mut LocalContext, max_wait: Duration, sink: &Sink) -> Result<(), DriverError> {
        let Some(driver) = local.driver.as_mut() else {
            return Ok(());
        };
        driver.execute_cycle(&mut Cycle::new(max_wait)).inspect_err(|error| {
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
        Self::fail_runtime(&self.runtime, &self.dispatcher, error)
    }

    fn fail_runtime(runtime: &IoRuntime, dispatcher: &DispatcherClient, error: DriverError) -> Error {
        let error = runtime.record_failure(error);
        dispatcher.stop();
        Error::io_failure(error)
    }

    fn runtime_error(runtime: &IoRuntime, dispatcher: &DispatcherClient) -> Option<Error> {
        runtime
            .failure()
            .or_else(|| dispatcher.is_shutting_down().then(Error::runtime_shutting_down))
    }

    fn shutdown(&self) {
        DRIVERS_ACTIVE.set(false);

        let mut primary = None;
        let mut secondaries = Vec::new();
        for (_, mut local) in self.contexts.borrow_mut().drain() {
            let Some(driver) = local.driver.take() else {
                continue;
            };
            match local.role {
                DriverRole::Primary => {
                    assert!(
                        primary.replace((driver, local.context_name)).is_none(),
                        "a worker has at most one primary I/O driver"
                    );
                }
                DriverRole::Secondary => secondaries.push((driver, local.context_name)),
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
