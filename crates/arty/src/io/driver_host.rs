// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, TypeId, type_name};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;
use std::time::Duration;

use arty_io_core::{Cycle, Driver, DriverError, DriverInstance, DriverOptions, DriverRole, IoContext};
use foldhash::{HashMap, HashSet};
use observed::{Sink, emit};
use thread_aware::Thread;

use super::IoRuntime;
use super::runtime_state::Resolved;
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
        if let Some(mut local) = local {
            let result = if local.driver.is_some() {
                Ok(Self::clone_context::<C>(&local))
            } else {
                self.initialize_existing::<C>(&mut local)
            };
            if result.is_ok() {
                assert!(
                    self.contexts.borrow_mut().insert(context_type, local).is_none(),
                    "worker-local I/O initialization cannot replace an existing context"
                );
            }
            return result.map(|context| (context, false)).map_err(|error| self.fail(error));
        }

        let thread = self.thread.clone();
        let initialization_thread = thread.clone();
        let owner_waker = self.owner_waker.clone();
        let sink = self.sink.clone();
        let signal = performables::arc::Arc::clone(&self.signal);
        let failure_runtime = Arc::clone(&self.runtime);
        let failure_dispatcher = self.dispatcher.clone();
        let resolved = self.runtime.resolve::<C, _, Error>(&thread, move |allowed_roles| {
            if let Some(error) = Self::runtime_error(&failure_runtime, &failure_dispatcher) {
                return Err(error);
            }
            let mut context = C::context(options());
            context.relocate(None, &initialization_thread);
            let (driver, role) =
                Self::initialize_driver::<C>(&mut context, &allowed_roles, &initialization_thread, owner_waker, &signal, &sink)
                    .map_err(|error| Self::fail_runtime(&failure_runtime, &failure_dispatcher, error))?;
            Ok((context, role, driver))
        });

        match resolved {
            Ok(Resolved::Existing { context, role }) => {
                if let Some(error) = Self::runtime_error(&self.runtime, &self.dispatcher) {
                    return Err(error);
                }
                let mut local = LocalContext {
                    context: Box::new(context),
                    driver: None,
                    role,
                    context_name: type_name::<C>(),
                };
                let context = self.initialize_existing::<C>(&mut local).map_err(|error| self.fail(error))?;
                assert!(
                    self.contexts.borrow_mut().insert(context_type, local).is_none(),
                    "worker-local I/O initialization cannot replace an existing context"
                );
                Ok((context, false))
            }
            Ok(Resolved::Registered { context, role, driver }) => {
                assert!(
                    self.contexts
                        .borrow_mut()
                        .insert(
                            context_type,
                            LocalContext {
                                context: Box::new(context.clone()),
                                driver: Some(driver),
                                role,
                                context_name: type_name::<C>(),
                            },
                        )
                        .is_none(),
                    "worker-local I/O initialization cannot replace an existing context"
                );
                Ok((context, true))
            }
            Err(error) => Err(error),
        }
    }

    fn prepare<C>(&self)
    where
        C: IoContext,
    {
        if self.contexts.borrow().contains_key(&TypeId::of::<C>()) {
            return;
        }
        let Some((context, role)) = self.runtime.prepared::<C>(&self.thread) else {
            return;
        };
        self.contexts.borrow_mut().entry(TypeId::of::<C>()).or_insert_with(|| LocalContext {
            context: Box::new(context),
            driver: None,
            role,
            context_name: type_name::<C>(),
        });
    }

    fn initialize_existing<C>(&self, local: &mut LocalContext) -> Result<C, DriverError>
    where
        C: IoContext,
    {
        let context = local
            .context
            .downcast_mut::<C>()
            .expect("worker-local I/O contexts are stored under their own TypeId");
        let (driver, role) = Self::initialize_driver::<C>(
            context,
            &[local.role],
            &self.thread,
            self.owner_waker.clone(),
            &self.signal,
            &self.sink,
        )?;
        debug_assert_eq!(role, local.role);
        local.driver = Some(driver);
        Ok(context.clone())
    }

    fn initialize_driver<C>(
        context: &mut C,
        allowed_roles: &[DriverRole],
        thread: &Thread,
        owner_waker: Waker,
        signal: &performables::arc::Arc<WorkerSignal>,
        sink: &Sink,
    ) -> Result<(Box<dyn ErasedDriver>, DriverRole), DriverError>
    where
        C: IoContext,
    {
        let options = DriverOptions::new(thread.clone(), owner_waker, allowed_roles.to_vec());
        let DriverInstance { mut driver, role, .. } = context.driver(options).inspect_err(|error| {
            emit!(
                sink,
                IoDriverInitializationFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(error),
                }
            );
        })?;
        if !allowed_roles.contains(&role) {
            let error = DriverError::from_message(format!("I/O driver selected disallowed role {role:?}"));
            emit!(
                sink,
                IoDriverInitializationFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(&error),
                }
            );
            Self::shutdown_driver::<C>(Box::new(driver), sink);
            return Err(error);
        }
        if let Err(error) = Driver::execute_cycle(&mut driver, &mut Cycle::new(Duration::ZERO)) {
            emit!(
                sink,
                IoDriverInitializationFailed {
                    context: IoContextName(type_name::<C>()),
                    error: IoDriverErrorMessage::new(&error),
                }
            );
            Self::shutdown_driver::<C>(Box::new(driver), sink);
            return Err(error);
        }
        if role == DriverRole::Primary {
            signal.set_primary_waker(driver.waker());
        }
        DRIVERS_ACTIVE.set(true);
        emit!(
            sink,
            IoDriverInitialized {
                context: IoContextName(type_name::<C>()),
                role: IoDriverRoleLabel::from(role),
            }
        );
        Ok((Box::new(driver), role))
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
            Self::execute_driver(local, max_wait, &self.sink)?;
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
