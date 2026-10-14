// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, TypeId};
use std::fmt;
use std::sync::Arc;
use std::thread::{self, ThreadId};

use arty_io_core::{DriverError, DriverRole, IoContext};
use foldhash::{HashMap, HashSet};
use performables::sync::condition::Condvar;
use performables::sync::mutex::{Mutex, MutexGuard};
use thread_aware::Thread;

use crate::runtime::Error;

pub(crate) enum Resolution<'a, C> {
    Existing {
        context: C,
        allowed_roles: Vec<DriverRole>,
    },
    Register {
        allowed_roles: Vec<DriverRole>,
        reservation: RegistrationReservation<'a>,
    },
}

trait ErasedRegistration: Send {
    fn clone_context(&self, destination: &Thread) -> Box<dyn Any>;
}

struct Registration<C> {
    context: C,
    thread: Thread,
}

impl<C> ErasedRegistration for Registration<C>
where
    C: IoContext,
{
    fn clone_context(&self, destination: &Thread) -> Box<dyn Any> {
        let mut context = self.context.clone();
        context.relocate(Some(&self.thread), destination);
        Box::new(context)
    }
}

struct RegistrationState {
    registration: Box<dyn ErasedRegistration>,
    allowed_roles: Vec<DriverRole>,
    role: Option<DriverRole>,
    initialized_workers: HashSet<ThreadId>,
    reserved_primary: bool,
}

#[derive(Default)]
struct Registry {
    registrations: HashMap<TypeId, RegistrationState>,
    primary: Option<TypeId>,
    primary_reservation: Option<TypeId>,
    constructing: HashMap<TypeId, ThreadId>,
}

pub(crate) struct IoRuntime {
    worker_count: usize,
    registry: Mutex<Registry>,
    registration_changed: Condvar,
    failure: Mutex<Option<Arc<DriverError>>>,
}

pub(crate) struct RegistrationReservation<'a> {
    runtime: &'a IoRuntime,
    context_type: TypeId,
    allowed_roles: Vec<DriverRole>,
    reserved_primary: bool,
    published: bool,
}

impl RegistrationReservation<'_> {
    pub(crate) fn publish<C>(mut self, context: &C, thread: &Thread)
    where
        C: IoContext,
    {
        let registration = Registration {
            context: context.clone(),
            thread: thread.clone(),
        };
        let mut registry = self.runtime.registry();
        assert!(
            registry
                .registrations
                .insert(
                    self.context_type,
                    RegistrationState {
                        registration: Box::new(registration),
                        allowed_roles: self.allowed_roles.clone(),
                        role: None,
                        initialized_workers: HashSet::default(),
                        reserved_primary: self.reserved_primary,
                    },
                )
                .is_none(),
            "an I/O context registration cannot replace an existing registration"
        );
        assert_eq!(
            registry.constructing.remove(&self.context_type),
            Some(thread::current().id()),
            "the publishing thread must own the I/O context construction reservation"
        );
        self.published = true;
        drop(registry);
        self.runtime.registration_changed.notify_all();
    }

    fn release(&self, registry: &mut Registry) {
        registry.constructing.remove(&self.context_type);
        if self.reserved_primary {
            assert_eq!(
                registry.primary_reservation.take(),
                Some(self.context_type),
                "the primary reservation belongs to the constructing context"
            );
        }
    }
}

impl Drop for RegistrationReservation<'_> {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let mut registry = self.runtime.registry();
        self.release(&mut registry);
        drop(registry);
        self.runtime.registration_changed.notify_all();
    }
}

impl fmt::Debug for IoRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoRuntime")
            .field("worker_count", &self.worker_count)
            .finish_non_exhaustive()
    }
}

impl Default for IoRuntime {
    fn default() -> Self {
        Self::new(1)
    }
}

impl IoRuntime {
    pub(crate) fn new(worker_count: usize) -> Self {
        assert!(worker_count > 0, "an I/O runtime requires at least one async worker");
        Self {
            worker_count,
            registry: Mutex::new(Registry::default()),
            registration_changed: Condvar::new(),
            failure: Mutex::new(None),
        }
    }

    pub(crate) fn is_registered<C>(&self) -> bool
    where
        C: IoContext,
    {
        self.registry().registrations.contains_key(&TypeId::of::<C>())
    }

    pub(crate) fn resolve<C>(&self, destination: &Thread) -> Resolution<'_, C>
    where
        C: IoContext,
    {
        let context_type = TypeId::of::<C>();
        let current_thread = thread::current().id();
        let mut registry = self.registry();
        loop {
            if let Some(registration) = registry.registrations.get(&context_type) {
                let context = registration
                    .registration
                    .clone_context(destination)
                    .downcast::<C>()
                    .expect("I/O contexts are stored under their own TypeId");
                let allowed_roles = registration
                    .role
                    .map_or_else(|| registration.allowed_roles.clone(), |role| vec![role]);
                return Resolution::Existing {
                    context: *context,
                    allowed_roles,
                };
            }
            if let Some(constructing_thread) = registry.constructing.get(&context_type) {
                if *constructing_thread == current_thread {
                    drop(registry);
                    Self::recursive_registration::<C>();
                }
                registry = self.registration_changed.wait(registry);
                continue;
            }

            let reserved_primary = registry.primary.is_none() && registry.primary_reservation.is_none();
            if reserved_primary {
                registry.primary_reservation = Some(context_type);
            }
            registry.constructing.insert(context_type, current_thread);
            let allowed_roles = if reserved_primary {
                vec![DriverRole::Primary, DriverRole::Secondary]
            } else {
                vec![DriverRole::Secondary]
            };
            return Resolution::Register {
                allowed_roles: allowed_roles.clone(),
                reservation: RegistrationReservation {
                    runtime: self,
                    context_type,
                    allowed_roles,
                    reserved_primary,
                    published: false,
                },
            };
        }
    }

    pub(crate) fn prepared<C>(&self, destination: &Thread) -> Option<(C, Vec<DriverRole>)>
    where
        C: IoContext,
    {
        let registry = self.registry();
        let registration = registry.registrations.get(&TypeId::of::<C>())?;
        let context = registration
            .registration
            .clone_context(destination)
            .downcast::<C>()
            .expect("I/O contexts are stored under their own TypeId");
        let allowed_roles = registration
            .role
            .map_or_else(|| registration.allowed_roles.clone(), |role| vec![role]);
        Some((*context, allowed_roles))
    }

    pub(crate) fn complete_driver<C>(&self, worker: ThreadId, role: DriverRole) -> Result<(), DriverError>
    where
        C: IoContext,
    {
        let context_type = TypeId::of::<C>();
        let mut registry = self.registry();
        let registration = registry
            .registrations
            .get_mut(&context_type)
            .expect("driver completion requires a published I/O registration");
        if let Some(expected) = registration.role {
            if role != expected {
                return Err(DriverError::from_message(format!(
                    "I/O driver selected {role:?}, but the registration uses {expected:?}"
                )));
            }
        } else {
            registration.role = Some(role);
            let reserved_primary = registration.reserved_primary;
            if reserved_primary {
                assert_eq!(
                    registry.primary_reservation.take(),
                    Some(context_type),
                    "the primary reservation belongs to the completing context"
                );
            }
            if role == DriverRole::Primary {
                assert!(reserved_primary, "only the reserved registration may select the primary role");
                assert!(
                    registry.primary.replace(context_type).is_none(),
                    "a runtime has at most one primary I/O context"
                );
            }
        }
        let registration = registry
            .registrations
            .get_mut(&context_type)
            .expect("the completed registration remains present");
        assert!(
            registration.initialized_workers.insert(worker),
            "each worker initializes a context driver at most once"
        );
        drop(registry);
        self.registration_changed.notify_all();
        Ok(())
    }

    pub(crate) fn is_ready<C>(&self) -> Result<bool, Error>
    where
        C: IoContext,
    {
        if let Some(error) = self.failure() {
            return Err(error);
        }
        let context_type = TypeId::of::<C>();
        let registry = self.registry();
        let registration = registry
            .registrations
            .get(&context_type)
            .expect("a caller waits only after publishing an I/O registration");
        Ok(registration.initialized_workers.len() == self.worker_count)
    }

    pub(crate) fn abort_registration<C>(&self)
    where
        C: IoContext,
    {
        let context_type = TypeId::of::<C>();
        let mut registry = self.registry();
        let Some(registration) = registry.registrations.remove(&context_type) else {
            return;
        };
        if registration.reserved_primary && registry.primary_reservation == Some(context_type) {
            registry.primary_reservation = None;
        }
        if registry.primary == Some(context_type) {
            registry.primary = None;
        }
        drop(registry);
        self.registration_changed.notify_all();
    }

    pub(crate) fn record_failure(&self, error: DriverError) -> Arc<DriverError> {
        let mut failure = self.failure.lock();
        let error = Arc::clone(failure.get_or_insert_with(|| Arc::new(error)));
        drop(failure);
        self.registration_changed.notify_all();
        error
    }

    pub(crate) fn failure(&self) -> Option<Error> {
        self.failure.lock().as_ref().map(|error| Error::io_failure(Arc::clone(error)))
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        match self.registry.lock_result() {
            Ok(registry) => registry,
            Err(poisoned) => {
                let registry = poisoned.into_inner();
                self.registry.clear_poison();
                registry
            }
        }
    }

    #[cold]
    #[inline(never)]
    #[expect(clippy::panic, reason = "recursive registration is a caller programming error")]
    fn recursive_registration<C>() -> !
    where
        C: IoContext,
    {
        panic!(
            "I/O context {} recursively requested its own registration",
            std::any::type_name::<C>()
        );
    }
}
