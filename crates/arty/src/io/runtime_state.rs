// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, TypeId};
use std::fmt;
use std::sync::Arc;
use std::thread::{self, ThreadId};

use arty_io_core::{DriverError, DriverRole, IoContext};
use foldhash::HashMap;
use performables::sync::condition::Condvar;
use performables::sync::mutex::{Mutex, MutexGuard};
use thread_aware::Thread;

use crate::runtime::Error;

pub(crate) enum Resolved<C, D> {
    Existing { context: C, role: DriverRole },
    Registered { context: C, role: DriverRole, driver: D },
}

trait ErasedRegistration: Send {
    fn clone_context(&self, destination: &Thread) -> Box<dyn Any>;
    fn role(&self) -> DriverRole;
}

struct Registration<C> {
    context: C,
    thread: Thread,
    role: DriverRole,
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

    fn role(&self) -> DriverRole {
        self.role
    }
}

#[derive(Default)]
struct Registry {
    registrations: HashMap<TypeId, Box<dyn ErasedRegistration>>,
    primary: Option<TypeId>,
    primary_reservation: Option<TypeId>,
    initializing: HashMap<TypeId, ThreadId>,
}

#[derive(Default)]
pub(crate) struct IoRuntime {
    registry: Mutex<Registry>,
    registration_completed: Condvar,
    failure: Mutex<Option<Arc<DriverError>>>,
}

enum ResolutionStart<'a, C> {
    Existing {
        context: C,
        role: DriverRole,
    },
    Register {
        allowed_roles: Vec<DriverRole>,
        reservation: RegistrationReservation<'a>,
    },
}

struct RegistrationReservation<'a> {
    runtime: &'a IoRuntime,
    context_type: TypeId,
    reserved_primary: bool,
    completed: bool,
}

impl RegistrationReservation<'_> {
    fn commit<C>(mut self, context: &C, thread: &Thread, role: DriverRole)
    where
        C: IoContext,
    {
        let registration = Registration {
            context: context.clone(),
            thread: thread.clone(),
            role,
        };
        let mut registry = self.runtime.registry();
        if role == DriverRole::Primary {
            assert!(
                self.reserved_primary,
                "only the primary reservation may initialize a primary driver"
            );
            assert!(
                registry.primary.replace(self.context_type).is_none(),
                "a runtime has at most one primary I/O context"
            );
        }
        registry.registrations.insert(self.context_type, Box::new(registration));
        self.release(&mut registry);
        self.completed = true;
        drop(registry);
        self.runtime.registration_completed.notify_all();
    }

    fn release(&self, registry: &mut Registry) {
        registry.initializing.remove(&self.context_type);
        if self.reserved_primary {
            assert_eq!(
                registry.primary_reservation.take(),
                Some(self.context_type),
                "the primary reservation belongs to the initializing context"
            );
        }
    }
}

impl Drop for RegistrationReservation<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut registry = self.runtime.registry();
        self.release(&mut registry);
        drop(registry);
        self.runtime.registration_completed.notify_all();
    }
}

impl fmt::Debug for IoRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoRuntime").finish_non_exhaustive()
    }
}

impl IoRuntime {
    pub(crate) fn is_registered<C>(&self) -> bool
    where
        C: IoContext,
    {
        self.registry().registrations.contains_key(&TypeId::of::<C>())
    }

    pub(crate) fn resolve<C, D, E>(
        &self,
        destination: &Thread,
        register: impl FnOnce(Vec<DriverRole>) -> Result<(C, DriverRole, D), E>,
    ) -> Result<Resolved<C, D>, E>
    where
        C: IoContext,
    {
        match self.begin_resolution::<C>(destination) {
            ResolutionStart::Existing { context, role } => Ok(Resolved::Existing { context, role }),
            ResolutionStart::Register {
                allowed_roles,
                reservation,
            } => {
                let (context, role, driver) = register(allowed_roles)?;
                reservation.commit(&context, destination, role);
                Ok(Resolved::Registered { context, role, driver })
            }
        }
    }

    pub(crate) fn prepared<C>(&self, destination: &Thread) -> Option<(C, DriverRole)>
    where
        C: IoContext,
    {
        let registry = self.registry();
        let registration = registry.registrations.get(&TypeId::of::<C>())?;
        let role = registration.role();
        let context = registration
            .clone_context(destination)
            .downcast::<C>()
            .expect("I/O contexts are stored under their own TypeId");
        Some((*context, role))
    }

    pub(crate) fn record_failure(&self, error: DriverError) -> Arc<DriverError> {
        let mut failure = self.failure.lock();
        Arc::clone(failure.get_or_insert_with(|| Arc::new(error)))
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

    fn begin_resolution<C>(&self, destination: &Thread) -> ResolutionStart<'_, C>
    where
        C: IoContext,
    {
        let context_type = TypeId::of::<C>();
        let current_thread = thread::current().id();
        let mut registry = self.registry();
        loop {
            if let Some(registration) = registry.registrations.get(&context_type) {
                let role = registration.role();
                let context = registration
                    .clone_context(destination)
                    .downcast::<C>()
                    .expect("I/O contexts are stored under their own TypeId");
                return ResolutionStart::Existing { context: *context, role };
            }
            if let Some(initializing_thread) = registry.initializing.get(&context_type) {
                if *initializing_thread == current_thread {
                    drop(registry);
                    Self::recursive_registration::<C>();
                }
                registry = self.registration_completed.wait(registry);
                continue;
            }

            let reserved_primary = registry.primary.is_none() && registry.primary_reservation.is_none();
            if reserved_primary {
                registry.primary_reservation = Some(context_type);
            }
            registry.initializing.insert(context_type, current_thread);
            let allowed_roles = if reserved_primary {
                vec![DriverRole::Primary, DriverRole::Secondary]
            } else {
                vec![DriverRole::Secondary]
            };
            return ResolutionStart::Register {
                allowed_roles,
                reservation: RegistrationReservation {
                    runtime: self,
                    context_type,
                    reserved_primary,
                    completed: false,
                },
            };
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
