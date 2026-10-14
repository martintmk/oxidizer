// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::any::{Any, TypeId};
use std::fmt;
use std::thread::ThreadId;

use arty_io_core::{DriverError, DriverRole, IoContext};
use foldhash::{HashMap, HashSet};
use performables::arc::Arc;
use performables::sync::condition::Condvar;
use performables::sync::mutex::{Mutex, MutexGuard};
use thread_aware::Thread;

use crate::runtime::{Error, WorkerSignal};

const PRIMARY_ROLE: &[DriverRole] = &[DriverRole::Primary];
const SECONDARY_ROLE: &[DriverRole] = &[DriverRole::Secondary];
const PRIMARY_OR_SECONDARY: &[DriverRole] = &[DriverRole::Primary, DriverRole::Secondary];

pub(crate) enum Resolution<'a, C> {
    Existing { context: C, allowed_roles: &'static [DriverRole] },
    Register(RegistrationReservation<'a>),
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
    role: Option<DriverRole>,
    initialized_workers: HashSet<ThreadId>,
    primary_candidate: bool,
    owner_signal: Arc<WorkerSignal>,
}

impl RegistrationState {
    fn prepare<C>(&self, destination: &Thread) -> (C, &'static [DriverRole])
    where
        C: IoContext,
    {
        let context = self
            .registration
            .clone_context(destination)
            .downcast::<C>()
            .expect("I/O contexts are stored under their own TypeId");
        (*context, allowed_roles(self.role, self.primary_candidate))
    }
}

#[derive(Default)]
struct Registry {
    registrations: HashMap<TypeId, RegistrationState>,
    primary: Option<TypeId>,
    constructing: HashMap<TypeId, ThreadId>,
    failure: Option<Arc<DriverError>>,
}

pub(crate) struct IoRuntime {
    worker_count: usize,
    registry: Mutex<Registry>,
    registration_changed: Condvar,
    #[cfg(test)]
    resolution_waiting: std::sync::atomic::AtomicBool,
}

pub(crate) struct RegistrationReservation<'a> {
    runtime: &'a IoRuntime,
    context_type: TypeId,
    primary_candidate: bool,
    published: bool,
}

impl RegistrationReservation<'_> {
    pub(crate) fn publish<C>(mut self, context: &C, thread: &Thread, owner_signal: Arc<WorkerSignal>) -> &'static [DriverRole]
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
                        role: None,
                        initialized_workers: HashSet::default(),
                        primary_candidate: self.primary_candidate,
                        owner_signal,
                    },
                )
                .is_none(),
            "an I/O context registration cannot replace an existing registration"
        );
        assert_eq!(
            registry.constructing.remove(&self.context_type),
            Some(thread.id()),
            "the publishing thread must own the I/O context construction reservation"
        );
        self.published = true;
        drop(registry);
        self.runtime.registration_changed.notify_all();
        allowed_roles(None, self.primary_candidate)
    }
}

impl Drop for RegistrationReservation<'_> {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let mut registry = self.runtime.registry();
        registry.constructing.remove(&self.context_type);
        if self.primary_candidate {
            assert_eq!(
                registry.primary.take(),
                Some(self.context_type),
                "the primary reservation belongs to the constructing context"
            );
        }
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
            #[cfg(test)]
            resolution_waiting: std::sync::atomic::AtomicBool::new(false),
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
        let current_thread = destination.id();
        let mut registry = self.registry();
        loop {
            if let Some(registration) = registry.registrations.get(&context_type) {
                let (context, allowed_roles) = registration.prepare(destination);
                return Resolution::Existing { context, allowed_roles };
            }
            if let Some(constructing_thread) = registry.constructing.get(&context_type) {
                if *constructing_thread == current_thread {
                    drop(registry);
                    Self::recursive_registration::<C>();
                }
                #[cfg(test)]
                self.report_resolution_waiting();
                registry = self.registration_changed.wait(registry);
                continue;
            }

            let primary_candidate = registry.primary.is_none();
            if primary_candidate {
                registry.primary = Some(context_type);
            }
            registry.constructing.insert(context_type, current_thread);
            return Resolution::Register(RegistrationReservation {
                runtime: self,
                context_type,
                primary_candidate,
                published: false,
            });
        }
    }

    pub(crate) fn prepared<C>(&self, destination: &Thread) -> Option<(C, &'static [DriverRole])>
    where
        C: IoContext,
    {
        let registry = self.registry();
        registry
            .registrations
            .get(&TypeId::of::<C>())
            .map(|registration| registration.prepare(destination))
    }

    pub(crate) fn complete_driver<C>(&self, worker: ThreadId, role: DriverRole) -> Result<(), DriverError>
    where
        C: IoContext,
    {
        let context_type = TypeId::of::<C>();
        let mut registry = self.registry();
        let (primary_candidate, owner_signal) = {
            let registration = registry
                .registrations
                .get_mut(&context_type)
                .expect("driver completion requires a published I/O registration");
            let primary_candidate = if let Some(expected) = registration.role {
                if role != expected {
                    return Err(DriverError::from_message(format!(
                        "I/O driver selected {role:?}, but the registration uses {expected:?}"
                    )));
                }
                false
            } else {
                registration.role = Some(role);
                registration.primary_candidate
            };
            assert!(
                registration.initialized_workers.insert(worker),
                "each worker initializes a context driver at most once"
            );
            (primary_candidate, Arc::clone(&registration.owner_signal))
        };
        if primary_candidate {
            assert_eq!(
                registry.primary,
                Some(context_type),
                "the primary reservation belongs to the completing context"
            );
            if role == DriverRole::Secondary {
                registry.primary = None;
            }
        }
        drop(registry);
        owner_signal.notify();
        Ok(())
    }

    pub(crate) fn is_ready<C>(&self) -> Result<bool, Error>
    where
        C: IoContext,
    {
        let context_type = TypeId::of::<C>();
        let registry = self.registry();
        if let Some(error) = &registry.failure {
            return Err(Error::io_failure(Arc::clone(error)));
        }
        let registration = registry
            .registrations
            .get(&context_type)
            .expect("a caller waits only after publishing an I/O registration");
        Ok(registration.initialized_workers.len() == self.worker_count)
    }

    pub(crate) fn record_failure(&self, error: DriverError) -> Arc<DriverError> {
        let mut registry = self.registry();
        let error = Arc::clone(registry.failure.get_or_insert_with(|| Arc::new(error)));
        drop(registry);
        self.registration_changed.notify_all();
        error
    }

    pub(crate) fn failure(&self) -> Option<Error> {
        self.registry().failure.as_ref().map(|error| Error::io_failure(Arc::clone(error)))
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

    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn report_resolution_waiting(&self) {
        self.resolution_waiting.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::task::Waker;
    use std::thread;

    use arty_io_core::{Cycle, Driver, DriverInstance, DriverOptions};
    use thread_aware::{ThreadAware, ThreadBuilder};

    use super::*;

    #[derive(Clone)]
    pub(crate) struct Context;
    #[derive(Clone)]
    struct OtherContext;

    impl ThreadAware for Context {
        fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
    }

    impl IoContext for Context {
        type ContextOptions = ();
        type Driver = TestDriver;

        fn context((): Self::ContextOptions) -> Self {
            Self
        }

        fn driver(&mut self, _options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError> {
            Ok(DriverInstance::new(TestDriver, DriverRole::Primary))
        }
    }

    impl ThreadAware for OtherContext {
        fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
    }

    impl IoContext for OtherContext {
        type ContextOptions = ();
        type Driver = TestDriver;

        fn context((): Self::ContextOptions) -> Self {
            Self
        }

        fn driver(&mut self, _options: DriverOptions) -> Result<DriverInstance<Self::Driver>, DriverError> {
            Ok(DriverInstance::new(TestDriver, DriverRole::Secondary))
        }
    }

    pub(crate) struct TestDriver;

    impl Driver for TestDriver {
        fn waker(&self) -> Waker {
            Waker::noop().clone()
        }

        fn execute_cycle(&mut self, _cycle: &mut Cycle) -> Result<(), DriverError> {
            Ok(())
        }

        fn shutdown(self) -> Result<(), DriverError> {
            Ok(())
        }
    }

    fn thread() -> Thread {
        ThreadBuilder::default().build(thread::current().id())
    }

    #[test]
    fn abandoned_reservation_releases_primary_role() {
        let runtime = IoRuntime::default();
        let destination = thread();
        let Resolution::Register(reservation) = runtime.resolve::<Context>(&destination) else {
            panic!("first resolution must reserve registration");
        };
        drop(reservation);

        let Resolution::Register(reservation) = runtime.resolve::<Context>(&destination) else {
            panic!("abandoned registration must be reservable again");
        };
        let allowed_roles = reservation.publish(&Context, &destination);
        assert_eq!(allowed_roles, [DriverRole::Primary, DriverRole::Secondary]);
    }

    #[test]
    fn abandoning_secondary_reservation_preserves_primary_registration() {
        let runtime = IoRuntime::default();
        let destination = thread();
        let Resolution::Register(reservation) = runtime.resolve::<Context>(&destination) else {
            panic!("first resolution must reserve registration");
        };
        reservation.publish(&Context, &destination);
        runtime
            .complete_driver::<Context>(thread::current().id(), DriverRole::Primary)
            .unwrap();

        let Resolution::Register(reservation) = runtime.resolve::<OtherContext>(&destination) else {
            panic!("second context must reserve registration");
        };
        assert!(!reservation.primary_candidate);
        drop(reservation);

        assert!(runtime.is_registered::<Context>());
    }

    #[test]
    fn completed_registration_accepts_the_published_role() {
        let runtime = IoRuntime::new(2);
        let destination = thread();
        let Resolution::Register(reservation) = runtime.resolve::<Context>(&destination) else {
            panic!("first resolution must reserve registration");
        };
        reservation.publish(&Context, &destination);
        runtime
            .complete_driver::<Context>(thread::current().id(), DriverRole::Primary)
            .unwrap();
        runtime
            .complete_driver::<Context>(
                std::thread::spawn(|| thread::current().id()).join().unwrap(),
                DriverRole::Primary,
            )
            .unwrap();

        assert!(runtime.is_ready::<Context>().unwrap());
    }

    #[test]
    fn concurrent_resolution_waits_for_publication() {
        let runtime = Arc::new(IoRuntime::default());
        let destination = thread();
        let Resolution::Register(reservation) = runtime.resolve::<Context>(&destination) else {
            panic!("first resolution must reserve registration");
        };
        let waiting_runtime = Arc::clone(&runtime);
        let waiting_destination = destination.clone();
        let waiter = std::thread::spawn(move || {
            matches!(
                waiting_runtime.resolve::<Context>(&waiting_destination),
                Resolution::Existing { .. }
            )
        });
        while !runtime.resolution_waiting.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::yield_now();
        }

        reservation.publish(&Context, &destination);

        assert!(waiter.join().unwrap());
    }

    #[test]
    fn poisoned_registry_is_recovered() {
        let runtime = IoRuntime::default();
        _ = catch_unwind(AssertUnwindSafe(|| {
            let _registry = runtime.registry.lock();
            panic!("poison registry");
        }));

        assert!(!runtime.is_registered::<Context>());
    }

    #[test]
    #[should_panic(expected = "at least one async worker")]
    fn zero_workers_are_rejected() {
        _ = IoRuntime::new(0);
    }

    #[test]
    fn debug_reports_worker_count() {
        assert!(format!("{:?}", IoRuntime::new(3)).contains("worker_count: 3"));
    }
}

fn allowed_roles(role: Option<DriverRole>, primary_candidate: bool) -> &'static [DriverRole] {
    match role {
        None if primary_candidate => PRIMARY_OR_SECONDARY,
        Some(DriverRole::Primary) => PRIMARY_ROLE,
        Some(DriverRole::Secondary) | None => SECONDARY_ROLE,
    }
}
