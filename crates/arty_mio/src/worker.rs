// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Waker;
use std::thread::ThreadId;

use mio::event::Event;
use mio::{Registry, Token};

use crate::registration::RegistrationEntry;

pub(crate) const DRIVER_WAKE_TOKEN: Token = Token(usize::MAX);

#[derive(Default)]
pub(crate) struct ContextState {
    workers: Mutex<HashMap<ThreadId, Arc<WorkerState>>>,
}

impl ContextState {
    pub(crate) fn worker(&self, thread_id: ThreadId) -> Option<Arc<WorkerState>> {
        self.workers
            .lock()
            .expect("worker lookup does not call user code while the worker map lock is held")
            .get(&thread_id)
            .cloned()
    }

    pub(crate) fn try_worker(&self, thread_id: ThreadId) -> Option<Arc<WorkerState>> {
        self.workers.try_lock().ok()?.get(&thread_id).cloned()
    }

    pub(crate) fn install(&self, worker: Arc<WorkerState>) -> io::Result<()> {
        let mut workers = self
            .workers
            .lock()
            .expect("driver creation is the only code that mutates the worker map while its lock is held");
        if workers.contains_key(&worker.thread_id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the current thread already has an Arty Mio driver",
            ));
        }
        workers.insert(worker.thread_id, worker);
        Ok(())
    }

    pub(crate) fn remove(&self, worker: &Arc<WorkerState>) {
        let mut workers = self
            .workers
            .lock()
            .expect("driver shutdown is the only code that mutates the worker map while its lock is held");
        if workers.get(&worker.thread_id).is_some_and(|current| Arc::ptr_eq(current, worker)) {
            workers.remove(&worker.thread_id);
        }
    }
}

pub(crate) struct WorkerState {
    pub(crate) thread_id: ThreadId,
    owner_waker: Waker,
    next_token: AtomicUsize,
    registry: Registry,
    registrations: Mutex<Registrations>,
    registrations_idle: Condvar,
    failure: Mutex<Option<io::Error>>,
}

#[derive(Default)]
struct Registrations {
    closed: bool,
    active_operations: usize,
    values: HashMap<Token, Arc<RegistrationEntry>>,
}

impl WorkerState {
    pub(crate) fn new(thread_id: ThreadId, registry: Registry, owner_waker: Waker) -> Self {
        Self {
            thread_id,
            owner_waker,
            next_token: AtomicUsize::new(0),
            registry,
            registrations: Mutex::new(Registrations::default()),
            registrations_idle: Condvar::new(),
            failure: Mutex::new(None),
        }
    }

    pub(crate) const fn registry(&self) -> &Registry {
        &self.registry
    }

    pub(crate) fn allocate_token(&self) -> io::Result<Token> {
        self.next_token
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
                (token < usize::MAX - 1).then_some(token + 1)
            })
            .map(Token)
            .map_err(|_exhausted| io::Error::other("the Arty Mio token space is exhausted"))
    }

    pub(crate) fn begin_registration(&self, token: Token, entry: Arc<RegistrationEntry>) -> io::Result<()> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("registration insertion does not call user code while the registration lock is held");
        if registrations.closed {
            return Err(closed_error());
        }
        registrations.active_operations += 1;
        registrations.values.insert(token, entry);
        Ok(())
    }

    pub(crate) fn finish_registration(&self, token: Token, entry: &Arc<RegistrationEntry>, registered: bool) -> bool {
        let mut registrations = self
            .registrations
            .lock()
            .expect("registration completion does not call user code while the registration lock is held");
        let active =
            registered && !registrations.closed && registrations.values.get(&token).is_some_and(|current| Arc::ptr_eq(current, entry));
        if !active {
            registrations.values.remove(&token);
        }
        active
    }

    pub(crate) fn begin_deregistration(&self, token: Token, entry: &Arc<RegistrationEntry>) -> bool {
        let mut registrations = self
            .registrations
            .lock()
            .expect("deregistration removal does not call user code while the registration lock is held");
        if registrations.closed || !registrations.values.get(&token).is_some_and(|current| Arc::ptr_eq(current, entry)) {
            return false;
        }
        registrations.active_operations += 1;
        registrations.values.remove(&token);
        true
    }

    pub(crate) fn finish_registry_operation(&self) {
        let mut registrations = self
            .registrations
            .lock()
            .expect("deregistration completion does not call user code while the registration lock is held");
        registrations.active_operations -= 1;
        if registrations.active_operations == 0 {
            self.registrations_idle.notify_all();
        }
    }

    pub(crate) fn dispatch_batch(&self, events: &[Event], resolved: &mut Vec<Option<Arc<RegistrationEntry>>>) {
        resolved.clear();
        if events.is_empty() {
            return;
        }
        let registrations = self
            .registrations
            .lock()
            .expect("event batch resolution does not call user code while the registration lock is held");
        resolved.extend(events.iter().map(|event| registrations.values.get(&event.token()).cloned()));
        drop(registrations);
        for (event, registration) in events.iter().zip(resolved.iter()) {
            if let Some(registration) = registration {
                registration.notify(event);
            }
        }
    }

    pub(crate) fn close_registrations(&self) {
        let registrations: Vec<_> = {
            let mut registrations = self
                .registrations
                .lock()
                .expect("driver shutdown does not call user code while the registration lock is held");
            registrations.closed = true;
            while registrations.active_operations != 0 {
                registrations = self
                    .registrations_idle
                    .wait(registrations)
                    .expect("only driver shutdown waits for active registry operations");
            }
            registrations.values.drain().map(|(_, registration)| registration).collect()
        };
        for registration in registrations {
            registration.close();
        }
    }

    #[cfg(test)]
    pub(crate) fn registrations_are_closed(&self) -> bool {
        self.registrations
            .lock()
            .expect("test inspection does not call user code while the registration lock is held")
            .closed
    }

    pub(crate) fn wake_owner(&self) {
        self.owner_waker.wake_by_ref();
    }

    pub(crate) fn record_failure(&self, error: io::Error) {
        let mut failure = self
            .failure
            .lock()
            .expect("driver failure recording does not call user code while the failure lock is held");
        if failure.is_none() {
            *failure = Some(error);
            drop(failure);
            self.owner_waker.wake_by_ref();
        }
    }

    pub(crate) fn take_failure(&self) -> Option<io::Error> {
        self.failure
            .lock()
            .expect("driver failure inspection does not call user code while the failure lock is held")
            .take()
    }
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "the Arty Mio driver is closed")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::atomic::Ordering;
    use std::task::Waker;

    use mio::{Poll, Token};

    use super::WorkerState;

    #[test]
    fn token_allocation_stops_before_the_reserved_waker_token() {
        let poll = Poll::new().unwrap();
        let worker = WorkerState::new(
            std::thread::current().id(),
            poll.registry().try_clone().unwrap(),
            Waker::noop().clone(),
        );
        worker.next_token.store(usize::MAX - 2, Ordering::Relaxed);

        assert_eq!(worker.allocate_token().unwrap(), Token(usize::MAX - 2));
        assert_eq!(worker.allocate_token().unwrap_err().kind(), std::io::ErrorKind::Other);
    }
}
