// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread::ThreadId;

use mio::event::Event;
use mio::{Registry, Token};

use crate::driver::PollWake;
use crate::registration::RegistrationEntry;

pub(crate) const DRIVER_WAKE_TOKEN: Token = Token(usize::MAX);
const MAX_COMMAND_QUEUE: usize = 1_024;

pub(crate) trait DriverCommand: Send {
    fn execute(self: Box<Self>, registry: &Registry, worker: &Arc<WorkerState>);
    fn cancel(self: Box<Self>);
}

pub(crate) trait ControlCommand: Send {
    fn execute(self: Box<Self>, registry: &Registry, worker: &Arc<WorkerState>);
    fn cancel(self: Box<Self>);
}

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
    poll_waker: Arc<PollWake>,
    owner_waker: Waker,
    next_token: AtomicUsize,
    commands: Mutex<CommandQueue>,
    registrations: Mutex<Registrations>,
    failure: Mutex<Option<io::Error>>,
}

#[derive(Default)]
struct CommandQueue {
    closed: bool,
    values: VecDeque<Box<dyn DriverCommand>>,
    controls_closed: bool,
    controls: VecDeque<Box<dyn ControlCommand>>,
}

#[derive(Default)]
struct Registrations {
    closed: bool,
    values: HashMap<Token, Arc<RegistrationEntry>>,
}

impl WorkerState {
    pub(crate) fn new(thread_id: ThreadId, poll_waker: Arc<PollWake>, owner_waker: Waker) -> Self {
        Self {
            thread_id,
            poll_waker,
            owner_waker,
            next_token: AtomicUsize::new(0),
            commands: Mutex::new(CommandQueue::default()),
            registrations: Mutex::new(Registrations::default()),
            failure: Mutex::new(None),
        }
    }

    pub(crate) fn enqueue(&self, command: Box<dyn DriverCommand>) -> io::Result<()> {
        {
            let mut commands = self
                .commands
                .lock()
                .expect("command submission does not call user code while the command queue lock is held");
            if commands.closed {
                return Err(closed_error());
            }
            if commands.values.len() == MAX_COMMAND_QUEUE {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "the Arty Mio registration queue is full"));
            }
            commands.values.push_back(command);
        }
        self.poll_waker.wake_poll();
        self.owner_waker.wake_by_ref();
        Ok(())
    }

    pub(crate) fn enqueue_control(&self, command: Box<dyn ControlCommand>) {
        let command = {
            let mut commands = self
                .commands
                .lock()
                .expect("control submission does not call user code while the command queue lock is held");
            if commands.controls_closed {
                Some(command)
            } else {
                commands.controls.push_back(command);
                None
            }
        };
        if let Some(command) = command {
            command.cancel();
        } else {
            self.poll_waker.wake_poll();
        }
    }

    pub(crate) fn take_controls(&self) -> Vec<Box<dyn ControlCommand>> {
        self.commands
            .lock()
            .expect("control draining does not call user code while the command queue lock is held")
            .controls
            .drain(..)
            .collect()
    }

    pub(crate) fn seal_controls(&self) -> Vec<Box<dyn ControlCommand>> {
        let mut commands = self
            .commands
            .lock()
            .expect("control sealing does not call user code while the command queue lock is held");
        commands.controls_closed = true;
        commands.controls.drain(..).collect()
    }

    pub(crate) fn take_commands(&self, limit: usize) -> Vec<Box<dyn DriverCommand>> {
        let mut commands = self
            .commands
            .lock()
            .expect("command draining does not call user code while the command queue lock is held");
        let count = limit.min(commands.values.len());
        commands.values.drain(..count).collect()
    }

    pub(crate) fn has_commands(&self) -> bool {
        !self
            .commands
            .lock()
            .expect("command inspection does not call user code while the command queue lock is held")
            .values
            .is_empty()
    }

    pub(crate) fn allocate_token(&self) -> io::Result<Token> {
        self.next_token
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
                (token < usize::MAX - 1).then_some(token + 1)
            })
            .map(Token)
            .map_err(|_exhausted| io::Error::other("the Arty Mio token space is exhausted"))
    }

    pub(crate) fn add_registration(&self, token: Token, entry: Arc<RegistrationEntry>) -> io::Result<()> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("registration insertion does not call user code while the registration lock is held");
        if registrations.closed {
            return Err(closed_error());
        }
        registrations.values.insert(token, entry);
        Ok(())
    }

    pub(crate) fn registration_is_active(&self, token: Token, entry: &Arc<RegistrationEntry>) -> bool {
        let registrations = self
            .registrations
            .lock()
            .expect("registration inspection does not call user code while the registration lock is held");
        !registrations.closed && registrations.values.get(&token).is_some_and(|current| Arc::ptr_eq(current, entry))
    }

    pub(crate) fn remove_registration(&self, token: Token) {
        self.registrations
            .lock()
            .expect("registration removal does not call user code while the registration lock is held")
            .values
            .remove(&token);
    }

    pub(crate) fn dispatch(&self, event: &Event) {
        let registration = self
            .registrations
            .lock()
            .expect("event dispatch does not call user code while the registration lock is held")
            .values
            .get(&event.token())
            .cloned();
        if let Some(registration) = registration {
            registration.notify(event);
        }
    }

    pub(crate) fn begin_close(&self) {
        let commands = {
            let mut commands = self
                .commands
                .lock()
                .expect("driver shutdown does not call user code while the command queue lock is held");
            commands.closed = true;
            std::mem::take(&mut commands.values)
        };
        for command in commands {
            command.cancel();
        }
    }

    pub(crate) fn close_registrations(&self) {
        let registrations: Vec<_> = {
            let mut registrations = self
                .registrations
                .lock()
                .expect("driver shutdown does not call user code while the registration lock is held");
            registrations.closed = true;
            registrations.values.values().cloned().collect()
        };
        for registration in registrations {
            registration.close();
        }
    }

    pub(crate) fn drain_registrations(&self) -> Vec<Arc<RegistrationEntry>> {
        self.registrations
            .lock()
            .expect("native registration draining does not call user code while the registration lock is held")
            .values
            .drain()
            .map(|(_, registration)| registration)
            .collect()
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
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::Waker;

    use mio::{Poll, Token};

    use super::WorkerState;
    use crate::driver::PollWake;

    #[test]
    fn token_allocation_stops_before_the_reserved_waker_token() {
        let poll = Poll::new().unwrap();
        let poll_waker = Arc::new(PollWake::new(poll.registry()).unwrap());
        let worker = WorkerState::new(std::thread::current().id(), poll_waker, Waker::noop().clone());
        worker.next_token.store(usize::MAX - 2, Ordering::Relaxed);

        assert_eq!(worker.allocate_token().unwrap(), Token(usize::MAX - 2));
        assert_eq!(worker.allocate_token().unwrap_err().kind(), std::io::ErrorKind::Other);
    }
}
