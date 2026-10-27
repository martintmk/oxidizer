// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::fmt::{self, Debug};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task;

use arty_executor::{ShutdownCause, TaskCompletion, TaskPanicPayload, discard_task_panic_payload};
use events_once::{EventLake, PooledReceiver, PooledSender};

enum TaskResult<R> {
    Completed(R),
    Panicked(TaskPanicPayload),
}

pub(crate) enum ReceiveError {
    Panicked(TaskPanicPayload),
    Disconnected,
}

impl<R> Debug for TaskResult<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(_) => f.write_str("Completed(..)"),
            Self::Panicked(_) => f.write_str("Panicked(..)"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Pool {
    events: EventLake,
}

impl Pool {
    pub(crate) fn new() -> Self {
        Self { events: EventLake::new() }
    }

    pub(crate) fn channel<R: Send + 'static>(&self) -> (Sender<R>, Receiver<R>) {
        let (tx, rx) = self.events.rent();
        (Sender { tx: Some(tx) }, Receiver { rx })
    }
}

pub(crate) struct Receiver<R: Send + 'static> {
    rx: PooledReceiver<TaskResult<R>>,
}

impl<R: Send + 'static> Future for Receiver<R> {
    type Output = Result<R, ReceiveError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().rx).poll(cx).map(|result| match result {
            Ok(TaskResult::Completed(value)) => Ok(value),
            Ok(TaskResult::Panicked(panic)) => Err(ReceiveError::Panicked(panic)),
            Err(_) => Err(ReceiveError::Disconnected),
        })
    }
}

impl<R: Send + 'static> Debug for Receiver<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

pub(crate) struct Sender<R: Send + 'static> {
    tx: Option<PooledSender<TaskResult<R>>>,
}

impl<R: Send + 'static> Sender<R> {
    fn publish(mut self, result: TaskResult<R>) {
        let tx = self.tx.take().expect("an Arty result sender publishes at most one result");
        discard_unwind(|| tx.send(result));
    }
}

impl<R: Send + 'static> Debug for Sender<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>()).finish_non_exhaustive()
    }
}

impl<R: Send + 'static> TaskCompletion<R> for Sender<R> {
    fn completed(self, result: R) {
        self.publish(TaskResult::Completed(result));
    }

    fn panicked(self, panic: TaskPanicPayload) {
        self.publish(TaskResult::Panicked(panic));
    }

    fn cancelled(self, _cause: ShutdownCause) {
        drop(self);
    }
}

impl<R: Send + 'static> Drop for Sender<R> {
    fn drop(&mut self) {
        let Some(tx) = self.tx.take() else {
            return;
        };
        discard_unwind(|| drop(tx));
    }
}

fn discard_unwind(action: impl FnOnce()) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(action)) {
        discard_task_panic_payload(panic);
    }
}
