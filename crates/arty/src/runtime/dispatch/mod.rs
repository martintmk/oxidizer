// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Worker routing, submission capabilities, and stop coordination.

mod dispatcher_client;
mod dispatcher_core;
pub(crate) use dispatcher_client::DispatcherClient;
pub(crate) use dispatcher_core::WorkerIndex;
pub(super) use dispatcher_core::{DispatcherCore, WorkerEndpoint};

#[cfg(test)]
fn test_threads(count: usize) -> Vec<thread_aware::Thread> {
    let builder = thread_aware::ThreadBuilder::default();
    (0..count)
        .map(|_| {
            let builder = builder.clone();
            std::thread::spawn(move || builder.build(std::thread::current().id()))
                .join()
                .unwrap()
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn disconnected_test_dispatcher() -> (DispatcherClient, thread_aware::Thread) {
    use nonempty::NonEmpty;
    use performables::arc::Arc;
    use performables::sync::channel;

    use crate::runtime::blocking_worker::{BlockingPool, BlockingWorker};
    use crate::runtime::thread::waiter::ThreadWaiter;

    let thread = test_threads(1).remove(0);
    let endpoint = WorkerEndpoint {
        command_tx: channel::unbounded().0,
        io_command_tx: channel::unbounded().0,
        waker: std::task::Waker::noop().clone(),
        thread: thread.clone(),
        blocking_worker: BlockingWorker::new(BlockingPool::new(None), observed::Sink::noop()),
    };
    let dispatcher = Arc::new(DispatcherCore::new(
        ThreadWaiter::new(vec![]),
        NonEmpty::from_vec(vec![endpoint]).unwrap(),
        observed::Sink::noop(),
    ));
    (DispatcherClient::new(dispatcher), thread)
}
