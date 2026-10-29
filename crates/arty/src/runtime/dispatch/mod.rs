// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Worker routing, submission capabilities, and stop coordination.

mod client;
mod core;
pub(crate) use core::WorkerIndex;
pub(super) use core::{DispatcherCore, WorkerEndpoint};

pub(crate) use client::DispatcherClient;

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
