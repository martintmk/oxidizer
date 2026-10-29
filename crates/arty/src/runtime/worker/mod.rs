// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Async and blocking worker execution.

pub(crate) mod blocking;
pub(crate) mod task;

mod local_worker;

pub(crate) use local_worker::LocalWorker;
