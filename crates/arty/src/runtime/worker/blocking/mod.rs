// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Blocking worker admission and pool execution.

mod local_worker;
mod panic_policy;
mod pools;
mod worker;

pub(crate) use local_worker::LocalWorker;
pub(crate) use panic_policy::PanicPolicy;
pub(crate) use pools::Pools;
#[cfg(test)]
pub(crate) use worker::blocking_worker_tests;
pub(crate) use worker::{Pool, Worker};

pub(crate) type PoolIdentity = performables::arc::Arc<()>;
