// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Async and blocking worker execution.

pub(crate) mod asynchronous;
pub(crate) mod blocking;

mod worker_kind;

pub(crate) use worker_kind::WorkerKind;
