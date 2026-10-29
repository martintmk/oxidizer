// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Runtime worker OS-thread lifecycle and joining.

mod lifecycle;
mod waiter;

pub(super) use lifecycle::spawn;
pub(crate) use waiter::ThreadWaiter;
