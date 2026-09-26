// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/// A driver's fixed waiting role on its runtime worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverRole {
    /// The driver allowed to block the runtime worker.
    ///
    /// A worker has at most one primary, assigned only when
    /// [`DriverProvider::CAN_BE_PRIMARY`](crate::DriverProvider::CAN_BE_PRIMARY) is `true`.
    /// It runs after all secondaries and may wait when [`Cycle::can_block`](crate::Cycle::can_block)
    /// permits it.
    Primary,
    /// A driver whose worker-local cycle must not block.
    ///
    /// Runs before the primary with [`Cycle::can_block`](crate::Cycle::can_block) set to `false`.
    /// It may apply [`Cycle::max_wait`](crate::Cycle::max_wait) only to background waits
    /// registered through [`Cycle::start_work`](crate::Cycle::start_work).
    ///
    /// Indefinite waits need independent execution capacity. A bounded system-task pool is
    /// suitable only if it has capacity for every simultaneously blocked observer.
    Secondary,
}
