// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/// Selects how the executor responds to a contained task panic.
#[derive(Clone, Debug, Default)]
pub enum PanicPolicy {
    /// Contain the failed task and continue running other tasks.
    #[default]
    Ignore,

    /// Start executor shutdown with a panic cause.
    Shutdown,
}

impl PanicPolicy {
    /// Creates a policy that contains task panics without stopping the executor.
    #[must_use]
    pub const fn ignore() -> Self {
        Self::Ignore
    }

    /// Creates a policy that starts executor shutdown after a task panic.
    #[must_use]
    pub const fn shutdown() -> Self {
        Self::Shutdown
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn starts_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}
