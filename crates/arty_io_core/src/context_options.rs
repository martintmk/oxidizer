// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/// Options supplied by a runtime when creating an I/O context.
///
/// No runtime facilities are currently exposed.
#[derive(Debug)]
#[non_exhaustive]
pub struct ContextOptions;

#[expect(
    clippy::new_without_default,
    reason = "context options intentionally require explicit construction"
)]
impl ContextOptions {
    /// Creates an empty set of context options.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}
