// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/// Options for creating a [`MioContext`](crate::MioContext).
///
/// This type has no options in this release. Use [`MioOptions::default`] so
/// new options can be added without changing call sites.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct MioOptions;
