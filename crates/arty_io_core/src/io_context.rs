// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use thread_aware_core::ThreadAware;

use crate::{DriverProvider, ProviderOptions};

/// A consumer-facing I/O handle associated with a driver provider.
///
/// The runtime registers each concrete context type once. On its first request, it creates a
/// driver/context pair on every active worker and completes each driver's zero-wait
/// initialization cycle before publishing the contexts. Later requests reuse the registration.
pub trait IoContext: Clone + ThreadAware + 'static {
    /// The provider used to create this context and its drivers.
    type Provider: DriverProvider<Context = Self>;

    /// Returns the provider for this context type.
    ///
    /// The runtime calls this at most once per registered context type, passing its facilities in
    /// `options`. Repeated calls should return equivalent providers.
    #[must_use]
    fn provider(options: ProviderOptions) -> Self::Provider;
}
