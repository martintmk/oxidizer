// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use arty::core::{Thread, ThreadAware};
use arty::runtime::hook::{HookFn, WorkerInit};
use arty::runtime::{RuntimeBuilder, RuntimeControl};
use tokio::runtime::Handle;

use crate::TokioHandle;

#[derive(Clone)]
struct TokioSupport {
    handle: Handle,
    _owned_runtime: Option<std::sync::Arc<tokio::runtime::Runtime>>,
}

impl ThreadAware for TokioSupport {
    fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {}
}

/// Adds Tokio support to an Arty runtime.
///
/// The implementation for [`RuntimeBuilder`] configures workers before
/// startup. The implementation for a [`RuntimeControl`] reference configures
/// live workers and workers that start later.
pub trait TokioSupportExt: private::Sealed {
    /// The result of adding Tokio support.
    type Output;

    /// Enters the Tokio runtime context on each applicable Arty worker.
    ///
    /// Keep the Tokio runtime alive while Arty tasks use Tokio.
    ///
    /// # Panics
    ///
    /// Panics if `tokio` belongs to a current-thread runtime. A live worker
    /// also panics if it has a context from a different Tokio runtime.
    fn tokio_support(self, tokio: impl Into<TokioHandle>) -> Self::Output;
}

impl TokioSupportExt for RuntimeBuilder {
    type Output = Self;

    fn tokio_support(self, tokio: impl Into<TokioHandle>) -> Self::Output {
        self.on(worker_init(tokio.into()))
    }
}

impl TokioSupportExt for &RuntimeControl {
    type Output = ();

    fn tokio_support(self, tokio: impl Into<TokioHandle>) {
        self.on(worker_init(tokio.into()));
    }
}

fn worker_init(tokio: TokioHandle) -> HookFn<WorkerInit, TokioSupport> {
    tokio.assert_supported();
    HookFn::new(
        TokioSupport {
            handle: tokio.handle,
            _owned_runtime: tokio.owned_runtime,
        },
        enter_worker,
    )
}

fn enter_worker(_event: &mut WorkerInit, tokio: &TokioSupport) {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        assert_eq!(
            current.id(),
            tokio.handle.id(),
            "Tokio support cannot replace a different runtime already installed on this Arty worker"
        );
        return;
    }

    // The worker thread owns this context until it exits. Tokio drops the
    // thread-local handle during thread teardown.
    std::mem::forget(tokio.handle.enter());
}

mod private {
    use arty::runtime::{RuntimeBuilder, RuntimeControl};

    pub trait Sealed {}

    impl Sealed for RuntimeBuilder {}
    impl Sealed for &RuntimeControl {}
}
