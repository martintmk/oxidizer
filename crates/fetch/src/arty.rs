// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Arty runtime integration for [`HttpClient`].
//!
//! Create dependencies from one [`Builtins`](arty::task::Builtins) value. This
//! keeps the scheduler, clock, and Mio context on the same Arty worker.
//!
//! Enable the `arty` feature and one TLS feature. Use `rustls` for the default
//! TLS backend or `native-tls` for the platform TLS backend.

use std::pin::Pin;

use anyspawn::Spawner;
use arty::task::Builtins;
use arty_mio::MioContext;
pub use arty_mio::MioOptions;
use fetch_hyper::{HyperIo, HyperTransportBuilder};
use fetch_mio::MioConnector;
pub use fetch_mio::MioConnectorOptions;
use thread_aware::ThreadAware;

use crate::custom::{CustomContext, CustomDeps, Isolation};
use crate::handlers::TransportHandler;
use crate::{HttpClient, HttpClientBuilder};

/// A self-signed certificate for an in-process Arty TLS test.
#[cfg(feature = "test-util")]
#[derive(Clone, Debug)]
pub struct ArtyTestCertificate(fetch_hyper::test_util::ScriptedCertificate);

#[cfg(feature = "test-util")]
impl ArtyTestCertificate {
    /// Returns the DER-encoded certificate.
    #[must_use]
    pub fn as_der(&self) -> &[u8] {
        self.0.as_der()
    }
}

/// An in-process TLS server script for Arty transport tests.
#[cfg(feature = "test-util")]
#[derive(Clone, Debug)]
pub struct ArtyTlsScript(fetch_hyper::test_util::ScriptedTlsConnector);

#[cfg(feature = "test-util")]
impl ArtyTlsScript {
    /// Creates a TLS script with a self-signed certificate for `127.0.0.1`.
    ///
    /// # Errors
    ///
    /// Returns an error if certificate or TLS server configuration fails.
    pub fn self_signed(response: impl AsRef<[u8]>) -> std::io::Result<(Self, ArtyTestCertificate)> {
        let (connector, certificate) = fetch_hyper::test_util::ScriptedTlsConnector::self_signed(response)?;
        Ok((Self(connector), ArtyTestCertificate(certificate)))
    }
}

/// Transport options for an Arty HTTP client.
///
/// Use [`connector`](Self::connector) to configure socket no-delay and buffer
/// sizes through [`MioConnectorOptions`]. Use [`mio`](Self::mio) for options
/// that control the Arty Mio context.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ArtyTransportOptions {
    /// Options for the Arty Mio context.
    pub mio: MioOptions,
    /// Options for the Mio HTTP connector.
    pub connector: MioConnectorOptions,
    #[cfg(feature = "test-util")]
    scripted: Option<ScriptedTransport>,
}

impl ArtyTransportOptions {
    /// Sets the options for the Arty Mio context.
    #[must_use]
    pub fn mio(mut self, options: MioOptions) -> Self {
        self.mio = options;
        self
    }

    /// Sets the options for the Mio HTTP connector.
    ///
    /// # Examples
    ///
    /// ```
    /// use fetch::arty::{ArtyTransportOptions, MioConnectorOptions};
    /// use fetch::options::SocketOptions;
    ///
    /// let options = ArtyTransportOptions::default().connector(
    ///     MioConnectorOptions::default().socket(
    ///         SocketOptions::default()
    ///             .no_delay(true)
    ///             .send_buffer_size(256 * 1024),
    ///     ),
    /// );
    /// assert_eq!(options.connector.socket.no_delay, Some(true));
    /// ```
    #[must_use]
    pub fn connector(mut self, options: MioConnectorOptions) -> Self {
        self.connector = options;
        self
    }

    /// Replaces network I/O with one raw HTTP response.
    ///
    /// This method is available only with `test-util`. It lets tests run the
    /// bundled Hyper transport without DNS or socket I/O.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn scripted_response(mut self, response: impl AsRef<[u8]>) -> Self {
        self.scripted = Some(ScriptedTransport::Plain(bytes::Bytes::copy_from_slice(response.as_ref())));
        self
    }

    /// Replaces network I/O with an in-process TLS server.
    ///
    /// This method is available only with `test-util`.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn scripted_tls(mut self, script: ArtyTlsScript) -> Self {
        self.scripted = Some(ScriptedTransport::Tls(script));
        self
    }
}

/// Dependencies for an Arty HTTP client.
///
/// Create this value from one [`Builtins`] value. The fields are private so
/// services from different Arty workers cannot be mixed.
#[derive(Clone, Debug)]
pub struct ArtyDeps {
    builtins: Builtins,
    global_pool: bytesbuf::mem::GlobalPool,
}

impl ArtyDeps {
    /// Creates dependencies from one Arty worker.
    #[must_use]
    pub fn from_builtins(builtins: &Builtins) -> Self {
        Self {
            builtins: builtins.clone(),
            global_pool: bytesbuf::mem::GlobalPool::new(),
        }
    }
}

impl From<&Builtins> for ArtyDeps {
    fn from(builtins: &Builtins) -> Self {
        Self::from_builtins(builtins)
    }
}

impl HttpClient {
    /// Creates an HTTP client builder for the Arty runtime.
    ///
    /// The client creates one Hyper transport for each Arty worker. The
    /// connector uses Mio for socket I/O and keeps worker-local state isolated.
    ///
    /// Enable the `arty` feature and one TLS feature to use this method.
    pub fn builder_arty(deps: impl Into<ArtyDeps>) -> HttpClientBuilder {
        Self::builder_arty_with_options(deps, ArtyTransportOptions::default())
    }

    /// Creates an Arty HTTP client builder with transport options.
    ///
    /// The options apply to each worker-local Mio context and connector.
    pub fn builder_arty_with_options(deps: impl Into<ArtyDeps>, options: ArtyTransportOptions) -> HttpClientBuilder {
        let ArtyDeps { builtins, global_pool } = deps.into();
        let clock = builtins.clock().clone();

        Self::builder_custom_internal(
            crate::constants::ARTY_RUNTIME_NAME,
            crate::constants::HYPER_TRANSPORT_NAME,
            |cx| TransportHandler(build_arty_handler(cx).into()),
            Isolation::Isolated,
            CustomDeps {
                clock,
                global_pool,
                extras: ArtyExtras { builtins, options },
            },
        )
    }

    /// Creates an HTTP client for the Arty runtime.
    ///
    /// Use [`builder_arty`](Self::builder_arty) when the client needs custom
    /// Fetch settings.
    #[must_use]
    pub fn new_arty(builtins: &Builtins) -> Self {
        Self::builder_arty(builtins).build()
    }
}

#[derive(Clone, Debug, ThreadAware)]
struct ArtyExtras {
    builtins: Builtins,
    #[thread_aware(skip)]
    options: ArtyTransportOptions,
}

fn build_arty_handler(cx: CustomContext<ArtyExtras>) -> fetch_hyper::HyperTransport {
    let scheduler = cx.extras.builtins.scheduler().clone();
    let context = cx.extras.builtins.io().context::<MioContext>(&cx.extras.options.mio);
    let connector = MioConnector::new(context, scheduler.clone(), cx.clock.clone(), cx.extras.options.connector.clone());
    let connector = BundledConnector::new(connector, &cx);
    let tls_backend = crate::tls::build_tls_backend(&cx.options, cx.tls);

    HyperTransportBuilder::new(connector, Spawner::new_arty(scheduler), cx.clock, cx.options)
        .body_builder(cx.body_builder)
        .pool_index(cx.pool_index)
        .meter(cx.meter)
        .build(tls_backend)
}

#[derive(Clone, Debug)]
struct BundledConnector {
    mio: MioConnector,
    #[cfg(feature = "test-util")]
    scripted: Option<ScriptedTransport>,
}

impl BundledConnector {
    fn new(connector: MioConnector, cx: &CustomContext<ArtyExtras>) -> Self {
        Self {
            mio: connector,
            #[cfg(feature = "test-util")]
            scripted: cx.extras.options.scripted.clone(),
        }
    }
}

impl layered::Service<templated_uri::BaseUri> for BundledConnector {
    type Out = crate::Result<Pin<Box<dyn HyperIo>>>;

    async fn execute(&self, input: templated_uri::BaseUri) -> Self::Out {
        #[cfg(feature = "test-util")]
        if let Some(scripted) = &self.scripted {
            return match scripted {
                ScriptedTransport::Plain(response) => {
                    let stream = fetch_hyper::test_util::ScriptedConnector::new(response.clone())
                        .execute(input)
                        .await?;
                    Ok(Box::pin(stream))
                }
                ScriptedTransport::Tls(script) => {
                    let stream = script.0.execute(input).await?;
                    Ok(Box::pin(stream))
                }
            };
        }

        self.mio
            .execute(input)
            .await
            .map(|stream| Box::pin(stream) as Pin<Box<dyn HyperIo>>)
    }
}

#[cfg(feature = "test-util")]
#[derive(Clone, Debug)]
enum ScriptedTransport {
    Plain(bytes::Bytes),
    Tls(ArtyTlsScript),
}
