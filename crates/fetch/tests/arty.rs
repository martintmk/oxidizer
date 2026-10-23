// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Public behavior for the Arty runtime integration.

#![allow(clippy::unwrap_used, reason = "test code")]

testing_aids::init_tracing!();

use std::sync::Arc;

use arty::task::Builtins;
use fetch::arty::{ArtyDeps, ArtyTlsScript, ArtyTransportOptions, MioConnectorOptions};
use fetch::tls::TlsOptions;
use fetch::{HttpClient, StatusExt as _};
use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(ArtyDeps: Clone, Send, Sync, std::fmt::Debug);
assert_impl_all!(ArtyTransportOptions: Clone, Send, Sync, std::fmt::Debug, Default);
assert_not_impl_any!(ArtyDeps: Copy);
assert_not_impl_any!(ArtyTransportOptions: Copy);

#[arty::test]
async fn public_constructors_build_clients(builtins: Builtins) {
    let deps = ArtyDeps::from_builtins(&builtins);
    let _builder = HttpClient::builder_arty(deps);
    let _builder_from_builtins = HttpClient::builder_arty(&builtins);
    let _builder_with_options = HttpClient::builder_arty_with_options(&builtins, ArtyTransportOptions::default());
    let _client = HttpClient::new_arty(&builtins);
}

#[test]
fn transport_options_carry_mio_and_connector_settings() {
    let options = ArtyTransportOptions::default().connector(
        MioConnectorOptions::default().socket(fetch::options::SocketOptions::default().no_delay(true).send_buffer_size(64 * 1024)),
    );

    assert_eq!(options.connector.socket.no_delay, Some(true));
    assert_eq!(options.connector.socket.send_buffer_size, Some(64 * 1024));
}

#[arty::test(workers = 2)]
async fn scripted_transport_serves_requests_after_worker_relocation(builtins: Builtins) {
    let options = ArtyTransportOptions::default()
        .scripted_response(&b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\nhello from Arty"[..]);
    let client = HttpClient::builder_arty_with_options(&builtins, options)
        .insecure_allow_http()
        .minimal_pipeline()
        .build();

    let response = client.get("http://example.com/").fetch_text().await.unwrap();
    assert_eq!(response.status(), fetch::StatusCode::OK);
    assert_eq!(response.into_body(), "hello from Arty");

    let destination = builtins
        .control()
        .workers()
        .into_iter()
        .find(|worker| worker.id() != builtins.thread().id())
        .unwrap();
    builtins
        .scheduler()
        .spawn_at(destination, client, |client| async move {
            let response = client.get("http://example.com/").fetch_text().await.unwrap();
            assert_eq!(response.status(), fetch::StatusCode::OK);
            assert_eq!(response.into_body(), "hello from Arty");
        })
        .await
        .unwrap();
}

#[arty::test]
async fn rustls_https_uses_explicit_backend(builtins: Builtins) {
    let (script, certificate) = ArtyTlsScript::self_signed(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK").unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(certificate.as_der().to_vec()))
        .unwrap();
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client = HttpClient::builder_arty_with_options(&builtins, ArtyTransportOptions::default().scripted_tls(script))
        .supported_http_versions(&[fetch::Version::HTTP_11])
        .tls_options(TlsOptions::from(tls))
        .build();

    let response = client.get("https://127.0.0.1/").fetch().await.unwrap().ensure_success().unwrap();
    assert_eq!(response.status(), fetch::StatusCode::OK);
}
