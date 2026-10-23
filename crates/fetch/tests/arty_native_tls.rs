// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Native TLS behavior for the Arty runtime integration.

#![allow(clippy::unwrap_used, reason = "test code")]

testing_aids::init_tracing!();

use arty::task::Builtins;
use fetch::arty::{ArtyTlsScript, ArtyTransportOptions};
use fetch::tls::TlsOptions;
use fetch::{HttpClient, StatusExt as _};

#[arty::test]
async fn native_tls_https_uses_explicit_backend(builtins: Builtins) {
    let (script, certificate) = ArtyTlsScript::self_signed(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK").unwrap();
    let certificate = native_tls::Certificate::from_der(certificate.as_der()).unwrap();
    let tls = TlsOptions::builder_native_tls().root_certificate(certificate).build();
    let client = HttpClient::builder_arty_with_options(&builtins, ArtyTransportOptions::default().scripted_tls(script))
        .supported_http_versions(&[fetch::Version::HTTP_11])
        .tls_options(tls)
        .build();

    let response = client.get("https://127.0.0.1/").fetch().await.unwrap().ensure_success().unwrap();
    assert_eq!(response.status(), fetch::StatusCode::OK);
}
