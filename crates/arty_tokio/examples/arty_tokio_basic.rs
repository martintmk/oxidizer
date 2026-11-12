// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Uses a default Tokio runtime from an Arty task.

use arty::task::Context;
use arty_tokio::{TokioHandle, TokioSupportExt as _};

#[arty::main]
async fn main(cx: Context) {
    cx.control().tokio_support(TokioHandle::default());

    let value = tokio::spawn(async { 42 }).await.expect("Tokio task must complete");
    println!("Tokio returned {value}");
}
