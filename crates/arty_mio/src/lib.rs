// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(missing_docs)]
#![cfg_attr(all(coverage_nightly, test), feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_logo_url = "https://media.githubusercontent.com/media/microsoft/oxidizer/refs/heads/main/crates/arty/logo.png")]
#![doc(html_favicon_url = "https://media.githubusercontent.com/media/microsoft/oxidizer/refs/heads/main/crates/arty/favicon.ico")]

//! Mio I/O integration for the Arty runtime.
//!
//! [`MioContext`] submits owned Mio sources to one Arty driver. A primary
//! driver owns its native Mio poller on the Arty worker. A secondary driver is
//! a nonblocking proxy for one dedicated native poll thread.
//!
//! A primary driver polls Mio on the Arty worker. A secondary driver uses one
//! helper thread to wait for Mio and wake the owning Arty worker. Its
//! `execute_cycle` call never waits.
//!
//! # Example
//!
//! ```no_run
//! use std::io;
//!
//! use arty::task::Builtins;
//! use arty_mio::{MioContext, MioOptions};
//! use mio::Interest;
//!
//! async fn receive(builtins: &Builtins) -> io::Result<()> {
//!     let io = builtins.io().context::<MioContext>(&MioOptions::default());
//!     let address = "127.0.0.1:0".parse().expect("the example address is valid");
//!     let socket = mio::net::UdpSocket::bind(address)?;
//!     let socket = io.register(socket, Interest::READABLE).await?;
//!
//!     loop {
//!         socket
//!             .readiness(Interest::READABLE)
//!             .await
//!             .map_err(io::Error::other)?;
//!         let mut buffer = [0; 1_024];
//!         match socket
//!             .source_mut()
//!             .map_err(io::Error::other)?
//!             .recv(&mut buffer)
//!         {
//!             Ok(length) => {
//!                 println!("received {length} bytes");
//!                 return Ok(());
//!             }
//!             Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
//!             Err(error) => return Err(error),
//!         }
//!     }
//! }
//! ```

pub use context::MioContext;
pub use driver::MioDriver;
pub use options::MioOptions;
pub use readiness::{Readiness, RegistrationClosed};
pub use registration::{Registered, SourceMut, SourceRef};

mod context;
mod driver;
mod options;
mod readiness;
mod registration;
mod worker;
