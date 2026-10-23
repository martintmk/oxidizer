<div align="center">
 <img src="https://raw.githubusercontent.com/microsoft/oxidizer/refs/heads/main/logo.svg" alt="Arty Mio Logo" width="96">

# Arty Mio

[![crate.io](https://img.shields.io/crates/v/arty_mio.svg)](https://crates.io/crates/arty_mio)
[![docs.rs](https://docs.rs/arty_mio/badge.svg)](https://docs.rs/arty_mio)
[![MSRV](https://img.shields.io/crates/msrv/arty_mio)](https://crates.io/crates/arty_mio)
[![CI](https://github.com/microsoft/oxidizer/actions/workflows/anvil-pr.yml/badge.svg)](https://github.com/microsoft/oxidizer/actions/workflows/anvil-pr.yml)
[![Coverage](https://codecov.io/gh/microsoft/oxidizer/graph/badge.svg?token=FCUG0EL5TI)](https://codecov.io/gh/microsoft/oxidizer)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/microsoft/oxidizer/blob/main/LICENSE)
<a href="https://github.com/microsoft/oxidizer"><img src="https://raw.githubusercontent.com/microsoft/oxidizer/refs/heads/main/logo.svg" alt="This crate was developed as part of the Oxidizer project" width="20"></a>

</div>

Mio I/O integration for the Arty runtime.

[`MioContext`][__link0] submits owned Mio sources to one Arty driver. A primary
driver owns its native Mio poller on the Arty worker. A secondary driver is
a nonblocking proxy for one dedicated native poll thread.

A primary driver polls Mio on the Arty worker. A secondary driver uses one
helper thread to wait for Mio and wake the owning Arty worker. Its
`execute_cycle` call never waits.

## Example

```rust
use std::io;

use arty::task::Builtins;
use arty_mio::{MioContext, MioOptions};
use mio::Interest;

async fn receive(builtins: &Builtins) -> io::Result<()> {
    let io = builtins.io().context::<MioContext>(&MioOptions::default());
    let address = "127.0.0.1:0".parse().expect("the example address is valid");
    let socket = mio::net::UdpSocket::bind(address)?;
    let socket = io.register(socket, Interest::READABLE)?;

    loop {
        socket
            .readiness(Interest::READABLE)
            .await
            .map_err(io::Error::other)?;
        let mut buffer = [0; 1_024];
        match socket
            .source_mut()
            .map_err(io::Error::other)?
            .recv(&mut buffer)
        {
            Ok(length) => {
                println!("received {length} bytes");
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
}
```


<hr/>
<sub>
This crate was developed as part of <a href="https://github.com/microsoft/oxidizer">The Oxidizer Project</a>. Browse this crate's <a href="https://github.com/microsoft/oxidizer/tree/main/crates/arty_mio">source code</a>.
</sub>

 [__cargo_doc2readme_dependencies_info]: ggGmYW0CYXZlMC43LjNhdIQborR2_k_xJd4bTcf2krrNPIcbP72Pw1UdRjkbim_eMDe2BBthYvRhcoQbZrnjxuowXW4bCj2Y-5pzChob8MdMLuiqlVwbwTHtdy7cy79hZIGCaGFydHlfbWlvZTAuMS4w
 [__link0]: https://docs.rs/arty_mio/0.1.0/arty_mio/?search=MioContext
