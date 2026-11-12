<div align="center">
 <img src="https://raw.githubusercontent.com/microsoft/oxidizer/refs/heads/main/logo.svg" alt="Arty Tokio Logo" width="96">

# Arty Tokio

[![crate.io](https://img.shields.io/crates/v/arty_tokio.svg)](https://crates.io/crates/arty_tokio)
[![docs.rs](https://docs.rs/arty_tokio/badge.svg)](https://docs.rs/arty_tokio)
[![MSRV](https://img.shields.io/crates/msrv/arty_tokio)](https://crates.io/crates/arty_tokio)
[![CI](https://github.com/microsoft/oxidizer/actions/workflows/anvil-pr.yml/badge.svg)](https://github.com/microsoft/oxidizer/actions/workflows/anvil-pr.yml)
[![Coverage](https://codecov.io/gh/microsoft/oxidizer/graph/badge.svg?token=FCUG0EL5TI)](https://codecov.io/gh/microsoft/oxidizer)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/microsoft/oxidizer/blob/main/LICENSE)
<a href="https://github.com/microsoft/oxidizer"><img src="https://raw.githubusercontent.com/microsoft/oxidizer/refs/heads/main/logo.svg" alt="This crate was developed as part of the Oxidizer project" width="20"></a>

</div>

Lets [`arty`][__link0] tasks use Tokio-based libraries.

Use [`TokioSupportExt::tokio_support`][__link1] from an async main function to enter
the same Tokio runtime context on every live Arty worker:

```rust
use arty::task::Context;
use arty_tokio::TokioSupportExt as _;
use tokio::runtime::Builder;

let tokio = Builder::new_multi_thread().enable_all().build()?;
cx.control().tokio_support(tokio.handle());
let value = tokio::spawn(async { 42 }).await?;
assert_eq!(value, 42);
```

Use [`TokioSupportExt::tokio_support`][__link2] to configure workers before the
runtime starts:

```rust
use arty::runtime::{Runtime, WorkersPolicy};
use arty_tokio::TokioSupportExt as _;
use tokio::runtime::Builder;

let tokio = Builder::new_multi_thread().enable_all().build()?;
let runtime = Runtime::builder()
    .workers(WorkersPolicy::exactly(2))
    .tokio_support(tokio.handle())
    .build()?;
assert_eq!(runtime.scheduler().block_on(async |_| 42)?, 42);
runtime.stop()?;
```

The application owns the Tokio runtime. Keep it alive while Arty tasks use
Tokio. This crate stores a clone of its handle on each Arty worker.
Current-thread Tokio runtimes are not supported.
Use [`TokioHandle::default`][__link3] when the application does not supply a runtime.
The returned handle owns a default multi-thread Tokio runtime.

After setup, Arty tasks can use Tokio timers, networking, I/O resources,
task spawning, and libraries that require a Tokio runtime context.
Tokio tasks run on Tokio workers. They do not receive
[`arty::task::Context`][__link4] or keep Arty thread-aware state.

Do not call `tokio::task::block_in_place`, `Runtime::block_on`, or
`Handle::block_on` from an Arty worker. `LocalSet` and `spawn_local` are not
integrated with Arty.


<hr/>
<sub>
This crate was developed as part of <a href="https://github.com/microsoft/oxidizer">The Oxidizer Project</a>. Browse this crate's <a href="https://github.com/microsoft/oxidizer/tree/main/crates/arty_tokio">source code</a>.
</sub>

 [__cargo_doc2readme_dependencies_info]: ggGmYW0CYXZlMC43LjNhdIQborR2_k_xJd4bTcf2krrNPIcbP72Pw1UdRjkbim_eMDe2BBthYvRhcoQb5k5h6G2ZX38bhtnvKJ0XycYb4R-TYyrPr8Ibo2QdOiGf42thZIKCZGFydHllMC42LjGCamFydHlfdG9raW9lMC4yLjE
 [__link0]: https://crates.io/crates/arty/0.6.1
 [__link1]: https://docs.rs/arty_tokio/0.2.1/arty_tokio/?search=TokioSupportExt::tokio_support
 [__link2]: https://docs.rs/arty_tokio/0.2.1/arty_tokio/?search=TokioSupportExt::tokio_support
 [__link3]: https://docs.rs/arty_tokio/0.2.1/arty_tokio/?search=TokioHandle::default
 [__link4]: https://docs.rs/arty/0.6.1/arty/?search=task::Context
