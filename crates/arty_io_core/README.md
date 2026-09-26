<div align="center">
 <img src="https://raw.githubusercontent.com/microsoft/oxidizer/refs/heads/main/logo.svg" alt="Arty Io Core Logo" width="96">

# Arty Io Core

[![crate.io](https://img.shields.io/crates/v/arty_io_core.svg)](https://crates.io/crates/arty_io_core)
[![docs.rs](https://docs.rs/arty_io_core/badge.svg)](https://docs.rs/arty_io_core)
[![MSRV](https://img.shields.io/crates/msrv/arty_io_core)](https://crates.io/crates/arty_io_core)
[![CI](https://github.com/microsoft/oxidizer/actions/workflows/anvil-pr.yml/badge.svg)](https://github.com/microsoft/oxidizer/actions/workflows/anvil-pr.yml)
[![Coverage](https://codecov.io/gh/microsoft/oxidizer/graph/badge.svg?token=FCUG0EL5TI)](https://codecov.io/gh/microsoft/oxidizer)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/microsoft/oxidizer/blob/main/LICENSE)
<a href="https://github.com/microsoft/oxidizer"><img src="https://raw.githubusercontent.com/microsoft/oxidizer/refs/heads/main/logo.svg" alt="This crate was developed as part of the Oxidizer project" width="20"></a>

</div>

Contracts for integrating I/O drivers with an async runtime.

Shared types for runtimes and independently versioned drivers. This crate provides neither
a runtime nor an I/O implementation.

## Core types

* [`IoContext`][__link0] selects a [`DriverProvider`][__link1], which creates a [`Driver`][__link2] and context per worker.
* [`DriverRole`][__link3] identifies an optional worker-blocking primary and non-blocking secondaries.
* [`Cycle`][__link4] supplies timing, blocking permission, and work registration through [`PendingWorkTracker`][__link5].
* [`PendingWork`][__link6] holds one participation in the runtime’s completion barrier.
* [`DriverOptions`][__link7] supplies per-worker construction facilities and peer handles.
* [`SystemTaskSpawner`][__link8] runs blocking system work outside async workers.
* [`DriverError`][__link9] and [`ShutdownError`][__link10] report infrastructure and cleanup failures.

## Registration

The runtime registers each context type once, initializing a driver/context pair on every
active worker. It assigns a role and completes a zero-wait cycle before publishing the
context. Peers discover each other through [`DriverOptions::drivers`][__link11] and
[`Driver::on_peer_registered`][__link12].

## Driving I/O

Secondaries run before the primary, with a shared time snapshot and wait bound. Only an
invocation with [`Cycle::can_block`][__link13] set to `true` may block the worker.

Drivers register native waits through [`Cycle::start_work`][__link14]. They keep the handle until
work ends and publish any results before completing or dropping it; both notify the runtime.
After the primary returns, the runtime interrupts remaining waits and waits for all handles
before advancing.

The runtime implements [`PendingWorkTracker`][__link15], including interruption state, counters,
and parking. The registration example uses a no-op tracker and performs no I/O.

## Shutdown

[`Driver::shutdown`][__link16] consumes the driver and drains its resources within a bounded wait.
Contexts remain valid as closed handles. Shutdown must progress independently of other
drivers on the same worker.

## Project documents

* [Requirements][__link17]
* [Design][__link18]
* [Completion coordination (exploratory)][__link19]


<hr/>
<sub>
This crate was developed as part of <a href="https://github.com/microsoft/oxidizer">The Oxidizer Project</a>. Browse this crate's <a href="https://github.com/microsoft/oxidizer/tree/main/crates/arty_io_core">source code</a>.
</sub>

 [__cargo_doc2readme_dependencies_info]: ggGmYW0CYXZlMC43LjNhdIQborR2_k_xJd4bTcf2krrNPIcbP72Pw1UdRjkbim_eMDe2BBthYvRhcoQb67IB_hoc_ZQb8Jdi7OnoQIIbV9okkSowNmobSHk2lw6JtQphZIGCbGFydHlfaW9fY29yZWUwLjIuMA
 [__link0]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=IoContext
 [__link1]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=DriverProvider
 [__link10]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=ShutdownError
 [__link11]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=DriverOptions::drivers
 [__link12]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=Driver::on_peer_registered
 [__link13]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=Cycle::can_block
 [__link14]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=Cycle::start_work
 [__link15]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=PendingWorkTracker
 [__link16]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=Driver::shutdown
 [__link17]: https://github.com/microsoft/oxidizer/blob/main/crates/arty_io_core/docs/REQUIREMENTS.md
 [__link18]: https://github.com/microsoft/oxidizer/blob/main/crates/arty_io_core/docs/DESIGN.md
 [__link19]: https://github.com/microsoft/oxidizer/blob/main/crates/arty_io_core/docs/COMPLETION_COORDINATION.md
 [__link2]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=Driver
 [__link3]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=DriverRole
 [__link4]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=Cycle
 [__link5]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=PendingWorkTracker
 [__link6]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=PendingWork
 [__link7]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=DriverOptions
 [__link8]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=SystemTaskSpawner
 [__link9]: https://docs.rs/arty_io_core/0.2.0/arty_io_core/?search=DriverError
