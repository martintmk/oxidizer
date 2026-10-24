// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::error::Error;
use std::fmt;

use mio::event::Event;

/// Readiness reported for a registered Mio source.
///
/// Readiness is a hint. The operation can still return
/// [`WouldBlock`](std::io::ErrorKind::WouldBlock). If it does, wait for new
/// readiness before you try the operation again.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the fields mirror Mio readiness flags that can be set independently"
)]
pub struct Readiness {
    readable: bool,
    writable: bool,
    error: bool,
    read_closed: bool,
    write_closed: bool,
    priority: bool,
    aio: bool,
    lio: bool,
}

impl Readiness {
    /// Returns `true` when the source can have data to read.
    #[must_use]
    pub const fn is_readable(self) -> bool {
        self.readable
    }

    /// Returns `true` when the source can accept data to write.
    #[must_use]
    pub const fn is_writable(self) -> bool {
        self.writable
    }

    /// Returns `true` when the source reports an error.
    #[must_use]
    pub const fn is_error(self) -> bool {
        self.error
    }

    /// Returns `true` when the read half can be closed.
    #[must_use]
    pub const fn is_read_closed(self) -> bool {
        self.read_closed
    }

    /// Returns `true` when the write half can be closed.
    #[must_use]
    pub const fn is_write_closed(self) -> bool {
        self.write_closed
    }

    /// Returns `true` when the source reports priority readiness.
    #[must_use]
    pub const fn is_priority(self) -> bool {
        self.priority
    }

    /// Returns `true` when the source reports asynchronous I/O readiness.
    #[must_use]
    pub const fn is_aio(self) -> bool {
        self.aio
    }

    /// Returns `true` when the source reports list I/O readiness.
    #[must_use]
    pub const fn is_lio(self) -> bool {
        self.lio
    }

    pub(crate) fn merge(&mut self, event: &Event) {
        self.readable |= event.is_readable();
        self.writable |= event.is_writable();
        self.error |= event.is_error();
        self.read_closed |= event.is_read_closed();
        self.write_closed |= event.is_write_closed();
        self.priority |= event.is_priority();
        self.aio |= event.is_aio();
        self.lio |= event.is_lio();
    }

    pub(crate) const fn is_empty(self) -> bool {
        !(self.readable || self.writable || self.error || self.read_closed || self.write_closed || self.priority || self.aio || self.lio)
    }
}

/// The Mio driver closed before it reported more readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistrationClosed;

impl fmt::Display for RegistrationClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the Arty Mio registration is closed")
    }
}

impl Error for RegistrationClosed {}
