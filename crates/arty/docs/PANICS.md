# Panic policy

When unwinding is enabled, Arty catches panics raised while it:

- creates an async task from its task factory;
- polls an async task;
- runs a blocking callback.

Arty reports these panics through `JoinError` when possible. By default, the
first contained async task panic shuts down the runtime.
`PanicPolicy::ignore()` keeps the runtime running.

A blocking callback panic fails its join handle but does not start shutdown
under either policy.

Shutdown after an async task panic does not depend on joining that task.
It still starts if the caller drops the join handle. All runtime workers
receive the same shutdown cause.

The executor selects the shutdown cause before it publishes the panic result
or notifies cancelled tasks. Arty preserves that cause across workers.
Later stop requests do not replace that cause.
An async worker then broadcasts shutdown to all workers.

Arty does not guarantee recovery from panics raised by destructors, custom
wakers, join notifications, telemetry processors, or other cleanup and
notification hooks. Arty also does not recover poisoned state, failed
invariants, or unsafe-contract violations.

Arty may catch some of these panics to maintain memory safety, but callers
cannot rely on continued operation or complete cleanup. The process may abort,
resources may leak, or affected tasks and runtime operations may stop making
progress. Arty remains memory-safe under all panic conditions.
