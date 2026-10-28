# Panic policy

When unwinding is enabled, Arty catches panics raised while it:

- creates an async task from its task factory;
- polls an async task;
- runs a blocking callback.

Arty reports these panics through `JoinError` when possible. By default, the
first contained async task or blocking callback panic shuts down the runtime.
`PanicPolicy::ignore()` keeps the runtime running.

Shutdown after a contained task or callback panic does not depend on joining it.
It still starts if the caller drops the join handle. All runtime workers
receive the same shutdown cause.

Arty records the shutdown cause before it publishes the panic result or
notifies cancelled tasks. Arty preserves that cause across workers.
Later stop requests do not replace that cause.
After a blocking callback panic starts shutdown, Arty wakes its associated
async worker. That worker broadcasts shutdown to all workers.

Arty does not guarantee recovery from panics raised by destructors, custom
wakers, join notifications, telemetry processors, or other cleanup and
notification hooks. Arty also does not recover poisoned state, failed
invariants, or unsafe-contract violations.

Arty may catch some of these panics to maintain memory safety, but callers
cannot rely on continued operation or complete cleanup. The process may abort,
resources may leak, or affected tasks and runtime operations may stop making
progress. Arty remains memory-safe under all panic conditions.
