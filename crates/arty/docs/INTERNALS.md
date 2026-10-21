# Runtime implementation map

This document is for maintainers. It connects the cross-module ownership and
coordination invariants behind Arty's public runtime behavior.

## Bootstrap and readiness

[`runtime/bootstrap/startup.rs`](../src/runtime/bootstrap/startup.rs) starts
workers in stable processor order. Each worker first publishes a
`WorkerEndpoint`: its command sender, wake handle, thread coordinate, and
blocking worker. Bootstrap collects every endpoint before constructing the
dispatcher, then distributes a `DispatcherClient` back to every worker through
its start channel.

Each worker constructs its `Builtins` after receiving the shared dispatcher.
Bootstrap waits for every worker's success signal before returning the
`Runtime`, so public scheduling starts with endpoints and worker services ready.

OS thread-creation errors and cleanup after partial startup remain deferred to a
separate lifecycle/error design. The accepted scope decision is recorded in
[the startup review discussion](https://github.com/microsoft/oxidizer/pull/785#discussion_r4183811801).

## Dispatch and ownership

[`runtime/dispatch/dispatcher_core.rs`](../src/runtime/dispatch/dispatcher_core.rs)
owns the complete endpoint set and maps runtime thread IDs to worker indexes.
`DispatcherClient` is the cloneable capability used by schedulers and workers;
it does not own a separate runtime or worker set.

`Runtime` owns the dispatcher and the worker-shutdown waiter. Scheduler,
`Builtins`, join, clock, and operations handles can outlive individual tasks,
but they do not keep the runtime running. `Runtime::stop` and `Runtime::drop`
request shutdown through the dispatcher; the waiter joins async workers, and
each worker closes its blocking queue and joins every owned blocking thread
before its own thread exits. Blocking task completion alone is not a shutdown
barrier because a pool thread may still be running thread-local destructors.
Blocking-thread membership remains registered through those destructors so a
runtime owner released there requests shutdown without waiting on itself.

### Shutdown guard tests

Tests that consume or drop a runtime owner on an async worker must retain a
dispatcher on the test thread and call its `wait` method after the worker-side
operation. Joining a task or a different runtime does not join that runtime's
workers. Cancellation can also destroy captured runtime owners on the worker,
so each nested runtime needs its own completion barrier.

The consuming-owner cases live in `runtime/handle/worker_wait_guards_tests.rs`
because the public API does not expose a completion handle after consuming the owner.
The public cancellation test retains its runtime owner outside the cancelled
task and calls `stop` from the test thread. Both forms keep the worker wait
guards active and join every runtime before returning.

## Worker command lifecycle

[`runtime/worker/async_worker.rs`](../src/runtime/worker/async_worker.rs)
combines command admission, bounded command batches, executor polling, timer
advancement, and shutdown. The dispatcher publishes shutdown before queuing
stop commands so workers can reject or dispose queued factories without waiting
for FIFO delivery. The worker must complete executor retirement before its
unsafe executor storage is dropped.

`ShutdownSignal` is a shared, monotonic flag: the first request starts shutdown
and later requests are idempotent. Its atomic operations use relaxed ordering
because the flag carries no data payload. Command channels, locks, and thread
joins independently synchronize the runtime state.

The worker checks shutdown before admitting commands, after a nonempty command
batch, and after task polling and timer callbacks. Task commands only register
deferred futures, so they do not need an individual check; the deferred future
checks again before invoking its user factory. Synchronous I/O access commands
have their own admission check. Blocking submissions check at admission and
again on the pool thread before invoking the callback.

The worker and executor share a fast system clock for cycle measurements, timer
advancement, and wait budgets. The executor's completion timestamp is reused
directly for timer advancement. Controlled-clock auto-advancement runs inside
the timer driver after task polling, independently of cycle timing. Final
shutdown uses a precise system sample so due timers registered during task
cancellation are not missed by a coarse clock sample.

## Scoped borrowed work

[`task/runtime_scheduler.rs`](../src/task/runtime_scheduler.rs) lets
`RuntimeScheduler::block_on` run a future that borrows synchronous caller data.
`ScopedStorage` owns the borrowing factory or future and a completion sender.
`ScopedJoin` cannot return or unwind until the receiver observes destruction of
that storage. The field order and the safety comment beside the lifetime
extension are part of the invariant: task completion alone is not sufficient;
the borrowing future must be destroyed before the caller's stack is released.
