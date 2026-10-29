# Runtime implementation map

This document is for maintainers. It connects the cross-module ownership and
coordination invariants behind Arty's public runtime behavior.

## Bootstrap and readiness

[`runtime/bootstrap/startup.rs`](../src/runtime/bootstrap/startup.rs) starts
workers in a stable placement order. An exact count that exceeds processor
capacity repeats the available processors in processor-ID order. Each worker
still owns a separate OS thread and first publishes a
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

`Runtime` owns the dispatcher and the worker joiner. Scheduler,
`Builtins`, join, clock, and control handles can outlive individual tasks,
but they do not keep the runtime running. `Runtime::stop` and `Runtime::drop`
request shutdown through the dispatcher; the joiner joins task workers, and
each worker closes its blocking queue and joins every owned blocking thread
before its own thread exits. Blocking task completion alone is not a shutdown
barrier because a pool thread may still be running thread-local destructors.
The runtime owner must stay on an external thread. Stopping or dropping it on
a task or blocking worker panics.

### Relocated result delivery

Worker schedulers relocate the explicit input for `spawn_anywhere` and
`spawn_at` before the task starts. The dispatcher also records the worker that
executes the task on the returned join handle. This registration is optional,
so ordinary join handles keep a `Send` result bound without requiring
`ThreadAware`.

When an Arty worker polls a registered join to successful completion, the join
relocates the result from the execution worker to the receiving worker. Polling
the join outside an Arty worker returns the result without relocation.
Runtime-wide submissions use the ordinary path and do not register result
relocation. See the [scheduling guide](../src/documentation/scheduling.rs) for
the public contract.

### Worker-side owner tests

Tests that stop or drop a runtime owner on a worker retain a private dispatcher
on the test thread. The worker operation must panic and request shutdown. The
test then uses the dispatcher to join the worker and confirm cleanup.

These cases live in `runtime/handle/worker_shutdown_panics_tests.rs`. Public
tests confirm the panic contract without using the private completion handle.

## Worker command lifecycle

[`runtime/worker/async_worker.rs`](../src/runtime/worker/async_worker.rs)
combines command admission, bounded command batches, executor polling, timer
advancement, and shutdown. The dispatcher publishes shutdown before queuing
stop commands so workers can reject or dispose queued factories without waiting
for FIFO delivery. The worker must complete executor retirement before its
unsafe executor storage is dropped.

Each worker cycle selects one next action after command processing, task polling,
and timer advancement. The action can poll again, wait in the primary I/O
driver, wait for a worker signal, or finish shutdown. All non-shutdown actions
still execute secondary I/O drivers before the primary driver.

The executor and worker wait budgets use the fast system clock. The worker
reuses the executor's cycle-end sample directly for system timer advancement, so
normal cycles take no additional time sample after task polling. Tick treats a
timer removed by the driver as complete, even when a precise sample has not
reached that coarse deadline. Controlled time remains externally owned:
`ClockControl` advancement wakes the worker when a timer becomes due. Final
shutdown takes a fresh precise system sample after task cancellation before it
runs timer callbacks.

## Scoped borrowed work

[`task/runtime_scheduler.rs`](../src/task/runtime_scheduler.rs) lets
`RuntimeScheduler::block_on` run a future that borrows synchronous caller data.
`ScopedStorage` owns the borrowing factory or future and a completion sender.
`ScopedJoin` cannot return or unwind until the receiver observes destruction of
that storage. The field order and the safety comment beside the lifetime
extension are part of the invariant: task completion alone is not sufficient;
the borrowing future must be destroyed before the caller's stack is released.
