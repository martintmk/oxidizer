# Design

## Purpose

`arty_io_core` defines the contract between runtimes and independently versioned
I/O drivers. It provides neither a runtime nor an I/O implementation.
Scheduling, native queues, and synchronization stay with their implementations.

## Public model

```text
IoContext::provider(ProviderOptions)
    -> DriverProvider: clone and relocate to each worker
    -> DriverProvider::create(DriverOptions)
    -> worker-owned Driver + consumer-held IoContext
```

Drivers are worker-local and receive exclusive access during completion
processing. Context relocation may optimize placement, but correctness must not
depend on it. Providers decide whether driver instances share state.

`Driver` is dyn-compatible except for its consuming `shutdown` method. A runtime
may use a private owning shim to store heterogeneous driver/context pairs and
dispatch shutdown.

## Registration stays in the runtime

The runtime keys registration by the concrete context type. The first request
initializes a pair on every active worker; later requests clone the existing
context. Different driver versions may coexist when they share a compatible
`arty_io_core` contract.

For each worker, the runtime:

1. Clones and relocates the provider.
2. Assigns a fixed role and supplies earlier peers through `DriverOptions`.
3. Creates the driver/context pair without publishing it.
4. Completes a separate zero-wait cycle with `can_block = false`.
5. Stores the pair, notifies earlier drivers in registration order, and
   acknowledges registration.

Peer handles are borrowed and worker-local. A driver may downcast them and clone
independently owned state, but cannot retain the borrow.

There is at most one primary per worker, selected only from providers with
`CAN_BE_PRIMARY = true`. Without a primary, the runtime owns worker parking.

## Execution and waiting

Secondaries run before the primary. All receive the same `started_at` snapshot
and `max_wait` bound. Only invocations with `can_block = true` may wait on the
worker; secondaries may arm background waits but must return promptly.

`Cycle` mutably borrows a runtime-provided `PendingWorkTracker`. Before entering
or scheduling a native wait, the driver calls `Cycle::start_work(interrupt)`.
The tracker enrolls that work in the completion barrier and registers its
interruption waker before returning. A pending interruption must signal a late
registration immediately.

Native signals must remain latched across the transition into a wait. Wakers
may run inline on any thread and must only signal, not join work or acquire
locks held by the completing work.

Each returned `PendingWork` owns one barrier participation. Completing and
dropping the handle are equivalent: both notify the runtime to retire the
registration, interrupt peers, then release that participation. Publish any
results before either action.

After the primary returns, the runtime interrupts remaining waits and waits
for every handle before beginning another cycle. It begins coordination once
per logical cycle, before checking work, never between driver calls.

Drivers process bounded batches. Immediately serviceable work left after a
batch requests another cycle; in-flight operations alone do not.

The core handle invokes one runtime-supplied waker exactly once. Ordinary waker
cloning and dropping do not complete work. Counters, interruption state, and
parking are runtime responsibilities, not shared-crate implementations.

## Shutdown

`Driver::shutdown` consumes the driver, closes admission, and drains active
operations, callbacks, and observers within a bounded wait.

- Contexts may outlive the driver as closed handles; they do not delay draining.
- Native operation storage stays alive until native access has ended.
  Cancellation alone is not proof that it has ended.
- Normal cycles have stopped. Shutdown progresses locally or on independent
  threads, not through another driver serialized on the same worker.
- The runtime keeps `SystemTaskSpawner` available until all shutdown calls
  return and attempts remaining drivers after an error.
- Dropping is memory-safe at every lifecycle point, including after failure.
  Raw pointers retained by an operating system require independently owned
  backing storage.

## Creation failure

Provider creation and the initial cycle return `DriverError` on failure; the
runtime rolls back the unpublished pair. A normal cycle failure is reported
and initiates driver shutdown. Failure to integrate a newly registered peer
panics because the runtime cannot continue a partially connected registration.

Drivers with conditional availability expose a capability check before
consumers request their context.

## System tasks

`SystemTaskSpawner` accepts blocking `FnOnce` work for runtime-owned system
threads. Submission returns after acceptance, not completion. Indefinite
observers need independent execution capacity; a bounded pool is suitable only
when it can run all simultaneous observers.

## Compatibility

Runtimes and drivers must use compatible copies of the shared contract.
Placement uses `thread_aware_core` types directly, without re-exporting them;
`arty_io_core` must not stabilize before that dependency.

Optional construction facilities belong in `ProviderOptions` or
`DriverOptions`. Mandatory constructor arguments and trait methods are
compatibility commitments. Registries, type erasure, and placement policy
remain private to the runtime.

## Example

The [single-thread example](../examples/single_thread_runtime/main.rs) demonstrates
registration and peer discovery. Its drivers perform no I/O and its tracker
is a no-op, not a reference implementation of completion coordination.

## Deferred decisions

The contract does not choose thread pinning, cross-worker registration
atomicity, shutdown ordering, memory pools, clocks, or telemetry.
[Completion coordination](COMPLETION_COORDINATION.md) explores native wait
sharing and routing beyond this contract.
