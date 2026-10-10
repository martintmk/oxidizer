# Requirements

These contracts govern independently versioned I/O drivers and thread-aware
runtimes. `arty_io_core` provides neither implementation. [Design](DESIGN.md)
describes the lifecycle; the no-op example tracker does not implement coordination.

## R1: Stable shared vocabulary

- Runtimes and drivers use compatible `arty_io_core` types.
- Public signatures prefer standard-library types. Placement uses
  `thread_aware_core` directly, without re-exports.
- Registries, scheduling policy, and native coordination stay outside core.

## R2: Lazy and independent registration

- The runtime keys registration by concrete `IoContext` type and supplies
  `ContextOptions` when creating it.
- The first request returns only after every active worker has an initialized
  driver/context pair. Later requests clone existing contexts.
- Different driver versions can coexist through distinct context types.
- Registration coordination, cancellation, and rollback belong to the runtime.

## R3: Per-worker initialization

- The runtime clones and relocates the context to each owning worker before
  creating its driver. `DriverOptions` supplies that worker, its allowed roles,
  the owner waker used to request another cycle, and the system task spawner.
- `DriverOptions::allowed_roles` lists the roles available during creation.
  `DriverInstance` returns the selected fixed role, which must be allowed.
  Each worker has at most one primary. Without one, the runtime owns worker parking.
- Before publishing a context, the runtime completes a separate cycle with
  `max_wait = Duration::ZERO`.
- Contexts choose whether driver instances share queues, memory, or threads.

## R4: Driver-owned execution strategy

- Driver methods run on the owning worker. Completion processing takes
  `&mut self`; drivers need not be `Send` or `Sync`.
- Each driver supplies a waker that interrupts a pending completion wait. The
  waker remains safe to invoke after the driver is dropped.
- `Driver` remains dyn-compatible; consuming `shutdown` is not callable through
  `dyn Driver`. A private owning shim may adapt it for erased storage.
- Secondaries run before the primary. Every invocation receives the same
  `started_at` and `max_wait`.
- Only the primary may wait on the worker, for up to `max_wait`. A zero wait
  bound means no waiting. Secondaries may register background waits within the
  same bound but return without joining them.
- Bounded batches with serviceable work remaining request another cycle.
  In-flight operations alone do not indicate serviceable work.
- Drivers may use `SystemTaskSpawner` or context-owned threads.

## R5: Safe and blocking shutdown

- Dropping a driver is always memory-safe and closes admission if necessary.
  Contexts may outlive it as closed handles without delaying draining.
- Operations and native callbacks retain their accessible state. Storage
  referenced by native raw pointers has an owner independent of the driver.
- `shutdown` consumes the driver, closes admission, and drains within a bounded
  wait, or returns `ShutdownError` without relaxing drop safety.
- Shutdown uses driver-owned progress mechanisms: normal cycle coordination
  has stopped, and no other driver on the same worker may be required to run.
- The runtime reports failures, attempts remaining drivers, and keeps the system
  task spawner available until all shutdown calls return.
- Safety does not depend on successful shutdown or a caller-checked inertness
  flag. Platform-specific unsafe code stays private to driver implementations.

## R6: Registration failure

- Creation and the initial cycle may return `DriverError`; the runtime rolls
  back the unpublished pair.
- The runtime reports normal cycle errors and shuts down the worker's drivers.
- Drivers with conditional availability expose a capability check before a
  consumer requests the context.

## R7: System work is named explicitly

- `SystemTask` is blocking driver work on system threads, not an async task.
- Submission returns after acceptance, without waiting for completion.
- `SystemTaskSpawner` remains available through shutdown and hides the runtime's
  shared-ownership mechanism.

## R8: Scope of the initial API

Core does not provide peer-driver discovery, a driver registry, native observer
placement, memory pools, configurable clocks, telemetry, ecosystem-specific
errors, batching optimizations, `no_std` support, or a default I/O implementation.
