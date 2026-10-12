# Driver Contract Follow-ups

## TL;DR

- Introduce a `DriverProvider` between `IoContext` and `Driver`.
- Make `Driver::shutdown` infallible.

Keep `Driver::execute_cycle` fallible. A driver may recover internally, but an
error returned to the runtime means the driver is poisoned and the runtime
should shut down.

## Introduce `DriverProvider`

`IoContext` serves endpoint authors and application I/O operations.
Worker-local driver construction serves the runtime. Combining both
responsibilities makes the context API dual-purpose and exposes runtime
lifecycle details to endpoint implementations.

Add a provider layer with narrowly separated responsibilities:

- `IoContext` remains the consumer-facing I/O handle.
- `DriverProvider` owns runtime-facing driver construction.
- The runtime clones and relocates the provider to each worker before asking it
  to create a driver.
- Provider construction accepts implementation-specific configuration, such as
  a GPU handle, without adding those inputs to the shared core API.
- The relationship between a context and its provider is explicit in the type
  system.

The design should answer:

- Which type creates the initial context and provider?
- Where provider-specific options live and how callers supply them.
- Which `Clone`, `Send`, and `ThreadAware` bounds belong on the context and the
  provider.
- How the runtime obtains the context it publishes after every worker-local
  driver has completed its initialization cycle.
- Whether one provider instance is relocated between workers or cloned into
  independently owned worker instances.

Acceptance criteria:

- Endpoint implementations do not need to implement or understand
  worker-driver construction methods.
- The runtime can create one worker-local driver per registration.
- Custom provider inputs are strongly typed and do not require a universal
  options bag.
- Context and provider lifetimes, ownership, and relocation responsibilities
  are unambiguous in rustdoc and the runtime example.

## Make shutdown infallible

Change the shutdown contract from:

```rust
fn shutdown(self) -> Result<(), DriverError>;
```

to:

```rust
fn shutdown(self);
```

The runtime cannot recover differently based on a shutdown error: it must
continue shutting down the remaining drivers either way. Returning an error
therefore adds API surface and a second reporting path without changing runtime
control flow.

Driver implementations should report shutdown anomalies at the layer that has
the implementation-specific context. This keeps diagnostics close to the
failing operating-system operation and avoids requiring the runtime to define a
generic shutdown telemetry contract.

The shutdown contract must still require:

- closing admission to new operations;
- bounded draining of active operations, callbacks, and observers;
- memory-safe drop even when graceful cleanup cannot finish;
- no panic from shutdown or drop;
- no dependency on another driver running on the same worker.

Acceptance criteria:

- The runtime attempts shutdown for every driver.
- A shutdown anomaly cannot prevent runtime teardown from continuing.
- Drivers document and emit implementation-specific diagnostics for incomplete
  cleanup.
- Tests cover continued teardown after a driver cannot complete graceful
  cleanup without introducing a fallible shutdown return value.

## Retain fallible cycle execution

Keep:

```rust
fn execute_cycle(&mut self, cycle: &mut Cycle) -> Result<(), DriverError>;
```

Operating-system resource failures can make completion processing fail. A
driver may retry or recover internally when appropriate, but implementation-
specific recovery should remain inside the driver. If the error reaches the
runtime, the driver is no longer usable and the runtime should stop normal
cycles and begin shutdown.
