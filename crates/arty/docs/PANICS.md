# Panics

Arty catches panics from task factories and task bodies and returns a
`JoinError`. Awaiting or waiting on the join returns that error rather than
unwinding the caller.

Dropping a join neither resumes a panic nor cancels the task. Runtime shutdown
cancels pending tasks; their joins return a shutdown error, not a task panic.

The `main` and `test` attributes resume the entry-point body's original panic
on the calling thread. They also panic if runtime construction fails or the
entry-point body is cancelled during shutdown. Explicit runtime construction
returns an error where possible, although worker creation and initialization
can still panic.

Calling `JoinHandle::wait` on an async Arty worker panics.
`RuntimeScheduler::block_on` and `Runtime::stop` return an error instead of
waiting in that context.
A blocking callback cannot wait for its own runtime to shut down; dropping the
runtime owner there requests shutdown without waiting for the callback itself.
