# fetch crate

## Conditional Compilation

**A TLS backend is mandatory to create an `HttpClient` for real network use.**
The `arty` and `tokio` features do not enable TLS. Enable one runtime feature and
one TLS feature. There are two TLS backends, `rustls` and `native-tls`.

```rust
// Compile the Tokio integration when a TLS backend is enabled.
#[cfg(all(feature = "tokio", any(feature = "rustls", feature = "native-tls")))]

// Compile the Arty integration when a TLS backend is enabled.
#[cfg(all(feature = "arty", any(feature = "rustls", feature = "native-tls")))]
```

If you add a TLS backend, add its feature to each applicable `any(...)` expression.

Some items are guarded by a single TLS backend plus `test`, for example the TLS error
label resolution:

```rust
#[cfg(any(feature = "rustls", test))]
// ...
#[cfg(any(feature = "native-tls", test))]
```

`test-util` is the only escape hatch: it enables `FakeHandler` (canned responses, no
network), so no TLS backend is needed.
