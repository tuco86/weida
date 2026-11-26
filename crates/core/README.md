# weida-core

The I/O-free core model of [weida](https://crates.io/crates/weida), a messaging framework built
around QUIC: endpoint addresses, limits, trace context, errors and the host a peer counts as.
It has no networking, no Tokio and no serialization, so everything in it can be tested without
a socket.

Most applications use it through `weida`, which re-exports what a caller needs. Depend on it
directly when you write a transport, a codec or a tool that shares weida's vocabulary.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [architecture](https://weida.doodleshnookie.net/reference/architecture/) that places this crate
- [API documentation](https://docs.rs/weida-core)
- [Repository](https://github.com/tuco86/weida)

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
