# weida-winpipe

The Win32 calls a named-pipe transport needs, behind a safe surface, and nothing else:

- an owner-only security descriptor for `CreateNamedPipe`,
- the connected client's user SID, through impersonation of its token,
- the pipe object's owner SID, for the client side,
- the process ids of both ends.

Neither `std` nor Tokio exposes these. This crate keeps the `unsafe` they need in one place a
reviewer can read in one sitting, so its consumer
[weida-runtime](https://crates.io/crates/weida-runtime) keeps `unsafe_code = "forbid"`. It takes
and returns Tokio's pipe handles, strings and integers. On every platform but Windows it is
empty.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the decision on
  [local transports](https://weida.doodleshnookie.net/decisions/0010-local-transport/)
- [API documentation](https://docs.rs/weida-winpipe)
- [Repository](https://github.com/tuco86/weida)

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
