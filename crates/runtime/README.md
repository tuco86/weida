# weida-runtime

The reactor, the resolver and the OS plumbing a networking library needs, with no protocol in
it. It answers one question: where does the async runtime live? Its answer lets a library own
its reactor instead of requiring its callers to be inside one.

- **`Exec`**: the whole surface onto the runtime (`spawn`, `sleep`, `within`, `enter`,
  `resolve`). A library that goes through it never touches Tokio directly.
  `Exec::current` borrows the ambient reactor, `Exec::from_handle` takes someone else's, and
  `Exec::owned` creates one with a safe shutdown.
- **`CloseBudget`**: a finite budget across the phases of a shutdown, so no close waits on a
  peer forever.
- **`NameRegistry`**: a bounded namespace of in-process endpoint names.
- **Local sockets:** `AF_UNIX` binding with permission and cleanup hygiene and the kernel's peer
  credentials. On Windows: named pipes with an owner-only DACL and the client's SID.

It serves [weida](https://crates.io/crates/weida) and the standalone protocol libraries of the
same repository, such as [weida-http](https://crates.io/crates/weida-http), which send their
tasks, timers and name lookups through it.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [architecture](https://weida.doodleshnookie.net/reference/architecture/)
- [API documentation](https://docs.rs/weida-runtime)
- [Repository](https://github.com/tuco86/weida)

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
