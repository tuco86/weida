# weida-raft

[openraft](https://crates.io/crates/openraft) plus the I/O it deliberately leaves out, carried by
[weida](https://crates.io/crates/weida). A replicated service supplies its state machine and log
store; this crate supplies the network.

| Provided here | Yours |
| --- | --- |
| the transport: one QUIC exchange per RPC, on its own port and ALPN `weida-raft/0` | the state machine: what a committed entry means |
| node identity: the key fingerprint a peer proves in the TLS handshake | what the bytes of a `Request` and a `Response` mean |
| follower writes forwarded to the leader, membership changes | the log store; `MemoryStore` is the reference to match |
| snapshots as one streamed exchange | what a snapshot of your state is |
| a CBOR codec for openraft's persisted types | where those bytes are stored |

The log carries opaque bytes, so the transport and the codec are written once and an
application encodes its commands however it likes. `openraft` is re-exported on purpose: a
`weida-raft` release names one openraft minor line, and you implement its storage traits
against that version.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [consensus decision](https://weida.doodleshnookie.net/decisions/0021-consensus-openraft/)
- [API documentation](https://docs.rs/weida-raft)
- [Repository](https://github.com/tuco86/weida)

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
