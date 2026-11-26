# weida-protocol

The wire codec of [weida](https://crates.io/crates/weida) protocol version 0: framing, CBOR
headers, negotiation and QUIC application error codes.

It performs no I/O: it parses and encodes byte slices. Every decoder treats its input as hostile:
lengths are checked before allocation, duplicate map keys are rejected and nesting depth is
bounded. The crate is fuzzed, and the golden vectors of the specification are asserted
byte for byte.

Use it to inspect, generate or fuzz weida traffic, or to implement a peer in another runtime. To
send messages, use `weida`.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the normative
  [wire protocol](https://weida.doodleshnookie.net/reference/protocol/)
- [API documentation](https://docs.rs/weida-protocol)
- [Repository](https://github.com/tuco86/weida)

Status: alpha. Protocol version 0 is experimental, and breaking changes are permitted within
`0.x`.

## License

MIT OR Apache-2.0, at your option.
