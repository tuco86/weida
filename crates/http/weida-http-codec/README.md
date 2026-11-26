# weida-http-codec

HTTP/1.1, HPACK, HTTP/2 framing, QPACK and HTTP/3 framing, sans-I/O. It has no sockets, no
executor and no dependencies; it encodes and decodes bytes per the RFCs and is checked against
their examples.

| Module | Covers |
| --- | --- |
| `h1` | request and response heads, body framing, chunked bodies (RFC 9112) |
| `h2`, `hpack`, `huffman` | HTTP/2 frames and field compression (RFC 9113, RFC 7541) |
| `h3`, `qpack`, `varint` | HTTP/3 frames, static-table QPACK, QUIC varints (RFC 9114, 9204, 9000) |
| `fields`, `uri`, `date` | field grammar, `http`/`https` URLs, IMF-fixdate |

Every decode takes the caller's `Limits` as an argument, and a declared size is refused from the
declaration alone, before anything is allocated.

```rust
use weida_http_codec::{Limits, h1};

let wire = b"GET /hello.txt HTTP/1.1\r\nHost: www.example.com\r\n\r\n";
let (head, used) = h1::decode_request(wire, Limits::DEFAULT).expect("a request");
assert_eq!(used, wire.len());
assert_eq!(head.target, b"/hello.txt");
```

It is the codec under [weida-http](https://crates.io/crates/weida-http).

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [HTTP feature sheet](https://weida.doodleshnookie.net/libraries/http/)
- [API documentation](https://docs.rs/weida-http-codec)
- [Repository](https://github.com/tuco86/weida)

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
