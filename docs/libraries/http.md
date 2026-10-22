# HTTP (hyper 1.11.1, h3 0.0.8) — feature parity

`weida-http-codec` (codec) and `weida-http` (client and server), the sixth protocol family
([0044](../decisions/0044-weida-http.md)). Every row below is a verdict on what this library
does with an item of HTTP's inventory as RFC 9110, 9112, 9113 and 9114 define it and as hyper
(HTTP/1.1, HTTP/2) and h3 (HTTP/3) implement it.

## 1. What a row means

- **present**: implemented; the row names the module and the test that proves it.
- **refused with a reason**: the library says no at the call or on the wire, by name.
- **absent with a reason**: not implemented; the row says what a caller who wants it does not
  get. 0044 §6 lists the absences that are decisions.

There is no "partial". Measured claims were measured against hyper 1.11.1 with hyper-util
0.1.20 and http-body-util 0.1.5, h3 0.0.8 with h3-quinn 0.0.10 on quinn 0.11.12 (all from
`Cargo.lock`), and curl 8.23.0 with nghttp2 1.70.0, ngtcp2 1.25.0 and nghttp3 1.18.0.

## 2. Versions, roles and message features

| Item | Verdict | Evidence |
| --- | --- | --- |
| HTTP/1.1 client | present | `h1/client.rs`; `io::interop_hyper::our_client_against_hyper_h1{,_over_tls}` |
| HTTP/1.1 server | present | `h1/server.rs`; `io::interop_hyper::hyper_h1_client_against_our_server` |
| HTTP/1.0 peers | present: read, answered with keep-alive only on `connection: keep-alive`, bodies close-delimited | `h1::server` framing, `h1::client::read_response` |
| HTTP/2 client | present, over TLS by ALPN `h2` | `h2/conn.rs`; `io::interop_hyper::our_client_against_hyper_h2` |
| HTTP/2 server | present, offered by `Server::bind_tls` | `io::interop_hyper::hyper_h2_client_against_our_server` |
| HTTP/3 client | present with feature `h3`, `Protocol::Http3` | `h3.rs`; `io::interop_h3::our_client_against_h3_server` |
| HTTP/3 server | present with feature `h3`, `Server::bind_quic` | `io::interop_h3::h3_client_against_our_server` |
| Every method token | present; `Method::from_bytes` validates a token | `types::tests::methods_are_tokens` |
| `CONNECT` | refused: `Error::Unsupported("CONNECT")` on the client, 501 (h1) or a stream error (h2, h3) on the server | `h1::server` refusal tests |
| `Upgrade`, `101 Switching Protocols`, WebSocket | refused: a 101 is `Error::Protocol`; `upgrade` is a connection-specific field the client refuses | `h1::client::tests::interim_responses_are_skipped_and_101_refused` |
| Interim (1xx) responses | present: skipped by the client; a server cannot send one (`Error::Protocol`) except `100 Continue` | same test |
| `Expect: 100-continue` | present on the server (sent at the first body read); the client never sends `expect` | `h1::server::tests::continue_is_sent_only_when_the_body_is_first_read` |
| Content-Length bodies | present, enforced both ways | `h1::client::tests::a_declared_length_is_enforced_on_upload`, `BodyWriter` |
| Chunked bodies and trailers | present on HTTP/1.1; trailers on every version | `io::loopback` trailer rows, `h2::conn::tests::trailers_both_ways` |
| Transfer codings other than `chunked` | refused: 501 on the server, `"transfer coding"` on the client | `golden_framing_refusals` |
| Persistent connections | present: idle HTTP/1.1 connections per origin, one shared HTTP/2 or HTTP/3 connection per origin | `h1::client::tests::keep_alive_returns_the_connection_and_close_does_not`, `io::interop_hyper` connection counts |
| Pipelining | present on the server, answered in order; absent on the client, which sends one request per connection at a time | `h1::server::tests::pipelined_requests_are_answered_in_order` |
| Server push | refused: `SETTINGS_ENABLE_PUSH = 0`, no MAX_PUSH_ID, PUSH_PROMISE is a connection error | `h2/conn.rs`, `h3.rs` |
| h2c (cleartext HTTP/2) | refused: `Error::Unsupported("h2 without TLS (h2c)")` | `client.rs` |
| Redirects | absent by decision: a 3xx is returned (0044 §4) | `weida-openbao` redirect test |
| Cookies, content coding, proxies, Alt-Svc | absent by decision (0044 §6) | — |

## 3. Transports

| Transport | Verdict | Evidence |
| --- | --- | --- |
| TCP | present | `dial.rs`, `Server::bind_tcp` |
| TLS over TCP, ALPN `http/1.1` and `h2` | present, the caller's rustls config with only ALPN set | `dial::upgrade_tls`, `Server::bind_tls` |
| QUIC (HTTP/3) | present with `h3` | `h3::connect`, `Server::bind_quic` |
| Several addresses for one name | present: TCP tried in turn with half the budget each; QUIC raced 250 ms apart (RFC 8305) | `dial.rs`, `h3::connect` |
| UNIX-domain sockets | absent: no URL form names one and no peer here uses one | — |

## 4. Mechanisms and authentication

| Item | Verdict | Evidence |
| --- | --- | --- |
| Server certificate validation | present, by the caller's `rustls::ClientConfig` | `ClientOptions::tls` |
| Client certificates | present, whatever the caller's rustls configs set | `ServerOptions` takes the caller's `ServerConfig` |
| `Authorization`, cookies as credentials | present as fields; never HPACK/QPACK-indexed, printed `<redacted>` | `types::tests::credentials_never_print`, `hpack::tests::a_sensitive_field_is_never_indexed_and_never_enters_the_table` |
| HTTP authentication schemes (Basic, Digest, Bearer) | absent: the field is the application's | — |

## 5. Options

| hyper / h2 / h3 option | Here | Verdict |
| --- | --- | --- |
| `http2::Builder::initial_stream_window_size` | `stream_window` | present |
| `http2::Builder::initial_connection_window_size` | `connection_window` | present |
| `http2::Builder::max_concurrent_streams` | `ServerOptions::max_concurrent_streams` | present |
| `http2::Builder::max_frame_size` | `Limits::max_frame_size` | present |
| `http2::Builder::max_header_list_size`, `http1::Builder::max_buf_size` | `Limits::max_head_bytes` | present |
| `http1::Builder::header_read_timeout` | `ServerOptions::header_timeout` | present |
| `http1::Builder::keep_alive`, h2 keep-alive pings | `idle_timeout` closes idle connections; no pings are sent | present / absent (no ping keep-alive) |
| pool `max_idle_per_host`, `pool_idle_timeout` | `ClientOptions::max_idle_per_origin`, `idle_timeout` | present |
| connect timeout | `ClientOptions::connect_timeout` (lookup, TCP, TLS or QUIC together) | present |
| request timeout | absent: the caller's deadline (`Exec::within`); filed as B-372 | absent |
| `http2::Builder::adaptive_window` | absent: windows are fixed by the options | absent |
| `h3` `max_field_section_size` | `Limits::max_head_bytes`, advertised in SETTINGS | present |
| QPACK table capacity, blocked streams | fixed at 0 | refused (0044 §4) |

## 6. Observability

`tracing` events at debug level for TLS handshake failures, HTTP/2 connection errors and a
peer's GOAWAY with a code. No counters and no metrics hook: absent, because no consumer asked
for them and `weida`'s statistics model (0036) is per connection of a weida endpoint.

## 7. Devices and helpers

None. hyper's `service` and tower layers have no counterpart: the server is pull-style
(`Server::accept` yields an `Exchange`, 0044 §4). No proxy, no router, no static file server.

## 8. Interop evidence

| Pairing | Roles | Rows | Test |
| --- | --- | --- | --- |
| hyper 1.11.1 `server::conn::http1` | our client | 3 B GET, 1 MiB echo, 4 MiB chunked, keep-alive reuse on one connection, plain and TLS | `io::interop_hyper::our_client_against_hyper_h1{,_over_tls}`, `two_requests_share_one_keep_alive_connection` |
| hyper 1.11.1 `client::conn::http1` | our server | GET (with `date`), 1 MiB echo, chunked response | `io::interop_hyper::hyper_h1_client_against_our_server` |
| hyper 1.11.1 `server::conn::http2` | our client | GET, 1 MiB echo, 50 concurrent on one connection, trailers, `Version::Http2` | `io::interop_hyper::our_client_against_hyper_h2` |
| hyper 1.11.1 `client::conn::http2` | our server | GET, 1 MiB echo, 50 concurrent on one connection, trailers | `io::interop_hyper::hyper_h2_client_against_our_server` |
| h3 0.0.8 server on h3-quinn 0.0.10 | our client | GET, 1 MiB echo, 50 concurrent, trailers | `io::interop_h3::our_client_against_h3_server` |
| h3 0.0.8 client on h3-quinn 0.0.10 | our server | GET, 1 MiB echo, 50 concurrent on one connection, trailers | `io::interop_h3::h3_client_against_our_server` |
| curl 8.23.0 | our server (`examples/hello.rs`) | HTTP/1.1, HTTP/2 (`--http2`), HTTP/3 (`--http3-only`) | smoke, recorded in the NIGHTLOG for B-368 |

Disagreements found and settled: the h3 client sends unknown (grease) frames after a request's
trailers, which this library first refused; RFC 9114 §9 allows them and they are now skipped.
The suites run in the integration tier on four platforms (runner pipelines #41, #43).

## 9. Deliberate deviations and bounds

Deviations:

- Obsolete line folding is refused in responses too (RFC 9112 §5.2 lets a client replace it),
  so one parser serves both directions; pinned by `golden_head_refusals`.
- A transfer coding other than `chunked` is refused rather than passed through undecoded.
- The HPACK encoder writes credentials never-indexed and takes Huffman only when strictly
  shorter, so RFC 7541 C.5.3, C.6.2 and C.6.3 are asserted decode-only (codec `hpack` docs).
- An uppercase field name in HTTP/2 is a stream error with the HPACK table kept in step.

Bounds, each the caller's:

| Bound | Default | Holds back |
| --- | --- | --- |
| `Limits::max_head_bytes` | 64 KiB | an h1 head or trailer section; a decoded h2/h3 field section (name + value + 32); an h2 header block; an h3 HEADERS payload |
| `Limits::max_fields` | 128 | fields in one section |
| `Limits::max_chunk_line` | 1 KiB | a chunk-size line with extensions |
| `Limits::max_frame_size` | 16 384 | an h2 frame; an h3 frame other than DATA and HEADERS |
| `stream_window` | 1 MiB | what one h2/h3 stream may send before it is read |
| `connection_window` | 4 MiB | what one h2/h3 connection may send before it is read |
| `ServerOptions::max_concurrent_streams` | 100 | streams a client may open at once |
| `ServerOptions::max_connections` | 1024 | connections served at once |
| `ServerOptions::accept_queue` | 64 | exchanges waiting for `accept` |
| `ClientOptions::max_idle_per_origin` | 4 | idle HTTP/1.1 connections per origin |
| `ClientOptions::max_resolved_addresses` | 8 | addresses tried for one name |

Fixed bounds: four queued body chunks per HTTP/1.1 exchange, 64 KiB of unread request body
drained before a connection is kept, 16 KiB per read, 256 KiB of queued HTTP/2 output before
the peer is read again.

## 10. The definition of done

| 0013 §4.7 clause | Verdict | Section |
| --- | --- | --- |
| 1. typed coverage of the primitive inventory | met: three versions, both roles | §2 |
| 2. transports implemented or refused | met | §3 |
| 3. security mechanisms implemented or refused | met | §4 |
| 4. options and bounds row by row | met | §5, §9 |
| 5. monitoring and native devices | met: tracing only, no devices by decision | §6, §7 |
| 6. parity document backed by interop against named versions | met | §8 |

## 11. Sources

- [`docs/research/http.md`](../research/http.md) §1-§3, §5, §8, §10, §11, §13.
- [0044](../decisions/0044-weida-http.md), [0013](../decisions/0013-competitor-libraries.md)
  §4.7.
- `crates/http/weida-http-codec/src/{h1,hpack,huffman,h2,qpack,h3,varint,uri}.rs`;
  `crates/http/weida-http/src/{client,server,body,h1,h2,h3,pool,dial,options,types}.rs`;
  `crates/http/weida-http/tests/io/{interop_hyper,interop_h3,loopback}.rs`.
