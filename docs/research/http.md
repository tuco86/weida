# HTTP (HTTP/1.1, HTTP/2, HTTP/3; RFC 9110 semantics)

## 0. Identity card

- **Name:** the Hypertext Transfer Protocol: one set of semantics (RFC 9110) carried by three
  message syntaxes, HTTP/1.1 (RFC 9112), HTTP/2 (RFC 9113) and HTTP/3 (RFC 9114). [1][2][3][5]
- **Versions researched:** the June 2022 RFC set (9110, 9111, 9112, 9113, 9114, 9204), HPACK
  RFC 7541 (May 2015) and QUIC RFC 9000 (May 2021). [1]-[7]
- **Governing body:** the IETF HTTP working group (HTTPBIS) for the semantics and the three
  syntaxes; the QUIC working group for QUIC, HTTP/3 and QPACK. [1][5][7]
- **Specification documents:** RFC 9110 (semantics), RFC 9111 (caching), RFC 9112 (HTTP/1.1),
  RFC 9113 (HTTP/2), RFC 7541 (HPACK), RFC 9114 (HTTP/3), RFC 9204 (QPACK), RFC 9000 (QUIC). [1]-[8]
- **Reference implementation:** none is named by the IETF; interoperability is shown between
  independent implementations (curl, nghttp2, hyper, quiche, browsers). [10]-[13] [inference]
- **Wire type:** HTTP/1.1 is text (ASCII start line and field lines, CRLF-delimited, opaque body
  octets); HTTP/2 and HTTP/3 are binary frames whose field sections are compressed by HPACK and
  QPACK. [2][3][5]
- **Transports:** HTTP/1.1 over any reliable in-order byte stream, in practice TCP and TLS over
  TCP; HTTP/2 over TLS (ALPN `h2`) or cleartext TCP with prior knowledge; HTTP/3 over QUIC
  (ALPN `h3`). [2][3][5]

## 1. Connection and session lifecycle

- **HTTP/1.1:** a client opens a TCP connection (optionally TLS, ALPN `http/1.1`), writes a
  request, reads the response; connections are persistent by default in HTTP/1.1 and the
  `Connection: close` option ends them after the current exchange. [2] §9.3, §9.6
- An HTTP/1.0 connection is closed after each response unless both sides send
  `Connection: keep-alive` (a historical extension RFC 9112 Appendix C.2.2 describes). [2]
- A client may pipeline requests on a persistent connection; the server must answer them in
  the order received. [2] §9.3.2
- A request that waits on `Expect: 100-continue` lets the server answer `100 Continue` before
  the client sends the body, or a final status without reading it. [1] §10.1.1
- There is no keep-alive message in HTTP/1.1; an idle connection is closed by either side at
  any time, which races a request the client is writing. [2] §9.5
- **HTTP/2:** after TLS with ALPN `h2` the client sends the 24-octet connection preface
  `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` followed by SETTINGS; the server's preface is a SETTINGS
  frame; each SETTINGS is acknowledged. [3] §3.4, §6.5
- HTTP/2 multiplexes streams on one connection; client-initiated streams have odd ids,
  monotonically increasing. [3] §5.1.1
- PING is a liveness probe answered with the ACK flag; GOAWAY is the orderly close and names the
  last stream id the sender processed. [3] §6.7, §6.8
- **HTTP/3:** a QUIC connection (ALPN `h3`); each side opens one unidirectional control stream
  whose first frame is SETTINGS; each request is one client-initiated bidirectional stream.
  [5] §3, §6.2.1
- GOAWAY on the control stream starts an orderly close; QUIC's idle timeout and PING frames are
  the transport's, not HTTP/3's. [5] §5.2, [8] §10.1
- No version defines resumption of exchanges across connections; a client retries on a new
  connection only what it knows was not processed or is idempotent. [1] §9.2.2

## 2. Primitives

- A **connection** carries exchanges between one client and one server; in HTTP/1.1 one exchange
  at a time, in HTTP/2 and HTTP/3 many concurrent streams. [2][3][5]
- A **request** has a method, a target URI, header fields, content and optional trailer fields;
  a **response** has a status code (100-599 in use, three digits on the wire), fields, content
  and trailers. [1] §3.9, §6
- A **stream** (HTTP/2, HTTP/3) carries exactly one request and its response. [3] §5, [5] §4.1
- **Pseudo-header fields** (`:method`, `:scheme`, `:authority`, `:path`, `:status`) carry the
  control data in HTTP/2 and HTTP/3 and precede regular fields. [3] §8.3, [5] §4.3
- An **origin** is scheme, host and port; it scopes connection reuse. [1] §4.3.1
- An **intermediary** (proxy, gateway, tunnel) can sit between client and origin server; HTTP/2
  and HTTP/3 connections are hop-by-hop. [1] §3.7

## 3. Message model

- **HTTP/1.1 framing:** start line, field lines, empty line, then content framed by
  `Transfer-Encoding: chunked`, by `Content-Length`, or, for a response only, by connection close.
  [2] §6.3
- A message with both `Transfer-Encoding` and `Content-Length` is a request-smuggling vector;
  a server may reject it and must close the connection after it. [2] §6.1, §11.2
- Responses to HEAD, 1xx, 204 and 304 carry no content regardless of the fields. [2] §6.3
- Chunked content is a sequence of hex-sized chunks with optional extensions, a zero-size last
  chunk and optional trailer fields. [2] §7.1
- Obsolete line folding (a field line starting with SP or HTAB) must be rejected with 400 in a
  request, or replaced with SP, by a server; a user agent receiving it in a response must replace
  it. [2] §5.2
- No whitespace is allowed between a field name and its colon; a server must reject such a
  request with 400. [2] §5.1
- Field names are tokens (`tchar`), case-insensitive; values are VCHAR, obs-text, SP and HTAB,
  without leading or trailing whitespace. [1] §5.1, §5.5
- **HTTP/2 framing:** a 9-octet frame header (24-bit length, 8-bit type, 8-bit flags, 31-bit
  stream id) and a payload of at most SETTINGS_MAX_FRAME_SIZE (16 384 by default, 16 777 215
  at most). [3] §4.1, §4.2
- Field sections are HPACK blocks in HEADERS plus CONTINUATION frames; no other frame may
  interleave until END_HEADERS. [3] §4.3, §6.10
- Field names must be lowercase in HTTP/2 and HTTP/3; connection-specific fields (`Connection`,
  `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`, `Upgrade`) are malformed, and `TE` may
  only carry `trailers`. [3] §8.2.2, [5] §4.2
- **HTTP/3 framing:** frames of type and length as QUIC variable-length integers; DATA and
  HEADERS on request streams, SETTINGS and GOAWAY on the control stream. [5] §7
- **HPACK** encodes fields against a 61-entry static table and a FIFO dynamic table whose size is
  name + value + 32 octets per entry; strings may be Huffman-coded with the static code of
  Appendix B. [4] §2.3, §4.1, §5.2
- **QPACK** uses a 99-entry static table and a dynamic table updated on dedicated encoder and
  decoder streams; an encoder that never uses the dynamic table writes Required Insert Count 0.
  [6] §2.1, §4.5.1
- A QUIC variable-length integer is 1, 2, 4 or 8 octets, the two high bits of the first octet
  giving the length, values up to 2^62-1. [8] §16

## 4. Patterns and topologies

- **Request/response** is the only pattern: a client sends one request, a server sends zero or
  more interim (1xx) responses and one final response. [1] §3.3
- Server push (HTTP/2 PUSH_PROMISE, HTTP/3 PUSH_PROMISE) lets a server send a response to a
  request it predicts; a client disables it with SETTINGS_ENABLE_PUSH 0 (HTTP/2) or by never
  sending MAX_PUSH_ID (HTTP/3). [3] §8.4, [5] §4.6
- The client always connects; the server always binds. An idle client with nothing to request
  holds no connection. [1] §3.3
- Alternative services (`Alt-Svc`, RFC 7838) advertise that an origin is also reachable by
  another protocol, which is how a client usually discovers HTTP/3. [5] §3.1.1

## 5. Flow control and backpressure

- **HTTP/1.1:** none of its own; TCP's receive window is the backpressure, and a reader that stops
  reading stops the writer. [2] [inference]
- **HTTP/2:** credit in octets of DATA payload, per stream and per connection; the initial stream
  window is SETTINGS_INITIAL_WINDOW_SIZE (65 535 by default), the connection window starts at
  65 535 and is raised only by WINDOW_UPDATE. [3] §5.2, §6.9
- A sender must not send DATA beyond either window; a receiver treats a window above 2^31-1 as
  FLOW_CONTROL_ERROR. A change of SETTINGS_INITIAL_WINDOW_SIZE adjusts every open stream's window
  by the difference, possibly below zero. [3] §6.9.1, §6.9.2
- SETTINGS_MAX_CONCURRENT_STREAMS caps open streams the peer may initiate; streams beyond it are
  refused with RST_STREAM REFUSED_STREAM. [3] §5.1.2
- **HTTP/3:** QUIC stream and connection credit (MAX_STREAM_DATA, MAX_DATA) and stream count
  credit (MAX_STREAMS); HTTP/3 adds no flow control. [5] §4.1, [8] §4

## 6. Delivery guarantees and acknowledgement

- The final response is the only acknowledgement; it states the result of processing, not
  durability. [1] §15
- Safe methods (GET, HEAD, OPTIONS, TRACE) and idempotent methods (those plus PUT, DELETE) may be
  retried automatically; POST and PATCH may not unless the client knows the request was not
  processed. [1] §9.2.1, §9.2.2
- An HTTP/2 GOAWAY's last stream id and RST_STREAM REFUSED_STREAM tell the client which streams
  were not processed and are safe to retry. [3] §6.8, §8.7
- HTTP/3 offers the same through GOAWAY and H3_REQUEST_REJECTED. [5] §5.2, §8.1

## 7. Ordering and duplicates

- Pipelined HTTP/1.1 responses come in request order. [2] §9.3.2
- HTTP/2 and HTTP/3 promise no order between streams; within a stream, frames are ordered. [3]
  §5, [5] §4.1
- Duplicates arise only from retries; the protocol has no duplicate detection. [1] §9.2.2
  [inference]

## 8. Failure behaviour

| event | what each side observes | lost or ambiguous |
| --- | --- | --- |
| HTTP/1.1 connection closed during a response | client reads a short body (Content-Length) or no last chunk | the response; a non-idempotent request's outcome is unknown |
| HTTP/1.1 idle connection closed by the server as a request is written | client sees a reset or EOF before any response | the request was not processed only if no octet of a response arrived [inference] |
| HTTP/2 RST_STREAM | that stream ends with an error code | only that exchange; REFUSED_STREAM means not processed |
| HTTP/2 GOAWAY | streams above the last id were not processed | streams at or below may complete |
| HTTP/2 connection error | GOAWAY with the code, then close | every open stream |
| HTTP/3 stream reset (RESET_STREAM / STOP_SENDING) | the request or response half ends with an H3 code | that exchange; H3_REQUEST_REJECTED means not processed |
| HTTP/3 connection error | CONNECTION_CLOSE with an H3 or QPACK code | every open stream |
| oversized head | 431 Request Header Fields Too Large (HTTP/1.1) or a stream error | the request |
| malformed request | 400; HTTP/2 and HTTP/3 a stream PROTOCOL_ERROR / H3_MESSAGE_ERROR | the request |

Sources: [1] §15.5.20, [2] §9.6, [3] §5.4, §6.4, §6.8, [5] §8, [9].

## 9. Reliability recipes

- **Retry of a refused request:** a client retries on a new connection a request that GOAWAY or
  REFUSED_STREAM marked unprocessed; cost one round trip. [3] §8.7
- **Graceful shutdown:** a server sends GOAWAY (HTTP/2 often twice: first with 2^31-1, then the
  real last id) and finishes in-flight streams before closing. [3] §6.8
- **Conditional requests and caching** (RFC 9111) avoid re-transfer; out of scope for a
  transport sheet. [1] §13

## 10. Security and identity

- TLS authenticates the server by certificate; ALPN selects `http/1.1`, `h2` or `h3`. [3] §3.2,
  [5] §3.1
- HTTP/2 over TLS requires TLS 1.2 or later with a cipher-suite blocklist, and forbids TLS
  renegotiation and compression. [3] §9.2
- Client authentication is in-band (`Authorization`) or by TLS client certificate; identity
  travels per request in fields. [1] §11
- HPACK and QPACK let an encoder mark a field never-indexed so that an intermediary does not put
  a secret into a compression context (CRIME-class attacks). [4] §7.1, [6] §7.1

## 11. Limits and resource bounds

- RFC 9110 sets no limit on field section size, field count or content length; it lets a
  recipient refuse with 413, 414 or 431. [1] §15.5.14, §15.5.15, [9]
- HTTP/2 SETTINGS_MAX_HEADER_LIST_SIZE advises the largest field section the sender accepts,
  counted as name + value + 32 per field; unlimited by default. [3] §6.5.2
- HTTP/2 SETTINGS_MAX_FRAME_SIZE: 16 384 default, between 16 384 and 16 777 215. [3] §6.5.2
- HTTP/2 SETTINGS_HEADER_TABLE_SIZE: the decoder's dynamic table limit, 4 096 by default. [3]
  §6.5.2, [4] §4.2
- HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS: unlimited by default; recommended no lower than 100.
  [3] §6.5.2
- HTTP/3 SETTINGS_MAX_FIELD_SECTION_SIZE: as HTTP/2's header list size. [5] §4.2.2, §7.2.4.1
- QPACK SETTINGS_QPACK_MAX_TABLE_CAPACITY and SETTINGS_QPACK_BLOCKED_STREAMS: 0 by default.
  [6] §5
- HPACK integers and string lengths are unbounded in the encoding; a decoder must bound them.
  [4] §5.1, §7.4 [inference]

## 12. Answers to the problem catalogue

- **P1:** loss is a connection or stream error; a safe retry is limited to safe or idempotent
  methods or to requests known unprocessed. [1] §9.2.2, [3] §8.7
- **P2:** HTTP/1.1 has no liveness probe; HTTP/2 PING; HTTP/3 relies on QUIC's idle timeout.
  [3] §6.7, [8] §10.1
- **P3:** no answer in this protocol.
- **P4:** the backlog sits in TCP buffers (HTTP/1.1) or within the HTTP/2/QUIC windows; at the
  bound the sender blocks. [3] §5.2
- **P5:** no answer in this protocol (caching RFC 9111 is the nearest). 
- **P6:** no failover in the protocol; a client dials again. [inference]
- **P7:** no answer in this protocol.
- **P8:** per HTTP/1.1 connection (pipelining) and per stream. [2] §9.3.2
- **P9:** duplicates from retries; no suppression. [1] §9.2.2
- **P10:** a reply is the response on the same connection (HTTP/1.1) or stream. [3] §8.1
- **P11:** the client connects, the server binds; DNS and Alt-Svc are discovery. [1] §4.3
- **P12:** octets per stream and per connection (HTTP/2, QUIC); none in HTTP/1.1. [3] §5.2
- **P13:** content is streamed; no maximum in the protocol; chunked when length is unknown. [2]
  §7.1
- **P14:** TLS server certificates; per-request credentials in fields. [1] §11
- **P15:** field sections, frame sizes, stream counts, compression tables; limits as §11. [9]
- **P16:** status codes; no delivery receipts. [1] §15
- **P17:** GOAWAY and in-flight completion; HTTP/1.1 `Connection: close`. [3] §6.8, [2] §9.6
- **P18:** TCP, TLS over TCP, QUIC. [2][3][5]

## 13. Ecosystem

- **hyper** (Rust): HTTP/1.1 and HTTP/2 client and server on tokio, 1.x stable since 2023. [10]
- **h2** (Rust): the HTTP/2 implementation under hyper. [10]
- **h3** and **h3-quinn** (Rust): HTTP/3 over quinn, versions 0.0.x (pre-1.0). [11]
- **nghttp2** (C): the HTTP/2 library behind curl and many servers; **nghttp3** and **ngtcp2**
  provide HTTP/3 and QUIC for curl. [12]
- **quiche** (Rust, Cloudflare): QUIC and HTTP/3. [13]
- **reqwest** (Rust): a client on hyper with redirects, cookies, proxies and compression. [10]
- Known incompatibilities: HTTP/2 implementations differ on h2c upgrade support and on how
  strictly they enforce the TLS cipher blocklist; HTTP/3 implementations differ on QPACK
  dynamic-table use. [inference]

## 14. Sources

1. RFC 9110, HTTP Semantics, June 2022. https://www.rfc-editor.org/rfc/rfc9110 — semantics,
   fields, methods, status codes, retries.
2. RFC 9112, HTTP/1.1, June 2022. https://www.rfc-editor.org/rfc/rfc9112 — message syntax,
   framing, persistence, pipelining.
3. RFC 9113, HTTP/2, June 2022. https://www.rfc-editor.org/rfc/rfc9113 — frames, streams, flow
   control, SETTINGS.
4. RFC 7541, HPACK: Header Compression for HTTP/2, May 2015.
   https://www.rfc-editor.org/rfc/rfc7541 — integer and string coding, tables, Huffman code.
5. RFC 9114, HTTP/3, June 2022. https://www.rfc-editor.org/rfc/rfc9114 — streams, frames,
   SETTINGS, errors.
6. RFC 9204, QPACK: Field Compression for HTTP/3, June 2022.
   https://www.rfc-editor.org/rfc/rfc9204 — static table, field line representations, settings.
7. RFC 9111, HTTP Caching, June 2022. https://www.rfc-editor.org/rfc/rfc9111 — named only.
8. RFC 9000, QUIC, May 2021. https://www.rfc-editor.org/rfc/rfc9000 — §16 variable-length
   integers, §4 flow control, §10 idle timeout.
9. RFC 6585, Additional HTTP Status Codes, April 2012. https://www.rfc-editor.org/rfc/rfc6585 —
   431, as incorporated by RFC 9110 §15.5.
10. hyperium, `hyper` 1.x and `h2` 0.4, read 2026-10-22. https://github.com/hyperium/hyper —
    ecosystem.
11. hyperium, `h3` 0.0.8 and `h3-quinn` 0.0.10, read 2026-10-22. https://github.com/hyperium/h3 —
    ecosystem.
12. nghttp2.org, nghttp2, nghttp3, ngtcp2, read 2026-10-22. https://nghttp2.org — ecosystem.
13. Cloudflare, `quiche`, read 2026-10-22. https://github.com/cloudflare/quiche — ecosystem.
