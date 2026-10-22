# 0044 — weida-http: HTTP as the sixth protocol family

- **Status:** accepted
- **Date:** 2026-10-22
- **Items:** B-360 to B-371
- **Related:** [0013](0013-competitor-libraries.md) §4, [0032](0032-identity-sources-and-the-handoff.md) §4.3

## 1. The question

The workspace's only HTTP stack is `reqwest`, in `weida-openbao`
([0032](0032-identity-sources-and-the-handoff.md) §4.3). It adds about 90 crates on top of the 84
that `weida` itself links, among them hyper, tower, tower-http, url, idna, the icu crates and
encoding_rs, for one client that sends JSON requests to one server. `weida` has no HTTP
dependency, and nothing in the workspace uses axum or any other server framework.

The owner wants HTTP to be a protocol family like zmq, nng, mqtt, amqp and nats: a sans-I/O
codec, a library with both roles, interop proved against an upstream implementation, and a
binding later. So the question is: **what does a weida HTTP family consist of, which versions
does it speak, and what does it leave out.**

## 2. The evidence

- HTTP semantics are RFC 9110 (June 2022), HTTP/1.1 message syntax RFC 9112 (June 2022),
  HTTP/2 RFC 9113 (June 2022) with HPACK RFC 7541 (May 2015), HTTP/3 RFC 9114 (June 2022) with
  QPACK RFC 9204 (June 2022), and the variable-length integer of QUIC RFC 9000 §16 (May 2021).
- RFC 9204 §4.2 lets an endpoint that never uses the dynamic table open no encoder or decoder
  stream; advertising `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0` and
  `SETTINGS_QPACK_BLOCKED_STREAMS = 0` makes the peer's encoder use the static table only.
- RFC 9113 §3.3 starts h2 over TLS by ALPN `h2`; h2 over cleartext (h2c, prior knowledge) is
  a separate deployment that no browser uses.
- `cargo tree -e normal -p weida-openbao --prefix none | sort -u | wc -l` gives **178** crates
  at `c9f902e`; the same for `-p weida` gives **84**.
- quinn, rustls, tokio-rustls and `ring` are already in the workspace graph through `weida`
  and the TLS transports of `weida-amqp`, `weida-nats` and `weida-nng`, so an own library adds
  the codec and no transport stack.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — keep reqwest | `weida-openbao` as today | ninety crates for one JSON client; no server; no family a user can take alone |
| B — wrap hyper, h2 and h3 | a thin library over the upstream crates | the weight stays (hyper, tower, http, h2, h3); the codec is not ours to bound, fuzz or cap by argument; interop proves one implementation against itself |
| **C — own codecs plus own library** | **`weida-http-codec` with an empty `[dependencies]`, `weida-http` on `weida-runtime`** | **chosen** |

## 4. Decision

**Option C.** Two crates under `crates/http/`, interop against hyper (h1, h2) and h3/h3-quinn
(h3) as dev-dependencies only.

- **Two crates, both published:** `weida-http-codec` with an empty `[dependencies]`;
  `weida-http` on `weida-core`, `weida-runtime`, `tokio`, `bytes`, `tracing` and the codec, never
  on `weida` or `weida-protocol`. Features: `default = ["tls"]`;
  `tls = ["dep:tokio-rustls", "dep:rustls"]`, which enables https and h2;
  `h3 = ["tls", "dep:quinn"]`, opt-in, so a default build links no QUIC stack. No `blocking`
  feature. Both are published because `weida-openbao` is, and a published crate cannot depend
  on an unpublished one.
- **The codec's caps are arguments** (`Limits` by value) and are checked from a declaration
  before anything is allocated ([0013](0013-competitor-libraries.md) §4.3).
- **HTTP/2 only over TLS by ALPN `h2`; no h2c.**
- **HTTP/3 only when asked** (`Protocol::Http3`); no Alt-Svc.
- **The QPACK dynamic table is never used**: we advertise `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0`
  and `SETTINGS_QPACK_BLOCKED_STREAMS = 0`, open no encoder or decoder stream (RFC 9204 §4.2
  permits it), drain the peer's encoder and decoder streams unparsed, and a field section whose
  Required Insert Count is not 0 is `QPACK_DECOMPRESSION_FAILED` (0x0200).
- **No server push**: the client sends `SETTINGS_ENABLE_PUSH = 0` (h2) and never sends
  MAX_PUSH_ID (h3); the server never pushes.
- **The trust anchors are the caller's**: the client takes `Arc<rustls::ClientConfig>`, the
  server `Arc<rustls::ServerConfig>`. The library clones each and sets only `alpn_protocols`.
- **Pull-style server**, as weida's patterns: `Server::accept()` yields an `Exchange`. No handler
  closures, no tower.
- **The client follows no redirects** and stores no cookies; 3xx is returned to the caller.
- **The origin server adds `date`** (RFC 9110 §6.6.1 MUST) when the application set none.
- **Only obs-fold is refused where RFC 9112 allows replacing it**: obs-fold in a response is
  refused too, so one parser serves both directions. The departure is pinned by a golden vector.

## 5. Backlog

### B-360 — HTTP research sheet
kind: research | size: 60 | status: ready | needs: []
acceptance: `docs/research/http.md` in the research template (§0-§14), protocol-native vocabulary, every claim sourced (RFC 9110/9112/9113/7541/9114/9204/9000 with dates), README "Sheets" row added.

### B-361 — `weida-http-codec`: HTTP/1.1, fields, URLs, dates
kind: protocol | size: 90 | status: ready | needs: []
acceptance: `crates/http/weida-http-codec` with an empty `[dependencies]`; `limits`, `error`, `fields`, `uri`, `date`, `h1`; golden vectors from RFC 9112/9110 examples both directions; fuzz workspace with `h1_request`, `h1_response`, `chunked`, `uri` and their `fuzz_smoke` mirrors; caps checked before allocation.

### B-362 — `weida-http-codec`: HPACK
kind: protocol | size: 90 | status: ready | needs: [B-361]
acceptance: `prefix`, `huffman`, `hpack`; RFC 7541 Appendix C.1-C.6 decode and encode byte-exact (C.2 decode only); fuzz target `hpack` and mirror.

### B-363 — `weida-http-codec`: HTTP/2 frames
kind: protocol | size: 60 | status: ready | needs: [B-361]
acceptance: `h2`; every frame type encodes and decodes; length, stream-id and SETTINGS value rules refused by name; fuzz target `h2_frame` and mirror.

### B-364 — `weida-http-codec`: QUIC varints, QPACK (static), HTTP/3 frames
kind: protocol | size: 90 | status: ready | needs: [B-362]
acceptance: `varint`, `qpack`, `h3`; RFC 9000 §A.1 varint examples, RFC 9204 Appendix B.1 field section decoded; a Required Insert Count above 0 refused; fuzz targets `qpack`, `h3_frame` and mirrors.

### B-365 — `weida-http`: types, dial, HTTP/1.1 client and pool
kind: protocol | size: 90 | status: ready | needs: [B-361]
acceptance: `crates/http/weida-http`; the client speaks HTTP/1.1 over TCP and TLS, keeps idle connections per origin; unit tests over `tokio::io::duplex`; `tests/io/interop_hyper.rs` h1 rows against a hyper 1 server; CI doc loops list `weida-http`.

### B-366 — `weida-http`: HTTP/1.1 server
kind: protocol | size: 90 | status: ready | needs: [B-365]
acceptance: `Server`, `Exchange`, `Responder`; keep-alive, pipelined requests answered in order, 100-continue, 400/431/501/505 refusals, header timeout; hyper 1 client interop rows; our client against our server in `tests/io/loopback.rs`.

### B-367 — `weida-http`: HTTP/2 client and server
kind: protocol | size: 90 | status: ready | needs: [B-363, B-366]
acceptance: one connection driver for both roles; ALPN `h2`; flow control both directions; GOAWAY and RST_STREAM; hyper 1 http2 interop rows both roles; the pool shares one h2 connection per origin.

### B-368 — `weida-http`: HTTP/3 client and server (feature `h3`)
kind: protocol | size: 90 | status: ready | needs: [B-364, B-367]
acceptance: HTTP/3 over quinn; control stream SETTINGS, no QPACK streams opened; h3 0.0.8 + h3-quinn 0.0.10 interop rows both roles; LOOP §6 and `.zeughaus-ci` feature row `weida-http:h3`.

### B-369 — `weida-openbao` onto `weida-http`; `reqwest` removed
kind: code | size: 60 | status: ready | needs: [B-367]
acceptance: every existing `weida-openbao` test passes unchanged; `cargo tree -e normal -p weida-openbao` lists none of hyper, reqwest, tower, url, idna; the root `reqwest` entry and its comment are gone.

### B-370 — HTTP parity document and the family's documents
kind: spec | size: 45 | status: ready | needs: [B-368, B-369]
acceptance: `docs/libraries/http.md` in the eleven-section shape; every listed document names the sixth family; `git grep -n 'five foreign-protocol\|all five libraries\|twenty-five' -- README.md docs` finds no stale count.

### B-371 — `weida-http-py`
kind: protocol | size: 90 | status: ready | needs: [B-366]
acceptance: `crates/http/weida-http-py` on `weida-py-core` with an asyncio and a synchronous surface over `Client` and `Server`, the shape of `weida-nats-py`; `docs/libraries/http-py.md`; excluded from the darwin/windows checks like the other bindings.

## 6. What this note does not decide

- **The Python binding's API** (B-371).
- **Compression** (gzip, br): a body is the caller's bytes.
- **Cookies.**
- **Redirects followed by the client.**
- **Proxies.**
- **Alt-Svc discovery.**
- **h2c.**
- **Server push.**
- **CONNECT and extended CONNECT, WebTransport, `Upgrade`/WebSocket.**
- **The QPACK dynamic table.**

A later ask for any of these is a new decision, not a silent extension.

## 7. Sources

RFC 9110, RFC 9112, RFC 9113, RFC 9114, RFC 9204 (all June 2022), RFC 7541 (May 2015), RFC 9000
§16 (May 2021). `cargo tree` of `weida-openbao` and `weida` at `c9f902e`.
