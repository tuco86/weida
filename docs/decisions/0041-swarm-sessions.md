# 0041 — SWARM sessions: pipelined orders, backpressure from caller to sender

- **Status:** accepted
- **Date:** 2026-10-19
- **Items:** B-344, B-345, B-346, B-347
- **Related:** [0040](0040-swarm-pattern.md) §4.2-§4.4 (amended here),
  [requirements/griasdi-swarm.md](../requirements/griasdi-swarm.md)

## 1. The question

griasdi fetches speech models and its own updates through SWARM. From a desktop on a 1 Gbit line
to its shared server (27 ms ping), one fetch of a 200 MiB file measured:

| asks per seeder (`FetchOptions::per_peer`) | swarm, MB/s |
| --- | --- |
| 2 (the 0040 default) | 4.2 - 8.2 |
| 8 | 9.6 |
| 16 | 8.2 - 10.7 |
| HTTPS download of the same file, same path | 53.7 |

The server's path report: with 2 asks the congestion window stayed at 163-193 KiB with 37-42
congestion events per 200 MiB; with 16 asks it reached 804 KiB with 3 events.

The cause is the wire of 0040 §4.2: every ask is its own exchange (`gather.rs` `ask_one` calls
`open_exchange_on` per ask). Between the end of one answer and the start of the next the seeder
has nothing to send, so its connection is application-limited and the congestion window never
grows past what a few asks fill. More asks help a little; the per-ask exchange stays the
bottleneck.

The owner's direction: a swarm fetch from one seeder should behave like a normal download, only
with pieces in any order, and how many orders to keep queued follows established BitTorrent
practice rather than something invented here. So the question is: **what wire and what
scheduling let one fetch fill a seeder's path, with backpressure from the caller's disk to the
QUIC sender and no unbounded queue.**

## 2. The evidence

- **BEP 3, the peer protocol:** "downloaders should keep several piece requests queued up at
  once ... (this is called 'pipelining')". Requests ride one connection, answered in order.
  <https://www.bittorrent.org/beps/bep_0003.html>
- **libtorrent, `peer_connection::update_desired_queue_size`** (RC_2_0
  `src/peer_connection.cpp`): the desired queue is `request_queue_time` (3 s) × payload download
  rate / block size, clamped to `[min_request_queue = 2, max_out_request_queue]`. While in slow
  start it grows by one per received piece, and it leaves slow start once a second's payload is
  no more than 5000 bytes above the second before. Defaults in `src/settings_pack.cpp`:
  `request_queue_time` 3, `max_out_request_queue` 500.
- **Receive windows.** quiche's `QuicFlowController::MaybeIncreaseMaxWindowSize` auto-tunes the
  receive window; quinn 0.11 does not: its `stream_receive_window` is fixed per connection at dial
  (`quinn-proto` 0.11.19 `config/transport.rs`), and its `send_window` defaults to 8 × 1.25 MB =
  10 MB. weida's default `stream_receive_window` of 1 MiB caps one stream near 1 MiB per RTT,
  about 35 MB/s at 30 ms.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — deeper per-ask pipelining | more asks in flight on the 0040 wire | the per-ask exchange stays the bottleneck, as measured: 16 asks gave 8.2-10.7 MB/s |
| B — sessions with a fixed W | one stream per (fetch, seeder), W orders queued | a fixed depth is either too shallow for a fast path or holds W pieces of memory for a slow one |
| **C — B, plus libtorrent's queue-time depth capped by W** | **the depth follows the seeder's rate, W is the hard cap** | **chosen** |
| D — the fetch names every missing piece, the seeder pushes | one order, the seeder streams | seeders join and leave during a fetch, so the same piece would be pushed by several of them, and nothing would bound what is in flight |

## 4. Decision

**Option C.** One bidirectional stream per (fetch, seeder), a *session*, carries small orders
one way and framed answers the other. The fetch holds a credit of W orders per seeder, from the
moment an order is written until the caller takes the piece from `Fetch::next`; how many of
those W it uses follows libtorrent's queue time. A seeder backpressures by not reading orders.
The version-1 ask of 0040 §4.2 stays served and is the fallback toward a seeder that does not
speak sessions.

### 4.1 The session wire (B-344)

The version-1 ask (0040 §4.2) is unchanged. A session is one exchange on the same request route;
its request body starts with an open and continues with orders, and its reply body is a sequence
of answers, one per order, in the order of the orders. All big-endian:

| unit | bytes | layout |
| --- | --- | --- |
| session open (start of the request body) | `OPEN_BYTES` = 48 | `u8` version = 2, `u8` piece log2, `[u8; 32]` root, `u64` size, 6 zero bytes |
| order (request body, after the open) | `ORDER_BYTES` = 5 | `u8` 1 LIST / 2 HAVE / 3 PIECE, `u32` index (0 for LIST and HAVE) |
| answer head (reply body) | `FRAME_HEAD_BYTES` = 9 | `u8` 0 DECLINE / 1 LIST / 2 HAVE / 3 PIECE, `u32` index (0 except PIECE), `u32` length (0 for DECLINE) |

An answer head is followed by exactly `length` bytes, which are the version-1 reply of that ask
(`count × 32`, `ceil(count / 8)`, the piece's span).

The 6 zero bytes make the open one byte longer than the longest version-1 ask (47). A version-1
seeder reads an ask under a 47-byte cap, so a session open overflows it at once and is refused
with `REJECTED`, instead of the fetch waiting out `header_timeout_ms` for a FIN that never comes.
That refusal before any answer head is how a fetch recognizes a version-1 seeder.

Refused: an open of another version, with non-zero padding, a piece log2 out of range, size 0 or
a blob `Blob::new` refuses; an order of an unknown kind, a non-zero index on LIST or HAVE, LIST
for a blob of at most one piece, PIECE at or past `count`; an answer head of kind > 3, a
non-zero index on DECLINE, LIST or HAVE, or a non-zero length on DECLINE.

### 4.2 The seeder: `SeedSession` (B-344)

`Seeder::accept() -> Result<SeedSession, Error>` replaces `SeedRequest`. A version-1 ask arrives
as a session of one order. `MAX_PENDING_ASKS` becomes `MAX_PENDING_SESSIONS` (64): sessions read
or waiting for `accept`, the surplus dropped. The open, or the version-1 ask, is read under
`header_timeout_ms`.

| Method | Meaning |
| --- | --- |
| `blob()`, `meta()`, `connection()` | what admission needs, asked once per session |
| `next() -> Result<Option<Ask>, Error>` | the next order; `Ok(None)` once the fetch finished the session. Reads one order only when called, with no timeout: an idle session is the backpressure. `Err(Protocol)` with an order still unanswered |
| `answer_list`, `answer_have`, `answer_piece` | answer the current order; the same shape checks as 0040 §4.3. A wrong answer cancels the session and returns `Err(Protocol)` |
| `decline(self)` | ends the session with a DECLINE frame (version 1: `Rejected`); the fetch drops this seeder |

Each answer is written under `SEED_STALL + (9 + len) / MIN_RATE`. A session never refuses a
well-formed open with `REJECTED`, so `REJECTED` before any answer head keeps meaning
"version-1 seeder" to the fetch.

### 4.3 The fetch: credits and queue-time depth (B-345)

Per seeder the fetch opens one session at its first order and writes orders into it. The
scheduler is libtorrent's, inside the cap W:

- `per_peer` is **W**, the credit: the most orders one seeder holds, from writing an order until
  the caller takes the piece (`Fetch::next` returns it). Pieces received and verified but not
  yet taken hold their credit, so a caller that stops taking stops new orders, the seeder stops
  getting work, and QUIC flow control stops the sender.
- The **depth** in use starts at `MIN_DEPTH` = 2 (`min_request_queue`) and, in slow start, grows
  by one per received piece. Payload is counted in one-second buckets; slow start ends once a
  closed second is no more than `SLOW_START_GAIN` = 5000 bytes above the one before. After that
  the depth is `ceil(QUEUE_TIME (3 s) × last second's bytes / piece size)`, clamped to
  `[MIN_DEPTH, W]`.
- `max_inflight` bounds orders across seeders and so the piece memory at `max_inflight` × piece
  size.
- Claims, the endgame, LIST once and the HAVE refresh of 0040 §4.4 stay. In the endgame the
  first copy wins; a duplicate that was already ordered in a session arrives and is dropped.
- Defaults: `per_peer` 8, `max_inflight` 32.
- `Fetch::in_flight() -> Bits`: pieces ordered and not yet taken.

The reader of a session checks each answer exactly as `ask_one` does (length, `PieceList::parse`,
`Bits::from_bytes`, `list.check`). The answer at the head of the line has the deadline
`max(written, previous answer read) + stall + (9 + len) / min_rate`.

| Session outcome | Seeder |
| --- | --- |
| `Rejected` before any answer head | version 1: the fetch asks it per exchange from now on (0040 §4.2) |
| a transport failure (`ConnectionLost`, `NotConnected`, `Indeterminate`) | keeps its place; its orders are given back |
| DECLINE, a mismatched head, a wrong payload, a passed deadline, an end with an order outstanding | dropped for this fetch |

**Receive windows.** Because quinn fixes them at dial, a swarm dials its QUIC connections with
`stream_receive_window` at least `SWARM_STREAM_WINDOW` (32 MiB) and `connection_receive_window`
at least `SWARM_CONNECTION_WINDOW` (64 MiB), whatever the runtime's `Limits` say; local
transports keep the runtime's. The seeder's in-flight bytes are bounded by quinn's `send_window`
(10 MB: 1 Gbit up to about 80 ms RTT).

### 4.4 Throughput proof (B-346)

A criterion bench (`benches/swarm.rs`) of one seeder serving 64 MiB in 1 MiB pieces over
loopback, and an ignored test (`tests/swarm_large.rs`) that moves 256 MiB through a 27 ms
delay proxy, compares it with one 256 MiB reply on the same path, and asserts at least 0.7 × that
baseline with peak RSS under 128 MiB.

### 4.5 The normative documents (B-347)

PATTERNS §6.5 (session API, routing, backpressure, wire, failure rows for the version-1
fallback and the stopped caller), INVARIANTS (the bounds above), ARCHITECTURE §7, STATUS, the
requirement, and a proposal to griasdi.

## 5. Backlog

### B-344 — SWARM session wire and the seeder's SeedSession
kind: code | size: 90 | status: ready | needs: []
acceptance: [0041](0041-swarm-sessions.md) §4.1-§4.2: `swarm/wire.rs` with the 48-byte open, 5-byte order and 9-byte answer head and their refusals (round trips and one refusal table); `IncomingTransfer::read_exact_into`; `SeedSession` (`next`, `answer_*`, `decline`) replacing `SeedRequest`, `MAX_PENDING_SESSIONS` 64, version-1 asks still served; every swarm integration test green on sessions; `grep -rn 'SeedRequest\|MAX_PENDING_ASKS' crates/` empty.

### B-345 — SWARM fetch over sessions, scheduled by queue time
kind: code | size: 90 | status: ready | needs: [B-344]
acceptance: [0041](0041-swarm-sessions.md) §4.3: `swarm/schedule.rs` (`Scheduler`, `MIN_DEPTH`, `QUEUE_TIME`, `SLOW_START_GAIN`, `claim` moved) with unit tests `an_order_is_written_only_when_a_piece_is_taken` and `the_depth_grows_in_slow_start_then_follows_the_rate`; `Fetch` over one session per seeder with the version-1 fallback; swarm dials with 32/64 MiB receive windows; `FetchOptions` defaults 8/32; `Fetch::in_flight`; integration tests `a_caller_that_stops_taking_pieces_stops_the_seeder` and `a_seeder_that_speaks_only_version_1_still_serves_a_fetch`; clippy `-p weida --no-default-features` clean.

### B-346 — SWARM throughput: 256 MiB session fetch and a bench
kind: measure | size: 60 | status: ready | needs: [B-345]
acceptance: [0041](0041-swarm-sessions.md) §4.4: `benches/swarm.rs` prints MiB/s for 64 MiB from one seeder; `tests/swarm_large.rs` (ignored) fetches 256 MiB through a 27 ms delay proxy at ≥ 0.7 × one 256 MiB reply on the same path with peak RSS < 128 MiB; both figures and the bench in IMPLEMENTATION verified results and the NIGHTLOG.

### B-347 — 0041 into the normative documents, griasdi proposal
kind: spec | size: 45 | status: ready | needs: [B-346]
acceptance: [0041](0041-swarm-sessions.md) §4.5: PATTERNS §6.5, INVARIANTS, ARCHITECTURE §7, STATUS §1/§4, requirements/griasdi-swarm.md on sessions; `grep -rn 'SeedRequest\|MAX_PENDING_ASKS' docs crates` only in 0040, BACKLOG and NIGHTLOG history; `PROPOSAL-weida-swarm-sessions.md` in griasdi's checkout.

## 6. What this note does not decide

- **Several sessions per seeder.** One stream per (fetch, seeder) is the BitTorrent shape; a
  path that one stream cannot fill is quinn's window question, not a second stream.
- **Auto-tuned receive windows.** quinn 0.11 has none; the swarm windows are fixed and raised.
- **Mirrors in `weida::blocking` and Python.** Unchanged from 0040 §6.

## 7. Sources

BEP 3 <https://www.bittorrent.org/beps/bep_0003.html>; libtorrent RC_2_0
`src/peer_connection.cpp` (`update_desired_queue_size`) and `src/settings_pack.cpp`; quiche
`QuicFlowController::MaybeIncreaseMaxWindowSize`; `quinn-proto` 0.11.19 `config/transport.rs`.
weida: `crates/weida/src/swarm/gather.rs` (`ask_one`), `swarm/seed.rs`, `transfer.rs`
(`IncomingRequest::take_body`, `reply`, `OutgoingTransfer::expire_at`), `pool.rs`
(`ClientPool::connect`), `stream.rs` (`PeerShared`, `dial_address`). griasdi's measurements are
from its proposal `PROPOSAL-griasdi-swarm-pipeline.md`, consumed by this note.
