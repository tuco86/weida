# 0040 — SWARM: many sources, one verified blob

- **Status:** accepted; §4.2-§4.4 amended by [0041](0041-swarm-sessions.md)
- **Date:** 2026-10-17
- **Items:** B-340, B-341, B-342, B-343
- **Related:** [requirements/griasdi-swarm.md](../requirements/griasdi-swarm.md),
  [0001](0001-sequence-field.md) Option D, [0023](0023-completion-is-a-cursor.md),
  [0020](0020-cluster-and-discovery.md) §4.5

## 1. The question

griasdi asks weida for a whole piece swarm: a BLAKE3 piece tree, an on-disk blob store, a
seeder, a resumable multi-source fetcher and a tracker
([requirements/griasdi-swarm.md](../requirements/griasdi-swarm.md) (a)-(e)). The owner does not
want a half-finished P2P system inside the transport. He wants the **pattern** that lets a weida
user build something torrent-like, with storage out of scope.

So the question is: **what transport-level pattern lets a weida user build a torrent-like
system, when storage and discovery stay the application's.**

## 2. The evidence, condensed

Verified at `1339b3b`.

**2.1 Req/Rep picks one peer, and the application cannot choose it.** `PeerShared::try_pick`
(`crates/weida/src/stream.rs:130`) picks one live slot round-robin per exchange. SURVEY sends one
body to every peer. Choosing the peer of an exchange is crate-private: `open_exchange_on`
(`stream.rs:1032`) opens an exchange on a given connection, and `Peer::live_peers`
(`stream.rs:710`) lists the slots. An application can target one peer only by building one
`Requester` per peer.

**2.2 A piece tree whose root is the plain BLAKE3 hash.** `blake3::hazmat` (1.8.7, an
unconditional module) gives position-bound chaining values: `HasherExt::set_input_offset`,
`finalize_non_root`, `merge_subtrees_non_root`, `merge_subtrees_root` and `left_subtree_len`.
With them a list of per-piece chaining values merges to a root equal to `blake3::hash(blob)` for
any power-of-two piece of at least 1 KiB (one BLAKE3 chunk). griasdi proved this in
`griasdi-proto/src/pieces.rs` (test `the_piece_tree_has_the_root_of_the_whole_stream`). So the
blob id is the ordinary BLAKE3 hash of the bytes, and a piece checks against the root without
any other trusted input.

**2.3 Bulk stays out of consensus.** [INVARIANTS.md](../INVARIANTS.md) lines 28-29: "Raft
coordinates control state, not bulk payload transport"; "Payload replication remains
stream-oriented". A catalog in Raft names roots; the swarm moves the bytes.

**2.4 The three topologies the pattern has to carry.** Each maps onto it with no griasdi logic
in weida:

- **august Raft group.** The manifest (the `Blob`s) is replicated through Raft. Every member
  binds a `Seeder` and dials a `Swarm` to every other member's URL. The node that received the
  push answers HAVE with full bits; the others answer with what they have so far, and the 5 s
  HAVE refresh lets them fetch from each other while they are still filling. No tracker is
  needed.
- **griasdi-server.** The `Blob`s arrive in august's update notification. Each server binds a
  `Seeder` and dials a `Swarm` to every august member plus a limited subset of other servers.
  august chooses that subset with its own Req/Rep tracker, which is application logic.
  `FetchOptions::max_peers` caps how many of them take pieces at once.
- **griasdi-desktop.** Only a `Swarm`, dialled to every server the user is connected to. A
  desktop never binds a `Seeder` and never talks to another desktop.

In all three the side that holds pieces is reachable and the side that wants them dials, so
connections run in one direction. Because the pool keys a connection by peer **and path**
(`crates/weida/src/pool.rs:67-70`, `ConnKey = (PeerKey, String)`), a swarm path gets its own QUIC
connection beside the voice connection to the same server. Bulk pieces therefore never share
flow-control windows with voice (the separation of
[0002](0002-control-and-bulk-separation.md)).

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — no pattern | applications write it on Req/Rep | every application rewrites scheduling, endgame and verification, and needs one `Requester` per peer |
| B — the proposal as written | store, fetcher, tracker, announcer in weida | storage and discovery policy inside the transport; the owner rejected it |
| **C — the SWARM pattern** | **`Seeder` plus `Swarm`, the piece tree; storage and discovery the application's** | **chosen** |
| D — C as a separate crate `weida-swarm` | the same API outside `weida` | it cannot reach the per-peer exchange without making `open_exchange_on` and the slot list public |

## 4. Decision

**Option C.** One new pattern, SWARM, in the `weida` crate behind the off-by-default feature
`swarm`. The bound side, `Seeder` (`Listener::seeder`), hands every well-formed ask to the
application through an accept loop and sends back what the application answers from its own
storage. The dialling side, `Swarm` (`Runtime::swarm`), fetches one content-addressed blob from
every connected seeder at once and verifies each piece against the BLAKE3 root before it hands
the piece to the application. The application stores what it is handed, decides which blobs it
serves, and finds its seeders itself.

### 4.1 The piece tree and the feature (B-340)

`blake3 = "1.8"` is a workspace dependency, linked only through `weida`'s feature
`swarm = ["dep:blake3"]`. The module `crates/weida/src/swarm.rs` with `swarm/tree.rs` exports:

| Item | Shape |
| --- | --- |
| `MAX_PIECES` | `1 << 20`: the piece list is at most 32 MiB, a bit map at most 128 KiB |
| `BlobId` | 32 bytes, the BLAKE3 root; `of(bytes)`, `from_bytes`, `as_bytes`; `Display`/`FromStr` as 64 hex digits (`ParseBlobIdError`) |
| `PieceSize` | a power of two from `MIN` (1 KiB) to `MAX` (16 MiB), `DEFAULT` 1 MiB; held as its log2 |
| `Blob` | root, size, piece size: the torrent-file analogue. `new` refuses more than `MAX_PIECES` pieces and an empty blob whose root is not `BlobId::of(b"")`; `count`, `span(index)` |
| `PieceList` | the per-piece chaining values; `of`, `parse` (refuses a list that does not merge to the root), `to_bytes`, `check(index, piece)` |
| `TreeHasher` | incremental `update`/`finish`, bounded at `MAX_PIECES` |
| `Bits` | which pieces; bit `i` is byte `i/8`, mask `1 << (i % 8)`; `from_bytes` refuses a wrong length and a set padding bit |

A piece's chaining value is BLAKE3 at input offset `index × piece` finalized as a non-root node;
lists merge by `left_subtree_len`, so the root is `blake3::hash(blob)` (§2.2). A blob of one
piece has no list: its piece is checked against the root directly. An empty blob has no pieces.

### 4.2 The ask wire (B-341)

One exchange per ask on the request route. The request body, big-endian:

| offset | field |
| --- | --- |
| 0 | `u8` version = 1 |
| 1 | `u8` ask: 1 = LIST, 2 = HAVE, 3 = PIECE |
| 2 | `u8` piece log2 (10..=24) |
| 3 | `[u8; 32]` root |
| 35 | `u64` size |
| 43 | `u32` index (PIECE only) |

LIST and HAVE are 43 bytes, PIECE 47. The reply is the raw answer with an exact length the asker
knows in advance: LIST `count × 32`, HAVE `ceil(count / 8)`, PIECE the piece's span. A body of
another length, another version, an unknown ask, a piece size out of range, a `Blob` that
`Blob::new` refuses, size 0, LIST for a blob of at most one piece, or PIECE past the end is
refused with `Rejected` before the application sees it.

### 4.3 The seeder (B-341)

`Listener::seeder(path) -> Result<Seeder, Error>` registers the same request route a replier
does, so the connection layer needs no change. `Seeder::accept() -> Result<SeedRequest, Error>`
yields one decoded ask: `blob()`, `ask()` (`Ask::List`, `Ask::Have`, `Ask::Piece(index)`),
`meta()` and `connection()`, which is what the application's admission needs. It answers with
`answer_list(&PieceList)`, `answer_have(&Bits)`, `answer_piece(&[u8])` or `decline()`. An answer
of the wrong kind or shape is refused with `Internal` and returned as `Error::Protocol`; a piece
is not verified on the seeder, the fetcher verifies it.

Bounds: at most `MAX_PENDING_ASKS` (64) asks are read or waiting for `accept` at once; more are
dropped and their asker sees no reply. An ask body is read under `Limits::header_timeout_ms`.
An answer's write expires after `SEED_STALL` (30 s) plus its length at `MIN_RATE` (64 KiB/s).

### 4.4 The fetch (B-342)

`Runtime::swarm(tls) -> Swarm` dials like a `Surveyor`: `connect`, `disconnect`, `peer_count`,
`connection_stats`, `events`. `Swarm::fetch(blob, have, options) -> Fetch` starts one fetch;
`Fetch::next() -> Option<Result<Piece, Error>>` is cancel-safe and yields each verified
`Piece { index, bytes }` once, in any order, until every piece the caller lacked is delivered.
`Fetch::list()` hands out the verified piece list so the application can persist it and resume
without LIST; `delivered()` is the running bit map. Dropping the `Fetch` aborts every ask.

Per connected seeder the fetch asks HAVE, then claims pieces that seeder holds: uniformly among
pieces no one is fetching, and, only when every missing piece is in flight, the least-copied
one (endgame). The first copy that verifies wins and the others are aborted. LIST is asked once,
of any seeder, and checked against the root. A seeder with nothing to give is asked HAVE again
every `have_refresh`, so seeders that are still filling serve each other.

`FetchOptions`: `list` (a list the caller already holds), `stall` (30 s), `min_rate`
(64 KiB/s), `per_peer` (2), `max_inflight` (16), `max_peers` (unbounded), `have_refresh` (5 s).
Each ask has the deadline `stall + reply length / min_rate`.

A seeder whose ask fails at the transport (`ConnectionLost`, `NotConnected`, `Indeterminate`)
keeps its place and its pieces are asked elsewhere; a redial brings it back. A seeder that
declines, stalls past its deadline, answers with a wrong length or sends bytes that do not
verify is dropped for this fetch. When no task runs and no usable seeder is left, `next()`
returns `Err(NotConnected)`; connecting a seeder and calling `next()` again resumes.

### 4.5 The normative documents (B-343)

[PATTERNS.md](../PATTERNS.md) gets §6.5 SWARM with its table, the wire and the failure table,
and a §7 row; [INVARIANTS.md](../INVARIANTS.md) the bounds of §4.3 and §4.4;
[ARCHITECTURE.md](../ARCHITECTURE.md) §7 the API; [GUARANTEES.md](../GUARANTEES.md) §6 the
guarantee (content integrity against the root, not a hop receipt); [STATUS.md](../STATUS.md)
the shipped pattern; the requirement points at what shipped.

## 5. Consequences and follow-ups

A weida user builds a torrent-like system from a `Seeder`, a `Swarm` and a place to keep bytes.
griasdi keeps its store, its tracker and its admission rule, and deletes its fetcher, its piece
tree and its `/piece` wire. Backlog:

### B-340 — SWARM piece tree: BlobId, PieceList, Bits, feature swarm
kind: code | size: 90 | status: ready | needs: []
acceptance: [0040](0040-swarm-pattern.md) §4.1: feature `swarm` on `weida` linking `blake3`; `crates/weida/src/swarm/tree.rs` with `MAX_PIECES`, `BlobId` (hex `Display`/`FromStr`), `PieceSize` (1 KiB..16 MiB), `Blob` (`count`, `span`), `PieceList` (`of`, `parse`, `to_bytes`, `check`), `TreeHasher`, `Bits`; unit tests: the root equals `blake3::hash` for twelve sizes around piece boundaries, a changed list byte is refused, `check` refuses a flipped bit, a piece in another slot and a wrong length, `Bits` refuses padding bits, `Blob::new` refuses `MAX_PIECES + 1`; LOOP §6 lists the feature run.

### B-341 — SWARM seeder: Listener::seeder, the ask wire, SeedRequest
kind: code | size: 90 | status: ready | needs: [B-340]
acceptance: [0040](0040-swarm-pattern.md) §4.2-§4.3: `swarm/wire.rs` with the 43/47-byte ask and its refusals (table-driven unit test); `Seed`/`Seeder` on `Route::Request`, `Listener::seeder`, `Seeder::accept`, `SeedRequest` with `answer_list`/`answer_have`/`answer_piece`/`decline`; `MAX_PENDING_ASKS` 64, ask body under `header_timeout_ms`, answer under `SEED_STALL` + length / `MIN_RATE`; tests: a malformed ask is refused before `accept` and a hand-built HAVE reaches it; an answer of the wrong length is refused and reported as `Error::Protocol`.

### B-342 — SWARM fetch: Runtime::swarm, Fetch over every seeder
kind: code | size: 90 | status: ready | needs: [B-341]
acceptance: [0040](0040-swarm-pattern.md) §4.4: `Gather`/`Swarm`, `Runtime::swarm`, `Swarm::fetch`, `FetchOptions`, `Piece`, `Fetch` (`next`, `list`, `delivered`, abort on drop), `claim` with endgame (unit-tested); integration tests over QUIC loopback: a blob gathered from two seeders holding different halves, a corrupting seeder and a lying list dropped while the blob completes, a resumed fetch asks for no piece the caller holds, a seeder that connects mid-fetch joins it, no usable seeder is `NotConnected`, a silent seeder does not hold up the last piece, a one-piece blob needs no LIST and an empty one no seeder, a dropped fetch asks nothing more, and a seeder that is still fetching serves what it has.

### B-343 — 0040 into the normative documents
kind: spec | size: 45 | status: ready | needs: [B-342]
acceptance: [0040](0040-swarm-pattern.md) §4.5: PATTERNS §6.5 SWARM (pattern table, wire table, failure table naming each test) and a §7 row; INVARIANTS bounds (`MAX_PENDING_ASKS`, the 47-byte ask, `max_inflight` × piece size, `MAX_PIECES`, `SEED_STALL` + length / `MIN_RATE`); ARCHITECTURE §7 `Listener::seeder`, `Runtime::swarm`, `Fetch`, feature `swarm`; GUARANTEES §6 row; STATUS §1 and §4; requirements/griasdi-swarm.md answered.

## 6. What this note does not decide

- **A tracker or announcer.** It is the application's: Req/Rep plus `IncomingMeta::peer` names
  who asks, and `Swarm::connect` during a fetch is the seam through which a new holder joins.
- **One identity per connection.** Dial anonymously (`ClientTls` without identity), or use one
  `Swarm` per identity.
- **Fetching from a peer that cannot be dialled.** Connections run one way only: a `Swarm` dials
  and asks, a `Seeder` binds and answers. Nothing is fetched back over a connection a peer
  opened, and there is no reciprocity (no tit-for-tat). A node that seeds while it fetches runs
  both endpoints: it feeds the pieces its `Fetch` yields into the storage its `Seeder` answers
  from. A peer behind NAT therefore fetches but does not seed.
- **How the descriptor travels.** The torrent-file analogue is `Blob` (root, size, piece size),
  41 bytes. The piece hashes are not part of it, because LIST fetches them from any seeder and
  checks them against the root (BitTorrent v2's Merkle root plays the same role). Signing and
  distributing the `Blob`, for example in a Raft catalog, is the application's job.
- **A store.** The application keeps the bytes, the bit map and the list wherever it likes.
- **Following `weida_raft::Consensus::leader()`** (the requirement's (e)). That is the
  application's, which maps the `NodeId` to a URL.
- **Mirrors in `weida::blocking` and Python.** Filed when a caller needs them.
- **PROTOCOL.md.** Nothing changes there, because SWARM rides the request route like SURVEY.

## 7. Sources

weida documents: [requirements/griasdi-swarm.md](../requirements/griasdi-swarm.md);
[INVARIANTS.md](../INVARIANTS.md); decisions [0001](0001-sequence-field.md),
[0002](0002-control-and-bulk-separation.md), [0020](0020-cluster-and-discovery.md),
[0023](0023-completion-is-a-cursor.md).

Code read for this note, at `1339b3b`: `crates/weida/src/stream.rs:130, 232, 710, 1032`;
`crates/weida/src/pool.rs:67-70`; `crates/weida/src/endpoint.rs` (`Respond`, `Surveyor`);
`crates/weida/src/listener.rs:583` (`replier`). griasdi: `crates/griasdi-proto/src/pieces.rs`,
`crates/griasdi-update/src/fetch.rs`, `crates/griasdi-update/src/swarm.rs`,
`crates/griasdi-update/src/fetch/wire.rs`, `crates/griasdi-august/src/servers.rs`. `blake3`
1.8.7 `src/hazmat.rs`.
