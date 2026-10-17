# Proposal from griasdi: a content-addressed piece swarm

Written by the griasdi side against weida `1339b3b` (0.1.0-alpha.8-dev);
griasdi pins `=0.1.0-alpha.7` (root `Cargo.toml`, `weida`, `weida-raft`).

**Answered by [decisions/0040](../decisions/0040-swarm-pattern.md) (accepted; shipped in
B-340 to B-342).** The owner took the **pattern, not the system**: weida gets a SWARM pattern
behind the off-by-default feature `swarm`, and storage and discovery stay the application's.
Shipped: (a) the piece tree, as `BlobId`, `PieceSize`, `Blob`, `PieceList`, `TreeHasher` and
`Bits`, with the root equal to `blake3::hash` of the bytes so griasdi's existing roots stay
valid; (b) in its pattern form, `Listener::seeder`, whose `Seeder::accept` hands each decoded
ask to the application as a `SeedRequest` that it answers from its own storage; (c) in its
pattern form, `Runtime::swarm` and `Swarm::fetch`, whose `Fetch` asks every connected seeder
at once, claims pieces at random with an endgame, bounds each ask by stall plus length at a
minimum rate, drops a seeder that fails, stalls or sends a wrong piece, yields only verified
pieces, and takes `max_peers` as the cap on concurrent sources. Not taken, each with its
reason from 0040 §6: the **store** (`BlobStore`, the on-disk layout, `ingest`, resume), because
the application keeps the bytes, the bit map and the list wherever it likes and passes `have`
and the list back to resume; the **tracker and announcer** of (d), because discovery is
Req/Rep plus `IncomingMeta::peer` and `Swarm::connect` during a fetch is the seam; the **seed**
concept and `Seeder::admit`/`retire`, because every connected seeder is asked HAVE and a full
holder simply answers full bits, while admission is the application's decision on each
`SeedRequest`; **per-connection TLS**, because a `Swarm` has one `ClientTls`, so the
application dials anonymously or uses one `Swarm` per identity; and **(e)**, following
`weida_raft::Consensus::leader()`, because mapping a `NodeId` to a URL is the application's.

## Why griasdi asks

griasdi ships every push to `main`. Today the CI signs one combined manifest
of whole archives (desktop and server, all targets) and uploads it to august
(`/publish`). august keeps one head per channel in its Raft catalog and
computes a BLAKE3 piece root per file. Servers watch the catalog
(`AugustRequest::Watch`) and fetch the files piece by piece from august and
from peers that august names (`Holding`/`Peers`). Desktops download one
archive from one server at a time over `/update`, with no resume.

Measured on build 331, the x86_64 Linux desktop tar.gz is 86.5 MB and
unpacks to:

- `bin/griasdi`: 160 MB;
- `lib/griasdi/libgstreamer-full-1.0.so`: 33 MB, unchanged across most
  pushes;
- `bin/griasdi-share`: 7.6 MB;
- license texts.

Every push moves all of it again to every server and every desktop.

The redesign griasdi plans:

- Per component, OS, architecture and channel
  (`<desktop|server|august>-<os>-<arch>-<channel>`), a signed manifest of the
  unpacked files with their hashes.
- An installer copies the files that are already correct and fetches only the
  missing ones.
- Desktops fetch from all their connected servers at once.
- Servers fetch from august and from each other, with august as the tracker.
- august itself becomes a component that updates the same way.
- Later, the members of a multi-node august group fetch from each other, with
  the leader as tracker.
- Hash-identical files (the GStreamer library, an unchanged component binary)
  are never transferred, and their component is not restarted.

The piece machinery exists today as griasdi code (listed below). It is
generic: content-addressed, multi-source, resumable. It is transfer work that
weida's patterns do not cover. griasdi asks for it as a weida pattern and will
not grow its own further.

weida's `docs/INVARIANTS.md` (lines 28-29: "Raft coordinates control state,
not bulk payload transport"; "Payload replication remains stream-oriented")
already puts bulk outside consensus. The swarm keeps it there: the catalog in
Raft names roots, and the swarm moves the bytes.

Prior art: BitTorrent (pieces, trackers, endgame), Bao (verified BLAKE3
streaming), iroh-blobs (BLAKE3 content-addressed blob transfer over QUIC).

## What griasdi has today (to move or replace)

| Path | Role |
|---|---|
| `crates/griasdi-proto/src/pieces.rs` | BLAKE3 piece tree. A piece is 1 MiB (`HOSTED_PIECE`), one subtree. Chaining value via `blake3::hazmat` (`set_input_offset`, `finalize_non_root`, `merge_subtrees_*`), which is Bao with the outboard cut at piece level. Also `count`, `span`, `covering`, `root`, `parse`/`encode`, `Checker`, `TreeHasher`, `Bits`. A one-piece file is checked against the root directly. |
| `crates/griasdi-update/src/swarm.rs` | `PieceStore`, a directory named only by `root.to_hex()`: `<hex>` whole, `<hex>.part`, `<hex>.bits`, `<hex>.pieces`, `tmp-*`. `ingest`, `begin`/`Partial::write_piece`/`flush_bits`/`finish`, `whole`, `list`, `have`, `read_piece`, `remove`, `roots`. |
| `crates/griasdi-update/src/fetch.rs` | `fetch()`. One worker per source, random claim of missing pieces the source holds, endgame (duplicate the last in-flight pieces), stall limit plus a deadline at 64 KiB/s minimum rate, a source that fails or lies is dropped for that file, bits flushed every 16 pieces for resume, peers re-asked every 60 s, a `stop` future for preemption. Traits `Source` (`list`/`have`/`get`) and `Fetcher` (`connect(addr, seed)`, `peers(root)`, `peer_workers`, `stall`, `holding`, `landed`). |
| `crates/griasdi-update/src/fetch/wire.rs` | `RequesterSource`: the `/piece` protocol over a `weida::Requester`, one postcard `PieceRequest::{List, Have, Get}` frame per request, raw bytes back. |
| `crates/griasdi-august/src/servers.rs` | The tracker. `Registry` keyed by the TLS-proved fingerprint. `Watch{addr}` registers an address whose fingerprint must equal the proved key. `Holding{roots}` replaces the held roots (at most 4096). `Peers{root}` returns up to 16 random holders, never the asker, public addresses only unless configured. |
| `crates/griasdi-august/src/piece.rs`, `crates/griasdi-server/src/august/piece.rs` | The seeding endpoint. It answers only roots the current catalog names (`size_of(root)` is the gate) and serves a partial file's flushed pieces. |
| `crates/griasdi-server/src/august/swarm/fetch.rs` (`ServerFetcher`), `crates/griasdi-client/src/models.rs` (`ClientFetcher`) | The two callers. The server dials with its own key, 4 peer workers, and august as seed. The desktop dials every connection with a throwaway key (`open_dial.rs`) and takes pieces from every server that offers the file. |

## (a) Content addressing and verification

- **Missing.** weida has no content-addressed type or piece tree: no blob,
  swarm or BLAKE3 API. `docs/decisions/0001` and `0023` mention content
  hashes only as research.
- **API.**
  - A `BlobId` (BLAKE3 root, 32 bytes) and a piece size fixed per swarm, a
    power of two times 1 KiB. griasdi uses 1 MiB and asks for that as the
    default.
  - `BlobId::of(bytes)` and an incremental `TreeHasher`.
  - A `Checker` built from a piece list that must hash to the root, with
    `check(index, piece) -> bool`.
  - `Bits` for "which pieces".
  - The tree is the one `griasdi_proto::pieces` uses, so the BlobIds griasdi
    already has (catalog roots, hosted blobs) stay valid.
- **Why in weida.** The fetcher and the seeder of (b) and (c) need it.
  Verification at piece level is what makes a source untrusted by default.
- **griasdi code that would use it.**
  - Everything in the table above.
  - `crates/griasdi-client/src/hosted/` (hosted files use the same tree for
    sealed blobs).

## (b) Seeder: serve pieces of admitted blobs from a store

- **Missing.** Serving pieces is a hand-written `Replier` loop in two griasdi
  crates.
- **API.**
  - `Listener::seeder(path: &str, store: impl BlobStore) -> Result<Seeder,
    Error>`, next to `Listener::replier` (`crates/weida/src/listener.rs:583`).
  - The seeder answers list, have and get only for roots the application
    admitted: `Seeder::admit(root, size)` and `Seeder::retire(root)`, or a
    policy closure. The admission rule is weida's call. griasdi needs "only
    what my catalog names, partial files included".
  - A concurrency bound per seeder (griasdi uses 32 and 64 streams today).
  - A `BlobStore` trait with an on-disk default store using the layout of
    `PieceStore`: names derived from the root only, so no network name
    becomes a path; partial files with a bit map and piece list; `tmp-*`
    cleaned on open; `ingest(reader, size) -> BlobId`.
  - The wire format between seeder and fetcher is weida's. griasdi's
    `PieceRequest` frames are not a constraint.
- **Why in weida.** The request framing, stream budget and stall handling are
  the same per-stream concerns weida owns for Req/Rep (`docs/PATTERNS.md`
  §1.4, §1.8, §2).
- **griasdi code that would use it.**
  - august's `/piece` (`crates/griasdi-august/src/piece.rs`);
  - the server's `/piece` (`crates/griasdi-server/src/august/piece.rs`);
  - later, a desktop that seeds to its own other devices (not asked for now).

## (c) Fetcher: one blob from many sources, resumable

- **Missing.** See `crates/griasdi-update/src/fetch.rs` in the table above.
- **API.**
  - `Runtime::blob_fetcher(tls: impl Into<ClientTls>) -> BlobFetcher`, next to
    `Runtime::requester` (`crates/weida/src/runtime.rs:300`).
  - `BlobFetcher::fetch(store, root, size, sources) -> Fetch`. `Fetch` is a
    future that resolves to the whole file in the store, can be cancelled
    (dropping it keeps the written pieces), and reports landed bytes for
    progress.
  - `sources` holds:
    - the URLs of sources known to the caller;
    - optionally one "seed" assumed to hold every piece (august today);
    - optionally a tracker URL (d) that names more holders and is re-asked
      periodically.
  - Behavior griasdi relies on, as `fetch.rs` does it today:
    - random claim of pieces the source holds;
    - endgame;
    - a per-transfer deadline of stall plus length at a minimum rate;
    - a source that fails, stalls or sends a wrong piece is dropped for this
      blob;
    - written pieces persist and the next fetch resumes;
    - a cap on concurrent sources per fetch, set by the caller (4 on a
      server, unbounded on a desktop).
  - A connection hook so each source connection can use its own `ClientTls`.
    The desktop dials every connection with a fresh throwaway identity, so a
    download reveals no identity (`crates/griasdi-client/src/open_dial.rs`).
  - The root authenticates the content. Whether the root is trusted is the
    caller's job: griasdi will sign roots in its manifests.
- **Why in weida.** Multi-connection scheduling, deadlines and cancellation
  are transport behavior. Every weida user who moves large files would
  otherwise write the same loop.
- **griasdi code that would use it.**
  - `ServerFetcher` and the downloader `run_downloader`
    (`crates/griasdi-server/src/august/swarm/fetch.rs`), which preempts a
    fetch when the catalog changes and resumes it later;
  - `ClientFetcher`/`install_set_from` (`crates/griasdi-client/src/models.rs`);
  - the planned file-set installer for updates.

## (d) Tracker: who holds which blob

- **Missing.** See `crates/griasdi-august/src/servers.rs` in the table above.
- **API.**
  - `Listener::tracker(path, policy) -> Tracker` with two requests:
    - `announce(holder_url, roots)`: replaces what the holder holds. The URL's
      fingerprint must equal the proved key.
    - `peers(root) -> Vec<url>`: random holders, never the asker, capped.
  - Announcements live while the announcing connection lives. griasdi ties
    them to its catalog watch today, so an end of life on disconnect fits.
  - Policy hooks: who may announce (proved key required), which hosts may be
    handed out (griasdi filters non-public addresses unless a test allows
    them), caps.
  - The fetcher of (c) asks a tracker named in `sources`. A seeder can
    announce itself to a tracker.
- **Why in weida.** Discovery of who holds what is a standing pattern next to
  `Discovery` and `weida-broker`. It needs the proved peer identity that only
  weida sees at the handshake.
- **griasdi code that would use it.**
  - august (`crates/griasdi-august/src/servers.rs`: `Registry`, `Holding`,
    `Peers`);
  - the server's `inform` loop (`crates/griasdi-server/src/august/mod.rs`).

## (e) Several trackers over one Raft group (later, sketched only)

Not needed for the first release.

griasdi's august is one weida-raft member today
(`crates/griasdi-august/src/consensus.rs`). Only signed manifests and roots
are replicated; files stay in each member's local store.

With more members, each member runs a seeder and a tracker. Followers fetch
the files the replicated catalog names from the leader and from each other,
with the leader's tracker named as the source of holders. Tracker state stays
per process and outside consensus.

Open question for weida: should the tracker follow
`weida_raft::Consensus::leader()` itself, or is that left to the application?

## What griasdi does afterwards

Once a weida release ships (a) to (d):

- griasdi moves to that version;
- it deletes `crates/griasdi-proto/src/pieces.rs`,
  `crates/griasdi-update/src/swarm.rs`, `fetch.rs` and `fetch/wire.rs`, and
  the hand-written piece endpoints and tracker requests;
- it builds the update redesign above on the new pattern, behind a bridge
  release that keeps today's combined manifest and `/update` for installs
  that predate it.

griasdi's `/piece` wire changes with this, and griasdi coordinates that with
its bridge, not weida.

## Checks griasdi will run

- Two servers and a desktop in griasdi's integration tests fetch one file from
  a seed plus peers, with one peer sending a wrong piece: the file completes
  and that peer is dropped.
- A fetch cancelled halfway resumes without re-fetching written pieces.
- A tracker never names the asker and never names a non-public address unless
  allowed.
- A desktop's connections each present a different key.
