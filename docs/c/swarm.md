# SWARM

A seeder answers each fetch's orders about one blob from the caller's own storage, one session per
fetch; a swarm fetches one blob from every connected seeder at once and hands out each piece once
it verifies against the blob's root, in any order. The piece tree values build the root and the
piece list from the bytes, and keep track of which pieces someone holds. SWARM is behind weida's
`swarm` feature, which `libweida` enables. Every function is callable from any thread (R5). Every
`int` function can also return `-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## Piece tree values

A blob id is its `WEIDA_BLOB_ID_LEN` bytes, a BLAKE3 hash, read from and written into the caller's
buffer. A piece size is its base-2 logarithm, `uint8_t`, from 10 (1 KiB) to 24 (16 MiB); any other
value is `-WEIDA_EINVAL`. A bit map travels as `(len + 7) / 8` bytes: bit `i` is bit `i % 8`, least
significant first, of byte `i / 8`, and the padding bits are clear.

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_blob_id_of(const uint8_t *data, size_t len, uint8_t *out)` | never; hashes on the caller's thread | 0 and the BLAKE3 hash of the bytes, `WEIDA_BLOB_ID_LEN` bytes in `out` |
| `int weida_blob_new(const uint8_t *root, uint64_t size, uint8_t piece_log2, weida_blob **out)` | never | 0; `-WEIDA_EINVAL` for a piece size out of range, more than `WEIDA_MAX_PIECES` pieces, or an empty blob whose root is not the hash of no bytes |
| `int weida_blob_root(const weida_blob *b, uint8_t *out)` | never | 0 and the root, `WEIDA_BLOB_ID_LEN` bytes in `out` |
| `uint64_t weida_blob_size(const weida_blob *b)` | never | the length in bytes; 0 for NULL |
| `uint8_t weida_blob_piece_log2(const weida_blob *b)` | never | the piece size's base-2 logarithm; 0 for NULL |
| `uint32_t weida_blob_count(const weida_blob *b)` | never | the pieces, the size over the piece size rounded up; 0 for an empty blob and for NULL |
| `int weida_blob_span(const weida_blob *b, uint32_t index, uint64_t *out_start, uint64_t *out_end)` | never | 1 and the byte range of piece `index`, end exclusive (the last piece is shorter when the size is not a multiple of the piece size); 0 past the last piece and for any NULL argument |
| `void weida_blob_drop(weida_blob *b)` | never | — |
| `int weida_piece_list_of(const uint8_t *data, size_t len, uint8_t piece_log2, weida_piece_list **out)` | never; hashes on the caller's thread | 0; `-WEIDA_EINVAL` for a piece size out of range or more than `WEIDA_MAX_PIECES` pieces |
| `int weida_piece_list_parse(const weida_blob *blob, const uint8_t *data, size_t len, weida_piece_list **out)` | never | 0; `-WEIDA_EINVAL` unless the bytes are one chaining value per piece that hash to the blob's root |
| `int weida_piece_list_blob(const weida_piece_list *l, weida_blob **out)` | never | 0 and the blob the list belongs to |
| `const uint8_t *weida_piece_list_to_bytes(const weida_piece_list *l, size_t *len_out)` | never | the chaining values one after the other, 32 bytes each, empty for a blob of at most one piece; valid as long as `l`; NULL for NULL |
| `int weida_piece_list_check(const weida_piece_list *l, uint32_t index, const uint8_t *piece, size_t len)` | never | 1 when the bytes are piece `index` of the list's blob, 0 otherwise and past the last piece |
| `void weida_piece_list_drop(weida_piece_list *l)` | never | — |
| `int weida_tree_hasher_new(uint8_t piece_log2, weida_tree_hasher **out)` | never | 0; `-WEIDA_EINVAL` for a piece size out of range |
| `int weida_tree_hasher_update(const weida_tree_hasher *h, const uint8_t *data, size_t len)` | never; hashes on the caller's thread | 0 once the next bytes of the blob are fed; `-WEIDA_ECLOSED` |
| `int weida_tree_hasher_finish(weida_tree_hasher *h, weida_piece_list **out)` | never; **consumes** `h` | 0 and the piece list, whose blob holds the root; `-WEIDA_EINVAL` for more than `WEIDA_MAX_PIECES` pieces |
| `void weida_tree_hasher_drop(weida_tree_hasher *h)` | never | — |
| `int weida_bits_new(uint32_t len, weida_bits **out)` | never | 0 and `len` bits, all clear |
| `int weida_bits_full(uint32_t len, weida_bits **out)` | never | 0 and `len` bits, all set |
| `int weida_bits_from_bytes(uint32_t len, const uint8_t *data, size_t n, weida_bits **out)` | never | 0; `-WEIDA_EINVAL` unless `n` is `(len + 7) / 8` and the padding bits are clear |
| `int weida_bits_as_bytes(const weida_bits *b, uint8_t *buf, size_t cap, size_t *out_n)` | never | 0, the map's bytes copied into `buf` and their count in `*out_n`; `-WEIDA_EINVAL` for a `cap` below that count; `buf` may be NULL only for a map of no bytes |
| `uint32_t weida_bits_len(const weida_bits *b)` | never | the bits; 0 for NULL |
| `uint32_t weida_bits_ones(const weida_bits *b)` | never | the bits set; 0 for NULL |
| `int weida_bits_get(const weida_bits *b, uint32_t index)` | never | 1 when bit `index` is set; 0 when clear, past the end and for NULL |
| `int weida_bits_all(const weida_bits *b)` | never | 1 when every bit is set, also for a map of no bits; 0 otherwise and for NULL |
| `int weida_bits_set(const weida_bits *b, uint32_t index, int on)` | never | 0; sets bit `index` when `on` is non-zero, clears it otherwise; past the end it does nothing |
| `void weida_bits_drop(weida_bits *b)` | never | — |

| Constant | Value |
| --- | --- |
| `WEIDA_BLOB_ID_LEN` | 32, the bytes of a blob id |
| `WEIDA_MAX_PIECES` | 1048576 (`2^20`), the most pieces a blob has |
| `WEIDA_PIECE_SIZE_MIN_LOG2` | 10, 1 KiB, the size of a BLAKE3 chunk |
| `WEIDA_PIECE_SIZE_MAX_LOG2` | 24, 16 MiB |
| `WEIDA_PIECE_SIZE_DEFAULT_LOG2` | 20, 1 MiB |

## `weida_fetch_options`

How a fetch orders, under construction; NULL options are the defaults.

| Function | Default | Meaning |
| --- | --- | --- |
| `int weida_fetch_options_new(weida_fetch_options **out)` | — | 0 |
| `int weida_fetch_options_set_list(const weida_fetch_options *o, const weida_piece_list *list)` | none | the piece list of an earlier fetch of the same blob (`weida_fetch_list`), which spares the LIST order; shared by the options and copied by the fetch that takes them; NULL clears it |
| `int weida_fetch_options_set_stall_ms(const weida_fetch_options *o, uint64_t v)` | 30000 | the fixed part of every answer's deadline |
| `int weida_fetch_options_set_min_rate(const weida_fetch_options *o, uint64_t v)` | 65536 | bytes per second below which a seeder's answer is too slow: the answer to an order of `n` bytes is due `stall + (9 + n) / min_rate` after it was written, or after the answer before it, whichever is later; zero counts as one |
| `int weida_fetch_options_set_per_peer(const weida_fetch_options *o, uint64_t v)` | 8 | the credit of one seeder: the most orders it holds at once, each from the moment it is written until the caller takes its piece; zero counts as one |
| `int weida_fetch_options_set_max_inflight(const weida_fetch_options *o, uint64_t v)` | 32 | orders over all seeders at once, which bounds the piece bytes a fetch holds at `max_inflight` times the piece size; zero counts as one |
| `int weida_fetch_options_set_max_peers(const weida_fetch_options *o, uint64_t v)` | unbounded | the seeders a fetch takes pieces from; the others stay connected and unused |
| `int weida_fetch_options_set_have_refresh_ms(const weida_fetch_options *o, uint64_t v)` | 5000 | how long a seeder with nothing the fetch can take waits before it is asked HAVE again |
| `void weida_fetch_options_drop(weida_fetch_options *o)` | — | — |

A count above `SIZE_MAX` becomes `SIZE_MAX` (R7).

## `weida_swarm`, `weida_fetch` and `weida_piece`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_swarm(const weida_runtime *rt, const weida_client_tls *tls, weida_swarm **out)` | never | 0 |
| `int weida_swarm_connect(const weida_swarm *s, const char *url, weida_swarm_connect_op **out)` | never (R3b) | 0; the wait returns 0 when connected, `-WEIDA_EUNTRUSTED` for a key not trusted; the runtime redials a lost address |
| `int weida_swarm_disconnect(const weida_swarm *s, const char *url)` | never | 1 when `url` was dialled and is now forgotten, its connection closed and no longer redialled; 0 when it was not |
| `size_t weida_swarm_peer_count(const weida_swarm *s)` | never | the connected seeders; 0 for NULL |
| `int weida_swarm_connection_stats(const weida_swarm *s, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `int weida_swarm_events(const weida_swarm *s, weida_peer_events **out)` | never | 0, a new reader ([stats.md](stats.md)) |
| `int weida_swarm_fetch(const weida_swarm *s, const weida_blob *blob, const weida_bits *have, const weida_fetch_options *options, weida_fetch **out)` | never; `have` and `options` are copied, NULL `options` are the defaults | 0; a fetch of the pieces `have` lacks; `-WEIDA_EINVAL` for a `have` without one bit per piece of `blob` or a list of another blob |
| `void weida_swarm_drop(weida_swarm *s)` | never | —; drops its connections, and a fetch made from it then reads `-WEIDA_ENOT_CONNECTED` |
| `int weida_fetch_fd(const weida_fetch *f, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_fetch_next(const weida_fetch *f, int64_t timeout_ms, weida_piece **out)` | up to `timeout_ms` | 0 and the next verified piece, in any order; 0 with `*out == NULL` once the caller holds every piece, and on every later call; `-WEIDA_ENOT_CONNECTED` when no order is in flight and no seeder is left that has not failed this fetch, or the swarm is gone; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_fetch_blob(const weida_fetch *f, weida_blob **out)` | never | 0 and the blob being fetched, also while a read is in flight |
| `int weida_fetch_list(const weida_fetch *f, weida_piece_list **out)` | never | 1 and a copy of the piece list once it is known and verified; 0 with `*out == NULL` while it is not; `-WEIDA_EINVAL` while a read is in flight |
| `int weida_fetch_delivered(const weida_fetch *f, weida_bits **out)` | never | 0 and a copy of the pieces the caller holds: those it started with and those the fetch yielded; `-WEIDA_EINVAL` while a read is in flight |
| `int weida_fetch_in_flight(const weida_fetch *f, weida_bits **out)` | never | 0 and a copy of the pieces ordered and not yet taken, on their way or verified and waiting for `weida_fetch_next`; `-WEIDA_EINVAL` while a read is in flight |
| `void weida_fetch_drop(weida_fetch *f)` | never | —; aborts every order in flight, and a waiting read returns `-WEIDA_ECLOSED` |
| `uint32_t weida_piece_index(const weida_piece *p)` | never | which piece; 0 for NULL |
| `const uint8_t *weida_piece_bytes(const weida_piece *p, size_t *len_out)` | never | the piece's bytes, `weida_blob_span` of its index, checked against the root; valid as long as `p`; NULL for NULL |
| `void weida_piece_drop(weida_piece *p)` | never | — |

`weida_swarm_connect_op` has `_op_fd(op, int64_t *out)` (0; `-WEIDA_EIO`), `_op_wait(op,
int64_t timeout_ms)` (0 when connected, `-WEIDA_EAGAIN`, `-WEIDA_EINTR` or `-WEIDA_ECLOSED`) and
`_op_drop(op)`, which aborts an unfinished dial. A swarm takes pieces from every seeder it
connected, and one connected during a fetch joins it at the fetch's next read.

## `weida_seeder` and `weida_seed_session`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_seeder(const weida_listener *l, const char *path, weida_seeder **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_seeder_path(const weida_seeder *s, size_t *len_out)` | never | the path, valid as long as `s`; NULL for NULL |
| `int weida_seeder_fd(const weida_seeder *s, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_seeder_accept(const weida_seeder *s, int64_t timeout_ms, weida_seed_session **out)` | up to `timeout_ms` | 0 and the next session, always with a well-formed open; `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_seeder_drop(weida_seeder *s)` | never | —; sessions not yet accepted are dropped, a waiting accept returns `-WEIDA_ECLOSED` |
| `int weida_seed_session_blob(const weida_seed_session *s, weida_blob **out)` | never | 0 and the blob the session is about |
| `int weida_seed_session_meta(const weida_seed_session *s, weida_incoming_meta **out)` | never | 0 and a new reference to its metadata ([transfer.md](transfer.md)), whose peer names the fetching side where its connection proved a key |
| `uint64_t weida_seed_session_connection(const weida_seed_session *s)` | never | the number of the connection it arrived on (`ConnectionId`); 0 for NULL |
| `int weida_seed_session_fd(const weida_seed_session *s, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_seed_session_next(const weida_seed_session *s, int64_t timeout_ms, int *out_ask, uint32_t *out_index)` | up to `timeout_ms`; waits as long as the fetch sends no order | 1, the order's `WEIDA_ASK_*` kind and, for a PIECE, its piece index (0 otherwise); 0 once the fetch finished its orders; `-WEIDA_EPROTOCOL` while the order before is unanswered, for an order this blob cannot have (the session is then refused) and after the session failed; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `int weida_seed_session_answer_list(const weida_seed_session *s, const weida_piece_list *list, weida_seed_session_answer_list_op **out)` | never (R3b); `list` is shared | 0; answers LIST |
| `int weida_seed_session_answer_have(const weida_seed_session *s, const weida_bits *have, weida_seed_session_answer_have_op **out)` | never (R3b); `have` is copied | 0; answers HAVE with the pieces this seeder can serve now |
| `int weida_seed_session_answer_piece(const weida_seed_session *s, const uint8_t *data, size_t len, weida_seed_session_answer_piece_op **out)` | never (R3b); the bytes are copied | 0; answers PIECE; the bytes are not checked here, the fetch checks them |
| `int weida_seed_session_decline(weida_seed_session *s, weida_seed_session_decline_op **out)` | never (R3b); **consumes** `s` | 0; ends the session: a blob this seeder does not serve or a piece it does not hold, and the fetch drops this seeder for the blob |
| `void weida_seed_session_drop(weida_seed_session *s)` | never | —; the session ends with its last holder, and the fetch drops this seeder when an answer was still due |

| Constant | Value |
| --- | --- |
| `WEIDA_ASK_LIST` | 0, the piece list; never asked for a blob of at most one piece |
| `WEIDA_ASK_HAVE` | 1, which pieces the seeder can serve now |
| `WEIDA_ASK_PIECE` | 2, the bytes of one piece |
| `WEIDA_MAX_PENDING_SESSIONS` | 64, sessions a seeder holds before `weida_seeder_accept` takes them |
| `WEIDA_SEED_STALL_MS` | 30000, the fixed part of an answer's deadline |
| `WEIDA_MIN_RATE` | 65536, bytes per second below which an answer is abandoned |

Each op type, `weida_seed_session_answer_list_op`, `weida_seed_session_answer_have_op`,
`weida_seed_session_answer_piece_op` and `weida_seed_session_decline_op`, has `_op_fd(op, int64_t
*out)` (0; `-WEIDA_EIO`), `_op_wait(op, int64_t timeout_ms)` and `_op_drop(op)`. An answer's wait
returns 0 once it was written and `-WEIDA_EPROTOCOL` for an answer that does not match the order:
another kind, another blob's list, a map without one bit per piece, bytes of another length than
the piece's span; the session is then refused. The decline's wait returns 0 once the DECLINE was
sent or the session was gone. Every wait also returns `-WEIDA_EAGAIN`, `-WEIDA_EINTR` or
`-WEIDA_ECLOSED`. Dropping an unfinished op aborts it.

## Rules

- A fetch orders nothing before its first `weida_fetch_next`, and resumes from `have`: it orders
  only the pieces `have` lacks, and with a list in its options it asks no seeder for it.
- `weida_fetch_next`, `weida_seeder_accept` and `weida_seed_session_next` are tries (R3a): the read
  in flight keeps its place across timeouts and `EINTR`, and `timeout_ms` 0 is the non-blocking
  read. Taking a piece frees its order's credit, which is all the backpressure a fetch needs.
- While a `weida_fetch_next` read is in flight, `weida_fetch_list`, `weida_fetch_delivered` and
  `weida_fetch_in_flight` are `-WEIDA_EINVAL`; `weida_fetch_blob` always answers.
- A session's calls are one at a time: while its `next` is in flight or an answer op runs, `next`,
  every answer and `decline` are `-WEIDA_EINVAL` ("a call is in flight on this seed session"); a
  `decline` refused so still consumes the handle, and the session is dropped when that answer
  ends. Each order is answered before the next `next`. A `next` must not be abandoned midway: drop
  the session instead.
- A seeder that sends a wrong piece, a wrong list, too slow an answer or a DECLINE is dropped by
  the fetch for its blob; the fetch goes on with the others.
- At most `WEIDA_MAX_PENDING_SESSIONS` sessions wait for `weida_seeder_accept`; the rest wait on
  their streams, as a replier's requests do.
- The write of an answer of `n` bytes is abandoned after `WEIDA_SEED_STALL_MS` plus `n /
  WEIDA_MIN_RATE` seconds.
