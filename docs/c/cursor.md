# Cursors

A transfer orders cursor levels in its metadata; the receiver reports how far each level got on
a stream of its own through a reporter, and the sender reads the reports as a queue of cursor
sets. Every function is callable from any thread (R5). Every `int` function can also return
`-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## Levels and modes

| Constant | Value |
| --- | --- |
| `WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR` | 16, the first level an application may name |
| `WEIDA_REPORT_MODE_PROGRESS` | 0, records as a level advances, at the reporter's granularity; the default |
| `WEIDA_REPORT_MODE_FINAL_ONLY` | 1, one record per level, at the end |
| `WEIDA_REPORTER_DEFAULT_BYTES` | 1048576, a level advances this far before another progress record |
| `WEIDA_REPORTER_DEFAULT_INTERVAL_MS` | 100, or this much time passes, whichever comes first |

A cursor level is its wire value, a `uint64_t`, in and out: below
`WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR` a `WEIDA_ACKNOWLEDGEMENT_*` value
([runtime.md](runtime.md#constants)), at or above it an application stage weida carries but never
interprets. A value from 6 to 15 is reserved and `-WEIDA_EINVAL` wherever a level goes in.

## Ordering a report

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_transfer_meta_set_report(const weida_transfer_meta *m, const uint64_t *levels, size_t len)` | never; the levels are copied | 0; replaces the order, sorted and deduplicated, `len == 0` orders nothing; `-WEIDA_EINVAL` for a reserved level, the metadata unchanged; more than 16 distinct levels fail the send with `-WEIDA_ELIMIT_EXCEEDED` ([tls.md](tls.md)) |
| `int weida_transfer_meta_set_report_mode(const weida_transfer_meta *m, int mode)` | never | 0; a `WEIDA_REPORT_MODE_*` constant ([tls.md](tls.md)) |
| `int weida_outgoing_transfer_cursors(const weida_outgoing_transfer *t, weida_cursors **out)` | never | 1 and the queue the first time; 0 with NULL after that and when nothing was ordered; `-WEIDA_ECLOSED` once finished ([transfer.md](transfer.md)) |
| `int weida_incoming_transfer_reporter(const weida_incoming_transfer *t, weida_reporter **out)` | never | 1 and a new reporter on every call; 0 with NULL when nothing was ordered; `-WEIDA_ECLOSED` once collected ([transfer.md](transfer.md)) |

The receiver reads the order on the metadata: `weida_incoming_meta_report`,
`weida_incoming_meta_report_mode` and `weida_incoming_meta_report_id` ([msg.md](msg.md)).

## `weida_cursors`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_cursors_fd(const weida_cursors *c, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_cursors_recv(const weida_cursors *c, int64_t timeout_ms, weida_cursor_set **out)` | up to `timeout_ms` | 0 and the newest set after a change, the changes since the last recv coalesced into one; 0 with NULL once no more cursors come, and on every recv after that; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_cursors_drop(weida_cursors *c)` | never | —; releases the report id, records arriving after are ignored |

`weida_cursors_recv` is a try (R3a) like `weida_peer_events_recv`: the recv in flight is kept
between tries, and `timeout_ms` 0 is the non-blocking read.

## `weida_cursor_set`

The newest offset per reported level of one transfer, read-only.

| Function | Returns |
| --- | --- |
| `size_t weida_cursor_set_len(const weida_cursor_set *s)` | the levels reported; 0 for NULL |
| `int weida_cursor_set_level(const weida_cursor_set *s, size_t index, uint64_t *out_level, uint64_t *out_offset)` | 1 with entry `index`'s level and offset, levels ascending with the index; 0 past the end and for a NULL argument |
| `int weida_cursor_set_offset(const weida_cursor_set *s, uint64_t level, uint64_t *out)` | 1 and the offset reported for `level`; 0 when it was not reported |
| `void weida_cursor_set_drop(weida_cursor_set *s)` | — |

## `weida_reporter`

| Function | Blocks | Returns |
| --- | --- | --- |
| `const uint64_t *weida_reporter_levels(const weida_reporter *r, size_t *len_out)` | never | the ordered levels, ascending, their count through `len_out` (NULL allowed), valid as long as `r`; NULL for NULL |
| `int weida_reporter_mode(const weida_reporter *r)` | never | the `WEIDA_REPORT_MODE_*` the sender asked for |
| `int weida_reporter_set_granularity(const weida_reporter *r, uint64_t bytes, uint64_t interval_ms)` | never | 0; progress records are written when a level advanced `bytes` or `interval_ms` passed, whichever comes first; 0 and 0 write every report; no effect in final-only mode |
| `int weida_reporter_report(const weida_reporter *r, uint64_t level, uint64_t offset, weida_reporter_report_op **out)` | never (R3b) | 0; a level the sender did not order is ignored |
| `int weida_reporter_finish(weida_reporter *r, weida_reporter_finish_op **out)` | never (R3b); **consumes** `r` | 0 |
| `void weida_reporter_drop(weida_reporter *r)` | never | —; flushes nothing |

Each op type, `weida_reporter_report_op` and `_finish_op`, has `_op_fd(op, int64_t *out)`,
`_op_drop(op)` and `int _op_wait(op, int64_t timeout_ms)`, which returns 0 when done,
`-WEIDA_EAGAIN`, `-WEIDA_EINTR` or `-WEIDA_ECLOSED`; neither fails for a transport reason.
Dropping an unfinished op aborts it.

## Rules

- A report is an order, not a guarantee. A level the receiver cannot reach is absent from every
  set; the transfer does not fail for it.
- Offsets are absolute and only grow, so the queue coalesces and a late reader loses nothing:
  the set `weida_cursors_recv` returned last is the whole picture until it returns another.
- The queue's end is the same for a finished report, a reset stream and a lost connection.
- Nothing waits on a cursor: an unread queue blocks no transfer, an untaken reporter fails none,
  and a report op never fails for a transport reason.
- Progress mode writes at the granularity (`WEIDA_REPORTER_DEFAULT_BYTES` or
  `WEIDA_REPORTER_DEFAULT_INTERVAL_MS` until set; 0 and 0 for every report). Final-only writes
  nothing until `weida_reporter_finish`. Finish flushes the newest offset per reported level and
  writes no record for a level never reported.
- Take the queue before `weida_outgoing_transfer_finish` and the reporter before
  `weida_incoming_transfer_collect`; both outlive their transfer, since `Accepted` arrives after
  the payload's end.
- One report op at a time: every other call on the reporter is `-WEIDA_EINVAL` while one runs,
  and `weida_reporter_finish` then consumes the handle and the reporter is dropped, unflushed,
  when that report ends.
- Dropping the queue releases the report id; dropping a reporter flushes nothing, so the
  records its granularity held back are never sent.
