# Metadata and messages (R8)

Metadata is a handle of its own, read-only and safe to read from any thread. Every
`weida_*_meta(h, &out)` returns a new reference the caller drops, so metadata outlives the
request or transfer it came from. None of these functions blocks.

## `weida_incoming_meta`

| Function | Returns |
| --- | --- |
| `const char *weida_incoming_meta_endpoint(const weida_incoming_meta *m, size_t *len_out)` | the endpoint path; NULL on a reply |
| `const char *weida_incoming_meta_content_type(const weida_incoming_meta *m, size_t *len_out)` | the content type label; NULL when unset |
| `const char *weida_incoming_meta_topic(const weida_incoming_meta *m, size_t *len_out)` | the Pub/Sub topic; NULL when absent |
| `const char *weida_incoming_meta_peer_fingerprint(const weida_incoming_meta *m, size_t *len_out)` | the `sha256:` key the sender proved; NULL for an anonymous or local peer |
| `int weida_incoming_meta_content_len(const weida_incoming_meta *m, uint64_t *out)` | 1 and `*out` when the sender stated a length, else 0 |
| `int weida_incoming_meta_sequence(const weida_incoming_meta *m, uint64_t *out)` | 1 and `*out` when the sender numbered the transfer, else 0 |
| `void weida_incoming_meta_drop(weida_incoming_meta *m)` | — |

Strings follow R7: NUL-terminated, the exact length through `len_out` (NULL allowed), valid
until `m` is dropped. The other fields of `IncomingMeta` (`peer_chain`, `trace`, `tracestate`,
`gap`, `achieved`, the report fields, `segment`, `layer`) are not mapped yet
([libraries/weida-c.md](../libraries/weida-c.md)).

## `weida_msg`

What a collect op yields: the payload, owned by the library, and the metadata of the transfer
it consumed.

| Function | Returns |
| --- | --- |
| `const uint8_t *weida_msg_data(const weida_msg *msg)` | the payload; valid until `msg` is dropped |
| `size_t weida_msg_len(const weida_msg *msg)` | its length |
| `int weida_msg_meta(const weida_msg *msg, weida_incoming_meta **out)` | 0 and a new metadata reference; `-WEIDA_EINVAL` |
| `void weida_msg_drop(weida_msg *msg)` | — |
