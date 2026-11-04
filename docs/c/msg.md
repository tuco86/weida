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
| `int weida_incoming_meta_peer_chain_len(const weida_incoming_meta *m, size_t *out)` | 1 and `*out`, the number of certificates (1 to 8), when the sender presented a chain, else 0: an anonymous or local peer, or a chain over the bound weida keeps |
| `const uint8_t *weida_incoming_meta_peer_chain_cert(const weida_incoming_meta *m, size_t index, size_t *len_out)` | the DER of certificate `index`, leaf first, its length through `len_out` (NULL allowed), valid until `m` is dropped; NULL when the chain is absent or `index` is out of range |
| `int weida_incoming_meta_peer_uid(const weida_incoming_meta *m, uint32_t *out)` | 1 and `*out`, the effective uid the kernel proved for an `AF_UNIX` peer at connect time; 0 for any other peer |
| `int weida_incoming_meta_peer_gid(const weida_incoming_meta *m, uint32_t *out)` | 1 and `*out`, the primary group id the kernel proved for an `AF_UNIX` peer; 0 for any other peer |
| `int weida_incoming_meta_peer_pid(const weida_incoming_meta *m, uint32_t *out)` | 1 and `*out`, the local peer's process id where the platform reports one (Linux `SO_PEERCRED`, a named pipe; macOS reports none), else 0. An observation only: it MUST NOT be authorized on (decisions/0010 §4.4) |
| `const char *weida_incoming_meta_peer_sid(const weida_incoming_meta *m, size_t *len_out)` | the account SID (`S-1-...`) the kernel proved for a named-pipe peer, valid until `m` is dropped; NULL for any other peer |
| `int weida_incoming_meta_content_len(const weida_incoming_meta *m, uint64_t *out)` | 1 and `*out` when the sender stated a length, else 0 |
| `int weida_incoming_meta_sequence(const weida_incoming_meta *m, uint64_t *out)` | 1 and `*out` when the sender numbered the transfer, else 0 |
| `const char *weida_incoming_meta_trace(const weida_incoming_meta *m, size_t *len_out)` | the sender's trace context as `traceparent` text; NULL when it sent none or one that did not parse |
| `const char *weida_incoming_meta_tracestate(const weida_incoming_meta *m, size_t *len_out)` | the opaque `tracestate`; NULL when absent |
| `int weida_incoming_meta_achieved(const weida_incoming_meta *m, int *out)` | 1 and the `WEIDA_ACKNOWLEDGEMENT_*` level in `*out` when the sender claimed one, else 0 |
| `void weida_incoming_meta_drop(weida_incoming_meta *m)` | — |

Strings follow R7: NUL-terminated, the exact length through `len_out` (NULL allowed), valid
until `m` is dropped. The other fields of `IncomingMeta` (`gap`, `report`, `report_mode`,
`report_id`, `segment`, `layer`) are not mapped yet
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
