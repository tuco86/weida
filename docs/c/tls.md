# TLS values and transfer metadata

Builders the caller fills and hands to the calls that take them; those calls copy them, so a
value may be dropped right after use. None of these functions blocks, and every one is callable
from any thread (R5).

## `weida_trust`

| Function | Returns |
| --- | --- |
| `int weida_trust_new(weida_trust **out)` | 0. The trust is `Trust::by_address()`: a `weida://sha256:…@host:port/path` URL pins its own key |
| `int weida_trust_add_pin(const weida_trust *t, const char *fingerprint)` | 0; `-WEIDA_EINVALID_FINGERPRINT` for anything but `sha256:` and 64 hex digits; `-WEIDA_EINVAL` for NULL or non-UTF-8 |
| `int weida_trust_add_anchor(const weida_trust *t, const uint8_t *pem, size_t len)` | 0; `-WEIDA_EINVAL`. Trusts the certificate authorities in the PEM for a peer whose chain leads to one and names the dialled host. The bytes are copied and parsed at each dial, which fails with `-WEIDA_ETLS` ("in-memory PEM contains no certificates", "parsing certificates: …") |
| `int weida_trust_add_anchor_file(const weida_trust *t, const char *path)` | 0; `-WEIDA_EINVAL`. The file is read at each dial, which fails with `-WEIDA_ETLS` when it is missing or bad |
| `void weida_trust_drop(weida_trust *t)` | — |

`Trust::pin`, `anchor` and `anchor_file` are `weida_trust_new` plus the matching `_add_*`.

## `weida_identity`

| Function | Returns |
| --- | --- |
| `int weida_identity_generate(weida_identity **out)` | 0; `-WEIDA_ETLS` when generation fails |
| `int weida_identity_generate_for(const char *const *names, size_t n, weida_identity **out)` | 0; `-WEIDA_EINVAL` when `names` is NULL with `n > 0`, an element is NULL or a name is not UTF-8; `-WEIDA_ETLS` when a name is refused. The certificate names `names` (DNS names or IP literals), so a peer anchoring it can check the dialled host. `n == 0` is `weida_identity_generate` |
| `int weida_identity_from_pem(const uint8_t *cert, size_t cert_len, const uint8_t *key, size_t key_len, weida_identity **out)` | 0; `-WEIDA_EINVAL`. The PEM is copied and checked when first used |
| `int weida_identity_from_pem_files(const char *cert_chain, const char *key, weida_identity **out)` | 0; `-WEIDA_EINVAL`. Nothing is read here: a missing or bad file fails the first use (fingerprint, PEM getters, bind, dial) with `-WEIDA_ETLS`, "reading certificates: …" or "reading the private key: …" |
| `int weida_identity_from_pem_file(const char *path, weida_identity **out)` | as `_from_pem_files`, with chain and key in one file, the form `weida_identity_to_pem` writes |
| `int weida_identity_fingerprint(const weida_identity *id, const char **out)` | 0 and the `sha256:` text, which lives as long as `id`; `-WEIDA_ETLS` when the certificate does not parse |
| `int weida_identity_certificate_pem(const weida_identity *id, weida_pem **out)` | 0 and the chain as PEM, no key; `-WEIDA_ETLS` when the source does not read or parse. Read anew on every call |
| `int weida_identity_to_pem(const weida_identity *id, weida_pem **out)` | 0 and the chain plus the private key (PKCS#8, PKCS#1 or SEC1, as the source held it) as one PEM document; `-WEIDA_ETLS` as above. Read anew on every call |
| `void weida_identity_drop(weida_identity *id)` | — |

## `weida_pem`

PEM text a getter hands out: owned bytes, not NUL-terminated, that the caller releases with
`weida_pem_drop`, which wipes them. `weida_identity_to_pem`'s bytes hold the private key; every
copy the caller makes of them is the caller's to wipe.

| Function | Returns |
| --- | --- |
| `const uint8_t *weida_pem_data(const weida_pem *p)` | the bytes, valid as long as `p`; NULL for NULL |
| `size_t weida_pem_len(const weida_pem *p)` | the length; 0 for NULL |
| `void weida_pem_drop(weida_pem *p)` | — |

## `weida_client_tls`, `weida_server_tls`

| Function | Returns |
| --- | --- |
| `int weida_client_tls_new(const weida_trust *trust, weida_client_tls **out)` | 0; `-WEIDA_EINVAL`. Dials anonymously under `trust` |
| `int weida_client_tls_set_identity(const weida_client_tls *tls, const weida_identity *identity)` | 0; `-WEIDA_EINVAL`. Presents `identity` (copied) to every peer that requesters made from these terms afterwards dial |
| `void weida_client_tls_drop(weida_client_tls *tls)` | — |
| `int weida_server_tls_new(const weida_identity *identity, weida_server_tls **out)` | 0; `-WEIDA_EINVAL`. Presents `identity`, accepts anonymous clients |
| `int weida_server_tls_require_client(const weida_server_tls *tls, const weida_trust *trust)` | 0; `-WEIDA_EINVAL`. Requires every client to present a key `trust` (copied) accepts, pinned or chaining to an anchor |
| `int weida_server_tls_require_any_key(const weida_server_tls *tls)` | 0; `-WEIDA_EINVAL`. Requires every client to present a key and prove it holds it, whatever the key |
| `void weida_server_tls_drop(weida_server_tls *tls)` | — |

The last `_require_client` or `_require_any_key` wins.

### Required clients

A binding that requires clients refuses an anonymous client, and under `_require_client` also a
key its trust does not accept, in the TLS handshake. The client sees `-WEIDA_ETLS` from the
connect op or, when the alert arrives after the dial completed, from its first request. The
server's replier sees nothing: no refused client reaches `weida_replier_accept`. An empty trust
given to `_require_client` is taken there, and the bind's op ends with `-WEIDA_ETLS` ("client
trust is empty: a binding cannot require clients it would never accept"). What the server learns
of an admitted client is `weida_incoming_meta_peer_fingerprint` and the chain getters
([msg.md](msg.md)), nothing else.

## `weida_transfer_meta`

What an outgoing transfer carries in its header. Where a function takes
`const weida_transfer_meta *`, NULL means `TransferMeta::default()` (R7).

| Function | Returns |
| --- | --- |
| `int weida_transfer_meta_new(weida_transfer_meta **out)` | 0 |
| `int weida_transfer_meta_set_content_type(const weida_transfer_meta *m, const char *content_type)` | 0; `-WEIDA_EINVAL` |
| `int weida_transfer_meta_set_content_len(const weida_transfer_meta *m, uint64_t len)` | 0; `-WEIDA_EINVAL`. The advisory payload length |
| `int weida_transfer_meta_set_topic(const weida_transfer_meta *m, const char *topic)` | 0; `-WEIDA_EINVAL`. A topic over 256 bytes fails the send with `-WEIDA_ELIMIT_EXCEEDED` |
| `int weida_transfer_meta_set_trace(const weida_transfer_meta *m, const char *traceparent)` | 0; `-WEIDA_EINVAL`, also for text that is not a W3C version-00 `traceparent` |
| `int weida_transfer_meta_set_achieved(const weida_transfer_meta *m, int level)` | 0; `-WEIDA_EINVAL`, also for a value not `WEIDA_ACKNOWLEDGEMENT_*` ([runtime.md](runtime.md#constants)) |
| `void weida_transfer_meta_drop(weida_transfer_meta *m)` | — |

`int weida_new_trace(char *out, size_t len)` (`weida::new_trace`) writes a fresh root trace
context as its `traceparent` text: `WEIDA_TRACEPARENT_LEN` (55) bytes and a NUL. It returns 0, or
`-WEIDA_EINVAL` for a NULL `out` or a `len` below `WEIDA_TRACEPARENT_LEN + 1`. Weida never
mints a trace on its own; this is how a caller starts one.
