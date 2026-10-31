# TLS values and transfer metadata

Builders the caller fills and hands to the calls that take them; those calls copy them, so a
value may be dropped right after use. None of these functions blocks, and every one is callable
from any thread (R5).

## `weida_trust`

| Function | Returns |
| --- | --- |
| `int weida_trust_new(weida_trust **out)` | 0. The trust is `Trust::by_address()`: a `weida://sha256:…@host:port/path` URL pins its own key |
| `int weida_trust_add_pin(const weida_trust *t, const char *fingerprint)` | 0; `-WEIDA_EINVALID_FINGERPRINT` for anything but `sha256:` and 64 hex digits; `-WEIDA_EINVAL` for NULL or non-UTF-8 |
| `void weida_trust_drop(weida_trust *t)` | — |

## `weida_identity`

| Function | Returns |
| --- | --- |
| `int weida_identity_generate(weida_identity **out)` | 0; `-WEIDA_ETLS` when generation fails |
| `int weida_identity_from_pem(const uint8_t *cert, size_t cert_len, const uint8_t *key, size_t key_len, weida_identity **out)` | 0; `-WEIDA_EINVAL`. The PEM is copied and checked when first used |
| `int weida_identity_fingerprint(const weida_identity *id, const char **out)` | 0 and the `sha256:` text, which lives as long as `id`; `-WEIDA_ETLS` when the certificate does not parse |
| `void weida_identity_drop(weida_identity *id)` | — |

## `weida_client_tls`, `weida_server_tls`

| Function | Returns |
| --- | --- |
| `int weida_client_tls_new(const weida_trust *trust, weida_client_tls **out)` | 0; `-WEIDA_EINVAL`. Dials anonymously under `trust` |
| `void weida_client_tls_drop(weida_client_tls *tls)` | — |
| `int weida_server_tls_new(const weida_identity *identity, weida_server_tls **out)` | 0; `-WEIDA_EINVAL`. Presents `identity`, accepts anonymous clients |
| `void weida_server_tls_drop(weida_server_tls *tls)` | — |

## `weida_transfer_meta`

What an outgoing transfer carries in its header. Where a function takes
`const weida_transfer_meta *`, NULL means `TransferMeta::default()` (R7).

| Function | Returns |
| --- | --- |
| `int weida_transfer_meta_new(weida_transfer_meta **out)` | 0 |
| `int weida_transfer_meta_set_content_type(const weida_transfer_meta *m, const char *content_type)` | 0; `-WEIDA_EINVAL` |
| `void weida_transfer_meta_drop(weida_transfer_meta *m)` | — |
