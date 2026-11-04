# Identity and trust sources

The *identity* area (0045 §5): where an identity and a trust come from, changeable under a live
binding ([decisions/0032](../decisions/0032-identity-sources-and-the-handoff.md)), and the
transitions of a source, read from a queue (decision 10). Every function is non-blocking except
`weida_identity_events_recv`, and every one is callable from any thread (R5); every `int`
function can also return `-WEIDA_EINVAL` for a NULL or non-UTF-8 argument and `-WEIDA_EPANIC`.

A source is shared, never copied. The `_source` forms of the TLS terms ([tls.md](tls.md)) hold it,
so an update reaches every binding and requester made from them at its next handshake, with no
re-bind and no redial. Dropping a source handle releases the handle only: the source lives on
wherever it is held, and its events end (`weida_identity_events_recv` returns 0 with
`*out == NULL`) once nothing, C or Rust, holds it any more.

A renewal thread holds a weak handle rather than a source: it calls `_upgrade` each round, works
on the source it gets and drops it again, and stops once `_upgrade` returns 0, so it never keeps
a source and its key alive after the application dropped it.

## `weida_identity_source`

| Function | Returns |
| --- | --- |
| `int weida_identity_source_from_identity(const weida_identity *id, weida_identity_source **out)` | 0; `-WEIDA_ETLS` when the material does not read or parse: a wrong path is caught here, not at a handshake. A source that never changes; the identity is copied |
| `int weida_identity_source_ephemeral(const char *const *names, size_t n, weida_identity_source **out)` | 0; `-WEIDA_EINVAL` as `weida_identity_generate_for`; `-WEIDA_ETLS` when a name is refused. A self-signed identity generated in memory for `names`, gone with the process |
| `int weida_identity_source_external(const weida_identity *initial, weida_identity_source **out)` | 0; `-WEIDA_ETLS` as `_from_identity`. Starts as `initial` and changes on `_update` |
| `int weida_identity_source_files(const char *dir, const weida_files_options *options, weida_identity_source **out)` | 0; `-WEIDA_ETLS` when one file exists without the other or the material does not parse; `-WEIDA_EIO` when the bootstrap could not create or write. See [File sources](#file-sources) |
| `int weida_identity_source_current(const weida_identity_source *src, weida_identity **out)` | 0 and the identity of the moment as a new `weida_identity` the caller drops. A file source runs its due check here; its identity names the two files, read at each use as `weida_identity_from_pem_files` |
| `int weida_identity_source_fingerprint(const weida_identity_source *src, char *out, size_t len)` | 0 and the current key's fingerprint in the caller's buffer: `WEIDA_FINGERPRINT_LEN` (71) bytes and a NUL, since it changes with the source; `-WEIDA_EINVAL` for a `len` below `WEIDA_FINGERPRINT_LEN + 1`; `-WEIDA_ETLS` only for a static source whose material does not read. A file source runs its due check here |
| `int weida_identity_source_update(const weida_identity_source *src, const weida_identity *id)` | 0, reported as `RENEWED` when the key is unchanged and `KEY_CHANGED` when it is not; `-WEIDA_ETLS` when the material does not read, which changes nothing and is reported as `RENEWAL_FAILED`. The identity is copied |
| `int weida_identity_source_report_failure(const weida_identity_source *src, const char *reason)` | 0, reported as `RENEWAL_FAILED` with `reason`; the identity in service is unchanged |
| `int weida_identity_source_report_handoff_stolen(const weida_identity_source *src)` | 0, reported as `HANDOFF_STOLEN`: the hand-off credential had already been redeemed (0032 §2); the caller is expected not to continue |
| `int weida_identity_source_reload(const weida_identity_source *src)` | 1 when the files changed and were loaded; 0 when nothing changed or the source is not a file source; `-WEIDA_ETLS` or `-WEIDA_EIO` when they changed and could not be loaded, also reported as `RENEWAL_FAILED`, the identity in service staying. Runs regardless of the poll interval |
| `int weida_identity_source_events(const weida_identity_source *src, weida_identity_events **out)` | 0 and an independent reader that sees every event from the call on |
| `int weida_identity_source_downgrade(const weida_identity_source *src, weida_weak_identity_source **out)` | 0 and a weak handle |
| `void weida_identity_source_drop(weida_identity_source *src)` | — releases the handle; the source lives on in every terms, binding and requester that holds it |

### File sources

`dir` holds `cert.pem` and `key.pem`. When neither exists, a self-signed identity for the
options' names is written there first (the directory `0700`, both files `0600` on Unix), so a
second start finds the same key and the same fingerprint. Whatever rewrites the files (a Vault
Agent template, cert-manager, `certbot --reuse-key`) replaces them atomically, or the certificate
first and the key second, so that no check sees a chain that does not match its key. The source
compares the files' modification times at the next use of the identity (a handshake,
`_current`, `_fingerprint`) once the poll interval has passed; it has no timer and no thread.
`dir` is UTF-8 text (R7).

## `weida_weak_identity_source`

| Function | Returns |
| --- | --- |
| `int weida_weak_identity_source_upgrade(const weida_weak_identity_source *w, weida_identity_source **out)` | 1 and a new source handle while anything holds the source; 0 with `*out == NULL` once nothing does |
| `void weida_weak_identity_source_drop(weida_weak_identity_source *w)` | — |

## `weida_trust_source`

| Function | Returns |
| --- | --- |
| `int weida_trust_source_from_trust(const weida_trust *t, weida_trust_source **out)` | 0. A source that never changes; the trust is copied |
| `int weida_trust_source_external(const weida_trust *initial, weida_trust_source **out)` | 0. Starts as `initial` (copied) and changes on `_update` |
| `int weida_trust_source_current(const weida_trust_source *ts, weida_trust **out)` | 0 and the trust of the moment as a new `weida_trust` the caller drops: a copy; changing it does not change the source |
| `int weida_trust_source_update(const weida_trust_source *ts, const weida_trust *t)` | 0, reported as `TRUST_REFRESHED`; reaches every binding and requester holding the source at its next handshake. The trust is copied |
| `int weida_trust_source_report_failure(const weida_trust_source *ts, const char *reason)` | 0, reported as `RENEWAL_FAILED` with `reason`; the trust in service is unchanged |
| `int weida_trust_source_is_empty(const weida_trust_source *ts)` | 1 when the trust of the moment holds no pin and no anchor; 0 otherwise |
| `int weida_trust_source_events(const weida_trust_source *ts, weida_identity_events **out)` | 0 and an independent reader, as `weida_identity_source_events` |
| `int weida_trust_source_downgrade(const weida_trust_source *ts, weida_weak_trust_source **out)` | 0 and a weak handle |
| `void weida_trust_source_drop(weida_trust_source *ts)` | — releases the handle, as `weida_identity_source_drop` |

## `weida_weak_trust_source`

| Function | Returns |
| --- | --- |
| `int weida_weak_trust_source_upgrade(const weida_weak_trust_source *w, weida_trust_source **out)` | 1 and a new source handle while anything holds the source; 0 with `*out == NULL` once nothing does |
| `void weida_weak_trust_source_drop(weida_weak_trust_source *w)` | — |

## `weida_files_options`

What `weida_identity_source_files` takes; NULL there means `FilesOptions::default()` (R7): no
names, a check every 10 s.

| Function | Returns |
| --- | --- |
| `int weida_files_options_new(weida_files_options **out)` | 0 |
| `int weida_files_options_set_names(const weida_files_options *o, const char *const *names, size_t n)` | 0; `-WEIDA_EINVAL` when `names` is NULL with `n > 0`, an element is NULL or a name is not UTF-8, which leaves the list unchanged. Replaces the list: the names the bootstrapped certificate is made for when the directory is empty. Unused when files exist |
| `int weida_files_options_set_poll_ms(const weida_files_options *o, uint64_t v)` | 0. How often the files' modification times are checked, at the next use of the identity after the interval; 0 checks at every use |
| `void weida_files_options_drop(weida_files_options *o)` | — |

## `weida_identity_events` (a try handle, R3a)

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_identity_events_fd(const weida_identity_events *e, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_identity_events_recv(const weida_identity_events *e, int64_t timeout_ms, weida_identity_event **out)` | up to `timeout_ms` | 0 and the next event; 0 with `*out == NULL` once the source is gone (its last strong handle, C or Rust, dropped); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_identity_events_drop(weida_identity_events *e)` | never | — |

The queue holds 64 events; a reader that falls further behind gets one
`WEIDA_IDENTITY_EVENT_MISSED` event carrying how many it lost. Dropping a reader ends a thread
waiting in `recv` with `-WEIDA_ECLOSED`.

## `weida_identity_event`

| Function | Returns |
| --- | --- |
| `int weida_identity_event_kind(const weida_identity_event *e)` | the kind, a `WEIDA_IDENTITY_EVENT_*` constant |
| `const char *weida_identity_event_fingerprint(const weida_identity_event *e, size_t *len_out)` | `LOADED`, `RENEWED`: the key's fingerprint (`sha256:` text). Lives as long as `e` |
| `const char *weida_identity_event_from(const weida_identity_event *e, size_t *len_out)` | `KEY_CHANGED`: the fingerprint that was |
| `const char *weida_identity_event_to(const weida_identity_event *e, size_t *len_out)` | `KEY_CHANGED`: the fingerprint that is |
| `const char *weida_identity_event_message(const weida_identity_event *e, size_t *len_out)` | `RENEWAL_FAILED`: why |
| `int weida_identity_event_missed(const weida_identity_event *e, uint64_t *out)` | `MISSED`: 1 and how many events this reader lost |
| `void weida_identity_event_drop(weida_identity_event *e)` | — |

A getter of another kind's field reads absent: 0, or NULL; so does every getter for a NULL `e`.

## Constants

| Rust type | Constants, value |
| --- | --- |
| `IdentityEvent` | `WEIDA_IDENTITY_EVENT_LOADED` 0, `_RENEWED` 1, `_KEY_CHANGED` 2, `_RENEWAL_FAILED` 3, `_HANDOFF_STOLEN` 4, `_TRUST_REFRESHED` 5, `_MISSED` 6 |
