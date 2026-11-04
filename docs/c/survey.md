# SURVEY

A surveyor dials respondents and asks every one of them the same question; each answer that
arrives within the survey's deadline is collected, the later ones are counted. A respondent
serves one endpoint path of a listener and answers each question as a replier answers a
request. Every function is callable from any thread (R5). Every `int` function can also return
`-WEIDA_EINVAL` and `-WEIDA_EPANIC`.

## `weida_surveyor`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_runtime_surveyor(const weida_runtime *rt, const weida_client_tls *tls, weida_surveyor **out)` | never | 0 |
| `int weida_surveyor_connect(const weida_surveyor *s, const char *url, weida_surveyor_connect_op **out)` | never (R3b) | 0; the URL is parsed by the op |
| `int weida_surveyor_connect_op_fd(const weida_surveyor_connect_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_surveyor_connect_op_wait(const weida_surveyor_connect_op *op, int64_t timeout_ms)` | up to `timeout_ms` | 0 when connected; `-WEIDA_EINVALID_ADDRESS`, `-WEIDA_EUNTRUSTED` (with `weida_last_error_fingerprint`), `-WEIDA_ETLS`, `-WEIDA_ENEGOTIATION`, `-WEIDA_EIO`, `-WEIDA_ECONNECTION_LOST`; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_surveyor_connect_op_drop(weida_surveyor_connect_op *op)` | never | — |
| `size_t weida_surveyor_peer_count(const weida_surveyor *s)` | never | the connected respondents; 0 for NULL |
| `int weida_surveyor_connection_stats(const weida_surveyor *s, weida_connection_stats **out)` | never; a snapshot | 0 ([stats.md](stats.md)) |
| `int weida_surveyor_survey(const weida_surveyor *s, const uint8_t *body, size_t len, uint64_t deadline_ms, weida_surveyor_survey_op **out)` | never; the body is copied | 0; `-WEIDA_EINVAL` for a `deadline_ms` the clock cannot add |
| `int weida_surveyor_survey_with(const weida_surveyor *s, const weida_transfer_meta *meta, const uint8_t *body, size_t len, uint64_t deadline_ms, weida_surveyor_survey_op **out)` | never; `meta` and the body are copied | 0. `weida_surveyor_survey` with metadata ([tls.md](tls.md)); NULL `meta` is `TransferMeta::default()` |
| `int weida_surveyor_survey_op_fd(const weida_surveyor_survey_op *op, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_surveyor_survey_op_wait(const weida_surveyor_survey_op *op, int64_t timeout_ms, weida_survey_run **out)` | up to `timeout_ms` | 0 and the run once every respondent was asked or the deadline passed; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_surveyor_survey_op_drop(weida_surveyor_survey_op *op)` | never | — |
| `void weida_surveyor_drop(weida_surveyor *s)` | never | — |

## `weida_survey_run`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_survey_run_fd(const weida_survey_run *r, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_survey_run_next(const weida_survey_run *r, uint64_t max_bytes, int64_t timeout_ms, weida_survey_answer **out)` | up to `timeout_ms` | 0 and the next answer; 0 with `*out == NULL` at the end; an answer that failed returns its code (`-WEIDA_ELIMIT_EXCEEDED` above `max_bytes`, a refusal such as `-WEIDA_EREJECTED`, `-WEIDA_ECONNECTION_LOST`, ...); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `uint64_t weida_survey_run_late(const weida_survey_run *r)` | never | the answers that arrived after the deadline; 0 for NULL |
| `size_t weida_survey_run_respondents(const weida_survey_run *r)` | never | how many respondents were asked; 0 for NULL |
| `void weida_survey_run_drop(weida_survey_run *r)` | never | — |

## `weida_survey_answer`

| Function | Blocks | Returns |
| --- | --- | --- |
| `const uint8_t *weida_survey_answer_data(const weida_survey_answer *a)` | never | the answer's bytes, valid as long as `a`; NULL for NULL |
| `size_t weida_survey_answer_len(const weida_survey_answer *a)` | never | their length; 0 for NULL |
| `void weida_survey_answer_drop(weida_survey_answer *a)` | never | — |

## `weida_respondent`

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_listener_respondent(const weida_listener *l, const char *path, weida_respondent **out)` | never | 0; `-WEIDA_EINVALID_ENDPOINT_PATH`, `-WEIDA_EALREADY_REGISTERED` |
| `const char *weida_respondent_path(const weida_respondent *r, size_t *len_out)` | never | the path |
| `int weida_respondent_fd(const weida_respondent *r, int64_t *out)` | never | 0; `-WEIDA_EIO` |
| `int weida_respondent_accept(const weida_respondent *r, int64_t timeout_ms, weida_incoming_request **out)` | up to `timeout_ms` | 0 and the question ([listener.md](listener.md)); `-WEIDA_ENOT_CONNECTED` once the queue ended; `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` |
| `void weida_respondent_drop(weida_respondent *r)` | never | — |

A survey asks every respondent connected when it starts, one exchange each on the connection
that respondent is on. Its deadline is the caller's and never goes on the wire; it bounds the
asking as well as the answers, so a respondent that stalls cannot hold the survey open. A survey
with no respondents is not an error: its run is empty and the first `weida_survey_run_next`
is the end.

`weida_survey_run_next` is a try (R3a), as `weida_peer_events_recv` is: the read in flight is
kept in the run between tries and keeps the `max_bytes` of the try that started it. 0 with
`*out == NULL` is the end, once the deadline passed or every respondent answered, and every
later call returns it again. An answer that failed returns its code and the run goes on: the
other respondents are independent exchanges. An answer carries no metadata, as in Rust. While a
read is in flight, `weida_survey_run_late` is the count as of the last read that ended.
Dropping the run ends a waiting `weida_survey_run_next` with `-WEIDA_ECLOSED` and resets every
exchange not yet answered.

A respondent's questions are `weida_incoming_request`s, read and answered with the
`weida_incoming_request_*` functions ([listener.md](listener.md)). `weida_respondent_accept`
is a try (R3a) and follows R11 as `weida_replier_accept` does. Dropping the respondent ends a
waiting accept with `-WEIDA_ECLOSED`; the path stays registered, as a dropped Rust
`Respondent` leaves it.
