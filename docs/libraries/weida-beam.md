# weida for Erlang and Elixir (`crates/beam/weida`, against the C ABI `libweida`) — feature parity

## 1. What a row means

The package is one OTP application, `weida`, built with rebar3: a NIF written in C against
`weida.h` ([decisions/0046](../decisions/0046-how-each-language-binds-libweida.md)) and the
module `weida` on top, which Elixir calls as `:weida`. Its API mirrors the C ABI: one function per
C function, named like it without the `weida_` prefix, with a trailing timeout on every waiting
function. Its reference is therefore [weida-c.md](weida-c.md): every row present there is present
here unless a row below says otherwise, and every row absent there is absent here for the same
reason. This document lists what the BEAM surface adds or changes. Verdicts are **present** (with
the test), **refused** (with the reason) or **absent** (with the reason); there is no "partial".

The tests are EUnit modules: `test/weida_tests.erl` (the unit tier: in-process transport only,
each test well under a second) and `test/weida_quic_tests.erl` (the integration tier: QUIC on
localhost). `smoke.exs` makes one round trip from Elixir. Measured on Erlang/OTP 29, Elixir 1.20
and rebar3 3.27 on linux-amd64 (the `beam-linux-amd64` job and the workstation); the
`beam-linux-arm64` job runs the same, but its image does not build on its runner at the time of
writing (zeughaus proposals #24 and #25).

## 2. Functions

| C ABI | Erlang (`weida`) | Verdict |
| --- | --- | --- |
| `weida_runtime_config` | a map `#{worker_threads => N, shutdown_timeout_ms => Ms}` given to `runtime_owned/1`, turned into a handle in the same call | present; `weida_tests` "a runtime with no worker threads is a runtime failure" |
| `weida_runtime` | `runtime_owned/1`, `runtime_listener/1`, `runtime_requester/2`, `runtime_shutdown(Rt, Timeout)` (consumes) | present; every round-trip test |
| `weida_trust` | `trust_new/0`, `trust_add_pin(Trust, Fingerprint) -> ok`, `trust_add_anchor(Trust, Pem \| {cacerts, [Der]} \| {cacertfile, Path}) -> ok` (maps `weida_trust_add_anchor` and `weida_trust_add_anchor_file`; the PEM is copied, and the PEM or file is parsed or read at each dial) | present; `weida_tests` "a malformed pin is an invalid fingerprint", "anchors take PEM and DER", `weida_quic_tests` "a wrong pin is untrusted and carries the fingerprint", "an anchor accepts the dialled name and refuses another", "identity and anchor files read back" |
| `weida_identity` | `identity_generate/0`, `identity_generate_for([Name])`, `identity_from_pem(Cert, Key)`, `identity_from_pem_file(Path)`, `identity_from_pem_files(Cert, Key)`, `identity_from(Opts)` (ssl's `certfile`, `keyfile`, `cert` and `key`, as a proplist or a map; `password` and any other key are `badarg`), `identity_fingerprint/1 -> {ok, binary()}`, `identity_certificate_pem/1 -> {ok, binary()}`, `identity_certificates/1 -> {ok, [Der]}` (DER, leaf first, for `public_key:pkix_decode_cert/2`), `identity_to_pem/1 -> {ok, binary()}` (holds the private key; an Erlang binary cannot be wiped) | present; `weida_quic_tests` "a round trip over QUIC with a pinned key", "identity and anchor files read back"; `weida_tests` "to_pem reads back with the same fingerprint", "the certificates are the identity's key", "generate_for names the certificate", "identity_from takes ssl's in-memory options", "identity_from refuses what it cannot honour" |
| `weida_pem` | not exposed: copied out at once into a binary (`identity_certificate_pem/1`, `identity_to_pem/1`) and dropped, which wipes it, in the same NIF call | refused: an Erlang caller needs the bytes, not the handle |
| `weida_client_tls`, `weida_server_tls` | `client_tls_new(Trust)`, `client_tls_set_identity(Tls, Identity) -> ok`, `server_tls_new(Identity)`, `server_tls_require_client(Tls, Trust) -> ok`, `server_tls_require_any_key(Tls) -> ok` | present; `weida_quic_tests` "a pinned client key is accepted and the server sees its key and chain", "an anonymous or unknown client is refused with tls and nothing reaches the server", "a binding that requires clients from an empty trust does not bind", "any key exposes the fingerprint and the chain"; `weida_tests` "anchors take PEM and DER" |
| `weida_transfer_meta` | a map `#{content_type => iodata()}`, turned into a handle in the NIF call that needs it | present; `weida_tests` "a streamed round trip carries the body and the content type" |
| `weida_listener` | `listener_bind_quic(L, Addr, ServerTls, Timeout) -> {ok, Binding}`, `listener_bind_inproc/2`, `listener_replier/2` | present |
| `weida_binding`, `weida_local_binding` | `binding_local_addr/1`, `local_binding_bus/1`, each a binary | present |
| `weida_requester` | `requester_connect(R, Url, Timeout) -> ok`, `requester_request(R, Body, Timeout) -> {ok, IncomingTransfer}`, `Body` iodata | present; `weida_tests` "a streamed round trip carries the body and the content type" |
| `weida_replier` | `replier_path/1`, `replier_accept(R, Timeout) -> {ok, IncomingRequest}` | present; `weida_tests` "two processes accepting on one replier both get a request" |
| `weida_incoming_request` | `incoming_request_meta/1`, `incoming_request_take_body/1`, `incoming_request_reply(Q, Meta, Timeout)` (consumes) | present; `weida_tests` "a second reply is closed" |
| `weida_incoming_transfer` | `incoming_transfer_meta/1`, `incoming_transfer_read(T, Max, Timeout) -> {ok, binary()} \| eof`, `incoming_transfer_collect(T, Max, Timeout) -> {ok, Message}` (consumes) | present; `weida_tests` "reads of 3 bytes reassemble the body", "a collect over its ceiling is limit_exceeded", "a reply dropped unfinished reads as canceled" |
| `weida_outgoing_transfer` | `outgoing_transfer_write(T, Data, Timeout) -> {ok, Count}`, `outgoing_transfer_write_all/3`, `outgoing_transfer_finish/1 -> {ok, Delivery}` (consumes) | present; `write_all` is the Erlang loop R3c leaves to the caller |
| `weida_delivery` | `delivery_delivered(D, Timeout) -> ok` (consumes) | present |
| `weida_incoming_meta` | a map `#{endpoint, content_type, topic, peer_fingerprint, peer_chain, content_len, sequence}`, copied out at once; absent values are `undefined`; `peer_chain` is a list of DER binaries, leaf first, or `undefined` for an anonymous or local peer or a chain over the bound weida keeps | present; `weida_quic_tests` "a pinned client key is accepted and the server sees its key and chain", "an anchor accepts the dialled name and refuses another" |
| `weida_msg` | a map `#{payload := binary(), meta := Meta}`, copied out at once and the C message dropped in the same call | present |
| the ops (`*_op`) | not exposed: a waiting function starts its op, waits, and drops it in an `after` clause, which cancels it only when it did not finish | refused: an op is the C caller's way to wait, not a value an Erlang caller needs |
| the `_fd` functions, `weida_poll` | not exposed: the NIF hands the descriptor to the VM's own poller (§4) | refused: a process waits in `receive`, not on a descriptor |
| `weida_version`, `weida_strerror` | `version/0`, `strerror(Code)` with `Code` an R9 atom or an integer of either sign | present; `smoke.exs`, `weida_tests` "every code atom round-trips through strerror" |

The conversions to ssl's forms follow [0047](../decisions/0047-key-material-in-the-language-packages.md):
a certificate is a DER binary and a key stays in its ssl encoding (`{'RSAPrivateKey' \| 'ECPrivateKey' \| 'PrivateKeyInfo', Der}`, which weida reads as PKCS#1, SEC1 and PKCS#8).
A key held by an engine or a `sign_fun` never leaves its store and is case 2 (§3).

A handle is an opaque term. `drop/1` releases one of any kind at once; a dropped or consumed
handle answers every call with the `closed` error without calling C, and its pending waits, in
any process, end with `closed`. Present; `weida_tests` "dropping a replier ends another process's
accept with closed". A handle that is never dropped is released when the garbage collector frees
its last term, also while or after a process waited on it, and also when that process was killed
in the middle of a wait. Present; `weida_tests` "a replier its process left without drop is
released", which observes the release as the requester's `no_reply`: libweida's replier drop
leaves the path registered on its listener (the Rust `Replier` has no `Drop` that unregisters
it), so a second `listener_replier/2` on the same path is `already_registered` after any drop.

## 3. Errors

A failure is `{error, Map}`: `code` is the R9 code as a snake_case atom (`runtime`,
`invalid_fingerprint`, `connection_lost`, ..., and the ABI's own `again`, `intr`, `inval`,
`closed`, `panic`), `message` the `Display` text as a binary, and the thread-local record adds
`loss_cause` (`idle_timeout`, `peer_closed`, `peer_restarting`, `locally_closed`, `reset`,
`transport_error`), `fingerprint`, `max` and `os_code`, each only when it applies. The NIF reads
the record in the same call that failed, so a later call on that scheduler thread cannot overwrite
it. A caller matches on `#{code := untrusted}`; there is no exception for a C failure. A passed
timeout is `{error, timeout}`, the BEAM idiom, kept apart from the map. A wrong argument (a
handle of another kind, a string with a NUL, an option `identity_from/1` does not take) raises
`badarg`; a private key held by an engine or a `sign_fun` raises `{unsupported_key, Reason}`
(case 2 of [0047](../decisions/0047-key-material-in-the-language-packages.md): such a key needs a
signer handle, which weida does not have yet). Present; `weida_tests` "every code
atom round-trips through strerror", "identity_from refuses what it cannot honour",
`weida_quic_tests` "a wrong pin is untrusted and carries the fingerprint".

## 4. Waiting

| Behaviour | Verdict |
| --- | --- |
| No NIF call blocks a scheduler and none is dirty: every C call passes `timeout_ms = 0`, and a try or op wait that would block returns `again` | present by construction (`c_src/weida_nif.c`) |
| The waiting process sleeps in `receive`: `select/2` queues it on the handle, `enif_select_read` watches the descriptor in the VM's own poller and sends `{select, H, Ref, ready_input}`; the process then tries again | present; every unit test past its first `again` |
| Several processes waiting on one handle (two accepts on a replier) form a FIFO: the head is selected, and a head that leaves (`select_cancel/2`, after its notification or its timeout) hands the selection to the next, which is notified at once while the descriptor is still readable (R11) | present; `weida_tests` "two processes accepting on one replier both get a request" |
| The VM selects its own duplicate of the descriptor (`F_DUPFD_CLOEXEC`), closed in the resource's `stop` callback, so dropping or consuming the C handle never closes a descriptor the VM still watches | present by construction |
| The selection is stopped as soon as no process waits, so an idle handle holds no select relation and the garbage collector destructs it; each waiter is monitored, and one that dies leaves the queue in the `down` callback | present; `weida_tests` "a replier its process left without drop is released" |
| A timeout cancels the wait, flushes a notification that raced with it, and returns `{error, timeout}`; for an operation the op is dropped, which cancels it, and the runtime stays usable | present; `weida_tests` "a request that times out leaves the runtime able to shut down" |
| Signals and `EINTR`: the NIF never waits in C, so `intr` never reaches a caller | present by construction |

## 5. Packaging

| Row | Verdict |
| --- | --- |
| One OTP application `weida`: `c_src/build.sh` (a rebar3 pre-compile hook) builds `priv/weida_nif.so` against `$WEIDA_LIB_DIR` and copies `libweida` beside it, found through the NIF's rpath (`$ORIGIN`, `@loader_path`) | present; the `beam-linux-*` jobs |
| `src/weida.app.src` says `0.0.0-dev` and stays there: no release commit touches it | present |
| Elixir calls the Erlang module directly (`:weida`) | present; `smoke.exs` under Elixir 1.20 |
| An idiomatic Elixir module over `:weida` | absent: not part of this package; Elixir callers use `:weida` |
| macOS | absent: `build.sh` has the macOS link flags, but no job builds or runs the NIF there |
| Windows | absent: no Windows BEAM job, and the NIF selects POSIX descriptors |
| hex | absent: nothing of this repository is published to hex |

## 6. What is not mapped

Every pattern, stream, cursor and statistic the C ABI does not map yet is absent here exactly as
it is absent from the C ABI ([weida-c.md](weida-c.md) §5 to §7), and arrives when the ABI maps it.

The C ABI's *config* area is absent here: every row of [weida-c.md](weida-c.md) marked
(*config*), that is the rest of `RuntimeConfig`, `Limits`, `GuaranteeSet`, `ReconnectPolicy`,
the `TransferMeta` setters beyond the content type, `Requester::request_with`, the trace and
achieved getters and `OutgoingTransfer::trace`, `set_priority` and `expire_at`. This package
keeps its two configuration fields and the content type.

The C ABI's *refusals* area is absent here: the rows of [weida-c.md](weida-c.md) marked
(*refusals*), `IncomingRequest::refuse` with the `WEIDA_ERROR_CODE_*` constants and
`IncomingRequest::canceled`. A request is still declined only by dropping it, which answers
`NO_REPLY`.

The C ABI's *streams* area is absent here: the rows of [weida-c.md](weida-c.md) marked
(*streams*), `Requester::open` with `weida_reply_stream` and its `recv`. A request body is still
sent whole, as one buffer.

The C ABI's *stats* area is absent here: the rows of [weida-c.md](weida-c.md) marked
(*stats*), the connection statistics snapshot, the runtime's two counters, connection numbers
and the peer event queue.

The C ABI's *drain* area is absent here: the rows of [weida-c.md](weida-c.md) marked (*drain*),
`weida_runtime_drain` and its `weida_drained` report. A runtime still ends only by shutdown, which
is abortive.

## 7. Sources

`crates/beam/weida/c_src/` (`weida_nif.c`, `build.sh`), `src/` (`weida.erl`, `weida_nif.erl`,
`weida.app.src`), `rebar.config`, `develop.sh`, `smoke.exs`, `test/`;
`.zeughaus-ci/beam-linux-amd64.sh`, `.zeughaus-ci/beam-linux-arm64.sh`; [weida-c.md](weida-c.md);
[docs/c/](../c/README.md); [decisions/0046](../decisions/0046-how-each-language-binds-libweida.md).
