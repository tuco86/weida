%% weida for Erlang and Elixir (docs/libraries/weida-beam.md): one function
%% per function of the C ABI's slice (docs/c/README.md), named like it
%% without the `weida_` prefix. From Elixir it is `:weida`.
%%
%% A handle is an opaque term. drop/1 releases it at once; one that is
%% never dropped is released when the garbage collector frees its last
%% term. A dropped or consumed handle answers every call with the `closed`
%% error.
%%
%% Every waiting function takes a trailing Timeout (milliseconds or
%% `infinity`) and returns {error, timeout} once it passed. No call blocks
%% a scheduler: the NIF only ever tries, and the calling process waits in a
%% `receive` for the handle's readiness descriptor. An operation (a bind, a
%% dial, a request, a reply, a collect, a delivery, a shutdown) is started
%% once and dropped on every return path, which cancels it when it did not
%% finish (R3b).
%%
%% Every other failure is {error, Error}: Error is a map with the R9 code as
%% a snake_case atom (`untrusted`, `limit_exceeded`, `closed`, ...) under
%% `code`, the `Display` text under `message`, and the thread-local record's
%% `loss_cause`, `fingerprint`, `max` and `os_code` where they apply. A
%% caller's mistake is an error of class `error`: `badarg`, or
%% {unsupported_key, Reason} for a private key that never leaves its engine
%% or sign_fun (docs/decisions/0047, case 2). Key material in ssl's forms
%% (identity_from/1, trust_add_anchor/2, identity_certificates/1 and the
%% meta's `peer_chain`) is DER or PEM, as docs/decisions/0047 fixes.
-module(weida).

-export([
    version/0,
    strerror/1,
    drop/1,
    runtime_owned/1,
    runtime_listener/1,
    runtime_requester/2,
    runtime_shutdown/2,
    trust_new/0,
    trust_add_pin/2,
    trust_add_anchor/2,
    identity_generate/0,
    identity_generate_for/1,
    identity_from_pem/2,
    identity_from_pem_file/1,
    identity_from_pem_files/2,
    identity_from/1,
    identity_certificate_pem/1,
    identity_certificates/1,
    identity_to_pem/1,
    identity_fingerprint/1,
    client_tls_new/1,
    server_tls_new/1,
    client_tls_set_identity/2,
    server_tls_require_client/2,
    server_tls_require_any_key/1,
    listener_bind_quic/4,
    listener_bind_inproc/2,
    listener_replier/2,
    binding_local_addr/1,
    local_binding_bus/1,
    replier_path/1,
    replier_accept/2,
    incoming_request_meta/1,
    incoming_request_take_body/1,
    incoming_request_reply/3,
    requester_connect/3,
    requester_request/3,
    incoming_transfer_meta/1,
    incoming_transfer_read/3,
    incoming_transfer_collect/3,
    outgoing_transfer_write/3,
    outgoing_transfer_write_all/3,
    outgoing_transfer_finish/1,
    delivery_delivered/2
]).

-export_type([handle/0, error/0, meta/0, message/0, transfer_meta/0, runtime_config/0]).

-type handle() :: reference().
-type error() :: #{
    code := atom(),
    message := binary(),
    loss_cause => atom(),
    fingerprint => binary(),
    max => non_neg_integer(),
    os_code => integer()
}.
-type failure() :: {error, error()}.
-type wait_failure() :: {error, error() | timeout}.
-type meta() :: #{
    endpoint := binary() | undefined,
    content_type := binary() | undefined,
    topic := binary() | undefined,
    peer_fingerprint := binary() | undefined,
    peer_chain := [binary()] | undefined,
    content_len := non_neg_integer() | undefined,
    sequence := non_neg_integer() | undefined
}.
-type message() :: #{payload := binary(), meta := meta()}.
-type transfer_meta() :: #{content_type => iodata()}.
-type runtime_config() :: #{
    worker_threads => non_neg_integer(), shutdown_timeout_ms => non_neg_integer()
}.

%% ---- library ----

%% The library's version, e.g. <<"0.1.0-alpha.11-dev">>.
-spec version() -> binary().
version() -> weida_nif:version().

%% The description of a status code: an R9 atom or an integer of either
%% sign; <<"unknown error">> for an integer this library does not define.
-spec strerror(atom() | integer()) -> binary().
strerror(Code) -> weida_nif:strerror(Code).

%% Releases a handle of any kind at once (R1); its pending waits end with
%% the `closed` error. Dropping a handle twice does nothing.
-spec drop(handle()) -> ok.
drop(H) -> weida_nif:drop(H).

%% ---- runtime ----

-spec runtime_owned(runtime_config()) -> {ok, handle()} | failure().
runtime_owned(Config) -> weida_nif:runtime_owned(Config).

-spec runtime_listener(handle()) -> {ok, handle()} | failure().
runtime_listener(Runtime) -> weida_nif:runtime_listener(Runtime).

-spec runtime_requester(handle(), handle()) -> {ok, handle()} | failure().
runtime_requester(Runtime, ClientTls) -> weida_nif:runtime_requester(Runtime, ClientTls).

%% Shuts the runtime down; consumes it (R6). Waiting is optional, the
%% runtime is closed either way.
-spec runtime_shutdown(handle(), timeout()) -> ok | wait_failure().
runtime_shutdown(Runtime, Timeout) ->
    op(weida_nif:runtime_shutdown(Runtime), fun weida_nif:runtime_shutdown_op_wait/1, Timeout).

%% ---- TLS values ----

-spec trust_new() -> {ok, handle()} | failure().
trust_new() -> weida_nif:trust_new().

-spec trust_add_pin(handle(), iodata()) -> ok | failure().
trust_add_pin(Trust, Fingerprint) -> weida_nif:trust_add_pin(Trust, Fingerprint).

%% Adds the certificate authorities of Anchor: PEM iodata, {cacerts, [Der]}
%% or {cacertfile, Path}. The PEM is copied and the file named is read, and
%% either is parsed, at each dial.
-spec trust_add_anchor(
    handle(), iodata() | {cacerts, [binary(), ...]} | {cacertfile, iodata()}
) -> ok | failure().
trust_add_anchor(Trust, {cacertfile, Path}) ->
    weida_nif:trust_add_anchor_file(Trust, Path);
trust_add_anchor(Trust, {cacerts, [_ | _] = Ders}) ->
    weida_nif:trust_add_anchor(Trust, cert_pem(Ders));
trust_add_anchor(Trust, Pem) ->
    weida_nif:trust_add_anchor(Trust, Pem).

-spec identity_generate() -> {ok, handle()} | failure().
identity_generate() -> weida_nif:identity_generate().

%% A fresh identity whose certificate names Names (DNS names and IP
%% literals); no names behaves as identity_generate/0.
-spec identity_generate_for([iodata()]) -> {ok, handle()} | failure().
identity_generate_for(Names) -> weida_nif:identity_generate_for(Names).

-spec identity_from_pem(iodata(), iodata()) -> {ok, handle()} | failure().
identity_from_pem(Cert, Key) -> weida_nif:identity_from_pem(Cert, Key).

%% Nothing is read here; a missing or bad file is the `tls` error at first
%% use (identity_fingerprint/1, a bind or a dial).
-spec identity_from_pem_file(iodata()) -> {ok, handle()} | failure().
identity_from_pem_file(Path) -> weida_nif:identity_from_pem_file(Path).

-spec identity_from_pem_files(iodata(), iodata()) -> {ok, handle()} | failure().
identity_from_pem_files(Cert, Key) -> weida_nif:identity_from_pem_files(Cert, Key).

%% An identity from ssl's certificate options, a proplist (an Elixir keyword
%% list) or a map: `certfile` and `keyfile`, `certfile` alone (the chain and
%% the key in one file, as ssl defaults `keyfile` to `certfile`), or `cert`
%% (a DER or a chain of them, leaf first) and `key` ({Type, Der} with Type
%% `RSAPrivateKey`, `ECPrivateKey` or `PrivateKeyInfo`). Any other key,
%% `password` included (weida reads no encrypted keys), and any other
%% combination is `badarg`; a `key` held by an engine or a sign_fun is
%% {unsupported_key, Reason} (docs/decisions/0047, case 2).
-spec identity_from(
    #{
        certfile => iodata(),
        keyfile => iodata(),
        cert => binary() | [binary(), ...],
        key => {'RSAPrivateKey' | 'ECPrivateKey' | 'PrivateKeyInfo', binary()} | map()
    }
    | [{certfile | keyfile | cert | key, term()}]
) -> {ok, handle()} | failure().
identity_from(Opts) when is_list(Opts) ->
    identity_from(maps:from_list(Opts));
identity_from(Opts) when is_map(Opts) ->
    case maps:keys(Opts) -- [certfile, keyfile, cert, key] of
        [] -> identity_from_known(Opts);
        _ -> erlang:error(badarg, [Opts])
    end;
identity_from(Opts) ->
    erlang:error(badarg, [Opts]).

identity_from_known(#{certfile := Cert, keyfile := Key} = Opts) when map_size(Opts) =:= 2 ->
    identity_from_pem_files(Cert, Key);
identity_from_known(#{certfile := Cert} = Opts) when map_size(Opts) =:= 1 ->
    identity_from_pem_file(Cert);
identity_from_known(#{cert := Cert, key := Key} = Opts) when map_size(Opts) =:= 2 ->
    identity_from_pem(cert_pem(Cert), key_pem(Key));
identity_from_known(Opts) ->
    erlang:error(badarg, [Opts]).

%% The chain only, as PEM, read anew on every call.
-spec identity_certificate_pem(handle()) -> {ok, binary()} | failure().
identity_certificate_pem(Identity) -> weida_nif:identity_certificate_pem(Identity).

%% The chain, leaf first, as DER for public_key:pkix_decode_cert/2.
-spec identity_certificates(handle()) -> {ok, [binary()]} | failure().
identity_certificates(Identity) ->
    case identity_certificate_pem(Identity) of
        {ok, Pem} -> {ok, [Der || {'Certificate', Der, not_encrypted} <- public_key:pem_decode(Pem)]};
        Failure -> Failure
    end.

%% The chain and the private key as one PEM document, read anew on every
%% call. The binary holds the private key; an Erlang binary cannot be wiped
%% (docs/decisions/0047).
-spec identity_to_pem(handle()) -> {ok, binary()} | failure().
identity_to_pem(Identity) -> weida_nif:identity_to_pem(Identity).

-spec identity_fingerprint(handle()) -> {ok, binary()} | failure().
identity_fingerprint(Identity) -> weida_nif:identity_fingerprint(Identity).

-spec client_tls_new(handle()) -> {ok, handle()} | failure().
client_tls_new(Trust) -> weida_nif:client_tls_new(Trust).

-spec server_tls_new(handle()) -> {ok, handle()} | failure().
server_tls_new(Identity) -> weida_nif:server_tls_new(Identity).

%% The identity is copied; it affects requesters made afterwards.
-spec client_tls_set_identity(handle(), handle()) -> ok | failure().
client_tls_set_identity(Tls, Identity) -> weida_nif:client_tls_set_identity(Tls, Identity).

%% Every client must present a key Trust accepts, pinned or chaining to an
%% anchor. The trust is copied; an empty one makes the bind fail with `tls`.
%% The last of this and server_tls_require_any_key/1 wins.
-spec server_tls_require_client(handle(), handle()) -> ok | failure().
server_tls_require_client(Tls, Trust) -> weida_nif:server_tls_require_client(Tls, Trust).

%% Every client must present a key and prove it holds it, whatever the key.
-spec server_tls_require_any_key(handle()) -> ok | failure().
server_tls_require_any_key(Tls) -> weida_nif:server_tls_require_any_key(Tls).

%% ssl's `cert` (a DER or a chain of them, leaf first) as PEM.
cert_pem(Der) when is_binary(Der) ->
    cert_pem([Der]);
cert_pem(Ders) when is_list(Ders), length(Ders) > 0 ->
    case lists:all(fun erlang:is_binary/1, Ders) of
        true -> public_key:pem_encode([{'Certificate', Der, not_encrypted} || Der <- Ders]);
        false -> erlang:error(badarg, [Ders])
    end;
cert_pem(Other) ->
    erlang:error(badarg, [Other]).

%% ssl's `key` as PEM: weida reads PKCS#1, SEC1 and PKCS#8, so the key
%% keeps its encoding.
key_pem({Type, Der}) when
    is_binary(Der),
    (Type =:= 'RSAPrivateKey' orelse Type =:= 'ECPrivateKey' orelse Type =:= 'PrivateKeyInfo')
->
    public_key:pem_encode([{Type, Der, not_encrypted}]);
key_pem(Key) when is_map(Key) ->
    erlang:error(
        {unsupported_key,
            <<"a key held by an engine or a sign_fun never leaves its store and needs a signer handle, which weida does not have yet (docs/decisions/0047, case 2)">>}
    );
key_pem(Other) ->
    erlang:error(badarg, [Other]).

%% ---- listener and bindings ----

-spec listener_bind_quic(handle(), iodata(), handle(), timeout()) ->
    {ok, handle()} | wait_failure().
listener_bind_quic(Listener, Addr, ServerTls, Timeout) ->
    op(
        weida_nif:listener_bind_quic(Listener, Addr, ServerTls),
        fun weida_nif:listener_bind_quic_op_wait/1,
        Timeout
    ).

-spec listener_bind_inproc(handle(), iodata()) -> {ok, handle()} | failure().
listener_bind_inproc(Listener, Bus) -> weida_nif:listener_bind_inproc(Listener, Bus).

-spec listener_replier(handle(), iodata()) -> {ok, handle()} | failure().
listener_replier(Listener, Path) -> weida_nif:listener_replier(Listener, Path).

-spec binding_local_addr(handle()) -> binary() | failure().
binding_local_addr(Binding) -> weida_nif:binding_local_addr(Binding).

-spec local_binding_bus(handle()) -> binary() | failure().
local_binding_bus(Binding) -> weida_nif:local_binding_bus(Binding).

%% ---- replier and incoming request ----

-spec replier_path(handle()) -> binary() | failure().
replier_path(Replier) -> weida_nif:replier_path(Replier).

%% Takes the next request, as soon as its header arrived. Several processes
%% may accept on one replier; they are served in the order they waited.
-spec replier_accept(handle(), timeout()) -> {ok, handle()} | wait_failure().
replier_accept(Replier, Timeout) ->
    wait(Replier, fun() -> weida_nif:replier_accept(Replier) end, deadline(Timeout)).

-spec incoming_request_meta(handle()) -> {ok, meta()} | failure().
incoming_request_meta(Request) -> weida_nif:incoming_request_meta(Request).

-spec incoming_request_take_body(handle()) -> {ok, handle()} | failure().
incoming_request_take_body(Request) -> weida_nif:incoming_request_take_body(Request).

%% Opens the reply stream; consumes the request (R6).
-spec incoming_request_reply(handle(), transfer_meta(), timeout()) ->
    {ok, handle()} | wait_failure().
incoming_request_reply(Request, Meta, Timeout) ->
    op(
        weida_nif:incoming_request_reply(Request, Meta),
        fun weida_nif:incoming_request_reply_op_wait/1,
        Timeout
    ).

%% ---- requester ----

-spec requester_connect(handle(), iodata(), timeout()) -> ok | wait_failure().
requester_connect(Requester, Url, Timeout) ->
    op(weida_nif:requester_connect(Requester, Url), fun weida_nif:requester_connect_op_wait/1, Timeout).

%% Sends Body as one request and returns the reply, an incoming transfer,
%% as soon as its header arrived.
-spec requester_request(handle(), iodata(), timeout()) -> {ok, handle()} | wait_failure().
requester_request(Requester, Body, Timeout) ->
    op(weida_nif:requester_request(Requester, Body), fun weida_nif:requester_request_op_wait/1, Timeout).

%% ---- incoming transfer ----

-spec incoming_transfer_meta(handle()) -> {ok, meta()} | failure().
incoming_transfer_meta(Transfer) -> weida_nif:incoming_transfer_meta(Transfer).

%% Reads up to Max bytes of the payload; `eof` at its end. A transfer the
%% peer canceled fails with `canceled` and never reads as an end.
-spec incoming_transfer_read(handle(), pos_integer(), timeout()) ->
    {ok, binary()} | eof | wait_failure().
incoming_transfer_read(Transfer, Max, Timeout) ->
    wait(Transfer, fun() -> weida_nif:incoming_transfer_read(Transfer, Max) end, deadline(Timeout)).

%% Reads the whole remaining payload, refusing more than Max bytes with
%% `limit_exceeded`; consumes the transfer (R6).
-spec incoming_transfer_collect(handle(), non_neg_integer(), timeout()) ->
    {ok, message()} | wait_failure().
incoming_transfer_collect(Transfer, Max, Timeout) ->
    op(
        weida_nif:incoming_transfer_collect(Transfer, Max),
        fun weida_nif:incoming_transfer_collect_op_wait/1,
        Timeout
    ).

%% ---- outgoing transfer and delivery ----

%% Writes as much of Data as flow control takes, at least one byte unless
%% Data is empty, and returns the count.
-spec outgoing_transfer_write(handle(), iodata(), timeout()) ->
    {ok, non_neg_integer()} | wait_failure().
outgoing_transfer_write(Transfer, Data, Timeout) ->
    wait(Transfer, fun() -> weida_nif:outgoing_transfer_write(Transfer, Data) end, deadline(Timeout)).

%% Writes all of Data: the loop R3c leaves to the caller.
-spec outgoing_transfer_write_all(handle(), iodata(), timeout()) -> ok | wait_failure().
outgoing_transfer_write_all(Transfer, Data, Timeout) ->
    write_all(Transfer, iolist_to_binary(Data), deadline(Timeout)).

write_all(_Transfer, <<>>, _Deadline) ->
    ok;
write_all(Transfer, Data, Deadline) ->
    case wait(Transfer, fun() -> weida_nif:outgoing_transfer_write(Transfer, Data) end, Deadline) of
        {ok, N} ->
            <<_:N/binary, Rest/binary>> = Data,
            write_all(Transfer, Rest, Deadline);
        Failure ->
            Failure
    end.

%% Ends the payload and returns its delivery receipt; consumes the
%% transfer (R6).
-spec outgoing_transfer_finish(handle()) -> {ok, handle()} | failure().
outgoing_transfer_finish(Transfer) -> weida_nif:outgoing_transfer_finish(Transfer).

%% Waits until the peer's transport acknowledged the whole payload;
%% consumes the receipt (R6).
-spec delivery_delivered(handle(), timeout()) -> ok | wait_failure().
delivery_delivered(Delivery, Timeout) ->
    op(weida_nif:delivery_delivered(Delivery), fun weida_nif:delivery_delivered_op_wait/1, Timeout).

%% ---- waiting ----

%% Waits for the op a start function returned and drops it on every path.
op({ok, Op}, Wait, Timeout) ->
    try
        wait(Op, fun() -> Wait(Op) end, deadline(Timeout))
    after
        weida_nif:drop(Op)
    end;
op(Failure, _Wait, _Timeout) ->
    Failure.

deadline(infinity) ->
    infinity;
deadline(Ms) when is_integer(Ms), Ms >= 0 ->
    erlang:monotonic_time(millisecond) + Ms.

remaining(infinity) ->
    infinity;
remaining(Deadline) ->
    max(0, Deadline - erlang:monotonic_time(millisecond)).

%% Runs Attempt, a NIF try or op wait on H, until it returns anything but
%% `again`, waiting for H's descriptor in between (R11). A notification
%% may be stale; the next attempt then returns `again` and the process
%% waits again.
wait(H, Attempt, Deadline) ->
    case Attempt() of
        again -> wait_ready(H, Attempt, Deadline);
        Result -> Result
    end.

wait_ready(H, Attempt, Deadline) ->
    case remaining(Deadline) of
        0 ->
            {error, timeout};
        Remaining ->
            Ref = make_ref(),
            case weida_nif:select(H, Ref) of
                ok ->
                    receive
                        {select, H, Ref, ready_input} ->
                            leave(H, Ref),
                            wait(H, Attempt, Deadline);
                        {weida_closed, Ref} ->
                            leave(H, Ref),
                            wait(H, Attempt, Deadline)
                    after Remaining ->
                        leave(H, Ref),
                        {error, timeout}
                    end;
                Failure ->
                    Failure
            end
    end.

%% Leaves H's queue of waiters, which hands H's readiness to the next one,
%% and flushes a notification that raced with leaving.
leave(H, Ref) ->
    ok = weida_nif:select_cancel(H, Ref),
    receive
        {select, H, Ref, _} -> ok
    after 0 -> ok
    end,
    receive
        {weida_closed, Ref} -> ok
    after 0 -> ok
    end.
