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
%% `loss_cause`, `fingerprint`, `max` and `os_code` where they apply.
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
    identity_generate/0,
    identity_from_pem/2,
    identity_fingerprint/1,
    client_tls_new/1,
    server_tls_new/1,
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

-spec identity_generate() -> {ok, handle()} | failure().
identity_generate() -> weida_nif:identity_generate().

-spec identity_from_pem(iodata(), iodata()) -> {ok, handle()} | failure().
identity_from_pem(Cert, Key) -> weida_nif:identity_from_pem(Cert, Key).

-spec identity_fingerprint(handle()) -> {ok, binary()} | failure().
identity_fingerprint(Identity) -> weida_nif:identity_fingerprint(Identity).

-spec client_tls_new(handle()) -> {ok, handle()} | failure().
client_tls_new(Trust) -> weida_nif:client_tls_new(Trust).

-spec server_tls_new(handle()) -> {ok, handle()} | failure().
server_tls_new(Identity) -> weida_nif:server_tls_new(Identity).

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
