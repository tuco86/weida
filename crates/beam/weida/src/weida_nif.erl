%% The NIF over libweida (c_src/weida_nif.c): one function per weida.h
%% function of the slice, called by the module `weida`, which is the API.
%% Every function returns at once: a try or an op wait that would block
%% returns `again`, and `weida` waits for the handle in a `receive`.
-module(weida_nif).

-export([
    version/0,
    strerror/1,
    drop/1,
    select/2,
    select_cancel/2,
    runtime_owned/1,
    runtime_listener/1,
    runtime_requester/2,
    runtime_shutdown/1,
    runtime_shutdown_op_wait/1,
    trust_new/0,
    trust_add_pin/2,
    identity_generate/0,
    identity_from_pem/2,
    identity_fingerprint/1,
    client_tls_new/1,
    server_tls_new/1,
    listener_bind_quic/3,
    listener_bind_quic_op_wait/1,
    listener_bind_inproc/2,
    listener_replier/2,
    binding_local_addr/1,
    local_binding_bus/1,
    replier_path/1,
    replier_accept/1,
    incoming_request_meta/1,
    incoming_request_take_body/1,
    incoming_request_reply/2,
    incoming_request_reply_op_wait/1,
    requester_connect/2,
    requester_connect_op_wait/1,
    requester_request/2,
    requester_request_op_wait/1,
    incoming_transfer_meta/1,
    incoming_transfer_read/2,
    incoming_transfer_collect/2,
    incoming_transfer_collect_op_wait/1,
    outgoing_transfer_write/2,
    outgoing_transfer_finish/1,
    delivery_delivered/1,
    delivery_delivered_op_wait/1
]).

-nifs([
    version/0,
    strerror/1,
    drop/1,
    select/2,
    select_cancel/2,
    runtime_owned/1,
    runtime_listener/1,
    runtime_requester/2,
    runtime_shutdown/1,
    runtime_shutdown_op_wait/1,
    trust_new/0,
    trust_add_pin/2,
    identity_generate/0,
    identity_from_pem/2,
    identity_fingerprint/1,
    client_tls_new/1,
    server_tls_new/1,
    listener_bind_quic/3,
    listener_bind_quic_op_wait/1,
    listener_bind_inproc/2,
    listener_replier/2,
    binding_local_addr/1,
    local_binding_bus/1,
    replier_path/1,
    replier_accept/1,
    incoming_request_meta/1,
    incoming_request_take_body/1,
    incoming_request_reply/2,
    incoming_request_reply_op_wait/1,
    requester_connect/2,
    requester_connect_op_wait/1,
    requester_request/2,
    requester_request_op_wait/1,
    incoming_transfer_meta/1,
    incoming_transfer_read/2,
    incoming_transfer_collect/2,
    incoming_transfer_collect_op_wait/1,
    outgoing_transfer_write/2,
    outgoing_transfer_finish/1,
    delivery_delivered/1,
    delivery_delivered_op_wait/1
]).

-on_load(init/0).

init() ->
    erlang:load_nif(filename:join(code:priv_dir(weida), "weida_nif"), 0).

version() -> erlang:nif_error(nif_not_loaded).
strerror(_Code) -> erlang:nif_error(nif_not_loaded).
drop(_H) -> erlang:nif_error(nif_not_loaded).
select(_H, _Ref) -> erlang:nif_error(nif_not_loaded).
select_cancel(_H, _Ref) -> erlang:nif_error(nif_not_loaded).
runtime_owned(_Config) -> erlang:nif_error(nif_not_loaded).
runtime_listener(_Rt) -> erlang:nif_error(nif_not_loaded).
runtime_requester(_Rt, _Tls) -> erlang:nif_error(nif_not_loaded).
runtime_shutdown(_Rt) -> erlang:nif_error(nif_not_loaded).
runtime_shutdown_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
trust_new() -> erlang:nif_error(nif_not_loaded).
trust_add_pin(_Trust, _Fingerprint) -> erlang:nif_error(nif_not_loaded).
identity_generate() -> erlang:nif_error(nif_not_loaded).
identity_from_pem(_Cert, _Key) -> erlang:nif_error(nif_not_loaded).
identity_fingerprint(_Identity) -> erlang:nif_error(nif_not_loaded).
client_tls_new(_Trust) -> erlang:nif_error(nif_not_loaded).
server_tls_new(_Identity) -> erlang:nif_error(nif_not_loaded).
listener_bind_quic(_L, _Addr, _Tls) -> erlang:nif_error(nif_not_loaded).
listener_bind_quic_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
listener_bind_inproc(_L, _Bus) -> erlang:nif_error(nif_not_loaded).
listener_replier(_L, _Path) -> erlang:nif_error(nif_not_loaded).
binding_local_addr(_B) -> erlang:nif_error(nif_not_loaded).
local_binding_bus(_B) -> erlang:nif_error(nif_not_loaded).
replier_path(_R) -> erlang:nif_error(nif_not_loaded).
replier_accept(_R) -> erlang:nif_error(nif_not_loaded).
incoming_request_meta(_Q) -> erlang:nif_error(nif_not_loaded).
incoming_request_take_body(_Q) -> erlang:nif_error(nif_not_loaded).
incoming_request_reply(_Q, _Meta) -> erlang:nif_error(nif_not_loaded).
incoming_request_reply_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
requester_connect(_R, _Url) -> erlang:nif_error(nif_not_loaded).
requester_connect_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
requester_request(_R, _Body) -> erlang:nif_error(nif_not_loaded).
requester_request_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
incoming_transfer_meta(_T) -> erlang:nif_error(nif_not_loaded).
incoming_transfer_read(_T, _Max) -> erlang:nif_error(nif_not_loaded).
incoming_transfer_collect(_T, _Max) -> erlang:nif_error(nif_not_loaded).
incoming_transfer_collect_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
outgoing_transfer_write(_T, _Data) -> erlang:nif_error(nif_not_loaded).
outgoing_transfer_finish(_T) -> erlang:nif_error(nif_not_loaded).
delivery_delivered(_D) -> erlang:nif_error(nif_not_loaded).
delivery_delivered_op_wait(_Op) -> erlang:nif_error(nif_not_loaded).
