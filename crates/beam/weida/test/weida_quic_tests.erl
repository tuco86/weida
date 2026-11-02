%% The integration tier: Req/Rep over real QUIC on localhost, with a
%% generated identity.
-module(weida_quic_tests).

-include_lib("eunit/include/eunit.hrl").

%% A hang guard only, in seconds (see weida_tests).
-define(HANG, 600).

weida_quic_test_() ->
    [
        {Title, {timeout, ?HANG, Test}}
     || {Title, Test} <- [
            {"a round trip over QUIC with a pinned key", fun pinned_round_trip/0},
            {"a wrong pin is untrusted and carries the fingerprint", fun wrong_pin/0}
        ]
    ].

pinned_round_trip() ->
    {ok, Identity} = weida:identity_generate(),
    {ok, Fingerprint} = weida:identity_fingerprint(Identity),
    #{binding := Binding, replier := Replier, runtime := Runtime} = P = serving(Identity),
    {ok, Trust} = weida:trust_new(),
    ok = weida:trust_add_pin(Trust, Fingerprint),
    {ok, Tls} = weida:client_tls_new(Trust),
    {ok, Requester} = weida:runtime_requester(Runtime, Tls),
    Url = <<"weida://", Fingerprint/binary, "@", (weida:binding_local_addr(Binding))/binary, "/echo">>,
    ok = weida:requester_connect(Requester, Url, infinity),

    Parent = self(),
    Ref = make_ref(),
    spawn_link(fun() ->
        {ok, Request} = weida:replier_accept(Replier, infinity),
        {ok, Body} = weida:incoming_request_take_body(Request),
        {ok, #{payload := Payload}} = weida:incoming_transfer_collect(Body, 1024, infinity),
        {ok, Reply} =
            weida:incoming_request_reply(Request, #{content_type => <<"text/plain">>}, infinity),
        Reversed = list_to_binary(lists:reverse(binary_to_list(Payload))),
        ok = weida:outgoing_transfer_write_all(Reply, Reversed, infinity),
        {ok, Delivery} = weida:outgoing_transfer_finish(Reply),
        ok = weida:delivery_delivered(Delivery, infinity),
        Parent ! {Ref, Payload}
    end),
    {ok, Reply} = weida:requester_request(Requester, <<"ping">>, infinity),
    ?assertMatch({ok, #{content_type := <<"text/plain">>}}, weida:incoming_transfer_meta(Reply)),
    ?assertMatch(
        {ok, #{payload := <<"gnip">>}}, weida:incoming_transfer_collect(Reply, 1024, infinity)
    ),
    receive
        {Ref, Served} -> ?assertEqual(<<"ping">>, Served)
    end,
    close(P#{requester => Requester}).

wrong_pin() ->
    {ok, Identity} = weida:identity_generate(),
    {ok, Fingerprint} = weida:identity_fingerprint(Identity),
    #{binding := Binding, runtime := Runtime} = P = serving(Identity),
    {ok, Other} = weida:identity_generate(),
    {ok, OtherFingerprint} = weida:identity_fingerprint(Other),
    {ok, Trust} = weida:trust_new(),
    ok = weida:trust_add_pin(Trust, OtherFingerprint),
    {ok, Tls} = weida:client_tls_new(Trust),
    {ok, Requester} = weida:runtime_requester(Runtime, Tls),
    Url = <<"weida://", (weida:binding_local_addr(Binding))/binary, "/echo">>,
    Result =
        case weida:requester_connect(Requester, Url, infinity) of
            ok -> weida:requester_request(Requester, <<"hello">>, infinity);
            Failure -> Failure
        end,
    ?assertMatch({error, #{code := untrusted, fingerprint := Fingerprint}}, Result),
    close(P#{requester => Requester}).

%% A runtime serving QUIC on localhost with Identity, a replier at /echo.
serving(Identity) ->
    {ok, Runtime} = weida:runtime_owned(#{}),
    {ok, Listener} = weida:runtime_listener(Runtime),
    {ok, ServerTls} = weida:server_tls_new(Identity),
    {ok, Binding} = weida:listener_bind_quic(Listener, <<"127.0.0.1:0">>, ServerTls, infinity),
    {ok, Replier} = weida:listener_replier(Listener, <<"/echo">>),
    #{runtime => Runtime, listener => Listener, binding => Binding, replier => Replier}.

close(#{runtime := Runtime} = P) ->
    [weida:drop(maps:get(K, P)) || K <- [requester, replier, binding, listener]],
    ok = weida:runtime_shutdown(Runtime, infinity).
