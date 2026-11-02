%% The integration tier: Req/Rep over real QUIC on localhost, with a
%% generated identity, and mutual TLS: required client keys, anchors and the
%% presented chain.
-module(weida_quic_tests).

-include_lib("eunit/include/eunit.hrl").

%% A hang guard only, in seconds (see weida_tests).
-define(HANG, 600).

weida_quic_test_() ->
    [
        {Title, {timeout, ?HANG, Test}}
     || {Title, Test} <- [
            {"a round trip over QUIC with a pinned key", fun pinned_round_trip/0},
            {"a wrong pin is untrusted and carries the fingerprint", fun wrong_pin/0},
            {"a pinned client key is accepted and the server sees its key and chain",
                fun pinned_client/0},
            {"an anonymous or unknown client is refused with tls and nothing reaches the server",
                fun refused_clients/0},
            {"a binding that requires clients from an empty trust does not bind",
                fun empty_client_trust/0},
            {"any key exposes the fingerprint and the chain", fun any_key/0},
            {"an anchor accepts the dialled name and refuses another", fun anchors/0},
            {"identity and anchor files read back", fun files/0}
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

pinned_client() ->
    {ok, Server} = weida:identity_generate(),
    {ok, ServerFingerprint} = weida:identity_fingerprint(Server),
    {ok, Client} = weida:identity_generate(),
    {ok, ClientFingerprint} = weida:identity_fingerprint(Client),
    {ok, [ClientDer]} = weida:identity_certificates(Client),
    {ok, ServerTls} = weida:server_tls_new(Server),
    ok = weida:server_tls_require_client(ServerTls, pinning(ClientFingerprint)),
    #{binding := Binding, replier := Replier, runtime := Runtime} = P = serving_tls(ServerTls),
    Ref = serve_meta(Replier),
    Requester = requester(Runtime, pinning(ServerFingerprint), Client),
    round_trip(Requester, url(Binding)),
    receive
        {Ref, Meta} ->
            ?assertMatch(
                #{peer_fingerprint := ClientFingerprint, peer_chain := [ClientDer]}, Meta
            )
    end,
    close(P#{requester => Requester}).

refused_clients() ->
    {ok, Server} = weida:identity_generate(),
    {ok, ServerFingerprint} = weida:identity_fingerprint(Server),
    {ok, Client} = weida:identity_generate(),
    {ok, ClientFingerprint} = weida:identity_fingerprint(Client),
    {ok, Stranger} = weida:identity_generate(),
    {ok, ServerTls} = weida:server_tls_new(Server),
    ok = weida:server_tls_require_client(ServerTls, pinning(ClientFingerprint)),
    #{binding := Binding, replier := Replier, runtime := Runtime} = P = serving_tls(ServerTls),
    Anonymous = requester(Runtime, pinning(ServerFingerprint)),
    refused(Anonymous, url(Binding)),
    Unknown = requester(Runtime, pinning(ServerFingerprint), Stranger),
    refused(Unknown, url(Binding)),
    ?assertEqual({error, timeout}, weida:replier_accept(Replier, 200)),
    weida:drop(Unknown),
    close(P#{requester => Anonymous}).

empty_client_trust() ->
    {ok, Server} = weida:identity_generate(),
    {ok, Runtime} = weida:runtime_owned(#{}),
    {ok, Listener} = weida:runtime_listener(Runtime),
    {ok, ServerTls} = weida:server_tls_new(Server),
    {ok, Empty} = weida:trust_new(),
    ok = weida:server_tls_require_client(ServerTls, Empty),
    Result = weida:listener_bind_quic(Listener, <<"127.0.0.1:0">>, ServerTls, infinity),
    ?assertMatch({error, #{code := tls, message := <<_/binary>>}}, Result),
    {error, #{message := Message}} = Result,
    ?assertNotEqual(nomatch, binary:match(Message, <<"client trust is empty">>)),
    weida:drop(Listener),
    ok = weida:runtime_shutdown(Runtime, infinity).

any_key() ->
    {ok, Server} = weida:identity_generate(),
    {ok, ServerFingerprint} = weida:identity_fingerprint(Server),
    {ok, Client} = weida:identity_generate(),
    {ok, ClientFingerprint} = weida:identity_fingerprint(Client),
    {ok, ServerTls} = weida:server_tls_new(Server),
    ok = weida:server_tls_require_any_key(ServerTls),
    #{binding := Binding, replier := Replier, runtime := Runtime} = P = serving_tls(ServerTls),
    Anonymous = requester(Runtime, pinning(ServerFingerprint)),
    refused(Anonymous, url(Binding)),
    Ref = serve_meta(Replier),
    Requester = requester(Runtime, pinning(ServerFingerprint), Client),
    round_trip(Requester, url(Binding)),
    receive
        {Ref, Meta} ->
            ?assertMatch(#{peer_fingerprint := ClientFingerprint, peer_chain := [_]}, Meta)
    end,
    weida:drop(Anonymous),
    close(P#{requester => Requester}).

anchors() ->
    {ok, Named} = weida:identity_generate_for([<<"127.0.0.1">>]),
    {ok, [NamedDer]} = weida:identity_certificates(Named),
    #{binding := Binding, replier := Replier, runtime := Runtime} = P = serving(Named),
    Ref = serve_meta(Replier),
    Requester = requester(Runtime, anchored(NamedDer)),
    round_trip(Requester, url(Binding)),
    receive
        {Ref, Meta} -> ?assertMatch(#{peer_chain := undefined}, Meta)
    end,
    close(P#{requester => Requester}),

    {ok, Other} = weida:identity_generate_for([<<"localhost">>]),
    {ok, OtherFingerprint} = weida:identity_fingerprint(Other),
    {ok, [OtherDer]} = weida:identity_certificates(Other),
    #{binding := OtherBinding, runtime := OtherRuntime} = OtherP = serving(Other),
    OtherRequester = requester(OtherRuntime, anchored(OtherDer)),
    Result =
        case weida:requester_connect(OtherRequester, url(OtherBinding), infinity) of
            ok -> weida:requester_request(OtherRequester, <<"hello">>, infinity);
            Failure -> Failure
        end,
    ?assertMatch({error, #{code := untrusted, fingerprint := OtherFingerprint}}, Result),
    close(OtherP#{requester => OtherRequester}).

files() ->
    Dir = filename:join(os:getenv("TMPDIR", "/tmp"), "weida-beam-" ++ os:getpid()),
    ok = file:make_dir(Dir),
    try
        {ok, Generated} = weida:identity_generate_for([<<"127.0.0.1">>]),
        {ok, Fingerprint} = weida:identity_fingerprint(Generated),
        {ok, IdPem} = weida:identity_to_pem(Generated),
        {ok, CertPem} = weida:identity_certificate_pem(Generated),
        IdPath = filename:join(Dir, "id.pem"),
        CertPath = filename:join(Dir, "cert.pem"),
        ok = file:write_file(IdPath, IdPem),
        ok = file:write_file(CertPath, CertPem),
        {ok, OneFile} = weida:identity_from(#{certfile => IdPath}),
        ?assertEqual({ok, Fingerprint}, weida:identity_fingerprint(OneFile)),
        {ok, TwoFiles} = weida:identity_from([{certfile, CertPath}, {keyfile, IdPath}]),
        ?assertEqual({ok, Fingerprint}, weida:identity_fingerprint(TwoFiles)),

        #{binding := Binding, replier := Replier, runtime := Runtime} = P = serving(OneFile),
        Ref = serve_meta(Replier),
        {ok, Anchors} = weida:trust_new(),
        ok = weida:trust_add_anchor(Anchors, {cacertfile, CertPath}),
        Requester = requester(Runtime, Anchors),
        round_trip(Requester, url(Binding)),
        receive
            {Ref, _} -> ok
        end,
        close(P#{requester => Requester}),

        {ok, Missing} = weida:identity_from_pem_file(filename:join(Dir, "missing.pem")),
        ?assertMatch({error, #{code := tls}}, weida:identity_fingerprint(Missing))
    after
        file:del_dir_r(Dir)
    end.

%% A runtime serving QUIC on localhost with Identity, a replier at /echo.
serving(Identity) ->
    {ok, ServerTls} = weida:server_tls_new(Identity),
    serving_tls(ServerTls).

%% The same with the server terms ServerTls.
serving_tls(ServerTls) ->
    {ok, Runtime} = weida:runtime_owned(#{}),
    {ok, Listener} = weida:runtime_listener(Runtime),
    {ok, Binding} = weida:listener_bind_quic(Listener, <<"127.0.0.1:0">>, ServerTls, infinity),
    {ok, Replier} = weida:listener_replier(Listener, <<"/echo">>),
    #{runtime => Runtime, listener => Listener, binding => Binding, replier => Replier}.

%% A linked process that answers one request with <<"hi">> and, once the
%% reply is delivered, sends the parent {Ref, Meta}, the request's
%% metadata; returns Ref. The parent receives it before it closes, so the
%% delivery does not race the close.
serve_meta(Replier) ->
    Parent = self(),
    Ref = make_ref(),
    spawn_link(fun() ->
        {ok, Request} = weida:replier_accept(Replier, infinity),
        {ok, Meta} = weida:incoming_request_meta(Request),
        {ok, Body} = weida:incoming_request_take_body(Request),
        {ok, _} = weida:incoming_transfer_collect(Body, 1024, infinity),
        {ok, Reply} = weida:incoming_request_reply(Request, #{}, infinity),
        ok = weida:outgoing_transfer_write_all(Reply, <<"hi">>, infinity),
        {ok, Delivery} = weida:outgoing_transfer_finish(Reply),
        ok = weida:delivery_delivered(Delivery, infinity),
        Parent ! {Ref, Meta}
    end),
    Ref.

%% connect, then request: the result must be the `tls` failure.
refused(Requester, Url) ->
    Result =
        case weida:requester_connect(Requester, Url, infinity) of
            ok -> weida:requester_request(Requester, <<"hello">>, infinity);
            Failure -> Failure
        end,
    ?assertMatch({error, #{code := tls}}, Result),
    Result.

round_trip(Requester, Url) ->
    ok = weida:requester_connect(Requester, Url, infinity),
    {ok, Reply} = weida:requester_request(Requester, <<"ping">>, infinity),
    ?assertMatch({ok, #{payload := <<"hi">>}}, weida:incoming_transfer_collect(Reply, 1024, infinity)).

url(Binding) ->
    <<"weida://", (weida:binding_local_addr(Binding))/binary, "/echo">>.

%% A trust pinning Fingerprint.
pinning(Fingerprint) ->
    {ok, Trust} = weida:trust_new(),
    ok = weida:trust_add_pin(Trust, Fingerprint),
    Trust.

%% A trust anchored at the certificate Der.
anchored(Der) ->
    {ok, Trust} = weida:trust_new(),
    ok = weida:trust_add_anchor(Trust, {cacerts, [Der]}),
    Trust.

%% A requester on Runtime that dials under Trust, anonymously.
requester(Runtime, Trust) ->
    {ok, Tls} = weida:client_tls_new(Trust),
    {ok, Requester} = weida:runtime_requester(Runtime, Tls),
    Requester.

%% The same presenting Identity.
requester(Runtime, Trust, Identity) ->
    {ok, Tls} = weida:client_tls_new(Trust),
    ok = weida:client_tls_set_identity(Tls, Identity),
    {ok, Requester} = weida:runtime_requester(Runtime, Tls),
    Requester.

close(#{runtime := Runtime} = P) ->
    [weida:drop(maps:get(K, P)) || K <- [requester, replier, binding, listener]],
    ok = weida:runtime_shutdown(Runtime, infinity).
