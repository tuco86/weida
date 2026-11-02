%% The unit tier: status codes, the failures that need no peer, and Req/Rep
%% over the in-process transport, each test on a bus of its own. Every
%% wait goes through a readiness descriptor and a `receive`.
-module(weida_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

%% A hang guard only, in seconds: a wait that should succeed passes
%% `infinity`, and the CI job's budget bounds the run.
-define(HANG, 600).

weida_test_() ->
    [
        {Title, {timeout, ?HANG, Test}}
     || {Title, Test} <- [
            {"every code atom round-trips through strerror", fun codes/0},
            {"a runtime with no worker threads is a runtime failure", fun zero_workers/0},
            {"a malformed pin is an invalid fingerprint", fun malformed_pin/0},
            {"a streamed round trip carries the body and the content type", fun round_trip/0},
            {"reads of 3 bytes reassemble the body", fun small_reads/0},
            {"a collect over its ceiling is limit_exceeded", fun collect_ceiling/0},
            {"a reply dropped unfinished reads as canceled", fun reply_dropped/0},
            {"a second reply is closed", fun second_reply/0},
            {"two processes accepting on one replier both get a request", fun two_accepts/0},
            {"a request that times out leaves the runtime able to shut down", fun request_timeout/0},
            {"dropping a replier ends another process's accept with closed", fun drop_ends_accept/0},
            {"a replier its process left without drop is released", fun garbage_collected/0},
            {"to_pem reads back with the same fingerprint", fun to_pem_reads_back/0},
            {"the certificates are the identity's key", fun certificates_are_the_key/0},
            {"generate_for names the certificate", fun generate_for_names/0},
            {"identity_from takes ssl's in-memory options", fun identity_from_memory/0},
            {"identity_from refuses what it cannot honour", fun identity_from_refuses/0},
            {"anchors take PEM and DER", fun anchors_and_terms/0}
        ]
    ].

%% docs/c/README.md R9: the 24 variants and the ABI's own five codes.
r9() ->
    [
        {1, runtime},
        {2, invalid_address},
        {3, invalid_endpoint_path},
        {4, invalid_fingerprint},
        {5, already_registered},
        {6, not_connected},
        {7, connection_lost},
        {8, negotiation},
        {9, protocol},
        {10, rejected},
        {11, unknown_endpoint},
        {12, unsupported},
        {13, no_parked_connection},
        {14, no_reply},
        {15, canceled},
        {16, expired},
        {17, indeterminate},
        {18, limit_exceeded},
        {19, datagrams_unavailable},
        {20, too_large},
        {21, tls},
        {22, untrusted},
        {23, io},
        {24, transport},
        {100, again},
        {101, intr},
        {102, inval},
        {103, closed},
        {104, panic}
    ].

codes() ->
    lists:foreach(
        fun({Code, Atom}) ->
            Message = weida:strerror(Code),
            ?assertNotEqual(<<"unknown error">>, Message),
            ?assertEqual(Message, weida:strerror(-Code)),
            ?assertEqual({Atom, Message}, {Atom, weida:strerror(Atom)})
        end,
        r9()
    ),
    ?assertEqual(<<"unknown error">>, weida:strerror(9999)),
    ?assertError(badarg, weida:strerror(no_such_code)).

zero_workers() ->
    ?assertMatch(
        {error, #{code := runtime, message := <<_/binary>>}},
        weida:runtime_owned(#{worker_threads => 0})
    ).

malformed_pin() ->
    {ok, Trust} = weida:trust_new(),
    ?assertMatch({error, #{code := invalid_fingerprint}}, weida:trust_add_pin(Trust, <<"nope">>)).

to_pem_reads_back() ->
    {ok, Id} = weida:identity_generate(),
    {ok, Fingerprint} = weida:identity_fingerprint(Id),
    {ok, Pem} = weida:identity_to_pem(Id),
    ?assertNotEqual(nomatch, binary:match(Pem, <<"PRIVATE KEY">>)),
    {ok, Back} = weida:identity_from_pem(Pem, Pem),
    ?assertEqual({ok, Fingerprint}, weida:identity_fingerprint(Back)),
    {ok, CertPem} = weida:identity_certificate_pem(Id),
    ?assertNotEqual(nomatch, binary:match(CertPem, <<"-----BEGIN CERTIFICATE-----">>)),
    ?assertEqual(nomatch, binary:match(CertPem, <<"PRIVATE KEY">>)).

certificates_are_the_key() ->
    {ok, Id} = weida:identity_generate(),
    {ok, Fingerprint} = weida:identity_fingerprint(Id),
    {ok, [Der]} = weida:identity_certificates(Id),
    #'Certificate'{tbsCertificate = #'TBSCertificate'{subjectPublicKeyInfo = Spki}} =
        public_key:pkix_decode_cert(Der, plain),
    Hash = crypto:hash(sha256, public_key:der_encode('SubjectPublicKeyInfo', Spki)),
    ?assertEqual(
        <<"sha256:", (binary:encode_hex(Hash, lowercase))/binary>>, Fingerprint
    ).

generate_for_names() ->
    {ok, Id} = weida:identity_generate_for([<<"localhost">>, <<"127.0.0.1">>]),
    {ok, [Der]} = weida:identity_certificates(Id),
    #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = Extensions}} =
        public_key:pkix_decode_cert(Der, otp),
    [Names] = [
        Value
     || #'Extension'{extnID = ?'id-ce-subjectAltName', extnValue = Value} <- Extensions
    ],
    ?assert(lists:member({dNSName, "localhost"}, Names)),
    ?assert(lists:member({iPAddress, <<127, 0, 0, 1>>}, Names)),
    ?assertMatch({ok, _}, weida:identity_generate_for([])).

identity_from_memory() ->
    {ok, Id} = weida:identity_generate(),
    {ok, Fingerprint} = weida:identity_fingerprint(Id),
    {ok, ToPem} = weida:identity_to_pem(Id),
    [{'Certificate', CertDer, _}, {KeyType, KeyDer, _}] = public_key:pem_decode(ToPem),
    {ok, FromMap} = weida:identity_from(#{cert => CertDer, key => {KeyType, KeyDer}}),
    ?assertEqual({ok, Fingerprint}, weida:identity_fingerprint(FromMap)),
    {ok, FromList} = weida:identity_from([{cert, [CertDer]}, {key, {KeyType, KeyDer}}]),
    ?assertEqual({ok, Fingerprint}, weida:identity_fingerprint(FromList)).

identity_from_refuses() ->
    {ok, Id} = weida:identity_generate(),
    {ok, ToPem} = weida:identity_to_pem(Id),
    [{'Certificate', CertDer, _}, {KeyType, KeyDer, _}] = public_key:pem_decode(ToPem),
    ?assertError(badarg, weida:identity_from(#{cert => CertDer})),
    ?assertError(badarg, weida:identity_from(#{certfile => "a", key => {KeyType, KeyDer}})),
    ?assertError(badarg, weida:identity_from(#{cert => CertDer, key => {'DSAPrivateKey', <<>>}})),
    ?assertError(badarg, weida:identity_from([{certfile, "a"}, {password, "x"}])),
    ?assertError(
        {unsupported_key, _},
        weida:identity_from(#{
            cert => CertDer, key => #{algorithm => ecdsa, sign_fun => fun(_, _, _) -> <<>> end}
        })
    ).

anchors_and_terms() ->
    {ok, Id} = weida:identity_generate(),
    {ok, CertPem} = weida:identity_certificate_pem(Id),
    {ok, [CertDer]} = weida:identity_certificates(Id),
    {ok, T} = weida:trust_new(),
    ?assertEqual(ok, weida:trust_add_anchor(T, CertPem)),
    ?assertEqual(ok, weida:trust_add_anchor(T, {cacerts, [CertDer]})),
    ?assertError(badarg, weida:trust_add_anchor(T, {cacerts, []})),
    {ok, Tls} = weida:server_tls_new(Id),
    {ok, EmptyTrust} = weida:trust_new(),
    ?assertEqual(ok, weida:server_tls_require_client(Tls, EmptyTrust)),
    ?assertEqual(ok, weida:server_tls_require_any_key(Tls)),
    ?assertError(badarg, weida:identity_generate_for([<<"a", 0, "b">>])).

round_trip() ->
    #{replier := Replier, requester := Requester} = P = pair(),
    Served = async(fun() ->
        serve(Replier, fun string:uppercase/1, #{content_type => <<"text/plain">>})
    end),
    {ok, Reply} = weida:requester_request(Requester, <<"ping">>, infinity),
    ?assertMatch({ok, #{content_type := <<"text/plain">>}}, weida:incoming_transfer_meta(Reply)),
    {ok, #{payload := Payload, meta := Meta}} =
        weida:incoming_transfer_collect(Reply, 1024, infinity),
    ?assertEqual(<<"PING">>, Payload),
    ?assertMatch(#{content_type := <<"text/plain">>, endpoint := undefined}, Meta),
    ?assertEqual(<<"ping">>, await(Served)),
    close(P).

small_reads() ->
    #{replier := Replier, requester := Requester} = P = pair(),
    Client = request_quietly(Requester, <<"0123456789">>),
    {ok, Request} = weida:replier_accept(Replier, infinity),
    ?assertMatch({ok, #{endpoint := <<"/echo">>}}, weida:incoming_request_meta(Request)),
    {ok, Body} = weida:incoming_request_take_body(Request),
    ?assertEqual(<<"0123456789">>, read_all(Body, 3)),
    ?assertEqual(eof, weida:incoming_transfer_read(Body, 3, infinity)),
    weida:drop(Request),
    await(Client),
    close(P).

collect_ceiling() ->
    #{replier := Replier, requester := Requester} = P = pair(),
    Client = request_quietly(Requester, <<"four">>),
    {ok, Request} = weida:replier_accept(Replier, infinity),
    {ok, Body} = weida:incoming_request_take_body(Request),
    ?assertMatch(
        {error, #{code := limit_exceeded}}, weida:incoming_transfer_collect(Body, 2, infinity)
    ),
    weida:drop(Request),
    await(Client),
    close(P).

reply_dropped() ->
    #{replier := Replier, requester := Requester} = P = pair(),
    Server = async(fun() ->
        {ok, Request} = weida:replier_accept(Replier, infinity),
        {ok, Reply} = weida:incoming_request_reply(Request, #{}, infinity),
        ok = weida:outgoing_transfer_write_all(Reply, <<"ha">>, infinity),
        weida:drop(Reply)
    end),
    {ok, Reply} = weida:requester_request(Requester, <<"go">>, infinity),
    ?assertMatch({error, #{code := canceled}}, drain(Reply)),
    await(Server),
    close(P).

second_reply() ->
    #{replier := Replier, requester := Requester} = P = pair(),
    Client = request_quietly(Requester, <<"once">>),
    {ok, Request} = weida:replier_accept(Replier, infinity),
    {ok, Reply} = weida:incoming_request_reply(Request, #{}, infinity),
    weida:drop(Reply),
    ?assertMatch({error, #{code := closed}}, weida:incoming_request_reply(Request, #{}, infinity)),
    await(Client),
    close(P).

two_accepts() ->
    #{replier := Replier, requester := Requester} = P = pair(),
    Reverse = fun(Body) -> list_to_binary(lists:reverse(binary_to_list(Body))) end,
    Accepts = [spawn_async(fun() -> serve(Replier, Reverse, #{}) end) || _ <- [1, 2]],
    [until_waiting(Pid) || {Pid, _} <- Accepts],
    Replies = [
        async(fun() ->
            {ok, Reply} = weida:requester_request(Requester, Body, infinity),
            {ok, #{payload := Payload}} = weida:incoming_transfer_collect(Reply, 1024, infinity),
            Payload
        end)
     || Body <- [<<"one">>, <<"two">>]
    ],
    ?assertEqual([<<"eno">>, <<"owt">>], lists:sort([await(R) || R <- Replies])),
    ?assertEqual([<<"one">>, <<"two">>], lists:sort([await(R) || {_, R} <- Accepts])),
    close(P).

request_timeout() ->
    #{requester := Requester} = P = pair(),
    ?assertEqual({error, timeout}, weida:requester_request(Requester, <<"unanswered">>, 50)),
    close(P).

drop_ends_accept() ->
    #{replier := Replier} = P = pair(),
    {Pid, Accept} = spawn_async(fun() -> weida:replier_accept(Replier, infinity) end),
    until_waiting(Pid),
    weida:drop(Replier),
    ?assertMatch({error, #{code := closed}}, await(Accept)),
    close(P).

%% The resource of a replier whose process exited is released by the
%% garbage collector, also when its descriptor was selected: once after a
%% wait that timed out, once with the process killed in the middle of one.
%% A released replier answers a request to its path with `no_reply`; a
%% live one nobody accepts on would hold it forever. (The path itself stays
%% registered on the listener: libweida's replier drop does not unregister
%% it, released or not.)
garbage_collected() ->
    #{runtime := Runtime, listener := Listener, bus := Bus} = P = pair(),
    {Waited, Result} = spawn_async(fun() ->
        {ok, Replier} = weida:listener_replier(Listener, <<"/timed-out">>),
        weida:replier_accept(Replier, 10)
    end),
    ?assertEqual({error, timeout}, await(Result)),
    until_dead(Waited),
    erlang:garbage_collect(),
    ?assertMatch({error, #{code := no_reply}}, request_to(Runtime, Bus, <<"/timed-out">>)),

    Parent = self(),
    Killed = spawn(fun() ->
        {ok, Replier} = weida:listener_replier(Listener, <<"/killed">>),
        Parent ! {self(), registered},
        weida:replier_accept(Replier, infinity)
    end),
    receive
        {Killed, registered} -> ok
    end,
    until_waiting(Killed),
    exit(Killed, kill),
    until_dead(Killed),
    erlang:garbage_collect(),
    ?assertMatch({error, #{code := no_reply}}, request_to(Runtime, Bus, <<"/killed">>)),
    close(P).

%% ---- helpers ----

%% A runtime with a replier at /echo and a requester connected to it over
%% a bus of its own.
pair() ->
    Bus = <<"test-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, Runtime} = weida:runtime_owned(#{}),
    {ok, Listener} = weida:runtime_listener(Runtime),
    {ok, Binding} = weida:listener_bind_inproc(Listener, Bus),
    ?assertEqual(Bus, weida:local_binding_bus(Binding)),
    {ok, Replier} = weida:listener_replier(Listener, <<"/echo">>),
    ?assertEqual(<<"/echo">>, weida:replier_path(Replier)),
    {ok, Trust} = weida:trust_new(),
    {ok, Tls} = weida:client_tls_new(Trust),
    {ok, Requester} = weida:runtime_requester(Runtime, Tls),
    ok = weida:requester_connect(Requester, <<"weida+inproc://", Bus/binary, "/echo">>, infinity),
    #{
        bus => Bus,
        runtime => Runtime,
        listener => Listener,
        binding => Binding,
        replier => Replier,
        requester => Requester
    }.

%% One request from a requester of its own to Path on Bus.
request_to(Runtime, Bus, Path) ->
    {ok, Trust} = weida:trust_new(),
    {ok, Tls} = weida:client_tls_new(Trust),
    {ok, Requester} = weida:runtime_requester(Runtime, Tls),
    ok = weida:requester_connect(Requester, <<"weida+inproc://", Bus/binary, Path/binary>>, infinity),
    Result = weida:requester_request(Requester, <<"anyone?">>, infinity),
    weida:drop(Requester),
    Result.

close(#{runtime := Runtime} = P) ->
    [weida:drop(maps:get(K, P)) || K <- [requester, replier, binding, listener]],
    ok = weida:runtime_shutdown(Runtime, infinity).

%% Answers one request with Answer(Body), reading the body 3 bytes at a
%% time; returns the body.
serve(Replier, Answer, Meta) ->
    {ok, Request} = weida:replier_accept(Replier, infinity),
    {ok, Body} = weida:incoming_request_take_body(Request),
    Received = read_all(Body, 3),
    {ok, Reply} = weida:incoming_request_reply(Request, Meta, infinity),
    ok = weida:outgoing_transfer_write_all(Reply, Answer(Received), infinity),
    {ok, Delivery} = weida:outgoing_transfer_finish(Reply),
    ok = weida:delivery_delivered(Delivery, infinity),
    Received.

read_all(Transfer, Max) ->
    read_all(Transfer, Max, []).

read_all(Transfer, Max, Acc) ->
    case weida:incoming_transfer_read(Transfer, Max, infinity) of
        {ok, Piece} ->
            ?assert(byte_size(Piece) =< Max),
            read_all(Transfer, Max, [Piece | Acc]);
        eof ->
            iolist_to_binary(lists:reverse(Acc))
    end.

drain(Transfer) ->
    case weida:incoming_transfer_read(Transfer, 64, infinity) of
        {ok, _} -> drain(Transfer);
        Other -> Other
    end.

%% A request whose answer the test does not look at.
request_quietly(Requester, Body) ->
    async(fun() ->
        case weida:requester_request(Requester, Body, infinity) of
            {ok, Reply} -> weida:incoming_transfer_collect(Reply, 1024, infinity);
            Failure -> Failure
        end
    end).

async(Fun) ->
    {_, Ref} = spawn_async(Fun),
    Ref.

spawn_async(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn_link(fun() -> Parent ! {Ref, Fun()} end),
    {Pid, Ref}.

await(Ref) ->
    receive
        {Ref, Value} -> Value
    end.

%% Until Pid waits in weida's `receive` for a descriptor.
until_waiting(Pid) ->
    case erlang:process_info(Pid, [current_function, status]) of
        [{current_function, {weida, wait_ready, 3}}, {status, waiting}] ->
            ok;
        _ ->
            erlang:yield(),
            until_waiting(Pid)
    end.

until_dead(Pid) ->
    Monitor = erlang:monitor(process, Pid),
    receive
        {'DOWN', Monitor, process, Pid, _} -> ok
    end.
