"""Options: honoured, or refused where they are written.

The rule is 0013 §4.4 item 4 — nothing is silently ignored — and the claim
these tests make is that the refusal happens at *configuration time*, which
is the only moment it is useful. A depth past the ceiling, a `max_ttl` of
zero, a subscription on a socket type that has none: each fails at the line
that writes it, not at the first send.
"""

import asyncio

import pytest

import waits
import weida_nng


def run(coroutine):
    return waits.run(coroutine)


def test_the_option_table_answers_for_every_name_it_carries():
    assert len(weida_nng.OPTIONS) >= 50
    for row in weida_nng.OPTIONS:
        assert row.name.startswith("NNG_OPT_")
        assert row.note, f"{row.name} has no note"
        assert row.scope in {"socket", "dialer", "listener", "pipe", "transport"}
        if row.honoured:
            assert row.refusal is None
        else:
            assert row.refusal, f"{row.name} is refused without a reason"


def test_a_name_nng_does_not_have_is_not_in_the_table():
    assert weida_nng.option("NNG_OPT_RECVMAXSZ").honoured
    assert weida_nng.option("NNG_OPT_INVENTED") is None


def test_a_refused_option_says_why():
    zerotier = weida_nng.option("NNG_OPT_ZT_HOME")
    assert not zerotier.honoured
    assert "experimental" in zerotier.refusal

    websocket = weida_nng.option("NNG_OPT_WS_PROTOCOL")
    assert not websocket.honoured
    assert "transport" in websocket.refusal


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_ttl": 0},
        {"max_ttl": 256},
        {"max_pipes": 0},
        {"send_depth": weida_nng.MAX_QUEUE_DEPTH + 1},
        {"recv_depth": weida_nng.MAX_QUEUE_DEPTH + 1},
        {"handshake_timeout": 0.0},
        {"max_addresses": 0},
        {"max_pipes_per_peer": 0},
        {"max_tls_handshakes": 0},
        {"message_timeout": 0.0},
        {"idle_timeout": 0.0},
    ],
)
def test_an_impossible_configuration_is_refused_at_configuration_time(kwargs):
    with pytest.raises(weida_nng.EINVAL) as refused:
        weida_nng.SocketOptions(**kwargs)
    assert refused.value.errno == "EINVAL"


def test_a_negative_duration_is_not_a_duration():
    with pytest.raises(weida_nng.EINVAL):
        weida_nng.SocketOptions(recv_timeout=-1.0)


def test_the_connection_bounds_default_to_the_librarys_own_and_are_settable():
    defaults = weida_nng.SocketOptions()
    assert defaults.max_pipes_per_peer == 256
    assert defaults.message_timeout is None and defaults.idle_timeout is None

    tuned = weida_nng.SocketOptions(
        max_pipes_per_peer=8,
        max_tls_handshakes=4,
        message_timeout=2.5,
        idle_timeout=30.0,
    )
    assert tuned.max_pipes_per_peer == 8
    assert tuned.message_timeout == 2.5
    assert tuned.idle_timeout == 30.0


def test_a_buffer_a_protocol_does_not_have_is_refused_when_the_socket_opens():
    """REQ holds one transaction per context, so there is nothing to queue."""

    async def refused():
        ctx = weida_nng.Context()
        options = weida_nng.SocketOptions(send_depth=4)
        with pytest.raises(weida_nng.ENOTSUP) as error:
            weida_nng.ReqSocket(ctx, options)
        assert "NNG_OPT_SENDBUF" in error.value.cause

        # The same options are fine on a socket type that has the buffer.
        weida_nng.PushSocket(ctx, options)
        await ctx.shutdown()

    run(refused())


def test_a_subscription_on_a_socket_without_subscriptions_is_refused_by_name():
    async def refused():
        ctx = weida_nng.Context()
        options = weida_nng.SocketOptions(subscribe=[b"weather."])
        with pytest.raises(weida_nng.ENOTSUP) as error:
            weida_nng.PushSocket(ctx, options)
        assert "NNG_OPT_SUB_SUBSCRIBE" in error.value.cause

        weida_nng.SubSocket(ctx, options)
        await ctx.shutdown()

    run(refused())


def test_an_unknown_transport_is_refused_with_what_this_library_speaks():
    async def refused():
        ctx = weida_nng.Context()
        socket = weida_nng.PullSocket(ctx)
        with pytest.raises(weida_nng.ENOTSUP) as error:
            await socket.listen("ws://127.0.0.1:0")
        assert "WebSocket" in error.value.cause or "websocket" in error.value.cause
        await ctx.shutdown()

    run(refused())


def test_a_tls_endpoint_is_refused_because_this_binding_configures_none():
    """The library has the transport; this surface has no certificates yet,
    and an endpoint that cannot be configured is refused rather than
    dialled."""

    async def refused():
        ctx = weida_nng.Context()
        socket = weida_nng.ReqSocket(ctx)
        with pytest.raises(weida_nng.EINVAL) as error:
            await socket.dial("tls+tcp://127.0.0.1:9")
        assert "tls" in error.value.cause.lower()
        await ctx.shutdown()

    run(refused())


def test_a_url_past_nng_maxaddrlen_is_refused_before_it_is_parsed():
    async def refused():
        ctx = weida_nng.Context()
        socket = weida_nng.PullSocket(ctx)
        long_name = "inproc://" + "n" * weida_nng.NNG_MAXADDRLEN
        with pytest.raises(weida_nng.EADDRINVAL) as error:
            await socket.listen(long_name)
        assert str(weida_nng.NNG_MAXADDRLEN) in error.value.cause
        await ctx.shutdown()

    run(refused())


def test_the_defaults_are_the_librarys_own_numbers():
    defaults = weida_nng.SocketOptions()
    assert defaults.recv_max_size == weida_nng.DEFAULT_RECV_MAX_SIZE
    assert defaults.max_pipes == weida_nng.DEFAULT_MAX_PIPES
    assert defaults.survey_time == weida_nng.DEFAULT_SURVEY_TIME
    assert defaults.resend_time == weida_nng.DEFAULT_RESEND_TIME
    assert defaults.sub_prefer_new is True
    assert defaults.send_timeout is None, "NNG's default is to wait forever"
    assert weida_nng.NNG_MAX_TTL == 15 and weida_nng.SPEC_MAX_TTL == 255


def test_recvmaxsz_is_settable_per_socket_and_bounds_a_peers_declaration():
    async def bounded():
        ctx = weida_nng.Context()
        options = weida_nng.SocketOptions(recv_max_size=64, recv_timeout=1.0)
        assert options.recv_max_size == 64
        puller = weida_nng.PullSocket(ctx, options)
        url = await puller.listen("tcp://127.0.0.1:0")

        pusher = weida_nng.PushSocket(ctx, weida_nng.SocketOptions())
        await pusher.dial(url)
        await pusher.send(b"x" * 128)

        # The oversize message is refused from its declared length and the
        # connection ends; nothing arrives.
        with pytest.raises(weida_nng.ETIMEDOUT):
            await puller.recv()

        await ctx.shutdown()

    run(bounded())


@pytest.mark.parametrize("seconds", [float("nan"), float("inf"), -1.0, 1e30])
def test_a_timeout_no_duration_can_hold_is_einval_not_a_panic(seconds):
    with pytest.raises(weida_nng.EINVAL):
        weida_nng.SocketOptions(recv_timeout=seconds)


def test_a_negative_close_budget_is_einval_not_a_panic():
    with pytest.raises(weida_nng.EINVAL):
        weida_nng.Context(close_budget=-1)
