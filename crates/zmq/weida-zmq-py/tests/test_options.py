"""The option table and the error vocabulary, from Python.

The table is `weida_zmq.OPTIONS`, all 98 rows of the library's own, and
the point of the tests below is that **no row is a silence**: an option this
library implements can be set, and one it does not raises where it is set, with
the reason the Rust table names.

The first test walks every row, so a row that the binding cannot reach at all
fails here rather than in a user's port of a C program.
"""

import asyncio

import pytest

import weida_zmq

DEADLINE = 10.0

# One value per honoured socket row, in the type that row takes. The walk below
# sets every one of them; a row missing from this table fails the test, which is
# what keeps the binding's `set` in step with the library's.
HONOURED_SOCKET_VALUES = {
    "ZMQ_SNDHWM": 500,
    "ZMQ_RCVHWM": 500,
    "ZMQ_MAXMSGSIZE": 4096,
    "ZMQ_SNDTIMEO": 0.25,
    "ZMQ_RCVTIMEO": 0.25,
    "ZMQ_RECONNECT_IVL": 0.1,
    "ZMQ_RECONNECT_IVL_MAX": 1.0,
    "ZMQ_HANDSHAKE_IVL": 5.0,
    "ZMQ_CONNECT_TIMEOUT": 2.0,
    "ZMQ_HEARTBEAT_IVL": 1.0,
    "ZMQ_HEARTBEAT_TIMEOUT": 3.0,
    "ZMQ_HEARTBEAT_TTL": 4.0,
    "ZMQ_IMMEDIATE": True,
    "ZMQ_BACKLOG": 42,
    "ZMQ_ROUTING_ID": b"worker-3",
    "ZMQ_IDENTITY": b"worker-3",
    "ZMQ_ROUTER_MANDATORY": True,
    "ZMQ_ROUTER_HANDOVER": True,
    "ZMQ_PROBE_ROUTER": True,
    "ZMQ_REQ_CORRELATE": True,
    "ZMQ_REQ_RELAXED": False,
    "ZMQ_SUBSCRIBE": b"topic",
    "ZMQ_UNSUBSCRIBE": b"topic",
    "ZMQ_XPUB_VERBOSE": True,
    "ZMQ_XPUB_VERBOSER": True,
    "ZMQ_XPUB_MANUAL": True,
    "ZMQ_XPUB_WELCOME_MSG": b"hello",
    "ZMQ_PLAIN_SERVER": True,
    "ZMQ_PLAIN_USERNAME": "admin",
    "ZMQ_PLAIN_PASSWORD": "secret",
    "ZMQ_CURVE_SERVER": True,
    "ZMQ_CURVE_PUBLICKEY": b"0" * 32,
    "ZMQ_CURVE_SECRETKEY": b"1" * 32,
    "ZMQ_CURVE_SERVERKEY": b"2" * 32,
    "ZMQ_ZAP_DOMAIN": "global",
    "ZMQ_ZAP_ENFORCE_DOMAIN": True,
}

HONOURED_CONTEXT_VALUES = {
    "ZMQ_MAX_SOCKETS": 64,
    "ZMQ_LINGER": 0.5,
}

FIVE_REASONS = {
    "no transport",
    "draft only",
    "deprecated in favour of ZAP",
    "replaced by a weida-runtime construct",
    "absent",
}


def run(coroutine):
    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


def test_the_whole_table_is_reachable():
    assert len(weida_zmq.OPTIONS) == 98
    for row in weida_zmq.OPTIONS:
        assert weida_zmq.option(row.name).name == row.name
        assert row.scope in {"socket", "context"}
        if row.honoured:
            assert row.binding, row.name
            assert row.reason is None and row.refusal is None
        else:
            assert row.reason in FIVE_REASONS, (row.name, row.reason)
            assert row.refusal, row.name
    assert weida_zmq.option("ZMQ_NOT_AN_OPTION") is None
    # A read-only libzmq option is not in the table, because it is not set.
    assert weida_zmq.option("ZMQ_LAST_ENDPOINT") is None


def test_every_row_is_honoured_or_refused_and_never_ignored():
    """The walk of every one of the 98 rows, set."""
    seen_reasons = set()
    for row in weida_zmq.OPTIONS:
        # ZMQ_LINGER's home here is the context, whatever libzmq's scope for
        # it is; the binding says so and this asserts it.
        context_scope = row.scope == "context" or row.name == "ZMQ_LINGER"
        options = (
            weida_zmq.ContextOptions() if context_scope else weida_zmq.SocketOptions()
        )
        if row.honoured:
            values = HONOURED_CONTEXT_VALUES if context_scope else HONOURED_SOCKET_VALUES
            assert row.name in values, f"{row.name} is honoured but this test has no value"
            options.set(row.name, values[row.name])
            continue

        with pytest.raises(weida_zmq.EINVAL) as refused:
            options.set(row.name, 1)
        message = str(refused.value)
        assert row.name in message, message
        assert row.reason.split(":")[0] in message or row.reason in message, message
        seen_reasons.add(row.reason)

    # One row of each refusal reason, which the walk has now covered.
    assert seen_reasons == FIVE_REASONS


@pytest.mark.parametrize(
    "name,reason",
    [
        ("ZMQ_TCP_KEEPALIVE", "absent"),
        ("ZMQ_WSS_TRUST_SYSTEM", "no transport"),
        ("ZMQ_ROUTER_NOTIFY", "draft only"),
        ("ZMQ_IPC_FILTER_UID", "deprecated in favour of ZAP"),
        ("ZMQ_IO_THREADS", "replaced by a weida-runtime construct"),
    ],
)
def test_one_row_of_each_refusal_reason_names_its_reason(name, reason):
    row = weida_zmq.option(name)
    assert row is not None, name
    assert row.reason == reason, (name, row.reason)
    options = (
        weida_zmq.SocketOptions() if row.scope == "socket" else weida_zmq.ContextOptions()
    )
    with pytest.raises(weida_zmq.EINVAL) as refused:
        options.set(name, 1)
    assert refused.value.errno == "EINVAL"
    assert name in refused.value.cause


def test_an_honoured_row_changes_what_the_socket_does():
    """`ZMQ_RCVTIMEO` set through the table, observed through a receive."""

    async def bounded_receive():
        options = weida_zmq.SocketOptions()
        options.set("ZMQ_RCVTIMEO", 0.05)
        context = weida_zmq.Context()
        sink = weida_zmq.PullSocket(context, options)
        await sink.bind("inproc://rcvtimeo")
        with pytest.raises(weida_zmq.EAGAIN):
            await sink.recv()

        # And without it, the same receive waits — long enough to be cancelled
        # rather than expiring on its own.
        plain = weida_zmq.PullSocket(context)
        await plain.bind("inproc://no-rcvtimeo")
        waiting = asyncio.create_task(plain.recv())
        await asyncio.sleep(0.1)
        assert not waiting.done()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

    run(bounded_receive())


def test_a_subscription_set_as_an_option_is_applied_or_refused_at_construction():
    async def subscribe_through_the_table():
        options = weida_zmq.SocketOptions()
        options.set("ZMQ_SUBSCRIBE", b"weather.")
        context = weida_zmq.Context()
        publisher = weida_zmq.PubSocket(context)
        subscriber = weida_zmq.SubSocket(context, options)
        bound = await publisher.bind("inproc://option-subscribe")
        await subscriber.connect(bound)

        for _ in range(200):
            published = await publisher.send([b"weather.eu", b"rain"])
            if published.delivered == 1:
                break
            await asyncio.sleep(0.01)
        assert published.delivered == 1, "the option's subscription was not applied"
        assert await subscriber.recv() == [b"weather.eu", b"rain"]

        # A socket type with no subscriptions refuses the same options where
        # they are used, which is what libzmq does too.
        with pytest.raises(weida_zmq.EINVAL) as refused:
            weida_zmq.PushSocket(context, options)
        assert "ZMQ_SUBSCRIBE" in refused.value.cause

    run(subscribe_through_the_table())


def test_the_two_deliberate_default_changes_are_readable_as_such():
    # 1. ZMQ_LINGER: libzmq's default is -1, infinite. Here it is finite, and
    # the row says so in the binding it names.
    linger = weida_zmq.option("ZMQ_LINGER")
    assert linger.honoured
    assert "finite" in linger.binding
    assert "close_budget" in linger.binding
    assert weida_zmq.DEFAULT_CLOSE_BUDGET == 1.0
    assert weida_zmq.Context().close_budget == weida_zmq.DEFAULT_CLOSE_BUDGET

    # 2. ZMQ_MAXMSGSIZE: libzmq's default is -1, no limit. Here it is a number.
    maxmsgsize = weida_zmq.option("ZMQ_MAXMSGSIZE")
    assert maxmsgsize.honoured
    assert "bounded by default" in maxmsgsize.binding
    assert weida_zmq.DEFAULT_MAX_MESSAGE_SIZE == 1024 * 1024

    # And neither is a silence: both are settable back through the table.
    options = weida_zmq.SocketOptions()
    options.set("ZMQ_MAXMSGSIZE", 8 * 1024 * 1024)
    context_options = weida_zmq.ContextOptions()
    context_options.set("ZMQ_LINGER", 30.0)
    assert weida_zmq.Context(options=context_options).close_budget == 30.0

    # libzmq's "wait forever" is the value this library refuses to have.
    with pytest.raises(weida_zmq.EINVAL) as forever:
        context_options.set("ZMQ_LINGER", None)
    assert "finite" in forever.value.cause


def test_a_context_option_on_a_socket_says_where_it_lives():
    with pytest.raises(weida_zmq.EINVAL) as wrong_scope:
        weida_zmq.SocketOptions().set("ZMQ_MAX_SOCKETS", 10)
    assert "ContextOptions" in wrong_scope.value.cause

    with pytest.raises(weida_zmq.EINVAL) as also_wrong:
        weida_zmq.ContextOptions().set("ZMQ_SNDHWM", 10)
    assert "SocketOptions" in also_wrong.value.cause


def test_a_value_the_option_cannot_take_is_refused_where_it_is_given():
    options = weida_zmq.SocketOptions()
    with pytest.raises(weida_zmq.EINVAL):
        options.set("ZMQ_SNDHWM", "many")
    with pytest.raises(weida_zmq.EINVAL):
        options.set("ZMQ_IMMEDIATE", "yes")
    with pytest.raises(weida_zmq.EINVAL):
        options.set("ZMQ_RCVTIMEO", -1.0)
    with pytest.raises(weida_zmq.EINVAL):
        options.set("ZMQ_ROUTING_ID", b"")
    with pytest.raises(weida_zmq.EINVAL):
        options.set("ZMQ_CURVE_PUBLICKEY", b"too short")
    with pytest.raises(weida_zmq.EINVAL):
        options.set("ZMQ_NOT_AN_OPTION", 1)


def test_the_context_options_and_the_keyword_arguments_agree():
    context_options = weida_zmq.ContextOptions()
    context_options.set("ZMQ_MAX_SOCKETS", 7)
    assert weida_zmq.Context(options=context_options).max_sockets == 7
    # The keyword argument is written at the call site, so it wins.
    assert (
        weida_zmq.Context(options=context_options, max_sockets=9).max_sockets == 9
    )
