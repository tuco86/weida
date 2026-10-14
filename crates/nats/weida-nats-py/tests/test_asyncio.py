"""The asyncio surface, against the scripted server of `scripted.py`.

What is asserted here beyond "a message crossed":

* the two wildcards reach the subscription that asked for them, delivered by
  a server that routes rather than by a hand-written `MSG` with a chosen sid;
* a queue group's name is **on the wire**, in the `SUB` line, where the
  protocol puts it;
* `request` has three outcomes and they are three classes — a reply,
  `NoResponders` for the `NATS/1.0 503` that says nobody was listening *now*,
  and `RequestTimeout` for a window that closed on a subject somebody was
  listening to;
* `unsubscribe_after(1)` ends the subscription, which is the count honoured
  by the client and not only by the server;
* headers cross in both directions, names and order kept;
* a cancelled `asyncio.Task` leaves the subscription usable, which is the
  lease giving it back rather than the absence of an exception.

Nothing here needs a `nats-server`; `test_interop.py` is the one that does.
"""

import asyncio

import pytest

import scripted
import weida_nats

DEADLINE = 10.0


def run(coroutine):
    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


@pytest.fixture
def server():
    running = scripted.Server.start()
    try:
        yield running
    finally:
        running.stop()


async def connected(server, **options):
    return await weida_nats.connect("127.0.0.1", server.port, **options)


# --------------------------------------------------------------------------
# Publish and subscribe


def test_a_publish_reaches_a_subscription(server):
    async def exchange():
        nats = await connected(server)
        assert nats.state().name == "connected"
        assert nats.headers_supported()
        assert nats.max_payload() == 1048576
        assert nats.info().server_name == "scripted"

        orders = await nats.subscribe("orders.created")
        await nats.publish("orders.created", b"one")
        message = await orders.next()

        assert message.subject == b"orders.created"
        assert message.subject_str == "orders.created"
        assert message.payload == b"one"
        assert message.sid == orders.sid()
        assert message.reply_to is None
        assert message.headers is None
        assert message.status is None
        assert not message.is_no_responders
        await nats.close()
        return nats

    nats = run(exchange())
    assert nats.state().name == "closed"
    assert [op.render() for op in server.ops_until("PUB")] == [
        "CONNECT",
        "PING",
        "SUB orders.created - 1",
        "PUB orders.created - one",
    ]


def test_the_two_wildcards_reach_the_right_subscription(server):
    async def exchange():
        nats = await connected(server)
        one_token = await nats.subscribe("orders.*")
        the_rest = await nats.subscribe("orders.>")
        exact = await nats.subscribe("orders.created")

        await nats.publish("orders.created", b"shallow")
        await nats.publish("orders.eu.created", b"deep")

        # `*` is exactly one token, so only the shallow subject reaches it.
        assert (await one_token.next()).payload == b"shallow"
        assert one_token.try_next() is None
        # `>` is one or more, so both do, in the order they were published.
        assert (await the_rest.next()).payload == b"shallow"
        assert (await the_rest.next()).payload == b"deep"
        # A literal subject is not a pattern at all.
        assert (await exact.next()).payload == b"shallow"
        assert exact.try_next() is None
        await nats.close()

    run(exchange())
    lines = [op.render() for op in server.ops_until("SUB")]
    assert lines[-1] == "SUB orders.* - 1"


def test_a_queue_group_travels_in_the_sub_line(server):
    async def exchange():
        nats = await connected(server)
        workers = await nats.subscribe_with_queue_group("work.>", "builders")
        assert workers.queue_group() == b"builders"
        assert workers.subject() == b"work.>"
        # A second member of the same group: the server sends one copy
        # between them, which is what a queue group is.
        second = await nats.subscribe_with_queue_group("work.>", "builders")
        loner = await nats.subscribe("work.>")

        await nats.publish("work.item", b"job")
        assert (await workers.next()).payload == b"job"
        assert second.try_next() is None
        # An ordinary subscription beside a group still gets its own copy.
        assert (await loner.next()).payload == b"job"
        await nats.close()

    run(exchange())
    subs = [op.render() for op in server.ops_until("PUB") if op.verb == "SUB"]
    assert subs == [
        "SUB work.> builders 1",
        "SUB work.> builders 2",
        "SUB work.> - 3",
    ]


def test_a_subscription_is_an_async_iterator(server):
    async def exchange():
        nats = await connected(server)
        orders = await nats.subscribe("orders.>")
        await nats.publish("orders.a", b"1")
        await nats.publish("orders.b", b"2")
        await nats.flush()

        seen = []
        async for message in orders:
            seen.append(message.payload)
            if len(seen) == 2:
                await orders.unsubscribe()
        # The loop ends by itself once the subscription does: an ended
        # iteration is not a failure, and StopAsyncIteration is how Python
        # spells it.
        assert seen == [b"1", b"2"]
        await nats.close()

    run(exchange())


def test_unsubscribe_after_one_ends_the_subscription(server):
    async def exchange():
        nats = await connected(server)
        orders = await nats.subscribe("orders.>")
        await orders.unsubscribe_after(1)
        # The flush makes the UNSUB a fact on the server before anything is
        # published: PING/PONG is the protocol's only round trip.
        await nats.flush()

        await nats.publish("orders.a", b"1")
        assert (await orders.next()).payload == b"1"
        # The count is the client's too, so the subscription is over whether
        # or not the server sends more.
        assert await orders.next() is None
        with pytest.raises(StopAsyncIteration):
            await orders.__anext__()
        await nats.close()

    run(exchange())
    assert "UNSUB 1 1" in [op.render() for op in server.ops_until("PUB")]


# --------------------------------------------------------------------------
# Request and reply


def test_request_is_answered(server):
    async def exchange():
        nats = await connected(server)
        echo = await nats.subscribe("service.echo")

        async def responder():
            request = await echo.next()
            assert request.reply_to is not None
            await nats.publish(request.reply_to, b"pong:" + request.payload)

        answering = asyncio.create_task(responder())
        reply = await nats.request("service.echo", b"ping", 2.0)
        await answering

        assert reply.payload == b"pong:ping"
        assert reply.subject_str.startswith("_INBOX.")
        await nats.close()

    run(exchange())


def test_a_request_without_a_responder_times_out(server):
    async def exchange():
        nats = await connected(server)
        # Somebody *is* subscribed, so there is no 503 — the window simply
        # closes, which is the other fact entirely. The handle is the
        # subscription's lifetime, so it is kept: dropping it unsubscribes.
        listening = await nats.subscribe("service.slow")
        await nats.flush()
        with pytest.raises(weida_nats.RequestTimeout) as timeout:
            await nats.request("service.slow", b"ping", 0.2)
        assert timeout.value.errno == "RequestTimeout"
        assert not isinstance(timeout.value, weida_nats.NoResponders)
        assert listening.sid() == 1
        await nats.close()

    run(exchange())


def test_a_503_is_no_responders_and_not_the_timeout_class(server):
    async def exchange():
        nats = await connected(server)
        started = asyncio.get_running_loop().time()
        with pytest.raises(weida_nats.NoResponders) as refused:
            # A generous window: the point is that the answer does not wait
            # for it. 503 means "nobody was listening, now".
            await nats.request("nobody.listening", b"ping", 30.0)
        assert asyncio.get_running_loop().time() - started < 5.0
        assert refused.value.errno == "NoResponders"
        assert not isinstance(refused.value, weida_nats.RequestTimeout)
        assert isinstance(refused.value, weida_nats.NatsError)
        await nats.close()

    run(exchange())


def test_request_many_collects_what_arrived_in_the_window(server):
    async def exchange():
        nats = await connected(server)
        echo = await nats.subscribe("service.fan")

        async def responder():
            request = await echo.next()
            await nats.publish(request.reply_to, b"first")
            await nats.publish(request.reply_to, b"second")

        answering = asyncio.create_task(responder())
        replies = await nats.request_many("service.fan", b"ping", 2.0, 2)
        await answering

        assert [reply.payload for reply in replies] == [b"first", b"second"]
        await nats.close()

    run(exchange())


# --------------------------------------------------------------------------
# Headers


def test_headers_cross_in_both_directions(server):
    async def exchange():
        nats = await connected(server)
        traced = await nats.subscribe("orders.>")
        await nats.publish_with(
            "orders.created",
            headers=[("Trace-Id", "abc"), ("Link", "a"), ("Link", "b")],
            payload=b"body",
        )
        message = await traced.next()
        # Order and duplicates survive, because the block is a list of pairs
        # and not a mapping: names may repeat with different values.
        assert message.headers == [("Trace-Id", "abc"), ("Link", "a"), ("Link", "b")]
        assert message.payload == b"body"
        assert message.status is None

        # A mapping is the convenience spelling of the same thing.
        await nats.publish_with("orders.created", headers={"X": "1"}, payload=b"")
        assert (await traced.next()).headers == [("X", "1")]
        await nats.close()

    run(exchange())
    written = [op for op in server.ops_until("HPUB") if op.verb == "HPUB"]
    assert written[0].headers == [("Trace-Id", "abc"), ("Link", "a"), ("Link", "b")]
    assert written[0].payload == b"body"


# --------------------------------------------------------------------------
# The lease: one reader at a time, and cancellation gives it back


def test_a_cancelled_next_leaves_the_subscription_usable(server):
    async def exchange():
        nats = await connected(server)
        orders = await nats.subscribe("orders.>")

        waiting = asyncio.create_task(orders.next())
        await asyncio.sleep(0.1)
        # While one coroutine holds the subscription, a call that was told
        # not to wait says so rather than reporting "no message".
        with pytest.raises(BlockingIOError):
            orders.try_next()

        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        # The lease came back: the next reader gets the subscription, and
        # the message that arrives afterwards is delivered to it.
        await nats.publish("orders.a", b"after")
        assert (await orders.next()).payload == b"after"
        await nats.close()

    run(exchange())


# --------------------------------------------------------------------------
# Bounds and refusals, all of them the library's


def test_a_payload_above_max_payload_is_refused_before_the_wire():
    small = scripted.Server.start(info=dict(scripted.FULL_INFO, max_payload=16))

    async def exchange():
        nats = await connected(small)
        assert nats.max_payload() == 16
        with pytest.raises(weida_nats.PayloadTooLarge) as refused:
            await nats.publish("orders.created", b"x" * 17)
        assert "17" in refused.value.cause
        # The connection is still usable, which is the point of checking
        # locally: the server would have answered -ERR and closed.
        await nats.publish("orders.created", b"x" * 16)
        await nats.close()

    try:
        run(exchange())
    finally:
        small.stop()


def test_a_subject_this_client_will_not_send_is_refused(server):
    async def exchange():
        nats = await connected(server)
        for subject in ["", "a..b", "orders.*", "orders.>.created"]:
            with pytest.raises(weida_nats.InvalidSubject):
                await nats.publish(subject, b"x")
        with pytest.raises(weida_nats.InvalidSubject):
            await nats.subscribe("orders.>.created")
        await nats.close()

    run(exchange())


def test_an_unknown_connection_option_is_refused(server):
    async def exchange():
        with pytest.raises(weida_nats.Configuration) as refused:
            await connected(server, ping_intervall=5.0)
        assert "ping_intervall" in refused.value.cause
        assert "ping_interval" in refused.value.cause

    run(exchange())


def test_no_responders_needs_headers(server):
    async def exchange():
        with pytest.raises(weida_nats.Configuration):
            await connected(server, headers=False, no_responders=True)

    run(exchange())


def test_a_connection_option_reaches_the_connect_line(server):
    async def exchange():
        nats = await connected(server, name="python", echo=False, verbose=False)
        await nats.flush()
        await nats.close()

    run(exchange())
    assert server.connect["name"] == "python"
    assert server.connect["echo"] is False


def test_a_later_info_carries_the_lame_duck_notice(server):
    async def exchange():
        nats = await connected(server)
        assert not nats.is_lame_duck()
        notified = asyncio.create_task(nats.lame_duck_notice())
        await asyncio.sleep(0.05)

        # An asynchronous INFO, which is how a draining server says so. It
        # mentions nothing else, and nothing else changes: a topology notice
        # that omitted `headers` must not revoke header support.
        server.send_info({"ldm": True})
        assert await notified
        assert nats.is_lame_duck()
        assert nats.headers_supported()
        assert nats.max_payload() == 1048576
        assert nats.info().lame_duck
        await nats.close()

    run(exchange())


# --------------------------------------------------------------------------
# The reactor, three ways


def test_the_ambient_reactor_is_absent_in_a_plain_python_process(server):
    async def exchange():
        with pytest.raises(weida_nats.Runtime) as no_reactor:
            await weida_nats.Connection.connect_current("127.0.0.1", server.port)
        assert "tokio" in no_reactor.value.cause

    run(exchange())


def test_a_second_connection_can_share_the_first_ones_reactor(server):
    other = scripted.Server.start()

    async def exchange():
        first = await connected(server)
        second = await weida_nats.Connection.connect_sharing(
            first, "127.0.0.1", other.port
        )
        subscription = await second.subscribe("orders.>")
        await second.publish("orders.a", b"shared")
        assert (await subscription.next()).payload == b"shared"
        await second.close()
        await first.close()

    try:
        run(exchange())
    finally:
        other.stop()


# --------------------------------------------------------------------------
# The module's shape


def test_the_exception_family_is_the_librarys_enum():
    assert issubclass(weida_nats.NoResponders, weida_nats.NatsError)
    assert issubclass(weida_nats.RequestTimeout, weida_nats.NatsError)
    assert weida_nats.NoResponders is not weida_nats.RequestTimeout
    for name in [
        "Io",
        "Runtime",
        "Decode",
        "Encode",
        "Protocol",
        "Server",
        "AuthenticationRequired",
        "NonceMissing",
        "Signature",
        "TlsRequired",
        "TlsUnsupported",
        "Tls",
        "PayloadTooLarge",
        "ControlLineTooLong",
        "HeadersUnsupported",
        "NoResponders",
        "RequestTimeout",
        "StaleConnection",
        "HandshakeTimeout",
        "ConnectionGone",
        "TooManySubscriptions",
        "TooManyPendingRequests",
        "InvalidSubject",
        "Configuration",
    ]:
        assert issubclass(getattr(weida_nats, name), weida_nats.NatsError), name
        assert name in weida_nats.__all__


def test_the_connection_ends_when_the_server_goes_away(server):
    async def exchange():
        nats = await connected(server)
        ending = asyncio.create_task(nats.closed())
        server.stop()
        state = await ending
        assert not state.is_usable
        assert state.name in ("closed", "failed")
        # Nothing further can be sent on it, and saying so is not a hang.
        with pytest.raises(weida_nats.NatsError):
            await nats.publish("orders.a", b"late")

    run(exchange())
