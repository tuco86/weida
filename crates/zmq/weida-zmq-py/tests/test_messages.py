"""Sending and receiving: the asyncio surface of all eleven socket types.

What is asserted here beyond "a message crossed":

* a `Multipart` is one value, sent and received atomically — all frames or
  none, which is 37/ZMTP's rule and not an accident of this binding;
* `send_nowait`/`recv_nowait` are `ZMQ_DONTWAIT`, so they report `EAGAIN`
  rather than waiting;
* a `timeout` argument bounds one call, and expiring is `EAGAIN`;
* a cancelled `asyncio.Task` waiting in `recv` leaves the socket **usable**,
  which is proved by receiving the next message on that same socket rather
  than by the absence of an exception.
"""

import asyncio

import pytest

import weida_zmq

DEADLINE = 10.0


def run(coroutine):
    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


async def connected(context, server_type, client_type, endpoint):
    """A bound socket and a connected one, in that order."""
    server = getattr(weida_zmq, server_type)(context)
    client = getattr(weida_zmq, client_type)(context)
    bound = await server.bind(endpoint)
    await client.connect(bound)
    return server, client


# --------------------------------------------------------------------------
# The message value


def test_multipart_is_a_sequence_of_frames():
    message = weida_zmq.Multipart(b"one", b"", b"three")
    assert len(message) == 3
    assert message[0] == b"one"
    assert message[1] == b""
    assert message[-1] == b"three"
    assert list(message) == [b"one", b"", b"three"]
    assert message.frames == [b"one", b"", b"three"]
    assert message == [b"one", b"", b"three"]
    assert message == weida_zmq.Multipart([b"one", b"", b"three"])
    assert message != [b"one"]
    assert message != "not a message"
    with pytest.raises(IndexError):
        message[3]
    assert "3 frame(s)" in repr(message)


def test_a_message_of_no_frames_does_not_exist():
    with pytest.raises(weida_zmq.EINVAL) as empty:
        weida_zmq.Multipart()
    assert empty.value.errno == "EINVAL"
    # The smallest message is one empty frame, and that one is legal: it is
    # the envelope delimiter every request-reply pattern prepends.
    assert len(weida_zmq.Multipart(b"")) == 1


def test_a_multipart_message_crosses_whole_or_not_at_all():
    async def exchange():
        context = weida_zmq.Context()
        sink, source = await connected(
            context, "PullSocket", "PushSocket", "inproc://atomic"
        )
        await source.send(weida_zmq.Multipart(b"header", b"", b"body"))
        received = await sink.recv()
        assert isinstance(received, weida_zmq.Multipart)
        assert received == [b"header", b"", b"body"]
        # Nothing arrived frame by frame: one receive took the whole message,
        # three frames and all, and a second receive has nothing to take.
        with pytest.raises(weida_zmq.EAGAIN):
            sink.recv_nowait()

    run(exchange())


# --------------------------------------------------------------------------
# Every pattern, both directions


def test_req_rep():
    async def exchange():
        context = weida_zmq.Context()
        server, client = await connected(
            context, "RepSocket", "ReqSocket", "inproc://req-rep"
        )
        await client.send(b"question")
        assert await server.recv() == [b"question"]
        report = await server.send(b"answer")
        assert report.queued and not report.dropped and bool(report)
        assert await client.recv() == [b"answer"]

    run(exchange())


def test_dealer_router_carry_the_routing_id():
    async def exchange():
        context = weida_zmq.Context()
        router, dealer = await connected(
            context, "RouterSocket", "DealerSocket", "inproc://dealer-router"
        )
        await dealer.send(b"work")
        request = await router.recv()
        assert len(request) == 2, "ROUTER prepends the peer's routing id"
        routing_id, body = request[0], request[1]
        assert body == b"work"

        report = await router.send([routing_id, b"result"])
        assert report.queued
        assert await dealer.recv() == [b"result"]

    run(exchange())


def test_push_pull():
    async def pipeline():
        context = weida_zmq.Context()
        sink, source = await connected(
            context, "PullSocket", "PushSocket", "inproc://pipeline"
        )
        await source.send(b"task")
        assert await sink.recv() == [b"task"]
        assert not hasattr(sink, "send"), "a PULL socket does not send"
        assert not hasattr(source, "recv"), "a PUSH socket does not receive"

    run(pipeline())


def test_pub_sub_with_prefix_matching():
    async def broadcast():
        context = weida_zmq.Context()
        publisher, subscriber = await connected(
            context, "PubSocket", "SubSocket", "inproc://pub-sub"
        )
        await subscriber.subscribe(b"weather.")

        # The zguide's slow joiner: publish until a subscriber's queue took a
        # copy, which `Published` is the only way to know.
        for _ in range(200):
            published = await publisher.send([b"weather.eu", b"rain"])
            if published.delivered == 1:
                break
            await asyncio.sleep(0.01)
        assert published.delivered == 1
        assert published.dropped == 0
        assert await subscriber.recv() == [b"weather.eu", b"rain"]

        # A topic nobody subscribed to reaches nobody, rather than being
        # filtered after it crossed.
        assert (await publisher.send([b"sports.eu", b"goal"])).delivered == 0
        assert not hasattr(publisher, "recv"), "a PUB socket does not receive"

    run(broadcast())


def test_xpub_xsub_carry_the_subscription_itself():
    async def broadcast():
        context = weida_zmq.Context()
        xpub, xsub = await connected(
            context, "XPubSocket", "XSubSocket", "inproc://xpub-xsub"
        )
        await xsub.subscribe(b"topic")
        # An XPUB is given its subscribers' subscriptions as messages, in
        # libzmq's 1/0 form — that is what makes a pub/sub proxy possible.
        subscription = await xpub.recv()
        assert subscription == [b"\x01topic"]

        for _ in range(200):
            published = await xpub.send([b"topic", b"payload"])
            if published.delivered == 1:
                break
            await asyncio.sleep(0.01)
        assert published.delivered == 1
        assert await xsub.recv() == [b"topic", b"payload"]

    run(broadcast())


def test_pair():
    async def exchange():
        context = weida_zmq.Context()
        left, right = await connected(
            context, "PairSocket", "PairSocket", "inproc://pair"
        )
        await left.send(b"ping")
        assert await right.recv() == [b"ping"]
        await right.send(b"pong")
        assert await left.recv() == [b"pong"]

    run(exchange())


# --------------------------------------------------------------------------
# ZMQ_DONTWAIT and the timeouts


def test_recv_nowait_reports_eagain_rather_than_waiting():
    async def empty():
        context = weida_zmq.Context()
        sink = weida_zmq.PullSocket(context)
        await sink.bind("inproc://nowait")
        with pytest.raises(weida_zmq.EAGAIN) as nothing:
            sink.recv_nowait()
        assert nothing.value.errno == "EAGAIN"

    run(empty())


def test_send_nowait_reports_eagain_with_nowhere_to_send():
    async def nowhere():
        context = weida_zmq.Context()
        source = weida_zmq.PushSocket(context)
        await source.bind("inproc://nowait-send")
        with pytest.raises(weida_zmq.EAGAIN):
            source.send_nowait(b"task")

    run(nowhere())


def test_nowait_takes_what_is_already_there():
    async def queued():
        context = weida_zmq.Context()
        sink, source = await connected(
            context, "PullSocket", "PushSocket", "inproc://nowait-queued"
        )
        await source.send(b"task")
        for _ in range(200):
            try:
                assert sink.recv_nowait() == [b"task"]
                break
            except weida_zmq.EAGAIN:
                await asyncio.sleep(0.01)
        else:
            raise AssertionError("the message never arrived")

    run(queued())


def test_a_timeout_expires_as_eagain():
    async def expire():
        context = weida_zmq.Context()
        sink = weida_zmq.PullSocket(context)
        await sink.bind("inproc://timeout")
        with pytest.raises(weida_zmq.EAGAIN):
            await sink.recv(timeout=0.05)
        # And the socket still works afterwards.
        source = weida_zmq.PushSocket(context)
        await source.connect("inproc://timeout")
        await source.send(b"late")
        assert await sink.recv(timeout=5.0) == [b"late"]

    run(expire())


def test_a_timeout_that_is_not_a_bound_is_refused_where_it_is_given():
    async def refuse():
        context = weida_zmq.Context()
        sink = weida_zmq.PullSocket(context)
        with pytest.raises(weida_zmq.EINVAL):
            await sink.recv(timeout=-1.0)
        with pytest.raises(weida_zmq.EINVAL):
            await sink.recv(timeout=float("inf"))

    run(refuse())


# --------------------------------------------------------------------------
# Cancellation


def test_a_cancelled_receive_leaves_the_socket_usable():
    """The proof is the next message, not the absence of an exception."""

    async def cancel_then_receive():
        context = weida_zmq.Context()
        server, client = await connected(
            context, "RepSocket", "ReqSocket", "inproc://cancelled"
        )

        waiting = asyncio.create_task(server.recv())
        await asyncio.sleep(0.05)
        assert not waiting.done()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        # The socket is not half-used: the request sent now is received on it.
        await client.send(b"after the cancellation")
        assert await server.recv() == [b"after the cancellation"]
        await server.send(b"still working")
        assert await client.recv() == [b"still working"]

    run(cancel_then_receive())


def test_a_cancelled_receive_does_not_swallow_the_message_it_did_not_take():
    async def cancel_with_a_message_in_flight():
        context = weida_zmq.Context()
        sink, source = await connected(
            context, "PullSocket", "PushSocket", "inproc://cancel-queued"
        )
        waiting = asyncio.create_task(sink.recv())
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        await source.send(b"one")
        await source.send(b"two")
        assert await sink.recv() == [b"one"]
        assert await sink.recv() == [b"two"]

    run(cancel_with_a_message_in_flight())


def test_a_timed_out_receive_and_a_later_one_share_the_socket():
    """Queueing on one socket is serialisation, not a deadlock."""

    async def two_receivers():
        context = weida_zmq.Context()
        sink, source = await connected(
            context, "PullSocket", "PushSocket", "inproc://two-receivers"
        )
        first = asyncio.create_task(sink.recv())
        second = asyncio.create_task(sink.recv())
        await asyncio.sleep(0.05)
        await source.send(b"one")
        await source.send(b"two")
        assert {bytes((await first)[0]), bytes((await second)[0])} == {b"one", b"two"}

    run(two_receivers())
