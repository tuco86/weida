"""`sock.split()`: sending while another coroutine is parked in a receive.

Without a split, every operation on one socket object queues
behind the one in front, because the binding leases the socket to one call at
a time (`weida_zmq.sockets`' module documentation). A DEALER waiting for a
reply therefore could not send the next request until the reply arrived, and
pyzmq lets it. `split()` hands out two halves onto the same connections, and
the tests here are what says they run at the same time:

* `test_dealer_sends_while_recv_is_parked` parks a receive first, checks it
  is still pending, and then sends and completes a full round trip on the
  other half. Before the split the send could not even start.
* the retirement tests pin what happens to the object that was split, in
  both directions: its methods raise `ENOTSOCK`, and splitting twice is the
  same refusal rather than a second pair of halves.

Every coroutine runs under a wall-clock bound, so a binding that deadlocks
fails instead of hanging (docs/LOOP.md 2).
"""

import asyncio

import pytest

import weida_zmq

DEADLINE = 10.0


def run(coroutine):
    """Runs one coroutine to completion, under a deadline."""

    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


def test_dealer_sends_while_recv_is_parked():
    """A parked receive does not hold up the other direction."""

    async def exchange():
        context = weida_zmq.Context()
        server = weida_zmq.RouterSocket(context)
        await server.bind("inproc://split-dealer")
        client = weida_zmq.DealerSocket(context)
        await client.connect("inproc://split-dealer")
        send, recv = await client.split()

        # Park the receive first: nothing has been sent, so it waits.
        parked = asyncio.create_task(recv.recv())
        await asyncio.sleep(0)
        assert not parked.done()

        # This is the part the whole socket could not do.
        await send.send([b"ask"])
        asked = await server.recv()
        assert asked.frames[1:] == [b"ask"]
        await server.send([asked.frames[0], b"answer"])

        answer = await asyncio.wait_for(parked, DEADLINE)
        assert answer == [b"answer"]

    run(exchange())


def test_pair_halves_cross_at_once():
    """Two PAIR sockets, each split, both directions in flight together."""

    async def exchange():
        context = weida_zmq.Context()
        left = weida_zmq.PairSocket(context)
        await left.bind("inproc://split-pair")
        right = weida_zmq.PairSocket(context)
        await right.connect("inproc://split-pair")
        left_send, left_recv = await left.split()
        right_send, right_recv = await right.split()

        waiting_left = asyncio.create_task(left_recv.recv())
        waiting_right = asyncio.create_task(right_recv.recv())
        await asyncio.gather(
            left_send.send([b"to-right"]),
            right_send.send([b"to-left"]),
        )
        at_right, at_left = await asyncio.gather(waiting_right, waiting_left)
        assert at_right == [b"to-right"]
        assert at_left == [b"to-left"]

    run(exchange())


def test_xpub_publishes_while_subscriptions_arrive():
    """XPUB's halves: a subscription is read while a publish goes out."""

    async def exchange():
        context = weida_zmq.Context()
        publisher = weida_zmq.XPubSocket(context)
        bound = await publisher.bind("inproc://split-xpub")
        subscriber = weida_zmq.SubSocket(context)
        await subscriber.subscribe(b"topic")
        await subscriber.connect(bound)
        publish, events = await publisher.split()

        subscription = await events.recv()
        assert subscription == [b"\x01topic"]

        published = await publish.send([b"topic", b"body"])
        assert published.delivered == 1
        arrived = await subscriber.recv()
        assert arrived == [b"topic", b"body"]

    run(exchange())


def test_split_retires_the_socket():
    """The object that was split is no longer a socket."""

    async def exchange():
        context = weida_zmq.Context()
        peer = weida_zmq.PairSocket(context)
        bound = await peer.bind("inproc://split-retired")
        socket = weida_zmq.DealerSocket(context)
        await socket.connect(bound)
        send, _recv = await socket.split()

        with pytest.raises(weida_zmq.ENOTSOCK) as refused:
            await socket.send([b"through the old object"])
        assert "split" in str(refused.value)

        # The halves still work: retirement is about the old handle only.
        assert send.send_nowait([b"through the half"]) is None

    run(exchange())


def test_splitting_twice_is_refused():
    """A second `split()` is the same refusal, not a second pair."""

    async def exchange():
        context = weida_zmq.Context()
        peer = weida_zmq.PairSocket(context)
        bound = await peer.bind("inproc://split-twice")
        socket = weida_zmq.PairSocket(context)
        await socket.connect(bound)
        _halves = await socket.split()
        with pytest.raises(weida_zmq.ENOTSOCK):
            await socket.split()

    run(exchange())
