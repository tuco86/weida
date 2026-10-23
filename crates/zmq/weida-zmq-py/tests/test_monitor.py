"""The monitor and the devices, from Python.

Two renderings of one event stream — an async iterator of typed events
and libzmq's two-frame `inproc://` PAIR form — and the devices over the same
socket types the patterns use.

The proxy test is the central one: an XSUB/XPUB proxy with a capture socket,
a message crossing in each direction, and one captured copy.
"""

import asyncio
import struct

import pytest

import waits
import weida_zmq

DEADLINE = 15.0


def run(coroutine):
    return waits.run(coroutine, DEADLINE)


async def until(predicate, what):
    """Waits for something another task must do, reporting while it takes long."""

    async def poll():
        while not await predicate():
            await asyncio.sleep(0.01)

    await waits.within(what, poll(), 3.0)


def test_the_monitor_is_an_async_iterator_of_typed_events():
    async def watch():
        context = weida_zmq.Context(worker_threads=2)
        server = weida_zmq.PullSocket(context)
        events = await server.monitor()

        endpoint = await server.bind("tcp://127.0.0.1:0")
        client = weida_zmq.PushSocket(context)
        await client.connect(endpoint)
        await client.send(b"hello")
        assert await server.recv() == [b"hello"]

        seen = []
        async for event in events:
            seen.append(event.name)
            assert event.endpoint, event
            assert isinstance(event, weida_zmq.MonitorEvent)
            if "ACCEPTED" in seen and "HANDSHAKE_SUCCEEDED" in seen:
                break
        assert "LISTENING" in seen, seen
        assert "ACCEPTED" in seen, seen
        # The typed event carries the words, where libzmq's wire form carries a
        # number a caller has to look up.
        assert all(isinstance(name, str) for name in seen)

    run(watch())


def test_the_monitor_ends_the_iteration_when_its_socket_is_gone():
    async def watch():
        context = weida_zmq.Context(worker_threads=2)
        socket = weida_zmq.PullSocket(context)
        events = await socket.monitor()
        await socket.bind("inproc://monitor-end")

        # Dropping the socket ends the stream, which for `async for` is the end
        # of the iteration rather than an exception.
        del socket
        collected = [event.name async for event in events]
        assert collected, "the LISTENING event arrived before the end"

    run(watch())


def test_the_same_events_arrive_over_the_inproc_pair_form():
    """What the zguide's Espresso recipe reads: two frames, event and endpoint."""

    async def espresso():
        context = weida_zmq.Context(worker_threads=3)
        watched = weida_zmq.PullSocket(context)
        events = await watched.monitor()

        reader = weida_zmq.PairSocket(context)
        writer = weida_zmq.PairSocket(context)
        await reader.bind("inproc://monitor.pair")
        await writer.connect("inproc://monitor.pair")
        serving = asyncio.create_task(events.serve_pair(writer))

        endpoint = await watched.bind("tcp://127.0.0.1:0")
        source = weida_zmq.PushSocket(context)
        await source.connect(endpoint)
        await source.send(b"brew")
        assert await watched.recv() == [b"brew"]

        message = await reader.recv()
        assert len(message) == 2, "libzmq's monitor form is two frames"
        event_id, value = struct.unpack("<HI", bytes(message[0]))
        assert event_id != 0
        assert value >= 0
        assert bytes(message[1]).startswith(b"tcp://") or bytes(message[1])

        serving.cancel()
        try:
            await serving
        except asyncio.CancelledError:
            pass

    run(espresso())


def test_an_xsub_xpub_proxy_with_a_capture_socket():
    """Both directions cross, and the capture gets a copy."""

    async def pubsub_proxy():
        context = weida_zmq.Context(worker_threads=4)

        # The proxy's two ends, plus a capture socket for the trace.
        frontend = weida_zmq.XSubSocket(context)  # faces the publishers
        backend = weida_zmq.XPubSocket(context)  # faces the subscribers
        await frontend.bind("inproc://proxy.in")
        await backend.bind("inproc://proxy.out")
        capture = weida_zmq.PubSocket(context)
        await capture.bind("inproc://proxy.capture")

        trace = weida_zmq.SubSocket(context)
        await trace.connect("inproc://proxy.capture")
        await trace.subscribe(b"")

        running = asyncio.create_task(weida_zmq.proxy(frontend, backend, capture))

        publisher = weida_zmq.PubSocket(context)
        await publisher.connect("inproc://proxy.in")
        subscriber = weida_zmq.SubSocket(context)
        await subscriber.connect("inproc://proxy.out")
        await subscriber.subscribe(b"weather.")

        # Direction one: the subscription travels from the subscriber, through
        # the XPUB, through the proxy, to the XSUB and upstream to the
        # publisher. It has arrived when the publisher's send reaches someone.
        async def published():
            return (await publisher.send([b"weather.eu", b"rain"])).delivered == 1

        await until(published, "the subscription never reached the publisher")

        # Direction two: the message itself, all the way to the subscriber.
        assert await subscriber.recv() == [b"weather.eu", b"rain"]

        # And the capture socket got a copy of what crossed — in **both**
        # directions, which is what a capture socket is for: the subscription
        # travelling upstream is a message too, and it crossed first.
        captured = []
        for _ in range(10):
            captured.append(list(await trace.recv()))
            if [b"weather.eu", b"rain"] in captured:
                break
        assert [b"weather.eu", b"rain"] in captured, captured
        assert [b"\x01weather."] in captured, captured
        running.cancel()
        try:
            await running
        except asyncio.CancelledError:
            pass

    run(pubsub_proxy())


def test_a_steerable_proxy_pauses_resumes_reports_and_terminates():
    async def steered():
        context = weida_zmq.Context(worker_threads=4)
        # DEALER on both ends, so that both directions of the steering are
        # observable; the streamer's one-way ends are covered by
        # `test_a_streamer_device_forwards`.
        frontend = weida_zmq.DealerSocket(context)
        backend = weida_zmq.DealerSocket(context)
        await frontend.bind("inproc://steer.in")
        await backend.bind("inproc://steer.out")
        # The control socket is a PAIR: libzmq allows PAIR or REP, and REP is
        # not a device end here (its alternation is the application's), so the
        # steering side is a PAIR too.
        control = weida_zmq.PairSocket(context)
        await control.bind("inproc://steer.control")
        steering = weida_zmq.PairSocket(context)
        await steering.connect("inproc://steer.control")

        running = asyncio.create_task(
            weida_zmq.proxy_steerable(frontend, backend, control)
        )

        source = weida_zmq.DealerSocket(context)
        await source.connect("inproc://steer.in")
        sink = weida_zmq.DealerSocket(context)
        await sink.connect("inproc://steer.out")

        await source.send(b"one")
        assert await sink.recv(timeout=5.0) == [b"one"]

        # STATISTICS answers on the control socket with the eight counters.
        await steering.send(weida_zmq.CONTROL_STATISTICS)
        counters = await steering.recv()
        assert len(counters) == 8
        assert struct.unpack("<Q", bytes(counters[0]))[0] >= 1

        # PAUSE stops reading; RESUME starts again and the paused message
        # arrives rather than being lost. Only STATISTICS answers — the other
        # three commands are told, not asked, which is libzmq's shape.
        await steering.send(weida_zmq.CONTROL_PAUSE)
        await source.send(b"two")
        with pytest.raises(weida_zmq.EAGAIN):
            await sink.recv(timeout=0.2)
        await steering.send(weida_zmq.CONTROL_RESUME)
        assert await sink.recv() == [b"two"]

        # TERMINATE ends the proxy and hands back the counters.
        await steering.send(weida_zmq.CONTROL_TERMINATE)
        statistics = await waits.within("the proxy to end", running, 5.0)
        assert isinstance(statistics, weida_zmq.ProxyStatistics)
        assert statistics.frontend_messages_in >= 2
        assert statistics.backend_messages_out >= 2
        assert statistics.frontend_bytes_in >= 6

    run(steered())


def test_req_and_rep_are_not_device_ends():
    async def refused():
        context = weida_zmq.Context()
        request = weida_zmq.ReqSocket(context)
        reply = weida_zmq.RepSocket(context)
        with pytest.raises(TypeError):
            await weida_zmq.proxy(request, reply)

    run(refused())


def test_a_streamer_device_forwards():
    """The zguide's streamer: PULL in, PUSH out.

    A PUSH end cannot be read, and the device does not try: it is a side that
    never delivers rather than an ``ENOTSUP`` at the first poll. The capture
    socket is a PUSH too, so nothing on this device receives but the
    frontend.
    """

    async def streamer():
        context = weida_zmq.Context(worker_threads=4)
        frontend = weida_zmq.PullSocket(context)
        backend = weida_zmq.PushSocket(context)
        capture = weida_zmq.PushSocket(context)
        await frontend.bind("inproc://streamer.in")
        await backend.bind("inproc://streamer.out")
        await capture.bind("inproc://streamer.capture")

        producer = weida_zmq.PushSocket(context)
        await producer.connect("inproc://streamer.in")
        consumer = weida_zmq.PullSocket(context)
        await consumer.connect("inproc://streamer.out")
        trace = weida_zmq.PullSocket(context)
        await trace.connect("inproc://streamer.capture")

        running = asyncio.create_task(weida_zmq.proxy(frontend, backend, capture))

        await producer.send(b"job 1")
        assert await consumer.recv() == [b"job 1"]
        assert await trace.recv() == [b"job 1"]
        assert not running.done(), "a streamer does not die at its first poll"
        running.cancel()
        try:
            await running
        except asyncio.CancelledError:
            pass

    run(streamer())
