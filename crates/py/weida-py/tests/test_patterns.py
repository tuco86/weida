"""Pub/Sub and the streamed surface, from Python.

The claim worth the file is the third test: a payload sixteen times the
subscriber budget reaches two subscribers whole, which `publish` cannot do at
all — the streamed-fan-out guarantee, asserted from the binding rather than
taken from the Rust suite.
"""

import asyncio

import pytest

import weida

DEADLINE = 15.0
CAP = 1 << 20


def run(coroutine):
    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


async def served(path):
    """A runtime, a binding and the url a client dials for `path`."""
    runtime = weida.Runtime()
    binding = await runtime.bind("127.0.0.1:0", weida.Identity.generate())
    return runtime, binding, binding.url(path)


async def subscribed(url, filter_):
    """A connected subscriber holding `filter_`."""
    client = weida.Runtime()
    subscriber = client.subscriber(weida.Trust.by_address())
    await subscriber.connect(url)
    await subscriber.subscribe(filter_)
    return client, subscriber


async def await_filters(publisher, count):
    """Waits until the publisher has seen `count` subscriptions.

    A subscription is recorded in the frame-processing task, so it becomes
    visible a moment after `subscribe` returns. Polling the publisher's own
    counter is exact; publishing a probe would leave copies in the
    subscribers' queues and a sleep would be a guess.
    """
    deadline = asyncio.get_running_loop().time() + DEADLINE
    while publisher.filter_count() != count:
        assert asyncio.get_running_loop().time() < deadline, "no subscription arrived"
        await asyncio.sleep(0.005)


def test_a_published_message_reaches_the_matching_filter_only():
    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        _client, subscriber = await subscribed(url, "px.#")

        await await_filters(publisher, 1)
        assert publisher.publish("px.eur", b"1.0812") == 1
        payload, meta = await subscriber.recv(CAP)
        assert payload == b"1.0812"
        assert meta.topic == "px.eur"

        # The segmented grammar is the library's: `px.#` takes everything
        # under `px` and nothing under `fx`.
        assert publisher.publish("fx.chf", b"0.95") == 0
        assert publisher.subscriber_count() == 1

    run(exchange())


def test_a_slow_subscriber_loses_copies_and_the_publisher_says_which_topic():
    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        _client, subscriber = await subscribed(url, "")

        # Nobody reads: the budget fills and the copies are dropped, counted
        # per topic and per cause.
        await await_filters(publisher, 1)
        for _ in range(2000):
            publisher.publish("px.eur", b"x" * 32768)
        assert publisher.dropped() > 0
        total, budget, queue, no_parked = publisher.dropped_on("px.eur")
        assert total == publisher.dropped()
        assert budget + queue + no_parked == total
        assert publisher.dropped_on("fx.chf") is None

        # The subscriber still works: what it got is well formed.
        payload, meta = await subscriber.recv(CAP)
        assert len(payload) == 32768
        assert meta.topic == "px.eur"

    run(exchange())


def test_a_streamed_publish_carries_what_publish_cannot():
    """The budget bounds a chunk, not the payload."""

    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        first_client, first = await subscribed(url, "px.#")
        second_client, second = await subscribed(url, "px.#")
        assert (first_client, second_client) is not None

        # Both subscriptions, with nothing published yet: a probe would leave
        # copies in the queues and the budget below is the point of the test.
        await await_filters(publisher, 2)

        budget = 8 * 1024 * 1024  # Limits::subscriber_buffer_bytes
        chunk = b"a" * (64 * 1024)
        chunks = 160  # 10 MiB: more than the budget
        with pytest.raises(weida.LimitExceeded):
            publisher.publish("px.eur", b"b" * (budget + 1))

        readers = asyncio.gather(first.recv(CAP * 16), second.recv(CAP * 16))
        fan = publisher.open("px.eur")
        assert fan.topic == "px.eur"
        assert await fan.subscribers() == 2
        for _ in range(chunks):
            left = await fan.write_within(chunk, DEADLINE)
            assert left == 2, "a reading subscriber must not lose the transfer"
        assert await fan.finish() == 2

        (one, _), (two, _) = await readers
        assert len(one) == len(chunk) * chunks
        assert one == two

    run(exchange())


def test_a_streamed_transfer_is_written_and_read_in_pieces():
    async def exchange():
        _server, binding, url = await served("/ingest")
        puller = binding.puller("/ingest")

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)

        async def drain():
            stream, meta = await puller.recv_stream()
            assert meta.endpoint == "/ingest"
            pieces = []
            while True:
                piece = await stream.read(4096)
                if not piece:
                    break
                pieces.append(piece)
            return b"".join(pieces), len(pieces)

        draining = asyncio.create_task(drain())
        out = await pusher.open()
        for _ in range(4):
            await out.write(b"c" * 8192)
        await out.finish()

        payload, pieces = await draining
        assert payload == b"c" * 32768
        # More than one read: the point of the surface is that the payload is
        # never held whole on either side.
        assert pieces > 1

    run(exchange())


def test_a_streamed_exchange_reads_the_reply_while_writing():
    async def exchange():
        _server, binding, url = await served("/echo")
        replier = binding.replier("/echo")

        async def serve():
            request = await replier.accept(CAP)
            await request.reply(request.payload)

        answering = asyncio.create_task(serve())

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        await requester.connect(url)
        request, reply = await requester.open()
        await request.write(b"streamed ")
        await request.write(b"request")
        await request.finish()

        stream, meta = await reply.recv()
        assert meta.endpoint is None, "a reply half carries no endpoint"
        assert await stream.collect(CAP) == b"streamed request"
        await answering

    run(exchange())


def test_a_finished_transfer_is_finished():
    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        fan = publisher.open("px.eur")
        assert await fan.finish() == 0
        with pytest.raises(weida.NoReply):
            await fan.finish()
        with pytest.raises(weida.NoReply):
            await fan.write_now(b"after the end")

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url.replace("/md", "/md"))

    run(exchange())


def test_a_pair_carries_both_directions_and_keeps_its_first_peer():
    """PAIR from Python, and its one rule: the first peer is the one kept."""

    async def exchange():
        _server, binding, url = await served("/link")
        bound = binding.pair("/link")

        client = weida.Runtime()
        paired = client.pair(weida.Trust.by_address())
        await paired.connect(url)
        assert paired.peer_count() == 1

        await paired.send(b"ping")
        heard, meta = await bound.recv(CAP)
        assert heard == b"ping"
        assert meta.endpoint == "/link"
        await bound.send(b"pong")
        answer, _ = await paired.recv(CAP)
        assert answer == b"pong"

        # A second dialler: the connection is accepted, because the refusal
        # is per transfer, and the transfer is refused. Written past the 1 MiB
        # stream window so the refusal cannot lose the race to the receipt
        # (docs/decisions/0005-refusal-race.md).
        newcomer = weida.Runtime()
        second = newcomer.pair(weida.Trust.by_address())
        await second.connect(url)
        with pytest.raises(weida.LimitExceeded):
            await second.send(b"z" * (2 << 20))

        # And the first peer still delivers, which is the whole rule.
        await paired.send(b"still mine")
        kept, _ = await bound.recv(CAP)
        assert kept == b"still mine"

        # A pair has one peer, so dialling twice on one endpoint is refused
        # too — before any transfer.
        with pytest.raises(weida.LimitExceeded):
            await paired.connect(url)

    run(exchange())


def test_a_survey_collects_what_answers_and_counts_the_silence():
    """SURVEY from Python, and its one rule: the deadline is the caller's."""

    async def exchange():
        _server, binding, url = await served("/poll")
        answering = binding.respondent("/poll")
        quiet = binding.respondent("/quiet")

        async def answer():
            question = await answering.accept(CAP)
            assert question.payload == b"who is there"
            await question.reply(b"me")

        async def say_nothing():
            # Accepts the question and never answers: not unreachable, not
            # refusing, just silent — which is what a deadline is for. The
            # request is held, because dropping it would report NO_REPLY and
            # that would be a failure rather than silence.
            question = await quiet.accept(CAP)
            await asyncio.sleep(DEADLINE)
            return question

        answered = asyncio.create_task(answer())
        holding = asyncio.create_task(say_nothing())

        client = weida.Runtime()
        surveyor = client.surveyor(weida.Trust.by_address())
        await surveyor.connect(url)
        await surveyor.connect(url.replace("/poll", "/quiet"))
        assert surveyor.peer_count() == 2

        survey = await surveyor.survey(b"who is there", 0.5, CAP)
        assert survey.asked == 2, repr(survey)
        assert survey.replies == [b"me"], repr(survey)
        assert survey.silent() == 1, repr(survey)
        assert survey.failed == 0, repr(survey)

        await answered
        holding.cancel()

    run(exchange())


def test_a_bus_message_reaches_every_other_member_and_never_the_sender():
    """BUS from Python, and its one rule: never your own message."""

    async def exchange():
        _first_rt, first_binding, first_url = await served("/bus1")
        _second_rt, second_binding, second_url = await served("/bus2")
        one = first_binding.bus("/bus1", weida.Trust.by_address())
        two = second_binding.bus("/bus2", weida.Trust.by_address())
        await one.connect(second_url)
        await two.connect(first_url)
        assert one.peer_count() == 1

        assert await one.send(b"hello all") == 1
        heard, meta = await two.recv(CAP)
        assert heard == b"hello all"
        assert meta.endpoint == "/bus2"

        # Nothing came back to the sender: `one` has no message of its own to
        # read, so a receive with everything already delivered would hang —
        # which is why this is a timeout and not an assertion on a queue.
        assert await two.send(b"and back") == 1
        mine, _ = await one.recv(CAP)
        assert mine == b"and back"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(one.recv(CAP), 0.3)
        assert one.dropped() == 0

    run(exchange())


def test_a_radio_segment_and_datagram_reach_a_dish():
    """RADIO/DISH from Python: one stream segment and one datagram segment."""

    async def exchange():
        server = weida.Runtime(datagrams=True)
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        radio = binding.radio("/r")
        client = weida.Runtime(datagrams=True)
        dish = client.dish(weida.Trust.by_address())
        await dish.join("v", 0.5)
        await dish.connect(binding.url("/r"))
        deadline = asyncio.get_running_loop().time() + DEADLINE
        while radio.dish_count() != 1:
            assert asyncio.get_running_loop().time() < deadline, "no join arrived"
            await asyncio.sleep(0.005)

        segment = radio.segment("v")
        assert segment.write(b"key") == 1
        assert segment.write(b"frame") == 1
        assert segment.finish() == 1
        kind, payload, meta = await dish.recv(CAP)
        assert (kind, payload) == ("segment", b"keyframe")
        assert (meta.topic, meta.segment) == ("v", 0)

        assert radio.datagram("v", b"voice") == 1
        assert await dish.recv(CAP) == ("datagram", "v", 1, b"voice")
        assert radio.dropped() == 0

    run(exchange())


def test_a_layered_segment_reaches_a_dish_layer_by_layer():
    """Layered segments from Python: each layer is its own segment arrival."""

    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        radio = binding.radio("/r")
        client = weida.Runtime()
        dish = client.dish(weida.Trust.by_address())
        await dish.join("v", max_layer=1)
        await dish.connect(binding.url("/r"))
        deadline = asyncio.get_running_loop().time() + DEADLINE
        while radio.dish_count() != 1:
            assert asyncio.get_running_loop().time() < deadline, "no join arrived"
            await asyncio.sleep(0.005)

        seg = radio.segment("v", max_age=5.0, priority=3)
        assert seg.write_layer(0, b"base") == 1
        assert seg.write_layer(1, b"more") == 1
        assert seg.write_layer(2, b"capped away") == 0
        assert seg.finish() == 1
        heard = set()
        for _ in range(2):
            kind, payload, meta = await asyncio.wait_for(dish.recv(CAP), DEADLINE)
            assert kind == "segment"
            assert meta.segment == 0
            heard.add((meta.layer, payload))
        assert heard == {(0, b"base"), (1, b"more")}

        drops = radio.dish_drops()
        assert len(drops) == 1
        assert drops[0].layers_cut == 0

    run(exchange())
