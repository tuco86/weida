"""weida from Python: Req/Rep, Push/Pull, trust and the failure classes.

Both halves of every exchange are this library, so these tests need no
external process at all — which is what makes weida's own binding the
easiest of the six to test.

Nothing here fails for being slow: a slow machine is still a run, and a binding
that deadlocks is caught by the CI job's own budget (docs/LOOP.md 2).
"""

import asyncio

import pytest

import weida

# Every receive takes a ceiling: the binding has no default, because a
# default would be a decision about how much memory a stranger may make this
# process allocate.
CAP = 1 << 20


def run(coroutine):
    """Runs one coroutine to completion."""
    return asyncio.run(coroutine)


def test_a_request_is_answered_over_quic():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        replier = binding.replier("/echo")

        async def serve():
            request = await replier.accept(CAP)
            assert request.payload == b"ping"
            # The metadata is the header's: the path it was addressed to is
            # there, and the peer is None because this client presented no
            # identity.
            assert request.meta.endpoint == "/echo"
            assert request.meta.peer is None
            assert request.meta.content_len == 4
            await request.reply(b"pong")

        answering = asyncio.create_task(serve())

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        # The address is the whole configuration: it names the key that must
        # answer, so there is no CA and no certificate file.
        await requester.connect(binding.url("/echo"))
        assert await requester.request(b"ping", CAP) == b"pong"
        await answering

    run(exchange())


def test_a_push_reaches_a_puller_with_its_metadata():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        puller = binding.puller("/ingest")

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(binding.url("/ingest"))

        receiving = asyncio.create_task(puller.recv(CAP))
        # `send` returns when the peer's transport holds the bytes — the
        # receipt, not an application acknowledgement.
        await pusher.send(b"a sample")
        payload, meta = await receiving
        assert payload == b"a sample"
        assert meta.endpoint == "/ingest"
        assert meta.content_len == 8

    run(exchange())


def test_a_payload_above_the_ceiling_is_refused_rather_than_held():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        replier = binding.replier("/echo")

        async def serve():
            request = await replier.accept(CAP)
            await request.reply(b"x" * 4096)

        answering = asyncio.create_task(serve())

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        await requester.connect(binding.url("/echo"))
        with pytest.raises(weida.LimitExceeded):
            # The reply is 4096 bytes and the caller allowed 16.
            await requester.request(b"ping", 16)
        await answering

    run(exchange())


def test_an_unknown_path_and_a_refusal_are_distinct_classes():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        replier = binding.replier("/echo")

        async def refuse_one():
            request = await replier.accept(CAP)
            await request.refuse()

        answering = asyncio.create_task(refuse_one())

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        await requester.connect(binding.url("/echo"))
        with pytest.raises(weida.Rejected):
            await requester.request(b"ping", CAP)
        await answering

        # A path nothing is registered under is a different failure, and a
        # script that retries needs to tell them apart.
        other = client.requester(weida.Trust.by_address())
        await other.connect(binding.url("/nowhere"))
        with pytest.raises(weida.UnknownEndpoint):
            await other.request(b"ping", CAP)

    run(exchange())


def test_the_wrong_key_is_untrusted_and_says_which_one_answered():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        _replier = binding.replier("/echo")

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        wrong = "weida://sha256:%s@%s/echo" % ("0" * 64, binding.local_addr())
        with pytest.raises(weida.Untrusted) as refused:
            await requester.connect(wrong)
        # The fingerprint that answered is in the message, so an operator can
        # check it out of band and paste it into the address.
        assert binding.fingerprint() in refused.value.cause

    run(exchange())


def test_a_request_carries_one_reply():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        replier = binding.replier("/echo")

        async def answer_twice():
            request = await replier.accept(CAP)
            await request.reply(b"once")
            with pytest.raises(weida.NoReply):
                await request.reply(b"twice")

        answering = asyncio.create_task(answer_twice())

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        await requester.connect(binding.url("/echo"))
        assert await requester.request(b"ping", CAP) == b"once"
        await answering

    run(exchange())


def test_the_module_surface_is_what_it_says():
    for name in weida.__all__:
        assert hasattr(weida, name), name
    # Every failure class derives from the base, which is what lets a caller
    # catch the family.
    assert issubclass(weida.Untrusted, weida.WeidaError)
    assert issubclass(weida.Indeterminate, weida.WeidaError)
    assert weida.VERSION == 0
    assert isinstance(weida.ALPN, bytes)


def test_a_drain_reports_what_it_achieved():
    async def exchange():
        server = weida.Runtime()
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        puller = binding.puller("/ingest")

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(binding.url("/ingest"))
        receiving = asyncio.create_task(puller.recv(CAP))
        await pusher.send(b"one")
        assert (await receiving)[0] == b"one"

        # Nothing is in flight, so the drain is immediate and reports zero
        # outstanding — a number to log rather than an error.
        delivered, outstanding = await client.drain(5.0)
        assert outstanding == 0
        assert delivered >= 0

    run(exchange())


def test_connection_stats_over_quic_and_in_process():
    """A dialling class reads its own link: an RTT over QUIC, with the peer's
    view when both runtimes report; no transport in process."""

    async def exchange():
        server = weida.Runtime(path_report=True)
        binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
        binding.puller("/jobs")
        client = weida.Runtime(path_report=True)
        pusher = client.pusher(weida.Trust.by_address())
        assert pusher.connection_stats() == []
        url = binding.url("/jobs")
        await pusher.connect(url)
        while (stats := pusher.connection_stats()[0]).remote is None:
            await asyncio.sleep(0.01)
        assert stats.url == url
        assert stats.redials == 0
        assert stats.transport.path.rtt > 0
        assert stats.transport.path.current_mtu >= 1200
        assert stats.transport.tx.datagrams > 0
        assert stats.remote.rtt > 0

        local = weida.Runtime()
        bus = local.bind_inproc("weida-py-stats")
        bus.puller("/jobs")
        assert bus.fingerprint() is None
        dialler = local.pusher(weida.Trust.by_address())
        await dialler.connect(bus.url("/jobs"))
        [record] = dialler.connection_stats()
        assert record.url == "weida+inproc://weida-py-stats/jobs"
        assert record.transport is None
        assert record.remote is None

    run(exchange())
