"""Cursors from Python: a verdict for a transfer that has no reply.

Both surfaces are here, because cursors are one subject: the asyncio cases
first, the synchronous one after them. What each case is for:

* a Push producer gets ``Processed`` **without an exchange**, which is the
  whole point of the cursor surface -- a fire-and-forget transfer has no reply
  to carry a verdict, and the receipt it does get is the transport's;
* an **application** level is carried and never interpreted, which is visible
  here in a way it is not in Rust: a level is an integer on this surface, so
  ``weida.APPLICATION_FLOOR + 3`` is an ordinary argument;
* a value the protocol *reserves* is refused rather than reinterpreted;
* ordering nothing hands out nothing, on both ends.
"""

import asyncio
import threading

import pytest

import weida
from weida import sync

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


async def until(cursors, level):
    """Reads cursors until `level` is reported, or the report ends."""
    while True:
        latest = await cursors.changed()
        if latest is None:
            return None
        if level in latest:
            return latest


def test_a_push_gets_a_verdict_from_a_cursor_with_no_exchange():
    async def exchange():
        _server, binding, url = await served("/work")
        puller = binding.puller("/work")

        async def receive():
            payload, meta, reporter = await puller.recv_reporting(CAP)
            assert payload == b"a unit of work"
            # The order is in the DATA header the sender wrote, which is what
            # makes a report the sender's request and not this side's idea.
            assert meta.report == [weida.ACCEPTED, weida.PROCESSED]
            assert meta.report_mode == weida.PROGRESS
            assert meta.report_id is not None

            assert reporter is not None, "the sender ordered a report"
            assert reporter.levels == [weida.ACCEPTED, weida.PROCESSED]
            assert reporter.mode == weida.PROGRESS
            await reporter.report(weida.ACCEPTED, len(payload))
            # A level nobody ordered is ignored rather than refused: the order
            # says what the sender wants to hear.
            await reporter.report(weida.STORED, 1)
            await reporter.report(weida.PROCESSED, len(payload))
            await reporter.finish()

        receiving = asyncio.create_task(receive())

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)
        body = b"a unit of work"
        cursors = await pusher.send(body, report=[weida.PROCESSED, weida.ACCEPTED])
        assert cursors is not None, "the send ordered a report"

        # The verdict arrives **after** the transport receipt `send` already
        # waited for: `Processed` is not something a FIN can carry.
        latest = await until(cursors, weida.PROCESSED)
        assert latest is not None, "the terminal cursor"
        assert latest[weida.PROCESSED] == len(body)
        # A cursor is absolute, so the earlier level is still readable.
        assert latest[weida.ACCEPTED] == len(body)
        assert cursors.offset(weida.PROCESSED) == len(body)
        # The level nobody ordered never arrived.
        assert weida.STORED not in cursors.snapshot()
        assert cursors.offset(weida.STORED) is None

        await receiving

    run(exchange())


def test_an_application_level_is_carried_and_never_interpreted():
    async def exchange():
        _server, binding, url = await served("/stage")
        puller = binding.puller("/stage")
        stage = weida.APPLICATION_FLOOR + 3

        async def receive():
            payload, meta, reporter = await puller.recv_reporting(CAP)
            assert meta.report == [stage]
            await reporter.report(stage, 4096)
            await reporter.finish()
            return payload

        receiving = asyncio.create_task(receive())

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)
        cursors = await pusher.send(b"staged", report=[stage])
        latest = await until(cursors, stage)
        # 4096 is nothing the library can check against a 6-byte payload, and
        # that is the claim: an application stage is carried and ordered, never
        # interpreted.
        assert latest[stage] == 4096
        assert await receiving == b"staged"

    run(exchange())


def test_a_value_the_protocol_reserves_is_refused():
    async def exchange():
        _server, binding, url = await served("/reserved")
        _puller = binding.puller("/reserved")
        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)

        # 7 is below the application floor and is not a named level: the
        # reserved range is where a later version of the protocol puts its own
        # stages, so this is a violation rather than a private level.
        with pytest.raises(weida.Protocol):
            await pusher.send(b"x", report=[7])
        with pytest.raises(weida.Protocol):
            await pusher.send(b"x", report=[weida.ACCEPTED], mode=9)
        # And the floor itself is fine, which is what makes the line a line.
        assert await pusher.send(b"x", report=[weida.APPLICATION_FLOOR]) is not None

    run(exchange())


def test_a_transfer_that_orders_no_report_hands_out_no_handles():
    async def exchange():
        _server, binding, url = await served("/plain")
        puller = binding.puller("/plain")

        async def receive():
            payload, meta, reporter = await puller.recv_reporting(CAP)
            assert payload == b"no report"
            assert meta.report == []
            assert meta.report_id is None
            assert reporter is None, "a reporter with nothing ordered writes to nobody"

        receiving = asyncio.create_task(receive())

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)
        assert await pusher.send(b"no report") is None
        await receiving

    run(exchange())


def test_a_streamed_transfer_orders_a_report_too():
    async def exchange():
        _server, binding, url = await served("/bulk")
        puller = binding.puller("/bulk")

        async def receive():
            stream, meta = await puller.recv_stream()
            assert meta.report == [weida.PROCESSED]
            reporter = await stream.reporter()
            payload = await stream.collect(CAP)
            await reporter.report(weida.PROCESSED, len(payload))
            await reporter.finish()
            return payload

        receiving = asyncio.create_task(receive())

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)
        out = await pusher.open(report=[weida.PROCESSED])
        cursors = await out.cursors()
        assert cursors is not None
        # One report, so one reader: a second ask gets nothing.
        assert await out.cursors() is None
        await out.write(b"a" * 8192)
        await out.finish()

        latest = await until(cursors, weida.PROCESSED)
        assert latest[weida.PROCESSED] == 8192
        assert len(await receiving) == 8192

    run(exchange())


def test_a_cursor_reaches_a_synchronous_sender():
    """The same verdict with no event loop anywhere (`sync`)."""
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    puller = binding.puller("/work")

    def receive():
        payload, meta, reporter = puller.recv_reporting(CAP)
        assert meta.report == [weida.ACCEPTED, weida.PROCESSED]
        assert reporter.levels == [weida.ACCEPTED, weida.PROCESSED]
        assert reporter.mode == weida.FINAL_ONLY
        reporter.report(weida.ACCEPTED, len(payload))
        reporter.report(weida.PROCESSED, len(payload))
        # FinalOnly writes nothing until here, and the flush is what makes
        # that lossless.
        reporter.finish()

    receiving = threading.Thread(target=receive, daemon=True)
    receiving.start()

    client = sync.Runtime()
    pusher = client.pusher(weida.Trust.by_address())
    pusher.connect(binding.url("/work"))
    body = b"a unit of work"
    cursors = pusher.send(
        body,
        report=[weida.ACCEPTED, weida.PROCESSED],
        mode=weida.FINAL_ONLY,
    )
    assert cursors is not None

    # The wait is bounded, because a synchronous one has to be: `changed`
    # takes a deadline and raises `TimeoutError` when it passes, so a
    # receiver that died on an assertion fails this case in seconds instead
    # of parking the thread on a connection nobody closes.
    seen = {}
    while weida.PROCESSED not in seen:
        latest = cursors.changed(DEADLINE)
        assert latest is not None, f"the report ended without a verdict: {seen}"
        seen.update(latest)
    assert seen[weida.PROCESSED] == len(body)
    assert cursors.offset(weida.ACCEPTED) == len(body)

    receiving.join(DEADLINE)
    assert not receiving.is_alive(), "the receiving thread must have finished"

    client.shutdown()
    server.shutdown()


def test_the_cursor_vocabulary_is_shared_by_both_surfaces():
    # One vocabulary: the levels and the modes are module constants, so the
    # same integer means the same thing on `weida` and on `weida.sync`.
    assert (weida.TRANSPORT_RECEIPT, weida.ACCEPTED, weida.STORED) == (1, 2, 3)
    assert (weida.REPLICATED, weida.PROCESSED) == (4, 5)
    assert weida.APPLICATION_FLOOR == 16
    assert (weida.PROGRESS, weida.FINAL_ONLY) == (0, 1)
    assert "Cursors" in weida.__all__ and "Cursors" in sync.__all__
    assert "PROCESSED" in weida.__all__
