"""The eleven protocols, from Python, over real connections.

The first test here is an asyncio REQ/REP round trip between
two sockets of this module, over each of the three transports the binding
exposes. The rest assert the sentence each protocol's manual page claims —
PUSH never discards, PUB is best effort and says how many copies it lost, SUB
filters at the receiver, a surveyor's deadline starts at the send, BUS
travels one hop — rather than merely that bytes moved.

Every test runs its coroutine through `waits.run`: a slow run is reported on
stderr and goes on, because the suite has no hard time limit on a real clock
and the CI job's budget is the hang guard.
"""

import asyncio
import time

import pytest

import waits
import weida_nng


def run(coroutine):
    """Runs one coroutine to completion, reporting while it takes long."""
    return waits.run(coroutine)


def options(**kwargs):
    """NNG's own defaults: wait forever, the CI job's budget is the guard."""
    return weida_nng.SocketOptions(**kwargs)


def endpoints(tmp_path):
    """One endpoint per transport this binding exposes."""
    return {
        "tcp": "tcp://127.0.0.1:0",
        "ipc": f"ipc://{tmp_path}/round-trip.sock",
        "inproc": "inproc://round-trip",
    }


@pytest.mark.parametrize("transport", ["tcp", "ipc", "inproc"])
def test_a_request_reaches_a_replier_and_the_reply_comes_back(tmp_path, transport):
    async def exchange():
        ctx = weida_nng.Context()
        server = weida_nng.RepSocket(ctx, options())
        client = weida_nng.ReqSocket(ctx, options())
        url = await server.listen(endpoints(tmp_path)[transport])
        await client.dial(url)

        await client.send(b"ping")
        assert await server.recv() == b"ping"
        await server.send(b"pong")
        assert await client.recv() == b"pong"

        await ctx.shutdown()

    run(exchange())


def test_a_reply_nobody_asked_for_is_refused_by_name():
    async def out_of_turn():
        ctx = weida_nng.Context()
        server = weida_nng.RepSocket(ctx, options())
        await server.listen("tcp://127.0.0.1:0")

        with pytest.raises(weida_nng.ESTATE) as refused:
            await server.send(b"unasked")
        assert refused.value.errno == "ESTATE"
        assert isinstance(refused.value, weida_nng.NngError)

        await ctx.shutdown()

    run(out_of_turn())


def test_work_reaches_a_puller_and_a_push_without_one_times_out():
    async def pipeline():
        ctx = weida_nng.Context()
        puller = weida_nng.PullSocket(ctx, options())
        pusher = weida_nng.PushSocket(ctx, options())
        url = await puller.listen("tcp://127.0.0.1:0")
        await pusher.dial(url)

        await pusher.send(b"task")
        assert await puller.recv() == b"task"

        # A PUSH never discards: with no eligible peer it waits and then
        # says so.
        alone = weida_nng.PushSocket(ctx, options(send_timeout=0.2))
        with pytest.raises(weida_nng.ETIMEDOUT):
            await alone.send(b"nowhere")

        await ctx.shutdown()

    run(pipeline())


def test_a_subscriber_is_sent_only_what_it_asked_for():
    async def prefix_match():
        ctx = weida_nng.Context()
        publisher = weida_nng.PubSocket(ctx, options())
        subscriber = weida_nng.SubSocket(
            ctx, options(subscribe=b"weather.")
        )
        url = await publisher.listen("tcp://127.0.0.1:0")
        await subscriber.dial(url)
        while publisher.pipe_count == 0:
            await asyncio.sleep(0.005)

        assert subscriber.subscriptions == [b"weather."]

        published = await publisher.send(b"sports.result")
        assert published.queued == 1, "the filter is the subscriber's, not the publisher's"
        await publisher.send(b"weather.rain")

        assert await subscriber.recv() == b"weather.rain"
        assert subscriber.discarded == 1, "the unsubscribed publication was dropped here"

        await ctx.shutdown()

    run(prefix_match())


def test_a_publication_with_no_subscriber_reaches_nobody_and_succeeds():
    async def into_the_void():
        ctx = weida_nng.Context()
        publisher = weida_nng.PubSocket(ctx, options())
        await publisher.listen("tcp://127.0.0.1:0")

        published = await publisher.send(b"nobody is listening")
        assert (published.queued, published.dropped) == (0, 0)

        await ctx.shutdown()

    run(into_the_void())


def test_a_pair_talks_both_ways():
    async def pairing():
        ctx = weida_nng.Context()
        first = weida_nng.Pair1Socket(ctx, options())
        second = weida_nng.Pair1Socket(ctx, options())
        url = await first.listen("tcp://127.0.0.1:0")
        await second.dial(url)

        await second.send(b"hello")
        assert await first.recv() == b"hello"
        await first.send(b"hello back")
        assert await second.recv() == b"hello back"

        await ctx.shutdown()

    run(pairing())


def test_a_survey_reaches_every_respondent_and_the_deadline_ends_it():
    async def survey():
        ctx = weida_nng.Context()
        # The answers must come inside the survey; the window is generous so
        # a slow machine still makes it, and the last collect waits it out.
        surveyor = weida_nng.SurveyorSocket(ctx, options(survey_time=2.0))
        url = await surveyor.listen("tcp://127.0.0.1:0")
        respondents = [weida_nng.RespondentSocket(ctx, options()) for _ in range(2)]
        for respondent in respondents:
            await respondent.dial(url)
        while surveyor.pipe_count < 2:
            await asyncio.sleep(0.005)

        await surveyor.send(b"who is there")
        for respondent in respondents:
            assert await respondent.recv() == b"who is there"
            await respondent.send(b"here")

        assert await surveyor.recv() == b"here"
        assert await surveyor.recv() == b"here"
        # The deadline started at the send, so the third collect ends it.
        with pytest.raises(weida_nng.ETIMEDOUT):
            await surveyor.recv()

        await ctx.shutdown()

    run(survey())


def test_a_bus_message_reaches_every_directly_connected_peer():
    async def mesh():
        ctx = weida_nng.Context()
        hub = weida_nng.BusSocket(ctx, options())
        url = await hub.listen("tcp://127.0.0.1:0")
        spokes = [weida_nng.BusSocket(ctx, options()) for _ in range(2)]
        for spoke in spokes:
            await spoke.dial(url)
        while hub.pipe_count < 2:
            await asyncio.sleep(0.005)

        broadcast = await hub.send(b"to everyone")
        assert (broadcast.queued, broadcast.dropped) == (2, 0)
        for spoke in spokes:
            assert await spoke.recv() == b"to everyone"

        await ctx.shutdown()

    run(mesh())


def test_a_closed_socket_says_so_rather_than_hanging():
    async def closed():
        ctx = weida_nng.Context()
        puller = weida_nng.PullSocket(ctx, options())
        await puller.listen("tcp://127.0.0.1:0")
        puller.close()

        with pytest.raises(weida_nng.ECLOSED):
            await puller.recv()

        await ctx.shutdown()

    run(closed())


def test_a_cancelled_receive_leaves_the_socket_usable():
    async def cancelled():
        ctx = weida_nng.Context()
        puller = weida_nng.PullSocket(ctx, options())
        pusher = weida_nng.PushSocket(ctx, options())
        url = await puller.listen("tcp://127.0.0.1:0")
        await pusher.dial(url)

        waiting = asyncio.ensure_future(puller.recv())
        await asyncio.sleep(0.05)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        await pusher.send(b"after the cancel")
        assert await puller.recv() == b"after the cancel"

        await ctx.shutdown()

    run(cancelled())


def test_two_coroutines_may_use_one_socket_at_once():
    """SP has no thread rule, so a parked receive does not hold the socket."""

    async def both_at_once():
        ctx = weida_nng.Context()
        left = weida_nng.Pair0Socket(ctx, options())
        right = weida_nng.Pair0Socket(ctx, options())
        url = await left.listen("tcp://127.0.0.1:0")
        await right.dial(url)

        receiving = asyncio.ensure_future(left.recv())
        await asyncio.sleep(0.05)
        # The receive above is parked on `left`; this send uses that socket.
        await left.send(b"outbound")
        assert await right.recv() == b"outbound"

        await right.send(b"inbound")
        assert await receiving == b"inbound"

        await ctx.shutdown()

    run(both_at_once())


def test_a_wildcard_listen_reports_the_port_it_got():
    async def bound():
        ctx = weida_nng.Context()
        socket = weida_nng.PullSocket(ctx, options())
        url = await socket.listen("tcp://127.0.0.1:0")
        assert url.startswith("tcp://127.0.0.1:")
        assert url != "tcp://127.0.0.1:0"
        await ctx.shutdown()

    run(bound())


def test_a_message_taken_by_a_cancelled_receive_is_read_by_the_next_one():
    async def scenario():
        ctx = weida_nng.Context()
        pull = weida_nng.PullSocket(ctx, options())
        push = weida_nng.PushSocket(ctx, options())
        url = await pull.listen("tcp://127.0.0.1:0")
        await push.dial(url)

        task = asyncio.ensure_future(pull.recv())
        await asyncio.sleep(0.05)
        task.cancel()
        push.send_nowait(b"m1")
        time.sleep(0.1)
        await asyncio.sleep(0.05)
        assert await pull.recv() == b"m1"
        await ctx.shutdown()

    run(scenario())
