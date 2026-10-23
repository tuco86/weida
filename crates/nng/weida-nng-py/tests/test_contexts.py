"""`nng_ctx` from Python: several transactions over one socket, at once.

"Several requests can be processed in parallel over one socket" is what a
context is for. The claim these tests make is the one that matters to a
caller: two coroutines that each hold their own context overlap, and each
reply finds the context that asked for it — which is not true of the socket's
implicit context, where the second request would have to wait.
"""

import asyncio

import pytest

import waits
import weida_nng


def run(coroutine):
    return waits.run(coroutine)


def options(**kwargs):
    # NNG's own default: wait forever; the CI job's budget is the hang guard.
    return weida_nng.SocketOptions(**kwargs)


def test_two_contexts_have_two_requests_outstanding_at_once():
    async def overlapping():
        ctx = weida_nng.Context()
        server = weida_nng.RepSocket(ctx, options())
        client = weida_nng.ReqSocket(ctx, options())
        url = await server.listen("tcp://127.0.0.1:0")
        await client.dial(url)

        first = client.context()
        second = client.context()
        assert first.id != second.id

        # Both requests go out before either is answered, which is the whole
        # point: with one context the second send would wait.
        await first.send(b"first")
        await second.send(b"second")

        # The replier answers them in the order they arrive, on two contexts
        # of its own, so the answers may come back in either order.
        answered = {}
        for _ in range(2):
            transaction = server.context()
            request = await transaction.recv()
            answered[bytes(request)] = transaction

        for body, transaction in answered.items():
            await transaction.send(b"re:" + body)

        assert await first.recv() == b"re:first"
        assert await second.recv() == b"re:second"

        await ctx.shutdown()

    run(overlapping())


def test_a_context_that_answers_out_of_turn_is_refused_and_its_neighbour_is_not():
    async def per_context_state():
        ctx = weida_nng.Context()
        server = weida_nng.RepSocket(ctx, options())
        client = weida_nng.ReqSocket(ctx, options())
        url = await server.listen("tcp://127.0.0.1:0")
        await client.dial(url)

        idle = server.context()
        with pytest.raises(weida_nng.ESTATE):
            await idle.send(b"nobody asked")

        # The refusal was this context's, not the socket's: another context
        # on the same socket still works.
        working = server.context()
        asking = client.context()
        await asking.send(b"ping")
        assert await working.recv() == b"ping"
        await working.send(b"pong")
        assert await asking.recv() == b"pong"

        await ctx.shutdown()

    run(per_context_state())


def test_a_survey_context_carries_its_own_deadline():
    async def two_deadlines():
        ctx = weida_nng.Context()
        surveyor = weida_nng.SurveyorSocket(ctx, options(survey_time=2.0))
        url = await surveyor.listen("tcp://127.0.0.1:0")
        respondent = weida_nng.RespondentSocket(ctx, options())
        await respondent.dial(url)
        while surveyor.pipe_count == 0:
            await asyncio.sleep(0.005)

        early = surveyor.context()
        await early.send(b"early")
        assert await respondent.recv() == b"early"
        await respondent.send(b"here")
        assert await early.recv() == b"here"

        # A second survey is a second deadline, started now rather than when
        # the first one was sent.
        late = surveyor.context()
        await late.send(b"late")
        assert await respondent.recv() == b"late"
        with pytest.raises(weida_nng.ETIMEDOUT):
            await late.recv()

        await ctx.shutdown()

    run(two_deadlines())


def test_a_respondent_answers_on_the_context_that_received():
    async def respond():
        ctx = weida_nng.Context()
        surveyor = weida_nng.SurveyorSocket(ctx, options(survey_time=2.0))
        url = await surveyor.listen("tcp://127.0.0.1:0")
        respondent = weida_nng.RespondentSocket(ctx, options())
        await respondent.dial(url)
        while surveyor.pipe_count == 0:
            await asyncio.sleep(0.005)

        await surveyor.send(b"question")
        answering = respondent.context()
        assert await answering.recv() == b"question"
        await answering.send(b"answer")
        assert await surveyor.recv() == b"answer"

        await ctx.shutdown()

    run(respond())
