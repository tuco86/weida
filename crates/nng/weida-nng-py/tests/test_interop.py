"""Interop: this binding against the NNG C library, in both roles.

The peer is `pynng`, which bundles and builds the NNG C library itself, so
what these tests exercise is the reference implementation on the wire and not
a second copy of this codec agreeing with itself. The version is asserted
below rather than assumed, and it is a *different* build from the one the
Rust interop suite measures (`crates/nng/weida-nng/tests/interop_nng.rs`,
NNG 1.4.0-rc.0 through the `nng` crate) — so between them the library is
measured against two releases of the C implementation.

`pynng` is not a dependency of this package. Where it is absent every test
here is skipped, with the install command in the skip message — the same
place LOOP.md 2 puts one.

Every `pynng` call is blocking, so each one runs in a worker thread through
`asyncio.to_thread`; a blocking C call on the event-loop thread would deadlock
against the coroutine it is waiting for.
"""

import asyncio

import pytest

import waits
import weida_nng

pynng = pytest.importorskip(
    "pynng",
    reason="pynng is absent; install it into the repository venv with: "
    "uv pip install --python .venv pynng",
)


def run(coroutine):
    return waits.run(coroutine)


def options(**kwargs):
    # No timeouts here or on the pynng side: NNG's default is to wait, and
    # the CI job's budget is the hang guard.
    return weida_nng.SocketOptions(**kwargs)


def free_url():
    """A loopback address nobody is listening on, released before it is
    dialled — pynng's listeners take a concrete port."""
    import socket as stdlib_socket

    with stdlib_socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return f"tcp://127.0.0.1:{probe.getsockname()[1]}"


def test_the_c_library_is_the_one_this_test_claims():
    version = pynng.ffi.string(pynng.lib.nng_version()).decode()
    assert version.startswith("1."), f"an NNG 1.x C library, not {version}"


def test_a_request_from_the_c_library_is_answered_by_this_binding():
    """This binding bound, NNG dialling."""

    async def exchange():
        ctx = weida_nng.Context()
        server = weida_nng.RepSocket(ctx, options())
        url = await server.listen("tcp://127.0.0.1:0")

        with pynng.Req0(dial=url) as peer:
            await asyncio.to_thread(peer.send, b"from C")
            assert await server.recv() == b"from C"
            await server.send(b"from Rust")
            assert await asyncio.to_thread(peer.recv) == b"from Rust"

        await ctx.shutdown()

    run(exchange())


def test_a_request_from_this_binding_is_answered_by_the_c_library():
    """NNG bound, this binding dialling."""

    async def exchange():
        ctx = weida_nng.Context()
        url = free_url()
        with pynng.Rep0(listen=url) as peer:
            client = weida_nng.ReqSocket(ctx, options())
            await client.dial(url)

            answering = asyncio.create_task(
                asyncio.to_thread(lambda: peer.send(b"re:" + peer.recv()))
            )
            await client.send(b"ping")
            assert await client.recv() == b"re:ping"
            await answering

        await ctx.shutdown()

    run(exchange())


def test_a_publication_from_the_c_library_is_filtered_by_this_subscriber():
    async def prefix_match():
        ctx = weida_nng.Context()
        url = free_url()
        with pynng.Pub0(listen=url) as peer:
            subscriber = weida_nng.SubSocket(
                ctx, options(subscribe=b"weather.")
            )
            await subscriber.dial(url)
            # Give the C library's accept loop a moment: a publication sent
            # before the pipe exists reaches nobody, here as there.
            await asyncio.sleep(0.2)

            await asyncio.to_thread(peer.send, b"sports.result")
            await asyncio.to_thread(peer.send, b"weather.rain")

            assert await subscriber.recv() == b"weather.rain"
            assert subscriber.discarded == 1

        await ctx.shutdown()

    run(prefix_match())


def test_work_pushed_by_this_binding_is_pulled_by_the_c_library():
    async def pipeline():
        ctx = weida_nng.Context()
        url = free_url()
        with pynng.Pull0(listen=url) as peer:
            pusher = weida_nng.PushSocket(ctx, options())
            await pusher.dial(url)
            await pusher.send(b"task")
            assert await asyncio.to_thread(peer.recv) == b"task"

        await ctx.shutdown()

    run(pipeline())


def test_a_survey_from_the_c_library_is_answered_by_this_respondent():
    async def survey():
        ctx = weida_nng.Context()
        url = free_url()
        # No pynng timeouts: a blocking call waits as long as the peer needs.
        # The survey window is wide for the same reason; the answer ends the
        # wait early, so a wide window costs nothing.
        with pynng.Surveyor0(
            listen=url, survey_time=30000
        ) as peer:
            respondent = weida_nng.RespondentSocket(ctx, options())
            await respondent.dial(url)
            await asyncio.sleep(0.2)

            await asyncio.to_thread(peer.send, b"who is there")
            assert await respondent.recv() == b"who is there"
            await respondent.send(b"here")
            assert await asyncio.to_thread(peer.recv) == b"here"

        await ctx.shutdown()

    run(survey())
