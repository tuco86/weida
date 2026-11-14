"""Serving through a relay over real QUIC on localhost: a relay, a dial-only
replier and a requester, each in a runtime of its own, on the asyncio
surface."""

import asyncio
import time
from types import SimpleNamespace

import pytest

import weida
import weida.asyncio as wa

BULK = 256 * 1024


async def start_relay() -> SimpleNamespace:
    """A relay runtime on localhost: its binding, its relay and the URL a
    hidden peer registers at. A registration is addressed by its key, so the
    relay asks every peer for one."""
    relay_id = weida.Identity.generate()
    runtime = wa.Runtime.owned()
    listener = runtime.listener()
    binding = await listener.bind_quic("127.0.0.1:0", weida.ServerTls(relay_id, weida.ANY_KEY))
    relay = listener.relay()
    url = f"weida://{relay_id.fingerprint()}@{binding.local_addr()}/.weida/relay"
    return SimpleNamespace(
        relay_id=relay_id, runtime=runtime, listener=listener, binding=binding, relay=relay, url=url
    )


async def admit(relay: wa.Relay, count: int, asked: list, budget=None) -> None:
    """Serves exactly `count` asks, each recorded in `asked` as
    `(peer, target)`; with `budget`, a tunnel is admitted with it and a
    registration with 0. The tests cause exactly that many asks, so no
    accept is left waiting when the relay is dropped."""
    for _ in range(count):
        ask = await relay.accept()
        asked.append((ask.peer_fingerprint(), ask.target()))
        if budget is None:
            await ask.admit()
        else:
            await ask.admit_with(0 if ask.target() is None else budget)


async def hidden_peer(r):
    """A dial-only peer registered at the relay under a key of its own."""
    hidden_id = weida.Identity.generate()
    hidden = wa.Runtime.owned()
    hidden_listener = hidden.listener()
    hidden_binding = await hidden_listener.bind_relay(
        r.url,
        weida.ClientTls(weida.Trust(), hidden_id),
        weida.ServerTls(hidden_id, weida.ANY_KEY),
    )
    return hidden_id, hidden, hidden_listener, hidden_binding


async def dial(r, hidden_id, path):
    """A requester with a key of its own, connected to the hidden peer's
    `path` through the relay."""
    dialler_id = weida.Identity.generate()
    dialler = wa.Runtime.owned()
    requester = dialler.requester(weida.ClientTls(weida.Trust(), dialler_id))
    await requester.connect(
        f"weida+relay://{hidden_id.fingerprint()}@{r.relay_id.fingerprint()}@{r.binding.local_addr()}{path}"
    )
    return dialler_id, dialler, requester


def test_a_dial_only_replier_reached_through_a_relay_answers_a_requester():
    async def main() -> None:
        r = await start_relay()
        with pytest.raises(weida.AlreadyRegistered):
            r.listener.relay()
        assert r.binding.relay_events() is None

        asked = []
        serving = asyncio.ensure_future(admit(r.relay, 2, asked))
        hidden_id, hidden, hidden_listener, hidden_binding = await hidden_peer(r)
        replier = hidden_listener.replier("/echo")
        dialler_id, dialler, requester = await dial(r, hidden_id, "/echo")

        async def serve() -> str:
            request = await replier.accept()
            peer = request.meta().peer_fingerprint
            request.take_body().drop()
            out = await request.reply()
            await out.write_all(b"pong")
            await out.finish().delivered()
            return peer

        served = asyncio.ensure_future(serve())
        reply = await requester.request(b"ping")
        assert (await reply.collect(1024)).payload == b"pong"
        assert await served == dialler_id.fingerprint(), "the replier sees the dialler's key, not the relay's"
        await serving
        assert asked == [
            (hidden_id.fingerprint(), None),
            (dialler_id.fingerprint(), hidden_id.fingerprint()),
        ]

        events = hidden_binding.relay_events()
        assert isinstance(events, wa.PeerEvents)
        r.relay.drop()
        lost = await events.recv()
        assert lost.kind is weida.PeerEventKind.LOST
        assert lost.cause is weida.LossCause.PEER_CLOSED

        for handle in (events, requester, replier, hidden_binding, hidden_listener):
            handle.drop()
        r.binding.drop()
        r.listener.drop()
        await dialler.shutdown()
        await hidden.shutdown()
        await r.runtime.shutdown()

    asyncio.run(main())


def test_a_registration_the_relay_refuses_raises_rejected_from_bind_relay():
    async def main() -> None:
        r = await start_relay()
        hidden_id = weida.Identity.generate()
        hidden = wa.Runtime.owned()
        hidden_listener = hidden.listener()
        via = weida.ClientTls(weida.Trust(), hidden_id)
        tls = weida.ServerTls(hidden_id)

        async def refuse_one() -> None:
            ask = await r.relay.accept()
            await ask.refuse()

        refusing = asyncio.ensure_future(refuse_one())
        with pytest.raises(weida.Rejected):
            await hidden_listener.bind_relay(r.url, via, tls)
        await refusing

        for handle in (via, tls, hidden_listener, r.relay, r.binding, r.listener):
            handle.drop()
        await hidden.shutdown()
        await r.runtime.shutdown()

    asyncio.run(main())


# QUIC backs off at the budget, so this run is longer than the others here.
@pytest.mark.timeout(30)
def test_a_tunnel_admitted_with_a_budget_moves_no_faster_than_it():
    async def main() -> None:
        r = await start_relay()
        serving = asyncio.ensure_future(admit(r.relay, 2, [], budget=BULK))
        hidden_id, hidden, hidden_listener, hidden_binding = await hidden_peer(r)
        replier = hidden_listener.replier("/bulk")
        dialler_id, dialler, requester = await dial(r, hidden_id, "/bulk")

        async def serve() -> int:
            request = await replier.accept()
            payload = (await request.take_body().collect(4 * BULK)).payload
            out = await request.reply()
            await out.write_all(b"ok")
            await out.finish().delivered()
            return len(payload)

        served = asyncio.ensure_future(serve())
        started = time.monotonic()
        reply = await requester.request(bytes(3 * BULK))
        assert (await reply.collect(1024)).payload == b"ok"
        elapsed = time.monotonic() - started
        assert await served == 3 * BULK
        assert elapsed >= 1.5, f"3 x {BULK} bytes at {BULK} B/s took {elapsed:.2f} s"

        await serving
        r.relay.drop()
        for handle in (requester, replier, hidden_binding, hidden_listener):
            handle.drop()
        r.binding.drop()
        r.listener.drop()
        await dialler.shutdown()
        await hidden.shutdown()
        await r.runtime.shutdown()

    asyncio.run(main())
