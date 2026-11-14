"""A binding's close, hand-over and disconnect over real QUIC on localhost."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida
import weida.asyncio as wa

WAIT = 5.0


def echo(requester, replier, body: bytes) -> bytes:
    """One request answered with its own body, served in a thread: the
    reply's payload."""

    def serve() -> None:
        request = replier.accept(WAIT)
        message = request.take_body().collect(1024, WAIT)
        out = request.reply(None, WAIT)
        out.write_all(message.payload, WAIT)
        out.finish().delivered(WAIT)

    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(serve)
        answer = requester.request(body, WAIT).collect(1024, WAIT).payload
        served.result(WAIT)
    return answer


async def echo_on_loop(requester, replier, body: bytes) -> bytes:
    """`echo` on the running loop."""

    async def serve() -> None:
        request = await replier.accept()
        message = await request.take_body().collect(1024)
        out = await request.reply()
        await out.write_all(message.payload)
        await out.finish().delivered()

    served = asyncio.ensure_future(serve())
    answer = (await (await requester.request(body)).collect(1024)).payload
    await served
    return answer


def test_a_closed_binding_refuses_the_next_dial(server_identity):
    server = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(server_identity), WAIT)
    replier = listener.replier("/echo")
    url = f"weida://{server_identity.fingerprint()}@{binding.local_addr()}/echo"
    client = weida.Runtime.owned()
    requester = client.requester(weida.ClientTls(weida.Trust()))
    requester.connect(url, WAIT)
    assert echo(requester, replier, b"ping") == b"ping"
    events = requester.events()

    binding.close(WAIT)
    lost = events.recv(WAIT)
    assert lost.kind is weida.PeerEventKind.LOST
    assert lost.cause is weida.LossCause.PEER_CLOSED

    # Nobody answers on the closed port; the handshake times out.
    late = weida.Runtime.owned(weida.RuntimeConfig(limits=weida.Limits(idle_timeout=1)))
    late_requester = late.requester(weida.ClientTls(weida.Trust()))
    with pytest.raises(weida.ConnectionLost) as caught:
        late_requester.connect(url, WAIT)
    assert caught.value.cause == "idle_timeout"
    with pytest.raises(weida.Closed):
        binding.hand_over(WAIT)

    for handle in (events, late_requester, requester, replier, listener):
        handle.drop()
    late.shutdown(WAIT)
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_hand_over_keeps_a_requesters_connection(server_identity):
    async def main() -> None:
        old = wa.Runtime.owned()
        old_listener = old.listener()
        binding = await old_listener.bind_quic("127.0.0.1:0", weida.ServerTls(server_identity))
        addr = binding.local_addr()
        old_replier = old_listener.replier("/echo")
        client = wa.Runtime.owned()
        requester = client.requester(weida.ClientTls(weida.Trust()))
        await requester.connect(f"weida://{server_identity.fingerprint()}@{addr}/echo")
        assert await echo_on_loop(requester, old_replier, b"before") == b"before"
        events = requester.events()
        old_replier.drop()

        handoff = await binding.hand_over()
        assert isinstance(handoff, weida.Handoff)
        assert handoff.held() >= 1, "the client's close is held"
        assert handoff.local_addr() == addr
        fd = handoff.take_socket()
        with pytest.raises(weida.Closed):
            handoff.take_socket()
        held = handoff.bytes()
        assert isinstance(held, bytes) and held
        handoff.drop()

        successor = wa.Runtime.owned()
        listener = successor.listener()
        new_replier = listener.replier("/echo")
        new_binding = await listener.bind_quic_socket(fd, weida.ServerTls(server_identity))
        assert new_binding.local_addr() == addr
        assert await new_binding.take_over(held) >= 1

        lost = await events.recv()
        assert lost.kind is weida.PeerEventKind.LOST
        assert lost.cause is weida.LossCause.PEER_RESTARTING
        event = await events.recv()
        while event.kind is not weida.PeerEventKind.CONNECTED:
            event = await events.recv()
        # The requester kept its address: no second connect.
        assert await echo_on_loop(requester, new_replier, b"after") == b"after"

        for handle in (events, requester, new_replier, new_binding, listener, old_listener):
            handle.drop()
        await successor.shutdown()
        await client.shutdown()
        # The predecessor's runtime lives to the end, as a predecessor process would.
        await old.shutdown()

    asyncio.run(main())


def test_disconnect_closes_a_peers_connections_by_its_key():
    server_id = weida.Identity.generate()
    client_id = weida.Identity.generate()
    server = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(server_id, weida.ANY_KEY), WAIT)
    replier = listener.replier("/echo")
    client = weida.Runtime.owned()
    requester = client.requester(weida.ClientTls(weida.Trust(), client_id))
    requester.connect(f"weida://{server_id.fingerprint()}@{binding.local_addr()}/echo", WAIT)
    assert echo(requester, replier, b"ping") == b"ping"
    events = requester.events()

    assert binding.disconnect(weida.Identity.generate().fingerprint()) == 0
    with pytest.raises(weida.InvalidFingerprint):
        binding.disconnect("nope")
    assert binding.disconnect(client_id.fingerprint()) == 1
    lost = events.recv(WAIT)
    assert lost.kind is weida.PeerEventKind.LOST
    assert lost.cause is weida.LossCause.PEER_CLOSED

    for handle in (events, requester, replier, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)
