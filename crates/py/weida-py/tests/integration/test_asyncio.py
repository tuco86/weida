"""The asyncio surface: every wait on a readiness descriptor of the loop."""

import asyncio

import pytest

import weida
import weida.asyncio as wa


async def quic_pair(identity: weida.Identity):
    runtime = wa.Runtime.owned()
    listener = runtime.listener()
    binding = await listener.bind_quic("127.0.0.1:0", weida.ServerTls(identity))
    replier = listener.replier("/echo")
    fingerprint = identity.fingerprint()
    requester = runtime.requester(weida.ClientTls(weida.Trust().add_pin(fingerprint)))
    await requester.connect(f"weida://{fingerprint}@{binding.local_addr()}/echo")
    return runtime, listener, binding, replier, requester


async def answer(replier: wa.Replier) -> bytes:
    request = await replier.accept()
    body = await request.take_body().collect(1024)
    reply = await request.reply(weida.TransferMeta("text/plain"))
    await reply.write_all(body.payload[::-1])
    await reply.finish().delivered()
    return body.payload


def test_a_round_trip_over_quic(server_identity):
    async def main() -> None:
        runtime, listener, binding, replier, requester = await quic_pair(server_identity)
        served = asyncio.ensure_future(answer(replier))
        reply = await requester.request(b"ping")
        assert reply.meta().content_type == "text/plain"
        assert (await reply.collect(1024)).payload == b"gnip"
        assert await served == b"ping"
        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())


def test_two_accepts_on_one_replier_serve_two_requests(server_identity):
    async def main() -> None:
        runtime, listener, binding, replier, requester = await quic_pair(server_identity)
        accepts = [asyncio.ensure_future(answer(replier)) for _ in range(2)]
        replies = await asyncio.gather(requester.request(b"one"), requester.request(b"two"))
        payloads = sorted([(await reply.collect(1024)).payload for reply in replies])
        assert payloads == [b"eno", b"owt"]
        assert sorted(await asyncio.gather(*accepts)) == [b"one", b"two"]
        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())


def test_a_cancelled_request_ends_and_the_runtime_still_shuts_down(bus):
    async def main() -> None:
        runtime = wa.Runtime.owned()
        listener = runtime.listener()
        binding = listener.bind_inproc(bus)
        replier = listener.replier("/never")
        requester = runtime.requester(weida.ClientTls(weida.Trust()))
        await requester.connect(f"weida+inproc://{bus}/never")
        task = asyncio.ensure_future(requester.request(b"unanswered"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())


def test_request_with_carries_metadata_on_the_loop(bus):
    async def main() -> None:
        runtime = wa.Runtime.owned()
        listener = runtime.listener()
        binding = listener.bind_inproc(bus)
        replier = listener.replier("/echo")
        requester = runtime.requester(weida.ClientTls(weida.Trust()))
        await requester.connect(f"weida+inproc://{bus}/echo")
        t = weida.new_trace()

        async def serve() -> None:
            request = await replier.accept()
            assert request.meta().trace == t
            out = await request.reply(weida.TransferMeta(achieved=weida.Acknowledgement.ACCEPTED))
            await out.write_all(b"pong")
            out.finish()

        served = asyncio.ensure_future(serve())
        reply = await requester.request_with(weida.TransferMeta(trace=t), b"ping")
        assert reply.meta().trace == t
        assert reply.meta().achieved is weida.Acknowledgement.ACCEPTED
        assert (await reply.collect(64)).payload == b"pong"
        await served
        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())


def test_canceled_starts_at_the_call_and_returns_once_the_requester_gives_up(bus):
    async def main() -> None:
        runtime = wa.Runtime.owned()
        listener = runtime.listener()
        binding = listener.bind_inproc(bus)
        replier = listener.replier("/echo")
        requester = runtime.requester(weida.ClientTls(weida.Trust()))
        await requester.connect(f"weida+inproc://{bus}/echo")

        # The requester gives up: cancelling its request drops its op.
        pending = asyncio.ensure_future(requester.request(b"wait"))
        request = await replier.accept()
        watching = asyncio.ensure_future(request.canceled())
        done, _ = await asyncio.wait({watching}, timeout=0.05)
        assert not done, "the requester still waits"
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await watching
        request.drop()

        # The watch starts at the call, so it outlives the request the
        # refusal consumes; the finished reply half ends it.
        asking = asyncio.ensure_future(requester.request(b"ask"))
        request = await replier.accept()
        watch = request.canceled()
        await request.refuse(weida.ErrorCode.REJECTED)
        with pytest.raises(weida.Rejected):
            await asking
        await watch

        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())


def test_an_opened_exchange_and_a_dropped_reply_stream_on_the_loop(bus):
    async def main() -> None:
        runtime = wa.Runtime.owned()
        listener = runtime.listener()
        binding = listener.bind_inproc(bus)
        replier = listener.replier("/echo")
        requester = runtime.requester(weida.ClientTls(weida.Trust()))
        await requester.connect(f"weida+inproc://{bus}/echo")

        # The reply is read while the request body is still open.
        body, reply = await requester.open(weida.TransferMeta("text/plain"))
        await body.write_all(b"onetwothree")
        request = await replier.accept()
        assert request.meta().content_type == "text/plain"
        received = request.take_body()
        answer = await request.reply()
        await answer.write_all(b"ack")
        answer.finish()
        assert (await (await reply.recv()).collect(64)).payload == b"ack"
        body.finish()
        assert (await received.collect(64)).payload == b"onetwothree"

        # Dropping the reply stream ends the replier's watch.
        body, reply = await requester.open()
        request = await replier.accept()
        watching = asyncio.ensure_future(request.canceled())
        done, _ = await asyncio.wait({watching}, timeout=0.05)
        assert not done, "the requester still listens"
        reply.drop()
        await watching
        request.drop()
        body.drop()

        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())


def test_peer_events_iterate_with_async_for(bus):
    async def main() -> None:
        url = f"weida+inproc://{bus}/echo"
        server = wa.Runtime.owned()
        listener = server.listener()
        binding = listener.bind_inproc(bus)
        replier = listener.replier("/echo")
        client = wa.Runtime.owned()
        requester = client.requester(weida.ClientTls(weida.Trust()))
        await requester.connect(url)
        events = requester.events()
        assert isinstance(events, wa.PeerEvents)
        for handle in (replier, binding, listener):
            handle.drop()
        await server.shutdown()
        kinds = []
        async for event in events:
            kinds.append(event.kind)
            assert event.url == url
            if event.kind is weida.PeerEventKind.RETRYING:
                requester.drop()
        # The loop ends because the dropped requester ends the queue.
        assert kinds[:2] == [weida.PeerEventKind.LOST, weida.PeerEventKind.RETRYING]
        events.drop()
        await client.shutdown()

    asyncio.run(main())
