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
