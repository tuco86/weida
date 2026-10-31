"""What the wheel must do, run from a fresh virtualenv that holds only it.

Not a test suite (that is `tests/`, against an editable install): the
wheel's own proof, with no cargo or compiler on PATH, of the three things a
wheel can get wrong. It imports, its metadata carries the workspace version,
and a round trip works: synchronous over QUIC with a pinned key, and asyncio
over the in-process transport.
"""

import asyncio
import importlib.metadata
import os
import threading

import weida
import weida.asyncio as wa

WAIT = 10.0


def pep440(version: str) -> str:
    """PEP 440 spelling of a Cargo version, as `hatch_build.py` writes it."""
    dev = version.endswith("-dev")
    if dev:
        version = version[: -len("-dev")]
    base, _, pre = version.partition("-")
    if pre:
        kind, number = pre.split(".")
        base += {"alpha": "a", "beta": "b", "rc": "rc"}[kind] + number
    return base + ".dev0" if dev else base


def metadata() -> None:
    distribution = importlib.metadata.distribution("weida")
    assert distribution.metadata["Name"] == "weida", distribution.metadata["Name"]
    expected = pep440(os.environ["WEIDA_WORKSPACE_VERSION"])
    assert distribution.version == expected, (distribution.version, expected)
    assert weida.version() == os.environ["WEIDA_WORKSPACE_VERSION"], weida.version()
    print(f"weida {distribution.version} installed as a wheel, libweida {weida.version()}")


def sync_quic() -> None:
    identity = weida.Identity.generate()
    fingerprint = identity.fingerprint()
    runtime = weida.Runtime.owned()
    listener = runtime.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(identity), WAIT)
    replier = listener.replier("/echo")

    def serve() -> None:
        request = replier.accept(WAIT)
        body = request.take_body()
        received = b""
        while chunk := body.read(4, WAIT):
            received += chunk
        reply = request.reply(weida.TransferMeta("text/plain"), WAIT)
        reply.write_all(received[::-1], WAIT)
        reply.finish().delivered(WAIT)

    server = threading.Thread(target=serve)
    server.start()
    requester = runtime.requester(weida.ClientTls(weida.Trust().add_pin(fingerprint)))
    requester.connect(f"weida://{fingerprint}@{binding.local_addr()}/echo", WAIT)
    reply = requester.request(b"streamed over quic", WAIT)
    assert reply.meta().content_type == "text/plain"
    message = reply.collect(1024, WAIT)
    assert message.payload == b"ciuq revo demaerts", message.payload
    server.join(WAIT)
    for handle in (requester, replier, binding, listener):
        handle.drop()
    runtime.shutdown(WAIT)
    print("sync round trip over QUIC with a pinned key: ok")


async def asyncio_inproc() -> None:
    runtime = wa.Runtime.owned()
    listener = runtime.listener()
    binding = listener.bind_inproc("smoke")
    replier = listener.replier("/echo")
    requester = runtime.requester(weida.ClientTls(weida.Trust()))
    await requester.connect("weida+inproc://smoke/echo")

    async def serve() -> None:
        request = await replier.accept()
        body = await request.take_body().collect(1024)
        reply = await request.reply()
        await reply.write_all(body.payload.upper())
        await reply.finish().delivered()

    server = asyncio.ensure_future(serve())
    reply = await requester.request(b"in process")
    assert (await reply.collect(1024)).payload == b"IN PROCESS"
    await server
    for handle in (requester, replier, binding, listener):
        handle.drop()
    await runtime.shutdown()
    print("asyncio round trip in process: ok")


metadata()
sync_quic()
asyncio.run(asyncio_inproc())
