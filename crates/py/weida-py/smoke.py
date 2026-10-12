"""What the release wheel must do, run from a virtualenv that holds only it.

Not a test suite: the suite is `tests/`, and it runs against an editable
build. This is the wheel's own proof - installed into a fresh virtualenv with
no `cargo`, `rustc` or `maturin` on `PATH` - and it checks the three claims a
wheel can be wrong about: that it imports at all, that the metadata is the
one `pyproject.toml` and the workspace version declare, and that a round trip
over QUIC works with both halves inside this one process.
"""

import asyncio
import importlib.metadata
import sys

import weida

CAP = 1 << 20


def metadata() -> None:
    """The wheel's own metadata, read from the installed distribution."""
    distribution = importlib.metadata.distribution("weida")
    assert distribution.metadata["Name"] == "weida", distribution.metadata["Name"]
    # PEP 440 spelling of the crate's `0.1.0-alpha.6-dev`, converted by maturin.
    assert distribution.version == "0.1.0a6.dev0", distribution.version
    print(f"weida {distribution.version} installed as a wheel")


def surface() -> None:
    """Every name the module promises is there."""
    for name in weida.__all__:
        assert hasattr(weida, name), name
    assert issubclass(weida.Untrusted, weida.WeidaError)
    # `weida.Runtime` is the runtime and `weida.RuntimeFailure` is the
    # failure: a wheel that lost the rename would shadow one with the other.
    assert not issubclass(weida.Runtime, BaseException)
    assert issubclass(weida.RuntimeFailure, weida.WeidaError)
    assert hasattr(weida.sync, "Runtime")
    print(f"{len(weida.__all__)} names, {weida.VERSION} the wire version")


async def round_trip() -> None:
    """A Req/Rep exchange and a Push/Pull transfer, over real QUIC."""
    server = weida.Runtime()
    binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
    replier = binding.replier("/echo")
    puller = binding.puller("/ingest")

    async def serve() -> None:
        request = await replier.accept(CAP)
        await request.reply(request.payload)

    answering = asyncio.create_task(serve())

    client = weida.Runtime()
    requester = client.requester(weida.Trust.by_address())
    await requester.connect(binding.url("/echo"))
    reply = await requester.request(b"through a wheel", CAP)
    assert reply == b"through a wheel", reply
    await answering
    print(f"req/rep over {binding.local_addr()}: {reply!r}")

    pusher = client.pusher(weida.Trust.by_address())
    await pusher.connect(binding.url("/ingest"))
    receiving = asyncio.create_task(puller.recv(CAP))
    await pusher.send(b"a sample")
    payload, meta = await receiving
    assert payload == b"a sample", payload
    print(f"push/pull to {meta.endpoint}: {payload!r}")

    # Fan-out, including the streamed form: a payload larger than a
    # subscriber's whole budget, which `publish` refuses and `open` carries.
    publisher = binding.publisher("/md")
    subscriber = client.subscriber(weida.Trust.by_address())
    await subscriber.connect(binding.url("/md"))
    await subscriber.subscribe("px.#")
    while publisher.filter_count() != 1:
        await asyncio.sleep(0.005)
    assert publisher.publish("px.eur", b"1.0812") == 1
    payload, meta = await subscriber.recv(CAP)
    assert (payload, meta.topic) == (b"1.0812", "px.eur"), (payload, meta.topic)
    print(f"pub/sub on {meta.topic}: {payload!r}")

    chunk = b"a" * (64 * 1024)
    chunks = 160  # 10 MiB against an 8 MiB subscriber budget
    reading = asyncio.create_task(subscriber.recv(16 * CAP))
    fan = publisher.open("px.eur")
    for _ in range(chunks):
        assert await fan.write_within(chunk, 15.0) == 1
    assert await fan.finish() == 1
    streamed, _ = await reading
    assert len(streamed) == len(chunk) * chunks, len(streamed)
    print(f"streamed fan-out: {len(streamed)} bytes, more than the budget")

    # The address names the key, so a wrong key is refused rather than
    # trusted: the one security claim a wheel could silently lose.
    wrong = "weida://sha256:%s@%s/echo" % ("0" * 64, binding.local_addr())
    try:
        await client.requester(weida.Trust.by_address()).connect(wrong)
    except weida.Untrusted as refused:
        assert binding.fingerprint() in refused.cause
        print("a wrong fingerprint is refused, naming the key that answered")
    else:
        raise AssertionError("an untrusted key must not connect")


def main() -> int:
    metadata()
    surface()
    asyncio.run(asyncio.wait_for(round_trip(), 30.0))
    print("the wheel works")
    return 0


if __name__ == "__main__":
    sys.exit(main())
