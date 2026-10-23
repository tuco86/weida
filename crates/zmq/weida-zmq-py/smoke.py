"""The asyncio round trip, run against an installed wheel.

What `package.sh` runs inside a fresh virtualenv with no Rust toolchain on
`PATH`: an asyncio REQ/REP exchange between two sockets
of this module — plus the metadata a registry needs, read back from the
installed distribution rather than from the source tree.
"""

import asyncio
import sys

import weida_zmq


async def round_trip():
    context = weida_zmq.Context()
    server = weida_zmq.RepSocket(context)
    client = weida_zmq.ReqSocket(context)

    endpoint = await server.bind("tcp://127.0.0.1:0")
    await client.connect(endpoint)

    await client.send(b"Hello")
    assert await server.recv() == [b"Hello"], "the request did not arrive"
    await server.send(b"World")
    assert await client.recv() == [b"World"], "the reply did not arrive"

    assert await context.shutdown() >= 0


def metadata():
    from importlib.metadata import metadata as read

    installed = read("weida-zmq")
    for required in ["Name", "Version", "Summary", "Requires-Python"]:
        assert installed[required], f"the wheel carries no {required}"
    print(f"{installed['Name']} {installed['Version']} on {sys.implementation.name}")
    print(f"requires-python: {installed['Requires-Python']}")
    return installed["Version"]


def main():
    version = metadata()
    asyncio.run(round_trip())
    # The surfaces the wheel is supposed to carry, not only the one exercised.
    for name in ["Context", "ReqSocket", "PairSocket", "Multipart", "ZmqError", "EAGAIN"]:
        assert hasattr(weida_zmq, name), name
    from weida_zmq import sync

    assert hasattr(sync, "ReqSocket")
    assert len(weida_zmq.OPTIONS) == 98
    print(f"weida_zmq {version}: round trip, sync surface and option table all present")


if __name__ == "__main__":
    main()
