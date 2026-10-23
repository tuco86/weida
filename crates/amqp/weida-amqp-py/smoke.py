"""One AMQP 1.0 exchange, run against an installed wheel.

What `package.sh` runs inside a fresh virtualenv with no Rust toolchain on
`PATH`: a settled transfer to a scripted peer and the outcome it committed to,
plus the metadata a registry needs, read back from the installed distribution
rather than from the source tree.

The peer is the tests' own scripted one - the *extension* is what has to come
out of the wheel, not the script that talks to it.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests"))

import broker  # noqa: E402

import weida_amqp  # noqa: E402


async def exchange():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=False)
        await peer.grant_credit(1)
        payload = await peer.read_transfer()
        assert b"from the wheel" in payload, payload
        await peer.settle(broker.ACCEPTED)
        await peer.expect_close()

    async def accept(reader, writer):
        await script(broker.Peer(reader, writer))

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="smoke")
        session = await connection.begin()
        sender = await session.attach("orders", "sender", "/queues/orders")
        outcome = await broker.within("the outcome", sender.send(b"from the wheel"))
        assert outcome == "accepted", outcome
        await connection.close()


def metadata():
    from importlib.metadata import metadata as read

    installed = read("weida-amqp")
    for required in ["Name", "Version", "Summary", "Requires-Python"]:
        assert installed[required], f"the wheel carries no {required}"
    print(f"{installed['Name']} {installed['Version']} on {sys.implementation.name}")
    print(f"requires-python: {installed['Requires-Python']}")
    return installed["Version"]


def main():
    version = metadata()
    asyncio.run(exchange())
    # The surfaces the wheel is supposed to carry, not only the one exercised.
    for name in [
        "Connection",
        "Session",
        "Link",
        "Delivery",
        "Outcome",
        "AmqpError",
        "ResourceLimitExceeded",
        "CONDITIONS",
    ]:
        assert hasattr(weida_amqp, name), name
    from weida_amqp import sync

    assert hasattr(sync, "connect")
    assert weida_amqp.CONDITIONS["ResourceLimitExceeded"] == "amqp:resource-limit-exceeded"
    print(f"weida_amqp {version}: one accepted transfer, sync surface and conditions present")


if __name__ == "__main__":
    main()
