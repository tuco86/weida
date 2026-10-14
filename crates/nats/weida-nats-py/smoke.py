"""One publish/subscribe round trip and one request, against an installed wheel.

What `package.sh` runs inside a fresh virtualenv with no Rust toolchain on
`PATH`: a subscription that receives its publication and a request that gets
its reply, plus the metadata a registry needs, read back from the installed
distribution rather than from the source tree.

The peer is the tests' own scripted server and **not** `nats-server`: the
binary is absent on most machines (`tests/test_interop.py` skips without
it), and what this script has to prove is that the *extension in the wheel*
works, not that a server accepts it. Routing, the 503 and the reply subject
all come from `tests/scripted.py`, which is a server and nothing else.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests"))

import scripted  # noqa: E402

import weida_nats  # noqa: E402

DEADLINE = 10.0


async def round_trip(server):
    nats = await weida_nats.connect("127.0.0.1", server.port)
    orders = await nats.subscribe("orders.*")

    await nats.publish("orders.created", b"from the wheel")
    message = await asyncio.wait_for(orders.next(), DEADLINE)
    assert message is not None, "the subscription received nothing"
    # A subject is bytes on the wire and bytes here: the protocol calls it a
    # name and never a string, and this binding does not guess an encoding.
    assert message.subject == b"orders.created", message.subject
    assert message.payload == b"from the wheel", message.payload

    # A request is the inbox round trip: a reply subject on the wire, a
    # subscription under `_INBOX.`, and an answer routed back to it.
    server.respond_to("service.echo", b"answered")
    reply = await asyncio.wait_for(nats.request("service.echo", b"ask", timeout=DEADLINE), DEADLINE)
    assert reply.payload == b"answered", reply.payload

    # And the no-responder status is its own class rather than a timeout.
    try:
        await nats.request("nobody.here", b"ask", timeout=DEADLINE)
    except weida_nats.NoResponders:
        pass
    else:
        raise AssertionError("a request with no responder should raise NoResponders")

    await nats.close()


def metadata():
    from importlib.metadata import metadata as read

    installed = read("weida-nats")
    for required in ["Name", "Version", "Summary", "Requires-Python"]:
        assert installed[required], f"the wheel carries no {required}"
    print(f"{installed['Name']} {installed['Version']} on {sys.implementation.name}")
    print(f"requires-python: {installed['Requires-Python']}")
    return installed["Version"]


def main():
    version = metadata()
    server = scripted.Server.start()
    try:
        asyncio.run(round_trip(server))
    finally:
        server.stop()

    # The surfaces the wheel is supposed to carry, not only the one exercised.
    for name in [
        "Connection",
        "Subscription",
        "Message",
        "RemoteInfo",
        "State",
        "NatsError",
        "NoResponders",
        "RequestTimeout",
        "INBOX_PREFIX",
        "DEFAULT_PORT",
    ]:
        assert hasattr(weida_nats, name), name
    from weida_nats import sync

    assert hasattr(sync, "connect")
    assert weida_nats.INBOX_PREFIX == "_INBOX."
    assert weida_nats.NO_RESPONDERS == 503
    print(f"weida_nats {version}: pub/sub, request-reply, the 503 and the sync surface present")


if __name__ == "__main__":
    main()
