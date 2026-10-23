"""One REQ/REP round trip on each surface, against an installed wheel.

What `package.sh` runs inside a fresh virtualenv with no Rust toolchain on
`PATH`: a request that reaches a replier and its reply that comes back, once
under asyncio and once through `weida_nng.sync` with no event loop in the
process, plus the metadata a registry needs, read back from the installed
distribution rather than from the source tree.

Both ends are this module. No `pynng` and no NNG C library are installed in
the virtualenv, because what this script has to prove is that the *extension
in the wheel* works, not that a foreign peer accepts it - the interop suite
under `tests/` is where that is measured.
"""

import asyncio
import sys

import weida_nng


def options():
    # No timeouts: NNG waits, and the CI job's budget is the hang guard.
    return weida_nng.SocketOptions()


async def round_trip():
    ctx = weida_nng.Context()
    server = weida_nng.RepSocket(ctx, options())
    client = weida_nng.ReqSocket(ctx, options())
    url = await server.listen("tcp://127.0.0.1:0")
    await client.dial(url)

    await client.send(b"from the wheel")
    request = await server.recv()
    assert request == b"from the wheel", request
    await server.send(b"answered")
    reply = await client.recv()
    assert reply == b"answered", reply

    await ctx.shutdown()


def sync_round_trip():
    from weida_nng import sync

    context = sync.Context()
    server = sync.RepSocket(context, options())
    client = sync.ReqSocket(context, options())
    url = server.listen("tcp://127.0.0.1:0")
    client.dial(url)

    client.send(b"no loop needed")
    assert server.recv() == b"no loop needed"
    server.send(b"and none used")
    assert client.recv() == b"and none used"

    context.shutdown()


def metadata():
    from importlib.metadata import metadata as read

    installed = read("weida-nng")
    for required in ["Name", "Version", "Summary", "Requires-Python"]:
        assert installed[required], f"the wheel carries no {required}"
    print(f"{installed['Name']} {installed['Version']} on {sys.implementation.name}")
    print(f"requires-python: {installed['Requires-Python']}")
    return installed["Version"]


def main():
    version = metadata()
    asyncio.run(round_trip())
    sync_round_trip()

    # The surfaces the wheel is supposed to carry, not only the one exercised.
    for name in [
        "Context",
        "SocketOptions",
        "Pair0Socket",
        "Pair1Socket",
        "ReqSocket",
        "RepSocket",
        "PushSocket",
        "PullSocket",
        "PubSocket",
        "SubSocket",
        "SurveyorSocket",
        "RespondentSocket",
        "BusSocket",
        "NngError",
        "ETIMEDOUT",
    ]:
        assert hasattr(weida_nng, name), name
    print(f"weida_nng {version}: REQ/REP on the asyncio and the sync surface present")


if __name__ == "__main__":
    main()
