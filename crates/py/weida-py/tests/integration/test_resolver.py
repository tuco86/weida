"""A resolver callable over real QUIC on localhost: a name only it knows,
answered on its thread or on the loop, refused and malformed answers, and a
process that exits by itself."""

import asyncio
import dataclasses
import subprocess
import sys
import threading

import pytest

import weida
import weida.asyncio as wa

WAIT = 5.0
NAME = "svc.weida.test"


def pinned_tls() -> weida.ClientTls:
    """Client TLS pinning a key nobody holds: a dial with nothing to trust
    is refused before it resolves."""
    return weida.ClientTls(weida.Trust().add_pin(weida.Identity.generate().fingerprint()))


def answer(replier: weida.Replier) -> None:
    request = replier.accept(WAIT)
    request.take_body().collect(64, WAIT)
    reply = request.reply(None, WAIT)
    reply.write_all(b"hi", WAIT)
    reply.finish().delivered(WAIT)


async def answer_async(replier: wa.Replier) -> None:
    request = await replier.accept()
    await request.take_body().collect(64)
    reply = await request.reply(None)
    await reply.write_all(b"hi")
    await reply.finish().delivered()


def test_a_name_only_the_resolver_knows_is_dialled_through_it():
    identity = weida.Identity.generate_for([NAME])
    server = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(identity), WAIT)
    replier = listener.replier("/echo")
    port = binding.local_addr().rsplit(":", 1)[1]
    seen = []

    def resolve(lookup):
        seen.append((lookup, threading.current_thread().name))
        return [f"127.0.0.1:{port}"]

    client = weida.Runtime.owned(weida.RuntimeConfig(resolver=resolve))
    requester = client.requester(weida.ClientTls(weida.Trust().add_pin(identity.fingerprint())))
    requester.connect(f"weida://{NAME}/echo", WAIT)
    served = threading.Thread(target=answer, args=(replier,))
    served.start()
    assert requester.request(b"hello", WAIT).collect(64, WAIT).payload == b"hi"
    served.join(WAIT)
    assert seen == [(weida.Lookup(NAME, None, 8), "weida-resolver")]
    with pytest.raises(dataclasses.FrozenInstanceError):
        seen[0][0].name = "other"  # type: ignore[misc]

    requester.drop()
    client.shutdown(WAIT)
    for handle in (replier, binding, listener):
        handle.drop()
    server.shutdown(WAIT)


def test_a_coroutine_function_resolver_answers_on_the_running_loop():
    async def main() -> None:
        identity = weida.Identity.generate_for([NAME])
        server = wa.Runtime.owned()
        listener = server.listener()
        binding = await listener.bind_quic("127.0.0.1:0", weida.ServerTls(identity))
        replier = listener.replier("/echo")
        port = binding.local_addr().rsplit(":", 1)[1]
        threads = []

        async def resolve(lookup):
            threads.append(threading.get_ident())
            await asyncio.sleep(0)
            if lookup.name != NAME:
                raise LookupError("no such service")
            return [f"127.0.0.1:{port}"]

        client = wa.Runtime.owned(weida.RuntimeConfig(resolver=resolve))
        requester = client.requester(weida.ClientTls(weida.Trust().add_pin(identity.fingerprint())))
        await requester.connect(f"weida://{NAME}/echo")
        served = asyncio.ensure_future(answer_async(replier))
        reply = await requester.request(b"hello")
        assert (await reply.collect(64)).payload == b"hi"
        await served
        refused = client.requester(pinned_tls())
        with pytest.raises(weida.InvalidAddress, match="no such service"):
            await refused.connect("weida://nowhere.weida.test:7443/echo")
        assert threads == [threading.get_ident()] * 2

        for handle in (refused, requester):
            handle.drop()
        await client.shutdown()
        for handle in (replier, binding, listener):
            handle.drop()
        await server.shutdown()

    asyncio.run(main())


def test_a_refused_or_malformed_answer_raises_invalid_address():
    def resolve(lookup):
        if lookup.name == "garbled.weida.test":
            return "127.0.0.1:1"
        if lookup.name == "many.weida.test":
            return [f"127.0.0.1:{i}" for i in range(1, 10)]
        raise LookupError("no such service")

    client = weida.Runtime.owned(weida.RuntimeConfig(resolver=resolve))
    requesters = []
    for name, text in (
        ("nowhere.weida.test", "no such service"),
        ("garbled.weida.test", "list of address strings"),
        ("many.weida.test", "9 addresses"),
    ):
        requester = client.requester(pinned_tls())
        requesters.append(requester)
        with pytest.raises(weida.InvalidAddress, match=text):
            requester.connect(f"weida://{name}:7443/echo", WAIT)

    for requester in requesters:
        requester.drop()
    client.shutdown(WAIT)


SYNC_SCRIPT = """
import weida

def resolve(lookup):
    raise LookupError("none")

runtime = weida.Runtime.owned(weida.RuntimeConfig(resolver=resolve))
pin = weida.Identity.generate().fingerprint()
requester = runtime.requester(weida.ClientTls(weida.Trust().add_pin(pin)))
try:
    requester.connect("weida://x.weida.test:1/p", 5.0)
except weida.InvalidAddress as error:
    print(type(error).__name__)
"""

ASYNC_SCRIPT = """
import asyncio
import weida
import weida.asyncio as wa

async def resolve(lookup):
    raise LookupError("none")

async def main():
    runtime = wa.Runtime.owned(weida.RuntimeConfig(resolver=resolve))
    pin = weida.Identity.generate().fingerprint()
    requester = runtime.requester(weida.ClientTls(weida.Trust().add_pin(pin)))
    try:
        await requester.connect("weida://x.weida.test:1/p")
    except weida.InvalidAddress as error:
        print(type(error).__name__)

asyncio.run(main())
"""


@pytest.mark.parametrize("script", [SYNC_SCRIPT, ASYNC_SCRIPT], ids=["sync", "asyncio"])
def test_a_runtime_with_a_resolver_does_not_keep_the_process_alive(script):
    done = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "InvalidAddress"
