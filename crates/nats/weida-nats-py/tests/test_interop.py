"""Interop: both surfaces of this binding against a real `nats-server`.

`scripted.py` proves the sequence — what this client puts on the wire and in
what order — which is what a hand-written server half is for. It cannot prove
interoperability: a server this repository wrote agrees with a client this
repository wrote by construction. Only the real binary can.

**`nats-server` is absent on the machine this was written on**, so every test
here skips, with the install command in the skip message — the same place
`test_interop.py` in `crates/zmq/weida-zmq-py` puts one for `pyzmq`, and the
same command `crates/nats/weida-nats/tests/interop_nats_server.rs` names for
the Rust half of the same question.
"""

import asyncio
import shutil
import socket
import subprocess
import threading
import time

import pytest

import weida_nats
from weida_nats import sync

import waits

NEEDS_SERVER = (
    "needs nats-server: `pacman -S nats-server`, a release tarball from "
    "github.com/nats-io/nats-server/releases, "
    "`go install github.com/nats-io/nats-server/v2@latest` "
    "or `docker run -p 4222:4222 nats:2`"
)


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def nats_server():
    """A supervised `nats-server`, stopped on success and on failure alike."""
    binary = shutil.which("nats-server")
    if binary is None:
        pytest.skip(NEEDS_SERVER)

    port = free_port()
    child = subprocess.Popen(
        # No cluster, no JetStream, no monitoring: this is Core NATS.
        [binary, "--port", str(port), "--no_sys_acc"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Readiness is observed, not slept on: the server says so on stderr.
        # The read blocks until it does; the CI job's budget is the guard.
        while True:
            line = child.stderr.readline()
            if not line:
                raise AssertionError("nats-server exited before it was ready")
            if "Server is ready" in line:
                break
        yield port
    finally:
        child.kill()
        child.wait()


def test_the_asyncio_surface_against_nats_server(nats_server):
    async def exchange():
        nats = await weida_nats.connect("127.0.0.1", nats_server)
        assert nats.headers_supported()

        orders = await nats.subscribe("orders.>")
        await nats.publish("orders.created", b"one")
        assert (await orders.next()).payload == b"one"

        echo = await nats.subscribe("service.echo")

        async def responder():
            request = await echo.next()
            await nats.publish(request.reply_to, b"pong")

        answering = asyncio.create_task(responder())
        assert (await nats.request("service.echo", b"ping", waits.PATIENT)).payload == b"pong"
        await answering

        with pytest.raises(weida_nats.NoResponders):
            await nats.request("nobody.listening", b"?", waits.PATIENT)
        await nats.close()

    waits.run(exchange())


def test_the_synchronous_surface_against_nats_server(nats_server):
    responder = sync.connect("127.0.0.1", nats_server)
    echo = responder.subscribe("service.echo")
    responder.flush()

    def answer():
        request = waits.next_msg(echo, "the request")
        responder.publish(request.reply_to, b"pong")

    thread = threading.Thread(target=answer)
    thread.start()
    try:
        client = sync.connect("127.0.0.1", nats_server)
        assert client.request("service.echo", b"ping", waits.PATIENT).payload == b"pong"
        with pytest.raises(weida_nats.NoResponders):
            client.request("nobody.listening", b"?", waits.PATIENT)
        client.close()
    finally:
        waits.join(thread, "the responder thread")
        responder.close()
