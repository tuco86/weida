"""Both surfaces against `fe2o3-amqp`, a foreign peer and not a script.

A send awaited to `accepted` and a receive settled from Python are checked
*against that peer*, because a client checked only against a server written by
the same hand is checked against one opinion twice. `fe2o3-amqp` is a pure-Rust
AMQP 1.0 implementation with an `acceptor`, so the peer needs no broker and no
C toolchain - it is a `cargo` example of the library this binding wraps,
`crates/amqp/weida-amqp/examples/fe2o3_peer.rs`.

Where that example has not been built the tests **skip** with the one command
that builds it, in the shape `crates/amqp/weida-amqp/tests/interop_rabbitmq.rs`
uses for an absent broker. `develop.sh` builds it, so a normal run does not
skip.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

import weida_amqp
from weida_amqp import sync

# Marked per test rather than for the file: the synchronous surface's test is
# an ordinary function and pytest-asyncio warns about a coroutine mark on one.
asyncio_test = pytest.mark.asyncio

DEADLINE = 20.0

BUILD = "cargo build -p weida-amqp --example fe2o3_peer"


def peer_binary():
    """The built example, or `None`."""
    target = os.environ.get("CARGO_TARGET_DIR")
    roots = [Path(target)] if target else []
    roots.append(Path(__file__).resolve().parents[4] / "target")
    for root in roots:
        for profile in ("debug", "release"):
            candidate = root / profile / "examples" / "fe2o3_peer"
            if candidate.is_file():
                return candidate
    return None


needs_peer = pytest.mark.skipif(
    peer_binary() is None,
    reason=f"the fe2o3-amqp peer has not been built: {BUILD}",
)


class Peer:
    """One `fe2o3-amqp` connection, as a child process.

    Reads the port the peer announces rather than picking one: two tests in
    the same run would otherwise collide on a fixed number.
    """

    def __init__(self, role):
        self.process = subprocess.Popen(  # noqa: S603
            [str(peer_binary()), role],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        line = self.process.stdout.readline()
        assert line.startswith("PORT "), f"the peer announced {line!r}"
        self.port = int(line.split()[1])

    def finish(self, timeout=DEADLINE):
        """Waits for the peer to exit and asserts it was satisfied."""
        code = self.process.wait(timeout=timeout)
        if code != 0:
            sys.stderr.write(self.process.stderr.read())
        assert code == 0, f"the peer exited {code}"

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.process.poll() is None:
            self.process.kill()
        self.process.stdout.close()
        self.process.stderr.close()


@asyncio_test
@needs_peer
async def test_a_send_is_accepted_by_a_fe2o3_receiver():
    # A foreign implementation's disposition, as a Python outcome object.
    with Peer("receiver") as peer:
        connection = await weida_amqp.connect(
            "127.0.0.1", peer.port, container_id="weida-amqp-py", idle_time_out=None
        )
        session = await connection.begin()
        sender = await session.attach("py-sender", "sender", "q1")
        outcome = await asyncio.wait_for(sender.send(b"from weida-amqp-py"), DEADLINE)
        assert outcome == "accepted"
        assert not outcome.may_be_redelivered
        await connection.close()
        peer.finish()


@asyncio_test
@needs_peer
async def test_a_delivery_from_a_fe2o3_sender_is_settled_from_python():
    # The other direction: credit granted from Python, the peer's message
    # arriving with its sections, and `accept` reaching it as a disposition -
    # the peer's own `send` only returns once that outcome has landed.
    with Peer("sender") as peer:
        connection = await weida_amqp.connect(
            "127.0.0.1", peer.port, container_id="weida-amqp-py", idle_time_out=None
        )
        session = await connection.begin()
        receiver = await session.attach("py-receiver", "receiver", "q1")
        await receiver.grant_credit(1)
        delivery = await asyncio.wait_for(receiver.next_delivery(), DEADLINE)
        assert delivery is not None
        assert delivery.body() == "from fe2o3-amqp"
        await receiver.accept(delivery.delivery_id)
        await connection.close()
        peer.finish()


@needs_peer
def test_the_synchronous_surface_is_accepted_by_the_same_peer():
    # The same exchange, no event loop in the process at all.
    with Peer("receiver") as peer:
        connection = sync.connect(
            "127.0.0.1", peer.port, container_id="weida-amqp-py", idle_time_out=None
        )
        session = connection.begin()
        sender = session.attach("py-sender", "sender", "q1")
        outcome = sender.send(b"from weida-amqp-py")
        assert outcome == "accepted"
        connection.close()
        peer.finish()
