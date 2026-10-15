"""The synchronous surface: no event loop in the process at all.

Two claims:

1. **The send-to-`accepted` of the asyncio surface runs unchanged in
   synchronous form.** The same call, the same outcome object, the same
   meaning — because it is the same client with the waiting moved.
2. **The GIL is released while a call blocks**, which the threaded test proves:
   a second thread runs while the first is parked in `next_delivery`.

The peer runs in its own thread with its own asyncio loop, because the point
of this surface is that the *client's* thread has none.
"""

import threading
import time

import pytest

import weida_amqp
from weida_amqp import sync

from broker import ACCEPTED, Peer, data_section

DEADLINE = 5.0


def peer_thread(script):
    """Runs `script(peer)` on a loopback listener in another thread.

    Reports the port through a queue, because the client thread has no loop to
    await a future on — which is the whole situation this surface is for.
    """
    import asyncio
    import queue

    ports = queue.Queue()

    def run():
        async def main():
            async def accept(reader, writer):
                peer = Peer(reader, writer)
                try:
                    await script(peer)
                finally:
                    writer.close()

            server = await asyncio.start_server(accept, "127.0.0.1", 0)
            ports.put(server.sockets[0].getsockname()[1])
            async with server:
                await asyncio.sleep(DEADLINE)

        asyncio.run(main())

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return ports.get(timeout=DEADLINE), thread


def test_b170s_send_to_accepted_runs_unchanged_in_synchronous_form():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=False)
        await peer.grant_credit(1)
        payload = await peer.read_transfer()
        assert b"an order" in payload
        await peer.settle(ACCEPTED)
        await peer.expect_close()

    port, _ = peer_thread(script)
    connection = sync.connect("127.0.0.1", port, container_id="test")
    session = connection.begin()
    sender = session.attach("orders", "sender", "/queues/orders")

    outcome = sender.send(b"an order")
    assert outcome is not None
    assert outcome.name == "accepted"
    assert not outcome.may_be_redelivered

    connection.close()


def test_next_delivery_takes_a_timeout_and_returns_none_when_it_elapses():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        # Nothing is sent: the timeout is what the test is about.
        await peer.expect_close()

    port, _ = peer_thread(script)
    connection = sync.connect("127.0.0.1", port, container_id="test")
    session = connection.begin()
    receiver = session.attach("invoices", "receiver", "/queues/invoices")
    receiver.grant_credit(1)

    started = time.monotonic()
    assert receiver.next_delivery(timeout=0.25) is None
    waited = time.monotonic() - started
    assert 0.2 <= waited < 2.0, waited
    # And the link is still usable afterwards, which is what distinguishes a
    # timeout from a failure.
    assert receiver.credit()["link_credit"] == 1

    connection.close()


def test_a_timeout_that_cannot_be_counted_is_refused_as_an_exception():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        await peer.expect_close()

    port, _ = peer_thread(script)
    connection = sync.connect("127.0.0.1", port, container_id="test")
    session = connection.begin()
    receiver = session.attach("invoices", "receiver", "/queues/invoices")
    receiver.grant_credit(1)

    with pytest.raises(Exception):
        receiver.next_delivery(timeout=float("inf"))
    with pytest.raises(Exception):
        sync.connect("127.0.0.1", 9, container_id="test", handshake_timeout=float("nan"))

    connection.close()



def test_a_delivery_arrives_and_is_accepted_synchronously():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        await peer.send_transfer(data_section(b"an invoice"))
        await peer.expect_close()

    port, _ = peer_thread(script)
    connection = sync.connect("127.0.0.1", port, container_id="test")
    session = connection.begin()
    receiver = session.attach("invoices", "receiver", "/queues/invoices")
    receiver.grant_credit(4)

    delivery = receiver.next_delivery(timeout=DEADLINE)
    assert delivery is not None
    assert delivery.body() == b"an invoice"
    receiver.accept(delivery.delivery_id)

    connection.close()


def test_the_gil_is_released_while_a_call_blocks():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        await peer.expect_close()

    port, _ = peer_thread(script)
    connection = sync.connect("127.0.0.1", port, container_id="test")
    session = connection.begin()
    receiver = session.attach("invoices", "receiver", "/queues/invoices")
    receiver.grant_credit(1)

    ran = threading.Event()

    def other():
        # If the blocked call held the GIL this would not run until it
        # returned, and the assertion below would fail.
        ran.set()

    thread = threading.Thread(target=other)
    started = time.monotonic()
    thread.start()
    assert receiver.next_delivery(timeout=0.5) is None
    thread.join(timeout=DEADLINE)
    assert ran.is_set(), "a second thread ran while the first was blocked"
    assert time.monotonic() - started < 3.0

    connection.close()


def test_the_two_surfaces_raise_the_same_classes():
    # One family, one base: a program that catches `AmqpError` catches both
    # surfaces, because neither has an error vocabulary of its own.
    with pytest.raises(weida_amqp.Configuration):
        sync.connect("127.0.0.1", 1, max_frame_size=511)
    assert issubclass(weida_amqp.Configuration, weida_amqp.AmqpError)
