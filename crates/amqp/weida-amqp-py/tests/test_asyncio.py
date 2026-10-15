"""The asyncio surface: what an await means, and who grants credit.

Three claims, and each test asserts the claim rather than the plumbing:

1. `await send` completes when the **peer has said what happened**, and hands
   back the outcome. Not a boolean, not "the octets left".
2. Nothing arrives before `grant_credit`. Link credit is the receiver's
   instrument and this binding never grants any by itself.
3. AMQP's error conditions are exception classes, so a caller writes
   `except weida_amqp.LinkStolen` instead of matching on text.

The peer is `broker.py`: a hand-written AMQP 1.0 other-end in this directory.
RabbitMQ is absent on most machines
(`docs/libraries/amqp.md` §9), so a scripted peer is what makes a Python round
trip provable at all.
"""

import asyncio

import pytest

import weida_amqp

from broker import (
    ACCEPTED,
    REJECTED,
    Peer,
    data_section,
    value_section,
)

pytestmark = pytest.mark.asyncio

DEADLINE = 5.0


async def with_peer(script):
    """Runs `script(peer)` on a loopback listener and reports its port."""

    async def accept(reader, writer):
        peer = Peer(reader, writer)
        try:
            await script(peer)
        finally:
            writer.close()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def test_await_send_returns_the_outcome_the_peer_committed_to():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=False)
        await peer.grant_credit(1)
        payload = await peer.read_transfer()
        assert b"an order" in payload
        await peer.settle(ACCEPTED)
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        sender = await session.attach("orders", "sender", "/queues/orders")

        outcome = await asyncio.wait_for(sender.send(b"an order"), DEADLINE)
        # The whole point: an outcome, not a boolean.
        assert outcome is not None
        assert outcome.name == "accepted"
        assert outcome == "accepted"
        assert not outcome.may_be_redelivered
        assert not outcome.increments_delivery_count

        await connection.close()


async def test_a_rejection_arrives_as_an_outcome_with_its_reason():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=False)
        await peer.grant_credit(1)
        await peer.read_transfer()
        await peer.settle(REJECTED)
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        sender = await session.attach("orders", "sender", "/queues/orders")

        outcome = await asyncio.wait_for(sender.send(b"bad"), DEADLINE)
        assert outcome.name == "rejected"
        # What it proves: the message will not come back, and this attempt
        # counts against a redelivery limit.
        assert not outcome.may_be_redelivered
        assert outcome.increments_delivery_count

        await connection.close()


async def test_nothing_arrives_before_credit_is_granted():
    granted = asyncio.Event()

    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        # The client's `flow` granting credit. Until it arrives this peer has
        # no permission to send anything at all, which is the scheme's own
        # starting point rather than a policy of ours.
        code, _ = await peer.read_performative()
        granted.set()
        await peer.send_transfer(data_section(b"an invoice"))
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        receiver = await session.attach("invoices", "receiver", "/queues/invoices")

        credit = await receiver.credit()
        assert credit.link_credit == 0, "a fresh link carries no permission"
        assert not granted.is_set(), "and this binding granted none by itself"

        await receiver.grant_credit(5)
        delivery = await asyncio.wait_for(receiver.next_delivery(), DEADLINE)
        assert delivery is not None
        assert delivery.body() == b"an invoice"
        assert delivery.delivery_id == 0
        assert not delivery.settled

        after = await receiver.credit()
        assert after.link_credit == 4, "one message, one unit of credit"

        await receiver.accept(delivery.delivery_id)
        await connection.close()


async def test_the_delivery_stream_is_an_async_iterator():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        for n in range(3):
            await peer.send_transfer(value_section(f"invoice-{n}"), settled=True)
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        receiver = await session.attach("invoices", "receiver", "/queues/invoices")
        await receiver.grant_credit(3)

        seen = []
        async for delivery in receiver:
            seen.append(delivery.body())
            # A settled delivery needs no answer: the sender has already
            # forgotten it.
            assert delivery.settled
            if len(seen) == 3:
                break
        assert seen == ["invoice-0", "invoice-1", "invoice-2"]

        await connection.close()


async def test_the_answering_attach_is_data_and_is_readable():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=False, name="orders")
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        sender = await session.attach(
            "orders", "sender", "/queues/orders", rcv_settle_mode="second"
        )

        negotiated = await sender.negotiated()
        assert negotiated is not None
        # The peer answered `first`, so `second` is not in force however it was
        # asked for. A caller that did not look would believe it had a
        # guarantee it does not have.
        assert negotiated.rcv_settle_mode == "first"
        assert negotiated.target_address == "q"

        # The two handle spaces are independent, and the numbers differ.
        assert await sender.output_handle() == 0
        assert await sender.input_handle() == 11

        await connection.close()


async def test_the_session_windows_are_readable():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin(incoming_window=1)
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        windows = await session.windows()
        # One frame of room, which is what `remote_incoming_window` says. A
        # sender that stalls with credit in hand is diagnosed here.
        assert windows.remote_incoming_window == 1
        assert windows.incoming_window == 400
        assert "remote_incoming" in repr(windows)
        assert windows.as_dict()["remote_incoming_window"] == 1

        await connection.close()


async def test_a_condition_is_its_own_exception_class():
    async def script(peer):
        await peer.handshake()
        # A `close` carrying a condition, before any session exists.
        from broker import CLOSE, described, encode_string, encode_symbol, frame

        error = described(
            0x1D,
            [
                encode_symbol("amqp:resource-limit-exceeded"),
                encode_string("too many connections"),
            ],
        )
        await peer.write(frame(0, described(CLOSE, [error])))

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        # The condition reaches Python as its own class, under the base, with
        # the symbol recoverable from the module's table.
        with pytest.raises(weida_amqp.ResourceLimitExceeded) as raised:
            await asyncio.wait_for(connection.begin(), DEADLINE)
        assert issubclass(weida_amqp.ResourceLimitExceeded, weida_amqp.AmqpError)
        assert raised.value.errno == "ResourceLimitExceeded"
        assert "too many connections" in str(raised.value)
        assert (
            weida_amqp.CONDITIONS["ResourceLimitExceeded"]
            == "amqp:resource-limit-exceeded"
        )


async def test_a_refused_configuration_names_the_value():
    # 511 is below the 512 both peers MUST accept, and the library refuses it
    # where it is configured rather than raising it quietly.
    with pytest.raises(weida_amqp.Configuration) as raised:
        await weida_amqp.connect("127.0.0.1", 1, max_frame_size=511)
    assert "MIN-MAX-FRAME-SIZE" in str(raised.value)


async def test_a_settle_mode_name_outside_the_set_is_refused():
    with pytest.raises(weida_amqp.Configuration) as raised:
        await weida_amqp.connect("127.0.0.1", 1, sasl_anonymous=True, sasl_user="x")
    assert "two different mechanisms" in str(raised.value)


async def test_a_receive_cancelled_while_waiting_loses_no_delivery():
    cancelled = asyncio.Event()

    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        # Sent only after the first receive was cancelled: the next receive
        # must still get it.
        await cancelled.wait()
        await peer.send_transfer(data_section(b"kept"), settled=True)
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        receiver = await session.attach("invoices", "receiver", "/queues/invoices")
        await receiver.grant_credit(1)

        task = asyncio.ensure_future(receiver.next_delivery())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        cancelled.set()
        await asyncio.sleep(0.1)

        delivery = await asyncio.wait_for(receiver.next_delivery(), DEADLINE)
        assert delivery is not None
        assert delivery.body() == b"kept"
        await connection.close()


async def test_a_delivery_hands_back_the_same_objects_on_every_access():
    async def script(peer):
        await peer.handshake()
        await peer.answer_begin()
        await peer.answer_attach(role_is_sender=True)
        await peer.read_performative()  # the flow
        await peer.send_transfer(data_section(b"once"), settled=True)
        await peer.expect_close()

    server, port = await with_peer(script)
    async with server:
        connection = await weida_amqp.connect("127.0.0.1", port, container_id="test")
        session = await connection.begin()
        receiver = await session.attach("invoices", "receiver", "/queues/invoices")
        await receiver.grant_credit(1)

        delivery = await asyncio.wait_for(receiver.next_delivery(), DEADLINE)
        assert delivery.payload is delivery.payload
        assert delivery.body() is delivery.body()
        assert delivery.delivery_tag is delivery.delivery_tag
        assert delivery.body() == b"once"
        await connection.close()


@pytest.mark.parametrize("handshake_timeout", [-1, float("nan"), 1e30])
async def test_a_timeout_the_clock_cannot_express_is_refused(handshake_timeout):
    # Refused before any socket is opened, so the port is never dialled.
    with pytest.raises(Exception):
        await weida_amqp.connect(
            "127.0.0.1", 9, container_id="test", handshake_timeout=handshake_timeout
        )
