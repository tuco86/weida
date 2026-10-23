"""Regression tests for the blocking surface, bad seconds, payload identity and
cancelled receives.

The scripted broker is a plain socket that accepts one connection, answers the
CONNECT with a CONNACK, and then reads and ignores everything: it never
acknowledges a PUBLISH, which is the state a blocked publish is in.

A `sync.Context` owns the reactor its connections run on, so each test keeps
its context in a variable: a temporary one is dropped at once and takes the
reactor, and with it the connection, down.
"""

import asyncio
import socket
import threading
import time

import pytest

import waits
import weida_mqtt
from weida_mqtt import sync

# CONNACK: no Session Present, reason 0x00 Success, no properties.
CONNACK_OK = bytes([0x20, 0x03, 0x00, 0x00, 0x00])


@pytest.fixture
def scripted():
    """The address of a broker that answers CONNECT and never answers PUBLISH."""
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]

    def serve():
        conn, _ = listener.accept()
        with conn:
            conn.recv(4096)  # the CONNECT; its contents are not needed here
            conn.sendall(CONNACK_OK)
            while conn.recv(4096):
                pass

    threading.Thread(target=serve, daemon=True).start()
    yield f"127.0.0.1:{port}"
    listener.close()


def test_getters_and_disconnect_do_not_wait_for_a_blocked_publish(scripted):
    context = sync.Context()
    client, _deliveries = context.connect(scripted, weida_mqtt.ConnectOptions("blocked-1"))

    failures = []

    def publisher():
        try:
            client.publish(weida_mqtt.Message("t", b"x", qos=1))
        except Exception as error:  # the disconnect below must end this call
            failures.append(error)

    thread = threading.Thread(target=publisher, daemon=True)
    thread.start()
    time.sleep(0.2)

    # No watchdog here: if a getter or the disconnect waited for the blocked
    # publish this would hang, and the CI job's budget is the guard.
    assert client.client_id == "blocked-1"
    assert "blocked-1" in repr(client)
    assert client.session_present is False
    assert client.is_encrypted is False
    client.disconnect()

    waits.join(thread, "the blocked publish to end after the disconnect")
    assert len(failures) == 1


def test_bad_seconds_are_exceptions_not_panics(scripted):
    # `pytest.raises(Exception)` does not catch PanicException, which is a
    # BaseException, so a panic fails these assertions rather than passing.
    context = sync.Context()
    client, deliveries = context.connect(scripted, weida_mqtt.ConnectOptions("seconds-1"))
    with pytest.raises(Exception):
        client.disconnect(session_expiry=float("inf"))
    with pytest.raises(Exception):
        deliveries.recv(timeout=1e30)
    with pytest.raises(Exception):
        weida_mqtt.Message("t", b"", message_expiry=float("nan"))
    with pytest.raises(Exception):
        weida_mqtt.ConnectOptions("c", keep_alive=-1)
    with pytest.raises(Exception):
        weida_mqtt.ConnectOptions("c", session_expiry=float("inf"))


def test_payload_is_the_bytes_object_given():
    payload = b"hello"
    message = weida_mqtt.Message("t", payload)
    assert message.payload is payload
    assert message.payload is message.payload
    assert weida_mqtt.Message("t", bytearray(b"hi")).payload == b"hi"


def test_cancelled_receive_gives_the_delivery_back(broker, run, client_id):
    topic = f"w3py/restore/{client_id}"
    expected = b"kept-for-the-next-receive"

    async def scenario():
        context = weida_mqtt.Context()
        client, events = await context.connect(
            broker, weida_mqtt.ConnectOptions(client_id + "-sub")
        )
        await client.subscribe([weida_mqtt.Subscription(topic, 1)])
        task = asyncio.ensure_future(events.__anext__())
        await asyncio.sleep(0.05)

        # A blocking publish from this process: it does not yield to the loop,
        # so the delivery completes inside the bridge while the loop is busy
        # and its completion is queued behind the cancel below.
        sync_context = sync.Context()
        publisher, _ = sync_context.connect(
            broker, weida_mqtt.ConnectOptions(client_id + "-pub")
        )
        publisher.publish(weida_mqtt.Message(topic, expected, qos=0))
        publisher.disconnect()
        time.sleep(0.2)

        task.cancel()
        await asyncio.sleep(0.05)
        delivery = await events.__anext__()
        assert delivery.payload == expected
        await client.disconnect()

    run(scenario())
