"""`weida_mqtt.sync`: MQTT from a Python program with no event loop.

The claim: the synchronous Python surface over the same client, no asyncio
loop required in the process and no second implementation of any protocol
behaviour, with a receive timeout where the async surface has cancellation;
the QoS 2 round trip of the asyncio surface runs unchanged in synchronous
form.

`test_the_qos_2_round_trip_runs_unchanged_in_synchronous_form` is the last
clause, and it is deliberately the *same* assertions as
`test_a_qos_2_publish_is_awaited_to_its_pubcomp` in `test_interop.py` with the
`await`s removed — because "unchanged" is the claim and a differently-shaped
test would not be evidence for it.
"""

import threading
import time

import pytest

import weida_mqtt
from weida_mqtt import sync


def test_no_event_loop_exists_in_this_process():
    """The claim, asserted before anything else uses it.

    Every other test in this file runs under the same condition, so stating it
    once here is what makes them all evidence: a surface that secretly needed a
    loop would fail on the first `connect`, and this is the assertion that says
    there is none to need.
    """
    import asyncio

    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()


class TestRoundTrip:
    def test_the_qos_2_round_trip_runs_unchanged_in_synchronous_form(
        self, broker, client_id
    ):
        """The QoS 2 round trip, with the `await`s removed and nothing else.

        `Completion.kind == "complete"` is the PUBCOMP: the fourth packet of
        the four-packet handshake. A facade that resolved on the write would
        say `"sent"` and one that resolved on the PUBREC would say
        `"acknowledged"`, so the string is what separates the three — and it is
        the **same** `Completion` class the asynchronous surface returns,
        because the facade adds no type of its own.
        """
        context = sync.Context(worker_threads=1)
        options = weida_mqtt.ConnectOptions(client_id, keep_alive=30.0)
        client, _deliveries = context.connect(broker, options)

        done = client.publish(
            weida_mqtt.Message(f"w3sync/{client_id}/two", b"exactly once", qos=2)
        )
        assert done.kind == "complete", repr(done)
        assert done.accepted
        assert done.reason_code in (0x00, 0x92), hex(done.reason_code)
        assert isinstance(done, weida_mqtt.Completion), (
            "the same class the asynchronous surface returns, not a second one"
        )

        # The three levels side by side, as in the asynchronous test.
        zero = client.publish(weida_mqtt.Message(f"w3sync/{client_id}/zero", b"0"))
        assert zero.kind == "sent"
        assert zero.reason_code is None

        one = client.publish(
            weida_mqtt.Message(f"w3sync/{client_id}/one", b"1", qos=1)
        )
        assert one.kind == "acknowledged"

        client.disconnect()

    def test_subscribe_publish_receive_with_no_loop(self, broker, client_id):
        """The whole round trip, blocking, our client on both sides."""
        context = sync.Context(worker_threads=1)
        topic = f"w3sync/{client_id}/room/12"

        subscriber, deliveries = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-sub", keep_alive=30.0)
        )
        granted = subscriber.subscribe(
            [weida_mqtt.Subscription(f"w3sync/{client_id}/room/+", 2)]
        )
        assert granted == [2], "one granted code per filter"

        publisher, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-pub", keep_alive=30.0)
        )
        publisher.publish(
            weida_mqtt.Message(topic, b"21.5", qos=1, content_type="text/plain")
        )

        delivery = deliveries.recv(timeout=10.0)
        assert delivery.topic == topic
        assert delivery.payload == b"21.5"
        assert delivery.properties["content_type"] == "text/plain"
        assert isinstance(delivery, weida_mqtt.Delivery), "the same class again"

        publisher.disconnect()
        subscriber.disconnect()

    def test_deliveries_is_an_ordinary_python_iterator(self, broker, client_id):
        """`for delivery in deliveries:` — the synchronous mirror of the
        asynchronous surface's `async for`."""
        context = sync.Context(worker_threads=1)
        prefix = f"w3sync/{client_id}/stream"
        subscriber, deliveries = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-it-sub", keep_alive=30.0)
        )
        subscriber.subscribe([weida_mqtt.Subscription(f"{prefix}/+", 1)])

        publisher, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-it-pub", keep_alive=30.0)
        )
        for index in range(3):
            publisher.publish(
                weida_mqtt.Message(f"{prefix}/{index}", str(index).encode(), qos=1)
            )

        seen = []
        for delivery in deliveries:
            seen.append((delivery.topic, delivery.payload))
            if len(seen) == 3:
                break
        assert sorted(seen) == [
            (f"{prefix}/0", b"0"),
            (f"{prefix}/1", b"1"),
            (f"{prefix}/2", b"2"),
        ]

        publisher.disconnect()
        subscriber.disconnect()


class TestDeadline:
    def test_the_deadline_is_where_cancellation_goes(self, broker, client_id):
        """**The clause that distinguishes this surface**: a receive timeout
        where the asynchronous surface has cancellation.

        A coroutine that wants to stop waiting cancels its task; a blocking
        caller has no task, so the deadline is the argument. The expiry must
        leave the connection alone — nothing was consumed — and the second half
        of this test is that: the message published afterwards still arrives.
        """
        context = sync.Context(worker_threads=1)
        topic = f"w3sync/{client_id}/late"
        subscriber, deliveries = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-late-sub", keep_alive=30.0)
        )
        subscriber.subscribe([weida_mqtt.Subscription(topic, 1)])

        started = time.monotonic()
        with pytest.raises(weida_mqtt.Timeout) as expired:
            deliveries.recv(timeout=0.3)
        elapsed = time.monotonic() - started
        assert 0.2 < elapsed < 5.0, f"the deadline was honoured: {elapsed}s"
        assert expired.value.reason_code is None, "a deadline is ours, not the protocol's"

        # The connection is untouched by the expiry.
        publisher, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-late-pub", keep_alive=30.0)
        )
        publisher.publish(weida_mqtt.Message(topic, b"after", qos=1))
        delivery = deliveries.recv(timeout=10.0)
        assert delivery.payload == b"after"

        publisher.disconnect()
        subscriber.disconnect()

    def test_a_negative_timeout_is_refused(self, broker, client_id):
        context = sync.Context(worker_threads=1)
        client, deliveries = context.connect(
            broker, weida_mqtt.ConnectOptions(client_id, keep_alive=30.0)
        )
        with pytest.raises(ValueError):
            deliveries.recv(timeout=-1.0)
        client.disconnect()

    def test_publish_has_no_timeout_argument_and_the_omission_is_deliberate(
        self, broker, client_id
    ):
        """A publish abandoned mid-exchange is still session state on both
        sides, so a deadline there would return control while the exchange
        continued — which the asynchronous surface's cancellation does and is
        honest about, and which an argument named `timeout` would not be.

        Asserted rather than only documented, because "there is no such
        argument" is exactly the kind of claim that rots.
        """
        context = sync.Context(worker_threads=1)
        client, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(client_id, keep_alive=30.0)
        )
        with pytest.raises(TypeError):
            client.publish(
                weida_mqtt.Message("a/b", b"x", qos=1), timeout=1.0
            )
        client.disconnect()


class TestThreads:
    def test_the_gil_is_released_while_blocked(self, broker, client_id):
        """One thread waiting in `recv` must not stop another from running.

        The assertion is a **count**: a second thread increments a counter
        while the first is blocked, and a binding that held the GIL would leave
        that counter at or near zero. The usual shape of a synchronous MQTT
        program is one thread consuming and one publishing, and it only works
        if this does.
        """
        context = sync.Context(worker_threads=2)
        topic = f"w3sync/{client_id}/gil"
        subscriber, deliveries = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-gil-sub", keep_alive=30.0)
        )
        subscriber.subscribe([weida_mqtt.Subscription(topic, 1)])

        spun = [0]
        stop = threading.Event()

        def spin():
            while not stop.is_set():
                spun[0] += 1
                time.sleep(0.001)

        spinner = threading.Thread(target=spin, daemon=True)
        spinner.start()
        try:
            # Blocks for a third of a second with no message coming.
            with pytest.raises(weida_mqtt.Timeout):
                deliveries.recv(timeout=0.3)
        finally:
            stop.set()
            spinner.join(timeout=2.0)

        assert spun[0] > 10, (
            f"the second thread ran {spun[0]} times while the first was blocked "
            "in recv; a binding holding the GIL would have left it near zero"
        )
        subscriber.disconnect()

    def test_one_thread_publishes_while_another_consumes(self, broker, client_id):
        """The program shape the surface exists for."""
        context = sync.Context(worker_threads=2)
        topic = f"w3sync/{client_id}/pair"
        subscriber, deliveries = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-pair-sub", keep_alive=30.0)
        )
        subscriber.subscribe([weida_mqtt.Subscription(topic, 1)])

        received = []

        def consume():
            for _ in range(3):
                received.append(deliveries.recv(timeout=10.0).payload)

        consumer = threading.Thread(target=consume)
        consumer.start()

        publisher, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(f"{client_id}-pair-pub", keep_alive=30.0)
        )
        for index in range(3):
            publisher.publish(
                weida_mqtt.Message(topic, str(index).encode(), qos=1)
            )

        consumer.join(timeout=20.0)
        assert not consumer.is_alive(), "the consumer finished"
        assert sorted(received) == [b"0", b"1", b"2"]

        publisher.disconnect()
        subscriber.disconnect()


class TestNoSecondImplementation:
    def test_a_refusal_is_the_asynchronous_surfaces_refusal(self, broker, client_id):
        """The same class, the same code, from both surfaces.

        The point is not the refusal — that is tested against the asynchronous
        surface — but that the synchronous one reports the identical exception,
        which is what "no protocol behaviour is decided twice" means where a
        caller can see it.
        """
        context = sync.Context(worker_threads=1)
        client, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(client_id, keep_alive=30.0)
        )

        # The grammar of 4.7.1, refused before the wire.
        with pytest.raises(weida_mqtt.InvalidTopic) as refused:
            client.subscribe([weida_mqtt.Subscription("a/#/b", 1)])
        assert refused.value.reason_code is None
        assert "#" in refused.value.cause

        # An empty SUBSCRIBE ([MQTT-3.8.3-2]).
        with pytest.raises(weida_mqtt.MqttError):
            client.subscribe([])

        client.disconnect()

    def test_a_use_after_disconnect_is_not_connected(self, broker, client_id):
        context = sync.Context(worker_threads=1)
        client, _ = context.connect(
            broker, weida_mqtt.ConnectOptions(client_id, keep_alive=30.0)
        )
        client.disconnect()
        with pytest.raises(weida_mqtt.NotConnected):
            client.publish(weida_mqtt.Message("a/b", b"x", qos=1))

    def test_the_session_is_the_same_object_in_both_surfaces(self, broker, client_id):
        """`weida_mqtt.Session` is passed to `sync.Context.connect_session`
        unchanged, which is what makes a reconnect a resumption on either
        surface."""
        context = sync.Context(worker_threads=1)
        options = weida_mqtt.ConnectOptions(
            client_id, clean_start=False, keep_alive=30.0, session_expiry=300.0
        )
        session = weida_mqtt.Session(client_id, options)

        first, _ = context.connect_session(broker, session, options)
        assert not first.session_present
        first.subscribe([weida_mqtt.Subscription(f"w3sync/{client_id}/resume/+", 1)])
        assert session.subscriptions
        first.disconnect()

        second, _ = context.connect_session(broker, session, options)
        assert second.session_present, "the broker kept it across the reconnect"
        second.disconnect(session_expiry=0.0)
