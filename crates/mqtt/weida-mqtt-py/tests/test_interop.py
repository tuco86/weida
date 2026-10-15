"""The round trip from Python, through a real broker, and back.

The claim in one sentence: a QoS 2 publish awaited to its PUBCOMP from
Python, with the whole round trip run against a real broker.
`test_a_qos_2_publish_is_awaited_to_its_pubcomp` is that, and the
rest of this file is what a caller needs around it to believe it.

Skips with the command that starts a broker where none answers — see
`conftest.py`.
"""

import asyncio

import weida_mqtt


async def _connected(context, broker, client_id, **kwargs):
    options = weida_mqtt.ConnectOptions(client_id, keep_alive=30.0, **kwargs)
    return await context.connect(broker, options)


class TestRoundTrip:
    def test_a_qos_2_publish_is_awaited_to_its_pubcomp(self, broker, run, client_id):
        """**The acceptance, and the only assertion that proves the await.**

        `Completion.kind == "complete"` means the PUBCOMP arrived: the fourth
        packet of the four-packet handshake, after PUBLISH, PUBREC and PUBREL.
        A binding that resolved on the write would say `"sent"`, and a binding
        that resolved on the PUBREC would say `"acknowledged"` — so the string
        is what separates the three, and it is why `Completion` is a class
        rather than a boolean.
        """

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            client, events = await _connected(context, broker, client_id)

            done = await client.publish(
                weida_mqtt.Message(f"w3py/{client_id}/two", b"exactly once", qos=2)
            )
            assert done.kind == "complete", repr(done)
            assert done.accepted
            # The PUBCOMP's own reason code, which is 0x00 on success and
            # 0x92 where the server had already released the identifier.
            assert done.reason_code in (0x00, 0x92), hex(done.reason_code)

            # The three levels side by side, so the three completions are
            # distinguishable rather than asserted one at a time.
            zero = await client.publish(
                weida_mqtt.Message(f"w3py/{client_id}/zero", b"at most once")
            )
            assert zero.kind == "sent"
            assert zero.reason_code is None, "QoS 0 has no acknowledgement at all"

            one = await client.publish(
                weida_mqtt.Message(f"w3py/{client_id}/one", b"at least once", qos=1)
            )
            assert one.kind == "acknowledged"
            assert one.reason_code is not None

            await client.disconnect()

        run(scenario())

    def test_the_whole_round_trip_from_python(self, broker, run, client_id):
        """Subscribe, publish, receive — our client on both sides of the
        broker, driven from Python."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            topic = f"w3py/{client_id}/room/12"

            subscriber, events = await _connected(
                context, broker, f"{client_id}-sub"
            )
            granted = await subscriber.subscribe(
                [weida_mqtt.Subscription(f"w3py/{client_id}/room/+", 2)]
            )
            assert granted == [2], "one code per filter, and 2 was granted"

            publisher, _ = await _connected(context, broker, f"{client_id}-pub")
            await publisher.publish(
                weida_mqtt.Message(
                    topic,
                    b"21.5",
                    qos=1,
                    content_type="text/plain",
                    response_topic=f"w3py/{client_id}/ack",
                    correlation_data=b"req-7",
                    user_properties=[("unit", "celsius")],
                )
            )

            delivery = await events.__anext__()
            assert delivery.topic == topic
            assert delivery.payload == b"21.5"
            assert delivery.qos in (1, 2), "delivered at or below the publish's QoS"
            assert not delivery.retain, "a live delivery, not one from the cache"
            assert delivery.origin() == "live"
            assert delivery.packet_id is not None, "QoS > 0 carries an identifier"

            properties = delivery.properties
            assert properties["content_type"] == "text/plain"
            assert properties["response_topic"] == f"w3py/{client_id}/ack"
            assert properties["correlation_data"] == b"req-7"
            assert ("unit", "celsius") in properties["user_properties"]

            await publisher.disconnect()
            await subscriber.disconnect()

        run(scenario())

    def test_the_async_iterator_yields_deliveries(self, broker, run, client_id):
        """`async for delivery in events:` — the shape a consumer writes."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            prefix = f"w3py/{client_id}/stream"
            subscriber, events = await _connected(
                context, broker, f"{client_id}-stream-sub"
            )
            await subscriber.subscribe([weida_mqtt.Subscription(f"{prefix}/+", 1)])

            publisher, _ = await _connected(
                context, broker, f"{client_id}-stream-pub"
            )
            for index in range(3):
                await publisher.publish(
                    weida_mqtt.Message(f"{prefix}/{index}", str(index).encode(), qos=1)
                )

            seen = []
            async for delivery in events:
                seen.append((delivery.topic, delivery.payload))
                if len(seen) == 3:
                    break
            assert sorted(seen) == [
                (f"{prefix}/0", b"0"),
                (f"{prefix}/1", b"1"),
                (f"{prefix}/2", b"2"),
            ]

            await publisher.disconnect()
            await subscriber.disconnect()

        run(scenario())

    def test_a_subscription_identifier_names_the_filter_that_matched(
        self, broker, run, client_id
    ):
        """The identifier comes back "on every delivery it caused"
        ([MQTT-3.3.4-4]), which is how a client with several filters tells
        which one selected a message."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            prefix = f"w3py/{client_id}/ident"
            subscriber, events = await _connected(
                context, broker, f"{client_id}-ident-sub"
            )
            await subscriber.subscribe(
                [weida_mqtt.Subscription(f"{prefix}/+", 1)], subscription_id=42
            )

            publisher, _ = await _connected(context, broker, f"{client_id}-ident-pub")
            await publisher.publish(
                weida_mqtt.Message(f"{prefix}/one", b"x", qos=1)
            )

            delivery = await events.__anext__()
            # rumqttd 0.20.0 does report it; a broker declaring
            # `Subscription Identifiers Available` 0 would not, and the
            # fallback below is what a client uses then.
            assert delivery.subscription_ids == [42], delivery.subscription_ids

            # The fallback, which needs nothing from the server: MQTT's
            # matcher over the filters this client subscribed with.
            assert weida_mqtt.matches(f"{prefix}/+", delivery.topic)

            await publisher.disconnect()
            await subscriber.disconnect()

        run(scenario())


class TestRetained:
    def test_a_retained_value_is_stored_and_then_deleted(self, broker, run, client_id):
        """The three states of a retained topic, and the delete that is
        itself not stored ([MQTT-3.3.1-6])."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            topic = f"w3py/{client_id}/state"
            publisher, _ = await _connected(context, broker, f"{client_id}-ret-pub")

            await publisher.publish(
                weida_mqtt.Message(topic, b"on", qos=1, retain=True)
            )

            # A subscriber arriving afterwards gets it from the cache, with
            # RETAIN 1 ([MQTT-3.3.1-8]).
            late, late_events = await _connected(context, broker, f"{client_id}-ret-late")
            await late.subscribe([weida_mqtt.Subscription(topic, 1)])
            cached = await late_events.__anext__()
            assert cached.payload == b"on"
            assert cached.retain
            assert cached.origin() == "retained", "RETAIN 1 under RAP 0 is the cache"

            # The delete is a message: the live subscriber sees an empty
            # payload arrive.
            await publisher.publish(weida_mqtt.Message.delete_retained(topic, qos=1))
            deleted = await late_events.__anext__()
            assert deleted.payload == b""

            # And a subscriber arriving after it receives nothing.
            after, after_events = await _connected(
                context, broker, f"{client_id}-ret-after"
            )
            await after.subscribe([weida_mqtt.Subscription(topic, 1)])
            try:
                await asyncio.wait_for(after_events.__anext__(), timeout=0.7)
            except asyncio.TimeoutError:
                pass
            else:
                raise AssertionError("the retained message should be gone")

            for handle in (publisher, late, after):
                await handle.disconnect()

        run(scenario())


class TestSession:
    def test_a_session_survives_a_reconnect(self, broker, run, client_id):
        """`Session` is what makes a reconnect a resumption: the session is
        the caller's object and outlives every connection made on it."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            options = weida_mqtt.ConnectOptions(
                client_id, clean_start=False, keep_alive=30.0, session_expiry=300.0
            )
            session = weida_mqtt.Session(client_id, options)
            assert session.client_id == client_id
            assert session.in_flight == 0

            first, _ = await context.connect_session(broker, session, options)
            assert not first.session_present, "a Client Identifier no run has used"
            await first.subscribe(
                [weida_mqtt.Subscription(f"w3py/{client_id}/resume/+", 1)]
            )
            # The mirror of what the server holds, keyed by filter.
            assert session.subscriptions, session.subscriptions
            assert session.matching(f"w3py/{client_id}/resume/a")
            await first.disconnect()

            second, _ = await context.connect_session(broker, session, options)
            assert second.session_present, "the broker kept it across the reconnect"
            # The session the connection runs on is reachable from the client
            # rather than only from the caller's own variable.
            assert second.session.client_id == client_id
            await second.disconnect(session_expiry=0.0)

        run(scenario())

    def test_the_send_quota_is_the_servers_receive_maximum(
        self, broker, run, client_id
    ):
        """Two different numbers that one shared field would collapse: the
        server's `Receive Maximum` bounds this client's publishes, and this
        client's own bounds what the server may send it."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            options = weida_mqtt.ConnectOptions(
                client_id, clean_start=False, keep_alive=30.0, receive_maximum=7
            )
            session = weida_mqtt.Session(client_id, options)
            client, _ = await context.connect_session(broker, session, options)

            # `Limits::max_inflight`'s default: the library holds the quota to
            # its own ceiling whatever the server declares (65,535 when it
            # declares nothing), and this binding has no keyword to change it.
            ceiling = 1024
            assert session.receive_maximum == 7, "this client's own declaration"
            assert session.send_quota == min(
                client.server["receive_maximum"], ceiling
            ), (
                "the server's number, which replaced the placeholder "
                "([MQTT-4.9.0-1]) up to the library's own ceiling, and not "
                "the same field as the client's"
            )
            await client.disconnect(session_expiry=0.0)

        run(scenario())


class TestDeclarations:
    def test_the_connack_reaches_python(self, broker, run, client_id):
        """What the server declared, with section 11's defaults applied to
        what it left out — which is what makes an absent property safe."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            client, _ = await _connected(context, broker, client_id)
            server = client.server

            assert server["maximum_qos"] in (0, 1, 2)
            assert isinstance(server["retain_available"], bool)
            assert server["receive_maximum"] >= 1
            assert server["topic_alias_maximum"] >= 0
            assert client.client_id == client_id
            # The keep-alive in force: ours, or the server's where it sent one.
            assert client.keep_alive in (30.0, server["server_keep_alive"])
            # No TLS in this binding, and it says so rather than being absent.
            assert client.is_encrypted is False

            await client.disconnect()

        run(scenario())

    def test_a_refusal_carries_the_code_the_server_would_have_sent(
        self, broker, run, client_id
    ):
        """A declared capability refused **locally**, with the reason code on
        the exception — which is the whole reason the classes exist."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            client, _ = await _connected(context, broker, client_id)

            # A filter the grammar forbids: refused before the wire, and the
            # class names which rule.
            try:
                await client.subscribe([weida_mqtt.Subscription("a/#/b", 1)])
            except weida_mqtt.InvalidTopic as refused:
                assert refused.reason_code is None, "the grammar has no code"
                assert "#" in refused.cause
            else:
                raise AssertionError("a/#/b should be refused")

            # A Subscription Identifier of 0 is a Protocol Error
            # ([MQTT-3.8.3-4]).
            try:
                await client.subscribe(
                    [weida_mqtt.Subscription("a/b", 1)], subscription_id=0
                )
            except weida_mqtt.MqttError as refused:
                assert refused.cause
            else:
                raise AssertionError("identifier 0 should be refused")

            # An empty SUBSCRIBE ([MQTT-3.8.3-2]).
            try:
                await client.subscribe([])
            except weida_mqtt.MqttError:
                pass
            else:
                raise AssertionError("an empty SUBSCRIBE should be refused")

            await client.disconnect()

        run(scenario())

    def test_an_unsubscribe_length_mismatch_is_reported_and_not_guessed(
        self, broker, run, client_id
    ):
        """**A measured disagreement, and the first time a real broker raised
        the error variant written for it.**

        [MQTT-3.11.3-1] requires one UNSUBACK reason code per Topic Filter, in
        the order the filters were sent. rumqttd 0.20.0 answers a two-filter
        UNSUBSCRIBE with **one** code — and answers an UNSUBSCRIBE that
        removes nothing with no UNSUBACK at all.

        Position is the only thing binding a code to a filter: the
        acknowledgement carries no filters. So a count that disagrees is
        *unreadable* rather than merely odd, and this client raises
        `AcknowledgementLengthMismatch` rather than zipping short and
        mis-attributing the one code it got — which would have told a caller
        that the second filter was successfully removed with the first
        filter's code.

        One filter at a time is what interoperates with this broker, and the
        second half of this test shows it does.
        """

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            client, _ = await _connected(context, broker, client_id)
            first = f"w3py/{client_id}/a"
            second = f"w3py/{client_id}/b"
            await client.subscribe(
                [weida_mqtt.Subscription(first, 1), weida_mqtt.Subscription(second, 1)]
            )

            try:
                await client.unsubscribe([first, second])
            except weida_mqtt.AcknowledgementLengthMismatch as mismatch:
                assert "1 reason codes for 2 filters" in mismatch.cause, mismatch.cause
            else:
                raise AssertionError(
                    "measured against rumqttd 0.20.0: a two-filter UNSUBSCRIBE "
                    "is answered with one code. A broker that answers with two "
                    "has fixed it, and this assertion is how we find out"
                )

            # One filter per UNSUBSCRIBE is what interoperates with this
            # broker, and a fresh filter shows it: the two-filter call above
            # removed both — it acknowledged once for two removals — so this
            # subscribes a third and removes it alone.
            third = f"w3py/{client_id}/c"
            await client.subscribe([weida_mqtt.Subscription(third, 1)])
            codes = await client.unsubscribe([third])
            assert len(codes) == 1, codes
            assert codes[0] < 0x80, f"0x{codes[0]:02X} should not be a failure"

            await client.disconnect()

        run(scenario())


class TestFailures:
    def test_a_use_after_disconnect_is_not_connected(self, broker, run, client_id):
        """The class a caller branches on when the connection is gone."""

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            client, _ = await _connected(context, broker, client_id)
            await client.disconnect()
            try:
                await client.publish(weida_mqtt.Message("a/b", b"x", qos=1))
            except weida_mqtt.NotConnected as gone:
                assert gone.reason_code is None
            else:
                raise AssertionError("a publish after DISCONNECT should fail")

        run(scenario())

    def test_a_cancelled_publish_leaves_the_client_usable(
        self, broker, run, client_id
    ):
        """A cancelled `asyncio.Task` cancels the Rust future underneath.

        The caller stops waiting; the exchange, which is **session state**,
        survives to be retransmitted on the next connection. That asymmetry is
        the protocol's and not the binding's, and what this asserts is the half
        the binding owes: the client is still usable afterwards.
        """

        async def scenario():
            context = weida_mqtt.Context(worker_threads=1)
            client, _ = await _connected(context, broker, client_id)

            task = asyncio.ensure_future(
                client.publish(
                    weida_mqtt.Message(f"w3py/{client_id}/cancel", b"x", qos=2)
                )
            )
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

            # Still usable: the next publish completes.
            done = await client.publish(
                weida_mqtt.Message(f"w3py/{client_id}/after", b"y", qos=1)
            )
            assert done.kind == "acknowledged"
            await client.disconnect()

        run(scenario())
