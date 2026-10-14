"""The synchronous surface, against the same scripted server.

`weida_nats.sync` drives the *same* futures `weida_nats` awaits, on the
calling thread, with the GIL released. So the assertions here are about the
two things a second surface can get wrong:

* that it is the same client — one `Message` class, one exception family, the
  same three outcomes of `request`, and no protocol behaviour written twice.
  `test_the_two_surfaces_agree` is the one that would fail if it were not;
* that nothing in it can hang. `next_msg` takes a window and raises
  `weida_nats.RequestTimeout` when it closes, which is the same class the
  library raises for a request's window, because it is the same fact.

No asyncio loop exists in this file except inside the scripted server's own
thread, which is the server and not the client.
"""

import asyncio
import threading
import time

import pytest

import scripted
import weida_nats
from weida_nats import sync


@pytest.fixture
def server():
    running = scripted.Server.start()
    try:
        yield running
    finally:
        running.stop()


def connected(server, **options):
    return sync.connect("127.0.0.1", server.port, **options)


# --------------------------------------------------------------------------
# The round trip, with no loop anywhere


def test_a_synchronous_round_trip_needs_no_event_loop(server):
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()

    nats = connected(server)
    assert nats.state().name == "connected"
    assert nats.headers_supported()
    assert nats.info().server_name == "scripted"

    orders = nats.subscribe("orders.created")
    nats.publish("orders.created", b"one")
    message = orders.next_msg(2.0)

    assert message.payload == b"one"
    assert message.subject_str == "orders.created"
    assert message.sid == orders.sid()
    assert isinstance(message, weida_nats.Message)
    nats.close()
    assert nats.state().name == "closed"

    assert [op.render() for op in server.ops_until("PUB")] == [
        "CONNECT",
        "PING",
        "SUB orders.created - 1",
        "PUB orders.created - one",
    ]


def test_the_two_wildcards_reach_the_right_subscription(server):
    nats = connected(server)
    one_token = nats.subscribe("orders.*")
    the_rest = nats.subscribe("orders.>")

    nats.publish("orders.created", b"shallow")
    nats.publish("orders.eu.created", b"deep")

    assert one_token.next_msg(2.0).payload == b"shallow"
    assert the_rest.next_msg(2.0).payload == b"shallow"
    assert the_rest.next_msg(2.0).payload == b"deep"
    # `*` is one token, so the deep subject never reached it.
    with pytest.raises(weida_nats.RequestTimeout):
        one_token.next_msg(0.2)
    nats.close()


def test_a_queue_group_travels_in_the_sub_line(server):
    nats = connected(server)
    workers = nats.subscribe_with_queue_group("work.>", "builders")
    second = nats.subscribe_with_queue_group("work.>", "builders")
    assert workers.queue_group() == b"builders"

    nats.publish("work.item", b"job")
    assert workers.next_msg(2.0).payload == b"job"
    # One delivery per group, not one per member.
    assert second.try_next() is None
    nats.close()

    subs = [op.render() for op in server.ops_until("PUB") if op.verb == "SUB"]
    assert subs == ["SUB work.> builders 1", "SUB work.> builders 2"]


def test_unsubscribe_after_one_ends_the_subscription(server):
    nats = connected(server)
    orders = nats.subscribe("orders.>")
    orders.unsubscribe_after(1)
    nats.flush()

    nats.publish("orders.a", b"1")
    assert orders.next_msg(2.0).payload == b"1"
    # An ended subscription is a value, not a failure: `None` rather than the
    # timeout class, so that a caller that retries can tell them apart.
    assert orders.next_msg(2.0) is None
    nats.close()
    assert "UNSUB 1 1" in [op.render() for op in server.ops_until("PUB")]


def test_next_msg_times_out_rather_than_blocking_forever(server):
    nats = connected(server)
    quiet = nats.subscribe("orders.>")
    started = time.monotonic()
    with pytest.raises(weida_nats.RequestTimeout) as expired:
        quiet.next_msg(0.25)
    assert 0.2 < time.monotonic() - started < 5.0
    assert expired.value.errno == "RequestTimeout"
    nats.close()


def test_a_bad_timeout_is_refused_where_it_was_given(server):
    nats = connected(server)
    quiet = nats.subscribe("orders.>")
    with pytest.raises(weida_nats.Configuration):
        quiet.next_msg(-1.0)
    with pytest.raises(weida_nats.Configuration):
        nats.request("orders.a", b"x", float("inf"))
    nats.close()


# --------------------------------------------------------------------------
# The request-reply of the asyncio surface, unchanged, in synchronous form


def test_request_is_answered(server):
    # The responder is the server here: a synchronous `request` parks the
    # calling thread, so the answer cannot come from this one.
    server.respond_to("service.echo", b"pong")
    nats = connected(server)

    reply = nats.request("service.echo", b"ping", 2.0)
    assert reply.payload == b"pong"
    assert reply.subject_str.startswith("_INBOX.")
    nats.close()


def test_a_request_without_a_responder_times_out(server):
    nats = connected(server)
    # Somebody is subscribed, so there is no 503 — the window simply closes.
    listening = nats.subscribe("service.slow")
    nats.flush()
    with pytest.raises(weida_nats.RequestTimeout) as timeout:
        nats.request("service.slow", b"ping", 0.25)
    assert not isinstance(timeout.value, weida_nats.NoResponders)
    assert listening.sid() == 1
    nats.close()


def test_a_503_is_no_responders_and_not_the_timeout_class(server):
    nats = connected(server)
    started = time.monotonic()
    with pytest.raises(weida_nats.NoResponders) as refused:
        # A generous window: 503 does not wait for it.
        nats.request("nobody.listening", b"ping", 30.0)
    assert time.monotonic() - started < 5.0
    assert not isinstance(refused.value, weida_nats.RequestTimeout)
    nats.close()


def test_request_many_collects_what_arrived_in_the_window(server):
    server.respond_to("service.fan", b"one")
    server.respond_to("service.fan", b"two")
    nats = connected(server)

    replies = nats.request_many("service.fan", b"ping", 2.0, 2)
    assert [reply.payload for reply in replies] == [b"one", b"two"]
    nats.close()


# --------------------------------------------------------------------------
# Headers


def test_headers_cross_in_both_directions(server):
    nats = connected(server)
    traced = nats.subscribe("orders.>")
    nats.publish_with(
        "orders.created",
        headers=[("Trace-Id", "abc"), ("Link", "a"), ("Link", "b")],
        payload=b"body",
    )
    message = traced.next_msg(2.0)
    assert message.headers == [("Trace-Id", "abc"), ("Link", "a"), ("Link", "b")]
    assert message.payload == b"body"
    nats.close()

    written = [op for op in server.ops_until("HPUB") if op.verb == "HPUB"]
    assert written[0].headers == [("Trace-Id", "abc"), ("Link", "a"), ("Link", "b")]


# --------------------------------------------------------------------------
# The GIL, and the reactor


def test_the_gil_is_released_while_a_call_blocks(server):
    """A second Python thread runs while the first is parked in `next_msg`."""
    nats = connected(server)
    quiet = nats.subscribe("orders.>")
    ticks = []
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            ticks.append(time.monotonic())
            time.sleep(0.01)

    thread = threading.Thread(target=ticker)
    thread.start()
    try:
        with pytest.raises(weida_nats.RequestTimeout):
            quiet.next_msg(0.3)
    finally:
        stop.set()
        thread.join()
    # Had `next_msg` held the GIL for its 300 ms, the ticker could not run.
    assert len(ticks) > 5, ticks
    nats.close()


def test_a_second_connection_can_share_the_first_ones_reactor(server):
    other = scripted.Server.start()
    try:
        first = connected(server)
        second = sync.connect_sharing(first, "127.0.0.1", other.port)
        subscription = second.subscribe("orders.>")
        second.publish("orders.a", b"shared")
        assert subscription.next_msg(2.0).payload == b"shared"
        second.close()
        first.close()
    finally:
        other.stop()


def test_the_submodule_is_importable_under_its_dotted_name():
    import weida_nats.sync as dotted

    assert dotted is sync


# --------------------------------------------------------------------------
# One client, two surfaces


def test_the_two_surfaces_agree(server):
    """No test may pass on one surface and fail on the other.

    The same three outcomes of `request`, the same classes for them, and the
    same `Message` type — because both surfaces call the same library. This
    runs the asynchronous half in an event loop and the synchronous half
    without one, against two servers of the same script, and compares.
    """
    other = scripted.Server.start()
    try:
        blocking = connected(server)
        blocking_reply = None
        with pytest.raises(weida_nats.NoResponders):
            blocking.request("nobody.listening", b"?", 5.0)
        server.respond_to("service.echo", b"pong")
        blocking_reply = blocking.request("service.echo", b"ping", 2.0)
        blocking.close()

        async def coroutines():
            other.respond_to("service.echo", b"pong")
            nats = await weida_nats.connect("127.0.0.1", other.port)
            with pytest.raises(weida_nats.NoResponders):
                await nats.request("nobody.listening", b"?", 5.0)
            reply = await nats.request("service.echo", b"ping", 2.0)
            await nats.close()
            return reply

        awaited_reply = asyncio.run(coroutines())

        assert type(blocking_reply) is type(awaited_reply) is weida_nats.Message
        assert blocking_reply.payload == awaited_reply.payload == b"pong"
        assert blocking_reply.status is awaited_reply.status is None
    finally:
        other.stop()
