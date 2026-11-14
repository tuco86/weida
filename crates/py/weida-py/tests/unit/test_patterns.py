"""The patterns over the in-process transport."""

import dataclasses
import itertools
import time

import pytest

import weida

WAIT = 5.0

_BUSES = itertools.count()


def bus() -> str:
    return f"py-patterns-{next(_BUSES)}"


def client_tls():
    return weida.ClientTls(weida.Trust())


def bound(runtime):
    """A listener of `runtime` bound to a bus of its own: `(bus, listener,
    binding)`."""
    name = bus()
    listener = runtime.listener()
    return name, listener, listener.bind_inproc(name)


def await_filters(publisher, n: int) -> None:
    """Waits until `publisher` holds `n` filters: the subscriptions arrived."""
    while publisher.filter_count() != n:
        time.sleep(0.001)


class PubSub:
    """A publisher at `/md` on `server` and a subscriber of `px.#` on a
    runtime of its own, made from `client_config`."""

    def __init__(self, server, client_config=None) -> None:
        self.server = server
        name, self.listener, self.binding = bound(server)
        self.publisher = self.listener.publisher("/md")
        self.client = weida.Runtime.owned(client_config)
        self.subscriber = self.client.subscriber(client_tls())
        self.subscriber.connect(f"weida+inproc://{name}/md", WAIT)
        self.subscriber.subscribe("px.#", WAIT)
        await_filters(self.publisher, 1)

    def close(self) -> None:
        for handle in (self.subscriber, self.publisher, self.binding, self.listener):
            handle.drop()
        self.client.shutdown(WAIT)
        self.server.shutdown(WAIT)


def test_push_to_pull_over_inproc_arrives_in_order():
    config = weida.RuntimeConfig(
        guarantees=weida.GuaranteeSet(ordering=weida.OrderingMode.PER_PRODUCER_REASSEMBLE)
    )
    server = weida.Runtime.owned(config)
    client = weida.Runtime.owned(config)
    name, listener, binding = bound(server)
    puller = listener.puller("/jobs")
    assert puller.path() == "/jobs"
    pusher = client.pusher(client_tls())
    pusher.connect(f"weida+inproc://{name}/jobs", WAIT)
    assert pusher.peer_count() == 1

    # The first number a connection sees sets its position, and two streams
    # may reach the puller in either order: "one" is taken before the rest
    # is sent, so what follows is ordered against it.
    def expect(n, body, content_type):
        transfer = puller.recv(WAIT)
        meta = transfer.meta()
        assert meta.sequence == n
        assert meta.gap is None
        assert meta.content_type == content_type
        assert transfer.collect(1024, WAIT).payload == body

    pusher.send(b"one", WAIT)
    expect(0, b"one", None)
    pusher.send(b"two", WAIT)
    pusher.send_with(weida.TransferMeta("application/json"), b"three", WAIT)
    expect(1, b"two", None)
    expect(2, b"three", "application/json")

    out = pusher.open(weida.TransferMeta("text/plain"), WAIT)
    out.write_all(b"four", WAIT)
    delivery = out.finish()
    fourth = puller.recv(WAIT)
    assert fourth.meta().content_type == "text/plain"
    assert fourth.collect(1024, WAIT).payload == b"four"
    delivery.delivered(WAIT)
    assert pusher.dropped() == 0

    for handle in (pusher, puller, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_subscriber_receives_only_its_topic():
    server = weida.Runtime.owned()
    name, listener, binding = bound(server)
    publisher = listener.publisher("/md")
    url = f"weida+inproc://{name}/md"
    # One runtime per subscriber: two on one runtime collide on the path.
    clients = [weida.Runtime.owned(), weida.Runtime.owned()]
    px, fx = (c.subscriber(client_tls()) for c in clients)
    px.connect(url, WAIT)
    fx.connect(url, WAIT)
    px.subscribe("px.#", WAIT)
    fx.subscribe("fx.#", WAIT)
    with pytest.raises(weida.Protocol):
        px.subscribe("#.px", WAIT)
    await_filters(publisher, 2)
    assert publisher.subscriber_count() == 2

    assert publisher.publish("px.eur", b"price") == 1
    trace = weida.new_trace()
    assert publisher.publish_with_trace("fx.usd", b"rate", trace) == 1
    price = px.recv(WAIT)
    assert price.meta().topic == "px.eur"
    assert price.collect(1024, WAIT).payload == b"price"
    rate = fx.recv(WAIT)
    assert rate.meta().topic == "fx.usd"
    assert rate.meta().trace == trace
    assert rate.collect(1024, WAIT).payload == b"rate"
    for s in (px, fx):
        with pytest.raises(weida.Again):
            s.recv(0.1)

    for handle in (px, fx, publisher, binding, listener):
        handle.drop()
    for c in clients:
        c.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_skipped_sequence_reads_as_a_gap_under_per_producer_detect():
    MSG = 32 * 1024
    detect = weida.GuaranteeSet(ordering=weida.OrderingMode.PER_PRODUCER_DETECT)
    # A budget of two copies, and a stream window the first copy overflows:
    # its writer stalls, the second fills the budget, the third is dropped.
    server = weida.Runtime.owned(
        weida.RuntimeConfig(guarantees=detect, limits=weida.Limits(subscriber_buffer_bytes=2 * MSG))
    )
    p = PubSub(
        server,
        weida.RuntimeConfig(guarantees=detect, limits=weida.Limits(stream_receive_window=8192)),
    )
    payload = bytes(MSG)

    published = 0
    while p.publisher.dropped() == 0:
        p.publisher.publish("px.eur", payload)
        published += 1
        time.sleep(0.001)
    lost = p.publisher.dropped()
    assert published > lost, "the first copy was deliverable"
    (drops,) = p.publisher.drops()
    assert drops.topic == "px.eur"
    assert drops.subscriber_budget == lost
    with pytest.raises(dataclasses.FrozenInstanceError):
        drops.topic = "x"
    assert p.publisher.dropped_on("fx.none") is None

    for _ in range(published - lost):
        copy = p.subscriber.recv(WAIT)
        assert copy.meta().gap is None, "the lost copies were the last ones"
        assert len(copy.collect(MSG, WAIT).payload) == MSG

    revealing = None
    while revealing is None:
        p.publisher.publish("px.eur", payload)
        try:
            revealing = p.subscriber.recv(0.1)
        except weida.Again:
            pass
    gap = revealing.meta().gap
    assert isinstance(gap, weida.Gap)
    assert gap.seen - gap.expected >= lost, f"{gap.seen} - {gap.expected} < {lost}"
    with pytest.raises(dataclasses.FrozenInstanceError):
        gap.seen = 0
    revealing.drop()
    p.close()


def test_a_fan_out_streams_one_payload_to_every_subscriber():
    p = PubSub(weida.Runtime.owned())
    fan = p.publisher.open("px.frame")
    assert fan.topic() == "px.frame"
    assert fan.subscribers() == 1
    assert fan.write_within(b"abc", 1.0, WAIT) == 1
    assert fan.write_now(b"def") == 1
    assert fan.finish() == 1
    with pytest.raises(weida.Closed):
        fan.finish()
    copy = p.subscriber.recv(WAIT)
    assert copy.meta().topic == "px.frame"
    assert copy.collect(1024, WAIT).payload == b"abcdef"
    p.close()
