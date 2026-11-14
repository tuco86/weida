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


def test_a_pair_exchanges_in_both_directions():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    name, listener, binding = bound(server)
    url = f"weida+inproc://{name}/link"
    near = listener.pair("/link")
    assert near.path() == "/link"
    with pytest.raises(weida.Unsupported):
        near.connect(url, WAIT)
    assert near.events() is None
    far = client.pair(client_tls())
    assert far.path() == ""
    far.connect(url, WAIT)
    with pytest.raises(weida.LimitExceeded):
        far.connect(url, WAIT)
    assert far.peer_count() == 1

    far.send(b"ping", WAIT)
    assert near.recv(WAIT).collect(1024, WAIT).payload == b"ping"
    near.send_with(weida.TransferMeta("text/plain"), b"pong", WAIT)
    pong = far.recv(WAIT)
    assert pong.meta().content_type == "text/plain"
    assert pong.collect(1024, WAIT).payload == b"pong"
    assert far.dropped() == 0

    for handle in (far, near, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_survey_collects_two_respondents_answers_within_its_window():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    ats = [bound(server), bound(server)]
    respondents = [listener.respondent("/poll") for _, listener, _ in ats]
    surveyor = client.surveyor(client_tls())
    for name, _, _ in ats:
        surveyor.connect(f"weida+inproc://{name}/poll", WAIT)
    assert surveyor.peer_count() == 2

    # The survey returns once both are asked; the answers wait in the run.
    run = surveyor.survey(b"who is there", 60.0, WAIT)
    for respondent, answer in zip(respondents, (b"a", b"b")):
        request = respondent.accept(WAIT)
        assert request.take_body().collect(1024, WAIT).payload == b"who is there"
        reply = request.reply(None, WAIT)
        reply.write_all(answer, WAIT)
        reply.finish().delivered(WAIT)
    assert run.respondents() == 2
    assert sorted([run.next(1024, WAIT), run.next(1024, WAIT)]) == [b"a", b"b"]
    assert run.next(1024, WAIT) is None
    assert run.late() == 0

    for handle in (run, surveyor, *respondents):
        handle.drop()
    for _, listener, binding in ats:
        binding.drop()
        listener.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_bus_message_reaches_every_other_member():
    runtimes = [weida.Runtime.owned() for _ in range(3)]
    ats = [bound(runtime) for runtime in runtimes]
    members = [listener.bus("/bus", client_tls()) for _, listener, _ in ats]
    for i, member in enumerate(members):
        for j, (name, _, _) in enumerate(ats):
            if j != i:
                member.connect(f"weida+inproc://{name}/bus", WAIT)

    for i, member in enumerate(members):
        assert member.send(f"from {i}".encode(), WAIT) == 2
    for i, member in enumerate(members):
        seen = sorted(member.recv(WAIT).collect(1024, WAIT).payload for _ in range(2))
        assert seen == [f"from {j}".encode() for j in range(3) if j != i]
    for member in members:
        with pytest.raises(weida.Again):
            member.recv(0.1)
        assert member.dropped() == 0

    for member in members:
        member.drop()
    for _, listener, binding in ats:
        binding.drop()
        listener.drop()
    for runtime in runtimes:
        runtime.shutdown(WAIT)


def radio_dish(server, client, topic: str, **terms):
    """A radio at `/r` on `server` and a dish of `topic` on `client`, joined
    on `terms`: `(listener, binding, url, radio, dish)`."""
    name, listener, binding = bound(server)
    radio = listener.radio("/r")
    dish = client.dish(client_tls())
    dish.join(topic, WAIT, **terms)
    url = f"weida+inproc://{name}/r"
    dish.connect(url, WAIT)
    return listener, binding, url, radio, dish


def await_dishes(radio, n: int) -> None:
    """Waits until `n` dishes joined `radio`."""
    while radio.dish_count() != n:
        time.sleep(0.001)


def next_segment(dish):
    """`(segment, layer, body)` of the next stream segment `dish` receives."""
    r = dish.recv(WAIT)
    assert r.kind == weida.ReceivedKind.SEGMENT
    meta = r.transfer.meta()
    return (meta.segment, meta.layer, r.transfer.collect(1024, WAIT).payload)


def test_a_dish_receives_the_radios_segments_with_their_numbers_and_layers():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    listener, binding, _, radio, dish = radio_dish(server, client, "v", max_layer=1)
    await_dishes(radio, 1)
    assert weida.MAX_SEGMENT_LAYERS == 16

    first = radio.segment("v")
    assert first.number() == 0
    assert first.topic() == "v"
    assert first.write_layer(0, b"a0") == 1
    assert first.write_layer(1, b"a1") == 1
    assert first.write_layer(2, b"a2") == 0, "the copy is capped at layer 1"
    assert first.finish() == 1
    seen = [next_segment(dish), next_segment(dish)]

    second = radio.segment("v", priority=1)
    assert second.number() == 1
    assert second.write(b"b0") == 1
    assert second.finish() == 1
    seen.append(next_segment(dish))
    assert sorted(seen) == [(0, 0, b"a0"), (0, 1, b"a1"), (1, 0, b"b0")]
    assert radio.dropped() == 0
    assert dish.stale() == 0
    assert dish.overflow() == 0

    for handle in (dish, radio, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_superseded_segment_counts_in_dish_drops():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    listener, binding, _, radio, dish = radio_dish(server, client, "v")
    await_dishes(radio, 1)

    first = radio.segment("v")
    assert first.write(b"first") == 1
    second = radio.segment("v")
    assert second.write(b"second") == 1
    assert second.finish() == 1
    while not (drops := radio.dish_drops()) or drops[0].superseded != 1:
        time.sleep(0.001)
    with pytest.raises(dataclasses.FrozenInstanceError):
        drops[0].superseded = 0
    assert radio.dropped_on("v").superseded == 1
    assert first.finish() == 0, "the copy was reset"

    while True:
        r = dish.recv(WAIT)
        assert r.kind == weida.ReceivedKind.SEGMENT
        if r.transfer.meta().segment == 1:
            assert r.transfer.collect(1024, WAIT).payload == b"second"
            break
        with pytest.raises(weida.WeidaError):
            r.transfer.collect(1024, WAIT)

    for handle in (dish, radio, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_radio_datagram_reaches_a_dish_that_carries_datagrams():
    datagrams = weida.RuntimeConfig(limits=weida.Limits(datagram_receive_bytes=65536))
    server = weida.Runtime.owned(datagrams)
    near = weida.Runtime.owned(datagrams)
    far = weida.Runtime.owned()
    listener, binding, url, radio, carrying = radio_dish(server, near, "voice")
    plain = far.dish(client_tls())
    plain.join("voice", WAIT)
    plain.connect(url, WAIT)
    await_dishes(radio, 2)

    assert radio.datagram("voice", b"hello") == 1
    r = carrying.recv(WAIT)
    assert r.kind == weida.ReceivedKind.DATAGRAM
    assert (r.topic, r.segment, r.payload, r.transfer) == ("voice", 0, b"hello", None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.payload = b""
    assert radio.dropped_on("voice").no_datagrams == 1
    assert sorted(d.no_datagrams for d in radio.dish_drops()) == [0, 1]

    for handle in (carrying, plain, radio, binding, listener):
        handle.drop()
    near.shutdown(WAIT)
    far.shutdown(WAIT)
    server.shutdown(WAIT)
