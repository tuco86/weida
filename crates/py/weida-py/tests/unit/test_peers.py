"""Raw peers over the in-process transport: a peer's streams, exchanges,
sends and segments, and a queue's consumers and credit."""

import itertools

import pytest

import weida

WAIT = 5.0

_BUSES = itertools.count()


def bus() -> str:
    return f"py-peers-{next(_BUSES)}"


def client_tls() -> weida.ClientTls:
    return weida.ClientTls(weida.Trust())


def test_a_raw_peer_opens_a_stream_that_the_acceptor_reads():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    name = bus()
    url = f"weida+inproc://{name}/raw"
    listener = server.listener()
    binding = listener.bind_inproc(name)
    acceptor = listener.acceptor("/raw")
    peer = client.peer(client_tls())
    events = peer.events()
    peer.connect(url, WAIT)
    while events.recv(WAIT).kind is not weida.PeerEventKind.CONNECTED:
        pass
    events.drop()
    assert peer.peer_count() == 1
    assert len(peer.connection_stats()) == 1
    assert peer.dropped() == 0

    out = peer.open(weida.TransferMeta("text/plain"), WAIT)
    out.write_all(b"one", WAIT)
    out.finish().drop()
    stream = acceptor.accept(WAIT)
    assert stream.kind is weida.IncomingKind.STREAM
    assert isinstance(stream.transfer, weida.IncomingTransfer)
    assert (stream.request, stream.consumer, stream.flow, stream.id, stream.filter, stream.limit) == (None,) * 6
    one = stream.transfer.collect(64, WAIT)
    assert one.payload == b"one"
    assert one.meta.content_type == "text/plain"

    body, reply = peer.open_bi(None, WAIT)
    body.write_all(b"ask", WAIT)
    body.finish().drop()
    exchange = acceptor.accept(WAIT)
    assert exchange.kind is weida.IncomingKind.EXCHANGE
    assert isinstance(exchange.request, weida.IncomingRequest)
    assert exchange.request.take_body().collect(64, WAIT).payload == b"ask"
    answer = exchange.request.reply(None, WAIT)
    answer.write_all(b"answer", WAIT)
    answer.finish().drop()
    assert reply.recv(WAIT).collect(64, WAIT).payload == b"answer"

    peer.send(None, b"sent", WAIT)
    sent = acceptor.accept(WAIT)
    assert sent.kind is weida.IncomingKind.STREAM
    assert sent.transfer.collect(64, WAIT).payload == b"sent"

    segment = peer.segment("cam", WAIT)
    assert segment.number() == 0
    segment.write(b"frame")
    segment.finish()
    frame = acceptor.accept(WAIT)
    assert frame.kind is weida.IncomingKind.STREAM
    meta = frame.transfer.meta()
    assert (meta.topic, meta.segment) == ("cam", 0)
    assert frame.transfer.collect(64, WAIT).payload == b"frame"
    assert peer.segment_drops("cam") is None

    assert peer.disconnect(url) is True
    assert peer.disconnect(url) is False
    assert peer.peer_count() == 0
    with pytest.raises(weida.NotConnected):
        peer.open(None, WAIT)

    for handle in (peer, acceptor, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_credit_granted_in_two_steps_releases_exactly_that_many_deliveries():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    name = bus()
    listener = server.listener()
    binding = listener.bind_inproc(name)
    acceptor = listener.acceptor("/q")
    subscriber = client.subscriber(client_tls())
    subscriber.connect(f"weida+inproc://{name}/q", WAIT)
    subscriber.subscribe("jobs.#", WAIT)

    subscribed = acceptor.accept(WAIT)
    assert subscribed.kind is weida.IncomingKind.SUBSCRIBED
    assert isinstance(subscribed.consumer, weida.Consumer)
    consumer = subscribed.consumer
    consumer_id = consumer.id()
    assert isinstance(consumer_id, int)
    assert consumer.filter() == "jobs.#"
    assert consumer.path() == "/q"

    # Each delivery is its own stream, so they may arrive in any order; and
    # nothing beyond the credit arrives.
    def receive(n):
        got = [subscriber.recv(WAIT).collect(64, WAIT).payload for _ in range(n)]
        with pytest.raises(weida.Again):
            subscriber.recv(0.1)
        return sorted(got)

    none = dict(transfer=None, request=None, consumer=None, flow=None)
    credit = weida.IncomingKind.CREDIT
    unsubscribed = weida.IncomingKind.UNSUBSCRIBED

    # The test is the queue that honours the credit.
    subscriber.grant("jobs.#", 2, WAIT)
    assert acceptor.accept(WAIT) == weida.Incoming(kind=credit, **none, id=consumer_id, filter="jobs.#", limit=2)
    out = consumer.open(weida.TransferMeta(topic="jobs.a"), WAIT)
    out.write_all(b"job-0", WAIT)
    out.finish().drop()
    consumer.deliver(weida.TransferMeta(topic="jobs.a"), b"job-1", WAIT)
    assert receive(2) == [b"job-0", b"job-1"]

    subscriber.grant("jobs.#", 5, WAIT)
    assert acceptor.accept(WAIT) == weida.Incoming(kind=credit, **none, id=consumer_id, filter="jobs.#", limit=5)
    for n in (2, 3, 4):
        consumer.deliver(weida.TransferMeta(topic="jobs.a"), f"job-{n}".encode(), WAIT)
    assert receive(3) == [b"job-2", b"job-3", b"job-4"]

    subscriber.unsubscribe("jobs.#", WAIT)
    assert acceptor.accept(WAIT) == weida.Incoming(
        kind=unsubscribed, **none, id=consumer_id, filter="jobs.#", limit=None
    )

    subscriber.subscribe("other", WAIT)
    second = acceptor.accept(WAIT).consumer
    assert second.id() == consumer_id
    second.close(6, "done")
    while True:
        arrived = acceptor.accept(WAIT)
        if arrived.kind is unsubscribed:
            assert arrived == weida.Incoming(kind=unsubscribed, **none, id=consumer_id, filter=None, limit=None)
            break
        # The redialled connection subscribing again, under a new number.
        assert arrived.kind is weida.IncomingKind.SUBSCRIBED
        assert arrived.consumer.id() != consumer_id
        arrived.consumer.drop()

    for handle in (consumer, second, subscriber, acceptor, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)
