"""Connection statistics, the runtime's counters, connection numbers and peer
events over the in-process transport."""

import dataclasses
import itertools
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida

WAIT = 5.0

_BUSES = itertools.count()


def bus() -> str:
    return f"py-stats-{next(_BUSES)}"


class Pair:
    """A runtime with a replier at `/echo` and a requester connected to it
    over a bus of its own."""

    def __init__(self) -> None:
        name = bus()
        self.url = f"weida+inproc://{name}/echo"
        self.runtime = weida.Runtime.owned()
        self.listener = self.runtime.listener()
        self.binding = self.listener.bind_inproc(name)
        self.replier = self.listener.replier("/echo")
        self.requester = self.runtime.requester(weida.ClientTls(weida.Trust()))
        self.requester.connect(self.url, WAIT)

    def close(self) -> None:
        for handle in (self.requester, self.replier, self.binding, self.listener):
            handle.drop()
        self.runtime.shutdown(WAIT)


@pytest.fixture
def pair():
    p = Pair()
    yield p
    p.close()


@pytest.fixture
def pool():
    with ThreadPoolExecutor(max_workers=1) as executor:
        yield executor


def test_an_inproc_connection_is_reported_with_its_url_and_no_transport(pair, pool):
    def serve() -> None:
        request = pair.replier.accept(WAIT)
        number = request.connection()
        assert isinstance(number, int)
        body = request.take_body()
        assert body.connection() == number
        body.drop()
        out = request.reply(None, WAIT)
        out.write_all(b"pong", WAIT)
        out.finish().drop()

    served = pool.submit(serve)
    assert pair.requester.request(b"ping", WAIT).collect(64, WAIT).payload == b"pong"
    served.result(WAIT)

    stats = pair.requester.connection_stats()
    assert isinstance(stats, tuple)
    assert len(stats) == 1
    (record,) = stats
    assert isinstance(record, weida.ConnectionStats)
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.url = "x"
    assert record.url == pair.url
    assert record.redials == 0
    assert isinstance(record.age, float)
    assert record.age >= 0
    assert record.transport is None
    assert record.remote is None
    assert pair.runtime.suppressed_duplicates() == 0


def test_expired_transfers_counts_a_reply_expired_with_expire_after_0(pair, pool):
    assert pair.runtime.expired_transfers() == 0

    def serve() -> None:
        request = pair.replier.accept(WAIT)
        out = request.reply(None, WAIT)
        out.expire_after(0)
        with pytest.raises(weida.Expired):
            out.finish()

    served = pool.submit(serve)
    try:
        reply = pair.requester.request(b"go", WAIT)
        with pytest.raises(weida.Canceled):
            reply.collect(64, WAIT)
    finally:
        served.result(WAIT)
    assert pair.runtime.expired_transfers() == 1


def test_a_replier_runtime_shut_down_yields_lost_then_retrying():
    name = bus()
    url = f"weida+inproc://{name}/echo"
    server = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_inproc(name)
    replier = listener.replier("/echo")
    client = weida.Runtime.owned()
    requester = client.requester(weida.ClientTls(weida.Trust()))
    requester.connect(url, WAIT)

    events = requester.events()
    it = iter(events)
    for handle in (replier, binding, listener):
        handle.drop()
    server.shutdown(WAIT)

    lost = next(it)
    assert lost.kind is weida.PeerEventKind.LOST
    assert lost.url == url
    assert isinstance(lost.cause, weida.LossCause)
    retrying = next(it)
    assert retrying.kind is weida.PeerEventKind.RETRYING
    assert retrying.attempt == 1
    assert isinstance(retrying.delay, float)
    assert retrying.cause is None

    requester.drop()
    assert next(it, None) is None, "a requester that is gone ends its events"
    assert events.recv(WAIT) is None
    events.drop()
    client.shutdown(WAIT)
