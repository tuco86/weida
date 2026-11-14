"""Exchanges with a streamed request body over the in-process transport."""

import itertools
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida

WAIT = 5.0

_BUSES = itertools.count()


def bus() -> str:
    return f"py-streams-{next(_BUSES)}"


class Pair:
    """A runtime with a replier at `/echo` and a requester connected to it
    over a bus of its own."""

    def __init__(self) -> None:
        name = bus()
        self.runtime = weida.Runtime.owned()
        self.listener = self.runtime.listener()
        self.binding = self.listener.bind_inproc(name)
        self.replier = self.listener.replier("/echo")
        self.requester = self.runtime.requester(weida.ClientTls(weida.Trust()))
        self.requester.connect(f"weida+inproc://{name}/echo", WAIT)

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


def test_an_opened_exchange_reads_its_reply_while_the_request_is_still_open(pair):
    body, reply = pair.requester.open(weida.TransferMeta("text/plain"), WAIT)
    for piece in (b"one", b"two", b"three"):
        body.write_all(piece, WAIT)
    request = pair.replier.accept(WAIT)
    assert request.meta().content_type == "text/plain"
    received = request.take_body()
    answer = request.reply(None, WAIT)
    answer.write_all(b"ack", WAIT)
    answer.finish().drop()
    assert reply.recv(WAIT).collect(1024, WAIT).payload == b"ack"
    with pytest.raises(weida.Closed):
        reply.recv(0)  # recv consumed the stream
    body.finish().drop()
    pieces = []
    while piece := received.read(4, WAIT):
        pieces.append(piece)
    assert b"".join(pieces) == b"onetwothree"


def test_dropping_a_reply_stream_tells_the_replier_nobody_is_listening(pair, pool):
    body, reply = pair.requester.open(None, WAIT)
    request = pair.replier.accept(WAIT)
    with pytest.raises(weida.Again):
        request.canceled(0)  # the requester still listens
    watching = pool.submit(request.canceled, WAIT)
    reply.drop()
    watching.result(WAIT)
    request.drop()
    body.drop()
