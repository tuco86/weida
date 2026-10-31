"""The synchronous surface against real QUIC and the in-process transport."""

import os
import signal
import sys
import threading

import pytest

import weida

WAIT = 5.0


class Pair:
    """A runtime with a replier at `/echo` and a requester connected to it,
    over QUIC with a pinned key or over an in-process bus."""

    def __init__(self, transport: str, bus: str, identity: weida.Identity) -> None:
        self.runtime = weida.Runtime.owned()
        self.listener = self.runtime.listener()
        if transport == "quic":
            self.binding = self.listener.bind_quic("127.0.0.1:0", weida.ServerTls(identity), WAIT)
            fingerprint = identity.fingerprint()
            trust = weida.Trust().add_pin(fingerprint)
            url = f"weida://{fingerprint}@{self.binding.local_addr()}/echo"
        else:
            self.binding = self.listener.bind_inproc(bus)
            trust = weida.Trust()
            url = f"weida+inproc://{bus}/echo"
        self.replier = self.listener.replier("/echo")
        self.requester = self.runtime.requester(weida.ClientTls(trust))
        self.requester.connect(url, WAIT)

    def close(self) -> None:
        self.requester.drop()
        self.replier.drop()
        self.binding.drop()
        self.listener.drop()
        self.runtime.shutdown(WAIT)


@pytest.fixture(params=["quic", "inproc"])
def pair(request, bus, server_identity):
    pair = Pair(request.param, bus, server_identity)
    yield pair
    pair.close()


@pytest.fixture
def inproc(bus, server_identity):
    pair = Pair("inproc", bus, server_identity)
    yield pair
    pair.close()


def serve(replier: weida.Replier, answer, content_type=None) -> threading.Thread:
    """Answers one request in a thread with `answer(body) -> bytes`."""

    def run() -> None:
        request = replier.accept(WAIT)
        body = request.take_body()
        received = b""
        while chunk := body.read(3, WAIT):
            received += chunk
        reply = request.reply(weida.TransferMeta(content_type), WAIT)
        reply.write_all(answer(received), WAIT)
        reply.finish().delivered(WAIT)

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def test_a_streamed_round_trip(pair):
    server = serve(pair.replier, lambda body: body.upper(), "text/plain")
    reply = pair.requester.request(b"ping", WAIT)
    assert reply.meta().content_type == "text/plain"
    message = reply.collect(1024, WAIT)
    assert message.payload == b"PING"
    assert message.meta.content_type == "text/plain"
    server.join(WAIT)


def test_an_accept_with_nothing_pending_raises_again(inproc):
    with pytest.raises(weida.Again):
        inproc.replier.accept(timeout=0)


def test_small_reads_reassemble_the_body(inproc):
    thread = threading.Thread(target=lambda: _request_quietly(inproc.requester, b"0123456789"))
    thread.start()
    request = inproc.replier.accept(WAIT)
    body = request.take_body()
    pieces = []
    while piece := body.read(3, WAIT):
        assert len(piece) <= 3
        pieces.append(piece)
    assert b"".join(pieces) == b"0123456789"
    assert body.read(3, WAIT) == b""
    request.drop()
    thread.join(WAIT)


def test_a_collect_over_its_ceiling_raises_limit_exceeded(inproc):
    thread = threading.Thread(target=lambda: _request_quietly(inproc.requester, b"four"))
    thread.start()
    request = inproc.replier.accept(WAIT)
    with pytest.raises(weida.LimitExceeded):
        request.take_body().collect(2, WAIT)
    request.drop()
    thread.join(WAIT)


def _request_quietly(requester: weida.Requester, body: bytes) -> None:
    """A request whose answer the test does not look at."""
    try:
        requester.request(body, WAIT).collect(1024, WAIT)
    except weida.WeidaError:
        pass


def test_a_reply_dropped_unfinished_reads_as_canceled(inproc):
    def run() -> None:
        request = inproc.replier.accept(WAIT)
        reply = request.reply(None, WAIT)
        reply.write_all(b"ha", WAIT)
        reply.drop()

    thread = threading.Thread(target=run)
    thread.start()
    reply = inproc.requester.request(b"go", WAIT)
    with pytest.raises(weida.Canceled):
        while reply.read(64, WAIT):
            pass
    thread.join(WAIT)


def test_a_second_reply_raises_closed(inproc):
    thread = threading.Thread(target=lambda: _request_quietly(inproc.requester, b"once"))
    thread.start()
    request = inproc.replier.accept(WAIT)
    request.reply(None, WAIT).drop()
    with pytest.raises(weida.Closed):
        request.reply(None, WAIT)
    thread.join(WAIT)


def test_a_wrong_pin_raises_untrusted(server_identity):
    runtime = weida.Runtime.owned()
    listener = runtime.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(server_identity), WAIT)
    replier = listener.replier("/echo")
    other = weida.Identity.generate()
    requester = runtime.requester(weida.ClientTls(weida.Trust().add_pin(other.fingerprint())))
    with pytest.raises(weida.Untrusted) as caught:
        requester.connect(f"weida://{binding.local_addr()}/echo", WAIT)
        requester.request(b"hello", WAIT)
    assert caught.value.fingerprint == server_identity.fingerprint()
    for handle in (requester, replier, binding, listener):
        handle.drop()
    runtime.shutdown(WAIT)


@pytest.mark.skipif(sys.platform == "win32", reason="Windows has no EINTR")
def test_ctrl_c_ends_a_blocked_accept(inproc):
    # A process-directed SIGINT, as a terminal's Ctrl-C sends: the reactor's
    # threads block it (R12), so it lands on this thread and ends its poll.
    timer = threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGINT))
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            inproc.replier.accept()
    finally:
        timer.cancel()
    with pytest.raises(weida.Again):
        inproc.replier.accept(timeout=0)
