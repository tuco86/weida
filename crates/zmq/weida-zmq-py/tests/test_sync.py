"""The synchronous surface, and the zguide's Lazy Pirate written on it.

`weida_zmq.sync` is `weida-zmq`'s own `blocking` facade with Python
argument conversion around it, so a Python program needs no event loop at all.
Nothing about a pattern is implemented twice, which is why the assertions here
are about the *guarantee* rather than about the plumbing:

Lazy Pirate's claim (zguide chapter 4) is that a client which times out,
retries and eventually gives up either gets **the reply to its own request**
or abandons the exchange — never a stale reply to an earlier one, and never a
hang. That is what the recipe below asserts, on a server that deliberately
drops the first requests.

The GIL is released while a call blocks, which the threaded test proves: a
second thread runs while the first is parked in `recv`.
"""

import signal
import subprocess
import sys
import threading
import time

import pytest

import waits
from weida_zmq import sync

import weida_zmq

RETRIES = 3
TIMEOUT = 0.5


def test_a_synchronous_round_trip_needs_no_event_loop():
    context = sync.Context()
    server = sync.RepSocket(context)
    client = sync.ReqSocket(context)
    endpoint = server.bind("tcp://127.0.0.1:0")
    client.connect(endpoint)

    client.send(b"Hello")
    assert server.recv() == [b"Hello"]
    assert server.send(b"World").queued
    assert client.recv() == [b"World"]
    assert client.socket_type == "REQ"
    assert "sync" in repr(client)


def test_every_socket_type_exists_on_the_synchronous_surface():
    context = sync.Context()
    for name, kind in [
        ("ReqSocket", "REQ"),
        ("RepSocket", "REP"),
        ("DealerSocket", "DEALER"),
        ("RouterSocket", "ROUTER"),
        ("PubSocket", "PUB"),
        ("SubSocket", "SUB"),
        ("XPubSocket", "XPUB"),
        ("XSubSocket", "XSUB"),
        ("PushSocket", "PUSH"),
        ("PullSocket", "PULL"),
        ("PairSocket", "PAIR"),
    ]:
        socket = getattr(sync, name)(context)
        assert socket.socket_type == kind
        bound = socket.bind(f"inproc://sync-{name}")
        assert socket.last_endpoint() == bound
        socket.close()


def test_the_timeouts_and_dontwait_are_the_asynchronous_surfaces():
    context = sync.Context()
    sink = sync.PullSocket(context)
    sink.bind("inproc://sync-timeouts")

    # ZMQ_DONTWAIT on an empty queue.
    with pytest.raises(weida_zmq.EAGAIN):
        sink.recv_nowait()
    # A per-call bound, expiring as EAGAIN.
    started = time.monotonic()
    with pytest.raises(weida_zmq.EAGAIN):
        sink.recv(timeout=0.1)
    assert time.monotonic() - started >= 0.05

    # And ZMQ_RCVTIMEO through the option table, honoured by the same code.
    options = weida_zmq.SocketOptions()
    options.set("ZMQ_RCVTIMEO", 0.1)
    bounded = sync.PullSocket(context, options)
    bounded.bind("inproc://sync-rcvtimeo")
    with pytest.raises(weida_zmq.EAGAIN):
        bounded.recv()

    # A timeout that is not a bound is refused where it is given.
    with pytest.raises(weida_zmq.EINVAL):
        sink.recv(timeout=-1.0)


def test_subscriptions_work_on_the_synchronous_surface():
    context = sync.Context()
    publisher = sync.PubSocket(context)
    subscriber = sync.SubSocket(context)
    endpoint = publisher.bind("inproc://sync-pubsub")
    subscriber.connect(endpoint)
    subscriber.subscribe(b"weather.")

    while publisher.send([b"weather.eu", b"rain"]).delivered != 1:
        time.sleep(0.01)
    assert subscriber.recv() == [b"weather.eu", b"rain"]
    subscriber.unsubscribe(b"weather.")


def test_the_gil_is_released_while_a_call_blocks():
    """A second Python thread runs while the first is parked in `recv`."""
    context = sync.Context()
    sink = sync.PullSocket(context)
    sink.bind("inproc://sync-gil")
    ticks = []
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            ticks.append(time.monotonic())
            time.sleep(0.01)

    thread = threading.Thread(target=ticker)
    thread.start()
    try:
        with pytest.raises(weida_zmq.EAGAIN):
            sink.recv(timeout=0.3)
    finally:
        stop.set()
        thread.join()
    # If `recv` had held the GIL for its 300 ms, the ticker could not have run.
    assert len(ticks) > 5, ticks


def lazy_pirate(client_factory, request, retries=RETRIES, timeout=TIMEOUT):
    """The zguide's Lazy Pirate client, in synchronous Python.

    Send, wait with a timeout, and on expiry throw the socket away and open a
    new one — "the only way to recover is to close and reopen the socket",
    which is why the client is a factory here. Each attempt carries its own
    number, so a reply to an *earlier* attempt is recognisable as the stale
    reply it is. Returns the reply to the attempt that succeeded, or `None`
    when the retries are spent.
    """
    client = client_factory()
    for attempt in range(1, retries + 1):
        sent = request + b"-attempt-" + str(attempt).encode()
        client.send(sent)
        try:
            reply = client.recv(timeout=timeout)
        except weida_zmq.EAGAIN:
            # A REQ socket that timed out is out of step; the recipe discards
            # it rather than trying to use it again.
            client.close()
            client = client_factory()
            continue
        return sent, reply
    client.close()
    return None, None


def test_lazy_pirate_gets_the_reply_to_its_own_request():
    """The guide's claim: an in-order reply, or abandonment. Never a stale one."""
    context = sync.Context(worker_threads=2)
    server = sync.RepSocket(context)
    endpoint = server.bind("tcp://127.0.0.1:0")

    def client():
        socket = sync.ReqSocket(context)
        socket.connect(endpoint)
        return socket

    # The zguide's unreliable server, as a server that answers too late: the
    # first reply is addressed to a socket the client has already thrown away,
    # so if anything of it reached the retry it would be a stale reply.
    def unreliable(late):
        def serve():
            for answer in range(late + 1):
                request = server.recv()
                if answer < late:
                    time.sleep(TIMEOUT * 1.5)
                server.send(b"reply-to-" + bytes(request[0]))

        thread = threading.Thread(target=serve)
        thread.start()
        return thread

    thread = unreliable(late=1)
    sent, reply = lazy_pirate(client, b"request")
    thread.join()

    assert reply is not None, "three retries against one late answer must succeed"
    # The reply answers the attempt this client last sent, not an earlier one.
    assert reply == [b"reply-to-" + sent]
    assert sent == b"request-attempt-2", sent


def test_lazy_pirate_abandons_rather_than_hanging():
    context = sync.Context()
    server = sync.RepSocket(context)  # bound, never answering
    endpoint = server.bind("tcp://127.0.0.1:0")

    def client():
        socket = sync.ReqSocket(context)
        socket.connect(endpoint)
        return socket

    started = time.monotonic()
    _, reply = lazy_pirate(client, b"into the void", retries=2, timeout=0.2)
    elapsed = time.monotonic() - started

    assert reply is None, "a server that never answers is abandoned"
    # Abandoned, not hung: the call returned after its two retries of 200 ms.
    # That it waited them out is the lower bound; how much longer it took is
    # printed, not bounded, because a limit on a real clock turns a slow
    # machine into a failure.
    assert elapsed >= 0.4, elapsed
    print(f"two retries of 200 ms took {elapsed:.2f} s")


def parked(call):
    """Runs `call` on a thread and records how it ended."""
    outcome = {}

    def run():
        try:
            outcome["result"] = call()
        except BaseException as error:  # noqa: BLE001 - the test inspects it
            outcome["error"] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(0.2)  # long enough to be parked in the wait
    assert thread.is_alive(), outcome
    return thread, outcome


def test_close_from_another_thread_ends_a_receive_with_no_deadline():
    """The reader-thread shutdown pattern: a thread parked in `recv()` on a
    quiet socket, and the main thread closing the socket to stop it. `close()`
    used to queue behind the receive for as long as no message came."""
    context = sync.Context()
    sink = sync.PullSocket(context)
    sink.bind("inproc://sync-close-parked-recv")
    reader, outcome = parked(sink.recv)

    closer = threading.Thread(target=sink.close, daemon=True)
    closer.start()
    # `close()` returned although no message will ever come: the join ends.
    waits.join(closer, "close() to return", 5.0)
    waits.join(reader, "the parked receive to end", 5.0)
    assert isinstance(outcome.get("error"), weida_zmq.ENOTSOCK), outcome
    # And the socket stays what it is: closed.
    with pytest.raises(weida_zmq.ENOTSOCK):
        sink.recv(timeout=0.5)


def test_close_from_another_thread_ends_a_send_that_has_no_room():
    context = sync.Context()
    source = sync.PushSocket(context)
    source.bind("inproc://sync-close-parked-send")  # no worker, so nowhere to send
    writer, outcome = parked(lambda: source.send(b"nobody takes this"))

    closer = threading.Thread(target=source.close, daemon=True)
    closer.start()
    waits.join(closer, "close() to return", 5.0)
    waits.join(writer, "the parked send to end", 5.0)
    assert isinstance(outcome.get("error"), weida_zmq.ENOTSOCK), outcome


def test_ctrl_c_ends_a_receive_with_no_peer_and_no_deadline():
    # A child parks in `recv()` on an idle socket with no timeout; the parent
    # interrupts it once it says it is waiting. The wait must end with
    # KeyboardInterrupt rather than hold the process until a message comes.
    script = (
        "from weida_zmq import sync\n"
        "sock = sync.PullSocket(sync.Context())\n"
        "sock.bind('inproc://ctrl-c')\n"
        "print('waiting', flush=True)\n"
        "sock.recv()\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "waiting"
        time.sleep(0.5)
        child.send_signal(signal.SIGINT)
        _, stderr = child.communicate()
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    assert "KeyboardInterrupt" in stderr


def test_a_timeout_that_is_not_a_number_of_seconds_is_an_exception_not_a_panic():
    # `pytest.raises(Exception)` does not catch a PanicException, which is a
    # BaseException, so a panic fails here instead of passing.
    context = sync.Context()
    sink = sync.PullSocket(context)
    sink.bind("inproc://sync-bad-timeout")
    for seconds in (1e30, float("inf"), float("nan"), -1.0):
        with pytest.raises(weida_zmq.EINVAL):
            sink.recv(timeout=seconds)
