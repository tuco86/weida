"""`weida_nng.sync`: the same sockets, with no event loop in the process.

The first test is the asynchronous round trip written synchronously — the same
exchange, the same assertions, no `asyncio` imported at all. The rest assert
what a synchronous surface has to get right and an asynchronous one does not
have to think about: a timeout turns a stalled exchange into an exception
rather than a parked thread, and the GIL is released while a thread blocks,
so a second thread runs.

Nothing here re-implements a protocol: `weida_nng.sync` is Python argument
conversion over `weida-nng`'s own blocking facade, which is `block_on`
around the asynchronous sockets these tests' asyncio siblings use.
"""

import signal
import subprocess
import sys
import threading
import time

import pytest

import weida_nng
from weida_nng import sync

# Long enough for a TCP handshake on a loaded machine, short enough that a
# deadlock is a failure rather than a coffee break.
TIMEOUT = 5.0


def options(**kwargs):
    kwargs.setdefault("recv_timeout", TIMEOUT)
    kwargs.setdefault("send_timeout", TIMEOUT)
    return weida_nng.SocketOptions(**kwargs)


def test_the_asyncio_round_trip_runs_unchanged_in_synchronous_form():
    context = sync.Context()
    server = sync.RepSocket(context, options())
    client = sync.ReqSocket(context, options())
    url = server.listen("tcp://127.0.0.1:0")
    client.dial(url)

    client.send(b"Hello")
    assert server.recv() == b"Hello"
    server.send(b"World")
    assert client.recv() == b"World"

    context.shutdown()


def test_no_event_loop_is_running_while_this_suite_runs():
    """No asyncio loop is required in the process."""
    import asyncio

    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()

    context = sync.Context()
    puller = sync.PullSocket(context, options())
    pusher = sync.PushSocket(context, options())
    url = puller.listen("tcp://127.0.0.1:0")
    pusher.dial(url)
    pusher.send(b"no loop needed")
    assert puller.recv() == b"no loop needed"
    context.shutdown()


def test_a_stalled_receive_becomes_an_exception_rather_than_a_hang():
    context = sync.Context()
    puller = sync.PullSocket(context, options(recv_timeout=0.2))
    puller.listen("tcp://127.0.0.1:0")

    started = time.monotonic()
    with pytest.raises(weida_nng.ETIMEDOUT):
        puller.recv()
    assert time.monotonic() - started < 2.0

    context.shutdown()


def test_a_stalled_send_becomes_an_exception_rather_than_a_hang():
    context = sync.Context()
    pusher = sync.PushSocket(context, options(send_timeout=0.2))

    started = time.monotonic()
    with pytest.raises(weida_nng.ETIMEDOUT):
        pusher.send(b"nowhere")
    assert time.monotonic() - started < 2.0

    context.shutdown()


def test_a_blocked_thread_does_not_hold_the_gil():
    """One thread parked in `recv` while another one works is the whole
    reason a synchronous binding releases the GIL."""
    context = sync.Context()
    puller = sync.PullSocket(context, options(recv_timeout=3.0))
    url = puller.listen("tcp://127.0.0.1:0")

    received = []
    waiting = threading.Thread(target=lambda: received.append(puller.recv()))
    waiting.start()

    # This thread only runs at all if the parked one let the GIL go.
    counted = 0
    while counted < 1000:
        counted += 1

    pusher = sync.PushSocket(context, options())
    pusher.dial(url)
    pusher.send(b"from the other thread")
    waiting.join(timeout=TIMEOUT)

    assert not waiting.is_alive(), "the parked receive returned"
    assert received == [b"from the other thread"]
    assert counted == 1000

    context.shutdown()


def test_two_threads_run_two_transactions_on_one_socket():
    """A context each, which is what makes them independent."""
    context = sync.Context()
    server = sync.RepSocket(context, options())
    client = sync.ReqSocket(context, options())
    url = server.listen("tcp://127.0.0.1:0")
    client.dial(url)

    def answer():
        transaction = server.context()
        body = transaction.recv()
        transaction.send(b"re:" + body)

    answering = [threading.Thread(target=answer) for _ in range(2)]
    for thread in answering:
        thread.start()

    replies = []

    def ask(question):
        transaction = client.context()
        transaction.send(question)
        replies.append(transaction.recv())

    asking = [threading.Thread(target=ask, args=(body,)) for body in (b"one", b"two")]
    for thread in asking:
        thread.start()
    for thread in asking + answering:
        thread.join(timeout=TIMEOUT)
        assert not thread.is_alive()

    assert sorted(replies) == [b"re:one", b"re:two"]

    context.shutdown()


def test_the_subscription_filter_is_the_subscribers_here_too():
    context = sync.Context()
    publisher = sync.PubSocket(context, options())
    subscriber = sync.SubSocket(context, options(recv_timeout=2.0, subscribe=b"weather."))
    url = publisher.listen("tcp://127.0.0.1:0")
    subscriber.dial(url)
    while publisher.pipe_count == 0:
        time.sleep(0.005)

    published = publisher.send(b"sports.result")
    assert published.queued == 1
    publisher.send(b"weather.rain")

    assert subscriber.recv() == b"weather.rain"
    assert subscriber.discarded == 1

    context.shutdown()


def test_an_option_a_protocol_cannot_honour_is_refused_here_as_well():
    context = sync.Context()
    with pytest.raises(weida_nng.ENOTSUP):
        sync.ReqSocket(context, weida_nng.SocketOptions(send_depth=4))
    context.shutdown()


def test_the_two_surfaces_talk_to_each_other():
    """The proof that there is one implementation and not two: a
    synchronous socket and an asynchronous one exchange a message."""
    import asyncio

    async def asynchronous_half(url, ready):
        context = weida_nng.Context()
        puller = weida_nng.PullSocket(context, options())
        bound = await puller.listen(url)
        ready.put(bound)
        received = await puller.recv()
        await context.shutdown()
        return received

    import queue

    ready = queue.Queue()
    result = {}

    def run_async():
        result["body"] = asyncio.run(
            asynchronous_half("tcp://127.0.0.1:0", ready)
        )

    thread = threading.Thread(target=run_async)
    thread.start()
    url = ready.get(timeout=TIMEOUT)

    context = sync.Context()
    pusher = sync.PushSocket(context, options())
    pusher.dial(url)
    pusher.send(b"one implementation")
    thread.join(timeout=TIMEOUT)
    context.shutdown()

    assert result["body"] == b"one implementation"


def test_ctrl_c_ends_a_receive_with_no_peer_and_no_deadline():
    # A child process parks in `recv` on an idle PULL with no timeout; the
    # parent interrupts it after the child reports it is waiting. The wait
    # must end with KeyboardInterrupt rather than hold the process until a
    # message arrives.
    script = (
        "from weida_nng import sync\n"
        "sock = sync.PullSocket(sync.Context())\n"
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
        _, stderr = child.communicate(timeout=3)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    assert "KeyboardInterrupt" in stderr
