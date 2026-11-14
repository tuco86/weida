"""Runtime.drain over the in-process transport."""

import dataclasses
import itertools
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida

WAIT = 5.0
HOUR = 3600.0  # a drain that waited for its deadline would time the test out

_BUSES = itertools.count()


def test_a_drain_counts_a_reply_finished_before_it_as_delivered():
    bus = f"py-drain-{next(_BUSES)}"
    server = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_inproc(bus)
    replier = listener.replier("/echo")
    client = weida.Runtime.owned()
    requester = client.requester(weida.ClientTls(weida.Trust()))
    requester.connect(f"weida+inproc://{bus}/echo", WAIT)

    def serve() -> None:
        request = replier.accept(WAIT)
        out = request.reply(None, WAIT)
        out.write_all(b"pong", WAIT)
        out.finish().drop()

    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(serve)
        assert requester.request(b"ping", WAIT).collect(64, WAIT).payload == b"pong"
        served.result(WAIT)

    drained = server.drain(HOUR, WAIT)
    assert drained == weida.Drained(delivered=1, outstanding=0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        drained.delivered = 2
    with pytest.raises(weida.Closed):
        server.listener()  # the drain consumed the runtime
    for handle in (requester, replier, binding, listener):
        handle.drop()
    client.shutdown(WAIT)


def test_a_drain_with_nothing_pending_returns_at_once():
    drained = weida.Runtime.owned().drain(HOUR, WAIT)
    assert drained == weida.Drained(delivered=0, outstanding=0)


@pytest.mark.parametrize("deadline", [-1, float("nan"), float("inf"), 2**64 // 1000 + 1])
def test_a_deadline_out_of_range_is_a_value_error_and_consumes_nothing(deadline):
    runtime = weida.Runtime.owned()
    with pytest.raises(ValueError):
        runtime.drain(deadline)
    runtime.listener().drop()
    runtime.shutdown(WAIT)
