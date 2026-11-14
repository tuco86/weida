"""Refusals and the requester's cancellation over the in-process transport."""

import itertools
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida
from weida._native import lib

WAIT = 5.0

_BUSES = itertools.count()


def bus() -> str:
    return f"py-refusals-{next(_BUSES)}"


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


CODES = [
    (weida.ErrorCode.REJECTED, weida.Rejected),
    (weida.ErrorCode.UNKNOWN_ENDPOINT, weida.UnknownEndpoint),
    (weida.ErrorCode.UNSUPPORTED, weida.Unsupported),
    (weida.ErrorCode.INTERNAL, weida.Transport),
    (weida.ErrorCode.NO_REPLY, weida.NoReply),
]


def test_a_refused_request_raises_the_refused_codes_error_at_the_requester(pair, pool):
    for code, expected in CODES:
        asking = pool.submit(pair.requester.request, b"ask", WAIT)
        request = pair.replier.accept(WAIT)
        request.refuse(code, WAIT)
        with pytest.raises(expected):
            asking.result(WAIT)


def test_a_refusal_code_that_is_not_an_error_code_is_a_type_error_and_leaves_the_request_usable(
    pair, pool
):
    asking = pool.submit(pair.requester.request, b"ask", WAIT)
    request = pair.replier.accept(WAIT)
    # The plain 2 equals ErrorCode.REJECTED's value: the converter is strict.
    for bad in (2, "REJECTED", weida.Acknowledgement.ACCEPTED):
        with pytest.raises(TypeError, match="code is a weida.ErrorCode"):
            request.refuse(bad, WAIT)
    request.refuse(weida.ErrorCode.REJECTED, WAIT)
    with pytest.raises(weida.Rejected):
        asking.result(WAIT)
    # The refusal consumed the request.
    with pytest.raises(weida.Closed):
        request.refuse(weida.ErrorCode.REJECTED, WAIT)


def test_canceled_returns_once_the_requester_gives_up(pair, pool):
    # Held here so that the test decides when the requester gives up;
    # dropping the op is what Requester.request does when its timeout passes.
    pending = pair.requester._start_request(b"wait")
    request = pair.replier.accept(WAIT)
    with pytest.raises(weida.Again):
        request.canceled(0)  # the requester still waits
    watching = pool.submit(request.canceled, WAIT)
    lib.weida_requester_request_op_drop(pending)
    watching.result(WAIT)
    request.drop()
