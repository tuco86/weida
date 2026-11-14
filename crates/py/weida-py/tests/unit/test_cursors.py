"""The cursor surface over the in-process transport: report orders, the
sender's queue of cursor sets, and the receiver's reporter."""

import itertools

import pytest

import weida

WAIT = 5.0
ACCEPTED = weida.Acknowledgement.ACCEPTED
entry = weida.CursorEntry

_BUSES = itertools.count()


def bus() -> str:
    return f"py-cursors-{next(_BUSES)}"


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


def read_all(transfer) -> bytes:
    """`transfer` read to its end."""
    pieces = []
    while piece := transfer.read(4, WAIT):
        pieces.append(piece)
    return b"".join(pieces)


def test_a_request_that_orders_accepted_receives_the_reported_offsets_in_order(pair):
    body, reply = pair.requester.open(weida.TransferMeta(report=[ACCEPTED, ACCEPTED]), WAIT)
    cursors = body.cursors()
    assert isinstance(cursors, weida.Cursors)
    assert body.cursors() is None  # the queue is handed out once
    body.write_all(b"aaaabbbbcccc", WAIT)
    body.finish().drop()

    request = pair.replier.accept(WAIT)
    meta = request.meta()
    assert meta.report == (ACCEPTED,)
    assert meta.report[0] is ACCEPTED
    assert meta.report_mode is weida.ReportMode.PROGRESS
    assert isinstance(meta.report_id, int)
    received = request.take_body()
    reporter = received.reporter()
    assert reporter.levels() == (ACCEPTED,)
    assert reporter.mode() is weida.ReportMode.PROGRESS
    reporter.set_granularity(0, 0)
    assert read_all(received) == b"aaaabbbbcccc"
    reporter.report(weida.Acknowledgement.STORED, 12, WAIT)  # not ordered: ignored

    for offset in (4, 8):
        reporter.report(ACCEPTED, offset, WAIT)
        assert cursors.recv(WAIT) == (entry(ACCEPTED, offset),)
    reporter.report(ACCEPTED, 12, WAIT)
    reporter.finish(WAIT)
    assert cursors.recv(WAIT) == (entry(ACCEPTED, 12),)
    assert cursors.recv(WAIT) is None

    answer = request.reply(None, WAIT)
    assert answer.cursors() is None  # the reply ordered nothing
    answer.write_all(b"ok", WAIT)
    answer.finish().drop()
    back = reply.recv(WAIT)
    assert back.reporter() is None
    assert read_all(back) == b"ok"

    for handle in (cursors, received, back):
        handle.drop()


def test_final_only_yields_exactly_one_update_per_level(pair):
    floor = weida.CURSOR_LEVEL_APPLICATION_FLOOR
    with pytest.raises(weida.LimitExceeded):
        pair.requester.open(weida.TransferMeta(report=range(floor, floor + 17)), WAIT)
    with pytest.raises(weida.InvalidArgument):
        pair.requester.open(weida.TransferMeta(report=[7]), WAIT)

    stage = floor + 1
    body, reply = pair.requester.open(
        weida.TransferMeta(report=[stage, ACCEPTED], report_mode=weida.ReportMode.FINAL_ONLY), WAIT
    )
    cursors = body.cursors()
    body.write_all(b"0123456789", WAIT)
    body.finish().drop()

    request = pair.replier.accept(WAIT)
    meta = request.meta()
    assert meta.report == (ACCEPTED, stage)
    assert meta.report_mode is weida.ReportMode.FINAL_ONLY
    received = request.take_body()
    reporter = received.reporter()
    assert reporter.levels() == (ACCEPTED, stage)
    assert reporter.mode() is weida.ReportMode.FINAL_ONLY
    assert read_all(received) == b"0123456789"
    for offset in (5, 10):
        for level in (ACCEPTED, stage):
            reporter.report(level, offset, WAIT)
    with pytest.raises(weida.Again):
        cursors.recv(0.1)  # nothing arrives before finish

    reporter.finish(WAIT)
    sets = list(cursors)
    assert {e for s in sets for e in s} == {entry(ACCEPTED, 10), entry(stage, 10)}
    assert sets[-1] == (entry(ACCEPTED, 10), entry(stage, 10))

    for handle in (reply, request, received, cursors):
        handle.drop()


def test_a_report_order_of_the_wrong_type_is_a_type_error(pair):
    for meta in (
        weida.TransferMeta(report=ACCEPTED),
        weida.TransferMeta(report=["ACCEPTED"]),
        weida.TransferMeta(report=[weida.Durability.WRITTEN]),
        weida.TransferMeta(report_mode=1),
    ):
        with pytest.raises(TypeError):
            pair.requester.open(meta, WAIT)
    with pytest.raises(ValueError):
        pair.requester.open(weida.TransferMeta(report=[-1]), WAIT)
