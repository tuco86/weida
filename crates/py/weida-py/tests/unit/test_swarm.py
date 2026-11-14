"""SWARM over the in-process transport: a blob seeded by one runtime and
fetched by two others, a declined session, and a piece a seeder says is
gone."""

import itertools
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida

WAIT = 5.0

_BUSES = itertools.count()

# 18 pieces of 1 KiB, the last one short.
LEN = 17 * 1024 + 100
DATA = bytes(((i * 0x9E3779B1) & 0xFFFFFFFF) >> 24 for i in range(LEN))


def bus() -> str:
    return f"py-swarm-{next(_BUSES)}"


def client_tls() -> weida.ClientTls:
    return weida.ClientTls(weida.Trust())


def answer(session, data, piece_list, asks):
    """Answers a session's orders from `data` until the fetch finished them,
    recording each order's kind."""
    try:
        assert session.meta().endpoint == "/blob"
        assert session.connection() > 0
        with session.blob() as blob:
            while (order := session.next(WAIT)) is not None:
                asks.append(order.kind)
                if order.kind is weida.Ask.LIST:
                    session.answer_list(piece_list, WAIT)
                elif order.kind is weida.Ask.HAVE:
                    with weida.Bits.full(blob.count()) as have:
                        session.answer_have(have, WAIT)
                else:
                    start, end = blob.span(order.index)
                    session.answer_piece(data[start:end], WAIT)
    finally:
        session.drop()


def serve(pool, seeder, data, piece_list, asks, stop: threading.Event):
    """Accepts sessions until `stop` is set, answering each on a thread of
    `pool`, then waits for every one."""
    sessions = []
    while not stop.is_set():
        try:
            session = seeder.accept(0.02)
        except weida.Again:
            continue
        sessions.append(pool.submit(answer, session, data, piece_list, asks))
    for session in sessions:
        session.result(WAIT)


def gather(fetch, blob, out: bytearray):
    """Reads `fetch` to its end, copying each piece to its span of `out`:
    the indices it yielded, sorted."""
    indices = []
    for piece in fetch:
        start, end = blob.span(piece.index)
        assert len(piece.bytes) == end - start
        out[start:end] = piece.bytes
        indices.append(piece.index)
    return sorted(indices)


def test_a_blob_seeded_by_one_runtime_is_fetched_by_two_others_which_verify_it():
    server = weida.Runtime.owned()
    near = weida.Runtime.owned()
    far = weida.Runtime.owned()
    name = bus()
    url = f"weida+inproc://{name}/blob"
    listener = server.listener()
    binding = listener.bind_inproc(name)
    seeder = listener.seeder("/blob")
    assert seeder.path() == "/blob"

    hasher = weida.TreeHasher(weida.PIECE_SIZE_MIN_LOG2)
    hasher.update(DATA[:5000]).update(DATA[5000:])
    piece_list = hasher.finish()
    of = weida.PieceList.of(DATA, weida.PIECE_SIZE_MIN_LOG2)
    assert piece_list.to_bytes() == of.to_bytes()
    blob = piece_list.blob()
    assert blob.count() == 18
    assert blob.size() == LEN
    assert blob.piece_log2() == weida.PIECE_SIZE_MIN_LOG2
    assert blob.root() == weida.blob_id_of(DATA)
    assert len(blob.root()) == weida.BLOB_ID_LEN
    assert blob.span(17) == (17 * 1024, LEN)
    assert blob.span(18) is None
    assert weida.Blob(blob.root(), LEN, weida.PIECE_SIZE_MIN_LOG2).count() == 18
    assert weida.PieceList.parse(blob, piece_list.to_bytes()).to_bytes() == piece_list.to_bytes()
    assert piece_list.check(0, DATA[:1024]) is True
    assert piece_list.check(1, DATA[:1024]) is False

    asks = []
    stop = threading.Event()
    with ThreadPoolExecutor(max_workers=4) as pool:
        serving = pool.submit(serve, pool, seeder, DATA, piece_list, asks, stop)

        # A fetches every piece and keeps the list it learnt.
        a = near.swarm(client_tls())
        events = a.events()
        a.connect(url, WAIT)
        while events.recv(WAIT).kind is not weida.PeerEventKind.CONNECTED:
            pass
        events.drop()
        assert a.peer_count() == 1
        assert len(a.connection_stats()) == 1
        have_a = weida.Bits(18)
        fetch_a = a.fetch(blob, have_a)
        out_a = bytearray(LEN)
        assert gather(fetch_a, blob, out_a) == list(range(18))
        assert weida.blob_id_of(out_a) == blob.root()
        assert fetch_a.delivered().all() is True
        assert fetch_a.in_flight().ones() == 0
        assert fetch_a.next(0) is None  # a finished fetch stays finished
        known = fetch_a.list()
        assert known.to_bytes() == piece_list.to_bytes()

        # B holds the even pieces and the list: it takes only the odd ones.
        b = far.swarm(client_tls())
        b.connect(url, WAIT)
        have_b = weida.Bits(18)
        out_b = bytearray(LEN)
        for i in range(0, 18, 2):
            have_b.set(i, True)
            start, end = blob.span(i)
            out_b[start:end] = DATA[start:end]
        assert have_b.as_bytes() == b"\x55\x55\x01"
        assert weida.Bits.from_bytes(18, have_b.as_bytes()).ones() == 9
        fetch_b = b.fetch(blob, have_b, list=known, per_peer=2, max_inflight=4)
        assert gather(fetch_b, blob, out_b) == list(range(1, 18, 2))
        assert weida.blob_id_of(out_b) == blob.root()
        assert asks.count(weida.Ask.LIST) == 1

        stop.set()
        serving.result(WAIT)

    assert a.disconnect(url) is True
    assert a.disconnect(url) is False
    for handle in (fetch_b, fetch_a, have_b, have_a, known, b, a):
        handle.drop()
    for handle in (seeder, blob, of, piece_list, binding, listener):
        handle.drop()
    far.shutdown(WAIT)
    near.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_declined_session_leaves_the_fetch_not_connected():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    name = bus()
    listener = server.listener()
    binding = listener.bind_inproc(name)
    seeder = listener.seeder("/blob")
    swarm = client.swarm(client_tls())
    swarm.connect(f"weida+inproc://{name}/blob", WAIT)

    piece_list = weida.PieceList.of(DATA, weida.PIECE_SIZE_MIN_LOG2)
    blob = piece_list.blob()
    have = weida.Bits(18)
    short = weida.Bits(17)
    with pytest.raises(weida.InvalidArgument):
        swarm.fetch(blob, short)
    with pytest.raises(TypeError):
        swarm.fetch(blob, have, list=b"x")

    fetch = swarm.fetch(blob, have)
    # The orders are written, nobody answers yet.
    with pytest.raises(weida.Again):
        fetch.next(0)
    # The maps answer while a read is in flight.
    assert fetch.delivered().ones() == 0
    assert fetch.blob().count() == 18

    session = seeder.accept(WAIT)
    assert session.next(WAIT) is not None
    # The order before is unanswered.
    with pytest.raises(weida.Protocol):
        session.next(0)
    session.decline(WAIT)

    with pytest.raises(weida.NotConnected):
        fetch.next(WAIT)
    assert fetch.delivered().ones() == 0
    assert fetch.list() is None

    for handle in (fetch, short, have, blob, piece_list, swarm, seeder, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_a_piece_a_seeder_says_is_gone_is_unavailable_until_another_seeder_serves_it():
    server = weida.Runtime.owned()
    client = weida.Runtime.owned()
    name_a, name_b = bus(), bus()
    listener_a = server.listener()
    binding_a = listener_a.bind_inproc(name_a)
    seeder_a = listener_a.seeder("/blob")
    listener_b = server.listener()
    binding_b = listener_b.bind_inproc(name_b)
    seeder_b = listener_b.seeder("/blob")
    piece_list = weida.PieceList.of(DATA, weida.PIECE_SIZE_MIN_LOG2)
    blob = piece_list.blob()

    # Seeder A holds every piece by HAVE, then says piece 3 is gone when it
    # is asked for it.
    asked = []

    def serve_a():
        session = seeder_a.accept(WAIT)
        try:
            with weida.Bits.full(18) as held, session.blob() as b:
                while (order := session.next(WAIT)) is not None:
                    if order.kind is weida.Ask.LIST:
                        session.answer_list(piece_list, WAIT)
                    elif order.kind is weida.Ask.HAVE:
                        session.answer_have(held, WAIT)
                    else:
                        asked.append(order.index)
                        if order.index == 3:
                            held.set(3, False)
                            session.answer_gone(3, WAIT)
                        else:
                            start, end = b.span(order.index)
                            session.answer_piece(DATA[start:end], WAIT)
        finally:
            session.drop()

    stop = threading.Event()
    with ThreadPoolExecutor(max_workers=4) as pool:
        serving_a = pool.submit(serve_a)
        serving_b = pool.submit(serve, pool, seeder_b, DATA, piece_list, [], stop)

        swarm = client.swarm(client_tls())
        swarm.connect(f"weida+inproc://{name_a}/blob", WAIT)
        have = weida.Bits(18)
        fetch = swarm.fetch(blob, have)
        fetch.prioritize(3, 4)

        out = bytearray(LEN)
        for _ in range(17):
            piece = fetch.next(WAIT)
            assert piece is not None
            assert piece.index != 3, "piece 3 is gone at the only seeder"
            start, end = blob.span(piece.index)
            out[start:end] = piece.bytes
        # Every round reads the map while a read is in flight.
        while True:
            with pytest.raises(weida.Again):
                fetch.next(0.02)
            with fetch.unavailable() as unavailable:
                if unavailable.get(3):
                    assert unavailable.ones() == 1
                    break
        assert asked[0] == 3, "the prioritized piece is ordered first"

        fetch.clear_priority()
        swarm.connect(f"weida+inproc://{name_b}/blob", WAIT)
        piece = fetch.next(WAIT)
        assert piece.index == 3
        start, end = blob.span(3)
        out[start:end] = piece.bytes
        assert fetch.next(WAIT) is None
        assert weida.blob_id_of(out) == blob.root()
        assert fetch.unavailable().ones() == 0

        serving_a.result(WAIT)
        stop.set()
        serving_b.result(WAIT)

    for handle in (fetch, have, swarm, seeder_a, seeder_b, blob, piece_list):
        handle.drop()
    for handle in (binding_a, binding_b, listener_a, listener_b):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)


def test_piece_tree_input_errors_raise_at_once():
    with pytest.raises(ValueError):
        weida.Blob(b"short", 1)
    with pytest.raises(weida.InvalidArgument):
        weida.Blob(bytes(weida.BLOB_ID_LEN), 1, 9)
    with pytest.raises(weida.InvalidArgument):
        weida.TreeHasher(weida.PIECE_SIZE_MAX_LOG2 + 1)
    # Padding bits set.
    with pytest.raises(weida.InvalidArgument):
        weida.Bits.from_bytes(18, b"\xff\xff\xff")
    with pytest.raises(TypeError):
        weida.PieceList()
    with pytest.raises(TypeError):
        weida.Bits.full(True)
    with pytest.raises(ValueError):
        weida.Bits(-1)
    assert weida.Bits.from_bytes(18, b"\x55\x55\x01").ones() == 9
