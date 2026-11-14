"""Datagram flows over the in-process transport: a raw peer opens them, an
acceptor takes them."""

import dataclasses
import itertools
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida

WAIT = 5.0
FLOWS = weida.RuntimeConfig(limits=weida.Limits(datagram_receive_bytes=weida.DEFAULT_DATAGRAM_RECEIVE_BYTES))

_BUSES = itertools.count()


def bus() -> str:
    return f"py-flows-{next(_BUSES)}"


def client_tls() -> weida.ClientTls:
    return weida.ClientTls(weida.Trust())


def test_a_flow_carries_datagrams_to_an_acceptor_and_ends_after_them():
    name = bus()
    runtime = weida.Runtime.owned(FLOWS)
    listener = runtime.listener()
    binding = listener.bind_inproc(name)
    acceptor = listener.acceptor("/v")
    assert acceptor.path() == "/v"
    peer = runtime.peer(client_tls())
    peer.connect(f"weida+inproc://{name}/v", WAIT)
    flow = peer.open_flow(weida.FlowMeta(topic="mic", content_type="audio/opus"), WAIT)
    assert isinstance(flow, weida.Flow)
    flow.send(b"one")
    flow.send(bytearray(b"two"))
    flow.send(memoryview(b"\x03"))

    incoming = acceptor.accept(WAIT)
    assert incoming.kind is weida.IncomingKind.FLOW
    assert isinstance(incoming.flow, weida.IncomingFlow)
    with pytest.raises(dataclasses.FrozenInstanceError):
        incoming.flow = None
    info = incoming.flow.info()
    assert (info.endpoint, info.topic, info.content_type, info.trace) == ("/v", "mic", "audio/opus", None)
    assert (info.peer_fingerprint, info.peer_chain, info.peer_local) == (None, None, None)
    max_payload = flow.max_payload()
    assert max_payload > 0
    assert flow.path_stats() is None
    assert incoming.flow.path_stats() is None
    with pytest.raises(weida.TooLarge) as too_large:
        flow.send(bytes(max_payload + 1))
    assert too_large.value.max == max_payload
    assert flow.stats() == weida.FlowStats(sent=3, too_large=1, discarded=0, not_live=0)

    flow.drop()
    got = []
    for d in incoming.flow:
        got.append(d)
        if len(got) == 3:
            assert incoming.flow.stats() == weida.IncomingFlowStats(received=3, overflow=0)
    assert got == [b"one", b"two", b"\x03"]
    assert incoming.flow.recv(WAIT) is None  # the end stays the end

    for handle in (incoming.flow, peer, acceptor, binding, listener):
        handle.drop()
    runtime.shutdown(WAIT)


def test_a_flow_without_datagrams_is_unavailable_other_kinds_are_tagged_and_a_refusal_reaches_the_sender():
    name = bus()
    server = weida.Runtime.owned(FLOWS)
    near = weida.Runtime.owned(FLOWS)
    far = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_inproc(name)
    acceptor = listener.acceptor("/v")
    url = f"weida+inproc://{name}/v"

    far_peer = far.peer(client_tls())
    far_peer.connect(url, WAIT)
    with pytest.raises(weida.DatagramsUnavailable):
        far_peer.open_flow(None, WAIT)

    requester = near.requester(client_tls())
    requester.connect(url, WAIT)
    with ThreadPoolExecutor(max_workers=1) as pool:
        asking = pool.submit(requester.request, b"x", WAIT)
        assert acceptor.accept(WAIT) == weida.Incoming(kind=weida.IncomingKind.EXCHANGE, flow=None)
        with pytest.raises(weida.NoReply):
            asking.result(WAIT)

    peer = near.peer(client_tls())
    peer.connect(url, WAIT)
    flow = peer.open_flow(timeout=WAIT)
    acceptor.accept(WAIT).flow.refuse()
    while True:
        try:
            flow.send(b"x")
        except weida.Rejected:
            break
        time.sleep(0.001)
    flow.abort()

    for handle in (far_peer, requester, peer, acceptor, binding, listener):
        handle.drop()
    far.shutdown(WAIT)
    near.shutdown(WAIT)
    server.shutdown(WAIT)


def test_flow_metadata_of_the_wrong_type_is_a_type_error():
    runtime = weida.Runtime.owned(FLOWS)
    peer = runtime.peer(client_tls())
    with pytest.raises(TypeError):
        weida.FlowMeta(label="mic")
    for meta in (weida.FlowMeta(topic=1), weida.FlowMeta(content_type=b"audio/opus"), "mic"):
        with pytest.raises(TypeError):
            peer.open_flow(meta, WAIT)
    with pytest.raises(weida.InvalidArgument):
        peer.open_flow(weida.FlowMeta(trace="nope"), WAIT)

    peer.drop()
    runtime.shutdown(WAIT)
