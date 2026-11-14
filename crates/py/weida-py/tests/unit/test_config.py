"""The runtime configuration and request metadata over the in-process
transport: guarantee negotiation, trace propagation, deadlines and the
errors a configuration raises."""

import itertools
import re
from concurrent.futures import ThreadPoolExecutor

import pytest

import weida

WAIT = 5.0

_BUSES = itertools.count()


def bus() -> str:
    return f"py-config-{next(_BUSES)}"


class Pair:
    """A runtime with a replier at `/echo` and a requester connected to it
    over a bus of its own."""

    def __init__(self, config=None) -> None:
        name = bus()
        self.runtime = weida.Runtime.owned(config)
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


def test_a_guarantee_the_dialler_does_not_offer_raises_negotiation(pool):
    name = bus()
    url = f"weida+inproc://{name}/echo"
    guarantees = weida.GuaranteeSet(ordering=weida.OrderingMode.PER_PRODUCER_DETECT)
    server = weida.Runtime.owned(weida.RuntimeConfig(guarantees=guarantees))
    listener = server.listener()
    binding = listener.bind_inproc(name)
    replier = listener.replier("/echo")

    plain = weida.Runtime.owned()
    refused = plain.requester(weida.ClientTls(weida.Trust()))
    with pytest.raises(weida.Negotiation):
        refused.connect(url, WAIT)
        refused.request(b"hello", WAIT)

    alike = weida.Runtime.owned(weida.RuntimeConfig(guarantees=guarantees))
    requester = alike.requester(weida.ClientTls(weida.Trust()))
    requester.connect(url, WAIT)

    def serve() -> None:
        request = replier.accept(WAIT)
        # Read whole first: a body dropped unread is refused REJECTED, which
        # fails a request still writing it.
        assert request.take_body().collect(64, WAIT).payload == b"hello"
        reply = request.reply(None, WAIT)
        reply.write_all(b"hi", WAIT)
        reply.finish()

    served = pool.submit(serve)
    try:
        assert requester.request(b"hello", WAIT).collect(64, WAIT).payload == b"hi"
    finally:
        served.result(WAIT)

    for handle in (requester, refused, replier, binding, listener):
        handle.drop()
    alike.shutdown(WAIT)
    plain.shutdown(WAIT)
    server.shutdown(WAIT)


def test_metadata_travels_with_a_request_and_its_reply(pair, pool):
    t = weida.new_trace()

    def serve() -> None:
        request = pair.replier.accept(WAIT)
        meta = request.meta()
        assert meta.endpoint == "/echo"
        assert meta.content_len == 4
        assert meta.topic == "jobs"
        assert meta.trace == t
        assert meta.tracestate is None
        assert meta.peer_local is None
        out = request.reply(weida.TransferMeta(achieved=weida.Acknowledgement.ACCEPTED), WAIT)
        out.set_priority(5)
        assert out.trace() == t, "a reply inherits the request's trace"
        out.write_all(b"pong", WAIT)
        out.finish()

    served = pool.submit(serve)
    try:
        meta = weida.TransferMeta(content_len=4, topic="jobs", trace=t)
        reply = pair.requester.request_with(meta, b"ping", WAIT)
        assert reply.meta().trace == t
        assert reply.meta().achieved is weida.Acknowledgement.ACCEPTED
        assert reply.collect(64, WAIT).payload == b"pong"
    finally:
        served.result(WAIT)


def test_an_expired_reply_fails_its_finish_with_expired(pair, pool):
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


def test_a_full_configuration_builds_a_runtime():
    limits = weida.Limits(
        max_header_bytes=65536,
        max_concurrent_uni_streams=64,
        max_concurrent_bidi_streams=64,
        stream_receive_window=1 << 20,
        connection_receive_window=4 << 20,
        keep_alive=2.0,
        idle_timeout=6.0,
        hello_timeout=5.0,
        max_subscriptions=128,
        subscriber_buffer_bytes=1 << 20,
        max_sequence_scopes=64,
        max_reorder_hold=64,
        max_local_streams=64,
        max_parked_reverse=4,
        max_dedup_entries=1024,
        datagram_receive_bytes=weida.DEFAULT_DATAGRAM_RECEIVE_BYTES,
        datagram_send_bytes=65536,
        path_report=True,
        max_flows=8,
        flow_queue_bytes=65536,
        flow_early_bytes=65536,
        flow_early_hold=0.5,
        congestion=weida.Congestion.BBR,
        header_timeout=5.0,
        max_header_streams=16,
    )
    guarantees = weida.GuaranteeSet(
        delivery=weida.DeliveryLevel.AT_LEAST_ONCE,
        acknowledgement=weida.Acknowledgement.STORED,
        durability=weida.Durability.FLUSHED,
        replicas=None,
        ordering=weida.OrderingMode.PER_PRODUCER_DETECT,
        deduplication=weida.Deduplication.BOUNDED,
        dedup_window=60.0,
        backpressure=weida.Backpressure.BLOCK,
        producer_naming=weida.ProducerNaming.FINGERPRINT,
        control_isolated=False,
    )
    reconnect = weida.ReconnectPolicy(
        initial=0.05, max=1.0, jitter=False, max_attempts=3, stop_on_peer_closed=True
    )
    runtime = weida.Runtime.owned(
        weida.RuntimeConfig(
            worker_threads=2,
            shutdown_timeout=0.5,
            max_connections=100,
            max_connections_per_peer=10,
            endpoint_queue=16,
            max_resolved_addresses=4,
            connect_attempt_timeout=0.1,
            send_timeout=2.0,
            outbox_messages=10,
            outbox_bytes=1 << 20,
            outbox_full=weida.OutboxFull.REJECT,
            max_handshakes=32,
            retry_above_handshakes=8,
            discovery=weida.Discovery.SINGLE,
            limits=limits,
            guarantees=guarantees,
            reconnect=reconnect,
        )
    )
    runtime.listener().drop()
    runtime.shutdown(WAIT)

    never = weida.Runtime.owned(weida.RuntimeConfig(reconnect=weida.ReconnectPolicy.never()))
    never.listener().drop()
    never.shutdown(WAIT)


def test_configuration_input_errors_raise_at_once():
    with pytest.raises(TypeError):
        weida.Runtime.owned(weida.RuntimeConfig(outbox_full=weida.Backpressure.SPILL))
    with pytest.raises(TypeError):
        weida.Runtime.owned(weida.RuntimeConfig(outbox_full=2))
    with pytest.raises(TypeError, match="unknown Limits field: idle_timeout_ms"):
        weida.Limits(idle_timeout_ms=1)
    with pytest.raises(weida.InvalidArgument):
        weida.Runtime.owned(weida.RuntimeConfig(endpoint_queue=0))
    with pytest.raises(ValueError) as caught:
        weida.Runtime.owned(weida.RuntimeConfig(max_connections=-1))
    assert type(caught.value) is ValueError
    with pytest.raises(ValueError):
        weida.Runtime.owned(weida.RuntimeConfig(send_timeout=-1.0))
    with pytest.raises(TypeError, match="limits is a weida.Limits"):
        weida.Runtime.owned(weida.RuntimeConfig(limits={"idle_timeout": 1.0}))


def test_new_trace_is_a_sampled_traceparent():
    assert re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-01", weida.new_trace())
