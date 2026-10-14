"""The module, the context and the eleven socket types, from Python.

The first test here is an asyncio REQ/REP round trip between two
sockets of this module, over each of the three transports the library
implements. The rest assert what the module promises beside it — the three
context constructors, the endpoint surface of all eleven socket types, and
failures arriving as exception classes that carry libzmq's errno name rather
than as one `RuntimeError`.

Every test runs its coroutine under a wall-clock bound, so a binding that
deadlocks fails the suite instead of hanging it (docs/LOOP.md 2).
"""

import asyncio
import pathlib
import tempfile

import pytest

import weida_zmq

# Long enough for a TCP handshake on a loaded machine, short enough that a
# deadlock is a failure rather than a coffee break.
DEADLINE = 10.0


def run(coroutine):
    """Runs one coroutine to completion, under a deadline."""

    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


ALL_SOCKET_TYPES = [
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
]


def endpoints(tmp_path):
    """One endpoint per transport, all three of which the library implements."""
    return {
        "tcp": "tcp://127.0.0.1:0",
        "ipc": f"ipc://{tmp_path}/round-trip.sock",
        "inproc": "inproc://round-trip",
    }


@pytest.mark.parametrize("transport", ["tcp", "ipc", "inproc"])
def test_req_rep_round_trip(transport, tmp_path):
    """The round trip, over every transport."""

    async def exchange():
        context = weida_zmq.Context()
        server = weida_zmq.RepSocket(context)
        client = weida_zmq.ReqSocket(context)

        bound = await server.bind(endpoints(tmp_path)[transport])
        await client.connect(bound)

        await client.send(b"Hello")
        assert await server.recv() == [b"Hello"]
        await server.send(b"World")
        assert await client.recv() == [b"World"]

        # A second exchange on the same pair: the REQ state machine is back in
        # step rather than stuck after the first.
        await client.send(b"again")
        assert await server.recv() == [b"again"]
        await server.send(b"and again")
        assert await client.recv() == [b"and again"]

        await client.close()
        await server.close()
        # `close()` tears the connections down; the socket's slot under
        # ZMQ_MAX_SOCKETS is the object's, and comes back when Python drops
        # it. A shutdown before that reports the sockets still held, which is
        # a number rather than an error.
        assert await context.shutdown() == 2
        del client, server
        assert await context.shutdown() == 0

    run(exchange())


def test_a_bound_tcp_endpoint_reports_the_port_the_kernel_chose():
    async def bind():
        context = weida_zmq.Context()
        socket = weida_zmq.RepSocket(context)
        bound = await socket.bind("tcp://127.0.0.1:0")
        assert bound.startswith("tcp://127.0.0.1:")
        assert bound != "tcp://127.0.0.1:0"
        assert await socket.last_endpoint() == bound

    run(bind())


def test_a_multipart_message_crosses_whole():
    async def exchange():
        context = weida_zmq.Context()
        server = weida_zmq.RepSocket(context)
        client = weida_zmq.ReqSocket(context)
        bound = await server.bind("inproc://multipart")
        await client.connect(bound)

        await client.send([b"one", b"", b"three"])
        assert await server.recv() == [b"one", b"", b"three"]
        await server.send([b"a", b"b"])
        assert await client.recv() == [b"a", b"b"]

    run(exchange())


@pytest.mark.parametrize("name,kind", ALL_SOCKET_TYPES)
def test_every_socket_type_binds_connects_and_disconnects(name, kind, tmp_path):
    """All eleven, with the endpoint surface every socket has."""

    async def endpoints_of_one():
        context = weida_zmq.Context()
        socket = getattr(weida_zmq, name)(context)
        assert socket.socket_type == kind
        assert await socket.last_endpoint() is None

        bound = await socket.bind(f"ipc://{tmp_path}/{name}.sock")
        assert await socket.last_endpoint() == bound
        await socket.unbind(bound)

        other = getattr(weida_zmq, name)(context)
        # PAIR takes exactly one peer, so every type connects at most once here.
        second = await other.bind(f"inproc://{name}")
        await socket.connect(second)
        discarded = await socket.disconnect(second)
        assert discarded.outgoing == 0
        assert discarded.incoming == 0

        await socket.close()
        await other.close()

    run(endpoints_of_one())


def test_the_context_has_the_three_constructors():
    async def three():
        owned = weida_zmq.Context(max_sockets=17, worker_threads=2, close_budget=0.5)
        assert owned.max_sockets == 17
        assert owned.worker_threads == 2
        assert owned.close_budget == 0.5

        # `Context.sharing` runs on the first context's reactor, and is its own
        # context: its own ceiling and its own inproc namespace.
        shared = weida_zmq.Context.sharing(owned, max_sockets=3)
        assert shared.max_sockets == 3
        socket = weida_zmq.PairSocket(shared)
        assert await socket.bind("inproc://shared") == "inproc://shared"
        # The same name is free in the other context, which is what "two
        # contexts are two ZeroMQ instances" means.
        twin = weida_zmq.PairSocket(owned)
        assert await twin.bind("inproc://shared") == "inproc://shared"
        assert shared.socket_count == 1
        assert owned.socket_count == 1

        # `Context.current()` wants an ambient Tokio reactor, which a Python
        # process does not have. It says so with libzmq's name for it.
        with pytest.raises(weida_zmq.EMTHREAD) as refused:
            weida_zmq.Context.current()
        assert refused.value.errno == "EMTHREAD"

    run(three())


def test_the_close_budget_is_finite_by_default():
    """One of the two defaults that deliberately differ from libzmq."""
    assert weida_zmq.Context().close_budget == 1.0


def test_a_refused_option_is_refused_where_it_is_configured():
    with pytest.raises(weida_zmq.EINVAL) as refused:
        weida_zmq.Context(max_sockets=0)
    assert refused.value.errno == "EINVAL"
    assert "ZMQ_MAX_SOCKETS" in refused.value.cause

    with pytest.raises(weida_zmq.EINVAL):
        weida_zmq.Context(close_budget=-1.0)


def test_the_socket_ceiling_is_the_contexts():
    context = weida_zmq.Context(max_sockets=1)
    first = weida_zmq.PairSocket(context)
    with pytest.raises(weida_zmq.EMFILE) as full:
        weida_zmq.PairSocket(context)
    assert full.value.errno == "EMFILE"
    assert isinstance(full.value, weida_zmq.ZmqError)
    assert first.socket_type == "PAIR"


def test_every_failure_is_its_own_class_under_one_base():
    async def refusals():
        context = weida_zmq.Context()
        socket = weida_zmq.DealerSocket(context)

        # A transport this library does not implement is named, not called a
        # typo.
        with pytest.raises(weida_zmq.EPROTONOSUPPORT) as unsupported:
            await socket.connect("udp://127.0.0.1:5555")
        assert unsupported.value.errno == "EPROTONOSUPPORT"
        assert isinstance(unsupported.value, weida_zmq.ZmqError)

        # An endpoint that was never bound.
        with pytest.raises(weida_zmq.ENOENT):
            await socket.unbind("inproc://never-bound")

        # A malformed endpoint.
        with pytest.raises(weida_zmq.EINVAL):
            await socket.connect("tcp://")

        # The classes are distinct, which is the whole point of having one per
        # errno rather than one for all of them.
        assert weida_zmq.EAGAIN is not weida_zmq.ETERM
        assert issubclass(weida_zmq.EAGAIN, weida_zmq.ZmqError)
        assert issubclass(weida_zmq.ZmqError, Exception)

    run(refusals())


def test_the_req_state_machine_reports_efsm():
    """`EFSM` is recoverable by discarding the socket, which Lazy Pirate needs."""

    async def out_of_step():
        context = weida_zmq.Context()
        client = weida_zmq.ReqSocket(context)
        server = weida_zmq.RepSocket(context)
        bound = await server.bind("inproc://efsm")
        await client.connect(bound)

        with pytest.raises(weida_zmq.EFSM) as wrong_order:
            await client.recv()
        assert wrong_order.value.errno == "EFSM"

        await client.send(b"request")
        with pytest.raises(weida_zmq.EFSM):
            await client.send(b"a second request")

    run(out_of_step())


def test_a_str_is_not_a_payload():
    async def refuse():
        context = weida_zmq.Context()
        socket = weida_zmq.ReqSocket(context)
        with pytest.raises(TypeError):
            await socket.send("a str is not bytes")

    run(refuse())


def test_the_module_names_what_it_exports():
    exported = set(weida_zmq.__all__)
    for name, _ in ALL_SOCKET_TYPES:
        assert name in exported
        assert hasattr(weida_zmq, name)
    assert {"Context", "ZmqError", "EAGAIN", "ETERM"} <= exported
    for name in exported:
        assert hasattr(weida_zmq, name), name


def test_an_ipc_endpoint_is_a_path_that_exists():
    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "socket"

        async def bind():
            context = weida_zmq.Context()
            socket = weida_zmq.RepSocket(context)
            bound = await socket.bind(f"ipc://{path}")
            assert bound == f"ipc://{path}"
            assert path.is_socket()

        run(bind())
