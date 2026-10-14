"""Interop: this binding against libzmq through `pyzmq`, in both roles.

The interop bench of this binding (results in
[IMPLEMENTATION.md](../../../docs/IMPLEMENTATION.md) §4.15). Every pairing both
implementations support, each pattern with

* this module as the **bound** side and as the **connecting** side, and
* this module in **either role** of the pattern (the REQ and the REP, the
  DEALER and the ROUTER, and so on),

which is four runs per pattern, plus PLAIN and CURVE with the security server
on each side in turn.

`pyzmq` is not a dependency of this package. Where it is absent every test
here is skipped, with the install command in the skip message — the same place
LOOP.md §2 puts one.
"""

import time

import pytest

import weida_zmq
from weida_zmq import sync

zmq = pytest.importorskip(
    "zmq",
    reason="pyzmq is absent; install it into the repository venv with: "
    "uv pip install --python .venv pyzmq",
)

# libzmq's own timeouts, so that a disagreement between the two
# implementations is a failed assertion rather than a hung suite.
TIMEOUT_MS = 5000
TIMEOUT_S = 5.0

# The socket type names, as each side spells them.
WEIDA_CLASSES = {
    "REQ": "ReqSocket",
    "REP": "RepSocket",
    "DEALER": "DealerSocket",
    "ROUTER": "RouterSocket",
    "PUB": "PubSocket",
    "SUB": "SubSocket",
    "XPUB": "XPubSocket",
    "XSUB": "XSubSocket",
    "PUSH": "PushSocket",
    "PULL": "PullSocket",
    "PAIR": "PairSocket",
}

# Every pairing both implementations support, as (initiator, responder).
PAIRINGS = [
    ("REQ", "REP"),
    ("DEALER", "ROUTER"),
    ("DEALER", "REP"),
    ("REQ", "ROUTER"),
    ("PUSH", "PULL"),
    ("PUB", "SUB"),
    ("XPUB", "XSUB"),
    ("PAIR", "PAIR"),
]


@pytest.fixture
def foreign():
    """A libzmq context that goes away with the test."""
    context = zmq.Context()
    yield context
    context.destroy(linger=0)


@pytest.fixture
def native():
    """A context of this module, synchronous so a test reads as a script."""
    return sync.Context(worker_threads=2)


def foreign_socket(context, kind):
    socket = context.socket(getattr(zmq, kind))
    socket.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    socket.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
    socket.setsockopt(zmq.LINGER, 0)
    return socket


def native_socket(context, kind, options=None):
    return getattr(sync, WEIDA_CLASSES[kind])(context, options)


class Native:
    """One end of an exchange, spoken by this module."""

    def __init__(self, context, kind, options=None):
        self.kind = kind
        self.socket = native_socket(context, kind, options)

    def bind(self):
        return self.socket.bind("tcp://127.0.0.1:0")

    def connect(self, endpoint):
        self.socket.connect(endpoint)

    def subscribe(self, prefix):
        self.socket.subscribe(prefix)

    def send(self, frames):
        return self.socket.send(frames)

    def recv(self):
        return [bytes(frame) for frame in self.socket.recv(timeout=TIMEOUT_S)]

    def try_recv(self):
        try:
            return [bytes(frame) for frame in self.socket.recv(timeout=0.05)]
        except weida_zmq.EAGAIN:
            return None


class Foreign:
    """The same end, spoken by libzmq through pyzmq."""

    def __init__(self, context, kind):
        self.kind = kind
        self.socket = foreign_socket(context, kind)

    def bind(self):
        self.socket.bind("tcp://127.0.0.1:0")
        return self.socket.getsockopt(zmq.LAST_ENDPOINT).decode()

    def connect(self, endpoint):
        self.socket.connect(endpoint)

    def subscribe(self, prefix):
        self.socket.setsockopt(zmq.SUBSCRIBE, prefix)

    def send(self, frames):
        self.socket.send_multipart(frames if isinstance(frames, list) else [frames])

    def recv(self):
        return self.socket.recv_multipart()

    def try_recv(self):
        try:
            return self.socket.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again:
            return None


def exchange(initiator, responder):
    """One message each way, in whatever shape the pattern requires."""
    pattern = (initiator.kind, responder.kind)

    if pattern in {("PUB", "SUB"), ("XPUB", "XSUB")}:
        publisher, subscriber = initiator, responder
        if subscriber.kind == "SUB":
            # A SUB's subscription travels with its session, so it is set
            # once; subscriptions here are additive and not idempotent.
            subscriber.subscribe(b"topic")
        # The slow joiner, which applies to the subscription too: an XSUB's
        # subscription is a *message* sent upstream, and a publisher with no
        # peer yet drops what it is given rather than blocking. So both the
        # subscription and the payload are retried, bounded.
        for _ in range(300):
            if subscriber.kind == "XSUB":
                subscriber.send([b"\x01topic"])
            publisher.send([b"topic", b"payload"])
            received = subscriber.try_recv()
            if received is not None:
                assert received == [b"topic", b"payload"]
                return
            time.sleep(0.01)
        raise AssertionError(f"nothing crossed for {pattern}")

    if pattern == ("PUSH", "PULL"):
        initiator.send([b"task"])
        assert responder.recv() == [b"task"]
        return

    if pattern == ("PAIR", "PAIR"):
        initiator.send([b"ping"])
        assert responder.recv() == [b"ping"]
        responder.send([b"pong"])
        assert initiator.recv() == [b"pong"]
        return

    # The request-reply family: the envelope is the pattern, so what a ROUTER
    # sees and what a REQ sends differ by exactly the frames the RFC names.
    initiator.send([b"question"] if initiator.kind != "DEALER" else [b"", b"question"])
    request = responder.recv()
    if responder.kind == "ROUTER":
        routing_id = request[0]
        body = request[1:]
        assert body[-1] == b"question", request
        responder.send([routing_id, *body[:-1], b"answer"])
    else:
        assert request[-1] == b"question", request
        responder.send([b"answer"])
    reply = initiator.recv()
    assert reply[-1] == b"answer", reply


@pytest.mark.parametrize("initiator,responder", PAIRINGS)
@pytest.mark.parametrize("native_side", ["initiator", "responder"])
@pytest.mark.parametrize("native_binds", [True, False])
def test_every_pairing_both_implementations_support(
    native, foreign, initiator, responder, native_side, native_binds
):
    """Four runs per pattern: this module in each role, binding and connecting."""
    if native_side == "initiator":
        first = Native(native, initiator)
        second = Foreign(foreign, responder)
    else:
        first = Foreign(foreign, initiator)
        second = Native(native, responder)

    native_is_first = native_side == "initiator"
    binder = first if native_binds == native_is_first else second
    dialler = second if binder is first else first
    endpoint = binder.bind()
    dialler.connect(endpoint)

    exchange(first, second)


def test_plain_with_this_module_as_the_server(native, foreign):
    """A ZAP handler written in Python authorizes a libzmq client."""
    import threading

    handler = sync.RepSocket(native)
    handler.bind(weida_zmq.ZAP_ENDPOINT)
    answered = threading.Event()

    def serve():
        request = handler.recv(timeout=TIMEOUT_S)
        assert bytes(request[5]) == b"PLAIN"
        assert bytes(request[6]) == b"admin"
        assert bytes(request[7]) == b"secret"
        handler.send([b"1.0", bytes(request[1]), b"200", b"", b"admin", b""])
        answered.set()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        options = weida_zmq.SocketOptions()
        options.set("ZMQ_PLAIN_SERVER", True)
        options.set("ZMQ_ZAP_DOMAIN", "interop")
        server = sync.PullSocket(native, options)
        endpoint = server.bind("tcp://127.0.0.1:0")

        client = foreign_socket(foreign, "PUSH")
        client.plain_username = b"admin"
        client.plain_password = b"secret"
        client.connect(endpoint)
        client.send(b"plain from libzmq")

        assert server.recv(timeout=TIMEOUT_S) == [b"plain from libzmq"]
        assert answered.is_set()
    finally:
        thread.join(timeout=TIMEOUT_S)


def test_plain_with_libzmq_as_the_server(native, foreign):
    """libzmq's own authenticator, and this module as the PLAIN client."""
    authenticator = pytest.importorskip(
        "zmq.auth.thread",
        reason="pyzmq's authenticator is absent; install with: "
        "uv pip install --python .venv pyzmq",
    )
    auth = authenticator.ThreadAuthenticator(foreign)
    auth.start()
    try:
        auth.configure_plain(domain="*", passwords={"admin": "secret"})
        server = foreign_socket(foreign, "PULL")
        server.plain_server = True
        server.bind("tcp://127.0.0.1:0")
        endpoint = server.getsockopt(zmq.LAST_ENDPOINT).decode()

        options = weida_zmq.SocketOptions()
        options.set("ZMQ_PLAIN_USERNAME", "admin")
        options.set("ZMQ_PLAIN_PASSWORD", "secret")
        client = sync.PushSocket(native, options)
        client.connect(endpoint)
        client.send(b"plain from weida")

        assert server.recv_multipart() == [b"plain from weida"]
    finally:
        auth.stop()


def test_curve_with_this_module_as_the_server(native, foreign):
    """CURVE, with the boxes opened by libsodium on the other side."""
    if not zmq.has("curve"):
        pytest.skip("the installed libzmq has no CURVE; it needs libsodium")
    import threading

    server_public, server_secret = weida_zmq.curve_keypair()
    client_public, client_secret = weida_zmq.curve_keypair()

    handler = sync.RepSocket(native)
    handler.bind(weida_zmq.ZAP_ENDPOINT)

    def serve():
        request = handler.recv(timeout=TIMEOUT_S)
        assert bytes(request[5]) == b"CURVE"
        assert bytes(request[6]) == client_public.bytes
        handler.send([b"1.0", bytes(request[1]), b"200", b"", b"curve-client", b""])

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        options = weida_zmq.SocketOptions()
        options.set("ZMQ_CURVE_SERVER", True)
        options.set("ZMQ_CURVE_SECRETKEY", server_secret)
        options.set("ZMQ_ZAP_DOMAIN", "interop")
        server = sync.PullSocket(native, options)
        endpoint = server.bind("tcp://127.0.0.1:0")

        client = foreign_socket(foreign, "PUSH")
        client.curve_serverkey = server_public.z85.encode()
        client.curve_publickey = client_public.z85.encode()
        client.curve_secretkey = client_secret.z85.encode()
        client.connect(endpoint)
        client.send(b"curve from libzmq")

        assert server.recv(timeout=TIMEOUT_S) == [b"curve from libzmq"]
    finally:
        thread.join(timeout=TIMEOUT_S)


def test_curve_with_libzmq_as_the_server(native, foreign):
    if not zmq.has("curve"):
        pytest.skip("the installed libzmq has no CURVE; it needs libsodium")
    auth_module = pytest.importorskip(
        "zmq.auth.thread",
        reason="pyzmq's authenticator is absent; install with: "
        "uv pip install --python .venv pyzmq",
    )
    curve_allow_any = pytest.importorskip("zmq.auth").CURVE_ALLOW_ANY

    server_public, server_secret = weida_zmq.curve_keypair()
    client_public, client_secret = weida_zmq.curve_keypair()

    auth = auth_module.ThreadAuthenticator(foreign)
    auth.start()
    try:
        auth.configure_curve(domain="*", location=curve_allow_any)
        server = foreign_socket(foreign, "PULL")
        server.curve_server = True
        server.curve_secretkey = server_secret.z85.encode()
        server.curve_publickey = server_public.z85.encode()
        server.bind("tcp://127.0.0.1:0")
        endpoint = server.getsockopt(zmq.LAST_ENDPOINT).decode()

        options = weida_zmq.SocketOptions()
        options.set("ZMQ_CURVE_SERVERKEY", server_public)
        options.set("ZMQ_CURVE_PUBLICKEY", client_public)
        options.set("ZMQ_CURVE_SECRETKEY", client_secret)
        client = sync.PushSocket(native, options)
        client.connect(endpoint)
        client.send(b"curve from weida")

        assert server.recv_multipart() == [b"curve from weida"]
    finally:
        auth.stop()


def test_the_asyncio_surface_also_speaks_to_libzmq(foreign):
    """The matrix above is synchronous for readability; the wire is the same."""
    import asyncio

    async def exchange_async():
        context = weida_zmq.Context(worker_threads=2)
        server = weida_zmq.RepSocket(context)
        endpoint = await server.bind("tcp://127.0.0.1:0")

        client = foreign_socket(foreign, "REQ")
        client.connect(endpoint)
        client.send(b"from libzmq")

        assert await server.recv(timeout=TIMEOUT_S) == [b"from libzmq"]
        await server.send(b"from weida")
        assert client.recv_multipart() == [b"from weida"]

    asyncio.run(asyncio.wait_for(exchange_async(), TIMEOUT_S * 2))
