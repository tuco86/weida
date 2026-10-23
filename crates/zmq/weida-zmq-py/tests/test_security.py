"""PLAIN, CURVE, and a ZAP handler written in Python.

The handler is the interesting part: 27/ZAP is an **in-process
request-reply dialog** over `inproc://zeromq.zap.01`, so a handler written in
Python is a `RepSocket` of this module bound to that endpoint and nothing else
— no callback registration, no Rust glue, and no GIL held while the Rust
handshake waits for the answer, because the Rust side is talking to a socket
rather than calling into Python.

The refusal test asserts that a 400 stops the connection
**before any message flows**, proved by the absence of the message rather than
by the status code alone.
"""

import asyncio
import subprocess
import sys

import pytest

import waits
import weida_zmq

DEADLINE = 10.0

# Frame order of a ZAP request, once the REP socket has stripped the envelope
# (27/ZAP, and `weida_zmq`'s own zap module): version, request id, domain,
# address, identity, mechanism, then the mechanism's credentials.
VERSION, REQUEST_ID, DOMAIN, ADDRESS, IDENTITY, MECHANISM = range(6)


def run(coroutine):
    return waits.run(coroutine, DEADLINE)


class ZapHandler:
    """A ZAP handler, in Python, answering whatever it is told to answer."""

    def __init__(self, context, decide):
        self.socket = weida_zmq.RepSocket(context)
        self.decide = decide
        self.requests = []
        self.task = None

    async def start(self):
        await self.socket.bind(weida_zmq.ZAP_ENDPOINT)
        self.task = asyncio.create_task(self.serve())

    async def serve(self):
        while True:
            request = await self.socket.recv()
            self.requests.append(list(request))
            status, text, user_id = self.decide(request)
            await self.socket.send(
                [
                    b"1.0",
                    request[REQUEST_ID],
                    status,
                    text,
                    user_id,
                    b"",  # metadata, whose meaning is an application's
                ]
            )

    async def stop(self):
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        await self.socket.close()


def plain_server_options(domain="test"):
    options = weida_zmq.SocketOptions()
    options.set("ZMQ_PLAIN_SERVER", True)
    options.set("ZMQ_ZAP_DOMAIN", domain)
    return options


def plain_client_options(username="admin", password="secret"):
    options = weida_zmq.SocketOptions()
    options.set("ZMQ_PLAIN_USERNAME", username)
    options.set("ZMQ_PLAIN_PASSWORD", password)
    return options


def test_a_python_zap_handler_answers_200_and_the_message_flows():
    def allow(request):
        assert request[VERSION] == b"1.0"
        assert request[DOMAIN] == b"test"
        assert request[MECHANISM] == b"PLAIN"
        # PLAIN's credentials are two frames: username and password.
        assert request[6] == b"admin"
        assert request[7] == b"secret"
        return b"200", b"welcome", b"admin"

    async def exchange():
        context = weida_zmq.Context(worker_threads=2)
        handler = ZapHandler(context, allow)
        await handler.start()

        sink = weida_zmq.PullSocket(context, plain_server_options())
        endpoint = await sink.bind("tcp://127.0.0.1:0")
        source = weida_zmq.PushSocket(context, plain_client_options())
        await source.connect(endpoint)

        await source.send(b"authorized")
        assert await sink.recv() == [b"authorized"]
        assert handler.requests, "the handler was asked"

        # The user id the handler issued is readable on the peer, as its own
        # type, and it is not the routing id.
        peers = await sink.peers()
        authorized = [peer for peer in peers if peer.user_id is not None]
        assert authorized, "a 200 carries a user id"
        assert authorized[0].user_id == weida_zmq.ZapUserId("admin")
        assert authorized[0].user_id == "admin"
        assert authorized[0].routing_id != authorized[0].user_id

        await handler.stop()

    run(exchange())


@pytest.mark.parametrize(
    "status,text",
    [(b"400", b"no"), (b"300", b"try again later"), (b"500", b"broken handler")],
)
def test_a_refusal_stops_the_connection_before_any_message_flows(status, text):
    """The assertion is the **absence** of the message, not the status code."""

    def refuse(_request):
        return status, text, b""

    async def refused():
        context = weida_zmq.Context(worker_threads=2)
        handler = ZapHandler(context, refuse)
        await handler.start()

        sink = weida_zmq.PullSocket(context, plain_server_options())
        endpoint = await sink.bind("tcp://127.0.0.1:0")
        source = weida_zmq.PushSocket(context, plain_client_options())
        await source.connect(endpoint)

        # The send is a local queue operation; what must not happen is the
        # message arriving at a socket whose handler said no.
        try:
            await source.send(b"unauthorized", timeout=0.5)
        except weida_zmq.EAGAIN:
            pass  # nowhere to send it, which is also a refused connection

        with pytest.raises(weida_zmq.EAGAIN):
            await sink.recv(timeout=1.0)
        assert handler.requests, "the handler was asked before anything flowed"

        # And no peer of the server was ever authorized.
        assert all(peer.user_id is None for peer in await sink.peers())

        await handler.stop()

    run(refused())


def test_curve_keys_are_taken_as_32_bytes_or_40_characters_of_z85():
    public, secret = weida_zmq.curve_keypair()
    assert isinstance(public, weida_zmq.CurveKey)
    assert len(public.bytes) == 32
    assert len(public.z85) == 40
    # Both forms name the same key.
    assert weida_zmq.CurveKey(public.bytes) == public
    assert weida_zmq.CurveKey(public.z85) == public
    assert weida_zmq.CurveKey(public.z85.encode()) == public
    # X25519 derives the public key from the secret one, which is why a CURVE
    # server need not be told its own.
    assert secret.public_key() == public
    assert public != secret
    for wrong in [b"", b"too short", b"x" * 31, "Z" * 39]:
        with pytest.raises(weida_zmq.EINVAL):
            weida_zmq.CurveKey(wrong)


def test_a_curve_key_never_shows_its_octets_in_a_repr_or_a_str():
    """`CurveKey` is one type for the public and the secret half, so none of
    them may print: a log line, a traceback or an assertion diff holding the
    secret half would be the secret on a disk."""
    public, secret = weida_zmq.curve_keypair()
    for key in (public, secret, weida_zmq.CurveKey(secret.z85)):
        for text in (repr(key), str(key), f"{key}", f"{[key]}", f"{{'k': {key!r}}}"):
            assert key.z85 not in text
            assert key.bytes.hex() not in text.lower()
            assert key.z85[:8] not in text
            assert "CurveKey" in text, "it still says what it is"


def test_a_curve_keys_hash_is_not_made_of_its_octets():
    public, secret = weida_zmq.curve_keypair()
    leading = int.from_bytes(secret.bytes[:8], "little")
    assert hash(secret) not in (leading, leading - (1 << 64))
    # Equal keys still hash equal and work as dictionary keys: a ZAP handler's
    # allow list is a set of them.
    twin = weida_zmq.CurveKey(secret.z85)
    assert hash(twin) == hash(secret)
    assert {secret: "s", public: "p"}[twin] == "s"
    assert len({public, weida_zmq.CurveKey(public.bytes), secret}) == 2


def test_a_curve_keys_hash_is_keyed_per_process():
    """Two processes hash one key differently, so a hash read in one tells
    nothing about the octets and cannot be compared with another's."""
    program = (
        "import weida_zmq; "
        "print(hash(weida_zmq.CurveKey(b'0123456789abcdef0123456789abcdef')))"
    )
    seen = {
        subprocess.run(
            [sys.executable, "-c", program],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for _ in range(3)
    }
    assert len(seen) > 1


def test_curve_keys_compare_by_every_octet():
    key = weida_zmq.CurveKey(bytes(range(32)))
    assert key == weida_zmq.CurveKey(bytes(range(32)))
    for position in range(32):
        other = bytearray(range(32))
        other[position] ^= 1
        assert key != weida_zmq.CurveKey(bytes(other)), position
    assert key != bytes(range(32))
    assert key != key.z85


def test_curve_authenticates_both_ends_and_zap_sees_the_clients_key():
    server_public, server_secret = weida_zmq.curve_keypair()
    client_public, client_secret = weida_zmq.curve_keypair()
    seen = {}

    def allow(request):
        assert request[MECHANISM] == b"CURVE"
        # CURVE's credential is the peer's long-term public key, 32 octets.
        seen["key"] = request[6]
        return b"200", b"", b"curve-client"

    async def exchange():
        context = weida_zmq.Context(worker_threads=2)
        handler = ZapHandler(context, allow)
        await handler.start()

        server_options = weida_zmq.SocketOptions()
        server_options.set("ZMQ_CURVE_SERVER", True)
        server_options.set("ZMQ_CURVE_SECRETKEY", server_secret)
        server_options.set("ZMQ_ZAP_DOMAIN", "test")
        sink = weida_zmq.PullSocket(context, server_options)
        endpoint = await sink.bind("tcp://127.0.0.1:0")

        client_options = weida_zmq.SocketOptions()
        client_options.set("ZMQ_CURVE_SERVERKEY", server_public)
        client_options.set("ZMQ_CURVE_PUBLICKEY", client_public)
        client_options.set("ZMQ_CURVE_SECRETKEY", client_secret)
        source = weida_zmq.PushSocket(context, client_options)
        await source.connect(endpoint)

        await source.send(b"encrypted")
        assert await sink.recv() == [b"encrypted"]
        assert seen["key"] == client_public.bytes
        assert (await sink.peers())[0].user_id == "curve-client"

        await handler.stop()

    run(exchange())


def test_the_three_identities_are_three_types_that_convert_to_nothing_else():
    key = weida_zmq.CurveKey(b"k" * 32)
    routing_id = weida_zmq.RoutingId(b"worker-3")
    user = weida_zmq.ZapUserId("admin")

    assert type(key) is not type(routing_id)
    assert type(routing_id) is not type(user)
    # None of them is any of the others, whatever their octets say.
    assert key != routing_id
    assert routing_id != user
    assert user != key
    # A routing id is octets a peer asserts about itself, checked here.
    assert routing_id.bytes == b"worker-3"
    assert routing_id == b"worker-3"
    for wrong in [b"", b"\x00leading-zero", b"x" * 256]:
        with pytest.raises(weida_zmq.EINVAL):
            weida_zmq.RoutingId(wrong)
    # And no weida identity exists here to convert to: the module has none.
    assert not [name for name in dir(weida_zmq) if "ingerprint" in name]
    assert not [name for name in dir(weida_zmq) if "rincipal" in name]


def test_a_routing_id_set_as_an_option_is_what_the_peer_announces():
    async def announce():
        context = weida_zmq.Context()
        router = weida_zmq.RouterSocket(context)
        endpoint = await router.bind("inproc://routing-id")

        options = weida_zmq.SocketOptions()
        options.set("ZMQ_ROUTING_ID", weida_zmq.RoutingId(b"worker-3"))
        dealer = weida_zmq.DealerSocket(context, options)
        await dealer.connect(endpoint)

        await dealer.send(b"hello")
        request = await router.recv()
        assert request[0] == b"worker-3", "ROUTER addresses the id the peer chose"

        peers = await router.peers()
        assert weida_zmq.RoutingId(b"worker-3") in [
            peer.routing_id for peer in peers if peer.routing_id is not None
        ]

    run(announce())


def test_plain_without_a_handler_refuses_rather_than_admits():
    """"The handler SHALL start before any server starts" — and if it did not."""

    async def no_handler():
        context = weida_zmq.Context(worker_threads=2)
        sink = weida_zmq.PullSocket(context, plain_server_options())
        endpoint = await sink.bind("tcp://127.0.0.1:0")
        source = weida_zmq.PushSocket(context, plain_client_options())
        await source.connect(endpoint)

        try:
            await source.send(b"never arrives", timeout=0.5)
        except weida_zmq.EAGAIN:
            pass
        with pytest.raises(weida_zmq.EAGAIN):
            await sink.recv(timeout=1.0)

    run(no_handler())
