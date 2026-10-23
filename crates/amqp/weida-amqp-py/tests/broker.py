"""A scripted AMQP 1.0 peer, in Python, for the tests to talk to.

Not a broker: it holds no nodes, creates nothing and remembers nothing beyond
one link. It is the *other end* of the four exchanges the tests need, written
by hand so that the assertions are about what this binding put on the wire.

Why in Python at all, when the Rust tests already script a server: because a
Python test that needs a peer must not need a second process, a broker
install or a Rust test harness it cannot see into. `nats-server` is absent on
this machine and so is RabbitMQ (`docs/libraries/amqp.md` §9), so a hand-rolled
peer is what makes `pytest` prove a round trip at all.

The frame encoder here is deliberately minimal: the eight-octet header, a
described list for the performative, and the few field layouts these tests
need. It is not a codec and must never grow into one — the codec is
`weida-amqp-codec`, checked against the specification's own figures.
"""

import asyncio
import queue
import struct
import subprocess
import sys
import time

# How long one step of a test is expected to take at most. Past it a wait
# says so on stderr and goes on: a slow run is still a run, and a limit that
# fires on a loaded machine turns a healthy test red. The hang guard is the
# CI job's own budget. (The Rust side's `weida_testkit::within`, same rule.)
EXPECTED = 5.0


def _still_waiting(what, waited, expected):
    print(
        f"still waiting for {what}: {waited:.1f}s, past its expected {expected}s",
        file=sys.stderr,
        flush=True,
    )


async def within(what, awaitable, expected=EXPECTED):
    """Awaits `awaitable`, reporting at every multiple of `expected`, never failing."""
    task = asyncio.ensure_future(awaitable)
    started = time.monotonic()
    while True:
        done, _ = await asyncio.wait({task}, timeout=expected)
        if done:
            return task.result()
        _still_waiting(what, time.monotonic() - started, expected)


def wait_set(event, what, expected=EXPECTED):
    """Waits for a `threading.Event`, reporting at every multiple of `expected`."""
    started = time.monotonic()
    while not event.wait(expected):
        _still_waiting(what, time.monotonic() - started, expected)


def wait_get(source, what, expected=EXPECTED):
    """Takes from a `queue.Queue`, reporting at every multiple of `expected`."""
    started = time.monotonic()
    while True:
        try:
            return source.get(timeout=expected)
        except queue.Empty:
            _still_waiting(what, time.monotonic() - started, expected)


def wait_exit(process, what, expected=EXPECTED):
    """Waits for a `subprocess.Popen` to exit and returns its code."""
    started = time.monotonic()
    while True:
        try:
            return process.wait(timeout=expected)
        except subprocess.TimeoutExpired:
            _still_waiting(what, time.monotonic() - started, expected)


# Part 2 §2.7's descriptors, as the performatives the peer sends and reads.
OPEN = 0x10
BEGIN = 0x11
ATTACH = 0x12
FLOW = 0x13
TRANSFER = 0x14
DISPOSITION = 0x15
DETACH = 0x16
END = 0x17
CLOSE = 0x18

# Part 3 §3.4's four outcomes, and §3.2's body sections.
ACCEPTED = 0x24
REJECTED = 0x25
DATA = 0x75
AMQP_VALUE = 0x77

HEADER = b"AMQP\x00\x01\x00\x00"


def _u32(value):
    return struct.pack(">I", value)


def encode_string(text):
    octets = text.encode("utf-8")
    if len(octets) < 256:
        return b"\xa1" + bytes([len(octets)]) + octets
    return b"\xb1" + _u32(len(octets)) + octets


def encode_symbol(text):
    octets = text.encode("ascii")
    if len(octets) < 256:
        return b"\xa3" + bytes([len(octets)]) + octets
    return b"\xb3" + _u32(len(octets)) + octets


def encode_binary(octets):
    if len(octets) < 256:
        return b"\xa0" + bytes([len(octets)]) + octets
    return b"\xb0" + _u32(len(octets)) + octets


def encode_uint(value):
    if value == 0:
        return b"\x43"
    if value < 256:
        return b"\x52" + bytes([value])
    return b"\x70" + _u32(value)


def encode_ushort(value):
    return b"\x60" + struct.pack(">H", value)


def encode_bool(flag):
    return b"\x41" if flag else b"\x42"


NULL = b"\x40"


def encode_list_full(items):
    """A `list32`, always: these tests care about correctness, not compactness."""
    body = b"".join(items)
    return b"\xd0" + _u32(len(body) + 4) + _u32(len(items)) + body


def described(code, items):
    """`0x00`, a `smallulong` descriptor, and the list it describes."""
    return b"\x00\x53" + bytes([code]) + encode_list_full(items)


def frame(channel, body, payload=b""):
    size = 8 + len(body) + len(payload)
    return _u32(size) + b"\x02\x00" + struct.pack(">H", channel) + body + payload


def read_u32(data, at):
    return struct.unpack_from(">I", data, at)[0]


class Peer:
    """One connection, scripted.

    Reads whole frames and answers them. Every handler asserts only what the
    test it serves needs; anything else is passed over, because a test that
    failed on an unrelated field would be a test about the peer.
    """

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.channel = 4
        self.handle = 11
        self.delivery_id = 0

    async def read_frame(self):
        head = await self.reader.readexactly(8)
        size = read_u32(head, 0)
        rest = await self.reader.readexactly(size - 8)
        channel = struct.unpack_from(">H", head, 6)[0]
        return channel, rest

    async def read_performative(self):
        """The next frame's descriptor code and its raw body."""
        while True:
            _, body = await self.read_frame()
            if not body:
                # An empty frame is the keep-alive; nothing to answer.
                continue
            # `0x00 0x53 <code>` is the described list every performative is.
            assert body[0] == 0x00, body[:4]
            return body[2], body

    async def write(self, data):
        self.writer.write(data)
        await self.writer.drain()

    async def handshake(self):
        assert await self.reader.readexactly(8) == HEADER
        await self.write(HEADER)
        code, _ = await self.read_performative()
        assert code == OPEN, hex(code)
        # container-id, hostname, max-frame-size, channel-max
        await self.write(
            frame(
                0,
                described(
                    OPEN,
                    [
                        encode_string("scripted-peer"),
                        NULL,
                        encode_uint(65536),
                        encode_ushort(15),
                    ],
                ),
            )
        )

    async def answer_begin(self, incoming_window=400):
        code, _ = await self.read_performative()
        assert code == BEGIN, hex(code)
        await self.write(
            frame(
                self.channel,
                described(
                    BEGIN,
                    [
                        encode_ushort(0),  # remote-channel: the client's
                        encode_uint(0),  # next-outgoing-id
                        encode_uint(incoming_window),
                        encode_uint(400),  # outgoing-window
                        encode_uint(255),  # handle-max
                    ],
                ),
            )
        )

    async def answer_attach(self, role_is_sender, name=None):
        """The answering `attach`, in the role opposite the client's.

        `role_is_sender` is *this peer's* role, so a client that attached as a
        sender gets `False` here.

        The name is **echoed** rather than chosen: the answer is correlated by
        name, because the two ends pick their handles independently, so an
        answer naming something else is an *unsolicited* attach and the client
        is right to refuse it.
        """
        code, body = await self.read_performative()
        assert code == ATTACH, hex(code)
        name = name or first_string(body)
        items = [
            encode_string(name),
            encode_uint(self.handle),
            # `role` is a boolean and `true` means **receiver** (Part 2
            # §2.7.3), so a peer that is the sender encodes `false`.
            encode_bool(not role_is_sender),
            b"\x50\x02",  # snd-settle-mode: mixed
            b"\x50\x00",  # rcv-settle-mode: first
            described(0x28, [encode_string("q")]),  # source
            described(0x29, [encode_string("q")]),  # target
            NULL,  # unsettled
            encode_bool(False),  # incomplete-unsettled
            encode_uint(0) if role_is_sender else NULL,  # initial-delivery-count
        ]
        await self.write(frame(self.channel, described(ATTACH, items)))

    async def grant_credit(self, credit, delivery_count=0):
        """A `flow` carrying link credit, for a client that is the sender."""
        items = [
            encode_uint(0),  # next-incoming-id
            encode_uint(400),  # incoming-window
            encode_uint(0),  # next-outgoing-id
            encode_uint(400),  # outgoing-window
            # A frame names the link by **the sending end's** handle: each end
            # numbers independently, and the client resolves this one through
            # the handle its answer to the attach carried.
            encode_uint(self.handle),
            encode_uint(delivery_count),
            encode_uint(credit),
            encode_uint(0),  # available
            encode_bool(False),  # drain
            encode_bool(False),  # echo
        ]
        await self.write(frame(self.channel, described(FLOW, items)))

    async def read_transfer(self):
        """The next `transfer`, with the payload that followed it."""
        code, body = await self.read_performative()
        assert code == TRANSFER, hex(code)
        # The payload is everything after the described list, and the list's
        # own size field says where that is. Both constructors appear here,
        # because the client encodes canonically: `list8` for a performative
        # that fits in 255 octets, `list32` for one that does not.
        kind = body[3]
        if kind == 0xC0:
            return body[3 + 2 + body[4] :]
        assert kind == 0xD0, hex(kind)
        # `0x00 0x53 code 0xd0 <size:4> <count:4>`: the size counts the count
        # field and the elements, so the elements end 8 octets in plus it.
        return body[8 + read_u32(body, 4) :]

    async def settle(self, outcome=ACCEPTED, first=0, last=None):
        """A `disposition` from the receiver, settled."""
        items = [
            encode_bool(True),  # role: receiver
            encode_uint(first),
            NULL if last is None else encode_uint(last),
            encode_bool(True),  # settled
            described(outcome, []),
        ]
        await self.write(frame(self.channel, described(DISPOSITION, items)))

    async def send_transfer(self, payload, delivery_id=None, settled=False):
        """A `transfer` from this peer, for a client that is the receiver."""
        if delivery_id is None:
            delivery_id = self.delivery_id
            self.delivery_id += 1
        items = [
            encode_uint(self.handle),
            encode_uint(delivery_id),
            encode_binary(b"t-%d" % delivery_id),
            encode_uint(0),  # message-format
            encode_bool(settled),
            encode_bool(False),  # more
        ]
        await self.write(frame(self.channel, described(TRANSFER, items), payload))

    async def expect_close(self):
        """Reads until the client's `close` and answers it."""
        while True:
            code, _ = await self.read_performative()
            if code == CLOSE:
                break
        await self.write(frame(0, described(CLOSE, [NULL])))


def first_string(body):
    """The first field of a described list, as a string.

    Enough of a decoder for the one field these tests read back — `attach`'s
    `name`, which the answer has to echo.

    Both list constructors have to be handled: the client encodes canonically,
    so a performative that fits writes `list8` (`0xc0`, one-octet size and
    count) and only a large one writes `list32` (`0xd0`, four and four). A
    reader that assumed one form would work against this peer and fail against
    a real broker's client, which is the wrong way round for a test.
    """
    at = 3
    kind = body[at]
    if kind == 0xC0:
        at += 3
    elif kind == 0xD0:
        at += 9
    else:
        raise AssertionError(f"not a described list: {hex(kind)}")
    field = body[at]
    if field == 0xA1:
        length = body[at + 1]
        return body[at + 2 : at + 2 + length].decode("utf-8")
    if field == 0xB1:
        length = read_u32(body, at + 1)
        return body[at + 5 : at + 5 + length].decode("utf-8")
    raise AssertionError(f"the first field is not a string: {hex(field)}")


def data_section(payload):
    """One `data` section, as a message body."""
    return b"\x00\x53" + bytes([DATA]) + encode_binary(payload)


def value_section(text):
    """One `amqp-value` section holding a string."""
    return b"\x00\x53" + bytes([AMQP_VALUE]) + encode_string(text)
