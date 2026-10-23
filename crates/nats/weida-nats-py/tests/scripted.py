"""The NATS server the tests of this binding play against, scripted in Python.

`crates/nats/weida-nats/tests/support/mod.rs` does this for the Rust tests: a
listener in the same process with the server half written by hand, because
that is the only way to assert what the *client* put on the wire and in what
order. This is the same idea in Python, and for the same reason — a real
`nats-server` proves interoperability and is absent on most machines, while
these tests prove the sequence.

It goes one step further than the Rust harness in one respect: it **routes**.
`SUB` is recorded, a publication is delivered to every matching subscription
by the sid that subscription chose, a request whose subject nobody is
subscribed to gets the `NATS/1.0 503` answer, and `UNSUB <sid> <max>` stops
delivery at the count. That is what a server does, and it is what lets one
test assert that a wildcard subscription received a publication rather than
that a hand-written `MSG` was dispatched by its sid.

None of it is client behaviour: nothing here decodes what the client decodes
or decides what the client decides.

The server runs its own asyncio loop in a daemon thread, so the same object
serves the coroutine tests and the blocking ones. Every wait from the test
thread goes through `waits`: a slow run is reported and goes on, because no
test has a hard time limit on a real clock.
"""

import asyncio
import json
import threading

import waits

# An INFO that offers everything this client can claim: headers, so
# `no_responders` is negotiable, and protocol level 1.
FULL_INFO = {
    "server_id": "SCRIPTED",
    "server_name": "scripted",
    "version": "2.14.0",
    "proto": 1,
    "max_payload": 1048576,
    "headers": True,
}


def matches(pattern, subject):
    """Subject matching: `*` is exactly one token, `>` is one or more.

    The server's rule, because this is the server.
    """
    tokens = pattern.split(b".")
    parts = subject.split(b".")
    for index, token in enumerate(tokens):
        if token == b">":
            return index < len(parts)
        if index >= len(parts):
            return False
        if token != b"*" and token != parts[index]:
            return False
    return len(tokens) == len(parts)


def header_block(entries=None, status=None, description=None):
    """`NATS/1.0[ status[ description]]` and its `name: value` lines."""
    line = b"NATS/1.0"
    if status is not None:
        line += b" " + str(status).encode()
        if description is not None:
            line += b" " + description.encode()
    out = line + b"\r\n"
    for name, value in entries or []:
        out += name.encode() + b": " + value.encode() + b"\r\n"
    return out + b"\r\n"


def parse_header_block(raw):
    """The inverse, for asserting what the client sent."""
    line, _, rest = raw.partition(b"\r\n")
    tail = line[len(b"NATS/1.0"):].strip()
    status = None
    description = None
    if tail:
        code, _, text = tail.partition(b" ")
        status = int(code)
        description = text.decode() if text else None
    entries = []
    for entry in rest.split(b"\r\n"):
        if not entry:
            continue
        name, _, value = entry.partition(b":")
        entries.append((name.decode(), value.strip().decode()))
    return entries, status, description


class Op:
    """One operation the client sent, flat enough to assert on."""

    def __init__(
        self,
        verb,
        subject=None,
        sid=None,
        reply_to=None,
        queue_group=None,
        payload=b"",
        headers=None,
        status=None,
        max_msgs=None,
        connect=None,
    ):
        self.verb = verb
        self.subject = subject
        self.sid = sid
        self.reply_to = reply_to
        self.queue_group = queue_group
        self.payload = payload
        self.headers = headers
        self.status = status
        self.max_msgs = max_msgs
        self.connect = connect

    def render(self):
        """The operation as one assertable line.

        `-` stands for an absent optional argument, which is the thing the
        protocol's argument-count rule turns on: `PUB a 5` and `PUB a b 5`
        differ only in how many arguments follow the verb.
        """

        def text(raw):
            return "-" if raw is None else raw.decode("utf-8", "replace")

        if self.verb == "PUB":
            return f"PUB {text(self.subject)} {text(self.reply_to)} {text(self.payload)}"
        if self.verb == "HPUB":
            status = "-" if self.status is None else str(self.status)
            return (
                f"HPUB {text(self.subject)} {text(self.reply_to)} {status} "
                f"{text(self.payload)}"
            )
        if self.verb == "SUB":
            return f"SUB {text(self.subject)} {text(self.queue_group)} {self.sid}"
        if self.verb == "UNSUB":
            max_msgs = "-" if self.max_msgs is None else str(self.max_msgs)
            return f"UNSUB {self.sid} {max_msgs}"
        return self.verb

    def __repr__(self):
        return f"<Op {self.render()}>"


class _Subscription:
    def __init__(self, pattern, queue_group, sid):
        self.pattern = pattern
        self.queue_group = queue_group
        self.sid = sid
        self.delivered = 0
        self.max_msgs = None


class Server:
    """A NATS server for one client, on a port the kernel chose."""

    def __init__(self, info=None):
        self.info = dict(FULL_INFO if info is None else info)
        self.port = None
        self.connect = None
        self._ops = None
        self._loop = None
        self._thread = None
        self._writer = None
        self._serving = None
        self._ready = threading.Event()
        self._connected = threading.Event()
        self._subscriptions = []
        self._responders = []

    # -- lifecycle ----------------------------------------------------

    @classmethod
    def start(cls, info=None):
        server = cls(info=info)
        server._thread = threading.Thread(target=server._run, daemon=True)
        server._thread.start()
        waits.wait_set(server._ready, "the scripted server to start")
        return server

    def stop(self):
        """Ends the connection and the loop.

        The transport is closed first and on purpose: NATS has no `CLOSE`
        verb, so a server going away *is* the transport ending, and a test
        that only stopped the loop would leave the client's socket open and
        the client none the wiser.
        """
        if self._loop is not None and self._loop.is_running():
            try:
                self._call(self._shutdown())
            except (RuntimeError, TimeoutError):
                pass
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            waits.join(self._thread, "the scripted server's thread to end")

    async def _shutdown(self):
        if self._serving is not None:
            self._serving.cancel()
        if self._writer is not None:
            self._writer.close()

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._ops = asyncio.Queue()
        listener = self._loop.run_until_complete(
            asyncio.start_server(self._serve, "127.0.0.1", 0)
        )
        self.port = listener.sockets[0].getsockname()[1]
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            listener.close()
            self._loop.close()

    # -- the wire -----------------------------------------------------

    async def _serve(self, reader, writer):
        self._writer = writer
        self._serving = asyncio.current_task()
        # The server speaks first: INFO before anything the client may send.
        writer.write(b"INFO " + json.dumps(self.info).encode() + b"\r\n")
        await writer.drain()
        self._connected.set()
        try:
            while True:
                line = await reader.readuntil(b"\r\n")
                op = await self._read_op(reader, line[:-2])
                if op is None:
                    continue
                await self._route(op)
                await self._ops.put(op)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return

    async def _read_op(self, reader, line):
        parts = line.split()
        if not parts:
            return None
        verb = parts[0].upper().decode()
        if verb == "PING":
            self._writer.write(b"PONG\r\n")
            await self._writer.drain()
            return Op("PING")
        if verb == "PONG":
            return Op("PONG")
        if verb == "CONNECT":
            self.connect = json.loads(line[len(b"CONNECT ") :])
            return Op("CONNECT", connect=self.connect)
        if verb == "SUB":
            # `SUB <subject> [queue group] <sid>`.
            subject = parts[1]
            queue_group = parts[2] if len(parts) == 4 else None
            sid = _int(parts[-1])
            return Op("SUB", subject=subject, queue_group=queue_group, sid=sid)
        if verb == "UNSUB":
            sid = _int(parts[1])
            max_msgs = _int(parts[2]) if len(parts) == 3 else None
            return Op("UNSUB", sid=sid, max_msgs=max_msgs)
        if verb == "PUB":
            # `PUB <subject> [reply-to] <#bytes>`.
            subject = parts[1]
            reply_to = parts[2] if len(parts) == 4 else None
            payload = await self._read_payload(reader, _int(parts[-1]))
            return Op("PUB", subject=subject, reply_to=reply_to, payload=payload)
        if verb == "HPUB":
            # `HPUB <subject> [reply-to] <#header bytes> <#total bytes>`.
            subject = parts[1]
            reply_to = parts[2] if len(parts) == 5 else None
            header_len = _int(parts[-2])
            body = await self._read_payload(reader, _int(parts[-1]))
            entries, status, _description = parse_header_block(body[:header_len])
            return Op(
                "HPUB",
                subject=subject,
                reply_to=reply_to,
                payload=body[header_len:],
                headers=entries,
                status=status,
            )
        raise AssertionError(f"the client wrote an unknown operation: {line!r}")

    async def _read_payload(self, reader, count):
        body = await reader.readexactly(count + 2)
        assert body[-2:] == b"\r\n", "a payload is followed by CRLF"
        return body[:-2]

    async def _route(self, op):
        if op.verb == "SUB":
            self._subscriptions.append(
                _Subscription(op.subject, op.queue_group, op.sid)
            )
            return
        if op.verb == "UNSUB":
            for subscription in list(self._subscriptions):
                if subscription.sid != op.sid:
                    continue
                if op.max_msgs is None or subscription.delivered >= op.max_msgs:
                    self._subscriptions.remove(subscription)
                else:
                    subscription.max_msgs = op.max_msgs
            return
        if op.verb not in ("PUB", "HPUB"):
            return

        answered = False
        for subject, payload, headers in self._responders:
            if matches(subject, op.subject) and op.reply_to is not None:
                await self._deliver(op.reply_to, payload, headers)
                answered = True
        # The reply subject travels with the publication, because that is
        # what a responder answers to: "the reply subject that subscribers
        # can use to send a response back to the requestor".
        delivered = await self._deliver(
            op.subject, op.payload, op.headers, reply_to=op.reply_to
        )
        if delivered or answered or op.reply_to is None:
            return
        # No interest in the request's subject, so the fast no-responder
        # answer — which is only sent where the client negotiated it.
        if (self.connect or {}).get("no_responders") and (self.connect or {}).get(
            "headers"
        ):
            await self._deliver(op.reply_to, b"", None, status=503)

    async def _deliver(self, subject, payload, headers, status=None, reply_to=None):
        """One copy per matching subscription, one per queue group."""
        chosen = []
        groups = set()
        for subscription in self._subscriptions:
            if not matches(subscription.pattern, subject):
                continue
            if subscription.queue_group is not None:
                if subscription.queue_group in groups:
                    continue
                groups.add(subscription.queue_group)
            chosen.append(subscription)
        for subscription in chosen:
            if headers or status is not None:
                self._writer.write(
                    _hmsg(subject, subscription.sid, payload, headers, status, reply_to)
                )
            else:
                self._writer.write(_msg(subject, subscription.sid, payload, reply_to))
            subscription.delivered += 1
            if (
                subscription.max_msgs is not None
                and subscription.delivered >= subscription.max_msgs
            ):
                self._subscriptions.remove(subscription)
        if chosen:
            await self._writer.drain()
        return bool(chosen)

    # -- what a test drives -------------------------------------------

    def wait_connected(self):
        waits.wait_set(self._connected, "a client to connect")

    def respond_to(self, subject, payload=b"", headers=None):
        """Answer every request published to `subject` with `payload`.

        For the blocking surface above all: a synchronous `request` parks the
        calling thread, so the responder cannot be the test.
        """
        self._responders.append((subject.encode(), payload, headers))

    def op(self):
        """The next operation the client sent."""
        return self.ops(1)[0]

    def ops(self, count=1):
        """The next `count` operations, in order."""
        got = []
        for _ in range(count):
            future = asyncio.run_coroutine_threadsafe(self._ops.get(), self._loop)
            got.append(waits.result(future, "the next operation from the client"))
        return got

    def ops_until(self, verb):
        """Operations up to and including the first with this verb.

        The handshake writes a `CONNECT` and a `PING` before anything a test
        asked for, and a test that cares about neither says so with this.
        """
        got = []
        while True:
            got.append(self.op())
            if got[-1].verb == verb:
                return got

    def send(self, data):
        """Raw octets, for the cases a helper does not cover."""
        self._call(self._send(data))

    def send_msg(self, subject, sid, payload=b"", reply_to=None):
        self._call(self._send(_msg(subject, sid, payload, reply_to)))

    def send_hmsg(self, subject, sid, payload=b"", headers=None, status=None):
        self._call(self._send(_hmsg(subject, sid, payload, headers, status)))

    def send_info(self, info):
        self._call(self._send(b"INFO " + json.dumps(info).encode() + b"\r\n"))

    def subscriptions(self):
        """`(pattern, queue group, sid)` for every live subscription."""
        return [(s.pattern, s.queue_group, s.sid) for s in self._subscriptions]

    async def _send(self, data):
        self._writer.write(data)
        await self._writer.drain()

    def _call(self, coroutine):
        return waits.result(
            asyncio.run_coroutine_threadsafe(coroutine, self._loop),
            "the scripted server's loop to run a call",
        )

    # -- the same, for a caller that is inside an event loop ----------

    async def aop(self):
        return await asyncio.to_thread(self.op)

    async def aops(self, count=1):
        return await asyncio.to_thread(self.ops, count)

    async def aops_until(self, verb):
        return await asyncio.to_thread(self.ops_until, verb)


def _msg(subject, sid, payload, reply_to=None):
    """`MSG <subject> <sid> [reply-to] <#bytes>`."""
    head = b"MSG " + _bytes(subject) + b" " + str(sid).encode()
    if reply_to is not None:
        head += b" " + _bytes(reply_to)
    return head + b" " + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"


def _hmsg(subject, sid, payload, headers, status=None, reply_to=None):
    """`HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>`."""
    block = header_block(headers, status)
    head = b"HMSG " + _bytes(subject) + b" " + str(sid).encode()
    if reply_to is not None:
        head += b" " + _bytes(reply_to)
    head += (
        b" "
        + str(len(block)).encode()
        + b" "
        + str(len(block) + len(payload)).encode()
    )
    return head + b"\r\n" + block + payload + b"\r\n"


def _bytes(value):
    return value if isinstance(value, bytes) else value.encode()


def _int(raw):
    """A control-line count, which arrives as ASCII octets."""
    return int(raw.decode())
