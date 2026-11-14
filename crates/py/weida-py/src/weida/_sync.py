"""The synchronous surface: every class mirrors one C handle, every method
one C function with the `weida_<type>_` prefix dropped. A waiting method
takes `timeout` in seconds (`None` waits forever) and raises `Again` when
it passes."""

import time
from typing import Callable, Iterator, Optional, Tuple

from weida._enums import ErrorCode
from weida._errors import check
from weida._native import ffi, lib
from weida._stats import ConnectionStats, PeerEvent, _take_connection_stats
from weida._values import (
    ClientTls,
    IncomingMeta,
    Message,
    RuntimeConfig,
    ServerTls,
    TransferMeta,
    _Handle,
    _enum,
    _millis,
    _text,
    _with_transfer_meta,
)
from weida._wait import Waiters, call_async, call_sync


def _start(starter: Callable, op_type: str, *args):
    """Calls a start function (R3b) and returns its op."""
    out = ffi.new(op_type + " **")
    check(starter(*args, out))
    return out[0]


def _new_outs(out_type) -> tuple:
    """The out-parameters of an op wait: none for `None`, one for a C
    pointer type, one per type of a tuple of them."""
    if out_type is None:
        return ()
    if isinstance(out_type, tuple):
        return tuple(ffi.new(t) for t in out_type)
    return (ffi.new(out_type),)


def _results(out_type, outs: tuple):
    """What the wait wrote: `None`, the one value, or a tuple of them."""
    if out_type is None:
        return None
    if isinstance(out_type, tuple):
        return tuple(out[0] for out in outs)
    return outs[0][0]


def _op_sync(op, wait: Callable, drop: Callable, timeout: Optional[float], out_type=None):
    """Waits for an op's result and drops the op, which cancels it when it
    did not finish. `out_type` is `None`, a C pointer type, or a tuple of
    them (one value per out-parameter)."""
    try:
        outs = _new_outs(out_type)
        call_sync(lambda ms: wait(op, ms, *outs), timeout)
        return _results(out_type, outs)
    finally:
        drop(op)


async def _op_async(op, wait: Callable, fd_of: Callable, drop: Callable, out_type=None):
    """`_op_sync` on the running loop, through the op's descriptor.
    `out_type` is `None`, a C pointer type, or a tuple of them (one value
    per out-parameter)."""
    dropped = []

    def drop_once() -> None:
        if not dropped:
            dropped.append(True)
            drop(op)

    def fileno() -> int:
        fd = ffi.new("int64_t *")
        check(fd_of(op, fd))
        return fd[0]

    waiters = Waiters(fileno)
    try:
        outs = _new_outs(out_type)
        await call_async(waiters, lambda ms: wait(op, ms, *outs), drop_once)
        return _results(out_type, outs)
    finally:
        waiters.close()
        drop_once()


class Runtime(_Handle):
    """A weida runtime on a reactor of its own."""

    @classmethod
    def owned(cls, config: Optional[RuntimeConfig] = None) -> "Runtime":
        """`weida_runtime_owned`: weida's defaults when `config` is `None`."""
        out = ffi.new("weida_runtime **")
        if config is None:
            check(lib.weida_runtime_owned(ffi.NULL, out))
        else:
            cfg = config._handle()
            try:
                check(lib.weida_runtime_owned(cfg, out))
            finally:
                lib.weida_runtime_config_drop(cfg)
        runtime = cls.__new__(cls)
        _Handle.__init__(runtime, out[0], lib.weida_runtime_drop)
        return runtime

    def listener(self) -> "Listener":
        out = ffi.new("weida_listener **")
        check(lib.weida_runtime_listener(self._p(), out))
        return self._Listener(out[0], lib.weida_listener_drop, self)

    def requester(self, tls: ClientTls) -> "Requester":
        out = ffi.new("weida_requester **")
        check(lib.weida_runtime_requester(self._p(), tls._p(), out))
        return self._Requester(out[0], lib.weida_requester_drop, self)

    def suppressed_duplicates(self) -> int:
        """Duplicates discarded inside a negotiated deduplication window; 0
        without deduplication."""
        return lib.weida_runtime_suppressed_duplicates(self._p())

    def expired_transfers(self) -> int:
        """Outgoing transfers reset because their deadline passed, counted
        apart from a cancel."""
        return lib.weida_runtime_expired_transfers(self._p())

    def _start_shutdown(self):
        return _start(lib.weida_runtime_shutdown, "weida_runtime_shutdown_op", self._consume())

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """Closes every endpoint and waits for the connections to go idle.
        Consumes the runtime."""
        op = self._start_shutdown()
        _op_sync(op, lib.weida_runtime_shutdown_op_wait, lib.weida_runtime_shutdown_op_drop, timeout)


class Listener(_Handle):
    """Where a runtime serves."""

    def _start_bind_quic(self, addr: str, tls: ServerTls):
        return _start(
            lib.weida_listener_bind_quic,
            "weida_listener_bind_quic_op",
            self._p(),
            addr.encode(),
            tls._p(),
        )

    def _binding(self, ptr) -> "Binding":
        return Binding(ptr, lib.weida_binding_drop, self)

    def bind_quic(self, addr: str, tls: ServerTls, timeout: Optional[float] = None) -> "Binding":
        """Serves QUIC on `addr`, e.g. `"127.0.0.1:0"`."""
        op = self._start_bind_quic(addr, tls)
        ptr = _op_sync(
            op,
            lib.weida_listener_bind_quic_op_wait,
            lib.weida_listener_bind_quic_op_drop,
            timeout,
            "weida_binding **",
        )
        return self._binding(ptr)

    def bind_inproc(self, bus: str) -> "LocalBinding":
        """Answers on the in-process bus `bus`."""
        out = ffi.new("weida_local_binding **")
        check(lib.weida_listener_bind_inproc(self._p(), bus.encode(), out))
        return LocalBinding(out[0], lib.weida_local_binding_drop, self)

    def replier(self, path: str) -> "Replier":
        """Registers a replier at the endpoint path `path`."""
        out = ffi.new("weida_replier **")
        check(lib.weida_listener_replier(self._p(), path.encode(), out))
        return self._Replier(out[0], lib.weida_replier_drop, self)


class Binding(_Handle):
    """A QUIC binding; dropping it stops serving."""

    def local_addr(self) -> str:
        return _text(lib.weida_binding_local_addr, self._p())


class LocalBinding(_Handle):
    """An in-process binding; dropping it frees the bus name."""

    def bus(self) -> str:
        return _text(lib.weida_local_binding_bus, self._p())


class Requester(_Handle):
    """The requesting half of Req/Rep."""

    def _start_connect(self, url: str):
        return _start(lib.weida_requester_connect, "weida_requester_connect_op", self._p(), url.encode())

    def _start_request(self, body: bytes):
        data = ffi.from_buffer("uint8_t[]", body)
        return _start(
            lib.weida_requester_request, "weida_requester_request_op", self._p(), data, len(data)
        )

    def _start_request_with(self, meta: Optional[TransferMeta], body: bytes):
        data = ffi.from_buffer("uint8_t[]", body)
        return _with_transfer_meta(
            meta,
            lambda h: _start(
                lib.weida_requester_request_with,
                "weida_requester_request_op",
                self._p(),
                h,
                data,
                len(data),
            ),
        )

    def _start_open(self, meta: Optional[TransferMeta]):
        return _with_transfer_meta(
            meta,
            lambda h: _start(lib.weida_requester_open, "weida_requester_open_op", self._p(), h),
        )

    def _opened(self, ptrs) -> Tuple["OutgoingTransfer", "ReplyStream"]:
        body, reply = ptrs
        return (
            self._OutgoingTransfer(body, lib.weida_outgoing_transfer_drop, self),
            self._ReplyStream(reply, lib.weida_reply_stream_drop, self),
        )

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Dials `url`; the runtime redials it when the connection is lost."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_requester_connect_op_wait, lib.weida_requester_connect_op_drop, timeout)

    def request(self, body: bytes, timeout: Optional[float] = None) -> "IncomingTransfer":
        """Sends `body` and returns the reply as soon as its header arrives."""
        op = self._start_request(body)
        ptr = _op_sync(
            op,
            lib.weida_requester_request_op_wait,
            lib.weida_requester_request_op_drop,
            timeout,
            "weida_incoming_transfer **",
        )
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    def request_with(
        self, meta: Optional[TransferMeta], body: bytes, timeout: Optional[float] = None
    ) -> "IncomingTransfer":
        """Sends `body` with `meta` (copied when the request starts) and
        returns the reply as soon as its header arrives."""
        op = self._start_request_with(meta, body)
        ptr = _op_sync(
            op,
            lib.weida_requester_request_op_wait,
            lib.weida_requester_request_op_drop,
            timeout,
            "weida_incoming_transfer **",
        )
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    def open(
        self, meta: Optional[TransferMeta] = None, timeout: Optional[float] = None
    ) -> Tuple["OutgoingTransfer", "ReplyStream"]:
        """Opens one exchange carrying `meta` (copied when it starts):
        `(body, reply)`, the request body written like a socket and ended
        with `finish()`, and the stream its reply is awaited on. The halves
        are independent, so the reply can be read while the body is still
        open."""
        op = self._start_open(meta)
        ptrs = _op_sync(
            op,
            lib.weida_requester_open_op_wait,
            lib.weida_requester_open_op_drop,
            timeout,
            ("weida_outgoing_transfer **", "weida_reply_stream **"),
        )
        return self._opened(ptrs)

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_requester_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def events(self) -> "PeerEvents":
        """A new reader of this requester's peer events, from now on; each
        call is an independent reader."""
        out = ffi.new("weida_peer_events **")
        check(lib.weida_requester_events(self._p(), out))
        # No parent: the events end once the requester is gone, which
        # holding it would prevent.
        return self._PeerEvents(out[0], lib.weida_peer_events_drop)


class ReplyStream(_Handle):
    """The reply half of an exchange `Requester.open` opened. Dropping it
    before `recv` tells the replier nobody is listening: its `canceled`
    returns and its reply fails with `Canceled`."""

    def _start_recv(self):
        return _start(lib.weida_reply_stream_recv, "weida_reply_stream_recv_op", self._consume())

    def _incoming(self, ptr) -> "IncomingTransfer":
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self._parent)

    def recv(self, timeout: Optional[float] = None) -> "IncomingTransfer":
        """The reply, as soon as its header arrives; a refused request
        raises the refusal's error (`Rejected`, `UnknownEndpoint`, ...).
        Consumes the stream: a `recv` cut short by `timeout` abandons the
        reply, and the replier sees the requester gone."""
        op = self._start_recv()
        ptr = _op_sync(
            op,
            lib.weida_reply_stream_recv_op_wait,
            lib.weida_reply_stream_recv_op_drop,
            timeout,
            "weida_incoming_transfer **",
        )
        return self._incoming(ptr)


class Replier(_Handle):
    """The replying half of Req/Rep, serving one endpoint path."""

    _fd_of = lib.weida_replier_fd

    def path(self) -> str:
        return _text(lib.weida_replier_path, self._p())

    def _accept_attempt(self):
        handle = self._p()
        out = ffi.new("weida_incoming_request **")
        return out, lambda ms: lib.weida_replier_accept(handle, ms, out)

    def _request(self, ptr) -> "IncomingRequest":
        return self._IncomingRequest(ptr, lib.weida_incoming_request_drop, self)

    def accept(self, timeout: Optional[float] = None) -> "IncomingRequest":
        """The next request, as soon as its header arrives."""
        out, attempt = self._accept_attempt()
        call_sync(attempt, timeout)
        return self._request(out[0])


class IncomingRequest(_Handle):
    """One accepted request: its body and the right to reply."""

    def meta(self) -> IncomingMeta:
        out = ffi.new("weida_incoming_meta **")
        check(lib.weida_incoming_request_meta(self._p(), out))
        return IncomingMeta._take(out[0])

    def take_body(self) -> "IncomingTransfer":
        """The request body, read like a socket. Once only."""
        out = ffi.new("weida_incoming_transfer **")
        check(lib.weida_incoming_request_take_body(self._p(), out))
        return self._IncomingTransfer(out[0], lib.weida_incoming_transfer_drop, self)

    def connection(self) -> int:
        """The number of the connection this request arrived on: equal for
        one connection, never shared by two in this process. Not an
        address."""
        out = ffi.new("uint64_t *")
        check(lib.weida_incoming_request_connection(self._p(), out))
        return out[0]

    def _start_reply(self, meta: Optional[TransferMeta]):
        request = self._consume()
        return _with_transfer_meta(
            meta,
            lambda h: _start(
                lib.weida_incoming_request_reply, "weida_incoming_request_reply_op", request, h
            ),
        )

    def _outgoing(self, ptr) -> "OutgoingTransfer":
        return self._OutgoingTransfer(ptr, lib.weida_outgoing_transfer_drop, self._parent)

    def reply(
        self, meta: Optional[TransferMeta] = None, timeout: Optional[float] = None
    ) -> "OutgoingTransfer":
        """Opens the reply stream. Consumes the request."""
        op = self._start_reply(meta)
        ptr = _op_sync(
            op,
            lib.weida_incoming_request_reply_op_wait,
            lib.weida_incoming_request_reply_op_drop,
            timeout,
            "weida_outgoing_transfer **",
        )
        return self._outgoing(ptr)

    def _start_refuse(self, code: ErrorCode):
        # Checked before the request is consumed: C consumes it even for a
        # code it refuses.
        value = _enum(ErrorCode)("code", code)
        return _start(
            lib.weida_incoming_request_refuse,
            "weida_incoming_request_refuse_op",
            self._consume(),
            value,
        )

    def refuse(self, code: ErrorCode, timeout: Optional[float] = None) -> None:
        """Refuses the exchange with `code` instead of a reply: the
        requester's request raises the code's error (`REJECTED` `Rejected`,
        `UNKNOWN_ENDPOINT` `UnknownEndpoint`, `UNSUPPORTED` `Unsupported`,
        `INTERNAL` `Transport`, `NO_REPLY` `NoReply`). Waits until the
        refusal is written; one the connection could not carry returns all
        the same, and one cut short by `timeout` may reach the requester as
        `NO_REPLY`. Consumes the request; a `code` that is not an
        `ErrorCode` is a `TypeError` and consumes nothing."""
        op = self._start_refuse(code)
        _op_sync(
            op,
            lib.weida_incoming_request_refuse_op_wait,
            lib.weida_incoming_request_refuse_op_drop,
            timeout,
        )

    def _start_canceled(self):
        return _start(
            lib.weida_incoming_request_canceled,
            "weida_incoming_request_canceled_op",
            self._p(),
        )

    def canceled(self, timeout: Optional[float] = None) -> None:
        """Waits until the requester stops wanting a reply: it gave up its
        request or dropped the reply, a finished reply was read whole, or
        the connection is gone. The watch starts at the call and outlives
        the request, so another thread may reply to, refuse or drop the
        request meanwhile (a finished reply half also ends the watch)."""
        op = self._start_canceled()
        _op_sync(
            op,
            lib.weida_incoming_request_canceled_op_wait,
            lib.weida_incoming_request_canceled_op_drop,
            timeout,
        )


class IncomingTransfer(_Handle):
    """An inbound payload, read like a socket or collected whole."""

    _fd_of = lib.weida_incoming_transfer_fd

    def meta(self) -> IncomingMeta:
        out = ffi.new("weida_incoming_meta **")
        check(lib.weida_incoming_transfer_meta(self._p(), out))
        return IncomingMeta._take(out[0])

    def connection(self) -> int:
        """The number of the connection this transfer arrived on: equal for
        one connection, never shared by two in this process. Not an
        address."""
        out = ffi.new("uint64_t *")
        check(lib.weida_incoming_transfer_connection(self._p(), out))
        return out[0]

    def _read_attempt(self, buffer):
        handle = self._p()
        view = ffi.from_buffer("uint8_t[]", buffer, require_writable=True)
        count = ffi.new("size_t *")
        return count, len(view), lambda ms: lib.weida_incoming_transfer_read(
            handle, view, len(view), ms, count
        )

    def readinto(self, buffer, timeout: Optional[float] = None) -> int:
        """Reads into `buffer`; 0 is the end of the payload."""
        count, size, attempt = self._read_attempt(buffer)
        if size == 0:
            return 0
        call_sync(attempt, timeout)
        return count[0]

    def read(self, n: int, timeout: Optional[float] = None) -> bytes:
        """Up to `n` bytes; `b""` at the end of the payload."""
        buffer = bytearray(n)
        got = self.readinto(buffer, timeout)
        return bytes(buffer[:got])

    def _start_collect(self, max_bytes: int):
        return _start(
            lib.weida_incoming_transfer_collect,
            "weida_incoming_transfer_collect_op",
            self._consume(),
            max_bytes,
        )

    def collect(self, max_bytes: int, timeout: Optional[float] = None) -> Message:
        """The whole remaining payload, refusing more than `max_bytes`.
        Consumes the transfer."""
        op = self._start_collect(max_bytes)
        ptr = _op_sync(
            op,
            lib.weida_incoming_transfer_collect_op_wait,
            lib.weida_incoming_transfer_collect_op_drop,
            timeout,
            "weida_msg **",
        )
        return Message._take(ptr)


class OutgoingTransfer(_Handle):
    """An outbound payload, written like a socket and finished into a
    delivery receipt. Dropped unfinished, it is reset with `CANCELED`."""

    _fd_of = lib.weida_outgoing_transfer_fd

    def _write_attempt(self, data):
        handle = self._p()
        view = ffi.from_buffer("uint8_t[]", data)
        count = ffi.new("size_t *")
        return count, lambda ms: lib.weida_outgoing_transfer_write(handle, view, len(view), ms, count)

    def write(self, data, timeout: Optional[float] = None) -> int:
        """Writes some of `data`; returns how much flow control took."""
        count, attempt = self._write_attempt(data)
        call_sync(attempt, timeout)
        return count[0]

    def write_all(self, data, timeout: Optional[float] = None) -> None:
        """Writes all of `data`; `timeout` bounds the whole call."""
        deadline = None if timeout is None else time.monotonic() + timeout
        view = memoryview(data).cast("B")
        while len(view):
            left = None if deadline is None else max(0.0, deadline - time.monotonic())
            view = view[self.write(view, left) :]

    def trace(self) -> Optional[str]:
        """The W3C traceparent this transfer carries; a reply inherits its
        request's."""
        return _text(lib.weida_outgoing_transfer_trace, self._p())

    def set_priority(self, priority: int) -> None:
        """Higher is sent first; 0 by default, a no-op on local transports."""
        check(lib.weida_outgoing_transfer_set_priority(self._p(), priority))

    def expire_after(self, seconds: float) -> None:
        """Past the deadline, `write` and `finish` raise `Expired` and the
        reader sees `Canceled`; on QUIC the deadline also holds after
        `finish`."""
        check(lib.weida_outgoing_transfer_expire_after_ms(self._p(), _millis("seconds", seconds)))

    def finish(self) -> "Delivery":
        """Ends the payload. Consumes the transfer."""
        out = ffi.new("weida_delivery **")
        check(lib.weida_outgoing_transfer_finish(self._consume(), out))
        return self._Delivery(out[0], lib.weida_delivery_drop, self._parent)


class Delivery(_Handle):
    """The receipt of a finished transfer."""

    def _start_delivered(self):
        return _start(lib.weida_delivery_delivered, "weida_delivery_delivered_op", self._consume())

    def delivered(self, timeout: Optional[float] = None) -> None:
        """Waits for the peer's transport to acknowledge every byte.
        Consumes the receipt."""
        op = self._start_delivered()
        _op_sync(op, lib.weida_delivery_delivered_op_wait, lib.weida_delivery_delivered_op_drop, timeout)


class PeerEvents(_Handle):
    """A reader of a requester's peer events, a queue (docs/libraries/weida-py.md
    §1): `recv` returns the next `PeerEvent`, or `None` once the requester is
    gone. A reader that fell more than 64 events behind gets one `MISSED`
    event carrying how many it lost."""

    _fd_of = lib.weida_peer_events_fd

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_peer_event **")
        return out, lambda ms: lib.weida_peer_events_recv(handle, ms, out)

    @staticmethod
    def _event(ptr) -> Optional[PeerEvent]:
        return None if ptr == ffi.NULL else PeerEvent._take(ptr)

    def recv(self, timeout: Optional[float] = None) -> Optional[PeerEvent]:
        """The next event, or `None` once the requester is gone."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._event(out[0])

    def __iter__(self) -> Iterator[PeerEvent]:
        """Every event until the requester is gone, each `recv` without a
        timeout."""
        while (event := self.recv()) is not None:
            yield event


# The classes a factory method hands out; `weida.asyncio` replaces them with
# its own.
Runtime._Listener = Listener
Runtime._Requester = Requester
Listener._Replier = Replier
Requester._IncomingTransfer = IncomingTransfer
Replier._IncomingRequest = IncomingRequest
IncomingRequest._IncomingTransfer = IncomingTransfer
IncomingRequest._OutgoingTransfer = OutgoingTransfer
OutgoingTransfer._Delivery = Delivery
Requester._OutgoingTransfer = OutgoingTransfer
Requester._ReplyStream = ReplyStream
ReplyStream._IncomingTransfer = IncomingTransfer
Requester._PeerEvents = PeerEvents

__all__ = [
    "Binding",
    "Delivery",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "LocalBinding",
    "OutgoingTransfer",
    "PeerEvents",
    "Replier",
    "ReplyStream",
    "Requester",
    "Runtime",
]
