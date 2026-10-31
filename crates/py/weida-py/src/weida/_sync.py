"""The synchronous surface: every class mirrors one C handle, every method
one C function with the `weida_<type>_` prefix dropped. A waiting method
takes `timeout` in seconds (`None` waits forever) and raises `Again` when
it passes."""

import time
from typing import Callable, Optional

from weida._errors import check
from weida._native import ffi, lib
from weida._values import (
    ClientTls,
    IncomingMeta,
    Message,
    RuntimeConfig,
    ServerTls,
    TransferMeta,
    _Handle,
    _text,
)
from weida._wait import Waiters, call_async, call_sync


def _start(starter: Callable, op_type: str, *args):
    """Calls a start function (R3b) and returns its op."""
    out = ffi.new(op_type + " **")
    check(starter(*args, out))
    return out[0]


def _op_sync(op, wait: Callable, drop: Callable, timeout: Optional[float], out_type=None):
    """Waits for an op's result and drops the op, which cancels it when it
    did not finish."""
    try:
        if out_type is None:
            call_sync(lambda ms: wait(op, ms), timeout)
            return None
        out = ffi.new(out_type)
        call_sync(lambda ms: wait(op, ms, out), timeout)
        return out[0]
    finally:
        drop(op)


async def _op_async(op, wait: Callable, fd_of: Callable, drop: Callable, out_type=None):
    """`_op_sync` on the running loop, through the op's descriptor."""
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
        if out_type is None:
            await call_async(waiters, lambda ms: wait(op, ms), drop_once)
            return None
        out = ffi.new(out_type)
        await call_async(waiters, lambda ms: wait(op, ms, out), drop_once)
        return out[0]
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

    def _start_reply(self, meta: Optional[TransferMeta]):
        request = self._consume()
        handle = ffi.NULL if meta is None else meta._handle()
        try:
            return _start(
                lib.weida_incoming_request_reply, "weida_incoming_request_reply_op", request, handle
            )
        finally:
            if handle != ffi.NULL:
                lib.weida_transfer_meta_drop(handle)

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


class IncomingTransfer(_Handle):
    """An inbound payload, read like a socket or collected whole."""

    _fd_of = lib.weida_incoming_transfer_fd

    def meta(self) -> IncomingMeta:
        out = ffi.new("weida_incoming_meta **")
        check(lib.weida_incoming_transfer_meta(self._p(), out))
        return IncomingMeta._take(out[0])

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

__all__ = [
    "Binding",
    "Delivery",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "LocalBinding",
    "OutgoingTransfer",
    "Replier",
    "Requester",
    "Runtime",
]
