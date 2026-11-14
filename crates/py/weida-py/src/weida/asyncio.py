"""The asyncio surface: the classes of `weida` whose methods wait, with
those methods as coroutines (the same arguments minus `timeout`). Every
wait goes through a readiness descriptor watched by the running loop;
cancelling a coroutine cancels its operation. The value classes are
`weida`'s own."""

from typing import Any, Coroutine, Optional, Tuple

from weida import _sync
from weida._enums import ErrorCode
from weida._native import ffi, lib
from weida._sync import _op_async
from weida._values import Message, ServerTls, TransferMeta
from weida._wait import call_async

__all__ = [
    "Delivery",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "OutgoingTransfer",
    "Replier",
    "ReplyStream",
    "Requester",
    "Runtime",
]


class Runtime(_sync.Runtime):
    async def shutdown(self) -> None:  # type: ignore[override]
        """Closes every endpoint and waits for the connections to go idle.
        Consumes the runtime."""
        op = self._start_shutdown()
        await _op_async(
            op,
            lib.weida_runtime_shutdown_op_wait,
            lib.weida_runtime_shutdown_op_fd,
            lib.weida_runtime_shutdown_op_drop,
        )


class Listener(_sync.Listener):
    async def bind_quic(self, addr: str, tls: ServerTls) -> _sync.Binding:  # type: ignore[override]
        """Serves QUIC on `addr`, e.g. `"127.0.0.1:0"`."""
        op = self._start_bind_quic(addr, tls)
        ptr = await _op_async(
            op,
            lib.weida_listener_bind_quic_op_wait,
            lib.weida_listener_bind_quic_op_fd,
            lib.weida_listener_bind_quic_op_drop,
            "weida_binding **",
        )
        return self._binding(ptr)


class Requester(_sync.Requester):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Dials `url`; the runtime redials it when the connection is lost."""
        op = self._start_connect(url)
        await _op_async(
            op,
            lib.weida_requester_connect_op_wait,
            lib.weida_requester_connect_op_fd,
            lib.weida_requester_connect_op_drop,
        )

    async def request(self, body: bytes) -> "IncomingTransfer":  # type: ignore[override]
        """Sends `body` and returns the reply as soon as its header arrives.
        A cancelled request abandons the exchange."""
        op = self._start_request(body)
        ptr = await _op_async(
            op,
            lib.weida_requester_request_op_wait,
            lib.weida_requester_request_op_fd,
            lib.weida_requester_request_op_drop,
            "weida_incoming_transfer **",
        )
        return IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    async def request_with(self, meta: Optional[TransferMeta], body: bytes) -> "IncomingTransfer":  # type: ignore[override]
        """Sends `body` with `meta` (copied when the request starts) and
        returns the reply as soon as its header arrives. A cancelled request
        abandons the exchange."""
        op = self._start_request_with(meta, body)
        ptr = await _op_async(
            op,
            lib.weida_requester_request_op_wait,
            lib.weida_requester_request_op_fd,
            lib.weida_requester_request_op_drop,
            "weida_incoming_transfer **",
        )
        return IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    async def open(  # type: ignore[override]
        self, meta: Optional[TransferMeta] = None
    ) -> Tuple["OutgoingTransfer", "ReplyStream"]:
        """Opens one exchange carrying `meta`, as `weida.Requester.open`
        does. A cancelled open abandons the exchange."""
        op = self._start_open(meta)
        ptrs = await _op_async(
            op,
            lib.weida_requester_open_op_wait,
            lib.weida_requester_open_op_fd,
            lib.weida_requester_open_op_drop,
            ("weida_outgoing_transfer **", "weida_reply_stream **"),
        )
        return self._opened(ptrs)


class Replier(_sync.Replier):
    async def accept(self) -> "IncomingRequest":  # type: ignore[override]
        """The next request, as soon as its header arrives."""
        out, attempt = self._accept_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._request(out[0])


class ReplyStream(_sync.ReplyStream):
    async def recv(self) -> "IncomingTransfer":  # type: ignore[override]
        """The reply, as soon as its header arrives, as
        `weida.ReplyStream.recv` gives it. Consumes the stream; a cancelled
        recv abandons the reply."""
        op = self._start_recv()
        ptr = await _op_async(
            op,
            lib.weida_reply_stream_recv_op_wait,
            lib.weida_reply_stream_recv_op_fd,
            lib.weida_reply_stream_recv_op_drop,
            "weida_incoming_transfer **",
        )
        return self._incoming(ptr)


class IncomingRequest(_sync.IncomingRequest):
    async def reply(self, meta: Optional[TransferMeta] = None) -> "OutgoingTransfer":  # type: ignore[override]
        """Opens the reply stream. Consumes the request."""
        op = self._start_reply(meta)
        ptr = await _op_async(
            op,
            lib.weida_incoming_request_reply_op_wait,
            lib.weida_incoming_request_reply_op_fd,
            lib.weida_incoming_request_reply_op_drop,
            "weida_outgoing_transfer **",
        )
        return self._outgoing(ptr)

    async def refuse(self, code: ErrorCode) -> None:  # type: ignore[override]
        """Refuses the exchange with `code` instead of a reply, as
        `weida.IncomingRequest.refuse` does. Consumes the request; a `code`
        that is not an `ErrorCode` is a `TypeError` and consumes nothing."""
        op = self._start_refuse(code)
        await _op_async(
            op,
            lib.weida_incoming_request_refuse_op_wait,
            lib.weida_incoming_request_refuse_op_fd,
            lib.weida_incoming_request_refuse_op_drop,
        )

    def canceled(self) -> Coroutine[Any, Any, None]:  # type: ignore[override]
        """An awaitable that finishes once the requester stops wanting a
        reply, as `weida.IncomingRequest.canceled` does. A plain method: the
        watch starts here, not at the first await, so take it before `reply`
        or `refuse` consumes the request; it outlives the request.
        Cancelling the await ends the watch, and an awaitable that is never
        awaited ends it when it is collected."""
        op = ffi.gc(self._start_canceled(), lib.weida_incoming_request_canceled_op_drop)
        return _op_async(
            op,
            lib.weida_incoming_request_canceled_op_wait,
            lib.weida_incoming_request_canceled_op_fd,
            ffi.release,
        )


class IncomingTransfer(_sync.IncomingTransfer):
    async def readinto(self, buffer) -> int:  # type: ignore[override]
        """Reads into `buffer`; 0 is the end of the payload."""
        count, size, attempt = self._read_attempt(buffer)
        if size == 0:
            return 0
        await call_async(self._waiters(), attempt, self.drop)
        return count[0]

    async def read(self, n: int) -> bytes:  # type: ignore[override]
        """Up to `n` bytes; `b""` at the end of the payload."""
        buffer = bytearray(n)
        got = await self.readinto(buffer)
        return bytes(buffer[:got])

    async def collect(self, max_bytes: int) -> Message:  # type: ignore[override]
        """The whole remaining payload, refusing more than `max_bytes`.
        Consumes the transfer."""
        op = self._start_collect(max_bytes)
        ptr = await _op_async(
            op,
            lib.weida_incoming_transfer_collect_op_wait,
            lib.weida_incoming_transfer_collect_op_fd,
            lib.weida_incoming_transfer_collect_op_drop,
            "weida_msg **",
        )
        return Message._take(ptr)


class OutgoingTransfer(_sync.OutgoingTransfer):
    async def write(self, data) -> int:  # type: ignore[override]
        """Writes some of `data`; returns how much flow control took."""
        count, attempt = self._write_attempt(data)
        await call_async(self._waiters(), attempt, self.drop)
        return count[0]

    async def write_all(self, data) -> None:  # type: ignore[override]
        """Writes all of `data`."""
        view = memoryview(data).cast("B")
        while len(view):
            view = view[await self.write(view) :]


class Delivery(_sync.Delivery):
    async def delivered(self) -> None:  # type: ignore[override]
        """Waits for the peer's transport to acknowledge every byte.
        Consumes the receipt."""
        op = self._start_delivered()
        await _op_async(
            op,
            lib.weida_delivery_delivered_op_wait,
            lib.weida_delivery_delivered_op_fd,
            lib.weida_delivery_delivered_op_drop,
        )


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
