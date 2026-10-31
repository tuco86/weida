"""The asyncio surface: the classes of `weida` whose methods wait, with
those methods as coroutines (the same arguments minus `timeout`). Every
wait goes through a readiness descriptor watched by the running loop;
cancelling a coroutine cancels its operation. The value classes are
`weida`'s own."""

from typing import Optional

from weida import _sync
from weida._native import lib
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


class Replier(_sync.Replier):
    async def accept(self) -> "IncomingRequest":  # type: ignore[override]
        """The next request, as soon as its header arrives."""
        out, attempt = self._accept_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._request(out[0])


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
