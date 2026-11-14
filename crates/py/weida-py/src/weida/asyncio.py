"""The asyncio surface: the classes of `weida` whose methods wait, with
those methods as coroutines (the same arguments minus `timeout`). Every
wait goes through a readiness descriptor watched by the running loop;
cancelling a coroutine cancels its operation. The value classes are
`weida`'s own, except the identity and trust sources, whose `events()`
hands out a `weida.asyncio.IdentityEvents`, and the publisher, whose
`open()` hands out a `weida.asyncio.FanOut`."""

from typing import Any, AsyncIterator, Coroutine, Optional, Tuple

from weida import _sync, _values
from weida._enums import ErrorCode
from weida._native import ffi, lib
from weida._stats import PeerEvent
from weida._sync import Handoff, Received, _op_async
from weida._values import ClientTls, CursorSet, Drained, IdentityEvent, Message, ServerTls, TransferMeta
from weida._wait import call_async

__all__ = [
    "Binding",
    "BusMember",
    "Cursors",
    "Delivery",
    "Dish",
    "FanOut",
    "IdentityEvents",
    "IdentitySource",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "OutgoingTransfer",
    "Paired",
    "PeerEvents",
    "Publisher",
    "Puller",
    "Pusher",
    "Relay",
    "RelayRequest",
    "Replier",
    "ReplyStream",
    "Reporter",
    "Requester",
    "Respondent",
    "Runtime",
    "Subscriber",
    "SurveyRun",
    "Surveyor",
    "TrustSource",
    "WeakIdentitySource",
    "WeakTrustSource",
]


class Runtime(_sync.Runtime):
    _LOOP_RESOLVER = True

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

    async def drain(self, deadline: float) -> Drained:  # type: ignore[override]
        """Stops admitting work, gives every finished transfer whose receipt
        nobody holds until `deadline` seconds to reach the peer's transport,
        then closes as `shutdown` does within what is left of it. Cancelling
        aborts the drain where it is. Consumes the runtime."""
        op = self._start_drain(deadline)
        ptr = await _op_async(
            op,
            lib.weida_runtime_drain_op_wait,
            lib.weida_runtime_drain_op_fd,
            lib.weida_runtime_drain_op_drop,
            "weida_drained **",
        )
        return Drained._take(ptr)


class Listener(_sync.Listener):
    async def bind_quic(self, addr: str, tls: ServerTls) -> "Binding":  # type: ignore[override]
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

    async def bind_quic_socket(self, socket: int, tls: ServerTls) -> "Binding":  # type: ignore[override]
        """Serves QUIC on a UDP socket that is already bound: its descriptor
        (on Windows its SOCKET), e.g. `socket.socket.detach()` or a
        predecessor's `Handoff.take_socket()`. The descriptor is libweida's
        once the call reaches C; weida neither binds it nor sets options on
        it. A socket that is not UDP raises `Io`. A cancelled bind drops its
        op."""
        op = self._start_bind_quic_socket(socket, tls)
        ptr = await _op_async(
            op,
            lib.weida_listener_bind_quic_socket_op_wait,
            lib.weida_listener_bind_quic_socket_op_fd,
            lib.weida_listener_bind_quic_socket_op_drop,
            "weida_binding **",
        )
        return self._binding(ptr)

    async def bind_relay(self, relay: str, via: ClientTls, tls: ServerTls) -> "Binding":  # type: ignore[override]
        """Serves this listener's endpoints through the relay at `relay`
        (`weida://[relay-key@]host:port/.weida/relay`), registered under the
        key `via` presents; diallers reach them as
        `weida+relay://<key>@[relay-key@]host:port/path`. `tls` is the
        server identity and client trust, as for `bind_quic`. Raises
        `Rejected` where the relay refused the registration. A lost relay is
        redialled under the runtime's reconnect policy, reported on
        `Binding.relay_events()`. A cancelled bind drops its op."""
        op = self._start_bind_relay(relay, via, tls)
        ptr = await _op_async(
            op,
            lib.weida_listener_bind_relay_op_wait,
            lib.weida_listener_bind_relay_op_fd,
            lib.weida_listener_bind_relay_op_drop,
            "weida_binding **",
        )
        return self._binding(ptr)


class Binding(_sync.Binding):
    async def close(self) -> None:  # type: ignore[override]
        """Stops accepting and closes the binding's connections with
        SHUTDOWN; returns once the port is free to bind again, within the
        runtime's `shutdown_timeout`. A relayed binding ends its registration.
        Cancelling it aborts the close where it is: the port is then freed
        only by the runtime's shutdown."""
        await _op_async(
            self._start_close(),
            lib.weida_binding_close_op_wait,
            lib.weida_binding_close_op_fd,
            lib.weida_binding_close_op_drop,
        )

    async def hand_over(self) -> Handoff:  # type: ignore[override]
        """Hands the socket over to a successor: every connection is closed
        with RESTARTING, its close packet held in the `Handoff` and not
        sent. Raises `Unsupported` for a relayed binding and `Closed` once
        a close was started. Consumes the binding."""
        ptr = await _op_async(
            self._start_hand_over(),
            lib.weida_binding_hand_over_op_wait,
            lib.weida_binding_hand_over_op_fd,
            lib.weida_binding_hand_over_op_drop,
            "weida_handoff **",
        )
        return self._handoff(ptr)

    async def take_over(self, held) -> int:  # type: ignore[override]
        """Sends the close packets a predecessor held (`Handoff.bytes()`, any
        bytes-like), so its clients redial into this binding; call it once
        their endpoints are registered. Returns how many were sent. Bytes
        that are not a handoff, or are another address's, raise `Io`. The
        bytes need the integrity of the channel that carries the socket."""
        return await _op_async(
            self._start_take_over(held),
            lib.weida_binding_take_over_op_wait,
            lib.weida_binding_take_over_op_fd,
            lib.weida_binding_take_over_op_drop,
            "size_t *",
        )


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


class Pusher(_sync.Pusher):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Dials `url`; the runtime redials it when the connection is lost."""
        op = self._start_connect(url)
        await _op_async(
            op,
            lib.weida_pusher_connect_op_wait,
            lib.weida_pusher_connect_op_fd,
            lib.weida_pusher_connect_op_drop,
        )

    async def open(self, meta: Optional[TransferMeta] = None) -> "OutgoingTransfer":  # type: ignore[override]
        """Opens a one-way transfer to the next peer carrying `meta`, as
        `weida.Pusher.open` does. A cancelled open resets the transfer
        `CANCELED`."""
        op = self._start_open(meta)
        ptr = await _op_async(
            op,
            lib.weida_pusher_open_op_wait,
            lib.weida_pusher_open_op_fd,
            lib.weida_pusher_open_op_drop,
            "weida_outgoing_transfer **",
        )
        return self._outgoing(ptr)

    async def send(self, body) -> None:  # type: ignore[override]
        """Sends `body` (any bytes-like) as one message; returns once it is
        written or held in the outbox (a full outbox applies
        `RuntimeConfig.outbox_full`). A cancelled send drops its op."""
        op = self._start_send(body)
        await _op_async(
            op,
            lib.weida_pusher_send_op_wait,
            lib.weida_pusher_send_op_fd,
            lib.weida_pusher_send_op_drop,
        )

    async def send_with(self, meta: Optional[TransferMeta], body) -> None:  # type: ignore[override]
        """`send` with `meta`, copied when the send starts."""
        op = self._start_send_with(meta, body)
        await _op_async(
            op,
            lib.weida_pusher_send_op_wait,
            lib.weida_pusher_send_op_fd,
            lib.weida_pusher_send_op_drop,
        )


class Puller(_sync.Puller):
    async def recv(self) -> "IncomingTransfer":  # type: ignore[override]
        """The next pushed transfer, as soon as its header arrives. A `recv`
        cancelled loses nothing: the take in flight stays in the puller."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._incoming(out[0])


class Publisher(_sync.Publisher):
    """`weida.Publisher` whose `open()` hands out a `weida.asyncio.FanOut`."""


class FanOut(_sync.FanOut):
    async def write_within(self, chunk, limit: float) -> int:  # type: ignore[override]
        """Writes `chunk` to every subscriber still receiving, as
        `weida.FanOut.write_within` does; cancelling it abandons the
        transfer."""
        return await _op_async(
            self._start_write_within(chunk, limit),
            lib.weida_fan_out_write_within_op_wait,
            lib.weida_fan_out_write_within_op_fd,
            lib.weida_fan_out_write_within_op_drop,
            "size_t *",
        )


class Subscriber(_sync.Subscriber):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Dials `url` and subscribes there to every filter registered so
        far; the runtime redials it, filters included."""
        await _op_async(
            self._start_connect(url),
            lib.weida_subscriber_connect_op_wait,
            lib.weida_subscriber_connect_op_fd,
            lib.weida_subscriber_connect_op_drop,
        )

    async def subscribe(self, filter: str) -> None:  # type: ignore[override]
        """Registers interest in every topic `filter` matches, as
        `weida.Subscriber.subscribe` does."""
        await _op_async(
            self._start_subscribe(filter),
            lib.weida_subscriber_subscribe_op_wait,
            lib.weida_subscriber_subscribe_op_fd,
            lib.weida_subscriber_subscribe_op_drop,
        )

    async def unsubscribe(self, filter: str) -> None:  # type: ignore[override]
        """Withdraws `filter`; an unknown one is ignored."""
        await _op_async(
            self._start_unsubscribe(filter),
            lib.weida_subscriber_unsubscribe_op_wait,
            lib.weida_subscriber_unsubscribe_op_fd,
            lib.weida_subscriber_unsubscribe_op_drop,
        )

    async def grant(self, filter: str, limit: int) -> None:  # type: ignore[override]
        """States how many messages this subscriber accepts in total on
        `filter`, as `weida.Subscriber.grant` does."""
        await _op_async(
            self._start_grant(filter, limit),
            lib.weida_subscriber_grant_op_wait,
            lib.weida_subscriber_grant_op_fd,
            lib.weida_subscriber_grant_op_drop,
        )

    async def recv(self) -> "IncomingTransfer":  # type: ignore[override]
        """The next published copy, as soon as its header arrives; its topic
        is `meta().topic`. Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._incoming(out[0])


class Paired(_sync.Paired):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Dials `url`, once, as `weida.Paired.connect` does."""
        await _op_async(
            self._start_connect(url),
            lib.weida_paired_connect_op_wait,
            lib.weida_paired_connect_op_fd,
            lib.weida_paired_connect_op_drop,
        )

    async def open(self, meta: Optional[TransferMeta] = None) -> "OutgoingTransfer":  # type: ignore[override]
        """Opens a one-way transfer to the peer carrying `meta`, as
        `weida.Paired.open` does. A cancelled open resets the transfer
        `CANCELED`."""
        ptr = await _op_async(
            self._start_open(meta),
            lib.weida_paired_open_op_wait,
            lib.weida_paired_open_op_fd,
            lib.weida_paired_open_op_drop,
            "weida_outgoing_transfer **",
        )
        return self._outgoing(ptr)

    async def send(self, body) -> None:  # type: ignore[override]
        """Sends `body` (any bytes-like) as one message; returns once it is
        written or held in the outbox. A cancelled send drops its op."""
        await _op_async(
            self._start_send(body),
            lib.weida_paired_send_op_wait,
            lib.weida_paired_send_op_fd,
            lib.weida_paired_send_op_drop,
        )

    async def send_with(self, meta: Optional[TransferMeta], body) -> None:  # type: ignore[override]
        """`send` with `meta`, copied when the send starts."""
        await _op_async(
            self._start_send_with(meta, body),
            lib.weida_paired_send_op_wait,
            lib.weida_paired_send_op_fd,
            lib.weida_paired_send_op_drop,
        )

    async def recv(self) -> "IncomingTransfer":  # type: ignore[override]
        """The next transfer from the peer, as soon as its header arrives.
        Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._incoming(out[0])


class Surveyor(_sync.Surveyor):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Dials the respondent at `url`, adding it to the set every survey
        asks; the runtime redials it when the connection is lost."""
        await _op_async(
            self._start_connect(url),
            lib.weida_surveyor_connect_op_wait,
            lib.weida_surveyor_connect_op_fd,
            lib.weida_surveyor_connect_op_drop,
        )

    async def survey(self, body, deadline: float) -> "SurveyRun":  # type: ignore[override]
        """Asks every connected respondent `body`, as
        `weida.Surveyor.survey` does. A cancelled survey aborts it."""
        ptr = await _op_async(
            self._start_survey(body, deadline),
            lib.weida_surveyor_survey_op_wait,
            lib.weida_surveyor_survey_op_fd,
            lib.weida_surveyor_survey_op_drop,
            "weida_survey_run **",
        )
        return self._run(ptr)

    async def survey_with(  # type: ignore[override]
        self, meta: Optional[TransferMeta], body, deadline: float
    ) -> "SurveyRun":
        """`survey` with `meta`, copied when the survey starts."""
        ptr = await _op_async(
            self._start_survey_with(meta, body, deadline),
            lib.weida_surveyor_survey_op_wait,
            lib.weida_surveyor_survey_op_fd,
            lib.weida_surveyor_survey_op_drop,
            "weida_survey_run **",
        )
        return self._run(ptr)


class SurveyRun(_sync.SurveyRun):
    async def next(self, max_bytes: int) -> Optional[bytes]:  # type: ignore[override]
        """The next answer's bytes, or `None` once the run ended, as
        `weida.SurveyRun.next` does; a cancelled `next` leaves its read in
        flight for the next call (on the Windows Proactor loop it drops the
        run, §4)."""
        out, attempt = self._next_attempt(max_bytes)
        await call_async(self._waiters(), attempt, self.drop)
        return self._answer(out[0])


class Respondent(_sync.Respondent):
    async def accept(self) -> "IncomingRequest":  # type: ignore[override]
        """The next question, as soon as its header arrives: an
        `IncomingRequest`, answered as a replier's is. Raises `NotConnected`
        once the queue ended."""
        out, attempt = self._accept_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._request(out[0])


class BusMember(_sync.BusMember):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Joins the member at `url`; the runtime redials it when the
        connection is lost."""
        await _op_async(
            self._start_connect(url),
            lib.weida_bus_member_connect_op_wait,
            lib.weida_bus_member_connect_op_fd,
            lib.weida_bus_member_connect_op_drop,
        )

    async def send(self, body) -> int:  # type: ignore[override]
        """Sends `body` to every member this one dialled, as
        `weida.BusMember.send` does; returns how many it reached."""
        return await _op_async(
            self._start_send(body),
            lib.weida_bus_member_send_op_wait,
            lib.weida_bus_member_send_op_fd,
            lib.weida_bus_member_send_op_drop,
            "size_t *",
        )

    async def send_with(self, meta: Optional[TransferMeta], body) -> int:  # type: ignore[override]
        """`send` with `meta`, copied when the send starts."""
        return await _op_async(
            self._start_send_with(meta, body),
            lib.weida_bus_member_send_op_wait,
            lib.weida_bus_member_send_op_fd,
            lib.weida_bus_member_send_op_drop,
            "size_t *",
        )

    async def recv(self) -> "IncomingTransfer":  # type: ignore[override]
        """The next message from another member, as soon as its header
        arrives. Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._incoming(out[0])


class Dish(_sync.Dish):
    async def connect(self, url: str) -> None:  # type: ignore[override]
        """Dials the radio at `url` and joins there every filter joined so
        far, as `weida.Dish.connect` does."""
        await _op_async(
            self._start_connect(url),
            lib.weida_dish_connect_op_wait,
            lib.weida_dish_connect_op_fd,
            lib.weida_dish_connect_op_drop,
        )

    async def join(  # type: ignore[override]
        self, filter: str, *, max_age: Optional[float] = None, max_layer: Optional[int] = None
    ) -> None:
        """Joins every topic `filter` matches, as `weida.Dish.join` does."""
        await _op_async(
            self._start_join(filter, max_age, max_layer),
            lib.weida_dish_join_op_wait,
            lib.weida_dish_join_op_fd,
            lib.weida_dish_join_op_drop,
        )

    async def leave(self, filter: str) -> None:  # type: ignore[override]
        """Leaves `filter`; an unknown one is ignored."""
        await _op_async(
            self._start_leave(filter),
            lib.weida_dish_leave_op_wait,
            lib.weida_dish_leave_op_fd,
            lib.weida_dish_leave_op_drop,
        )

    async def recv(self) -> Received:  # type: ignore[override]
        """The next segment, a frozen `Received` whose stream segment
        transfer is a `weida.asyncio.IncomingTransfer`. A cancelled `recv`
        loses nothing. Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._received(out[0])


class Relay(_sync.Relay):
    async def accept(self) -> "RelayRequest":  # type: ignore[override]
        """The next registration or tunnel asking to be served. An anonymous
        registration, a tunnel to a key nobody registered and an unknown
        preamble are refused before they get here."""
        out, attempt = self._accept_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._request(out[0])


class RelayRequest(_sync.RelayRequest):
    async def admit(self) -> None:  # type: ignore[override]
        """Serves it. A registration replaces one of the same key on another
        connection and closes that connection. Consumes the request."""
        await _op_async(
            self._start_admit(),
            lib.weida_relay_request_admit_op_wait,
            lib.weida_relay_request_admit_op_fd,
            lib.weida_relay_request_admit_op_drop,
        )

    async def admit_with(self, bytes_per_sec: int) -> None:  # type: ignore[override]
        """Serves it as `admit` does and holds a tunnel to `bytes_per_sec`,
        both directions together, with one second of burst (0 counts as 1);
        a registration's budget is unused. Consumes the request; a budget
        that is not an `int` is a `TypeError`, a negative one or one above
        2**64 - 1 a `ValueError`, and neither consumes it."""
        await _op_async(
            self._start_admit_with(bytes_per_sec),
            lib.weida_relay_request_admit_op_wait,
            lib.weida_relay_request_admit_op_fd,
            lib.weida_relay_request_admit_op_drop,
        )

    async def refuse(self) -> None:  # type: ignore[override]
        """Refuses it: the asker fails with `Rejected`. Consumes the
        request."""
        await _op_async(
            self._start_refuse(),
            lib.weida_relay_request_refuse_op_wait,
            lib.weida_relay_request_refuse_op_fd,
            lib.weida_relay_request_refuse_op_drop,
        )



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


class Cursors(_sync.Cursors):
    __iter__ = None  # type: ignore[assignment]  # `async for` only

    async def recv(self) -> Optional[CursorSet]:  # type: ignore[override]
        """The newest set after a change, or `None` once no more cursors
        come."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._set(out[0])

    async def __aiter__(self) -> AsyncIterator[CursorSet]:
        """Every set until no more cursors come."""
        while (s := await self.recv()) is not None:
            yield s


class Reporter(_sync.Reporter):
    async def report(self, level, offset: int) -> None:  # type: ignore[override]
        """Reports that `level` has reached `offset`, as
        `weida.Reporter.report` does; a cancelled report is aborted."""
        op = self._start_report(level, offset)
        await _op_async(
            op,
            lib.weida_reporter_report_op_wait,
            lib.weida_reporter_report_op_fd,
            lib.weida_reporter_report_op_drop,
        )

    async def finish(self) -> None:  # type: ignore[override]
        """Flushes the newest offset per reported level and ends the
        report. Consumes the reporter."""
        op = self._start_finish()
        await _op_async(
            op,
            lib.weida_reporter_finish_op_wait,
            lib.weida_reporter_finish_op_fd,
            lib.weida_reporter_finish_op_drop,
        )


class PeerEvents(_sync.PeerEvents):
    __iter__ = None  # type: ignore[assignment]  # `async for` only

    async def recv(self) -> Optional[PeerEvent]:  # type: ignore[override]
        """The next event, or `None` once the requester, or the relayed
        binding's registration, is gone."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._event(out[0])

    async def __aiter__(self) -> AsyncIterator[PeerEvent]:
        """Every event until the requester, or the relayed binding's
        registration, is gone."""
        while (event := await self.recv()) is not None:
            yield event


class IdentityEvents(_sync.IdentityEvents):
    __iter__ = None  # type: ignore[assignment]  # `async for` only

    async def recv(self) -> Optional[IdentityEvent]:  # type: ignore[override]
        """The next event, or `None` once the source is gone."""
        out, attempt = self._recv_attempt()
        await call_async(self._waiters(), attempt, self.drop)
        return self._event(out[0])

    async def __aiter__(self) -> AsyncIterator[IdentityEvent]:
        """Every event until the source is gone."""
        while (event := await self.recv()) is not None:
            yield event


class IdentitySource(_values.IdentitySource):
    """`weida.IdentitySource` whose `events()` is a `weida.asyncio.IdentityEvents`."""


class WeakIdentitySource(_values.WeakIdentitySource):
    """`weida.WeakIdentitySource` whose `upgrade()` is a `weida.asyncio.IdentitySource`."""


class TrustSource(_values.TrustSource):
    """`weida.TrustSource` whose `events()` is a `weida.asyncio.IdentityEvents`."""


class WeakTrustSource(_values.WeakTrustSource):
    """`weida.WeakTrustSource` whose `upgrade()` is a `weida.asyncio.TrustSource`."""


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
Listener._Binding = Binding
Listener._Relay = Relay
Relay._RelayRequest = RelayRequest
Binding._PeerEvents = PeerEvents
IdentitySource._IdentityEvents = IdentityEvents
TrustSource._IdentityEvents = IdentityEvents
Runtime._Pusher = Pusher
Runtime._Subscriber = Subscriber
Listener._Puller = Puller
Listener._Publisher = Publisher
Pusher._OutgoingTransfer = OutgoingTransfer
Pusher._PeerEvents = PeerEvents
Puller._IncomingTransfer = IncomingTransfer
Publisher._FanOut = FanOut
Subscriber._IncomingTransfer = IncomingTransfer
Subscriber._PeerEvents = PeerEvents
Runtime._Paired = Paired
Runtime._Surveyor = Surveyor
Listener._Paired = Paired
Listener._Respondent = Respondent
Listener._BusMember = BusMember
Paired._OutgoingTransfer = OutgoingTransfer
Paired._IncomingTransfer = IncomingTransfer
Paired._PeerEvents = PeerEvents
Surveyor._SurveyRun = SurveyRun
Respondent._IncomingRequest = IncomingRequest
BusMember._IncomingTransfer = IncomingTransfer
Runtime._Dish = Dish
Dish._IncomingTransfer = IncomingTransfer
Dish._PeerEvents = PeerEvents
OutgoingTransfer._Cursors = Cursors
IncomingTransfer._Reporter = Reporter
IdentitySource._Weak = WeakIdentitySource
WeakIdentitySource._Strong = IdentitySource
TrustSource._Weak = WeakTrustSource
WeakTrustSource._Strong = TrustSource
