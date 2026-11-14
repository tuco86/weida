"""The synchronous surface: every class mirrors one C handle, every method
one C function with the `weida_<type>_` prefix dropped. A waiting method
takes `timeout` in seconds (`None` waits forever) and raises `Again` when
it passes."""

import time
from dataclasses import dataclass
from typing import Callable, Iterator, Optional, Tuple, Union

from weida import _resolver
from weida._crypto import path as _fspath
from weida._enums import Acknowledgement, ErrorCode, ReceivedKind, ReportMode
from weida._errors import check
from weida._native import ffi, lib
from weida._stats import ConnectionStats, PeerEvent, _take_connection_stats
from weida._values import (
    ClientTls,
    CursorSet,
    DishDrops,
    Drained,
    IdentityEvent,
    IdentitySource,
    IncomingMeta,
    Message,
    RuntimeConfig,
    ServerTls,
    TopicDrops,
    TransferMeta,
    TrustSource,
    _Handle,
    _JoinTerms,
    _SegmentTerms,
    _count,
    _cursor_level,
    _enum,
    _levels,
    _millis,
    _take_cursor_set,
    _take_dish_drops,
    _take_topic_drops,
    _text,
    _with_terms,
    _with_transfer_meta,
)
from weida._wait import Waiters, call_async, call_sync

_U64_MAX = 2**64 - 1
_I64_MAX = 2**63 - 1


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


def _deadline_ms(deadline) -> int:
    """`deadline` seconds as C's `uint64_t` milliseconds, converted before
    anything starts so a bad one raises first."""
    ms = _millis("deadline", deadline)
    if ms > _U64_MAX:
        raise ValueError("deadline must be below 2**64 milliseconds")
    return ms


class Runtime(_Handle):
    """A weida runtime on a reactor of its own."""

    # Whether a coroutine function resolver runs on the running loop
    # (weida.asyncio.Runtime); here it is a TypeError.
    _LOOP_RESOLVER = False

    @classmethod
    def owned(cls, config: Optional[RuntimeConfig] = None) -> "Runtime":
        """`weida_runtime_owned`: weida's defaults when `config` is `None`.
        A `resolver` in `config` answers each lookup of this runtime's
        dials. It is called with a frozen `Lookup` and returns `"ip:port"`
        strings in dial order. A plain callable runs on a daemon thread of
        its own, one lookup at a time. A coroutine function needs a
        `weida.asyncio.Runtime` made on the running loop, and runs there. A
        raise, or an answer that is not a list of `str`, refuses the lookup,
        and the dial raises `InvalidAddress` carrying why. The resolver is
        served until the runtime and everything made from it are gone, and
        never keeps the process alive."""
        resolve = None if config is None else config._fields.get("resolver")
        loop = None if resolve is None else _resolver.loop_for(resolve, cls._LOOP_RESOLVER)
        resolver = None if resolve is None else _resolver.new()
        try:
            out = ffi.new("weida_runtime **")
            if config is None:
                check(lib.weida_runtime_owned(ffi.NULL, out))
            else:
                cfg = config._handle()
                try:
                    if resolver is not None:
                        check(lib.weida_runtime_config_set_resolver(cfg, resolver))
                    check(lib.weida_runtime_owned(cfg, out))
                finally:
                    lib.weida_runtime_config_drop(cfg)
            runtime = cls.__new__(cls)
            _Handle.__init__(runtime, out[0], lib.weida_runtime_drop)
            if resolver is not None:
                _resolver.serve(resolver, resolve, loop)
        except BaseException:
            if resolver is not None:
                lib.weida_resolver_drop(resolver)
            raise
        return runtime

    def listener(self) -> "Listener":
        out = ffi.new("weida_listener **")
        check(lib.weida_runtime_listener(self._p(), out))
        return self._Listener(out[0], lib.weida_listener_drop, self)

    def requester(self, tls: ClientTls) -> "Requester":
        out = ffi.new("weida_requester **")
        check(lib.weida_runtime_requester(self._p(), tls._p(), out))
        return self._Requester(out[0], lib.weida_requester_drop, self)

    def pusher(self, tls: ClientTls) -> "Pusher":
        out = ffi.new("weida_pusher **")
        check(lib.weida_runtime_pusher(self._p(), tls._p(), out))
        return self._Pusher(out[0], lib.weida_pusher_drop, self)

    def subscriber(self, tls: ClientTls) -> "Subscriber":
        out = ffi.new("weida_subscriber **")
        check(lib.weida_runtime_subscriber(self._p(), tls._p(), out))
        return self._Subscriber(out[0], lib.weida_subscriber_drop, self)

    def pair(self, tls: ClientTls) -> "Paired":
        """A dialling pair: it dials its one peer on `tls`'s terms."""
        out = ffi.new("weida_paired **")
        check(lib.weida_runtime_pair(self._p(), tls._p(), out))
        return self._Paired(out[0], lib.weida_paired_drop, self)

    def surveyor(self, tls: ClientTls) -> "Surveyor":
        """A surveyor: it dials respondents on `tls`'s terms."""
        out = ffi.new("weida_surveyor **")
        check(lib.weida_runtime_surveyor(self._p(), tls._p(), out))
        return self._Surveyor(out[0], lib.weida_surveyor_drop, self)

    def dish(self, tls: ClientTls) -> "Dish":
        """A dish: it dials radios on `tls`'s terms."""
        out = ffi.new("weida_dish **")
        check(lib.weida_runtime_dish(self._p(), tls._p(), out))
        return self._Dish(out[0], lib.weida_dish_drop, self)

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

    def _start_drain(self, deadline: float):
        # Converted before the runtime is consumed: a deadline out of range
        # raises here and leaves the runtime usable.
        ms = _deadline_ms(deadline)
        return _start(lib.weida_runtime_drain, "weida_runtime_drain_op", self._consume(), ms)

    def drain(self, deadline: float, timeout: Optional[float] = None) -> Drained:
        """Stops admitting work, gives every finished transfer whose receipt
        nobody holds until `deadline` seconds to reach the peer's transport,
        then closes as `shutdown` does within what is left of it. A non-zero
        `Drained.outstanding` is not an error. A wait cut short by `timeout`
        raises `Again` and aborts the drain where it is: admission stays
        stopped and no close runs. Consumes the runtime."""
        op = self._start_drain(deadline)
        ptr = _op_sync(
            op,
            lib.weida_runtime_drain_op_wait,
            lib.weida_runtime_drain_op_drop,
            timeout,
            "weida_drained **",
        )
        return Drained._take(ptr)


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
        return self._Binding(ptr, lib.weida_binding_drop, self)

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

    def bind_unix(self, path) -> "UnixBinding":
        """Serves the listener's endpoints on the `AF_UNIX` socket `path`
        (`str`, `bytes` or `os.PathLike`; UTF-8), reached at
        `weida+unix://<percent-encoded path>/<endpoint>`. No TLS: the kernel
        proves the peer (`IncomingMeta.peer_local`). `path` must be in a
        directory this process owns and no other user may write; a stale
        socket file is replaced. Raises `Io` for a live listener on the path,
        `InvalidAddress` for a path over the `sun_path` budget or something
        at it that is not a socket, `InvalidArgument` for a path that is not
        UTF-8, and `Unsupported` on Windows."""
        out = ffi.new("weida_unix_binding **")
        check(lib.weida_listener_bind_unix(self._p(), _fspath(path), out))
        return UnixBinding(out[0], lib.weida_unix_binding_drop, self)

    def bind_pipe(self, name: str) -> "PipeBinding":
        r"""Binds the named pipe `\\.\pipe\<name>` owner-only and local-only,
        only if the name does not exist yet, and serves the listener's
        endpoints on it, reached at `weida+pipe://<name>/<endpoint>`. Raises
        `InvalidAddress` for an invalid name or one that exists already, `Io`,
        and `Unsupported` off Windows."""
        out = ffi.new("weida_pipe_binding **")
        check(lib.weida_listener_bind_pipe(self._p(), name.encode(), out))
        return PipeBinding(out[0], lib.weida_pipe_binding_drop, self)

    def replier(self, path: str) -> "Replier":
        """Registers a replier at the endpoint path `path`."""
        out = ffi.new("weida_replier **")
        check(lib.weida_listener_replier(self._p(), path.encode(), out))
        return self._Replier(out[0], lib.weida_replier_drop, self)

    def puller(self, path: str) -> "Puller":
        """Registers a puller at the endpoint path `path`."""
        out = ffi.new("weida_puller **")
        check(lib.weida_listener_puller(self._p(), path.encode(), out))
        return self._Puller(out[0], lib.weida_puller_drop, self)

    def publisher(self, path: str) -> "Publisher":
        """Registers a publisher at the endpoint path `path`; a dropped
        publisher leaves its path registered."""
        out = ffi.new("weida_publisher **")
        check(lib.weida_listener_publisher(self._p(), path.encode(), out))
        return self._Publisher(out[0], lib.weida_publisher_drop, self)

    def pair(self, path: str) -> "Paired":
        """Registers a bound pair at the endpoint path `path`: it is
        dialled, and its first send or open waits until its peer has sent. A
        dropped pair leaves its path registered."""
        out = ffi.new("weida_paired **")
        check(lib.weida_listener_pair(self._p(), path.encode(), out))
        return self._Paired(out[0], lib.weida_paired_drop, self)

    def respondent(self, path: str) -> "Respondent":
        """Registers a respondent at the endpoint path `path`; a dropped
        respondent leaves its path registered."""
        out = ffi.new("weida_respondent **")
        check(lib.weida_listener_respondent(self._p(), path.encode(), out))
        return self._Respondent(out[0], lib.weida_respondent_drop, self)

    def bus(self, path: str, tls: ClientTls) -> "BusMember":
        """Registers a bus member at the endpoint path `path` that dials the
        other members on `tls`'s terms; a dropped member leaves its path
        registered."""
        out = ffi.new("weida_bus_member **")
        check(lib.weida_listener_bus(self._p(), path.encode(), tls._p(), out))
        return self._BusMember(out[0], lib.weida_bus_member_drop, self)

    def radio(self, path: str) -> "Radio":
        """Registers a radio at the endpoint path `path`; dropping the radio
        releases the path."""
        out = ffi.new("weida_radio **")
        check(lib.weida_listener_radio(self._p(), path.encode(), out))
        return Radio(out[0], lib.weida_radio_drop, self)

    def _start_bind_quic_socket(self, socket: int, tls: ServerTls):
        # Checked before C takes the descriptor: a range error leaves it the
        # caller's, as a dropped listener or TLS term (Closed) does.
        if isinstance(socket, bool) or not isinstance(socket, int):
            raise TypeError("socket is a descriptor number (an int)")
        if socket < 0 or socket > _I64_MAX:
            raise ValueError("socket must be a descriptor number from 0 to 2**63 - 1")
        return _start(
            lib.weida_listener_bind_quic_socket,
            "weida_listener_bind_quic_socket_op",
            self._p(),
            socket,
            tls._p(),
        )

    def bind_quic_socket(self, socket: int, tls: ServerTls, timeout: Optional[float] = None) -> "Binding":
        """Serves QUIC on a UDP socket that is already bound: its descriptor
        (on Windows its SOCKET), e.g. `socket.socket.detach()` or a
        predecessor's `Handoff.take_socket()`. The descriptor is libweida's
        once the call reaches C; weida neither binds it nor sets options on
        it. A socket that is not UDP raises `Io`."""
        op = self._start_bind_quic_socket(socket, tls)
        ptr = _op_sync(
            op,
            lib.weida_listener_bind_quic_socket_op_wait,
            lib.weida_listener_bind_quic_socket_op_drop,
            timeout,
            "weida_binding **",
        )
        return self._binding(ptr)

    def _start_bind_relay(self, relay: str, via: ClientTls, tls: ServerTls):
        return _start(
            lib.weida_listener_bind_relay,
            "weida_listener_bind_relay_op",
            self._p(),
            relay.encode(),
            via._p(),
            tls._p(),
        )

    def bind_relay(
        self, relay: str, via: ClientTls, tls: ServerTls, timeout: Optional[float] = None
    ) -> "Binding":
        """Serves this listener's endpoints through the relay at `relay`
        (`weida://[relay-key@]host:port/.weida/relay`), registered under the
        key `via` presents; diallers reach them as
        `weida+relay://<key>@[relay-key@]host:port/path`. `tls` is the
        server identity and client trust, as for `bind_quic`. Raises
        `Rejected` where the relay refused the registration. A lost relay is
        redialled under the runtime's reconnect policy, reported on
        `Binding.relay_events()`."""
        op = self._start_bind_relay(relay, via, tls)
        ptr = _op_sync(
            op,
            lib.weida_listener_bind_relay_op_wait,
            lib.weida_listener_bind_relay_op_drop,
            timeout,
            "weida_binding **",
        )
        return self._binding(ptr)

    def relay(self) -> "Relay":
        """Serves a relay at `/.weida/relay` on this listener's bindings:
        registrations and tunnels arrive at `Relay.accept()`. Raises
        `AlreadyRegistered` on a second call."""
        out = ffi.new("weida_relay **")
        check(lib.weida_listener_relay(self._p(), out))
        return self._Relay(out[0], lib.weida_relay_drop, self)


class Binding(_Handle):
    """A QUIC binding, served directly or through a relay. Dropping it
    releases the handle only: the binding serves until `close`, a hand-over
    or the runtime's shutdown."""

    def local_addr(self) -> str:
        return _text(lib.weida_binding_local_addr, self._p())

    def _start_close(self):
        return _start(lib.weida_binding_close, "weida_binding_close_op", self._p())

    def close(self, timeout: Optional[float] = None) -> None:
        """Stops accepting and closes the binding's connections with
        SHUTDOWN; returns once the port is free to bind again, within the
        runtime's `shutdown_timeout`. A relayed binding ends its registration.
        A wait cut short by `timeout` raises `Again` and aborts the close
        where it is: the port is then freed only by the runtime's shutdown."""
        op = self._start_close()
        _op_sync(op, lib.weida_binding_close_op_wait, lib.weida_binding_close_op_drop, timeout)

    def _start_hand_over(self):
        return _start(lib.weida_binding_hand_over, "weida_binding_hand_over_op", self._consume())

    def _handoff(self, ptr) -> "Handoff":
        return Handoff(ptr, lib.weida_handoff_drop, self._parent)

    def hand_over(self, timeout: Optional[float] = None) -> "Handoff":
        """Hands the socket over to a successor: every connection is closed
        with RESTARTING, its close packet held in the `Handoff` and not
        sent. Raises `Unsupported` for a relayed binding and `Closed` once
        a close was started. Consumes the binding."""
        op = self._start_hand_over()
        ptr = _op_sync(
            op,
            lib.weida_binding_hand_over_op_wait,
            lib.weida_binding_hand_over_op_drop,
            timeout,
            "weida_handoff **",
        )
        return self._handoff(ptr)

    def _start_take_over(self, held):
        data = ffi.from_buffer("uint8_t[]", held)
        return _start(
            lib.weida_binding_take_over, "weida_binding_take_over_op", self._p(), data, len(data)
        )

    def take_over(self, held, timeout: Optional[float] = None) -> int:
        """Sends the close packets a predecessor held (`Handoff.bytes()`, any
        bytes-like), so its clients redial into this binding; call it once
        their endpoints are registered. Returns how many were sent. Bytes
        that are not a handoff, or are another address's, raise `Io`. The
        bytes need the integrity of the channel that carries the socket."""
        op = self._start_take_over(held)
        return _op_sync(
            op,
            lib.weida_binding_take_over_op_wait,
            lib.weida_binding_take_over_op_drop,
            timeout,
            "size_t *",
        )

    def disconnect(self, fingerprint: str) -> int:
        """Closes every connection from the peer with this key (`sha256:`
        text) with REJECTED and returns how many it closed. Not a ban: the
        peer redials under its own policy. Malformed text raises
        `InvalidFingerprint`."""
        out = ffi.new("size_t *")
        check(lib.weida_binding_disconnect(self._p(), fingerprint.encode(), out))
        return out[0]

    def relay_events(self) -> Optional["PeerEvents"]:
        """A new reader of a relayed binding's registration events, or
        `None` for a binding that is not relayed or whose registration
        ended."""
        out = ffi.new("weida_peer_events **")
        if check(lib.weida_binding_relay_events(self._p(), out)) == 0:
            return None
        # No parent, as Requester.events: the queue ends with the
        # registration, not with this handle.
        return self._PeerEvents(out[0], lib.weida_peer_events_drop)


class LocalBinding(_Handle):
    """An in-process binding; dropping it frees the bus name."""

    def bus(self) -> str:
        return _text(lib.weida_local_binding_bus, self._p())


class UnixBinding(_Handle):
    """An `AF_UNIX` binding. Dropping it ends the binding and removes the
    socket file."""

    def path(self) -> str:
        return _text(lib.weida_unix_binding_path, self._p())


class PipeBinding(_Handle):
    """A named-pipe binding. Dropping it ends the binding and releases
    the name."""

    def name(self) -> str:
        return _text(lib.weida_pipe_binding_name, self._p())


class Handoff(_Handle):
    """What `Binding.hand_over` leaves for a successor: the socket and the
    close packets held for its clients. Dropping it closes a socket not
    taken."""

    def held(self) -> int:
        """How many close packets are held."""
        return lib.weida_handoff_held(self._p())

    def local_addr(self) -> str:
        """The address the socket is bound to."""
        return _text(lib.weida_handoff_local_addr, self._p())

    def take_socket(self) -> int:
        """The socket's descriptor (on Windows its SOCKET), the caller's from
        then on, e.g. for `socket.socket(fileno=...)` or a successor's
        `Listener.bind_quic_socket`. Once; a second call raises `Closed`."""
        out = ffi.new("int64_t *")
        check(lib.weida_handoff_take_socket(self._p(), out))
        return out[0]

    # Last: a `bytes` annotation below this line would name the method.
    def bytes(self) -> bytes:
        """A copy of the held packets, for the successor's
        `Binding.take_over`."""
        length = ffi.new("size_t *")
        ptr = lib.weida_handoff_bytes(self._p(), length)
        if length[0] == 0:
            return b""
        return ffi.buffer(ptr, length[0])[:]


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


class Pusher(_Handle):
    """The sending half of Push/Pull: dials pullers and sends each message
    to the next one, round-robin."""

    def _start_connect(self, url: str):
        return _start(lib.weida_pusher_connect, "weida_pusher_connect_op", self._p(), url.encode())

    def _start_open(self, meta: Optional[TransferMeta]):
        return _with_transfer_meta(
            meta,
            lambda h: _start(lib.weida_pusher_open, "weida_pusher_open_op", self._p(), h),
        )

    def _start_send(self, body):
        data = ffi.from_buffer("uint8_t[]", body)
        return _start(lib.weida_pusher_send, "weida_pusher_send_op", self._p(), data, len(data))

    def _start_send_with(self, meta: Optional[TransferMeta], body):
        data = ffi.from_buffer("uint8_t[]", body)
        return _with_transfer_meta(
            meta,
            lambda h: _start(
                lib.weida_pusher_send_with, "weida_pusher_send_op", self._p(), h, data, len(data)
            ),
        )

    def _outgoing(self, ptr) -> "OutgoingTransfer":
        return self._OutgoingTransfer(ptr, lib.weida_outgoing_transfer_drop, self)

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Dials `url`; the runtime redials it when the connection is lost."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_pusher_connect_op_wait, lib.weida_pusher_connect_op_drop, timeout)

    def disconnect(self, url: str) -> bool:
        """Forgets a dialled address, closing its connection: whether it was
        dialled."""
        return check(lib.weida_pusher_disconnect(self._p(), url.encode())) == 1

    def peer_count(self) -> int:
        return lib.weida_pusher_peer_count(self._p())

    def dropped(self) -> int:
        """The bodies the outbox discarded."""
        return lib.weida_pusher_dropped(self._p())

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_pusher_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def events(self) -> "PeerEvents":
        """A new reader of this pusher's peer events, from now on; each call
        is an independent reader."""
        out = ffi.new("weida_peer_events **")
        check(lib.weida_pusher_events(self._p(), out))
        # No parent: the events end once the pusher is gone, which holding
        # it would prevent.
        return self._PeerEvents(out[0], lib.weida_peer_events_drop)

    def open(self, meta: Optional[TransferMeta] = None, timeout: Optional[float] = None) -> "OutgoingTransfer":
        """Opens a one-way transfer to the next peer carrying `meta` (copied
        when it starts), written like a socket; its `finish()` hands back the
        delivery receipt `send` discards. With every peer down it waits for
        the redial."""
        op = self._start_open(meta)
        ptr = _op_sync(
            op,
            lib.weida_pusher_open_op_wait,
            lib.weida_pusher_open_op_drop,
            timeout,
            "weida_outgoing_transfer **",
        )
        return self._outgoing(ptr)

    def send(self, body, timeout: Optional[float] = None) -> None:
        """Sends `body` (any bytes-like) as one message; returns once it is
        written or held in the outbox (a full outbox applies
        `RuntimeConfig.outbox_full`)."""
        op = self._start_send(body)
        _op_sync(op, lib.weida_pusher_send_op_wait, lib.weida_pusher_send_op_drop, timeout)

    def send_with(self, meta: Optional[TransferMeta], body, timeout: Optional[float] = None) -> None:
        """`send` with `meta`, copied when the send starts."""
        op = self._start_send_with(meta, body)
        _op_sync(op, lib.weida_pusher_send_op_wait, lib.weida_pusher_send_op_drop, timeout)


class Puller(_Handle):
    """The receiving half of Push/Pull, serving one endpoint path."""

    _fd_of = lib.weida_puller_fd

    def path(self) -> str:
        return _text(lib.weida_puller_path, self._p())

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_incoming_transfer **")
        return out, lambda ms: lib.weida_puller_recv(handle, ms, out)

    def _incoming(self, ptr) -> "IncomingTransfer":
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    def recv(self, timeout: Optional[float] = None) -> "IncomingTransfer":
        """The next pushed transfer, as soon as its header arrives. A `recv`
        cut short loses nothing: the take in flight stays in the puller.
        Raises `NotConnected` once the runtime closed the endpoint."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._incoming(out[0])


class Publisher(_Handle):
    """The publishing half of Pub/Sub, serving one endpoint path: each
    message goes to every subscriber whose filter matches its topic."""

    def path(self) -> str:
        return _text(lib.weida_publisher_path, self._p())

    def publish(self, topic: str, payload) -> int:
        """Fans `payload` (any bytes-like) out to every matching subscriber
        without waiting on any; returns how many it was enqueued to. One
        whose byte budget is exhausted loses this copy, counted by
        `dropped()`. Raises `LimitExceeded` for a payload above
        `Limits.subscriber_buffer_bytes` or a topic above 256 bytes."""
        data = ffi.from_buffer("uint8_t[]", payload)
        out = ffi.new("size_t *")
        check(lib.weida_publisher_publish(self._p(), topic.encode(), data, len(data), out))
        return out[0]

    def publish_with_trace(self, topic: str, payload, trace: str) -> int:
        """`publish` carrying the W3C traceparent `trace`; a malformed one
        raises `InvalidArgument`."""
        data = ffi.from_buffer("uint8_t[]", payload)
        out = ffi.new("size_t *")
        check(
            lib.weida_publisher_publish_with_trace(
                self._p(), topic.encode(), data, len(data), trace.encode(), out
            )
        )
        return out[0]

    def _fan_out(self, ptr) -> "FanOut":
        return self._FanOut(ptr, lib.weida_fan_out_drop, self)

    def open(self, topic: str) -> "FanOut":
        """A streamed publish on `topic` to the subscribers matched now."""
        out = ffi.new("weida_fan_out **")
        check(lib.weida_publisher_open(self._p(), topic.encode(), out))
        return self._fan_out(out[0])

    def open_with_trace(self, topic: str, trace: str) -> "FanOut":
        """`open` carrying the W3C traceparent `trace`; a malformed one
        raises `InvalidArgument`."""
        out = ffi.new("weida_fan_out **")
        check(lib.weida_publisher_open_with_trace(self._p(), topic.encode(), trace.encode(), out))
        return self._fan_out(out[0])

    def subscriber_count(self) -> int:
        return lib.weida_publisher_subscriber_count(self._p())

    def filter_count(self) -> int:
        return lib.weida_publisher_filter_count(self._p())

    def dropped(self) -> int:
        """The copies lost over every topic and cause."""
        return lib.weida_publisher_dropped(self._p())

    def dropped_on(self, topic: str) -> Optional[TopicDrops]:
        """What was dropped on `topic`, a frozen record, or `None` when
        nothing was."""
        out = ffi.new("weida_topic_drops **")
        check(lib.weida_publisher_dropped_on(self._p(), topic.encode(), out))
        records = _take_topic_drops(out[0])
        return records[0] if records else None

    def drops(self) -> Tuple[TopicDrops, ...]:
        """One frozen record per topic that lost a copy, at most
        `Limits.max_sequence_scopes` topics."""
        out = ffi.new("weida_topic_drops **")
        check(lib.weida_publisher_drops(self._p(), out))
        return _take_topic_drops(out[0])


class FanOut(_Handle):
    """A publish in progress: one payload written chunk by chunk, one stream
    per subscriber matched at the open. Dropping it without `finish()`
    aborts every copy, so no subscriber mistakes a partial payload for a
    whole one."""

    def topic(self) -> str:
        return _text(lib.weida_fan_out_topic, self._p())

    def subscribers(self) -> int:
        """The subscribers still receiving."""
        out = ffi.new("size_t *")
        check(lib.weida_fan_out_subscribers(self._p(), out))
        return out[0]

    def _start_write_within(self, chunk, limit: float):
        ms = _millis("limit", limit)
        if ms > _U64_MAX:
            raise ValueError("limit must be below 2**64 milliseconds")
        data = ffi.from_buffer("uint8_t[]", chunk)
        return _start(
            lib.weida_fan_out_write_within,
            "weida_fan_out_write_within_op",
            self._p(),
            data,
            len(data),
            ms,
        )

    def write_within(self, chunk, limit: float, timeout: Optional[float] = None) -> int:
        """Writes `chunk` to every subscriber still receiving, waiting up to
        `limit` seconds for all of those with no room together; one that
        frees none in time loses the transfer. Returns the subscribers left.
        While it runs, every other call on the fan-out raises
        `InvalidArgument`. A wait cut short by `timeout` abandons the
        transfer: later writes raise `Canceled` and `finish()` returns 0,
        aborting every copy."""
        op = self._start_write_within(chunk, limit)
        return _op_sync(
            op,
            lib.weida_fan_out_write_within_op_wait,
            lib.weida_fan_out_write_within_op_drop,
            timeout,
            "size_t *",
        )

    def write_now(self, chunk) -> int:
        """Writes `chunk` without waiting: a subscriber with no room right
        now loses the transfer. Returns the subscribers left."""
        data = ffi.from_buffer("uint8_t[]", chunk)
        out = ffi.new("size_t *")
        check(lib.weida_fan_out_write_now(self._p(), data, len(data), out))
        return out[0]

    def finish(self) -> int:
        """Ends the payload: the subscribers that received all of it.
        Consumes the fan-out."""
        out = ffi.new("size_t *")
        check(lib.weida_fan_out_finish(self._consume(), out))
        return out[0]


class Subscriber(_Handle):
    """The subscribing half of Pub/Sub: dials publishers and receives the
    copies whose topic its filters match. Two subscribers of one path on one
    runtime collide (`AlreadyRegistered`): use one runtime per subscriber of
    a path."""

    _fd_of = lib.weida_subscriber_fd

    def _start_connect(self, url: str):
        return _start(lib.weida_subscriber_connect, "weida_subscriber_connect_op", self._p(), url.encode())

    def _start_subscribe(self, filter: str):
        return _start(
            lib.weida_subscriber_subscribe, "weida_subscriber_subscribe_op", self._p(), filter.encode()
        )

    def _start_unsubscribe(self, filter: str):
        return _start(
            lib.weida_subscriber_unsubscribe, "weida_subscriber_unsubscribe_op", self._p(), filter.encode()
        )

    def _start_grant(self, filter: str, limit: int):
        n = _count("limit", limit)
        if n > _U64_MAX:
            raise ValueError("limit must be below 2**64")
        return _start(lib.weida_subscriber_grant, "weida_subscriber_grant_op", self._p(), filter.encode(), n)

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Dials `url` and subscribes there to every filter registered so
        far; the runtime redials it, filters included."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_subscriber_connect_op_wait, lib.weida_subscriber_connect_op_drop, timeout)

    def subscribe(self, filter: str, timeout: Optional[float] = None) -> None:
        """Registers interest in every topic `filter` matches (PROTOCOL.md
        §6.4: segments split on `.`, `*` one whole segment, a trailing `#`
        any number; `""` and `#` match everything). Subscribing twice is
        idempotent. A filter outside the grammar raises `Protocol`."""
        op = self._start_subscribe(filter)
        _op_sync(op, lib.weida_subscriber_subscribe_op_wait, lib.weida_subscriber_subscribe_op_drop, timeout)

    def unsubscribe(self, filter: str, timeout: Optional[float] = None) -> None:
        """Withdraws `filter`; an unknown one is ignored."""
        op = self._start_unsubscribe(filter)
        _op_sync(
            op, lib.weida_subscriber_unsubscribe_op_wait, lib.weida_subscriber_unsubscribe_op_drop, timeout
        )

    def grant(self, filter: str, limit: int, timeout: Optional[float] = None) -> None:
        """States how many messages this subscriber accepts in total on
        `filter`, the credit a queue serving the path consults (absolute,
        cumulative, never lowered). A `limit` that is not an `int` is a
        `TypeError`, a negative one or one above 2**64 - 1 a `ValueError`."""
        op = self._start_grant(filter, limit)
        _op_sync(op, lib.weida_subscriber_grant_op_wait, lib.weida_subscriber_grant_op_drop, timeout)

    def disconnect(self, url: str) -> bool:
        """Forgets a dialled address, closing its connection: whether it was
        dialled."""
        return check(lib.weida_subscriber_disconnect(self._p(), url.encode())) == 1

    def peer_count(self) -> int:
        return lib.weida_subscriber_peer_count(self._p())

    def filter_count(self) -> int:
        return lib.weida_subscriber_filter_count(self._p())

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_subscriber_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def events(self) -> "PeerEvents":
        """A new reader of this subscriber's peer events, from now on; each
        call is an independent reader."""
        out = ffi.new("weida_peer_events **")
        check(lib.weida_subscriber_events(self._p(), out))
        # No parent: the events end once the subscriber is gone, which
        # holding it would prevent.
        return self._PeerEvents(out[0], lib.weida_peer_events_drop)

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_incoming_transfer **")
        return out, lambda ms: lib.weida_subscriber_recv(handle, ms, out)

    def _incoming(self, ptr) -> "IncomingTransfer":
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    def recv(self, timeout: Optional[float] = None) -> "IncomingTransfer":
        """The next published copy, as soon as its header arrives; its topic
        is `meta().topic`. Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._incoming(out[0])


class Paired(_Handle):
    """Either half of PAIR: one peer, one-way transfers both ways. A
    dialling pair (`Runtime.pair`) dials its peer; a bound one
    (`Listener.pair`) is dialled, and its first send or open waits until the
    peer has sent."""

    _fd_of = lib.weida_paired_fd

    def _start_connect(self, url: str):
        return _start(lib.weida_paired_connect, "weida_paired_connect_op", self._p(), url.encode())

    def _start_open(self, meta: Optional[TransferMeta]):
        return _with_transfer_meta(
            meta,
            lambda h: _start(lib.weida_paired_open, "weida_paired_open_op", self._p(), h),
        )

    def _start_send(self, body):
        data = ffi.from_buffer("uint8_t[]", body)
        return _start(lib.weida_paired_send, "weida_paired_send_op", self._p(), data, len(data))

    def _start_send_with(self, meta: Optional[TransferMeta], body):
        data = ffi.from_buffer("uint8_t[]", body)
        return _with_transfer_meta(
            meta,
            lambda h: _start(
                lib.weida_paired_send_with, "weida_paired_send_op", self._p(), h, data, len(data)
            ),
        )

    def _outgoing(self, ptr) -> "OutgoingTransfer":
        return self._OutgoingTransfer(ptr, lib.weida_outgoing_transfer_drop, self)

    def path(self) -> str:
        """The bound pair's path; `""` on a dialling pair."""
        return _text(lib.weida_paired_path, self._p())

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Dials `url`, once: a pair has one peer, and the runtime redials it
        when the connection is lost. A second address raises
        `LimitExceeded`, and a bound pair raises `Unsupported`."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_paired_connect_op_wait, lib.weida_paired_connect_op_drop, timeout)

    def peer_count(self) -> int:
        """The connected peers, 0 or 1; 0 on a bound pair."""
        return lib.weida_paired_peer_count(self._p())

    def dropped(self) -> int:
        """The bodies a dialling pair's outbox discarded."""
        return lib.weida_paired_dropped(self._p())

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none, and so has a bound pair. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_paired_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def events(self) -> Optional["PeerEvents"]:
        """A new reader of a dialling pair's peer events, from now on, or
        `None` on a bound pair, which dials nothing; each call is an
        independent reader."""
        out = ffi.new("weida_peer_events **")
        if check(lib.weida_paired_events(self._p(), out)) == 0:
            return None
        # No parent, as Pusher.events: the events end once the pair is gone,
        # which holding it would prevent.
        return self._PeerEvents(out[0], lib.weida_peer_events_drop)

    def open(self, meta: Optional[TransferMeta] = None, timeout: Optional[float] = None) -> "OutgoingTransfer":
        """Opens a one-way transfer to the peer carrying `meta` (copied when
        it starts), written like a socket; its `finish()` hands back the
        delivery receipt `send` discards. A wait cut short resets the
        transfer `CANCELED`."""
        op = self._start_open(meta)
        ptr = _op_sync(
            op,
            lib.weida_paired_open_op_wait,
            lib.weida_paired_open_op_drop,
            timeout,
            "weida_outgoing_transfer **",
        )
        return self._outgoing(ptr)

    def send(self, body, timeout: Optional[float] = None) -> None:
        """Sends `body` (any bytes-like) as one message; returns once it is
        written or held in the outbox."""
        op = self._start_send(body)
        _op_sync(op, lib.weida_paired_send_op_wait, lib.weida_paired_send_op_drop, timeout)

    def send_with(self, meta: Optional[TransferMeta], body, timeout: Optional[float] = None) -> None:
        """`send` with `meta`, copied when the send starts."""
        op = self._start_send_with(meta, body)
        _op_sync(op, lib.weida_paired_send_op_wait, lib.weida_paired_send_op_drop, timeout)

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_incoming_transfer **")
        return out, lambda ms: lib.weida_paired_recv(handle, ms, out)

    def _incoming(self, ptr) -> "IncomingTransfer":
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    def recv(self, timeout: Optional[float] = None) -> "IncomingTransfer":
        """The next transfer from the peer, as soon as its header arrives.
        Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._incoming(out[0])


class Surveyor(_Handle):
    """The asking half of SURVEY: dials respondents and asks every one of
    them."""

    def _start_connect(self, url: str):
        return _start(lib.weida_surveyor_connect, "weida_surveyor_connect_op", self._p(), url.encode())

    def _start_survey(self, body, deadline: float):
        ms = _deadline_ms(deadline)
        data = ffi.from_buffer("uint8_t[]", body)
        return _start(lib.weida_surveyor_survey, "weida_surveyor_survey_op", self._p(), data, len(data), ms)

    def _start_survey_with(self, meta: Optional[TransferMeta], body, deadline: float):
        ms = _deadline_ms(deadline)
        data = ffi.from_buffer("uint8_t[]", body)
        return _with_transfer_meta(
            meta,
            lambda h: _start(
                lib.weida_surveyor_survey_with, "weida_surveyor_survey_op", self._p(), h, data, len(data), ms
            ),
        )

    def _run(self, ptr) -> "SurveyRun":
        return self._SurveyRun(ptr, lib.weida_survey_run_drop, self)

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Dials the respondent at `url`, adding it to the set every survey
        asks; the runtime redials it when the connection is lost."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_surveyor_connect_op_wait, lib.weida_surveyor_connect_op_drop, timeout)

    def peer_count(self) -> int:
        """The connected respondents."""
        return lib.weida_surveyor_peer_count(self._p())

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_surveyor_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def survey(self, body, deadline: float, timeout: Optional[float] = None) -> "SurveyRun":
        """Asks every connected respondent `body` (any bytes-like) and
        collects their answers for `deadline` seconds, which bounds the
        asking too; returns the run once every respondent was asked. A
        survey with no respondents is an empty run. A `deadline` that is not
        a number is a `TypeError`, a negative or non-finite one a
        `ValueError`, and one the clock cannot add `InvalidArgument`. A wait
        cut short by `timeout` aborts the survey."""
        op = self._start_survey(body, deadline)
        ptr = _op_sync(
            op,
            lib.weida_surveyor_survey_op_wait,
            lib.weida_surveyor_survey_op_drop,
            timeout,
            "weida_survey_run **",
        )
        return self._run(ptr)

    def survey_with(
        self, meta: Optional[TransferMeta], body, deadline: float, timeout: Optional[float] = None
    ) -> "SurveyRun":
        """`survey` with `meta`, copied when the survey starts."""
        op = self._start_survey_with(meta, body, deadline)
        ptr = _op_sync(
            op,
            lib.weida_surveyor_survey_op_wait,
            lib.weida_surveyor_survey_op_drop,
            timeout,
            "weida_survey_run **",
        )
        return self._run(ptr)


class SurveyRun(_Handle):
    """One survey in progress: its answers, and what the deadline cost.
    Dropping it resets every exchange not yet answered."""

    _fd_of = lib.weida_survey_run_fd

    def _next_attempt(self, max_bytes: int):
        handle = self._p()
        out = ffi.new("weida_survey_answer **")
        return out, lambda ms: lib.weida_survey_run_next(handle, max_bytes, ms, out)

    @staticmethod
    def _answer(ptr) -> Optional[bytes]:
        if ptr == ffi.NULL:
            return None
        try:
            length = lib.weida_survey_answer_len(ptr)
            if length == 0:
                return b""
            return ffi.buffer(lib.weida_survey_answer_data(ptr), length)[:]
        finally:
            lib.weida_survey_answer_drop(ptr)

    def next(self, max_bytes: int, timeout: Optional[float] = None) -> Optional[bytes]:
        """The next answer's bytes, at most `max_bytes` of them (a longer one
        raises `TooLarge`), or `None` once the deadline passed or every
        respondent answered; every later call returns `None` too. An answer
        that failed raises its error and the run goes on. A `next` cut short
        by `timeout` leaves its read in flight for the next call, which keeps
        the first call's `max_bytes`."""
        out, attempt = self._next_attempt(max_bytes)
        call_sync(attempt, timeout)
        return self._answer(out[0])

    def late(self) -> int:
        """The answers that arrived after the deadline, dropped and
        counted."""
        return lib.weida_survey_run_late(self._p())

    def respondents(self) -> int:
        """How many respondents were asked."""
        return lib.weida_survey_run_respondents(self._p())


class Respondent(_Handle):
    """The answering half of SURVEY, serving one endpoint path."""

    _fd_of = lib.weida_respondent_fd

    def path(self) -> str:
        return _text(lib.weida_respondent_path, self._p())

    def _accept_attempt(self):
        handle = self._p()
        out = ffi.new("weida_incoming_request **")
        return out, lambda ms: lib.weida_respondent_accept(handle, ms, out)

    def _request(self, ptr) -> "IncomingRequest":
        return self._IncomingRequest(ptr, lib.weida_incoming_request_drop, self)

    def accept(self, timeout: Optional[float] = None) -> "IncomingRequest":
        """The next question, as soon as its header arrives: an
        `IncomingRequest`, answered as a replier's is. Raises `NotConnected`
        once the queue ended."""
        out, attempt = self._accept_attempt()
        call_sync(attempt, timeout)
        return self._request(out[0])


class BusMember(_Handle):
    """One member of a BUS: accepts on its path and dials the other members;
    a message goes to every member it dialled, never to itself, and nothing
    is relayed."""

    _fd_of = lib.weida_bus_member_fd

    def path(self) -> str:
        return _text(lib.weida_bus_member_path, self._p())

    def _start_connect(self, url: str):
        return _start(lib.weida_bus_member_connect, "weida_bus_member_connect_op", self._p(), url.encode())

    def _start_send(self, body):
        data = ffi.from_buffer("uint8_t[]", body)
        return _start(lib.weida_bus_member_send, "weida_bus_member_send_op", self._p(), data, len(data))

    def _start_send_with(self, meta: Optional[TransferMeta], body):
        data = ffi.from_buffer("uint8_t[]", body)
        return _with_transfer_meta(
            meta,
            lambda h: _start(
                lib.weida_bus_member_send_with, "weida_bus_member_send_op", self._p(), h, data, len(data)
            ),
        )

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Joins the member at `url`; the runtime redials it when the
        connection is lost."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_bus_member_connect_op_wait, lib.weida_bus_member_connect_op_drop, timeout)

    def peer_count(self) -> int:
        """The members this one joined."""
        return lib.weida_bus_member_peer_count(self._p())

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_bus_member_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def dropped(self) -> int:
        """The copies that never reached a member."""
        return lib.weida_bus_member_dropped(self._p())

    def send(self, body, timeout: Optional[float] = None) -> int:
        """Sends `body` (any bytes-like) to every member this one dialled;
        returns how many it reached. A member that cannot take a copy loses
        it, counted by `dropped()`. A body above
        `Limits.subscriber_buffer_bytes` raises `LimitExceeded`."""
        op = self._start_send(body)
        return _op_sync(
            op, lib.weida_bus_member_send_op_wait, lib.weida_bus_member_send_op_drop, timeout, "size_t *"
        )

    def send_with(self, meta: Optional[TransferMeta], body, timeout: Optional[float] = None) -> int:
        """`send` with `meta`, copied when the send starts."""
        op = self._start_send_with(meta, body)
        return _op_sync(
            op, lib.weida_bus_member_send_op_wait, lib.weida_bus_member_send_op_drop, timeout, "size_t *"
        )

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_incoming_transfer **")
        return out, lambda ms: lib.weida_bus_member_recv(handle, ms, out)

    def _incoming(self, ptr) -> "IncomingTransfer":
        return self._IncomingTransfer(ptr, lib.weida_incoming_transfer_drop, self)

    def recv(self, timeout: Optional[float] = None) -> "IncomingTransfer":
        """The next message from another member, as soon as its header
        arrives. Raises `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._incoming(out[0])


class Radio(_Handle):
    """The sending half of RADIO/DISH, serving one endpoint path: each
    segment goes to every dish joined to its topic when it opens, written in
    layers and never waiting. Dropping it releases the path."""

    def path(self) -> str:
        return _text(lib.weida_radio_path, self._p())

    def segment(
        self,
        topic: str,
        *,
        max_age: Optional[float] = None,
        priority: int = 0,
        follows_upstream: bool = False,
    ) -> "Segment":
        """Opens the topic's next segment to the dishes joined now; opening
        it resets every unfinished layer of the topic's previous segment,
        counted `superseded`. `max_age` (seconds, `None` for no budget)
        resets every copy still unfinished that long after the open;
        `priority` puts this topic's copies before other topics' of the same
        layer on one connection (outside -32768..32767 `InvalidArgument`);
        `follows_upstream` marks a segment relayed while its upstream still
        streams it. A topic above 256 bytes, or every one of
        `Limits.max_sequence_scopes` topics with a copy in flight, raises
        `LimitExceeded`."""
        out = ffi.new("weida_segment **")
        terms = _SegmentTerms(max_age=max_age, priority=priority, follows_upstream=follows_upstream)
        _with_terms(terms, lambda t: check(lib.weida_radio_segment(self._p(), topic.encode(), t, out)))
        return Segment(out[0], lib.weida_segment_drop, self)

    def datagram(self, topic: str, payload) -> int:
        """Sends `payload` (any bytes-like, copied) as one datagram segment
        to every dish joined to `topic`; returns how many it was handed to. A
        dish that carries no datagrams counts a `no_datagrams` drop."""
        data = ffi.from_buffer("uint8_t[]", payload)
        out = ffi.new("size_t *")
        check(lib.weida_radio_datagram(self._p(), topic.encode(), data, len(data), out))
        return out[0]

    def dish_count(self) -> int:
        """The dishes joined."""
        return lib.weida_radio_dish_count(self._p())

    def dropped(self) -> int:
        """The copies dropped, over every topic and cause."""
        return lib.weida_radio_dropped(self._p())

    def dropped_on(self, topic: str) -> Optional[TopicDrops]:
        """What was dropped on `topic`, a frozen record, or `None` when
        nothing was."""
        out = ffi.new("weida_topic_drops **")
        check(lib.weida_radio_dropped_on(self._p(), topic.encode(), out))
        records = _take_topic_drops(out[0])
        return records[0] if records else None

    def drops(self) -> Tuple[TopicDrops, ...]:
        """One frozen record per topic that lost a copy."""
        out = ffi.new("weida_topic_drops **")
        check(lib.weida_radio_drops(self._p(), out))
        return _take_topic_drops(out[0])

    def evict(self, fingerprint: str, filter: str) -> int:
        """Withdraws the joins on `filter` of the dishes whose key is
        `fingerprint` (`sha256:` text) and frees their subscription slots;
        returns how many went. Malformed text raises `InvalidFingerprint`."""
        out = ffi.new("size_t *")
        check(lib.weida_radio_evict(self._p(), fingerprint.encode(), filter.encode(), out))
        return out[0]

    def dish_drops(self) -> Tuple[DishDrops, ...]:
        """One frozen record per joined dish connection; a record goes when
        its connection closes."""
        out = ffi.new("weida_dish_drops **")
        check(lib.weida_radio_dish_drops(self._p(), out))
        return _take_dish_drops(out[0])


class Segment(_Handle):
    """One open segment of a radio: layers written without waiting. Dropping
    it without `finish()` resets every unfinished layer on every copy."""

    def number(self) -> int:
        """Its number on its topic; the first is 0."""
        return lib.weida_segment_number(self._p())

    def topic(self) -> str:
        return _text(lib.weida_segment_topic, self._p())

    def write(self, chunk) -> int:
        """`write_layer(0, chunk)`."""
        data = ffi.from_buffer("uint8_t[]", chunk)
        out = ffi.new("size_t *")
        check(lib.weida_segment_write(self._p(), data, len(data), out))
        return out[0]

    def write_layer(self, layer: int, chunk) -> int:
        """Writes `chunk` (any bytes-like, copied) to `layer` without
        waiting; returns how many copies took it. A copy without room loses
        this layer and every one above it (the whole segment for layer 0); a
        copy whose dish caps below `layer` is skipped, which is not a drop. A
        layer of `MAX_SEGMENT_LAYERS` or above raises `LimitExceeded`, a
        finished one `RuntimeFailure`."""
        n = _count("layer", layer)
        data = ffi.from_buffer("uint8_t[]", chunk)
        out = ffi.new("size_t *")
        check(lib.weida_segment_write_layer(self._p(), n, data, len(data), out))
        return out[0]

    def finish_layer(self, layer: int) -> None:
        """Finishes `layer`; a layer of `MAX_SEGMENT_LAYERS` or above is
        ignored."""
        check(lib.weida_segment_finish_layer(self._p(), _count("layer", layer)))

    def finish(self) -> int:
        """Finishes every open layer; returns how many copies still hold
        layer 0. Consumes the segment."""
        out = ffi.new("size_t *")
        check(lib.weida_segment_finish(self._consume(), out))
        return out[0]


@dataclass(frozen=True)
class Received:
    """One thing a dish received, copied out at once. A stream segment
    (`ReceivedKind.SEGMENT`) carries its `transfer`, whose `meta()` holds
    its topic, segment number and layer; a datagram segment
    (`ReceivedKind.DATAGRAM`) carries its `topic`, `segment` number and
    `payload`. A field the kind does not carry is `None`."""

    kind: ReceivedKind
    transfer: Optional["IncomingTransfer"]
    topic: Optional[str]
    segment: Optional[int]
    payload: Optional[bytes]


class Dish(_Handle):
    """The receiving half of RADIO/DISH: dials radios, joins topics and
    receives their segments. Two dishes on one runtime collide on the path:
    use one runtime per dish."""

    _fd_of = lib.weida_dish_fd

    def _start_connect(self, url: str):
        return _start(lib.weida_dish_connect, "weida_dish_connect_op", self._p(), url.encode())

    def _start_join(self, filter: str, max_age: Optional[float], max_layer: Optional[int]):
        terms = _JoinTerms(max_age=max_age, max_layer=max_layer)
        return _with_terms(
            terms,
            lambda t: _start(lib.weida_dish_join, "weida_dish_join_op", self._p(), filter.encode(), t),
        )

    def _start_leave(self, filter: str):
        return _start(lib.weida_dish_leave, "weida_dish_leave_op", self._p(), filter.encode())

    def connect(self, url: str, timeout: Optional[float] = None) -> None:
        """Dials the radio at `url` and joins there every filter joined so
        far; a redial joins again, and what was sent in between is gone."""
        op = self._start_connect(url)
        _op_sync(op, lib.weida_dish_connect_op_wait, lib.weida_dish_connect_op_drop, timeout)

    def peer_count(self) -> int:
        """The connected radios."""
        return lib.weida_dish_peer_count(self._p())

    def connection_stats(self) -> Tuple[ConnectionStats, ...]:
        """One frozen record per live connection, in dial order; an address
        being redialled has none. Never blocks."""
        out = ffi.new("weida_connection_stats **")
        check(lib.weida_dish_connection_stats(self._p(), out))
        return _take_connection_stats(out[0])

    def events(self) -> "PeerEvents":
        """A new reader of this dish's peer events, from now on; each call
        is an independent reader."""
        out = ffi.new("weida_peer_events **")
        check(lib.weida_dish_events(self._p(), out))
        # No parent, as Pusher.events: the events end once the dish is gone,
        # which holding it would prevent.
        return self._PeerEvents(out[0], lib.weida_peer_events_drop)

    def join(
        self,
        filter: str,
        timeout: Optional[float] = None,
        *,
        max_age: Optional[float] = None,
        max_layer: Optional[int] = None,
    ) -> None:
        """Joins every topic `filter` matches (the grammar of
        `Subscriber.subscribe`) at every radio, now and on each dial.
        `max_age` (seconds, `None` for no budget): a copy still unfinished
        that long after its segment opened is reset at the radio. `max_layer`
        (`None` for every layer): the highest layer this dish takes; 16 to
        255 raise `LimitExceeded`, above 255 `InvalidArgument`. A filter
        outside the grammar raises `Protocol`."""
        op = self._start_join(filter, max_age, max_layer)
        _op_sync(op, lib.weida_dish_join_op_wait, lib.weida_dish_join_op_drop, timeout)

    def leave(self, filter: str, timeout: Optional[float] = None) -> None:
        """Leaves `filter`; an unknown one is ignored."""
        op = self._start_leave(filter)
        _op_sync(op, lib.weida_dish_leave_op_wait, lib.weida_dish_leave_op_drop, timeout)

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_received **")
        return out, lambda ms: lib.weida_dish_recv(handle, ms, out)

    def _received(self, ptr) -> Received:
        """Copies a `weida_received` out, taking a stream segment's
        transfer, and drops it."""
        try:
            kind = ReceivedKind(lib.weida_received_kind(ptr))
            if kind == ReceivedKind.SEGMENT:
                out = ffi.new("weida_incoming_transfer **")
                check(lib.weida_received_take_segment(ptr, out))
                transfer = self._IncomingTransfer(out[0], lib.weida_incoming_transfer_drop, self)
                return Received(kind=kind, transfer=transfer, topic=None, segment=None, payload=None)
            number = ffi.new("uint64_t *")
            lib.weida_received_segment(ptr, number)  # 1 on a datagram
            length = ffi.new("size_t *")
            data = lib.weida_received_payload(ptr, length)
            payload = b"" if length[0] == 0 else ffi.buffer(data, length[0])[:]
            return Received(
                kind=kind,
                transfer=None,
                topic=_text(lib.weida_received_topic, ptr),
                segment=number[0],
                payload=payload,
            )
        finally:
            lib.weida_received_drop(ptr)

    def recv(self, timeout: Optional[float] = None) -> Received:
        """The next segment, a frozen `Received`; a stream segment's
        transfer arrives as soon as its header does. A `recv` cut short
        loses nothing: the take in flight stays in the dish. Raises
        `NotConnected` once the queue ended."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._received(out[0])

    def stale(self) -> int:
        """Segments discarded because a newer one on their topic was already
        delivered."""
        return lib.weida_dish_stale(self._p())

    def overflow(self) -> int:
        """Segments discarded because the receive queue was full."""
        return lib.weida_dish_overflow(self._p())


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


class Relay(_Handle):
    """The relay a listener serves at `/.weida/relay`. Dropping it
    unregisters the path and closes every registration's connection with
    SHUTDOWN, so the hidden peers redial."""

    _fd_of = lib.weida_relay_fd

    def _accept_attempt(self):
        handle = self._p()
        out = ffi.new("weida_relay_request **")
        return out, lambda ms: lib.weida_relay_accept(handle, ms, out)

    def _request(self, ptr) -> "RelayRequest":
        return self._RelayRequest(ptr, lib.weida_relay_request_drop, self)

    def accept(self, timeout: Optional[float] = None) -> "RelayRequest":
        """The next registration or tunnel asking to be served. An anonymous
        registration, a tunnel to a key nobody registered and an unknown
        preamble are refused before they get here."""
        out, attempt = self._accept_attempt()
        call_sync(attempt, timeout)
        return self._request(out[0])


class RelayRequest(_Handle):
    """A registration or a tunnel asking the relay to be served. Dropped
    unanswered, it answers NO_REPLY."""

    def peer_fingerprint(self) -> Optional[str]:
        """The key (`sha256:` text) the asker proved; `None` for an
        anonymous dialler, always a key for a registration."""
        return _text(lib.weida_relay_request_peer_fingerprint, self._p())

    def target(self) -> Optional[str]:
        """`None` for a registration; for a tunnel, the key of the peer it
        reaches."""
        return _text(lib.weida_relay_request_target, self._p())

    def _start_admit(self):
        return _start(lib.weida_relay_request_admit, "weida_relay_request_admit_op", self._consume())

    def _start_admit_with(self, bytes_per_sec: int):
        # Converted before the request is consumed: a budget out of range
        # raises here and leaves the request usable.
        budget = _count("bytes_per_sec", bytes_per_sec)
        if budget > _U64_MAX:
            raise ValueError("bytes_per_sec must be below 2**64")
        return _start(
            lib.weida_relay_request_admit_with,
            "weida_relay_request_admit_op",
            self._consume(),
            budget,
        )

    def _start_refuse(self):
        return _start(lib.weida_relay_request_refuse, "weida_relay_request_refuse_op", self._consume())

    def admit(self, timeout: Optional[float] = None) -> None:
        """Serves it. A registration replaces one of the same key on another
        connection and closes that connection. Consumes the request."""
        op = self._start_admit()
        _op_sync(op, lib.weida_relay_request_admit_op_wait, lib.weida_relay_request_admit_op_drop, timeout)

    def admit_with(self, bytes_per_sec: int, timeout: Optional[float] = None) -> None:
        """Serves it as `admit` does and holds a tunnel to `bytes_per_sec`,
        both directions together, with one second of burst (0 counts as 1);
        a registration's budget is unused. Consumes the request; a budget
        that is not an `int` is a `TypeError`, a negative one or one above
        2**64 - 1 a `ValueError`, and neither consumes it."""
        op = self._start_admit_with(bytes_per_sec)
        _op_sync(op, lib.weida_relay_request_admit_op_wait, lib.weida_relay_request_admit_op_drop, timeout)

    def refuse(self, timeout: Optional[float] = None) -> None:
        """Refuses it: the asker fails with `Rejected`. Consumes the
        request."""
        op = self._start_refuse()
        _op_sync(op, lib.weida_relay_request_refuse_op_wait, lib.weida_relay_request_refuse_op_drop, timeout)


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

    def reporter(self) -> Optional["Reporter"]:
        """A reporter for the cursor levels the sender ordered, or `None`
        when it ordered none. Each call hands out a new reporter on the same
        report, so take one, and take it before `collect`, which raises
        `Closed` for it afterwards; the reporter outlives the transfer."""
        out = ffi.new("weida_reporter **")
        if check(lib.weida_incoming_transfer_reporter(self._p(), out)) == 0:
            return None
        return self._Reporter(out[0], lib.weida_reporter_drop, self._parent)

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

    def cursors(self) -> Optional["Cursors"]:
        """The queue of cursor sets the receiver reports for this transfer:
        a `Cursors` the first time, `None` after that and when the metadata
        ordered no levels. Take it before `finish`, which raises `Closed` for
        it afterwards; the queue outlives the transfer."""
        out = ffi.new("weida_cursors **")
        if check(lib.weida_outgoing_transfer_cursors(self._p(), out)) == 0:
            return None
        # No parent: the queue ends once the connection is gone, which
        # holding the dialler would prevent.
        return self._Cursors(out[0], lib.weida_cursors_drop)

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


class Cursors(_Handle):
    """The cursor sets one transfer's receiver reports, a queue
    (docs/libraries/weida-py.md §1): `recv` returns the newest `CursorSet`
    after a change (a tuple of frozen `CursorEntry`, ascending by level;
    changes since the last `recv` coalesce, offsets are absolute), or `None`
    once no more cursors come (the reporter finished, the stream was reset
    or the connection is gone)."""

    _fd_of = lib.weida_cursors_fd

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_cursor_set **")
        return out, lambda ms: lib.weida_cursors_recv(handle, ms, out)

    @staticmethod
    def _set(ptr) -> Optional[CursorSet]:
        return None if ptr == ffi.NULL else _take_cursor_set(ptr)

    def recv(self, timeout: Optional[float] = None) -> Optional[CursorSet]:
        """The newest set after a change, or `None` once no more cursors
        come."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._set(out[0])

    def __iter__(self) -> Iterator[CursorSet]:
        """Every set until no more cursors come, each `recv` without a
        timeout."""
        while (s := self.recv()) is not None:
            yield s


class Reporter(_Handle):
    """The receiver's end of a report: how far each ordered level got.
    Dropping it without `finish` flushes nothing."""

    def levels(self) -> Tuple[Union[Acknowledgement, int], ...]:
        """The levels the sender ordered, ascending."""
        return _levels(lib.weida_reporter_levels, self._p())

    def mode(self) -> ReportMode:
        """The `ReportMode` the sender asked for."""
        return ReportMode(check(lib.weida_reporter_mode(self._p())))

    def set_granularity(self, bytes: int, interval: float) -> None:
        """Progress records are written when a level advanced `bytes` or
        `interval` seconds passed since its last record, whichever comes
        first; 0 and 0 write every report. No effect in final-only mode;
        `InvalidArgument` while a report runs."""
        check(
            lib.weida_reporter_set_granularity(
                self._p(), _count("bytes", bytes), _millis("interval", interval)
            )
        )

    def _start_report(self, level, offset):
        return _start(
            lib.weida_reporter_report,
            "weida_reporter_report_op",
            self._p(),
            _cursor_level("level", level),
            _count("offset", offset),
        )

    def report(self, level, offset: int, timeout: Optional[float] = None) -> None:
        """Reports that `level` (an `Acknowledgement` or an application
        stage) has reached `offset`; a level the sender did not order is
        ignored. Never raises for a transport reason; a wait cut short by
        `timeout` aborts the report."""
        op = self._start_report(level, offset)
        _op_sync(op, lib.weida_reporter_report_op_wait, lib.weida_reporter_report_op_drop, timeout)

    def _start_finish(self):
        return _start(lib.weida_reporter_finish, "weida_reporter_finish_op", self._consume())

    def finish(self, timeout: Optional[float] = None) -> None:
        """Flushes the newest offset per reported level and ends the
        report. Consumes the reporter (also when C refuses it because a
        report is in flight)."""
        op = self._start_finish()
        _op_sync(op, lib.weida_reporter_finish_op_wait, lib.weida_reporter_finish_op_drop, timeout)


class PeerEvents(_Handle):
    """A reader of a dialler's peer events (a requester, pusher, subscriber,
    dialling pair or dish), a queue (docs/libraries/weida-py.md §1): `recv`
    returns the next `PeerEvent`, or `None` once the dialler, or the relayed
    binding's registration, is gone. A reader that fell more than 64 events
    behind gets one `MISSED` event carrying how many it lost."""

    _fd_of = lib.weida_peer_events_fd

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_peer_event **")
        return out, lambda ms: lib.weida_peer_events_recv(handle, ms, out)

    @staticmethod
    def _event(ptr) -> Optional[PeerEvent]:
        return None if ptr == ffi.NULL else PeerEvent._take(ptr)

    def recv(self, timeout: Optional[float] = None) -> Optional[PeerEvent]:
        """The next event, or `None` once the dialler, or the relayed
        binding's registration, is gone."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._event(out[0])

    def __iter__(self) -> Iterator[PeerEvent]:
        """Every event until the dialler is gone, each `recv` without a
        timeout."""
        while (event := self.recv()) is not None:
            yield event


class IdentityEvents(_Handle):
    """A reader of an identity or trust source's events, a queue
    (docs/libraries/weida-py.md §1): `recv` returns the next `IdentityEvent`,
    or `None` once the source is gone (its last strong handle, here or in a
    terms, binding or requester, was dropped). A reader that fell more than
    64 events behind gets one `MISSED` event carrying how many it lost."""

    _fd_of = lib.weida_identity_events_fd

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_identity_event **")
        return out, lambda ms: lib.weida_identity_events_recv(handle, ms, out)

    @staticmethod
    def _event(ptr) -> Optional[IdentityEvent]:
        return None if ptr == ffi.NULL else IdentityEvent._take(ptr)

    def recv(self, timeout: Optional[float] = None) -> Optional[IdentityEvent]:
        """The next event, or `None` once the source is gone."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._event(out[0])

    def __iter__(self) -> Iterator[IdentityEvent]:
        """Every event until the source is gone, each `recv` without a
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

__all__ = [
    "Binding",
    "BusMember",
    "Cursors",
    "Delivery",
    "Dish",
    "FanOut",
    "Handoff",
    "IdentityEvents",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "LocalBinding",
    "OutgoingTransfer",
    "Paired",
    "PeerEvents",
    "PipeBinding",
    "Publisher",
    "Puller",
    "Pusher",
    "Radio",
    "Received",
    "Relay",
    "RelayRequest",
    "Replier",
    "ReplyStream",
    "Reporter",
    "Requester",
    "Respondent",
    "Runtime",
    "Segment",
    "Subscriber",
    "SurveyRun",
    "Surveyor",
    "UnixBinding",
]
