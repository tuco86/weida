"""The synchronous surface: every class mirrors one C handle, every method
one C function with the `weida_<type>_` prefix dropped. A waiting method
takes `timeout` in seconds (`None` waits forever) and raises `Again` when
it passes."""

import time
from typing import Callable, Iterator, Optional, Tuple

from weida._crypto import path as _fspath
from weida._enums import ErrorCode
from weida._errors import check
from weida._native import ffi, lib
from weida._stats import ConnectionStats, PeerEvent, _take_connection_stats
from weida._values import (
    ClientTls,
    Drained,
    IdentityEvent,
    IdentitySource,
    IncomingMeta,
    Message,
    RuntimeConfig,
    ServerTls,
    TransferMeta,
    TrustSource,
    _Handle,
    _count,
    _enum,
    _millis,
    _text,
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

    def _start_drain(self, deadline: float):
        # Converted before the runtime is consumed: a deadline out of range
        # raises here and leaves the runtime usable.
        ms = _millis("deadline", deadline)
        if ms > _U64_MAX:
            raise ValueError("deadline must be below 2**64 milliseconds")
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
§1): `recv` returns the next `PeerEvent`, or `None` once the requester, or
    the relayed binding's registration, is gone. A reader that fell more
    than 64 events behind gets one `MISSED` event carrying how many it lost."""

    _fd_of = lib.weida_peer_events_fd

    def _recv_attempt(self):
        handle = self._p()
        out = ffi.new("weida_peer_event **")
        return out, lambda ms: lib.weida_peer_events_recv(handle, ms, out)

    @staticmethod
    def _event(ptr) -> Optional[PeerEvent]:
        return None if ptr == ffi.NULL else PeerEvent._take(ptr)

    def recv(self, timeout: Optional[float] = None) -> Optional[PeerEvent]:
        """The next event, or `None` once the requester, or the relayed
        binding's registration, is gone."""
        out, attempt = self._recv_attempt()
        call_sync(attempt, timeout)
        return self._event(out[0])

    def __iter__(self) -> Iterator[PeerEvent]:
        """Every event until the requester is gone, each `recv` without a
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

__all__ = [
    "Binding",
    "Delivery",
    "Handoff",
    "IdentityEvents",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "LocalBinding",
    "OutgoingTransfer",
    "PeerEvents",
    "PipeBinding",
    "Relay",
    "RelayRequest",
    "Replier",
    "ReplyStream",
    "Requester",
    "Runtime",
    "UnixBinding",
]
