"""The value classes both surfaces share: configuration, TLS values,
metadata and messages, and the handle base every object stands on."""

import base64
import math
import os
import re
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple

from weida._crypto import certificate_pem, key_pem, path
from weida._enums import (
    Acknowledgement,
    Backpressure,
    Congestion,
    Deduplication,
    DeliveryLevel,
    Discovery,
    Durability,
    OrderingMode,
    OutboxFull,
    ProducerNaming,
)
from weida._errors import Closed, check
from weida._native import ffi, lib
from weida._wait import Waiters


def _closed() -> Closed:
    return Closed(103, "the object was dropped, spent or consumed")


def _text(getter: Callable, handle) -> Optional[str]:
    """A string getter's result (R7): exact length, NULL as `None`."""
    length = ffi.new("size_t *")
    ptr = getter(handle, length)
    if ptr == ffi.NULL:
        return None
    return ffi.unpack(ptr, length[0]).decode("utf-8")


def _pem_take(ptr, wipeable: bool):
    """Copies a `weida_pem` out and drops it, which wipes it: into a
    `bytearray` the caller can wipe when it holds a key, else `bytes`."""
    try:
        buffer = ffi.buffer(lib.weida_pem_data(ptr), lib.weida_pem_len(ptr))
        return bytearray(buffer) if wipeable else buffer[:]
    finally:
        lib.weida_pem_drop(ptr)


def _count(name: str, value) -> int:
    """A non-negative integer field; above the C type's range it is cffi's
    `OverflowError` at the setter call."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} is an int")
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return value


def _optional_count(name: str, value) -> int:
    """`_count`, with `None` as the C ABI's `None` (-1, R7)."""
    return -1 if value is None else _count(name, value)


def _millis(name: str, value) -> int:
    """A duration in seconds, as the C ABI's milliseconds."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} is a number of seconds")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite, non-negative number of seconds")
    return round(value * 1000)


def _optional_millis(name: str, value) -> int:
    """`_millis`, with `None` as the C ABI's `None` (-1, R7)."""
    return -1 if value is None else _millis(name, value)


def _flag(name: str, value) -> int:
    return 1 if value else 0


def _enum(cls) -> Callable:
    """A converter that takes only members of `cls`: an `IntEnum` compares
    equal to plain ints, so a laxer check would turn `Backpressure.DROP`
    into `OutboxFull.REJECT` without a word."""

    def convert(name: str, value) -> int:
        if not isinstance(value, cls):
            raise TypeError(f"{name} is a weida.{cls.__name__}")
        return int(value)

    return convert


def _optional_enum(cls) -> Callable:
    """`_enum(cls)`, with `None` as the C ABI's `None` (-1, R7)."""
    convert = _enum(cls)
    return lambda name, value: -1 if value is None else convert(name, value)


class _Handle:
    """One C handle (R5): released by the garbage collector or `drop()`,
    and refused with `Closed` once dropped or consumed, without calling C.
    `_parent` keeps whatever the handle was made from alive."""

    _fd_of: Optional[Callable] = None

    def __init__(self, ptr, drop: Callable, parent: object = None) -> None:
        self._ptr = ffi.gc(ptr, drop)
        self._parent = parent
        self._waiting: Optional[Waiters] = None

    def _p(self):
        if self._ptr is None:
            raise _closed()
        return self._ptr

    def fileno(self) -> int:
        """The readiness descriptor (R11)."""
        if self._fd_of is None:
            raise TypeError(f"{type(self).__name__} has no readiness descriptor")
        out = ffi.new("int64_t *")
        check(type(self)._fd_of(self._p(), out))
        return out[0]

    def _waiters(self) -> Waiters:
        if self._waiting is None:
            self._waiting = Waiters(self.fileno)
        return self._waiting

    def _consume(self):
        """Detaches the pointer for a C function that consumes it (R6)."""
        ptr = self._p()
        if self._waiting is not None:
            self._waiting.close()
        self._ptr = None
        # Detaches the destructor in place; the pointer stays usable.
        ffi.gc(ptr, None)
        return ptr

    def drop(self) -> None:
        """Releases the handle now, with Rust's `Drop` effects (R1)."""
        if self._ptr is None:
            return
        if self._waiting is not None:
            self._waiting.close()
        ptr, self._ptr = self._ptr, None
        ffi.release(ptr)

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.drop()


class _Fields:
    """A configuration value: keyword fields checked against _FIELDS, turned
    into a fresh handle per call by _handle()."""

    # (name, setter, convert) triples.
    _FIELDS: tuple = ()
    _OUT: str
    _NEW: Callable
    _DROP: Callable

    def _known(self) -> set:
        return {name for name, _, _ in self._FIELDS}

    def __init__(self, **fields) -> None:
        known = self._known()
        for key in fields:
            if key not in known:
                raise TypeError(f"unknown {type(self).__name__} field: {key}")
        self._fields = dict(fields)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(f'{k}={v!r}' for k, v in self._fields.items())})"

    def _new_handle(self):
        out = ffi.new(self._OUT)
        check(type(self)._NEW(out))
        return out[0]

    def _extra(self, handle) -> None:
        pass

    def _handle(self):
        handle = self._new_handle()
        try:
            for name, setter, convert in self._FIELDS:
                if name in self._fields:
                    check(setter(handle, convert(name, self._fields[name])))
            self._extra(handle)
        except BaseException:
            type(self)._DROP(handle)
            raise
        return handle


class Limits(_Fields):
    """`Limits`: the fields set here, weida's defaults for the rest. Byte,
    stream and entry counts are `int`s; `keep_alive`, `idle_timeout`,
    `hello_timeout`, `flow_early_hold` and `header_timeout` are seconds;
    `path_report` a flag; `congestion` a `Congestion`."""

    _OUT = "weida_limits **"
    _NEW = lib.weida_limits_new
    _DROP = lib.weida_limits_drop
    _FIELDS = (
        ("max_header_bytes", lib.weida_limits_set_max_header_bytes, _count),
        ("max_concurrent_uni_streams", lib.weida_limits_set_max_concurrent_uni_streams, _count),
        ("max_concurrent_bidi_streams", lib.weida_limits_set_max_concurrent_bidi_streams, _count),
        ("stream_receive_window", lib.weida_limits_set_stream_receive_window, _count),
        ("connection_receive_window", lib.weida_limits_set_connection_receive_window, _count),
        ("keep_alive", lib.weida_limits_set_keep_alive_ms, _millis),
        ("idle_timeout", lib.weida_limits_set_idle_timeout_ms, _millis),
        ("hello_timeout", lib.weida_limits_set_hello_timeout_ms, _millis),
        ("max_subscriptions", lib.weida_limits_set_max_subscriptions, _count),
        ("subscriber_buffer_bytes", lib.weida_limits_set_subscriber_buffer_bytes, _count),
        ("max_sequence_scopes", lib.weida_limits_set_max_sequence_scopes, _count),
        ("max_reorder_hold", lib.weida_limits_set_max_reorder_hold, _count),
        ("max_local_streams", lib.weida_limits_set_max_local_streams, _count),
        ("max_parked_reverse", lib.weida_limits_set_max_parked_reverse, _count),
        ("max_dedup_entries", lib.weida_limits_set_max_dedup_entries, _count),
        ("datagram_receive_bytes", lib.weida_limits_set_datagram_receive_bytes, _count),
        ("datagram_send_bytes", lib.weida_limits_set_datagram_send_bytes, _count),
        ("path_report", lib.weida_limits_set_path_report, _flag),
        ("max_flows", lib.weida_limits_set_max_flows, _count),
        ("flow_queue_bytes", lib.weida_limits_set_flow_queue_bytes, _count),
        ("flow_early_bytes", lib.weida_limits_set_flow_early_bytes, _count),
        ("flow_early_hold", lib.weida_limits_set_flow_early_hold_ms, _millis),
        ("congestion", lib.weida_limits_set_congestion, _enum(Congestion)),
        ("header_timeout", lib.weida_limits_set_header_timeout_ms, _millis),
        ("max_header_streams", lib.weida_limits_set_max_header_streams, _count),
    )


class GuaranteeSet(_Fields):
    """`GuaranteeSet`, starting from `CORE`: `delivery` a `DeliveryLevel`,
    `acknowledgement` an `Acknowledgement`, `durability` a `Durability` or
    `None`, `replicas` an `int` or `None`, `ordering` an `OrderingMode`,
    `deduplication` a `Deduplication`, `dedup_window` seconds or `None`,
    `backpressure` a `Backpressure`, `producer_naming` a `ProducerNaming`,
    `control_isolated` a flag. No combination is checked here, as in the C
    ABI: a set a peer cannot meet fails the connect or the first request
    with `Negotiation`."""

    _OUT = "weida_guarantee_set **"
    _NEW = lib.weida_guarantee_set_new
    _DROP = lib.weida_guarantee_set_drop
    _FIELDS = (
        ("delivery", lib.weida_guarantee_set_set_delivery, _enum(DeliveryLevel)),
        ("acknowledgement", lib.weida_guarantee_set_set_acknowledgement, _enum(Acknowledgement)),
        ("durability", lib.weida_guarantee_set_set_durability, _optional_enum(Durability)),
        ("replicas", lib.weida_guarantee_set_set_replicas, _optional_count),
        ("ordering", lib.weida_guarantee_set_set_ordering, _enum(OrderingMode)),
        ("deduplication", lib.weida_guarantee_set_set_deduplication, _enum(Deduplication)),
        ("dedup_window", lib.weida_guarantee_set_set_dedup_window_ms, _optional_millis),
        ("backpressure", lib.weida_guarantee_set_set_backpressure, _enum(Backpressure)),
        ("producer_naming", lib.weida_guarantee_set_set_producer_naming, _enum(ProducerNaming)),
        ("control_isolated", lib.weida_guarantee_set_set_control_isolated, _flag),
    )


class ReconnectPolicy(_Fields):
    """`ReconnectPolicy`: `initial` and `max` backoff in seconds, `jitter` a
    flag, `max_attempts` an `int` or `None` (unbounded; above `u32::MAX` it
    is `InvalidArgument`), `stop_on_peer_closed` a flag."""

    _OUT = "weida_reconnect_policy **"
    _NEW = lib.weida_reconnect_policy_new
    _DROP = lib.weida_reconnect_policy_drop
    _FIELDS = (
        ("initial", lib.weida_reconnect_policy_set_initial_ms, _millis),
        ("max", lib.weida_reconnect_policy_set_max_ms, _millis),
        ("jitter", lib.weida_reconnect_policy_set_jitter, _flag),
        ("max_attempts", lib.weida_reconnect_policy_set_max_attempts, _optional_count),
        ("stop_on_peer_closed", lib.weida_reconnect_policy_set_stop_on_peer_closed, _flag),
    )
    _never = False

    @classmethod
    def never(cls, **fields) -> "ReconnectPolicy":
        """`ReconnectPolicy::never()`: no redial; the fields given override it."""
        policy = cls(**fields)
        policy._never = True
        return policy

    def _new_handle(self):
        if not self._never:
            return super()._new_handle()
        out = ffi.new(self._OUT)
        check(lib.weida_reconnect_policy_never(out))
        return out[0]


class RuntimeConfig(_Fields):
    """`RuntimeConfig`: the fields set here, weida's defaults for the rest.
    Counts are `int`s; `shutdown_timeout` and `connect_attempt_timeout` are
    seconds, `send_timeout` seconds or `None` (no bound); `outbox_full` an
    `OutboxFull`, `discovery` a `Discovery`; `limits` a `Limits`,
    `guarantees` a `GuaranteeSet`, `reconnect` a `ReconnectPolicy`, each
    copied when `Runtime.owned` builds the runtime."""

    _OUT = "weida_runtime_config **"
    _NEW = lib.weida_runtime_config_new
    _DROP = lib.weida_runtime_config_drop
    _FIELDS = (
        ("worker_threads", lib.weida_runtime_config_set_worker_threads, _count),
        ("shutdown_timeout", lib.weida_runtime_config_set_shutdown_timeout_ms, _millis),
        ("max_connections", lib.weida_runtime_config_set_max_connections, _count),
        ("max_connections_per_peer", lib.weida_runtime_config_set_max_connections_per_peer, _count),
        ("endpoint_queue", lib.weida_runtime_config_set_endpoint_queue, _count),
        ("max_resolved_addresses", lib.weida_runtime_config_set_max_resolved_addresses, _count),
        ("connect_attempt_timeout", lib.weida_runtime_config_set_connect_attempt_timeout_ms, _millis),
        ("send_timeout", lib.weida_runtime_config_set_send_timeout_ms, _optional_millis),
        ("outbox_messages", lib.weida_runtime_config_set_outbox_messages, _count),
        ("outbox_bytes", lib.weida_runtime_config_set_outbox_bytes, _count),
        ("outbox_full", lib.weida_runtime_config_set_outbox_full, _enum(OutboxFull)),
        ("max_handshakes", lib.weida_runtime_config_set_max_handshakes, _count),
        ("retry_above_handshakes", lib.weida_runtime_config_set_retry_above_handshakes, _count),
        ("discovery", lib.weida_runtime_config_set_discovery, _enum(Discovery)),
    )
    _NESTED = (
        ("limits", Limits, lib.weida_runtime_config_set_limits),
        ("guarantees", GuaranteeSet, lib.weida_runtime_config_set_guarantees),
        ("reconnect", ReconnectPolicy, lib.weida_runtime_config_set_reconnect),
    )

    def _known(self) -> set:
        return super()._known() | {name for name, _, _ in self._NESTED}

    def _extra(self, handle) -> None:
        for name, cls, setter in self._NESTED:
            if name not in self._fields:
                continue
            value = self._fields[name]
            if not isinstance(value, cls):
                raise TypeError(f"{name} is a weida.{cls.__name__}")
            nested = value._handle()
            try:
                check(setter(handle, nested))
            finally:
                cls._DROP(nested)



class Trust(_Handle):
    """Whom a dialler trusts: what each address names, plus pins and anchors."""

    def __init__(self) -> None:
        out = ffi.new("weida_trust **")
        check(lib.weida_trust_new(out))
        super().__init__(out[0], lib.weida_trust_drop)

    def add_pin(self, fingerprint: str) -> "Trust":
        """Pins a `sha256:` fingerprint; returns this trust."""
        check(lib.weida_trust_add_pin(self._p(), fingerprint.encode()))
        return self

    def add_anchor(self, anchor) -> "Trust":
        """Trusts certificates chaining to `anchor` (PEM bytes, a cryptography
        `x509.Certificate` or a list of them) for the names they carry;
        returns this trust. The bytes are copied, and parsed at each dial: a
        bad PEM fails that dial with `Tls`."""
        if isinstance(anchor, (str, os.PathLike)):
            raise TypeError(
                "add_anchor takes PEM bytes or a cryptography x509.Certificate;"
                " a path goes to add_anchor_file"
            )
        pem = certificate_pem(anchor)
        data = ffi.from_buffer("uint8_t[]", pem)
        check(lib.weida_trust_add_anchor(self._p(), data, len(data)))
        return self

    def add_anchor_file(self, file) -> "Trust":
        """Trusts the PEM anchors in `file` (`str`, `bytes` or `os.PathLike`),
        read at each dial; returns this trust."""
        check(lib.weida_trust_add_anchor_file(self._p(), path(file)))
        return self


class Identity(_Handle):
    """A certificate chain and its private key."""

    @classmethod
    def generate(cls) -> "Identity":
        """A self-signed identity with a fresh key, for pinning."""
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_generate(out))
        return cls._wrap(out[0])

    @classmethod
    def generate_for(cls, names: Sequence[str]) -> "Identity":
        """A self-signed identity whose certificate names `names` (DNS names or
        IP literals), so a peer anchoring it can check the dialled host. An
        empty list is `generate()`; a name the certificate cannot carry raises
        `Tls`."""
        if isinstance(names, (str, bytes, bytearray)):
            raise TypeError("generate_for takes a list of names, not one string")
        names = list(names)
        for name in names:
            if not isinstance(name, str):
                raise TypeError("every name is a str")
            if "\0" in name:
                raise ValueError("embedded null byte")
        encoded = [ffi.new("char[]", name.encode()) for name in names]
        array = ffi.new("const char *[]", encoded) if encoded else ffi.NULL
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_generate_for(array, len(encoded), out))
        return cls._wrap(out[0])

    @classmethod
    def from_pem(cls, cert, key) -> "Identity":
        """An identity from PEM bytes (any bytes-like): the chain, leaf first,
        and the key."""
        cert_data = ffi.from_buffer("uint8_t[]", cert)
        key_data = ffi.from_buffer("uint8_t[]", key)
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_from_pem(cert_data, len(cert_data), key_data, len(key_data), out))
        return cls._wrap(out[0])

    @classmethod
    def from_pem_file(cls, file) -> "Identity":
        """An identity from one PEM file holding the chain and the key, the
        form `to_pem()` writes, as `ssl.SSLContext.load_cert_chain(certfile)`
        takes it. Nothing is read here; a missing or bad file raises `Tls` at
        first use (`fingerprint()`, the PEM getters, a bind or a dial)."""
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_from_pem_file(path(file), out))
        return cls._wrap(out[0])

    @classmethod
    def from_pem_files(cls, cert_chain, key) -> "Identity":
        """An identity from a PEM chain file (leaf first) and a PEM key file.
        Nothing is read here; a missing or bad file raises `Tls` at first use
        (`fingerprint()`, the PEM getters, a bind or a dial)."""
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_from_pem_files(path(cert_chain), path(key), out))
        return cls._wrap(out[0])

    @classmethod
    def from_cryptography(cls, cert, key) -> "Identity":
        """An identity from `cryptography` objects. `cert` is a cryptography
        `x509.Certificate`, a list of them leaf first, or PEM bytes; `key` is a
        cryptography RSA, EC or Ed25519 private key (exported as PKCS#8) or
        PEM bytes. Needs `cryptography` only for its objects."""
        return cls.from_pem(certificate_pem(cert), key_pem(key))

    @classmethod
    def _wrap(cls, ptr) -> "Identity":
        identity = cls.__new__(cls)
        _Handle.__init__(identity, ptr, lib.weida_identity_drop)
        return identity

    def fingerprint(self) -> str:
        """The public-key fingerprint, `sha256:` text."""
        out = ffi.new("char **")
        check(lib.weida_identity_fingerprint(self._p(), out))
        return ffi.string(out[0]).decode()

    def certificate_pem(self) -> bytes:
        """The certificate chain as PEM, leaf first, without the key; read
        anew on every call."""
        out = ffi.new("weida_pem **")
        check(lib.weida_identity_certificate_pem(self._p(), out))
        return _pem_take(out[0], False)

    def to_pem(self) -> bytearray:
        """The chain and the private key as one PEM document, the form
        `from_pem_file` reads. It holds the private key: wipe it when done
        (`pem[:] = bytes(len(pem))`); every other copy is the caller's to
        wipe."""
        out = ffi.new("weida_pem **")
        check(lib.weida_identity_to_pem(self._p(), out))
        return _pem_take(out[0], True)

    def certificates(self) -> List[bytes]:
        """The DER of each certificate of the chain, leaf first;
        `cryptography.x509.load_der_x509_certificate` reads each one."""
        blocks = re.findall(
            rb"-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----",
            self.certificate_pem(),
            re.S,
        )
        return [base64.b64decode(block) for block in blocks]


class _AnyKey:
    """`ClientTrust::AnyKey`: any client key the client proves it holds."""

    def __repr__(self) -> str:
        return "weida.ANY_KEY"


ANY_KEY = _AnyKey()


class ClientTls(_Handle):
    """The TLS terms a requester dials on, under `trust`: anonymous, or
    presenting `identity`."""

    def __init__(self, trust: Trust, identity: Optional[Identity] = None) -> None:
        if identity is not None and not isinstance(identity, Identity):
            raise TypeError("identity is a weida.Identity")
        out = ffi.new("weida_client_tls **")
        check(lib.weida_client_tls_new(trust._p(), out))
        super().__init__(out[0], lib.weida_client_tls_drop)
        if identity is not None:
            try:
                check(lib.weida_client_tls_set_identity(self._p(), identity._p()))
            except BaseException:
                self.drop()
                raise


class ServerTls(_Handle):
    """The TLS terms a QUIC binding serves on, presenting `identity`.

    With `require_client` every client must present a key: one the `Trust`
    accepts (pinned or chaining to an anchor), or with `ANY_KEY` any key it
    proves it holds. A refused client raises `Tls` on its side, and nothing
    reaches the replier. An empty `Trust` makes the bind raise `Tls`."""

    def __init__(self, identity: Identity, require_client=None) -> None:
        if not (require_client is None or require_client is ANY_KEY or isinstance(require_client, Trust)):
            raise TypeError("require_client is a weida.Trust or weida.ANY_KEY")
        out = ffi.new("weida_server_tls **")
        check(lib.weida_server_tls_new(identity._p(), out))
        super().__init__(out[0], lib.weida_server_tls_drop)
        if require_client is None:
            return
        try:
            if require_client is ANY_KEY:
                check(lib.weida_server_tls_require_any_key(self._p()))
            else:
                check(lib.weida_server_tls_require_client(self._p(), require_client._p()))
        except BaseException:
            self.drop()
            raise


class TransferMeta:
    """The metadata an outgoing transfer carries: `content_type`,
    `content_len` (an `int`), `trace` (a W3C traceparent; a malformed one is
    `InvalidArgument`), `topic`, and `achieved` (an `Acknowledgement`)."""

    def __init__(
        self,
        content_type: Optional[str] = None,
        *,
        content_len: Optional[int] = None,
        trace: Optional[str] = None,
        topic: Optional[str] = None,
        achieved: Optional[Acknowledgement] = None,
    ) -> None:
        self.content_type = content_type
        self.content_len = content_len
        self.trace = trace
        self.topic = topic
        self.achieved = achieved

    def _handle(self):
        out = ffi.new("weida_transfer_meta **")
        check(lib.weida_transfer_meta_new(out))
        meta = out[0]
        try:
            if self.content_type is not None:
                check(lib.weida_transfer_meta_set_content_type(meta, self.content_type.encode()))
            if self.content_len is not None:
                check(lib.weida_transfer_meta_set_content_len(meta, _count("content_len", self.content_len)))
            if self.topic is not None:
                check(lib.weida_transfer_meta_set_topic(meta, self.topic.encode()))
            if self.trace is not None:
                check(lib.weida_transfer_meta_set_trace(meta, self.trace.encode()))
            if self.achieved is not None:
                level = _enum(Acknowledgement)("achieved", self.achieved)
                check(lib.weida_transfer_meta_set_achieved(meta, level))
        except BaseException:
            lib.weida_transfer_meta_drop(meta)
            raise
        return meta


def _with_transfer_meta(meta: Optional[TransferMeta], use: Callable):
    """Calls `use` with `meta` as a fresh handle (`NULL` for `None`) and
    drops the handle afterwards; the C call copies it."""
    if not (meta is None or isinstance(meta, TransferMeta)):
        raise TypeError("meta is a weida.TransferMeta or None")
    handle = ffi.NULL if meta is None else meta._handle()
    try:
        return use(handle)
    finally:
        if handle != ffi.NULL:
            lib.weida_transfer_meta_drop(handle)


@dataclass(frozen=True)
class IncomingMeta:
    """The metadata of an inbound transfer or request.

    `peer_chain`: the DER certificates the peer presented, leaf first; `None`
    for an anonymous or local peer, or a chain over the bound weida keeps.
    `trace`: the W3C traceparent the transfer carries, or `None`.
    `tracestate`: the W3C tracestate beside it, or `None`.
    `achieved`: the `Acknowledgement` the sender reports, or `None`."""

    endpoint: Optional[str]
    content_type: Optional[str]
    topic: Optional[str]
    peer_fingerprint: Optional[str]
    content_len: Optional[int]
    sequence: Optional[int]
    peer_chain: Optional[Tuple[bytes, ...]]
    trace: Optional[str]
    tracestate: Optional[str]
    achieved: Optional[Acknowledgement]

    @classmethod
    def _take(cls, ptr) -> "IncomingMeta":
        """Copies a `weida_incoming_meta` out and drops it."""
        try:
            number = ffi.new("uint64_t *")

            def optional(getter: Callable) -> Optional[int]:
                return number[0] if getter(ptr, number) == 1 else None

            count = ffi.new("size_t *")
            peer_chain = None
            if lib.weida_incoming_meta_peer_chain_len(ptr, count) == 1:
                length = ffi.new("size_t *")
                certs = []
                for i in range(count[0]):
                    cert = lib.weida_incoming_meta_peer_chain_cert(ptr, i, length)
                    certs.append(ffi.buffer(cert, length[0])[:])
                peer_chain = tuple(certs)

            level = ffi.new("int *")
            achieved = Acknowledgement(level[0]) if lib.weida_incoming_meta_achieved(ptr, level) == 1 else None

            return cls(
                endpoint=_text(lib.weida_incoming_meta_endpoint, ptr),
                content_type=_text(lib.weida_incoming_meta_content_type, ptr),
                topic=_text(lib.weida_incoming_meta_topic, ptr),
                peer_fingerprint=_text(lib.weida_incoming_meta_peer_fingerprint, ptr),
                content_len=optional(lib.weida_incoming_meta_content_len),
                sequence=optional(lib.weida_incoming_meta_sequence),
                peer_chain=peer_chain,
                trace=_text(lib.weida_incoming_meta_trace, ptr),
                tracestate=_text(lib.weida_incoming_meta_tracestate, ptr),
                achieved=achieved,
            )
        finally:
            lib.weida_incoming_meta_drop(ptr)


@dataclass(frozen=True)
class Message:
    """A collected payload and the metadata of its transfer."""

    payload: bytes
    meta: IncomingMeta

    @classmethod
    def _take(cls, ptr) -> "Message":
        """Copies a `weida_msg` out and drops it."""
        try:
            payload = ffi.buffer(lib.weida_msg_data(ptr), lib.weida_msg_len(ptr))[:]
            out = ffi.new("weida_incoming_meta **")
            check(lib.weida_msg_meta(ptr, out))
            return cls(payload=payload, meta=IncomingMeta._take(out[0]))
        finally:
            lib.weida_msg_drop(ptr)


@dataclass(frozen=True)
class Drained:
    """What one drain achieved, counted over the transfers still
    unacknowledged when it started. `delivered`: acknowledged by the peer's
    transport before the deadline. `outstanding`: unacknowledged at the
    deadline, refused, or lost with their connection; not an error."""

    delivered: int
    outstanding: int

    @classmethod
    def _take(cls, ptr) -> "Drained":
        """Copies a `weida_drained` out and drops it."""
        try:
            return cls(
                delivered=lib.weida_drained_delivered(ptr),
                outstanding=lib.weida_drained_outstanding(ptr),
            )
        finally:
            lib.weida_drained_drop(ptr)
