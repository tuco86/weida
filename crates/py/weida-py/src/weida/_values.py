"""The value classes both surfaces share: configuration, TLS values,
metadata and messages, and the handle base every object stands on."""

import base64
import enum
import math
import os
import re
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple, Union

from weida._crypto import certificate_pem, key_pem, path
from weida._enums import (
    Acknowledgement,
    Alpn,
    Backpressure,
    Congestion,
    Deduplication,
    DeliveryLevel,
    Discovery,
    Durability,
    IdentityEventKind,
    OrderingMode,
    OutboxFull,
    ProducerNaming,
    ReportMode,
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


def _int(name: str, value) -> int:
    """A signed integer field; outside the C type's range it is cffi's
    `OverflowError` at the setter call."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} is an int")
    return value


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


def _c_string(name: str, value) -> bytes:
    """A `str` argument as the C string the ABI takes."""
    if not isinstance(value, str):
        raise TypeError(f"{name} is a str")
    if "\0" in value:
        raise ValueError("embedded null byte")
    return value.encode()


def _c_names(caller: str, names) -> Tuple[list, object]:
    """`names`, a sequence of `str`, as a C `const char *[]` (NULL when
    empty) and the buffers it points into, which the caller keeps alive
    across the C call."""
    if isinstance(names, (str, bytes, bytearray)):
        raise TypeError(f"{caller} takes a list of names, not one string")
    encoded = [ffi.new("char[]", _c_string("every name", name)) for name in names]
    return encoded, (ffi.new("const char *[]", encoded) if encoded else ffi.NULL)


def _of(name: str, value, cls):
    """The handle of `value`, which must be a `cls`."""
    if not isinstance(value, cls):
        raise TypeError(f"{name} is a weida.{cls.__name__}")
    return value._p()


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
    copied when `Runtime.owned` builds the runtime; `resolver` a callable
    `(Lookup) -> list[str]`, or `None` for the system resolver
    (`Runtime.owned` says how it is called)."""

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
        return super()._known() | {name for name, _, _ in self._NESTED} | {"resolver"}

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


class _JoinTerms(_Fields):
    """`JoinTerms`, the keyword arguments of `Dish.join`: `max_age` seconds
    or `None` (no budget), `max_layer` an `int` or `None` (every layer)."""

    _OUT = "weida_join_terms **"
    _NEW = lib.weida_join_terms_new
    _DROP = lib.weida_join_terms_drop
    _FIELDS = (
        ("max_age", lib.weida_join_terms_set_max_age_ms, _optional_millis),
        ("max_layer", lib.weida_join_terms_set_max_layer, _optional_count),
    )


class _SegmentTerms(_Fields):
    """`SegmentTerms`, the keyword arguments of `Radio.segment`: `max_age`
    seconds or `None` (no budget), `priority` an `int`, `follows_upstream`
    a flag."""

    _OUT = "weida_segment_terms **"
    _NEW = lib.weida_segment_terms_new
    _DROP = lib.weida_segment_terms_drop
    _FIELDS = (
        ("max_age", lib.weida_segment_terms_set_max_age_ms, _optional_millis),
        ("priority", lib.weida_segment_terms_set_priority, _int),
        ("follows_upstream", lib.weida_segment_terms_set_follows_upstream, _flag),
    )


def _with_terms(terms: _Fields, use: Callable):
    """Calls `use` with `terms` as a fresh handle and drops the handle
    afterwards; the C call copies it. A conversion error raises before
    `use` runs."""
    handle = terms._handle()
    try:
        return use(handle)
    finally:
        type(terms)._DROP(handle)


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

    def is_empty(self) -> bool:
        """Whether this trust holds no pin and no anchor, so it accepts only
        what each dialled address names."""
        return check(lib.weida_trust_is_empty(self._p())) == 1

    @classmethod
    def _wrap(cls, ptr) -> "Trust":
        trust = cls.__new__(cls)
        _Handle.__init__(trust, ptr, lib.weida_trust_drop)
        return trust


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
        encoded, array = _c_names("generate_for", names)
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


@dataclass(frozen=True)
class IdentityEvent:
    """One change of an identity or trust source; a field another kind does
    not carry is `None`: `fingerprint` for LOADED and RENEWED, `from_` and
    `to` for KEY_CHANGED, `message` for RENEWAL_FAILED, `missed` for MISSED."""

    kind: IdentityEventKind
    fingerprint: Optional[str]
    from_: Optional[str]
    to: Optional[str]
    message: Optional[str]
    missed: Optional[int]

    @classmethod
    def _take(cls, ptr) -> "IdentityEvent":
        """Copies a `weida_identity_event` out and drops it."""
        try:
            count = ffi.new("uint64_t *")
            missed = count[0] if lib.weida_identity_event_missed(ptr, count) == 1 else None
            return cls(
                kind=IdentityEventKind(lib.weida_identity_event_kind(ptr)),
                fingerprint=_text(lib.weida_identity_event_fingerprint, ptr),
                from_=_text(lib.weida_identity_event_from, ptr),
                to=_text(lib.weida_identity_event_to, ptr),
                message=_text(lib.weida_identity_event_message, ptr),
                missed=missed,
            )
        finally:
            lib.weida_identity_event_drop(ptr)


class _Made(_Handle):
    """A handle only its factories make: calling the class is a TypeError."""

    _DROP: Callable
    _MADE_BY: str

    def __init__(self, *args, **kwargs) -> None:
        raise TypeError(f"{type(self).__name__} is made by {self._MADE_BY}")

    @classmethod
    def _wrap(cls, ptr):
        made = cls.__new__(cls)
        _Handle.__init__(made, ptr, cls._DROP)
        return made


class IdentitySource(_Made):
    """A source of the identity a TLS term presents. `ClientTls` and
    `ServerTls` share it, so an update reaches the next handshake with no
    re-bind; dropping this handle leaves the source alive wherever it is
    held."""

    _DROP = lib.weida_identity_source_drop
    _MADE_BY = "IdentitySource.from_identity, .ephemeral, .external or .files"

    @classmethod
    def from_identity(cls, identity: Identity) -> "IdentitySource":
        """A source that holds `identity` (copied) for good; `Tls` when its
        material does not read."""
        out = ffi.new("weida_identity_source **")
        check(lib.weida_identity_source_from_identity(_of("identity", identity, Identity), out))
        return cls._wrap(out[0])

    @classmethod
    def ephemeral(cls, names: Sequence[str]) -> "IdentitySource":
        """A source of a fresh self-signed identity for `names` (DNS names or
        IP literals), held in memory only."""
        encoded, array = _c_names("ephemeral", names)
        out = ffi.new("weida_identity_source **")
        check(lib.weida_identity_source_ephemeral(array, len(encoded), out))
        return cls._wrap(out[0])

    @classmethod
    def external(cls, identity: Identity) -> "IdentitySource":
        """A source starting at `identity` that the application renews with
        `update`, `report_failure` and `report_handoff_stolen`."""
        out = ffi.new("weida_identity_source **")
        check(lib.weida_identity_source_external(_of("identity", identity, Identity), out))
        return cls._wrap(out[0])

    @classmethod
    def files(cls, dir, names: Optional[Sequence[str]] = None, poll: Optional[float] = None) -> "IdentitySource":
        """A source reading `cert.pem` and `key.pem` from the directory `dir`
        (`str`, `bytes` or `os.PathLike`). When neither exists a self-signed
        identity for `names` is written there first (directory 0700, files
        0600 on Unix). Replace the files atomically, or the certificate first
        and the key second; they are checked at the next use after `poll`
        seconds (default 10, `0` checks at every use). `Tls` when one file
        exists without the other or does not parse, `Io` when the bootstrap
        cannot write."""
        # Every argument is converted before C, so a wrong one touches
        # nothing on disk.
        if names is not None:
            encoded, array = _c_names("files", names)
        if poll is not None:
            poll_ms = _millis("poll", poll)
        directory = path(dir)
        opts = ffi.new("weida_files_options **")
        check(lib.weida_files_options_new(opts))
        try:
            if names is not None:
                check(lib.weida_files_options_set_names(opts[0], array, len(encoded)))
            if poll is not None:
                check(lib.weida_files_options_set_poll_ms(opts[0], poll_ms))
            out = ffi.new("weida_identity_source **")
            check(lib.weida_identity_source_files(directory, opts[0], out))
        finally:
            lib.weida_files_options_drop(opts[0])
        return cls._wrap(out[0])

    def current(self) -> Identity:
        """The identity the source holds now, a copy."""
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_source_current(self._p(), out))
        return Identity._wrap(out[0])

    def fingerprint(self) -> str:
        """The fingerprint of the current identity, `sha256:` text."""
        buffer = ffi.new("char[]", lib.WEIDA_FINGERPRINT_LEN + 1)
        check(lib.weida_identity_source_fingerprint(self._p(), buffer, len(buffer)))
        return ffi.string(buffer).decode()

    def update(self, identity: Identity) -> None:
        """Replaces the identity (copied); the next handshake presents it."""
        check(lib.weida_identity_source_update(self._p(), _of("identity", identity, Identity)))

    def report_failure(self, reason: str) -> None:
        """Reports a failed renewal: a `RENEWAL_FAILED` event with `reason`."""
        check(lib.weida_identity_source_report_failure(self._p(), _c_string("reason", reason)))

    def report_handoff_stolen(self) -> None:
        """Reports that another holder took the identity over: a
        `HANDOFF_STOLEN` event."""
        check(lib.weida_identity_source_report_handoff_stolen(self._p()))

    def reload(self) -> bool:
        """Checks a `files` source's files now; whether the identity changed.
        Always `False` for the other sources."""
        return check(lib.weida_identity_source_reload(self._p())) == 1

    def events(self) -> "IdentityEvents":
        """A new reader of this source's events, from now on; each call is an
        independent reader."""
        out = ffi.new("weida_identity_events **")
        check(lib.weida_identity_source_events(self._p(), out))
        # No parent: the events end once the source is gone, which holding
        # it would prevent.
        return self._IdentityEvents(out[0], lib.weida_identity_events_drop)

    def downgrade(self) -> "WeakIdentitySource":
        """A weak handle, which does not keep the source alive."""
        out = ffi.new("weida_weak_identity_source **")
        check(lib.weida_identity_source_downgrade(self._p(), out))
        return self._Weak._wrap(out[0])


class WeakIdentitySource(_Made):
    """A weak handle of an `IdentitySource`."""

    _DROP = lib.weida_weak_identity_source_drop
    _MADE_BY = "IdentitySource.downgrade()"

    def upgrade(self) -> Optional[IdentitySource]:
        """A strong handle, or `None` once the last strong handle is gone."""
        out = ffi.new("weida_identity_source **")
        if check(lib.weida_weak_identity_source_upgrade(self._p(), out)) == 0:
            return None
        return self._Strong._wrap(out[0])


class TrustSource(_Made):
    """A source of the `Trust` a TLS term checks peers against, shared like
    an `IdentitySource`: an update reaches the next handshake."""

    _DROP = lib.weida_trust_source_drop
    _MADE_BY = "TrustSource.from_trust or .external"

    @classmethod
    def from_trust(cls, trust: Trust) -> "TrustSource":
        """A source that holds `trust` (copied) for good."""
        out = ffi.new("weida_trust_source **")
        check(lib.weida_trust_source_from_trust(_of("trust", trust, Trust), out))
        return cls._wrap(out[0])

    @classmethod
    def external(cls, trust: Trust) -> "TrustSource":
        """A source starting at `trust` that the application refreshes with
        `update` and `report_failure`."""
        out = ffi.new("weida_trust_source **")
        check(lib.weida_trust_source_external(_of("trust", trust, Trust), out))
        return cls._wrap(out[0])

    def current(self) -> Trust:
        """The trust the source holds now, a copy."""
        out = ffi.new("weida_trust **")
        check(lib.weida_trust_source_current(self._p(), out))
        return Trust._wrap(out[0])

    def update(self, trust: Trust) -> None:
        """Replaces the trust (copied): a `TRUST_REFRESHED` event."""
        check(lib.weida_trust_source_update(self._p(), _of("trust", trust, Trust)))

    def report_failure(self, reason: str) -> None:
        """Reports a failed refresh: a `RENEWAL_FAILED` event with `reason`."""
        check(lib.weida_trust_source_report_failure(self._p(), _c_string("reason", reason)))

    def is_empty(self) -> bool:
        """Whether the current trust holds no pin and no anchor."""
        return check(lib.weida_trust_source_is_empty(self._p())) == 1

    def events(self) -> "IdentityEvents":
        """A new reader of this source's events, from now on; each call is an
        independent reader."""
        out = ffi.new("weida_identity_events **")
        check(lib.weida_trust_source_events(self._p(), out))
        # No parent: the events end once the source is gone, which holding
        # it would prevent.
        return self._IdentityEvents(out[0], lib.weida_identity_events_drop)

    def downgrade(self) -> "WeakTrustSource":
        """A weak handle, which does not keep the source alive."""
        out = ffi.new("weida_weak_trust_source **")
        check(lib.weida_trust_source_downgrade(self._p(), out))
        return self._Weak._wrap(out[0])


class WeakTrustSource(_Made):
    """A weak handle of a `TrustSource`."""

    _DROP = lib.weida_weak_trust_source_drop
    _MADE_BY = "TrustSource.downgrade()"

    def upgrade(self) -> Optional[TrustSource]:
        """A strong handle, or `None` once the last strong handle is gone."""
        out = ffi.new("weida_trust_source **")
        if check(lib.weida_weak_trust_source_upgrade(self._p(), out)) == 0:
            return None
        return self._Strong._wrap(out[0])


# The classes a factory method hands out; `weida.asyncio` replaces them with
# its own.
IdentitySource._Weak = WeakIdentitySource
WeakIdentitySource._Strong = IdentitySource
TrustSource._Weak = WeakTrustSource
WeakTrustSource._Strong = TrustSource


class _AnyKey:
    """`ClientTrust::AnyKey`: any client key the client proves it holds."""

    def __repr__(self) -> str:
        return "weida.ANY_KEY"


ANY_KEY = _AnyKey()


class ClientTls(_Handle):
    """The TLS terms a requester dials on, under `trust`: anonymous, or
    presenting `identity`.

    `trust` is a `Trust` or a `TrustSource`, `identity` an `Identity` or an
    `IdentitySource`: a value is copied, a source shared, so its update
    reaches the next connection. `alpn` is the token asked for (`None` is
    `Alpn.WEIDA`); a server offering another fails the dial with `Tls`."""

    def __init__(self, trust, identity=None, *, alpn: Optional[Alpn] = None) -> None:
        if not isinstance(trust, (Trust, TrustSource)):
            raise TypeError("trust is a weida.Trust or weida.TrustSource")
        if not (identity is None or isinstance(identity, (Identity, IdentitySource))):
            raise TypeError("identity is a weida.Identity or weida.IdentitySource")
        token = None if alpn is None else _enum(Alpn)("alpn", alpn)
        out = ffi.new("weida_client_tls **")
        if isinstance(trust, TrustSource):
            check(lib.weida_client_tls_new_source(trust._p(), out))
        else:
            check(lib.weida_client_tls_new(trust._p(), out))
        super().__init__(out[0], lib.weida_client_tls_drop)
        try:
            if isinstance(identity, IdentitySource):
                check(lib.weida_client_tls_set_identity_source(self._p(), identity._p()))
            elif identity is not None:
                check(lib.weida_client_tls_set_identity(self._p(), identity._p()))
            if token is not None:
                check(lib.weida_client_tls_set_alpn(self._p(), token))
        except BaseException:
            self.drop()
            raise


class ServerTls(_Handle):
    """The TLS terms a QUIC binding serves on, presenting `identity`, an
    `Identity` or an `IdentitySource`: a value is copied, a source shared, so
    its update reaches the next handshake.

    With `require_client` every client must present a key: one the `Trust`
    or `TrustSource` accepts (pinned or chaining to an anchor), or with
    `ANY_KEY` any key it proves it holds. A refused client raises `Tls` on
    its side, and nothing reaches the replier. An empty `Trust` or
    `TrustSource` makes the bind raise `Tls`. `alpn` is the token offered
    (`None` is `Alpn.WEIDA`); a client asking for another fails with `Tls`."""

    def __init__(self, identity, require_client=None, *, alpn: Optional[Alpn] = None) -> None:
        if not isinstance(identity, (Identity, IdentitySource)):
            raise TypeError("identity is a weida.Identity or weida.IdentitySource")
        if not (
            require_client is None
            or require_client is ANY_KEY
            or isinstance(require_client, (Trust, TrustSource))
        ):
            raise TypeError("require_client is a weida.Trust, a weida.TrustSource or weida.ANY_KEY")
        token = None if alpn is None else _enum(Alpn)("alpn", alpn)
        out = ffi.new("weida_server_tls **")
        if isinstance(identity, IdentitySource):
            check(lib.weida_server_tls_new_source(identity._p(), out))
        else:
            check(lib.weida_server_tls_new(identity._p(), out))
        super().__init__(out[0], lib.weida_server_tls_drop)
        try:
            if require_client is ANY_KEY:
                check(lib.weida_server_tls_require_any_key(self._p()))
            elif isinstance(require_client, TrustSource):
                check(lib.weida_server_tls_require_client_source(self._p(), require_client._p()))
            elif require_client is not None:
                check(lib.weida_server_tls_require_client(self._p(), require_client._p()))
            if token is not None:
                check(lib.weida_server_tls_set_alpn(self._p(), token))
        except BaseException:
            self.drop()
            raise


def _cursor_level(name: str, value) -> int:
    """A cursor level in: an `Acknowledgement` member or an application
    stage as an `int`, passed on as its wire value; C refuses 6 to 15 with
    `InvalidArgument`, and above `u64` it is cffi's `OverflowError`."""
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or (isinstance(value, enum.IntEnum) and not isinstance(value, Acknowledgement))
    ):
        raise TypeError(f"{name} is a weida.Acknowledgement or an int")
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return int(value)


def _level(wire: int) -> Union[Acknowledgement, int]:
    """A cursor level out: weida's own as an `Acknowledgement`, an
    application stage as an `int`."""
    return Acknowledgement(wire) if wire < lib.WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR else wire


def _levels(getter: Callable, handle) -> Tuple[Union[Acknowledgement, int], ...]:
    """An array of cursor levels a getter returns (R7), ascending; `()` for
    NULL."""
    length = ffi.new("size_t *")
    ptr = getter(handle, length)
    if ptr == ffi.NULL:
        return ()
    return tuple(_level(ptr[i]) for i in range(length[0]))


class TransferMeta:
    """The metadata an outgoing transfer carries: `content_type`,
    `content_len` (an `int`), `trace` (a W3C traceparent; a malformed one is
    `InvalidArgument`), `topic`, `achieved` (an `Acknowledgement`), `report`
    (a sequence of cursor levels: `Acknowledgement` members, or application
    stages as `int`s at or above `CURSOR_LEVEL_APPLICATION_FLOOR`; 6 to 15
    is `InvalidArgument`, more than 16 distinct ones fail the send with
    `LimitExceeded`; an empty sequence orders nothing) and `report_mode` (a
    `ReportMode`)."""

    def __init__(
        self,
        content_type: Optional[str] = None,
        *,
        content_len: Optional[int] = None,
        trace: Optional[str] = None,
        topic: Optional[str] = None,
        achieved: Optional[Acknowledgement] = None,
        report: Optional[Sequence[Union[Acknowledgement, int]]] = None,
        report_mode: Optional[ReportMode] = None,
    ) -> None:
        self.content_type = content_type
        self.content_len = content_len
        self.trace = trace
        self.topic = topic
        self.achieved = achieved
        self.report = report
        self.report_mode = report_mode

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
            if self.report is not None:
                if isinstance(self.report, (str, bytes)) or not isinstance(self.report, Sequence):
                    raise TypeError("report is a sequence of cursor levels")
                levels = [_cursor_level(f"report[{i}]", level) for i, level in enumerate(self.report)]
                array = ffi.new("uint64_t[]", levels) if levels else ffi.NULL
                check(lib.weida_transfer_meta_set_report(meta, array, len(levels)))
            if self.report_mode is not None:
                mode = _enum(ReportMode)("report_mode", self.report_mode)
                check(lib.weida_transfer_meta_set_report_mode(meta, mode))
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
class LocalPrincipal:
    """The principal the kernel proved for an `AF_UNIX` peer at connect
    time. `pid` is `None` where the platform reports none (macOS once the
    peer closed); it is an observation only, never authorize on it."""

    uid: int
    gid: int
    pid: Optional[int]


@dataclass(frozen=True)
class WindowsPrincipal:
    """The account the kernel proved for a named-pipe peer: `sid` in its
    string form (`S-1-...`). `pid` is `None` where the pipe reports none;
    it is an observation only, never authorize on it."""

    sid: str
    pid: Optional[int]


def _peer_local(ptr) -> Optional[Union[LocalPrincipal, WindowsPrincipal]]:
    """The local principal of a live `weida_incoming_meta`, or `None` for
    a peer that is not local (QUIC, in-process)."""
    number = ffi.new("uint32_t *")
    pid = number[0] if lib.weida_incoming_meta_peer_pid(ptr, number) == 1 else None
    if lib.weida_incoming_meta_peer_uid(ptr, number) == 1:
        uid = number[0]
        # Set whenever the uid is: both come from one PeerIdentity::Local.
        lib.weida_incoming_meta_peer_gid(ptr, number)
        return LocalPrincipal(uid=uid, gid=number[0], pid=pid)
    sid = _text(lib.weida_incoming_meta_peer_sid, ptr)
    return None if sid is None else WindowsPrincipal(sid=sid, pid=pid)


@dataclass(frozen=True)
class Gap:
    """What went missing before a transfer under `PER_PRODUCER_DETECT`
    (weida's `Gap`): `expected` is the sequence number expected next,
    `seen` the one that arrived; `seen - expected` transfers are missing."""

    expected: int
    seen: int


@dataclass(frozen=True)
class IncomingMeta:
    """The metadata of an inbound transfer or request.

    `peer_chain`: the DER certificates the peer presented, leaf first; `None`
    for an anonymous or local peer, or a chain over the bound weida keeps.
    `trace`: the W3C traceparent the transfer carries, or `None`.
    `tracestate`: the W3C tracestate beside it, or `None`.
    `achieved`: the `Acknowledgement` the sender reports, or `None`.
    `peer_local`: the principal the kernel proved for a local peer, a
    `LocalPrincipal` for an `AF_UNIX` peer, a `WindowsPrincipal` for a
    named-pipe peer, `None` for any other.
    `gap`: a `Gap` when the connection negotiated
    `OrderingMode.PER_PRODUCER_DETECT` and a sequence number was skipped,
    else `None`.
    `segment` and `layer`: the segment number and the layer (0 to 15) of a
    RADIO stream segment, both or neither; `None` for any other transfer.
    `report`: the cursor levels the sender ordered, ascending,
    `Acknowledgement` members below `CURSOR_LEVEL_APPLICATION_FLOOR` and
    `int`s at or above it; `()` when it ordered none. `report_mode`: the
    `ReportMode` it asked for (`PROGRESS` when it stated none). `report_id`:
    the id of the receiver's cursor stream, an `int` exactly when levels
    were ordered, else `None`."""

    endpoint: Optional[str]
    content_type: Optional[str]
    topic: Optional[str]
    peer_fingerprint: Optional[str]
    content_len: Optional[int]
    sequence: Optional[int]
    gap: Optional[Gap]
    segment: Optional[int]
    layer: Optional[int]
    peer_chain: Optional[Tuple[bytes, ...]]
    peer_local: Optional[Union[LocalPrincipal, WindowsPrincipal]]
    trace: Optional[str]
    tracestate: Optional[str]
    achieved: Optional[Acknowledgement]
    report: Tuple[Union[Acknowledgement, int], ...]
    report_mode: ReportMode
    report_id: Optional[int]

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

            expected = ffi.new("uint64_t *")
            seen = ffi.new("uint64_t *")
            gap = (
                Gap(expected=expected[0], seen=seen[0])
                if lib.weida_incoming_meta_gap(ptr, expected, seen) == 1
                else None
            )

            layer_out = ffi.new("uint8_t *")
            layer = layer_out[0] if lib.weida_incoming_meta_layer(ptr, layer_out) == 1 else None

            return cls(
                endpoint=_text(lib.weida_incoming_meta_endpoint, ptr),
                content_type=_text(lib.weida_incoming_meta_content_type, ptr),
                topic=_text(lib.weida_incoming_meta_topic, ptr),
                peer_fingerprint=_text(lib.weida_incoming_meta_peer_fingerprint, ptr),
                content_len=optional(lib.weida_incoming_meta_content_len),
                sequence=optional(lib.weida_incoming_meta_sequence),
                gap=gap,
                segment=optional(lib.weida_incoming_meta_segment),
                layer=layer,
                peer_chain=peer_chain,
                peer_local=_peer_local(ptr),
                trace=_text(lib.weida_incoming_meta_trace, ptr),
                tracestate=_text(lib.weida_incoming_meta_tracestate, ptr),
                achieved=achieved,
                report=_levels(lib.weida_incoming_meta_report, ptr),
                report_mode=ReportMode(check(lib.weida_incoming_meta_report_mode(ptr))),
                report_id=optional(lib.weida_incoming_meta_report_id),
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


@dataclass(frozen=True)
class TopicDrops:
    """What a publisher (or a radio) dropped on one topic, by cause, copied
    out at once. `TopicDrops::total` is not mapped: sum the counts."""

    topic: str
    subscriber_budget: int
    subscriber_queue: int
    no_parked_connection: int
    superseded: int
    expired: int
    too_large: int
    no_datagrams: int
    layers_cut: int


_DROPS_COUNTS = (
    ("subscriber_budget", lib.weida_topic_drops_subscriber_budget),
    ("subscriber_queue", lib.weida_topic_drops_subscriber_queue),
    ("no_parked_connection", lib.weida_topic_drops_no_parked_connection),
    ("superseded", lib.weida_topic_drops_superseded),
    ("expired", lib.weida_topic_drops_expired),
    ("too_large", lib.weida_topic_drops_too_large),
    ("no_datagrams", lib.weida_topic_drops_no_datagrams),
    ("layers_cut", lib.weida_topic_drops_layers_cut),
)


def _take_topic_drops(ptr) -> Tuple[TopicDrops, ...]:
    """Copies every record of a `weida_topic_drops` snapshot out and drops it."""
    try:
        number = ffi.new("uint64_t *")
        length = ffi.new("size_t *")
        records = []
        for i in range(lib.weida_topic_drops_len(ptr)):
            counts = {}
            for name, getter in _DROPS_COUNTS:
                getter(ptr, i, number)  # 1 for every index below len
                counts[name] = number[0]
            topic = lib.weida_topic_drops_topic(ptr, i, length)
            records.append(TopicDrops(topic=ffi.unpack(topic, length[0]).decode("utf-8"), **counts))
        return tuple(records)
    finally:
        lib.weida_topic_drops_drop(ptr)


@dataclass(frozen=True)
class DishDrops:
    """What a radio dropped toward one joined dish connection, by cause,
    copied out at once. `peer_fingerprint` is the `sha256:` key the dish
    proved, `None` for one that proved none."""

    peer_fingerprint: Optional[str]
    subscriber_budget: int
    subscriber_queue: int
    superseded: int
    expired: int
    too_large: int
    no_datagrams: int
    layers_cut: int


_DISH_DROPS_COUNTS = (
    ("subscriber_budget", lib.weida_dish_drops_subscriber_budget),
    ("subscriber_queue", lib.weida_dish_drops_subscriber_queue),
    ("superseded", lib.weida_dish_drops_superseded),
    ("expired", lib.weida_dish_drops_expired),
    ("too_large", lib.weida_dish_drops_too_large),
    ("no_datagrams", lib.weida_dish_drops_no_datagrams),
    ("layers_cut", lib.weida_dish_drops_layers_cut),
)


def _take_dish_drops(ptr) -> Tuple[DishDrops, ...]:
    """Copies every record of a `weida_dish_drops` snapshot out and drops it."""
    try:
        number = ffi.new("uint64_t *")
        length = ffi.new("size_t *")
        records = []
        for i in range(lib.weida_dish_drops_len(ptr)):
            counts = {}
            for name, getter in _DISH_DROPS_COUNTS:
                getter(ptr, i, number)  # 1 for every index below len
                counts[name] = number[0]
            key = lib.weida_dish_drops_peer_fingerprint(ptr, i, length)
            fingerprint = None if key == ffi.NULL else ffi.unpack(key, length[0]).decode("utf-8")
            records.append(DishDrops(peer_fingerprint=fingerprint, **counts))
        return tuple(records)
    finally:
        lib.weida_dish_drops_drop(ptr)


@dataclass(frozen=True)
class CursorEntry:
    """One level of a cursor set and how far it got: `level` an
    `Acknowledgement` below `CURSOR_LEVEL_APPLICATION_FLOOR`, else an
    application stage as an `int`; `offset` an `int`."""

    level: Union[Acknowledgement, int]
    offset: int


# The newest offset per reported level, ascending by level.
CursorSet = Tuple[CursorEntry, ...]


def _take_cursor_set(ptr) -> CursorSet:
    """Copies a `weida_cursor_set` out and drops it."""
    try:
        level = ffi.new("uint64_t *")
        offset = ffi.new("uint64_t *")
        entries = []
        for i in range(lib.weida_cursor_set_len(ptr)):
            lib.weida_cursor_set_level(ptr, i, level, offset)
            entries.append(CursorEntry(level=_level(level[0]), offset=offset[0]))
        return tuple(entries)
    finally:
        lib.weida_cursor_set_drop(ptr)
