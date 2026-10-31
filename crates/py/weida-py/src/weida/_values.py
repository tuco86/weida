"""The value classes both surfaces share: configuration, TLS values,
metadata and messages, and the handle base every object stands on."""

from dataclasses import dataclass
from typing import Callable, Optional

from weida._errors import Closed, check
from weida._native import ffi, lib
from weida._wait import Waiters

_UNSET = object()


def _closed() -> Closed:
    return Closed(103, "the object was dropped, spent or consumed")


def _text(getter: Callable, handle) -> Optional[str]:
    """A string getter's result (R7): exact length, NULL as `None`."""
    length = ffi.new("size_t *")
    ptr = getter(handle, length)
    if ptr == ffi.NULL:
        return None
    return ffi.unpack(ptr, length[0]).decode("utf-8")


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


class RuntimeConfig:
    """`RuntimeConfig`: the fields set here, weida's defaults for the rest.
    `shutdown_timeout` is in seconds."""

    def __init__(self, worker_threads=_UNSET, shutdown_timeout=_UNSET) -> None:
        self.worker_threads = worker_threads
        self.shutdown_timeout = shutdown_timeout

    def _handle(self):
        out = ffi.new("weida_runtime_config **")
        check(lib.weida_runtime_config_new(out))
        cfg = out[0]
        try:
            if self.worker_threads is not _UNSET:
                check(lib.weida_runtime_config_set_worker_threads(cfg, self.worker_threads))
            if self.shutdown_timeout is not _UNSET:
                ms = round(self.shutdown_timeout * 1000)
                check(lib.weida_runtime_config_set_shutdown_timeout_ms(cfg, ms))
        except BaseException:
            lib.weida_runtime_config_drop(cfg)
            raise
        return cfg


class Trust(_Handle):
    """Whom a dialler trusts: what each address names, plus pins."""

    def __init__(self) -> None:
        out = ffi.new("weida_trust **")
        check(lib.weida_trust_new(out))
        super().__init__(out[0], lib.weida_trust_drop)

    def add_pin(self, fingerprint: str) -> "Trust":
        """Pins a `sha256:` fingerprint; returns this trust."""
        check(lib.weida_trust_add_pin(self._p(), fingerprint.encode()))
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
    def from_pem(cls, cert: bytes, key: bytes) -> "Identity":
        """An identity from PEM bytes: the chain, leaf first, and the key."""
        out = ffi.new("weida_identity **")
        check(lib.weida_identity_from_pem(cert, len(cert), key, len(key), out))
        return cls._wrap(out[0])

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


class ClientTls(_Handle):
    """The TLS terms a requester dials on: anonymous, under `trust`."""

    def __init__(self, trust: Trust) -> None:
        out = ffi.new("weida_client_tls **")
        check(lib.weida_client_tls_new(trust._p(), out))
        super().__init__(out[0], lib.weida_client_tls_drop)


class ServerTls(_Handle):
    """The TLS terms a QUIC binding serves on, presenting `identity`."""

    def __init__(self, identity: Identity) -> None:
        out = ffi.new("weida_server_tls **")
        check(lib.weida_server_tls_new(identity._p(), out))
        super().__init__(out[0], lib.weida_server_tls_drop)


class TransferMeta:
    """The metadata an outgoing transfer carries."""

    def __init__(self, content_type: Optional[str] = None) -> None:
        self.content_type = content_type

    def _handle(self):
        out = ffi.new("weida_transfer_meta **")
        check(lib.weida_transfer_meta_new(out))
        meta = out[0]
        try:
            if self.content_type is not None:
                check(lib.weida_transfer_meta_set_content_type(meta, self.content_type.encode()))
        except BaseException:
            lib.weida_transfer_meta_drop(meta)
            raise
        return meta


@dataclass(frozen=True)
class IncomingMeta:
    """The metadata of an inbound transfer or request."""

    endpoint: Optional[str]
    content_type: Optional[str]
    topic: Optional[str]
    peer_fingerprint: Optional[str]
    content_len: Optional[int]
    sequence: Optional[int]

    @classmethod
    def _take(cls, ptr) -> "IncomingMeta":
        """Copies a `weida_incoming_meta` out and drops it."""
        try:
            number = ffi.new("uint64_t *")

            def optional(getter: Callable) -> Optional[int]:
                return number[0] if getter(ptr, number) == 1 else None

            return cls(
                endpoint=_text(lib.weida_incoming_meta_endpoint, ptr),
                content_type=_text(lib.weida_incoming_meta_content_type, ptr),
                topic=_text(lib.weida_incoming_meta_topic, ptr),
                peer_fingerprint=_text(lib.weida_incoming_meta_peer_fingerprint, ptr),
                content_len=optional(lib.weida_incoming_meta_content_len),
                sequence=optional(lib.weida_incoming_meta_sequence),
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
