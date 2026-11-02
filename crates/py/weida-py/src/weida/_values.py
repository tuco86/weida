"""The value classes both surfaces share: configuration, TLS values,
metadata and messages, and the handle base every object stands on."""

import base64
import os
import re
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple

from weida._crypto import certificate_pem, key_pem, path
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


def _pem_take(ptr, wipeable: bool):
    """Copies a `weida_pem` out and drops it, which wipes it: into a
    `bytearray` the caller can wipe when it holds a key, else `bytes`."""
    try:
        buffer = ffi.buffer(lib.weida_pem_data(ptr), lib.weida_pem_len(ptr))
        return bytearray(buffer) if wipeable else buffer[:]
    finally:
        lib.weida_pem_drop(ptr)


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
    """The metadata of an inbound transfer or request.

    `peer_chain`: the DER certificates the peer presented, leaf first; `None`
    for an anonymous or local peer, or a chain over the bound weida keeps."""

    endpoint: Optional[str]
    content_type: Optional[str]
    topic: Optional[str]
    peer_fingerprint: Optional[str]
    content_len: Optional[int]
    sequence: Optional[int]
    peer_chain: Optional[Tuple[bytes, ...]]

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

            return cls(
                endpoint=_text(lib.weida_incoming_meta_endpoint, ptr),
                content_type=_text(lib.weida_incoming_meta_content_type, ptr),
                topic=_text(lib.weida_incoming_meta_topic, ptr),
                peer_fingerprint=_text(lib.weida_incoming_meta_peer_fingerprint, ptr),
                content_len=optional(lib.weida_incoming_meta_content_len),
                sequence=optional(lib.weida_incoming_meta_sequence),
                peer_chain=peer_chain,
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
