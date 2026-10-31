"""One exception class per status code of the C ABI (docs/c/README.md R9)."""

from typing import Dict, Optional, Type

from weida._native import ffi, lib


class WeidaError(Exception):
    """A failed call. `code` is the positive `WEIDA_E*` constant and
    `detail` the library's own text for this failure."""

    code = 0

    def __init__(self, code: int, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class RuntimeFailure(WeidaError):
    """`WEIDA_ERUNTIME`: the runtime could not be created or used."""


class InvalidAddress(WeidaError):
    """`WEIDA_EINVALID_ADDRESS`."""


class InvalidEndpointPath(WeidaError):
    """`WEIDA_EINVALID_ENDPOINT_PATH`."""


class InvalidFingerprint(WeidaError):
    """`WEIDA_EINVALID_FINGERPRINT`."""


class AlreadyRegistered(WeidaError):
    """`WEIDA_EALREADY_REGISTERED`."""


class NotConnected(WeidaError):
    """`WEIDA_ENOT_CONNECTED`."""


class ConnectionLost(WeidaError):
    """`WEIDA_ECONNECTION_LOST`: definitely not delivered. `cause` names
    why: `idle_timeout`, `peer_closed`, `peer_restarting`, `locally_closed`,
    `reset` or `transport_error`."""

    cause: Optional[str] = None


class Negotiation(WeidaError):
    """`WEIDA_ENEGOTIATION`."""


class Protocol(WeidaError):
    """`WEIDA_EPROTOCOL`."""


class Rejected(WeidaError):
    """`WEIDA_EREJECTED`: the peer refused the transfer."""


class UnknownEndpoint(WeidaError):
    """`WEIDA_EUNKNOWN_ENDPOINT`."""


class Unsupported(WeidaError):
    """`WEIDA_EUNSUPPORTED`."""


class NoParkedConnection(WeidaError):
    """`WEIDA_ENO_PARKED_CONNECTION`."""


class NoReply(WeidaError):
    """`WEIDA_ENO_REPLY`: the request was accepted and never answered."""


class Canceled(WeidaError):
    """`WEIDA_ECANCELED`: the transfer was canceled."""


class Expired(WeidaError):
    """`WEIDA_EEXPIRED`."""


class Indeterminate(WeidaError):
    """`WEIDA_EINDETERMINATE`: the outcome is unknown."""


class LimitExceeded(WeidaError):
    """`WEIDA_ELIMIT_EXCEEDED`: a limit or a ceiling was reached."""


class DatagramsUnavailable(WeidaError):
    """`WEIDA_EDATAGRAMS_UNAVAILABLE`."""


class TooLarge(WeidaError):
    """`WEIDA_ETOO_LARGE`; `max` is the largest payload carried."""

    max: Optional[int] = None


class Tls(WeidaError):
    """`WEIDA_ETLS`."""


class Untrusted(WeidaError):
    """`WEIDA_EUNTRUSTED`; `fingerprint` is what the peer presented."""

    fingerprint: Optional[str] = None


class Io(WeidaError):
    """`WEIDA_EIO`; `os_errno` is the OS error when there was one."""

    os_errno: Optional[int] = None


class Transport(WeidaError):
    """`WEIDA_ETRANSPORT`."""


class Again(WeidaError, TimeoutError):
    """`WEIDA_EAGAIN`: nothing was ready within the timeout."""


class Interrupted(WeidaError):
    """`WEIDA_EINTR`: a signal reached the waiting thread."""


class InvalidArgument(WeidaError, ValueError):
    """`WEIDA_EINVAL`."""


class Closed(WeidaError):
    """`WEIDA_ECLOSED`: the object was dropped, spent or consumed."""


class Panic(WeidaError):
    """`WEIDA_EPANIC`: the library caught a panic."""


_BY_CODE: Dict[int, Type[WeidaError]] = {
    1: RuntimeFailure,
    2: InvalidAddress,
    3: InvalidEndpointPath,
    4: InvalidFingerprint,
    5: AlreadyRegistered,
    6: NotConnected,
    7: ConnectionLost,
    8: Negotiation,
    9: Protocol,
    10: Rejected,
    11: UnknownEndpoint,
    12: Unsupported,
    13: NoParkedConnection,
    14: NoReply,
    15: Canceled,
    16: Expired,
    17: Indeterminate,
    18: LimitExceeded,
    19: DatagramsUnavailable,
    20: TooLarge,
    21: Tls,
    22: Untrusted,
    23: Io,
    24: Transport,
    100: Again,
    101: Interrupted,
    102: InvalidArgument,
    103: Closed,
    104: Panic,
}

_LOSS = {
    1: "idle_timeout",
    2: "peer_closed",
    3: "peer_restarting",
    4: "locally_closed",
    5: "reset",
    6: "transport_error",
}

AGAIN = -100
INTERRUPTED = -101


def _string(ptr) -> Optional[str]:
    if ptr == ffi.NULL:
        return None
    return ffi.string(ptr).decode("utf-8", "replace")


def error(rc: int) -> WeidaError:
    """The exception for the failure `rc` (negative) that the call just
    made on this thread recorded."""
    code = -rc
    cls = _BY_CODE.get(code, WeidaError)
    detail = _string(lib.weida_last_error_message()) or _string(lib.weida_strerror(rc)) or ""
    e = cls(code, detail)
    if cls is ConnectionLost:
        e.cause = _LOSS.get(lib.weida_last_error_loss_cause())
    elif cls is Untrusted:
        e.fingerprint = _string(lib.weida_last_error_fingerprint())
    elif cls is TooLarge:
        e.max = lib.weida_last_error_max()
    elif cls is Io:
        e.os_errno = lib.weida_last_error_os_code() or None
    return e


def check(rc: int) -> int:
    """Raises for a negative status, reading the record of the failing call
    on this thread right after it; returns a non-negative one."""
    if rc < 0:
        raise error(rc)
    return rc

