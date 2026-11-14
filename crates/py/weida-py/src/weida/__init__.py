"""weida for Python, over the C ABI (`libweida`) with cffi.

The API mirrors the C ABI (docs/c/README.md): one class per handle, one
method per function. This module is the synchronous surface;
`weida.asyncio` has the same classes with coroutines for every method that
waits, and both share the value classes defined here.
"""

from weida._enums import (
    Acknowledgement,
    Backpressure,
    Congestion,
    Deduplication,
    DeliveryLevel,
    Discovery,
    Durability,
    ErrorCode,
    GiveUp,
    LossCause,
    OrderingMode,
    OutboxFull,
    PeerEventKind,
    ProducerNaming,
)
from weida._errors import (
    AlreadyRegistered,
    Again,
    Canceled,
    Closed,
    ConnectionLost,
    DatagramsUnavailable,
    Expired,
    Indeterminate,
    InvalidAddress,
    InvalidArgument,
    InvalidEndpointPath,
    InvalidFingerprint,
    Interrupted,
    Io,
    LimitExceeded,
    Negotiation,
    NoParkedConnection,
    NoReply,
    NotConnected,
    Panic,
    Protocol,
    Rejected,
    RuntimeFailure,
    Tls,
    TooLarge,
    Transport,
    UnknownEndpoint,
    Unsupported,
    Untrusted,
    WeidaError,
)
from weida._errors import check as _check
from weida._native import ffi as _ffi
from weida._native import lib as _lib
from weida._stats import (
    ConnectionStats,
    PathStats,
    PeerEvent,
    RemoteStats,
    TransportStats,
    UdpCounts,
)
from weida._sync import (
    Binding,
    Delivery,
    Handoff,
    IncomingRequest,
    IncomingTransfer,
    Listener,
    LocalBinding,
    OutgoingTransfer,
    PeerEvents,
    PipeBinding,
    Relay,
    RelayRequest,
    Replier,
    ReplyStream,
    Requester,
    Runtime,
    UnixBinding,
)
from weida._values import (
    ANY_KEY,
    ClientTls,
    Drained,
    GuaranteeSet,
    Identity,
    IncomingMeta,
    Limits,
    LocalPrincipal,
    Message,
    ReconnectPolicy,
    RuntimeConfig,
    ServerTls,
    TransferMeta,
    Trust,
    WindowsPrincipal,
)


def version() -> str:
    """The version of the loaded `libweida`."""
    return _ffi.string(_lib.weida_version()).decode()


def strerror(code: int) -> str:
    """The C ABI's text for a status code, positive or negative; "unknown
    error" for any other."""
    return _ffi.string(_lib.weida_strerror(code)).decode()


def new_trace() -> str:
    """A fresh sampled W3C traceparent for `TransferMeta(trace=...)`; weida
    never starts a trace on its own."""
    buffer = _ffi.new("char[]", _lib.WEIDA_TRACEPARENT_LEN + 1)
    _check(_lib.weida_new_trace(buffer, len(buffer)))
    return _ffi.string(buffer).decode()


# The `Limits.datagram_receive_bytes` a profile with flows uses.
DEFAULT_DATAGRAM_RECEIVE_BYTES: int = _lib.WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES


__all__ = [
    # Values.
    "ANY_KEY",
    "ClientTls",
    "ConnectionStats",
    "Drained",
    "GuaranteeSet",
    "Identity",
    "IncomingMeta",
    "Limits",
    "LocalPrincipal",
    "Message",
    "PathStats",
    "PeerEvent",
    "ReconnectPolicy",
    "RemoteStats",
    "RuntimeConfig",
    "ServerTls",
    "TransferMeta",
    "TransportStats",
    "Trust",
    "UdpCounts",
    "WindowsPrincipal",
    # Enumerations.
    "Acknowledgement",
    "Backpressure",
    "Congestion",
    "Deduplication",
    "DeliveryLevel",
    "Discovery",
    "Durability",
    "ErrorCode",
    "GiveUp",
    "LossCause",
    "OrderingMode",
    "OutboxFull",
    "PeerEventKind",
    "ProducerNaming",
    "DEFAULT_DATAGRAM_RECEIVE_BYTES",
    # Handles.
    "Binding",
    "Delivery",
    "Handoff",
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
    # Errors.
    "WeidaError",
    "AlreadyRegistered",
    "Again",
    "Canceled",
    "Closed",
    "ConnectionLost",
    "DatagramsUnavailable",
    "Expired",
    "Indeterminate",
    "InvalidAddress",
    "InvalidArgument",
    "InvalidEndpointPath",
    "InvalidFingerprint",
    "Interrupted",
    "Io",
    "LimitExceeded",
    "Negotiation",
    "NoParkedConnection",
    "NoReply",
    "NotConnected",
    "Panic",
    "Protocol",
    "Rejected",
    "RuntimeFailure",
    "Tls",
    "TooLarge",
    "Transport",
    "UnknownEndpoint",
    "Unsupported",
    "Untrusted",
    "new_trace",
    "strerror",
    "version",
]
