"""weida for Python, over the C ABI (`libweida`) with cffi.

The API mirrors the C ABI (docs/c/README.md): one class per handle, one
method per function. This module is the synchronous surface;
`weida.asyncio` has the same classes with coroutines for every method that
waits, and both share the value classes defined here.
"""

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
from weida._native import ffi as _ffi
from weida._native import lib as _lib
from weida._sync import (
    Binding,
    Delivery,
    IncomingRequest,
    IncomingTransfer,
    Listener,
    LocalBinding,
    OutgoingTransfer,
    Replier,
    Requester,
    Runtime,
)
from weida._values import (
    ClientTls,
    Identity,
    IncomingMeta,
    Message,
    RuntimeConfig,
    ServerTls,
    TransferMeta,
    Trust,
)


def version() -> str:
    """The version of the loaded `libweida`."""
    return _ffi.string(_lib.weida_version()).decode()


__all__ = [
    # Values.
    "ClientTls",
    "Identity",
    "IncomingMeta",
    "Message",
    "RuntimeConfig",
    "ServerTls",
    "TransferMeta",
    "Trust",
    # Handles.
    "Binding",
    "Delivery",
    "IncomingRequest",
    "IncomingTransfer",
    "Listener",
    "LocalBinding",
    "OutgoingTransfer",
    "Replier",
    "Requester",
    "Runtime",
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
    "version",
]
