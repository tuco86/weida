//! I/O-free core model of the weida messaging framework.
//!
//! This crate contains no networking, no `tokio` and no serialization: only the
//! vocabulary types (endpoint addresses, limits, trace context, errors, the
//! host a peer counts as) that the wire codec and the transport share.
//! Everything here is unit-testable without a socket.
//!
//! There are no correlation or acknowledgement state machines here: Req/Rep
//! rides one bidirectional QUIC stream, so the stream *is* the correlation, and
//! delivery is QUIC's own transport receipt rather than an application-level
//! ACK (`docs/ARCHITECTURE.md`, layer model).

pub mod addr;
pub mod error;
pub mod host;
pub mod identity;
pub mod limits;
pub mod trace;

pub use addr::{
    Address, DEFAULT_PORT, EndpointAddr, InprocAddr, MAX_BUS_BYTES, MAX_PATH_BYTES,
    MAX_PIPE_NAME_BYTES, MAX_SOCKET_PATH_BYTES, PIPE_NAMESPACE, PipeAddr, SCHEME, SCHEME_INPROC,
    SCHEME_PIPE, SCHEME_UNIX, UnixAddr, validate_endpoint_path,
};
pub use error::{Error, ErrorCode, LossCause, Result, StopReason};
pub use host::{HostKey, host_key};
pub use identity::{Fingerprint, LocalPrincipal, PeerIdentity, WindowsPrincipal};
pub use limits::{Congestion, DEFAULT_DATAGRAM_RECEIVE_BYTES, Limits};
pub use trace::{TraceContext, TraceError};
