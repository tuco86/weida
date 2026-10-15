//! An implementation of the nanomsg Scalability Protocols in Rust, usable
//! with no weida in the picture.
//!
//! This crate is a **product, not an adapter**: it implements SP for
//! applications that speak SP, rather than bridging SP into weida. Nothing
//! of weida's protocol is in it and nothing of it is in weida — the two meet
//! only in a forwarder that terminates both, which is a third crate
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4, §4.5).
//!
//! What it shares with weida is the machine room: `weida-runtime`'s reactor,
//! resolver, `AF_UNIX` hygiene and name registry, and `weida-core`'s
//! `Error`/`LocalPrincipal` vocabulary at the boundary. `[dependencies]`
//! therefore names `weida-runtime`, `weida-core` and `weida-sp`, and
//! **never** `weida` or `weida-protocol`: an SP user must not link quinn,
//! rustls and weida's pattern layer to open a `tcp://` socket, and the
//! dependency direction is what keeps an SP socket from reaching weida's
//! types at all [0013 §4.2].
//!
//! # What exists so far
//!
//! - [`Context`] — the reactor, the `inproc://` namespace and the socket
//!   ceiling, created three ways ([`Context::new`], [`Context::with_handle`],
//!   [`Context::owned`]) and closed with a **finite** budget. NNG keeps these
//!   three things global and this crate cannot; see the module note for why,
//!   and for why this is not `nng_ctx`.
//! - [`Endpoint`] — `tcp://`, `tls+tcp://`, `ipc://` and `inproc://` parsed
//!   under NNG's own length rules — [`NNG_MAXADDRLEN`], the kernel's
//!   `sun_path` budget and the 122-bytes-including-NUL legacy IPC form —
//!   with every other NNG and SP transport named absent rather than called a
//!   typo.
//! - [`Protocol`] — one row per SP protocol: its wire identity from
//!   [`weida_sp`], whether it sends, whether it receives, and whether it
//!   holds the per-transaction state an `nng_ctx` is made of.
//! - [`Error`] — NNG's own `NNG_E*` vocabulary, each error carrying a cause,
//!   with weida's `Error` converted at the boundary and never re-exported.
//! - [`Message`] — a protocol header and an application body in separate
//!   storage, delivered whole or not at all, with `NNG_OPT_RECVMAXSZ`
//!   judged from the declared 64-bit length before a body is allocated.
//! - [`Queue`] and [`PipeId`] — `NNG_OPT_SENDBUF`/`NNG_OPT_RECVBUF` as
//!   depths in messages, where depth zero is NNG's rendezvous rather than
//!   libzmq's "unlimited", with [`FullAction`] carrying each protocol's
//!   behaviour at the bound so that no socket type re-decides it — plus the
//!   octet ceiling NNG has no option for ([`DEFAULT_QUEUE_BYTES`]), because
//!   a depth in messages times `NNG_OPT_RECVMAXSZ` is a product nobody
//!   bounded.
//!
//! # Rules this crate keeps
//!
//! - **Options are honoured or refused, never silently ignored** [0013 §4.4
//!   item 4]. A configuration this library cannot deliver fails where it is
//!   configured, with the NNG code that names the reason.
//! - **No remote input causes unbounded allocation** (`docs/INVARIANTS.md`).
//!   SP bounds nothing: a message may declare 2^64-1 octets and
//!   `NNG_OPT_RECVMAXSZ` is unlimited by default, the tag stack has no
//!   length field, and nothing limits how many connections a stranger opens
//!   (`docs/research/nanomsg-nng.md` §3, §5, §11). Every one of those gets a
//!   named, documented bound here.
//! - **Where NNG and the RFCs disagree, both numbers are published.** The
//!   PAIR v1 initial hop count and the `MAXTTL` ceiling are the two the
//!   sheet records (§3, §11); this crate says which it sends and accepts
//!   both, rather than choosing silently.
//! - [`Engine`], [`Dialer`] and [`Listener`] — one socket's dialling and
//!   listening endpoints, the pipes they create, the reconnect backoff of
//!   `NNG_OPT_RECONNMINT`/`RECONNMAXT`, and the three pipe events, with a
//!   synchronous dial that waits for the peer's protocol header without
//!   costing a thread and a named ceiling on how many pipes a socket
//!   admits ([`DEFAULT_MAX_PIPES`]).
//! - [`SocketOptions`] — every option under NNG's own name, honoured or
//!   refused where it is set, plus the three bounds SP leaves open and this
//!   library closes.
//! - [`SpSession`] — the protocol itself over one connection: the 8-octet
//!   header out before anything arrives, the peer's read and its endpoint
//!   type checked against the pairing rule before any traffic, a refusal
//!   that is a close and nothing else, and `weida-sp`'s 64-bit framing in
//!   both directions.
//!
//! # Sources
//!
//! Every number and every quoted rule comes from
//! `docs/research/nanomsg-nng.md`, which cites the nanomsg RFCs and NNG
//! 1.10.0's manual pages. Section references in this crate's documentation
//! (`§2`, `§11`) are that sheet's.
//!
//! The socket types arrive one protocol family at a time:
//!
//! - [`ReqSocket`] and [`RepSocket`] — request-reply with `nng_ctx`
//!   contexts, the 32-bit tag stack, the resend timer and its three
//!   triggers, and `NNG_ESTATE` for every order the protocol forbids.
//! - [`PushSocket`] and [`PullSocket`] — the pipeline: round-robin over
//!   the pullers that can accept a message now, fair-queued at the sink,
//!   and each direction absent from the other's type.
//! - [`PubSocket`] and [`SubSocket`] — broadcast with the filter at the
//!   receiver: every publication is offered to every subscriber, each one
//!   matches an arbitrary byte prefix locally, and
//!   [`SubSocket::discarded`] counts what that costs.
//! - [`Pair0Socket`] and [`Pair1Socket`] — the exclusive pair: one peer
//!   at a time refused at the pipe-add-pre hook, PAIR v0 with no header
//!   and PAIR v1 with its hop-count word under `NNG_OPT_MAXTTL`.
//! - [`SurveyorSocket`] and [`RespondentSocket`] — a broadcast with a
//!   deadline that starts at the send, at most one response per
//!   respondent, late responses discarded, and silence indistinguishable
//!   from slowness.
//! - [`BusSocket`] — one hop and best effort: every directly connected
//!   peer and nobody else, a discarded copy that the send still calls a
//!   success, and the ingress pipe carried so a re-broadcast can exclude
//!   it.
//! - [`RawSocket`] and [`raw::device`] — the wire with the pattern
//!   taken out: headers preserved verbatim, no state machine, no retry, no
//!   matching, contexts refused in the type, and forwarding between two
//!   raw sockets that pushes and pops the tag stack and bounds the PAIR v1
//!   hop count.

#![warn(missing_docs)]

#[cfg(feature = "blocking")]
pub mod blocking;
pub mod bus;
pub mod context;
pub mod endpoint;
pub mod engine;
pub mod error;
pub mod inproc;
#[cfg(unix)]
pub mod ipc;
pub mod message;
pub mod options;
pub mod optiontable;
pub mod pair;
pub mod pipe;
pub mod pipeline;
pub mod protocol;
pub mod pubsub;
pub mod raw;
pub mod replier;
pub mod reqrep;
pub mod session;
pub mod socket;
pub mod survey;
pub mod tls;
pub mod transport;

pub use bus::BusSocket;
pub use bytes::Bytes;
pub use context::{
    Closed, Context, ContextConfig, DEFAULT_CLOSE_BUDGET, DEFAULT_MAX_SOCKETS, SocketId, SocketSlot,
};
pub use endpoint::{
    Endpoint, MAX_INPROC_NAME_BYTES, MAX_LEGACY_IPC_PATH_BYTES, NNG_MAXADDRLEN, TcpHost,
};
pub use engine::{
    Admission, Connection, Dialer, DialerId, Engine, HandshakeGate, Listener, ListenerId,
    PipeCallback, PipeEvent, PipeInfo, PipeSet, Role, Session, SessionFuture,
};
pub use error::{Cause, Error, Result};
pub use message::{DEFAULT_RECV_MAX_SIZE, Message, RECV_MAX_SIZE_UNLIMITED};
pub use options::{
    DEFAULT_HANDSHAKE_TIMEOUT, DEFAULT_MAX_ADDRESSES, DEFAULT_MAX_PIPES,
    DEFAULT_MAX_PIPES_PER_PEER, DEFAULT_MAX_TLS_HANDSHAKES, DEFAULT_MAX_TTL, DEFAULT_RECONNECT_MAX,
    DEFAULT_RECONNECT_MIN, DEFAULT_RESEND_TIME, DEFAULT_SURVEY_TIME, EndpointOptions, NNG_MAX_TTL,
    SPEC_MAX_TTL, SocketOptions,
};
pub use pair::{POLYAMOROUS_ABSENT, Pair0Socket, Pair1Socket};
pub use pipe::{
    DEFAULT_QUEUE_BYTES, DEFAULT_RECV_DEPTH, DEFAULT_SEND_DEPTH, FullAction, MAX_QUEUE_DEPTH,
    PipeId, Queue, QueueConfig, Sent,
};
pub use pipe::{Discarded, Pipe, PipeConfig, Signals};
pub use pipeline::{PullSocket, PushSocket};
pub use protocol::{PROTOCOLS, Protocol, protocol};
pub use pubsub::{PubSocket, SubSocket};
pub use raw::{Forwarded, RawSocket, device};
pub use replier::{CtxId, Replier, ReplierCtx};
pub use reqrep::{RepCtx, RepSocket, ReqCtx, ReqSocket};
pub use session::SpSession;
pub use socket::{Broadcast, SocketCore};
pub use survey::{RespondentCtx, RespondentSocket, SurveyorCtx, SurveyorSocket};
pub use transport::{Stream, TCP_KEEPALIVE_IDLE, TCP_KEEPALIVE_INTERVAL};

pub use inproc::{INPROC_BUFFER_BYTES, Inproc, InprocBinding, InprocDial};
#[cfg(unix)]
pub use ipc::IpcBinding;
pub use optiontable::{Disposition, OPTIONS, OptionRow, Refusal, Scope};
pub use tls::{AuthMode, TlsConfig, TlsPeer};
