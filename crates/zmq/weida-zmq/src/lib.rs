//! A ZeroMQ implementation in Rust, usable with no weida in the picture.
//!
//! This crate is a **product, not an adapter**: it implements ZeroMQ for
//! applications that speak ZeroMQ. Nothing of weida's protocol is in it and
//! nothing of it is in weida. Applications may compose both public APIs, but
//! this workspace defines no implicit mapping between their messaging models
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md) §4).
//!
//! What it shares with weida is the machine room: `weida-runtime`'s reactor,
//! resolver, `AF_UNIX` hygiene and name registry, and `weida-core`'s
//! `Error`/`LocalPrincipal` vocabulary at the boundary. `[dependencies]`
//! therefore names `weida-runtime` and `weida-core` and **never** `weida` or
//! `weida-protocol`: a ZeroMQ user must not link quinn, rustls and weida's
//! pattern layer to open a `tcp://` socket, and the dependency direction is
//! what keeps a ZeroMQ socket from reaching weida's types at all [0013 §4.2].
//!
//! # What is here
//!
//! - [`Context`] — the reactor, the `inproc://` namespace and the socket
//!   ceiling, created three ways ([`Context::new`], [`Context::with_handle`],
//!   [`Context::owned`]) and terminated with a **finite** close budget.
//! - [`Endpoint`] — `tcp://`, `ipc://` and `inproc://` parsed under libzmq's
//!   own length rules, with every other ZeroMQ transport named absent rather
//!   than called a typo.
//! - [`Message`] and [`Multipart`] — a frame and a whole message, the latter
//!   handed over as one value because ZMTP delivers "all frames or none", with
//!   `ZMQ_MAXMSGSIZE` judged from the declared length before a body is
//!   allocated.
//! - [`Pipe`] and [`Queue`] — the per-peer double queue every pattern RFC
//!   specifies, bounded by `ZMQ_SNDHWM`/`ZMQ_RCVHWM` in messages and by a byte
//!   ceiling that charges every frame and message its overhead, with
//!   [`MuteAction`] carrying `zmq_socket(3)`'s "Action in mute state" column
//!   so that no socket type re-decides it.
//! - [`Engine`] and [`SocketOptions`] — one socket's binds, connects and
//!   reconnects, with `ZMQ_RECONNECT_IVL`/`_IVL_MAX` backoff,
//!   `ZMQ_HANDSHAKE_IVL`, `ZMQ_CONNECT_TIMEOUT`, `ZMQ_IMMEDIATE`,
//!   `ZMQ_BACKLOG` and `ZMQ_LAST_ENDPOINT`, handing each established
//!   connection to a [`Session`].
//! - [`ZmtpSession`] and [`Wire`] — the protocol itself over one connection:
//!   the greeting with its 3.0 downgrade, the NULL handshake, `READY` with
//!   `Socket-Type` and `Identity`, MORE/COMMAND framing, `PING`/`PONG` gated
//!   on the negotiated version, and `ERROR` sent and understood. Everything
//!   byte-exact in it is `weida-zmtp`'s.
//! - [`RoutingId`] — 1-255 self-asserted bytes with a nonzero first octet,
//!   and the type-level distance from weida's proved identities.
//! - [`SocketCore`] — what every socket type shares: the engine, the
//!   round-robin and fair-queue decisions, the `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO`
//!   bound, and libzmq's thread rule as a type (`Send + !Sync`).
//! - [`ReqSocket`] and [`RepSocket`] — request-reply with its state machine:
//!   strict alternation reported as `EFSM`, the envelope prepended and
//!   stripped, round-robin out and last-peer in, a reply to a vanished
//!   requester discarded rather than blocking, `ZMQ_REQ_CORRELATE` and
//!   `ZMQ_REQ_RELAXED`.
//! - [`DealerSocket`] and [`RouterSocket`] — the same pattern without the
//!   lockstep: unrestricted in both directions, ROUTER addressing peers by
//!   the [`RoutingKey`] a peer announced or one it was given, with
//!   `ZMQ_ROUTER_MANDATORY`, `ZMQ_ROUTER_HANDOVER` and `ZMQ_PROBE_ROUTER`.
//! - [`PushSocket`] and [`PullSocket`] — the pipeline: round-robin over the
//!   workers with room, blocking rather than discarding, fair-queued at the
//!   sink.
//! - [`PairSocket`] — the exclusive pair: one peer, no auto-reconnect, and a
//!   further incoming connection terminated while one is live.
//! - [`PubSocket`] and [`SubSocket`] — publish-subscribe with
//!   publisher-side prefix matching, additive non-idempotent
//!   [`Subscriptions`], drop-not-block at the high-water mark, and both
//!   subscription wire forms accepted with the sent one chosen by
//!   [`SubscriptionForm`].
//! - [`XPubSocket`] and [`XSubSocket`] — the raw pair a pub/sub proxy is
//!   built from: subscriptions delivered to the application in the `1`/`0`
//!   form with `ZMQ_XPUB_VERBOSE`, `_VERBOSER`, `_MANUAL` and
//!   `_WELCOME_MSG`, an unsubscribe synthesized when a subscriber vanishes,
//!   and messages and subscriptions sent upstream.
//! - [`Error`] — libzmq's errno vocabulary, each error carrying a cause,
//!   with weida's `Error` converted at the boundary and never re-exported.
//! - `IpcBinding` (unix only) — the `ipc` transport on `AF_UNIX`, with
//!   `weida-runtime`'s bind hygiene (socket-type check, unlink-then-bind,
//!   explicit `0600`, the node removed on drop), the kernel's path budget
//!   beside libzmq's published 113, the peer's credentials captured at
//!   connect time and kept on the [`Peer`], and the endpoint-stealing hazard
//!   documented where it lives rather than papered over.
//! - [`Inproc`] — the `inproc` transport: a context-scoped namespace on
//!   `weida-runtime`'s registry with libzmq's 256-character budget, a
//!   connection that is a pair of memory buffers rather than a socket, and a
//!   connect that **parks** until the bind arrives, which is what libzmq 4.0
//!   changed.
//! - [`Security`] — PLAIN, as either end: `ZMQ_PLAIN_SERVER`,
//!   `ZMQ_PLAIN_USERNAME` and `ZMQ_PLAIN_PASSWORD` selecting the mechanism,
//!   and 24/ZMTP-PLAIN's `HELLO`/`WELCOME`/`INITIATE`/`READY` over the
//!   codec's octets.
//! - [`ZapRequest`], [`ZapReply`] and [`ZapUserId`] — 27/ZAP over
//!   `inproc://zeromq.zap.01`: status 200/300/400/500, the user id of a 200
//!   held per connection and convertible to no weida identity,
//!   `ZMQ_ZAP_DOMAIN` as the switch that turns authorization on and
//!   `ZMQ_ZAP_ENFORCE_DOMAIN` as the refusal to send an empty one, one
//!   handler per context because the namespace says so, and a refusal that
//!   lands before any message flows.
//!
//! - [`CurveClient`], [`CurveServer`] and [`CurveTransport`] — CURVE, as
//!   either end: the four keys, the cookie discarded by a valid `INITIATE`
//!   or by its interval, the nonce counters that never repeat within a
//!   connection, session keys destroyed when the connection closes, the
//!   peer's long-term key handed to the ZAP handler as the CURVE credential,
//!   and every frame after the `READY` inside a `MESSAGE` box. The layouts
//!   are `weida-zmtp`'s; the one cryptographic dependency is `crypto_box`,
//!   RustCrypto's NaCl `crypto_box` — see [`curve`].
//!
//! - [`optiontable`] — every `zmq_setsockopt` and `zmq_ctx_set` option by
//!   name, honoured under the name this library gives it or refused with one
//!   of five reasons: no transport, draft only, deprecated in favour of ZAP,
//!   replaced by a `weida-runtime` construct, or absent with what is missing
//!   named. Nothing is silently ignored, and the two deliberate default
//!   changes are rows in that table.
//!
//! - [`Monitor`] and [`MonitorEvent`] — `zmq_socket_monitor`'s event set as
//!   a typed stream **and**, through [`monitor::serve_pair`], over the
//!   two-frame `inproc://` PAIR form the Espresso recipe reads. One
//!   publisher, two renderings, and one place where the octets are decided.
//! - [`proxy`](proxy()) and [`proxy_steerable`] — the devices, over the
//!   typed socket surfaces behind [`Device`]: both directions, a capture
//!   socket that gets a copy of every message, one message held at a time,
//!   and PAUSE/RESUME/TERMINATE/STATISTICS on a control socket with
//!   libzmq's eight counters.
//!
//! - `blocking` — the synchronous facade behind the non-default `blocking`
//!   feature: one wrapper per socket type over a context that owns its
//!   reactor, `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO`/`ZMQ_DONTWAIT` where libzmq puts
//!   them, and no second implementation of any protocol behaviour.
//!
//! The zguide's canonical recipes run against this crate as examples, each
//! asserting the guarantee its original claims [0013 §4.7 clause 5]:
//! `lazy_pirate`, `simple_pirate` and `paranoid_pirate` (chapter 4's
//! pirates), `majordomo` (18/MDP 0.2 with `mmi.service`), `freelance`
//! (10/FLP models one and two), `clone` (12/CHP's three-port wire),
//! `binary_star`, `espresso` and `last_value_cache`. The `tests/zguide_*.rs`
//! files include those example files as modules, so what is asserted is the
//! code a reader runs rather than a second copy of it.
//!
//! There is no default [`Session`] beyond [`ZmtpSession`]: a socket that
//! handed its peers to a no-op would be a ZeroMQ implementation that speaks
//! nothing.
//!
//! # Rules this crate keeps
//!
//! - **Options are honoured or refused, never silently ignored** [0013 §4.4
//!   item 4]. A configuration this library cannot deliver fails where it is
//!   configured, with `EINVAL` and a message naming the value.
//! - **Two defaults deliberately differ from libzmq** [0013 §4.4 item 5]:
//!   the close budget is finite where `ZMQ_LINGER` is infinite (see
//!   [`DEFAULT_CLOSE_BUDGET`]), and `ZMQ_MAXMSGSIZE` has a real default where
//!   libzmq has "no limit" (see [`DEFAULT_MAX_MESSAGE_SIZE`]). Both are
//!   settable back; neither is silent.
//! - **Identity types stay apart** [0013 §4.4 item 6]. A ZeroMQ identity —
//!   a CURVE key, a routing id, a ZAP user id — and weida's proved
//!   `Fingerprint` are different claims about different things, so no `From`,
//!   `Into`, `AsRef` or `Deref` between the two groups exists in this
//!   workspace, and none may be added. `weida_core::LocalPrincipal` is
//!   shared, because `ipc://` peer credentials are the same kernel fact, and
//!   it is not a ZAP identity.
//! - **No remote input causes unbounded allocation** (`docs/INVARIANTS.md`),
//!   under libzmq's own names: `ZMQ_MAX_SOCKETS` in `context`, `ZMQ_SNDHWM`
//!   and `ZMQ_RCVHWM` in `pipe`, `ZMQ_MAXMSGSIZE` in `message` and
//!   `ZMQ_BACKLOG` in `options`.
//!
//! # Sources
//!
//! Every number and every quoted rule comes from `docs/research/zeromq.md`,
//! which cites libzmq 4.3.x's manual pages, 37/ZMTP and the zguide. Section
//! references in this crate's documentation (`§2`, `§11`) are that sheet's.

#![warn(missing_docs)]

#[cfg(feature = "blocking")]
pub mod blocking;
pub mod context;
pub mod curve;
pub mod dealerrouter;
pub mod endpoint;
pub mod engine;
pub mod error;
pub mod identity;
pub mod inproc;
#[cfg(unix)]
pub mod ipc;
pub mod message;
pub mod monitor;
pub mod options;
pub mod optiontable;
pub mod pair;
pub mod pipe;
pub mod pipeline;
pub mod proxy;
pub mod pubsub;
pub mod reqrep;
pub mod session;
pub mod socket;
pub mod split;
pub mod subscriptions;
pub mod transport;
pub mod xpubxsub;
pub mod zap;

pub use context::{
    Context, ContextConfig, DEFAULT_CLOSE_BUDGET, DEFAULT_MAX_SOCKETS, SocketId, SocketSlot,
    Terminated,
};
pub use curve::{
    COOKIE_LIFETIME, CurveClient, CurvePublicKey, CurveSecretKey, CurveServer, CurveTransport,
    OpenedInitiate, SecurityModel,
};
pub use dealerrouter::{DealerSocket, RouterSocket};
pub use endpoint::{Endpoint, MAX_INPROC_NAME_BYTES, MAX_IPC_ENDPOINT_BYTES, TcpHost};
pub use engine::{
    AnnouncedIdentity, Connection, Discarded, Engine, HandshakeGate, Peer, PeerId, Role, Session,
    SessionFuture,
};
pub use error::{Cause, Error, Result};
pub use identity::{MAX_ROUTING_ID_BYTES, RoutingId, RoutingKey};
pub use inproc::{INPROC_BUFFER_BYTES, Inproc, InprocBinding, InprocDial};
#[cfg(unix)]
pub use ipc::IpcBinding;
pub use message::{
    DEFAULT_MAX_MESSAGE_FRAMES, DEFAULT_MAX_MESSAGE_SIZE, Decoded, Message, MessageLimits,
    Multipart,
};
pub use monitor::{
    MONITOR_CAPACITY, Monitor, MonitorEvent, MonitorEvents, MonitorSink, serve_pair,
};
pub use options::{
    DEFAULT_BACKLOG, DEFAULT_HANDSHAKE_IVL, DEFAULT_MAX_RESOLVED_ADDRESSES, DEFAULT_RECONNECT_IVL,
    MAX_ZAP_DOMAIN_BYTES, Security, SocketOptions,
};
pub use optiontable::{OPTIONS, Refusal, Scope, Verdict, ZmqOption};
pub use pair::PairSocket;
pub use pipe::{
    DEFAULT_QUEUE_BYTES, DEFAULT_RCVHWM, DEFAULT_SNDHWM, MuteAction, Pipe, PipeConfig, Queue,
    QueueConfig, Sent,
};
pub use pipeline::{PullSocket, PushSocket};
pub use proxy::{
    CONTROL_PAUSE, CONTROL_RESUME, CONTROL_STATISTICS, CONTROL_TERMINATE, Counter, Device,
    ProxyStatistics, Steer, proxy, proxy_steerable,
};
pub use pubsub::{PubSocket, Published, SubSocket};
pub use reqrep::{RepSocket, ReqSocket};
pub use session::{Incoming, Negotiated, Wire, ZmtpSession};
pub use socket::{Delivered, SocketCore};
pub use split::{
    DealerRecv, DealerSend, PairRecv, PairSend, RouterRecv, RouterSend, XPubPublish, XPubRecv,
    XSubRecv, XSubSend,
};
pub use subscriptions::{
    DEFAULT_MAX_SUBSCRIPTION_BYTES, DEFAULT_MAX_SUBSCRIPTIONS, SubscriptionForm, Subscriptions,
};
pub use transport::Stream;
pub use xpubxsub::{XPubSocket, XSubSocket};
pub use zap::{
    AuthenticatedUser, ZAP_ENDPOINT, ZAP_NAME, ZapReply, ZapRequest, ZapStatus, ZapUserId,
};
