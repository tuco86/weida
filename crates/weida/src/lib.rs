//! weida: a QUIC-native messaging framework.
//!
//! This crate hosts the runtime, the native QUIC transport, the raw stream core
//! and the brokerless messaging patterns: Req/Rep, Push/Pull, Pub/Sub, PAIR,
//! SURVEY and BUS — the whole nanomsg set, none of which adds wire
//! vocabulary. A completion is a **cursor**: a level plus an absolute byte
//! offset, reported on a unidirectional stream of its own
//! ([`Cursors`], [`Reporter`]).
//! `docs/ARCHITECTURE.md` describes the layer model,
//! `docs/PROTOCOL.md` is the normative wire specification, and
//! `docs/FAILURE_MODEL.md` defines what each outcome means.
//!
//! ```no_run
//! use weida::{Runtime, RuntimeConfig, TransferMeta, Trust};
//!
//! # async fn example() -> weida::Result<()> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//!
//! // Trust belongs to the dialling endpoint, not to the runtime. Here the
//! // address itself names the peer's public key, so nothing else is needed.
//! let requester = runtime.requester(Trust::by_address());
//! requester
//!     .connect("weida://sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@127.0.0.1:7443/transform")
//!     .await?;
//!
//! // One bidirectional stream: the request half and the reply half. The
//! // stream is the correlation, so nothing on the wire names the exchange.
//! let (mut transfer, reply) = requester.open(TransferMeta::default()).await?;
//! transfer.write_all(b"hello weida").await?;
//! transfer.finish()?;
//!
//! let body = reply.recv().await?.collect(64 * 1024).await?;
//! println!("{}", String::from_utf8_lossy(&body));
//! # Ok(())
//! # }
//! ```
//!
//! The example above runs on the caller's ambient Tokio reactor. A caller
//! that has none — or whose executor is not Tokio at all — uses
//! [`Runtime::owned`] instead: the runtime then owns the reactor `quinn`
//! needs, and every task, timer and name lookup weida performs runs there,
//! while the futures it hands back may be driven by any executor. Transfer
//! payloads implement both the `tokio::io` and the `futures-io` traits for
//! the same reason.

#[cfg(feature = "blocking")]
pub mod blocking;

// The chunk framing both socket transports share: a named pipe has no
// half-close and a unix socket has no abort, so the end of a payload is a
// frame rather than a socket state.
#[cfg(any(unix, windows))]
mod chunked;
mod config;
mod conn;
mod cursor;
mod dedup;
mod drain;
mod endpoint;
mod flow;
#[cfg(any(unix, windows))]
mod grouped;
mod handoff;
mod identity;
mod inproc;
mod listener;
mod ordering;
#[cfg(windows)]
mod pipe;
mod pool;
mod pubsub;
mod radio;
mod reconnect;
mod report;
mod runtime;
mod segment;
mod stats;
mod stream;
mod tls;
mod transfer;
mod transport;
#[cfg(unix)]
mod unix;
// SWARM: many sources, one verified blob (decisions/0040).
#[cfg(feature = "swarm")]
mod swarm;

pub use config::{
    Alpn, ClientTls, ClientTrust, Discovery, Identity, Pem, RuntimeConfig, ServerTls, Trust,
};
pub use cursor::{CursorSet, Cursors, Reported, Reporter};
pub use drain::Drained;
pub use endpoint::{
    Bus, BusMember, Cast, Dish, Endpoint, Pair, Paired, Pattern, Pub, Publisher, Pull, Puller,
    Push, Pusher, Radio, Rep, Replier, Req, Requester, Respond, Respondent, Sub, Subscriber,
    Survey, SurveyRun, Surveyor, Tune,
};
#[cfg(feature = "swarm")]
pub use endpoint::{Gather, Seed, Seeder, Swarm};
pub use flow::{Flow, FlowInfo, FlowMeta, FlowStats, IncomingFlow};
pub use handoff::Handoff;
pub use identity::{
    FilesOptions, IdentityEvent, IdentityEvents, IdentitySource, PeerChain, TrustSource,
    WeakIdentitySource, WeakTrustSource,
};
#[cfg(windows)]
pub use listener::PipeBinding;
#[cfg(unix)]
pub use listener::UnixBinding;
pub use listener::{Binding, Listener, LocalBinding};
pub use ordering::Gap;
pub use pubsub::{DishDrops, FanOut, TopicDrops};
pub use radio::{Join, JoinTerms, Received};
pub use reconnect::{GiveUp, OutboxFull, PeerEvent, PeerEvents, ReconnectPolicy};
pub use runtime::Runtime;
pub use segment::{MAX_SEGMENT_LAYERS, Segment, SegmentTerms};
pub use stats::{ConnectionStats, PathStats, RemoteStats, TransportStats, UdpCounts};
pub use stream::{Acceptor, Consumer, ConsumerId, CreditGrant, Incoming, Peer};
#[cfg(feature = "swarm")]
pub use swarm::{
    Ask, Bits, Blob, BlobId, Fetch, FetchOptions, MAX_PENDING_SESSIONS, MAX_PIECES, MIN_RATE,
    ParseBlobIdError, Piece, PieceList, PieceSize, SEED_STALL, SeedSession, TreeHasher,
};
pub use transfer::{
    ConnectionId, Delivery, IncomingMeta, IncomingRequest, IncomingTransfer, OutgoingTransfer,
    ReplyStream, TransferMeta, new_trace,
};
pub use weida_core::{
    Address, Congestion, DEFAULT_DATAGRAM_RECEIVE_BYTES, DEFAULT_PORT, EndpointAddr, Error,
    ErrorCode, Fingerprint, InprocAddr, Limits, LocalPrincipal, LossCause, PeerIdentity, PipeAddr,
    Result, StopReason, TraceContext, UnixAddr, WindowsPrincipal,
};
pub use weida_protocol::{ALPN, RAFT_ALPN, VERSION, codes, filter};
pub use weida_runtime::{Resolved, Resolver, SharedResolver, SystemResolver};
// `Delivery` keeps its transfer-receipt meaning at this level, so the
// dimension of the same name is re-exported under the name the guarantee
// documents use for it.
pub use weida_protocol::header::{
    Acknowledgement, Backpressure, CursorLevel, Deduplication, Delivery as DeliveryLevel,
    Durability, GuaranteeSet, OrderingMode, ProducerNaming, ReportMode,
};
