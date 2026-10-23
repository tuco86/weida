//! ZMTP over one connection: the greeting, the NULL handshake, the framing
//! and the heartbeat.
//!
//! This is the [`Session`] the connection engine hands every established
//! connection to. It is the same code for every socket type, because the
//! pattern is a *property* in `READY` and not a different wire format
//! (`docs/research/zeromq.md` §3) — what differs per socket type is which
//! peer types are legal opposite it, and that table lives in the codec.
//!
//! **Nothing byte-exact is written here.** `weida-zmtp` owns the greeting,
//! the flags octet, the size field, the command bodies and the property
//! dictionary, and its `[dependencies]` is empty so that it can be checked
//! against 37/ZMTP rather than against our reading of it
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3). This
//! module is I/O, sequencing and policy: which command answers which, when a
//! `PING` may be sent, and what a mismatch does.
//!
//! **The 3.0 downgrade is load-bearing.** 37/ZMTP permits a peer to
//! "downgrade to a lower protocol version", and the interop bench showed why
//! it must: the pure-Rust `zeromq` crate announces ZMTP 3.0 and answers any
//! command but `READY` with "Unknown command received" and a close
//! (`docs/research/zeromq.md` §13). So a 3.0 peer is accepted, and it gets no
//! `PING` — the version decides whether the heartbeat can be honoured, and a
//! suppressed heartbeat says so once rather than leaving a silently different
//! behaviour to be found in a packet capture.
//!
//! Interop tests exercise this state machine against libzmq and the
//! pure-Rust `zeromq` crate in both roles, including the downgrade and
//! heartbeat cases above.

use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use weida_runtime::Exec;
use weida_zmtp::{
    Command, CommandError, FrameError, FrameKind, Greeting, GreetingError, Metadata, SocketType,
    Version, curve as curve_layout, frame, greeting,
};
use zeroize::Zeroize;

use crate::context::Context;
use crate::curve::{
    COOKIE_LIFETIME, CurveClient, CurvePublicKey, CurveSecretKey, CurveServer, CurveTransport,
};
use crate::engine::{Connection, LogGate, Role, Session, SessionFuture};
use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::message::{
    Assembly, Message, MessageLimits, Multipart, command_in_multipart, frame_error, header_octets,
};
use crate::options::{Security, SocketOptions};
use crate::pipe::{Queue, Sent};
use crate::subscriptions::{self, SubscriptionForm, Subscriptions};
use crate::zap::{self, ZapRequest, ZapUserId};

/// What the receive buffer grows by when a read would be offered less than
/// [`MIN_READ_SPARE`]. A frame header is at most nine octets and a command
/// body a few hundred, so the interesting case is a payload, which is read
/// straight into this buffer and handed on as a slice of it.
const READ_CHUNK: usize = 16 * 1024;

/// Spare room below which the receive buffer is grown before the next read,
/// so that a read is never offered a handful of octets.
const MIN_READ_SPARE: usize = 4 * 1024;

/// The largest frame the greeting and the security handshake accept, header
/// included, and the most a peer can make this side buffer before it has
/// authenticated.
///
/// The largest legitimate handshake command is a few hundred octets — a
/// CURVE `HELLO` is 200, a PLAIN `HELLO` at most 520, a `WELCOME` 168, and
/// `READY`/`INITIATE` carry a `Socket-Type`, an `Identity` of at most 255
/// octets and a handful of properties — so this is far above anything an
/// honest peer sends and far below `ZMQ_MAXMSGSIZE`, which an operator may
/// have raised for bulk traffic. A frame that is not a command, or declares
/// more than this, is refused at its header, before a body octet is read.
pub const HANDSHAKE_MAX_FRAME: u64 = 64 * 1024;

/// How long a peer may take over a message it has started before the octets
/// it has sent buy it more time ([`MIN_MESSAGE_RATE`]). Without it a peer
/// could announce a message and send a byte a minute, holding the connection
/// and its buffers for ever whenever the heartbeat is off, which is the
/// default.
pub const MESSAGE_IVL: Duration = Duration::from_secs(30);

/// The slowest rate, in octets per second, at which a message is expected to
/// keep arriving: every this many octets received extend the time a peer has
/// for the message by a second. A message that keeps up is never overdue,
/// however large, and one that crawls in more slowly — or that was announced
/// and not sent — ends the connection once [`MESSAGE_IVL`] and what it has
/// earned are spent. A modem's pace keeps up.
pub const MIN_MESSAGE_RATE: u64 = 4 * 1024;

/// Output waiting for a peer that is not draining it, above which no more is
/// taken from the pipe: the pipe's high-water marks are the bound, and a
/// second, unbounded buffer behind them would defeat it.
const OUT_HIGH_WATER: usize = 256 * 1024;

/// Messages written with one `write`, at most: what the pipe already holds is
/// gathered into the connection's output before it is written, as libzmq
/// batches. One syscall per message is a ceiling on small-message throughput.
const BATCH_MESSAGES: usize = 64;

/// Output at which a batch stops growing.
const BATCH_BYTES: usize = 64 * 1024;

/// Frames shorter than this are copied out of the receive buffer rather than
/// sliced off it: the copy is cheaper than the shared bookkeeping, and the
/// frame does not pin the buffer it arrived in.
const SLICE_MIN: usize = 512;

/// A larger frame is sliced off the receive buffer only when it is at least
/// one `SLICE_SHARE`th of the buffer's allocation, so that a queued frame
/// pins at most that many times its own size and a peer cannot turn a small
/// accounted message into a large resident one.
const SLICE_SHARE: usize = 4;

/// The receive and send buffers keep at most this much capacity while they
/// are empty; a connection that once carried a large message does not hold
/// its high-water mark for the rest of its life.
const RETAINED_BUFFER: usize = 64 * 1024;

/// The floor under `ZMQ_HEARTBEAT_IVL`, so that a zero interval is a fast
/// heartbeat rather than a spin.
const MIN_HEARTBEAT_IVL: Duration = Duration::from_millis(1);

/// How often a refused subscription is logged, per connection. A subscriber
/// at its table ceiling can send a `SUBSCRIBE` per eleven octets, and a log
/// line per refusal would turn each of them into a hundred octets of log.
const REFUSAL_LOG_PERIOD: Duration = Duration::from_secs(1);

/// The context this library puts in its `PING`s. Opaque by specification —
/// the `PONG` echoes it and nothing interprets it.
const PING_CONTEXT: &[u8] = b"weida-zmq";

/// The ZMTP session every socket type of this crate uses.
///
/// Constructed with the socket type it announces, because that is the one
/// thing about the handshake that differs per pattern: `Socket-Type` in our
/// `READY`, and the compatibility check against the peer's.
///
/// Run against a *socket's* pipe and never against another session of this
/// crate: a pipe is oriented from the socket's side, so pairing two sessions
/// needs two pipes with crossed halves and [`crate::pipe::Pipe`] has no
/// constructor that builds one. The diagnosis is on that type and on the
/// `drive` this delegates to; a harness that wants the pairing has
/// `Pipe::crossed` under `cfg(test)`.
#[derive(Clone, Debug)]
pub struct ZmtpSession {
    socket_type: SocketType,
    /// The socket's **own** subscriptions, for a SUB or XSUB: what it asks
    /// every publisher for, and what it re-sends on every reconnect, since a
    /// reconnect runs a new session and a new handshake.
    mine: Option<Arc<Subscriptions>>,
}

impl ZmtpSession {
    /// A session that announces `socket_type`.
    pub fn new(socket_type: SocketType) -> ZmtpSession {
        ZmtpSession {
            socket_type,
            mine: None,
        }
    }

    /// A session for a subscribing socket type, carrying the set it must
    /// ask every publisher for.
    ///
    /// This is where "re-sends them on reconnect" comes from: a reconnect is
    /// a new session, and a new session sends the set it finds — so the
    /// socket never has to notice that a connection was replaced.
    pub fn subscribing(socket_type: SocketType, mine: Arc<Subscriptions>) -> ZmtpSession {
        ZmtpSession {
            socket_type,
            mine: Some(mine),
        }
    }

    /// The socket type this session announces.
    pub const fn socket_type(&self) -> SocketType {
        self.socket_type
    }
}

impl Session for ZmtpSession {
    fn run(&self, connection: Connection) -> SessionFuture {
        let ours = self.socket_type;
        let mine = self.mine.clone();
        Box::pin(async move { drive(ours, mine, connection).await })
    }
}

/// What the handshake agreed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Negotiated {
    /// The peer's socket type, from its `READY`.
    pub peer_type: SocketType,
    /// The version to speak: ours, or the peer's if it is lower and still
    /// 3.x. `PING`/`PONG` are gated on it.
    pub version: Version,
    /// The peer's `Identity` property, if it announced one. Self-asserted —
    /// see [`RoutingId`] — and what a ROUTER keys its queue by, which is that
    /// socket type's concern rather than this one's.
    pub identity: Option<RoutingId>,
    /// The user id a ZAP handler returned with its 200, where this side
    /// authorized the connection.
    ///
    /// A per-connection fact and not an identity: see [`ZapUserId`]. `None`
    /// when nothing was authorized, and `None` for a handler that allowed the
    /// connection without naming a user.
    pub user_id: Option<ZapUserId>,
}

/// Runs one connection: handshake, then pump until it ends.
///
/// **The pipe is the socket's end, not the peer's**, and that is what defeats
/// a harness of two sessions driven against each other over a TCP pair. This
/// pumps *against* the pipe's orientation — it pops [`crate::pipe::Pipe`]'s
/// `outgoing` onto the wire and pushes what it reads into `incoming` — so two
/// sessions can only be paired through two pipes whose halves are crossed,
/// one's `outgoing` being the other's `incoming`. `Pipe::new` is the only
/// public constructor and builds both queues itself, so that crossing cannot
/// be expressed through the public API; handing both sessions the same pipe,
/// the obvious substitute, makes them pop the same `outgoing` queue and push
/// the same `incoming` one, and a message is then raced by both readers and
/// never arrives where the test looks. Nothing here is broken by it — the
/// engine pairs a session with a *socket*, one pipe per peer with the socket
/// holding the other end, which is why every socket-level test over TCP
/// delivers, `tests/plain_zap.rs` included. A harness that wants the pairing
/// anyway takes `Pipe::crossed`, which exists under `cfg(test)`.
async fn drive(
    ours: SocketType,
    mine: Option<Arc<Subscriptions>>,
    connection: Connection,
) -> Result<()> {
    let Connection {
        stream,
        pipe,
        peer,
        endpoint,
        options,
        exec,
        mut handshake,
        identity,
        subscriptions,
        role,
        context,
        peer_address,
        credentials,
        user,
    } = connection;

    let mut wire = Wire::new(stream, options.message_limits());
    let facts = PeerFacts {
        role,
        address: peer_address,
        credentials,
        context,
    };
    let negotiated = handshake_on(&mut wire, ours, &options, &facts).await?;
    // What the handler said about this connection, where the socket can read
    // it: a per-connection fact held by the server.
    user.set(negotiated.user_id.clone());
    // The engine's ZMQ_HANDSHAKE_IVL stops counting here.
    handshake.complete();
    // A ROUTER addresses this peer by what it just announced — and needs to
    // know that the handshake has happened at all, because a peer exists
    // before its READY is read.
    identity.announce(negotiated.identity.clone());
    tracing::debug!(
        %peer,
        endpoint = %endpoint,
        ours = ours.as_str(),
        theirs = negotiated.peer_type.as_str(),
        version = %negotiated.version,
        identity = ?negotiated.identity,
        "ZMTP handshake complete"
    );

    if let Some(mine) = &mine {
        // A subscribing socket asks for everything it holds, now — which is
        // also what makes a reconnect re-subscribe, because this runs again,
        // and it asks for each subscription as many times as it holds it,
        // because 29/PUBSUB counts rather than sets.
        //
        // What the pipe already holds is *also* in that table: a socket
        // updates its own table before it queues the announcement, so a
        // subscription made between `connect` and this handshake would
        // otherwise reach the publisher twice. Counting makes that a
        // divergence and not a nuisance — the publisher would need two
        // `CANCEL`s for a subscription the application holds once, so the
        // first `unsubscribe` would change nothing and an XPUB would report
        // nothing. The queued copies go; the table is the truth. Anything
        // else an XSUB queued is upstream data and is kept, in order.
        let queued = pipe.outgoing();
        let mut carried = Vec::new();
        while let Ok(message) = queued.try_recv() {
            if subscribing_form(ours, &message).is_none() {
                carried.push(message);
            }
        }
        for (prefix, count) in mine.counted() {
            for _ in 0..count {
                queue_subscription(&mut wire, options.subscription_form, true, &prefix)?;
            }
        }
        for message in carried {
            wire.queue_message(&message)?;
        }
        wire.flush().await?;
    }

    if ours == SocketType::XPub
        && let Some(welcome) = &options.xpub_welcome_msg
    {
        // ZMQ_XPUB_WELCOME_MSG: "sent on connect and reconnect", so it is
        // sent here — a session is exactly one connection.
        wire.write_message(&Multipart::single(welcome.clone()))
            .await?;
    }

    if options.probe_router {
        // ZMQ_PROBE_ROUTER: "send an empty message on every new connection",
        // so the peer's ROUTER learns this peer exists before it has
        // anything to say. Sent here rather than by the socket, because this
        // is where "a new connection" happens.
        wire.write_message(&Multipart::single(Message::empty()))
            .await?;
    }

    let outcome = pump(
        &mut wire,
        ours,
        &subscriptions,
        &pipe,
        &options,
        &exec,
        negotiated.version,
    )
    .await;
    // "Session keys are held in memory and destroyed when the connection is
    // closed" — this is that close, and it happens on the way out of every
    // ending, not only the clean one.
    wire.destroy_session_keys();
    outcome
}

/// What the handshake knows about the connection besides its bytes.
///
/// Gathered rather than passed as five arguments, and each field is here
/// because the security handshake needs it: the role decides who is the
/// server under NULL, the address and the kernel credentials are what a ZAP
/// handler is told about the peer, and the context is where the handler
/// lives.
#[derive(Clone, Debug)]
pub struct PeerFacts {
    /// Which side dialled. Under NULL "the peer that binds SHALL be the
    /// server"; under PLAIN the `as-server` octet decides instead.
    pub role: Role,
    /// The peer's address for a ZAP request: an IP for `tcp://`, empty for
    /// every local transport, which is what 27/ZAP's `address` frame can
    /// carry.
    pub address: String,
    /// The kernel's statement about a local peer, offered to the handler as
    /// the extension frame [`crate::zap`] documents.
    pub credentials: Option<weida_core::LocalPrincipal>,
    /// The context whose `inproc://` namespace holds the ZAP handler.
    pub context: Context,
}

/// Drives the greeting and the security handshake, whichever mechanism the
/// options select.
///
/// The order is the specification's: send the full greeting, read the peer's,
/// then the mechanism's own exchange — `READY` both ways for NULL,
/// `HELLO`/`WELCOME`/`INITIATE`/`READY` for PLAIN (24/ZMTP-PLAIN). Two
/// refusals are shared by both, and both send an `ERROR` before the close,
/// which is what 37/ZMTP asks for: a socket type that may not talk to `ours`,
/// and a handshake that names no socket type at all — the second is a
/// `SHOULD` in the specification and a MUST for an implementation, which
/// cannot check compatibility against a peer that will not say what it is.
///
/// **Authorization happens here, before this returns**, so no message can
/// flow under a refused connection: a ZAP status other than 200 ends the
/// handshake with an `ERROR` carrying the handler's text.
pub async fn handshake_on<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    ours: SocketType,
    options: &SocketOptions,
    facts: &PeerFacts,
) -> Result<Negotiated> {
    let security = options.security();
    let greeting = Greeting {
        mechanism: security.mechanism(),
        as_server: security.as_server(),
        ..Greeting::null()
    };
    wire.write_raw(&greeting.encode()).await?;

    let mut theirs = [0u8; greeting::GREETING_LEN];
    wire.read_exactly(&mut theirs).await?;
    let peer = Greeting::decode(&theirs).map_err(greeting_error)?;
    let version = peer
        .accept_downgrading(security.mechanism())
        .map_err(greeting_error)?;
    if security.as_server() && peer.as_server {
        // Two servers on one connection is a configuration mistake that
        // would otherwise deadlock, both sides waiting for a `HELLO`
        // neither will send.
        //
        // **Only that half is checkable.** 24/ZMTP-PLAIN and 25/ZMTP-CURVE
        // say the octet "SHALL be 1 for a server", but libzmq 4.3.5 sends
        // **0** from a socket with `ZMQ_PLAIN_SERVER` or
        // `ZMQ_CURVE_SERVER` set — measured in
        // `tests/interop_libzmq.rs`, both mechanisms, with the octets read
        // off the wire. It never reads the peer's octet either: the role is
        // decided locally by the options on each side, so nobody noticed.
        // A zero therefore proves nothing, and refusing on it refuses the
        // reference implementation. Two clients are left to
        // `ZMQ_HANDSHAKE_IVL`, which is what bounds every other handshake
        // that goes quiet.
        return Err(Error::ENOCOMPATPROTO(
            format!(
                "both ends of this connection are the {} server",
                security.mechanism()
            )
            .into(),
        ));
    }

    let ours_metadata = handshake_metadata(ours, options);
    let mut user_id = None;
    let peer_metadata = match security {
        Security::Null => {
            wire.write_command(&Command::Ready(ours_metadata)).await?;
            PeerMetadata::Command(wire.read_command("READY").await?)
        }
        Security::PlainClient => {
            // `C:HELLO(user,pass) -> S:WELCOME|S:ERROR`, then
            // `C:INITIATE(metadata) -> S:READY|S:ERROR`.
            wire.write_command(&Command::Hello {
                username: options.plain_username.as_deref().unwrap_or("").as_bytes(),
                password: options.plain_password.as_deref().unwrap_or("").as_bytes(),
            })
            .await?;
            let body = wire.read_command("WELCOME").await?;
            match Command::decode(&body).map_err(command_error)? {
                Command::Welcome => {}
                other => {
                    return Err(Error::ENOCOMPATPROTO(
                        format!("expected WELCOME, got {}", other.name()).into(),
                    ));
                }
            }
            wire.write_command(&Command::Initiate(ours_metadata))
                .await?;
            PeerMetadata::Command(wire.read_command("READY").await?)
        }
        Security::PlainServer => {
            let body = wire.read_command("HELLO").await?;
            let (username, password) = match Command::decode(&body).map_err(command_error)? {
                Command::Hello { username, password } => (username.to_vec(), password.to_vec()),
                other => {
                    return Err(Error::ENOCOMPATPROTO(
                        format!("expected HELLO, got {}", other.name()).into(),
                    ));
                }
            };
            // The credentials are checked by the handler, not here: a
            // username this library judged itself would be a policy nobody
            // configured.
            user_id = authorize(
                wire,
                options,
                facts,
                "PLAIN",
                vec![username, password],
                Vec::new(),
            )
            .await?;
            wire.write_command(&Command::Welcome).await?;
            let peer_metadata = wire.read_command("INITIATE").await?;
            wire.write_command(&Command::Ready(ours_metadata)).await?;
            PeerMetadata::Command(peer_metadata)
        }
        Security::CurveClient => {
            // 26/CURVEZMQ: `C:HELLO -> S:WELCOME`, `C:INITIATE -> S:READY`,
            // and then nothing in clear text ever again.
            let mut client = CurveClient::new(
                curve_key(options.curve_publickey, "ZMQ_CURVE_PUBLICKEY")?,
                curve_secret(options)?,
                curve_key(options.curve_serverkey, "ZMQ_CURVE_SERVERKEY")?,
            );
            let hello = client.hello()?;
            wire.write_handshake(&hello).await?;
            let body = wire.read_command("WELCOME").await?;
            refused(&body)?;
            client.read_welcome(&body)?;
            let initiate = client.initiate(&ours_metadata)?;
            wire.write_handshake(&initiate).await?;
            let body = wire.read_command("READY").await?;
            refused(&body)?;
            let metadata = client.read_ready(&body)?;
            // Everything after this point is a MESSAGE box.
            wire.encrypt_with(client.into_transport()?);
            PeerMetadata::Dictionary(metadata)
        }
        Security::CurveServer => {
            let mut server = CurveServer::new(curve_secret(options)?, COOKIE_LIFETIME);
            let body = wire.read_command("HELLO").await?;
            refused(&body)?;
            if let Err(e) = server.read_hello(&body) {
                // A HELLO that does not open is a peer that does not know
                // this server's key: it gets an ERROR rather than silence,
                // which is what 37/ZMTP asks for and what a misconfigured
                // client needs to see.
                wire.refuse("the HELLO signature box does not open").await;
                return Err(e);
            }
            let welcome = server.welcome()?;
            wire.write_handshake(&welcome).await?;
            let body = wire.read_command("INITIATE").await?;
            refused(&body)?;
            let opened = match server.read_initiate(&body) {
                Ok(opened) => opened,
                Err(e) => {
                    wire.refuse("the INITIATE was refused").await;
                    return Err(e);
                }
            };
            // Whether the client's key goes to a handler: always when one is
            // bound, and under a domain even when none is, because then the
            // refusal is the answer. Without either, "the server does not
            // check client keys at all" and knowing `S` was the
            // authorization (26/CURVEZMQ's first model). With one, the
            // credential is `C` — "a 32-byte long-term public key of the peer
            // being authenticated", the key that never travelled in clear
            // text — and the identity frame is the peer's own `Identity`
            // property, out of the same box.
            if zap::asks_handler(options, &facts.context) {
                let identity = opened
                    .metadata()?
                    .get("Identity")
                    .map(<[u8]>::to_vec)
                    .unwrap_or_default();
                user_id = authorize(
                    wire,
                    options,
                    facts,
                    "CURVE",
                    vec![opened.client_key.as_bytes().to_vec()],
                    identity,
                )
                .await?;
            }
            let ready = server.ready(&ours_metadata)?;
            wire.write_handshake(&ready).await?;
            let metadata = opened.metadata_bytes().to_vec();
            wire.encrypt_with(server.into_transport()?);
            PeerMetadata::Dictionary(metadata)
        }
    };

    let metadata = match &peer_metadata {
        PeerMetadata::Command(body) => match Command::decode(body).map_err(command_error)? {
            Command::Ready(metadata) | Command::Initiate(metadata) => metadata,
            // "The peer SHALL treat an incoming ERROR command as fatal."
            Command::Error(reason) => {
                return Err(Error::ENOCOMPATPROTO(
                    format!("the peer refused the handshake: {}", sanitize(reason)).into(),
                ));
            }
            other => {
                return Err(Error::ENOCOMPATPROTO(
                    format!("expected the peer's metadata, got {}", other.name()).into(),
                ));
            }
        },
        // CURVE's metadata came out of a box rather than out of a command:
        // the property dictionary is all that was in there.
        PeerMetadata::Dictionary(bytes) => Metadata::decode(bytes).map_err(command_error)?,
    };

    let identity = match metadata.get("Identity") {
        // An **empty** `Identity` is the absence of one, not a malformed
        // one: 37/ZMTP's grammar is `identity = 0*255OCTET` and only a
        // non-empty identity must not begin with a zero octet. libzmq sends
        // the property with an empty value for every REQ, DEALER and ROUTER
        // socket that has no `ZMQ_ROUTING_ID` set, so refusing it refuses
        // the reference implementation — measured against libzmq 4.3.5 in
        // `tests/interop_libzmq.rs`.
        Some(bytes) if !bytes.is_empty() => Some(RoutingId::new(bytes)?),
        _ => None,
    };

    let Some(peer_type) = metadata.socket_type() else {
        wire.refuse("READY carries no Socket-Type").await;
        return Err(Error::ENOCOMPATPROTO(
            "the peer announced no socket type, so no compatibility check is possible".into(),
        ));
    };
    if !ours.accepts(peer_type) {
        wire.refuse(&format!(
            "{} may not talk to {}",
            peer_type.as_str(),
            ours.as_str()
        ))
        .await;
        return Err(Error::ENOCOMPATPROTO(
            format!(
                "a {} peer may not talk to this {}",
                peer_type.as_str(),
                ours.as_str()
            )
            .into(),
        ));
    }

    if security == Security::Null && facts.role == Role::Binder && options.authorizes() {
        // NULL "provides no security credentials but allows a server to
        // filter bogus clients on the basis of IP address", and the identity
        // the peer just announced is the other thing 27/ZAP hands over. Run
        // after the metadata so that both are known, and before this returns,
        // which is before any message may flow.
        user_id = authorize(
            wire,
            options,
            facts,
            "NULL",
            Vec::new(),
            identity
                .as_ref()
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
        )
        .await?;
    }

    Ok(Negotiated {
        peer_type,
        version,
        identity,
        user_id,
    })
}

/// Where the peer's metadata dictionary came from.
///
/// NULL and PLAIN carry it in a command — `READY` or `INITIATE` — and CURVE
/// carries it inside a box, where the box holds the property dictionary and
/// nothing else. Both shapes are held as owned octets so that the
/// [`Metadata`] borrowed from them outlives the handshake.
enum PeerMetadata {
    /// A `READY` or `INITIATE` command body.
    Command(Bytes),
    /// The property dictionary out of an `INITIATE` or `READY` box.
    Dictionary(Vec<u8>),
}

/// Reports a peer that answered a handshake command with `ERROR`.
///
/// Every mechanism may: 26/CURVEZMQ has `ERROR` in its own grammar, and
/// 37/ZMTP calls an incoming one fatal. A CURVE command can never be
/// mistaken for one, because no CURVE command is named `ERROR`.
fn refused(body: &[u8]) -> Result<()> {
    // Only an `ERROR` is looked at: a handshake body is decoded as the
    // command it is in the one place that acts on it, and a CURVE body that
    // is not an `ERROR` is not a NULL-style command to be parsed here.
    if command_name(body) != Some(b"ERROR") {
        return Ok(());
    }
    match Command::decode(body) {
        Ok(Command::Error(reason)) => Err(Error::ENOCOMPATPROTO(
            format!("the peer refused the handshake: {}", sanitize(reason)).into(),
        )),
        _ => Ok(()),
    }
}

/// The name a command body announces: its length-prefixed name, or `None`
/// when the body does not hold one. Nothing past the name is looked at.
fn command_name(body: &[u8]) -> Option<&[u8]> {
    let len = usize::from(*body.first()?);
    body.get(1..1 + len)
}

/// A CURVE key an option must carry, or `EINVAL` naming the option.
///
/// [`SocketOptions::validate`] refuses the missing ones at configuration
/// time, so this is the same rule stated where the value is finally read
/// rather than a second policy.
fn curve_key(key: Option<CurvePublicKey>, option: &str) -> Result<CurvePublicKey> {
    key.ok_or_else(|| {
        Error::EINVAL(format!("this CURVE socket has no {option}, so it cannot handshake").into())
    })
}

/// This socket's long-term secret key, which both CURVE roles need.
fn curve_secret(options: &SocketOptions) -> Result<&CurveSecretKey> {
    options.curve_secretkey.as_ref().ok_or_else(|| {
        Error::EINVAL(
            "this CURVE socket has no ZMQ_CURVE_SECRETKEY, so it holds no key at all".into(),
        )
    })
}

/// This socket's own metadata: the socket type it announces and, where it has
/// one, the routing id a ROUTER peer should address it by.
fn handshake_metadata(ours: SocketType, options: &SocketOptions) -> Metadata<'_> {
    let mut metadata = Metadata::new().with_socket_type(ours);
    if let Some(routing_id) = &options.routing_id {
        // libzmq deprecates the name `ZMQ_IDENTITY` for `ZMQ_ROUTING_ID`, but
        // the wire property is still `Identity` and a peer that reads the
        // other name would not find it.
        metadata = metadata.with("Identity", routing_id.as_bytes());
    }
    metadata
}

/// Asks the ZAP handler about this connection, and refuses the connection
/// with an `ERROR` when the answer is not 200.
///
/// Returns the user id of a 200, which is a per-connection fact and never an
/// identity ([`crate::ZapUserId`]).
async fn authorize<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    options: &SocketOptions,
    facts: &PeerFacts,
    mechanism: &str,
    credentials: Vec<Vec<u8>>,
    identity: Vec<u8>,
) -> Result<Option<ZapUserId>> {
    if credentials
        .iter()
        .any(|credential| zap::imitates_local_principal(credential))
    {
        // A PLAIN username and password are whatever octets the peer chose,
        // and a handler written against the extension frame's convention
        // would read one that starts like it as the kernel's statement about
        // a local peer. No honest peer has such a credential, so nobody is
        // asked: the connection is refused here.
        wire.refuse("400 a credential imitates the local principal extension")
            .await;
        return Err(Error::EACCES(
            format!(
                "a {mechanism} credential begins like the X-Local-Principal extension frame, \
                 which only this library may send; the connection is refused"
            )
            .into(),
        ));
    }
    let request = ZapRequest {
        request_id: next_request_id(),
        domain: options.zap_domain.clone(),
        address: facts.address.clone(),
        identity,
        mechanism: mechanism.to_owned(),
        credentials,
        local_principal: facts.credentials,
    };
    let outcome = zap::authorize(&facts.context, &request).await;
    let reply = match outcome {
        Ok(reply) => reply,
        Err(e) => {
            // No handler, or a handler that malfunctioned: the connection is
            // refused, because a server that cannot ask must not admit.
            wire.refuse("authorization is unavailable").await;
            return Err(e);
        }
    };
    if !reply.status.is_allowed() {
        wire.refuse(&format!("{} {}", reply.status, reply.text))
            .await;
        return Err(Error::EACCES(
            format!(
                "the ZAP handler answered {} for this {mechanism} connection: {}",
                reply.status,
                if reply.text.is_empty() {
                    "no reason given"
                } else {
                    &reply.text
                }
            )
            .into(),
        ));
    }
    Ok(reply.user_id)
}

/// A request id unique within this process, which is all 27/ZAP needs: the
/// reply is read on the socket that asked, and the id only has to catch a
/// handler answering the wrong question.
fn next_request_id() -> Vec<u8> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
        .to_string()
        .into_bytes()
}

/// Moves messages both ways until the connection or the pipe ends.
///
/// Four things happen in one loop, and they are one loop on purpose: the wire
/// (reading, and writing what has been queued for it), draining this peer's
/// outgoing queue into that output, the `ERROR`s the application asks this
/// connection to carry, and the timers. A separate reader and writer task
/// would need a lock over the same socket and would not make either
/// direction faster.
///
/// **Nothing waits on the peer while the rest stands still.** A write is an
/// arm of this loop, not an await inside another arm's body, so a peer that
/// stops reading cannot stop the timers or this side's reading: with the
/// heartbeat on, the connection ends by `ETIMEDOUT` instead of hanging on a
/// full socket buffer for good.
///
/// **Backpressure is the receive queue not being drained.** When this peer's
/// incoming queue is full and its socket type blocks, the `send` below waits,
/// which stops this loop reading — which is how a PULL slows a PUSH down.
/// When the socket type drops instead (SUB, XSUB per 29/PUBSUB), the message
/// is discarded and counted, and reading continues.
async fn pump<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    ours: SocketType,
    subscriptions: &Arc<Subscriptions>,
    pipe: &crate::pipe::Pipe,
    options: &SocketOptions,
    exec: &Exec,
    version: Version,
) -> Result<()> {
    let outgoing: Arc<Queue> = pipe.outgoing();
    let incoming: Arc<Queue> = pipe.incoming();
    let refusals = LogGate::new(REFUSAL_LOG_PERIOD);
    let mut liveness = Liveness::new(options, version, exec, wire);

    loop {
        // Not while the peer is sitting on what is already queued.
        let room = wire.pending() < OUT_HIGH_WATER;
        tokio::select! {
            event = wire.next_event() => match event? {
                Event::Received(Incoming::Message(message)) => {
                    if publishes(ours) {
                        // A publisher never hands an inbound message to
                        // its application: "PUB SHALL silently discard
                        // any messages that subscribers send it". The one
                        // thing such a message may be is a subscription
                        // in ZMTP 2.0's form, which is how a 3.0 peer
                        // asks.
                        apply_message_form(
                            ours,
                            subscriptions,
                            &incoming,
                            options,
                            &refusals,
                            message,
                        )
                        .await?;
                    } else if incoming.send(message).await? == Sent::Dropped {
                        tracing::trace!(
                            "dropped an inbound message: the queue is at its high-water mark"
                        );
                    }
                }
                Event::Received(Incoming::Command(body)) => {
                    answer(wire, ours, subscriptions, &incoming, options, &refusals, &body)
                        .await?;
                }
                Event::Wrote => {}
            },
            queued = outgoing.recv(), if room => match queued {
                Ok(message) => {
                    dispatch(wire, ours, options, &message)?;
                    // What the pipe already holds goes out in the same
                    // write.
                    let mut taken = 1;
                    while taken < BATCH_MESSAGES && wire.pending() < BATCH_BYTES {
                        let Ok(next) = outgoing.try_recv() else { break };
                        dispatch(wire, ours, options, &next)?;
                        taken += 1;
                    }
                }
                // The pipe was destroyed: the socket closed or disconnected
                // this endpoint, and this connection has nothing left to do.
                Err(_) => return Ok(()),
            },
            // An `ERROR` the application asked this connection to carry:
            // 37/ZMTP's only per-connection error channel, and the one thing
            // an adapter can say to a peer whose request it refuses. The
            // connection is not ended here — "the peer SHALL treat an
            // incoming ERROR command as fatal" is the *peer's* rule, and
            // whether it closes is its choice.
            reason = pipe.refusal() => {
                wire.queue_command(&Command::Error(&reason))?;
            }
            () = liveness.expired() => liveness.fire(wire)?,
        }
    }
}

/// Puts one message from the pipe into the connection's output: a
/// subscribing socket's own subscriptions in the form this socket sends
/// them, anything else as the message it is.
fn dispatch<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    ours: SocketType,
    options: &SocketOptions,
    message: &Multipart,
) -> Result<()> {
    // A subscribing socket's outgoing messages *are* its subscriptions, in
    // the `%x01`/`%x00` form the API uses; which form goes on the wire is
    // this socket's choice.
    match subscribing_form(ours, message) {
        Some((subscribe, prefix)) => {
            queue_subscription(wire, options.subscription_form, subscribe, &prefix)
        }
        None => wire.queue_message(message),
    }
}

/// Answers a command.
///
/// `PING` gets its `PONG` echoing the context and `ERROR` is fatal, for
/// every socket type. `SUBSCRIBE`/`CANCEL` belong to a publisher, so they
/// are applied for PUB and XPUB and ignored everywhere else — ignoring a
/// subscription on a REQ or a PUSH is not a loss, it is the only correct
/// answer. Anything else is noted and ignored, which is what a peer must do
/// with a command it has no use for.
///
/// `READY` and `INITIATE` are the handshake's, and once it is over nothing
/// uses them, so they are dropped **without being parsed**: parsing a
/// property dictionary only to ignore it is work a peer could ask for as
/// often as it likes.
async fn answer<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    ours: SocketType,
    subscriptions: &Arc<Subscriptions>,
    incoming: &Arc<Queue>,
    options: &SocketOptions,
    refusals: &LogGate,
    body: &[u8],
) -> Result<()> {
    if matches!(command_name(body), Some(b"READY" | b"INITIATE")) {
        tracing::debug!("ignoring a handshake command that arrived after the handshake");
        return Ok(());
    }
    match Command::decode(body).map_err(command_error)? {
        Command::Subscribe(prefix) if publishes(ours) => {
            apply_subscription(
                ours,
                subscriptions,
                incoming,
                options,
                refusals,
                true,
                prefix,
            )
            .await
        }
        Command::Cancel(prefix) if publishes(ours) => {
            apply_subscription(
                ours,
                subscriptions,
                incoming,
                options,
                refusals,
                false,
                prefix,
            )
            .await
        }
        Command::Ping { context, .. } => {
            // "When a peer receives a PING command it SHALL respond with a
            // PONG command that echoes the ping-context."
            wire.queue_command(&Command::Pong { context })
        }
        Command::Error(reason) => Err(Error::ENOCOMPATPROTO(
            format!("the peer sent ERROR: {}", sanitize(reason)).into(),
        )),
        other => {
            tracing::debug!(
                command = other.name(),
                "ignoring a command this socket has no use for"
            );
            Ok(())
        }
    }
}

/// Whether this socket type keeps a subscription table for its peers.
const fn publishes(ours: SocketType) -> bool {
    matches!(ours, SocketType::Pub | SocketType::XPub)
}

/// Whether this socket type's outgoing messages are subscriptions.
///
/// SUB can send nothing else at all; XSUB can send messages upstream too, and
/// `zmq_socket(3)` gives it the same `%x01`/`%x00` convention for the
/// subscriptions among them.
fn subscribing_form(ours: SocketType, message: &Multipart) -> Option<(bool, Vec<u8>)> {
    if !matches!(ours, SocketType::Sub | SocketType::XSub) {
        return None;
    }
    if message.len() != 1 {
        return None;
    }
    let (subscribe, prefix) = subscriptions::read_message_form(message.frames()[0].as_slice())?;
    Some((subscribe, prefix.to_vec()))
}

/// Queues one subscription in the configured form.
fn queue_subscription<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    form: SubscriptionForm,
    subscribe: bool,
    prefix: &[u8],
) -> Result<()> {
    match form {
        SubscriptionForm::Commands => {
            let command = if subscribe {
                Command::Subscribe(prefix)
            } else {
                Command::Cancel(prefix)
            };
            wire.queue_command(&command)
        }
        SubscriptionForm::LegacyMessage => {
            let frame = subscriptions::write_message_form(subscribe, prefix);
            wire.queue_message(&Multipart::single(frame))
        }
    }
}

/// Applies a subscription a peer sent, and — for an XPUB — hands it to the
/// application.
async fn apply_subscription(
    ours: SocketType,
    subscriptions: &Arc<Subscriptions>,
    incoming: &Arc<Queue>,
    options: &SocketOptions,
    refusals: &LogGate,
    subscribe: bool,
    prefix: &[u8],
) -> Result<()> {
    // ZMQ_XPUB_MANUAL: the application decides what this socket matches, so
    // the subscription is reported and *not* applied. A broker that
    // authorizes subscriptions needs exactly that.
    let manual = ours == SocketType::XPub && options.xpub_manual;
    let changed = if manual {
        true
    } else if subscribe {
        match subscriptions.subscribe(prefix) {
            Some(first) => first,
            None => {
                if let Some(suppressed) = refusals.permit() {
                    tracing::warn!(
                        held = subscriptions.len(),
                        suppressed,
                        "refused a subscription: this peer is at its table ceiling"
                    );
                }
                return Ok(());
            }
        }
    } else {
        subscriptions.cancel(prefix).unwrap_or(false)
    };

    if ours == SocketType::XPub {
        // XPUB hands subscriptions to its application in the `%x01`/`%x00`
        // form. Which ones: the first for a prefix by default — 29/PUBSUB's
        // normalization "so that multiple identical subscriptions result in
        // a single command only" — every subscribe under ZMQ_XPUB_VERBOSE,
        // and every subscribe *and* unsubscribe under ZMQ_XPUB_VERBOSER.
        let deliver = if options.xpub_verboser {
            true
        } else if options.xpub_verbose {
            subscribe || changed
        } else {
            changed
        };
        if deliver {
            // Waits for room rather than dropping: a notification is how a
            // proxy learns what to forward upstream, so a lost one is a topic
            // its subscribers never receive. The wait is this connection's
            // own — its reads stop until the application drains, which is the
            // back-pressure a full queue is everywhere else — and it ends
            // with `EHOSTUNREACH` when the pipe is destroyed, or is dropped
            // with the session.
            let frame = subscriptions::write_message_form(subscribe, prefix);
            match incoming.send(Multipart::single(frame)).await {
                Ok(Sent::Queued) => {}
                // Only a receive queue configured to drop or to refuse at
                // its mark gets here; `XPubSocket` leaves it blocking.
                Ok(Sent::Dropped) | Err(Error::EAGAIN(_)) => {
                    tracing::debug!(
                        "dropped a subscription notification: the receive queue is set not to wait"
                    );
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

/// Reads a message a publisher received as ZMTP 2.0's subscription form, or
/// discards it.
async fn apply_message_form(
    ours: SocketType,
    subscriptions: &Arc<Subscriptions>,
    incoming: &Arc<Queue>,
    options: &SocketOptions,
    refusals: &LogGate,
    message: Multipart,
) -> Result<()> {
    if message.len() == 1
        && let Some((subscribe, prefix)) =
            subscriptions::read_message_form(message.frames()[0].as_slice())
    {
        return apply_subscription(
            ours,
            subscriptions,
            incoming,
            options,
            refusals,
            subscribe,
            prefix,
        )
        .await;
    }
    if ours == SocketType::XPub {
        // "XPUB: as PUB plus … inbound messages fair-queued to the
        // application", and "messages without a sub/unsub prefix are also
        // received, but have no effect on subscription status" — which is how
        // an XSUB sends upstream through a proxy.
        if incoming.send(message).await? == Sent::Dropped {
            tracing::trace!("dropped an inbound message: the queue is at its high-water mark");
        }
        return Ok(());
    }
    // "PUB SHALL silently discard any messages that subscribers send it."
    tracing::debug!(
        frames = message.len(),
        "a publisher discarded a message a subscriber sent it"
    );
    Ok(())
}

/// The connection's timers: the heartbeat, and the watch on a message that
/// has been started and not finished.
///
/// **One timer, kept across iterations.** It is armed for the nearest of the
/// moments something must be looked at and re-armed only when it fires, so
/// neither this side's own traffic nor the peer's can keep pushing it out:
/// a timer rebuilt on every pass of the loop never fires on a connection
/// that is busy in either direction, which is exactly the connection whose
/// peer may have vanished.
///
/// **The heartbeat is libzmq's.** A `PING` every `ZMQ_HEARTBEAT_IVL`
/// whatever else is going on, and a connection is dead when nothing at all
/// has arrived for `ZMQ_HEARTBEAT_TIMEOUT` after a `PING` that was not
/// answered. "A peer SHOULD treat any incoming traffic (not just a PONG
/// reply) as a sign of life", and traffic is **octets**: a message that
/// takes longer than the timeout to arrive over a slow link is alive, not
/// silent.
///
/// `PING`/`PONG` are 3.1 commands, so a connection that negotiated 3.0 gets
/// no heartbeat at all whatever `ZMQ_HEARTBEAT_IVL` says. That is not
/// caution: sending a command a 3.0 peer does not know is a protocol
/// violation, and the interop tests show what it costs.
struct Liveness {
    interval: Option<Duration>,
    timeout: Duration,
    ttl_deciseconds: u16,
    message_ivl: Duration,
    next_ping: Instant,
    /// When the oldest `PING` that nothing has answered was queued.
    ping_sent: Option<Instant>,
    exec: Exec,
    sleep: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Liveness {
    fn new<S: AsyncRead + AsyncWrite + Unpin>(
        options: &SocketOptions,
        version: Version,
        exec: &Exec,
        wire: &Wire<S>,
    ) -> Liveness {
        let interval = match options.heartbeat_ivl {
            Some(interval) if version >= greeting::VERSION => Some(interval.max(MIN_HEARTBEAT_IVL)),
            Some(_) => {
                tracing::info!(
                    %version,
                    "the peer speaks ZMTP 3.0, which has no PING: the heartbeat is off for \
                     this connection and liveness is the transport's business"
                );
                None
            }
            None => None,
        };
        // ZMQ_HEARTBEAT_TIMEOUT of 0 means ZMQ_HEARTBEAT_IVL.
        let timeout = options
            .heartbeat_timeout
            .or(interval)
            .unwrap_or(Duration::ZERO);
        let ttl_deciseconds = options
            .heartbeat_ttl
            .map(|ttl| u16::try_from(ttl.as_millis() / 100).unwrap_or(u16::MAX))
            .unwrap_or(0);
        let now = Instant::now();
        let mut liveness = Liveness {
            interval,
            timeout,
            ttl_deciseconds,
            message_ivl: wire.message_ivl(),
            next_ping: now + interval.unwrap_or_default(),
            ping_sent: None,
            exec: exec.clone(),
            sleep: Box::pin(std::future::ready(())),
        };
        liveness.arm(now);
        liveness
    }

    /// Arms the timer for the nearest of: the next `PING`, the end of the
    /// silence a `PING` is waiting out, and — always — half a
    /// [`MESSAGE_IVL`] from now, which is how often a stalled message is
    /// looked for (a stall is found within one and a half intervals).
    fn arm(&mut self, now: Instant) {
        let mut due = now + self.message_ivl / 2;
        if self.interval.is_some() {
            due = due.min(self.next_ping);
        }
        if let Some(sent) = self.ping_sent
            && !self.timeout.is_zero()
        {
            due = due.min(sent + self.timeout);
        }
        self.sleep = Box::pin(self.exec.sleep(due.saturating_duration_since(now)));
    }

    /// Completes when the timer is due. Cancel-safe: the timer is state of
    /// this value, not of the future.
    async fn expired(&mut self) {
        (&mut self.sleep).await;
    }

    /// Looks at everything that was waiting for the timer: ends the
    /// connection for a peer that has gone quiet or a message that has
    /// stalled, queues the next `PING`, and arms the timer again.
    fn fire<S: AsyncRead + AsyncWrite + Unpin>(&mut self, wire: &mut Wire<S>) -> Result<()> {
        let now = Instant::now();
        // Anything that arrived since the `PING` was queued answers it.
        if let Some(sent) = self.ping_sent
            && wire.last_read() > sent
        {
            self.ping_sent = None;
        }
        if let Some(sent) = self.ping_sent
            && !self.timeout.is_zero()
            && now.saturating_duration_since(sent) >= self.timeout
        {
            return Err(Error::ETIMEDOUT(
                format!(
                    "nothing has arrived for {:?} since a PING went unanswered \
                     (ZMQ_HEARTBEAT_TIMEOUT)",
                    now.saturating_duration_since(sent)
                )
                .into(),
            ));
        }
        if let Some(interval) = self.interval
            && now >= self.next_ping
        {
            wire.queue_command(&Command::Ping {
                ttl: self.ttl_deciseconds,
                context: PING_CONTEXT,
            })?;
            self.next_ping = now + interval;
            if self.ping_sent.is_none() {
                self.ping_sent = Some(now);
            }
        }
        if let Some(overdue) = wire.message_deadline()
            && now >= overdue
        {
            return Err(Error::ETIMEDOUT(
                format!(
                    "the peer started a message and has not finished it within {:?} plus a \
                     second for every {MIN_MESSAGE_RATE} octets that arrived",
                    self.message_ivl
                )
                .into(),
            ));
        }
        self.arm(now);
        Ok(())
    }
}

/// What arrived on the connection.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    /// A whole message, every frame of it.
    Message(Multipart),
    /// A command frame's body, still encoded: `READY`, `PING`, `SUBSCRIBE`
    /// and the rest. "Commands always consist of one frame."
    Command(Bytes),
}

/// What [`Wire::next_event`] woke for.
enum Event {
    /// A whole message or command arrived.
    Received(Incoming),
    /// Queued output was written, or flushed.
    Wrote,
}

/// One frame taken off the receive buffer.
struct Frame {
    kind: FrameKind,
    body: Bytes,
    /// What the frame spends of its message's budget: its header and body.
    cost: u64,
}

/// ZMTP over a byte stream: the I/O the codec deliberately does not do.
///
/// Public because the socket types drive it, and because it is what makes the
/// session testable against a raw stream.
///
/// **Each octet is looked at once.** Frames are parsed off the front of one
/// receive buffer as they complete and moved into the message being
/// assembled; a frame that has not fully arrived costs one header parse per
/// read, never a re-walk of what came before it, so receiving is linear in
/// the octets received however slowly a peer sends them. A large frame is a
/// slice of the buffer it was read into, copied by nobody.
#[derive(Debug)]
pub struct Wire<S> {
    io: S,
    /// Received octets not yet parsed. Persisting across calls is half of
    /// what makes reading cancel-safe; the other half is that octets are
    /// appended only once a read has **completed** — see [`Wire::fill`].
    inbound: BytesMut,
    /// The size of the allocation `inbound` points into, which is what a frame
    /// sliced off it keeps alive. `BytesMut::capacity` is not that: it counts
    /// from the front of what has been taken, so a buffer a large frame was
    /// sliced off reports only the few kilobytes behind it. Set where the
    /// buffer is given an allocation ([`Wire::reserve_spare`]) and zero for
    /// none.
    allocated: usize,
    /// Output not yet written: whole frames, appended by the `queue_*`
    /// methods and written by [`Wire::write_some`], so that everything queued
    /// between two writes goes out in one.
    outbound: Vec<u8>,
    /// How much of `outbound` has been written.
    written: usize,
    /// Whether written octets have not been flushed yet.
    flush_due: bool,
    /// A command body being encoded, reused so that a `PING` costs no
    /// allocation.
    scratch: Vec<u8>,
    limits: MessageLimits,
    /// The frames of the message now arriving, held until its last one.
    assembly: Assembly,
    /// The CURVE session keys, once a handshake has agreed them. `None` is
    /// NULL and PLAIN, where the wire is what it says it is.
    curve: Option<CurveTransport>,
    /// Which of reading and writing goes first the next time both are ready.
    write_first: bool,
    /// When the last read completed: "any incoming traffic" for the
    /// heartbeat, measured in octets arriving rather than in messages
    /// finishing.
    last_read: Instant,
    /// When the first octet of the message now arriving was read, or `None`
    /// while nothing is half received.
    started: Option<Instant>,
    /// Octets read from the stream so far.
    received: u64,
    /// What `received` was when the message now arriving began, so that the
    /// octets it has brought are known.
    message_origin: u64,
    message_ivl: Duration,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Wire<S> {
    /// Wraps `io`, bounding every inbound message by `limits`:
    /// `ZMQ_MAXMSGSIZE` in octets — frame headers included — and a ceiling on
    /// the frame count. Both are needed; see [`Multipart::decode`].
    pub fn new(io: S, limits: MessageLimits) -> Wire<S> {
        Wire {
            io,
            inbound: BytesMut::new(),
            allocated: 0,
            outbound: Vec::new(),
            written: 0,
            flush_due: false,
            scratch: Vec::new(),
            limits,
            assembly: Assembly::default(),
            curve: None,
            write_first: false,
            last_read: Instant::now(),
            started: None,
            received: 0,
            message_origin: 0,
            message_ivl: MESSAGE_IVL,
        }
    }

    /// Overrides [`MESSAGE_IVL`], so that a test need not wait half a minute.
    #[cfg(test)]
    pub(crate) fn with_message_ivl(mut self, message_ivl: Duration) -> Wire<S> {
        self.message_ivl = message_ivl;
        self
    }

    /// Octets received and not yet parsed.
    #[cfg(test)]
    pub(crate) fn buffered(&self) -> usize {
        self.inbound.len()
    }

    /// The size of the allocation the receive buffer holds on to.
    #[cfg(test)]
    pub(crate) fn receive_capacity(&self) -> usize {
        self.allocated
    }

    /// Installs the CURVE session keys a handshake agreed, after which every
    /// frame in either direction travels inside a `MESSAGE` box.
    pub fn encrypt_with(&mut self, transport: CurveTransport) {
        self.curve = Some(transport);
    }

    /// Destroys the session keys: "Session keys are held in memory and
    /// destroyed when the connection is closed."
    ///
    /// What was opened in place in the receive buffer is plaintext and is
    /// overwritten too, with a write the optimizer may not remove. Frames
    /// already handed to the application are the application's.
    pub fn destroy_session_keys(&mut self) {
        if let Some(curve) = &mut self.curve {
            curve.destroy();
        }
        self.curve = None;
        self.inbound[..].zeroize();
        self.inbound.clear();
    }

    /// When the last octet was read.
    fn last_read(&self) -> Instant {
        self.last_read
    }

    /// How long a peer may take over a message before the octets it has sent
    /// buy it more time: [`MESSAGE_IVL`].
    fn message_ivl(&self) -> Duration {
        self.message_ivl
    }

    /// The moment after which the message now arriving is overdue, or `None`
    /// when none is half received.
    ///
    /// [`MESSAGE_IVL`] after its first octet, and one second more for every
    /// [`MIN_MESSAGE_RATE`] octets it has brought: a message that keeps
    /// arriving at that pace is never overdue, however large, and a peer that
    /// announces a message and sends it too slowly — or not at all — is, and
    /// gains nothing by announcing a large one, because the time it is
    /// allowed is bought with octets that arrived and not with octets it
    /// declared.
    fn message_deadline(&self) -> Option<Instant> {
        let started = self.started?;
        let arrived = self.received - self.message_origin;
        started.checked_add(self.message_ivl + Duration::from_secs(arrived / MIN_MESSAGE_RATE))
    }

    /// Output queued and not yet written.
    fn pending(&self) -> usize {
        self.outbound.len() - self.written
    }

    fn has_pending(&self) -> bool {
        self.written < self.outbound.len() || self.flush_due
    }

    /// Reads the next whole message or command.
    ///
    /// Cancel-safe: the buffer and the frames of a half-received message
    /// survive, so a dropped call loses nothing but the in-flight `read`.
    ///
    /// The limits are [`MessageLimits`]': `ZMQ_MAXMSGSIZE` is judged from
    /// declared lengths, at the header of the frame that crosses it, and a
    /// multipart message is delivered only once its last frame has arrived.
    pub async fn read_next(&mut self) -> Result<Incoming> {
        loop {
            if let Some(incoming) = self.next_buffered()? {
                return Ok(incoming);
            }
            self.fill().await?;
        }
    }

    /// Reads the next command body of a handshake, and refuses everything
    /// else **at its header**: nothing may precede a handshake command, so a
    /// message frame is a protocol violation, and nothing is buffered on its
    /// behalf. A command declaring more than [`HANDSHAKE_MAX_FRAME`] is
    /// refused the same way, which is what keeps a peer that has not
    /// authenticated from making this side buffer `ZMQ_MAXMSGSIZE`.
    pub(crate) async fn read_command(&mut self, what: &str) -> Result<Bytes> {
        let cap = HANDSHAKE_MAX_FRAME.min(self.limits.max_bytes);
        loop {
            if let Some((header, used)) = self.buffered_header(cap)? {
                if header.kind != FrameKind::Command {
                    return Err(Error::ENOCOMPATPROTO(
                        format!("the peer sent a message before its {what}").into(),
                    ));
                }
                let (len, end) = frame_end(used, header.len)?;
                if self.inbound.len() >= end {
                    self.inbound.advance(used);
                    let body = self.take_body(len);
                    self.after_delivery();
                    return Ok(body);
                }
                self.make_room(end);
            }
            self.fill().await?;
        }
    }

    /// Waits for the next thing that needs doing on the wire: a message or
    /// command to hand on, or queued output that was written.
    ///
    /// Reading and writing are polled by **one** future, which is what lets a
    /// loop wait for both at once while the rest of it borrows the wire for
    /// nothing else. Cancel-safe: state moves only when an octet has really
    /// been read or written.
    async fn next_event(&mut self) -> Result<Event> {
        poll_fn(|cx| self.poll_event(cx)).await
    }

    fn poll_event(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<Event>> {
        // Reading and writing take turns at going first, so a peer that
        // keeps one of them ready for ever cannot starve the other.
        self.write_first = !self.write_first;
        for write in [self.write_first, !self.write_first] {
            if write {
                if self.has_pending() {
                    let mut step = pin!(self.write_some());
                    if let Poll::Ready(outcome) = step.as_mut().poll(cx) {
                        return Poll::Ready(outcome.map(|()| Event::Wrote));
                    }
                }
            } else if let Poll::Ready(outcome) = self.poll_receive(cx) {
                return Poll::Ready(outcome.map(Event::Received));
            }
        }
        Poll::Pending
    }

    fn poll_receive(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<Incoming>> {
        loop {
            match self.next_buffered() {
                Ok(Some(incoming)) => return Poll::Ready(Ok(incoming)),
                Ok(None) => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            let mut step = pin!(self.fill());
            match step.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// The next whole message or command already in the buffer, assembling
    /// frames until one completes. `None` when more octets are needed.
    fn next_buffered(&mut self) -> Result<Option<Incoming>> {
        loop {
            let frame = if self.curve.is_some() {
                self.next_sealed_frame()?
            } else {
                self.next_plain_frame()?
            };
            let Some(Frame { kind, body, cost }) = frame else {
                return Ok(None);
            };
            match kind {
                FrameKind::Command => {
                    if !self.assembly.is_empty() {
                        return Err(command_in_multipart());
                    }
                    self.after_delivery();
                    return Ok(Some(Incoming::Command(body)));
                }
                FrameKind::Message { more } => {
                    if let Some(message) = self.assembly.push(Message::from(body), cost, more) {
                        self.after_delivery();
                        return Ok(Some(Incoming::Message(message)));
                    }
                }
            }
        }
    }

    /// The frame header at the front of the buffer, checked against `cap`,
    /// or `None` while it has not fully arrived.
    fn buffered_header(&self, cap: u64) -> Result<Option<(frame::FrameHeader, usize)>> {
        match frame::decode_header(&self.inbound, cap) {
            Ok(decoded) => Ok(Some(decoded)),
            Err(FrameError::Incomplete) => Ok(None),
            Err(e) => Err(frame_error(e, self.limits.max_bytes)),
        }
    }

    /// The next frame of an unencrypted connection, if it is all there. Its
    /// header is judged against what the message may still spend **before
    /// its body is awaited**, and the body is room reserved once.
    fn next_plain_frame(&mut self) -> Result<Option<Frame>> {
        let remaining = self.assembly.budget().remaining(&self.limits);
        let Some((header, used)) = self.buffered_header(remaining)? else {
            return Ok(None);
        };
        let cost = self.assembly.budget().check(used, header, &self.limits)?;
        let (len, end) = frame_end(used, header.len)?;
        if self.inbound.len() < end {
            self.make_room(end);
            return Ok(None);
        }
        self.inbound.advance(used);
        let body = self.take_body(len);
        Ok(Some(Frame {
            kind: header.kind,
            body,
            cost,
        }))
    }

    /// The next frame of a CURVE connection: one `MESSAGE` box, opened, or an
    /// `ERROR` in clear text.
    ///
    /// Nothing is opened until the box is whole, and a box that holds more
    /// than the message may still spend is refused from its header, before it
    /// is received and before any cryptographic work is spent on it. Opening
    /// and handing the frame on have no await between them, so a dropped
    /// future never loses an opened box — and never re-opens one, which would
    /// fail the nonce check.
    fn next_sealed_frame(&mut self) -> Result<Option<Frame>> {
        let remaining = self.assembly.budget().remaining(&self.limits);
        // The box and its framing cost `MESSAGE_MIN_LEN` octets beyond the
        // frame body inside, which is what the plaintext header adds to the
        // largest frame that fits.
        let cap = remaining.saturating_add(curve_layout::MESSAGE_MIN_LEN as u64 + 9);
        let Some((header, used)) = self.buffered_header(cap)? else {
            return Ok(None);
        };
        let (len, end) = frame_end(used, header.len)?;
        if len >= curve_layout::MESSAGE_MIN_LEN {
            let plain = (len - curve_layout::MESSAGE_MIN_LEN) as u64;
            self.assembly.budget().check(
                header_octets(plain),
                frame::FrameHeader {
                    kind: FrameKind::Command,
                    len: plain,
                },
                &self.limits,
            )?;
        }
        if self.inbound.len() < end {
            self.make_room(end);
            return Ok(None);
        }

        // An `ERROR` is the one command that still arrives in clear text:
        // 37/ZMTP calls it fatal and a peer sending it is giving up, so
        // reading it turns a clean refusal into a reason instead of a
        // timeout. Nothing else is accepted outside a box.
        if self.inbound[used..end].starts_with(b"\x05ERROR")
            && let Ok(Command::Error(reason)) = Command::decode(&self.inbound[used..end])
        {
            return Err(Error::ENOCOMPATPROTO(
                format!("the peer refused the connection: {}", sanitize(reason)).into(),
            ));
        }
        // **The frame kind is not the test; the box is.** 26/CURVEZMQ calls
        // `MESSAGE` a command, and libzmq 4.3.5 sends it in a **message**
        // frame — flags without the COMMAND bit, body `\x07MESSAGE` —
        // measured in `tests/interop_libzmq.rs`. So both kinds are read, the
        // body decides what arrived, and the outer MORE flag is ignored
        // because the real one is inside the box. A body that is not a
        // `MESSAGE` is refused by the opening, and a `MESSAGE` whose box
        // does not open is refused after that, which is the check that
        // matters.
        let curve = self
            .curve
            .as_mut()
            .ok_or_else(|| Error::ENOTSOCK("this connection has no CURVE session".into()))?;
        // The box is opened where it lies: the plaintext stays in the receive
        // buffer and leaves it like a clear frame's body, so a sealed frame
        // costs its decryption and no copy of its own.
        let (flags, offset) = curve.open_message_in_place(&mut self.inbound[used..end])?;
        let kind = if flags & curve_layout::MESSAGE_FLAG_COMMAND != 0 {
            FrameKind::Command
        } else {
            FrameKind::Message {
                more: flags & curve_layout::MESSAGE_FLAG_MORE != 0,
            }
        };
        let body_at = used + offset;
        let plain_len = (end - body_at) as u64;
        let cost = self.assembly.budget().check(
            header_octets(plain_len),
            frame::FrameHeader {
                kind,
                len: plain_len,
            },
            &self.limits,
        )?;
        self.inbound.advance(body_at);
        let body = self.take_body(end - body_at);
        Ok(Some(Frame { kind, body, cost }))
    }

    /// Takes `len` octets off the front of the buffer as a frame's body.
    ///
    /// A large frame is a slice of the buffer it was read into, so the
    /// payload is copied by nobody after the kernel's read; a small one is
    /// copied out, because a slice keeps the whole allocation alive for as
    /// long as it is queued and a few octets must not pin a read buffer.
    ///
    /// **Large is judged against the allocation** ([`Wire::allocated`]) and
    /// not against `BytesMut::capacity`, which counts from the front of what
    /// is left: frames taken off the front before this one are not in it. A
    /// small frame that arrived in the slack behind a megabyte-sized one
    /// would be a large share of that slack, and would keep the megabyte
    /// resident for a few kilobytes of charge in whatever queue holds it.
    fn take_body(&mut self, len: usize) -> Bytes {
        if len == 0 {
            return Bytes::new();
        }
        if len >= SLICE_MIN && len.saturating_mul(SLICE_SHARE) >= self.allocated {
            return self.inbound.split_to(len).freeze();
        }
        let body = Message::copy_of(&self.inbound[..len]).into_inner();
        self.inbound.advance(len);
        body
    }

    /// Reserves room for a frame whose header has been read and whose body
    /// has not arrived, in one step: its declared length is already within
    /// its budget, so asking for all of it now avoids growing the buffer
    /// (and copying what is in it) once per doubling.
    fn make_room(&mut self, end: usize) {
        // The slack is what keeps the last read of a large frame from finding
        // less than [`MIN_READ_SPARE`] and growing — which would copy the
        // whole frame to make room for a few octets.
        self.reserve_spare(end - self.inbound.len() + MIN_READ_SPARE);
    }

    /// Makes sure `spare` more octets fit behind what the buffer holds.
    ///
    /// When they do not, the buffer moves to an allocation sized for them and
    /// what it held is copied across. It is done here rather than by
    /// `BytesMut::reserve`, which reuses an allocation, grows it in place or
    /// replaces it as it finds convenient and does not say which:
    /// [`Wire::allocated`] has to be the size of the allocation the buffer
    /// points into, and this is the one place that makes one.
    fn reserve_spare(&mut self, spare: usize) {
        if self.inbound.capacity() - self.inbound.len() >= spare {
            return;
        }
        let mut grown = BytesMut::with_capacity(self.inbound.len() + spare);
        grown.extend_from_slice(&self.inbound);
        self.allocated = grown.capacity();
        self.inbound = grown;
    }

    /// Bookkeeping after a message or command has been handed on.
    fn after_delivery(&mut self) {
        // What is left in the buffer is the start of the next message, which
        // arrived with the last read.
        self.started = if self.inbound.is_empty() {
            None
        } else {
            Some(self.last_read)
        };
        self.message_origin = self.received - self.inbound.len() as u64;
        // A connection that once received a large message does not keep its
        // buffer for the rest of its life. The size of the allocation is what
        // counts: an empty buffer still points into the one a large frame was
        // sliced off, and reports only the few kilobytes behind it.
        if self.inbound.is_empty() && self.allocated > RETAINED_BUFFER {
            self.inbound = BytesMut::new();
            self.allocated = 0;
        }
    }

    /// Queues one whole message: every frame, MORE on all but the last.
    ///
    /// Nothing is written: the frames go into the connection's output, and a
    /// message is never split because the output is only ever written from
    /// its front. A peer must never see half a message — "on sending, the
    /// peer SHALL queue all frames of a message in memory until the final
    /// frame is sent" — and a batch of messages shares one write. Under CURVE
    /// each frame is sealed on its own, with its MORE flag inside the box.
    pub(crate) fn queue_message(&mut self, message: &Multipart) -> Result<()> {
        match self.curve.as_mut() {
            None => message.encode_into(&mut self.outbound),
            Some(curve) => {
                // One reservation for the whole message, so that sealing a
                // frame appends into room that is already there.
                self.outbound.reserve(
                    message
                        .frames()
                        .iter()
                        .map(|frame| sealed_frame_len(frame.len()))
                        .sum(),
                );
                let last = message.len() - 1;
                for (index, frame) in message.frames().iter().enumerate() {
                    let flags = if index == last {
                        0
                    } else {
                        curve_layout::MESSAGE_FLAG_MORE
                    };
                    curve.seal_frame_into(flags, frame.as_slice(), &mut self.outbound)?;
                }
            }
        }
        Ok(())
    }

    /// Queues one command frame — sealed, on a CURVE connection.
    pub(crate) fn queue_command(&mut self, command: &Command<'_>) -> Result<()> {
        self.scratch.clear();
        command
            .encode_body(&mut self.scratch)
            .map_err(command_error)?;
        match self.curve.as_mut() {
            None => {
                frame::encode_header(
                    FrameKind::Command,
                    self.scratch.len() as u64,
                    &mut self.outbound,
                );
                self.outbound.extend_from_slice(&self.scratch);
            }
            Some(curve) => {
                self.outbound.reserve(sealed_frame_len(self.scratch.len()));
                curve.seal_frame_into(
                    curve_layout::MESSAGE_FLAG_COMMAND,
                    &self.scratch,
                    &mut self.outbound,
                )?;
            }
        }
        Ok(())
    }

    /// Writes one whole message and waits until it has been written.
    pub async fn write_message(&mut self, message: &Multipart) -> Result<()> {
        self.queue_message(message)?;
        self.flush().await
    }

    /// Writes one command frame — sealed, on a CURVE connection — and waits
    /// until it has been written.
    pub async fn write_command(&mut self, command: &Command<'_>) -> Result<()> {
        self.queue_command(command)?;
        self.flush().await
    }

    /// Writes octets as they are — the greeting — and waits until they have
    /// been written.
    pub(crate) async fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.outbound.extend_from_slice(bytes);
        self.flush().await
    }

    /// Writes one CURVE handshake frame, which is already complete octets:
    /// the handshake is what agrees the session keys, so it is never sealed
    /// by them.
    pub(crate) async fn write_handshake(&mut self, frame_bytes: &[u8]) -> Result<()> {
        self.write_raw(frame_bytes).await
    }

    /// Writes everything queued.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        while self.has_pending() {
            self.write_some().await?;
        }
        Ok(())
    }

    /// One step of getting queued output onto the wire: a `write`, and the
    /// flush that follows once the output is empty.
    ///
    /// Cancel-safe: `AsyncWriteExt::write` writes nothing when it is
    /// dropped, and the position is advanced only for octets it reported.
    async fn write_some(&mut self) -> Result<()> {
        if self.written < self.outbound.len() {
            let wrote = self.io.write(&self.outbound[self.written..]).await?;
            if wrote == 0 {
                return Err(Error::EHOSTUNREACH("the peer closed the connection".into()));
            }
            self.written += wrote;
            self.flush_due = true;
            if self.written < self.outbound.len() {
                return Ok(());
            }
            self.outbound.clear();
            self.written = 0;
            if self.outbound.capacity() > RETAINED_BUFFER {
                self.outbound = Vec::new();
            }
        }
        self.io.flush().await?;
        self.flush_due = false;
        Ok(())
    }

    /// Reads exactly `out.len()` octets — a peer's greeting — using whatever
    /// is already buffered.
    async fn read_exactly(&mut self, out: &mut [u8]) -> Result<()> {
        while self.inbound.len() < out.len() {
            self.fill().await?;
        }
        out.copy_from_slice(&self.inbound[..out.len()]);
        self.inbound.advance(out.len());
        self.after_delivery();
        Ok(())
    }

    /// Reads exactly `out.len()` octets — a peer's greeting, in a test that
    /// drives the other side of a connection.
    #[cfg(test)]
    pub(crate) async fn read_exactly_for_test(&mut self, out: &mut [u8]) {
        self.read_exactly(out).await.expect("the peer's greeting");
    }

    /// Writes bytes with no framing — a greeting, in the same tests.
    #[cfg(test)]
    pub(crate) async fn write_raw_for_test(&mut self, bytes: &[u8]) {
        self.write_raw(bytes).await.expect("write");
    }

    /// Writes an `ERROR` and gives up on the connection.
    ///
    /// Best effort by construction: the peer is being closed on, so a write
    /// that fails changes nothing — including one that cannot be sealed.
    async fn refuse(&mut self, reason: &str) {
        if self.queue_command(&Command::Error(reason)).is_ok() {
            let _ = self.flush().await;
        }
    }

    /// Reads more octets, or reports the peer's close.
    ///
    /// Cancel-safe: `AsyncReadExt::read_buf` appends nothing when it is
    /// dropped, so a cancelled call leaves the buffer exactly as it was. The
    /// version that grows the buffer first and truncates after the await
    /// looks equivalent and is not — a cancelled read leaves the slack
    /// behind, and a zero-filled buffer decodes as a stream of empty message
    /// frames.
    async fn fill(&mut self) -> Result<()> {
        if self.inbound.capacity() - self.inbound.len() < MIN_READ_SPARE {
            self.reserve_spare(READ_CHUNK);
        }
        let before = self.received;
        let read = self.io.read_buf(&mut self.inbound).await?;
        if read == 0 {
            return Err(Error::EHOSTUNREACH("the peer closed the connection".into()));
        }
        self.received += read as u64;
        self.last_read = Instant::now();
        if self.started.is_none() {
            self.started = Some(self.last_read);
            self.message_origin = before;
        }
        Ok(())
    }
}

/// A declared body length and where its frame ends, for a header of `used`
/// octets. The cap has already bounded the length, so this can only fail on
/// a target whose address space is smaller than the cap.
fn frame_end(used: usize, len: u64) -> Result<(usize, usize)> {
    let unaddressable =
        || Error::EMSGSIZE(format!("a frame declaring {len} bytes cannot be addressed").into());
    let body = usize::try_from(len).map_err(|_| unaddressable())?;
    let end = used.checked_add(body).ok_or_else(unaddressable)?;
    Ok((body, end))
}

/// The octets one sealed frame with a body of `body_len` octets takes on the
/// wire: the frame header and the `MESSAGE` command around its box.
fn sealed_frame_len(body_len: usize) -> usize {
    let command = curve_layout::MESSAGE_MIN_LEN + body_len;
    header_octets(command as u64) + command
}

/// Makes a reason printable, since `ERROR` carries printable ASCII only and
/// the reasons here quote what a peer sent.
fn sanitize(reason: &str) -> String {
    reason
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .take(255)
        .collect()
}

fn greeting_error(error: GreetingError) -> Error {
    Error::ENOCOMPATPROTO(format!("greeting refused: {error}").into())
}

fn command_error(error: CommandError) -> Error {
    Error::ENOCOMPATPROTO(format!("malformed command: {error}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, ContextConfig};
    use crate::endpoint::Endpoint;
    use crate::engine::{Engine, HandshakeGate, PeerId, Role};
    use crate::message::Message;
    use crate::pipe::Pipe;
    use crate::transport::Stream;
    use crate::zap::{AuthenticatedUser, ZapReply, ZapStatus};
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};
    use weida_zmtp::{FrameKind, frame};

    /// A peer driven directly by the codec: the only honest way to check a
    /// protocol implementation is against the specification's own bytes.
    struct CodecPeer<S = TcpStream> {
        io: S,
        buf: Vec<u8>,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin> CodecPeer<S> {
        fn new(io: S) -> CodecPeer<S> {
            CodecPeer {
                io,
                buf: Vec::new(),
            }
        }

        async fn read_greeting(&mut self) -> [u8; greeting::GREETING_LEN] {
            let mut out = [0u8; greeting::GREETING_LEN];
            self.read_exactly(&mut out).await;
            out
        }

        async fn read_exactly(&mut self, out: &mut [u8]) {
            while self.buf.len() < out.len() {
                let mut chunk = [0u8; 4096];
                let read = self.io.read(&mut chunk).await.expect("read");
                assert_ne!(read, 0, "the peer closed while we waited");
                self.buf.extend_from_slice(&chunk[..read]);
            }
            out.copy_from_slice(&self.buf[..out.len()]);
            self.buf.drain(..out.len());
        }

        /// The next frame, header and body, decoded by the codec.
        async fn read_frame(&mut self) -> (FrameKind, Vec<u8>) {
            loop {
                match frame::decode(&self.buf, 1 << 20) {
                    Ok((header, body, used)) => {
                        let body = body.to_vec();
                        self.buf.drain(..used);
                        return (header.kind, body);
                    }
                    Err(e) if !e.is_violation() => {
                        let mut chunk = [0u8; 4096];
                        let read = self.io.read(&mut chunk).await.expect("read");
                        assert_ne!(read, 0, "the peer closed while we waited for a frame");
                        self.buf.extend_from_slice(&chunk[..read]);
                    }
                    Err(e) => panic!("the session sent something malformed: {e}"),
                }
            }
        }

        async fn send(&mut self, bytes: &[u8]) {
            self.io.write_all(bytes).await.expect("write");
            self.io.flush().await.expect("flush");
        }

        async fn greet(&mut self, version: Version) {
            let greeting = Greeting {
                version,
                ..Greeting::null()
            };
            self.send(&greeting.encode()).await;
        }

        async fn ready(&mut self, socket_type: SocketType) {
            let ready = Command::Ready(Metadata::new().with_socket_type(socket_type))
                .encode()
                .expect("READY");
            self.send(&ready).await;
        }

        /// Greets with a mechanism and an `as-server` octet, which is what
        /// PLAIN needs and NULL forbids.
        async fn greet_as(&mut self, mechanism: weida_zmtp::Mechanism, as_server: bool) {
            let greeting = Greeting {
                mechanism,
                as_server,
                ..Greeting::null()
            };
            self.send(&greeting.encode()).await;
        }

        async fn command(&mut self, command: Command<'_>) {
            let bytes = command.encode().expect("encode");
            self.send(&bytes).await;
        }

        /// The next command, decoded by name, for a handshake assertion.
        async fn read_command_name(&mut self) -> (String, Vec<u8>) {
            let (kind, body) = self.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            let name = Command::decode(&body)
                .map(|command| command.name().to_owned())
                .unwrap_or_else(|e| panic!("the session sent a bad command: {e}"));
            (name, body)
        }
    }

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    /// Runs a session over one end of a connected pair, with no engine, and
    /// hands back the pipe so a test can see both directions.
    ///
    /// `role` is the one fact the engine would otherwise supply that the
    /// security handshake reads: under NULL it decides which side
    /// authorizes, so a test of a server's ZAP dialog binds rather than
    /// dials.
    fn drive_session(
        stream: TcpStream,
        ours: SocketType,
        options: SocketOptions,
        role: Role,
        context: &Context,
    ) -> (Pipe, AuthenticatedUser, tokio::task::JoinHandle<Result<()>>) {
        drive_stream(Stream::tcp(stream), ours, options, role, context)
    }

    /// [`drive_session`] over any stream: an in-memory one when a test wants a
    /// peer that can be made to stop reading.
    fn drive_stream(
        stream: Stream,
        ours: SocketType,
        options: SocketOptions,
        role: Role,
        context: &Context,
    ) -> (Pipe, AuthenticatedUser, tokio::task::JoinHandle<Result<()>>) {
        let pipe = Pipe::new(options.pipe);
        let user = AuthenticatedUser::default();
        let connection = Connection {
            stream,
            pipe: pipe.clone(),
            peer: PeerId::detached(),
            role,
            endpoint: Endpoint::parse("tcp://127.0.0.1:1").expect("endpoint"),
            options,
            exec: Exec::current().expect("ambient reactor"),
            handshake: HandshakeGate::detached(),
            identity: crate::engine::AnnouncedIdentity::default(),
            subscriptions: Arc::new(Subscriptions::new(
                crate::subscriptions::DEFAULT_MAX_SUBSCRIPTIONS,
                crate::subscriptions::DEFAULT_MAX_SUBSCRIPTION_BYTES,
            )),
            context: context.clone(),
            peer_address: "127.0.0.1".to_owned(),
            credentials: None,
            user: user.clone(),
        };
        let session = ZmtpSession::new(ours);
        let task = tokio::spawn(async move { session.run(connection).await });
        (pipe, user, task)
    }

    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("addr");
        let dialling = tokio::spawn(async move { TcpStream::connect(addr).await });
        let (accepted, _) = listener.accept().await.expect("accept");
        let dialled = dialling.await.expect("task").expect("connect");
        (dialled, accepted)
    }

    fn options() -> SocketOptions {
        SocketOptions::default()
    }

    /// A ZAP handler in this context that answers every request with
    /// `decide`, and reports what it was asked.
    ///
    /// A real handler on a real REP socket over `inproc://`, because a ZAP
    /// dialog that skipped the transport would prove nothing about the
    /// transport it is specified to run over.
    async fn zap_handler(
        context: &Context,
        decide: impl Fn(crate::zap::ZapRequest) -> ZapReply + Send + 'static,
    ) -> (
        Arc<std::sync::Mutex<Vec<crate::zap::ZapRequest>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&asked);
        let mut handler = crate::RepSocket::new(context).expect("rep");
        // Bound **before** the task is spawned, which is 27/ZAP's "the
        // handler SHALL start before any server starts" made structural, and
        // which also keeps the spawned future `Send`: a socket may be moved
        // into a task but not shared with one, so the borrow `bind` takes
        // must not cross the spawn.
        handler
            .bind(crate::zap::ZAP_ENDPOINT)
            .await
            .expect("bind the ZAP endpoint");
        let task = tokio::spawn(async move {
            while let Ok(message) = handler.recv().await {
                let request = crate::zap::ZapRequest::decode(&message).expect("a ZAP request");
                seen.lock().expect("seen").push(request.clone());
                let reply = decide(request);
                if handler.send(reply.encode()).await.is_err() {
                    return;
                }
            }
        });
        (asked, task)
    }

    /// A CURVE server's options, and the key a client needs to reach it.
    fn curve_server_options(zap_domain: &str) -> (SocketOptions, crate::curve::CurvePublicKey) {
        let (public, secret) = crate::curve::keypair();
        (
            SocketOptions {
                curve_server: true,
                curve_secretkey: Some(secret),
                zap_domain: zap_domain.to_owned(),
                ..options()
            },
            public,
        )
    }

    /// A CURVE client on a raw socket: greets, runs 26/CURVEZMQ's four
    /// commands against a server session, and hands back the transport the
    /// handshake agreed.
    ///
    /// Driven by this crate's client half over the wire rather than by a
    /// second session, so what a test asserts is octets on a socket and not
    /// an agreement between two copies of one state machine.
    async fn curve_handshake_as_client(
        peer: &mut CodecPeer,
        server_key: crate::curve::CurvePublicKey,
        client_public: crate::curve::CurvePublicKey,
        client_secret: &crate::curve::CurveSecretKey,
    ) -> CurveTransport {
        let greeting = Greeting::decode(&peer.read_greeting().await).expect("greeting");
        assert_eq!(greeting.mechanism, weida_zmtp::Mechanism::CURVE);
        assert!(
            greeting.as_server,
            "a CURVE server's as-server octet is one"
        );
        peer.greet_as(weida_zmtp::Mechanism::CURVE, false).await;

        let mut client = CurveClient::new(client_public, client_secret, server_key);
        let hello = client.hello().expect("HELLO");
        assert_eq!(
            hello.len(),
            2 + curve_layout::HELLO_LEN,
            "a HELLO is 200 octets behind a short frame header"
        );
        peer.send(&hello).await;

        let (kind, body) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command);
        assert_eq!(body.len(), curve_layout::WELCOME_LEN);
        client.read_welcome(&body).expect("read WELCOME");
        let initiate = client
            .initiate(&Metadata::new().with_socket_type(SocketType::Pair))
            .expect("INITIATE");
        peer.send(&initiate).await;

        let (kind, body) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command);
        let metadata_bytes = client.read_ready(&body).expect("read READY");
        let metadata = Metadata::decode(&metadata_bytes).expect("the server's metadata");
        assert_eq!(metadata.socket_type(), Some(SocketType::Pair));
        client.into_transport().expect("transport")
    }

    // ---- hostile peers: what a stranger can make this side hold or wait for ----

    /// A wait for every await in these tests: it reports past ten seconds
    /// and keeps waiting.
    async fn within<T>(future: impl Future<Output = T>) -> T {
        weida_testkit::within("the operation", Duration::from_secs(10), future).await
    }

    /// A stream that yields what it holds a few octets per read — the way a
    /// peer that sends one octet per packet does — and swallows what is
    /// written to it.
    struct Drip {
        data: Vec<u8>,
        at: usize,
        chunk: usize,
    }

    impl AsyncRead for Drip {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let take = self
                .chunk
                .min(self.data.len() - self.at)
                .min(buf.remaining());
            let (from, to) = (self.at, self.at + take);
            buf.put_slice(&self.data[from..to]);
            self.at = to;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for Drip {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A session over an in-memory stream, handshaken as a NULL `PUSH` that
    /// dialled a `PULL`, with the peer's end of the stream handed back.
    async fn handshaken_over_a_duplex(
        options: SocketOptions,
        window: usize,
    ) -> (
        Pipe,
        tokio::task::JoinHandle<Result<()>>,
        CodecPeer<tokio::io::DuplexStream>,
    ) {
        let (ours, theirs) = tokio::io::duplex(window);
        let mut peer = CodecPeer::new(theirs);
        let (pipe, _user, task) = drive_stream(
            Stream::inproc(ours),
            SocketType::Push,
            options,
            Role::Connecter,
            &context(),
        );
        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.ready(SocketType::Pull).await;
        (pipe, task, peer)
    }

    /// Claim: **before a peer has authenticated, this side buffers what a
    /// handshake needs and no more.** A frame that cannot be a handshake
    /// command is refused at its header — the body is neither awaited nor
    /// buffered — even when `ZMQ_MAXMSGSIZE` has been raised to a gigabyte
    /// for bulk traffic.
    #[tokio::test]
    async fn a_handshake_refuses_at_the_header_what_it_cannot_need() {
        let limits = MessageLimits::new(1 << 30, crate::message::DEFAULT_MAX_MESSAGE_FRAMES);

        // A message frame declaring 900 KiB where READY belongs, followed by
        // a few KiB of it: refused on the header alone.
        let (ours, mut theirs) = tokio::io::duplex(1 << 20);
        let mut wire = Wire::new(ours, limits);
        let mut header = vec![0x02u8];
        header.extend_from_slice(&(900u64 * 1024).to_be_bytes());
        theirs.write_all(&header).await.expect("write");
        theirs.write_all(&[0u8; 4096]).await.expect("write");
        let err = within(wire.read_command("READY")).await.unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
        assert!(
            wire.buffered() < 8 * 1024,
            "{} octets were buffered for a frame no handshake sends",
            wire.buffered()
        );

        // A *command* just past what any handshake command needs.
        let (ours, mut theirs) = tokio::io::duplex(1 << 20);
        let mut wire = Wire::new(ours, limits);
        let mut header = vec![0x06u8];
        header.extend_from_slice(&(HANDSHAKE_MAX_FRAME + 1).to_be_bytes());
        theirs.write_all(&header).await.expect("write");
        let err = within(wire.read_command("READY")).await.unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");

        // A message frame inside the cap, whose body never arrives: the
        // refusal does not wait for it.
        let (ours, mut theirs) = tokio::io::duplex(1 << 20);
        let mut wire = Wire::new(ours, limits);
        theirs.write_all(&[0x00, 100]).await.expect("write");
        let err = within(wire.read_command("READY")).await.unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
        assert!(err.cause().contains("before its READY"), "{err}");

        // And a legitimate command still goes through.
        let (ours, mut theirs) = tokio::io::duplex(1 << 20);
        let mut wire = Wire::new(ours, limits);
        theirs
            .write_all(&Command::Ready(Metadata::new()).encode().expect("READY"))
            .await
            .expect("write");
        let body = within(wire.read_command("READY")).await.expect("READY");
        assert_eq!(&body[..], b"\x05READY");
    }

    /// Claim: **receiving is linear in the octets received.** A message
    /// whose first frame is half a mebibyte and whose second arrives a few
    /// octets at a time — about ten thousand reads — costs a copy of each
    /// frame at most once, not a copy of the first frame per read.
    #[tokio::test]
    async fn a_message_dripped_octet_by_octet_is_not_copied_per_read() {
        let message = Multipart::new(vec![
            Message::from(vec![1u8; 512 * 1024]),
            Message::from(vec![2u8; 400 * 1024]),
        ])
        .expect("frames");
        let total = message.total_bytes();
        let drip = Drip {
            data: message.encode(),
            at: 0,
            chunk: 97,
        };
        let mut wire = Wire::new(drip, MessageLimits::new(2 * 1024 * 1024, 16));

        let before = crate::message::copied_bytes();
        let Incoming::Message(arrived) = within(wire.read_next()).await.expect("the message")
        else {
            panic!("expected a message");
        };
        assert_eq!(arrived, message);
        let copied = crate::message::copied_bytes() - before;
        assert!(
            copied <= total,
            "{copied} octets were copied to receive a {total}-octet message"
        );
    }

    /// Claim: a connection does not keep the capacity of one large message
    /// for the rest of its life — and the frame it handed on is what it was
    /// read into, not a second copy.
    #[tokio::test]
    async fn a_connection_does_not_keep_the_capacity_of_one_large_message() {
        let (ours, mut theirs) = tokio::io::duplex(4 << 20);
        let mut wire = Wire::new(ours, MessageLimits::new(2 * 1024 * 1024, 16));
        let big = Multipart::single(vec![1u8; 1_000_000]).encode();
        theirs.write_all(&big).await.expect("write");

        let before = crate::message::copied_bytes();
        let Incoming::Message(message) = within(wire.read_next()).await.expect("the message")
        else {
            panic!("expected a message");
        };
        assert_eq!(message.total_bytes(), 1_000_000);
        assert_eq!(
            crate::message::copied_bytes(),
            before,
            "a large frame is a slice of the receive buffer"
        );
        assert!(
            wire.receive_capacity() <= RETAINED_BUFFER,
            "the empty buffer kept {} octets",
            wire.receive_capacity()
        );
    }

    /// Claim: **a small frame that arrives behind a large one does not keep
    /// the large one's allocation alive.** A SUB drops a large message at a
    /// full queue and keeps a later small one, so a small frame that was a
    /// slice of the large frame's allocation would turn a kilobyte of queue
    /// charge into a megabyte resident. The two arrive in one read, which
    /// puts the small one in the slack the large one's allocation was given.
    #[tokio::test]
    async fn a_small_frame_behind_a_large_one_does_not_keep_its_allocation_alive() {
        let (ours, mut theirs) = tokio::io::duplex(4 << 20);
        let mut wire = Wire::new(ours, MessageLimits::new(2 * 1024 * 1024, 16));
        let mut both = Multipart::single(vec![1u8; 1_000_000]).encode();
        both.extend_from_slice(&Multipart::single(vec![2u8; 1_200]).encode());
        theirs.write_all(&both).await.expect("write");

        let Incoming::Message(large) = within(wire.read_next()).await.expect("the large message")
        else {
            panic!("expected a message");
        };
        let Incoming::Message(small) = within(wire.read_next()).await.expect("the small message")
        else {
            panic!("expected a message");
        };
        assert_eq!(small.total_bytes(), 1_200);

        // The large frame ends where the slack of its allocation begins, and a
        // slice of that slack starts inside it.
        let large = large.frames()[0].as_slice();
        let slack_at = large.as_ptr() as usize + large.len();
        let small_at = small.frames()[0].as_slice().as_ptr() as usize;
        assert!(
            !(slack_at..slack_at + MIN_READ_SPARE).contains(&small_at),
            "the small frame is a slice of the large frame's allocation"
        );
        assert!(
            wire.receive_capacity() <= RETAINED_BUFFER,
            "the empty buffer kept {} octets",
            wire.receive_capacity()
        );
    }

    /// Claim: the allowance is bought by octets that arrive, so a message
    /// that keeps arriving — slowly, but steadily — is never overdue.
    #[tokio::test(start_paused = true)]
    async fn a_message_that_keeps_arriving_is_never_overdue() {
        let (ours, mut theirs) = tokio::io::duplex(1 << 20);
        let mut wire =
            Wire::new(ours, MessageLimits::default()).with_message_ivl(Duration::from_millis(200));
        let options = options();
        let pipe = Pipe::new(options.pipe);
        let subscriptions = Arc::new(Subscriptions::new(
            crate::subscriptions::DEFAULT_MAX_SUBSCRIPTIONS,
            crate::subscriptions::DEFAULT_MAX_SUBSCRIPTION_BYTES,
        ));
        let exec = Exec::current().expect("ambient reactor");

        // 40 KiB in four pieces, 150 ms apart: more than a message interval
        // in all, at a pace far above the minimum rate.
        let frame = Multipart::single(vec![5u8; 40 * 1024]).encode();
        let sender = tokio::spawn(async move {
            for piece in frame.chunks(10 * 1024) {
                theirs.write_all(piece).await.expect("write");
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            theirs
        });
        let incoming = pipe.incoming();
        tokio::select! {
            outcome = pump(
                &mut wire,
                SocketType::Pull,
                &subscriptions,
                &pipe,
                &options,
                &exec,
                greeting::VERSION,
            ) => panic!("the pump ended while a message was arriving: {outcome:?}"),
            arrived = within(incoming.recv()) => {
                assert_eq!(arrived.expect("a message").frames()[0].len(), 40 * 1024);
            }
        }
        let _ = sender.await;
    }

    /// Claim: **a message that takes longer than the heartbeat timeout to
    /// arrive is traffic, not silence.** The octets of a slow transfer count
    /// as a sign of life as they arrive, and not when the message is done.
    #[tokio::test(start_paused = true)]
    async fn a_slowly_arriving_message_is_not_silence() {
        let (pipe, task, mut peer) = handshaken_over_a_duplex(
            SocketOptions {
                heartbeat_ivl: Some(Duration::from_millis(100)),
                heartbeat_timeout: Some(Duration::from_millis(300)),
                ..options()
            },
            1 << 20,
        )
        .await;

        // 100 KiB over about a second: ten pieces, a tenth of a second
        // apart, each inside the timeout.
        let frame = Multipart::single(vec![9u8; 100 * 1024]).encode();
        for piece in frame.chunks(10 * 1024) {
            peer.send(piece).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let arrived = within(pipe.incoming().recv()).await.expect("a message");
        assert_eq!(arrived.frames()[0].len(), 100 * 1024);
        assert!(
            !task.is_finished(),
            "the session survived its own heartbeat"
        );
    }

    /// Claim: **a stray `READY` after the handshake is ignored without being
    /// parsed**, so a malformed one — which a parse would refuse by ending
    /// the connection — changes nothing, and the connection still answers.
    #[tokio::test]
    async fn a_stray_ready_after_the_handshake_is_not_parsed() {
        let (_pipe, task, mut peer) = handshaken_over_a_duplex(options(), 1 << 20).await;

        // A property whose value claims nine octets and holds two.
        peer.send(&frame::encode(
            FrameKind::Command,
            b"\x05READY\x01A\x00\x00\x00\x09ab",
        ))
        .await;
        peer.send(
            &Command::Ping {
                ttl: 0,
                context: b"still here",
            }
            .encode()
            .expect("PING"),
        )
        .await;
        let (name, body) = within(peer.read_command_name()).await;
        assert_eq!(
            name, "PONG",
            "the connection answered after the stray READY"
        );
        assert!(matches!(
            Command::decode(&body).expect("PONG"),
            Command::Pong { context } if context == b"still here"
        ));
        assert!(!task.is_finished());
    }

    // ---- the Wire against arbitrary streams ----

    /// Deterministic xorshift64*, as the codec's smoke tests use: reproducible
    /// failures and no dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn bytes(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| self.next() as u8).collect()
        }
    }

    /// A random multipart message whose frames straddle every size class of
    /// the frame header: empty, one octet, the short form's edge, the long
    /// form's start, and a few kilobytes.
    fn random_message(rng: &mut Rng) -> Multipart {
        const SIZES: [usize; 8] = [0, 1, 40, 254, 255, 256, 700, 5000];
        let frames = (0..1 + rng.below(5))
            .map(|_| {
                let len = SIZES[rng.below(SIZES.len())];
                Message::from(rng.bytes(len))
            })
            .collect();
        Multipart::new(frames).expect("frames")
    }

    /// A matched pair of CURVE transports, from a whole handshake.
    fn curve_transports() -> (CurveTransport, CurveTransport) {
        let body = |frame_bytes: &[u8]| {
            frame::decode(frame_bytes, 1 << 20)
                .expect("a frame")
                .1
                .to_vec()
        };
        let (server_public, server_secret) = crate::curve::keypair();
        let (client_public, client_secret) = crate::curve::keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);
        server
            .read_hello(&body(&client.hello().expect("HELLO")))
            .expect("read HELLO");
        client
            .read_welcome(&body(&server.welcome().expect("WELCOME")))
            .expect("read WELCOME");
        server
            .read_initiate(&body(&client.initiate(&Metadata::new()).expect("INITIATE")))
            .expect("read INITIATE");
        client
            .read_ready(&body(&server.ready(&Metadata::new()).expect("READY")))
            .expect("read READY");
        (
            client.into_transport().expect("client transport"),
            server.into_transport().expect("server transport"),
        )
    }

    /// Claim: **however a valid stream is cut into reads, the same messages
    /// and commands come out, in order** — one octet at a time, a few hundred
    /// at a time, and whether or not the stream is sealed. Frames are taken
    /// off one buffer as they complete, so this is where an off-by-one in
    /// that bookkeeping would show.
    #[tokio::test]
    async fn any_chunking_of_a_valid_stream_delivers_the_same_messages() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for round in 0..150 {
            let sealed = round % 2 == 1;
            let (mut sender, receiver) = if sealed {
                let (client, server) = curve_transports();
                (Some(client), Some(server))
            } else {
                (None, None)
            };

            let mut expected: Vec<Incoming> = Vec::new();
            let mut stream = Vec::new();
            for _ in 0..1 + rng.below(6) {
                if rng.below(4) == 0 {
                    let ping = Command::Ping {
                        ttl: rng.below(100) as u16,
                        context: b"ctx",
                    };
                    let mut body = Vec::new();
                    ping.encode_body(&mut body).expect("PING body");
                    match sender.as_mut() {
                        None => stream.extend_from_slice(&frame::encode(FrameKind::Command, &body)),
                        Some(transport) => stream.extend_from_slice(
                            &transport
                                .seal_frame(curve_layout::MESSAGE_FLAG_COMMAND, &body)
                                .expect("seal"),
                        ),
                    }
                    expected.push(Incoming::Command(Bytes::from(body)));
                } else {
                    let message = random_message(&mut rng);
                    match sender.as_mut() {
                        None => message.encode_into(&mut stream),
                        Some(transport) => {
                            let last = message.len() - 1;
                            for (index, frame) in message.frames().iter().enumerate() {
                                let flags = if index == last {
                                    0
                                } else {
                                    curve_layout::MESSAGE_FLAG_MORE
                                };
                                stream.extend_from_slice(
                                    &transport.seal_frame(flags, frame.as_slice()).expect("seal"),
                                );
                            }
                        }
                    }
                    expected.push(Incoming::Message(message));
                }
            }

            let drip = Drip {
                data: stream,
                at: 0,
                chunk: 1 + rng.below(300),
            };
            let mut wire = Wire::new(drip, MessageLimits::new(1 << 20, 16));
            if let Some(transport) = receiver {
                wire.encrypt_with(transport);
            }
            for want in expected {
                let got = within(wire.read_next()).await.expect("the next item");
                assert_eq!(got, want, "round {round}, sealed: {sealed}");
            }
            let end = within(wire.read_next()).await.unwrap_err();
            assert_eq!(end.errno(), "EHOSTUNREACH", "{end}");
        }
    }

    /// Claim: **batching never reorders or splits anything.** Two thousand
    /// two-frame messages — one in ten spanning many windows — go through a
    /// stream whose window is a kilobyte, with a peer that reads as it can:
    /// every message arrives whole, in order, frame by frame, whatever share
    /// of them were gathered into one write.
    #[tokio::test]
    async fn a_burst_through_a_tiny_window_arrives_complete_and_in_order() {
        let (pipe, task, mut peer) = handshaken_over_a_duplex(options(), 1024).await;
        let outgoing = pipe.outgoing();
        let producer = tokio::spawn(async move {
            for n in 0u32..2000 {
                let body = if n % 10 == 0 {
                    vec![(n % 251) as u8; 5000]
                } else {
                    n.to_be_bytes().to_vec()
                };
                let message = Multipart::new(vec![
                    Message::from(n.to_be_bytes().to_vec()),
                    Message::from(body),
                ])
                .expect("frames");
                outgoing.send(message).await.expect("queued");
            }
        });
        for n in 0u32..2000 {
            let (kind, first) = within(peer.read_frame()).await;
            assert_eq!(kind, FrameKind::Message { more: true }, "message {n}");
            assert_eq!(first, n.to_be_bytes(), "message {n}");
            let (kind, body) = within(peer.read_frame()).await;
            assert_eq!(kind, FrameKind::Message { more: false }, "message {n}");
            if n % 10 == 0 {
                assert_eq!(body, vec![(n % 251) as u8; 5000], "message {n}");
            } else {
                assert_eq!(body, n.to_be_bytes(), "message {n}");
            }
        }
        within(producer).await.expect("the producer");
        assert!(!task.is_finished());
    }

    /// Claim: **a subscription notification is never dropped because the
    /// application is slow.** An XPUB whose receive queue holds four messages
    /// is sent twenty distinct subscriptions by one peer before the
    /// application reads anything: the session waits for room, which pauses
    /// that peer's reads and nobody else's, and the application then receives
    /// every `\x01` notification, in order. A proxy forwarding them upstream
    /// would otherwise never learn some of its subscribers' topics.
    #[tokio::test(start_paused = true)]
    async fn an_xpub_notification_waits_for_the_application_instead_of_being_dropped() {
        let mut xpub_options = options();
        xpub_options.pipe.incoming.hwm = 4;
        let (ours, theirs) = tokio::io::duplex(64 * 1024);
        let mut peer = CodecPeer::new(theirs);
        let (pipe, _user, task) = drive_stream(
            Stream::inproc(ours),
            SocketType::XPub,
            xpub_options,
            Role::Binder,
            &context(),
        );
        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.ready(SocketType::Sub).await;

        let mut burst = Vec::new();
        for n in 0..20 {
            let prefix = format!("topic-{n:02}");
            burst.extend_from_slice(
                &Command::Subscribe(prefix.as_bytes())
                    .encode()
                    .expect("SUBSCRIBE"),
            );
        }
        peer.send(&burst).await;

        // The queue fills to its mark and the rest waits in the session.
        within(async {
            while pipe.incoming().len() < 4 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(pipe.incoming().len(), 4);
        assert_eq!(pipe.incoming().dropped(), 0);

        for n in 0..20 {
            let notice = within(pipe.incoming().recv())
                .await
                .expect("a notification");
            let mut want = vec![0x01];
            want.extend_from_slice(format!("topic-{n:02}").as_bytes());
            assert_eq!(
                notice.frames()[0].as_slice(),
                want.as_slice(),
                "notification {n}"
            );
        }
        assert!(!task.is_finished());
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: a PLAIN client's handshake is 24/ZMTP-PLAIN's — a greeting
        /// naming PLAIN with `as-server` clear, `HELLO` carrying the username
        /// and password, `INITIATE` carrying the metadata `READY` would have
        /// carried under NULL, and nothing sent before the server's `WELCOME`.
        #[tokio::test]
        async fn a_plain_client_sends_hello_then_initiate() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Req,
                SocketOptions {
                    plain_username: Some("admin".to_owned()),
                    plain_password: Some("secret".to_owned()),
                    ..options()
                },
                Role::Connecter,
                &context(),
            );

            let greeting = Greeting::decode(&peer.read_greeting().await).expect("greeting");
            assert_eq!(greeting.mechanism, weida_zmtp::Mechanism::PLAIN);
            assert!(!greeting.as_server, "a client's as-server octet is zero");
            peer.greet_as(weida_zmtp::Mechanism::PLAIN, true).await;

            let (name, body) = peer.read_command_name().await;
            assert_eq!(name, "HELLO");
            assert_eq!(
                Command::decode(&body).expect("HELLO"),
                Command::Hello {
                    username: b"admin",
                    password: b"secret",
                }
            );

            peer.command(Command::Welcome).await;
            let (name, body) = peer.read_command_name().await;
            assert_eq!(name, "INITIATE", "the metadata travels in INITIATE");
            let Command::Initiate(metadata) = Command::decode(&body).expect("INITIATE") else {
                panic!("expected INITIATE");
            };
            assert_eq!(metadata.socket_type(), Some(SocketType::Req));

            peer.ready(SocketType::Rep).await;
            drop(peer);
            let _ = task.await;
        }
        /// Claim: a PLAIN server asks the ZAP handler about the credentials it
        /// was given, sends `WELCOME` on a 200, and records the user id the
        /// handler named — a per-connection fact, not an identity.
        #[tokio::test]
        async fn a_plain_server_asks_the_handler_and_keeps_its_answer() {
            let context = context();
            let (_asked, handler) = zap_handler(&context, |request| {
                assert_eq!(request.mechanism, "PLAIN");
                assert_eq!(request.domain, "test");
                assert_eq!(request.address, "127.0.0.1");
                let (username, password) = request.plain_credentials().expect("PLAIN credentials");
                assert_eq!(username, b"admin");
                assert_eq!(password, b"secret");
                ZapReply::allowed(request.request_id, "operator")
            })
            .await;

            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, user, task) = drive_session(
                ours,
                SocketType::Rep,
                SocketOptions {
                    plain_server: true,
                    zap_domain: "test".to_owned(),
                    ..options()
                },
                Role::Binder,
                &context,
            );

            let greeting = Greeting::decode(&peer.read_greeting().await).expect("greeting");
            assert_eq!(greeting.mechanism, weida_zmtp::Mechanism::PLAIN);
            assert!(greeting.as_server, "a server's as-server octet is one");
            peer.greet_as(weida_zmtp::Mechanism::PLAIN, false).await;

            peer.command(Command::Hello {
                username: b"admin",
                password: b"secret",
            })
            .await;
            let (name, _) = peer.read_command_name().await;
            assert_eq!(name, "WELCOME", "a 200 is a WELCOME");

            peer.command(Command::Initiate(
                Metadata::new().with_socket_type(SocketType::Req),
            ))
            .await;
            let (name, _) = peer.read_command_name().await;
            assert_eq!(name, "READY");

            weida_testkit::until(
                "the user id to be recorded",
                weida_testkit::EXPECTED,
                || user.get().is_some(),
            )
            .await;
            let recorded = user.get().expect("the user id was recorded");
            assert_eq!(recorded.as_str(), "operator");

            drop(peer);
            let _ = task.await;
            handler.abort();
        }
        /// Claim: a 400 refuses the connection **before any message flows** —
        /// the server answers `ERROR` with the handler's text instead of
        /// `WELCOME`, and the session ends with `EACCES`.
        #[tokio::test]
        async fn a_four_hundred_refuses_before_any_message() {
            let context = context();
            let (_asked, handler) = zap_handler(&context, |request| {
                ZapReply::refused(
                    request.request_id,
                    ZapStatus::AuthenticationFailure,
                    "unknown user",
                )
            })
            .await;

            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, user, task) = drive_session(
                ours,
                SocketType::Rep,
                SocketOptions {
                    plain_server: true,
                    zap_domain: "test".to_owned(),
                    ..options()
                },
                Role::Binder,
                &context,
            );

            peer.read_greeting().await;
            peer.greet_as(weida_zmtp::Mechanism::PLAIN, false).await;
            peer.command(Command::Hello {
                username: b"nobody",
                password: b"guess",
            })
            .await;

            let (name, body) = peer.read_command_name().await;
            assert_eq!(name, "ERROR", "a refusal is an ERROR, not a WELCOME");
            let Command::Error(reason) = Command::decode(&body).expect("ERROR") else {
                panic!("expected ERROR");
            };
            assert!(reason.contains("400"), "{reason}");

            let outcome = task.await.expect("the session task");
            let err = outcome.unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
            assert!(
                err.cause().contains("unknown user"),
                "the handler's reason must survive: {err}"
            );
            assert_eq!(user.get(), None, "nothing was authorized");
            handler.abort();
        }
        /// Claim: a server that must authorize and finds no handler refuses the
        /// connection instead of admitting one nobody checked — 27/ZAP's "the
        /// handler SHALL start before any server starts", enforced where it can
        /// be.
        #[tokio::test]
        async fn a_server_without_a_handler_admits_nobody() {
            let context = context();
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Rep,
                SocketOptions {
                    plain_server: true,
                    zap_domain: "test".to_owned(),
                    ..options()
                },
                Role::Binder,
                &context,
            );

            peer.read_greeting().await;
            peer.greet_as(weida_zmtp::Mechanism::PLAIN, false).await;
            peer.command(Command::Hello {
                username: b"admin",
                password: b"secret",
            })
            .await;

            let (name, _) = peer.read_command_name().await;
            assert_eq!(name, "ERROR");
            let err = task.await.expect("the session task").unwrap_err();
            assert_eq!(err.errno(), "ENOTSOCK", "{err}");
            assert!(err.cause().contains("zeromq.zap.01"), "{err}");
        }
        /// Claim: NULL authorizes too when a domain is configured — "NULL
        /// provides no security credentials but allows a server to filter bogus
        /// clients on the basis of IP address" — and the handler is told the
        /// address and the identity the peer announced, with no credential
        /// frames.
        #[tokio::test]
        async fn null_with_a_domain_asks_the_handler_about_the_address() {
            let context = context();
            let (asked, handler) = zap_handler(&context, |request| {
                ZapReply::allowed(request.request_id, "by-address")
            })
            .await;

            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, user, task) = drive_session(
                ours,
                SocketType::Rep,
                SocketOptions {
                    zap_domain: "global".to_owned(),
                    ..options()
                },
                Role::Binder,
                &context,
            );

            let greeting = Greeting::decode(&peer.read_greeting().await).expect("greeting");
            assert_eq!(
                greeting.mechanism,
                weida_zmtp::Mechanism::NULL,
                "a domain does not change the mechanism"
            );
            peer.greet(greeting::VERSION).await;
            peer.read_command_name().await;
            let ready = Command::Ready(
                Metadata::new()
                    .with_socket_type(SocketType::Req)
                    .with("Identity", b"client-9"),
            )
            .encode()
            .expect("READY");
            peer.send(&ready).await;

            weida_testkit::until(
                "the user id to be recorded",
                weida_testkit::EXPECTED,
                || user.get().is_some(),
            )
            .await;
            let recorded = user.get().expect("the user id was recorded");
            assert_eq!(recorded.as_str(), "by-address");

            let request = asked
                .lock()
                .expect("asked")
                .first()
                .cloned()
                .expect("one request");
            assert_eq!(request.mechanism, "NULL");
            assert!(
                request.credentials.is_empty(),
                "NULL has no credentials to send"
            );
            assert_eq!(request.address, "127.0.0.1");
            assert_eq!(request.identity, b"client-9".to_vec());
            assert_eq!(request.domain, "global");

            drop(peer);
            let _ = task.await;
            handler.abort();
        }
        /// Claim: without a domain, NULL does not authorize at all — libzmq's
        /// "when the ZAP domain is empty, which is the default, ZAP
        /// authentication is disabled" — so a handler that would refuse
        /// everything is never asked.
        #[tokio::test]
        async fn null_without_a_domain_does_not_authorize() {
            let context = context();
            let (asked, handler) = zap_handler(&context, |request| {
                ZapReply::refused(
                    request.request_id,
                    ZapStatus::AuthenticationFailure,
                    "would refuse",
                )
            })
            .await;

            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, user, task) =
                drive_session(ours, SocketType::Pull, options(), Role::Binder, &context);

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_command_name().await;
            peer.ready(SocketType::Push).await;

            // The handshake completed and a message flows, which it could not
            // have done if the refusing handler had been consulted.
            let message = Multipart::single(Message::from("work")).encode();
            peer.send(&message).await;
            let arrived = within(pipe.incoming().recv()).await.expect("a message");
            assert_eq!(arrived.frames()[0].as_slice(), b"work");
            assert_eq!(user.get(), None, "nothing was authorized");
            assert!(asked.lock().expect("asked").is_empty(), "nobody was asked");

            drop(peer);
            let _ = task.await;
            handler.abort();
        }
        /// Claim: the greeting we send is the specification's 64 octets, exactly
        /// as the codec encodes them, and our `READY` carries `Socket-Type` and —
        /// when `ZMQ_ROUTING_ID` is set — `Identity`.
        #[tokio::test]
        async fn the_handshake_sends_the_greeting_and_a_ready() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Req,
                SocketOptions {
                    routing_id: Some(RoutingId::new(b"client-7").expect("id")),
                    ..options()
                },
                Role::Connecter,
                &context(),
            );

            // Byte-for-byte: what the codec says a NULL 3.1 greeting is.
            assert_eq!(peer.read_greeting().await, Greeting::null().encode());
            peer.greet(greeting::VERSION).await;

            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            let metadata = match Command::decode(&body).expect("a command") {
                Command::Ready(metadata) => metadata,
                other => panic!("expected READY, got {}", other.name()),
            };
            assert_eq!(metadata.socket_type(), Some(SocketType::Req));
            assert_eq!(metadata.get("Identity"), Some(b"client-7".as_slice()));

            peer.ready(SocketType::Rep).await;
            drop(peer);
            // The peer's close ends the session, which is not an error worth
            // asserting beyond its shape.
            let _ = task.await.expect("the session task");
        }
        /// Claim: messages cross in both directions, multipart intact, with MORE
        /// on every frame but the last.
        #[tokio::test]
        async fn messages_cross_in_both_directions() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, _user, task) = drive_session(
                ours,
                SocketType::Dealer,
                options(),
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            peer.ready(SocketType::Router).await;

            // Outbound: a two-frame message goes out as MORE then last.
            let envelope =
                Multipart::new(vec![Message::empty(), Message::from("hello")]).expect("frames");
            pipe.outgoing()
                .send(envelope.clone())
                .await
                .expect("queued");
            let (first, body) = peer.read_frame().await;
            assert_eq!(first, FrameKind::Message { more: true });
            assert!(body.is_empty(), "the delimiter frame is empty");
            let (second, body) = peer.read_frame().await;
            assert_eq!(second, FrameKind::Message { more: false });
            assert_eq!(body, b"hello");

            // Inbound: the same shape arrives as one Multipart.
            peer.send(&envelope.encode()).await;
            let arrived = weida_testkit::within(
                "the delivery",
                Duration::from_secs(5),
                pipe.incoming().recv(),
            )
            .await
            .expect("a message");
            assert_eq!(arrived, envelope);

            drop(peer);
            let _ = task.await.expect("the session task");
        }
        /// Claim: a ZMTP 3.0 peer is accepted by downgrading, and **never
        /// receives a PING** even with the heartbeat configured — the interop
        /// lesson, where a command a 3.0 peer does not know ends the connection.
        #[tokio::test]
        async fn a_three_zero_peer_is_accepted_and_never_pinged() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, _user, task) = drive_session(
                ours,
                SocketType::Push,
                SocketOptions {
                    heartbeat_ivl: Some(Duration::from_millis(10)),
                    ..options()
                },
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(Version { major: 3, minor: 0 }).await;
            let (kind, _) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command, "READY is still a command");
            peer.ready(SocketType::Pull).await;

            // Margin: fifteen heartbeat intervals (150 ms over a 10 ms
            // ZMQ_HEARTBEAT_IVL). The phenomenon is a PING that must never come,
            // so the interval count is the margin: a session that sent one would
            // have sent fifteen. The message afterwards proves the silence is the
            // version's doing rather than a stalled session.
            tokio::time::sleep(Duration::from_millis(150)).await;
            pipe.outgoing()
                .send(Multipart::single("still alive"))
                .await
                .expect("queued");
            let (kind, body) = peer.read_frame().await;
            assert_eq!(
                kind,
                FrameKind::Message { more: false },
                "a 3.0 peer must receive no PING"
            );
            assert_eq!(body, b"still alive");

            drop(peer);
            let _ = task.await.expect("the session task");
        }
        /// Claim: a 3.1 peer does get `PING`s on `ZMQ_HEARTBEAT_IVL`, carrying
        /// the `ZMQ_HEARTBEAT_TTL` hint in deciseconds.
        ///
        /// Margin: none is needed in the assertion — the read below waits for
        /// the PING rather than sampling for it, so a 10 ms interval only sets
        /// how soon the test finishes, and a heartbeat that never came would
        /// hang the read until the harness kills it rather than pass.
        #[tokio::test]
        async fn a_three_one_peer_is_pinged_on_the_interval() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Push,
                SocketOptions {
                    heartbeat_ivl: Some(Duration::from_millis(10)),
                    heartbeat_timeout: Some(Duration::from_secs(30)),
                    heartbeat_ttl: Some(Duration::from_secs(3)),
                    ..options()
                },
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            peer.ready(SocketType::Pull).await;

            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            match Command::decode(&body).expect("a command") {
                Command::Ping { ttl, context } => {
                    assert_eq!(ttl, 30, "3 s is 30 deciseconds on the wire");
                    assert_eq!(context, PING_CONTEXT);
                }
                other => panic!("expected PING, got {}", other.name()),
            }

            drop(peer);
            let _ = task.await.expect("the session task");
        }
        /// Claim: our session answers a peer's `PING` with a `PONG` that echoes
        /// the context, whatever else is going on.
        #[tokio::test]
        async fn a_ping_is_answered_with_a_pong_that_echoes() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Rep,
                options(),
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            peer.ready(SocketType::Req).await;

            let ping = Command::Ping {
                ttl: 100,
                context: b"abcd",
            }
            .encode()
            .expect("PING");
            peer.send(&ping).await;

            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            match Command::decode(&body).expect("a command") {
                Command::Pong { context } => assert_eq!(context, b"abcd"),
                other => panic!("expected PONG, got {}", other.name()),
            }

            drop(peer);
            let _ = task.await.expect("the session task");
        }
        /// Claim: an incompatible socket type is refused with an `ERROR` on the
        /// wire and then a close — the specification's own way of saying no — and
        /// the session reports `ENOCOMPATPROTO`.
        #[tokio::test]
        async fn an_incompatible_socket_type_gets_an_error_then_a_close() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Req,
                options(),
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            // A PUB may not talk to a REQ.
            peer.ready(SocketType::Pub).await;

            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            match Command::decode(&body).expect("a command") {
                Command::Error(reason) => {
                    assert!(reason.contains("PUB"), "{reason}");
                    assert!(reason.contains("REQ"), "{reason}");
                }
                other => panic!("expected ERROR, got {}", other.name()),
            }

            let err = task.await.expect("the session task").unwrap_err();
            assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
        }
        /// Claim: a `READY` with no `Socket-Type` is refused, because no
        /// compatibility check is possible against a peer that will not say what
        /// it is.
        #[tokio::test]
        async fn a_ready_without_a_socket_type_is_refused() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Pull,
                options(),
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            peer.send(&Command::Ready(Metadata::new()).encode().expect("READY"))
                .await;

            let (_, body) = peer.read_frame().await;
            assert!(matches!(
                Command::decode(&body).expect("a command"),
                Command::Error(_)
            ));
            let err = task.await.expect("the session task").unwrap_err();
            assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
        }
        /// Claim: an `ERROR` from the peer is fatal, at the handshake and after
        /// it — "the peer SHALL treat an incoming ERROR command as fatal".
        #[tokio::test]
        async fn an_error_from_the_peer_is_fatal() {
            for during_handshake in [true, false] {
                let (ours, theirs) = pair().await;
                let mut peer = CodecPeer::new(theirs);
                let (_pipe, _user, task) = drive_session(
                    ours,
                    SocketType::Pull,
                    options(),
                    Role::Connecter,
                    &context(),
                );

                peer.read_greeting().await;
                peer.greet(greeting::VERSION).await;
                peer.read_frame().await;
                if !during_handshake {
                    peer.ready(SocketType::Push).await;
                }
                peer.send(&Command::Error("go away").encode().expect("ERROR"))
                    .await;

                let err = task.await.expect("the session task").unwrap_err();
                assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
                assert!(err.cause().contains("go away"), "{err}");
            }
        }
        /// Claim: `ZMQ_MAXMSGSIZE` is enforced on the wire from the declared
        /// length, so a peer cannot make us allocate by announcing a huge frame.
        #[tokio::test]
        async fn an_oversized_declaration_ends_the_connection() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Pull,
                SocketOptions {
                    max_message_size: 1024,
                    ..options()
                },
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            peer.ready(SocketType::Push).await;

            // A long header declaring 2^31 octets, and no body at all.
            let mut header = vec![0x02u8];
            header.extend_from_slice(&(1u64 << 31).to_be_bytes());
            peer.send(&header).await;

            let err = task.await.expect("the session task").unwrap_err();
            assert_eq!(err.errno(), "EMSGSIZE", "{err}");
        }
        /// Claim: the session is the seam the engine expects — an `Engine`
        /// carrying `ZmtpSession` completes the handshake against a real ZeroMQ
        /// peer driven by the codec, and messages then flow through the engine's
        /// pipe.
        #[tokio::test]
        async fn the_engine_and_the_session_speak_zmtp_end_to_end() {
            let ctx = Context::new(ContextConfig::default()).expect("context");
            let engine = Engine::new(
                &ctx,
                options(),
                Arc::new(ZmtpSession::new(SocketType::Push)) as Arc<dyn Session>,
            )
            .expect("engine");

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let port = listener.local_addr().expect("addr").port();
            let peer_task = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut peer = CodecPeer::new(stream);
                assert_eq!(peer.read_greeting().await, Greeting::null().encode());
                peer.greet(greeting::VERSION).await;
                let (kind, body) = peer.read_frame().await;
                assert_eq!(kind, FrameKind::Command);
                match Command::decode(&body).expect("a command") {
                    Command::Ready(metadata) => {
                        assert_eq!(metadata.socket_type(), Some(SocketType::Push));
                    }
                    other => panic!("expected READY, got {}", other.name()),
                }
                peer.ready(SocketType::Pull).await;
                let (kind, body) = peer.read_frame().await;
                assert_eq!(kind, FrameKind::Message { more: false });
                body
            });

            let endpoint = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
            let peer_id = engine.connect(&endpoint).expect("connect");
            let pipe = engine.peer(peer_id).expect("peer").pipe;
            pipe.outgoing()
                .send(Multipart::single("through the engine"))
                .await
                .expect("queued");

            let delivered = within(peer_task).await.expect("the peer task");
            assert_eq!(delivered, b"through the engine");
        }
        /// Claim: the octets a CURVE server puts on the wire are 26/CURVEZMQ's,
        /// and nothing after the `READY` is in clear text.
        ///
        /// Driven from the other side by this crate's own client half over a raw
        /// socket, so what is asserted is the wire and not an agreement between
        /// two copies of the same state machine.
        #[tokio::test]
        async fn a_curve_server_seals_everything_after_its_ready() {
            let context = context();
            let (server_options, server_key) = curve_server_options("");
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, _user, task) = drive_session(
                ours,
                SocketType::Pair,
                server_options,
                Role::Binder,
                &context,
            );

            let (public, secret) = crate::curve::keypair();
            let mut transport =
                curve_handshake_as_client(&mut peer, server_key, public, &secret).await;

            // Outbound: the message is a `MESSAGE` whose body is a command body
            // behind a **message** frame header, which is what libzmq sends and
            // accepts (`tests/interop_libzmq.rs`), and the plaintext is nowhere
            // in the octets. The outer MORE is always zero: the real one is the
            // flags octet inside the box.
            pipe.outgoing()
                .send(Multipart::single("the quiet part"))
                .await
                .expect("queued");
            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Message { more: false });
            assert!(body.starts_with(b"\x07MESSAGE"), "named MESSAGE");
            assert!(
                !body.windows(14).any(|window| window == b"the quiet part"),
                "the payload travelled in clear text"
            );
            let (flags, payload) = transport.open_message(&body).expect("open");
            assert_eq!(flags, 0, "one frame, so no MORE");
            assert_eq!(payload, b"the quiet part");

            // A multipart message: MORE travels inside the box under CURVE, so a
            // single frame would not exercise it.
            let mut multipart = Multipart::single("first");
            multipart.push("second");
            pipe.outgoing().send(multipart).await.expect("queued");
            for (expected_flags, expected_body) in [
                (curve_layout::MESSAGE_FLAG_MORE, &b"first"[..]),
                (0, &b"second"[..]),
            ] {
                let (kind, body) = peer.read_frame().await;
                assert_eq!(kind, FrameKind::Message { more: false });
                let (flags, payload) = transport.open_message(&body).expect("open");
                assert_eq!(flags, expected_flags);
                assert_eq!(payload, expected_body);
            }

            // Inbound: a sealed message reaches the application, and a plaintext
            // one does not.
            peer.send(&transport.seal_frame(0, b"the other way").expect("seal"))
                .await;
            let arrived = within(pipe.incoming().recv()).await.expect("a message");
            assert_eq!(arrived.frames()[0].as_slice(), b"the other way");

            // A frame in clear text is refused — not because of its frame kind,
            // which is the same kind a sealed `MESSAGE` uses, but because its
            // body is not a `MESSAGE` at all and no box was opened.
            peer.send(&Multipart::single("in the open").encode()).await;
            let outcome = within(task).await.expect("the task");
            let err = outcome.unwrap_err();
            assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
            assert!(err.cause().contains("CURVE"), "{err}");
        }
        /// Claim: the CURVE credential a ZAP handler is given is the peer's
        /// long-term public key — the one that never travelled in clear text —
        /// and 26/CURVEZMQ's second and third security models are the handler's
        /// answer to it: an allowed key is admitted, a 200 that names a user is
        /// kept per connection, and a key that is not in the table is refused
        /// before any message flows.
        #[tokio::test]
        async fn a_curve_server_hands_the_peers_key_to_the_handler() {
            let context = context();
            let (server_options, server_key) = curve_server_options("realm");
            let (allowed_public, allowed_secret) = crate::curve::keypair();

            // Model 3: a table with one entry per client, whose 200 also names
            // the user that key belongs to. Model 2 is the same handler with one
            // shared entry, and this one's refusal path is what both do to a key
            // they do not hold.
            let allowed = allowed_public;
            let (asked, handler) = zap_handler(&context, move |request| {
                assert_eq!(request.mechanism, "CURVE");
                assert_eq!(request.domain, "realm");
                let key = crate::curve::CurvePublicKey::parse(&request.credentials[0])
                    .expect("a 32-octet key");
                if key == allowed {
                    ZapReply::allowed(request.request_id, "client-a")
                } else {
                    ZapReply::refused(
                        request.request_id,
                        ZapStatus::AuthenticationFailure,
                        "not on the list",
                    )
                }
            })
            .await;

            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, user, task) = drive_session(
                ours,
                SocketType::Pair,
                server_options.clone(),
                Role::Binder,
                &context,
            );
            let mut transport =
                curve_handshake_as_client(&mut peer, server_key, allowed_public, &allowed_secret)
                    .await;

            peer.send(&transport.seal_frame(0, b"authorized").expect("seal"))
                .await;
            let arrived = within(pipe.incoming().recv()).await.expect("a message");
            assert_eq!(arrived.frames()[0].as_slice(), b"authorized");
            assert_eq!(
                user.get().expect("a user id").as_str(),
                "client-a",
                "model 3: access granted according to an authenticated identity"
            );
            {
                let asked = asked.lock().expect("asked");
                assert_eq!(asked.len(), 1);
                assert_eq!(
                    asked[0].credentials,
                    vec![allowed_public.as_bytes().to_vec()],
                    "one frame, the peer's 32-octet long-term key"
                );
            }
            assert_eq!(
                crate::curve::security_model(&server_options, &context),
                Some(crate::curve::SecurityModel::CheckedByHandler)
            );
            drop(peer);
            let _ = task.await;

            // A key the table does not hold: the handshake ends with the
            // handler's own text, and no message ever flows.
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, user, task) = drive_session(
                ours,
                SocketType::Pair,
                server_options,
                Role::Binder,
                &context,
            );
            let (stranger_public, stranger_secret) = crate::curve::keypair();
            let greeting = Greeting::decode(&peer.read_greeting().await).expect("greeting");
            assert_eq!(greeting.mechanism, weida_zmtp::Mechanism::CURVE);
            peer.greet_as(weida_zmtp::Mechanism::CURVE, false).await;
            let mut client = CurveClient::new(stranger_public, &stranger_secret, server_key);
            peer.send(&client.hello().expect("HELLO")).await;
            let (_, body) = peer.read_frame().await;
            client.read_welcome(&body).expect("read WELCOME");
            peer.send(&client.initiate(&Metadata::new()).expect("INITIATE"))
                .await;

            // The refusal is an ERROR carrying the handler's status and text,
            // sent before the READY that would have completed the handshake.
            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            let Command::Error(reason) = Command::decode(&body).expect("a command") else {
                panic!("expected an ERROR");
            };
            assert!(reason.contains("not on the list"), "{reason}");
            let outcome = within(task).await.expect("the task");
            let err = outcome.unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
            assert_eq!(user.get(), None, "a refusal names no user");
            assert!(
                pipe.incoming().try_recv().is_err(),
                "nothing flowed under a refused connection"
            );
            handler.abort();
        }
        /// Claim: a client that does not hold the server's public key cannot get
        /// past `HELLO`, and is told so rather than left waiting.
        #[tokio::test]
        async fn a_curve_client_with_the_wrong_server_key_is_refused() {
            let context = context();
            let (server_options, _) = curve_server_options("");
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Pair,
                server_options,
                Role::Binder,
                &context,
            );

            peer.read_greeting().await;
            peer.greet_as(weida_zmtp::Mechanism::CURVE, false).await;
            // A HELLO whose signature box is sealed to another server's key: the
            // octets are 26/CURVEZMQ's and the box does not open.
            let (elsewhere, _) = crate::curve::keypair();
            let (public, secret) = crate::curve::keypair();
            let mut client = CurveClient::new(public, &secret, elsewhere);
            peer.send(&client.hello().expect("HELLO")).await;

            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            let Command::Error(reason) = Command::decode(&body).expect("a command") else {
                panic!("expected an ERROR rather than silence");
            };
            assert!(reason.contains("signature box"), "{reason}");

            let outcome = within(task).await.expect("the task");
            let err = outcome.unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
        }
        /// Claim: a `READY` with more properties than any peer has is refused in
        /// the handshake, from the codec's own cap, instead of being parsed into
        /// a list a peer sized.
        #[tokio::test]
        async fn a_ready_with_too_many_properties_is_refused() {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, _user, task) = drive_session(
                ours,
                SocketType::Pull,
                options(),
                Role::Connecter,
                &context(),
            );

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            let mut body = b"\x05READY".to_vec();
            for _ in 0..(weida_zmtp::MAX_PROPERTIES + 1) {
                body.extend_from_slice(b"\x01a\x00\x00\x00\x00");
            }
            peer.send(&frame::encode(FrameKind::Command, &body)).await;

            let err = within(task).await.expect("the task").unwrap_err();
            assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
            assert!(err.cause().contains("properties"), "{err}");
        }
        /// Claim: **a CURVE server asks a bound ZAP handler about a client even
        /// when no `ZMQ_ZAP_DOMAIN` is set**, which is libzmq's rule (measured in
        /// `tests/interop_libzmq.rs`). A deployment that bound a handler to
        /// allow-list client keys and never set a domain is not silently open: the
        /// handler's 400 refuses the client, and it was asked once.
        #[tokio::test]
        async fn a_curve_server_asks_a_bound_handler_without_a_domain() {
            let context = context();
            let (server_options, server_key) = curve_server_options("");
            let (asked, handler) = zap_handler(&context, |request| {
                ZapReply::refused(
                    request.request_id,
                    ZapStatus::AuthenticationFailure,
                    "not on the list",
                )
            })
            .await;

            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (pipe, user, task) = drive_session(
                ours,
                SocketType::Pair,
                server_options.clone(),
                Role::Binder,
                &context,
            );
            let (public, secret) = crate::curve::keypair();
            peer.read_greeting().await;
            peer.greet_as(weida_zmtp::Mechanism::CURVE, false).await;
            let mut client = CurveClient::new(public, &secret, server_key);
            peer.send(&client.hello().expect("HELLO")).await;
            let (_, body) = peer.read_frame().await;
            client.read_welcome(&body).expect("read WELCOME");
            peer.send(&client.initiate(&Metadata::new()).expect("INITIATE"))
                .await;

            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            let Command::Error(reason) = Command::decode(&body).expect("a command") else {
                panic!("expected an ERROR, not a READY: the handler was not asked");
            };
            assert!(reason.contains("not on the list"), "{reason}");
            let err = within(task).await.expect("the task").unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
            assert_eq!(user.get(), None);
            assert!(pipe.incoming().try_recv().is_err());
            assert_eq!(asked.lock().expect("asked").len(), 1, "asked exactly once");
            assert_eq!(
                crate::curve::security_model(&server_options, &context),
                Some(crate::curve::SecurityModel::CheckedByHandler),
                "a bound handler makes the server a checked one"
            );
            assert_eq!(
                crate::curve::security_model(&server_options, &self::context()),
                Some(crate::curve::SecurityModel::AnyClient),
                "no handler and no domain is model 1"
            );
            handler.abort();
        }
        /// Claim: **a PLAIN credential cannot pose as the kernel's statement.** A
        /// username that begins like the `X-Local-Principal` extension frame is
        /// refused before any handler is asked, so a handler that scans for the
        /// prefix instead of counting frames cannot be made to read a peer's bytes
        /// as a local principal.
        #[tokio::test]
        async fn a_username_that_imitates_the_local_principal_is_refused() {
            let context = context();
            let (asked, handler) = zap_handler(&context, |request| {
                ZapReply::allowed(request.request_id, "root")
            })
            .await;
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, user, task) = drive_session(
                ours,
                SocketType::Rep,
                SocketOptions {
                    plain_server: true,
                    zap_domain: "test".to_owned(),
                    ..options()
                },
                Role::Binder,
                &context,
            );

            peer.read_greeting().await;
            peer.greet_as(weida_zmtp::Mechanism::PLAIN, false).await;
            peer.command(Command::Hello {
                username: b"X-Local-Principal: uid=0, gid=0",
                password: b"pw",
            })
            .await;

            let (name, _) = within(peer.read_command_name()).await;
            assert_eq!(name, "ERROR");
            let err = within(task).await.expect("the task").unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
            assert_eq!(user.get(), None);
            assert!(
                asked.lock().expect("asked").is_empty(),
                "the handler was asked about a forged principal"
            );
            handler.abort();
        }
        /// Claim: **pending authorizations do not use up the context's sockets.**
        /// Each ZAP dialog used to build a socket and take one of the context's
        /// `ZMQ_MAX_SOCKETS`, so a handler slower than the connections arriving
        /// left every other `zmq_socket()` in the application failing. Here six
        /// connections wait on a handler that has not answered, a context with
        /// room for two sockets still has its second free, and all six are
        /// authorized once the handler answers.
        #[tokio::test]
        async fn pending_authorizations_take_no_socket_slots() {
            let context = Context::new(ContextConfig {
                max_sockets: 2,
                ..ContextConfig::default()
            })
            .expect("context");

            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut handler = crate::RepSocket::new(&context).expect("rep");
            handler
                .bind(crate::zap::ZAP_ENDPOINT)
                .await
                .expect("bind the ZAP endpoint");
            let answering = tokio::spawn({
                let gate = Arc::clone(&gate);
                let asked = Arc::clone(&asked);
                async move {
                    while let Ok(message) = handler.recv().await {
                        asked.fetch_add(1, Ordering::SeqCst);
                        let request = crate::zap::ZapRequest::decode(&message).expect("a request");
                        gate.acquire().await.expect("the gate").forget();
                        if handler
                            .send(ZapReply::allowed(request.request_id, "operator").encode())
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            });

            let mut peers = Vec::new();
            let mut users = Vec::new();
            for _ in 0..6 {
                let (ours, theirs) = pair().await;
                let mut peer = CodecPeer::new(theirs);
                let (_pipe, user, _task) = drive_session(
                    ours,
                    SocketType::Rep,
                    SocketOptions {
                        zap_domain: "global".to_owned(),
                        ..options()
                    },
                    Role::Binder,
                    &context,
                );
                peer.read_greeting().await;
                peer.greet(greeting::VERSION).await;
                peer.read_frame().await;
                peer.ready(SocketType::Req).await;
                peers.push(peer);
                users.push(user);
            }

            // The handler holds the first request; the other five dialogs wait
            // behind it. Margin: five connections need microseconds to dial; the
            // phenomenon is the absence of a free slot, which a dialog that took
            // one would show at once.
            within(async {
                while asked.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let slot = context
                .open_socket()
                .expect("a socket slot is free while authorizations are pending");
            drop(slot);

            gate.add_permits(6);
            within(async {
                while users.iter().any(|user| user.get().is_none()) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            assert_eq!(asked.load(Ordering::SeqCst), 6);
            answering.abort();
        }

        /// Claim: **a message that is started and not finished does not hold the
        /// connection for ever**, whether the heartbeat is on or not (it is off
        /// here, which is the default): a peer that announces a large message
        /// and sends a few kilobytes of it ends the connection.
        #[tokio::test]
        async fn a_message_that_stops_arriving_ends_the_connection() {
            let (ours, mut theirs) = tokio::io::duplex(1 << 20);
            let mut wire = Wire::new(ours, MessageLimits::default())
                .with_message_ivl(Duration::from_millis(200));
            let options = options();
            let pipe = Pipe::new(options.pipe);
            let subscriptions = Arc::new(Subscriptions::new(
                crate::subscriptions::DEFAULT_MAX_SUBSCRIPTIONS,
                crate::subscriptions::DEFAULT_MAX_SUBSCRIPTION_BYTES,
            ));
            let exec = Exec::current().expect("ambient reactor");

            let mut head = vec![0x02u8];
            head.extend_from_slice(&(900u64 * 1024).to_be_bytes());
            head.extend_from_slice(&[0u8; 2048]);
            theirs.write_all(&head).await.expect("write");

            let started = std::time::Instant::now();
            let outcome = within(pump(
                &mut wire,
                SocketType::Pull,
                &subscriptions,
                &pipe,
                &options,
                &exec,
                greeting::VERSION,
            ))
            .await;
            let err = outcome.unwrap_err();
            assert_eq!(err.errno(), "ETIMEDOUT", "{err}");
            // The claim is that the session ended, by ETIMEDOUT, instead of
            // waiting for the rest of the message; how long that took is
            // printed, not asserted, because the session times itself on the
            // wall clock and a loaded machine stretches it.
            eprintln!(
                "a stalled message ended the session after {:?}",
                started.elapsed()
            );
        }
        /// Claim: **a peer that stops reading cannot hold a session** with the
        /// heartbeat on. The write that the peer's full window blocks is an arm
        /// of the session's loop rather than an await that stops everything, so
        /// the heartbeat still runs and ends the connection by `ETIMEDOUT`.
        #[tokio::test]
        async fn a_peer_that_stops_reading_cannot_hold_the_session() {
            let (pipe, task, _peer) = handshaken_over_a_duplex(
                SocketOptions {
                    heartbeat_ivl: Some(Duration::from_millis(50)),
                    heartbeat_timeout: Some(Duration::from_millis(200)),
                    ..options()
                },
                64 * 1024,
            )
            .await;

            // Far more than the stream's window: the writer blocks for good.
            for _ in 0..16 {
                pipe.outgoing()
                    .send(Multipart::single(vec![7u8; 64 * 1024]))
                    .await
                    .expect("queued");
            }
            let err = within(task).await.expect("the task").unwrap_err();
            assert_eq!(err.errno(), "ETIMEDOUT", "{err}");
        }
        /// Claim: **our own traffic does not keep a vanished peer alive.** The
        /// heartbeat timer is not rebuilt on every message, so an application
        /// that publishes every few milliseconds to a peer that has gone quiet
        /// still finds the connection ended.
        #[tokio::test]
        async fn a_busy_session_still_times_out_a_silent_peer() {
            let (pipe, task, _peer) = handshaken_over_a_duplex(
                SocketOptions {
                    heartbeat_ivl: Some(Duration::from_millis(100)),
                    heartbeat_timeout: Some(Duration::from_millis(500)),
                    ..options()
                },
                1 << 20,
            )
            .await;

            let outgoing = pipe.outgoing();
            let sender = tokio::spawn(async move {
                while outgoing.send(Multipart::single("tick")).await.is_ok() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            let err = within(task).await.expect("the task").unwrap_err();
            assert_eq!(err.errno(), "ETIMEDOUT", "{err}");
            sender.abort();
        }

        /// Claim: **arbitrary octets never panic the reader** — they are a
        /// message, a command or an error, and the stream ends — in the clear and
        /// under a CURVE session, where every frame has to open as a box.
        #[tokio::test]
        async fn arbitrary_octets_are_never_a_panic() {
            let mut rng = Rng(0xD1B5_4A32_D192_ED03);
            for round in 0..3000 {
                let len = rng.below(700);
                let mut data = rng.bytes(len);
                // Bias a share of the streams towards plausible frame headers, so
                // that the parser is exercised beyond its first refusal.
                if round % 3 == 0 {
                    for at in (0..data.len()).step_by(1 + rng.below(40)) {
                        data[at] = [0x00, 0x01, 0x02, 0x04, 0x06][rng.below(5)];
                    }
                }
                let sealed = round % 2 == 0;
                let drip = Drip {
                    data,
                    at: 0,
                    chunk: 1 + rng.below(64),
                };
                let mut wire = Wire::new(drip, MessageLimits::new(4096, 8));
                if sealed {
                    wire.encrypt_with(curve_transports().1);
                }
                // Every item consumes at least two octets, so this ends.
                for _ in 0..=len {
                    if within(wire.read_next()).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}
