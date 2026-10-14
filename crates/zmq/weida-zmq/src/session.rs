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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use weida_runtime::Exec;
use weida_zmtp::{
    Command, CommandError, FrameKind, Greeting, GreetingError, Metadata, SocketType, Version,
    curve as curve_layout, frame, greeting,
};

use crate::context::Context;
use crate::curve::{
    COOKIE_LIFETIME, CurveClient, CurvePublicKey, CurveSecretKey, CurveServer, CurveTransport,
};
use crate::engine::{Connection, Role, Session, SessionFuture};
use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::message::{Decoded, Message, MessageLimits, Multipart};
use crate::options::{Security, SocketOptions};
use crate::pipe::{Queue, Sent};
use crate::subscriptions::{self, SubscriptionForm, Subscriptions};
use crate::zap::{self, ZapRequest, ZapUserId};

/// Read buffer growth step. A frame header is at most nine octets and a
/// command body a few hundred, so the interesting case is a payload, which is
/// read straight into this buffer.
const CHUNK: usize = 16 * 1024;

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
                write_subscription(&mut wire, options.subscription_form, true, &prefix).await?;
            }
        }
        for message in carried {
            wire.write_message(&message).await?;
        }
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
    wire.write_all(&greeting.encode()).await?;

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
            PeerMetadata::Command(expect_command(wire, "READY").await?)
        }
        Security::PlainClient => {
            // `C:HELLO(user,pass) -> S:WELCOME|S:ERROR`, then
            // `C:INITIATE(metadata) -> S:READY|S:ERROR`.
            wire.write_command(&Command::Hello {
                username: options.plain_username.as_deref().unwrap_or("").as_bytes(),
                password: options.plain_password.as_deref().unwrap_or("").as_bytes(),
            })
            .await?;
            let body = expect_command(wire, "WELCOME").await?;
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
            PeerMetadata::Command(expect_command(wire, "READY").await?)
        }
        Security::PlainServer => {
            let body = expect_command(wire, "HELLO").await?;
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
            let peer_metadata = expect_command(wire, "INITIATE").await?;
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
            let body = expect_command(wire, "WELCOME").await?;
            refused(&body)?;
            client.read_welcome(&body)?;
            let initiate = client.initiate(&ours_metadata)?;
            wire.write_handshake(&initiate).await?;
            let body = expect_command(wire, "READY").await?;
            refused(&body)?;
            let metadata = client.read_ready(&body)?;
            // Everything after this point is a MESSAGE box.
            wire.encrypt_with(client.into_transport()?);
            PeerMetadata::Dictionary(metadata)
        }
        Security::CurveServer => {
            let mut server = CurveServer::new(curve_secret(options)?, COOKIE_LIFETIME);
            let body = expect_command(wire, "HELLO").await?;
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
            let body = expect_command(wire, "INITIATE").await?;
            refused(&body)?;
            let opened = match server.read_initiate(&body) {
                Ok(opened) => opened,
                Err(e) => {
                    wire.refuse("the INITIATE was refused").await;
                    return Err(e);
                }
            };
            // The domain is the switch between 26/CURVEZMQ's security
            // models: without one, "the server does not check client keys at
            // all" and knowing `S` was the authorization. With one, the
            // credential is `C` — "a 32-byte long-term public key of the peer
            // being authenticated", the key that never travelled in clear
            // text — and the identity frame is the peer's own `Identity`
            // property, out of the same box.
            if options.authorizes() {
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
    Command(Vec<u8>),
    /// The property dictionary out of an `INITIATE` or `READY` box.
    Dictionary(Vec<u8>),
}

/// Reports a peer that answered a handshake command with `ERROR`.
///
/// Every mechanism may: 26/CURVEZMQ has `ERROR` in its own grammar, and
/// 37/ZMTP calls an incoming one fatal. A CURVE command can never be
/// mistaken for one, because no CURVE command is named `ERROR`.
fn refused(body: &[u8]) -> Result<()> {
    match Command::decode(body) {
        Ok(Command::Error(reason)) => Err(Error::ENOCOMPATPROTO(
            format!("the peer refused the handshake: {}", sanitize(reason)).into(),
        )),
        _ => Ok(()),
    }
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

/// Reads the next command body, refusing a message: nothing may precede a
/// handshake command, so early data is a protocol violation.
async fn expect_command<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    what: &str,
) -> Result<Vec<u8>> {
    match wire.read_next().await? {
        Incoming::Command(body) => Ok(body),
        Incoming::Message(_) => Err(Error::ENOCOMPATPROTO(
            format!("the peer sent a message before its {what}").into(),
        )),
    }
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
/// Three things happen in one loop, and they are one loop on purpose: reading
/// the wire, draining this peer's outgoing queue onto it, and beating the
/// heartbeat. A separate reader and writer task would need a lock over the
/// same socket and would not make either direction faster.
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
    let outgoing: std::sync::Arc<Queue> = pipe.outgoing();
    let incoming: std::sync::Arc<Queue> = pipe.incoming();
    let mut liveness = Liveness::new(options, version);

    loop {
        tokio::select! {
            arrived = wire.read_next() => {
                liveness.saw_traffic();
                match arrived? {
                    Incoming::Message(message) => {
                        if publishes(ours) {
                            // A publisher never hands an inbound message to
                            // its application: "PUB SHALL silently discard
                            // any messages that subscribers send it". The one
                            // thing such a message may be is a subscription
                            // in ZMTP 2.0's form, which is how a 3.0 peer
                            // asks.
                            apply_message_form(ours, subscriptions, &incoming, options, &message)
                                .await?;
                        } else if incoming.send(message).await? == Sent::Dropped {
                            tracing::trace!(
                                "dropped an inbound message: the queue is at its high-water mark"
                            );
                        }
                    }
                    Incoming::Command(body) => {
                        answer(wire, ours, subscriptions, &incoming, options, &body).await?;
                    }
                }
            }
            queued = outgoing.recv() => match queued {
                Ok(message) => {
                    // A subscribing socket's outgoing messages *are* its
                    // subscriptions, in the `%x01`/`%x00` form the API uses;
                    // which form goes on the wire is this socket's choice.
                    match subscribing_form(ours, &message) {
                        Some((subscribe, prefix)) => {
                            write_subscription(
                                wire,
                                options.subscription_form,
                                subscribe,
                                &prefix,
                            )
                            .await?;
                        }
                        None => wire.write_message(&message).await?,
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
                wire.write_command(&Command::Error(&reason)).await?;
            }
            () = liveness.tick(exec) => liveness.beat(wire).await?,
        }
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
async fn answer<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    ours: SocketType,
    subscriptions: &Arc<Subscriptions>,
    incoming: &Arc<Queue>,
    options: &SocketOptions,
    body: &[u8],
) -> Result<()> {
    match Command::decode(body).map_err(command_error)? {
        Command::Subscribe(prefix) if publishes(ours) => {
            apply_subscription(ours, subscriptions, incoming, options, true, prefix).await
        }
        Command::Cancel(prefix) if publishes(ours) => {
            apply_subscription(ours, subscriptions, incoming, options, false, prefix).await
        }
        Command::Ping { context, .. } => {
            // "When a peer receives a PING command it SHALL respond with a
            // PONG command that echoes the ping-context."
            wire.write_command(&Command::Pong { context }).await
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

/// Writes one subscription in the configured form.
async fn write_subscription<S: AsyncRead + AsyncWrite + Unpin>(
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
            wire.write_command(&command).await
        }
        SubscriptionForm::LegacyMessage => {
            let frame = subscriptions::write_message_form(subscribe, prefix);
            wire.write_message(&Multipart::single(frame)).await
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
                tracing::warn!(
                    held = subscriptions.len(),
                    "refused a subscription: this peer is at its table ceiling"
                );
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
            let frame = subscriptions::write_message_form(subscribe, prefix);
            if incoming.try_send(Multipart::single(frame)).is_err() {
                tracing::debug!(
                    "dropped a subscription notification: the application is not reading"
                );
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
    message: &Multipart,
) -> Result<()> {
    if message.len() == 1
        && let Some((subscribe, prefix)) =
            subscriptions::read_message_form(message.frames()[0].as_slice())
    {
        return apply_subscription(ours, subscriptions, incoming, options, subscribe, prefix).await;
    }
    if ours == SocketType::XPub {
        // "XPUB: as PUB plus … inbound messages fair-queued to the
        // application", and "messages without a sub/unsub prefix are also
        // received, but have no effect on subscription status" — which is how
        // an XSUB sends upstream through a proxy.
        if incoming.send(message.clone()).await? == Sent::Dropped {
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

/// The heartbeat: `PING` on a timer, and silence declared fatal.
///
/// `PING`/`PONG` are 3.1 commands, so a connection that negotiated 3.0 gets
/// no heartbeat at all whatever `ZMQ_HEARTBEAT_IVL` says. That is not
/// caution: sending a command a 3.0 peer does not know is a protocol
/// violation, and the interop tests show what it costs.
struct Liveness {
    interval: Option<Duration>,
    timeout: Duration,
    ttl_deciseconds: u16,
    last: Instant,
}

impl Liveness {
    fn new(options: &SocketOptions, version: Version) -> Liveness {
        let interval = match options.heartbeat_ivl {
            Some(interval) if version >= greeting::VERSION => Some(interval),
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
        Liveness {
            interval,
            timeout,
            ttl_deciseconds,
            last: Instant::now(),
        }
    }

    /// "A peer SHOULD treat any incoming traffic (not just a PONG reply) as a
    /// sign of life."
    fn saw_traffic(&mut self) {
        self.last = Instant::now();
    }

    /// Sleeps until the next beat is due, or forever when the heartbeat is
    /// off. Cancel-safe, because it holds no state: the deadline is
    /// recomputed each time.
    async fn tick(&self, exec: &Exec) {
        match self.interval {
            Some(interval) => exec.sleep(interval).await,
            None => std::future::pending().await,
        }
    }

    /// Sends one `PING`, and reports the peer dead when it has been silent
    /// past `ZMQ_HEARTBEAT_TIMEOUT`.
    async fn beat<S: AsyncRead + AsyncWrite + Unpin>(&mut self, wire: &mut Wire<S>) -> Result<()> {
        if self.interval.is_none() {
            return Ok(());
        }
        if !self.timeout.is_zero() && self.last.elapsed() >= self.timeout {
            return Err(Error::ETIMEDOUT(
                format!(
                    "the peer has been silent for {:?} (ZMQ_HEARTBEAT_TIMEOUT)",
                    self.last.elapsed()
                )
                .into(),
            ));
        }
        wire.write_command(&Command::Ping {
            ttl: self.ttl_deciseconds,
            context: PING_CONTEXT,
        })
        .await
    }
}

/// What arrived on the connection.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    /// A whole message, every frame of it.
    Message(Multipart),
    /// A command frame's body, still encoded: `READY`, `PING`, `SUBSCRIBE`
    /// and the rest. "Commands always consist of one frame."
    Command(Vec<u8>),
}

/// ZMTP over a byte stream: the I/O the codec deliberately does not do.
///
/// Public because the socket types drive it, and because it is what makes the
/// session testable against a raw stream.
#[derive(Debug)]
pub struct Wire<S> {
    io: S,
    /// Unparsed inbound bytes. Persisting across calls is half of what makes
    /// reading cancel-safe; the other half is that bytes are appended only
    /// once a read has **completed** — see [`Wire::fill`].
    buf: Vec<u8>,
    /// Where a read lands before it is appended. Reused, so a read costs no
    /// allocation, and separate from `buf` so that a cancelled read cannot
    /// change what is parsed.
    scratch: Vec<u8>,
    limits: MessageLimits,
    /// The CURVE session keys, once a handshake has agreed them. `None` is
    /// NULL and PLAIN, where the wire is what it says it is.
    curve: Option<CurveTransport>,
    /// Opened frames waiting to be assembled into a message, under CURVE.
    ///
    /// Re-framed rather than re-implemented: each opened box holds one
    /// frame's flags and body, which are written back as ZMTP frame octets so
    /// that [`Multipart::decode`] does the assembly and applies
    /// `ZMQ_MAXMSGSIZE` and the frame ceiling — the same code, and therefore
    /// the same limits, as an unencrypted connection. It is bounded by those
    /// limits: the assembly is attempted after every opened frame, so a peer
    /// that sends MORE forever is refused with `EMSGSIZE` one frame past the
    /// cap rather than growing this buffer.
    plain: Vec<u8>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Wire<S> {
    /// Wraps `io`, bounding every inbound message by `limits`:
    /// `ZMQ_MAXMSGSIZE` in octets — frame headers included — and a ceiling on
    /// the frame count. Both are needed; see [`Multipart::decode`].
    pub fn new(io: S, limits: MessageLimits) -> Wire<S> {
        Wire {
            io,
            buf: Vec::new(),
            scratch: vec![0u8; CHUNK],
            limits,
            curve: None,
            plain: Vec::new(),
        }
    }

    /// Installs the CURVE session keys a handshake agreed, after which every
    /// frame in either direction travels inside a `MESSAGE` box.
    pub fn encrypt_with(&mut self, transport: CurveTransport) {
        self.curve = Some(transport);
    }

    /// Destroys the session keys: "Session keys are held in memory and
    /// destroyed when the connection is closed."
    ///
    /// The decrypted staging buffer is overwritten as well, for the same
    /// reason and with the same honesty about it: without a volatile write
    /// neither erasure is guaranteed to survive the optimizer, and what is
    /// guaranteed is that nothing can be sealed or opened afterwards.
    pub fn destroy_session_keys(&mut self) {
        if let Some(curve) = &mut self.curve {
            curve.destroy();
        }
        self.curve = None;
        self.plain.fill(0);
        self.plain.clear();
    }

    /// Reads the next whole message or command.
    ///
    /// Cancel-safe: the buffer survives, so a dropped call loses nothing but
    /// the in-flight `read` — which the session's own `select!` relies on,
    /// because it drops this future on every message it writes.
    ///
    /// The parsing is [`Multipart::decode`]'s, so `ZMQ_MAXMSGSIZE` is judged
    /// from declared lengths and a multipart message is delivered only once
    /// its last frame has arrived.
    pub async fn read_next(&mut self) -> Result<Incoming> {
        if self.curve.is_some() {
            return self.read_next_sealed().await;
        }
        loop {
            match Multipart::decode(&self.buf, self.limits)? {
                Decoded::Message { message, consumed } => {
                    self.buf.drain(..consumed);
                    return Ok(Incoming::Message(message));
                }
                Decoded::Command { body, consumed } => {
                    let body = body.to_vec();
                    self.buf.drain(..consumed);
                    return Ok(Incoming::Command(body));
                }
                Decoded::Incomplete => self.fill().await?,
            }
        }
    }

    /// [`Self::read_next`] for a CURVE connection: the same assembly over
    /// the frames that came out of the boxes.
    ///
    /// Cancel-safe for the same two reasons and one more: an opened frame is
    /// appended to `plain` before anything is returned, so a dropped future
    /// loses neither the octets nor the plaintext — and never re-opens a box,
    /// which would fail the nonce check.
    async fn read_next_sealed(&mut self) -> Result<Incoming> {
        loop {
            match Multipart::decode(&self.plain, self.limits)? {
                Decoded::Message { message, consumed } => {
                    self.plain.drain(..consumed);
                    return Ok(Incoming::Message(message));
                }
                Decoded::Command { body, consumed } => {
                    let body = body.to_vec();
                    self.plain.drain(..consumed);
                    return Ok(Incoming::Command(body));
                }
                Decoded::Incomplete => self.open_next_box().await?,
            }
        }
    }

    /// Reads one `MESSAGE` command, opens its box and appends the frame it
    /// held to the plaintext buffer.
    async fn open_next_box(&mut self) -> Result<()> {
        let body = loop {
            match frame::decode(&self.buf, self.limits.max_bytes) {
                Ok((_header, body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    // **The frame kind is not the test; the box is.**
                    // 26/CURVEZMQ calls `MESSAGE` a command, and libzmq
                    // 4.3.5 sends it in a **message** frame — flags without
                    // the COMMAND bit, body `\x07MESSAGE` — measured in
                    // `tests/interop_libzmq.rs`. So both kinds are read, the
                    // body decides what arrived, and the outer MORE flag is
                    // ignored because the real one is inside the box. A
                    // frame that is neither a `MESSAGE` nor an `ERROR` is
                    // refused below, and a `MESSAGE` whose box does not open
                    // is refused after that, which is the check that matters.
                    break body;
                }
                Err(e) if !e.is_violation() => self.fill().await?,
                Err(e) => {
                    return Err(Error::ENOCOMPATPROTO(
                        format!("malformed frame: {e}").into(),
                    ));
                }
            }
        };
        // An `ERROR` is the one command that still arrives in clear text:
        // 37/ZMTP calls it fatal and a peer sending it is giving up, so
        // reading it turns a clean refusal into a reason instead of a
        // timeout. Nothing else is accepted outside a box.
        if let Ok(Command::Error(reason)) = Command::decode(&body) {
            return Err(Error::ENOCOMPATPROTO(
                format!("the peer refused the connection: {}", sanitize(reason)).into(),
            ));
        }
        let curve = self
            .curve
            .as_mut()
            .ok_or_else(|| Error::ENOTSOCK("this connection has no CURVE session".into()))?;
        let (flags, frame_body) = curve.open_message(&body)?;
        let kind = if flags & curve_layout::MESSAGE_FLAG_COMMAND != 0 {
            FrameKind::Command
        } else {
            FrameKind::Message {
                more: flags & curve_layout::MESSAGE_FLAG_MORE != 0,
            }
        };
        self.plain
            .extend_from_slice(&frame::encode(kind, &frame_body));
        Ok(())
    }

    /// Writes one whole message: every frame in one buffer, MORE on all but
    /// the last.
    ///
    /// One write, because "on sending, the peer SHALL queue all frames of a
    /// message in memory until the final frame is sent" — a peer must never
    /// see half a message. Under CURVE each frame is sealed on its own, with
    /// its MORE flag inside the box, and the boxes go out in that one write.
    pub async fn write_message(&mut self, message: &Multipart) -> Result<()> {
        let bytes = match self.curve.as_mut() {
            None => message.encode(),
            Some(curve) => {
                let last = message.len() - 1;
                let mut out = Vec::new();
                for (index, frame) in message.frames().iter().enumerate() {
                    let flags = if index == last {
                        0
                    } else {
                        curve_layout::MESSAGE_FLAG_MORE
                    };
                    out.extend_from_slice(&curve.seal_frame(flags, frame.as_slice())?);
                }
                out
            }
        };
        self.write_all(&bytes).await
    }

    /// Writes one command frame — sealed, on a CURVE connection.
    pub async fn write_command(&mut self, command: &Command<'_>) -> Result<()> {
        let bytes = self.command_bytes(command)?;
        self.write_all(&bytes).await
    }

    /// Writes one CURVE handshake frame, which is already complete octets:
    /// the handshake is what agrees the session keys, so it is never sealed
    /// by them.
    async fn write_handshake(&mut self, frame_bytes: &[u8]) -> Result<()> {
        self.write_all(frame_bytes).await
    }

    /// One command's frame octets, sealed or not.
    fn command_bytes(&mut self, command: &Command<'_>) -> Result<Vec<u8>> {
        match self.curve.as_mut() {
            None => command.encode().map_err(command_error),
            Some(curve) => {
                let mut body = Vec::new();
                command.encode_body(&mut body).map_err(command_error)?;
                curve.seal_frame(curve_layout::MESSAGE_FLAG_COMMAND, &body)
            }
        }
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
        self.write_all(bytes).await.expect("write");
    }

    /// Writes an `ERROR` and gives up on the connection.
    ///
    /// Best effort by construction: the peer is being closed on, so a write
    /// that fails changes nothing — including one that cannot be sealed.
    async fn refuse(&mut self, reason: &str) {
        if let Ok(bytes) = self.command_bytes(&Command::Error(reason)) {
            let _ = self.io.write_all(&bytes).await;
            let _ = self.io.flush().await;
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.io.write_all(bytes).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Reads exactly `out.len()` octets, using whatever is already buffered.
    async fn read_exactly(&mut self, out: &mut [u8]) -> Result<()> {
        while self.buf.len() < out.len() {
            self.fill().await?;
        }
        out.copy_from_slice(&self.buf[..out.len()]);
        self.buf.drain(..out.len());
        Ok(())
    }

    /// Reads more bytes, or reports the peer's close.
    ///
    /// Reading into `scratch` and appending afterwards is what makes this
    /// cancel-safe: `AsyncReadExt::read` is cancel-safe, so a dropped call
    /// reads nothing and leaves `buf` exactly as it was. Growing `buf` first
    /// and truncating after the await is the version that looks equivalent
    /// and is not — a cancelled read leaves the slack behind, and a
    /// zero-filled buffer decodes as a stream of empty message frames.
    async fn fill(&mut self) -> Result<()> {
        let read = self.io.read(&mut self.scratch).await?;
        if read == 0 {
            return Err(Error::EHOSTUNREACH("the peer closed the connection".into()));
        }
        self.buf.extend_from_slice(&self.scratch[..read]);
        Ok(())
    }
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
    struct CodecPeer {
        io: TcpStream,
        buf: Vec<u8>,
    }

    impl CodecPeer {
        fn new(io: TcpStream) -> CodecPeer {
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

    /// Runs a session over one end of a connected pair, with no engine, and
    /// hands back the pipe so a test can see both directions.
    ///
    /// `role` is the one fact the engine would otherwise supply that the
    /// security handshake reads: under NULL it decides which side
    /// authorizes, so a test of a server's ZAP dialog binds rather than
    /// dials.
    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn drive_session(
        stream: TcpStream,
        ours: SocketType,
        options: SocketOptions,
        role: Role,
        context: &Context,
    ) -> (Pipe, AuthenticatedUser, tokio::task::JoinHandle<Result<()>>) {
        let pipe = Pipe::new(options.pipe);
        let user = AuthenticatedUser::default();
        let connection = Connection {
            stream: Stream::tcp(stream),
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

        let recorded = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(id) = user.get() {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the user id was recorded");
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

        let recorded = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(id) = user.get() {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the user id was recorded");
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
        let arrived = tokio::time::timeout(Duration::from_secs(10), pipe.incoming().recv())
            .await
            .expect("the message arrived")
            .expect("a message");
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
        let arrived = tokio::time::timeout(Duration::from_secs(5), pipe.incoming().recv())
            .await
            .expect("delivered")
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

        let delivered = tokio::time::timeout(Duration::from_secs(10), peer_task)
            .await
            .expect("the peer finished")
            .expect("the peer task");
        assert_eq!(delivered, b"through the engine");
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
        let mut transport = curve_handshake_as_client(&mut peer, server_key, public, &secret).await;

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
        let arrived = tokio::time::timeout(Duration::from_secs(10), pipe.incoming().recv())
            .await
            .expect("the message arrived")
            .expect("a message");
        assert_eq!(arrived.frames()[0].as_slice(), b"the other way");

        // A frame in clear text is refused — not because of its frame kind,
        // which is the same kind a sealed `MESSAGE` uses, but because its
        // body is not a `MESSAGE` at all and no box was opened.
        peer.send(&Multipart::single("in the open").encode()).await;
        let outcome = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the session ended")
            .expect("the task");
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
            curve_handshake_as_client(&mut peer, server_key, allowed_public, &allowed_secret).await;

        peer.send(&transport.seal_frame(0, b"authorized").expect("seal"))
            .await;
        let arrived = tokio::time::timeout(Duration::from_secs(10), pipe.incoming().recv())
            .await
            .expect("the message arrived")
            .expect("a message");
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
            crate::curve::security_model(&server_options),
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
        let outcome = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the session ended")
            .expect("the task");
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

        let outcome = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the session ended")
            .expect("the task");
        let err = outcome.unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
    }
}
