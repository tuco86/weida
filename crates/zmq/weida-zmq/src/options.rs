//! Socket options the connection engine reads, with libzmq's names, numbers
//! and sentinels.
//!
//! Two rules from
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item 4
//! govern this module. An option is **honoured or refused**, never silently
//! ignored, so an unusable value fails where it is set rather than at the
//! first message. And every default here is libzmq's own
//! (`docs/research/zeromq.md` §11), except where 0013 §4.4 item 5 names the
//! deviation.
//!
//! **libzmq's sentinels become `Option`.** `-1` means "no reconnect" for
//! `ZMQ_RECONNECT_IVL` and "no limit" for `ZMQ_MAXMSGSIZE`; `0` means "no
//! backoff" for `ZMQ_RECONNECT_IVL_MAX`, "no limit" for `ZMQ_HANDSHAKE_IVL`
//! and "the OS default" for `ZMQ_CONNECT_TIMEOUT`. Three different meanings
//! for two numbers is a footgun in C and does not need reproducing: each one
//! is a `None` whose documentation says which libzmq value it is.

use std::time::Duration;

use weida_zmtp::SocketType;

use crate::curve::{CurvePublicKey, CurveSecretKey};
use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::message::{DEFAULT_MAX_MESSAGE_FRAMES, DEFAULT_MAX_MESSAGE_SIZE, MessageLimits};
use crate::pipe::PipeConfig;
use crate::subscriptions::{
    DEFAULT_MAX_SUBSCRIPTION_BYTES, DEFAULT_MAX_SUBSCRIPTIONS, SubscriptionForm,
};

/// `ZMQ_RECONNECT_IVL` default: 100 ms (`docs/research/zeromq.md` §11).
pub const DEFAULT_RECONNECT_IVL: Duration = Duration::from_millis(100);

/// `ZMQ_HANDSHAKE_IVL` default: 30 s (`docs/research/zeromq.md` §11).
pub const DEFAULT_HANDSHAKE_IVL: Duration = Duration::from_secs(30);

/// `ZMQ_BACKLOG` default: 100 pending connections
/// (`docs/research/zeromq.md` §11).
pub const DEFAULT_BACKLOG: u32 = 100;

/// Addresses one hostname may resolve to before the rest are ignored.
///
/// **Not a libzmq option.** It exists because a resolver answer is remote
/// input and "no remote input can cause unbounded memory allocation"
/// (`docs/INVARIANTS.md`), and because `weida-runtime`'s resolver takes the
/// cap as an argument rather than owning one. More than one address is
/// necessary because the first is not necessarily reachable — `localhost`
/// commonly resolves to both `::1` and `127.0.0.1`.
pub const DEFAULT_MAX_RESOLVED_ADDRESSES: usize = 8;

/// Peers one socket admits from accepted connections, by default.
///
/// libzmq has no such option, so the number is ours to choose and to state.
/// 1024 is `ZMQ_MAX_SOCKETS`'s own order of magnitude and one above
/// `ZMQ_BACKLOG`'s 100, so a listener that fills its accept queue several
/// times over is still admitted; what it bounds is the product underneath —
/// at the default high-water marks and message size, 1024 peers is the
/// ceiling on `1024 × 2 × ZMQ_RCVHWM × ZMQ_MAXMSGSIZE` rather than on
/// nothing at all. A deployment that expects more peers than this raises it
/// deliberately, having seen that arithmetic.
pub const DEFAULT_MAX_PEERS: usize = 1024;

/// The largest `ZMQ_HEARTBEAT_TTL` the wire can carry: the field is
/// deciseconds in a `u16`, so 6553.5 s (`docs/research/zeromq.md` §11).
pub const MAX_HEARTBEAT_TTL: Duration = Duration::from_millis(6_553_500);

/// Longest `ZMQ_ZAP_DOMAIN`, in bytes.
///
/// **Not a libzmq limit**: 27/ZAP requires the domain frame to be non-empty
/// and says nothing about its length, and a domain reaches a handler as a
/// frame it must read. 256 is the same number this library uses for every
/// other name a peer or a configuration can set.
pub const MAX_ZAP_DOMAIN_BYTES: usize = 256;

/// Which security mechanism a socket speaks, and on which side.
///
/// One per socket, announced in the greeting and never negotiated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Security {
    /// No authentication and no confidentiality. `as-server` MUST be zero
    /// and "the peer that binds SHALL be the server".
    Null,
    /// PLAIN, as the server: the side that receives `HELLO`, asks the ZAP
    /// handler and answers `WELCOME` or `ERROR`.
    PlainServer,
    /// PLAIN, as the client: the side that sends `HELLO` and `INITIATE`.
    PlainClient,
    /// CURVE, as the server: the side that holds `s`, answers a `HELLO` with
    /// a `WELCOME` and a cookie, and learns the client's long-term key from
    /// the `INITIATE` box ([`crate::curve`]).
    CurveServer,
    /// CURVE, as the client: the side that knows `S`, sends `HELLO` and
    /// `INITIATE`, and proves its own long-term key with a vouch.
    CurveClient,
}

impl Security {
    /// The mechanism field this puts in the greeting.
    pub const fn mechanism(&self) -> weida_zmtp::Mechanism {
        match self {
            Security::Null => weida_zmtp::Mechanism::NULL,
            Security::PlainServer | Security::PlainClient => weida_zmtp::Mechanism::PLAIN,
            Security::CurveServer | Security::CurveClient => weida_zmtp::Mechanism::CURVE,
        }
    }

    /// The `as-server` octet: set for the server side of PLAIN and CURVE,
    /// and refused by the codec under NULL.
    pub const fn as_server(&self) -> bool {
        matches!(self, Security::PlainServer | Security::CurveServer)
    }

    /// Whether this mechanism encrypts what follows the handshake, so that
    /// every frame travels inside a `MESSAGE` box.
    pub const fn encrypts(&self) -> bool {
        matches!(self, Security::CurveServer | Security::CurveClient)
    }
}

/// What a socket's connection engine is configured with.
///
/// The socket types own the rest of the option surface — subscriptions,
/// routing ids, the XPUB flags — and keep these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketOptions {
    /// `ZMQ_RECONNECT_IVL`: how long to wait before reconnecting a dropped
    /// or refused outgoing connection. `None` is libzmq's `-1`: do not
    /// reconnect at all.
    pub reconnect_ivl: Option<Duration>,
    /// `ZMQ_RECONNECT_IVL_MAX`: the ceiling the interval doubles towards.
    /// `None` is libzmq's `0`: no backoff, retry at `reconnect_ivl` forever.
    pub reconnect_ivl_max: Option<Duration>,
    /// `ZMQ_HANDSHAKE_IVL`: how long a connection has to complete its
    /// handshake before it is closed. `None` is libzmq's `0`: no limit —
    /// which is also how an unauthenticated peer holds a connection open, so
    /// the default is finite.
    pub handshake_ivl: Option<Duration>,
    /// `ZMQ_CONNECT_TIMEOUT`: how long one connect attempt may take.
    /// `None` is libzmq's `0`: whatever the OS does, which on Linux is tens
    /// of seconds of SYN retries.
    pub connect_timeout: Option<Duration>,
    /// `ZMQ_IMMEDIATE`: queue only to completed connections.
    ///
    /// `false` — libzmq's default — lets a sender fill the queue of an
    /// endpoint that has never connected, which is what the pattern RFCs
    /// require ("SHALL maintain the double queue whether or not the
    /// connection is established") and what loses messages on a round-robin
    /// socket whose peer never arrives. `true` hides such a peer from the
    /// sender instead (`docs/research/zeromq.md` §5).
    pub immediate: bool,
    /// `ZMQ_BACKLOG`: pending connections the OS may hold on a bound
    /// endpoint.
    pub backlog: u32,
    /// `ZMQ_MAXMSGSIZE` for inbound messages. Finite by default, which is
    /// 0013 §4.4 item 5's second deliberate deviation — see
    /// [`DEFAULT_MAX_MESSAGE_SIZE`].
    pub max_message_size: u64,
    /// `ZMQ_HEARTBEAT_IVL`: how often to send a `PING`. `None` is libzmq's
    /// `0`: no heartbeat, and liveness is the transport's business.
    ///
    /// `PING`/`PONG` are ZMTP 3.1 commands, so a connection that negotiated
    /// 3.0 gets no heartbeat whatever this says — see
    /// [`crate::session::ZmtpSession`].
    pub heartbeat_ivl: Option<Duration>,
    /// `ZMQ_HEARTBEAT_TIMEOUT`: how long a peer may be silent before it is
    /// declared dead. `None` is libzmq's `0`, which means
    /// `ZMQ_HEARTBEAT_IVL` (`docs/research/zeromq.md` §11).
    pub heartbeat_timeout: Option<Duration>,
    /// `ZMQ_HEARTBEAT_TTL`: the hint carried in our `PING` telling the peer
    /// how long to wait for us before giving up. `None` is libzmq's `0`: no
    /// hint. The wire field is deciseconds in a `u16`, so the maximum is
    /// 6553.5 s and finer granularity is rounded down.
    pub heartbeat_ttl: Option<Duration>,
    /// `ZMQ_SNDTIMEO`: how long a blocking send may wait before reporting
    /// `EAGAIN`. `None` is libzmq's `-1`: wait forever.
    pub send_timeout: Option<Duration>,
    /// `ZMQ_RCVTIMEO`: the same bound on a blocking receive.
    pub recv_timeout: Option<Duration>,
    /// `ZMQ_REQ_CORRELATE`: prefix every REQ request with a request-id frame
    /// and discard replies that do not start with it
    /// (`docs/research/zeromq.md` §3).
    pub req_correlate: bool,
    /// `ZMQ_REQ_RELAXED`: let a REQ socket send a new request without having
    /// received the previous reply, abandoning that exchange rather than
    /// reporting `EFSM`.
    ///
    /// **Requires `req_correlate`, and this library refuses the pair rather
    /// than warning about it.** libzmq's own note: without correlation "a
    /// late reply to an aborted request can be reported as the reply to the
    /// superseding request", which is a wrong answer rather than a slow one.
    /// Refusing at configuration time is 0013 §4.4 item 4's rule; libzmq
    /// documents the hazard and allows it, and that difference is named here.
    pub req_relaxed: bool,
    /// `ZMQ_ROUTER_MANDATORY`: report an unroutable message instead of
    /// dropping it silently.
    ///
    /// libzmq: `0` "discards silently when it cannot be routed or the peer's
    /// SNDHWM is reached"; `1` reports `EHOSTUNREACH` when unroutable and
    /// `EAGAIN` at the high-water mark under `ZMQ_DONTWAIT`, blocking
    /// otherwise (`docs/research/zeromq.md` §4.2). ROUTER's own default is
    /// the brutal one, and it is kept: a ROUTER that blocked on one slow
    /// peer would stall every other.
    pub router_mandatory: bool,
    /// `ZMQ_ROUTER_HANDOVER`: let a newcomer claim an identity an existing
    /// peer already holds, disconnecting the incumbent.
    ///
    /// `false` — libzmq's default — rejects the newcomer instead, which is
    /// the safe answer when an identity is a name two peers may both believe
    /// they own.
    pub router_handover: bool,
    /// `ZMQ_PROBE_ROUTER`: send an empty message on every new connection, so
    /// that a ROUTER peer learns of this socket before it has anything to
    /// say.
    ///
    /// Legal on REQ, DEALER and ROUTER only — "the option must not be set
    /// against other socket types" — and refused at construction elsewhere.
    /// The receiving application must filter the empty message out.
    pub probe_router: bool,
    /// `ZMQ_ROUTING_ID`: the identity this socket announces in its `READY`,
    /// so that a ROUTER peer can address it by a name it chose rather than
    /// by a generated one (`docs/research/zeromq.md` §4.2).
    pub routing_id: Option<RoutingId>,
    /// Frames one inbound message may have — not a libzmq option, because
    /// 37/ZMTP has no such limit and an unbounded frame count is unbounded
    /// memory. See [`DEFAULT_MAX_MESSAGE_FRAMES`].
    pub max_message_frames: usize,
    /// Distinct subscription prefixes one peer may hold on this socket.
    ///
    /// **Not a libzmq option**, for the reason
    /// [`DEFAULT_MAX_SUBSCRIPTIONS`]
    /// gives: subscriptions are additive and non-idempotent, so a peer can
    /// buy N table entries with N commands and 37/ZMTP bounds neither the
    /// count nor the length. The exposure is this times `max_peers`.
    pub max_subscriptions: usize,
    /// Longest subscription prefix one peer may send this socket.
    ///
    /// **Not a libzmq option** either: 37/ZMTP's `subscription = *OCTET`
    /// bounds nothing, so a count ceiling without a length ceiling is not a
    /// bound at all. See [`DEFAULT_MAX_SUBSCRIPTION_BYTES`].
    pub max_subscription_bytes: usize,
    /// Which wire form this socket **sends** subscriptions in; both are
    /// accepted on receive. See [`SubscriptionForm`].
    pub subscription_form: SubscriptionForm,
    /// `ZMQ_XPUB_VERBOSE`: deliver **every** subscription to the
    /// application, not only the first for a prefix.
    ///
    /// libzmq's default deduplicates — 29/PUBSUB's optional normalization
    /// "so that multiple identical subscriptions result in a single command
    /// only" — which loses the count a proxy needs to forward upstream
    /// faithfully (`docs/research/zeromq.md` §4.3).
    pub xpub_verbose: bool,
    /// `ZMQ_XPUB_VERBOSER`: deliver every subscription **and** every
    /// unsubscription, including the ones that changed nothing.
    pub xpub_verboser: bool,
    /// `ZMQ_XPUB_MANUAL`: deliver subscriptions without applying them.
    ///
    /// The application decides what this socket will match, with
    /// [`crate::XPubSocket::subscribe`] — which is how a broker
    /// authorizes subscriptions instead of honouring whatever a peer asks
    /// for.
    pub xpub_manual: bool,
    /// `ZMQ_XPUB_WELCOME_MSG`: a message sent to every subscriber as soon as
    /// it connects, and again on every reconnect.
    pub xpub_welcome_msg: Option<Vec<u8>>,
    /// Peers this socket will admit from **accepted** connections.
    ///
    /// **Not a libzmq option**, and the parity table says so in those terms:
    /// `ZMQ_MAX_SOCKETS` bounds sockets per context, `ZMQ_BACKLOG` bounds the
    /// kernel's accept queue, and neither bounds how many established
    /// connections one socket holds — libzmq has no option that does. Every
    /// admitted peer carries a queue of `ZMQ_RCVHWM` messages inbound and
    /// `ZMQ_SNDHWM` outbound, so without this the exposure is that product
    /// times a number a stranger chooses. See
    /// [`DEFAULT_MAX_PEERS`] for the default and its arithmetic. Connections
    /// this socket dialled are not counted: their number is how many times
    /// the application called `connect`.
    pub max_peers: usize,
    /// `ZMQ_SNDHWM`/`ZMQ_RCVHWM` and the mute action, per peer.
    pub pipe: PipeConfig,
    /// See [`DEFAULT_MAX_RESOLVED_ADDRESSES`].
    pub max_resolved_addresses: usize,
    /// `ZMQ_PLAIN_SERVER`: this socket is the PLAIN **server**, and every
    /// connection it makes or accepts announces the PLAIN mechanism with the
    /// `as-server` octet set (24/ZMTP-PLAIN, `docs/research/zeromq.md` §6).
    ///
    /// A PLAIN server authorizes nothing by itself: the username and
    /// password go to the ZAP handler, which decides
    /// ([27/ZAP](https://rfc.zeromq.org/spec/27/)). A PLAIN server with no
    /// handler bound refuses every connection rather than admitting one it
    /// never checked.
    pub plain_server: bool,
    /// `ZMQ_PLAIN_USERNAME`: this socket is a PLAIN **client** and sends this
    /// username in its `HELLO`.
    ///
    /// Setting it chooses the mechanism, as in libzmq: "setting 0 shall reset
    /// the socket security to NULL", and setting a username selects PLAIN.
    /// At most [`weida_zmtp::MAX_PLAIN_FIELD`] octets — the field's own
    /// length octet is the bound.
    pub plain_username: Option<String>,
    /// `ZMQ_PLAIN_PASSWORD`: the password sent with the username, in clear
    /// text. PLAIN "is not robust against even the simplest traffic snooping
    /// or spoofing attacks" and its own RFC says so.
    ///
    /// Required whenever a username is set, including when it is empty: the
    /// `HELLO` carries both fields, so an unset password is a configuration
    /// gap rather than an empty string.
    pub plain_password: Option<String>,
    /// `ZMQ_CURVE_SERVER`: this socket is the CURVE **server**, and every
    /// connection it makes or accepts announces the CURVE mechanism with the
    /// `as-server` octet set (26/CURVEZMQ, [`crate::curve`]).
    ///
    /// It needs [`Self::curve_secretkey`] and nothing else: "a server does
    /// not need to know its own public key", because X25519 derives it.
    pub curve_server: bool,
    /// `ZMQ_CURVE_PUBLICKEY`: this socket's own long-term public key — `C`
    /// for a client, `S` for a server that states it.
    ///
    /// 32 binary octets or 40 characters of Z85, as
    /// [`CurvePublicKey::parse`] takes them. A client must set it, because
    /// the key travels inside its `INITIATE` box and is what a ZAP handler
    /// is given; a server may, and a server that states one that does not
    /// belong to its secret key is refused rather than trusted.
    pub curve_publickey: Option<CurvePublicKey>,
    /// `ZMQ_CURVE_SECRETKEY`: this socket's own long-term secret key, `c`
    /// or `s`. 32 binary octets or 40 characters of Z85.
    pub curve_secretkey: Option<CurveSecretKey>,
    /// `ZMQ_CURVE_SERVERKEY`: the server's long-term public key `S`, which
    /// makes this socket a CURVE **client**.
    ///
    /// Setting it chooses the mechanism and the side, as `ZMQ_PLAIN_USERNAME`
    /// does for PLAIN. A client that does not hold `S` cannot get past
    /// `HELLO`, which is what the signature box is for — and is also why
    /// CURVE needs no certificate authority and offers no discovery:
    /// "CurveCP does not explain how keys are exchanged".
    pub curve_serverkey: Option<CurvePublicKey>,
    /// `ZMQ_ZAP_DOMAIN`: the authorization domain this socket's connections
    /// are checked under, and the switch that turns authorization on.
    ///
    /// libzmq: "A ZAP domain must be specified to enable authentication. When
    /// the ZAP domain is empty, which is the default, ZAP authentication is
    /// disabled" — and that is exactly the behaviour here, for NULL and for
    /// CURVE, where an empty domain is 26/CURVEZMQ's first security model,
    /// "where the server does not check client keys at all". PLAIN always
    /// authenticates, because a username nobody checks is theatre.
    ///
    /// The domain's meaning is the application's: 27/ZAP calls it "the only
    /// scoping string" and leaves it at that.
    pub zap_domain: String,
    /// `ZMQ_ZAP_ENFORCE_DOMAIN`: refuse to authenticate without a domain
    /// instead of sending an empty one.
    ///
    /// 27/ZAP requires a non-empty domain in a request; libzmq sent an empty
    /// one for years and this option is its way back. Here it is a
    /// configuration check: with it set, a socket that would run a ZAP
    /// dialog without a domain is refused at construction rather than at the
    /// handshake.
    pub zap_enforce_domain: bool,
}

impl Default for SocketOptions {
    fn default() -> SocketOptions {
        SocketOptions {
            reconnect_ivl: Some(DEFAULT_RECONNECT_IVL),
            reconnect_ivl_max: None,
            handshake_ivl: Some(DEFAULT_HANDSHAKE_IVL),
            connect_timeout: None,
            immediate: false,
            backlog: DEFAULT_BACKLOG,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_message_frames: DEFAULT_MAX_MESSAGE_FRAMES,
            max_peers: DEFAULT_MAX_PEERS,
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_subscription_bytes: DEFAULT_MAX_SUBSCRIPTION_BYTES,
            subscription_form: SubscriptionForm::default(),
            xpub_verbose: false,
            xpub_verboser: false,
            xpub_manual: false,
            xpub_welcome_msg: None,
            heartbeat_ivl: None,
            heartbeat_timeout: None,
            heartbeat_ttl: None,
            send_timeout: None,
            recv_timeout: None,
            req_correlate: false,
            req_relaxed: false,
            router_mandatory: false,
            router_handover: false,
            probe_router: false,
            routing_id: None,
            pipe: PipeConfig::default(),
            max_resolved_addresses: DEFAULT_MAX_RESOLVED_ADDRESSES,
            plain_server: false,
            plain_username: None,
            plain_password: None,
            curve_server: false,
            curve_publickey: None,
            curve_secretkey: None,
            curve_serverkey: None,
            zap_domain: String::new(),
            zap_enforce_domain: false,
        }
    }
}

impl SocketOptions {
    /// Refuses a configuration that cannot be delivered, with `EINVAL` and a
    /// message naming the value.
    pub fn validate(&self) -> Result<()> {
        if self.max_message_size == 0 {
            return Err(Error::EINVAL(
                "ZMQ_MAXMSGSIZE is zero, so every message would be refused".into(),
            ));
        }
        if self.req_relaxed && !self.req_correlate {
            return Err(Error::EINVAL(
                "ZMQ_REQ_RELAXED without ZMQ_REQ_CORRELATE lets a late reply to an abandoned \
                 request be reported as the reply to the one that superseded it; libzmq \
                 documents that hazard and this library refuses it"
                    .into(),
            ));
        }
        if self.max_subscription_bytes == 0 {
            return Err(Error::EINVAL(
                "max_subscription_bytes is zero, so only the empty subscription could be sent"
                    .into(),
            ));
        }
        if self.max_subscriptions == 0 {
            return Err(Error::EINVAL(
                "max_subscriptions is zero, so no peer could subscribe to anything".into(),
            ));
        }
        if self.max_peers == 0 {
            return Err(Error::EINVAL(
                "max_peers is zero, so this socket could accept no connection at all".into(),
            ));
        }
        if self.max_message_frames == 0 {
            return Err(Error::EINVAL(
                "max_message_frames is zero, so every message would be refused".into(),
            ));
        }
        if self.plain_server && self.plain_username.is_some() {
            return Err(Error::EINVAL(
                "a socket is the PLAIN server or a PLAIN client, not both: ZMQ_PLAIN_SERVER with \
                 ZMQ_PLAIN_USERNAME has no meaning on the wire, where one as-server octet decides"
                    .into(),
            ));
        }
        match (&self.plain_username, &self.plain_password) {
            (Some(_), None) => {
                return Err(Error::EINVAL(
                    "ZMQ_PLAIN_USERNAME without ZMQ_PLAIN_PASSWORD: HELLO carries both fields, so \
                     an unset password is a gap rather than an empty one"
                        .into(),
                ));
            }
            (None, Some(_)) => {
                return Err(Error::EINVAL(
                    "ZMQ_PLAIN_PASSWORD without ZMQ_PLAIN_USERNAME, so the mechanism would still \
                     be NULL and the password would never be sent"
                        .into(),
                ));
            }
            _ => {}
        }
        for (option, value) in [
            ("ZMQ_PLAIN_USERNAME", &self.plain_username),
            ("ZMQ_PLAIN_PASSWORD", &self.plain_password),
        ] {
            if let Some(value) = value
                && value.len() > weida_zmtp::MAX_PLAIN_FIELD
            {
                return Err(Error::EINVAL(
                    format!(
                        "{option} is {} octets; the field's own length octet bounds it at {}",
                        value.len(),
                        weida_zmtp::MAX_PLAIN_FIELD
                    )
                    .into(),
                ));
            }
        }
        let plain_set = self.plain_server || self.plain_username.is_some();
        let curve_set = self.curve_server
            || self.curve_serverkey.is_some()
            || self.curve_publickey.is_some()
            || self.curve_secretkey.is_some();
        if plain_set && curve_set {
            return Err(Error::EINVAL(
                "a socket speaks one mechanism: \"security in ZMTP is assertive in that all \
                 peers on a given socket have the same, required level of security\", and one \
                 mechanism field in the greeting is all there is to announce it"
                    .into(),
            ));
        }
        if self.curve_server && self.curve_serverkey.is_some() {
            return Err(Error::EINVAL(
                "a socket is the CURVE server or a CURVE client, not both: ZMQ_CURVE_SERVER \
                 with ZMQ_CURVE_SERVERKEY names two sides of one connection"
                    .into(),
            ));
        }
        if self.curve_server && self.curve_secretkey.is_none() {
            return Err(Error::EINVAL(
                "ZMQ_CURVE_SERVER without ZMQ_CURVE_SECRETKEY: a server needs s to open a \
                 HELLO, and no default key exists to fall back on"
                    .into(),
            ));
        }
        if self.curve_serverkey.is_some()
            && (self.curve_secretkey.is_none() || self.curve_publickey.is_none())
        {
            return Err(Error::EINVAL(
                "a CURVE client needs ZMQ_CURVE_PUBLICKEY and ZMQ_CURVE_SECRETKEY beside \
                 ZMQ_CURVE_SERVERKEY: C travels in the INITIATE box and is the credential a \
                 ZAP handler is given, and c signs the vouch"
                    .into(),
            ));
        }
        if !self.curve_server && self.curve_serverkey.is_none() && curve_set {
            return Err(Error::EINVAL(
                "a CURVE key pair with neither ZMQ_CURVE_SERVER nor ZMQ_CURVE_SERVERKEY \
                 selects no mechanism, so the greeting would still say NULL and the keys \
                 would never be used"
                    .into(),
            ));
        }
        if let (Some(public), Some(secret)) = (&self.curve_publickey, &self.curve_secretkey)
            && secret.public_key() != *public
        {
            return Err(Error::EINVAL(
                "ZMQ_CURVE_PUBLICKEY is not the public key of ZMQ_CURVE_SECRETKEY; X25519 \
                 derives one from the other, so the pair cannot be half right"
                    .into(),
            ));
        }
        if self.zap_enforce_domain && self.zap_domain.is_empty() {
            return Err(Error::EINVAL(
                "ZMQ_ZAP_ENFORCE_DOMAIN with an empty ZMQ_ZAP_DOMAIN: the option exists to stop \
                 an empty domain reaching the handler, so a socket that would send one is \
                 refused here"
                    .into(),
            ));
        }
        if !self.zap_domain.is_empty() && self.zap_domain.len() > MAX_ZAP_DOMAIN_BYTES {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_ZAP_DOMAIN is {} octets; this library bounds it at {MAX_ZAP_DOMAIN_BYTES} \
                     because it is a frame a handler must read",
                    self.zap_domain.len()
                )
                .into(),
            ));
        }
        if self.max_resolved_addresses == 0 {
            return Err(Error::EINVAL(
                "max_resolved_addresses is zero, so no hostname could be dialled".into(),
            ));
        }
        if let (Some(ivl), Some(max)) = (self.reconnect_ivl, self.reconnect_ivl_max)
            && max < ivl
        {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_RECONNECT_IVL_MAX ({max:?}) is below ZMQ_RECONNECT_IVL ({ivl:?}), \
                     so the backoff would count downwards"
                )
                .into(),
            ));
        }
        if self.reconnect_ivl.is_none() && self.reconnect_ivl_max.is_some() {
            return Err(Error::EINVAL(
                "ZMQ_RECONNECT_IVL_MAX is set while reconnection is disabled \
                 (ZMQ_RECONNECT_IVL = -1)"
                    .into(),
            ));
        }
        if let Some(ttl) = self.heartbeat_ttl
            && ttl > MAX_HEARTBEAT_TTL
        {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_HEARTBEAT_TTL travels as deciseconds in a u16, so its maximum is \
                     {MAX_HEARTBEAT_TTL:?}; this one is {ttl:?}"
                )
                .into(),
            ));
        }
        if self.heartbeat_timeout.is_some() && self.heartbeat_ivl.is_none() {
            return Err(Error::EINVAL(
                "ZMQ_HEARTBEAT_TIMEOUT is set while the heartbeat is off \
                 (ZMQ_HEARTBEAT_IVL = 0), so nothing would ever measure the silence"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Refuses an option this socket *type* cannot honour, which no
    /// type-blind [`SocketOptions::validate`] can see.
    ///
    /// 0013 §4.4 item 4's rule is "honoured or refused, never silently
    /// ignored", and an option that means nothing for a socket type is the
    /// case libzmq itself names: `ZMQ_PROBE_ROUTER` "must not be set against
    /// other socket types" (`docs/research/zeromq.md` §4.2).
    pub fn validate_for(&self, socket_type: SocketType) -> Result<()> {
        if self.probe_router
            && !matches!(
                socket_type,
                SocketType::Req | SocketType::Dealer | SocketType::Router
            )
        {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_PROBE_ROUTER is a REQ, DEALER and ROUTER option; a {} socket \
                     cannot honour it",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        if (self.router_mandatory || self.router_handover) && socket_type != SocketType::Router {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_ROUTER_MANDATORY and ZMQ_ROUTER_HANDOVER are ROUTER options; a {} \
                     socket cannot honour them",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        if (self.xpub_verbose
            || self.xpub_verboser
            || self.xpub_manual
            || self.xpub_welcome_msg.is_some())
            && socket_type != SocketType::XPub
        {
            return Err(Error::EINVAL(
                format!(
                    "the ZMQ_XPUB_* options are XPUB's; a {} socket cannot honour them",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        if (self.req_correlate || self.req_relaxed) && socket_type != SocketType::Req {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_REQ_CORRELATE and ZMQ_REQ_RELAXED are REQ options; a {} socket \
                     cannot honour them",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        Ok(())
    }

    /// The two ceilings one peer's subscription table lives under.
    pub const fn subscription_limits(&self) -> (usize, usize) {
        (self.max_subscriptions, self.max_subscription_bytes)
    }

    /// The security mechanism these options select, and which side of it
    /// this socket is.
    ///
    /// "Security in ZMTP is *assertive* in that all peers on a given socket
    /// have the same, required level of security. This prevents downgrade
    /// attacks" (`docs/research/zeromq.md` §6), so this is one answer per
    /// socket and there is no negotiation.
    pub fn security(&self) -> Security {
        match (
            self.curve_server,
            self.curve_serverkey.is_some(),
            self.plain_server,
            self.plain_username.is_some(),
        ) {
            (true, ..) => Security::CurveServer,
            (_, true, ..) => Security::CurveClient,
            (.., true, _) => Security::PlainServer,
            (.., true) => Security::PlainClient,
            _ => Security::Null,
        }
    }

    /// Whether a connection this socket **accepted or bound** must be
    /// authorized by a ZAP handler.
    ///
    /// PLAIN always is: the credentials are only worth sending if somebody
    /// checks them. NULL and CURVE are when a domain is configured, which is
    /// libzmq's switch — "when the ZAP domain is empty… ZAP authentication
    /// is disabled". For CURVE that switch is also the choice between
    /// 26/CURVEZMQ's security models: no domain is "the server does not
    /// check client keys at all", and a domain hands every client's
    /// long-term key to the handler ([`crate::curve::SecurityModel`]).
    pub fn authorizes(&self) -> bool {
        match self.security() {
            Security::PlainServer => true,
            Security::PlainClient | Security::CurveClient => false,
            Security::Null | Security::CurveServer => !self.zap_domain.is_empty(),
        }
    }

    /// What bounds one inbound message: `ZMQ_MAXMSGSIZE` and the frame
    /// ceiling, which the session hands to every connection it drives.
    pub const fn message_limits(&self) -> MessageLimits {
        MessageLimits::new(self.max_message_size, self.max_message_frames)
    }

    /// The next reconnect interval after `previous`, doubling towards
    /// `ZMQ_RECONNECT_IVL_MAX`.
    ///
    /// libzmq: with `_IVL_MAX` above `_IVL` the interval grows exponentially
    /// up to that ceiling, and with `_IVL_MAX` at its default the interval
    /// never changes (`docs/research/zeromq.md` §11). No jitter is added
    /// here; libzmq randomizes and this does not, which matters only to a
    /// thundering herd of reconnecting peers and is a named difference rather
    /// than an oversight.
    ///
    /// Returns `None` when reconnection is disabled.
    pub fn next_reconnect_ivl(&self, previous: Option<Duration>) -> Option<Duration> {
        let base = self.reconnect_ivl?;
        let Some(previous) = previous else {
            return Some(base);
        };
        match self.reconnect_ivl_max {
            None => Some(base),
            Some(max) => Some(previous.saturating_mul(2).min(max)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: with no `_IVL_MAX` the interval never changes, and with one it
    /// doubles up to that ceiling and stops there.
    #[test]
    fn the_backoff_doubles_towards_the_ceiling() {
        let flat = SocketOptions::default();
        assert_eq!(
            flat.next_reconnect_ivl(None),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            flat.next_reconnect_ivl(Some(Duration::from_millis(100))),
            Some(Duration::from_millis(100)),
            "no ceiling means no backoff"
        );

        let backing_off = SocketOptions {
            reconnect_ivl: Some(Duration::from_millis(100)),
            reconnect_ivl_max: Some(Duration::from_millis(350)),
            ..SocketOptions::default()
        };
        let mut ivl = backing_off.next_reconnect_ivl(None);
        assert_eq!(ivl, Some(Duration::from_millis(100)));
        ivl = backing_off.next_reconnect_ivl(ivl);
        assert_eq!(ivl, Some(Duration::from_millis(200)));
        ivl = backing_off.next_reconnect_ivl(ivl);
        assert_eq!(ivl, Some(Duration::from_millis(350)), "capped at _IVL_MAX");
        ivl = backing_off.next_reconnect_ivl(ivl);
        assert_eq!(ivl, Some(Duration::from_millis(350)), "and it stays there");
    }

    /// Claim: `ZMQ_RECONNECT_IVL = -1` disables reconnection, and nothing
    /// resurrects it.
    #[test]
    fn reconnection_can_be_disabled() {
        let once = SocketOptions {
            reconnect_ivl: None,
            reconnect_ivl_max: None,
            ..SocketOptions::default()
        };
        assert_eq!(once.next_reconnect_ivl(None), None);
        assert_eq!(once.next_reconnect_ivl(Some(Duration::from_secs(1))), None);
        once.validate().expect("disabling reconnection is legal");
    }

    /// Claim: a configuration that cannot be delivered is refused where it is
    /// set, with the value named.
    #[test]
    fn unusable_options_are_refused() {
        // The defaults are a configuration this library can deliver, which
        // is the only claim about them worth a test: the numbers themselves
        // are libzmq's and are stated where they are defined.
        SocketOptions::default()
            .validate()
            .expect("the defaults are usable");

        for broken in [
            SocketOptions {
                max_message_size: 0,
                ..SocketOptions::default()
            },
            SocketOptions {
                max_resolved_addresses: 0,
                ..SocketOptions::default()
            },
            SocketOptions {
                reconnect_ivl: Some(Duration::from_millis(500)),
                reconnect_ivl_max: Some(Duration::from_millis(100)),
                ..SocketOptions::default()
            },
            SocketOptions {
                reconnect_ivl: None,
                reconnect_ivl_max: Some(Duration::from_millis(100)),
                ..SocketOptions::default()
            },
        ] {
            let err = broken.validate().unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
            assert!(!err.cause().is_empty());
        }
    }

    /// Claim: the four CURVE options select the mechanism and the side, and
    /// every half-configured combination is refused where it is set rather
    /// than at the handshake.
    #[test]
    fn the_curve_options_are_honoured_or_refused() {
        let (server_public, server_secret) = crate::curve::keypair();
        let (client_public, client_secret) = crate::curve::keypair();

        let server = SocketOptions {
            curve_server: true,
            curve_secretkey: Some(server_secret.clone()),
            ..SocketOptions::default()
        };
        server
            .validate()
            .expect("a server needs only its secret key");
        assert_eq!(server.security(), Security::CurveServer);
        assert!(server.security().as_server());
        assert!(server.security().encrypts());
        assert!(!server.authorizes(), "no domain is model 1");

        let client = SocketOptions {
            curve_publickey: Some(client_public),
            curve_secretkey: Some(client_secret.clone()),
            curve_serverkey: Some(server_public),
            ..SocketOptions::default()
        };
        client.validate().expect("a client needs all three");
        assert_eq!(client.security(), Security::CurveClient);
        assert!(!client.security().as_server());
        assert!(!client.authorizes(), "a client authorizes nobody");

        for broken in [
            // A server with no key at all.
            SocketOptions {
                curve_server: true,
                ..SocketOptions::default()
            },
            // A client missing its own pair.
            SocketOptions {
                curve_serverkey: Some(server_public),
                ..SocketOptions::default()
            },
            // Both sides of one connection.
            SocketOptions {
                curve_server: true,
                curve_secretkey: Some(server_secret.clone()),
                curve_serverkey: Some(server_public),
                ..SocketOptions::default()
            },
            // Keys that select no mechanism.
            SocketOptions {
                curve_publickey: Some(client_public),
                curve_secretkey: Some(client_secret.clone()),
                ..SocketOptions::default()
            },
            // A pair that is half right.
            SocketOptions {
                curve_publickey: Some(server_public),
                curve_secretkey: Some(client_secret.clone()),
                curve_serverkey: Some(server_public),
                ..SocketOptions::default()
            },
            // Two mechanisms on one socket.
            SocketOptions {
                curve_server: true,
                curve_secretkey: Some(server_secret),
                plain_server: true,
                ..SocketOptions::default()
            },
        ] {
            let err = broken.validate().unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
            assert!(!err.cause().is_empty());
        }
    }
}
