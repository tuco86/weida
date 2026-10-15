//! How a connection is configured: every `CONNECT` field, every credential
//! form, and every bound.
//!
//! Each field is a field of `CONNECT` under the name the client protocol
//! reference gives it, or a local bound the protocol does not supply. Which
//! of the two it is is stated per field, because NATS bounds very little on
//! the client side: the numbers in `docs/research/nats.md` §11 —
//! `max_payload`, `max_pending`, `max_control_line`, `ping_interval`,
//! `max_pings_out` — are all *server* configuration, and a client that
//! adopted them as its own would be adopting values the server may have
//! changed.
//!
//! # Which bound is whose
//!
//! | Bound | Whose | Where it comes from |
//! | --- | --- | --- |
//! | `max_payload` | **the protocol's own** | `INFO.max_payload`, learned at connect time, enforced locally before every publish |
//! | `max_control_line` | **the protocol's own** | the `max_control_line` server option; the server answers `-ERR 'Maximum Control Line Exceeded'`, so it is checked locally on every line this client **writes** |
//! | [`ConnectionOptions::max_inbound_control_line`] | **ours** | every line the server sends except `INFO`. The server's option bounds what *clients* send it, and the lines it sends back are built from those — a `MSG` adds the `sid` to the `PUB` it answers — so the two bounds are different numbers that happen to start equal |
//! | `max_pending` | **the protocol's own**, and the *server's* to enforce | 64 MiB of pending outbound bytes per client, after which the server disconnects a slow consumer. A client cannot set it; what a client controls is how fast it drains, which is [`ConnectionOptions::subscription_queue`] |
//! | `max_pings_out` | **the protocol's own** for the server's pings, **ours** for the client's | the server's documented default is 2 unanswered pings before a stale disconnect; [`DEFAULT_MAX_PINGS_OUT`] is this client applying the same rule to its own |
//! | [`ConnectionOptions::max_subscriptions`] | **ours** | the protocol bounds the subscription table nowhere, and the client picks its own `sid`s |
//! | [`ConnectionOptions::max_pending_requests`] | **ours** | the inbox map, likewise unbounded by the protocol |
//! | [`ConnectionOptions::max_connect_urls`] | **ours** | `INFO.connect_urls` grows with the cluster and announces no length |
//! | [`ConnectionOptions::max_header_entries`] | **ours** | one `NATS/1.0` block's entries, which its byte count bounds only weakly |
//! | [`ConnectionOptions::subscription_queue`] | **ours** | messages waiting for an application to collect them, per subscription |
//! | [`ConnectionOptions::max_queued_bytes`] | **ours** | the octets of every delivered message waiting for an application, over the whole connection: a message count is not a memory bound |
//! | [`ConnectionOptions::max_inbound_payload`] | **ours** | the largest `MSG` or `HMSG` this client accepts. `INFO.max_payload` is the *server's* bound on what a client publishes, announced by the peer itself and unauthenticated, so it cannot be the bound on what the client receives |
//! | [`ConnectionOptions::max_info_line`] | **ours** | one `INFO` line, which grows with the cluster and so cannot share `max_control_line` |
//! | [`ConnectionOptions::outgoing_queue`] | **ours** | operations queued for the driver |
//! | [`ConnectionOptions::handshake_timeout`] | **ours** | the whole handshake, from the opening `INFO` to the `PONG`: the protocol gives no deadline for any of it |
//! | [`ConnectionOptions::write_timeout`] | **ours** | how long the socket may accept nothing while output waits for it: the protocol gives a client no write deadline |
//! | [`ConnectionOptions::max_resolved_addresses`] | **ours** | a resolver answer is remote input |
//!
//! # Where this client's defaults differ from the protocol's stated ones
//!
//! | Field | Reference's stated default | Here | Why |
//! | --- | --- | --- | --- |
//! | `verbose` | `true` ("turns on `+OK` protocol acknowledgements") | `false` | An `+OK` per operation doubles the server's writes, and this client does not use them to decide anything: `CONNECT` is confirmed by the `PING`/`PONG` that follows it, which works whether `verbose` is on or off |
//! | `protocol` | `0` or absent ("client supports original protocol") | `1` | This client handles asynchronous `INFO`, which is exactly what `1` claims |
//! | `headers` | absent (the server will not send `HMSG`) | `true`, where `INFO.headers` says the server has them | Without it a `NATS/1.0 503` cannot arrive, and request-reply loses the difference between "no responder" and "slow responder" |
//! | `no_responders` | absent | `true`, where headers were negotiated | Same reason; and the reference makes it depend on headers, so the two are set together or not at all |
//! | `echo` | absent ("original protocol") | absent | A client that publishes and subscribes on one connection usually wants its own copies; suppressing them is a decision only the application can make |
//!
//! All five are settable, and none of them is silent.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};

/// The IANA port for the NATS client protocol.
///
/// There is no second port for TLS: `INFO.tls_required` upgrades *this*
/// connection in place, so a TLS deployment listens on the same port. That is
/// the opposite of AMQP's 5671/5672 split and is worth stating once.
pub const PORT: u16 = 4222;

/// The conventional prefix for a reply inbox.
///
/// A convention, not a reserved word in the wire protocol: the reference
/// documents the reply subject as an ordinary subject, and `_INBOX.` is what
/// every client uses and what server permissions are written against.
pub const INBOX_PREFIX: &str = "_INBOX.";

/// What this client puts in `CONNECT.lang`.
pub const LANG: &str = "rust";

/// How often this client sends its own `PING`.
///
/// **Ours.** The protocol has no client ping interval; `ping_interval` in
/// `docs/research/nats.md` §11 is the *server's* setting for the pings it
/// sends, and its documented default is 2 minutes. This client uses the same
/// number for its own pings, because the two clocks measure the same thing
/// from opposite ends and picking a different period would only mean
/// discovering a dead connection at a different arbitrary time.
///
/// A fixed period, not a silence timeout: the clock is not restarted by
/// traffic, so a connection that never pauses — a publisher, above all —
/// still tests its peer every interval.
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(120);

/// How many of this client's `PING`s may be outstanding before the connection
/// is called stale.
///
/// **Ours, mirroring the protocol's own on the other side.** `max_pings_out`
/// is a server option whose documented default is 2: "the allowed unanswered
/// pings before stale disconnect" (§11). The server applies it to the client;
/// this is the client applying it to the server, which is the half the
/// protocol leaves to the client library. Bounded rather than infinite is the
/// whole point — a client that keeps pinging a server that never answers has
/// replaced a failure with a silence.
///
/// With the default of 2 and [`DEFAULT_PING_INTERVAL`], a server that stops
/// answering is reported failed after three intervals: two pings go
/// unanswered, and the third tick finds the bound already reached.
pub const DEFAULT_MAX_PINGS_OUT: u32 = 2;

/// How many subscriptions one connection may hold.
///
/// **Ours.** Nothing in the protocol bounds this: the `sid` is
/// "a unique alphanumeric subscription ID, generated by the client", so the
/// client is the only party that could bound the table, and every entry is a
/// live channel plus a subject. 4096 is far above what an application
/// subscribes to and far below what would matter.
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 4096;

/// How many requests may be outstanding on the inbox at once.
///
/// **Ours.** A pending request is a map entry waiting for a reply that may
/// never come; the protocol has no notion of the map at all.
pub const DEFAULT_MAX_PENDING_REQUESTS: usize = 1024;

/// How many messages may wait for one subscription's reader.
///
/// **Ours.** The protocol's analogue is `max_pending` — 64 MiB of pending
/// outbound bytes, after which the *server* disconnects a slow consumer
/// (§5, §11) — and the same reasoning applies one hop further on: a client
/// that let one slow application subscription block its read loop would stall
/// every other subscription on the connection, and then be disconnected as a
/// slow consumer anyway. So the queue is bounded and a message that arrives
/// for a full queue is dropped and counted ([`Subscription::dropped`]), which
/// is what Core NATS at-most-once delivery already permits.
///
/// A count, and a count is not a memory bound: see
/// [`DEFAULT_MAX_QUEUED_BYTES`] for the one that is.
///
/// [`Subscription::dropped`]: crate::Subscription::dropped
pub const DEFAULT_SUBSCRIPTION_QUEUE: usize = 512;

/// How many octets of delivered messages may wait for application readers,
/// over the whole connection.
///
/// **Ours.** `subscription_queue` counts messages, so on its own it bounds
/// nothing: 512 messages of [`DEFAULT_MAX_INBOUND_PAYLOAD`] each is 4 GiB for
/// one subscription, and there may be thousands. Every message handed to a
/// reader — a subscription's, or the reply a request is waiting for — is
/// charged its octets plus a floor that stands for the read buffer chunk it
/// keeps alive, and a message that does not fit is dropped and counted rather
/// than queued. The charge is returned when the reader takes the message.
///
/// 64 MiB is the server's own `max_pending` for a slow client (§11), which is
/// the same trade one hop on: past it the data is somebody else's problem,
/// and Core NATS stores nothing for replay. Keep it above
/// [`DEFAULT_MAX_INBOUND_PAYLOAD`], or the largest legal message can never be
/// queued.
///
/// The budget is the connection's, not a subscription's: a subscription nobody
/// reads can use all of it, and what arrives for every other subscription is
/// then dropped and counted until that one is read or dropped. An application
/// with one slow consumer among fast ones reads it or unsubscribes it.
pub const DEFAULT_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;

/// How many operations may be queued for the driver before a caller waits.
///
/// **Ours.** The protocol has no queue here; an unbounded one would let an
/// application outrun the socket without ever being told.
pub const DEFAULT_OUTGOING_QUEUE: usize = 256;

/// How long the whole handshake may take, from the opening `INFO` to the
/// `PONG` that confirms the `CONNECT`.
///
/// **Ours.** The protocol gives no deadline for any of it — not for the
/// `INFO` the server is supposed to send unprompted — so a server that
/// accepts a TCP connection and then says nothing would hold a client
/// forever. One budget for the lot rather than one per step: a server that
/// answers every operation with a `PING` a moment before the deadline never
/// lets a per-step clock expire and never sends the `PONG`.
///
/// The same number is how long [`Connection::flush`](crate::Connection::flush),
/// `subscribe`, `close` and the inbox registration wait for the driver to
/// answer them.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// How long the socket may accept nothing while output is waiting for it.
///
/// **Ours.** The protocol gives a client no write deadline; `nats-server`'s
/// own `write_deadline`, 10 seconds by default, is what it applies to a slow
/// *client*. A peer that stops reading — a stalled link, a black hole, a
/// hostile server — fills the kernel's buffers, after which a write never
/// completes. Without a bound that one peer holds the connection, everything
/// waiting to publish on it, and the socket itself for as long as it likes.
///
/// The clock restarts every time the socket accepts octets, so a slow link
/// that keeps moving is not a stall: what is bounded is no progress at all.
/// Reads are not affected either way; the driver keeps reading while a write
/// is blocked.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many addresses a hostname may expand to.
///
/// **Ours.** A resolver answer is remote input (`docs/INVARIANTS.md`);
/// `localhost` routinely yields two.
pub const DEFAULT_MAX_RESOLVED_ADDRESSES: usize = 8;

/// How many entries of `INFO.connect_urls` are retained.
///
/// **Ours**, and the same number the codec bounds the array's *decode* by:
/// `weida_nats_codec::limits::DEFAULT_MAX_CONNECT_URLS`. The list "grows with
/// the cluster" and a JSON array announces no length, so the only place to
/// stop is while reading it.
pub const DEFAULT_MAX_CONNECT_URLS: u32 = weida_nats_codec::limits::DEFAULT_MAX_CONNECT_URLS;

/// How many `name: value` entries one `NATS/1.0` header block may hold.
///
/// **Ours**, and the same number the codec defaults to. Neither the protocol
/// reference nor ADR-4 bounds the entry count, and the block's own byte count
/// bounds it only weakly: `a:\r\n` is four octets, so a 1 MiB block can
/// declare a quarter of a million entries, each one costing a vector slot
/// rather than four octets.
pub const DEFAULT_MAX_HEADER_ENTRIES: u32 = weida_nats_codec::limits::DEFAULT_MAX_HEADER_ENTRIES;

/// The largest `MSG` or `HMSG` this client accepts from the server, in octets
/// (the total, headers included, for `HMSG`).
///
/// **Ours.** `INFO.max_payload` is "the maximum payload size the server will
/// accept *from the client*": it bounds what this client publishes. It is also
/// a number the peer announces itself, in a line that arrives before TLS and
/// before authentication, so using it as the bound on what the client
/// *receives* would let the peer choose how much memory to commit — `INFO
/// {"max_payload":18446744073709551615}` is a legal line. The decode bound is
/// the smaller of the two: a server cannot raise it, only lower it. A message
/// above it ends the connection: the codec refuses the declared count from the
/// control line, before any of the body is read, and carrying on would mean
/// reading and discarding as many octets as the peer cares to send.
///
/// 8 MiB is eight times the server's documented default of 1 MiB and half of
/// what `nats-server` allows at most (64 MiB). A client whose servers are
/// configured above this raises it, knowingly.
pub const DEFAULT_MAX_INBOUND_PAYLOAD: u64 = 8 * 1024 * 1024;

/// The largest `INFO` line this client accepts, in octets before the `CRLF`.
///
/// **Ours**, and the codec's own default
/// (`weida_nats_codec::limits::DEFAULT_MAX_INFO_LINE`). An `INFO` grows with
/// the cluster — `connect_urls`, `ws_connect_urls`, account fields, the nonce —
/// so it cannot share the 4096-octet bound of `MSG` and `PING` without a large
/// cluster becoming a connection that cannot be made.
pub const DEFAULT_MAX_INFO_LINE: usize = weida_nats_codec::limits::DEFAULT_MAX_INFO_LINE;

/// The largest control line, `INFO` aside, this client accepts from the server.
///
/// **Ours.** The server's `max_control_line` bounds what *clients* send it,
/// and the lines it sends back are built from those: a `MSG` is the `PUB` it
/// answers with the subscriber's `sid` added (up to twenty digits and a
/// separator), so a message published on a line at the server's own limit
/// arrives on a line longer than it. The default is the codec's starting value
/// plus room for that, so the largest legal publication is not one this client
/// cannot receive.
pub const DEFAULT_MAX_INBOUND_CONTROL_LINE: usize =
    weida_nats_codec::limits::DEFAULT_MAX_CONTROL_LINE + 64;

/// What a caller's signer returns for one nonce.
///
/// Two strings, both base64url as the reference's `sig` and `nkey` fields
/// carry them. `Debug` prints the public key and never the signature: a
/// signature over a server nonce is single-use, but it is also the proof of
/// holding the key, and nothing that logs a value has a reason to keep it.
#[derive(Clone, PartialEq, Eq)]
pub struct NonceSignature {
    /// `CONNECT.sig`: "in case the server has responded with a `nonce` on
    /// `INFO`, then a NATS client must use this field to reply with the
    /// signed `nonce`."
    pub signature: String,
    /// `CONNECT.nkey`: "the public NKey to authenticate the client. This will
    /// be used to verify the signature (`sig`) against the `nonce` provided
    /// in the `INFO` message."
    ///
    /// `None` where the identity travels in a JWT instead, which already
    /// carries the public key the server verifies against.
    pub public_key: Option<String>,
}

impl fmt::Debug for NonceSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NonceSignature")
            .field("signature", &Redacted)
            .field("public_key", &self.public_key)
            .finish()
    }
}

/// What a secret prints as.
struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// What a nonce signer is, as a function.
///
/// Named rather than written out at the one place it is used, because the
/// signature is the contract between this crate and the application's key
/// material: octets in, [`NonceSignature`] or the application's own
/// explanation out.
pub type SignNonce = dyn Fn(&[u8]) -> std::result::Result<NonceSignature, String> + Send + Sync;

/// The caller's nonce signer.
///
/// **The signing is the application's, not this crate's.** An NKey signature
/// is an Ed25519 signature over the server's nonce with the user's seed, and
/// this crate has no signing dependency and will not grow one: the key
/// material is the application's, exactly as the `rustls::ClientConfig` for
/// TLS is the application's. Giving a messaging library a private key to hold
/// and an algorithm to choose would be making the application's security
/// decision for it.
///
/// So the caller supplies a closure. It receives the nonce **exactly as
/// `INFO` carried it**, with no decoding, trimming or re-encoding, because
/// the server verifies the signature against those same octets.
#[derive(Clone)]
pub struct Signer(Arc<SignNonce>);

impl Signer {
    /// Wraps a closure that signs the server's nonce.
    #[must_use]
    pub fn new<F>(sign: F) -> Self
    where
        F: Fn(&[u8]) -> std::result::Result<NonceSignature, String> + Send + Sync + 'static,
    {
        Self(Arc::new(sign))
    }

    /// Signs `nonce`, which is the `INFO.nonce` octets unchanged.
    pub fn sign(&self, nonce: &[u8]) -> std::result::Result<NonceSignature, String> {
        (self.0)(nonce)
    }
}

impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the closure and never anything it captured: this is the one
        // field in the configuration that may be holding key material.
        f.write_str("Signer(<caller-supplied>)")
    }
}

/// The one credential form a `CONNECT` carries.
///
/// An enum rather than a bag of options, because the reference's four forms
/// are alternatives: a `CONNECT` carrying both `auth_token` and `user`/`pass`
/// is a `CONNECT` whose author did not know which one the server wants. Which
/// one is *required* is the server's to say through `INFO.auth_required`; this
/// type makes "exactly one" structural.
///
/// `Debug` prints which form this is and, for a user name, the name — and never
/// a token, a password or a JWT, so that a `{:?}` of the options in a log
/// line, a failed assertion or a `tracing` field cannot carry them out.
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum Credentials {
    /// No credentials. Refused at connect time where `INFO.auth_required` is
    /// set, rather than sent and rejected.
    #[default]
    None,
    /// `CONNECT.auth_token`: "client authorization token".
    Token(String),
    /// `CONNECT.user` and `CONNECT.pass`.
    ///
    /// Both travel in the clear unless TLS is underneath, which is why
    /// [`ConnectionOptions::require_tls`] exists beside this variant.
    UserPassword {
        /// "Connection username."
        user: String,
        /// "Connection password."
        password: String,
    },
    /// `CONNECT.jwt`: "the JWT that identifies a user permissions and
    /// account", with the caller's signature over the nonce where the server
    /// sent one.
    ///
    /// The signer is optional because a server may issue a JWT-authenticated
    /// connection without a nonce; where a nonce *does* arrive and no signer
    /// is configured, the connection fails with [`Error::NonceMissing`]'s
    /// counterpart rather than sending an unsigned `CONNECT` the server will
    /// reject.
    Jwt {
        /// The user JWT, as the credentials file carries it.
        jwt: String,
        /// The caller's signer for `INFO.nonce`.
        signer: Option<Signer>,
    },
    /// `CONNECT.nkey` and `CONNECT.sig`: the public NKey and the caller's
    /// signature over `INFO.nonce`.
    ///
    /// "A server that gives a nonce expects the NKey client to sign that
    /// nonce in `CONNECT`" (`docs/research/nats.md` §1), so this form needs a
    /// nonce and fails without one.
    Nkey(Signer),
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Token(_) => f.debug_tuple("Token").field(&Redacted).finish(),
            Self::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &Redacted)
                .finish(),
            Self::Jwt { signer, .. } => f
                .debug_struct("Jwt")
                .field("jwt", &Redacted)
                .field("signer", signer)
                .finish(),
            Self::Nkey(signer) => f.debug_tuple("Nkey").field(signer).finish(),
        }
    }
}

impl Credentials {
    /// Whether this form can satisfy a server that set `auth_required`.
    #[must_use]
    pub const fn is_some(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Which form this is, for a log line and for an error message.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Token(_) => "auth_token",
            Self::UserPassword { .. } => "user/pass",
            Self::Jwt { .. } => "jwt",
            Self::Nkey(_) => "nkey",
        }
    }
}

/// What this client tells the server about itself, and every bound it
/// applies.
#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    /// `CONNECT.name`: "client name", for the server's `/connz` and its logs.
    pub name: Option<String>,
    /// The one credential form.
    pub credentials: Credentials,
    /// `CONNECT.verbose`: "turns on `+OK` protocol acknowledgements".
    /// `false` here; see the module table.
    pub verbose: bool,
    /// `CONNECT.pedantic`: "turns on additional strict format checking, e.g.
    /// for properly formed subjects".
    pub pedantic: bool,
    /// Whether to claim `CONNECT.headers` where the server advertises them.
    ///
    /// A claim, not a demand: `INFO.headers` decides, and where the server
    /// has no headers this stays out of the `CONNECT` entirely.
    pub headers: bool,
    /// Whether to claim `CONNECT.no_responders` where headers were
    /// negotiated.
    ///
    /// Refused at configuration time together with `headers: false`, because
    /// the reference makes the fast no-responder answer depend on headers and
    /// a `CONNECT` claiming one without the other asks for something that
    /// cannot arrive.
    pub no_responders: bool,
    /// `CONNECT.echo`: `Some(false)` asks the server not to send this
    /// connection its own publications. Only sent where `INFO.proto` is at
    /// least 1, which is the version that introduced it.
    pub echo: Option<bool>,
    /// Whether to insist on TLS even where `INFO.tls_required` is not set.
    ///
    /// This becomes `CONNECT.tls_required`. Useful beside
    /// [`Credentials::UserPassword`], which otherwise travels in the clear.
    /// [`Connection::connect_tls`](crate::Connection::connect_tls) insists
    /// whatever this says: a caller that hands over a TLS configuration has
    /// said it wants TLS, and an `INFO` that omits the flag must not be able to
    /// talk it out of it.
    pub require_tls: bool,
    /// The name to validate the server certificate against, and to put in
    /// SNI. Defaults to the host that was dialled.
    pub tls_server_name: Option<String>,
    /// How often this client sends its own `PING`. See
    /// [`DEFAULT_PING_INTERVAL`].
    pub ping_interval: Duration,
    /// How many unanswered `PING`s make the connection stale. See
    /// [`DEFAULT_MAX_PINGS_OUT`].
    pub max_pings_out: u32,
    /// The prefix every reply inbox on this connection sits under. See
    /// [`INBOX_PREFIX`].
    pub inbox_prefix: String,
    /// How many subscriptions the table may hold. See
    /// [`DEFAULT_MAX_SUBSCRIPTIONS`].
    pub max_subscriptions: usize,
    /// How many requests may be outstanding. See
    /// [`DEFAULT_MAX_PENDING_REQUESTS`].
    pub max_pending_requests: usize,
    /// How many messages may wait for one subscription's reader. See
    /// [`DEFAULT_SUBSCRIPTION_QUEUE`].
    pub subscription_queue: usize,
    /// How many octets of delivered messages may wait for readers, over the
    /// whole connection. See [`DEFAULT_MAX_QUEUED_BYTES`].
    pub max_queued_bytes: usize,
    /// How many operations may be queued for the driver. See
    /// [`DEFAULT_OUTGOING_QUEUE`].
    pub outgoing_queue: usize,
    /// How many `connect_urls` are **retained** from an `INFO`. See
    /// [`DEFAULT_MAX_CONNECT_URLS`].
    ///
    /// A retention preference, not a refusal: a longer list is truncated, and
    /// the bound the *decoder* applies is never lowered below
    /// [`DEFAULT_MAX_CONNECT_URLS`], so a cluster that grew past what a
    /// caller wants to keep does not become a dropped connection.
    pub max_connect_urls: u32,
    /// How many `name: value` entries one `NATS/1.0` header block may hold.
    /// See [`DEFAULT_MAX_HEADER_ENTRIES`].
    pub max_header_entries: u32,
    /// The largest control line this client will **write**, in octets before
    /// the `CRLF`. The server's own `max_control_line`; see the module table.
    /// What it accepts is [`max_inbound_control_line`](Self::max_inbound_control_line).
    pub max_control_line: usize,
    /// The largest control line this client accepts from the server, `INFO`
    /// aside. See [`DEFAULT_MAX_INBOUND_CONTROL_LINE`].
    pub max_inbound_control_line: usize,
    /// The largest `INFO` line this client accepts. See
    /// [`DEFAULT_MAX_INFO_LINE`].
    pub max_info_line: usize,
    /// The largest `MSG` or `HMSG` this client accepts, headers included. The
    /// decode bound is the smaller of this and `INFO.max_payload`. See
    /// [`DEFAULT_MAX_INBOUND_PAYLOAD`].
    pub max_inbound_payload: u64,
    /// How long the whole handshake may take. See
    /// [`DEFAULT_HANDSHAKE_TIMEOUT`].
    pub handshake_timeout: Duration,
    /// How long the socket may accept nothing while output waits for it. See
    /// [`DEFAULT_WRITE_TIMEOUT`].
    pub write_timeout: Duration,
    /// How many addresses a hostname may expand to. See
    /// [`DEFAULT_MAX_RESOLVED_ADDRESSES`].
    pub max_resolved_addresses: usize,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionOptions {
    /// This client's defaults, with no credentials.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: None,
            credentials: Credentials::None,
            verbose: false,
            pedantic: false,
            headers: true,
            no_responders: true,
            echo: None,
            require_tls: false,
            tls_server_name: None,
            ping_interval: DEFAULT_PING_INTERVAL,
            max_pings_out: DEFAULT_MAX_PINGS_OUT,
            inbox_prefix: INBOX_PREFIX.to_owned(),
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_pending_requests: DEFAULT_MAX_PENDING_REQUESTS,
            subscription_queue: DEFAULT_SUBSCRIPTION_QUEUE,
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
            outgoing_queue: DEFAULT_OUTGOING_QUEUE,
            max_connect_urls: DEFAULT_MAX_CONNECT_URLS,
            max_header_entries: DEFAULT_MAX_HEADER_ENTRIES,
            max_control_line: weida_nats_codec::limits::DEFAULT_MAX_CONTROL_LINE,
            max_inbound_control_line: DEFAULT_MAX_INBOUND_CONTROL_LINE,
            max_info_line: DEFAULT_MAX_INFO_LINE,
            max_inbound_payload: DEFAULT_MAX_INBOUND_PAYLOAD,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            max_resolved_addresses: DEFAULT_MAX_RESOLVED_ADDRESSES,
        }
    }

    /// Refuses an unusable configuration where it was configured, rather than
    /// correcting it silently
    /// (`docs/decisions/0013-competitor-libraries.md` §4.4 item 4).
    pub fn validate(&self) -> Result<()> {
        if self.no_responders && !self.headers {
            return Err(Error::Configuration(
                "no_responders needs headers: the fast no-responder answer is a \
                 NATS/1.0 503 status message, and without headers the server \
                 cannot send one"
                    .into(),
            ));
        }
        if self.max_pings_out == 0 {
            return Err(Error::Configuration(
                "max_pings_out must be at least 1: zero would call a connection \
                 stale before a single PING had a chance to be answered"
                    .into(),
            ));
        }
        if self.ping_interval.is_zero() {
            return Err(Error::Configuration(
                "ping_interval must be non-zero".into(),
            ));
        }
        if self.handshake_timeout.is_zero() {
            return Err(Error::Configuration(
                "handshake_timeout must be non-zero: the protocol gives no \
                 deadline of its own, so zero would mean no handshake can ever \
                 finish"
                    .into(),
            ));
        }
        if self.write_timeout.is_zero() {
            return Err(Error::Configuration(
                "write_timeout must be non-zero: zero would call every write a \
                 stall before the socket had a chance to take it"
                    .into(),
            ));
        }
        if self.max_subscriptions == 0 {
            return Err(Error::Configuration(
                "max_subscriptions must be at least 1".into(),
            ));
        }
        if self.max_pending_requests == 0 {
            return Err(Error::Configuration(
                "max_pending_requests must be at least 1".into(),
            ));
        }
        if self.subscription_queue == 0 {
            return Err(Error::Configuration(
                "subscription_queue must be at least 1".into(),
            ));
        }
        if self.max_queued_bytes == 0 {
            return Err(Error::Configuration(
                "max_queued_bytes must be non-zero: with no budget every \
                 delivered message would be dropped"
                    .into(),
            ));
        }
        if self.outgoing_queue == 0 {
            return Err(Error::Configuration(
                "outgoing_queue must be at least 1".into(),
            ));
        }
        if self.max_resolved_addresses == 0 {
            return Err(Error::Configuration(
                "max_resolved_addresses must be at least 1".into(),
            ));
        }
        if self.max_control_line == 0 {
            return Err(Error::Configuration(
                "max_control_line must be non-zero".into(),
            ));
        }
        if self.max_inbound_control_line == 0 {
            return Err(Error::Configuration(
                "max_inbound_control_line must be non-zero".into(),
            ));
        }
        if self.max_info_line == 0 {
            return Err(Error::Configuration(
                "max_info_line must be non-zero".into(),
            ));
        }
        if self.max_inbound_payload == 0 {
            return Err(Error::Configuration(
                "max_inbound_payload must be non-zero: it is the largest message \
                 this client will accept, and zero accepts only empty ones"
                    .into(),
            ));
        }
        // The inbox prefix becomes the leading tokens of every reply subject,
        // so it has to be a subject this client would publish on — minus the
        // trailing dot, which is what makes it a prefix.
        let stem = self
            .inbox_prefix
            .strip_suffix('.')
            .unwrap_or(&self.inbox_prefix);
        crate::subject::check_publish_subject(stem.as_bytes()).map_err(|error| {
            Error::Configuration(format!(
                "inbox_prefix {:?} is not a usable subject prefix: {error}",
                self.inbox_prefix
            ))
        })?;
        if !self.inbox_prefix.ends_with('.') {
            return Err(Error::Configuration(format!(
                "inbox_prefix {:?} must end in a dot: it is joined to a unique \
                 token to form the reply subject",
                self.inbox_prefix
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The dependency the reference states — the fast no-responder answer is
    /// a status *header* — refused where it is configured rather than
    /// discovered as a request that times out instead of failing fast.
    #[test]
    fn no_responders_without_headers_is_refused() {
        let mut options = ConnectionOptions::new();
        options.headers = false;
        let error = options.validate().expect_err("refused");
        assert!(
            matches!(&error, Error::Configuration(why) if why.contains("503")),
            "{error}"
        );

        options.no_responders = false;
        assert!(
            options.validate().is_ok(),
            "a client that wants neither is a client that gets neither"
        );
    }

    /// Zero unanswered pings would fail a connection that is merely waiting
    /// for its first answer, which is not what "bounded rather than infinite"
    /// means.
    #[test]
    fn a_zero_ping_bound_is_refused() {
        let mut options = ConnectionOptions::new();
        options.max_pings_out = 0;
        assert!(matches!(options.validate(), Err(Error::Configuration(_))));
    }

    /// The prefix is joined to a unique token, so a prefix without its dot
    /// would produce `_INBOX7` rather than `_INBOX.7`, and a prefix with a
    /// doubled dot an empty token.
    #[test]
    fn an_inbox_prefix_must_be_a_subject_prefix() {
        let mut options = ConnectionOptions::new();
        assert!(options.validate().is_ok(), "the default ends in a dot");

        for bad in ["_INBOX", "", ".", "_INBOX..", "_IN BOX."] {
            options.inbox_prefix = bad.to_owned();
            assert!(
                matches!(options.validate(), Err(Error::Configuration(_))),
                "{bad:?} must be refused"
            );
        }

        options.inbox_prefix = "app.replies.".to_owned();
        assert!(
            options.validate().is_ok(),
            "a prefix need not be _INBOX.: it is a convention, not a reserved word"
        );
    }

    /// The signer is the one field that may hold key material, so its `Debug`
    /// must not carry anything the closure captured into a log.
    #[test]
    fn a_signer_never_debugs_its_closure() {
        let secret = "seed-material".to_owned();
        let signer = Signer::new(move |nonce| {
            Ok(NonceSignature {
                signature: format!("{secret}:{}", nonce.len()),
                public_key: Some("UPUB".into()),
            })
        });
        let rendered = format!(
            "{:?}",
            ConnectionOptions {
                credentials: Credentials::Nkey(signer.clone()),
                ..ConnectionOptions::new()
            }
        );
        assert!(!rendered.contains("seed-material"), "{rendered}");
        assert!(rendered.contains("caller-supplied"), "{rendered}");

        // And it still signs the octets it is handed.
        assert_eq!(
            signer.sign(b"abc").expect("signed").signature,
            "seed-material:3"
        );
    }

    /// Every credential form prints without its secret: a `{:?}` of the
    /// options is what lands in a log line, a failed assertion or a `tracing`
    /// field, and the password, token and JWT must not be in it.
    #[test]
    fn no_credential_form_debugs_its_secret() {
        let forms = [
            Credentials::Token("t0k3n-secret".into()),
            Credentials::UserPassword {
                user: "alice".into(),
                password: "hunter2-secret".into(),
            },
            Credentials::Jwt {
                jwt: "ey.jwt-secret.sig".into(),
                signer: Some(Signer::new(|_| Err("unused".to_owned()))),
            },
            Credentials::Nkey(Signer::new(|_| Err("unused".to_owned()))),
        ];
        for credentials in forms {
            let name = credentials.name();
            let options = ConnectionOptions {
                credentials: credentials.clone(),
                ..ConnectionOptions::new()
            };
            for rendered in [format!("{credentials:?}"), format!("{options:?}")] {
                for secret in ["t0k3n-secret", "hunter2-secret", "jwt-secret"] {
                    assert!(!rendered.contains(secret), "{name}: {rendered}");
                }
            }
        }
        let password = format!(
            "{:?}",
            Credentials::UserPassword {
                user: "alice".into(),
                password: "hunter2-secret".into(),
            }
        );
        assert!(password.contains("alice"), "a user name is not a secret");
        assert!(password.contains("<redacted>"), "{password}");
    }

    /// A signature is the proof of holding the key; the public key beside it
    /// is public.
    #[test]
    fn a_nonce_signature_debugs_without_its_signature() {
        let rendered = format!(
            "{:?}",
            NonceSignature {
                signature: "SIGNATURE-secret".into(),
                public_key: Some("UPUBLICKEY".into()),
            }
        );
        assert!(!rendered.contains("SIGNATURE-secret"), "{rendered}");
        assert!(rendered.contains("UPUBLICKEY"), "{rendered}");
    }

    /// A bound of zero would refuse everything the connection exists to do, so
    /// it is refused where it is configured.
    #[test]
    fn a_zero_bound_is_refused() {
        type Zero = fn(&mut ConnectionOptions);
        let zeroes: [(&str, Zero); 6] = [
            ("write_timeout", |o| o.write_timeout = Duration::ZERO),
            ("max_queued_bytes", |o| o.max_queued_bytes = 0),
            ("max_inbound_payload", |o| o.max_inbound_payload = 0),
            ("max_inbound_control_line", |o| {
                o.max_inbound_control_line = 0
            }),
            ("max_info_line", |o| o.max_info_line = 0),
            ("handshake_timeout", |o| {
                o.handshake_timeout = Duration::ZERO
            }),
        ];
        for (name, zero) in zeroes {
            let mut options = ConnectionOptions::new();
            zero(&mut options);
            assert!(
                matches!(options.validate(), Err(Error::Configuration(why)) if why.contains(name)),
                "{name} of zero must be refused by name"
            );
        }
    }
}
