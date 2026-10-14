//! The connection: `INFO`, `CONNECT`, TLS, the pings, and the driver that
//! owns the wire.
//!
//! ```text
//! TCP connect
//!       <---------------------- INFO {...}        unprompted, and first
//!   [TLS handshake, where INFO.tls_required or the caller asked for it]
//! CONNECT {...} ------------->                    exactly one credential form
//! PING          ------------->                    so that a refused CONNECT is
//!       <---------------------- PONG              an answer rather than a silence
//! ...
//!       <---------------------- INFO {...}        asynchronous, any time
//! PING/PONG both ways, bounded
//! (no CLOSE verb: ending the transport ends the connection)
//! ```
//!
//! # `INFO` comes first, and it is the server that speaks
//!
//! "The server sends `INFO` after accepting the connection"
//! (`docs/research/nats.md` §1) — before the client says anything at all.
//! That is the opposite of AMQP, where the client's protocol header opens the
//! conversation, and it is load-bearing: the `INFO` carries `max_payload`,
//! `tls_required`, `auth_required`, `headers` and the `nonce`, so **every**
//! decision the `CONNECT` encodes depends on having read it. A client that
//! wrote first would be guessing at all five.
//!
//! # The `CONNECT` is confirmed by a `PING`
//!
//! The protocol has no positive acknowledgement for `CONNECT`. With
//! `verbose: false` a server that accepts it says nothing, and a server that
//! refuses it sends `-ERR 'Authorization Violation'` and closes. So this
//! client writes `CONNECT` and `PING` together and waits for the `PONG`: a
//! `PONG` means the `CONNECT` was read and accepted, an `-ERR` means it was
//! not, and either way the handshake ends in an answer instead of in a
//! connection that looks open and is not. This is what every mainstream NATS
//! client does, for the same reason.
//!
//! # The pings are two clocks and one bound
//!
//! * **Ours to answer.** A server `PING` is answered with `PONG` immediately.
//!   The server's own `ping_interval` (2 minutes) and `max_pings_out` (2) are
//!   its half of the rule (§11): miss enough of them and it disconnects us as
//!   stale.
//! * **Ours to send, and ours to bound.** This client pings on
//!   [`ConnectionOptions::ping_interval`] and counts how many went
//!   unanswered. At [`ConnectionOptions::max_pings_out`] the connection is
//!   reported [`State::Failed`] rather than pinged forever — a client that
//!   kept pinging a server that never answers has replaced a failure with a
//!   silence, and the application would wait on it indefinitely.
//!
//! # Asynchronous `INFO`
//!
//! "A server may send later asynchronous `INFO` messages; a capable client
//! must handle them outside the initial handshake" (§1). This client claims
//! `protocol: 1`, which is the claim that it does. A later `INFO`:
//!
//! * replaces `connect_urls`, retained under
//!   [`ConnectionOptions::max_connect_urls`] and readable through
//!   [`Connection::info`];
//! * raises or lowers `max_payload`, which takes effect on the next publish;
//! * carries `ldm: true` when the server has entered Lame Duck Mode, which
//!   [`Connection::lame_duck_notice`] surfaces as the drain notice it is:
//!   the server is about to stop accepting connections and will drain the
//!   ones it has (§1, §12 P17).
//!
//! # There is no reconnect loop
//!
//! Deliberately, and stated rather than left ambiguous: "client reconnection
//! policy is a client-library policy, not a Core NATS wire guarantee" (§1),
//! so none is built in. When the connection ends,
//! [`Connection::closed`] reports why and every subscription ends with it —
//! which is the protocol's own behaviour, since "core subscriptions vanish
//! with the connection and have no stored session". The material a policy
//! needs is exposed instead: the failure reason, the retained `connect_urls`,
//! and the lame-duck notice. Redialling, re-subscribing and backing off are
//! the caller's, because only the caller knows whether its subscriptions are
//! still wanted.

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use weida_nats_codec::limits::DEFAULT_MAX_PAYLOAD;
use weida_nats_codec::{Connect, Limits, Op, ServerInfo};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::message::{Message, OwnedHeaders};
use crate::options::{ConnectionOptions, Credentials, LANG, NonceSignature};
use crate::request::{self, Inbox, Pending};
use crate::subscription::{Delivered, Subscription, Table};
use crate::transport::{self, Incoming, OpReader, OpWriter, Wire};

/// What the server said in an `INFO`, owned and with the absences resolved.
///
/// The codec's `ServerInfo` makes every field optional, which is right for a
/// reader of a document. A connection has to *act*, so the booleans fold to
/// `false` — an absent `tls_required` is not a demand — and `max_payload`
/// folds to the server documentation's default, because a server that omits
/// the field it marks "always" present is still a server this client can
/// publish to conservatively.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteInfo {
    /// "The unique identifier of the NATS server."
    pub server_id: Option<String>,
    /// "The name of the NATS server."
    pub server_name: Option<String>,
    /// "The version of NATS."
    pub version: Option<String>,
    /// "An integer indicating the protocol version of the server." `1` or
    /// above is what permits `echo` and asynchronous `INFO`.
    pub proto: u64,
    /// "The IP address used to start the NATS server."
    pub host: Option<String>,
    /// "The port number the NATS server is configured to listen on."
    pub port: Option<u64>,
    /// "Maximum payload size, in bytes, that the server will accept from the
    /// client." **The protocol's own bound**, and the one every publish is
    /// checked against locally.
    pub max_payload: u64,
    /// "Whether the server supports headers."
    pub headers: bool,
    /// "If this is true, then the client should try to authenticate upon
    /// connect."
    pub auth_required: bool,
    /// "If this is true, then the client must perform the TLS/1.2
    /// handshake" — before ordinary protocol exchange.
    pub tls_required: bool,
    /// "If this is true, the client must provide a valid certificate during
    /// the TLS handshake."
    pub tls_verify: bool,
    /// "The nonce for use in CONNECT", where the server sent one.
    pub nonce: Option<String>,
    /// "List of server urls that a client can connect to", truncated to
    /// [`ConnectionOptions::max_connect_urls`].
    pub connect_urls: Vec<String>,
    /// `ldm`: the server has entered Lame Duck Mode and will drain its
    /// clients.
    pub lame_duck: bool,
}

impl Default for RemoteInfo {
    fn default() -> Self {
        Self {
            server_id: None,
            server_name: None,
            version: None,
            proto: 0,
            host: None,
            port: None,
            max_payload: DEFAULT_MAX_PAYLOAD,
            headers: false,
            auth_required: false,
            tls_required: false,
            tls_verify: false,
            nonce: None,
            connect_urls: Vec::new(),
            lame_duck: false,
        }
    }
}

impl RemoteInfo {
    /// Reads one `INFO` object into a fresh view of the server.
    fn parse(json: &[u8], limits: Limits, max_connect_urls: usize) -> Result<Self> {
        let parsed = ServerInfo::parse(json, limits)?;
        let mut info = Self::default();
        info.merge(&parsed, max_connect_urls);
        Ok(info)
    }

    /// Applies a later `INFO` over this one.
    ///
    /// Only the fields the server *stated* are replaced. A later `INFO` is
    /// commonly a topology notice and need not repeat the handshake's
    /// `max_payload` or `headers`; treating an absent field as a change would
    /// mean a `connect_urls` update silently revoking header support.
    ///
    /// `ldm` is the one field that only ever moves one way: a server that has
    /// entered Lame Duck Mode does not leave it, so a later `INFO` without
    /// the flag does not cancel the drain notice.
    fn merge(&mut self, parsed: &ServerInfo<'_>, max_connect_urls: usize) {
        if let Some(value) = &parsed.server_id {
            self.server_id = Some(value.to_string());
        }
        if let Some(value) = &parsed.server_name {
            self.server_name = Some(value.to_string());
        }
        if let Some(value) = &parsed.version {
            self.version = Some(value.to_string());
        }
        if let Some(value) = parsed.proto {
            self.proto = value;
        }
        if let Some(value) = &parsed.host {
            self.host = Some(value.to_string());
        }
        if let Some(value) = parsed.port {
            self.port = Some(value);
        }
        if let Some(value) = parsed.max_payload {
            self.max_payload = value;
        }
        if let Some(value) = parsed.headers {
            self.headers = value;
        }
        if let Some(value) = parsed.auth_required {
            self.auth_required = value;
        }
        if let Some(value) = parsed.tls_required {
            self.tls_required = value;
        }
        if let Some(value) = parsed.tls_verify {
            self.tls_verify = value;
        }
        if let Some(value) = &parsed.nonce {
            self.nonce = Some(value.to_string());
        }
        if let Some(urls) = &parsed.connect_urls {
            // Replaced rather than merged: "an `INFO` message is sent to the
            // client with an updated `connect_urls` list", so the list is the
            // topology as it now is and a client that accumulated would keep
            // servers that have left the cluster.
            self.connect_urls = urls
                .iter()
                .take(max_connect_urls)
                .map(std::string::ToString::to_string)
                .collect();
        }
        if parsed.ldm == Some(true) {
            self.lame_duck = true;
        }
    }
}

/// Where a connection is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// The handshake finished and the connection is usable.
    Connected,
    /// The transport was closed on purpose. NATS has no `CLOSE` verb: this is
    /// what ending it looks like.
    Closed,
    /// The connection failed; the string is the error as it was reported.
    Failed(String),
}

impl State {
    /// Whether anything further can be sent.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Connected)
    }
}

/// What the driver accepts from a handle.
#[derive(Debug)]
pub(crate) enum Command {
    /// Octets a handle has already encoded and already checked against
    /// `max_payload` and `max_control_line`.
    ///
    /// Encoding in the handle rather than in the driver is what makes an
    /// oversized publish fail **before the wire**: the caller gets the error
    /// from its own `publish` call and nothing was queued, let alone
    /// written.
    Write(Vec<u8>),
    /// `SUB`, with the `sid` the driver allocates.
    Subscribe {
        subject: Vec<u8>,
        queue_group: Option<Vec<u8>>,
        reply: oneshot::Sender<Result<Subscription>>,
    },
    /// `UNSUB`, with or without a message count.
    Unsubscribe { sid: u64, max_msgs: Option<u64> },
    /// Registers a reply subject on the inbox, subscribing to the inbox
    /// first where this is the connection's first request.
    Register {
        reply_subject: Vec<u8>,
        replies: mpsc::Sender<Message>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Drops a reply subject, because its caller is done waiting.
    Forget { reply_subject: Vec<u8> },
    /// `PING`, answered when the matching `PONG` arrives.
    Flush { done: oneshot::Sender<Result<()>> },
    /// End the transport.
    Close { done: oneshot::Sender<()> },
}

/// How TLS is reached, where it is reached at all.
///
/// Without the `tls` feature this enum has one variant, which is what makes
/// `INFO.tls_required` on a build with no TLS a plain
/// [`Error::TlsUnsupported`] rather than a path that silently continues in
/// the clear.
#[derive(Clone, Debug)]
enum TlsChoice {
    /// No `rustls::ClientConfig` was supplied, or this build has no TLS.
    Unavailable,
    #[cfg(feature = "tls")]
    Available {
        config: Arc<tokio_rustls::rustls::ClientConfig>,
        server_name: String,
    },
}

/// A NATS core connection.
///
/// Cloning shares the connection: every clone speaks to the same driver task
/// and sees the same state.
#[derive(Clone, Debug)]
pub struct Connection {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    exec: Exec,
    options: Arc<ConnectionOptions>,
    /// The publish path's copy of `INFO.max_payload`.
    ///
    /// Duplicated from the `INFO` watch on purpose: an asynchronous `INFO`
    /// can change it at any moment and every publish has to see the current
    /// value, so the check is one relaxed atomic load rather than a lock.
    max_payload: Arc<AtomicU64>,
    /// Whether `headers` was negotiated, which decides whether `HPUB` may be
    /// written at all.
    headers: bool,
    info: watch::Receiver<RemoteInfo>,
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<State>,
    inbox: Inbox,
}

impl Connection {
    /// Connects to `host:port` over plain TCP.
    ///
    /// Fails with [`Error::TlsRequired`] where the server's `INFO` demands
    /// TLS, because the trust decision needs the caller's
    /// `rustls::ClientConfig` and continuing in the clear would put a
    /// `CONNECT` — credentials and all — on a socket the server is about to
    /// stop reading.
    pub async fn connect(
        exec: &Exec,
        host: &str,
        port: u16,
        options: ConnectionOptions,
    ) -> Result<Self> {
        options.validate()?;
        if options.require_tls {
            return Err(Error::Configuration(
                "this configuration asks for TLS; use Connection::connect_tls, \
                 which takes the rustls::ClientConfig the trust decision needs"
                    .into(),
            ));
        }
        let stream = transport::connect(exec, host, port, &options).await?;
        Self::establish(exec, Wire::new(stream), options, TlsChoice::Unavailable).await
    }

    /// Connects to `host:port`, completing TLS before any ordinary traffic
    /// where the server's `INFO` requires it or
    /// [`ConnectionOptions::require_tls`] asks for it.
    ///
    /// `config` is the caller's: which certificates are valid is the
    /// application's decision. The SNI name is
    /// [`ConnectionOptions::tls_server_name`] or, failing that, `host`.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        exec: &Exec,
        host: &str,
        port: u16,
        options: ConnectionOptions,
        config: Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> Result<Self> {
        options.validate()?;
        let server_name = options
            .tls_server_name
            .clone()
            .unwrap_or_else(|| host.to_owned());
        let stream = transport::connect(exec, host, port, &options).await?;
        Self::establish(
            exec,
            Wire::new(stream),
            options,
            TlsChoice::Available {
                config,
                server_name,
            },
        )
        .await
    }

    /// Runs the handshake on an established stream and starts the driver.
    async fn establish(
        exec: &Exec,
        mut wire: Wire,
        options: ConnectionOptions,
        tls: TlsChoice,
    ) -> Result<Self> {
        let options = Arc::new(options);
        let max_connect_urls = options.max_connect_urls as usize;
        let mut limits = limits_for(&options, DEFAULT_MAX_PAYLOAD);

        // The server speaks first, unprompted. Nothing has been written yet
        // and nothing may be: the `CONNECT` this client is going to send is
        // assembled out of what this `INFO` says.
        let opening = step(
            exec,
            &options,
            "the server's opening INFO",
            wire.read_op(limits),
        )
        .await?;
        let Incoming::Info(json) = opening else {
            return Err(Error::Protocol(format!(
                "the first operation on a connection is INFO, and the server \
                 sent {}",
                describe(&opening)
            )));
        };
        let mut info = RemoteInfo::parse(&json, limits, max_connect_urls)?;
        limits = limits_for(&options, info.max_payload);

        let want_tls = info.tls_required || options.require_tls;
        if want_tls {
            match tls {
                #[cfg(feature = "tls")]
                TlsChoice::Available {
                    config,
                    server_name,
                } => {
                    // Before any ordinary traffic: the only octets that have
                    // crossed are the server's `INFO`, and everything this
                    // client says travels inside the session.
                    let stream =
                        transport::upgrade_tls(wire.into_stream()?, config, &server_name).await?;
                    wire = Wire::new(stream);
                }
                TlsChoice::Unavailable => {
                    return Err(if cfg!(feature = "tls") {
                        Error::TlsRequired
                    } else {
                        Error::TlsUnsupported
                    });
                }
            }
        }

        if info.auth_required && !options.credentials.is_some() {
            return Err(Error::AuthenticationRequired);
        }
        let headers = options.headers && info.headers;
        let signature = sign_nonce(&options, &info)?;
        let connect = local_connect(&options, &info, want_tls, headers, signature.as_ref());

        // `CONNECT` and `PING` in one write. The `PONG` that comes back is
        // the only acknowledgement the protocol offers for the `CONNECT`.
        let mut out = Vec::new();
        Op::Connect(connect).encode(&mut out)?;
        Op::Ping.encode(&mut out)?;
        wire.write_all(&out).await?;

        loop {
            let answer = step(
                exec,
                &options,
                "the PONG that confirms the CONNECT",
                wire.read_op(limits),
            )
            .await?;
            match answer {
                Incoming::Pong => break,
                // A server that pings during the handshake gets its answer;
                // the rule is bidirectional from the first octet.
                Incoming::Ping => {
                    let mut pong = Vec::new();
                    Op::Pong.encode(&mut pong)?;
                    wire.write_all(&pong).await?;
                }
                // An asynchronous `INFO` may arrive at any time, the
                // handshake included.
                Incoming::Info(json) => {
                    let parsed = ServerInfo::parse(&json, limits)?;
                    info.merge(&parsed, max_connect_urls);
                    limits = limits_for(&options, info.max_payload);
                }
                // `verbose` was left on by the caller.
                Incoming::Ok => {}
                Incoming::Err(reason) => return Err(Error::Server(reason)),
                Incoming::Msg(message) => {
                    return Err(Error::Protocol(format!(
                        "the server delivered a message on sid {} before this \
                         client had subscribed to anything",
                        message.sid
                    )));
                }
            }
        }

        let (reader, writer) = wire.split();
        let max_payload = Arc::new(AtomicU64::new(info.max_payload));
        let (commands, rx) = mpsc::channel(options.outgoing_queue);
        let (state_tx, state) = watch::channel(State::Connected);
        let (info_tx, info_rx) = watch::channel(info);
        let inbox = Inbox::new(&options.inbox_prefix);

        let driver = Driver {
            exec: exec.clone(),
            options: Arc::clone(&options),
            rx,
            commands: commands.clone(),
            state: state_tx,
            info: info_tx,
            max_payload: Arc::clone(&max_payload),
            table: Table::new(options.max_subscriptions),
            pending: Pending::new(options.max_pending_requests),
            inbox_pattern: inbox.pattern(),
            inbox_sid: None,
            next_sid: 1,
            pings_out: 0,
            flushes: Vec::new(),
        };
        exec.spawn(driver.run(reader, writer, limits));

        Ok(Self {
            inner: Arc::new(Inner {
                exec: exec.clone(),
                options,
                max_payload,
                headers,
                info: info_rx,
                commands,
                state,
                inbox,
            }),
        })
    }

    /// The latest `INFO`, handshake or asynchronous.
    #[must_use]
    pub fn info(&self) -> RemoteInfo {
        self.inner.info.borrow().clone()
    }

    /// The server's `max_payload`, as the last `INFO` left it.
    ///
    /// **The protocol's own bound**, and the one every publish is checked
    /// against before anything is written.
    #[must_use]
    pub fn max_payload(&self) -> u64 {
        self.inner.max_payload.load(Ordering::Relaxed)
    }

    /// Whether `headers` was negotiated, and therefore whether `HPUB` may be
    /// written and a `NATS/1.0 503` can arrive.
    #[must_use]
    pub fn headers_supported(&self) -> bool {
        self.inner.headers
    }

    /// Whether the server has announced Lame Duck Mode.
    #[must_use]
    pub fn is_lame_duck(&self) -> bool {
        self.inner.info.borrow().lame_duck
    }

    /// Waits for the server's drain notice.
    ///
    /// `true` where an `INFO` carried `ldm: true` — the server has stopped
    /// accepting new connections and is draining the ones it has, so a caller
    /// that wants to move has this much warning. `false` where the connection
    /// ended first.
    pub async fn lame_duck_notice(&self) -> bool {
        let mut info = self.inner.info.clone();
        loop {
            if info.borrow_and_update().lame_duck {
                return true;
            }
            if info.changed().await.is_err() {
                return false;
            }
        }
    }

    /// What this client was configured with.
    #[must_use]
    pub fn options(&self) -> &ConnectionOptions {
        &self.inner.options
    }

    /// The reactor this connection runs on.
    #[must_use]
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }

    /// Where the connection is now.
    #[must_use]
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }

    /// Waits for the connection to leave [`State::Connected`].
    pub async fn closed(&self) -> State {
        let mut state = self.inner.state.clone();
        loop {
            {
                let current = state.borrow_and_update();
                if !current.is_usable() {
                    return current.clone();
                }
            }
            if state.changed().await.is_err() {
                return State::Failed("the connection driver stopped".into());
            }
        }
    }

    /// `PUB <subject> <#bytes>`.
    ///
    /// Fails with [`Error::PayloadTooLarge`] **before anything is written**
    /// where the payload is above the server's `max_payload`. Success means
    /// the operation was queued for the driver and nothing more: Core NATS
    /// has no publish acknowledgement, and "a successful socket write only
    /// establishes that the client sent bytes toward its connected server"
    /// (`docs/research/nats.md` §5).
    pub async fn publish(
        &self,
        subject: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.publish_with(subject, None, None, payload).await
    }

    /// `PUB` or `HPUB`, with an optional reply subject and an optional header
    /// block.
    ///
    /// The reply subject is what makes a publication a request: "the reply
    /// subject that subscribers can use to send a response back to the
    /// publisher/requestor". A header block needs
    /// [`Connection::headers_supported`], because a server that did not
    /// advertise headers does not know the `HPUB` verb.
    pub async fn publish_with(
        &self,
        subject: impl AsRef<[u8]>,
        reply_to: Option<&[u8]>,
        headers: Option<&OwnedHeaders>,
        payload: impl AsRef<[u8]>,
    ) -> Result<()> {
        let bytes = self.encode_publish(subject.as_ref(), reply_to, headers, payload.as_ref())?;
        self.send(Command::Write(bytes)).await
    }

    /// `SUB <subject> <sid>`: ordinary interest, one copy per matching
    /// publication.
    pub async fn subscribe(&self, subject: impl AsRef<[u8]>) -> Result<Subscription> {
        self.subscribe_inner(subject.as_ref(), None).await
    }

    /// `SUB <subject> <queue group> <sid>`: one copy per publication *between
    /// all members of the group*.
    ///
    /// "For each publication, the server selects one eligible member from
    /// each matching queue group", and an ordinary subscription beside them
    /// still gets its own copy — a queue group is not a broker queue, it is a
    /// set of subscriptions sharing one delivery (§2, §4).
    pub async fn subscribe_with_queue_group(
        &self,
        subject: impl AsRef<[u8]>,
        queue_group: impl AsRef<[u8]>,
    ) -> Result<Subscription> {
        let group = queue_group.as_ref();
        crate::subject::check_queue_group(group)?;
        self.subscribe_inner(subject.as_ref(), Some(group.to_vec()))
            .await
    }

    /// Publishes a request and waits for one reply, for at most `window`.
    ///
    /// `window` is a plain argument and not an option with a default: a
    /// request API that can hang is the failure this method exists to
    /// prevent.
    ///
    /// Three outcomes, and they are different facts:
    ///
    /// * a reply — [`Ok`];
    /// * no responder was subscribed when the request was published —
    ///   [`Error::NoResponders`], which arrives in one round trip rather than
    ///   at the end of `window`, and only where `headers` and
    ///   `no_responders` were negotiated;
    /// * `window` elapsed — [`Error::RequestTimeout`].
    pub async fn request(
        &self,
        subject: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        window: Duration,
    ) -> Result<Message> {
        let (reply_subject, mut replies) = self.register(1).await?;
        let result = async {
            self.publish_with(
                subject.as_ref(),
                Some(&reply_subject),
                None,
                payload.as_ref(),
            )
            .await?;
            match self.inner.exec.within(window, replies.recv()).await {
                Some(Some(message)) => request::outcome(message),
                Some(None) => Err(Error::ConnectionGone),
                None => Err(Error::RequestTimeout { after: window }),
            }
        }
        .await;
        self.forget(reply_subject).await;
        result
    }

    /// Publishes a request and collects every reply that arrives inside
    /// `window`, up to `max_responses`.
    ///
    /// The scatter-gather form: "a requester can receive replies from
    /// multiple responders during its collection window; NATS documents this
    /// as scatter-gather" (§4). It returns **what arrived when the window
    /// closed**, which may be nothing — the caller asked for a window, not
    /// for an answer, so an empty collection is a result and not an error.
    ///
    /// A `NATS/1.0 503` is still [`Error::NoResponders`], because that is a
    /// definite answer rather than an empty window.
    ///
    /// `max_responses` is required and bounds the collection: it is the queue
    /// of messages waiting for this caller, and an unbounded one would let a
    /// misbehaving responder fill memory inside one window.
    pub async fn request_many(
        &self,
        subject: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        window: Duration,
        max_responses: usize,
    ) -> Result<Vec<Message>> {
        if max_responses == 0 {
            return Err(Error::Configuration(
                "max_responses must be at least 1: a scatter-gather that \
                 collects nothing is a publish"
                    .into(),
            ));
        }
        let (reply_subject, mut replies) = self.register(max_responses).await?;
        let mut collected: Vec<Message> = Vec::new();
        let result: Result<()> = async {
            self.publish_with(
                subject.as_ref(),
                Some(&reply_subject),
                None,
                payload.as_ref(),
            )
            .await?;
            // The window bounds the whole collection, not each reply. What
            // has been pushed by the time it closes is what is returned,
            // which is why the vector lives outside the future.
            self.inner
                .exec
                .within(window, async {
                    while collected.len() < max_responses {
                        match replies.recv().await {
                            Some(message) => collected.push(message),
                            None => break,
                        }
                    }
                })
                .await;
            Ok(())
        }
        .await;
        self.forget(reply_subject).await;
        result?;
        if collected.first().is_some_and(Message::is_no_responders) {
            return Err(Error::NoResponders);
        }
        Ok(collected)
    }

    /// `PING`, answered when the `PONG` comes back.
    ///
    /// The protocol's only round trip, and therefore the only way to learn
    /// that the server has read everything written before it. It is not a
    /// delivery confirmation: Core NATS has none.
    pub async fn flush(&self) -> Result<()> {
        let (done, wait) = oneshot::channel();
        self.send(Command::Flush { done }).await?;
        match self
            .inner
            .exec
            .within(self.inner.options.handshake_timeout, wait)
            .await
        {
            Some(Ok(result)) => result,
            Some(Err(_)) => Err(Error::ConnectionGone),
            None => Err(Error::HandshakeTimeout {
                step: "the PONG answering a flush",
            }),
        }
    }

    /// Ends the connection.
    ///
    /// There is no `CLOSE` verb: "a normal client protocol close has no
    /// dedicated `CLOSE` verb; ending the transport ends the connection and
    /// its subscriptions" (§1). Every operation queued before this call is
    /// written first, because the driver takes commands in order.
    ///
    /// Idempotent: closing a closed connection is `Ok(())`.
    pub async fn close(&self) -> Result<()> {
        if !self.state().is_usable() {
            return Ok(());
        }
        let (done, wait) = oneshot::channel();
        if self
            .inner
            .commands
            .send(Command::Close { done })
            .await
            .is_err()
        {
            return Ok(());
        }
        // The close budget is finite for the same reason every one in this
        // repository is; here there is nothing to wait *for* beyond the
        // driver draining what was queued.
        let _ = self
            .inner
            .exec
            .within(self.inner.options.handshake_timeout, wait)
            .await;
        Ok(())
    }

    async fn subscribe_inner(
        &self,
        subject: &[u8],
        queue_group: Option<Vec<u8>>,
    ) -> Result<Subscription> {
        crate::subject::check_subscribe_subject(subject)?;
        if !self.state().is_usable() {
            return Err(Error::ConnectionGone);
        }
        let (reply, wait) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Subscribe {
                subject: subject.to_vec(),
                queue_group,
                reply,
            })
            .await
            .map_err(|_| Error::ConnectionGone)?;
        match self
            .inner
            .exec
            .within(self.inner.options.handshake_timeout, wait)
            .await
        {
            Some(Ok(result)) => result,
            Some(Err(_)) => Err(Error::ConnectionGone),
            None => Err(Error::HandshakeTimeout {
                step: "the SUB reaching the connection driver",
            }),
        }
    }

    /// Registers a reply subject and returns it with the channel its replies
    /// will arrive on.
    async fn register(&self, capacity: usize) -> Result<(Vec<u8>, mpsc::Receiver<Message>)> {
        let reply_subject = self.inner.inbox.next_reply_subject().into_bytes();
        let (replies, rx) = mpsc::channel(capacity);
        let (reply, wait) = oneshot::channel();
        self.send(Command::Register {
            reply_subject: reply_subject.clone(),
            replies,
            reply,
        })
        .await?;
        match self
            .inner
            .exec
            .within(self.inner.options.handshake_timeout, wait)
            .await
        {
            Some(Ok(result)) => result?,
            Some(Err(_)) => return Err(Error::ConnectionGone),
            None => {
                return Err(Error::HandshakeTimeout {
                    step: "the inbox subscription",
                });
            }
        }
        Ok((reply_subject, rx))
    }

    async fn forget(&self, reply_subject: Vec<u8>) {
        let _ = self
            .inner
            .commands
            .send(Command::Forget { reply_subject })
            .await;
    }

    async fn send(&self, command: Command) -> Result<()> {
        if !self.state().is_usable() {
            return Err(Error::ConnectionGone);
        }
        self.inner
            .commands
            .send(command)
            .await
            .map_err(|_| Error::ConnectionGone)
    }

    /// Every local check a publish gets, and the octets it produces.
    ///
    /// All of it happens here, in the caller's own task, so that a refused
    /// publish is refused *before the wire*: nothing is queued, nothing is
    /// written, and the error is the return value of the call that asked.
    fn encode_publish(
        &self,
        subject: &[u8],
        reply_to: Option<&[u8]>,
        headers: Option<&OwnedHeaders>,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        crate::subject::check_publish_subject(subject)?;
        if let Some(reply_to) = reply_to {
            // A reply subject is a subject a responder will publish on, so it
            // has to be a literal one.
            crate::subject::check_publish_subject(reply_to)?;
        }
        if headers.is_some() && !self.inner.headers {
            return Err(Error::HeadersUnsupported);
        }

        // "Headers count within the `HPUB` total size and therefore within
        // the server's accepted message size", so the number checked is the
        // total and not the payload alone.
        let borrowed = headers.map(OwnedHeaders::as_borrowed);
        let declared = match &borrowed {
            Some(block) => (block.encoded_len() + payload.len()) as u64,
            None => payload.len() as u64,
        };
        let max = self.max_payload();
        if declared > max {
            return Err(Error::PayloadTooLarge { declared, max });
        }

        let mut out = Vec::with_capacity(declared as usize + subject.len() + 32);
        match &borrowed {
            Some(block) => Op::Hpub {
                subject,
                reply_to,
                headers: block.clone(),
                payload,
            }
            .encode(&mut out)?,
            None => Op::Pub {
                subject,
                reply_to,
                payload,
            }
            .encode(&mut out)?,
        }
        check_control_line(&out, self.inner.options.max_control_line)?;
        Ok(out)
    }
}

/// The largest bound in force on a line this client writes.
///
/// `max_control_line` is **the protocol's own**: it is a server option, and a
/// line above it earns `-ERR 'Maximum Control Line Exceeded'` followed by a
/// close. Checking it locally is the same trade as `max_payload` — one
/// comparison against a connection.
fn check_control_line(encoded: &[u8], max: usize) -> Result<()> {
    // The control line is everything up to the first `CR`; a subject or a
    // `sid` cannot contain one, because the codec's encoder refuses it.
    let len = encoded
        .iter()
        .position(|byte| *byte == b'\r')
        .unwrap_or(encoded.len());
    if len > max {
        return Err(Error::ControlLineTooLong { len, max });
    }
    Ok(())
}

/// The codec bounds this connection applies to the server's octets.
///
/// `max_connect_urls` is deliberately **not** the retention bound here. The
/// codec *refuses* a `connect_urls` array longer than its cap, because a JSON
/// array announces no length and the only place to stop is while reading it.
/// So the decode cap is never lower than
/// [`DEFAULT_MAX_CONNECT_URLS`](crate::options::DEFAULT_MAX_CONNECT_URLS):
/// a caller who wants to *keep* two URLs is stating a retention preference,
/// and a preference must not turn a cluster that grew past it into a dropped
/// connection. How many are kept is applied in
/// [`RemoteInfo::merge`] instead.
fn limits_for(options: &ConnectionOptions, max_payload: u64) -> Limits {
    Limits {
        max_payload,
        max_control_line: options.max_control_line,
        max_header_entries: options.max_header_entries,
        max_connect_urls: options
            .max_connect_urls
            .max(crate::options::DEFAULT_MAX_CONNECT_URLS),
    }
}

/// Signs `INFO.nonce` with the caller's signer, where the credentials have
/// one.
///
/// The nonce is handed to the signer **exactly as `INFO` carried it**: the
/// server verifies the signature against those octets, so decoding,
/// re-encoding or trimming them here would produce a signature it rejects.
fn sign_nonce(options: &ConnectionOptions, info: &RemoteInfo) -> Result<Option<NonceSignature>> {
    match &options.credentials {
        Credentials::Nkey(signer) => {
            let nonce = info.nonce.as_deref().ok_or(Error::NonceMissing)?;
            let signature = signer.sign(nonce.as_bytes()).map_err(Error::Signature)?;
            if signature.public_key.is_none() {
                return Err(Error::Signature(
                    "an NKey CONNECT carries the public key beside the signature, \
                     because that is what the server verifies against"
                        .into(),
                ));
            }
            Ok(Some(signature))
        }
        Credentials::Jwt {
            signer: Some(signer),
            ..
        } => {
            let nonce = info.nonce.as_deref().ok_or(Error::NonceMissing)?;
            Ok(Some(
                signer.sign(nonce.as_bytes()).map_err(Error::Signature)?,
            ))
        }
        Credentials::Jwt { signer: None, .. } if info.nonce.is_some() => Err(Error::Signature(
            "the server sent a nonce and these JWT credentials carry no signer \
             to answer it"
                .into(),
        )),
        _ => Ok(None),
    }
}

/// Builds this client's `CONNECT`.
fn local_connect<'a>(
    options: &'a ConnectionOptions,
    info: &RemoteInfo,
    want_tls: bool,
    headers: bool,
    signature: Option<&'a NonceSignature>,
) -> Connect<'a> {
    let mut connect = Connect {
        verbose: options.verbose,
        pedantic: options.pedantic,
        tls_required: want_tls,
        name: options.name.as_deref().map(Cow::Borrowed),
        lang: Some(Cow::Borrowed(LANG)),
        version: Some(Cow::Borrowed(env!("CARGO_PKG_VERSION"))),
        // "Sending `1` indicates that the client supports dynamic
        // reconfiguration of cluster topology changes by asynchronously
        // receiving `INFO` messages with known servers it can reconnect to" —
        // which this client does.
        protocol: Some(1),
        ..Connect::default()
    };
    // `echo` arrived with protocol level 1, so it is only claimed where the
    // server says it speaks it.
    if info.proto >= 1 {
        connect.echo = options.echo;
    }
    if headers {
        connect.headers = Some(true);
        // The reference makes the fast no-responder answer depend on headers,
        // so the two travel together or not at all.
        if options.no_responders {
            connect.no_responders = Some(true);
        }
    }
    match &options.credentials {
        Credentials::None => {}
        Credentials::Token(token) => connect.auth_token = Some(Cow::Borrowed(token)),
        Credentials::UserPassword { user, password } => {
            connect.user = Some(Cow::Borrowed(user));
            connect.pass = Some(Cow::Borrowed(password));
        }
        Credentials::Jwt { jwt, .. } => {
            connect.jwt = Some(Cow::Borrowed(jwt));
            // No `nkey`: the JWT already carries the public key the server
            // verifies the signature against.
            if let Some(signature) = signature {
                connect.sig = Some(Cow::Borrowed(&signature.signature));
            }
        }
        Credentials::Nkey(_) => {
            if let Some(signature) = signature {
                connect.sig = Some(Cow::Borrowed(&signature.signature));
                connect.nkey = signature.public_key.as_deref().map(Cow::Borrowed);
            }
        }
    }
    connect
}

/// Bounds one handshake step on wall-clock time.
///
/// Every step needs it and none of them has a deadline in the protocol — not
/// even the opening `INFO`, which a server that accepts a TCP connection and
/// then says nothing never sends.
async fn step<T, F>(
    exec: &Exec,
    options: &ConnectionOptions,
    name: &'static str,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match exec.within(options.handshake_timeout, future).await {
        Some(result) => result,
        None => Err(Error::HandshakeTimeout { step: name }),
    }
}

fn describe(incoming: &Incoming) -> &'static str {
    match incoming {
        Incoming::Info(_) => "INFO",
        Incoming::Msg(_) => "MSG",
        Incoming::Ping => "PING",
        Incoming::Pong => "PONG",
        Incoming::Ok => "+OK",
        Incoming::Err(_) => "-ERR",
    }
}

/// The task that owns the wire once the handshake is done.
struct Driver {
    exec: Exec,
    options: Arc<ConnectionOptions>,
    rx: mpsc::Receiver<Command>,
    /// Kept so that a `Subscription` handed to a caller can be given a
    /// sender to unsubscribe through, without the driver having to clone one
    /// out of its own receiver.
    commands: mpsc::Sender<Command>,
    state: watch::Sender<State>,
    info: watch::Sender<RemoteInfo>,
    max_payload: Arc<AtomicU64>,
    table: Table,
    pending: Pending,
    /// `<inbox stem>.>`, subscribed to lazily on the first request.
    inbox_pattern: String,
    /// The `sid` of that one subscription.
    ///
    /// Held apart from [`Table`] and not counted against
    /// `max_subscriptions`: it is the connection's own single subscription
    /// rather than one the application asked for, and there is exactly one of
    /// it.
    inbox_sid: Option<u64>,
    next_sid: u64,
    pings_out: u32,
    /// Callers waiting for a `PONG`.
    flushes: Vec<oneshot::Sender<Result<()>>>,
}

impl Driver {
    async fn run(mut self, mut reader: OpReader, mut writer: OpWriter, mut limits: Limits) {
        let exec = self.exec.clone();
        let ping_interval = self.options.ping_interval;
        let max_pings_out = self.options.max_pings_out;
        let max_connect_urls = self.options.max_connect_urls as usize;
        let outcome: State;

        loop {
            tokio::select! {
                biased;

                command = self.rx.recv() => match command {
                    Some(command) => {
                        if let Some(state) = self.command(command, &mut writer).await {
                            outcome = state;
                            break;
                        }
                    }
                    // Every handle is gone. There is no `CLOSE` verb to
                    // write: ending the transport is the close.
                    None => {
                        outcome = State::Closed;
                        break;
                    }
                },

                // Read inline rather than through a forwarding task:
                // `next_op` is cancel-safe, so a `select!` that drops it
                // keeps every octet it had buffered.
                incoming = reader.next_op(limits) => match incoming {
                    Ok(op) => {
                        match self.incoming(op, &mut writer, &mut limits, max_connect_urls).await {
                            Ok(()) => {}
                            Err(error) => {
                                outcome = State::Failed(error.to_string());
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        outcome = State::Failed(error.to_string());
                        break;
                    }
                },

                () = exec.sleep(ping_interval) => {
                    if self.pings_out >= max_pings_out {
                        // Bounded rather than infinite: a client that keeps
                        // pinging a server that never answers has replaced a
                        // failure with a silence.
                        outcome = State::Failed(
                            Error::StaleConnection {
                                unanswered: self.pings_out,
                                max: max_pings_out,
                            }
                            .to_string(),
                        );
                        break;
                    }
                    let mut ping = Vec::new();
                    if Op::Ping.encode(&mut ping).is_ok() {
                        if let Err(error) = writer.send(&ping).await {
                            outcome = State::Failed(error.to_string());
                            break;
                        }
                        self.pings_out += 1;
                    }
                }
            }
        }

        // "Core subscriptions vanish with the connection and have no stored
        // session" (§12 P2). Dropping the table and the inbox closes every
        // channel, which is how a caller waiting on a `Subscription` or a
        // request finds out rather than waiting forever.
        self.table.clear();
        self.pending.clear();
        for flush in self.flushes.drain(..) {
            let _ = flush.send(Err(Error::ConnectionGone));
        }
        let _ = writer.shutdown().await;
        self.state.send_replace(outcome);
    }

    /// One command. `Some(state)` ends the connection.
    async fn command(&mut self, command: Command, writer: &mut OpWriter) -> Option<State> {
        match command {
            Command::Write(bytes) => {
                if let Err(error) = writer.send(&bytes).await {
                    return Some(State::Failed(error.to_string()));
                }
            }

            Command::Subscribe {
                subject,
                queue_group,
                reply,
            } => {
                if !self.table.has_room() {
                    let _ = reply.send(Err(Error::TooManySubscriptions {
                        max: self.table.max(),
                    }));
                    return None;
                }
                let sid = self.take_sid();
                match self
                    .write_sub(writer, &subject, queue_group.as_deref(), sid)
                    .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        let fatal = matches!(error, Error::Io(_));
                        let message = error.to_string();
                        let _ = reply.send(Err(error));
                        // A refused encode is the caller's problem and the
                        // connection survives it; a broken socket is not.
                        return fatal.then_some(State::Failed(message));
                    }
                }
                let (tx, rx) = mpsc::channel(self.options.subscription_queue);
                self.table.insert(sid, subject.clone(), tx);
                let _ = reply.send(Ok(Subscription::new(
                    sid,
                    subject,
                    queue_group,
                    rx,
                    self.commands.clone(),
                )));
            }

            Command::Unsubscribe { sid, max_msgs } => {
                // `UNSUB <sid> <max_msgs>` leaves the subscription in place
                // until the count is reached, and the count is honoured here
                // as well as sent: the server may write the Nth message
                // before it reads the `UNSUB`.
                let known = match max_msgs {
                    Some(max) => self.table.set_max_msgs(sid, max),
                    None => self.table.remove(sid),
                };
                if !known {
                    // Already gone, so the `UNSUB` has either been written or
                    // was never needed. Writing a second one would ask the
                    // server about a `sid` it no longer has.
                    return None;
                }
                if let Err(error) = self.write_unsub(writer, sid, max_msgs).await {
                    return Some(State::Failed(error.to_string()));
                }
            }

            Command::Register {
                reply_subject,
                replies,
                reply,
            } => {
                if self.inbox_sid.is_none() {
                    let sid = self.take_sid();
                    let pattern = self.inbox_pattern.clone();
                    if let Err(error) = self.write_sub(writer, pattern.as_bytes(), None, sid).await
                    {
                        let message = error.to_string();
                        let _ = reply.send(Err(error));
                        return Some(State::Failed(message));
                    }
                    self.inbox_sid = Some(sid);
                }
                if self.pending.insert(reply_subject, replies) {
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(Error::TooManyPendingRequests {
                        max: self.pending.max(),
                    }));
                }
            }

            Command::Forget { reply_subject } => self.pending.remove(&reply_subject),

            Command::Flush { done } => {
                let mut ping = Vec::new();
                if Op::Ping.encode(&mut ping).is_ok() {
                    if let Err(error) = writer.send(&ping).await {
                        let message = error.to_string();
                        let _ = done.send(Err(error));
                        return Some(State::Failed(message));
                    }
                    self.pings_out += 1;
                    self.flushes.push(done);
                }
            }

            Command::Close { done } => {
                let _ = writer.shutdown().await;
                let _ = done.send(());
                return Some(State::Closed);
            }
        }
        None
    }

    /// One operation from the server.
    async fn incoming(
        &mut self,
        op: Incoming,
        writer: &mut OpWriter,
        limits: &mut Limits,
        max_connect_urls: usize,
    ) -> Result<()> {
        match op {
            Incoming::Ping => {
                // Bidirectional from the first octet: the server pings on its
                // own `ping_interval` and disconnects a client that does not
                // answer.
                let mut pong = Vec::new();
                Op::Pong.encode(&mut pong)?;
                writer.send(&pong).await?;
            }
            Incoming::Pong => {
                self.pings_out = 0;
                // A `PONG` says the server has read everything written
                // before the `PING` that earned it, which is what every
                // waiting flush was asking.
                for flush in self.flushes.drain(..) {
                    let _ = flush.send(Ok(()));
                }
            }
            Incoming::Info(json) => {
                let parsed = ServerInfo::parse(&json, *limits)?;
                self.info.send_modify(|info| {
                    info.merge(&parsed, max_connect_urls);
                });
                let info = self.info.borrow().clone();
                self.max_payload.store(info.max_payload, Ordering::Relaxed);
                *limits = limits_for(&self.options, info.max_payload);
                tracing::info!(
                    server_id = info.server_id.as_deref().unwrap_or("?"),
                    connect_urls = info.connect_urls.len(),
                    lame_duck = info.lame_duck,
                    max_payload = info.max_payload,
                    "asynchronous INFO"
                );
            }
            Incoming::Ok => {}
            Incoming::Err(reason) => {
                // "A protocol, authorization, or other runtime connection
                // error", and most of them are followed by the server
                // closing. Reported as the failure it is rather than logged
                // and ignored.
                return Err(Error::Server(reason));
            }
            Incoming::Msg(message) => self.dispatch(message, writer).await?,
        }
        Ok(())
    }

    /// Hands one message to whatever asked for it: the inbox, or the
    /// subscription whose `sid` it carries.
    async fn dispatch(&mut self, message: Message, writer: &mut OpWriter) -> Result<()> {
        if Some(message.sid) == self.inbox_sid {
            if !self.pending.dispatch(message) {
                // A reply whose caller has given up, or one that arrived for
                // a window that is over. Core NATS keeps nothing for replay,
                // so there is nothing to do but say so.
                tracing::debug!("a reply arrived on the inbox with nobody waiting for it");
            }
            return Ok(());
        }
        let sid = message.sid;
        match self.table.dispatch(sid, message) {
            Delivered::Ok { exhausted } => {
                if exhausted {
                    tracing::debug!(sid, "the auto-unsubscribe count was reached");
                }
            }
            Delivered::Dropped => {
                let subject = self
                    .table
                    .subject(sid)
                    .map(|subject| String::from_utf8_lossy(subject).into_owned())
                    .unwrap_or_default();
                // One copy, not the connection. Blocking here would stall
                // every other subscription and then earn a slow-consumer
                // disconnect anyway.
                tracing::warn!(
                    sid,
                    subject,
                    queue = self.options.subscription_queue,
                    "dropped a message: the subscription's queue is full"
                );
            }
            Delivered::Gone => {
                // The reader went away without an `UNSUB` reaching us first.
                self.write_unsub(writer, sid, None).await?;
            }
            Delivered::Unknown => {
                tracing::debug!(
                    sid,
                    "a message arrived for a subscription this client has already removed"
                );
            }
        }
        Ok(())
    }

    async fn write_sub(
        &self,
        writer: &mut OpWriter,
        subject: &[u8],
        queue_group: Option<&[u8]>,
        sid: u64,
    ) -> Result<()> {
        let sid_text = sid.to_string();
        let mut out = Vec::new();
        Op::Sub {
            subject,
            queue_group,
            sid: sid_text.as_bytes(),
        }
        .encode(&mut out)?;
        check_control_line(&out, self.options.max_control_line)?;
        writer.send(&out).await
    }

    async fn write_unsub(
        &self,
        writer: &mut OpWriter,
        sid: u64,
        max_msgs: Option<u64>,
    ) -> Result<()> {
        let sid_text = sid.to_string();
        let mut out = Vec::new();
        Op::Unsub {
            sid: sid_text.as_bytes(),
            max_msgs,
        }
        .encode(&mut out)?;
        writer.send(&out).await
    }

    /// A `sid` no other subscription on this connection has had.
    ///
    /// Monotonic, and never reused even after an `UNSUB`: a reused `sid`
    /// could collect a message the server wrote for the previous
    /// subscription before it read the `UNSUB`.
    fn take_sid(&mut self) -> u64 {
        let sid = self.next_sid;
        self.next_sid += 1;
        sid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(json: &str) -> RemoteInfo {
        RemoteInfo::parse(json.as_bytes(), Limits::DEFAULT, 8).expect("an INFO")
    }

    /// An absent field is not a statement: a server that omits
    /// `tls_required` has not demanded TLS, and one that omits `max_payload`
    /// gets the documented default rather than zero.
    #[test]
    fn absences_resolve_to_the_documented_defaults() {
        let bare = info("{}");
        assert_eq!(bare.max_payload, DEFAULT_MAX_PAYLOAD);
        assert!(!bare.tls_required);
        assert!(!bare.headers);
        assert!(!bare.auth_required);
        assert!(!bare.lame_duck);
        assert_eq!(bare.proto, 0);
    }

    /// A later `INFO` replaces what it states and leaves the rest alone, or a
    /// topology notice would revoke header support.
    #[test]
    fn a_later_info_merges_rather_than_replaces() {
        let mut first = info("{\"server_id\":\"S1\",\"headers\":true,\"max_payload\":64}");
        assert!(first.headers);

        let later = ServerInfo::parse(
            b"{\"connect_urls\":[\"10.0.0.1:4222\",\"10.0.0.2:4222\"],\"ldm\":true}",
            Limits::DEFAULT,
        )
        .expect("a later INFO");
        first.merge(&later, 8);

        assert!(first.headers, "a field the notice did not mention");
        assert_eq!(first.max_payload, 64);
        assert_eq!(first.server_id.as_deref(), Some("S1"));
        assert_eq!(first.connect_urls.len(), 2);
        assert!(first.lame_duck);

        // And lame duck does not un-set itself.
        let quiet = ServerInfo::parse(b"{\"server_id\":\"S1\"}", Limits::DEFAULT).expect("an INFO");
        first.merge(&quiet, 8);
        assert!(
            first.lame_duck,
            "a server that has entered lame duck mode does not leave it"
        );
    }

    /// The retained list is bounded, because it grows with the cluster.
    #[test]
    fn connect_urls_are_truncated_to_the_bound() {
        let listed = info("{\"connect_urls\":[\"a:1\",\"b:2\",\"c:3\",\"d:4\"]}");
        assert_eq!(listed.connect_urls.len(), 4);

        let parsed = ServerInfo::parse(
            b"{\"connect_urls\":[\"a:1\",\"b:2\",\"c:3\",\"d:4\"]}",
            Limits::DEFAULT,
        )
        .expect("an INFO");
        let mut capped = RemoteInfo::default();
        capped.merge(&parsed, 2);
        assert_eq!(
            capped.connect_urls,
            vec!["a:1".to_owned(), "b:2".to_owned()]
        );
    }

    /// The control-line bound applies to the line and not to the payload,
    /// which may be much larger and is framed by its declared count.
    #[test]
    fn the_control_line_bound_ignores_the_payload() {
        let mut out = Vec::new();
        Op::Pub {
            subject: b"a.b",
            reply_to: None,
            payload: &vec![b'x'; 4096],
        }
        .encode(&mut out)
        .expect("encodable");
        assert!(
            check_control_line(&out, 32).is_ok(),
            "`PUB a.b 4096` is twelve octets"
        );

        let long = vec![b'a'; 64];
        let mut out = Vec::new();
        Op::Pub {
            subject: &long,
            reply_to: None,
            payload: b"",
        }
        .encode(&mut out)
        .expect("encodable");
        let error = check_control_line(&out, 32).expect_err("refused");
        assert!(
            matches!(error, Error::ControlLineTooLong { max: 32, .. }),
            "{error}"
        );
    }

    /// An NKey `CONNECT` needs the public key: without it the server has
    /// nothing to verify the signature against, so a signer that returns none
    /// is a configuration error and not a connection that quietly fails
    /// authentication.
    #[test]
    fn an_nkey_signer_must_return_its_public_key() {
        use crate::options::Signer;

        let mut options = ConnectionOptions::new();
        options.credentials = Credentials::Nkey(Signer::new(|nonce| {
            Ok(NonceSignature {
                signature: String::from_utf8_lossy(nonce).into_owned(),
                public_key: None,
            })
        }));
        let with_nonce = RemoteInfo {
            nonce: Some("abc".into()),
            ..RemoteInfo::default()
        };
        let error = sign_nonce(&options, &with_nonce).expect_err("refused");
        assert!(matches!(error, Error::Signature(_)), "{error}");

        // And a nonce is required: signing nothing would be signing nothing.
        assert!(matches!(
            sign_nonce(&options, &RemoteInfo::default()),
            Err(Error::NonceMissing)
        ));
    }

    /// The nonce reaches the signer as `INFO` carried it, byte for byte.
    #[test]
    fn the_signer_sees_the_nonce_unchanged() {
        use crate::options::Signer;

        // A channel rather than a lock: the signer is a synchronous closure
        // and what the test wants is the octets it was handed.
        let (recorder, mut seen) = mpsc::unbounded_channel::<Vec<u8>>();
        let mut options = ConnectionOptions::new();
        options.credentials = Credentials::Nkey(Signer::new(move |nonce| {
            let _ = recorder.send(nonce.to_vec());
            Ok(NonceSignature {
                signature: "SIG".into(),
                public_key: Some("UPUB".into()),
            })
        }));

        let server = RemoteInfo {
            nonce: Some("nonce-0123_-".into()),
            ..RemoteInfo::default()
        };
        let signature = sign_nonce(&options, &server)
            .expect("signed")
            .expect("a signature");
        assert_eq!(seen.try_recv().expect("the signer ran"), b"nonce-0123_-");

        let connect = local_connect(&options, &server, false, false, Some(&signature));
        assert_eq!(connect.sig.as_deref(), Some("SIG"));
        assert_eq!(connect.nkey.as_deref(), Some("UPUB"));
    }

    /// Exactly one credential form travels, and the JWT form puts its
    /// signature in `sig` without an `nkey` — the JWT already carries the key
    /// the server verifies against.
    #[test]
    fn one_credential_form_at_a_time() {
        use crate::options::Signer;

        let mut options = ConnectionOptions::new();
        options.credentials = Credentials::Token("t0ken".into());
        let bare = RemoteInfo::default();
        let connect = local_connect(&options, &bare, false, false, None);
        assert_eq!(connect.auth_token.as_deref(), Some("t0ken"));
        assert!(connect.user.is_none() && connect.jwt.is_none() && connect.nkey.is_none());

        options.credentials = Credentials::UserPassword {
            user: "u".into(),
            password: "p".into(),
        };
        let connect = local_connect(&options, &bare, false, false, None);
        assert_eq!(connect.user.as_deref(), Some("u"));
        assert_eq!(connect.pass.as_deref(), Some("p"));
        assert!(connect.auth_token.is_none());

        let signature = NonceSignature {
            signature: "SIG".into(),
            public_key: Some("UPUB".into()),
        };
        options.credentials = Credentials::Jwt {
            jwt: "ey...".into(),
            signer: Some(Signer::new(|_| Err("never called in this test".to_owned()))),
        };
        let connect = local_connect(&options, &bare, false, false, Some(&signature));
        assert_eq!(connect.jwt.as_deref(), Some("ey..."));
        assert_eq!(connect.sig.as_deref(), Some("SIG"));
        assert!(
            connect.nkey.is_none(),
            "the JWT carries the public key already"
        );
    }

    /// `headers` and `no_responders` are claimed only where the server has
    /// headers, and `echo` only where the server speaks protocol level 1.
    #[test]
    fn capabilities_are_claimed_against_what_info_offered() {
        let mut options = ConnectionOptions::new();
        options.echo = Some(false);

        let old = RemoteInfo::default();
        let connect = local_connect(&options, &old, false, false, None);
        assert_eq!(connect.protocol, Some(1), "this client handles async INFO");
        assert!(connect.headers.is_none(), "the server has no headers");
        assert!(connect.no_responders.is_none());
        assert!(connect.echo.is_none(), "echo arrived with protocol level 1");

        let modern = RemoteInfo {
            proto: 1,
            headers: true,
            ..RemoteInfo::default()
        };
        let connect = local_connect(&options, &modern, false, true, None);
        assert_eq!(connect.headers, Some(true));
        assert_eq!(connect.no_responders, Some(true));
        assert_eq!(connect.echo, Some(false));
    }
}
