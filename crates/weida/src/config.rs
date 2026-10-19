//! Runtime and TLS configuration.
//!
//! Two questions, two types. [`Identity`] answers *who am I*: a certificate
//! chain and the private key behind it. [`Trust`] answers *whom do I accept*:
//! public-key fingerprints, certificate authorities, or the fingerprint the
//! dialled address itself names. [`ClientTls`] and [`ServerTls`] are the two
//! ways of combining them: a dialling endpoint always needs trust and may
//! carry an identity, a binding always needs an identity and may demand trust.

use std::path::PathBuf;
use std::time::Duration;

use weida_core::{Error, Fingerprint, Limits};
use weida_protocol::header::GuaranteeSet;
use weida_runtime::SharedResolver;
use zeroize::Zeroizing;

use crate::identity::{IdentitySource, TrustSource};
use crate::network::Network;
use crate::reconnect::{OutboxFull, ReconnectPolicy};

/// Whether a dialled authority may name a **set** of nodes.
///
/// The configuration knob of
/// [0020](https://github.com/tuco86/weida/blob/main/docs/decisions/0020-cluster-and-discovery.md)
/// §4.1: awareness is a capability, never a requirement, so both values speak
/// the same wire protocol and a peer behaves identically toward either.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Discovery {
    /// A URL without a port names a set: the host is resolved and every
    /// address it answers with is a candidate, dialled on
    /// [`weida_core::DEFAULT_PORT`].
    ///
    /// The default, and what a Kubernetes headless service answers with —
    /// "the set of IPs of all of the Pods selected by the Service", filtered
    /// by readiness.
    #[default]
    Aware,
    /// Every authority names exactly one endpoint, and a URL without a port
    /// is refused rather than resolved.
    ///
    /// For a deployment that puts one address in front of a cluster, and for
    /// an adapter edge that has no way to express a redirect to its foreign
    /// peer (0020 §4.1).
    Single,
}

/// Configuration for one [`crate::Runtime`].
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Resource limits applied to every connection this runtime owns or
    /// accepts.
    ///
    /// One profile, not yet one per connection tier: the control tier of
    /// `docs/decisions/0002-control-and-bulk-separation.md` §6.3 does not
    /// exist in the code yet, and a second profile nothing reads would be
    /// worse than none.
    pub limits: Limits,
    /// Concurrently accepted connections per binding. A QUIC Initial that
    /// arrives at the cap is refused (`CONNECTION_REFUSED`) before its
    /// handshake is accepted, so the dialler sees a refused connection and
    /// the binding builds no state for it. On a local socket transport the
    /// cap counts live local connections: each takes a place before it is
    /// accepted and keeps it for as long as it lives (a control connection
    /// for the life of its peer, a transfer or parked connection until its
    /// last half is gone), so a surplus connection waits in the kernel's
    /// queue instead of being accepted and closed, and a sender that outruns
    /// a slow consumer loses nothing.
    ///
    /// Per binding rather than per connection, which is why it is here and
    /// not in [`Limits`].
    pub max_connections: usize,
    /// Connections one **peer** may hold on one binding at once, counted by
    /// the fingerprint it proved.
    ///
    /// One connection per dialled endpoint path means the dialling side
    /// chooses how many connections it opens, so `max_connections` alone
    /// would let a single peer fill a binding: 64 connections to one peer
    /// measured about 50 MiB of transport state on the pair
    /// (`docs/IMPLEMENTATION.md` §4.8), and the default of 64 is that
    /// measured number — enough for a control connection plus 63 dialled
    /// paths, and a sixteenth of the default `max_connections`.
    ///
    /// Counted **per proved fingerprint**. Connections that proved no
    /// identity are each their own peer and are bounded only by
    /// `max_connections`, because two anonymous connections cannot be shown
    /// to be one peer ([`PROTOCOL.md`] §2.5,
    /// `docs/decisions/0008-session-identity.md` §4.2). A binding that wants
    /// this bound therefore requires a client identity.
    ///
    /// [`PROTOCOL.md`]: https://github.com/tuco86/weida/blob/main/docs/PROTOCOL.md
    pub max_connections_per_peer: usize,
    /// Depth of an endpoint's accept queue. Senders await a free slot, so QUIC
    /// flow control carries the backpressure to the peer.
    pub endpoint_queue: usize,
    /// Addresses a dialling endpoint will try for one hostname, in the order
    /// the resolver returned them.
    ///
    /// More than one is necessary because the first is not necessarily
    /// reachable — `localhost` commonly resolves to both `::1` and
    /// `127.0.0.1` — and a ceiling is necessary because a resolver answer is
    /// remote input.
    pub max_resolved_addresses: usize,
    /// Worker threads of the Tokio runtime [`crate::Runtime::owned`] creates.
    ///
    /// Ignored by [`crate::Runtime::new`] and [`crate::Runtime::with_handle`],
    /// which run on a reactor somebody else sized. One is the default because
    /// a messaging runtime is I/O bound and every extra worker is a thread a
    /// library takes from its host process without being asked; `0` is
    /// rejected rather than silently corrected.
    pub worker_threads: usize,
    /// How long [`crate::Runtime::shutdown`] waits for closed sockets to go
    /// idle before it returns anyway.
    ///
    /// The wait exists so peers see a clean `SHUTDOWN` rather than a timeout,
    /// and it is bounded because otherwise a peer's behaviour decides when
    /// this process may exit — the failure ZeroMQ's infinite `ZMQ_LINGER`
    /// default is known for
    /// (`docs/decisions/0009-drain.md` §4.4). QUIC's own closing and draining
    /// periods last about three times the path's probe timeout, so the default
    /// of one second is generous on any network where a clean close was
    /// possible at all, and it is not a deadline anything waits for twice:
    /// every endpoint is closed first, and only the idle wait is capped. The
    /// same bound caps [`crate::Binding::close`] and
    /// [`crate::Binding::hand_over`].
    pub shutdown_timeout: Duration,
    /// Guarantee set this runtime offers **and** requires of its peers
    /// (`docs/PROTOCOL.md` §6.1, §6.5).
    ///
    /// Offered and required are one setting in v0 on purpose: a set is a
    /// statement of what this side runs, and a peer that cannot match it
    /// fails the handshake rather than quietly giving less
    /// (`docs/decisions/0006-guarantee-sets.md` §4.4). The default is `core`,
    /// which is what every v0 peer declares by declaring nothing.
    pub guarantees: GuaranteeSet,
    /// Whether an authority may name a set of nodes ([`Discovery`]).
    pub discovery: Discovery,
    /// What a name means.
    ///
    /// The system resolver by default — A and AAAA, one port for the whole
    /// set. Replace it to answer from DNS SRV, a service registry or a table:
    /// a cloud load balancer that forwards several ports from one address is
    /// the deployment the system resolver **cannot** express, and
    /// [`weida_runtime::Resolver`] is how it brings its own answer
    /// ([decisions/0020](../../../docs/decisions/0020-cluster-and-discovery.md)
    /// §4.2).
    ///
    /// A deadline a caller puts around a dial (`send_timeout`, a timeout
    /// around `connect`) ends the wait for the resolver, not the lookup: the
    /// system resolver's thread outlives the deadline until the OS answers,
    /// and [`weida_runtime::SystemResolver`] caps how many such lookups are
    /// outstanding at once.
    pub resolver: SharedResolver,
    /// Where this runtime's QUIC sockets live: the operating system's UDP (the default) or, with
    /// the cargo feature `test-util`, a `SimNet` shared by the runtimes of one test. Local
    /// transports (`weida+inproc`, `+unix`, `+pipe`) are not affected.
    ///
    /// With `test-util`, `Network::sim(&net)` puts this runtime on `net`: give every runtime of
    /// one test the same `SimNet`
    /// ([decisions/0042](../../../docs/decisions/0042-simulated-network.md)).
    pub network: Network,
    /// How long a dial waits on one resolved address before trying the next.
    ///
    /// Only the addresses *before* the last one are bounded by it: a name
    /// that resolves to one address, and every IP literal, keeps the full
    /// handshake budget. The default of 250 ms is RFC 8305's Connection
    /// Attempt Delay, which exists for exactly this case — `localhost`
    /// resolving to `::1` before `127.0.0.1`, where the first address answers
    /// nothing at all and QUIC has no refusal to observe, so without a bound
    /// the second address is reached only after a handshake timeout.
    ///
    /// It does **not** cover the name lookup that precedes the first
    /// attempt: the system resolver's `getaddrinfo` blocks a thread and no
    /// deadline can end it. The wait for it can be dropped, the lookup thread
    /// outlives the deadline, and at most
    /// [`weida_runtime::MAX_OUTSTANDING_LOOKUPS`] of them exist at once; a dial
    /// beyond that fails with [`crate::Error::LimitExceeded`] rather than
    /// waiting for a free place.
    pub connect_attempt_timeout: Duration,
    /// How a dialling endpoint redials an address whose connection it lost
    /// ([decisions/0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)
    /// §4.5). The default doubles from 100 ms to 30 s with jitter and never
    /// gives up; [`ReconnectPolicy::never`] is the behaviour before 0031.
    pub reconnect: ReconnectPolicy,
    /// How long an `open` on a dialling endpoint waits for a peer to come
    /// back before it fails with the loss cause; `None` waits as long as the
    /// redial does (0031 §4.4). ZeroMQ's `ZMQ_SNDTIMEO`, with its default.
    ///
    /// The redial it waits on starts with a name lookup, and a lookup the
    /// deadline abandons keeps its thread until the system resolver answers
    /// ([`weida_runtime::SystemResolver`]).
    pub send_timeout: Option<Duration>,
    /// Bodies a dialling endpoint's outbox holds while no peer is live, and
    /// the bytes they may sum to (0031 §4.2). At either bound
    /// [`RuntimeConfig::outbox_full`] decides. ZeroMQ's `ZMQ_SNDHWM` of 1000
    /// for the count; the byte bound is what ZeroMQ lacks and
    /// `docs/INVARIANTS.md` needs.
    pub outbox_messages: usize,
    /// See [`RuntimeConfig::outbox_messages`].
    pub outbox_bytes: usize,
    /// What a `send` does at the outbox bound: wait, drop and count, or
    /// fail. Local to the sender, unlike the negotiated
    /// [`GuaranteeSet::backpressure`], because a puller has no say in how
    /// its pusher waits.
    pub outbox_full: OutboxFull,
    /// Handshakes one QUIC binding runs at once, counted from the moment an
    /// Initial is accepted until its handshake completes or fails.
    ///
    /// Accepting an Initial builds the connection's state and runs the
    /// server half of the TLS handshake before the dialler has proved the
    /// address it wrote, so what a spoofed or silent source can make a
    /// binding hold is this count and nothing else: an Initial that finds
    /// the binding at the cap is refused (`CONNECTION_REFUSED`) before any
    /// state exists for it. A completed handshake no longer counts here and
    /// is bounded by [`RuntimeConfig::max_connections`] instead.
    pub max_handshakes: usize,
    /// Handshakes in progress from which a QUIC binding answers an Initial
    /// from an address it has not validated with a stateless Retry instead
    /// of accepting it.
    ///
    /// A Retry makes the dialler prove it can receive at the address it
    /// wrote and costs it one round trip, so below this count an Initial is
    /// accepted at once and a quiet binding adds no latency; above it only
    /// a source that answered gets a handshake, and a flood from spoofed
    /// sources allocates nothing. `0` retries every dial from an
    /// unvalidated address, `usize::MAX` never does.
    pub retry_above_handshakes: usize,
    /// How long the body of one received transfer may take, from the moment
    /// its header was read to the end of the body, where the library collects
    /// that body for the caller: the blocking receives of `weida::blocking`
    /// and `weida serve`. Expiry ends that transfer only, which stops
    /// its stream, and the receive fails with [`Error::Expired`].
    ///
    /// The failure this prevents is one peer parking a single-threaded
    /// consumer. A peer that sends a header and then stalls, or sends one byte
    /// inside every idle window, keeps `idle_timeout` from ever firing, so the
    /// consumer would receive nothing from anyone else for as long as that
    /// connection chose. `None` waits as long as the peer likes.
    pub receive_timeout: Option<Duration>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            limits: Limits::default(),
            max_connections_per_peer: 64,
            max_connections: 1024,
            endpoint_queue: 256,
            max_resolved_addresses: 8,
            worker_threads: 1,
            shutdown_timeout: Duration::from_secs(1),
            guarantees: GuaranteeSet::CORE,
            connect_attempt_timeout: Duration::from_millis(250),
            discovery: Discovery::default(),
            resolver: SharedResolver::default(),
            network: Network::default(),
            reconnect: ReconnectPolicy::default(),
            send_timeout: None,
            outbox_messages: 1000,
            outbox_full: OutboxFull::default(),
            outbox_bytes: 8 * 1024 * 1024,
            max_handshakes: 256,
            retry_above_handshakes: 64,
            receive_timeout: Some(Duration::from_secs(30)),
        }
    }
}

/// Where PEM material comes from.
///
/// Certificates and keys are not always files. They arrive from a secret
/// manager, from an environment variable, from `include_str!`, or from a
/// Kubernetes secret already in memory. Requiring a path would force callers
/// to write private keys to disk just to hand them back — so both sources are
/// first class, and neither is a workaround for the other.
///
/// The in-memory form is a [`Zeroizing`] buffer: the key it may hold is wiped
/// when the last copy is dropped, and a clone is itself a zeroizing buffer, so
/// cloning an [`Identity`] never leaves an unwiped copy of the key behind.
#[derive(Clone, PartialEq, Eq)]
pub enum Pem {
    /// PEM bytes already in memory.
    Bytes(Zeroizing<Vec<u8>>),
    /// A PEM file, read when the TLS configuration is built.
    File(PathBuf),
}

impl std::hash::Hash for Pem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Pem::Bytes(bytes) => bytes.as_slice().hash(state),
            Pem::File(path) => path.hash(state),
        }
    }
}

impl Pem {
    /// Names the source for an error message.
    pub(crate) fn describe(&self) -> String {
        match self {
            Pem::Bytes(_) => "in-memory PEM".to_owned(),
            Pem::File(path) => path.display().to_string(),
        }
    }
}

/// Never prints the bytes: a `Pem` may hold a private key, and `Debug` output
/// ends up in logs.
impl std::fmt::Debug for Pem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pem::Bytes(b) => write!(f, "Pem::Bytes({} bytes)", b.len()),
            Pem::File(p) => f.debug_tuple("Pem::File").field(p).finish(),
        }
    }
}

impl From<PathBuf> for Pem {
    fn from(path: PathBuf) -> Pem {
        Pem::File(path)
    }
}

/// Who this endpoint is: a certificate chain and the private key behind it.
///
/// An identity is what a peer authenticates. Its [`Identity::fingerprint`] is
/// the value other endpoints pin, embed in an address, or list in a
/// [`Trust`] — the way an SSH host key or a WireGuard public key is the whole
/// of a peer's identity. The certificate around the key exists because TLS
/// requires one; when the fingerprint is what is trusted, nothing in the
/// certificate is consulted.
///
// A link to a `#[cfg(feature = "generate")]` item is a hard rustdoc error in
// the configuration that lacks the item, so the sentence naming it is
// conditional too.
#[cfg_attr(
    feature = "generate",
    doc = "Generated identities ([`Identity::generate`]) are self-signed and made for"
)]
#[cfg_attr(
    not(feature = "generate"),
    doc = "Generated identities (`Identity::generate`, behind the `generate` feature) are self-signed and made for"
)]
/// pinning. Identities issued by a certificate authority work the same way and
/// can *additionally* be trusted through that authority ([`Trust::anchor`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Identity {
    /// PEM source holding the certificate chain, leaf first.
    pub cert_chain: Pem,
    /// PEM source holding the private key.
    pub key: Pem,
}

impl Identity {
    /// A fresh self-signed identity with a new key, held only in memory.
    ///
    /// Made for pinning: the certificate carries no names, so it can only be
    /// trusted by fingerprint. Persist it with [`Identity::to_pem`] if the
    /// fingerprint must survive a restart.
    #[cfg(feature = "generate")]
    pub fn generate() -> Result<Identity, Error> {
        Identity::generate_for(Vec::<String>::new())
    }

    /// A fresh self-signed identity whose certificate also names `names`
    /// (DNS names or IP literals), so that a peer trusting the certificate
    /// itself as an anchor ([`Trust::anchor`]) can verify the host it dialled.
    #[cfg(feature = "generate")]
    pub fn generate_for(
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Identity, Error> {
        let names: Vec<String> = names.into_iter().map(Into::into).collect();
        let generated = rcgen::generate_simple_self_signed(names)
            .map_err(|e| Error::Tls(format!("generating an identity: {e}")))?;
        Ok(Identity {
            cert_chain: Pem::Bytes(Zeroizing::new(generated.cert.pem().into_bytes())),
            key: Pem::Bytes(Zeroizing::new(
                generated.signing_key.serialize_pem().into_bytes(),
            )),
        })
    }

    /// An identity from PEM buffers already in memory.
    ///
    /// Preferred when the key comes from a secret store: it never touches the
    /// filesystem, so there is no key file to create, protect and unlink. The
    /// two buffers may be the same bytes; sections of the wrong kind are
    /// skipped.
    ///
    /// The buffers are kept in zeroizing storage: handing a `Vec<u8>` or a
    /// `String` moves it in without a copy, and the memory is wiped when the
    /// identity and every clone of it are gone. A borrowed `&str` or `&[u8]`
    /// is copied once, and the caller's own buffer is the caller's to wipe.
    pub fn from_pem(cert_chain: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Identity {
        Identity {
            cert_chain: Pem::Bytes(Zeroizing::new(cert_chain.into())),
            key: Pem::Bytes(Zeroizing::new(key.into())),
        }
    }

    /// An identity from a chain file and a key file.
    pub fn from_pem_files(cert_chain: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Identity {
        Identity {
            cert_chain: Pem::File(cert_chain.into()),
            key: Pem::File(key.into()),
        }
    }

    /// An identity from one PEM file holding both the chain and the key —
    /// the form [`Identity::to_pem`] writes.
    pub fn from_pem_file(path: impl Into<PathBuf>) -> Identity {
        let path = path.into();
        Identity {
            cert_chain: Pem::File(path.clone()),
            key: Pem::File(path),
        }
    }

    /// The fingerprint of this identity's public key: what peers pin.
    ///
    /// Reads the certificate source, so it can fail on a missing file or a
    /// malformed certificate.
    pub fn fingerprint(&self) -> Result<Fingerprint, Error> {
        let chain = crate::tls::certs_from(&self.cert_chain)?;
        crate::tls::spki_fingerprint(&chain[0])
    }

    /// The certificate chain as PEM: publishable, contains no key.
    pub fn certificate_pem(&self) -> Result<String, Error> {
        crate::tls::certificate_pem(&self.cert_chain)
    }

    /// Chain and key as one PEM document, for persisting a generated identity.
    ///
    /// Contains the private key. Write it with owner-only permissions and
    /// load it back with [`Identity::from_pem_file`].
    ///
    /// The returned `String` is an ordinary buffer holding the key, and
    /// wiping it is the caller's job (wrap it in `zeroize::Zeroizing`).
    pub fn to_pem(&self) -> Result<String, Error> {
        let mut out = self.certificate_pem()?;
        out.push_str(&crate::tls::key_pem(&self.key)?);
        Ok(out)
    }
}

/// Whom a peer is accepted as.
///
/// A peer is accepted if its public-key fingerprint is one of `pins`, **or**
/// its certificate chains to one of `anchors` and names the host that was
/// dialled. An address that names a fingerprint
/// (`weida://sha256:…@host:port/path`) overrides both: only that identity is
/// accepted on that connection.
///
/// An empty `Trust` — [`Trust::by_address`] — accepts nothing but what the
/// address names. Dialling an address without a fingerprint under it fails
/// before any packet is sent. There is no platform root store and no
/// verification bypass anywhere in the shipped code.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Trust {
    /// PEM sources holding certificate-authority certificates.
    pub anchors: Vec<Pem>,
    /// Public-key fingerprints accepted outright.
    pub pins: Vec<Fingerprint>,
}

impl Trust {
    /// Trust only what each dialled address names.
    pub fn by_address() -> Trust {
        Trust::default()
    }

    /// Trust one public-key fingerprint.
    pub fn pin(fingerprint: Fingerprint) -> Trust {
        Trust::default().and_pin(fingerprint)
    }

    /// Trust the certificate authorities in one PEM buffer.
    pub fn anchor(pem: impl Into<Vec<u8>>) -> Trust {
        Trust::default().and_anchor(pem)
    }

    /// Trust the certificate authorities in one PEM file.
    pub fn anchor_file(path: impl Into<PathBuf>) -> Trust {
        Trust::default().and_anchor_file(path)
    }

    /// Adds a fingerprint.
    pub fn and_pin(mut self, fingerprint: Fingerprint) -> Trust {
        if !self.pins.contains(&fingerprint) {
            self.pins.push(fingerprint);
        }
        self
    }

    /// Adds the certificate authorities in a PEM buffer.
    pub fn and_anchor(mut self, pem: impl Into<Vec<u8>>) -> Trust {
        self.anchors.push(Pem::Bytes(Zeroizing::new(pem.into())));
        self
    }

    /// Adds the certificate authorities in a PEM file.
    pub fn and_anchor_file(mut self, path: impl Into<PathBuf>) -> Trust {
        self.anchors.push(Pem::File(path.into()));
        self
    }

    /// True when nothing is trusted beyond what an address names.
    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty() && self.pins.is_empty()
    }
}

/// The TLS ALPN token a binding offers or a dial asks for.
///
/// A binding serves exactly one token and a dial offers exactly one, so a
/// connection of one kind can never carry traffic of the other: a mismatch
/// fails the TLS handshake (PROTOCOL.md §2.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Alpn {
    /// `weida/0`: application connections.
    #[default]
    Weida,
    /// `weida-raft/0`: consensus connections of `weida-raft`
    /// ([decisions/0021](../../../docs/decisions/0021-consensus-openraft.md) §4.4).
    Raft,
}

impl Alpn {
    /// The token as it goes on the wire.
    pub const fn token(self) -> &'static [u8] {
        match self {
            Alpn::Weida => weida_protocol::ALPN,
            Alpn::Raft => weida_protocol::RAFT_ALPN,
        }
    }
}

/// TLS configuration of a dialling endpoint: what it trusts and, optionally,
/// who it is.
///
/// Both are sources ([`TrustSource`], [`IdentitySource`]): a value converts
/// into a source that never changes, and a source that does — a directory
/// an agent rewrites, an authority that renews — is read again on every
/// connection this endpoint builds
/// ([decisions/0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md)).
///
/// Equality is by content for static sources and by identity for the rest,
/// and the connection pool keys on it: two endpoints dialling the same
/// authority under different trust or identity must never share a
/// connection, or one would be using a peer authenticated on the other's
/// terms — while two configured alike do share one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientTls {
    /// Whom to accept as the peer.
    pub trust: TrustSource,
    /// The identity to present. `None` dials anonymously, which a binding
    /// that requires clients ([`ServerTls::require_client`]) refuses, under
    /// [`ClientTrust::AnyKey`] as under a trust.
    pub identity: Option<IdentitySource>,
    /// The ALPN token to ask for; [`Alpn::Weida`] unless changed with
    /// [`ClientTls::with_alpn`]. Part of the pool key, so a dial under one
    /// token never reuses a connection negotiated under the other.
    pub alpn: Alpn,
}

impl ClientTls {
    /// Dials anonymously under `trust`.
    pub fn new(trust: impl Into<TrustSource>) -> ClientTls {
        ClientTls {
            trust: trust.into(),
            identity: None,
            alpn: Alpn::Weida,
        }
    }

    /// Presents `identity` — a value or a source — to every peer.
    pub fn with_identity(mut self, identity: impl Into<IdentitySource>) -> ClientTls {
        self.identity = Some(identity.into());
        self
    }

    /// Asks for `alpn` instead of [`Alpn::Weida`].
    pub fn with_alpn(mut self, alpn: Alpn) -> ClientTls {
        self.alpn = alpn;
        self
    }
}

impl From<Trust> for ClientTls {
    fn from(trust: Trust) -> ClientTls {
        ClientTls::new(trust)
    }
}

impl From<TrustSource> for ClientTls {
    fn from(trust: TrustSource) -> ClientTls {
        ClientTls::new(trust)
    }
}

/// Whom a binding lets in when it requires clients to present a key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ClientTrust {
    /// Keys this source trusts: pinned, or chaining to an anchor.
    Trusted(TrustSource),
    /// Any key the client proves it holds. The handshake signature is
    /// verified and nothing about the key is judged: no pin, no issuer, no
    /// validity dates
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.1).
    AnyKey,
}

impl From<TrustSource> for ClientTrust {
    fn from(trust: TrustSource) -> ClientTrust {
        ClientTrust::Trusted(trust)
    }
}

impl From<Trust> for ClientTrust {
    fn from(trust: Trust) -> ClientTrust {
        ClientTrust::Trusted(TrustSource::from(trust))
    }
}

/// TLS configuration of a binding: who it is and, optionally, whom it lets in.
///
/// The identity is a source: a binding serves whatever the source holds at
/// each handshake, so a certificate renewed under it — by a file the agent
/// rewrote, by an authority the process asked — reaches the next peer
/// without a re-bind
/// ([decisions/0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md)
/// §4.1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerTls {
    /// The identity presented to every dialling peer.
    pub identity: IdentitySource,
    /// When set, every peer must present a key and prove it holds it;
    /// anonymous peers fail the handshake. [`ClientTrust::Trusted`] also
    /// refuses a key its trust does not accept, [`ClientTrust::AnyKey`]
    /// refuses none. When `None`, peers are anonymous and
    /// [`crate::IncomingMeta::peer`] is `None`.
    pub client_trust: Option<ClientTrust>,
    /// The one ALPN token this binding offers; [`Alpn::Weida`] unless
    /// changed with [`ServerTls::with_alpn`].
    pub alpn: Alpn,
}

impl ServerTls {
    /// Serves as `identity` — a value or a source — accepting anonymous
    /// peers.
    pub fn new(identity: impl Into<IdentitySource>) -> ServerTls {
        ServerTls {
            identity: identity.into(),
            client_trust: None,
            alpn: Alpn::Weida,
        }
    }

    /// Requires every peer to present a key: one `trust` accepts — a
    /// [`Trust`] or a [`TrustSource`] — or, with [`ClientTrust::AnyKey`],
    /// any key the peer proves it holds.
    pub fn require_client(mut self, trust: impl Into<ClientTrust>) -> ServerTls {
        self.client_trust = Some(trust.into());
        self
    }

    /// Offers `alpn` instead of [`Alpn::Weida`].
    pub fn with_alpn(mut self, alpn: Alpn) -> ServerTls {
        self.alpn = alpn;
        self
    }
}

impl From<Identity> for ServerTls {
    fn from(identity: Identity) -> ServerTls {
        ServerTls::new(identity)
    }
}

impl From<IdentitySource> for ServerTls {
    fn from(identity: IdentitySource) -> ServerTls {
        ServerTls::new(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_protocol_document() {
        let c = RuntimeConfig::default();
        assert_eq!(c.limits, Limits::default());
        assert_eq!(c.limits.keep_alive, Duration::from_secs(10));
        assert_eq!(c.limits.idle_timeout, Duration::from_secs(30));
        assert_eq!(c.max_connections, 1024);
        assert_eq!(c.max_connections_per_peer, 64);
        assert_eq!(c.endpoint_queue, 256);
        assert_eq!(c.max_resolved_addresses, 8);
        assert_eq!(c.max_handshakes, 256);
        assert_eq!(c.retry_above_handshakes, 64);
    }

    #[test]
    fn a_pem_source_never_prints_its_bytes() {
        // Error messages and Debug output must not leak key material.
        assert_eq!(Pem::File("/tmp/ca.pem".into()).describe(), "/tmp/ca.pem");
        assert_eq!(
            Pem::Bytes(Zeroizing::new(b"secret".to_vec())).describe(),
            "in-memory PEM"
        );
        let debug = format!("{:?}", Identity::from_pem("-cert-", "-secret-key-"));
        assert!(!debug.contains("secret"), "{debug}");
    }

    #[test]
    fn trust_composes_and_deduplicates_pins() {
        let fp = Fingerprint::from_bytes([7; 32]);
        let t = Trust::pin(fp).and_pin(fp).and_anchor("-ca-");
        assert_eq!(t.pins, vec![fp]);
        assert_eq!(
            t.anchors,
            vec![Pem::Bytes(Zeroizing::new(b"-ca-".to_vec()))]
        );
        assert!(!t.is_empty());
        assert!(Trust::by_address().is_empty());
    }

    #[test]
    fn a_combined_pem_file_feeds_both_sources() {
        let id = Identity::from_pem_file("/tmp/id.pem");
        assert_eq!(id.cert_chain, Pem::File("/tmp/id.pem".into()));
        assert_eq!(id.key, Pem::File("/tmp/id.pem".into()));
    }

    #[test]
    fn different_trust_or_identity_is_a_different_client_tls() {
        // The pool keys on `ClientTls`; equality decides connection sharing.
        let fp = Fingerprint::from_bytes([1; 32]);
        let a = ClientTls::new(Trust::pin(fp));
        let b = ClientTls::new(Trust::pin(Fingerprint::from_bytes([2; 32])));
        let c = a.clone().with_identity(Identity::from_pem("-c-", "-k-"));
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a, ClientTls::from(Trust::pin(fp)));
    }
}
