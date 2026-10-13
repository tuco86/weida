//! TLS and QUIC transport parameters.
//!
//! The ALPN token is set on both sides, so a peer speaking a different protocol
//! fails the TLS handshake rather than reaching our frame parser.
//!
//! The crypto provider is named explicitly instead of relying on the rustls
//! process default: a library must not depend on, or install, global state in
//! its host application.
//!
//! Peer verification is one policy applied in both directions
//! ([`Policy`]): a presented leaf certificate passes if its public-key
//! fingerprint is pinned, or — when anchors are configured — if it chains to
//! one of them under the usual webpki rules. A fingerprint named by the dialled
//! address is the only thing accepted on that connection. A binding may
//! instead accept any key a client proves ([`ClientTrust::AnyKey`], judged by
//! nothing but the chain bound). Whatever the trust path, the handshake
//! signature is verified with the provider's algorithms, so a peer is only
//! ever accepted for a key it proved it holds.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use quinn::rustls::client::ResolvesClientCert;
use quinn::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use quinn::rustls::crypto::CryptoProvider;
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use quinn::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use quinn::rustls::server::{ClientHello, ResolvesServerCert};
use quinn::rustls::sign::CertifiedKey;
use quinn::rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme,
};
use quinn::{TransportConfig, VarInt};
use weida_core::{Error, Fingerprint, Limits};

use crate::config::{ClientTls, ClientTrust, Identity, Pem, ServerTls, Trust};
use crate::identity::{IdentitySource, PeerChain, TrustSource};

/// Reads a certificate chain from either PEM source.
pub(crate) fn certs_from(pem: &Pem) -> Result<Vec<CertificateDer<'static>>, Error> {
    let chain: Vec<CertificateDer<'static>> = match pem {
        Pem::Bytes(bytes) => CertificateDer::pem_slice_iter(bytes)
            .collect::<Result<_, _>>()
            .map_err(|e| tls_err("parsing certificates", e))?,
        Pem::File(path) => CertificateDer::pem_file_iter(path)
            .map_err(|e| tls_err("reading certificates", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| tls_err("parsing certificates", e))?,
    };
    if chain.is_empty() {
        return Err(Error::Tls(format!(
            "{} contains no certificates",
            pem.describe()
        )));
    }
    Ok(chain)
}

/// Reads a private key from either PEM source.
fn key_from(pem: &Pem) -> Result<PrivateKeyDer<'static>, Error> {
    match pem {
        Pem::Bytes(bytes) => PrivateKeyDer::from_pem_slice(bytes),
        Pem::File(path) => PrivateKeyDer::from_pem_file(path),
    }
    .map_err(|e| tls_err("reading the private key", e))
}

/// Re-encodes the certificate chain of a PEM source as PEM, and nothing else.
pub(crate) fn certificate_pem(pem: &Pem) -> Result<String, Error> {
    let mut out = String::new();
    for cert in certs_from(pem)? {
        pem_block(&mut out, "CERTIFICATE", &cert);
    }
    Ok(out)
}

/// Re-encodes the private key of a PEM source as a PKCS#8 PEM block.
pub(crate) fn key_pem(pem: &Pem) -> Result<String, Error> {
    let key = key_from(pem)?;
    let (label, der) = match &key {
        PrivateKeyDer::Pkcs8(k) => ("PRIVATE KEY", k.secret_pkcs8_der()),
        PrivateKeyDer::Pkcs1(k) => ("RSA PRIVATE KEY", k.secret_pkcs1_der()),
        PrivateKeyDer::Sec1(k) => ("EC PRIVATE KEY", k.secret_sec1_der()),
        _ => return Err(Error::Tls("unsupported private key encoding".into())),
    };
    let mut out = String::new();
    pem_block(&mut out, label, der);
    Ok(out)
}

fn pem_block(out: &mut String, label: &str, der: &[u8]) {
    use std::fmt::Write as _;
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let _ = writeln!(out, "-----BEGIN {label}-----");
    let mut line = 0;
    for chunk in der.chunks(3) {
        let n = chunk.len();
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let idx = [
            b[0] >> 2,
            ((b[0] & 0x03) << 4) | (b[1] >> 4),
            ((b[1] & 0x0f) << 2) | (b[2] >> 6),
            b[2] & 0x3f,
        ];
        for (i, ix) in idx.iter().enumerate() {
            out.push(if i <= n {
                ALPHABET[*ix as usize] as char
            } else {
                '='
            });
            line += 1;
            if line == 64 {
                out.push('\n');
                line = 0;
            }
        }
    }
    if line != 0 {
        out.push('\n');
    }
    let _ = writeln!(out, "-----END {label}-----");
}

/// SHA-256 of the leaf's DER `SubjectPublicKeyInfo`: the peer's fingerprint.
pub(crate) fn spki_fingerprint(cert: &CertificateDer<'_>) -> Result<Fingerprint, Error> {
    let parsed =
        webpki::EndEntityCert::try_from(cert).map_err(|e| tls_err("parsing the certificate", e))?;
    let spki = parsed.subject_public_key_info();
    let digest = ring::digest::digest(&ring::digest::SHA256, spki.as_ref());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    Ok(Fingerprint::from_bytes(out))
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(quinn::rustls::crypto::ring::default_provider())
}

fn tls_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Tls(format!("{what}: {e}"))
}

/// Loads the anchors of a `Trust` into a root store; `None` when it has none.
fn root_store(trust: &Trust) -> Result<Option<Arc<RootCertStore>>, Error> {
    if trust.anchors.is_empty() {
        return Ok(None);
    }
    let mut roots = RootCertStore::empty();
    for source in &trust.anchors {
        for cert in certs_from(source)? {
            roots
                .add(cert)
                .map_err(|e| tls_err("adding a trust anchor", e))?;
        }
    }
    Ok(Some(Arc::new(roots)))
}

/// The fingerprint the peer presented and was refused for, if any.
///
/// Filled in by the verifier and read by the dialling side after a failed
/// handshake, so the failure can name what showed up instead of what was
/// expected. A refusal for a malformed certificate leaves it empty.
pub(crate) type Refused = Arc<Mutex<Option<Fingerprint>>>;

/// The trust decision shared by both verifiers.
struct Policy {
    /// The one identity an address named; overrides everything else.
    expected: Option<Fingerprint>,
    pins: HashSet<Fingerprint>,
    refused: Refused,
}

/// What the policy decided about a leaf without looking at its chain.
enum Verdict {
    Accept,
    /// Not pinned: only a chain to an anchor can still accept it.
    Chain(Fingerprint),
}

impl Policy {
    fn new(expected: Option<Fingerprint>, trust: &Trust) -> Policy {
        Policy {
            expected,
            pins: trust.pins.iter().copied().collect(),
            refused: Arc::new(Mutex::new(None)),
        }
    }

    fn judge(&self, end_entity: &CertificateDer<'_>) -> Result<Verdict, quinn::rustls::Error> {
        let presented = spki_fingerprint(end_entity)
            .map_err(|_| quinn::rustls::Error::InvalidCertificate(CertificateError::BadEncoding))?;
        if let Some(expected) = self.expected {
            return if presented == expected {
                Ok(Verdict::Accept)
            } else {
                Err(self.refuse(presented))
            };
        }
        if self.pins.contains(&presented) {
            return Ok(Verdict::Accept);
        }
        Ok(Verdict::Chain(presented))
    }

    fn refuse(&self, presented: Fingerprint) -> quinn::rustls::Error {
        *self.refused.lock().expect("refusal record poisoned") = Some(presented);
        quinn::rustls::Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
    }
}

impl std::fmt::Debug for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Policy")
            .field("expected", &self.expected)
            .field("pins", &self.pins.len())
            .finish()
    }
}

/// Verifies the server a client dialled.
#[derive(Debug)]
struct PeerVerifier {
    policy: Policy,
    chain: Option<Arc<dyn ServerCertVerifier>>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, quinn::rustls::Error> {
        match self.policy.judge(end_entity)? {
            Verdict::Accept => Ok(ServerCertVerified::assertion()),
            Verdict::Chain(presented) => match &self.chain {
                Some(chain) => chain
                    .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
                    .map_err(|_| self.policy.refuse(presented)),
                None => Err(self.policy.refuse(presented)),
            },
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verifies the client a binding accepted, against the trust of the moment.
///
/// The binding lives longer than its trust: an anchor an authority rotates
/// is a new root store, and a pin an operator adds is a new policy. Both are
/// rebuilt when the source's generation moved and cached otherwise.
#[derive(Debug)]
struct ClientVerifier {
    trust: TrustSource,
    provider: Arc<CryptoProvider>,
    cached: Mutex<(u64, Arc<ClientGeneration>)>,
}

/// One generation of a binding's client trust.
#[derive(Debug)]
struct ClientGeneration {
    policy: Policy,
    chain: Option<Arc<dyn ClientCertVerifier>>,
}

impl ClientVerifier {
    fn new(trust: TrustSource, provider: Arc<CryptoProvider>) -> Result<ClientVerifier, Error> {
        let generation = trust.generation();
        let built = ClientGeneration::build(&trust.current(), &provider)?;
        Ok(ClientVerifier {
            trust,
            provider,
            cached: Mutex::new((generation, Arc::new(built))),
        })
    }

    fn current(&self) -> Arc<ClientGeneration> {
        let generation = self.trust.generation();
        let mut cached = self.cached.lock().expect("trust cache poisoned");
        if cached.0 != generation {
            match ClientGeneration::build(&self.trust.current(), &self.provider) {
                Ok(built) => *cached = (generation, Arc::new(built)),
                // Unreadable anchors: the previous trust stays in force and
                // the source reported it.
                Err(e) => self.trust.report_failure(e.to_string()),
            }
        }
        Arc::clone(&cached.1)
    }
}

impl ClientGeneration {
    fn build(trust: &Trust, provider: &Arc<CryptoProvider>) -> Result<ClientGeneration, Error> {
        let chain = match root_store(trust)? {
            Some(roots) => Some(
                quinn::rustls::server::WebPkiClientVerifier::builder_with_provider(
                    roots,
                    Arc::clone(provider),
                )
                .build()
                .map(|v| v as Arc<dyn ClientCertVerifier>)
                .map_err(|e| tls_err("building the client verifier", e))?,
            ),
            None => None,
        };
        Ok(ClientGeneration {
            policy: Policy::new(None, trust),
            chain,
        })
    }
}

impl ClientCertVerifier for ClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // A pinned peer has no issuer to be hinted about, and webpki's hints
        // would make a pinned-but-unanchored client believe it cannot satisfy
        // the request. Sending none lets every client offer what it has.
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, quinn::rustls::Error> {
        let trust = self.current();
        match trust.policy.judge(end_entity)? {
            Verdict::Accept => Ok(ClientCertVerified::assertion()),
            Verdict::Chain(presented) => match &trust.chain {
                Some(chain) => chain
                    .verify_client_cert(end_entity, intermediates, now)
                    .map_err(|_| trust.policy.refuse(presented)),
                None => Err(trust.policy.refuse(presented)),
            },
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// The most certificates of a peer's chain weida keeps
/// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
/// §4.2). The chain is the peer's choice, so it is bounded before it is kept.
pub(crate) const MAX_PEER_CHAIN_CERTS: usize = 8;

/// The most bytes of a peer's chain weida keeps, all certificates together
/// (decisions/0035 §4.2).
pub(crate) const MAX_PEER_CHAIN_BYTES: usize = 32 * 1024;

/// Whether a presented chain fits the bound weida keeps: the leaf and at
/// most `MAX_PEER_CHAIN_CERTS - 1` more.
fn chain_fits(leaf: &CertificateDer<'_>, rest: &[CertificateDer<'_>]) -> bool {
    rest.len() < MAX_PEER_CHAIN_CERTS
        && leaf.len() + rest.iter().map(|c| c.len()).sum::<usize>() <= MAX_PEER_CHAIN_BYTES
}

/// Accepts any client key the client proves it holds
/// ([`ClientTrust::AnyKey`]): the leaf must parse and the chain fit the
/// bound, and the handshake signature is verified as for any peer. Nothing
/// about the key is judged.
#[derive(Debug)]
struct AnyKeyVerifier {
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for AnyKeyVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, quinn::rustls::Error> {
        if !chain_fits(end_entity, intermediates) {
            return Err(quinn::rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }
        // The fingerprint is the peer: a leaf it cannot be computed from
        // names nobody.
        spki_fingerprint(end_entity)
            .map_err(|_| quinn::rustls::Error::InvalidCertificate(CertificateError::BadEncoding))?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Builds the QUIC transport parameters shared by both sides.
///
/// Every value here bounds memory a remote peer can cause us to hold; see
/// `docs/PROTOCOL.md` §10. The profile decides all of them, including the
/// timers, because the control and bulk tiers are two profiles of the same
/// type (`docs/decisions/0002-control-and-bulk-separation.md` §6.4).
///
/// `dialling` is what separates the two uses: keep-alives are sent by the
/// dialling side only, so a binding passes `false` and its `keep_alive` is
/// not read.
pub(crate) fn transport_config(limits: &Limits, dialling: bool) -> Result<TransportConfig, Error> {
    let mut tc = TransportConfig::default();
    tc.max_concurrent_uni_streams(VarInt::from_u32(limits.max_concurrent_uni_streams));
    // Bidirectional streams carry Req/Rep exchanges: one per live request, so
    // this bounds concurrent exchanges the peer can hold open on us.
    tc.max_concurrent_bidi_streams(VarInt::from_u32(limits.max_concurrent_bidi_streams));
    tc.stream_receive_window(
        VarInt::from_u64(limits.stream_receive_window)
            .map_err(|_| Error::Runtime("stream_receive_window exceeds 2^62-1".into()))?,
    );
    tc.receive_window(
        VarInt::from_u64(limits.connection_receive_window)
            .map_err(|_| Error::Runtime("connection_receive_window exceeds 2^62-1".into()))?,
    );
    let idle = quinn::IdleTimeout::try_from(limits.idle_timeout)
        .map_err(|e| Error::Runtime(format!("idle_timeout out of range: {e}")))?;
    tc.max_idle_timeout(Some(idle));
    tc.keep_alive_interval(dialling.then_some(limits.keep_alive));
    // Datagrams follow the profile: with flows off (the default) the
    // connection advertises no `max_datagram_frame_size` and quinn buffers
    // none of a peer's datagrams (docs/decisions/0034-late-is-lost.md §2.3,
    // §4.5).
    tc.datagram_receive_buffer_size(
        (limits.datagram_receive_bytes > 0).then_some(limits.datagram_receive_bytes),
    );
    tc.datagram_send_buffer_size(limits.datagram_send_bytes);
    let controller: Arc<dyn quinn::congestion::ControllerFactory + Send + Sync> = match limits
        .congestion
    {
        weida_core::Congestion::Cubic => Arc::new(quinn::congestion::CubicConfig::default()),
        weida_core::Congestion::NewReno => Arc::new(quinn::congestion::NewRenoConfig::default()),
        weida_core::Congestion::Bbr => Arc::new(quinn::congestion::BbrConfig::default()),
    };
    tc.congestion_controller_factory(controller);
    Ok(tc)
}

/// Loads an identity's chain and key.
fn load_identity(
    identity: &Identity,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Error> {
    Ok((certs_from(&identity.cert_chain)?, key_from(&identity.key)?))
}

/// Builds the key rustls signs handshakes with from a loaded identity.
fn certified_key(
    provider: &CryptoProvider,
    identity: &Identity,
) -> Result<Arc<CertifiedKey>, Error> {
    let (chain, key) = load_identity(identity)?;
    let key = provider
        .key_provider
        .load_private_key(key)
        .map_err(|e| tls_err("loading the private key", e))?;
    let certified = CertifiedKey::new(chain, key);
    certified
        .keys_match()
        .map_err(|e| tls_err("the certificate does not match its key", e))?;
    Ok(Arc::new(certified))
}

/// Serves the identity a source holds **now** to every handshake.
///
/// The certified key is rebuilt when the source's generation moved and
/// cached otherwise, so a rotation costs one parse and an unchanged source
/// costs an atomic load per handshake. A connection already established
/// keeps the key it was made with; only the next handshake sees the new
/// one, which is what "rotation without re-binding" means (0032 §4.1).
#[derive(Debug)]
struct LiveKey {
    source: IdentitySource,
    provider: Arc<CryptoProvider>,
    cached: Mutex<(u64, Arc<CertifiedKey>)>,
}

impl LiveKey {
    fn new(source: IdentitySource, provider: Arc<CryptoProvider>) -> Result<LiveKey, Error> {
        let generation = source.generation();
        let key = certified_key(&provider, &source.loaded().identity)?;
        Ok(LiveKey {
            source,
            provider,
            cached: Mutex::new((generation, key)),
        })
    }

    fn current(&self) -> Option<Arc<CertifiedKey>> {
        // `loaded()` is also where a file source's due check runs.
        let loaded = self.source.loaded();
        let generation = self.source.generation();
        let mut cached = self.cached.lock().expect("key cache poisoned");
        if cached.0 == generation {
            return Some(Arc::clone(&cached.1));
        }
        match certified_key(&self.provider, &loaded.identity) {
            Ok(key) => {
                *cached = (generation, Arc::clone(&key));
                Some(key)
            }
            // The source accepted material rustls cannot use; the previous
            // key stays in service and the source reported the failure.
            Err(e) => {
                self.source.report_failure(e.to_string());
                Some(Arc::clone(&cached.1))
            }
        }
    }
}

impl ResolvesServerCert for LiveKey {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.current()
    }
}

impl ResolvesClientCert for LiveKey {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        self.current()
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Builds a QUIC server configuration from the binding's identity and, when
/// it requires one, the trust it holds clients to.
pub(crate) fn server_config(
    tls: &ServerTls,
    limits: &Limits,
) -> Result<quinn::ServerConfig, Error> {
    let provider = provider();
    let key = LiveKey::new(tls.identity.clone(), Arc::clone(&provider))?;

    let builder = quinn::rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|e| tls_err("selecting TLS 1.3", e))?;
    let builder = match &tls.client_trust {
        None => builder.with_no_client_auth(),
        Some(ClientTrust::AnyKey) => builder.with_client_cert_verifier(Arc::new(AnyKeyVerifier {
            provider: Arc::clone(&provider),
        })),
        Some(ClientTrust::Trusted(trust)) => {
            if trust.is_empty() {
                return Err(Error::Tls(
                    "client trust is empty: a binding cannot require clients it would never accept"
                        .into(),
                ));
            }
            builder.with_client_cert_verifier(Arc::new(ClientVerifier::new(
                trust.clone(),
                Arc::clone(&provider),
            )?))
        }
    };
    let mut crypto = builder.with_cert_resolver(Arc::new(key));
    crypto.alpn_protocols = vec![tls.alpn.token().to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .map_err(|e| tls_err("building the QUIC server crypto", e))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    config.transport_config(Arc::new(transport_config(limits, false)?));
    Ok(config)
}

/// Builds a QUIC client configuration for one dial.
///
/// `expected` is the fingerprint the address named, if any. The returned
/// [`Refused`] is where the verifier records a peer it turned away, so the
/// caller can report [`Error::Untrusted`] with the identity that showed up.
///
/// Built per dial, so the trust and the identity are whatever their sources
/// hold at that moment.
pub(crate) fn client_config(
    tls: &ClientTls,
    expected: Option<Fingerprint>,
    limits: &Limits,
) -> Result<(quinn::ClientConfig, Refused), Error> {
    let trust = tls.trust.current();
    if expected.is_none() && trust.is_empty() {
        return Err(Error::Tls(
            "nothing to trust: the address names no fingerprint and the trust set is empty".into(),
        ));
    }
    let provider = provider();
    let chain_verifier = match root_store(&trust)? {
        Some(roots) => Some(
            quinn::rustls::client::WebPkiServerVerifier::builder_with_provider(
                roots,
                Arc::clone(&provider),
            )
            .build()
            .map(|v| v as Arc<dyn ServerCertVerifier>)
            .map_err(|e| tls_err("building the server verifier", e))?,
        ),
        None => None,
    };
    let policy = Policy::new(expected, &trust);
    let refused = Arc::clone(&policy.refused);
    let verifier = Arc::new(PeerVerifier {
        policy,
        chain: chain_verifier,
        provider: Arc::clone(&provider),
    });

    let builder = quinn::rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|e| tls_err("selecting TLS 1.3", e))?
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let mut crypto = match &tls.identity {
        None => builder.with_no_client_auth(),
        Some(identity) => {
            let key = LiveKey::new(identity.clone(), provider)?;
            builder.with_client_cert_resolver(Arc::new(key))
        }
    };
    crypto.alpn_protocols = vec![tls.alpn.token().to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| tls_err("building the QUIC client crypto", e))?;
    let mut config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    config.transport_config(Arc::new(transport_config(limits, true)?));
    Ok((config, refused))
}

/// The chain a connected peer presented, if it presented one.
fn presented(conn: &quinn::Connection) -> Option<Vec<CertificateDer<'static>>> {
    conn.peer_identity()?
        .downcast::<Vec<CertificateDer<'static>>>()
        .ok()
        .map(|chain| *chain)
}

/// The fingerprint of the identity a connected peer proved, if it presented
/// one. Computed once per connection, after the handshake.
pub(crate) fn peer_fingerprint(conn: &quinn::Connection) -> Option<Fingerprint> {
    let chain = presented(conn)?;
    let leaf = chain.first()?;
    // The verifier already parsed this certificate; a failure here would mean
    // it accepted something it could not parse, which it does not.
    spki_fingerprint(leaf).ok()
}

/// The chain a connected peer presented, when it fits the bound weida keeps.
/// An `AnyKey` binding refused a larger one in the handshake; under a trust,
/// and on the dialling side, it is not kept and the peer is still known by
/// its fingerprint.
pub(crate) fn peer_chain(conn: &quinn::Connection) -> Option<PeerChain> {
    let chain = presented(conn)?;
    let (leaf, rest) = chain.split_first()?;
    if !chain_fits(leaf, rest) {
        tracing::debug!(
            certs = chain.len(),
            "peer chain exceeds the bound weida keeps; not kept"
        );
        return None;
    }
    Some(PeerChain::new(chain))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn transport_config_accepts_the_defaults() {
        assert!(
            transport_config(&Limits::default(), true).is_ok()
                && transport_config(&Limits::default(), false).is_ok()
        );
    }

    #[test]
    fn an_absurd_idle_timeout_is_rejected_not_clamped() {
        let limits = Limits {
            idle_timeout: Duration::from_secs(u64::MAX),
            ..Limits::default()
        };
        let err = transport_config(&limits, false).unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    #[test]
    fn an_oversized_receive_window_is_rejected() {
        let limits = Limits {
            stream_receive_window: u64::MAX,
            ..Limits::default()
        };
        assert!(matches!(
            transport_config(&limits, false).unwrap_err(),
            Error::Runtime(_)
        ));
    }

    #[test]
    fn missing_tls_files_are_reported_as_tls_errors() {
        let err = server_config(
            &ServerTls::new(Identity::from_pem_files(
                "/nonexistent/cert.pem",
                "/nonexistent/key.pem",
            )),
            &Limits::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        let err = client_config(
            &ClientTls::new(Trust::anchor_file("/nonexistent/ca.pem")),
            None,
            &Limits::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[test]
    fn empty_trust_is_rejected_unless_the_address_names_a_peer() {
        let err = client_config(
            &ClientTls::new(Trust::by_address()),
            None,
            &Limits::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        assert!(
            client_config(
                &ClientTls::new(Trust::by_address()),
                Some(Fingerprint::from_bytes([0; 32])),
                &Limits::default(),
            )
            .is_ok()
        );
    }

    #[cfg(feature = "generate")]
    #[test]
    fn a_binding_cannot_require_clients_it_would_never_accept() {
        let identity = Identity::generate().expect("identity");
        let err = server_config(
            &ServerTls::new(identity).require_client(Trust::by_address()),
            &Limits::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[cfg(feature = "generate")]
    #[test]
    fn a_generated_identity_roundtrips_through_pem_with_the_same_fingerprint() {
        let identity = Identity::generate().expect("identity");
        let fp = identity.fingerprint().expect("fingerprint");
        let pem = identity.to_pem().expect("pem");
        assert!(pem.contains("BEGIN CERTIFICATE"));
        assert!(pem.contains("BEGIN PRIVATE KEY"));
        let reloaded = Identity::from_pem(pem.clone(), pem);
        assert_eq!(reloaded.fingerprint().expect("fingerprint"), fp);
        assert!(
            !identity
                .certificate_pem()
                .expect("cert")
                .contains("PRIVATE"),
        );
    }

    #[cfg(feature = "generate")]
    #[test]
    fn the_fingerprint_is_over_the_key_not_the_certificate() {
        // Two certificates for one key must fingerprint identically: that is
        // what makes a pin survive a certificate renewal. A different key must
        // not.
        let key = rcgen::KeyPair::generate().expect("key");
        let a = rcgen::CertificateParams::new(vec!["a.example".to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        let b = rcgen::CertificateParams::new(vec!["b.example".to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        assert_ne!(a.der(), b.der());
        assert_eq!(
            spki_fingerprint(a.der()).unwrap(),
            spki_fingerprint(b.der()).unwrap()
        );
        let other = Identity::generate().unwrap().fingerprint().unwrap();
        assert_ne!(spki_fingerprint(a.der()).unwrap(), other);
    }

    #[test]
    fn any_key_bounds_the_chain_it_keeps() {
        let key = rcgen::KeyPair::generate().expect("key");
        let leaf = CertificateDer::from(
            rcgen::CertificateParams::new(vec!["d".to_owned()])
                .expect("params")
                .self_signed(&key)
                .expect("cert")
                .der()
                .to_vec(),
        );
        let verifier = AnyKeyVerifier {
            provider: provider(),
        };
        let verify = |leaf: &CertificateDer<'_>, rest: &[CertificateDer<'_>]| {
            verifier.verify_client_cert(leaf, rest, UnixTime::now())
        };

        assert!(verify(&leaf, &vec![leaf.clone(); MAX_PEER_CHAIN_CERTS - 1]).is_ok());
        assert!(matches!(
            verify(&leaf, &vec![leaf.clone(); MAX_PEER_CHAIN_CERTS]),
            Err(quinn::rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure
            ))
        ));
        assert!(matches!(
            verify(
                &leaf,
                &[CertificateDer::from(vec![0u8; MAX_PEER_CHAIN_BYTES])]
            ),
            Err(quinn::rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure
            ))
        ));
        assert!(matches!(
            verify(&CertificateDer::from(vec![0x30, 0x00]), &[]),
            Err(quinn::rustls::Error::InvalidCertificate(
                CertificateError::BadEncoding
            ))
        ));
    }

    #[test]
    fn pem_encoding_matches_the_reference_alphabet() {
        let mut out = String::new();
        pem_block(&mut out, "TEST", b"hello world");
        assert_eq!(
            out,
            "-----BEGIN TEST-----\naGVsbG8gd29ybGQ=\n-----END TEST-----\n"
        );
        let mut out = String::new();
        pem_block(&mut out, "TEST", b"ab");
        assert_eq!(out, "-----BEGIN TEST-----\nYWI=\n-----END TEST-----\n");
        let mut out = String::new();
        pem_block(&mut out, "TEST", &[0u8; 48]);
        let body: Vec<&str> = out.lines().collect();
        assert_eq!(body[1].len(), 64, "lines wrap at 64 columns");
    }
}
