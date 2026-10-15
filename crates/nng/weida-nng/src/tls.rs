//! The `tls+tcp` transport: TLS over TCP, and what it does and does not
//! prove.
//!
//! "TLS transport provides TLS 1.2 over TCP with configurable
//! authentication mode, CA file, certificate/key file, verification
//! result, peer common name, and peer alternative names"
//! (`docs/research/nanomsg-nng.md` §10). All six are here: the first three
//! as [`TlsConfig`], the last three as [`TlsPeer`], which reaches the
//! pipe-add-pre callback on [`PipeInfo::tls`](crate::PipeInfo::tls) where
//! an allow-list runs.
//!
//! # What this authenticates, and where it stops
//!
//! **It authenticates the transport peer of one SP connection, and it
//! terminates there.** A verified certificate says that whoever holds the
//! other end of *this* TCP connection holds the private key for that
//! certificate. It says nothing about any message: "the SP pattern
//! protocols define no message-level authentication, authorization, or
//! authenticated sender identity" (§10), so a message that arrives on an
//! authenticated pipe carries no claim about who composed it, and a
//! message forwarded by a device arrives on a pipe authenticated as *the
//! device*.
//!
//! For that reason **no API here turns a [`TlsPeer`] into a sender
//! identity.** There is no conversion to any identity type, no accessor on
//! a [`Message`](crate::Message) that returns one, and none may be added:
//! SP has no such concept to convert into, and offering one would be this
//! library inventing a guarantee the protocol does not make
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4
//! item 6). What an application may do is exactly what NNG's own hook
//! offers: refuse the pipe.
//!
//! # The dependency
//!
//! `tokio-rustls`, and through it `rustls` — a pure-Rust TLS
//! implementation, so `unsafe_code = "forbid"` survives it. Certificate
//! parsing is `rustls-pki-types`' PEM reader and path validation is
//! `rustls-webpki`, both of which this workspace already depends on for
//! weida's own transport. No OpenSSL and no C.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
    ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use crate::error::{Error, Result};

/// `NNG_OPT_TLS_AUTH_MODE`: how hard this side insists on the peer's
/// certificate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum AuthMode {
    /// `NNG_TLS_AUTH_MODE_NONE`: the peer's certificate is not validated at
    /// all. The connection is still encrypted; nothing about the peer is
    /// established, and [`TlsPeer::verified`] says so.
    None,
    /// `NNG_TLS_AUTH_MODE_OPTIONAL`: a certificate is validated if the peer
    /// offers one, and its absence is not an error. Meaningful on a
    /// listener, where it is how an allow-list can admit both kinds and
    /// decide for itself.
    Optional,
    /// `NNG_TLS_AUTH_MODE_REQUIRED`: the peer must present a certificate
    /// that validates against the configured CA, and the handshake fails
    /// otherwise. The default here, because a transport whose default
    /// proves nothing is a transport whose users think it proves something.
    #[default]
    Required,
}

/// `tls+tcp`'s configuration: the authentication mode, the CA, and this
/// side's certificate and key.
///
/// PEM, because that is what NNG's `NNG_OPT_TLS_CA_FILE` and
/// `NNG_OPT_TLS_CERT_KEY_FILE` take. Bytes rather than paths, because a
/// library that read files would have to decide what a relative path means
/// and when to re-read it; a caller that has a path calls
/// [`std::fs::read`].
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// How hard to insist on the peer's certificate.
    pub auth_mode: AuthMode,
    /// `NNG_OPT_TLS_CA_FILE`: the anchors a peer's certificate is
    /// validated against. Required for [`AuthMode::Required`] and
    /// [`AuthMode::Optional`].
    pub ca_pem: Option<Vec<u8>>,
    /// `NNG_OPT_TLS_CERT_KEY_FILE`, the certificate half: this side's
    /// chain, leaf first. Required on a listener.
    pub cert_pem: Option<Vec<u8>>,
    /// `NNG_OPT_TLS_CERT_KEY_FILE`, the key half.
    ///
    /// **Write-only**, like NNG's: it is configuration going in and there
    /// is no accessor that hands it back, which is what
    /// [`Error::EWRITEONLY`] exists to say if one is ever asked for.
    pub key_pem: Option<Vec<u8>>,
    /// `NNG_OPT_TLS_SERVER_NAME`: the name a dialled peer's certificate is
    /// checked against, overriding the one in the URL.
    ///
    /// NNG "can validate the server name from the dial URL" (§1), so this
    /// is only needed when the URL names an address rather than a host —
    /// in which case there is nothing in it to validate and this is the
    /// only way to say what to expect.
    pub server_name: Option<String>,
}

/// Prints a configuration without the secret half of it.
///
/// The key is configuration going in and nothing a log line or a failed
/// `assert_eq!` should ever carry: it is "write-only, like NNG's", and a
/// derived `Debug` would print the whole PEM as a list of numbers. The
/// certificate chain and the CA are public material, so their size is
/// shown and their content is not — enough to see that a configuration is
/// the one that was meant.
impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn size(pem: &Option<Vec<u8>>) -> String {
            match pem {
                Some(pem) => format!("Some(<{} octets>)", pem.len()),
                None => "None".to_owned(),
            }
        }
        f.debug_struct("TlsConfig")
            .field("auth_mode", &self.auth_mode)
            .field("ca_pem", &format_args!("{}", size(&self.ca_pem)))
            .field("cert_pem", &format_args!("{}", size(&self.cert_pem)))
            .field(
                "key_pem",
                &format_args!(
                    "{}",
                    if self.key_pem.is_some() {
                        "Some(<redacted>)"
                    } else {
                        "None"
                    }
                ),
            )
            .field("server_name", &self.server_name)
            .finish()
    }
}

impl TlsConfig {
    /// Refuses a configuration that cannot do what it says, **where it is
    /// configured**.
    ///
    /// An authentication mode with no CA to validate against would
    /// otherwise fail at the first connection, or worse, quietly validate
    /// against nothing.
    pub fn validate(&self, listening: bool) -> Result<()> {
        if self.auth_mode != AuthMode::None && self.ca_pem.is_none() {
            return Err(Error::EINVAL(
                format!(
                    "NNG_OPT_TLS_AUTH_MODE is {:?} and no CA is configured; there would be \
                     nothing to validate the peer against",
                    self.auth_mode
                )
                .into(),
            ));
        }
        if self.cert_pem.is_some() != self.key_pem.is_some() {
            return Err(Error::EINVAL(
                "a TLS certificate and its key are configured together; one without the other \
                 cannot be used"
                    .into(),
            ));
        }
        if listening && self.cert_pem.is_none() {
            return Err(Error::EINVAL(
                "a tls+tcp listener needs a certificate and key: there is no anonymous server \
                 side"
                    .into(),
            ));
        }
        Ok(())
    }

    fn anchors(&self) -> Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        let Some(pem) = self.ca_pem.as_deref() else {
            return Ok(roots);
        };
        for certificate in CertificateDer::pem_slice_iter(pem) {
            let certificate = certificate
                .map_err(|why| Error::EINVAL(format!("the CA PEM is unreadable: {why}").into()))?;
            roots
                .add(certificate)
                .map_err(|why| Error::EINVAL(format!("the CA is unusable: {why}").into()))?;
        }
        if roots.is_empty() {
            return Err(Error::EINVAL("the CA PEM contains no certificate".into()));
        }
        Ok(roots)
    }

    fn identity(&self) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
        let (Some(cert), Some(key)) = (self.cert_pem.as_deref(), self.key_pem.as_deref()) else {
            return Ok(None);
        };
        let chain = CertificateDer::pem_slice_iter(cert)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|why| {
                Error::EINVAL(format!("the certificate PEM is unreadable: {why}").into())
            })?;
        if chain.is_empty() {
            return Err(Error::EINVAL(
                "the certificate PEM contains no certificate".into(),
            ));
        }
        let key = PrivateKeyDer::from_pem_slice(key)
            .map_err(|why| Error::EINVAL(format!("the key PEM is unreadable: {why}").into()))?;
        Ok(Some((chain, key)))
    }

    /// The client side of this configuration.
    fn client(&self) -> Result<Arc<ClientConfig>> {
        let builder = ClientConfig::builder();
        let builder = match self.auth_mode {
            AuthMode::None => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AnyServer)),
            _ => builder.with_root_certificates(self.anchors()?),
        };
        let config = match self.identity()? {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key).map_err(|why| {
                Error::EINVAL(format!("the client identity is unusable: {why}").into())
            })?,
            None => builder.with_no_client_auth(),
        };
        Ok(Arc::new(config))
    }

    /// The listening side of this configuration.
    fn server(&self) -> Result<Arc<ServerConfig>> {
        let Some((chain, key)) = self.identity()? else {
            return Err(Error::EINVAL(
                "a tls+tcp listener needs a certificate and key".into(),
            ));
        };
        let builder = match self.auth_mode {
            AuthMode::None => ServerConfig::builder().with_no_client_auth(),
            AuthMode::Optional => {
                let verifier = WebPkiClientVerifier::builder(Arc::new(self.anchors()?))
                    .allow_unauthenticated()
                    .build()
                    .map_err(|why| {
                        Error::EINVAL(format!("the client verifier is unusable: {why}").into())
                    })?;
                ServerConfig::builder().with_client_cert_verifier(verifier)
            }
            AuthMode::Required => {
                let verifier = WebPkiClientVerifier::builder(Arc::new(self.anchors()?))
                    .build()
                    .map_err(|why| {
                        Error::EINVAL(format!("the client verifier is unusable: {why}").into())
                    })?;
                ServerConfig::builder().with_client_cert_verifier(verifier)
            }
        };
        let config = builder.with_single_cert(chain, key).map_err(|why| {
            Error::EINVAL(format!("the server identity is unusable: {why}").into())
        })?;
        Ok(Arc::new(config))
    }
}

/// What TLS established about the peer of **one connection**.
///
/// Read by a pipe-add-pre callback, which may refuse the pipe on it. Not
/// convertible to any identity type, and deliberately: see the module
/// note.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsPeer {
    /// `NNG_OPT_TLS_VERIFIED`: whether the peer presented a certificate
    /// that validated against the configured CA.
    ///
    /// `false` under [`AuthMode::None`] always, and under
    /// [`AuthMode::Optional`] for a peer that presented nothing. An
    /// allow-list that ignores this field is an allow-list that admits
    /// anybody.
    pub verified: bool,
    /// `NNG_OPT_TLS_PEER_CN`: the common name of the peer's certificate,
    /// for a log line.
    ///
    /// **For display, not for matching.** A name in the common name has
    /// not been validated as a name by anything — modern path validation
    /// checks the subject alternative names — so an allow-list belongs on
    /// [`TlsPeer::subject_alt_names`] or on the certificate's issuer, and
    /// this is what to print beside a refusal.
    pub common_name: Option<String>,
    /// `NNG_OPT_TLS_PEER_ALT_NAMES`: the DNS names in the peer
    /// certificate's subject alternative name extension, which is what
    /// path validation actually checks a name against.
    pub subject_alt_names: Vec<String>,
}

impl TlsPeer {
    /// Reads the facts out of a peer's certificate chain.
    fn of(chain: Option<&[CertificateDer<'static>]>, verified: bool) -> TlsPeer {
        let Some(leaf) = chain.and_then(<[CertificateDer<'static>]>::first) else {
            return TlsPeer {
                verified: false,
                ..TlsPeer::default()
            };
        };
        let Ok(cert) = webpki::EndEntityCert::try_from(leaf) else {
            return TlsPeer {
                verified,
                ..TlsPeer::default()
            };
        };
        TlsPeer {
            verified,
            common_name: common_name(cert.subject()),
            subject_alt_names: cert.valid_dns_names().map(str::to_owned).collect(),
        }
    }
}

/// Pulls the common name out of a DER-encoded `RDNSequence`.
///
/// A narrow scan rather than an X.509 parser: it walks the sequence
/// looking for the attribute whose OID is `2.5.4.3` and returns its value
/// if that value is a printable or UTF-8 string. Anything it does not
/// understand yields `None`, which is the right answer for a field that is
/// for display only ([`TlsPeer::common_name`]).
fn common_name(subject_der: &[u8]) -> Option<String> {
    /// `2.5.4.3`, `id-at-commonName`.
    const CN_OID: [u8; 3] = [0x55, 0x04, 0x03];
    let mut rest = subject_der;
    while let Some(at) = rest.windows(CN_OID.len()).position(|w| w == CN_OID) {
        // The OID is preceded by its own tag and length; the value follows
        // it as another tag-length-value.
        let after = &rest[at + CN_OID.len()..];
        if after.len() >= 2 {
            let tag = after[0];
            let len = after[1] as usize;
            // 0x0c UTF8String, 0x13 PrintableString, 0x16 IA5String: the
            // three a common name is allowed to be, in the short-form
            // length that a name shorter than 128 octets always has.
            if matches!(tag, 0x0c | 0x13 | 0x16)
                && after.len() >= 2 + len
                && let Ok(text) = std::str::from_utf8(&after[2..2 + len])
            {
                return Some(text.to_owned());
            }
        }
        rest = after;
    }
    None
}

/// The server side of a configuration, built once.
///
/// Parsing the PEM, loading the key and building the client verifier is
/// work for the moment a listener binds, not for every connection that
/// reaches it: a stranger chooses how many of those there are, and each
/// would otherwise pay for it before it has proved anything. Cloning shares
/// the built configuration.
#[derive(Clone)]
pub struct Acceptor {
    acceptor: TlsAcceptor,
    auth_mode: AuthMode,
}

impl Acceptor {
    /// Validates `config` for a listener and builds its server side.
    pub fn new(config: &TlsConfig) -> Result<Acceptor> {
        config.validate(true)?;
        Ok(Acceptor {
            acceptor: TlsAcceptor::from(config.server()?),
            auth_mode: config.auth_mode,
        })
    }
}

/// The client side of a configuration, built once for as many connections
/// as a dialler makes.
#[derive(Clone)]
pub struct Connector {
    connector: TlsConnector,
    auth_mode: AuthMode,
}

impl Connector {
    /// Validates `config` for a dialler and builds its client side.
    pub fn new(config: &TlsConfig) -> Result<Connector> {
        config.validate(false)?;
        Ok(Connector {
            connector: TlsConnector::from(config.client()?),
            auth_mode: config.auth_mode,
        })
    }
}

/// A TLS handshake that is still to run on an established TCP connection.
///
/// Separate from the connection because the engine runs it **after** it has
/// reserved the pipe slot and **under** the handshake deadline: a peer that
/// connects and says nothing must hold nothing but that slot, and for no
/// longer than the deadline, and a refused connection must cost the
/// listener no cryptography.
pub enum Handshake {
    /// This side listens: it answers the peer's `ClientHello`.
    Accept(Acceptor),
    /// This side dialled: it checks the peer against `name`.
    Connect {
        /// The client side to run.
        connector: Connector,
        /// The name the peer's certificate is checked against.
        name: ServerName<'static>,
    },
}

impl Handshake {
    /// The handshake of a listener's connection.
    pub fn accept(acceptor: &Acceptor) -> Handshake {
        Handshake::Accept(acceptor.clone())
    }

    /// The handshake of a dialled connection, checking the peer against
    /// `server_name`. `NNG_EADDRINVAL` when that is not a name a
    /// certificate can carry, which is judged before anything is dialled.
    pub fn connect(connector: &Connector, server_name: &str) -> Result<Handshake> {
        let name = ServerName::try_from(server_name.to_owned()).map_err(|_| {
            Error::EADDRINVAL(
                format!("{server_name:?} is not a name a certificate can carry").into(),
            )
        })?;
        Ok(Handshake::Connect {
            connector: connector.clone(),
            name,
        })
    }

    /// Runs the handshake on `stream`.
    ///
    /// Unbounded by itself: the caller races it against the handshake
    /// deadline, which is what ends a peer that never answers.
    pub async fn run(self, stream: TcpStream) -> Result<(TlsStream<TcpStream>, TlsPeer)> {
        match self {
            Handshake::Accept(side) => {
                let tls = side
                    .acceptor
                    .accept(stream)
                    .await
                    .map_err(peer_auth_error)?;
                let certificates = tls.get_ref().1.peer_certificates().map(<[_]>::to_vec);
                // Under `Optional` a client that presented nothing is
                // unverified; under `Required` it could not have got this
                // far without validating.
                let verified = side.auth_mode != AuthMode::None && certificates.is_some();
                let peer = TlsPeer::of(certificates.as_deref(), verified);
                Ok((TlsStream::Server(tls), peer))
            }
            Handshake::Connect { connector, name } => {
                let tls = connector
                    .connector
                    .connect(name, stream)
                    .await
                    .map_err(peer_auth_error)?;
                let verified = connector.auth_mode != AuthMode::None;
                let peer = TlsPeer::of(tls.get_ref().1.peer_certificates(), verified);
                Ok((TlsStream::Client(tls), peer))
            }
        }
    }
}

/// A failed TLS handshake is `NNG_EPEERAUTH` where it is about the peer's
/// certificate and `NNG_EPROTO` otherwise, which is the split NNG's own
/// `nng_dial` reports (§8).
fn peer_auth_error(error: std::io::Error) -> Error {
    let text = error.to_string();
    if text.contains("certificate")
        || text.contains("CertificateUnknown")
        || text.contains("UnknownIssuer")
        || text.contains("BadCertificate")
        || text.contains("CertRevoked")
        || text.contains("NotValidForName")
    {
        return Error::EPEERAUTH(text.into());
    }
    Error::EPROTO(text.into())
}

/// The verifier [`AuthMode::None`] uses: it establishes nothing, which is
/// what that mode means.
///
/// It exists because rustls has no "do not check" switch and demands that
/// a caller asking for one say so in code. That is the right demand, and
/// this type is the whole of this library's answer to it: the connection is
/// encrypted, [`TlsPeer::verified`] is `false`, and an allow-list reading
/// that field refuses the pipe.
#[derive(Debug)]
struct AnyServer;

impl ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: a configuration that cannot do what it says is refused where
    /// it is set, not at the first connection.
    #[test]
    fn an_impossible_tls_configuration_is_refused_at_configuration_time() {
        let no_ca = TlsConfig {
            auth_mode: AuthMode::Required,
            ..TlsConfig::default()
        };
        let err = no_ca.validate(false).unwrap_err();
        assert!(matches!(err, Error::EINVAL(_)), "{err:?}");
        assert!(err.cause().contains("NNG_OPT_TLS_AUTH_MODE"));

        let half = TlsConfig {
            auth_mode: AuthMode::None,
            cert_pem: Some(b"-----BEGIN CERTIFICATE-----".to_vec()),
            ..TlsConfig::default()
        };
        assert!(matches!(half.validate(false), Err(Error::EINVAL(_))));

        let anonymous_server = TlsConfig {
            auth_mode: AuthMode::None,
            ..TlsConfig::default()
        };
        assert!(
            anonymous_server.validate(false).is_ok(),
            "a client may be anonymous"
        );
        assert!(
            matches!(anonymous_server.validate(true), Err(Error::EINVAL(_))),
            "a listener may not"
        );
    }

    /// Claim: the default authentication mode is the one that proves
    /// something. A transport whose default proves nothing is a transport
    /// whose users believe it proves something.
    #[test]
    fn the_default_authentication_mode_is_required() {
        assert_eq!(TlsConfig::default().auth_mode, AuthMode::Required);
        assert_eq!(AuthMode::default(), AuthMode::Required);
    }

    /// Claim: a peer with no certificate is never `verified`, whatever the
    /// mode said — the field is about what happened, not about what was
    /// asked for.
    #[test]
    fn a_peer_with_no_certificate_is_not_verified() {
        let peer = TlsPeer::of(None, true);
        assert!(!peer.verified);
        assert_eq!(peer.common_name, None);
        assert!(peer.subject_alt_names.is_empty());
    }

    /// Claim: the common-name scan finds a printable common name and
    /// answers `None` for a subject it does not understand, which is the
    /// right answer for a display-only field.
    #[test]
    fn the_common_name_scan_is_narrow() {
        // SET { SEQUENCE { OID 2.5.4.3, PrintableString "peer.test" } }
        let subject = [
            0x31, 0x14, 0x30, 0x12, 0x06, 0x03, 0x55, 0x04, 0x03, 0x13, 0x09, b'p', b'e', b'e',
            b'r', b'.', b't', b'e', b's', b't',
        ];
        assert_eq!(common_name(&subject), Some("peer.test".to_owned()));
        assert_eq!(common_name(&[]), None);
        assert_eq!(common_name(&[0x30, 0x03, 0x55, 0x04, 0x03]), None);
    }

    /// Claim: no printout of a configuration, or of the options that carry
    /// one, contains a byte of the private key. The key is write-only, and a
    /// derived `Debug` printed it whole — as a list of numbers, which is why
    /// the check is for the key's text and for its decimal rendering both.
    #[test]
    fn a_configuration_never_prints_its_private_key() {
        let secret = b"SECRET-KEY-MATERIAL".to_vec();
        let config = TlsConfig {
            key_pem: Some(secret.clone()),
            cert_pem: Some(b"-----BEGIN CERTIFICATE-----".to_vec()),
            ..TlsConfig::default()
        };
        let options = crate::SocketOptions {
            tls: Some(config.clone()),
            ..crate::SocketOptions::default()
        };
        let decimal = format!("{secret:?}");
        for shown in [format!("{config:?}"), format!("{options:?}")] {
            assert!(!shown.contains("SECRET"), "{shown}");
            assert!(!shown.contains(&decimal[1..decimal.len() - 1]), "{shown}");
            assert!(shown.contains("redacted"), "{shown}");
        }
        let keyless = format!("{:?}", TlsConfig::default());
        assert!(keyless.contains("key_pem: None"), "{keyless}");
    }
}
