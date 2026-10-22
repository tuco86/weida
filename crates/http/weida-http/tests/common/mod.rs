//! What every test of this crate needs: a deadline and a certificate.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

/// Bounds every await of a test: ten seconds, then a panic naming the step.
pub async fn within<F: Future>(what: &str, f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within 10 s"))
}

/// A self-signed certificate for `localhost` and `127.0.0.1`, with a client
/// configuration that trusts it and a server configuration that presents it
/// (no ALPN set on either).
pub fn tls_pair() -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let issued =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .unwrap();
    let cert = CertificateDer::from(issued.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(issued.signing_key.serialize_der()));
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    (Arc::new(client), Arc::new(server))
}

/// `n` octets of a repeating, position-dependent pattern, so a reordered or
/// truncated echo cannot compare equal.
pub fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}
