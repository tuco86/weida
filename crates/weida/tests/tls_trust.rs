//! Client trust and served identity, as the handshake sees them.
//!
//! A binding judges a client against the trust of the moment, and serves the
//! identity of the moment. Two claims are defended here:
//!
//! - a trust withdrawn under a live binding applies to every later handshake,
//!   including one that would otherwise resume an earlier session, so a
//!   revoked key cannot come back through a ticket;
//! - a source whose new material cannot be used is reported once per change
//!   of the source, not once per handshake, and the previous material stays in
//!   service.
//!
//! The clients here are raw `quinn` endpoints so the test controls resumption
//! and the exact handshake sequence.

use crate::common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::Certs;
use quinn::rustls::client::Resumption;
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use weida::{
    ALPN, Fingerprint, Identity, IdentityEvent, IdentityEvents, IdentitySource, Runtime,
    RuntimeConfig, ServerTls, Trust, TrustSource,
};

const DEADLINE: Duration = Duration::from_secs(10);

/// A client key, its certificate and its fingerprint as a binding sees it.
struct ClientKey {
    fingerprint: Fingerprint,
    chain: CertificateDer<'static>,
    key_der: Vec<u8>,
}

impl ClientKey {
    fn generate() -> ClientKey {
        let key = rcgen::KeyPair::generate().expect("client key");
        let cert = rcgen::CertificateParams::new(vec!["client.weida.test".to_owned()])
            .expect("client params")
            .self_signed(&key)
            .expect("client certificate");
        let fingerprint = Identity::from_pem(cert.pem(), key.serialize_pem())
            .fingerprint()
            .expect("client fingerprint");
        ClientKey {
            fingerprint,
            chain: cert.der().clone(),
            key_der: key.serialize_der(),
        }
    }
}

/// A client configuration that trusts `server` and offers the weida ALPN.
/// With `client`, it presents that key. With `resume`, its session store
/// keeps the tickets the binding issues, so a reconnect presents one.
fn client_config(server: &Certs, client: Option<&ClientKey>, resume: bool) -> quinn::ClientConfig {
    let mut roots = quinn::rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&server.cert_pem).expect("read server certificate") {
        roots
            .add(cert.expect("parse server certificate"))
            .expect("add root");
    }
    let builder = quinn::rustls::ClientConfig::builder_with_provider(Arc::new(
        quinn::rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&quinn::rustls::version::TLS13])
    .expect("tls 1.3")
    .with_root_certificates(Arc::new(roots));
    let mut crypto = match client {
        None => builder.with_no_client_auth(),
        Some(client) => builder
            .with_client_auth_cert(
                vec![client.chain.clone()],
                PrivateKeyDer::from(PrivatePkcs8KeyDer::from(client.key_der.clone())),
            )
            .expect("client auth"),
    };
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    crypto.resumption = if resume {
        Resumption::in_memory_sessions(8)
    } else {
        Resumption::disabled()
    };
    quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).expect("quic client crypto"),
    ))
}

fn client_endpoint() -> quinn::Endpoint {
    quinn::Endpoint::client("127.0.0.1:0".parse().expect("loopback")).expect("client endpoint")
}

/// A path that does not exist: a trust anchor the rebuild cannot read.
fn missing_anchor(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "weida-tls-trust-missing-{}-{name}.pem",
        std::process::id()
    ))
}

/// Counts the `RenewalFailed` events that arrive within a short quiet window.
async fn renewal_failures(events: &mut IdentityEvents) -> usize {
    let mut failures = 0;
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(300), events.recv()).await
    {
        if matches!(event, IdentityEvent::RenewalFailed(_)) {
            failures += 1;
        }
    }
    failures
}

/// Opens `count` connections that stay open, and returns them. Each must be
/// accepted by the handshake, which is what the caller relies on.
async fn connect_held(
    endpoint: &quinn::Endpoint,
    config: &quinn::ClientConfig,
    addr: SocketAddr,
    count: usize,
) -> Vec<quinn::Connection> {
    let mut held = Vec::with_capacity(count);
    for _ in 0..count {
        let connecting = endpoint
            .connect_with(config.clone(), addr, "localhost")
            .expect("start a connection");
        let conn = tokio::time::timeout(DEADLINE, connecting)
            .await
            .expect("handshake within the deadline")
            .expect("handshake accepted");
        held.push(conn);
    }
    held
}

/// Claim: a client trust withdrawn under a live binding applies to a
/// reconnect that would resume an earlier session. The binding issues no
/// tickets, so the reconnect is a full handshake that the current trust
/// judges ([0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
/// §4.1).
#[tokio::test]
async fn a_withdrawn_client_trust_refuses_a_resumed_session() {
    let server = Certs::generate();
    let client = ClientKey::generate();
    let trust = TrustSource::external(Trust::pin(client.fingerprint));

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(server.identity()).require_client(trust.clone()),
        )
        .await
        .expect("bind");
    let addr = binding.local_addr();

    let endpoint = client_endpoint();
    let config = client_config(&server, Some(&client), true);

    // Control: while the pin is in force the first connection is accepted and
    // stays open.
    let first = tokio::time::timeout(
        DEADLINE,
        endpoint
            .connect_with(config.clone(), addr, "localhost")
            .expect("start a connection"),
    )
    .await
    .expect("handshake within the deadline")
    .expect("the pinned client is accepted");
    // Keep the connection open long enough for the server's session ticket to
    // reach the client's store.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        first.close_reason().is_none(),
        "the pinned client is served"
    );

    trust.update(Trust::default());

    // The same client configuration, and so the same session store, dials
    // again. The handshake must not be accepted on the strength of the old
    // session: the binding closes the connection, or refuses the handshake.
    let second = endpoint
        .connect_with(config, addr, "localhost")
        .expect("start a connection");
    let accepted = match tokio::time::timeout(DEADLINE, second)
        .await
        .expect("handshake within the deadline")
    {
        Err(_) => false,
        // The client finishes its side first; a refusal by the binding then
        // arrives as a close, so an accepted connection stays open.
        Ok(conn) => tokio::time::timeout(Duration::from_secs(2), conn.closed())
            .await
            .is_err(),
    };
    assert!(
        !accepted,
        "a reconnect after the trust was withdrawn must be refused"
    );

    drop(first);
    drop(binding);
    runtime.shutdown().await;
}

/// Claim: a client trust that cannot be rebuilt is reported once per change of
/// the source, not once per handshake, and the trust in service keeps
/// deciding while the rebuild fails.
#[tokio::test]
async fn a_trust_that_cannot_be_rebuilt_is_reported_once_per_generation() {
    let server = Certs::generate();
    let client = ClientKey::generate();
    let trust = TrustSource::external(Trust::pin(client.fingerprint));

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(server.identity()).require_client(trust.clone()),
        )
        .await
        .expect("bind");
    let addr = binding.local_addr();
    let endpoint = client_endpoint();
    let config = client_config(&server, Some(&client), false);
    let mut events = trust.events();

    // The anchor cannot be read, so every handshake attempts a rebuild. The
    // pin of the previous generation stays in service and keeps accepting the
    // pinned client.
    trust.update(Trust::anchor_file(missing_anchor("first")));
    let held = connect_held(&endpoint, &config, addr, 20).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        held.iter().all(|conn| conn.close_reason().is_none()),
        "the previous trust stays in service while the rebuild fails"
    );
    assert_eq!(
        renewal_failures(&mut events).await,
        1,
        "one failed rebuild is reported once, not once per handshake"
    );

    // A new generation that also cannot be rebuilt is reported once more.
    trust.update(Trust::anchor_file(missing_anchor("second")));
    let more = connect_held(&endpoint, &config, addr, 3).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        renewal_failures(&mut events).await,
        1,
        "a new generation is reported once"
    );

    drop(held);
    drop(more);
    drop(binding);
    runtime.shutdown().await;
}

/// Claim: an identity whose key rustls cannot use is reported once per change
/// of the source, and the key in service keeps serving every handshake.
#[tokio::test]
async fn an_identity_rustls_cannot_serve_is_reported_once_per_generation() {
    let server = Certs::generate();
    let other = Certs::generate();
    let source = IdentitySource::external(server.identity()).expect("identity");

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(source.clone()),
        )
        .await
        .expect("bind");
    let addr = binding.local_addr();
    let endpoint = client_endpoint();
    let config = client_config(&server, None, false);
    let mut events = source.events();

    // A certificate of one key with the other key: it parses, so the update
    // is accepted, but rustls cannot pair them.
    source
        .update(Identity::from_pem_files(&other.cert_pem, &server.key_pem))
        .expect("the material parses");

    let held = connect_held(&endpoint, &config, addr, 20).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        held.iter().all(|conn| conn.close_reason().is_none()),
        "the previous key stays in service"
    );
    assert_eq!(
        renewal_failures(&mut events).await,
        1,
        "one unusable generation is reported once, not once per handshake"
    );

    drop(held);
    drop(binding);
    runtime.shutdown().await;
}
