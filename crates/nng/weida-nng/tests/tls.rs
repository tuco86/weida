//! `tls+tcp://`, end to end, with certificates generated for the test.
//!
//! What these assert is the six things NNG exposes (§10) and the one thing
//! this library refuses to offer: a sender identity.

use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use weida_nng::{
    AuthMode, Context, ContextConfig, Error, RepSocket, ReqSocket, SocketOptions, TlsConfig,
};

/// A certificate authority and one identity signed by it.
pub(crate) struct Pki {
    pub(crate) ca_pem: Vec<u8>,
    pub(crate) cert_pem: Vec<u8>,
    pub(crate) key_pem: Vec<u8>,
}

pub(crate) fn pki(name: &str) -> Pki {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "weida-nng test CA");
    let ca =
        rcgen::CertifiedIssuer::self_signed(ca_params, rcgen::KeyPair::generate().expect("ca key"))
            .expect("ca");

    let mut leaf_params =
        rcgen::CertificateParams::new(vec![name.to_owned()]).expect("leaf params");
    leaf_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    let leaf_key = rcgen::KeyPair::generate().expect("leaf key");
    let leaf = leaf_params.signed_by(&leaf_key, &ca).expect("leaf");

    Pki {
        ca_pem: ca.pem().into_bytes(),
        cert_pem: leaf.pem().into_bytes(),
        key_pem: leaf_key.serialize_pem().into_bytes(),
    }
}

pub(crate) fn options(tls: TlsConfig) -> SocketOptions {
    SocketOptions {
        tls: Some(tls),
        // No socket timeout: a slow run is reported by the test's own waits,
        // not failed by an option. The handshake bound is an hour.
        recv_timeout: None,
        send_timeout: None,
        handshake_timeout: Duration::from_secs(3600),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

/// Claim: a `tls+tcp` dial whose URL names an address and whose
/// configuration names no server name is refused, because there would be
/// nothing for the certificate to be checked against.
#[tokio::test]
async fn a_tls_dial_with_no_name_to_check_is_refused() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(pki("x").ca_pem),
            cert_pem: None,
            key_pem: None,
            server_name: None,
        }),
    )
    .expect("req");
    let err = req.dial("tls+tcp://127.0.0.1:9").await.unwrap_err();
    assert!(matches!(err, Error::EADDRINVAL(_)), "{err:?}");
    assert!(err.cause().contains("NNG_OPT_TLS_SERVER_NAME"));
}

/// The expected time of every await in the tests below.
pub(crate) const BOUND: Duration = Duration::from_secs(10);

/// The listening side of a configuration that requires the client's
/// certificate, as the round-trip test builds it.
pub(crate) fn listener_tls(server: &Pki, client: &Pki) -> TlsConfig {
    TlsConfig {
        auth_mode: AuthMode::Required,
        ca_pem: Some(client.ca_pem.clone()),
        cert_pem: Some(server.cert_pem.clone()),
        key_pem: Some(server.key_pem.clone()),
        server_name: None,
    }
}

/// The dialling side of the same exchange, naming the server it expects.
pub(crate) fn dialer_tls(server: &Pki, client: &Pki) -> TlsConfig {
    TlsConfig {
        auth_mode: AuthMode::Required,
        ca_pem: Some(server.ca_pem.clone()),
        cert_pem: Some(client.cert_pem.clone()),
        key_pem: Some(client.key_pem.clone()),
        server_name: Some("server.test".to_owned()),
    }
}

/// The `host:port` a plain TCP peer connects to, from a `tls+tcp://` URL.
pub(crate) fn tcp_address(url: &str) -> &str {
    url.trim_start_matches("tls+tcp://")
}

/// Waits for the listener to close `stream` and returns how long that took.
/// End of stream and a reset both count as closed. Bytes arriving first mean
/// the listener spoke to a connection it had not admitted, which is a failure.
/// A close that takes longer than `expected` is reported, not failed.
pub(crate) async fn closed_by_listener(stream: &mut TcpStream, expected: Duration) -> Duration {
    let started = Instant::now();
    let mut byte = [0u8; 1];
    match weida_testkit::within(
        "the listener closing the connection",
        expected,
        stream.read(&mut byte),
    )
    .await
    {
        Ok(0) | Err(_) => started.elapsed(),
        Ok(count) => {
            panic!("the listener sent {count} byte(s) to a connection it had not admitted")
        }
    }
}

/// One request and its reply over an admitted pair, every await reported when
/// it takes longer than [`BOUND`].
pub(crate) async fn exchange_once(req: &ReqSocket, rep: &RepSocket) {
    weida_testkit::within("the request being sent", BOUND, req.send(b"ping".to_vec()))
        .await
        .expect("send");
    let request = weida_testkit::within("the request arriving", BOUND, rep.recv())
        .await
        .expect("recv");
    assert_eq!(request.body(), b"ping");
    weida_testkit::within("the reply being sent", BOUND, rep.send(b"pong".to_vec()))
        .await
        .expect("reply");
    let reply = weida_testkit::within("the reply arriving", BOUND, req.recv())
        .await
        .expect("recv");
    assert_eq!(reply.body(), b"pong");
}

/// Dials until the listener admits the connection, reporting a wait past
/// `budget`. A listener frees a slot a moment after the peer holding it has
/// closed, so a single attempt can still meet that slot as taken.
pub(crate) async fn dial_until_admitted(req: &ReqSocket, url: &str, budget: Duration) {
    weida_testkit::within("the listener admitting a connection", budget, async {
        loop {
            if req.dial(url).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
}
