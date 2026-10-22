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

/// Two signals around the test application's `/slow` exchange.
#[derive(Default)]
pub struct Gate {
    /// The exchange is in the application's hands.
    pub arrived: tokio::sync::Notify,
    /// The application may answer it.
    pub release: tokio::sync::Notify,
}

/// The test application: answers every exchange `server` accepts until it
/// closes.
///
/// * `/three`: `abc`;
/// * `/echo`: the request body, collected under 8 MiB;
/// * `/stream`: 4 MiB in 64 KiB writes with no length (chunked on HTTP/1.1);
/// * `/peer`: the client's port, which names its connection;
/// * `/trailers`: the request body's `x-in` trailer back as an `x-out`
///   trailer, after the body `t`;
/// * `/slow`: says it `arrived` on the gate, then waits for its `release`
///   before answering `late`.
pub fn serve_app(
    server: Arc<weida_http::Server>,
    gate: Option<Arc<Gate>>,
) -> tokio::task::JoinHandle<()> {
    use bytes::Bytes;
    use weida_http::{Fields, ResponseHead, Status};
    tokio::spawn(async move {
        while let Ok(mut ex) = server.accept().await {
            let gate = gate.clone();
            tokio::spawn(async move {
                let ok = ResponseHead::new(Status::OK);
                match ex.head.path.as_str() {
                    "/three" => {
                        let _ = ex
                            .responder
                            .respond_bytes(ok, Bytes::from_static(b"abc"))
                            .await;
                    }
                    "/echo" => {
                        let body = ex.body.collect(8 << 20).await.unwrap();
                        let _ = ex.responder.respond_bytes(ok, body).await;
                    }
                    "/stream" => {
                        let mut w = ex.responder.respond(ok).await.unwrap();
                        let piece = Bytes::from(pattern(64 * 1024));
                        for _ in 0..64 {
                            w.write(piece.clone()).await.unwrap();
                        }
                        w.finish().await.unwrap();
                    }
                    "/peer" => {
                        let port = ex.peer.port().to_string();
                        let _ = ex.responder.respond_bytes(ok, Bytes::from(port)).await;
                    }
                    "/trailers" => {
                        ex.body.collect(1 << 20).await.unwrap();
                        let echoed = ex
                            .body
                            .trailers()
                            .and_then(|t| t.get("x-in").cloned())
                            .unwrap_or_default();
                        let mut w = ex.responder.respond(ok).await.unwrap();
                        w.write(Bytes::from_static(b"t")).await.unwrap();
                        let mut trailers = Fields::new();
                        trailers.append("x-out", echoed).unwrap();
                        w.finish_with_trailers(trailers).await.unwrap();
                    }
                    "/slow" => {
                        if let Some(gate) = gate {
                            gate.arrived.notify_one();
                            gate.release.notified().await;
                        }
                        let _ = ex
                            .responder
                            .respond_bytes(ok, Bytes::from_static(b"late"))
                            .await;
                    }
                    _ => {
                        let _ = ex
                            .responder
                            .respond_bytes(ResponseHead::new(Status::NOT_FOUND), Bytes::new())
                            .await;
                    }
                }
            });
        }
    })
}
