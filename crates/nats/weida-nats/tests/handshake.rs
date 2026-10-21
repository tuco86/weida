//! The handshake against a scripted server, control line by control
//! line.
//!
//! The thing to assert first is the one that is the opposite of every
//! client-speaks-first protocol: **the server sends `INFO` before the client
//! sends anything at all**. So the scripted server, having accepted the
//! connection, first proves the socket is silent and only then writes its
//! `INFO`.
//!
//! The rest of the file walks the handshake: the `CONNECT` and its one
//! credential form, TLS completed before ordinary traffic, `PING`/`PONG` in
//! both directions with the unanswered ones bounded, asynchronous `INFO` with
//! `connect_urls` and the `ldm` drain notice, and `max_payload` enforced
//! locally.

use crate::support;

use support::options;
use weida_nats::{Connection, Error};
use weida_runtime::Exec;

/// A configuration that asks for TLS on the plain constructor is refused
/// where it was configured, before anything is dialled.
#[tokio::test]
async fn require_tls_on_the_plain_constructor_is_refused_where_it_is_configured() {
    let exec = Exec::current().unwrap();
    let mut options = options();
    options.require_tls = true;
    // No listener at all: the refusal must happen before a socket exists.
    let error = Connection::connect(&exec, "127.0.0.1", 1, options)
        .await
        .expect_err("refused");
    assert!(
        matches!(&error, Error::Configuration(why) if why.contains("connect_tls")),
        "{error}"
    );
}

/// The same scripted server, one layer up: everything after the cleartext
/// `INFO` happens inside the TLS session.
#[cfg(feature = "tls")]
pub(crate) struct TlsServer {
    pub(crate) stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    pub(crate) buf: Vec<u8>,
    pub(crate) from: usize,
}

#[cfg(feature = "tls")]
impl TlsServer {
    pub(crate) async fn read_op(&mut self) -> String {
        use tokio::io::AsyncReadExt;
        use weida_nats_codec::{Limits, Op};

        loop {
            if self.buf.len() > self.from {
                match Op::decode(&self.buf[self.from..], Limits::DEFAULT) {
                    Ok((op, used)) => {
                        let rendered = support::render(&op);
                        self.from += used;
                        return rendered;
                    }
                    Err(error) if !error.is_violation() => {}
                    Err(error) => panic!("unreadable inside TLS: {error}"),
                }
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed inside TLS");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    pub(crate) async fn write(&mut self, bytes: &[u8]) {
        use tokio::io::AsyncWriteExt;

        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }
}

/// A certificate for `localhost`, and the two configurations that present and
/// trust it.
#[cfg(feature = "tls")]
pub(crate) fn tls_configs() -> (
    std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    std::sync::Arc<tokio_rustls::rustls::ServerConfig>,
) {
    use std::sync::Arc;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
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
