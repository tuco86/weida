//! Answers every request with `hello over <version>`, on HTTP/1.1 (plain),
//! HTTP/1.1 or HTTP/2 over TLS, and HTTP/3 over QUIC, with a self-signed
//! certificate for `localhost` and `127.0.0.1`.
//!
//! ```text
//! cargo run -p weida-http --features h3 --example hello -- [http-port] [https-port]
//! curl http://127.0.0.1:8080/
//! curl -k --http2 https://127.0.0.1:8443/
//! curl -k --http3-only https://127.0.0.1:8443/
//! ```
//!
//! The QUIC listener shares the TLS port number, on UDP.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use weida_http::{ResponseHead, Server, ServerOptions, Status};
use weida_runtime::Exec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let http: u16 = args.next().map_or(Ok(8080), |a| a.parse())?;
    let https: u16 = args.next().map_or(Ok(8443), |a| a.parse())?;

    let issued =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    let cert = CertificateDer::from(issued.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(issued.signing_key.serialize_der()));
    let tls = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?,
    );

    let server = Server::new(&Exec::current()?, ServerOptions::default())?;
    let tcp = server
        .bind_tcp(SocketAddr::from(([0, 0, 0, 0], http)))
        .await?;
    let tls_addr = server
        .bind_tls(SocketAddr::from(([0, 0, 0, 0], https)), tls.clone())
        .await?;
    let quic = server
        .bind_quic(SocketAddr::from(([0, 0, 0, 0], https)), tls)
        .await?;
    println!("http  {tcp}");
    println!("https {tls_addr} (HTTP/1.1, HTTP/2)");
    println!("h3    {quic} (UDP)");

    loop {
        let exchange = server.accept().await?;
        tokio::spawn(async move {
            let mut head = ResponseHead::new(Status::OK);
            head.fields
                .insert("content-type", "text/plain")
                .expect("a field value");
            let body = Bytes::from(format!("hello over {}\n", exchange.version));
            if let Err(error) = exchange.responder.respond_bytes(head, body).await {
                eprintln!("{}: {error}", exchange.peer);
            }
        });
    }
}
