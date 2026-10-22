//! Interop against hyper 1, the foreign peer: hyper's HTTP/1.1 server under
//! our client, plain and over TLS.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use weida_http::{Client, ClientOptions, Fields, Method, Status, Version};
use weida_runtime::Exec;

use crate::common::{pattern, tls_pair, within};

type HyperBody = BoxBody<Bytes, Infallible>;

/// `/three` answers `abc`; `/echo` answers the request body; `/stream`
/// answers 4 MiB in 64 KiB pieces without a length, which hyper sends
/// chunked.
async fn handle(req: hyper::Request<Incoming>) -> Result<hyper::Response<HyperBody>, Infallible> {
    let body = match req.uri().path() {
        "/three" => Full::new(Bytes::from_static(b"abc")).boxed(),
        "/echo" => {
            let got = req.into_body().collect().await.unwrap().to_bytes();
            Full::new(got).boxed()
        }
        "/stream" => {
            let (mut tx, body) = http_body_util::channel::Channel::<Bytes, Infallible>::new(4);
            tokio::spawn(async move {
                let piece = Bytes::from(pattern(64 * 1024));
                for _ in 0..64 {
                    if tx.send_data(piece.clone()).await.is_err() {
                        return;
                    }
                }
            });
            body.boxed()
        }
        _ => Full::new(Bytes::new()).boxed(),
    };
    Ok(hyper::Response::new(body))
}

/// A hyper HTTP/1.1 server on loopback; returns its address and a count of
/// accepted connections.
async fn hyper_h1(tls: Option<Arc<ServerConfig>>) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let tls = tls.map(|config| {
        let mut config = (*config).clone();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    });
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            count.fetch_add(1, Ordering::SeqCst);
            let tls = tls.clone();
            tokio::spawn(async move {
                let builder = hyper::server::conn::http1::Builder::new();
                match tls {
                    Some(acceptor) => {
                        let Ok(stream) = acceptor.accept(tcp).await else {
                            return;
                        };
                        let _ = builder
                            .serve_connection(TokioIo::new(stream), service_fn(handle))
                            .await;
                    }
                    None => {
                        let _ = builder
                            .serve_connection(TokioIo::new(tcp), service_fn(handle))
                            .await;
                    }
                }
            });
        }
    });
    (addr, accepted)
}

fn client(tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>) -> Client {
    let options = ClientOptions {
        tls,
        ..ClientOptions::default()
    };
    Client::new(&Exec::current().unwrap(), options).unwrap()
}

/// The four h1 rows against one server.
async fn h1_rows(base: &str, client: &Client, accepted: &AtomicUsize) {
    let mut r = within(
        "GET /three",
        client.request(
            Method::GET,
            &format!("{base}/three"),
            Fields::new(),
            Bytes::new(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(r.head.status, Status::OK);
    assert_eq!(r.version, Version::Http11);
    assert_eq!(within("the body", r.body.collect(16)).await.unwrap(), "abc");

    let sent = Bytes::from(pattern(1 << 20));
    let mut r = within(
        "POST /echo",
        client.request(
            Method::POST,
            &format!("{base}/echo"),
            Fields::new(),
            sent.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(r.body.content_length(), Some(1 << 20));
    assert_eq!(
        within("the echo", r.body.collect(2 << 20)).await.unwrap(),
        sent
    );

    let mut r = within(
        "GET /stream",
        client.request(
            Method::GET,
            &format!("{base}/stream"),
            Fields::new(),
            Bytes::new(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(r.body.content_length(), None, "chunked");
    let got = within("the stream", r.body.collect(8 << 20)).await.unwrap();
    assert_eq!(got.len(), 4 << 20);
    assert!(got.chunks(64 * 1024).all(|c| c == pattern(64 * 1024)));

    // Every request above went over the connection the first one opened.
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn our_client_against_hyper_h1() {
    let (addr, accepted) = hyper_h1(None).await;
    h1_rows(&format!("http://{addr}"), &client(None), &accepted).await;
}

#[tokio::test]
async fn our_client_against_hyper_h1_over_tls() {
    let (client_tls, server_tls) = tls_pair();
    let (addr, accepted) = hyper_h1(Some(server_tls)).await;
    h1_rows(
        &format!("https://localhost:{}", addr.port()),
        &client(Some(client_tls)),
        &accepted,
    )
    .await;
}

#[tokio::test]
async fn two_requests_share_one_keep_alive_connection() {
    let (addr, accepted) = hyper_h1(None).await;
    let client = client(None);
    for _ in 0..2 {
        let mut r = within(
            "GET /three",
            client.request(
                Method::GET,
                &format!("http://{addr}/three"),
                Fields::new(),
                Bytes::new(),
            ),
        )
        .await
        .unwrap();
        within("the body", r.body.collect(16)).await.unwrap();
    }
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}
