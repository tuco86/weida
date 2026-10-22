//! Interop against hyper 1, the foreign peer: hyper's HTTP/1.1 and HTTP/2
//! servers under our client, and hyper's clients against our server.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use weida_http::{Client, ClientOptions, Fields, Method, Protocol, Status, Version};
use weida_runtime::Exec;

use crate::common::{pattern, tls_pair, within};

type HyperBody = BoxBody<Bytes, Infallible>;

/// `/three` answers `abc`; `/echo` answers the request body; `/stream`
/// answers 4 MiB in 64 KiB pieces without a length, which hyper sends
/// chunked on HTTP/1.1; `/trailers` answers `t` with the request's `x-in`
/// trailer back as `x-out`.
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
        "/trailers" => {
            let collected = req.into_body().collect().await.unwrap();
            let echoed = collected
                .trailers()
                .and_then(|t| t.get("x-in").cloned())
                .unwrap();
            let (mut tx, body) = http_body_util::channel::Channel::<Bytes, Infallible>::new(2);
            tokio::spawn(async move {
                let _ = tx.send_data(Bytes::from_static(b"t")).await;
                let mut trailers = http::HeaderMap::new();
                trailers.insert("x-out", echoed);
                let _ = tx.send_trailers(trailers).await;
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

/// hyper's HTTP/1.1 client against our server: one connection, three
/// requests on it.
#[tokio::test]
async fn hyper_h1_client_against_our_server() {
    let exec = Exec::current().unwrap();
    let server =
        Arc::new(weida_http::Server::new(&exec, weida_http::ServerOptions::default()).unwrap());
    let addr = server
        .bind_tcp(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    crate::common::serve_app(server.clone(), None);

    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);

    let send = |path: &str, method: &str, body: Bytes| {
        hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", addr.to_string())
            .body(Full::new(body))
            .unwrap()
    };

    let r = within(
        "GET /three",
        sender.send_request(send("/three", "GET", Bytes::new())),
    )
    .await
    .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().contains_key("date"));
    let body = within("the body", r.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, "abc");

    let sent = Bytes::from(pattern(1 << 20));
    let r = within(
        "POST /echo",
        sender.send_request(send("/echo", "POST", sent.clone())),
    )
    .await
    .unwrap();
    let body = within("the echo", r.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, sent);

    let r = within(
        "GET /stream",
        sender.send_request(send("/stream", "GET", Bytes::new())),
    )
    .await
    .unwrap();
    assert_eq!(
        r.headers().get("transfer-encoding").map(|v| v.as_bytes()),
        Some(&b"chunked"[..])
    );
    let body = within("the stream", r.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body.len(), 4 << 20);
}

/// A hyper HTTP/2 server on loopback over TLS with ALPN `h2`.
async fn hyper_h2(tls: Arc<ServerConfig>) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let mut config = (*tls).clone();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            count.fetch_add(1, Ordering::SeqCst);
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(tcp).await else {
                    return;
                };
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service_fn(handle))
                    .await;
            });
        }
    });
    (addr, accepted)
}

#[tokio::test]
async fn our_client_against_hyper_h2() {
    let (client_tls, server_tls) = tls_pair();
    let (addr, accepted) = hyper_h2(server_tls).await;
    let base = format!("https://localhost:{}", addr.port());
    let client = Client::new(
        &Exec::current().unwrap(),
        ClientOptions {
            tls: Some(client_tls),
            protocol: Protocol::Http2,
            ..ClientOptions::default()
        },
    )
    .unwrap();

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
    assert_eq!(r.version, Version::Http2);
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
    assert_eq!(
        within("the echo", r.body.collect(2 << 20)).await.unwrap(),
        sent
    );

    // Fifty at once, on the connection the first request opened.
    let mut tasks = Vec::new();
    for i in 0..50u32 {
        let client = client.clone();
        let url = format!("{base}/echo");
        tasks.push(tokio::spawn(async move {
            let body = Bytes::from(i.to_string());
            let mut r = client
                .request(Method::POST, &url, Fields::new(), body.clone())
                .await
                .unwrap();
            assert_eq!(r.body.collect(64).await.unwrap(), body);
        }));
    }
    for task in tasks {
        within("a concurrent request", task).await.unwrap();
    }

    let mut open = within(
        "open /trailers",
        client.open(Method::POST, &format!("{base}/trailers"), Fields::new()),
    )
    .await
    .unwrap();
    within("a write", open.write(Bytes::from_static(b"x")))
        .await
        .unwrap();
    let mut trailers = Fields::new();
    trailers.append("x-in", "42").unwrap();
    let mut r = within("finish", open.finish_with_trailers(trailers))
        .await
        .unwrap();
    assert_eq!(within("the body", r.body.collect(16)).await.unwrap(), "t");
    assert_eq!(
        r.body
            .trailers()
            .and_then(|t| t.get("x-out"))
            .map(|v| &v[..]),
        Some(&b"42"[..])
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn hyper_h2_client_against_our_server() {
    let exec = Exec::current().unwrap();
    let (client_tls, server_tls) = tls_pair();
    let server =
        Arc::new(weida_http::Server::new(&exec, weida_http::ServerOptions::default()).unwrap());
    let addr = server
        .bind_tls(SocketAddr::from(([127, 0, 0, 1], 0)), server_tls)
        .await
        .unwrap();
    crate::common::serve_app(server.clone(), None);

    let mut config = (*client_tls).clone();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls = within("the TLS handshake", connector.connect(name, tcp))
        .await
        .unwrap();
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    let (sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
            .await
            .unwrap();
    tokio::spawn(conn);

    let uri = |path: &str| format!("https://localhost:{}{path}", addr.port());
    let request = |path: &str, method: &str, body: Bytes| {
        hyper::Request::builder()
            .method(method)
            .uri(uri(path))
            .body(Full::new(body).boxed())
            .unwrap()
    };

    let mut s = sender.clone();
    let r = within(
        "GET /three",
        s.send_request(request("/three", "GET", Bytes::new())),
    )
    .await
    .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.version(), http::Version::HTTP_2);
    assert!(r.headers().contains_key("date"));
    let body = within("the body", r.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, "abc");

    let sent = Bytes::from(pattern(1 << 20));
    let r = within(
        "POST /echo",
        s.send_request(request("/echo", "POST", sent.clone())),
    )
    .await
    .unwrap();
    let body = within("the echo", r.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, sent);

    // Fifty at once on hyper's one connection: every one names the same
    // client port.
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let mut s = sender.clone();
        let req = request("/peer", "GET", Bytes::new());
        tasks.push(tokio::spawn(async move {
            let r = s.send_request(req).await.unwrap();
            r.into_body().collect().await.unwrap().to_bytes()
        }));
    }
    let mut ports = std::collections::HashSet::new();
    for task in tasks {
        ports.insert(within("a concurrent request", task).await.unwrap());
    }
    assert_eq!(ports.len(), 1);

    let (mut tx, body) = http_body_util::channel::Channel::<Bytes, Infallible>::new(2);
    tokio::spawn(async move {
        let _ = tx.send_data(Bytes::from_static(b"x")).await;
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-in", http::HeaderValue::from_static("42"));
        let _ = tx.send_trailers(trailers).await;
    });
    let req = hyper::Request::builder()
        .method("POST")
        .uri(uri("/trailers"))
        .body(body.boxed())
        .unwrap();
    let r = within("POST /trailers", s.send_request(req)).await.unwrap();
    let collected = within("the body", r.into_body().collect()).await.unwrap();
    assert_eq!(
        collected
            .trailers()
            .and_then(|t| t.get("x-out"))
            .map(|v| v.as_bytes()),
        Some(&b"42"[..])
    );
    assert_eq!(collected.to_bytes(), "t");
}
