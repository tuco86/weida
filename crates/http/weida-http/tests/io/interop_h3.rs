//! Interop against `h3` 0.0.8 on `h3-quinn` 0.0.10, the foreign HTTP/3
//! peer: its server under our client, and its client against our server.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use tokio_rustls::rustls::{ClientConfig, ServerConfig};
use weida_http::{Client, ClientOptions, Fields, Method, Protocol, Status, Version};
use weida_runtime::Exec;

use crate::common::{pattern, serve_app, tls_pair, within};

type H3Stream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

async fn read_all(stream: &mut H3Stream) -> Bytes {
    let mut out = BytesMut::new();
    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
        out.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    out.freeze()
}

/// `/three` answers `abc`, `/echo` the request body, `/trailers` `t` with
/// the request's `x-in` trailer back as `x-out`.
async fn answer(req: http::Request<()>, mut stream: H3Stream) {
    let ok = http::Response::builder().status(200).body(()).unwrap();
    match req.uri().path() {
        "/three" => {
            read_all(&mut stream).await;
            stream.send_response(ok).await.unwrap();
            stream.send_data(Bytes::from_static(b"abc")).await.unwrap();
        }
        "/echo" => {
            let body = read_all(&mut stream).await;
            stream.send_response(ok).await.unwrap();
            stream.send_data(body).await.unwrap();
        }
        "/trailers" => {
            read_all(&mut stream).await;
            let got = stream.recv_trailers().await.unwrap().unwrap();
            let echoed = got.get("x-in").cloned().unwrap();
            stream.send_response(ok).await.unwrap();
            stream.send_data(Bytes::from_static(b"t")).await.unwrap();
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-out", echoed);
            stream.send_trailers(trailers).await.unwrap();
        }
        _ => stream.send_response(ok).await.unwrap(),
    }
    stream.finish().await.unwrap();
}

/// An `h3` server on loopback; returns its address.
fn h3_server(tls: Arc<ServerConfig>) -> SocketAddr {
    let mut tls = (*tls).clone();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(crypto)),
        SocketAddr::from(([127, 0, 0, 1], 0)),
    )
    .unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let mut conn = h3::server::Connection::new(h3_quinn::Connection::new(conn))
                    .await
                    .unwrap();
                while let Ok(Some(resolver)) = conn.accept().await {
                    tokio::spawn(async move {
                        if let Ok((req, stream)) = resolver.resolve_request().await {
                            answer(req, stream).await;
                        }
                    });
                }
            });
        }
    });
    addr
}

#[tokio::test]
async fn our_client_against_h3_server() {
    let (client_tls, server_tls) = tls_pair();
    let addr = h3_server(server_tls);
    let base = format!("https://localhost:{}", addr.port());
    let client = Client::new(
        &Exec::current().unwrap(),
        ClientOptions {
            tls: Some(client_tls),
            protocol: Protocol::Http3,
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
    assert_eq!(r.version, Version::Http3);
    assert_eq!(r.head.status, Status::OK);
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
}

async fn h3_client(
    tls: Arc<ClientConfig>,
    addr: SocketAddr,
) -> h3::client::SendRequest<h3_quinn::OpenStreams, Bytes> {
    let mut tls = (*tls).clone();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    let conn = within(
        "the QUIC handshake",
        endpoint.connect(addr, "localhost").unwrap(),
    )
    .await
    .unwrap();
    let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        drop(endpoint);
    });
    send
}

#[tokio::test]
async fn h3_client_against_our_server() {
    let exec = Exec::current().unwrap();
    let (client_tls, server_tls) = tls_pair();
    let server =
        Arc::new(weida_http::Server::new(&exec, weida_http::ServerOptions::default()).unwrap());
    let addr = server
        .bind_quic(SocketAddr::from(([127, 0, 0, 1], 0)), server_tls)
        .await
        .unwrap();
    serve_app(server.clone(), None);
    let send = h3_client(client_tls, addr).await;
    let uri = |path: &str| format!("https://localhost:{}{path}", addr.port());

    let request = |method: &str, path: &str| {
        http::Request::builder()
            .method(method)
            .uri(uri(path))
            .body(())
            .unwrap()
    };
    let exchange = |method: &'static str, path: &'static str, body: Bytes| {
        let mut send = send.clone();
        let req = request(method, path);
        async move {
            let mut stream = send.send_request(req).await.unwrap();
            if !body.is_empty() {
                stream.send_data(body).await.unwrap();
            }
            stream.finish().await.unwrap();
            let resp = stream.recv_response().await.unwrap();
            let mut out = BytesMut::new();
            while let Some(mut chunk) = stream.recv_data().await.unwrap() {
                out.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
            }
            (resp, out.freeze())
        }
    };

    let (resp, body) = within("GET /three", exchange("GET", "/three", Bytes::new())).await;
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().contains_key("date"));
    assert_eq!(body, "abc");

    let sent = Bytes::from(pattern(1 << 20));
    let (_, body) = within("POST /echo", exchange("POST", "/echo", sent.clone())).await;
    assert_eq!(body, sent);

    let mut tasks = Vec::new();
    for _ in 0..50 {
        tasks.push(tokio::spawn(exchange("GET", "/peer", Bytes::new())));
    }
    let mut ports = std::collections::HashSet::new();
    for task in tasks {
        ports.insert(within("a concurrent request", task).await.unwrap().1);
    }
    assert_eq!(ports.len(), 1, "one connection");

    let mut s = send.clone();
    let mut stream = s.send_request(request("POST", "/trailers")).await.unwrap();
    stream.send_data(Bytes::from_static(b"x")).await.unwrap();
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-in", http::HeaderValue::from_static("42"));
    stream.send_trailers(trailers).await.unwrap();
    stream.finish().await.unwrap();
    let resp = within("the trailers response", stream.recv_response())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut body = BytesMut::new();
    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
        body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    assert_eq!(body, "t");
    let trailers = stream.recv_trailers().await.unwrap().unwrap();
    assert_eq!(
        trailers.get("x-out").map(|v| v.as_bytes()),
        Some(&b"42"[..])
    );
}
