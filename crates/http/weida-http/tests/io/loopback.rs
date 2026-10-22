//! Our client against our server on loopback, plain and over TLS.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::common::Gate;
use bytes::Bytes;
use weida_http::{
    Client, ClientOptions, Fields, Method, Protocol, Server, ServerOptions, Status, Version,
};
use weida_runtime::Exec;

use crate::common::{pattern, serve_app, tls_pair, within};

struct Fixture {
    base: String,
    client: Client,
    server: Arc<Server>,
    gate: Arc<Gate>,
    version: Version,
}

#[cfg(feature = "h3")]
async fn bind_quic(
    server: &Server,
    addr: SocketAddr,
    tls: Arc<tokio_rustls::rustls::ServerConfig>,
) -> SocketAddr {
    server.bind_quic(addr, tls).await.unwrap()
}

#[cfg(not(feature = "h3"))]
async fn bind_quic(
    _: &Server,
    _: SocketAddr,
    _: Arc<tokio_rustls::rustls::ServerConfig>,
) -> SocketAddr {
    unreachable!("no h3 rows without the feature")
}

async fn fixture(tls: bool, protocol: Protocol) -> Fixture {
    let exec = Exec::current().unwrap();
    let server = Arc::new(Server::new(&exec, ServerOptions::default()).unwrap());
    let any = SocketAddr::from(([127, 0, 0, 1], 0));
    let version = match (tls, protocol) {
        (true, Protocol::Auto | Protocol::Http2) => Version::Http2,
        (true, Protocol::Http3) => Version::Http3,
        _ => Version::Http11,
    };
    let (base, client_tls) = if tls {
        let (client_tls, server_tls) = tls_pair();
        if protocol == Protocol::Http3 {
            let addr = bind_quic(&server, any, server_tls).await;
            (format!("https://{addr}"), Some(client_tls))
        } else {
            let addr = server.bind_tls(any, server_tls).await.unwrap();
            (
                format!("https://localhost:{}", addr.port()),
                Some(client_tls),
            )
        }
    } else {
        let addr = server.bind_tcp(any).await.unwrap();
        (format!("http://{addr}"), None)
    };
    let gate = Arc::new(Gate::default());
    serve_app(server.clone(), Some(gate.clone()));
    let options = ClientOptions {
        tls: client_tls,
        protocol,
        ..ClientOptions::default()
    };
    Fixture {
        base,
        client: Client::new(&exec, options).unwrap(),
        server,
        gate,
        version,
    }
}

async fn get(f: &Fixture, path: &str) -> (Status, Bytes) {
    let mut r = within(
        path,
        f.client.request(
            Method::GET,
            &format!("{}{path}", f.base),
            Fields::new(),
            Bytes::new(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(r.version, f.version);
    let body = within("the body", r.body.collect(8 << 20)).await.unwrap();
    (r.head.status, body)
}

async fn rows(tls: bool, protocol: Protocol) {
    let f = fixture(tls, protocol).await;
    assert_eq!(get(&f, "/three").await, (Status::OK, Bytes::from("abc")));

    let sent = Bytes::from(pattern(1 << 20));
    let mut r = within(
        "POST /echo",
        f.client.request(
            Method::POST,
            &format!("{}/echo", f.base),
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

    // A streamed upload: no length, so chunked.
    let mut open = within(
        "open /echo",
        f.client
            .open(Method::POST, &format!("{}/echo", f.base), Fields::new()),
    )
    .await
    .unwrap();
    for _ in 0..16 {
        within("a write", open.write(Bytes::from(pattern(4096))))
            .await
            .unwrap();
    }
    let mut r = within("finish", open.finish()).await.unwrap();
    let echoed = within("the echo", r.body.collect(1 << 20)).await.unwrap();
    assert_eq!(echoed.len(), 16 * 4096);
    assert!(echoed.chunks(4096).all(|c| c == pattern(4096)));

    let (_, streamed) = get(&f, "/stream").await;
    assert_eq!(streamed.len(), 4 << 20);

    // Keep-alive: the server sees every request above come from one
    // connection, named by the client's port.
    let (_, first) = get(&f, "/peer").await;
    let (_, second) = get(&f, "/peer").await;
    assert_eq!(first, second);

    // Trailers both ways over chunked framing.
    let mut fields = Fields::new();
    fields.append("x-ignored", "1").unwrap();
    let mut open = within(
        "open /trailers",
        f.client
            .open(Method::POST, &format!("{}/trailers", f.base), fields),
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

#[tokio::test]
async fn our_client_against_our_server() {
    rows(false, Protocol::Auto).await;
}

#[tokio::test]
async fn our_client_against_our_server_over_tls() {
    rows(true, Protocol::Http1).await;
}

#[tokio::test]
async fn our_client_against_our_server_over_h2() {
    rows(true, Protocol::Http2).await;
    // Auto prefers h2 where the server offers it.
    rows(true, Protocol::Auto).await;
}

async fn close_with_one_in_flight(tls: bool, protocol: Protocol) {
    let f = fixture(tls, protocol).await;
    let url = format!("{}/slow", f.base);
    let client = f.client.clone();
    let pending = tokio::spawn(async move {
        client
            .request(Method::GET, &url, Fields::new(), Bytes::new())
            .await
    });
    // Close only once the exchange is in the application's hands.
    within("the slow request", f.gate.arrived.notified()).await;
    let server = f.server.clone();
    let closing = tokio::spawn(async move { server.close().await });
    tokio::task::yield_now().await;
    f.gate.release.notify_one();
    let mut r = within("the slow answer", pending).await.unwrap().unwrap();
    assert_eq!(
        within("its body", r.body.collect(16)).await.unwrap(),
        "late"
    );
    within("close", closing).await.unwrap();
    assert!(matches!(
        f.server.accept().await,
        Err(weida_http::Error::Closed)
    ));
}

#[tokio::test]
async fn close_lets_a_response_in_flight_finish() {
    close_with_one_in_flight(false, Protocol::Http1).await;
}

#[tokio::test]
async fn close_lets_an_h2_response_in_flight_finish() {
    close_with_one_in_flight(true, Protocol::Http2).await;
}

#[cfg(feature = "h3")]
#[tokio::test]
async fn our_client_against_our_server_over_h3() {
    rows(true, Protocol::Http3).await;
}

#[cfg(feature = "h3")]
#[tokio::test]
async fn close_lets_an_h3_response_in_flight_finish() {
    close_with_one_in_flight(true, Protocol::Http3).await;
}
