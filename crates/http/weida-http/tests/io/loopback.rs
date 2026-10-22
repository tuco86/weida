//! Our client against our server on loopback, plain and over TLS.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;
use weida_http::{Client, ClientOptions, Fields, Method, Server, ServerOptions, Status, Version};
use weida_runtime::Exec;

use crate::common::{pattern, serve_app, tls_pair, within};

struct Fixture {
    base: String,
    client: Client,
    server: Arc<Server>,
    release: Arc<Notify>,
}

async fn fixture(tls: bool) -> Fixture {
    let exec = Exec::current().unwrap();
    let server = Arc::new(Server::new(&exec, ServerOptions::default()).unwrap());
    let any = SocketAddr::from(([127, 0, 0, 1], 0));
    let (base, client_tls) = if tls {
        let (client_tls, server_tls) = tls_pair();
        let addr = server.bind_tls(any, server_tls).await.unwrap();
        (
            format!("https://localhost:{}", addr.port()),
            Some(client_tls),
        )
    } else {
        let addr = server.bind_tcp(any).await.unwrap();
        (format!("http://{addr}"), None)
    };
    let release = Arc::new(Notify::new());
    serve_app(server.clone(), Some(release.clone()));
    let options = ClientOptions {
        tls: client_tls,
        ..ClientOptions::default()
    };
    Fixture {
        base,
        client: Client::new(&exec, options).unwrap(),
        server,
        release,
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
    assert_eq!(r.version, Version::Http11);
    let body = within("the body", r.body.collect(8 << 20)).await.unwrap();
    (r.head.status, body)
}

async fn rows(tls: bool) {
    let f = fixture(tls).await;
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
    rows(false).await;
}

#[tokio::test]
async fn our_client_against_our_server_over_tls() {
    rows(true).await;
}

#[tokio::test]
async fn close_lets_a_response_in_flight_finish() {
    let f = fixture(false).await;
    let url = format!("{}/slow", f.base);
    let client = f.client.clone();
    let pending = tokio::spawn(async move {
        client
            .request(Method::GET, &url, Fields::new(), Bytes::new())
            .await
    });
    // Wait until the exchange is in the application's hands: a second
    // request on another connection gets its answer only after that.
    assert_eq!(get(&f, "/three").await.0, Status::OK);
    let server = f.server.clone();
    let closing = tokio::spawn(async move { server.close().await });
    tokio::task::yield_now().await;
    f.release.notify_one();
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
