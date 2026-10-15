//! The `ipc` transport through the public API.
//!
//! Patterns are transport-blind, so this is not a second copy of the pattern
//! suites either (see `tests/inproc.rs`). What is asserted here is what only
//! `AF_UNIX` has: the socket file, the credentials an application can read
//! off a peer, the path budget, and the endpoint-stealing behaviour libzmq
//! documents and this library shares.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, Endpoint, MAX_IPC_ENDPOINT_BYTES, Multipart, PubSocket, PullSocket,
    PushSocket, RepSocket, ReqSocket, SubSocket,
};

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// A socket path in the temporary directory, unique per test.
///
/// Honest for a test and wrong for a program: `/tmp` is world-writable, which
/// is the substitution hazard `weida_zmq::ipc` documents. A program puts its
/// socket in a directory it owns.
fn socket_path(what: &str) -> PathBuf {
    std::env::temp_dir().join(format!("weida-zmq-it-{}-{what}.sock", std::process::id()))
}

fn endpoint(path: &Path) -> String {
    format!("ipc://{}", path.display())
}

fn text(message: &Multipart) -> Vec<u8> {
    message.frames()[0].as_slice().to_vec()
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "condition never held");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Claim: a request-reply round trip completes over `ipc://`, and the socket
/// file exists while the bind does and is gone once the socket closes — the
/// node belongs to the binding.
#[tokio::test]
async fn req_and_rep_over_ipc() {
    let ctx = context();
    let path = socket_path("reqrep");
    let mut server = RepSocket::new(&ctx).expect("rep");
    server.bind(&endpoint(&path)).await.expect("bind");
    assert!(path.exists(), "the endpoint is a file");

    let mut client = ReqSocket::new(&ctx).expect("req");
    client.connect(&endpoint(&path)).expect("connect");
    client.send("question").await.expect("request");
    assert_eq!(text(&server.recv().await.expect("recv")), b"question");
    server.send("answer").await.expect("reply");
    assert_eq!(text(&client.recv().await.expect("reply")), b"answer");

    server.close();
    wait_for(|| !path.exists()).await;
}

/// Claim: publisher-side filtering works over `ipc://` too, which is the
/// transport v3.x filters at the publisher for, exactly as for `tcp`.
#[tokio::test]
async fn pub_and_sub_over_ipc() {
    let ctx = context();
    let path = socket_path("pubsub");
    let mut publisher = PubSocket::new(&ctx).expect("pub");
    publisher.bind(&endpoint(&path)).await.expect("bind");
    let mut subscriber = SubSocket::new(&ctx).expect("sub");
    subscriber.connect(&endpoint(&path)).expect("connect");
    wait_for(|| publisher.subscriber_count() == 1).await;

    subscriber.subscribe("prices").expect("subscribe");
    wait_for(|| publisher.anybody_wants(b"prices.eur")).await;
    assert_eq!(publisher.publish("prices.eur 1.09").delivered, 1);
    assert_eq!(
        text(&subscriber.recv().await.expect("recv")),
        b"prices.eur 1.09"
    );
}

/// Claim: an application can read the kernel's statement about a local peer
/// off the peer itself — what an authorization handler will be given, and
/// what a TCP peer does not have.
#[tokio::test]
async fn a_local_peer_carries_the_kernels_credentials() {
    let ctx = context();
    let path = socket_path("credentials");
    let mut puller = PullSocket::new(&ctx).expect("pull");
    puller.bind(&endpoint(&path)).await.expect("bind");
    let mut pusher = PushSocket::new(&ctx).expect("push");
    pusher.connect(&endpoint(&path)).expect("connect");

    pusher.send("work").await.expect("send");
    assert_eq!(text(&puller.recv().await.expect("recv")), b"work");

    let peers = puller.connections();
    let accepted = peers.first().expect("one accepted peer");
    let credentials = accepted.credentials.expect("an ipc peer has credentials");
    if let Some(pid) = credentials.pid {
        assert_eq!(
            pid,
            std::process::id(),
            "the peer is this process, and Linux reports its pid"
        );
    }
    // The dialling side holds the same fact about the binder.
    let dialled = pusher.connections();
    assert!(
        dialled
            .first()
            .expect("one dialled peer")
            .credentials
            .is_some()
    );
}

/// Claim: the endpoint string is bounded where libzmq bounds it — 113 bytes
/// including the prefix — so a program ported from libzmq is refused at the
/// same character, before anything touches the filesystem.
#[tokio::test]
async fn the_endpoint_budget_is_libzmqs() {
    let longest = "x".repeat(MAX_IPC_ENDPOINT_BYTES - "ipc:///".len());
    let ok = format!("ipc:///{longest}");
    assert_eq!(ok.len(), MAX_IPC_ENDPOINT_BYTES);
    assert!(Endpoint::parse(&ok).is_ok());

    let err = Endpoint::parse(&format!("{ok}y")).unwrap_err();
    assert_eq!(err.errno(), "EINVAL", "{err}");
}

/// Claim: a second bind of a path somebody is listening on is **refused**
/// with `EADDRINUSE`, as `tcp` and `inproc` refuse it, and the incumbent keeps
/// its endpoint. libzmq's `ipc` instead lets the second bind take the name
/// over ("this will succeed and the first process will lose its binding"),
/// which lets any process that can write the directory silence a service by
/// binding its path. A *stale* node — a socket file nobody listens on, which
/// is what a crash leaves — is still replaced, so the endpoint does not
/// become unbindable.
#[tokio::test]
async fn a_second_bind_does_not_steal_a_live_endpoint() {
    let ctx = context();
    let path = socket_path("stealing");
    let mut incumbent = PullSocket::new(&ctx).expect("pull");
    incumbent.bind(&endpoint(&path)).await.expect("bind");

    let thief = PullSocket::new(&ctx).expect("pull");
    let err = thief
        .bind(&endpoint(&path))
        .await
        .expect_err("a live endpoint is not taken over");
    assert_eq!(err.errno(), "EADDRINUSE", "{err}");

    let mut pusher = PushSocket::new(&ctx).expect("push");
    pusher.connect(&endpoint(&path)).expect("connect");
    pusher.send("for the incumbent").await.expect("send");
    let received = tokio::time::timeout(std::time::Duration::from_secs(10), incumbent.recv())
        .await
        .expect("the incumbent receives in time")
        .expect("a message");
    assert_eq!(text(&received), b"for the incumbent");
}
