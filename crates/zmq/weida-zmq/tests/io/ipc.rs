//! The tests of `tests/ipc.rs` that need the network, a process or real time.

#![cfg(unix)]

use std::time::Duration;
use weida_zmq::{PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, SubSocket};

use crate::ipc::*;

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
/// Claim: a second bind of the same path **takes the endpoint over**, which
/// is libzmq's documented behaviour — "this will succeed and the first process
/// will lose its binding. In this behaviour, the `ipc` transport is not
/// consistent with the `tcp` or `inproc` transports." A new connection
/// reaches the second binder, not the incumbent.
///
/// This is a hazard rather than a feature, and it is asserted so that it
/// cannot change by accident: a program ported from libzmq may rely on it.
/// `weida-runtime`'s plain bind refuses a live listener; `weida-zmq` opts into
/// the takeover.
#[tokio::test]
async fn a_second_bind_takes_the_endpoint_over() {
    let ctx = context();
    let path = socket_path("stealing");
    let mut incumbent = PullSocket::new(&ctx).expect("pull");
    incumbent.bind(&endpoint(&path)).await.expect("bind");

    let mut thief = PullSocket::new(&ctx).expect("pull");
    thief
        .bind(&endpoint(&path))
        .await
        .expect("a second bind succeeds, which is the hazard");

    let mut pusher = PushSocket::new(&ctx).expect("push");
    pusher.connect(&endpoint(&path)).expect("connect");
    pusher
        .send("for whoever holds the name")
        .await
        .expect("send");

    let received = tokio::time::timeout(Duration::from_secs(10), thief.recv())
        .await
        .expect("the second binder receives in time")
        .expect("a message");
    assert_eq!(text(&received), b"for whoever holds the name");
    assert!(
        incumbent.try_recv().is_err(),
        "the incumbent lost its binding and receives nothing new"
    );
}
