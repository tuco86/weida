//! BUS against itself, over real connections.

use std::time::Duration;

use weida_nng::{BusSocket, Context, ContextConfig, Error, SocketOptions, bus};

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: Some(Duration::from_millis(500)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

async fn node(ctx: &Context) -> (BusSocket, String) {
    let node = BusSocket::with_options(ctx, options()).expect("bus");
    let url = node
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    (node, url)
}

/// Claim: a message reaches every **directly connected** peer and nobody
/// else. A three-node line — A to B to C — leaves C with nothing when A
/// sends, which is what "a mesh must be fully connected" costs (§4).
#[tokio::test]
async fn a_three_node_line_does_not_reach_the_far_node() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (middle, middle_url) = node(&ctx).await;

    let near = BusSocket::with_options(&ctx, options()).expect("bus");
    near.dial(&middle_url).await.expect("dial");
    let far = BusSocket::with_options(&ctx, options()).expect("bus");
    far.dial(&middle_url).await.expect("dial");
    while middle.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let sent = near.send(b"hello".to_vec()).expect("send");
    assert_eq!(sent.queued, 1, "A has exactly one direct peer: B");

    let at_middle = middle.recv().await.expect("B is directly connected");
    assert_eq!(at_middle.body(), b"hello");

    let nothing = far.recv().await.unwrap_err();
    assert!(
        matches!(nothing, Error::ETIMEDOUT(_)),
        "C is two hops away and BUS does not flood: {nothing:?}"
    );

    // The mesh has to be built by the application. Once it is, C sees it.
    far.dial(&near_url(&near).await).await.expect("dial A");
    while near.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let sent = near.send(b"again".to_vec()).expect("send");
    assert_eq!(sent.queued, 2);
    assert_eq!(far.recv().await.expect("now direct").body(), b"again");
}

async fn near_url(near: &BusSocket) -> String {
    near.listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string()
}

/// Claim: a peer that cannot receive loses its copy while the send still
/// succeeds — best effort, non-blocking, no receipt (§4).
#[tokio::test]
async fn a_peer_that_cannot_receive_loses_its_copy_and_the_send_succeeds() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (hub, url) = node(&ctx).await;

    let stalled = BusSocket::with_options(
        &ctx,
        SocketOptions {
            recv_depth: Some(1),
            ..options()
        },
    )
    .expect("bus");
    stalled.dial(&url).await.expect("dial");
    while hub.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Fill everything between the two: the stalled node never reads, so
    // its queue and the kernel's buffers are all there is.
    let mut dropped = 0usize;
    let payload = vec![0x5Au8; 32 * 1024];
    for _ in 0..400 {
        let sent = hub.send(payload.clone()).expect("a BUS send never fails");
        dropped += sent.dropped;
    }
    assert!(
        dropped > 0,
        "a node that never reads must eventually lose copies; none were dropped"
    );
}

/// Claim: a re-broadcast excludes the pipe the message arrived on, which
/// is the one hop of loop control BUS has — and the ingress pipe is
/// carried in the message's header, in the shape raw BUS puts on the wire
/// (§3, §4).
#[tokio::test]
async fn a_rebroadcast_excludes_the_pipe_it_arrived_on() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (hub, url) = node(&ctx).await;

    let first = BusSocket::with_options(&ctx, options()).expect("bus");
    let second = BusSocket::with_options(&ctx, options()).expect("bus");
    first.dial(&url).await.expect("dial");
    second.dial(&url).await.expect("dial");
    while hub.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    first.send(b"around".to_vec()).expect("send");
    let arrived = hub.recv().await.expect("the hub sees it");
    assert_eq!(arrived.body(), b"around");
    let ingress = bus::ingress_of(&arrived).expect("the ingress pipe is in the header");
    assert_eq!(arrived.header().len(), 4, "four big-endian octets");

    let forwarded = hub.rebroadcast(&arrived).expect("rebroadcast");
    assert_eq!(
        forwarded.queued, 1,
        "one of the two pipes was excluded: the one it came in on"
    );
    assert_eq!(
        second.recv().await.expect("the other node").body(),
        b"around"
    );
    let echo = first.recv().await.unwrap_err();
    assert!(
        matches!(echo, Error::ETIMEDOUT(_)),
        "the sender must not be echoed its own message: {echo:?}"
    );

    // And the same exclusion by pipe id, for a caller that kept it itself.
    first.send(b"twice".to_vec()).expect("send");
    let arrived = hub.recv().await.expect("again");
    assert_eq!(bus::ingress_of(&arrived), Some(ingress));
    let forwarded = hub
        .send_excluding(ingress, arrived.body().to_vec())
        .expect("send excluding");
    assert_eq!(forwarded.queued, 1);

    // A message that never came from a BUS socket cannot be re-broadcast.
    let err = hub
        .rebroadcast(&weida_nng::Message::from_body(b"invented".to_vec()))
        .unwrap_err();
    assert!(matches!(err, Error::EPROTO(_)), "{err:?}");
}

/// Claim: a BUS node with no peer succeeds and reaches nobody, and a
/// closed socket says so (§4, §6).
#[tokio::test]
async fn a_send_with_no_peer_reaches_nobody_and_succeeds() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (alone, _url) = node(&ctx).await;
    let sent = alone.send(b"nobody".to_vec()).expect("send");
    assert_eq!(sent.queued, 0);
    assert_eq!(sent.dropped, 0);

    alone.close();
    let err = alone.send(b"after".to_vec()).unwrap_err();
    assert!(matches!(err, Error::ECLOSED(_)), "{err:?}");
}
