//! Claim: a socket whose two directions are independent can be split into
//! halves that are usable at the same time, so a receive that has nothing to
//! receive does not hold up a send on the same socket.
//!
//! The shape of every test here: park a receive on one half in a task,
//! prove it is parked (it has not returned after the send), send on the
//! other half, and assert the send completed — then let the receive finish
//! by giving it something. REQ and REP have no `split`, and that is asserted
//! by the type system rather than here.

mod common;

use std::time::Duration;

use common::within;
use weida_zmq::{
    Context, ContextConfig, DealerSocket, Message, Multipart, PairSocket, RouterSocket, Sent,
    XPubSocket, XSubSocket,
};

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

fn body(message: &Multipart) -> &[u8] {
    message.frames().last().expect("a frame").as_slice()
}

/// DEALER against DEALER: the receive parked on one half, the send on the
/// other half goes through, and the reply the send provokes unparks it.
#[tokio::test]
async fn a_dealers_send_does_not_wait_behind_its_parked_receive() {
    let ctx = context();
    let ours = DealerSocket::new(&ctx).expect("dealer");
    let bound = ours.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut theirs = DealerSocket::new(&ctx).expect("dealer");
    theirs.connect(&bound.to_string()).expect("connect");

    let (mut send, mut recv) = ours.split();
    // Nothing has been sent to us: this receive parks.
    let parked = tokio::spawn(async move {
        let got = recv.recv().await.expect("recv");
        (recv, got)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !parked.is_finished(),
        "nothing was sent, so the receive is parked"
    );

    // The send on the other half is not behind it.
    within(send.send("ping")).await.expect("send");
    let request = within(theirs.recv()).await.expect("the peer got it");
    assert_eq!(body(&request), b"ping");

    // Now the receive has something.
    within(theirs.send("pong")).await.expect("reply");
    let (_recv, got) = within(parked).await.expect("task");
    assert_eq!(body(&got), b"pong");
    drop(send);
}

/// PAIR: the same claim over the exclusive pair.
#[tokio::test]
async fn a_pairs_send_does_not_wait_behind_its_parked_receive() {
    let ctx = context();
    let ours = PairSocket::new(&ctx).expect("pair");
    let bound = ours.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut theirs = PairSocket::new(&ctx).expect("pair");
    theirs.connect(&bound.to_string()).expect("connect");

    let (mut send, mut recv) = ours.split();
    let parked = tokio::spawn(async move { recv.recv().await.expect("recv") });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!parked.is_finished());

    within(send.send("ping")).await.expect("send");
    assert_eq!(body(&within(theirs.recv()).await.expect("recv")), b"ping");
    within(theirs.send("pong")).await.expect("reply");
    assert_eq!(body(&within(parked).await.expect("task")), b"pong");
}

/// ROUTER: the two halves share the routing table, so a routing id learned
/// by the receiving half is one the sending half can address.
#[tokio::test]
async fn a_routers_halves_share_the_routing_table() {
    let ctx = context();
    let router = RouterSocket::new(&ctx).expect("router");
    let bound = router.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut client = DealerSocket::new(&ctx).expect("dealer");
    client.connect(&bound.to_string()).expect("connect");

    let (mut send, mut recv) = router.split();
    // The receive parks until the client speaks; the send half is free
    // meanwhile, and a send to a routing id nobody holds is the drop the
    // ROUTER documents — proving the send did not wait behind the receive.
    let parked = tokio::spawn(async move {
        let got = recv.recv().await.expect("recv");
        (recv, got)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!parked.is_finished());
    let stranger =
        Multipart::new(vec![Message::from("nobody"), Message::from("x")]).expect("frames");
    assert_eq!(
        within(send.send(stranger)).await.expect("send"),
        Sent::Dropped,
        "a send on the free half completes while the receive is parked"
    );

    within(client.send("hello")).await.expect("send");
    let (_recv, request) = within(parked).await.expect("task");
    let key = request.frames()[0].clone();
    assert_eq!(body(&request), b"hello");

    // The id the receiving half learned is routable from the sending half.
    let reply = Multipart::new(vec![key, Message::from("world")]).expect("frames");
    assert_eq!(within(send.send(reply)).await.expect("send"), Sent::Queued);
    assert_eq!(body(&within(client.recv()).await.expect("recv")), b"world");
}

/// XPUB/XSUB: the subscription travels through the XSUB's sending half and
/// arrives on the XPUB's receiving half while the XPUB's publishing half is
/// in use and the XSUB's receiving half is parked.
#[tokio::test]
async fn a_pubsub_pairs_halves_are_independent() {
    let ctx = context();
    let xpub = XPubSocket::new(&ctx).expect("xpub");
    let bound = xpub.bind("tcp://127.0.0.1:0").await.expect("bind");
    let xsub = XSubSocket::new(&ctx).expect("xsub");
    xsub.connect(&bound.to_string()).expect("connect");

    let (mut publish, mut events) = xpub.split();
    let (mut subscribe, mut deliveries) = xsub.split();

    // The subscriber's receive parks: nothing has been published yet.
    let parked = tokio::spawn(async move { deliveries.recv().await.expect("recv") });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!parked.is_finished());

    // The subscription goes out on the other half and reaches the
    // publisher's receiving half.
    subscribe.subscribe(b"px").expect("subscribe");
    let seen = within(events.recv()).await.expect("subscription");
    assert_eq!(body(&seen), b"\x01px");
    assert!(publish.anybody_wants(b"px.eur"));

    // Publishing on the publishing half — while the receiving half of the
    // same socket is free to be read again — unparks the subscriber.
    within(async {
        loop {
            let report = publish.publish(Multipart::single("px.eur 1.09"));
            if report.delivered == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(body(&within(parked).await.expect("task")), b"px.eur 1.09");
}

/// Claim: the connections live as long as either half does. Dropping one
/// half does not close the socket; dropping both does.
#[tokio::test]
async fn the_engine_lives_while_either_half_does() {
    let ctx = context();
    let ours = DealerSocket::new(&ctx).expect("dealer");
    let bound = ours.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut theirs = DealerSocket::new(&ctx).expect("dealer");
    theirs.connect(&bound.to_string()).expect("connect");
    within(theirs.send("first")).await.expect("send");

    let (send, mut recv) = ours.split();
    assert_eq!(body(&within(recv.recv()).await.expect("recv")), b"first");
    drop(send);
    // One half gone: the connection is still there.
    within(theirs.send("second")).await.expect("send");
    assert_eq!(body(&within(recv.recv()).await.expect("recv")), b"second");

    drop(recv);
    // Both gone: the peer sees the close. Its dialled queue stays — that
    // is the engine's rule, so the socket can reconnect — but the
    // connection behind it is gone.
    within(async {
        while theirs.connections().iter().any(|peer| peer.connected) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}
