//! Every socket type over `inproc://`.
//!
//! The point of this file is a negative: **no pattern knows which transport
//! carried it.** A socket type chooses which pipe a message goes to and in
//! what shape; the bytes underneath are an `AsyncRead + AsyncWrite` either
//! way, and the ZMTP session above them is the same code. So this is not a
//! second copy of the pattern suites — those live beside the patterns and
//! run over `tcp://` — it is one exchange per socket type over `inproc://`,
//! which is the claim the transport has to earn.
//!
//! What is specific to `inproc` and tested here rather than there: the
//! connect that arrives before the bind, and two contexts that never meet.
//! The namespace's own rules (one owner per name, the 256-byte budget, drop
//! as unbind) are unit-tested in `src/inproc.rs`.

use weida_zmq::{
    Context, ContextConfig, DealerSocket, Message, Multipart, PairSocket, PubSocket, PullSocket,
    PushSocket, RepSocket, ReqSocket, RouterSocket, SubSocket, XPubSocket, XSubSocket,
};

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// A fresh `inproc://` name per exchange, so that one test cannot take
/// another's: the namespace is context-scoped, not test-scoped.
fn name(what: &str) -> String {
    format!("inproc://{what}")
}

fn body(message: &Multipart) -> Vec<Vec<u8>> {
    message
        .frames()
        .iter()
        .map(|frame| frame.as_slice().to_vec())
        .collect()
}

fn text(message: &Multipart) -> Vec<u8> {
    message.frames()[0].as_slice().to_vec()
}

async fn wait_for(done: impl FnMut() -> bool) {
    weida_testkit::until("the condition", weida_testkit::EXPECTED, done).await;
}

/// Claim: REQ and REP complete a round trip over `inproc://` — the state
/// machine is the socket type's and the transport is none of its business.
#[tokio::test]
async fn req_and_rep_over_inproc() {
    let ctx = context();
    let endpoint = name("reqrep");
    let mut server = RepSocket::new(&ctx).expect("rep");
    server.bind(&endpoint).await.expect("bind");
    let mut client = ReqSocket::new(&ctx).expect("req");
    client.connect(&endpoint).expect("connect");

    client.send("question").await.expect("request");
    assert_eq!(
        body(&server.recv().await.expect("recv")),
        vec![b"question".to_vec()]
    );
    server.send("answer").await.expect("reply");
    assert_eq!(text(&client.recv().await.expect("reply")), b"answer");
}

/// Claim: DEALER and ROUTER cross `inproc://` with the routing id intact,
/// which is the one thing about ROUTER that could have depended on a peer
/// address.
#[tokio::test]
async fn dealer_and_router_over_inproc() {
    let ctx = context();
    let endpoint = name("dealerrouter");
    let mut router = RouterSocket::new(&ctx).expect("router");
    router.bind(&endpoint).await.expect("bind");
    let mut dealer = DealerSocket::new(&ctx).expect("dealer");
    dealer.connect(&endpoint).expect("connect");

    dealer.send("work").await.expect("send");
    let request = router.recv().await.expect("recv");
    let frames = body(&request);
    assert_eq!(frames.len(), 2, "a routing id and the message");
    assert_eq!(frames[1], b"work".to_vec());

    let mut reply = Multipart::single(Message::from(frames[0].clone()));
    reply.push(Message::from("done"));
    router.send(reply).await.expect("reply");
    assert_eq!(text(&dealer.recv().await.expect("reply")), b"done");
}

/// Claim: PUB and SUB filter at the publisher over `inproc://` too — the
/// subscription travels in the session, and the session is transport-blind.
#[tokio::test]
async fn pub_and_sub_over_inproc() {
    let ctx = context();
    let endpoint = name("pubsub");
    let mut publisher = PubSocket::new(&ctx).expect("pub");
    publisher.bind(&endpoint).await.expect("bind");
    let mut subscriber = SubSocket::new(&ctx).expect("sub");
    subscriber.connect(&endpoint).expect("connect");
    wait_for(|| publisher.subscriber_count() == 1).await;

    subscriber.subscribe("weather").expect("subscribe");
    wait_for(|| publisher.anybody_wants(b"weather.uk")).await;
    assert_eq!(publisher.publish("sport.uk").unmatched, 1);
    assert_eq!(publisher.publish("weather.uk sunny").delivered, 1);
    assert_eq!(
        text(&subscriber.recv().await.expect("recv")),
        b"weather.uk sunny"
    );
}

/// Claim: XPUB and XSUB proxy over `inproc://`, which is where a pub/sub
/// proxy actually lives: the subscription reaches the application as a
/// message and the message reaches the publisher's application.
#[tokio::test]
async fn xpub_and_xsub_over_inproc() {
    let ctx = context();
    let endpoint = name("xpubxsub");
    let mut broker = XPubSocket::new(&ctx).expect("xpub");
    broker.bind(&endpoint).await.expect("bind");
    let mut upstream = XSubSocket::new(&ctx).expect("xsub");
    upstream.connect(&endpoint).expect("connect");
    wait_for(|| broker.subscriber_count() == 1).await;

    upstream.subscribe("topic").expect("subscribe");
    assert_eq!(
        text(&broker.recv().await.expect("subscription")),
        b"\x01topic"
    );
    assert_eq!(broker.publish("topic.one").delivered, 1);
    assert_eq!(text(&upstream.recv().await.expect("recv")), b"topic.one");
}

/// Claim: PUSH and PULL carry a message over `inproc://`, which is the
/// transport the pipeline pattern is most often used on — "a SUB thread
/// pushing to PULL workers over inproc".
#[tokio::test]
async fn push_and_pull_over_inproc() {
    let ctx = context();
    let endpoint = name("pushpull");
    let mut puller = PullSocket::new(&ctx).expect("pull");
    puller.bind(&endpoint).await.expect("bind");
    let mut pusher = PushSocket::new(&ctx).expect("push");
    pusher.connect(&endpoint).expect("connect");

    pusher.send("task").await.expect("send");
    assert_eq!(text(&puller.recv().await.expect("recv")), b"task");
}

/// Claim: PAIR works over `inproc://` in both directions — and this is the
/// transport it is really for, since a PAIR socket does not reconnect and is
/// therefore "effectively inproc-only" (`docs/research/zeromq.md` §12).
#[tokio::test]
async fn pair_over_inproc() {
    let ctx = context();
    let endpoint = name("pair");
    let mut bound = PairSocket::new(&ctx).expect("pair");
    bound.bind(&endpoint).await.expect("bind");
    let mut dialled = PairSocket::new(&ctx).expect("pair");
    dialled.connect(&endpoint).expect("connect");
    wait_for(|| dialled.has_peer()).await;

    dialled.send("there").await.expect("send");
    assert_eq!(text(&bound.recv().await.expect("recv")), b"there");
    bound.send("and back").await.expect("send back");
    assert_eq!(text(&dialled.recv().await.expect("recv")), b"and back");
}

/// Claim: a socket may connect before anybody binds and the exchange still
/// completes when the bind arrives — libzmq 4.0's change, at the level an
/// application sees it.
#[tokio::test]
async fn a_connect_before_the_bind_still_exchanges() {
    let ctx = context();
    let endpoint = name("late-bind");
    let mut pusher = PushSocket::new(&ctx).expect("push");
    pusher
        .connect(&endpoint)
        .expect("connect with nobody there");

    // The queue exists before the connection does, so this is accepted now
    // and travels once the bind happens.
    pusher.send("early").await.expect("queued");

    let mut puller = PullSocket::new(&ctx).expect("pull");
    puller.bind(&endpoint).await.expect("bind, afterwards");
    assert_eq!(text(&puller.recv().await.expect("recv")), b"early");
}

/// Claim: two contexts in one process never meet, even on the same name —
/// "two contexts are two separate ZeroMQ instances".
#[tokio::test]
async fn two_contexts_never_meet() {
    let here = context();
    let there = context();
    let endpoint = name("shared-name");

    let mut mine = PullSocket::new(&here).expect("pull");
    mine.bind(&endpoint).await.expect("bind here");
    // The same name is free in the other context, which it would not be if
    // the namespace were process-wide.
    let mut theirs = PullSocket::new(&there).expect("pull");
    theirs.bind(&endpoint).await.expect("bind there");

    let mut pusher = PushSocket::new(&there).expect("push");
    pusher.connect(&endpoint).expect("connect");
    pusher.send("for the other context").await.expect("send");
    assert_eq!(
        text(&theirs.recv().await.expect("recv")),
        b"for the other context"
    );
    assert!(
        mine.try_recv().is_err(),
        "a message must not cross a context boundary"
    );
}
