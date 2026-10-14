//! Interop against an independent ZeroMQ: the pure-Rust `zeromq` crate
//! ("zmq.rs") **0.6.0**, measured on 2026-09-11.
//!
//! Every other test in this crate drives our sockets against our own codec,
//! which is byte-exact against the golden vectors of `weida-zmtp` and
//! therefore faithful — and shares every assumption with the code under
//! test. This file is the other kind: an implementation that shares no code
//! and no assumptions with ours, where a disagreement is an interoperability
//! bug rather than a tautology. No supervised process and no C library:
//! zmq.rs is a dev-dependency, so `cargo test` is the whole apparatus.
//!
//! **Both roles, every pairing both implement.** Ten tests: for REQ/REP,
//! DEALER/ROUTER, PUSH/PULL, PUB/SUB and XPUB/XSUB, once with our socket
//! **bound** and zmq.rs connecting, once with our socket **connecting** and
//! zmq.rs bound. PAIR is absent from the matrix because zmq.rs does not
//! implement it (`docs/research/zeromq.md` §13), which is a gap in the peer
//! and not a disagreement.
//!
//! # The three skews, exercised rather than assumed
//!
//! `docs/research/zeromq.md` §13 records three things about zmq.rs 0.6. Each
//! has a test here that fails if we stop accommodating it:
//!
//! 1. **It announces ZMTP 3.0.** Every test in this file proves we accept a
//!    3.0 greeting, because none of them would connect otherwise.
//! 2. **It decodes exactly one command, `READY`.** Anything else is "Unknown
//!    command received" and the connection dies. So `ZMQ_HEARTBEAT_IVL` must
//!    not produce a `PING` against a 3.0 peer, and
//!    [`a_heartbeat_is_suppressed_against_a_three_zero_peer`] sets a 50 ms
//!    interval, talks for two seconds, and asserts the connection lives —
//!    which it could not do if a single `PING` had gone out.
//! 3. **Its subscriptions are ZMTP 2.0's one-frame message form.** Our
//!    default is the 3.x `SUBSCRIBE` command, which zmq.rs's publisher
//!    cannot read, so a subscriber of ours must be configured with
//!    [`SubscriptionForm::LegacyMessage`].
//!    [`the_command_subscription_form_is_not_understood_by_zmq_rs`] measures
//!    both directions of that: the command form delivers nothing and the
//!    legacy form delivers.
//!
//! # Disagreements measured against zmq.rs 0.6.0
//!
//! * **Subscriptions.** As skew 3. zmq.rs's `SubSocket` and `XSubSocket`
//!   send the `%x01`/`%x00` message form and its `PubSocket`/`XPubSocket`
//!   read only that form. We accept both on the way in and choose the form
//!   we send with `SubscriptionForm`; against zmq.rs the legacy form is
//!   mandatory, and against libzmq either works. Recorded, not worked
//!   around: the option exists for this.
//! * **Heartbeats.** As skew 2. A `PING` is fatal to zmq.rs 0.6, so our
//!   heartbeat is gated on the negotiated version rather than on the option.
//!   The cost is named where it is paid: a 3.0 peer gets no liveness check
//!   at all.
//! * **An XPUB applies a subscription only when the application receives
//!   it.** zmq.rs 0.6's `XPubSocket::recv` calls `message_received` on its
//!   backend and then hands the message on (xpub.rs:181-204); a publisher
//!   that never calls `recv` has no subscribers and drops everything.
//!   libzmq applies the subscription in the socket unless
//!   `ZMQ_XPUB_MANUAL` is set, and so do we — for us that behaviour is the
//!   option and not the default.
//!   [`our_xsub_subscribes_to_a_zmq_rs_xpub`] therefore reads the
//!   subscription on their side before publishing, and says so.
//! * **Their DEALER announces no routing id.** So the id our ROUTER routes
//!   it by is one we generated, which is 37/ZMTP's own rule ("the Identity
//!   property shall be ignored" for all but the REQ/DEALER/ROUTER peers of
//!   a ROUTER, and a peer that announces none gets one). Not a
//!   disagreement, but it is the reason
//!   [`our_router_routes_a_zmq_rs_dealer`] asserts a non-empty id rather
//!   than a particular one.
//! * **An empty subscription.** zmq.rs's `SubSocket::subscribe("")`
//!   subscribes to everything, as ours does, and our publisher's prefix
//!   match agrees with theirs on every topic tested.
//! * **Nothing else disagreed.** All ten pairings carried a message in both
//!   roles, in order where order is promised, with no adjustment on either
//!   side beyond the subscription form and the XPUB `recv` above.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::within;
use weida_zmq::{
    Context, ContextConfig, DealerSocket, Message, Multipart, PairSocket, PubSocket, PullSocket,
    PushSocket, RepSocket, ReqSocket, RouterSocket, SocketOptions, SubSocket, SubscriptionForm,
    XPubSocket, XSubSocket,
};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// How many OS-chosen ports one bind may lose to another test before the
/// machine, rather than the race, is the explanation.
const PROBES: usize = 8;

/// Binds *their* socket to a loopback port the OS picked, retrying on
/// `AddrInUse` with a fresh probe, and returns the endpoint both sides use.
///
/// Only our own `bind` can report a port back; zmq.rs's takes a concrete one,
/// so the port has to be probed by binding `127.0.0.1:0`, reading it and
/// letting go — and between letting go and their bind, another test binary of
/// the suite can take it. That window cannot be closed while their bind needs
/// a number, so it is retried instead; the bound keeps a machine with no free
/// ports from looking like a flake.
async fn bound_by_them(socket: &mut impl Socket) -> String {
    let mut taken = None;
    for _ in 0..PROBES {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        let endpoint = format!("tcp://127.0.0.1:{port}");
        match within(socket.bind(&endpoint)).await {
            Ok(_) => return endpoint,
            Err(zeromq::ZmqError::Network(error))
                if error.kind() == std::io::ErrorKind::AddrInUse =>
            {
                taken = Some(port);
            }
            Err(error) => panic!("zmq.rs binds {endpoint}: {error}"),
        }
    }
    panic!("{PROBES} probed ports in a row were taken, the last of them {taken:?}");
}

/// The text of a single-frame message of ours.
fn text(message: &Multipart) -> String {
    String::from_utf8_lossy(message.frames()[0].as_slice()).into_owned()
}

/// The text of a single-frame message of theirs.
fn zmq_text(message: &ZmqMessage) -> String {
    String::from_utf8_lossy(message.get(0).expect("a frame")).into_owned()
}

/// Publishes until the subscriber gets one, which is the only honest way to
/// wait for a subscription: "SHALL silently drop the message if the queue
/// for a subscriber is full", and a subscription in flight is a subscriber
/// that is not there yet.
///
/// A macro rather than a function because both halves borrow a socket
/// mutably, and one `async` block that holds both is what a test would have
/// written by hand anyway.
macro_rules! until_delivered {
    ($publisher:expr, $subscriber:expr, $payload:expr) => {
        within(async {
            loop {
                let _ = $publisher.send(ZmqMessage::from($payload)).await;
                if let Ok(message) = $subscriber.try_recv() {
                    return text(&message);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    };
}

// ---- REQ/REP ---------------------------------------------------------

/// Our REP bound, their REQ connecting.
#[tokio::test]
async fn our_rep_answers_a_zmq_rs_req() {
    let context = context();
    let mut responder = RepSocket::new(&context).expect("rep");
    let bound = within(responder.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");

    let mut requester = zeromq::ReqSocket::new();
    within(requester.connect(&bound.to_string()))
        .await
        .expect("zmq.rs connects");

    within(requester.send(ZmqMessage::from("ping")))
        .await
        .expect("their request");
    let request = within(responder.recv()).await.expect("our recv");
    assert_eq!(text(&request), "ping");
    within(responder.send(Multipart::single("pong")))
        .await
        .expect("our reply");
    let reply = within(requester.recv()).await.expect("their reply");
    assert_eq!(zmq_text(&reply), "pong");
}

/// Our REQ connecting, their REP bound.
#[tokio::test]
async fn our_req_is_answered_by_a_zmq_rs_rep() {
    let context = context();
    let mut responder = zeromq::RepSocket::new();
    let endpoint = bound_by_them(&mut responder).await;

    let mut requester = ReqSocket::new(&context).expect("req");
    requester.connect(&endpoint).expect("connect");

    within(requester.send(Multipart::single("ping")))
        .await
        .expect("our request");
    let request = within(responder.recv()).await.expect("their recv");
    assert_eq!(zmq_text(&request), "ping");
    within(responder.send(ZmqMessage::from("pong")))
        .await
        .expect("their reply");
    let reply = within(requester.recv()).await.expect("our reply");
    assert_eq!(text(&reply), "pong");
}

// ---- DEALER/ROUTER ---------------------------------------------------

/// Our ROUTER bound, their DEALER connecting: the routing id we prepend is
/// the one we send back with, and their DEALER never sees it.
#[tokio::test]
async fn our_router_routes_a_zmq_rs_dealer() {
    let context = context();
    let mut router = RouterSocket::new(&context).expect("router");
    let bound = within(router.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");

    let mut dealer = zeromq::DealerSocket::new();
    within(dealer.connect(&bound.to_string()))
        .await
        .expect("zmq.rs connects");

    within(dealer.send(ZmqMessage::from("task")))
        .await
        .expect("their send");
    let arrived = within(router.recv()).await.expect("our recv");
    assert_eq!(arrived.len(), 2, "the routing id and the body");
    let routing_id = arrived.frames()[0].clone();
    assert!(
        !routing_id.is_empty(),
        "a routing id was learned: zmq.rs 0.6's DEALER announces none, so it is ours to generate"
    );
    assert_eq!(
        String::from_utf8_lossy(arrived.frames()[1].as_slice()),
        "task"
    );

    let reply = Multipart::new(vec![routing_id, Message::from("done")]).expect("a reply");
    within(router.send(reply)).await.expect("our send");
    let back = within(dealer.recv()).await.expect("their recv");
    assert_eq!(back.len(), 1, "a DEALER sees no envelope");
    assert_eq!(zmq_text(&back), "done");
}

/// Our DEALER connecting, their ROUTER bound.
#[tokio::test]
async fn our_dealer_talks_to_a_zmq_rs_router() {
    let context = context();
    let mut router = zeromq::RouterSocket::new();
    let endpoint = bound_by_them(&mut router).await;

    let mut dealer = DealerSocket::new(&context).expect("dealer");
    dealer.connect(&endpoint).expect("connect");

    within(dealer.send(Multipart::single("task")))
        .await
        .expect("our send");
    let arrived = within(router.recv()).await.expect("their recv");
    assert!(arrived.len() >= 2, "their ROUTER prepends a routing id");
    let routing_id = arrived.get(0).expect("a routing id").clone();
    assert_eq!(
        String::from_utf8_lossy(arrived.get(arrived.len() - 1).expect("the body")),
        "task"
    );

    let mut reply = ZmqMessage::from(routing_id);
    reply.push_back(Bytes::from_static(b"done"));
    within(router.send(reply)).await.expect("their send");
    let back = within(dealer.recv()).await.expect("our recv");
    assert_eq!(text(&back), "done");
}

// ---- PUSH/PULL -------------------------------------------------------

/// Our PULL bound, their PUSH connecting.
#[tokio::test]
async fn our_pull_drains_a_zmq_rs_push() {
    let context = context();
    let mut puller = PullSocket::new(&context).expect("pull");
    let bound = within(puller.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");

    let mut pusher = zeromq::PushSocket::new();
    within(pusher.connect(&bound.to_string()))
        .await
        .expect("zmq.rs connects");

    for index in 0..3 {
        within(pusher.send(ZmqMessage::from(format!("task {index}"))))
            .await
            .expect("their send");
    }
    for index in 0..3 {
        let arrived = within(puller.recv()).await.expect("our recv");
        assert_eq!(text(&arrived), format!("task {index}"), "in order");
    }
}

/// Our PUSH connecting, their PULL bound.
#[tokio::test]
async fn our_push_feeds_a_zmq_rs_pull() {
    let context = context();
    let mut puller = zeromq::PullSocket::new();
    let endpoint = bound_by_them(&mut puller).await;

    let mut pusher = PushSocket::new(&context).expect("push");
    pusher.connect(&endpoint).expect("connect");

    for index in 0..3 {
        within(pusher.send(Multipart::single(format!("task {index}"))))
            .await
            .expect("our send");
    }
    for index in 0..3 {
        let arrived = within(puller.recv()).await.expect("their recv");
        assert_eq!(zmq_text(&arrived), format!("task {index}"), "in order");
    }
}

// ---- PUB/SUB ---------------------------------------------------------

/// Our PUB bound, their SUB connecting: their subscription arrives in ZMTP
/// 2.0's message form and our publisher's prefix match honours it.
#[tokio::test]
async fn our_pub_reaches_a_zmq_rs_sub() {
    let context = context();
    let mut publisher = PubSocket::new(&context).expect("pub");
    let bound = within(publisher.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");

    let mut subscriber = zeromq::SubSocket::new();
    within(subscriber.connect(&bound.to_string()))
        .await
        .expect("zmq.rs connects");
    within(subscriber.subscribe("px")).await.expect("subscribe");

    let received = within(async {
        loop {
            publisher.publish(Multipart::single("px.eur 1.09"));
            if let Ok(Ok(message)) =
                tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await
            {
                return message;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(zmq_text(&received), "px.eur 1.09");

    // A topic nobody subscribed to is not delivered: their filter and ours
    // agree about what "px" matches.
    publisher.publish(Multipart::single("fx.usd 1.00"));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), subscriber.recv())
            .await
            .is_err(),
        "an unsubscribed topic was delivered"
    );
}

/// Our SUB connecting, their PUB bound — with the legacy subscription form,
/// which is the only one their publisher reads.
#[tokio::test]
async fn our_sub_receives_from_a_zmq_rs_pub() {
    let context = context();
    let mut publisher = zeromq::PubSocket::new();
    let endpoint = bound_by_them(&mut publisher).await;

    let mut subscriber = SubSocket::with_options(
        &context,
        SocketOptions {
            subscription_form: SubscriptionForm::LegacyMessage,
            ..SocketOptions::default()
        },
    )
    .expect("sub");
    subscriber.connect(&endpoint).expect("connect");
    subscriber.subscribe("px").expect("subscribe");

    let received = until_delivered!(publisher, subscriber, "px.eur 1.09");
    assert_eq!(received, "px.eur 1.09");
}

// ---- XPUB/XSUB -------------------------------------------------------

/// Our XPUB bound, their XSUB connecting: the subscription reaches our
/// application as a message, in the `1`/`0` form the API uses whatever the
/// wire form was.
#[tokio::test]
async fn our_xpub_serves_a_zmq_rs_xsub() {
    let context = context();
    let mut publisher = XPubSocket::new(&context).expect("xpub");
    let bound = within(publisher.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");

    let mut subscriber = zeromq::XSubSocket::new();
    within(subscriber.connect(&bound.to_string()))
        .await
        .expect("zmq.rs connects");
    within(subscriber.subscribe("px")).await.expect("subscribe");

    let announced = within(publisher.recv()).await.expect("a subscription");
    let frame = announced.frames()[0].as_slice();
    assert_eq!(frame[0], 1, "a subscription, not a cancellation");
    assert_eq!(&frame[1..], b"px");

    let received = within(async {
        loop {
            publisher.publish(Multipart::single("px.eur 1.09"));
            if let Ok(Ok(message)) =
                tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await
            {
                return message;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(zmq_text(&received), "px.eur 1.09");
}

/// Our XSUB connecting, their XPUB bound.
///
/// **Measured disagreement.** zmq.rs 0.6's `XPubSocket` applies a
/// subscription only as a side effect of the application calling `recv()`:
/// `XPubSocket::recv` calls `message_received` on its backend and then hands
/// the message on (xpub.rs:181-204). A publisher that never receives
/// therefore has no subscribers and drops everything. libzmq applies the
/// subscription in the socket unless `ZMQ_XPUB_MANUAL` is set, and so do we
/// — our `ZMQ_XPUB_MANUAL` is the option that asks for zmq.rs's behaviour.
/// So this test reads the subscription on their side before publishing,
/// which is what a zmq.rs proxy has to do anyway.
#[tokio::test]
async fn our_xsub_subscribes_to_a_zmq_rs_xpub() {
    let context = context();
    let mut publisher = zeromq::XPubSocket::new();
    let endpoint = bound_by_them(&mut publisher).await;

    let mut subscriber = XSubSocket::with_options(
        &context,
        SocketOptions {
            subscription_form: SubscriptionForm::LegacyMessage,
            ..SocketOptions::default()
        },
    )
    .expect("xsub");
    subscriber.connect(&endpoint).expect("connect");
    subscriber.subscribe("px").expect("subscribe");
    // Their XPUB learns the subscription here and nowhere else.
    let announced = within(publisher.recv()).await.expect("a subscription");
    let frame = announced.get(0).expect("a frame");
    assert_eq!(frame[0], 1, "a subscription in the legacy message form");
    assert_eq!(&frame[1..], b"px");

    let received = until_delivered!(publisher, subscriber, "px.eur 1.09");
    assert_eq!(received, "px.eur 1.09");
}

// ---- the skews -------------------------------------------------------

/// Skew 2, measured: `ZMQ_HEARTBEAT_IVL` must produce no `PING` against a
/// peer that announced ZMTP 3.0, because zmq.rs 0.6 answers any command but
/// `READY` with "Unknown command received" and closes.
///
/// A 50 ms interval over two seconds is forty `PING`s that must not happen.
#[tokio::test]
async fn a_heartbeat_is_suppressed_against_a_three_zero_peer() {
    let context = context();
    let mut puller = PullSocket::with_options(
        &context,
        SocketOptions {
            heartbeat_ivl: Some(Duration::from_millis(50)),
            heartbeat_timeout: Some(Duration::from_millis(500)),
            ..SocketOptions::default()
        },
    )
    .expect("pull");
    let bound = within(puller.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");

    let mut pusher = zeromq::PushSocket::new();
    within(pusher.connect(&bound.to_string()))
        .await
        .expect("zmq.rs connects");
    within(pusher.send(ZmqMessage::from("first")))
        .await
        .expect("their send");
    assert_eq!(text(&within(puller.recv()).await.expect("recv")), "first");

    // Forty heartbeat intervals of silence, and then the same connection
    // again: a single PING in there would have ended it.
    tokio::time::sleep(Duration::from_secs(2)).await;
    within(pusher.send(ZmqMessage::from("second")))
        .await
        .expect("their send after the silence");
    assert_eq!(text(&within(puller.recv()).await.expect("recv")), "second");
}

/// Skews 2 and 3, measured: the 3.x `SUBSCRIBE` command — our default — is
/// not understood by zmq.rs 0.6's publisher, and the ZMTP 2.0 message form
/// is. This is the disagreement `SubscriptionForm` exists for, and the test
/// measures both sides of it rather than trusting the sheet.
#[tokio::test]
async fn the_command_subscription_form_is_not_understood_by_zmq_rs() {
    let context = context();
    let mut publisher = zeromq::PubSocket::new();
    let endpoint = bound_by_them(&mut publisher).await;

    // The default form: the command goes out and nothing comes back.
    let mut commanding = SubSocket::new(&context).expect("sub");
    commanding.connect(&endpoint).expect("connect");
    commanding.subscribe("px").expect("subscribe");
    for _ in 0..10 {
        let _ = publisher.send(ZmqMessage::from("px.eur 1.09")).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        commanding.try_recv().is_err(),
        "zmq.rs 0.6 answered a SUBSCRIBE command after all - the sheet's skew 3 is stale"
    );

    // The legacy form, on the same publisher: delivered.
    let mut legacy = SubSocket::with_options(
        &context,
        SocketOptions {
            subscription_form: SubscriptionForm::LegacyMessage,
            ..SocketOptions::default()
        },
    )
    .expect("sub");
    legacy.connect(&endpoint).expect("connect");
    legacy.subscribe("px").expect("subscribe");
    let received = until_delivered!(publisher, legacy, "px.eur 1.09");
    assert_eq!(received, "px.eur 1.09");
}

/// The gap in the peer rather than a disagreement: zmq.rs 0.6 has no PAIR
/// socket, so the pairing cannot be tested against it at all. Ours exists
/// and talks to itself, which is what `interop_libzmq.rs` checks against
/// libzmq.
#[tokio::test]
async fn pair_is_absent_from_zmq_rs() {
    let context = context();
    let mut left = PairSocket::new(&context).expect("pair");
    let bound = within(left.bind("tcp://127.0.0.1:0")).await.expect("bind");
    let mut right = PairSocket::new(&context).expect("pair");
    right.connect(&bound.to_string()).expect("connect");
    within(left.send(Multipart::single("hello")))
        .await
        .expect("send");
    assert_eq!(
        text(&within(right.recv()).await.expect("recv")),
        "hello",
        "our PAIR works; zmq.rs has none to test it against"
    );
}
