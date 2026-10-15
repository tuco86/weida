#![cfg(feature = "libzmq-interop")]
//! Interop against the reference implementation: **libzmq 4.3.5**, through
//! the `zmq` crate 0.10, measured on 2026-09-11.
//!
//! `interop_zmqrs.rs` measures against a second Rust implementation, which
//! is independent but not normative. This file measures against the C
//! library every other ZeroMQ is compared to — and it is where the CURVE
//! claim stops being a claim: a `crypto_box` box sealed by
//! [`weida_zmq::curve`] is opened by libsodium inside libzmq, or these tests
//! fail.
//!
//! # Running them
//!
//! libzmq is a **build** condition, not a runtime one: with the feature on
//! and the C library absent, nothing links. So the dependency is optional and
//! the tests are `#[ignore]`d on top of that:
//!
//! ```text
//! # Debian/Ubuntu: apt install libzmq3-dev
//! # Arch:          pacman -S zeromq
//! # macOS:         brew install zeromq
//! cargo test -p weida-zmq --features libzmq-interop -- --ignored
//! ```
//!
//! `pkg-config --modversion libzmq` says which version a machine would
//! measure against; the facts below were measured against 4.3.5.
//!
//! # The matrix
//!
//! Every pairing, with our socket **bound** and then **connecting**:
//! REQ/REP, DEALER/ROUTER, PUSH/PULL, PUB/SUB, XPUB/XSUB and — unlike
//! zmq.rs, which has none — PAIR. Then the mechanisms: PLAIN both roles,
//! CURVE both roles, and ZAP as the thing that decides them, including a
//! refusal.
//!
//! # Measured, not inferred
//!
//! * **Our `SUBSCRIBE`/`CANCEL` commands are what libzmq speaks.** The
//!   default [`SubscriptionForm::Commands`] needs no adjustment here, which
//!   is the other half of what zmq.rs 0.6 measured: the legacy message form
//!   is a concession to a 3.0 peer and not the wire.
//! * **Our `PING` is answered.** libzmq announces ZMTP 3.1, so the heartbeat
//!   this library suppresses against a 3.0 peer runs here and the connection
//!   survives a heartbeat interval many times over.
//! * **CURVE interoperates in both roles.** A libzmq client reaches our
//!   CURVE server and our CURVE client reaches a libzmq server, which means
//!   the `HELLO`/`WELCOME`/`INITIATE`/`READY` octets, the nonce prefixes,
//!   the cookie and the vouch box all agree with 26/CURVEZMQ as libsodium
//!   implements it. That is "the boxes open in libsodium", measured.
//! * **ZAP: the CURVE credential libzmq presents is the client's long-term
//!   public key**, 32 octets, and the PLAIN credential is the username and
//!   password — the frames 27/ZAP specifies, in that order, from a foreign
//!   server's request as well as from ours.
//! * **A ZAP refusal is a closed connection, not a rejected message.** Our
//!   handler answering `400` leaves a libzmq client unable to exchange
//!   anything at all.
//! * **libzmq's ROUTER gives our DEALER an envelope of one frame**, the
//!   routing id it generated for us, and takes the same frame back — the
//!   same shape our ROUTER uses.
//!
//! # Three disagreements, each one a fix on our side
//!
//! All three show up here and nowhere else, and each costs one test run
//! before it costs a line of code. The socket monitor names the first two
//! without a packet capture.
//!
//! 1. **libzmq sends an `Identity` property with an empty value** on every
//!    REQ, DEALER and ROUTER socket that has no `ZMQ_ROUTING_ID` set. A
//!    routing id is 1 to 255 octets, but 37/ZMTP's grammar is
//!    `identity = 0*255OCTET` and only a non-empty identity must not begin
//!    with a zero octet, so an empty property is the absence of one.
//!    `session.rs` reads it that way, and half this matrix depends on it.
//! 2. **libzmq's `as-server` octet is 0 even on a PLAIN or CURVE server.**
//!    24/ZMTP-PLAIN and 25/ZMTP-CURVE both say it "SHALL be 1 for a
//!    server"; the reference implementation disagrees with them and never
//!    reads the peer's octet either, taking each side's role from its own
//!    option. So the guard cannot refuse "both ends are the client" on the
//!    strength of that octet, which would refuse every libzmq server; it
//!    refuses only two announced servers, and
//!    [`libzmq_announces_as_server_zero_even_as_a_server`] pins the
//!    measurement so that a future libzmq that fixes it is noticed.
//! 3. **A CURVE `MESSAGE` travels in a *message* frame, not a command
//!    frame.** 26/CURVEZMQ calls it a command and its body is a command
//!    body — `\x07MESSAGE`, the nonce, the box — but libzmq puts a message
//!    frame header in front of it and closes the connection on the
//!    command-framed form. A handshake with libzmq succeeds and the first
//!    data frame ends it, which is how this shows. So this library sends
//!    what libzmq sends and accepts either kind, because the check that
//!    matters is whether the box opens rather than which flag bit was set.
//!
//! Nothing else disagreed: no pairing and no mechanism needed an adjustment
//! on either side beyond those three.

use crate::common;

use std::time::Duration;

use common::within;
use weida_zmq::curve::{CurvePublicKey, CurveSecretKey, keypair};
use weida_zmq::{
    Context, ContextConfig, DealerSocket, Message, Multipart, PairSocket, PubSocket, PullSocket,
    PushSocket, RepSocket, ReqSocket, RouterSocket, SocketOptions, SubSocket, XPubSocket,
    XSubSocket, ZapReply, ZapRequest, ZapStatus,
};

/// The same bound as `common::DEADLINE`, inside libzmq, so its blocking calls
/// report rather than hang.
const LIBZMQ_TIMEOUT_MS: i32 = 10_000;

/// What every `#[ignore]` says, so that the install command is one grep away
/// from a skipped test.
macro_rules! needs_libzmq {
    () => {
        "needs libzmq installed (apt install libzmq3-dev / pacman -S zeromq / brew install \
         zeromq); run with --features libzmq-interop -- --ignored"
    };
}

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// A libzmq socket with the timeouts these tests need.
fn libzmq_socket(context: &zmq::Context, kind: zmq::SocketType) -> zmq::Socket {
    let socket = context.socket(kind).expect("a libzmq socket");
    socket.set_rcvtimeo(LIBZMQ_TIMEOUT_MS).expect("rcvtimeo");
    socket.set_sndtimeo(LIBZMQ_TIMEOUT_MS).expect("sndtimeo");
    socket.set_linger(0).expect("linger");
    socket
}

fn text(message: &Multipart) -> String {
    String::from_utf8_lossy(message.frames()[0].as_slice()).into_owned()
}

/// A ZAP handler of ours, on the context's `inproc://zeromq.zap.01`, which
/// answers with `decide` and records what it was asked.
///
/// 27/ZAP: "The handler SHALL start before any server starts", so the bind
/// happens before this returns.
async fn zap_handler(
    context: &Context,
    decide: impl Fn(ZapRequest) -> ZapReply + Send + 'static,
) -> (
    std::sync::Arc<std::sync::Mutex<Vec<ZapRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = std::sync::Arc::clone(&asked);
    let mut handler = RepSocket::new(context).expect("rep");
    handler
        .bind(weida_zmq::zap::ZAP_ENDPOINT)
        .await
        .expect("bind the ZAP endpoint");
    let task = tokio::spawn(async move {
        while let Ok(message) = handler.recv().await {
            let request = ZapRequest::decode(&message).expect("a ZAP request");
            seen.lock().expect("seen").push(request.clone());
            let reply = decide(request);
            if handler.send(reply.encode()).await.is_err() {
                return;
            }
        }
    });
    (asked, task)
}

/// A ZAP handler inside a **libzmq** context, for the half of the mechanism
/// matrix where libzmq is the server: it needs one, or it refuses every
/// connection.
///
/// The thread outlives the test on purpose. Its socket holds a reference to
/// the context, so nothing blocks in `zmq_ctx_term`, and a handler that
/// stopped answering would be a hang rather than a failure.
fn libzmq_zap_handler(context: &zmq::Context) {
    let handler = context.socket(zmq::REP).expect("a REP socket");
    handler
        .bind("inproc://zeromq.zap.01")
        .expect("bind the ZAP endpoint");
    std::thread::spawn(move || {
        while let Ok(request) = handler.recv_multipart(0) {
            // version, request id, domain, address, identity, mechanism,
            // credentials… — 27/ZAP's frames, with the envelope already
            // stripped by the REP socket.
            let reply: Vec<Vec<u8>> = vec![
                b"1.0".to_vec(),
                request[1].clone(),
                b"200".to_vec(),
                b"OK".to_vec(),
                b"interop".to_vec(),
                Vec::new(),
            ];
            if handler.send_multipart(reply, 0).is_err() {
                return;
            }
        }
    });
}

// ---- REQ/REP ---------------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_rep_answers_a_libzmq_req() {
    let context = context();
    let mut responder = RepSocket::new(&context).expect("rep");
    let bound = within(responder.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let requester = libzmq_socket(&ctx, zmq::REQ);
        requester.connect(&endpoint).expect("connect");
        requester.send("ping", 0).expect("send");
        requester.recv_bytes(0).expect("recv")
    });

    let request = within(responder.recv()).await.expect("our recv");
    assert_eq!(text(&request), "ping");
    within(responder.send(Multipart::single("pong")))
        .await
        .expect("our reply");
    assert_eq!(peer.await.expect("the peer"), b"pong");
}

/// **Measured, not inferred**: libzmq 4.3.5 sends the greeting's `as-server`
/// octet as **0** from a socket configured as the PLAIN *or* CURVE
/// **server**.
///
/// 24/ZMTP-PLAIN and 25/ZMTP-CURVE both say the octet "SHALL be 1 for a
/// server, 0 for a client", so this is the reference implementation
/// disagreeing with the specifications its own community wrote. It never
/// reads the peer's octet either — each side takes its role from its own
/// `ZMQ_PLAIN_SERVER`/`ZMQ_CURVE_SERVER` option — which is presumably why
/// nobody noticed.
///
/// The consequence for this library is in `session.rs`: a peer's zero proves
/// nothing, so only "both ends announce 1" is refused and two clients are
/// left to `ZMQ_HANDSHAKE_IVL`. If this test ever fails, libzmq changed and
/// that guard can be tightened again — it is not our side that is wrong.
///
/// The octets are read with a raw socket, because the claim is about octets.
/// Our greeting goes first: libzmq writes the signature and major version
/// immediately and the remaining 53 octets only after reading the peer's, so
/// a reader that says nothing gets eleven octets and an EOF.
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn libzmq_announces_as_server_zero_even_as_a_server() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (_public, secret) = keypair();
    let curve_secret = secret_key_bytes(&secret);
    for mechanism in [weida_zmtp::Mechanism::PLAIN, weida_zmtp::Mechanism::CURVE] {
        let curve_secret = curve_secret.clone();
        let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
        let plain = mechanism == weida_zmtp::Mechanism::PLAIN;
        tokio::task::spawn_blocking(move || {
            let ctx = zmq::Context::new();
            libzmq_zap_handler(&ctx);
            let responder = libzmq_socket(&ctx, zmq::REP);
            if plain {
                responder.set_plain_server(true).expect("plain server");
            } else {
                responder.set_curve_server(true).expect("curve server");
                responder
                    .set_curve_secretkey(&curve_secret)
                    .expect("secret key");
            }
            responder.bind("tcp://127.0.0.1:0").expect("bind");
            endpoint_tx
                .send(
                    responder
                        .get_last_endpoint()
                        .expect("last endpoint")
                        .expect("utf-8"),
                )
                .expect("the test is listening");
            let _ = responder.recv_bytes(0);
        });

        let endpoint = within(endpoint_rx).await.expect("the endpoint");
        let mut stream = tokio::net::TcpStream::connect(endpoint.trim_start_matches("tcp://"))
            .await
            .expect("connect");
        let ours = weida_zmtp::Greeting {
            mechanism,
            as_server: false,
            ..weida_zmtp::Greeting::null()
        };
        stream.write_all(&ours.encode()).await.expect("write");
        let mut theirs = [0u8; weida_zmtp::GREETING_LEN];
        within(stream.read_exact(&mut theirs))
            .await
            .expect("their greeting");
        let greeting = weida_zmtp::Greeting::decode(&theirs).expect("a greeting");
        assert_eq!(greeting.mechanism, mechanism, "the mechanism it announced");
        assert_eq!(greeting.version.major, 3);
        assert_eq!(greeting.version.minor, 1);
        assert!(
            !greeting.as_server,
            "libzmq now announces as-server 1 for a {mechanism} server; the specifications \
             always said it should, and session.rs's guard can be tightened"
        );
    }
}

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_req_is_answered_by_a_libzmq_rep() {
    let context = context();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let responder = libzmq_socket(&ctx, zmq::REP);
        responder.bind("tcp://127.0.0.1:0").expect("bind");
        let endpoint = responder
            .get_last_endpoint()
            .expect("last endpoint")
            .expect("utf-8");
        endpoint_tx.send(endpoint).expect("the test is listening");
        let request = responder.recv_bytes(0).expect("recv");
        responder.send("pong", 0).expect("reply");
        request
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut requester = ReqSocket::new(&context).expect("req");
    requester.connect(&endpoint).expect("connect");
    within(requester.send(Multipart::single("ping")))
        .await
        .expect("our request");
    let reply = within(requester.recv()).await.expect("our reply");
    assert_eq!(text(&reply), "pong");
    assert_eq!(peer.await.expect("the peer"), b"ping");
}

// ---- DEALER/ROUTER ---------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_router_routes_a_libzmq_dealer() {
    let context = context();
    let mut router = RouterSocket::new(&context).expect("router");
    let bound = within(router.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let dealer = libzmq_socket(&ctx, zmq::DEALER);
        dealer.connect(&endpoint).expect("connect");
        dealer.send("task", 0).expect("send");
        dealer.recv_bytes(0).expect("recv")
    });

    let arrived = within(router.recv()).await.expect("our recv");
    assert_eq!(arrived.len(), 2, "the routing id and the body");
    let routing_id = arrived.frames()[0].clone();
    assert!(!routing_id.is_empty());
    assert_eq!(
        String::from_utf8_lossy(arrived.frames()[1].as_slice()),
        "task"
    );
    let reply = Multipart::new(vec![routing_id, Message::from("done")]).expect("a reply");
    within(router.send(reply)).await.expect("our send");
    assert_eq!(peer.await.expect("the peer"), b"done");
}

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_dealer_talks_to_a_libzmq_router() {
    let context = context();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let router = libzmq_socket(&ctx, zmq::ROUTER);
        router.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                router
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        let arrived = router.recv_multipart(0).expect("recv");
        // libzmq's ROUTER prepends exactly one frame, the routing id it gave
        // this peer, and takes the same frame back.
        assert_eq!(arrived.len(), 2, "one routing id frame and the body");
        let reply = vec![arrived[0].clone(), b"done".to_vec()];
        router.send_multipart(reply, 0).expect("reply");
        arrived[1].clone()
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut dealer = DealerSocket::new(&context).expect("dealer");
    dealer.connect(&endpoint).expect("connect");
    within(dealer.send(Multipart::single("task")))
        .await
        .expect("our send");
    let back = within(dealer.recv()).await.expect("our recv");
    assert_eq!(text(&back), "done");
    assert_eq!(peer.await.expect("the peer"), b"task");
}

// ---- PUSH/PULL -------------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_pull_drains_a_libzmq_push() {
    let context = context();
    let mut puller = PullSocket::new(&context).expect("pull");
    let bound = within(puller.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let pusher = libzmq_socket(&ctx, zmq::PUSH);
        pusher.connect(&endpoint).expect("connect");
        for index in 0..3 {
            pusher
                .send(format!("task {index}").as_bytes(), 0)
                .expect("send");
        }
        // Linger is zero, so the socket must not be dropped before the
        // messages are on the wire; libzmq has no flush, and a receive that
        // never arrives is what this waits out.
        std::thread::sleep(Duration::from_millis(200));
    });

    for index in 0..3 {
        let arrived = within(puller.recv()).await.expect("our recv");
        assert_eq!(text(&arrived), format!("task {index}"), "in order");
    }
}

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_push_feeds_a_libzmq_pull() {
    let context = context();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let puller = libzmq_socket(&ctx, zmq::PULL);
        puller.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                puller
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        (0..3)
            .map(|_| puller.recv_bytes(0).expect("recv"))
            .collect::<Vec<_>>()
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut pusher = PushSocket::new(&context).expect("push");
    pusher.connect(&endpoint).expect("connect");
    for index in 0..3 {
        within(pusher.send(Multipart::single(format!("task {index}"))))
            .await
            .expect("our send");
    }
    let got = peer.await.expect("the peer");
    for (index, task) in got.iter().enumerate() {
        assert_eq!(task, format!("task {index}").as_bytes(), "in order");
    }
}

// ---- PUB/SUB ---------------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_pub_reaches_a_libzmq_sub() {
    let context = context();
    let mut publisher = PubSocket::new(&context).expect("pub");
    let bound = within(publisher.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let subscriber = libzmq_socket(&ctx, zmq::SUB);
        subscriber.connect(&endpoint).expect("connect");
        subscriber.set_subscribe(b"px").expect("subscribe");
        ready_tx.send(()).expect("the test is listening");
        subscriber.recv_bytes(0).expect("recv")
    });

    within(ready_rx).await.expect("the subscriber is up");
    // The subscription may still be in flight: publish until it lands, which
    // is the only honest wait for a subscription.
    let peer = within(async {
        let mut peer = peer;
        loop {
            publisher.publish(Multipart::single("px.eur 1.09"));
            match tokio::time::timeout(Duration::from_millis(50), &mut peer).await {
                Ok(done) => return done,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await;
    assert_eq!(peer.expect("the peer"), b"px.eur 1.09");
}

/// Our SUB with the **default** subscription form — the 3.x `SUBSCRIBE`
/// command — which is what libzmq speaks and zmq.rs 0.6 does not.
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_sub_receives_from_a_libzmq_pub_with_the_command_form() {
    let context = context();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let publisher = libzmq_socket(&ctx, zmq::PUB);
        publisher.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                publisher
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        while stop_rx.try_recv().is_err() {
            publisher.send("px.eur 1.09", 0).expect("publish");
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut subscriber = SubSocket::new(&context).expect("sub");
    subscriber.connect(&endpoint).expect("connect");
    subscriber.subscribe("px").expect("subscribe");
    let arrived = within(subscriber.recv()).await.expect("our recv");
    assert_eq!(text(&arrived), "px.eur 1.09");
    let _ = stop_tx.send(());
}

// ---- XPUB/XSUB -------------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_xpub_serves_a_libzmq_xsub() {
    let context = context();
    let mut publisher = XPubSocket::new(&context).expect("xpub");
    let bound = within(publisher.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let subscriber = libzmq_socket(&ctx, zmq::XSUB);
        subscriber.connect(&endpoint).expect("connect");
        // An XSUB's subscription is a message, `%x01` then the prefix.
        subscriber.send(&b"\x01px"[..], 0).expect("subscribe");
        subscriber.recv_bytes(0).expect("recv")
    });

    let announced = within(publisher.recv()).await.expect("a subscription");
    let frame = announced.frames()[0].as_slice();
    assert_eq!(frame[0], 1, "a subscription, in the API's 1/0 form");
    assert_eq!(&frame[1..], b"px");

    let peer = within(async {
        let mut peer = peer;
        loop {
            publisher.publish(Multipart::single("px.eur 1.09"));
            match tokio::time::timeout(Duration::from_millis(50), &mut peer).await {
                Ok(done) => return done,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await;
    assert_eq!(peer.expect("the peer"), b"px.eur 1.09");
}

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_xsub_subscribes_to_a_libzmq_xpub() {
    let context = context();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let publisher = libzmq_socket(&ctx, zmq::XPUB);
        publisher.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                publisher
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        // libzmq's XPUB applies the subscription in the socket; reading it is
        // the application's business and not a condition of delivery, which
        // is where zmq.rs 0.6 differs.
        let announced = publisher.recv_bytes(0).expect("a subscription");
        assert_eq!(announced[0], 1, "a subscription");
        assert_eq!(&announced[1..], b"px");
        while stop_rx.try_recv().is_err() {
            publisher.send("px.eur 1.09", 0).expect("publish");
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut subscriber = XSubSocket::new(&context).expect("xsub");
    subscriber.connect(&endpoint).expect("connect");
    subscriber.subscribe("px").expect("subscribe");
    let arrived = within(subscriber.recv()).await.expect("our recv");
    assert_eq!(text(&arrived), "px.eur 1.09");
    let _ = stop_tx.send(());
}

// ---- PAIR ------------------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_pair_talks_to_a_libzmq_pair_both_ways() {
    let context = context();
    let mut ours = PairSocket::new(&context).expect("pair");
    let bound = within(ours.bind("tcp://127.0.0.1:0")).await.expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let theirs = libzmq_socket(&ctx, zmq::PAIR);
        theirs.connect(&endpoint).expect("connect");
        theirs.send("from libzmq", 0).expect("send");
        let back = theirs.recv_bytes(0).expect("recv");
        std::thread::sleep(Duration::from_millis(100));
        back
    });

    let arrived = within(ours.recv()).await.expect("our recv");
    assert_eq!(text(&arrived), "from libzmq");
    within(ours.send(Multipart::single("from weida")))
        .await
        .expect("our send");
    assert_eq!(peer.await.expect("the peer"), b"from weida");
}

// ---- PLAIN -----------------------------------------------------------

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_plain_server_authorizes_a_libzmq_client() {
    let context = context();
    let (asked, handler) = zap_handler(&context, |request| {
        ZapReply::allowed(request.request_id, "operator")
    })
    .await;

    let mut responder = RepSocket::with_options(
        &context,
        SocketOptions {
            plain_server: true,
            zap_domain: "interop".to_owned(),
            ..SocketOptions::default()
        },
    )
    .expect("rep");
    let bound = within(responder.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let requester = libzmq_socket(&ctx, zmq::REQ);
        requester
            .set_plain_username(Some("admin"))
            .expect("username");
        requester
            .set_plain_password(Some("secret"))
            .expect("password");
        requester.connect(&endpoint).expect("connect");
        requester.send("ping", 0).expect("send");
        requester.recv_bytes(0).expect("recv")
    });

    let request = within(responder.recv()).await.expect("our recv");
    assert_eq!(text(&request), "ping");
    within(responder.send(Multipart::single("pong")))
        .await
        .expect("our reply");
    assert_eq!(peer.await.expect("the peer"), b"pong");

    let asked = asked.lock().expect("asked");
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].mechanism, "PLAIN");
    assert_eq!(asked[0].domain, "interop");
    assert_eq!(asked[0].address, "127.0.0.1");
    let (username, password) = asked[0]
        .plain_credentials()
        .expect("the PLAIN credential frames");
    assert_eq!(username, b"admin");
    assert_eq!(password, b"secret");
    handler.abort();
}

#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_plain_client_is_admitted_by_a_libzmq_server() {
    let context = context();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        // libzmq refuses every PLAIN connection without a handler, so its
        // context gets one.
        libzmq_zap_handler(&ctx);
        let responder = libzmq_socket(&ctx, zmq::REP);
        responder.set_plain_server(true).expect("plain server");
        responder.set_zap_domain("interop").expect("zap domain");
        responder.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                responder
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        let request = responder.recv_bytes(0).expect("recv");
        responder.send("pong", 0).expect("reply");
        std::thread::sleep(Duration::from_millis(100));
        request
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut requester = ReqSocket::with_options(
        &context,
        SocketOptions {
            plain_username: Some("admin".to_owned()),
            plain_password: Some("secret".to_owned()),
            ..SocketOptions::default()
        },
    )
    .expect("req");
    requester.connect(&endpoint).expect("connect");
    within(requester.send(Multipart::single("ping")))
        .await
        .expect("our request");
    let reply = within(requester.recv()).await.expect("our reply");
    assert_eq!(text(&reply), "pong");
    assert_eq!(peer.await.expect("the peer"), b"ping");
}

// ---- CURVE -----------------------------------------------------------

/// The test the `crypto_box` dependency exists to pass: a libzmq client,
/// which seals its boxes with libsodium, completes 26/CURVEZMQ's handshake
/// with our server and exchanges an encrypted message.
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_curve_server_is_reached_by_a_libzmq_client() {
    let context = context();
    let (server_public, server_secret) = keypair();
    let (client_public, client_secret) = keypair();
    let expected_client = client_public;
    let (asked, handler) = zap_handler(&context, move |request| {
        let key = CurvePublicKey::parse(&request.credentials[0]).expect("a 32-octet key");
        assert_eq!(key, expected_client, "the peer's long-term key");
        ZapReply::allowed(request.request_id, "curve-client")
    })
    .await;

    let mut responder = RepSocket::with_options(
        &context,
        SocketOptions {
            curve_server: true,
            curve_secretkey: Some(server_secret),
            zap_domain: "interop".to_owned(),
            ..SocketOptions::default()
        },
    )
    .expect("rep");
    let bound = within(responder.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let requester = libzmq_socket(&ctx, zmq::REQ);
        requester
            .set_curve_serverkey(server_public.as_bytes())
            .expect("server key");
        requester
            .set_curve_publickey(client_public.as_bytes())
            .expect("public key");
        requester
            .set_curve_secretkey(&secret_key_bytes(&client_secret))
            .expect("secret key");
        requester.connect(&endpoint).expect("connect");
        requester.send("ping", 0).expect("send");
        requester.recv_bytes(0).expect("recv")
    });

    let request = within(responder.recv()).await.expect("our recv");
    assert_eq!(text(&request), "ping");
    within(responder.send(Multipart::single("pong")))
        .await
        .expect("our reply");
    assert_eq!(peer.await.expect("the peer"), b"pong");
    let asked = asked.lock().expect("asked");
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].mechanism, "CURVE");
    assert_eq!(
        asked[0].credentials,
        vec![expected_client.as_bytes().to_vec()],
        "one frame, the client's 32-octet long-term public key"
    );
    handler.abort();
}

/// The other direction: our client's `HELLO`, vouch and `INITIATE` are
/// opened by libsodium inside libzmq.
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_curve_client_reaches_a_libzmq_server() {
    let context = context();
    let (server_public, server_secret) = keypair();
    let (client_public, client_secret) = keypair();
    let server_secret_bytes = secret_key_bytes(&server_secret);

    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        libzmq_zap_handler(&ctx);
        let responder = libzmq_socket(&ctx, zmq::REP);
        responder.set_curve_server(true).expect("curve server");
        responder
            .set_curve_secretkey(&server_secret_bytes)
            .expect("secret key");
        responder.set_zap_domain("interop").expect("zap domain");
        responder.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                responder
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        let request = responder.recv_bytes(0).expect("recv");
        responder.send("pong", 0).expect("reply");
        std::thread::sleep(Duration::from_millis(100));
        request
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut requester = ReqSocket::with_options(
        &context,
        SocketOptions {
            curve_publickey: Some(client_public),
            curve_secretkey: Some(client_secret),
            curve_serverkey: Some(server_public),
            ..SocketOptions::default()
        },
    )
    .expect("req");
    requester.connect(&endpoint).expect("connect");
    within(requester.send(Multipart::single("ping")))
        .await
        .expect("our request");
    let reply = within(requester.recv()).await.expect("our reply");
    assert_eq!(text(&reply), "pong");
    assert_eq!(peer.await.expect("the peer"), b"ping");
}

/// Measured against libzmq 4.3.5: **a CURVE server asks a bound ZAP handler
/// even when `ZMQ_ZAP_DOMAIN` is empty.** The 4.3.5 manual's sentence for
/// that option, "when the ZAP domain is empty, which is the default, ZAP
/// authentication is disabled", holds for NULL and not for CURVE, where the
/// trigger is the handler being bound. Treating an empty domain as "do not
/// authorize" would admit every client of a deployment that bound a handler
/// to allow-list keys and never set a domain, so this library asks too (and
/// stays closed, where libzmq opens, when a domain is set and nothing is
/// bound).
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn libzmq_asks_a_bound_handler_for_curve_without_a_domain() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let context = context();
    let (server_public, server_secret) = keypair();
    let (client_public, client_secret) = keypair();
    let server_secret_bytes = secret_key_bytes(&server_secret);
    let asked = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&asked);

    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let handler = ctx.socket(zmq::REP).expect("a REP socket");
        handler
            .bind("inproc://zeromq.zap.01")
            .expect("bind the ZAP endpoint");
        std::thread::spawn(move || {
            while let Ok(request) = handler.recv_multipart(0) {
                counted.fetch_add(1, Ordering::SeqCst);
                let reply: Vec<Vec<u8>> = vec![
                    b"1.0".to_vec(),
                    request[1].clone(),
                    b"200".to_vec(),
                    b"OK".to_vec(),
                    b"interop".to_vec(),
                    Vec::new(),
                ];
                if handler.send_multipart(reply, 0).is_err() {
                    return;
                }
            }
        });
        let responder = libzmq_socket(&ctx, zmq::REP);
        responder.set_curve_server(true).expect("curve server");
        responder
            .set_curve_secretkey(&server_secret_bytes)
            .expect("secret key");
        // No `set_zap_domain`: the default, empty.
        responder.bind("tcp://127.0.0.1:0").expect("bind");
        endpoint_tx
            .send(
                responder
                    .get_last_endpoint()
                    .expect("last endpoint")
                    .expect("utf-8"),
            )
            .expect("the test is listening");
        let request = responder.recv_bytes(0).expect("recv");
        responder.send("pong", 0).expect("reply");
        std::thread::sleep(Duration::from_millis(100));
        request
    });

    let endpoint = within(endpoint_rx).await.expect("the endpoint");
    let mut requester = ReqSocket::with_options(
        &context,
        SocketOptions {
            curve_publickey: Some(client_public),
            curve_secretkey: Some(client_secret),
            curve_serverkey: Some(server_public),
            ..SocketOptions::default()
        },
    )
    .expect("req");
    requester.connect(&endpoint).expect("connect");
    within(requester.send(Multipart::single("ping")))
        .await
        .expect("our request");
    let reply = within(requester.recv()).await.expect("our reply");
    assert_eq!(text(&reply), "pong");
    assert_eq!(peer.await.expect("the peer"), b"ping");
    assert_eq!(
        asked.load(Ordering::SeqCst),
        1,
        "libzmq asks a bound handler about a CURVE client whatever the domain"
    );
}

// ---- ZAP: the refusal ------------------------------------------------

/// A ZAP refusal closes the connection rather than rejecting a message: the
/// libzmq client gets nothing, and our server never sees a request.
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn a_zap_refusal_stops_a_libzmq_client() {
    let context = context();
    let (asked, handler) = zap_handler(&context, |request| {
        ZapReply::refused(
            request.request_id,
            ZapStatus::AuthenticationFailure,
            "not on the list",
        )
    })
    .await;

    let mut responder = RepSocket::with_options(
        &context,
        SocketOptions {
            plain_server: true,
            zap_domain: "interop".to_owned(),
            ..SocketOptions::default()
        },
    )
    .expect("rep");
    let bound = within(responder.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind");
    let endpoint = bound.to_string();

    let peer = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let requester = ctx.socket(zmq::REQ).expect("req");
        requester.set_linger(0).expect("linger");
        // A short bound: the point is that nothing comes back.
        requester.set_sndtimeo(2_000).expect("sndtimeo");
        requester.set_rcvtimeo(2_000).expect("rcvtimeo");
        requester.set_plain_username(Some("nobody")).expect("user");
        requester.set_plain_password(Some("wrong")).expect("pass");
        requester.connect(&endpoint).expect("connect");
        let _ = requester.send("ping", 0);
        requester.recv_bytes(0).is_err()
    });

    assert!(
        peer.await.expect("the peer"),
        "a refused client received a reply"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), responder.recv())
            .await
            .is_err(),
        "a refused connection delivered a request"
    );
    assert_eq!(asked.lock().expect("asked").len(), 1, "the handler decided");
    handler.abort();
}

// ---- the heartbeat ---------------------------------------------------

/// `PING`/`PONG` are 3.1 commands and libzmq speaks 3.1, so the heartbeat
/// this library suppresses against a 3.0 peer runs here — and the connection
/// survives twenty intervals of it, which it would not if libzmq disliked
/// our `PING` or we disliked its `PONG`.
#[tokio::test]
#[ignore = needs_libzmq!()]
async fn our_heartbeat_is_answered_by_libzmq() {
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
    let endpoint = bound.to_string();

    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let pusher = libzmq_socket(&ctx, zmq::PUSH);
        pusher.connect(&endpoint).expect("connect");
        pusher.send("first", 0).expect("send");
        // Silence over many heartbeat intervals, then prove the connection
        // is the same one.
        std::thread::sleep(Duration::from_secs(1));
        pusher.send("second", 0).expect("send after the silence");
        let _ = stop_rx.recv_timeout(Duration::from_secs(5));
    });

    assert_eq!(text(&within(puller.recv()).await.expect("recv")), "first");
    assert_eq!(text(&within(puller.recv()).await.expect("recv")), "second");
    let _ = stop_tx.send(());
}

/// libzmq wants a CURVE secret key as 32 octets; ours does not hand its
/// bytes out casually, so the Z85 form is the way across — which is also the
/// form `zmq_curve_keypair` prints and a configuration file carries.
fn secret_key_bytes(secret: &CurveSecretKey) -> Vec<u8> {
    weida_zmtp::z85::decode_key(&secret.to_z85())
        .expect("40 characters of Z85")
        .to_vec()
}
