#![cfg(feature = "nng-interop")]
//! Interop against the reference implementation: **NNG 1.4.0-rc.0**,
//! vendored and built from source by `nng-sys` 1.4.0-rc.0 under the `nng`
//! crate 1.0.1, measured on 2026-09-12 on Linux x86-64.
//!
//! This is where the library stops agreeing with itself. `weida-sp`'s
//! golden vectors say what the RFCs say; these tests say what a real NNG
//! node does, in both roles — our socket **dialling** and our socket
//! **listening** — for every pairing both implementations have.
//!
//! # Running them
//!
//! The C library is a **build** condition, not a runtime one: the `nng`
//! crate compiles NNG from vendored sources with `cmake` and a C compiler,
//! so with the feature on and no toolchain present nothing links. The
//! dependency is therefore optional and the tests are `#[ignore]`d on top
//! of it:
//!
//! ```text
//! # needs: cmake, a C compiler
//! cargo test -p weida-nng --features nng-interop -- --ignored
//! ```
//!
//! # The matrix
//!
//! REQ/REP, PUSH/PULL, PUB/SUB, PAIR v0, PAIR v1, SURVEYOR/RESPONDENT and
//! BUS, each with our socket on both ends of the pairing in turn; plus the
//! raw device, forwarding a cooked NNG request through two of our raw
//! sockets to a cooked NNG replier and back.
//!
//! # The two things only an implementation can settle
//!
//! Both are settled here by measurement; the research sheet leaves them open
//! (`docs/research/nanomsg-nng.md` §11).
//!
//! 1. **The endpoint-type role nibble.** The SP RFCs assign the 12-bit
//!    protocol ids and delegate the 4-bit roles to the per-protocol RFCs,
//!    which never published them. `the_role_nibble_is_nngs_registry` reads
//!    the eight octets NNG puts on a plain TCP connection for every
//!    protocol and compares them to what `weida_sp::ProtocolHeader` encodes.
//!    They agree, for all eleven — which is also why every other test in
//!    this file gets past the handshake at all.
//!
//! 2. **PAIR v1's initial hop count.** The RFC says the counter starts at
//!    one; a reading of NNG's source suggested zero, and the sheet
//!    recorded both without choosing.
//!    `nngs_pair1_originates_the_rfcs_hop_count` reads the 32-bit word off
//!    a cooked NNG PAIR v1 send through one of our raw sockets.
//!    **Measured: one.** The RFC was right and the source reading was
//!    wrong, or true of another version.
//!
//! # Disagreements
//!
//! **One, and it is settled on our side.** `weida_sp::pair::INITIAL_HOPS` is
//! `1`: a real NNG 1.4.0-rc.0 sends `1` where a reading of NNG's source
//! suggested `0`, and the constant, the codec's golden vector 18 and this
//! crate's PAIR v1 vector all say one. Nothing about interoperability turns
//! on it — a receiver compares the count to its own `MAXTTL` and nothing
//! else, so both readings are accepted in both directions — but the
//! documentation must state the number NNG sends, which is the thing this
//! file exists to check.
//!
//! Otherwise none, at this version: every pairing interoperates in both
//! roles with no adjustment on either side. Two divergences this library
//! chose deliberately are *not* visible from here and are named in
//! `docs/libraries/nng.md` instead, because they are about what happens
//! when something goes wrong rather than about the wire: an oversized
//! declared length closes the pipe where NNG discards the message and
//! keeps it, and `NNG_OPT_RECVMAXSZ` defaults to a megabyte where NNG's
//! default is unlimited.
//!
//! # Keeping the blocking calls off the reactor
//!
//! Every `nng` crate call is blocking, `Socket::recv` included, so each one
//! runs on [`tokio::task::spawn_blocking`]. No socket, ours or NNG's, carries
//! a `RecvTimeout` or `SendTimeout` here: a stall is reported by the waits of
//! the test and ended by the CI job's budget, and no option fails a slow run
//! [nanomsg-nng §13].

use std::time::Duration;

use nng::options::Options;
use nng::options::protocol::{pubsub, survey};
use nng::{Protocol, Socket};
use weida_nng::{
    BusSocket, Context, ContextConfig, Message, Pair0Socket, Pair1Socket, PubSocket, PullSocket,
    PushSocket, RawSocket, RepSocket, ReqSocket, RespondentSocket, SocketOptions, SubSocket,
    SurveyorSocket, raw,
};
use weida_sp::{EndpointType, HEADER_LEN, ProtocolHeader, backtrace};

/// How long a wait goes on before it reports itself. A report, never a failure.
const PATIENCE: Duration = Duration::from_secs(5);

fn nng_cooked(protocol: Protocol) -> Socket {
    // NNG's receive and send timeouts default to forever, which is what a
    // slow run needs.
    Socket::new(protocol).expect("an NNG socket")
}

/// Runs one blocking `nng` call off the reactor's threads.
async fn off_reactor<T: Send + 'static>(what: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(what)
        .await
        .expect("the blocking task")
}

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: None,
        send_timeout: None,
        handshake_timeout: Duration::from_secs(3600),
        reconnect_min: Duration::from_millis(10),
        resend_time: Duration::from_secs(30),
        survey_time: Duration::from_secs(2),
        ..SocketOptions::default()
    }
}

/// A free loopback address, as a URL both implementations parse.
async fn free_url() -> String {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);
    format!("tcp://127.0.0.1:{port}")
}

/// Claim: the 4-bit endpoint role NNG puts on the wire is the registry
/// `weida_sp` encodes, for every protocol both implementations have.
///
/// This is the open question of §11 settled by measurement: the RFCs never
/// published the role nibble, so until an NNG node was asked, the values
/// were NNG's source read by a human.
#[tokio::test]
#[ignore = "builds the vendored NNG C library: cargo test -p weida-nng --features nng-interop -- --ignored"]
async fn the_role_nibble_is_nngs_registry() {
    for (protocol, ours) in [
        (Protocol::Pair0, EndpointType::PairV0),
        (Protocol::Pair1, EndpointType::PairV1),
        (Protocol::Pub0, EndpointType::Pub),
        (Protocol::Sub0, EndpointType::Sub),
        (Protocol::Req0, EndpointType::Req),
        (Protocol::Rep0, EndpointType::Rep),
        (Protocol::Push0, EndpointType::Push),
        (Protocol::Pull0, EndpointType::Pull),
        (Protocol::Surveyor0, EndpointType::Surveyor),
        (Protocol::Respondent0, EndpointType::Respondent),
        (Protocol::Bus0, EndpointType::Bus),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let url = format!(
            "tcp://127.0.0.1:{}",
            listener.local_addr().expect("addr").port()
        );

        // `nng_dial` is synchronous until the peer's header arrives, so the
        // dial and the accept have to overlap.
        let dialling = tokio::task::spawn_blocking(move || {
            let socket = nng_cooked(protocol);
            let outcome = socket.dial(&url);
            // Hold the socket until the header has been read.
            std::thread::sleep(Duration::from_millis(50));
            outcome
        });

        let (mut stream, _) = listener.accept().await.expect("accept");
        // Our header first, so a peer that waits for one is not deadlocked.
        let peer_of_ours = ours.peer();
        tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            &ProtocolHeader::new(peer_of_ours).encode(),
        )
        .await
        .expect("write");
        let mut theirs = [0u8; HEADER_LEN];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut theirs)
            .await
            .expect("their header");
        assert_eq!(
            theirs,
            ProtocolHeader::new(ours).encode(),
            "NNG's {protocol:?} header is not what weida-sp encodes for {ours:?}"
        );
        let _ = dialling.await.expect("task");
    }
}

/// Claim: PAIR v1's initial hop count, as NNG actually sends it.
///
/// The sheet records two readings — the RFC's one and a source reading's
/// zero — and says only an implementation can settle it. **Measured:
/// one**, which is what this library now sends.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn nngs_pair1_originates_the_rfcs_hop_count() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let ours = RawSocket::with_options(&ctx, EndpointType::PairV1, options()).expect("raw pair1");
    let url = ours
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();

    let sent = off_reactor(move || {
        let peer = nng_cooked(Protocol::Pair1);
        peer.dial(&url).expect("dial");
        peer.send(nng::Message::from(&b"hop"[..])).expect("send");
        // Keep the socket alive until the message has left.
        std::thread::sleep(Duration::from_millis(100));
    });
    let (_, message) = tokio::join!(sent, ours.recv()).1.expect("the raw message");

    assert_eq!(
        &message.body()[..4],
        [0, 0, 0, 1],
        "NNG originates the hop count the RFC describes, not the zero its source suggested"
    );
    assert_eq!(&message.body()[4..], b"hop");
    assert_eq!(weida_sp::pair::INITIAL_HOPS, 1, "and we send the same");
}

/// Claim: REQ and REP interoperate in both roles — our requester against
/// NNG's replier, and NNG's requester against our replier — with the tag
/// stack, the terminal bit and the reply matching agreeing on both sides.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn req_and_rep_interoperate_in_both_roles() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // Ours dials, NNG listens.
    let url = free_url().await;
    let serving = {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            let rep = nng_cooked(Protocol::Rep0);
            rep.listen(&url).expect("listen");
            let request = rep.recv().expect("a request");
            assert_eq!(&request[..], b"ping");
            let mut reply = nng::Message::new();
            reply.push_back(b"pong");
            rep.send(reply).expect("reply");
        })
    };
    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    for _ in 0..50 {
        if req.dial_nonblocking(&url).is_ok() {
            break;
        }
    }
    while req.pipe_count() == 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    req.send(b"ping".to_vec()).await.expect("send");
    assert_eq!(req.recv().await.expect("reply").body(), b"pong");
    serving.await.expect("task");

    // Ours listens, NNG dials.
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let url = rep
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();
    let asking = tokio::task::spawn_blocking(move || {
        let peer = nng_cooked(Protocol::Req0);
        peer.dial(&url).expect("dial");
        peer.send(nng::Message::from(&b"who"[..])).expect("send");
        let reply = peer.recv().expect("a reply");
        assert_eq!(&reply[..], b"us");
    });
    let request = rep.recv().await.expect("a request");
    assert_eq!(request.body(), b"who");
    assert_eq!(request.header().len() % 4, 0, "a tag stack of whole words");
    assert_eq!(
        request.header()[0] & 0x80,
        0x80,
        "NNG sets the terminal bit on the request ID"
    );
    rep.send(b"us".to_vec()).await.expect("reply");
    asking.await.expect("task");
}

/// Claim: PUSH and PULL interoperate in both roles.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn push_and_pull_interoperate_in_both_roles() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // Ours pushes into NNG's puller.
    let url = free_url().await;
    let pulling = {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            let pull = nng_cooked(Protocol::Pull0);
            pull.listen(&url).expect("listen");
            let work = pull.recv().expect("work");
            assert_eq!(&work[..], b"job");
        })
    };
    let push = PushSocket::with_options(&ctx, options()).expect("push");
    for _ in 0..50 {
        if push.dial_nonblocking(&url).is_ok() {
            break;
        }
    }
    while push.pipe_count() == 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    push.send(b"job".to_vec()).await.expect("send");
    pulling.await.expect("task");

    // NNG pushes into ours.
    let pull = PullSocket::with_options(&ctx, options()).expect("pull");
    let url = pull
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();
    let pushing = tokio::task::spawn_blocking(move || {
        let peer = nng_cooked(Protocol::Push0);
        peer.dial(&url).expect("dial");
        peer.send(nng::Message::from(&b"theirs"[..])).expect("send");
        std::thread::sleep(Duration::from_millis(100));
    });
    assert_eq!(pull.recv().await.expect("work").body(), b"theirs");
    pushing.await.expect("task");
}

/// Claim: PUB and SUB interoperate in both roles, and the subscription is
/// the subscriber's business on both sides — an NNG SUB filters our
/// publications locally, and our SUB filters NNG's.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn pub_and_sub_interoperate_in_both_roles() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // Ours publishes, NNG subscribes to one prefix.
    let publisher = PubSocket::with_options(&ctx, options()).expect("pub");
    let url = publisher
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();
    let subscribing = tokio::task::spawn_blocking(move || {
        let sub = nng_cooked(Protocol::Sub0);
        sub.dial(&url).expect("dial");
        sub.set_opt::<pubsub::Subscribe>(b"weather.".to_vec())
            .expect("subscribe");
        let seen = sub.recv().expect("a publication");
        assert_eq!(&seen[..], b"weather.rain");
    });
    // Publish until the subscriber has arrived and taken one: PUB is
    // best-effort, so a publication before the pipe exists is lost, which
    // is the pattern rather than a fault.
    for _ in 0..200 {
        let _ = publisher.send(b"sports.result".to_vec());
        let _ = publisher.send(b"weather.rain".to_vec());
        if subscribing.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    subscribing.await.expect("task");

    // NNG publishes, ours subscribes.
    let subscriber = SubSocket::with_options(&ctx, options()).expect("sub");
    subscriber.subscribe(b"weather.".to_vec());
    let url = free_url().await;
    let publishing = {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            let publisher = nng_cooked(Protocol::Pub0);
            publisher.listen(&url).expect("listen");
            for _ in 0..200 {
                let _ = publisher.send(nng::Message::from(&b"sports.fixture"[..]));
                let _ = publisher.send(nng::Message::from(&b"weather.sun"[..]));
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };
    for _ in 0..50 {
        if subscriber.dial_nonblocking(&url).is_ok() {
            break;
        }
    }
    let admitted = subscriber.recv().await.expect("a publication");
    assert_eq!(admitted.body(), b"weather.sun");
    assert!(
        subscriber.discarded() > 0,
        "the unsubscribed publications crossed the link and were dropped here, which is where \
         SP puts the filter"
    );
    publishing.abort();
}

/// Claim: PAIR v0 interoperates in both directions on one pairing, which
/// is also the claim that v0 has no protocol header on either side — a
/// header would be read as payload and the assertion would fail.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn pair_v0_interoperates_in_both_directions() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let ours = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    let url = ours
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();

    let peering = tokio::task::spawn_blocking(move || {
        let peer = nng_cooked(Protocol::Pair0);
        peer.dial(&url).expect("dial");
        peer.send(nng::Message::from(&b"theirs"[..])).expect("send");
        let back = peer.recv().expect("ours");
        assert_eq!(&back[..], b"ours");
    });
    assert_eq!(ours.recv().await.expect("recv").body(), b"theirs");
    ours.send(b"ours".to_vec()).await.expect("send");
    peering.await.expect("task");
}

/// Claim: PAIR v1 interoperates in both directions, and the hop-count
/// word agrees: what NNG originates arrives here as one, and what we
/// originate is accepted there.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn pair_v1_interoperates_in_both_directions() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let ours = Pair1Socket::with_options(&ctx, options()).expect("pair1");
    let url = ours
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();

    let peering = tokio::task::spawn_blocking(move || {
        let peer = nng_cooked(Protocol::Pair1);
        peer.dial(&url).expect("dial");
        peer.send(nng::Message::from(&b"theirs"[..])).expect("send");
        let back = peer.recv().expect("ours");
        assert_eq!(&back[..], b"ours");
    });
    let arrived = ours.recv().await.expect("recv");
    assert_eq!(arrived.body(), b"theirs");
    assert_eq!(
        arrived.header(),
        [0, 0, 0, 1],
        "an NNG-originated PAIR v1 message arrives with the RFC's hop count of one"
    );
    ours.send(b"ours".to_vec()).await.expect("send");
    peering.await.expect("task");
}

/// Claim: SURVEYOR and RESPONDENT interoperate in both roles, survey ids
/// and all.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn survey_interoperates_in_both_roles() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // Ours surveys, NNG responds.
    let surveyor = SurveyorSocket::with_options(&ctx, options()).expect("surveyor");
    let url = surveyor
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();
    let responding = tokio::task::spawn_blocking(move || {
        let peer = nng_cooked(Protocol::Respondent0);
        peer.dial(&url).expect("dial");
        let survey = peer.recv().expect("a survey");
        assert_eq!(&survey[..], b"who is there");
        let mut answer = nng::Message::new();
        answer.push_back(b"nng");
        peer.send(answer).expect("answer");
    });
    weida_testkit::until("the NNG respondent connecting", PATIENCE, || {
        surveyor.pipe_count() > 0
    })
    .await;
    surveyor
        .send(b"who is there".to_vec())
        .await
        .expect("survey");
    assert_eq!(surveyor.recv().await.expect("an answer").body(), b"nng");
    responding.await.expect("task");

    // NNG surveys, ours responds.
    let respondent = RespondentSocket::with_options(&ctx, options()).expect("respondent");
    let url = free_url().await;
    let surveying = {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            let peer = nng_cooked(Protocol::Surveyor0);
            // A survey sent before the respondent's pipe exists reaches
            // nobody, which is the pattern, so NNG surveys again until one
            // answers. The survey time is what paces that loop; our
            // respondent has no receive timeout to race it.
            peer.set_opt::<survey::SurveyTime>(Some(Duration::from_millis(250)))
                .expect("SurveyTime");
            peer.listen(&url).expect("listen");
            // Reported rather than failed when it goes on: the answer comes
            // from our respondent, whose own wait is what the test asserts.
            let mut answer = None;
            weida_testkit::poll_until("a respondent answering a survey", 2 * PATIENCE, || {
                peer.send(nng::Message::from(&b"anybody"[..]))
                    .expect("send");
                answer = peer.recv().ok();
                answer.is_some()
            });
            assert_eq!(&answer.expect("an answer")[..], b"here");
        })
    };
    for _ in 0..50 {
        if respondent.dial_nonblocking(&url).is_ok() {
            break;
        }
    }
    let survey = respondent.recv().await.expect("a survey");
    assert_eq!(survey.body(), b"anybody");
    respondent.send(b"here".to_vec()).await.expect("answer");
    surveying.await.expect("task");
}

/// Claim: BUS interoperates in both roles, one hop and best effort on both
/// sides.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn bus_interoperates_in_both_roles() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let ours = BusSocket::with_options(&ctx, options()).expect("bus");
    let url = ours
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();

    let peering = tokio::task::spawn_blocking(move || {
        let peer = nng_cooked(Protocol::Bus0);
        peer.dial(&url).expect("dial");
        // Send until ours has the pipe: BUS drops rather than waiting.
        for _ in 0..200 {
            let _ = peer.send(nng::Message::from(&b"theirs"[..]));
            std::thread::sleep(Duration::from_millis(20));
            if let Ok(back) = peer.recv() {
                assert_eq!(&back[..], b"ours");
                return;
            }
        }
        panic!("the NNG bus peer never saw our message");
    });

    let arrived = ours.recv().await.expect("recv");
    assert_eq!(arrived.body(), b"theirs");
    for _ in 0..200 {
        let sent = ours.send(b"ours".to_vec()).expect("send");
        if sent.queued > 0 && peering.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    peering.await.expect("task");
}

/// Claim: the device forwards a cooked NNG request through two of our raw
/// sockets to a cooked NNG replier and the reply finds its way back, which
/// means the peer id our forwarder pushed was popped again and the tag
/// stack NNG builds is the one our device manipulates.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn the_device_forwards_between_two_nng_sockets() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let front = RawSocket::with_options(&ctx, EndpointType::Rep, options()).expect("raw rep");
    let back = RawSocket::with_options(&ctx, EndpointType::Req, options()).expect("raw req");
    let front_url = front
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();
    let back_url = back
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();

    let forwarding = {
        let front = front.clone();
        let back = back.clone();
        tokio::spawn(async move { raw::device(&front, &back).await })
    };

    let serving = tokio::task::spawn_blocking(move || {
        let rep = nng_cooked(Protocol::Rep0);
        rep.dial(&back_url).expect("dial the device");
        let request = rep.recv().expect("a request");
        assert_eq!(&request[..], b"through");
        let mut reply = nng::Message::new();
        reply.push_back(b"and back");
        rep.send(reply).expect("reply");
    });

    let asking = tokio::task::spawn_blocking(move || {
        let req = nng_cooked(Protocol::Req0);
        req.dial(&front_url).expect("dial the device");
        req.send(nng::Message::from(&b"through"[..])).expect("send");
        let reply = req.recv().expect("a reply");
        assert_eq!(&reply[..], b"and back");
    });

    serving.await.expect("the replier");
    asking.await.expect("the requester");
    forwarding.abort();
}

/// Claim: what our REQ puts on the wire is a tag stack an NNG replier
/// accepts and repeats, which is the same claim the codec's golden vectors
/// make — measured here against the implementation rather than the RFC.
#[tokio::test]
#[ignore = "builds the vendored NNG C library"]
async fn the_tag_stack_survives_a_round_trip_through_nng() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let url = rep
        .listen(&free_url().await)
        .await
        .expect("listen")
        .url()
        .to_string();
    let asking = tokio::task::spawn_blocking(move || {
        let req = nng_cooked(Protocol::Req0);
        req.dial(&url).expect("dial");
        req.send(nng::Message::from(&b"tags"[..])).expect("send");
        let reply = req.recv().expect("a reply");
        assert_eq!(&reply[..], b"tags");
    });

    let request = rep.recv().await.expect("a request");
    let (stack, payload) =
        backtrace::decode(request.header(), 8).expect("NNG's stack decodes under our codec");
    assert!(
        payload.is_empty(),
        "the header is the stack and nothing else"
    );
    assert!(
        stack.peers.is_empty(),
        "a direct request carries no peer ids"
    );
    assert!(stack.id <= backtrace::MAX_ID);
    rep.send(Message::from_body(request.body().to_vec()).into_body())
        .await
        .expect("reply");
    asking.await.expect("task");
}
