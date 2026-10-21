//! The tests of `tests/blocking.rs` that need the network, a process or real time.
#![cfg(all(feature = "blocking", feature = "generate"))]

use crate::common;
use common::Certs;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use weida::blocking::Runtime;
use weida::{Error, Identity, Network, RuntimeConfig, SimNet, Trust};
use weida_protocol::{DataHeader, FrameKind, encode_frame};

use crate::blocking::*;

#[test]
fn a_published_message_reaches_a_blocking_subscriber() {
    let net = SimNet::new();
    let (server, binding, url) = served(&net, "/md");
    let publisher = binding.publisher("/md").expect("publisher");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let subscriber = client.subscriber(Trust::by_address());
    subscriber.connect(&url).expect("connect");
    subscriber.subscribe("px.#").expect("subscribe");

    // Publishing never waits for a subscriber, so the publisher may run
    // before the subscription has arrived: retry until it has, exactly as the
    // asynchronous Pub/Sub tests do, because a sleep would only be a guess.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the subscription never reached the publisher"
        );
        if publisher
            .publish("px.eur", &b"1.0812"[..])
            .expect("publish")
            > 0
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let message = subscriber.recv(CAP).expect("recv");
    assert_eq!(message.payload, b"1.0812");
    assert_eq!(message.meta.topic.as_deref(), Some("px.eur"));
    // A filter that does not match gets nothing, which is the one claim a
    // facade could break by dropping the filter on the way down.
    assert_eq!(publisher.publish("fx.chf", &b"x"[..]).expect("publish"), 0);

    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}
/// RADIO/DISH over the facade: one stream segment and one datagram segment,
/// each whole, and neither call on the radio blocks (0034 §4.6).
#[test]
fn a_radio_segment_and_datagram_reach_a_blocking_dish() {
    let flows = RuntimeConfig {
        limits: weida::Limits {
            datagram_receive_bytes: weida::DEFAULT_DATAGRAM_RECEIVE_BYTES,
            ..weida::Limits::default()
        },
        ..RuntimeConfig::default()
    };
    let server = Runtime::new(flows.clone()).expect("an owned reactor");
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let binding = server
        .bind_quic("127.0.0.1:0".parse().expect("addr"), identity)
        .expect("bind");
    let radio = binding.radio("/r").expect("radio");

    let client = Runtime::new(flows).expect("client runtime");
    let dish = client.dish(Trust::by_address());
    dish.join("v", weida::JoinTerms::default()).expect("join");
    dish.connect(&format!(
        "weida://{fingerprint}@{}/r",
        binding.local_addr().expect("a QUIC binding")
    ))
    .expect("connect");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while radio.dish_count() < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the join never arrived"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let mut segment = radio
        .segment("v", weida::SegmentTerms::default())
        .expect("segment");
    assert_eq!(segment.write(b"key").expect("write"), 1);
    assert_eq!(segment.write(b"frame").expect("write"), 1);
    assert_eq!(segment.finish(), 1);
    match dish.recv(CAP).expect("recv") {
        weida::blocking::Delivered::Segment(message) => {
            assert_eq!(message.payload, b"keyframe");
            assert_eq!(message.meta.segment, Some(0));
        }
        other => panic!("expected a stream segment, got {other:?}"),
    }

    assert_eq!(radio.datagram("v", b"voice").expect("datagram"), 1);
    match dish.recv(CAP).expect("recv") {
        weida::blocking::Delivered::Datagram {
            topic,
            segment,
            payload,
        } => {
            assert_eq!(topic, "v");
            assert_eq!(segment, 1);
            assert_eq!(payload, b"voice");
        }
        other => panic!("expected a datagram segment, got {other:?}"),
    }

    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}
/// SURVEY over the facade, and the one rule a caller gets wrong: the
/// **deadline** is the caller's and silence is a number.
#[test]
fn a_survey_collects_what_answers_and_counts_what_does_not() {
    let net = SimNet::new();
    let (server, binding, url) = served(&net, "/poll");
    let quiet_url = url.replace("/poll", "/quiet");
    let answering = binding.respondent("/poll").expect("respondent");
    let silent = binding.respondent("/quiet").expect("a second respondent");

    let answers = std::thread::spawn(move || {
        let question = answering.accept(CAP).expect("accept");
        assert_eq!(question.message().payload, b"who is there");
        question.reply(b"me").expect("reply");
    });
    // Accepts the question and never answers it: not unreachable, not
    // refusing, just silent — which is what a deadline exists for.
    let holding = std::thread::spawn(move || {
        let question = silent.accept(CAP).expect("accept");
        std::thread::sleep(Duration::from_secs(3));
        drop(question);
    });

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let surveyor = client.surveyor(Trust::by_address());
    surveyor.connect(&url).expect("connect the answering one");
    surveyor
        .connect(&quiet_url)
        .expect("connect the silent one");

    let survey = surveyor
        .survey(b"who is there", Duration::from_millis(500), CAP)
        .expect("the survey ran");
    assert_eq!(survey.asked, 2, "both respondents were asked: {survey:?}");
    assert_eq!(
        survey.replies,
        vec![b"me".to_vec()],
        "one answered: {survey:?}"
    );
    assert_eq!(
        survey.silent(),
        1,
        "and the other's silence is a number, not an error: {survey:?}"
    );

    answers.join().expect("the answering thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
    holding.join().expect("the silent thread");
}
/// The receive deadline, end to end. A peer sends a DATA header and one byte
/// of its payload, then stalls with its connection kept alive. The receive
/// must give up within `receive_timeout`, and the transfer after it, from a
/// second client, must still arrive.
#[test]
fn a_stalled_body_expires_and_the_next_message_still_arrives() {
    let certs = Certs::generate();
    let files = certs.files();
    let server = Runtime::new(RuntimeConfig {
        receive_timeout: Some(Duration::from_millis(300)),
        ..RuntimeConfig::default()
    })
    .expect("server runtime");
    let binding = server
        .bind_quic("127.0.0.1:0".parse().expect("addr"), certs.identity())
        .expect("bind");
    let addr = binding.local_addr().expect("a QUIC binding");
    let puller = binding.puller("/in").expect("puller");

    // The stalling peer runs on a reactor of its own, on its own thread.
    let (stalled_tx, stalled_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let peer_cert = files.cert_pem.clone();
    let peer = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the peer's reactor")
            .block_on(async move {
                let endpoint = pinging_client(&peer_cert);
                // Bounded, so that a handshake that never completes fails the
                // test instead of hanging it.
                let (conn, stream) = tokio::time::timeout(Duration::from_secs(10), async {
                    let conn = endpoint
                        .connect(addr, "127.0.0.1")
                        .expect("connect")
                        .await
                        .expect("handshake");
                    common::raw::send_hello(&conn).await;
                    let mut stream = conn.open_uni().await.expect("open uni");
                    let header = DataHeader::addressed("/in").encode();
                    stream
                        .write_all(&encode_frame(FrameKind::Data, &header))
                        .await
                        .expect("write the header");
                    stream.write_all(&[7]).await.expect("write one byte");
                    (conn, stream)
                })
                .await
                .expect("the stalling peer set up in time");
                stalled_tx.send(()).expect("the test is waiting");
                // Stalled until the test releases it. A test that panics drops
                // the sender, which releases the peer just the same.
                let _ = release_rx.await;
                drop(stream);
                conn.close(0u32.into(), b"done");
            });
    });
    stalled_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the stalling peer sent its header");
    let started = Instant::now();

    // The receiver is a synchronous consumer on its own thread: a first
    // receive that must give up, then one that must return the next message.
    let (results_tx, results_rx) = mpsc::channel();
    let receiving = std::thread::spawn(move || {
        let first = puller.recv(CAP).map(|message| message.payload);
        results_tx.send(first).expect("the test is waiting");
        let second = puller.recv(CAP).map(|message| message.payload);
        results_tx.send(second).expect("the test is waiting");
    });

    // The second client sends its message while the first receive is still
    // waiting on the stalled body.
    let url = format!("weida://127.0.0.1:{}/in", addr.port());
    let anchor = files.cert_pem.clone();
    let sender = std::thread::spawn(move || {
        // The header of the stalled transfer was read before this client
        // connects, so its transfer is the one the puller takes first.
        std::thread::sleep(Duration::from_millis(200));
        let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
        let pusher = client.pusher(Trust::anchor_file(&anchor));
        pusher.connect(&url).expect("connect");
        pusher.send(b"after the stall").expect("send");
        drop(pusher);
        client.shutdown().expect("client shutdown");
    });

    let first = results_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the first receive returned");
    let elapsed = started.elapsed();
    assert!(
        matches!(first, Err(Error::Expired)),
        "a stalled body must expire, got {first:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the expiry took {elapsed:?}"
    );

    let second = results_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the next receive returned");
    assert_eq!(
        second.expect("the next message arrives"),
        b"after the stall"
    );

    let _ = release_tx.send(());
    peer.join().expect("the stalling peer");
    sender.join().expect("the second client");
    receiving.join().expect("the receiving thread");
    server.shutdown().expect("server shutdown");
}
