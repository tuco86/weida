//! The blocking facade, from threads with no executor.
//!
//! These are plain `#[test]` functions: no `#[tokio::test]` anywhere, which
//! is the point — a synchronous caller has no reactor, and everything below
//! runs on the reactor `weida::blocking::Runtime` owns and never shows.
//!
//! One round trip per pattern, because what needs proving is that the facade
//! drives the asynchronous surface correctly, not that the patterns work: they
//! have their own suites. The fourth test is the one that earns its place
//! twice over — calling a blocking method from inside a Tokio runtime is the
//! one mistake that deadlocks instead of failing, and the facade refuses it.
#![cfg(all(feature = "blocking", feature = "generate"))]

mod common;

use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use common::Certs;
use quinn::rustls::pki_types::CertificateDer;
use quinn::rustls::pki_types::pem::PemObject;
use weida::blocking::Runtime;
use weida::{
    Acknowledgement, CursorLevel, Error, Identity, ReportMode, Reported, RuntimeConfig,
    TransferMeta, Trust,
};
use weida_protocol::{DataHeader, FrameKind, encode_frame};

/// A payload ceiling for every receive: the facade has no default, on purpose.
const CAP: usize = 1024 * 1024;

/// Binds a server on loopback and returns the runtime, the binding and the
/// address a client dials.
fn served(path: &str) -> (Runtime, weida::blocking::Binding, String) {
    let runtime = Runtime::new(RuntimeConfig::default()).expect("an owned reactor");
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let binding = runtime
        .bind_quic("127.0.0.1:0".parse().expect("addr"), identity)
        .expect("bind");
    let url = format!(
        "weida://{fingerprint}@{}{path}",
        binding.local_addr().expect("a QUIC binding")
    );
    (runtime, binding, url)
}

#[test]
fn a_request_and_its_reply_cross_between_two_threads() {
    let (server, binding, url) = served("/echo");
    let replier = binding.replier("/echo").expect("replier");

    // The replier runs on its own thread, blocking on `accept`, which is how
    // a synchronous server is written.
    let answering = std::thread::spawn(move || {
        let request = replier.accept(CAP).expect("accept");
        assert_eq!(request.message().payload, b"ping");
        // The metadata is the asynchronous surface's, passed through: this
        // client presented no identity, so the peer is `None` — which is the
        // documented answer and not a facade artefact.
        assert!(request.message().meta.peer.is_none());
        assert_eq!(request.message().meta.endpoint.as_deref(), Some("/echo"));
        request.reply(b"pong").expect("reply");
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let requester = client.requester(Trust::by_address());
    requester.connect(&url).expect("connect");
    assert_eq!(requester.request(b"ping", CAP).expect("request"), b"pong");

    answering.join().expect("the replier thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

#[test]
fn a_push_reaches_a_puller_and_the_receipt_comes_back() {
    let (server, binding, url) = served("/ingest");
    let puller = binding.puller("/ingest").expect("puller");

    let draining = std::thread::spawn(move || {
        let first = puller.recv(CAP).expect("recv");
        assert_eq!(first.payload, b"one");
        let second = puller.recv(CAP).expect("recv");
        assert_eq!(second.payload, b"two");
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&url).expect("connect");
    // `send` returns when the peer's transport holds the bytes, which is what
    // a synchronous caller has to be able to check.
    pusher.send(b"one").expect("send");
    pusher.send(b"two").expect("send");

    draining.join().expect("the puller thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

#[test]
fn a_published_message_reaches_a_blocking_subscriber() {
    let (server, binding, url) = served("/md");
    let publisher = binding.publisher("/md").expect("publisher");

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
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

/// PAIR over the facade, and the one rule a caller gets wrong: the **first**
/// peer is kept and a second is refused.
#[test]
fn a_pair_carries_both_directions_and_refuses_a_second_peer() {
    let (server, binding, url) = served("/link");
    let bound = binding.pair("/link").expect("bound pair");

    // The bound half runs on its own thread: a pair is symmetric, so both
    // ends block on `recv` and answer. It is **handed back** rather than
    // dropped: dropping a bound pair unregisters its route, so a first peer
    // that "still works" has to have something to talk to.
    let answering = std::thread::spawn(move || {
        let first = bound.recv(CAP).expect("recv");
        assert_eq!(first.payload, b"ping");
        bound.send(b"pong").expect("send back");
        bound
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let paired = client.pair(Trust::by_address());
    paired.connect(&url).expect("connect");
    paired.send(b"ping").expect("send");
    assert_eq!(paired.recv(CAP).expect("recv").payload, b"pong");
    let bound = answering.join().expect("the bound thread");

    // A second peer: the connection is accepted — the refusal is per stream —
    // and the transfer is refused. Past the peer's 1 MiB stream window, so the
    // refusal cannot lose the race to the receipt
    // (`docs/decisions/0005-refusal-race.md`).
    let newcomer = Runtime::new(RuntimeConfig::default()).expect("second client runtime");
    let second = newcomer.pair(Trust::by_address());
    second
        .connect(&url)
        .expect("the refusal is per stream, not per connection");
    let refused = second
        .send(&vec![0x7au8; 2 * 1024 * 1024])
        .expect_err("a second peer is refused");
    assert!(
        matches!(refused, Error::LimitExceeded),
        "a capacity decision said out loud, got {refused:?}"
    );

    // And the first peer still **delivers**, which is the whole rule: ZeroMQ's
    // PAIR would have dropped it for the newcomer.
    paired.send(b"still mine").expect("the first peer is kept");
    assert_eq!(bound.recv(CAP).expect("recv").payload, b"still mine");

    newcomer.shutdown().expect("second client shutdown");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

/// SURVEY over the facade, and the one rule a caller gets wrong: the
/// **deadline** is the caller's and silence is a number.
#[test]
fn a_survey_collects_what_answers_and_counts_what_does_not() {
    let (server, binding, url) = served("/poll");
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

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
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

/// BUS over the facade, and the one rule a caller gets wrong: **never your
/// own message**.
#[test]
fn a_bus_message_reaches_every_other_member_and_never_the_sender() {
    let (first_rt, first_binding, first_url) = served("/bus1");
    let (second_rt, second_binding, second_url) = served("/bus2");
    let one = first_binding
        .bus("/bus1", Trust::by_address())
        .expect("first member");
    let two = second_binding
        .bus("/bus2", Trust::by_address())
        .expect("second member");
    one.connect(&second_url).expect("one dials two");
    two.connect(&first_url).expect("two dials one");

    let listening = std::thread::spawn(move || {
        let heard = two.recv(CAP).expect("recv");
        assert_eq!(heard.payload, b"hello all");
        two.send(b"and back").expect("send back");
        two
    });

    assert_eq!(one.send(b"hello all").expect("send"), 1, "one other member");
    assert_eq!(one.recv(CAP).expect("recv").payload, b"and back");

    // The sender never hears itself: with the exchange above complete, a
    // further send leaves nothing for this member to receive.
    let two = listening.join().expect("the second member's thread");
    one.send(b"mine alone").expect("send");
    assert_eq!(two.recv(CAP).expect("recv").payload, b"mine alone");
    drop(two);

    first_rt.shutdown().expect("first shutdown");
    second_rt.shutdown().expect("second shutdown");
}

/// The mistake that would otherwise be a hang: blocking a reactor worker.
#[test]
fn the_facade_refuses_to_block_a_reactor_thread() {
    let reactor = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime to be inside of");
    let refused = reactor.block_on(async { Runtime::new(RuntimeConfig::default()) });
    match refused {
        Err(Error::Runtime(message)) => {
            assert!(
                message.contains("deadlock"),
                "the refusal must say why: {message}"
            );
        }
        Ok(_) => panic!("a blocking runtime built inside a reactor would deadlock on first use"),
        Err(other) => panic!("{other:?}"),
    }

    // And the same for a call on an endpoint built outside: it is the *call*
    // that blocks, so the check belongs on every entry point rather than only
    // on the constructor.
    let outside = Runtime::new(RuntimeConfig::default()).expect("outside a reactor");
    let requester = outside.requester(Trust::by_address());
    let refused = reactor.block_on(async { requester.connect("weida://127.0.0.1:1/x") });
    assert!(
        matches!(refused, Err(Error::Runtime(_))),
        "{refused:?} must be refused rather than deadlock"
    );
    outside.shutdown().expect("shutdown");
}

/// The cursor surface, synchronously: a fire-and-forget producer that gets a
/// **verdict** without an exchange, and a staged receiver that gives it.
#[test]
fn a_push_gets_a_verdict_from_a_cursor_with_no_exchange() {
    const ACCEPTED: CursorLevel = CursorLevel::Known(Acknowledgement::Accepted);
    const PROCESSED: CursorLevel = CursorLevel::Known(Acknowledgement::Processed);

    let (server, binding, url) = served("/work");
    let puller = binding.puller("/work").expect("puller");

    let receiving = std::thread::spawn(move || {
        let (message, reporter) = puller.recv_reporting(CAP).expect("recv");
        assert_eq!(message.payload, b"a unit of work");
        // The order is in the metadata the DATA header carried, which is what
        // makes a report the sender's request rather than this side's idea.
        assert_eq!(message.meta.report, vec![ACCEPTED, PROCESSED]);
        assert_eq!(message.meta.report_mode, ReportMode::Progress);
        assert!(message.meta.report_id.is_some());

        let mut reporter = reporter.expect("the sender ordered a report");
        assert_eq!(reporter.levels(), [ACCEPTED, PROCESSED]);
        reporter
            .report(ACCEPTED, message.payload.len() as u64)
            .expect("accepted");
        // A level nobody ordered is ignored rather than refused: the order
        // says what the sender wants to hear.
        reporter
            .report(CursorLevel::Known(Acknowledgement::Stored), 1)
            .expect("ignored");
        reporter
            .report(PROCESSED, message.payload.len() as u64)
            .expect("processed");
        reporter.finish().expect("finish the report");
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&url).expect("connect");
    let body = b"a unit of work";
    let mut cursors = pusher
        .send_reporting(
            TransferMeta::default()
                .with_content_len(body.len() as u64)
                .with_report([PROCESSED, ACCEPTED]),
            body,
        )
        .expect("send")
        .expect("the metadata ordered a report");

    // The verdict arrives **after** the transport receipt `send_reporting`
    // already waited for, which is the whole point of a cursor: `Processed` is
    // not something a FIN can carry. The wait is bounded, because a
    // synchronous one has to be: a receiver that died would otherwise park
    // this thread on a connection nobody closes.
    let mut processed = None;
    while processed.is_none() {
        match cursors.changed(Duration::from_secs(15)).expect("a cursor") {
            Reported::Changed => {
                processed = cursors.snapshot().offset(PROCESSED);
            }
            Reported::Waiting => panic!("no cursor within the deadline"),
            Reported::Ended => panic!("the report ended without a verdict"),
        }
    }
    assert_eq!(processed, Some(body.len() as u64));
    assert_eq!(
        cursors.snapshot().offset(ACCEPTED),
        Some(body.len() as u64),
        "a cursor is absolute, so the earlier level is still readable"
    );
    // The level nobody ordered never arrived.
    assert_eq!(
        cursors
            .snapshot()
            .offset(CursorLevel::Known(Acknowledgement::Stored)),
        None
    );

    receiving.join().expect("the receiving thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

/// A send that orders nothing has nothing to read, and a receiver of it has
/// nothing to report with.
#[test]
fn a_transfer_that_orders_no_report_hands_out_no_handles() {
    let (server, binding, url) = served("/plain");
    let puller = binding.puller("/plain").expect("puller");

    let receiving = std::thread::spawn(move || {
        let (message, reporter) = puller.recv_reporting(CAP).expect("recv");
        assert_eq!(message.payload, b"no report");
        assert!(message.meta.report.is_empty());
        assert!(message.meta.report_id.is_none());
        assert!(
            reporter.is_none(),
            "a reporter with nothing ordered would be a handle that writes to nobody"
        );
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&url).expect("connect");
    assert!(
        pusher
            .send_reporting(TransferMeta::default(), b"no report")
            .expect("send")
            .is_none(),
        "nothing was ordered, so there is nothing to read"
    );

    receiving.join().expect("the receiving thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

/// A raw client that trusts `cert_pem` and keeps its connection alive with
/// QUIC PINGs, so that only the receive deadline can end a stall: the idle
/// timeout never fires for a peer that is never silent.
fn pinging_client(cert_pem: &std::path::Path) -> quinn::Endpoint {
    let mut roots = quinn::rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(cert_pem).expect("read the certificate") {
        roots
            .add(cert.expect("parse the certificate"))
            .expect("add the root");
    }
    let provider = Arc::new(quinn::rustls::crypto::ring::default_provider());
    let mut crypto = quinn::rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .expect("tls 1.3")
        .with_root_certificates(Arc::new(roots))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_millis(100)));
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).expect("quic crypto"),
    ));
    config.transport_config(Arc::new(transport));
    let mut endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().expect("loopback")).expect("client endpoint");
    endpoint.set_default_client_config(config);
    endpoint
}

/// The receive deadline, end to end. A peer sends a DATA header and one byte
/// of its payload, then stalls with its connection kept alive. The receive
/// must give up within `receive_timeout`, and the transfer after it, from a
/// second client, must still arrive.
#[test]
fn a_stalled_body_expires_and_the_next_message_still_arrives() {
    let certs = Certs::generate();
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
    let peer_cert = certs.cert_pem.clone();
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
    let anchor = certs.cert_pem.clone();
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
