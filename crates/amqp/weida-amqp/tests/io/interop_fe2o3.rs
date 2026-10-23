//! Interop against `fe2o3-amqp` 0.17, in both roles.
//!
//! The peer that always runs. `fe2o3-amqp` is a pure-Rust AMQP 1.0
//! implementation with an `acceptor` module, so a foreign peer is one
//! dev-dependency away and nothing here is `#[ignore]`d: no broker to install,
//! no C toolchain, no supervisor. What it buys is the only check that matters
//! for a codec written from a specification — that an implementation nobody
//! here wrote accepts what this client emits, and that this client accepts
//! what it emits back.
//!
//! RabbitMQ 4.x is the second foreign peer and lives in
//! `tests/interop_rabbitmq.rs`, `#[ignore]`d because the broker is a system
//! package.

use std::time::Duration;

use fe2o3_amqp::acceptor::{
    ConnectionAcceptor, LinkAcceptor, LinkEndpoint, SessionAcceptor, SupportedReceiverSettleModes,
};
use tokio::net::TcpListener;
use weida_amqp::link::LinkOptions;
use weida_amqp::session::SessionOptions;
use weida_amqp::{Connection, ConnectionOptions, Outcome, Source, Target, TerminusExpiryPolicy};
use weida_amqp_codec::message::{Body, Message};
use weida_amqp_codec::{ReceiverSettleMode, SenderSettleMode};
use weida_runtime::Exec;

const DEADLINE: Duration = Duration::from_secs(10);

/// A bounded wait that cannot fail a test for being slow: it has the shape of
/// `tokio::time::timeout(..)`, but past `expected` the step is reported by
/// `weida_testkit::within` and the wait goes on.
#[track_caller]
fn timeout<F: Future>(
    expected: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, std::convert::Infallible>> {
    let at = std::panic::Location::caller();
    async move {
        let what = format!("the step at {}:{}", at.file(), at.line());
        Ok(weida_testkit::within(&what, expected, future).await)
    }
}

/// The peer's version, so that a measurement in `docs/libraries/amqp.md` names
/// what it was measured against.
const PEER: &str = "fe2o3-amqp 0.18.4";

fn options() -> ConnectionOptions {
    let mut options = ConnectionOptions::new("weida-amqp-interop");
    // The peer's acceptor sends no `idle-time-out`, and ours would otherwise
    // start a clock nothing resets.
    options.idle_time_out = None;
    options.handshake_timeout = Duration::from_secs(5);
    options.close_budget = Duration::from_secs(5);
    options
}

async fn bound() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

async fn connect(exec: &Exec, port: u16) -> Connection {
    timeout(
        DEADLINE,
        Connection::connect(exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("the peer answered the handshake")
    .expect("the handshake succeeded")
}

#[tokio::test]
async fn our_sender_is_read_by_a_fe2o3_receiver() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;

    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let endpoint = LinkAcceptor::new().accept(&mut session).await.unwrap();
        let mut receiver = match endpoint {
            LinkEndpoint::Receiver(receiver) => receiver,
            LinkEndpoint::Sender(_) => panic!("our sender should attach as its receiver"),
        };

        // What the foreign implementation made of our `transfer` and our
        // message sections.
        let delivery: fe2o3_amqp::link::delivery::Delivery<String> = receiver.recv().await.unwrap();
        assert_eq!(delivery.body(), "from weida-amqp");
        receiver.accept(&delivery).await.unwrap();

        // Draining the rest of the connection is the peer's half of an
        // orderly close.
        let _ = receiver.close().await;
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("interop-sender", Target::at("q1"))),
    )
    .await
    .unwrap()
    .expect("the peer answered our attach");

    let message = Message::value(weida_amqp_codec::Value::String("from weida-amqp"));
    let sent = timeout(DEADLINE, sender.send(&message))
        .await
        .expect("credit arrived from the peer")
        .expect("the message went out");

    // `fe2o3-amqp`'s acceptor grants credit by itself
    // (`CreditMode::Auto`), which is why this send completes without the
    // test granting anything — and the disposition is the peer's answer.
    let outcome = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .expect("the peer settled")
        .expect("an outcome");
    assert_eq!(outcome, Outcome::Accepted, "{PEER} accepted the message");

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn our_receiver_reads_a_fe2o3_sender() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;

    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let endpoint = LinkAcceptor::new().accept(&mut session).await.unwrap();
        let mut sender = match endpoint {
            LinkEndpoint::Sender(sender) => sender,
            LinkEndpoint::Receiver(_) => panic!("our receiver should attach as its sender"),
        };

        // Blocks until our credit arrives, which is the credit scheme working
        // across two implementations.
        let outcome = sender.send("from fe2o3").await.unwrap();
        assert!(
            outcome.is_accepted(),
            "our disposition reached it as accepted: {outcome:?}"
        );
        let _ = sender.close().await;
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("interop-receiver", Source::at("q1"))),
    )
    .await
    .unwrap()
    .expect("the peer answered our attach");

    timeout(DEADLINE, receiver.grant_credit(4))
        .await
        .unwrap()
        .unwrap();
    let delivery = timeout(DEADLINE, receiver.next_delivery())
        .await
        .expect("the peer sent under our credit")
        .expect("a delivery");
    let message = delivery.message().expect("the sections decoded");
    assert_eq!(
        message.body,
        Body::Value(weida_amqp_codec::Value::String("from fe2o3")),
        "{PEER} encodes an amqp-value body of one string"
    );
    timeout(DEADLINE, receiver.accept(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_message_split_across_frames_arrives_whole_at_the_peer() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;

    let peer = tokio::spawn(async move {
        // A small ceiling on the peer's side is what forces our splitting:
        // the number a sender splits on is the *peer's* `max-frame-size`.
        let acceptor = ConnectionAcceptor::builder()
            .container_id("fe2o3-listener")
            .max_frame_size(1024)
            .build();
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let endpoint = LinkAcceptor::new().accept(&mut session).await.unwrap();
        let mut receiver = match endpoint {
            LinkEndpoint::Receiver(receiver) => receiver,
            LinkEndpoint::Sender(_) => panic!("expected a receiver"),
        };
        // `Body<Binary>` rather than `Binary`: our payload is a **data**
        // section, and asking this peer for a bare `Binary` asks it for an
        // `amqp-value` of binary instead — a different descriptor, which is
        // itself worth knowing about the peer's deserialiser.
        let delivery: fe2o3_amqp::link::delivery::Delivery<
            fe2o3_amqp::types::messaging::Body<fe2o3_amqp::types::primitives::Binary>,
        > = receiver.recv().await.unwrap();
        let data = delivery
            .body()
            .try_as_data()
            .expect("a data section")
            .next()
            .expect("one data section");
        assert_eq!(data.len(), 8192, "reassembled from several frames");
        assert!(data.iter().all(|octet| *octet == b'z'));
        receiver.accept(&delivery).await.unwrap();

        let _ = receiver.close().await;
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("interop-large", Target::at("q1"))),
    )
    .await
    .unwrap()
    .unwrap();

    let sent = timeout(DEADLINE, sender.send(&Message::data(&[b'z'; 8192])))
        .await
        .unwrap()
        .unwrap();
    assert!(
        sent.frames > 1,
        "8 KiB through a 1 KiB frame is more than one transfer, got {}",
        sent.frames
    );
    // Waiting for the outcome before closing is not politeness: `close` is
    // the last frame ever written, so a client that closed here would be
    // asserting that the frames left rather than that the peer read them.
    let outcome = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .expect("the peer answered")
        .expect("an outcome");
    assert_eq!(outcome, Outcome::Accepted);

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn rcv_settle_mode_second_is_accepted_by_this_peer_and_refused_when_it_says_so() {
    let exec = Exec::current().unwrap();

    // Probe one: an acceptor that supports both modes. The sheet's claim is
    // that implementations refuse `second`; this one does not, and the
    // measurement is the answering `attach`.
    let (listener, port) = bound().await;
    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let link_acceptor = LinkAcceptor::builder()
            .supported_receiver_settle_modes(SupportedReceiverSettleModes::Both)
            .build();
        let endpoint = link_acceptor.accept(&mut session).await.unwrap();
        drop(endpoint);
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut asked = LinkOptions::sender("second-probe", Target::at("q1"));
    asked.snd_settle_mode = SenderSettleMode::Unsettled;
    asked.rcv_settle_mode = ReceiverSettleMode::Second;
    let link = timeout(DEADLINE, session.attach(asked))
        .await
        .unwrap()
        .expect("the attach was answered");
    assert_eq!(
        link.negotiated().unwrap().rcv_settle_mode,
        ReceiverSettleMode::Second,
        "{PEER} with SupportedReceiverSettleModes::Both honours `second`"
    );
    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, peer).await.unwrap().unwrap();

    // Probe two: an acceptor configured to support `first` only, which is the
    // shape the sheet describes. The refusal is what a client has to read,
    // and reading it is the whole reason the answering `attach` is data.
    let (listener, port) = bound().await;
    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let link_acceptor = LinkAcceptor::builder()
            .supported_receiver_settle_modes(SupportedReceiverSettleModes::First)
            .build();
        // The acceptor's own answer to an unsupported mode; whatever it is,
        // the client below records what reached it.
        let accepted = link_acceptor.accept(&mut session).await;
        let _ = accepted;
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut asked = LinkOptions::sender("second-refused", Target::at("q1"));
    asked.snd_settle_mode = SenderSettleMode::Unsettled;
    asked.rcv_settle_mode = ReceiverSettleMode::Second;
    let refused = timeout(DEADLINE, session.attach(asked))
        .await
        .expect("the peer answered rather than hanging");
    match refused {
        // Either shape is a refusal a client can act on, and which one it is
        // goes in the parity table rather than being asserted away: the
        // specification lets a peer answer with the mode it will honour and
        // then detach, or refuse the attach outright.
        Ok(link) => assert_ne!(
            link.negotiated().unwrap().rcv_settle_mode,
            ReceiverSettleMode::Second,
            "{PEER} restricted to `first` must not claim to honour `second`"
        ),
        Err(error) => {
            let text = error.to_string();
            assert!(
                !text.is_empty(),
                "a refusal a caller cannot read is not a refusal"
            );
        }
    }
    let _ = timeout(DEADLINE, connection.close()).await;
    let _ = timeout(DEADLINE, peer).await;
}

#[tokio::test]
async fn the_terminus_expiry_policy_we_ask_for_comes_back_in_the_answer() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;

    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let endpoint = LinkAcceptor::new().accept(&mut session).await.unwrap();
        drop(endpoint);
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut source = Source::at("q1");
    source.expiry_policy = TerminusExpiryPolicy::Never;
    source.timeout = 600;
    let mut asked = LinkOptions::receiver("expiry-probe", source);
    asked.max_message_size = Some(64 * 1024);
    let link = timeout(DEADLINE, session.attach(asked))
        .await
        .unwrap()
        .expect("the attach was answered");

    // The measurement. A terminus is created by the *peer*, and the answering
    // `attach` reports what it actually created — "MAY adjust properties but
    // MUST then report what it actually created". Whatever this peer chose,
    // the client reads it rather than assuming its own request survived.
    let negotiated = link.negotiated().unwrap();
    let source = negotiated.remote_source;
    assert!(
        source.is_some(),
        "{PEER} provided a source rather than refusing the terminus"
    );
    let reported = source.unwrap().expiry_policy;
    assert_eq!(
        reported,
        TerminusExpiryPolicy::Never,
        "{PEER} echoed the expiry policy asked for rather than substituting its own"
    );

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    let _ = timeout(DEADLINE, peer).await;
}

#[tokio::test]
async fn a_reattach_after_the_peer_let_go_is_a_new_link_and_not_a_resumption() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;

    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        // The peer lets go of each link before the next attach, which is what
        // makes the second one a fresh attach rather than a duplicate name.
        for _ in 0..2 {
            match LinkAcceptor::new().accept(&mut session).await.unwrap() {
                LinkEndpoint::Receiver(receiver) => {
                    let _ = receiver.close().await;
                }
                LinkEndpoint::Sender(sender) => {
                    let _ = sender.close().await;
                }
            }
        }
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut first = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("resume-probe", Target::at("q1"))),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        first.unsettled().is_empty(),
        "nothing unsettled, so nothing a resumption would carry"
    );
    // The peer closes it; waiting for that is what makes the next attach
    // sequential rather than racing.
    let _ = timeout(DEADLINE, first.next_delivery()).await;
    let _ = timeout(DEADLINE, first.detach()).await;

    // The measurement: this client's second `attach` of the same name
    // carries **no `unsettled` map**, which is exactly what distinguishes a
    // re-attach from a *resuming* attach (Part 2 §2.6.13). So the peer gets
    // a new link, nothing is recovered, and this client claims no link
    // recovery.
    let again = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("resume-probe", Target::at("q1"))),
    )
    .await
    .unwrap()
    .expect("a re-attach of a name the peer has let go is a new link");
    assert!(again.unsettled().is_empty(), "and it resumed nothing");

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    let _ = timeout(DEADLINE, peer).await;
}

#[tokio::test]
async fn a_duplicate_link_name_gets_the_peers_answer() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;
    // The peer answers the second attach, reports how, and tears down only
    // once the client has its own answer: a peer that closed right away
    // would make every client outcome look like a lost connection.
    let (answered_tx, answered) = tokio::sync::oneshot::channel();
    let (done, done_rx) = tokio::sync::oneshot::channel::<()>();
    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        // The first endpoint is *held*, so the name is still in use when the
        // second attach arrives.
        let held = LinkAcceptor::new().accept(&mut session).await.unwrap();
        let second = LinkAcceptor::new().accept(&mut session).await;
        let _ = answered_tx.send(second.as_ref().err().map(|error| error.to_string()));
        let _ = done_rx.await;
        drop(second);
        match held {
            LinkEndpoint::Receiver(receiver) => {
                let _ = receiver.close().await;
            }
            LinkEndpoint::Sender(sender) => {
                let _ = sender.close().await;
            }
        }
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let _first = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("stolen-probe", Target::at("q1"))),
    )
    .await
    .unwrap()
    .unwrap();

    // Our own second attach steals locally — that is Part 2 §2.6.1 and this
    // client implements it: it detaches the first link before it attaches
    // the name again. What the *peer* then does is the peer's: fe2o3-amqp
    // 0.17 refused the second attach of a name it still held, 0.18 lets the
    // detach free the name and accepts the new attach. Either way the client
    // must report the peer's answer rather than hang or invent one.
    let second = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("stolen-probe", Target::at("q1"))),
    )
    .await
    .expect("the peer answered rather than hanging");
    let refusal = timeout(DEADLINE, answered).await.unwrap().unwrap();
    eprintln!(
        "{PEER}: refusal {refusal:?}, our attach {:?}",
        second.as_ref().map(|_| "attached")
    );
    match refusal {
        Some(reason) => {
            assert!(!reason.is_empty(), "{PEER} says why it refuses");
            assert!(
                second.is_err(),
                "the peer's refusal has to reach the caller as an error"
            );
        }
        None => assert!(
            second.is_ok(),
            "the peer accepted the name again, so the caller holds the new link"
        ),
    }
    let _ = done.send(());
    let _ = timeout(DEADLINE, peer).await;
    let _ = timeout(DEADLINE, connection.close()).await;
}

#[tokio::test]
async fn a_transactional_attach_is_not_something_this_client_can_ask_for() {
    let exec = Exec::current().unwrap();
    let (listener, port) = bound().await;

    let peer = tokio::spawn(async move {
        let acceptor = ConnectionAcceptor::new("fe2o3-listener");
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = acceptor.accept(stream).await.unwrap();
        let mut session = SessionAcceptor::new()
            .accept(&mut connection)
            .await
            .unwrap();
        let endpoint = LinkAcceptor::new().accept(&mut session).await.unwrap();
        drop(endpoint);
        let _ = session.end().await;
        let _ = connection.close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    // The measurement, and it is structural rather than behavioural: Part 4's
    // transactional target is a `coordinator`, a *different described type*
    // from `target`, and this client's `LinkOptions::target` is typed as a
    // `Target`. There is no value of this API that attaches to a coordinator,
    // so "transactions" is absent-by-construction here and not a runtime
    // refusal — which is what the parity table records.
    //
    // What the probe does establish is the other half: the capability is not
    // silently claimed either. A transaction-capable peer advertises one of
    // Part 4's five coordinator capabilities in its `attach`
    // offered-capabilities, and this client reads them rather than ignoring
    // them. Their names are `amqp:local-transactions` and its four
    // relatives - not an `amqp:txn:` prefix, which is what the *state* and
    // the error conditions use (Part 4 §4.5.1, §4.5.5).
    const COORDINATOR: [&str; 5] = [
        "amqp:local-transactions",
        "amqp:distributed-transactions",
        "amqp:promotable-transactions",
        "amqp:multi-txns-per-ssn",
        "amqp:multi-ssns-per-txn",
    ];
    let link = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("txn-probe", Target::at("q1"))),
    )
    .await
    .unwrap()
    .unwrap();
    let offered = link.negotiated().unwrap().remote_offered_capabilities;
    assert!(
        !offered
            .iter()
            .any(|capability| COORDINATOR.contains(&capability.as_str())),
        "{PEER} built without its `transaction` feature offers none of Part 4's \
         coordinator capabilities, and this client offers none either: {offered:?}"
    );

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    let _ = timeout(DEADLINE, peer).await;
}
