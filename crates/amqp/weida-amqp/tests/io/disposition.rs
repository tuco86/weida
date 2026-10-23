//! Dispositions, settlement and the unsettled map, against a scripted server.
//!
//! Every test here asserts what the exchange **proves to an application** —
//! the message will not come back, the message is available again, the peer
//! knows that this end knows — rather than that a frame arrived. That is the
//! only reason the settle modes exist: they are three different answers to
//! "what may I conclude now?", and a client that reported frames instead of
//! conclusions would leave the application to guess.

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use support::{DEADLINE, Server, accept, options, scripted, timeout};
use weida_amqp::link::{LinkEvent, LinkOptions};
use weida_amqp::session::SessionOptions;
use weida_amqp::{Condition, Connection, Outcome, Source, Target};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Message;
use weida_amqp_codec::performative::{
    Attach, Begin, Close, Disposition, Flow, Open, Performative, Transfer as TransferFrame,
};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::state::DeliveryState;
use weida_amqp_codec::types::{AmqpError, condition};
use weida_amqp_codec::{Limits, ReceiverSettleMode, Role, SenderSettleMode};
use weida_runtime::Exec;

/// Long enough that a frame the client meant to send would have arrived.
const QUIET: Duration = Duration::from_millis(150);

/// What a test looks at in a `transfer` it read.
struct TransferSeen {
    delivery_id: Option<u32>,
    settled: Option<bool>,
}

/// What a test looks at in a `disposition` it read.
///
/// The state is kept as an [`Outcome`], which is owned: the frame's buffer
/// dies at the end of the read and every assertion here outlives it.
struct DispositionSeen {
    role: Role,
    first: u32,
    last: Option<u32>,
    settled: bool,
    outcome: Option<Outcome>,
}

impl Server {
    async fn read_transfer(&mut self) -> TransferSeen {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Transfer(transfer) => TransferSeen {
                delivery_id: transfer.delivery_id,
                settled: transfer.settled,
            },
            other => panic!("expected transfer, got {}", other.name()),
        }
    }

    async fn read_disposition(&mut self) -> DispositionSeen {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Disposition(disposition) => DispositionSeen {
                role: disposition.role,
                first: disposition.first,
                last: disposition.last,
                settled: disposition.settled,
                outcome: disposition.state.and_then(|value| {
                    let state = DeliveryState::from_value(value).unwrap();
                    Outcome::of(&state).unwrap()
                }),
            },
            other => panic!("expected disposition, got {}", other.name()),
        }
    }

    async fn send_transfer(&mut self, channel: u16, transfer: TransferFrame<'_>, payload: &[u8]) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, channel, u32::MAX, |body| {
            Performative::Transfer(transfer).encode(body)?;
            body.extend_from_slice(payload);
            Ok(())
        })
        .unwrap();
        self.write(&out).await;
    }

    /// A `disposition` covering `first..=last`.
    async fn send_disposition(
        &mut self,
        channel: u16,
        role: Role,
        first: u32,
        last: Option<u32>,
        settled: bool,
        state: Option<&DeliveryState<'_>>,
    ) {
        let mut disposition = Disposition::new(role, first);
        disposition.last = last;
        disposition.settled = settled;
        disposition.state = state.map(DeliveryState::to_value);
        self.send(channel, Performative::Disposition(disposition))
            .await;
    }

    /// Credit for `credit` messages on `handle`.
    async fn send_flow(&mut self, channel: u16, handle: u32, delivery_count: u32, credit: u32) {
        let flow = Flow {
            next_incoming_id: Some(0),
            incoming_window: 400,
            next_outgoing_id: 0,
            outgoing_window: 400,
            handle: Some(handle),
            delivery_count: Some(delivery_count),
            link_credit: Some(credit),
            available: Some(0),
            drain: false,
            echo: false,
            properties: None,
        };
        self.send(channel, Performative::Flow(flow)).await;
    }

    async fn handshake(&mut self) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "open");
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "begin");
        let mut begin = Begin::new(0, 400, 400);
        begin.remote_channel = Some(0);
        begin.handle_max = 255;
        self.send(4, Performative::Begin(begin)).await;
    }

    /// The answering `attach`, in the role opposite the client's, with the
    /// settle modes the broker is imposing.
    async fn answer_attach(
        &mut self,
        name: &str,
        handle: u32,
        role: Role,
        snd: SenderSettleMode,
        rcv: ReceiverSettleMode,
    ) {
        let (_, seen, _) = self.read_named().await;
        assert_eq!(seen, "attach");
        let source = Source::default();
        let target = Target::at("q");
        let mut attach = Attach::new(name, handle, role);
        attach.snd_settle_mode = snd;
        attach.rcv_settle_mode = rcv;
        attach.source = Some(source.to_value());
        attach.target = Some(target.to_value());
        if role == Role::Sender {
            attach.initial_delivery_count = Some(0);
        }
        self.send(4, Performative::Attach(attach)).await;
    }

    /// Reads until the client's `close` and answers it, asserting that no
    /// `disposition` passes on the way.
    async fn expect_close_without_disposition(&mut self) {
        loop {
            let (_, name, _) = self.read_named().await;
            assert_ne!(name, "disposition", "nothing left to settle");
            if name == "close" {
                break;
            }
        }
        self.send(0, Performative::Close(Close { error: None }))
            .await;
    }

    async fn expect_close(&mut self) {
        loop {
            let (_, name, _) = self.read_named().await;
            if name == "close" {
                break;
            }
        }
        self.send(0, Performative::Close(Close { error: None }))
            .await;
    }
}

async fn connect(exec: &Exec, port: u16) -> Connection {
    timeout(
        DEADLINE,
        Connection::connect(exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn settled_on_send_leaves_no_state_and_proves_nothing() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "orders",
                11,
                Role::Receiver,
                SenderSettleMode::Settled,
                ReceiverSettleMode::First,
            )
            .await;
        server.send_flow(4, 11, 0, 1).await;

        let transfer = server.read_transfer().await;
        assert_eq!(
            transfer.settled,
            Some(true),
            "settled on send: the sender has already forgotten it"
        );
        assert_eq!(transfer.delivery_id, Some(0));

        // No disposition is expected, wanted, or answerable.
        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = LinkOptions::sender("orders", Target::at("q"));
    link.snd_settle_mode = SenderSettleMode::Settled;
    let mut sender = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();

    let sent = timeout(DEADLINE, sender.send(&Message::data(b"order-1")))
        .await
        .unwrap()
        .unwrap();
    assert!(sent.settled);
    assert!(
        sender.unsettled().is_empty(),
        "a settled delivery is recorded nowhere, because there is nothing to answer"
    );
    // And the application cannot wait for an answer that is not coming: this
    // is the whole cost of the mode.
    let error = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .unwrap()
        .expect_err("nothing to wait for");
    assert!(error.to_string().contains("settled on send"), "{error}");

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

/// One scripted exchange: the client sends one message unsettled, the server
/// answers with `state` and settles, and the test gets the outcome the
/// application saw.
async fn outcome_of(exec: &Exec, state: DeliveryState<'static>) -> (Outcome, usize) {
    let (listener, port) = scripted().await;
    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "orders",
                11,
                Role::Receiver,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::First,
            )
            .await;
        server.send_flow(4, 11, 0, 1).await;
        let transfer = server.read_transfer().await;
        assert_eq!(
            transfer.settled,
            Some(false),
            "unsettled: the answer is what the sender is waiting for"
        );
        let id = transfer.delivery_id.unwrap();
        // `first` alone, settled: one frame, and the delivery is over at both
        // ends when it lands.
        server
            .send_disposition(4, Role::Receiver, id, None, true, Some(&state))
            .await;
        server.expect_close().await;
    });

    let connection = connect(exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = LinkOptions::sender("orders", Target::at("q"));
    link.snd_settle_mode = SenderSettleMode::Unsettled;
    let mut sender = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();
    let sent = timeout(DEADLINE, sender.send(&Message::data(b"order-1")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sender.unsettled().len(),
        1,
        "unsettled here until the receiver answers"
    );
    let outcome = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .unwrap()
        .unwrap();
    let left = sender.unsettled().len();
    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
    (outcome, left)
}

#[tokio::test]
async fn accepted_means_the_message_is_gone_and_counts_against_nothing() {
    let exec = Exec::current().unwrap();
    let (outcome, left) = outcome_of(&exec, DeliveryState::Accepted).await;
    assert_eq!(outcome, Outcome::Accepted);
    assert!(
        !outcome.may_be_redelivered(),
        "the receiver processed it and the sender may retire it at the source"
    );
    assert!(!outcome.increments_delivery_count());
    assert_eq!(left, 0, "settled at both ends, so forgotten here");
}

#[tokio::test]
async fn rejected_means_it_will_not_come_back_and_the_attempt_is_counted() {
    let exec = Exec::current().unwrap();
    let (outcome, left) = outcome_of(
        &exec,
        DeliveryState::Rejected {
            error: Some(AmqpError {
                condition: condition::DECODE_ERROR,
                description: Some("the body is not a JSON object"),
                info: None,
            }),
        },
    )
    .await;
    match &outcome {
        Outcome::Rejected { error } => {
            let error = error.as_ref().expect("the broker said why");
            assert_eq!(error.condition, condition::DECODE_ERROR);
            assert_eq!(
                error.description.as_deref(),
                Some("the body is not a JSON object"),
                "a rejection without a reason leaves nothing to log"
            );
        }
        other => panic!("expected rejected, got {other:?}"),
    }
    assert!(!outcome.may_be_redelivered(), "invalid and unprocessable");
    assert!(
        outcome.increments_delivery_count(),
        "which is what a redelivery limit counts"
    );
    assert_eq!(left, 0);
}

#[tokio::test]
async fn released_means_the_message_is_available_again_and_uncounted() {
    let exec = Exec::current().unwrap();
    let (outcome, left) = outcome_of(&exec, DeliveryState::Released).await;
    assert_eq!(outcome, Outcome::Released);
    assert!(outcome.may_be_redelivered(), "not acted upon");
    assert!(
        !outcome.increments_delivery_count(),
        "a released message is indistinguishable from one never delivered"
    );
    assert_eq!(left, 0);
}

#[tokio::test]
async fn modified_carries_what_the_peer_recorded_about_the_attempt() {
    let exec = Exec::current().unwrap();
    let (outcome, left) = outcome_of(
        &exec,
        DeliveryState::Modified {
            delivery_failed: Some(true),
            undeliverable_here: Some(true),
            message_annotations: None,
        },
    )
    .await;
    match &outcome {
        Outcome::Modified {
            delivery_failed,
            undeliverable_here,
            ..
        } => {
            assert_eq!(*delivery_failed, Some(true));
            assert_eq!(*undeliverable_here, Some(true));
        }
        other => panic!("expected modified, got {other:?}"),
    }
    assert!(outcome.may_be_redelivered(), "available again");
    assert!(
        outcome.increments_delivery_count(),
        "delivery-failed=true is what makes this attempt count"
    );
    assert_eq!(left, 0);
}

#[tokio::test]
async fn one_disposition_settles_every_delivery_in_its_range() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "orders",
                11,
                Role::Receiver,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::First,
            )
            .await;
        server.send_flow(4, 11, 0, 3).await;
        for expected in 0..3u32 {
            let transfer = server.read_transfer().await;
            assert_eq!(transfer.delivery_id, Some(expected));
        }
        // One frame for three deliveries, which is the point of the range.
        server
            .send_disposition(
                4,
                Role::Receiver,
                0,
                Some(2),
                true,
                Some(&DeliveryState::Accepted),
            )
            .await;
        server.expect_close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = LinkOptions::sender("orders", Target::at("q"));
    link.snd_settle_mode = SenderSettleMode::Unsettled;
    let mut sender = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();

    for n in 0..3u32 {
        let sent = timeout(DEADLINE, sender.send(&Message::data(b"order")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sent.delivery_id, n);
    }
    assert_eq!(sender.unsettled().len(), 3);

    // Every one of the three is answered by the single frame.
    let mut settled = Vec::new();
    while settled.len() < 3 {
        match timeout(DEADLINE, sender.next_event())
            .await
            .unwrap()
            .expect("an event")
        {
            LinkEvent::Outcome {
                delivery_id,
                outcome,
                settled: true,
            } => {
                assert_eq!(outcome, Outcome::Accepted);
                settled.push(delivery_id);
            }
            LinkEvent::Outcome { settled: false, .. } => panic!("the frame settled"),
            _ => {}
        }
    }
    settled.sort_unstable();
    assert_eq!(settled, vec![0, 1, 2]);
    assert!(sender.unsettled().is_empty());

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_repeated_disposition_is_nothing_rather_than_an_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "orders",
                11,
                Role::Receiver,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::First,
            )
            .await;
        server.send_flow(4, 11, 0, 1).await;
        let transfer = server.read_transfer().await;
        let id = transfer.delivery_id.unwrap();
        server
            .send_disposition(
                4,
                Role::Receiver,
                id,
                None,
                true,
                Some(&DeliveryState::Accepted),
            )
            .await;
        // The same frame again. Settlement is idempotent, which is what makes
        // a `disposition` safe to repeat and why a lost one needs no
        // recovery protocol.
        server
            .send_disposition(
                4,
                Role::Receiver,
                id,
                None,
                true,
                Some(&DeliveryState::Released),
            )
            .await;
        server.expect_close().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = LinkOptions::sender("orders", Target::at("q"));
    link.snd_settle_mode = SenderSettleMode::Unsettled;
    let mut sender = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();
    let sent = timeout(DEADLINE, sender.send(&Message::data(b"order-1")))
        .await
        .unwrap()
        .unwrap();
    let outcome = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome, Outcome::Accepted);

    // The second frame named a different outcome for a delivery this end had
    // forgotten. It changes nothing and reports nothing: the connection is
    // still usable and no second event arrives.
    assert!(
        tokio::time::timeout(QUIET, sender.next_event())
            .await
            .is_err(),
        "a repeated disposition is not a second answer"
    );
    assert!(sender.unsettled().is_empty());

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn under_second_the_sender_settles_before_the_receiver_does() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "orders",
                11,
                Role::Receiver,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::Second,
            )
            .await;
        server.send_flow(4, 11, 0, 1).await;
        let transfer = server.read_transfer().await;
        let id = transfer.delivery_id.unwrap();

        // The receiver publishes its outcome and does **not** settle: under
        // `second` the answer is provisional until the sender settles.
        server
            .send_disposition(
                4,
                Role::Receiver,
                id,
                None,
                false,
                Some(&DeliveryState::Accepted),
            )
            .await;

        // The sender's half of the dance, which this client owes without the
        // application doing anything.
        let answer = server.read_disposition().await;
        assert_eq!(answer.role, Role::Sender, "the sender is speaking now");
        assert_eq!(answer.first, id);
        assert_eq!(answer.last, None, "one delivery, so `first` alone");
        assert!(answer.settled, "and this is the settlement");
        assert_eq!(
            answer.outcome,
            Some(Outcome::Accepted),
            "carrying the outcome it is confirming"
        );

        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = LinkOptions::sender("orders", Target::at("q"));
    link.snd_settle_mode = SenderSettleMode::Unsettled;
    link.rcv_settle_mode = ReceiverSettleMode::Second;
    let mut sender = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sender.negotiated().unwrap().rcv_settle_mode,
        ReceiverSettleMode::Second,
        "the answering attach is what says the mode is actually in force"
    );

    let sent = timeout(DEADLINE, sender.send(&Message::data(b"order-1")))
        .await
        .unwrap()
        .unwrap();
    // What the mode proves: when this returns, the receiver knows the outcome
    // *and* knows that this end knows it. That is the whole difference from
    // `first`, and it costs a round trip in each direction.
    let outcome = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome, Outcome::Accepted);
    assert!(sender.unsettled().is_empty(), "settled here too");

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_receiver_under_first_settles_with_its_own_disposition() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "invoices",
                11,
                Role::Sender,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::First,
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "flow", "the receiver granted credit");

        let mut transfer = TransferFrame::new(11);
        transfer.delivery_id = Some(7);
        transfer.delivery_tag = Some(b"t-7");
        transfer.message_format = Some(0);
        transfer.settled = Some(false);
        let message = Message::data(b"invoice-1").to_vec().unwrap();
        server.send_transfer(4, transfer, &message).await;

        let answer = server.read_disposition().await;
        assert_eq!(answer.role, Role::Receiver);
        assert_eq!(answer.first, 7);
        assert!(
            answer.settled,
            "under `first` the receiver settles as it reports: one frame, done"
        );
        assert_eq!(answer.outcome, Some(Outcome::Accepted));

        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(DEADLINE, receiver.grant_credit(2))
        .await
        .unwrap()
        .unwrap();

    let delivery = timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("a delivery");
    assert!(!delivery.settled, "so an answer is expected");
    assert_eq!(
        receiver.unsettled().len(),
        1,
        "the receiver holds state until it says what happened"
    );
    assert_eq!(receiver.unsettled()[0].delivery_tag, b"t-7");

    timeout(DEADLINE, receiver.accept(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        receiver.unsettled().is_empty(),
        "settled on the disposition: nothing more is coming"
    );
    // Settling twice writes nothing at all, which the server asserts by
    // seeing no second disposition before the close.
    timeout(DEADLINE, receiver.accept(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_receiver_under_second_waits_for_the_senders_settlement() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "invoices",
                11,
                Role::Sender,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::Second,
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "flow");

        let mut transfer = TransferFrame::new(11);
        transfer.delivery_id = Some(7);
        transfer.delivery_tag = Some(b"t-7");
        transfer.message_format = Some(0);
        transfer.settled = Some(false);
        let message = Message::data(b"invoice-1").to_vec().unwrap();
        server.send_transfer(4, transfer, &message).await;

        let answer = server.read_disposition().await;
        assert_eq!(answer.role, Role::Receiver);
        assert_eq!(answer.first, 7);
        assert!(
            !answer.settled,
            "under `second` the receiver reports and waits: settling first \
             would leave the sender unable to tell it had been heard"
        );
        assert_eq!(answer.outcome, Some(Outcome::Accepted));

        // The sender settles, carrying no state: it confirms what was already
        // said rather than saying it again.
        server
            .send_disposition(4, Role::Sender, 7, None, true, None)
            .await;

        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = LinkOptions::receiver("invoices", Source::at("q"));
    link.rcv_settle_mode = ReceiverSettleMode::Second;
    let mut receiver = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, receiver.grant_credit(2))
        .await
        .unwrap()
        .unwrap();

    let delivery = timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("a delivery");
    timeout(DEADLINE, receiver.accept(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();
    let pending = receiver.unsettled();
    assert_eq!(pending.len(), 1, "still held: the sender has not settled");
    assert_eq!(pending[0].ours, Some(Outcome::Accepted));
    assert!(!pending[0].settled_here, "and this end has not settled");

    let outcome = timeout(DEADLINE, receiver.settled(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        outcome,
        Outcome::Accepted,
        "the outcome the settlement confirmed is the one this end published"
    );
    assert!(receiver.unsettled().is_empty());

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_delivery_that_arrived_settled_is_not_answerable() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "invoices",
                11,
                Role::Sender,
                SenderSettleMode::Settled,
                ReceiverSettleMode::First,
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "flow");

        let mut transfer = TransferFrame::new(11);
        transfer.delivery_id = Some(3);
        transfer.delivery_tag = Some(b"t-3");
        transfer.message_format = Some(0);
        transfer.settled = Some(true);
        let message = Message::data(b"invoice-1").to_vec().unwrap();
        server.send_transfer(4, transfer, &message).await;

        // Nothing to answer: the sender has forgotten the delivery, so a
        // disposition would name something it no longer holds.
        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(DEADLINE, receiver.grant_credit(2))
        .await
        .unwrap()
        .unwrap();

    let delivery = timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("a delivery");
    assert!(
        delivery.settled,
        "which is the application's cue that this message is all it will ever know"
    );
    assert!(receiver.unsettled().is_empty());
    // Accepting it writes nothing: there is no delivery here to settle.
    timeout(DEADLINE, receiver.accept(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_receiver_may_reject_release_and_modify_over_a_range() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "invoices",
                11,
                Role::Sender,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::First,
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "flow");

        for id in 0..3u32 {
            let mut transfer = TransferFrame::new(11);
            transfer.delivery_id = Some(id);
            transfer.delivery_tag = Some(b"tag");
            transfer.message_format = Some(0);
            transfer.settled = Some(false);
            let message = Message::data(b"invoice").to_vec().unwrap();
            server.send_transfer(4, transfer, &message).await;
        }

        let answer = server.read_disposition().await;
        assert_eq!(answer.first, 0);
        assert_eq!(answer.last, Some(2), "one frame covering three deliveries");
        assert!(answer.settled);
        assert_eq!(
            answer.outcome,
            Some(Outcome::Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(true),
                message_annotations: None,
            })
        );

        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(DEADLINE, receiver.grant_credit(3))
        .await
        .unwrap()
        .unwrap();

    for _ in 0..3 {
        let _ = timeout(DEADLINE, receiver.next_delivery())
            .await
            .unwrap()
            .expect("a delivery");
    }
    assert_eq!(receiver.unsettled().len(), 3);

    // "Available again, with a note", for a batch, in one frame: this
    // consumer cannot process any of them and says so about all three.
    timeout(
        DEADLINE,
        receiver.settle(
            0,
            2,
            &Outcome::Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(true),
                message_annotations: None,
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(receiver.unsettled().is_empty());

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_rejection_from_the_receiver_carries_the_reason_it_was_given() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        server
            .answer_attach(
                "invoices",
                11,
                Role::Sender,
                SenderSettleMode::Unsettled,
                ReceiverSettleMode::First,
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "flow");

        let mut transfer = TransferFrame::new(11);
        transfer.delivery_id = Some(1);
        transfer.delivery_tag = Some(b"t-1");
        transfer.message_format = Some(0);
        transfer.settled = Some(false);
        let message = Message::data(b"not json").to_vec().unwrap();
        server.send_transfer(4, transfer, &message).await;

        let answer = server.read_disposition().await;
        match answer.outcome {
            Some(Outcome::Rejected { error }) => {
                let error = error.expect("a reason");
                assert_eq!(error.condition, condition::DECODE_ERROR);
                assert_eq!(
                    error.description.as_deref(),
                    Some("the body is not a JSON object")
                );
            }
            other => panic!("expected rejected, got {other:?}"),
        }
        server.expect_close_without_disposition().await;
    });

    let connection = connect(&exec, port).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(DEADLINE, receiver.grant_credit(1))
        .await
        .unwrap()
        .unwrap();
    let delivery = timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("a delivery");
    timeout(
        DEADLINE,
        receiver.reject(
            delivery.delivery_id,
            Some(Condition::described(
                condition::DECODE_ERROR,
                "the body is not a JSON object",
            )),
        ),
    )
    .await
    .unwrap()
    .unwrap();

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
