//! Receiver credit, enforced at the receiving end, against a scripted sender.
//!
//! A receiver announces limits with `flow` and the sender may use every one it
//! was given: a delivery past all of them detaches the link, and what was in
//! flight under a limit that was later lowered still arrives. The unsettled map
//! is bounded, so a grant it could not hold is refused before it is sent, and a
//! delivery that arrives unsettled when the map is full is refused too. The
//! server half is the sender, so each test says what it sends and what the
//! client has to do about it.

#[path = "support/mod.rs"]
mod support;

use support::{DEADLINE, Server, accept, options, scripted};
use weida_amqp::link::{LinkEvent, LinkOptions};
use weida_amqp::session::SessionOptions;
use weida_amqp::{Connection, Error, Source, Target};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Message;
use weida_amqp_codec::performative::{
    Attach, Begin, Close, Detach, Open, Performative, Transfer as TransferFrame,
};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Role, SenderSettleMode};
use weida_runtime::Exec;

/// What a test looks at in a `detach` the client wrote, owned.
struct DetachSeen {
    handle: u32,
    closed: bool,
    condition: Option<String>,
}

/// What a test looks at in a `flow` the client wrote, owned.
struct FlowSeen {
    link_credit: Option<u32>,
    echo: bool,
}

/// The performative of a frame that is not a keep-alive.
fn decode_performative(bytes: &[u8]) -> Performative<'_> {
    let frame = frame::decode(bytes, u32::MAX).unwrap();
    Performative::decode(frame.body, Limits::DEFAULT).unwrap().0
}

/// A first `transfer` of an unsettled delivery, with no payload attached yet.
fn transfer_on(handle: u32, delivery_id: u32, tag: &[u8]) -> TransferFrame<'_> {
    let mut transfer = TransferFrame::new(handle);
    transfer.delivery_id = Some(delivery_id);
    transfer.delivery_tag = Some(tag);
    transfer.message_format = Some(0);
    transfer.settled = Some(false);
    transfer
}

/// A continuation frame, which carries no identity of its own.
fn continuation(handle: u32, more: bool) -> TransferFrame<'static> {
    let mut transfer = TransferFrame::new(handle);
    transfer.more = more;
    transfer
}

impl Server {
    async fn handshake(&mut self) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "open");
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        open.max_frame_size = u32::MAX;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn answer_begin(&mut self, ours: u16, theirs: u16) {
        let mut begin = Begin::new(0, 400, 400);
        begin.remote_channel = Some(theirs);
        begin.handle_max = 255;
        self.send(ours, Performative::Begin(begin)).await;
    }

    /// The answering `attach` of a sending link, carrying the settle mode the
    /// sender imposes. The sender's sequence starts at zero.
    async fn answer_sender(
        &mut self,
        channel: u16,
        name: &str,
        handle: u32,
        snd: SenderSettleMode,
    ) {
        let source = Source::at("q");
        let target = Target::default();
        let mut attach = Attach::new(name, handle, Role::Sender);
        attach.snd_settle_mode = snd;
        attach.source = Some(source.to_value());
        attach.target = Some(target.to_value());
        attach.initial_delivery_count = Some(0);
        self.send(channel, Performative::Attach(attach)).await;
    }

    /// The next `flow` the client writes on any link.
    async fn read_flow(&mut self) -> FlowSeen {
        loop {
            let (_, name, bytes) = self.read_named().await;
            if name == "flow" {
                match decode_performative(&bytes) {
                    Performative::Flow(flow) => {
                        return FlowSeen {
                            link_credit: flow.link_credit,
                            echo: flow.echo,
                        };
                    }
                    _ => unreachable!("the name said flow"),
                }
            }
        }
    }

    /// The next `detach` the client writes.
    async fn read_detach(&mut self) -> DetachSeen {
        loop {
            let (_, name, bytes) = self.read_named().await;
            if name == "detach" {
                match decode_performative(&bytes) {
                    Performative::Detach(detach) => {
                        return DetachSeen {
                            handle: detach.handle,
                            closed: detach.closed,
                            condition: detach.error.map(|e| e.condition.to_owned()),
                        };
                    }
                    _ => unreachable!("the name said detach"),
                }
            }
        }
    }

    /// A `transfer` and its payload in one frame.
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

    /// Reads until the client's `close` and answers it. A `disposition` in
    /// between would be a settlement the client had no business sending here.
    async fn expect_close(&mut self) {
        loop {
            let (_, name, _) = self.read_named().await;
            assert_ne!(
                name, "disposition",
                "nothing was settled, so nothing is acknowledged"
            );
            if name == "close" {
                break;
            }
        }
        self.send(0, Performative::Close(Close { error: None }))
            .await;
    }
}

#[tokio::test]
async fn a_delivery_nobody_granted_credit_for_detaches_the_link() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_sender(4, "invoices", 11, SenderSettleMode::Mixed)
            .await;

        // The application granted nothing, so no limit was ever announced
        // and this delivery is past all of them.
        let message = Message::data(b"invoice-1").to_vec().unwrap();
        server
            .send_transfer(4, transfer_on(11, 0, b"t-0"), &message)
            .await;

        let detach = server.read_detach().await;
        assert_eq!(detach.handle, 0, "the client's own handle");
        assert!(detach.closed);
        assert_eq!(
            detach.condition.as_deref(),
            Some(condition::LINK_TRANSFER_LIMIT_EXCEEDED)
        );
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    match tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
    {
        Some(LinkEvent::Detached(Some(detached))) => {
            assert_eq!(detached.condition, condition::LINK_TRANSFER_LIMIT_EXCEEDED);
        }
        other => panic!("expected a detach with transfer-limit-exceeded, got {other:?}"),
    }

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn what_was_in_flight_when_credit_was_lowered_is_still_accepted() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_sender(4, "invoices", 11, SenderSettleMode::Mixed)
            .await;

        let granted = server.read_flow().await;
        assert_eq!(granted.link_credit, Some(5), "the grant, relative to 0");
        assert!(!granted.echo);

        let message = Message::data(b"m").to_vec().unwrap();
        for id in 0..2u32 {
            let tag = id.to_be_bytes();
            server
                .send_transfer(4, transfer_on(11, id, &tag), &message)
                .await;
        }

        // The application stops the link. The echo is the marker after which
        // nothing further is due, and the limit it names is zero.
        let stopped = server.read_flow().await;
        assert_eq!(stopped.link_credit, Some(0));
        assert!(stopped.echo);

        // Sent after the stop but still within the limit granted before it.
        for id in 2..5u32 {
            let tag = id.to_be_bytes();
            server
                .send_transfer(4, transfer_on(11, id, &tag), &message)
                .await;
        }
        // The sixth delivery in total, past the limit of five.
        let tag = 5u32.to_be_bytes();
        server
            .send_transfer(4, transfer_on(11, 5, &tag), &message)
            .await;

        let detach = server.read_detach().await;
        assert!(detach.closed);
        assert_eq!(
            detach.condition.as_deref(),
            Some(condition::LINK_TRANSFER_LIMIT_EXCEEDED)
        );
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    tokio::time::timeout(DEADLINE, receiver.grant_credit(5))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        tokio::time::timeout(DEADLINE, receiver.next_delivery())
            .await
            .unwrap()
            .expect("an in-flight delivery under the granted credit");
    }

    tokio::time::timeout(DEADLINE, receiver.stop())
        .await
        .unwrap()
        .unwrap();
    for expected in 2..5u32 {
        let delivery = tokio::time::timeout(DEADLINE, receiver.next_delivery())
            .await
            .unwrap()
            .expect("a delivery that was in flight when credit was lowered");
        assert_eq!(delivery.delivery_id, expected);
    }

    // The link was not detached by the five deliveries above: the sixth is
    // the first event that can be a detach.
    match tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
    {
        Some(LinkEvent::Detached(Some(detached))) => {
            assert_eq!(detached.condition, condition::LINK_TRANSFER_LIMIT_EXCEEDED);
        }
        other => panic!("expected a detach with transfer-limit-exceeded, got {other:?}"),
    }

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn an_unsettled_delivery_that_finds_the_map_full_is_refused() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        // The sender settles everything it sends, so the receiver may grant
        // more than its map holds. A peer that then sends unsettled
        // deliveries anyway is breaking the mode it negotiated.
        server
            .answer_sender(4, "invoices", 11, SenderSettleMode::Settled)
            .await;

        let granted = server.read_flow().await;
        assert_eq!(granted.link_credit, Some(5000));

        let payload = b"x";
        for id in 0..2049u32 {
            let tag = id.to_be_bytes();
            server
                .send_transfer(4, transfer_on(11, id, &tag), payload)
                .await;
        }

        let detach = server.read_detach().await;
        assert!(detach.closed);
        assert_eq!(
            detach.condition.as_deref(),
            Some(condition::RESOURCE_LIMIT_EXCEEDED)
        );
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    // The session window has to cover the 2049 transfers, or the window would
    // be the refusal and not the map.
    let session_options = SessionOptions {
        incoming_window: 4096,
        ..SessionOptions::default()
    };
    let session = tokio::time::timeout(DEADLINE, connection.begin(session_options))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    tokio::time::timeout(DEADLINE, receiver.grant_credit(5000))
        .await
        .unwrap()
        .expect("a settled-on-send link may be granted past the map");

    let mut delivered = 0usize;
    loop {
        match tokio::time::timeout(DEADLINE, receiver.next_event())
            .await
            .unwrap()
        {
            Some(LinkEvent::Delivery(_)) => delivered += 1,
            Some(LinkEvent::Detached(Some(detached))) => {
                assert_eq!(detached.condition, condition::RESOURCE_LIMIT_EXCEEDED);
                break;
            }
            other => panic!("expected deliveries then a detach, got {other:?}"),
        }
    }
    assert_eq!(delivered, 2048, "the map holds exactly this many");
    assert_eq!(
        receiver.unsettled().len(),
        2048,
        "the first 2048 were recorded, the refused one was not"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_grant_the_map_cannot_hold_is_refused() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_sender(4, "invoices", 11, SenderSettleMode::Mixed)
            .await;

        // Only the 2048 grant reaches the wire: the 2049 one is refused before
        // any flow is written.
        let granted = server.read_flow().await;
        assert_eq!(granted.link_credit, Some(2048));

        let payload = b"m";
        for id in 0..3u32 {
            let tag = id.to_be_bytes();
            server
                .send_transfer(4, transfer_on(11, id, &tag), payload)
                .await;
        }
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    let refused = tokio::time::timeout(DEADLINE, receiver.grant_credit(2049))
        .await
        .unwrap();
    assert!(
        matches!(refused, Err(Error::Configuration(_))),
        "one more than the map can hold is refused before it is sent"
    );
    tokio::time::timeout(DEADLINE, receiver.grant_credit(2048))
        .await
        .unwrap()
        .expect("exactly what the map can hold");

    for _ in 0..3 {
        tokio::time::timeout(DEADLINE, receiver.next_delivery())
            .await
            .unwrap()
            .expect("an unsettled delivery within the grant");
    }
    assert_eq!(receiver.unsettled().len(), 3);

    // Three are held, so 2045 is the room left.
    let refused = tokio::time::timeout(DEADLINE, receiver.grant_credit(2046))
        .await
        .unwrap();
    assert!(
        matches!(refused, Err(Error::Configuration(_))),
        "the held deliveries count against the room"
    );
    tokio::time::timeout(DEADLINE, receiver.grant_credit(2045))
        .await
        .unwrap()
        .expect("the room that is left");

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_settle_on_a_detached_link_is_an_error_and_not_an_acknowledgement() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_sender(4, "invoices", 11, SenderSettleMode::Mixed)
            .await;
        let _ = server.read_flow().await;

        let message = Message::data(b"m").to_vec().unwrap();
        server
            .send_transfer(4, transfer_on(11, 0, b"t-0"), &message)
            .await;
        // The peer detaches without an error, and the client answers.
        let mut detach = Detach::new(11);
        detach.closed = true;
        server.send(4, Performative::Detach(detach)).await;
        let answer = server.read_detach().await;
        assert_eq!(answer.handle, 0, "the client's own handle");
        assert!(answer.closed);
        assert!(answer.condition.is_none());
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(DEADLINE, receiver.grant_credit(1))
        .await
        .unwrap()
        .unwrap();

    // The delivery was handed over before the detach, so it comes first.
    let delivery = tokio::time::timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("the delivery that arrived before the detach");
    assert_eq!(delivery.delivery_id, 0);
    match tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
    {
        Some(LinkEvent::Detached(None)) => {}
        other => panic!("expected a clean detach, got {other:?}"),
    }

    // Delivery 0 is still held, and the link is gone, so acknowledging it
    // cannot reach the peer: that is an error, not a silent success.
    let refused = tokio::time::timeout(DEADLINE, receiver.accept(0))
        .await
        .unwrap();
    assert!(
        matches!(refused, Err(Error::Closed(_))),
        "a held delivery on a detached link cannot be settled"
    );
    // Nothing is held for 99, so there is nothing to acknowledge and no frame
    // to write.
    tokio::time::timeout(DEADLINE, receiver.accept(99))
        .await
        .unwrap()
        .expect("an id never delivered is not an error");

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_multi_frame_delivery_settled_on_its_first_frame_is_not_recorded_as_unsettled() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_sender(4, "invoices", 11, SenderSettleMode::Mixed)
            .await;
        let _ = server.read_flow().await;

        // Two frames. The first says the delivery is settled and the last
        // leaves the flag unset, which Part 2 §2.7.5 reads as "as the
        // preceding transfers said".
        let message = Message::data(&[b'x'; 200]).to_vec().unwrap();
        let (a, b) = message.split_at(100);
        let mut first = TransferFrame::new(11);
        first.delivery_id = Some(0);
        first.delivery_tag = Some(b"t-0".as_slice());
        first.message_format = Some(0);
        first.settled = Some(true);
        first.more = true;
        server.send_transfer(4, first, a).await;
        server.send_transfer(4, continuation(11, false), b).await;
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(DEADLINE, receiver.grant_credit(1))
        .await
        .unwrap()
        .unwrap();

    let delivery = tokio::time::timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("the reassembled delivery");
    assert!(
        delivery.settled,
        "the first frame settled it, and the last frame's unset flag does not undo that"
    );
    assert!(
        receiver.unsettled().is_empty(),
        "a delivery settled on arrival is recorded nowhere"
    );
    assert!(delivery.message().is_ok(), "the payload reassembled");

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}
