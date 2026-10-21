//! Transfers and link credit against a scripted server.
//!
//! The two credit schemes are independent and simultaneously active, so every
//! test here says which one it is about. Link credit is counted in
//! **messages** and granted only by the receiver; the session window is
//! counted in **transfer frames**. Two of these tests exist only because the
//! schemes are separate: a sender with generous credit and a one-frame window
//! sends one frame, and a multi-frame message spends the window once per
//! frame while spending link credit once in total.

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use support::{DEADLINE, Server, accept, options, scripted};
use weida_amqp::link::{LinkEvent, LinkOptions};
use weida_amqp::session::SessionOptions;
use weida_amqp::{Connection, Source, Target};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Message;
use weida_amqp_codec::performative::{
    Attach, Begin, Close, Flow, Open, Performative, Transfer as TransferFrame,
};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Role};
use weida_runtime::Exec;

/// Long enough that a frame the client meant to send would have arrived, short
/// enough that asserting an absence does not dominate the suite.
const QUIET: Duration = Duration::from_millis(150);

/// What a test looks at in a `transfer` it read, owned.
struct TransferSeen {
    delivery_id: Option<u32>,
    delivery_tag: Option<Vec<u8>>,
    message_format: Option<u32>,
    settled: Option<bool>,
    more: bool,
    aborted: bool,
    payload: Vec<u8>,
}

/// What a test looks at in a `flow` it read, owned.
struct FlowSeen {
    handle: Option<u32>,
    delivery_count: Option<u32>,
    link_credit: Option<u32>,
    available: Option<u32>,
    drain: bool,
    echo: bool,
    incoming_window: u32,
    next_outgoing_id: u32,
}

/// What the scripted server puts in a `flow`.
///
/// A struct rather than eight positional arguments, because two of the eight
/// are session state and six are link state and mixing them up in a call is
/// exactly the confusion the two schemes invite.
struct Grant {
    handle: u32,
    delivery_count: Option<u32>,
    link_credit: Option<u32>,
    drain: bool,
    echo: bool,
    next_incoming_id: u32,
    incoming_window: u32,
}

impl Server {
    /// The next `transfer`, with the payload that followed the performative in
    /// the same body.
    async fn read_transfer(&mut self) -> TransferSeen {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, used) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        let payload = frame.body[used..].to_vec();
        match performative {
            Performative::Transfer(transfer) => TransferSeen {
                delivery_id: transfer.delivery_id,
                delivery_tag: transfer.delivery_tag.map(<[u8]>::to_vec),
                message_format: transfer.message_format,
                settled: transfer.settled,
                more: transfer.more,
                aborted: transfer.aborted,
                payload,
            },
            other => panic!("expected transfer, got {}", other.name()),
        }
    }

    async fn read_flow(&mut self) -> FlowSeen {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Flow(flow) => FlowSeen {
                handle: flow.handle,
                delivery_count: flow.delivery_count,
                link_credit: flow.link_credit,
                available: flow.available,
                drain: flow.drain,
                echo: flow.echo,
                incoming_window: flow.incoming_window,
                next_outgoing_id: flow.next_outgoing_id,
            },
            other => panic!("expected flow, got {}", other.name()),
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

    /// A `flow` naming a link, with the session fields every `flow` must
    /// carry.
    async fn send_flow(
        &mut self,
        channel: u16,
        handle: u32,
        delivery_count: Option<u32>,
        link_credit: Option<u32>,
        drain: bool,
        echo: bool,
    ) {
        self.send_flow_frame(
            channel,
            Grant {
                handle,
                delivery_count,
                link_credit,
                drain,
                echo,
                next_incoming_id: 0,
                incoming_window: 400,
            },
        )
        .await;
    }

    async fn send_flow_frame(&mut self, channel: u16, grant: Grant) {
        let flow = Flow {
            next_incoming_id: Some(grant.next_incoming_id),
            incoming_window: grant.incoming_window,
            next_outgoing_id: 0,
            outgoing_window: 400,
            handle: Some(grant.handle),
            delivery_count: grant.delivery_count,
            link_credit: grant.link_credit,
            available: Some(0),
            drain: grant.drain,
            echo: grant.echo,
            properties: None,
        };
        self.send(channel, Performative::Flow(flow)).await;
    }

    async fn handshake(&mut self) {
        self.handshake_with_frame_size(u32::MAX).await;
    }

    /// The handshake, advertising `max_frame_size` in the server's `open`.
    ///
    /// The number the client splits a message on is the **peer's**, which is
    /// why a test that wants several frames sets it here and not on the
    /// client.
    async fn handshake_with_frame_size(&mut self, max_frame_size: u32) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "open");
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        open.max_frame_size = max_frame_size;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn answer_begin(&mut self, ours: u16, theirs: u16, incoming_window: u32) {
        let mut begin = Begin::new(0, incoming_window, 400);
        begin.remote_channel = Some(theirs);
        begin.handle_max = 255;
        self.send(ours, Performative::Begin(begin)).await;
    }

    /// The answering `attach`, in the role opposite to the client's.
    async fn answer_attach(
        &mut self,
        channel: u16,
        name: &str,
        handle: u32,
        role: Role,
        initial_delivery_count: u32,
    ) {
        let source = Source::default();
        let target = Target::at("q");
        let mut attach = Attach::new(name, handle, role);
        attach.source = Some(source.to_value());
        attach.target = Some(target.to_value());
        if role == Role::Sender {
            attach.initial_delivery_count = Some(initial_delivery_count);
        }
        self.send(channel, Performative::Attach(attach)).await;
    }

    /// Reads frames until the client's `close` and answers it.
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

/// A `transfer` from the server, first frame of its delivery.
fn first(handle: u32, delivery_id: u32, tag: &[u8], more: bool) -> TransferFrame<'_> {
    let mut transfer = TransferFrame::new(handle);
    transfer.delivery_id = Some(delivery_id);
    transfer.delivery_tag = Some(tag);
    transfer.message_format = Some(0);
    transfer.settled = Some(false);
    transfer.more = more;
    transfer
}

/// A continuation, carrying no identity of its own.
fn more(handle: u32, more: bool) -> TransferFrame<'static> {
    let mut transfer = TransferFrame::new(handle);
    transfer.more = more;
    transfer
}

#[tokio::test]
async fn a_receivers_grant_reaches_the_wire_as_a_limit_and_a_delivery_spends_one() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        // The sender's sequence starts at 7, which the receiver has no say in
        // and must adopt before it can name a delivery-count at all.
        server
            .answer_attach(4, "invoices", 11, Role::Sender, 7)
            .await;

        let flow = server.read_flow().await;
        assert_eq!(flow.handle, Some(0), "the client's own handle");
        assert_eq!(
            flow.delivery_count,
            Some(7),
            "the last known value of the sender's, learned from its attach"
        );
        assert_eq!(flow.link_credit, Some(2));
        assert!(!flow.drain);
        assert!(!flow.echo);
        assert_eq!(
            flow.incoming_window, 400,
            "every flow carries session state"
        );

        let message = Message::data(b"invoice-1").to_vec().unwrap();
        server
            .send_transfer(4, first(11, 100, b"t-1", false), &message)
            .await;

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

    assert_eq!(
        receiver.credit().link_credit(),
        0,
        "a freshly attached link carries no permission to send anything"
    );
    assert_eq!(receiver.credit().delivery_count(), 7);

    tokio::time::timeout(DEADLINE, receiver.grant_credit(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.credit().delivery_limit(), 9);

    let delivery = tokio::time::timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("a delivery arrived");
    assert_eq!(delivery.delivery_id, 100);
    assert_eq!(delivery.delivery_tag, b"t-1");
    assert_eq!(delivery.message_format, 0);
    assert!(!delivery.settled);
    assert_eq!(
        delivery.message().unwrap().body,
        weida_amqp_codec::message::Body::Data(vec![b"invoice-1".as_slice()])
    );

    let credit = receiver.credit();
    assert_eq!(credit.link_credit(), 1, "one message, one unit of credit");
    assert_eq!(credit.delivery_count(), 8);
    assert_eq!(
        credit.delivery_limit(),
        9,
        "the limit is what does not move"
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
async fn a_multi_frame_delivery_reassembles_and_spends_credit_once() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "invoices", 11, Role::Sender, 0)
            .await;
        let _ = server.read_flow().await;

        // One message in three frames. The continuations carry no
        // delivery-id, delivery-tag or message-format, which is legal and is
        // what a receiver has to cope with.
        let message = Message::data(&[b'x'; 300]).to_vec().unwrap();
        let (a, rest) = message.split_at(100);
        let (b, c) = rest.split_at(100);
        server.send_transfer(4, first(11, 5, b"t-1", true), a).await;
        server.send_transfer(4, more(11, true), b).await;
        server.send_transfer(4, more(11, false), c).await;

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
    tokio::time::timeout(DEADLINE, receiver.grant_credit(4))
        .await
        .unwrap()
        .unwrap();

    let delivery = tokio::time::timeout(DEADLINE, receiver.next_delivery())
        .await
        .unwrap()
        .expect("one delivery, not three");
    assert_eq!(delivery.delivery_id, 5);
    assert_eq!(delivery.delivery_tag, b"t-1");
    assert_eq!(
        delivery.message().unwrap().body,
        weida_amqp_codec::message::Body::Data(vec![[b'x'; 300].as_slice()]),
        "three frames, one message"
    );

    let credit = receiver.credit();
    assert_eq!(
        credit.link_credit(),
        3,
        "link credit is counted in messages, so three frames cost one unit"
    );
    assert_eq!(credit.delivery_count(), 1);

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
async fn a_second_delivery_before_the_first_completes_detaches_the_link() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "invoices", 11, Role::Sender, 0)
            .await;
        let _ = server.read_flow().await;

        server
            .send_transfer(4, first(11, 1, b"first", true), b"aaa")
            .await;
        // A different tag while the first delivery is incomplete: two
        // deliveries interleaving on one link, which Part 2 §2.6.14 forbids.
        server
            .send_transfer(4, first(11, 2, b"second", false), b"bbb")
            .await;

        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Detach(detach) => {
                assert!(detach.closed);
                let error = detach.error.expect("an errored detach");
                assert_eq!(error.condition, condition::NOT_ALLOWED);
            }
            other => panic!("expected detach, got {}", other.name()),
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
    tokio::time::timeout(DEADLINE, receiver.grant_credit(4))
        .await
        .unwrap()
        .unwrap();

    // Neither half-message is handed over: the octets of two messages
    // concatenated are not a message, and the link is gone instead.
    let event = tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
        .expect("an event");
    match event {
        LinkEvent::Detached(Some(condition)) => {
            assert_eq!(condition.condition, condition::NOT_ALLOWED);
            assert!(
                condition
                    .description
                    .unwrap_or_default()
                    .contains("before the first was complete")
            );
        }
        other => panic!("expected a detach, got {other:?}"),
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
async fn a_message_above_max_message_size_is_refused_with_the_condition() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let attach = server.read_frame().await;
        let frame = frame::decode(&attach.1, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Attach(attach) => assert_eq!(
                attach.max_message_size,
                Some(64),
                "the bound the receiver advertised is the bound it enforces"
            ),
            other => panic!("expected attach, got {}", other.name()),
        }
        server
            .answer_attach(4, "invoices", 11, Role::Sender, 0)
            .await;
        let _ = server.read_flow().await;

        // Two frames of 60 octets each: neither frame is over the limit and
        // the message is, which is the case `max-frame-size` alone cannot
        // catch.
        server
            .send_transfer(4, first(11, 1, b"t-1", true), &[b'x'; 60])
            .await;
        server.send_transfer(4, more(11, false), &[b'x'; 60]).await;

        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Detach(detach) => {
                let error = detach.error.expect("an errored detach");
                assert_eq!(error.condition, condition::LINK_MESSAGE_SIZE_EXCEEDED);
            }
            other => panic!("expected detach, got {}", other.name()),
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
    let mut link = LinkOptions::receiver("invoices", Source::at("q"));
    link.max_message_size = Some(64);
    let mut receiver = tokio::time::timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, receiver.grant_credit(4))
        .await
        .unwrap()
        .unwrap();

    let event = tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
        .expect("an event");
    match event {
        LinkEvent::Detached(Some(condition)) => {
            assert_eq!(condition.condition, condition::LINK_MESSAGE_SIZE_EXCEEDED);
        }
        other => panic!("expected a detach, got {other:?}"),
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
async fn a_sender_with_no_credit_stalls_until_the_flow_arrives() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;

        // The assertion is an absence: with no credit granted, nothing may go
        // out at all, and a client that sent anyway would be violating the
        // one rule the scheme has.
        assert!(
            tokio::time::timeout(QUIET, server.read_frame())
                .await
                .is_err(),
            "a sender with no credit sends nothing"
        );

        server
            .send_flow(4, 11, Some(0), Some(1), false, false)
            .await;

        let transfer = server.read_transfer().await;
        assert_eq!(transfer.delivery_id, Some(0), "the first transfer-id");
        assert_eq!(transfer.delivery_tag, Some(vec![0, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(transfer.message_format, Some(0));
        assert_eq!(
            transfer.settled,
            Some(false),
            "a Mixed link sends unsettled, so the receiver can report"
        );
        assert!(!transfer.more);
        assert!(!transfer.aborted);
        assert_eq!(
            transfer.payload,
            Message::data(b"order-1").to_vec().unwrap()
        );

        // And the second message stays off the wire: one credit, one message.
        assert!(
            tokio::time::timeout(QUIET, server.read_frame())
                .await
                .is_err(),
            "the grant was one message, not a licence"
        );
        server
            .send_flow(4, 11, Some(1), Some(1), false, false)
            .await;
        let second = server.read_transfer().await;
        assert_eq!(second.delivery_id, Some(1));
        assert_eq!(second.delivery_tag, Some(vec![0, 0, 0, 0, 0, 0, 0, 1]));

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
    let sender = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!sender.may_send(), "no credit yet");

    let first = tokio::time::timeout(DEADLINE, sender.send(&Message::data(b"order-1")))
        .await
        .expect("the send completed once credit arrived")
        .unwrap();
    assert_eq!(first.delivery_id, 0);
    assert_eq!(first.frames, 1);
    assert!(!first.settled);
    assert_eq!(sender.credit().delivery_count(), 1);
    assert_eq!(sender.credit().link_credit(), 0, "spent");

    let second = tokio::time::timeout(DEADLINE, sender.send(&Message::data(b"order-2")))
        .await
        .expect("the second send completed once the second grant arrived")
        .unwrap();
    assert_eq!(second.delivery_id, 1);

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
async fn a_message_larger_than_a_frame_goes_out_in_several_transfers() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        // The peer's ceiling is what splits a message, so the server sets the
        // smallest one both peers MUST accept.
        server.handshake_with_frame_size(MIN_MAX_FRAME_SIZE).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
        server
            .send_flow(4, 11, Some(0), Some(1), false, false)
            .await;

        let mut payload = Vec::new();
        let mut frames = 0usize;
        loop {
            let transfer = server.read_transfer().await;
            if frames == 0 {
                assert_eq!(transfer.delivery_id, Some(0));
                assert_eq!(transfer.delivery_tag, Some(vec![0, 0, 0, 0, 0, 0, 0, 0]));
            } else {
                assert_eq!(
                    transfer.delivery_id, None,
                    "a continuation MAY omit the id, and this client does"
                );
                assert_eq!(transfer.delivery_tag, None);
            }
            payload.extend_from_slice(&transfer.payload);
            frames += 1;
            if !transfer.more {
                break;
            }
        }
        assert!(
            frames >= 4,
            "1600 octets through a 512-octet frame is at least four frames, got {frames}"
        );
        assert_eq!(payload, Message::data(&[b'y'; 1600]).to_vec().unwrap());

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
    let sender = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    let sent = tokio::time::timeout(DEADLINE, sender.send(&Message::data(&[b'y'; 1600])))
        .await
        .unwrap()
        .unwrap();
    assert!(sent.frames >= 4, "{} frames", sent.frames);
    assert_eq!(sent.delivery_id, 0, "the id of the first frame");
    assert_eq!(
        sender.credit().delivery_count(),
        1,
        "one message however many frames: the two schemes count different things"
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
async fn the_session_window_stalls_a_sender_that_still_has_link_credit() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        // One transfer frame of room on the session, and generous link
        // credit below. Both must permit a transfer, and the specification
        // ties them together nowhere.
        server.answer_begin(4, 0, 1).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
        // Generous link credit, and the session window left exactly where
        // `begin` put it: every `flow` carries `incoming-window`, so a grant
        // that named a larger one would have reopened the window it is
        // supposed to be independent of.
        server
            .send_flow_frame(
                4,
                Grant {
                    handle: 11,
                    delivery_count: Some(0),
                    link_credit: Some(5),
                    drain: false,
                    echo: false,
                    next_incoming_id: 0,
                    incoming_window: 1,
                },
            )
            .await;

        let first = server.read_transfer().await;
        assert_eq!(first.delivery_id, Some(0));
        assert!(
            tokio::time::timeout(QUIET, server.read_frame())
                .await
                .is_err(),
            "four units of link credit left and no window: nothing goes out"
        );

        // The window, not the credit, is what reopens: one frame has been
        // received, so the peer's next-incoming-id is 1.
        server
            .send_flow_frame(
                4,
                Grant {
                    handle: 11,
                    delivery_count: Some(0),
                    link_credit: Some(5),
                    drain: false,
                    echo: false,
                    next_incoming_id: 1,
                    incoming_window: 5,
                },
            )
            .await;
        let second = server.read_transfer().await;
        assert_eq!(second.delivery_id, Some(1));

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
    let sender = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    tokio::time::timeout(DEADLINE, sender.send(&Message::data(b"one")))
        .await
        .unwrap()
        .unwrap();
    // Link credit remains; the window is what is missing, and the send parks
    // until the `flow` refreshes it.
    assert!(sender.credit().may_send(), "credit is not the constraint");
    let second = tokio::time::timeout(DEADLINE, sender.send(&Message::data(b"two")))
        .await
        .expect("the second send completed once the window reopened")
        .unwrap();
    assert_eq!(second.delivery_id, 1);

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
async fn a_drain_makes_the_sender_consume_its_credit_and_report() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;

        // Three credits and a drain, with the sender holding nothing: it MUST
        // advance delivery-count until link-credit is zero and report.
        server.send_flow(4, 11, Some(0), Some(3), true, false).await;
        let report = server.read_flow().await;
        assert_eq!(report.handle, Some(0));
        assert_eq!(
            report.delivery_count,
            Some(3),
            "advanced by the credit it could not use"
        );
        assert_eq!(report.link_credit, Some(0), "the definite answer");
        assert_eq!(report.available, Some(0));
        assert!(report.drain);
        assert!(!report.echo, "answering an echo with an echo loops forever");
        assert_eq!(report.next_outgoing_id, 0, "no transfer was sent");

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
    let mut sender = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    let event = tokio::time::timeout(DEADLINE, sender.next_event())
        .await
        .unwrap()
        .expect("the flow was reported");
    match event {
        LinkEvent::Flow(credit) => {
            assert_eq!(credit.link_credit(), 0);
            assert_eq!(credit.delivery_count(), 3);
            assert!(credit.drain());
        }
        other => panic!("expected a flow, got {other:?}"),
    }
    assert!(!sender.may_send(), "drained");

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
async fn an_echo_is_answered_once_with_this_ends_own_state() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "invoices", 11, Role::Sender, 0)
            .await;
        let granted = server.read_flow().await;
        assert_eq!(granted.link_credit, Some(2));

        // The sender asks for the receiver's state and names a credit of its
        // own, which is not its to name.
        server.send_flow(4, 11, Some(0), Some(0), false, true).await;
        let answer = server.read_flow().await;
        assert_eq!(answer.handle, Some(0));
        assert_eq!(
            answer.link_credit,
            Some(2),
            "only the receiver may choose link-credit, so the sender's flow did not change it"
        );
        assert_eq!(answer.delivery_count, Some(0));
        assert_eq!(answer.available, Some(0));
        assert!(!answer.echo, "an echo is never answered with an echo");
        assert_eq!(answer.incoming_window, 400, "and it carries session state");
        // The canonical stop, from the end that owns it: credit to zero and
        // echo set, so that the answer marks the point after which no further
        // transfer will come (Part 2 §2.6.10).
        let stop = server.read_flow().await;
        assert_eq!(stop.link_credit, Some(0));
        assert!(stop.echo);
        assert!(!stop.drain, "a stop is not a drain");

        // Two flows in total: the answer and the stop. Nothing after them but
        // the close, which is how "answered once" is asserted — the client
        // never sets `echo` on an answer, so the loop the specification warns
        // about cannot start here.
        loop {
            let (_, name, _) = server.read_named().await;
            assert_ne!(name, "flow", "the echo was answered once");
            if name == "close" {
                break;
            }
        }
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
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
    tokio::time::timeout(DEADLINE, receiver.grant_credit(2))
        .await
        .unwrap()
        .unwrap();

    // The barrier: the sender's `flow` has been seen, so its answer has been
    // written, and the stop below cannot reach the wire first.
    let event = tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
        .expect("the sender's flow");
    match event {
        LinkEvent::Flow(credit) => assert_eq!(credit.link_credit(), 2),
        other => panic!("expected a flow, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, receiver.stop())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.credit().link_credit(), 0);

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
async fn a_receivers_drain_reaches_the_wire_and_the_answer_zeroes_its_credit() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "invoices", 11, Role::Sender, 0)
            .await;
        let granted = server.read_flow().await;
        assert_eq!(granted.link_credit, Some(1));
        assert!(!granted.drain);

        let drained = server.read_flow().await;
        assert!(drained.drain, "the receiver asked for a definite answer");
        assert_eq!(drained.link_credit, Some(1), "the limit has not moved");

        // The sender had nothing: it consumed the credit and says so.
        server.send_flow(4, 11, Some(1), Some(0), true, false).await;

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
    tokio::time::timeout(DEADLINE, receiver.drain())
        .await
        .unwrap()
        .unwrap();

    let event = tokio::time::timeout(DEADLINE, receiver.next_event())
        .await
        .unwrap()
        .expect("the sender reported");
    match event {
        LinkEvent::Flow(credit) => {
            assert_eq!(
                credit.link_credit(),
                0,
                "credit consumed by the drain and not by a message"
            );
            assert_eq!(credit.delivery_count(), 1);
        }
        other => panic!("expected a flow, got {other:?}"),
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
async fn a_receiver_may_not_send_and_a_sender_may_not_grant() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 400).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
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
    let sender = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    // Credit is granted by the receiver and only by the receiver: a sender
    // granting itself credit is the one thing the scheme has to make
    // impossible.
    let error = tokio::time::timeout(DEADLINE, sender.grant_credit(5))
        .await
        .unwrap()
        .expect_err("refused");
    assert!(
        error.to_string().contains("only the receiver may grant"),
        "{error}"
    );
    let error = tokio::time::timeout(DEADLINE, sender.drain())
        .await
        .unwrap()
        .expect_err("refused");
    assert!(
        error.to_string().contains("only the receiver may drain"),
        "{error}"
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
