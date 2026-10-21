//! The tests of `tests/cancel_and_retention.rs` that need the network, a process or real time.

use crate::cancel_and_retention::support::{DEADLINE, accept, options, scripted};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;
use weida_amqp::link::{LinkEvent, LinkOptions};
use weida_amqp::session::SessionOptions;
use weida_amqp::{Outcome, Source, Target};
use weida_amqp_codec::Role;
use weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE;
use weida_amqp_codec::message::Message;
use weida_amqp_codec::performative::{Disposition, Performative, Transfer as TransferFrame};
use weida_amqp_codec::state::DeliveryState;
use weida_amqp_codec::types::condition;
use weida_runtime::Exec;

use crate::cancel_and_retention::*;

/// A send that is given up on before its first frame is queued has taken
/// nothing: not the credit, not a place in the unsettled map, not a tag. The
/// window is what it waits for, and it was the credit that used to be gone by
/// then.
#[tokio::test]
async fn a_send_dropped_while_it_waits_for_window_has_taken_nothing() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;
    let (reopen, reopened) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        // A session window of nothing: every frame has to wait.
        server.answer_begin(0).await;
        server.answer_attach("orders", 11, Role::Receiver).await;
        server
            .send_flow(flow(Some(11), Some(0), Some(1), Some(0), 0, 0, 0))
            .await;
        let _ = reopened.await;
        server
            .send_flow(flow(Some(11), Some(0), Some(1), Some(0), 0, 10, 0))
            .await;
        let seen = server.read_transfer().await;
        assert_eq!(seen.delivery_id, Some(0), "the delivery the credit was for");
        server.expect_close().await;
    });

    let connection = connect(&exec, port, options()).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    // The grant has to be in before the send looks at it.
    timeout(DEADLINE, async {
        while sender.credit().link_credit() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the grant arrived");

    assert!(
        timeout(
            Duration::from_millis(300),
            sender.send(&Message::data(b"one"))
        )
        .await
        .is_err(),
        "no window, so the send is still waiting when it is dropped"
    );
    assert_eq!(
        sender.credit().link_credit(),
        1,
        "the credit is still there"
    );
    assert_eq!(sender.credit().delivery_count(), 0, "and was never spent");
    assert!(sender.unsettled().is_empty(), "no place was taken for it");

    reopen.send(()).unwrap();
    let sent = timeout(DEADLINE, sender.send(&Message::data(b"one")))
        .await
        .expect("the window reopened")
        .unwrap();
    assert_eq!(sent.delivery_id, 0);
    assert_eq!(
        sent.delivery_tag,
        vec![0u8; 8],
        "the dropped call burned no tag: the first delivery still has the first"
    );

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
/// A message of several frames is never left half sent. With room for two
/// frames in the window and a message of five, the first two go out; the send
/// is dropped; and when the window reopens the receiver is told to discard
/// what it has — `transfer(aborted)` — before the next delivery starts. The
/// credit stays spent, and nothing is held for the delivery here.
#[tokio::test]
async fn a_send_dropped_between_frames_aborts_its_delivery() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;
    let (reopen, reopened) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        // The smallest frame both peers must accept, so that 1600 octets are
        // several frames.
        server.handshake(MIN_MAX_FRAME_SIZE).await;
        server.answer_begin(2).await;
        server.answer_attach("orders", 11, Role::Receiver).await;
        server
            .send_flow(flow(Some(11), Some(0), Some(10), Some(0), 0, 2, 0))
            .await;

        let first = server.read_transfer().await;
        assert_eq!(first.delivery_id, Some(0));
        assert!(first.more && !first.aborted);
        let second = server.read_transfer().await;
        assert!(second.more && !second.aborted);
        let _ = reopened.await;
        // Two frames received, ten more of room.
        server
            .send_flow(flow(None, None, None, None, 2, 10, 0))
            .await;

        let closing = server.read_transfer().await;
        assert!(closing.aborted, "the half delivery is closed on the wire");
        assert_eq!(closing.delivery_id, Some(0), "the one that was started");
        let next = server.read_transfer().await;
        assert_eq!(next.delivery_id, Some(3), "a clean delivery follows it");
        assert!(!next.more && !next.aborted);
        server.expect_close().await;
    });

    let connection = connect(&exec, port, options()).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(DEADLINE, async {
        while sender.credit().link_credit() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the grant arrived");

    let long = vec![b'y'; 1600];
    assert!(
        timeout(Duration::from_millis(300), sender.send_payload(&long))
            .await
            .is_err(),
        "two frames of room for five: the send is waiting when it is dropped"
    );
    assert_eq!(sender.credit().delivery_count(), 1, "the credit is spent");
    assert!(sender.unsettled().is_empty(), "and nothing is held for it");

    reopen.send(()).unwrap();
    let sent = timeout(DEADLINE, sender.send(&Message::data(b"next")))
        .await
        .expect("the next send follows the abort")
        .unwrap();
    assert_eq!(sent.delivery_id, 3);

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
/// The only copy of a rejection is not an event. A producer that sends more
/// than a link keeps events for, reads none, and then asks about the first
/// delivery is told what the peer answered.
#[tokio::test]
async fn a_final_outcome_outlives_the_events_that_reported_it() {
    const SENT: u32 = 1100;
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        server.answer_begin(LARGE_WINDOW).await;
        server.answer_attach("orders", 11, Role::Receiver).await;
        server
            .send_flow(flow(
                Some(11),
                Some(0),
                Some(5000),
                Some(0),
                0,
                LARGE_WINDOW,
                0,
            ))
            .await;
        for _ in 0..SENT {
            let seen = server.read_transfer().await;
            assert!(!seen.more);
        }
        // One frame answers them all, and rejects them all.
        let mut answer = Disposition::new(Role::Receiver, 0);
        answer.last = Some(SENT - 1);
        answer.settled = true;
        answer.state = Some(DeliveryState::Rejected { error: None }.to_value());
        server.send(4, Performative::Disposition(answer)).await;
        // The marker: a credit nothing else sets, which the client can see
        // without reading an event.
        server
            .send_flow(flow(
                Some(11),
                Some(SENT),
                Some(4321),
                Some(0),
                SENT,
                LARGE_WINDOW,
                0,
            ))
            .await;
        server.expect_close().await;
    });

    let connection = connect(&exec, port, options()).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    for _ in 0..SENT {
        timeout(DEADLINE, sender.send_payload(b"x"))
            .await
            .expect("the send completed")
            .unwrap();
    }
    // Every answer has been applied once the marker is visible: the frames
    // were read in order.
    timeout(DEADLINE, async {
        while sender.credit().link_credit() != 4321 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the answers and the marker arrived");

    let held = sender.unsettled();
    assert_eq!(held.len(), SENT as usize, "every answer is still held");
    assert!(held.iter().all(|pending| pending.settled_there));

    for id in [0, SENT / 2, SENT - 1] {
        let outcome = timeout(DEADLINE, sender.settled(id))
            .await
            .expect("an answer is not waited for: it is there")
            .expect("and it is the peer's");
        assert!(
            matches!(outcome, Outcome::Rejected { .. }),
            "delivery {id}: {outcome:?}"
        );
    }
    assert_eq!(sender.unsettled().len(), SENT as usize - 3, "taking frees");

    // The queue still reports what it kept, and reading it takes the answer
    // the same way.
    let mut taken = 0;
    while sender.unsettled().len() > SENT as usize - 3 - 50 {
        match timeout(DEADLINE, sender.next_event()).await.unwrap() {
            Some(LinkEvent::Outcome {
                settled: true,
                outcome,
                ..
            }) => {
                assert!(matches!(outcome, Outcome::Rejected { .. }));
                taken += 1;
            }
            Some(_) => {}
            None => panic!("the link ended"),
        }
    }
    assert!(taken >= 50, "reading the events took {taken} answers");

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
/// A `flow` that says the sender's count went backwards does not hand back
/// credit that was spent: ten were granted and ten arrived, so the eleventh
/// is past every limit however the count is told.
#[tokio::test]
async fn a_flow_that_rewinds_the_count_does_not_make_room_for_more() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        server.answer_begin(400).await;
        server.answer_attach("invoices", 11, Role::Sender).await;
        // The grant, which is the client's `flow`.
        loop {
            let (_, name, _) = server.read_named().await;
            if name == "flow" {
                break;
            }
        }
        for id in 0..10u32 {
            let tag = [id as u8];
            let mut transfer = TransferFrame::new(11);
            transfer.delivery_id = Some(id);
            transfer.delivery_tag = Some(&tag);
            transfer.message_format = Some(0);
            transfer.settled = Some(true);
            server.send_transfer(transfer, b"m").await;
        }
        // The sender "reports" a count of zero, ten deliveries in.
        server
            .send_flow(flow(Some(11), Some(0), None, Some(0), 0, 400, 10))
            .await;
        let mut transfer = TransferFrame::new(11);
        transfer.delivery_id = Some(10);
        transfer.delivery_tag = Some(b"x");
        transfer.message_format = Some(0);
        transfer.settled = Some(true);
        server.send_transfer(transfer, b"m").await;
        server.expect_close().await;
    });

    let connection = connect(&exec, port, options()).await;
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
    timeout(DEADLINE, receiver.grant_credit(10))
        .await
        .unwrap()
        .unwrap();

    let mut delivered = 0;
    let detached = timeout(DEADLINE, async {
        loop {
            match receiver.next_event().await {
                Some(LinkEvent::Delivery(_)) => delivered += 1,
                Some(LinkEvent::Detached(condition)) => return condition,
                Some(_) => {}
                None => panic!("the link's events ended without a detach"),
            }
        }
    })
    .await
    .expect("the eleventh delivery detached the link");
    assert_eq!(delivered, 10, "the ten that were granted, and no more");
    assert_eq!(
        detached.expect("with a condition").condition,
        condition::LINK_TRANSFER_LIMIT_EXCEEDED
    );

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
/// The application's own backlog does not stop the driver reading. The
/// client floods a peer that never reads until the outbox is full; the peer
/// then says something about another link, and the client hears it.
#[tokio::test]
async fn the_applications_backlog_does_not_stop_the_driver_reading() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;
    let (speak, spoken) = oneshot::channel::<()>();
    let (release, released) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(128 * 1024).await;
        server.answer_begin(LARGE_WINDOW).await;
        server.answer_attach("orders", 11, Role::Receiver).await;
        server.answer_attach("events", 12, Role::Sender).await;
        server
            .send_flow(flow(
                Some(11),
                Some(0),
                Some(1000),
                Some(0),
                0,
                LARGE_WINDOW,
                0,
            ))
            .await;
        // Not reading from here on, until the client says the outbox is full.
        let _ = spoken.await;
        server
            .send_flow(flow(
                Some(12),
                Some(0),
                Some(0),
                Some(7),
                0,
                LARGE_WINDOW,
                0,
            ))
            .await;
        let _ = released.await;
    });

    let mut options = options();
    options.write_timeout = Duration::from_secs(30);
    let connection = connect(&exec, port, options).await;
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    let mut events = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("events", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    let flooding = tokio::spawn(async move {
        let chunk = vec![0x5au8; MIB];
        while let Ok(Ok(_)) = timeout(DEADLINE, sender.send_payload(&chunk)).await {}
    });
    // Long enough for the flood to fill what the socket will take, so that
    // the outbox holds far more than its high-water mark.
    tokio::time::sleep(Duration::from_millis(500)).await;
    speak.send(()).unwrap();

    let heard = timeout(DEADLINE, async {
        loop {
            if let Some(LinkEvent::Flow(credit)) = events.next_event().await {
                return credit;
            }
        }
    })
    .await
    .expect("the driver went on reading while the outbox was full of the application's frames");
    assert_eq!(heard.available(), 7);

    flooding.abort();
    let _ = release.send(());
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
/// The last `Connection` going out of scope does not close the connection
/// under the session it returned: the session and its links keep it alive,
/// and the `close` comes when the last of them goes.
#[tokio::test]
async fn a_session_keeps_the_connection_alive_after_the_connection_is_dropped() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        server.answer_begin(400).await;
        // The next thing the client says is an `attach`; a `close` here is
        // the connection having gone with the handle that was dropped.
        server.answer_attach("orders", 11, Role::Receiver).await;
        server.expect_close().await;
    });

    let session = {
        let connection = connect(&exec, port, options()).await;
        timeout(DEADLINE, connection.begin(SessionOptions::default()))
            .await
            .unwrap()
            .unwrap()
    };
    let link = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .expect("the session still has a connection")
    .expect("and the peer answered the attach");
    drop(link);
    drop(session);
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
