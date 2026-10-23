//! `SUB`, `UNSUB`, queue groups, and `MSG`/`HMSG` dispatched to the
//! subscription that asked for it.
//!
//! Subject matching itself is a pure function and is tested exhaustively in
//! `src/subject.rs`'s unit tests, where the two wildcards and the degenerate
//! subjects can be asserted without a socket. What is asserted here is
//! everything that needs a wire: the `sid`s this client chooses, the
//! queue-group argument's position, the two forms of `UNSUB`, and the routing
//! of a delivery to exactly one subscription.
//!
//! Every await is bounded, so a wrong turn fails the test rather than hanging
//! the suite.

use crate::support;

use support::{Server, options, within};
use weida_nats::{Connection, Error};
use weida_runtime::Exec;

/// A `SUB` carries the subject and a `sid` this client generated, and the
/// `sid` comes back in the `MSG` that the delivery is routed by.
#[tokio::test]
async fn a_sub_carries_a_client_chosen_sid_and_the_msg_comes_back_on_it() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        let sub = server.read_op().await;
        assert_eq!(
            sub, "SUB orders.> - 1",
            "the subject, no queue group, and the sid the client chose"
        );
        // The server quotes the client's sid back; that is the whole routing
        // key for a delivery.
        server
            .send_msg("orders.created", "1", None, b"{\"id\":7}")
            .await;
        server
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");

    let mut orders = nats.subscribe("orders.>").await.expect("subscribed");
    assert_eq!(orders.sid(), 1);
    assert_eq!(orders.subject(), b"orders.>");
    assert!(orders.queue_group().is_none());

    let mut server = within("a step of the test", server).await.unwrap();
    let message = within("a message arrived", orders.next())
        .await
        .expect("the subscription is live");
    assert_eq!(message.sid, 1);
    assert_eq!(
        message.subject_str(),
        Some("orders.created"),
        "the literal subject the publisher used, not the pattern that matched"
    );
    assert_eq!(message.payload, &b"{\"id\":7}"[..]);
    assert!(message.reply_to.is_none());
    assert!(message.headers.is_none(), "a MSG has no header block");

    drop(orders);
    // Dropping the handle is the subscription's end, and that is an `UNSUB`.
    assert_eq!(server.read_op().await, "UNSUB 1 -");
    within("a step of the test", nats.close()).await.unwrap();
}

/// `HMSG` arrives with its block, and it is dispatched exactly like a `MSG`:
/// the header is a payload concern, the `sid` is the routing concern.
#[tokio::test]
async fn an_hmsg_is_dispatched_with_its_header_block() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        server
            .send_hmsg("events", "1", "Nats-Msg-Id", "abc-1", b"body")
            .await;
        server
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    let mut events = nats.subscribe("events").await.expect("subscribed");
    let _server = within("a step of the test", server).await.unwrap();

    let message = within("a message arrived", events.next())
        .await
        .expect("live");
    let headers = message.headers.as_ref().expect("a header block");
    assert_eq!(headers.get("Nats-Msg-Id"), Some("abc-1"));
    assert_eq!(
        headers.get_ignore_ascii_case("nats-msg-id"),
        Some("abc-1"),
        "case is preserved on the wire and ignorable on lookup"
    );
    assert_eq!(message.payload, &b"body"[..]);
    assert!(message.status().is_none());

    within("a step of the test", nats.close()).await.unwrap();
}

/// Two subscriptions on one connection matching the same subject each get
/// their own copy, and each copy goes to the `sid` the server addressed it
/// to and to no other.
#[tokio::test]
async fn a_delivery_reaches_the_sid_it_names_and_no_other() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB orders.* - 1");
        assert_eq!(server.read_op().await, "SUB orders.> - 2");
        // One publication, two matching subscriptions, two MSG lines: "every
        // active ordinary subscription matching that subject receives one
        // copy".
        server.send_msg("orders.created", "1", None, b"first").await;
        server
            .send_msg("orders.created", "2", None, b"second")
            .await;
        // And one that only the wider pattern can match.
        server
            .send_msg("orders.eu.created", "2", None, b"nested")
            .await;
        server
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    let mut one_token = nats.subscribe("orders.*").await.expect("subscribed");
    let mut trailing = nats.subscribe("orders.>").await.expect("subscribed");
    assert_eq!((one_token.sid(), trailing.sid()), (1, 2));

    let _server = within("a step of the test", server).await.unwrap();

    let first = within("a step of the test", one_token.next())
        .await
        .unwrap();
    assert_eq!((first.sid, &first.payload[..]), (1, &b"first"[..]));

    let second = within("a step of the test", trailing.next()).await.unwrap();
    assert_eq!((second.sid, &second.payload[..]), (2, &b"second"[..]));
    let nested = within("a step of the test", trailing.next()).await.unwrap();
    assert_eq!(nested.subject_str(), Some("orders.eu.created"));

    assert!(
        one_token.try_next().is_none(),
        "`orders.*` is one token, and nothing else was addressed to sid 1"
    );

    within("a step of the test", nats.close()).await.unwrap();
}

/// Two subscriptions in one queue group beside one ordinary subscription.
///
/// **What this test proves is the client's half.** The three `SUB` lines carry
/// the right queue-group argument in the right position and three distinct
/// `sid`s, and a `MSG` for each `sid` reaches that subscription and no other.
///
/// **What it does not prove is the server's half** — that one publication
/// yields one copy *between* the two group members while the ordinary
/// subscriber gets its own. That is the server's selection ("for each
/// publication, the server selects one eligible member from each matching
/// queue group") and no scripted server can establish it: a test that made
/// its own fake server deliver one copy would be asserting the script, not
/// the behaviour. It is proved against a real `nats-server` in
/// `tests/interop_nats_server.rs`.
#[tokio::test]
async fn a_queue_group_is_one_argument_between_subject_and_sid() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        // "Subscribers join a queue group by placing the group name between
        // subject and `sid` in `SUB`" — so the group is the middle argument,
        // and the argument count is the only thing that says so.
        assert_eq!(server.read_op().await, "SUB jobs workers 1");
        assert_eq!(server.read_op().await, "SUB jobs workers 2");
        assert_eq!(
            server.read_op().await,
            "SUB jobs - 3",
            "an ordinary subscription beside them has no group argument at all"
        );

        server.send_msg("jobs", "1", None, b"to-worker-a").await;
        server.send_msg("jobs", "3", None, b"to-the-observer").await;
        server
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");

    let mut worker_a = nats
        .subscribe_with_queue_group("jobs", "workers")
        .await
        .expect("joined");
    let mut worker_b = nats
        .subscribe_with_queue_group("jobs", "workers")
        .await
        .expect("joined");
    let mut observer = nats.subscribe("jobs").await.expect("subscribed");

    assert_eq!(worker_a.queue_group(), Some(&b"workers"[..]));
    assert_eq!(worker_b.queue_group(), Some(&b"workers"[..]));
    assert!(
        observer.queue_group().is_none(),
        "a queue group is not a subject: the ordinary subscriber shares the \
         subject and not the group"
    );
    assert_eq!(
        (worker_a.sid(), worker_b.sid(), observer.sid()),
        (1, 2, 3),
        "three distinct sids, all chosen by this client"
    );

    let _server = within("a step of the test", server).await.unwrap();

    let to_a = within("a step of the test", worker_a.next()).await.unwrap();
    assert_eq!(to_a.payload, &b"to-worker-a"[..]);
    let to_observer = within("a step of the test", observer.next()).await.unwrap();
    assert_eq!(to_observer.payload, &b"to-the-observer"[..]);
    assert!(
        worker_b.try_next().is_none(),
        "the copy addressed to sid 1 is not delivered to sid 2"
    );

    within("a step of the test", nats.close()).await.unwrap();
}

/// An empty queue-group name is refused where it was given: on the wire it
/// would vanish, turning `SUB jobs "" 1` into `SUB jobs 1` — an ordinary
/// subscription, silently.
#[tokio::test]
async fn an_unusable_queue_group_is_refused_before_the_wire() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        // Nothing at all: both refusals were local.
        server.expect_silence().await;
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");

    assert!(matches!(
        nats.subscribe_with_queue_group("jobs", "").await,
        Err(Error::InvalidSubject { .. })
    ));
    assert!(matches!(
        nats.subscribe_with_queue_group("jobs", "two words").await,
        Err(Error::InvalidSubject { .. })
    ));
    // And the pattern rule, at the call that asked for it.
    assert!(matches!(
        nats.subscribe("orders.>.created").await,
        Err(Error::InvalidSubject { .. })
    ));
    assert!(matches!(
        nats.subscribe("orders..created").await,
        Err(Error::InvalidSubject { .. })
    ));

    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}

/// `UNSUB <sid>` with no count: the subscription ends now.
#[tokio::test]
async fn unsub_without_a_count_ends_the_subscription() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        assert_eq!(server.read_op().await, "UNSUB 1 -");
        // A message the server had already written before it read the UNSUB.
        // The subscription is gone locally, so it goes nowhere — which is the
        // client honouring its own removal rather than delivering past it.
        server.send_msg("events", "1", None, b"too-late").await;
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        server
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    let mut events = nats.subscribe("events").await.expect("subscribed");
    events.unsubscribe().await;

    // The flush goes out first: the server's script reads that PING, so
    // joining before issuing it would wait on each other.
    within("a step of the test", nats.flush()).await.unwrap();
    let _server = within("a step of the test", server).await.unwrap();
    assert!(
        events.try_next().is_none(),
        "a message written before the UNSUB was read is not delivered after it"
    );

    within("a step of the test", nats.close()).await.unwrap();
}

/// `UNSUB <sid> <max_msgs>` is auto-unsubscribe-after-N, and the count is
/// honoured **locally as well as sent**.
///
/// The local half is the point: the server may write the Nth message before
/// it processes the `UNSUB`, and it certainly writes anything already in
/// flight. So the scripted server sends four messages for a subscription
/// capped at three, and the fourth must not be delivered.
#[tokio::test]
async fn unsub_with_a_count_is_honoured_locally_as_well_as_sent() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        assert_eq!(
            server.read_op().await,
            "UNSUB 1 3",
            "the count travels, because the server is the one that stops sending"
        );
        for n in 0..4u8 {
            server.send_msg("events", "1", None, &[b'0' + n]).await;
        }
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        server
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    let mut events = nats.subscribe("events").await.expect("subscribed");
    events.unsubscribe_after(3).await;

    // The flush goes out first: its PING is what the server's script reads
    // last, and it is what establishes that all four MSG lines have been
    // read by the driver before the assertion below.
    within("a step of the test", nats.flush()).await.unwrap();
    let _server = within("a step of the test", server).await.unwrap();

    let mut seen = Vec::new();
    while let Some(message) = events.try_next() {
        seen.push(message.payload);
    }
    assert_eq!(
        seen,
        vec![b"0".to_vec(), b"1".to_vec(), b"2".to_vec()],
        "three messages, and the fourth the server had already written is not \
         delivered"
    );

    within("a step of the test", nats.close()).await.unwrap();
}

/// The count in `UNSUB <sid> <max_msgs>` is what the *server* sent, so a copy
/// this client had no room for still counts. With a queue of one and a cap of
/// three, the second and third copies are dropped, and the subscription must
/// still end with the third — the server has already ended it — so that the
/// reader sees the end instead of waiting for a message that never comes.
#[tokio::test]
async fn a_dropped_copy_still_counts_toward_the_auto_unsubscribe_count() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        assert_eq!(server.read_op().await, "UNSUB 1 3");
        for n in 0..3u8 {
            server.send_msg("events", "1", None, &[b'0' + n]).await;
        }
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        server
    });

    let mut options = options();
    options.subscription_queue = 1;
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");
    let mut events = nats.subscribe("events").await.expect("subscribed");
    events.unsubscribe_after(3).await;

    // The PONG establishes that all three MSG lines reached the driver while
    // nobody was reading, which is what makes two of them drops.
    within("a step of the test", nats.flush()).await.unwrap();
    let _server = within("a step of the test", server).await.unwrap();
    assert_eq!(events.dropped(), 2, "the queue held one copy of three");

    let first = within("the queued copy is readable", events.next())
        .await
        .expect("it is a message");
    assert_eq!(first.payload, &b"0"[..]);
    assert!(
        within(
            "the subscription ended rather than waiting for a fourth",
            events.next()
        )
        .await
        .is_none(),
        "three copies were sent, so the count is reached"
    );

    within("a step of the test", nats.close()).await.unwrap();
}

/// The subscription table is bounded, and the bound is ours because the `sid`
/// is: nothing in the protocol could stop the table from growing.
#[tokio::test]
async fn the_subscription_table_is_bounded() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB a - 1");
        assert_eq!(server.read_op().await, "SUB b - 2");
        // No third SUB: the refusal happened before the wire.
        server.expect_silence().await;
    });

    let mut options = options();
    options.max_subscriptions = 2;
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");

    let _first = nats.subscribe("a").await.expect("subscribed");
    let _second = nats.subscribe("b").await.expect("subscribed");
    let error = nats.subscribe("c").await.expect_err("the table is full");
    assert!(
        matches!(error, Error::TooManySubscriptions { max: 2 }),
        "{error}"
    );

    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}

/// A publish subject is literal. Publishing to a pattern is refused before
/// the wire, because Core NATS has no such thing.
#[tokio::test]
async fn publishing_to_a_pattern_is_refused_before_the_wire() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let (refused, mut wait) = tokio::sync::mpsc::channel::<()>(1);
    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        // Every refusal below is local, so nothing at all reaches the wire.
        server.expect_silence().await;
        refused.send(()).await.unwrap();
        assert_eq!(server.read_op().await, "PUB orders.created - {}");
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");

    for pattern in ["orders.*", "orders.>", "", ".orders", "orders."] {
        assert!(
            matches!(
                nats.publish(pattern, b"{}").await,
                Err(Error::InvalidSubject { .. })
            ),
            "{pattern:?} must be refused"
        );
    }
    // Ordered by a channel, not a sleep: the server has established the
    // silence before the legal publish goes out.
    within("a step of the test", wait.recv()).await.unwrap();
    nats.publish("orders.created", b"{}")
        .await
        .expect("a literal subject");
    within("a step of the test", nats.flush()).await.unwrap();

    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}

/// A header block on a server that did not advertise `headers`: refused
/// locally, because `HPUB` is a verb such a server does not know.
#[tokio::test]
async fn headers_against_a_server_without_them_are_refused() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server.handshake("{\"server_id\":\"S1\",\"proto\":1}").await;
        assert!(
            !connect.contains("\"headers\""),
            "a capability the server did not offer is not claimed: {connect}"
        );
        server.expect_silence().await;
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    assert!(!nats.headers_supported());

    let mut headers = weida_nats::OwnedHeaders::new();
    headers.push("Nats-Msg-Id", "abc-1");
    let error = nats
        .publish_with("events", None, Some(&headers), b"body")
        .await
        .expect_err("no headers on this server");
    assert!(matches!(error, Error::HeadersUnsupported), "{error}");

    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}

/// Every subscription ends when the connection does: "core subscriptions
/// vanish with the connection and have no stored session". A caller waiting
/// on one finds out rather than waiting forever.
#[tokio::test]
async fn subscriptions_end_with_the_connection() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        // The server drops the transport, which is how a NATS connection
        // ends in either direction.
        drop(server);
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    let mut events = nats.subscribe("events").await.expect("subscribed");
    within("a step of the test", server).await.unwrap();

    assert!(
        within("the subscription ended within the deadline", events.next())
            .await
            .is_none(),
        "the subscription ends rather than hanging"
    );
    assert!(
        !within("the connection reported", nats.closed())
            .await
            .is_usable()
    );
    assert!(matches!(
        nats.publish("events", b"x").await,
        Err(Error::ConnectionGone)
    ));
    assert!(matches!(
        nats.subscribe("more").await,
        Err(Error::ConnectionGone)
    ));
}

/// A subscription whose queue is full loses the copy and keeps the
/// connection, which is the trade Core NATS at-most-once delivery already
/// allows — and the alternative is a stalled read loop and then a
/// slow-consumer disconnect.
#[tokio::test]
async fn a_full_subscription_queue_drops_copies_rather_than_the_connection() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake_full().await;
        assert_eq!(server.read_op().await, "SUB events - 1");
        for n in 0..5u8 {
            server.send_msg("events", "1", None, &[b'0' + n]).await;
        }
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        server
    });

    let mut options = options();
    options.subscription_queue = 2;
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");
    let mut events = nats.subscribe("events").await.expect("subscribed");

    // The flush goes out first: its PING is what the server's script reads
    // last, and the PONG establishes that all five MSG lines reached the
    // driver before the count below.
    within("a step of the test", nats.flush()).await.unwrap();
    let _server = within("a step of the test", server).await.unwrap();

    let mut seen = 0;
    while events.try_next().is_some() {
        seen += 1;
    }
    assert_eq!(seen, 2, "the queue holds what it was sized for");
    assert!(
        nats.state().is_usable(),
        "a dropped copy costs one message, not the connection"
    );

    // The subscription is still in the table: a drop is one lost copy, not a
    // removal, so a further publish is accepted rather than refused.
    nats.publish("events", b"still here").await.expect("usable");
    within("a step of the test", nats.close()).await.unwrap();
}
