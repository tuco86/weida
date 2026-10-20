//! Pub/Sub over real QUIC on loopback.
//!
//! Fan-out is the one genuinely new selection policy of the pattern layer;
//! everything else reuses the one-way transfer path. These tests pin the parts that are
//! easy to get subtly wrong: prefix filtering, per-subscriber FIFO delivery,
//! subscribe/unsubscribe bookkeeping, and the drop policy that keeps a slow
//! subscriber from stalling the publisher.

use crate::common;

use std::time::Duration;

use common::Server;
use weida::{Error, GuaranteeSet, Limits, OrderingMode, Publisher, RuntimeConfig, Subscriber};

/// Generous ceiling: every assertion below should settle in milliseconds.
pub(crate) const DEADLINE: Duration = Duration::from_secs(15);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Waits until the publisher observes `count` filters.
///
/// Registry updates happen in the frame-processing task, so a subscribe is
/// visible a moment after `subscribe()` returns. Polling the publisher's own
/// counter is exact; a sleep would only be a guess.
pub(crate) async fn await_filters(publisher: &Publisher, count: usize) {
    within(async {
        while publisher.filter_count() != count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}

/// Receives one message and returns `(topic, payload)`.
pub(crate) async fn recv_one(sub: &Subscriber) -> (String, Vec<u8>) {
    let transfer = within(sub.recv()).await.expect("recv");
    let topic = transfer
        .meta()
        .topic
        .clone()
        .expect("a published message carries its topic");
    let body = within(transfer.collect(1024 * 1024))
        .await
        .expect("collect");
    (topic, body)
}

/// The segmented grammar over a real connection: one-segment `*`, trailing
/// `#`, and the boundary a byte prefix could not see.
#[tokio::test(start_paused = true)]
async fn subscribe_filters_topics_by_segment() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.*")).await.expect("subscribe px.*");
    within(sub.subscribe("ctl.#"))
        .await
        .expect("subscribe ctl.#");
    within(sub.subscribe("sensors.temp"))
        .await
        .expect("subscribe sensors.temp");
    await_filters(&publisher, 3).await;

    // `px.*` takes exactly one segment.
    assert_eq!(
        publisher.publish("px.eur", &b"one"[..]).expect("publish"),
        1
    );
    assert_eq!(
        publisher
            .publish("px.eur.spot", &b"too deep"[..])
            .expect("publish"),
        0
    );
    assert_eq!(
        publisher.publish("px", &b"too short"[..]).expect("publish"),
        0
    );
    // No filter matches `fx` at all.
    assert_eq!(
        publisher
            .publish("fx.usd", &b"nobody"[..])
            .expect("publish"),
        0
    );
    // The boundary a byte prefix over-matched: `sensors.temp` must not select
    // `sensors.temperature`.
    assert_eq!(
        publisher
            .publish("sensors.temperature", &b"not mine"[..])
            .expect("publish"),
        0
    );
    assert_eq!(
        publisher
            .publish("sensors.temp", &b"mine"[..])
            .expect("publish"),
        1
    );
    // `ctl.#` takes the parent and everything under it.
    assert_eq!(
        publisher.publish("ctl", &b"parent"[..]).expect("publish"),
        1
    );
    assert_eq!(
        publisher
            .publish("ctl.end.now", &b"deep"[..])
            .expect("publish"),
        1
    );

    // The per-subscriber writer is FIFO, so the order below also proves the
    // unmatched topics were filtered out rather than merely late.
    assert_eq!(recv_one(&sub).await, ("px.eur".to_owned(), b"one".to_vec()));
    assert_eq!(
        recv_one(&sub).await,
        ("sensors.temp".to_owned(), b"mine".to_vec())
    );
    assert_eq!(recv_one(&sub).await, ("ctl".to_owned(), b"parent".to_vec()));
    assert_eq!(
        recv_one(&sub).await,
        ("ctl.end.now".to_owned(), b"deep".to_vec())
    );

    client.shutdown().await;
}

/// A published topic is data, not a pattern: its `*` is an ordinary byte.
#[tokio::test(start_paused = true)]
async fn a_topic_containing_a_wildcard_byte_is_literal() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.*")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    // `px.*` selects it as one segment, like any other one-segment topic.
    assert_eq!(publisher.publish("px.*", &b"star"[..]).expect("publish"), 1);
    assert_eq!(recv_one(&sub).await, ("px.*".to_owned(), b"star".to_vec()));

    client.shutdown().await;
}

/// A filter the grammar forbids fails locally instead of travelling to the
/// publisher, which would answer it by closing the connection.
#[tokio::test]
async fn an_illegal_filter_is_refused_before_it_reaches_the_wire() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    for bad in ["px*", "px.#.eur", "p*x"] {
        let err = within(sub.subscribe(bad))
            .await
            .expect_err("the grammar must refuse it");
        assert!(matches!(err, Error::Protocol(_)), "{bad}: {err:?}");
    }
    assert_eq!(sub.filter_count(), 0);

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn empty_filter_receives_all() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe all");
    await_filters(&publisher, 1).await;

    for topic in ["px.eur", "fx.usd", "anything"] {
        assert_eq!(publisher.publish(topic, &b"x"[..]).expect("publish"), 1);
    }
    for topic in ["px.eur", "fx.usd", "anything"] {
        assert_eq!(recv_one(&sub).await, (topic.to_owned(), b"x".to_vec()));
    }

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn two_subscribers_both_receive() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // Two runtimes, hence two connections: one subscriber per connection is
    // the supported shape, since a subscriber claims its path in the
    // connection's namespace.
    let client_a = server.client_runtime();
    let client_b = server.client_runtime();
    let a = client_a.subscriber(server.trust());
    let b = client_b.subscriber(server.trust());
    within(a.connect(&server.url("/md")))
        .await
        .expect("connect a");
    within(b.connect(&server.url("/md")))
        .await
        .expect("connect b");
    within(a.subscribe("px.#")).await.expect("subscribe a");
    within(b.subscribe("px.#")).await.expect("subscribe b");
    await_filters(&publisher, 2).await;
    assert_eq!(publisher.subscriber_count(), 2);

    assert_eq!(
        publisher.publish("px.eur", &b"tick"[..]).expect("publish"),
        2
    );
    assert_eq!(recv_one(&a).await, ("px.eur".to_owned(), b"tick".to_vec()));
    assert_eq!(recv_one(&b).await, ("px.eur".to_owned(), b"tick".to_vec()));

    client_a.shutdown().await;
    client_b.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unsubscribe_stops_delivery() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.#")).await.expect("subscribe px.#");
    within(sub.subscribe("ctl.#"))
        .await
        .expect("subscribe ctl.#");
    await_filters(&publisher, 2).await;

    within(sub.unsubscribe("px.#")).await.expect("unsubscribe");
    await_filters(&publisher, 1).await;

    assert_eq!(publisher.publish("px.x", &b"gone"[..]).expect("publish"), 0);
    assert_eq!(
        publisher.publish("ctl.end", &b"kept"[..]).expect("publish"),
        1
    );

    // Only the sentinel arrives; FIFO ordering makes this conclusive.
    assert_eq!(
        recv_one(&sub).await,
        ("ctl.end".to_owned(), b"kept".to_vec())
    );

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn late_publisher_receives_early_subscription() {
    let server = Server::start().await;
    // No publisher yet: the path is not registered at all.
    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.#")).await.expect("subscribe");

    // The publisher appears afterwards and still finds the subscription.
    let publisher = server.listener.publisher("/md").expect("publisher");
    await_filters(&publisher, 1).await;
    assert_eq!(
        publisher.publish("px.eur", &b"late"[..]).expect("publish"),
        1
    );
    assert_eq!(
        recv_one(&sub).await,
        ("px.eur".to_owned(), b"late".to_vec())
    );

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn slow_subscriber_drops_not_blocks() {
    const MSG: usize = 32 * 1024;
    const COUNT: usize = 100;

    // The byte budget is a *publisher-side* limit, so it belongs to the server
    // runtime that owns the registry. 64 KiB holds two messages.
    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: 64 * 1024,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // A subscriber that never reads only exerts backpressure once its QUIC
    // receive window fills, so the window has to be small enough to reach.
    // With a 128 KiB connection window, four unread messages stall the
    // publisher's writer for this subscriber; its budget is then never
    // returned and further publishes are dropped for it.
    let slow_rt = server.client_runtime_with(Limits {
        connection_receive_window: 128 * 1024,
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    });
    let fast_rt = server.client_runtime();
    let slow = slow_rt.subscriber(server.trust());
    let fast = fast_rt.subscriber(server.trust());
    within(slow.connect(&server.url("/md")))
        .await
        .expect("connect slow");
    within(fast.connect(&server.url("/md")))
        .await
        .expect("connect fast");
    within(slow.subscribe("")).await.expect("subscribe slow");
    within(fast.subscribe("")).await.expect("subscribe fast");
    await_filters(&publisher, 2).await;

    // Publish and drain the fast subscriber in lockstep: its budget is always
    // free before the next publish, so it can never be the one dropping.
    // The slow subscriber never reads.
    let payload = vec![0xa5u8; MSG];
    let mut fast_received = 0usize;
    // The whole loop sits inside the deadline: were `publish` to wait on the
    // slow subscriber, this would time out instead of dropping messages.
    // A second topic goes out first, while every budget is still free, so it
    // is delivered to both and never dropped — and its own count says so
    // after the other topic has starved.
    within(async {
        publisher.publish("fx.usd", &b"tiny"[..]).expect("publish");
        let (topic, _) = recv_one(&fast).await;
        assert_eq!(topic, "fx.usd");
        for _ in 0..COUNT {
            publisher
                .publish("px.eur", payload.clone())
                .expect("publish");
            let (topic, body) = recv_one(&fast).await;
            assert_eq!(topic, "px.eur");
            assert_eq!(body.len(), MSG);
            fast_received += 1;
        }
    })
    .await;

    assert_eq!(fast_received, COUNT, "the fast subscriber lost messages");
    assert!(
        publisher.dropped() > 0,
        "the slow subscriber should have lost messages"
    );
    // Which signal starved, and why: the count is per topic and per cause,
    // so a drop on `px.eur` is not a drop on `fx.usd`, and it was the byte
    // budget and not a full queue or a missing parked connection.
    let starved = publisher
        .dropped_on("px.eur")
        .expect("the dropped topic is counted");
    assert_eq!(starved.total(), publisher.dropped());
    assert!(starved.subscriber_budget > 0, "{starved:?}");
    assert_eq!(starved.subscriber_queue, 0, "{starved:?}");
    assert_eq!(starved.no_parked_connection, 0, "{starved:?}");
    assert_eq!(
        publisher.dropped_on("fx.usd"),
        None,
        "the other topic's count is untouched"
    );
    assert_eq!(publisher.drops().len(), 1);

    drop(fast);
    drop(slow);
    slow_rt.shutdown().await;
    fast_rt.shutdown().await;
}

/// A fan-out copy's metadata, including the trace context:
/// a publisher propagates one and never mints one
/// ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)).
///
/// Both directions are asserted on the same subscriber, because the claim is
/// the difference between the two calls and not either one alone: `publish`
/// carries no context, `publish_with_trace` carries exactly the caller's.
#[tokio::test(start_paused = true)]
async fn sub_meta_carries_topic_and_the_trace_the_publisher_propagated() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    publisher.publish("px.eur", &b"body"[..]).expect("publish");
    let transfer = within(sub.recv()).await.expect("recv");
    let meta = transfer.meta().clone();
    assert_eq!(meta.topic.as_deref(), Some("px.eur"));
    assert_eq!(meta.endpoint.as_deref(), Some("/md"));
    assert_eq!(meta.content_len, Some(4));
    assert!(
        meta.trace.is_none(),
        "a fan-out mints no trace context: it cost 60 bytes on every copy"
    );

    let trace = weida::new_trace();
    publisher
        .publish_with_trace("px.eur", &b"body"[..], trace)
        .expect("publish with a trace");
    let transfer = within(sub.recv()).await.expect("recv the traced copy");
    assert_eq!(
        transfer.meta().trace,
        Some(trace),
        "a propagated context reaches every subscriber verbatim"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn a_payload_larger_than_the_budget_is_refused() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // Nothing could ever enqueue this, so it is an error rather than a silent
    // drop for every subscriber.
    let oversized = vec![0u8; Limits::default().subscriber_buffer_bytes + 1];
    let err = publisher.publish("px.eur", oversized).unwrap_err();
    assert!(matches!(err, Error::LimitExceeded), "{err:?}");
}

/// The payload `publish` refuses is an ordinary streamed publish, and
/// two subscribers both get all of it.
#[tokio::test(start_paused = true)]
async fn a_streamed_publish_carries_a_payload_no_publish_could_take() {
    const BUDGET: usize = 64 * 1024;
    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 64;

    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: BUDGET,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // The whole payload is sixteen times the budget: `publish` cannot take
    // it at all, which is the premise of the item.
    let whole = vec![0xa5u8; CHUNK * CHUNKS];
    assert!(
        matches!(
            publisher.publish("px.eur", whole.clone()).unwrap_err(),
            Error::LimitExceeded
        ),
        "the whole-payload publish must still refuse what cannot be enqueued"
    );

    let first_rt = server.client_runtime();
    let second_rt = server.client_runtime();
    let first = first_rt.subscriber(server.trust());
    let second = second_rt.subscriber(server.trust());
    within(first.connect(&server.url("/md")))
        .await
        .expect("connect first");
    within(second.connect(&server.url("/md")))
        .await
        .expect("connect second");
    within(first.subscribe("px.#")).await.expect("subscribe");
    within(second.subscribe("px.#")).await.expect("subscribe");
    await_filters(&publisher, 2).await;

    // Both subscribers read concurrently, so the 64 KiB budget cycles while
    // the megabyte goes out.
    let readers = tokio::spawn(async move {
        let one = recv_one(&first).await;
        let two = recv_one(&second).await;
        (one, two)
    });

    let mut fan = publisher.open("px.eur");
    assert_eq!(fan.subscribers(), 2);
    assert_eq!(fan.topic(), "px.eur");
    let chunk = vec![0xa5u8; CHUNK];
    for _ in 0..CHUNKS {
        // The budget is a sixteenth of the payload, so this waits for room
        // repeatedly. Both subscribers are reading, so both keep the
        // transfer.
        let still = within(fan.write_within(chunk.clone(), DEADLINE))
            .await
            .expect("write");
        assert_eq!(still, 2, "a reading subscriber must not lose the transfer");
    }
    assert_eq!(fan.finish(), 2);

    let ((topic_one, body_one), (topic_two, body_two)) =
        within(readers).await.expect("both subscribers");
    assert_eq!(topic_one, "px.eur");
    assert_eq!(topic_two, "px.eur");
    assert_eq!(body_one.len(), CHUNK * CHUNKS);
    assert_eq!(body_two, body_one);
    assert_eq!(body_one, whole);
    assert_eq!(publisher.dropped(), 0, "nobody was behind");
}

/// The drop is per subscriber, not per publish. One subscriber that
/// never reads loses the streamed transfer; the other gets every byte, and
/// the publisher never waits for the one that stalled.
#[tokio::test(start_paused = true)]
async fn a_streamed_publish_drops_the_subscriber_that_stalls_and_keeps_the_other() {
    const BUDGET: usize = 64 * 1024;
    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 64;

    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: BUDGET,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // Small windows, so a subscriber that never reads stops taking bytes
    // instead of absorbing the payload in its transport.
    let slow_rt = server.client_runtime_with(Limits {
        connection_receive_window: 128 * 1024,
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    });
    let fast_rt = server.client_runtime();
    let slow = slow_rt.subscriber(server.trust());
    let fast = fast_rt.subscriber(server.trust());
    within(slow.connect(&server.url("/md")))
        .await
        .expect("connect slow");
    within(fast.connect(&server.url("/md")))
        .await
        .expect("connect fast");
    within(slow.subscribe("")).await.expect("subscribe slow");
    within(fast.subscribe("")).await.expect("subscribe fast");
    await_filters(&publisher, 2).await;

    let reader = tokio::spawn(async move { recv_one(&fast).await });

    // The whole loop is inside the deadline: a publisher that waited for the
    // stalled subscriber would time out here instead of dropping it.
    // A short per-chunk bound: the reading subscriber frees room inside it
    // every time, the one that never reads never does. Short enough that 64
    // chunks fit the test's own deadline even if every one of them waits.
    let squeeze = Duration::from_millis(100);
    let (remaining, delivered) = within(async {
        let mut fan = publisher.open("px.eur");
        assert_eq!(fan.subscribers(), 2);
        let chunk = vec![0x5au8; CHUNK];
        let mut remaining = 2;
        for _ in 0..CHUNKS {
            remaining = fan
                .write_within(chunk.clone(), squeeze)
                .await
                .expect("write");
        }
        let delivered = fan.finish();
        (remaining, delivered)
    })
    .await;

    assert_eq!(
        remaining, 1,
        "the subscriber that never read must lose this transfer and the other must keep it"
    );
    assert_eq!(delivered, 1);

    let (topic, body) = within(reader).await.expect("the reading subscriber");
    assert_eq!(topic, "px.eur");
    assert_eq!(body.len(), CHUNK * CHUNKS);
    assert!(
        publisher.dropped() >= 1,
        "the abandoned copy is counted like any other fan-out drop"
    );
    let drops = publisher
        .dropped_on("px.eur")
        .expect("the topic that lost a copy");
    assert_eq!(drops.total(), 1, "one copy, counted once: {drops:?}");
}

/// `write_now` is fan-out's `Drop` without a wait — the right call
/// where a later chunk supersedes an earlier one. A subscriber that is not
/// keeping up loses the transfer at the budget rather than slowing the
/// publisher by even a bounded wait.
#[tokio::test(start_paused = true)]
async fn a_streamed_publish_that_never_waits_drops_at_the_budget() {
    const BUDGET: usize = 64 * 1024;
    const CHUNK: usize = 16 * 1024;

    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: BUDGET,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime_with(Limits {
        connection_receive_window: 128 * 1024,
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    });
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    // Nobody reads, so the budget can only shrink. Every call returns at
    // once: the whole loop is inside the deadline, and a `write_within` here
    // would spend its bound on every chunk.
    let mut fan = publisher.open("px.eur");
    assert_eq!(fan.subscribers(), 1);
    let chunk = vec![0x11u8; CHUNK];
    let mut written = 0usize;
    within(async {
        while fan.write_now(chunk.clone()).expect("write") == 1 {
            written += 1;
        }
    })
    .await;

    // The subscriber's transport absorbs some of it, so the exact count is
    // the machine's; what is pinned is that the drop happened without a
    // wait and was counted once, for this topic.
    assert!(written >= 1, "the first chunks fit the budget");
    assert_eq!(fan.subscribers(), 0);
    assert_eq!(fan.finish(), 0);
    let drops = publisher
        .dropped_on("px.eur")
        .expect("the topic that lost the copy");
    assert_eq!(drops.total(), 1, "{drops:?}");
    assert_eq!(drops.subscriber_budget, 1);
}

#[tokio::test]
async fn publishing_to_nobody_is_not_an_error() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");
    assert_eq!(publisher.subscriber_count(), 0);
    assert_eq!(publisher.publish("px.eur", &b"x"[..]).expect("publish"), 0);
}

/// A publisher path accepts no inbound stream of either kind.
///
/// The 2 MiB payload is what makes the refusal deterministic: a transfer that
/// fits in flight can be acknowledged by the peer's *transport* before the
/// peer's *application* refuses it, and the receipt then truthfully says
/// "delivered", since it says nothing about the application. Past the stream
/// receive window the write cannot complete until the peer reads or refuses,
/// so the refusal is the only way out
/// (`docs/decisions/0005-refusal-race.md` §4.3).
#[tokio::test]
async fn a_publisher_path_refuses_inbound_transfers() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/md")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(weida::TransferMeta::default()))
        .await
        .expect("open");
    let payload = vec![0u8; 2 * 1024 * 1024];
    let refused = async {
        transfer.write_all(&payload).await?;
        transfer.finish()?.delivered().await
    };
    let err = within(refused)
        .await
        .expect_err("a push to a publisher path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/md")))
        .await
        .expect("connect");
    let err = within(requester.request(b"nope"))
        .await
        .expect_err("an exchange with a publisher path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    client.shutdown().await;
}

#[tokio::test]
async fn a_second_subscriber_on_one_connection_collides() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    // Both subscribers share the runtime's pooled connection and claim the
    // same path in its namespace. Refusing beats silently multiplexing two
    // subscribers onto one queue.
    let client = server.client_runtime();
    let first = client.subscriber(server.trust());
    within(first.connect(&server.url("/md")))
        .await
        .expect("connect first");
    let second = client.subscriber(server.trust());
    let err = within(second.connect(&server.url("/md")))
        .await
        .expect_err("the path is already claimed on this connection");
    assert!(matches!(err, Error::AlreadyRegistered), "{err:?}");

    client.shutdown().await;
}

/// `disconnect` forgets an address, so the subscriber must be able to dial it
/// again on the same pooled connection, and the publisher must stop sending
/// it copies once it is forgotten.
#[tokio::test]
async fn a_subscriber_can_dial_an_address_it_disconnected_again() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    let url = server.url("/md");
    within(sub.connect(&url)).await.expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    assert!(sub.disconnect(&url), "the address was known");
    await_filters(&publisher, 0).await;

    within(sub.connect(&url))
        .await
        .expect("a forgotten address can be dialled again");
    await_filters(&publisher, 1).await;
    publisher.publish("px.eur", &b"again"[..]).expect("publish");
    let (topic, body) = recv_one(&sub).await;
    assert_eq!(topic, "px.eur");
    assert_eq!(body, b"again");

    client.shutdown().await;
}

/// Detect mode over a real Pub/Sub drop: the subscriber that lost a message
/// to its byte budget sees the gap on the next one it receives.
///
/// This is the capability [decision 0001](../../docs/decisions/0001-sequence-field.md)
/// §7.2 required: before the sequence field, a drop was silent on the wire
/// and only the publisher counted it.
#[tokio::test(start_paused = true)]
async fn a_dropped_fan_out_copy_shows_up_as_a_gap() {
    const MSG: usize = 32 * 1024;

    let detect = GuaranteeSet {
        ordering: OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    // A budget that holds two messages, so a subscriber that stops reading
    // starts losing copies while the publisher keeps going.
    let server = Server::start_with_config(RuntimeConfig {
        limits: Limits {
            subscriber_buffer_bytes: 64 * 1024,
            ..Limits::default()
        },
        guarantees: detect,
        ..RuntimeConfig::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime_with_config(RuntimeConfig {
        limits: Limits {
            connection_receive_window: 128 * 1024,
            stream_receive_window: 64 * 1024,
            ..Limits::default()
        },
        guarantees: detect,
        ..RuntimeConfig::default()
    });
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.#")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    let payload = vec![0u8; MSG];
    // Publish until the publisher reports a drop for this subscriber: that is
    // the moment a copy was lost, and it is observed rather than assumed.
    let mut published = 0usize;
    within(async {
        while publisher.dropped() == 0 {
            publisher
                .publish("px.eur", payload.clone())
                .expect("publish");
            published += 1;
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(published > 1, "the first copy must have been deliverable");

    // Drain what did arrive. The lost copies are the last ones published, so
    // nothing seen so far can carry their gap yet.
    let lost = publisher.dropped();
    let mut gaps = Vec::new();
    let mut received = 0usize;
    while received < published - lost as usize {
        let transfer = within(sub.recv()).await.expect("recv");
        let meta = transfer.meta().clone();
        assert!(
            meta.sequence.is_some(),
            "detect mode numbers every fan-out copy"
        );
        if let Some(gap) = meta.gap {
            gaps.push(gap);
        }
        within(transfer.collect(MSG)).await.expect("collect");
        received += 1;
    }
    assert!(gaps.is_empty(), "nothing was missing yet: {gaps:?}");

    // The budget is free again, so this one is delivered — and it is the
    // message that reveals the hole the drops left.
    publisher
        .publish("px.eur", payload.clone())
        .expect("publish the sentinel");
    let sentinel = within(sub.recv()).await.expect("recv the sentinel");
    let gap = sentinel
        .meta()
        .gap
        .expect("the sentinel must carry the gap");
    assert_eq!(
        gap.missed(),
        lost,
        "the gap must name exactly the copies the publisher dropped: {gap:?}"
    );
    assert!(gap.seen > gap.expected, "{gap:?}");
    assert_eq!(
        publisher.dropped(),
        lost,
        "the sentinel must not be dropped"
    );

    client.shutdown().await;
}

/// Reassemble mode over the same drop: the hole is not reported when it
/// happens — the copies behind it are held, waiting for numbers that will
/// never arrive — and the bound is what makes it observable. At the cap the
/// oldest held copy is released out of order and carries the gap.
#[tokio::test(start_paused = true)]
async fn a_full_hold_reports_the_pub_sub_drop_it_was_waiting_for() {
    const MSG: usize = 32 * 1024;
    const HOLD: usize = 2;

    let reassemble = GuaranteeSet {
        ordering: OrderingMode::PerProducerReassemble,
        ..GuaranteeSet::CORE
    };
    let server = Server::start_with_config(RuntimeConfig {
        limits: Limits {
            subscriber_buffer_bytes: 64 * 1024,
            ..Limits::default()
        },
        guarantees: reassemble,
        ..RuntimeConfig::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // The receive window must hold the two parked copies *and* still have
    // room for the arrival that forces them out: a held transfer is an
    // unread stream, so it pins quinn's buffer until it is released.
    let client = server.client_runtime_with_config(RuntimeConfig {
        limits: Limits {
            connection_receive_window: 512 * 1024,
            stream_receive_window: 64 * 1024,
            max_reorder_hold: HOLD,
            ..Limits::default()
        },
        guarantees: reassemble,
        ..RuntimeConfig::default()
    });
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.#")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    let payload = vec![0u8; MSG];
    let mut published = 0usize;
    within(async {
        while publisher.dropped() == 0 {
            publisher
                .publish("px.eur", payload.clone())
                .expect("publish");
            published += 1;
            tokio::task::yield_now().await;
        }
    })
    .await;
    let lost = publisher.dropped();

    // Everything published before the hole is in order and arrives
    // untouched, which also frees the byte budget again.
    for _ in 0..published - lost as usize {
        let transfer = within(sub.recv()).await.expect("recv");
        assert_eq!(transfer.meta().gap, None, "nothing is missing yet");
        within(transfer.collect(MSG)).await.expect("collect");
    }

    // Keep publishing. Every copy after the hole is held — the numbers it
    // waits for were dropped at the publisher and will never arrive — so
    // nothing reaches the application until the hold is full, and then the
    // oldest held copy comes out carrying the gap. Publishing in a loop
    // rather than exactly `HOLD + 1` times keeps the test honest about a
    // slow writer: a sentinel that is itself dropped only widens the hole.
    let mut sequences = Vec::new();
    let gap = within(async {
        loop {
            publisher
                .publish("px.eur", payload.clone())
                .expect("publish");
            tokio::task::yield_now().await;
            let Ok(transfer) = tokio::time::timeout(Duration::from_millis(20), sub.recv()).await
            else {
                continue;
            };
            let transfer = transfer.expect("recv");
            let meta = transfer.meta().clone();
            sequences.push(meta.sequence.expect("fan-out copies are numbered"));
            transfer.collect(MSG).await.expect("collect");
            if let Some(gap) = meta.gap {
                break gap;
            }
        }
    })
    .await;

    assert!(
        gap.missed() >= lost,
        "the gap must cover the copies the publisher dropped: {gap:?}, dropped {}",
        publisher.dropped()
    );
    assert!(
        gap.missed() <= publisher.dropped(),
        "the gap must not invent losses: {gap:?}, dropped {}",
        publisher.dropped()
    );
    assert!(
        sequences.windows(2).all(|pair| pair[0] < pair[1]),
        "reassemble mode never delivers backwards: {sequences:?}"
    );

    client.shutdown().await;
}

/// A receiver that drops a message unread — the header read, the body not —
/// stops that one transfer with `STOP_SENDING`. The subscription is the
/// connection's, not the message's: the next message must still arrive.
#[tokio::test(start_paused = true)]
async fn a_subscriber_that_refuses_one_message_keeps_its_subscription() {
    // Larger than the 1 MiB stream window, so the publisher is still writing
    // when the refusal reaches it.
    const BIG: usize = 3 * 1024 * 1024;

    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");
    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    assert_eq!(
        publisher.publish("big", vec![0u8; BIG]).expect("publish"),
        1
    );
    drop(within(sub.recv()).await.expect("the big message arrives"));

    assert_eq!(
        publisher
            .publish("small", &b"0123456789"[..])
            .expect("publish"),
        1
    );
    assert_eq!(
        recv_one(&sub).await,
        ("small".to_owned(), b"0123456789".to_vec()),
        "the refused message ended itself, not the subscription"
    );
    assert_eq!(
        publisher.dropped(),
        0,
        "a refusal is the subscriber's choice, not a drop"
    );
    assert_eq!(publisher.subscriber_count(), 1);

    client.shutdown().await;
}

/// A topic a subscriber's decoder refuses is refused at the publisher, with
/// nothing written: the decoder's answer would be to close the connection,
/// and the connection is every subscription on it.
#[tokio::test(start_paused = true)]
async fn a_topic_above_the_cap_is_refused_and_no_connection_is_closed() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");
    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    let long = "t".repeat(257);
    assert!(matches!(
        publisher.publish(&long, &b"x"[..]),
        Err(Error::LimitExceeded)
    ));
    let mut fan = publisher.open(&long);
    assert!(matches!(
        fan.write_now(&b"x"[..]),
        Err(Error::LimitExceeded)
    ));
    assert_eq!(fan.finish(), 0);

    // Still connected, and still receiving: a topic at the cap is fine.
    let at_the_cap = "t".repeat(256);
    assert_eq!(
        publisher.publish(&at_the_cap, &b"ok"[..]).expect("publish"),
        1
    );
    assert_eq!(recv_one(&sub).await, (at_the_cap, b"ok".to_vec()));

    client.shutdown().await;
}

/// Two publishers of one listener that use one topic string are two
/// producers: a subscriber of one must see its own numbers, dense, and no
/// gap for the other's messages.
#[tokio::test(start_paused = true)]
async fn two_publishers_of_one_topic_number_it_separately() {
    let detect = GuaranteeSet {
        ordering: OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    let config = RuntimeConfig {
        guarantees: detect,
        ..RuntimeConfig::default()
    };
    let server = Server::start_with_config(config.clone()).await;
    let a = server.listener.publisher("/a").expect("publisher a");
    let b = server.listener.publisher("/b").expect("publisher b");

    let client_a = server.client_runtime_with_config(config.clone());
    let client_b = server.client_runtime_with_config(config);
    let sub_a = client_a.subscriber(server.trust());
    let sub_b = client_b.subscriber(server.trust());
    within(sub_a.connect(&server.url("/a")))
        .await
        .expect("connect a");
    within(sub_b.connect(&server.url("/b")))
        .await
        .expect("connect b");
    within(sub_a.subscribe("#")).await.expect("subscribe a");
    within(sub_b.subscribe("#")).await.expect("subscribe b");
    await_filters(&a, 1).await;
    await_filters(&b, 1).await;

    for _ in 0..4 {
        assert_eq!(a.publish("t", &b"a"[..]).expect("publish a"), 1);
        assert_eq!(b.publish("t", &b"b"[..]).expect("publish b"), 1);
    }
    for (name, sub) in [("a", &sub_a), ("b", &sub_b)] {
        for expected in 0..4u64 {
            let transfer = within(sub.recv()).await.expect("recv");
            let meta = transfer.meta().clone();
            assert_eq!(meta.sequence, Some(expected), "{name}'s own numbering");
            assert_eq!(meta.gap, None, "{name} lost nothing: {meta:?}");
            within(transfer.collect(16)).await.expect("collect");
        }
    }

    client_a.shutdown().await;
    client_b.shutdown().await;
}

/// Subscribers that stall are waited for together: the bound of
/// `write_within` is for the call, so eight stalled subscribers cost one
/// bound and not eight.
#[tokio::test(start_paused = true)]
async fn stalled_subscribers_are_waited_for_together() {
    const BUDGET: usize = 64 * 1024;
    const CHUNK: usize = 16 * 1024;
    const SUBSCRIBERS: usize = 8;
    const BOUND: Duration = Duration::from_millis(250);

    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: BUDGET,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // One runtime each, so that each is its own connection; small windows,
    // so that a subscriber that never reads stops taking bytes at once.
    let mut clients = Vec::new();
    let mut subs = Vec::new();
    for _ in 0..SUBSCRIBERS {
        let client = server.client_runtime_with(Limits {
            connection_receive_window: 128 * 1024,
            stream_receive_window: 64 * 1024,
            ..Limits::default()
        });
        let sub = client.subscriber(server.trust());
        within(sub.connect(&server.url("/md")))
            .await
            .expect("connect");
        within(sub.subscribe("")).await.expect("subscribe");
        clients.push(client);
        subs.push(sub);
    }
    await_filters(&publisher, SUBSCRIBERS).await;

    let mut fan = publisher.open("px.eur");
    assert_eq!(fan.subscribers(), SUBSCRIBERS);
    let started = tokio::time::Instant::now();
    let chunk = vec![0x5au8; CHUNK];
    within(async {
        while fan.subscribers() > 0 {
            fan.write_within(chunk.clone(), BOUND).await.expect("write");
        }
    })
    .await;
    let elapsed = started.elapsed();

    // Every subscriber stalls at the same chunk, and the call that finds
    // them out waits for all of them together: one bound, plus slack. One
    // after the other it would take eight.
    assert!(
        elapsed < BOUND * 4,
        "{SUBSCRIBERS} stalled subscribers took {elapsed:?} against a bound of {BOUND:?}"
    );
    assert_eq!(fan.finish(), 0);
    assert_eq!(
        publisher
            .dropped_on("px.eur")
            .expect("drops")
            .subscriber_budget,
        SUBSCRIBERS as u64
    );

    for client in clients {
        client.shutdown().await;
    }
}

/// A chunk no budget can ever hold is refused at once, instead of every
/// copy waiting for room that cannot exist and then being aborted.
#[tokio::test(start_paused = true)]
async fn a_chunk_above_the_budget_is_refused_without_waiting() {
    const BUDGET: usize = 64 * 1024;

    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: BUDGET,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");
    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    let mut fan = publisher.open("px.eur");
    let started = tokio::time::Instant::now();
    assert!(matches!(
        fan.write_within(vec![0u8; BUDGET + 1], Duration::from_secs(5))
            .await,
        Err(Error::LimitExceeded)
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "refused up front, not after a wait"
    );
    assert!(matches!(
        fan.write_now(vec![0u8; BUDGET + 1]),
        Err(Error::LimitExceeded)
    ));
    assert_eq!(fan.subscribers(), 1, "nothing was aborted");
    assert_eq!(
        fan.write_now(vec![0u8; 1024]).expect("a chunk that fits"),
        1
    );
    assert_eq!(fan.finish(), 1);

    client.shutdown().await;
}
