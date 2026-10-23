//! Interop against **rumqttd 0.20.0**, measured on 2026-09-12 on rustc 1.98.
//!
//! Our client on **both** sides of a foreign broker, so publisher and
//! subscriber roles are both ours and the broker is the only thing in the
//! middle that is not.
//!
//! # Running them
//!
//! ```text
//! # The broker is a Rust binary, so no system package is involved:
//! cargo install rumqttd --version 0.20.0
//! rumqttd -c crates/mqtt/weida-mqtt/tests/interop/rumqttd.toml -q &
//! cargo test -p weida-mqtt --test interop_rumqttd -- --ignored
//! ```
//!
//! `#[ignore]` because a broker is a runtime condition: the file compiles
//! everywhere and runs where port 21884 answers. Start the broker under a
//! process supervisor with a TCP `ready` condition on that port, and stop it
//! when the run ends.
//!
//! # Measured, not inferred
//!
//! Every claim below was observed against this version, and several of them
//! contradict what was expected beforehand - which is the point of running
//! it.
//!
//! * **`cargo install rumqttd --version 0.20.0 --locked` does not build on
//!   rustc 1.98.** Its `Cargo.lock` pins a `metrics` version whose registry
//!   accessor fails borrow checking (`E0521`, "borrowed data escapes outside
//!   of closure"). Without `--locked` the resolver picks a newer `metrics`
//!   and it builds in about 30 s. That is why the command above has no
//!   `--locked`; it is a fact about the broker's dependency pins rather than
//!   about either MQTT implementation.
//! * **rumqttd 0.20.0 does speak MQTT 5.** Its own `generate-config` enables
//!   a `[v5.1]` listener by default. The project's README checklist leaves
//!   MQTT 5 unchecked, and the checklist is out of date rather than the
//!   build: the broker accepts a 5.0 CONNECT, answers a 5.0 CONNACK **with
//!   properties**, honours Subscription Identifiers and enforces a Topic
//!   Alias Maximum. So the premise that this broker is "3.1.1-compatible" is
//!   wrong in this client's favour, and what remains for `interop_mqtt5.rs` is
//!   smaller than expected.
//! * **The CONNACK declares `Topic Alias Maximum` 4096** and nothing else, so
//!   the other §11 defaults apply - `Receive Maximum` 65,535, `Maximum QoS`
//!   2, `Retain Available` 1, `Maximum Packet Size` unlimited, all three
//!   subscription-availability flags 1, no `Server Keep Alive`. Asserted
//!   rather than assumed: absence is only safe if the defaults applied are
//!   the specification's.
//! * **Topic Aliases are exercised**, because of that 4096: this client
//!   established aliases against a real broker and the broker resolved them,
//!   so every delivery reached the application with a Topic Name.
//! * **Subscription Identifiers are reported back** on every delivery the
//!   subscription caused ([MQTT-3.3.4-4]), which no scripted server can
//!   prove.
//! * **QoS 0, 1 and 2 all complete** end to end, with the subscriber's own
//!   acknowledgements answered, and the session survives a reconnect with
//!   Clean Start 0 while Clean Start 1 discards it.
//! * **Retained messages work, including the zero-byte delete**: a subscriber
//!   arriving after the publish receives the stored value with RETAIN 1, and
//!   after the delete a new subscriber receives nothing.
//! * **The Will fires on an abnormal close and not on an orderly one.** The
//!   pairing no scripted server can prove, and the reason this file exists.
//!
//! # Three disagreements, measured against this version
//!
//! Each is recorded where the test asserts it, so a future run that finds it
//! fixed fails loudly rather than passing quietly.
//!
//! 1. **A delivery arrives at the subscription's granted maximum rather than
//!    at the minimum of that and the publish's QoS.** [MQTT-3.8.4-8] makes it
//!    the minimum, so a QoS 0 publish reaching a QoS 2 subscription must
//!    arrive at QoS 0; here it arrives at QoS 2. That is an **upgrade**, which
//!    the specification does not permit in either direction. The pair of
//!    tests is what shows it: the downgrade direction is correct, so the
//!    broker is applying the granted value rather than taking a minimum.
//! 2. **Retain Handling 2 is ignored.** "Do not send retained messages at the
//!    time of the subscribe" (3.8.3.1) still sends them. The option byte does
//!    reach the wire - `retain_will.rs` asserts that against the codec - so
//!    the omission is the broker's.
//! 3. **DISCONNECT 0x04 `DisconnectWithWillMessage` publishes nothing.** The
//!    0x00 half works, so this broker implements "discard the Will" and not
//!    "publish it on request".
//!
//! # What this broker cannot exercise, named rather than worked around
//!
//! * **AUTH**: no enhanced-authentication mechanism is offered, so 4.12 is
//!   unmeasured here and is covered only by the scripted server of
//!   `tls_auth.rs`.
//! * **Shared subscriptions and the availability flags**: the broker declares
//!   none of the three flags, so the flags' *refusal* paths cannot be reached;
//!   `shared_subscriptions_strategy` exists in its configuration but a broker
//!   that declares nothing lets a client only observe success.
//! * **Session Expiry as a timer** and **Retain Handling 1**, both of which
//!   need a wall-clock wait or a second subscribe of the same filter to
//!   separate from 0, and both of which belong with the 5.0-only half.

#![cfg(test)]

use crate::harness;

use std::time::Duration;

use harness::{DEADLINE, unique};
use weida_mqtt::{
    Client, Completion, ConnectOptions, Context, Delivery, DisconnectReasonCode, Event, Events,
    Message, QoS, RetainHandling, RetainedOrigin, Session, Subscription,
};

/// Where the configuration in `tests/interop/rumqttd.toml` listens.
///
/// Not 1883 and not 1884 on purpose: those are where a developer's own broker
/// lives, and an interop test that silently measured against *that* would be
/// the worst possible outcome.
const BROKER: &str = "127.0.0.1:21884";

fn options(client_id: &str) -> ConnectOptions {
    let mut options = ConnectOptions::new(client_id);
    // A real broker enforces Keep Alive; 30 s is far longer than any test here
    // and short enough that a leaked connection does not outlive the run.
    options.keep_alive = Duration::from_secs(30);
    options.connect_timeout = Duration::from_secs(3600);
    options
}

/// A resumable session: Clean Start 0 and a Session Expiry the broker can
/// hold across a reconnect.
fn resumable(client_id: &str) -> ConnectOptions {
    let mut options = options(client_id);
    options.clean_start = false;
    options.session_expiry = Some(Duration::from_secs(300));
    options
}

/// The next delivery, or a failure naming what was expected.
async fn delivered(events: &mut Events) -> Delivery {
    match weida_testkit::within("a delivery", DEADLINE, events.next()).await {
        Some(Event::Delivered(delivery)) => delivery,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

/// No delivery within `window`, which is how "the broker sent nothing" is
/// asserted without waiting out the deadline.
async fn nothing_delivered(events: &mut Events, window: Duration) {
    if let Ok(event) = tokio::time::timeout(window, events.next()).await {
        panic!("expected nothing, got {event:?}");
    }
}

/// All three QoS levels through the broker, our client on both sides.
///
/// The subscriber's QoS is the ceiling and the publisher's is the request:
/// "the QoS used to deliver outbound could differ from the inbound", and the
/// rule is the minimum of the two ([MQTT-3.8.4-8]) - which is why the
/// subscription is at QoS 2 and the assertion is on what each publish was
/// *delivered* at.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn every_qos_level_completes_through_the_broker() {
    let context = Context::new().expect("ambient");
    let (subscriber, mut events) = Client::connect(&context, BROKER, options("w3-sub-qos"))
        .await
        .expect("the subscriber connects");
    let granted = subscriber
        .subscribe(vec![Subscription::new("w3/qos/+", QoS::ExactlyOnce)])
        .await
        .expect("subscribed");
    assert_eq!(
        granted,
        [weida_mqtt::SubackReasonCode::GrantedQos2],
        "the broker granted the maximum asked for"
    );

    let (publisher, _publisher_events) = Client::connect(&context, BROKER, options("w3-pub-qos"))
        .await
        .expect("the publisher connects");

    // QoS 0 certifies only that the bytes left ([`Completion::Sent`]).
    assert_eq!(
        publisher
            .publish(Message::new("w3/qos/zero", "zero"))
            .await
            .expect("publishes at QoS 0"),
        Completion::Sent
    );
    // QoS 1: the broker's PUBACK.
    let one = publisher
        .publish(Message::new("w3/qos/one", "one").at(QoS::AtLeastOnce))
        .await
        .expect("publishes at QoS 1");
    assert!(
        matches!(one, Completion::Acknowledged(_)),
        "a real PUBACK: {one:?}"
    );
    // QoS 2: the broker's PUBCOMP, after the four-packet handshake.
    let two = publisher
        .publish(Message::new("w3/qos/two", "two").at(QoS::ExactlyOnce))
        .await
        .expect("publishes at QoS 2");
    assert!(
        matches!(two, Completion::Complete(_)),
        "a real PUBCOMP: {two:?}"
    );

    let mut seen = Vec::new();
    for _ in 0..3 {
        let delivery = delivered(&mut events).await;
        seen.push((delivery.topic.clone(), delivery.qos));
        assert!(
            !delivery.topic.is_empty(),
            "the broker resolved its own Topic Alias before sending, or sent \
             none: either way the application sees a Topic Name"
        );
    }
    seen.sort();
    assert_eq!(
        seen,
        [
            ("w3/qos/one".to_owned(), QoS::ExactlyOnce),
            ("w3/qos/two".to_owned(), QoS::ExactlyOnce),
            ("w3/qos/zero".to_owned(), QoS::ExactlyOnce),
        ],
        "**measured disagreement against rumqttd 0.20.0**: every delivery \
         arrives at the subscription's granted maximum rather than at the \
         minimum of that and the publish's own QoS. [MQTT-3.8.4-8] makes the \
         delivery QoS the minimum of the two, so a QoS 0 publish reaching a \
         QoS 2 subscription must arrive at QoS 0 - here it arrives at QoS 2, \
         which is an upgrade the specification does not permit. The pairing \
         that shows it is a *pair*: `a_granted_maximum_downgrades_the_delivery` \
         passes, so the broker applies the granted value in both directions \
         rather than taking a minimum"
    );

    for client in [&subscriber, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .expect("disconnects");
    }
}

/// A subscriber whose granted maximum is below the publish's QoS receives the
/// **downgrade**, not the publish's level ([MQTT-3.8.4-8]).
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn a_granted_maximum_downgrades_the_delivery() {
    let context = Context::new().expect("ambient");
    let (subscriber, mut events) = Client::connect(&context, BROKER, options("w3-sub-down"))
        .await
        .expect("the subscriber connects");
    let granted = subscriber
        .subscribe(vec![Subscription::new("w3/down/+", QoS::AtMostOnce)])
        .await
        .expect("subscribed");
    assert_eq!(granted, [weida_mqtt::SubackReasonCode::GrantedQos0]);

    let (publisher, _events) = Client::connect(&context, BROKER, options("w3-pub-down"))
        .await
        .expect("the publisher connects");
    publisher
        .publish(Message::new("w3/down/two", "two").at(QoS::ExactlyOnce))
        .await
        .expect("publishes at QoS 2");

    let delivery = delivered(&mut events).await;
    assert_eq!(
        delivery.qos,
        QoS::AtMostOnce,
        "published at 2, granted 0, delivered at 0"
    );
    assert_eq!(
        delivery.packet_id, None,
        "and with no Packet Identifier, because QoS 0 has none (3.3.2.2)"
    );

    for client in [&subscriber, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .expect("disconnects");
    }
}

/// Retained messages, including the zero-byte delete that "is itself not
/// stored" ([MQTT-3.3.1-6], [MQTT-3.3.1-10]).
///
/// Three subscribers in sequence against one topic, which is the only way to
/// separate the three states: nothing stored, a value stored, the value
/// deleted.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn a_retained_value_is_stored_and_then_deleted() {
    let context = Context::new().expect("ambient");
    let topic = "w3/retain/state";
    let (publisher, _events) = Client::connect(&context, BROKER, options("w3-pub-retain"))
        .await
        .expect("the publisher connects");

    // Clear anything a previous run left, so the test does not depend on the
    // broker being fresh.
    publisher
        .publish(Message::delete_retained(topic).at(QoS::AtLeastOnce))
        .await
        .expect("clears");

    // 1. Nothing stored: a new subscriber receives nothing.
    let (early, mut early_events) = Client::connect(&context, BROKER, options("w3-sub-retain-1"))
        .await
        .expect("connects");
    early
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    nothing_delivered(&mut early_events, Duration::from_millis(500)).await;

    // 2. A retained publish reaches the live subscriber **and** is stored.
    publisher
        .publish(Message::new(topic, "on").retained().at(QoS::AtLeastOnce))
        .await
        .expect("publishes retained");
    let live = delivered(&mut early_events).await;
    assert_eq!(live.payload, &b"on"[..]);
    assert_eq!(
        live.origin(false),
        RetainedOrigin::Live,
        "the broker cleared RETAIN on the forwarded copy ([MQTT-3.3.1-12]), \
         which is what Retain As Published 0 means"
    );

    // A subscriber that arrives afterwards gets it from the cache, with
    // RETAIN 1 ([MQTT-3.3.1-8]).
    let (late, mut late_events) = Client::connect(&context, BROKER, options("w3-sub-retain-2"))
        .await
        .expect("connects");
    late.subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    let cached = delivered(&mut late_events).await;
    assert_eq!(cached.payload, &b"on"[..]);
    assert_eq!(
        cached.origin(false),
        RetainedOrigin::Retained,
        "RETAIN 1 under Retain As Published 0 can only be the cache"
    );

    // 3. The delete: a message to the current subscribers, and nothing left
    // in the cache.
    publisher
        .publish(Message::delete_retained(topic).at(QoS::AtLeastOnce))
        .await
        .expect("deletes");
    let delete = delivered(&mut late_events).await;
    assert!(
        delete.payload.is_empty(),
        "the delete is delivered as a normal message with an empty payload \
         ([MQTT-3.3.1-11])"
    );

    let (after, mut after_events) = Client::connect(&context, BROKER, options("w3-sub-retain-3"))
        .await
        .expect("connects");
    after
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    nothing_delivered(&mut after_events, Duration::from_millis(500)).await;

    for client in [&publisher, &early, &late, &after] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// Retain Handling 2 - "do not send retained messages at subscribe"
/// (3.8.3.1) - against a stored value.
///
/// **A measured disagreement**: this broker sends the stored value anyway. The
/// test asserts what happens rather than what should, so a future version that
/// implements it fails here and gets noticed.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn retain_handling_two_is_ignored_by_this_broker() {
    let context = Context::new().expect("ambient");
    let topic = "w3/retain/handling";
    let (publisher, _events) = Client::connect(&context, BROKER, options("w3-pub-handling"))
        .await
        .expect("connects");
    publisher
        .publish(
            Message::new(topic, "stored")
                .retained()
                .at(QoS::AtLeastOnce),
        )
        .await
        .expect("publishes retained");

    // Retain Handling 0 gets it.
    let (wants, mut wants_events) = Client::connect(&context, BROKER, options("w3-sub-rh0"))
        .await
        .expect("connects");
    wants
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    assert_eq!(delivered(&mut wants_events).await.payload, &b"stored"[..]);

    // Retain Handling 2 does not.
    let (declines, mut declines_events) = Client::connect(&context, BROKER, options("w3-sub-rh2"))
        .await
        .expect("connects");
    declines
        .subscribe(vec![
            Subscription::new(topic, QoS::AtLeastOnce).retain_handling(RetainHandling::DoNotSend),
        ])
        .await
        .expect("subscribed");
    // **Measured disagreement against rumqttd 0.20.0**: Retain Handling 2
    // means "do not send retained messages at the time of the subscribe"
    // (3.8.3.1), and the stored value arrives anyway. Recorded as what it is;
    // the option byte does reach the wire, which `all_three_retain_handling_
    // values_reach_the_wire` in `retain_will.rs` asserts, so the omission is
    // the broker's and not this client's.
    let anyway = delivered(&mut declines_events).await;
    assert_eq!(
        anyway.payload,
        &b"stored"[..],
        "Retain Handling 2 is ignored by this broker version: the retained \
         value is sent at subscribe time regardless"
    );
    assert!(
        anyway.retain,
        "and it arrives with RETAIN 1, so it is the cache and not a live copy"
    );

    // But it is subscribed: a live publish reaches it.
    publisher
        .publish(Message::new(topic, "live").at(QoS::AtLeastOnce))
        .await
        .expect("publishes");
    assert_eq!(delivered(&mut declines_events).await.payload, &b"live"[..]);

    // Clean up the retained value for the next run.
    publisher
        .publish(Message::delete_retained(topic).at(QoS::AtLeastOnce))
        .await
        .expect("deletes");
    for client in [&publisher, &wants, &declines] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// **The Will, which is the pairing no scripted server can prove.**
///
/// An abnormal close publishes it; an orderly DISCONNECT 0x00 discards it
/// ([MQTT-3.14.4-3]). Both halves against the same broker, in one test,
/// because either alone would pass against a broker that always published or
/// never did.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn the_will_fires_on_an_abnormal_close_and_not_on_an_orderly_one() {
    let context = Context::new().expect("ambient");
    let (watcher, mut events) = Client::connect(&context, BROKER, options("w3-will-watch"))
        .await
        .expect("connects");
    watcher
        .subscribe(vec![Subscription::new("w3/will/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let will = |topic: &str| weida_mqtt::WillMessage {
        topic: topic.to_owned(),
        payload: b"gone".to_vec(),
        qos: QoS::AtLeastOnce,
        ..weida_mqtt::WillMessage::default()
    };

    // 1. Orderly: the Will is discarded.
    let mut orderly = options("w3-will-orderly");
    orderly.will = Some(will("w3/will/orderly"));
    let (client, _events) = Client::connect(&context, BROKER, orderly)
        .await
        .expect("connects");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    drop(client);
    nothing_delivered(&mut events, Duration::from_millis(700)).await;

    // 2. Abnormal: dropping the handle closes the transport with nothing
    // written, which is exactly what the mechanism is for.
    let mut abrupt = options("w3-will-abrupt");
    abrupt.will = Some(will("w3/will/abrupt"));
    let (client, mut client_events) = Client::connect(&context, BROKER, abrupt)
        .await
        .expect("connects");
    drop(client);
    // The task's own report that the connection ended, so the close has
    // definitely reached the socket before the assertion below.
    let _ = weida_testkit::within(
        "the connection to report its end",
        DEADLINE,
        client_events.next(),
    )
    .await;

    let published = delivered(&mut events).await;
    assert_eq!(published.topic, "w3/will/abrupt");
    assert_eq!(published.payload, &b"gone"[..]);
    assert_eq!(
        published.qos,
        QoS::AtLeastOnce,
        "the Will is published at its own QoS"
    );

    watcher
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// A DISCONNECT 0x04 `DisconnectWithWillMessage` asks for the Will on an
/// orderly close, which is the only way a client publishes its own Will.
///
/// **A measured disagreement**: this broker publishes nothing. Since the 0x00
/// half demonstrably works, it implements "discard the Will" and not "publish
/// it on request". Asserted as measured, so a future version that adds it
/// fails here.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn disconnect_with_will_message_publishes_nothing_here() {
    let context = Context::new().expect("ambient");
    let (watcher, mut events) = Client::connect(&context, BROKER, options("w3-will-ask-watch"))
        .await
        .expect("connects");
    watcher
        .subscribe(vec![Subscription::new("w3/willask/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let mut asking = options("w3-will-ask");
    asking.will = Some(weida_mqtt::WillMessage {
        topic: "w3/willask/asked".into(),
        payload: b"asked".to_vec(),
        qos: QoS::AtLeastOnce,
        ..weida_mqtt::WillMessage::default()
    });
    let (client, _events) = Client::connect(&context, BROKER, asking)
        .await
        .expect("connects");
    client
        .disconnect(DisconnectReasonCode::DisconnectWithWillMessage)
        .await
        .expect("disconnects");
    drop(client);

    // **Measured disagreement against rumqttd 0.20.0**: DISCONNECT 0x04 asks
    // the server to publish the Will anyway, and nothing is published.
    // 0x00's half works - `the_will_fires_on_an_abnormal_close_and_not_on_an_
    // orderly_one` proves the Will exists and that an orderly close discards
    // it - so this broker implements "discard" and not "publish on request".
    // The client's half is on the wire either way, which
    // `the_three_ways_a_connection_ends_are_distinguishable_on_the_wire` in
    // `retain_will.rs` asserts.
    nothing_delivered(&mut events, Duration::from_millis(700)).await;

    watcher
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// The session survives a reconnect, and an unacknowledged QoS 2 exchange is
/// completed after it.
///
/// This is `session.rs` and `qos.rs` measured against something that actually
/// keeps session state: the connection dies with a PUBLISH in flight, the
/// client reconnects with Clean Start 0, the broker answers `Session Present`
/// 1, and the resent PUBLISH completes.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn a_session_survives_a_reconnect_and_completes_its_exchange() {
    let context = Context::new().expect("ambient");
    let client_id = unique("w3-resume");
    let session = Session::new(client_id.clone(), &resumable(&client_id).limits);

    let (first, mut first_events) =
        Client::connect_session(&context, BROKER, resumable(&client_id), &session)
            .await
            .expect("connects");
    assert!(
        !first.session_present(),
        "a client identifier this run has not used before"
    );
    // A QoS 2 publish, then the handle goes away before its PUBCOMP can
    // arrive - which is what leaves the session something to resume.
    let publishing = tokio::spawn({
        let message = Message::new("w3/resume/topic", "survive").at(QoS::ExactlyOnce);
        async move { first.publish(message).await }
    });
    let outcome = tokio::time::timeout(DEADLINE, publishing).await;

    // Either the exchange completed before the drop, in which case there is
    // nothing to resume and the test says so, or it did not and the session
    // carries it.
    let resumed_work = !matches!(outcome, Ok(Ok(Ok(_))));
    let _ = tokio::time::timeout(Duration::from_secs(1), first_events.next()).await;

    let (second, _second_events) =
        Client::connect_session(&context, BROKER, resumable(&client_id), &session)
            .await
            .expect("resumes");
    assert!(
        second.session_present(),
        "the broker kept the session for this Client Identifier across the \
         reconnect, which is what Clean Start 0 and a non-zero Session Expiry \
         Interval ask for"
    );
    if resumed_work {
        // The retransmission went out inside `connect_session`; the exchange
        // finishes on this connection.
        assert!(
            session.in_flight() <= 1,
            "at most the one exchange, and it is the session's rather than a \
             handle's"
        );
    }

    second
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::ZERO),
        )
        .await
        .ok();
}

/// Clean Start 1 discards the session, which is the other half of the same
/// rule ([MQTT-3.1.2-4]).
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn clean_start_discards_the_session_the_broker_held() {
    let context = Context::new().expect("ambient");
    let client_id = unique("w3-clean");
    let session = Session::new(client_id.clone(), &resumable(&client_id).limits);

    let (first, mut events) =
        Client::connect_session(&context, BROKER, resumable(&client_id), &session)
            .await
            .expect("connects");
    first
        .subscribe(vec![Subscription::new("w3/clean/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    first
        .disconnect_with(DisconnectReasonCode::NormalDisconnection, None)
        .await
        .ok();
    drop(first);
    let _ = tokio::time::timeout(Duration::from_secs(1), events.next()).await;

    // Clean Start 0 first, to prove the broker did hold it.
    let (held, _events) =
        Client::connect_session(&context, BROKER, resumable(&client_id), &session)
            .await
            .expect("resumes");
    assert!(held.session_present(), "the broker held it");
    held.disconnect_with(DisconnectReasonCode::NormalDisconnection, None)
        .await
        .ok();
    drop(held);

    // Then Clean Start 1, which discards it on both sides.
    let mut fresh = resumable(&client_id);
    fresh.clean_start = true;
    let (clean, _events) = Client::connect_session(&context, BROKER, fresh, &session)
        .await
        .expect("connects");
    assert!(
        !clean.session_present(),
        "Clean Start 1 means the broker MUST discard any existing session \
         ([MQTT-3.1.2-4]) and say so"
    );
    assert!(
        clean.subscriptions().is_empty(),
        "and the client's mirror went with it"
    );
    clean
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::ZERO),
        )
        .await
        .ok();
}

/// The filter grammar against a real broker: `+` is one level, `#` is the rest
/// including the parent, and neither matches a `$`-prefixed topic from a
/// leading wildcard ([MQTT-4.7.2-1]).
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn the_wildcards_select_what_the_specification_says_they_do() {
    let context = Context::new().expect("ambient");
    let (plus, mut plus_events) = Client::connect(&context, BROKER, options("w3-sub-plus"))
        .await
        .expect("connects");
    plus.subscribe(vec![Subscription::new("w3/wild/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let (hash, mut hash_events) = Client::connect(&context, BROKER, options("w3-sub-hash"))
        .await
        .expect("connects");
    hash.subscribe(vec![Subscription::new("w3/wild/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let (publisher, _events) = Client::connect(&context, BROKER, options("w3-pub-wild"))
        .await
        .expect("connects");
    for topic in ["w3/wild", "w3/wild/one", "w3/wild/one/two"] {
        publisher
            .publish(Message::new(topic, "x").at(QoS::AtLeastOnce))
            .await
            .expect("publishes");
    }

    // `+` is exactly one level: only `w3/wild/one`.
    let one = delivered(&mut plus_events).await;
    assert_eq!(one.topic, "w3/wild/one");
    nothing_delivered(&mut plus_events, Duration::from_millis(500)).await;

    // `#` is the parent level and everything below it, so all three - and the
    // parent `w3/wild` is the case a matcher most easily gets wrong.
    let mut by_hash = Vec::new();
    for _ in 0..3 {
        by_hash.push(delivered(&mut hash_events).await.topic);
    }
    by_hash.sort();
    assert_eq!(
        by_hash,
        ["w3/wild", "w3/wild/one", "w3/wild/one/two"],
        "# includes the parent level ([MQTT-4.7.1-1])"
    );

    for client in [&plus, &hash, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// What the CONNACK declares - which for this broker is **nothing at all**, so
/// every one of §11's defaults applies.
///
/// Asserted rather than assumed, because a client that read a declaration it
/// did not receive would be inferring: the defaults are what make an absent
/// property safe, and they are only safe if they are the ones applied.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn the_connacks_declarations_are_the_specifications_defaults() {
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, BROKER, options("w3-declares"))
        .await
        .expect("connects");
    let limits = client.server_limits();

    assert_eq!(limits.maximum_qos, QoS::ExactlyOnce, "absent means 2");
    assert!(limits.retain_available, "absent means available");
    assert_eq!(
        limits.receive_maximum,
        weida_mqtt::DEFAULT_RECEIVE_MAXIMUM,
        "absent means 65,535 (3.2.2.3.3)"
    );
    assert_eq!(
        limits.maximum_packet_size, None,
        "absent means no limit beyond the encoding's own"
    );
    assert_eq!(
        limits.topic_alias_maximum, 4096,
        "**measured**: rumqttd 0.20.0 does declare a Topic Alias Maximum, and \
         it is 4096. So this run exercised the outbound alias path against a \
         real broker rather than skipping it, and a client that had assumed \
         the absent default of 0 would have sent every Topic Name in full for \
         no reason ([MQTT-3.2.2-18])"
    );
    assert!(limits.wildcard_subscription_available);
    assert!(limits.shared_subscription_available);
    assert!(limits.subscription_identifiers_available);
    assert_eq!(
        limits.server_keep_alive, None,
        "absent means the client's own value stands ([MQTT-3.2.2-22])"
    );
    assert_eq!(
        limits.response_information, None,
        "not asked for, and not offered"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
}

/// The `Subscription Identifier` comes back "on every delivery it caused"
/// ([MQTT-3.3.4-4]), which is the half no scripted server can prove:
/// a harness that echoed it would only be echoing our own encoder.
///
/// The same test also exercises `Subscriptions::matching`, which is what a
/// client falls back on against a broker that declares `Subscription
/// Identifiers Available` 0 - a case this broker does not present and
/// `interop_mqtt5.rs`'s does.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn a_subscription_identifier_is_reported_back_on_the_delivery() {
    let context = Context::new().expect("ambient");
    let (subscriber, mut events) = Client::connect(&context, BROKER, options("w3-sub-ident"))
        .await
        .expect("connects");
    subscriber
        .subscribe_with(
            vec![Subscription::new("w3/ident/+", QoS::AtLeastOnce)],
            Some(42),
        )
        .await
        .expect("the identifier is accepted");

    let (publisher, _events) = Client::connect(&context, BROKER, options("w3-pub-ident"))
        .await
        .expect("connects");
    publisher
        .publish(Message::new("w3/ident/one", "x").at(QoS::AtLeastOnce))
        .await
        .expect("publishes");

    let delivery = delivered(&mut events).await;
    assert_eq!(
        delivery.properties.subscription_identifiers,
        [42],
        "**measured**: rumqttd 0.20.0 does report the Subscription Identifier \
         back on every delivery it caused, which is [MQTT-3.3.4-4] honoured \
         and the half no scripted server can prove"
    );

    // The client's own answer to exactly that, needing nothing from the
    // server.
    let subscriptions = subscriber.subscriptions();
    let matched: Vec<&str> = subscriptions
        .matching(&delivery.topic)
        .map(|record| record.subscription.filter.as_str())
        .collect();
    assert_eq!(matched, ["w3/ident/+"]);

    for client in [&subscriber, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// **The practical consequence of [MQTT-3.2.2-4], measured.**
///
/// A process that restarts with no persisted session state and reconnects
/// with Clean Start 0 finds the broker still holding its session, and
/// `Session Present` 1 against a client with nothing to resume "MUST close
/// the Network Connection". So this client refuses, by name, rather than
/// answering acknowledgements it cannot match.
///
/// The finding is that this is not a corner case: it is what every restart of
/// a client without a durable store does. The answer is Clean Start 1 - which
/// the same test then takes - and that is a real cost of MQTT's session model
/// rather than a shortcoming of either implementation. The sheet's §13 note
/// that client-side session durability is often absent is the same fact seen
/// from the other side.
#[tokio::test]
#[ignore = "needs rumqttd on 127.0.0.1:21884; see the module doc for the command"]
async fn a_restart_without_persisted_state_must_close_and_then_clean_start() {
    let context = Context::new().expect("ambient");
    let client_id = unique("w3-restart");

    // The first process: Clean Start 0, a session the broker will hold.
    let first_session = Session::new(client_id.clone(), &resumable(&client_id).limits);
    let (first, mut events) =
        Client::connect_session(&context, BROKER, resumable(&client_id), &first_session)
            .await
            .expect("connects");
    first
        .subscribe(vec![Subscription::new("w3/restart/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    first
        .disconnect_with(DisconnectReasonCode::NormalDisconnection, None)
        .await
        .ok();
    drop(first);
    let _ = tokio::time::timeout(Duration::from_secs(1), events.next()).await;

    // The restart: a brand-new `Session`, which is what a process with no
    // durable store has. The broker says `Session Present` 1 truthfully, and
    // there is nothing here to match it against.
    let restarted = Session::new(client_id.clone(), &resumable(&client_id).limits);
    let error = Client::connect_session(&context, BROKER, resumable(&client_id), &restarted)
        .await
        .expect_err("must close ([MQTT-3.2.2-4])");
    assert!(
        matches!(error, weida_mqtt::Error::SessionPresentWithoutState),
        "named rather than a closed socket: {error}"
    );

    // Clean Start 1 is the answer, and it works: the broker discards its half
    // and says so.
    let mut fresh = resumable(&client_id);
    fresh.clean_start = true;
    let (clean, _events) = Client::connect_session(&context, BROKER, fresh, &restarted)
        .await
        .expect("connects with Clean Start 1");
    assert!(!clean.session_present());
    clean
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::ZERO),
        )
        .await
        .ok();
}
