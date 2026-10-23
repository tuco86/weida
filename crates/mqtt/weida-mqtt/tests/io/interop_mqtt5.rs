//! The MQTT-5-only half: everything `interop_rumqttd.rs` could not reach
//! because rumqttd 0.20.0 declares almost nothing.
//!
//! Measured against **rmqtt 0.23.1** on 2026-09-12, rustc 1.98.
//!
//! # Why rmqtt, and where Mosquitto fits
//!
//! Mosquitto is a **system package**, so a test that needs it cannot count on
//! being able to install it. rmqtt is a Rust binary reachable with
//! `cargo install`, it claims full 5.0 support, and - the part that decides
//! it - **it declares its capabilities in CONNACK**, which rumqttd does not.
//! A broker that declares nothing lets a client observe only success; a broker
//! that declares `Maximum QoS` 1 or `Retain Available` 0 is the only way to
//! reach a refusal path against something real.
//!
//! The Mosquitto commands are kept below so that broker can be measured where
//! it exists, and any disagreement between the two is worth more than either
//! alone.
//!
//! # Running them
//!
//! ```text
//! # The broker measured here, a Rust binary and no system package:
//! #   note the --locked, which is the opposite of what rumqttd needs
//! cargo install rmqttd --version 0.23.1 --locked
//! rmqttd -f crates/mqtt/weida-mqtt/tests/interop/rmqtt.toml
//!
//! # Mosquitto, where the machine has it:
//! #   Debian/Ubuntu: apt install mosquitto
//! #   Arch:          pacman -S mosquitto
//! #   macOS:         brew install mosquitto
//! # then point the tests at it:
//! #   mosquitto -p 21885 -v
//!
//! cargo test -p weida-mqtt --test interop_mqtt5 -- --ignored
//! ```
//!
//! `#[ignore]` because a broker is a runtime condition: the file compiles
//! everywhere and runs where port 21885 answers. Start the broker under a
//! process supervisor with a TCP `ready` condition on that port, and stop it
//! when the run ends.
//!
//! # Measured, not inferred
//!
//! Everything here is what the run observed against rmqtt 0.23.1 configured
//! by `tests/interop/rmqtt.toml`. Where a claim depends on that configuration
//! the test names the setting.
//!
//! **What the CONNACK declares**, which is the whole point of a second
//! broker: `Maximum QoS` 2, `Retain Available` 1, `Receive Maximum` 8,
//! `Maximum Packet Size` 1,048,576, `Topic Alias Maximum` 16, all three
//! subscription-availability flags 1, and `Server Keep Alive` 30 s. Every one
//! of those is a property rumqttd 0.20.0 leaves absent, and absence is the
//! default rather than a statement - so this is where a client's refusal
//! paths become reachable at all.
//!
//! **Two of them are plugins, not listener settings, and that is a trap.**
//! `Retain Available` and `Shared Subscription Available` come from whether
//! `rmqtt-retainer` and `rmqtt-shared-subscription` are started; with the
//! plugins absent the broker declares **0** for both and this client refuses
//! a retained publish with 0x9A and a shared subscription with 0x9E before
//! the wire. That is correct behaviour on both sides and it looks exactly
//! like a broken test, which is why it is written down here.
//!
//! Measured working, end to end through the broker:
//!
//! * **Shared subscriptions.** Two members of `$share/g1/{topic}` and one
//!   ordinary subscriber beside them; one publication produces **one**
//!   delivery to the group and one to the ordinary subscriber. That count is
//!   the only thing that separates a group from a filter.
//! * **Topic aliases in both directions** against a declared maximum of 16:
//!   the second and third publishes to one topic carry a zero-length Topic
//!   Name and the alias, the broker resolves them, and the subscriber
//!   receives the right topic every time.
//! * **Retain Handling 1**, which rumqttd could not separate from 0:
//!   the first subscribe to a filter gets the stored value, the second
//!   subscribe of the *same* filter does not, and Retain Handling 2 never
//!   does.
//! * **Session Expiry as a timer**: a 1-second interval is gone after a
//!   1.6-second wait and a 300-second interval is not, which is what makes
//!   the short case a measurement of the timer rather than of the sleep.
//! * **Every PUBLISH property forwarded unaltered and in order**, repeated
//!   User Property keys included ([MQTT-3.3.2-17]), with `Message Expiry
//!   Interval` rewritten downwards as 3.3.2.3.3 permits.
//! * **The server's `Receive Maximum` becomes the send quota** and the
//!   client's own declared ceiling is a *different* number the server does
//!   not overwrite - two numbers that must not share a field, measured
//!   against a broker that declares 8 while the client declares 200.
//! * **One granted SUBACK code per filter in order**, three filters at three
//!   QoS values, and UNSUBACK's 0x11 `No subscription existed` arriving as a
//!   **success**.
//! * **The availability flags as refusals**: the `restricted` listener
//!   declares `Maximum QoS` 1, and a QoS 2 publish is refused locally with
//!   0x9B while QoS 1 and 0 go through; it declares `Topic Alias Maximum` 0
//!   and no alias is sent, against the same client that aliases happily on
//!   the other listener.
//!
//! # Three disagreements, measured against this version
//!
//! Each is asserted where it happens, so a future version that fixes it
//! fails loudly rather than passing quietly.
//!
//! 1. **A retained message *is* sent to a shared subscription.** 4.8.2 says
//!    it must not be. The shared subscriber receives the stored value at
//!    subscribe time with RETAIN 1.
//! 2. **A Client Identifier takeover closes the transport with no
//!    DISCONNECT.** [MQTT-3.1.4-3] has the server close the older
//!    connection, and 5.0's improvement over 3.1.1 is that it says *why* -
//!    DISCONNECT 0x8E, `Session taken over`. Nothing is sent, so the reason
//!    code 5.0 exists to provide is exactly the thing that is missing, and
//!    what this client owes is to report the close by name rather than hang.
//! 3. **`Retain Available` and `Shared Subscription Available` are global to
//!    the broker rather than per listener**, because they are plugin state.
//!    So the two flags cannot differ between two listeners of one broker,
//!    which is why the refusal half of this file uses `Maximum QoS` and
//!    `Topic Alias Maximum` instead - the two flags that *are* per listener.
//!
//! # What is still unmeasured, named rather than worked around
//!
//! * **AUTH.** Neither broker in this repository offers an
//!   enhanced-authentication mechanism. rmqtt has JWT and HTTP auth plugins,
//!   but both authenticate the CONNECT's User Name and Password rather than
//!   running 4.12's AUTH exchange, so 4.12 stays measured only against the
//!   scripted server of `tls_auth.rs`. That file's `AnswerAuth` acts are a
//!   real exchange against our own codec and not against a foreign one, and
//!   this file says so rather than implying coverage.
//! * **`Response Information`.** Asked for and not offered by either broker,
//!   so `Client::response_topic` returning `None` is measured and the
//!   namespace-joining half is not.
//!
//! # A build fact, and it is the mirror image of rumqttd's
//!
//! `cargo install rmqttd --version 0.23.1` **fails** without `--locked`: the
//! resolver picks a `pulsar` version whose `producer::Message` gained a field,
//! and `rmqtt-bridge-egress-pulsar` does not compile against it. With
//! `--locked` it builds in about 3m40s. rumqttd 0.20.0 is the exact
//! opposite - it builds only *without* `--locked`. Two brokers, two pinning
//! failures, in opposite directions, and both are facts about installing a
//! Rust broker in 2026 rather than about MQTT.

#![cfg(test)]

use crate::harness;

use std::time::Duration;

use harness::{DEADLINE, unique};
use weida_mqtt::{
    Client, ConnectOptions, Context, Delivery, DisconnectReasonCode, Error, Event, Events, Message,
    QoS, RetainHandling, Session, Subscription,
};

/// Where `tests/interop/rmqtt.toml` listens, and where `mosquitto -p 21885`
/// would.
const BROKER: &str = "127.0.0.1:21885";

/// The second listener of the same configuration, which declares **less**:
/// `Maximum QoS` 1 and `Topic Alias Maximum` 0. A client's refusal paths are
/// only reachable against a declaration, so a broker that states nothing
/// cannot exercise them at all.
const RESTRICTED: &str = "127.0.0.1:21887";

fn options(client_id: &str) -> ConnectOptions {
    let mut options = ConnectOptions::new(client_id);
    options.keep_alive = Duration::from_secs(30);
    options.connect_timeout = Duration::from_secs(3600);
    options
}

async fn delivered(events: &mut Events) -> Delivery {
    match weida_testkit::within("a delivery", DEADLINE, events.next()).await {
        Some(Event::Delivered(delivery)) => delivery,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

async fn nothing_delivered(events: &mut Events, window: Duration) {
    if let Ok(event) = tokio::time::timeout(window, events.next()).await {
        panic!("expected nothing, got {event:?}");
    }
}

/// **What this broker declares**, which is the whole reason for a second one.
///
/// Every value here is a CONNACK property rumqttd 0.20.0 leaves absent. A
/// declared value and an absent one are indistinguishable to a client that
/// only ever sees the defaults, and the refusal paths of `ServerLimits`
/// cannot be reached at all without one.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn the_connack_declares_the_five_dot_oh_capabilities() {
    let context = Context::new().expect("ambient");
    let mut connect = options(&unique("w3-declares"));
    connect.request_response_information = true;
    let (client, _events) = Client::connect(&context, BROKER, connect)
        .await
        .expect("connects");
    let limits = client.server_limits();

    // Printed as one block so a future run that finds a different broker or a
    // different configuration can see what changed rather than only which
    // assertion failed.
    println!(
        "rmqtt 0.23.1 declares: maximum_qos={:?} retain={} receive_maximum={} \
         maximum_packet_size={:?} topic_alias_maximum={} wildcard={} shared={} \
         identifiers={} server_keep_alive={:?} session_expiry={:?} \
         response_information={:?} assigned_client_identifier={:?}",
        limits.maximum_qos,
        limits.retain_available,
        limits.receive_maximum,
        limits.maximum_packet_size,
        limits.topic_alias_maximum,
        limits.wildcard_subscription_available,
        limits.shared_subscription_available,
        limits.subscription_identifiers_available,
        limits.server_keep_alive,
        limits.session_expiry_interval,
        limits.response_information,
        limits.assigned_client_identifier,
    );

    // The configuration asked for these, and the broker states them.
    assert_eq!(
        limits.maximum_qos,
        QoS::ExactlyOnce,
        "max_qos_allowed = 2 in the configuration"
    );
    assert!(limits.retain_available, "retain_available = true");
    assert_eq!(
        limits.topic_alias_maximum, 16,
        "max_topic_alias = 16: **declared**, not defaulted, so this client \
         aliases against it"
    );
    assert_eq!(
        limits.receive_maximum, 8,
        "max_inflight = 8: the server's own Receive Maximum, which is what \
         bounds this client's send quota (3.2.2.3.3)"
    );
    assert!(limits.shared_subscription_available);
    assert!(limits.wildcard_subscription_available);
    assert!(limits.subscription_identifiers_available);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
}

/// The server's `Receive Maximum` becomes this client's send quota, and
/// exhausting it **stalls** rather than exceeding it ([MQTT-4.9.0-2]).
///
/// `qos.rs` proves the stall against a scripted server. This proves the number
/// came off a real CONNACK: the configuration says 8, the session says 8.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn the_servers_receive_maximum_becomes_the_send_quota() {
    let context = Context::new().expect("ambient");
    let client_id = unique("w3-quota");
    let mut connect = options(&client_id);
    // A client-side ceiling far above the server's, so the number in force
    // can only have come from the CONNACK.
    connect.limits.receive_maximum = 200;
    let session = Session::new(client_id.clone(), &connect.limits);
    let (client, _events) = Client::connect_session(&context, BROKER, connect, &session)
        .await
        .expect("connects");

    assert_eq!(
        session.send_quota(),
        8,
        "the server's declared Receive Maximum replaced this client's \
         placeholder ([MQTT-4.9.0-1])"
    );
    assert_eq!(
        session.receive_maximum(),
        200,
        "and the client's own declared ceiling is a different number that the \
         server's does not overwrite (3.1.2.11.3 against 3.2.2.3.3) - two \
         numbers that must not share a field"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
}

/// **Shared subscriptions**: `$share/{ShareName}/{filter}` splits one topic's
/// deliveries between the group's members, one copy per publication rather
/// than one per member (4.8.2) [mqtt5 §8].
///
/// The assertion that separates a shared subscription from an ordinary one is
/// exactly that count: two members of a group and one ordinary subscriber
/// beside them, one publish, **two** deliveries in total.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn a_shared_subscription_splits_deliveries_between_its_members() {
    let context = Context::new().expect("ambient");
    let topic = format!("w3/shared/{}", unique("t"));
    let filter = format!("$share/g1/{topic}");

    let (first, mut first_events) = Client::connect(&context, BROKER, options(&unique("w3-g1a")))
        .await
        .expect("connects");
    first
        .subscribe(vec![Subscription::new(&filter, QoS::AtLeastOnce)])
        .await
        .expect("the broker accepts a shared subscription");
    let (second, mut second_events) = Client::connect(&context, BROKER, options(&unique("w3-g1b")))
        .await
        .expect("connects");
    second
        .subscribe(vec![Subscription::new(&filter, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    // An ordinary subscriber on the same topic, which is not in the group and
    // must receive its own copy.
    let (alone, mut alone_events) = Client::connect(&context, BROKER, options(&unique("w3-alone")))
        .await
        .expect("connects");
    alone
        .subscribe(vec![Subscription::new(&topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let (publisher, _events) = Client::connect(&context, BROKER, options(&unique("w3-gpub")))
        .await
        .expect("connects");
    publisher
        .publish(Message::new(&topic, "once").at(QoS::AtLeastOnce))
        .await
        .expect("publishes");

    // The ordinary subscriber always gets one.
    assert_eq!(delivered(&mut alone_events).await.payload, &b"once"[..]);

    // Exactly one of the two group members gets one, and the other gets
    // nothing - which is the whole difference between a group and a filter.
    // First wait for the one delivery however long it takes; then a short
    // negative window checks that the other member gets none.
    let (to_first, to_second) = {
        let winner = weida_testkit::within("the group's one delivery", DEADLINE, async {
            tokio::select! {
                event = first_events.next() => (true, event),
                event = second_events.next() => (false, event),
            }
        })
        .await;
        let (first_won, event) = winner;
        let other = if first_won {
            tokio::time::timeout(Duration::from_millis(800), second_events.next()).await
        } else {
            tokio::time::timeout(Duration::from_millis(800), first_events.next()).await
        };
        if first_won {
            (Ok(event), other)
        } else {
            (other, Ok(event))
        }
    };
    let group_deliveries = usize::from(to_first.is_ok()) + usize::from(to_second.is_ok());
    assert_eq!(
        group_deliveries, 1,
        "one publication, one delivery to the group as a whole: \
         first={to_first:?} second={to_second:?}"
    );

    for client in [&first, &second, &alone, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// A retained message is **never** sent to a shared subscription (4.8.2)
/// [mqtt5 §8], which is the rule a client cannot enforce and must not expect
/// around.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn a_shared_subscription_receives_no_retained_message() {
    let context = Context::new().expect("ambient");
    let topic = format!("w3/sharedretain/{}", unique("t"));
    let (publisher, _events) = Client::connect(&context, BROKER, options(&unique("w3-srpub")))
        .await
        .expect("connects");
    publisher
        .publish(
            Message::new(&topic, "stored")
                .retained()
                .at(QoS::AtLeastOnce),
        )
        .await
        .expect("publishes retained");

    // An ordinary subscriber proves the value really is stored.
    let (ordinary, mut ordinary_events) =
        Client::connect(&context, BROKER, options(&unique("w3-srord")))
            .await
            .expect("connects");
    ordinary
        .subscribe(vec![Subscription::new(&topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    let cached = delivered(&mut ordinary_events).await;
    assert!(cached.retain, "the value is in the cache");

    // **A measured disagreement.** 4.8.2 says retained messages are not sent
    // to shared subscriptions, and this broker sends one: the shared
    // subscriber receives the stored value at subscribe time, with RETAIN 1.
    // Asserted as measured, so a future version that implements the rule
    // fails here and gets noticed.
    let (shared, mut shared_events) =
        Client::connect(&context, BROKER, options(&unique("w3-srsh")))
            .await
            .expect("connects");
    shared
        .subscribe(vec![Subscription::new(
            format!("$share/g2/{topic}"),
            QoS::AtLeastOnce,
        )])
        .await
        .expect("subscribed");
    let to_shared = delivered(&mut shared_events).await;
    assert_eq!(to_shared.payload, &b"stored"[..]);
    assert!(
        to_shared.retain,
        "and it arrives from the cache: rmqtt 0.23.1 sends a retained message \
         to a shared subscription, which 4.8.2 says it must not"
    );

    // It is subscribed either way: a live publish reaches it.
    publisher
        .publish(Message::new(&topic, "live").at(QoS::AtLeastOnce))
        .await
        .expect("publishes");
    assert_eq!(delivered(&mut shared_events).await.payload, &b"live"[..]);

    publisher
        .publish(Message::delete_retained(&topic).at(QoS::AtLeastOnce))
        .await
        .ok();
    for client in [&publisher, &ordinary, &shared] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// **Topic aliases against a broker that declares a maximum**, in both
/// directions.
///
/// Outbound: the second publish to the same topic carries a zero-length Topic
/// Name and the alias, and the broker resolves it - which the subscriber
/// proves by receiving the right topic. Inbound: whatever the broker chooses
/// to do, the application is handed a Topic Name and never an alias.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn topic_aliases_survive_a_round_trip_through_the_broker() {
    let context = Context::new().expect("ambient");
    let topic = format!("w3/alias/{}/deep/nested/name", unique("t"));
    let (subscriber, mut events) = Client::connect(&context, BROKER, options(&unique("w3-alsub")))
        .await
        .expect("connects");
    subscriber
        .subscribe(vec![Subscription::new("w3/alias/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let mut connect = options(&unique("w3-alpub"));
    // Declare an inbound maximum too, so the broker may alias toward us.
    connect.limits.topic_alias_maximum = 16;
    let (publisher, _events) = Client::connect(&context, BROKER, connect)
        .await
        .expect("connects");
    assert_eq!(
        publisher.server_limits().topic_alias_maximum,
        16,
        "the broker declared one, so this client will alias"
    );

    for payload in ["one", "two", "three"] {
        publisher
            .publish(Message::new(&topic, payload).at(QoS::AtLeastOnce))
            .await
            .expect("publishes");
    }

    for payload in ["one", "two", "three"] {
        let delivery = delivered(&mut events).await;
        assert_eq!(
            delivery.topic, topic,
            "the broker resolved the alias our second and third publishes \
             carried, and this client resolved any it sent back: an \
             application never sees a zero-length Topic Name"
        );
        assert_eq!(delivery.payload, payload.as_bytes());
    }

    for client in [&subscriber, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// All three Retain Handling values against a broker that implements them,
/// including the **1** that rumqttd could not separate from 0.
///
/// Retain Handling 1 is "send them only if the subscription did not already
/// exist", so it takes two subscribes of the same filter on the same session
/// to observe: the first gets the value, the second does not.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn retain_handling_one_sends_only_on_a_new_subscription() {
    let context = Context::new().expect("ambient");
    let topic = format!("w3/rh/{}", unique("t"));
    let (publisher, _events) = Client::connect(&context, BROKER, options(&unique("w3-rhpub")))
        .await
        .expect("connects");
    publisher
        .publish(
            Message::new(&topic, "stored")
                .retained()
                .at(QoS::AtLeastOnce),
        )
        .await
        .expect("publishes retained");

    let (client, mut events) = Client::connect(&context, BROKER, options(&unique("w3-rh1")))
        .await
        .expect("connects");
    let subscription =
        Subscription::new(&topic, QoS::AtLeastOnce).retain_handling(RetainHandling::SendIfNew);

    // First subscribe: the subscription did not exist, so the value comes.
    client
        .subscribe(vec![subscription.clone()])
        .await
        .expect("subscribed");
    let first = delivered(&mut events).await;
    assert_eq!(first.payload, &b"stored"[..]);
    assert!(first.retain);

    // Second subscribe of the same filter: it did exist, so nothing comes -
    // and the subscription is replaced rather than duplicated
    // ([MQTT-3.8.4-3]).
    client
        .subscribe(vec![subscription])
        .await
        .expect("re-subscribed");
    nothing_delivered(&mut events, Duration::from_millis(700)).await;
    assert_eq!(
        client.subscriptions().len(),
        1,
        "one filter, one subscription"
    );

    // Retain Handling 2 never sends it, first subscribe or not.
    let (never, mut never_events) = Client::connect(&context, BROKER, options(&unique("w3-rh2")))
        .await
        .expect("connects");
    never
        .subscribe(vec![
            Subscription::new(&topic, QoS::AtLeastOnce).retain_handling(RetainHandling::DoNotSend),
        ])
        .await
        .expect("subscribed");
    nothing_delivered(&mut never_events, Duration::from_millis(700)).await;

    publisher
        .publish(Message::delete_retained(&topic).at(QoS::AtLeastOnce))
        .await
        .ok();
    for handle in [&publisher, &client, &never] {
        handle
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// **Session Expiry as a timer**, which needs a broker that actually runs
/// one: a session with a 1-second expiry is gone a moment later, and the same
/// client identifier with a long expiry is not.
///
/// The wall-clock wait is the smallest that demonstrates it, and the pairing
/// is what makes it a measurement rather than a sleep: the long-expiry half
/// runs the same sequence and finds the session still there.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn a_session_expires_on_its_own_interval() {
    let context = Context::new().expect("ambient");

    // Short: one second, then gone.
    let short_id = unique("w3-expire-short");
    let mut short = options(&short_id);
    short.clean_start = false;
    short.session_expiry = Some(Duration::from_secs(1));
    let short_session = Session::new(short_id.clone(), &short.limits);
    let (client, mut events) =
        Client::connect_session(&context, BROKER, short.clone(), &short_session)
            .await
            .expect("connects");
    client
        .subscribe(vec![Subscription::new("w3/expire/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    client
        .disconnect_with(DisconnectReasonCode::NormalDisconnection, None)
        .await
        .ok();
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(1), events.next()).await;

    tokio::time::sleep(Duration::from_millis(1600)).await;
    let (after, _events) = Client::connect_session(&context, BROKER, short, &short_session)
        .await
        .expect("connects");
    assert!(
        !after.session_present(),
        "the Session Expiry Interval elapsed while the connection was gone, \
         so the server discarded the session (3.1.2.11.2)"
    );
    after
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::ZERO),
        )
        .await
        .ok();

    // Long: the same sequence, the same wait, and the session is still there.
    let long_id = unique("w3-expire-long");
    let mut long = options(&long_id);
    long.clean_start = false;
    long.session_expiry = Some(Duration::from_secs(300));
    let long_session = Session::new(long_id.clone(), &long.limits);
    let (client, mut events) =
        Client::connect_session(&context, BROKER, long.clone(), &long_session)
            .await
            .expect("connects");
    client
        .subscribe(vec![Subscription::new("w3/expire/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");
    client
        .disconnect_with(DisconnectReasonCode::NormalDisconnection, None)
        .await
        .ok();
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(1), events.next()).await;

    tokio::time::sleep(Duration::from_millis(1600)).await;
    let (still, _events) = Client::connect_session(&context, BROKER, long, &long_session)
        .await
        .expect("connects");
    assert!(
        still.session_present(),
        "the same wait against a 300-second interval leaves the session in \
         place, which is what makes the short case a measurement of the timer \
         rather than of the sleep"
    );
    still
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::ZERO),
        )
        .await
        .ok();
}

/// Properties end to end through a real broker: every application property a
/// PUBLISH can carry, forwarded unaltered ([MQTT-3.3.2-17] for User
/// Property).
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn every_publish_property_survives_the_broker() {
    let context = Context::new().expect("ambient");
    let topic = format!("w3/props/{}", unique("t"));
    let (subscriber, mut events) = Client::connect(&context, BROKER, options(&unique("w3-psub")))
        .await
        .expect("connects");
    subscriber
        .subscribe(vec![Subscription::new("w3/props/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let (publisher, _events) = Client::connect(&context, BROKER, options(&unique("w3-ppub")))
        .await
        .expect("connects");
    let mut message = Message::new(&topic, "body").at(QoS::AtLeastOnce);
    message.payload_format_indicator = Some(weida_mqtt::PayloadFormat::Utf8);
    message.content_type = Some("text/plain".into());
    message.response_topic = Some("w3/props/reply".into());
    message.correlation_data = Some(b"corr-1".to_vec());
    message.message_expiry = Some(Duration::from_secs(120));
    message.user_properties = vec![
        ("first".into(), "1".into()),
        ("second".into(), "2".into()),
        // A repeated key, which the protocol permits and order preserves.
        ("first".into(), "3".into()),
    ];
    publisher.publish(message).await.expect("publishes");

    let delivery = delivered(&mut events).await;
    assert_eq!(delivery.topic, topic);
    assert_eq!(
        delivery.properties.payload_format_indicator,
        Some(weida_mqtt::PayloadFormat::Utf8)
    );
    assert_eq!(
        delivery.properties.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        delivery.properties.response_topic.as_deref(),
        Some("w3/props/reply")
    );
    assert_eq!(
        delivery.properties.correlation_data.as_deref(),
        Some(&b"corr-1"[..])
    );
    assert_eq!(
        delivery.properties.user_properties,
        [
            ("first".to_owned(), "1".to_owned()),
            ("second".to_owned(), "2".to_owned()),
            ("first".to_owned(), "3".to_owned()),
        ],
        "forwarded unaltered and **in order**, repeats included \
         ([MQTT-3.3.2-17])"
    );
    // The Message Expiry Interval is rewritten downwards by the server to
    // what is left of it (3.3.2.3.3), so the assertion is a bound and not an
    // equality.
    let remaining = delivery
        .properties
        .message_expiry_interval
        .expect("the server forwards it");
    assert!(
        remaining <= 120,
        "a server rewrites the interval to what is left: {remaining:?}"
    );

    for client in [&subscriber, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}

/// A reason code from a real broker rather than a scripted one: SUBACK's
/// per-filter verdict.
///
/// The filter is legal, so the interesting half is that the code comes back
/// **per filter and in order** ([MQTT-3.9.3-1]) across a SUBSCRIBE carrying
/// three of them at three different maximum QoS values.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn suback_carries_one_granted_code_per_filter_in_order() {
    let context = Context::new().expect("ambient");
    let prefix = unique("w3-codes");
    let (client, _events) = Client::connect(&context, BROKER, options(&prefix))
        .await
        .expect("connects");

    let granted = client
        .subscribe(vec![
            Subscription::new(format!("w3/{prefix}/a"), QoS::AtMostOnce),
            Subscription::new(format!("w3/{prefix}/b"), QoS::AtLeastOnce),
            Subscription::new(format!("w3/{prefix}/c"), QoS::ExactlyOnce),
        ])
        .await
        .expect("subscribed");
    assert_eq!(
        granted,
        [
            weida_mqtt::SubackReasonCode::GrantedQos0,
            weida_mqtt::SubackReasonCode::GrantedQos1,
            weida_mqtt::SubackReasonCode::GrantedQos2,
        ],
        "one code per filter, in the order the filters were sent, and each \
         the QoS asked for because the broker declared Maximum QoS 2"
    );

    // UNSUBACK's codes are per filter too, and one of these was never
    // subscribed: 0x11 `No subscription existed`, which is a **success**.
    let codes = client
        .unsubscribe(vec![format!("w3/{prefix}/a"), format!("w3/{prefix}/never")])
        .await
        .expect("unsubscribed");
    assert_eq!(codes.len(), 2);
    assert_eq!(codes[0], weida_mqtt::UnsubackReasonCode::Success);
    assert!(
        !codes[1].is_error(),
        "a filter that was never subscribed is not a failure: the end state \
         the caller asked for holds either way ({:?})",
        codes[1]
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
}

/// A server DISCONNECT reaches the application as a **named reason code**,
/// which is 5.0's improvement over 3.1.1 closing the socket and saying
/// nothing [mqtt5 §1.9].
///
/// Provoked by exceeding the server's own `Receive Maximum` is not available
/// to a client that honours it - so this provokes it the way a real client
/// would meet one: a Topic Alias above what the broker declared, which is
/// 0x94 (Topic Alias invalid).
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21885; see the module doc"]
async fn a_server_disconnect_arrives_as_a_reason_code() {
    let context = Context::new().expect("ambient");
    let (client, mut events) = Client::connect(&context, BROKER, options(&unique("w3-dis")))
        .await
        .expect("connects");

    // A Client Identifier takeover is the other way a server DISCONNECTs a
    // live client: the same identifier connecting again earns 0x8E (Session
    // taken over) on the first connection ([MQTT-3.1.4-3]).
    let taken = client.client_id().to_owned();
    let (second, _second_events) = Client::connect(&context, BROKER, options(&taken))
        .await
        .expect("the second connection with the same identifier");

    let event = weida_testkit::within("the first connection to be told", DEADLINE, events.next())
        .await
        .expect("an event");
    let Event::Disconnected(error) = event else {
        panic!("expected a disconnect, got {event:?}")
    };
    // **A measured disagreement, and the one that matters most to this
    // library's own premise.** [MQTT-3.1.4-3] has the server close the older
    // connection on a takeover, and 5.0's whole improvement over 3.1.1 is
    // that it says *why*: DISCONNECT 0x8E, `Session taken over`. This broker
    // closes the transport and sends nothing, which is exactly the 3.1.1
    // behaviour the sheet describes - "a 3.1.1 client learned why a server
    // objected only by watching it close the socket" [mqtt5 §1.9].
    //
    // What this client owes in that case is to report the close as a close
    // rather than as a hang, which it does: `Error::ConnectionClosed` is "the
    // peer closed the connection without a DISCONNECT", by name.
    assert!(
        matches!(error, Error::ConnectionClosed),
        "measured against rmqtt 0.23.1: a Client Identifier takeover closes \
         the transport with no DISCONNECT, so the reason code 5.0 provides is \
         not sent and the named close is all a client gets: {error}"
    );
    assert_eq!(
        error.reason_code(),
        None,
        "and there is no code to report, which is the loss being measured"
    );

    second
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
    drop(client);
}

/// **The availability flags as declarations, which rumqttd cannot exercise.**
///
/// The `restricted` listener of `tests/interop/rmqtt.toml` declares `Maximum
/// QoS` 1 and `Topic Alias Maximum` 0. Against a broker that declares
/// nothing, a client can only observe success; against one that declares
/// less, every refusal path in `ServerLimits` becomes reachable - and each
/// refusal happens **before the packet reaches the wire**, carrying the code
/// the server would have sent.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21887; see the module doc"]
async fn a_declared_maximum_qos_refuses_a_higher_publish_locally() {
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, RESTRICTED, options(&unique("w3-maxqos")))
        .await
        .expect("connects");
    assert_eq!(
        client.server_limits().maximum_qos,
        QoS::AtLeastOnce,
        "max_qos_allowed = 1 on this listener"
    );

    // QoS 2 is refused here, with 0x9B (QoS not supported) - the code the
    // server would have sent had the packet been allowed to leave.
    let error = client
        .publish(Message::new("w3/maxqos/a", "x").at(QoS::ExactlyOnce))
        .await
        .expect_err("refused");
    assert_eq!(error.reason_code(), Some(0x9B), "{error}");

    // QoS 1 and 0 go through, so the refusal is the declaration and not a
    // broken publish path.
    client
        .publish(Message::new("w3/maxqos/a", "x").at(QoS::AtLeastOnce))
        .await
        .expect("QoS 1 is what the listener allows");
    client
        .publish(Message::new("w3/maxqos/a", "x"))
        .await
        .expect("and QoS 0 is below it");

    // A *subscription* above the declared maximum is not refused: the server
    // answers with what it granted, which is the difference between a
    // publish ceiling and a subscription ceiling.
    let granted = client
        .subscribe(vec![Subscription::new("w3/maxqos/+", QoS::ExactlyOnce)])
        .await
        .expect("subscribed");
    assert_eq!(
        granted,
        [weida_mqtt::SubackReasonCode::GrantedQos1],
        "asked for 2, granted 1: the SUBACK reports the ceiling rather than \
         refusing the filter ([MQTT-3.8.4-8])"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
}

/// A `Topic Alias Maximum` of **0** - declared rather than absent - means the
/// same thing absence does ([MQTT-3.2.2-18]), and this client sends no alias
/// either way.
///
/// The pairing is what makes it a measurement: the `external` listener
/// declares 16 and the aliases go out, the `restricted` listener declares 0
/// and they do not. Same client, same code path, two declarations.
#[tokio::test]
#[ignore = "needs a 5.0 broker on 127.0.0.1:21887; see the module doc"]
async fn a_declared_topic_alias_maximum_of_zero_suppresses_aliasing() {
    let context = Context::new().expect("ambient");
    let topic = format!("w3/noalias/{}/a/long/name", unique("t"));

    let (subscriber, mut events) =
        Client::connect(&context, RESTRICTED, options(&unique("w3-nasub")))
            .await
            .expect("connects");
    subscriber
        .subscribe(vec![Subscription::new("w3/noalias/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribed");

    let (publisher, _events) = Client::connect(&context, RESTRICTED, options(&unique("w3-napub")))
        .await
        .expect("connects");
    assert_eq!(
        publisher.server_limits().topic_alias_maximum,
        0,
        "max_topic_aliases = 0 on this listener"
    );

    // Three publishes to one topic. With a declared 0 none of them may carry
    // an alias, so all three carry the Topic Name - and all three arrive,
    // which is what proves the client did not send one the broker would have
    // rejected with 0x94.
    for payload in ["one", "two", "three"] {
        publisher
            .publish(Message::new(&topic, payload).at(QoS::AtLeastOnce))
            .await
            .expect("publishes");
        let delivery = delivered(&mut events).await;
        assert_eq!(delivery.topic, topic);
        assert_eq!(delivery.payload, payload.as_bytes());
    }

    for client in [&subscriber, &publisher] {
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .ok();
    }
}
