//! Retained messages and the Will on the wire: RETAIN, the zero-byte delete,
//! telling a retained delivery from a live one, and the Will.
//!
//! # What a scripted server can and cannot prove about the Will
//!
//! The Will is **published by the server**, so "the Will fires" is not an
//! observable of this client at all - no client ever sees its own Will. What
//! *is* observable, and what every rule in 3.1.2.5 and 3.14.4 turns on, is
//! **how the connection ended**:
//!
//! * a DISCONNECT with reason code 0x00 - the server "MUST discard the Will
//!   without publishing it" ([MQTT-3.14.4-3]);
//! * a DISCONNECT with 0x04 `DisconnectWithWillMessage` - the server publishes
//!   it anyway, which is the only way a client asks for its own Will;
//! * the transport closing with no DISCONNECT at all - the "abnormal
//!   disconnection" that is the mechanism's entire purpose (3.1.2.5).
//!
//! So these tests assert which of those three the server saw, which is exactly
//! the client's half of the contract. The broker half - that the message
//! actually appears on the Will Topic, and that it does not appear when a new
//! connection to the session arrives inside the Will Delay Interval - is
//! measured against a real broker in the interop tests (`interop_rumqttd.rs`,
//! `interop_mqtt5.rs`), because asserting it here would only be asserting our
//! own scripted server.

mod harness;

use std::time::Duration;

use harness::{Act, Server, bytes, connack};
use weida_mqtt::{
    Client, ConnectOptions, Context, DisconnectReasonCode, Event, Message, PacketType, QoS,
    RetainHandling, RetainedOrigin, ServerLimits, Session, Subscription, WillMessage,
};
use weida_mqtt_codec::{Connack, ConnectReasonCode, Packet, Properties, Publish};

fn options() -> ConnectOptions {
    harness::options("retain-client")
}

/// A delivery on `topic` with the RETAIN flag as given.
fn delivery(topic: &str, payload: &[u8], retain: bool) -> Vec<u8> {
    bytes(&Packet::Publish(Publish {
        topic,
        payload,
        qos: QoS::AtMostOnce,
        retain,
        ..Publish::default()
    }))
}

/// RETAIN reaches the wire, and the zero-byte delete is a publish and not an
/// operation: RETAIN 1 with an empty payload ([MQTT-3.3.1-6]).
#[tokio::test]
async fn retain_and_the_zero_byte_delete_reach_the_wire() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        // QoS 1 throughout, so each publish resolves only once the server has
        // read and answered it: a QoS 0 publish resolves on the write and the
        // assertions below would race the server's read.
        Act::AckPublish, // the retained publish
        Act::AckPublish, // the delete
        Act::AckPublish, // an ordinary publish, for the contrast
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .publish(
            Message::new("state/a", "on")
                .retained()
                .at(QoS::AtLeastOnce),
        )
        .await
        .expect("publishes");
    client
        .publish(Message::delete_retained("state/a").at(QoS::AtLeastOnce))
        .await
        .expect("deletes");
    client
        .publish(Message::new("state/a", "on").at(QoS::AtLeastOnce))
        .await
        .expect("publishes live");

    let recorded = server.seen_bytes().await;
    let publishes: Vec<Publish<'_>> = recorded[1..4]
        .iter()
        .map(|raw| {
            let (Packet::Publish(publish), _) = Packet::decode(raw, u32::MAX).expect("a publish")
            else {
                panic!("a publish")
            };
            publish
        })
        .collect();

    assert!(publishes[0].retain);
    assert_eq!(publishes[0].payload, b"on");

    assert!(publishes[1].retain, "the delete is RETAIN 1");
    assert!(
        publishes[1].payload.is_empty(),
        "and a zero-length payload, which is what makes it a delete rather than \
         a stored empty message"
    );

    assert!(
        !publishes[2].retain,
        "the same topic and payload without RETAIN is an ordinary publish: the \
         flag is what decides whether the server stores it"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// Under Retain As Published **0** - the default - RETAIN is the answer: the
/// server sets it on a message sent because a subscription was made
/// ([MQTT-3.3.1-8]) and clears it on everything it forwards live
/// ([MQTT-3.3.1-12]).
///
/// Under **1** the flag is the publisher's ([MQTT-3.3.1-13]) and the receiver
/// can no longer tell, which is [`RetainedOrigin::Unknowable`] - a named loss
/// and not a gap in this client.
#[tokio::test]
async fn a_receiver_tells_retained_from_live_only_where_the_protocol_lets_it() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x00]),
        Act::Send(delivery("state/a", b"snapshot", true)),
        Act::Send(delivery("state/a", b"change", false)),
        Act::Suback(vec![0x00]),
        // The same two bytes again, for the Retain As Published subscription.
        Act::Send(delivery("rap/a", b"snapshot", true)),
        Act::Send(delivery("rap/a", b"change", false)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, mut events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe(vec![Subscription::new("state/+", QoS::AtMostOnce)])
        .await
        .expect("subacked");

    let Some(Event::Delivered(snapshot)) = events.next().await else {
        panic!("a delivery")
    };
    assert_eq!(
        snapshot.origin(false),
        RetainedOrigin::Retained,
        "RETAIN 1 under Retain As Published 0 can only be the cache"
    );
    let Some(Event::Delivered(change)) = events.next().await else {
        panic!("a delivery")
    };
    assert_eq!(change.origin(false), RetainedOrigin::Live);

    // The same wire bytes read against a Retain As Published subscription.
    client
        .subscribe(vec![
            Subscription::new("rap/+", QoS::AtMostOnce).retain_as_published(),
        ])
        .await
        .expect("subacked");
    let Some(Event::Delivered(snapshot)) = events.next().await else {
        panic!("a delivery")
    };
    let Some(Event::Delivered(change)) = events.next().await else {
        panic!("a delivery")
    };
    assert_eq!(
        snapshot.origin(true),
        RetainedOrigin::Unknowable,
        "the flag now reports what the publisher asked to store"
    );
    assert_eq!(
        change.origin(true),
        RetainedOrigin::Unknowable,
        "and the same is true when it is clear: RETAIN 0 under RAP 1 means the \
         publisher did not ask for storage, not that this copy is live"
    );
    // The flag itself is still reported, because it is on the wire - what is
    // refused is the inference.
    assert!(snapshot.retain);
    assert!(!change.retain);

    // And the option that decides it is on the subscription the mirror holds,
    // so a caller reads the verdict without tracking it separately.
    let subscriptions = client.subscriptions();
    let record = subscriptions
        .matching("rap/a")
        .next()
        .expect("the filter that matched");
    assert!(record.subscription.options.retain_as_published);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// All three Retain Handling values reach the wire, and each is a different
/// question about the subscribe moment (3.8.3.1) [mqtt5 §4.4].
#[tokio::test]
async fn all_three_retain_handling_values_reach_the_wire() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x00, 0x00, 0x00]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe(vec![
            // 0: send them now.
            Subscription::new("all/+", QoS::AtMostOnce),
            // 1: only if this subscription did not already exist - "useful
            // when a reconnect is done and the Client is not certain whether
            // the subscriptions were completed".
            Subscription::new("new/+", QoS::AtMostOnce).retain_handling(RetainHandling::SendIfNew),
            // 2: never - "wishes to receive change notifications and does not
            // need to know the initial state".
            Subscription::new("changes/+", QoS::AtMostOnce)
                .retain_handling(RetainHandling::DoNotSend),
        ])
        .await
        .expect("subacked");

    let recorded = server.seen_bytes().await;
    let (Packet::Subscribe(subscribe), _) =
        Packet::decode(&recorded[1], u32::MAX).expect("a subscribe")
    else {
        panic!("a subscribe")
    };
    let handling: Vec<RetainHandling> = subscribe
        .filters
        .iter()
        .map(|filter| filter.options.retain_handling)
        .collect();
    assert_eq!(
        handling,
        [
            RetainHandling::SendAtSubscribe,
            RetainHandling::SendIfNew,
            RetainHandling::DoNotSend,
        ]
    );
    // The two reserved bits stay clear, and 3 is unrepresentable: it is not a
    // value `RetainHandling` has, so "a Protocol Error to send" (3.8.3.1) is
    // enforced by the type and refused by the codec on the way in.
    for filter in subscribe.filters.iter() {
        assert_eq!(filter.options.as_byte() & 0b1100_0000, 0);
    }

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// A retained delivery is never sent to a Shared Subscription (4.8.2)
/// [mqtt5 §8], which is why this client refuses to expect one: the test is
/// that the two options are **both** on the wire and that the documentation
/// is the only place the exclusion can live, because the server is the one
/// that honours it.
///
/// What is asserted here is the half that is ours: a `$share/` subscription
/// carries Retain Handling unchanged rather than having it quietly rewritten
/// to 2, because rewriting it would be inventing a guarantee - the server's
/// behaviour is the same either way and a rewritten byte would hide which
/// server did what.
#[tokio::test]
async fn a_shared_subscription_carries_its_options_unrewritten() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x00]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe(vec![Subscription::new("$share/g/state/+", QoS::AtMostOnce)])
        .await
        .expect("subacked");

    let recorded = server.seen_bytes().await;
    let (Packet::Subscribe(subscribe), _) =
        Packet::decode(&recorded[1], u32::MAX).expect("a subscribe")
    else {
        panic!("a subscribe")
    };
    let filter = subscribe.filters.iter().next().expect("one filter");
    assert_eq!(filter.filter, "$share/g/state/+");
    assert_eq!(
        filter.options.retain_handling,
        RetainHandling::SendAtSubscribe,
        "sent as asked: the server is the one that never sends a retained \
         message to a shared subscription, and rewriting the byte here would \
         hide which server honoured it"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The whole Will reaches the wire: topic, payload, QoS, the retain flag and
/// its four properties including the Will Delay Interval (3.1.3.2)
/// [mqtt5 §4.5].
#[tokio::test]
async fn the_whole_will_reaches_the_wire() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.will = Some(WillMessage {
        topic: "status/client".into(),
        payload: b"gone".to_vec(),
        qos: QoS::AtLeastOnce,
        retain: true,
        delay: Some(Duration::from_secs(30)),
        message_expiry: Some(Duration::from_secs(60)),
        content_type: Some("text/plain".into()),
        response_topic: Some("status/ask".into()),
        correlation_data: Some(b"id".to_vec()),
    });
    let (client, _events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    let recorded = server.seen_bytes().await;
    let (Packet::Connect(connect), _) = Packet::decode(&recorded[0], u32::MAX).expect("a connect")
    else {
        panic!("a connect")
    };
    let will = connect.will.as_ref().expect("a will");
    assert_eq!(will.topic, "status/client");
    assert_eq!(will.payload, b"gone");
    assert_eq!(will.qos, QoS::AtLeastOnce);
    assert!(will.retain);
    assert_eq!(
        will.properties.will_delay_interval,
        Some(30),
        "the interval that decides whether a reconnect can beat the Will \
         ([MQTT-3.1.3-9])"
    );
    assert_eq!(will.properties.message_expiry_interval, Some(60));
    assert_eq!(will.properties.content_type, Some("text/plain"));
    assert_eq!(will.properties.response_topic, Some("status/ask"));
    assert_eq!(will.properties.correlation_data, Some(&b"id"[..]));

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// The three ways a connection ends, which is the client's whole half of the
/// Will contract.
///
/// An orderly DISCONNECT 0x00 discards the Will ([MQTT-3.14.4-3]); 0x04
/// `DisconnectWithWillMessage` asks for it anyway; and dropping the handle
/// closes the transport with **no DISCONNECT at all**, which is the abnormal
/// disconnection the mechanism exists for.
#[tokio::test]
async fn the_three_ways_a_connection_ends_are_distinguishable_on_the_wire() {
    let server = Server::start_all(vec![
        vec![Act::Send(connack(Properties::new())), Act::Expect],
        vec![Act::Send(connack(Properties::new())), Act::Expect],
        vec![
            Act::Send(connack(Properties::new())),
            Act::Idle(Duration::from_secs(2)),
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let will = Some(WillMessage {
        topic: "status/client".into(),
        payload: b"gone".to_vec(),
        ..WillMessage::default()
    });

    // 1. Orderly: the Will is discarded.
    let mut with_will = options();
    with_will.will = will.clone();
    let (client, _events) = Client::connect(&context, &address, with_will.clone())
        .await
        .expect("connects");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    drop(client);

    // 2. Orderly, but asking for the Will anyway.
    let (client, _events) = Client::connect(&context, &address, with_will.clone())
        .await
        .expect("connects");
    client
        .disconnect(DisconnectReasonCode::DisconnectWithWillMessage)
        .await
        .expect("disconnects");
    drop(client);

    // 3. Abnormal: the handle is dropped, so the transport closes with
    // nothing written. Inventing an orderly close here would suppress the
    // Will, which is why the task does not.
    let (client, mut events) = Client::connect(&context, &address, with_will)
        .await
        .expect("connects");
    drop(client);
    let event = events.next().await.expect("the close is reported");
    assert!(matches!(event, Event::Disconnected(_)), "{event:?}");

    let recorded = server.seen_bytes().await;
    let codes: Vec<Option<u8>> = recorded
        .iter()
        .filter_map(|raw| {
            let (packet, _) = Packet::decode(raw, u32::MAX).expect("a packet");
            match packet {
                Packet::Disconnect(disconnect) => Some(Some(disconnect.reason_code.as_byte())),
                Packet::Connect(_) => None,
                other => panic!("unexpected {}", other.packet_type()),
            }
        })
        .collect();
    assert_eq!(
        codes,
        [Some(0x00), Some(0x04)],
        "two DISCONNECTs for three connections: the third wrote nothing, which \
         is what makes it an abnormal disconnection ([MQTT-3.1.2-8])"
    );
    let types = server.seen().await;
    assert_eq!(
        types
            .iter()
            .filter(|packet| **packet == PacketType::Connect)
            .count(),
        3,
        "all three connections happened"
    );
}

/// A Will Retain against a server that declared `Retain Available` 0 cannot
/// be refused at configuration time, and this records why rather than
/// pretending otherwise: **the Will goes out in the CONNECT, before the
/// CONNACK that carries the declaration exists.**
///
/// The server's answer is CONNACK 0x9A (Retain not supported)
/// ([MQTT-3.2.2-14]), and what this client owes is to surface that code
/// instead of a closed socket - which is the same contract every refusal
/// carries (`connection.rs`).
#[tokio::test]
async fn will_retain_against_a_server_without_retain_is_the_connacks_code() {
    let refusal = bytes(&Packet::Connack(Connack {
        session_present: false,
        reason_code: ConnectReasonCode::RetainNotSupported,
        properties: Properties::new(),
    }));
    let mut server = Server::start(vec![Act::Send(refusal)]).await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.will = Some(WillMessage {
        topic: "status/client".into(),
        payload: b"gone".to_vec(),
        retain: true,
        ..WillMessage::default()
    });

    let error = Client::connect(&context, &server.address(), options)
        .await
        .expect_err("refused");
    assert_eq!(error.reason_code(), Some(0x9A));
    server.finished().await;

    // A *publish* with RETAIN is refused locally, because by then the
    // declaration has arrived - the asymmetry is the handshake's, not ours.
    let limits = ServerLimits {
        retain_available: false,
        ..ServerLimits::default()
    };
    assert_eq!(
        Message::new("a", "b")
            .retained()
            .check(&limits)
            .expect_err("refused")
            .reason_code(),
        Some(0x9A)
    );
}

/// The Will Topic is a Topic **Name**: no wildcards, never zero-length
/// ([MQTT-3.1.3-10]), refused at configuration time because nothing about it
/// depends on the server.
#[tokio::test]
async fn a_will_topic_is_a_topic_name_and_is_refused_otherwise() {
    let context = Context::new().expect("ambient");
    // No server is started: none of these reaches a socket, which is the
    // point.
    for topic in ["status/+", "status/#", ""] {
        let mut options = options();
        options.will = Some(WillMessage {
            topic: topic.into(),
            payload: b"gone".to_vec(),
            ..WillMessage::default()
        });
        assert!(options.validate().is_err(), "{topic:?} is not a Topic Name");
        assert!(
            Client::connect(&context, "127.0.0.1:1", options)
                .await
                .is_err(),
            "{topic:?} never reaches a dial"
        );
    }

    // A Will Delay Interval above the Four Byte Integer is refused the same
    // way.
    let mut options = options();
    options.will = Some(WillMessage {
        topic: "status/client".into(),
        delay: Some(Duration::from_secs(u64::from(u32::MAX) + 1)),
        ..WillMessage::default()
    });
    assert!(options.validate().is_err());
}

/// A Session the client resumes is what can beat the Will: "the server MUST
/// NOT publish the Will at all if a new connection to that session arrives
/// inside the Will Delay Interval" ([MQTT-3.1.3-9]) [mqtt5 §4.5].
///
/// The publishing is the server's, so what is asserted here is the client
/// half that makes it possible: after an abnormal close the client can
/// reconnect **to the same session** - same Client Identifier, Clean Start 0 -
/// and the CONNECT it sends declares the Will again, because the Will is
/// per-connection state and not per-session.
#[tokio::test]
async fn a_resumption_inside_the_delay_re_declares_the_will() {
    let server = Server::start_all(vec![
        vec![Act::Send(connack(Properties::new())), Act::Close],
        vec![
            Act::Send(bytes(&Packet::Connack(Connack {
                session_present: true,
                reason_code: ConnectReasonCode::Success,
                properties: Properties::new(),
            }))),
            Act::Idle(Duration::from_secs(2)),
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let session = Session::new("retain-client", &options().limits);
    let mut with_will = options();
    with_will.session_expiry = Some(Duration::from_secs(300));
    // Clean Start 0 from the first connection: a session that outlives the
    // connection is the precondition [MQTT-3.1.3-9] needs.
    with_will.clean_start = false;
    with_will.will = Some(WillMessage {
        topic: "status/client".into(),
        payload: b"gone".to_vec(),
        delay: Some(Duration::from_secs(30)),
        ..WillMessage::default()
    });

    let (client, mut events) =
        Client::connect_session(&context, &address, with_will.clone(), &session)
            .await
            .expect("connects");
    drop(client);
    events.next().await.expect("the close is reported");

    // Clean Start stays 0, so this is the *same* session - which is what
    // [MQTT-3.1.3-9] is about.
    let mut resume = with_will;
    resume.clean_start = false;
    let (client, _events) = Client::connect_session(&context, &address, resume, &session)
        .await
        .expect("resumes");
    assert!(client.session_present(), "the server resumed it");

    let recorded = server.seen_bytes().await;
    let connects: Vec<_> = recorded
        .iter()
        .filter_map(|raw| match Packet::decode(raw, u32::MAX) {
            Ok((Packet::Connect(connect), _)) => Some(connect),
            _ => None,
        })
        .collect();
    assert_eq!(connects.len(), 2);
    for connect in &connects {
        assert!(!connect.clean_start, "the same session both times");
        assert_eq!(connect.client_id, "retain-client");
        let will = connect.will.as_ref().expect("declared again");
        assert_eq!(will.properties.will_delay_interval, Some(30));
    }

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}
