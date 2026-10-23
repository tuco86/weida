//! Delivery on the wire: both delivery state machines in both directions, the
//! send quota, and the four points a QoS 2 handshake can be interrupted at.
//!
//! # The four lost-packet cases
//!
//! Figure 4.3 of the specification has four packets — PUBLISH, PUBREC, PUBREL,
//! PUBCOMP — and an interrupted connection can lose any one of them
//! [mqtt5 §6]. Each gets a test, because each leaves the two peers in a
//! *different* state and the correct recovery differs:
//!
//! | Lost | The client resends | Because | The server answers |
//! | --- | --- | --- | --- |
//! | PUBLISH | the PUBLISH, DUP 1 | it never got a PUBREC ([MQTT-4.4.0-1]) | PUBREC; one delivery |
//! | PUBREC | the PUBLISH, DUP 1 | the client cannot tell this from the first case | another PUBREC, and **no second delivery** ([MQTT-4.3.3-10]) |
//! | PUBREL | the PUBREL only | "MUST NOT resend the PUBLISH once PUBREL has been sent" ([MQTT-4.3.3-6]) | PUBCOMP 0x00 |
//! | PUBCOMP | the PUBREL only | same rule | PUBCOMP **0x92**, because it already released the identifier |
//!
//! The last row is why the four are four rather than two: on the wire the
//! PUBREL and PUBCOMP cases are identical from the client's side, and only the
//! server's answer distinguishes them. 0x92 "is not an error during recovery"
//! (3.6.2.1) [mqtt5 §6], so the client must complete the exchange on it rather
//! than hang or fail — and the second and fourth rows together are the
//! "neither loss nor duplication" of QoS 2.

use crate::harness;

use std::time::Duration;

use harness::{Act, Server, bytes, connack_session, decode};
use weida_mqtt::{
    Client, Completion, ConnectOptions, Context, DisconnectReasonCode, Error, Event, Message,
    Packet, QoS, Session,
};
use weida_mqtt_codec::{PacketType, Properties, PubackReasonCode, PubcompReasonCode, Publish};

fn options(clean_start: bool) -> ConnectOptions {
    let mut options = harness::options("qos-client");
    options.clean_start = clean_start;
    options.session_expiry = Some(Duration::from_secs(300));
    options
}

/// QoS 0: no identifier, no response, and nothing stored. "The message arrives
/// at the receiver either once or not at all" (4.3.1) [mqtt5 §6], so
/// [`Completion::Sent`] is the strongest true statement.
#[tokio::test]
async fn qos_0_completes_at_once_and_stores_nothing() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::AckPublish,
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");

    let completion = client
        .publish(Message::new("a/b", "hi"))
        .await
        .expect("publishes");
    assert_eq!(completion, Completion::Sent);
    assert!(!completion.is_error());
    assert_eq!(completion.reason_code(), None, "QoS 0 certifies nothing");
    assert_eq!(session.in_flight(), 0, "QoS 0 is never stored");

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let Packet::Publish(publish) = decode(&recorded[1]) else {
        panic!("a publish")
    };
    assert_eq!(publish.qos, QoS::AtMostOnce);
    assert_eq!(publish.packet_id, None, "no identifier at QoS 0 (3.3.2.2)");
    assert!(!publish.dup, "DUP MUST be 0 at QoS 0 ([MQTT-3.3.1-2])");
}

/// QoS 1: PUBLISH out with DUP 0 ([MQTT-4.3.2-2]), PUBACK in, identifier
/// freed.
#[tokio::test]
async fn qos_1_completes_on_the_puback_and_frees_its_identifier() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::AckPublish,
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");

    let completion = client
        .publish(Message::new("a/b", "hi").at(QoS::AtLeastOnce))
        .await
        .expect("publishes");
    assert_eq!(
        completion,
        Completion::Acknowledged(PubackReasonCode::Success)
    );
    assert_eq!(session.in_flight(), 0, "the identifier is freed on PUBACK");
    assert_eq!(server.deliveries(), 1);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let Packet::Publish(publish) = decode(&recorded[1]) else {
        panic!("a publish")
    };
    assert!(publish.packet_id.is_some());
    assert!(!publish.dup, "a first attempt is DUP 0 ([MQTT-4.3.2-2])");
}

/// QoS 2: four packets, two round trips, and the identifier freed on the
/// PUBCOMP.
#[tokio::test]
async fn qos_2_completes_on_the_pubcomp() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::AckPublish,     // answers PUBREC
        Act::CompletePubrel, // answers PUBCOMP
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");

    let completion = client
        .publish(Message::new("a/b", "hi").at(QoS::ExactlyOnce))
        .await
        .expect("publishes");
    assert_eq!(completion, Completion::Complete(PubcompReasonCode::Success));
    assert_eq!(session.in_flight(), 0);
    assert_eq!(server.deliveries(), 1);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    assert_eq!(
        server.seen().await,
        [
            PacketType::Connect,
            PacketType::Publish,
            PacketType::Pubrel,
            PacketType::Disconnect
        ]
    );
}

/// A PUBREC of 0x80 or above ends the exchange: the message "counts as
/// acknowledged and MUST NOT be retransmitted" ([MQTT-4.4.0-2]) [mqtt5 §6], so
/// no PUBREL goes out, the identifier frees, and nothing is left to resend.
#[tokio::test]
async fn a_failing_pubrec_ends_the_exchange_without_a_pubrel() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::RefusePublish(0x87), // Not authorized
        Act::Expect,              // the DISCONNECT, and no PUBREL before it
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");

    let completion = client
        .publish(Message::new("a/b", "hi").at(QoS::ExactlyOnce))
        .await
        .expect("the exchange ends");
    assert_eq!(
        completion,
        Completion::Refused(PubackReasonCode::NotAuthorized)
    );
    assert!(completion.is_error());
    assert_eq!(completion.reason_code(), Some(0x87));
    assert_eq!(session.in_flight(), 0, "the identifier is freed");
    assert!(
        session.resend().is_empty(),
        "a message answered >= 0x80 is never resent ([MQTT-4.4.0-2])"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
    assert_eq!(
        server.seen().await,
        [
            PacketType::Connect,
            PacketType::Publish,
            PacketType::Disconnect
        ],
        "no PUBREL for a refused PUBREC"
    );
}

/// `0x10 No matching subscribers` is a **success** and the only in-protocol
/// signal that a message reached nobody [mqtt5 §12/P16].
#[tokio::test]
async fn no_matching_subscribers_is_a_success() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::RefusePublish(0x10),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");
    let completion = client
        .publish(Message::new("a/b", "hi").at(QoS::AtLeastOnce))
        .await
        .expect("publishes");
    assert_eq!(
        completion,
        Completion::Acknowledged(PubackReasonCode::NoMatchingSubscribers)
    );
    assert!(!completion.is_error(), "0x10 is below the 0x80 line");
    assert_eq!(completion.reason_code(), Some(0x10));

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// Lost-packet case 1 of 4: the **PUBLISH** never arrived. The client resends
/// it with DUP 1 on the resumed session and the exchange completes with one
/// delivery.
#[tokio::test]
async fn a_lost_publish_is_replayed_on_reconnect() {
    let mut server = Server::start_all(vec![
        // The PUBLISH is read and not answered, then the connection drops:
        // from the client's side this is a lost PUBLISH.
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Expect,
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::AckPublish,     // the resent PUBLISH, answered PUBREC
            Act::CompletePubrel, // PUBCOMP
            Act::Expect,         // the DISCONNECT
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    let publishing = tokio::spawn({
        let message = Message::new("a/b", "hi").at(QoS::ExactlyOnce);
        async move { client.publish(message).await }
    });

    // The connection dies with the exchange in flight, so the publish handle
    // never resolves — the session, not the handle, is what survives.
    let event = events.next().await.expect("the close is reported");
    assert!(matches!(event, Event::Disconnected(_)), "{event:?}");
    assert!(publishing.await.expect("the task finishes").is_err());
    server.finished().await;
    assert_eq!(session.in_flight(), 1, "the exchange is session state");

    let (client, mut events) =
        Client::connect_session(&context, &address, options(false), &session)
            .await
            .expect("resumes");

    // No handle is left, so the completion arrives as an event.
    let Some(Event::Completed {
        completion,
        packet_id,
    }) = events.next().await
    else {
        panic!("the resumed exchange must report its completion")
    };
    assert_eq!(completion, Completion::Complete(PubcompReasonCode::Success));
    assert!(packet_id > 0);
    assert_eq!(session.in_flight(), 0);
    assert_eq!(server.deliveries(), 1, "neither loss nor duplication");

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let Packet::Publish(first) = decode(&recorded[1]) else {
        panic!("a publish")
    };
    let Packet::Publish(resent) = decode(&recorded[3]) else {
        panic!("a publish")
    };
    assert!(!first.dup, "the first attempt is DUP 0");
    assert!(resent.dup, "the replay is DUP 1 ([MQTT-3.3.1-1])");
    assert_eq!(resent.packet_id, first.packet_id, "the original identifier");
}

/// Lost-packet case 2 of 4: the **PUBREC** was lost. The client cannot tell
/// this from case 1, so it resends the PUBLISH with DUP 1 — and the server,
/// which did accept the message, answers with another PUBREC and MUST NOT
/// deliver it again ([MQTT-4.3.3-10]) [mqtt5 §6]. One delivery is the whole
/// assertion.
#[tokio::test]
async fn a_lost_pubrec_is_replayed_without_a_second_delivery() {
    let mut server = Server::start_all(vec![
        // The server **accepts** the message and answers nothing: the PUBREC
        // is lost. Expressed as an accept rather than as a send-then-close,
        // because a race between the write and the close would decide which
        // packet was lost and a test must not be decided by a race.
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::AcceptPublish,
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::AckPublish, // the resent PUBLISH: a repeat, so no new delivery
            Act::CompletePubrel,
            Act::Expect,
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    let publishing = tokio::spawn({
        let message = Message::new("a/b", "hi").at(QoS::ExactlyOnce);
        async move { client.publish(message).await }
    });
    events.next().await.expect("the close is reported");
    let _ = publishing.await;
    server.finished().await;

    let (client, mut events) =
        Client::connect_session(&context, &address, options(false), &session)
            .await
            .expect("resumes");
    let Some(Event::Completed { completion, .. }) = events.next().await else {
        panic!("the resumed exchange must complete")
    };
    assert_eq!(completion, Completion::Complete(PubcompReasonCode::Success));
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    assert_eq!(
        server.deliveries(),
        1,
        "the repeat was answered again and not delivered twice ([MQTT-4.3.3-10])"
    );
    // The client resent the PUBLISH, not the PUBREL: it had no PUBREC, so it
    // could not have sent a PUBREL.
    let recorded = server.seen_bytes().await;
    let Packet::Publish(resent) = decode(&recorded[3]) else {
        panic!("a publish, not a pubrel")
    };
    assert!(resent.dup);
}

/// Lost-packet case 3 of 4: the **PUBREL** was lost. The client resends the
/// PUBREL and **not** the PUBLISH — "MUST NOT resend the PUBLISH once PUBREL
/// has been sent" ([MQTT-4.3.3-6]) [mqtt5 §6] — and the server, which still
/// holds the identifier, answers PUBCOMP 0x00.
#[tokio::test]
async fn a_lost_pubrel_replays_the_pubrel_and_not_the_publish() {
    let mut server = Server::start_all(vec![
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::AckPublish, // PUBREC
            Act::Expect,     // the PUBREL, read and not answered
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::CompletePubrel, // the resent PUBREL, answered 0x00
            Act::Expect,
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    let publishing = tokio::spawn({
        let message = Message::new("a/b", "hi").at(QoS::ExactlyOnce);
        async move { client.publish(message).await }
    });
    events.next().await.expect("the close is reported");
    let _ = publishing.await;
    server.finished().await;

    let (client, mut events) =
        Client::connect_session(&context, &address, options(false), &session)
            .await
            .expect("resumes");
    let Some(Event::Completed { completion, .. }) = events.next().await else {
        panic!("the resumed exchange must complete")
    };
    assert_eq!(completion, Completion::Complete(PubcompReasonCode::Success));
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let types = server.seen().await;
    assert_eq!(
        types,
        [
            PacketType::Connect,
            PacketType::Publish,
            PacketType::Pubrel,
            PacketType::Connect,
            PacketType::Pubrel,
            PacketType::Disconnect
        ],
        "the second connection carries a PUBREL and no PUBLISH"
    );
    assert_eq!(server.deliveries(), 1);
}

/// Lost-packet case 4 of 4: the **PUBCOMP** was lost. On the wire this is
/// identical to case 3 from the client's side — it resends the PUBREL — and
/// only the server's answer differs: it has already released the identifier,
/// so it answers PUBCOMP **0x92**. "Not an error during recovery" (3.6.2.1)
/// [mqtt5 §6], so the client completes the exchange on it rather than hanging
/// or failing.
#[tokio::test]
async fn a_lost_pubcomp_completes_on_0x92() {
    let mut server = Server::start_all(vec![
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::AckPublish, // PUBREC
            // Released by the server, and the PUBCOMP lost on the way back —
            // again an accept rather than a send-then-close, so the test is
            // not decided by a race.
            Act::AcceptPubrel,
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::ForgetPubrel, // the resent PUBREL, answered 0x92
            Act::Expect,
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    let publishing = tokio::spawn({
        let message = Message::new("a/b", "hi").at(QoS::ExactlyOnce);
        async move { client.publish(message).await }
    });
    events.next().await.expect("the close is reported");
    let _ = publishing.await;
    server.finished().await;

    let (client, mut events) =
        Client::connect_session(&context, &address, options(false), &session)
            .await
            .expect("resumes");
    let Some(Event::Completed { completion, .. }) = events.next().await else {
        panic!("0x92 must complete the exchange, not hang it")
    };
    assert_eq!(
        completion,
        Completion::Complete(PubcompReasonCode::PacketIdentifierNotFound)
    );
    assert_eq!(completion.reason_code(), Some(0x92));
    assert_eq!(
        session.in_flight(),
        0,
        "the identifier is released either way"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
    assert_eq!(
        server.deliveries(),
        1,
        "the message was delivered exactly once across the whole recovery"
    );
}

/// The send quota is the server's `Receive Maximum` counted in QoS 1 and 2
/// PUBLISH packets **and nothing else** ([MQTT-4.9.0-1], 4.9) [mqtt5 §5], and
/// exhausting it **stalls** the sender rather than exceeding it: the third
/// publish's future stays pending and its packet stays off the wire.
///
/// The server here never answers, so the stall never releases and the two
/// pending futures never resolve — which is the point. The release half, that
/// an acknowledgement makes room for exactly one more, is asserted by
/// `session::tests::allocation_stops_at_the_peers_receive_maximum`, where it
/// needs no socket.
#[tokio::test]
async fn the_quota_stalls_the_sender_rather_than_being_exceeded() {
    let server = Server::start(vec![
        Act::Send(connack_session(
            false,
            Properties {
                receive_maximum: Some(2),
                ..Properties::new()
            },
        )),
        Act::Expect, // the first PUBLISH, deliberately unanswered
        Act::Expect, // the second
        Act::Expect, // the QoS 0 publish, which the spent quota does not stop
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");
    assert_eq!(session.send_quota(), 2, "the server's Receive Maximum");

    let client = std::sync::Arc::new(client);
    let mut handles = Vec::new();
    for index in 0..3 {
        let client = std::sync::Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            client
                .publish(Message::new(format!("t/{index}"), "x").at(QoS::AtLeastOnce))
                .await
        }));
    }

    // Two are on the wire; the third is stalled, so it has not been sent.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(session.in_flight(), 2, "never more than the quota");
    let types = server.seen().await;
    assert_eq!(
        types.iter().filter(|t| **t == PacketType::Publish).count(),
        2,
        "the third publish is stalled off the wire: {types:?}"
    );
    assert!(
        handles.iter().all(|handle| !handle.is_finished()),
        "all three futures are still pending, including the two on the wire"
    );

    // QoS 0 is not counted and does not wait: the quota's unit is "one QoS 1
    // or QoS 2 PUBLISH packet — not bytes, not QoS 0, not any other packet
    // type" [mqtt5 §5]. So this one goes out **past** a spent quota, and the
    // third PUBLISH on the wire is it and not the stalled QoS 1 one.
    client
        .publish(Message::new("t/qos0", "x"))
        .await
        .expect("QoS 0 never stalls");

    weida_testkit::within(
        "the QoS 0 publish to reach the server",
        Duration::from_secs(5),
        async {
            while server.seen_bytes().await.len() < 4 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await;
    let recorded = server.seen_bytes().await;
    let Packet::Publish(third) = decode(&recorded[3]) else {
        panic!("a publish")
    };
    assert_eq!(
        third.topic, "t/qos0",
        "the QoS 0 publish overtook the stalled QoS 1 one"
    );
    assert_eq!(third.qos, QoS::AtMostOnce);
    assert_eq!(session.in_flight(), 2, "and it was not counted against it");

    // The two acknowledged futures will never resolve, by construction: the
    // server is silent. Abandoning them is the end of the test, not a leak —
    // the connection dies with the runtime.
    for handle in handles {
        handle.abort();
    }
}

/// The inbound direction of QoS 1: the client delivers the message and
/// acknowledges it. The acknowledgement is sent **after** the delivery has
/// been read but without waiting for the application, which is what the
/// footnote to figure 4.2 permits: "the receiver does not need to complete
/// delivery of the Application Message before sending the PUBACK"
/// ([MQTT-4.3.2-4]) [mqtt5 §6].
#[tokio::test]
async fn an_inbound_qos_1_publish_is_delivered_and_acknowledged() {
    let delivery = bytes(&Packet::Publish(Publish {
        topic: "in/1",
        payload: b"hello",
        qos: QoS::AtLeastOnce,
        dup: false,
        retain: true,
        packet_id: Some(77),
        properties: Properties {
            content_type: Some("text/plain"),
            ..Properties::new()
        },
    }));
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::Send(delivery),
        Act::Expect, // the PUBACK
        Act::Expect, // the DISCONNECT
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, mut events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");

    let Some(Event::Delivered(received)) = events.next().await else {
        panic!("the message must be delivered")
    };
    assert_eq!(received.topic, "in/1");
    assert_eq!(received.payload, &b"hello"[..]);
    assert_eq!(received.qos, QoS::AtLeastOnce);
    assert!(received.retain);
    assert_eq!(received.packet_id, Some(77));
    assert_eq!(
        received.properties.content_type.as_deref(),
        Some("text/plain")
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let Packet::Puback(puback) = decode(&recorded[1]) else {
        panic!("a puback")
    };
    assert_eq!(puback.packet_id, 77);
    assert_eq!(puback.reason_code, PubackReasonCode::Success);
}

/// The inbound direction of QoS 2, including the suppression that makes it
/// exactly-once **for this hop**: a repeat of a Packet Identifier still
/// awaiting its PUBREL is answered with another PUBREC and MUST NOT be
/// delivered again ([MQTT-4.3.3-10]) [mqtt5 §6].
#[tokio::test]
async fn an_inbound_qos_2_repeat_is_answered_again_and_not_delivered_twice() {
    let first = bytes(&Packet::Publish(Publish {
        topic: "in/2",
        payload: b"once",
        qos: QoS::ExactlyOnce,
        dup: false,
        retain: false,
        packet_id: Some(9),
        properties: Properties::new(),
    }));
    let repeat = bytes(&Packet::Publish(Publish {
        topic: "in/2",
        payload: b"once",
        qos: QoS::ExactlyOnce,
        dup: true,
        retain: false,
        packet_id: Some(9),
        properties: Properties::new(),
    }));
    let pubrel = bytes(&Packet::Pubrel(weida_mqtt_codec::Pubrel::new(9)));

    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::Send(first),
        Act::Expect, // PUBREC
        Act::Send(repeat),
        Act::Expect, // another PUBREC, and no second delivery
        Act::Send(pubrel),
        Act::Expect, // PUBCOMP
        Act::Expect, // the DISCONNECT
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, mut events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");

    let Some(Event::Delivered(received)) = events.next().await else {
        panic!("the message must be delivered once")
    };
    assert_eq!(received.payload, &b"once"[..]);

    // Wait for the whole exchange rather than racing it: the script still has
    // the repeat and the PUBREL to send, and a DISCONNECT sent now would cut
    // the connection before the second PUBREC and the PUBCOMP.
    // The PUBREL releasing the identifier is the observable end of it
    // ([MQTT-4.3.3-12]).
    weida_testkit::until(
        "the PUBREL to release the identifier",
        Duration::from_secs(5),
        || !session.holds_inbound_qos2(9),
    )
    .await;
    assert!(
        !session.holds_inbound_qos2(9),
        "the PUBREL released the identifier ([MQTT-4.3.3-12])"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let types = server.seen().await;
    assert_eq!(
        types,
        [
            PacketType::Connect,
            PacketType::Pubrec,
            PacketType::Pubrec,
            PacketType::Pubcomp,
            PacketType::Disconnect
        ],
        "two PUBRECs for one delivery"
    );

    // Exactly one delivery reached the application, which is the whole claim.
    let mut extra = 0;
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), events.next()).await
    {
        if matches!(event, Event::Delivered(_)) {
            extra += 1;
        }
    }
    assert_eq!(extra, 0, "the repeat was not delivered again");

    let recorded = server.seen_bytes().await;
    let Packet::Pubcomp(pubcomp) = decode(&recorded[3]) else {
        panic!("a pubcomp")
    };
    assert_eq!(pubcomp.reason_code, PubcompReasonCode::Success);
}

/// A PUBREL for an identifier this client does not hold is answered 0x92,
/// which the specification explicitly declines to call an error during
/// recovery (3.6.2.1) [mqtt5 §6] — so the connection survives it.
#[tokio::test]
async fn an_unknown_pubrel_is_answered_0x92_and_the_connection_survives() {
    let pubrel = bytes(&Packet::Pubrel(weida_mqtt_codec::Pubrel::new(4242)));
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::Send(pubrel),
        Act::Expect, // the PUBCOMP 0x92
        Act::Expect, // the DISCONNECT, so the connection did survive
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("qos-client", &ConnectOptions::default().limits);

    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");
    tokio::time::sleep(Duration::from_millis(100)).await;
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("the connection survived the unknown PUBREL");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let Packet::Pubcomp(pubcomp) = decode(&recorded[1]) else {
        panic!("a pubcomp")
    };
    assert_eq!(
        pubcomp.reason_code,
        PubcompReasonCode::PacketIdentifierNotFound
    );
    assert_eq!(pubcomp.packet_id, 4242);
}

/// The client's own `Receive Maximum` bounds what the server may have in
/// flight toward it: a server that exceeds it is reported rather than letting
/// the receive table grow. "Exceeding the peer's Receive Maximum earns
/// DISCONNECT 0x93" [mqtt5 §5].
#[tokio::test]
async fn a_server_that_exceeds_the_declared_receive_maximum_is_reported() {
    let mut script = vec![Act::Send(connack_session(false, Properties::new()))];
    for packet_id in 1..=3u16 {
        script.push(Act::Send(bytes(&Packet::Publish(Publish {
            topic: "in/flood",
            payload: b"x",
            qos: QoS::ExactlyOnce,
            dup: false,
            retain: false,
            packet_id: Some(packet_id),
            properties: Properties::new(),
        }))));
    }
    let mut server = Server::start(script).await;
    let context = Context::new().expect("ambient");

    let mut options = options(true);
    // Two unreleased QoS 2 identifiers is all this client will hold.
    options.limits.receive_maximum = 2;
    let session = Session::new("qos-client", &options.limits);
    let (_client, mut events) =
        Client::connect_session(&context, &server.address(), options, &session)
            .await
            .expect("connects");

    let mut error = None;
    while let Some(event) = events.next().await {
        if let Event::Disconnected(reported) = event {
            error = Some(reported);
            break;
        }
    }
    let error = error.expect("the flood must be reported");
    assert!(
        matches!(error, Error::ReceiveMaximumExceeded { quota: 2 }),
        "{error}"
    );
    assert_eq!(error.reason_code(), Some(0x93));
    server.finished().await;
}
