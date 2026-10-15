//! The session on the wire: Clean Start, `Session Present`, the DISCONNECT
//! expiry rule, and the one circumstance that permits retransmission.
//!
//! The publish path itself is `qos.rs`'s, so these tests put messages into the
//! session through [`Session::allocate`] — which is the session's own public
//! surface — and then assert what the *connection* does with them. That
//! separation is deliberate: what the session owes is the rule about when a
//! stored exchange goes back on the wire, and that is checkable without a
//! publisher.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::{Act, Server, bytes, connack_session, decode};
use weida_mqtt::{
    Client, ConnectOptions, Context, DisconnectReasonCode, Error, Packet, QoS, Resumption, Session,
    StoredPublish,
};
use weida_mqtt_codec::{PacketType, Properties};

fn options(clean_start: bool) -> ConnectOptions {
    let mut options = harness::options("session-client");
    options.clean_start = clean_start;
    options.keep_alive = Duration::from_secs(60);
    options.session_expiry = Some(Duration::from_secs(300));
    options
}

fn message(topic: &str, qos: QoS) -> Arc<StoredPublish> {
    Arc::new(StoredPublish {
        topic: topic.into(),
        payload: weida_mqtt::Bytes::from_static(b"body"),
        qos,
        retain: false,
        payload_format_indicator: None,
        message_expiry_interval: None,
        content_type: None,
        response_topic: None,
        correlation_data: None,
        user_properties: Vec::new(),
    })
}

/// The reconnect: a QoS 1 message left unacknowledged on one connection goes
/// back out on the next, **with its original Packet Identifier and DUP 1**
/// ([MQTT-4.4.0-1], [MQTT-3.3.1-1]).
#[tokio::test]
async fn an_unacknowledged_publish_is_resent_on_reconnect_with_dup_and_its_identifier() {
    let mut server = Server::start_all(vec![
        // First connection: accept, then drop it with the message in flight.
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Close,
        ],
        // Second connection: Session Present 1, so the client resumes.
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::Expect, // the resent PUBLISH
            Act::Expect, // the DISCONNECT
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    assert_eq!(client.resumption(), Resumption::Fresh);

    // Stand in for the publish path: the message is in the session, sent but
    // not acknowledged.
    let packet_id = session
        .allocate(message("a/b", QoS::AtLeastOnce))
        .expect("room in the quota");
    assert_eq!(session.in_flight(), 1);

    // The server drops the connection, which is always available to it.
    let event = events.next().await.expect("the close is reported");
    drop(client);
    drop(event);
    server.finished().await;

    // Reconnect on the same session with Clean Start 0.
    let (client, _events) = Client::connect_session(&context, &address, options(false), &session)
        .await
        .expect("resumes");
    assert_eq!(client.resumption(), Resumption::Resumed);
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let types: Vec<PacketType> = server.seen().await;
    assert_eq!(
        types,
        [
            PacketType::Connect,
            PacketType::Connect,
            PacketType::Publish,
            PacketType::Disconnect
        ],
        "the PUBLISH appears only on the second connection"
    );

    let Packet::Publish(resent) = decode(&recorded[2]) else {
        panic!("a publish")
    };
    assert_eq!(resent.packet_id, Some(packet_id), "the original identifier");
    assert!(resent.dup, "DUP 1 ([MQTT-3.3.1-1])");
    assert_eq!(resent.topic, "a/b");
    assert_eq!(resent.qos, QoS::AtLeastOnce);
    // A stored message carries its full Topic Name and no alias, because
    // "a receiver MUST NOT carry mappings across connections"
    // ([MQTT-3.3.2-7]).
    assert_eq!(resent.properties.topic_alias, None);
}

/// **The test that earns [MQTT-4.4.0-1]**: "Clients and Servers MUST NOT
/// resend messages at any other time." Nothing is resent inside a live
/// connection, however long it stays open — the keep-alive interval is one
/// second here so the connection lives through several of them and the only
/// thing on the wire is PINGREQ.
///
/// This asserts an **absence**, which is the whole point: EMQX ships a
/// 30-second `retry_interval` and Paho's Python client republishes after a
/// reconnect even with `clean_session=True`, calls that non-compliant itself,
/// and warns QoS 2 messages can therefore arrive twice [mqtt5 §6].
#[tokio::test]
async fn nothing_is_resent_inside_a_live_connection() {
    let pingresp = bytes(&Packet::Pingresp);
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::Expect,
        Act::Send(pingresp.clone()),
        Act::Expect,
        Act::Send(pingresp.clone()),
        Act::Expect,
        Act::Send(pingresp),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let mut options = options(true);
    options.keep_alive = Duration::from_secs(1);
    let (client, _events) = Client::connect_session(&context, &server.address(), options, &session)
        .await
        .expect("connects");

    // Three messages in flight, two of them QoS 2 and one past its PUBREC, so
    // every stage that *could* be resent is represented.
    let first = session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
    let second = session.allocate(message("b", QoS::ExactlyOnce)).unwrap();
    session.allocate(message("c", QoS::ExactlyOnce)).unwrap();
    assert!(session.pubrec_received(second));
    assert_eq!(session.in_flight(), 3);
    assert_eq!(session.resend().len(), 3, "all three would be resent");

    // Three keep-alive intervals of doing nothing.
    tokio::time::sleep(Duration::from_millis(3300)).await;
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("still connected");
    server.finished().await;

    let types = server.seen().await;
    assert!(
        types.iter().all(|packet_type| matches!(
            packet_type,
            PacketType::Connect | PacketType::Pingreq | PacketType::Disconnect
        )),
        "nothing but CONNECT, PINGREQ and DISCONNECT crossed the wire: {types:?}"
    );
    assert!(
        types.iter().filter(|t| **t == PacketType::Pingreq).count() >= 2,
        "the connection really did stay open across intervals: {types:?}"
    );
    // And the session still holds everything, untouched.
    assert_eq!(session.in_flight(), 3);
    assert!(session.stage_of(first).is_some());
}

/// [MQTT-3.2.2-4]: a client with no session state that receives
/// `Session Present` 1 MUST close the connection. Believing the server would
/// mean answering acknowledgements for exchanges this client has no record of.
#[tokio::test]
async fn session_present_without_local_state_closes_the_connection() {
    let mut server = Server::start(vec![Act::Send(connack_session(true, Properties::new()))]).await;
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    assert!(session.is_empty());
    let error = Client::connect_session(&context, &server.address(), options(false), &session)
        .await
        .expect_err("must close");
    assert!(
        matches!(error, Error::SessionPresentWithoutState),
        "{error}"
    );
    server.finished().await;
}

/// [MQTT-3.2.2-5]: a client with state that receives `Session Present` 0 MUST
/// discard it — and then has nothing to resend, which is the observable half.
#[tokio::test]
async fn session_present_zero_discards_local_state() {
    let mut server = Server::start_all(vec![
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Expect, // the DISCONNECT, and no PUBLISH before it
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    session.allocate(message("a", QoS::ExactlyOnce)).unwrap();
    session.inbound_qos2_received(42).unwrap();
    assert!(!session.is_empty());

    events.next().await.expect("the close is reported");
    drop(client);
    server.finished().await;

    let (client, _events) = Client::connect_session(&context, &address, options(false), &session)
        .await
        .expect("connects");
    assert_eq!(client.resumption(), Resumption::Discarded);
    assert!(session.is_empty(), "both halves were discarded");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    assert_eq!(
        server.seen().await,
        [
            PacketType::Connect,
            PacketType::Connect,
            PacketType::Disconnect
        ],
        "a discarded session resends nothing"
    );
}

/// [MQTT-3.1.2-4]: Clean Start 1 discards the session **before** the CONNECT,
/// so a resumed identifier space cannot survive into a session the server
/// threw away.
#[tokio::test]
async fn clean_start_discards_before_connecting() {
    let mut server = Server::start_all(vec![
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Expect, // the DISCONNECT
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
    events.next().await.expect("the close is reported");
    drop(client);
    server.finished().await;

    // Clean Start 1 again: the state is gone before the CONNECT is built, so
    // the server's Session Present 0 is simply agreement rather than a
    // discard.
    let (client, _events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    assert_eq!(client.resumption(), Resumption::Fresh);
    assert!(session.is_empty());
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The rule the codec deferred to the session: "a non-zero Session Expiry
/// Interval on DISCONNECT when CONNECT carried zero is a Protocol Error"
/// (3.14.2.2.2). Refused **before sending**, so the client never earns the
/// server's DISCONNECT 0x82 for it — and the wire shows the packet was not
/// sent.
#[tokio::test]
async fn a_revised_expiry_is_refused_where_connect_declared_none() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::Expect, // exactly one DISCONNECT, the permitted one
    ])
    .await;
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let mut options = options(true);
    options.session_expiry = None; // which means zero
    let (client, _events) = Client::connect_session(&context, &server.address(), options, &session)
        .await
        .expect("connects");

    let error = client
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::from_secs(30)),
        )
        .await
        .expect_err("a Protocol Error, refused locally");
    assert!(matches!(error, Error::Configuration(_)), "{error}");

    // Shortening to zero is always permitted, and is what a client that is
    // finished should do so the session is not orphaned (3.1.2.11.2).
    client
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::ZERO),
        )
        .await
        .expect("zero is permitted");
    server.finished().await;

    assert_eq!(
        server.seen().await,
        [PacketType::Connect, PacketType::Disconnect],
        "the refused DISCONNECT never reached the wire"
    );
}

/// The other half of the same rule: where CONNECT declared a non-zero
/// interval, the client may revise it in either direction at close, and the
/// DISCONNECT carries the property.
#[tokio::test]
async fn a_revised_expiry_is_permitted_where_connect_declared_one() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(false, Properties::new())),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(true), &session)
            .await
            .expect("connects");
    client
        .disconnect_with(
            DisconnectReasonCode::NormalDisconnection,
            Some(Duration::from_secs(30)),
        )
        .await
        .expect("permitted");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    let Packet::Disconnect(disconnect) = decode(recorded.last().expect("a disconnect")) else {
        panic!("a disconnect")
    };
    assert_eq!(disconnect.properties.session_expiry_interval, Some(30));
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::NormalDisconnection,
        "which is what makes the server discard the Will ([MQTT-3.14.4-3])"
    );
}

/// [MQTT-4.3.3-6]: once the PUBREL is out, the PUBLISH is never resent — only
/// the PUBREL is. The wire is the only place that distinction shows.
#[tokio::test]
async fn a_qos2_exchange_past_pubrec_resends_only_the_pubrel() {
    let mut server = Server::start_all(vec![
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::Expect, // the resent PUBREL, and no PUBLISH
            Act::Expect, // the DISCONNECT
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");
    let packet_id = session.allocate(message("a", QoS::ExactlyOnce)).unwrap();
    assert!(session.pubrec_received(packet_id));
    events.next().await.expect("the close is reported");
    drop(client);
    server.finished().await;

    let (client, _events) = Client::connect_session(&context, &address, options(false), &session)
        .await
        .expect("resumes");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    assert_eq!(
        server.seen().await,
        [
            PacketType::Connect,
            PacketType::Connect,
            PacketType::Pubrel,
            PacketType::Disconnect
        ]
    );
    let recorded = server.seen_bytes().await;
    let Packet::Pubrel(pubrel) = decode(&recorded[2]) else {
        panic!("a pubrel")
    };
    assert_eq!(pubrel.packet_id, packet_id, "the original identifier");
}

/// [MQTT-4.6.0-1]: resends go out in the order the originals were sent, which
/// is not the order a map keyed by Packet Identifier would give.
#[tokio::test]
async fn resends_go_out_in_the_original_send_order() {
    let mut server = Server::start_all(vec![
        vec![
            Act::Send(connack_session(false, Properties::new())),
            Act::Close,
        ],
        vec![
            Act::Send(connack_session(true, Properties::new())),
            Act::Expect,
            Act::Expect,
            Act::Expect,
            Act::Expect, // the DISCONNECT
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");

    let session = Session::new("session-client", &ConnectOptions::default().limits);
    let (client, mut events) = Client::connect_session(&context, &address, options(true), &session)
        .await
        .expect("connects");

    // Allocate four, release the second, allocate a fourth: the identifiers
    // are then out of order relative to the send order.
    session
        .allocate(message("first", QoS::AtLeastOnce))
        .unwrap();
    let second = session
        .allocate(message("second", QoS::AtLeastOnce))
        .unwrap();
    session
        .allocate(message("third", QoS::AtLeastOnce))
        .unwrap();
    assert!(session.release(second));
    session
        .allocate(message("fourth", QoS::AtLeastOnce))
        .unwrap();

    events.next().await.expect("the close is reported");
    drop(client);
    server.finished().await;

    let (client, _events) = Client::connect_session(&context, &address, options(false), &session)
        .await
        .expect("resumes");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;

    let recorded = server.seen_bytes().await;
    // Skip the two CONNECTs.
    let topics: Vec<String> = recorded[2..5]
        .iter()
        .map(|bytes| match decode(bytes) {
            Packet::Publish(publish) => publish.topic.to_owned(),
            other => panic!("a publish, got {}", other.packet_type()),
        })
        .collect();
    assert_eq!(topics, ["first", "third", "fourth"]);
}

/// The session's ceiling is the server's `Receive Maximum` ([MQTT-4.9.0-1]),
/// replaced on every CONNACK — not the client's own configured value, which is
/// only a placeholder until the server has spoken.
#[tokio::test]
async fn the_servers_receive_maximum_becomes_the_sessions_ceiling() {
    let mut server = Server::start(vec![
        Act::Send(connack_session(
            false,
            Properties {
                receive_maximum: Some(2),
                ..Properties::new()
            },
        )),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");

    let mut options = options(true);
    options.limits.receive_maximum = 64;
    let session = Session::new("session-client", &options.limits);
    assert_eq!(session.send_quota(), 64, "the client's placeholder");

    let (client, _events) = Client::connect_session(&context, &server.address(), options, &session)
        .await
        .expect("connects");
    assert_eq!(session.send_quota(), 2, "the server's number replaced it");
    assert_eq!(
        session.receive_maximum(),
        64,
        "the client's own declared value is a different number and is not \
         overwritten by the server's (3.1.2.11.3 against 3.2.2.3.3)"
    );

    session.allocate(message("a", QoS::AtLeastOnce)).unwrap();
    session.allocate(message("b", QoS::AtLeastOnce)).unwrap();
    let (error, _) = session
        .allocate(message("c", QoS::AtLeastOnce))
        .expect_err("the quota is spent");
    assert!(
        matches!(error, Error::QuotaExhausted { quota: 2 }),
        "{error}"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}
