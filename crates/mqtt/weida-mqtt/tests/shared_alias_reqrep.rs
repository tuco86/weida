//! Shared subscriptions as a client, Topic Aliases in both directions, and
//! request/response, on the wire.
//!
//! The three have one thing in common that decides how they are tested: each
//! is a mechanism whose *effect* lives on the server, and each gives the
//! client exactly one thing it can observe. Shared subscriptions: what the
//! SUBSCRIBE carried, and which Subscription Identifier a delivery reports.
//! Topic Aliases: the bytes on the wire and the topic the application is
//! handed. Request/response: the two properties on the PUBLISH and the
//! namespace CONNACK offered. Anything past that belongs to the interop tests,
//! against a broker that actually implements it.

use crate::harness;

use std::time::Duration;

use harness::{Act, Server, bytes, connack};
use weida_mqtt::{
    Client, ConnectOptions, Context, DisconnectReasonCode, Event, Message, QoS, Session,
    Subscription, split_shared,
};
use weida_mqtt_codec::{Connack, ConnectReasonCode, Packet, Properties, Publish};

fn options() -> ConnectOptions {
    harness::options("shared-alias-client")
}

/// A PUBLISH with an explicit alias and topic, which is how a server
/// establishes and then uses one.
fn aliased(topic: &str, payload: &[u8], alias: Option<u16>) -> Vec<u8> {
    bytes(&Packet::Publish(Publish {
        topic,
        payload,
        qos: QoS::AtMostOnce,
        properties: Properties {
            topic_alias: alias,
            ..Properties::new()
        },
        ..Publish::default()
    }))
}

/// A `$share/{ShareName}/{filter}` subscription reaches the wire whole, and
/// the ShareName's three rules are enforced before it does ([MQTT-4.8.2-2]).
///
/// **What a client cannot observe:** which of its subscriptions a delivery
/// came through, unless it asked for Subscription Identifiers. A shared
/// delivery and a non-shared one are the same PUBLISH on the wire - there is
/// no shared-delivery flag - so a client subscribed to both `state/+` and
/// `$share/g/state/+` has exactly one way to tell them apart, which is why
/// this test uses it.
#[tokio::test]
async fn a_shared_subscription_is_told_apart_only_by_its_identifier() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x01]),
        Act::Suback(vec![0x01]),
        // Two deliveries on the same topic, one per subscription. Identical
        // but for the identifier, which is the whole point.
        Act::Send(bytes(&Packet::Publish(Publish {
            topic: "state/a",
            payload: b"x",
            qos: QoS::AtMostOnce,
            properties: Properties::new().with_subscription_identifiers(&[1]),
            ..Publish::default()
        }))),
        Act::Send(bytes(&Packet::Publish(Publish {
            topic: "state/a",
            payload: b"x",
            qos: QoS::AtMostOnce,
            properties: Properties::new().with_subscription_identifiers(&[2]),
            ..Publish::default()
        }))),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, mut events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe_with(
            vec![Subscription::new("state/+", QoS::AtLeastOnce)],
            Some(1),
        )
        .await
        .expect("subacked");
    client
        .subscribe_with(
            vec![Subscription::new("$share/g/state/+", QoS::AtLeastOnce)],
            Some(2),
        )
        .await
        .expect("subacked");

    let recorded = server.seen_bytes().await;
    let (Packet::Subscribe(shared), _) =
        Packet::decode(&recorded[2], u32::MAX).expect("a subscribe")
    else {
        panic!("a subscribe")
    };
    assert_eq!(
        shared.filters.iter().next().expect("one").filter,
        "$share/g/state/+",
        "the $share prefix and the ShareName are on the wire, because the \
         server is what routes on them"
    );

    // The two deliveries are indistinguishable but for the identifier.
    let Some(Event::Delivered(first)) = events.next().await else {
        panic!("a delivery")
    };
    let Some(Event::Delivered(second)) = events.next().await else {
        panic!("a delivery")
    };
    assert_eq!(first.topic, second.topic);
    assert_eq!(first.payload, second.payload);
    assert_eq!(first.retain, second.retain);
    assert_eq!(first.qos, second.qos);
    assert_eq!(first.properties.subscription_identifiers, [1]);
    assert_eq!(second.properties.subscription_identifiers, [2]);

    // And matching cannot separate them, because both filters match: the
    // `$share/` prefix is not considered when matching [mqtt5 §4.2].
    let subscriptions = client.subscriptions();
    assert_eq!(
        subscriptions.matching("state/a").count(),
        2,
        "both filters select the topic; only the identifier says which \
         delivery came through which"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The ShareName's rules, all refused before the wire: at least one character
/// and containing none of `/`, `+`, `#` ([MQTT-4.8.2-2]), and No Local set on
/// a Shared Subscription "is a Protocol Error" ([MQTT-3.8.3-4]).
#[tokio::test]
async fn the_share_name_rules_are_refused_at_configuration_time() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    for filter in [
        "$share/",          // no ShareName and no filter
        "$share/g",         // a ShareName and no filter
        "$share//state",    // an empty ShareName
        "$share/a+b/state", // `+` in the ShareName
        "$share/a#b/state", // `#` in the ShareName
        "$share/g/",        // an empty filter
    ] {
        assert!(
            client
                .subscribe(vec![Subscription::new(filter, QoS::AtLeastOnce)])
                .await
                .is_err(),
            "{filter:?} is not a shared subscription"
        );
    }

    // `/` cannot appear in a ShareName, which is what makes the split
    // unambiguous: the first `/` after `$share/` ends the name, so
    // `$share/a/b/state` is the group `a` on the filter `b/state` and is
    // perfectly legal. Asserted through the splitter rather than through
    // `subscribe`, because a legal filter would reach the wire and wait for a
    // SUBACK this script deliberately does not send.
    let shared = split_shared("$share/a/b/state")
        .expect("legal")
        .expect("shared");
    assert_eq!(shared.name, "a");
    assert_eq!(shared.filter, "b/state");

    // No Local on a shared subscription.
    assert!(
        client
            .subscribe(vec![
                Subscription::new("$share/g/state/+", QoS::AtLeastOnce).no_local()
            ])
            .await
            .is_err()
    );

    // Nothing reached the wire but the CONNECT.
    assert_eq!(server.seen().await.len(), 1);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// The outbound half: an alias is **established** by a PUBLISH carrying the
/// full Topic Name and the alias, then **used** by one carrying a zero-length
/// Topic Name and the alias (3.3.2.3.4, 3.3.4) [mqtt5 §3].
///
/// This is the only place a zero-length Topic Name is legal, and the saving is
/// the point: the second publish to a long topic is two bytes instead of the
/// topic.
#[tokio::test]
async fn an_outbound_alias_is_established_once_and_then_used() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            topic_alias_maximum: Some(2),
            ..Properties::new()
        })),
        Act::AckPublish, // establishes alias 1
        Act::AckPublish, // uses alias 1
        Act::AckPublish, // establishes alias 2
        Act::AckPublish, // the table is full: full topic, no alias
        Act::AckPublish, // uses alias 1 again
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");
    assert_eq!(client.server_limits().topic_alias_maximum, 2);

    for topic in [
        "sensors/floor3/room12/temperature",
        "sensors/floor3/room12/temperature",
        "sensors/floor3/room12/humidity",
        "sensors/floor3/room12/pressure",
        "sensors/floor3/room12/temperature",
    ] {
        client
            .publish(Message::new(topic, "x").at(QoS::AtLeastOnce))
            .await
            .expect("publishes");
    }

    let recorded = server.seen_bytes().await;
    let sent: Vec<(String, Option<u16>)> = recorded[1..]
        .iter()
        .map(|raw| {
            let (Packet::Publish(publish), _) = Packet::decode(raw, u32::MAX).expect("a publish")
            else {
                panic!("a publish")
            };
            (publish.topic.to_owned(), publish.properties.topic_alias)
        })
        .collect();
    assert_eq!(
        sent,
        [
            ("sensors/floor3/room12/temperature".to_owned(), Some(1)),
            (String::new(), Some(1)),
            ("sensors/floor3/room12/humidity".to_owned(), Some(2)),
            // The table is full at the server's declared 2, so this one goes
            // out whole and unaliased rather than stealing an alias that is
            // still in use.
            ("sensors/floor3/room12/pressure".to_owned(), None),
            (String::new(), Some(1)),
        ]
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// A server that declares no `Topic Alias Maximum` - the protocol's own
/// default of 0 ([MQTT-3.2.2-18]) - accepts no alias, so nothing is ever
/// aliased and every Topic Name goes out whole.
#[tokio::test]
async fn a_server_that_declares_no_aliases_gets_none() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::AckPublish,
        Act::AckPublish,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");
    assert_eq!(client.server_limits().topic_alias_maximum, 0);

    for _ in 0..2 {
        client
            .publish(Message::new("a/long/topic/name", "x").at(QoS::AtLeastOnce))
            .await
            .expect("publishes");
    }

    let recorded = server.seen_bytes().await;
    for raw in &recorded[1..] {
        let (Packet::Publish(publish), _) = Packet::decode(raw, u32::MAX).expect("a publish")
        else {
            panic!("a publish")
        };
        assert_eq!(publish.topic, "a/long/topic/name");
        assert_eq!(publish.properties.topic_alias, None);
    }

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The inbound half: the application is handed the Topic Name and never an
/// alias, whichever form the server used.
#[tokio::test]
async fn an_inbound_alias_is_resolved_before_the_application_sees_it() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x00]),
        // Established.
        Act::Send(aliased("deep/nested/topic", b"one", Some(3))),
        // Used.
        Act::Send(aliased("", b"two", Some(3))),
        // Overwritten: "a sender can modify the mapping by sending another
        // PUBLISH with the same alias and a different Topic Name".
        Act::Send(aliased("other/topic", b"three", Some(3))),
        Act::Send(aliased("", b"four", Some(3))),
        // And an ordinary unaliased delivery beside them.
        Act::Send(aliased("plain/topic", b"five", None)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.limits.topic_alias_maximum = 4;
    let (client, mut events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");
    client
        .subscribe(vec![Subscription::new("#", QoS::AtMostOnce)])
        .await
        .expect("subacked");

    let mut seen = Vec::new();
    for _ in 0..5 {
        let Some(Event::Delivered(delivery)) = events.next().await else {
            panic!("a delivery")
        };
        seen.push((
            delivery.topic,
            String::from_utf8(delivery.payload.to_vec()).unwrap(),
        ));
    }
    assert_eq!(
        seen,
        [
            ("deep/nested/topic".to_owned(), "one".to_owned()),
            ("deep/nested/topic".to_owned(), "two".to_owned()),
            ("other/topic".to_owned(), "three".to_owned()),
            ("other/topic".to_owned(), "four".to_owned()),
            ("plain/topic".to_owned(), "five".to_owned()),
        ],
        "no application ever sees a zero-length Topic Name or an alias number"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// An alias above what **this client** declared is a Protocol Error earning
/// 0x94, and the map is not grown by it - which is the whole reason the bound
/// is the receiver's number and not the sender's.
#[tokio::test]
async fn an_inbound_alias_above_our_declared_maximum_is_refused() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Send(aliased("a/b", b"x", Some(40_000))),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.limits.topic_alias_maximum = 4;
    let (client, mut events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    let Some(Event::Disconnected(error)) = events.next().await else {
        panic!("the connection ends")
    };
    assert_eq!(
        error.reason_code(),
        Some(0x94),
        "Topic Alias invalid, which is what a client answers rather than \
         allocating a 40,000-entry table: {error}"
    );
    drop(client);
}

/// A zero-length Topic Name with an alias that was never established has no
/// answer, so it is a Protocol Error rather than a delivery on an unknown
/// topic.
#[tokio::test]
async fn an_unestablished_alias_is_not_a_delivery() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Send(aliased("", b"x", Some(2))),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.limits.topic_alias_maximum = 4;
    let (client, mut events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    let Some(Event::Disconnected(error)) = events.next().await else {
        panic!("the connection ends")
    };
    assert_eq!(error.reason_code(), Some(0x94), "{error}");
    drop(client);
}

/// **Aliases do not survive a reconnect** ([MQTT-3.3.2-7]), and the case that
/// proves it is a retransmission: a QoS 2 exchange interrupted after its
/// PUBLISH was aliased is resent on the next connection with its **full**
/// Topic Name, because the stored message never held an alias.
#[tokio::test]
async fn an_alias_does_not_survive_the_connection_that_established_it() {
    let server = Server::start_all(vec![
        vec![
            Act::Send(connack(Properties {
                topic_alias_maximum: Some(4),
                ..Properties::new()
            })),
            Act::AckPublish, // establishes alias 1 and answers PUBREC
            Act::CompletePubrel,
            Act::Expect, // the second publish, using alias 1
            Act::Close,  // with the exchange in flight
        ],
        vec![
            Act::Send(bytes(&Packet::Connack(Connack {
                session_present: true,
                reason_code: ConnectReasonCode::Success,
                properties: Properties {
                    topic_alias_maximum: Some(4),
                    ..Properties::new()
                },
            }))),
            Act::AckPublish,     // the retransmitted PUBLISH, answered PUBREC
            Act::CompletePubrel, // and PUBCOMP
        ],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");
    let mut resumable = options();
    resumable.clean_start = false;
    resumable.session_expiry = Some(Duration::from_secs(300));
    let session = Session::new("shared-alias-client", &resumable.limits);

    let (client, mut events) =
        Client::connect_session(&context, &address, resumable.clone(), &session)
            .await
            .expect("connects");
    client
        .publish(Message::new("long/topic/name", "one").at(QoS::ExactlyOnce))
        .await
        .expect("completes");
    // The second one is aliased, and the connection dies with it in flight.
    let publishing = tokio::spawn({
        let message = Message::new("long/topic/name", "two").at(QoS::ExactlyOnce);
        async move { client.publish(message).await }
    });
    events.next().await.expect("the close is reported");
    assert!(publishing.await.expect("the task finishes").is_err());
    assert_eq!(session.in_flight(), 1, "the exchange is session state");

    let (client, mut events) = Client::connect_session(&context, &address, resumable, &session)
        .await
        .expect("resumes");
    // No handle is left for the resumed exchange, so its completion arrives
    // as an event - and waiting for it is what guarantees the server has read
    // the retransmission the assertions below inspect.
    let Some(Event::Completed { .. }) = events.next().await else {
        panic!("the resumed exchange completes")
    };

    let recorded = server.seen_bytes().await;
    let publishes: Vec<(String, Option<u16>, bool)> = recorded
        .iter()
        .filter_map(|raw| match Packet::decode(raw, u32::MAX) {
            Ok((Packet::Publish(publish), _)) => Some((
                publish.topic.to_owned(),
                publish.properties.topic_alias,
                publish.dup,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        publishes,
        [
            ("long/topic/name".to_owned(), Some(1), false),
            // Aliased on the first connection.
            (String::new(), Some(1), false),
            // Resent on the second with the full Topic Name and no alias:
            // the mapping did not cross the connection, and a zero-length
            // Topic Name with a stale alias would have been a Protocol Error
            // at the server.
            ("long/topic/name".to_owned(), None, true),
        ]
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// Request/response: `Response Topic` and `Correlation Data` on the request,
/// copied back by the responder (4.10) [mqtt5 §4.3].
///
/// The correlation is the **client's**: "the Server does not use it" and does
/// not even have to forward the two properties' meaning - it forwards them
/// unaltered like any other property. So the responder copying Correlation
/// Data into its reply is the whole mechanism, and there is no server-side
/// table.
#[tokio::test]
async fn a_request_carries_its_response_topic_and_correlation_data() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            response_information: Some("clients/shared-alias-client"),
            ..Properties::new()
        })),
        Act::Suback(vec![0x00]),
        Act::AckPublish, // the request
        // The responder's reply, carrying the correlation data back.
        Act::Send(bytes(&Packet::Publish(Publish {
            topic: "clients/shared-alias-client/reply",
            payload: b"pong",
            qos: QoS::AtMostOnce,
            properties: Properties {
                correlation_data: Some(b"request-7"),
                ..Properties::new()
            },
            ..Publish::default()
        }))),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.request_response_information = true;
    let (client, mut events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    // The namespace the server granted, and a topic built under it.
    assert_eq!(
        client.response_information(),
        Some("clients/shared-alias-client"),
        "the server answered Request Response Information with a namespace"
    );
    let reply_to = client
        .response_topic("reply")
        .expect("a Topic Name")
        .expect("the server offered a namespace");
    assert_eq!(reply_to, "clients/shared-alias-client/reply");

    client
        .subscribe(vec![Subscription::new(&reply_to, QoS::AtMostOnce)])
        .await
        .expect("subacked");

    let mut request = Message::new("service/ping", "ping").at(QoS::AtLeastOnce);
    request.response_topic = Some(reply_to.clone());
    request.correlation_data = Some(b"request-7".to_vec());
    client.publish(request).await.expect("publishes");

    let recorded = server.seen_bytes().await;
    let (Packet::Publish(sent), _) = Packet::decode(&recorded[2], u32::MAX).expect("a publish")
    else {
        panic!("a publish")
    };
    assert_eq!(sent.properties.response_topic, Some(reply_to.as_str()));
    assert_eq!(sent.properties.correlation_data, Some(&b"request-7"[..]));

    let Some(Event::Delivered(reply)) = events.next().await else {
        panic!("a delivery")
    };
    assert_eq!(reply.payload, &b"pong"[..]);
    assert_eq!(
        reply.properties.correlation_data.as_deref(),
        Some(&b"request-7"[..]),
        "the responder copied it back, which is the only thing that pairs a \
         reply with its request"
    );
    // And a reply is an ordinary message: it carries no Response Topic of its
    // own unless the responder wants one.
    assert_eq!(reply.properties.response_topic, None);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// A server that offers no `Response Information` - which it MAY do even when
/// asked ([MQTT-3.1.2-28]) - leaves the client with no namespace, and this
/// client says so rather than inventing one.
#[tokio::test]
async fn a_server_that_offers_no_response_information_says_so() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::AckPublish,
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.request_response_information = true;
    let (client, _events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    // Asked for, and not given.
    let recorded = server.seen_bytes().await;
    let (Packet::Connect(connect), _) = Packet::decode(&recorded[0], u32::MAX).expect("a connect")
    else {
        panic!("a connect")
    };
    assert_eq!(
        connect.properties.request_response_information,
        Some(true),
        "the client did ask"
    );
    assert_eq!(client.response_information(), None);
    assert_eq!(
        client.response_topic("reply").expect("no topic to check"),
        None,
        "there is no honest fallback: a namespace this client made up is one \
         the server's authorization rules never granted"
    );

    // A caller that knows its reply topic out of band sets it directly, which
    // is the documented answer.
    let mut request = Message::new("service/ping", "ping").at(QoS::AtLeastOnce);
    request.response_topic = Some("agreed/out/of/band".to_owned());
    client.publish(request).await.expect("publishes");
    let recorded = server.seen_bytes().await;
    let (Packet::Publish(sent), _) = Packet::decode(&recorded[1], u32::MAX).expect("a publish")
    else {
        panic!("a publish")
    };
    assert_eq!(sent.properties.response_topic, Some("agreed/out/of/band"));

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// A Response Topic is a Topic Name, so a suffix carrying a wildcard is
/// refused where it is built rather than where it is published.
#[tokio::test]
async fn a_response_topic_is_checked_where_it_is_built() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            response_information: Some("clients/x/"),
            ..Properties::new()
        })),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    // The trailing `/` of the namespace is not doubled.
    assert_eq!(
        client.response_topic("reply").unwrap(),
        Some("clients/x/reply".to_owned())
    );
    // An empty suffix is the namespace itself.
    assert_eq!(
        client.response_topic("").unwrap(),
        Some("clients/x/".to_owned())
    );
    // And a wildcard cannot become a Response Topic.
    for suffix in ["+", "a/#", "re+ply"] {
        assert!(client.response_topic(suffix).is_err(), "{suffix:?}");
    }

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}
