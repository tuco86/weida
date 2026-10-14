//! SUBSCRIBE and UNSUBSCRIBE on the wire: their per-filter verdicts, the
//! Subscription Identifier a delivery reports back, and the identifier space
//! the three packet types share.
//!
//! The matcher itself is unit-tested in `filter.rs`, against the
//! specification's own worked examples. What needs a socket is everything the
//! *acknowledgements* decide: which code belongs to which filter, what a
//! partial failure leaves behind, and what a re-subscribe does to the client's
//! own view.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::{Act, Server, bytes, connack};
use weida_mqtt::{
    Client, ConnectOptions, Context, DisconnectReasonCode, Error, Event, Message, PacketType, QoS,
    RetainHandling, Session, SubackReasonCode, Subscription, UnsubackReasonCode, matches,
};
use weida_mqtt_codec::{Packet, Properties, Publish};

fn options() -> ConnectOptions {
    harness::options("sub-client")
}

/// One SUBSCRIBE, one SUBACK, and the granted codes come back in the order the
/// filters went out ([MQTT-3.9.3-1], [MQTT-3.9.3-2]).
///
/// The granted QoS is "the minimum of the QoS of the originally published
/// message and the Maximum QoS granted" ([MQTT-3.8.4-8]) [mqtt5 §6], so a
/// server may grant **less** than was asked for, and the client's own record
/// has to hold what was granted rather than what was requested.
#[tokio::test]
async fn a_suback_reports_one_code_per_filter_in_order() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        // Asked for 2, 2, 1; granted 2, 0, 1 - the middle one downgraded.
        Act::Suback(vec![0x02, 0x00, 0x01]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let granted = client
        .subscribe(vec![
            Subscription::new("a/+", QoS::ExactlyOnce),
            Subscription::new("b/#", QoS::ExactlyOnce),
            Subscription::new("c", QoS::AtLeastOnce),
        ])
        .await
        .expect("subacked");
    assert_eq!(
        granted,
        [
            SubackReasonCode::GrantedQos2,
            SubackReasonCode::GrantedQos0,
            SubackReasonCode::GrantedQos1,
        ]
    );

    // The mirror holds what was granted, keyed by filter.
    let subscriptions = client.subscriptions();
    assert_eq!(subscriptions.len(), 3);
    assert_eq!(
        subscriptions.get("b/#").expect("recorded").granted,
        SubackReasonCode::GrantedQos0,
        "the downgrade is what is recorded, not the request"
    );
    assert_eq!(
        subscriptions
            .get("b/#")
            .expect("recorded")
            .subscription
            .options
            .maximum_qos,
        QoS::ExactlyOnce,
        "and what was asked for is still there beside it"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The options are **per filter** and not per packet (3.8.3.1) [mqtt5 §4.4],
/// so one SUBSCRIBE can carry three filters with three different option
/// bytes - which is what the wire has to show, because a client that sent one
/// byte for all of them would still pass every SUBACK test.
///
/// Retain Handling 3 does not appear here because it cannot: it is not a
/// value [`RetainHandling`] has, so the only way to meet one is off the wire
/// and the codec refuses it there ("a Protocol Error to send", 3.8.3.1).
#[tokio::test]
async fn the_options_are_per_filter_on_the_wire() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x00, 0x01, 0x02]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe(vec![
            Subscription::new("plain", QoS::AtMostOnce),
            // The two options "primarily defined to allow for message bridge
            // applications" [mqtt5 §4.6].
            Subscription::new("bridge/+", QoS::AtLeastOnce)
                .no_local()
                .retain_as_published(),
            Subscription::new("changes/#", QoS::ExactlyOnce)
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
    let filters: Vec<_> = subscribe.filters.iter().collect();
    assert_eq!(filters.len(), 3);

    assert_eq!(filters[0].filter, "plain");
    assert_eq!(filters[0].options.maximum_qos, QoS::AtMostOnce);
    assert!(!filters[0].options.no_local);
    assert!(!filters[0].options.retain_as_published);
    assert_eq!(
        filters[0].options.retain_handling,
        RetainHandling::SendAtSubscribe,
        "the default is 0: send the retained messages at subscribe time"
    );

    assert_eq!(filters[1].filter, "bridge/+");
    assert_eq!(filters[1].options.maximum_qos, QoS::AtLeastOnce);
    assert!(filters[1].options.no_local, "[MQTT-3.8.3-3]");
    assert!(filters[1].options.retain_as_published, "[MQTT-3.3.1-13]");

    assert_eq!(filters[2].filter, "changes/#");
    assert_eq!(filters[2].options.maximum_qos, QoS::ExactlyOnce);
    assert_eq!(
        filters[2].options.retain_handling,
        RetainHandling::DoNotSend,
        "for a client that wants change notifications and not the initial state"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// A failure code for one filter "leaves the others unaffected": the SUBSCRIBE
/// as a whole does not fail, and 0x8F (Topic Filter invalid) or 0x87 (Not
/// authorized) reaches the caller as a code rather than as an error (3.9.3)
/// [mqtt5 §8].
#[tokio::test]
async fn one_refused_filter_leaves_the_others_granted() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x01, 0x87, 0x01]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let granted = client
        .subscribe(vec![
            Subscription::new("open/+", QoS::AtLeastOnce),
            Subscription::new("secret/+", QoS::AtLeastOnce),
            Subscription::new("other/+", QoS::AtLeastOnce),
        ])
        .await
        .expect("a refused filter is not a failed call");
    assert!(granted[1].is_error(), "0x87 is a failure for that filter");
    assert!(!granted[0].is_error());
    assert!(!granted[2].is_error());

    // A refused filter is not a subscription, so the mirror does not claim
    // one the server never made.
    let subscriptions = client.subscriptions();
    assert_eq!(subscriptions.len(), 2);
    assert!(subscriptions.get("secret/+").is_none());
    assert!(subscriptions.get("open/+").is_some());

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// **Position is the only binding between a SUBACK code and a filter.** A
/// count that disagrees with the filters sent leaves no way to say which code
/// is about which, so it is reported rather than guessed at.
#[tokio::test]
async fn a_suback_with_the_wrong_number_of_codes_is_reported() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x01]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let error = client
        .subscribe(vec![
            Subscription::new("a", QoS::AtLeastOnce),
            Subscription::new("b", QoS::AtLeastOnce),
        ])
        .await
        .expect_err("refused");
    assert!(
        matches!(
            error,
            Error::AcknowledgementLengthMismatch {
                packet_type: PacketType::Suback,
                sent: 2,
                received: 1,
            }
        ),
        "{error}"
    );
    // Nothing is recorded from an answer that cannot be read.
    assert!(client.subscriptions().is_empty());

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// UNSUBSCRIBE's codes are per filter too, and `NoSubscriptionExisted` (0x11)
/// is a **success**: the end state the client asked for holds either way, and
/// 3.1.1's UNSUBACK carried no status at all [mqtt5 §1.9].
#[tokio::test]
async fn no_subscription_existed_is_a_success_and_forgets_the_filter() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x01, 0x01]),
        Act::Unsuback(vec![0x00, 0x11]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe(vec![
            Subscription::new("a/+", QoS::AtLeastOnce),
            Subscription::new("b/+", QoS::AtLeastOnce),
        ])
        .await
        .expect("subacked");
    assert_eq!(client.subscriptions().len(), 2);

    let codes = client
        .unsubscribe(vec!["a/+".to_owned(), "b/+".to_owned()])
        .await
        .expect("unsubacked");
    assert_eq!(
        codes,
        [
            UnsubackReasonCode::Success,
            UnsubackReasonCode::NoSubscriptionExisted
        ]
    );
    assert!(!codes[1].is_error(), "0x11 is not a failure");
    assert!(
        client.subscriptions().is_empty(),
        "both are forgotten, because both reported the state the caller asked for"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// Re-subscribing the same filter "MUST replace the subscription and any
/// existing retained messages matching it are sent again"
/// ([MQTT-3.8.4-3], [MQTT-3.8.4-4]) (3.8.4) [mqtt5 §2].
///
/// **The observable is that it is one SUBSCRIBE and not an UNSUBSCRIBE plus a
/// SUBSCRIBE**, which is what "without losing messages" means: there is no
/// window in which the filter is not subscribed. The client's own mirror has
/// one entry for the filter either way, because "a session cannot hold two
/// subscriptions with the same filter".
#[tokio::test]
async fn re_subscribing_replaces_rather_than_adds() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x01]),
        Act::Suback(vec![0x02]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe(vec![Subscription::new("a/+", QoS::AtLeastOnce)])
        .await
        .expect("subacked");
    client
        .subscribe(vec![
            Subscription::new("a/+", QoS::ExactlyOnce).retain_handling(RetainHandling::DoNotSend),
        ])
        .await
        .expect("subacked again");

    let subscriptions = client.subscriptions();
    assert_eq!(subscriptions.len(), 1, "one filter, one subscription");
    let record = subscriptions.get("a/+").expect("recorded");
    assert_eq!(record.granted, SubackReasonCode::GrantedQos2);
    assert_eq!(
        record.subscription.options.retain_handling,
        RetainHandling::DoNotSend,
        "the replacement's options are what stands"
    );

    // No UNSUBSCRIBE was involved: two SUBSCRIBEs and nothing between them.
    let types = server.seen().await;
    assert_eq!(
        types,
        [
            PacketType::Connect,
            PacketType::Subscribe,
            PacketType::Subscribe
        ],
        "replacing a subscription is one packet, so no window exists in which \
         the filter is unsubscribed"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The Subscription Identifier reaches the wire on the SUBSCRIBE and comes
/// back "on every delivery it caused" ([MQTT-3.3.4-4]) [mqtt5 §4.1], which is
/// how a client tells which of its own filters matched.
#[tokio::test]
async fn a_subscription_identifier_is_sent_and_reported_on_the_delivery() {
    let delivery = bytes(&Packet::Publish(Publish {
        topic: "a/one",
        payload: b"hello",
        qos: QoS::AtMostOnce,
        properties: Properties::new().with_subscription_identifiers(&[7]),
        ..Publish::default()
    }));
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x00]),
        Act::Send(delivery),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, mut events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    client
        .subscribe_with(vec![Subscription::new("a/+", QoS::AtMostOnce)], Some(7))
        .await
        .expect("subacked");
    assert_eq!(
        client
            .subscriptions()
            .get("a/+")
            .expect("recorded")
            .identifier,
        Some(7)
    );

    // The identifier went out on the SUBSCRIBE.
    let recorded = server.seen_bytes().await;
    let (Packet::Subscribe(subscribe), _) =
        Packet::decode(&recorded[1], u32::MAX).expect("a subscribe")
    else {
        panic!("a subscribe")
    };
    assert_eq!(
        subscribe
            .properties
            .subscription_identifiers()
            .collect::<Vec<_>>(),
        [7],
        "one identifier per SUBSCRIBE packet, not per filter (3.8.2.1.2)"
    );

    let Some(Event::Delivered(message)) = events.next().await else {
        panic!("a delivery")
    };
    assert_eq!(message.properties.subscription_identifiers, [7]);
    assert_eq!(message.topic, "a/one");
    // And the identifier is what names the filter: the client looks its own
    // record up by it rather than re-matching the topic.
    let subscriptions = client.subscriptions();
    let cause = subscriptions
        .iter()
        .find(|record| record.identifier == Some(7))
        .expect("the filter that caused it");
    assert_eq!(cause.subscription.filter, "a/+");
    assert!(matches(&cause.subscription.filter, &message.topic));

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// A server that declares `Subscription Identifiers Available` 0 (3.2.2.3.12)
/// [mqtt5 §11] refuses the whole feature, and the refusal happens **before the
/// packet reaches the wire** with the 0xA1 the server would have sent.
///
/// Against such a server the only way to tell which filter caused a delivery
/// is to match the topic, which is what `Subscriptions::matching` exists for.
#[tokio::test]
async fn an_identifier_against_a_server_that_declined_them_is_refused() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            subscription_identifier_available: Some(false),
            ..Properties::new()
        })),
        Act::Suback(vec![0x00]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let error = client
        .subscribe_with(vec![Subscription::new("a/+", QoS::AtMostOnce)], Some(1))
        .await
        .expect_err("refused");
    assert_eq!(error.reason_code(), Some(0xA1));

    // The same subscription without an identifier is fine, and matching is
    // what replaces the identifier.
    client
        .subscribe(vec![Subscription::new("a/+", QoS::AtMostOnce)])
        .await
        .expect("subacked");
    let subscriptions = client.subscriptions();
    let matched: Vec<&str> = subscriptions
        .matching("a/one")
        .map(|record| record.subscription.filter.as_str())
        .collect();
    assert_eq!(matched, ["a/+"]);
    assert_eq!(subscriptions.matching("b/one").count(), 0);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// The Packet Identifier space is **one space per session, shared across
/// PUBLISH, SUBSCRIBE and UNSUBSCRIBE** ([MQTT-2.2.1-3]) [mqtt5 §2].
///
/// So a SUBSCRIBE in flight cannot be handed the identifier an unacknowledged
/// QoS 1 PUBLISH is holding, and it is not counted against the send quota
/// either, whose unit is "one QoS 1 or QoS 2 PUBLISH packet - not bytes, not
/// QoS 0, not any other packet type" (4.9) [mqtt5 §5].
#[tokio::test]
async fn subscribe_shares_the_publish_identifier_space_but_not_the_quota() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            receive_maximum: Some(1),
            ..Properties::new()
        })),
        // The PUBLISH is accepted and not answered, so it keeps its
        // identifier and spends the whole quota.
        Act::AcceptPublish,
        Act::Suback(vec![0x01]),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("sub-client", &options().limits);
    let (client, _events) =
        Client::connect_session(&context, &server.address(), options(), &session)
            .await
            .expect("connects");

    let client = Arc::new(client);
    let publishing = tokio::spawn({
        let client = Arc::clone(&client);
        async move {
            client
                .publish(Message::new("t", "x").at(QoS::AtLeastOnce))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(session.in_flight(), 1, "the publish holds an identifier");
    assert_eq!(session.send_quota(), 1, "and the quota is spent");

    let recorded = server.seen_bytes().await;
    let (Packet::Publish(sent), _) = Packet::decode(&recorded[1], u32::MAX).expect("a publish")
    else {
        panic!("a publish")
    };
    let publish_id = sent.packet_id.expect("QoS 1 carries one");

    // The quota is spent, so a second QoS 1 publish would stall here. The
    // SUBSCRIBE does not, because the quota does not count it - and it gets
    // an identifier the publish is not holding, because the space is shared.
    let granted = client
        .subscribe(vec![Subscription::new("a/+", QoS::AtLeastOnce)])
        .await;
    assert!(
        granted.is_ok(),
        "a spent send quota does not stall a SUBSCRIBE: {granted:?}"
    );

    let recorded = server.seen_bytes().await;
    let (Packet::Subscribe(subscribe), _) =
        Packet::decode(&recorded[2], u32::MAX).expect("a subscribe")
    else {
        panic!("a subscribe")
    };
    assert_ne!(
        subscribe.packet_id, publish_id,
        "one space: the SUBSCRIBE cannot reuse an identifier a PUBLISH is holding"
    );
    assert_ne!(
        subscribe.packet_id, 0,
        "and it is non-zero ([MQTT-2.2.1-3])"
    );

    // The publish is never answered, by construction: abandoning it is the
    // end of the test, not a leak.
    publishing.abort();
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// Everything refused at configuration time, each with the code the server
/// would have sent or the rule it broke - the point being that none of these
/// reaches the wire, so none of them costs a round trip.
#[tokio::test]
async fn the_configuration_refusals_never_reach_the_wire() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            wildcard_subscription_available: Some(false),
            shared_subscription_available: Some(false),
            ..Properties::new()
        })),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    // An empty list: "a SUBSCRIBE with no payload entry is a Protocol Error"
    // ([MQTT-3.8.3-2]).
    assert!(client.subscribe(vec![]).await.is_err());
    assert!(client.unsubscribe(vec![]).await.is_err());

    // The grammar of 4.7.1.
    for filter in ["sport/tennis#", "sport/#/ranking", "", "sp+rt"] {
        assert!(
            client
                .subscribe(vec![Subscription::new(filter, QoS::AtMostOnce)])
                .await
                .is_err(),
            "{filter:?}"
        );
        assert!(
            client.unsubscribe(vec![filter.to_owned()]).await.is_err(),
            "{filter:?}"
        );
    }

    // The two availability flags this server cleared.
    assert_eq!(
        client
            .subscribe(vec![Subscription::new("a/+", QoS::AtMostOnce)])
            .await
            .expect_err("refused")
            .reason_code(),
        Some(0xA2)
    );
    assert_eq!(
        client
            .subscribe(vec![Subscription::new("$share/g/a", QoS::AtMostOnce)])
            .await
            .expect_err("refused")
            .reason_code(),
        Some(0x9E)
    );

    // A Subscription Identifier of 0 "is a Protocol Error" ([MQTT-3.8.3-4]),
    // and so is one above the Variable Byte Integer's ceiling.
    assert!(
        client
            .subscribe_with(vec![Subscription::new("a", QoS::AtMostOnce)], Some(0))
            .await
            .is_err()
    );
    assert!(
        client
            .subscribe_with(
                vec![Subscription::new("a", QoS::AtMostOnce)],
                Some(268_435_456)
            )
            .await
            .is_err()
    );

    // Not one of them reached the wire: the CONNECT is all the server saw.
    assert_eq!(server.seen().await, [PacketType::Connect]);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}

/// Subscriptions "do not survive Clean Start 1" [mqtt5 §1]: the server
/// discards its half, so the client's mirror must not outlive it and claim
/// subscriptions that no longer exist.
#[tokio::test]
async fn clean_start_discards_the_subscription_mirror() {
    let server = Server::start_all(vec![
        vec![
            Act::Send(connack(Properties::new())),
            Act::Suback(vec![0x01]),
            Act::Close,
        ],
        vec![
            Act::Send(connack(Properties::new())),
            Act::Idle(Duration::from_secs(2)),
        ],
    ])
    .await;
    let context = Context::new().expect("ambient");
    let session = Session::new("sub-client", &options().limits);

    let (client, mut events) =
        Client::connect_session(&context, &server.address(), options(), &session)
            .await
            .expect("connects");
    client
        .subscribe(vec![Subscription::new("a/+", QoS::AtLeastOnce)])
        .await
        .expect("subacked");
    assert_eq!(session.subscriptions().len(), 1);
    let _ = events.next().await;

    // Clean Start 1 on the reconnect, which is what discards the session.
    let mut fresh = options();
    fresh.clean_start = true;
    let (client, _events) = Client::connect_session(&context, &server.address(), fresh, &session)
        .await
        .expect("reconnects");
    assert!(
        session.subscriptions().is_empty(),
        "the server discarded its half, so the mirror cannot claim one"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}
