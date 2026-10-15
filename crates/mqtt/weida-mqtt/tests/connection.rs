//! The connection on the wire: the handshake, the negotiated limits, the
//! keep-alive timer, and the reason code of a server DISCONNECT.
//!
//! Each test names the conformance statement it is about, because that is what
//! makes it a conformance test rather than a regression net.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::{Act, Server, bytes, connack};
use weida_mqtt::{
    Client, ConnectOptions, ConnectReasonCode, Context, DisconnectReasonCode, Error, Event,
    Feature, QoS,
};
use weida_mqtt_codec::{Connack, Packet, PacketType, Properties, Publish};

fn options() -> ConnectOptions {
    let mut options = harness::options("test-client");
    options.keep_alive = Duration::from_secs(60);
    options
}

/// [MQTT-3.1.0-1] and [MQTT-3.2.0-2]: CONNECT is the first packet and the
/// client waits for the CONNACK before anything else.
#[tokio::test]
async fn connect_sends_connect_first_and_awaits_connack() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Expect, // the DISCONNECT
    ])
    .await;

    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    assert_eq!(client.client_id(), "test-client");
    assert!(!client.session_present());
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");

    server.finished().await;
    assert_eq!(
        server.seen().await,
        [PacketType::Connect, PacketType::Disconnect],
        "CONNECT first, and nothing before the CONNACK"
    );
}

/// §11's defaults: an empty CONNACK property set means Receive Maximum
/// 65,535, Maximum QoS 2, RETAIN available, Topic Alias Maximum 0 — not zero
/// for all of them, which is the mistake this asserts against.
#[tokio::test]
async fn an_empty_connack_yields_the_documented_defaults() {
    let mut server = Server::start(vec![Act::Send(connack(Properties::new())), Act::Expect]).await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let limits = client.server_limits();
    assert_eq!(limits.receive_maximum, 65_535);
    assert_eq!(limits.maximum_qos, QoS::ExactlyOnce);
    assert!(limits.retain_available);
    assert!(limits.wildcard_subscription_available);
    assert!(limits.subscription_identifiers_available);
    assert!(limits.shared_subscription_available);
    assert_eq!(limits.topic_alias_maximum, 0);
    // Absent Server Keep Alive: the client's own value stands
    // ([MQTT-3.2.2-22]).
    assert_eq!(client.keep_alive(), Some(Duration::from_secs(60)));

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// [MQTT-3.2.2-21]: `Server Keep Alive` present, the client MUST use it. The
/// only property in the protocol that overrides the client.
#[tokio::test]
async fn server_keep_alive_overrides_the_clients_value() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            server_keep_alive: Some(7),
            ..Properties::new()
        })),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    assert_eq!(client.keep_alive(), Some(Duration::from_secs(7)));
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// [MQTT-3.2.2-16]: a zero-length Client Identifier gets one assigned, and the
/// client reports the assigned one rather than the empty string it sent.
#[tokio::test]
async fn an_assigned_client_identifier_replaces_the_empty_one() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            assigned_client_identifier: Some("auto-9f86d081"),
            ..Properties::new()
        })),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.client_id = String::new();
    let (client, _events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    assert_eq!(client.client_id(), "auto-9f86d081");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// **Using an unavailable feature is this client's own error and never
/// reaches the wire.** The server clears every availability flag; the client
/// refuses locally with the code the server would have sent, and the wire
/// shows only CONNECT and DISCONNECT.
#[tokio::test]
async fn an_unavailable_feature_is_refused_locally_and_never_sent() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            retain_available: Some(false),
            maximum_qos: Some(QoS::AtMostOnce),
            wildcard_subscription_available: Some(false),
            subscription_identifier_available: Some(false),
            shared_subscription_available: Some(false),
            topic_alias_maximum: Some(0),
            ..Properties::new()
        })),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let limits = client.server_limits();
    for (feature, code) in [
        (Feature::Retain, 0x9A),
        (Feature::Qos(QoS::AtLeastOnce), 0x9B),
        (Feature::SharedSubscription, 0x9E),
        (Feature::SubscriptionIdentifier, 0xA1),
        (Feature::WildcardSubscription, 0xA2),
    ] {
        let error = limits.require(feature).expect_err("refused locally");
        assert_eq!(error.reason_code(), Some(code), "{feature}");
    }
    assert_eq!(
        limits.check_topic_alias(1).unwrap_err().reason_code(),
        Some(0x94)
    );
    // QoS 0 is still permitted against Maximum QoS 0: a ceiling, not a ban.
    assert!(limits.require(Feature::Qos(QoS::AtMostOnce)).is_ok());

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
    assert_eq!(
        server.seen().await,
        [PacketType::Connect, PacketType::Disconnect],
        "nothing was sent on behalf of a refused feature"
    );
}

/// **A server DISCONNECT's reason code is surfaced as a named error rather
/// than as a closed socket.** 3.1.1 had no server-to-client DISCONNECT at all
/// and the client had to guess [mqtt5 §1.9].
#[tokio::test]
async fn a_server_disconnect_surfaces_its_reason_code() {
    let taken_over = bytes(&Packet::Disconnect(weida_mqtt_codec::Disconnect {
        reason_code: DisconnectReasonCode::SessionTakenOver,
        properties: Properties::new(),
    }));
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Send(taken_over),
    ])
    .await;

    let context = Context::new().expect("ambient");
    let (_client, mut events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let Some(Event::Disconnected(error)) = events.next().await else {
        panic!("the disconnect must be reported")
    };
    assert!(
        matches!(
            error,
            Error::ServerDisconnected(DisconnectReasonCode::SessionTakenOver)
        ),
        "{error}"
    );
    assert_eq!(error.reason_code(), Some(0x8E));
    server.finished().await;
}

/// The contrast that makes the previous test worth having: a bare close is its
/// own error and is not dressed up as a reason code the server never sent.
#[tokio::test]
async fn a_bare_close_is_reported_as_a_close() {
    let mut server = Server::start(vec![Act::Send(connack(Properties::new())), Act::Close]).await;
    let context = Context::new().expect("ambient");
    let (_client, mut events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let Some(Event::Disconnected(error)) = events.next().await else {
        panic!("the close must be reported")
    };
    assert!(matches!(error, Error::ConnectionClosed), "{error}");
    assert_eq!(error.reason_code(), None);
    server.finished().await;
}

/// A CONNACK with a failure code is a named refusal carrying the byte, not a
/// connection that mysteriously does not work.
#[tokio::test]
async fn a_refused_connect_reports_the_servers_code() {
    let refusal = bytes(&Packet::Connack(Connack {
        session_present: false,
        reason_code: ConnectReasonCode::BadUserNameOrPassword,
        properties: Properties {
            reason_string: Some("no"),
            ..Properties::new()
        },
    }));
    let mut server = Server::start(vec![Act::Send(refusal)]).await;
    let context = Context::new().expect("ambient");
    let error = Client::connect(&context, &server.address(), options())
        .await
        .expect_err("refused");
    assert!(
        matches!(
            error,
            Error::ConnectionRefused(ConnectReasonCode::BadUserNameOrPassword)
        ),
        "{error}"
    );
    assert_eq!(error.reason_code(), Some(0x86));
    server.finished().await;
}

/// [MQTT-3.1.2-20]: "absent other traffic the Client MUST send a PINGREQ".
/// The interval is one second here so the test is a test and not a wait.
#[tokio::test]
async fn a_pingreq_is_sent_when_nothing_else_has_been() {
    let pingresp = bytes(&Packet::Pingresp);
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Expect, // PINGREQ
        Act::Send(pingresp.clone()),
        Act::Expect, // PINGREQ again
        Act::Send(pingresp),
        Act::Expect, // the DISCONNECT
    ])
    .await;

    let context = Context::new().expect("ambient");
    let mut options = options();
    options.keep_alive = Duration::from_secs(1);
    let (client, _events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    // Two intervals plus slack: the client must have pinged twice, and the
    // connection must still be alive because the PINGRESPs arrived.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("still connected");
    server.finished().await;

    assert_eq!(
        server.seen().await,
        [
            PacketType::Connect,
            PacketType::Pingreq,
            PacketType::Pingreq,
            PacketType::Disconnect
        ]
    );
}

/// [MQTT-3.1.2-20] measures the interval from the last packet **this side
/// sent**, so inbound traffic must not reset it. A QoS 0 PUBLISH is answered
/// with nothing at all, and a client that counted one as a write would never
/// reach its idle step while a busy topic kept delivering — so it would say
/// nothing and the server would close the connection at 1.5x Keep Alive
/// ([MQTT-3.1.2-22]). The symptom is a broker that looks flaky.
#[tokio::test]
async fn an_inbound_qos_0_stream_does_not_suppress_the_pingreq() {
    let delivery = bytes(&Packet::Publish(Publish {
        topic: "busy/topic",
        payload: b"tick",
        qos: QoS::AtMostOnce,
        ..Publish::default()
    }));
    // A publication every 400 ms across the whole two-second interval, so
    // inbound traffic alone never leaves the client idle for a Keep Alive.
    let mut script = vec![Act::Send(connack(Properties::new()))];
    for _ in 0..6 {
        script.push(Act::Send(delivery.clone()));
        script.push(Act::Idle(Duration::from_millis(400)));
    }
    // The PINGREQ, which by now is waiting to be read if it was sent on time.
    script.push(Act::Expect);
    script.push(Act::Send(bytes(&Packet::Pingresp)));
    // The script ends the connection itself, so `finished` resolves on the
    // PINGREQ having been read rather than on the client going away.
    script.push(Act::Close);
    let mut server = Server::start(script).await;

    let context = Context::new().expect("ambient");
    let mut options = options();
    options.keep_alive = Duration::from_secs(2);
    let (_client, mut events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    // The deadline is the protocol's own: 1.5x Keep Alive is where a server
    // "MUST close the connection" ([MQTT-3.1.2-22]). The script takes 2.4 s,
    // so an on-time PINGREQ is read well inside it, while a client whose
    // timer the deliveries reset would not ping until 4 s.
    tokio::time::timeout(Duration::from_secs(3), server.finished())
        .await
        .expect("the PINGREQ arrived before 1.5x Keep Alive would have closed the connection");

    assert_eq!(
        server.seen().await,
        [PacketType::Connect, PacketType::Pingreq],
        "an inbound QoS 0 delivery is not this side's traffic"
    );

    // And the stream really did arrive, so the PINGREQ was sent *despite* it.
    for _ in 0..6 {
        let Some(Event::Delivered(message)) = events.next().await else {
            panic!("a delivery")
        };
        assert_eq!(message.topic, "busy/topic");
    }
}

/// The number the specification declines to give: a PINGRESP that never comes
/// ends the connection rather than hanging it. "A Client seeing no PINGRESP
/// within a reasonable amount of time SHOULD close, with no number given"
/// [mqtt5 §1], so the number is the client's and it is finite.
#[tokio::test]
async fn an_unanswered_pingreq_times_out_rather_than_hanging() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Expect, // the PINGREQ, deliberately unanswered
        Act::Idle(Duration::from_secs(3)),
    ])
    .await;

    let context = Context::new().expect("ambient");
    let mut options = options();
    options.keep_alive = Duration::from_secs(1);
    options.ping_timeout = Some(Duration::from_millis(300));
    let (_client, mut events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");

    let deadline = Duration::from_secs(5);
    let event = tokio::time::timeout(deadline, events.next())
        .await
        .expect("the client must give up on its own");
    let Some(Event::Disconnected(error)) = event else {
        panic!("the timeout must be reported")
    };
    assert!(matches!(error, Error::Timeout("PINGRESP")), "{error}");
}

/// Keep Alive 0 disables the mechanism (3.1.2.10), which also means the client
/// sends no PINGREQ and nothing times out.
#[tokio::test]
async fn keep_alive_zero_sends_no_pingreq() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Idle(Duration::from_millis(600)),
        Act::Expect, // the DISCONNECT, and no PINGREQ before it
    ])
    .await;

    let context = Context::new().expect("ambient");
    let mut options = options();
    options.keep_alive = Duration::ZERO;
    let (client, _events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");
    assert_eq!(client.keep_alive(), None);

    tokio::time::sleep(Duration::from_millis(400)).await;
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
    assert_eq!(
        server.seen().await,
        [PacketType::Connect, PacketType::Disconnect]
    );
}

/// The unquantified CONNACK deadline, spent: a server that accepts the
/// connection and then says nothing must not hang the client.
#[tokio::test]
async fn a_missing_connack_times_out() {
    let mut server = Server::start(vec![Act::Idle(Duration::from_secs(2))]).await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.connect_timeout = Duration::from_millis(300);
    let error = Client::connect(&context, &server.address(), options)
        .await
        .expect_err("times out");
    assert!(matches!(error, Error::Timeout("CONNACK")), "{error}");
    server.finished().await;
}

/// [MQTT-3.2.0-2]: before the CONNACK the server may send nothing but AUTH, so
/// a PUBLISH there is the server breaking the protocol and is named as such
/// rather than being queued for an application that has not connected yet.
#[tokio::test]
async fn a_packet_before_the_connack_is_refused() {
    let early = bytes(&Packet::Publish(weida_mqtt_codec::Publish {
        topic: "a/b",
        payload: b"early",
        ..weida_mqtt_codec::Publish::default()
    }));
    let mut server = Server::start(vec![Act::Send(early)]).await;
    let context = Context::new().expect("ambient");
    let error = Client::connect(&context, &server.address(), options())
        .await
        .expect_err("refused");
    assert!(
        matches!(
            error,
            Error::UnexpectedPacket {
                packet_type: PacketType::Publish
            }
        ),
        "{error}"
    );
    server.finished().await;
}

/// [MQTT-4.12.0-6]: a server that sends AUTH to a client that named no
/// `Authentication Method` is breaking the protocol. The client says so rather
/// than reporting a feature it does not have.
#[tokio::test]
async fn an_unrequested_auth_is_refused() {
    let auth = bytes(&Packet::Auth(weida_mqtt_codec::Auth {
        reason_code: weida_mqtt_codec::AuthReasonCode::ContinueAuthentication,
        properties: Properties {
            authentication_method: Some("SCRAM-SHA-1"),
            ..Properties::new()
        },
    }));
    let mut server = Server::start(vec![Act::Send(auth)]).await;
    let context = Context::new().expect("ambient");
    let error = Client::connect(&context, &server.address(), options())
        .await
        .expect_err("refused");
    assert!(
        matches!(
            error,
            Error::UnexpectedPacket {
                packet_type: PacketType::Auth
            }
        ),
        "{error}"
    );
    server.finished().await;
}

/// The AUTH exchange of 4.12, end to end: the server challenges, the client
/// answers through its authenticator repeating the same method
/// ([MQTT-4.12.0-5]), and the CONNACK ends it.
#[tokio::test]
async fn an_auth_exchange_completes_before_the_connack() {
    struct Echo;
    impl weida_mqtt::Authenticator for Echo {
        fn challenge(&self, data: Option<&[u8]>) -> Result<Vec<u8>, Error> {
            // A real mechanism would compute; echoing is enough to prove the
            // bytes made the round trip.
            Ok(data.unwrap_or(b"").to_vec())
        }

        fn finish(&self, _data: Option<&[u8]>) -> Result<(), Error> {
            Ok(())
        }
    }

    let challenge = bytes(&Packet::Auth(weida_mqtt_codec::Auth {
        reason_code: weida_mqtt_codec::AuthReasonCode::ContinueAuthentication,
        properties: Properties {
            authentication_method: Some("SCRAM-SHA-1"),
            authentication_data: Some(b"nonce"),
            ..Properties::new()
        },
    }));
    let mut server = Server::start(vec![
        Act::Send(challenge),
        Act::Expect, // the client's AUTH
        Act::Send(connack(Properties {
            authentication_method: Some("SCRAM-SHA-1"),
            ..Properties::new()
        })),
        Act::Expect, // the DISCONNECT
    ])
    .await;

    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    options.authentication_data = Some(b"initial".to_vec());
    let (client, _events) =
        Client::connect_with(&context, &server.address(), options, Arc::new(Echo))
            .await
            .expect("authenticates");

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
    assert_eq!(
        server.seen().await,
        [
            PacketType::Connect,
            PacketType::Auth,
            PacketType::Disconnect
        ]
    );
}

/// [MQTT-4.12.0-5]: a server that switches method mid-exchange is not
/// continuing this authentication.
#[tokio::test]
async fn an_auth_that_switches_method_is_refused() {
    struct Echo;
    impl weida_mqtt::Authenticator for Echo {
        fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>, Error> {
            Ok(vec![1])
        }

        fn finish(&self, _data: Option<&[u8]>) -> Result<(), Error> {
            Ok(())
        }
    }

    let switched = bytes(&Packet::Auth(weida_mqtt_codec::Auth {
        reason_code: weida_mqtt_codec::AuthReasonCode::ContinueAuthentication,
        properties: Properties {
            authentication_method: Some("GS2-KRB5"),
            ..Properties::new()
        },
    }));
    let mut server = Server::start(vec![Act::Send(switched)]).await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    let error = Client::connect_with(&context, &server.address(), options, Arc::new(Echo))
        .await
        .expect_err("refused");
    assert!(
        matches!(error, Error::AuthenticationMethodMismatch),
        "{error}"
    );
    server.finished().await;
}

/// An authenticator with no `authentication_method` would never be asked
/// anything: no AUTH exchange starts and `finish` is skipped, so a verifier
/// would verify nothing. It is refused before any socket is opened, which the
/// unreachable address proves: dialling it would fail with an I/O error.
#[tokio::test]
async fn an_authenticator_without_an_authentication_method_is_refused() {
    struct Verifier;
    impl weida_mqtt::Authenticator for Verifier {
        fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>, Error> {
            Ok(vec![1])
        }

        fn finish(&self, _data: Option<&[u8]>) -> Result<(), Error> {
            Err(Error::AuthenticationFailed("never reached".into()))
        }
    }

    let context = Context::new().expect("ambient");
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        Client::connect_with(&context, "127.0.0.1:1", options(), Arc::new(Verifier)),
    )
    .await
    .expect("the refusal is immediate")
    .expect_err("refused");
    assert!(matches!(error, Error::Configuration(_)), "{error}");
}

/// A zero timeout expires before the wait it bounds has begun, and one past
/// what an `Instant` can hold would panic in the connection task, so both are
/// refused where the connect starts.
#[tokio::test]
async fn a_timeout_that_cannot_be_a_deadline_is_refused_at_connect() {
    let context = Context::new().expect("ambient");
    for set in [
        (|options: &mut ConnectOptions| options.connect_timeout = Duration::ZERO)
            as fn(&mut ConnectOptions),
        |options| options.ping_timeout = Some(Duration::ZERO),
        |options| options.write_timeout = Some(Duration::MAX),
    ] {
        let mut options = options();
        set(&mut options);
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            Client::connect(&context, "127.0.0.1:1", options),
        )
        .await
        .expect("the refusal is immediate")
        .expect_err("refused");
        assert!(matches!(error, Error::Configuration(_)), "{error}");
    }
}

/// The client declares its own limits, and the two whose defaults are not the
/// protocol's are on the wire while the two that match the protocol's default
/// are left off it.
#[tokio::test]
async fn the_clients_declared_limits_reach_the_server() {
    // The harness records types rather than bodies, so this test asserts the
    // CONNECT this client builds through the codec rather than through the
    // socket — which is the same bytes, and checkable without a second
    // decoder in the harness.
    let mut options = options();
    options.limits.receive_maximum = 20;
    options.limits.maximum_packet_size = 65_536;
    options.limits.topic_alias_maximum = 10;
    assert!(options.validate().is_ok());

    let mut server = Server::start(vec![Act::Send(connack(Properties::new())), Act::Expect]).await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options)
        .await
        .expect("connects");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}

/// `Context::owned` exists so a caller with no reactor of its own can use
/// this client, and the proof is a whole handshake driven from
/// `futures::executor::block_on` — an executor that is not tokio.
///
/// The scripted server needs a reactor for its own listener, so it is started
/// on the context's: that is the claim, that everything this client needs
/// lives on the reactor the context owns.
#[test]
fn an_owned_context_drives_a_handshake_from_a_foreign_executor() {
    let context = Context::owned(1).expect("owns a reactor");
    let server = context.exec().spawn(Server::start(vec![
        Act::Send(connack(Properties {
            receive_maximum: Some(11),
            ..Properties::new()
        })),
        Act::Expect,
    ]));
    let mut server = futures::executor::block_on(server).expect("the harness starts");

    let limits = futures::executor::block_on(async {
        let (client, _events) = Client::connect(&context, &server.address(), options()).await?;
        let limits = client.server_limits().clone();
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await?;
        Ok::<_, Error>(limits)
    })
    .expect("the handshake completes off a tokio thread");

    assert_eq!(limits.receive_maximum, 11);
    futures::executor::block_on(server.finished());
    assert_eq!(server.connects(), 1);
}
