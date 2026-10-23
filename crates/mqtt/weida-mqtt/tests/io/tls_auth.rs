//! The tests of `tests/tls_auth.rs` that need the network, a process or real time.

use crate::harness;
use harness::{Act, Server, bytes, connack};
use std::sync::Arc;
use std::time::Duration;
use weida_mqtt::{Authenticator, Client, Context, DisconnectReasonCode, Error, Event, Result};
use weida_mqtt_codec::{Connack, ConnectReasonCode, Packet, Properties};

use crate::tls_auth::*;

/// User Name and Password reach the wire, **including 5.0's password with no
/// user name**, which 3.1.1 forbade (3.1.2.9) [mqtt5 §10].
#[tokio::test]
async fn the_connect_credentials_reach_the_wire_in_all_three_forms() {
    let server = Server::start_all(vec![
        vec![Act::Send(connack(Properties::new())), Act::Expect],
        vec![Act::Send(connack(Properties::new())), Act::Expect],
        vec![Act::Send(connack(Properties::new())), Act::Expect],
    ])
    .await;
    let address = server.address();
    let context = Context::new().expect("ambient");

    // Both.
    let mut both = options();
    both.user_name = Some("operator".into());
    both.password = Some(b"secret".to_vec());
    // A user name and no password.
    let mut name_only = options();
    name_only.user_name = Some("operator".into());
    // A password and no user name: legal in 5.0 and a Protocol Error in
    // 3.1.1, which is the difference this asserts.
    let mut password_only = options();
    password_only.password = Some(b"secret".to_vec());

    for options in [both, name_only, password_only] {
        let (client, _events) = Client::connect(&context, &address, options)
            .await
            .expect("connects");
        client
            .disconnect(DisconnectReasonCode::NormalDisconnection)
            .await
            .expect("disconnects");
        // A plain connection: the credentials just travelled in the clear,
        // and a client can ask.
        assert!(!client.is_encrypted());
    }

    let recorded = server.seen_bytes().await;
    let credentials: Vec<(Option<String>, Option<Vec<u8>>)> = recorded
        .iter()
        .filter_map(|raw| match Packet::decode(raw, u32::MAX) {
            Ok((Packet::Connect(connect), _)) => Some((
                connect.user_name.map(str::to_owned),
                connect.password.map(<[u8]>::to_vec),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        credentials,
        [
            (Some("operator".to_owned()), Some(b"secret".to_vec())),
            (Some("operator".to_owned()), None),
            (None, Some(b"secret".to_vec())),
        ]
    );
}
/// The AUTH exchange of 4.12 during the handshake: the server challenges, the
/// client answers, and every packet repeats the same method
/// ([MQTT-4.12.0-5]).
#[tokio::test]
async fn the_handshake_auth_exchange_runs_to_a_connack() {
    let mut server = Server::start(vec![
        // The **server** challenges first: the client's CONNECT is already
        // read by the time a script starts, and it is then waiting for either
        // an AUTH or the CONNACK ([MQTT-3.1.2-30]).
        Act::Send(auth(0x18, "SCRAM-SHA-1", b"challenge-1")),
        // Reads the client's answer and challenges again.
        Act::AnswerAuth(0x18),
        // Reads the second answer, then ends the exchange with the CONNACK.
        Act::Expect,
        Act::Send(connack(Properties::new())),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    options.authentication_data = Some(b"opening".to_vec());
    let authenticator = Counting::new();

    let (client, _events) = Client::connect_with(
        &context,
        &server.address(),
        options,
        authenticator.clone() as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects");
    assert_eq!(
        authenticator.calls(),
        2,
        "one call per challenge, and none for the CONNECT's opening data - \
         which the caller supplied directly"
    );

    let recorded = server.seen_bytes().await;
    // The CONNECT carried the method and the opening data.
    let (Packet::Connect(connect), _) = Packet::decode(&recorded[0], u32::MAX).expect("a connect")
    else {
        panic!("a connect")
    };
    assert_eq!(
        connect.properties.authentication_method,
        Some("SCRAM-SHA-1")
    );
    assert_eq!(
        connect.properties.authentication_data,
        Some(&b"opening"[..])
    );

    // Both answers repeated the method, which is what [MQTT-4.12.0-5]
    // requires of every AUTH in the exchange.
    let auths: Vec<(u8, Option<String>)> = recorded
        .iter()
        .filter_map(|raw| match Packet::decode(raw, u32::MAX) {
            Ok((Packet::Auth(auth), _)) => Some((
                auth.reason_code.as_byte(),
                auth.properties.authentication_method.map(str::to_owned),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        auths,
        [
            (0x18, Some("SCRAM-SHA-1".to_owned())),
            (0x18, Some("SCRAM-SHA-1".to_owned())),
        ]
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}
/// Re-authentication over the live connection: AUTH 0x19 from the client,
/// challenges, then AUTH 0x00 ([MQTT-4.12.1-1]) (4.12.1) [mqtt5 §10].
///
/// **Other traffic continues during the exchange**, which the specification is
/// explicit about, and the test proves it by publishing between the rounds and
/// asserting the publish reached the wire while the re-authentication was
/// still open.
#[tokio::test]
async fn re_authentication_runs_on_the_live_connection() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::AnswerAuth(0x18), // the client's 0x19, answered with a challenge
        Act::AnswerAuth(0x00), // the client's 0x18, answered with success
        Act::AckPublish,
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    let authenticator = Counting::new();
    let (client, _events) = Client::connect_with(
        &context,
        &server.address(),
        options,
        authenticator.clone() as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects");

    client.reauthenticate().await.expect("re-authenticates");
    assert_eq!(
        authenticator.calls(),
        2,
        "the opening move and one challenge answer"
    );

    // The connection is still usable, and its credentials are the new ones.
    client
        .publish(weida_mqtt::Message::new("a/b", "x").at(weida_mqtt::QoS::AtLeastOnce))
        .await
        .expect("publishes after re-authenticating");

    let recorded = server.seen_bytes().await;
    let kinds: Vec<weida_mqtt::PacketType> = recorded
        .iter()
        .map(|raw| {
            Packet::decode(raw, u32::MAX)
                .expect("a packet")
                .0
                .packet_type()
        })
        .collect();
    assert_eq!(
        kinds,
        [
            weida_mqtt::PacketType::Connect,
            weida_mqtt::PacketType::Auth,
            weida_mqtt::PacketType::Auth,
            weida_mqtt::PacketType::Publish,
        ]
    );
    // The client's opening move is 0x19 and its answer is 0x18: the two codes
    // are not interchangeable, and a client that sent 0x18 first would be
    // continuing an exchange that does not exist.
    let codes: Vec<u8> = recorded[1..3]
        .iter()
        .map(|raw| match Packet::decode(raw, u32::MAX) {
            Ok((Packet::Auth(auth), _)) => auth.reason_code.as_byte(),
            _ => panic!("an auth"),
        })
        .collect();
    assert_eq!(codes, [0x19, 0x18]);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}
/// A client that named no `Authentication Method` has nothing to
/// re-authenticate with, and the refusal is local: a server "MUST NOT send
/// AUTH" to such a client ([MQTT-4.12.0-6]), so there is nobody to ask.
#[tokio::test]
async fn re_authentication_without_a_method_is_refused_locally() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect(&context, &server.address(), options())
        .await
        .expect("connects");

    let error = client.reauthenticate().await.expect_err("refused");
    assert!(matches!(error, Error::Configuration(_)), "{error}");
    // Nothing reached the wire.
    assert_eq!(server.seen().await.len(), 1);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}
/// A server that switches method mid-exchange is not continuing this
/// authentication ([MQTT-4.12.0-5]), and the client says which fault it is
/// rather than reporting a generic protocol error.
#[tokio::test]
async fn a_method_switch_during_re_authentication_is_named() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::AnswerAuthAs(0x18, "SCRAM-SHA-256"),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    let (client, mut events) = Client::connect_with(
        &context,
        &server.address(),
        options,
        Counting::new() as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects");

    let error = client.reauthenticate().await.expect_err("refused");
    assert!(
        matches!(error, Error::AuthenticationMethodMismatch),
        "{error}"
    );
    // And the connection ends, because "both sides SHOULD send DISCONNECT and
    // MUST close" ([MQTT-4.12.1-2]).
    let Some(Event::Disconnected(reported)) = events.next().await else {
        panic!("the connection ends")
    };
    assert!(
        matches!(reported, Error::AuthenticationMethodMismatch),
        "{reported}"
    );
}
/// **The failure mode the sheet records for two large deployments**: a server
/// that does not implement AUTH.
///
/// It is reported by its reason code and not by a hang. Two shapes, and this
/// client answers both with a code: a CONNACK 0x8C (Bad authentication
/// method), and a server that simply never answers - for which the client's
/// own `connect_timeout` is the bound, because the specification gives none.
#[tokio::test]
async fn a_server_without_auth_is_a_code_and_not_a_hang() {
    // Shape 1: a refusal with the code that names the fault.
    let refusal = bytes(&Packet::Connack(Connack {
        session_present: false,
        reason_code: ConnectReasonCode::BadAuthenticationMethod,
        properties: Properties::new(),
    }));
    let mut server = Server::start(vec![Act::Send(refusal)]).await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    let error = Client::connect_with(
        &context,
        &server.address(),
        options.clone(),
        Counting::new() as Arc<dyn Authenticator>,
    )
    .await
    .expect_err("refused");
    assert_eq!(error.reason_code(), Some(0x8C));
    server.finished().await;

    // Shape 2: silence. The specification puts no deadline on the CONNACK, so
    // the bound is this client's and it is finite.
    let silent = Server::start(vec![Act::Idle(Duration::from_secs(5))]).await;
    options.connect_timeout = Duration::from_millis(300);
    let error = Client::connect_with(
        &context,
        &silent.address(),
        options,
        Counting::new() as Arc<dyn Authenticator>,
    )
    .await
    .expect_err("times out");
    assert!(matches!(error, Error::Timeout("CONNACK")), "{error}");
}
/// An authenticator that cannot answer ends the connection, which is the MUST
/// of [MQTT-4.12.1-2] - and the caller who asked for the re-authentication
/// learns why rather than only that it ended.
#[tokio::test]
async fn an_unanswerable_challenge_ends_the_connection_with_its_cause() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::AnswerAuth(0x18),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    // Answers the opening move, refuses the challenge.
    struct OpeningOnly;
    impl Authenticator for OpeningOnly {
        fn challenge(&self, data: Option<&[u8]>) -> Result<Vec<u8>> {
            match data {
                None => Ok(b"opening".to_vec()),
                Some(_) => Err(Error::Configuration("cannot answer that".into())),
            }
        }

        fn finish(&self, _data: Option<&[u8]>) -> Result<()> {
            Ok(())
        }
    }
    let (client, mut events) = Client::connect_with(
        &context,
        &server.address(),
        options,
        Arc::new(OpeningOnly) as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects");

    let error = client.reauthenticate().await.expect_err("refused");
    assert!(
        error.to_string().contains("cannot answer that"),
        "the caller learns the authenticator's own reason: {error}"
    );
    let Some(Event::Disconnected(reported)) = events.next().await else {
        panic!("the connection ends")
    };
    assert!(
        reported.to_string().contains("re-authentication failed"),
        "and the event stream learns why the connection ended: {reported}"
    );
}
/// An authenticator that refuses the **opening** move never reaches the wire.
#[tokio::test]
async fn an_authenticator_that_refuses_the_opening_move_sends_nothing() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    options.authentication_data = Some(b"opening".to_vec());
    let (client, _events) = Client::connect_with(
        &context,
        &server.address(),
        options,
        Arc::new(Refusing) as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects: the CONNECT's own data came from the options");

    assert!(client.reauthenticate().await.is_err());
    assert_eq!(
        server.seen().await.len(),
        1,
        "the CONNECT and nothing else: a re-authentication that cannot even \
         open does not send an AUTH"
    );

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
}
/// An AUTH arriving outside an exchange the client started is the server
/// breaking the protocol ([MQTT-4.12.1-1]), and it is reported as an
/// unexpected packet rather than answered.
#[tokio::test]
async fn an_unrequested_auth_after_the_connack_is_refused() {
    let unrequested = bytes(&Packet::Auth(weida_mqtt_codec::Auth {
        reason_code: weida_mqtt_codec::AuthReasonCode::ContinueAuthentication,
        properties: Properties {
            authentication_method: Some("SCRAM-SHA-1"),
            ..Properties::new()
        },
    }));
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Send(unrequested),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let mut options = options();
    options.authentication_method = Some("SCRAM-SHA-1".into());
    let (client, mut events) = Client::connect_with(
        &context,
        &server.address(),
        options,
        Counting::new() as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects");

    let Some(Event::Disconnected(error)) = events.next().await else {
        panic!("the connection ends")
    };
    assert!(
        matches!(
            error,
            Error::UnexpectedPacket {
                packet_type: weida_mqtt::PacketType::Auth
            }
        ),
        "{error}"
    );
    drop(client);
}
/// TLS on 8883, against a certificate this test generates: a **real**
/// handshake, and the CONNECT travels inside it.
///
/// The trust anchors are the caller's everywhere in this crate; here the test
/// is the caller, which is the only way to prove the contract.
#[cfg(feature = "tls")]
#[tokio::test]
async fn the_connect_travels_inside_a_real_tls_handshake() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

    let issued =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("a certificate");
    let cert = CertificateDer::from(issued.cert.der().to_vec());
    let key = PrivateKeyDer::try_from(issued.signing_key.serialize_der()).expect("a key");

    let server_config = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .expect("a server config"),
    );
    let mut roots = RootCertStore::empty();
    roots.add(cert).expect("a trust anchor");
    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binds");
    let port = listener.local_addr().expect("an address").port();
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let connack = connack(Properties::new());
    let served = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accepts");
        let mut tls = acceptor.accept(tcp).await.expect("the TLS handshake");
        // Read exactly the CONNECT, which arrives decrypted here and only
        // here: on the wire it was TLS application data.
        let mut header = [0u8; 2];
        tls.read_exact(&mut header).await.expect("a fixed header");
        let remaining = usize::from(header[1]);
        let mut body = vec![0u8; remaining];
        tls.read_exact(&mut body).await.expect("a body");
        tls.write_all(&connack).await.expect("writes the connack");
        tls.flush().await.expect("flushes");
        let mut packet = header.to_vec();
        packet.extend_from_slice(&body);
        // Hold the connection until the client goes away.
        let mut sink = [0u8; 64];
        while let Ok(read) = tls.read(&mut sink).await {
            if read == 0 {
                break;
            }
        }
        packet
    });

    let context = Context::new().expect("ambient");
    let mut options = options();
    options.user_name = Some("operator".into());
    options.password = Some(b"secret".to_vec());
    options.tls = Some(weida_mqtt::TlsOptions::new(client_config).with_server_name("localhost"));

    let (client, _events) = Client::connect(&context, &format!("127.0.0.1:{port}"), options)
        .await
        .expect("connects over TLS");
    assert!(
        client.is_encrypted(),
        "the credentials in that CONNECT were protected, and a caller can ask"
    );
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .ok();
    drop(client);

    let connect = served.await.expect("the server task");
    let (Packet::Connect(connect), _) = Packet::decode(&connect, u32::MAX).expect("a connect")
    else {
        panic!("a connect")
    };
    assert_eq!(connect.user_name, Some("operator"));
    assert_eq!(connect.password, Some(&b"secret"[..]));
}
/// The server name is what the certificate is validated against, and it has to
/// be settable because the connect address is often an IP literal that no
/// certificate names.
///
/// Validating `127.0.0.1` against a certificate for `localhost` fails, which
/// is the whole point of validation - and the failure is reported rather than
/// papered over.
#[cfg(feature = "tls")]
#[tokio::test]
async fn a_certificate_that_does_not_name_the_server_is_refused() {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

    let issued = rcgen::generate_simple_self_signed(vec!["some.other.host".to_owned()])
        .expect("a certificate");
    let cert = CertificateDer::from(issued.cert.der().to_vec());
    let key = PrivateKeyDer::try_from(issued.signing_key.serialize_der()).expect("a key");
    let server_config = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .expect("a server config"),
    );
    let mut roots = RootCertStore::empty();
    roots.add(cert).expect("a trust anchor");
    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binds");
    let port = listener.local_addr().expect("an address").port();
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    tokio::spawn(async move {
        if let Ok((tcp, _)) = listener.accept().await {
            let _ = acceptor.accept(tcp).await;
        }
    });

    let context = Context::new().expect("ambient");
    let mut options = options();
    // The certificate is trusted and names `some.other.host`, so validating
    // it against `localhost` must fail.
    options.tls = Some(weida_mqtt::TlsOptions::new(client_config).with_server_name("localhost"));
    let error = Client::connect(&context, &format!("127.0.0.1:{port}"), options)
        .await
        .expect_err("the certificate does not name localhost");
    assert!(
        matches!(error, Error::Io(_)),
        "a validation failure is a transport failure and is reported: {error}"
    );
}
/// The impostor's shortcut: a CONNACK 0x00 at once, with no exchange and so
/// no proof. The mechanism refuses it, the connect fails, and the client says
/// goodbye with a code a client may send.
#[tokio::test]
async fn a_connack_that_skips_the_exchange_is_refused() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Expect, // the DISCONNECT
    ])
    .await;
    let context = Context::new().expect("ambient");
    let error = Client::connect_with(
        &context,
        &server.address(),
        scram_options(),
        Arc::new(Mutual { proof: b"v=ok" }),
    )
    .await
    .expect_err("an impostor is not authenticated");
    assert!(matches!(error, Error::AuthenticationFailed(_)), "{error}");

    server.finished().await;
    let seen = server.seen_bytes().await;
    let (Packet::Disconnect(disconnect), _) = Packet::decode(&seen[1], u32::MAX).expect("a packet")
    else {
        panic!("the client sent DISCONNECT")
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::UnspecifiedError,
        "0x87 is the server's to send"
    );
}
/// A CONNACK that carries a proof, and the wrong one.
#[tokio::test]
async fn a_wrong_server_proof_in_the_connack_is_refused() {
    let server = Server::start(vec![Act::Send(connack(Properties {
        authentication_method: Some("SCRAM-SHA-1"),
        authentication_data: Some(b"v=forged"),
        ..Properties::new()
    }))])
    .await;
    let context = Context::new().expect("ambient");
    let error = Client::connect_with(
        &context,
        &server.address(),
        scram_options(),
        Arc::new(Mutual { proof: b"v=ok" }),
    )
    .await
    .expect_err("the proof is wrong");
    assert!(matches!(error, Error::AuthenticationFailed(_)), "{error}");
}
/// And the right one is what the exchange is for.
#[tokio::test]
async fn the_right_server_proof_is_accepted() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            authentication_method: Some("SCRAM-SHA-1"),
            authentication_data: Some(b"v=ok"),
            ..Properties::new()
        })),
        Act::Expect,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = Client::connect_with(
        &context,
        &server.address(),
        scram_options(),
        Arc::new(Mutual { proof: b"v=ok" }),
    )
    .await
    .expect("authenticates");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    server.finished().await;
}
/// A client that was given no authenticator cannot check a proof, so a
/// CONNACK that offers one is refused and not ignored.
#[tokio::test]
async fn a_proof_nobody_can_check_is_refused() {
    let server = Server::start(vec![Act::Send(connack(Properties {
        authentication_method: Some("SCRAM-SHA-1"),
        authentication_data: Some(b"v=whatever"),
        ..Properties::new()
    }))])
    .await;
    let context = Context::new().expect("ambient");
    let error = Client::connect(&context, &server.address(), scram_options())
        .await
        .expect_err("unverifiable");
    assert!(matches!(error, Error::AuthenticationFailed(_)), "{error}");
}
/// The same on the live connection: the AUTH 0x00 that ends a
/// re-authentication carries the proof, and a bad one ends the connection.
#[tokio::test]
async fn a_re_authentication_that_ends_without_the_proof_ends_the_connection() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            authentication_method: Some("SCRAM-SHA-1"),
            authentication_data: Some(b"v=ok"),
            ..Properties::new()
        })),
        // Answers the client's 0x19 with success, echoing the client's own
        // data back: not the proof.
        Act::AnswerAuth(0x00),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, mut events) = Client::connect_with(
        &context,
        &server.address(),
        scram_options(),
        Arc::new(Mutual { proof: b"v=ok" }),
    )
    .await
    .expect("connects");

    let error = client.reauthenticate().await.expect_err("no proof");
    assert!(matches!(error, Error::AuthenticationFailed(_)), "{error}");
    let Some(Event::Disconnected(reported)) = events.next().await else {
        panic!("the connection ends")
    };
    assert!(
        matches!(reported, Error::AuthenticationFailed(_)),
        "{reported}"
    );
}
/// A server that never ends the exchange makes the client do a bounded amount
/// of work: nine AUTH packets are answered and the tenth is where it stops.
#[tokio::test]
async fn an_endless_handshake_exchange_is_cut_off() {
    let flood: Vec<u8> = (0..20)
        .flat_map(|_| auth(0x18, "SCRAM-SHA-1", b"again"))
        .collect();
    let server = Server::start(vec![Act::Send(flood), Act::Idle(Duration::from_secs(2))]).await;
    let context = Context::new().expect("ambient");
    let authenticator = Counting::new();
    let error = Client::connect_with(
        &context,
        &server.address(),
        scram_options(),
        authenticator.clone() as Arc<dyn Authenticator>,
    )
    .await
    .expect_err("never finishes");
    assert!(matches!(error, Error::Timeout(_)), "{error}");
    assert!(
        authenticator.calls() <= 8,
        "{} challenges answered",
        authenticator.calls()
    );
}
/// And on the live connection, where nothing else bounded it.
#[tokio::test]
async fn an_endless_re_authentication_is_cut_off() {
    let mut script = vec![Act::Send(connack(Properties::new()))];
    script.extend((0..12).map(|_| Act::AnswerAuth(0x18)));
    script.push(Act::Idle(Duration::from_secs(2)));
    let server = Server::start(script).await;
    let context = Context::new().expect("ambient");
    let authenticator = Counting::new();
    let (client, _events) = Client::connect_with(
        &context,
        &server.address(),
        scram_options(),
        authenticator.clone() as Arc<dyn Authenticator>,
    )
    .await
    .expect("connects");

    let error = weida_testkit::within(
        "the re-authentication to end",
        Duration::from_secs(8),
        client.reauthenticate(),
    )
    .await
    .expect_err("never finishes");
    assert!(matches!(error, Error::Timeout(_)), "{error}");
    assert!(
        authenticator.calls() <= 9,
        "{} challenges answered",
        authenticator.calls()
    );
}
