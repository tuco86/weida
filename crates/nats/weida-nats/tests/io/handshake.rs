//! The tests of `tests/handshake.rs` that need the network, a process or real time.

use crate::support;
use std::time::Duration;
use support::{SILENCE, Server, options, within};
use weida_nats::options::{Credentials, NonceSignature, Signer};
use weida_nats::{Connection, Error, State};
use weida_runtime::Exec;

use crate::handshake::*;

/// `INFO` arrives first, unprompted, and only then does the client speak.
/// This is the assertion that separates NATS from every client-first
/// protocol, and it is load-bearing: `max_payload`, `tls_required`,
/// `auth_required`, `headers` and the `nonce` all come out of this one
/// operation, so a client that wrote first would be guessing at all five.
#[tokio::test]
async fn the_server_sends_info_before_the_client_says_anything() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server.handshake_full().await;
        // The selected capabilities, and no credential field at all.
        assert!(connect.contains("\"protocol\":1"), "{connect}");
        assert!(connect.contains("\"headers\":true"), "{connect}");
        assert!(connect.contains("\"no_responders\":true"), "{connect}");
        assert!(connect.contains("\"lang\":\"rust\""), "{connect}");
        assert!(connect.contains("\"verbose\":false"), "{connect}");
        assert!(connect.contains("\"tls_required\":false"), "{connect}");
        assert!(!connect.contains("auth_token"), "{connect}");
        assert!(!connect.contains("\"user\""), "{connect}");
        assert!(!connect.contains("\"sig\""), "{connect}");
        server
    });

    let nats = within(
        "the handshake finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("the connection opened");

    assert_eq!(nats.state(), State::Connected);
    assert_eq!(nats.info().server_id.as_deref(), Some("S1"));
    assert_eq!(nats.max_payload(), 1_048_576);
    assert!(nats.headers_supported());

    let mut server = within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
    // There is no CLOSE verb: ending the transport is the close.
    server.expect_end_of_stream().await;
}
/// A server that says nothing at all holds the client for its own deadline
/// and no longer: the protocol gives none for the opening `INFO`.
#[tokio::test]
async fn a_server_that_never_sends_info_hits_our_own_deadline() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    // The server holds the connection open and silent until the test is
    // done with it, so the only way out is the client's own deadline.
    let server = tokio::spawn(async move {
        let server = Server::accept(&listener).await;
        std::future::pending::<()>().await;
        drop(server);
    });

    let mut options = options();
    options.handshake_timeout = Duration::from_millis(150);
    let error = within(
        "the handshake finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect_err("no INFO ever arrived");
    assert!(
        matches!(error, Error::HandshakeTimeout { step } if step.contains("INFO")),
        "{error}"
    );
    server.abort();
}
/// The first operation is `INFO` and nothing else.
#[tokio::test]
async fn a_first_operation_that_is_not_info_is_a_protocol_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server.write(b"PING\r\n").await;
        tokio::time::sleep(SILENCE).await;
    });

    let error = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect_err("the first operation must be INFO");
    assert!(
        matches!(&error, Error::Protocol(why) if why.contains("INFO") && why.contains("PING")),
        "{error}"
    );
    within("a step of the test", server).await.unwrap();
}
/// Each credential form travels alone and in its own fields.
#[tokio::test]
async fn a_token_connect_carries_the_token_and_nothing_else() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server
            .handshake("{\"server_id\":\"S1\",\"auth_required\":true}")
            .await;
        assert!(connect.contains("\"auth_token\":\"t0ken\""), "{connect}");
        assert!(!connect.contains("\"user\""), "{connect}");
        assert!(!connect.contains("\"jwt\""), "{connect}");
        assert!(!connect.contains("\"nkey\""), "{connect}");
    });

    let mut options = options();
    options.credentials = Credentials::Token("t0ken".into());
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// The user/password form, with the two fields the reference names.
#[tokio::test]
async fn a_user_password_connect_carries_both_fields() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server
            .handshake("{\"server_id\":\"S1\",\"auth_required\":true}")
            .await;
        assert!(connect.contains("\"user\":\"alice\""), "{connect}");
        assert!(connect.contains("\"pass\":\"s3cret\""), "{connect}");
        assert!(!connect.contains("auth_token"), "{connect}");
    });

    let mut options = options();
    options.credentials = Credentials::UserPassword {
        user: "alice".into(),
        password: "s3cret".into(),
    };
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// The JWT form: the JWT in `jwt`, the caller's signature in `sig`, and no
/// `nkey` — the JWT already carries the public key the server verifies
/// against.
#[tokio::test]
async fn a_jwt_connect_signs_the_nonce_without_sending_an_nkey() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server
            .handshake("{\"server_id\":\"S1\",\"auth_required\":true,\"nonce\":\"nonceZ\"}")
            .await;
        assert!(connect.contains("\"jwt\":\"ey.hdr.sig\""), "{connect}");
        assert!(connect.contains("\"sig\":\"signed:nonceZ\""), "{connect}");
        assert!(!connect.contains("\"nkey\""), "{connect}");
    });

    let mut options = options();
    options.credentials = Credentials::Jwt {
        jwt: "ey.hdr.sig".into(),
        signer: Some(Signer::new(|nonce| {
            Ok(NonceSignature {
                signature: format!("signed:{}", String::from_utf8_lossy(nonce)),
                // Offered and deliberately ignored on the JWT path.
                public_key: Some("UIGNORED".into()),
            })
        })),
    };
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// A server that demands authentication and a client with nothing to offer:
/// refused locally, before a `CONNECT` the server would answer with
/// `-ERR 'Authorization Violation'` and a close.
#[tokio::test]
async fn auth_required_with_no_credentials_is_refused_before_the_connect() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"auth_required\":true}")
            .await;
        // No CONNECT: the client knows it has nothing to say, so it ends the
        // transport instead. `expect_end_of_stream` fails with the octets if
        // anything was written first.
        server.expect_end_of_stream().await;
    });

    let error = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect_err("no credentials");
    assert!(matches!(error, Error::AuthenticationRequired), "{error}");
    within("a step of the test", server).await.unwrap();
}
/// The NKey path: the nonce reaches the caller's signer **unchanged**, and
/// what the signer returned lands in `CONNECT.sig` and `CONNECT.nkey`.
///
/// The signing itself is the application's — this crate has no signing
/// dependency and will not grow one — so what is asserted here is the whole
/// of this client's part: the octets in, the two fields out.
#[tokio::test]
async fn the_nonce_reaches_the_signer_unchanged_and_the_signature_lands_in_sig() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;
    let nonce = "8vGgtz8Rq9kQh2Nn";

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        let connect = server
            .handshake(&format!(
                "{{\"server_id\":\"S1\",\"auth_required\":true,\"nonce\":\"{nonce}\"}}"
            ))
            .await;
        assert!(
            connect.contains("\"sig\":\"signed:8vGgtz8Rq9kQh2Nn\""),
            "{connect}"
        );
        assert!(connect.contains("\"nkey\":\"UTESTPUBKEY\""), "{connect}");
    });

    // A channel rather than a lock: the signer is a synchronous closure and
    // all the test wants out of it is the octets it was handed.
    let (recorder, mut seen) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let mut options = options();
    options.credentials = Credentials::Nkey(Signer::new(move |nonce| {
        let _ = recorder.send(nonce.to_vec());
        Ok(NonceSignature {
            signature: format!("signed:{}", String::from_utf8_lossy(nonce)),
            public_key: Some("UTESTPUBKEY".into()),
        })
    }));

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");

    assert_eq!(
        seen.try_recv().expect("the signer ran"),
        nonce.as_bytes(),
        "the signer sees the nonce exactly as INFO carried it"
    );
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// An NKey configuration against a server that sent no nonce: there is
/// nothing to sign, and signing nothing would be signing nothing.
#[tokio::test]
async fn nkey_credentials_without_a_nonce_fail_rather_than_sign_nothing() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"auth_required\":true}")
            .await;
        // The signer was never called, so no CONNECT exists to send: the
        // client ends the transport.
        server.expect_end_of_stream().await;
    });

    let mut options = options();
    options.credentials = Credentials::Nkey(Signer::new(|_| {
        panic!("the signer must not be called when there is no nonce")
    }));
    let error = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect_err("no nonce");
    assert!(matches!(error, Error::NonceMissing), "{error}");
    within("a step of the test", server).await.unwrap();
}
/// `-ERR` during the handshake is the server refusing the `CONNECT`, and it
/// is reported in the server's own words.
#[tokio::test]
async fn a_refused_connect_is_reported_with_the_servers_own_reason() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"auth_required\":true}")
            .await;
        assert!(server.read_op().await.starts_with("CONNECT"));
        assert_eq!(server.read_op().await, "PING");
        // What a server actually answers a bad credential with, followed by a
        // close.
        server.write(b"-ERR 'Authorization Violation'\r\n").await;
    });

    let mut options = options();
    options.credentials = Credentials::UserPassword {
        user: "u".into(),
        password: "wrong".into(),
    };
    let error = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect_err("refused");
    assert!(
        matches!(&error, Error::Server(reason) if reason == "Authorization Violation"),
        "{error}"
    );
    within("a step of the test", server).await.unwrap();
}
/// A server `PING` is answered with `PONG`, in both directions from the first
/// octet.
#[tokio::test]
async fn a_server_ping_is_answered_with_a_pong() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake("{\"server_id\":\"S1\"}").await;
        server.write(b"PING\r\n").await;
        assert_eq!(
            server.read_op().await,
            "PONG",
            "the server's PING is answered, or the server disconnects us as stale"
        );
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// The unanswered pings are **bounded**. A server that answers the
/// handshake's `PING` and then goes silent gets exactly `max_pings_out`
/// further pings, and then the connection is reported failed rather than
/// pinged forever.
#[tokio::test]
async fn unanswered_pings_are_bounded_and_the_connection_is_reported_failed() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake("{\"server_id\":\"S1\"}").await;
        // Two keep-alive pings, and no answer to either.
        assert_eq!(server.read_op().await, "PING");
        assert_eq!(server.read_op().await, "PING");
        // A third would mean the bound was not a bound. The client ends the
        // transport instead, which this read observes as end-of-stream.
        server.expect_end_of_stream().await;
    });

    let mut options = options();
    options.ping_interval = Duration::from_millis(60);
    options.max_pings_out = 2;
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");

    let state = within("the connection failed within the deadline", nats.closed()).await;
    match state {
        State::Failed(why) => {
            assert!(why.contains("stale"), "{why}");
            assert!(why.contains('2'), "{why}");
        }
        other => panic!("expected a failed connection, got {other:?}"),
    }
    within("a step of the test", server).await.unwrap();
}
/// An asynchronous `INFO`, after the handshake: `connect_urls` retained under
/// their bound, and `ldm: true` surfaced as the drain notice it is.
#[tokio::test]
async fn a_later_info_carries_connect_urls_and_the_lame_duck_notice() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake("{\"server_id\":\"S1\",\"proto\":1}").await;
        // "When a NATS server cluster expands, an INFO message is sent to the
        // client with an updated connect_urls list", and lame duck mode marks
        // later INFO with ldm.
        server
            .send_info(
                "{\"server_id\":\"S1\",\"connect_urls\":\
                  [\"10.0.0.1:4222\",\"10.0.0.2:4222\",\"10.0.0.3:4222\"],\"ldm\":true}",
            )
            .await;
        server
    });

    let mut options = options();
    // The bound is ours, so a cluster larger than it is truncated rather than
    // refused.
    options.max_connect_urls = 2;
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("opened");

    assert!(nats.info().connect_urls.is_empty(), "none yet");
    assert!(!nats.is_lame_duck());

    let mut server = within("a step of the test", server).await.unwrap();

    assert!(
        within("the notice arrived", nats.lame_duck_notice()).await,
        "ldm: true is the server saying it will drain its clients"
    );
    let info = nats.info();
    assert_eq!(
        info.connect_urls,
        vec!["10.0.0.1:4222".to_owned(), "10.0.0.2:4222".to_owned()],
        "retained under the bound, which is ours because the list grows with the cluster"
    );
    assert!(info.lame_duck);

    within("a step of the test", nats.close()).await.unwrap();
    server.expect_end_of_stream().await;
}
/// `max_payload` is enforced **locally**: an oversized publish fails before
/// the wire, and the point of the test is that nothing was written.
#[tokio::test]
async fn an_oversized_publish_fails_locally_and_nothing_reaches_the_wire() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let (refused, mut wait) = tokio::sync::mpsc::channel::<()>(1);
    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server
            .handshake("{\"server_id\":\"S1\",\"max_payload\":64}")
            .await;
        // Nothing at all after the handshake: the refusal was local.
        server.expect_silence().await;
        refused.send(()).await.unwrap();
        // And the connection is still usable, which is the other half of
        // "before the wire" — a publish that had reached the server would
        // have earned a close.
        assert_eq!(server.read_op().await, "PUB a.b - within");
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    assert_eq!(nats.max_payload(), 64);

    let error = nats
        .publish("a.b", vec![b'x'; 65])
        .await
        .expect_err("above max_payload");
    match error {
        Error::PayloadTooLarge { declared, max } => assert_eq!((declared, max), (65, 64)),
        other => panic!("expected PayloadTooLarge, got {other}"),
    }
    assert_eq!(
        nats.state(),
        State::Connected,
        "a local refusal does not cost the connection"
    );

    // Ordered by a channel, not a sleep: the server has established the
    // silence before the legal publish goes out.
    within("a step of the test", wait.recv()).await.unwrap();
    nats.publish("a.b", b"within")
        .await
        .expect("inside the bound");
    within("a step of the test", nats.flush()).await.unwrap();
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// An asynchronous `INFO` that lowers `max_payload` takes effect on the next
/// publish, which is the whole reason the bound cannot be a constant.
#[tokio::test]
async fn a_later_info_moves_the_payload_bound() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server
            .handshake("{\"server_id\":\"S1\",\"max_payload\":1024}")
            .await;
        server
            .send_info("{\"server_id\":\"S1\",\"max_payload\":8}")
            .await;
        assert_eq!(server.read_op().await, "PING");
        server.write(b"PONG\r\n").await;
        server.expect_silence().await;
    });

    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("opened");
    assert_eq!(nats.max_payload(), 1024);

    // A flush is the protocol's only round trip, and the PONG that ends it
    // establishes that the driver has already read the INFO that came first.
    within("a step of the test", nats.flush()).await.unwrap();

    assert_eq!(nats.max_payload(), 8);
    assert!(matches!(
        nats.publish("a.b", b"nine byte").await,
        Err(Error::PayloadTooLarge {
            declared: 9,
            max: 8
        })
    ));

    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// `INFO.tls_required` on a build **with** TLS but no `ClientConfig`: the
/// only honest answer is to say TLS is required, because the trust decision
/// is the caller's.
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_required_without_a_client_config_says_so_plainly() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"tls_required\":true}")
            .await;
        // Not one octet of CONNECT in the clear: the client reports that TLS
        // is required and ends the transport instead.
        server.expect_end_of_stream().await;
    });

    let error = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect_err("TLS is required");
    assert!(matches!(error, Error::TlsRequired), "{error}");
    within("a step of the test", server).await.unwrap();
}
/// `INFO.tls_required` on a build **without** TLS: reported plainly rather
/// than continued in the clear. A client that ignored the flag would put its
/// `CONNECT`, credentials and all, on an unencrypted socket the server is
/// about to stop reading.
#[cfg(not(feature = "tls"))]
#[tokio::test]
async fn tls_required_on_a_build_without_tls_is_reported_rather_than_ignored() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"tls_required\":true}")
            .await;
        // Nothing in the clear on a build that cannot encrypt.
        server.expect_end_of_stream().await;
    });

    let error = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect_err("this build has no TLS");
    assert!(matches!(error, Error::TlsUnsupported), "{error}");
    within("a step of the test", server).await.unwrap();
}
/// The real thing: `INFO` in the clear, the TLS handshake, and then every
/// client octet inside the session — `CONNECT` first of all.
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_is_completed_before_any_ordinary_traffic() {
    use std::sync::Arc;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

    let exec = Exec::current().unwrap();
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert = CertificateDer::from(issued.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(issued.signing_key.serialize_der()));

    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let server_config = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap(),
    );

    let (listener, port) = Server::listen().await;
    let server = tokio::spawn(async move {
        let mut plain = Server::accept(&listener).await;
        // The INFO that demands TLS is the one operation that travels in the
        // clear, and it is the server's.
        plain.expect_silence().await;
        plain
            .send_info("{\"server_id\":\"S1\",\"tls_required\":true,\"headers\":true,\"proto\":1}")
            .await;

        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let tls = acceptor
            .accept(plain.stream)
            .await
            .expect("the client started TLS before saying anything");

        // Everything from here is inside the session.
        let mut inside = TlsServer {
            stream: tls,
            buf: Vec::new(),
            from: 0,
        };
        let connect = inside.read_op().await;
        assert!(connect.starts_with("CONNECT"), "{connect}");
        assert!(
            connect.contains("\"tls_required\":true"),
            "the client states the layer it is on: {connect}"
        );
        assert_eq!(inside.read_op().await, "PING");
        inside.write(b"PONG\r\n").await;
        assert_eq!(inside.read_op().await, "PUB inside - secret");
    });

    let mut options = options();
    options.tls_server_name = Some("localhost".to_owned());
    let nats = within(
        "finished",
        Connection::connect_tls(&exec, "127.0.0.1", port, options, client_config),
    )
    .await
    .expect("opened inside TLS");
    assert_eq!(nats.info().server_id.as_deref(), Some("S1"));

    // The server's script reads this publish inside the session, and joining
    // it is what establishes that the octets arrived encrypted.
    nats.publish("inside", b"secret").await.unwrap();
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// The TLS handshake is inside the handshake's deadline. A server that sends
/// an `INFO` demanding TLS and then goes quiet after the `ClientHello` would
/// otherwise hold the connect, and the socket, for as long as it liked.
#[cfg(feature = "tls")]
#[tokio::test]
async fn a_tls_handshake_that_never_finishes_hits_the_handshake_deadline() {
    let exec = Exec::current().unwrap();
    let (client_config, _) = tls_configs();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"tls_required\":true}")
            .await;
        // The `ClientHello` arrives and is never answered.
        std::future::pending::<()>().await;
        drop(server);
    });

    let mut options = options();
    options.handshake_timeout = Duration::from_millis(300);
    options.tls_server_name = Some("localhost".to_owned());
    let error = within(
        "the connect returned instead of waiting on the TLS handshake",
        Connection::connect_tls(&exec, "127.0.0.1", port, options, client_config),
    )
    .await
    .expect_err("no TLS session was ever established");
    assert!(
        matches!(&error, Error::HandshakeTimeout { step } if step.contains("TLS")),
        "{error}"
    );
    // The server never ends its silence, so the connect ended on the
    // handshake's deadline and not on the server's.
    assert!(!server.is_finished());
    server.abort();
}
/// A caller that hands over a TLS configuration has asked for TLS, and an
/// `INFO` that says the server has none is an answer to refuse, not to obey:
/// it arrives before TLS and before authentication, so an attacker on the path
/// can rewrite `tls_required` to `false`. The client writes no `CONNECT` — and
/// above all no password — in the clear.
#[cfg(feature = "tls")]
#[tokio::test]
async fn connect_tls_refuses_an_info_that_offers_no_tls_and_sends_no_credentials() {
    let exec = Exec::current().unwrap();
    let (client_config, _) = tls_configs();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server
            .send_info("{\"server_id\":\"S1\",\"auth_required\":true,\"tls_required\":false}")
            .await;
        // Not one octet: `expect_end_of_stream` fails with whatever arrived.
        server.expect_end_of_stream().await;
    });

    let mut options = options();
    options.credentials = Credentials::UserPassword {
        user: "alice".into(),
        password: "s3cret".into(),
    };
    options.tls_server_name = Some("localhost".to_owned());
    let error = within(
        "finished",
        Connection::connect_tls(&exec, "127.0.0.1", port, options, client_config),
    )
    .await
    .expect_err("the server offered no TLS");
    assert!(
        matches!(&error, Error::Tls(why) if why.contains("INFO")),
        "{error}"
    );
    within("the server script finished", server)
        .await
        .expect("the client wrote nothing in the clear");
}
/// A server in mixed mode offers TLS without demanding it. A caller that asked
/// for TLS takes the offer, and the `CONNECT` goes inside the session.
#[cfg(feature = "tls")]
#[tokio::test]
async fn connect_tls_upgrades_where_the_server_only_offers_tls() {
    let exec = Exec::current().unwrap();
    let (client_config, server_config) = tls_configs();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        let mut plain = Server::accept(&listener).await;
        plain.expect_silence().await;
        plain
            .send_info("{\"server_id\":\"S1\",\"tls_available\":true,\"proto\":1}")
            .await;
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let tls = acceptor
            .accept(plain.stream)
            .await
            .expect("the client started TLS on an offer");
        let mut inside = TlsServer {
            stream: tls,
            buf: Vec::new(),
            from: 0,
        };
        let connect = inside.read_op().await;
        assert!(connect.contains("\"tls_required\":true"), "{connect}");
        assert_eq!(inside.read_op().await, "PING");
        inside.write(b"PONG\r\n").await;
    });

    let mut options = options();
    options.tls_server_name = Some("localhost".to_owned());
    let nats = within(
        "finished",
        Connection::connect_tls(&exec, "127.0.0.1", port, options, client_config),
    )
    .await
    .expect("opened inside TLS");
    within("a step of the test", server).await.unwrap();
    within("a step of the test", nats.close()).await.unwrap();
}
/// The handshake is **one** budget. A server that answers everything the
/// client says with a `PING` a moment before the deadline never lets a clock
/// that restarts per operation run out, and never sends the `PONG`.
#[tokio::test]
async fn a_server_that_keeps_the_handshake_busy_without_a_pong_runs_into_one_deadline() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;

    let server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;

        let mut server = Server::accept(&listener).await;
        server.expect_silence().await;
        server.send_info("{\"server_id\":\"S1\"}").await;
        assert!(server.read_op().await.starts_with("CONNECT"));
        assert_eq!(server.read_op().await, "PING");
        // A `PING` every hundred milliseconds and never the `PONG`.
        while server.stream.write_all(b"PING\r\n").await.is_ok() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let mut options = options();
    options.handshake_timeout = Duration::from_millis(500);
    // Only the outcome is asserted: the connect ended on its own budget while
    // the server was still keeping it busy, whatever that took on the wall.
    let error = within(
        "the connect returned instead of being kept busy for ever",
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect_err("the PONG never came");
    assert!(
        matches!(&error, Error::HandshakeTimeout { step } if step.contains("PONG")),
        "{error}"
    );
    assert!(
        !server.is_finished(),
        "the server was still sending: the budget is the handshake's, not the server's"
    );
    server.abort();
}
/// An `INFO` grows with the cluster. Two hundred servers' worth of
/// `connect_urls` is a legitimate line well past the 4096 octets that bound a
/// `MSG`, and the connection has to be made.
#[tokio::test]
async fn a_large_info_from_a_big_cluster_is_accepted() {
    let exec = Exec::current().unwrap();
    let (listener, port) = Server::listen().await;
    let urls: Vec<String> = (0..200)
        .map(|n| format!("\"10.0.{n}.1.cluster.internal.example:4222\""))
        .collect();
    let info = format!(
        "{{\"server_id\":\"S1\",\"headers\":true,\"connect_urls\":[{}]}}",
        urls.join(",")
    );
    assert!(info.len() > 4096, "the line is above the bound of a MSG");

    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake(&info).await;
    });
    let nats = within(
        "finished",
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("a large INFO is not a protocol violation");
    assert_eq!(nats.info().connect_urls.len(), 200);
    within("a step of the test", server).await.unwrap();
}
