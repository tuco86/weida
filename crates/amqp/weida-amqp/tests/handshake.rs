//! The handshake against a scripted server, octet by octet.
//!
//! Every test here stands up a `TcpListener` in the same process and plays
//! the server half by hand, because that is the only way to assert what the
//! *client* put on the wire and in what order. A real peer
//! (`tests/interop_fe2o3.rs`, `tests/interop_rabbitmq.rs`) proves
//! interoperability; this proves the sequence.
//!
//! Part 2 §2.2's four negotiation outcomes each get a test, because they
//! arrive in the same eight octets and a client that confused them would
//! retry the wrong thing:
//!
//! * the header we asked for comes back — the connection opens;
//! * a *different protocol-id* comes back — a security layer is mandatory;
//! * a *different version* comes back — the peer is not a 1.0 server;
//! * nothing comes back — the handshake deadline, which is ours because the
//!   protocol has none.

mod support;

#[cfg(feature = "tls")]
use std::sync::Arc;
use std::time::Duration;

use support::{DEADLINE, Server, accept, options, scripted};
#[cfg(feature = "tls")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
use weida_amqp::options::Sasl;
use weida_amqp::{Connection, Error, SessionOptions, State};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Close, Open, Performative};
#[cfg(feature = "tls")]
use weida_amqp_codec::protocol_header;
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::sasl::{SaslCode, SaslFrame, SaslMechanisms, SaslOutcome};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Multiple};
use weida_runtime::Exec;

impl Server {
    async fn send_header(&mut self, header: ProtocolHeader) {
        self.write(&header.encode()).await;
    }

    async fn send_open(&mut self, idle_time_out: Option<u32>) {
        let mut open = Open::new("broker-1");
        open.max_frame_size = 4096;
        open.channel_max = 15;
        open.idle_time_out = idle_time_out;
        open.offered_capabilities = Multiple::One("ANONYMOUS-RELAY");
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn send_sasl(&mut self, body: SaslFrame<'_>) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Sasl, 0, MIN_MAX_FRAME_SIZE, |buf| {
            body.encode(buf)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn send_close(&mut self, error: Option<weida_amqp_codec::AmqpError<'_>>) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Close(Close { error }).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }
}

#[tokio::test]
async fn a_bare_connection_opens_and_closes() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        // The client MUST send its header immediately on connect, before it
        // has seen anything from us: that is what makes a multi-protocol
        // server able to dispatch on it.
        assert_eq!(
            server.read_header().await,
            ProtocolHeader::AMQP,
            "the first eight octets are AMQP %d0 1.0.0"
        );
        server.send_header(ProtocolHeader::AMQP).await;

        // `open` is the first frame and it is on channel 0.
        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "open"));
        server.send_open(None).await;

        // ...and `close` is the last thing the client ever writes.
        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "close"));
        server.send_close(None).await;
        server
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("the handshake finished")
    .expect("the connection opened");

    assert_eq!(connection.remote().container_id, "broker-1");
    assert_eq!(connection.remote().max_frame_size, 4096);
    assert_eq!(connection.remote().channel_max, 15);
    assert!(connection.remote().offers("ANONYMOUS-RELAY"));
    assert!(
        !connection.remote().offers("DELAYED_DELIVERY"),
        "a capability the peer did not list is not offered"
    );
    assert_eq!(connection.state(), State::Open);
    assert_eq!(
        connection.options().max_sessions(),
        256,
        "the session table is bounded by our own channel-max"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .expect("close finished")
        .expect("close succeeded");
    assert!(matches!(
        tokio::time::timeout(DEADLINE, connection.closed())
            .await
            .unwrap(),
        State::Closed(None)
    ));
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_different_protocol_id_is_a_demand_and_not_a_retry() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        assert_eq!(server.read_header().await, ProtocolHeader::AMQP);
        // Part 2 §2.2: a server requiring SASL answers a %d0 request with
        // %d3 and closes.
        server.send_header(ProtocolHeader::SASL).await;
        drop(server);
    });

    let error = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("the handshake finished")
    .expect_err("a security layer is mandatory");

    match error {
        Error::SecurityLayerRequired { requested, offered } => {
            assert_eq!(requested.octet(), 0);
            assert_eq!(offered.octet(), 3);
        }
        other => panic!("expected SecurityLayerRequired, got {other}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn an_amqp_0_9_1_server_is_a_version_mismatch() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        // 0-9-1 shares this port and this four-octet prefix. Its header is
        // `AMQP` followed by 0 0 9 1, and the protocol-id octet is the same
        // %d0 we asked for - so only the version tells the two apart.
        server.write(b"AMQP\x00\x00\x09\x01").await;
        drop(server);
    });

    let error = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("the handshake finished")
    .expect_err("not a 1.0 server");

    match error {
        Error::VersionMismatch { offered } => assert_eq!(offered.to_string(), "0.9.1"),
        other => panic!("expected VersionMismatch, got {other}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_server_that_says_nothing_hits_our_own_deadline() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        // Say nothing at all. The protocol gives no deadline for the
        // answering header, so without one of ours this would hang for as
        // long as the OS allows.
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let mut options = options();
    options.handshake_timeout = Duration::from_millis(200);
    let error = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("the deadline fired well inside the test's own")
    .expect_err("the handshake timed out");

    match error {
        Error::HandshakeTimeout { step } => {
            assert_eq!(step, "the answering AMQP protocol header");
        }
        other => panic!("expected HandshakeTimeout, got {other}"),
    }
    server.abort();
}

#[tokio::test]
async fn the_sasl_plain_dialog_runs_inside_the_512_octet_frame() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        // %d3 both ways, then the dialog, then %d0 both ways.
        assert_eq!(server.read_header().await, ProtocolHeader::SASL);
        server.send_header(ProtocolHeader::SASL).await;
        server
            .send_sasl(SaslFrame::Mechanisms(SaslMechanisms {
                server_mechanisms: Multiple::from_slice(&["PLAIN", "ANONYMOUS"]),
            }))
            .await;

        let (_, bytes) = server.read_frame().await;
        let init = frame::decode(&bytes, u32::MAX).unwrap();
        assert_eq!(
            init.header.kind,
            FrameKind::Sasl,
            "the dialog runs in type-0x01 frames"
        );
        assert!(
            init.used() <= 512,
            "a SASL frame is capped at 512 octets with no way to negotiate more"
        );
        let (body, _) = SaslFrame::decode(init.body, Limits::DEFAULT).unwrap();
        match body {
            SaslFrame::Init(init) => {
                assert_eq!(init.mechanism, "PLAIN");
                assert_eq!(
                    init.initial_response,
                    Some(b"\0guest\0secret".as_slice()),
                    "PLAIN is authzid NUL authcid NUL passwd, RFC 4616"
                );
            }
            other => panic!("expected sasl-init, got {}", other.name()),
        }

        server
            .send_sasl(SaslFrame::Outcome(SaslOutcome {
                code: SaslCode::Ok,
                additional_data: None,
            }))
            .await;

        assert_eq!(
            server.read_header().await,
            ProtocolHeader::AMQP,
            "a successful outcome is followed by AMQP %d0 1.0.0"
        );
        server.send_header(ProtocolHeader::AMQP).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "open");
        server.send_open(None).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close");
        server.send_close(None).await;
    });

    let mut options = options();
    options.sasl = Sasl::Plain {
        username: "guest".into(),
        password: "secret".into(),
    };
    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("the handshake finished")
    .expect("authenticated");

    // The connection keeps its options for its whole life, and applications
    // log both: neither print may carry the password.
    let printed = format!(
        "{connection:?} {:?} {:#?}",
        connection.options(),
        connection.options()
    );
    assert!(!printed.contains("secret"), "{printed}");
    assert!(printed.contains("guest"), "the identity is not a secret");
    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_failed_sasl_outcome_carries_its_code() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::SASL).await;
        server
            .send_sasl(SaslFrame::Mechanisms(SaslMechanisms {
                server_mechanisms: Multiple::One("PLAIN"),
            }))
            .await;
        let _ = server.read_frame().await;
        server
            .send_sasl(SaslFrame::Outcome(SaslOutcome {
                code: SaslCode::Auth,
                additional_data: None,
            }))
            .await;
        drop(server);
    });

    let mut options = options();
    options.sasl = Sasl::Plain {
        username: "guest".into(),
        password: "wrong".into(),
    };
    let error = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("finished")
    .expect_err("the credentials were refused");
    match error {
        Error::Sasl { code, mechanism } => {
            assert_eq!(code, SaslCode::Auth);
            assert_eq!(mechanism, "PLAIN");
        }
        other => panic!("expected Sasl, got {other}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_mechanism_the_server_does_not_offer_is_refused_rather_than_downgraded() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::SASL).await;
        server
            .send_sasl(SaslFrame::Mechanisms(SaslMechanisms {
                server_mechanisms: Multiple::One("GSSAPI"),
            }))
            .await;
        drop(server);
    });

    let mut options = options();
    options.sasl = Sasl::Plain {
        username: "guest".into(),
        password: "guest".into(),
    };
    let error = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("finished")
    .expect_err("nothing in common");
    match error {
        Error::NoSharedSaslMechanism { offered } => assert_eq!(offered, ["GSSAPI"]),
        other => panic!("expected NoSharedSaslMechanism, got {other}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn an_empty_frame_answers_the_peers_idle_timeout_at_half_the_interval() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::AMQP).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "open");
        // We ask for a frame every 200 ms, so the client should emit one
        // every 100 ms: a keep-alive that arrives exactly at the deadline
        // arrives too late.
        server.send_open(Some(200)).await;

        let started = std::time::Instant::now();
        for _ in 0..2 {
            let (channel, name, _) = server.read_named().await;
            assert_eq!(
                (channel, name.as_str()),
                (0, "empty"),
                "the keep-alive is a header and nothing else, on channel 0"
            );
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(200 * 2),
            "two keep-alives at half of 200 ms took {elapsed:?}"
        );
        server
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");
    assert_eq!(connection.remote().idle_time_out, Some(200));
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_zero_idle_timeout_means_unset_and_sends_no_keepalive() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::AMQP).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "open");
        // Part 2 §2.4.5: zero equals unset, and unset means no timeout.
        server.send_open(Some(0)).await;
        // Nothing may arrive for a while. The next thing must be the close.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(
            name, "close",
            "a zero idle-time-out asks for no keep-alive at all"
        );
        server.send_close(None).await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");
    assert_eq!(
        connection.remote().idle_time_out,
        None,
        "zero is folded into None so no caller has to remember the rule"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn our_own_idle_threshold_closes_with_an_explanation() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::AMQP).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "open");
        server.send_open(None).await;
        // Then go quiet. Part 2 §2.4.5: the client SHOULD close with an
        // error explaining why, and MAY then drop the socket.
        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Close(Close { error: Some(error) }) => {
                assert_eq!(error.condition, condition::CONNECTION_FORCED);
                assert!(
                    error.description.unwrap().contains("idle threshold"),
                    "the close says why"
                );
            }
            other => panic!("expected close with an error, got {}", other.name()),
        }
    });

    let mut options = options();
    options.idle_time_out = Some(Duration::from_millis(150));
    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("finished")
    .expect("opened");

    let state = tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .expect("the threshold fired");
    match state {
        State::Failed(why) => assert!(why.contains("idle threshold"), "{why}"),
        other => panic!("expected Failed, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

/// Claim: the read threshold is measured from the last frame that *arrived*,
/// so a connection whose peer has silently gone is closed even while this
/// side is still busy (Part 2 §2.4.5).
///
/// The companion of the test above, and the one that pins the mechanism: with
/// the driver's timer rebuilt inside its `select!`, every command the
/// application handed over restarted the threshold, so exactly the connection
/// that is carrying work — the one worth noticing the loss of — never timed
/// out at all.
#[tokio::test]
async fn our_idle_threshold_is_not_reset_by_our_own_traffic() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::AMQP).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "open");
        // No idle-time-out of its own, so this side sends no keep-alive: every
        // frame on the wire is one the application asked for.
        server.send_open(None).await;
        // Then answer nothing ever again, while still reading — a server that
        // stopped reading would stall the client's writes and the test would
        // be proving something else.
        loop {
            let (_, name, _) = server.read_named().await;
            if name == "begin" {
                continue;
            }
            assert_eq!(
                name, "close",
                "the only other thing the client may send is its own close"
            );
            return;
        }
    });

    let mut options = options();
    options.idle_time_out = Some(Duration::from_millis(150));
    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("finished")
    .expect("opened");

    // Keep the driver working: a session request every 20 ms, well inside the
    // 150 ms threshold, none of them answered. Each is a turn of the driver's
    // loop, and a threshold that restarted on every turn would never be reached.
    let busy = {
        let connection = connection.clone();
        tokio::spawn(async move {
            loop {
                let asking = connection.clone();
                tokio::spawn(async move {
                    let _ = asking.begin(SessionOptions::default()).await;
                });
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };

    let state = tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .expect("a busy connection reaches its own threshold too");
    match state {
        State::Failed(why) => assert!(why.contains("idle threshold"), "{why}"),
        other => panic!("expected Failed, got {other:?}"),
    }
    busy.abort();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_peer_initiated_close_is_reported_with_its_condition() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let _ = server.read_header().await;
        server.send_header(ProtocolHeader::AMQP).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "open");
        server.send_open(None).await;
        server
            .send_close(Some(
                weida_amqp_codec::AmqpError::new(condition::CONNECTION_FORCED)
                    .described("an operator intervened"),
            ))
            .await;
        // The client answers with its own close: simultaneous close is legal
        // and answering is what makes it orderly.
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close");
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .expect("finished")
    .expect("opened");

    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .expect("the peer closed")
    {
        State::Closed(Some(condition)) => {
            assert_eq!(condition.condition, condition::CONNECTION_FORCED);
            assert_eq!(
                condition.description.as_deref(),
                Some("an operator intervened")
            );
        }
        other => panic!("expected Closed with a condition, got {other:?}"),
    }
    // Closing an already-closed connection is not an error: a caller racing
    // its own shutdown should not need a lock to be correct.
    connection.close().await.expect("idempotent");
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_tls_mode_on_the_plain_constructor_is_refused_where_it_is_configured() {
    let exec = Exec::current().unwrap();
    let mut options = options();
    options.tls = weida_amqp::TlsMode::Layered;
    // No listener at all: the refusal must happen before anything is dialled,
    // which is what "refused where it is configured" means.
    let error = Connection::connect(&exec, "127.0.0.1", 1, options)
        .await
        .expect_err("refused");
    assert!(
        matches!(error, Error::Configuration(ref why) if why.contains("connect_tls")),
        "{error}"
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_is_layered_by_protocol_id_two_and_the_second_header_is_inside_it() {
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

    let (listener, port) = scripted().await;
    let server = tokio::spawn(async move {
        let mut plain = accept(&listener).await;
        // In the clear, before any TLS: AMQP %d2 1.0.0 both ways.
        assert_eq!(
            plain.read_header().await,
            ProtocolHeader::TLS,
            "the layered form announces itself in the clear"
        );
        plain.send_header(ProtocolHeader::TLS).await;

        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let tls = acceptor.accept(plain.stream).await.unwrap();
        let mut inside = TlsServer {
            stream: tls,
            buf: Vec::new(),
            from: 0,
        };
        // And inside TLS: AMQP %d0 1.0.0, then open, then close.
        assert_eq!(inside.read_header().await, ProtocolHeader::AMQP);
        inside.send_header(ProtocolHeader::AMQP).await;
        assert_eq!(inside.read_performative().await, "open");
        inside.send_open().await;
        assert_eq!(inside.read_performative().await, "close");
        inside.send_close().await;
    });

    let mut options = options();
    options.tls = weida_amqp::TlsMode::Layered;
    options.tls_server_name = Some("localhost".to_owned());
    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect_tls(&exec, "127.0.0.1", port, options, client_config),
    )
    .await
    .expect("finished")
    .expect("opened inside TLS");
    assert!(connection.state().is_usable());
    assert_eq!(connection.remote().container_id, "broker-1");
    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

/// The same scripted server, one layer up: everything after the `%d2`
/// exchange happens inside the TLS session.
#[cfg(feature = "tls")]
struct TlsServer {
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    buf: Vec<u8>,
    from: usize,
}

#[cfg(feature = "tls")]
impl TlsServer {
    async fn fill(&mut self, want: usize) {
        while self.buf.len() - self.from < want {
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed early");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    async fn read_header(&mut self) -> ProtocolHeader {
        self.fill(protocol_header::LEN).await;
        let header = protocol_header::decode(&self.buf[self.from..]).unwrap();
        self.from += protocol_header::LEN;
        header
    }

    async fn read_performative(&mut self) -> String {
        self.fill(8).await;
        let header = frame::decode_header(&self.buf[self.from..], u32::MAX).unwrap();
        let size = header.size as usize;
        self.fill(size).await;
        let bytes = self.buf[self.from..self.from + size].to_vec();
        self.from += size;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        performative.name().to_owned()
    }

    async fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn send_header(&mut self, header: ProtocolHeader) {
        self.write(&header.encode()).await;
    }

    async fn send_open(&mut self) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(Open::new("broker-1")).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn send_close(&mut self) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Close(Close { error: None }).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_server_that_goes_silent_during_the_tls_handshake_is_given_up_on() {
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    // Both ways of reaching TLS end in the same handshake, and in both a
    // server that accepts the connection and then says nothing would hold the
    // caller for as long as the OS allows.
    for mode in [weida_amqp::TlsMode::Direct, weida_amqp::TlsMode::Layered] {
        let exec = Exec::current().unwrap();
        let (listener, port) = scripted().await;
        let layered = matches!(mode, weida_amqp::TlsMode::Layered);
        let server = tokio::spawn(async move {
            let mut server = accept(&listener).await;
            if layered {
                // The clear-text header exchange is answered; the handshake
                // after it never is.
                assert_eq!(server.read_header().await, ProtocolHeader::TLS);
                server.send_header(ProtocolHeader::TLS).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(server);
        });

        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let mut options = options();
        options.tls = mode;
        options.handshake_timeout = Duration::from_millis(200);
        let started = std::time::Instant::now();
        let error = tokio::time::timeout(
            DEADLINE,
            Connection::connect_tls(&exec, "127.0.0.1", port, options, config),
        )
        .await
        .expect("connect_tls returned instead of waiting on the peer")
        .expect_err("a silent server has not completed a handshake");
        assert!(
            matches!(error, Error::HandshakeTimeout { step } if step.contains("TLS")),
            "{mode:?}: {error}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "{mode:?} took {:?} with a 200 ms deadline",
            started.elapsed()
        );
        server.abort();
    }
}

#[tokio::test]
async fn a_begin_that_races_the_peers_close_reports_the_peers_condition() {
    // The refusal comes straight behind the open, so whether the client's
    // `begin` is taken before or after the driver has seen it is a race. Both
    // outcomes must say the same thing: the caller asked for a session on a
    // connection the peer refused, and the condition is the only explanation
    // the protocol carries.
    for _ in 0..25 {
        let exec = Exec::current().unwrap();
        let (listener, port) = scripted().await;
        let server = tokio::spawn(async move {
            let mut server = accept(&listener).await;
            assert_eq!(server.read_header().await, ProtocolHeader::AMQP);
            server.send_header(ProtocolHeader::AMQP).await;
            let (_, name, _) = server.read_named().await;
            assert_eq!(name, "open");
            server.send_open(None).await;
            server
                .send_close(Some(
                    weida_amqp_codec::AmqpError::new(condition::RESOURCE_LIMIT_EXCEEDED)
                        .described("too many connections"),
                ))
                .await;
            server
        });
        let connection = tokio::time::timeout(
            DEADLINE,
            Connection::connect(&exec, "127.0.0.1", port, options()),
        )
        .await
        .expect("the handshake finished")
        .expect("opened");
        let error = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
            .await
            .expect("begin returned instead of waiting")
            .expect_err("the peer closed the connection");
        match error {
            Error::Closed(Some(condition)) => {
                assert_eq!(condition.condition, condition::RESOURCE_LIMIT_EXCEEDED);
            }
            other => panic!("expected the peer's condition, got {other}"),
        }
        server.abort();
    }
}
