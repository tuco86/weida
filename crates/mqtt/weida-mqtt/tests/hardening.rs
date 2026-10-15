//! A hostile or merely stuck peer, and what the client owes it: the waits that
//! used to be unbounded, the queues that used to be, and the acknowledgements
//! that used to be believed.
//!
//! The scripted [`harness::Server`] is used where a script can say it; the
//! cases that are about *not reading*, or about counting what a socket took,
//! drive a raw listener, because a script that reads is the one thing those
//! cases cannot have.

use crate::harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use harness::{Act, DEADLINE, Server, bytes, connack, connack_session, decode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use weida_mqtt::{
    Client, Completion, Context, DisconnectReasonCode, Error, Event, Message, QoS, Session,
    StoredPublish,
};
use weida_mqtt_codec::{
    FixedHeader, Packet, Properties, Puback, PubackReasonCode, Publish, Pubrec,
};

/// Every await in this file is bounded.
async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("the await finished")
}

/// A listener whose accepted sockets have tiny buffers, so that a peer that
/// does not read stalls a write of any real size at once, on any machine.
fn small_buffers_listener() -> TcpListener {
    let socket = TcpSocket::new_v4().expect("a socket");
    socket.set_recv_buffer_size(4096).expect("a buffer");
    socket.set_send_buffer_size(4096).expect("a buffer");
    socket
        .bind("127.0.0.1:0".parse().expect("an address"))
        .expect("binds");
    socket.listen(1).expect("listens")
}

/// Accepts one client, reads its CONNECT and answers `connack`.
async fn raw_accept(listener: &TcpListener, connack: &[u8]) -> TcpStream {
    let (mut stream, _) = within(listener.accept()).await.expect("accepts");
    let mut buf = [0u8; 4096];
    let read = within(stream.read(&mut buf)).await.expect("reads");
    assert!(read > 0, "the CONNECT arrived");
    stream.write_all(connack).await.expect("writes the connack");
    stream
}

fn publish_bytes(topic: &str, payload: &[u8]) -> Vec<u8> {
    bytes(&Packet::Publish(Publish {
        topic,
        payload,
        ..Publish::default()
    }))
}

/// A peer that completes the handshake and then stops reading must not wedge
/// the connection: the write fails with a named error within the write
/// timeout, `disconnect` returns, and the event stream says why.
#[tokio::test]
async fn a_peer_that_stops_reading_ends_the_connection_and_disconnect_still_returns() {
    let listener = small_buffers_listener();
    let address = listener.local_addr().expect("an address").to_string();
    let accepted =
        tokio::spawn(async move { raw_accept(&listener, &connack(Properties::new())).await });

    let mut options = harness::options("stall");
    options.write_timeout = Some(Duration::from_secs(1));
    let context = Context::new().expect("ambient");
    let (client, mut events) = within(Client::connect(&context, &address, options))
        .await
        .expect("connects");
    // Held open and never read again.
    let _silent = within(accepted).await.expect("task");

    let client = Arc::new(client);
    let publishing = {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            client
                .publish(Message::new("t", vec![0u8; 16 * 1024 * 1024]))
                .await
        })
    };
    // Long enough for the task to take the command and fill the socket.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    let disconnected = tokio::time::timeout(
        Duration::from_secs(4),
        client.disconnect(DisconnectReasonCode::NormalDisconnection),
    )
    .await;
    assert!(
        disconnected.is_ok(),
        "disconnect returned although a write is stuck"
    );
    assert!(started.elapsed() < Duration::from_secs(4));

    match within(events.next()).await {
        Some(Event::Disconnected(Error::Timeout(what))) => assert_eq!(what, "write"),
        other => panic!("expected Disconnected(Timeout(\"write\")), got {other:?}"),
    }
    assert!(
        within(publishing).await.expect("task").is_err(),
        "the stuck publish is released, not left pending"
    );
}

/// A full event queue stops the socket being read and nothing else: the
/// keep-alive still goes out, and `disconnect` still returns, while the
/// application has taken not one event.
#[tokio::test]
async fn a_full_event_queue_leaves_keep_alive_and_disconnect_running() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("an address").to_string();
    let server = tokio::spawn(async move {
        let mut stream = raw_accept(&listener, &connack(Properties::new())).await;
        let delivery = publish_bytes("t", b"x");
        for _ in 0..1100 {
            stream.write_all(&delivery).await.expect("writes");
        }
        // Everything the client sends from here on, for a while.
        let mut seen = Vec::new();
        let end = tokio::time::Instant::now() + Duration::from_millis(3000);
        let mut buf = [0u8; 256];
        while let Ok(Ok(read)) = tokio::time::timeout_at(end, stream.read(&mut buf)).await {
            if read == 0 {
                break;
            }
            seen.extend_from_slice(&buf[..read]);
        }
        seen
    });

    let mut options = harness::options("full");
    options.keep_alive = Duration::from_secs(1);
    options.limits.incoming_queue = 16;
    let context = Context::new().expect("ambient");
    let (client, _events) = within(Client::connect(&context, &address, options))
        .await
        .expect("connects");

    // The application takes nothing. The PINGREQ is unanswered, and its
    // deadline does not run while the reader is held back for the queue.
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let disconnected = tokio::time::timeout(
        Duration::from_secs(2),
        client.disconnect(DisconnectReasonCode::NormalDisconnection),
    )
    .await;
    assert!(
        disconnected.is_ok(),
        "disconnect returned with the queue full"
    );

    let seen = within(server).await.expect("task");
    assert!(
        seen.windows(2).any(|pair| pair == [0xC0, 0x00]),
        "a PINGREQ went out while the queue was full: {seen:02X?}"
    );
    assert!(
        seen.windows(2).any(|pair| pair == [0xE0, 0x00]),
        "and so did the DISCONNECT: {seen:02X?}"
    );
}

/// The PINGRESP deadline runs from the PINGREQ and not from the last turn of
/// the loop: a client that keeps publishing still learns that nobody answers.
#[tokio::test]
async fn an_unanswered_ping_times_out_even_while_traffic_flows() {
    let server = Server::start(vec![Act::Send(connack(Properties::new()))]).await;
    let mut options = harness::options("ping");
    options.keep_alive = Duration::from_secs(1);
    options.ping_timeout = Some(Duration::from_secs(1));
    let context = Context::new().expect("ambient");
    let (client, mut events) = within(Client::connect(&context, &server.address(), options))
        .await
        .expect("connects");

    // Idle first: Keep Alive sends a PINGREQ only when nothing else has been
    // written within the interval, so the ping is out, and unanswered, before
    // the traffic starts.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let publishing = async {
        loop {
            let _ = client.publish(Message::new("t", "x")).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    tokio::select! {
        event = within(events.next()) => match event {
            Some(Event::Disconnected(Error::Timeout(what))) => assert_eq!(what, "PINGRESP"),
            other => panic!("expected Disconnected(Timeout(\"PINGRESP\")), got {other:?}"),
        },
        () = publishing => unreachable!("the publishing loop does not end"),
    }
}

/// A packet over the server's `Maximum Packet Size` is the packet's failure
/// and not the connection's.
#[tokio::test]
async fn an_oversize_publish_is_refused_without_closing_the_connection() {
    let mut server = Server::start(vec![
        Act::Send(connack(Properties {
            maximum_packet_size: Some(64),
            ..Properties::new()
        })),
        Act::Expect, // the small publish
        Act::Expect, // the DISCONNECT
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = within(Client::connect(
        &context,
        &server.address(),
        harness::options("oversize"),
    ))
    .await
    .expect("connects");

    let error = within(client.publish(Message::new("t", vec![0u8; 200])))
        .await
        .expect_err("over the ceiling");
    assert!(matches!(error, Error::Encode(_)), "{error}");

    let completion = within(client.publish(Message::new("t", "ok")))
        .await
        .expect("the connection is still there");
    assert_eq!(completion, Completion::Sent);
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    within(server.finished()).await;
}

/// Stalled publishes go out in the order they were made, whatever else the
/// server sends meanwhile.
#[tokio::test]
async fn stalled_publishes_keep_their_order() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            receive_maximum: Some(1),
            ..Properties::new()
        })),
        Act::Expect, // A, unanswered: the quota is spent
        Act::Send(bytes(&Packet::Pingresp)),
        Act::Send(bytes(&Packet::Puback(Puback::new(1)))),
        Act::Expect, // what follows the acknowledgement
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = within(Client::connect(
        &context,
        &server.address(),
        harness::options("order"),
    ))
    .await
    .expect("connects");
    let client = Arc::new(client);

    for topic in ["a", "b", "c"] {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            let _ = client
                .publish(Message::new(topic, "x").at(QoS::AtLeastOnce))
                .await;
        });
        tokio::task::yield_now().await;
    }

    let started = Instant::now();
    let third = loop {
        let seen = server.seen_bytes().await;
        if let Some(third) = seen.get(2) {
            break third.clone();
        }
        assert!(
            started.elapsed() < DEADLINE,
            "the second publish never came"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let Packet::Publish(publish) = decode(&third) else {
        panic!("a publish follows the acknowledgement");
    };
    assert_eq!(publish.topic, "b", "the oldest waiting publish goes first");
}

/// A publish whose future was dropped while it waited for the quota is not
/// sent when the quota frees: its caller was told it did not happen, and a
/// retry would otherwise be a duplicate.
#[tokio::test]
async fn a_cancelled_stalled_publish_is_never_sent() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            receive_maximum: Some(1),
            ..Properties::new()
        })),
        Act::Expect, // A, unanswered: the quota is spent
        Act::Idle(Duration::from_millis(800)),
        Act::Send(bytes(&Packet::Puback(Puback::new(1)))),
        Act::Expect, // what the freed quota admits
        Act::Idle(Duration::from_millis(500)),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = within(Client::connect(
        &context,
        &server.address(),
        harness::options("cancelled"),
    ))
    .await
    .expect("connects");
    let client = Arc::new(client);

    let first = {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            let _ = client
                .publish(Message::new("a", "x").at(QoS::AtLeastOnce))
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let cancelled = tokio::time::timeout(
        Duration::from_millis(100),
        client.publish(Message::new("b", "x").at(QoS::AtLeastOnce)),
    )
    .await;
    assert!(cancelled.is_err(), "the publish is stalled and times out");
    let last = {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            let _ = client
                .publish(Message::new("c", "x").at(QoS::AtLeastOnce))
                .await;
        })
    };

    let started = Instant::now();
    let next = loop {
        let seen = server.seen_bytes().await;
        if let Some(next) = seen.get(2) {
            break next.clone();
        }
        assert!(started.elapsed() < DEADLINE, "nothing followed the PUBACK");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let Packet::Publish(publish) = decode(&next) else {
        panic!("a publish follows the acknowledgement");
    };
    assert_eq!(publish.topic, "c", "the cancelled publish was skipped");
    first.abort();
    last.abort();
}

/// The queue of publishes waiting for the quota is bounded, and the entries of
/// dropped futures do not count against the bound: a loop of timed-out
/// publishes against a server that stopped acknowledging does not grow it,
/// and a live one is refused once the queue holds as many as are allowed.
#[tokio::test]
async fn the_stalled_queue_is_bounded_and_forgets_cancelled_publishes() {
    let server = Server::start(vec![
        Act::Send(connack(Properties {
            receive_maximum: Some(1),
            ..Properties::new()
        })),
        Act::Expect, // A, unanswered: the quota is spent
        Act::Idle(Duration::from_secs(5)),
    ])
    .await;
    let mut options = harness::options("bounded");
    options.limits.max_inflight = 2;
    let context = Context::new().expect("ambient");
    let (client, _events) = within(Client::connect(&context, &server.address(), options))
        .await
        .expect("connects");
    let client = Arc::new(client);
    let publish = |topic: &'static str| {
        let client = Arc::clone(&client);
        async move {
            client
                .publish(Message::new(topic, "x").at(QoS::AtLeastOnce))
                .await
        }
    };

    let mut waiting = vec![tokio::spawn(publish("a"))];
    tokio::time::sleep(Duration::from_millis(100)).await;
    waiting.push(tokio::spawn(publish("live-1")));
    tokio::time::sleep(Duration::from_millis(50)).await;
    for _ in 0..6 {
        let cancelled = tokio::time::timeout(Duration::from_millis(30), publish("gone")).await;
        assert!(cancelled.is_err(), "stalled, so it times out");
    }
    waiting.push(tokio::spawn(publish("live-2")));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let refused = within(publish("one-too-many")).await;
    assert!(
        matches!(refused, Err(Error::QuotaExhausted { .. })),
        "{refused:?}"
    );
    for handle in &waiting {
        assert!(!handle.is_finished(), "the live publishes still wait");
    }
    for handle in waiting {
        handle.abort();
    }
}

/// One PUBLISH with more User Properties than this client keeps is delivered
/// with the first of them and says so, is acknowledged like any other, and
/// costs the connection nothing.
#[tokio::test]
async fn a_delivery_past_the_user_property_cap_is_truncated_not_fatal() {
    let pairs: Vec<(String, String)> = (0..10).map(|n| (format!("k{n}"), "v".into())).collect();
    let borrowed: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let poisoned = bytes(&Packet::Publish(Publish {
        topic: "t",
        payload: b"first",
        qos: QoS::AtLeastOnce,
        packet_id: Some(1),
        properties: Properties::new().with_user_properties(&borrowed),
        ..Publish::default()
    }));
    let mut server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Send(poisoned),
        Act::Expect, // the PUBACK
        Act::Send(publish_bytes("t", b"second")),
        Act::Expect, // the DISCONNECT
    ])
    .await;
    let mut options = harness::options("poison");
    options.limits.max_user_properties = 4;
    let context = Context::new().expect("ambient");
    let (client, mut events) = within(Client::connect(&context, &server.address(), options))
        .await
        .expect("connects");

    let Some(Event::Delivered(first)) = within(events.next()).await else {
        panic!("the poisoned message is still a delivery");
    };
    assert_eq!(first.payload, b"first");
    assert!(first.properties.truncated);
    assert_eq!(first.properties.user_properties.len(), 4);
    let Some(Event::Delivered(second)) = within(events.next()).await else {
        panic!("the connection survived it");
    };
    assert_eq!(second.payload, b"second");

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .await
        .expect("disconnects");
    within(server.finished()).await;
    let seen = server.seen_bytes().await;
    let Packet::Puback(puback) = decode(&seen[1]) else {
        panic!("the PUBACK");
    };
    assert_eq!(puback.reason_code, PubackReasonCode::Success);
}

/// An acknowledgement for an identifier nothing holds settles nothing and
/// tells the application nothing.
#[tokio::test]
async fn an_acknowledgement_for_nothing_is_not_an_event() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Send(bytes(&Packet::Puback(Puback::new(999)))),
        Act::Send(publish_bytes("t", b"real")),
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (_client, mut events) = within(Client::connect(
        &context,
        &server.address(),
        harness::options("bogus-ack"),
    ))
    .await
    .expect("connects");
    match within(events.next()).await {
        Some(Event::Delivered(delivery)) => assert_eq!(delivery.payload, b"real"),
        other => panic!("the bogus PUBACK must not surface: {other:?}"),
    }
}

/// A PUBACK is the end of a QoS 1 exchange and means nothing to a QoS 2 one:
/// the publish stays pending until its own PUBCOMP.
#[tokio::test]
async fn a_puback_does_not_settle_a_qos2_exchange() {
    let server = Server::start(vec![
        Act::Send(connack(Properties::new())),
        Act::Expect, // the PUBLISH, identifier 1
        Act::Send(bytes(&Packet::Puback(Puback::new(1)))),
        Act::Send(bytes(&Packet::Pubrec(Pubrec::new(1)))),
        Act::CompletePubrel,
    ])
    .await;
    let context = Context::new().expect("ambient");
    let (client, _events) = within(Client::connect(
        &context,
        &server.address(),
        harness::options("stage"),
    ))
    .await
    .expect("connects");
    let completion = within(client.publish(Message::new("t", "x").at(QoS::ExactlyOnce)))
        .await
        .expect("completes");
    assert!(
        matches!(completion, Completion::Complete(_)),
        "settled by the PUBCOMP and not by the PUBACK: {completion:?}"
    );
}

/// Reads packets from `stream` for `window` and returns the identifiers of the
/// PUBLISHes among them.
async fn publishes_within(stream: &mut TcpStream, window: Duration) -> Vec<u16> {
    let end = tokio::time::Instant::now() + window;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut ids = Vec::new();
    while let Ok(Ok(read)) = tokio::time::timeout_at(end, stream.read(&mut chunk)).await {
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..read]);
        while let Ok((header, header_len)) = FixedHeader::decode(&buf, u32::MAX) {
            let total = header_len + header.remaining_length as usize;
            if buf.len() < total {
                break;
            }
            if let Packet::Publish(publish) = decode(&buf[..total]) {
                ids.extend(publish.packet_id);
            }
            buf.drain(..total);
        }
    }
    ids
}

fn stored(topic: &str, payload: Vec<u8>) -> Arc<StoredPublish> {
    Arc::new(StoredPublish {
        topic: topic.into(),
        payload: payload.into(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload_format_indicator: None,
        message_expiry_interval: None,
        content_type: None,
        response_topic: None,
        correlation_data: None,
        user_properties: Vec::new(),
    })
}

/// A session that held more than the server now lets be in flight goes back
/// out through the server's `Receive Maximum`: one PUBLISH, then one more per
/// acknowledgement.
#[tokio::test]
async fn a_resumed_session_is_paced_through_the_servers_receive_maximum() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("an address").to_string();
    let server = tokio::spawn(async move {
        let mut stream = raw_accept(
            &listener,
            &connack_session(
                true,
                Properties {
                    receive_maximum: Some(1),
                    ..Properties::new()
                },
            ),
        )
        .await;
        let first = publishes_within(&mut stream, Duration::from_millis(700)).await;
        let acknowledged = bytes(&Packet::Puback(Puback::new(first[0])));
        stream.write_all(&acknowledged).await.expect("writes");
        let second = publishes_within(&mut stream, Duration::from_millis(700)).await;
        (first, second, stream)
    });

    let limits = weida_mqtt::ConnectOptions::default().limits;
    let session = Session::new("resume", &limits);
    for index in 0..5 {
        session
            .allocate(stored(&format!("t{index}"), vec![index]))
            .expect("room");
    }
    let mut options = harness::options("resume");
    options.clean_start = false;
    options.session_expiry = Some(Duration::from_secs(300));
    let context = Context::new().expect("ambient");
    let (_client, _events) = within(Client::connect_session(
        &context, &address, options, &session,
    ))
    .await
    .expect("connects");

    let (first, second, _stream) = within(server).await.expect("task");
    assert_eq!(
        first.len(),
        1,
        "one PUBLISH before any acknowledgement: {first:?}"
    );
    assert_eq!(second.len(), 1, "and one more after it: {second:?}");
    assert_ne!(first, second);
}

/// A stored message too large for what the server now accepts is let go on
/// its own and reported, and does not fail the connect — or every later one.
#[tokio::test]
async fn a_stored_message_that_no_longer_fits_is_abandoned_not_fatal() {
    let server = Server::start(vec![
        Act::Send(connack_session(
            true,
            Properties {
                maximum_packet_size: Some(100),
                ..Properties::new()
            },
        )),
        Act::Idle(Duration::from_secs(2)),
    ])
    .await;
    let limits = weida_mqtt::ConnectOptions::default().limits;
    let session = Session::new("resume-big", &limits);
    let id = session
        .allocate(stored("big", vec![0u8; 2048]))
        .expect("room");
    let mut options = harness::options("resume-big");
    options.clean_start = false;
    options.session_expiry = Some(Duration::from_secs(300));
    let context = Context::new().expect("ambient");
    let (_client, mut events) = within(Client::connect_session(
        &context,
        &server.address(),
        options,
        &session,
    ))
    .await
    .expect("the connect succeeds");

    match within(events.next()).await {
        Some(Event::Abandoned { packet_id, error }) => {
            assert_eq!(packet_id, id);
            assert!(matches!(error, Error::Encode(_)), "{error}");
        }
        other => panic!("expected Abandoned, got {other:?}"),
    }
    assert_eq!(
        session.in_flight(),
        0,
        "released, so no later connect trips on it"
    );
}

/// The queue of deliveries is bounded in bytes beside its count: with the
/// byte ceiling at 1 MiB, a peer pushing 35 MiB at a client that takes
/// nothing is stopped by the socket, where a count of 1,024 would have taken
/// it all.
#[tokio::test]
async fn the_event_queue_is_bounded_in_bytes() {
    let listener = small_buffers_listener();
    let address = listener.local_addr().expect("an address").to_string();
    let server = tokio::spawn(async move {
        let mut stream = raw_accept(&listener, &connack(Properties::new())).await;
        let delivery = publish_bytes("t", &vec![7u8; 60 * 1024]);
        let all = async {
            for _ in 0..600 {
                stream.write_all(&delivery).await.expect("writes");
            }
        };
        let finished = tokio::time::timeout(Duration::from_secs(3), all)
            .await
            .is_ok();
        (finished, stream)
    });

    let mut options = harness::options("bytes");
    options.limits.incoming_queue_bytes = 1024 * 1024;
    let context = Context::new().expect("ambient");
    let (_client, _events) = within(Client::connect(&context, &address, options))
        .await
        .expect("connects");

    let (finished, _stream) = within(server).await.expect("task");
    assert!(
        !finished,
        "the client absorbed 35 MiB of deliveries it was never going to read"
    );
}

/// With the TLS handshake stalled by the server, the connect still ends at
/// `connect_timeout`, naming the step.
#[cfg(feature = "tls")]
#[tokio::test]
async fn a_stalled_tls_handshake_is_bounded_by_the_connect_timeout() {
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let port = listener.local_addr().expect("an address").port();
    // Accepts and says nothing, for as long as the test needs.
    let _held = tokio::spawn(async move {
        let accepted = listener.accept().await;
        tokio::time::sleep(Duration::from_secs(8)).await;
        drop(accepted);
    });

    let config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let mut options = harness::options("tls-stall");
    options.connect_timeout = Duration::from_secs(1);
    options.tls = Some(weida_mqtt::TlsOptions::new(config).with_server_name("localhost"));
    let context = Context::new().expect("ambient");

    let started = Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&context, &format!("127.0.0.1:{port}"), options),
    )
    .await
    .expect("the connect ended by itself")
    .expect_err("no handshake");
    assert!(
        matches!(error, Error::Timeout("the TLS handshake")),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(4));
}
