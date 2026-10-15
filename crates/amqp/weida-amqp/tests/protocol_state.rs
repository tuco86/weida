//! Connection-level frame checks and the answers to undecodable input, against
//! a scripted server.
//!
//! After `open` only AMQP frames on channel 0 are the connection's own; a frame
//! of the SASL type, a `close` on another channel, or a second `open` is a
//! protocol violation that ends the connection with the condition the
//! specification names. A frame whose header declares more than we accept, or
//! whose body does not decode, is answered with a `close` before the transport
//! is dropped. A keep-alive is a valid frame and keeps the connection open.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use support::{DEADLINE, Server, accept, options, scripted};
use tokio::io::AsyncReadExt;
use weida_amqp::{Connection, State};
use weida_amqp_codec::Limits;
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Close, Open, Performative};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::types::condition;
use weida_runtime::Exec;

/// Long enough that a frame the client meant to send would have arrived, short
/// enough that asserting an absence does not dominate the suite.
const QUIET: Duration = Duration::from_millis(150);

/// The performative of a frame that is not a keep-alive.
fn decode_performative(bytes: &[u8]) -> Performative<'_> {
    let frame = frame::decode(bytes, u32::MAX).unwrap();
    Performative::decode(frame.body, Limits::DEFAULT).unwrap().0
}

impl Server {
    async fn handshake(&mut self) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "open");
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    /// The client's `close`, on channel 0, with its condition if it gave one.
    async fn read_close(&mut self) -> Option<String> {
        let (channel, name, bytes) = self.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "close"));
        match decode_performative(&bytes) {
            Performative::Close(close) => close.error.map(|e| e.condition.to_owned()),
            _ => unreachable!("the name said close"),
        }
    }

    /// Reads until the client's `close` and answers it.
    async fn expect_close(&mut self) {
        loop {
            let (_, name, _) = self.read_named().await;
            if name == "close" {
                break;
            }
        }
        self.send(0, Performative::Close(Close { error: None }))
            .await;
    }
}

#[tokio::test]
async fn a_frame_of_the_sasl_type_after_open_is_a_framing_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // A valid performative, but in a SASL-typed frame: the SASL layer
        // ended with the header exchange.
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Sasl, 0, u32::MAX, |body| {
            Performative::Close(Close { error: None }).encode(body)
        })
        .unwrap();
        server.write(&out).await;

        let condition = server.read_close().await;
        assert_eq!(
            condition.as_deref(),
            Some(condition::CONNECTION_FRAMING_ERROR)
        );
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::CONNECTION_FRAMING_ERROR);
        }
        other => panic!("expected Closed with framing-error, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn an_empty_sasl_type_frame_after_open_is_a_framing_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // A header and no body, of the SASL type: size 8, doff 2, type 1,
        // channel 0. Being empty does not make it a keep-alive.
        server.write(&[0, 0, 0, 8, 2, 1, 0, 0]).await;

        let condition = server.read_close().await;
        assert_eq!(
            condition.as_deref(),
            Some(condition::CONNECTION_FRAMING_ERROR)
        );
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::CONNECTION_FRAMING_ERROR);
        }
        other => panic!("expected Closed with framing-error, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn close_on_a_channel_other_than_zero_is_a_framing_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // A `close` on channel 1 is not connection control, which lives on
        // channel 0. It must not be taken as the peer's close.
        server
            .send(1, Performative::Close(Close { error: None }))
            .await;

        let condition = server.read_close().await;
        assert_eq!(
            condition.as_deref(),
            Some(condition::CONNECTION_FRAMING_ERROR)
        );
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::CONNECTION_FRAMING_ERROR);
        }
        other => panic!("expected Closed with framing-error, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_second_open_is_illegal_state() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // The connection is already open; a second `open` is not a state the
        // protocol permits.
        server
            .send(0, Performative::Open(Open::new("broker-2")))
            .await;

        let condition = server.read_close().await;
        assert_eq!(condition.as_deref(), Some(condition::ILLEGAL_STATE));
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::ILLEGAL_STATE);
        }
        other => panic!("expected Closed with illegal-state, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_frame_declaring_more_than_we_accept_is_answered_with_a_close() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // A header declaring a size of 0xFFFF_FFFF, far above the 128 KiB
        // this client accepts. Nothing of the frame is read.
        server.write(&[0xFF, 0xFF, 0xFF, 0xFF, 2, 0, 0, 0]).await;

        let condition = server.read_close().await;
        assert_eq!(
            condition.as_deref(),
            Some(condition::CONNECTION_FRAMING_ERROR)
        );

        // The transport is dropped after the close, so the next read is EOF.
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(DEADLINE, server.stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, 0, "the client drops the transport after its close");
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Failed(_) => {}
        other => panic!("expected Failed, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_sound_frame_with_an_unreadable_body_is_answered_with_decode_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // The header is sound: size 12, doff 2, AMQP type, channel 0. The body
        // is a described list whose descriptor no performative has.
        server
            .write(&[0, 0, 0, 12, 2, 0, 0, 0, 0x00, 0x53, 0x7f, 0x45])
            .await;

        let condition = server.read_close().await;
        assert_eq!(condition.as_deref(), Some(condition::DECODE_ERROR));
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let state = tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap();
    assert!(
        !state.is_usable(),
        "a frame that does not decode ends the connection, got {state:?}"
    );
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_valid_keepalive_is_still_accepted() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;

        // An empty AMQP frame on channel 0: size 8, doff 2, type 0.
        server.write(&[0, 0, 0, 8, 2, 0, 0, 0]).await;

        // Then the client's own close, answered as usual: the keep-alive did
        // not end the connection.
        server.expect_close().await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(QUIET).await;
    assert!(
        connection.state().is_usable(),
        "an empty frame is a keep-alive, not a violation: {:?}",
        connection.state()
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}
