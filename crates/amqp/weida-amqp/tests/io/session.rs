//! Sessions against a scripted server: the two channel numberings, `end`,
//! `DISCARDING`, and the two kinds of wrong channel.

#[path = "../support/mod.rs"]
mod support;

use support::{DEADLINE, Server, accept, options, scripted};
use weida_amqp::session::{SessionEvent, SessionOptions, SessionState};
use weida_amqp::{Condition, Connection, State};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Begin, Close, End, Open, Performative};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{AmqpError, Limits};
use weida_runtime::Exec;

impl Server {
    /// The header exchange and `open`, with `channel-max` on our side.
    async fn handshake(&mut self, channel_max: u16) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (channel, name, _) = self.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "open"));
        let mut open = Open::new("broker-1");
        open.channel_max = channel_max;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    /// The answering `begin`, deliberately on a *different* channel from the
    /// client's.
    async fn answer_begin(&mut self, ours: u16, theirs: u16) {
        let mut begin = Begin::new(0, 400, 400);
        begin.remote_channel = Some(theirs);
        self.send(ours, Performative::Begin(begin)).await;
    }
}

#[tokio::test]
async fn a_session_is_begun_on_the_lowest_free_channel_and_answered_on_another() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(15).await;

        // The first session takes channel 0, the second channel 1: lowest
        // free, as Part 2 §2.5.1 recommends.
        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "begin"));
        // Answer on our own channel 4. The two directions are numbered
        // independently and usually differ, which is the whole point.
        server.answer_begin(4, 0).await;

        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (1, "begin"));
        server.answer_begin(9, 1).await;

        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "end"));
        server.send(4, Performative::End(End { error: None })).await;

        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "close"));
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();

    let first = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .expect("the first session");
    assert_eq!(first.outgoing_channel(), 0);
    assert_eq!(
        first.incoming_channel(),
        Some(4),
        "the peer answered on its own channel, not on ours"
    );
    assert_eq!(first.state(), SessionState::Begun);

    let second = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .expect("the second session");
    assert_eq!(second.outgoing_channel(), 1, "lowest free");
    assert_eq!(second.incoming_channel(), Some(9));

    // The peer's begin opened the window, which a fresh session does not
    // have: until the partner's begin there is no permission to send.
    let windows = first.windows();
    assert!(windows.seen_remote_begin());
    assert_eq!(windows.remote_incoming_window, 400);
    assert!(first.may_send());

    let mut first = first;
    tokio::time::timeout(DEADLINE, first.end())
        .await
        .unwrap()
        .unwrap();
    match tokio::time::timeout(DEADLINE, first.next_event())
        .await
        .unwrap()
    {
        Some(SessionEvent::Ended(None)) => {}
        other => panic!("expected Ended(None), got {other:?}"),
    }
    assert_eq!(first.state(), SessionState::Ended(None));

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
async fn an_end_carrying_an_error_discards_input_until_the_partners_end() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(15).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;

        // The client ends with an error and then enters DISCARDING.
        let (channel, bytes) = server.read_frame().await;
        assert_eq!(channel, 0);
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::End(End { error: Some(error) }) => {
                assert_eq!(error.condition, condition::DECODE_ERROR);
            }
            other => panic!("expected end with an error, got {}", other.name()),
        }

        // Keep talking. Everything here must be discarded rather than acted
        // upon: a `flow` that moved the window would be acting on a session
        // whose state is by definition no longer trustworthy.
        let mut flow = weida_amqp_codec::performative::Flow::session(1, 0, 1);
        flow.next_incoming_id = Some(0);
        server.send(4, Performative::Flow(flow)).await;
        server.send(4, Performative::End(End { error: None })).await;

        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "close"));
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let mut session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let before = session.windows();

    let condition =
        Condition::described(condition::DECODE_ERROR, "a list header we could not read");
    tokio::time::timeout(DEADLINE, session.end_with(Some(condition.clone())))
        .await
        .unwrap()
        .unwrap();
    assert!(
        session.state().is_discarding(),
        "an errored end enters DISCARDING immediately"
    );

    // The partner's `end` is the one frame DISCARDING does act on.
    match tokio::time::timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
    {
        Some(SessionEvent::Ended(None)) => {}
        other => panic!("expected Ended, got {other:?}"),
    }
    assert_eq!(
        session.windows().remote_incoming_window,
        before.remote_incoming_window,
        "the flow that arrived while discarding changed nothing"
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

#[tokio::test]
async fn a_frame_on_an_in_range_but_unmapped_channel_closes_the_connection() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(15).await;
        // Channel 3 is inside the client's channel-max and no session was
        // ever begun on it. The specification names no condition for this
        // case, so the client's is amqp:not-allowed and it says which
        // channel.
        server.send(3, Performative::End(End { error: None })).await;
        let (channel, bytes) = server.read_frame().await;
        assert_eq!(channel, 0, "close is on channel 0");
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Close(Close { error: Some(error) }) => {
                assert_eq!(error.condition, condition::NOT_ALLOWED);
                let description = error.description.unwrap();
                assert!(description.contains("channel 3"), "{description}");
            }
            other => panic!("expected close with an error, got {}", other.name()),
        }
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
        State::Closed(Some(condition)) => {
            assert_eq!(condition.condition, condition::NOT_ALLOWED);
        }
        other => panic!("expected Closed with a condition, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_channel_above_our_channel_max_is_a_framing_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(15).await;
        // Above what the client advertised. Part 2 §2.7.1 names this one:
        // "MUST close the connection with amqp:connection:framing-error".
        server.send(9, Performative::End(End { error: None })).await;
        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Close(Close { error: Some(error) }) => {
                assert_eq!(error.condition, condition::CONNECTION_FRAMING_ERROR);
            }
            other => panic!("expected close with an error, got {}", other.name()),
        }
    });

    let mut options = options();
    options.channel_max = 3;
    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .unwrap()
    .unwrap();
    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Closed(Some(condition)) => {
            assert_eq!(condition.condition, condition::CONNECTION_FRAMING_ERROR);
        }
        other => panic!("expected Closed, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_peers_channel_max_bounds_how_many_sessions_we_may_begin() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        // channel-max 0 admits exactly one session, on channel 0.
        server.handshake(0).await;
        let (channel, name, _) = server.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "begin"));
        server.answer_begin(0, 0).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close", "the second begin never reached the wire");
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(connection.remote().channel_max, 0);
    let _first = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .expect("the one session the peer admits");

    let error = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .expect_err("no channel left");
    assert!(
        error.to_string().contains("channel-max"),
        "the refusal names the bound: {error}"
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

#[tokio::test]
async fn a_connection_close_ends_every_session_on_it() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(15).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        // The connection goes, without ending the session first. Part 2
        // §2.5.2: "sessions also end automatically when the connection
        // closes or is interrupted".
        server
            .send(
                0,
                Performative::Close(Close {
                    error: Some(AmqpError::new(condition::CONNECTION_FORCED)),
                }),
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close");
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let mut session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    match tokio::time::timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
    {
        Some(SessionEvent::Ended(Some(condition))) => {
            assert_eq!(condition.condition, condition::CONNECTION_FORCED);
        }
        other => panic!("expected Ended with the connection's condition, got {other:?}"),
    }
    assert!(!session.state().is_usable());
    assert!(
        !session.may_send(),
        "a session whose connection went cannot send"
    );
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}
