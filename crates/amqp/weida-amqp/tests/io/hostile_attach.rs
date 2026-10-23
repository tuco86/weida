//! Attach and begin frames a peer uses to take over state it does not own,
//! against a scripted server.
//!
//! An `attach` that names a link already answered is a takeover and steals the
//! link; one whose handle is above the `handle-max` this end advertised is a
//! framing error; and a `begin` for a session that has begun, or that this end
//! ended with an error, is illegal state. Every one of these must leave the
//! client's tables as they were.

#[path = "../support/mod.rs"]
mod support;

use support::{DEADLINE, Server, accept, options, scripted, timeout};
use weida_amqp::link::{LinkEvent, LinkOptions};
use weida_amqp::session::{SessionOptions, SessionState};
use weida_amqp::{Condition, Connection, Source, State, Target};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Attach, Begin, Close, Open, Performative};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Role};
use weida_runtime::Exec;

/// What a test looks at in an `attach` the client wrote, owned.
struct AttachSeen {
    /// The channel the `attach` was written on: the session's own.
    channel: u16,
    name: String,
    handle: u32,
    role: Role,
    termini_null: bool,
}

/// What a test looks at in a `detach` the client wrote, owned.
struct DetachSeen {
    handle: u32,
    closed: bool,
    condition: Option<String>,
}

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
        open.max_frame_size = u32::MAX;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn answer_begin(&mut self, ours: u16, theirs: u16) {
        let mut begin = Begin::new(0, 400, 400);
        begin.remote_channel = Some(theirs);
        begin.handle_max = 255;
        self.send(ours, Performative::Begin(begin)).await;
    }

    /// An `attach` from the server with both termini named, as a real
    /// sending link would be answered.
    async fn answer_attach(&mut self, channel: u16, name: &str, handle: u32, role: Role) {
        let source = Source::default();
        let target = Target::at("q");
        let mut attach = Attach::new(name, handle, role);
        attach.source = Some(source.to_value());
        attach.target = Some(target.to_value());
        if role == Role::Sender {
            attach.initial_delivery_count = Some(0);
        }
        self.send(channel, Performative::Attach(attach)).await;
    }

    /// The next `attach` the client writes.
    async fn read_attach(&mut self) -> AttachSeen {
        let (channel, name, bytes) = self.read_named().await;
        assert_eq!(name, "attach");
        match decode_performative(&bytes) {
            Performative::Attach(attach) => AttachSeen {
                channel,
                name: attach.name.to_owned(),
                handle: attach.handle,
                role: attach.role,
                termini_null: attach.source.is_none() && attach.target.is_none(),
            },
            _ => unreachable!("the name said attach"),
        }
    }

    /// The next `detach` the client writes.
    async fn read_detach(&mut self) -> DetachSeen {
        let (_, name, bytes) = self.read_named().await;
        assert_eq!(name, "detach");
        match decode_performative(&bytes) {
            Performative::Detach(detach) => DetachSeen {
                handle: detach.handle,
                closed: detach.closed,
                condition: detach.error.map(|e| e.condition.to_owned()),
            },
            _ => unreachable!("the name said detach"),
        }
    }

    /// The next `end` the client writes, with its condition if it gave one.
    async fn read_end(&mut self) -> Option<String> {
        let (channel, name, bytes) = self.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "end"));
        match decode_performative(&bytes) {
            Performative::End(end) => end.error.map(|e| e.condition.to_owned()),
            _ => unreachable!("the name said end"),
        }
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
async fn a_second_attach_for_an_answered_link_steals_it_instead_of_remapping() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;

        let first = server.read_attach().await;
        assert_eq!((first.name.as_str(), first.handle), ("invoices", 0));
        assert_eq!(first.role, Role::Receiver);
        server.answer_attach(4, "invoices", 0, Role::Sender).await;

        // The same name in the same role as the answer, on a fresh handle:
        // the peer taking the link over.
        server.answer_attach(4, "invoices", 1, Role::Sender).await;
        let stolen = server.read_detach().await;
        assert_eq!(stolen.handle, 0, "the incumbent's handle");
        assert!(
            stolen.closed,
            "a stolen link is destroyed, not merely unmapped"
        );
        assert_eq!(stolen.condition.as_deref(), Some(condition::LINK_STOLEN));

        // The newcomer is answered as an unsolicited attach: nulled termini,
        // then a detach for its handle. It is never mapped.
        let answered = server.read_attach().await;
        assert_eq!(
            answered.channel, 0,
            "the session's own channel, not the peer's"
        );
        assert_eq!((answered.name.as_str(), answered.handle), ("invoices", 1));
        assert!(answered.termini_null);
        assert_eq!(answered.role, Role::Receiver);
        let detached = server.read_detach().await;
        assert_eq!(detached.handle, 1);
        assert!(detached.closed);
        assert!(detached.condition.is_none());

        // Repeating the takeover on fresh handles changes nothing: each one is
        // answered the same way, and the session stays begun.
        for handle in [2, 3] {
            server
                .answer_attach(4, "invoices", handle, Role::Sender)
                .await;
            let answered = server.read_attach().await;
            assert_eq!(answered.handle, handle);
            assert_eq!(
                answered.channel, 0,
                "the session's own channel, not the peer's"
            );
            assert!(answered.termini_null);
            let detached = server.read_detach().await;
            assert_eq!(detached.handle, handle);
            assert!(detached.closed);
            assert!(detached.condition.is_none());
        }
        // The client closes only after this, so every answer above has been
        // read before the connection goes away.
        let _ = done_tx.send(());
        server.expect_close().await;
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session_options = SessionOptions {
        handle_max: 3,
        ..SessionOptions::default()
    };
    let session = timeout(DEADLINE, connection.begin(session_options))
        .await
        .unwrap()
        .unwrap();
    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    match timeout(DEADLINE, receiver.next_event()).await.unwrap() {
        Some(LinkEvent::Detached(Some(detached))) => {
            assert_eq!(detached.condition, condition::LINK_STOLEN);
        }
        other => panic!("expected Detached with amqp:link:stolen, got {other:?}"),
    }
    assert!(!receiver.state().is_usable());
    assert!(
        session.state().is_usable(),
        "the takeover is of the link, not the session"
    );
    // Closing before the server has read the answers would race them.
    timeout(DEADLINE, done_rx).await.unwrap().unwrap();

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn an_attach_handle_above_the_handle_max_we_advertised_closes_the_connection() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;

        let first = server.read_attach().await;
        assert_eq!(first.name, "invoices");
        // We advertised a handle-max of 3, so the handle 4 is out of range.
        server.answer_attach(4, "invoices", 4, Role::Sender).await;

        let condition = server.read_close().await;
        assert_eq!(
            condition.as_deref(),
            Some(condition::CONNECTION_FRAMING_ERROR)
        );
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session_options = SessionOptions {
        handle_max: 3,
        ..SessionOptions::default()
    };
    let session = timeout(DEADLINE, connection.begin(session_options))
        .await
        .unwrap()
        .unwrap();

    let attached = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("invoices", Source::at("q"))),
    )
    .await
    .unwrap();
    assert!(
        attached.is_err(),
        "the attach that was waiting on the answer is not completed"
    );

    match timeout(DEADLINE, connection.closed()).await.unwrap() {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::CONNECTION_FRAMING_ERROR);
        }
        other => panic!("expected Closed with framing-error, got {other:?}"),
    }
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_second_answering_begin_is_illegal_state() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;
    // The second answer waits until the test has seen the session begin:
    // sent at once, it can reach the client before `begin` returns, and the
    // session is then already ended when its state is read.
    let (begun_tx, begun_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;
        begun_rx.await.expect("the test saw the session begin");

        // A second answer for the same session, on another incoming channel.
        let mut again = Begin::new(0, 400, 400);
        again.remote_channel = Some(0);
        again.handle_max = 255;
        server.send(5, Performative::Begin(again)).await;

        let condition = server.read_close().await;
        assert_eq!(condition.as_deref(), Some(condition::ILLEGAL_STATE));
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.state(), SessionState::Begun);
    begun_tx.send(()).expect("the server is waiting");

    match timeout(DEADLINE, connection.closed()).await.unwrap() {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::ILLEGAL_STATE);
        }
        other => panic!("expected Closed with illegal-state, got {other:?}"),
    }
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_begin_cannot_resurrect_a_session_the_client_ended_with_an_error() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0).await;

        // The client ended the session with an error, and has said so.
        let ended_with = server.read_end().await;
        assert_eq!(ended_with.as_deref(), Some(condition::DECODE_ERROR));

        // Now a begin that names the client's channel as its remote channel,
        // on a channel that has never been used.
        let mut resurrect = Begin::new(0, 400, 400);
        resurrect.remote_channel = Some(0);
        resurrect.handle_max = 255;
        server.send(5, Performative::Begin(resurrect)).await;

        let condition = server.read_close().await;
        assert_eq!(condition.as_deref(), Some(condition::ILLEGAL_STATE));
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    timeout(
        DEADLINE,
        session.end_with(Some(Condition::new(condition::DECODE_ERROR))),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(session.state().is_discarding());

    match timeout(DEADLINE, connection.closed()).await.unwrap() {
        State::Closed(Some(closed)) => {
            assert_eq!(closed.condition, condition::ILLEGAL_STATE);
        }
        other => panic!("expected Closed with illegal-state, got {other:?}"),
    }
    assert_ne!(
        session.state(),
        SessionState::Begun,
        "a session ended with an error is not begun again"
    );
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
