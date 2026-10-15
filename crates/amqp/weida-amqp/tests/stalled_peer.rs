//! A peer that stops reading, and what the connection does about it.
//!
//! Findings covered: a session-level `flow` nobody reads must not wedge the
//! connection (2a); a settled sender whose flows nobody reads keeps sending
//! (2b); a full link queue does not stop the connection ending (2c); a peer
//! that stops reading fails the connection by the stall clock, not by the
//! idle timer, and `close` is bounded while the peer is stalled (4).

mod support;

use std::time::Duration;

use support::{DEADLINE, Server, accept, options, scripted};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use weida_amqp::link::LinkOptions;
use weida_amqp::session::{Session, SessionEvent, SessionOptions};
use weida_amqp::{Connection, ConnectionOptions, Link, Source, State, Target};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Message;
use weida_amqp_codec::performative::{
    Attach, Begin, Close, Disposition, End, Flow, Open, Performative,
};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::state::DeliveryState;
use weida_amqp_codec::{Limits, Role, SenderSettleMode};
use weida_runtime::Exec;

/// The `max-frame-size` the stalled and slow peers advertise: small enough
/// that a megabyte is several frames.
const PEER_MAX_FRAME_SIZE: u32 = 128 * 1024;

/// A window in transfer frames that no test here comes near.
const LARGE_WINDOW: u32 = 100_000;

const MIB: usize = 1 << 20;

impl Server {
    /// The header exchange and `open`, advertising `max_frame_size`.
    async fn handshake(&mut self, max_frame_size: u32) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (channel, name, _) = self.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "open"));
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        open.max_frame_size = max_frame_size;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn answer_begin(&mut self, ours: u16, theirs: u16, incoming_window: u32) {
        let mut begin = Begin::new(0, incoming_window, 400);
        begin.remote_channel = Some(theirs);
        begin.handle_max = 255;
        self.send(ours, Performative::Begin(begin)).await;
    }

    /// The answering `attach`, in the role opposite to the client's.
    async fn answer_attach(
        &mut self,
        channel: u16,
        name: &str,
        handle: u32,
        role: Role,
        initial_delivery_count: u32,
    ) {
        let source = Source::default();
        let target = Target::at("q");
        let mut attach = Attach::new(name, handle, role);
        attach.source = Some(source.to_value());
        attach.target = Some(target.to_value());
        if role == Role::Sender {
            attach.initial_delivery_count = Some(initial_delivery_count);
        }
        self.send(channel, Performative::Attach(attach)).await;
    }

    /// A `flow` with the given link fields (`None` makes it session-level)
    /// and a session window of `incoming_window`.
    async fn send_flow(
        &mut self,
        channel: u16,
        handle: Option<u32>,
        delivery_count: Option<u32>,
        link_credit: Option<u32>,
        incoming_window: u32,
    ) {
        let flow = Flow {
            next_incoming_id: Some(0),
            incoming_window,
            next_outgoing_id: 0,
            outgoing_window: 400,
            handle,
            delivery_count,
            link_credit,
            available: Some(0),
            drain: false,
            echo: false,
            properties: None,
        };
        self.send(channel, Performative::Flow(flow)).await;
    }

    /// A settled-or-not `accepted` disposition for one delivery of ours.
    async fn send_accepted(&mut self, channel: u16, first: u32, settled: bool) {
        let mut disposition = Disposition::new(Role::Receiver, first);
        disposition.settled = settled;
        disposition.state = Some(DeliveryState::Accepted.to_value());
        self.send(channel, Performative::Disposition(disposition))
            .await;
    }

    /// The next `transfer`: whether more frames of its message follow, and
    /// the payload that came after the performative.
    async fn read_transfer(&mut self) -> (bool, Vec<u8>) {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, used) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Transfer(transfer) => (transfer.more, frame.body[used..].to_vec()),
            other => panic!("expected transfer, got {}", other.name()),
        }
    }

    /// Reads and discards frames until the client's `close`.
    async fn read_until_close(&mut self) {
        loop {
            let (_, name, _) = self.read_named().await;
            if name == "close" {
                break;
            }
        }
    }

    /// Reads until the client's `close`, then answers it.
    async fn expect_close(&mut self) {
        self.read_until_close().await;
        self.send(0, Performative::Close(Close { error: None }))
            .await;
    }
}

/// A connection whose sender has been granted credit by a peer that then
/// never reads again.
struct Stalled {
    connection: Connection,
    _session: Session,
    sender: Link,
    /// Dropping the server's socket is what the test does last.
    release: oneshot::Sender<()>,
    server: JoinHandle<()>,
}

async fn stalled(options: ConnectionOptions) -> Stalled {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;
    let (release, released) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(PEER_MAX_FRAME_SIZE).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, LARGE_WINDOW).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
        server
            .send_flow(4, Some(11), Some(0), Some(1000), LARGE_WINDOW)
            .await;
        // The socket stays open and unread until the test lets go of it.
        let _ = released.await;
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    Stalled {
        connection,
        _session: session,
        sender,
        release,
        server,
    }
}

/// Sends `payload` until the connection refuses it. Each send is bounded, so
/// a sender that is never refused ends the task rather than the test.
async fn flood(sender: Link, payload: Vec<u8>) {
    while let Ok(Ok(_)) = timeout(DEADLINE, sender.send_payload(&payload)).await {}
}

/// The state once it has left `Closing`. `closed()` returns at once while the
/// driver is still ending the connection, so this waits that out.
async fn settled_state(connection: &Connection) -> State {
    timeout(DEADLINE, async {
        loop {
            let state = connection.state();
            if !matches!(state, State::Closing) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the connection left Closing")
}

#[tokio::test]
async fn session_level_flows_nobody_reads_do_not_wedge_the_connection() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, LARGE_WINDOW).await;
        for _ in 0..200 {
            server.send_flow(4, None, None, None, LARGE_WINDOW).await;
        }
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
        server.read_until_close().await;
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let _session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    let state = timeout(DEADLINE, connection.closed()).await.unwrap();
    assert!(matches!(state, State::Closed(_)), "{state:?}");
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn session_level_flows_then_the_clients_close_completes() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, LARGE_WINDOW).await;
        for _ in 0..200 {
            server.send_flow(4, None, None, None, LARGE_WINDOW).await;
        }
        server.expect_close().await;
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let _session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    // Long enough for the flood to reach the driver before `close` queues.
    tokio::time::sleep(Duration::from_millis(200)).await;

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled_state(&connection).await, State::Closed(None));
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_settled_sender_whose_flows_nobody_reads_keeps_sending() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, LARGE_WINDOW).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
        // Every flow grants plenty of credit, so the sends are limited by
        // nothing but the connection itself.
        for _ in 0..100 {
            server
                .send_flow(4, Some(11), Some(0), Some(1000), LARGE_WINDOW)
                .await;
        }
        for _ in 0..200 {
            let (more, _) = server.read_transfer().await;
            assert!(!more, "each message is one frame");
        }
        server.expect_close().await;
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
    let mut link = LinkOptions::sender("orders", Target::at("q"));
    link.snd_settle_mode = SenderSettleMode::Settled;
    let sender = timeout(DEADLINE, session.attach(link))
        .await
        .unwrap()
        .unwrap();

    for i in 0..200 {
        let body = format!("m-{i}");
        let sent = timeout(DEADLINE, sender.send(&Message::data(body.as_bytes())))
            .await
            .unwrap_or_else(|_| panic!("send {i} completed"))
            .unwrap();
        assert!(sent.settled, "a settled sender settles what it sends");
        assert_eq!(sent.frames, 1);
    }

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_full_link_queue_does_not_stop_the_connection_ending() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(u32::MAX).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, LARGE_WINDOW).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
        server
            .send_flow(4, Some(11), Some(0), Some(1), LARGE_WINDOW)
            .await;
        let (more, _) = server.read_transfer().await;
        assert!(!more);
        // Nobody on the client reads the link, so its queue overflows into
        // merged reports while the frames keep coming.
        for _ in 0..150 {
            server
                .send_flow(4, Some(11), Some(1), Some(1000), LARGE_WINDOW)
                .await;
            server.send_accepted(4, 0, false).await;
        }
        server.send(4, Performative::End(End { error: None })).await;
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
        server.read_until_close().await;
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let mut session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(DEADLINE, sender.send(&Message::data(b"order-1")))
        .await
        .unwrap()
        .unwrap();

    let state = timeout(DEADLINE, connection.closed()).await.unwrap();
    assert!(matches!(state, State::Closed(_)), "{state:?}");
    match timeout(DEADLINE, session.next_event()).await.unwrap() {
        Some(SessionEvent::Ended(_)) => {}
        other => panic!("expected the session to end, got {other:?}"),
    }
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_peer_that_stops_reading_fails_the_connection_with_a_write_timeout() {
    let mut options = options();
    options.write_timeout = Duration::from_millis(300);
    let Stalled {
        connection,
        _session,
        sender,
        release,
        server,
    } = stalled(options).await;
    let flooding = tokio::spawn(flood(sender, vec![0x5a; MIB]));

    let state = timeout(Duration::from_secs(5), connection.closed())
        .await
        .unwrap();
    match state {
        State::Failed(text) => assert!(text.contains("stopped reading"), "{text}"),
        other => panic!("expected Failed for a stalled peer, got {other:?}"),
    }
    timeout(DEADLINE, flooding).await.unwrap().unwrap();
    let _ = release.send(());
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn the_idle_timeout_still_fires_while_the_peer_has_stopped_reading() {
    let mut options = options();
    options.write_timeout = Duration::from_secs(30);
    options.idle_time_out = Some(Duration::from_millis(300));
    let Stalled {
        connection,
        _session,
        sender,
        release,
        server,
    } = stalled(options).await;
    let flooding = tokio::spawn(flood(sender, vec![0x5a; MIB]));

    let state = timeout(Duration::from_secs(5), connection.closed())
        .await
        .unwrap();
    match state {
        State::Failed(text) => assert!(text.contains("idle threshold"), "{text}"),
        other => panic!("expected Failed on idle expiry, got {other:?}"),
    }
    timeout(DEADLINE, flooding).await.unwrap().unwrap();
    let _ = release.send(());
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn close_returns_and_ends_the_connection_while_the_peer_has_stopped_reading() {
    let mut options = options();
    options.write_timeout = Duration::from_secs(30);
    options.close_budget = Duration::from_millis(500);
    let Stalled {
        connection,
        _session,
        sender,
        release,
        server,
    } = stalled(options).await;
    let flooding = tokio::spawn(flood(sender, vec![0x5a; MIB]));
    // Let the flood fill what the socket will take, so that close really
    // waits behind queued frames.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "close is bounded by its budget, took {:?}",
        started.elapsed()
    );

    let state = settled_state(&connection).await;
    assert_eq!(state, State::Closed(None));
    timeout(DEADLINE, flooding).await.unwrap().unwrap();
    let _ = release.send(());
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_slow_reader_is_not_a_stalled_one() {
    const MESSAGES: usize = 16;
    const MESSAGE: usize = 256 * 1024;

    let mut options = options();
    options.write_timeout = Duration::from_secs(1);
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake(PEER_MAX_FRAME_SIZE).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, LARGE_WINDOW).await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "attach");
        server
            .answer_attach(4, "orders", 11, Role::Receiver, 0)
            .await;
        server
            .send_flow(4, Some(11), Some(0), Some(1000), LARGE_WINDOW)
            .await;

        // Reads a frame at a time with a pause after each, so the socket
        // takes octets steadily but well below what the client offers.
        let mut messages = 0;
        let mut octets = 0;
        while messages < MESSAGES {
            let (more, payload) = server.read_transfer().await;
            octets += payload.len();
            if !more {
                messages += 1;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(octets, MESSAGES * MESSAGE, "every octet arrived");
        server.expect_close().await;
    });

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .unwrap();

    let chunk = vec![0x33u8; MESSAGE];
    for _ in 0..MESSAGES {
        timeout(DEADLINE, sender.send_payload(&chunk))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(connection.state(), State::Open, "a slow reader stays open");
    }

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
