//! What a dropped future, a hostile `flow`, a backlog of the application's own
//! frames and the last `Connection` going out of scope each do to a link.
//!
//! One scripted server per test, with the half the test is about written by
//! hand. Every await is bounded.

#[path = "support/mod.rs"]
pub(crate) mod support;

use std::time::Duration;

use support::{DEADLINE, Server, options, timeout};
use weida_amqp::{Connection, ConnectionOptions, Error, Source, Target};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{
    Attach, Begin, Close, Flow, Open, Performative, Transfer as TransferFrame,
};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::{Limits, Role};
use weida_runtime::Exec;

/// A window in transfer frames that no test here comes near.
pub(crate) const LARGE_WINDOW: u32 = 100_000;

pub(crate) const MIB: usize = 1 << 20;

/// What a test looks at in a `transfer` the client wrote.
pub(crate) struct Seen {
    pub(crate) delivery_id: Option<u32>,
    pub(crate) more: bool,
    pub(crate) aborted: bool,
}

/// A session and link state in one `flow`, from the server's side.
pub(crate) fn flow(
    handle: Option<u32>,
    delivery_count: Option<u32>,
    link_credit: Option<u32>,
    available: Option<u32>,
    next_incoming_id: u32,
    incoming_window: u32,
    next_outgoing_id: u32,
) -> Flow<'static> {
    Flow {
        next_incoming_id: Some(next_incoming_id),
        incoming_window,
        next_outgoing_id,
        outgoing_window: 400,
        handle,
        delivery_count,
        link_credit,
        available,
        drain: false,
        echo: false,
        properties: None,
    }
}

impl Server {
    /// The header exchange and `open`, advertising `max_frame_size`.
    pub(crate) async fn handshake(&mut self, max_frame_size: u32) {
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

    pub(crate) async fn answer_begin(&mut self, incoming_window: u32) {
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "begin");
        let mut begin = Begin::new(0, incoming_window, 400);
        begin.remote_channel = Some(0);
        begin.handle_max = 255;
        self.send(4, Performative::Begin(begin)).await;
    }

    /// The answering `attach`, in the role opposite to the client's.
    pub(crate) async fn answer_attach(&mut self, name: &str, handle: u32, role: Role) {
        let (_, kind, _) = self.read_named().await;
        assert_eq!(kind, "attach");
        let source = Source::default();
        let target = Target::at("q");
        let mut attach = Attach::new(name, handle, role);
        attach.source = Some(source.to_value());
        attach.target = Some(target.to_value());
        if role == Role::Sender {
            attach.initial_delivery_count = Some(0);
        }
        self.send(4, Performative::Attach(attach)).await;
    }

    pub(crate) async fn send_flow(&mut self, flow: Flow<'static>) {
        self.send(4, Performative::Flow(flow)).await;
    }

    /// The next `transfer` the client wrote.
    pub(crate) async fn read_transfer(&mut self) -> Seen {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Transfer(transfer) => Seen {
                delivery_id: transfer.delivery_id,
                more: transfer.more,
                aborted: transfer.aborted,
            },
            other => panic!("expected transfer, got {}", other.name()),
        }
    }

    /// A `transfer` and its payload in one frame.
    pub(crate) async fn send_transfer(&mut self, transfer: TransferFrame<'_>, payload: &[u8]) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 4, u32::MAX, |body| {
            Performative::Transfer(transfer).encode(body)?;
            body.extend_from_slice(payload);
            Ok(())
        })
        .unwrap();
        self.write(&out).await;
    }

    /// Reads until the client's `close`, and answers it.
    pub(crate) async fn expect_close(&mut self) {
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

pub(crate) async fn connect(exec: &Exec, port: u16, options: ConnectionOptions) -> Connection {
    timeout(
        DEADLINE,
        Connection::connect(exec, "127.0.0.1", port, options),
    )
    .await
    .unwrap()
    .unwrap()
}

/// A deadline meant as "never" is refused where it is configured and not
/// discovered as a panic in the driver after the connection opened.
#[tokio::test]
async fn a_deadline_too_large_for_an_instant_is_refused_at_connect() {
    let exec = Exec::current().unwrap();
    for huge in [Duration::MAX, Duration::from_secs(u64::MAX / 4)] {
        let mut options = options();
        options.write_timeout = huge;
        // Nothing listens on port 1: a refusal that is not about the
        // configuration would be an `Io` error from the dial.
        let error = Connection::connect(&exec, "127.0.0.1", 1, options)
            .await
            .expect_err("refused");
        assert!(matches!(error, Error::Configuration(_)), "{error}");
    }
}
