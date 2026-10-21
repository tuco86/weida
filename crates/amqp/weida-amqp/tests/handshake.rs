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

#[path = "support/mod.rs"]
pub(crate) mod support;

use support::{Server, options};
#[cfg(feature = "tls")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
use weida_amqp::{Connection, Error};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Close, Open, Performative};
#[cfg(feature = "tls")]
use weida_amqp_codec::protocol_header;
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_amqp_codec::sasl::SaslFrame;
use weida_amqp_codec::{Limits, Multiple};
use weida_runtime::Exec;

impl Server {
    pub(crate) async fn send_header(&mut self, header: ProtocolHeader) {
        self.write(&header.encode()).await;
    }

    pub(crate) async fn send_open(&mut self, idle_time_out: Option<u32>) {
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

    pub(crate) async fn send_sasl(&mut self, body: SaslFrame<'_>) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Sasl, 0, MIN_MAX_FRAME_SIZE, |buf| {
            body.encode(buf)
        })
        .unwrap();
        self.write(&out).await;
    }

    pub(crate) async fn send_close(&mut self, error: Option<weida_amqp_codec::AmqpError<'_>>) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Close(Close { error }).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }
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

/// The same scripted server, one layer up: everything after the `%d2`
/// exchange happens inside the TLS session.
#[cfg(feature = "tls")]
pub(crate) struct TlsServer {
    pub(crate) stream: tokio_rustls::server::TlsStream<TcpStream>,
    pub(crate) buf: Vec<u8>,
    pub(crate) from: usize,
}

#[cfg(feature = "tls")]
impl TlsServer {
    pub(crate) async fn fill(&mut self, want: usize) {
        while self.buf.len() - self.from < want {
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed early");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    pub(crate) async fn read_header(&mut self) -> ProtocolHeader {
        self.fill(protocol_header::LEN).await;
        let header = protocol_header::decode(&self.buf[self.from..]).unwrap();
        self.from += protocol_header::LEN;
        header
    }

    pub(crate) async fn read_performative(&mut self) -> String {
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

    pub(crate) async fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    pub(crate) async fn send_header(&mut self, header: ProtocolHeader) {
        self.write(&header.encode()).await;
    }

    pub(crate) async fn send_open(&mut self) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(Open::new("broker-1")).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    pub(crate) async fn send_close(&mut self) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Close(Close { error: None }).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }
}
