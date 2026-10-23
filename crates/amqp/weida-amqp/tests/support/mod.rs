//! The scripted server the integration tests play against.
//!
//! A `TcpListener` in the same process, with the server half written by hand,
//! because that is the only way to assert what the *client* put on the wire
//! and in what order. A real peer (`tests/interop_fe2o3.rs`,
//! `tests/interop_rabbitmq.rs`) proves interoperability; these tests prove the
//! sequence.
//!
//! Every await in every test that uses this goes through [`timeout`], which
//! never fails a slow run: it reports a step that takes longer than
//! [`DEADLINE`] and keeps waiting (see `weida-testkit`).
//!
//! Each test file is its own crate and includes this module, so a helper that
//! one file does not use is dead code in that crate: it carries its own
//! `#[allow(dead_code)]`. What only some files need, namely the performatives
//! each one scripts, stays in that file as an `impl Server` block of its own.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use weida_amqp::ConnectionOptions;
use weida_amqp_codec::Limits;
use weida_amqp_codec::frame::{self, FrameKind};
use weida_amqp_codec::performative::Performative;
use weida_amqp_codec::protocol_header::{self, ProtocolHeader};

/// Every await in a test is bounded by this.
pub const DEADLINE: Duration = Duration::from_secs(5);

/// A bounded wait that cannot fail a test for being slow. It has the shape of
/// `tokio::time::timeout(..)`, so the call sites keep their `.await.unwrap()`,
/// but the error is never produced: past `expected` the step is reported by
/// `weida_testkit::within`, naming the calling line, and the wait goes on.
#[allow(dead_code)]
#[track_caller]
pub fn timeout<F: Future>(
    expected: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, std::convert::Infallible>> {
    let at = std::panic::Location::caller();
    async move {
        let what = format!("the step at {}:{}", at.file(), at.line());
        Ok(weida_testkit::within(&what, expected, future).await)
    }
}

/// The server half of a scripted exchange.
pub struct Server {
    pub stream: TcpStream,
    buf: Vec<u8>,
    from: usize,
}

impl Server {
    async fn fill(&mut self, want: usize) {
        while self.buf.len() - self.from < want {
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed before sending {want} octets");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    pub async fn read_header(&mut self) -> ProtocolHeader {
        self.fill(protocol_header::LEN).await;
        let header = protocol_header::decode(&self.buf[self.from..]).unwrap();
        self.from += protocol_header::LEN;
        header
    }

    pub async fn read_frame(&mut self) -> (u16, Vec<u8>) {
        self.fill(8).await;
        let header = frame::decode_header(&self.buf[self.from..], u32::MAX).unwrap();
        let size = header.size as usize;
        self.fill(size).await;
        let bytes = self.buf[self.from..self.from + size].to_vec();
        self.from += size;
        (header.channel, bytes)
    }

    /// The next frame, decoded far enough to match on: its channel, the name
    /// of its performative (`"empty"` for a keep-alive) and its bytes.
    pub async fn read_named(&mut self) -> (u16, String, Vec<u8>) {
        let (channel, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        if frame.is_empty() {
            return (channel, "empty".to_owned(), bytes);
        }
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        (channel, performative.name().to_owned(), bytes)
    }

    pub async fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    #[allow(dead_code, reason = "handshake.rs sends through its own helpers")]
    pub async fn send(&mut self, channel: u16, performative: Performative<'_>) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, channel, u32::MAX, |body| {
            performative.encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }
}

pub async fn scripted() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

pub async fn accept(listener: &TcpListener) -> Server {
    let (stream, _) = listener.accept().await.unwrap();
    Server {
        stream,
        buf: Vec::new(),
        from: 0,
    }
}

/// Test options: no keep-alive unless a test asks for one, and a handshake
/// deadline an hour long so that it cannot fire in a test whose claim is not
/// its expiry (a test that is about it sets it itself). The close budget stays
/// short: with a peer that has stopped reading, it is the only thing that ends
/// a close, so those tests need it to expire.
pub fn options() -> ConnectionOptions {
    let mut options = ConnectionOptions::new("client-1");
    options.idle_time_out = None;
    options.handshake_timeout = Duration::from_secs(3600);
    options.close_budget = Duration::from_secs(2);
    options
}
