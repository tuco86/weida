//! The scripted server the integration tests play against.
//!
//! A `TcpListener` in the same process, with the server half written by hand,
//! because that is the only way to assert what the *client* put on the wire
//! and in what order. A real `nats-server` (`tests/interop_nats_server.rs`)
//! proves interoperability; these tests prove the sequence.
//!
//! Every await in every test that uses this is bounded by [`DEADLINE`], so a
//! wrong turn fails the test rather than hanging the suite.

// Each integration test binary uses part of this module. A helper that is
// unused in one binary is not dead code in the crate.
#![allow(dead_code)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use weida_nats::Connection;
use weida_nats::options::ConnectionOptions;
use weida_nats_codec::{Headers, Limits, Op};
use weida_runtime::Exec;

/// Every await in a test is bounded by this.
pub const DEADLINE: Duration = Duration::from_secs(5);

/// How long a test waits to establish that the client has written *nothing*.
///
/// Short, because it is paid on every test that asserts silence, and a client
/// that writes when it should not writes immediately rather than late.
pub const SILENCE: Duration = Duration::from_millis(200);

/// The server half of a scripted exchange.
pub struct Server {
    pub stream: TcpStream,
    buf: Vec<u8>,
    from: usize,
}

impl Server {
    pub async fn listen() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    /// A listener whose accepted sockets have a **small receive buffer**, so
    /// that a peer that stops reading stalls the sender after a few kilobytes
    /// instead of after the several megabytes loopback would otherwise absorb.
    /// The tests that need a stall use it, so that they pay milliseconds.
    pub async fn listen_small_window() -> (TcpListener, u16) {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(16).unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    pub async fn accept(listener: &TcpListener) -> Self {
        let (stream, _) = listener.accept().await.unwrap();
        Self {
            stream,
            buf: Vec::new(),
            from: 0,
        }
    }

    /// Asserts that the client has sent nothing, which is what "the server
    /// speaks first" means in octets.
    pub async fn expect_silence(&mut self) {
        let mut probe = [0u8; 256];
        match tokio::time::timeout(SILENCE, self.stream.read(&mut probe)).await {
            Err(_elapsed) => {}
            Ok(Ok(0)) => panic!("the client closed instead of staying quiet"),
            Ok(Ok(n)) => panic!(
                "the client wrote {:?} when it should have written nothing",
                String::from_utf8_lossy(&probe[..n])
            ),
            Ok(Err(error)) => panic!("the client's socket failed: {error}"),
        }
    }

    /// Asserts the client has ended the transport, which is the whole of a
    /// NATS close: there is no `CLOSE` verb.
    pub async fn expect_end_of_stream(&mut self) {
        let mut probe = [0u8; 256];
        let read = tokio::time::timeout(DEADLINE, self.stream.read(&mut probe))
            .await
            .expect("the client closed within the deadline")
            .expect("a clean read");
        assert_eq!(
            read,
            0,
            "expected the transport to end, got {:?}",
            String::from_utf8_lossy(&probe[..read])
        );
    }

    /// One whole operation, rendered flat, decoded with the same codec the
    /// client encodes with.
    pub async fn read_op(&mut self) -> String {
        loop {
            if self.buf.len() > self.from {
                match Op::decode(&self.buf[self.from..], Limits::DEFAULT) {
                    Ok((op, used)) => {
                        let rendered = render(&op);
                        self.from += used;
                        return rendered;
                    }
                    Err(error) if !error.is_violation() => {}
                    Err(error) => panic!("the client wrote something unreadable: {error}"),
                }
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed mid-operation");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    /// The next `n` operations, in order.
    pub async fn read_ops(&mut self, n: usize) -> Vec<String> {
        let mut ops = Vec::with_capacity(n);
        for _ in 0..n {
            ops.push(self.read_op().await);
        }
        ops
    }

    pub async fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    pub async fn send_info(&mut self, json: &str) {
        self.write(format!("INFO {json}\r\n").as_bytes()).await;
    }

    /// `MSG <subject> <sid> [reply-to] <#bytes>`.
    pub async fn send_msg(
        &mut self,
        subject: &str,
        sid: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) {
        let mut out = Vec::new();
        Op::Msg {
            subject: subject.as_bytes(),
            sid: sid.as_bytes(),
            reply_to: reply_to.map(str::as_bytes),
            payload,
        }
        .encode(&mut out)
        .unwrap();
        self.write(&out).await;
    }

    /// `HMSG` with one header entry, for the dispatch tests.
    pub async fn send_hmsg(
        &mut self,
        subject: &str,
        sid: &str,
        name: &str,
        value: &str,
        payload: &[u8],
    ) {
        let mut headers = Headers::new();
        headers.push(name, value);
        let mut out = Vec::new();
        Op::Hmsg {
            subject: subject.as_bytes(),
            sid: sid.as_bytes(),
            reply_to: None,
            headers,
            payload,
        }
        .encode(&mut out)
        .unwrap();
        self.write(&out).await;
    }

    /// `HMSG` with a `NATS/1.0 <status>` block and no entries, which is the
    /// shape of a status message — the no-responder answer above all.
    pub async fn send_status(&mut self, subject: &str, sid: &str, status: u16) {
        let mut headers = Headers::new();
        headers.status = Some(status);
        let mut out = Vec::new();
        Op::Hmsg {
            subject: subject.as_bytes(),
            sid: sid.as_bytes(),
            reply_to: None,
            headers,
            payload: b"",
        }
        .encode(&mut out)
        .unwrap();
        self.write(&out).await;
    }

    /// The opening exchange every test needs: `INFO`, then the `CONNECT` and
    /// the `PING` that confirms it. Returns the `CONNECT` line. It does not
    /// first wait out [`SILENCE`]: the tests of the server speaking first
    /// assert that themselves, and every other test would pay it per
    /// connection.
    pub async fn handshake(&mut self, json: &str) -> String {
        self.send_info(json).await;
        let connect = self.read_op().await;
        assert!(
            connect.starts_with("CONNECT"),
            "the answer to INFO is CONNECT, got {connect}"
        );
        assert_eq!(
            self.read_op().await,
            "PING",
            "the CONNECT is confirmed by a PING"
        );
        self.write(b"PONG\r\n").await;
        connect
    }

    /// An `INFO` that offers everything this client can claim: headers, so
    /// `no_responders` is negotiable, and protocol level 1.
    pub async fn handshake_full(&mut self) -> String {
        self.handshake(
            "{\"server_id\":\"S1\",\"version\":\"2.14.0\",\"proto\":1,\
              \"max_payload\":1048576,\"headers\":true}",
        )
        .await
    }
}

/// An operation as a flat, assertable string.
///
/// `-` stands for an absent optional argument, which is the thing the
/// protocol's argument-count rule turns on: `PUB a 5` and `PUB a b 5` differ
/// only in how many arguments follow the verb.
pub fn render(op: &Op<'_>) -> String {
    match op {
        Op::Connect(_) => {
            let mut out = Vec::new();
            op.encode(&mut out).unwrap();
            String::from_utf8_lossy(&out).trim_end().to_owned()
        }
        Op::Ping => "PING".to_owned(),
        Op::Pong => "PONG".to_owned(),
        Op::Pub {
            subject,
            reply_to,
            payload,
        } => format!(
            "PUB {} {} {}",
            text(subject),
            optional(*reply_to),
            text(payload)
        ),
        Op::Hpub {
            subject,
            reply_to,
            headers,
            payload,
        } => format!(
            "HPUB {} {} {} {}",
            text(subject),
            optional(*reply_to),
            headers
                .status
                .map_or_else(|| "-".to_owned(), |status| status.to_string()),
            text(payload)
        ),
        Op::Sub {
            subject,
            queue_group,
            sid,
        } => format!(
            "SUB {} {} {}",
            text(subject),
            optional(*queue_group),
            text(sid)
        ),
        Op::Unsub { sid, max_msgs } => format!(
            "UNSUB {} {}",
            text(sid),
            max_msgs.map_or_else(|| "-".to_owned(), |n| n.to_string())
        ),
        other => other.verb().to_owned(),
    }
}

fn text(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).into_owned()
}

fn optional(raw: Option<&[u8]>) -> String {
    raw.map_or_else(|| "-".to_owned(), text)
}

/// Test options: short deadlines, and a ping interval long enough that no
/// keep-alive fires unless a test asks for one.
pub fn options() -> ConnectionOptions {
    let mut options = ConnectionOptions::new();
    options.handshake_timeout = Duration::from_secs(2);
    options.ping_interval = Duration::from_secs(60);
    options
}

/// An `INFO` that offers headers and nothing that needs a reaction.
pub const PLAIN_INFO: &str =
    "{\"server_id\":\"S1\",\"proto\":1,\"max_payload\":1048576,\"headers\":true}";

/// A connection whose handshake is done, and the scripted server on its far
/// end. `listener` is where the connection dials: [`Server::listen`] or
/// [`Server::listen_small_window`].
pub async fn connected(
    (listener, port): (TcpListener, u16),
    info: &str,
    options: ConnectionOptions,
) -> (Connection, Server) {
    let exec = Exec::current().unwrap();
    let info = info.to_owned();
    let server = tokio::spawn(async move {
        let mut server = Server::accept(&listener).await;
        server.handshake(&info).await;
        server
    });
    let nats = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options),
    )
    .await
    .expect("the handshake finished")
    .expect("the connection opened");
    let server = tokio::time::timeout(DEADLINE, server)
        .await
        .expect("the server script finished")
        .expect("the server script did not panic");
    (nats, server)
}
