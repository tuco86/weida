//! A silent peer does not keep a socket open once the connection has failed.
//!
//! Counted through `/proc/self/fd`, which is Linux only, and this file holds
//! that one test so that no other test in the binary opens or closes a socket
//! while the count is taken.

#![cfg(target_os = "linux")]

mod support;

use std::time::Duration;

use support::{DEADLINE, Server, accept, options, scripted};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};
use weida_amqp::{Connection, State};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Open, Performative};
use weida_amqp_codec::protocol_header::ProtocolHeader;
use weida_runtime::Exec;

/// The number of entries in this process's descriptor table.
fn open_descriptors() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

impl Server {
    /// The header exchange and `open`, and nothing after it.
    async fn handshake(&mut self) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (channel, name, _) = self.read_named().await;
        assert_eq!((channel, name.as_str()), (0, "open"));
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }
}

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    use super::*;

    #[tokio::test]
    async fn a_silent_peer_does_not_keep_the_socket_open_after_the_idle_expiry() {
        let exec = Exec::current().unwrap();
        let (listener, port) = scripted().await;
        let baseline = open_descriptors();

        let (release, released) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut server = accept(&listener).await;
            server.handshake().await;
            // Silent from here on: the socket stays open and unread until the
            // test releases it.
            let _ = released.await;
            drop(server);
        });

        let mut options = options();
        options.idle_time_out = Some(Duration::from_millis(200));
        let connection = timeout(
            DEADLINE,
            Connection::connect(&exec, "127.0.0.1", port, options),
        )
        .await
        .unwrap()
        .unwrap();

        let state = timeout(DEADLINE, connection.closed()).await.unwrap();
        match &state {
            State::Failed(text) => assert!(text.contains("idle threshold"), "{text}"),
            other => panic!("expected Failed on idle expiry, got {other:?}"),
        }

        release.send(()).unwrap();
        timeout(DEADLINE, server).await.unwrap().unwrap();

        // The connection handle is still alive here: what must be gone is the
        // driver's transport, not the caller's handle.
        let started = Instant::now();
        loop {
            if open_descriptors() <= baseline {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "descriptors did not return to {baseline}: {} open",
                open_descriptors()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(connection);
    }
}
