//! PAIR: the exclusive pair, 31/EXPAIR.
//!
//! "PAIR is not a general-purpose socket but is intended for specific use
//! cases where the two peers are architecturally stable. This usually limits
//! PAIR to use within a single process, for inter-thread communication." At
//! most one peer, a double queue, block or error when the peer is
//! unavailable, never discard (`docs/research/zeromq.md` §4.5).
//!
//! **Two properties of the type, not settings**, and `zmq_socket(3)` states
//! them as the reason PAIR is "unsuitable for TCP in most cases":
//!
//! - **It does not auto-reconnect.** `ZMQ_RECONNECT_IVL` is forced off, the
//!   way the mute action is forced by every other socket type: a PAIR whose
//!   peer went away stays without one until the application acts, because
//!   the pattern's whole premise is that the two ends are stable and a
//!   reconnect would silently admit a *different* peer.
//! - **A further incoming connection is terminated while one is live.** That
//!   is `max_peers = 1` on the engine, which closes the excess connection
//!   where it is accepted — so a second connector observes an immediate
//!   close rather than a working socket that quietly interleaves two peers.
//!
//! A second `connect` is refused rather than accepted: one peer means one
//! endpoint, and a PAIR with two dialled endpoints has no defined behaviour
//! in 31/EXPAIR at all.

use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::MuteAction;
use crate::socket::{SocketCore, socket_endpoints};
use crate::split::{self, PairRecv, PairSend};

/// A PAIR socket: one peer, both directions, no reconnect.
#[derive(Debug)]
pub struct PairSocket {
    core: SocketCore,
}

impl PairSocket {
    /// A PAIR socket on `context`.
    pub fn new(context: &Context) -> Result<PairSocket> {
        PairSocket::with_options(context, SocketOptions::default())
    }

    /// A PAIR socket with `options`.
    ///
    /// `max_peers` and `ZMQ_RECONNECT_IVL` are the socket type's, not the
    /// caller's — see the module documentation for why — and the mute action
    /// is `zmq_socket(3)`'s row for PAIR: block.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<PairSocket> {
        options.max_peers = 1;
        options.reconnect_ivl = None;
        options.reconnect_ivl_max = None;
        options.pipe.outgoing.mute = MuteAction::Block;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(PairSocket {
            core: SocketCore::new(context, SocketType::Pair, options)?,
        })
    }

    /// Connects the one endpoint this socket may have.
    ///
    /// Fails with `EINVAL` on a second call: "at most one peer", and a PAIR
    /// dialling two endpoints is outside 31/EXPAIR.
    pub fn connect(&self, endpoint: &str) -> Result<()> {
        if !self.core.engine().connected().is_empty() {
            return Err(Error::EINVAL(
                "a PAIR socket has at most one peer, and this one is already connected".into(),
            ));
        }
        self.core.connect(endpoint)
    }

    /// Whether the one peer is **connected** right now.
    ///
    /// A dialled endpoint keeps its queue when its connection is lost —
    /// that is the engine's rule, and it is what lets a message be queued
    /// for a peer that has not arrived yet — so the question a PAIR
    /// application asks is about the connection rather than about the entry.
    pub fn has_peer(&self) -> bool {
        self.core.engine().peers().iter().any(|peer| peer.connected)
    }

    /// Sends a message to the peer, waiting for it to exist or to have room.
    ///
    /// Bounded by `ZMQ_SNDTIMEO`; never discards.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        split::send_never_dropping(&self.core, message.into()).await
    }

    /// The `ZMQ_DONTWAIT` form.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// Sends under an explicit wall-clock bound.
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        split::send_within(&self.core, message.into(), limit).await
    }

    /// Receives the next message from the peer, bounded by `ZMQ_RCVTIMEO`.
    pub async fn recv(&mut self) -> Result<Multipart> {
        split::recv_fair(&self.core).await
    }

    /// The `ZMQ_DONTWAIT` form.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        split::recv_fair_within(&self.core, limit).await
    }

    /// Splits the socket into a sending half and a receiving half, usable
    /// at the same time from two tasks.
    ///
    /// A PAIR is a double queue with no alternation between its directions,
    /// so the two halves are independent ([`crate::split`]).
    pub fn split(self) -> (PairSend, PairRecv) {
        (
            PairSend {
                core: self.core.clone(),
            },
            PairRecv { core: self.core },
        )
    }
}

socket_endpoints!(PairSocket, no_connect);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn text(message: &Multipart) -> String {
        String::from_utf8_lossy(message.frames()[0].as_slice()).into_owned()
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: the pair talks both ways with no envelope and no state — and
        /// only the two of them.
        #[tokio::test]
        async fn a_pair_talks_both_ways() {
            let ctx = context();
            let mut bound = PairSocket::new(&ctx).expect("pair");
            let endpoint = bound.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut dialled = PairSocket::new(&ctx).expect("pair");
            dialled.connect(&endpoint.to_string()).expect("connect");

            dialled.send("hello").await.expect("send");
            assert_eq!(text(&bound.recv().await.expect("recv")), "hello");
            bound.send("hello back").await.expect("send");
            assert_eq!(text(&dialled.recv().await.expect("recv")), "hello back");

            // Both directions again, unrestricted: PAIR has no alternation.
            dialled.send("one").await.expect("send");
            dialled.send("two").await.expect("send");
            assert_eq!(text(&bound.recv().await.expect("recv")), "one");
            assert_eq!(text(&bound.recv().await.expect("recv")), "two");
        }
        /// Claim: while one peer is live, a further incoming connection is
        /// **terminated** — `zmq_socket(3)`'s own warning, and the reason PAIR is
        /// unsuitable for TCP in most cases. The interloper reads EOF and the
        /// live pair keeps working.
        #[tokio::test]
        async fn a_second_incoming_connection_is_terminated() {
            let ctx = context();
            let mut bound = PairSocket::new(&ctx).expect("pair");
            let endpoint = bound.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut dialled = PairSocket::new(&ctx).expect("pair");
            dialled.connect(&endpoint.to_string()).expect("connect");
            dialled.send("mine").await.expect("send");
            assert_eq!(text(&bound.recv().await.expect("recv")), "mine");

            let addr: std::net::SocketAddr = endpoint.to_string()["tcp://".len()..]
                .parse()
                .expect("addr");
            let mut interloper = TcpStream::connect(addr).await.expect("connect");
            let mut byte = [0u8; 1];
            let read = tokio::time::timeout(Duration::from_secs(5), interloper.read(&mut byte))
                .await
                .expect("the second connection was not left hanging")
                .expect("read");
            assert_eq!(read, 0, "a second peer must be terminated, not served");

            // And the established pair is untouched.
            assert!(bound.has_peer());
            dialled.send("still here").await.expect("send");
            assert_eq!(text(&bound.recv().await.expect("recv")), "still here");
        }
        /// Claim: PAIR does not auto-reconnect. When the peer goes away the
        /// socket stays without one, and no further connect attempt is made —
        /// the property `zmq_socket(3)` names, and the one that keeps a PAIR from
        /// silently acquiring a *different* peer.
        #[tokio::test]
        async fn a_pair_does_not_auto_reconnect() {
            let ctx = context();
            let mut bound = PairSocket::new(&ctx).expect("pair");
            let endpoint = bound.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut dialled = PairSocket::new(&ctx).expect("pair");
            dialled.connect(&endpoint.to_string()).expect("connect");
            dialled.send("hello").await.expect("send");
            assert_eq!(text(&bound.recv().await.expect("recv")), "hello");
            assert_eq!(dialled.core.options().reconnect_ivl, None);

            // The bound end goes away entirely.
            bound.close();
            drop(bound);
            wait_for(|| !dialled.has_peer()).await;

            // One attempt was made and no more: a reconnecting socket would be
            // climbing here. Margin: 100 ms against a 100 ms default reconnect
            // interval, so a socket that did reconnect would have tried at least
            // once inside it.
            let attempts = dialled
                .core
                .engine()
                .peers()
                .first()
                .map(|peer| peer.attempts);
            tokio::time::sleep(Duration::from_millis(100)).await;
            let later = dialled
                .core
                .engine()
                .peers()
                .first()
                .map(|peer| peer.attempts);
            assert_eq!(attempts, later, "a PAIR must not try again");
        }
        /// Claim: one peer means one endpoint — a second `connect` is refused
        /// rather than quietly interleaving two peers, and a `disconnect` makes
        /// the socket usable again.
        #[tokio::test]
        async fn a_pair_has_at_most_one_endpoint() {
            let ctx = context();
            let listener = PairSocket::new(&ctx).expect("pair");
            let endpoint = listener.bind("tcp://127.0.0.1:0").await.expect("bind");
            let second = PairSocket::new(&ctx).expect("pair");
            let elsewhere = second.bind("tcp://127.0.0.1:0").await.expect("bind");

            let dialled = PairSocket::new(&ctx).expect("pair");
            dialled.connect(&endpoint.to_string()).expect("first");
            let err = dialled.connect(&elsewhere.to_string()).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");

            dialled
                .disconnect(&endpoint.to_string())
                .expect("disconnect");
            dialled
                .connect(&elsewhere.to_string())
                .expect("after disconnecting, one endpoint is free again");
        }
    }
}
