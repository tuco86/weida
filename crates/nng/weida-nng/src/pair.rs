//! PAIR v0 and PAIR v1: the exclusive pair, with and without a hop count.
//!
//! "PAIR v0: a one-to-one peer relationship; it normally blocks when no
//! peer can receive. v0 has no protocol header and is the interoperable
//! legacy choice. PAIR v1: also one-to-one by default; it adds hop-count
//! loop protection across devices" (`docs/research/nanomsg-nng.md` §4).
//! Two protocols with two type ids on the wire — `0x0010` and `0x0011`
//! (§3) — so two socket types, [`Pair0Socket`] and [`Pair1Socket`], which
//! is also how NNG spells them (`nng_pair0_open`, `nng_pair1_open`).
//!
//! **One peer at a time, enforced where NNG enforces it.** "A peer rejects
//! another connection when already actively paired" (§4). That refusal
//! happens at the pipe-add-pre hook, before the pipe enters the socket,
//! and the peer is simply disconnected — SP has no frame that could tell
//! it why (§6).
//!
//! **The hop count, and the disagreement about where it starts.** PAIR v1
//! prefixes the body with one big-endian 32-bit word whose low octet is a
//! hop count. The RFC says the counter is "initialized to one and
//! incremented at each node"; a reading of NNG's source suggested it
//! originates `0` (§3). **Measured against a running NNG 1.4.0-rc.0, the
//! implementation sends one**, so the RFC and the implementation agree and
//! the source reading was wrong (§31). **This library sends one**, which
//! [`weida_sp::pair::INITIAL_HOPS`] holds and
//! [`the wire vector`](Pair1Socket::send) asserts, and it **accepts
//! both** readings on receipt, because the difference is a count and not a
//! format: a receiver compares it to its own `MAXTTL` and nothing else.
//!
//! **`NNG_OPT_MAXTTL` is local and is checked here.** "A forwarder checks
//! its own limit" (§4) and a message past it is "dropped on receipt, the
//! pipe kept, and nothing is sent back: the originator observes only the
//! absence of a reply until its own timeout" (§8). That is what
//! [`Pair1Socket::recv`] does, and [`Pair1Socket::dropped_over_ttl`] is
//! the only trace it leaves, because SP leaves none on the wire.
//!
//! **Polyamorous PAIR is absent, and the manual is the reason.**
//! `nng_pair1_open_poly()` permits several direct peers, addresses them by
//! pipe handle rather than by anything routable, "cannot route through
//! devices", and silently discards for a directed peer that is not there.
//! The manual's own words are that this "deprecated mode should not be
//! chosen for new designs" (§4), so it is not implemented; an application
//! that wants many peers over one socket wants BUS, which is the pattern
//! that means it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use weida_sp::{EndpointType, pair};

use crate::context::Context;
use crate::engine::{Admission, PipeEvent, PipeInfo};
use crate::error::Result;
use crate::message::Message;
use crate::options::SocketOptions;
use crate::socket::{SocketCore, socket_endpoints, within};

/// Installs the one-peer rule on a socket's pipe-event callback.
///
/// The count is the callback's own rather than the engine's, so that the
/// callback needs no handle on the socket it belongs to — which is the
/// rule NNG can only write down (§2) and this crate makes structural.
fn admit_one_peer_only(core: &SocketCore) -> Arc<AtomicUsize> {
    let paired = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&paired);
    core.engine()
        .notify(Arc::new(move |event, _info: &PipeInfo| match event {
            PipeEvent::AddPre => {
                // Claim the single slot here rather than at `AddPost`: two
                // connections arriving at once must not both be admitted,
                // and the window between the two events is exactly where
                // that would happen.
                if counter
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    Admission::Accept
                } else {
                    Admission::Reject(
                        "this PAIR socket is already paired; SP admits one peer at a time".into(),
                    )
                }
            }
            PipeEvent::RemPost => {
                counter.store(0, Ordering::SeqCst);
                Admission::Accept
            }
            PipeEvent::AddPost => Admission::Accept,
        }));
    paired
}

/// A PAIR v0 socket: one peer, no protocol header, the legacy
/// interoperable form.
///
/// "PAIR v0 contains no protocol header and is the legacy wire form
/// recommended when communicating with libnanomsg or mangos" (§4), so a
/// message is its body and nothing else.
#[derive(Clone, Debug)]
pub struct Pair0Socket {
    core: Arc<SocketCore>,
    _paired: Arc<AtomicUsize>,
}

impl Pair0Socket {
    /// A PAIR v0 socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<Pair0Socket> {
        Pair0Socket::with_options(context, SocketOptions::default())
    }

    /// A PAIR v0 socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<Pair0Socket> {
        let core = Arc::new(SocketCore::new(context, EndpointType::PairV0, options)?);
        let paired = admit_one_peer_only(&core);
        Ok(Pair0Socket {
            core,
            _paired: paired,
        })
    }

    /// Sends `body` to the paired peer.
    ///
    /// Waits when the peer cannot receive and when there is no peer at all
    /// — "PAIR normally blocks when no peer can receive" (§5) — bounded by
    /// `NNG_OPT_SENDTIMEO`, which turns the wait into `NNG_ETIMEDOUT`.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.core
            .send_round_robin(Message::from_body(body.into()))
            .await
            .map(|_| ())
    }

    /// The non-blocking form.
    pub fn try_send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.core
            .try_send_round_robin(Message::from_body(body.into()))
            .map(|_| ())
    }

    /// Receives the next message from the paired peer.
    pub async fn recv(&self) -> Result<Message> {
        self.core.recv_any().await.map(|(_, message)| message)
    }

    /// The non-blocking form.
    pub fn try_recv(&self) -> Result<Message> {
        self.core.try_recv_any().map(|(_, message)| message)
    }
}

socket_endpoints!(Pair0Socket);

/// A PAIR v1 socket: one peer, and a hop count in front of every body.
#[derive(Clone, Debug)]
pub struct Pair1Socket {
    core: Arc<SocketCore>,
    _paired: Arc<AtomicUsize>,
    over_ttl: Arc<AtomicU64>,
}

impl Pair1Socket {
    /// A PAIR v1 socket on `context`, with NNG's defaults — including
    /// `NNG_OPT_MAXTTL` of 8.
    pub fn new(context: &Context) -> Result<Pair1Socket> {
        Pair1Socket::with_options(context, SocketOptions::default())
    }

    /// A PAIR v1 socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<Pair1Socket> {
        let core = Arc::new(SocketCore::new(context, EndpointType::PairV1, options)?);
        let paired = admit_one_peer_only(&core);
        Ok(Pair1Socket {
            core,
            _paired: paired,
            over_ttl: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Sends `body` with a fresh hop count.
    ///
    /// The count this library originates is [`pair::INITIAL_HOPS`], which
    /// is **one**: the RFC's value, and the one a running NNG
    /// 1.4.0-rc.0 was measured to send (§3, §31). On the wire a message is
    /// therefore `00 00 00 01` followed by the body, which
    /// `tests/pair.rs` pins as a vector.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let message = Message::from_parts(pair::INITIAL_HOPS.to_be_bytes(), body.into());
        self.core.send_round_robin(message).await.map(|_| ())
    }

    /// The non-blocking form.
    pub fn try_send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let message = Message::from_parts(pair::INITIAL_HOPS.to_be_bytes(), body.into());
        self.core.try_send_round_robin(message).map(|_| ())
    }

    /// Receives the next message whose hop count is within
    /// `NNG_OPT_MAXTTL`.
    ///
    /// A message past the local ceiling is **dropped and the pipe kept**,
    /// with nothing sent back: "a ceiling reached at a forwarder is
    /// invisible to the peers on either side of it" (§8). The count is
    /// kept in the returned message's protocol header, so an application
    /// that wants to know how far a message travelled can read it.
    ///
    /// Both readings of the initial count are accepted — the RFC's one and
    /// NNG's zero — because the difference is a count and not a format.
    pub async fn recv(&self) -> Result<Message> {
        let max_hops = self.core.options().max_ttl as u32;
        // One timeout around the whole loop, and the untimed receive inside
        // it: a timeout per receive would restart after every message this
        // drops, and never expire while a peer keeps sending ones that are
        // past the ceiling.
        within(self.core.exec(), self.core.options().recv_timeout, async {
            loop {
                let (_, mut message) = self.core.recv_any_untimed().await?;
                let within_ttl = pair::decode(message.body(), max_hops).is_ok()
                    && message.split_header_off_body(pair::HEADER_LEN).is_ok();
                if within_ttl {
                    return Ok(message);
                }
                self.over_ttl.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
    }

    /// Messages dropped because their hop count was past this socket's
    /// `NNG_OPT_MAXTTL`, or because their PAIR v1 header was malformed.
    ///
    /// The only trace such a drop leaves anywhere: the sender is told
    /// nothing and the pipe stays up (§8).
    pub fn dropped_over_ttl(&self) -> u64 {
        self.over_ttl.load(Ordering::Relaxed)
    }

    /// `NNG_OPT_MAXTTL` for this socket.
    pub fn max_ttl(&self) -> usize {
        self.core.options().max_ttl
    }
}

socket_endpoints!(Pair1Socket);

/// Why polyamorous PAIR v1 is not here, as a value a caller can print
/// rather than a paragraph nobody reads.
///
/// `nng_pair1_open_poly()` exists in NNG and is deprecated by NNG: the
/// manual says the mode "should not be chosen for new designs" (§4). This
/// library implements what the manual recommends and names the omission
/// here so that a reader looking for it finds the reason rather than a
/// gap.
pub const POLYAMOROUS_ABSENT: &str = "polyamorous PAIR v1 (nng_pair1_open_poly) is absent: the NNG manual deprecates it — its \
     destination is a local pipe handle rather than a routable identity, a directed send to an \
     unavailable pipe discards silently, and it cannot route through devices. Use BUS for many \
     direct peers.";

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the count this library originates is NNG's zero, and the
    /// codec agrees with the socket about it — one number, in one place.
    #[test]
    fn the_originated_hop_count_is_the_measured_one() {
        assert_eq!(pair::INITIAL_HOPS, 1);
        assert_eq!(pair::INITIAL_HOPS.to_be_bytes(), [0, 0, 0, 1]);
    }

    /// Claim: the absence of polyamorous mode carries the manual's own
    /// reason, so a reader who looks for it is told why rather than left
    /// to guess.
    #[test]
    fn polyamorous_mode_is_absent_with_a_reason() {
        assert!(POLYAMOROUS_ABSENT.contains("deprecates"));
        assert!(POLYAMOROUS_ABSENT.contains("pair1_open_poly"));
    }
}
