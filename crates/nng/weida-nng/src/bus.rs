//! BUS: one hop, best effort, and nothing else.
//!
//! "Each node sends to every *directly connected* peer; a mesh must
//! therefore be fully connected for all nodes to see a publication. Send
//! is best-effort, nonblocking, and discards when a peer cannot receive;
//! delivery may reach some, all, or none"
//! (`docs/research/nanomsg-nng.md` §4).
//!
//! **Fan-out is one hop, not flooding.** "A node must have a direct pipe to
//! receive a given send. Consequently, an application that expects every
//! participant to observe every BUS message must establish a fully
//! connected mesh itself" (§4). `tests/bus.rs` stands up a three-node line
//! and asserts the far node receives nothing, which is that sentence with a
//! failure mode.
//!
//! **A send never blocks and never fails for a peer that cannot take its
//! copy.** The copy is discarded "while the originating send succeeds
//! without blocking" (§4), and [`Broadcast`] is the only trace: SP has no
//! receipt, no refusal frame and no way to tell a sender anything (§6).
//!
//! **The ingress pipe, and why it is in the header.** "A raw BUS receive
//! carries the incoming pipe ID in its sole header element" and "a resend
//! excludes that pipe. This prevents immediate echo in the documented
//! single-socket device arrangement; it does not identify a message
//! globally or prevent a larger forwarding cycle" (§3, §4). A cooked BUS
//! message has no header on the wire, so what
//! [`BusSocket::recv`] puts there is **local metadata**: the pipe the
//! message came in on, in the same four big-endian octets the raw form
//! uses, so that [`BusSocket::send_excluding`] takes the same value either
//! way. Nothing of it is ever sent: a send builds its own message.

use std::sync::Arc;

use weida_sp::EndpointType;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::PipeId;
use crate::socket::{Broadcast, SocketCore, socket_endpoints};

/// A BUS socket: send to every direct peer, receive from every direct
/// peer.
#[derive(Clone, Debug)]
pub struct BusSocket {
    core: Arc<SocketCore>,
}

impl BusSocket {
    /// A BUS socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<BusSocket> {
        BusSocket::with_options(context, SocketOptions::default())
    }

    /// A BUS socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<BusSocket> {
        Ok(BusSocket {
            core: Arc::new(SocketCore::new(context, EndpointType::Bus, options)?),
        })
    }

    /// Sends `body` to every directly connected peer.
    ///
    /// Never waits. A peer whose queue is full loses its copy and the send
    /// still succeeds, which is what "best-effort, nonblocking, and
    /// discards when a peer cannot receive" means (§4). The returned
    /// [`Broadcast`] says how many copies were queued and how many were
    /// dropped, and that is the whole of what a BUS sender can ever learn.
    pub fn send(&self, body: impl Into<Vec<u8>>) -> Result<Broadcast> {
        let body: Vec<u8> = body.into();
        self.broadcast(&Message::from_body(body), None)
    }

    /// Sends `body` to every directly connected peer **except** `pipe`.
    ///
    /// The operation a re-broadcaster needs: "a raw BUS receive records its
    /// ingress pipe ID in the header, and a resend excludes that pipe. This
    /// prevents immediate echo in the documented single-socket device
    /// arrangement; it does not identify a message globally or prevent a
    /// larger forwarding cycle" (§4). The exclusion is one hop of loop
    /// control and is not a loop detector, which is why that sentence is
    /// quoted here rather than summarised.
    pub fn send_excluding(&self, pipe: PipeId, body: impl Into<Vec<u8>>) -> Result<Broadcast> {
        let body: Vec<u8> = body.into();
        self.broadcast(&Message::from_body(body), Some(pipe))
    }

    /// Re-broadcasts a message received by [`BusSocket::recv`], excluding
    /// the pipe it arrived on.
    ///
    /// Reads the ingress pipe out of the message's header, which is where
    /// this socket put it, so a forwarder does not have to carry the pipe
    /// id beside the message itself. `NNG_EPROTO` for a message with no
    /// such header — a message this socket did not receive.
    pub fn rebroadcast(&self, received: &Message) -> Result<Broadcast> {
        let Some(pipe) = ingress_of(received) else {
            return Err(Error::EPROTO(
                "this message carries no ingress pipe; only a message from BusSocket::recv \
                 can be re-broadcast"
                    .into(),
            ));
        };
        // The body is shared with the received message, not copied: the
        // header it carried is local metadata and is not re-sent.
        self.broadcast(
            &Message::from_body(received.body_bytes().clone()),
            Some(pipe),
        )
    }

    fn broadcast(&self, message: &Message, exclude: Option<PipeId>) -> Result<Broadcast> {
        self.core.ensure_open()?;
        Ok(self.core.send_to_all_except(message, exclude))
    }

    /// Receives the next message from any directly connected peer.
    ///
    /// The message's protocol header carries the pipe it arrived on, as
    /// four big-endian octets — local metadata in the same shape raw BUS
    /// puts on the wire (§3). [`ingress_of`] reads it back.
    pub async fn recv(&self) -> Result<Message> {
        let (pipe, message) = self.core.recv_any().await?;
        Ok(with_ingress(pipe, message))
    }

    /// The non-blocking form: `NNG_EAGAIN` when nothing has arrived.
    pub fn try_recv(&self) -> Result<Message> {
        let (pipe, message) = self.core.try_recv_any()?;
        Ok(with_ingress(pipe, message))
    }
}

socket_endpoints!(BusSocket);

/// Puts the ingress pipe id in a received message's protocol header.
fn with_ingress(pipe: PipeId, message: Message) -> Message {
    Message::from_parts(pipe.get().to_be_bytes(), message.into_body())
}

/// The pipe a message received by a BUS socket arrived on, or `None` for a
/// message that did not come from one.
pub fn ingress_of(message: &Message) -> Option<PipeId> {
    let header = message.header();
    if header.len() != 4 {
        return None;
    }
    Some(PipeId::new(u32::from_be_bytes([
        header[0], header[1], header[2], header[3],
    ])))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the ingress pipe survives a round trip through the header in
    /// the same four big-endian octets raw BUS uses on the wire, and a
    /// message that never came from a BUS socket has none.
    #[test]
    fn the_ingress_pipe_round_trips_through_the_header() {
        let tagged = with_ingress(PipeId::new(0x0A0B0C0D), Message::from_body(b"x".to_vec()));
        assert_eq!(tagged.header(), [0x0A, 0x0B, 0x0C, 0x0D]);
        assert_eq!(tagged.body(), b"x");
        assert_eq!(ingress_of(&tagged), Some(PipeId::new(0x0A0B0C0D)));
        assert_eq!(ingress_of(&Message::from_body(b"x".to_vec())), None);
    }
}
