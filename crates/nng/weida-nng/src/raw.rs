//! Raw sockets and the device: the wire, with the protocol taken out.
//!
//! "Cooked sockets enforce the pattern state machines and headers; raw
//! constructors bypass them, leaving send/receive and header semantics to
//! the application. `nng_device()` requires raw sockets but just forwards
//! messages" (`docs/research/nanomsg-nng.md` §4).
//!
//! **What a raw socket keeps and what it gives up.** It keeps the
//! handshake, the pairing rule, the framing, the pipes and every bound —
//! those are the transport, not the pattern. It gives up the state machine
//! (`NNG_ESTATE` has nothing to be in state *for*), the retry, the
//! matching, the local filtering and the header manipulation: a raw
//! receive hands over the payload exactly as it arrived, tag stack and hop
//! count included, and a raw send writes exactly what it is given.
//!
//! **Contexts are refused, and the reason is the definition.** "Raw
//! sockets cannot use contexts: raw mode makes the application responsible
//! for the omitted protocol state" (§4). An `nng_ctx` *is* per-transaction
//! protocol state, so on a socket that deliberately holds none there is
//! nothing for one to hold — which is why [`RawSocket::context`] returns
//! [`Infallible`]: the type says there is no
//! context to be had, and the error says why.
//!
//! **The device is two raw sockets and a routing rule.** "Protocol header
//! designs such as REQ/REP backtraces and PAIR v1 TTL supply the routing
//! or loop information that a forwarding topology needs" (§4). [`device`]
//! is that rule and nothing else: it adds no processing, no queueing
//! discipline and no retry, and what it does with a header depends only on
//! which protocol the socket it arrived on speaks.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use weida_sp::{EndpointType, backtrace, pair};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::{PipeId, Sent};
use crate::socket::{Broadcast, SocketCore, socket_endpoints};

/// A raw socket of any SP protocol: the wire, with the pattern's state
/// machine absent.
#[derive(Clone, Debug)]
pub struct RawSocket {
    core: Arc<SocketCore>,
}

impl RawSocket {
    /// A raw socket speaking `protocol`, with NNG's defaults.
    ///
    /// This is `nng_<protocol>_open_raw()`, and every SP protocol has one:
    /// raw mode is a property of the socket rather than of the protocol
    /// (§4).
    pub fn open(context: &Context, protocol: EndpointType) -> Result<RawSocket> {
        RawSocket::with_options(context, protocol, SocketOptions::default())
    }

    /// A raw socket with `options`.
    ///
    /// The buffer options are refused for the same protocols a cooked
    /// socket refuses them on, because that refusal is about the protocol's
    /// transaction model and not about the state machine raw mode drops.
    pub fn with_options(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
    ) -> Result<RawSocket> {
        Ok(RawSocket {
            core: Arc::new(SocketCore::new(context, protocol, options)?),
        })
    }

    /// Refused: a raw socket has no per-transaction state for a context to
    /// hold (§4).
    ///
    /// The return type is [`Infallible`], so the
    /// absence is in the signature rather than only in the error message.
    pub fn context(&self) -> Result<Infallible> {
        Err(Error::ENOTSUP(
            format!(
                "a raw {} socket holds no protocol state, so a context would hold nothing; \
                 raw mode makes the application responsible for what a context would have kept",
                crate::protocol::protocol(self.core.protocol()).name
            )
            .into(),
        ))
    }

    /// Sends `message` exactly as given — header then body, with nothing
    /// added and nothing checked — to the next pipe that can accept it.
    pub async fn send(&self, message: Message) -> Result<PipeId> {
        self.core.send_round_robin(message).await
    }

    /// Sends `message` to one named pipe, which is how a reverse route is
    /// followed.
    pub async fn send_to(&self, pipe: PipeId, message: Message) -> Result<()> {
        self.core.send_to(pipe, message).await.map(|_| ())
    }

    /// Offers `message` to every pipe, which is what a raw PUB, BUS or
    /// SURVEYOR send is.
    pub fn send_to_all(&self, message: &Message) -> Broadcast {
        self.core.send_to_all_except(message, None)
    }

    /// Offers `message` to every pipe except `pipe` — a raw BUS
    /// re-broadcast, which "excludes the incoming pipe ID … preventing
    /// immediate reflection in a one-socket device" (§4).
    pub fn send_to_all_excluding(&self, pipe: PipeId, message: &Message) -> Broadcast {
        self.core.send_to_all_except(message, Some(pipe))
    }

    /// Receives the next message from any pipe, with its wire payload
    /// untouched.
    ///
    /// The returned message's body is exactly the octets that followed the
    /// length field — tag stack, hop count, pipe id and all — because
    /// deciding what any of them mean is what raw mode hands back to the
    /// application. The pipe it arrived on comes beside it, which is what
    /// a raw BUS would otherwise read out of its header.
    pub async fn recv(&self) -> Result<(PipeId, Message)> {
        self.core.recv_any().await
    }

    /// The non-blocking form.
    pub fn try_recv(&self) -> Result<(PipeId, Message)> {
        self.core.try_recv_any()
    }

    /// The protocol this socket speaks.
    pub fn raw_protocol(&self) -> EndpointType {
        self.core.protocol()
    }
}

socket_endpoints!(RawSocket);

/// What a forwarder does with a message, decided by the protocol of the
/// socket it arrived on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Routing {
    /// Push the ingress pipe id onto the front of the tag stack and send
    /// the message onward. "A device prepends its local peer ID on request
    /// reception" (§4).
    Push,
    /// Pop the front of the tag stack and use it to select the pipe the
    /// message goes back out on. "Each forwarder pops its ID to select the
    /// reverse pipe" (§4).
    Pop,
    /// Increment the PAIR v1 hop count and drop the message past
    /// `NNG_OPT_MAXTTL`. "A forwarder's local `MAXTTL` policy bounds
    /// forwarding" (§4).
    Hop,
    /// Forward the payload unchanged: the protocol carries no routing
    /// information at all.
    Plain,
}

impl Routing {
    const fn of(protocol: EndpointType) -> Routing {
        match protocol {
            // The sides that receive a request or a survey are the sides
            // that push: the stack has to grow on the way in so that it can
            // shrink on the way back.
            EndpointType::Rep | EndpointType::Respondent => Routing::Push,
            // The sides that receive an answer pop what they pushed.
            EndpointType::Req | EndpointType::Surveyor => Routing::Pop,
            EndpointType::PairV1 => Routing::Hop,
            _ => Routing::Plain,
        }
    }
}

/// Whether a destination protocol delivers to every peer or to one.
const fn broadcasts(protocol: EndpointType) -> bool {
    matches!(
        protocol,
        EndpointType::Pub | EndpointType::Bus | EndpointType::Surveyor
    )
}

/// What one forwarded message did, for a caller that wants to count.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Forwarded {
    /// Messages handed to the other socket.
    pub forwarded: u64,
    /// Messages dropped: a hop count past `NNG_OPT_MAXTTL`, a malformed
    /// tag stack, a reverse route whose pipe has gone, or a peer that
    /// could not take the copy. Every one of them is silent on the wire
    /// (§6, §8).
    pub dropped: u64,
}

/// `nng_device()`: forwards messages between two raw sockets, in both
/// directions, until either is closed.
///
/// The two sockets must speak compatible protocols — the pairing rule of
/// the SP registry — which is the same rule a pipe is admitted under. A
/// device between a raw REP socket (which REQ clients dial) and a raw REQ
/// socket (which dials REP servers) is the load-balancing intermediary the
/// sheet describes (§9).
///
/// **It adds nothing.** No retry, no matching, no state machine and no
/// application processing: "`nng_device()` operates on raw sockets and
/// forwards messages without adding application processing" (§4). What it
/// does do is the header bookkeeping the topology needs: push and pop the
/// tag stack for request-reply and survey, increment and bound the PAIR v1
/// hop count, and exclude the ingress pipe when a raw BUS re-broadcasts
/// through one socket.
///
/// **The two directions run independently**, so that nothing a peer does
/// can stall the other side of the device: a request waits for a backend
/// with room and only requests wait, a reply is discarded when the client
/// it is for has stopped reading, and neither direction is served ahead of
/// the other.
///
/// Fails with `NNG_ENOTSUP` when the two protocols cannot pair, because a
/// device between incompatible sockets would forward messages nobody can
/// interpret.
pub async fn device(first: &RawSocket, second: &RawSocket) -> Result<Forwarded> {
    let left = first.raw_protocol();
    let right = second.raw_protocol();
    if left.peer() != right {
        return Err(Error::ENOTSUP(
            format!(
                "a device needs two raw sockets that can pair: {left:?} pairs with {:?}, \
                 not with {right:?}",
                left.peer()
            )
            .into(),
        ));
    }

    // **Each direction is a loop of its own**, as NNG runs them. A request
    // that finds no backend to take it waits, and a reply to a client that
    // has stopped reading is discarded rather than waited for, but whatever
    // one direction waits for, the other keeps going: one slow client or
    // one full backend must not stop the replies of everybody else, and a
    // stream of requests must not starve the replies that would make room
    // for them.
    let (there, back) = tokio::join!(pump(first, second), pump(second, first));
    Ok(Forwarded {
        forwarded: there.forwarded + back.forwarded,
        dropped: there.dropped + back.dropped,
    })
}

/// Whether either of the device's sockets has been closed, which ends the
/// device in both directions.
fn either_closed(first: &RawSocket, second: &RawSocket) -> bool {
    first.core.engine().is_closed() || second.core.engine().is_closed()
}

/// Waits until either socket is closed.
async fn closed(first: &RawSocket, second: &RawSocket) {
    loop {
        let (one, other) = (
            first.core.engine().changed(),
            second.core.engine().changed(),
        );
        if either_closed(first, second) {
            return;
        }
        tokio::select! {
            () = one => {}
            () = other => {}
        }
    }
}

/// One direction of a device: what arrives on `from` is forwarded to `to`,
/// until either socket is closed.
async fn pump(from: &RawSocket, to: &RawSocket) -> Forwarded {
    let mut tally = Forwarded::default();
    loop {
        // Created before the look, so that an arrival, or a close of either
        // socket, between the look and the await ends the wait.
        let arrived = from.core.message_signal();
        let (one, other) = (from.core.engine().changed(), to.core.engine().changed());
        if either_closed(from, to) {
            return tally;
        }
        let Some((ingress, message)) = from.core.try_take_any() else {
            tokio::select! {
                () = arrived => {}
                () = one => {}
                () = other => {}
            }
            continue;
        };
        let outcome = tokio::select! {
            outcome = forward(from, to, ingress, message) => outcome,
            () = closed(from, to) => return tally,
        };
        match outcome {
            Ok(Forward::Sent) => tally.forwarded += 1,
            Ok(Forward::Dropped) | Err(_) => tally.dropped += 1,
        }
    }
}

/// What became of one forwarded message that was not an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Forward {
    /// The other socket took it.
    Sent,
    /// The other socket discarded it at a full queue, which is the answer a
    /// reply to a pipe that cannot take it gets.
    Dropped,
}

/// One message across the device.
///
/// The body is never copied: what is forwarded is a slice of the octets
/// that arrived, with a header of at most a few tags in front of it.
async fn forward(
    from: &RawSocket,
    to: &RawSocket,
    ingress: PipeId,
    message: Message,
) -> Result<Forward> {
    let max_hops = from.core.options().max_ttl;
    let payload: Bytes = message.into_body();
    let malformed = |why: weida_sp::TagError| Error::EPROTO(why.to_string().into());
    let onward = match Routing::of(from.raw_protocol()) {
        Routing::Push => {
            let shape = backtrace::measure(&payload, max_hops).map_err(malformed)?;
            // The payload is judged against the limit of the pipe it came
            // in on before it goes anywhere: the transport only admitted
            // it by its declared length with the stack in it, and a message
            // that is over the limit must be lost here, to its own sender,
            // and not forwarded to a backend whose pipe to this device is
            // every client's.
            if !from.core.payload_fits(ingress, payload.len() - shape.len) {
                return Err(Error::EMSGSIZE(
                    "the payload is over NNG_OPT_RECVMAXSZ once the tag stack is taken off".into(),
                ));
            }
            if shape.peers >= max_hops {
                // The next hop bounds the stack by the same `MAXTTL`, and
                // it already counts this device's tag against it: a
                // message forwarded from here would be one the next hop
                // has to refuse. Dropped here, where only this message is
                // lost, and not forwarded to be refused there, where the
                // pipe it arrives on is shared by every client of this
                // device.
                return Err(Error::EPROTO(
                    format!(
                        "a tag stack of {} peers has no room for this device's under MAXTTL \
                         {max_hops}",
                        shape.peers
                    )
                    .into(),
                ));
            }
            // The forwarder's own peer id goes in front of whatever is
            // already there, so the outermost hop is popped first (§4):
            // pushing a tag is putting four octets before the stack, which
            // is otherwise left as it arrived.
            let stack = payload.get(..shape.len).unwrap_or_default();
            let mut header = Vec::with_capacity(backtrace::TAG_LEN + stack.len());
            header.extend_from_slice(&(ingress.get() & backtrace::MAX_ID).to_be_bytes());
            header.extend_from_slice(stack);
            Message::from_parts(header, payload.slice(shape.len..))
        }
        Routing::Pop => {
            let shape = backtrace::measure(&payload, max_hops).map_err(malformed)?;
            if !from.core.payload_fits(ingress, payload.len() - shape.len) {
                return Err(Error::EMSGSIZE(
                    "the payload is over NNG_OPT_RECVMAXSZ once the tag stack is taken off".into(),
                ));
            }
            let stack = payload.get(..shape.len).unwrap_or_default();
            let (Some(first), true) = (
                stack.first_chunk::<{ backtrace::TAG_LEN }>(),
                shape.peers > 0,
            ) else {
                // Nothing left to pop: this answer was not routed through
                // here, and there is nowhere to send it.
                return Err(Error::EPROTO(
                    "a reply reached this device with an empty peer stack".into(),
                ));
            };
            let reverse = PipeId::new(u32::from_be_bytes(*first));
            // Popping a tag is dropping the first four octets of the
            // stack: what remains is the stack the requester's own side
            // pushed, byte for byte.
            let popped = Message::from_parts(
                stack.get(backtrace::TAG_LEN..).unwrap_or_default(),
                payload.slice(shape.len..),
            );
            return match to.core.send_to(reverse, popped).await? {
                Sent::Queued => Ok(Forward::Sent),
                Sent::Dropped => Ok(Forward::Dropped),
            };
        }
        Routing::Hop => {
            let (hops, _) = pair::decode(&payload, max_hops as u32).map_err(malformed)?;
            let next = pair::next_hop(hops);
            if next as usize > max_hops {
                // "Reply past the local hop ceiling: dropped on receipt,
                // the pipe kept, and nothing is sent back" (§8).
                return Err(Error::EPROTO(
                    format!("hop count {next} is past this device's MAXTTL of {max_hops}").into(),
                ));
            }
            Message::from_parts(next.to_be_bytes(), payload.slice(pair::HEADER_LEN..))
        }
        Routing::Plain => Message::from_body(payload),
    };

    if broadcasts(to.raw_protocol()) {
        // A single-socket device re-broadcasting a BUS message must not
        // reflect it to the pipe it came from (§4). The two handles are one
        // socket when they share a core, whichever clone of it each is.
        let broadcast = if Arc::ptr_eq(&from.core, &to.core) {
            to.send_to_all_excluding(ingress, &onward)
        } else {
            to.send_to_all(&onward)
        };
        if broadcast.queued == 0 && broadcast.dropped > 0 {
            return Err(Error::ETIMEDOUT("no peer took the forwarded copy".into()));
        }
        return Ok(Forward::Sent);
    }
    to.send(onward).await.map(|_| Forward::Sent)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the routing rule is read off the protocol, so a device does
    /// not have to be told which of its two sockets faces the requesters.
    #[test]
    fn the_routing_rule_comes_from_the_protocol() {
        assert_eq!(Routing::of(EndpointType::Rep), Routing::Push);
        assert_eq!(Routing::of(EndpointType::Respondent), Routing::Push);
        assert_eq!(Routing::of(EndpointType::Req), Routing::Pop);
        assert_eq!(Routing::of(EndpointType::Surveyor), Routing::Pop);
        assert_eq!(Routing::of(EndpointType::PairV1), Routing::Hop);
        assert_eq!(Routing::of(EndpointType::PairV0), Routing::Plain);
        assert_eq!(Routing::of(EndpointType::Push), Routing::Plain);
        assert!(broadcasts(EndpointType::Pub));
        assert!(broadcasts(EndpointType::Bus));
        assert!(!broadcasts(EndpointType::Push));
    }
}
