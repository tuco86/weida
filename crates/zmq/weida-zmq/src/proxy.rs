//! The devices: `zmq_proxy` and `zmq_proxy_steerable`.
//!
//! "For many-to-many use-cases the pattern provides raw socket types (XPUB,
//! XSUB) to construct distribution proxies, also called brokers"
//! (`docs/research/zeromq.md` §4.3). A proxy is the smallest of them: read a
//! message on one socket, write it on the other, and hand a copy to a
//! capture socket if there is one. The zguide builds the forwarder, the
//! streamer, the queue device and Espresso out of exactly that.
//!
//! # What differs from `zmq_proxy`'s C signature
//!
//! `int zmq_proxy (void *frontend, void *backend, void *capture)` takes three
//! `void*` and returns `-1` with `errno` set, or `0` when the context is
//! terminated. Here:
//!
//! * **The operands are typed.** [`proxy`] takes `&mut` socket values behind
//!   the [`Device`] trait, so a socket that cannot receive cannot be a
//!   frontend and the compiler says so — where C finds out at run time with
//!   `ENOTSUP`. The trait is implemented for the socket types a proxy is
//!   built from, and its two halves are exactly what a device needs: receive
//!   one whole message, send one whole message.
//! * **`capture` is an `Option`, not a null pointer.** libzmq's third
//!   argument is `NULL` for "no capture"; a missing socket has a name here.
//! * **It returns the counters.** libzmq's `zmq_proxy` returns nothing but
//!   success or failure, and only `zmq_proxy_steerable`'s `STATISTICS`
//!   command can be asked for numbers. Both return [`ProxyStatistics`] here,
//!   because a proxy that ends has counted its traffic either way and
//!   throwing that away would be a loss nobody asked for.
//! * **There is no polling loop.** libzmq's proxy is a `zmq_poll` over two
//!   sockets; this is a `select!` over two futures, which is the same
//!   decision made by the reactor instead of by a timeout.
//! * **A refused message does not end the device.** A socket refuses one
//!   message with `EINVAL`, `EHOSTUNREACH` or `EAGAIN`: a ROUTER given a
//!   message with no routing id, a routing id nobody holds under
//!   `ZMQ_ROUTER_MANDATORY`, a send that ran out of `ZMQ_SNDTIMEO`. The proxy
//!   drops that message, counts it in [`ProxyStatistics::refused`] and goes
//!   on, because the message came from a peer and one peer's message must not
//!   end the device for every other peer. Any other failure is the socket's,
//!   not the message's, and ends the device with its error.
//!
//! # No unbounded buffering
//!
//! One message at a time, and the send that forwards it is the blocking one:
//! a message is read, optionally copied to the capture socket, and written to
//! the other side **before** the next message is read. The proxy therefore
//! holds exactly one message, and backpressure is the peer's high-water mark
//! reaching back through the blocking send — which is how a slow backend
//! slows its frontend down rather than filling the proxy's memory.
//!
//! The one exception is the socket types that drop rather than block: XPUB,
//! XSUB and PUB "SHALL silently drop the message if the queue for a
//! subscriber is full" (29/PUBSUB), so a pub-sub proxy loses a message for a
//! slow subscriber instead of stalling every other one. That is the
//! mechanism's rule and not this module's choice, and it is why the
//! statistics count what was handed over rather than what arrived. A message
//! a socket refused counts in [`ProxyStatistics::refused`] and in neither
//! `_out` counter.
//!
//! # Subscriptions through a pub-sub proxy
//!
//! A forwarder has an XSUB toward its publishers and an XPUB toward its
//! subscribers, and the subscriptions its XPUB reads are what it hands the XSUB
//! to send upstream. Forwarded as they come they would let one subscriber with
//! a thousand junk prefixes fill the table the publishers keep for the proxy,
//! after which every topic an honest subscriber asks for is refused upstream in
//! silence; and they would put one subscription upstream for every subscriber
//! of a prefix, so that the first of them to leave cancelled it for the rest.
//!
//! So an XPUB end is read with [`XPubSocket::recv_forwarded`]: a prefix goes
//! upstream when its first subscriber takes it and is cancelled when its last
//! lets go or leaves, and each subscriber gets
//! [`upstream_share`](crate::upstream_share) of the table — an eighth of
//! `max_subscriptions`, and at least eight prefixes. A subscriber that asks for
//! more is sent an `ERROR` naming why, once, and what it asked for is not
//! forwarded. The XSUB end makes what it forwards through its own table
//! instead of queueing it as a message, so a publisher that restarts or
//! connects later is told again at its handshake.
//!
//! Give the two sockets the same `max_subscriptions`: the share is a fraction
//! of the XPUB's, and a table that is smaller upstream refuses in silence what
//! the share allowed. A capture socket gets a copy of every subscription the
//! device forwards, which are the ones it handled.

use std::fmt;

use crate::dealerrouter::{DealerSocket, RouterSocket};
use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::pair::PairSocket;
use crate::pipeline::{PullSocket, PushSocket};
use crate::pubsub::{PubSocket, SubSocket};
use crate::split;
use crate::xpubxsub::{XPubSocket, XSubSocket};

/// `PAUSE`: stop reading either side until `RESUME`.
pub const CONTROL_PAUSE: &[u8] = b"PAUSE";
/// `RESUME`: read again.
pub const CONTROL_RESUME: &[u8] = b"RESUME";
/// `TERMINATE`: end the proxy and return its counters.
pub const CONTROL_TERMINATE: &[u8] = b"TERMINATE";
/// `STATISTICS`: reply with the eight counters, on the control socket.
pub const CONTROL_STATISTICS: &[u8] = b"STATISTICS";

/// One end of a device: something that can receive and send a whole message.
///
/// Implemented for the socket types a proxy is built from. A socket type
/// that cannot do one half says so: [`Device::receives`] is `false` for a
/// PUSH or a PUB, and the proxy then never reads that side — it is a side
/// that never delivers, which is what the zguide's streamer (PULL in, PUSH
/// out) needs — while a `send` toward a PULL or a SUB reports `ENOTSUP`,
/// which is `zmq_socket(3)`'s own table rather than a limitation here.
/// The futures are **`Send`**, and that is written out rather than left to
/// `async fn` in a trait: a socket is `!Sync` but it is `Send`, and a
/// `&mut socket` future is therefore `Send` too, so a device may be driven on
/// a multi-thread executor — which every caller outside `block_on` needs,
/// `weida-zmq-py`'s `proxy` among them. An `async fn` in a trait promises no
/// such bound and cannot be spawned at all.
pub trait Device {
    /// Whether this end ever delivers a message. `false` for a socket type
    /// that only sends, so a proxy does not wait on a receive that can only
    /// fail. A method rather than a constant, because a device end whose
    /// type is erased — `weida-zmq-py`'s — only knows at run time.
    fn receives(&self) -> bool {
        true
    }

    /// Receives one whole message, waiting for one.
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send;

    /// Sends one whole message.
    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send;
}

/// A socket type that receives but does not send, and one that sends but does
/// not receive, said once each.
macro_rules! one_way_device {
    ($socket:ty, recv_only) => {
        impl Device for $socket {
            fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
                <$socket>::recv(self)
            }

            fn send(&mut self, _message: Multipart) -> impl Future<Output = Result<()>> + Send {
                std::future::ready(Err(Error::ENOTSUP(
                    concat!(
                        "a ",
                        stringify!($socket),
                        " does not send, so it can only be the frontend a device reads"
                    )
                    .into(),
                )))
            }
        }
    };
    ($socket:ty, publish_only) => {
        impl Device for $socket {
            fn receives(&self) -> bool {
                false
            }

            fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
                std::future::ready(Err(Error::ENOTSUP(
                    concat!(
                        "a ",
                        stringify!($socket),
                        " does not receive, so it can only be the backend a device writes"
                    )
                    .into(),
                )))
            }

            fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
                // A publisher drops at the high-water mark rather than
                // blocking, which 29/PUBSUB requires; the report says how
                // many peers took it and the proxy does not turn a drop into
                // an error.
                <$socket>::publish(self, message);
                std::future::ready(Ok(()))
            }
        }
    };
}

impl Device for PairSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        PairSocket::recv(self)
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        PairSocket::send(self, message)
    }
}

impl Device for DealerSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        DealerSocket::recv(self)
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        DealerSocket::send(self, message)
    }
}

impl Device for RouterSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        RouterSocket::recv(self)
    }

    /// The routing id is the first frame, which is exactly what a ROUTER's
    /// own `recv` prepended on the other side of the device: the queue device
    /// forwards the envelope rather than inventing one.
    async fn send(&mut self, message: Multipart) -> Result<()> {
        RouterSocket::send(self, message).await.map(|_| ())
    }
}

impl Device for XSubSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        XSubSocket::recv(self)
    }

    /// An XSUB sends subscriptions upstream, and a pub-sub proxy's XSUB is
    /// where the subscriptions its XPUB read are forwarded.
    ///
    /// A subscription is made through the socket's own table and not queued as
    /// a bare message, so that a publisher this socket connects to later, or
    /// reconnects to after it restarted, is told what the proxy holds at its
    /// handshake. A device that forwarded subscriptions without keeping them
    /// would go deaf upstream when the publisher did. The table refuses with
    /// `EINVAL` what a table refuses — a prefix it is full of, an
    /// unsubscribe for a prefix it does not hold — and a device counts that
    /// as the refusal it is and goes on.
    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        let outcome = match split::subscription_of(&message) {
            Some((true, prefix)) => XSubSocket::subscribe(self, prefix),
            Some((false, prefix)) => XSubSocket::unsubscribe(self, prefix),
            None => {
                XSubSocket::send(self, message);
                Ok(())
            }
        };
        std::future::ready(outcome)
    }
}

impl Device for XPubSocket {
    /// An XPUB end reports the subscriptions behind it the way a device needs
    /// them ([`XPubSocket::recv_forwarded`]): each prefix once however many
    /// subscribers hold it, and none for a subscriber over its share of the
    /// table the XSUB end forwards them into.
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        XPubSocket::recv_forwarded(self)
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        XPubSocket::publish(self, message);
        std::future::ready(Ok(()))
    }
}

one_way_device!(PullSocket, recv_only);
one_way_device!(SubSocket, recv_only);
one_way_device!(PubSocket, publish_only);

impl Device for PushSocket {
    fn receives(&self) -> bool {
        false
    }

    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        std::future::ready(Err(Error::ENOTSUP(
            "a PUSH socket does not receive, so it can only be the backend a device writes".into(),
        )))
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        PushSocket::send(self, message)
    }
}

/// A device with two ends that never deliver has nothing to do.
fn nothing_to_read() -> Error {
    Error::ENOTSUP("neither side of this device receives, so it would forward nothing".into())
}

/// One direction's traffic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counter {
    /// Messages handed over.
    pub messages: u64,
    /// Octets in those messages, frame bodies only — the same thing
    /// `ZMQ_MAXMSGSIZE` counts on the way in.
    pub bytes: u64,
}

impl Counter {
    /// Counts one message of `bytes` octets.
    fn record(&mut self, bytes: u64) {
        self.messages += 1;
        self.bytes += bytes;
    }
}

/// The eight counters `zmq_proxy_steerable`'s `STATISTICS` reports.
///
/// libzmq replies with eight 64-bit numbers in this order, one frame each:
/// the frontend's messages in, bytes in, messages out, bytes out, then the
/// backend's four. "In" is what the socket received and "out" is what it
/// accepted for sending. A message a socket refused is in neither: it is
/// counted in [`ProxyStatistics::refused`] instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProxyStatistics {
    /// What the frontend received.
    pub frontend_in: Counter,
    /// What the frontend accepted for sending.
    pub frontend_out: Counter,
    /// What the backend received.
    pub backend_in: Counter,
    /// What the backend accepted for sending.
    pub backend_out: Counter,
    /// Messages a socket refused with `EINVAL`, `EHOSTUNREACH` or `EAGAIN`:
    /// forwarded messages, capture copies and steering replies that were
    /// dropped rather than handed over. Not one of libzmq's eight counters, so
    /// [`ProxyStatistics::encode`] does not send it and
    /// [`ProxyStatistics::decode`] sets it to zero.
    pub refused: u64,
}

impl ProxyStatistics {
    /// The eight frames, in libzmq's order, each an eight-octet little-endian
    /// number.
    ///
    /// Little endian for the reason [`crate::monitor`]'s header gives: libzmq
    /// copies the numbers into the frames, so the octet order is the host's,
    /// and every platform this library runs on is little endian.
    pub fn encode(&self) -> Multipart {
        let mut frames = self.as_array().into_iter();
        let first = frames.next().expect("eight counters");
        let mut message = Multipart::single(first.to_le_bytes().to_vec());
        for value in frames {
            message.push(value.to_le_bytes().to_vec());
        }
        message
    }

    /// Reads the eight frames back.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` unless the message is eight frames of eight octets.
    pub fn decode(message: &Multipart) -> Result<ProxyStatistics> {
        let frames = message.frames();
        if frames.len() != 8 {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "a STATISTICS reply is eight frames; this one has {}",
                    frames.len()
                )
                .into(),
            ));
        }
        let mut values = [0u64; 8];
        for (value, frame) in values.iter_mut().zip(frames) {
            let bytes: [u8; 8] = frame.as_slice().try_into().map_err(|_| {
                Error::ENOCOMPATPROTO(
                    format!(
                        "a STATISTICS counter is eight octets; this one has {}",
                        frame.len()
                    )
                    .into(),
                )
            })?;
            *value = u64::from_le_bytes(bytes);
        }
        Ok(ProxyStatistics {
            frontend_in: Counter {
                messages: values[0],
                bytes: values[1],
            },
            frontend_out: Counter {
                messages: values[2],
                bytes: values[3],
            },
            backend_in: Counter {
                messages: values[4],
                bytes: values[5],
            },
            backend_out: Counter {
                messages: values[6],
                bytes: values[7],
            },
            refused: 0,
        })
    }

    fn as_array(&self) -> [u64; 8] {
        [
            self.frontend_in.messages,
            self.frontend_in.bytes,
            self.frontend_out.messages,
            self.frontend_out.bytes,
            self.backend_in.messages,
            self.backend_in.bytes,
            self.backend_out.messages,
            self.backend_out.bytes,
        ]
    }
}

impl fmt::Display for ProxyStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "frontend in {}/{}B out {}/{}B, backend in {}/{}B out {}/{}B",
            self.frontend_in.messages,
            self.frontend_in.bytes,
            self.frontend_out.messages,
            self.frontend_out.bytes,
            self.backend_in.messages,
            self.backend_in.bytes,
            self.backend_out.messages,
            self.backend_out.bytes,
        )?;
        if self.refused > 0 {
            write!(f, ", refused {}", self.refused)?;
        }
        Ok(())
    }
}

/// What a control message asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Steer {
    /// `PAUSE`.
    Pause,
    /// `RESUME`.
    Resume,
    /// `TERMINATE`.
    Terminate,
    /// `STATISTICS`.
    Statistics,
}

impl Steer {
    /// Reads a control message.
    ///
    /// # Errors
    ///
    /// `EINVAL` for a command libzmq does not define, naming the four that
    /// exist — a proxy that ignored an unknown command would be steered by
    /// something nobody can see.
    pub fn parse(message: &Multipart) -> Result<Steer> {
        let command = message.frames()[0].as_slice();
        match command {
            CONTROL_PAUSE => Ok(Steer::Pause),
            CONTROL_RESUME => Ok(Steer::Resume),
            CONTROL_TERMINATE => Ok(Steer::Terminate),
            CONTROL_STATISTICS => Ok(Steer::Statistics),
            other => Err(Error::EINVAL(
                format!(
                    "{} is not a proxy command; zmq_proxy_steerable defines PAUSE, RESUME, \
                     TERMINATE and STATISTICS",
                    String::from_utf8_lossy(other)
                )
                .into(),
            )),
        }
    }
}

/// `zmq_proxy`: moves messages both ways until a socket ends, copying every
/// one to `capture` if there is one.
///
/// A side that does not receive — a PUSH or a PUB — is never read, so the
/// zguide's streamer, PULL in and PUSH out, forwards instead of failing at
/// its first poll; both sides not receiving is `ENOTSUP`, because such a
/// device would forward nothing. The steerable form has no such refusal:
/// its control socket is always read, so a `TERMINATE` still ends it.
///
/// Runs until one of the sockets reports an error that is not a refusal —
/// which is what closing a socket or terminating the context looks like from
/// in here — and returns the counters it reached.
///
/// # Errors
///
/// Whatever a socket reports other than a refusal, which is counted and does
/// not end the device. `ENOTSOCK` and `ETERM` are the ordinary endings: the
/// sockets went away.
pub async fn proxy<F: Device, B: Device, C: Device>(
    frontend: &mut F,
    backend: &mut B,
    capture: Option<&mut C>,
) -> Result<ProxyStatistics> {
    let mut statistics = ProxyStatistics::default();
    let mut capture = capture;
    let (front_receives, back_receives) = (frontend.receives(), backend.receives());
    loop {
        tokio::select! {
            arrived = frontend.recv(), if front_receives => {
                let message = arrived?;
                statistics.frontend_in.record(message.total_bytes());
                capture_copy(&mut capture, &message, &mut statistics.refused).await?;
                forward(backend, message, &mut statistics.backend_out, &mut statistics.refused)
                    .await?;
            }
            arrived = backend.recv(), if back_receives => {
                let message = arrived?;
                statistics.backend_in.record(message.total_bytes());
                capture_copy(&mut capture, &message, &mut statistics.refused).await?;
                forward(frontend, message, &mut statistics.frontend_out, &mut statistics.refused)
                    .await?;
            }
            else => return Err(nothing_to_read()),
        }
    }
}

/// `zmq_proxy_steerable`: a [`proxy`] with a control socket.
///
/// `PAUSE` stops reading both data sockets — their queues fill and their
/// peers feel it, which is the point: a paused proxy is backpressure rather
/// than a black hole. `RESUME` reads again. `STATISTICS` replies on the
/// control socket with [`ProxyStatistics::encode`]'s eight frames. `TERMINATE`
/// returns the counters.
///
/// # Errors
///
/// As [`proxy`]. A control command libzmq does not define is refused the way a
/// message is: counted in [`ProxyStatistics::refused`] and skipped, so one
/// malformed control message does not end the device.
pub async fn proxy_steerable<F: Device, B: Device, C: Device, S: Device>(
    frontend: &mut F,
    backend: &mut B,
    capture: Option<&mut C>,
    control: &mut S,
) -> Result<ProxyStatistics> {
    let mut statistics = ProxyStatistics::default();
    let mut capture = capture;
    let (front_receives, back_receives) = (frontend.receives(), backend.receives());
    let mut paused = false;
    loop {
        tokio::select! {
            steering = control.recv() => {
                if let Some(steer) = parse_or_refuse(&steering?, &mut statistics.refused)? {
                    match steer {
                        Steer::Pause => paused = true,
                        Steer::Resume => paused = false,
                        Steer::Statistics => {
                            let reply = statistics.encode();
                            hand_over(control, reply, &mut statistics.refused).await?;
                        }
                        Steer::Terminate => return Ok(statistics),
                    }
                }
            }
            arrived = frontend.recv(), if front_receives && !paused => {
                let message = arrived?;
                statistics.frontend_in.record(message.total_bytes());
                capture_copy(&mut capture, &message, &mut statistics.refused).await?;
                forward(backend, message, &mut statistics.backend_out, &mut statistics.refused)
                    .await?;
            }
            arrived = backend.recv(), if back_receives && !paused => {
                let message = arrived?;
                statistics.backend_in.record(message.total_bytes());
                capture_copy(&mut capture, &message, &mut statistics.refused).await?;
                forward(frontend, message, &mut statistics.frontend_out, &mut statistics.refused)
                    .await?;
            }
        }
    }
}

/// Whether a send's failure is about the message and not the socket.
///
/// `EINVAL` is a message the socket cannot frame for its type, such as a
/// ROUTER given a message with no routing id. `EHOSTUNREACH` is a routing id
/// nobody holds under `ZMQ_ROUTER_MANDATORY`, the everyday race in which the
/// client has already gone away. `EAGAIN` is a send that ran out of
/// `ZMQ_SNDTIMEO`. The socket is usable after each of them, so the device
/// keeps it. Any other failure is the socket's own, and ends the device.
fn is_refusal(error: &Error) -> bool {
    matches!(
        error,
        Error::EINVAL(_) | Error::EHOSTUNREACH(_) | Error::EAGAIN(_)
    )
}

/// Counts one refused message and says so at debug level. A peer chooses what
/// it sends, so a refusal is never a warning: at a peer's rate, a line per
/// message would flood the log.
fn refuse(refused: &mut u64, error: &Error) {
    *refused += 1;
    tracing::debug!(
        errno = error.errno(),
        cause = error.cause(),
        "a socket refused a message; the proxy dropped it and went on"
    );
}

/// Hands `message` to `socket`. Returns whether it was handed over: a refusal
/// is counted in `refused` and the message dropped, and any other failure is
/// returned, because it ends the device.
async fn hand_over<D: Device>(
    socket: &mut D,
    message: Multipart,
    refused: &mut u64,
) -> Result<bool> {
    match socket.send(message).await {
        Ok(()) => Ok(true),
        Err(error) if is_refusal(&error) => {
            refuse(refused, &error);
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

/// Forwards `message` to `socket` and counts it in `out` once it was handed
/// over, so the counters say what the socket accepted, not what was tried.
async fn forward<D: Device>(
    socket: &mut D,
    message: Multipart,
    out: &mut Counter,
    refused: &mut u64,
) -> Result<()> {
    let bytes = message.total_bytes();
    if hand_over(socket, message, refused).await? {
        out.record(bytes);
    }
    Ok(())
}

/// Parses a control message, or counts it as refused when it names no command.
fn parse_or_refuse(message: &Multipart, refused: &mut u64) -> Result<Option<Steer>> {
    match Steer::parse(message) {
        Ok(steer) => Ok(Some(steer)),
        Err(error) if is_refusal(&error) => {
            refuse(refused, &error);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Hands the capture socket a copy, if there is one.
///
/// A copy and not the message: the capture socket is an observer, so what it
/// gets must not be what the backend gets. This is the one clone a proxy
/// makes, and it is made only when somebody is watching. A refused copy is
/// counted and the message still goes on: an observer that refuses must not
/// stop the traffic it watches.
async fn capture_copy<C: Device>(
    capture: &mut Option<&mut C>,
    message: &Multipart,
    refused: &mut u64,
) -> Result<()> {
    if let Some(capture) = capture {
        hand_over(&mut **capture, message.clone(), refused).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, ContextConfig};
    use crate::message::Message;
    use crate::options::SocketOptions;
    use crate::subscriptions::{self, DEFAULT_MAX_SUBSCRIPTIONS};
    use crate::upstream::upstream_share;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::task::JoinHandle;

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    /// Claim: the eight counters go both ways through libzmq's frame form, and
    /// a malformed reply is refused.
    #[test]
    fn the_statistics_are_eight_counters() {
        let statistics = ProxyStatistics {
            frontend_in: Counter {
                messages: 1,
                bytes: 2,
            },
            frontend_out: Counter {
                messages: 3,
                bytes: 4,
            },
            backend_in: Counter {
                messages: 5,
                bytes: 6,
            },
            backend_out: Counter {
                messages: 7,
                bytes: 8,
            },
            refused: 0,
        };
        let message = statistics.encode();
        assert_eq!(message.len(), 8);
        assert!(message.frames().iter().all(|frame| frame.len() == 8));
        assert_eq!(
            ProxyStatistics::decode(&message).expect("decode"),
            statistics
        );
        assert!(ProxyStatistics::decode(&Multipart::single(vec![0u8; 8])).is_err());
        let mut short = Multipart::single(vec![0u8; 7]);
        for _ in 0..7 {
            short.push(vec![0u8; 8]);
        }
        assert!(ProxyStatistics::decode(&short).is_err());
    }

    /// Claim: the four commands parse and nothing else does.
    #[test]
    fn the_control_commands_are_libzmqs_four() {
        for (frame, expected) in [
            (CONTROL_PAUSE, Steer::Pause),
            (CONTROL_RESUME, Steer::Resume),
            (CONTROL_TERMINATE, Steer::Terminate),
            (CONTROL_STATISTICS, Steer::Statistics),
        ] {
            assert_eq!(
                Steer::parse(&Multipart::single(frame.to_vec())).expect("a command"),
                expected
            );
        }
        let err = Steer::parse(&Multipart::single(b"STOP".to_vec())).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(err.cause().contains("TERMINATE"), "{err}");
    }

    /// Claim: a device whose two ends both never deliver is refused rather
    /// than parked forever.
    #[tokio::test]
    async fn a_device_with_nothing_to_read_is_refused() {
        let context = context();
        let mut frontend = PushSocket::new(&context).expect("push");
        let mut backend = PubSocket::new(&context).expect("pub");
        let err = proxy::<_, _, PairSocket>(&mut frontend, &mut backend, None)
            .await
            .expect_err("nothing to forward");
        assert_eq!(err.errno(), "ENOTSUP", "{err}");
    }

    /// Bounds one await in a test: a hang becomes a failure that names the
    /// step, not a test run that never ends.
    async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .unwrap_or_else(|_| panic!("{what} did not finish within 10 s"))
    }

    /// Asks a running steerable proxy for its counters until its backend has
    /// received `backend_in` messages. The loop that reads the data sockets
    /// also answers STATISTICS, one event at a time, and a message's forward
    /// is awaited inside its own branch, so a reply that counts a message
    /// comes after that forward finished, refused or not.
    async fn statistics_once_received(
        device: &JoinHandle<Result<ProxyStatistics>>,
        steer: &mut PairSocket,
        backend_in: u64,
    ) -> ProxyStatistics {
        bounded("the proxy answers STATISTICS", async {
            loop {
                assert!(
                    !device.is_finished(),
                    "the proxy ended on a message it had to survive"
                );
                steer
                    .send(Multipart::single(CONTROL_STATISTICS.to_vec()))
                    .await
                    .expect("ask");
                let reply = steer.recv().await.expect("a reply");
                let statistics = ProxyStatistics::decode(&reply).expect("decode");
                if statistics.backend_in.messages >= backend_in {
                    return statistics;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    }

    /// The queue device the refusal tests share: a REQ client on the
    /// frontend, a raw DEALER worker on the backend, and the proxy steered
    /// over `control_endpoint`. The worker first sends `unprompted`, which the
    /// frontend refuses; then one request must still cross and its reply come
    /// back. Returns the counters the device reports at `TERMINATE`.
    async fn a_queue_device_outlives_a_refused_message(
        context: &Context,
        mut frontend: RouterSocket,
        unprompted: Multipart,
        control_endpoint: &str,
    ) -> ProxyStatistics {
        use crate::reqrep::ReqSocket;

        let front = bounded("bind the frontend", frontend.bind("tcp://127.0.0.1:0"))
            .await
            .expect("bind");
        let mut backend = DealerSocket::new(context).expect("dealer");
        let back = bounded("bind the backend", backend.bind("tcp://127.0.0.1:0"))
            .await
            .expect("bind");
        let mut worker = DealerSocket::new(context).expect("dealer");
        worker.connect(&back.to_string()).expect("connect");
        let mut client = ReqSocket::new(context).expect("req");
        client.connect(&front.to_string()).expect("connect");

        let mut control = PairSocket::new(context).expect("pair");
        bounded("bind the control", control.bind(control_endpoint))
            .await
            .expect("bind");
        let mut steer = PairSocket::new(context).expect("pair");
        steer.connect(control_endpoint).expect("connect");

        let device = tokio::spawn(async move {
            proxy_steerable::<_, _, PairSocket, _>(&mut frontend, &mut backend, None, &mut control)
                .await
        });

        bounded("the worker sends", worker.send(unprompted))
            .await
            .expect("send");
        let seen = statistics_once_received(&device, &mut steer, 1).await;
        assert_eq!(
            seen.backend_in.messages, 1,
            "the unprompted message was taken"
        );

        bounded("the client sends", client.send(Multipart::single("work")))
            .await
            .expect("send");
        let request = bounded("the request arrives", worker.recv())
            .await
            .expect("a request");
        assert_eq!(request.len(), 3, "routing id, empty delimiter, body");
        assert_eq!(request.frames()[2].as_slice(), b"work");
        let mut answer = Multipart::single(request.frames()[0].as_slice().to_vec());
        answer.push(Vec::new());
        answer.push(b"done".to_vec());
        bounded("the worker answers", worker.send(answer))
            .await
            .expect("reply");
        let reply = bounded("the reply arrives", client.recv())
            .await
            .expect("a reply");
        assert_eq!(reply.frames()[0].as_slice(), b"done");

        bounded(
            "TERMINATE",
            steer.send(Multipart::single(CONTROL_TERMINATE.to_vec())),
        )
        .await
        .expect("terminate");
        bounded("the proxy ends", device)
            .await
            .expect("the task")
            .expect("the counters")
    }

    /// Claim: a control message that names no command is counted and skipped;
    /// the proxy keeps running, and a TERMINATE sent after it still ends it.
    #[tokio::test]
    async fn an_unknown_control_command_is_counted_and_skipped() {
        let context = context();
        let mut frontend = PairSocket::new(&context).expect("pair");
        let mut backend = PairSocket::new(&context).expect("pair");
        let mut control = PairSocket::new(&context).expect("pair");
        bounded(
            "bind the control",
            control.bind("inproc://proxy.unknown.command"),
        )
        .await
        .expect("bind");
        let mut steer = PairSocket::new(&context).expect("pair");
        steer
            .connect("inproc://proxy.unknown.command")
            .expect("connect");

        let device = tokio::spawn(async move {
            proxy_steerable::<_, _, PairSocket, _>(&mut frontend, &mut backend, None, &mut control)
                .await
        });
        bounded("send STOP", steer.send(Multipart::single(b"STOP".to_vec())))
            .await
            .expect("send");
        bounded(
            "TERMINATE",
            steer.send(Multipart::single(CONTROL_TERMINATE.to_vec())),
        )
        .await
        .expect("terminate");
        let statistics = bounded("the proxy ends", device)
            .await
            .expect("the task")
            .expect("the counters");
        assert_eq!(statistics.refused, 1, "STOP was counted");
    }

    fn bytes_of(message: &Multipart) -> Vec<u8> {
        message.frames()[0].as_slice().to_vec()
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A pub-sub forwarder, running: an XSUB facing `upstream` and an XPUB
    /// facing subscribers, with the XPUB as the device's frontend or as its
    /// backend. Returns where subscribers connect, and the device.
    async fn forwarder(
        context: &Context,
        upstream: &str,
        xpub_is_frontend: bool,
    ) -> (String, JoinHandle<Result<ProxyStatistics>>) {
        let mut xsub = XSubSocket::new(context).expect("xsub");
        xsub.connect(upstream).expect("connect upstream");
        let mut xpub = XPubSocket::new(context).expect("xpub");
        let bound = bounded("bind the XPUB", xpub.bind("tcp://127.0.0.1:0"))
            .await
            .expect("bind");
        let device = if xpub_is_frontend {
            tokio::spawn(async move { proxy::<_, _, PairSocket>(&mut xpub, &mut xsub, None).await })
        } else {
            tokio::spawn(async move { proxy::<_, _, PairSocket>(&mut xsub, &mut xpub, None).await })
        };
        (bound.to_string(), device)
    }

    fn subscriber_of(context: &Context, at: &str) -> SubSocket {
        let subscriber = SubSocket::new(context).expect("sub");
        subscriber.connect(at).expect("connect");
        subscriber
    }

    /// Publishes `topic` on `upstream` until `subscriber` receives one: the
    /// publisher drops what nobody wants, so a subscription in effect is
    /// what the first receipt shows.
    async fn published_until_received(
        upstream: &mut impl FnMut(Multipart),
        subscriber: &mut SubSocket,
        topic: &str,
    ) {
        bounded("the subscriber receives", async {
            loop {
                upstream(Multipart::single(topic.to_owned()));
                if subscriber.try_recv().is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
    }

    /// Every event `upstream` reports before `window` passes with none.
    async fn events_within(upstream: &mut XPubSocket, window: Duration) -> Vec<Vec<u8>> {
        let mut events = Vec::new();
        while let Ok(event) = upstream.recv_timeout(window).await {
            events.push(bytes_of(&event));
        }
        events
    }

    async fn two_subscribers_hold_a_prefix(xpub_is_frontend: bool) {
        let context = context();
        let mut upstream = XPubSocket::with_options(
            &context,
            SocketOptions {
                xpub_verbose: true,
                ..SocketOptions::default()
            },
        )
        .expect("xpub");
        let upstream_at = bounded("bind upstream", upstream.bind("tcp://127.0.0.1:0"))
            .await
            .expect("bind")
            .to_string();
        let (downstream, _device) = forwarder(&context, &upstream_at, xpub_is_frontend).await;

        let mut first = subscriber_of(&context, &downstream);
        let mut second = subscriber_of(&context, &downstream);
        first.subscribe("news").expect("subscribe");
        second.subscribe("news").expect("subscribe");

        // A message reaching a subscriber is its subscription in effect, here
        // and upstream, so both are in by the time both have one.
        let mut publish = |message| {
            upstream.publish(message);
        };
        published_until_received(&mut publish, &mut first, "news 1").await;
        published_until_received(&mut publish, &mut second, "news 1").await;

        let announced = events_within(&mut upstream, Duration::from_millis(300)).await;
        assert_eq!(
            announced,
            [b"\x01news".to_vec()],
            "one subscription upstream for two subscribers"
        );

        first.unsubscribe("news").expect("unsubscribe");
        assert!(
            events_within(&mut upstream, Duration::from_millis(300))
                .await
                .is_empty(),
            "the second subscriber still holds the prefix, so nothing is cancelled"
        );
        let mut publish = |message| {
            upstream.publish(message);
        };
        published_until_received(&mut publish, &mut second, "news 2").await;

        second.unsubscribe("news").expect("unsubscribe");
        let cancelled = bounded("the last cancel", upstream.recv())
            .await
            .expect("an event");
        assert_eq!(bytes_of(&cancelled), b"\x00news");
    }

    /// The junk subscriber's prefixes: as many as a table takes, all different.
    fn junk(index: usize) -> String {
        format!("junk-{index:04}")
    }

    /// Claim: **an XSUB device end keeps what it forwards in its own table**,
    /// so that it is what a publisher is told at a handshake, and refuses what a
    /// table refuses: a cancel for a prefix it does not hold. Data passes as
    /// before.
    #[tokio::test]
    async fn an_xsub_device_end_keeps_the_subscriptions_it_forwards() {
        let context = context();
        let mut xsub = XSubSocket::new(&context).expect("xsub");
        let form =
            |subscribe| Multipart::single(subscriptions::write_message_form(subscribe, b"news"));

        Device::send(&mut xsub, form(true))
            .await
            .expect("a subscription");
        assert_eq!(xsub.subscriptions(), [b"news".to_vec()]);
        Device::send(&mut xsub, Multipart::single("data"))
            .await
            .expect("data passes");
        assert_eq!(xsub.subscriptions().len(), 1, "data is no subscription");

        Device::send(&mut xsub, form(false))
            .await
            .expect("a cancel");
        assert!(xsub.subscriptions().is_empty());
        let err = Device::send(&mut xsub, form(false))
            .await
            .expect_err("a second cancel is for nothing");
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: a pub-sub proxy over XSUB/XPUB forwards a publisher's messages
        /// to a subscriber, forwards the subscriber's subscription upstream, and
        /// gives the capture socket a copy of what passed — the zguide's
        /// forwarder and Espresso in one.
        #[tokio::test]
        async fn a_pubsub_proxy_forwards_both_ways_and_captures() {
            use crate::pubsub::{PubSocket, SubSocket};

            let context = context();
            let mut publisher = PubSocket::new(&context).expect("pub");
            let front = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut frontend = XSubSocket::new(&context).expect("xsub");
            frontend.connect(&front.to_string()).expect("connect");
            let mut backend = XPubSocket::new(&context).expect("xpub");
            let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut subscriber = SubSocket::new(&context).expect("sub");
            subscriber.connect(&back.to_string()).expect("connect");
            subscriber.subscribe("px").expect("subscribe");

            // The capture socket: a PAIR over inproc, which is what the Espresso
            // recipe reads.
            let mut capture = PairSocket::new(&context).expect("pair");
            capture
                .bind("inproc://capture.pubsub")
                .await
                .expect("bind capture");
            let mut watcher = PairSocket::new(&context).expect("pair");
            watcher
                .connect("inproc://capture.pubsub")
                .expect("connect capture");

            let device = tokio::spawn(async move {
                proxy(&mut frontend, &mut backend, Some(&mut capture)).await
            });

            // The subscription reaches the publisher through the proxy, so a
            // message published after it arrives is matched. The publisher drops
            // what nobody wants, so this is retried until one lands.
            let delivered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    publisher.publish(Multipart::single(Message::from("px.eur 1.09")));
                    if let Ok(message) = subscriber.try_recv() {
                        return message;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the subscriber got a message");
            assert_eq!(delivered.frames()[0].as_slice(), b"px.eur 1.09");

            // The capture socket saw a copy of what went past — subscriptions
            // included, since a proxy forwards those too.
            let seen = tokio::time::timeout(std::time::Duration::from_secs(10), watcher.recv())
                .await
                .expect("a captured message")
                .expect("a message");
            assert!(
                !seen.frames().is_empty(),
                "the capture socket received a copy"
            );
            device.abort();
        }
        /// Claim: the queue device over ROUTER/DEALER carries a request to a
        /// worker and its reply back to the right client, which is the envelope
        /// passing through the proxy untouched.
        #[tokio::test]
        async fn a_queue_device_routes_a_request_and_its_reply() {
            use crate::reqrep::{RepSocket, ReqSocket};

            let context = context();
            let mut frontend = RouterSocket::new(&context).expect("router");
            let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut backend = DealerSocket::new(&context).expect("dealer");
            let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut client = ReqSocket::new(&context).expect("req");
            client.connect(&front.to_string()).expect("connect");
            let mut worker = RepSocket::new(&context).expect("rep");
            worker.connect(&back.to_string()).expect("connect");

            let device = tokio::spawn(async move {
                proxy::<_, _, PairSocket>(&mut frontend, &mut backend, None).await
            });

            client.send(Multipart::single("work")).await.expect("send");
            let request = tokio::time::timeout(std::time::Duration::from_secs(10), worker.recv())
                .await
                .expect("the request arrived")
                .expect("a request");
            assert_eq!(request.frames()[0].as_slice(), b"work");
            worker.send(Multipart::single("done")).await.expect("reply");
            let reply = tokio::time::timeout(std::time::Duration::from_secs(10), client.recv())
                .await
                .expect("the reply arrived")
                .expect("a reply");
            assert_eq!(reply.frames()[0].as_slice(), b"done");
            device.abort();
        }
        /// Claim: the zguide's streamer — PULL in, PUSH out — forwards, and so
        /// does the same pair the other way round; a side that cannot receive is
        /// a side that never delivers, not a failure at the first poll. The
        /// steerable form and a send-only capture socket get the same treatment.
        #[tokio::test]
        async fn a_streamer_forwards_through_a_side_that_never_receives() {
            let context = context();
            // PULL frontend, PUSH backend: the streamer as the guide draws it.
            let mut frontend = PullSocket::new(&context).expect("pull");
            let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut backend = PushSocket::new(&context).expect("push");
            let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");
            // The capture socket is a PUSH too: it only ever sends.
            let mut capture = PushSocket::new(&context).expect("push");
            let tap = capture.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut producer = PushSocket::new(&context).expect("push");
            producer.connect(&front.to_string()).expect("connect");
            let mut consumer = PullSocket::new(&context).expect("pull");
            consumer.connect(&back.to_string()).expect("connect");
            let mut watcher = PullSocket::new(&context).expect("pull");
            watcher.connect(&tap.to_string()).expect("connect");

            let device = tokio::spawn(async move {
                proxy(&mut frontend, &mut backend, Some(&mut capture)).await
            });

            producer
                .send(Multipart::single("job 1"))
                .await
                .expect("send");
            let crossed = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
                .await
                .expect("the job crossed the streamer")
                .expect("a job");
            assert_eq!(crossed.frames()[0].as_slice(), b"job 1");
            let seen = tokio::time::timeout(std::time::Duration::from_secs(10), watcher.recv())
                .await
                .expect("the capture socket saw it")
                .expect("a copy");
            assert_eq!(seen.frames()[0].as_slice(), b"job 1");
            assert!(
                !device.is_finished(),
                "a streamer does not die at its first poll"
            );
            device.abort();

            // The same pair the other way round: PUSH frontend, PULL backend.
            // Traffic flows backend to frontend and nothing is read from the
            // PUSH.
            let mut frontend = PushSocket::new(&context).expect("push");
            let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut backend = PullSocket::new(&context).expect("pull");
            let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut control = PairSocket::new(&context).expect("pair");
            control
                .bind("inproc://proxy.streamer.control")
                .await
                .expect("bind control");
            let mut steer = PairSocket::new(&context).expect("pair");
            steer
                .connect("inproc://proxy.streamer.control")
                .expect("connect control");
            let mut consumer = PullSocket::new(&context).expect("pull");
            consumer.connect(&front.to_string()).expect("connect");
            let mut producer = PushSocket::new(&context).expect("push");
            producer.connect(&back.to_string()).expect("connect");

            let device = tokio::spawn(async move {
                proxy_steerable::<_, _, PushSocket, _>(
                    &mut frontend,
                    &mut backend,
                    None,
                    &mut control,
                )
                .await
            });
            producer
                .send(Multipart::single("job 2"))
                .await
                .expect("send");
            let crossed = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
                .await
                .expect("the job crossed the reversed streamer")
                .expect("a job");
            assert_eq!(crossed.frames()[0].as_slice(), b"job 2");
            steer
                .send(Multipart::single(CONTROL_TERMINATE.to_vec()))
                .await
                .expect("terminate");
            let statistics = tokio::time::timeout(std::time::Duration::from_secs(10), device)
                .await
                .expect("the proxy terminated")
                .expect("the task")
                .expect("the counters");
            assert_eq!(statistics.backend_in.messages, 1);
            assert_eq!(statistics.frontend_out.messages, 1);
            assert_eq!(
                statistics.frontend_in.messages, 0,
                "a PUSH frontend is never read"
            );
        }
        /// Claim: the control socket steers — `PAUSE` stops the traffic,
        /// `RESUME` starts it again, `STATISTICS` answers with the eight
        /// counters, and `TERMINATE` ends the proxy and returns them.
        #[tokio::test]
        async fn the_steerable_proxy_pauses_resumes_reports_and_terminates() {
            let context = context();
            // A PAIR-to-PAIR proxy: the simplest device there is, and the one
            // where a paused proxy is observable without a pattern's own rules in
            // the way.
            let mut frontend = PairSocket::new(&context).expect("pair");
            let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut backend = PairSocket::new(&context).expect("pair");
            let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut sender = PairSocket::new(&context).expect("pair");
            sender.connect(&front.to_string()).expect("connect");
            let mut receiver = PairSocket::new(&context).expect("pair");
            receiver.connect(&back.to_string()).expect("connect");

            let mut control = PairSocket::new(&context).expect("pair");
            control
                .bind("inproc://proxy.control")
                .await
                .expect("bind control");
            let mut steer = PairSocket::new(&context).expect("pair");
            steer
                .connect("inproc://proxy.control")
                .expect("connect control");

            let device = tokio::spawn(async move {
                proxy_steerable::<_, _, PairSocket, _>(
                    &mut frontend,
                    &mut backend,
                    None,
                    &mut control,
                )
                .await
            });

            // Running: a message goes through.
            sender.send(Multipart::single("first")).await.expect("send");
            let arrived = tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
                .await
                .expect("forwarded")
                .expect("a message");
            assert_eq!(arrived.frames()[0].as_slice(), b"first");

            // Paused: the next message waits in the frontend's queue rather than
            // reaching the receiver.
            steer
                .send(Multipart::single(CONTROL_PAUSE.to_vec()))
                .await
                .expect("pause");
            // The pause has to be observed before the message is sent, and the
            // only observable that does not race is STATISTICS: the proxy answers
            // it from the same loop that took the PAUSE.
            steer
                .send(Multipart::single(CONTROL_STATISTICS.to_vec()))
                .await
                .expect("ask");
            let reply = tokio::time::timeout(std::time::Duration::from_secs(10), steer.recv())
                .await
                .expect("a statistics reply")
                .expect("a reply");
            let statistics = ProxyStatistics::decode(&reply).expect("decode");
            assert_eq!(statistics.frontend_in.messages, 1);
            assert_eq!(statistics.backend_out.messages, 1);
            assert_eq!(statistics.frontend_in.bytes, 5, "\"first\" is five octets");

            sender
                .send(Multipart::single("second"))
                .await
                .expect("send");
            assert!(
                receiver.try_recv().is_err(),
                "a paused proxy forwards nothing"
            );

            // Resumed: it arrives.
            steer
                .send(Multipart::single(CONTROL_RESUME.to_vec()))
                .await
                .expect("resume");
            let arrived = tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
                .await
                .expect("forwarded after the resume")
                .expect("a message");
            assert_eq!(arrived.frames()[0].as_slice(), b"second");

            // Terminated: the proxy ends and hands back its counters.
            steer
                .send(Multipart::single(CONTROL_TERMINATE.to_vec()))
                .await
                .expect("terminate");
            let statistics = tokio::time::timeout(std::time::Duration::from_secs(10), device)
                .await
                .expect("the proxy ended")
                .expect("the task")
                .expect("the counters");
            assert_eq!(statistics.frontend_in.messages, 2);
            assert_eq!(statistics.backend_out.messages, 2);
            assert_eq!(statistics.frontend_in.bytes, 11, "five and six octets");
            assert_eq!(
                statistics.backend_in.messages, 0,
                "nothing came back the other way"
            );
        }
        /// Claim: a ROUTER frontend refuses a message that carries only a routing
        /// id with EINVAL, and that refusal does not end the queue device. One
        /// worker's malformed message must not take the broker down for every
        /// client: the message is counted and dropped, and the next request still
        /// crosses.
        #[tokio::test]
        async fn a_refused_message_does_not_end_a_queue_device() {
            let context = context();
            let frontend = RouterSocket::new(&context).expect("router");
            let statistics = a_queue_device_outlives_a_refused_message(
                &context,
                frontend,
                Multipart::single(b"x".to_vec()),
                "inproc://proxy.queue.refused",
            )
            .await;
            assert_eq!(statistics.refused, 1, "{statistics}");
        }
        /// Claim: with ZMQ_ROUTER_MANDATORY a reply to a routing id nobody holds
        /// is refused with EHOSTUNREACH, which is the everyday race in which the
        /// client has already gone away. It must not end the device that serves
        /// everybody else.
        #[tokio::test]
        async fn a_reply_to_a_departed_client_does_not_end_a_queue_device() {
            let context = context();
            let options = SocketOptions {
                router_mandatory: true,
                ..SocketOptions::default()
            };
            let frontend = RouterSocket::with_options(&context, options).expect("router");
            // A reply addressed to a routing id that no connected client holds.
            let mut departed = Multipart::single(b"gone".to_vec());
            departed.push(Vec::new());
            departed.push(b"late reply".to_vec());
            let statistics = a_queue_device_outlives_a_refused_message(
                &context,
                frontend,
                departed,
                "inproc://proxy.queue.departed",
            )
            .await;
            assert_eq!(statistics.refused, 1, "{statistics}");
        }
        /// Claim: a capture socket that refuses its copy does not stop the message
        /// reaching its destination, and the refusal is counted. The capture is a
        /// ROUTER, which refuses a message that has no routing id.
        #[tokio::test]
        async fn a_refused_capture_copy_still_reaches_its_destination() {
            let context = context();
            let mut frontend = PairSocket::new(&context).expect("pair");
            let front = bounded("bind the frontend", frontend.bind("tcp://127.0.0.1:0"))
                .await
                .expect("bind");
            let mut backend = PairSocket::new(&context).expect("pair");
            let back = bounded("bind the backend", backend.bind("tcp://127.0.0.1:0"))
                .await
                .expect("bind");
            let mut capture = RouterSocket::new(&context).expect("router");

            let mut sender = PairSocket::new(&context).expect("pair");
            sender.connect(&front.to_string()).expect("connect");
            let mut receiver = PairSocket::new(&context).expect("pair");
            receiver.connect(&back.to_string()).expect("connect");

            let mut control = PairSocket::new(&context).expect("pair");
            bounded(
                "bind the control",
                control.bind("inproc://proxy.capture.refused"),
            )
            .await
            .expect("bind");
            let mut steer = PairSocket::new(&context).expect("pair");
            steer
                .connect("inproc://proxy.capture.refused")
                .expect("connect");

            let device = tokio::spawn(async move {
                proxy_steerable::<_, _, RouterSocket, _>(
                    &mut frontend,
                    &mut backend,
                    Some(&mut capture),
                    &mut control,
                )
                .await
            });

            bounded("the sender sends", sender.send(Multipart::single("first")))
                .await
                .expect("send");
            let arrived = bounded("the message is forwarded", receiver.recv())
                .await
                .expect("a message");
            assert_eq!(arrived.frames()[0].as_slice(), b"first");

            bounded(
                "TERMINATE",
                steer.send(Multipart::single(CONTROL_TERMINATE.to_vec())),
            )
            .await
            .expect("terminate");
            let statistics = bounded("the proxy ends", device)
                .await
                .expect("the task")
                .expect("the counters");
            assert_eq!(statistics.refused, 1, "the capture copy was refused");
            assert_eq!(statistics.frontend_in.messages, 1);
            assert_eq!(
                statistics.backend_out.messages, 1,
                "the message still went on"
            );
        }
        /// Claim: a failure that is about the socket, not the message, still ends
        /// the device with its error. A PULL cannot send, so forwarding to it is
        /// ENOTSUP: a wrong device, which no later message can fix.
        #[tokio::test]
        async fn a_socket_that_cannot_send_still_ends_the_device() {
            let context = context();
            let mut frontend = PairSocket::new(&context).expect("pair");
            let front = bounded("bind the frontend", frontend.bind("tcp://127.0.0.1:0"))
                .await
                .expect("bind");
            let mut backend = PullSocket::new(&context).expect("pull");
            let mut sender = PairSocket::new(&context).expect("pair");
            sender.connect(&front.to_string()).expect("connect");

            let device = tokio::spawn(async move {
                proxy::<_, _, PairSocket>(&mut frontend, &mut backend, None).await
            });
            bounded("the sender sends", sender.send(Multipart::single("job")))
                .await
                .expect("send");
            let err = bounded("the device ends", device)
                .await
                .expect("the task")
                .expect_err("a send the socket cannot make ends the device");
            assert_eq!(err.errno(), "ENOTSUP", "{err}");
        }
        /// Claim: **a prefix two subscribers hold goes upstream once, and its
        /// cancel goes when the last of them lets go.** Upstream is an XPUB that
        /// reports every subscribe it is sent, repeats included, so a prefix
        /// forwarded for each subscriber shows as two. The first subscriber to
        /// leave must not cancel it for the other, which a publisher that keeps a
        /// set of connections per prefix — libzmq's — would otherwise do.
        #[tokio::test]
        async fn a_prefix_two_subscribers_hold_goes_upstream_once_and_is_cancelled_with_the_last() {
            two_subscribers_hold_a_prefix(false).await;
        }
        /// Claim: the same holds with the XPUB as the device's **frontend** and the
        /// XSUB as its backend: a device is symmetric, and which end faces the
        /// subscribers is the caller's choice.
        #[tokio::test]
        async fn the_same_holds_with_the_xpub_as_the_frontend() {
            two_subscribers_hold_a_prefix(true).await;
        }
        /// Claim: **one subscriber with 1024 junk prefixes cannot stop an honest
        /// one from receiving a new topic.** The subscribers behind a proxy share
        /// one table upstream; a subscriber that filled it would have every topic
        /// asked for afterwards refused there in silence. It is held to its share
        /// instead: the prefixes past it are not forwarded, it is told with an
        /// `ERROR` — which this library's own subscriber answers by ending its
        /// connection — and what it did hold is cancelled upstream as it goes.
        #[tokio::test]
        async fn one_subscriber_with_junk_prefixes_cannot_stop_an_honest_one() {
            let context = context();
            let upstream = XPubSocket::new(&context).expect("xpub");
            let upstream_at = bounded("bind upstream", upstream.bind("tcp://127.0.0.1:0"))
                .await
                .expect("bind")
                .to_string();
            let (mut publish, mut reports) = upstream.split();
            let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
            let collecting = {
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    while let Ok(event) = reports.recv().await {
                        seen.lock().expect("seen").push(bytes_of(&event));
                    }
                })
            };
            let (downstream, _device) = forwarder(&context, &upstream_at, false).await;

            // The junk subscriber holds as many prefixes as a table takes, and says
            // so all at once at its handshake. It does not reconnect after the
            // `ERROR`, so that what upstream sees is one try.
            let mut junk_subscriber = SubSocket::with_options(
                &context,
                SocketOptions {
                    reconnect_ivl: None,
                    ..SocketOptions::default()
                },
            )
            .expect("sub");
            for index in 0..DEFAULT_MAX_SUBSCRIPTIONS {
                junk_subscriber
                    .subscribe(junk(index))
                    .expect("a prefix the subscriber's own table takes");
            }
            junk_subscriber.connect(&downstream).expect("connect");

            let share = upstream_share(DEFAULT_MAX_SUBSCRIPTIONS);
            let upstream_saw = |kind: &[u8]| {
                seen.lock()
                    .expect("seen")
                    .iter()
                    .filter(|event| event.starts_with(kind) && event[1..].starts_with(b"junk-"))
                    .count()
            };
            let junk_subscriber_is_gone = |subscriber: &SubSocket| {
                subscriber.connections().iter().all(|peer| !peer.connected)
            };

            // The honest subscriber asks for its topic once the junk has been taken
            // in: all of it is upstream, or its subscriber was refused and let go
            // after the share. Asking sooner would let the topic in ahead of the
            // flood whatever the proxy does about it.
            let mut honest = subscriber_of(&context, &downstream);
            wait_for(|| {
                upstream_saw(b"\x01") == DEFAULT_MAX_SUBSCRIPTIONS
                    || (upstream_saw(b"\x01") >= share && junk_subscriber_is_gone(&junk_subscriber))
            })
            .await;
            honest.subscribe("news").expect("subscribe");
            let mut publishing = |message: Multipart| {
                publish.publish(message);
            };
            published_until_received(&mut publishing, &mut honest, "news 1").await;

            // The junk subscriber was refused, in the way this library has of
            // refusing a subscriber, and its connection ended on it.
            wait_for(|| junk_subscriber_is_gone(&junk_subscriber)).await;

            // Upstream was asked for no more of the junk than the share, and was
            // told to cancel all of it once its subscriber was gone.
            wait_for(|| upstream_saw(b"\x00") == share).await;
            assert_eq!(
                upstream_saw(b"\x01"),
                share,
                "no more than its share was forwarded"
            );
            assert!(
                seen.lock().expect("seen").contains(&b"\x01news".to_vec()),
                "the honest subscriber's topic was forwarded"
            );
            collecting.abort();
        }
        /// Claim: **a forwarder tells a publisher that restarted what it holds**,
        /// because the subscription it forwarded is in its XSUB's table and goes
        /// out again at every handshake. A forwarder that counts a prefix once
        /// and forwards it once would otherwise go deaf upstream the first time its
        /// publisher did, for every subscriber that already held the prefix.
        #[tokio::test]
        async fn a_forwarder_tells_a_restarted_publisher_what_it_holds() {
            let context = context();
            let mut upstream = XPubSocket::new(&context).expect("xpub");
            let upstream_at = bounded("bind upstream", upstream.bind("tcp://127.0.0.1:0"))
                .await
                .expect("bind")
                .to_string();
            let (downstream, _device) = forwarder(&context, &upstream_at, false).await;
            let mut subscriber = subscriber_of(&context, &downstream);
            subscriber.subscribe("news").expect("subscribe");
            let first = bounded("the subscription", upstream.recv())
                .await
                .expect("an event");
            assert_eq!(bytes_of(&first), b"\x01news");

            // The publisher goes, and a new one binds where it was.
            upstream.close();
            drop(upstream);
            let mut restarted = XPubSocket::new(&context).expect("xpub");
            bounded("the new publisher binds", async {
                while restarted.bind(&upstream_at).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            let again = bounded("the subscription again", restarted.recv())
                .await
                .expect("an event");
            assert_eq!(bytes_of(&again), b"\x01news");
        }
    }
}
