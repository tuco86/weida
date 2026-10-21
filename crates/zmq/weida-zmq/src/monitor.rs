//! `zmq_socket_monitor`'s event set, as a typed stream and as the
//! `inproc://` PAIR form.
//!
//! libzmq gives a socket's connection lifecycle to a PAIR socket bound on an
//! `inproc://` endpoint, two frames per event: a six-octet header and the
//! endpoint. That is what the zguide's Espresso recipe reads, so it has to
//! exist byte for byte. It is also a poor interface for a Rust caller, who
//! wants an enum.
//!
//! **Both, from one source.** [`MonitorEvent`] is the event; the engine
//! publishes it exactly once per transition; [`Monitor`] hands it to a Rust
//! caller as a value, and [`serve_pair`] renders the same value onto a PAIR
//! socket through [`MonitorEvent::encode`]. The wire form is decided in that
//! one method and nowhere else, so the typed stream and the `inproc://` form
//! cannot disagree about what happened — the same rule the subscription
//! forms follow, where the API form is enqueued once and the session alone
//! turns it into a command.
//!
//! # The wire form
//!
//! ```text
//! frame 1: event id, 2 octets, little endian
//!          value,    4 octets, little endian
//! frame 2: the endpoint, as text
//! frame 3: the reason, as text        (this library's addition, see below)
//! ```
//!
//! The first two frames are libzmq's `zmq_event_t` as it reaches the wire:
//! the struct is copied into the frame, so the octet order is the host's,
//! which on every platform this library runs on is little endian. A reader
//! written against libzmq reads those two frames and stops.
//!
//! **The third frame is a named addition, because of a named loss.** libzmq
//! puts a C `errno`, a reconnect interval or a `ZMQ_PROTOCOL_ERROR_*` code
//! in the four-octet value field. This library's errors are named rather
//! than numbered ([`crate::Error`]), so for a failure the value field
//! carries zero and the reason travels as a third frame — additive, the same
//! convention [`crate::zap`]'s `X-Local-Principal` frame uses, and invisible
//! to a two-frame reader. `ZMQ_EVENT_CONNECT_RETRIED` does carry its
//! interval in milliseconds, because that is a number and not an errno.
//!
//! # What is not here
//!
//! `ZMQ_EVENT_PIPES_STATS` and the `zmq_socket_monitor_versioned` family are
//! DRAFT in libzmq and absent here, which
//! [`crate::optiontable`]'s reasoning covers: a draft event no peer can rely
//! on is not an event this library invents a rendering for.

use std::fmt;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::pair::PairSocket;

/// `ZMQ_EVENT_CONNECTED`: an outbound connection was established.
pub const EVENT_CONNECTED: u16 = 0x0001;
/// `ZMQ_EVENT_CONNECT_DELAYED`: a connect could not complete at once.
pub const EVENT_CONNECT_DELAYED: u16 = 0x0002;
/// `ZMQ_EVENT_CONNECT_RETRIED`: a connect failed and will be retried after
/// the interval in the value field.
pub const EVENT_CONNECT_RETRIED: u16 = 0x0004;
/// `ZMQ_EVENT_LISTENING`: an endpoint was bound and is accepting.
pub const EVENT_LISTENING: u16 = 0x0008;
/// `ZMQ_EVENT_BIND_FAILED`: a bind was refused.
pub const EVENT_BIND_FAILED: u16 = 0x0010;
/// `ZMQ_EVENT_ACCEPTED`: an inbound connection was accepted.
pub const EVENT_ACCEPTED: u16 = 0x0020;
/// `ZMQ_EVENT_ACCEPT_FAILED`: an accept was refused.
pub const EVENT_ACCEPT_FAILED: u16 = 0x0040;
/// `ZMQ_EVENT_CLOSED`: a connection was closed by this side.
pub const EVENT_CLOSED: u16 = 0x0080;
/// `ZMQ_EVENT_CLOSE_FAILED`: a close reported an error.
pub const EVENT_CLOSE_FAILED: u16 = 0x0100;
/// `ZMQ_EVENT_DISCONNECTED`: an established connection ended.
pub const EVENT_DISCONNECTED: u16 = 0x0200;
/// `ZMQ_EVENT_MONITOR_STOPPED`: no further events will arrive.
pub const EVENT_MONITOR_STOPPED: u16 = 0x0400;
/// `ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL`.
pub const EVENT_HANDSHAKE_FAILED_NO_DETAIL: u16 = 0x0800;
/// `ZMQ_EVENT_HANDSHAKE_SUCCEEDED`: the security handshake completed, so
/// messages may flow.
pub const EVENT_HANDSHAKE_SUCCEEDED: u16 = 0x1000;
/// `ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL`.
pub const EVENT_HANDSHAKE_FAILED_PROTOCOL: u16 = 0x2000;
/// `ZMQ_EVENT_HANDSHAKE_FAILED_AUTH`: a ZAP handler refused the connection.
pub const EVENT_HANDSHAKE_FAILED_AUTH: u16 = 0x4000;

/// How many events a [`Monitor`] holds before the oldest are dropped.
///
/// Monitoring must never slow the engine down, so the sink drops rather than
/// blocks and says so: a [`Monitor`] that falls this far behind reports
/// [`Error::EAGAIN`] naming how many events it missed, which is a fact and
/// not a silence.
pub const MONITOR_CAPACITY: usize = 1024;

/// Which events a monitor asks for: `zmq_socket_monitor`'s `events`
/// argument, as a bit mask of the `EVENT_*` constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MonitorEvents(pub u16);

impl MonitorEvents {
    /// `ZMQ_EVENT_ALL`.
    pub const ALL: MonitorEvents = MonitorEvents(0xFFFF);

    /// Whether this mask asks for `id`.
    pub const fn contains(self, id: u16) -> bool {
        self.0 & id != 0
    }
}

/// One connection-lifecycle event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MonitorEvent {
    /// An outbound connection was established.
    Connected {
        /// The endpoint, as the application named it.
        endpoint: String,
    },
    /// A connect could not complete immediately, so a peer exists with no
    /// connection behind it yet — which is the state `ZMQ_IMMEDIATE` is
    /// about.
    ConnectDelayed {
        /// The endpoint.
        endpoint: String,
    },
    /// A connect attempt failed; the next one follows after `interval`.
    ConnectRetried {
        /// The endpoint.
        endpoint: String,
        /// `ZMQ_RECONNECT_IVL` as it stands after the backoff.
        interval: Duration,
    },
    /// An endpoint was bound and is accepting.
    Listening {
        /// The endpoint actually bound, wildcard port resolved.
        endpoint: String,
    },
    /// A bind was refused.
    BindFailed {
        /// The endpoint asked for.
        endpoint: String,
        /// Why, in this library's words.
        reason: String,
    },
    /// An inbound connection was accepted.
    Accepted {
        /// The endpoint it arrived on.
        endpoint: String,
    },
    /// An accept was refused — including by this socket's own peer ceiling.
    AcceptFailed {
        /// The endpoint.
        endpoint: String,
        /// Why.
        reason: String,
    },
    /// A connection was closed by this side.
    Closed {
        /// The endpoint.
        endpoint: String,
    },
    /// The security handshake completed; messages may flow.
    HandshakeSucceeded {
        /// The endpoint.
        endpoint: String,
    },
    /// The handshake failed for a reason that is neither the protocol nor
    /// authentication — a closed connection, a timeout, an I/O error.
    HandshakeFailedNoDetail {
        /// The endpoint.
        endpoint: String,
        /// Why.
        reason: String,
    },
    /// The peer violated ZMTP, or spoke something this socket cannot pair
    /// with.
    HandshakeFailedProtocol {
        /// The endpoint.
        endpoint: String,
        /// Why.
        reason: String,
    },
    /// A ZAP handler refused the connection, or a mechanism's own check did.
    HandshakeFailedAuth {
        /// The endpoint.
        endpoint: String,
        /// Why.
        reason: String,
    },
    /// An established connection ended.
    Disconnected {
        /// The endpoint.
        endpoint: String,
    },
    /// No further events will arrive on this monitor.
    MonitorStopped {
        /// The endpoint, empty because the socket rather than one connection
        /// is what stopped.
        endpoint: String,
    },
}

impl MonitorEvent {
    /// The `ZMQ_EVENT_*` id.
    pub const fn id(&self) -> u16 {
        match self {
            MonitorEvent::Connected { .. } => EVENT_CONNECTED,
            MonitorEvent::ConnectDelayed { .. } => EVENT_CONNECT_DELAYED,
            MonitorEvent::ConnectRetried { .. } => EVENT_CONNECT_RETRIED,
            MonitorEvent::Listening { .. } => EVENT_LISTENING,
            MonitorEvent::BindFailed { .. } => EVENT_BIND_FAILED,
            MonitorEvent::Accepted { .. } => EVENT_ACCEPTED,
            MonitorEvent::AcceptFailed { .. } => EVENT_ACCEPT_FAILED,
            MonitorEvent::Closed { .. } => EVENT_CLOSED,
            MonitorEvent::HandshakeSucceeded { .. } => EVENT_HANDSHAKE_SUCCEEDED,
            MonitorEvent::HandshakeFailedNoDetail { .. } => EVENT_HANDSHAKE_FAILED_NO_DETAIL,
            MonitorEvent::HandshakeFailedProtocol { .. } => EVENT_HANDSHAKE_FAILED_PROTOCOL,
            MonitorEvent::HandshakeFailedAuth { .. } => EVENT_HANDSHAKE_FAILED_AUTH,
            MonitorEvent::Disconnected { .. } => EVENT_DISCONNECTED,
            MonitorEvent::MonitorStopped { .. } => EVENT_MONITOR_STOPPED,
        }
    }

    /// The endpoint this event is about.
    pub fn endpoint(&self) -> &str {
        match self {
            MonitorEvent::Connected { endpoint }
            | MonitorEvent::ConnectDelayed { endpoint }
            | MonitorEvent::ConnectRetried { endpoint, .. }
            | MonitorEvent::Listening { endpoint }
            | MonitorEvent::BindFailed { endpoint, .. }
            | MonitorEvent::Accepted { endpoint }
            | MonitorEvent::AcceptFailed { endpoint, .. }
            | MonitorEvent::Closed { endpoint }
            | MonitorEvent::HandshakeSucceeded { endpoint }
            | MonitorEvent::HandshakeFailedNoDetail { endpoint, .. }
            | MonitorEvent::HandshakeFailedProtocol { endpoint, .. }
            | MonitorEvent::HandshakeFailedAuth { endpoint, .. }
            | MonitorEvent::Disconnected { endpoint }
            | MonitorEvent::MonitorStopped { endpoint } => endpoint,
        }
    }

    /// Why, where this library has a reason to give. Empty for the events
    /// that are not failures.
    pub fn reason(&self) -> &str {
        match self {
            MonitorEvent::BindFailed { reason, .. }
            | MonitorEvent::AcceptFailed { reason, .. }
            | MonitorEvent::HandshakeFailedNoDetail { reason, .. }
            | MonitorEvent::HandshakeFailedProtocol { reason, .. }
            | MonitorEvent::HandshakeFailedAuth { reason, .. } => reason,
            _ => "",
        }
    }

    /// The four-octet value field: the reconnect interval in milliseconds
    /// for `ZMQ_EVENT_CONNECT_RETRIED`, and zero everywhere else — see the
    /// module documentation for why a failure's value is not an errno here.
    pub const fn value(&self) -> u32 {
        match self {
            MonitorEvent::ConnectRetried { interval, .. } => interval.as_millis() as u32,
            _ => 0,
        }
    }

    /// The wire form. **The only place it is decided.**
    pub fn encode(&self) -> Multipart {
        let mut header = Vec::with_capacity(6);
        header.extend_from_slice(&self.id().to_le_bytes());
        header.extend_from_slice(&self.value().to_le_bytes());
        let mut message = Multipart::single(header);
        message.push(self.endpoint().as_bytes().to_vec());
        let reason = self.reason();
        if !reason.is_empty() {
            message.push(reason.as_bytes().to_vec());
        }
        message
    }

    /// Reads the wire form back.
    ///
    /// What a reader on the other end of the PAIR socket does, and what makes
    /// [`encode`](Self::encode) checkable in both directions rather than
    /// asserted once.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` for a message that is not two or three frames, a
    /// header that is not six octets, or an event id this library does not
    /// know.
    pub fn decode(message: &Multipart) -> Result<MonitorEvent> {
        let frames = message.frames();
        if !(2..=3).contains(&frames.len()) {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "a monitor event is two frames, three with a reason; this one has {}",
                    frames.len()
                )
                .into(),
            ));
        }
        let header = frames[0].as_slice();
        if header.len() != 6 {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "a monitor event header is six octets; this one has {}",
                    header.len()
                )
                .into(),
            ));
        }
        let id = u16::from_le_bytes([header[0], header[1]]);
        let value = u32::from_le_bytes([header[2], header[3], header[4], header[5]]);
        let endpoint = String::from_utf8_lossy(frames[1].as_slice()).into_owned();
        let reason = frames
            .get(2)
            .map(|frame| String::from_utf8_lossy(frame.as_slice()).into_owned())
            .unwrap_or_default();
        Ok(match id {
            EVENT_CONNECTED => MonitorEvent::Connected { endpoint },
            EVENT_CONNECT_DELAYED => MonitorEvent::ConnectDelayed { endpoint },
            EVENT_CONNECT_RETRIED => MonitorEvent::ConnectRetried {
                endpoint,
                interval: Duration::from_millis(u64::from(value)),
            },
            EVENT_LISTENING => MonitorEvent::Listening { endpoint },
            EVENT_BIND_FAILED => MonitorEvent::BindFailed { endpoint, reason },
            EVENT_ACCEPTED => MonitorEvent::Accepted { endpoint },
            EVENT_ACCEPT_FAILED => MonitorEvent::AcceptFailed { endpoint, reason },
            EVENT_CLOSED => MonitorEvent::Closed { endpoint },
            EVENT_HANDSHAKE_SUCCEEDED => MonitorEvent::HandshakeSucceeded { endpoint },
            EVENT_HANDSHAKE_FAILED_NO_DETAIL => {
                MonitorEvent::HandshakeFailedNoDetail { endpoint, reason }
            }
            EVENT_HANDSHAKE_FAILED_PROTOCOL => {
                MonitorEvent::HandshakeFailedProtocol { endpoint, reason }
            }
            EVENT_HANDSHAKE_FAILED_AUTH => MonitorEvent::HandshakeFailedAuth { endpoint, reason },
            EVENT_DISCONNECTED => MonitorEvent::Disconnected { endpoint },
            EVENT_MONITOR_STOPPED => MonitorEvent::MonitorStopped { endpoint },
            other => {
                return Err(Error::ENOCOMPATPROTO(
                    format!("{other:#06X} is not a monitor event id this library knows").into(),
                ));
            }
        })
    }

    /// The event a failed handshake is, judged from the error the session
    /// reported.
    ///
    /// libzmq splits the failure three ways and this is the same split
    /// against this library's errno vocabulary: a refusal is
    /// authentication, a violation or an incompatible peer is the protocol,
    /// and everything else has no detail.
    pub fn handshake_failure(endpoint: String, error: &Error) -> MonitorEvent {
        let reason = error.to_string();
        match error {
            Error::EACCES(_) => MonitorEvent::HandshakeFailedAuth { endpoint, reason },
            Error::ENOCOMPATPROTO(_) | Error::EMSGSIZE(_) => {
                MonitorEvent::HandshakeFailedProtocol { endpoint, reason }
            }
            _ => MonitorEvent::HandshakeFailedNoDetail { endpoint, reason },
        }
    }
}

impl fmt::Display for MonitorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.id() {
            EVENT_CONNECTED => "CONNECTED",
            EVENT_CONNECT_DELAYED => "CONNECT_DELAYED",
            EVENT_CONNECT_RETRIED => "CONNECT_RETRIED",
            EVENT_LISTENING => "LISTENING",
            EVENT_BIND_FAILED => "BIND_FAILED",
            EVENT_ACCEPTED => "ACCEPTED",
            EVENT_ACCEPT_FAILED => "ACCEPT_FAILED",
            EVENT_CLOSED => "CLOSED",
            EVENT_HANDSHAKE_SUCCEEDED => "HANDSHAKE_SUCCEEDED",
            EVENT_HANDSHAKE_FAILED_NO_DETAIL => "HANDSHAKE_FAILED_NO_DETAIL",
            EVENT_HANDSHAKE_FAILED_PROTOCOL => "HANDSHAKE_FAILED_PROTOCOL",
            EVENT_HANDSHAKE_FAILED_AUTH => "HANDSHAKE_FAILED_AUTH",
            EVENT_DISCONNECTED => "DISCONNECTED",
            _ => "MONITOR_STOPPED",
        };
        write!(f, "{name} {}", self.endpoint())?;
        if !self.reason().is_empty() {
            write!(f, ": {}", self.reason())?;
        }
        Ok(())
    }
}

/// Where the engine publishes events, and the mask one monitor asked for.
///
/// Cloned into every connection task. Publishing costs one lock and, with no
/// monitor installed, nothing else: [`publish`](Self::publish) takes a
/// closure, so an event nobody asked for is never built.
#[derive(Clone, Default)]
pub struct MonitorSink {
    inner: std::sync::Arc<std::sync::Mutex<Option<Installed>>>,
}

struct Installed {
    events: MonitorEvents,
    sender: broadcast::Sender<MonitorEvent>,
}

impl fmt::Debug for MonitorSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let installed = self.inner.lock().map(|slot| slot.is_some());
        f.debug_struct("MonitorSink")
            .field("installed", &installed.unwrap_or(false))
            .finish()
    }
}

impl MonitorSink {
    /// Installs a monitor for `events`, replacing any previous one — which
    /// gets a `ZMQ_EVENT_MONITOR_STOPPED` first, because a replaced monitor
    /// that simply went quiet would look like a socket with nothing to say.
    pub fn install(&self, events: MonitorEvents) -> Monitor {
        let (sender, receiver) = broadcast::channel(MONITOR_CAPACITY);
        let mut slot = self.inner.lock().expect("the monitor sink");
        if let Some(previous) = slot.take() {
            let _ = previous.sender.send(MonitorEvent::MonitorStopped {
                endpoint: String::new(),
            });
        }
        *slot = Some(Installed {
            events,
            sender: sender.clone(),
        });
        Monitor { receiver }
    }

    /// Publishes one event, if a monitor asked for it.
    ///
    /// The closure is what keeps an unmonitored socket free: no endpoint
    /// string is formatted for an event nobody reads.
    pub fn publish(&self, id: u16, event: impl FnOnce() -> MonitorEvent) {
        let slot = self.inner.lock().expect("the monitor sink");
        if let Some(installed) = slot.as_ref()
            && installed.events.contains(id)
        {
            // An error here is "nobody is listening any more", which is not
            // the engine's problem.
            let _ = installed.sender.send(event());
        }
    }

    /// Ends the monitor: a last `ZMQ_EVENT_MONITOR_STOPPED` and no more.
    pub fn stop(&self) {
        let mut slot = self.inner.lock().expect("the monitor sink");
        if let Some(installed) = slot.take()
            && installed.events.contains(EVENT_MONITOR_STOPPED)
        {
            let _ = installed.sender.send(MonitorEvent::MonitorStopped {
                endpoint: String::new(),
            });
        }
    }
}

/// A socket's event stream, typed.
///
/// The Rust rendering of `zmq_socket_monitor`. [`serve_pair`] is the other
/// one, and both read the same published event.
#[derive(Debug)]
pub struct Monitor {
    receiver: broadcast::Receiver<MonitorEvent>,
}

impl Monitor {
    /// The next event.
    ///
    /// # Errors
    ///
    /// `EAGAIN` naming how many events were dropped when this monitor fell
    /// [`MONITOR_CAPACITY`] behind — monitoring never blocks the engine, so
    /// falling behind costs events and says so. `ENOTSOCK` once the socket
    /// is gone and no further event can arrive.
    pub async fn recv(&mut self) -> Result<MonitorEvent> {
        match self.receiver.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(missed)) => Err(Error::EAGAIN(
                format!("this monitor fell behind and missed {missed} events").into(),
            )),
            Err(broadcast::error::RecvError::Closed) => Err(Error::ENOTSOCK(
                "the monitored socket is gone, so no further event can arrive".into(),
            )),
        }
    }

    /// A second reader of the same stream, from here on.
    ///
    /// What makes the two renderings one source: [`serve_pair`] takes one of
    /// these while the application keeps the typed one, and both see the same
    /// published events rather than two emitters that can drift. Events
    /// published before this call are not repeated.
    pub fn resubscribe(&self) -> Monitor {
        Monitor {
            receiver: self.receiver.resubscribe(),
        }
    }

    /// The next event if one is queued, without waiting.
    ///
    /// # Errors
    ///
    /// `EAGAIN` when nothing is queued or when events were missed, and
    /// `ENOTSOCK` when the socket is gone.
    pub fn try_recv(&mut self) -> Result<MonitorEvent> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(event),
            Err(broadcast::error::TryRecvError::Empty) => {
                Err(Error::EAGAIN("no monitor event is queued".into()))
            }
            Err(broadcast::error::TryRecvError::Lagged(missed)) => Err(Error::EAGAIN(
                format!("this monitor fell behind and missed {missed} events").into(),
            )),
            Err(broadcast::error::TryRecvError::Closed) => Err(Error::ENOTSOCK(
                "the monitored socket is gone, so no further event can arrive".into(),
            )),
        }
    }
}

/// Renders a monitor onto a PAIR socket: libzmq's `inproc://` form, which is
/// what the Espresso recipe reads.
///
/// The caller binds the PAIR socket wherever libzmq's caller would have
/// passed an endpoint to `zmq_socket_monitor`, and the reader connects its
/// own PAIR socket there. Runs until the socket is gone or the peer closes.
///
/// # Errors
///
/// Whatever the PAIR socket reports; `ENOTSOCK` when the monitored socket is
/// gone is the normal ending and is reported as `Ok(())`.
pub async fn serve_pair(mut monitor: Monitor, pair: &mut PairSocket) -> Result<()> {
    loop {
        match monitor.recv().await {
            Ok(event) => pair.send(event.encode()).await?,
            // The socket ended, which ends this too. A lagging monitor is
            // not fatal: the missed events are reported by the next
            // successful read on the typed side, and this side keeps
            // rendering what it does get.
            Err(Error::ENOTSOCK(_)) => return Ok(()),
            Err(Error::EAGAIN(_)) => continue,
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_event() -> Vec<MonitorEvent> {
        let endpoint = "tcp://127.0.0.1:5555".to_owned();
        let reason = "because".to_owned();
        vec![
            MonitorEvent::Connected {
                endpoint: endpoint.clone(),
            },
            MonitorEvent::ConnectDelayed {
                endpoint: endpoint.clone(),
            },
            MonitorEvent::ConnectRetried {
                endpoint: endpoint.clone(),
                interval: Duration::from_millis(250),
            },
            MonitorEvent::Listening {
                endpoint: endpoint.clone(),
            },
            MonitorEvent::BindFailed {
                endpoint: endpoint.clone(),
                reason: reason.clone(),
            },
            MonitorEvent::Accepted {
                endpoint: endpoint.clone(),
            },
            MonitorEvent::AcceptFailed {
                endpoint: endpoint.clone(),
                reason: reason.clone(),
            },
            MonitorEvent::Closed {
                endpoint: endpoint.clone(),
            },
            MonitorEvent::HandshakeSucceeded {
                endpoint: endpoint.clone(),
            },
            MonitorEvent::HandshakeFailedNoDetail {
                endpoint: endpoint.clone(),
                reason: reason.clone(),
            },
            MonitorEvent::HandshakeFailedProtocol {
                endpoint: endpoint.clone(),
                reason: reason.clone(),
            },
            MonitorEvent::HandshakeFailedAuth {
                endpoint: endpoint.clone(),
                reason,
            },
            MonitorEvent::Disconnected { endpoint },
            MonitorEvent::MonitorStopped {
                endpoint: String::new(),
            },
        ]
    }

    /// Claim: the wire form is libzmq's — a six-octet little-endian header
    /// and the endpoint — and every event of the set goes both ways through
    /// it, which is what makes the typed stream and the `inproc://` form one
    /// thing rather than two.
    #[test]
    fn every_event_goes_both_ways_through_the_wire_form() {
        let events = every_event();
        assert_eq!(events.len(), 14, "libzmq's stable event set");
        let mut ids = Vec::new();
        for event in &events {
            let message = event.encode();
            let header = message.frames()[0].as_slice();
            assert_eq!(header.len(), 6, "{event}");
            assert_eq!(
                u16::from_le_bytes([header[0], header[1]]),
                event.id(),
                "{event}"
            );
            assert_eq!(message.frames()[1].as_slice(), event.endpoint().as_bytes());
            assert_eq!(
                message.len(),
                if event.reason().is_empty() { 2 } else { 3 },
                "{event}: the reason frame is there only when there is one"
            );
            assert_eq!(&MonitorEvent::decode(&message).expect("decode"), event);
            ids.push(event.id());
        }
        // Every id is distinct and a single bit, because they are a mask.
        ids.sort_unstable();
        let count = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), count);
        assert!(ids.iter().all(|id| id.count_ones() == 1));
    }

    /// Claim: `ZMQ_EVENT_CONNECT_RETRIED` carries its interval in the value
    /// field, in milliseconds, as libzmq does.
    #[test]
    fn the_retry_interval_travels_in_the_value_field() {
        let event = MonitorEvent::ConnectRetried {
            endpoint: "tcp://host:1".to_owned(),
            interval: Duration::from_millis(1234),
        };
        assert_eq!(event.value(), 1234);
        let decoded = MonitorEvent::decode(&event.encode()).expect("decode");
        assert_eq!(decoded, event);
    }

    /// Claim: a malformed monitor message is refused rather than guessed at.
    #[test]
    fn a_malformed_event_is_refused() {
        let one_frame = Multipart::single(vec![0u8; 6]);
        assert!(MonitorEvent::decode(&one_frame).is_err());
        let mut short_header = Multipart::single(vec![0u8; 5]);
        short_header.push(Vec::new());
        assert!(MonitorEvent::decode(&short_header).is_err());
        let mut unknown = Multipart::single(vec![0xFF, 0xFF, 0, 0, 0, 0]);
        unknown.push(Vec::new());
        assert!(MonitorEvent::decode(&unknown).is_err());
    }

    /// Claim: the sink publishes only what the mask asks for, builds nothing
    /// for an event nobody asked for, and a replaced monitor is told it was
    /// replaced.
    #[test]
    fn the_mask_decides_what_is_published() {
        let sink = MonitorSink::default();
        // With nothing installed the closure is never called.
        sink.publish(EVENT_CONNECTED, || panic!("built an unwanted event"));

        let mut monitor = sink.install(MonitorEvents(EVENT_LISTENING | EVENT_ACCEPTED));
        sink.publish(EVENT_CONNECTED, || panic!("built an unwanted event"));
        sink.publish(EVENT_LISTENING, || MonitorEvent::Listening {
            endpoint: "tcp://127.0.0.1:1".to_owned(),
        });
        assert_eq!(
            monitor.try_recv().expect("an event"),
            MonitorEvent::Listening {
                endpoint: "tcp://127.0.0.1:1".to_owned()
            }
        );
        assert!(monitor.try_recv().is_err(), "and nothing else");

        // A second install replaces the first, which learns of it.
        let _second = sink.install(MonitorEvents::ALL);
        assert_eq!(
            monitor.try_recv().expect("a stop"),
            MonitorEvent::MonitorStopped {
                endpoint: String::new()
            }
        );
    }

    /// Claim: a handshake failure is split the way libzmq splits it, from
    /// this library's own errno vocabulary.
    #[test]
    fn a_handshake_failure_is_classified() {
        let endpoint = "tcp://127.0.0.1:1".to_owned();
        for (error, id) in [
            (Error::EACCES("refused".into()), EVENT_HANDSHAKE_FAILED_AUTH),
            (
                Error::ENOCOMPATPROTO("bad".into()),
                EVENT_HANDSHAKE_FAILED_PROTOCOL,
            ),
            (
                Error::ETIMEDOUT("slow".into()),
                EVENT_HANDSHAKE_FAILED_NO_DETAIL,
            ),
        ] {
            let event = MonitorEvent::handshake_failure(endpoint.clone(), &error);
            assert_eq!(event.id(), id, "{error}");
            assert!(!event.reason().is_empty(), "{error}");
        }
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: a real socket's lifecycle reaches **both** renderings, and
        /// they agree — the typed stream and the two-frame `inproc://` PAIR form
        /// carry the same events in the same order, because there is one
        /// publisher behind them.
        #[tokio::test]
        async fn a_sockets_lifecycle_reaches_both_renderings() {
            use crate::context::{Context, ContextConfig};
            use crate::pipeline::{PullSocket, PushSocket};

            let context = Context::new(ContextConfig::default()).expect("context");
            let puller = PullSocket::new(&context).expect("pull");
            let mut typed = puller.monitor(MonitorEvents::ALL);

            // The inproc PAIR form of the same stream, bound where libzmq's
            // caller would have passed an endpoint to zmq_socket_monitor.
            let rendered = typed.resubscribe();
            let mut publisher = PairSocket::new(&context).expect("pair");
            publisher
                .bind("inproc://monitor.pull")
                .await
                .expect("bind the monitor endpoint");
            let mut reader = PairSocket::new(&context).expect("pair");
            reader
                .connect("inproc://monitor.pull")
                .expect("connect to the monitor endpoint");
            tokio::spawn(async move {
                let _ = serve_pair(rendered, &mut publisher).await;
            });

            let bound = puller.bind("tcp://127.0.0.1:0").await.expect("bind");
            let pusher = PushSocket::new(&context).expect("push");
            pusher.connect(&bound.to_string()).expect("connect");

            // The binder's side: listening, then a connection accepted, then its
            // handshake.
            let mut seen = Vec::new();
            while seen.len() < 3 {
                let event = tokio::time::timeout(Duration::from_secs(10), typed.recv())
                    .await
                    .expect("an event")
                    .expect("an event");
                seen.push(event);
            }
            assert_eq!(
                seen.iter().map(MonitorEvent::id).collect::<Vec<_>>(),
                vec![EVENT_LISTENING, EVENT_ACCEPTED, EVENT_HANDSHAKE_SUCCEEDED],
                "{seen:?}"
            );
            assert_eq!(seen[0].endpoint(), bound.to_string());

            // The same three, off the PAIR socket, decoded from the octets.
            for expected in &seen {
                let message = tokio::time::timeout(Duration::from_secs(10), reader.recv())
                    .await
                    .expect("a rendered event")
                    .expect("a message");
                assert_eq!(
                    &MonitorEvent::decode(&message).expect("decode"),
                    expected,
                    "the two renderings disagree"
                );
            }

            // Closing the socket ends the monitor, which is the last event a
            // reader sees rather than a stream that simply stops.
            puller.close();
            let mut stopped = false;
            while let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_secs(10), typed.recv()).await
            {
                if event.id() == EVENT_MONITOR_STOPPED {
                    stopped = true;
                    break;
                }
            }
            assert!(stopped, "the monitor said when it stopped");
        }
    }
}
