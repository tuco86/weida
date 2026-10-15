//! Sending and receiving, once, for whichever socket types have them.
//!
//! Eleven socket types do not have eleven sends. They have four shapes, and
//! `zmq_socket(3)`'s "action in mute state" column is what tells them apart:
//!
//! | Shape | Socket types | What a full queue does |
//! | --- | --- | --- |
//! | [`Transmit`] | REQ, DEALER, PUSH, PAIR | Blocks, bounded by `ZMQ_SNDTIMEO`; never discards. |
//! | [`Report`] | REP, ROUTER | Drops, and says so — the drop is the return value. |
//! | [`Publish`] | PUB, XPUB, XSUB | Fans out to the matching subscribers, dropping for any whose queue is full. A publisher never blocks. |
//! | [`Receive`] | everything except PUB and PUSH | Blocks, bounded by `ZMQ_RCVTIMEO`. |
//!
//! Each shape is a trait here and a generic function over it, so the eleven
//! Python classes are eleven one-line delegations rather than eleven
//! implementations that could disagree. **No protocol behaviour is decided in
//! this file**: the mute actions, REQ's alternation, ROUTER's routing table
//! and the timeouts are `weida-zmq`'s, called rather than re-derived.
//!
//! # `_nowait` is synchronous, and that is not a shortcut
//!
//! `ZMQ_DONTWAIT` means "do not wait", so `send_nowait` and `recv_nowait` are
//! ordinary Python methods rather than coroutines: there is nothing to await,
//! the library's `try_send`/`try_recv` do not touch the reactor, and making
//! them coroutines would add a loop round trip to the one operation whose
//! entire point is not to have one. They report `EAGAIN` when the socket is
//! busy with another coroutine's operation, which is the same answer as a full
//! queue and for the same reason: this call was told not to wait.

use std::time::Duration;

use pyo3::prelude::*;
use weida_py_core::{Bridge, Errno, Live, Retirable, duration};
use weida_zmq::{Multipart, Published, Result, Sent};

use crate::errors::{errno_of, retired, to_py};
use crate::values::{PyMultipart, PyPublished, PySent, message_from};

/// A socket type that receives: blocking, bounded, or not at all.
pub trait Receive: Send + 'static {
    /// `zmq_recv`, bounded by `ZMQ_RCVTIMEO`.
    fn receive(&mut self) -> impl Future<Output = Result<Multipart>> + Send;
    /// `zmq_recv` under an explicit bound.
    fn receive_within(&mut self, limit: Duration)
    -> impl Future<Output = Result<Multipart>> + Send;
    /// `ZMQ_DONTWAIT`.
    fn receive_now(&mut self) -> Result<Multipart>;
}

/// A socket type whose send blocks rather than discards.
pub trait Transmit: Send + 'static {
    /// `zmq_send`, bounded by `ZMQ_SNDTIMEO`.
    fn transmit(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send;
    /// `zmq_send` under an explicit bound.
    fn transmit_within(
        &mut self,
        message: Multipart,
        limit: Duration,
    ) -> impl Future<Output = Result<()>> + Send;
    /// `ZMQ_DONTWAIT`.
    fn transmit_now(&mut self, message: Multipart) -> Result<()>;
}

/// A socket type whose send reports what became of the message.
pub trait Report: Send + 'static {
    /// `zmq_send`, whose drop is a return value here.
    fn transmit(&mut self, message: Multipart) -> impl Future<Output = Result<Sent>> + Send;
}

/// A reporting socket type that also has libzmq's `ZMQ_DONTWAIT` form.
///
/// ROUTER has one and REP does not, and that asymmetry is the library's
/// rather than an omission here: a REP reply is delivered to the peer that
/// asked or discarded because that peer is gone, so its `send` never waits
/// for room and a non-blocking variant of it would be the same call under a
/// second name.
pub trait ReportNow: Report {
    /// `ZMQ_DONTWAIT`.
    fn transmit_now(&mut self, message: Multipart) -> Result<Sent>;
}

/// A socket type that fans a message out to its subscribers.
pub trait Publish: Send + 'static {
    /// Never blocks, never fails: it reports what it reached.
    fn publish(&mut self, message: Multipart) -> Published;
}

/// A socket type that keeps a subscription set.
pub trait Subscribe: Send + 'static {
    /// Adds a prefix. Subscriptions are additive and not idempotent: two
    /// subscriptions to one prefix need two cancellations.
    fn subscribe(&mut self, prefix: &[u8]) -> Result<()>;
    /// Removes one subscription to a prefix.
    fn unsubscribe(&mut self, prefix: &[u8]) -> Result<()>;
}

macro_rules! receives {
    ($($socket:path),+ $(,)?) => {
        $(impl Receive for $socket {
            fn receive(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
                self.recv()
            }

            fn receive_within(
                &mut self,
                limit: Duration,
            ) -> impl Future<Output = Result<Multipart>> + Send {
                self.recv_timeout(limit)
            }

            fn receive_now(&mut self) -> Result<Multipart> {
                self.try_recv()
            }
        })+
    };
}

receives!(
    weida_zmq::ReqSocket,
    weida_zmq::RepSocket,
    weida_zmq::DealerSocket,
    weida_zmq::RouterSocket,
    weida_zmq::SubSocket,
    weida_zmq::XPubSocket,
    weida_zmq::XSubSocket,
    weida_zmq::PullSocket,
    weida_zmq::PairSocket,
);

// The receiving halves `split` hands out. Their methods are the whole
// socket's under the same names, so the shape is the same delegation.
receives!(
    weida_zmq::split::DealerRecv,
    weida_zmq::split::RouterRecv,
    weida_zmq::split::PairRecv,
    weida_zmq::split::XPubRecv,
    weida_zmq::split::XSubRecv,
);

macro_rules! transmits {
    ($($socket:path),+ $(,)?) => {
        $(impl Transmit for $socket {
            fn transmit(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
                self.send(message)
            }

            fn transmit_within(
                &mut self,
                message: Multipart,
                limit: Duration,
            ) -> impl Future<Output = Result<()>> + Send {
                self.send_timeout(message, limit)
            }

            fn transmit_now(&mut self, message: Multipart) -> Result<()> {
                self.try_send(message)
            }
        })+
    };
}

transmits!(
    weida_zmq::ReqSocket,
    weida_zmq::DealerSocket,
    weida_zmq::PushSocket,
    weida_zmq::PairSocket,
);

transmits!(weida_zmq::split::DealerSend, weida_zmq::split::PairSend);

macro_rules! reports {
    ($($socket:path),+ $(,)?) => {
        $(impl Report for $socket {
            fn transmit(&mut self, message: Multipart) -> impl Future<Output = Result<Sent>> + Send {
                self.send(message)
            }
        })+
    };
}

reports!(
    weida_zmq::RepSocket,
    weida_zmq::RouterSocket,
    weida_zmq::split::RouterSend,
);

impl ReportNow for weida_zmq::RouterSocket {
    fn transmit_now(&mut self, message: Multipart) -> Result<Sent> {
        self.try_send(message)
    }
}

impl ReportNow for weida_zmq::split::RouterSend {
    fn transmit_now(&mut self, message: Multipart) -> Result<Sent> {
        self.try_send(message)
    }
}

impl Publish for weida_zmq::PubSocket {
    fn publish(&mut self, message: Multipart) -> Published {
        weida_zmq::PubSocket::publish(self, message)
    }
}

impl Publish for weida_zmq::XPubSocket {
    fn publish(&mut self, message: Multipart) -> Published {
        weida_zmq::XPubSocket::publish(self, message)
    }
}

impl Publish for weida_zmq::XSubSocket {
    /// XSUB's own name for it is `send`, because what an XSUB sends upstream
    /// is a subscription as often as a message; the shape is the publisher's.
    fn publish(&mut self, message: Multipart) -> Published {
        self.send(message)
    }
}

impl Publish for weida_zmq::split::XPubPublish {
    fn publish(&mut self, message: Multipart) -> Published {
        weida_zmq::split::XPubPublish::publish(self, message)
    }
}

impl Publish for weida_zmq::split::XSubSend {
    fn publish(&mut self, message: Multipart) -> Published {
        self.send(message)
    }
}

macro_rules! subscribes {
    ($($socket:path),+ $(,)?) => {
        $(impl Subscribe for $socket {
            fn subscribe(&mut self, prefix: &[u8]) -> Result<()> {
                <$socket>::subscribe(self, prefix)
            }

            fn unsubscribe(&mut self, prefix: &[u8]) -> Result<()> {
                <$socket>::unsubscribe(self, prefix)
            }
        })+
    };
}

subscribes!(
    weida_zmq::SubSocket,
    weida_zmq::XSubSocket,
    weida_zmq::XPubSocket,
    weida_zmq::split::XSubSend,
    weida_zmq::split::XPubRecv,
);

/// Applies the `ZMQ_SUBSCRIBE`/`ZMQ_UNSUBSCRIBE` rows a `SocketOptions`
/// collected, at construction.
///
/// libzmq's two subscription options are honoured by a *method* here rather
/// than by a field, and only three socket types have one. Both halves are
/// written out per socket type below rather than left to a blanket
/// implementation, so that a socket type which grows subscriptions later
/// cannot keep the refusal by accident.
pub trait ApplySubscriptions {
    /// Each prefix, `true` to subscribe and `false` to cancel.
    fn apply_subscriptions(&mut self, wanted: &[(Vec<u8>, bool)]) -> Result<()>;
}

macro_rules! applies_subscriptions {
    ($($socket:ident),+ $(,)?) => {
        $(impl ApplySubscriptions for weida_zmq::$socket {
            fn apply_subscriptions(&mut self, wanted: &[(Vec<u8>, bool)]) -> Result<()> {
                for (prefix, add) in wanted {
                    if *add {
                        Subscribe::subscribe(self, prefix)?;
                    } else {
                        Subscribe::unsubscribe(self, prefix)?;
                    }
                }
                Ok(())
            }
        })+
    };
}

macro_rules! refuses_subscriptions {
    ($($socket:ident),+ $(,)?) => {
        $(impl ApplySubscriptions for weida_zmq::$socket {
            fn apply_subscriptions(&mut self, wanted: &[(Vec<u8>, bool)]) -> Result<()> {
                if wanted.is_empty() {
                    return Ok(());
                }
                Err(weida_zmq::Error::EINVAL(
                    concat!(
                        "ZMQ_SUBSCRIBE and ZMQ_UNSUBSCRIBE belong to a subscribing socket; ",
                        stringify!($socket),
                        " has no subscriptions, and libzmq refuses them here too",
                    )
                    .into(),
                ))
            }
        })+
    };
}

applies_subscriptions!(SubSocket, XSubSocket, XPubSocket);
refuses_subscriptions!(
    ReqSocket,
    RepSocket,
    DealerSocket,
    RouterSocket,
    PubSocket,
    PushSocket,
    PullSocket,
    PairSocket,
);

/// Applies what a `SocketOptions` collected, at construction.
pub fn apply_subscriptions<S: ApplySubscriptions>(
    socket: &mut S,
    wanted: &[(Vec<u8>, bool)],
) -> Result<()> {
    socket.apply_subscriptions(wanted)
}

/// A timeout in seconds, as the library's `Duration`.
///
/// Refused where it is given rather than rounded: a negative, infinite or
/// absurdly large number of seconds is not a bound, and libzmq's `-1` for
/// "wait forever" is spelled `None` here because Python has a word for it.
fn limit(py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Duration>> {
    match timeout {
        None => Ok(None),
        Some(seconds) => duration("a timeout", seconds).map(Some).map_err(|bad| {
            to_py(
                py,
                &Errno::new(
                    "EINVAL",
                    format!("{bad}; omit it to wait as long as ZMQ_SNDTIMEO/ZMQ_RCVTIMEO allow"),
                ),
            )
        }),
    }
}

/// `EAGAIN` for a `_nowait` call that found the socket busy.
fn busy(py: Python<'_>) -> PyErr {
    to_py(
        py,
        &Errno::new(
            "EAGAIN",
            "another coroutine is using this socket, and this call was told not to wait",
        ),
    )
}

/// Takes the socket, waiting behind whoever has it.
///
/// # Errors
///
/// `ENOTSOCK` once the socket was split: its slot is retired and the socket
/// lives on in its halves.
pub async fn lease<S>(slot: &Retirable<S>) -> std::result::Result<Live<S>, Errno> {
    slot.acquire().await.map_err(retired)
}

/// [`lease`] without waiting: `None` when another coroutine has the socket.
///
/// # Errors
///
/// `ENOTSOCK` once the socket was split.
pub fn try_lease<S>(slot: &Retirable<S>) -> std::result::Result<Option<Live<S>>, Errno> {
    slot.try_acquire().map_err(retired)
}

/// `await sock.recv(timeout=None)`.
pub fn recv<'py, S: Receive>(
    py: Python<'py>,
    bridge: &Bridge,
    socket: &Retirable<S>,
    timeout: Option<f64>,
) -> PyResult<Bound<'py, PyAny>> {
    let limit = limit(py, timeout)?;
    let slot = socket.clone();
    bridge.awaitable(py, async move {
        let mut socket = lease(&slot).await?;
        let received = match limit {
            Some(limit) => socket.receive_within(limit).await,
            None => socket.receive().await,
        };
        received.map(PyMultipart::of).map_err(errno_of)
    })
}

/// `sock.recv_nowait()`.
pub fn recv_nowait<S: Receive>(py: Python<'_>, socket: &Retirable<S>) -> PyResult<PyMultipart> {
    let Some(mut socket) = try_lease(socket).map_err(|errno| to_py(py, &errno))? else {
        return Err(busy(py));
    };
    socket
        .receive_now()
        .map(PyMultipart::of)
        .map_err(|error| to_py(py, &errno_of(error)))
}

/// `await sock.send(message, timeout=None)` for a socket type that blocks.
pub fn send<'py, S: Transmit>(
    py: Python<'py>,
    bridge: &Bridge,
    socket: &Retirable<S>,
    message: &Bound<'py, PyAny>,
    timeout: Option<f64>,
) -> PyResult<Bound<'py, PyAny>> {
    let message = message_from(message)?;
    let limit = limit(py, timeout)?;
    let slot = socket.clone();
    bridge.awaitable(py, async move {
        let mut socket = lease(&slot).await?;
        match limit {
            Some(limit) => socket.transmit_within(message, limit).await,
            None => socket.transmit(message).await,
        }
        .map_err(errno_of)
    })
}

/// `sock.send_nowait(message)` for a socket type that blocks.
pub fn send_nowait<S: Transmit>(
    py: Python<'_>,
    socket: &Retirable<S>,
    message: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let message = message_from(message)?;
    let Some(mut socket) = try_lease(socket).map_err(|errno| to_py(py, &errno))? else {
        return Err(busy(py));
    };
    socket
        .transmit_now(message)
        .map_err(|error| to_py(py, &errno_of(error)))
}

/// `await sock.send(message)` for a socket type that reports a drop.
pub fn send_reporting<'py, S: Report>(
    py: Python<'py>,
    bridge: &Bridge,
    socket: &Retirable<S>,
    message: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let message = message_from(message)?;
    let slot = socket.clone();
    bridge.awaitable(py, async move {
        lease(&slot)
            .await?
            .transmit(message)
            .await
            .map(PySent::of)
            .map_err(errno_of)
    })
}

/// `sock.send_nowait(message)` for a socket type that reports a drop.
pub fn send_reporting_nowait<S: ReportNow>(
    py: Python<'_>,
    socket: &Retirable<S>,
    message: &Bound<'_, PyAny>,
) -> PyResult<PySent> {
    let message = message_from(message)?;
    let Some(mut socket) = try_lease(socket).map_err(|errno| to_py(py, &errno))? else {
        return Err(busy(py));
    };
    socket
        .transmit_now(message)
        .map(PySent::of)
        .map_err(|error| to_py(py, &errno_of(error)))
}

/// `await sock.send(message)` for a publisher.
pub fn publish<'py, S: Publish>(
    py: Python<'py>,
    bridge: &Bridge,
    socket: &Retirable<S>,
    message: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let message = message_from(message)?;
    let slot = socket.clone();
    bridge.awaitable(py, async move {
        Ok::<PyPublished, Errno>(PyPublished::of(lease(&slot).await?.publish(message)))
    })
}

/// `sock.send_nowait(message)` for a publisher, which never waits anyway.
pub fn publish_nowait<S: Publish>(
    py: Python<'_>,
    socket: &Retirable<S>,
    message: &Bound<'_, PyAny>,
) -> PyResult<PyPublished> {
    let message = message_from(message)?;
    let Some(mut socket) = try_lease(socket).map_err(|errno| to_py(py, &errno))? else {
        return Err(busy(py));
    };
    Ok(PyPublished::of(socket.publish(message)))
}

/// `await sock.subscribe(prefix)`.
pub fn subscribe<'py, S: Subscribe>(
    py: Python<'py>,
    bridge: &Bridge,
    socket: &Retirable<S>,
    prefix: Vec<u8>,
) -> PyResult<Bound<'py, PyAny>> {
    let slot = socket.clone();
    bridge.awaitable(py, async move {
        lease(&slot).await?.subscribe(&prefix).map_err(errno_of)
    })
}

/// `await sock.unsubscribe(prefix)`.
pub fn unsubscribe<'py, S: Subscribe>(
    py: Python<'py>,
    bridge: &Bridge,
    socket: &Retirable<S>,
    prefix: Vec<u8>,
) -> PyResult<Bound<'py, PyAny>> {
    let slot = socket.clone();
    bridge.awaitable(py, async move {
        lease(&slot).await?.unsubscribe(&prefix).map_err(errno_of)
    })
}
