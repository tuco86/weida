//! MQTT for a caller with no event loop.
//!
//! ```no_run
//! use std::time::Duration;
//! use weida_mqtt::blocking::BlockingContext;
//! use weida_mqtt::{ConnectOptions, Message, QoS, Subscription};
//!
//! # fn main() -> weida_mqtt::Result<()> {
//! let context = BlockingContext::owned(1)?;
//! let (client, mut deliveries) =
//!     context.connect("127.0.0.1:1883", ConnectOptions::new("sync-1"))?;
//!
//! client.subscribe(vec![Subscription::new("room/+", QoS::AtLeastOnce)])?;
//! client.publish(Message::new("room/12", "21.5").at(QoS::ExactlyOnce))?;
//!
//! // Where the asynchronous surface has cancellation, this has a deadline.
//! let delivery = deliveries.recv_timeout(Duration::from_secs(5))?;
//! println!("{} {:?}", delivery.topic, delivery.payload);
//!
//! client.disconnect(weida_mqtt::DisconnectReasonCode::NormalDisconnection)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Every method here is a `block_on` and nothing else
//!
//! **No protocol behaviour is decided twice.** The QoS state machines, the
//! session, the retransmission rule, the send quota, the topic aliases, the
//! keep-alive timer and every refusal are the asynchronous surface's, and this
//! module is a thin wrapper that drives its futures to completion. A
//! synchronous MQTT that re-derived any of them would be a second
//! implementation that could disagree with the first
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item 3).
//!
//! # Where the future is polled, and why not on the reactor
//!
//! Not the reactor's `block_on`: the reactor's threads are busy driving the
//! connection, and a caller that parked one of them would be waiting for a
//! worker that is waiting for it. `weida-runtime`'s contract is that "the
//! caller may drive the returned futures on any executor —
//! `futures::executor::block_on` included", and this is that sentence used:
//! the client's future is polled here, on the thread that asked, while the
//! connection task it waits for runs on the reactor. Two threads may
//! therefore each block on their own call.
//!
//! # Cancellation becomes a deadline
//!
//! The asynchronous surface's answer to "wait, but not forever" is to cancel
//! the task; a blocking caller has no task to cancel. So the one place a wait
//! is open-ended — reading the next delivery — takes a deadline instead:
//! [`Deliveries::recv_timeout`]. There is no `publish_timeout`, and the
//! omission is deliberate: a publish that is abandoned mid-exchange is still
//! **session state** on both sides, so a deadline there would return control
//! to the caller while the exchange continued — which is exactly what the
//! asynchronous surface's cancellation does and is honest about, and what a
//! method named `publish_timeout` would not be. A caller who wants that runs
//! the publish on its own thread.
//!
//! # One thread, and what an acknowledgement waits behind
//!
//! The pattern of the example above — publish, then read deliveries on the same
//! thread — is safe for QoS 0, whose `publish` returns once the packet has gone
//! out whatever the delivery queue holds: a full queue stops the socket being
//! read, and nothing else. A call that waits for an **acknowledgement** — a
//! QoS 1 or 2 publish, a subscribe — is answered by a packet that arrives
//! *behind* every delivery the server sent first, so with the queue full and
//! the only reader blocked in that call it cannot be read: the connection is
//! not wedged, its keep-alive and `disconnect` still run, but the call waits
//! for deliveries only this thread can take. A caller that mixes the two
//! drains [`Deliveries`] between calls, or from another thread.

use std::time::Duration;

use weida_runtime::Exec;

use crate::client::{Client, Context, Event, Events};
use crate::error::{Error, Result};
use crate::message::{Completion, Delivery, Message};
use crate::options::ConnectOptions;
use crate::session::Session;
use crate::{DisconnectReasonCode, SubackReasonCode, Subscription, UnsubackReasonCode};

/// Drives one future to completion on the calling thread.
fn drive<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

/// A context that owns its reactor, for a caller with no loop.
#[derive(Clone, Debug)]
pub struct BlockingContext {
    inner: Context,
}

impl BlockingContext {
    /// A context owning a reactor of `worker_threads` threads.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] where `worker_threads` is zero or the OS refuses
    /// the threads.
    pub fn owned(worker_threads: usize) -> Result<BlockingContext> {
        Ok(BlockingContext {
            inner: Context::owned(worker_threads)?,
        })
    }

    /// A blocking surface over a context that already exists, which is what
    /// lets one process hold both.
    #[must_use]
    pub fn from_context(inner: Context) -> BlockingContext {
        BlockingContext { inner }
    }

    /// The asynchronous context underneath.
    #[must_use]
    pub fn context(&self) -> &Context {
        &self.inner
    }

    /// Connects, and returns the client and its deliveries.
    ///
    /// # Errors
    ///
    /// Everything [`Client::connect`] reports.
    pub fn connect(
        &self,
        address: &str,
        options: ConnectOptions,
    ) -> Result<(BlockingClient, Deliveries)> {
        let (client, events) = drive(Client::connect(&self.inner, address, options))?;
        Ok((
            BlockingClient { inner: client },
            Deliveries {
                inner: events,
                exec: self.inner.exec().clone(),
            },
        ))
    }

    /// Connects on a session the caller holds, which is what makes a later
    /// reconnect a resumption.
    ///
    /// # Errors
    ///
    /// Everything [`Client::connect_session`] reports.
    pub fn connect_session(
        &self,
        address: &str,
        options: ConnectOptions,
        session: &Session,
    ) -> Result<(BlockingClient, Deliveries)> {
        let (client, events) = drive(Client::connect_session(
            &self.inner,
            address,
            options,
            session,
        ))?;
        Ok((
            BlockingClient { inner: client },
            Deliveries {
                inner: events,
                exec: self.inner.exec().clone(),
            },
        ))
    }
}

/// The application's end of one connection, synchronously.
#[derive(Debug)]
pub struct BlockingClient {
    inner: Client,
}

impl BlockingClient {
    /// Publishes, blocking until the hop has certified what it is going to.
    ///
    /// # Errors
    ///
    /// Everything [`Client::publish`] reports, including the refusals it
    /// makes before the packet reaches the wire.
    pub fn publish(&self, message: Message) -> Result<Completion> {
        drive(self.inner.publish(message))
    }

    /// Subscribes, blocking until the SUBACK arrives.
    ///
    /// # Errors
    ///
    /// Everything [`Client::subscribe`] reports.
    pub fn subscribe(&self, subscriptions: Vec<Subscription>) -> Result<Vec<SubackReasonCode>> {
        drive(self.inner.subscribe(subscriptions))
    }

    /// The same, with a `Subscription Identifier`.
    ///
    /// # Errors
    ///
    /// Everything [`Client::subscribe_with`] reports.
    pub fn subscribe_with(
        &self,
        subscriptions: Vec<Subscription>,
        identifier: Option<u32>,
    ) -> Result<Vec<SubackReasonCode>> {
        drive(self.inner.subscribe_with(subscriptions, identifier))
    }

    /// Unsubscribes, blocking until the UNSUBACK arrives.
    ///
    /// # Errors
    ///
    /// Everything [`Client::unsubscribe`] reports.
    pub fn unsubscribe(&self, filters: Vec<String>) -> Result<Vec<UnsubackReasonCode>> {
        drive(self.inner.unsubscribe(filters))
    }

    /// Sends DISCONNECT and closes.
    ///
    /// # Errors
    ///
    /// Everything [`Client::disconnect`] reports.
    pub fn disconnect(&self, reason_code: DisconnectReasonCode) -> Result<()> {
        drive(self.inner.disconnect(reason_code))
    }

    /// The same, revising the `Session Expiry Interval` at close.
    ///
    /// # Errors
    ///
    /// Everything [`Client::disconnect_with`] reports.
    pub fn disconnect_with(
        &self,
        reason_code: DisconnectReasonCode,
        session_expiry: Option<Duration>,
    ) -> Result<()> {
        drive(self.inner.disconnect_with(reason_code, session_expiry))
    }

    /// Sends PINGREQ now.
    ///
    /// # Errors
    ///
    /// Everything [`Client::ping`] reports.
    pub fn ping(&self) -> Result<()> {
        drive(self.inner.ping())
    }

    /// Re-authenticates on the live connection.
    ///
    /// # Errors
    ///
    /// Everything [`Client::reauthenticate`] reports.
    pub fn reauthenticate(&self) -> Result<()> {
        drive(self.inner.reauthenticate())
    }

    /// The asynchronous client underneath, so that a process holding both
    /// surfaces reads one connection's state through one object.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.inner
    }
}

/// The deliveries of one connection, synchronously.
#[derive(Debug)]
pub struct Deliveries {
    inner: Events,
    /// The reactor's handle, for `recv_timeout`'s deadline.
    ///
    /// `futures::executor::block_on` has no timer of its own, so a `tokio`
    /// sleep created on this thread would panic. `Exec::within` enters the
    /// runtime's handle before creating one, which is what makes the timer
    /// the runtime's rather than the caller's — and is the same reason the
    /// asynchronous surface's own deadlines go through it.
    exec: Exec,
}

impl Deliveries {
    /// The next delivery, blocking until one arrives.
    ///
    /// # Errors
    ///
    /// [`Error::ServerDisconnected`] or whatever else ended the connection —
    /// which is the distinction 5.0 exists to make and 3.1.1 could not — and
    /// [`Error::NotConnected`] where the connection task has finished and
    /// every event has been taken.
    pub fn recv(&mut self) -> Result<Delivery> {
        drive(next_delivery(&mut self.inner))
    }

    /// The next delivery, or [`Error::Timeout`] after `limit`.
    ///
    /// **This is where the asynchronous surface's cancellation goes.** A
    /// coroutine that wants to stop waiting cancels its task; a blocking
    /// caller has no task, so the deadline is the argument. The connection is
    /// untouched by the expiry: nothing was consumed, and the next call waits
    /// again.
    ///
    /// # Errors
    ///
    /// As [`Deliveries::recv`], plus [`Error::Timeout`] where `limit`
    /// elapsed first.
    pub fn recv_timeout(&mut self, limit: Duration) -> Result<Delivery> {
        let events = &mut self.inner;
        let exec = &self.exec;
        drive(async move {
            // The timer is the reactor's, which is what makes it a timer at
            // all: `futures::executor::block_on` has none of its own, and a
            // `tokio` sleep created on this thread would panic.
            match exec.within(limit, next_delivery(events)).await {
                Some(delivery) => delivery,
                None => Err(Error::Timeout("a delivery")),
            }
        })
    }

    /// The next event of any kind, blocking until one arrives.
    ///
    /// `None` once the connection task has finished and every event has been
    /// taken. Unlike [`Deliveries::recv`] this does not turn the connection
    /// ending into an error: the event carries it, which is what a caller
    /// driving its own reconnect wants.
    pub fn recv_event(&mut self) -> Option<Event> {
        drive(self.inner.next())
    }

    /// The asynchronous event stream underneath.
    #[must_use]
    pub fn events(&mut self) -> &mut Events {
        &mut self.inner
    }
}

/// The next delivery, skipping the events that are not one.
///
/// A resumed exchange's completion is not a delivery and is not an error
/// either; it is skipped here and available through
/// [`Deliveries::recv_event`], which is the same split the asynchronous
/// surface makes.
async fn next_delivery(events: &mut Events) -> Result<Delivery> {
    loop {
        match events.next().await {
            Some(Event::Delivered(delivery)) => return Ok(delivery),
            Some(Event::Disconnected(error)) => return Err(error),
            Some(_) => continue,
            None => return Err(Error::NotConnected),
        }
    }
}
