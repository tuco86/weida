//! The link: credit granted from Python, and a send that returns an outcome.
//!
//! Two things here define the binding and both are about *what an
//! await means*:
//!
//! * **`await send(...)` returns the delivery's terminal state**, not a
//!   boolean. On an unsettled link the coroutine completes when the peer's
//!   `disposition` has settled the delivery, so `await` means "the receiver
//!   has told me what happened" rather than "the octets left". On a
//!   `settled`-mode link there is no answer coming and `send` says so by
//!   returning `None` — which is the honest report, because the mode's whole
//!   cost is that nothing can be concluded.
//! * **credit is granted explicitly.** A receiver calls `grant_credit(n)` and
//!   nothing arrives before it does. No hidden auto-credit: AMQP's link credit
//!   is the receiver's instrument, and a binding that granted some behind the
//!   caller's back would be deciding its prefetch for it.
//!
//! # The delivery stream is an async iterator
//!
//! `async for delivery in link:` is the receive loop, and it ends with
//! `StopAsyncIteration` when the link detaches — which is what a detached link
//! is: no more deliveries, ever. A caller that needs to know *why* it ended
//! reads `state()` afterwards or catches the condition from `detach()`.

use std::sync::Arc;

use pyo3::prelude::*;
use weida_amqp::link::LinkState;
use weida_amqp::{Condition, Link};
use weida_py_core::Bridge;
use weida_runtime::OwnedReactor;

use crate::errors;
use crate::lease::Slot;
use crate::values::{
    Outgoing, PyDelivery, PyOutcome, message_from, outcome_of, receiver_mode_name, sender_mode_name,
};

/// `weida_amqp.Link`.
#[pyclass(frozen, name = "Link", module = "weida_amqp")]
pub struct PyLink {
    inner: Arc<Slot<Link>>,
    bridge: Bridge,
    #[allow(
        dead_code,
        reason = "the reactor is alive because this field is, and a link \
                  outliving its session's Python object must keep it so"
    )]
    reactor: Arc<OwnedReactor>,
}

impl PyLink {
    pub fn of(link: Link, bridge: Bridge, reactor: Arc<OwnedReactor>) -> PyLink {
        PyLink {
            inner: Slot::new(link),
            bridge,
            reactor,
        }
    }
}

#[pymethods]
impl PyLink {
    /// The link's name, which is its identity between the two containers.
    fn name<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(
            py,
            async move { Ok(slot.acquire().await.name().to_owned()) },
        )
    }

    /// `sender` or `receiver`.
    fn role<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            Ok(match slot.acquire().await.role() {
                weida_amqp_codec::Role::Sender => "sender",
                weida_amqp_codec::Role::Receiver => "receiver",
            }
            .to_owned())
        })
    }

    /// Our handle: the number this end writes into every frame it sends.
    fn output_handle<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge
            .awaitable(py, async move { Ok(slot.acquire().await.output_handle()) })
    }

    /// The peer's handle, or `None` before the answering `attach`. A different
    /// number in general, which is why the answer is correlated by name.
    fn input_handle<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge
            .awaitable(py, async move { Ok(slot.acquire().await.input_handle()) })
    }

    /// What the answering `attach` said, as a dict: the settle modes actually
    /// in force and the addresses the peer actually created.
    ///
    /// Worth reading rather than assuming. A broker may narrow
    /// `rcv-settle-mode` from `second` to `first`, and a caller that did not
    /// look would believe it had a guarantee it does not have.
    fn negotiated<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let link = slot.acquire().await;
            Ok(link.negotiated().map(|negotiated| Negotiated {
                snd_settle_mode: sender_mode_name(negotiated.snd_settle_mode).to_owned(),
                rcv_settle_mode: receiver_mode_name(negotiated.rcv_settle_mode).to_owned(),
                source_address: negotiated.remote_source.and_then(|source| source.address),
                target_address: negotiated.remote_target.and_then(|target| target.address),
                max_message_size: negotiated.remote_max_message_size,
                offered_capabilities: negotiated.remote_offered_capabilities,
            }))
        })
    }

    /// This link's credit: `delivery-count`, `link-credit`, `available` and
    /// `drain`.
    fn credit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let credit = slot.acquire().await.credit();
            Ok(Credit {
                delivery_count: credit.delivery_count(),
                link_credit: credit.link_credit(),
                available: credit.available(),
                drain: credit.drain(),
            })
        })
    }

    /// A **receiver**: grants credit for `credit` more messages.
    ///
    /// Sets rather than adds: what goes on the wire is the absolute limit
    /// `delivery-count + link-credit`, so granting 10 twice grants 10.
    fn grant_credit<'py>(&self, py: Python<'py>, credit: u32) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire()
                .await
                .grant_credit(credit)
                .await
                .map_err(errors::errno_of)
        })
    }

    /// A **receiver**: asks the sender to drain.
    ///
    /// What turns "wait for a message" into "wait for a definite answer":
    /// credit reaching zero with no delivery in between means there was
    /// nothing to get.
    fn drain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire().await.drain().await.map_err(errors::errno_of)
        })
    }

    /// A **receiver**: stops the link with `flow(link-credit=0, echo=true)`.
    ///
    /// The echo is the point: transfers already in flight may still arrive,
    /// and the echoed `flow` is the marker after which no further transfer
    /// will come.
    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire().await.stop().await.map_err(errors::errno_of)
        })
    }

    /// A **sender**: sends one message and waits for its terminal state.
    ///
    /// Returns an [`Outcome`](crate::values::PyOutcome), or `None` on a link
    /// whose `snd-settle-mode` is `settled` — where the sender forgets the
    /// delivery as it sends it and no answer is coming.
    ///
    /// `text=True` sends the body as an `amqp-value` of a string, which is
    /// what every broker's own example sends; the default sends `bytes` as a
    /// `data` section, which is what a payload is.
    #[pyo3(signature = (
        body,
        *,
        text=false,
        subject=None,
        message_id=None,
        correlation_id=None,
        content_type=None,
        reply_to=None,
        durable=false,
    ))]
    #[allow(clippy::too_many_arguments, reason = "one keyword argument per field")]
    fn send<'py>(
        &self,
        py: Python<'py>,
        body: &Bound<'py, PyAny>,
        text: bool,
        subject: Option<String>,
        message_id: Option<String>,
        correlation_id: Option<String>,
        content_type: Option<String>,
        reply_to: Option<String>,
        durable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Converted under the GIL, before the coroutine starts: the body must
        // outlive the call that made it, and a `&[u8]` borrowed from a Python
        // object does not.
        let payload = if text {
            body.extract::<String>()?.into_bytes()
        } else {
            weida_py_core::payload_of(body)?
        };
        // The message is encoded here rather than in the future, because
        // `Message` borrows its fields and a future must own what it carries.
        let encoded = {
            let message = message_from(
                py,
                &Outgoing {
                    body: &payload,
                    text,
                    subject: subject.as_deref(),
                    message_id: message_id.as_deref(),
                    correlation_id: correlation_id.as_deref(),
                    content_type: content_type.as_deref(),
                    reply_to: reply_to.as_deref(),
                    durable,
                },
            )?;
            errors::raise(py, message.to_vec().map_err(weida_amqp::Error::Encode))?
        };
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let mut link = slot.acquire().await;
            let sent = link
                .send_payload(&encoded)
                .await
                .map_err(errors::errno_of)?;
            if sent.settled {
                // Settled on send: there is nothing to wait for and nothing to
                // report. Saying `None` is the whole of what the mode proves.
                return Ok(None);
            }
            let outcome = link
                .settled(sent.delivery_id)
                .await
                .map_err(errors::errno_of)?;
            Ok(Some(PyOutcome::of(outcome)))
        })
    }

    /// A **sender**: sends one message without waiting for its outcome.
    ///
    /// Returns `(delivery_id, delivery_tag, frames)`. For a caller that wants
    /// many in flight and will wait for each with
    /// [`settled`](PyLink::settled); the credit and the window still bound how
    /// many are in flight, so this is a pipeline rather than a firehose.
    #[pyo3(signature = (body, *, text=false, durable=false))]
    fn send_nowait<'py>(
        &self,
        py: Python<'py>,
        body: &Bound<'py, PyAny>,
        text: bool,
        durable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload = if text {
            body.extract::<String>()?.into_bytes()
        } else {
            weida_py_core::payload_of(body)?
        };
        let encoded = {
            let message = message_from(
                py,
                &Outgoing {
                    body: &payload,
                    text,
                    durable,
                    ..Outgoing::default()
                },
            )?;
            errors::raise(py, message.to_vec().map_err(weida_amqp::Error::Encode))?
        };
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let sent = slot
                .acquire()
                .await
                .send_payload(&encoded)
                .await
                .map_err(errors::errno_of)?;
            Ok((sent.delivery_id, sent.delivery_tag, sent.frames))
        })
    }

    /// Waits for one delivery's terminal state.
    ///
    /// Raises `Configuration` for a delivery this end is not holding — settled
    /// on send, already settled, or never sent from here — because waiting for
    /// an answer that cannot arrive is the one thing a request API must not
    /// let a caller do.
    fn settled<'py>(&self, py: Python<'py>, delivery_id: u32) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let outcome = slot
                .acquire()
                .await
                .settled(delivery_id)
                .await
                .map_err(errors::errno_of)?;
            Ok(PyOutcome::of(outcome))
        })
    }

    /// The next delivery, or `None` once the link has detached.
    fn next_delivery<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            Ok(slot
                .acquire()
                .await
                .next_delivery()
                .await
                .map(PyDelivery::of))
        })
    }

    /// `async for delivery in link:`
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next delivery, ending the iteration when the link detaches.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            match slot.acquire().await.next_delivery().await {
                Some(delivery) => Ok(PyDelivery::of(delivery)),
                // A detached link delivers nothing further, which for an
                // `async for` is the end rather than a failure.
                None => Err(errors::end_of_stream("the link detached")),
            }
        })
    }

    /// A **receiver**: accepts a delivery.
    ///
    /// What it commits to: the message was processed and the sender may retire
    /// it. Not "on disk" — durability is asserted by `durable` on the way in,
    /// and a target that cannot honour it MUST refuse the message instead.
    fn accept<'py>(&self, py: Python<'py>, delivery_id: u32) -> PyResult<Bound<'py, PyAny>> {
        self.settle_one(py, delivery_id, weida_amqp::settlement::Outcome::Accepted)
    }

    /// A **receiver**: rejects a delivery as invalid and unprocessable.
    ///
    /// Not redelivered by this node, and it increments `delivery-count`, which
    /// is what a redelivery limit counts.
    #[pyo3(signature = (delivery_id, condition=None, description=None))]
    fn reject<'py>(
        &self,
        py: Python<'py>,
        delivery_id: u32,
        condition: Option<String>,
        description: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let error = condition.map(|condition| match description {
            Some(description) => Condition::described(condition, description),
            None => Condition::new(condition),
        });
        self.settle_one(
            py,
            delivery_id,
            weida_amqp::settlement::Outcome::Rejected { error },
        )
    }

    /// A **receiver**: releases a delivery, unchanged and available again.
    ///
    /// `delivery-count` is **not** incremented, which makes a released
    /// message indistinguishable from one never delivered.
    fn release<'py>(&self, py: Python<'py>, delivery_id: u32) -> PyResult<Bound<'py, PyAny>> {
        self.settle_one(py, delivery_id, weida_amqp::settlement::Outcome::Released)
    }

    /// A **receiver**: makes a delivery available again, with a note.
    ///
    /// Both flags are tri-state because the specification gives them no
    /// defaults: `None` leaves the node's own policy in charge, and that is
    /// not the same as `False`.
    #[pyo3(signature = (delivery_id, *, delivery_failed=None, undeliverable_here=None))]
    fn modify<'py>(
        &self,
        py: Python<'py>,
        delivery_id: u32,
        delivery_failed: Option<bool>,
        undeliverable_here: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle_one(
            py,
            delivery_id,
            weida_amqp::settlement::Outcome::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations: None,
            },
        )
    }

    /// Settles every delivery this end holds in `first..=last` with **one**
    /// `disposition`.
    ///
    /// One frame for a batch is the point of the range. Deliveries in it that
    /// this end no longer holds are simply not there, which is idempotence
    /// rather than an error.
    #[pyo3(signature = (first, last, outcome, *, condition=None, description=None,
                        delivery_failed=None, undeliverable_here=None))]
    #[allow(clippy::too_many_arguments, reason = "one keyword argument per field")]
    fn settle_range<'py>(
        &self,
        py: Python<'py>,
        first: u32,
        last: u32,
        outcome: &str,
        condition: Option<String>,
        description: Option<String>,
        delivery_failed: Option<bool>,
        undeliverable_here: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let outcome = outcome_of(
            py,
            outcome,
            condition.map(|condition| (condition, description)),
            delivery_failed,
            undeliverable_here,
        )?;
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire()
                .await
                .settle(first, last, &outcome)
                .await
                .map_err(errors::errno_of)
        })
    }

    /// The deliveries this end still holds state for.
    ///
    /// Empty is the answer an application usually wants: nothing outstanding
    /// in either direction.
    fn unsettled<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            Ok(slot
                .acquire()
                .await
                .unsettled()
                .into_iter()
                .map(|pending| {
                    (
                        pending.delivery_id,
                        pending.delivery_tag,
                        pending.ours.map(|outcome| outcome.name().to_owned()),
                        pending.theirs.map(|outcome| outcome.name().to_owned()),
                        pending.settled_here,
                    )
                })
                .collect::<Vec<_>>())
        })
    }

    /// `attaching`, `attached`, `detaching` or `detached`.
    fn state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            Ok(match slot.acquire().await.state() {
                LinkState::Attaching => "attaching",
                LinkState::Attached => "attached",
                LinkState::Detaching => "detaching",
                LinkState::Detached(_) => "detached",
            }
            .to_owned())
        })
    }

    /// Whether a message could go out right now.
    ///
    /// Three conditions because the protocol has three: attached, link credit,
    /// and room in the session window. A generous grant with a full window
    /// sends nothing.
    fn may_send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge
            .awaitable(py, async move { Ok(slot.acquire().await.may_send()) })
    }

    /// Detaches the link, destroying the endpoint at both ends. Idempotent.
    #[pyo3(signature = (*, closed=true, condition=None, description=None))]
    fn detach<'py>(
        &self,
        py: Python<'py>,
        closed: bool,
        condition: Option<String>,
        description: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let error = condition.map(|condition| match description {
            Some(description) => Condition::described(condition, description),
            None => Condition::new(condition),
        });
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire()
                .await
                .detach_with(closed, error)
                .await
                .map_err(errors::errno_of)
        })
    }
}

impl PyLink {
    /// One delivery, settled with `outcome` — the shape `accept`, `reject`,
    /// `release` and `modify` share, so the four cannot disagree.
    fn settle_one<'py>(
        &self,
        py: Python<'py>,
        delivery_id: u32,
        outcome: weida_amqp::settlement::Outcome,
    ) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire()
                .await
                .settle(delivery_id, delivery_id, &outcome)
                .await
                .map_err(errors::errno_of)
        })
    }
}

/// What the answering `attach` reported.
#[pyclass(frozen, name = "Negotiated", module = "weida_amqp", get_all)]
pub struct Negotiated {
    /// How the sender settles, from whichever end has `role=sender`.
    pub snd_settle_mode: String,
    /// When the receiver settles, from whichever end has `role=receiver`.
    pub rcv_settle_mode: String,
    /// The address the peer says the source has, or `None` if it refused to
    /// provide a terminus — after which it MUST immediately detach.
    pub source_address: Option<String>,
    /// The address the peer says the target has.
    pub target_address: Option<String>,
    /// The peer's `max-message-size`; `None` means no limit.
    pub max_message_size: Option<u64>,
    /// What the peer offered.
    pub offered_capabilities: Vec<String>,
}

#[pymethods]
impl Negotiated {
    fn __repr__(&self) -> String {
        format!(
            "<Negotiated snd={} rcv={} source={:?} target={:?}>",
            self.snd_settle_mode, self.rcv_settle_mode, self.source_address, self.target_address
        )
    }
}

/// One link's credit, counted in messages.
#[pyclass(frozen, name = "Credit", module = "weida_amqp", get_all)]
pub struct Credit {
    /// Where the sequence has reached. An RFC 1982 serial number, not a count.
    pub delivery_count: u32,
    /// How many more messages may be sent. Zero is a stall, not an error.
    pub link_credit: u32,
    /// The sender's backlog.
    pub available: u32,
    /// Whether the receiver has asked the sender to drain.
    pub drain: bool,
}

#[pymethods]
impl Credit {
    fn __repr__(&self) -> String {
        format!(
            "<Credit count={} credit={} available={} drain={}>",
            self.delivery_count, self.link_credit, self.available, self.drain
        )
    }
}
