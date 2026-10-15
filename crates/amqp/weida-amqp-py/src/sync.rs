//! `weida_amqp.sync`: AMQP 1.0 for a Python program with no event loop.
//!
//! ```python
//! from weida_amqp import sync
//!
//! connection = sync.connect("127.0.0.1", 5672, container_id="app")
//! session = connection.begin()
//! sender = session.attach("orders", "sender", "/queues/orders")
//! outcome = sender.send(b"an order")          # blocks until the peer answers
//! assert outcome.name == "accepted"
//! connection.close()
//! ```
//!
//! # It implements nothing
//!
//! Every method here drives the *same* asynchronous object's method to
//! completion. The handshake, both credit schemes, the settle modes, the
//! unsettled map and the reassembly are `weida-amqp`'s and cannot disagree
//! with the asyncio surface, because this module decides none of them.
//!
//! Unlike `weida_zmq.sync`, which sits on `weida-zmq`'s own `blocking` facade,
//! there is no `weida_amqp::blocking`: AMQP's Rust API is asynchronous and
//! nobody has asked for a synchronous one, so the waiting happens here. That
//! is a difference in *where* the wrapper lives, not in how many
//! implementations exist — there is still one.
//!
//! # The operation runs on the reactor, and this thread waits for it
//!
//! Each call but one spawns the library's future on the connection's reactor
//! and blocks on the join handle, with the wait [`interruptible`]. Two
//! reasons, and the second is the load-bearing one:
//!
//! * the reactor's threads drive the connection, so a caller that parked one
//!   of them would be waiting for a worker that is waiting for it;
//! * AMQP's own operations use **timers** — the handshake deadline, the idle
//!   timeout, a receive bound — and a tokio timer only runs inside a tokio
//!   context, which a future polled by `futures::executor::block_on` off the
//!   runtime would not have.
//!
//! **`next_delivery` is the one that is polled here instead**, on the calling
//! thread, inside the runtime's context ([`interruptible`] enters it). It
//! takes a delivery off the link, and a task that took one after its caller
//! was interrupted would be holding a delivery nobody can read: consumed from
//! the link, never settled, its credit spent. A future polled by the thread
//! that waits for it consumes only what that thread is about to return, so a
//! Ctrl-C that drops it has dropped nothing.
//!
//! # The GIL is released while blocked
//!
//! Every call waits inside [`Python::detach`], so other Python threads run
//! while one sits in `next_delivery`. A blocking binding that held the GIL
//! would make a second thread pointless, and the usual shape of a synchronous
//! program — one thread per link — would run one link at a time.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyModule;
use weida_amqp::session::SessionOptions;
use weida_amqp::{Connection, Error, Link, Session};
use weida_py_core::{Bridge, Slot, duration, interruptible};
use weida_runtime::OwnedReactor;

use crate::connection::{drive, open, options_from};
use crate::errors;
use crate::link::{Taken, next_of};
use crate::values::{Octets, Outgoing, PyDelivery, PyOutcome, message_from, outcome_of};

/// `weida_amqp.sync.Connection`.
#[pyclass(frozen, name = "Connection", module = "weida_amqp.sync")]
pub struct SyncConnection {
    inner: Connection,
    bridge: Bridge,
    reactor: Arc<OwnedReactor>,
}

#[pymethods]
impl SyncConnection {
    /// Begins a session and waits for the answering `begin`.
    #[pyo3(signature = (*, incoming_window=None, outgoing_window=None, handle_max=None))]
    fn begin(
        &self,
        py: Python<'_>,
        incoming_window: Option<u32>,
        outgoing_window: Option<u32>,
        handle_max: Option<u32>,
    ) -> PyResult<SyncSession> {
        let mut options = SessionOptions::default();
        if let Some(window) = incoming_window {
            options.incoming_window = window;
        }
        if let Some(window) = outgoing_window {
            options.outgoing_window = window;
        }
        if let Some(handles) = handle_max {
            options.handle_max = handles;
        }
        let connection = self.inner.clone();
        let session = drive(py, self.bridge.exec(), async move {
            connection.begin(options).await
        })?;
        Ok(SyncSession {
            inner: Slot::new(session),
            bridge: self.bridge.clone(),
            reactor: Arc::clone(&self.reactor),
        })
    }

    /// The peer's `open`, as a dict.
    fn remote<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let remote = self.inner.remote();
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("container_id", remote.container_id.clone())?;
        dict.set_item("max_frame_size", remote.max_frame_size)?;
        dict.set_item("channel_max", remote.channel_max)?;
        Ok(dict)
    }

    /// Whether a session may still be begun.
    fn is_usable(&self) -> bool {
        self.inner.state().is_usable()
    }

    /// Closes the connection. Idempotent.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let connection = self.inner.clone();
        drive(
            py,
            self.bridge.exec(),
            async move { connection.close().await },
        )
    }
}

/// `weida_amqp.sync.Session`.
#[pyclass(frozen, name = "Session", module = "weida_amqp.sync")]
pub struct SyncSession {
    inner: Arc<Slot<Session>>,
    bridge: Bridge,
    reactor: Arc<OwnedReactor>,
}

#[pymethods]
impl SyncSession {
    /// Attaches a link and waits for the answering `attach`.
    #[pyo3(signature = (
        name,
        role,
        address,
        *,
        snd_settle_mode=None,
        rcv_settle_mode=None,
        max_message_size=None,
    ))]
    #[allow(clippy::too_many_arguments, reason = "one keyword argument per field")]
    fn attach(
        &self,
        py: Python<'_>,
        name: &str,
        role: &str,
        address: &str,
        snd_settle_mode: Option<&str>,
        rcv_settle_mode: Option<&str>,
        max_message_size: Option<u64>,
    ) -> PyResult<SyncLink> {
        // The same builder the asyncio surface uses, so the two cannot
        // disagree about what an argument means. `dynamic` is not offered
        // here: a dynamic terminus's address is only readable from the
        // answering `attach`, and offering the flag without an accessor for
        // the answer would be offering half a feature.
        let options = crate::session::link_options(
            py,
            name,
            role,
            address,
            snd_settle_mode,
            rcv_settle_mode,
            max_message_size,
            false,
        )?;
        let slot = Arc::clone(&self.inner);
        let link = drive(py, self.bridge.exec(), async move {
            slot.acquire().await.attach(options).await
        })?;
        Ok(SyncLink {
            inner: Slot::new(link),
            bridge: self.bridge.clone(),
            reactor: Arc::clone(&self.reactor),
        })
    }

    /// Replenishes the incoming window and tells the peer.
    #[pyo3(signature = (*, echo=false))]
    fn flow(&self, py: Python<'_>, echo: bool) -> PyResult<()> {
        let slot = Arc::clone(&self.inner);
        drive(py, self.bridge.exec(), async move {
            slot.acquire().await.flow(echo).await
        })
    }

    /// Ends the session. Idempotent.
    fn end(&self, py: Python<'_>) -> PyResult<()> {
        let slot = Arc::clone(&self.inner);
        drive(py, self.bridge.exec(), async move {
            slot.acquire().await.end().await
        })
    }
}

/// `weida_amqp.sync.Link`.
#[pyclass(frozen, name = "Link", module = "weida_amqp.sync")]
pub struct SyncLink {
    inner: Arc<Slot<Link, Taken>>,
    bridge: Bridge,
    #[allow(
        dead_code,
        reason = "the reactor is alive because this field is: a link outliving \
                  its connection's Python object must keep the runtime up"
    )]
    reactor: Arc<OwnedReactor>,
}

#[pymethods]
impl SyncLink {
    /// Sends one message and blocks until the peer's `disposition` settles it.
    ///
    /// Returns the outcome, or `None` on a `settled`-mode link where no answer
    /// is coming — the synchronous form of `await send`: the same call
    /// with the same meaning.
    #[pyo3(signature = (body, *, text=false, durable=false))]
    fn send(
        &self,
        py: Python<'_>,
        body: &Bound<'_, PyAny>,
        text: bool,
        durable: bool,
    ) -> PyResult<Option<PyOutcome>> {
        let octets = Octets::of(body, text)?;
        let encoded = {
            let message = message_from(
                py,
                &Outgoing {
                    body: octets.as_slice(),
                    text,
                    durable,
                    ..Outgoing::default()
                },
            )?;
            errors::raise(py, message.to_vec().map_err(Error::Encode))?
        };
        let slot = Arc::clone(&self.inner);
        let outcome = drive(py, self.bridge.exec(), async move {
            let mut link = slot.acquire().await;
            let sent = link.send_payload(&encoded).await?;
            if sent.settled {
                // Settled on send: nothing to wait for and nothing to report.
                return Ok(None);
            }
            link.settled(sent.delivery_id).await.map(Some)
        })?;
        Ok(outcome.map(PyOutcome::of))
    }

    /// Grants credit for `credit` more messages. Sets rather than adds.
    fn grant_credit(&self, py: Python<'_>, credit: u32) -> PyResult<()> {
        let slot = Arc::clone(&self.inner);
        drive(py, self.bridge.exec(), async move {
            slot.acquire().await.grant_credit(credit).await
        })
    }

    /// Drains the link: the sender consumes the remaining credit and reports.
    fn drain(&self, py: Python<'_>) -> PyResult<()> {
        let slot = Arc::clone(&self.inner);
        drive(py, self.bridge.exec(), async move {
            slot.acquire().await.drain().await
        })
    }

    /// The next delivery, waiting at most `timeout` seconds.
    ///
    /// The blocking drain. `None` for `timeout` waits until the
    /// link detaches, which is the honest synchronous equivalent of the
    /// asyncio surface's cancellation; a timeout that elapses returns `None`
    /// and leaves the link usable, because AMQP has no receive deadline and
    /// inventing one in the library would be inventing protocol behaviour.
    #[pyo3(signature = (timeout=None))]
    fn next_delivery(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<PyDelivery>> {
        let limit = timeout
            .map(|seconds| duration("timeout", seconds))
            .transpose()
            .map_err(|error| errors::configuration(py, error.to_string()))?;
        let slot = &self.inner;
        let exec = self.bridge.exec();
        let delivery = interruptible(py, exec, move || async move {
            let mut link = slot.acquire().await;
            match limit {
                None => next_of(&mut link).await,
                // A bound that elapses leaves the link usable: the receive is
                // dropped at its await point, and AMQP has no receive deadline
                // for the library to keep.
                Some(limit) => exec.within(limit, next_of(&mut link)).await.flatten(),
            }
        })?;
        Ok(delivery.map(PyDelivery::of))
    }

    /// Accepts a delivery.
    fn accept(&self, py: Python<'_>, delivery_id: u32) -> PyResult<()> {
        self.settle_with(py, delivery_id, delivery_id, "accepted", None, None)
    }

    /// Releases a delivery: available again, and not counted.
    fn release(&self, py: Python<'_>, delivery_id: u32) -> PyResult<()> {
        self.settle_with(py, delivery_id, delivery_id, "released", None, None)
    }

    /// Rejects a delivery as invalid and unprocessable.
    #[pyo3(signature = (delivery_id, condition=None, description=None))]
    fn reject(
        &self,
        py: Python<'_>,
        delivery_id: u32,
        condition: Option<String>,
        description: Option<String>,
    ) -> PyResult<()> {
        self.settle_with(
            py,
            delivery_id,
            delivery_id,
            "rejected",
            condition,
            description,
        )
    }

    /// Settles a range with one `disposition`.
    #[pyo3(signature = (first, last, outcome, *, condition=None, description=None))]
    fn settle_range(
        &self,
        py: Python<'_>,
        first: u32,
        last: u32,
        outcome: &str,
        condition: Option<String>,
        description: Option<String>,
    ) -> PyResult<()> {
        self.settle_with(py, first, last, outcome, condition, description)
    }

    /// The credit state, as a dict.
    fn credit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let slot = Arc::clone(&self.inner);
        let credit = drive(py, self.bridge.exec(), async move {
            Ok::<_, Error>(slot.acquire().await.credit())
        })?;
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("delivery_count", credit.delivery_count())?;
        dict.set_item("link_credit", credit.link_credit())?;
        dict.set_item("available", credit.available())?;
        dict.set_item("drain", credit.drain())?;
        Ok(dict)
    }

    /// Detaches the link. Idempotent.
    fn detach(&self, py: Python<'_>) -> PyResult<()> {
        let slot = Arc::clone(&self.inner);
        drive(py, self.bridge.exec(), async move {
            slot.acquire().await.detach().await
        })
    }
}

impl SyncLink {
    fn settle_with(
        &self,
        py: Python<'_>,
        first: u32,
        last: u32,
        outcome: &str,
        condition: Option<String>,
        description: Option<String>,
    ) -> PyResult<()> {
        let outcome = outcome_of(
            py,
            outcome,
            condition.map(|condition| (condition, description)),
            None,
            None,
        )?;
        let slot = Arc::clone(&self.inner);
        drive(py, self.bridge.exec(), async move {
            slot.acquire().await.settle(first, last, &outcome).await
        })
    }
}

/// `weida_amqp.sync.connect(host, port, ...)`.
///
/// The reactor is the connection's and the caller never sees it, so no asyncio
/// loop exists in such a process and none is needed.
#[pyfunction]
#[pyo3(signature = (
    host,
    port,
    *,
    container_id="weida-amqp-py",
    hostname=None,
    max_frame_size=None,
    channel_max=None,
    idle_time_out=None,
    sasl_user=None,
    sasl_password=None,
    sasl_anonymous=false,
    handshake_timeout=None,
    worker_threads=1,
))]
#[allow(clippy::too_many_arguments, reason = "one keyword argument per option")]
fn connect(
    py: Python<'_>,
    host: &str,
    port: u16,
    container_id: &str,
    hostname: Option<String>,
    max_frame_size: Option<u32>,
    channel_max: Option<u16>,
    idle_time_out: Option<f64>,
    sasl_user: Option<String>,
    sasl_password: Option<String>,
    sasl_anonymous: bool,
    handshake_timeout: Option<f64>,
    worker_threads: usize,
) -> PyResult<SyncConnection> {
    let options = options_from(
        py,
        container_id,
        hostname,
        max_frame_size,
        channel_max,
        idle_time_out,
        sasl_user,
        sasl_password,
        sasl_anonymous,
        handshake_timeout,
    )?;
    let (inner, _exec, reactor) = open(py, worker_threads, host, port, options)?;
    let bridge = Bridge::new(inner.exec().clone(), errors::to_py);
    Ok(SyncConnection {
        inner,
        bridge,
        reactor: Arc::new(reactor),
    })
}

/// Registers the submodule.
pub fn install(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let module = PyModule::new(py, "sync")?;
    module.add_class::<SyncConnection>()?;
    module.add_class::<SyncSession>()?;
    module.add_class::<SyncLink>()?;
    module.add_function(pyo3::wrap_pyfunction!(connect, &module)?)?;
    parent.add_submodule(&module)?;
    // `from weida_amqp import sync` needs the submodule in `sys.modules`;
    // `add_submodule` alone makes it an attribute and not an importable name.
    py.import("sys")?
        .getattr("modules")?
        .set_item("weida_amqp.sync", &module)?;
    Ok(())
}
