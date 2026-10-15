//! The session: the two channel numberings and the window links ride on.
//!
//! A session is not a convenience wrapper over a connection. It carries the
//! **frame** half of AMQP's flow control — the six variables of Part 2 §2.5.6
//! — and every link on it competes for that window, so a Python caller that
//! wants two independent windows begins two sessions. `windows()` reports the
//! six so that a stalled sender can be diagnosed rather than guessed at.
//!
//! # The handle is leased
//!
//! `weida_amqp::Session` needs `&mut self` to read its events, so it lives in
//! a [`Slot`](weida_py_core::Slot) and each coroutine leases it for the length
//! of one operation. A cancelled task gives it back: the lease's `Drop` does.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use weida_amqp::link::LinkOptions;
use weida_amqp::session::SessionState;
use weida_amqp::{Session, Source, Target};
use weida_py_core::Bridge;
use weida_runtime::OwnedReactor;

use crate::errors;
use crate::link::PyLink;
use crate::values::{receiver_settle_mode, sender_settle_mode};
use weida_py_core::Slot;

/// `weida_amqp.Session`.
#[pyclass(frozen, name = "Session", module = "weida_amqp")]
pub struct PySession {
    inner: Arc<Slot<Session>>,
    bridge: Bridge,
    reactor: Arc<OwnedReactor>,
}

impl PySession {
    pub fn of(session: Session, bridge: Bridge, reactor: Arc<OwnedReactor>) -> PySession {
        PySession {
            inner: Slot::new(session),
            bridge,
            reactor,
        }
    }
}

/// The link options a Python caller may set, turned into the library's.
///
/// `role` decides which terminus the address goes in, and that is not a
/// convenience: "the link is always considered to be between the source as
/// described by the sender and the target as described by the receiver", so a
/// sender names a target and a receiver names a source.
#[allow(clippy::too_many_arguments, reason = "one keyword argument per field")]
pub(crate) fn link_options(
    py: Python<'_>,
    name: &str,
    role: &str,
    address: &str,
    snd_settle_mode: Option<&str>,
    rcv_settle_mode: Option<&str>,
    max_message_size: Option<u64>,
    dynamic: bool,
) -> PyResult<LinkOptions> {
    let mut options = match role {
        "sender" => LinkOptions::sender(name, Target::at(address)),
        "receiver" => LinkOptions::receiver(name, Source::at(address)),
        other => {
            return Err(errors::configuration(
                py,
                format!("{other:?} is not a role; Part 2 §2.7.3 names two: sender, receiver"),
            ));
        }
    };
    if let Some(mode) = snd_settle_mode {
        options.snd_settle_mode = sender_settle_mode(py, mode)?;
    }
    if let Some(mode) = rcv_settle_mode {
        options.rcv_settle_mode = receiver_settle_mode(py, mode)?;
    }
    if let Some(size) = max_message_size {
        // Zero means no limit, which is the specification's own default and
        // this client's deliberate departure from it; a caller asking for zero
        // is asking for it explicitly, which is the whole point of the option.
        options.max_message_size = Some(size);
    }
    if dynamic {
        // A dynamic terminus asks the peer to create a node and report its
        // address, so the address a caller gave is not the one it will use.
        match role {
            "sender" => {
                if let Some(target) = options.target.as_mut() {
                    target.dynamic = true;
                    target.address = None;
                }
            }
            _ => {
                if let Some(source) = options.source.as_mut() {
                    source.dynamic = true;
                    source.address = None;
                }
            }
        }
    }
    errors::raise(py, options.validate())?;
    Ok(options)
}

#[pymethods]
impl PySession {
    /// The channel this session writes into every frame it sends.
    fn outgoing_channel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(
            py,
            async move { Ok(slot.acquire().await.outgoing_channel()) },
        )
    }

    /// The channel the peer writes into every frame it sends.
    ///
    /// A different number in general: the two directions are numbered
    /// independently and the answering `begin` is what ties them together.
    fn incoming_channel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(
            py,
            async move { Ok(slot.acquire().await.incoming_channel()) },
        )
    }

    /// The six flow-control variables, as a dict.
    ///
    /// `remote_incoming_window` at zero is what a stalled sender is waiting
    /// for, and having it readable is the difference between diagnosing that
    /// and guessing at it.
    fn windows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let windows = slot.acquire().await.windows();
            Ok(Windows {
                next_incoming_id: windows.next_incoming_id,
                incoming_window: windows.incoming_window,
                next_outgoing_id: windows.next_outgoing_id,
                outgoing_window: windows.outgoing_window,
                remote_incoming_window: windows.remote_incoming_window,
                remote_outgoing_window: windows.remote_outgoing_window,
            })
        })
    }

    /// Attaches a link, returning once the answering `attach` has arrived.
    ///
    /// Until then nothing is known about what the peer actually created —
    /// which terminus, which settle modes, which handle its frames will carry
    /// — so a link handed over earlier would be one whose configuration the
    /// caller could not read.
    #[pyo3(signature = (
        name,
        role,
        address,
        *,
        snd_settle_mode=None,
        rcv_settle_mode=None,
        max_message_size=None,
        dynamic=false,
    ))]
    #[allow(clippy::too_many_arguments, reason = "one keyword argument per field")]
    fn attach<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        role: &str,
        address: &str,
        snd_settle_mode: Option<&str>,
        rcv_settle_mode: Option<&str>,
        max_message_size: Option<u64>,
        dynamic: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = link_options(
            py,
            name,
            role,
            address,
            snd_settle_mode,
            rcv_settle_mode,
            max_message_size,
            dynamic,
        )?;
        let slot = Arc::clone(&self.inner);
        let bridge = self.bridge.clone();
        let reactor = Arc::clone(&self.reactor);
        self.bridge.awaitable(py, async move {
            let link = slot
                .acquire()
                .await
                .attach(options)
                .await
                .map_err(errors::errno_of)?;
            Ok(PyLink::of(link, bridge, reactor))
        })
    }

    /// Replenishes this session's incoming window and tells the peer.
    ///
    /// The window shrinks as `transfer` frames arrive and grows again only
    /// when the receiver says so; this is the saying so. A receiver that
    /// granted link credit and never replenished the window stalls its sender
    /// with credit in hand.
    #[pyo3(signature = (*, echo=false))]
    fn flow<'py>(&self, py: Python<'py>, echo: bool) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire()
                .await
                .flow(echo)
                .await
                .map_err(errors::errno_of)
        })
    }

    /// `begun`, `beginning`, `ending`, `discarding` or `ended`.
    fn state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            Ok(match slot.acquire().await.state() {
                SessionState::Beginning => "beginning",
                SessionState::Begun => "begun",
                SessionState::Ending => "ending",
                SessionState::Discarding(_) => "discarding",
                SessionState::Ended(_) => "ended",
            }
            .to_owned())
        })
    }

    /// Ends the session. Idempotent.
    fn end<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            slot.acquire().await.end().await.map_err(errors::errno_of)
        })
    }
}

/// The six variables, as Python sees them.
#[pyclass(frozen, name = "Windows", module = "weida_amqp", get_all)]
pub struct Windows {
    /// The transfer-id we expect next from the peer.
    pub next_incoming_id: u32,
    /// How many `transfer` frames we can receive now.
    pub incoming_window: u32,
    /// The transfer-id of our next outgoing frame.
    pub next_outgoing_id: u32,
    /// How many we are willing to send.
    pub outgoing_window: u32,
    /// How many we may send without exceeding the peer's incoming window. At
    /// zero, a sender stalls however much link credit it holds.
    pub remote_incoming_window: u32,
    /// How many may arrive without exceeding the peer's outgoing window.
    pub remote_outgoing_window: u32,
}

#[pymethods]
impl Windows {
    fn __repr__(&self) -> String {
        format!(
            "<Windows incoming={} outgoing={} remote_incoming={} remote_outgoing={}>",
            self.incoming_window,
            self.outgoing_window,
            self.remote_incoming_window,
            self.remote_outgoing_window
        )
    }

    /// So a caller can hand the six straight to a logger.
    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("next_incoming_id", self.next_incoming_id)?;
        dict.set_item("incoming_window", self.incoming_window)?;
        dict.set_item("next_outgoing_id", self.next_outgoing_id)?;
        dict.set_item("outgoing_window", self.outgoing_window)?;
        dict.set_item("remote_incoming_window", self.remote_incoming_window)?;
        dict.set_item("remote_outgoing_window", self.remote_outgoing_window)?;
        Ok(dict)
    }
}
