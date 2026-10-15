//! `zmq_socket_monitor`, twice over: an async iterator and the `inproc` PAIR.
//!
//! One publisher, two renderings, and the octets decided in exactly one place
//! — `weida_zmq::monitor` — which is why neither of these is a second
//! implementation of anything:
//!
//! * **As an async iterator.** `async for event in socket.monitor():` hands
//!   the application typed events, so a Python program that wants to know
//!   about a reconnect does not have to decode a two-frame message first.
//! * **As the `inproc://` PAIR.** `await monitor.serve_pair(pair)` writes the
//!   same events onto a PAIR socket in libzmq's two-frame form, which is what
//!   the zguide's Espresso recipe reads and what a port from C expects.
//!
//! # Why the end of the stream is `StopAsyncIteration`
//!
//! A monitor ends when its socket is gone, which the library reports as
//! `ENOTSOCK`. For an `async for` that is not an error, it is the end of the
//! iteration, so this module turns exactly that one errno into Python's
//! `StopAsyncIteration` — and nothing else, so a lagging monitor
//! (`EAGAIN`, events dropped) still arrives as the exception it is.

use std::sync::Arc;

use pyo3::prelude::*;
use weida_py_core::{Bridge, Errno, Slot};
use weida_zmq::{MonitorEvent, MonitorEvents};

use crate::errors::errno_of;
use crate::ops::lease;

/// The errno name this crate's mapper turns into `StopAsyncIteration`.
///
/// Not one of libzmq's: it is the marker for "this stream has ended", and it
/// exists because an iteration's end is not a failure. See
/// [`crate::errors::to_py`].
pub const STOP_ASYNC_ITERATION: &str = "StopAsyncIteration";

/// One `ZMQ_EVENT_*`, typed.
#[pyclass(frozen, name = "MonitorEvent", module = "weida_zmq")]
pub struct PyMonitorEvent {
    name: &'static str,
    id: u16,
    endpoint: String,
    reason: String,
    value: u32,
}

impl PyMonitorEvent {
    /// Wraps the library's event.
    pub fn of(event: &MonitorEvent) -> PyMonitorEvent {
        PyMonitorEvent {
            name: name_of(event),
            id: event.id(),
            endpoint: event.endpoint().to_owned(),
            reason: event.reason().to_owned(),
            value: event.value(),
        }
    }
}

/// libzmq's own name for each event, minus the `ZMQ_EVENT_` prefix.
fn name_of(event: &MonitorEvent) -> &'static str {
    match event {
        MonitorEvent::Connected { .. } => "CONNECTED",
        MonitorEvent::ConnectDelayed { .. } => "CONNECT_DELAYED",
        MonitorEvent::ConnectRetried { .. } => "CONNECT_RETRIED",
        MonitorEvent::Listening { .. } => "LISTENING",
        MonitorEvent::BindFailed { .. } => "BIND_FAILED",
        MonitorEvent::Accepted { .. } => "ACCEPTED",
        MonitorEvent::AcceptFailed { .. } => "ACCEPT_FAILED",
        MonitorEvent::Closed { .. } => "CLOSED",
        MonitorEvent::Disconnected { .. } => "DISCONNECTED",
        MonitorEvent::MonitorStopped { .. } => "MONITOR_STOPPED",
        MonitorEvent::HandshakeSucceeded { .. } => "HANDSHAKE_SUCCEEDED",
        MonitorEvent::HandshakeFailedNoDetail { .. } => "HANDSHAKE_FAILED_NO_DETAIL",
        MonitorEvent::HandshakeFailedProtocol { .. } => "HANDSHAKE_FAILED_PROTOCOL",
        MonitorEvent::HandshakeFailedAuth { .. } => "HANDSHAKE_FAILED_AUTH",
    }
}

#[pymethods]
impl PyMonitorEvent {
    /// libzmq's name without the prefix: `"CONNECTED"`, `"ACCEPTED"`, and so
    /// on.
    #[getter]
    fn name(&self) -> &'static str {
        self.name
    }

    /// The `ZMQ_EVENT_*` bit.
    #[getter]
    fn id(&self) -> u16 {
        self.id
    }

    /// The endpoint the event is about, as the application named it.
    #[getter]
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Why it failed, for the events that are failures; empty otherwise.
    ///
    /// libzmq's wire form carries a number in this position and a caller has
    /// to look it up; the words are what a log needs.
    #[getter]
    fn reason(&self) -> &str {
        &self.reason
    }

    /// libzmq's value field: the retry interval in milliseconds for
    /// `CONNECT_RETRIED`, and zero elsewhere.
    #[getter]
    fn value(&self) -> u32 {
        self.value
    }

    fn __repr__(&self) -> String {
        if self.reason.is_empty() {
            format!("<weida_zmq.MonitorEvent {} {}>", self.name, self.endpoint)
        } else {
            format!(
                "<weida_zmq.MonitorEvent {} {}: {}>",
                self.name, self.endpoint, self.reason
            )
        }
    }
}

/// A socket's connection lifecycle: an async iterator of typed events.
#[pyclass(frozen, name = "Monitor", module = "weida_zmq")]
pub struct PyMonitor {
    monitor: Arc<Slot<weida_zmq::Monitor>>,
    bridge: Bridge,
}

impl PyMonitor {
    /// Wraps an installed monitor.
    pub fn new(monitor: weida_zmq::Monitor, bridge: Bridge) -> PyMonitor {
        PyMonitor {
            monitor: Slot::new(monitor),
            bridge,
        }
    }

    /// The mask, from an optional Python argument: `ZMQ_EVENT_ALL` by default.
    pub fn events(mask: Option<u16>) -> MonitorEvents {
        mask.map_or(MonitorEvents::ALL, MonitorEvents)
    }
}

#[pymethods]
impl PyMonitor {
    fn __aiter__(slf: PyRef<'_, PyMonitor>) -> PyRef<'_, PyMonitor> {
        slf
    }

    /// The next event, or `StopAsyncIteration` when the socket is gone.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.recv(py)
    }

    /// The next event, waiting for one.
    ///
    /// `EAGAIN` when events were dropped because the application did not read
    /// them (the monitor holds `MONITOR_CAPACITY` of them); the stream
    /// continues after that.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.monitor);
        self.bridge.awaitable(py, async move {
            match slot.acquire().await.recv().await {
                Ok(event) => Ok(PyMonitorEvent::of(&event)),
                // The socket is gone: for an `async for`, that is the end.
                Err(weida_zmq::Error::ENOTSOCK(_)) => Err(Errno::new(
                    STOP_ASYNC_ITERATION,
                    "the monitored socket is gone, so no further event can arrive",
                )),
                Err(other) => Err(errno_of(other)),
            }
        })
    }

    /// Renders the same events onto a PAIR socket in libzmq's two-frame
    /// `inproc://` form, which is what the zguide's Espresso recipe reads.
    ///
    /// Runs until the monitored socket is gone, and returns then. The events
    /// it writes are the ones published from this call onwards: a monitor that
    /// has already been read from does not repeat what the reader took.
    fn serve_pair<'py>(
        &self,
        py: Python<'py>,
        pair: PyRef<'py, crate::sockets::PyPairSocket>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let monitor = Arc::clone(&self.monitor);
        let socket = pair.slot().clone();
        self.bridge.awaitable(py, async move {
            let events = monitor.acquire().await.resubscribe();
            let mut pair = lease(&socket).await?;
            weida_zmq::serve_pair(events, &mut pair)
                .await
                .map_err(errno_of)
        })
    }

    /// `ZMQ_DONTWAIT`: the next event if one is queued.
    fn recv_nowait(&self, py: Python<'_>) -> PyResult<PyMonitorEvent> {
        let Some(mut monitor) = self.monitor.try_acquire() else {
            return Err(crate::errors::to_py(
                py,
                &Errno::new(
                    "EAGAIN",
                    "another coroutine is reading this monitor, and this call was told not to \
                     wait",
                ),
            ));
        };
        crate::errors::raise(py, monitor.try_recv()).map(|event| PyMonitorEvent::of(&event))
    }
}
