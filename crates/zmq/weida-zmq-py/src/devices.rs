//! `zmq_proxy` and `zmq_proxy_steerable`, over the sockets of this module.
//!
//! The loop itself is `weida_zmq::proxy`: both directions, one message at a
//! time, a capture socket that gets a copy of every message, and
//! PAUSE/RESUME/TERMINATE/STATISTICS on a control socket. **None of it is
//! written twice** — this module's whole job is to get a Python socket object
//! into that function.
//!
//! # Why the type erasure
//!
//! `weida_zmq::Device` is a trait with `async fn` methods, and `proxy` is
//! generic over three or four of them. A Python caller chooses the socket
//! types at runtime — `proxy(xsub, xpub, capture)` — so a generic
//! instantiation per combination would be eleven cubed of them. An `async fn`
//! in a trait is not dyn-compatible either, so the erasure is done here, once:
//! [`ErasedDevice`] boxes the two futures, [`Erased`] implements
//! `weida_zmq::Device` over that box, and `proxy` is instantiated exactly
//! once with `Erased` in every position.
//!
//! The erasure costs one `Box` per message per side. The alternative — a
//! proxy loop written in this crate — would be a second implementation of the
//! device, which [0013](../../../../docs/decisions/0013-competitor-libraries.md)
//! §4.4 forbids for the reason a reader can guess: the two would disagree
//! exactly where no test looked.

use std::pin::Pin;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use weida_py_core::{Errno, Live, Retirable};
use weida_zmq::{Device, Multipart, ProxyStatistics, Result};

use crate::errors::errno_of;

/// A boxed future, so that an `async fn` in a trait can be dyn-compatible.
type Boxed<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// One end of a device, with its futures boxed.
pub trait ErasedDevice: Send {
    /// Whether this end ever delivers a message.
    fn receives(&self) -> bool;
    /// Receives one whole message.
    fn recv(&mut self) -> Boxed<'_, Multipart>;
    /// Sends one whole message.
    fn send(&mut self, message: Multipart) -> Boxed<'_, ()>;
}

/// A leased socket as an erased device.
struct Leased<S: Device + Send + 'static>(Live<S>);

impl<S: Device + Send + 'static> ErasedDevice for Leased<S> {
    fn receives(&self) -> bool {
        Device::receives(&*self.0)
    }

    fn recv(&mut self) -> Boxed<'_, Multipart> {
        Box::pin(Device::recv(&mut *self.0))
    }

    fn send(&mut self, message: Multipart) -> Boxed<'_, ()> {
        Box::pin(Device::send(&mut *self.0, message))
    }
}

/// What `proxy` is instantiated with: the library's own trait over the box.
pub struct Erased(Box<dyn ErasedDevice>);

impl Device for Erased {
    fn receives(&self) -> bool {
        self.0.receives()
    }

    async fn recv(&mut self) -> Result<Multipart> {
        self.0.recv().await
    }

    async fn send(&mut self, message: Multipart) -> Result<()> {
        self.0.send(message).await
    }
}

/// A future that leases one Python socket and hands it over as a device.
///
/// `Err` when the socket was split and its slot retired.
pub type Leasing = Pin<Box<dyn Future<Output = Result<Erased, Errno>> + Send + 'static>>;

/// Builds the leasing future for one socket class. Called from the socket
/// macro, which is the only place that has the slot.
pub fn leasing<S: Device + Send + 'static>(slot: &Retirable<S>) -> Leasing {
    let slot = slot.clone();
    Box::pin(async move { Ok(Erased(Box::new(Leased(crate::ops::lease(&slot).await?)))) })
}

/// Takes a socket of this module as a device end.
///
/// REQ and REP are not device ends: 28/REQREP's alternation is the
/// application's to drive, so a proxy between them would be driving two state
/// machines nobody owns. `weida_zmq::Device` is not implemented for them and
/// libzmq's own devices refuse them too, so the refusal here is a `TypeError`
/// at the call rather than an `ENOTSUP` at the first message.
pub fn device_of(object: &Bound<'_, PyAny>) -> PyResult<Leasing> {
    macro_rules! any_of {
        ($($class:ident),+ $(,)?) => {
            $(if let Ok(socket) = object.extract::<PyRef<'_, crate::sockets::$class>>() {
                return Ok(socket.leasing());
            })+
        };
    }
    any_of!(
        PyDealerSocket,
        PyRouterSocket,
        PyPubSocket,
        PySubSocket,
        PyXPubSocket,
        PyXSubSocket,
        PyPushSocket,
        PyPullSocket,
        PyPairSocket,
    );
    Err(PyTypeError::new_err(
        "a device end is a DealerSocket, RouterSocket, PubSocket, SubSocket, XPubSocket, \
         XSubSocket, PushSocket, PullSocket or PairSocket; REQ and REP drive an alternation \
         only the application can drive, so they are not device ends",
    ))
}

/// The eight counters `zmq_proxy_steerable`'s `STATISTICS` reports.
#[pyclass(frozen, get_all, name = "ProxyStatistics", module = "weida_zmq")]
pub struct PyProxyStatistics {
    /// Messages the frontend received.
    pub frontend_messages_in: u64,
    /// Octets the frontend received, frame bodies only.
    pub frontend_bytes_in: u64,
    /// Messages the frontend was given to send.
    pub frontend_messages_out: u64,
    /// Octets the frontend was given to send.
    pub frontend_bytes_out: u64,
    /// Messages the backend received.
    pub backend_messages_in: u64,
    /// Octets the backend received.
    pub backend_bytes_in: u64,
    /// Messages the backend was given to send.
    pub backend_messages_out: u64,
    /// Octets the backend was given to send.
    pub backend_bytes_out: u64,
}

impl PyProxyStatistics {
    /// Wraps the library's counters.
    pub fn of(statistics: ProxyStatistics) -> PyProxyStatistics {
        PyProxyStatistics {
            frontend_messages_in: statistics.frontend_in.messages,
            frontend_bytes_in: statistics.frontend_in.bytes,
            frontend_messages_out: statistics.frontend_out.messages,
            frontend_bytes_out: statistics.frontend_out.bytes,
            backend_messages_in: statistics.backend_in.messages,
            backend_bytes_in: statistics.backend_in.bytes,
            backend_messages_out: statistics.backend_out.messages,
            backend_bytes_out: statistics.backend_out.bytes,
        }
    }
}

#[pymethods]
impl PyProxyStatistics {
    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.ProxyStatistics frontend in {}/{} out {}/{}, backend in {}/{} out {}/{}>",
            self.frontend_messages_in,
            self.frontend_bytes_in,
            self.frontend_messages_out,
            self.frontend_bytes_out,
            self.backend_messages_in,
            self.backend_bytes_in,
            self.backend_messages_out,
            self.backend_bytes_out,
        )
    }
}

/// `await weida_zmq.proxy(frontend, backend, capture=None)`.
///
/// Runs until the sockets go away, which is a device's normal ending: a proxy
/// is a program's main loop, and a caller that wants to stop it on purpose
/// wants [`proxy_steerable`] or an `asyncio.Task` it can cancel. Returns the
/// counters it accumulated.
///
/// Every message crossing in either direction is copied to `capture` when one
/// is given, which is the zguide's Espresso recipe: a proxy is where a trace
/// belongs, because it is the one place both directions pass through.
#[pyfunction]
#[pyo3(signature = (frontend, backend, capture=None))]
pub fn proxy<'py>(
    py: Python<'py>,
    frontend: &Bound<'py, PyAny>,
    backend: &Bound<'py, PyAny>,
    capture: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let bridge = crate::sockets::bridge_of(frontend)?;
    let frontend = device_of(frontend)?;
    let backend = device_of(backend)?;
    let capture = capture.map(device_of).transpose()?;
    bridge.awaitable(py, async move {
        let mut frontend = frontend.await?;
        let mut backend = backend.await?;
        let mut capture = match capture {
            Some(capture) => Some(capture.await?),
            None => None,
        };
        weida_zmq::proxy(&mut frontend, &mut backend, capture.as_mut())
            .await
            .map(PyProxyStatistics::of)
            .map_err(errno_of)
    })
}

/// `await weida_zmq.proxy_steerable(frontend, backend, control, capture=None)`.
///
/// The same proxy with libzmq's control socket: `PAUSE` stops reading either
/// side, `RESUME` starts again, `STATISTICS` replies with the eight counters
/// on the control socket, and `TERMINATE` ends the proxy and returns them.
#[pyfunction]
#[pyo3(signature = (frontend, backend, control, capture=None))]
pub fn proxy_steerable<'py>(
    py: Python<'py>,
    frontend: &Bound<'py, PyAny>,
    backend: &Bound<'py, PyAny>,
    control: &Bound<'py, PyAny>,
    capture: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let bridge = crate::sockets::bridge_of(frontend)?;
    let frontend = device_of(frontend)?;
    let backend = device_of(backend)?;
    let control = device_of(control)?;
    let capture = capture.map(device_of).transpose()?;
    bridge.awaitable(py, async move {
        let mut frontend = frontend.await?;
        let mut backend = backend.await?;
        let mut control = control.await?;
        let mut capture = match capture {
            Some(capture) => Some(capture.await?),
            None => None,
        };
        weida_zmq::proxy_steerable(&mut frontend, &mut backend, capture.as_mut(), &mut control)
            .await
            .map(PyProxyStatistics::of)
            .map_err(errno_of)
    })
}

/// The control words, so that a Python caller sends libzmq's own octets rather
/// than a string it guessed.
pub fn controls(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("CONTROL_PAUSE", weida_zmq::CONTROL_PAUSE)?;
    module.add("CONTROL_RESUME", weida_zmq::CONTROL_RESUME)?;
    module.add("CONTROL_TERMINATE", weida_zmq::CONTROL_TERMINATE)?;
    module.add("CONTROL_STATISTICS", weida_zmq::CONTROL_STATISTICS)?;
    Ok(())
}
