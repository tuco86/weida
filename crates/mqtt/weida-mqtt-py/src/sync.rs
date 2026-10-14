//! `weida_mqtt.sync`: MQTT for a Python program with no event loop.
//!
//! ```python
//! from weida_mqtt import sync
//! import weida_mqtt
//!
//! context = sync.Context()
//! client, deliveries = context.connect("127.0.0.1:1883",
//!                                      weida_mqtt.ConnectOptions("sync-1"))
//! client.subscribe([weida_mqtt.Subscription("room/+", 1)])
//! client.publish(weida_mqtt.Message("room/12", b"21.5", qos=2))
//!
//! # Where the asynchronous surface has cancellation, this has a deadline.
//! delivery = deliveries.recv(timeout=5.0)
//! print(delivery.topic, delivery.payload)
//!
//! client.disconnect()
//! ```
//!
//! # It is a facade over a facade, and implements nothing
//!
//! `weida-mqtt`'s own `blocking` module is the synchronous MQTT: a `block_on`
//! around each of the asynchronous client's methods, with the receive deadline
//! where cancellation would be and **no second implementation of any protocol
//! behaviour**. This module is that module with Python argument conversion
//! around it. The QoS state machines, the session, the send quota, the topic
//! aliases, the keep-alive timer and every refusal are decided once, in the
//! asynchronous client, which is why the synchronous surface cannot disagree
//! with the asynchronous one
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.4
//! item 3, [0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! The classes a caller *passes* are the same objects in both surfaces —
//! `weida_mqtt.Message`, `weida_mqtt.Subscription`, `weida_mqtt.ConnectOptions`
//! — and so are the ones it gets back, `Delivery` and `Completion`. Only the
//! three handles differ, because only they differ: `sync.Context`,
//! `sync.Client` and `sync.Deliveries` block where their asynchronous
//! counterparts return a coroutine.
//!
//! # The GIL is released while blocked
//!
//! Every call here parks the calling thread until the operation finishes, and
//! does so inside [`Python::detach`], so other Python threads run while one is
//! waiting in `recv`. A blocking binding that held the GIL would make a second
//! thread pointless — and the usual shape of a synchronous MQTT program, one
//! thread publishing and one consuming, would be a program that does one at a
//! time.
//!
//! The reactor belongs to the context, so no asyncio loop exists in such a
//! process and none is needed.

use std::sync::Mutex;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyModule;
use weida_mqtt::DisconnectReasonCode;
use weida_mqtt::blocking::{BlockingClient, BlockingContext, Deliveries};

use crate::client;
use crate::errors::raise;
use crate::options::PyConnectOptions;
use crate::values::{PyCompletion, PyDelivery, PyMessage, PySubscription};

/// A timeout in seconds, refusing a negative or non-finite one.
fn limit(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    match seconds {
        None => Ok(None),
        Some(value) if value.is_finite() && value >= 0.0 => {
            Ok(Some(Duration::from_secs_f64(value)))
        }
        Some(value) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "a timeout is a finite, non-negative number of seconds, not {value}"
        ))),
    }
}

/// `weida_mqtt.sync.Context`: a context that owns its reactor.
#[pyclass(frozen, name = "Context", module = "weida_mqtt.sync")]
pub struct SyncContext {
    inner: BlockingContext,
}

#[pymethods]
impl SyncContext {
    /// A context owning a reactor of `worker_threads` threads.
    ///
    /// The reactor is the library's and the caller never sees it: no asyncio
    /// loop is involved and none is needed.
    #[new]
    #[pyo3(signature = (*, worker_threads = None))]
    fn new(py: Python<'_>, worker_threads: Option<usize>) -> PyResult<SyncContext> {
        let threads = worker_threads.unwrap_or_else(client::num_threads);
        let inner = raise(py, BlockingContext::owned(threads))?;
        Ok(SyncContext { inner })
    }

    /// Connects, blocking until the CONNACK arrives, and returns
    /// `(client, deliveries)`.
    #[pyo3(signature = (address, options = None))]
    fn connect(
        &self,
        py: Python<'_>,
        address: &str,
        options: Option<&PyConnectOptions>,
    ) -> PyResult<(SyncClient, SyncDeliveries)> {
        let connect_options = PyConnectOptions::resolve(options)?;
        let connected = py.detach(|| self.inner.connect(address, connect_options));
        let (client, deliveries) = raise(py, connected)?;
        Ok((
            SyncClient {
                inner: Mutex::new(client),
            },
            SyncDeliveries {
                inner: Mutex::new(deliveries),
            },
        ))
    }

    /// Connects on a session the caller holds, which is what makes a later
    /// reconnect a **resumption**.
    #[pyo3(signature = (address, session, options = None))]
    fn connect_session(
        &self,
        py: Python<'_>,
        address: &str,
        session: &crate::client::PySession,
        options: Option<&PyConnectOptions>,
    ) -> PyResult<(SyncClient, SyncDeliveries)> {
        let connect_options = PyConnectOptions::resolve(options)?;
        let session = session.inner().clone();
        let connected = py.detach(|| {
            self.inner
                .connect_session(address, connect_options, &session)
        });
        let (client, deliveries) = raise(py, connected)?;
        Ok((
            SyncClient {
                inner: Mutex::new(client),
            },
            SyncDeliveries {
                inner: Mutex::new(deliveries),
            },
        ))
    }

    fn __repr__(&self) -> String {
        "sync.Context()".to_owned()
    }
}

/// `weida_mqtt.sync.Client`.
///
/// The `Mutex` is not about the protocol: `BlockingClient`'s methods take
/// `&self` and the library is already safe for concurrent calls. It is here
/// because a Python object is shared by reference and two threads calling one
/// blocking method would each want the GIL released, which `Python::detach`
/// gives them — so the lock is what keeps two *Python* callers from
/// interleaving two `block_on`s on one connection's command channel and
/// reading each other's answers.
#[pyclass(frozen, name = "Client", module = "weida_mqtt.sync")]
pub struct SyncClient {
    inner: Mutex<BlockingClient>,
}

impl SyncClient {
    fn with<T>(&self, body: impl FnOnce(&BlockingClient) -> T) -> T {
        let client = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        body(&client)
    }
}

#[pymethods]
impl SyncClient {
    /// Publishes, blocking until the hop has certified what it is going to.
    ///
    /// There is deliberately **no timeout argument**. A publish abandoned
    /// mid-exchange is still session state on both sides, so a deadline here
    /// would return control while the exchange continued — which the
    /// asynchronous surface's cancellation does and is honest about, and which
    /// an argument named `timeout` would not be. A caller who wants that runs
    /// the publish on its own thread.
    fn publish(&self, py: Python<'_>, message: &PyMessage) -> PyResult<PyCompletion> {
        let message = message.inner().clone();
        let published = py.detach(|| self.with(|client| client.publish(message)));
        raise(py, published).map(PyCompletion::of)
    }

    /// Subscribes, blocking until the SUBACK arrives, with one granted code
    /// per filter in the order the filters were sent.
    #[pyo3(signature = (subscriptions, *, subscription_id = None))]
    fn subscribe(
        &self,
        py: Python<'_>,
        subscriptions: Vec<PyRef<'_, PySubscription>>,
        subscription_id: Option<u32>,
    ) -> PyResult<Vec<u16>> {
        let subscriptions: Vec<_> = subscriptions
            .iter()
            .map(|subscription| subscription.inner().clone())
            .collect();
        let granted =
            py.detach(|| self.with(|client| client.subscribe_with(subscriptions, subscription_id)));
        raise(py, granted).map(|codes| codes.iter().map(|code| u16::from(code.as_byte())).collect())
    }

    /// Unsubscribes, blocking until the UNSUBACK arrives.
    fn unsubscribe(&self, py: Python<'_>, filters: Vec<String>) -> PyResult<Vec<u16>> {
        let codes = py.detach(|| self.with(|client| client.unsubscribe(filters)));
        raise(py, codes).map(|codes| codes.iter().map(|code| u16::from(code.as_byte())).collect())
    }

    /// Sends DISCONNECT and closes.
    #[pyo3(signature = (reason_code = 0x00, *, session_expiry = None))]
    fn disconnect(
        &self,
        py: Python<'_>,
        reason_code: u8,
        session_expiry: Option<f64>,
    ) -> PyResult<()> {
        let code = DisconnectReasonCode::from_byte(reason_code).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "0x{reason_code:02X} is not a DISCONNECT reason code (3.14.2.1)"
            ))
        })?;
        let expiry = limit(session_expiry)?;
        let sent = py.detach(|| self.with(|client| client.disconnect_with(code, expiry)));
        raise(py, sent)
    }

    /// Sends PINGREQ now, whatever the keep-alive timer thinks.
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let sent = py.detach(|| self.with(|client| client.ping()));
        raise(py, sent)
    }

    /// Re-authenticates on the live connection ([MQTT-4.12.1-1]).
    fn reauthenticate(&self, py: Python<'_>) -> PyResult<()> {
        let done = py.detach(|| self.with(|client| client.reauthenticate()));
        raise(py, done)
    }

    /// The Client Identifier in force.
    #[getter]
    fn client_id(&self) -> String {
        self.with(|client| client.client().client_id().to_owned())
    }

    /// `Session Present` from CONNACK.
    #[getter]
    fn session_present(&self) -> bool {
        self.with(|client| client.client().session_present())
    }

    /// Whether the transport is encrypted. Always `False`: this binding is
    /// built without the library's `tls` feature.
    #[getter]
    fn is_encrypted(&self) -> bool {
        self.with(|client| client.client().is_encrypted())
    }

    fn __repr__(&self) -> String {
        format!("sync.Client(client_id={:?})", self.client_id())
    }
}

/// `weida_mqtt.sync.Deliveries`: the deliveries of one connection, blocking.
///
/// It is also a plain Python iterator, so `for delivery in deliveries:` works
/// and ends when the connection does — the synchronous mirror of the
/// asynchronous surface's `async for`.
#[pyclass(frozen, name = "Deliveries", module = "weida_mqtt.sync")]
pub struct SyncDeliveries {
    inner: Mutex<Deliveries>,
}

#[pymethods]
impl SyncDeliveries {
    /// The next delivery, blocking until one arrives.
    ///
    /// `timeout` is where the asynchronous surface's cancellation goes: a
    /// coroutine that wants to stop waiting cancels its task, and a blocking
    /// caller has no task. The expiry raises `Timeout` and leaves the
    /// connection alone — nothing was consumed, and the next call waits again.
    ///
    /// The GIL is released while waiting, so a second Python thread runs.
    #[pyo3(signature = (*, timeout = None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<PyDelivery> {
        let limit = limit(timeout)?;
        let received = py.detach(|| {
            let mut deliveries = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match limit {
                Some(limit) => deliveries.recv_timeout(limit),
                None => deliveries.recv(),
            }
        });
        raise(py, received).map(PyDelivery::of)
    }

    fn __iter__(slf: PyRef<'_, SyncDeliveries>) -> PyRef<'_, SyncDeliveries> {
        slf
    }

    /// The next delivery, ending the iteration when the connection ends
    /// normally and raising the reason code where the server said why — the
    /// same split the asynchronous iterator makes.
    fn __next__(&self, py: Python<'_>) -> PyResult<PyDelivery> {
        match self.recv(py, None) {
            Ok(delivery) => Ok(delivery),
            Err(error) => {
                // `NotConnected` is the end of the stream rather than a
                // failure: the connection task finished and every event has
                // been taken. Anything else — a server DISCONNECT above all —
                // is a failure a caller must see.
                let ended = error
                    .value(py)
                    .getattr("errno")
                    .and_then(|errno| errno.extract::<String>())
                    .is_ok_and(|errno| errno == "NotConnected");
                if ended {
                    Err(pyo3::exceptions::PyStopIteration::new_err(
                        "the connection ended and every delivery has been taken",
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }

    fn __repr__(&self) -> String {
        "sync.Deliveries()".to_owned()
    }
}

/// Builds `weida_mqtt.sync` and hangs it off the parent module.
pub fn install(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "sync")?;
    module.add_class::<SyncContext>()?;
    module.add_class::<SyncClient>()?;
    module.add_class::<SyncDeliveries>()?;
    // A submodule created this way is not in `sys.modules`, so
    // `from weida_mqtt import sync` works and `import weida_mqtt.sync` would
    // not. Registering it is what makes both spellings the same module rather
    // than one of them an ImportError.
    parent
        .py()
        .import("sys")?
        .getattr("modules")?
        .set_item("weida_mqtt.sync", &module)?;
    parent.add_submodule(&module)?;
    Ok(())
}
