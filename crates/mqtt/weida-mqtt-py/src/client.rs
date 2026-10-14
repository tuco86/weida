//! The context, the client and the delivery stream.
//!
//! # Three objects and not one
//!
//! `Context` owns the reactor, `Client` is the application's end of one
//! connection, and `Events` is the stream of what arrived on it. They are
//! separate because their lifetimes are: a `Client` dies with its connection,
//! a `Context` outlives many, and an `Events` may be handed to a different
//! coroutine from the one that publishes — which is the shape an MQTT program
//! actually has, one task publishing and one consuming.
//!
//! # The GIL and the event loop
//!
//! Every call is a coroutine. Nothing blocks the event loop and nothing holds
//! the GIL while waiting: `Bridge::awaitable` returns as soon as the future is
//! spawned, the future runs on the reactor the context owns with no Python
//! state in it, and the GIL is taken once when the result is converted. A
//! cancelled `asyncio.Task` drops the Rust future, which for a publish means
//! the caller stops waiting — the exchange itself is session state and
//! survives, which is what makes a reconnect a resumption rather than a loss.

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::Mutex;
use weida_mqtt::{Client, Context, DisconnectReasonCode, Event, Events, Session};
use weida_py_core::{Bridge, Errno};

use crate::errors::{self, STOP_ASYNC_ITERATION, errno_of};
use crate::options::PyConnectOptions;
use crate::values::{PyCompletion, PyDelivery, PyMessage, PySubscription};

/// `weida_mqtt.Context`.
///
/// [0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.4 gives
/// a library context three constructors, differing only in where the reactor
/// comes from, and all three mean something to a Python process:
///
/// | Python | Rust | What it is for |
/// | --- | --- | --- |
/// | `Context()` / `Context.owned()` | `Context::owned` | the default, and the only one a plain Python program can use: the context owns a reactor and no asyncio loop drives it |
/// | `Context.current()` | `Context::new` | the *ambient* Tokio reactor. A plain Python process has none, so this raises — the honest answer rather than a hidden second reactor. It is reachable when this module is imported into a process a Rust host already runs a reactor in |
/// | `Context.sharing(other)` | `Context::with_handle` | a second context on the first one's reactor |
#[pyclass(frozen, name = "Context", module = "weida_mqtt")]
pub struct PyContext {
    inner: Context,
    bridge: Bridge,
}

impl PyContext {
    fn wrap(inner: Context) -> PyContext {
        let bridge = Bridge::new(inner.exec().clone(), errors::to_py);
        PyContext { inner, bridge }
    }
}

#[pymethods]
impl PyContext {
    /// A context owning a reactor of `worker_threads` threads.
    #[new]
    #[pyo3(signature = (*, worker_threads = None))]
    fn new(py: Python<'_>, worker_threads: Option<usize>) -> PyResult<PyContext> {
        PyContext::owned(py, worker_threads)
    }

    /// The same, named.
    #[staticmethod]
    #[pyo3(signature = (worker_threads = None))]
    fn owned(py: Python<'_>, worker_threads: Option<usize>) -> PyResult<PyContext> {
        let threads = worker_threads.unwrap_or_else(num_threads);
        let inner = errors::raise(py, Context::owned(threads))?;
        Ok(PyContext::wrap(inner))
    }

    /// A context on the **ambient** Tokio reactor.
    ///
    /// A plain Python process has none and this raises `Runtime`, which is the
    /// honest answer: the alternative is a second reactor nobody asked for.
    #[staticmethod]
    fn current(py: Python<'_>) -> PyResult<PyContext> {
        let inner = errors::raise(py, Context::new())?;
        Ok(PyContext::wrap(inner))
    }

    /// A second context on `other`'s reactor.
    #[staticmethod]
    fn sharing(other: &PyContext) -> PyContext {
        PyContext::wrap(other.inner.clone())
    }

    /// Connects, and returns `(client, events)`.
    ///
    /// The session is created here and dies with the client, so a connection
    /// made this way cannot be resumed. `connect_session` is the one that can.
    #[pyo3(signature = (address, options = None))]
    fn connect<'py>(
        &self,
        py: Python<'py>,
        address: String,
        options: Option<&PyConnectOptions>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let bridge = self.bridge.clone();
        let connect_options = PyConnectOptions::resolve(options)?;
        self.bridge.awaitable(py, async move {
            let (client, events) = Client::connect(&context, &address, connect_options)
                .await
                .map_err(errno_of)?;
            Ok(Connected {
                client: PyClient {
                    inner: Arc::new(client),
                    bridge: bridge.clone(),
                    session: None,
                },
                events: PyEvents {
                    inner: Arc::new(Mutex::new(events)),
                    bridge,
                },
            })
        })
    }

    /// Connects on a `Session` the caller holds, which is what makes a later
    /// reconnect a **resumption**.
    ///
    /// The unacknowledged QoS 1 and 2 exchanges are the session's rather than
    /// the client's, so they survive the connection and are retransmitted on
    /// the next one — once, just after a CONNACK with `Session Present` 1
    /// ([MQTT-4.4.0-1]). There is no retry timer.
    #[pyo3(signature = (address, session, options = None))]
    fn connect_session<'py>(
        &self,
        py: Python<'py>,
        address: String,
        session: &PySession,
        options: Option<&PyConnectOptions>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let bridge = self.bridge.clone();
        let connect_options = PyConnectOptions::resolve(options)?;
        let session = session.inner.clone();
        self.bridge.awaitable(py, async move {
            let (client, events) =
                Client::connect_session(&context, &address, connect_options, &session)
                    .await
                    .map_err(errno_of)?;
            Ok(Connected {
                client: PyClient {
                    inner: Arc::new(client),
                    bridge: bridge.clone(),
                    session: Some(session),
                },
                events: PyEvents {
                    inner: Arc::new(Mutex::new(events)),
                    bridge,
                },
            })
        })
    }

    fn __repr__(&self) -> String {
        "Context()".to_owned()
    }
}

/// One reactor thread per core, which is what a library that owns its reactor
/// should default to and what `Context::owned` needs told.
pub(crate) fn num_threads() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// The `(client, events)` pair a connect resolves to.
struct Connected {
    client: PyClient,
    events: PyEvents,
}

impl<'py> IntoPyObject<'py> for Connected {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let client = Py::new(py, self.client)?;
        let events = Py::new(py, self.events)?;
        Ok((client, events).into_pyobject(py)?.into_any())
    }
}

/// `weida_mqtt.Session`: the client half of an MQTT session.
///
/// It is a separate object because its lifetime is: the session outlives every
/// connection made on it, which is the whole of what "resumption" means. 4.1's
/// client-side session state is the unacknowledged QoS 1 and 2 exchanges, and
/// they live here rather than in a `Client`.
///
/// **It is in memory.** A process that exits loses it, and a process that
/// restarts and reconnects with `clean_start=False` will be told
/// `Session Present` 1 by a broker that still holds its half — with nothing
/// here to match it against, which "MUST close the Network Connection"
/// ([MQTT-3.2.2-4]) and arrives as `SessionPresentWithoutState`. The answer is
/// `clean_start=True`, and that is a real cost of MQTT's session model rather
/// than a shortcoming of this binding.
#[pyclass(frozen, name = "Session", module = "weida_mqtt")]
pub struct PySession {
    inner: Session,
}

impl PySession {
    /// The library's session, as the blocking surface takes it.
    pub fn inner(&self) -> &Session {
        &self.inner
    }
}

#[pymethods]
impl PySession {
    /// A fresh session for `client_id`.
    #[new]
    #[pyo3(signature = (client_id, options = None))]
    fn new(client_id: String, options: Option<&PyConnectOptions>) -> PyResult<PySession> {
        let resolved = PyConnectOptions::resolve(options)?;
        Ok(PySession {
            inner: Session::new(client_id, &resolved.limits),
        })
    }

    /// The Client Identifier this session is keyed by. "It is the key to
    /// session state."
    #[getter]
    fn client_id(&self) -> String {
        self.inner.client_id().to_owned()
    }

    /// Unacknowledged QoS 1 and 2 exchanges: the whole of 4.1's client-side
    /// session state.
    #[getter]
    fn in_flight(&self) -> usize {
        self.inner.in_flight()
    }

    /// How many more QoS 1 or 2 publishes may be in flight before the next one
    /// stalls: the server's `Receive Maximum` minus what is outstanding.
    #[getter]
    fn send_quota(&self) -> u16 {
        self.inner.send_quota()
    }

    /// This client's own declared `Receive Maximum`, which is a **different**
    /// number from `send_quota` and is not overwritten by the server's
    /// (3.1.2.11.3 against 3.2.2.3.3).
    #[getter]
    fn receive_maximum(&self) -> u16 {
        self.inner.receive_maximum()
    }

    /// The filters this session believes it is subscribed to, as
    /// `(filter, granted_qos, subscription_id)` triples.
    ///
    /// A **mirror** and not an authority: 4.1 lists subscriptions under the
    /// server's half of the session, so the server is the one that replaces a
    /// re-subscribed filter. The client keeps this so it can tell which of its
    /// own filters a delivery matched where the server sends no Subscription
    /// Identifier, and it is discarded with the session.
    #[getter]
    fn subscriptions(&self) -> Vec<(String, u8, Option<u32>)> {
        self.inner
            .subscriptions()
            .iter()
            .map(|record| {
                (
                    record.subscription.filter.clone(),
                    record.granted.as_byte(),
                    record.identifier,
                )
            })
            .collect()
    }

    /// Which of this session's filters select `topic`.
    ///
    /// The answer to a server that declared `Subscription Identifiers
    /// Available` 0: MQTT's matcher, run locally. More than one may match,
    /// because overlapping subscriptions are permitted.
    fn matching(&self, topic: &str) -> Vec<String> {
        self.inner
            .subscriptions()
            .matching(topic)
            .map(|record| record.subscription.filter.clone())
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "Session(client_id={:?}, in_flight={}, subscriptions={})",
            self.inner.client_id(),
            self.inner.in_flight(),
            self.inner.subscriptions().len(),
        )
    }
}

/// `weida_mqtt.Client`: the application's end of one connection.
#[pyclass(frozen, name = "Client", module = "weida_mqtt")]
pub struct PyClient {
    inner: Arc<Client>,
    bridge: Bridge,
    /// The session this connection runs on, where the caller gave one. Kept so
    /// `session` can hand it back rather than making a caller thread it
    /// through by hand.
    session: Option<Session>,
}

#[pymethods]
impl PyClient {
    /// Publishes `message`, resolving when the hop has certified what it is
    /// going to.
    ///
    /// QoS 0 resolves at once; QoS 1 on the PUBACK; QoS 2 on the PUBCOMP, or
    /// early on a PUBREC of 0x80 or above. See `Completion` for what each of
    /// those does and does not certify — none of them certifies durability,
    /// and none reaches past this hop.
    ///
    /// **A spent send quota stalls this call rather than being exceeded**
    /// ([MQTT-4.9.0-2]): with the server's `Receive Maximum` spent, the
    /// coroutine waits here and the packet stays off the wire until an
    /// exchange completes. QoS 0 is never counted and never waits.
    fn publish<'py>(&self, py: Python<'py>, message: &PyMessage) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.inner);
        let message = message.inner.clone();
        self.bridge.awaitable(py, async move {
            client
                .publish(message)
                .await
                .map(PyCompletion::of)
                .map_err(errno_of)
        })
    }

    /// Subscribes, resolving on the SUBACK with **one granted code per
    /// filter, in the order the filters were sent** ([MQTT-3.9.3-1]).
    ///
    /// A failure code for one filter leaves the others granted, so this does
    /// not raise when one filter is refused: the codes come back and the
    /// caller reads them. A code of 0x80 or above is a failure for that filter
    /// and 0x00, 0x01, 0x02 are the granted maximum QoS — which may be **less**
    /// than was asked for.
    #[pyo3(signature = (subscriptions, *, subscription_id = None))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        subscriptions: Vec<PyRef<'py, PySubscription>>,
        subscription_id: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.inner);
        let subscriptions: Vec<_> = subscriptions
            .iter()
            .map(|subscription| subscription.inner.clone())
            .collect();
        self.bridge.awaitable(py, async move {
            client
                .subscribe_with(subscriptions, subscription_id)
                .await
                .map(|codes| Codes(codes.iter().map(|code| code.as_byte()).collect()))
                .map_err(errno_of)
        })
    }

    /// Unsubscribes, resolving on the UNSUBACK with one code per filter.
    ///
    /// 0x11 `No subscription existed` is a **success**: the end state the
    /// caller asked for holds either way.
    fn unsubscribe<'py>(
        &self,
        py: Python<'py>,
        filters: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            client
                .unsubscribe(filters)
                .await
                .map(|codes| Codes(codes.iter().map(|code| code.as_byte()).collect()))
                .map_err(errno_of)
        })
    }

    /// Sends DISCONNECT and closes.
    ///
    /// `reason_code` 0x00 is a normal disconnection and tells the server to
    /// **discard** the Will without publishing it ([MQTT-3.14.4-3]); 0x04
    /// asks for the Will anyway, which is the only way a client publishes its
    /// own. `session_expiry` revises the interval at close, which a client MAY
    /// do (3.14.2.2.2) — shortening it to 0 is what a client that is finished
    /// should do so a session it will never return to is not orphaned.
    #[pyo3(signature = (reason_code = 0x00, *, session_expiry = None))]
    fn disconnect<'py>(
        &self,
        py: Python<'py>,
        reason_code: u8,
        session_expiry: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let code = DisconnectReasonCode::from_byte(reason_code).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "0x{reason_code:02X} is not a DISCONNECT reason code (3.14.2.1)"
            ))
        })?;
        let expiry = match session_expiry {
            Some(seconds) if seconds >= 0.0 => Some(Duration::from_secs_f64(seconds)),
            Some(_) => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "session_expiry must be a non-negative number of seconds",
                ));
            }
            None => None,
        };
        let client = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            client.disconnect_with(code, expiry).await.map_err(errno_of)
        })
    }

    /// Sends PINGREQ now, whatever the keep-alive timer thinks.
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.inner);
        self.bridge
            .awaitable(py, async move { client.ping().await.map_err(errno_of) })
    }

    /// Re-authenticates on the live connection ([MQTT-4.12.1-1]).
    ///
    /// Raises `Configuration` where this connection named no `Authentication
    /// Method`: there is nothing to re-authenticate with, and a server "MUST
    /// NOT send AUTH" to such a client.
    fn reauthenticate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            client.reauthenticate().await.map_err(errno_of)
        })
    }

    /// The Client Identifier in force: the one that was sent, or the one the
    /// server assigned to a zero-length request ([MQTT-3.1.3-6]).
    #[getter]
    fn client_id(&self) -> String {
        self.inner.client_id().to_owned()
    }

    /// `Session Present` from CONNACK.
    #[getter]
    fn session_present(&self) -> bool {
        self.inner.session_present()
    }

    /// Whether the transport is encrypted.
    ///
    /// The User Name, the Password and every byte of `Authentication Data`
    /// travel in the CONNECT, and on a plain connection they travel in the
    /// clear — so this is the only question a client can ask about what
    /// protected them. Always `False` here: the binding is built without the
    /// library's `tls` feature, because this surface has no way to hand a
    /// rustls configuration in from Python and the trust anchors are the
    /// caller's.
    #[getter]
    fn is_encrypted(&self) -> bool {
        self.inner.is_encrypted()
    }

    /// The keep-alive interval in force, in seconds: the client's, or the
    /// server's `Server Keep Alive` where it sent one ([MQTT-3.2.2-21]).
    /// `None` means the mechanism is off.
    #[getter]
    fn keep_alive(&self) -> Option<f64> {
        self.inner
            .keep_alive()
            .map(|interval| interval.as_secs_f64())
    }

    /// `Response Information` from CONNACK, where the server offered one.
    #[getter]
    fn response_information(&self) -> Option<String> {
        self.inner.response_information().map(str::to_owned)
    }

    /// A Response Topic under the namespace the server offered, or `None`
    /// where it offered none.
    ///
    /// `None` is the answer and not an omission: without `Response
    /// Information` there is no protocol-level way to learn which topics this
    /// client may publish replies on, and inventing one would be inventing a
    /// namespace the server never granted. A caller in that position knows its
    /// reply topic out of band and sets `Message(response_topic=...)`.
    #[pyo3(signature = (suffix = ""))]
    fn response_topic(&self, py: Python<'_>, suffix: &str) -> PyResult<Option<String>> {
        errors::raise(py, self.inner.response_topic(suffix))
    }

    /// Everything the server declared in CONNACK, with §11's defaults applied
    /// to what it left out.
    ///
    /// A dict rather than attributes because a caller most often logs the lot,
    /// and because a declaration absent from the CONNACK and a declaration
    /// equal to the default are the same thing to a client — which is exactly
    /// what §11's defaults mean and what this dict shows.
    #[getter]
    fn server<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let limits = self.inner.server_limits();
        let dict = PyDict::new(py);
        dict.set_item("maximum_qos", crate::values::qos_byte(limits.maximum_qos))?;
        dict.set_item("retain_available", limits.retain_available)?;
        dict.set_item("receive_maximum", limits.receive_maximum)?;
        dict.set_item("maximum_packet_size", limits.maximum_packet_size)?;
        dict.set_item("topic_alias_maximum", limits.topic_alias_maximum)?;
        dict.set_item(
            "wildcard_subscription_available",
            limits.wildcard_subscription_available,
        )?;
        dict.set_item(
            "subscription_identifiers_available",
            limits.subscription_identifiers_available,
        )?;
        dict.set_item(
            "shared_subscription_available",
            limits.shared_subscription_available,
        )?;
        dict.set_item(
            "server_keep_alive",
            limits
                .server_keep_alive
                .map(|interval| interval.as_secs_f64()),
        )?;
        dict.set_item(
            "session_expiry_interval",
            limits
                .session_expiry_interval
                .map(|interval| interval.as_secs_f64()),
        )?;
        dict.set_item(
            "assigned_client_identifier",
            limits.assigned_client_identifier.as_deref(),
        )?;
        dict.set_item(
            "response_information",
            limits.response_information.as_deref(),
        )?;
        dict.set_item("server_reference", limits.server_reference.as_deref())?;
        dict.set_item("reason_string", limits.reason_string.as_deref())?;
        Ok(dict)
    }

    /// The session this connection runs on, where one was given to
    /// `connect_session`.
    #[getter]
    fn session(&self, py: Python<'_>) -> PyResult<Option<Py<PySession>>> {
        match &self.session {
            Some(session) => Py::new(
                py,
                PySession {
                    inner: session.clone(),
                },
            )
            .map(Some),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Client(client_id={:?}, session_present={})",
            self.inner.client_id(),
            self.inner.session_present()
        )
    }
}

/// `weida_mqtt.Events`: the stream of what arrived on one connection.
///
/// `async for delivery in events:` yields deliveries and nothing else; the two
/// other events are reported differently, because they are not deliveries:
///
/// * the connection ending raises the failure that ended it, which is what
///   turns a closed socket into a named reason code;
/// * an exchange resumed from a previous connection completing has no handle
///   left to resolve, so it is dropped from the **iterator** and available
///   through `next_event`, which yields either kind.
///
/// A caller that wants all three uses `next_event`; a caller that wants
/// messages uses the iterator, which is what most callers want.
#[pyclass(frozen, name = "Events", module = "weida_mqtt")]
pub struct PyEvents {
    inner: Arc<Mutex<Events>>,
    bridge: Bridge,
}

#[pymethods]
impl PyEvents {
    fn __aiter__(slf: PyRef<'_, PyEvents>) -> PyRef<'_, PyEvents> {
        slf
    }

    /// The next **delivery**, skipping a resumed exchange's completion.
    ///
    /// Ends with `StopAsyncIteration` when the connection ends normally, and
    /// raises the reason code when it ends because the server said why — which
    /// is the distinction 5.0 exists to make and 3.1.1 could not.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let mut events = events.lock().await;
            loop {
                match events.next().await {
                    Some(Event::Delivered(delivery)) => return Ok(PyDelivery::of(delivery)),
                    // A resumed exchange completing is not a delivery. It is
                    // not dropped: `next_event` yields it.
                    Some(Event::Completed { .. }) => continue,
                    Some(Event::Disconnected(error)) => return Err(errno_of(error)),
                    // `Event` is `#[non_exhaustive]`: a variant added to the
                    // library must not silently become a delivery here, and a
                    // skipped unknown event is the only safe reading of "this
                    // iterator yields deliveries".
                    Some(_) => continue,
                    None => {
                        return Err(Errno::new(
                            STOP_ASYNC_ITERATION,
                            "the connection task has finished and every event has been taken",
                        ));
                    }
                }
            }
        })
    }

    /// The next event of **any** kind, as a `(kind, value)` pair:
    /// `("delivered", Delivery)`, `("completed", (packet_id, Completion))`, or
    /// `None` once the connection task has finished and every event has been
    /// taken.
    ///
    /// The connection ending raises, as in the iterator: a server DISCONNECT
    /// carries a reason code and a caller that only saw `None` would have lost
    /// it.
    fn next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = Arc::clone(&self.inner);
        self.bridge.awaitable(py, async move {
            let mut events = events.lock().await;
            match events.next().await {
                Some(Event::Delivered(delivery)) => Ok(Some(Reported::Delivered(delivery))),
                Some(Event::Completed {
                    packet_id,
                    completion,
                }) => Ok(Some(Reported::Completed {
                    packet_id,
                    completion,
                })),
                Some(Event::Disconnected(error)) => Err(errno_of(error)),
                // A variant the library added and this binding does not know.
                // Reported as such rather than mapped onto one of the two
                // above, because guessing which is what a caller would then
                // have to un-guess.
                Some(other) => Err(Errno::new(
                    "Configuration",
                    format!(
                        "this binding does not know the event {other:?}; \
                         weida-mqtt-py is older than weida-mqtt"
                    ),
                )),
                None => Ok(None),
            }
        })
    }

    fn __repr__(&self) -> String {
        "Events()".to_owned()
    }
}

/// The per-filter reason codes of a SUBACK or an UNSUBACK.
///
/// A newtype rather than `Vec<u8>`, because PyO3 maps `Vec<u8>` to `bytes` —
/// which is right for a payload and wrong here: a caller writes
/// `granted == [2]` and compares a **list of integers**, not a byte string.
/// The codes are bytes on the wire and integers in Python, and this is where
/// the two part company.
struct Codes(Vec<u8>);

impl<'py> IntoPyObject<'py> for Codes {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let codes: Vec<u16> = self.0.into_iter().map(u16::from).collect();
        Ok(codes.into_pyobject(py)?.into_any())
    }
}

/// One event, as the `(kind, value)` pair `next_event` yields.
enum Reported {
    Delivered(weida_mqtt::Delivery),
    Completed {
        packet_id: u16,
        completion: weida_mqtt::Completion,
    },
}

impl<'py> IntoPyObject<'py> for Reported {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Reported::Delivered(delivery) => {
                let value = Py::new(py, PyDelivery::of(delivery))?;
                Ok(("delivered", value).into_pyobject(py)?.into_any())
            }
            Reported::Completed {
                packet_id,
                completion,
            } => {
                let value = Py::new(py, PyCompletion::of(completion))?;
                Ok(("completed", (packet_id, value))
                    .into_pyobject(py)?
                    .into_any())
            }
        }
    }
}
