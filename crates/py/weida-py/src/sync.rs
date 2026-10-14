//! `weida.sync`: weida for a Python program with no event loop.
//!
//! ```python
//! from weida import Identity, Trust, sync
//!
//! runtime = sync.Runtime()
//! binding = runtime.bind("127.0.0.1:0", Identity.generate())
//! replier = binding.replier("/echo")
//!
//! client = sync.Runtime()
//! requester = client.requester(Trust.by_address())
//! requester.connect(binding.url("/echo"))
//! # ... in another thread, or before the request:
//! #   request = replier.accept(1 << 20); request.reply(request.payload)
//! ```
//!
//! # It implements nothing, and the `block_on` is not here
//!
//! This module is `weida::blocking` — not a link, because a feature-gated
//! item is a hard rustdoc error in the configuration that lacks it —
//! with argument conversion around it, and
//! that is the whole of it. The facade is where the `block_on` lives, where
//! the reactor is owned, and where the one refusal that matters is made: a
//! call from inside a Tokio runtime would park the worker that has to drive
//! what it is waiting for, so every entry point there checks for an ambient
//! runtime and fails with `weida.Runtime` instead of deadlocking. A Python
//! process has no ambient runtime, so the check is silent here — and it stays
//! correct in a process that embeds one.
//!
//! Building the `block_on` again in this file would have been the second
//! implementation [0013](../../../../docs/decisions/0013-competitor-libraries.md)
//! §4.4 exists to prevent, and the order the other five bindings used —
//! library facade first, binding `sync` module second — is what avoids it
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! # The GIL is released while a call waits
//!
//! Every call below runs inside [`Python::detach`], so other Python threads
//! run while one is parked in `accept` or `recv`. A blocking binding that
//! held the GIL would make a second thread pointless, and the usual shape of
//! a synchronous messaging program — one thread per endpoint — would be a
//! program that serves one endpoint at a time.
//!
//! # What has no synchronous form here
//!
//! The streamed surface: `open`, `recv_stream` and `FanOut`. The blocking
//! facade takes whole payloads by design, because a synchronous caller that
//! wants to stream wants the asyncio surface or the Rust API — and a `sync`
//! module that invented its own streaming would be inventing a second
//! facade. The value classes are shared, not copied: `weida.Trust`,
//! `weida.Identity`, `weida.IncomingMeta` and `weida.Survey` are the same
//! objects on both surfaces.
//!
//! # The one call whose signature differs, and why
//!
//! `Cursors.changed(seconds)` takes a **deadline** where the asyncio
//! `Cursors.changed()` takes nothing. There `asyncio.wait_for` bounds the
//! wait and composes; here a parked thread is interrupted by nothing — and
//! the wait is genuinely unbounded, because a peer that never reports opens
//! no stream, a report that never starts never ends, and a connection both
//! sides keep alive never closes. A deadline that passes raises
//! `TimeoutError`, the builtin, because it is **not** the end of the report:
//! returning `None` for it would make a caller stop reading a verdict that is
//! still coming.

use std::collections::BTreeMap;
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyModule;
use weida::blocking;
use weida_py_core::{payload_of, py_bytes};

use crate::cursors::{level_of, reporting_meta, set_of};
use crate::errors::raise;
use crate::values::{
    PyConnectionStats, PyDishDrops, PyIdentity, PyIncomingMeta, PySurvey, PyTrust,
};

/// What a call on a spent runtime gets.
fn spent<T>(py: Python<'_>, what: &str) -> PyResult<T> {
    raise(
        py,
        Err(weida::Error::Runtime(format!(
            "this {what} was already shut down"
        ))),
    )
}

/// A latency budget in seconds, or none.
fn seconds(py: Python<'_>, max_age: Option<f64>) -> PyResult<Option<std::time::Duration>> {
    raise(
        py,
        max_age
            .map(std::time::Duration::try_from_secs_f64)
            .transpose()
            .map_err(|e| weida::Error::Runtime(format!("max_age: {e}"))),
    )
}

/// `weida.sync.Runtime`: a reactor of the library's own, driven by its caller.
#[pyclass(frozen, name = "Runtime", module = "weida.sync")]
pub struct SyncRuntime {
    /// `None` after `shutdown` or `drain`, both of which consume the facade's
    /// runtime.
    runtime: Mutex<Option<blocking::Runtime>>,
}

#[pymethods]
impl SyncRuntime {
    /// A runtime with `worker_threads` reactor threads.
    ///
    /// # Errors
    ///
    /// `weida.Runtime` when `worker_threads` is `0`, when the OS refuses the
    /// threads, or when this is called from inside a Tokio runtime — which
    /// would deadlock on the first call.
    #[new]
    #[pyo3(signature = (worker_threads=1, datagrams=false, path_report=false))]
    fn new(
        py: Python<'_>,
        worker_threads: usize,
        datagrams: bool,
        path_report: bool,
    ) -> PyResult<SyncRuntime> {
        let config = weida::RuntimeConfig {
            worker_threads,
            limits: crate::limits_with(datagrams, path_report),
            ..weida::RuntimeConfig::default()
        };
        let runtime = raise(py, py.detach(|| blocking::Runtime::new(config)))?;
        Ok(SyncRuntime {
            runtime: Mutex::new(Some(runtime)),
        })
    }

    /// Binds a QUIC socket; port `0` lets the kernel choose.
    fn bind(&self, py: Python<'_>, addr: &str, identity: PyIdentity) -> PyResult<SyncBinding> {
        let socket: std::net::SocketAddr = raise(
            py,
            addr.parse()
                .map_err(|e| weida::Error::InvalidAddress(format!("{addr:?}: {e}"))),
        )?;
        let fingerprint = raise(py, identity.identity.fingerprint())?.to_string();
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        let Some(runtime) = guard.as_ref() else {
            return spent(py, "runtime");
        };
        let binding = raise(
            py,
            py.detach(|| runtime.bind_quic(socket, identity.identity.clone())),
        )?;
        let local = binding
            .local_addr()
            .expect("a QUIC binding has an address")
            .to_string();
        Ok(SyncBinding {
            binding,
            url_base: format!("weida://{fingerprint}@{local}"),
            local,
            fingerprint: Some(fingerprint),
        })
    }

    /// Binds an in-process bus, which a client in this process dials as
    /// `weida+inproc://<bus>/<path>`.
    fn bind_inproc(&self, py: Python<'_>, bus: &str) -> PyResult<SyncBinding> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        let Some(runtime) = guard.as_ref() else {
            return spent(py, "runtime");
        };
        let binding = raise(py, runtime.bind_inproc(bus))?;
        Ok(SyncBinding {
            binding,
            local: bus.to_owned(),
            fingerprint: None,
            url_base: format!("weida+inproc://{bus}"),
        })
    }

    /// A requester on this runtime.
    fn requester(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncRequester> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        match guard.as_ref() {
            Some(runtime) => Ok(SyncRequester {
                endpoint: runtime.requester(trust.trust.clone()),
            }),
            None => spent(py, "runtime"),
        }
    }

    /// A pusher on this runtime.
    fn pusher(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncPusher> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        match guard.as_ref() {
            Some(runtime) => Ok(SyncPusher {
                endpoint: runtime.pusher(trust.trust.clone()),
            }),
            None => spent(py, "runtime"),
        }
    }

    /// A subscriber on this runtime.
    fn subscriber(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncSubscriber> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        match guard.as_ref() {
            Some(runtime) => Ok(SyncSubscriber {
                endpoint: runtime.subscriber(trust.trust.clone()),
            }),
            None => spent(py, "runtime"),
        }
    }

    /// A dialling pair on this runtime.
    fn pair(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncPaired> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        match guard.as_ref() {
            Some(runtime) => Ok(SyncPaired {
                endpoint: runtime.pair(trust.trust.clone()),
            }),
            None => spent(py, "runtime"),
        }
    }

    /// A surveyor on this runtime.
    fn surveyor(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncSurveyor> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        match guard.as_ref() {
            Some(runtime) => Ok(SyncSurveyor {
                endpoint: runtime.surveyor(trust.trust.clone()),
            }),
            None => spent(py, "runtime"),
        }
    }

    /// A dish on this runtime.
    fn dish(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncDish> {
        let guard = self.runtime.lock().expect("runtime lock poisoned");
        match guard.as_ref() {
            Some(runtime) => Ok(SyncDish {
                endpoint: runtime.dish(trust.trust.clone()),
            }),
            None => spent(py, "runtime"),
        }
    }

    /// Stops admitting work and waits up to `deadline` seconds for finished
    /// transfers to reach the peer's transport, returning
    /// `(delivered, outstanding)`.
    fn drain(&self, py: Python<'_>, deadline: f64) -> PyResult<(u64, u64)> {
        let seconds = raise(
            py,
            std::time::Duration::try_from_secs_f64(deadline)
                .map_err(|e| weida::Error::Runtime(format!("deadline: {e}"))),
        )?;
        let taken = self.runtime.lock().expect("runtime lock poisoned").take();
        match taken {
            Some(runtime) => {
                let drained = raise(py, py.detach(|| runtime.drain(seconds)))?;
                Ok((drained.delivered, drained.outstanding))
            }
            None => spent(py, "runtime"),
        }
    }

    /// Closes every connection without waiting for anything in flight.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let taken = self.runtime.lock().expect("runtime lock poisoned").take();
        match taken {
            Some(runtime) => raise(py, py.detach(|| runtime.shutdown())),
            None => spent(py, "runtime"),
        }
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Runtime>".to_owned()
    }
}

/// `weida.sync.Binding`: a bound socket or in-process bus and the endpoints
/// on it.
#[pyclass(frozen, name = "Binding", module = "weida.sync")]
pub struct SyncBinding {
    binding: blocking::Binding,
    /// The socket address, or the bus name.
    local: String,
    /// `None` on an in-process bus.
    fingerprint: Option<String>,
    /// What a client's URL starts with, before the path.
    url_base: String,
}

#[pymethods]
impl SyncBinding {
    /// The address the socket is bound to, with the port the kernel chose;
    /// the bus name for an in-process binding.
    fn local_addr(&self) -> &str {
        &self.local
    }

    /// The fingerprint a client pins; `None` for an in-process binding.
    fn fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }

    /// The address a client dials for `path`.
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.url_base)
    }

    /// Registers a replier at `path`.
    fn replier(&self, py: Python<'_>, path: &str) -> PyResult<SyncReplier> {
        Ok(SyncReplier {
            endpoint: raise(py, self.binding.replier(path))?,
        })
    }

    /// Registers a puller at `path`.
    fn puller(&self, py: Python<'_>, path: &str) -> PyResult<SyncPuller> {
        Ok(SyncPuller {
            endpoint: raise(py, self.binding.puller(path))?,
        })
    }

    /// Registers a publisher at `path`.
    fn publisher(&self, py: Python<'_>, path: &str) -> PyResult<SyncPublisher> {
        Ok(SyncPublisher {
            endpoint: raise(py, self.binding.publisher(path))?,
        })
    }

    /// Registers a bound pair at `path`.
    fn pair(&self, py: Python<'_>, path: &str) -> PyResult<SyncPaired> {
        Ok(SyncPaired {
            endpoint: raise(py, self.binding.pair(path))?,
        })
    }

    /// Registers a respondent at `path`.
    fn respondent(&self, py: Python<'_>, path: &str) -> PyResult<SyncRespondent> {
        Ok(SyncRespondent {
            endpoint: raise(py, self.binding.respondent(path))?,
        })
    }

    /// Registers a bus member at `path`, dialling others on `trust`'s terms.
    fn bus(&self, py: Python<'_>, path: &str, trust: PyTrust) -> PyResult<SyncBusMember> {
        Ok(SyncBusMember {
            endpoint: raise(py, self.binding.bus(path, trust.trust.clone()))?,
        })
    }

    /// Registers a radio at `path`.
    fn radio(&self, py: Python<'_>, path: &str) -> PyResult<SyncRadio> {
        Ok(SyncRadio {
            endpoint: raise(py, self.binding.radio(path))?,
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Binding {}>", self.local)
    }
}

/// `weida.sync.Requester`.
#[pyclass(frozen, name = "Requester", module = "weida.sync")]
pub struct SyncRequester {
    endpoint: blocking::Requester,
}

#[pymethods]
impl SyncRequester {
    /// Dials `url`; see the asyncio `Requester.connect` for the forms.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection, labelled by the URL
    /// as dialled; empty when none is live.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Sends `payload` and returns the reply, at most `max_reply_bytes`.
    fn request<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        max_reply_bytes: usize,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let body = payload_of(payload)?;
        let reply = raise(
            py,
            py.detach(|| self.endpoint.request(&body, max_reply_bytes)),
        )?;
        Ok(py_bytes(py, &reply))
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Requester>".to_owned()
    }
}

/// `weida.sync.Pusher`.
#[pyclass(frozen, name = "Pusher", module = "weida.sync")]
pub struct SyncPusher {
    endpoint: blocking::Pusher,
}

#[pymethods]
impl SyncPusher {
    /// Dials `url`.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Sends `payload` and waits for the peer's transport to acknowledge it.
    ///
    /// `report` and `mode` order a cursor report, as on the asyncio surface,
    /// and the call then returns the `weida.Cursors` to read it on — `None`
    /// when nothing was ordered.
    #[pyo3(signature = (payload, report=None, mode=0))]
    fn send(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        report: Option<Vec<u64>>,
        mode: u64,
    ) -> PyResult<Option<SyncCursors>> {
        let body = payload_of(payload)?;
        let meta = reporting_meta(py, Some(body.len() as u64), report, mode)?;
        let cursors = raise(py, py.detach(|| self.endpoint.send_reporting(meta, &body)))?;
        Ok(cursors.map(|cursors| SyncCursors {
            cursors: Mutex::new(cursors),
        }))
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Pusher>".to_owned()
    }
}

/// `weida.sync.Subscriber`.
#[pyclass(frozen, name = "Subscriber", module = "weida.sync")]
pub struct SyncSubscriber {
    endpoint: blocking::Subscriber,
}

#[pymethods]
impl SyncSubscriber {
    /// Dials `url`.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Subscribes to `filter`; the empty filter takes every topic.
    fn subscribe(&self, py: Python<'_>, filter: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.subscribe(filter)))
    }

    /// Withdraws one filter.
    fn unsubscribe(&self, py: Python<'_>, filter: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.unsubscribe(filter)))
    }

    /// Waits for the next published message, at most `max_bytes`, and
    /// returns `(payload, meta)`.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = raise(py, py.detach(|| self.endpoint.recv(max_bytes)))?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
        ))
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Subscriber>".to_owned()
    }
}

/// `weida.sync.Replier`.
#[pyclass(frozen, name = "Replier", module = "weida.sync")]
pub struct SyncReplier {
    endpoint: blocking::Replier,
}

#[pymethods]
impl SyncReplier {
    /// The endpoint path this replier serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next request and reads its body, at most `max_bytes`.
    fn accept(&self, py: Python<'_>, max_bytes: usize) -> PyResult<SyncRequest> {
        let request = raise(py, py.detach(|| self.endpoint.accept(max_bytes)))?;
        Ok(SyncRequest {
            request: Mutex::new(Some(request)),
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Replier {}>", self.endpoint.path())
    }
}

/// `weida.sync.Request`: an accepted exchange and the reply it owes.
#[pyclass(frozen, name = "Request", module = "weida.sync")]
pub struct SyncRequest {
    request: Mutex<Option<blocking::Request>>,
}

#[pymethods]
impl SyncRequest {
    /// The request payload.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let guard = self.request.lock().expect("request lock poisoned");
        match guard.as_ref() {
            Some(request) => Ok(py_bytes(py, &request.message().payload)),
            None => spent(py, "request"),
        }
    }

    /// What the DATA header said, including the proved peer.
    #[getter]
    fn meta(&self, py: Python<'_>) -> PyResult<PyIncomingMeta> {
        let guard = self.request.lock().expect("request lock poisoned");
        match guard.as_ref() {
            Some(request) => Ok(PyIncomingMeta::of(&request.message().meta)),
            None => spent(py, "request"),
        }
    }

    /// Answers with `payload`.
    fn reply(&self, py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<()> {
        let body = payload_of(payload)?;
        let taken = self.request.lock().expect("request lock poisoned").take();
        match taken {
            Some(request) => raise(py, py.detach(|| request.reply(&body))),
            None => spent(py, "request"),
        }
    }

    /// Declines the request, so the requester gets `weida.Rejected`.
    fn refuse(&self, py: Python<'_>) -> PyResult<()> {
        let taken = self.request.lock().expect("request lock poisoned").take();
        match taken {
            Some(request) => raise(py, py.detach(|| request.refuse(weida::ErrorCode::Rejected))),
            None => spent(py, "request"),
        }
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Request>".to_owned()
    }
}

/// `weida.sync.Puller`.
#[pyclass(frozen, name = "Puller", module = "weida.sync")]
pub struct SyncPuller {
    endpoint: blocking::Puller,
}

#[pymethods]
impl SyncPuller {
    /// The endpoint path this puller serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next transfer and reads it, at most `max_bytes`,
    /// returning `(payload, meta)`.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = raise(py, py.detach(|| self.endpoint.recv(max_bytes)))?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
        ))
    }

    /// `recv`, plus the reporter the sender ordered:
    /// `(payload, meta, reporter)`.
    ///
    /// The third element is `None` unless the sender ordered a report. This
    /// is the call a staged receiver wants, for the reason the asyncio
    /// `Puller.recv_reporting` gives.
    fn recv_reporting<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(
        Bound<'py, pyo3::types::PyBytes>,
        PyIncomingMeta,
        Option<SyncReporter>,
    )> {
        let (message, reporter) = raise(py, py.detach(|| self.endpoint.recv_reporting(max_bytes)))?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
            reporter.map(|reporter| SyncReporter {
                reporter: Mutex::new(Some(reporter)),
            }),
        ))
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Puller {}>", self.endpoint.path())
    }
}

/// `weida.sync.Publisher`.
#[pyclass(frozen, name = "Publisher", module = "weida.sync")]
pub struct SyncPublisher {
    endpoint: blocking::Publisher,
}

#[pymethods]
impl SyncPublisher {
    /// The endpoint path this publisher serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Fans `payload` out to every matching subscriber and returns how many
    /// it was enqueued for. Never waits, on either surface.
    fn publish(&self, py: Python<'_>, topic: &str, payload: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(payload)?;
        raise(py, self.endpoint.publish(topic, body))
    }

    /// Subscribers currently connected.
    fn subscriber_count(&self) -> usize {
        self.endpoint.subscriber_count()
    }

    /// Copies dropped because a subscriber could not take them.
    fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Publisher {}>", self.endpoint.path())
    }
}

/// `weida.sync.Paired`: one peer, both directions.
#[pyclass(frozen, name = "Paired", module = "weida.sync")]
pub struct SyncPaired {
    endpoint: blocking::Paired,
}

#[pymethods]
impl SyncPaired {
    /// The endpoint path this pair uses.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Dials `url`, once; a second call is `weida.LimitExceeded`.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection; empty on a bound
    /// pair.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Peers connected: `0` or `1`.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `payload` and waits for the peer's transport to acknowledge it.
    ///
    /// `report` and `mode` order a cursor report, as on `Pusher.send`.
    #[pyo3(signature = (payload, report=None, mode=0))]
    fn send(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        report: Option<Vec<u64>>,
        mode: u64,
    ) -> PyResult<Option<SyncCursors>> {
        let body = payload_of(payload)?;
        let meta = reporting_meta(py, Some(body.len() as u64), report, mode)?;
        let cursors = raise(py, py.detach(|| self.endpoint.send_reporting(meta, &body)))?;
        Ok(cursors.map(|cursors| SyncCursors {
            cursors: Mutex::new(cursors),
        }))
    }

    /// Waits for the next transfer from the peer, at most `max_bytes`, and
    /// returns `(payload, meta)`.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = raise(py, py.detach(|| self.endpoint.recv(max_bytes)))?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
        ))
    }

    /// `recv`, plus the reporter the peer ordered:
    /// `(payload, meta, reporter)`.
    fn recv_reporting<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(
        Bound<'py, pyo3::types::PyBytes>,
        PyIncomingMeta,
        Option<SyncReporter>,
    )> {
        let (message, reporter) = raise(py, py.detach(|| self.endpoint.recv_reporting(max_bytes)))?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
            reporter.map(|reporter| SyncReporter {
                reporter: Mutex::new(Some(reporter)),
            }),
        ))
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Paired {}>", self.endpoint.path())
    }
}

/// `weida.sync.Surveyor`.
#[pyclass(frozen, name = "Surveyor", module = "weida.sync")]
pub struct SyncSurveyor {
    endpoint: blocking::Surveyor,
}

#[pymethods]
impl SyncSurveyor {
    /// Dials `url` and adds one respondent to the set.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Respondents currently connected.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Asks every respondent and returns a `weida.Survey` of what arrived
    /// within `deadline` seconds, each answer at most `max_reply_bytes`.
    ///
    /// The same value class the asyncio surface returns, for the reason
    /// `weida.Survey` documents.
    fn survey(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        deadline: f64,
        max_reply_bytes: usize,
    ) -> PyResult<PySurvey> {
        let body = payload_of(payload)?;
        let deadline = raise(
            py,
            std::time::Duration::try_from_secs_f64(deadline)
                .map_err(|e| weida::Error::Runtime(format!("deadline: {e}"))),
        )?;
        let survey = raise(
            py,
            py.detach(|| self.endpoint.survey(&body, deadline, max_reply_bytes)),
        )?;
        Ok(PySurvey {
            replies: survey.replies,
            asked: survey.asked,
            failed: survey.failed,
            late: survey.late,
        })
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Surveyor>".to_owned()
    }
}

/// `weida.sync.Respondent`.
#[pyclass(frozen, name = "Respondent", module = "weida.sync")]
pub struct SyncRespondent {
    endpoint: blocking::Respondent,
}

#[pymethods]
impl SyncRespondent {
    /// The endpoint path this respondent serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next question and reads it, at most `max_bytes`.
    ///
    /// Hands back the same `weida.sync.Request` a replier does: a question is
    /// an exchange.
    fn accept(&self, py: Python<'_>, max_bytes: usize) -> PyResult<SyncRequest> {
        let request = raise(py, py.detach(|| self.endpoint.accept(max_bytes)))?;
        Ok(SyncRequest {
            request: Mutex::new(Some(request)),
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Respondent {}>", self.endpoint.path())
    }
}

/// `weida.sync.BusMember`.
#[pyclass(frozen, name = "BusMember", module = "weida.sync")]
pub struct SyncBusMember {
    endpoint: blocking::BusMember,
}

#[pymethods]
impl SyncBusMember {
    /// The path this member accepts on.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Joins the member at `url`.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Members this one has joined.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `payload` to every **other** member, returning how many it
    /// reached. Never waits for a receipt: a fan-out has no single peer to
    /// get one from.
    fn send(&self, py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(payload)?;
        raise(py, py.detach(|| self.endpoint.send(&body)))
    }

    /// Waits for the next message from another member, at most `max_bytes`,
    /// and returns `(payload, meta)`.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = raise(py, py.detach(|| self.endpoint.recv(max_bytes)))?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
        ))
    }

    /// Copies that never reached a member.
    fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.BusMember {}>", self.endpoint.path())
    }
}

/// `weida.sync.Radio`: segments to every joined dish. Nothing here blocks.
#[pyclass(frozen, name = "Radio", module = "weida.sync")]
pub struct SyncRadio {
    endpoint: blocking::Radio,
}

#[pymethods]
impl SyncRadio {
    /// Opens the next segment on `topic`, superseding the previous one's
    /// copies there: an unfinished copy at once, a finished one after the
    /// time its path needs. `max_age` in seconds is the sender's latency
    /// budget, `priority` orders topics within a layer, and
    /// `follows_upstream` marks a relay's segment.
    #[pyo3(signature = (topic, max_age=None, priority=0, follows_upstream=false))]
    fn segment(
        &self,
        py: Python<'_>,
        topic: &str,
        max_age: Option<f64>,
        priority: i16,
        follows_upstream: bool,
    ) -> PyResult<SyncSegment> {
        let max_age = seconds(py, max_age)?;
        let terms = crate::patterns::segment_terms(max_age, priority, follows_upstream);
        Ok(SyncSegment {
            segment: Mutex::new(Some(raise(py, self.endpoint.segment(topic, terms))?)),
        })
    }

    /// Sends `payload` as a one-packet segment on `topic`; returns how many
    /// dishes it was handed to.
    fn datagram(&self, py: Python<'_>, topic: &str, payload: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(payload)?;
        raise(py, self.endpoint.datagram(topic, &body))
    }

    /// Dishes currently joined.
    fn dish_count(&self) -> usize {
        self.endpoint.dish_count()
    }

    /// Copies dropped, over topics and causes.
    fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    /// What each joined dish connection lost, summed over topics.
    fn dish_drops(&self) -> Vec<PyDishDrops> {
        self.endpoint
            .dish_drops()
            .iter()
            .map(PyDishDrops::of)
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Radio {}>", self.endpoint.endpoint().path())
    }
}

/// `weida.sync.Segment`: one segment, written chunk by chunk.
#[pyclass(frozen, name = "Segment", module = "weida.sync")]
pub struct SyncSegment {
    /// `None` after `finish`.
    segment: Mutex<Option<blocking::Segment>>,
}

#[pymethods]
impl SyncSegment {
    /// Hands `chunk` to layer 0 of every copy still open; returns how many
    /// copies took it.
    fn write(&self, py: Python<'_>, chunk: &Bound<'_, PyAny>) -> PyResult<usize> {
        self.write_layer(py, 0, chunk)
    }

    /// Hands `chunk` to layer `layer` (`0..=15`) of every copy that wants
    /// it; returns how many copies took it.
    fn write_layer(&self, py: Python<'_>, layer: u8, chunk: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(chunk)?;
        let mut guard = self.segment.lock().expect("segment lock poisoned");
        match guard.as_mut() {
            Some(segment) => raise(py, segment.write_layer(layer, &body)),
            None => spent(py, "segment"),
        }
    }

    /// Ends layer `layer` on every copy that has it open.
    fn finish_layer(&self, py: Python<'_>, layer: u8) -> PyResult<()> {
        let mut guard = self.segment.lock().expect("segment lock poisoned");
        match guard.as_mut() {
            Some(segment) => {
                segment.finish_layer(layer);
                Ok(())
            }
            None => spent(py, "segment"),
        }
    }

    /// Ends every layer still open; returns how many copies still hold
    /// layer 0.
    fn finish(&self) -> usize {
        self.segment
            .lock()
            .expect("segment lock poisoned")
            .take()
            .map_or(0, blocking::Segment::finish)
    }
}

/// `weida.sync.Dish`: joins topics and receives the newest segment of each.
#[pyclass(frozen, name = "Dish", module = "weida.sync")]
pub struct SyncDish {
    endpoint: blocking::Dish,
}

#[pymethods]
impl SyncDish {
    /// Dials the radio at `url`.
    fn connect(&self, py: Python<'_>, url: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.connect(url)))
    }

    /// One `weida.ConnectionStats` per live connection to a radio.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Joins every topic `filter` matches; `max_age` in seconds is the
    /// latency budget after which the radio resets this dish's copy, and
    /// `max_layer` (`0..=15`) the highest layer the radio sends it.
    #[pyo3(signature = (filter, max_age=None, max_layer=None))]
    fn join(
        &self,
        py: Python<'_>,
        filter: &str,
        max_age: Option<f64>,
        max_layer: Option<u8>,
    ) -> PyResult<()> {
        let terms = crate::patterns::join_terms(seconds(py, max_age)?, max_layer);
        raise(py, py.detach(|| self.endpoint.join(filter, terms)))
    }

    /// Leaves a filter.
    fn leave(&self, py: Python<'_>, filter: &str) -> PyResult<()> {
        raise(py, py.detach(|| self.endpoint.leave(filter)))
    }

    /// Waits for the next segment: `("segment", payload, meta)` for a stream
    /// segment read whole, at most `max_bytes`, or
    /// `("datagram", topic, segment, payload)`.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let delivered = raise(py, py.detach(|| self.endpoint.recv(max_bytes)))?;
        match delivered {
            blocking::Delivered::Segment(message) => {
                let blocking::Message { payload, meta } = *message;
                crate::patterns::Heard::Segment(payload, Box::new(PyIncomingMeta::of(&meta)))
            }
            blocking::Delivered::Datagram {
                topic,
                segment,
                payload,
            } => crate::patterns::Heard::Datagram(topic, segment, payload),
        }
        .into_pyobject(py)
    }

    /// Radios connected.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Dish>".to_owned()
    }
}

/// `weida.sync.Cursors`: the sender's end of one transfer's report.
///
/// The same three calls the asyncio `weida.Cursors` has, blocking. The loop a
/// caller writes is `while (set := cursors.changed()) is not None:` — and
/// `changed` is where the thread parks with the GIL released, which is what
/// makes waiting for a verdict on one thread while another works the ordinary
/// shape rather than a trick.
#[pyclass(frozen, name = "Cursors", module = "weida.sync")]
pub struct SyncCursors {
    /// `&mut` on the facade's side; a `std` mutex is right here because no
    /// guard crosses an await on this surface.
    cursors: Mutex<blocking::Cursors>,
}

#[pymethods]
impl SyncCursors {
    /// The latest set as `{level: offset}`, without waiting.
    fn snapshot(&self) -> BTreeMap<u64, u64> {
        set_of(
            self.cursors
                .lock()
                .expect("cursors lock poisoned")
                .snapshot(),
        )
    }

    /// The latest offset for `level`, or `None` if it was never reported.
    fn offset(&self, py: Python<'_>, level: u64) -> PyResult<Option<u64>> {
        let level = level_of(py, level)?;
        Ok(self
            .cursors
            .lock()
            .expect("cursors lock poisoned")
            .offset(level))
    }

    /// Waits up to `deadline` seconds for the next change and returns the new
    /// set, or `None` once no further cursors are coming.
    ///
    /// The deadline is **mandatory**, unlike the asyncio `Cursors.changed`:
    /// there `asyncio.wait_for` bounds the wait and composes, while a parked
    /// thread is interrupted by nothing. A deadline that passes with nothing
    /// new raises `TimeoutError` — the builtin, as `socket.settimeout` does —
    /// because it is **not** the end of the report and returning `None` for
    /// it would make a caller stop reading a verdict that is still coming.
    fn changed(&self, py: Python<'_>, deadline: f64) -> PyResult<Option<BTreeMap<u64, u64>>> {
        let deadline = raise(
            py,
            std::time::Duration::try_from_secs_f64(deadline)
                .map_err(|e| weida::Error::Runtime(format!("deadline: {e}"))),
        )?;
        let reported = raise(
            py,
            py.detach(|| {
                self.cursors
                    .lock()
                    .expect("cursors lock poisoned")
                    .changed(deadline)
            }),
        )?;
        match reported {
            weida::Reported::Changed => Ok(Some(self.snapshot())),
            weida::Reported::Ended => Ok(None),
            weida::Reported::Waiting => Err(pyo3::exceptions::PyTimeoutError::new_err(
                "no cursor arrived within the deadline; the report may still continue",
            )),
        }
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Cursors>".to_owned()
    }
}

/// `weida.sync.Reporter`: the receiver's end, blocking.
#[pyclass(frozen, name = "Reporter", module = "weida.sync")]
pub struct SyncReporter {
    /// `None` after `finish`, which consumes the reporter: a report ends
    /// once.
    reporter: Mutex<Option<blocking::Reporter>>,
}

#[pymethods]
impl SyncReporter {
    /// The levels the sender ordered, ascending.
    #[getter]
    fn levels(&self, py: Python<'_>) -> PyResult<Vec<u64>> {
        let guard = self.reporter.lock().expect("reporter lock poisoned");
        match guard.as_ref() {
            Some(reporter) => Ok(reporter.levels().iter().map(|l| l.to_wire()).collect()),
            None => spent(py, "report"),
        }
    }

    /// The mode the sender asked for.
    #[getter]
    fn mode(&self, py: Python<'_>) -> PyResult<u64> {
        let guard = self.reporter.lock().expect("reporter lock poisoned");
        match guard.as_ref() {
            Some(reporter) => Ok(reporter.mode().to_wire()),
            None => spent(py, "report"),
        }
    }

    /// Reports that `level` has reached `offset`.
    ///
    /// A level the sender did not order is ignored rather than refused.
    fn report(&self, py: Python<'_>, level: u64, offset: u64) -> PyResult<()> {
        let level = level_of(py, level)?;
        let mut guard = self.reporter.lock().expect("reporter lock poisoned");
        match guard.as_mut() {
            Some(reporter) => raise(py, py.detach(|| reporter.report(level, offset))),
            None => spent(py, "report"),
        }
    }

    /// Flushes the latest offset per level and ends the cursor stream.
    fn finish(&self, py: Python<'_>) -> PyResult<()> {
        let taken = self.reporter.lock().expect("reporter lock poisoned").take();
        match taken {
            Some(reporter) => raise(py, py.detach(|| reporter.finish())),
            None => spent(py, "report"),
        }
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Reporter>".to_owned()
    }
}

/// Builds the `weida.sync` submodule.
///
/// A real submodule rather than a naming convention, so that
/// `from weida import sync` works and `weida.sync.Runtime` is the class a
/// traceback names.
pub fn install(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let sync = PyModule::new(py, "sync")?;
    sync.add_class::<SyncRuntime>()?;
    sync.add_class::<SyncBinding>()?;
    sync.add_class::<SyncRequester>()?;
    sync.add_class::<SyncPusher>()?;
    sync.add_class::<SyncSubscriber>()?;
    sync.add_class::<SyncReplier>()?;
    sync.add_class::<SyncRequest>()?;
    sync.add_class::<SyncPuller>()?;
    sync.add_class::<SyncPublisher>()?;
    sync.add_class::<SyncPaired>()?;
    sync.add_class::<SyncSurveyor>()?;
    sync.add_class::<SyncRespondent>()?;
    sync.add_class::<SyncBusMember>()?;
    sync.add_class::<SyncRadio>()?;
    sync.add_class::<SyncSegment>()?;
    sync.add_class::<SyncDish>()?;
    sync.add_class::<SyncCursors>()?;
    sync.add_class::<SyncReporter>()?;
    sync.add(
        "__all__",
        vec![
            "Runtime",
            "Binding",
            "Requester",
            "Pusher",
            "Subscriber",
            "Replier",
            "Request",
            "Puller",
            "Publisher",
            "Paired",
            "Surveyor",
            "Respondent",
            "BusMember",
            "Radio",
            "Segment",
            "Dish",
            "Cursors",
            "Reporter",
        ],
    )?;
    parent.add_submodule(&sync)?;
    // So that `import weida.sync` works and not only
    // `from weida import sync`: a submodule added to its parent is not in
    // `sys.modules` until something puts it there.
    py.import("sys")?
        .getattr("modules")?
        .set_item("weida.sync", &sync)?;
    Ok(())
}
