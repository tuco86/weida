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
//! # It is the facade, and the waits that cannot be interrupted are not
//!
//! This module is `weida::blocking` — not a link, because a feature-gated
//! item is a hard rustdoc error in the configuration that lacks it —
//! with argument conversion around it. The facade is where the owned
//! reactor lives and where the one refusal that matters is made: a call from
//! inside a Tokio runtime would park the worker that has to drive what it is
//! waiting for, so every entry point there checks for an ambient runtime and
//! fails with `weida.Runtime` instead of deadlocking. A Python process has no
//! ambient runtime, so the check is silent here — and it stays correct in a
//! process that embeds one.
//!
//! The facade drives its futures with `futures::executor::block_on`, and a
//! thread parked there never returns to the interpreter: Ctrl-C,
//! `signal.alarm` and every other signal handler would wait for the peer to
//! speak, and a silent peer would leave the process unkillable. So the calls
//! that wait on a peer — `accept`, `recv`, `recv_reporting` and `request`,
//! and the `reply` and report calls that wait for the transport — are driven
//! here instead, on the asynchronous endpoint the facade hands out for what it
//! does not wrap, by [`weida_py_core::interruptible`]. What is written twice
//! is the few `await`s of each call (the facade's own, including its receive
//! deadline), never a protocol: HELLO, negotiation, dispatch and every limit
//! are the library's.
//!
//! Everything else is the facade's call, with the GIL released.
//!
//! # The GIL is released while a call waits
//!
//! Every call below runs with the GIL released ([`Python::detach`]), so other
//! Python threads run while one is parked in `accept` or `recv`. A blocking
//! binding that held the GIL would make a second thread pointless, and the
//! usual shape of a synchronous messaging program — one thread per endpoint —
//! would be a program that serves one endpoint at a time.
//!
//! No lock is ever waited for with the GIL held while another thread holds it
//! across a wait: a lock that is held for the length of a call is taken inside
//! the released section, and what a getter reads is kept where no waiting call
//! holds it.
//!
//! # What ends a wait that has no deadline
//!
//! Ctrl-C, on the main thread: it asks Python every 100 ms whether a signal
//! handler has something to raise, and `KeyboardInterrupt` ends the call. The
//! transfer being read when it arrives is dropped, which resets it — what
//! cancelling an `asyncio.Task` does to the same future. A thread that is not
//! the main one cannot receive a signal and waits for its peer.
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
//! wait and composes; here the wait is bounded by an argument — a peer that
//! never reports opens no stream, a report that never starts never ends, and a
//! connection both sides keep alive never closes. A deadline that passes raises
//! `TimeoutError`, the builtin, because it is **not** the end of the report:
//! returning `None` for it would make a caller stop reading a verdict that is
//! still coming.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError, RwLock};

use pyo3::prelude::*;
use pyo3::types::PyModule;
use weida::blocking;
use weida_py_core::{interruptible, payload_of, py_bytes};

use crate::cursors::{level_of, reporting_meta, set_of};
use crate::errors::raise;
use crate::lock::locked;
use crate::values::{
    PyConnectionStats, PyDishDrops, PyIdentity, PyIncomingMeta, PySurvey, PyTrust,
};

/// What a call on a spent runtime, request or report is told.
fn spent_error(what: &str) -> weida::Error {
    weida::Error::Runtime(format!("this {what} was already shut down"))
}

/// What a call on a spent runtime gets.
fn spent<T>(py: Python<'_>, what: &str) -> PyResult<T> {
    raise(py, Err(spent_error(what)))
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

/// What a call that waits on a peer takes from the runtime its endpoint
/// belongs to: the executor it waits with, and the receive deadline.
///
/// Cloned into every endpoint when the endpoint is made, from the facade's own
/// runtime, so a call needs nothing from the interpreter's side to know whose
/// clock it is on.
#[derive(Clone)]
struct Waiter {
    runtime: weida::Runtime,
}

impl Waiter {
    fn of(runtime: &blocking::Runtime) -> Waiter {
        Waiter {
            runtime: runtime.runtime().clone(),
        }
    }

    /// Drives the future `work` builds with the GIL released, until it
    /// finishes or a Python signal handler raises, and maps the library's
    /// failure onto its class.
    ///
    /// A signal drops the future where it stands, which is what cancelling an
    /// `asyncio.Task` does to the same future on the asyncio surface: a
    /// receive that had not produced its transfer leaves it queued.
    fn wait<F, T>(&self, py: Python<'_>, work: impl FnOnce() -> F + Send) -> PyResult<T>
    where
        F: Future<Output = Result<T, weida::Error>>,
        T: Send,
    {
        // What the facade refuses at every entry point: parking a reactor
        // worker on a call that needs a reactor worker.
        if tokio::runtime::Handle::try_current().is_ok() {
            return raise(
                py,
                Err(weida::Error::Runtime(
                    "weida.sync blocks the calling thread and was called from inside a Tokio \
                     runtime, which would deadlock: use the asynchronous API there, or call this \
                     from a thread the runtime does not own"
                        .to_owned(),
                )),
            );
        }
        raise(py, interruptible(py, self.runtime.exec(), work)?)
    }

    /// Awaits one body under `RuntimeConfig::receive_timeout`. Past it the
    /// body is dropped, which stops a transfer's stream, and the result is
    /// `Expired`.
    async fn bounded<T>(
        &self,
        body: impl Future<Output = Result<T, weida::Error>>,
    ) -> Result<T, weida::Error> {
        match self.runtime.config().receive_timeout {
            None => body.await,
            Some(limit) => self
                .runtime
                .exec()
                .within(limit, body)
                .await
                .unwrap_or(Err(weida::Error::Expired)),
        }
    }

    /// A transfer read whole, at most `max_bytes`, with the metadata it
    /// arrived with.
    async fn collected(
        &self,
        transfer: weida::IncomingTransfer,
        max_bytes: usize,
    ) -> Result<blocking::Message, weida::Error> {
        let meta = transfer.meta().clone();
        let payload = self.bounded(transfer.collect(max_bytes)).await?;
        Ok(blocking::Message { payload, meta })
    }

    /// [`collected`](Waiter::collected), and the reporter the sender ordered,
    /// if it ordered one.
    async fn collected_reporting(
        &self,
        transfer: weida::IncomingTransfer,
        max_bytes: usize,
    ) -> Result<(blocking::Message, Option<weida::Reporter>), weida::Error> {
        let meta = transfer.meta().clone();
        // Taken before the payload: `collect` consumes the transfer.
        let reporter = transfer.reporter();
        let payload = self.bounded(transfer.collect(max_bytes)).await?;
        Ok((blocking::Message { payload, meta }, reporter))
    }

    /// A request whose body is read whole, at most `max_bytes`.
    async fn accepted(
        &self,
        mut request: weida::IncomingRequest,
        max_bytes: usize,
    ) -> Result<Accepted, weida::Error> {
        let meta = request.meta().clone();
        let payload = self.bounded(request.take_body().collect(max_bytes)).await?;
        Ok(Accepted {
            request,
            message: blocking::Message { payload, meta },
        })
    }
}

/// `weida.sync.Runtime`: a reactor of the library's own, driven by its caller.
#[pyclass(frozen, name = "Runtime", module = "weida.sync")]
pub struct SyncRuntime {
    /// `None` after `shutdown` or `drain`, both of which consume the facade's
    /// runtime.
    ///
    /// A read lock for every call that uses the runtime and the write lock
    /// for the two that consume it, and **every lock is taken inside a
    /// released section** ([`SyncRuntime::with`]): a `bind` holds its read
    /// lock for as long as the socket takes to bind, and a thread that waited
    /// for it with the GIL held would keep the binding thread from ever
    /// getting the GIL back to let go.
    runtime: RwLock<Option<blocking::Runtime>>,
}

impl SyncRuntime {
    /// Runs `body` on the live runtime, with the GIL released from before the
    /// lock is taken until after it is let go, or says the runtime is spent.
    fn with<T: Send>(
        &self,
        py: Python<'_>,
        body: impl FnOnce(&blocking::Runtime) -> T + Send,
    ) -> PyResult<T> {
        let outcome = py.detach(|| {
            let guard = self.runtime.read().unwrap_or_else(PoisonError::into_inner);
            guard.as_ref().map(body)
        });
        match outcome {
            Some(done) => Ok(done),
            None => spent(py, "runtime"),
        }
    }

    /// Takes the runtime out for good, waiting for the calls that are using it
    /// to finish first, or says it is spent.
    fn take(&self, py: Python<'_>) -> PyResult<blocking::Runtime> {
        let taken = py.detach(|| {
            self.runtime
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        });
        match taken {
            Some(runtime) => Ok(runtime),
            None => spent(py, "runtime"),
        }
    }
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
            runtime: RwLock::new(Some(runtime)),
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
        let (binding, waiter) = self.with(py, move |runtime| {
            (
                runtime.bind_quic(socket, identity.identity.clone()),
                Waiter::of(runtime),
            )
        })?;
        let binding = raise(py, binding)?;
        let local = binding
            .local_addr()
            .expect("a QUIC binding has an address")
            .to_string();
        Ok(SyncBinding {
            binding,
            url_base: format!("weida://{fingerprint}@{local}"),
            local,
            fingerprint: Some(fingerprint),
            waiter,
        })
    }

    /// Binds an in-process bus, which a client in this process dials as
    /// `weida+inproc://<bus>/<path>`.
    fn bind_inproc(&self, py: Python<'_>, bus: &str) -> PyResult<SyncBinding> {
        let name = bus.to_owned();
        let (binding, waiter) = self.with(py, move |runtime| {
            (runtime.bind_inproc(&name), Waiter::of(runtime))
        })?;
        let binding = raise(py, binding)?;
        Ok(SyncBinding {
            binding,
            local: bus.to_owned(),
            fingerprint: None,
            url_base: format!("weida+inproc://{bus}"),
            waiter,
        })
    }

    /// A requester on this runtime.
    fn requester(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncRequester> {
        let trust = trust.trust;
        self.with(py, move |runtime| SyncRequester {
            endpoint: runtime.requester(trust),
            waiter: Waiter::of(runtime),
        })
    }

    /// A pusher on this runtime.
    fn pusher(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncPusher> {
        let trust = trust.trust;
        self.with(py, move |runtime| SyncPusher {
            endpoint: runtime.pusher(trust),
        })
    }

    /// A subscriber on this runtime.
    fn subscriber(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncSubscriber> {
        let trust = trust.trust;
        self.with(py, move |runtime| SyncSubscriber {
            endpoint: runtime.subscriber(trust),
            waiter: Waiter::of(runtime),
        })
    }

    /// A dialling pair on this runtime.
    fn pair(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncPaired> {
        let trust = trust.trust;
        self.with(py, move |runtime| SyncPaired {
            endpoint: runtime.pair(trust),
            waiter: Waiter::of(runtime),
        })
    }

    /// A surveyor on this runtime.
    fn surveyor(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncSurveyor> {
        let trust = trust.trust;
        self.with(py, move |runtime| SyncSurveyor {
            endpoint: runtime.surveyor(trust),
        })
    }

    /// A dish on this runtime.
    fn dish(&self, py: Python<'_>, trust: PyTrust) -> PyResult<SyncDish> {
        let trust = trust.trust;
        self.with(py, move |runtime| SyncDish {
            endpoint: runtime.dish(trust),
            waiter: Waiter::of(runtime),
        })
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
        let runtime = self.take(py)?;
        let drained = raise(py, py.detach(move || runtime.drain(seconds)))?;
        Ok((drained.delivered, drained.outstanding))
    }

    /// Closes every connection without waiting for anything in flight.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = self.take(py)?;
        raise(py, py.detach(move || runtime.shutdown()))
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
    /// Handed to the endpoints registered here, which wait on their peers.
    waiter: Waiter,
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
            waiter: self.waiter.clone(),
        })
    }

    /// Registers a puller at `path`.
    fn puller(&self, py: Python<'_>, path: &str) -> PyResult<SyncPuller> {
        Ok(SyncPuller {
            endpoint: raise(py, self.binding.puller(path))?,
            waiter: self.waiter.clone(),
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
            waiter: self.waiter.clone(),
        })
    }

    /// Registers a respondent at `path`.
    fn respondent(&self, py: Python<'_>, path: &str) -> PyResult<SyncRespondent> {
        Ok(SyncRespondent {
            endpoint: raise(py, self.binding.respondent(path))?,
            waiter: self.waiter.clone(),
        })
    }

    /// Registers a bus member at `path`, dialling others on `trust`'s terms.
    fn bus(&self, py: Python<'_>, path: &str, trust: PyTrust) -> PyResult<SyncBusMember> {
        Ok(SyncBusMember {
            endpoint: raise(py, self.binding.bus(path, trust.trust.clone()))?,
            waiter: self.waiter.clone(),
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
    waiter: Waiter,
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
    ///
    /// Waits for the reply as long as the peer takes, and Ctrl-C ends the
    /// wait; see the module documentation.
    fn request<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        max_reply_bytes: usize,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let body = payload_of(payload)?;
        let reply = self.waiter.wait(py, || async {
            let reply = self
                .endpoint
                .endpoint()
                .request_with(weida::TransferMeta::default(), &body)
                .await?;
            self.waiter.bounded(reply.collect(max_reply_bytes)).await
        })?;
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
        Ok(cursors.map(SyncCursors::new))
    }

    fn __repr__(&self) -> String {
        "<weida.sync.Pusher>".to_owned()
    }
}

/// `weida.sync.Subscriber`.
#[pyclass(frozen, name = "Subscriber", module = "weida.sync")]
pub struct SyncSubscriber {
    endpoint: blocking::Subscriber,
    waiter: Waiter,
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
    ///
    /// Waits as long as the peer takes, and Ctrl-C ends the wait; see the
    /// module documentation.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = self.waiter.wait(py, || async {
            let transfer = self.endpoint.endpoint().recv().await?;
            self.waiter.collected(transfer, max_bytes).await
        })?;
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
    waiter: Waiter,
}

#[pymethods]
impl SyncReplier {
    /// The endpoint path this replier serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next request and reads its body, at most `max_bytes`.
    ///
    /// Waits as long as the peer takes, and Ctrl-C ends the wait; see the
    /// module documentation.
    fn accept(&self, py: Python<'_>, max_bytes: usize) -> PyResult<SyncRequest> {
        let accepted = self.waiter.wait(py, || async {
            let request = self.endpoint.endpoint().accept().await?;
            self.waiter.accepted(request, max_bytes).await
        })?;
        Ok(SyncRequest {
            request: Mutex::new(Some(accepted)),
            waiter: self.waiter.clone(),
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.sync.Replier {}>", self.endpoint.path())
    }
}

/// An accepted request whose body is already read, and the reply it owes:
/// what `weida::blocking::Request` is, built here because the call that waits
/// for one is driven here.
///
/// Dropping it without a reply causes ERROR `{NO_REPLY}` on the reply half,
/// exactly as the facade's does and for the same reason: a requester never
/// waits out an idle timeout for an answer nobody will send.
struct Accepted {
    request: weida::IncomingRequest,
    message: blocking::Message,
}

/// `weida.sync.Request`: an accepted exchange and the reply it owes.
#[pyclass(frozen, name = "Request", module = "weida.sync")]
pub struct SyncRequest {
    /// `None` once answered. Taken, never held, for the length of a call: a
    /// getter on another thread must not wait for a reply that is waiting for
    /// its transport.
    request: Mutex<Option<Accepted>>,
    waiter: Waiter,
}

#[pymethods]
impl SyncRequest {
    /// The request payload.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let guard = locked(&self.request);
        match guard.as_ref() {
            Some(accepted) => Ok(py_bytes(py, &accepted.message.payload)),
            None => spent(py, "request"),
        }
    }

    /// What the DATA header said, including the proved peer.
    #[getter]
    fn meta(&self, py: Python<'_>) -> PyResult<PyIncomingMeta> {
        let guard = locked(&self.request);
        match guard.as_ref() {
            Some(accepted) => Ok(PyIncomingMeta::of(&accepted.message.meta)),
            None => spent(py, "request"),
        }
    }

    /// Answers with `payload`.
    fn reply(&self, py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<()> {
        let body = payload_of(payload)?;
        let taken = locked(&self.request).take();
        match taken {
            Some(accepted) => self.waiter.wait(py, move || async move {
                let mut out = accepted
                    .request
                    .reply(weida::TransferMeta::default().with_content_len(body.len() as u64))
                    .await?;
                out.write_all(&body).await?;
                out.finish()?;
                Ok(())
            }),
            None => spent(py, "request"),
        }
    }

    /// Declines the request, so the requester gets `weida.Rejected`.
    fn refuse(&self, py: Python<'_>) -> PyResult<()> {
        let taken = locked(&self.request).take();
        match taken {
            Some(accepted) => self.waiter.wait(py, move || async move {
                accepted.request.refuse(weida::ErrorCode::Rejected).await;
                Ok(())
            }),
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
    waiter: Waiter,
}

#[pymethods]
impl SyncPuller {
    /// The endpoint path this puller serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next transfer and reads it, at most `max_bytes`,
    /// returning `(payload, meta)`.
    ///
    /// Waits as long as the peer takes, and Ctrl-C ends the wait; see the
    /// module documentation.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = self.waiter.wait(py, || async {
            let transfer = self.endpoint.endpoint().recv().await?;
            self.waiter.collected(transfer, max_bytes).await
        })?;
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
        let (message, reporter) = self.waiter.wait(py, || async {
            let transfer = self.endpoint.endpoint().recv().await?;
            self.waiter.collected_reporting(transfer, max_bytes).await
        })?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
            reporter.map(|reporter| SyncReporter::new(reporter, self.waiter.clone())),
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
    waiter: Waiter,
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
        Ok(cursors.map(SyncCursors::new))
    }

    /// Waits for the next transfer from the peer, at most `max_bytes`, and
    /// returns `(payload, meta)`.
    ///
    /// Waits as long as the peer takes, and Ctrl-C ends the wait; see the
    /// module documentation.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = self.waiter.wait(py, || async {
            let transfer = self.endpoint.endpoint().recv().await?;
            self.waiter.collected(transfer, max_bytes).await
        })?;
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
        let (message, reporter) = self.waiter.wait(py, || async {
            let transfer = self.endpoint.endpoint().recv().await?;
            self.waiter.collected_reporting(transfer, max_bytes).await
        })?;
        Ok((
            py_bytes(py, &message.payload),
            PyIncomingMeta::of(&message.meta),
            reporter.map(|reporter| SyncReporter::new(reporter, self.waiter.clone())),
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
    waiter: Waiter,
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
    /// an exchange. Waits as long as the peer takes, and Ctrl-C ends the wait;
    /// see the module documentation.
    fn accept(&self, py: Python<'_>, max_bytes: usize) -> PyResult<SyncRequest> {
        let accepted = self.waiter.wait(py, || async {
            let request = self.endpoint.endpoint().accept().await?;
            self.waiter.accepted(request, max_bytes).await
        })?;
        Ok(SyncRequest {
            request: Mutex::new(Some(accepted)),
            waiter: self.waiter.clone(),
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
    waiter: Waiter,
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
    ///
    /// Waits as long as the peer takes, and Ctrl-C ends the wait; see the
    /// module documentation.
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<(Bound<'py, pyo3::types::PyBytes>, PyIncomingMeta)> {
        let message = self.waiter.wait(py, || async {
            let transfer = self.endpoint.endpoint().recv().await?;
            self.waiter.collected(transfer, max_bytes).await
        })?;
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
        let mut guard = locked(&self.segment);
        match guard.as_mut() {
            Some(segment) => raise(py, segment.write_layer(layer, &body)),
            None => spent(py, "segment"),
        }
    }

    /// Ends layer `layer` on every copy that has it open.
    fn finish_layer(&self, py: Python<'_>, layer: u8) -> PyResult<()> {
        let mut guard = locked(&self.segment);
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
        locked(&self.segment)
            .take()
            .map_or(0, blocking::Segment::finish)
    }
}

/// `weida.sync.Dish`: joins topics and receives the newest segment of each.
#[pyclass(frozen, name = "Dish", module = "weida.sync")]
pub struct SyncDish {
    endpoint: blocking::Dish,
    waiter: Waiter,
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
    ///
    /// Waits as long as the radio takes, and Ctrl-C ends the wait; see the
    /// module documentation.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let heard = self.waiter.wait(py, || async {
            match self.endpoint.endpoint().recv().await? {
                weida::Received::Segment(transfer) => {
                    let message = self.waiter.collected(transfer, max_bytes).await?;
                    Ok(crate::patterns::Heard::Segment(
                        message.payload,
                        Box::new(PyIncomingMeta::of(&message.meta)),
                    ))
                }
                weida::Received::Datagram {
                    topic,
                    segment,
                    payload,
                } => Ok(crate::patterns::Heard::Datagram(
                    topic.to_string(),
                    segment,
                    payload,
                )),
            }
        })?;
        heard.into_pyobject(py)
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
    /// `&mut` on the facade's side, and held for the whole of a `changed`, so
    /// it is only ever taken inside a released section. Nothing that reads
    /// waits for it.
    cursors: Mutex<blocking::Cursors>,
    /// The latest set, replaced whenever `changed` sees one. What
    /// `snapshot` and `offset` read, so that neither waits behind a `changed`
    /// that is parked for its whole deadline with the GIL held by the reader.
    latest: Mutex<weida::CursorSet>,
}

impl SyncCursors {
    fn new(cursors: blocking::Cursors) -> SyncCursors {
        SyncCursors {
            latest: Mutex::new(cursors.snapshot()),
            cursors: Mutex::new(cursors),
        }
    }
}

#[pymethods]
impl SyncCursors {
    /// The latest set as `{level: offset}`, without waiting.
    fn snapshot(&self) -> BTreeMap<u64, u64> {
        set_of(*locked(&self.latest))
    }

    /// The latest offset for `level`, or `None` if it was never reported.
    fn offset(&self, py: Python<'_>, level: u64) -> PyResult<Option<u64>> {
        let level = level_of(py, level)?;
        Ok(locked(&self.latest).offset(level))
    }

    /// Waits up to `deadline` seconds for the next change and returns the new
    /// set, or `None` once no further cursors are coming.
    ///
    /// The deadline is **mandatory**, unlike the asyncio `Cursors.changed`:
    /// there `asyncio.wait_for` bounds the wait and composes, and here the
    /// argument does. A deadline that passes with nothing
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
                let mut cursors = locked(&self.cursors);
                let reported = cursors.changed(deadline);
                *locked(&self.latest) = cursors.snapshot();
                reported
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
    /// once. Held for the length of a `report`, so taken only inside the
    /// released section: a call on another thread queues behind it there, and
    /// the getters below do not need it at all.
    reporter: Mutex<Option<weida::Reporter>>,
    /// What the sender ordered, fixed when it did, so that reading it never
    /// waits behind a `report`.
    levels: Vec<u64>,
    mode: u64,
    /// Set by `finish`, from when the getters refuse as they did when the
    /// reporter was gone.
    finished: AtomicBool,
    waiter: Waiter,
}

impl SyncReporter {
    fn new(reporter: weida::Reporter, waiter: Waiter) -> SyncReporter {
        SyncReporter {
            levels: reporter.levels().iter().map(|l| l.to_wire()).collect(),
            mode: reporter.mode().to_wire(),
            reporter: Mutex::new(Some(reporter)),
            finished: AtomicBool::new(false),
            waiter,
        }
    }
}

#[pymethods]
impl SyncReporter {
    /// The levels the sender ordered, ascending.
    #[getter]
    fn levels(&self, py: Python<'_>) -> PyResult<Vec<u64>> {
        if self.finished.load(Ordering::SeqCst) {
            return spent(py, "report");
        }
        Ok(self.levels.clone())
    }

    /// The mode the sender asked for.
    #[getter]
    fn mode(&self, py: Python<'_>) -> PyResult<u64> {
        if self.finished.load(Ordering::SeqCst) {
            return spent(py, "report");
        }
        Ok(self.mode)
    }

    /// Reports that `level` has reached `offset`.
    ///
    /// A level the sender did not order is ignored rather than refused.
    // The guard is held across the call on purpose: a report borrows the
    // reporter for as long as it runs, the future is polled on this thread
    // only, and a second report must queue behind it.
    #[allow(clippy::await_holding_lock)]
    fn report(&self, py: Python<'_>, level: u64, offset: u64) -> PyResult<()> {
        let level = level_of(py, level)?;
        self.waiter.wait(py, || async {
            let mut guard = locked(&self.reporter);
            match guard.as_mut() {
                Some(reporter) => reporter.report(level, offset).await,
                None => Err(spent_error("report")),
            }
        })
    }

    /// Flushes the latest offset per level and ends the cursor stream.
    fn finish(&self, py: Python<'_>) -> PyResult<()> {
        self.finished.store(true, Ordering::SeqCst);
        self.waiter.wait(py, || async {
            let taken = locked(&self.reporter).take();
            match taken {
                Some(reporter) => reporter.finish().await,
                None => Err(spent_error("report")),
            }
        })
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
