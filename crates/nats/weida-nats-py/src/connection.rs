//! The connection: the handshake, the reactor it runs on, and every
//! operation a Core NATS client has.
//!
//! # Where the reactor comes from
//!
//! [0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.4 gives
//! a library three ways to reach a reactor, and `weida_zmq.Context` offers
//! all three because all three mean something to a Python process. A NATS
//! connection is not a context — it is one socket with a driver behind it —
//! so the same three are offered where they mean something and are absent
//! where they do not:
//!
//! | Python | Rust | What it is for |
//! | --- | --- | --- |
//! | `await weida_nats.connect(host, port)` | `Exec::owned` | The default, and the only one a plain Python program can use: the connection owns a reactor of `worker_threads` threads, and no asyncio loop drives it. |
//! | `await Connection.connect_current(host, port)` | `Exec::current` | The *ambient* Tokio reactor. A plain Python process has none, so this raises `weida_nats.Runtime` — the honest answer rather than a hidden second reactor. It is reachable when this module is imported into a process a Rust host already runs a reactor in. |
//! | `await Connection.connect_sharing(other, host, port)` | `other`'s `Exec`, cloned | A second connection on the first one's reactor, holding the same `OwnedReactor` alive. Two connections to two clusters over one thread pool, which is what a program with several connections wants and what ten `connect` calls would not give it. |
//!
//! There is no fourth, and in particular there is no `Connection()`
//! constructor: connecting reads the server's `INFO` and answers it, so it is
//! a coroutine, and a `__new__` that dialled would be a constructor that
//! blocks an event loop.
//!
//! # What keeps the reactor alive
//!
//! [`Exec::owned`] hands back an `OwnedReactor` beside the handle, and the
//! runtime dies with it. So every object that can still cause work on that
//! runtime holds one — the connection, and each subscription taken from it —
//! rather than the connection alone: a Python program that keeps a
//! subscription and drops the connection object is a program whose messages
//! must keep arriving.
//!
//! # No protocol behaviour lives here
//!
//! Subject validation, `INFO.max_payload`, the inbox, the mandatory request
//! window, the 503-versus-timeout distinction, queue groups and every bound
//! are `weida-nats`'s. This file converts arguments and awaits.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use weida_nats::{Connection, ConnectionOptions, Error};
use weida_py_core::{Bridge, payload_of};
use weida_runtime::{Exec, OwnedReactor};

use crate::errors::{errno_of, raise, to_py};
use crate::options;
use crate::subscription::PySubscription;
use crate::values::{PyMessage, PyRemoteInfo, PyState, headers_of, subject_of};

/// The reactor a connection runs on, and what keeps it alive.
///
/// `_owned` is never read: it is held so that the last connection or
/// subscription to go out of scope is what shuts the runtime down. It is
/// `None` for the ambient runtime, which this module did not create and
/// therefore does not shut down, and an `Arc` because a subscription
/// outlives the Python object it came from as easily as not.
#[derive(Clone)]
pub struct Reactor {
    exec: Exec,
    _owned: Option<Arc<OwnedReactor>>,
}

impl Reactor {
    /// A reactor of this library's own, with `worker_threads` threads.
    pub fn owned(py: Python<'_>, worker_threads: usize) -> PyResult<Reactor> {
        let (exec, owned) = raise(
            py,
            Exec::owned(worker_threads, "weida-nats").map_err(Error::from),
        )?;
        Ok(Reactor {
            exec,
            _owned: Some(Arc::new(owned)),
        })
    }

    /// The ambient Tokio reactor, where the calling thread is inside one.
    fn current(py: Python<'_>) -> PyResult<Reactor> {
        let exec = raise(py, Exec::current().map_err(Error::from))?;
        Ok(Reactor { exec, _owned: None })
    }

    /// The reactor every future of this connection runs on.
    pub fn exec(&self) -> &Exec {
        &self.exec
    }
}

/// `weida_nats.Connection`: one connection to one server.
#[pyclass(frozen, name = "Connection", module = "weida_nats")]
pub struct PyConnection {
    inner: Connection,
    bridge: Bridge,
    reactor: Reactor,
}

/// Runs the handshake on `reactor` and wraps what comes back.
///
/// The one place a `Connection` is built, so the three constructors differ in
/// exactly the line that chose the reactor and in nothing else.
fn dial<'py>(
    py: Python<'py>,
    reactor: Reactor,
    host: String,
    port: u16,
    options: ConnectionOptions,
) -> PyResult<Bound<'py, PyAny>> {
    let bridge = Bridge::new(reactor.exec().clone(), to_py);
    let spawned = bridge.clone();
    bridge.awaitable(py, async move {
        let inner = Connection::connect(reactor.exec(), &host, port, options)
            .await
            .map_err(errno_of)?;
        Ok(PyConnection {
            inner,
            bridge: spawned,
            reactor,
        })
    })
}

/// `await weida_nats.connect("127.0.0.1", 4222)`: a connection that owns its
/// reactor.
///
/// Every keyword of `**options` is a `CONNECT` field or a bound; see
/// [`crate::options`]. `worker_threads` is not one of them — it sizes the
/// reactor rather than the connection, and one thread is enough for a driver
/// whose work is a socket and a timer.
#[pyfunction]
#[pyo3(signature = (host, port, *, worker_threads=1, **options))]
pub fn connect<'py>(
    py: Python<'py>,
    host: String,
    port: u16,
    worker_threads: usize,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let configured = options::from_kwargs(py, options)?;
    dial(
        py,
        Reactor::owned(py, worker_threads)?,
        host,
        port,
        configured,
    )
}

#[pymethods]
impl PyConnection {
    /// A connection on the **ambient** Tokio reactor.
    ///
    /// Raises `weida_nats.Runtime` where there is none, which is what a plain
    /// Python process has. There is no `worker_threads`: the reactor was
    /// sized by whoever created it.
    #[classmethod]
    #[pyo3(signature = (host, port, **options))]
    fn connect_current<'py>(
        _class: &Bound<'py, PyType>,
        py: Python<'py>,
        host: String,
        port: u16,
        options: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let configured = options::from_kwargs(py, options)?;
        dial(py, Reactor::current(py)?, host, port, configured)
    }

    /// A second connection on `other`'s reactor.
    ///
    /// Two connections over one thread pool, which is what a program that
    /// talks to two clusters wants: the alternative is a thread pool per
    /// connection, and nothing about a second connection needs one.
    #[classmethod]
    #[pyo3(signature = (other, host, port, **options))]
    fn connect_sharing<'py>(
        _class: &Bound<'py, PyType>,
        py: Python<'py>,
        other: &PyConnection,
        host: String,
        port: u16,
        options: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let configured = options::from_kwargs(py, options)?;
        dial(py, other.reactor.clone(), host, port, configured)
    }

    /// The server's `INFO`, as it now stands.
    ///
    /// A later `INFO` merges into this rather than replacing it, so a
    /// topology notice that mentions no `max_payload` does not change one.
    fn info(&self) -> PyRemoteInfo {
        PyRemoteInfo::of(self.inner.info())
    }

    /// `INFO.max_payload`: the largest publication this server accepts.
    ///
    /// The check against it is the library's, before anything is written.
    /// This is here to be read, not to be re-applied.
    fn max_payload(&self) -> u64 {
        self.inner.max_payload()
    }

    /// Whether headers were negotiated, which is what `HPUB` needs.
    fn headers_supported(&self) -> bool {
        self.inner.headers_supported()
    }

    /// Whether the server has said it is draining (`INFO.ldm`).
    fn is_lame_duck(&self) -> bool {
        self.inner.is_lame_duck()
    }

    /// Waits for the server to enter Lame Duck Mode.
    ///
    /// `True` when the notice arrived, `False` when the connection ended
    /// first. A program that wants to redial before being dropped awaits
    /// this; where to redial *to* is `info().connect_urls`, because
    /// reconnection is a client-library policy and this library has none.
    fn lame_duck_notice<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        self.bridge
            .awaitable(py, async move { Ok(connection.lame_duck_notice().await) })
    }

    /// Where this connection is, now.
    fn state(&self) -> PyState {
        PyState::of(self.inner.state())
    }

    /// Waits for the connection to stop being usable, and says why.
    fn closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        self.bridge.awaitable(
            py,
            async move { Ok(PyState::of(connection.closed().await)) },
        )
    }

    /// `PUB <subject> <#bytes>`.
    ///
    /// Returning means the operation was queued for the driver and nothing
    /// more: Core NATS has no publish acknowledgement, and a successful
    /// socket write only establishes that bytes went toward the server.
    /// `flush` is how a caller learns the server read them.
    fn publish<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'py, PyAny>,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject = subject_of(subject)?;
        let payload = payload_of(payload)?;
        let connection = self.inner.clone();
        self.bridge.awaitable(py, async move {
            connection.publish(subject, payload).await.map_err(errno_of)
        })
    }

    /// `PUB` or `HPUB`, with an optional reply subject and header block.
    ///
    /// The reply subject is what makes a publication a request: it is where a
    /// responder publishes its answer. Headers need
    /// [`headers_supported`](PyConnection::headers_supported) — a server that
    /// did not advertise them does not know the `HPUB` verb — and that
    /// refusal is the library's.
    #[pyo3(signature = (subject, *, reply_to=None, headers=None, payload=None))]
    fn publish_with<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'py, PyAny>,
        reply_to: Option<&Bound<'py, PyAny>>,
        headers: Option<&Bound<'py, PyAny>>,
        payload: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject = subject_of(subject)?;
        let reply_to = reply_to.map(subject_of).transpose()?;
        let headers = headers.map(headers_of).transpose()?;
        let payload = payload.map(payload_of).transpose()?.unwrap_or_default();
        let connection = self.inner.clone();
        self.bridge.awaitable(py, async move {
            connection
                .publish_with(subject, reply_to.as_deref(), headers.as_ref(), payload)
                .await
                .map_err(errno_of)
        })
    }

    /// `SUB <subject> <sid>`: one copy of every matching publication.
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject = subject_of(subject)?;
        let connection = self.inner.clone();
        let bridge = self.bridge.clone();
        let reactor = self.reactor.clone();
        self.bridge.awaitable(py, async move {
            let subscription = connection.subscribe(subject).await.map_err(errno_of)?;
            Ok(PySubscription::of(subscription, bridge, reactor))
        })
    }

    /// `SUB <subject> <queue group> <sid>`: one copy per publication between
    /// all members of the group.
    ///
    /// A queue group is not a broker queue — an ordinary subscription beside
    /// one still gets its own copy — and which member receives a publication
    /// is the server's choice.
    fn subscribe_with_queue_group<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'py, PyAny>,
        queue_group: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject = subject_of(subject)?;
        let queue_group = subject_of(queue_group)?;
        let connection = self.inner.clone();
        let bridge = self.bridge.clone();
        let reactor = self.reactor.clone();
        self.bridge.awaitable(py, async move {
            let subscription = connection
                .subscribe_with_queue_group(subject, queue_group)
                .await
                .map_err(errno_of)?;
            Ok(PySubscription::of(subscription, bridge, reactor))
        })
    }

    /// Publishes a request and waits at most `timeout` seconds for one reply.
    ///
    /// `timeout` is a plain argument with no default, here as in the library,
    /// because a request API that can hang is the failure this call exists to
    /// prevent. Three outcomes, and they are different facts: a `Message`;
    /// `weida_nats.NoResponders`, which arrives in one round trip because
    /// nobody was subscribed *then*; and `weida_nats.RequestTimeout`, which
    /// means somebody may well have been.
    fn request<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'py, PyAny>,
        payload: &Bound<'py, PyAny>,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject = subject_of(subject)?;
        let payload = payload_of(payload)?;
        let window = options::window(py, "timeout", timeout)?;
        let connection = self.inner.clone();
        self.bridge.awaitable(py, async move {
            connection
                .request(subject, payload, window)
                .await
                .map(PyMessage::of)
                .map_err(errno_of)
        })
    }

    /// Publishes a request and collects every reply that arrives inside
    /// `timeout` seconds, up to `max_replies`.
    ///
    /// Scatter-gather. It returns what arrived when the window closed, which
    /// may be an empty list: the caller asked for a window, not for an
    /// answer. A `NATS/1.0 503` is still `weida_nats.NoResponders`, because
    /// that is a definite answer rather than an empty window.
    fn request_many<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'py, PyAny>,
        payload: &Bound<'py, PyAny>,
        timeout: f64,
        max_replies: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject = subject_of(subject)?;
        let payload = payload_of(payload)?;
        let window = options::window(py, "timeout", timeout)?;
        let max_replies = options::bounded(py, "max_replies", max_replies)?;
        let connection = self.inner.clone();
        self.bridge.awaitable(py, async move {
            connection
                .request_many(subject, payload, window, max_replies)
                .await
                .map(|replies| replies.into_iter().map(PyMessage::of).collect::<Vec<_>>())
                .map_err(errno_of)
        })
    }

    /// `PING`, answered when the `PONG` comes back.
    ///
    /// The protocol's only round trip, and therefore the only way to learn
    /// that the server has read everything written before it. It is not a
    /// delivery confirmation: Core NATS has none.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        self.bridge.awaitable(
            py,
            async move { connection.flush().await.map_err(errno_of) },
        )
    }

    /// Ends the connection. Idempotent, because closing a closed connection
    /// is not a failure and there is no `CLOSE` verb to be refused.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        self.bridge.awaitable(
            py,
            async move { connection.close().await.map_err(errno_of) },
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nats.Connection {} max_payload={}{}>",
            match self.inner.state() {
                weida_nats::State::Connected => "connected".to_owned(),
                weida_nats::State::Closed => "closed".to_owned(),
                weida_nats::State::Failed(why) => format!("failed: {why}"),
            },
            self.inner.max_payload(),
            if self.inner.headers_supported() {
                " headers"
            } else {
                ""
            }
        )
    }
}
