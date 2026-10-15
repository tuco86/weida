//! The connection: the handshake, the options, and the reactor underneath.
//!
//! `await weida_amqp.connect(host, port, ...)` is the whole entry point. It
//! returns a `Connection` whose reactor the library owns, so a plain Python
//! process needs no ambient Tokio runtime and no asyncio loop is involved in
//! driving the wire — the loop only awaits the futures the bridge hands it.
//!
//! # One constructor, not three
//!
//! `weida_zmq.Context` offers three, because libzmq's context is a
//! configurable object a program may want on an ambient reactor. AMQP's
//! connection is not: it is a TCP connection with a handshake, and
//! `Connection::connect` takes the `Exec` as an argument. So this module
//! creates the reactor with `Exec::owned` and keeps it alive for as long as
//! the connection object exists. The ambient-runtime form would be reachable
//! only from a Rust host that already runs one, and such a host would use the
//! Rust client rather than the binding.
//!
//! # TLS is the caller's certificates or nothing
//!
//! `connect_tls` is deliberately absent from this surface. The Rust client
//! takes a `rustls::ClientConfig`, because which certificates an application
//! trusts is the application's decision, and there is no way to express one
//! from Python without either inventing a trust policy here or exposing
//! rustls's whole builder. Saying so is better than a `verify=False` that
//! nobody audits: `docs/libraries/amqp.md` records it as the one row this
//! binding does not carry.

use pyo3::prelude::*;
use weida_amqp::session::SessionOptions;
use weida_amqp::{Connection, ConnectionOptions, Error, Sasl, State};
use weida_py_core::{Bridge, duration, interruptible};
use weida_runtime::{Exec, OwnedReactor};

use crate::errors;
use crate::session::PySession;

/// `weida_amqp.Connection`.
///
/// Holds the reactor: an `OwnedReactor` shuts its runtime down in the
/// background when the last owner drops it, so a Python program that lets a
/// connection go does not leave threads behind.
#[pyclass(frozen, name = "Connection", module = "weida_amqp")]
pub struct PyConnection {
    inner: Connection,
    bridge: Bridge,
    /// Kept beside the connection for exactly as long as it lives.
    #[allow(
        dead_code,
        reason = "the reactor is alive because this field is: dropping it \
                  shuts the runtime down"
    )]
    reactor: std::sync::Arc<OwnedReactor>,
}

impl PyConnection {
    pub fn bridge(&self) -> &Bridge {
        &self.bridge
    }

    pub fn inner(&self) -> &Connection {
        &self.inner
    }

    pub fn reactor(&self) -> &std::sync::Arc<OwnedReactor> {
        &self.reactor
    }
}

/// The options a Python caller may set, turned into the library's.
///
/// Every one is refused where it is configured rather than corrected: a
/// `max_frame_size` below the 512 both peers MUST accept is a
/// `Configuration` error here and not a number quietly raised, which is
/// 0013 §4.4 item 4.
#[allow(clippy::too_many_arguments, reason = "one keyword argument per field")]
pub fn options_from(
    py: Python<'_>,
    container_id: &str,
    hostname: Option<String>,
    max_frame_size: Option<u32>,
    channel_max: Option<u16>,
    idle_time_out: Option<f64>,
    sasl_user: Option<String>,
    sasl_password: Option<String>,
    sasl_anonymous: bool,
    handshake_timeout: Option<f64>,
) -> PyResult<ConnectionOptions> {
    let mut options = ConnectionOptions::new(container_id);
    options.hostname = hostname;
    if let Some(size) = max_frame_size {
        options.max_frame_size = size;
    }
    if let Some(channels) = channel_max {
        options.channel_max = channels;
    }
    if let Some(seconds) = idle_time_out {
        // Zero means unset on the wire, so it means unset here: a Python
        // caller writing 0 is asking for no idle timeout, not for one of zero
        // milliseconds.
        let limit = duration("idle_time_out", seconds)
            .map_err(|error| errors::configuration(py, error.to_string()))?;
        options.idle_time_out = (!limit.is_zero()).then_some(limit);
    }
    if let Some(seconds) = handshake_timeout {
        options.handshake_timeout = duration("handshake_timeout", seconds)
            .map_err(|error| errors::configuration(py, error.to_string()))?;
    }
    options.sasl = match (sasl_anonymous, sasl_user, sasl_password) {
        (true, None, None) => Sasl::Anonymous,
        (false, Some(username), Some(password)) => Sasl::Plain { username, password },
        (false, None, None) => Sasl::None,
        (true, _, _) => {
            return Err(errors::configuration(
                py,
                "sasl_anonymous and a username are two different mechanisms; \
                 AMQP chooses one and does not negotiate",
            ));
        }
        (false, user, password) => {
            return Err(errors::configuration(
                py,
                format!(
                    "PLAIN needs both a username and a password; got user={} \
                     password={}",
                    user.is_some(),
                    password.is_some()
                ),
            ));
        }
    };
    errors::raise(py, options.validate())?;
    Ok(options)
}

#[pymethods]
impl PyConnection {
    /// What the peer said in its `open`, as a dict.
    ///
    /// Read rather than assumed: `max-frame-size` here is the number this
    /// client's frames are bounded by, and `channel-max` the number that
    /// decides how many sessions it may begin.
    fn remote<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let remote = self.inner.remote();
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("container_id", remote.container_id.clone())?;
        dict.set_item("hostname", remote.hostname.clone())?;
        dict.set_item("max_frame_size", remote.max_frame_size)?;
        dict.set_item("channel_max", remote.channel_max)?;
        dict.set_item(
            "idle_time_out",
            remote.idle_time_out.map(|ms| f64::from(ms) / 1000.0),
        )?;
        dict.set_item("offered_capabilities", remote.offered_capabilities.clone())?;
        Ok(dict)
    }

    /// `open`, `closing`, `closed` or `failed`, as one word.
    fn state(&self) -> &'static str {
        match self.inner.state() {
            State::Open => "open",
            State::Closing => "closing",
            State::Closed(_) => "closed",
            State::Failed(_) => "failed",
        }
    }

    /// Whether a session may still be begun on this connection.
    fn is_usable(&self) -> bool {
        self.inner.state().is_usable()
    }

    /// Begins a session, returning once the answering `begin` has arrived.
    ///
    /// Until it has, nothing is known about the peer's channel numbering or
    /// its `handle-max`, so a session handed over earlier would be one whose
    /// bounds the caller could not read.
    #[pyo3(signature = (*, incoming_window=None, outgoing_window=None, handle_max=None))]
    fn begin<'py>(
        &self,
        py: Python<'py>,
        incoming_window: Option<u32>,
        outgoing_window: Option<u32>,
        handle_max: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        let bridge = self.bridge.clone();
        let reactor = std::sync::Arc::clone(&self.reactor);
        self.bridge.awaitable(py, async move {
            let session = connection
                .begin(options)
                .await
                .map_err(crate::errors::errno_of)?;
            Ok(PySession::of(session, bridge, reactor))
        })
    }

    /// Closes the connection: `close` is the last frame ever written, and
    /// everything already handed to the driver goes out before it.
    ///
    /// Idempotent.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        self.bridge.awaitable(py, async move {
            connection.close().await.map_err(crate::errors::errno_of)
        })
    }

    /// Waits for the connection to leave `open`, and reports why.
    ///
    /// What an application does instead of a reconnect loop this binding does
    /// not have: the reason is here, and redialling is the caller's.
    fn closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        self.bridge.awaitable(py, async move {
            let state = connection.closed().await;
            Ok(match state {
                State::Open => "open".to_owned(),
                State::Closing => "closing".to_owned(),
                State::Closed(None) => "closed".to_owned(),
                State::Closed(Some(condition)) => format!("closed: {condition}"),
                State::Failed(reason) => format!("failed: {reason}"),
            })
        })
    }

    fn __repr__(&self) -> String {
        format!("<Connection {}>", self.state())
    }
}

/// A reactor for one connection.
///
/// Created **synchronously**, because there is nothing to await on until it
/// exists and building a Tokio runtime does not block. The asyncio surface
/// then awaits the handshake on it; the synchronous surface waits for it with
/// [`open`].
pub fn reactor(worker_threads: usize) -> Result<(Exec, OwnedReactor), weida_amqp::Error> {
    Ok(Exec::owned(worker_threads.max(1), "weida-amqp-py")?)
}

/// Builds the reactor, connects, and hands back all three.
///
/// For the **synchronous** surface only. The connect future is spawned on the
/// reactor and waited for here rather than polled here: the handshake arms
/// timers — one deadline per step, because the protocol gives none — and a
/// tokio timer only runs inside a tokio context.
///
/// The asyncio surface must not use this: it would block the very event loop
/// that is supposed to be driving the caller's other tasks, which in a test
/// means blocking the peer it is connecting to.
pub fn open(
    py: Python<'_>,
    worker_threads: usize,
    host: &str,
    port: u16,
    options: ConnectionOptions,
) -> PyResult<(Connection, Exec, OwnedReactor)> {
    let (exec, reactor) = errors::raise(py, reactor(worker_threads))?;
    let connecting = exec.clone();
    let host = host.to_owned();
    let connection = drive(py, &exec, async move {
        Connection::connect(&connecting, &host, port, options).await
    })?;
    Ok((connection, exec, reactor))
}

/// Runs a library future on `exec` and waits for it here, with the GIL released.
///
/// The wait is [`interruptible`]: a signal that raises (Ctrl-C) ends it by
/// aborting the task, which is the cancellation an asyncio task gets. A task
/// that ends without an answer, because its reactor shut down under it, is the
/// connection being gone, so it raises `ConnectionGone` rather than panicking
/// on the Python thread.
pub fn drive<F, T>(py: Python<'_>, exec: &Exec, future: F) -> PyResult<T>
where
    F: Future<Output = Result<T, Error>> + Send + 'static,
    T: Send + 'static,
{
    let handle = exec.spawn(future);
    let abort = handle.abort_handle();
    match interruptible(py, exec, || handle) {
        Err(signal) => {
            abort.abort();
            Err(signal)
        }
        Ok(Ok(answer)) => errors::raise(py, answer),
        Ok(Err(_joined)) => errors::raise(py, Err(Error::ConnectionGone)),
    }
}

impl PyConnection {
    /// Wraps a connection whose reactor is `reactor`.
    pub fn of(inner: Connection, reactor: OwnedReactor) -> PyConnection {
        let bridge = Bridge::new(inner.exec().clone(), errors::to_py);
        PyConnection {
            inner,
            bridge,
            reactor: std::sync::Arc::new(reactor),
        }
    }
}
