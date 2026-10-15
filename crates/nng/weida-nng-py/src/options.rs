//! The socket options, and the table that says what NNG's own names do
//! here.
//!
//! Two surfaces, and they answer different questions. `SocketOptions` is
//! what a socket is opened with, and every value in it is validated **where
//! it is written** — a depth past the ceiling, a `max_ttl` of zero or a
//! buffer on a protocol that has none fails at the constructor rather than
//! at the first send. `OPTIONS` and `option(name)` are the table of
//! `nng_options(5)`, so a program being ported can ask what became of the
//! name it used instead of discovering that nothing did.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyList;
use weida_nng::optiontable::{self, Disposition, OptionRow, Scope};
use weida_nng::{Error, SocketOptions};

use crate::context::seconds_to_duration;
use crate::errors;

/// `weida_nng.SocketOptions`: what a socket is opened with.
///
/// Every field is a keyword argument, every omitted one is the library's
/// default, and the whole set is validated at construction. A socket type
/// that does not support an option refuses it when the socket is opened,
/// which is the second half of the same rule — a REQ socket with a
/// `send_depth` is refused by name, because a context holds one transaction
/// and there is nothing to queue.
#[pyclass(frozen, name = "SocketOptions", module = "weida_nng")]
pub struct PySocketOptions {
    options: SocketOptions,
    /// Prefixes a SUB socket subscribes to when it is opened. The Rust
    /// surface takes them after construction; a Python caller wants one
    /// object that says what the socket is.
    pub subscriptions: Vec<Vec<u8>>,
}

impl PySocketOptions {
    /// The library's own options value.
    pub fn library(&self) -> SocketOptions {
        self.options.clone()
    }
}

#[pymethods]
impl PySocketOptions {
    #[new]
    #[pyo3(signature = (
        *,
        recv_max_size=None,
        send_depth=None,
        recv_depth=None,
        send_timeout=None,
        recv_timeout=None,
        reconnect_min=None,
        reconnect_max=None,
        max_pipes=None,
        handshake_timeout=None,
        max_addresses=None,
        max_pipes_per_peer=None,
        max_tls_handshakes=None,
        message_timeout=None,
        idle_timeout=None,
        sub_prefer_new=None,
        resend_time=None,
        max_ttl=None,
        survey_time=None,
        subscribe=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        recv_max_size: Option<u64>,
        send_depth: Option<usize>,
        recv_depth: Option<usize>,
        send_timeout: Option<f64>,
        recv_timeout: Option<f64>,
        reconnect_min: Option<f64>,
        reconnect_max: Option<f64>,
        max_pipes: Option<usize>,
        handshake_timeout: Option<f64>,
        max_addresses: Option<usize>,
        max_pipes_per_peer: Option<usize>,
        max_tls_handshakes: Option<usize>,
        message_timeout: Option<f64>,
        idle_timeout: Option<f64>,
        sub_prefer_new: Option<bool>,
        resend_time: Option<f64>,
        max_ttl: Option<usize>,
        survey_time: Option<f64>,
        subscribe: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PySocketOptions> {
        let mut options = SocketOptions::default();
        if let Some(bytes) = recv_max_size {
            options.recv_max_size = bytes;
        }
        options.send_depth = send_depth.or(options.send_depth);
        options.recv_depth = recv_depth.or(options.recv_depth);
        if let Some(seconds) = send_timeout {
            options.send_timeout = Some(errors::raise(
                py,
                seconds_to_duration("send_timeout", seconds),
            )?);
        }
        if let Some(seconds) = recv_timeout {
            options.recv_timeout = Some(errors::raise(
                py,
                seconds_to_duration("recv_timeout", seconds),
            )?);
        }
        options.reconnect_min =
            duration_or(py, "reconnect_min", reconnect_min, options.reconnect_min)?;
        options.reconnect_max =
            duration_or(py, "reconnect_max", reconnect_max, options.reconnect_max)?;
        if let Some(pipes) = max_pipes {
            options.max_pipes = pipes;
        }
        options.handshake_timeout = duration_or(
            py,
            "handshake_timeout",
            handshake_timeout,
            options.handshake_timeout,
        )?;
        if let Some(addresses) = max_addresses {
            options.max_addresses = addresses;
        }
        if let Some(peers) = max_pipes_per_peer {
            options.max_pipes_per_peer = peers;
        }
        if let Some(handshakes) = max_tls_handshakes {
            options.max_tls_handshakes = handshakes;
        }
        if let Some(seconds) = message_timeout {
            options.message_timeout = Some(errors::raise(
                py,
                seconds_to_duration("message_timeout", seconds),
            )?);
        }
        if let Some(seconds) = idle_timeout {
            options.idle_timeout = Some(errors::raise(
                py,
                seconds_to_duration("idle_timeout", seconds),
            )?);
        }
        if let Some(prefer_new) = sub_prefer_new {
            options.sub_prefer_new = prefer_new;
        }
        options.resend_time = duration_or(py, "resend_time", resend_time, options.resend_time)?;
        if let Some(ttl) = max_ttl {
            options.max_ttl = ttl;
        }
        options.survey_time = duration_or(py, "survey_time", survey_time, options.survey_time)?;

        // Refused here, at configuration time, and not at the first send.
        errors::raise(py, options.validate())?;

        let subscriptions = match subscribe {
            None => Vec::new(),
            Some(value) => prefixes(value)?,
        };
        Ok(PySocketOptions {
            options,
            subscriptions,
        })
    }

    /// `NNG_OPT_RECVMAXSZ`, in bytes. Zero is NNG's "no limit".
    #[getter]
    fn recv_max_size(&self) -> u64 {
        self.options.recv_max_size
    }

    /// `NNG_OPT_SENDBUF`, or `None` for the protocol's own default.
    #[getter]
    fn send_depth(&self) -> Option<usize> {
        self.options.send_depth
    }

    /// `NNG_OPT_RECVBUF`, or `None` for the library's default depth.
    #[getter]
    fn recv_depth(&self) -> Option<usize> {
        self.options.recv_depth
    }

    /// `NNG_OPT_SENDTIMEO` in seconds, or `None` for NNG's forever.
    #[getter]
    fn send_timeout(&self) -> Option<f64> {
        self.options.send_timeout.map(|t| t.as_secs_f64())
    }

    /// `NNG_OPT_RECVTIMEO` in seconds, or `None` for NNG's forever.
    #[getter]
    fn recv_timeout(&self) -> Option<f64> {
        self.options.recv_timeout.map(|t| t.as_secs_f64())
    }

    /// `NNG_OPT_MAXTTL`.
    #[getter]
    fn max_ttl(&self) -> usize {
        self.options.max_ttl
    }

    /// `NNG_OPT_SURVEYOR_SURVEYTIME`, in seconds.
    #[getter]
    fn survey_time(&self) -> f64 {
        self.options.survey_time.as_secs_f64()
    }

    /// `NNG_OPT_REQ_RESENDTIME`, in seconds.
    #[getter]
    fn resend_time(&self) -> f64 {
        self.options.resend_time.as_secs_f64()
    }

    /// `NNG_OPT_SUB_PREFNEW`.
    #[getter]
    fn sub_prefer_new(&self) -> bool {
        self.options.sub_prefer_new
    }

    /// Pipes this socket admits at once — a bound SP does not have and
    /// this library adds.
    #[getter]
    fn max_pipes(&self) -> usize {
        self.options.max_pipes
    }

    /// Pipes one remote host (or one local user, on `ipc`) may hold on one
    /// listener's socket — a bound SP does not have and this library adds.
    #[getter]
    fn max_pipes_per_peer(&self) -> usize {
        self.options.max_pipes_per_peer
    }

    /// How long one message may take to cross a connection, in seconds, or
    /// `None` for no limit — a bound SP does not have and this library adds.
    #[getter]
    fn message_timeout(&self) -> Option<f64> {
        self.options.message_timeout.map(|t| t.as_secs_f64())
    }

    /// How long a pipe may go with no octet moving in either direction, in
    /// seconds, or `None` for no limit.
    #[getter]
    fn idle_timeout(&self) -> Option<f64> {
        self.options.idle_timeout.map(|t| t.as_secs_f64())
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nng.SocketOptions recv_max_size={} max_pipes={} max_ttl={}>",
            self.options.recv_max_size, self.options.max_pipes, self.options.max_ttl
        )
    }
}

/// A duration keyword, or the default it replaces.
fn duration_or(
    py: Python<'_>,
    what: &str,
    seconds: Option<f64>,
    default: Duration,
) -> PyResult<Duration> {
    match seconds {
        None => Ok(default),
        Some(seconds) => errors::raise(py, seconds_to_duration(what, seconds)),
    }
}

/// One prefix, or an iterable of them.
fn prefixes(value: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
    if let Ok(one) = weida_py_core::payload_of(value) {
        return Ok(vec![one]);
    }
    let mut all = Vec::new();
    for item in value.try_iter()? {
        all.push(weida_py_core::payload_of(&item?)?);
    }
    Ok(all)
}

/// `weida_nng.NngOption`: one row of `nng_options(5)`, as this library
/// answers it.
#[pyclass(frozen, get_all, name = "NngOption", module = "weida_nng")]
pub struct PyNngOption {
    /// The name NNG uses, which is what a ported program greps for.
    pub name: String,
    /// `"socket"`, `"dialer"`, `"listener"`, `"pipe"` or `"transport"`.
    pub scope: String,
    /// Whether this library honours it.
    pub honoured: bool,
    /// What honours it, or what is missing.
    pub note: String,
    /// Why it is refused, or `None` when it is honoured.
    pub refusal: Option<String>,
}

#[pymethods]
impl PyNngOption {
    fn __repr__(&self) -> String {
        format!(
            "<weida_nng.NngOption {} {}>",
            self.name,
            if self.honoured { "honoured" } else { "refused" }
        )
    }
}

impl PyNngOption {
    fn of(row: &OptionRow) -> PyNngOption {
        PyNngOption {
            name: row.name.to_owned(),
            scope: match row.scope {
                Scope::Socket => "socket",
                Scope::Dialer => "dialer",
                Scope::Listener => "listener",
                Scope::Pipe => "pipe",
                Scope::Transport => "transport",
            }
            .to_owned(),
            honoured: matches!(row.disposition, Disposition::Honoured(_)),
            note: row.note.to_owned(),
            refusal: row.refusal().map(|error| match error {
                Error::ENOTSUP(cause) => cause.to_string(),
                other => other.cause().to_owned(),
            }),
        }
    }
}

/// The whole table, in the library's order.
pub fn table(py: Python<'_>) -> PyResult<Py<PyList>> {
    let rows: Vec<Py<PyNngOption>> = optiontable::OPTIONS
        .iter()
        .map(|row| Py::new(py, PyNngOption::of(row)))
        .collect::<PyResult<_>>()?;
    Ok(PyList::new(py, rows)?.unbind())
}

/// One row by NNG's name, or `None` for a name NNG does not have.
pub fn row(py: Python<'_>, name: &str) -> PyResult<Option<Py<PyNngOption>>> {
    match optiontable::lookup(name) {
        None => Ok(None),
        Some(row) => Py::new(py, PyNngOption::of(row)).map(Some),
    }
}
