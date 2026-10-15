//! The option table, and setting an option by libzmq's own name.
//!
//! # The table is a value, not a document
//!
//! `weida_zmq.OPTIONS` is all 98 rows of `weida-zmq`'s table as Python
//! objects, and `weida_zmq.option("ZMQ_SNDHWM")` looks one up. A caller
//! porting a `zmq_setsockopt` call asks the module and gets an answer —
//! honoured under this name, or refused for one of five reasons — instead of
//! reading a release note. That is 0013 §4.4 item 4's rule carried into
//! Python: **honoured or refused, never silently ignored**.
//!
//! # Refused means refused where it is set
//!
//! `SocketOptions.set("ZMQ_TOS", 1)` raises `EINVAL` at that line,
//! with the table's reason in the message. It does not raise later, at a
//! send that behaves differently from the C program the caller is porting,
//! and it does not do nothing.
//!
//! # Seconds, not milliseconds
//!
//! Every duration in this module is a number of **seconds**, and `None` is
//! libzmq's sentinel — its `-1` for "wait forever", its `0` for "no limit",
//! "no backoff", "the OS default". libzmq spells those as milliseconds in an
//! `int` with three different meanings for two magic numbers; this binding
//! spells a duration the way the rest of its own surface does
//! (`recv(timeout=0.05)`, `Context(close_budget=1.0)`) and says which
//! sentinel a `None` is in each row's documentation. A port from `pyzmq`
//! divides by a thousand once, at the call it is porting, rather than
//! guessing which of two units a number is in.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use weida_zmq::optiontable::{OPTIONS, Refusal, Scope, Verdict, ZmqOption};
use weida_zmq::{
    ContextConfig, CurvePublicKey, CurveSecretKey, Error, Result, RoutingId, SocketOptions,
};

use crate::errors::{errno_of, raise, to_py};

/// One row of libzmq's option table, and what this library does with it.
#[pyclass(frozen, name = "ZmqOption", module = "weida_zmq")]
pub struct PyZmqOption {
    row: &'static ZmqOption,
}

#[pymethods]
impl PyZmqOption {
    /// libzmq's own name, `ZMQ_SNDHWM` and the like.
    #[getter]
    fn name(&self) -> &'static str {
        self.row.name
    }

    /// `"socket"` for `zmq_setsockopt`, `"context"` for `zmq_ctx_set`.
    #[getter]
    fn scope(&self) -> &'static str {
        match self.row.scope {
            Scope::Socket => "socket",
            Scope::Context => "context",
        }
    }

    /// Whether this library implements the option.
    #[getter]
    fn honoured(&self) -> bool {
        matches!(self.row.verdict, Verdict::Honoured(_))
    }

    /// What it is bound to here, for an honoured option.
    #[getter]
    fn binding(&self) -> Option<&'static str> {
        match self.row.verdict {
            Verdict::Honoured(binding) => Some(binding),
            Verdict::Refused(_) => None,
        }
    }

    /// Which of the five reasons a refusal is, as a short tag:
    /// `"no transport"`, `"draft only"`, `"deprecated in favour of ZAP"`,
    /// `"replaced by a weida-runtime construct"` or `"absent"`.
    #[getter]
    fn reason(&self) -> Option<&'static str> {
        match self.row.verdict {
            Verdict::Honoured(_) => None,
            Verdict::Refused(refusal) => Some(match refusal {
                Refusal::NoTransport(_) => "no transport",
                Refusal::DraftOnly => "draft only",
                Refusal::ZapInstead => "deprecated in favour of ZAP",
                Refusal::RuntimeInstead(_) => "replaced by a weida-runtime construct",
                Refusal::Absent(_) => "absent",
            }),
        }
    }

    /// The whole refusal in words, which is what the exception carries.
    #[getter]
    fn refusal(&self) -> Option<String> {
        match self.row.verdict {
            Verdict::Honoured(_) => None,
            Verdict::Refused(refusal) => Some(refusal.to_string()),
        }
    }

    fn __repr__(&self) -> String {
        match self.row.verdict {
            Verdict::Honoured(binding) => {
                format!(
                    "<weida_zmq.ZmqOption {} honoured: {binding}>",
                    self.row.name
                )
            }
            Verdict::Refused(refusal) => {
                format!("<weida_zmq.ZmqOption {} refused: {refusal}>", self.row.name)
            }
        }
    }
}

/// Every row of the table, in the library's own order.
pub fn table(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
    PyTuple::new(
        py,
        OPTIONS
            .iter()
            .map(|row| Py::new(py, PyZmqOption { row }))
            .collect::<PyResult<Vec<_>>>()?,
    )
}

/// One row by libzmq's name, or `None` for a name the table does not have —
/// which includes the read-only options, since those are not settable in
/// libzmq either.
pub fn row(py: Python<'_>, name: &str) -> PyResult<Option<Py<PyZmqOption>>> {
    match weida_zmq::optiontable::option(name) {
        Some(row) => Py::new(py, PyZmqOption { row }).map(Some),
        None => Ok(None),
    }
}

/// The refusal for a name that is refused, absent, or in the other scope.
fn verdict_for(name: &str, wanted: Scope) -> Result<()> {
    let Some(row) = weida_zmq::optiontable::option(name) else {
        return Err(Error::EINVAL(
            format!(
                "{name} is not an option this library records; weida_zmq.OPTIONS is the whole \
                 table, and libzmq's read-only options are not in it because they are not set"
            )
            .into(),
        ));
    };
    if let Some(refusal) = row.refusal() {
        return Err(refusal);
    }
    // `ZMQ_LINGER` is the one honoured row whose scope in libzmq is the
    // socket and whose home here is the context: the close budget spans a
    // context's shutdown, which is what it bounds.
    let scope = if row.name == "ZMQ_LINGER" {
        Scope::Context
    } else {
        row.scope
    };
    if scope != wanted {
        return Err(Error::EINVAL(match scope {
            Scope::Context => format!(
                "{name} is a context option here; set it on ContextOptions and pass that to \
                 Context(options=...)"
            )
            .into(),
            Scope::Socket => format!(
                "{name} is a socket option; set it on SocketOptions and pass that to the \
                 socket's constructor"
            )
            .into(),
        }));
    }
    Ok(())
}

/// A duration in seconds, or `None` for libzmq's sentinel.
fn seconds(name: &str, value: &Bound<'_, PyAny>) -> Result<Option<Duration>> {
    if value.is_none() {
        return Ok(None);
    }
    let seconds: f64 = value
        .extract()
        .map_err(|_| Error::EINVAL(format!("{name} takes a number of seconds, or None").into()))?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(Error::EINVAL(
            format!("{name} takes a finite, non-negative number of seconds, not {seconds}").into(),
        ));
    }
    Ok(Some(Duration::from_secs_f64(seconds)))
}

/// A whole number.
fn count<T>(name: &str, value: &Bound<'_, PyAny>) -> Result<T>
where
    T: for<'a, 'py> FromPyObject<'a, 'py>,
{
    value
        .extract::<T>()
        .map_err(|_| Error::EINVAL(format!("{name} takes a non-negative whole number").into()))
}

/// A flag.
fn flag(name: &str, value: &Bound<'_, PyAny>) -> Result<bool> {
    value
        .extract::<bool>()
        .map_err(|_| Error::EINVAL(format!("{name} takes True or False").into()))
}

/// libzmq's three-valued switch, `ZMQ_TCP_KEEPALIVE`'s: `None` or `-1` leaves
/// the setting to the operating system, `0` or `False` is off, `1` or `True`
/// is on.
fn switch(name: &str, value: &Bound<'_, PyAny>) -> Result<Option<bool>> {
    if value.is_none() {
        return Ok(None);
    }
    match value.extract::<i64>() {
        Ok(-1) => Ok(None),
        Ok(0) => Ok(Some(false)),
        Ok(1) => Ok(Some(true)),
        _ => Err(Error::EINVAL(
            format!("{name} takes None or -1, False or 0, True or 1").into(),
        )),
    }
}

/// Text.
fn text(name: &str, value: &Bound<'_, PyAny>) -> Result<String> {
    value
        .extract::<String>()
        .map_err(|_| Error::EINVAL(format!("{name} takes a str").into()))
}

/// Octets, from `bytes`, from a `bytearray`, or from the identity type the
/// option's own row is about — `CurveKey` for the four CURVE options,
/// `RoutingId` for `ZMQ_ROUTING_ID`. A key that is already a `CurveKey` is
/// not re-parsed and not re-validated: it was checked when it was made.
fn octets(name: &str, value: &Bound<'_, PyAny>) -> Result<Vec<u8>> {
    if let Ok(key) = value.extract::<PyRef<'_, crate::identity::PyCurveKey>>() {
        return Ok(key.octets().to_vec());
    }
    if let Ok(id) = value.extract::<PyRef<'_, crate::identity::PyRoutingId>>() {
        return Ok(id.octets().to_vec());
    }
    if let Ok(text) = value.extract::<String>() {
        // A 40-character Z85 key is the form a configuration file holds, and
        // `str` is how Python holds it.
        return Ok(text.into_bytes());
    }
    weida_py_core::payload_of(value)
        .map_err(|_| Error::EINVAL(format!("{name} takes bytes").into()))
}

/// What a socket is configured with, by libzmq's option names.
///
/// ```python
/// options = weida_zmq.SocketOptions()
/// options.set("ZMQ_RCVTIMEO", 0.25)
/// options.set("ZMQ_ROUTING_ID", b"worker-3")
/// worker = weida_zmq.DealerSocket(context, options)
/// ```
#[pyclass(name = "SocketOptions", module = "weida_zmq")]
#[derive(Default)]
pub struct PySocketOptions {
    inner: SocketOptions,
    /// `ZMQ_SUBSCRIBE`/`ZMQ_UNSUBSCRIBE` are honoured by a method rather than
    /// by a field, so they are held here and applied when the socket is
    /// built — and refused there for a socket type that has no
    /// subscriptions, exactly as libzmq refuses them.
    pub subscriptions: Vec<(Vec<u8>, bool)>,
}

impl PySocketOptions {
    /// The library's options, as the socket constructor takes them.
    pub fn library(&self) -> SocketOptions {
        self.inner.clone()
    }
}

#[pymethods]
impl PySocketOptions {
    /// The defaults, which are libzmq's except for the two this library
    /// deliberately changes.
    #[new]
    fn new() -> PySocketOptions {
        PySocketOptions::default()
    }

    /// Sets one option by libzmq's name.
    ///
    /// Raises `EINVAL` **here** — not later — for an option this library
    /// refuses, with the table's reason; for a context option; for a name
    /// libzmq does not have; and for a value the option cannot take.
    fn set(&mut self, py: Python<'_>, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        raise(py, verdict_for(name, Scope::Socket))?;
        raise(py, self.apply(name, value))
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.SocketOptions sndhwm={} rcvhwm={} maxmsgsize={}>",
            self.inner.pipe.outgoing.hwm, self.inner.pipe.incoming.hwm, self.inner.max_message_size
        )
    }
}

impl PySocketOptions {
    /// The honoured socket rows, one arm each. The compiler cannot check this
    /// against the table, so `test_options.py` walks every honoured row and
    /// sets it.
    fn apply(&mut self, name: &str, value: &Bound<'_, PyAny>) -> Result<()> {
        let options = &mut self.inner;
        match name {
            "ZMQ_SNDHWM" => options.pipe.outgoing.hwm = count(name, value)?,
            "ZMQ_RCVHWM" => options.pipe.incoming.hwm = count(name, value)?,
            "ZMQ_MAXMSGSIZE" => options.max_message_size = count(name, value)?,
            "ZMQ_SNDTIMEO" => options.send_timeout = seconds(name, value)?,
            "ZMQ_RCVTIMEO" => options.recv_timeout = seconds(name, value)?,
            "ZMQ_RECONNECT_IVL" => options.reconnect_ivl = seconds(name, value)?,
            "ZMQ_RECONNECT_IVL_MAX" => options.reconnect_ivl_max = seconds(name, value)?,
            "ZMQ_HANDSHAKE_IVL" => options.handshake_ivl = seconds(name, value)?,
            "ZMQ_CONNECT_TIMEOUT" => options.connect_timeout = seconds(name, value)?,
            "ZMQ_HEARTBEAT_IVL" => options.heartbeat_ivl = seconds(name, value)?,
            "ZMQ_HEARTBEAT_TIMEOUT" => options.heartbeat_timeout = seconds(name, value)?,
            "ZMQ_HEARTBEAT_TTL" => options.heartbeat_ttl = seconds(name, value)?,
            "ZMQ_TCP_KEEPALIVE" => options.tcp_keepalive = switch(name, value)?,
            "ZMQ_TCP_KEEPALIVE_IDLE" => options.tcp_keepalive_idle = seconds(name, value)?,
            "ZMQ_TCP_KEEPALIVE_INTVL" => options.tcp_keepalive_intvl = seconds(name, value)?,
            "ZMQ_TCP_KEEPALIVE_CNT" => {
                options.tcp_keepalive_cnt = if value.is_none() {
                    None
                } else {
                    Some(count(name, value)?)
                };
            }
            "ZMQ_IMMEDIATE" => options.immediate = flag(name, value)?,
            "ZMQ_BACKLOG" => options.backlog = count(name, value)?,
            "ZMQ_ROUTING_ID" | "ZMQ_IDENTITY" => {
                options.routing_id = Some(RoutingId::new(octets(name, value)?)?);
            }
            "ZMQ_ROUTER_MANDATORY" => options.router_mandatory = flag(name, value)?,
            "ZMQ_ROUTER_HANDOVER" => options.router_handover = flag(name, value)?,
            "ZMQ_PROBE_ROUTER" => options.probe_router = flag(name, value)?,
            "ZMQ_REQ_CORRELATE" => options.req_correlate = flag(name, value)?,
            "ZMQ_REQ_RELAXED" => options.req_relaxed = flag(name, value)?,
            "ZMQ_SUBSCRIBE" => self.subscriptions.push((octets(name, value)?, true)),
            "ZMQ_UNSUBSCRIBE" => self.subscriptions.push((octets(name, value)?, false)),
            "ZMQ_XPUB_VERBOSE" => options.xpub_verbose = flag(name, value)?,
            "ZMQ_XPUB_VERBOSER" => options.xpub_verboser = flag(name, value)?,
            "ZMQ_XPUB_MANUAL" => options.xpub_manual = flag(name, value)?,
            "ZMQ_XPUB_WELCOME_MSG" => options.xpub_welcome_msg = Some(octets(name, value)?),
            "ZMQ_PLAIN_SERVER" => options.plain_server = flag(name, value)?,
            "ZMQ_PLAIN_USERNAME" => options.plain_username = Some(text(name, value)?),
            "ZMQ_PLAIN_PASSWORD" => options.plain_password = Some(text(name, value)?),
            "ZMQ_CURVE_SERVER" => options.curve_server = flag(name, value)?,
            "ZMQ_CURVE_PUBLICKEY" => {
                options.curve_publickey = Some(CurvePublicKey::parse(&octets(name, value)?)?);
            }
            "ZMQ_CURVE_SECRETKEY" => {
                options.curve_secretkey = Some(CurveSecretKey::parse(&octets(name, value)?)?);
            }
            "ZMQ_CURVE_SERVERKEY" => {
                options.curve_serverkey = Some(CurvePublicKey::parse(&octets(name, value)?)?);
            }
            "ZMQ_ZAP_DOMAIN" => options.zap_domain = text(name, value)?,
            "ZMQ_ZAP_ENFORCE_DOMAIN" => options.zap_enforce_domain = flag(name, value)?,
            other => {
                return Err(Error::EINVAL(
                    format!(
                        "{other} is honoured by this library but not through SocketOptions.set; \
                         weida_zmq.option({other:?}).binding names where it lives"
                    )
                    .into(),
                ));
            }
        }
        Ok(())
    }
}

/// What a context is configured with, by libzmq's option names.
///
/// Two rows live here: `ZMQ_MAX_SOCKETS`, which is a context option in libzmq
/// too, and `ZMQ_LINGER`, which libzmq puts on the socket and this library
/// implements as the context's **finite** close budget.
#[pyclass(name = "ContextOptions", module = "weida_zmq")]
#[derive(Default)]
pub struct PyContextOptions {
    config: ContextConfig,
}

impl PyContextOptions {
    /// The library's configuration.
    pub fn library(&self) -> ContextConfig {
        self.config.clone()
    }
}

#[pymethods]
impl PyContextOptions {
    #[new]
    fn new() -> PyContextOptions {
        PyContextOptions::default()
    }

    /// Sets one option by libzmq's name, refusing here rather than later.
    fn set(&mut self, py: Python<'_>, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        raise(py, verdict_for(name, Scope::Context))?;
        match name {
            "ZMQ_MAX_SOCKETS" => self.config.max_sockets = raise(py, count(name, value))?,
            "ZMQ_LINGER" => {
                // libzmq's -1 is "wait forever"; this library has no such
                // value, which is the whole point of the deviation.
                let budget = raise(py, seconds(name, value))?.ok_or_else(|| {
                    to_py(
                        py,
                        &errno_of(Error::EINVAL(
                            "ZMQ_LINGER is finite here: libzmq's -1 (wait forever) is the \
                             default this library deliberately changed, so None is not a value \
                             it takes"
                                .into(),
                        )),
                    )
                })?;
                self.config.close_budget = budget;
            }
            other => {
                return Err(to_py(
                    py,
                    &errno_of(Error::EINVAL(
                        format!(
                            "{other} is honoured by this library but not through \
                             ContextOptions.set; weida_zmq.option({other:?}).binding names \
                             where it lives"
                        )
                        .into(),
                    )),
                ));
            }
        }
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.ContextOptions max_sockets={} close_budget={}>",
            self.config.max_sockets,
            self.config.close_budget.as_secs_f64()
        )
    }
}
