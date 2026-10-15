//! The values that cross: a message, a header block, the server's `INFO` and
//! where a connection is.
//!
//! All four are plain data, so both surfaces of this module — the coroutines
//! of `weida_nats` and the blocking calls of `weida_nats.sync` — hand back
//! the *same* classes. A Python program that checks
//! `isinstance(message, weida_nats.Message)` gets the same answer either way,
//! and there is no second `Message` to keep in step.
//!
//! # Octets, not text
//!
//! `subject`, `reply_to` and `payload` are `bytes`, because that is what the
//! library keeps them as and why: the payload is opaque by definition, and a
//! subject is remote input that a conforming server may deliver in any
//! encoding. [`PyMessage::subject_str`] is there for the ordinary case where
//! it is text, and is `None` where it is not — a decode that cannot fail is
//! a decode that lies.
//!
//! Header names and values are `str`, because ADR-4's grammar is HTTP-like
//! text and a client compares them.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use weida_nats::{Message, OwnedHeaders, RemoteInfo, State};
use weida_py_core::LazyBytes;

/// One delivered message.
///
/// `subject`, `reply_to` and `payload` become `bytes` on first access and the
/// same object is returned after that, so `m.payload is m.payload` holds and
/// reading a field twice costs one copy. `headers` is deliberately not cached:
/// it is a list of pairs the caller may change, so each access returns a
/// fresh one.
#[pyclass(frozen, name = "Message", module = "weida_nats")]
pub struct PyMessage {
    inner: Message,
    subject_cell: LazyBytes,
    reply_to_cell: LazyBytes,
    payload_cell: LazyBytes,
}

impl PyMessage {
    /// Wraps a delivered message.
    pub fn of(message: Message) -> PyMessage {
        PyMessage {
            inner: message,
            subject_cell: LazyBytes::new(),
            reply_to_cell: LazyBytes::new(),
            payload_cell: LazyBytes::new(),
        }
    }

    /// The library's message back, for a receive that took one and was
    /// cancelled before the caller got it.
    pub fn into_inner(self) -> Message {
        self.inner
    }
}

#[pymethods]
impl PyMessage {
    /// The literal subject this message was published on, as octets — not
    /// the pattern that matched it.
    #[getter]
    fn subject<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        self.subject_cell.get(py, &self.inner.subject)
    }

    /// The subject as text, or `None` where it is not UTF-8.
    #[getter]
    fn subject_str(&self) -> Option<&str> {
        self.inner.subject_str()
    }

    /// The `sid` of the subscription this copy was delivered to. Two
    /// subscriptions on one connection can match the same publication, and
    /// each gets its own copy.
    #[getter]
    fn sid(&self) -> u64 {
        self.inner.sid
    }

    /// The subject the publisher is listening for a response on, where there
    /// was one. This is the whole of request-reply's correlation.
    #[getter]
    fn reply_to<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.inner
            .reply_to
            .as_deref()
            .map(|reply_to| self.reply_to_cell.get(py, reply_to))
    }

    /// The payload, exactly as long as the control line declared. May be
    /// empty, and may hold `CRLF`.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        self.payload_cell.get(py, &self.inner.payload)
    }

    /// The header block as `[(name, value)]`, or `None` where the message
    /// arrived as `MSG` rather than `HMSG`.
    ///
    /// A list of pairs and not a dict, for the two reasons the codec gives:
    /// names may repeat with different values, and case is preserved between
    /// publisher and receiver. A dict would have to fold one of the two away.
    #[getter]
    fn headers(&self) -> Option<Vec<(String, String)>> {
        self.inner.headers.as_ref().map(|headers| {
            headers
                .entries()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect()
        })
    }

    /// The `NATS/1.0` status, where the block carried one.
    #[getter]
    fn status(&self) -> Option<u16> {
        self.inner.status()
    }

    /// Whether this is the no-responder answer to a request: a `503` with no
    /// payload.
    #[getter]
    fn is_no_responders(&self) -> bool {
        self.inner.is_no_responders()
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nats.Message subject={:?} sid={} payload={} octets{}>",
            String::from_utf8_lossy(&self.inner.subject),
            self.inner.sid,
            self.inner.payload.len(),
            self.inner
                .status()
                .map_or_else(String::new, |status| format!(" status={status}"))
        )
    }
}

/// Reads a subject argument: `str` or `bytes`.
///
/// A subject is dot-separated text in every example anyone has read, and it
/// is octets on the wire, so both spellings arrive here and `str` is encoded
/// as UTF-8. This is the one place in this module where a `str` is encoded
/// rather than refused, and the reason it is not the payload's reason: a
/// subject has a grammar the protocol defines in terms of characters, while
/// "the protocol assigns no payload schema or content type".
///
/// Whether the result *is* a subject — the empty-token rule, the two
/// wildcards, the final `>` — is the library's to say, and it says it at the
/// call this feeds.
pub fn subject_of(subject: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    match subject.extract::<String>() {
        Ok(text) => Ok(text.into_bytes()),
        Err(_) => weida_py_core::payload_of(subject),
    }
}

/// Reads a Python header argument into the library's owned block.
///
/// Two spellings, because both are what a Python caller reaches for: a
/// mapping (`{"Trace-Id": "abc"}`, whose insertion order is the order sent)
/// and a sequence of pairs (`[("Link", "a"), ("Link", "b")]`, which is the
/// only one of the two that can repeat a name). The pair form is the general
/// one; the mapping is the convenience.
pub fn headers_of(headers: &Bound<'_, PyAny>) -> PyResult<OwnedHeaders> {
    let mut block = OwnedHeaders::new();
    if let Ok(mapping) = headers.cast::<PyDict>() {
        for (name, value) in mapping.iter() {
            block.push(name.extract::<String>()?, value.extract::<String>()?);
        }
        return Ok(block);
    }
    for entry in headers.try_iter()? {
        let (name, value) = entry?.extract::<(String, String)>()?;
        block.push(name, value);
    }
    Ok(block)
}

/// The server's `INFO`, as this connection currently understands it.
#[pyclass(frozen, name = "RemoteInfo", module = "weida_nats")]
pub struct PyRemoteInfo {
    inner: RemoteInfo,
}

impl PyRemoteInfo {
    /// Wraps one view of the server.
    pub fn of(info: RemoteInfo) -> PyRemoteInfo {
        PyRemoteInfo { inner: info }
    }
}

#[pymethods]
impl PyRemoteInfo {
    /// "The unique identifier of the NATS server."
    #[getter]
    fn server_id(&self) -> Option<&str> {
        self.inner.server_id.as_deref()
    }

    /// "The name of the NATS server."
    #[getter]
    fn server_name(&self) -> Option<&str> {
        self.inner.server_name.as_deref()
    }

    /// "The version of NATS."
    #[getter]
    fn version(&self) -> Option<&str> {
        self.inner.version.as_deref()
    }

    /// "An integer indicating the protocol version of the server."
    #[getter]
    fn proto(&self) -> u64 {
        self.inner.proto
    }

    /// "The IP address used to start the NATS server."
    #[getter]
    fn host(&self) -> Option<&str> {
        self.inner.host.as_deref()
    }

    /// "The port number the NATS server is configured to listen on."
    #[getter]
    fn port(&self) -> Option<u64> {
        self.inner.port
    }

    /// "Maximum payload size, in bytes, that the server will accept from the
    /// client" — the protocol's own bound, checked by the library before
    /// anything is written.
    #[getter]
    fn max_payload(&self) -> u64 {
        self.inner.max_payload
    }

    /// "Whether the server supports headers."
    #[getter]
    fn headers(&self) -> bool {
        self.inner.headers
    }

    /// "If this is true, then the client should try to authenticate upon
    /// connect."
    #[getter]
    fn auth_required(&self) -> bool {
        self.inner.auth_required
    }

    /// "If this is true, then the client must perform the TLS/1.2
    /// handshake."
    #[getter]
    fn tls_required(&self) -> bool {
        self.inner.tls_required
    }

    /// "If this is true, the server accepts TLS but does not require it": a
    /// server in mixed mode.
    #[getter]
    fn tls_available(&self) -> bool {
        self.inner.tls_available
    }

    /// "If this is true, the client must provide a valid certificate during
    /// the TLS handshake."
    #[getter]
    fn tls_verify(&self) -> bool {
        self.inner.tls_verify
    }

    /// "The nonce for use in CONNECT", where the server sent one.
    #[getter]
    fn nonce(&self) -> Option<&str> {
        self.inner.nonce.as_deref()
    }

    /// "List of server urls that a client can connect to." The material a
    /// reconnect policy needs; this client has no such policy, because
    /// redialling is the caller's decision.
    #[getter]
    fn connect_urls(&self) -> Vec<String> {
        self.inner.connect_urls.clone()
    }

    /// `ldm`: the server has entered Lame Duck Mode and will drain its
    /// clients.
    #[getter]
    fn lame_duck(&self) -> bool {
        self.inner.lame_duck
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nats.RemoteInfo server={:?} max_payload={} headers={}{}>",
            self.inner.server_name.as_deref().unwrap_or("?"),
            self.inner.max_payload,
            self.inner.headers,
            if self.inner.lame_duck {
                " lame_duck"
            } else {
                ""
            }
        )
    }
}

/// Where a connection is.
///
/// Three states and no fourth: NATS has no `CLOSE` verb, so ending a
/// connection *is* ending the transport, and a connection that failed keeps
/// the reason it failed for as long as the object lives.
#[pyclass(frozen, name = "State", module = "weida_nats")]
pub struct PyState {
    inner: State,
}

impl PyState {
    /// Wraps one state.
    pub fn of(state: State) -> PyState {
        PyState { inner: state }
    }
}

#[pymethods]
impl PyState {
    /// `"connected"`, `"closed"` or `"failed"`.
    #[getter]
    fn name(&self) -> &'static str {
        match self.inner {
            State::Connected => "connected",
            State::Closed => "closed",
            State::Failed(_) => "failed",
        }
    }

    /// Why the connection failed, where it did.
    #[getter]
    fn why(&self) -> Option<&str> {
        match &self.inner {
            State::Failed(why) => Some(why),
            State::Connected | State::Closed => None,
        }
    }

    /// Whether anything further can be sent.
    #[getter]
    fn is_usable(&self) -> bool {
        self.inner.is_usable()
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            State::Failed(why) => format!("<weida_nats.State failed: {why}>"),
            other => format!(
                "<weida_nats.State {}>",
                match other {
                    State::Connected => "connected",
                    _ => "closed",
                }
            ),
        }
    }
}
