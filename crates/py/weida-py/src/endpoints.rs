//! The four endpoints of slice 1: Req/Rep and Push/Pull.
//!
//! Whole payloads in both directions, because that is what a Python object
//! is. The streaming surface — a payload written or read in pieces — stays on
//! the Rust API and is the second slice; what makes the difference visible
//! here is that **every receive takes a ceiling in bytes**, passed straight
//! to `IncomingTransfer::read_capped`, so a stranger cannot decide how much
//! memory a Python process allocates.
//!
//! Each object holds the runtime it came from, so a program that keeps a
//! `Replier` and drops the `Runtime` object is a program whose requests must
//! keep arriving: the reactor dies with the last handle, not with the first.
//!
//! No pattern behaviour is here. REQ's one-exchange-at-a-time, the dispatch
//! refusals, the receipt's meaning and every limit are `weida`'s
//! ([PATTERNS.md](../../../../docs/PATTERNS.md) §1, §2).

use std::sync::Arc;

use pyo3::prelude::*;
use weida::{Puller, Pusher, Replier, Requester, Runtime, TransferMeta};
use weida_py_core::{Bridge, payload_of, py_bytes};

use crate::cursors::{PyCursors, PyReporter, reporting_meta};
use crate::errors::errno_of;
use crate::streams::{PyIncomingStream, PyOutgoingStream, PyReply};
use crate::values::{PyConnectionStats, PyIncomingMeta};

/// Writes the wrapper, its constructor and the `connect` a dialling endpoint
/// has.
macro_rules! endpoint {
    ($name:ident, $inner:ty, $python:literal, $what:literal) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida")]
        pub struct $name {
            endpoint: Arc<$inner>,
            bridge: Bridge,
            /// Held so the reactor outlives this endpoint.
            _runtime: Arc<Runtime>,
        }

        impl $name {
            pub(crate) fn new(endpoint: $inner, bridge: Bridge, runtime: Arc<Runtime>) -> $name {
                $name {
                    endpoint: Arc::new(endpoint),
                    bridge,
                    _runtime: runtime,
                }
            }
        }
    };
}
pub(crate) use endpoint;

/// The addresses the two dialling endpoints accept, written once because
/// their `connect` methods cannot share an implementation: PyO3 allows one
/// `#[pymethods]` block per type and no macro items inside it, so the two
/// bodies are two lines each and this comment is the single source of the
/// rule they follow.
///
/// `weida://[sha256:…@]host:port/path` over QUIC, or `weida+unix://…`,
/// `weida+pipe://…` and `weida+inproc://…` locally — where the kernel proves
/// the peer instead of TLS, so no fingerprint belongs in the address
/// ([0010](../../../../docs/decisions/0010-local-transport.md) §4.8).
///
/// Failures: `weida.Untrusted` when the key that answered is not trusted,
/// carrying what it presented so an operator can pin it, plus
/// `weida.InvalidAddress`, `weida.Io`, `weida.Negotiation` and
/// `weida.ConnectionLost`.
const _ADDRESS_RULES: () = ();

endpoint!(
    PyRequester,
    Requester,
    "Requester",
    "`weida.Requester`: one exchange at a time, whole payloads both ways."
);
#[pymethods]
impl PyRequester {
    /// Dials `url` and returns when a peer exists. See [`_ADDRESS_RULES`].
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// One `weida.ConnectionStats` per live connection, labelled by the URL
    /// as dialled; empty when none is live. Synchronous and cheap.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Sends `payload` and returns the reply, at most `max_reply_bytes`.
    ///
    /// There is no timeout argument: a caller that wants one wraps this in
    /// `asyncio.wait_for`, which cancels the coroutine — and a cancelled
    /// exchange resets its streams, so the peer learns rather than waits.
    ///
    /// # Errors
    ///
    /// `weida.UnknownEndpoint` when nothing is registered at the path,
    /// `weida.Rejected` when the peer declined, `weida.NoReply` when it
    /// accepted and will not answer, `weida.Indeterminate` when the
    /// connection died with the outcome unknown — which is **not** a definite
    /// failure and must not be retried as one — and `weida.LimitExceeded`
    /// when the reply exceeds the ceiling.
    fn request<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        max_reply_bytes: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let reply = endpoint
                .request_with(
                    TransferMeta::default().with_content_len(body.len() as u64),
                    &body,
                )
                .await
                .map_err(errno_of)?;
            reply.collect(max_reply_bytes).await.map_err(errno_of)
        })
    }

    /// Opens a streamed exchange and returns `(request, reply)`.
    ///
    /// The two are independent streams, so a caller may read the reply while
    /// still writing the request — which is the point of the architecture and
    /// not an optimisation: a responder that answers while receiving has flow
    /// control live in both directions.
    fn open<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let (transfer, reply) = endpoint
                .open(TransferMeta::default())
                .await
                .map_err(errno_of)?;
            Ok((
                PyOutgoingStream::new(transfer, bridge.clone(), Arc::clone(&runtime)),
                PyReply::new(reply, bridge, runtime),
            ))
        })
    }

    fn __repr__(&self) -> String {
        "<weida.Requester>".to_owned()
    }
}

endpoint!(
    PyPusher,
    Pusher,
    "Pusher",
    "`weida.Pusher`: one-way transfers, round-robin over the peers it reached."
);
#[pymethods]
impl PyPusher {
    /// Dials `url` and returns when a peer exists. See [`_ADDRESS_RULES`].
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// One `weida.ConnectionStats` per live connection; see
    /// `Requester.connection_stats`.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Sends `payload` and waits for the peer's **transport** to acknowledge
    /// it.
    ///
    /// The receipt is QUIC's fin-acknowledgement and says nothing about the
    /// peer's application having read the bytes
    /// ([GUARANTEES.md](../../../../docs/GUARANTEES.md) §1). A caller that does
    /// not want to wait for it at all wants Req/Rep, where the reply is the
    /// proof, or `asyncio.create_task`.
    /// `report` orders a cursor report on the levels it names — see
    /// `weida.Cursors` — and the call then returns the handle to read it on;
    /// `None` when nothing was ordered, which is every ordinary send.
    /// `mode` is `weida.PROGRESS` or `weida.FINAL_ONLY`.
    ///
    /// # Errors
    ///
    /// `weida.ConnectionLost` before the FIN, `weida.Indeterminate` after it,
    /// `weida.Rejected` when the peer refused the payload,
    /// `weida.UnknownEndpoint`, `weida.LimitExceeded`, `weida.Protocol` for a
    /// value in `report` that is not a level.
    #[pyo3(signature = (payload, report=None, mode=0))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        report: Option<Vec<u64>>,
        mode: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let meta = reporting_meta(py, Some(body.len() as u64), report, mode)?;
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let mut transfer = endpoint.open(meta).await.map_err(errno_of)?;
            // Taken before the payload: `finish` consumes the transfer and
            // the report outlives it by design.
            let cursors = transfer.cursors();
            transfer.write_all(&body).await.map_err(errno_of)?;
            transfer
                .finish()
                .map_err(errno_of)?
                .delivered()
                .await
                .map_err(errno_of)?;
            Ok(cursors.map(|cursors| PyCursors::new(cursors, bridge, runtime)))
        })
    }

    /// Opens a streamed transfer, for a payload that does not fit memory.
    ///
    /// The stream's `finish` is what waits for the receipt; `send` is the
    /// whole-payload form of the same thing.
    #[pyo3(signature = (report=None, mode=0))]
    fn open<'py>(
        &self,
        py: Python<'py>,
        report: Option<Vec<u64>>,
        mode: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let meta = reporting_meta(py, None, report, mode)?;
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.open(meta).await.map_err(errno_of)?;
            Ok(PyOutgoingStream::new(transfer, bridge, runtime))
        })
    }

    fn __repr__(&self) -> String {
        "<weida.Pusher>".to_owned()
    }
}

endpoint!(
    PyReplier,
    Replier,
    "Replier",
    "`weida.Replier`: accepts exchanges and answers them."
);

#[pymethods]
impl PyReplier {
    /// The endpoint path this replier serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next request and reads its body, at most `max_bytes`.
    ///
    /// Backpressure is the transport's: requests queue up to
    /// `Limits::endpoint_queue` and a full queue stalls the requester through
    /// QUIC flow control rather than growing here.
    ///
    /// # Errors
    ///
    /// `weida.NotConnected` when the binding is gone,
    /// `weida.LimitExceeded` when the request body exceeds the ceiling.
    fn accept<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let request = endpoint.accept().await.map_err(errno_of)?;
            PyRequest::accepted(request, max_bytes, bridge, runtime).await
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Replier {}>", self.endpoint.path())
    }
}

/// `weida.Request`: an accepted exchange whose body is read, and the reply it
/// owes.
///
/// Dropping it without `reply` or `refuse` causes ERROR `{NO_REPLY}` on the
/// reply half, so a requester learns instead of waiting out an idle timeout —
/// the rule [PROTOCOL.md](../../../../docs/PROTOCOL.md) §9.4 makes mandatory.
#[pyclass(frozen, name = "Request", module = "weida")]
pub struct PyRequest {
    /// `None` once answered: a request is answered at most once, and the
    /// `Mutex` is what makes that true for a `frozen` class two coroutines
    /// may hold.
    request: std::sync::Mutex<Option<weida::IncomingRequest>>,
    payload: Vec<u8>,
    meta: PyIncomingMeta,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

#[pymethods]
impl PyRequest {
    /// The request payload.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        py_bytes(py, &self.payload)
    }

    /// What the DATA header said, including the proved peer.
    #[getter]
    fn meta(&self, py: Python<'_>) -> PyResult<Py<PyIncomingMeta>> {
        Py::new(py, self.meta.clone())
    }

    /// Answers with `payload`.
    ///
    /// # Errors
    ///
    /// `weida.Canceled` when the requester walked away — which a handler can
    /// see coming, on the Rust side, through `IncomingRequest::canceled` —
    /// `weida.ConnectionLost`, and `weida.NoReply` if this request was
    /// already answered.
    fn reply<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let request = self.take()?;
        self.bridge.awaitable(py, async move {
            let mut out = request
                .reply(TransferMeta::default().with_content_len(body.len() as u64))
                .await
                .map_err(errno_of)?;
            out.write_all(&body).await.map_err(errno_of)?;
            out.finish().map_err(errno_of)?;
            Ok(())
        })
    }

    /// Declines the request, so the requester gets `weida.Rejected` rather
    /// than silence.
    ///
    /// # Errors
    ///
    /// `weida.NoReply` if this request was already answered.
    fn refuse<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let request = self.take()?;
        self.bridge.awaitable(py, async move {
            request.refuse(weida::ErrorCode::Rejected).await;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Request {} bytes>", self.payload.len())
    }
}

impl PyRequest {
    /// One accepted exchange with its body read, at most `max_bytes`.
    ///
    /// Built here and nowhere else, so that a replier and a **respondent**
    /// hand back the same object: a survey question is an exchange, byte for
    /// byte, and inventing a second request class for it would make a Python
    /// caller learn two shapes for one thing.
    pub(crate) async fn accepted(
        mut request: weida::IncomingRequest,
        max_bytes: usize,
        bridge: Bridge,
        runtime: Arc<Runtime>,
    ) -> Result<PyRequest, weida_py_core::Errno> {
        let meta = PyIncomingMeta::of(request.meta());
        let payload = request
            .take_body()
            .collect(max_bytes)
            .await
            .map_err(errno_of)?;
        Ok(PyRequest {
            request: std::sync::Mutex::new(Some(request)),
            payload,
            meta,
            bridge,
            _runtime: runtime,
        })
    }

    /// Takes the request for the one answer it has, or reports that it is
    /// spent.
    fn take(&self) -> PyResult<weida::IncomingRequest> {
        let taken = self.request.lock().expect("request lock poisoned").take();
        match taken {
            Some(request) => Ok(request),
            None => Python::attach(|py| {
                Err(crate::errors::to_py(
                    py,
                    &weida_py_core::Errno::new(
                        "NoReply",
                        "this request was already answered; an exchange carries one reply",
                    ),
                ))
            }),
        }
    }
}

endpoint!(
    PyPuller,
    Puller,
    "Puller",
    "`weida.Puller`: receives one-way transfers from every pusher that reached it."
);

#[pymethods]
impl PyPuller {
    /// The endpoint path this puller serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next transfer and reads it, at most `max_bytes`,
    /// returning `(payload, meta)`.
    ///
    /// # Errors
    ///
    /// As `Replier.accept`.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta))
        })
    }

    /// `recv`, plus the reporter the sender ordered:
    /// `(payload, meta, reporter)`.
    ///
    /// The third element is `None` unless the sender ordered a report, which
    /// is the ordinary case. **This is the call a staged receiver wants**:
    /// `recv` forgets the transfer once the payload is in hand, and a
    /// receiver that wants to say "accepted", "stored", "processed" as it
    /// gets there needs the handle that outlives the payload
    /// ([0023](../../../../docs/decisions/0023-completion-is-a-cursor.md)).
    ///
    /// # Errors
    ///
    /// As `recv`.
    fn recv_reporting<'py>(
        &self,
        py: Python<'py>,
        max_bytes: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            // Taken before the payload: `collect` consumes the transfer.
            let reporter = transfer
                .reporter()
                .map(|reporter| PyReporter::new(reporter, bridge, runtime));
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta, reporter))
        })
    }

    /// Waits for the next transfer and returns it as a stream:
    /// `(stream, meta)`, for a payload too large to hold.
    fn recv_stream<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            Ok((PyIncomingStream::new(transfer, bridge, runtime), meta))
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Puller {}>", self.endpoint.path())
    }
}
