//! The streamed transfer surface: a payload written or read in pieces.
//!
//! Slice 1's calls take a whole payload and a ceiling, which is what a Python
//! object is. This module is the other half, and it exists for the case the
//! ceiling cannot answer: a payload larger than the memory the process has,
//! which is the reason [INVARIANTS.md](../../../../docs/INVARIANTS.md) forbids
//! the core from materializing one.
//!
//! * `await pusher.open()` → [`PyOutgoingStream`]: `write`, then `finish`,
//!   which returns when the peer's **transport** holds every byte.
//! * `await requester.open()` → `(stream, reply)`: the two halves of one
//!   exchange, which are independent streams — so a caller may read the reply
//!   while still writing the request, and a responder may answer before the
//!   request ends. That is not an optimisation: a responder that answers
//!   while receiving has flow control live in both directions, so a client
//!   that wrote everything before reading would stall the responder and
//!   therefore itself.
//! * `await puller.recv_stream()` / `await subscriber.recv_stream()` →
//!   `(stream, meta)`: `read(max)` yields the next piece and `b""` at the
//!   end, so a loop over it is the ordinary shape.
//!
//! Every read still takes a ceiling, because a *piece* is a Python object
//! too. What changes is that the ceiling bounds one piece rather than the
//! payload.
//!
//! Dropping an unfinished outgoing stream resets it, so the peer discards the
//! partial payload instead of waiting for a FIN — the same rule the Rust
//! `OutgoingTransfer` has.

use std::sync::Arc;

use pyo3::prelude::*;
// `AsyncReadExt` for `read`: an `IncomingTransfer` is an `AsyncRead`, which is
// what lets a caller read one piece at a time rather than the whole payload.
use tokio::io::AsyncReadExt as _;
use weida::{IncomingTransfer, OutgoingTransfer, ReplyStream, Runtime};
use weida_py_core::{Bridge, Errno, payload_of};

use crate::cursors::{PyCursors, PyReporter};
use crate::errors::errno_of;
use crate::values::PyIncomingMeta;

/// The most one [`PyIncomingStream::read`] hands back, whatever ceiling it is
/// given.
///
/// The ceiling is a Python argument, and a buffer the size of it is allocated
/// and zeroed before a byte is known to be there: `read(16 << 20)` on a stream
/// with one packet waiting would zero 16 MiB for 1.4 KB, and `read(2**40)`
/// would abort the interpreter in the allocator instead of raising. A read may
/// legally return fewer bytes than asked for, so the buffer is the size of a
/// piece the transport hands over at a time, not of the caller's hope.
const READ_PIECE_BYTES: usize = 64 * 1024;

/// What a call on a spent stream gets, rather than a panic.
fn spent(what: &str) -> Errno {
    Errno::new(
        "NoReply",
        format!("this {what} was already finished; a transfer ends once"),
    )
}

/// `weida.OutgoingStream`: a payload written in pieces.
#[pyclass(frozen, name = "OutgoingStream", module = "weida")]
pub struct PyOutgoingStream {
    /// `None` once finished. A `tokio::sync::Mutex` because the guard is held
    /// across an await.
    transfer: Arc<tokio::sync::Mutex<Option<OutgoingTransfer>>>,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

impl PyOutgoingStream {
    pub(crate) fn new(
        transfer: OutgoingTransfer,
        bridge: Bridge,
        runtime: Arc<Runtime>,
    ) -> PyOutgoingStream {
        PyOutgoingStream {
            transfer: Arc::new(tokio::sync::Mutex::new(Some(transfer))),
            bridge,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PyOutgoingStream {
    /// Writes the next piece, waiting for the peer's flow-control window
    /// where it has to.
    ///
    /// # Errors
    ///
    /// `weida.Rejected` when the peer refuses the payload mid-write,
    /// `weida.UnknownEndpoint`, `weida.Unsupported`, `weida.Canceled`,
    /// `weida.ConnectionLost`.
    fn write<'py>(
        &self,
        py: Python<'py>,
        chunk: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(chunk)?;
        let transfer = Arc::clone(&self.transfer);
        self.bridge.awaitable(py, async move {
            let mut guard = transfer.lock().await;
            let transfer = guard.as_mut().ok_or_else(|| spent("stream"))?;
            transfer.write_all(&body).await.map_err(errno_of)
        })
    }

    /// Marks the end of the payload and waits for the peer's transport to
    /// acknowledge every byte and the FIN.
    ///
    /// The receipt is QUIC's fin-acknowledgement and **not** an application
    /// acknowledgement: it says the bytes are in the peer's transport, not
    /// that its application read them
    /// ([GUARANTEES.md](../../../../docs/GUARANTEES.md) §1).
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transfer = Arc::clone(&self.transfer);
        self.bridge.awaitable(py, async move {
            let taken = transfer
                .lock()
                .await
                .take()
                .ok_or_else(|| spent("stream"))?;
            taken
                .finish()
                .map_err(errno_of)?
                .delivered()
                .await
                .map_err(errno_of)
        })
    }

    /// The report this transfer ordered, once.
    ///
    /// `None` when nothing was ordered, and `None` on every call after the
    /// first: there is one report, so there is one reader. The handle is
    /// independent of the stream and stays usable after `finish`, which is
    /// the point — a verdict such as `Accepted` arrives *after* the FIN
    /// ([0023](../../../../docs/decisions/0023-completion-is-a-cursor.md)).
    fn cursors<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transfer = Arc::clone(&self.transfer);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let mut guard = transfer.lock().await;
            let transfer = guard.as_mut().ok_or_else(|| spent("stream"))?;
            Ok(transfer
                .cursors()
                .map(|cursors| PyCursors::new(cursors, bridge, runtime)))
        })
    }

    /// Abandons the transfer, resetting the stream so the peer discards what
    /// arrived rather than waiting for a FIN.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transfer = Arc::clone(&self.transfer);
        self.bridge.awaitable(py, async move {
            let taken = transfer
                .lock()
                .await
                .take()
                .ok_or_else(|| spent("stream"))?;
            taken.cancel();
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        "<weida.OutgoingStream>".to_owned()
    }
}

/// `weida.IncomingStream`: a payload read in pieces.
#[pyclass(frozen, name = "IncomingStream", module = "weida")]
pub struct PyIncomingStream {
    transfer: Arc<tokio::sync::Mutex<Option<IncomingTransfer>>>,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

impl PyIncomingStream {
    pub(crate) fn new(
        transfer: IncomingTransfer,
        bridge: Bridge,
        runtime: Arc<Runtime>,
    ) -> PyIncomingStream {
        PyIncomingStream {
            transfer: Arc::new(tokio::sync::Mutex::new(Some(transfer))),
            bridge,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PyIncomingStream {
    /// Reads up to `max_bytes` of the payload, returning `b""` at the end.
    ///
    /// The ceiling bounds one piece, so a loop over `read` holds one piece at
    /// a time and the payload has no size limit at all. A piece may be
    /// shorter than `max_bytes`, as a read of any stream may, and is never
    /// longer than [`READ_PIECE_BYTES`]: `read(2**40)` is a read of one piece,
    /// not an allocation of a terabyte.
    ///
    /// # Errors
    ///
    /// `weida.Canceled` when the sender reset the stream,
    /// `weida.ConnectionLost`.
    fn read<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let transfer = Arc::clone(&self.transfer);
        self.bridge.awaitable(py, async move {
            let mut guard = transfer.lock().await;
            let Some(stream) = guard.as_mut() else {
                return Ok(Vec::new());
            };
            let mut buf = vec![0u8; max_bytes.clamp(1, READ_PIECE_BYTES)];
            // `AsyncRead`'s own `read`, whose error is an `io::Error`: the
            // stream is an `AsyncRead` precisely so that a streaming caller
            // does not go through `read_capped`, and the two error types are
            // the price of that. The library maps the transport's outcomes
            // into `io::Error` on the way out, so nothing is lost — a reset
            // arrives as an `io::Error` whose text names the stop code.
            let read = stream
                .read(&mut buf)
                .await
                .map_err(|e| errno_of(weida::Error::Io(e)))?;
            buf.truncate(read);
            if read == 0 {
                // Ended: drop the stream so the FIN is acknowledged and a
                // second read answers without touching the transport.
                *guard = None;
            }
            Ok(buf)
        })
    }

    /// Reads the rest of the payload, at most `max_bytes` — the whole-payload
    /// call, for a stream a caller decided is small after all.
    fn collect<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let transfer = Arc::clone(&self.transfer);
        self.bridge.awaitable(py, async move {
            let taken = transfer
                .lock()
                .await
                .take()
                .ok_or_else(|| spent("stream"))?;
            taken.collect(max_bytes).await.map_err(errno_of)
        })
    }

    /// The reporter this transfer's sender ordered, if it ordered one.
    ///
    /// `None` when nothing was ordered. Handed out as often as asked — unlike
    /// the sender's `cursors`, a reporter is a writer and the library's own
    /// accessor takes `&self` — but a caller wants one, because `finish`
    /// ends the report.
    fn reporter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transfer = Arc::clone(&self.transfer);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let guard = transfer.lock().await;
            let transfer = guard.as_ref().ok_or_else(|| spent("stream"))?;
            Ok(transfer
                .reporter()
                .map(|reporter| PyReporter::new(reporter, bridge, runtime)))
        })
    }

    fn __repr__(&self) -> String {
        "<weida.IncomingStream>".to_owned()
    }
}

/// `weida.Reply`: the reply half of an exchange a caller opened.
///
/// Separate from the request half on purpose: the two are independent
/// streams, so a requester may read the reply while still writing the
/// request. Dropping this without `recv` stops the reply half with
/// `CANCELED`, which the responder observes.
#[pyclass(frozen, name = "Reply", module = "weida")]
pub struct PyReply {
    reply: Arc<tokio::sync::Mutex<Option<ReplyStream>>>,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

impl PyReply {
    pub(crate) fn new(reply: ReplyStream, bridge: Bridge, runtime: Arc<Runtime>) -> PyReply {
        PyReply {
            reply: Arc::new(tokio::sync::Mutex::new(Some(reply))),
            bridge,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PyReply {
    /// Waits for the responder's reply and returns `(stream, meta)`.
    ///
    /// # Errors
    ///
    /// `weida.Rejected`, `weida.UnknownEndpoint`, `weida.Unsupported` and
    /// `weida.NoReply` from an ERROR frame on this half;
    /// `weida.Indeterminate` when the connection was lost — never
    /// `ConnectionLost`, because after the request's FIN the outcome is
    /// genuinely unknown ([FAILURE_MODEL.md](../../../../docs/FAILURE_MODEL.md)).
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let reply = Arc::clone(&self.reply);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let taken = reply.lock().await.take().ok_or_else(|| spent("reply"))?;
            let transfer = taken.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            Ok((PyIncomingStream::new(transfer, bridge, runtime), meta))
        })
    }

    fn __repr__(&self) -> String {
        "<weida.Reply>".to_owned()
    }
}
