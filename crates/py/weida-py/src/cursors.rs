//! The cursor surface on the asyncio side, and the level vocabulary both
//! surfaces share.
//!
//! A cursor is how a **one-way** transfer gets a verdict: a Push has no reply
//! to carry one, so `Accepted`, `Stored` and `Processed` arrive on a stream of
//! their own, after the payload's FIN
//! ([0023](../../../../docs/decisions/0023-completion-is-a-cursor.md)).
//! Two handles, one per direction:
//!
//! * [`PyCursors`] is the **sender's** end — what it ordered, as it arrives.
//! * [`PyReporter`] is the **receiver's** end — what this side has reached.
//!
//! # A level is an integer here, and that is the honest spelling
//!
//! The level space is **open**: values below `weida.APPLICATION_FLOOR` are
//! weida's own ladder and everything at or above it is an application stage
//! the library carries and never interprets (0023 §4.4). The named rungs are
//! module constants — `weida.ACCEPTED`, `weida.STORED`, `weida.REPLICATED`,
//! `weida.PROCESSED`, `weida.TRANSPORT_RECEIPT` — so a caller writes the name
//! and an application writes its own number. An **undefined** value below the
//! floor is a protocol violation rather than a private level, because the
//! reserved range is where a later version of the specification puts its own
//! stages: such a value is `weida.Protocol` here rather than a silent
//! reinterpretation.
//!
//! # Nothing waits on a cursor, and nothing here fails for a transport reason
//!
//! A report is never load-bearing. `Cursors.changed()` returning `None` means
//! "no more cursors are coming" and covers three cases on purpose
//! indistinguishable — the reporter finished, the stream was reset, the
//! connection went away — none of which is a failure. `Reporter.report()` is
//! best effort for the same reason seen from the writing side: a peer that
//! dropped the cursor stream must not fail the application doing the
//! reporting.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use weida::{CursorLevel, CursorSet, ReportMode, Reporter, Runtime};
use weida_py_core::{Bridge, Errno};

use crate::errors::to_py;
use crate::lock::locked;

/// Every named level, as `weida` exposes it.
///
/// `Acknowledgement::None` is not among them: "nothing is reported" is not a
/// level to order, and a caller that wants no report orders no levels.
pub(crate) const NAMED_LEVELS: [(&str, u64); 5] = [
    ("TRANSPORT_RECEIPT", 1),
    ("ACCEPTED", 2),
    ("STORED", 3),
    ("REPLICATED", 4),
    ("PROCESSED", 5),
];

/// The two report modes, as `weida` exposes them.
pub(crate) const MODES: [(&str, u64); 2] = [("PROGRESS", 0), ("FINAL_ONLY", 1)];

/// Turns a wire value into a level, or says why it is not one.
pub(crate) fn level_of(py: Python<'_>, value: u64) -> PyResult<CursorLevel> {
    CursorLevel::from_wire(value).ok_or_else(|| {
        to_py(
            py,
            &Errno::new(
                "Protocol",
                format!(
                    "{value} is not a cursor level: below weida.APPLICATION_FLOOR \
                     ({floor}) only the named levels exist, and the rest of that \
                     range is reserved for a later version of the protocol",
                    floor = CursorLevel::APPLICATION_FLOOR
                ),
            ),
        )
    })
}

/// Turns a list of wire values into levels.
pub(crate) fn levels_of(py: Python<'_>, values: &[u64]) -> PyResult<Vec<CursorLevel>> {
    values.iter().map(|value| level_of(py, *value)).collect()
}

/// Turns a wire value into a report mode, or says why it is not one.
pub(crate) fn mode_of(py: Python<'_>, value: u64) -> PyResult<ReportMode> {
    ReportMode::from_wire(value).ok_or_else(|| {
        to_py(
            py,
            &Errno::new(
                "Protocol",
                format!("{value} is not a report mode: weida.PROGRESS or weida.FINAL_ONLY"),
            ),
        )
    })
}

/// A set of cursors as Python sees it: level to offset, ascending.
///
/// A dict rather than a class, because a `CursorSet` is exactly that — the
/// latest absolute offset per level — and a class would add a vocabulary
/// without adding a fact.
pub(crate) fn set_of(set: CursorSet) -> BTreeMap<u64, u64> {
    set.iter()
        .map(|(level, offset)| (level.to_wire(), offset))
        .collect()
}

/// Builds the metadata a reporting send needs.
///
/// The report order lives in the DATA header, so it is set here rather than
/// passed as a separate argument to the library: what a receiver sees is what
/// the sender wrote, which is what makes a report the sender's request rather
/// than a convention.
pub(crate) fn reporting_meta(
    py: Python<'_>,
    content_len: Option<u64>,
    report: Option<Vec<u64>>,
    mode: u64,
) -> PyResult<weida::TransferMeta> {
    let mut meta = weida::TransferMeta::default();
    if let Some(len) = content_len {
        meta = meta.with_content_len(len);
    }
    let Some(levels) = report else {
        return Ok(meta);
    };
    // A mode without levels orders nothing, so it is not worth validating an
    // argument that cannot reach the wire.
    Ok(meta
        .with_report(levels_of(py, &levels)?)
        .with_report_mode(mode_of(py, mode)?))
}

/// `weida.Cursors`: the sender's end of one transfer's report.
///
/// Independent of the transfer that ordered it, deliberately: the terminal
/// cursor arrives **after** the payload's FIN, so a handle tied to the send
/// would be gone exactly when the interesting record lands.
#[pyclass(frozen, name = "Cursors", module = "weida")]
pub struct PyCursors {
    /// `&mut` on the Rust side, so a `frozen` class two coroutines may hold
    /// needs the lock: a `tokio` one, because the guard is held across the
    /// await in `changed`.
    cursors: Arc<tokio::sync::Mutex<weida::Cursors>>,
    /// Read without waiting, so it needs no async lock — and must not take
    /// the one above, which a parked `changed` is holding.
    snapshot: Arc<Mutex<CursorSet>>,
    bridge: Bridge,
    /// Held so the reactor outlives this handle.
    _runtime: Arc<Runtime>,
}

impl PyCursors {
    pub(crate) fn new(cursors: weida::Cursors, bridge: Bridge, runtime: Arc<Runtime>) -> PyCursors {
        let snapshot = cursors.snapshot();
        PyCursors {
            cursors: Arc::new(tokio::sync::Mutex::new(cursors)),
            snapshot: Arc::new(Mutex::new(snapshot)),
            bridge,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PyCursors {
    /// The latest set as `{level: offset}`, without waiting.
    fn snapshot(&self) -> BTreeMap<u64, u64> {
        set_of(*locked(&self.snapshot))
    }

    /// The latest offset for `level`, or `None` if it was never reported.
    ///
    /// # Errors
    ///
    /// `weida.Protocol` for a value that is not a level.
    fn offset(&self, py: Python<'_>, level: u64) -> PyResult<Option<u64>> {
        let level = level_of(py, level)?;
        Ok(locked(&self.snapshot).offset(level))
    }

    /// Waits for the next change and returns the new set, or `None` once no
    /// further cursors are coming.
    ///
    /// The loop a caller writes is
    /// `while (set := await cursors.changed()) is not None:` — there is no
    /// `async for` here, and the reason is the one `weida.Survey` gives: an
    /// end of iteration is not a failure, and the only channel out of a
    /// bridged future is the exception family.
    fn changed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursors = Arc::clone(&self.cursors);
        let snapshot = Arc::clone(&self.snapshot);
        self.bridge.awaitable(py, async move {
            let changed = cursors.lock().await.changed().await;
            match changed {
                Some(set) => {
                    *locked(&snapshot) = set;
                    Ok(Some(set_of(set)))
                }
                None => Ok(None),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida.Cursors {} level(s) reported>",
            locked(&self.snapshot).len()
        )
    }
}

/// `weida.Reporter`: the receiver's end, for a transfer whose sender ordered
/// a report.
///
/// Every write is best effort: a level the sender did not order is ignored
/// rather than refused, and a broken cursor stream leaves the application
/// unaffected.
#[pyclass(frozen, name = "Reporter", module = "weida")]
pub struct PyReporter {
    /// `None` after `finish`, which consumes the reporter: one report ends
    /// once, and a `frozen` class two coroutines may hold needs that to be
    /// true rather than documented.
    reporter: Arc<tokio::sync::Mutex<Option<Reporter>>>,
    levels: Vec<u64>,
    mode: u64,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

impl PyReporter {
    pub(crate) fn new(reporter: Reporter, bridge: Bridge, runtime: Arc<Runtime>) -> PyReporter {
        let levels = reporter
            .levels()
            .iter()
            .map(|level| level.to_wire())
            .collect();
        let mode = reporter.mode().to_wire();
        PyReporter {
            reporter: Arc::new(tokio::sync::Mutex::new(Some(reporter))),
            levels,
            mode,
            bridge,
            _runtime: runtime,
        }
    }

    /// What a call on a finished reporter gets.
    fn spent() -> Errno {
        Errno::new(
            "NoReply",
            "this report was already finished; a report ends once",
        )
    }
}

#[pymethods]
impl PyReporter {
    /// The levels the sender ordered, ascending.
    #[getter]
    fn levels(&self) -> Vec<u64> {
        self.levels.clone()
    }

    /// The mode the sender asked for: `weida.PROGRESS` or
    /// `weida.FINAL_ONLY`.
    #[getter]
    fn mode(&self) -> u64 {
        self.mode
    }

    /// Reports that `level` has reached `offset`.
    ///
    /// # Errors
    ///
    /// `weida.Protocol` for a value that is not a level, `weida.NoReply`
    /// after `finish`.
    fn report<'py>(&self, py: Python<'py>, level: u64, offset: u64) -> PyResult<Bound<'py, PyAny>> {
        let level = level_of(py, level)?;
        let reporter = Arc::clone(&self.reporter);
        self.bridge.awaitable(py, async move {
            // The guard is taken inside the future, so two coroutines
            // reporting on one handle serialize rather than race.
            let mut guard = reporter.lock().await;
            match guard.as_mut() {
                Some(reporter) => reporter
                    .report(level, offset)
                    .await
                    .map_err(crate::errors::errno_of),
                None => Err(PyReporter::spent()),
            }
        })
    }

    /// Flushes the latest offset per level and ends the cursor stream.
    ///
    /// The flush is what makes coalescing lossless: whatever the granularity
    /// suppressed, the last number each level reached is on the wire before
    /// the end.
    ///
    /// # Errors
    ///
    /// `weida.NoReply` on a second call.
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let reporter = Arc::clone(&self.reporter);
        self.bridge.awaitable(py, async move {
            let taken = reporter.lock().await.take();
            match taken {
                Some(reporter) => reporter.finish().await.map_err(crate::errors::errno_of),
                None => Err(PyReporter::spent()),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida.Reporter levels={:?} mode={}>",
            self.levels, self.mode
        )
    }
}
