//! PAIR, SURVEY and BUS on the asyncio surface, and RADIO/DISH.
//!
//! They are here rather than in [`crate::endpoints`] because they are a group
//! of their own, not because they are different in kind: whole payloads, an
//! explicit ceiling on every receive, and no pattern behaviour of their own —
//! the one-peer rule, the survey deadline and the never-yourself rule are
//! `weida`'s ([PATTERNS.md](../../../../docs/PATTERNS.md) §4, §5, §6).
//!
//! What a caller has to know per pattern, and nothing else:
//!
//! - **PAIR** keeps its **first** peer. A second dialler is refused on the
//!   transfer, not on the connection, so the refusal cannot be mistaken for a
//!   network failure — and the first peer keeps working, which is the opposite
//!   of ZeroMQ's PAIR.
//! - **SURVEY** is bounded by the **caller's** deadline. Nothing on the wire
//!   carries it and a respondent never learns it; silence is a number.
//! - **BUS** never delivers a member its own message, and forwards for nobody:
//!   a bus of *n* members is *n* × (*n* − 1) deliveries.
//! - **RADIO/DISH** never waits for a dish: `Radio.segment`, `Segment.write`
//!   and `Radio.datagram` are plain calls, and a dish that falls behind loses
//!   the old segment, never the new one
//!   ([0034](../../../../docs/decisions/0034-late-is-lost.md) §4.6).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use weida::{BusMember, Dish, Paired, Radio, Respondent, Runtime, Surveyor, TransferMeta};
use weida_py_core::{Bridge, payload_of};

use crate::cursors::{PyCursors, PyReporter, reporting_meta};
use crate::endpoints::{PyRequest, endpoint};
use crate::errors::{errno_of, to_py};
use crate::values::{PyConnectionStats, PyDishDrops, PyIncomingMeta, PySurvey};

/// Turns a Python float of seconds into a deadline, or says why not.
///
/// Mandatory and finite, for the reason `Runtime.drain` gives: a survey
/// without a deadline is a program that waits on a stranger.
fn deadline_of(py: Python<'_>, seconds: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(seconds).map_err(|e| {
        to_py(
            py,
            &errno_of(weida::Error::Runtime(format!("deadline: {e}"))),
        )
    })
}

endpoint!(
    PyPaired,
    Paired,
    "Paired",
    "`weida.Paired`: one peer, transfers in both directions, whole payloads."
);

#[pymethods]
impl PyPaired {
    /// The endpoint path this pair uses; empty on a dialling pair that has
    /// not connected yet.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Dials `url`, once. See [`crate::endpoints`] for the address forms.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` on a second call — a pair has one peer and no
    /// policy for choosing between two — `weida.Unsupported` on a bound pair,
    /// which has nothing to dial, plus the usual dialling failures.
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// One `weida.ConnectionStats` per live connection; empty on a bound
    /// pair, which dialled nothing. See `Requester.connection_stats`.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Peers connected: `0` or `1`.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `payload` to the peer.
    ///
    /// On a **bound** pair the first send waits for the peer to appear:
    /// nothing is buffered, and a bound pair with no peer has nobody to
    /// address rather than a backlog.
    ///
    /// `report` and `mode` order a cursor report exactly as `Pusher.send`
    /// does — a pair carries one-way transfers in each direction, so a
    /// verdict from the far end is a cursor here too — and the call then
    /// returns the `weida.Cursors` to read it on.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` when the peer's pair already belongs to somebody
    /// else, `weida.NotConnected`, `weida.ConnectionLost`, `weida.Protocol`
    /// for a value in `report` that is not a level.
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
            let cursors = transfer.cursors();
            transfer.write_all(&body).await.map_err(errno_of)?;
            transfer.finish().map_err(errno_of)?;
            Ok(cursors.map(|cursors| PyCursors::new(cursors, bridge, runtime)))
        })
    }

    /// Waits for the next transfer from the peer, at most `max_bytes`, and
    /// returns `(payload, meta)`.
    ///
    /// # Errors
    ///
    /// `weida.NotConnected` when the endpoint is gone,
    /// `weida.LimitExceeded` when the payload exceeds the ceiling.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta))
        })
    }

    /// `recv`, plus the reporter the peer ordered:
    /// `(payload, meta, reporter)`.
    ///
    /// As `Puller.recv_reporting`.
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
            let reporter = transfer
                .reporter()
                .map(|reporter| PyReporter::new(reporter, bridge, runtime));
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta, reporter))
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Paired {}>", self.endpoint.path())
    }
}

endpoint!(
    PySurveyor,
    Surveyor,
    "Surveyor",
    "`weida.Surveyor`: one question to every respondent, bounded by a deadline."
);

#[pymethods]
impl PySurveyor {
    /// Dials `url` and adds one respondent to the set a survey fans out over.
    ///
    /// Unlike a requester, a surveyor accumulates peers on purpose.
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

    /// Respondents currently connected.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Asks every connected respondent and returns a `weida.Survey` of what
    /// arrived within `deadline` seconds, each answer at most
    /// `max_reply_bytes`.
    ///
    /// A survey with no respondents is **not** an error: it is a `Survey` with
    /// nothing in it, because "nobody answered" is an answer. A respondent
    /// that refuses, dies or overruns the ceiling is counted in `failed`, and
    /// one that says nothing at all is `silent()` — neither ends the survey,
    /// because the respondents are independent exchanges.
    ///
    /// # Errors
    ///
    /// `weida.Runtime` for a deadline that is not a finite, non-negative
    /// number of seconds.
    fn survey<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        deadline: f64,
        max_reply_bytes: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let deadline = deadline_of(py, deadline)?;
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let mut run = endpoint
                .survey_with(
                    TransferMeta::default().with_content_len(body.len() as u64),
                    &body,
                    deadline,
                )
                .await
                .map_err(errno_of)?;
            let asked = run.respondents();
            let mut replies = Vec::new();
            let mut failed = 0usize;
            while let Some(answer) = run.next(max_reply_bytes).await {
                match answer {
                    Ok(reply) => replies.push(reply),
                    // A failure is a number and not an exception: one
                    // respondent's refusal is not the survey's outcome.
                    Err(_) => failed += 1,
                }
            }
            Ok(PySurvey {
                replies,
                asked,
                failed,
                late: run.late(),
            })
        })
    }

    fn __repr__(&self) -> String {
        "<weida.Surveyor>".to_owned()
    }
}

endpoint!(
    PyRespondent,
    Respondent,
    "Respondent",
    "`weida.Respondent`: accepts survey questions and answers them."
);

#[pymethods]
impl PyRespondent {
    /// The endpoint path this respondent serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next question and reads it, at most `max_bytes`,
    /// returning a `weida.Request`.
    ///
    /// The same class a replier hands out, because a question **is** an
    /// exchange: answer it with `reply`, decline it with `refuse`, and
    /// dropping it unanswered is the surveyor's `failed` rather than its
    /// deadline.
    ///
    /// # Errors
    ///
    /// As `Replier.accept`.
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
        format!("<weida.Respondent {}>", self.endpoint.path())
    }
}

endpoint!(
    PyBusMember,
    BusMember,
    "BusMember",
    "`weida.BusMember`: bound and dialling at once, every message to every other member."
);

#[pymethods]
impl PyBusMember {
    /// The path this member accepts on.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Joins the member at `url`.
    ///
    /// Joining is an ordinary connect and leaving an ordinary disconnect:
    /// there is no membership protocol and nothing to synchronize, which is
    /// what keeps a bus a fan-out rather than a group.
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

    /// Members this one has joined.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `payload` to every **other** member and returns how many it
    /// reached.
    ///
    /// Never the sender itself, structurally: a send writes to the members
    /// this one dialled, and a member does not dial itself. Best effort per
    /// member with counted drops, exactly as a fan-out, so this does not wait
    /// for a receipt — there is no single peer to get one from.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a payload above the per-member byte budget:
    /// such a message could never be enqueued for anybody, so reporting it
    /// beats dropping it for every member.
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            endpoint
                .send_with(
                    TransferMeta::default().with_content_len(body.len() as u64),
                    &body,
                )
                .await
                .map_err(errno_of)
        })
    }

    /// Waits for the next message from another member, at most `max_bytes`,
    /// and returns `(payload, meta)`.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta))
        })
    }

    /// Copies that never reached a member.
    fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    fn __repr__(&self) -> String {
        format!("<weida.BusMember {}>", self.endpoint.path())
    }
}

/// `max_age` in seconds, or none.
fn max_age_of(py: Python<'_>, seconds: Option<f64>) -> PyResult<Option<Duration>> {
    seconds.map(|s| deadline_of(py, s)).transpose()
}

/// What a dish receives, as the tuple Python sees:
/// `("segment", payload, meta)` or `("datagram", topic, segment, payload)`.
pub(crate) enum Heard {
    /// Boxed: the flattened metadata is several times a datagram's size.
    Segment(Vec<u8>, Box<PyIncomingMeta>),
    Datagram(String, u64, Vec<u8>),
}

impl<'py> IntoPyObject<'py> for Heard {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Heard::Segment(payload, meta) => {
                ("segment", PyBytes::new(py, &payload), *meta).into_bound_py_any(py)
            }
            Heard::Datagram(topic, segment, payload) => {
                ("datagram", topic, segment, PyBytes::new(py, &payload)).into_bound_py_any(py)
            }
        }
    }
}

/// `weida.Radio`: segments to every joined dish, dropped rather than waited
/// for.
///
/// Not written by `endpoint!`: nothing a radio does waits, so it holds no
/// bridge to the event loop.
#[pyclass(frozen, name = "Radio", module = "weida")]
pub struct PyRadio {
    endpoint: Radio,
    /// Held so the reactor outlives this endpoint.
    _runtime: Arc<Runtime>,
}

impl PyRadio {
    pub(crate) fn new(endpoint: Radio, runtime: Arc<Runtime>) -> PyRadio {
        PyRadio {
            endpoint,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PyRadio {
    /// The path this radio serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Opens the next segment on `topic` as one stream per joined dish and
    /// layer, and supersedes the previous segment's copies there: an
    /// unfinished copy at once, a finished one after the time its path
    /// needs. `max_age` in seconds is the sender's latency budget,
    /// `priority` orders topics within a layer, and `follows_upstream`
    /// marks a relay's segment whose upstream may still be streaming the
    /// previous one.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a topic above 256 bytes.
    #[pyo3(signature = (topic, max_age=None, priority=0, follows_upstream=false))]
    fn segment(
        &self,
        py: Python<'_>,
        topic: &str,
        max_age: Option<f64>,
        priority: i16,
        follows_upstream: bool,
    ) -> PyResult<PySegment> {
        let terms = segment_terms(max_age_of(py, max_age)?, priority, follows_upstream);
        let segment = self
            .endpoint
            .segment(topic, terms)
            .map_err(|e| to_py(py, &errno_of(e)))?;
        Ok(PySegment {
            segment: Mutex::new(Some(segment)),
        })
    }

    /// Sends `payload` as a one-packet segment on `topic`; returns how many
    /// dishes it was handed to. Never waits.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a topic above 256 bytes.
    fn datagram(&self, py: Python<'_>, topic: &str, payload: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(payload)?;
        self.endpoint
            .datagram(topic, body)
            .map_err(|e| to_py(py, &errno_of(e)))
    }

    /// Dishes currently joined.
    fn dish_count(&self) -> usize {
        self.endpoint.dish_count()
    }

    /// Copies dropped, over topics and causes.
    fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    /// What each joined dish connection lost, summed over topics: one
    /// `weida.DishDrops` per connection, gone when it closes.
    fn dish_drops(&self) -> Vec<PyDishDrops> {
        self.endpoint
            .dish_drops()
            .iter()
            .map(PyDishDrops::of)
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("<weida.Radio {}>", self.endpoint.path())
    }
}

/// `weida.Segment`: one segment, open on every dish joined when it opened.
#[pyclass(frozen, name = "Segment", module = "weida")]
pub struct PySegment {
    /// `None` after `finish`, which ends the segment once.
    segment: Mutex<Option<weida::Segment>>,
}

#[pymethods]
impl PySegment {
    /// Hands `chunk` to layer 0 of every copy still open; returns how many
    /// copies took it. A dish without room for it loses the segment,
    /// counted at the radio.
    fn write(&self, py: Python<'_>, chunk: &Bound<'_, PyAny>) -> PyResult<usize> {
        self.write_layer(py, 0, chunk)
    }

    /// Hands `chunk` to layer `layer` (`0..=15`) of every copy that wants
    /// it; returns how many copies took it. A copy without room cuts its
    /// layers from the top.
    fn write_layer(&self, py: Python<'_>, layer: u8, chunk: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(chunk)?;
        let mut guard = self.segment.lock().expect("segment lock poisoned");
        let Some(segment) = guard.as_mut() else {
            return Err(to_py(
                py,
                &errno_of(weida::Error::Runtime("the segment is finished".into())),
            ));
        };
        segment
            .write_layer(layer, body)
            .map_err(|e| to_py(py, &errno_of(e)))
    }

    /// Ends layer `layer` on every copy that has it open.
    fn finish_layer(&self, py: Python<'_>, layer: u8) -> PyResult<()> {
        let mut guard = self.segment.lock().expect("segment lock poisoned");
        let Some(segment) = guard.as_mut() else {
            return Err(to_py(
                py,
                &errno_of(weida::Error::Runtime("the segment is finished".into())),
            ));
        };
        segment.finish_layer(layer);
        Ok(())
    }

    /// Ends every layer still open; returns how many copies still hold
    /// layer 0.
    fn finish(&self) -> usize {
        self.segment
            .lock()
            .expect("segment lock poisoned")
            .take()
            .map_or(0, weida::Segment::finish)
    }
}

endpoint!(
    PyDish,
    Dish,
    "Dish",
    "`weida.Dish`: joins topics on a radio and receives the newest segment of each."
);

#[pymethods]
impl PyDish {
    /// Dials the radio at `url` and joins every topic joined so far.
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// One `weida.ConnectionStats` per live connection to a radio; see
    /// `Requester.connection_stats`.
    fn connection_stats(&self) -> Vec<PyConnectionStats> {
        PyConnectionStats::all(&self.endpoint.connection_stats())
    }

    /// Joins every topic `filter` matches; `max_age` in seconds is the
    /// latency budget after which the radio resets this dish's copy, and
    /// `max_layer` (`0..=15`) the highest layer the radio sends it.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a `max_layer` above 15.
    #[pyo3(signature = (filter, max_age=None, max_layer=None))]
    fn join<'py>(
        &self,
        py: Python<'py>,
        filter: String,
        max_age: Option<f64>,
        max_layer: Option<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let terms = join_terms(max_age_of(py, max_age)?, max_layer);
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            endpoint.join(&filter, terms).await.map_err(errno_of)
        })
    }

    /// Leaves a filter.
    fn leave<'py>(&self, py: Python<'py>, filter: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            endpoint.leave(&filter).await.map_err(errno_of)
        })
    }

    /// Waits for the next segment: `("segment", payload, meta)` for a stream
    /// segment read whole, at most `max_bytes`, or
    /// `("datagram", topic, segment, payload)` for a datagram segment.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            match endpoint.recv().await.map_err(errno_of)? {
                weida::Received::Segment(transfer) => {
                    let meta = PyIncomingMeta::of(transfer.meta());
                    let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
                    Ok(Heard::Segment(payload, Box::new(meta)))
                }
                weida::Received::Datagram {
                    topic,
                    segment,
                    payload,
                } => Ok(Heard::Datagram(
                    topic.to_string(),
                    segment,
                    payload.to_vec(),
                )),
            }
        })
    }

    /// Radios connected.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    fn __repr__(&self) -> String {
        "<weida.Dish>".to_owned()
    }
}

/// The join terms of a dish's latency budget and layer cap.
pub(crate) fn join_terms(
    max_age: Option<std::time::Duration>,
    max_layer: Option<u8>,
) -> weida::JoinTerms {
    let mut terms = weida::JoinTerms::default();
    if let Some(age) = max_age {
        terms = terms.with_max_age(age);
    }
    if let Some(layer) = max_layer {
        terms = terms.with_max_layer(layer);
    }
    terms
}

/// The terms a sender opens a segment under.
pub(crate) fn segment_terms(
    max_age: Option<std::time::Duration>,
    priority: i16,
    follows_upstream: bool,
) -> weida::SegmentTerms {
    let mut terms = weida::SegmentTerms::default()
        .with_priority(priority)
        .with_follows_upstream(follows_upstream);
    if let Some(age) = max_age {
        terms = terms.with_max_age(age);
    }
    terms
}
