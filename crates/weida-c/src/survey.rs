//! The two halves of SURVEY: surveys, their answers and respondents (docs/c/survey.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use ::weida_runtime::Exec;
use weida_rs::{Error, Respondent, SurveyRun, Surveyor, TransferMeta};

use crate::config::ms;
use crate::error::{WEIDA_EINVAL, fail, fail_abi, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::{Accepting, weida_incoming_request, weida_listener};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::stats::weida_connection_stats;
use crate::tls::{weida_client_tls, weida_transfer_meta};
use crate::try_handle::Tried;

/// The asking half of SURVEY: dials respondents and asks every one of them.
pub struct weida_surveyor {
    surveyor: Arc<Surveyor>,
    exec: Exec,
}

/// The pending dial of `weida_surveyor_connect`.
pub struct weida_surveyor_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_surveyor_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending ask of `weida_surveyor_survey` and
/// `weida_surveyor_survey_with`.
pub struct weida_surveyor_survey_op {
    op: Arc<Op<SurveyRun>>,
}

impl AsOp for weida_surveyor_survey_op {
    type Output = SurveyRun;
    fn op(&self) -> &Op<SurveyRun> {
        &self.op
    }
}

/// A read of the next answer in flight; it owns the run until it ends.
type Answering = Pin<Box<dyn Future<Output = (SurveyRun, Option<Result<Vec<u8>, Error>>)> + Send>>;

/// A run between tries: the run itself, or the read in flight that owns it
/// (R3a).
struct SurveyRunState {
    run: Option<SurveyRun>,
    answering: Option<Answering>,
    /// The late count as of the last read that ended.
    late: u64,
}

/// One survey in progress (`SurveyRun`): its answers, read as a try.
pub struct weida_survey_run {
    tried: Tried<SurveyRunState>,
    respondents: usize,
}

impl weida_survey_run {
    fn handle(run: SurveyRun) -> *mut weida_survey_run {
        into_handle(weida_survey_run {
            respondents: run.respondents(),
            tried: Tried::new(SurveyRunState {
                late: run.late(),
                run: Some(run),
                answering: None,
            }),
        })
    }
}

/// One answer's bytes (the `Vec<u8>` of `SurveyRun::next`).
pub struct weida_survey_answer {
    data: Vec<u8>,
}

/// A respondent's state between tries: the accept in flight is kept, never
/// recreated, so a timeout or `EINTR` loses nothing (R3a).
struct RespondentState {
    respondent: Arc<Respondent>,
    accepting: Option<Accepting>,
}

/// The answering half of SURVEY, serving one endpoint path.
pub struct weida_respondent {
    tried: Tried<RespondentState>,
    path: CText,
    exec: Exec,
}

// --- weida_surveyor ---------------------------------------------------------

/// Creates a surveyor that dials on `tls`'s terms (`Runtime::surveyor`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with `weida_surveyor_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_surveyor(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_surveyor,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_surveyor {
            surveyor: Arc::new(rt.runtime.surveyor(tls.get())),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials the respondent at `url` (`Surveyor::connect`), adding it to the
/// set every survey asks. The runtime redials it when the connection is
/// lost.
///
/// Never blocks; the dial runs as the op written to `out`.
///
/// # Safety
///
/// `s` is NULL or a live surveyor; `url` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_surveyor_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_connect(
    s: *const weida_surveyor,
    url: *const c_char,
    out: *mut *mut weida_surveyor_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let surveyor = Arc::clone(&s.surveyor);
        let op = Op::start(&s.exec, async move { surveyor.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_surveyor_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_connect_op_fd(
    op: *const weida_surveyor_connect_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the dial: 0 when connected.
///
/// # Safety
///
/// `op` is NULL or a live connect op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_connect_op_wait(
    op: *const weida_surveyor_connect_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a connect op, aborting the dial when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live connect op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_connect_op_drop(op: *mut weida_surveyor_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The number of connected respondents (`Surveyor::peer_count`); 0 for
/// NULL.
///
/// # Safety
///
/// `s` is NULL or a live surveyor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_peer_count(s: *const weida_surveyor) -> usize {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live surveyor by this function's
        // contract.
        unsafe { &*s }.surveyor.peer_count()
    })
}

/// A snapshot of the surveyor's live connections
/// (`Surveyor::connection_stats`): one record per live dialled address, in
/// dial order, read with the `weida_connection_stats_*` getters. Never
/// blocks.
///
/// # Safety
///
/// `s` is NULL or a live surveyor; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_connection_stats(
    s: *const weida_surveyor,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let handle = weida_connection_stats::handle(s.surveyor.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Asks every connected respondent `body` (`Surveyor::survey`) and collects
/// their answers for `deadline_ms`. The body is copied.
///
/// Never blocks; the asking runs as the op written to `out`, whose wait
/// yields the run. The deadline bounds the asking and the answers alike. A
/// survey with no respondents is an empty run, not an error. A
/// `deadline_ms` the clock cannot add is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `s` is NULL or a live surveyor; `body` is NULL only with `len == 0`,
/// else points to `len` readable bytes; `out` is NULL or writable. Release
/// the op with `weida_surveyor_survey_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_survey(
    s: *const weida_surveyor,
    body: *const u8,
    len: usize,
    deadline_ms: u64,
    out: *mut *mut weida_surveyor_survey_op,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe {
        survey(
            s,
            || Ok(TransferMeta::default()),
            body,
            len,
            deadline_ms,
            out,
        )
    })
}

/// Asks every connected respondent `body` carrying `meta`
/// (`Surveyor::survey_with`): `weida_surveyor_survey` with metadata. `meta`
/// is copied at the call; NULL is `TransferMeta::default()`. The body is
/// copied.
///
/// # Safety
///
/// `s` is NULL or a live surveyor; `meta` is NULL or live metadata; `body`
/// is NULL only with `len == 0`, else points to `len` readable bytes; `out`
/// is NULL or writable. Release the op with `weida_surveyor_survey_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_survey_with(
    s: *const weida_surveyor,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    deadline_ms: u64,
    out: *mut *mut weida_surveyor_survey_op,
) -> c_int {
    let meta = || {
        if meta.is_null() {
            Ok(TransferMeta::default())
        } else {
            // SAFETY: forwarded from this function's contract.
            Ok(unsafe { borrow(meta) }?.get())
        }
    };
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { survey(s, meta, body, len, deadline_ms, out) })
}

/// The body of both survey exports.
///
/// # Safety
///
/// As `weida_surveyor_survey_with`.
unsafe fn survey(
    s: *const weida_surveyor,
    meta: impl FnOnce() -> Result<TransferMeta, c_int>,
    body: *const u8,
    len: usize,
    deadline_ms: u64,
    out: *mut *mut weida_surveyor_survey_op,
) -> Result<c_int, c_int> {
    let out = out_ptr(out)?;
    // SAFETY: forwarded from this function's contract.
    let s = unsafe { borrow(s) }?;
    let meta = meta()?;
    // SAFETY: forwarded from this function's contract.
    let body = unsafe { bytes(body, len) }?.to_vec();
    let deadline = ms(deadline_ms);
    // Rust adds the deadline to the clock and panics where it cannot; in
    // the task that panic would read as a runtime shut down.
    if std::time::Instant::now().checked_add(deadline).is_none() {
        return Err(fail_abi(WEIDA_EINVAL, c"deadline_ms is out of range"));
    }
    let surveyor = Arc::clone(&s.surveyor);
    let op = Op::start(&s.exec, async move {
        surveyor.survey_with(meta, &body, deadline).await
    });
    // SAFETY: `out` passed `out_ptr`.
    unsafe { write_out(out, into_handle(weida_surveyor_survey_op { op })) };
    Ok(0)
}

/// The readiness descriptor of a survey op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live survey op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_survey_op_fd(
    op: *const weida_surveyor_survey_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for every respondent to be asked and
/// writes the run to `out`, whose answers `weida_survey_run_next` reads.
///
/// # Safety
///
/// `op` is NULL or a live survey op; `out` is NULL or writable. Release the
/// run with `weida_survey_run_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_survey_op_wait(
    op: *const weida_surveyor_survey_op,
    timeout_ms: i64,
    out: *mut *mut weida_survey_run,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let run = unsafe { op_wait(op, timeout_ms) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_survey_run::handle(run)) };
        Ok(0)
    })
}

/// Releases a survey op. An unfinished survey is aborted; a run it made
/// that no wait took is dropped, which resets its exchanges. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live survey op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_survey_op_drop(op: *mut weida_surveyor_survey_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a surveyor, which drops its connections. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live surveyor, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_surveyor_drop(s: *mut weida_surveyor) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}

// --- weida_survey_run -------------------------------------------------------

/// The run's readiness descriptor (R11): readable when the next
/// `weida_survey_run_next` will not block.
///
/// # Safety
///
/// `r` is NULL or a live run; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_run_fd(r: *const weida_survey_run, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let fd = r.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next answer, reading at most `max_bytes` of it, waiting up to
/// `timeout_ms` (R4) for one (`SurveyRun::next`).
///
/// Writes NULL to `out` and returns 0 once the deadline passed or every
/// respondent answered: the end of the run, and every later call returns
/// it too. An answer that failed returns its code and the run goes on. A
/// read in flight keeps the `max_bytes` of the try that started it.
///
/// # Safety
///
/// `r` is NULL or a live run; `out` is NULL or writable. Release an answer
/// with `weida_survey_answer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_run_next(
    r: *const weida_survey_run,
    max_bytes: u64,
    timeout_ms: i64,
    out: *mut *mut weida_survey_answer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let max = usize::try_from(max_bytes).unwrap_or(usize::MAX);
        let answer = r.tried.attempt(timeout_ms, |state, cx| {
            let answering = state.answering.get_or_insert_with(|| {
                let mut run = state
                    .run
                    .take()
                    .expect("the run is held whenever no read is in flight");
                Box::pin(async move {
                    let answer = run.next(max).await;
                    (run, answer)
                })
            });
            match answering.as_mut().poll(cx) {
                Poll::Ready((run, answer)) => {
                    state.answering = None;
                    state.late = run.late();
                    state.run = Some(run);
                    Poll::Ready(answer)
                }
                Poll::Pending => Poll::Pending,
            }
        })?;
        let handle = match answer {
            None => std::ptr::null_mut(),
            Some(Ok(data)) => into_handle(weida_survey_answer { data }),
            Some(Err(e)) => return Err(fail(e)),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Answers that arrived after the deadline, dropped and counted
/// (`SurveyRun::late`); 0 for NULL. While a `weida_survey_run_next` is in
/// flight, the count as of the last read that ended.
///
/// # Safety
///
/// `r` is NULL or a live run.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_run_late(r: *const weida_survey_run) -> u64 {
    guard_or(0, || {
        if r.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `r` is a live run by this function's contract.
        unsafe { &*r }
            .tried
            .with(|s| s.run.as_ref().map_or(s.late, SurveyRun::late))
            .unwrap_or(0)
    })
}

/// How many respondents were asked (`SurveyRun::respondents`); 0 for NULL.
///
/// # Safety
///
/// `r` is NULL or a live run.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_run_respondents(r: *const weida_survey_run) -> usize {
    guard_or(0, || {
        if r.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `r` is a live run by this function's contract.
        unsafe { &*r }.respondents
    })
}

/// Releases a run, which resets every exchange not yet answered. A thread
/// waiting in `weida_survey_run_next` returns `-WEIDA_ECLOSED`. NULL does
/// nothing.
///
/// # Safety
///
/// `r` is NULL or a live run, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_run_drop(r: *mut weida_survey_run) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(r) = unsafe { release(r) } {
            drop(r.tried.close());
        }
    });
}

// --- weida_survey_answer ----------------------------------------------------

/// The answer's bytes; `weida_survey_answer_len` of them. Valid as long as
/// `a`.
///
/// # Safety
///
/// `a` is NULL or a live answer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_answer_data(a: *const weida_survey_answer) -> *const u8 {
    guard_ptr(|| {
        if a.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `a` is a live answer by the caller's contract.
        unsafe { &*a }.data.as_ptr()
    })
}

/// The answer's length in bytes; 0 for NULL.
///
/// # Safety
///
/// `a` is NULL or a live answer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_answer_len(a: *const weida_survey_answer) -> usize {
    guard_or(0, || {
        if a.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `a` is a live answer by the caller's contract.
        unsafe { &*a }.data.len()
    })
}

/// Releases an answer and its bytes. NULL does nothing.
///
/// # Safety
///
/// `a` is NULL or a live answer, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_survey_answer_drop(a: *mut weida_survey_answer) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(a) }));
}

// --- weida_respondent -------------------------------------------------------

/// Registers a respondent at the endpoint path `path`
/// (`Listener::respondent`). Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_respondent_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_respondent(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_respondent,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let respondent = l.listener.respondent(path).map_err(fail)?;
        let handle = into_handle(weida_respondent {
            path: CText::new(respondent.path()),
            tried: Tried::new(RespondentState {
                respondent: Arc::new(respondent),
                accepting: None,
            }),
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this respondent serves. Lives as long as the
/// respondent.
///
/// # Safety
///
/// `r` is NULL or a live respondent; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_respondent_path(
    r: *const weida_respondent,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if r.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `r` is a live respondent by this function's
        // contract; the text lives as long as it.
        let r = unsafe { &*r };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&r.path), len_out) }
    })
}

/// The respondent's readiness descriptor (R11): readable when the next
/// `weida_respondent_accept` will not block.
///
/// # Safety
///
/// `r` is NULL or a live respondent; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_respondent_fd(r: *const weida_respondent, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let fd = r.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next survey question, waiting up to `timeout_ms` (R4) for one
/// (`Respondent::accept`). A question is a `weida_incoming_request`,
/// answered as a replier's is. `-WEIDA_ENOT_CONNECTED` once the queue
/// ended.
///
/// # Safety
///
/// `r` is NULL or a live respondent; `out` is NULL or writable. Release the
/// request with `weida_incoming_request_drop` or reply to it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_respondent_accept(
    r: *const weida_respondent,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_request,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let accepted = r.tried.attempt(timeout_ms, |state, cx| {
            let accepting = state.accepting.get_or_insert_with(|| {
                let respondent = Arc::clone(&state.respondent);
                Box::pin(async move { respondent.accept().await })
            });
            let polled = accepting.as_mut().poll(cx);
            if polled.is_ready() {
                state.accepting = None;
            }
            polled
        })?;
        let request = accepted.map_err(fail)?;
        let handle = weida_incoming_request::handle(request, r.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a respondent; its path stays registered, as a dropped Rust
/// `Respondent` leaves it. A thread waiting in `weida_respondent_accept`
/// returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live respondent, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_respondent_drop(r: *mut weida_respondent) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(r) = unsafe { release(r) } {
            drop(r.tried.close());
        }
    });
}
