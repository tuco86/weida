//! The cursor surface: the levels a transfer orders reported, the sender's
//! queue of cursor sets, and the receiver's reporter (docs/c/cursor.md).

use std::ffi::c_int;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;

use tokio::sync::{Mutex, MutexGuard};
use weida_rs::{CursorLevel, CursorSet, Cursors, Error, ReportMode, Reporter};
use weida_runtime::Exec;

use crate::error::{
    WEIDA_EINVAL, closed, fail_abi, guard, guard_or, guard_ptr, guard_void, null_arg,
};
use crate::handle::{borrow, into_handle, out_ptr, release, write_out};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait};
use crate::try_handle::Tried;

/// The first cursor level an application may name
/// (`CursorLevel::APPLICATION_FLOOR`); the levels below it are the
/// `WEIDA_ACKNOWLEDGEMENT_*` values.
pub const WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR: c_int = 16;
const _: () =
    assert!(WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR as u64 == CursorLevel::APPLICATION_FLOOR);

/// `ReportMode::Progress`.
pub const WEIDA_REPORT_MODE_PROGRESS: c_int = 0;
/// `ReportMode::FinalOnly`.
pub const WEIDA_REPORT_MODE_FINAL_ONLY: c_int = 1;

/// `Reporter::DEFAULT_BYTES`.
pub const WEIDA_REPORTER_DEFAULT_BYTES: c_int = 1_048_576;
const _: () = assert!(WEIDA_REPORTER_DEFAULT_BYTES as u64 == Reporter::DEFAULT_BYTES);
/// `Reporter::DEFAULT_INTERVAL`, in milliseconds.
pub const WEIDA_REPORTER_DEFAULT_INTERVAL_MS: c_int = 100;
const _: () =
    assert!(WEIDA_REPORTER_DEFAULT_INTERVAL_MS as u128 == Reporter::DEFAULT_INTERVAL.as_millis());

/// A cursor level in, as its wire value: `-WEIDA_EINVAL` for the reserved
/// values 6 to 15.
pub(crate) fn level(v: u64) -> Result<CursorLevel, c_int> {
    CursorLevel::from_wire(v)
        .ok_or_else(|| fail_abi(WEIDA_EINVAL, c"a cursor level from 6 to 15 is reserved"))
}

/// An array of cursor levels in; the first reserved value fails the whole
/// call.
///
/// # Safety
///
/// `p` is NULL only with `len == 0`, else points to `len` readable values.
pub(crate) unsafe fn levels(p: *const u64, len: usize) -> Result<Vec<CursorLevel>, c_int> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if p.is_null() {
        return Err(null_arg());
    }
    // SAFETY: a non-NULL `p` points to `len` readable values by this
    // function's contract.
    let wire = unsafe { std::slice::from_raw_parts(p, len) };
    wire.iter().map(|&v| level(v)).collect()
}

/// A report mode out, as its `WEIDA_REPORT_MODE_*` constant.
pub(crate) fn mode_constant(m: ReportMode) -> c_int {
    m.to_wire() as c_int
}

type Changing = Pin<Box<dyn Future<Output = (Cursors, Option<CursorSet>)> + Send>>;

/// A reader's state between tries: the recv in flight owns the reader and
/// hands it back, so the future is kept, never recreated, and a timeout or
/// `EINTR` loses nothing (R3a).
struct CursorsState {
    cursors: Option<Cursors>,
    changing: Option<Changing>,
}

/// The cursors one transfer ordered (`Cursors`), read as a queue of cursor
/// sets.
pub struct weida_cursors {
    tried: Tried<CursorsState>,
}

impl weida_cursors {
    pub(crate) fn handle(cursors: Cursors) -> *mut weida_cursors {
        into_handle(weida_cursors {
            tried: Tried::new(CursorsState {
                cursors: Some(cursors),
                changing: None,
            }),
        })
    }
}

/// The newest offset per reported level of one transfer (`CursorSet`),
/// read-only.
pub struct weida_cursor_set {
    set: CursorSet,
}

/// The receiver's end of a report (`Reporter`). A running
/// `weida_reporter_report` holds the lock; `weida_reporter_finish` takes the
/// value out.
pub struct weida_reporter {
    reporter: Arc<Mutex<Option<Reporter>>>,
    /// The ordered levels' wire values, ascending, cached at creation.
    levels: Box<[u64]>,
    mode: c_int,
    exec: Exec,
}

impl weida_reporter {
    pub(crate) fn handle(reporter: Reporter, exec: Exec) -> *mut weida_reporter {
        let levels = reporter.levels().iter().map(|l| l.to_wire()).collect();
        let mode = mode_constant(reporter.mode());
        into_handle(weida_reporter {
            reporter: Arc::new(Mutex::new(Some(reporter))),
            levels,
            mode,
            exec,
        })
    }
}

/// The pending report of `weida_reporter_report`.
pub struct weida_reporter_report_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_reporter_report_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending flush and FIN of `weida_reporter_finish`.
pub struct weida_reporter_finish_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_reporter_finish_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The reporter for one synchronous call: `-WEIDA_EINVAL` while a
/// `weida_reporter_report` op holds it.
fn locked(r: &weida_reporter) -> Result<MutexGuard<'_, Option<Reporter>>, c_int> {
    r.reporter.try_lock().map_err(|_| in_flight())
}

fn in_flight() -> c_int {
    fail_abi(WEIDA_EINVAL, c"a report is in flight on this reporter")
}

// --- weida_cursors ---------------------------------------------------------

/// The queue's readiness descriptor (R11): readable when the next
/// `weida_cursors_recv` will not block.
///
/// # Safety
///
/// `c` is NULL or a live queue; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursors_fd(c: *const weida_cursors, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let c = unsafe { borrow(c) }?;
        let fd = c.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the newest cursor set after a change, waiting up to `timeout_ms`
/// (R4) for one (`Cursors::changed`). Changes since the last recv coalesce
/// into one set: offsets are absolute, so the newest set loses nothing.
///
/// Writes NULL to `out` and returns 0 once no more cursors come (the
/// reporter finished, the stream was reset or the connection is gone,
/// indistinguishably), and on every recv after that. Fails with
/// `-WEIDA_EAGAIN` when nothing changed in time, `-WEIDA_EINTR` and
/// `-WEIDA_ECLOSED`.
///
/// # Safety
///
/// `c` is NULL or a live queue; `out` is NULL or writable. Release a set
/// with `weida_cursor_set_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursors_recv(
    c: *const weida_cursors,
    timeout_ms: i64,
    out: *mut *mut weida_cursor_set,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let c = unsafe { borrow(c) }?;
        let set = c.tried.attempt(timeout_ms, |state, cx| {
            let changing = state.changing.get_or_insert_with(|| {
                let mut cursors = state
                    .cursors
                    .take()
                    .expect("the reader is held whenever no recv is in flight");
                Box::pin(async move {
                    let set = cursors.changed().await;
                    (cursors, set)
                })
            });
            match changing.as_mut().poll(cx) {
                Poll::Ready((cursors, set)) => {
                    state.changing = None;
                    state.cursors = Some(cursors);
                    Poll::Ready(set)
                }
                Poll::Pending => Poll::Pending,
            }
        })?;
        let handle = set.map_or(ptr::null_mut(), |set| into_handle(weida_cursor_set { set }));
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a queue and with it the report id; records arriving after are
/// ignored. A thread waiting in `weida_cursors_recv` returns
/// `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `c` is NULL or a live queue, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursors_drop(c: *mut weida_cursors) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(c) = unsafe { release(c) } {
            drop(c.tried.close());
        }
    });
}

// --- weida_cursor_set ------------------------------------------------------

/// How many levels the set carries (`CursorSet::len`); 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursor_set_len(s: *const weida_cursor_set) -> usize {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live set by this function's contract.
        unsafe { &*s }.set.len()
    })
}

/// Reads entry `index` (`CursorSet::iter`): 1 with the level's wire value
/// in `*out_level` and its offset in `*out_offset`; levels ascend with the
/// index. 0 for an index past the end and for a NULL argument; the outs
/// are then untouched.
///
/// # Safety
///
/// `s` is NULL or a live set; `out_level` and `out_offset` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursor_set_level(
    s: *const weida_cursor_set,
    index: usize,
    out_level: *mut u64,
    out_offset: *mut u64,
) -> c_int {
    guard_or(0, || {
        if s.is_null() || out_level.is_null() || out_offset.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live set by this function's contract.
        let Some((level, offset)) = unsafe { &*s }.set.iter().nth(index) else {
            return 0;
        };
        // SAFETY: both outs are non-NULL and writable by this function's
        // contract.
        unsafe {
            out_level.write(level.to_wire());
            out_offset.write(offset);
        }
        1
    })
}

/// The offset reported for `level` (`CursorSet::offset`): 1 and `*out` when
/// it was reported, 0 when not. A level from 6 to 15 is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `s` is NULL or a live set; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursor_set_offset(
    s: *const weida_cursor_set,
    level: u64,
    out: *mut u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let out = out_ptr(out)?;
        let level = self::level(level)?;
        match s.set.offset(level) {
            Some(offset) => {
                // SAFETY: `out` passed `out_ptr`.
                unsafe { write_out(out, offset) };
                Ok(1)
            }
            None => Ok(0),
        }
    })
}

/// Releases a set. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live set, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_cursor_set_drop(s: *mut weida_cursor_set) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}

// --- weida_reporter --------------------------------------------------------

/// The levels the sender ordered (`Reporter::levels`), as ascending wire
/// values, and their count in `*len_out`; NULL for a NULL `r`, `*len_out`
/// then untouched.
///
/// # Safety
///
/// `r` is NULL or a live reporter; `len_out` is NULL or writable. The array
/// lives as long as `r`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_levels(
    r: *const weida_reporter,
    len_out: *mut usize,
) -> *const u64 {
    guard_ptr(|| {
        if r.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `r` is a live reporter by this function's
        // contract; the array lives as long as the handle.
        let r = unsafe { &*r };
        if !len_out.is_null() {
            // SAFETY: a non-NULL `len_out` is writable by this function's
            // contract.
            unsafe { len_out.write(r.levels.len()) };
        }
        r.levels.as_ptr()
    })
}

/// The mode the sender asked for (`Reporter::mode`), a
/// `WEIDA_REPORT_MODE_*` constant.
///
/// # Safety
///
/// `r` is NULL or a live reporter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_mode(r: *const weida_reporter) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| Ok(unsafe { borrow(r) }?.mode))
}

/// Sets how often progress records are written
/// (`Reporter::with_granularity`): when a level advanced `bytes` or
/// `interval_ms` passed since its last record, whichever comes first. 0 and
/// 0 write every report; no effect in final-only mode. `-WEIDA_EINVAL`
/// while a report op runs.
///
/// # Safety
///
/// `r` is NULL or a live reporter.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_set_granularity(
    r: *const weida_reporter,
    bytes: u64,
    interval_ms: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let mut held = locked(&r)?;
        let reporter = held.take().ok_or_else(closed)?;
        *held = Some(reporter.with_granularity(bytes, crate::config::ms(interval_ms)));
        Ok(0)
    })
}

/// Reports that `level` (a wire value) has reached `offset`
/// (`Reporter::report`). A level the sender did not order is ignored; one
/// from 6 to 15 is `-WEIDA_EINVAL`.
///
/// Never blocks; the report runs as the op written to `out`, whose wait
/// returns 0: a report never fails for a transport reason. Until it
/// finished, every other call on the reporter is `-WEIDA_EINVAL`. Dropping
/// an unfinished op aborts the report, as dropping Rust's future does.
///
/// # Safety
///
/// `r` is NULL or a live reporter; `out` is NULL or writable. Release the op
/// with `weida_reporter_report_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_report(
    r: *const weida_reporter,
    level: u64,
    offset: u64,
    out: *mut *mut weida_reporter_report_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let level = self::level(level)?;
        // Taken here, not in the task: a call after this one must not
        // overtake the report.
        let mut held = Arc::clone(&r.reporter)
            .try_lock_owned()
            .map_err(|_| in_flight())?;
        let op = Op::start(&r.exec, async move {
            match held.as_mut() {
                Some(reporter) => reporter.report(level, offset).await,
                None => Err(Error::Canceled),
            }
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_reporter_report_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a report op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live report op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_report_op_fd(
    op: *const weida_reporter_report_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the report: 0 once it was recorded or
/// written.
///
/// # Safety
///
/// `op` is NULL or a live report op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_report_op_wait(
    op: *const weida_reporter_report_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a report op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live report op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_report_op_drop(op: *mut weida_reporter_report_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Flushes the newest offset per reported level and ends the report
/// (`Reporter::finish`); a level never reported gets no record.
/// **Consumes** `r` on every return path.
///
/// Never blocks; the flush runs as the op written to `out`. While a report
/// op runs it fails with `-WEIDA_EINVAL`, and the reporter is dropped,
/// unflushed, when that report ends; a NULL `out` fails the same way and
/// drops it at once.
///
/// # Safety
///
/// `r` is NULL or a live reporter, not used again; `out` is NULL or
/// writable. Release the op with `weida_reporter_finish_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_finish(
    r: *mut weida_reporter,
    out: *mut *mut weida_reporter_finish_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let r = unsafe { release(r) }.ok_or_else(null_arg)?;
        let out = out_ptr(out)?;
        let reporter = locked(&r)?.take().ok_or_else(closed)?;
        let op = Op::start(&r.exec, reporter.finish());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_reporter_finish_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a finish op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live finish op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_finish_op_fd(
    op: *const weida_reporter_finish_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the flush: 0 once the stream was
/// finished, or the report was over already.
///
/// # Safety
///
/// `op` is NULL or a live finish op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_finish_op_wait(
    op: *const weida_reporter_finish_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a finish op, aborting the flush when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live finish op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_finish_op_drop(op: *mut weida_reporter_finish_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a reporter without finishing it: it flushes nothing, so records
/// the granularity held back are never sent and the sender's queue ends
/// without them. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live reporter, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reporter_drop(r: *mut weida_reporter) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(r) }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_rs::Acknowledgement;

    #[test]
    fn a_reserved_level_is_refused_and_the_rest_read_back() {
        assert_eq!(level(7), Err(-WEIDA_EINVAL));
        assert_eq!(level(2), Ok(CursorLevel::Known(Acknowledgement::Accepted)));
        assert_eq!(level(16), Ok(CursorLevel::Application(16)));
        // SAFETY: a NULL array with a zero length is never read.
        assert_eq!(unsafe { levels(ptr::null(), 0) }, Ok(vec![]));
        // SAFETY: a NULL array with a non-zero length is refused unread.
        assert_eq!(unsafe { levels(ptr::null(), 1) }, Err(-WEIDA_EINVAL));
    }
}
