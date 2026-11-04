//! The two halves of Pub/Sub, streamed fan-outs and drop counts
//! (docs/c/pubsub.md).

use std::ffi::{c_char, c_int};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use ::weida_runtime::Exec;
use tokio::sync::{Mutex, MutexGuard};
use weida_rs::{Error, FanOut, Publisher, Subscriber, TopicDrops, TraceContext};

use crate::error::{
    WEIDA_EINVAL, closed, fail, fail_abi, guard, guard_or, guard_ptr, guard_void, null_arg,
};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::push::Receiving;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::stats::{weida_connection_stats, weida_peer_events};
use crate::tls::weida_client_tls;
use crate::transfer::weida_incoming_transfer;
use crate::try_handle::Tried;

/// The publishing half of Pub/Sub, serving one endpoint path.
pub struct weida_publisher {
    publisher: Publisher,
    path: CText,
    exec: Exec,
}

/// A publish in progress (`FanOut`): one payload written once, a stream per
/// subscriber. A running `weida_fan_out_write_within` holds the lock;
/// `weida_fan_out_finish` takes the value out.
pub struct weida_fan_out {
    fan: Arc<Mutex<Option<FanOut>>>,
    topic: CText,
    exec: Exec,
}

/// The pending write of `weida_fan_out_write_within`.
pub struct weida_fan_out_write_within_op {
    op: Arc<Op<usize>>,
}

impl AsOp for weida_fan_out_write_within_op {
    type Output = usize;
    fn op(&self) -> &Op<usize> {
        &self.op
    }
}

/// One snapshot of what a publisher dropped, one record per topic
/// (`TopicDrops`).
pub struct weida_topic_drops {
    records: Vec<TopicDrops>,
    topics: Vec<CText>,
}

impl weida_topic_drops {
    pub(crate) fn handle(records: Vec<TopicDrops>) -> *mut weida_topic_drops {
        let topics = records
            .iter()
            .map(|r| CText::new(r.topic.as_bytes()))
            .collect();
        into_handle(weida_topic_drops { records, topics })
    }
}

/// The subscribing half of Pub/Sub. The recv in flight is kept between
/// tries, never recreated (R3a).
pub struct weida_subscriber {
    subscriber: Arc<Subscriber>,
    tried: Tried<Option<Receiving>>,
    exec: Exec,
}

/// The pending dial of `weida_subscriber_connect`.
pub struct weida_subscriber_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_subscriber_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending subscription of `weida_subscriber_subscribe`.
pub struct weida_subscriber_subscribe_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_subscriber_subscribe_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending withdrawal of `weida_subscriber_unsubscribe`.
pub struct weida_subscriber_unsubscribe_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_subscriber_unsubscribe_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending grant of `weida_subscriber_grant`.
pub struct weida_subscriber_grant_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_subscriber_grant_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// A trace context argument, given as its W3C version-00 `traceparent`
/// text; anything else is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `traceparent` is NULL or a NUL-terminated string.
unsafe fn trace(traceparent: *const c_char) -> Result<TraceContext, c_int> {
    // SAFETY: forwarded from this function's contract.
    let text = unsafe { cstr(traceparent) }?;
    TraceContext::parse_traceparent(text)
        .map_err(|_| fail_abi(WEIDA_EINVAL, c"not a W3C version-00 traceparent"))
}

/// The fan-out for one synchronous call: `-WEIDA_EINVAL` while a
/// `weida_fan_out_write_within` op holds it.
fn fan(f: &weida_fan_out) -> Result<MutexGuard<'_, Option<FanOut>>, c_int> {
    f.fan
        .try_lock()
        .map_err(|_| fail_abi(WEIDA_EINVAL, c"a write_within is in flight on this fan-out"))
}

// --- weida_publisher -------------------------------------------------------

/// Registers a publisher at the endpoint path `path`
/// (`Listener::publisher`). Subscriptions that arrived before this call are
/// already recorded. Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_publisher_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_publisher(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_publisher,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let publisher = l.listener.publisher(path).map_err(fail)?;
        let handle = into_handle(weida_publisher {
            path: CText::new(publisher.path()),
            publisher,
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this publisher serves. Lives as long as the publisher.
///
/// # Safety
///
/// `p` is NULL or a live publisher; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_path(
    p: *const weida_publisher,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if p.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `p` is a live publisher by this function's
        // contract; the text lives as long as it.
        let p = unsafe { &*p };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&p.path), len_out) }
    })
}

/// Fans `data` out to every subscriber whose filter matches `topic`
/// (`Publisher::publish`) and writes how many it was enqueued to. The data
/// is copied. Never blocks: a subscriber whose byte budget is exhausted
/// loses this message, counted by `weida_publisher_dropped`; zero
/// subscribers is not an error.
///
/// Fails with `-WEIDA_ELIMIT_EXCEEDED` for `len` above
/// `subscriber_buffer_bytes` or a topic above 256 bytes.
///
/// # Safety
///
/// `p` is NULL or a live publisher; `topic` is NULL or a NUL-terminated
/// string; `data` is NULL only with `len == 0`, else points to `len`
/// readable bytes; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_publish(
    p: *const weida_publisher,
    topic: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?.to_vec();
        let reached = p.publisher.publish(topic, data).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, reached) };
        Ok(0)
    })
}

/// `weida_publisher_publish` propagating a trace context
/// (`Publisher::publish_with_trace`), given as its W3C version-00
/// `traceparent` text; anything else is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// As `weida_publisher_publish`; `traceparent` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_publish_with_trace(
    p: *const weida_publisher,
    topic: *const c_char,
    data: *const u8,
    len: usize,
    traceparent: *const c_char,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?.to_vec();
        // SAFETY: forwarded from this function's contract.
        let trace = unsafe { trace(traceparent) }?;
        let reached = p
            .publisher
            .publish_with_trace(topic, data, trace)
            .map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, reached) };
        Ok(0)
    })
}

/// Opens a streamed publish on `topic` (`Publisher::open`): one stream per
/// subscriber matched at this call, written chunk by chunk with
/// `weida_fan_out_write_within` or `weida_fan_out_write_now` and ended with
/// `weida_fan_out_finish`. Never blocks.
///
/// # Safety
///
/// `p` is NULL or a live publisher; `topic` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_fan_out_finish` or `weida_fan_out_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_open(
    p: *const weida_publisher,
    topic: *const c_char,
    out: *mut *mut weida_fan_out,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        let handle = fan_out_handle(p.publisher.open(topic), &p.exec);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// `weida_publisher_open` propagating a trace context
/// (`Publisher::open_with_trace`), given as its W3C version-00
/// `traceparent` text; anything else is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// As `weida_publisher_open`; `traceparent` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_open_with_trace(
    p: *const weida_publisher,
    topic: *const c_char,
    traceparent: *const c_char,
    out: *mut *mut weida_fan_out,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        // SAFETY: forwarded from this function's contract.
        let trace = unsafe { trace(traceparent) }?;
        let handle = fan_out_handle(p.publisher.open_with_trace(topic, trace), &p.exec);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

fn fan_out_handle(fan: FanOut, exec: &Exec) -> *mut weida_fan_out {
    into_handle(weida_fan_out {
        topic: CText::new(fan.topic()),
        fan: Arc::new(Mutex::new(Some(fan))),
        exec: exec.clone(),
    })
}

/// The connections subscribed to this publisher
/// (`Publisher::subscriber_count`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live publisher.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_subscriber_count(p: *const weida_publisher) -> usize {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live publisher by this function's
        // contract.
        unsafe { &*p }.publisher.subscriber_count()
    })
}

/// The filters registered on this publisher, summed over subscribers
/// (`Publisher::filter_count`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live publisher.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_filter_count(p: *const weida_publisher) -> usize {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live publisher by this function's
        // contract.
        unsafe { &*p }.publisher.filter_count()
    })
}

/// The messages dropped because a subscriber could not take them, summed
/// over topics and causes (`Publisher::dropped`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live publisher.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_dropped(p: *const weida_publisher) -> u64 {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live publisher by this function's
        // contract.
        unsafe { &*p }.publisher.dropped()
    })
}

/// What was dropped on `topic`, by cause (`Publisher::dropped_on`): a
/// snapshot of one record, or of none when nothing was. Never blocks.
///
/// # Safety
///
/// `p` is NULL or a live publisher; `topic` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the snapshot with
/// `weida_topic_drops_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_dropped_on(
    p: *const weida_publisher,
    topic: *const c_char,
    out: *mut *mut weida_topic_drops,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        let records = p.publisher.dropped_on(topic).into_iter().collect();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_topic_drops::handle(records)) };
        Ok(0)
    })
}

/// Every topic that lost a copy, with its counts by cause
/// (`Publisher::drops`): a snapshot read with the `weida_topic_drops_*`
/// getters. Never blocks.
///
/// # Safety
///
/// `p` is NULL or a live publisher; `out` is NULL or writable. Release the
/// snapshot with `weida_topic_drops_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_drops(
    p: *const weida_publisher,
    out: *mut *mut weida_topic_drops,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let handle = weida_topic_drops::handle(p.publisher.drops());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a publisher; its path stays registered, as a dropped Rust
/// `Publisher` leaves it. NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live publisher, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_publisher_drop(p: *mut weida_publisher) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(p) }));
}

// --- weida_fan_out ---------------------------------------------------------

/// The topic this fan-out publishes on. Lives as long as the fan-out.
///
/// # Safety
///
/// `f` is NULL or a live fan-out; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_topic(
    f: *const weida_fan_out,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if f.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `f` is a live fan-out by this function's
        // contract; the text lives as long as it.
        let f = unsafe { &*f };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&f.topic), len_out) }
    })
}

/// Writes the subscribers still receiving this transfer
/// (`FanOut::subscribers`). It only falls. `-WEIDA_EINVAL` while a
/// `weida_fan_out_write_within` op runs.
///
/// # Safety
///
/// `f` is NULL or a live fan-out; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_subscribers(
    f: *const weida_fan_out,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let left = fan(&f)?.as_ref().ok_or_else(closed)?.subscribers();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, left) };
        Ok(0)
    })
}

/// Writes the next chunk to every subscriber still receiving, waiting up to
/// `limit_ms` for those that have no room (`FanOut::write_within`). The
/// limit is for the whole call, not per subscriber; one that frees no room
/// in time loses the transfer, and only it. The chunk is copied.
///
/// Never blocks; the write runs as the op written to `out`, whose wait
/// yields the subscribers left. Until it finished, every other call on the
/// fan-out is `-WEIDA_EINVAL`. A chunk above `subscriber_buffer_bytes`
/// fails the wait with `-WEIDA_ELIMIT_EXCEEDED`.
///
/// # Safety
///
/// `f` is NULL or a live fan-out; `chunk` is NULL only with `len == 0`,
/// else points to `len` readable bytes; `out` is NULL or writable. Release
/// the op with `weida_fan_out_write_within_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_write_within(
    f: *const weida_fan_out,
    chunk: *const u8,
    len: usize,
    limit_ms: u64,
    out: *mut *mut weida_fan_out_write_within_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        // SAFETY: forwarded from this function's contract.
        let chunk = unsafe { bytes(chunk, len) }?.to_vec();
        // Taken here, not in the task: a call after this one must not
        // overtake the chunk.
        let mut held = Arc::clone(&f.fan)
            .try_lock_owned()
            .map_err(|_| fail_abi(WEIDA_EINVAL, c"a write_within is in flight on this fan-out"))?;
        let limit = Duration::from_millis(limit_ms);
        let op = Op::start(&f.exec, async move {
            match held.as_mut() {
                Some(fan) => fan.write_within(chunk, limit).await,
                None => Err(Error::Canceled),
            }
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_fan_out_write_within_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a write op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live write op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_write_within_op_fd(
    op: *const weida_fan_out_write_within_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the write and writes the subscribers
/// left to `out`.
///
/// # Safety
///
/// `op` is NULL or a live write op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_write_within_op_wait(
    op: *const weida_fan_out_write_within_op,
    timeout_ms: i64,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let left = unsafe { op_wait(op, timeout_ms) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, left) };
        Ok(0)
    })
}

/// Releases a write op. Dropped unfinished, the write is abandoned: the
/// chunk reached some subscribers and not the rest, so later writes fail
/// with `-WEIDA_ECANCELED` and `weida_fan_out_finish` counts 0, aborting
/// every copy. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live write op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_write_within_op_drop(
    op: *mut weida_fan_out_write_within_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Writes the next chunk without ever waiting (`FanOut::write_now`) and
/// writes the subscribers left: one with no room right now loses the
/// transfer. The chunk is copied. Fails with `-WEIDA_ELIMIT_EXCEEDED` for
/// a chunk above `subscriber_buffer_bytes`, `-WEIDA_ECANCELED` after an
/// abandoned `weida_fan_out_write_within`, and `-WEIDA_EINVAL` while one
/// runs.
///
/// # Safety
///
/// `f` is NULL or a live fan-out; `chunk` is NULL only with `len == 0`,
/// else points to `len` readable bytes; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_write_now(
    f: *const weida_fan_out,
    chunk: *const u8,
    len: usize,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        // SAFETY: forwarded from this function's contract.
        let chunk = unsafe { bytes(chunk, len) }?.to_vec();
        let left = fan(&f)?
            .as_mut()
            .ok_or_else(closed)?
            .write_now(chunk)
            .map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, left) };
        Ok(0)
    })
}

/// Ends the transfer (`FanOut::finish`) and writes how many subscribers
/// received all of it as far as this side can tell. **Consumes** `f` on
/// every return path.
///
/// After an abandoned `weida_fan_out_write_within` it aborts every copy
/// and writes 0. While a write op runs it fails with `-WEIDA_EINVAL`, and
/// the copies abort once that write ends; a NULL `out` fails the same way
/// and aborts them at once.
///
/// # Safety
///
/// `f` is NULL or a live fan-out, not used again; `out` is NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_finish(f: *mut weida_fan_out, out: *mut usize) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let f = unsafe { release(f) }.ok_or_else(null_arg)?;
        let out = out_ptr(out)?;
        let fan = fan(&f)?.take().ok_or_else(closed)?;
        let delivered = fan.finish();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, delivered) };
        Ok(0)
    })
}

/// Releases a fan-out without finishing it: every copy not finished is
/// aborted, once a running write op ends. NULL does nothing.
///
/// # Safety
///
/// `f` is NULL or a live fan-out, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fan_out_drop(f: *mut weida_fan_out) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(f) }));
}

// --- weida_topic_drops -----------------------------------------------------

/// Reads one count of record `index`: 1 and `*out` when present, 0 when
/// absent (R2): a NULL `d` or `out`, or an index past the end.
unsafe fn drops_number(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
    pick: impl FnOnce(&TopicDrops) -> u64,
) -> c_int {
    guard_or(0, || {
        if d.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live snapshot by the caller's contract.
        let d = unsafe { &*d };
        match d.records.get(index).map(pick) {
            Some(v) => {
                // SAFETY: a non-NULL `out` is writable by the caller's contract.
                unsafe { out.write(v) };
                1
            }
            None => 0,
        }
    })
}

/// The number of records, one per topic that lost a copy; 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_len(d: *const weida_topic_drops) -> usize {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live snapshot by the caller's contract.
        unsafe { &*d }.records.len()
    })
}

/// The topic of record `index`; NULL past the end. Lives as long as `d`.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_topic(
    d: *const weida_topic_drops,
    index: usize,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if d.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `d` is a live snapshot by the caller's
        // contract; the text lives as long as it.
        let d = unsafe { &*d };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(d.topics.get(index), len_out) }
    })
}

/// `TopicDrops::subscriber_budget` of record `index`, copies dropped for an
/// exhausted subscriber byte budget: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_subscriber_budget(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.subscriber_budget) }
}

/// `TopicDrops::subscriber_queue` of record `index`, copies dropped for a
/// full subscriber queue: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_subscriber_queue(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.subscriber_queue) }
}

/// `TopicDrops::no_parked_connection` of record `index`, copies dropped
/// because the subscriber had parked no connection: 1 and `*out`, 0 past
/// the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_no_parked_connection(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.no_parked_connection) }
}

/// `TopicDrops::superseded` of record `index`, RADIO copies reset because a
/// newer segment opened on the topic: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_superseded(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.superseded) }
}

/// `TopicDrops::expired` of record `index`, RADIO copies reset because the
/// dish's `max_age` passed: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_expired(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.expired) }
}

/// `TopicDrops::too_large` of record `index`, RADIO datagram segments
/// larger than the dish's connection carries: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_too_large(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.too_large) }
}

/// `TopicDrops::no_datagrams` of record `index`, RADIO datagram segments
/// for a dish that carries no datagrams: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_no_datagrams(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.no_datagrams) }
}

/// `TopicDrops::layers_cut` of record `index`, segment copies that lost
/// layers above 0 and kept layer 0: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_layers_cut(
    d: *const weida_topic_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { drops_number(d, index, out, |r| r.layers_cut) }
}

/// Releases a drops snapshot. NULL does nothing.
///
/// # Safety
///
/// `d` is NULL or a live snapshot, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_topic_drops_drop(d: *mut weida_topic_drops) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(d) }));
}

// --- weida_subscriber ------------------------------------------------------

/// Creates a subscriber that dials on `tls`'s terms
/// (`Runtime::subscriber`). Two subscribers on one runtime share the pooled
/// connection, so their paths collide with `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with
/// `weida_subscriber_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_subscriber(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_subscriber,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_subscriber {
            subscriber: Arc::new(rt.runtime.subscriber(tls.get())),
            tried: Tried::new(None),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials `url` (`Subscriber::connect`), e.g. `weida+inproc://bus/md`, and
/// subscribes there to every filter registered so far. The runtime redials
/// it when the connection is lost, filters included; what was published in
/// between is gone.
///
/// Never blocks; the dial runs as the op written to `out`.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `url` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_subscriber_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_connect(
    s: *const weida_subscriber,
    url: *const c_char,
    out: *mut *mut weida_subscriber_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let subscriber = Arc::clone(&s.subscriber);
        let op = Op::start(&s.exec, async move { subscriber.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_subscriber_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_connect_op_fd(
    op: *const weida_subscriber_connect_op,
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
pub unsafe extern "C" fn weida_subscriber_connect_op_wait(
    op: *const weida_subscriber_connect_op,
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
pub unsafe extern "C" fn weida_subscriber_connect_op_drop(op: *mut weida_subscriber_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Forgets a dialled address (`Subscriber::disconnect`): 1 when `url` was
/// dialled and is now forgotten, 0 when it was not.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `url` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_disconnect(
    s: *const weida_subscriber,
    url: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?;
        Ok(c_int::from(s.subscriber.disconnect(url)))
    })
}

/// The number of connected peers (`Subscriber::peer_count`); 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live subscriber.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_peer_count(s: *const weida_subscriber) -> usize {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live subscriber by this function's
        // contract.
        unsafe { &*s }.subscriber.peer_count()
    })
}

/// A snapshot of the subscriber's live connections
/// (`Subscriber::connection_stats`), read with the
/// `weida_connection_stats_*` getters. Never blocks.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_connection_stats(
    s: *const weida_subscriber,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let handle = weida_connection_stats::handle(s.subscriber.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// A new reader of the subscriber's peer events (`Subscriber::events`),
/// read with `weida_peer_events_recv`: where a gap in the stream began and
/// ended. Each call is an independent reader.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_events(
    s: *const weida_subscriber,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let handle = weida_peer_events::handle(s.subscriber.events());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Registers interest in every topic `filter` matches
/// (`Subscriber::subscribe`), a segmented pattern of PROTOCOL.md §6.4: `*`
/// one whole segment, a trailing `#` zero or more. Subscribing twice is
/// idempotent. The filter is copied.
///
/// Never blocks; the subscription runs as the op written to `out`. A
/// filter outside the grammar fails its wait with `-WEIDA_EPROTOCOL`.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `filter` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_subscriber_subscribe_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_subscribe(
    s: *const weida_subscriber,
    filter: *const c_char,
    out: *mut *mut weida_subscriber_subscribe_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let filter = unsafe { cstr(filter) }?.to_owned();
        let subscriber = Arc::clone(&s.subscriber);
        let op = Op::start(&s.exec, async move { subscriber.subscribe(&filter).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_subscriber_subscribe_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a subscribe op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live subscribe op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_subscribe_op_fd(
    op: *const weida_subscriber_subscribe_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the subscription: 0 once every
/// connected peer was told.
///
/// # Safety
///
/// `op` is NULL or a live subscribe op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_subscribe_op_wait(
    op: *const weida_subscriber_subscribe_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a subscribe op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live subscribe op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_subscribe_op_drop(
    op: *mut weida_subscriber_subscribe_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Withdraws a filter (`Subscriber::unsubscribe`); an unknown one is
/// ignored. The filter is copied.
///
/// Never blocks; the withdrawal runs as the op written to `out`.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `filter` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_subscriber_unsubscribe_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_unsubscribe(
    s: *const weida_subscriber,
    filter: *const c_char,
    out: *mut *mut weida_subscriber_unsubscribe_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let filter = unsafe { cstr(filter) }?.to_owned();
        let subscriber = Arc::clone(&s.subscriber);
        let op = Op::start(
            &s.exec,
            async move { subscriber.unsubscribe(&filter).await },
        );
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_subscriber_unsubscribe_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of an unsubscribe op: readable once it
/// finished.
///
/// # Safety
///
/// `op` is NULL or a live unsubscribe op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_unsubscribe_op_fd(
    op: *const weida_subscriber_unsubscribe_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the withdrawal: 0 once every
/// connected peer was told.
///
/// # Safety
///
/// `op` is NULL or a live unsubscribe op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_unsubscribe_op_wait(
    op: *const weida_subscriber_unsubscribe_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases an unsubscribe op, aborting it when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live unsubscribe op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_unsubscribe_op_drop(
    op: *mut weida_subscriber_unsubscribe_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// States how many messages this subscriber accepts in total on `filter`
/// (`Subscriber::grant`) and sends it to every connected peer: the L2
/// credit a queue serving the path consults, absolute and cumulative; a
/// limit below the standing one is ignored. A publisher consults none. The
/// filter is copied.
///
/// Never blocks; the grant runs as the op written to `out`. A filter
/// outside the grammar fails its wait with `-WEIDA_EPROTOCOL`.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `filter` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_subscriber_grant_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_grant(
    s: *const weida_subscriber,
    filter: *const c_char,
    limit: u64,
    out: *mut *mut weida_subscriber_grant_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let filter = unsafe { cstr(filter) }?.to_owned();
        let subscriber = Arc::clone(&s.subscriber);
        let op = Op::start(
            &s.exec,
            async move { subscriber.grant(&filter, limit).await },
        );
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_subscriber_grant_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a grant op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live grant op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_grant_op_fd(
    op: *const weida_subscriber_grant_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the grant: 0 once every connected peer
/// was told.
///
/// # Safety
///
/// `op` is NULL or a live grant op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_grant_op_wait(
    op: *const weida_subscriber_grant_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a grant op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live grant op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_grant_op_drop(op: *mut weida_subscriber_grant_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The filters this subscriber registered (`Subscriber::filter_count`); 0
/// for NULL.
///
/// # Safety
///
/// `s` is NULL or a live subscriber.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_filter_count(s: *const weida_subscriber) -> usize {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live subscriber by this function's
        // contract.
        unsafe { &*s }.subscriber.filter_count()
    })
}

/// The subscriber's readiness descriptor (R11): readable when the next
/// `weida_subscriber_recv` will not block.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_fd(s: *const weida_subscriber, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let fd = s.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next published copy, waiting up to `timeout_ms` (R4) for one
/// (`Subscriber::recv`). Its topic is `weida_incoming_meta_topic`.
/// `-WEIDA_ENOT_CONNECTED` once the queue ended.
///
/// # Safety
///
/// `s` is NULL or a live subscriber; `out` is NULL or writable. Release the
/// transfer with `weida_incoming_transfer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_recv(
    s: *const weida_subscriber,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let received = s.tried.attempt(timeout_ms, |receiving, cx| {
            let pending = receiving.get_or_insert_with(|| {
                let subscriber = Arc::clone(&s.subscriber);
                Box::pin(async move { subscriber.recv().await })
            });
            let polled = pending.as_mut().poll(cx);
            if polled.is_ready() {
                *receiving = None;
            }
            polled
        })?;
        let transfer = received.map_err(fail)?;
        let handle = weida_incoming_transfer::handle(transfer, s.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a subscriber, which tells its peers it is gone and drops its
/// connections. A thread waiting in `weida_subscriber_recv` returns
/// `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live subscriber, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_subscriber_drop(s: *mut weida_subscriber) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(s) = unsafe { release(s) } {
            drop(s.tried.close());
        }
    });
}
