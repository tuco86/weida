//! The two halves of RADIO/DISH: segments fanned out without waiting, what a
//! dish receives, and drops by dish (docs/c/radio.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use ::weida_runtime::Exec;
use bytes::Bytes;
use weida_rs::{
    Dish, DishDrops, Error, Fingerprint, IncomingTransfer, JoinTerms, PeerIdentity, Radio,
    Received, SegmentTerms,
};

use crate::config::{Builder, weida_join_terms, weida_segment_terms};
use crate::error::{WEIDA_EINVAL, closed, fail, fail_abi, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::pubsub::weida_topic_drops;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::segment::weida_segment;
use crate::stats::{weida_connection_stats, weida_peer_events};
use crate::tls::weida_client_tls;
use crate::transfer::weida_incoming_transfer;
use crate::try_handle::Tried;

/// `Received::Segment`.
pub const WEIDA_RECEIVED_SEGMENT: c_int = 0;
/// `Received::Datagram`.
pub const WEIDA_RECEIVED_DATAGRAM: c_int = 1;

/// The sending half of RADIO/DISH, serving one endpoint path.
pub struct weida_radio {
    radio: Radio,
    path: CText,
}

/// A snapshot of what a radio dropped toward each joined dish
/// (`DishDrops`).
pub struct weida_dish_drops {
    records: Vec<DishDrops>,
    peers: Vec<Option<CText>>,
}

impl weida_dish_drops {
    fn handle(records: Vec<DishDrops>) -> *mut weida_dish_drops {
        let peers = records
            .iter()
            .map(|r| match &r.peer {
                Some(PeerIdentity::Key(fp)) => Some(CText::new(fp.to_string())),
                _ => None,
            })
            .collect();
        into_handle(weida_dish_drops { records, peers })
    }
}

/// A dish's recv in flight, kept across tries (R3a).
type Receiving = Pin<Box<dyn Future<Output = Result<Received, Error>> + Send>>;

/// The receiving half of RADIO/DISH. The recv in flight is kept across
/// tries, never recreated (R3a).
pub struct weida_dish {
    dish: Arc<Dish>,
    tried: Tried<Option<Receiving>>,
    exec: Exec,
}

/// The pending dial of `weida_dish_connect`.
pub struct weida_dish_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_dish_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending join of `weida_dish_join`.
pub struct weida_dish_join_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_dish_join_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending leave of `weida_dish_leave`.
pub struct weida_dish_leave_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_dish_leave_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// One thing a dish received (`Received`), as a tagged handle: a stream
/// segment's transfer until it is taken, or a datagram's fields.
pub struct weida_received {
    kind: c_int,
    transfer: Mutex<Option<IncomingTransfer>>,
    topic: Option<CText>,
    segment: Option<u64>,
    payload: Option<Bytes>,
    exec: Exec,
}

impl weida_received {
    fn handle(received: Received, exec: Exec) -> *mut weida_received {
        let value = match received {
            Received::Segment(transfer) => weida_received {
                kind: WEIDA_RECEIVED_SEGMENT,
                transfer: Mutex::new(Some(transfer)),
                topic: None,
                segment: None,
                payload: None,
                exec,
            },
            Received::Datagram {
                topic,
                segment,
                payload,
            } => weida_received {
                kind: WEIDA_RECEIVED_DATAGRAM,
                transfer: Mutex::new(None),
                topic: Some(CText::new(topic.as_bytes())),
                segment: Some(segment),
                payload: Some(payload),
                exec,
            },
        };
        into_handle(value)
    }
}

// --- weida_radio -----------------------------------------------------------

/// Registers a radio at the endpoint path `path` (`Listener::radio`).
/// Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`. The path is released when the handle is
/// dropped.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_radio_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_radio(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_radio,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let radio = l.listener.radio(path).map_err(fail)?;
        let handle = into_handle(weida_radio {
            path: CText::new(radio.path()),
            radio,
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this radio serves. Lives as long as the radio.
///
/// # Safety
///
/// `r` is NULL or a live radio; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_path(
    r: *const weida_radio,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if r.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `r` is a live radio by this function's
        // contract; the text lives as long as it.
        let r = unsafe { &*r };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&r.path), len_out) }
    })
}

/// Opens the next segment on `topic` (`Radio::segment`), copied to every
/// dish joined to it at this call and written with the `weida_segment_*`
/// calls. NULL `terms` are `SegmentTerms::default()`. Opening it resets
/// every unfinished layer of the topic's previous segment, counted
/// `superseded`. Never blocks.
///
/// Fails with `-WEIDA_ELIMIT_EXCEEDED` for a topic above 256 bytes, or when
/// `max_sequence_scopes` topics all have a copy in flight.
///
/// # Safety
///
/// `r` is NULL or a live radio; `topic` is NULL or a NUL-terminated
/// string; `terms` is NULL or live terms; `out` is NULL or writable.
/// Release the result with `weida_segment_finish` or `weida_segment_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_segment(
    r: *const weida_radio,
    topic: *const c_char,
    terms: *const weida_segment_terms,
    out: *mut *mut weida_segment,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        let terms = if terms.is_null() {
            SegmentTerms::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(terms) }?.get()
        };
        let segment = r.radio.segment(topic, terms).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_segment::handle(segment)) };
        Ok(0)
    })
}

/// Sends `payload` as one datagram segment to every dish joined to `topic`
/// (`Radio::datagram`) and writes how many dishes it was handed to. The
/// payload is copied. Never blocks: a dish whose connection carries no
/// datagrams is a `no_datagrams` drop, one whose connection carries less
/// a `too_large` drop.
///
/// Fails with `-WEIDA_ELIMIT_EXCEEDED` for a topic above 256 bytes.
///
/// # Safety
///
/// `r` is NULL or a live radio; `topic` is NULL or a NUL-terminated
/// string; `payload` is NULL only with `len == 0`, else points to `len`
/// readable bytes; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_datagram(
    r: *const weida_radio,
    topic: *const c_char,
    payload: *const u8,
    len: usize,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        // SAFETY: forwarded from this function's contract.
        let payload = unsafe { bytes(payload, len) }?;
        let handed = r.radio.datagram(topic, payload).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handed) };
        Ok(0)
    })
}

/// The dishes currently joined (`Radio::dish_count`); 0 for NULL.
///
/// # Safety
///
/// `r` is NULL or a live radio.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_dish_count(r: *const weida_radio) -> usize {
    guard_or(0, || {
        if r.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `r` is a live radio by this function's
        // contract.
        unsafe { &*r }.radio.dish_count()
    })
}

/// The copies dropped, summed over topics and causes (`Radio::dropped`); 0
/// for NULL.
///
/// # Safety
///
/// `r` is NULL or a live radio.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_dropped(r: *const weida_radio) -> u64 {
    guard_or(0, || {
        if r.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `r` is a live radio by this function's
        // contract.
        unsafe { &*r }.radio.dropped()
    })
}

/// What was dropped on `topic`, by cause (`Radio::dropped_on`): a snapshot
/// of one record, or of none when nothing was. Never blocks.
///
/// # Safety
///
/// `r` is NULL or a live radio; `topic` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the snapshot with
/// `weida_topic_drops_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_dropped_on(
    r: *const weida_radio,
    topic: *const c_char,
    out: *mut *mut weida_topic_drops,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        let records = r.radio.dropped_on(topic).into_iter().collect();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_topic_drops::handle(records)) };
        Ok(0)
    })
}

/// Every topic that lost a copy, with its counts by cause (`Radio::drops`):
/// a snapshot read with the `weida_topic_drops_*` getters. Never blocks.
///
/// # Safety
///
/// `r` is NULL or a live radio; `out` is NULL or writable. Release the
/// snapshot with `weida_topic_drops_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_drops(
    r: *const weida_radio,
    out: *mut *mut weida_topic_drops,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let handle = weida_topic_drops::handle(r.radio.drops());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Withdraws the joins on `filter` of every dish connection whose key is
/// `peer` (`sha256:` text) and frees their subscription slots
/// (`Radio::evict`); writes how many joins went. The dish is not told and
/// may join again. `-WEIDA_EINVALID_FINGERPRINT` for text that is not a
/// key.
///
/// # Safety
///
/// `r` is NULL or a live radio; `peer` and `filter` are NULL or
/// NUL-terminated strings; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_evict(
    r: *const weida_radio,
    peer: *const c_char,
    filter: *const c_char,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // SAFETY: forwarded from this function's contract.
        let text = unsafe { cstr(peer) }?;
        // SAFETY: forwarded from this function's contract.
        let filter = unsafe { cstr(filter) }?;
        let fp: Fingerprint = text.parse().map_err(fail)?;
        let evicted = r.radio.evict(&PeerIdentity::Key(fp), filter);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, evicted) };
        Ok(0)
    })
}

/// What the radio dropped toward each joined dish connection
/// (`Radio::dish_drops`): a snapshot read with the `weida_dish_drops_*`
/// getters. A record goes when its connection closes. Never blocks.
///
/// # Safety
///
/// `r` is NULL or a live radio; `out` is NULL or writable. Release the
/// snapshot with `weida_dish_drops_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_dish_drops(
    r: *const weida_radio,
    out: *mut *mut weida_dish_drops,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let handle = weida_dish_drops::handle(r.radio.dish_drops());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a radio, which releases its path. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live radio, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_radio_drop(r: *mut weida_radio) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(r) }));
}

// --- weida_dish_drops ------------------------------------------------------

/// Reads one count of record `index`: 1 and `*out` when present, 0 when
/// absent (R2): a NULL `d` or `out`, or an index past the end.
unsafe fn dish_drops_number(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
    pick: impl FnOnce(&DishDrops) -> u64,
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

/// The number of records, one per joined dish connection; 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_len(d: *const weida_dish_drops) -> usize {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live snapshot by the caller's contract.
        unsafe { &*d }.records.len()
    })
}

/// The key the dish of record `index` proved (`DishDrops::peer`), as
/// `sha256:` text; NULL past the end and for a dish that proved no key.
/// Lives as long as `d`.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_peer_fingerprint(
    d: *const weida_dish_drops,
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
        unsafe { text_out(d.peers.get(index).and_then(Option::as_ref), len_out) }
    })
}

/// `DishDrops::subscriber_budget` of record `index`, copies dropped for an
/// exhausted byte budget: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_subscriber_budget(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.subscriber_budget) }
}

/// `DishDrops::subscriber_queue` of record `index`, copies dropped for a
/// full queue: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_subscriber_queue(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.subscriber_queue) }
}

/// `DishDrops::superseded` of record `index`, copies reset because a newer
/// segment opened on their topic: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_superseded(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.superseded) }
}

/// `DishDrops::expired` of record `index`, copies reset because the
/// `max_age` passed: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_expired(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.expired) }
}

/// `DishDrops::too_large` of record `index`, datagram segments larger than
/// the dish's connection carries: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_too_large(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.too_large) }
}

/// `DishDrops::no_datagrams` of record `index`, datagram segments for a
/// dish that carries no datagrams: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_no_datagrams(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.no_datagrams) }
}

/// `DishDrops::layers_cut` of record `index`, segment copies that lost
/// layers above 0 and kept layer 0: 1 and `*out`, 0 past the end.
///
/// # Safety
///
/// `d` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_layers_cut(
    d: *const weida_dish_drops,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { dish_drops_number(d, index, out, |r| r.layers_cut) }
}

/// Releases a dish-drops snapshot. NULL does nothing.
///
/// # Safety
///
/// `d` is NULL or a live snapshot, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drops_drop(d: *mut weida_dish_drops) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(d) }));
}

// --- weida_dish ------------------------------------------------------------

/// Creates a dish that dials on `tls`'s terms (`Runtime::dish`). Two dishes
/// on one runtime share the pooled connection, so their paths collide with
/// `-WEIDA_EALREADY_REGISTERED`: use one runtime per dish.
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with `weida_dish_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_dish(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_dish,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_dish {
            dish: Arc::new(rt.runtime.dish(tls.get())),
            tried: Tried::new(None),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials a radio at `url` (`Dish::connect`), e.g. `weida+inproc://bus/r`,
/// and joins there every filter joined so far. A redial joins again; what
/// was sent in between is gone, and the numbers start over.
///
/// Never blocks; the dial runs as the op written to `out`.
///
/// # Safety
///
/// `d` is NULL or a live dish; `url` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the op with
/// `weida_dish_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_connect(
    d: *const weida_dish,
    url: *const c_char,
    out: *mut *mut weida_dish_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let dish = Arc::clone(&d.dish);
        let op = Op::start(&d.exec, async move { dish.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_dish_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_connect_op_fd(
    op: *const weida_dish_connect_op,
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
pub unsafe extern "C" fn weida_dish_connect_op_wait(
    op: *const weida_dish_connect_op,
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
pub unsafe extern "C" fn weida_dish_connect_op_drop(op: *mut weida_dish_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The number of connected radios (`Dish::peer_count`); 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live dish.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_peer_count(d: *const weida_dish) -> usize {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live dish by this function's contract.
        unsafe { &*d }.dish.peer_count()
    })
}

/// A snapshot of the dish's live connections (`Dish::connection_stats`),
/// read with the `weida_connection_stats_*` getters. Never blocks.
///
/// # Safety
///
/// `d` is NULL or a live dish; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_connection_stats(
    d: *const weida_dish,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        let handle = weida_connection_stats::handle(d.dish.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// A new reader of the dish's peer events (`Dish::events`), read with
/// `weida_peer_events_recv`. Each call is an independent reader.
///
/// # Safety
///
/// `d` is NULL or a live dish; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_events(
    d: *const weida_dish,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        let handle = weida_peer_events::handle(d.dish.events());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Joins every topic `filter` matches (`Dish::join`), a segmented pattern
/// of PROTOCOL.md §6.4, on `terms` (NULL is `JoinTerms::default()`).
/// Joining a filter again replaces its terms. The filter and the terms are
/// copied.
///
/// Never blocks; the join runs as the op written to `out`. Its wait fails
/// with `-WEIDA_ELIMIT_EXCEEDED` for a `max_layer` of
/// `WEIDA_MAX_SEGMENT_LAYERS` or above and with `-WEIDA_EPROTOCOL` for a
/// filter outside the grammar.
///
/// # Safety
///
/// `d` is NULL or a live dish; `filter` is NULL or a NUL-terminated
/// string; `terms` is NULL or live terms; `out` is NULL or writable.
/// Release the op with `weida_dish_join_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_join(
    d: *const weida_dish,
    filter: *const c_char,
    terms: *const weida_join_terms,
    out: *mut *mut weida_dish_join_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        // SAFETY: forwarded from this function's contract.
        let filter = unsafe { cstr(filter) }?.to_owned();
        let terms = if terms.is_null() {
            JoinTerms::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(terms) }?.get()
        };
        let dish = Arc::clone(&d.dish);
        let op = Op::start(&d.exec, async move { dish.join(&filter, terms).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_dish_join_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a join op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live join op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_join_op_fd(
    op: *const weida_dish_join_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the join: 0 once every connected radio
/// was told.
///
/// # Safety
///
/// `op` is NULL or a live join op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_join_op_wait(
    op: *const weida_dish_join_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a join op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live join op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_join_op_drop(op: *mut weida_dish_join_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Leaves a filter (`Dish::leave`); an unknown one is ignored. The filter
/// is copied.
///
/// Never blocks; the leave runs as the op written to `out`.
///
/// # Safety
///
/// `d` is NULL or a live dish; `filter` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_dish_leave_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_leave(
    d: *const weida_dish,
    filter: *const c_char,
    out: *mut *mut weida_dish_leave_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        // SAFETY: forwarded from this function's contract.
        let filter = unsafe { cstr(filter) }?.to_owned();
        let dish = Arc::clone(&d.dish);
        let op = Op::start(&d.exec, async move { dish.leave(&filter).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_dish_leave_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a leave op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live leave op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_leave_op_fd(
    op: *const weida_dish_leave_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the leave: 0 once every connected
/// radio was told.
///
/// # Safety
///
/// `op` is NULL or a live leave op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_leave_op_wait(
    op: *const weida_dish_leave_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a leave op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live leave op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_leave_op_drop(op: *mut weida_dish_leave_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The dish's readiness descriptor (R11): readable when the next
/// `weida_dish_recv` will not block.
///
/// # Safety
///
/// `d` is NULL or a live dish; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_fd(d: *const weida_dish, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        let fd = d.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next segment the dish received, waiting up to `timeout_ms`
/// (R4) for one (`Dish::recv`), as a `weida_received`.
/// `-WEIDA_ENOT_CONNECTED` once the queue ended.
///
/// # Safety
///
/// `d` is NULL or a live dish; `out` is NULL or writable. Release the
/// result with `weida_received_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_recv(
    d: *const weida_dish,
    timeout_ms: i64,
    out: *mut *mut weida_received,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let d = unsafe { borrow(d) }?;
        let received = d.tried.attempt(timeout_ms, |receiving, cx| {
            let pending = receiving.get_or_insert_with(|| {
                let dish = Arc::clone(&d.dish);
                Box::pin(async move { dish.recv().await })
            });
            let polled = pending.as_mut().poll(cx);
            if polled.is_ready() {
                *receiving = None;
            }
            polled
        })?;
        let received = received.map_err(fail)?;
        let handle = weida_received::handle(received, d.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Segments discarded on arrival because a newer one on their topic had
/// already been delivered (`Dish::stale`); 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live dish.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_stale(d: *const weida_dish) -> u64 {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live dish by this function's contract.
        unsafe { &*d }.dish.stale()
    })
}

/// Segments discarded on arrival because the receive queue was full
/// (`Dish::overflow`); 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live dish.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_overflow(d: *const weida_dish) -> u64 {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live dish by this function's contract.
        unsafe { &*d }.dish.overflow()
    })
}

/// Releases a dish, which drops its connections. A thread waiting in
/// `weida_dish_recv` returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `d` is NULL or a live dish, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_dish_drop(d: *mut weida_dish) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(d) = unsafe { release(d) } {
            drop(d.tried.close());
        }
    });
}

// --- weida_received --------------------------------------------------------

/// What the dish received, a `WEIDA_RECEIVED_*` constant.
///
/// # Safety
///
/// `r` is NULL or a live received segment.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_received_kind(r: *const weida_received) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        Ok(unsafe { borrow(r) }?.kind)
    })
}

/// Hands a stream segment's transfer over, once: its topic, number and
/// layer are on its metadata. A second call is `-WEIDA_ECLOSED`, a datagram
/// `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `r` is NULL or a live received segment; `out` is NULL or writable.
/// Release the transfer with `weida_incoming_transfer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_received_take_segment(
    r: *const weida_received,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        if r.kind == WEIDA_RECEIVED_DATAGRAM {
            return Err(fail_abi(
                WEIDA_EINVAL,
                c"a datagram segment carries no transfer",
            ));
        }
        let transfer = r
            .transfer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(closed)?;
        let handle = weida_incoming_transfer::handle(transfer, r.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The topic of a datagram segment; NULL for a stream segment, whose topic
/// is on its metadata. Lives as long as `r`.
///
/// # Safety
///
/// `r` is NULL or a live received segment; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_received_topic(
    r: *const weida_received,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if r.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `r` is live by this function's contract; the
        // text lives as long as it.
        let r = unsafe { &*r };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(r.topic.as_ref(), len_out) }
    })
}

/// The number of a datagram segment on its topic: 1 and `*out`; 0 for a
/// stream segment, whose number is on its metadata.
///
/// # Safety
///
/// `r` is NULL or a live received segment; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_received_segment(r: *const weida_received, out: *mut u64) -> c_int {
    guard_or(0, || {
        if r.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `r` is live by this function's contract.
        match unsafe { &*r }.segment {
            Some(v) => {
                // SAFETY: a non-NULL `out` is writable by this function's
                // contract.
                unsafe { out.write(v) };
                1
            }
            None => 0,
        }
    })
}

/// The payload of a datagram segment, with its length in `*len_out`; NULL
/// for a stream segment. Valid as long as `r`.
///
/// # Safety
///
/// `r` is NULL or a live received segment; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_received_payload(
    r: *const weida_received,
    len_out: *mut usize,
) -> *const u8 {
    guard_ptr(|| {
        if r.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `r` is live by this function's contract; the
        // payload lives in it.
        let r = unsafe { &*r };
        let Some(payload) = r.payload.as_ref() else {
            return ptr::null();
        };
        if !len_out.is_null() {
            // SAFETY: a non-NULL `len_out` is writable by this function's
            // contract.
            unsafe { len_out.write(payload.len()) };
        }
        payload.as_ptr()
    })
}

/// Releases a received segment. A stream segment's transfer that was not
/// taken is dropped, which refuses it with `REJECTED`. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live received segment, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_received_drop(r: *mut weida_received) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(r) }));
}
