//! Nested configuration values and the enumeration constants (docs/c/runtime.md, R7).

use std::ffi::{c_char, c_int};
use std::sync::Mutex;
use std::time::Duration;

use weida_rs::{
    Acknowledgement, Alpn, Backpressure, Congestion, Deduplication, DeliveryLevel, Discovery,
    Durability, FilesOptions, GuaranteeSet, JoinTerms, Limits, OrderingMode, OutboxFull,
    ProducerNaming, ReconnectPolicy, SegmentTerms,
};

use crate::error::{WEIDA_EINVAL, fail_abi, guard, guard_void};
use crate::handle::{borrow, into_handle, out_ptr, release, write_out};

/// `Congestion::Cubic`.
pub const WEIDA_CONGESTION_CUBIC: c_int = 0;
/// `Congestion::NewReno`.
pub const WEIDA_CONGESTION_NEW_RENO: c_int = 1;
/// `Congestion::Bbr`.
pub const WEIDA_CONGESTION_BBR: c_int = 2;

/// `Discovery::Aware`.
pub const WEIDA_DISCOVERY_AWARE: c_int = 0;
/// `Discovery::Single`.
pub const WEIDA_DISCOVERY_SINGLE: c_int = 1;

/// `OutboxFull::Block`.
pub const WEIDA_OUTBOX_FULL_BLOCK: c_int = 0;
/// `OutboxFull::Drop`.
pub const WEIDA_OUTBOX_FULL_DROP: c_int = 1;
/// `OutboxFull::Reject`.
pub const WEIDA_OUTBOX_FULL_REJECT: c_int = 2;

/// `DeliveryLevel::BestEffort`.
pub const WEIDA_DELIVERY_LEVEL_BEST_EFFORT: c_int = 0;
/// `DeliveryLevel::AtMostOnce`.
pub const WEIDA_DELIVERY_LEVEL_AT_MOST_ONCE: c_int = 1;
/// `DeliveryLevel::AtLeastOnce`.
pub const WEIDA_DELIVERY_LEVEL_AT_LEAST_ONCE: c_int = 2;

/// `Acknowledgement::None`.
pub const WEIDA_ACKNOWLEDGEMENT_NONE: c_int = 0;
/// `Acknowledgement::TransportReceipt`.
pub const WEIDA_ACKNOWLEDGEMENT_TRANSPORT_RECEIPT: c_int = 1;
/// `Acknowledgement::Accepted`.
pub const WEIDA_ACKNOWLEDGEMENT_ACCEPTED: c_int = 2;
/// `Acknowledgement::Stored`.
pub const WEIDA_ACKNOWLEDGEMENT_STORED: c_int = 3;
/// `Acknowledgement::Replicated`.
pub const WEIDA_ACKNOWLEDGEMENT_REPLICATED: c_int = 4;
/// `Acknowledgement::Processed`.
pub const WEIDA_ACKNOWLEDGEMENT_PROCESSED: c_int = 5;

/// `Durability::Written`.
pub const WEIDA_DURABILITY_WRITTEN: c_int = 0;
/// `Durability::Flushed`.
pub const WEIDA_DURABILITY_FLUSHED: c_int = 1;

/// `OrderingMode::None`.
pub const WEIDA_ORDERING_MODE_NONE: c_int = 0;
/// `OrderingMode::PerProducerDetect`.
pub const WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT: c_int = 1;
/// `OrderingMode::PerProducerReassemble`.
pub const WEIDA_ORDERING_MODE_PER_PRODUCER_REASSEMBLE: c_int = 2;
/// `OrderingMode::PerKey`.
pub const WEIDA_ORDERING_MODE_PER_KEY: c_int = 3;
/// `OrderingMode::Total`.
pub const WEIDA_ORDERING_MODE_TOTAL: c_int = 4;

/// `Deduplication::None`.
pub const WEIDA_DEDUPLICATION_NONE: c_int = 0;
/// `Deduplication::Bounded`.
pub const WEIDA_DEDUPLICATION_BOUNDED: c_int = 1;
/// `Deduplication::Durable`.
pub const WEIDA_DEDUPLICATION_DURABLE: c_int = 2;

/// `Backpressure::Block`.
pub const WEIDA_BACKPRESSURE_BLOCK: c_int = 0;
/// `Backpressure::Reject`.
pub const WEIDA_BACKPRESSURE_REJECT: c_int = 1;
/// `Backpressure::Drop`.
pub const WEIDA_BACKPRESSURE_DROP: c_int = 2;
/// `Backpressure::Spill`.
pub const WEIDA_BACKPRESSURE_SPILL: c_int = 3;
/// `Backpressure::Coalesce`.
pub const WEIDA_BACKPRESSURE_COALESCE: c_int = 4;

/// `ProducerNaming::Fingerprint`.
pub const WEIDA_PRODUCER_NAMING_FINGERPRINT: c_int = 0;
/// `ProducerNaming::Stable`.
pub const WEIDA_PRODUCER_NAMING_STABLE: c_int = 1;

/// `Alpn::Weida`: `weida/0`.
pub const WEIDA_ALPN_WEIDA: c_int = 0;
/// `Alpn::Raft`: `weida-raft/0`.
pub const WEIDA_ALPN_RAFT: c_int = 1;

/// `weida::DEFAULT_DATAGRAM_RECEIVE_BYTES`: the documented
/// `datagram_receive_bytes` of a profile that enables datagram flows.
pub const WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES: c_int = 65_536;

/// The length of a version-00 traceparent, without its NUL.
pub const WEIDA_TRACEPARENT_LEN: c_int = 55;

/// The length of a fingerprint's text, `sha256:` and 64 hex digits, without
/// its NUL.
pub const WEIDA_FINGERPRINT_LEN: c_int = 71;

pub(crate) fn invalid_enum() -> c_int {
    fail_abi(WEIDA_EINVAL, c"an enumeration argument was out of range")
}

pub(crate) fn out_of_range() -> c_int {
    fail_abi(WEIDA_EINVAL, c"an argument was out of range")
}

/// An enumeration whose constants are its wire values.
pub(crate) fn wire<T>(v: c_int, from: fn(u64) -> Option<T>) -> Result<T, c_int> {
    let v = u64::try_from(v).map_err(|_| invalid_enum())?;
    from(v).ok_or_else(invalid_enum)
}

pub(crate) fn congestion(v: c_int) -> Result<Congestion, c_int> {
    match v {
        WEIDA_CONGESTION_CUBIC => Ok(Congestion::Cubic),
        WEIDA_CONGESTION_NEW_RENO => Ok(Congestion::NewReno),
        WEIDA_CONGESTION_BBR => Ok(Congestion::Bbr),
        _ => Err(invalid_enum()),
    }
}

pub(crate) fn discovery(v: c_int) -> Result<Discovery, c_int> {
    match v {
        WEIDA_DISCOVERY_AWARE => Ok(Discovery::Aware),
        WEIDA_DISCOVERY_SINGLE => Ok(Discovery::Single),
        _ => Err(invalid_enum()),
    }
}

pub(crate) fn alpn(v: c_int) -> Result<Alpn, c_int> {
    match v {
        WEIDA_ALPN_WEIDA => Ok(Alpn::Weida),
        WEIDA_ALPN_RAFT => Ok(Alpn::Raft),
        _ => Err(invalid_enum()),
    }
}

pub(crate) fn outbox_full(v: c_int) -> Result<OutboxFull, c_int> {
    match v {
        WEIDA_OUTBOX_FULL_BLOCK => Ok(OutboxFull::Block),
        WEIDA_OUTBOX_FULL_DROP => Ok(OutboxFull::Drop),
        WEIDA_OUTBOX_FULL_REJECT => Ok(OutboxFull::Reject),
        _ => Err(invalid_enum()),
    }
}

/// A count in (R7): above `usize::MAX` it becomes `usize::MAX`.
pub(crate) fn count(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

/// A duration in (R7), in milliseconds.
pub(crate) fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

/// An optional value in (R7): negative is `None`.
pub(crate) fn optional(v: i64) -> Option<u64> {
    u64::try_from(v).ok()
}

/// A builder handle: a configuration value under a mutex.
pub(crate) trait Builder {
    type Value: Clone;
    fn cell(&self) -> &Mutex<Self::Value>;

    /// A copy of the value, taken by the call that hands it on.
    fn get(&self) -> Self::Value {
        self.cell()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// `Limits` under construction: one setter per field.
pub struct weida_limits {
    value: Mutex<Limits>,
}

/// `GuaranteeSet` under construction: one setter per dimension.
pub struct weida_guarantee_set {
    value: Mutex<GuaranteeSet>,
}

/// `ReconnectPolicy` under construction: one setter per field.
pub struct weida_reconnect_policy {
    value: Mutex<ReconnectPolicy>,
}

/// `FilesOptions` under construction: one setter per field.
pub struct weida_files_options {
    value: Mutex<FilesOptions>,
}

/// `JoinTerms` under construction: one setter per field.
pub struct weida_join_terms {
    value: Mutex<JoinTerms>,
}

/// `SegmentTerms` under construction: one setter per field.
pub struct weida_segment_terms {
    value: Mutex<SegmentTerms>,
}

impl Builder for weida_limits {
    type Value = Limits;
    fn cell(&self) -> &Mutex<Limits> {
        &self.value
    }
}

impl Builder for weida_guarantee_set {
    type Value = GuaranteeSet;
    fn cell(&self) -> &Mutex<GuaranteeSet> {
        &self.value
    }
}

impl Builder for weida_reconnect_policy {
    type Value = ReconnectPolicy;
    fn cell(&self) -> &Mutex<ReconnectPolicy> {
        &self.value
    }
}

impl Builder for weida_files_options {
    type Value = FilesOptions;
    fn cell(&self) -> &Mutex<FilesOptions> {
        &self.value
    }
}

impl Builder for weida_join_terms {
    type Value = JoinTerms;
    fn cell(&self) -> &Mutex<JoinTerms> {
        &self.value
    }
}

impl Builder for weida_segment_terms {
    type Value = SegmentTerms;
    fn cell(&self) -> &Mutex<SegmentTerms> {
        &self.value
    }
}

/// Writes a new handle holding `value` to `out`.
fn create<B>(out: *mut *mut B, value: B) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(value)) };
        Ok(0)
    })
}

/// Runs an infallible setter on a live builder.
///
/// # Safety
///
/// `h` is NULL or a live handle of type `B`.
unsafe fn update<B: Builder>(h: *const B, f: impl FnOnce(&mut B::Value)) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let h = unsafe { borrow(h) }?;
        f(&mut h.cell().lock().unwrap_or_else(|e| e.into_inner()));
        Ok(0)
    })
}

/// Runs a setter that converts its argument first and may refuse it: the
/// value is left as it was when `f` fails.
///
/// # Safety
///
/// `h` is NULL or a live handle of type `B`.
unsafe fn try_update<B: Builder>(
    h: *const B,
    f: impl FnOnce(&mut B::Value) -> Result<(), c_int>,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let h = unsafe { borrow(h) }?;
        let mut value = h.cell().lock().unwrap_or_else(|e| e.into_inner());
        let mut next = value.clone();
        f(&mut next)?;
        *value = next;
        Ok(0)
    })
}

// --- weida_limits ----------------------------------------------------------

/// Creates limits holding weida's defaults (`Limits::default`).
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with `weida_limits_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_new(out: *mut *mut weida_limits) -> c_int {
    create(
        out,
        weida_limits {
            value: Mutex::new(Limits::default()),
        },
    )
}

/// Releases limits. NULL does nothing.
///
/// # Safety
///
/// `l` is NULL or live limits, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_drop(l: *mut weida_limits) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(l) }));
}

/// Sets `Limits::max_header_bytes`: the largest frame header accepted, in
/// bytes.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_header_bytes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_header_bytes = v) }
}

/// Sets `Limits::max_concurrent_uni_streams`: QUIC inbound unidirectional
/// streams at once.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_concurrent_uni_streams(
    l: *const weida_limits,
    v: u32,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_concurrent_uni_streams = v) }
}

/// Sets `Limits::max_concurrent_bidi_streams`: QUIC inbound bidirectional
/// streams at once.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_concurrent_bidi_streams(
    l: *const weida_limits,
    v: u32,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_concurrent_bidi_streams = v) }
}

/// Sets `Limits::stream_receive_window`, in bytes. Above 2^62-1 the first
/// QUIC bind or dial fails with `-WEIDA_ERUNTIME`.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_stream_receive_window(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.stream_receive_window = v) }
}

/// Sets `Limits::connection_receive_window`, in bytes. Above 2^62-1 the
/// first QUIC bind or dial fails with `-WEIDA_ERUNTIME`.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_connection_receive_window(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.connection_receive_window = v) }
}

/// Sets `Limits::keep_alive`, in milliseconds: the dialling side's QUIC
/// keep-alive interval.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_keep_alive_ms(l: *const weida_limits, v: u64) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.keep_alive = ms(v)) }
}

/// Sets `Limits::idle_timeout`, in milliseconds. A value QUIC cannot carry
/// fails the first QUIC bind or dial with `-WEIDA_ERUNTIME`.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_idle_timeout_ms(l: *const weida_limits, v: u64) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.idle_timeout = ms(v)) }
}

/// Sets `Limits::flow_early_hold`, in milliseconds: how long a datagram
/// waits for its FLOW header.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_flow_early_hold_ms(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.flow_early_hold = ms(v)) }
}

/// Sets `Limits::hello_timeout_ms`: the wait for the peer's HELLO.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_hello_timeout_ms(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.hello_timeout_ms = v) }
}

/// Sets `Limits::header_timeout_ms`: the wait for one inbound stream's
/// preamble and then its header.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_header_timeout_ms(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.header_timeout_ms = v) }
}

/// Sets `Limits::max_subscriptions` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_subscriptions(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_subscriptions = count(v)) }
}

/// Sets `Limits::subscriber_buffer_bytes` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_subscriber_buffer_bytes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.subscriber_buffer_bytes = count(v)) }
}

/// Sets `Limits::max_sequence_scopes` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_sequence_scopes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_sequence_scopes = count(v)) }
}

/// Sets `Limits::max_reorder_hold` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_reorder_hold(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_reorder_hold = count(v)) }
}

/// Sets `Limits::max_local_streams` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_local_streams(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_local_streams = count(v)) }
}

/// Sets `Limits::max_parked_reverse` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_parked_reverse(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_parked_reverse = count(v)) }
}

/// Sets `Limits::max_dedup_entries` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_dedup_entries(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_dedup_entries = count(v)) }
}

/// Sets `Limits::datagram_receive_bytes` (a count, R7). 0 turns datagram
/// flows off; `WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES` is the documented value
/// for turning them on.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_datagram_receive_bytes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.datagram_receive_bytes = count(v)) }
}

/// Sets `Limits::datagram_send_bytes` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_datagram_send_bytes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.datagram_send_bytes = count(v)) }
}

/// Sets `Limits::max_flows` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_flows(l: *const weida_limits, v: u64) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_flows = count(v)) }
}

/// Sets `Limits::flow_queue_bytes` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_flow_queue_bytes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.flow_queue_bytes = count(v)) }
}

/// Sets `Limits::flow_early_bytes` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_flow_early_bytes(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.flow_early_bytes = count(v)) }
}

/// Sets `Limits::max_header_streams` (a count, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_max_header_streams(
    l: *const weida_limits,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.max_header_streams = count(v)) }
}

/// Sets `Limits::path_report` (a flag, R7).
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_path_report(l: *const weida_limits, v: c_int) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(l, |x| x.path_report = v != 0) }
}

/// Sets `Limits::congestion` to a `WEIDA_CONGESTION_*` constant; any other
/// value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `l` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_limits_set_congestion(l: *const weida_limits, v: c_int) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(l, |x| {
            x.congestion = congestion(v)?;
            Ok(())
        })
    }
}

// --- weida_guarantee_set ---------------------------------------------------

/// Creates a guarantee set holding `GuaranteeSet::CORE`.
///
/// No combination of dimensions is checked here, as in Rust: a set a peer
/// cannot meet fails the connect or the first request with
/// `-WEIDA_ENEGOTIATION`.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_guarantee_set_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_new(out: *mut *mut weida_guarantee_set) -> c_int {
    create(
        out,
        weida_guarantee_set {
            value: Mutex::new(GuaranteeSet::CORE),
        },
    )
}

/// Releases a guarantee set. NULL does nothing.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_drop(g: *mut weida_guarantee_set) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(g) }));
}

/// Sets `GuaranteeSet::delivery` to a `WEIDA_DELIVERY_LEVEL_*` constant; any
/// other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_delivery(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.delivery = wire(v, DeliveryLevel::from_wire)?;
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::acknowledgement` to a `WEIDA_ACKNOWLEDGEMENT_*` constant; any
/// other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_acknowledgement(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.acknowledgement = wire(v, Acknowledgement::from_wire)?;
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::durability` to a `WEIDA_DURABILITY_*` constant, or
/// to none for a negative value; any other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_durability(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.durability = if v < 0 {
                None
            } else {
                Some(wire(v, Durability::from_wire)?)
            };
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::replicas`, or none for a negative value (R7).
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_replicas(
    g: *const weida_guarantee_set,
    v: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(g, |x| x.replicas = optional(v)) }
}

/// Sets `GuaranteeSet::ordering` to a `WEIDA_ORDERING_MODE_*` constant; any
/// other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_ordering(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.ordering = wire(v, OrderingMode::from_wire)?;
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::deduplication` to a `WEIDA_DEDUPLICATION_*` constant; any
/// other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_deduplication(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.deduplication = wire(v, Deduplication::from_wire)?;
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::dedup_window_ms`, or none for a negative value (R7).
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_dedup_window_ms(
    g: *const weida_guarantee_set,
    v: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(g, |x| x.dedup_window_ms = optional(v)) }
}

/// Sets `GuaranteeSet::backpressure` to a `WEIDA_BACKPRESSURE_*` constant; any
/// other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_backpressure(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.backpressure = wire(v, Backpressure::from_wire)?;
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::producer_naming` to a `WEIDA_PRODUCER_NAMING_*` constant; any
/// other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_producer_naming(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(g, |x| {
            x.producer_naming = wire(v, ProducerNaming::from_wire)?;
            Ok(())
        })
    }
}

/// Sets `GuaranteeSet::control_isolated` (a flag, R7).
///
/// # Safety
///
/// `g` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_guarantee_set_set_control_isolated(
    g: *const weida_guarantee_set,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(g, |x| x.control_isolated = v != 0) }
}

// --- weida_reconnect_policy ------------------------------------------------

/// Creates a redial policy holding weida's default
/// (`ReconnectPolicy::default`): doubling from 100 ms to 30 s with jitter,
/// never giving up.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_reconnect_policy_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_new(
    out: *mut *mut weida_reconnect_policy,
) -> c_int {
    create(
        out,
        weida_reconnect_policy {
            value: Mutex::new(ReconnectPolicy::default()),
        },
    )
}

/// Creates a policy that never redials (`ReconnectPolicy::never`): a lost
/// peer fails the next operation with its loss cause.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_reconnect_policy_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_never(
    out: *mut *mut weida_reconnect_policy,
) -> c_int {
    create(
        out,
        weida_reconnect_policy {
            value: Mutex::new(ReconnectPolicy::never()),
        },
    )
}

/// Releases a redial policy. NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live policy, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_drop(p: *mut weida_reconnect_policy) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(p) }));
}

/// Sets `ReconnectPolicy::initial`, in milliseconds: the delay before the
/// first redial.
///
/// # Safety
///
/// `p` is NULL or a live policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_set_initial_ms(
    p: *const weida_reconnect_policy,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(p, |x| x.initial = ms(v)) }
}

/// Sets `ReconnectPolicy::max`, in milliseconds: the ceiling on the delay,
/// and how long a connection must live to earn a fresh schedule.
///
/// # Safety
///
/// `p` is NULL or a live policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_set_max_ms(
    p: *const weida_reconnect_policy,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(p, |x| x.max = ms(v)) }
}

/// Sets `ReconnectPolicy::jitter` (a flag, R7).
///
/// # Safety
///
/// `p` is NULL or a live policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_set_jitter(
    p: *const weida_reconnect_policy,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(p, |x| x.jitter = v != 0) }
}

/// Sets `ReconnectPolicy::max_attempts`, or none (redial for as long as the
/// endpoint lives) for a negative value. Above `UINT32_MAX` it is
/// `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `p` is NULL or a live policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_set_max_attempts(
    p: *const weida_reconnect_policy,
    v: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(p, |x| {
            x.max_attempts = match optional(v) {
                None => None,
                Some(n) => Some(u32::try_from(n).map_err(|_| out_of_range())?),
            };
            Ok(())
        })
    }
}

/// Sets `ReconnectPolicy::stop_on_peer_closed` (a flag, R7).
///
/// # Safety
///
/// `p` is NULL or a live policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reconnect_policy_set_stop_on_peer_closed(
    p: *const weida_reconnect_policy,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(p, |x| x.stop_on_peer_closed = v != 0) }
}

// --- weida_files_options ---------------------------------------------------

/// Creates file-source options holding weida's defaults
/// (`FilesOptions::default`): no names, a check every 10 s.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_files_options_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_files_options_new(out: *mut *mut weida_files_options) -> c_int {
    create(
        out,
        weida_files_options {
            value: Mutex::new(FilesOptions::default()),
        },
    )
}

/// Releases file-source options. NULL does nothing.
///
/// # Safety
///
/// `o` is NULL or live options, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_files_options_drop(o: *mut weida_files_options) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(o) }));
}

/// Sets `FilesOptions::names`, replacing the list: the names the
/// bootstrapped certificate is made for, when the directory is empty.
/// Unused when files exist. Fails with `-WEIDA_EINVAL`, leaving the list
/// as it was, when `names` is NULL with `n > 0`, an element is NULL or a
/// name is not UTF-8.
///
/// # Safety
///
/// `o` is NULL or live options; `names` is NULL only with `n == 0`, else
/// points to `n` readable pointers, each NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_files_options_set_names(
    o: *const weida_files_options,
    names: *const *const c_char,
    n: usize,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(o, |x| {
            x.names = crate::handle::names(names, n)?;
            Ok(())
        })
    }
}

/// Sets `FilesOptions::poll`, in milliseconds: how often the files'
/// modification times are checked, at the next use of the identity after
/// the interval; 0 checks at every use.
///
/// # Safety
///
/// `o` is NULL or live options.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_files_options_set_poll_ms(
    o: *const weida_files_options,
    v: u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(o, |x| x.poll = ms(v)) }
}

// --- weida_join_terms ------------------------------------------------------

/// Creates join terms holding weida's defaults (`JoinTerms::default`): no
/// latency budget, every layer.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_join_terms_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_join_terms_new(out: *mut *mut weida_join_terms) -> c_int {
    create(
        out,
        weida_join_terms {
            value: Mutex::new(JoinTerms::default()),
        },
    )
}

/// Releases join terms. NULL does nothing.
///
/// # Safety
///
/// `t` is NULL or live terms, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_join_terms_drop(t: *mut weida_join_terms) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(t) }));
}

/// Sets `JoinTerms::max_age`, in milliseconds, or none for a negative
/// value: a copy still unfinished this long after its segment opened is
/// reset at the radio.
///
/// # Safety
///
/// `t` is NULL or live terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_join_terms_set_max_age_ms(
    t: *const weida_join_terms,
    v: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(t, |x| x.max_age = optional(v).map(ms)) }
}

/// Sets `JoinTerms::max_layer`, the highest layer the dish takes, or none
/// (every layer) for a negative value. Above 255 it is `-WEIDA_EINVAL`; 16
/// to 255 are kept and fail the join with `-WEIDA_ELIMIT_EXCEEDED`, as in
/// Rust.
///
/// # Safety
///
/// `t` is NULL or live terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_join_terms_set_max_layer(
    t: *const weida_join_terms,
    v: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(t, |x| {
            x.max_layer = match optional(v) {
                None => None,
                Some(n) => Some(u8::try_from(n).map_err(|_| out_of_range())?),
            };
            Ok(())
        })
    }
}

// --- weida_segment_terms ---------------------------------------------------

/// Creates segment terms holding weida's defaults (`SegmentTerms::default`):
/// no latency budget, priority 0, not following an upstream.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_segment_terms_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_terms_new(out: *mut *mut weida_segment_terms) -> c_int {
    create(
        out,
        weida_segment_terms {
            value: Mutex::new(SegmentTerms::default()),
        },
    )
}

/// Releases segment terms. NULL does nothing.
///
/// # Safety
///
/// `t` is NULL or live terms, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_terms_drop(t: *mut weida_segment_terms) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(t) }));
}

/// Sets `SegmentTerms::max_age`, in milliseconds, or none for a negative
/// value: every copy still unfinished this long after the segment opened
/// is reset.
///
/// # Safety
///
/// `t` is NULL or live terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_terms_set_max_age_ms(
    t: *const weida_segment_terms,
    v: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(t, |x| x.max_age = optional(v).map(ms)) }
}

/// Sets `SegmentTerms::priority`: higher goes first against other topics'
/// copies of the same layer on one connection. Outside `int16_t` it is
/// `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `t` is NULL or live terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_terms_set_priority(
    t: *const weida_segment_terms,
    v: i32,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        try_update(t, |x| {
            x.priority = i16::try_from(v).map_err(|_| out_of_range())?;
            Ok(())
        })
    }
}

/// Sets `SegmentTerms::follows_upstream` (a flag, R7): the writer relays a
/// segment its upstream is still streaming.
///
/// # Safety
///
/// `t` is NULL or live terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_segment_terms_set_follows_upstream(
    t: *const weida_segment_terms,
    v: c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { update(t, |x| x.follows_upstream = v != 0) }
}
