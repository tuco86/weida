//! Connection statistics and peer events (docs/c/stats.md).

use std::ffi::{c_char, c_int};
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::task::Poll;
use std::time::Duration;

use weida_rs::{ConnectionStats, GiveUp, PathStats, PeerEvent, PeerEvents, PeerIdentity};

use crate::error::{guard, guard_or, guard_ptr, guard_void, loss_code};
use crate::handle::{CText, borrow, into_handle, out_ptr, release, text_out, write_out};
use crate::try_handle::Tried;

/// `PeerEvent::Connected`.
pub const WEIDA_PEER_EVENT_CONNECTED: c_int = 0;
/// `PeerEvent::Lost`.
pub const WEIDA_PEER_EVENT_LOST: c_int = 1;
/// `PeerEvent::Retrying`.
pub const WEIDA_PEER_EVENT_RETRYING: c_int = 2;
/// `PeerEvent::GaveUp`.
pub const WEIDA_PEER_EVENT_GAVE_UP: c_int = 3;
/// `PeerEvent::Missed`.
pub const WEIDA_PEER_EVENT_MISSED: c_int = 4;

/// `GiveUp::Policy`.
pub const WEIDA_GIVE_UP_POLICY: c_int = 0;
/// `GiveUp::PeerChanged`.
pub const WEIDA_GIVE_UP_PEER_CHANGED: c_int = 1;
/// `GiveUp::Failed`.
pub const WEIDA_GIVE_UP_FAILED: c_int = 2;

/// A duration out, in microseconds, saturating (R7).
fn us(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

// --- weida_connection_stats ------------------------------------------------

/// One snapshot of a requester's live connections, read by index
/// (`Requester::connection_stats`).
pub struct weida_connection_stats {
    records: Vec<ConnectionStats>,
    urls: Vec<CText>,
}

impl weida_connection_stats {
    pub(crate) fn handle(records: Vec<ConnectionStats>) -> *mut weida_connection_stats {
        let urls = records
            .iter()
            .map(|r| CText::new(r.url.as_bytes()))
            .collect();
        into_handle(weida_connection_stats { records, urls })
    }
}

/// Reads one number of record `index`: 1 and `*out` when present, 0 when
/// absent (R2): a NULL `s` or `out`, an index past the end, or a value the
/// record does not carry.
unsafe fn stats_number(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
    pick: impl FnOnce(&ConnectionStats) -> Option<u64>,
) -> c_int {
    guard_or(0, || {
        if s.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live snapshot by the caller's contract.
        let s = unsafe { &*s };
        match s.records.get(index).and_then(pick) {
            Some(v) => {
                // SAFETY: a non-NULL `out` is writable by the caller's contract.
                unsafe { out.write(v) };
                1
            }
            None => 0,
        }
    })
}

/// The number of records, one per live dialled address in dial order; 0
/// for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_len(s: *const weida_connection_stats) -> usize {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live snapshot by the caller's contract.
        unsafe { &*s }.records.len()
    })
}

/// The URL record `index` was dialled as, exactly as given to
/// `weida_requester_connect`; NULL past the end. Lives as long as `s`.
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_url(
    s: *const weida_connection_stats,
    index: usize,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if s.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `s` is a live snapshot by the caller's
        // contract; the text lives as long as it.
        let s = unsafe { &*s };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(s.urls.get(index), len_out) }
    })
}

/// `ConnectionStats::age` of record `index`, in microseconds: 1 and `*out`
/// when present, 0 when absent (only past the end or for a NULL argument).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_age_us(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| Some(us(r.age))) }
}

/// `ConnectionStats::redials` of record `index`: 1 and `*out` when present, 0
/// when absent (only past the end or for a NULL argument).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_redials(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| Some(r.redials)) }
}

/// `ConnectionStats::transport.path.rtt` of record `index`, in microseconds:
/// 1 and `*out` when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_rtt_us(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| us(t.path.rtt))) }
}

/// `ConnectionStats::transport.path.min_rtt` of record `index`, in
/// microseconds: 1 and `*out` when present, 0 when absent (the transport is
/// local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_min_rtt_us(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| us(t.path.min_rtt))) }
}

/// `ConnectionStats::transport.path.cwnd` of record `index`: 1 and `*out`
/// when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_cwnd(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.path.cwnd)) }
}

/// `ConnectionStats::transport.path.congestion_events` of record `index`: 1
/// and `*out` when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_congestion_events(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        stats_number(s, index, out, |r| {
            r.transport.map(|t| t.path.congestion_events)
        })
    }
}

/// `ConnectionStats::transport.path.lost_packets` of record `index`: 1 and
/// `*out` when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_lost_packets(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.path.lost_packets)) }
}

/// `ConnectionStats::transport.path.lost_bytes` of record `index`: 1 and
/// `*out` when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_lost_bytes(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.path.lost_bytes)) }
}

/// `ConnectionStats::transport.path.sent_packets` of record `index`: 1 and
/// `*out` when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_sent_packets(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.path.sent_packets)) }
}

/// `ConnectionStats::transport.path.current_mtu` of record `index`: 1 and
/// `*out` when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_current_mtu(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        stats_number(s, index, out, |r| {
            r.transport.map(|t| u64::from(t.path.current_mtu))
        })
    }
}

/// `ConnectionStats::transport.path.max_datagram_size` of record `index`: 1
/// and `*out` when present, 0 when absent (the transport is local, or the
/// peer accepts no datagram).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_path_max_datagram_size(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        stats_number(s, index, out, |r| {
            r.transport
                .and_then(|t| t.path.max_datagram_size)
                .map(|n| n as u64)
        })
    }
}

/// `ConnectionStats::transport.tx.datagrams` of record `index`: 1 and `*out`
/// when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_tx_datagrams(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.tx.datagrams)) }
}

/// `ConnectionStats::transport.tx.bytes` of record `index`: 1 and `*out` when
/// present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_tx_bytes(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.tx.bytes)) }
}

/// `ConnectionStats::transport.rx.datagrams` of record `index`: 1 and `*out`
/// when present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_rx_datagrams(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.rx.datagrams)) }
}

/// `ConnectionStats::transport.rx.bytes` of record `index`: 1 and `*out` when
/// present, 0 when absent (the transport is local).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_transport_rx_bytes(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.transport.map(|t| t.rx.bytes)) }
}

/// `ConnectionStats::remote.rtt` of record `index`, in microseconds: 1 and
/// `*out` when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_rtt_us(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| us(m.rtt))) }
}

/// `ConnectionStats::remote.min_rtt` of record `index`, in microseconds: 1
/// and `*out` when present, 0 when absent (no record from the peer yet, a
/// local transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_min_rtt_us(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| us(m.min_rtt))) }
}

/// `ConnectionStats::remote.cwnd` of record `index`: 1 and `*out` when
/// present, 0 when absent (no record from the peer yet, a local transport, or
/// `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_cwnd(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.cwnd)) }
}

/// `ConnectionStats::remote.congestion_events` of record `index`: 1 and
/// `*out` when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_congestion_events(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.congestion_events)) }
}

/// `ConnectionStats::remote.lost_packets` of record `index`: 1 and `*out`
/// when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_lost_packets(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.lost_packets)) }
}

/// `ConnectionStats::remote.lost_bytes` of record `index`: 1 and `*out` when
/// present, 0 when absent (no record from the peer yet, a local transport, or
/// `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_lost_bytes(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.lost_bytes)) }
}

/// `ConnectionStats::remote.sent_packets` of record `index`: 1 and `*out`
/// when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_sent_packets(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.sent_packets)) }
}

/// `ConnectionStats::remote.current_mtu` of record `index`: 1 and `*out` when
/// present, 0 when absent (no record from the peer yet, a local transport, or
/// `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_current_mtu(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        stats_number(s, index, out, |r| {
            r.remote.map(|m| u64::from(m.current_mtu))
        })
    }
}

/// `ConnectionStats::remote.tx.datagrams` of record `index`: 1 and `*out`
/// when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_tx_datagrams(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.tx.datagrams)) }
}

/// `ConnectionStats::remote.tx.bytes` of record `index`: 1 and `*out` when
/// present, 0 when absent (no record from the peer yet, a local transport, or
/// `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_tx_bytes(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.tx.bytes)) }
}

/// `ConnectionStats::remote.rx.datagrams` of record `index`: 1 and `*out`
/// when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_rx_datagrams(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.rx.datagrams)) }
}

/// `ConnectionStats::remote.rx.bytes` of record `index`: 1 and `*out` when
/// present, 0 when absent (no record from the peer yet, a local transport, or
/// `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_rx_bytes(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| m.rx.bytes)) }
}

/// `ConnectionStats::remote.age` of record `index`, in microseconds: 1 and
/// `*out` when present, 0 when absent (no record from the peer yet, a local
/// transport, or `path_report` off on either side).
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_remote_age_us(
    s: *const weida_connection_stats,
    index: usize,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { stats_number(s, index, out, |r| r.remote.map(|m| us(m.age))) }
}

/// Releases a snapshot. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live snapshot, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_connection_stats_drop(s: *mut weida_connection_stats) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}

// --- weida_path_stats ------------------------------------------------------

/// One snapshot of the path under a flow (`PathStats`), read-only.
pub struct weida_path_stats {
    stats: PathStats,
}

impl weida_path_stats {
    pub(crate) fn handle(stats: PathStats) -> *mut weida_path_stats {
        into_handle(weida_path_stats { stats })
    }
}

/// Reads one number of a path snapshot; 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
unsafe fn path_number(s: *const weida_path_stats, pick: impl FnOnce(&PathStats) -> u64) -> u64 {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live snapshot by this function's
        // contract.
        pick(&unsafe { &*s }.stats)
    })
}

/// `PathStats::rtt`, the smoothed round-trip time, in microseconds; 0 for
/// NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_rtt_us(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| us(p.rtt)) }
}

/// `PathStats::min_rtt`, the smallest round-trip time seen on the path, in
/// microseconds; 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_min_rtt_us(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| us(p.min_rtt)) }
}

/// `PathStats::cwnd`, the congestion window in bytes; 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_cwnd(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| p.cwnd) }
}

/// `PathStats::congestion_events`: the congestion events the controller
/// reacted to; 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_congestion_events(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| p.congestion_events) }
}

/// `PathStats::lost_packets`: packets this side sent and declared lost; 0
/// for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_lost_packets(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| p.lost_packets) }
}

/// `PathStats::lost_bytes`: bytes this side sent and declared lost; 0 for
/// NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_lost_bytes(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| p.lost_bytes) }
}

/// `PathStats::sent_packets`: packets this side sent; 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_sent_packets(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| p.sent_packets) }
}

/// `PathStats::current_mtu`: the largest UDP payload the path carries now;
/// 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_current_mtu(s: *const weida_path_stats) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { path_number(s, |p| u64::from(p.current_mtu)) }
}

/// `PathStats::max_datagram_size`: the largest datagram the connection
/// carries now, the flow's prefix included. 1 and `*out` when present, 0
/// when the peer accepts no datagram or an argument is NULL.
///
/// # Safety
///
/// `s` is NULL or a live snapshot; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_max_datagram_size(
    s: *const weida_path_stats,
    out: *mut u64,
) -> c_int {
    guard_or(0, || {
        if s.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live snapshot by this function's
        // contract.
        match unsafe { &*s }.stats.max_datagram_size {
            Some(n) => {
                // SAFETY: a non-NULL `out` is writable by this function's
                // contract.
                unsafe { write_out(out, u64::try_from(n).unwrap_or(u64::MAX)) };
                1
            }
            None => 0,
        }
    })
}

/// Releases a path snapshot. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live snapshot, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_path_stats_drop(s: *mut weida_path_stats) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}

// --- weida_peer_events -----------------------------------------------------

type Receiving = Pin<Box<dyn Future<Output = (PeerEvents, Option<PeerEvent>)> + Send>>;

/// A reader's state between tries: the receive in flight owns the reader
/// and hands it back, so the future is kept, never recreated, and a
/// timeout or `EINTR` loses nothing (R3a).
struct EventsState {
    events: Option<PeerEvents>,
    receiving: Option<Receiving>,
}

/// One reader of a requester's peer events (`PeerEvents`).
pub struct weida_peer_events {
    tried: Tried<EventsState>,
}

impl weida_peer_events {
    pub(crate) fn handle(events: PeerEvents) -> *mut weida_peer_events {
        into_handle(weida_peer_events {
            tried: Tried::new(EventsState {
                events: Some(events),
                receiving: None,
            }),
        })
    }
}

/// The reader's readiness descriptor (R11): readable when the next
/// `weida_peer_events_recv` will not block.
///
/// # Safety
///
/// `e` is NULL or a live reader; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_events_fd(e: *const weida_peer_events, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let e = unsafe { borrow(e) }?;
        let fd = e.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next event, waiting up to `timeout_ms` (R4) for one
/// (`PeerEvents::recv`).
///
/// Writes NULL to `out` and returns 0 once the requester is gone: the end
/// of the stream, as a read's end is `*out_n == 0`. A reader that fell more
/// than 64 events behind gets one `WEIDA_PEER_EVENT_MISSED` event carrying
/// how many it lost.
///
/// # Safety
///
/// `e` is NULL or a live reader; `out` is NULL or writable. Release an
/// event with `weida_peer_event_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_events_recv(
    e: *const weida_peer_events,
    timeout_ms: i64,
    out: *mut *mut weida_peer_event,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let e = unsafe { borrow(e) }?;
        let event = e.tried.attempt(timeout_ms, |state, cx| {
            let receiving = state.receiving.get_or_insert_with(|| {
                let mut events = state
                    .events
                    .take()
                    .expect("the reader is held whenever no receive is in flight");
                Box::pin(async move {
                    let event = events.recv().await;
                    (events, event)
                })
            });
            match receiving.as_mut().poll(cx) {
                Poll::Ready((events, event)) => {
                    state.receiving = None;
                    state.events = Some(events);
                    Poll::Ready(event)
                }
                Poll::Pending => Poll::Pending,
            }
        })?;
        let handle = event.map_or(ptr::null_mut(), weida_peer_event::handle);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a reader. A thread waiting in `weida_peer_events_recv` returns
/// `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `e` is NULL or a live reader, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_events_drop(e: *mut weida_peer_events) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(e) = unsafe { release(e) } {
            drop(e.tried.close());
        }
    });
}

// --- weida_peer_event ------------------------------------------------------

/// One transition of one dialled address (`PeerEvent`), read-only.
pub struct weida_peer_event {
    event: PeerEvent,
    url: Option<CText>,
    peer_fingerprint: Option<CText>,
    why_presented: Option<CText>,
    why_message: Option<CText>,
}

impl weida_peer_event {
    pub(crate) fn handle(event: PeerEvent) -> *mut weida_peer_event {
        let url = match &event {
            PeerEvent::Connected { url, .. }
            | PeerEvent::Lost { url, .. }
            | PeerEvent::Retrying { url, .. }
            | PeerEvent::GaveUp { url, .. } => Some(CText::new(url.as_bytes())),
            PeerEvent::Missed(_) => None,
        };
        let peer_fingerprint = match &event {
            PeerEvent::Connected {
                peer: Some(PeerIdentity::Key(fp)),
                ..
            } => Some(CText::new(fp.to_string())),
            _ => None,
        };
        let why_presented = match &event {
            PeerEvent::GaveUp {
                why:
                    GiveUp::PeerChanged {
                        presented: Some(fp),
                    },
                ..
            } => Some(CText::new(fp.to_string())),
            _ => None,
        };
        let why_message = match &event {
            PeerEvent::GaveUp {
                why: GiveUp::Failed(m),
                ..
            } => Some(CText::new(m.as_str())),
            _ => None,
        };
        into_handle(weida_peer_event {
            event,
            url,
            peer_fingerprint,
            why_presented,
            why_message,
        })
    }
}

/// Hands one of the event's strings out (R7).
unsafe fn event_text(
    e: *const weida_peer_event,
    len_out: *mut usize,
    pick: impl FnOnce(&weida_peer_event) -> Option<&CText>,
) -> *const c_char {
    guard_ptr(|| {
        if e.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `e` is a live event by the caller's contract,
        // borrowed for this call only; the text lives as long as the handle.
        let e = unsafe { &*e };
        // SAFETY: forwarded from the caller's contract.
        unsafe { text_out(pick(e), len_out) }
    })
}

/// Reads one field of the event: 1 and `*out` when the event carries it, 0
/// otherwise (R2), also for a NULL `e` or `out`.
unsafe fn event_value<T>(
    e: *const weida_peer_event,
    out: *mut T,
    pick: impl FnOnce(&PeerEvent) -> Option<T>,
) -> c_int {
    guard_or(0, || {
        if e.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `e` is a live event by the caller's contract.
        let e = unsafe { &*e };
        match pick(&e.event) {
            Some(v) => {
                // SAFETY: a non-NULL `out` is writable by the caller's contract.
                unsafe { out.write(v) };
                1
            }
            None => 0,
        }
    })
}

/// The event's kind, a `WEIDA_PEER_EVENT_*` constant.
///
/// # Safety
///
/// `e` is NULL or a live event.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_kind(e: *const weida_peer_event) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let e = unsafe { borrow(e) }?;
        Ok(match e.event {
            PeerEvent::Connected { .. } => WEIDA_PEER_EVENT_CONNECTED,
            PeerEvent::Lost { .. } => WEIDA_PEER_EVENT_LOST,
            PeerEvent::Retrying { .. } => WEIDA_PEER_EVENT_RETRYING,
            PeerEvent::GaveUp { .. } => WEIDA_PEER_EVENT_GAVE_UP,
            PeerEvent::Missed(_) => WEIDA_PEER_EVENT_MISSED,
        })
    })
}

/// The address as dialled; NULL for `WEIDA_PEER_EVENT_MISSED`. Lives as
/// long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_url(
    e: *const weida_peer_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.url.as_ref()) }
}

/// `CONNECTED`: the fingerprint (`sha256:` text) the peer proved; NULL for
/// any other kind, an anonymous peer or a local principal. Lives as long as
/// `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_peer_fingerprint(
    e: *const weida_peer_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.peer_fingerprint.as_ref()) }
}

/// `LOST`: 1 and the `WEIDA_LOSS_*` cause; 0 for any other kind.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_cause(
    e: *const weida_peer_event,
    out: *mut c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        event_value(e, out, |e| match e {
            PeerEvent::Lost { cause, .. } => Some(loss_code(*cause)),
            _ => None,
        })
    }
}

/// `RETRYING`: 1 and which attempt since the last connection that stayed
/// up, from 1; 0 for any other kind.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_attempt(
    e: *const weida_peer_event,
    out: *mut u32,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        event_value(e, out, |e| match e {
            PeerEvent::Retrying { attempt, .. } => Some(*attempt),
            _ => None,
        })
    }
}

/// `RETRYING`: 1 and the wait before it, in microseconds; 0 for any other
/// kind.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_delay_us(
    e: *const weida_peer_event,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        event_value(e, out, |e| match e {
            PeerEvent::Retrying { delay, .. } => Some(us(*delay)),
            _ => None,
        })
    }
}

/// `GAVE_UP`: 1 and why, a `WEIDA_GIVE_UP_*` constant; 0 for any other
/// kind.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_why(
    e: *const weida_peer_event,
    out: *mut c_int,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        event_value(e, out, |e| match e {
            PeerEvent::GaveUp { why, .. } => Some(match why {
                GiveUp::Policy { .. } => WEIDA_GIVE_UP_POLICY,
                GiveUp::PeerChanged { .. } => WEIDA_GIVE_UP_PEER_CHANGED,
                GiveUp::Failed(_) => WEIDA_GIVE_UP_FAILED,
            }),
            _ => None,
        })
    }
}

/// `GAVE_UP` for `POLICY`: 1 and the redials attempted after the loss; 0
/// for any other event.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_why_attempts(
    e: *const weida_peer_event,
    out: *mut u32,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        event_value(e, out, |e| match e {
            PeerEvent::GaveUp {
                why: GiveUp::Policy { attempts },
                ..
            } => Some(*attempts),
            _ => None,
        })
    }
}

/// `GAVE_UP` for `PEER_CHANGED`: the fingerprint that answered instead;
/// NULL when it proved none, and for any other event. Lives as long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_why_presented(
    e: *const weida_peer_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.why_presented.as_ref()) }
}

/// `GAVE_UP` for `FAILED`: why the redial could not be used; NULL for any
/// other event. Lives as long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_why_message(
    e: *const weida_peer_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.why_message.as_ref()) }
}

/// `MISSED`: 1 and how many events this reader lost; 0 for any other kind.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_missed(
    e: *const weida_peer_event,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe {
        event_value(e, out, |e| match e {
            PeerEvent::Missed(n) => Some(*n),
            _ => None,
        })
    }
}

/// Releases an event. NULL does nothing.
///
/// # Safety
///
/// `e` is NULL or a live event, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_event_drop(e: *mut weida_peer_event) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(e) }));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use weida_rs::{Fingerprint, LossCause};

    use super::*;
    use crate::error::{WEIDA_EINVAL, WEIDA_LOSS_PEER_CLOSED};

    /// A string a getter handed out, copied.
    fn text(p: *const c_char, len: usize) -> Option<String> {
        if p.is_null() {
            return None;
        }
        // SAFETY: a non-NULL getter result points to `len` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(p.cast::<u8>(), len) };
        Some(String::from_utf8(bytes.to_vec()).expect("UTF-8"))
    }

    #[test]
    fn every_peer_event_kind_reads_back_through_its_getters() {
        let fp = Fingerprint::from_bytes([7; 32]);
        let url: Arc<str> = Arc::from("weida+inproc://bus/p");
        let mut len = 0usize;
        let mut code: c_int = 0;
        let mut n32 = 0u32;
        let mut n64 = 0u64;
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable.
        unsafe {
            let e = weida_peer_event::handle(PeerEvent::Connected {
                url: Arc::clone(&url),
                peer: Some(PeerIdentity::Key(fp)),
            });
            assert_eq!(weida_peer_event_kind(e), WEIDA_PEER_EVENT_CONNECTED);
            assert_eq!(
                text(weida_peer_event_url(e, &mut len), len).as_deref(),
                Some("weida+inproc://bus/p")
            );
            assert_eq!(
                text(weida_peer_event_peer_fingerprint(e, &mut len), len),
                Some(fp.to_string())
            );
            assert_eq!(weida_peer_event_cause(e, &mut code), 0);
            weida_peer_event_drop(e);

            let e = weida_peer_event::handle(PeerEvent::Lost {
                url: Arc::clone(&url),
                cause: LossCause::PeerClosed,
            });
            assert_eq!(weida_peer_event_kind(e), WEIDA_PEER_EVENT_LOST);
            assert_eq!(weida_peer_event_cause(e, &mut code), 1);
            assert_eq!(code, WEIDA_LOSS_PEER_CLOSED);
            assert_eq!(weida_peer_event_attempt(e, &mut n32), 0);
            weida_peer_event_drop(e);

            let e = weida_peer_event::handle(PeerEvent::Retrying {
                url: Arc::clone(&url),
                attempt: 3,
                delay: Duration::from_micros(1500),
            });
            assert_eq!(weida_peer_event_kind(e), WEIDA_PEER_EVENT_RETRYING);
            assert_eq!(weida_peer_event_attempt(e, &mut n32), 1);
            assert_eq!(n32, 3);
            assert_eq!(weida_peer_event_delay_us(e, &mut n64), 1);
            assert_eq!(n64, 1500);
            weida_peer_event_drop(e);

            let e = weida_peer_event::handle(PeerEvent::GaveUp {
                url: Arc::clone(&url),
                why: GiveUp::Policy { attempts: 4 },
            });
            assert_eq!(weida_peer_event_kind(e), WEIDA_PEER_EVENT_GAVE_UP);
            assert_eq!(weida_peer_event_why(e, &mut code), 1);
            assert_eq!(code, WEIDA_GIVE_UP_POLICY);
            assert_eq!(weida_peer_event_why_attempts(e, &mut n32), 1);
            assert_eq!(n32, 4);
            assert!(weida_peer_event_why_presented(e, &mut len).is_null());
            weida_peer_event_drop(e);

            let e = weida_peer_event::handle(PeerEvent::GaveUp {
                url: Arc::clone(&url),
                why: GiveUp::PeerChanged {
                    presented: Some(fp),
                },
            });
            assert_eq!(weida_peer_event_why(e, &mut code), 1);
            assert_eq!(code, WEIDA_GIVE_UP_PEER_CHANGED);
            assert_eq!(
                text(weida_peer_event_why_presented(e, &mut len), len),
                Some(fp.to_string())
            );
            assert_eq!(weida_peer_event_why_attempts(e, &mut n32), 0);
            weida_peer_event_drop(e);

            let e = weida_peer_event::handle(PeerEvent::GaveUp {
                url: Arc::clone(&url),
                why: GiveUp::Failed("negotiation refused".into()),
            });
            assert_eq!(weida_peer_event_why(e, &mut code), 1);
            assert_eq!(code, WEIDA_GIVE_UP_FAILED);
            assert_eq!(
                text(weida_peer_event_why_message(e, &mut len), len).as_deref(),
                Some("negotiation refused")
            );
            weida_peer_event_drop(e);

            let e = weida_peer_event::handle(PeerEvent::Missed(9));
            assert_eq!(weida_peer_event_kind(e), WEIDA_PEER_EVENT_MISSED);
            assert!(weida_peer_event_url(e, &mut len).is_null());
            assert_eq!(weida_peer_event_missed(e, &mut n64), 1);
            assert_eq!(n64, 9);
            weida_peer_event_drop(e);

            assert_eq!(weida_peer_event_kind(ptr::null()), -WEIDA_EINVAL);
            assert_eq!(weida_peer_event_missed(ptr::null(), &mut n64), 0);
        }
    }
}
