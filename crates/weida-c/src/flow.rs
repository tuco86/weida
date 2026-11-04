//! Datagram flows: a raw peer's sending half, an acceptor's receiving half,
//! and the datagrams it reads (docs/c/flow.md).

use std::ffi::c_int;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use weida_rs::{Flow, FlowInfo, FlowStats, IncomingFlow, IncomingMeta, PathStats, ReportMode};

use crate::error::{closed, fail, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{borrow, bytes, into_handle, out_ptr, release, share, write_out};
use crate::stats::weida_path_stats;
use crate::transfer::weida_incoming_meta;
use crate::try_handle::Tried;

/// The sending half of one datagram flow (`Flow`). Its calls take the lock
/// in turn; `weida_flow_abort` takes the value out.
pub struct weida_flow {
    flow: Mutex<Option<Flow>>,
}

impl weida_flow {
    pub(crate) fn handle(flow: Flow) -> *mut weida_flow {
        into_handle(weida_flow {
            flow: Mutex::new(Some(flow)),
        })
    }
}

/// A recv in flight, kept across tries (R3a).
type Datagrams = Pin<Box<dyn Future<Output = Option<Bytes>> + Send>>;

/// An incoming flow's state between tries: the recv in flight is kept,
/// never recreated, so a timeout or `EINTR` loses nothing (R3a).
struct IncomingFlowState {
    flow: Arc<IncomingFlow>,
    receiving: Option<Datagrams>,
}

/// The receiving half of one datagram flow (`IncomingFlow`).
pub struct weida_incoming_flow {
    tried: Tried<IncomingFlowState>,
    /// `FlowInfo` as metadata, built once.
    info: Arc<weida_incoming_meta>,
}

impl weida_incoming_flow {
    pub(crate) fn handle(flow: IncomingFlow) -> *mut weida_incoming_flow {
        into_handle(weida_incoming_flow {
            info: info_meta(flow.info()),
            tried: Tried::new(IncomingFlowState {
                flow: Arc::new(flow),
                receiving: None,
            }),
        })
    }
}

/// One datagram a flow received (the `Bytes` of `IncomingFlow::recv`).
pub struct weida_datagram {
    data: Bytes,
}

/// `FlowInfo` as the metadata of an inbound transfer: its six fields are the
/// fields of the same names, every other one is absent.
fn info_meta(info: &FlowInfo) -> Arc<weida_incoming_meta> {
    weida_incoming_meta::new(IncomingMeta {
        endpoint: Some(info.endpoint.clone()),
        content_len: None,
        content_type: info.content_type.clone(),
        trace: info.trace,
        tracestate: None,
        topic: info.topic.clone(),
        peer: info.peer.clone(),
        peer_chain: info.peer_chain.clone(),
        sequence: None,
        gap: None,
        achieved: None,
        report: Vec::new(),
        report_mode: ReportMode::default(),
        report_id: None,
        segment: None,
        layer: None,
    })
}

/// Runs `g` on the flow; `-WEIDA_ECLOSED` once it was aborted.
fn with_flow<R>(f: &weida_flow, g: impl FnOnce(&Flow) -> R) -> Result<R, c_int> {
    let flow = f.flow.lock().unwrap_or_else(PoisonError::into_inner);
    Ok(g(flow.as_ref().ok_or_else(closed)?))
}

/// One sender counter; 0 for NULL or an aborted flow.
///
/// # Safety
///
/// `f` is NULL or a live flow.
unsafe fn counter(f: *const weida_flow, pick: impl FnOnce(FlowStats) -> u64) -> u64 {
    guard_or(0, || {
        if f.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `f` is a live flow by this function's contract.
        let f = unsafe { &*f };
        let flow = f.flow.lock().unwrap_or_else(PoisonError::into_inner);
        flow.as_ref().map_or(0, |fl| pick(fl.stats()))
    })
}

/// One receiver counter; 0 for NULL or a refused flow.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow.
unsafe fn incoming_counter(
    f: *const weida_incoming_flow,
    pick: impl FnOnce(FlowStats) -> u64,
) -> u64 {
    guard_or(0, || {
        if f.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `f` is a live incoming flow by this function's
        // contract.
        let f = unsafe { &*f };
        f.tried.with(|s| pick(s.flow.stats())).unwrap_or(0)
    })
}

/// Writes a path snapshot: 1 and the handle when there is a path, 0 and
/// NULL when there is none.
///
/// # Safety
///
/// `out` passed `out_ptr`.
unsafe fn path_out(path: Option<PathStats>, out: *mut *mut weida_path_stats) -> c_int {
    match path {
        Some(p) => {
            // SAFETY: forwarded from this function's contract.
            unsafe { write_out(out, weida_path_stats::handle(p)) };
            1
        }
        None => {
            // SAFETY: forwarded from this function's contract.
            unsafe { write_out(out, std::ptr::null_mut()) };
            0
        }
    }
}

// --- weida_flow ------------------------------------------------------------

/// Sends one datagram (`Flow::send`); the payload is copied. Never blocks.
///
/// 0 means the datagram was handed to the connection, not that it arrived.
/// On a dialled address with no live connection the datagram is dropped,
/// counted `not_live`, and 0 returned. Fails with `-WEIDA_ETOO_LARGE`
/// (`weida_last_error_max` names the limit) above `weida_flow_max_payload`,
/// `-WEIDA_EDATAGRAMS_UNAVAILABLE` when the connection carries no
/// datagrams, and on an ended flow with the error that ended it
/// (`-WEIDA_EREJECTED`, `-WEIDA_EUNKNOWN_ENDPOINT`, `-WEIDA_EUNSUPPORTED`,
/// `-WEIDA_ELIMIT_EXCEEDED`, `-WEIDA_ECANCELED`, `-WEIDA_ECONNECTION_LOST`,
/// `-WEIDA_ENOT_CONNECTED`). `-WEIDA_ECLOSED` after `weida_flow_abort`.
///
/// # Safety
///
/// `f` is NULL or a live flow; `payload` is NULL only with `len == 0`, else
/// points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_send(
    f: *const weida_flow,
    payload: *const u8,
    len: usize,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        // SAFETY: forwarded from this function's contract.
        let data = Bytes::copy_from_slice(unsafe { bytes(payload, len) }?);
        with_flow(&f, |fl| fl.send(data))?.map_err(fail)?;
        Ok(0)
    })
}

/// The largest payload `weida_flow_send` accepts now (`Flow::max_payload`):
/// 1 and `*out` when the connection carries datagrams, 0 when it does not,
/// for a NULL argument and after `weida_flow_abort`.
///
/// # Safety
///
/// `f` is NULL or a live flow; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_max_payload(f: *const weida_flow, out: *mut usize) -> c_int {
    guard_or(0, || {
        if f.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `f` is a live flow by this function's contract.
        let f = unsafe { &*f };
        match with_flow(f, Flow::max_payload) {
            Ok(Some(max)) => {
                // SAFETY: a non-NULL `out` is writable by this function's
                // contract.
                unsafe { write_out(out, max) };
                1
            }
            _ => 0,
        }
    })
}

/// `FlowStats::sent`: datagrams handed to the transport; 0 for NULL.
///
/// # Safety
///
/// `f` is NULL or a live flow.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_sent(f: *const weida_flow) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { counter(f, |s| s.sent) }
}

/// `FlowStats::too_large`: datagrams `weida_flow_send` refused with
/// `-WEIDA_ETOO_LARGE`; 0 for NULL.
///
/// # Safety
///
/// `f` is NULL or a live flow.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_too_large(f: *const weida_flow) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { counter(f, |s| s.too_large) }
}

/// `FlowStats::discarded`: queued datagrams discarded oldest-first before
/// they were written, counted on the local transports only; 0 for NULL.
///
/// # Safety
///
/// `f` is NULL or a live flow.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_discarded(f: *const weida_flow) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { counter(f, |s| s.discarded) }
}

/// `FlowStats::not_live`: datagrams dropped because no connection was
/// live; 0 for NULL.
///
/// # Safety
///
/// `f` is NULL or a live flow.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_not_live(f: *const weida_flow) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { counter(f, |s| s.not_live) }
}

/// A snapshot of the path under the flow (`Flow::path_stats`): 1 and the
/// snapshot, or 0 with `*out == NULL` on a local transport, which has no
/// path. `-WEIDA_ECLOSED` after `weida_flow_abort`.
///
/// # Safety
///
/// `f` is NULL or a live flow; `out` is NULL or writable. Release the
/// snapshot with `weida_path_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_path_stats(
    f: *const weida_flow,
    out: *mut *mut weida_path_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let path = with_flow(&f, Flow::path_stats)?;
        // SAFETY: `out` passed `out_ptr`.
        Ok(unsafe { path_out(path, out) })
    })
}

/// Abandons the flow (`Flow::abort`), which **consumes** the handle: the
/// flow is reset `CANCELED`. NULL does nothing.
///
/// # Safety
///
/// `f` is NULL or a live flow, consumed at most once; no other call on it
/// follows.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_abort(f: *mut weida_flow) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(f) = unsafe { release(f) } {
            let flow = f.flow.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(flow) = flow {
                flow.abort();
            }
        }
    });
}

/// Releases a flow, which closes it with FIN (`Flow::close`): the receiver
/// sees its end after the datagrams already delivered. NULL does nothing.
///
/// # Safety
///
/// `f` is NULL or a live flow, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_flow_drop(f: *mut weida_flow) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(f) }));
}

// --- weida_incoming_flow ---------------------------------------------------

/// What the sender attached to the flow (`IncomingFlow::info`): its
/// endpoint, topic, content type, trace, peer and chain, read with the
/// `weida_incoming_meta_*` getters (msg.md). Every other getter reads
/// absent. Readable after the flow is gone.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow; `out` is NULL or writable. Release
/// the metadata with `weida_incoming_meta_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_info(
    f: *const weida_incoming_flow,
    out: *mut *mut weida_incoming_meta,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, share(Arc::clone(&f.info))) };
        Ok(0)
    })
}

/// The incoming flow's readiness descriptor (R11): readable when the next
/// `weida_incoming_flow_recv` will not block.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_fd(
    f: *const weida_incoming_flow,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let fd = f.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next datagram, oldest first, waiting up to `timeout_ms` (R4)
/// for one (`IncomingFlow::recv`). 0 with `*out == NULL` once the flow ended
/// and every datagram that arrived before its end was taken, and on every
/// recv after that.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow; `out` is NULL or writable. Release
/// the datagram with `weida_datagram_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_recv(
    f: *const weida_incoming_flow,
    timeout_ms: i64,
    out: *mut *mut weida_datagram,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let received = f.tried.attempt(timeout_ms, |state, cx| {
            let receiving = state.receiving.get_or_insert_with(|| {
                let flow = Arc::clone(&state.flow);
                Box::pin(async move { flow.recv().await })
            });
            let polled = receiving.as_mut().poll(cx);
            if polled.is_ready() {
                state.receiving = None;
            }
            polled
        })?;
        let handle = match received {
            Some(data) => into_handle(weida_datagram { data }),
            None => std::ptr::null_mut(),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// `FlowStats::received`: datagrams that arrived for this flow; 0 for NULL.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_received(f: *const weida_incoming_flow) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { incoming_counter(f, |s| s.received) }
}

/// `FlowStats::overflow`: arrived datagrams dropped, oldest first, because
/// `flow_queue_bytes` were unread; 0 for NULL.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_overflow(f: *const weida_incoming_flow) -> u64 {
    // SAFETY: forwarded from this function's contract.
    unsafe { incoming_counter(f, |s| s.overflow) }
}

/// A snapshot of the path under the flow (`IncomingFlow::path_stats`): 1
/// and the snapshot, or 0 with `*out == NULL` on a local transport, which
/// has no path.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow; `out` is NULL or writable. Release
/// the snapshot with `weida_path_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_path_stats(
    f: *const weida_incoming_flow,
    out: *mut *mut weida_path_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let path = f.tried.with(|s| s.flow.path_stats())?;
        // SAFETY: `out` passed `out_ptr`.
        Ok(unsafe { path_out(path, out) })
    })
}

/// Refuses the flow (`IncomingFlow::refuse`), which **consumes** the
/// handle: the flow is stopped `REJECTED`, which the sender's next send
/// reports as `-WEIDA_EREJECTED`. A thread waiting in
/// `weida_incoming_flow_recv` returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow, consumed at most once; no other
/// call on it follows.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_refuse(f: *mut weida_incoming_flow) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        let Some(f) = (unsafe { release(f) }) else {
            return;
        };
        if let Some(state) = f.tried.close() {
            // The recv in flight holds the only other reference.
            drop(state.receiving);
            if let Ok(flow) = Arc::try_unwrap(state.flow) {
                flow.refuse();
            }
        }
    });
}

/// Releases an incoming flow, which stops it `CANCELED`: the sender's next
/// send reports `-WEIDA_ECANCELED`. A thread waiting in
/// `weida_incoming_flow_recv` returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `f` is NULL or a live incoming flow, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_flow_drop(f: *mut weida_incoming_flow) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(f) = unsafe { release(f) } {
            drop(f.tried.close());
        }
    });
}

// --- weida_datagram --------------------------------------------------------

/// The datagram's bytes; `weida_datagram_len` of them. Valid as long as
/// `d`.
///
/// # Safety
///
/// `d` is NULL or a live datagram.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_datagram_data(d: *const weida_datagram) -> *const u8 {
    guard_ptr(|| {
        if d.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `d` is a live datagram by the caller's contract.
        unsafe { &*d }.data.as_ptr()
    })
}

/// The datagram's length in bytes; 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live datagram.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_datagram_len(d: *const weida_datagram) -> usize {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live datagram by the caller's contract.
        unsafe { &*d }.data.len()
    })
}

/// Releases a datagram and its bytes. NULL does nothing.
///
/// # Safety
///
/// `d` is NULL or a live datagram, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_datagram_drop(d: *mut weida_datagram) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(d) }));
}

#[cfg(test)]
mod tests {
    use std::ffi::c_char;

    use super::*;
    use crate::transfer::{
        weida_incoming_meta_content_len, weida_incoming_meta_content_type,
        weida_incoming_meta_endpoint, weida_incoming_meta_peer_fingerprint,
        weida_incoming_meta_report, weida_incoming_meta_segment, weida_incoming_meta_topic,
        weida_incoming_meta_trace,
    };
    use weida_rs::{Fingerprint, PeerIdentity, TraceContext};

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
    fn flow_info_reads_back_through_the_metadata_getters() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let meta = info_meta(&FlowInfo {
            endpoint: "/v".to_owned(),
            topic: Some("mic".to_owned()),
            content_type: Some("audio/opus".to_owned()),
            trace: Some(TraceContext::parse_traceparent(traceparent).unwrap()),
            peer: Some(PeerIdentity::Key(Fingerprint::from_bytes([7; 32]))),
            peer_chain: None,
        });
        let m = Arc::as_ptr(&meta);
        let mut len = 0;
        // SAFETY: `m` is live for the test; `len` is writable.
        unsafe {
            let read = |p: *const c_char, len: usize| text(p, len);
            assert_eq!(
                read(weida_incoming_meta_endpoint(m, &mut len), len).as_deref(),
                Some("/v")
            );
            assert_eq!(
                read(weida_incoming_meta_topic(m, &mut len), len).as_deref(),
                Some("mic")
            );
            assert_eq!(
                read(weida_incoming_meta_content_type(m, &mut len), len).as_deref(),
                Some("audio/opus")
            );
            assert_eq!(
                read(weida_incoming_meta_trace(m, &mut len), len).as_deref(),
                Some(traceparent)
            );
            assert_eq!(
                read(weida_incoming_meta_peer_fingerprint(m, &mut len), len),
                Some(Fingerprint::from_bytes([7; 32]).to_string())
            );
            let mut n = 0u64;
            assert_eq!(weida_incoming_meta_content_len(m, &mut n), 0);
            assert_eq!(weida_incoming_meta_segment(m, &mut n), 0);
            assert!(weida_incoming_meta_report(m, &mut len).is_null());
        }
    }
}
