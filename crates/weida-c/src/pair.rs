//! Either half of PAIR: one peer, one-way transfers both ways (docs/c/pair.md).

use std::ffi::{c_char, c_int};
use std::sync::Arc;

use ::weida_runtime::Exec;
use weida_rs::{OutgoingTransfer, Paired, TransferMeta};

use crate::error::{fail, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::push::Receiving;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::stats::{weida_connection_stats, weida_peer_events};
use crate::tls::{weida_client_tls, weida_transfer_meta};
use crate::transfer::{weida_incoming_transfer, weida_outgoing_transfer};
use crate::try_handle::Tried;

/// Either half of PAIR (`Paired`): dialling (`weida_runtime_pair`) or bound
/// (`weida_listener_pair`). The recv in flight is kept across tries (R3a).
pub struct weida_paired {
    paired: Arc<Paired>,
    tried: Tried<Option<Receiving>>,
    path: CText,
    exec: Exec,
}

/// The pending dial of `weida_paired_connect`.
pub struct weida_paired_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_paired_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending send of `weida_paired_send` and `weida_paired_send_with`.
pub struct weida_paired_send_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_paired_send_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending open of `weida_paired_open`.
pub struct weida_paired_open_op {
    op: Arc<Op<OutgoingTransfer>>,
    exec: Exec,
}

impl AsOp for weida_paired_open_op {
    type Output = OutgoingTransfer;
    fn op(&self) -> &Op<OutgoingTransfer> {
        &self.op
    }
}

fn handle(paired: Paired, exec: Exec) -> *mut weida_paired {
    into_handle(weida_paired {
        path: CText::new(paired.path()),
        paired: Arc::new(paired),
        tried: Tried::new(None),
        exec,
    })
}

/// Creates a dialling pair that dials on `tls`'s terms (`Runtime::pair`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with `weida_paired_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_pair(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_paired,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = handle(rt.runtime.pair(tls.get()), rt.runtime.exec().clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Registers a bound pair at the endpoint path `path` (`Listener::pair`).
/// Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_paired_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_pair(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_paired,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let paired = l.listener.pair(path).map_err(fail)?;
        let handle = handle(paired, l.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this pair uses: a bound pair's path, the empty string
/// on a dialling pair. Lives as long as the pair.
///
/// # Safety
///
/// `p` is NULL or a live pair; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_path(
    p: *const weida_paired,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if p.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `p` is a live pair by this function's
        // contract; the text lives as long as it.
        let p = unsafe { &*p };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&p.path), len_out) }
    })
}

/// Dials `url` (`Paired::connect`), once: a pair has one peer. The runtime
/// redials it when the connection is lost.
///
/// Never blocks; the dial runs as the op written to `out`. Its wait fails
/// with `-WEIDA_EUNSUPPORTED` on a bound pair and `-WEIDA_ELIMIT_EXCEEDED`
/// once an address is dialled.
///
/// # Safety
///
/// `p` is NULL or a live pair; `url` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the op with
/// `weida_paired_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_connect(
    p: *const weida_paired,
    url: *const c_char,
    out: *mut *mut weida_paired_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let paired = Arc::clone(&p.paired);
        let op = Op::start(&p.exec, async move { paired.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_paired_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_connect_op_fd(
    op: *const weida_paired_connect_op,
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
pub unsafe extern "C" fn weida_paired_connect_op_wait(
    op: *const weida_paired_connect_op,
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
pub unsafe extern "C" fn weida_paired_connect_op_drop(op: *mut weida_paired_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The number of connected peers, 0 or 1 (`Paired::peer_count`); 0 for
/// NULL and on a bound pair.
///
/// # Safety
///
/// `p` is NULL or a live pair.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_peer_count(p: *const weida_paired) -> usize {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live pair by this function's contract.
        unsafe { &*p }.paired.peer_count()
    })
}

/// The bodies a dialling pair's outbox discarded (`Paired::dropped`); 0 for
/// NULL and on a bound pair.
///
/// # Safety
///
/// `p` is NULL or a live pair.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_dropped(p: *const weida_paired) -> u64 {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live pair by this function's contract.
        unsafe { &*p }.paired.dropped()
    })
}

/// A snapshot of a dialling pair's connection (`Paired::connection_stats`):
/// one record per live dialled address, read with the
/// `weida_connection_stats_*` getters; no records on a bound pair. Never
/// blocks.
///
/// # Safety
///
/// `p` is NULL or a live pair; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_connection_stats(
    p: *const weida_paired,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let handle = weida_connection_stats::handle(p.paired.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// A new reader of a dialling pair's peer events (`Paired::events`),
/// written to `out`: returns 1. A bound pair dials nothing and reports
/// nothing: returns 0 with `out` untouched. Each call is an independent
/// reader.
///
/// # Safety
///
/// `p` is NULL or a live pair; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_events(
    p: *const weida_paired,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let Some(events) = p.paired.events() else {
            return Ok(0);
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_peer_events::handle(events)) };
        Ok(1)
    })
}

/// Opens a one-way transfer to the peer (`Paired::open`), carrying `meta`,
/// or empty metadata when `meta` is NULL; `meta` is copied at the call. Its
/// `weida_outgoing_transfer_finish` hands back the delivery receipt that
/// `weida_paired_send` discards.
///
/// Never blocks; the open runs as the op written to `out`. A bound pair's
/// open waits until its peer has sent first.
///
/// # Safety
///
/// `p` is NULL or a live pair; `meta` is NULL or live metadata; `out` is
/// NULL or writable. Release the op with `weida_paired_open_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_open(
    p: *const weida_paired,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_paired_open_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let meta = if meta.is_null() {
            TransferMeta::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(meta) }?.get()
        };
        let paired = Arc::clone(&p.paired);
        let op = Op::start(&p.exec, async move { paired.open(meta).await });
        let handle = into_handle(weida_paired_open_op {
            op,
            exec: p.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The readiness descriptor of an open op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live open op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_open_op_fd(
    op: *const weida_paired_open_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the open and writes the transfer to
/// `out`, written like a socket and ended with
/// `weida_outgoing_transfer_finish`.
///
/// # Safety
///
/// `op` is NULL or a live open op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_open_op_wait(
    op: *const weida_paired_open_op,
    timeout_ms: i64,
    out: *mut *mut weida_outgoing_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let op = unsafe { borrow(op) }?;
        let transfer = op.op.wait(timeout_ms)?;
        let handle = weida_outgoing_transfer::handle(transfer, op.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases an open op. An unfinished open is aborted; a transfer it opened
/// that no wait took is reset `CANCELED`. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live open op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_open_op_drop(op: *mut weida_paired_open_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Sends `body` as one message (`Paired::send`). The body is copied.
///
/// Never blocks; the send runs as the op written to `out`. A dialling pair
/// writes it at once while its peer is live and holds it in the outbox
/// otherwise; a bound pair writes it once its peer has sent first. The
/// delivery receipt is discarded: use `weida_paired_open` for it.
///
/// # Safety
///
/// `p` is NULL or a live pair; `body` is NULL only with `len == 0`, else
/// points to `len` readable bytes; `out` is NULL or writable. Release the
/// op with `weida_paired_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_send(
    p: *const weida_paired,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_paired_send_op,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { send(p, || Ok(TransferMeta::default()), body, len, out) })
}

/// Sends `body` as one message carrying `meta` (`Paired::send_with`):
/// `weida_paired_send` with metadata. `meta` is copied at the call; NULL is
/// `TransferMeta::default()`. The body is copied.
///
/// # Safety
///
/// `p` is NULL or a live pair; `meta` is NULL or live metadata; `body` is
/// NULL only with `len == 0`, else points to `len` readable bytes; `out` is
/// NULL or writable. Release the op with `weida_paired_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_send_with(
    p: *const weida_paired,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_paired_send_op,
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
    guard(|| unsafe { send(p, meta, body, len, out) })
}

/// The body of both send exports.
///
/// # Safety
///
/// As `weida_paired_send_with`.
unsafe fn send(
    p: *const weida_paired,
    meta: impl FnOnce() -> Result<TransferMeta, c_int>,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_paired_send_op,
) -> Result<c_int, c_int> {
    let out = out_ptr(out)?;
    // SAFETY: forwarded from this function's contract.
    let p = unsafe { borrow(p) }?;
    let meta = meta()?;
    // SAFETY: forwarded from this function's contract.
    let body = unsafe { bytes(body, len) }?.to_vec();
    let paired = Arc::clone(&p.paired);
    let op = Op::start(&p.exec, async move { paired.send_with(meta, &body).await });
    // SAFETY: `out` passed `out_ptr`.
    unsafe { write_out(out, into_handle(weida_paired_send_op { op })) };
    Ok(0)
}

/// The readiness descriptor of a send op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live send op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_send_op_fd(
    op: *const weida_paired_send_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the send: 0 once the body is written
/// or held in the outbox.
///
/// # Safety
///
/// `op` is NULL or a live send op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_send_op_wait(
    op: *const weida_paired_send_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a send op, abandoning the send when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live send op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_send_op_drop(op: *mut weida_paired_send_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The pair's readiness descriptor (R11): readable when the next
/// `weida_paired_recv` will not block.
///
/// # Safety
///
/// `p` is NULL or a live pair; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_fd(p: *const weida_paired, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let fd = p.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next transfer from the peer, waiting up to `timeout_ms` (R4)
/// for one (`Paired::recv`). `-WEIDA_ENOT_CONNECTED` once the queue ended.
///
/// # Safety
///
/// `p` is NULL or a live pair; `out` is NULL or writable. Release the
/// transfer with `weida_incoming_transfer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_recv(
    p: *const weida_paired,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let received = p.tried.attempt(timeout_ms, |receiving, cx| {
            let pending = receiving.get_or_insert_with(|| {
                let paired = Arc::clone(&p.paired);
                Box::pin(async move { paired.recv().await })
            });
            let polled = pending.as_mut().poll(cx);
            if polled.is_ready() {
                *receiving = None;
            }
            polled
        })?;
        let transfer = received.map_err(fail)?;
        let handle = weida_incoming_transfer::handle(transfer, p.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a pair. A thread waiting in `weida_paired_recv` returns
/// `-WEIDA_ECLOSED`; a dialling pair's connection and route go with the
/// last op still holding it. A bound pair's path stays registered. NULL
/// does nothing.
///
/// # Safety
///
/// `p` is NULL or a live pair, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_paired_drop(p: *mut weida_paired) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(p) = unsafe { release(p) } {
            drop(p.tried.close());
        }
    });
}
