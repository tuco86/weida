//! A member of a BUS: bound and dialling at once (docs/c/bus.md).

use std::ffi::{c_char, c_int};
use std::sync::Arc;

use ::weida_runtime::Exec;
use weida_rs::{BusMember, TransferMeta};

use crate::error::{fail, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::push::Receiving;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait};
use crate::stats::weida_connection_stats;
use crate::tls::{weida_client_tls, weida_transfer_meta};
use crate::transfer::weida_incoming_transfer;
use crate::try_handle::Tried;

/// One member of a BUS (`BusMember`): accepts on its path and dials the
/// others. The recv in flight is kept across tries (R3a).
pub struct weida_bus_member {
    member: Arc<BusMember>,
    tried: Tried<Option<Receiving>>,
    path: CText,
    exec: Exec,
}

/// The pending dial of `weida_bus_member_connect`.
pub struct weida_bus_member_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_bus_member_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending send of `weida_bus_member_send` and
/// `weida_bus_member_send_with`.
pub struct weida_bus_member_send_op {
    op: Arc<Op<usize>>,
}

impl AsOp for weida_bus_member_send_op {
    type Output = usize;
    fn op(&self) -> &Op<usize> {
        &self.op
    }
}

/// Registers a bus member at the endpoint path `path` that dials the other
/// members on `tls`'s terms (`Listener::bus`). Fails with
/// `-WEIDA_EINVALID_ENDPOINT_PATH` or `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `tls` is NULL or live client terms; `out` is NULL or writable.
/// Release the result with `weida_bus_member_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bus(
    l: *const weida_listener,
    path: *const c_char,
    tls: *const weida_client_tls,
    out: *mut *mut weida_bus_member,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let member = l.listener.bus(path, tls.get()).map_err(fail)?;
        let handle = into_handle(weida_bus_member {
            path: CText::new(member.path()),
            member: Arc::new(member),
            tried: Tried::new(None),
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this member accepts on. Lives as long as the member.
///
/// # Safety
///
/// `m` is NULL or a live member; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_path(
    m: *const weida_bus_member,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if m.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `m` is a live member by this function's
        // contract; the text lives as long as it.
        let m = unsafe { &*m };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&m.path), len_out) }
    })
}

/// Joins the member at `url` (`BusMember::connect`). The runtime redials it
/// when the connection is lost.
///
/// Never blocks; the dial runs as the op written to `out`.
///
/// # Safety
///
/// `m` is NULL or a live member; `url` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the op with
/// `weida_bus_member_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_connect(
    m: *const weida_bus_member,
    url: *const c_char,
    out: *mut *mut weida_bus_member_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let member = Arc::clone(&m.member);
        let op = Op::start(&m.exec, async move { member.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_bus_member_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_connect_op_fd(
    op: *const weida_bus_member_connect_op,
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
pub unsafe extern "C" fn weida_bus_member_connect_op_wait(
    op: *const weida_bus_member_connect_op,
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
pub unsafe extern "C" fn weida_bus_member_connect_op_drop(op: *mut weida_bus_member_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The number of members this one has joined (`BusMember::peer_count`); 0
/// for NULL.
///
/// # Safety
///
/// `m` is NULL or a live member.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_peer_count(m: *const weida_bus_member) -> usize {
    guard_or(0, || {
        if m.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `m` is a live member by this function's
        // contract.
        unsafe { &*m }.member.peer_count()
    })
}

/// A snapshot of the connections to the members this one dialled
/// (`BusMember::connection_stats`): one record per live dialled address, in
/// dial order, read with the `weida_connection_stats_*` getters. Never
/// blocks.
///
/// # Safety
///
/// `m` is NULL or a live member; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_connection_stats(
    m: *const weida_bus_member,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        let handle = weida_connection_stats::handle(m.member.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The copies that never reached a member (`BusMember::dropped`); 0 for
/// NULL.
///
/// # Safety
///
/// `m` is NULL or a live member.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_dropped(m: *const weida_bus_member) -> u64 {
    guard_or(0, || {
        if m.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `m` is a live member by this function's
        // contract.
        unsafe { &*m }.member.dropped()
    })
}

/// Sends `body` to every other member this one dialled
/// (`BusMember::send`). The body is copied.
///
/// Never blocks; the send runs as the op written to `out`, whose wait
/// writes how many members it reached. A member that cannot take a copy
/// loses it and is counted in `weida_bus_member_dropped`.
///
/// # Safety
///
/// `m` is NULL or a live member; `body` is NULL only with `len == 0`, else
/// points to `len` readable bytes; `out` is NULL or writable. Release the
/// op with `weida_bus_member_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_send(
    m: *const weida_bus_member,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_bus_member_send_op,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { send(m, || Ok(TransferMeta::default()), body, len, out) })
}

/// Sends `body` carrying `meta` to every other member
/// (`BusMember::send_with`): `weida_bus_member_send` with metadata. `meta`
/// is copied at the call; NULL is `TransferMeta::default()`. The body is
/// copied.
///
/// # Safety
///
/// `m` is NULL or a live member; `meta` is NULL or live metadata; `body` is
/// NULL only with `len == 0`, else points to `len` readable bytes; `out` is
/// NULL or writable. Release the op with `weida_bus_member_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_send_with(
    m: *const weida_bus_member,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_bus_member_send_op,
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
    guard(|| unsafe { send(m, meta, body, len, out) })
}

/// The body of both send exports.
///
/// # Safety
///
/// As `weida_bus_member_send_with`.
unsafe fn send(
    m: *const weida_bus_member,
    meta: impl FnOnce() -> Result<TransferMeta, c_int>,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_bus_member_send_op,
) -> Result<c_int, c_int> {
    let out = out_ptr(out)?;
    // SAFETY: forwarded from this function's contract.
    let m = unsafe { borrow(m) }?;
    let meta = meta()?;
    // SAFETY: forwarded from this function's contract.
    let body = unsafe { bytes(body, len) }?.to_vec();
    let member = Arc::clone(&m.member);
    let op = Op::start(&m.exec, async move { member.send_with(meta, &body).await });
    // SAFETY: `out` passed `out_ptr`.
    unsafe { write_out(out, into_handle(weida_bus_member_send_op { op })) };
    Ok(0)
}

/// The readiness descriptor of a send op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live send op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_send_op_fd(
    op: *const weida_bus_member_send_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the send and writes how many members
/// it reached to `out`. `-WEIDA_ELIMIT_EXCEEDED` for a body above the
/// configured `subscriber_buffer_bytes`.
///
/// # Safety
///
/// `op` is NULL or a live send op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_send_op_wait(
    op: *const weida_bus_member_send_op,
    timeout_ms: i64,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let reached = unsafe { op_wait(op, timeout_ms) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, reached) };
        Ok(0)
    })
}

/// Releases a send op, abandoning the send when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live send op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_send_op_drop(op: *mut weida_bus_member_send_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// The member's readiness descriptor (R11): readable when the next
/// `weida_bus_member_recv` will not block.
///
/// # Safety
///
/// `m` is NULL or a live member; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_fd(m: *const weida_bus_member, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        let fd = m.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next message from another member, waiting up to `timeout_ms`
/// (R4) for one (`BusMember::recv`). `-WEIDA_ENOT_CONNECTED` once the
/// queue ended.
///
/// # Safety
///
/// `m` is NULL or a live member; `out` is NULL or writable. Release the
/// transfer with `weida_incoming_transfer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_recv(
    m: *const weida_bus_member,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        let received = m.tried.attempt(timeout_ms, |receiving, cx| {
            let pending = receiving.get_or_insert_with(|| {
                let member = Arc::clone(&m.member);
                Box::pin(async move { member.recv().await })
            });
            let polled = pending.as_mut().poll(cx);
            if polled.is_ready() {
                *receiving = None;
            }
            polled
        })?;
        let transfer = received.map_err(fail)?;
        let handle = weida_incoming_transfer::handle(transfer, m.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a member, which drops its connections; its path stays
/// registered. A thread waiting in `weida_bus_member_recv` returns
/// `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `m` is NULL or a live member, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_bus_member_drop(m: *mut weida_bus_member) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(m) = unsafe { release(m) } {
            drop(m.tried.close());
        }
    });
}
