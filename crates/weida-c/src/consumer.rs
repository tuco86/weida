//! What a queue serving a raw acceptor's path deals with: the consumers that
//! subscribed on it and the credit they granted (docs/c/peer.md).

use std::ffi::{c_char, c_int};
use std::ptr;
use std::sync::Arc;

use ::weida_runtime::Exec;
use weida_rs::{Consumer, CreditGrant, OutgoingTransfer, TransferMeta};

use crate::error::{guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait};
use crate::tls::weida_transfer_meta;
use crate::transfer::weida_outgoing_transfer;

/// One consumer that subscribed on an acceptor's path, and the way to
/// deliver to it (`Consumer`).
pub struct weida_consumer {
    consumer: Consumer,
    filter: CText,
    path: CText,
    exec: Exec,
}

impl weida_consumer {
    pub(crate) fn handle(consumer: Consumer, exec: Exec) -> *mut weida_consumer {
        into_handle(weida_consumer {
            filter: CText::new(consumer.filter()),
            path: CText::new(consumer.path()),
            consumer,
            exec,
        })
    }
}

/// The pending open of `weida_consumer_open`.
pub struct weida_consumer_open_op {
    op: Arc<Op<OutgoingTransfer>>,
    exec: Exec,
}

impl AsOp for weida_consumer_open_op {
    type Output = OutgoingTransfer;
    fn op(&self) -> &Op<OutgoingTransfer> {
        &self.op
    }
}

/// The pending delivery of `weida_consumer_deliver`.
pub struct weida_consumer_deliver_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_consumer_deliver_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// A consumer's absolute delivery limit for one subscription
/// (`CreditGrant`), read-only.
pub struct weida_credit_grant {
    grant: CreditGrant,
    filter: CText,
}

impl weida_credit_grant {
    pub(crate) fn handle(grant: CreditGrant) -> *mut weida_credit_grant {
        into_handle(weida_credit_grant {
            filter: CText::new(grant.filter.as_bytes()),
            grant,
        })
    }
}

/// The metadata a NULL-or-live pointer names: a copy, or the default.
///
/// # Safety
///
/// `meta` is NULL or live metadata.
pub(crate) unsafe fn meta_or_default(
    meta: *const weida_transfer_meta,
) -> Result<TransferMeta, c_int> {
    if meta.is_null() {
        Ok(TransferMeta::default())
    } else {
        // SAFETY: forwarded from this function's contract.
        Ok(unsafe { borrow(meta) }?.get())
    }
}

// --- weida_consumer --------------------------------------------------------

/// The consumer's number (`Consumer::id`): the number of the connection it
/// subscribed on, shared by every subscription of that connection and equal
/// to that connection's `ConnectionId`. Never reused while the process
/// lives. 0 for NULL.
///
/// # Safety
///
/// `c` is NULL or a live consumer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_id(c: *const weida_consumer) -> u64 {
    guard_or(0, || {
        if c.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `c` is a live consumer by this function's
        // contract.
        unsafe { &*c }.consumer.id().get()
    })
}

/// The filter this subscription was created with (`Consumer::filter`).
/// Lives as long as the consumer.
///
/// # Safety
///
/// `c` is NULL or a live consumer; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_filter(
    c: *const weida_consumer,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if c.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `c` is a live consumer by this function's
        // contract; the text lives as long as it.
        let c = unsafe { &*c };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&c.filter), len_out) }
    })
}

/// The endpoint path it subscribed on (`Consumer::path`). Lives as long as
/// the consumer.
///
/// # Safety
///
/// `c` is NULL or a live consumer; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_path(
    c: *const weida_consumer,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if c.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `c` is a live consumer by this function's
        // contract; the text lives as long as it.
        let c = unsafe { &*c };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&c.path), len_out) }
    })
}

/// Opens one delivery to this consumer (`Consumer::open`), carrying `meta`,
/// or empty metadata when `meta` is NULL; `meta` is copied at the call.
/// The transfer is addressed to the subscribed path and arrives at the
/// consumer's subscriber; the topic it filters on travels in `meta`.
///
/// Never blocks; the open runs as the op written to `out`.
///
/// # Safety
///
/// `c` is NULL or a live consumer; `meta` is NULL or live metadata; `out`
/// is NULL or writable. Release the op with `weida_consumer_open_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_open(
    c: *const weida_consumer,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_consumer_open_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let c = unsafe { borrow(c) }?;
        // SAFETY: forwarded from this function's contract.
        let meta = unsafe { meta_or_default(meta) }?;
        let consumer = c.consumer.clone();
        let op = Op::start(&c.exec, async move { consumer.open(meta).await });
        let handle = into_handle(weida_consumer_open_op {
            op,
            exec: c.exec.clone(),
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
pub unsafe extern "C" fn weida_consumer_open_op_fd(
    op: *const weida_consumer_open_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the open and writes the transfer to
/// `out`, written like a socket and ended with
/// `weida_outgoing_transfer_finish`. `-WEIDA_ECONNECTION_LOST` once the
/// consumer's connection is gone.
///
/// # Safety
///
/// `op` is NULL or a live open op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_open_op_wait(
    op: *const weida_consumer_open_op,
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
pub unsafe extern "C" fn weida_consumer_open_op_drop(op: *mut weida_consumer_open_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Delivers `body` as one whole transfer carrying `meta`
/// (`Consumer::deliver`); NULL `meta` is `TransferMeta::default()`. `meta`
/// and the body are copied at the call.
///
/// Never blocks; the delivery runs as the op written to `out`.
///
/// # Safety
///
/// `c` is NULL or a live consumer; `meta` is NULL or live metadata; `body`
/// is NULL only with `len == 0`, else points to `len` readable bytes; `out`
/// is NULL or writable. Release the op with `weida_consumer_deliver_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_deliver(
    c: *const weida_consumer,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_consumer_deliver_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let c = unsafe { borrow(c) }?;
        // SAFETY: forwarded from this function's contract.
        let meta = unsafe { meta_or_default(meta) }?;
        // SAFETY: forwarded from this function's contract.
        let body = unsafe { bytes(body, len) }?.to_vec();
        let consumer = c.consumer.clone();
        let op = Op::start(&c.exec, async move { consumer.deliver(meta, &body).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_consumer_deliver_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a deliver op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live deliver op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_deliver_op_fd(
    op: *const weida_consumer_deliver_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the delivery: 0 once the body is
/// written and finished. `-WEIDA_ECONNECTION_LOST` once the consumer's
/// connection is gone.
///
/// # Safety
///
/// `op` is NULL or a live deliver op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_deliver_op_wait(
    op: *const weida_consumer_deliver_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a deliver op, abandoning the delivery when unfinished. NULL
/// does nothing.
///
/// # Safety
///
/// `op` is NULL or a live deliver op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_deliver_op_drop(op: *mut weida_consumer_deliver_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Closes the connection the consumer subscribed on with `code` and the
/// human-readable `reason` (`Consumer::close`), ending every subscription
/// that connection holds, on every path; the acceptor then reports
/// `WEIDA_INCOMING_UNSUBSCRIBED` with a NULL filter. Never blocks.
///
/// `code` is one of docs/PROTOCOL.md §7's (e.g. 5 `LIMIT_EXCEEDED`, 6
/// `SHUTDOWN`); above `2^62-1` it is sent as 1 `INTERNAL`. Closing a
/// closed connection does nothing.
///
/// # Safety
///
/// `c` is NULL or a live consumer; `reason` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_close(
    c: *const weida_consumer,
    code: u64,
    reason: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let c = unsafe { borrow(c) }?;
        // SAFETY: forwarded from this function's contract.
        let reason = unsafe { cstr(reason) }?;
        c.consumer.close(code, reason);
        Ok(0)
    })
}

/// Releases a consumer handle; the subscription stays. NULL does nothing.
///
/// # Safety
///
/// `c` is NULL or a live consumer, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_consumer_drop(c: *mut weida_consumer) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(c) }));
}

// --- weida_credit_grant ----------------------------------------------------

/// The number of the consumer that granted it (`CreditGrant::id`), as
/// `weida_consumer_id` reads it; 0 for NULL.
///
/// # Safety
///
/// `g` is NULL or a live credit grant.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_credit_grant_id(g: *const weida_credit_grant) -> u64 {
    guard_or(0, || {
        if g.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `g` is a live grant by this function's contract.
        unsafe { &*g }.grant.id.get()
    })
}

/// The filter of the subscription it was granted for
/// (`CreditGrant::filter`). Lives as long as the grant.
///
/// # Safety
///
/// `g` is NULL or a live credit grant; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_credit_grant_filter(
    g: *const weida_credit_grant,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if g.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `g` is a live grant by this function's
        // contract; the text lives as long as it.
        let g = unsafe { &*g };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&g.filter), len_out) }
    })
}

/// The messages the subscription accepts in total since its creation
/// (`CreditGrant::limit`): absolute and cumulative, so a queue keeps the
/// highest it has seen. 0 for NULL.
///
/// # Safety
///
/// `g` is NULL or a live credit grant.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_credit_grant_limit(g: *const weida_credit_grant) -> u64 {
    guard_or(0, || {
        if g.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `g` is a live grant by this function's contract.
        unsafe { &*g }.grant.limit
    })
}

/// Releases a credit grant. NULL does nothing.
///
/// # Safety
///
/// `g` is NULL or a live credit grant, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_credit_grant_drop(g: *mut weida_credit_grant) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(g) }));
}
