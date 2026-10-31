//! Transfers, delivery receipts, metadata and collected messages
//! (docs/c/transfer.md, docs/c/msg.md).

use std::ffi::{c_char, c_int};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use weida_rs::{Delivery, Error, IncomingMeta, IncomingTransfer, OutgoingTransfer, PeerIdentity};
use weida_runtime::Exec;

use crate::error::{WEIDA_EINVAL, closed, fail, fail_abi, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, into_handle, out_ptr, release, share, text_out, write_out,
};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait};
use crate::try_handle::Tried;

/// The metadata of an inbound transfer or request, read-only. It outlives
/// the transfer it came from (R8).
pub struct weida_incoming_meta {
    meta: IncomingMeta,
    endpoint: Option<CText>,
    content_type: Option<CText>,
    topic: Option<CText>,
    peer_fingerprint: Option<CText>,
}

impl weida_incoming_meta {
    pub(crate) fn new(meta: IncomingMeta) -> Arc<weida_incoming_meta> {
        let text = |s: &Option<String>| s.as_deref().map(CText::new);
        Arc::new(weida_incoming_meta {
            endpoint: text(&meta.endpoint),
            content_type: text(&meta.content_type),
            topic: text(&meta.topic),
            peer_fingerprint: match &meta.peer {
                Some(PeerIdentity::Key(fp)) => Some(CText::new(fp.to_string())),
                _ => None,
            },
            meta,
        })
    }
}

/// A collected payload and the metadata of the transfer it consumed (R8).
pub struct weida_msg {
    data: Vec<u8>,
    meta: Arc<weida_incoming_meta>,
}

/// An inbound transfer: a payload read like a socket (R3a).
pub struct weida_incoming_transfer {
    tried: Tried<IncomingTransfer>,
    meta: OnceLock<Arc<weida_incoming_meta>>,
    exec: Exec,
}

impl weida_incoming_transfer {
    pub(crate) fn handle(transfer: IncomingTransfer, exec: Exec) -> *mut weida_incoming_transfer {
        into_handle(weida_incoming_transfer {
            tried: Tried::new(transfer),
            meta: OnceLock::new(),
            exec,
        })
    }
}

/// The pending collect of `weida_incoming_transfer_collect`.
pub struct weida_incoming_transfer_collect_op {
    op: Arc<Op<weida_msg>>,
}

impl AsOp for weida_incoming_transfer_collect_op {
    type Output = weida_msg;
    fn op(&self) -> &Op<weida_msg> {
        &self.op
    }
}

/// An outbound transfer: a payload written like a socket (R3a).
pub struct weida_outgoing_transfer {
    tried: Tried<OutgoingTransfer>,
    exec: Exec,
}

impl weida_outgoing_transfer {
    pub(crate) fn handle(transfer: OutgoingTransfer, exec: Exec) -> *mut weida_outgoing_transfer {
        into_handle(weida_outgoing_transfer {
            tried: Tried::new(transfer),
            exec,
        })
    }
}

/// The receipt of a finished outbound transfer.
pub struct weida_delivery {
    delivery: Mutex<Option<Delivery>>,
    exec: Exec,
}

/// The pending wait of `weida_delivery_delivered`.
pub struct weida_delivery_delivered_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_delivery_delivered_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// A new reference to the shared metadata, made on first use.
pub(crate) fn meta_of(
    cache: &OnceLock<Arc<weida_incoming_meta>>,
    make: impl FnOnce() -> Result<IncomingMeta, c_int>,
) -> Result<*mut weida_incoming_meta, c_int> {
    let meta = match cache.get() {
        Some(meta) => meta,
        None => {
            let meta = weida_incoming_meta::new(make()?);
            cache.get_or_init(|| meta)
        }
    };
    Ok(share(Arc::clone(meta)))
}

// --- weida_incoming_meta ---------------------------------------------------

/// Hands one of the metadata's strings out (R7).
unsafe fn meta_text(
    m: *const weida_incoming_meta,
    len_out: *mut usize,
    pick: impl FnOnce(&weida_incoming_meta) -> Option<&CText>,
) -> *const c_char {
    guard_ptr(|| {
        if m.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `m` is a live metadata handle by the caller's
        // contract, borrowed for this call only; the text lives as long as
        // the handle.
        let m = unsafe { &*m };
        // SAFETY: forwarded from the caller's contract.
        unsafe { text_out(pick(m), len_out) }
    })
}

/// The endpoint path the transfer was sent to; NULL on a reply.
///
/// # Safety
///
/// `m` is NULL or live metadata; `len_out` is NULL or writable. The string
/// lives as long as `m`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_endpoint(
    m: *const weida_incoming_meta,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { meta_text(m, len_out, |m| m.endpoint.as_ref()) }
}

/// The opaque content type label; NULL when the sender set none.
///
/// # Safety
///
/// `m` is NULL or live metadata; `len_out` is NULL or writable. The string
/// lives as long as `m`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_content_type(
    m: *const weida_incoming_meta,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { meta_text(m, len_out, |m| m.content_type.as_ref()) }
}

/// The Pub/Sub topic; NULL when the transfer has none.
///
/// # Safety
///
/// `m` is NULL or live metadata; `len_out` is NULL or writable. The string
/// lives as long as `m`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_topic(
    m: *const weida_incoming_meta,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { meta_text(m, len_out, |m| m.topic.as_ref()) }
}

/// The fingerprint (`sha256:` text) the sender proved in the TLS
/// handshake; NULL when it dialled anonymously or over a local transport.
///
/// # Safety
///
/// `m` is NULL or live metadata; `len_out` is NULL or writable. The string
/// lives as long as `m`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_peer_fingerprint(
    m: *const weida_incoming_meta,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { meta_text(m, len_out, |m| m.peer_fingerprint.as_ref()) }
}

/// Reads an optional number of the metadata: 1 and `*out` when present, 0
/// when absent (R2).
unsafe fn meta_number(
    m: *const weida_incoming_meta,
    out: *mut u64,
    pick: impl FnOnce(&IncomingMeta) -> Option<u64>,
) -> c_int {
    guard_or(0, || {
        if m.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `m` is live metadata by the caller's contract.
        let m = unsafe { &*m };
        match pick(&m.meta) {
            Some(v) => {
                // SAFETY: a non-NULL `out` is writable by the caller's contract.
                unsafe { out.write(v) };
                1
            }
            None => 0,
        }
    })
}

/// The advisory payload length the sender stated: 1 and `*out` when
/// present, 0 when absent.
///
/// # Safety
///
/// `m` is NULL or live metadata; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_content_len(
    m: *const weida_incoming_meta,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { meta_number(m, out, |m| m.content_len) }
}

/// The sender's per-producer sequence number: 1 and `*out` when present, 0
/// when absent.
///
/// # Safety
///
/// `m` is NULL or live metadata; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_sequence(
    m: *const weida_incoming_meta,
    out: *mut u64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { meta_number(m, out, |m| m.sequence) }
}

/// Releases a reference to metadata. NULL does nothing.
///
/// # Safety
///
/// `m` is NULL or a live metadata reference, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_meta_drop(m: *mut weida_incoming_meta) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(m) }));
}

// --- weida_msg -------------------------------------------------------------

/// The payload's bytes; `weida_msg_len` of them. Valid as long as `msg`.
///
/// # Safety
///
/// `msg` is NULL or a live message.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_msg_data(msg: *const weida_msg) -> *const u8 {
    guard_ptr(|| {
        if msg.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `msg` is a live message by the caller's contract.
        unsafe { &*msg }.data.as_ptr()
    })
}

/// The payload's length in bytes; 0 for NULL.
///
/// # Safety
///
/// `msg` is NULL or a live message.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_msg_len(msg: *const weida_msg) -> usize {
    guard_or(0, || {
        if msg.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `msg` is a live message by the caller's contract.
        unsafe { &*msg }.data.len()
    })
}

/// Writes a new reference to the metadata of the transfer the message was
/// collected from. Release it with `weida_incoming_meta_drop`.
///
/// # Safety
///
/// `msg` is NULL or a live message; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_msg_meta(
    msg: *const weida_msg,
    out: *mut *mut weida_incoming_meta,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let msg = unsafe { borrow(msg) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, share(Arc::clone(&msg.meta))) };
        Ok(0)
    })
}

/// Releases a message and its payload. NULL does nothing.
///
/// # Safety
///
/// `msg` is NULL or a live message, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_msg_drop(msg: *mut weida_msg) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(msg) }));
}

// --- weida_incoming_transfer -----------------------------------------------

/// Writes a new reference to the transfer's metadata. Release it with
/// `weida_incoming_meta_drop`; it outlives the transfer.
///
/// # Safety
///
/// `t` is NULL or a live incoming transfer; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_meta(
    t: *const weida_incoming_transfer,
    out: *mut *mut weida_incoming_meta,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        let meta = meta_of(&t.meta, || t.tried.with(|t| t.meta().clone()))?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, meta) };
        Ok(0)
    })
}

/// The transfer's readiness descriptor (R11): readable when the next
/// `weida_incoming_transfer_read` will not block.
///
/// # Safety
///
/// `t` is NULL or a live incoming transfer; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_fd(
    t: *const weida_incoming_transfer,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        let fd = t.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Reads up to `cap` bytes of the payload into `buf`, waiting up to
/// `timeout_ms` (R4) for some to arrive, and writes the count to `out_n`.
///
/// A count of 0 is the end of the payload. A peer that canceled the
/// transfer fails the read with `-WEIDA_ECANCELED`; a canceled transfer
/// never reads as an end. `cap == 0` is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `t` is NULL or a live incoming transfer; `buf` is NULL or points to
/// `cap` writable bytes; `out_n` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_read(
    t: *const weida_incoming_transfer,
    buf: *mut u8,
    cap: usize,
    timeout_ms: i64,
    out_n: *mut usize,
) -> c_int {
    guard(|| {
        let out_n = out_ptr(out_n)?;
        if buf.is_null() || cap == 0 {
            return Err(fail_abi(
                WEIDA_EINVAL,
                c"a read needs a non-NULL buffer of at least one byte",
            ));
        }
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        // SAFETY: by this function's contract `buf` points to `cap` writable
        // bytes, possibly uninitialized, which `ReadBuf` only writes.
        let space = unsafe { std::slice::from_raw_parts_mut(buf.cast::<MaybeUninit<u8>>(), cap) };
        let read = t.tried.attempt(timeout_ms, |transfer, cx| {
            let mut out = ReadBuf::uninit(&mut space[..]);
            match Pin::new(transfer).poll_read(cx, &mut out) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(out.filled().len())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
                Poll::Pending => Poll::Pending,
            }
        })?;
        let n = read.map_err(fail)?;
        // SAFETY: `out_n` passed `out_ptr`.
        unsafe { write_out(out_n, n) };
        Ok(0)
    })
}

/// Reads the whole remaining payload, refusing more than `max_bytes`
/// (`IncomingTransfer::collect`). **Consumes** `t` on every return path.
///
/// Never blocks; the collect runs as the op written to `out`, whose wait
/// yields a `weida_msg`. Over the ceiling the rest is refused and the wait
/// fails with `-WEIDA_ELIMIT_EXCEEDED`.
///
/// # Safety
///
/// `t` is NULL or a live incoming transfer, not used again; `out` is NULL
/// or writable. Release the op with `weida_incoming_transfer_collect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_collect(
    t: *mut weida_incoming_transfer,
    max_bytes: u64,
    out: *mut *mut weida_incoming_transfer_collect_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let t = unsafe { release(t) }.ok_or_else(crate::error::null_arg)?;
        let transfer = t.tried.close();
        let out = out_ptr(out)?;
        let transfer = transfer.ok_or_else(closed)?;
        let max = usize::try_from(max_bytes).unwrap_or(usize::MAX);
        let cached = t.meta.get().cloned();
        let op = Op::start(&t.exec, async move {
            let meta = match cached {
                Some(meta) => meta,
                None => weida_incoming_meta::new(transfer.meta().clone()),
            };
            let data = transfer.collect(max).await?;
            Ok(weida_msg { data, meta })
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_incoming_transfer_collect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a collect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live collect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_collect_op_fd(
    op: *const weida_incoming_transfer_collect_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the collected message and writes it to
/// `out`. Release it with `weida_msg_drop`.
///
/// # Safety
///
/// `op` is NULL or a live collect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_collect_op_wait(
    op: *const weida_incoming_transfer_collect_op,
    timeout_ms: i64,
    out: *mut *mut weida_msg,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let msg = unsafe { op_wait(op, timeout_ms) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(msg)) };
        Ok(0)
    })
}

/// Releases a collect op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live collect op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_collect_op_drop(
    op: *mut weida_incoming_transfer_collect_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases an incoming transfer. An unread payload is refused with
/// `REJECTED`. NULL does nothing.
///
/// # Safety
///
/// `t` is NULL or a live incoming transfer, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_transfer_drop(t: *mut weida_incoming_transfer) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(t) = unsafe { release(t) } {
            drop(t.tried.close());
        }
    });
}

// --- weida_outgoing_transfer -----------------------------------------------

/// The transfer's readiness descriptor (R11): readable when the next
/// `weida_outgoing_transfer_write` will not block.
///
/// # Safety
///
/// `t` is NULL or a live outgoing transfer; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_outgoing_transfer_fd(
    t: *const weida_outgoing_transfer,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        let fd = t.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Writes up to `len` bytes of `buf` into the payload, waiting up to
/// `timeout_ms` (R4) for flow control to take some, and writes the count
/// taken to `out_n`. `len == 0` writes 0 and touches nothing.
///
/// A peer that refused the payload fails the write with its outcome, e.g.
/// `-WEIDA_EREJECTED`.
///
/// # Safety
///
/// `t` is NULL or a live outgoing transfer; `buf` is NULL only with
/// `len == 0`, else points to `len` readable bytes; `out_n` is NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_outgoing_transfer_write(
    t: *const weida_outgoing_transfer,
    buf: *const u8,
    len: usize,
    timeout_ms: i64,
    out_n: *mut usize,
) -> c_int {
    guard(|| {
        let out_n = out_ptr(out_n)?;
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(buf, len) }?;
        if data.is_empty() {
            // SAFETY: `out_n` passed `out_ptr`.
            unsafe { write_out(out_n, 0) };
            return Ok(0);
        }
        let written = t.tried.attempt(timeout_ms, |transfer, cx| {
            Pin::new(transfer).poll_write(cx, data).map_err(Error::from)
        })?;
        let n = written.map_err(fail)?;
        // SAFETY: `out_n` passed `out_ptr`.
        unsafe { write_out(out_n, n) };
        Ok(0)
    })
}

/// Ends the payload (`OutgoingTransfer::finish`) and writes its delivery
/// receipt to `out`. **Consumes** `t` on every return path. Fails only with
/// `-WEIDA_EEXPIRED`.
///
/// # Safety
///
/// `t` is NULL or a live outgoing transfer, not used again; `out` is NULL
/// or writable. Release the receipt with `weida_delivery_drop` or
/// `weida_delivery_delivered`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_outgoing_transfer_finish(
    t: *mut weida_outgoing_transfer,
    out: *mut *mut weida_delivery,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let t = unsafe { release(t) }.ok_or_else(crate::error::null_arg)?;
        let transfer = t.tried.close();
        let out = out_ptr(out)?;
        let delivery = transfer.ok_or_else(closed)?.finish().map_err(fail)?;
        let handle = into_handle(weida_delivery {
            delivery: Mutex::new(Some(delivery)),
            exec: t.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases an outgoing transfer. An unfinished payload is reset with
/// `CANCELED`. NULL does nothing.
///
/// # Safety
///
/// `t` is NULL or a live outgoing transfer, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_outgoing_transfer_drop(t: *mut weida_outgoing_transfer) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(t) = unsafe { release(t) } {
            drop(t.tried.close());
        }
    });
}

// --- weida_delivery --------------------------------------------------------

/// Waits for the peer's transport to acknowledge the whole payload
/// (`Delivery::delivered`). **Consumes** `d` on every return path.
///
/// Never blocks; the wait runs as the op written to `out`. Its wait
/// returns 0 when every byte and the end were acknowledged, or the
/// refusal (`-WEIDA_EREJECTED`, ...) or loss that stopped it.
///
/// # Safety
///
/// `d` is NULL or a live receipt, not used again; `out` is NULL or
/// writable. Release the op with `weida_delivery_delivered_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_delivery_delivered(
    d: *mut weida_delivery,
    out: *mut *mut weida_delivery_delivered_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let d = unsafe { release(d) }.ok_or_else(crate::error::null_arg)?;
        let delivery = d.delivery.lock().unwrap_or_else(|e| e.into_inner()).take();
        let out = out_ptr(out)?;
        let delivery = delivery.ok_or_else(closed)?;
        let op = Op::start(&d.exec, delivery.delivered());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_delivery_delivered_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a delivered op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live delivered op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_delivery_delivered_op_fd(
    op: *const weida_delivery_delivered_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the delivery's outcome: 0 when it was
/// delivered.
///
/// # Safety
///
/// `op` is NULL or a live delivered op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_delivery_delivered_op_wait(
    op: *const weida_delivery_delivered_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a delivered op, aborting the wait when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live delivered op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_delivery_delivered_op_drop(op: *mut weida_delivery_delivered_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a receipt without waiting on it. NULL does nothing.
///
/// # Safety
///
/// `d` is NULL or a live receipt, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_delivery_drop(d: *mut weida_delivery) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(d) }));
}
