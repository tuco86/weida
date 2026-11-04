//! The requesting half of Req/Rep (docs/c/requester.md).

use std::ffi::{c_char, c_int};
use std::sync::{Arc, Mutex};

use ::weida_runtime::Exec;
use weida_rs::{IncomingTransfer, OutgoingTransfer, ReplyStream, Requester, TransferMeta};

use crate::error::{closed, guard, guard_void, null_arg};
use crate::handle::{borrow, bytes, cstr, into_handle, out_ptr, release, write_out};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::tls::{weida_client_tls, weida_transfer_meta};
use crate::transfer::{weida_incoming_transfer, weida_outgoing_transfer};

/// The requesting half of Req/Rep: dials repliers and sends them requests.
pub struct weida_requester {
    requester: Arc<Requester>,
    exec: Exec,
}

/// The pending dial of `weida_requester_connect`.
pub struct weida_requester_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_requester_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending exchange of `weida_requester_request`.
pub struct weida_requester_request_op {
    op: Arc<Op<IncomingTransfer>>,
    exec: Exec,
}

/// The pending exchange of `weida_requester_open`.
pub struct weida_requester_open_op {
    op: Arc<Op<(OutgoingTransfer, ReplyStream)>>,
    exec: Exec,
}

impl AsOp for weida_requester_open_op {
    type Output = (OutgoingTransfer, ReplyStream);
    fn op(&self) -> &Op<(OutgoingTransfer, ReplyStream)> {
        &self.op
    }
}

/// The reply half of an exchange `weida_requester_open` opened.
pub struct weida_reply_stream {
    stream: Mutex<Option<ReplyStream>>,
    exec: Exec,
}

/// The pending wait of `weida_reply_stream_recv`.
pub struct weida_reply_stream_recv_op {
    op: Arc<Op<IncomingTransfer>>,
    exec: Exec,
}

impl AsOp for weida_reply_stream_recv_op {
    type Output = IncomingTransfer;
    fn op(&self) -> &Op<IncomingTransfer> {
        &self.op
    }
}

/// Creates a requester that dials on `tls`'s terms (`Runtime::requester`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with
/// `weida_requester_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_requester(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_requester,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_requester {
            requester: Arc::new(rt.runtime.requester(tls.get())),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials `url` (`Requester::connect`), e.g.
/// `weida://sha256:…@127.0.0.1:7443/echo` or `weida+inproc://bus/echo`.
/// The runtime redials it when the connection is lost.
///
/// Never blocks; the dial runs as the op written to `out`. A peer whose key
/// is not trusted fails its wait with `-WEIDA_EUNTRUSTED`.
///
/// # Safety
///
/// `r` is NULL or a live requester; `url` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the op with
/// `weida_requester_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_connect(
    r: *const weida_requester,
    url: *const c_char,
    out: *mut *mut weida_requester_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let requester = Arc::clone(&r.requester);
        let op = Op::start(&r.exec, async move { requester.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_requester_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_connect_op_fd(
    op: *const weida_requester_connect_op,
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
pub unsafe extern "C" fn weida_requester_connect_op_wait(
    op: *const weida_requester_connect_op,
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
pub unsafe extern "C" fn weida_requester_connect_op_drop(op: *mut weida_requester_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Sends `body` as one request (`Requester::request`). The body is copied.
///
/// Never blocks; the exchange runs as the op written to `out`, whose wait
/// yields the reply as an incoming transfer as soon as its header arrives.
/// A request the replier dropped fails the wait with `-WEIDA_ENO_REPLY`.
///
/// # Safety
///
/// `r` is NULL or a live requester; `body` is NULL only with `len == 0`,
/// else points to `len` readable bytes; `out` is NULL or writable. Release
/// the op with `weida_requester_request_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_request(
    r: *const weida_requester,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_requester_request_op,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { request(r, || Ok(TransferMeta::default()), body, len, out) })
}

/// Sends `body` as one request carrying `meta` (`Requester::request_with`):
/// `weida_requester_request` with metadata. `meta` is copied at the call;
/// NULL is `TransferMeta::default()`. The body is copied.
///
/// Never blocks; the exchange runs as the op written to `out`, whose wait
/// yields the reply as an incoming transfer as soon as its header arrives.
/// A request the replier dropped fails the wait with `-WEIDA_ENO_REPLY`; a
/// topic over 256 bytes fails it with `-WEIDA_ELIMIT_EXCEEDED`.
///
/// # Safety
///
/// `r` is NULL or a live requester; `meta` is NULL or live metadata; `body`
/// is NULL only with `len == 0`, else points to `len` readable bytes; `out`
/// is NULL or writable. Release the op with `weida_requester_request_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_request_with(
    r: *const weida_requester,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_requester_request_op,
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
    guard(|| unsafe { request(r, meta, body, len, out) })
}

/// The body of both request exports.
///
/// # Safety
///
/// As `weida_requester_request_with`.
unsafe fn request(
    r: *const weida_requester,
    meta: impl FnOnce() -> Result<TransferMeta, c_int>,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_requester_request_op,
) -> Result<c_int, c_int> {
    let out = out_ptr(out)?;
    // SAFETY: forwarded from this function's contract.
    let r = unsafe { borrow(r) }?;
    let meta = meta()?;
    // SAFETY: forwarded from this function's contract.
    let body = unsafe { bytes(body, len) }?.to_vec();
    let requester = Arc::clone(&r.requester);
    let op = Op::start(
        &r.exec,
        async move { requester.request_with(meta, &body).await },
    );
    let handle = into_handle(weida_requester_request_op {
        op,
        exec: r.exec.clone(),
    });
    // SAFETY: `out` passed `out_ptr`.
    unsafe { write_out(out, handle) };
    Ok(0)
}

/// The readiness descriptor of a request op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live request op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_request_op_fd(
    op: *const weida_requester_request_op,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let op = unsafe { borrow(op) }?;
        let fd = op.op.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Waits up to `timeout_ms` (R4) for the reply and writes it to `out`, an
/// incoming transfer read like a socket or collected whole.
///
/// # Safety
///
/// `op` is NULL or a live request op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_request_op_wait(
    op: *const weida_requester_request_op,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let op = unsafe { borrow(op) }?;
        let reply = op.op.wait(timeout_ms)?;
        let handle = weida_incoming_transfer::handle(reply, op.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a request op, abandoning the exchange when unfinished. NULL
/// does nothing.
///
/// # Safety
///
/// `op` is NULL or a live request op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_request_op_drop(op: *mut weida_requester_request_op) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(op) = unsafe { release(op) } {
            op.op.close();
        }
    });
}

/// Opens one exchange (`Requester::open`) carrying `meta`, or empty
/// metadata when `meta` is NULL; `meta` is copied at the call.
///
/// Never blocks; the exchange opens as the op written to `out`. Its wait
/// yields the request body, written like a socket and ended with
/// `weida_outgoing_transfer_finish`, and the reply stream, whose
/// `weida_reply_stream_recv` waits for the reply. The two halves are
/// independent: the reply can be awaited and read while the body is still
/// open.
///
/// # Safety
///
/// `r` is NULL or a live requester; `meta` is NULL or live metadata; `out`
/// is NULL or writable. Release the op with `weida_requester_open_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_open(
    r: *const weida_requester,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_requester_open_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let meta = if meta.is_null() {
            TransferMeta::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(meta) }?.get()
        };
        let requester = Arc::clone(&r.requester);
        let op = Op::start(&r.exec, async move { requester.open(meta).await });
        let handle = into_handle(weida_requester_open_op {
            op,
            exec: r.exec.clone(),
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
pub unsafe extern "C" fn weida_requester_open_op_fd(
    op: *const weida_requester_open_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the exchange and writes both halves:
/// the request body to `out_body` and the reply stream to `out_reply`.
/// Both are written, or neither.
///
/// # Safety
///
/// `op` is NULL or a live open op; `out_body` and `out_reply` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_open_op_wait(
    op: *const weida_requester_open_op,
    timeout_ms: i64,
    out_body: *mut *mut weida_outgoing_transfer,
    out_reply: *mut *mut weida_reply_stream,
) -> c_int {
    guard(|| {
        let out_body = out_ptr(out_body)?;
        let out_reply = out_ptr(out_reply)?;
        // SAFETY: forwarded from this function's contract.
        let handle = unsafe { borrow(op) }?;
        let (body, reply) = handle.op.wait(timeout_ms)?;
        let body = weida_outgoing_transfer::handle(body, handle.exec.clone());
        let reply = into_handle(weida_reply_stream {
            stream: Mutex::new(Some(reply)),
            exec: handle.exec.clone(),
        });
        // SAFETY: both out-parameters passed `out_ptr`.
        unsafe {
            write_out(out_body, body);
            write_out(out_reply, reply);
        }
        Ok(0)
    })
}

/// Releases an open op. An unfinished open is aborted; an exchange it
/// opened that no wait took is abandoned: its body is reset `CANCELED` and
/// its reply half stopped `CANCELED`. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live open op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_open_op_drop(op: *mut weida_requester_open_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Waits for the reply (`ReplyStream::recv`). **Consumes** `s` on every
/// return path.
///
/// Never blocks; the wait runs as the op written to `out`. Its wait yields
/// the reply as an incoming transfer as soon as its header arrives, or the
/// error the replier answered with (README R9's `ErrorCode` table).
///
/// A NULL `out` fails with `-WEIDA_EINVAL`, and the stream is dropped all
/// the same: the replier sees `CANCELED`.
///
/// # Safety
///
/// `s` is NULL or a live reply stream, not used again; `out` is NULL or
/// writable. Release the op with `weida_reply_stream_recv_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reply_stream_recv(
    s: *mut weida_reply_stream,
    out: *mut *mut weida_reply_stream_recv_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let s = unsafe { release(s) }.ok_or_else(null_arg)?;
        let stream = s.stream.lock().unwrap_or_else(|e| e.into_inner()).take();
        let out = out_ptr(out)?;
        let stream = stream.ok_or_else(closed)?;
        let op = Op::start(&s.exec, stream.recv());
        let handle = into_handle(weida_reply_stream_recv_op {
            op,
            exec: s.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The readiness descriptor of a recv op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live recv op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reply_stream_recv_op_fd(
    op: *const weida_reply_stream_recv_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the reply and writes it to `out`, an
/// incoming transfer read like a socket or collected whole.
///
/// # Safety
///
/// `op` is NULL or a live recv op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reply_stream_recv_op_wait(
    op: *const weida_reply_stream_recv_op,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let op = unsafe { borrow(op) }?;
        let reply = op.op.wait(timeout_ms)?;
        let handle = weida_incoming_transfer::handle(reply, op.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a recv op. Dropped unfinished, it aborts the wait, and the
/// replier sees the requester gone. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live recv op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reply_stream_recv_op_drop(op: *mut weida_reply_stream_recv_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a reply stream without waiting. Its reply half is stopped with
/// `CANCELED`, so the replier learns nobody is listening (its
/// `weida_incoming_request_canceled` op settles). NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live reply stream, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_reply_stream_drop(s: *mut weida_reply_stream) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}

/// Releases a requester, which drops its connections. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live requester, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_requester_drop(r: *mut weida_requester) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(r) }));
}
