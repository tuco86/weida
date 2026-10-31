//! The requesting half of Req/Rep (docs/c/requester.md).

use std::ffi::{c_char, c_int};
use std::sync::Arc;

use ::weida_runtime::Exec;
use weida_rs::{IncomingTransfer, Requester};

use crate::error::{guard, guard_void};
use crate::handle::{borrow, bytes, cstr, into_handle, out_ptr, release, write_out};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::tls::weida_client_tls;
use crate::transfer::weida_incoming_transfer;

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
            requester: Arc::new(rt.runtime.requester(tls.tls.clone())),
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
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        // SAFETY: forwarded from this function's contract.
        let body = unsafe { bytes(body, len) }?.to_vec();
        let requester = Arc::clone(&r.requester);
        let op = Op::start(&r.exec, async move { requester.request(&body).await });
        let handle = into_handle(weida_requester_request_op {
            op,
            exec: r.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
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
