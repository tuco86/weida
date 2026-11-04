//! The relay a listener serves, and the registrations and tunnels it is
//! asked to serve (docs/c/relay.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use ::weida_runtime::Exec;
use weida_rs::{Error, PeerIdentity, Relay, RelayRequest};

use crate::error::{closed, fail, guard, guard_ptr, guard_void, null_arg};
use crate::handle::{CText, borrow, into_handle, out_ptr, release, text_out, write_out};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait};
use crate::try_handle::Tried;

type Accepting = Pin<Box<dyn Future<Output = Result<RelayRequest, Error>> + Send>>;

/// A relay's state between tries: the accept in flight is kept, never
/// recreated, so a timeout or `EINTR` loses nothing (R3a).
struct RelayState {
    relay: Arc<Relay>,
    accepting: Option<Accepting>,
}

/// The relay a listener serves on `/.weida/relay` (`Relay`), a try handle
/// (R3a).
pub struct weida_relay {
    tried: Tried<RelayState>,
    exec: Exec,
}

/// A registration or a tunnel asking the relay to be served
/// (`RelayRequest`).
pub struct weida_relay_request {
    request: Mutex<Option<RelayRequest>>,
    peer_fingerprint: Option<CText>,
    target: Option<CText>,
    exec: Exec,
}

impl weida_relay_request {
    fn lock(&self) -> MutexGuard<'_, Option<RelayRequest>> {
        self.request.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The pending admission of `weida_relay_request_admit`.
pub struct weida_relay_request_admit_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_relay_request_admit_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending refusal of `weida_relay_request_refuse`.
pub struct weida_relay_request_refuse_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_relay_request_refuse_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// Serves a relay on this listener (`Listener::relay`): hidden peers
/// register through `weida_listener_bind_relay`, and diallers reach them
/// through `weida+relay://` URLs. Claims `/.weida/relay` on every binding of
/// the listener, so a second call fails with `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `out` is NULL or writable. Release the
/// result with `weida_relay_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_relay(
    l: *const weida_listener,
    out: *mut *mut weida_relay,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        let relay = l.listener.relay().map_err(fail)?;
        let handle = into_handle(weida_relay {
            tried: Tried::new(RelayState {
                relay: Arc::new(relay),
                accepting: None,
            }),
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The relay's readiness descriptor (R11): readable when the next
/// `weida_relay_accept` will not block.
///
/// # Safety
///
/// `r` is NULL or a live relay; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_fd(r: *const weida_relay, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let fd = r.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next registration or tunnel, waiting up to `timeout_ms` (R4)
/// for one (`Relay::accept`).
///
/// An anonymous registration, a tunnel to a key nobody registered, and an
/// unknown preamble are refused before they get here.
/// `-WEIDA_ENOT_CONNECTED` once the relay stopped serving.
///
/// # Safety
///
/// `r` is NULL or a live relay; `out` is NULL or writable. Answer the
/// request with `weida_relay_request_admit` or `_refuse`, or release it
/// with `weida_relay_request_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_accept(
    r: *const weida_relay,
    timeout_ms: i64,
    out: *mut *mut weida_relay_request,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let accepted = r.tried.attempt(timeout_ms, |state, cx| {
            let accepting = state.accepting.get_or_insert_with(|| {
                let relay = Arc::clone(&state.relay);
                Box::pin(async move { relay.accept().await })
            });
            let polled = accepting.as_mut().poll(cx);
            if polled.is_ready() {
                state.accepting = None;
            }
            polled
        })?;
        let request = accepted.map_err(fail)?;
        let peer_fingerprint = match request.peer() {
            Some(PeerIdentity::Key(fp)) => Some(CText::new(fp.to_string())),
            _ => None,
        };
        let target = request.target().map(|fp| CText::new(fp.to_string()));
        let handle = into_handle(weida_relay_request {
            request: Mutex::new(Some(request)),
            peer_fingerprint,
            target,
            exec: r.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases the relay: `/.weida/relay` is unregistered and every
/// registration's connection is closed with `SHUTDOWN`, so the hidden peers
/// redial. A thread waiting in `weida_relay_accept` returns
/// `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live relay, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_drop(r: *mut weida_relay) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(r) = unsafe { release(r) } {
            drop(r.tried.close());
        }
    });
}

/// The key (`sha256:` text) the asking peer proved; NULL for an anonymous
/// dialler. Always a key for a registration. Lives as long as the request.
///
/// # Safety
///
/// `q` is NULL or a live request; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_peer_fingerprint(
    q: *const weida_relay_request,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if q.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `q` is a live request by this function's
        // contract; the text lives as long as it.
        let q = unsafe { &*q };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(q.peer_fingerprint.as_ref(), len_out) }
    })
}

/// NULL for a registration; for a tunnel, the key (`sha256:` text) of the
/// peer it reaches. Lives as long as the request.
///
/// # Safety
///
/// `q` is NULL or a live request; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_target(
    q: *const weida_relay_request,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if q.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `q` is a live request by this function's
        // contract; the text lives as long as it.
        let q = unsafe { &*q };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(q.target.as_ref(), len_out) }
    })
}

/// Serves the registration or tunnel (`RelayRequest::admit`). **Consumes**
/// `q` on every return path.
///
/// A registration replaces one of the same key on another connection, and
/// closes that connection. Never blocks; the admission runs as the op
/// written to `out`. Its wait returns `-WEIDA_EALREADY_REGISTERED` for a
/// second registration on one connection, `-WEIDA_EUNKNOWN_ENDPOINT` for a
/// tunnel whose target went away, and a connection error when the asker is
/// gone.
///
/// # Safety
///
/// `q` is NULL or a live request, not used again; `out` is NULL or
/// writable. Release the op with `weida_relay_request_admit_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_admit(
    q: *mut weida_relay_request,
    out: *mut *mut weida_relay_request_admit_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let q = unsafe { release(q) }.ok_or_else(null_arg)?;
        let request = q.lock().take();
        let out = out_ptr(out)?;
        let request = request.ok_or_else(closed)?;
        let op = Op::start(&q.exec, request.admit());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_relay_request_admit_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of an admit op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live admit op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_admit_op_fd(
    op: *const weida_relay_request_admit_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the admission; 0 once it is served.
///
/// # Safety
///
/// `op` is NULL or a live admit op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_admit_op_wait(
    op: *const weida_relay_request_admit_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases an admit op. Dropped unfinished, it aborts the admission, and
/// the asker may see `-WEIDA_ENO_REPLY`. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live admit op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_admit_op_drop(op: *mut weida_relay_request_admit_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Refuses the registration or tunnel (`RelayRequest::refuse`): the asker
/// fails with `-WEIDA_EREJECTED` (a hidden peer's
/// `weida_listener_bind_relay` op, a dialler's connect op). **Consumes**
/// `q` on every return path.
///
/// # Safety
///
/// `q` is NULL or a live request, not used again; `out` is NULL or
/// writable. Release the op with `weida_relay_request_refuse_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_refuse(
    q: *mut weida_relay_request,
    out: *mut *mut weida_relay_request_refuse_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let q = unsafe { release(q) }.ok_or_else(null_arg)?;
        let request = q.lock().take();
        let out = out_ptr(out)?;
        let request = request.ok_or_else(closed)?;
        let op = Op::start(&q.exec, async move {
            request.refuse().await;
            Ok(())
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_relay_request_refuse_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a refuse op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live refuse op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_refuse_op_fd(
    op: *const weida_relay_request_refuse_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the refusal; 0 once it was written or
/// could not be (the asker is gone).
///
/// # Safety
///
/// `op` is NULL or a live refuse op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_refuse_op_wait(
    op: *const weida_relay_request_refuse_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a refuse op. Dropped unfinished, it aborts the refusal, and the
/// asker may see `-WEIDA_ENO_REPLY` instead. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live refuse op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_refuse_op_drop(
    op: *mut weida_relay_request_refuse_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a request; one not answered answers `NO_REPLY`. NULL does
/// nothing.
///
/// # Safety
///
/// `q` is NULL or a live request, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_relay_request_drop(q: *mut weida_relay_request) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(q) }));
}
