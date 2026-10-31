//! The listener, its bindings, the replier and the requests it accepts
//! (docs/c/listener.md).

use std::ffi::{c_char, c_int};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use ::weida_runtime::Exec;
use weida_rs::{Binding, Error, IncomingRequest, Listener, LocalBinding, Replier, TransferMeta};

use crate::error::{fail, guard, guard_ptr, guard_void};
use crate::handle::{CText, borrow, cstr, into_handle, out_ptr, release, text_out, write_out};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::tls::{weida_server_tls, weida_transfer_meta};
use crate::transfer::{
    meta_of, weida_incoming_meta, weida_incoming_transfer, weida_outgoing_transfer,
};
use crate::try_handle::Tried;

/// Where a runtime serves: bindings and the endpoints registered on them.
pub struct weida_listener {
    listener: Listener,
    exec: Exec,
}

/// The pending bind of `weida_listener_bind_quic`.
pub struct weida_listener_bind_quic_op {
    op: Arc<Op<Binding>>,
}

impl AsOp for weida_listener_bind_quic_op {
    type Output = Binding;
    fn op(&self) -> &Op<Binding> {
        &self.op
    }
}

/// A QUIC binding: the socket a listener serves on.
pub struct weida_binding {
    _binding: Binding,
    local_addr: CText,
}

/// An in-process binding: the bus a listener answers on.
pub struct weida_local_binding {
    _binding: LocalBinding,
    bus: CText,
}

type Accepting = Pin<Box<dyn Future<Output = Result<IncomingRequest, Error>> + Send>>;

/// A replier's state between tries: the accept in flight is kept, never
/// recreated, so a timeout or `EINTR` loses nothing (R3a).
struct ReplierState {
    replier: Arc<Replier>,
    accepting: Option<Accepting>,
}

/// The replying half of Req/Rep, serving one endpoint path.
pub struct weida_replier {
    tried: Tried<ReplierState>,
    path: CText,
    exec: Exec,
}

struct RequestState {
    request: IncomingRequest,
    body_taken: bool,
}

/// One accepted request: its body and the right to reply.
pub struct weida_incoming_request {
    state: Mutex<Option<RequestState>>,
    meta: OnceLock<Arc<weida_incoming_meta>>,
    exec: Exec,
}

/// The pending reply of `weida_incoming_request_reply`.
pub struct weida_incoming_request_reply_op {
    op: Arc<Op<weida_rs::OutgoingTransfer>>,
    exec: Exec,
}

impl AsOp for weida_incoming_request_reply_op {
    type Output = weida_rs::OutgoingTransfer;
    fn op(&self) -> &Op<weida_rs::OutgoingTransfer> {
        &self.op
    }
}

/// Creates the runtime's listener (`Runtime::listener`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `out` is NULL or writable. Release the
/// result with `weida_listener_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_listener(
    rt: *const weida_runtime,
    out: *mut *mut weida_listener,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        let handle = into_handle(weida_listener {
            listener: rt.runtime.listener(),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Serves QUIC on `addr` (`Listener::bind_quic`), e.g. `"127.0.0.1:0"`,
/// presenting `tls`'s identity.
///
/// Never blocks; the bind runs as the op written to `out`, whose wait
/// yields the binding. An address that does not parse fails at once with
/// `-WEIDA_EINVALID_ADDRESS`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `addr` is NULL or a NUL-terminated
/// string; `tls` is NULL or live server terms; `out` is NULL or writable.
/// Release the op with `weida_listener_bind_quic_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic(
    l: *const weida_listener,
    addr: *const c_char,
    tls: *const weida_server_tls,
    out: *mut *mut weida_listener_bind_quic_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let text = unsafe { cstr(addr) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let addr: SocketAddr = text
            .parse()
            .map_err(|_| fail(Error::InvalidAddress(text.to_owned())))?;
        let listener = l.listener.clone();
        let tls = tls.tls.clone();
        let op = Op::start(&l.exec, async move { listener.bind_quic(addr, tls).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_listener_bind_quic_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a bind op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live bind op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_op_fd(
    op: *const weida_listener_bind_quic_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the binding and writes it to `out`.
/// Release it with `weida_binding_drop`, which stops serving.
///
/// # Safety
///
/// `op` is NULL or a live bind op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_op_wait(
    op: *const weida_listener_bind_quic_op,
    timeout_ms: i64,
    out: *mut *mut weida_binding,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let binding = unsafe { op_wait(op, timeout_ms) }?;
        let local_addr = CText::new(binding.local_addr().to_string());
        let handle = into_handle(weida_binding {
            _binding: binding,
            local_addr,
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a bind op, aborting the bind when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live bind op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_op_drop(op: *mut weida_listener_bind_quic_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Answers on the in-process bus `bus` (`Listener::bind_inproc`): a
/// requester in this process reaches it at `weida+inproc://<bus>/<path>`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `bus` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_local_binding_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_inproc(
    l: *const weida_listener,
    bus: *const c_char,
    out: *mut *mut weida_local_binding,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let bus = unsafe { cstr(bus) }?;
        let binding = l.listener.bind_inproc(bus).map_err(fail)?;
        let bus = CText::new(binding.bus());
        let handle = into_handle(weida_local_binding {
            _binding: binding,
            bus,
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Registers a replier at the endpoint path `path` (`Listener::replier`).
/// Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_replier_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_replier(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_replier,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let replier = l.listener.replier(path).map_err(fail)?;
        let handle = into_handle(weida_replier {
            path: CText::new(replier.path()),
            tried: Tried::new(ReplierState {
                replier: Arc::new(replier),
                accepting: None,
            }),
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a listener. Its bindings and endpoints stay. NULL does nothing.
///
/// # Safety
///
/// `l` is NULL or a live listener, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_drop(l: *mut weida_listener) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(l) }));
}

/// The address the binding serves on, with the port resolved, e.g.
/// `"127.0.0.1:40123"`. Lives as long as the binding.
///
/// # Safety
///
/// `b` is NULL or a live binding; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_local_addr(
    b: *const weida_binding,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if b.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `b` is a live binding by this function's
        // contract; the text lives as long as it.
        let b = unsafe { &*b };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&b.local_addr), len_out) }
    })
}

/// Releases a binding, which stops serving on it. NULL does nothing.
///
/// # Safety
///
/// `b` is NULL or a live binding, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_drop(b: *mut weida_binding) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(b) }));
}

/// The bus the binding answers on. Lives as long as the binding.
///
/// # Safety
///
/// `b` is NULL or a live local binding; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_local_binding_bus(
    b: *const weida_local_binding,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if b.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `b` is a live local binding by this function's
        // contract; the text lives as long as it.
        let b = unsafe { &*b };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&b.bus), len_out) }
    })
}

/// Releases a local binding, which frees its bus name. NULL does nothing.
///
/// # Safety
///
/// `b` is NULL or a live local binding, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_local_binding_drop(b: *mut weida_local_binding) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(b) }));
}

/// The endpoint path this replier serves. Lives as long as the replier.
///
/// # Safety
///
/// `r` is NULL or a live replier; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_replier_path(
    r: *const weida_replier,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if r.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `r` is a live replier by this function's
        // contract; the text lives as long as it.
        let r = unsafe { &*r };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&r.path), len_out) }
    })
}

/// The replier's readiness descriptor (R11): readable when the next
/// `weida_replier_accept` will not block.
///
/// # Safety
///
/// `r` is NULL or a live replier; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_replier_fd(r: *const weida_replier, out: *mut i64) -> c_int {
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

/// Takes the next request, waiting up to `timeout_ms` (R4) for one
/// (`Replier::accept`). The request arrives as soon as its header does;
/// its body is read from `weida_incoming_request_take_body`.
///
/// # Safety
///
/// `r` is NULL or a live replier; `out` is NULL or writable. Release the
/// request with `weida_incoming_request_drop` or reply to it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_replier_accept(
    r: *const weida_replier,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_request,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let r = unsafe { borrow(r) }?;
        let accepted = r.tried.attempt(timeout_ms, |state, cx| {
            let accepting = state.accepting.get_or_insert_with(|| {
                let replier = Arc::clone(&state.replier);
                Box::pin(async move { replier.accept().await })
            });
            let polled = accepting.as_mut().poll(cx);
            if polled.is_ready() {
                state.accepting = None;
            }
            polled
        })?;
        let request = accepted.map_err(fail)?;
        let handle = into_handle(weida_incoming_request {
            state: Mutex::new(Some(RequestState {
                request,
                body_taken: false,
            })),
            meta: OnceLock::new(),
            exec: r.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a replier, which unregisters its path. A thread waiting in
/// `weida_replier_accept` returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `r` is NULL or a live replier, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_replier_drop(r: *mut weida_replier) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(r) = unsafe { release(r) } {
            drop(r.tried.close());
        }
    });
}

impl weida_incoming_request {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<RequestState>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Writes a new reference to the request's metadata. Release it with
/// `weida_incoming_meta_drop`; it outlives the request.
///
/// # Safety
///
/// `q` is NULL or a live request; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_meta(
    q: *const weida_incoming_request,
    out: *mut *mut weida_incoming_meta,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let q = unsafe { borrow(q) }?;
        let meta = meta_of(&q.meta, || match q.lock().as_ref() {
            Some(state) => Ok(state.request.meta().clone()),
            None => Err(crate::error::closed()),
        })?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, meta) };
        Ok(0)
    })
}

/// Detaches the request's body, to be read like a socket
/// (`IncomingRequest::take_body`). A second call returns `-WEIDA_ECLOSED`.
///
/// # Safety
///
/// `q` is NULL or a live request; `out` is NULL or writable. Release the
/// body with `weida_incoming_transfer_drop` or collect it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_take_body(
    q: *const weida_incoming_request,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let q = unsafe { borrow(q) }?;
        let body = {
            let mut state = q.lock();
            match state.as_mut() {
                Some(state) if !state.body_taken => {
                    state.body_taken = true;
                    state.request.take_body()
                }
                _ => return Err(crate::error::closed()),
            }
        };
        let handle = weida_incoming_transfer::handle(body, q.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Opens the reply stream (`IncomingRequest::reply`) with `meta`, or empty
/// metadata when `meta` is NULL. **Consumes** `q` on every return path.
///
/// Never blocks; the reply opens as the op written to `out`, whose wait
/// yields the outgoing transfer the reply is written to. A body not taken
/// is refused.
///
/// # Safety
///
/// `q` is NULL or a live request, not used again; `meta` is NULL or live
/// metadata; `out` is NULL or writable. Release the op with
/// `weida_incoming_request_reply_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_reply(
    q: *mut weida_incoming_request,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_incoming_request_reply_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let q = unsafe { release(q) }.ok_or_else(crate::error::null_arg)?;
        let state = q.lock().take();
        let out = out_ptr(out)?;
        let state = state.ok_or_else(crate::error::closed)?;
        let meta = if meta.is_null() {
            TransferMeta::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(meta) }?.get()
        };
        let op = Op::start(&q.exec, state.request.reply(meta));
        let handle = into_handle(weida_incoming_request_reply_op {
            op,
            exec: q.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The readiness descriptor of a reply op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live reply op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_reply_op_fd(
    op: *const weida_incoming_request_reply_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the reply stream and writes it to
/// `out`. Write the reply to it and finish it; dropping it unfinished
/// resets the reply with `CANCELED`.
///
/// # Safety
///
/// `op` is NULL or a live reply op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_reply_op_wait(
    op: *const weida_incoming_request_reply_op,
    timeout_ms: i64,
    out: *mut *mut weida_outgoing_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let handle = unsafe { borrow(op) }?;
        let transfer = handle.op.wait(timeout_ms)?;
        let transfer = weida_outgoing_transfer::handle(transfer, handle.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, transfer) };
        Ok(0)
    })
}

/// Releases a reply op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live reply op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_reply_op_drop(
    op: *mut weida_incoming_request_reply_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a request. One not replied to answers the requester with
/// `NO_REPLY`. NULL does nothing.
///
/// # Safety
///
/// `q` is NULL or a live request, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_drop(q: *mut weida_incoming_request) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(q) = unsafe { release(q) } {
            drop(q.lock().take());
        }
    });
}
