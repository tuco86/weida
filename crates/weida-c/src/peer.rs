//! The raw peer surface this ABI maps so far: a peer that dials and opens
//! flows, an acceptor, and what it accepts as a tagged handle
//! (docs/c/flow.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use ::weida_runtime::Exec;
use weida_rs::{Acceptor, Error, Flow, FlowMeta, Incoming, Peer};

use crate::config::{Builder, weida_flow_meta};
use crate::error::{WEIDA_EINVAL, closed, fail, fail_abi, guard, guard_ptr, guard_void};
use crate::flow::{weida_flow, weida_incoming_flow};
use crate::handle::{CText, borrow, cstr, into_handle, out_ptr, release, text_out, write_out};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::tls::weida_client_tls;
use crate::try_handle::Tried;

/// `Incoming::Stream`.
pub const WEIDA_INCOMING_STREAM: c_int = 0;
/// `Incoming::Exchange`.
pub const WEIDA_INCOMING_EXCHANGE: c_int = 1;
/// `Incoming::Subscribed`.
pub const WEIDA_INCOMING_SUBSCRIBED: c_int = 2;
/// `Incoming::Credit`.
pub const WEIDA_INCOMING_CREDIT: c_int = 3;
/// `Incoming::Unsubscribed`.
pub const WEIDA_INCOMING_UNSUBSCRIBED: c_int = 4;
/// `Incoming::Flow`.
pub const WEIDA_INCOMING_FLOW: c_int = 5;

/// The dialling side of the stream core (`Peer`).
pub struct weida_peer {
    peer: Arc<Peer>,
    exec: Exec,
}

/// The pending dial of `weida_peer_connect`.
pub struct weida_peer_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_peer_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending registration of `weida_peer_open_flow`.
pub struct weida_peer_open_flow_op {
    op: Arc<Op<Flow>>,
}

impl AsOp for weida_peer_open_flow_op {
    type Output = Flow;
    fn op(&self) -> &Op<Flow> {
        &self.op
    }
}

/// An accept in flight, kept across tries (R3a).
type Accepting = Pin<Box<dyn Future<Output = Result<Incoming, Error>> + Send>>;

/// An acceptor's state between tries: the accept in flight is kept, never
/// recreated, so a timeout or `EINTR` loses nothing (R3a).
struct AcceptorState {
    acceptor: Arc<Acceptor>,
    accepting: Option<Accepting>,
}

/// The bound side of the stream core, serving one endpoint path
/// (`Acceptor`).
pub struct weida_acceptor {
    tried: Tried<AcceptorState>,
    path: CText,
}

/// One thing an acceptor took (`Incoming`), as a tagged handle.
pub struct weida_incoming {
    kind: c_int,
    incoming: Mutex<Option<Incoming>>,
}

impl weida_incoming {
    fn handle(incoming: Incoming) -> *mut weida_incoming {
        let kind = match &incoming {
            Incoming::Stream(_) => WEIDA_INCOMING_STREAM,
            Incoming::Exchange(_) => WEIDA_INCOMING_EXCHANGE,
            Incoming::Subscribed(_) => WEIDA_INCOMING_SUBSCRIBED,
            Incoming::Credit(_) => WEIDA_INCOMING_CREDIT,
            Incoming::Unsubscribed { .. } => WEIDA_INCOMING_UNSUBSCRIBED,
            Incoming::Flow(_) => WEIDA_INCOMING_FLOW,
        };
        into_handle(weida_incoming {
            kind,
            incoming: Mutex::new(Some(incoming)),
        })
    }
}

// --- weida_peer ------------------------------------------------------------

/// Creates a raw peer that dials on `tls`'s terms (`Runtime::peer`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with `weida_peer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_peer(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_peer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_peer {
            peer: Arc::new(rt.runtime.peer(tls.get())),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials `url` (`Peer::connect`), e.g.
/// `weida://sha256:…@127.0.0.1:7443/v` or `weida+inproc://bus/v`. The
/// runtime redials it when the connection is lost.
///
/// Never blocks; the dial runs as the op written to `out`. A peer whose key
/// is not trusted fails its wait with `-WEIDA_EUNTRUSTED`.
///
/// # Safety
///
/// `p` is NULL or a live peer; `url` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the op with
/// `weida_peer_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_connect(
    p: *const weida_peer,
    url: *const c_char,
    out: *mut *mut weida_peer_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let peer = Arc::clone(&p.peer);
        let op = Op::start(&p.exec, async move { peer.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_peer_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_connect_op_fd(
    op: *const weida_peer_connect_op,
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
pub unsafe extern "C" fn weida_peer_connect_op_wait(
    op: *const weida_peer_connect_op,
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
pub unsafe extern "C" fn weida_peer_connect_op_drop(op: *mut weida_peer_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Registers a datagram flow to the next peer, round-robin
/// (`Peer::open_flow`), carrying `meta`, or no keys when `meta` is NULL;
/// `meta` is copied at the call.
///
/// Never blocks; the open runs as the op written to `out`, whose wait
/// returns once the header is written. A refusal arrives later, as the
/// error of a `weida_flow_send`.
///
/// # Safety
///
/// `p` is NULL or a live peer; `meta` is NULL or live flow metadata; `out`
/// is NULL or writable. Release the op with `weida_peer_open_flow_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_flow(
    p: *const weida_peer,
    meta: *const weida_flow_meta,
    out: *mut *mut weida_peer_open_flow_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let meta = if meta.is_null() {
            FlowMeta::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(meta) }?.get()
        };
        let peer = Arc::clone(&p.peer);
        let op = Op::start(&p.exec, async move { peer.open_flow(meta).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_peer_open_flow_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of an open-flow op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live open-flow op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_flow_op_fd(
    op: *const weida_peer_open_flow_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the registration and writes the flow
/// to `out`. Fails with `-WEIDA_EDATAGRAMS_UNAVAILABLE` when the connection
/// did not agree datagrams (both runtimes need `datagram_receive_bytes`
/// above 0), `-WEIDA_ENOT_CONNECTED` for a peer never connected, and
/// `-WEIDA_ELIMIT_EXCEEDED` for a label above 256 bytes.
///
/// # Safety
///
/// `op` is NULL or a live open-flow op; `out` is NULL or writable. Release
/// the flow with `weida_flow_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_flow_op_wait(
    op: *const weida_peer_open_flow_op,
    timeout_ms: i64,
    out: *mut *mut weida_flow,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let op = unsafe { borrow(op) }?;
        let flow = op.op.wait(timeout_ms)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_flow::handle(flow)) };
        Ok(0)
    })
}

/// Releases an open-flow op. An unfinished open is aborted; a flow it
/// registered that no wait took is closed. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live open-flow op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_flow_op_drop(op: *mut weida_peer_open_flow_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a peer (`Drop for Peer`). NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live peer, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_drop(p: *mut weida_peer) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(p) }));
}

// --- weida_acceptor --------------------------------------------------------

/// Registers a raw acceptor at the endpoint path `path`
/// (`Listener::acceptor`): it takes both stream kinds and flows on the
/// path, tagged. Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_acceptor_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_acceptor(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_acceptor,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let acceptor = l.listener.acceptor(path).map_err(fail)?;
        let handle = into_handle(weida_acceptor {
            path: CText::new(acceptor.path()),
            tried: Tried::new(AcceptorState {
                acceptor: Arc::new(acceptor),
                accepting: None,
            }),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this acceptor serves. Lives as long as the acceptor.
///
/// # Safety
///
/// `a` is NULL or a live acceptor; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_acceptor_path(
    a: *const weida_acceptor,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if a.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `a` is a live acceptor by this function's
        // contract; the text lives as long as it.
        let a = unsafe { &*a };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&a.path), len_out) }
    })
}

/// The acceptor's readiness descriptor (R11): readable when the next
/// `weida_acceptor_accept` will not block.
///
/// # Safety
///
/// `a` is NULL or a live acceptor; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_acceptor_fd(a: *const weida_acceptor, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let a = unsafe { borrow(a) }?;
        let fd = a.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next thing that arrived on the path, waiting up to
/// `timeout_ms` (R4) for one (`Acceptor::accept`), as a tagged handle.
/// `-WEIDA_ENOT_CONNECTED` once the queue ended.
///
/// # Safety
///
/// `a` is NULL or a live acceptor; `out` is NULL or writable. Release the
/// result with `weida_incoming_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_acceptor_accept(
    a: *const weida_acceptor,
    timeout_ms: i64,
    out: *mut *mut weida_incoming,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let a = unsafe { borrow(a) }?;
        let accepted = a.tried.attempt(timeout_ms, |state, cx| {
            let accepting = state.accepting.get_or_insert_with(|| {
                let acceptor = Arc::clone(&state.acceptor);
                Box::pin(async move { acceptor.accept().await })
            });
            let polled = accepting.as_mut().poll(cx);
            if polled.is_ready() {
                state.accepting = None;
            }
            polled
        })?;
        let incoming = accepted.map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_incoming::handle(incoming)) };
        Ok(0)
    })
}

/// Releases an acceptor; its path stays registered, as a dropped Rust
/// `Acceptor` leaves it. A thread waiting in `weida_acceptor_accept`
/// returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `a` is NULL or a live acceptor, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_acceptor_drop(a: *mut weida_acceptor) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(a) = unsafe { release(a) } {
            drop(a.tried.close());
        }
    });
}

// --- weida_incoming --------------------------------------------------------

/// What the acceptor took, a `WEIDA_INCOMING_*` constant.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_kind(i: *const weida_incoming) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        Ok(unsafe { borrow(i) }?.kind)
    })
}

/// Hands a flow over, once. A second call is `-WEIDA_ECLOSED`, another
/// kind `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `out` is NULL or writable.
/// Release the flow with `weida_incoming_flow_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_take_flow(
    i: *const weida_incoming,
    out: *mut *mut weida_incoming_flow,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let i = unsafe { borrow(i) }?;
        if i.kind != WEIDA_INCOMING_FLOW {
            return Err(fail_abi(WEIDA_EINVAL, c"this incoming is not a flow"));
        }
        let incoming = i
            .incoming
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(closed)?;
        let Incoming::Flow(flow) = incoming else {
            unreachable!("the kind names the variant");
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_incoming_flow::handle(flow)) };
        Ok(0)
    })
}

/// Releases an incoming handle and drops what was not taken, as Rust does:
/// a stream is refused `REJECTED`, an exchange answers `NO_REPLY`, and a
/// flow is stopped `CANCELED`. NULL does nothing.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_drop(i: *mut weida_incoming) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(i) }));
}
