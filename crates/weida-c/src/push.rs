//! The two halves of Push/Pull (docs/c/push.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::sync::Arc;

use ::weida_runtime::Exec;
use weida_rs::{Error, IncomingTransfer, OutgoingTransfer, Puller, Pusher, TransferMeta};

use crate::error::{fail, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::stats::{weida_connection_stats, weida_peer_events};
use crate::tls::{weida_client_tls, weida_transfer_meta};
use crate::transfer::{weida_incoming_transfer, weida_outgoing_transfer};
use crate::try_handle::Tried;

/// The sending half of Push/Pull: dials pullers and pushes to them
/// round-robin.
pub struct weida_pusher {
    pusher: Arc<Pusher>,
    exec: Exec,
}

/// The pending dial of `weida_pusher_connect`.
pub struct weida_pusher_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_pusher_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending send of `weida_pusher_send` and `weida_pusher_send_with`.
pub struct weida_pusher_send_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_pusher_send_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending open of `weida_pusher_open`.
pub struct weida_pusher_open_op {
    op: Arc<Op<OutgoingTransfer>>,
    exec: Exec,
}

impl AsOp for weida_pusher_open_op {
    type Output = OutgoingTransfer;
    fn op(&self) -> &Op<OutgoingTransfer> {
        &self.op
    }
}

/// A queue take in flight, kept across tries (R3a).
pub(crate) type Receiving = Pin<Box<dyn Future<Output = Result<IncomingTransfer, Error>> + Send>>;

/// A puller's state between tries: the recv in flight is kept, never
/// recreated, so a timeout or `EINTR` loses nothing (R3a).
struct PullerState {
    puller: Arc<Puller>,
    receiving: Option<Receiving>,
}

/// The receiving half of Push/Pull, serving one endpoint path.
pub struct weida_puller {
    tried: Tried<PullerState>,
    path: CText,
    exec: Exec,
}

/// Creates a pusher that dials on `tls`'s terms (`Runtime::pusher`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with `weida_pusher_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_pusher(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_pusher,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_pusher {
            pusher: Arc::new(rt.runtime.pusher(tls.get())),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials `url` (`Pusher::connect`), e.g.
/// `weida://sha256:…@127.0.0.1:7443/jobs` or `weida+inproc://bus/jobs`.
/// The runtime redials it when the connection is lost.
///
/// Never blocks; the dial runs as the op written to `out`. A peer whose key
/// is not trusted fails its wait with `-WEIDA_EUNTRUSTED`.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `url` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the op with
/// `weida_pusher_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_connect(
    p: *const weida_pusher,
    url: *const c_char,
    out: *mut *mut weida_pusher_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let pusher = Arc::clone(&p.pusher);
        let op = Op::start(&p.exec, async move { pusher.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_pusher_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_connect_op_fd(
    op: *const weida_pusher_connect_op,
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
pub unsafe extern "C" fn weida_pusher_connect_op_wait(
    op: *const weida_pusher_connect_op,
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
pub unsafe extern "C" fn weida_pusher_connect_op_drop(op: *mut weida_pusher_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Forgets a dialled address (`Pusher::disconnect`): 1 when `url` was
/// dialled and is now forgotten, its connection closed and no longer
/// redialled; 0 when it was not dialled.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `url` is NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_disconnect(
    p: *const weida_pusher,
    url: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?;
        Ok(c_int::from(p.pusher.disconnect(url)))
    })
}

/// The number of connected peers (`Pusher::peer_count`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live pusher.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_peer_count(p: *const weida_pusher) -> usize {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live pusher by this function's contract.
        unsafe { &*p }.pusher.peer_count()
    })
}

/// The bodies the outbox discarded (`Pusher::dropped`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live pusher.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_dropped(p: *const weida_pusher) -> u64 {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live pusher by this function's contract.
        unsafe { &*p }.pusher.dropped()
    })
}

/// A snapshot of the pusher's live connections
/// (`Pusher::connection_stats`): one record per live dialled address, in
/// dial order, read with the `weida_connection_stats_*` getters. Never
/// blocks.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_connection_stats(
    p: *const weida_pusher,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let handle = weida_connection_stats::handle(p.pusher.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// A new reader of the pusher's peer events (`Pusher::events`): every
/// transition of its dialled addresses from this call on, read with
/// `weida_peer_events_recv`. Each call is an independent reader.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_events(
    p: *const weida_pusher,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let handle = weida_peer_events::handle(p.pusher.events());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Opens a one-way transfer to the next peer, round-robin (`Pusher::open`),
/// carrying `meta`, or empty metadata when `meta` is NULL; `meta` is copied
/// at the call. Its `weida_outgoing_transfer_finish` hands back the
/// delivery receipt that `weida_pusher_send` discards.
///
/// Never blocks; the open runs as the op written to `out`. With every peer
/// down it waits for the redial.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `meta` is NULL or live metadata; `out` is
/// NULL or writable. Release the op with `weida_pusher_open_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_open(
    p: *const weida_pusher,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_pusher_open_op,
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
        let pusher = Arc::clone(&p.pusher);
        let op = Op::start(&p.exec, async move { pusher.open(meta).await });
        let handle = into_handle(weida_pusher_open_op {
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
pub unsafe extern "C" fn weida_pusher_open_op_fd(
    op: *const weida_pusher_open_op,
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
pub unsafe extern "C" fn weida_pusher_open_op_wait(
    op: *const weida_pusher_open_op,
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
pub unsafe extern "C" fn weida_pusher_open_op_drop(op: *mut weida_pusher_open_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Sends `body` as one message (`Pusher::send`). The body is copied.
///
/// Never blocks; the send runs as the op written to `out`. With a live peer
/// the body is written at once; with none it waits in the outbox and is
/// written once a peer is back, in order, and a full outbox applies the
/// configured backpressure. The delivery receipt is discarded: use
/// `weida_pusher_open` for it.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `body` is NULL only with `len == 0`, else
/// points to `len` readable bytes; `out` is NULL or writable. Release the
/// op with `weida_pusher_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_send(
    p: *const weida_pusher,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_pusher_send_op,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { send(p, || Ok(TransferMeta::default()), body, len, out) })
}

/// Sends `body` as one message carrying `meta` (`Pusher::send_with`):
/// `weida_pusher_send` with metadata. `meta` is copied at the call; NULL is
/// `TransferMeta::default()`. The body is copied.
///
/// # Safety
///
/// `p` is NULL or a live pusher; `meta` is NULL or live metadata; `body` is
/// NULL only with `len == 0`, else points to `len` readable bytes; `out` is
/// NULL or writable. Release the op with `weida_pusher_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_send_with(
    p: *const weida_pusher,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_pusher_send_op,
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
/// As `weida_pusher_send_with`.
unsafe fn send(
    p: *const weida_pusher,
    meta: impl FnOnce() -> Result<TransferMeta, c_int>,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_pusher_send_op,
) -> Result<c_int, c_int> {
    let out = out_ptr(out)?;
    // SAFETY: forwarded from this function's contract.
    let p = unsafe { borrow(p) }?;
    let meta = meta()?;
    // SAFETY: forwarded from this function's contract.
    let body = unsafe { bytes(body, len) }?.to_vec();
    let pusher = Arc::clone(&p.pusher);
    let op = Op::start(&p.exec, async move { pusher.send_with(meta, &body).await });
    // SAFETY: `out` passed `out_ptr`.
    unsafe { write_out(out, into_handle(weida_pusher_send_op { op })) };
    Ok(0)
}

/// The readiness descriptor of a send op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live send op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_send_op_fd(
    op: *const weida_pusher_send_op,
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
pub unsafe extern "C" fn weida_pusher_send_op_wait(
    op: *const weida_pusher_send_op,
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
pub unsafe extern "C" fn weida_pusher_send_op_drop(op: *mut weida_pusher_send_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a pusher, which drops its connections. NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live pusher, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pusher_drop(p: *mut weida_pusher) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(p) }));
}

/// Registers a puller at the endpoint path `path` (`Listener::puller`).
/// Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_puller_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_puller(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_puller,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let puller = l.listener.puller(path).map_err(fail)?;
        let handle = into_handle(weida_puller {
            path: CText::new(puller.path()),
            tried: Tried::new(PullerState {
                puller: Arc::new(puller),
                receiving: None,
            }),
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this puller serves. Lives as long as the puller.
///
/// # Safety
///
/// `p` is NULL or a live puller; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_puller_path(
    p: *const weida_puller,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if p.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `p` is a live puller by this function's
        // contract; the text lives as long as it.
        let p = unsafe { &*p };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&p.path), len_out) }
    })
}

/// The puller's readiness descriptor (R11): readable when the next
/// `weida_puller_recv` will not block.
///
/// # Safety
///
/// `p` is NULL or a live puller; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_puller_fd(p: *const weida_puller, out: *mut i64) -> c_int {
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

/// Takes the next transfer, waiting up to `timeout_ms` (R4) for one
/// (`Puller::recv`). The transfer arrives as soon as its header does; its
/// body is read like a socket or collected whole. `-WEIDA_ENOT_CONNECTED`
/// once the queue ended.
///
/// # Safety
///
/// `p` is NULL or a live puller; `out` is NULL or writable. Release the
/// transfer with `weida_incoming_transfer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_puller_recv(
    p: *const weida_puller,
    timeout_ms: i64,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let received = p.tried.attempt(timeout_ms, |state, cx| {
            let receiving = state.receiving.get_or_insert_with(|| {
                let puller = Arc::clone(&state.puller);
                Box::pin(async move { puller.recv().await })
            });
            let polled = receiving.as_mut().poll(cx);
            if polled.is_ready() {
                state.receiving = None;
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

/// Releases a puller; its path stays registered, as a dropped Rust `Puller`
/// leaves it. A thread waiting in `weida_puller_recv` returns
/// `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live puller, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_puller_drop(p: *mut weida_puller) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(p) = unsafe { release(p) } {
            drop(p.tried.close());
        }
    });
}
