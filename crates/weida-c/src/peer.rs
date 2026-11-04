//! Raw peers: a peer that dials and opens streams, exchanges, segments and
//! flows, an acceptor, and what it accepts as a tagged handle
//! (docs/c/peer.md).

use std::ffi::{CStr, c_char, c_int};
use std::pin::Pin;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use ::weida_runtime::Exec;
use weida_rs::{
    Acceptor, Error, Flow, FlowMeta, Incoming, OutgoingTransfer, Peer, ReplyStream, Segment,
    SegmentTerms,
};

use crate::config::{Builder, weida_flow_meta, weida_segment_terms};
use crate::consumer::{meta_or_default, weida_consumer, weida_credit_grant};
use crate::error::{WEIDA_EINVAL, closed, fail, fail_abi, guard, guard_or, guard_ptr, guard_void};
use crate::flow::{weida_flow, weida_incoming_flow};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::listener::{weida_incoming_request, weida_listener};
use crate::op::Op;
use crate::pubsub::weida_topic_drops;
use crate::requester::weida_reply_stream;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::segment::weida_segment;
use crate::stats::{weida_connection_stats, weida_peer_events};
use crate::tls::{weida_client_tls, weida_transfer_meta};
use crate::transfer::{weida_incoming_transfer, weida_outgoing_transfer};
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

/// The pending open of `weida_peer_open`.
pub struct weida_peer_open_op {
    op: Arc<Op<OutgoingTransfer>>,
    exec: Exec,
}

impl AsOp for weida_peer_open_op {
    type Output = OutgoingTransfer;
    fn op(&self) -> &Op<OutgoingTransfer> {
        &self.op
    }
}

/// The pending exchange of `weida_peer_open_bi`.
pub struct weida_peer_open_bi_op {
    op: Arc<Op<(OutgoingTransfer, ReplyStream)>>,
    exec: Exec,
}

impl AsOp for weida_peer_open_bi_op {
    type Output = (OutgoingTransfer, ReplyStream);
    fn op(&self) -> &Op<(OutgoingTransfer, ReplyStream)> {
        &self.op
    }
}

/// The pending send of `weida_peer_send`.
pub struct weida_peer_send_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_peer_send_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending open of `weida_peer_segment`.
pub struct weida_peer_segment_op {
    op: Arc<Op<Segment>>,
}

impl AsOp for weida_peer_segment_op {
    type Output = Segment;
    fn op(&self) -> &Op<Segment> {
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
    exec: Exec,
}

/// One thing an acceptor took (`Incoming`), as a tagged handle.
pub struct weida_incoming {
    kind: c_int,
    incoming: Mutex<Option<Incoming>>,
    /// `Unsubscribed`'s id and filter, read without taking anything.
    unsubscribed: Option<(u64, Option<CText>)>,
    exec: Exec,
}

impl weida_incoming {
    fn handle(incoming: Incoming, exec: Exec) -> *mut weida_incoming {
        let kind = match &incoming {
            Incoming::Stream(_) => WEIDA_INCOMING_STREAM,
            Incoming::Exchange(_) => WEIDA_INCOMING_EXCHANGE,
            Incoming::Subscribed(_) => WEIDA_INCOMING_SUBSCRIBED,
            Incoming::Credit(_) => WEIDA_INCOMING_CREDIT,
            Incoming::Unsubscribed { .. } => WEIDA_INCOMING_UNSUBSCRIBED,
            Incoming::Flow(_) => WEIDA_INCOMING_FLOW,
        };
        let unsubscribed = match &incoming {
            Incoming::Unsubscribed { id, filter } => {
                Some((id.get(), filter.as_deref().map(CText::new)))
            }
            _ => None,
        };
        into_handle(weida_incoming {
            kind,
            incoming: Mutex::new(Some(incoming)),
            unsubscribed,
            exec,
        })
    }
}

/// Takes the value out of `i`, which must be of `kind`: `-WEIDA_EINVAL`
/// with `wrong` for another kind, `-WEIDA_ECLOSED` once taken.
fn take(i: &weida_incoming, kind: c_int, wrong: &'static CStr) -> Result<Incoming, c_int> {
    if i.kind != kind {
        return Err(fail_abi(WEIDA_EINVAL, wrong));
    }
    i.incoming
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .ok_or_else(closed)
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

/// Forgets a dialled address (`Peer::disconnect`): 1 when `url` was
/// dialled and is now forgotten, its connection closed and no longer
/// redialled; 0 when it was not dialled.
///
/// # Safety
///
/// `p` is NULL or a live peer; `url` is NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_disconnect(p: *const weida_peer, url: *const c_char) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?;
        Ok(c_int::from(p.peer.disconnect(url)))
    })
}

/// The number of connected peers (`Peer::peer_count`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live peer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_peer_count(p: *const weida_peer) -> usize {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live peer by this function's contract.
        unsafe { &*p }.peer.peer_count()
    })
}

/// The bodies the outbox discarded (`Peer::dropped`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live peer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_dropped(p: *const weida_peer) -> u64 {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live peer by this function's contract.
        unsafe { &*p }.peer.dropped()
    })
}

/// A snapshot of the peer's live connections (`Peer::connection_stats`):
/// one record per live dialled address, in dial order, read with the
/// `weida_connection_stats_*` getters. Never blocks.
///
/// # Safety
///
/// `p` is NULL or a live peer; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_connection_stats(
    p: *const weida_peer,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let handle = weida_connection_stats::handle(p.peer.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// A new reader of the peer's events (`Peer::events`): every transition of
/// its dialled addresses from this call on, read with
/// `weida_peer_events_recv`. Each call is an independent reader. Named
/// `_peer_events` because R1's `weida_peer_events` is the reader's type.
///
/// # Safety
///
/// `p` is NULL or a live peer; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_peer_events(
    p: *const weida_peer,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        let handle = weida_peer_events::handle(p.peer.events());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Opens a one-way stream to the next peer, round-robin (`Peer::open`),
/// carrying `meta`, or empty metadata when `meta` is NULL; `meta` is copied
/// at the call.
///
/// Never blocks; the open runs as the op written to `out`. With every peer
/// down it waits for the redial, bounded by `send_timeout`.
///
/// # Safety
///
/// `p` is NULL or a live peer; `meta` is NULL or live metadata; `out` is
/// NULL or writable. Release the op with `weida_peer_open_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open(
    p: *const weida_peer,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_peer_open_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let meta = unsafe { meta_or_default(meta) }?;
        let peer = Arc::clone(&p.peer);
        let op = Op::start(&p.exec, async move { peer.open(meta).await });
        let handle = into_handle(weida_peer_open_op {
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
pub unsafe extern "C" fn weida_peer_open_op_fd(
    op: *const weida_peer_open_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the open and writes the transfer to
/// `out`, written like a socket and ended with
/// `weida_outgoing_transfer_finish`. Fails with `-WEIDA_ECONNECTION_LOST`
/// when no peer came back within `send_timeout`, and
/// `-WEIDA_ENOT_CONNECTED` for a peer never connected.
///
/// # Safety
///
/// `op` is NULL or a live open op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_op_wait(
    op: *const weida_peer_open_op,
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
pub unsafe extern "C" fn weida_peer_open_op_drop(op: *mut weida_peer_open_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Opens one exchange to the next peer, round-robin (`Peer::open_bi`),
/// carrying `meta`, or empty metadata when `meta` is NULL; `meta` is copied
/// at the call.
///
/// Never blocks; the exchange opens as the op written to `out`. Its wait
/// yields the request body, written like a socket and ended with
/// `weida_outgoing_transfer_finish`, and the reply stream, whose
/// `weida_reply_stream_recv` waits for the reply. With every peer down it
/// waits for the redial, bounded by `send_timeout`.
///
/// # Safety
///
/// `p` is NULL or a live peer; `meta` is NULL or live metadata; `out` is
/// NULL or writable. Release the op with `weida_peer_open_bi_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_bi(
    p: *const weida_peer,
    meta: *const weida_transfer_meta,
    out: *mut *mut weida_peer_open_bi_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let meta = unsafe { meta_or_default(meta) }?;
        let peer = Arc::clone(&p.peer);
        let op = Op::start(&p.exec, async move { peer.open_bi(meta).await });
        let handle = into_handle(weida_peer_open_bi_op {
            op,
            exec: p.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The readiness descriptor of an open-bi op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live open-bi op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_bi_op_fd(
    op: *const weida_peer_open_bi_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the exchange and writes both halves:
/// the request body to `out_body` and the reply stream to `out_reply`.
/// Both are written, or neither. Fails with `-WEIDA_ECONNECTION_LOST` when
/// no peer came back within `send_timeout`, and `-WEIDA_ENOT_CONNECTED`
/// for a peer never connected.
///
/// # Safety
///
/// `op` is NULL or a live open-bi op; `out_body` and `out_reply` are NULL
/// or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_bi_op_wait(
    op: *const weida_peer_open_bi_op,
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
        let reply = weida_reply_stream::handle(reply, handle.exec.clone());
        // SAFETY: both out-parameters passed `out_ptr`.
        unsafe {
            write_out(out_body, body);
            write_out(out_reply, reply);
        }
        Ok(0)
    })
}

/// Releases an open-bi op. An unfinished open is aborted; an exchange it
/// opened that no wait took is abandoned: its body is reset `CANCELED` and
/// its reply half stopped `CANCELED`. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live open-bi op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_open_bi_op_drop(op: *mut weida_peer_open_bi_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Sends `body` as one message carrying `meta` (`Peer::send`); NULL `meta`
/// is `TransferMeta::default()`. `meta` and the body are copied, and the
/// runtime owns the body from the call.
///
/// Never blocks; the send runs as the op written to `out`. With a live peer
/// and an empty outbox the body is written at once; otherwise it is held in
/// the outbox under the configured backpressure and written once a peer is
/// back, in order.
///
/// # Safety
///
/// `p` is NULL or a live peer; `meta` is NULL or live metadata; `body` is
/// NULL only with `len == 0`, else points to `len` readable bytes; `out` is
/// NULL or writable. Release the op with `weida_peer_send_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_send(
    p: *const weida_peer,
    meta: *const weida_transfer_meta,
    body: *const u8,
    len: usize,
    out: *mut *mut weida_peer_send_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let meta = unsafe { meta_or_default(meta) }?;
        // SAFETY: forwarded from this function's contract.
        let body = unsafe { bytes(body, len) }?.to_vec();
        let peer = Arc::clone(&p.peer);
        let op = Op::start(&p.exec, async move { peer.send(meta, &body).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_peer_send_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a send op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live send op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_send_op_fd(
    op: *const weida_peer_send_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the send: 0 once the body is written
/// or held in the outbox. Fails with `-WEIDA_ELIMIT_EXCEEDED` under the
/// `Reject` backpressure or for a body larger than the outbox,
/// `-WEIDA_ECONNECTION_LOST` once every address is given up, and
/// `-WEIDA_ENOT_CONNECTED` for a peer never connected.
///
/// # Safety
///
/// `op` is NULL or a live send op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_send_op_wait(
    op: *const weida_peer_send_op,
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
pub unsafe extern "C" fn weida_peer_send_op_drop(op: *mut weida_peer_send_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Opens segment n+1 on `topic` to the next peer (`Peer::segment`): it
/// supersedes this peer's copy of segment n there as a radio does, and is
/// written with the `weida_segment_*` calls. NULL `terms` are
/// `SegmentTerms::default()`; `topic` and `terms` are copied at the call.
///
/// Never blocks; the open runs as the op written to `out`. With every peer
/// down it waits for the redial, bounded by `send_timeout`.
///
/// # Safety
///
/// `p` is NULL or a live peer; `topic` is NULL or a NUL-terminated string;
/// `terms` is NULL or live terms; `out` is NULL or writable. Release the op
/// with `weida_peer_segment_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_segment(
    p: *const weida_peer,
    topic: *const c_char,
    terms: *const weida_segment_terms,
    out: *mut *mut weida_peer_segment_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?.to_owned();
        let terms = if terms.is_null() {
            SegmentTerms::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(terms) }?.get()
        };
        let peer = Arc::clone(&p.peer);
        let op = Op::start(&p.exec, async move { peer.segment(&topic, terms).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_peer_segment_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a segment op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live segment op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_segment_op_fd(
    op: *const weida_peer_segment_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the open and writes the segment to
/// `out`. Fails with `-WEIDA_ELIMIT_EXCEEDED` for a topic above 256 bytes
/// or when `max_sequence_scopes` topics all have a copy in flight,
/// `-WEIDA_ECONNECTION_LOST` when no peer came back within `send_timeout`,
/// and `-WEIDA_ENOT_CONNECTED` for a peer never connected.
///
/// # Safety
///
/// `op` is NULL or a live segment op; `out` is NULL or writable. Release
/// the segment with `weida_segment_finish` or `weida_segment_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_segment_op_wait(
    op: *const weida_peer_segment_op,
    timeout_ms: i64,
    out: *mut *mut weida_segment,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let op = unsafe { borrow(op) }?;
        let segment = op.op.wait(timeout_ms)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_segment::handle(segment)) };
        Ok(0)
    })
}

/// Releases a segment op. An unfinished open is aborted; a segment it
/// opened that no wait took is dropped, resetting its unfinished layers.
/// NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live segment op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_segment_op_drop(op: *mut weida_peer_segment_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// What this peer's segments lost on `topic`, by cause
/// (`Peer::segment_drops`): a snapshot of one record, or of none when
/// nothing was. Never blocks.
///
/// # Safety
///
/// `p` is NULL or a live peer; `topic` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the snapshot with
/// `weida_topic_drops_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_peer_segment_drops(
    p: *const weida_peer,
    topic: *const c_char,
    out: *mut *mut weida_topic_drops,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let p = unsafe { borrow(p) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        let records = p.peer.segment_drops(topic).into_iter().collect();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_topic_drops::handle(records)) };
        Ok(0)
    })
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
            exec: l.exec.clone(),
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
        unsafe { write_out(out, weida_incoming::handle(incoming, a.exec.clone())) };
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

/// Hands a one-way stream over, once. A second call is `-WEIDA_ECLOSED`,
/// another kind `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `out` is NULL or writable.
/// Release the transfer with `weida_incoming_transfer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_take_stream(
    i: *const weida_incoming,
    out: *mut *mut weida_incoming_transfer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let i = unsafe { borrow(i) }?;
        let incoming = take(&i, WEIDA_INCOMING_STREAM, c"this incoming is not a stream")?;
        let Incoming::Stream(transfer) = incoming else {
            unreachable!("the kind names the variant");
        };
        let handle = weida_incoming_transfer::handle(transfer, i.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Hands an exchange over, once, as a request that owes a reply. A second
/// call is `-WEIDA_ECLOSED`, another kind `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `out` is NULL or writable.
/// Release the request with `weida_incoming_request_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_take_exchange(
    i: *const weida_incoming,
    out: *mut *mut weida_incoming_request,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let i = unsafe { borrow(i) }?;
        let incoming = take(
            &i,
            WEIDA_INCOMING_EXCHANGE,
            c"this incoming is not an exchange",
        )?;
        let Incoming::Exchange(request) = incoming else {
            unreachable!("the kind names the variant");
        };
        let handle = weida_incoming_request::handle(request, i.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Hands the consumer of a subscription over, once. A second call is
/// `-WEIDA_ECLOSED`, another kind `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `out` is NULL or writable.
/// Release the consumer with `weida_consumer_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_take_subscribed(
    i: *const weida_incoming,
    out: *mut *mut weida_consumer,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let i = unsafe { borrow(i) }?;
        let incoming = take(
            &i,
            WEIDA_INCOMING_SUBSCRIBED,
            c"this incoming is not a subscription",
        )?;
        let Incoming::Subscribed(consumer) = incoming else {
            unreachable!("the kind names the variant");
        };
        let handle = weida_consumer::handle(consumer, i.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Hands a credit grant over, once. A second call is `-WEIDA_ECLOSED`,
/// another kind `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `out` is NULL or writable.
/// Release the grant with `weida_credit_grant_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_take_credit(
    i: *const weida_incoming,
    out: *mut *mut weida_credit_grant,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let i = unsafe { borrow(i) }?;
        let incoming = take(
            &i,
            WEIDA_INCOMING_CREDIT,
            c"this incoming is not a credit grant",
        )?;
        let Incoming::Credit(grant) = incoming else {
            unreachable!("the kind names the variant");
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_credit_grant::handle(grant)) };
        Ok(0)
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
        let incoming = take(&i, WEIDA_INCOMING_FLOW, c"this incoming is not a flow")?;
        let Incoming::Flow(flow) = incoming else {
            unreachable!("the kind names the variant");
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_incoming_flow::handle(flow)) };
        Ok(0)
    })
}

/// The consumer number of an unsubscription: 1 and `*out`; 0 for another
/// kind. Taking nothing, it reads the same after any call.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_unsubscribed_id(
    i: *const weida_incoming,
    out: *mut u64,
) -> c_int {
    guard_or(0, || {
        if i.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `i` is live by this function's contract.
        match &unsafe { &*i }.unsubscribed {
            Some((id, _)) => {
                // SAFETY: a non-NULL `out` is writable by this function's
                // contract.
                unsafe { out.write(*id) };
                1
            }
            None => 0,
        }
    })
}

/// The filter of an unsubscription; NULL when the whole connection went
/// away (every subscription it held, on every path) and for another kind.
/// Lives as long as `i`.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_unsubscribed_filter(
    i: *const weida_incoming,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if i.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `i` is live by this function's contract; the
        // text lives as long as it.
        let i = unsafe { &*i };
        let filter = i.unsubscribed.as_ref().and_then(|(_, f)| f.as_ref());
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(filter, len_out) }
    })
}

/// Releases an incoming handle and drops what was not taken, as Rust does:
/// a stream is refused `REJECTED`, an exchange answers `NO_REPLY`, a flow
/// is stopped `CANCELED`, and a subscription, credit grant or
/// unsubscription not taken is forgotten. NULL does nothing.
///
/// # Safety
///
/// `i` is NULL or a live incoming handle, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_drop(i: *mut weida_incoming) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(i) }));
}
