//! The two halves of SWARM: a swarm whose fetches take verified pieces from
//! every connected seeder, and a seeder whose sessions the application
//! answers from its own storage (docs/c/swarm.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;

use ::weida_runtime::Exec;
use weida_rs::{
    Ask, Blob, Error, Fetch, FetchOptions, MAX_PENDING_SESSIONS, MIN_RATE, Piece, SEED_STALL,
    SeedSession, Seeder, Swarm,
};

use crate::blob::{weida_bits, weida_blob, weida_piece_list};
use crate::config::weida_fetch_options;
use crate::error::{
    WEIDA_EINVAL, closed, fail, fail_abi, guard, guard_or, guard_ptr, guard_void, null_arg,
};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, share, text_out, write_out,
};
use crate::listener::weida_listener;
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::stats::{weida_connection_stats, weida_peer_events};
use crate::tls::weida_client_tls;
use crate::transfer::weida_incoming_meta;
use crate::try_handle::Tried;

/// `Ask::List`: the piece list, answered with
/// `weida_seed_session_answer_list`.
pub const WEIDA_ASK_LIST: c_int = 0;
/// `Ask::Have`: the pieces the seeder can serve now, answered with
/// `weida_seed_session_answer_have`.
pub const WEIDA_ASK_HAVE: c_int = 1;
/// `Ask::Piece`: the bytes of one piece, answered with
/// `weida_seed_session_answer_piece`.
pub const WEIDA_ASK_PIECE: c_int = 2;

/// Sessions a seeder holds before `weida_seeder_accept` takes them
/// (`MAX_PENDING_SESSIONS`).
pub const WEIDA_MAX_PENDING_SESSIONS: c_int = 64;
const _: () = assert!(WEIDA_MAX_PENDING_SESSIONS as usize == MAX_PENDING_SESSIONS);
/// The fixed part of an answer's deadline (`SEED_STALL`), in milliseconds.
pub const WEIDA_SEED_STALL_MS: c_int = 30_000;
const _: () = assert!(WEIDA_SEED_STALL_MS as u128 == SEED_STALL.as_millis());
/// The rate, in bytes per second, below which an answer is abandoned
/// (`MIN_RATE`).
pub const WEIDA_MIN_RATE: c_int = 65_536;
const _: () = assert!(WEIDA_MIN_RATE as u64 == MIN_RATE);

/// The dialling half of SWARM (`Swarm`).
pub struct weida_swarm {
    swarm: Arc<Swarm>,
    exec: Exec,
}

/// The pending dial of `weida_swarm_connect`.
pub struct weida_swarm_connect_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_swarm_connect_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// A read of the next piece in flight; it owns the fetch until it ends.
type Gathering = Pin<Box<dyn Future<Output = (Fetch, Option<Result<Piece, Error>>)> + Send>>;

/// A fetch between tries: the fetch itself, or the read in flight that owns
/// it (R3a).
struct FetchState {
    fetch: Option<Fetch>,
    gathering: Option<Gathering>,
}

/// One blob being fetched (`Fetch`): its pieces, read as a try.
pub struct weida_fetch {
    tried: Tried<FetchState>,
    blob: Blob,
}

/// One verified piece (`Piece`), read-only.
pub struct weida_piece {
    piece: Piece,
}

/// An accept in flight; it is kept across tries (R3a).
type SessionAccepting = Pin<Box<dyn Future<Output = Result<SeedSession, Error>> + Send>>;

/// A seeder's state between tries.
struct SeederState {
    seeder: Arc<Seeder>,
    accepting: Option<SessionAccepting>,
}

/// The bound half of SWARM, serving one endpoint path (`Seeder`).
pub struct weida_seeder {
    tried: Tried<SeederState>,
    path: CText,
    exec: Exec,
}

/// A read of the next order in flight; it holds the session's lock.
type Ordering = Pin<Box<dyn Future<Output = Result<Option<Ask>, Error>> + Send>>;

/// A session's state between tries: the read in flight is kept, since a
/// half-read order cannot be abandoned (R3a).
struct SessionState {
    ordering: Option<Ordering>,
}

/// One fetch's session with this seeder (`SeedSession`). A `next` in flight
/// and every answer op hold the lock.
pub struct weida_seed_session {
    session: Arc<tokio::sync::Mutex<Option<SeedSession>>>,
    tried: Tried<SessionState>,
    blob: Blob,
    meta: Arc<weida_incoming_meta>,
    connection: u64,
    exec: Exec,
}

impl weida_seed_session {
    fn handle(session: SeedSession, exec: Exec) -> *mut weida_seed_session {
        into_handle(weida_seed_session {
            blob: session.blob(),
            meta: weida_incoming_meta::new(session.meta().clone()),
            connection: session.connection().get(),
            session: Arc::new(tokio::sync::Mutex::new(Some(session))),
            tried: Tried::new(SessionState { ordering: None }),
            exec,
        })
    }

    /// Takes the session's lock for an answer: `-WEIDA_EINVAL` while a
    /// `next` or another answer holds it. Taken in the call, not in the
    /// task, so a call after this one cannot overtake the answer.
    fn hold(&self) -> Result<tokio::sync::OwnedMutexGuard<Option<SeedSession>>, c_int> {
        Arc::clone(&self.session)
            .try_lock_owned()
            .map_err(|_| busy())
    }
}

/// The pending answer of `weida_seed_session_answer_list`.
pub struct weida_seed_session_answer_list_op {
    op: Arc<Op<()>>,
}

/// The pending answer of `weida_seed_session_answer_have`.
pub struct weida_seed_session_answer_have_op {
    op: Arc<Op<()>>,
}

/// The pending answer of `weida_seed_session_answer_piece`.
pub struct weida_seed_session_answer_piece_op {
    op: Arc<Op<()>>,
}

/// The pending DECLINE of `weida_seed_session_decline`.
pub struct weida_seed_session_decline_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_seed_session_answer_list_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

impl AsOp for weida_seed_session_answer_have_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

impl AsOp for weida_seed_session_answer_piece_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

impl AsOp for weida_seed_session_decline_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

fn busy() -> c_int {
    fail_abi(WEIDA_EINVAL, c"a call is in flight on this seed session")
}

fn reading() -> c_int {
    fail_abi(WEIDA_EINVAL, c"a read is in flight on this fetch")
}

/// An order out: its `WEIDA_ASK_*` kind and the piece index, 0 but for a
/// PIECE.
fn ask_out(ask: Ask) -> (c_int, u32) {
    match ask {
        Ask::List => (WEIDA_ASK_LIST, 0),
        Ask::Have => (WEIDA_ASK_HAVE, 0),
        Ask::Piece(index) => (WEIDA_ASK_PIECE, index),
    }
}

// --- weida_swarm ------------------------------------------------------------

/// Creates a swarm that dials seeders on `tls`'s terms (`Runtime::swarm`).
///
/// # Safety
///
/// `rt` is NULL or a live runtime; `tls` is NULL or live client terms;
/// `out` is NULL or writable. Release the result with `weida_swarm_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_swarm(
    rt: *const weida_runtime,
    tls: *const weida_client_tls,
    out: *mut *mut weida_swarm,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let rt = unsafe { borrow(rt) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let handle = into_handle(weida_swarm {
            swarm: Arc::new(rt.runtime.swarm(tls.get())),
            exec: rt.runtime.exec().clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Dials the seeder at `url` (`Swarm::connect`). A swarm takes pieces from
/// every seeder it connected, and one connected during a fetch joins it at
/// its next read. The runtime redials it when the connection is lost.
///
/// Never blocks; the dial runs as the op written to `out`.
///
/// # Safety
///
/// `s` is NULL or a live swarm; `url` is NULL or a NUL-terminated string;
/// `out` is NULL or writable. Release the op with
/// `weida_swarm_connect_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_connect(
    s: *const weida_swarm,
    url: *const c_char,
    out: *mut *mut weida_swarm_connect_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?.to_owned();
        let swarm = Arc::clone(&s.swarm);
        let op = Op::start(&s.exec, async move { swarm.connect(&url).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_swarm_connect_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a connect op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live connect op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_connect_op_fd(
    op: *const weida_swarm_connect_op,
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
pub unsafe extern "C" fn weida_swarm_connect_op_wait(
    op: *const weida_swarm_connect_op,
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
pub unsafe extern "C" fn weida_swarm_connect_op_drop(op: *mut weida_swarm_connect_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Forgets the dialled address `url` (`Swarm::disconnect`): 1 when it was
/// dialled, which closes its connection and stops it being redialled; 0
/// when it was not dialled.
///
/// # Safety
///
/// `s` is NULL or a live swarm; `url` is NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_disconnect(
    s: *const weida_swarm,
    url: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let url = unsafe { cstr(url) }?;
        Ok(c_int::from(s.swarm.disconnect(url)))
    })
}

/// The number of connected seeders (`Swarm::peer_count`); 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live swarm.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_peer_count(s: *const weida_swarm) -> usize {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live swarm by this function's contract.
        unsafe { &*s }.swarm.peer_count()
    })
}

/// A snapshot of the swarm's live connections (`Swarm::connection_stats`):
/// one record per live dialled address, in dial order, read with the
/// `weida_connection_stats_*` getters. Never blocks.
///
/// # Safety
///
/// `s` is NULL or a live swarm; `out` is NULL or writable. Release the
/// snapshot with `weida_connection_stats_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_connection_stats(
    s: *const weida_swarm,
    out: *mut *mut weida_connection_stats,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let handle = weida_connection_stats::handle(s.swarm.connection_stats());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// A new reader of the swarm's events (`Swarm::events`): every transition
/// of its dialled addresses from this call on, read with
/// `weida_peer_events_recv`. Each call is an independent reader.
///
/// # Safety
///
/// `s` is NULL or a live swarm; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_events(
    s: *const weida_swarm,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let handle = weida_peer_events::handle(s.swarm.events());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Starts fetching the pieces of `blob` that `have` lacks (`Swarm::fetch`),
/// ordered as `options` says, or by the defaults when it is NULL. `have` is
/// what the caller already holds, so a fetch resumes where an earlier one
/// stopped; it and the options are copied at the call.
///
/// Never blocks: nothing is ordered until the first `weida_fetch_next`. A
/// `have` without one bit per piece of `blob`, or a list in `options` of
/// another blob, is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `s` is NULL or a live swarm; `blob` is NULL or a live blob; `have` is
/// NULL or a live map; `options` is NULL or live options; `out` is NULL or
/// writable. Release the fetch with `weida_fetch_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_fetch(
    s: *const weida_swarm,
    blob: *const weida_blob,
    have: *const weida_bits,
    options: *const weida_fetch_options,
    out: *mut *mut weida_fetch,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let blob = unsafe { borrow(blob) }?.get();
        // SAFETY: forwarded from this function's contract.
        let have = unsafe { borrow(have) }?.get();
        let options = if options.is_null() {
            FetchOptions::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(options) }?.options()
        };
        // Rust panics on either; the ABI refuses them.
        if have.len() != blob.count() {
            return Err(fail_abi(
                WEIDA_EINVAL,
                c"have needs one bit per piece of the blob",
            ));
        }
        if options.list.as_ref().is_some_and(|l| l.blob() != blob) {
            return Err(fail_abi(WEIDA_EINVAL, c"the piece list is of another blob"));
        }
        let fetch = s.swarm.fetch(blob, have, options);
        let handle = into_handle(weida_fetch {
            blob,
            tried: Tried::new(FetchState {
                fetch: Some(fetch),
                gathering: None,
            }),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a swarm, which drops its connections. A fetch made from it
/// then reads `-WEIDA_ENOT_CONNECTED`. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live swarm, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_swarm_drop(s: *mut weida_swarm) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(s) }));
}

// --- weida_fetch ------------------------------------------------------------

/// The fetch's readiness descriptor (R11): readable when the next
/// `weida_fetch_next` will not block.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_fd(f: *const weida_fetch, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let fd = f.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next verified piece, in any order, waiting up to `timeout_ms`
/// (R4) for one (`Fetch::next`).
///
/// Writes NULL to `out` and returns 0 once the caller holds every piece:
/// the end of the fetch, and every later call returns it too.
/// `-WEIDA_ENOT_CONNECTED` when no order is in flight and no seeder is left
/// that has not failed this fetch, or when the swarm is gone; connecting a
/// seeder and reading again resumes. Taking a piece frees its order's
/// credit, which is the fetch's backpressure. A read in flight keeps its
/// place across timeouts and `EINTR`.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable. Release a piece
/// with `weida_piece_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_next(
    f: *const weida_fetch,
    timeout_ms: i64,
    out: *mut *mut weida_piece,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let piece = f.tried.attempt(timeout_ms, |state, cx| {
            let gathering = state.gathering.get_or_insert_with(|| {
                let mut fetch = state
                    .fetch
                    .take()
                    .expect("the fetch is held whenever no read is in flight");
                Box::pin(async move {
                    let piece = fetch.next().await;
                    (fetch, piece)
                })
            });
            match gathering.as_mut().poll(cx) {
                Poll::Ready((fetch, piece)) => {
                    state.gathering = None;
                    state.fetch = Some(fetch);
                    Poll::Ready(piece)
                }
                Poll::Pending => Poll::Pending,
            }
        })?;
        let handle = match piece {
            None => ptr::null_mut(),
            Some(Ok(piece)) => into_handle(weida_piece { piece }),
            Some(Err(e)) => return Err(fail(e)),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The blob being fetched (`Fetch::blob`), as a new handle; answers while a
/// read is in flight too.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable. Release the
/// result with `weida_blob_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_blob(
    f: *const weida_fetch,
    out: *mut *mut weida_blob,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_blob::handle(f.blob)) };
        Ok(0)
    })
}

/// The piece list, once it is known and verified (`Fetch::list`): 1 and a
/// copy in `*out`, to resume without LIST (`weida_fetch_options_set_list`);
/// 0 with `*out == NULL` while it is not known. `-WEIDA_EINVAL` while a
/// `weida_fetch_next` read is in flight.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable. Release the
/// result with `weida_piece_list_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_list(
    f: *const weida_fetch,
    out: *mut *mut weida_piece_list,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let list = f
            .tried
            .with(|s| s.fetch.as_ref().map(|f| f.list().cloned()))?
            .ok_or_else(reading)?;
        let (handle, rc) = match list {
            Some(list) => (weida_piece_list::handle(Arc::new(list)), 1),
            None => (ptr::null_mut(), 0),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(rc)
    })
}

/// The pieces the caller holds (`Fetch::delivered`): those it started with
/// and those this fetch yielded, copied into a new map. `-WEIDA_EINVAL`
/// while a `weida_fetch_next` read is in flight.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable. Release the
/// result with `weida_bits_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_delivered(
    f: *const weida_fetch,
    out: *mut *mut weida_bits,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { fetch_bits(f, out, |f| f.delivered().clone()) }
}

/// The pieces ordered of some seeder and not yet taken
/// (`Fetch::in_flight`): on their way, or verified and waiting for
/// `weida_fetch_next`, copied into a new map. `-WEIDA_EINVAL` while a
/// `weida_fetch_next` read is in flight.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable. Release the
/// result with `weida_bits_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_in_flight(
    f: *const weida_fetch,
    out: *mut *mut weida_bits,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { fetch_bits(f, out, Fetch::in_flight) }
}

/// A map of the fetch, written to `out`.
///
/// # Safety
///
/// `f` is NULL or a live fetch; `out` is NULL or writable.
unsafe fn fetch_bits(
    f: *const weida_fetch,
    out: *mut *mut weida_bits,
    map: impl FnOnce(&Fetch) -> weida_rs::Bits,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let f = unsafe { borrow(f) }?;
        let bits = f
            .tried
            .with(|s| s.fetch.as_ref().map(map))?
            .ok_or_else(reading)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_bits::handle(bits)) };
        Ok(0)
    })
}

/// Releases a fetch, which aborts every order it has in flight; the pieces
/// it yielded are the caller's. A thread waiting in `weida_fetch_next`
/// returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `f` is NULL or a live fetch, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_fetch_drop(f: *mut weida_fetch) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(f) = unsafe { release(f) } {
            drop(f.tried.close());
        }
    });
}

// --- weida_piece ------------------------------------------------------------

/// Which piece this is (`Piece::index`); 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live piece.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_index(p: *const weida_piece) -> u32 {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live piece by this function's contract.
        unsafe { &*p }.piece.index
    })
}

/// The piece's bytes (`Piece::bytes`), checked against the blob's root: the
/// bytes of `weida_blob_span` of its index, their length in `*len_out` when
/// that is not NULL. NULL for NULL. Valid as long as `p`.
///
/// # Safety
///
/// `p` is NULL or a live piece; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_bytes(
    p: *const weida_piece,
    len_out: *mut usize,
) -> *const u8 {
    guard_ptr(|| {
        if p.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `p` is live by this function's contract; the
        // bytes live in it.
        let bytes = &unsafe { &*p }.piece.bytes;
        if !len_out.is_null() {
            // SAFETY: a non-NULL `len_out` is writable by this function's
            // contract.
            unsafe { len_out.write(bytes.len()) };
        }
        bytes.as_ptr()
    })
}

/// Releases a piece and its bytes. NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live piece, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_piece_drop(p: *mut weida_piece) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(p) }));
}

// --- weida_seeder -----------------------------------------------------------

/// Registers a seeder at the endpoint path `path` (`Listener::seeder`).
/// Fails with `-WEIDA_EINVALID_ENDPOINT_PATH` or
/// `-WEIDA_EALREADY_REGISTERED`.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_seeder_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_seeder(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_seeder,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let seeder = l.listener.seeder(path).map_err(fail)?;
        let handle = into_handle(weida_seeder {
            path: CText::new(seeder.path()),
            tried: Tried::new(SeederState {
                seeder: Arc::new(seeder),
                accepting: None,
            }),
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The endpoint path this seeder serves. Lives as long as the seeder.
///
/// # Safety
///
/// `s` is NULL or a live seeder; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seeder_path(
    s: *const weida_seeder,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if s.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `s` is a live seeder by this function's
        // contract; the text lives as long as it.
        let s = unsafe { &*s };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&s.path), len_out) }
    })
}

/// The seeder's readiness descriptor (R11): readable when the next
/// `weida_seeder_accept` will not block.
///
/// # Safety
///
/// `s` is NULL or a live seeder; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seeder_fd(s: *const weida_seeder, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let fd = s.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next session, waiting up to `timeout_ms` (R4) for one
/// (`Seeder::accept`). Every session arrives with a well-formed open; admit
/// it from its blob and metadata, or decline it. `-WEIDA_ENOT_CONNECTED`
/// once the queue ended.
///
/// # Safety
///
/// `s` is NULL or a live seeder; `out` is NULL or writable. Release the
/// session with `weida_seed_session_drop` or decline it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seeder_accept(
    s: *const weida_seeder,
    timeout_ms: i64,
    out: *mut *mut weida_seed_session,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let accepted = s.tried.attempt(timeout_ms, |state, cx| {
            let accepting = state.accepting.get_or_insert_with(|| {
                let seeder = Arc::clone(&state.seeder);
                Box::pin(async move { seeder.accept().await })
            });
            let polled = accepting.as_mut().poll(cx);
            if polled.is_ready() {
                state.accepting = None;
            }
            polled
        })?;
        let session = accepted.map_err(fail)?;
        let handle = weida_seed_session::handle(session, s.exec.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a seeder; sessions not yet accepted are dropped, as a dropped
/// Rust `Seeder` drops them. A thread waiting in `weida_seeder_accept`
/// returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live seeder, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seeder_drop(s: *mut weida_seeder) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(s) = unsafe { release(s) } {
            drop(s.tried.close());
        }
    });
}

// --- weida_seed_session -----------------------------------------------------

/// The blob this session is about (`SeedSession::blob`), as a new handle.
///
/// # Safety
///
/// `s` is NULL or a live session; `out` is NULL or writable. Release the
/// result with `weida_blob_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_blob(
    s: *const weida_seed_session,
    out: *mut *mut weida_blob,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_blob::handle(s.blob)) };
        Ok(0)
    })
}

/// Writes a new reference to the session's metadata (`SeedSession::meta`);
/// its peer names the fetching side where its connection proved a key.
/// Release it with `weida_incoming_meta_drop`; it outlives the session.
///
/// # Safety
///
/// `s` is NULL or a live session; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_meta(
    s: *const weida_seed_session,
    out: *mut *mut weida_incoming_meta,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, share(Arc::clone(&s.meta))) };
        Ok(0)
    })
}

/// The number of the connection the session arrived on
/// (`SeedSession::connection`, `ConnectionId::get`); 0 for NULL.
///
/// # Safety
///
/// `s` is NULL or a live session.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_connection(s: *const weida_seed_session) -> u64 {
    guard_or(0, || {
        if s.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `s` is a live session by this function's
        // contract.
        unsafe { &*s }.connection
    })
}

/// The session's readiness descriptor (R11): readable when the next
/// `weida_seed_session_next` will not block.
///
/// # Safety
///
/// `s` is NULL or a live session; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_fd(
    s: *const weida_seed_session,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let fd = s.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next order, waiting up to `timeout_ms` (R4) for one
/// (`SeedSession::next`): 1 with its `WEIDA_ASK_*` kind in `*out_ask` and,
/// for a PIECE, the piece index in `*out_index` (0 otherwise); 0 once the
/// fetch finished its orders, the outs untouched. Answer each order with the
/// `weida_seed_session_answer_*` that matches it before the next call.
///
/// Waits as long as the fetch sends none: its idleness is its backpressure.
/// `-WEIDA_EPROTOCOL` while the order before is unanswered, for an order
/// this blob cannot have (the session is then refused), and after the
/// session failed. A read in flight keeps its place across timeouts and
/// `EINTR`; it must not be abandoned midway, so drop the session instead.
/// `-WEIDA_EINVAL` while an answer op runs.
///
/// # Safety
///
/// `s` is NULL or a live session; `out_ask` and `out_index` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_next(
    s: *const weida_seed_session,
    timeout_ms: i64,
    out_ask: *mut c_int,
    out_index: *mut u32,
) -> c_int {
    guard(|| {
        let out_ask = out_ptr(out_ask)?;
        let out_index = out_ptr(out_index)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        let ask = s.tried.attempt(timeout_ms, |state, cx| {
            if state.ordering.is_none() {
                let mut held = match s.hold() {
                    Ok(held) => held,
                    Err(code) => return Poll::Ready(Err(code)),
                };
                state.ordering = Some(Box::pin(async move {
                    match held.as_mut() {
                        Some(session) => session.next().await,
                        None => Err(Error::Canceled),
                    }
                }));
            }
            let polled = state
                .ordering
                .as_mut()
                .expect("set above")
                .as_mut()
                .poll(cx);
            if polled.is_ready() {
                state.ordering = None;
            }
            polled.map(|r| r.map_err(fail))
        })??;
        let Some(ask) = ask else {
            return Ok(0);
        };
        let (kind, index) = ask_out(ask);
        // SAFETY: both outs passed `out_ptr`.
        unsafe {
            write_out(out_ask, kind);
            write_out(out_index, index);
        }
        Ok(1)
    })
}

/// Answers `WEIDA_ASK_LIST` with the piece list of the session's blob
/// (`SeedSession::answer_list`). The session shares `list` until the answer
/// is written.
///
/// Never blocks; the answer runs as the op written to `out`. `-WEIDA_EINVAL`
/// while a `next` or another answer holds the session.
///
/// # Safety
///
/// `s` is NULL or a live session; `list` is NULL or a live piece list; `out`
/// is NULL or writable. Release the op with
/// `weida_seed_session_answer_list_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_list(
    s: *const weida_seed_session,
    list: *const weida_piece_list,
    out: *mut *mut weida_seed_session_answer_list_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let list = Arc::clone(&unsafe { borrow(list) }?.list);
        let mut held = s.hold()?;
        let op = Op::start(&s.exec, async move {
            match held.as_mut() {
                Some(session) => session.answer_list(&list).await,
                None => Err(Error::Canceled),
            }
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_seed_session_answer_list_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a list answer op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live list answer op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_list_op_fd(
    op: *const weida_seed_session_answer_list_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the answer: 0 once it was written.
/// `-WEIDA_EPROTOCOL` when the order was not LIST or the list is of another
/// blob; the session is then refused.
///
/// # Safety
///
/// `op` is NULL or a live list answer op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_list_op_wait(
    op: *const weida_seed_session_answer_list_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a list answer op, aborting the answer when unfinished. NULL
/// does nothing.
///
/// # Safety
///
/// `op` is NULL or a live list answer op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_list_op_drop(
    op: *mut weida_seed_session_answer_list_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Answers `WEIDA_ASK_HAVE` with the pieces this seeder can serve now
/// (`SeedSession::answer_have`). `have` is copied at the call.
///
/// Never blocks; the answer runs as the op written to `out`. `-WEIDA_EINVAL`
/// while a `next` or another answer holds the session.
///
/// # Safety
///
/// `s` is NULL or a live session; `have` is NULL or a live map; `out` is
/// NULL or writable. Release the op with
/// `weida_seed_session_answer_have_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_have(
    s: *const weida_seed_session,
    have: *const weida_bits,
    out: *mut *mut weida_seed_session_answer_have_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let have = unsafe { borrow(have) }?.get();
        let mut held = s.hold()?;
        let op = Op::start(&s.exec, async move {
            match held.as_mut() {
                Some(session) => session.answer_have(&have).await,
                None => Err(Error::Canceled),
            }
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_seed_session_answer_have_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a HAVE answer op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live HAVE answer op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_have_op_fd(
    op: *const weida_seed_session_answer_have_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the answer: 0 once it was written.
/// `-WEIDA_EPROTOCOL` when the order was not HAVE or the map does not have
/// one bit per piece; the session is then refused.
///
/// # Safety
///
/// `op` is NULL or a live HAVE answer op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_have_op_wait(
    op: *const weida_seed_session_answer_have_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a HAVE answer op, aborting the answer when unfinished. NULL
/// does nothing.
///
/// # Safety
///
/// `op` is NULL or a live HAVE answer op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_have_op_drop(
    op: *mut weida_seed_session_answer_have_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Answers `WEIDA_ASK_PIECE` with the piece's `len` bytes
/// (`SeedSession::answer_piece`), copied at the call. They are not checked
/// against the root here: the fetch checks every piece and drops a seeder
/// that sends a wrong one.
///
/// Never blocks; the answer runs as the op written to `out`. `-WEIDA_EINVAL`
/// while a `next` or another answer holds the session.
///
/// # Safety
///
/// `s` is NULL or a live session; `data` is NULL only with `len == 0`, else
/// points to `len` readable bytes; `out` is NULL or writable. Release the op
/// with `weida_seed_session_answer_piece_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_piece(
    s: *const weida_seed_session,
    data: *const u8,
    len: usize,
    out: *mut *mut weida_seed_session_answer_piece_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let s = unsafe { borrow(s) }?;
        // SAFETY: forwarded from this function's contract.
        let data = unsafe { bytes(data, len) }?.to_vec();
        let mut held = s.hold()?;
        let op = Op::start(&s.exec, async move {
            match held.as_mut() {
                Some(session) => session.answer_piece(&data).await,
                None => Err(Error::Canceled),
            }
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_seed_session_answer_piece_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a piece answer op: readable once it
/// finished.
///
/// # Safety
///
/// `op` is NULL or a live piece answer op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_piece_op_fd(
    op: *const weida_seed_session_answer_piece_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the answer: 0 once it was written.
/// `-WEIDA_EPROTOCOL` when the order was not PIECE or the bytes do not have
/// the length of its span; the session is then refused.
///
/// # Safety
///
/// `op` is NULL or a live piece answer op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_piece_op_wait(
    op: *const weida_seed_session_answer_piece_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a piece answer op, aborting the answer when unfinished. NULL
/// does nothing.
///
/// # Safety
///
/// `op` is NULL or a live piece answer op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_answer_piece_op_drop(
    op: *mut weida_seed_session_answer_piece_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Ends the session (`SeedSession::decline`): a blob this seeder does not
/// serve, or a piece it does not hold. The fetch drops this seeder for the
/// blob. **Consumes** `s` on every return path; a `next` in flight is
/// dropped.
///
/// Never blocks; the DECLINE runs as the op written to `out`. While an
/// answer op runs it fails with `-WEIDA_EINVAL`, and the session is dropped
/// when that answer ends; a NULL `out` fails the same way and drops it at
/// once.
///
/// # Safety
///
/// `s` is NULL or a live session, not used again; `out` is NULL or
/// writable. Release the op with `weida_seed_session_decline_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_decline(
    s: *mut weida_seed_session,
    out: *mut *mut weida_seed_session_decline_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let s = unsafe { release(s) }.ok_or_else(null_arg)?;
        // Drops a `next` in flight and the lock it holds.
        drop(s.tried.close());
        let out = out_ptr(out)?;
        let session = s
            .session
            .try_lock()
            .map_err(|_| busy())?
            .take()
            .ok_or_else(closed)?;
        let op = Op::start(&s.exec, async move {
            session.decline().await;
            Ok(())
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_seed_session_decline_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a decline op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live decline op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_decline_op_fd(
    op: *const weida_seed_session_decline_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the DECLINE: 0 once it was sent or the
/// session was gone.
///
/// # Safety
///
/// `op` is NULL or a live decline op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_decline_op_wait(
    op: *const weida_seed_session_decline_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a decline op, aborting the DECLINE when unfinished. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live decline op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_decline_op_drop(
    op: *mut weida_seed_session_decline_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Releases a session. It ends with the last holder (a running answer op
/// holds it), and the fetch drops this seeder when an answer was still due.
/// A thread waiting in `weida_seed_session_next` returns `-WEIDA_ECLOSED`.
/// NULL does nothing.
///
/// # Safety
///
/// `s` is NULL or a live session, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_seed_session_drop(s: *mut weida_seed_session) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(s) = unsafe { release(s) } {
            drop(s.tried.close());
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::blob::{weida_piece_list_drop, weida_piece_list_of};
    use crate::config::{
        weida_fetch_options_drop, weida_fetch_options_new, weida_fetch_options_set_have_refresh_ms,
        weida_fetch_options_set_list, weida_fetch_options_set_max_inflight,
        weida_fetch_options_set_max_peers, weida_fetch_options_set_min_rate,
        weida_fetch_options_set_per_peer, weida_fetch_options_set_stall_ms,
    };

    #[test]
    fn fetch_options_setters_land_on_their_fields() {
        let data = vec![7u8; 3000];
        let (mut o, mut list) = (ptr::null_mut(), ptr::null_mut());
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable, the buffer as long as passed.
        unsafe {
            assert_eq!(weida_fetch_options_new(&mut o), 0);
            assert!((*o).options().list.is_none());
            assert_eq!(
                weida_piece_list_of(data.as_ptr(), data.len(), 10, &mut list),
                0
            );
            assert_eq!(weida_fetch_options_set_list(o, list), 0);
            assert_eq!(weida_fetch_options_set_stall_ms(o, 1500), 0);
            assert_eq!(weida_fetch_options_set_min_rate(o, 1000), 0);
            assert_eq!(weida_fetch_options_set_per_peer(o, 2), 0);
            assert_eq!(weida_fetch_options_set_max_inflight(o, 4), 0);
            assert_eq!(weida_fetch_options_set_max_peers(o, 3), 0);
            assert_eq!(weida_fetch_options_set_have_refresh_ms(o, 250), 0);
            let options = (*o).options();
            assert_eq!(options.list.as_ref(), Some(&*(*list).list));
            assert_eq!(options.stall, Duration::from_millis(1500));
            assert_eq!(options.min_rate, 1000);
            assert_eq!(options.per_peer, 2);
            assert_eq!(options.max_inflight, 4);
            assert_eq!(options.max_peers, 3);
            assert_eq!(options.have_refresh, Duration::from_millis(250));

            assert_eq!(weida_fetch_options_set_list(o, ptr::null()), 0);
            assert!((*o).options().list.is_none());
            assert_eq!(weida_fetch_options_set_max_peers(o, u64::MAX), 0);
            assert_eq!((*o).options().max_peers, usize::MAX);
            assert_eq!(
                weida_fetch_options_set_stall_ms(ptr::null(), 1),
                -WEIDA_EINVAL
            );

            weida_piece_list_drop(list);
            weida_fetch_options_drop(o);
        }
    }
}
