//! The listener, its bindings, the replier and the requests it accepts
//! (docs/c/listener.md).

use std::ffi::{c_char, c_int};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use ::weida_runtime::Exec;
use weida_rs::{
    Binding, Error, ErrorCode, Fingerprint, Handoff, IncomingRequest, Listener, LocalBinding,
    Replier, TransferMeta,
};

use crate::config::wire;
use crate::error::{
    WEIDA_EINVAL, WEIDA_EUNSUPPORTED, closed, fail, fail_abi, guard, guard_or, guard_ptr,
    guard_void, null_arg,
};
use crate::handle::{
    CText, borrow, bytes, cstr, into_handle, out_ptr, release, text_out, write_out,
};
use crate::op::Op;
use crate::runtime::{AsOp, op_drop, op_fd, op_wait, weida_runtime};
use crate::stats::weida_peer_events;
use crate::tls::{weida_client_tls, weida_server_tls, weida_transfer_meta};
use crate::transfer::{
    meta_of, weida_incoming_meta, weida_incoming_transfer, weida_outgoing_transfer,
};
use crate::try_handle::Tried;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Where a runtime serves: bindings and the endpoints registered on them.
pub struct weida_listener {
    pub(crate) listener: Listener,
    pub(crate) exec: Exec,
}

/// The pending bind of `weida_listener_bind_quic`.
pub struct weida_listener_bind_quic_op {
    op: Arc<Op<Binding>>,
    exec: Exec,
}

impl AsOp for weida_listener_bind_quic_op {
    type Output = Binding;
    fn op(&self) -> &Op<Binding> {
        &self.op
    }
}

/// The pending bind of `weida_listener_bind_quic_socket`.
pub struct weida_listener_bind_quic_socket_op {
    op: Arc<Op<Binding>>,
    exec: Exec,
}

impl AsOp for weida_listener_bind_quic_socket_op {
    type Output = Binding;
    fn op(&self) -> &Op<Binding> {
        &self.op
    }
}

/// The pending registration of `weida_listener_bind_relay`.
pub struct weida_listener_bind_relay_op {
    op: Arc<Op<Binding>>,
    exec: Exec,
}

impl AsOp for weida_listener_bind_relay_op {
    type Output = Binding;
    fn op(&self) -> &Op<Binding> {
        &self.op
    }
}

/// A QUIC binding: the socket a listener serves on, directly or through a
/// relay.
pub struct weida_binding {
    /// Taken by `weida_binding_hand_over`; every later call is
    /// `-WEIDA_ECLOSED`.
    binding: Mutex<Option<Arc<Binding>>>,
    local_addr: CText,
    /// Made by `weida_listener_bind_relay`: no OS socket to hand over.
    relayed: bool,
    /// Set when a `weida_binding_close` starts.
    closed: AtomicBool,
    exec: Exec,
}

impl weida_binding {
    pub(crate) fn handle(binding: Binding, exec: Exec, relayed: bool) -> *mut weida_binding {
        let local_addr = CText::new(binding.local_addr().to_string());
        into_handle(weida_binding {
            binding: Mutex::new(Some(Arc::new(binding))),
            local_addr,
            relayed,
            closed: AtomicBool::new(false),
            exec,
        })
    }

    /// The binding for one call; `-WEIDA_ECLOSED` once handed over.
    fn get(&self) -> Result<Arc<Binding>, c_int> {
        lock(&self.binding).clone().ok_or_else(closed)
    }
}

/// The pending close of `weida_binding_close`.
pub struct weida_binding_close_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_binding_close_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending hand-over of `weida_binding_hand_over`.
pub struct weida_binding_hand_over_op {
    op: Arc<Op<Handoff>>,
}

impl AsOp for weida_binding_hand_over_op {
    type Output = Handoff;
    fn op(&self) -> &Op<Handoff> {
        &self.op
    }
}

/// The pending take-over of `weida_binding_take_over`.
pub struct weida_binding_take_over_op {
    op: Arc<Op<usize>>,
}

impl AsOp for weida_binding_take_over_op {
    type Output = usize;
    fn op(&self) -> &Op<usize> {
        &self.op
    }
}

/// What a hand-over leaves for the successor (`Handoff`): the socket, taken
/// once, and the held close packets as bytes.
pub struct weida_handoff {
    socket: Mutex<Option<std::net::UdpSocket>>,
    local_addr: CText,
    held: usize,
    bytes: Box<[u8]>,
}

/// An in-process binding: the bus a listener answers on.
pub struct weida_local_binding {
    _binding: LocalBinding,
    bus: CText,
}

/// An `AF_UNIX` binding: the socket file, removed when this is dropped.
pub struct weida_unix_binding {
    #[cfg(unix)]
    _binding: weida_rs::UnixBinding,
    path: CText,
}

/// A Windows named-pipe binding: the pipe name, released when this is
/// dropped.
pub struct weida_pipe_binding {
    #[cfg(windows)]
    _binding: weida_rs::PipeBinding,
    name: CText,
}

pub(crate) type Accepting = Pin<Box<dyn Future<Output = Result<IncomingRequest, Error>> + Send>>;

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

/// The pending refusal of `weida_incoming_request_refuse`.
pub struct weida_incoming_request_refuse_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_incoming_request_refuse_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending `IncomingRequest::canceled` of
/// `weida_incoming_request_canceled`.
pub struct weida_incoming_request_canceled_op {
    op: Arc<Op<()>>,
}

impl AsOp for weida_incoming_request_canceled_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
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
        let tls = tls.get();
        let op = Op::start(&l.exec, async move { listener.bind_quic(addr, tls).await });
        let handle = into_handle(weida_listener_bind_quic_op {
            op,
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
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
/// Release it with `weida_binding_drop`; the binding serves until
/// `weida_binding_close`, a hand-over or the runtime's shutdown.
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
        let handle = unsafe { borrow(op) }?;
        let binding = handle.op.wait(timeout_ms)?;
        let binding = weida_binding::handle(binding, handle.exec.clone(), false);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, binding) };
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

/// Takes ownership of the socket descriptor `socket`: a file descriptor on
/// Unix, a `SOCKET` on Windows (R6).
///
/// # Safety
///
/// A non-negative `socket` is an open UDP socket the caller owns and does
/// not use again.
unsafe fn udp_socket(socket: i64) -> Result<std::net::UdpSocket, c_int> {
    let not_a_socket = || fail_abi(WEIDA_EINVAL, c"not a socket descriptor");
    #[cfg(unix)]
    {
        use std::os::fd::{FromRawFd, RawFd};
        let fd = RawFd::try_from(socket)
            .ok()
            .filter(|fd| *fd >= 0)
            .ok_or_else(not_a_socket)?;
        // SAFETY: an open UDP socket the caller hands over, by this
        // function's contract.
        Ok(unsafe { std::net::UdpSocket::from_raw_fd(fd) })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{FromRawSocket, RawSocket};
        let raw = RawSocket::try_from(socket).map_err(|_| not_a_socket())?;
        // SAFETY: an open UDP socket the caller hands over, by this
        // function's contract.
        Ok(unsafe { std::net::UdpSocket::from_raw_socket(raw) })
    }
}

/// Serves QUIC on a socket that is already bound
/// (`Listener::bind_quic_socket`): a predecessor's after
/// `weida_binding_hand_over`, or one from socket activation. **Takes
/// ownership of `socket`** on every return path but a negative or
/// out-of-range number (`-WEIDA_EINVAL`, nothing taken). weida neither
/// binds it nor sets options on it.
///
/// Never blocks; the bind runs as the op written to `out`, whose wait
/// yields the binding.
///
/// # Safety
///
/// `l` is NULL or a live listener; a non-negative `socket` is an open UDP
/// socket descriptor the caller owns and does not use again; `tls` is NULL
/// or live server terms; `out` is NULL or writable. Release the op with
/// `weida_listener_bind_quic_socket_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_socket(
    l: *const weida_listener,
    socket: i64,
    tls: *const weida_server_tls,
    out: *mut *mut weida_listener_bind_quic_socket_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; from here on the
        // descriptor is owned, and closed by any failure below.
        let socket = unsafe { udp_socket(socket) }?;
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?.get();
        let listener = l.listener.clone();
        let op = Op::start(&l.exec, async move {
            listener.bind_quic_socket(socket, tls).await
        });
        let handle = into_handle(weida_listener_bind_quic_socket_op {
            op,
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The readiness descriptor of a socket bind op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live socket bind op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_socket_op_fd(
    op: *const weida_listener_bind_quic_socket_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the binding and writes it to `out`.
/// `-WEIDA_EIO` when the descriptor is not a usable UDP socket. Release the
/// binding with `weida_binding_drop`.
///
/// # Safety
///
/// `op` is NULL or a live socket bind op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_socket_op_wait(
    op: *const weida_listener_bind_quic_socket_op,
    timeout_ms: i64,
    out: *mut *mut weida_binding,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let handle = unsafe { borrow(op) }?;
        let binding = handle.op.wait(timeout_ms)?;
        let binding = weida_binding::handle(binding, handle.exec.clone(), false);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, binding) };
        Ok(0)
    })
}

/// Releases a socket bind op, aborting the bind when unfinished; the
/// socket is closed then. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live socket bind op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_quic_socket_op_drop(
    op: *mut weida_listener_bind_quic_socket_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Serves this listener's endpoints through a relay, for a peer that can
/// only dial out (`Listener::bind_relay`, PROTOCOL §9.6).
///
/// `relay` is `weida://[relay-key@]host[:port]/.weida/relay`; the peer
/// registers there under the key `via` presents, so `via` needs an
/// identity. Diallers reach the endpoints as
/// `weida+relay://<key>@[relay-key@]host[:port]/path`, with QUIC and TLS
/// end to end on `tls`'s terms. A lost relay is redialled and registered
/// again (`weida_binding_relay_events`).
///
/// Never blocks; the registration runs as the op written to `out`. Its
/// wait returns the first registration's failure: `-WEIDA_EREJECTED` where
/// the relay refused it, `-WEIDA_EINVALID_ADDRESS` for another path.
///
/// # Safety
///
/// `l` is NULL or a live listener; `relay` is NULL or a NUL-terminated
/// string; `via` is NULL or live client terms; `tls` is NULL or live
/// server terms; `out` is NULL or writable. Release the op with
/// `weida_listener_bind_relay_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_relay(
    l: *const weida_listener,
    relay: *const c_char,
    via: *const weida_client_tls,
    tls: *const weida_server_tls,
    out: *mut *mut weida_listener_bind_relay_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let relay = unsafe { cstr(relay) }?.to_owned();
        // SAFETY: forwarded from this function's contract.
        let via = unsafe { borrow(via) }?.get();
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?.get();
        let listener = l.listener.clone();
        let op = Op::start(&l.exec, async move {
            listener.bind_relay(&relay, via, tls).await
        });
        let handle = into_handle(weida_listener_bind_relay_op {
            op,
            exec: l.exec.clone(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// The readiness descriptor of a relay bind op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live relay bind op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_relay_op_fd(
    op: *const weida_listener_bind_relay_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the first registration and writes the
/// relayed binding to `out`. Release it with `weida_binding_drop`.
///
/// # Safety
///
/// `op` is NULL or a live relay bind op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_relay_op_wait(
    op: *const weida_listener_bind_relay_op,
    timeout_ms: i64,
    out: *mut *mut weida_binding,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let handle = unsafe { borrow(op) }?;
        let binding = handle.op.wait(timeout_ms)?;
        let binding = weida_binding::handle(binding, handle.exec.clone(), true);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, binding) };
        Ok(0)
    })
}

/// Releases a relay bind op, aborting the registration when unfinished.
/// NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live relay bind op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_relay_op_drop(op: *mut weida_listener_bind_relay_op) {
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

/// Serves the listener's endpoints on the `AF_UNIX` socket `path`
/// (`Listener::bind_unix`): a requester reaches it at
/// `weida+unix://<percent-encoded path>/<endpoint>`. No TLS: the kernel
/// proves the peer. `path` MUST be in a directory this process owns and no
/// other user may write; a stale socket file is replaced. Fails with
/// `-WEIDA_EIO` for a live listener on the path, `-WEIDA_EINVALID_ADDRESS`
/// for a path over the `sun_path` budget or something at it that is not a
/// socket, and `-WEIDA_EUNSUPPORTED` off Unix.
///
/// # Safety
///
/// `l` is NULL or a live listener; `path` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_unix_binding_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_unix(
    l: *const weida_listener,
    path: *const c_char,
    out: *mut *mut weida_unix_binding,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        #[cfg(unix)]
        {
            let binding = l.listener.bind_unix(path).map_err(fail)?;
            let text = CText::new(std::os::unix::ffi::OsStrExt::as_bytes(
                binding.path().as_os_str(),
            ));
            let handle = into_handle(weida_unix_binding {
                _binding: binding,
                path: text,
            });
            // SAFETY: `out` passed `out_ptr`.
            unsafe { write_out(out, handle) };
            Ok(0)
        }
        #[cfg(not(unix))]
        {
            let _ = (out, l, path);
            Err(fail_abi(
                WEIDA_EUNSUPPORTED,
                c"AF_UNIX bindings exist on Unix only",
            ))
        }
    })
}

/// Binds the Windows named pipe `\\.\pipe\<name>` owner-only and
/// local-only, only if the name does not exist yet, and serves the
/// listener's endpoints on it (`Listener::bind_pipe`): a requester reaches
/// it at `weida+pipe://<name>/<endpoint>`. Fails with
/// `-WEIDA_EINVALID_ADDRESS` for an invalid name or one that exists already,
/// `-WEIDA_EIO`, and `-WEIDA_EUNSUPPORTED` off Windows.
///
/// # Safety
///
/// `l` is NULL or a live listener; `name` is NULL or a NUL-terminated
/// string; `out` is NULL or writable. Release the result with
/// `weida_pipe_binding_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_listener_bind_pipe(
    l: *const weida_listener,
    name: *const c_char,
    out: *mut *mut weida_pipe_binding,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let l = unsafe { borrow(l) }?;
        // SAFETY: forwarded from this function's contract.
        let name = unsafe { cstr(name) }?;
        #[cfg(windows)]
        {
            let binding = l.listener.bind_pipe(name).map_err(fail)?;
            let text = CText::new(binding.name());
            let handle = into_handle(weida_pipe_binding {
                _binding: binding,
                name: text,
            });
            // SAFETY: `out` passed `out_ptr`.
            unsafe { write_out(out, handle) };
            Ok(0)
        }
        #[cfg(not(windows))]
        {
            let _ = (out, l, name);
            Err(fail_abi(
                WEIDA_EUNSUPPORTED,
                c"named-pipe bindings exist on Windows only",
            ))
        }
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

/// Stops accepting and closes the binding's connections with `SHUTDOWN`
/// (`Binding::close`).
///
/// Never blocks; the close runs as the op written to `out`. The op
/// finishes once the socket went idle, bounded by `shutdown_timeout`, and
/// the port is free to bind again. A relayed binding ends its registration
/// at the relay. The handle stays valid (`weida_binding_local_addr`); drop
/// it as usual. `-WEIDA_ECLOSED` once handed over.
///
/// # Safety
///
/// `b` is NULL or a live binding; `out` is NULL or writable. Release the op
/// with `weida_binding_close_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_close(
    b: *const weida_binding,
    out: *mut *mut weida_binding_close_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        let binding = b.get()?;
        b.closed.store(true, Ordering::Release);
        let op = Op::start(&b.exec, async move {
            binding.close().await;
            Ok(())
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_binding_close_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a close op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live close op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_close_op_fd(
    op: *const weida_binding_close_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the close; 0 once the port is free.
///
/// # Safety
///
/// `op` is NULL or a live close op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_close_op_wait(
    op: *const weida_binding_close_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a close op. Dropped unfinished, it aborts the close where it
/// is: the connections may already be closed, and the port is then freed
/// only by the runtime's shutdown. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live close op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_close_op_drop(op: *mut weida_binding_close_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Hands the binding's socket over to a successor process
/// (`Binding::hand_over`, decision 0039). **Consumes** `b` on every return
/// path.
///
/// From the call on, this process neither reads nor sends on the socket.
/// Every connection is closed with `RESTARTING`, with the close packets
/// held in the `weida_handoff` the op yields and not sent. Returns
/// `-WEIDA_EUNSUPPORTED` for a relayed binding, which has no socket;
/// `-WEIDA_ECLOSED` for one a `weida_binding_close` was started on;
/// `-WEIDA_EINVAL` while a `weida_binding_take_over` op of it is
/// unfinished. A binding consumed by one of these failures keeps serving
/// until its runtime shuts down. After the runtime's shutdown the op ends
/// with `-WEIDA_ERUNTIME`.
///
/// # Safety
///
/// `b` is NULL or a live binding, not used again; `out` is NULL or
/// writable. Release the op with `weida_binding_hand_over_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_hand_over(
    b: *mut weida_binding,
    out: *mut *mut weida_binding_hand_over_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let b = unsafe { release(b) }.ok_or_else(null_arg)?;
        let taken = lock(&b.binding).take();
        let out = out_ptr(out)?;
        let binding = taken.ok_or_else(closed)?;
        if b.relayed {
            return Err(fail(Error::Unsupported));
        }
        if b.closed.load(Ordering::Acquire) {
            return Err(closed());
        }
        let binding = Arc::try_unwrap(binding).map_err(|_| {
            fail_abi(
                WEIDA_EINVAL,
                c"a take-over of this binding is still running",
            )
        })?;
        let op = Op::start(&b.exec, async move { Ok(binding.hand_over().await) });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_binding_hand_over_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a hand-over op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live hand-over op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_hand_over_op_fd(
    op: *const weida_binding_hand_over_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the hand-over and writes what it left
/// for the successor to `out`. `-WEIDA_ERUNTIME` once the runtime shut
/// down.
///
/// # Safety
///
/// `op` is NULL or a live hand-over op; `out` is NULL or writable. Release
/// the handoff with `weida_handoff_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_hand_over_op_wait(
    op: *const weida_binding_hand_over_op,
    timeout_ms: i64,
    out: *mut *mut weida_handoff,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let handoff = unsafe { op_wait(op, timeout_ms) }?;
        let held = handoff.held();
        let local_addr = CText::new(handoff.local_addr().to_string());
        let (socket, bytes) = handoff.into_parts();
        let handle = into_handle(weida_handoff {
            socket: Mutex::new(Some(socket)),
            local_addr,
            held,
            bytes: bytes.into_boxed_slice(),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a hand-over op. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live hand-over op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_hand_over_op_drop(op: *mut weida_binding_hand_over_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Sends the close packets a predecessor held (`Binding::take_over`), so
/// its clients redial into this binding.
///
/// Call it once the endpoints those clients use are registered. Each
/// datagram is sent once. `held` is `weida_handoff_bytes` of the
/// predecessor's handoff, and needs the integrity of the channel that
/// carries the socket: whoever can write it decides what this binding
/// sends from its port. Never blocks; the sending runs as the op written
/// to `out`. `-WEIDA_ECLOSED` once handed over.
///
/// # Safety
///
/// `b` is NULL or a live binding; `held` is NULL or points to `len`
/// readable bytes (copied before the call returns); `out` is NULL or
/// writable. Release the op with `weida_binding_take_over_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_take_over(
    b: *const weida_binding,
    held: *const u8,
    len: usize,
    out: *mut *mut weida_binding_take_over_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        // SAFETY: forwarded from this function's contract.
        let held = unsafe { bytes(held, len) }?.to_vec();
        let binding = b.get()?;
        let op = Op::start(&b.exec, async move { binding.take_over(&held).await });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_binding_take_over_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a take-over op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live take-over op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_take_over_op_fd(
    op: *const weida_binding_take_over_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the take-over and writes how many
/// datagrams were sent to `out`. `-WEIDA_EIO` for bytes that are not a
/// handoff, or a handoff of another address.
///
/// # Safety
///
/// `op` is NULL or a live take-over op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_take_over_op_wait(
    op: *const weida_binding_take_over_op,
    timeout_ms: i64,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let sent = unsafe { op_wait(op, timeout_ms) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, sent) };
        Ok(0)
    })
}

/// Releases a take-over op, aborting it when unfinished. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live take-over op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_take_over_op_drop(op: *mut weida_binding_take_over_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Closes every live connection this binding holds from the peer whose key
/// is `peer` (`sha256:` text) with `REJECTED`, and writes how many it
/// closed (`Binding::disconnect`).
///
/// Not a ban: the peer sees `WEIDA_LOSS_PEER_CLOSED` and redials under its
/// own policy. An anonymous client cannot be named.
/// `-WEIDA_EINVALID_FINGERPRINT` for text that is not a key,
/// `-WEIDA_ECLOSED` once handed over.
///
/// # Safety
///
/// `b` is NULL or a live binding; `peer` is NULL or a NUL-terminated
/// string; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_disconnect(
    b: *const weida_binding,
    peer: *const c_char,
    out: *mut usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        // SAFETY: forwarded from this function's contract.
        let text = unsafe { cstr(peer) }?;
        let fp: Fingerprint = text.parse().map_err(fail)?;
        let closed = b.get()?.disconnect(fp);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, closed) };
        Ok(0)
    })
}

/// A new reader of a relayed binding's registration events
/// (`Binding::relay_events`): lost, redialled and registered again, given
/// up.
///
/// Returns 1 and writes the reader to `out`. Returns 0 and leaves `out`
/// untouched for a binding that is not relayed, or whose registration
/// ended with its close. `-WEIDA_ECLOSED` once handed over.
///
/// # Safety
///
/// `b` is NULL or a live binding; `out` is NULL or writable. Release the
/// reader with `weida_peer_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_binding_relay_events(
    b: *const weida_binding,
    out: *mut *mut weida_peer_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let b = unsafe { borrow(b) }?;
        let Some(events) = b.get()?.relay_events() else {
            return Ok(0);
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_peer_events::handle(events)) };
        Ok(1)
    })
}

/// How many close packets the handoff holds for the successor
/// (`Handoff::held`); 0 for NULL.
///
/// # Safety
///
/// `h` is NULL or a live handoff.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_handoff_held(h: *const weida_handoff) -> usize {
    guard_or(0, || {
        // SAFETY: a non-NULL `h` is a live handoff by this function's
        // contract.
        unsafe { h.as_ref() }.map_or(0, |h| h.held)
    })
}

/// The address the handed-over socket is bound to (`Handoff::local_addr`).
/// Lives as long as the handoff.
///
/// # Safety
///
/// `h` is NULL or a live handoff; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_handoff_local_addr(
    h: *const weida_handoff,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if h.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `h` is a live handoff by this function's
        // contract; the text lives as long as it.
        let h = unsafe { &*h };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&h.local_addr), len_out) }
    })
}

/// The held datagrams for the successor's `weida_binding_take_over`, with
/// their length in `*len_out`; NULL for NULL `h`. Lives as long as `h`.
///
/// They need the integrity of the channel that carries the socket (see
/// `Handoff` in Rust).
///
/// # Safety
///
/// `h` is NULL or a live handoff; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_handoff_bytes(
    h: *const weida_handoff,
    len_out: *mut usize,
) -> *const u8 {
    guard_ptr(|| {
        if h.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `h` is a live handoff by this function's
        // contract; the bytes live as long as it.
        let h = unsafe { &*h };
        if !len_out.is_null() {
            // SAFETY: a non-NULL `len_out` is writable by this function's
            // contract.
            unsafe { len_out.write(h.bytes.len()) };
        }
        h.bytes.as_ptr()
    })
}

/// Hands the socket's descriptor to the caller, who owns it from then on:
/// a file descriptor on Unix, a `SOCKET` on Windows. A second call returns
/// `-WEIDA_ECLOSED`.
///
/// # Safety
///
/// `h` is NULL or a live handoff; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_handoff_take_socket(
    h: *const weida_handoff,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let h = unsafe { borrow(h) }?;
        let socket = lock(&h.socket).take().ok_or_else(closed)?;
        #[cfg(unix)]
        let raw = {
            use std::os::fd::IntoRawFd;
            i64::from(socket.into_raw_fd())
        };
        #[cfg(windows)]
        let raw = {
            use std::os::windows::io::IntoRawSocket;
            socket.into_raw_socket() as i64
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, raw) };
        Ok(0)
    })
}

/// Releases a handoff; a socket not taken is closed. NULL does nothing.
///
/// # Safety
///
/// `h` is NULL or a live handoff, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_handoff_drop(h: *mut weida_handoff) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(h) }));
}

/// Releases a binding handle. The binding keeps serving, as a dropped Rust
/// `Binding` does, until `weida_binding_close`, a hand-over or the
/// runtime's shutdown. NULL does nothing.
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

/// The socket path bound. Lives as long as the binding.
///
/// # Safety
///
/// `b` is NULL or a live unix binding; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_unix_binding_path(
    b: *const weida_unix_binding,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if b.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `b` is a live unix binding by this function's
        // contract; the text lives as long as it.
        let b = unsafe { &*b };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&b.path), len_out) }
    })
}

/// Releases a unix binding, which removes the socket file, so a new dial
/// finds nobody. NULL does nothing.
///
/// # Safety
///
/// `b` is NULL or a live unix binding, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_unix_binding_drop(b: *mut weida_unix_binding) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(b) }));
}

/// The pipe name bound, without `\\.\pipe\`. Lives as long as the binding.
///
/// # Safety
///
/// `b` is NULL or a live pipe binding; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pipe_binding_name(
    b: *const weida_pipe_binding,
    len_out: *mut usize,
) -> *const c_char {
    guard_ptr(|| {
        if b.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `b` is a live pipe binding by this function's
        // contract; the text lives as long as it.
        let b = unsafe { &*b };
        // SAFETY: forwarded from this function's contract.
        unsafe { text_out(Some(&b.name), len_out) }
    })
}

/// Releases a pipe binding, which ends its accept loop and releases the
/// name. NULL does nothing.
///
/// # Safety
///
/// `b` is NULL or a live pipe binding, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pipe_binding_drop(b: *mut weida_pipe_binding) {
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
        let handle = weida_incoming_request::handle(request, r.exec.clone());
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
    /// A new handle for an accepted request, its body not yet taken.
    pub(crate) fn handle(request: IncomingRequest, exec: Exec) -> *mut weida_incoming_request {
        into_handle(weida_incoming_request {
            state: Mutex::new(Some(RequestState {
                request,
                body_taken: false,
            })),
            meta: OnceLock::new(),
            exec,
        })
    }

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

/// The number of the connection the request arrived on
/// (`IncomingRequest::connection`, `ConnectionId::get`): equal for requests
/// of one connection, never shared by two connections of this process. Not
/// an address.
///
/// # Safety
///
/// `q` is NULL or a live request; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_connection(
    q: *const weida_incoming_request,
    out: *mut u64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let q = unsafe { borrow(q) }?;
        let id = match q.lock().as_ref() {
            Some(state) => state.request.connection().get(),
            None => return Err(crate::error::closed()),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, id) };
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

/// Refuses the exchange (`IncomingRequest::refuse`): the requester's op
/// fails with the error of `code`, a `WEIDA_ERROR_CODE_*` constant (README
/// R9). **Consumes** `q` on every return path.
///
/// Never blocks; the ERROR frame is written as the op written to `out`. A
/// body still attached is stopped with `REJECTED`. A `code` that is not a
/// `WEIDA_ERROR_CODE_*` is `-WEIDA_EINVAL`, and the request is consumed all
/// the same: it answers `NO_REPLY`.
///
/// # Safety
///
/// `q` is NULL or a live request, not used again; `out` is NULL or
/// writable. Release the op with `weida_incoming_request_refuse_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_refuse(
    q: *mut weida_incoming_request,
    code: c_int,
    out: *mut *mut weida_incoming_request_refuse_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let q = unsafe { release(q) }.ok_or_else(crate::error::null_arg)?;
        let state = q.lock().take();
        let out = out_ptr(out)?;
        let state = state.ok_or_else(crate::error::closed)?;
        let code = wire(code, ErrorCode::from_wire)?;
        let request = state.request;
        let op = Op::start(&q.exec, async move {
            request.refuse(code).await;
            Ok(())
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_incoming_request_refuse_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a refuse op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live refuse op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_refuse_op_fd(
    op: *const weida_incoming_request_refuse_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the refusal to be written. A refusal
/// never fails: one that could not be written (the connection is gone)
/// also returns 0.
///
/// # Safety
///
/// `op` is NULL or a live refuse op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_refuse_op_wait(
    op: *const weida_incoming_request_refuse_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a refuse op. Dropped before its wait returned 0, it aborts the
/// refusal, and the requester may see `NO_REPLY` instead. NULL does
/// nothing.
///
/// # Safety
///
/// `op` is NULL or a live refuse op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_refuse_op_drop(
    op: *mut weida_incoming_request_refuse_op,
) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Starts watching for the requester to stop wanting a reply
/// (`IncomingRequest::canceled`).
///
/// The op is independent of `q`: it outlives a
/// `weida_incoming_request_reply`, `_refuse` or `_drop` of the request.
/// Start it before those consume `q`. Its wait returns 0 once any of these
/// happened: the requester dropped its request op or its reply; a finished
/// reply was read whole; the connection is gone.
///
/// # Safety
///
/// `q` is NULL or a live request; `out` is NULL or writable. Release the op
/// with `weida_incoming_request_canceled_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_canceled(
    q: *const weida_incoming_request,
    out: *mut *mut weida_incoming_request_canceled_op,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let q = unsafe { borrow(q) }?;
        let canceled = match q.lock().as_ref() {
            Some(state) => state.request.canceled(),
            None => return Err(crate::error::closed()),
        };
        let op = Op::start(&q.exec, async move {
            canceled.await;
            Ok(())
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_incoming_request_canceled_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a canceled op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live canceled op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_canceled_op_fd(
    op: *const weida_incoming_request_canceled_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the cancellation; 0 once it happened.
/// `-WEIDA_ERUNTIME` once the runtime shut down.
///
/// # Safety
///
/// `op` is NULL or a live canceled op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_canceled_op_wait(
    op: *const weida_incoming_request_canceled_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a canceled op, ending the watch. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live canceled op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_incoming_request_canceled_op_drop(
    op: *mut weida_incoming_request_canceled_op,
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
