//! The runtime and its configuration (docs/c/runtime.md, R13).

use std::ffi::c_int;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use weida_rs::{Drained, GuaranteeSet, Runtime, RuntimeConfig, SharedResolver};

use crate::config::{
    Builder, count, discovery, ms, optional, outbox_full, weida_guarantee_set, weida_limits,
    weida_reconnect_policy,
};
use crate::error::{WEIDA_EINVAL, fail_abi, guard, guard_or, guard_void};
use crate::handle::{borrow, into_handle, out_ptr, release, write_out};
use crate::op::Op;
use crate::resolver::weida_resolver;

/// A runtime configuration under construction: one setter per field but
/// `network`.
pub struct weida_runtime_config {
    pub(crate) config: Mutex<RuntimeConfig>,
}

impl weida_runtime_config {
    fn set(&self, f: impl FnOnce(&mut RuntimeConfig)) {
        f(&mut self.config.lock().unwrap_or_else(|e| e.into_inner()));
    }
}

/// A weida runtime: the reactor it owns, and every endpoint made from it.
pub struct weida_runtime {
    pub(crate) runtime: Runtime,
}

/// The pending `Runtime::shutdown` of `weida_runtime_shutdown`.
pub struct weida_runtime_shutdown_op {
    pub(crate) op: Arc<Op<()>>,
}

/// An op handle: the shared shape of every `_op_fd`, `_op_wait` and
/// `_op_drop`.
pub(crate) trait AsOp {
    type Output;
    fn op(&self) -> &Op<Self::Output>;
}

impl AsOp for weida_runtime_shutdown_op {
    type Output = ();
    fn op(&self) -> &Op<()> {
        &self.op
    }
}

/// The pending `Runtime::drain` of `weida_runtime_drain`.
pub struct weida_runtime_drain_op {
    pub(crate) op: Arc<Op<Drained>>,
}

impl AsOp for weida_runtime_drain_op {
    type Output = Drained;
    fn op(&self) -> &Op<Drained> {
        &self.op
    }
}

/// What one drain achieved (`Drained`), read-only: two counts about the
/// transfers that were still unacknowledged when it started.
pub struct weida_drained {
    drained: Drained,
}

/// `_op_fd`: writes the op's readiness descriptor to `out`.
///
/// # Safety
///
/// `h` is NULL or a live op handle of type `H`; `out` is NULL or writable.
pub(crate) unsafe fn op_fd<H: AsOp>(h: *const H, out: *mut i64) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let h = unsafe { borrow(h) }?;
        let fd = h.op().fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// `_op_wait`: takes the op's result within `timeout_ms`.
///
/// # Safety
///
/// `h` is NULL or a live op handle of type `H`.
pub(crate) unsafe fn op_wait<H: AsOp>(h: *const H, timeout_ms: i64) -> Result<H::Output, c_int> {
    // SAFETY: forwarded from this function's contract.
    let h = unsafe { borrow(h) }?;
    h.op().wait(timeout_ms)
}

/// `_op_drop`: aborts an unfinished op and releases the caller's reference.
///
/// # Safety
///
/// `h` is NULL or a live op handle of type `H`, dropped at most once.
pub(crate) unsafe fn op_drop<H: AsOp>(h: *const H) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(h) = unsafe { release(h) } {
            h.op().close();
        }
    });
}

/// Creates a configuration holding weida's defaults.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_runtime_config_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_new(out: *mut *mut weida_runtime_config) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let handle = into_handle(weida_runtime_config {
            config: Mutex::new(RuntimeConfig::default()),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Sets `RuntimeConfig::worker_threads`: the reactor's worker threads.
/// `0` is accepted here and refused by `weida_runtime_owned`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_worker_threads(
    cfg: *const weida_runtime_config,
    threads: u32,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.worker_threads = threads as usize);
        Ok(0)
    })
}

/// Sets `RuntimeConfig::shutdown_timeout`, in milliseconds: the bound on
/// `weida_runtime_shutdown`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_shutdown_timeout_ms(
    cfg: *const weida_runtime_config,
    ms: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.shutdown_timeout = Duration::from_millis(ms));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::max_connections` (a count, R7): connections one
/// binding accepts at once.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_max_connections(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.max_connections = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::max_connections_per_peer` (a count, R7):
/// connections one proved peer may hold on one binding.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_max_connections_per_peer(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.max_connections_per_peer = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::max_resolved_addresses` (a count, R7): addresses a
/// dial tries for one hostname.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_max_resolved_addresses(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.max_resolved_addresses = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::outbox_messages` (a count, R7): bodies a dialling
/// endpoint's outbox holds while no peer is live.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_outbox_messages(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.outbox_messages = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::outbox_bytes` (a count, R7): the bytes those bodies
/// may sum to.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_outbox_bytes(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.outbox_bytes = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::max_handshakes` (a count, R7): handshakes one QUIC
/// binding runs at once.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_max_handshakes(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.max_handshakes = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::retry_above_handshakes` (a count, R7): handshakes
/// in progress from which a QUIC binding answers with a stateless Retry.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_retry_above_handshakes(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.retry_above_handshakes = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::endpoint_queue` (a count, R7): the depth of an
/// endpoint's accept queue. 0 is `-WEIDA_EINVAL`: a queue needs a place.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_endpoint_queue(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        if v == 0 {
            return Err(fail_abi(WEIDA_EINVAL, c"endpoint_queue must be at least 1"));
        }
        cfg.set(|c| c.endpoint_queue = count(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::connect_attempt_timeout`, in milliseconds: how long
/// a dial waits on one resolved address before trying the next.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_connect_attempt_timeout_ms(
    cfg: *const weida_runtime_config,
    v: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.connect_attempt_timeout = ms(v));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::send_timeout`, in milliseconds, or none for a
/// negative value (R7): how long an `open` on a dialling endpoint waits for
/// a lost peer to come back before it fails with the loss cause. None waits
/// as long as the redial does.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_send_timeout_ms(
    cfg: *const weida_runtime_config,
    v: i64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        cfg.set(|c| c.send_timeout = optional(v).map(ms));
        Ok(0)
    })
}

/// Sets `RuntimeConfig::outbox_full` to a `WEIDA_OUTBOX_FULL_*` constant:
/// what a send does at the outbox bound. Any other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_outbox_full(
    cfg: *const weida_runtime_config,
    v: c_int,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        let full = outbox_full(v)?;
        cfg.set(|c| c.outbox_full = full);
        Ok(0)
    })
}

/// Sets `RuntimeConfig::discovery` to a `WEIDA_DISCOVERY_*` constant:
/// whether an authority may name a set of nodes. Any other value is
/// `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_discovery(
    cfg: *const weida_runtime_config,
    v: c_int,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        let discovery = discovery(v)?;
        cfg.set(|c| c.discovery = discovery);
        Ok(0)
    })
}

/// Sets `RuntimeConfig::limits` to a copy of `v`, taken now; NULL is
/// `Limits::default()`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration; `v` is NULL or live limits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_limits(
    cfg: *const weida_runtime_config,
    v: *const weida_limits,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        let value = if v.is_null() {
            Default::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(v) }?.get()
        };
        cfg.set(|c| c.limits = value);
        Ok(0)
    })
}

/// Sets `RuntimeConfig::guarantees` to a copy of `v`, taken now; NULL is
/// `GuaranteeSet::CORE`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration; `v` is NULL or a live guarantee set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_guarantees(
    cfg: *const weida_runtime_config,
    v: *const weida_guarantee_set,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        let value = if v.is_null() {
            GuaranteeSet::CORE
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(v) }?.get()
        };
        cfg.set(|c| c.guarantees = value);
        Ok(0)
    })
}

/// Sets `RuntimeConfig::reconnect` to a copy of `v`, taken now; NULL is
/// `ReconnectPolicy::default()`.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration; `v` is NULL or a live policy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_reconnect(
    cfg: *const weida_runtime_config,
    v: *const weida_reconnect_policy,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        let value = if v.is_null() {
            Default::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(v) }?.get()
        };
        cfg.set(|c| c.reconnect = value);
        Ok(0)
    })
}

/// Sets `RuntimeConfig::resolver`: every dial of a runtime made from this
/// configuration reads its addresses from `r`'s queue; NULL is the system
/// resolver (`SystemResolver`). The configuration shares `r`'s queue, it
/// does not copy it. `-WEIDA_ECLOSED` when `r`'s queue already ended.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration; `r` is NULL or a live resolver.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_set_resolver(
    cfg: *const weida_runtime_config,
    r: *const weida_resolver,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let cfg = unsafe { borrow(cfg) }?;
        let value = if r.is_null() {
            SharedResolver::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(r) }?.shared()?
        };
        cfg.set(|c| c.resolver = value);
        Ok(0)
    })
}

/// Releases a configuration. NULL does nothing.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_config_drop(cfg: *mut weida_runtime_config) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(cfg) }));
}

/// Creates a runtime on a reactor of its own (`Runtime::owned`), with the
/// configuration `cfg` holds, or weida's defaults when `cfg` is NULL.
///
/// The reactor's threads block every asynchronous signal (R12). The
/// reactor shuts down in the background once the runtime and everything
/// made from it are dropped (R13).
///
/// Fails with `-WEIDA_ERUNTIME` when `worker_threads` is 0 or the OS refuses
/// the threads.
///
/// # Safety
///
/// `cfg` is NULL or a live configuration; `out` is NULL or writable.
/// Release the result with `weida_runtime_drop` or `weida_runtime_shutdown`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_owned(
    cfg: *const weida_runtime_config,
    out: *mut *mut weida_runtime,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let config = if cfg.is_null() {
            RuntimeConfig::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            let cfg = unsafe { borrow(cfg) }?;
            let config = cfg.config.lock().unwrap_or_else(|e| e.into_inner());
            config.clone()
        };
        let runtime = Runtime::owned(config).map_err(crate::error::fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_runtime { runtime })) };
        Ok(0)
    })
}

/// Shuts the runtime down (`Runtime::shutdown`): closes every endpoint and
/// waits, bounded by `shutdown_timeout`, for the connections to go idle.
/// **Consumes** `rt` on every return path.
///
/// Never blocks; the shutdown runs as the op written to `out`. Waiting for
/// it is optional: the runtime is closed either way.
///
/// # Safety
///
/// `rt` is NULL or a live runtime, not used again; `out` is NULL or
/// writable. Release the op with `weida_runtime_shutdown_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_shutdown(
    rt: *mut weida_runtime,
    out: *mut *mut weida_runtime_shutdown_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let rt = unsafe { release(rt) }.ok_or_else(crate::error::null_arg)?;
        let out = out_ptr(out)?;
        let runtime = rt.runtime.clone();
        drop(rt);
        let exec = runtime.exec().clone();
        let op = Op::start(&exec, async move {
            runtime.shutdown().await;
            Ok(())
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_runtime_shutdown_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a shutdown op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live shutdown op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_shutdown_op_fd(
    op: *const weida_runtime_shutdown_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the shutdown to finish.
///
/// # Safety
///
/// `op` is NULL or a live shutdown op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_shutdown_op_wait(
    op: *const weida_runtime_shutdown_op,
    timeout_ms: i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| unsafe { op_wait(op, timeout_ms) }.map(|()| 0))
}

/// Releases a shutdown op. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live shutdown op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_shutdown_op_drop(op: *mut weida_runtime_shutdown_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Drains the runtime (`Runtime::drain`). First admission stops: bindings
/// accept no new connection, and a new inbound stream is refused with
/// `SHUTDOWN`. Then every finished transfer whose receipt nobody holds (a
/// `weida_delivery` dropped unwaited) gets until `deadline_ms` to reach the
/// peer's transport. Last comes the close of `weida_runtime_shutdown`,
/// within what is left of the same deadline. A drain never waits for a
/// reply or for a peer application. **Consumes** `rt` on every return path.
///
/// Never blocks; the drain runs as the op written to `out`, whose wait
/// yields its `weida_drained` report. Dropping the op unfinished aborts the
/// drain where it is: admission, once stopped, stays stopped, and no close
/// runs.
///
/// # Safety
///
/// `rt` is NULL or a live runtime, not used again; `out` is NULL or
/// writable. Release the op with `weida_runtime_drain_op_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_drain(
    rt: *mut weida_runtime,
    deadline_ms: u64,
    out: *mut *mut weida_runtime_drain_op,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract; the handle is
        // consumed here whatever happens next.
        let rt = unsafe { release(rt) }.ok_or_else(crate::error::null_arg)?;
        let out = out_ptr(out)?;
        let runtime = rt.runtime.clone();
        drop(rt);
        let exec = runtime.exec().clone();
        let deadline = Duration::from_millis(deadline_ms);
        let op = Op::start(&exec, async move { Ok(runtime.drain(deadline).await) });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_runtime_drain_op { op })) };
        Ok(0)
    })
}

/// The readiness descriptor of a drain op: readable once it finished.
///
/// # Safety
///
/// `op` is NULL or a live drain op; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_drain_op_fd(
    op: *const weida_runtime_drain_op,
    out: *mut i64,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_fd(op, out) }
}

/// Waits up to `timeout_ms` (R4) for the drain to finish and writes its
/// report to `out`.
///
/// # Safety
///
/// `op` is NULL or a live drain op; `out` is NULL or writable. Release the
/// report with `weida_drained_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_drain_op_wait(
    op: *const weida_runtime_drain_op,
    timeout_ms: i64,
    out: *mut *mut weida_drained,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let drained = unsafe { op_wait(op, timeout_ms) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_drained { drained })) };
        Ok(0)
    })
}

/// Releases a drain op, aborting an unfinished drain. NULL does nothing.
///
/// # Safety
///
/// `op` is NULL or a live drain op, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_drain_op_drop(op: *mut weida_runtime_drain_op) {
    // SAFETY: forwarded from this function's contract.
    unsafe { op_drop(op) }
}

/// Transfers whose payload and FIN the peer's transport acknowledged before
/// the deadline (`Drained::delivered`); 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live report.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_drained_delivered(d: *const weida_drained) -> u64 {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live report by the caller's contract.
        unsafe { &*d }.drained.delivered
    })
}

/// Transfers still unacknowledged at the deadline, plus those the peer
/// refused and those whose connection was lost (`Drained::outstanding`). A
/// non-zero count is not an error. 0 for NULL.
///
/// # Safety
///
/// `d` is NULL or a live report.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_drained_outstanding(d: *const weida_drained) -> u64 {
    guard_or(0, || {
        if d.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `d` is a live report by the caller's contract.
        unsafe { &*d }.drained.outstanding
    })
}

/// Releases a report. NULL does nothing.
///
/// # Safety
///
/// `d` is NULL or a live report, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_drained_drop(d: *mut weida_drained) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(d) }));
}

/// Duplicates every connection of this runtime read and discarded inside a
/// negotiated `Deduplication::Bounded` window
/// (`Runtime::suppressed_duplicates`); 0 without deduplication, and for
/// NULL.
///
/// # Safety
///
/// `rt` is NULL or a live runtime.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_suppressed_duplicates(rt: *const weida_runtime) -> u64 {
    guard_or(0, || {
        if rt.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `rt` is a live runtime by the caller's contract.
        unsafe { &*rt }.runtime.suppressed_duplicates()
    })
}

/// Outgoing transfers this runtime reset because their deadline passed
/// (`Runtime::expired_transfers`), counted apart from a cancel; 0 for NULL.
///
/// # Safety
///
/// `rt` is NULL or a live runtime.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_expired_transfers(rt: *const weida_runtime) -> u64 {
    guard_or(0, || {
        if rt.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `rt` is a live runtime by the caller's contract.
        unsafe { &*rt }.runtime.expired_transfers()
    })
}

/// Releases the caller's reference to a runtime. The reactor lives on while
/// anything made from it does (R13). NULL does nothing.
///
/// # Safety
///
/// `rt` is NULL or a live runtime, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_runtime_drop(rt: *mut weida_runtime) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(rt) }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use std::ptr;
    use weida_rs::{
        Acknowledgement, Backpressure, Congestion, Deduplication, DeliveryLevel, Discovery,
        Durability, Limits, OrderingMode, OutboxFull, ProducerNaming, ReconnectPolicy,
    };

    /// The four builders, freshly made.
    fn builders() -> (
        *mut weida_runtime_config,
        *mut weida_limits,
        *mut weida_guarantee_set,
        *mut weida_reconnect_policy,
    ) {
        let (mut cfg, mut l, mut g, mut p) = (
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        );
        // SAFETY: every out-pointer is writable.
        unsafe {
            assert_eq!(weida_runtime_config_new(&mut cfg), 0);
            assert_eq!(weida_limits_new(&mut l), 0);
            assert_eq!(weida_guarantee_set_new(&mut g), 0);
            assert_eq!(weida_reconnect_policy_new(&mut p), 0);
        }
        (cfg, l, g, p)
    }

    /// Drops what [`builders`] made.
    fn drop_all(
        (cfg, l, g, p): (
            *mut weida_runtime_config,
            *mut weida_limits,
            *mut weida_guarantee_set,
            *mut weida_reconnect_policy,
        ),
    ) {
        // SAFETY: each handle is live and dropped once.
        unsafe {
            weida_runtime_config_drop(cfg);
            weida_limits_drop(l);
            weida_guarantee_set_drop(g);
            weida_reconnect_policy_drop(p);
        }
    }

    #[test]
    fn every_setter_lands_on_its_field() {
        let handles = builders();
        let (cfg, l, g, p) = handles;
        // SAFETY: every handle is live until `drop_all`.
        unsafe {
            assert_eq!(weida_limits_set_max_header_bytes(l, 1), 0);
            assert_eq!(weida_limits_set_max_concurrent_uni_streams(l, 2), 0);
            assert_eq!(weida_limits_set_max_concurrent_bidi_streams(l, 3), 0);
            assert_eq!(weida_limits_set_stream_receive_window(l, 4), 0);
            assert_eq!(weida_limits_set_connection_receive_window(l, 5), 0);
            assert_eq!(weida_limits_set_keep_alive_ms(l, 6), 0);
            assert_eq!(weida_limits_set_idle_timeout_ms(l, 7), 0);
            assert_eq!(weida_limits_set_hello_timeout_ms(l, 8), 0);
            assert_eq!(weida_limits_set_max_subscriptions(l, 9), 0);
            assert_eq!(weida_limits_set_subscriber_buffer_bytes(l, 10), 0);
            assert_eq!(weida_limits_set_max_sequence_scopes(l, 11), 0);
            assert_eq!(weida_limits_set_max_reorder_hold(l, 12), 0);
            assert_eq!(weida_limits_set_max_local_streams(l, 13), 0);
            assert_eq!(weida_limits_set_max_parked_reverse(l, 14), 0);
            assert_eq!(weida_limits_set_max_dedup_entries(l, 15), 0);
            assert_eq!(weida_limits_set_datagram_receive_bytes(l, 16), 0);
            assert_eq!(weida_limits_set_datagram_send_bytes(l, 17), 0);
            assert_eq!(weida_limits_set_path_report(l, 1), 0);
            assert_eq!(weida_limits_set_max_flows(l, 19), 0);
            assert_eq!(weida_limits_set_flow_queue_bytes(l, 20), 0);
            assert_eq!(weida_limits_set_flow_early_bytes(l, 21), 0);
            assert_eq!(weida_limits_set_flow_early_hold_ms(l, 22), 0);
            assert_eq!(weida_limits_set_congestion(l, WEIDA_CONGESTION_BBR), 0);
            assert_eq!(weida_limits_set_header_timeout_ms(l, 24), 0);
            assert_eq!(weida_limits_set_max_header_streams(l, 25), 0);

            let delivery = WEIDA_DELIVERY_LEVEL_AT_LEAST_ONCE;
            assert_eq!(weida_guarantee_set_set_delivery(g, delivery), 0);
            let replicated = WEIDA_ACKNOWLEDGEMENT_REPLICATED;
            assert_eq!(weida_guarantee_set_set_acknowledgement(g, replicated), 0);
            assert_eq!(
                weida_guarantee_set_set_durability(g, WEIDA_DURABILITY_FLUSHED),
                0
            );
            assert_eq!(weida_guarantee_set_set_replicas(g, 3), 0);
            let detect = WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT;
            assert_eq!(weida_guarantee_set_set_ordering(g, detect), 0);
            let bounded = WEIDA_DEDUPLICATION_BOUNDED;
            assert_eq!(weida_guarantee_set_set_deduplication(g, bounded), 0);
            assert_eq!(weida_guarantee_set_set_dedup_window_ms(g, 5000), 0);
            let drop_level = WEIDA_BACKPRESSURE_DROP;
            assert_eq!(weida_guarantee_set_set_backpressure(g, drop_level), 0);
            let stable = WEIDA_PRODUCER_NAMING_STABLE;
            assert_eq!(weida_guarantee_set_set_producer_naming(g, stable), 0);
            assert_eq!(weida_guarantee_set_set_control_isolated(g, 1), 0);

            assert_eq!(weida_reconnect_policy_set_initial_ms(p, 50), 0);
            assert_eq!(weida_reconnect_policy_set_max_ms(p, 900), 0);
            assert_eq!(weida_reconnect_policy_set_jitter(p, 0), 0);
            assert_eq!(weida_reconnect_policy_set_max_attempts(p, 7), 0);
            assert_eq!(weida_reconnect_policy_set_stop_on_peer_closed(p, 1), 0);

            assert_eq!(weida_runtime_config_set_worker_threads(cfg, 3), 0);
            assert_eq!(weida_runtime_config_set_shutdown_timeout_ms(cfg, 2500), 0);
            assert_eq!(weida_runtime_config_set_max_connections(cfg, 101), 0);
            assert_eq!(
                weida_runtime_config_set_max_connections_per_peer(cfg, 102),
                0
            );
            assert_eq!(weida_runtime_config_set_endpoint_queue(cfg, 103), 0);
            assert_eq!(weida_runtime_config_set_max_resolved_addresses(cfg, 104), 0);
            assert_eq!(
                weida_runtime_config_set_connect_attempt_timeout_ms(cfg, 105),
                0
            );
            assert_eq!(weida_runtime_config_set_send_timeout_ms(cfg, 106), 0);
            assert_eq!(weida_runtime_config_set_outbox_messages(cfg, 107), 0);
            assert_eq!(weida_runtime_config_set_outbox_bytes(cfg, 108), 0);
            let reject = WEIDA_OUTBOX_FULL_REJECT;
            assert_eq!(weida_runtime_config_set_outbox_full(cfg, reject), 0);
            assert_eq!(weida_runtime_config_set_max_handshakes(cfg, 109), 0);
            assert_eq!(weida_runtime_config_set_retry_above_handshakes(cfg, 110), 0);
            assert_eq!(
                weida_runtime_config_set_discovery(cfg, WEIDA_DISCOVERY_SINGLE),
                0
            );
            assert_eq!(weida_runtime_config_set_limits(cfg, l), 0);
            assert_eq!(weida_runtime_config_set_guarantees(cfg, g), 0);
            assert_eq!(weida_runtime_config_set_reconnect(cfg, p), 0);
        }

        // SAFETY: `cfg` is live until `drop_all`.
        let c = unsafe { &*cfg }.config.lock().unwrap().clone();
        assert_eq!(
            c.limits,
            Limits {
                max_header_bytes: 1,
                max_concurrent_uni_streams: 2,
                max_concurrent_bidi_streams: 3,
                stream_receive_window: 4,
                connection_receive_window: 5,
                keep_alive: Duration::from_millis(6),
                idle_timeout: Duration::from_millis(7),
                hello_timeout_ms: 8,
                max_subscriptions: 9,
                subscriber_buffer_bytes: 10,
                max_sequence_scopes: 11,
                max_reorder_hold: 12,
                max_local_streams: 13,
                max_parked_reverse: 14,
                max_dedup_entries: 15,
                datagram_receive_bytes: 16,
                datagram_send_bytes: 17,
                path_report: true,
                max_flows: 19,
                flow_queue_bytes: 20,
                flow_early_bytes: 21,
                flow_early_hold: Duration::from_millis(22),
                congestion: Congestion::Bbr,
                header_timeout_ms: 24,
                max_header_streams: 25,
            }
        );
        assert_eq!(
            c.guarantees,
            GuaranteeSet {
                delivery: DeliveryLevel::AtLeastOnce,
                acknowledgement: Acknowledgement::Replicated,
                durability: Some(Durability::Flushed),
                replicas: Some(3),
                ordering: OrderingMode::PerProducerDetect,
                deduplication: Deduplication::Bounded,
                dedup_window_ms: Some(5000),
                backpressure: Backpressure::Drop,
                producer_naming: ProducerNaming::Stable,
                control_isolated: true,
            }
        );
        assert_eq!(
            c.reconnect,
            ReconnectPolicy {
                initial: Duration::from_millis(50),
                max: Duration::from_millis(900),
                jitter: false,
                max_attempts: Some(7),
                stop_on_peer_closed: true,
            }
        );
        assert_eq!(c.worker_threads, 3);
        assert_eq!(c.shutdown_timeout, Duration::from_millis(2500));
        assert_eq!(c.max_connections, 101);
        assert_eq!(c.max_connections_per_peer, 102);
        assert_eq!(c.endpoint_queue, 103);
        assert_eq!(c.max_resolved_addresses, 104);
        assert_eq!(c.connect_attempt_timeout, Duration::from_millis(105));
        assert_eq!(c.send_timeout, Some(Duration::from_millis(106)));
        assert_eq!(c.outbox_messages, 107);
        assert_eq!(c.outbox_bytes, 108);
        assert_eq!(c.outbox_full, OutboxFull::Reject);
        assert_eq!(c.max_handshakes, 109);
        assert_eq!(c.retry_above_handshakes, 110);
        assert_eq!(c.discovery, Discovery::Single);
        assert_eq!(
            WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES as usize,
            weida_rs::DEFAULT_DATAGRAM_RECEIVE_BYTES
        );

        // NULL restores each nested default, and a negative send timeout is
        // none.
        // SAFETY: `cfg` is live until `drop_all`.
        unsafe {
            assert_eq!(weida_runtime_config_set_limits(cfg, ptr::null()), 0);
            assert_eq!(weida_runtime_config_set_guarantees(cfg, ptr::null()), 0);
            assert_eq!(weida_runtime_config_set_reconnect(cfg, ptr::null()), 0);
            assert_eq!(weida_runtime_config_set_send_timeout_ms(cfg, -1), 0);
        }
        // SAFETY: `cfg` is live until `drop_all`.
        let c = unsafe { &*cfg }.config.lock().unwrap().clone();
        assert_eq!(c.limits, Limits::default());
        assert_eq!(c.guarantees, GuaranteeSet::CORE);
        assert_eq!(c.reconnect, ReconnectPolicy::default());
        assert_eq!(c.send_timeout, None);
        drop_all(handles);
    }

    #[test]
    fn out_of_range_arguments_are_invalid() {
        let handles = builders();
        let (cfg, l, g, p) = handles;
        // SAFETY: every handle is live until `drop_all`.
        unsafe {
            assert_eq!(
                weida_runtime_config_set_endpoint_queue(cfg, 0),
                -WEIDA_EINVAL
            );
            assert_eq!(weida_runtime_config_set_outbox_full(cfg, 3), -WEIDA_EINVAL);
            assert_eq!(weida_runtime_config_set_discovery(cfg, -1), -WEIDA_EINVAL);
            assert_eq!(weida_limits_set_congestion(l, 99), -WEIDA_EINVAL);
            assert_eq!(weida_guarantee_set_set_ordering(g, 5), -WEIDA_EINVAL);
            assert_eq!(
                weida_reconnect_policy_set_max_attempts(p, 1 << 32),
                -WEIDA_EINVAL
            );
            assert_eq!(weida_guarantee_set_set_durability(g, -1), 0);
        }
        // SAFETY: the handles are live until `drop_all`.
        let (c, g, p) = unsafe { (&*cfg, &*g, &*p) };
        let c = c.config.lock().unwrap().clone();
        assert_eq!(c.endpoint_queue, RuntimeConfig::default().endpoint_queue);
        assert_eq!(c.outbox_full, OutboxFull::Block);
        assert_eq!(g.get(), GuaranteeSet::CORE);
        assert_eq!(p.get(), ReconnectPolicy::default());
        assert_eq!(g.get().durability, None);
        drop_all(handles);
    }
}
