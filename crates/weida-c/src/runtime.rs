//! The runtime and its configuration (docs/c/runtime.md, R13).

use std::ffi::c_int;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use weida_rs::{Runtime, RuntimeConfig};

use crate::error::{guard, guard_void};
use crate::handle::{borrow, into_handle, out_ptr, release, write_out};
use crate::op::Op;

/// A runtime configuration under construction: one setter per field.
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
