//! Identity and trust sources and their events (docs/c/identity.md).

use std::ffi::{c_char, c_int};
use std::pin::Pin;
use std::ptr;
use std::task::Poll;

use weida_rs::{
    FilesOptions, IdentityEvent, IdentityEvents, IdentitySource, TrustSource, WeakIdentitySource,
    WeakTrustSource,
};

use crate::config::{Builder, WEIDA_FINGERPRINT_LEN, weida_files_options};
use crate::error::{WEIDA_EINVAL, fail, fail_abi, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{CText, borrow, cstr, into_handle, out_ptr, release, text_out, write_out};
use crate::tls::{identity_handle, weida_identity, weida_trust};
use crate::try_handle::Tried;

/// `IdentityEvent::Loaded`.
pub const WEIDA_IDENTITY_EVENT_LOADED: c_int = 0;
/// `IdentityEvent::Renewed`.
pub const WEIDA_IDENTITY_EVENT_RENEWED: c_int = 1;
/// `IdentityEvent::KeyChanged`.
pub const WEIDA_IDENTITY_EVENT_KEY_CHANGED: c_int = 2;
/// `IdentityEvent::RenewalFailed`.
pub const WEIDA_IDENTITY_EVENT_RENEWAL_FAILED: c_int = 3;
/// `IdentityEvent::HandoffStolen`.
pub const WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN: c_int = 4;
/// `IdentityEvent::TrustRefreshed`.
pub const WEIDA_IDENTITY_EVENT_TRUST_REFRESHED: c_int = 5;
/// `IdentityEvent::Missed`.
pub const WEIDA_IDENTITY_EVENT_MISSED: c_int = 6;

/// Where an identity comes from (`IdentitySource`); shared, never copied.
pub struct weida_identity_source {
    pub(crate) source: IdentitySource,
}

/// A handle to an identity source that does not keep it alive
/// (`WeakIdentitySource`).
pub struct weida_weak_identity_source {
    weak: WeakIdentitySource,
}

/// Whom a peer is accepted as, as a source (`TrustSource`); shared, never
/// copied.
pub struct weida_trust_source {
    pub(crate) source: TrustSource,
}

/// A handle to a trust source that does not keep it alive
/// (`WeakTrustSource`).
pub struct weida_weak_trust_source {
    weak: WeakTrustSource,
}

/// Writes a new handle to `out` and returns 0.
///
/// # Safety
///
/// `out` passed `out_ptr`.
unsafe fn hand_out<T>(out: *mut *mut T, value: T) -> Result<c_int, c_int> {
    // SAFETY: forwarded from this function's contract.
    unsafe { write_out(out, into_handle(value)) };
    Ok(0)
}

// --- weida_identity_source -------------------------------------------------

/// A source that holds `id` and never changes
/// (`IdentitySource::from_identity`). The identity is copied.
///
/// Fails with `-WEIDA_ETLS` when the material does not read or parse: a
/// wrong path is caught here, not at a handshake.
///
/// # Safety
///
/// `id` is NULL or a live identity; `out` is NULL or writable. Release the
/// result with `weida_identity_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_from_identity(
    id: *const weida_identity,
    out: *mut *mut weida_identity_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let id = unsafe { borrow(id) }?;
        let source = IdentitySource::from_identity(id.identity.clone()).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_identity_source { source }) }
    })
}

/// A self-signed identity generated in memory for `names`, gone with the
/// process (`IdentitySource::ephemeral`).
///
/// Fails with `-WEIDA_EINVAL` when `names` is NULL with `n > 0`, an element
/// is NULL or a name is not UTF-8, and with `-WEIDA_ETLS` when a name is
/// refused for the certificate.
///
/// # Safety
///
/// `names` is NULL only with `n == 0`, else points to `n` readable
/// pointers, each NULL or a NUL-terminated string; `out` is NULL or
/// writable. Release the result with `weida_identity_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_ephemeral(
    names: *const *const c_char,
    n: usize,
    out: *mut *mut weida_identity_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let names = unsafe { crate::handle::names(names, n) }?;
        let source = IdentitySource::ephemeral(names).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_identity_source { source }) }
    })
}

/// A source driven from outside: starts as `initial` and changes on
/// `weida_identity_source_update` (`IdentitySource::external`). The
/// identity is copied.
///
/// Fails with `-WEIDA_ETLS` when the material does not read or parse.
///
/// # Safety
///
/// `initial` is NULL or a live identity; `out` is NULL or writable. Release
/// the result with `weida_identity_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_external(
    initial: *const weida_identity,
    out: *mut *mut weida_identity_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let initial = unsafe { borrow(initial) }?;
        let source = IdentitySource::external(initial.identity.clone()).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_identity_source { source }) }
    })
}

/// A directory holding `cert.pem` and `key.pem`, reloaded when either
/// changes (`IdentitySource::files`); NULL `options` is the default.
///
/// When neither file exists, a self-signed identity for the options' names
/// is written there first (the directory `0700`, both files `0600` on
/// Unix), so a second start finds the same key. Replace the files
/// atomically, or the certificate first and the key second; the next use
/// of the identity after the poll interval picks them up.
///
/// Fails with `-WEIDA_ETLS` when one file exists without the other or the
/// material does not parse, and with `-WEIDA_EIO` when the bootstrap could
/// not create or write.
///
/// # Safety
///
/// `dir` is NULL or a NUL-terminated string; `options` is NULL or live
/// options; `out` is NULL or writable. Release the result with
/// `weida_identity_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_files(
    dir: *const c_char,
    options: *const weida_files_options,
    out: *mut *mut weida_identity_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let dir = unsafe { cstr(dir) }?;
        let options = if options.is_null() {
            FilesOptions::default()
        } else {
            // SAFETY: forwarded from this function's contract.
            unsafe { borrow(options) }?.get()
        };
        let source = IdentitySource::files(dir, options).map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_identity_source { source }) }
    })
}

/// Writes the identity of the moment to `out` as a new identity
/// (`IdentitySource::current`). A file source runs its due check here; its
/// identity names the two files, read at each use as with
/// `weida_identity_from_pem_files`.
///
/// # Safety
///
/// `src` is NULL or a live source; `out` is NULL or writable. Release the
/// result with `weida_identity_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_current(
    src: *const weida_identity_source,
    out: *mut *mut weida_identity,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let src = unsafe { borrow(src) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, identity_handle(src.source.current())) };
        Ok(0)
    })
}

/// Writes the current key's fingerprint (`IdentitySource::fingerprint`) to
/// `out`: `WEIDA_FINGERPRINT_LEN` bytes and a NUL, into the caller's buffer
/// since it changes with the source. A NULL `out` or a `len` below
/// `WEIDA_FINGERPRINT_LEN + 1` is `-WEIDA_EINVAL`. A file source runs its
/// due check here.
///
/// Fails with `-WEIDA_ETLS` only for a static source whose material does
/// not read.
///
/// # Safety
///
/// `src` is NULL or a live source; `out` is NULL or points to `len`
/// writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_fingerprint(
    src: *const weida_identity_source,
    out: *mut c_char,
    len: usize,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let src = unsafe { borrow(src) }?;
        let need = WEIDA_FINGERPRINT_LEN as usize;
        if len <= need {
            return Err(fail_abi(
                WEIDA_EINVAL,
                c"the buffer holds fewer than WEIDA_FINGERPRINT_LEN + 1 bytes",
            ));
        }
        let text = src.source.fingerprint().map_err(fail)?.to_string();
        debug_assert_eq!(text.len(), need);
        // SAFETY: `out` passed `out_ptr` and holds `len > need` bytes by this
        // function's contract; the text is `need` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr().cast::<c_char>(), out, need);
            out.add(need).write(0);
        }
        Ok(0)
    })
}

/// Replaces the identity (`IdentitySource::update`); the identity is
/// copied. Reported as `WEIDA_IDENTITY_EVENT_RENEWED` when the key is
/// unchanged and `WEIDA_IDENTITY_EVENT_KEY_CHANGED` when it is not.
///
/// Fails with `-WEIDA_ETLS` when the material does not read, which changes
/// nothing and is reported as `WEIDA_IDENTITY_EVENT_RENEWAL_FAILED`.
///
/// # Safety
///
/// `src` is NULL or a live source; `id` is NULL or a live identity.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_update(
    src: *const weida_identity_source,
    id: *const weida_identity,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let src = unsafe { borrow(src) }?;
        // SAFETY: forwarded from this function's contract.
        let id = unsafe { borrow(id) }?;
        src.source.update(id.identity.clone()).map_err(fail)?;
        Ok(0)
    })
}

/// Reports a failed renewal that never produced material
/// (`IdentitySource::report_failure`): `WEIDA_IDENTITY_EVENT_RENEWAL_FAILED`
/// with `reason`. The identity in service is unchanged.
///
/// # Safety
///
/// `src` is NULL or a live source; `reason` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_report_failure(
    src: *const weida_identity_source,
    reason: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let src = unsafe { borrow(src) }?;
        // SAFETY: forwarded from this function's contract.
        let reason = unsafe { cstr(reason) }?;
        src.source.report_failure(reason);
        Ok(0)
    })
}

/// Reports that this process's hand-off credential had already been
/// redeemed (`IdentitySource::report_handoff_stolen`):
/// `WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN`. The identity in service is
/// unchanged; the caller is expected not to continue.
///
/// # Safety
///
/// `src` is NULL or a live source.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_report_handoff_stolen(
    src: *const weida_identity_source,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        unsafe { borrow(src) }?.source.report_handoff_stolen();
        Ok(0)
    })
}

/// Reloads a file source now, regardless of the poll interval
/// (`IdentitySource::reload`): 1 when the files changed and were loaded, 0
/// when nothing changed or the source is not a file source, `-WEIDA_ETLS`
/// or `-WEIDA_EIO` when they changed and could not be loaded (also reported
/// as `WEIDA_IDENTITY_EVENT_RENEWAL_FAILED`; the identity in service
/// stays).
///
/// # Safety
///
/// `src` is NULL or a live source.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_reload(src: *const weida_identity_source) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let src = unsafe { borrow(src) }?;
        Ok(c_int::from(src.source.reload().map_err(fail)?))
    })
}

/// Writes a new reader of the source's events to `out`
/// (`IdentitySource::events`): an independent reader that sees every event
/// from this call on.
///
/// # Safety
///
/// `src` is NULL or a live source; `out` is NULL or writable. Release the
/// result with `weida_identity_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_events(
    src: *const weida_identity_source,
    out: *mut *mut weida_identity_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let src = unsafe { borrow(src) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_identity_events::handle(src.source.events())) };
        Ok(0)
    })
}

/// Writes a weak handle to the source to `out`
/// (`IdentitySource::downgrade`): one that does not keep it alive, for a
/// renewal thread that must stop once the application drops the source.
///
/// # Safety
///
/// `src` is NULL or a live source; `out` is NULL or writable. Release the
/// result with `weida_weak_identity_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_downgrade(
    src: *const weida_identity_source,
    out: *mut *mut weida_weak_identity_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let weak = unsafe { borrow(src) }?.source.downgrade();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_weak_identity_source { weak }) }
    })
}

/// Releases a source handle. The source lives on in every terms, binding
/// and requester that holds it. NULL does nothing.
///
/// # Safety
///
/// `src` is NULL or a live source, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_source_drop(src: *mut weida_identity_source) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(src) }));
}

/// Writes the source to `out` as a new handle and returns 1 while anything
/// still holds it (`WeakIdentitySource::upgrade`); writes NULL and returns
/// 0 once nothing does.
///
/// # Safety
///
/// `w` is NULL or a live weak handle; `out` is NULL or writable. Release a
/// result with `weida_identity_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_weak_identity_source_upgrade(
    w: *const weida_weak_identity_source,
    out: *mut *mut weida_identity_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let w = unsafe { borrow(w) }?;
        let (handle, rc) = match w.weak.upgrade() {
            Some(source) => (into_handle(weida_identity_source { source }), 1),
            None => (ptr::null_mut(), 0),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(rc)
    })
}

/// Releases a weak handle. NULL does nothing.
///
/// # Safety
///
/// `w` is NULL or a live weak handle, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_weak_identity_source_drop(w: *mut weida_weak_identity_source) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(w) }));
}

// --- weida_trust_source ----------------------------------------------------

/// A source that holds `t` and never changes (`TrustSource::from_trust`).
/// The trust is copied.
///
/// # Safety
///
/// `t` is NULL or a live trust; `out` is NULL or writable. Release the
/// result with `weida_trust_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_from_trust(
    t: *const weida_trust,
    out: *mut *mut weida_trust_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let source = TrustSource::from_trust(unsafe { borrow(t) }?.get());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_trust_source { source }) }
    })
}

/// A source driven from outside: starts as `initial` and changes on
/// `weida_trust_source_update` (`TrustSource::external`). The trust is
/// copied.
///
/// # Safety
///
/// `initial` is NULL or a live trust; `out` is NULL or writable. Release
/// the result with `weida_trust_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_external(
    initial: *const weida_trust,
    out: *mut *mut weida_trust_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let source = TrustSource::external(unsafe { borrow(initial) }?.get());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_trust_source { source }) }
    })
}

/// Writes the trust of the moment to `out` as a new trust
/// (`TrustSource::current`): a copy; changing it does not change the
/// source.
///
/// # Safety
///
/// `ts` is NULL or a live trust source; `out` is NULL or writable. Release
/// the result with `weida_trust_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_current(
    ts: *const weida_trust_source,
    out: *mut *mut weida_trust,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let trust = unsafe { borrow(ts) }?.source.current();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_trust::handle(trust)) };
        Ok(0)
    })
}

/// Replaces the trust (`TrustSource::update`); the trust is copied.
/// Reported as `WEIDA_IDENTITY_EVENT_TRUST_REFRESHED`; reaches every
/// binding and requester holding the source at its next handshake.
///
/// # Safety
///
/// `ts` is NULL or a live trust source; `t` is NULL or a live trust.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_update(
    ts: *const weida_trust_source,
    t: *const weida_trust,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let ts = unsafe { borrow(ts) }?;
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        ts.source.update(t.get());
        Ok(0)
    })
}

/// Reports a failed refresh (`TrustSource::report_failure`):
/// `WEIDA_IDENTITY_EVENT_RENEWAL_FAILED` with `reason`. The trust in
/// service is unchanged.
///
/// # Safety
///
/// `ts` is NULL or a live trust source; `reason` is NULL or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_report_failure(
    ts: *const weida_trust_source,
    reason: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let ts = unsafe { borrow(ts) }?;
        // SAFETY: forwarded from this function's contract.
        let reason = unsafe { cstr(reason) }?;
        ts.source.report_failure(reason);
        Ok(0)
    })
}

/// 1 when the trust of the moment holds no pin and no anchor
/// (`TrustSource::is_empty`); 0 otherwise; `-WEIDA_EINVAL` for NULL.
///
/// # Safety
///
/// `ts` is NULL or a live trust source.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_is_empty(ts: *const weida_trust_source) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| Ok(c_int::from(unsafe { borrow(ts) }?.source.is_empty())))
}

/// Writes a new reader of the source's events to `out`
/// (`TrustSource::events`), as `weida_identity_source_events`.
///
/// # Safety
///
/// `ts` is NULL or a live trust source; `out` is NULL or writable. Release
/// the result with `weida_identity_events_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_events(
    ts: *const weida_trust_source,
    out: *mut *mut weida_identity_events,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let ts = unsafe { borrow(ts) }?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, weida_identity_events::handle(ts.source.events())) };
        Ok(0)
    })
}

/// Writes a weak handle to the source to `out` (`TrustSource::downgrade`).
///
/// # Safety
///
/// `ts` is NULL or a live trust source; `out` is NULL or writable. Release
/// the result with `weida_weak_trust_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_downgrade(
    ts: *const weida_trust_source,
    out: *mut *mut weida_weak_trust_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let weak = unsafe { borrow(ts) }?.source.downgrade();
        // SAFETY: `out` passed `out_ptr`.
        unsafe { hand_out(out, weida_weak_trust_source { weak }) }
    })
}

/// Releases a trust source handle. The source lives on in every terms,
/// binding and requester that holds it. NULL does nothing.
///
/// # Safety
///
/// `ts` is NULL or a live trust source, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_source_drop(ts: *mut weida_trust_source) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(ts) }));
}

/// Writes the source to `out` as a new handle and returns 1 while anything
/// still holds it (`WeakTrustSource::upgrade`); writes NULL and returns 0
/// once nothing does.
///
/// # Safety
///
/// `w` is NULL or a live weak handle; `out` is NULL or writable. Release a
/// result with `weida_trust_source_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_weak_trust_source_upgrade(
    w: *const weida_weak_trust_source,
    out: *mut *mut weida_trust_source,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let w = unsafe { borrow(w) }?;
        let (handle, rc) = match w.weak.upgrade() {
            Some(source) => (into_handle(weida_trust_source { source }), 1),
            None => (ptr::null_mut(), 0),
        };
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(rc)
    })
}

/// Releases a weak handle. NULL does nothing.
///
/// # Safety
///
/// `w` is NULL or a live weak handle, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_weak_trust_source_drop(w: *mut weida_weak_trust_source) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(w) }));
}

// --- weida_identity_events -------------------------------------------------

type Receiving = Pin<Box<dyn Future<Output = (IdentityEvents, Option<IdentityEvent>)> + Send>>;

/// A reader's state between tries: the receive in flight owns the reader
/// and hands it back, so the future is kept, never recreated, and a
/// timeout or `EINTR` loses nothing (R3a).
struct EventsState {
    events: Option<IdentityEvents>,
    receiving: Option<Receiving>,
}

/// One reader of a source's events (`IdentityEvents`).
pub struct weida_identity_events {
    tried: Tried<EventsState>,
}

impl weida_identity_events {
    fn handle(events: IdentityEvents) -> *mut weida_identity_events {
        into_handle(weida_identity_events {
            tried: Tried::new(EventsState {
                events: Some(events),
                receiving: None,
            }),
        })
    }
}

/// The reader's readiness descriptor (R11): readable when the next
/// `weida_identity_events_recv` will not block.
///
/// # Safety
///
/// `e` is NULL or a live reader; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_events_fd(
    e: *const weida_identity_events,
    out: *mut i64,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let e = unsafe { borrow(e) }?;
        let fd = e.tried.fd()?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, fd) };
        Ok(0)
    })
}

/// Takes the next event, waiting up to `timeout_ms` (R4) for one
/// (`IdentityEvents::recv`).
///
/// Writes NULL to `out` and returns 0 once the source is gone: its last
/// strong handle, C or Rust, was dropped. A reader that fell more than 64
/// events behind gets one `WEIDA_IDENTITY_EVENT_MISSED` event carrying how
/// many it lost.
///
/// # Safety
///
/// `e` is NULL or a live reader; `out` is NULL or writable. Release an
/// event with `weida_identity_event_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_events_recv(
    e: *const weida_identity_events,
    timeout_ms: i64,
    out: *mut *mut weida_identity_event,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let e = unsafe { borrow(e) }?;
        let event = e.tried.attempt(timeout_ms, |state, cx| {
            let receiving = state.receiving.get_or_insert_with(|| {
                let mut events = state
                    .events
                    .take()
                    .expect("the reader is held whenever no receive is in flight");
                Box::pin(async move {
                    let event = events.recv().await;
                    (events, event)
                })
            });
            match receiving.as_mut().poll(cx) {
                Poll::Ready((events, event)) => {
                    state.receiving = None;
                    state.events = Some(events);
                    Poll::Ready(event)
                }
                Poll::Pending => Poll::Pending,
            }
        })?;
        let handle = event.map_or(ptr::null_mut(), weida_identity_event::handle);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Releases a reader. A thread waiting in `weida_identity_events_recv`
/// returns `-WEIDA_ECLOSED`. NULL does nothing.
///
/// # Safety
///
/// `e` is NULL or a live reader, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_events_drop(e: *mut weida_identity_events) {
    guard_void(|| {
        // SAFETY: forwarded from this function's contract.
        if let Some(e) = unsafe { release(e) } {
            drop(e.tried.close());
        }
    });
}

// --- weida_identity_event --------------------------------------------------

/// One transition of a source (`IdentityEvent`), read-only.
pub struct weida_identity_event {
    event: IdentityEvent,
    fingerprint: Option<CText>,
    from: Option<CText>,
    to: Option<CText>,
    message: Option<CText>,
}

impl weida_identity_event {
    fn handle(event: IdentityEvent) -> *mut weida_identity_event {
        let fingerprint = match &event {
            IdentityEvent::Loaded { fingerprint } | IdentityEvent::Renewed { fingerprint } => {
                Some(CText::new(fingerprint.to_string()))
            }
            _ => None,
        };
        let (from, to) = match &event {
            IdentityEvent::KeyChanged { from, to } => (
                Some(CText::new(from.to_string())),
                Some(CText::new(to.to_string())),
            ),
            _ => (None, None),
        };
        let message = match &event {
            IdentityEvent::RenewalFailed(m) => Some(CText::new(m.as_str())),
            _ => None,
        };
        into_handle(weida_identity_event {
            event,
            fingerprint,
            from,
            to,
            message,
        })
    }
}

/// Hands one of the event's strings out (R7).
unsafe fn event_text(
    e: *const weida_identity_event,
    len_out: *mut usize,
    pick: impl FnOnce(&weida_identity_event) -> Option<&CText>,
) -> *const c_char {
    guard_ptr(|| {
        if e.is_null() {
            return ptr::null();
        }
        // SAFETY: a non-NULL `e` is a live event by the caller's contract,
        // borrowed for this call only; the text lives as long as the handle.
        let e = unsafe { &*e };
        // SAFETY: forwarded from the caller's contract.
        unsafe { text_out(pick(e), len_out) }
    })
}

/// The event's kind, a `WEIDA_IDENTITY_EVENT_*` constant.
///
/// # Safety
///
/// `e` is NULL or a live event.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_kind(e: *const weida_identity_event) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let e = unsafe { borrow(e) }?;
        Ok(match e.event {
            IdentityEvent::Loaded { .. } => WEIDA_IDENTITY_EVENT_LOADED,
            IdentityEvent::Renewed { .. } => WEIDA_IDENTITY_EVENT_RENEWED,
            IdentityEvent::KeyChanged { .. } => WEIDA_IDENTITY_EVENT_KEY_CHANGED,
            IdentityEvent::RenewalFailed(_) => WEIDA_IDENTITY_EVENT_RENEWAL_FAILED,
            IdentityEvent::HandoffStolen => WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN,
            IdentityEvent::TrustRefreshed => WEIDA_IDENTITY_EVENT_TRUST_REFRESHED,
            IdentityEvent::Missed(_) => WEIDA_IDENTITY_EVENT_MISSED,
        })
    })
}

/// `LOADED` and `RENEWED`: the key's fingerprint (`sha256:` text); NULL for
/// any other kind. Lives as long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_fingerprint(
    e: *const weida_identity_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.fingerprint.as_ref()) }
}

/// `KEY_CHANGED`: the fingerprint that was; NULL for any other kind. Lives
/// as long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_from(
    e: *const weida_identity_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.from.as_ref()) }
}

/// `KEY_CHANGED`: the fingerprint that is; NULL for any other kind. Lives
/// as long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_to(
    e: *const weida_identity_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.to.as_ref()) }
}

/// `RENEWAL_FAILED`: why; NULL for any other kind. Lives as long as `e`.
///
/// # Safety
///
/// `e` is NULL or a live event; `len_out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_message(
    e: *const weida_identity_event,
    len_out: *mut usize,
) -> *const c_char {
    // SAFETY: forwarded from this function's contract.
    unsafe { event_text(e, len_out, |e| e.message.as_ref()) }
}

/// `MISSED`: 1 and how many events this reader lost; 0 for any other kind,
/// also for a NULL `e` or `out`.
///
/// # Safety
///
/// `e` is NULL or a live event; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_missed(
    e: *const weida_identity_event,
    out: *mut u64,
) -> c_int {
    guard_or(0, || {
        if e.is_null() || out.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `e` is a live event by the caller's contract.
        let e = unsafe { &*e };
        match e.event {
            IdentityEvent::Missed(n) => {
                // SAFETY: a non-NULL `out` is writable by the caller's contract.
                unsafe { out.write(n) };
                1
            }
            _ => 0,
        }
    })
}

/// Releases an event. NULL does nothing.
///
/// # Safety
///
/// `e` is NULL or a live event, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_event_drop(e: *mut weida_identity_event) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(e) }));
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use weida_rs::{ClientTrust, Fingerprint};

    use super::*;
    use crate::config::{
        weida_files_options_drop, weida_files_options_new, weida_files_options_set_names,
        weida_files_options_set_poll_ms,
    };
    use crate::error::WEIDA_EAGAIN;
    use crate::tls::{
        weida_client_tls_drop, weida_client_tls_new_source, weida_client_tls_set_identity_source,
        weida_identity_drop, weida_identity_generate, weida_server_tls_drop,
        weida_server_tls_new_source, weida_server_tls_require_client_source, weida_trust_add_pin,
        weida_trust_drop, weida_trust_is_empty, weida_trust_new,
    };

    /// A string a getter handed out, copied.
    fn text(p: *const c_char, len: usize) -> Option<String> {
        if p.is_null() {
            return None;
        }
        // SAFETY: a non-NULL getter result points to `len` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(p.cast::<u8>(), len) };
        Some(String::from_utf8(bytes.to_vec()).expect("UTF-8"))
    }

    fn generated() -> *mut weida_identity {
        let mut id = ptr::null_mut();
        // SAFETY: `id` is writable.
        assert_eq!(unsafe { weida_identity_generate(&mut id) }, 0);
        id
    }

    /// The fingerprint text of a live identity.
    fn fp(id: *const weida_identity) -> String {
        // SAFETY: `id` is live.
        unsafe { &*id }.identity.fingerprint().unwrap().to_string()
    }

    /// A trust pinning an all-zero key, or an empty one.
    fn trust(pinned: bool) -> *mut weida_trust {
        let mut t = ptr::null_mut();
        // SAFETY: `t` is writable, then live; the pin is NUL-terminated.
        unsafe {
            assert_eq!(weida_trust_new(&mut t), 0);
            if pinned {
                let pin = format!("sha256:{}\0", "0".repeat(64));
                assert_eq!(weida_trust_add_pin(t, pin.as_ptr().cast()), 0);
            }
        }
        t
    }

    /// The next event, which must be there already.
    fn next(events: *mut weida_identity_events) -> *mut weida_identity_event {
        let mut e = ptr::null_mut();
        // SAFETY: a live reader and a writable out-parameter.
        assert_eq!(unsafe { weida_identity_events_recv(events, 0, &mut e) }, 0);
        assert!(!e.is_null(), "the event stream ended");
        e
    }

    #[test]
    fn every_identity_event_kind_reads_back_through_its_getters() {
        let a = Fingerprint::from_bytes([7; 32]);
        let b = Fingerprint::from_bytes([8; 32]);
        let mut len = 0usize;
        let mut n = 0u64;
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable.
        unsafe {
            for (event, kind) in [
                (
                    IdentityEvent::Loaded { fingerprint: a },
                    WEIDA_IDENTITY_EVENT_LOADED,
                ),
                (
                    IdentityEvent::Renewed { fingerprint: a },
                    WEIDA_IDENTITY_EVENT_RENEWED,
                ),
            ] {
                let e = weida_identity_event::handle(event);
                assert_eq!(weida_identity_event_kind(e), kind);
                assert_eq!(
                    text(weida_identity_event_fingerprint(e, &mut len), len),
                    Some(a.to_string())
                );
                assert!(weida_identity_event_from(e, &mut len).is_null());
                assert!(weida_identity_event_message(e, &mut len).is_null());
                assert_eq!(weida_identity_event_missed(e, &mut n), 0);
                weida_identity_event_drop(e);
            }

            let e = weida_identity_event::handle(IdentityEvent::KeyChanged { from: a, to: b });
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_KEY_CHANGED
            );
            assert_eq!(
                text(weida_identity_event_from(e, &mut len), len),
                Some(a.to_string())
            );
            assert_eq!(
                text(weida_identity_event_to(e, &mut len), len),
                Some(b.to_string())
            );
            assert!(weida_identity_event_fingerprint(e, &mut len).is_null());
            weida_identity_event_drop(e);

            let e = weida_identity_event::handle(IdentityEvent::RenewalFailed("denied".into()));
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_RENEWAL_FAILED
            );
            assert_eq!(
                text(weida_identity_event_message(e, &mut len), len).as_deref(),
                Some("denied")
            );
            assert!(weida_identity_event_to(e, &mut len).is_null());
            weida_identity_event_drop(e);

            for (event, kind) in [
                (
                    IdentityEvent::HandoffStolen,
                    WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN,
                ),
                (
                    IdentityEvent::TrustRefreshed,
                    WEIDA_IDENTITY_EVENT_TRUST_REFRESHED,
                ),
            ] {
                let e = weida_identity_event::handle(event);
                assert_eq!(weida_identity_event_kind(e), kind);
                assert!(weida_identity_event_fingerprint(e, &mut len).is_null());
                assert!(weida_identity_event_message(e, &mut len).is_null());
                weida_identity_event_drop(e);
            }

            let e = weida_identity_event::handle(IdentityEvent::Missed(9));
            assert_eq!(weida_identity_event_kind(e), WEIDA_IDENTITY_EVENT_MISSED);
            assert_eq!(weida_identity_event_missed(e, &mut n), 1);
            assert_eq!(n, 9);
            assert!(weida_identity_event_fingerprint(e, &mut len).is_null());
            weida_identity_event_drop(e);

            assert_eq!(weida_identity_event_kind(ptr::null()), -WEIDA_EINVAL);
            assert!(weida_identity_event_fingerprint(ptr::null(), &mut len).is_null());
            assert_eq!(weida_identity_event_missed(ptr::null(), &mut n), 0);
        }
    }

    #[test]
    fn an_external_source_reports_its_updates_through_the_queue() {
        let (a, b) = (generated(), generated());
        let (fa, fb) = (fp(a), fp(b));
        let mut src = ptr::null_mut();
        let mut events = ptr::null_mut();
        let mut buf = [0 as c_char; WEIDA_FINGERPRINT_LEN as usize + 1];
        let mut len = 0usize;
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable, every buffer as long as its length.
        unsafe {
            assert_eq!(weida_identity_source_external(a, &mut src), 0);
            assert_eq!(
                weida_identity_source_fingerprint(src, buf.as_mut_ptr(), buf.len()),
                0
            );
            assert_eq!(std::ffi::CStr::from_ptr(buf.as_ptr()).to_str().unwrap(), fa);
            assert_eq!(
                weida_identity_source_fingerprint(src, buf.as_mut_ptr(), buf.len() - 1),
                -WEIDA_EINVAL
            );

            assert_eq!(weida_identity_source_events(src, &mut events), 0);
            let mut none = ptr::null_mut();
            assert_eq!(
                weida_identity_events_recv(events, 0, &mut none),
                -WEIDA_EAGAIN
            );

            assert_eq!(weida_identity_source_update(src, a), 0);
            let e = next(events);
            assert_eq!(weida_identity_event_kind(e), WEIDA_IDENTITY_EVENT_RENEWED);
            assert_eq!(
                text(weida_identity_event_fingerprint(e, &mut len), len),
                Some(fa.clone())
            );
            weida_identity_event_drop(e);

            assert_eq!(weida_identity_source_update(src, b), 0);
            let e = next(events);
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_KEY_CHANGED
            );
            assert_eq!(text(weida_identity_event_from(e, &mut len), len), Some(fa));
            assert_eq!(
                text(weida_identity_event_to(e, &mut len), len),
                Some(fb.clone())
            );
            weida_identity_event_drop(e);

            assert_eq!(
                weida_identity_source_report_failure(src, c"denied".as_ptr()),
                0
            );
            let e = next(events);
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_RENEWAL_FAILED
            );
            assert_eq!(
                text(weida_identity_event_message(e, &mut len), len).as_deref(),
                Some("denied")
            );
            weida_identity_event_drop(e);

            assert_eq!(weida_identity_source_report_handoff_stolen(src), 0);
            let e = next(events);
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN
            );
            weida_identity_event_drop(e);

            assert_eq!(weida_identity_source_reload(src), 0);
            let mut current = ptr::null_mut();
            assert_eq!(weida_identity_source_current(src, &mut current), 0);
            assert_eq!(fp(current), fb);
            weida_identity_drop(current);

            let mut weak = ptr::null_mut();
            let mut strong = ptr::null_mut();
            assert_eq!(weida_identity_source_downgrade(src, &mut weak), 0);
            assert_eq!(weida_weak_identity_source_upgrade(weak, &mut strong), 1);
            assert!(!strong.is_null());
            weida_identity_source_drop(strong);
            weida_identity_source_drop(src);
            assert_eq!(weida_weak_identity_source_upgrade(weak, &mut strong), 0);
            assert!(strong.is_null());

            let mut end = ptr::null_mut();
            assert_eq!(weida_identity_events_recv(events, 0, &mut end), 0);
            assert!(end.is_null());

            weida_weak_identity_source_drop(weak);
            weida_identity_events_drop(events);
            weida_identity_drop(b);
            weida_identity_drop(a);
        }
    }

    #[test]
    fn a_trust_source_reports_a_refresh_and_reads_back_empty() {
        let pinned = trust(true);
        let empty = trust(false);
        let mut ts = ptr::null_mut();
        let mut events = ptr::null_mut();
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable.
        unsafe {
            assert_eq!(weida_trust_source_external(pinned, &mut ts), 0);
            assert_eq!(weida_trust_source_is_empty(ts), 0);
            assert_eq!(weida_trust_source_events(ts, &mut events), 0);

            assert_eq!(weida_trust_source_update(ts, empty), 0);
            let e = next(events);
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_TRUST_REFRESHED
            );
            weida_identity_event_drop(e);
            assert_eq!(weida_trust_source_is_empty(ts), 1);
            let mut current = ptr::null_mut();
            assert_eq!(weida_trust_source_current(ts, &mut current), 0);
            assert_eq!(weida_trust_is_empty(current), 1);
            weida_trust_drop(current);

            assert_eq!(
                weida_trust_source_report_failure(ts, c"unreachable".as_ptr()),
                0
            );
            let e = next(events);
            assert_eq!(
                weida_identity_event_kind(e),
                WEIDA_IDENTITY_EVENT_RENEWAL_FAILED
            );
            weida_identity_event_drop(e);

            let mut weak = ptr::null_mut();
            let mut strong = ptr::null_mut();
            assert_eq!(weida_trust_source_downgrade(ts, &mut weak), 0);
            assert_eq!(weida_weak_trust_source_upgrade(weak, &mut strong), 1);
            weida_trust_source_drop(strong);
            weida_trust_source_drop(ts);
            assert_eq!(weida_weak_trust_source_upgrade(weak, &mut strong), 0);
            assert!(strong.is_null());
            assert_eq!(weida_trust_source_is_empty(ptr::null()), -WEIDA_EINVAL);

            weida_weak_trust_source_drop(weak);
            weida_identity_events_drop(events);
            weida_trust_drop(empty);
            weida_trust_drop(pinned);
        }
    }

    #[test]
    fn files_options_setters_land_on_their_fields() {
        let mut o = ptr::null_mut();
        let names = [c"a".as_ptr(), c"b".as_ptr()];
        let null = [ptr::null::<c_char>()];
        // SAFETY: `o` is writable, then live until its drop; every array
        // holds as many pointers as passed.
        unsafe {
            assert_eq!(weida_files_options_new(&mut o), 0);
            assert_eq!((*o).get(), FilesOptions::default());
            assert_eq!(weida_files_options_set_names(o, names.as_ptr(), 2), 0);
            assert_eq!((*o).get().names, ["a", "b"]);
            assert_eq!(
                weida_files_options_set_names(o, null.as_ptr(), 1),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_files_options_set_names(o, ptr::null(), 1),
                -WEIDA_EINVAL
            );
            assert_eq!((*o).get().names, ["a", "b"]);
            assert_eq!(weida_files_options_set_names(o, ptr::null(), 0), 0);
            assert!((*o).get().names.is_empty());
            assert_eq!(weida_files_options_set_poll_ms(o, 0), 0);
            assert_eq!((*o).get().poll, Duration::ZERO);
            weida_files_options_drop(o);
        }
    }

    #[test]
    fn the_source_forms_share_their_source() {
        let (first, other) = (generated(), generated());
        let pinned = trust(true);
        let empty = trust(false);
        let mut ts = ptr::null_mut();
        let mut is = ptr::null_mut();
        let mut ctls = ptr::null_mut();
        let mut stls = ptr::null_mut();
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable.
        unsafe {
            assert_eq!(weida_trust_source_external(pinned, &mut ts), 0);
            assert_eq!(weida_identity_source_external(first, &mut is), 0);
            assert_eq!(weida_client_tls_new_source(ts, &mut ctls), 0);
            assert_eq!(weida_client_tls_set_identity_source(ctls, is), 0);
            assert_eq!(weida_server_tls_new_source(is, &mut stls), 0);
            assert_eq!(weida_server_tls_require_client_source(stls, ts), 0);

            assert!(!(*ctls).get().trust.is_empty());
            assert_eq!(weida_trust_source_update(ts, empty), 0);
            assert!((*ctls).get().trust.is_empty());

            assert_eq!(weida_identity_source_update(is, other), 0);
            let want = (*other).identity.fingerprint().unwrap();
            assert_eq!((*ctls).get().identity.unwrap().fingerprint().unwrap(), want);
            assert_eq!((*stls).get().identity.fingerprint().unwrap(), want);
            assert_eq!(
                (*stls).get().client_trust,
                Some(ClientTrust::Trusted((*ts).source.clone()))
            );

            weida_server_tls_drop(stls);
            weida_client_tls_drop(ctls);
            weida_identity_source_drop(is);
            weida_trust_source_drop(ts);
            weida_trust_drop(empty);
            weida_trust_drop(pinned);
            weida_identity_drop(other);
            weida_identity_drop(first);
        }
    }
}
