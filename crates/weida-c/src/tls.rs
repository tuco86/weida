//! TLS values and transfer metadata (docs/c/tls.md): builders the caller
//! fills and hands to the calls that take them, which copy them.

use std::ffi::{c_char, c_int};
use std::sync::{Mutex, MutexGuard, OnceLock};

use weida_rs::{ClientTls, Fingerprint, Identity, ServerTls, TransferMeta, Trust};

use crate::error::{fail, guard, guard_void};
use crate::handle::{CText, borrow, bytes, cstr, into_handle, out_ptr, release, write_out};

fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whom a dialler trusts: pins added to what each address names.
pub struct weida_trust {
    trust: Mutex<Trust>,
}

/// A certificate chain and its private key.
pub struct weida_identity {
    pub(crate) identity: Identity,
    fingerprint: OnceLock<CText>,
}

/// The TLS terms a requester dials on.
pub struct weida_client_tls {
    pub(crate) tls: ClientTls,
}

/// The TLS terms a QUIC binding serves on.
pub struct weida_server_tls {
    pub(crate) tls: ServerTls,
}

/// The metadata an outgoing transfer carries in its header.
pub struct weida_transfer_meta {
    meta: Mutex<TransferMeta>,
}

impl weida_transfer_meta {
    pub(crate) fn get(&self) -> TransferMeta {
        locked(&self.meta).clone()
    }
}

/// Creates a trust that accepts only what each dialled address names
/// (`Trust::by_address`): a `weida://sha256:…@host/path` URL pins its key.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with `weida_trust_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_new(out: *mut *mut weida_trust) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let handle = into_handle(weida_trust {
            trust: Mutex::new(Trust::by_address()),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Adds a pinned fingerprint (`Trust::and_pin`): `sha256:` plus 64 hex
/// digits. Fails with `-WEIDA_EINVALID_FINGERPRINT` on anything else.
///
/// # Safety
///
/// `t` is NULL or a live trust; `fingerprint` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_add_pin(
    t: *const weida_trust,
    fingerprint: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        // SAFETY: forwarded from this function's contract.
        let text = unsafe { cstr(fingerprint) }?;
        let fp: Fingerprint = text.parse().map_err(fail)?;
        let mut trust = locked(&t.trust);
        *trust = std::mem::take(&mut *trust).and_pin(fp);
        Ok(0)
    })
}

/// Releases a trust. NULL does nothing.
///
/// # Safety
///
/// `t` is NULL or a live trust, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_drop(t: *mut weida_trust) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(t) }));
}

fn identity_handle(identity: Identity) -> *mut weida_identity {
    into_handle(weida_identity {
        identity,
        fingerprint: OnceLock::new(),
    })
}

/// Generates a self-signed identity with a fresh key, held in memory
/// (`Identity::generate`): made for pinning by fingerprint.
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_identity_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_generate(out: *mut *mut weida_identity) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let identity = Identity::generate().map_err(fail)?;
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, identity_handle(identity)) };
        Ok(0)
    })
}

/// An identity from PEM bytes: the certificate chain, leaf first, and the
/// private key (`Identity::from_pem`). Both are copied; the material is
/// checked when it is first used.
///
/// # Safety
///
/// `cert` and `key` are NULL only with a zero length, else point to that
/// many readable bytes; `out` is NULL or writable. Release the result with
/// `weida_identity_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_from_pem(
    cert: *const u8,
    cert_len: usize,
    key: *const u8,
    key_len: usize,
    out: *mut *mut weida_identity,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let cert = unsafe { bytes(cert, cert_len) }?;
        // SAFETY: forwarded from this function's contract.
        let key = unsafe { bytes(key, key_len) }?;
        let identity = Identity::from_pem(cert.to_vec(), key.to_vec());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, identity_handle(identity)) };
        Ok(0)
    })
}

/// Writes the identity's public-key fingerprint, `sha256:` text, to `out`.
/// The string lives as long as the identity.
///
/// Fails with `-WEIDA_ETLS` when the certificate does not parse.
///
/// # Safety
///
/// `id` is NULL or a live identity; `out` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_fingerprint(
    id: *const weida_identity,
    out: *mut *const c_char,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let handle = unsafe { borrow(id) }?;
        let text = match handle.fingerprint.get() {
            Some(text) => text,
            None => {
                let fp = handle.identity.fingerprint().map_err(fail)?;
                handle
                    .fingerprint
                    .get_or_init(|| CText::new(fp.to_string()))
            }
        };
        // The text lives in the identity, which the caller keeps alive.
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, text.as_ptr()) };
        Ok(0)
    })
}

/// Releases an identity. NULL does nothing.
///
/// # Safety
///
/// `id` is NULL or a live identity, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_drop(id: *mut weida_identity) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(id) }));
}

/// Client terms that dial anonymously under `trust` (`ClientTls::new`).
/// The trust is copied.
///
/// # Safety
///
/// `trust` is NULL or a live trust; `out` is NULL or writable. Release the
/// result with `weida_client_tls_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_client_tls_new(
    trust: *const weida_trust,
    out: *mut *mut weida_client_tls,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let trust = unsafe { borrow(trust) }?;
        let tls = ClientTls::new(locked(&trust.trust).clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_client_tls { tls })) };
        Ok(0)
    })
}

/// Releases client terms. NULL does nothing.
///
/// # Safety
///
/// `tls` is NULL or live client terms, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_client_tls_drop(tls: *mut weida_client_tls) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(tls) }));
}

/// Server terms that present `identity` and accept anonymous clients
/// (`ServerTls::new`). The identity is copied.
///
/// # Safety
///
/// `identity` is NULL or a live identity; `out` is NULL or writable.
/// Release the result with `weida_server_tls_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_new(
    identity: *const weida_identity,
    out: *mut *mut weida_server_tls,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let identity = unsafe { borrow(identity) }?;
        let tls = ServerTls::new(identity.identity.clone());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_server_tls { tls })) };
        Ok(0)
    })
}

/// Releases server terms. NULL does nothing.
///
/// # Safety
///
/// `tls` is NULL or live server terms, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_drop(tls: *mut weida_server_tls) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(tls) }));
}

/// Creates empty transfer metadata (`TransferMeta::default`).
///
/// # Safety
///
/// `out` is NULL or writable. Release the result with
/// `weida_transfer_meta_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_new(out: *mut *mut weida_transfer_meta) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let handle = into_handle(weida_transfer_meta {
            meta: Mutex::new(TransferMeta::default()),
        });
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, handle) };
        Ok(0)
    })
}

/// Sets the opaque content type label (`TransferMeta::with_content_type`).
///
/// # Safety
///
/// `m` is NULL or live metadata; `content_type` is NULL or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_content_type(
    m: *const weida_transfer_meta,
    content_type: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        // SAFETY: forwarded from this function's contract.
        let content_type = unsafe { cstr(content_type) }?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_content_type(content_type);
        Ok(0)
    })
}

/// Releases transfer metadata. NULL does nothing.
///
/// # Safety
///
/// `m` is NULL or live metadata, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_drop(m: *mut weida_transfer_meta) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(m) }));
}
