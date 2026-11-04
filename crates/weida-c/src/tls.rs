//! TLS values and transfer metadata (docs/c/tls.md): builders the caller
//! fills and hands to the calls that take them, which copy them.

use std::ffi::{c_char, c_int};
use std::sync::{Mutex, MutexGuard, OnceLock};

use weida_rs::{
    Acknowledgement, ClientTls, ClientTrust, Fingerprint, Identity, ReportMode, ServerTls,
    TraceContext, TransferMeta, Trust,
};
use zeroize::Zeroizing;

use crate::config::{WEIDA_TRACEPARENT_LEN, wire};
use crate::error::{WEIDA_EINVAL, fail, fail_abi, guard, guard_or, guard_ptr, guard_void};
use crate::handle::{CText, borrow, bytes, cstr, into_handle, out_ptr, release, write_out};
use crate::identity::{weida_identity_source, weida_trust_source};

fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whom a dialler trusts: pins added to what each address names.
pub struct weida_trust {
    trust: Mutex<Trust>,
}

impl weida_trust {
    /// A copy of the trust, taken by the call that hands it on.
    pub(crate) fn get(&self) -> Trust {
        locked(&self.trust).clone()
    }

    pub(crate) fn handle(trust: Trust) -> *mut weida_trust {
        into_handle(weida_trust {
            trust: Mutex::new(trust),
        })
    }
}

/// A certificate chain and its private key.
pub struct weida_identity {
    pub(crate) identity: Identity,
    fingerprint: OnceLock<CText>,
}

/// The TLS terms a requester dials on.
pub struct weida_client_tls {
    tls: Mutex<ClientTls>,
}

impl weida_client_tls {
    pub(crate) fn get(&self) -> ClientTls {
        locked(&self.tls).clone()
    }
}

/// The TLS terms a QUIC binding serves on.
pub struct weida_server_tls {
    tls: Mutex<ServerTls>,
}

impl weida_server_tls {
    pub(crate) fn get(&self) -> ServerTls {
        locked(&self.tls).clone()
    }
}

/// PEM text handed out (`Identity::certificate_pem`, `Identity::to_pem`):
/// owned bytes the caller drops, wiped when dropped.
pub struct weida_pem {
    data: Zeroizing<Vec<u8>>,
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
        let handle = weida_trust::handle(Trust::by_address());
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

/// Adds the certificate authorities in a PEM buffer (`Trust::and_anchor`):
/// a peer whose chain leads to one of them and names the dialled host is
/// accepted. The bytes are copied; they are parsed at each dial that uses
/// the trust, which fails with `-WEIDA_ETLS` when they hold no certificate
/// or do not parse.
///
/// # Safety
///
/// `t` is NULL or a live trust; `pem` is NULL only with a zero `len`, else
/// points to that many readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_add_anchor(
    t: *const weida_trust,
    pem: *const u8,
    len: usize,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        // SAFETY: forwarded from this function's contract.
        let pem = unsafe { bytes(pem, len) }?;
        let mut trust = locked(&t.trust);
        *trust = std::mem::take(&mut *trust).and_anchor(pem.to_vec());
        Ok(0)
    })
}

/// Adds the certificate authorities in a PEM file
/// (`Trust::and_anchor_file`). The file is read at each dial that uses the
/// trust, which fails with `-WEIDA_ETLS` when it is missing or bad.
///
/// # Safety
///
/// `t` is NULL or a live trust; `path` is NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_add_anchor_file(
    t: *const weida_trust,
    path: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let t = unsafe { borrow(t) }?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let mut trust = locked(&t.trust);
        *trust = std::mem::take(&mut *trust).and_anchor_file(path);
        Ok(0)
    })
}

/// 1 when the trust holds no pin and no anchor, so it accepts only what
/// each dialled address names (`Trust::is_empty`); 0 otherwise;
/// `-WEIDA_EINVAL` for NULL.
///
/// # Safety
///
/// `t` is NULL or a live trust.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_trust_is_empty(t: *const weida_trust) -> c_int {
    // SAFETY: forwarded from this function's contract.
    guard(|| Ok(c_int::from(unsafe { borrow(t) }?.get().is_empty())))
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

pub(crate) fn identity_handle(identity: Identity) -> *mut weida_identity {
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

/// Generates a self-signed identity with a fresh key whose certificate also
/// names `names`, DNS names or IP literals (`Identity::generate_for`): a
/// peer that takes the certificate as an anchor can then verify the host it
/// dialled. `n == 0` is `weida_identity_generate`.
///
/// Fails with `-WEIDA_EINVAL` when `names` is NULL with `n > 0`, an element
/// is NULL or a name is not UTF-8, and with `-WEIDA_ETLS` when a name is
/// refused for the certificate.
///
/// # Safety
///
/// `names` is NULL only with `n == 0`, else points to `n` readable
/// pointers, each NULL or a NUL-terminated string; `out` is NULL or
/// writable. Release the result with `weida_identity_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_generate_for(
    names: *const *const c_char,
    n: usize,
    out: *mut *mut weida_identity,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let names = unsafe { crate::handle::names(names, n) }?;
        let identity = Identity::generate_for(names).map_err(fail)?;
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

/// An identity from a chain file and a key file
/// (`Identity::from_pem_files`). Nothing is read here: a missing or bad file
/// fails the first use (the fingerprint, a PEM getter, the bind or the dial)
/// with `-WEIDA_ETLS`.
///
/// # Safety
///
/// `cert_chain` and `key` are NULL or NUL-terminated strings; `out` is NULL
/// or writable. Release the result with `weida_identity_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_from_pem_files(
    cert_chain: *const c_char,
    key: *const c_char,
    out: *mut *mut weida_identity,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let cert_chain = unsafe { cstr(cert_chain) }?;
        // SAFETY: forwarded from this function's contract.
        let key = unsafe { cstr(key) }?;
        let identity = Identity::from_pem_files(cert_chain, key);
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, identity_handle(identity)) };
        Ok(0)
    })
}

/// An identity from one PEM file holding both the chain and the key, the
/// form `weida_identity_to_pem` writes (`Identity::from_pem_file`). Nothing
/// is read here, as with `weida_identity_from_pem_files`.
///
/// # Safety
///
/// `path` is NULL or a NUL-terminated string; `out` is NULL or writable.
/// Release the result with `weida_identity_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_from_pem_file(
    path: *const c_char,
    out: *mut *mut weida_identity,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let path = unsafe { cstr(path) }?;
        let identity = Identity::from_pem_file(path);
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

/// Writes the PEM a getter made to `out` as a new `weida_pem`.
///
/// # Safety
///
/// `id` is NULL or a live identity; `out` is NULL or writable.
unsafe fn identity_pem(
    id: *const weida_identity,
    out: *mut *mut weida_pem,
    make: impl FnOnce(&Identity) -> Result<String, weida_rs::Error>,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from the caller's contract.
        let handle = unsafe { borrow(id) }?;
        // `into_bytes` moves the text into the zeroizing buffer, no copy.
        let data = Zeroizing::new(make(&handle.identity).map_err(fail)?.into_bytes());
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_pem { data })) };
        Ok(0)
    })
}

/// Writes the identity's certificate chain as PEM, without the key
/// (`Identity::certificate_pem`), to `out`: publishable, for a peer to
/// anchor or pin.
///
/// The source is read on every call; fails with `-WEIDA_ETLS` when it does
/// not read or parse.
///
/// # Safety
///
/// `id` is NULL or a live identity; `out` is NULL or writable. Release the
/// result with `weida_pem_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_certificate_pem(
    id: *const weida_identity,
    out: *mut *mut weida_pem,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { identity_pem(id, out, Identity::certificate_pem) }
}

/// Writes the chain and the private key as one PEM document
/// (`Identity::to_pem`) to `out`, the form `weida_identity_from_pem_file`
/// and `weida_identity_from_pem` read back. The key is PKCS#8, PKCS#1 or
/// SEC1, as the source held it.
///
/// Contains the private key; the bytes are wiped by `weida_pem_drop`, and
/// every copy the caller makes is the caller's to wipe.
///
/// The source is read on every call; fails with `-WEIDA_ETLS` when it does
/// not read or parse.
///
/// # Safety
///
/// `id` is NULL or a live identity; `out` is NULL or writable. Release the
/// result with `weida_pem_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_identity_to_pem(
    id: *const weida_identity,
    out: *mut *mut weida_pem,
) -> c_int {
    // SAFETY: forwarded from this function's contract.
    unsafe { identity_pem(id, out, Identity::to_pem) }
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

/// The PEM's bytes; `weida_pem_len` of them, not NUL-terminated. NULL for
/// NULL. Valid as long as `p`.
///
/// # Safety
///
/// `p` is NULL or a live PEM.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pem_data(p: *const weida_pem) -> *const u8 {
    guard_ptr(|| {
        if p.is_null() {
            return std::ptr::null();
        }
        // SAFETY: a non-NULL `p` is a live PEM by the caller's contract.
        unsafe { &*p }.data.as_ptr()
    })
}

/// The PEM's length in bytes; 0 for NULL.
///
/// # Safety
///
/// `p` is NULL or a live PEM.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pem_len(p: *const weida_pem) -> usize {
    guard_or(0, || {
        if p.is_null() {
            return 0;
        }
        // SAFETY: a non-NULL `p` is a live PEM by the caller's contract.
        unsafe { &*p }.data.len()
    })
}

/// Releases a PEM and wipes its bytes. NULL does nothing.
///
/// # Safety
///
/// `p` is NULL or a live PEM, dropped at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_pem_drop(p: *mut weida_pem) {
    // SAFETY: forwarded from this function's contract.
    guard_void(|| drop(unsafe { release(p) }));
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
        let tls = Mutex::new(ClientTls::new(locked(&trust.trust).clone()));
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_client_tls { tls })) };
        Ok(0)
    })
}

/// Client terms that dial anonymously under the trust source `trust` of the
/// moment (`ClientTls::new` given a `TrustSource`). The source is shared,
/// not copied: an update reaches the next connection.
///
/// # Safety
///
/// `trust` is NULL or a live trust source; `out` is NULL or writable.
/// Release the result with `weida_client_tls_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_client_tls_new_source(
    trust: *const weida_trust_source,
    out: *mut *mut weida_client_tls,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let trust = unsafe { borrow(trust) }?;
        let tls = Mutex::new(ClientTls::new(trust.source.clone()));
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_client_tls { tls })) };
        Ok(0)
    })
}

/// Presents `identity` to every peer a requester made from these terms
/// afterwards dials (`ClientTls::with_identity`). The identity is copied.
///
/// # Safety
///
/// `tls` is NULL or live client terms; `identity` is NULL or a live
/// identity.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_client_tls_set_identity(
    tls: *const weida_client_tls,
    identity: *const weida_identity,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        // SAFETY: forwarded from this function's contract.
        let identity = unsafe { borrow(identity) }?;
        let mut g = locked(&tls.tls);
        *g = g.clone().with_identity(identity.identity.clone());
        Ok(0)
    })
}

/// Presents the identity source `identity` holds at each connection
/// (`ClientTls::with_identity` given an `IdentitySource`). The source is
/// shared, not copied: an update reaches the next connection.
///
/// # Safety
///
/// `tls` is NULL or live client terms; `identity` is NULL or a live
/// identity source.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_client_tls_set_identity_source(
    tls: *const weida_client_tls,
    identity: *const weida_identity_source,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        // SAFETY: forwarded from this function's contract.
        let identity = unsafe { borrow(identity) }?;
        let mut g = locked(&tls.tls);
        *g = g.clone().with_identity(identity.source.clone());
        Ok(0)
    })
}

/// Asks for `alpn`, a `WEIDA_ALPN_*` constant, instead of `WEIDA_ALPN_WEIDA`
/// (`ClientTls::with_alpn`); any other value is `-WEIDA_EINVAL` and changes
/// nothing. A binding offering another token fails the handshake: the dial
/// sees `-WEIDA_ETLS`.
///
/// # Safety
///
/// `tls` is NULL or live client terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_client_tls_set_alpn(
    tls: *const weida_client_tls,
    alpn: c_int,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let alpn = crate::config::alpn(alpn)?;
        let mut g = locked(&tls.tls);
        *g = g.clone().with_alpn(alpn);
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
        let tls = Mutex::new(ServerTls::new(identity.identity.clone()));
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_server_tls { tls })) };
        Ok(0)
    })
}

/// Server terms that present the identity source `identity` holds at each
/// handshake and accept anonymous clients (`ServerTls::new` given an
/// `IdentitySource`). The source is shared, not copied: a renewed or
/// replaced certificate reaches the next peer with no re-bind.
///
/// # Safety
///
/// `identity` is NULL or a live identity source; `out` is NULL or writable.
/// Release the result with `weida_server_tls_drop`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_new_source(
    identity: *const weida_identity_source,
    out: *mut *mut weida_server_tls,
) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        // SAFETY: forwarded from this function's contract.
        let identity = unsafe { borrow(identity) }?;
        let tls = Mutex::new(ServerTls::new(identity.source.clone()));
        // SAFETY: `out` passed `out_ptr`.
        unsafe { write_out(out, into_handle(weida_server_tls { tls })) };
        Ok(0)
    })
}

/// Requires every client to present a key `trust` accepts, pinned or
/// chaining to an anchor (`ServerTls::require_client`). The trust is
/// copied. Replaces an earlier `_require_client` or `_require_any_key`.
///
/// An empty trust is taken here; the bind then fails with `-WEIDA_ETLS`,
/// since such a binding would accept no client.
///
/// # Safety
///
/// `tls` is NULL or live server terms; `trust` is NULL or a live trust.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_require_client(
    tls: *const weida_server_tls,
    trust: *const weida_trust,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        // SAFETY: forwarded from this function's contract.
        let trust = unsafe { borrow(trust) }?;
        let trust = locked(&trust.trust).clone();
        let mut g = locked(&tls.tls);
        *g = g.clone().require_client(trust);
        Ok(0)
    })
}

/// Requires every client to present a key the trust source `trust` accepts
/// at the handshake (`ServerTls::require_client` given a `TrustSource`).
/// The source is shared, not copied. Replaces an earlier `_require_client*`
/// or `_require_any_key`.
///
/// An empty source is taken here; the bind then fails with `-WEIDA_ETLS`,
/// as with `weida_server_tls_require_client`.
///
/// # Safety
///
/// `tls` is NULL or live server terms; `trust` is NULL or a live trust
/// source.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_require_client_source(
    tls: *const weida_server_tls,
    trust: *const weida_trust_source,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        // SAFETY: forwarded from this function's contract.
        let trust = unsafe { borrow(trust) }?;
        let mut g = locked(&tls.tls);
        *g = g.clone().require_client(trust.source.clone());
        Ok(0)
    })
}

/// Requires every client to present a key and prove it holds it, whatever
/// the key (`ServerTls::require_client(ClientTrust::AnyKey)`). Replaces an
/// earlier `_require_client` or `_require_any_key`.
///
/// # Safety
///
/// `tls` is NULL or live server terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_require_any_key(tls: *const weida_server_tls) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let mut g = locked(&tls.tls);
        *g = g.clone().require_client(ClientTrust::AnyKey);
        Ok(0)
    })
}

/// Offers `alpn`, a `WEIDA_ALPN_*` constant, instead of `WEIDA_ALPN_WEIDA`
/// (`ServerTls::with_alpn`); any other value is `-WEIDA_EINVAL` and changes
/// nothing. A dial asking for another token fails with `-WEIDA_ETLS`.
///
/// # Safety
///
/// `tls` is NULL or live server terms.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_server_tls_set_alpn(
    tls: *const weida_server_tls,
    alpn: c_int,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let tls = unsafe { borrow(tls) }?;
        let alpn = crate::config::alpn(alpn)?;
        let mut g = locked(&tls.tls);
        *g = g.clone().with_alpn(alpn);
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

/// Sets the advisory payload length (`TransferMeta::with_content_len`).
///
/// # Safety
///
/// `m` is NULL or live metadata.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_content_len(
    m: *const weida_transfer_meta,
    len: u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_content_len(len);
        Ok(0)
    })
}

/// Labels the transfer with a topic (`TransferMeta::with_topic`). A topic
/// longer than 256 bytes fails the send with `-WEIDA_ELIMIT_EXCEEDED`.
///
/// # Safety
///
/// `m` is NULL or live metadata; `topic` is NULL or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_topic(
    m: *const weida_transfer_meta,
    topic: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        // SAFETY: forwarded from this function's contract.
        let topic = unsafe { cstr(topic) }?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_topic(topic);
        Ok(0)
    })
}

/// Propagates a trace context (`TransferMeta::with_trace`), given as its W3C
/// version-00 `traceparent` text. Anything else is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `m` is NULL or live metadata; `traceparent` is NULL or a NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_trace(
    m: *const weida_transfer_meta,
    traceparent: *const c_char,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        // SAFETY: forwarded from this function's contract.
        let text = unsafe { cstr(traceparent) }?;
        let trace = TraceContext::parse_traceparent(text)
            .map_err(|_| fail_abi(WEIDA_EINVAL, c"not a W3C version-00 traceparent"))?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_trace(trace);
        Ok(0)
    })
}

/// States the completion level this side achieved for the message it
/// answers (`TransferMeta::with_achieved`), a `WEIDA_ACKNOWLEDGEMENT_*`
/// constant; any other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `m` is NULL or live metadata.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_achieved(
    m: *const weida_transfer_meta,
    level: c_int,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        let achieved = wire(level, Acknowledgement::from_wire)?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_achieved(achieved);
        Ok(0)
    })
}

/// Orders the cursor levels the receiver should report
/// (`TransferMeta::with_report`), `len` wire values from `levels`: below
/// `WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR` a `WEIDA_ACKNOWLEDGEMENT_*`
/// value, at or above it an application stage. Replaces the order; the
/// levels are sorted and deduplicated, and `len == 0` orders nothing. A
/// value from 6 to 15 is `-WEIDA_EINVAL` and leaves the metadata unchanged;
/// more than 16 distinct levels fail the send with
/// `-WEIDA_ELIMIT_EXCEEDED`.
///
/// # Safety
///
/// `m` is NULL or live metadata; `levels` is NULL only with `len == 0`,
/// else points to `len` readable values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_report(
    m: *const weida_transfer_meta,
    levels: *const u64,
    len: usize,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        // SAFETY: forwarded from this function's contract.
        let levels = unsafe { crate::cursor::levels(levels, len) }?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_report(levels);
        Ok(0)
    })
}

/// How the receiver should report the ordered levels
/// (`TransferMeta::with_report_mode`), a `WEIDA_REPORT_MODE_*` constant;
/// any other value is `-WEIDA_EINVAL`.
///
/// # Safety
///
/// `m` is NULL or live metadata.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_transfer_meta_set_report_mode(
    m: *const weida_transfer_meta,
    mode: c_int,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let m = unsafe { borrow(m) }?;
        let mode = wire(mode, ReportMode::from_wire)?;
        let mut meta = locked(&m.meta);
        *meta = std::mem::take(&mut *meta).with_report_mode(mode);
        Ok(0)
    })
}

/// Writes a fresh root trace context (`weida::new_trace`) to `out` as its
/// `traceparent` text: `WEIDA_TRACEPARENT_LEN` bytes and a NUL. A NULL `out`
/// or a `len` below `WEIDA_TRACEPARENT_LEN + 1` is `-WEIDA_EINVAL`.
///
/// Weida never mints a trace on its own; this is how a caller starts one.
///
/// # Safety
///
/// `out` is NULL or points to `len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn weida_new_trace(out: *mut c_char, len: usize) -> c_int {
    guard(|| {
        let out = out_ptr(out)?;
        let need = WEIDA_TRACEPARENT_LEN as usize;
        if len <= need {
            return Err(fail_abi(
                WEIDA_EINVAL,
                c"the buffer holds fewer than WEIDA_TRACEPARENT_LEN + 1 bytes",
            ));
        }
        let text = weida_rs::new_trace().to_traceparent();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::WEIDA_EINVAL;
    use std::ffi::CStr;
    use std::ptr;

    /// The identity's fingerprint text.
    fn fingerprint(id: *const weida_identity) -> String {
        let mut text = ptr::null();
        // SAFETY: `id` is a live identity and `text` is writable.
        assert_eq!(unsafe { weida_identity_fingerprint(id, &mut text) }, 0);
        // SAFETY: on success the text is a NUL-terminated string that lives
        // as long as `id`.
        unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned()
    }

    /// The bytes of a PEM handle; drops the handle.
    fn take(p: *mut weida_pem) -> Vec<u8> {
        // SAFETY: `p` is a live PEM; its data has `weida_pem_len` bytes.
        let bytes =
            unsafe { std::slice::from_raw_parts(weida_pem_data(p), weida_pem_len(p)) }.to_vec();
        // SAFETY: `p` is live and dropped once.
        unsafe { weida_pem_drop(p) };
        bytes
    }

    fn contains(haystack: &[u8], needle: &str) -> bool {
        haystack
            .windows(needle.len())
            .any(|w| w == needle.as_bytes())
    }

    #[test]
    fn a_pem_written_by_to_pem_reads_back_with_the_same_fingerprint() {
        let mut id = ptr::null_mut();
        let mut pem = ptr::null_mut();
        let mut cert = ptr::null_mut();
        let mut back = ptr::null_mut();
        // SAFETY: every out-pointer is writable and every handle is live
        // until it is dropped below.
        unsafe {
            assert_eq!(weida_identity_generate(&mut id), 0);
            assert_eq!(weida_identity_to_pem(id, &mut pem), 0);
            assert_eq!(weida_identity_certificate_pem(id, &mut cert), 0);
        }
        let pem = take(pem);
        let cert = take(cert);
        assert!(contains(&pem, "-----BEGIN PRIVATE KEY-----"));
        assert!(contains(&cert, "-----BEGIN CERTIFICATE-----"));
        assert!(!contains(&cert, "PRIVATE KEY"));
        // SAFETY: `pem` outlives the call, which copies it.
        unsafe {
            assert_eq!(
                weida_identity_from_pem(
                    pem.as_ptr(),
                    pem.len(),
                    pem.as_ptr(),
                    pem.len(),
                    &mut back
                ),
                0
            );
        }
        assert_eq!(fingerprint(back), fingerprint(id));
        // SAFETY: both are live and dropped once.
        unsafe {
            weida_identity_drop(back);
            weida_identity_drop(id);
        }
    }

    #[test]
    fn generate_for_refuses_a_null_name() {
        let names = [ptr::null::<c_char>()];
        let mut id = ptr::null_mut();
        // SAFETY: `names` holds one pointer, `id` is writable.
        unsafe {
            assert_eq!(
                weida_identity_generate_for(names.as_ptr(), 1, &mut id),
                -WEIDA_EINVAL
            );
            assert_eq!(
                weida_identity_generate_for(ptr::null(), 1, &mut id),
                -WEIDA_EINVAL
            );
            assert!(id.is_null());
            assert_eq!(weida_identity_generate_for(ptr::null(), 0, &mut id), 0);
        }
        assert!(!id.is_null());
        assert!(fingerprint(id).starts_with("sha256:"));
        // SAFETY: `id` is live and dropped once.
        unsafe { weida_identity_drop(id) };
    }

    #[test]
    fn alpn_setters_take_the_two_tokens_and_refuse_others() {
        use crate::config::{WEIDA_ALPN_RAFT, WEIDA_ALPN_WEIDA};
        use weida_rs::Alpn;

        let mut trust = ptr::null_mut();
        let mut ctls = ptr::null_mut();
        let mut id = ptr::null_mut();
        let mut stls = ptr::null_mut();
        let pin = format!("sha256:{}\0", "0".repeat(64));
        // SAFETY: every handle is live until its drop, every out-parameter
        // writable, the pin NUL-terminated.
        unsafe {
            assert_eq!(weida_trust_new(&mut trust), 0);
            assert_eq!(weida_trust_is_empty(trust), 1);
            assert_eq!(weida_client_tls_new(trust, &mut ctls), 0);
            assert_eq!(weida_client_tls_set_alpn(ctls, WEIDA_ALPN_RAFT), 0);
            assert_eq!((*ctls).get().alpn, Alpn::Raft);
            assert_eq!(weida_client_tls_set_alpn(ctls, 2), -WEIDA_EINVAL);
            assert_eq!(weida_client_tls_set_alpn(ctls, -1), -WEIDA_EINVAL);
            assert_eq!((*ctls).get().alpn, Alpn::Raft);
            assert_eq!(weida_client_tls_set_alpn(ctls, WEIDA_ALPN_WEIDA), 0);
            assert_eq!((*ctls).get().alpn, Alpn::Weida);

            assert_eq!(weida_identity_generate(&mut id), 0);
            assert_eq!(weida_server_tls_new(id, &mut stls), 0);
            assert_eq!(weida_server_tls_set_alpn(stls, WEIDA_ALPN_RAFT), 0);
            assert_eq!((*stls).get().alpn, Alpn::Raft);
            assert_eq!(weida_server_tls_set_alpn(stls, 2), -WEIDA_EINVAL);
            assert_eq!(weida_server_tls_set_alpn(stls, -1), -WEIDA_EINVAL);
            assert_eq!((*stls).get().alpn, Alpn::Raft);
            assert_eq!(weida_client_tls_set_alpn(ptr::null(), 0), -WEIDA_EINVAL);
            assert_eq!(weida_server_tls_set_alpn(ptr::null(), 0), -WEIDA_EINVAL);

            assert_eq!(weida_trust_add_pin(trust, pin.as_ptr().cast()), 0);
            assert_eq!(weida_trust_is_empty(trust), 0);
            assert_eq!(weida_trust_is_empty(ptr::null()), -WEIDA_EINVAL);

            weida_server_tls_drop(stls);
            weida_identity_drop(id);
            weida_client_tls_drop(ctls);
            weida_trust_drop(trust);
        }
    }

    fn new_meta() -> *mut weida_transfer_meta {
        let mut m = ptr::null_mut();
        // SAFETY: `m` is writable.
        assert_eq!(unsafe { weida_transfer_meta_new(&mut m) }, 0);
        m
    }

    #[test]
    fn new_trace_writes_a_traceparent_that_sets_back() {
        let mut buf = [0 as c_char; 56];
        // SAFETY: `buf` holds 56 writable bytes.
        assert_eq!(unsafe { weida_new_trace(buf.as_mut_ptr(), buf.len()) }, 0);
        // SAFETY: on success `buf` holds a NUL-terminated string.
        let text = unsafe { CStr::from_ptr(buf.as_ptr()) };
        let trace = TraceContext::parse_traceparent(text.to_str().unwrap()).unwrap();
        assert!(trace.is_sampled());
        let m = new_meta();
        // SAFETY: `m` is live and `text` is NUL-terminated.
        assert_eq!(
            unsafe { weida_transfer_meta_set_trace(m, text.as_ptr()) },
            0
        );
        // SAFETY: `m` is live.
        assert_eq!(unsafe { &*m }.get().trace, Some(trace));
        let mut short = [0 as c_char; 55];
        // SAFETY: `short` holds 55 writable bytes.
        let rc = unsafe { weida_new_trace(short.as_mut_ptr(), short.len()) };
        assert_eq!(rc, -WEIDA_EINVAL);
        // SAFETY: `m` is live and dropped once.
        unsafe { weida_transfer_meta_drop(m) };
    }

    #[test]
    fn a_malformed_traceparent_is_invalid() {
        let m = new_meta();
        for text in [
            c"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            c"not a trace",
        ] {
            // SAFETY: `m` is live and `text` is NUL-terminated.
            let rc = unsafe { weida_transfer_meta_set_trace(m, text.as_ptr()) };
            assert_eq!(rc, -WEIDA_EINVAL, "{text:?}");
        }
        // SAFETY: `m` is live.
        assert_eq!(unsafe { &*m }.get().trace, None);
        // SAFETY: `m` is live and dropped once.
        unsafe { weida_transfer_meta_drop(m) };
    }
}
