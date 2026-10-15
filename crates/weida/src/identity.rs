//! Where an identity comes from: a source rather than a value
//! ([decisions/0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md)).
//!
//! An [`IdentitySource`] yields the current [`Identity`] and changes under
//! whoever holds it: a binding serves the certificate of the moment to the
//! next handshake without re-binding, a dialling endpoint presents it on its
//! next connection. Three sources live here — a static value, an ephemeral
//! generated identity, and a directory of files that is bootstrapped and
//! reloaded — and an external source (`weida-openbao`) drives one through
//! [`IdentitySource::update`]. A [`TrustSource`] is the same shape for the
//! anchors and pins a peer is judged by.
//!
//! What a source changes and what it does not: weida's fingerprint is the
//! key's, so a certificate renewed under the same key is invisible to every
//! pinning peer, while a new key is a new peer
//! ([0008](../../../docs/decisions/0008-session-identity.md) §4.2,
//! [0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)
//! §4.7). The events say which of the two happened.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use quinn::rustls::pki_types::CertificateDer;
use tokio::sync::broadcast;
use weida_core::{Error, Fingerprint};

use crate::config::{Identity, Trust};

/// Depth of a source's event stream.
const EVENT_QUEUE: usize = 64;

/// One transition of an [`IdentitySource`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityEvent {
    /// The identity was loaded: at construction, and after a reload that
    /// found the same key.
    Loaded {
        /// The key's fingerprint.
        fingerprint: Fingerprint,
    },
    /// The certificate changed and the key did not: a renewal. Pinning peers
    /// notice nothing.
    Renewed {
        /// The key's fingerprint, unchanged.
        fingerprint: Fingerprint,
    },
    /// The key changed: this is a new peer for everyone who pinned the old
    /// one, and their redial will refuse it.
    KeyChanged {
        /// The fingerprint that was.
        from: Fingerprint,
        /// The fingerprint that is.
        to: Fingerprint,
    },
    /// A reload or renewal was attempted and failed; the previous identity
    /// stays in service. Reported by a file source that could not read what
    /// changed, and by an external source whose renewal failed.
    RenewalFailed(String),
    /// A hand-off credential had been redeemed by somebody else before this
    /// process could (0032 §2). Reported by an external source; the process
    /// should not continue.
    HandoffStolen,
    /// A [`TrustSource`] was given new anchors or pins.
    TrustRefreshed,
    /// The reader fell behind by this many events, which were discarded.
    Missed(u64),
}

/// The event stream of one [`IdentitySource`] or [`TrustSource`].
pub struct IdentityEvents {
    rx: broadcast::Receiver<IdentityEvent>,
}

impl IdentityEvents {
    /// The next event, or `None` once the source is gone.
    pub async fn recv(&mut self) -> Option<IdentityEvent> {
        match self.rx.recv().await {
            Ok(event) => Some(event),
            Err(broadcast::error::RecvError::Lagged(n)) => Some(IdentityEvent::Missed(n)),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

impl std::fmt::Debug for IdentityEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityEvents").finish_non_exhaustive()
    }
}

/// What a source currently holds: the identity as given and its key's
/// fingerprint, computed once per load — or not yet, for a static value
/// whose material is read at first use.
#[derive(Clone, Debug)]
pub(crate) struct Loaded {
    pub(crate) identity: Identity,
    fingerprint: Option<Fingerprint>,
}

impl Loaded {
    fn new(identity: Identity) -> Result<Loaded, Error> {
        let fingerprint = identity.fingerprint()?;
        Ok(Loaded {
            identity,
            fingerprint: Some(fingerprint),
        })
    }

    /// The fingerprint, computing it for a deferred static value.
    pub(crate) fn fingerprint(&self) -> Result<Fingerprint, Error> {
        match self.fingerprint {
            Some(fp) => Ok(fp),
            None => self.identity.fingerprint(),
        }
    }
}

/// How a file source watches its directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilesOptions {
    /// Names the bootstrapped certificate is made for, when the directory is
    /// empty and the `generate` feature is on. Unused when files exist.
    pub names: Vec<String>,
    /// How often the files' modification times are checked. The check runs
    /// on the next use of the identity after the interval, not on a timer:
    /// a source needs no task and no reactor.
    pub poll: Duration,
}

impl Default for FilesOptions {
    fn default() -> Self {
        FilesOptions {
            names: Vec::new(),
            poll: Duration::from_secs(10),
        }
    }
}

/// The two files of a file source.
const CERT_FILE: &str = "cert.pem";
const KEY_FILE: &str = "key.pem";

struct Files {
    dir: PathBuf,
    poll: Duration,
    /// Modification times of the two files as last loaded.
    seen: Mutex<FilesSeen>,
}

struct FilesSeen {
    cert: Option<SystemTime>,
    key: Option<SystemTime>,
    checked: Instant,
}

enum Kind {
    /// A value: equal to another static source holding the same value, so
    /// two endpoints configured alike still share a pooled connection.
    Static,
    /// Generated in memory, so it exists only where something can generate.
    #[cfg(feature = "generate")]
    Ephemeral,
    Files(Files),
    /// Driven from outside through [`IdentitySource::update`].
    External,
}

struct IdentityInner {
    id: u64,
    kind: Kind,
    current: RwLock<Loaded>,
    generation: AtomicU64,
    events: broadcast::Sender<IdentityEvent>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Where an identity comes from, and the identity of the moment.
///
/// Cloning shares the source. Equality is what the connection pool keys on:
/// two static sources holding the same identity are equal, every other
/// source is equal only to itself.
#[derive(Clone)]
pub struct IdentitySource {
    inner: Arc<IdentityInner>,
}

impl IdentitySource {
    fn build(kind: Kind, loaded: Loaded) -> IdentitySource {
        let (events, _) = broadcast::channel(EVENT_QUEUE);
        let source = IdentitySource {
            inner: Arc::new(IdentityInner {
                id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                kind,
                current: RwLock::new(loaded),
                generation: AtomicU64::new(0),
                events,
            }),
        };
        if let Ok(fingerprint) = source.fingerprint() {
            source.emit(IdentityEvent::Loaded { fingerprint });
        }
        source
    }

    /// A source that holds `identity` and never changes.
    ///
    /// Fails when the material cannot be read or parsed, which is where a
    /// wrong path is caught: before a binding or a dial, not during a
    /// handshake.
    pub fn from_identity(identity: Identity) -> Result<IdentitySource, Error> {
        Ok(IdentitySource::build(Kind::Static, Loaded::new(identity)?))
    }

    /// A self-signed identity generated in memory for `names`, gone with the
    /// process: the default, and what a peer that pins by address needs.
    #[cfg(feature = "generate")]
    pub fn ephemeral(
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<IdentitySource, Error> {
        let identity = Identity::generate_for(names)?;
        Ok(IdentitySource::build(
            Kind::Ephemeral,
            Loaded::new(identity)?,
        ))
    }

    /// A source driven from outside: starts as `initial` and changes on
    /// [`IdentitySource::update`]. What an issuing client uses.
    pub fn external(initial: Identity) -> Result<IdentitySource, Error> {
        Ok(IdentitySource::build(Kind::External, Loaded::new(initial)?))
    }

    /// A directory holding `cert.pem` and `key.pem`, reloaded when either
    /// changes.
    ///
    /// When neither file exists and the `generate` feature is on, a
    /// self-signed identity for `options.names` is written there first —
    /// the directory `0700`, both files `0600` on Unix — so a second start
    /// finds the same key and the same fingerprint. Without the feature an
    /// empty directory is an error. A directory with one of the two files
    /// is an error in either case: half an identity is not one to bootstrap
    /// over.
    ///
    /// This is also how every external issuer that writes files plugs in —
    /// a Vault Agent template, cert-manager, `certbot --reuse-key`, a SPIRE
    /// agent: replace the files and the next use of the identity picks them
    /// up (0032 §4.2). Replace both atomically, or the certificate first and
    /// the key second, so no check sees a chain that does not match its key.
    pub fn files(dir: impl Into<PathBuf>, options: FilesOptions) -> Result<IdentitySource, Error> {
        let dir = dir.into();
        let cert = dir.join(CERT_FILE);
        let key = dir.join(KEY_FILE);
        match (cert.exists(), key.exists()) {
            (true, true) => {}
            (false, false) => bootstrap(&dir, &cert, &key, &options.names)?,
            _ => {
                return Err(Error::Tls(format!(
                    "{}: one of {CERT_FILE} and {KEY_FILE} exists without the other",
                    dir.display()
                )));
            }
        }
        let identity = Identity::from_pem_files(&cert, &key);
        let loaded = Loaded::new(identity)?;
        let seen = FilesSeen {
            cert: mtime(&cert),
            key: mtime(&key),
            checked: Instant::now(),
        };
        Ok(IdentitySource::build(
            Kind::Files(Files {
                dir,
                poll: options.poll,
                seen: Mutex::new(seen),
            }),
            loaded,
        ))
    }

    /// The identity of the moment.
    ///
    /// On a file source this is also where a due modification-time check
    /// runs, so a caller pays it at most once per poll interval.
    pub fn current(&self) -> Identity {
        self.refresh_if_due();
        self.inner
            .current
            .read()
            .expect("identity poisoned")
            .identity
            .clone()
    }

    /// The fingerprint of the current key.
    ///
    /// Fails only on a static value whose material cannot be read; every
    /// other source checked it when it loaded.
    pub fn fingerprint(&self) -> Result<Fingerprint, Error> {
        self.refresh_if_due();
        self.inner
            .current
            .read()
            .expect("identity poisoned")
            .fingerprint()
    }

    /// The current snapshot with its fingerprint, for the resolvers.
    pub(crate) fn loaded(&self) -> Loaded {
        self.refresh_if_due();
        self.inner
            .current
            .read()
            .expect("identity poisoned")
            .clone()
    }

    /// Counts up on every change; what a resolver compares to know whether
    /// its cached key is current.
    pub(crate) fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    /// Replaces the identity. `Renewed` when the key is unchanged,
    /// `KeyChanged` when it is not.
    ///
    /// Fails, and changes nothing, when the material cannot be read: a
    /// renewal that delivered garbage leaves the previous identity in
    /// service and reports `RenewalFailed`.
    pub fn update(&self, identity: Identity) -> Result<(), Error> {
        let loaded = match Loaded::new(identity) {
            Ok(loaded) => loaded,
            Err(e) => {
                self.emit(IdentityEvent::RenewalFailed(e.to_string()));
                return Err(e);
            }
        };
        self.install(loaded);
        Ok(())
    }

    /// Reports a failed renewal that never produced material, from an
    /// external source. The identity in service is unchanged.
    pub fn report_failure(&self, reason: impl Into<String>) {
        self.emit(IdentityEvent::RenewalFailed(reason.into()));
    }

    /// Reports that this process's hand-off credential had already been
    /// redeemed (0032 §2). The identity in service is unchanged; the caller
    /// is expected not to continue.
    pub fn report_handoff_stolen(&self) {
        self.emit(IdentityEvent::HandoffStolen);
    }

    /// Reloads a file source now, regardless of the poll interval.
    ///
    /// `Ok(true)` when the files had changed and were loaded, `Ok(false)`
    /// when nothing changed, and an error — also reported as
    /// `RenewalFailed` — when they changed and could not be loaded. Not a
    /// file source: `Ok(false)`.
    pub fn reload(&self) -> Result<bool, Error> {
        let Kind::Files(files) = &self.inner.kind else {
            return Ok(false);
        };
        let cert = files.dir.join(CERT_FILE);
        let key = files.dir.join(KEY_FILE);
        let (cert_seen, key_seen) = (mtime(&cert), mtime(&key));
        {
            let mut seen = files.seen.lock().expect("file times poisoned");
            seen.checked = Instant::now();
            if seen.cert == cert_seen && seen.key == key_seen {
                return Ok(false);
            }
            seen.cert = cert_seen;
            seen.key = key_seen;
        }
        match Loaded::new(Identity::from_pem_files(&cert, &key)) {
            Ok(loaded) => {
                self.install(loaded);
                Ok(true)
            }
            Err(e) => {
                self.emit(IdentityEvent::RenewalFailed(e.to_string()));
                Err(e)
            }
        }
    }

    /// The event stream of this source; each call is an independent reader.
    pub fn events(&self) -> IdentityEvents {
        IdentityEvents {
            rx: self.inner.events.subscribe(),
        }
    }

    fn install(&self, loaded: Loaded) {
        let event = {
            let mut current = self.inner.current.write().expect("identity poisoned");
            let to = loaded
                .fingerprint()
                .expect("an installed identity was parsed when it was loaded");
            let event = match current.fingerprint() {
                Ok(from) if from == to => IdentityEvent::Renewed { fingerprint: to },
                Ok(from) => IdentityEvent::KeyChanged { from, to },
                // The previous value never parsed; there is no key to have
                // changed from.
                Err(_) => IdentityEvent::Loaded { fingerprint: to },
            };
            *current = loaded;
            self.inner.generation.fetch_add(1, Ordering::AcqRel);
            event
        };
        self.emit(event);
    }

    /// Runs the due check of a file source, reloading when the interval has
    /// passed. A caller that compares generations runs it first.
    pub(crate) fn refresh_if_due(&self) {
        let Kind::Files(files) = &self.inner.kind else {
            return;
        };
        let due = {
            let seen = files.seen.lock().expect("file times poisoned");
            seen.checked.elapsed() >= files.poll
        };
        if due {
            // The error is on the event stream; the identity in service is
            // what the caller gets either way.
            let _ = self.reload();
        }
    }

    fn emit(&self, event: IdentityEvent) {
        let _ = self.inner.events.send(event);
    }

    fn is_static(&self) -> bool {
        matches!(self.inner.kind, Kind::Static)
    }

    /// A weak handle to this source, for a background task that must end
    /// when the application drops its last strong handle.
    /// See [`WeakIdentitySource`].
    pub fn downgrade(&self) -> WeakIdentitySource {
        WeakIdentitySource {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl From<Identity> for IdentitySource {
    /// A static source. The material is read at first use rather than here:
    /// `From` cannot fail, and a wrong path is still caught before any
    /// handshake, by the bind or the dial that first needs the key.
    fn from(identity: Identity) -> IdentitySource {
        IdentitySource::build(
            Kind::Static,
            Loaded {
                identity,
                fingerprint: None,
            },
        )
    }
}

impl PartialEq for IdentitySource {
    fn eq(&self, other: &IdentitySource) -> bool {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            return true;
        }
        self.is_static()
            && other.is_static()
            && self
                .inner
                .current
                .read()
                .expect("identity poisoned")
                .identity
                == other
                    .inner
                    .current
                    .read()
                    .expect("identity poisoned")
                    .identity
    }
}

impl Eq for IdentitySource {}

impl std::hash::Hash for IdentitySource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.is_static() {
            self.inner
                .current
                .read()
                .expect("identity poisoned")
                .identity
                .hash(state);
        } else {
            self.inner.id.hash(state);
        }
    }
}

impl std::fmt::Debug for IdentitySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.inner.kind {
            Kind::Static => "static",
            #[cfg(feature = "generate")]
            Kind::Ephemeral => "ephemeral",
            Kind::Files(_) => "files",
            Kind::External => "external",
        };
        f.debug_struct("IdentitySource")
            .field("kind", &kind)
            .field(
                "fingerprint",
                &self
                    .inner
                    .current
                    .read()
                    .expect("identity poisoned")
                    .fingerprint()
                    .ok(),
            )
            .finish_non_exhaustive()
    }
}

/// A handle to an [`IdentitySource`] that does not keep it alive.
///
/// A background task that renews an identity holds one of these, not a
/// strong handle: once the application drops its last strong handle the task
/// sees that through [`WeakIdentitySource::upgrade`] and ends, rather than
/// keeping the source and its key alive for the life of the process.
#[derive(Clone)]
pub struct WeakIdentitySource {
    inner: Weak<IdentityInner>,
}

impl WeakIdentitySource {
    /// The source, if any strong handle to it is still alive.
    pub fn upgrade(&self) -> Option<IdentitySource> {
        self.inner.upgrade().map(|inner| IdentitySource { inner })
    }
}

impl std::fmt::Debug for WeakIdentitySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakIdentitySource").finish_non_exhaustive()
    }
}

/// Writes a fresh self-signed identity into an empty directory, owner-only.
#[cfg(feature = "generate")]
fn bootstrap(dir: &Path, cert: &Path, key: &Path, names: &[String]) -> Result<(), Error> {
    std::fs::create_dir_all(dir).map_err(Error::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(Error::Io)?;
    }
    let identity = Identity::generate_for(names.iter().cloned())?;
    write_owner_only(cert, identity.certificate_pem()?.as_bytes())?;
    write_owner_only(key, crate::tls::key_pem(&identity.key)?.as_bytes())?;
    Ok(())
}

#[cfg(not(feature = "generate"))]
fn bootstrap(dir: &Path, _cert: &Path, _key: &Path, _names: &[String]) -> Result<(), Error> {
    Err(Error::Tls(format!(
        "{}: no identity to load, and generating one needs the `generate` feature",
        dir.display()
    )))
}

/// Creates `path` with `0600` and writes `bytes`; the mode is set at
/// creation rather than after, so no instant exists in which the file is
/// wider than owner-only.
#[cfg(feature = "generate")]
fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(Error::Io)?;
    file.write_all(bytes).map_err(Error::Io)?;
    file.sync_all().map_err(Error::Io)
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

// --- trust --------------------------------------------------------------------

struct TrustInner {
    id: u64,
    is_static: bool,
    current: RwLock<Trust>,
    generation: AtomicU64,
    events: broadcast::Sender<IdentityEvent>,
}

/// Whom a peer is accepted as, as a source: the anchors and pins of the
/// moment, changeable under a live binding.
///
/// A static source is equal to another holding the same [`Trust`], which is
/// what keeps two endpoints configured alike on one pooled connection.
#[derive(Clone)]
pub struct TrustSource {
    inner: Arc<TrustInner>,
}

impl TrustSource {
    fn build(trust: Trust, is_static: bool) -> TrustSource {
        let (events, _) = broadcast::channel(EVENT_QUEUE);
        TrustSource {
            inner: Arc::new(TrustInner {
                id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                is_static,
                current: RwLock::new(trust),
                generation: AtomicU64::new(0),
                events,
            }),
        }
    }

    /// A source that holds `trust` and never changes.
    pub fn from_trust(trust: Trust) -> TrustSource {
        TrustSource::build(trust, true)
    }

    /// A source driven from outside through [`TrustSource::update`]: what
    /// an anchor fetched from an authority, and refreshed, uses.
    pub fn external(initial: Trust) -> TrustSource {
        TrustSource::build(initial, false)
    }

    /// The trust of the moment.
    pub fn current(&self) -> Trust {
        self.inner.current.read().expect("trust poisoned").clone()
    }

    /// Replaces the trust; reported as `TrustRefreshed`.
    pub fn update(&self, trust: Trust) {
        {
            let mut current = self.inner.current.write().expect("trust poisoned");
            *current = trust;
        }
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        let _ = self.inner.events.send(IdentityEvent::TrustRefreshed);
    }

    /// Reports a failed refresh from an external source; the trust in
    /// service is unchanged.
    pub fn report_failure(&self, reason: impl Into<String>) {
        let _ = self
            .inner
            .events
            .send(IdentityEvent::RenewalFailed(reason.into()));
    }

    /// The event stream of this source.
    pub fn events(&self) -> IdentityEvents {
        IdentityEvents {
            rx: self.inner.events.subscribe(),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    /// True when nothing is trusted beyond what an address names.
    pub fn is_empty(&self) -> bool {
        self.inner
            .current
            .read()
            .expect("trust poisoned")
            .is_empty()
    }

    /// A weak handle to this source, for a background task that must end
    /// when the application drops its last strong handle.
    /// See [`WeakTrustSource`].
    pub fn downgrade(&self) -> WeakTrustSource {
        WeakTrustSource {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl From<Trust> for TrustSource {
    fn from(trust: Trust) -> TrustSource {
        TrustSource::from_trust(trust)
    }
}

impl PartialEq for TrustSource {
    fn eq(&self, other: &TrustSource) -> bool {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            return true;
        }
        self.inner.is_static
            && other.inner.is_static
            && *self.inner.current.read().expect("trust poisoned")
                == *other.inner.current.read().expect("trust poisoned")
    }
}

impl Eq for TrustSource {}

impl std::hash::Hash for TrustSource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.inner.is_static {
            self.inner
                .current
                .read()
                .expect("trust poisoned")
                .hash(state);
        } else {
            self.inner.id.hash(state);
        }
    }
}

impl std::fmt::Debug for TrustSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrustSource")
            .field("static", &self.inner.is_static)
            .field(
                "trust",
                &*self.inner.current.read().expect("trust poisoned"),
            )
            .finish()
    }
}

/// A handle to a [`TrustSource`] that does not keep it alive.
///
/// A background task that refreshes anchors or pins holds one of these, not
/// a strong handle: once the application drops its last strong handle the
/// task sees that through [`WeakTrustSource::upgrade`] and ends, rather than
/// keeping the source alive for the life of the process.
#[derive(Clone)]
pub struct WeakTrustSource {
    inner: Weak<TrustInner>,
}

impl WeakTrustSource {
    /// The source, if any strong handle to it is still alive.
    pub fn upgrade(&self) -> Option<TrustSource> {
        self.inner.upgrade().map(|inner| TrustSource { inner })
    }
}

impl std::fmt::Debug for WeakTrustSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakTrustSource").finish_non_exhaustive()
    }
}

/// The certificate chain a peer presented in the TLS handshake: DER, leaf
/// first, as it arrived. weida parsed only the leaf's public key, whose
/// fingerprint is the peer; what the rest certifies is the application's to
/// read
/// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
/// §4.2).
///
/// Bounded before it is kept, because the peer chooses it: at most 8
/// certificates and 32 KiB together. Captured once per connection and shared,
/// so a clone is a reference count.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerChain(Arc<[CertificateDer<'static>]>);

impl PeerChain {
    /// A chain of at least one certificate.
    pub(crate) fn new(certs: Vec<CertificateDer<'static>>) -> PeerChain {
        debug_assert!(!certs.is_empty(), "a peer chain has a leaf");
        PeerChain(certs.into())
    }

    /// The leaf, whose public key is the peer's fingerprint.
    pub fn leaf(&self) -> &[u8] {
        &self.0[0]
    }

    /// Every certificate, leaf first.
    pub fn certs(&self) -> impl ExactSizeIterator<Item = &[u8]> + '_ {
        self.0.iter().map(|c| c.as_ref())
    }
}

impl std::fmt::Debug for PeerChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerChain")
            .field("certs", &self.0.len())
            .field("bytes", &self.0.iter().map(|c| c.len()).sum::<usize>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_weak_identity_source_lives_only_while_a_strong_handle_does() {
        let strong = IdentitySource::from(Identity::from_pem(Vec::new(), Vec::new()));
        let weak = strong.downgrade();
        let upgraded = weak.upgrade().expect("a strong handle is alive");
        assert_eq!(upgraded, strong);
        drop(strong);
        assert!(
            weak.upgrade().is_some(),
            "the handle upgraded from the weak one keeps the source alive"
        );
        drop(upgraded);
        assert!(weak.upgrade().is_none(), "no strong handle is left");
    }

    #[test]
    fn a_weak_trust_source_lives_only_while_a_strong_handle_does() {
        let strong = TrustSource::from_trust(Trust::default());
        let weak = strong.downgrade();
        let upgraded = weak.upgrade().expect("a strong handle is alive");
        assert_eq!(upgraded, strong);
        drop(strong);
        assert!(
            weak.upgrade().is_some(),
            "the handle upgraded from the weak one keeps the source alive"
        );
        drop(upgraded);
        assert!(weak.upgrade().is_none(), "no strong handle is left");
    }
}
