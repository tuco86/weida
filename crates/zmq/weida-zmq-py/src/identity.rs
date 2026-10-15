//! The three ZeroMQ identities, as three Python types that convert to nothing.
//!
//! A CURVE key, a routing id and a ZAP user id are **different claims about
//! different things**, and [0013](../../../../docs/decisions/0013-competitor-libraries.md)
//! §4.4 item 6 keeps them apart from weida's proved identities: no `From`,
//! `Into`, `AsRef` or `Deref` between the two groups exists in this workspace,
//! and none may be added.
//!
//! In this crate the rule is structural rather than a promise. `weida` and
//! `weida-protocol` are not in the manifest at all — `weida-zmq-py` depends on
//! `weida-zmq`, `weida-py-core` and `pyo3` — so there is no weida identity
//! here to convert to. What the three types below do have is the one
//! conversion each that is honest: bytes in, bytes out, under their own name.
//!
//! | Type | What it is | What it is not |
//! | --- | --- | --- |
//! | [`PyCurveKey`] | 32 octets of X25519, or 40 characters of Z85 | a proof of anything until a handshake uses it |
//! | [`PyRoutingId`] | 1-255 self-asserted octets with a nonzero first one | authenticated: a peer picks its own |
//! | [`PyZapUserId`] | whatever string a ZAP handler chose for a 200 | a weida principal, or a routing id |

use std::hash::{BuildHasher, RandomState};
use std::sync::LazyLock;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use weida_zmq::{CurvePublicKey, CurveSecretKey, RoutingId, ZapUserId};

use crate::errors::raise;

/// The key every [`PyCurveKey`]'s `hash()` is computed under, drawn once per
/// process.
///
/// SipHash under a key nobody outside the process knows: `hash(key)` is then a
/// value that equal keys share and that says nothing about the octets, however
/// many of them a log, a `dict` ordering or a debugging print shows. Hashing
/// the octets themselves (their first eight, say) would publish 64 bits of a
/// secret key the first time one was used as a `dict` key and printed.
static HASH_KEY: LazyLock<RandomState> = LazyLock::new(RandomState::new);

/// A CURVE key: 32 octets, taken as bytes or as 40 characters of Z85.
///
/// One type for both halves of a pair, as libzmq has one option type for
/// `ZMQ_CURVE_PUBLICKEY`, `ZMQ_CURVE_SECRETKEY` and `ZMQ_CURVE_SERVERKEY`:
/// the octets do not say which role they are for, and pretending they do
/// would be a type that lies.
///
/// # What it never does with the octets
///
/// Because the type cannot know whether it holds a secret, it treats every
/// key as one:
///
/// - `repr` and `str` name the type and print no octets, so a log line, a
///   traceback, a REPL echo or a test's assertion diff cannot carry a secret
///   key. [`bytes`](PyCurveKey::bytes) and [`z85`](PyCurveKey::z85) are the
///   deliberate way to write a key down.
/// - `hash` is keyed per process ([`HASH_KEY`]).
/// - `==` looks at every octet however early two keys differ, so how long a
///   comparison takes does not say how many leading octets a guess got right.
///
/// There is deliberately no `PartialEq`, `Eq`, `Hash` or `Debug` on the Rust
/// type either: the derived ones would compare early, hash the octets and
/// print them, and a later caller would not know to look.
#[pyclass(frozen, skip_from_py_object, name = "CurveKey", module = "weida_zmq")]
#[derive(Clone)]
pub struct PyCurveKey {
    octets: [u8; 32],
}

impl PyCurveKey {
    /// The octets, for the option table.
    pub fn octets(&self) -> &[u8; 32] {
        &self.octets
    }

    /// Takes either accepted form.
    pub fn parse(py: Python<'_>, given: &[u8]) -> PyResult<PyCurveKey> {
        let key = raise(py, CurvePublicKey::parse(given))?;
        Ok(PyCurveKey {
            octets: *key.as_bytes(),
        })
    }

    /// Whether `other` holds the same octets, comparing all of them whatever
    /// the first difference: the accumulator is complete before it is looked
    /// at, as `weida_zmq::CurveSecretKey`'s equality does.
    fn matches(&self, other: &PyCurveKey) -> bool {
        let mut difference = 0u8;
        for (ours, theirs) in self.octets.iter().zip(&other.octets) {
            difference |= ours ^ theirs;
        }
        difference == 0
    }

    /// The value `hash()` returns: [`HASH_KEY`]'s SipHash of the octets.
    fn hashed(&self) -> u64 {
        HASH_KEY.hash_one(self.octets)
    }
}

#[pymethods]
impl PyCurveKey {
    /// `CurveKey(b"<32 octets>")` or `CurveKey("<40 characters of Z85>")`.
    #[new]
    fn new(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<PyCurveKey> {
        let given = match value.extract::<PyRef<'_, PyCurveKey>>() {
            Ok(twin) => return Ok(twin.clone()),
            Err(_) => match value.extract::<String>() {
                Ok(text) => text.into_bytes(),
                Err(_) => weida_py_core::payload_of(value)?,
            },
        };
        PyCurveKey::parse(py, &given)
    }

    /// The 32 octets.
    #[getter]
    fn bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.octets)
    }

    /// The 40-character Z85 form, which is what a configuration file holds.
    #[getter]
    fn z85(&self) -> String {
        CurvePublicKey::from_bytes(self.octets).to_z85()
    }

    /// The public key these octets derive, **treating them as a secret key**.
    ///
    /// X25519 derives a public key from a secret one, which is why libzmq's
    /// manual says a CURVE server "does not need to know its own public key".
    fn public_key(&self) -> PyCurveKey {
        PyCurveKey {
            octets: *CurveSecretKey::from_bytes(self.octets)
                .public_key()
                .as_bytes(),
        }
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.extract::<PyRef<'_, PyCurveKey>>() {
            Ok(twin) => self.matches(&twin),
            Err(_) => false,
        }
    }

    fn __hash__(&self) -> u64 {
        self.hashed()
    }

    /// The type and nothing of the key: see the type's documentation.
    fn __repr__(&self) -> &'static str {
        "<weida_zmq.CurveKey redacted>"
    }
}

/// A fresh CURVE key pair, `(public, secret)`, as `zmq_curve_keypair` returns.
#[pyfunction]
pub fn curve_keypair(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
    let (public, secret) = weida_zmq::curve::keypair();
    PyTuple::new(
        py,
        [
            Py::new(
                py,
                PyCurveKey {
                    octets: *public.as_bytes(),
                },
            )?,
            Py::new(py, PyCurveKey::parse(py, secret.to_z85().as_bytes())?)?,
        ],
    )
}

/// `ZMQ_ROUTING_ID`: 1-255 octets a socket asserts about itself.
#[pyclass(frozen, skip_from_py_object, name = "RoutingId", module = "weida_zmq")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyRoutingId {
    octets: Vec<u8>,
}

impl PyRoutingId {
    /// The library's type, checked.
    pub fn library(&self, py: Python<'_>) -> PyResult<RoutingId> {
        raise(py, RoutingId::new(self.octets.clone()))
    }

    /// Wraps one the library produced.
    pub fn of(id: &RoutingId) -> PyRoutingId {
        PyRoutingId {
            octets: id.as_bytes().to_vec(),
        }
    }

    /// The octets, for the option table.
    pub fn octets(&self) -> &[u8] {
        &self.octets
    }
}

#[pymethods]
impl PyRoutingId {
    /// `RoutingId(b"worker-3")`, refused here if it is empty, longer than 255
    /// octets, or starts with a zero octet — which libzmq reserves.
    #[new]
    fn new(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<PyRoutingId> {
        if let Ok(twin) = value.extract::<PyRef<'_, PyRoutingId>>() {
            return Ok(twin.clone());
        }
        let octets = weida_py_core::payload_of(value)?;
        let checked = raise(py, RoutingId::new(octets))?;
        Ok(PyRoutingId::of(&checked))
    }

    /// The octets, as they go on the wire.
    #[getter]
    fn bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.octets)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.extract::<PyRef<'_, PyRoutingId>>() {
            Ok(twin) => self.octets == twin.octets,
            Err(_) => match weida_py_core::payload_of(other) {
                Ok(octets) => self.octets == octets,
                Err(_) => false,
            },
        }
    }

    fn __hash__(&self) -> u64 {
        self.octets
            .iter()
            .fold(0u64, |hash, octet| hash.rotate_left(5) ^ u64::from(*octet))
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.RoutingId {}>",
            String::from_utf8_lossy(&self.octets)
        )
    }
}

/// The user id a ZAP handler returned with a 200.
///
/// A string a handler chose. It proves that *that handler* said yes, and
/// nothing else: it is not a key, not a routing id and not a weida principal.
#[pyclass(frozen, skip_from_py_object, name = "ZapUserId", module = "weida_zmq")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyZapUserId {
    text: String,
}

impl PyZapUserId {
    /// Wraps one the library read off the dialog.
    pub fn of(id: &ZapUserId) -> PyZapUserId {
        PyZapUserId {
            text: id.as_str().to_owned(),
        }
    }
}

#[pymethods]
impl PyZapUserId {
    /// `ZapUserId("admin")`, for comparing with what a handler issued.
    #[new]
    fn new(text: String) -> PyZapUserId {
        PyZapUserId { text }
    }

    /// The string itself.
    #[getter]
    fn text(&self) -> &str {
        &self.text
    }

    fn __str__(&self) -> &str {
        &self.text
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        if let Ok(twin) = other.extract::<PyRef<'_, PyZapUserId>>() {
            return self.text == twin.text;
        }
        match other.extract::<String>() {
            Ok(text) => self.text == text,
            Err(_) => false,
        }
    }

    fn __hash__(&self) -> u64 {
        self.text
            .bytes()
            .fold(0u64, |hash, octet| hash.rotate_left(5) ^ u64::from(octet))
    }

    fn __repr__(&self) -> String {
        format!("<weida_zmq.ZapUserId {}>", self.text)
    }
}

/// One of a socket's peers, as a snapshot.
///
/// "Application code cannot manipulate individual underlying connections", but
/// it can read what the library knows about one — including what the kernel
/// said about a local peer and what a ZAP handler said about any peer.
#[pyclass(frozen, name = "Peer", module = "weida_zmq")]
pub struct PyPeer {
    id: u64,
    connected: bool,
    announced: bool,
    attempts: u64,
    endpoint: Option<String>,
    routing_id: Option<PyRoutingId>,
    user_id: Option<PyZapUserId>,
    credentials: Option<(u32, u32, Option<u32>)>,
}

impl PyPeer {
    /// Wraps the library's snapshot.
    pub fn of(peer: &weida_zmq::Peer) -> PyPeer {
        PyPeer {
            id: peer.id.get(),
            connected: peer.connected,
            announced: peer.announced,
            attempts: peer.attempts,
            endpoint: peer.endpoint.as_ref().map(|endpoint| endpoint.to_string()),
            routing_id: peer.identity.as_ref().map(PyRoutingId::of),
            user_id: peer.user_id.as_ref().map(PyZapUserId::of),
            credentials: peer
                .credentials
                .as_ref()
                .map(|who| (who.uid, who.gid, who.pid)),
        }
    }
}

#[pymethods]
impl PyPeer {
    /// Which peer of this socket.
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    /// Whether a connection is up right now.
    #[getter]
    fn connected(&self) -> bool {
        self.connected
    }

    /// Whether the handshake finished, which is when the routing id means
    /// anything.
    #[getter]
    fn announced(&self) -> bool {
        self.announced
    }

    /// Connect attempts made for this peer, which is how a reconnect loop is
    /// visible without a monitor.
    #[getter]
    fn attempts(&self) -> u64 {
        self.attempts
    }

    /// The endpoint this socket dialled, or `None` for a peer that arrived on
    /// a bound one.
    #[getter]
    fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// The identity this peer announced, if any.
    #[getter]
    fn routing_id(&self) -> Option<PyRoutingId> {
        self.routing_id.clone()
    }

    /// The user id a ZAP handler returned for this connection, if it was
    /// authorized.
    #[getter]
    fn user_id(&self) -> Option<PyZapUserId> {
        self.user_id.clone()
    }

    /// `(uid, gid, pid)` for an `ipc://` peer: the kernel's statement, and not
    /// an identity the peer asserted.
    #[getter]
    fn credentials(&self) -> Option<(u32, u32, Option<u32>)> {
        self.credentials
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.Peer {} {}{}>",
            self.id,
            if self.connected {
                "connected"
            } else {
                "disconnected"
            },
            match &self.user_id {
                Some(user) => format!(" user={}", user.text),
                None => String::new(),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(fill: u8) -> PyCurveKey {
        PyCurveKey { octets: [fill; 32] }
    }

    #[test]
    fn keys_match_exactly_when_every_octet_does() {
        assert!(key(7).matches(&key(7)));
        for position in 0..32 {
            let mut other = key(7);
            other.octets[position] ^= 0x01;
            assert!(
                !key(7).matches(&other),
                "a difference in octet {position} must be seen"
            );
        }
    }

    #[test]
    fn equal_keys_hash_equal_and_the_hash_is_not_the_octets() {
        assert_eq!(key(9).hashed(), key(9).hashed());
        let mut secret = key(0);
        secret.octets = *b"0123456789abcdef0123456789abcdef";
        let leading = u64::from_le_bytes(secret.octets[..8].try_into().expect("32 octets"));
        assert_ne!(
            secret.hashed(),
            leading,
            "a hash made of the leading octets would publish them"
        );
    }
}
