//! ZeroMQ's identities, kept apart from weida's on purpose.
//!
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item 6 is
//! a compile-time rule, and this module is where it is kept: a routing id is
//! **1-255 self-asserted bytes whose first octet is nonzero**, and
//! `weida_core::Fingerprint` is the SHA-256 of a proved TLS public key. One is
//! a name a peer chose for itself; the other is what a peer demonstrated
//! possession of. **No `From`, `Into`, `AsRef` or `Deref` between the two
//! groups exists anywhere in this workspace, and none may be added** — that is
//! the rule of [0008](../../../docs/decisions/0008-session-identity.md) §5,
//! that a foreign, self-asserted identifier is never presented as a weida peer
//! identity, turned from prose into a compile error. Where the two must meet,
//! the meeting is a human's configuration decision, never a conversion.
//!
//! `weida_core::LocalPrincipal` is shared, because `ipc://` peer credentials
//! are the same kernel fact on both sides of the workspace. It is not a ZAP
//! identity and must not be presented as one.

use crate::error::{Error, Result};

/// Longest routing id, in bytes (`docs/research/zeromq.md` §11).
pub const MAX_ROUTING_ID_BYTES: usize = 255;

/// A ROUTER/STREAM routing id: what `ZMQ_ROUTING_ID` sets and what a peer
/// announces as the `Identity` property of its `READY`.
///
/// "An identity (also called an address) is a binary string whose only
/// meaning is 'this is a unique handle to the connection'"
/// (`docs/research/zeromq.md` §4.2). Self-asserted, so it proves nothing; the
/// rules are only that it is 1 to 255 bytes long and that its first octet is
/// not zero, because libzmq reserves a leading zero for the identities it
/// generates itself (`0` plus a random 32-bit integer).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoutingId(Vec<u8>);

impl RoutingId {
    /// Checks `bytes` and takes them.
    ///
    /// Fails with `EINVAL` on an empty id, one over
    /// [`MAX_ROUTING_ID_BYTES`], or one whose first octet is zero.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<RoutingId> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > MAX_ROUTING_ID_BYTES {
            return Err(Error::EINVAL(
                format!(
                    "a routing id is 1..={MAX_ROUTING_ID_BYTES} bytes; this one is {}",
                    bytes.len()
                )
                .into(),
            ));
        }
        if bytes[0] == 0 {
            return Err(Error::EINVAL(
                "a routing id's first octet must not be zero: libzmq reserves a leading zero \
                 for the ids it generates itself"
                    .into(),
            ));
        }
        Ok(RoutingId(bytes))
    }

    /// The bytes, as they go on the wire.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// How many bytes it is.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always `false`: an empty routing id cannot be constructed.
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for RoutingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A routing id is binary and often printable; show it as text where
        // it is and as hex where it is not, because it ends up in log lines.
        if self.0.iter().all(|b| b.is_ascii_graphic()) {
            write!(f, "RoutingId({:?})", String::from_utf8_lossy(&self.0))
        } else {
            write!(f, "RoutingId(")?;
            for byte in &self.0 {
                write!(f, "{byte:02x}")?;
            }
            write!(f, ")")
        }
    }
}

/// What a ROUTER addresses a peer by: the identity the peer announced, or
/// one the ROUTER made up.
///
/// The two are distinguishable on purpose, and libzmq's convention is the
/// leading octet: a peer's own routing id "must not be zero" in its first
/// byte precisely because "v3.0 and later generate 5 bytes, `0` plus a
/// random 32-bit integer" for a peer that named itself nothing
/// (`docs/research/zeromq.md` §4.2). So a [`RoutingId`] is always
/// self-asserted, and a generated key is the other case — which is why this
/// type exists beside it rather than relaxing that rule.
///
/// **The generated form counts rather than randomizes.** libzmq uses a
/// random `u32`; a counter is enough, because the only property the pattern
/// needs is uniqueness among one ROUTER's live peers, and an opaque handle
/// that is also unguessable buys nothing when it never leaves the process
/// except to the peer it names. The difference is named here rather than
/// hidden.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoutingKey(Vec<u8>);

impl RoutingKey {
    /// The key for a peer that announced an `Identity`.
    pub fn announced(identity: &RoutingId) -> RoutingKey {
        RoutingKey(identity.as_bytes().to_vec())
    }

    /// A key for a peer that announced nothing: a zero octet and `n`, the
    /// shape libzmq generates.
    pub fn generated(n: u32) -> RoutingKey {
        let mut bytes = Vec::with_capacity(5);
        bytes.push(0);
        bytes.extend_from_slice(&n.to_be_bytes());
        RoutingKey(bytes)
    }

    /// Takes a key off the wire, where it is just bytes: a ROUTER's peer
    /// sends back whatever the ROUTER handed it.
    pub fn from_wire(bytes: &[u8]) -> RoutingKey {
        RoutingKey(bytes.to_vec())
    }

    /// The bytes, as the routing-id frame carries them.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Whether this key was made up rather than announced — the leading
    /// zero.
    pub fn is_generated(&self) -> bool {
        self.0.first() == Some(&0)
    }
}

impl std::fmt::Debug for RoutingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_generated() {
            write!(f, "RoutingKey(generated ")?;
        } else {
            write!(f, "RoutingKey(")?;
        }
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the two rules a routing id has, and nothing else — it is
    /// self-asserted, so the bytes are the peer's business.
    #[test]
    fn a_routing_id_is_one_to_255_bytes_with_a_nonzero_first_octet() {
        assert_eq!(
            RoutingId::new(b"worker-1").expect("ok").as_bytes(),
            b"worker-1"
        );
        assert_eq!(RoutingId::new(vec![1u8]).expect("ok").len(), 1);
        assert_eq!(
            RoutingId::new(vec![9u8; MAX_ROUTING_ID_BYTES])
                .expect("ok")
                .len(),
            MAX_ROUTING_ID_BYTES
        );

        for bad in [
            Vec::new(),
            vec![0u8, 1, 2],
            vec![7u8; MAX_ROUTING_ID_BYTES + 1],
        ] {
            let err = RoutingId::new(bad).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
        }
    }

    /// Claim: `Debug` is usable in a log line for both a printable id and a
    /// binary one.
    #[test]
    fn debug_shows_text_or_hex() {
        assert_eq!(
            format!("{:?}", RoutingId::new(b"abc").expect("ok")),
            "RoutingId(\"abc\")"
        );
        assert_eq!(
            format!("{:?}", RoutingId::new(vec![0x01, 0xff]).expect("ok")),
            "RoutingId(01ff)"
        );
    }
}
