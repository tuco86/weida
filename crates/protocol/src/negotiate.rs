//! Version and capability negotiation.
//!
//! Negotiation is an exchange during connection establishment, not a
//! long-lived control stream (master doc §13, §11). Both sides run this same
//! function against their own and the peer's HELLO, so both reach the same
//! verdict without a round trip.

use std::fmt;

use crate::header::{GuaranteeSet, HeaderError, Hello};

/// Capability code `1`, `datagram`: the sender reads QUIC DATAGRAM frames and
/// FLOW streams (`docs/PROTOCOL.md` §6.1). A FLOW stream or a datagram is
/// sent only when both HELLOs list it.
pub const CAPABILITY_DATAGRAM: u64 = 1;

/// Capability code `2`, `path_report`: the sender reads, and will send, one
/// REPORT stream (`docs/PROTOCOL.md` §6.1, §6.10). A REPORT stream is sent
/// only when both HELLOs list it.
pub const CAPABILITY_PATH_REPORT: u64 = 2;

/// The negotiated parameters of a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Agreed {
    /// Effective wire protocol version: the highest version both sides support.
    pub version: u64,
    /// Largest header the **peer** accepts. Outgoing headers must not exceed
    /// it; it is not a limit on what we receive.
    pub send_max_header_bytes: u64,
    /// The effective guarantee set: the weaker of the two offers, dimension
    /// by dimension (`docs/PROTOCOL.md` §2.3 step 5).
    ///
    /// Between two v0 peers this is [`GuaranteeSet::CORE`], because neither
    /// declares anything and an absent declaration means `core`.
    pub guarantees: GuaranteeSet,
    /// Both HELLOs listed [`CAPABILITY_DATAGRAM`], so FLOW streams and
    /// datagrams may be sent on this connection.
    pub datagrams: bool,
    /// Both HELLOs listed [`CAPABILITY_PATH_REPORT`], so each side sends one
    /// REPORT stream on this connection.
    pub path_report: bool,
}

impl Agreed {
    /// Refuses an encoded header of `len` bytes that the peer would not read.
    ///
    /// `send_max_header_bytes` is the peer's `max_header_bytes`; a longer
    /// header makes its preamble parser close the connection
    /// (`docs/PROTOCOL.md` §3.2), so a sender checks the length it is about to
    /// write rather than learn it from the close.
    pub fn check_header_len(&self, len: usize) -> Result<(), HeaderError> {
        if len as u64 > self.send_max_header_bytes {
            return Err(HeaderError::HeaderTooLong {
                len,
                max: self.send_max_header_bytes,
            });
        }
        Ok(())
    }
}

/// Why negotiation failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NegotiateError {
    /// The version sets do not intersect.
    NoCommonVersion,
    /// The peer requires a capability this implementation does not support.
    UnsupportedRequiredCapability(u64),
    /// An unordered dimension was declared differently by the two sides, so
    /// there is no weaker of the two to take.
    IncomparableGuarantee {
        /// Dimension that disagreed.
        dimension: &'static str,
    },
    /// The effective set does not reach what one side requires. There is no
    /// downgrade path: the handshake fails instead
    /// ([decisions/0006](../../../docs/decisions/0006-guarantee-sets.md) §4.4).
    GuaranteeNotOffered {
        /// `true` when it was the peer's requirement that went unmet.
        peers_requirement: bool,
    },
}

impl fmt::Display for NegotiateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NegotiateError::NoCommonVersion => {
                f.write_str("no wire protocol version is supported by both peers")
            }
            NegotiateError::UnsupportedRequiredCapability(c) => {
                write!(f, "peer requires unsupported capability {c}")
            }
            NegotiateError::IncomparableGuarantee { dimension } => {
                write!(
                    f,
                    "the two peers declare different {dimension}, which is not an ordered dimension"
                )
            }
            NegotiateError::GuaranteeNotOffered { peers_requirement } => {
                let who = if *peers_requirement { "peer" } else { "local" };
                write!(
                    f,
                    "the effective guarantee set does not reach the {who} requirement"
                )
            }
        }
    }
}

impl std::error::Error for NegotiateError {}

impl From<NegotiateError> for weida_core::Error {
    fn from(e: NegotiateError) -> weida_core::Error {
        weida_core::Error::Negotiation(e.to_string())
    }
}

/// Negotiates a connection from the two HELLO headers.
///
/// The effective version is the maximum of the intersection of the two version
/// lists. Every capability the peer marks as required must appear in our own
/// supported set, so a peer requiring a code we did not list fails. The codes
/// defined are [`CAPABILITY_DATAGRAM`] and [`CAPABILITY_PATH_REPORT`], each
/// agreed when both sides list it.
///
/// The guarantee set is the weaker of the two offers, and both sides'
/// requirements must be reachable by it — there is no downgrade path
/// (`docs/PROTOCOL.md` §2.3 steps 5 and 6). Two v0 peers declare nothing, so
/// the effective set is `core` and the two steps are no-ops.
pub fn negotiate(ours: &Hello, theirs: &Hello) -> Result<Agreed, NegotiateError> {
    let version = theirs
        .versions
        .iter()
        .copied()
        .filter(|v| ours.versions.contains(v))
        .max()
        .ok_or(NegotiateError::NoCommonVersion)?;

    if let Some(missing) = theirs
        .required_capabilities
        .iter()
        .copied()
        .find(|c| !ours.capabilities.contains(c))
    {
        return Err(NegotiateError::UnsupportedRequiredCapability(missing));
    }

    let guarantees = ours
        .offered()
        .intersect(&theirs.offered())
        .map_err(|dimension| NegotiateError::IncomparableGuarantee { dimension })?;
    for (required, peers_requirement) in [(theirs.required(), true), (ours.required(), false)] {
        if !guarantees.reaches(&required) {
            return Err(NegotiateError::GuaranteeNotOffered { peers_requirement });
        }
    }

    Ok(Agreed {
        version,
        send_max_header_bytes: theirs.max_header_bytes,
        guarantees,
        datagrams: ours.capabilities.contains(&CAPABILITY_DATAGRAM)
            && theirs.capabilities.contains(&CAPABILITY_DATAGRAM),
        path_report: ours.capabilities.contains(&CAPABILITY_PATH_REPORT)
            && theirs.capabilities.contains(&CAPABILITY_PATH_REPORT),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::header::{
        Acknowledgement, Backpressure, Deduplication, Durability, OrderingMode, ProducerNaming,
    };

    fn hello(versions: &[u64], caps: &[u64], required: &[u64], max_header: u64) -> Hello {
        Hello {
            versions: versions.to_vec(),
            max_header_bytes: max_header,
            max_transfers: 1024,
            capabilities: caps.to_vec(),
            required_capabilities: required.to_vec(),
            guarantees_offered: None,
            guarantees_required: None,
        }
    }

    /// A v0 HELLO that declares `offered` and `required` explicitly.
    fn declaring(offered: GuaranteeSet, required: GuaranteeSet) -> Hello {
        Hello {
            guarantees_offered: Some(offered),
            guarantees_required: Some(required),
            ..Hello::v0(16384, 1024)
        }
    }

    #[test]
    fn v0_peers_agree_on_version_zero() {
        let ours = Hello::v0(16384, 1024);
        let theirs = Hello::v0(4096, 512);
        let agreed = negotiate(&ours, &theirs).unwrap();
        assert_eq!(agreed.version, 0);
        // The send cap comes from the peer's advertised receive cap.
        assert_eq!(agreed.send_max_header_bytes, 4096);
    }

    #[test]
    fn a_header_longer_than_the_peer_accepts_is_refused() {
        let agreed = negotiate(&Hello::v0(16384, 1024), &Hello::v0(4096, 512)).unwrap();
        assert_eq!(agreed.check_header_len(4096), Ok(()));
        assert_eq!(
            agreed.check_header_len(4097),
            Err(HeaderError::HeaderTooLong {
                len: 4097,
                max: 4096
            })
        );
    }

    #[test]
    fn the_highest_common_version_wins() {
        let ours = hello(&[0, 1, 2, 5], &[], &[], 16384);
        let theirs = hello(&[1, 2, 3], &[], &[], 16384);
        assert_eq!(negotiate(&ours, &theirs).unwrap().version, 2);
    }

    #[test]
    fn version_order_in_the_list_does_not_matter() {
        let ours = hello(&[5, 0, 2], &[], &[], 16384);
        let theirs = hello(&[2, 5], &[], &[], 16384);
        assert_eq!(negotiate(&ours, &theirs).unwrap().version, 5);
    }

    #[test]
    fn an_empty_intersection_fails() {
        let ours = hello(&[0], &[], &[], 16384);
        for theirs in [hello(&[99], &[], &[], 16384), hello(&[], &[], &[], 16384)] {
            assert_eq!(
                negotiate(&ours, &theirs),
                Err(NegotiateError::NoCommonVersion)
            );
        }
    }

    #[test]
    fn a_required_capability_we_lack_fails() {
        let ours = Hello::v0(16384, 1024);
        let theirs = hello(&[0], &[7], &[7], 16384);
        assert_eq!(
            negotiate(&ours, &theirs),
            Err(NegotiateError::UnsupportedRequiredCapability(7))
        );
    }

    #[test]
    fn a_required_capability_we_have_succeeds() {
        let ours = hello(&[0], &[7, 9], &[], 16384);
        let theirs = hello(&[0], &[7], &[7], 8192);
        assert_eq!(
            negotiate(&ours, &theirs).unwrap(),
            Agreed {
                version: 0,
                send_max_header_bytes: 8192,
                guarantees: GuaranteeSet::CORE,
                datagrams: false,
                path_report: false,
            }
        );
    }

    #[test]
    fn datagrams_are_agreed_only_when_both_list_the_code() {
        let with = hello(&[0], &[CAPABILITY_DATAGRAM], &[], 16384);
        let without = Hello::v0(16384, 1024);
        assert!(negotiate(&with, &with).unwrap().datagrams);
        assert!(!negotiate(&with, &without).unwrap().datagrams);
        assert!(!negotiate(&without, &with).unwrap().datagrams);
    }

    #[test]
    fn path_reports_are_agreed_only_when_both_list_the_code() {
        let with = hello(&[0], &[CAPABILITY_PATH_REPORT], &[], 16384);
        let without = hello(&[0], &[CAPABILITY_DATAGRAM], &[], 16384);
        assert!(negotiate(&with, &with).unwrap().path_report);
        assert!(!negotiate(&with, &without).unwrap().path_report);
        assert!(!negotiate(&without, &with).unwrap().path_report);
    }

    #[test]
    fn two_v0_peers_negotiate_core_unchanged() {
        // The property the whole "spec ahead of code" arrangement rests on:
        // a peer that declares nothing still gets exactly `core`.
        let agreed = negotiate(&Hello::v0(16384, 1024), &Hello::v0(16384, 1024)).unwrap();
        assert_eq!(agreed.guarantees, GuaranteeSet::CORE);
        assert!(agreed.guarantees.is_core());
    }

    #[test]
    fn the_effective_set_is_the_weaker_offer_per_dimension() {
        let strong = GuaranteeSet {
            ordering: OrderingMode::PerProducerReassemble,
            deduplication: Deduplication::Bounded,
            dedup_window_ms: Some(60_000),
            control_isolated: true,
            ..GuaranteeSet::CORE
        };
        let weaker = GuaranteeSet {
            ordering: OrderingMode::PerProducerDetect,
            deduplication: Deduplication::Bounded,
            dedup_window_ms: Some(5_000),
            control_isolated: false,
            ..GuaranteeSet::CORE
        };
        let agreed = negotiate(
            &declaring(strong, GuaranteeSet::CORE),
            &declaring(weaker, GuaranteeSet::CORE),
        )
        .unwrap();
        assert_eq!(agreed.guarantees.ordering, OrderingMode::PerProducerDetect);
        // A shorter window is the weaker promise.
        assert_eq!(agreed.guarantees.dedup_window_ms, Some(5_000));
        assert!(!agreed.guarantees.control_isolated);
    }

    #[test]
    fn a_requested_level_the_peer_does_not_offer_fails() {
        let wants_ordering = GuaranteeSet {
            ordering: OrderingMode::PerProducerDetect,
            ..GuaranteeSet::CORE
        };
        // We offer and require detect ordering; the peer offers `core`.
        let err = negotiate(
            &declaring(wants_ordering, wants_ordering),
            &Hello::v0(16384, 1024),
        )
        .unwrap_err();
        assert_eq!(
            err,
            NegotiateError::GuaranteeNotOffered {
                peers_requirement: false
            }
        );
        // And symmetrically, when it is the peer that requires it.
        let err = negotiate(
            &Hello::v0(16384, 1024),
            &declaring(wants_ordering, wants_ordering),
        )
        .unwrap_err();
        assert_eq!(
            err,
            NegotiateError::GuaranteeNotOffered {
                peers_requirement: true
            }
        );
    }

    #[test]
    fn unordered_dimensions_must_match_exactly() {
        for (ours, theirs, dimension) in [
            (
                GuaranteeSet {
                    backpressure: Backpressure::Drop,
                    ..GuaranteeSet::CORE
                },
                GuaranteeSet::CORE,
                "backpressure",
            ),
            (
                GuaranteeSet {
                    producer_naming: ProducerNaming::Stable,
                    ..GuaranteeSet::CORE
                },
                GuaranteeSet::CORE,
                "producer naming",
            ),
            (
                GuaranteeSet {
                    acknowledgement: Acknowledgement::Stored,
                    durability: Some(Durability::Flushed),
                    ..GuaranteeSet::CORE
                },
                GuaranteeSet {
                    acknowledgement: Acknowledgement::Stored,
                    durability: Some(Durability::Written),
                    ..GuaranteeSet::CORE
                },
                "durability",
            ),
        ] {
            let err = negotiate(
                &declaring(ours, GuaranteeSet::CORE),
                &declaring(theirs, GuaranteeSet::CORE),
            )
            .unwrap_err();
            assert_eq!(err, NegotiateError::IncomparableGuarantee { dimension });
        }
    }

    #[test]
    fn a_weakened_acknowledgement_drops_the_axes_it_cannot_carry() {
        let stored = GuaranteeSet {
            acknowledgement: Acknowledgement::Stored,
            durability: Some(Durability::Flushed),
            ..GuaranteeSet::CORE
        };
        let agreed = negotiate(
            &declaring(stored, GuaranteeSet::CORE),
            &Hello::v0(16384, 1024),
        )
        .unwrap();
        // The peer offers only a transport receipt, so the durability axis
        // has nothing left to qualify and the set stays legal.
        assert_eq!(
            agreed.guarantees.acknowledgement,
            Acknowledgement::TransportReceipt
        );
        assert_eq!(agreed.guarantees.durability, None);
        assert!(agreed.guarantees.is_core());
    }

    #[test]
    fn an_axis_the_peer_never_declared_is_not_reached() {
        let stored = |durability| GuaranteeSet {
            acknowledgement: Acknowledgement::Stored,
            durability,
            ..GuaranteeSet::CORE
        };
        let replicated = |replicas| GuaranteeSet {
            acknowledgement: Acknowledgement::Replicated,
            replicas,
            ..GuaranteeSet::CORE
        };
        for (wanted, undeclared) in [
            (stored(Some(Durability::Flushed)), stored(None)),
            (replicated(Some(3)), replicated(None)),
        ] {
            let err = negotiate(
                &declaring(wanted, wanted),
                &declaring(undeclared, GuaranteeSet::CORE),
            )
            .unwrap_err();
            assert_eq!(
                err,
                NegotiateError::GuaranteeNotOffered {
                    peers_requirement: false
                }
            );
            // The peer's side of the same requirement.
            let err = negotiate(
                &declaring(undeclared, GuaranteeSet::CORE),
                &declaring(wanted, wanted),
            )
            .unwrap_err();
            assert_eq!(
                err,
                NegotiateError::GuaranteeNotOffered {
                    peers_requirement: true
                }
            );
        }
    }

    #[test]
    fn an_axis_both_sides_declared_survives_the_intersection() {
        let stored = GuaranteeSet {
            acknowledgement: Acknowledgement::Stored,
            durability: Some(Durability::Flushed),
            ..GuaranteeSet::CORE
        };
        let agreed = negotiate(&declaring(stored, stored), &declaring(stored, stored)).unwrap();
        assert_eq!(agreed.guarantees, stored);
    }

    #[test]
    fn optional_peer_capabilities_are_ignored() {
        let ours = Hello::v0(16384, 1024);
        let theirs = hello(&[0], &[1, 2, 3], &[], 16384);
        assert!(negotiate(&ours, &theirs).is_ok());
    }

    #[test]
    fn version_mismatch_is_checked_before_capabilities() {
        let ours = Hello::v0(16384, 1024);
        let theirs = hello(&[99], &[], &[42], 16384);
        assert_eq!(
            negotiate(&ours, &theirs),
            Err(NegotiateError::NoCommonVersion)
        );
    }

    #[test]
    fn negotiation_is_symmetric_for_the_agreed_version() {
        let a = hello(&[0, 1], &[], &[], 1000);
        let b = hello(&[1, 2], &[], &[], 2000);
        let ab = negotiate(&a, &b).unwrap();
        let ba = negotiate(&b, &a).unwrap();
        assert_eq!(ab.version, ba.version);
        assert_eq!(ab.send_max_header_bytes, 2000);
        assert_eq!(ba.send_max_header_bytes, 1000);
    }

    #[test]
    fn errors_become_negotiation_errors() {
        let e: weida_core::Error = NegotiateError::NoCommonVersion.into();
        assert!(matches!(e, weida_core::Error::Negotiation(_)));
    }
}
