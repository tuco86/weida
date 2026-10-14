//! The 8-octet SP protocol header of the TCP mapping.
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |      0x00     |      0x53     |      0x50     |    version    |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |             type              |           reserved            |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! ```
//!
//! Both sides send it immediately after the TCP handshake and both MUST wait
//! for the peer's before proceeding; a header whose first four octets differ,
//! or whose reserved field is nonzero, MUST close the connection
//! [rfc-tcp §2]. There is no other handshake: no round trip, no negotiation,
//! no capability exchange [rfc-tcp §2], which is why this module has no state
//! machine - two encodes and two decodes are the whole connection setup.
//!
//! The type field is a 12-bit protocol ID plus a 4-bit endpoint role
//! [rfc-ids §1]. The RFC assigns the protocol IDs and leaves the roles to the
//! per-protocol RFCs, which never published them, so the role halves here come
//! from NNG's registry - `NNI_PROTO(major, minor) = major * 16 + minor`
//! [nng-src `core/protocol.h`]. That gap is recorded in
//! `docs/research/nanomsg-nng.md` §3, not hidden in a comment.

use crate::error::HeaderError;

/// Octets in the protocol header.
pub const HEADER_LEN: usize = 8;

/// The first three octets: `0x00 'S' 'P'`. "The fact that the first byte of
/// the protocol header is binary zero eliminates any text-based protocols that
/// were accidentally connected to the endpoint" [rfc-tcp §2].
pub const MAGIC: [u8; 3] = [0x00, 0x53, 0x50];

/// The only SP version this codec speaks, and the only one the mapping
/// defines [rfc-tcp §2].
pub const VERSION: u8 = 0;

/// Which SP protocol an endpoint speaks, and in which role.
///
/// The numbers are the wire's: a 12-bit protocol ID and a 4-bit role
/// [rfc-ids §1], as `major * 16 + minor` [nng-src].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EndpointType {
    /// `0x10`, protocol 1 role 0. No protocol header; the legacy interoperable
    /// form [nanomsg-nng §4].
    PairV0,
    /// `0x11`, protocol 1 role 1. Carries the 32-bit hop count of
    /// [`crate::pair`] [nanomsg-nng §3].
    PairV1,
    /// `0x20`, protocol 2 role 0. Sends only [nanomsg-nng §4].
    Pub,
    /// `0x21`, protocol 2 role 1. Receives only, and filters locally
    /// [nanomsg-nng §4].
    Sub,
    /// `0x30`, protocol 3 role 0.
    Req,
    /// `0x31`, protocol 3 role 1.
    Rep,
    /// `0x50`, protocol 5 role 0.
    Push,
    /// `0x51`, protocol 5 role 1.
    Pull,
    /// `0x62`, protocol 6 role 2. Roles 0 and 1 are retired [nng-src].
    Surveyor,
    /// `0x63`, protocol 6 role 3.
    Respondent,
    /// `0x70`, protocol 7 role 0.
    Bus,
}

impl EndpointType {
    /// The 16-bit type field as it appears on the wire.
    pub const fn id(self) -> u16 {
        match self {
            EndpointType::PairV0 => 0x10,
            EndpointType::PairV1 => 0x11,
            EndpointType::Pub => 0x20,
            EndpointType::Sub => 0x21,
            EndpointType::Req => 0x30,
            EndpointType::Rep => 0x31,
            EndpointType::Push => 0x50,
            EndpointType::Pull => 0x51,
            EndpointType::Surveyor => 0x62,
            EndpointType::Respondent => 0x63,
            EndpointType::Bus => 0x70,
        }
    }

    /// The type this one may talk to. A socket "can send and/or receive only
    /// as that protocol permits" [nanomsg-nng §2], and every SP protocol has
    /// exactly one legal opposite - BUS and PAIR being their own.
    pub const fn peer(self) -> EndpointType {
        match self {
            EndpointType::PairV0 => EndpointType::PairV0,
            EndpointType::PairV1 => EndpointType::PairV1,
            EndpointType::Pub => EndpointType::Sub,
            EndpointType::Sub => EndpointType::Pub,
            EndpointType::Req => EndpointType::Rep,
            EndpointType::Rep => EndpointType::Req,
            EndpointType::Push => EndpointType::Pull,
            EndpointType::Pull => EndpointType::Push,
            EndpointType::Surveyor => EndpointType::Respondent,
            EndpointType::Respondent => EndpointType::Surveyor,
            EndpointType::Bus => EndpointType::Bus,
        }
    }

    /// Whether this protocol prefixes its bodies with a tag stack
    /// ([`crate::backtrace`]) [nanomsg-nng §3].
    pub const fn has_backtrace(self) -> bool {
        matches!(
            self,
            EndpointType::Req
                | EndpointType::Rep
                | EndpointType::Surveyor
                | EndpointType::Respondent
        )
    }

    /// Reads a type field. `None` for a value no SP protocol claims.
    pub const fn from_id(id: u16) -> Option<EndpointType> {
        Some(match id {
            0x10 => EndpointType::PairV0,
            0x11 => EndpointType::PairV1,
            0x20 => EndpointType::Pub,
            0x21 => EndpointType::Sub,
            0x30 => EndpointType::Req,
            0x31 => EndpointType::Rep,
            0x50 => EndpointType::Push,
            0x51 => EndpointType::Pull,
            0x62 => EndpointType::Surveyor,
            0x63 => EndpointType::Respondent,
            0x70 => EndpointType::Bus,
            _ => return None,
        })
    }

    /// The protocol ID, the upper 12 bits [rfc-ids §1].
    pub const fn protocol(self) -> u16 {
        self.id() >> 4
    }

    /// The endpoint role, the lower 4 bits [rfc-ids §1].
    pub const fn role(self) -> u16 {
        self.id() & 0x0f
    }
}

/// A decoded SP protocol header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolHeader {
    /// The version octet. Always [`VERSION`] for a header this codec accepts.
    pub version: u8,
    /// What the peer is.
    pub endpoint: EndpointType,
}

impl ProtocolHeader {
    /// The header this side sends for `endpoint`.
    pub const fn new(endpoint: EndpointType) -> ProtocolHeader {
        ProtocolHeader {
            version: VERSION,
            endpoint,
        }
    }

    /// The eight octets, ready for the wire.
    pub const fn encode(&self) -> [u8; HEADER_LEN] {
        let id = self.endpoint.id();
        [
            MAGIC[0],
            MAGIC[1],
            MAGIC[2],
            self.version,
            (id >> 8) as u8,
            id as u8,
            0,
            0,
        ]
    }

    /// Reads a peer's header from the front of `input`.
    ///
    /// Every rule the mapping states is checked here, because every one of
    /// them closes the connection and none of them can be retried: magic,
    /// version, reserved zeroes [rfc-tcp §2]. Nothing is allocated.
    pub fn decode(input: &[u8]) -> Result<ProtocolHeader, HeaderError> {
        let Some(head) = input.get(..HEADER_LEN) else {
            return Err(HeaderError::Incomplete);
        };
        if head[..3] != MAGIC {
            return Err(HeaderError::BadMagic([head[0], head[1], head[2]]));
        }
        if head[3] != VERSION {
            return Err(HeaderError::UnsupportedVersion(head[3]));
        }
        let reserved = u16::from_be_bytes([head[6], head[7]]);
        if reserved != 0 {
            return Err(HeaderError::ReservedNotZero(reserved));
        }
        let id = u16::from_be_bytes([head[4], head[5]]);
        let endpoint = EndpointType::from_id(id).ok_or(HeaderError::UnknownEndpoint(id))?;
        Ok(ProtocolHeader {
            version: head[3],
            endpoint,
        })
    }

    /// Whether a peer speaking `self` may talk to a local `local` endpoint.
    ///
    /// The mapping says "incompatible peers must disconnect"
    /// [nanomsg-nng §1] without saying what compatible means; the pairing
    /// rules come from the protocols themselves [nanomsg-nng §2, §4].
    pub const fn accepts(&self, local: EndpointType) -> bool {
        local.peer().id() == self.endpoint.id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_round_trips_for_every_endpoint_type() {
        for endpoint in [
            EndpointType::PairV0,
            EndpointType::PairV1,
            EndpointType::Pub,
            EndpointType::Sub,
            EndpointType::Req,
            EndpointType::Rep,
            EndpointType::Push,
            EndpointType::Pull,
            EndpointType::Surveyor,
            EndpointType::Respondent,
            EndpointType::Bus,
        ] {
            let bytes = ProtocolHeader::new(endpoint).encode();
            let back = ProtocolHeader::decode(&bytes).expect("decode");
            assert_eq!(back.endpoint, endpoint);
            assert_eq!(back.version, VERSION);
            // The id is a protocol number and a role, not an opaque tag
            // [rfc-ids §1].
            assert_eq!(endpoint.protocol() * 16 + endpoint.role(), endpoint.id());
        }
    }

    #[test]
    fn every_rule_of_the_mapping_is_checked() {
        let good = ProtocolHeader::new(EndpointType::Req).encode();
        assert!(ProtocolHeader::decode(&good[..7]).is_err());
        assert_eq!(
            ProtocolHeader::decode(&good[..7]),
            Err(HeaderError::Incomplete)
        );

        let mut magic = good;
        magic[2] = 0x51;
        assert_eq!(
            ProtocolHeader::decode(&magic),
            Err(HeaderError::BadMagic([0x00, 0x53, 0x51]))
        );

        let mut version = good;
        version[3] = 1;
        assert_eq!(
            ProtocolHeader::decode(&version),
            Err(HeaderError::UnsupportedVersion(1))
        );

        let mut reserved = good;
        reserved[7] = 1;
        assert_eq!(
            ProtocolHeader::decode(&reserved),
            Err(HeaderError::ReservedNotZero(1))
        );

        let mut unknown = good;
        unknown[5] = 0x99;
        assert_eq!(
            ProtocolHeader::decode(&unknown),
            Err(HeaderError::UnknownEndpoint(0x0099))
        );
    }

    #[test]
    fn pairing_is_symmetric_and_only_the_legal_opposite_is_accepted() {
        let req = ProtocolHeader::new(EndpointType::Req);
        let rep = ProtocolHeader::new(EndpointType::Rep);
        assert!(req.accepts(EndpointType::Rep));
        assert!(rep.accepts(EndpointType::Req));
        assert!(!req.accepts(EndpointType::Req));
        assert!(!req.accepts(EndpointType::Pull));
        // BUS and PAIR talk to themselves [nanomsg-nng §4].
        assert!(ProtocolHeader::new(EndpointType::Bus).accepts(EndpointType::Bus));
        assert!(ProtocolHeader::new(EndpointType::PairV1).accepts(EndpointType::PairV1));
        // PAIR v0 and v1 are different protocols on the wire, and the version
        // difference is in the type field, not the version octet
        // [nng-src, rfc-ids §1].
        assert!(!ProtocolHeader::new(EndpointType::PairV1).accepts(EndpointType::PairV0));
    }

    #[test]
    fn trailing_bytes_after_a_header_are_left_for_the_caller() {
        let mut input = ProtocolHeader::new(EndpointType::Pub).encode().to_vec();
        input.extend_from_slice(b"a message follows");
        let header = ProtocolHeader::decode(&input).expect("decode");
        assert_eq!(header.endpoint, EndpointType::Pub);
    }
}
