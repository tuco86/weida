//! Connection metadata: the property dictionary carried by `READY`.
//!
//! ```text
//! metadata = *property
//! property = name value
//! name = short-size 1*255name-char
//! name-char = ALPHA | DIGIT | "-" | "_" | "." | "+"
//! value = 4OCTET *OCTET       ; size in network byte order
//! ```
//!
//! "Metadata names SHALL be case-insensitive", and "note that this size field
//! will mostly not be aligned in memory" - which is the specification's own
//! warning that this is a parse, not a cast.
//!
//! Properties are borrowed, not copied: a decoded [`Metadata`] points into the
//! frame body it came from. Only the property list itself is owned, so
//! decoding a `READY` costs one allocation of at most [`MAX_PROPERTIES`]
//! entries, however much metadata the peer sends.

use crate::error::CommandError;

/// Largest metadata value the four-octet size field can describe: 2^31-1, not
/// 2^32-1. "The value SHALL be 0 to 2,147,483,647 (2^31-1 or INT32_MAX in
/// C/C++) octets of opaque binary data."
pub const MAX_VALUE_LEN: usize = i32::MAX as usize;

/// Most properties one dictionary may carry. A peer chooses the count, and a
/// property costs six octets on the wire but a 32-octet entry here, so without
/// this cap a 1 MiB `READY` of minimal properties would allocate ~5.6 MB before
/// anything is decided. Real `READY` and `INITIATE` metadata is `Socket-Type`,
/// `Identity` and a handful of `X-` properties.
pub const MAX_PROPERTIES: usize = 64;

/// A ZeroMQ socket type, as announced by the `Socket-Type` property.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(missing_docs, reason = "each variant is one socket type, named by it")]
pub enum SocketType {
    Req,
    Rep,
    Dealer,
    Router,
    Pub,
    XPub,
    Sub,
    XSub,
    Push,
    Pull,
    Pair,
    Client,
    Server,
    Radio,
    Dish,
    Scatter,
    Gather,
    Peer,
    Channel,
}

impl SocketType {
    /// The wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            SocketType::Req => "REQ",
            SocketType::Rep => "REP",
            SocketType::Dealer => "DEALER",
            SocketType::Router => "ROUTER",
            SocketType::Pub => "PUB",
            SocketType::XPub => "XPUB",
            SocketType::Sub => "SUB",
            SocketType::XSub => "XSUB",
            SocketType::Push => "PUSH",
            SocketType::Pull => "PULL",
            SocketType::Pair => "PAIR",
            SocketType::Client => "CLIENT",
            SocketType::Server => "SERVER",
            SocketType::Radio => "RADIO",
            SocketType::Dish => "DISH",
            SocketType::Scatter => "SCATTER",
            SocketType::Gather => "GATHER",
            SocketType::Peer => "PEER",
            SocketType::Channel => "CHANNEL",
        }
    }

    /// Parses a wire name. Case-sensitive: the grammar lists the names in
    /// uppercase, and unlike property *names* nothing makes socket types
    /// case-insensitive.
    pub fn parse(name: &[u8]) -> Option<Self> {
        const ALL: [SocketType; 19] = [
            SocketType::Req,
            SocketType::Rep,
            SocketType::Dealer,
            SocketType::Router,
            SocketType::Pub,
            SocketType::XPub,
            SocketType::Sub,
            SocketType::XSub,
            SocketType::Push,
            SocketType::Pull,
            SocketType::Pair,
            SocketType::Client,
            SocketType::Server,
            SocketType::Radio,
            SocketType::Dish,
            SocketType::Scatter,
            SocketType::Gather,
            SocketType::Peer,
            SocketType::Channel,
        ];
        ALL.into_iter().find(|t| t.as_str().as_bytes() == name)
    }

    /// Whether a peer of type `peer` is legal opposite this socket type.
    ///
    /// The specification's table, verbatim. "The peer SHOULD enforce that the
    /// other peer is using a valid socket type" and "SHOULD handle errors by
    /// returning an ERROR command, and then disconnecting the peer" - so this
    /// is the predicate behind that ERROR, and the session driving this codec
    /// decides when to consult it.
    pub const fn accepts(self, peer: SocketType) -> bool {
        use SocketType::{
            Channel, Client, Dealer, Dish, Gather, Pair, Peer, Pub, Pull, Push, Radio, Rep, Req,
            Router, Scatter, Server, Sub, XPub, XSub,
        };
        matches!(
            (self, peer),
            (Req, Rep | Router)
                | (Rep, Req | Dealer)
                | (Dealer, Rep | Dealer | Router)
                | (Router, Req | Dealer | Router)
                | (Pub | XPub, Sub | XSub)
                | (Sub | XSub, Pub | XPub)
                | (Push, Pull)
                | (Pull, Push)
                | (Pair, Pair)
                | (Client, Server)
                | (Server, Client)
                | (Radio, Dish)
                | (Dish, Radio)
                | (Scatter, Gather)
                | (Gather, Scatter)
                | (Peer, Peer)
                | (Channel, Channel)
        )
    }
}

/// A borrowed metadata dictionary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata<'a>(Vec<(&'a str, &'a [u8])>);

impl<'a> Metadata<'a> {
    /// An empty dictionary. Legal: `metadata = *property`, and a `READY` with
    /// no properties at all is a complete NULL handshake.
    pub const fn new() -> Self {
        Metadata(Vec::new())
    }

    /// Adds a property. Duplicate names are kept as sent; nothing in the
    /// specification forbids them, and `get` answers with the first.
    #[must_use]
    pub fn with(mut self, name: &'a str, value: &'a [u8]) -> Self {
        self.0.push((name, value));
        self
    }

    /// Adds the `Socket-Type` property, which a sender SHOULD always announce.
    #[must_use]
    pub fn with_socket_type(self, socket_type: SocketType) -> Self {
        self.with("Socket-Type", socket_type.as_str().as_bytes())
    }

    /// Looks a property up, case-insensitively.
    pub fn get(&self, name: &str) -> Option<&'a [u8]> {
        self.0
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| *v)
    }

    /// The announced socket type, if the property is present and names a type
    /// this version of ZMTP defines.
    pub fn socket_type(&self) -> Option<SocketType> {
        SocketType::parse(self.get("Socket-Type")?)
    }

    /// The properties, in the order they were sent.
    pub fn properties(&self) -> &[(&'a str, &'a [u8])] {
        &self.0
    }

    /// Appends the encoded dictionary to `out`. A dictionary over
    /// [`MAX_PROPERTIES`] is refused before anything is written, because the
    /// decoder would refuse it too.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CommandError> {
        if self.0.len() > MAX_PROPERTIES {
            return Err(CommandError::TooManyProperties(self.0.len()));
        }
        for (name, value) in &self.0 {
            let name = name.as_bytes();
            if name.is_empty() || name.len() > 255 || !name.iter().all(|b| is_name_char(*b)) {
                return Err(CommandError::BadPropertyName);
            }
            if value.len() > MAX_VALUE_LEN {
                return Err(CommandError::ValueTooLong(value.len()));
            }
            out.push(name.len() as u8);
            out.extend_from_slice(name);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
        }
        Ok(())
    }

    /// Decodes a dictionary that occupies all of `input`.
    ///
    /// `input` is the remainder of a command body, so it is already bounded by
    /// the frame cap: a declared value length reaching past the end is a
    /// violation rather than a request for more bytes, and cannot make this
    /// side allocate. A dictionary over [`MAX_PROPERTIES`] is refused at the
    /// first property past the cap, so the parse never walks a hostile body
    /// to its end.
    pub fn decode(input: &'a [u8]) -> Result<Self, CommandError> {
        let mut rest = input;
        let mut properties = Vec::new();
        while !rest.is_empty() {
            if properties.len() == MAX_PROPERTIES {
                return Err(CommandError::TooManyProperties(MAX_PROPERTIES + 1));
            }
            let name_len = usize::from(rest[0]);
            if name_len == 0 {
                return Err(CommandError::BadPropertyName);
            }
            let name = rest.get(1..1 + name_len).ok_or(CommandError::Truncated)?;
            if !name.iter().all(|b| is_name_char(*b)) {
                return Err(CommandError::BadPropertyName);
            }
            // Every name octet is ASCII by the check above.
            let name = std::str::from_utf8(name).expect("a validated property name is ASCII");
            rest = &rest[1 + name_len..];

            let size = rest.get(..4).ok_or(CommandError::Truncated)?;
            let value_len = u32::from_be_bytes([size[0], size[1], size[2], size[3]]) as usize;
            if value_len > MAX_VALUE_LEN {
                return Err(CommandError::ValueTooLong(value_len));
            }
            let value = rest.get(4..4 + value_len).ok_or(CommandError::Truncated)?;
            rest = &rest[4 + value_len..];

            properties.push((name, value));
        }
        Ok(Metadata(properties))
    }
}

/// `name-char = ALPHA | DIGIT | "-" | "_" | "." | "+"`.
const fn is_name_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'+')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_socket_type_property_round_trips() {
        let md = Metadata::new().with_socket_type(SocketType::Req);
        let mut bytes = Vec::new();
        md.encode(&mut bytes).expect("encode");
        assert_eq!(bytes, b"\x0BSocket-Type\x00\x00\x00\x03REQ");
        let back = Metadata::decode(&bytes).expect("decode");
        assert_eq!(back, md);
        assert_eq!(back.socket_type(), Some(SocketType::Req));
    }

    #[test]
    fn names_are_case_insensitive_on_lookup() {
        let bytes = b"\x0BSOCKET-TYPE\x00\x00\x00\x04PULL".to_vec();
        let md = Metadata::decode(&bytes).expect("decode");
        assert_eq!(md.get("Socket-Type"), Some(&b"PULL"[..]));
        assert_eq!(md.socket_type(), Some(SocketType::Pull));
        assert_eq!(md.get("socket-type"), Some(&b"PULL"[..]));
        assert_eq!(md.get("Identity"), None);
    }

    #[test]
    fn an_empty_dictionary_is_legal_in_both_directions() {
        let mut bytes = Vec::new();
        Metadata::new().encode(&mut bytes).expect("encode");
        assert!(bytes.is_empty());
        assert_eq!(Metadata::decode(&[]).expect("decode"), Metadata::new());
    }

    #[test]
    fn a_zero_length_value_is_legal() {
        let md = Metadata::new().with("X-Empty", b"");
        let mut bytes = Vec::new();
        md.encode(&mut bytes).expect("encode");
        assert_eq!(bytes, b"\x07X-Empty\x00\x00\x00\x00");
        assert_eq!(Metadata::decode(&bytes).expect("decode"), md);
    }

    #[test]
    fn several_properties_keep_their_order() {
        let md = Metadata::new()
            .with_socket_type(SocketType::Router)
            .with("Identity", b"worker-7")
            .with("Resource", b"system/name-service");
        let mut bytes = Vec::new();
        md.encode(&mut bytes).expect("encode");
        let back = Metadata::decode(&bytes).expect("decode");
        assert_eq!(back, md);
        assert_eq!(
            back.properties()
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>(),
            ["Socket-Type", "Identity", "Resource"]
        );
    }

    #[test]
    fn a_truncated_value_is_a_violation_not_a_short_read() {
        // The dictionary is inside a complete frame body, so there is nothing
        // more to wait for: 4 declared octets, 2 present.
        let bytes = b"\x01A\x00\x00\x00\x04ab";
        assert_eq!(Metadata::decode(bytes), Err(CommandError::Truncated));
        // A name that runs past the end, likewise.
        assert_eq!(Metadata::decode(b"\x09short"), Err(CommandError::Truncated));
        // And a size field cut in half.
        assert_eq!(
            Metadata::decode(b"\x01A\x00\x00"),
            Err(CommandError::Truncated)
        );
    }

    #[test]
    fn a_huge_declared_value_allocates_nothing() {
        // 0x7FFFFFFF is the largest legal value length; the body holds none of
        // it. The refusal must come from the length check, not from a failed
        // reservation.
        let bytes = b"\x01A\x7F\xFF\xFF\xFFzz";
        assert_eq!(Metadata::decode(bytes), Err(CommandError::Truncated));
        // Above the documented maximum it is named as such.
        let bytes = b"\x01A\x80\x00\x00\x00zz";
        assert_eq!(
            Metadata::decode(bytes),
            Err(CommandError::ValueTooLong(0x8000_0000))
        );
    }

    #[test]
    fn a_malformed_property_name_is_rejected_both_ways() {
        assert_eq!(
            Metadata::decode(b"\x00\x00\x00\x00\x00"),
            Err(CommandError::BadPropertyName),
            "a zero-length name is not valid"
        );
        assert_eq!(
            Metadata::decode(b"\x03A B\x00\x00\x00\x00"),
            Err(CommandError::BadPropertyName)
        );
        let mut out = Vec::new();
        assert_eq!(
            Metadata::new().with("", b"v").encode(&mut out),
            Err(CommandError::BadPropertyName)
        );
        assert_eq!(
            Metadata::new().with("has space", b"v").encode(&mut out),
            Err(CommandError::BadPropertyName)
        );
    }

    #[test]
    fn the_legal_peer_table_is_symmetric_where_the_specification_says_so() {
        use SocketType::*;
        for (a, b) in [
            (Req, Rep),
            (Req, Router),
            (Rep, Dealer),
            (Dealer, Router),
            (Pub, Sub),
            (XPub, XSub),
            (Push, Pull),
            (Pair, Pair),
            (Client, Server),
            (Radio, Dish),
            (Scatter, Gather),
            (Peer, Peer),
            (Channel, Channel),
        ] {
            assert!(a.accepts(b), "{a:?} accepts {b:?}");
            assert!(b.accepts(a), "{b:?} accepts {a:?}");
        }
        // The mismatches a session actually has to refuse.
        assert!(!Push.accepts(Sub));
        assert!(!Req.accepts(Req));
        assert!(!Pub.accepts(Pull));
        assert!(!Client.accepts(Client));
    }

    #[test]
    fn every_socket_type_name_parses_back() {
        for name in [
            "REQ", "REP", "DEALER", "ROUTER", "PUB", "XPUB", "SUB", "XSUB", "PUSH", "PULL", "PAIR",
            "CLIENT", "SERVER", "RADIO", "DISH", "SCATTER", "GATHER", "PEER", "CHANNEL",
        ] {
            let parsed = SocketType::parse(name.as_bytes()).expect(name);
            assert_eq!(parsed.as_str(), name);
        }
        assert_eq!(SocketType::parse(b"req"), None);
        assert_eq!(SocketType::parse(b""), None);
        assert_eq!(SocketType::parse(b"XREQ"), None);
    }

    #[test]
    fn a_mebibyte_of_minimal_properties_is_refused_at_the_cap() {
        // 174762 six-octet properties fill a 1 MiB body. The refusal must come
        // at the first property past the cap, so the parse stops after 65 of
        // them instead of building a list of all of them.
        let body = b"\x01a\x00\x00\x00\x00".repeat(174_762);
        assert_eq!(
            Metadata::decode(&body),
            Err(CommandError::TooManyProperties(MAX_PROPERTIES + 1))
        );
    }

    #[test]
    fn exactly_max_properties_decode_and_round_trip() {
        let names: Vec<String> = (0..MAX_PROPERTIES).map(|i| format!("X-{i}")).collect();
        let mut md = Metadata::new();
        for name in &names {
            md = md.with(name, b"v");
        }
        let mut bytes = Vec::new();
        md.encode(&mut bytes).expect("the cap itself encodes");
        let back = Metadata::decode(&bytes).expect("the cap itself decodes");
        assert_eq!(back.properties().len(), MAX_PROPERTIES);
        assert_eq!(back, md);
    }

    #[test]
    fn one_property_past_the_cap_is_refused_by_encode_before_any_output() {
        let names: Vec<String> = (0..=MAX_PROPERTIES).map(|i| format!("X-{i}")).collect();
        let mut md = Metadata::new();
        for name in &names {
            md = md.with(name, b"v");
        }
        let mut out = Vec::new();
        assert_eq!(
            md.encode(&mut out),
            Err(CommandError::TooManyProperties(MAX_PROPERTIES + 1))
        );
        assert!(out.is_empty(), "a refused dictionary writes nothing");
    }
}
