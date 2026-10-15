//! The message: seven kinds of section in one fixed order.
//!
//! ```text
//!                                Bare Message
//!                                      |
//!               .----------------------+---------------------.
//!               |                                            |
//! +--------+-------------+-------------+------------+--------------+--------------+--------+
//! | header | delivery-   | message-    | properties | application- | application- | footer |
//! |        | annotations | annotations |            | properties   | data         |        |
//! +--------+-------------+-------------+------------+--------------+--------------+--------+
//! |                                                                                        |
//! '--------------------------------------+-----------------------------------------------  '
//!                                        |
//!                                 Annotated Message
//! ```
//!
//! ```text
//! header 0x70   delivery-annotations 0x71   message-annotations 0x72
//! properties 0x73   application-properties 0x74
//! data 0x75   amqp-sequence 0x76   amqp-value 0x77   footer 0x78
//! ```
//!
//! The *order is normative*, not conventional: Part 3 §3.2 lists the sections
//! and [`Message::decode`] refuses one that arrives out of it with
//! [`DecodeError::SectionOutOfOrder`]. A decoder that sorted them instead
//! would be silently accepting a message whose bare part it could not have
//! hashed.
//!
//! # Immutability is the reason the sections are separate
//!
//! The *bare message* — `properties`, `application-properties` and the body —
//! "is immutable within the AMQP network. That is, none of the sections can
//! be changed by any node acting as an AMQP intermediary ... The exact
//! encoding of sections of the bare message MUST NOT be modified. This
//! preserves message hashes, HMACs and signatures based on the binary
//! encoding of the bare message." An intermediary may add or replace the
//! *annotation* sections around it and nothing else. That is why
//! [`Decoded::bare`] exists: the octets a signature is computed over are a
//! contiguous slice of what arrived, and this decoder reports that slice
//! rather than re-encoding the sections and hoping the result matches.
//!
//! # The body
//!
//! Part 3 §3.2 gives the body three choices — "one or more `data` sections,
//! one or more `amqp-sequence` sections, or a single `amqp-value` section" —
//! and mixing them is [`DecodeError::MixedBodySections`]. It offers no fourth
//! choice for "none", so a message with no body section is not conformant;
//! see [`Body::Empty`] for why this decoder reports one anyway.

use crate::decode;
use crate::encode;
use crate::error::{DecodeError, EncodeError};
use crate::fields::Fields;
use crate::limits::Limits;
use crate::value::{Descriptor, Value};

/// The nine section descriptors, numeric and symbolic, in the order
/// Part 3 §3.2 fixes.
pub const DESCRIPTORS: [(u64, &str); 9] = [
    (0x70, "amqp:header:list"),
    (0x71, "amqp:delivery-annotations:map"),
    (0x72, "amqp:message-annotations:map"),
    (0x73, "amqp:properties:list"),
    (0x74, "amqp:application-properties:map"),
    (0x75, "amqp:data:binary"),
    (0x76, "amqp:amqp-sequence:list"),
    (0x77, "amqp:amqp-value:*"),
    (0x78, "amqp:footer:map"),
];

/// `header.priority`'s default: 4, with higher numbers meaning higher
/// priority (Part 3 §3.2.1).
pub const DEFAULT_PRIORITY: u8 = 4;

/// `header`, `0x70`: how the network should treat the transfer
/// (Part 3 §3.2.1).
///
/// Every field has a default, so an absent `header` section means exactly
/// this value — which is why the struct is `Default` and
/// [`Message::header`] is an `Option` of it rather than the struct carrying
/// its own absence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// A *demand*, not a hint: a durable message "MUST NOT be lost even if
    /// an intermediary is unexpectedly terminated and restarted", and a
    /// target that cannot honour it MUST NOT accept the message — it rejects
    /// with `amqp:precondition-failed` or, where the source disallows
    /// `rejected`, detaches with the same error.
    pub durable: bool,
    /// Higher is higher. The specification permits reordering by priority
    /// outright, which is the one place it licenses a message to overtake
    /// another.
    pub priority: u8,
    /// Milliseconds. Each intermediary recomputes it downward as the
    /// difference between now and the previously computed expiry, so a
    /// looping message eventually dies.
    pub ttl: Option<u32>,
    /// `true` means no other link has acquired this message.
    pub first_acquirer: bool,
    /// Prior *unsuccessful* attempts. Zero on first delivery, and non-zero
    /// "can be taken as an indication that the delivery might be a
    /// duplicate".
    pub delivery_count: u32,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            durable: false,
            priority: DEFAULT_PRIORITY,
            ttl: None,
            first_acquirer: false,
            delivery_count: 0,
        }
    }
}

impl Header {
    fn read(mut fields: Fields<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            durable: fields.boolean_or("durable", false)?,
            priority: fields.ubyte("priority")?.unwrap_or(DEFAULT_PRIORITY),
            ttl: fields.uint("ttl")?,
            first_acquirer: fields.boolean_or("first-acquirer", false)?,
            delivery_count: fields.uint_or("delivery-count", 0)?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(0x70),
            &[
                if self.durable {
                    Value::Boolean(true)
                } else {
                    Value::Null
                },
                if self.priority == DEFAULT_PRIORITY {
                    Value::Null
                } else {
                    Value::Ubyte(self.priority)
                },
                self.ttl.map_or(Value::Null, Value::Uint),
                if self.first_acquirer {
                    Value::Boolean(true)
                } else {
                    Value::Null
                },
                if self.delivery_count == 0 {
                    Value::Null
                } else {
                    Value::Uint(self.delivery_count)
                },
            ],
            out,
        )
    }
}

/// A `message-id` or `correlation-id`: one of exactly four types
/// (Part 3 §3.2.11-3.2.14).
///
/// The specification types both fields `*` and then restricts them to this
/// archetype, so an enum is the honest representation: a decoder that
/// accepted any value would accept identifiers a conforming peer cannot send,
/// and the JMS mapping depends on the distinction — it requires application
/// string correlation identifiers to use the `string` form specifically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageId<'a> {
    /// `message-id-ulong`.
    Ulong(u64),
    /// `message-id-uuid`.
    Uuid([u8; 16]),
    /// `message-id-binary`.
    Binary(&'a [u8]),
    /// `message-id-string`.
    String(&'a str),
}

impl<'a> MessageId<'a> {
    fn from_value(value: &Value<'a>, field: &'static str) -> Result<Self, DecodeError> {
        match value {
            Value::Ulong(v) => Ok(Self::Ulong(*v)),
            Value::Uuid(octets) => Ok(Self::Uuid(*octets)),
            Value::Binary(data) => Ok(Self::Binary(data)),
            Value::String(text) => Ok(Self::String(text)),
            other => Err(DecodeError::WrongType {
                field,
                code: other.canonical_code(),
            }),
        }
    }

    fn to_value(self) -> Value<'a> {
        match self {
            Self::Ulong(v) => Value::Ulong(v),
            Self::Uuid(octets) => Value::Uuid(octets),
            Self::Binary(data) => Value::Binary(data),
            Self::String(text) => Value::String(text),
        }
    }
}

/// `properties`, `0x73`: the interpreted, immutable metadata
/// (Part 3 §3.2.4).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Properties<'a> {
    /// Uniquely identifies the message. "A broker MAY discard a message as a
    /// duplicate if the value of the message-id matches that of a previously
    /// received message sent to the same node" — which is the only
    /// deduplication the core protocol authorizes.
    pub message_id: Option<MessageId<'a>>,
    /// "The identity of the user responsible for producing the message", set
    /// by the client and possibly authenticated by intermediaries. The only
    /// per-message identity the protocol has, and immutable because it is in
    /// the bare message.
    pub user_id: Option<&'a [u8]>,
    /// The node the message is destined for, which "on any given transfer
    /// might not be the node at the receiving end of the link".
    pub to: Option<&'a str>,
    /// Summary information about the content and purpose.
    pub subject: Option<&'a str>,
    /// The node to send replies to.
    pub reply_to: Option<&'a str>,
    /// Ties a response to its request.
    pub correlation_id: Option<MessageId<'a>>,
    /// The RFC 2046 MIME type of the body, which SHOULD NOT be set when the
    /// body is not a `data` section.
    pub content_type: Option<&'a str>,
    /// The MIME content-encoding of the body.
    pub content_encoding: Option<&'a str>,
    /// When the message expires, absolutely rather than as a duration.
    pub absolute_expiry_time: Option<i64>,
    /// When the message was created.
    pub creation_time: Option<i64>,
    /// The group this message belongs to.
    pub group_id: Option<&'a str>,
    /// Its position within that group. Expresses grouping the protocol does
    /// not interpret: there is no ordering guarantee attached to it.
    pub group_sequence: Option<u32>,
    /// The group a reply belongs to.
    pub reply_to_group_id: Option<&'a str>,
}

impl<'a> Properties<'a> {
    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        let message_id = match fields.any()? {
            Some(value) => Some(MessageId::from_value(&value, "message-id")?),
            None => None,
        };
        let user_id = fields.binary("user-id")?;
        let to = fields.string("to")?;
        let subject = fields.string("subject")?;
        let reply_to = fields.string("reply-to")?;
        let correlation_id = match fields.any()? {
            Some(value) => Some(MessageId::from_value(&value, "correlation-id")?),
            None => None,
        };
        Ok(Self {
            message_id,
            user_id,
            to,
            subject,
            reply_to,
            correlation_id,
            content_type: fields.symbol("content-type")?,
            content_encoding: fields.symbol("content-encoding")?,
            absolute_expiry_time: fields.timestamp("absolute-expiry-time")?,
            creation_time: fields.timestamp("creation-time")?,
            group_id: fields.string("group-id")?,
            group_sequence: fields.uint("group-sequence")?,
            reply_to_group_id: fields.string("reply-to-group-id")?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(0x73),
            &[
                self.message_id.map_or(Value::Null, MessageId::to_value),
                self.user_id.map_or(Value::Null, Value::Binary),
                self.to.map_or(Value::Null, Value::String),
                self.subject.map_or(Value::Null, Value::String),
                self.reply_to.map_or(Value::Null, Value::String),
                self.correlation_id.map_or(Value::Null, MessageId::to_value),
                self.content_type.map_or(Value::Null, Value::Symbol),
                self.content_encoding.map_or(Value::Null, Value::Symbol),
                self.absolute_expiry_time
                    .map_or(Value::Null, Value::Timestamp),
                self.creation_time.map_or(Value::Null, Value::Timestamp),
                self.group_id.map_or(Value::Null, Value::String),
                self.group_sequence.map_or(Value::Null, Value::Uint),
                self.reply_to_group_id.map_or(Value::Null, Value::String),
            ],
            out,
        )
    }
}

/// The application data: one of Part 3 §3.2's three choices.
#[derive(Clone, Debug, PartialEq)]
pub enum Body<'a> {
    /// No body section at all.
    ///
    /// **Not conformant.** Part 3 §3.2 enumerates the body as "one or more
    /// `data` sections, one or more `amqp-sequence` sections, or a single
    /// `amqp-value` section" and offers no fourth choice. This decoder still
    /// reports it, for two reasons: refusing would turn a peer's minor
    /// non-conformance into a torn-down link, and the *application* is the
    /// only party that can decide whether a body-less delivery is acceptable
    /// — a codec cannot. Encoding it writes no body section, which reproduces
    /// what arrived rather than inventing a section around it.
    Empty,
    /// One or more `data` sections of opaque binary, in wire order.
    ///
    /// A `Vec` of slices rather than one concatenated buffer, because the
    /// boundaries are part of the message: a receiver that joined them could
    /// not write the bare message back byte for byte, and the bare message's
    /// exact encoding is what a signature covers.
    Data(Vec<&'a [u8]>),
    /// One or more `amqp-sequence` sections, each a list of values.
    Sequence(Vec<Value<'a>>),
    /// A single `amqp-value` section holding one value of any type.
    Value(Value<'a>),
}

impl Body<'_> {
    /// The section name this body is made of, for an error message.
    const fn name(&self) -> &'static str {
        match self {
            Self::Empty => "no body",
            Self::Data(_) => "data",
            Self::Sequence(_) => "amqp-sequence",
            Self::Value(_) => "amqp-value",
        }
    }

    /// Whether there is no body section.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

/// An annotated message: the bare message plus whatever the infrastructure
/// wrapped around it.
#[derive(Clone, Debug, PartialEq)]
pub struct Message<'a> {
    /// `header`. Absent means every field at its default.
    pub header: Option<Header>,
    /// `delivery-annotations`: one hop only, sender to receiver. Absent is
    /// equivalent to an empty map.
    pub delivery_annotations: Option<Value<'a>>,
    /// `message-annotations`: aimed at the infrastructure, and intermediaries
    /// MUST propagate them unless explicitly augmented or modified. Absent is
    /// equivalent to an empty map.
    pub message_annotations: Option<Value<'a>>,
    /// `properties`. Part of the bare message, therefore immutable.
    pub properties: Option<Properties<'a>>,
    /// `application-properties`. Part of the bare message. Keys are `string`
    /// and values are restricted to simple types — no map, list or array —
    /// which is what lets an intermediary filter on them cheaply.
    pub application_properties: Option<Value<'a>>,
    /// The body.
    pub body: Body<'a>,
    /// `footer`: values computable only once the whole bare message is seen —
    /// hashes, HMACs, signatures, encryption detail.
    pub footer: Option<Value<'a>>,
}

impl Default for Message<'_> {
    fn default() -> Self {
        Self {
            header: None,
            delivery_annotations: None,
            message_annotations: None,
            properties: None,
            application_properties: None,
            body: Body::Empty,
            footer: None,
        }
    }
}

/// A decoded message together with the slice of input its bare part
/// occupied.
#[derive(Clone, Debug, PartialEq)]
pub struct Decoded<'a> {
    /// The message.
    pub message: Message<'a>,
    /// The octets of the bare message — `properties`,
    /// `application-properties` and the body — exactly as they arrived.
    ///
    /// Empty where the message had none of those three sections. This is the
    /// input a signature or an HMAC in the `footer` is computed over, and it
    /// is reported rather than re-encoded because Part 3 §3.2 forbids
    /// changing the exact encoding and a canonical encoder would change it.
    pub bare: &'a [u8],
    /// How many octets the whole annotated message occupied.
    pub used: usize,
}

impl<'a> Message<'a> {
    /// A message whose body is one `data` section.
    #[must_use]
    pub fn data(payload: &'a [u8]) -> Self {
        Self {
            body: Body::Data(vec![payload]),
            ..Self::default()
        }
    }

    /// A message whose body is one `amqp-value` section.
    #[must_use]
    pub fn value(value: Value<'a>) -> Self {
        Self {
            body: Body::Value(value),
            ..Self::default()
        }
    }

    /// Decodes an annotated message from a `transfer` payload.
    ///
    /// Every section draws on one zero-width allowance, so a message of many
    /// small arrays is bounded as a whole, by `Limits::max_zero_width_elements`.
    ///
    /// ```
    /// use weida_amqp_codec::{Limits, message::{Body, Message}};
    ///
    /// let mut bytes = Vec::new();
    /// Message::data(b"hello").encode(&mut bytes).expect("encodes");
    /// assert_eq!(bytes, [0x00, 0x53, 0x75, 0xa0, 0x05, b'h', b'e', b'l', b'l', b'o']);
    ///
    /// let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");
    /// assert_eq!(decoded.message.body, Body::Data(vec![b"hello".as_slice()]));
    /// assert_eq!(decoded.bare, &bytes[..], "the whole message is bare here");
    /// ```
    pub fn decode(input: &'a [u8], limits: Limits) -> Result<Decoded<'a>, DecodeError> {
        let mut message = Self::default();
        let mut at = 0usize;
        // The section order of Part 3 §3.2, as a monotonic stage counter.
        let mut stage = 0u8;
        let mut last = "the start of the message";
        let mut bare_from: Option<usize> = None;
        let mut bare_to = 0usize;
        let mut zero_width = limits.max_zero_width_elements;

        while at < input.len() {
            let (descriptor, value, used) =
                decode::described_within(&input[at..], limits, &mut zero_width)?;
            let code = match resolve(&descriptor) {
                Some(code) => code,
                // Not a section this codec knows. The message ends here as
                // far as this decoder is concerned, and the caller gets to
                // see how much was consumed.
                None => break,
            };
            let (section_stage, name) = match stage_of(code) {
                Some(found) => found,
                None => break,
            };
            // The per-section handling below runs first, because a section
            // that repeats is *both* out of order and a duplicate and the
            // duplicate is the more useful thing to report: "a second header
            // section" says what happened, "a header cannot follow header"
            // does not.
            match code {
                0x70 => {
                    if message.header.is_some() {
                        return Err(DecodeError::DuplicateSection("header"));
                    }
                    message.header = Some(Header::read(list_fields(value, "header")?)?);
                }
                0x71 => {
                    if message.delivery_annotations.is_some() {
                        return Err(DecodeError::DuplicateSection("delivery-annotations"));
                    }
                    message.delivery_annotations = Some(map_of(value, "delivery-annotations")?);
                }
                0x72 => {
                    if message.message_annotations.is_some() {
                        return Err(DecodeError::DuplicateSection("message-annotations"));
                    }
                    message.message_annotations = Some(map_of(value, "message-annotations")?);
                }
                0x73 => {
                    if message.properties.is_some() {
                        return Err(DecodeError::DuplicateSection("properties"));
                    }
                    message.properties = Some(Properties::read(list_fields(value, "properties")?)?);
                }
                0x74 => {
                    if message.application_properties.is_some() {
                        return Err(DecodeError::DuplicateSection("application-properties"));
                    }
                    message.application_properties = Some(map_of(value, "application-properties")?);
                }
                0x75 => {
                    let data = match value {
                        Value::Binary(data) => data,
                        other => {
                            return Err(DecodeError::WrongType {
                                field: "data",
                                code: other.canonical_code(),
                            });
                        }
                    };
                    match &mut message.body {
                        Body::Empty => message.body = Body::Data(vec![data]),
                        Body::Data(sections) => sections.push(data),
                        other => {
                            return Err(DecodeError::MixedBodySections {
                                had: other.name(),
                                found: "data",
                            });
                        }
                    }
                }
                0x76 => {
                    // An `amqp-sequence` is a list of application values; a bare
                    // value is what `amqp-value` is for.
                    if !matches!(value, Value::List(_)) {
                        return Err(DecodeError::WrongType {
                            field: "amqp-sequence",
                            code: value.canonical_code(),
                        });
                    }
                    match &mut message.body {
                        Body::Empty => message.body = Body::Sequence(vec![value]),
                        Body::Sequence(sections) => sections.push(value),
                        other => {
                            return Err(DecodeError::MixedBodySections {
                                had: other.name(),
                                found: "amqp-sequence",
                            });
                        }
                    }
                }
                0x77 => match &message.body {
                    Body::Empty => message.body = Body::Value(value),
                    other => {
                        return Err(DecodeError::MixedBodySections {
                            had: other.name(),
                            found: "amqp-value",
                        });
                    }
                },
                0x78 => {
                    if message.footer.is_some() {
                        return Err(DecodeError::DuplicateSection("footer"));
                    }
                    message.footer = Some(map_of(value, "footer")?);
                }
                _ => unreachable!("stage_of covers every section this codec knows"),
            }
            if section_stage < stage {
                return Err(DecodeError::SectionOutOfOrder {
                    section: name,
                    after: last,
                });
            }
            // The bare message is the contiguous run of `properties`,
            // `application-properties` and body sections — stages 3 to 5.
            // The annotations before it and the footer after it are not part
            // of it, which is exactly why an intermediary may change them.
            if (3..=5).contains(&section_stage) {
                if bare_from.is_none() {
                    bare_from = Some(at);
                }
                bare_to = at + used;
            }
            // `data` and `amqp-sequence` repeat, so their stage does not
            // advance; every other section may appear once.
            stage = if matches!(code, 0x75 | 0x76) {
                section_stage
            } else {
                section_stage + 1
            };
            last = name;
            at += used;
        }

        let bare = match bare_from {
            Some(from) => &input[from..bare_to],
            None => &input[..0],
        };
        Ok(Decoded {
            message,
            bare,
            used: at,
        })
    }

    /// Appends this message's canonical encoding to `out`, in the section
    /// order Part 3 §3.2 fixes.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        if let Some(header) = &self.header {
            header.write(out)?;
        }
        if let Some(annotations) = &self.delivery_annotations {
            encode::described(&Descriptor::Code(0x71), annotations, out)?;
        }
        if let Some(annotations) = &self.message_annotations {
            encode::described(&Descriptor::Code(0x72), annotations, out)?;
        }
        if let Some(properties) = &self.properties {
            properties.write(out)?;
        }
        if let Some(properties) = &self.application_properties {
            encode::described(&Descriptor::Code(0x74), properties, out)?;
        }
        match &self.body {
            Body::Empty => {}
            Body::Data(sections) => {
                for data in sections {
                    encode::described(&Descriptor::Code(0x75), &Value::Binary(data), out)?;
                }
            }
            Body::Sequence(sections) => {
                for sequence in sections {
                    encode::described(&Descriptor::Code(0x76), sequence, out)?;
                }
            }
            Body::Value(value) => {
                encode::described(&Descriptor::Code(0x77), value, out)?;
            }
        }
        if let Some(footer) = &self.footer {
            encode::described(&Descriptor::Code(0x78), footer, out)?;
        }
        Ok(())
    }

    /// Encodes into a fresh `Vec`.
    pub fn to_vec(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        self.encode(&mut out)?;
        Ok(out)
    }
}

/// The stage a section occupies in Part 3 §3.2's order, and its name.
///
/// `data` and `amqp-sequence` share stage 5 with each other only in the sense
/// that both are body sections; the stage numbers are what make "a
/// `properties` section after the body" detectable without a state machine.
const fn stage_of(code: u64) -> Option<(u8, &'static str)> {
    Some(match code {
        0x70 => (0, "header"),
        0x71 => (1, "delivery-annotations"),
        0x72 => (2, "message-annotations"),
        0x73 => (3, "properties"),
        0x74 => (4, "application-properties"),
        0x75 => (5, "data"),
        0x76 => (5, "amqp-sequence"),
        0x77 => (5, "amqp-value"),
        0x78 => (7, "footer"),
        _ => return None,
    })
}

fn resolve(descriptor: &Descriptor<'_>) -> Option<u64> {
    match descriptor {
        Descriptor::Code(code) => Some(*code),
        Descriptor::Symbol(name) => DESCRIPTORS
            .iter()
            .find(|(_, symbolic)| symbolic == name)
            .map(|(code, _)| *code),
    }
}

/// A section whose described value must be a `list`, as a field reader.
fn list_fields<'a>(value: Value<'a>, section: &'static str) -> Result<Fields<'a>, DecodeError> {
    match value {
        Value::List(items) => Ok(Fields::from_values(items, section)),
        other => Err(DecodeError::WrongType {
            field: section,
            code: other.canonical_code(),
        }),
    }
}

/// A section whose described value must be a `map`.
fn map_of<'a>(value: Value<'a>, section: &'static str) -> Result<Value<'a>, DecodeError> {
    match value {
        Value::Map(_) => Ok(value),
        other => Err(DecodeError::WrongType {
            field: section,
            code: other.canonical_code(),
        }),
    }
}

/// Reassembles a multi-frame delivery, bounded by the link's
/// `max-message-size`.
///
/// A delivery may span any number of `transfer` frames with `more=true` on
/// all but the last (Part 2 §2.6.14). `max-frame-size` bounds one frame;
/// **nothing in the protocol bounds the number of frames**, so without
/// `max-message-size` a peer can hand a receiver an unbounded message one
/// bounded frame at a time. That is the bound this type enforces, and it
/// enforces it *before* appending: [`Reassembly::accept`] compares
/// `already + fragment` against the limit and refuses with
/// [`DecodeError::MessageTooLarge`] while the buffer is still the size it was.
///
/// Zero means no limit, which is what the field's own default means
/// (Part 2 §2.7.3) — and a caller passing zero has said so explicitly.
#[derive(Debug)]
pub struct Reassembly {
    max_message_size: u64,
    payload: Vec<u8>,
    delivery_id: Option<u32>,
    delivery_tag: Option<Vec<u8>>,
    message_format: Option<u32>,
    in_progress: bool,
}

/// What a fragment did to the delivery being reassembled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    /// More frames are expected; `received` octets are in hand.
    Incomplete {
        /// How much of the message has arrived.
        received: usize,
    },
    /// The last frame arrived: the payload is complete.
    Complete,
    /// The sender aborted. Everything transferred so far is discarded and the
    /// delivery is implicitly settled — `aborted` takes precedence over
    /// `more` (Part 2 §2.7.5).
    Aborted,
}

impl Reassembly {
    /// A reassembler bounded by `max_message_size`, where zero means no
    /// limit.
    #[must_use]
    pub const fn new(max_message_size: u64) -> Self {
        Self {
            max_message_size,
            payload: Vec::new(),
            delivery_id: None,
            delivery_tag: None,
            message_format: None,
            in_progress: false,
        }
    }

    /// The bound in force. Zero means no limit.
    #[must_use]
    pub const fn max_message_size(&self) -> u64 {
        self.max_message_size
    }

    /// The octets accumulated so far.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The delivery-id of the delivery being reassembled.
    #[must_use]
    pub const fn delivery_id(&self) -> Option<u32> {
        self.delivery_id
    }

    /// The delivery-tag of the delivery being reassembled.
    #[must_use]
    pub fn delivery_tag(&self) -> Option<&[u8]> {
        self.delivery_tag.as_deref()
    }

    /// The `message-format` of the delivery being reassembled.
    ///
    /// Carried by the first frame and legitimately absent from every
    /// continuation, so it is held here rather than read off the frame that
    /// happened to complete the delivery.
    #[must_use]
    pub const fn message_format(&self) -> Option<u32> {
        self.message_format
    }

    /// Whether a delivery is part-way through.
    #[must_use]
    pub const fn in_progress(&self) -> bool {
        self.in_progress
    }

    /// Takes the completed payload and readies the reassembler for the next
    /// delivery.
    pub fn take(&mut self) -> Vec<u8> {
        self.delivery_id = None;
        self.delivery_tag = None;
        self.message_format = None;
        self.in_progress = false;
        core::mem::take(&mut self.payload)
    }

    /// Discards whatever has accumulated.
    pub fn reset(&mut self) {
        // Released rather than cleared: an aborted delivery must not keep its
        // buffer pinned to the link until the next one arrives.
        self.payload = Vec::new();
        self.delivery_id = None;
        self.delivery_tag = None;
        self.message_format = None;
        self.in_progress = false;
    }

    /// Adds one `transfer` frame's payload.
    ///
    /// The delivery's identity is checked as it goes: a continuation may omit
    /// `delivery-id`, `delivery-tag` and `message-format`, but where it
    /// carries one it MUST NOT differ, and a difference is
    /// [`DecodeError::ContinuationMismatch`] rather than an update
    /// (Part 2 §2.7.5). A first frame arriving while another delivery is
    /// incomplete is [`DecodeError::InterleavedDelivery`], because deliveries
    /// on one link MUST NOT interleave.
    pub fn accept(
        &mut self,
        transfer: &crate::performative::Transfer<'_>,
        payload: &[u8],
    ) -> Result<Progress, DecodeError> {
        let first = !self.in_progress;
        if first {
            if transfer.resume {
                // A resumed delivery may carry no payload at all and act
                // purely as a vehicle for the sender's terminal state, so an
                // empty first fragment is not an error here.
                self.payload.clear();
            }
            self.delivery_id = transfer.delivery_id;
            self.delivery_tag = transfer.delivery_tag.map(<[u8]>::to_vec);
            self.message_format = transfer.message_format;
            self.in_progress = true;
        } else {
            if let (Some(have), Some(found)) = (self.delivery_id, transfer.delivery_id)
                && have != found
            {
                return Err(DecodeError::ContinuationMismatch {
                    field: "delivery-id",
                });
            }
            if let (Some(have), Some(found)) = (&self.delivery_tag, transfer.delivery_tag)
                && have.as_slice() != found
            {
                return Err(DecodeError::ContinuationMismatch {
                    field: "delivery-tag",
                });
            }
            if let (Some(have), Some(found)) = (self.message_format, transfer.message_format)
                && have != found
            {
                return Err(DecodeError::ContinuationMismatch {
                    field: "message-format",
                });
            }
        }

        if transfer.aborted {
            self.reset();
            return Ok(Progress::Aborted);
        }

        // The bound, before the buffer grows by a single octet.
        if self.max_message_size != 0 {
            let would_be = self.payload.len() as u64 + payload.len() as u64;
            if would_be > self.max_message_size {
                return Err(DecodeError::MessageTooLarge {
                    size: would_be,
                    max: self.max_message_size,
                });
            }
        }
        self.reserve_for(payload.len());
        self.payload.extend_from_slice(payload);

        if transfer.more {
            Ok(Progress::Incomplete {
                received: self.payload.len(),
            })
        } else {
            Ok(Progress::Complete)
        }
    }

    /// Refuses a first fragment that arrives while another delivery is
    /// incomplete.
    ///
    /// Separate from [`Reassembly::accept`] because only the link knows which
    /// `transfer` frames belong to which delivery: a reassembler handed
    /// frames from two deliveries cannot tell a continuation from a new
    /// delivery when both omit their `delivery-id`. The link calls this
    /// before `accept` for a frame it believes starts a delivery.
    pub fn expect_first(&self) -> Result<(), DecodeError> {
        if self.in_progress {
            return Err(DecodeError::InterleavedDelivery);
        }
        Ok(())
    }

    /// Makes room for `incoming` more octets.
    ///
    /// Growth is geometric, but under a `max-message-size` it stops at that
    /// size. The caller has checked the bound already, so the target is never
    /// below what is needed, and a message that reaches the limit does not
    /// hold up to twice it while the buffer doubles.
    fn reserve_for(&mut self, incoming: usize) {
        let needed = self.payload.len() + incoming;
        if needed <= self.payload.capacity() {
            return;
        }
        if self.max_message_size == 0 {
            self.payload.reserve(incoming);
            return;
        }
        let bound = usize::try_from(self.max_message_size).unwrap_or(usize::MAX);
        let target = self
            .payload
            .capacity()
            .saturating_mul(2)
            .max(needed)
            .min(bound);
        self.payload.reserve_exact(target - self.payload.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes;
    use crate::performative::Transfer;

    fn round_trip(message: &Message<'_>) -> Vec<u8> {
        let bytes = message.to_vec().expect("encodes");
        let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");
        assert_eq!(decoded.used, bytes.len());
        assert_eq!(&decoded.message, message);
        bytes
    }

    fn annotations() -> Value<'static> {
        Value::Map(vec![(
            Value::Symbol("x-opt-trace"),
            Value::String("00-abc-def-01"),
        )])
    }

    #[test]
    fn a_message_with_every_section_round_trips_in_order() {
        let message = Message {
            header: Some(Header {
                durable: true,
                priority: 9,
                ttl: Some(60_000),
                first_acquirer: true,
                delivery_count: 2,
            }),
            delivery_annotations: Some(annotations()),
            message_annotations: Some(annotations()),
            properties: Some(Properties {
                message_id: Some(MessageId::String("id-1")),
                user_id: Some(b"alice"),
                to: Some("/queues/q1"),
                subject: Some("a subject"),
                reply_to: Some("/queues/replies"),
                correlation_id: Some(MessageId::Ulong(7)),
                content_type: Some("application/octet-stream"),
                content_encoding: Some("identity"),
                absolute_expiry_time: Some(1_700_000_000_000),
                creation_time: Some(1_600_000_000_000),
                group_id: Some("g1"),
                group_sequence: Some(3),
                reply_to_group_id: Some("g2"),
            }),
            application_properties: Some(Value::Map(vec![(
                Value::String("attempt"),
                Value::Uint(1),
            )])),
            body: Body::Data(vec![b"first", b"second"]),
            footer: Some(Value::Map(vec![(
                Value::Symbol("x-opt-hmac"),
                Value::Binary(&[0xab; 8]),
            )])),
        };
        let bytes = round_trip(&message);

        // The nine descriptors appear in the order Part 3 fixes, and the
        // body's two data sections stay in their own order.
        let positions: Vec<usize> = [0x70u8, 0x71, 0x72, 0x73, 0x74, 0x75, 0x78]
            .iter()
            .map(|code| {
                bytes
                    .windows(3)
                    .position(|w| w == [0x00, 0x53, *code])
                    .unwrap_or_else(|| panic!("section 0x{code:02x} missing"))
            })
            .collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(positions, sorted, "sections are written in order");
    }

    #[test]
    fn each_body_form_round_trips() {
        round_trip(&Message::data(b"one"));
        round_trip(&Message {
            body: Body::Data(vec![b"one", b"two", b"three"]),
            ..Message::default()
        });
        round_trip(&Message {
            body: Body::Sequence(vec![
                Value::List(vec![Value::Uint(1), Value::String("a")]),
                Value::List(vec![Value::Uint(2)]),
            ]),
            ..Message::default()
        });
        round_trip(&Message::value(Value::String("a value body")));
        round_trip(&Message::value(Value::Null));
        round_trip(&Message::default());
    }

    #[test]
    fn an_absent_header_means_every_default() {
        let bytes = Message::data(b"x").to_vec().expect("encodes");
        let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");
        assert_eq!(decoded.message.header, None);
        // And a header carrying only the defaults encodes to nothing but its
        // own descriptor and an empty list.
        let message = Message {
            header: Some(Header::default()),
            ..Message::default()
        };
        let bytes = message.to_vec().unwrap();
        assert_eq!(bytes, [0x00, 0x53, 0x70, 0x45]);
        let decoded = Message::decode(&bytes, Limits::BODY).unwrap();
        assert_eq!(decoded.message.header, Some(Header::default()));
        assert_eq!(
            decoded.message.header.unwrap().priority,
            DEFAULT_PRIORITY,
            "an omitted priority is 4, not 0"
        );
    }

    #[test]
    fn a_section_out_of_order_is_refused() {
        // properties, then header: the order is normative.
        let mut bytes = Vec::new();
        Properties {
            subject: Some("s"),
            ..Properties::default()
        }
        .write(&mut bytes)
        .expect("encodes");
        Header::default().write(&mut bytes).expect("encodes");
        assert_eq!(
            Message::decode(&bytes, Limits::BODY),
            Err(DecodeError::SectionOutOfOrder {
                section: "header",
                after: "properties"
            })
        );
    }

    #[test]
    fn a_body_section_after_the_footer_is_refused() {
        let mut bytes = Vec::new();
        encode::described(&Descriptor::Code(0x78), &annotations(), &mut bytes).expect("encodes");
        encode::described(&Descriptor::Code(0x75), &Value::Binary(b"late"), &mut bytes)
            .expect("encodes");
        assert_eq!(
            Message::decode(&bytes, Limits::BODY),
            Err(DecodeError::SectionOutOfOrder {
                section: "data",
                after: "footer"
            })
        );
    }

    #[test]
    fn a_duplicate_single_section_is_refused() {
        let mut bytes = Vec::new();
        Header::default().write(&mut bytes).expect("encodes");
        Header::default().write(&mut bytes).expect("encodes");
        assert_eq!(
            Message::decode(&bytes, Limits::BODY),
            Err(DecodeError::DuplicateSection("header"))
        );
    }

    #[test]
    fn the_three_body_forms_may_not_be_mixed() {
        let mut bytes = Vec::new();
        encode::described(&Descriptor::Code(0x75), &Value::Binary(b"d"), &mut bytes)
            .expect("encodes");
        encode::described(
            &Descriptor::Code(0x76),
            &Value::List(vec![Value::Null]),
            &mut bytes,
        )
        .expect("encodes");
        assert_eq!(
            Message::decode(&bytes, Limits::BODY),
            Err(DecodeError::MixedBodySections {
                had: "data",
                found: "amqp-sequence"
            })
        );

        // And a second amqp-value: the specification says "a single"
        // amqp-value section.
        let mut bytes = Vec::new();
        encode::described(&Descriptor::Code(0x77), &Value::Uint(1), &mut bytes).expect("encodes");
        encode::described(&Descriptor::Code(0x77), &Value::Uint(2), &mut bytes).expect("encodes");
        assert_eq!(
            Message::decode(&bytes, Limits::BODY),
            Err(DecodeError::MixedBodySections {
                had: "amqp-value",
                found: "amqp-value"
            })
        );
    }

    #[test]
    fn the_bare_message_is_reported_as_the_octets_that_arrived() {
        // A signature in the footer covers the bare message's *exact*
        // encoding, so the decoder has to hand back a slice of the input
        // rather than something it re-encoded.
        let message = Message {
            header: Some(Header {
                durable: true,
                ..Header::default()
            }),
            message_annotations: Some(annotations()),
            properties: Some(Properties {
                subject: Some("s"),
                ..Properties::default()
            }),
            body: Body::Data(vec![b"payload"]),
            footer: Some(annotations()),
            ..Message::default()
        };
        let bytes = message.to_vec().expect("encodes");
        let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");

        let mut expected = Vec::new();
        Properties {
            subject: Some("s"),
            ..Properties::default()
        }
        .write(&mut expected)
        .expect("encodes");
        encode::described(
            &Descriptor::Code(0x75),
            &Value::Binary(b"payload"),
            &mut expected,
        )
        .expect("encodes");
        assert_eq!(decoded.bare, expected.as_slice());
        assert!(
            !decoded.bare.is_empty() && decoded.bare.len() < bytes.len(),
            "the bare message is a proper sub-slice"
        );
    }

    #[test]
    fn a_message_with_no_bare_sections_has_an_empty_bare_slice() {
        let message = Message {
            header: Some(Header {
                durable: true,
                ..Header::default()
            }),
            ..Message::default()
        };
        let bytes = message.to_vec().expect("encodes");
        let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");
        assert_eq!(decoded.bare, b"");
        assert!(decoded.message.body.is_empty());
    }

    #[test]
    fn all_four_message_id_types_round_trip() {
        for id in [
            MessageId::Ulong(u64::MAX),
            MessageId::Uuid([3; 16]),
            MessageId::Binary(b"\x00\xff"),
            MessageId::String("id"),
        ] {
            round_trip(&Message {
                properties: Some(Properties {
                    message_id: Some(id),
                    correlation_id: Some(id),
                    ..Properties::default()
                }),
                body: Body::Data(vec![b""]),
                ..Message::default()
            });
        }
    }

    #[test]
    fn a_message_id_of_the_wrong_type_is_refused() {
        // The archetype is ulong, uuid, binary or string and nothing else.
        let mut bytes = Vec::new();
        encode::composite(&Descriptor::Code(0x73), &[Value::Uint(1)], &mut bytes).expect("encodes");
        assert_eq!(
            Message::decode(&bytes, Limits::BODY),
            Err(DecodeError::WrongType {
                field: "message-id",
                code: codes::SMALLUINT
            })
        );
    }

    #[test]
    fn a_symbolic_section_descriptor_names_the_same_section() {
        let mut bytes = Vec::new();
        encode::described(
            &Descriptor::Symbol("amqp:data:binary"),
            &Value::Binary(b"x"),
            &mut bytes,
        )
        .expect("encodes");
        let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");
        assert_eq!(decoded.message.body, Body::Data(vec![b"x".as_slice()]));
    }

    #[test]
    fn an_unknown_section_ends_the_message_rather_than_failing_it() {
        // Part 3 defines nine sections and the extensions add none. A
        // described value that is not one of them is not part of the
        // message, and the caller sees how much was consumed so it can
        // decide.
        let mut bytes = Message::data(b"x").to_vec().expect("encodes");
        let body_len = bytes.len();
        encode::described(&Descriptor::Code(0x7f), &Value::Null, &mut bytes).expect("encodes");
        let decoded = Message::decode(&bytes, Limits::BODY).expect("decodes");
        assert_eq!(
            decoded.used, body_len,
            "the unknown section is not consumed"
        );
        assert_eq!(decoded.message.body, Body::Data(vec![b"x".as_slice()]));
    }

    fn fragment(more: bool) -> Transfer<'static> {
        let mut transfer = Transfer::new(0);
        transfer.delivery_id = Some(1);
        transfer.delivery_tag = Some(b"tag");
        transfer.message_format = Some(0);
        transfer.more = more;
        transfer
    }

    #[test]
    fn a_multi_frame_message_reassembles_into_one_delivery() {
        let whole = Message::data(b"a longer payload than one frame")
            .to_vec()
            .expect("encodes");
        let (first, rest) = whole.split_at(10);
        let (second, third) = rest.split_at(10);

        let mut assembler = Reassembly::new(4096);
        assembler.expect_first().expect("nothing in progress");
        assert_eq!(
            assembler.accept(&fragment(true), first).unwrap(),
            Progress::Incomplete { received: 10 }
        );
        assert!(assembler.in_progress());
        assert_eq!(
            assembler.accept(&fragment(true), second).unwrap(),
            Progress::Incomplete { received: 20 }
        );
        assert_eq!(
            assembler.accept(&fragment(false), third).unwrap(),
            Progress::Complete
        );
        assert_eq!(assembler.delivery_id(), Some(1));
        assert_eq!(assembler.delivery_tag(), Some(b"tag".as_slice()));

        let payload = assembler.take();
        assert_eq!(payload, whole);
        assert!(!assembler.in_progress());
        let decoded = Message::decode(&payload, Limits::BODY).expect("decodes");
        assert_eq!(
            decoded.message.body,
            Body::Data(vec![b"a longer payload than one frame".as_slice()])
        );
    }

    #[test]
    fn max_message_size_is_checked_before_the_buffer_grows() {
        let mut assembler = Reassembly::new(16);
        assert_eq!(
            assembler.accept(&fragment(true), &[0u8; 10]).unwrap(),
            Progress::Incomplete { received: 10 }
        );
        assert_eq!(
            assembler.accept(&fragment(false), &[0u8; 10]),
            Err(DecodeError::MessageTooLarge { size: 20, max: 16 })
        );
        assert_eq!(
            assembler.payload().len(),
            10,
            "the refused fragment was not appended"
        );
    }

    #[test]
    fn growth_stops_at_the_message_size_bound() {
        // Geometric growth would take the buffer to 4 MB here, past the 3 MiB
        // bound the message may not exceed.
        let bound: usize = 3 << 20;
        let mut assembler = Reassembly::new(bound as u64);
        let chunk = vec![0u8; 1_000_000];
        for _ in 0..3 {
            assembler.accept(&fragment(true), &chunk).unwrap();
        }
        assert_eq!(assembler.payload().len(), 3_000_000);
        assert!(
            assembler.payload.capacity() <= bound,
            "capacity {} is above the bound {bound}",
            assembler.payload.capacity()
        );
    }

    #[test]
    fn an_aborted_reassembly_releases_its_buffer() {
        // Fifteen mebibytes of continuation frames under a 16 MiB bound, then an
        // abort: the buffer goes with the delivery.
        let mut assembler = Reassembly::new(16 << 20);
        let frame = vec![0u8; 64 << 10];
        for _ in 0..240 {
            assembler.accept(&fragment(true), &frame).unwrap();
        }
        assert_eq!(assembler.payload().len(), 15 << 20);
        let mut aborted = fragment(true);
        aborted.aborted = true;
        assert_eq!(assembler.accept(&aborted, &[]), Ok(Progress::Aborted));
        assert_eq!(assembler.payload.capacity(), 0);
        assert!(assembler.payload().is_empty());
    }

    #[test]
    fn an_amqp_sequence_section_must_hold_a_list() {
        // A bare `uint` in an `amqp-sequence` is not a list of application values.
        let bytes = [
            codes::DESCRIBED,
            codes::SMALLULONG,
            0x76,
            codes::SMALLUINT,
            1,
        ];
        assert_eq!(
            Message::decode(&bytes, Limits::BODY).map(|decoded| decoded.used),
            Err(DecodeError::WrongType {
                field: "amqp-sequence",
                code: codes::SMALLUINT,
            })
        );
    }

    #[test]
    fn zero_max_message_size_is_no_limit() {
        // Which is what the field's own default means, and a caller passing
        // zero has said so rather than forgotten to pass anything.
        let mut assembler = Reassembly::new(0);
        assert_eq!(assembler.max_message_size(), 0);
        for _ in 0..8 {
            assembler.accept(&fragment(true), &[0u8; 1024]).unwrap();
        }
        assert_eq!(assembler.payload().len(), 8 * 1024);
    }

    #[test]
    fn an_aborted_delivery_discards_everything_transferred() {
        let mut assembler = Reassembly::new(4096);
        assembler.accept(&fragment(true), b"partial").unwrap();
        let mut aborted = fragment(true);
        aborted.aborted = true;
        assert_eq!(
            assembler.accept(&aborted, b"ignored").unwrap(),
            Progress::Aborted,
            "aborted takes precedence over more"
        );
        assert!(assembler.payload().is_empty());
        assert!(!assembler.in_progress());
        assembler
            .expect_first()
            .expect("ready for the next delivery");
    }

    #[test]
    fn a_continuation_that_changes_the_delivery_is_refused() {
        /// Changes one identity field of a continuation frame.
        type Mutate = fn(&mut Transfer<'static>);
        let cases: [(&str, Mutate); 3] = [
            ("delivery-id", |t: &mut Transfer<'static>| {
                t.delivery_id = Some(2)
            }),
            ("delivery-tag", |t: &mut Transfer<'static>| {
                t.delivery_tag = Some(b"other")
            }),
            ("message-format", |t: &mut Transfer<'static>| {
                t.message_format = Some(1)
            }),
        ];
        for (field, mutate) in cases {
            let mut assembler = Reassembly::new(4096);
            assembler.accept(&fragment(true), b"first").unwrap();
            let mut next = fragment(false);
            mutate(&mut next);
            assert_eq!(
                assembler.accept(&next, b"second"),
                Err(DecodeError::ContinuationMismatch { field })
            );
        }
    }

    #[test]
    fn a_continuation_may_omit_what_it_does_not_change() {
        let mut assembler = Reassembly::new(4096);
        assembler.accept(&fragment(true), b"first").unwrap();
        let mut bare = Transfer::new(0);
        bare.more = false;
        assert_eq!(
            assembler.accept(&bare, b"second").unwrap(),
            Progress::Complete
        );
        assert_eq!(assembler.payload(), b"firstsecond");
        assert_eq!(
            assembler.delivery_id(),
            Some(1),
            "the identity stays the first frame's"
        );
    }

    #[test]
    fn a_second_delivery_while_one_is_incomplete_is_refused() {
        let mut assembler = Reassembly::new(4096);
        assembler.accept(&fragment(true), b"first").unwrap();
        assert_eq!(
            assembler.expect_first(),
            Err(DecodeError::InterleavedDelivery)
        );
    }

    #[test]
    fn the_section_descriptors_are_the_nine_in_order() {
        let codes: Vec<u64> = DESCRIPTORS.iter().map(|(code, _)| *code).collect();
        assert_eq!(codes, (0x70..=0x78).collect::<Vec<u64>>());
        for (code, symbolic) in DESCRIPTORS {
            assert!(symbolic.starts_with("amqp:"), "{symbolic}");
            assert_eq!(resolve(&Descriptor::Symbol(symbolic)), Some(code));
            assert!(stage_of(code).is_some());
        }
    }
}
