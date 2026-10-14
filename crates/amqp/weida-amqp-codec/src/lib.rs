//! AMQP 1.0 codec: the type system, the frames and the performatives,
//! sans-I/O.
//!
//! This crate is the codec under `weida-amqp`, the AMQP 1.0 client library
//! described in
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3 and
//! [0014](../../../docs/decisions/0014-parallel-libraries.md) §2. It is
//! **sans-I/O and weida-free**: no sockets, no executor, no dependency — not
//! even on `weida-core`. What it exports is a byte-for-byte encoder and
//! decoder for the OASIS Standard of 29 October 2012, so that it can be
//! checked against a foreign specification rather than against weida's
//! opinion of it. `weida-amqp`, the client built on top, depends on this
//! crate and on `weida-runtime`; this one depends on neither.
//!
//! # Shape
//!
//! * [`codes`] — the format codes of Part 1 §1.6 and the category rule that
//!   fixes each one's width.
//! * [`Value`] — every primitive type, the three compound types, the array
//!   and the described type, borrowing its variable-width data from the
//!   caller's buffer.
//! * [`decode`] — bytes to values, in **every** form the specification
//!   permits: three encodings of `uint`, three of the empty list, two of
//!   every `binary`, `string` and `symbol`.
//! * [`encode`] — values to bytes in **one** canonical form, the shortest
//!   that carries the value, with a composite's trailing null fields omitted.
//! * [`Limits`] — the caller's bounds, an argument to every decode.
//! * [`fields`] — reading a composite's fields by position, with the
//!   specification's own names in the error messages.
//! * [`types`] — the restricted types the performatives are built from:
//!   `role`, the two settle modes, `multiple`, `error` and the error
//!   conditions.
//! * [`protocol_header`] — the eight octets before any frame, and the three
//!   protocol-ids that select a layer.
//! * [`frame`] — the 8-octet frame header, the two frame types, the empty
//!   keep-alive frame, and the 512-octet bound that holds until `open` has
//!   been read.
//! * [`performative`] — the nine performatives, `open` `0x10` through
//!   `close` `0x18`.
//! * [`sasl`] — the five SASL bodies, `0x40` through `0x44`, and `PLAIN`'s
//!   NUL-separated initial response.
//!
//! # Reading a value
//!
//! ```
//! use weida_amqp_codec::{Descriptor, Limits, Value, decode, encode};
//!
//! // A composite type — the shape every performative, message section and
//! // delivery state arrives in — is a described `list` read by position.
//! let mut bytes = Vec::new();
//! encode::composite(
//!     &Descriptor::Code(0x10),
//!     &[Value::String("client-1"), Value::Null, Value::Uint(4096)],
//!     &mut bytes,
//! )
//! .expect("an open encodes");
//!
//! let mut open = decode::composite(&bytes, Limits::DEFAULT).expect("a composite");
//! assert!(open.descriptor.is(0x10), "amqp:open:list");
//! assert_eq!(open.fields.next_value().unwrap(), Value::String("client-1"));
//! assert_eq!(open.fields.next_value().unwrap(), Value::Null, "hostname absent");
//! assert_eq!(open.fields.next_value().unwrap(), Value::Uint(4096));
//! // Past the declared count, every field is null: Part 1 lets a sender
//! // omit trailing nulls, so a short list and a list of nulls are one value.
//! assert_eq!(open.fields.next_value().unwrap(), Value::Null);
//! ```
//!
//! # Bounds
//!
//! AMQP grants no credit before `open` has been read, and its compound
//! headers declare their own element counts: a nine-octet `list32` can
//! announce 2^32-1 elements. Every decode entry point therefore takes
//! [`Limits`] as an **argument**, and the declared count is refused from the
//! header alone — before a single element is looked at and before anything is
//! reserved. Nothing in this crate reserves memory on behalf of a peer.
//!
//! The connection-level bounds — `max-frame-size` with its 512-octet floor,
//! `channel-max`, `handle-max`, `max-message-size` — belong to the frame and
//! link layers and arrive with them.
//!
//! # Where this codec departs from the text
//!
//! * **A descriptor is a `symbol` or a `ulong`, and nothing else.** Not a
//!   departure so much as the specification's own reservation taken
//!   literally: the grammar is `described = descriptor value` with
//!   `descriptor = value`, so a `string` descriptor is syntactically legal —
//!   Figure 1.2 uses one — but §1.5 says "descriptor values other than
//!   symbolic (symbol) or numeric (ulong) are, while not syntactically
//!   invalid, reserved", and Figure 1.2 itself carries the note "this
//!   example shows a string-typed descriptor, which is considered reserved".
//!   A reserved descriptor is refused with
//!   [`DecodeError::DescriptorNotSymbolicOrNumeric`]; accepting one would
//!   mean carrying a whole `Value` in the one position a decoder dispatches
//!   on. `tests/golden_vectors.rs` asserts the refusal against the figure.
//! * **An unknown format code is an error, not a skip.** The category rule
//!   means a decoder *could* step over a code it does not know, and this one
//!   does not: a field whose type is unknown is not a field a decoder may
//!   guess at, and the transport's answer to an unreadable frame is
//!   `amqp:decode-error` rather than a partial performative.
//! * **`symbol` is checked for ASCII in both directions.** Part 1 restricts
//!   symbolic values to ASCII; a decoder that accepted UTF-8 there would
//!   accept symbols an encoder could not write back.
//! * **`decimal32`, `decimal64` and `decimal128` travel as opaque octets.**
//!   IEEE 754 decimal has no Rust counterpart and this crate needs no
//!   arithmetic over it, so the octets are carried unread rather than through
//!   a lossy conversion. They round-trip exactly.
//! * **An `array` keeps the element width it declared.** Everywhere else the
//!   encoder narrows; in an array it must not, because the shared
//!   constructor *is* the array's declared element type and narrowing it
//!   would change the value. An array of `ulong` therefore re-encodes its
//!   elements as eight octets each, and an element the declared constructor
//!   cannot carry — `ulong` 300 in an array of `smallulong` — is
//!   [`EncodeError::ArrayElementMismatch`] rather than a silent widening.
//! * **`Value` compares floats the way Rust does.** `Value::Float` and
//!   `Value::Double` hold `f32` and `f64`, so no `NaN` equals itself. Where
//!   a round trip must be asserted over values that may hold one, assert it
//!   over the canonical encoding instead: it is a fixed point, which is what
//!   the fuzz targets check.
//!
//! # Sources
//!
//! Every rule and every number comes from `docs/research/amqp10.md`, which
//! cites the five OASIS parts of 29 October 2012. Section references in this
//! crate's documentation name the part in prose, so that `§1.6` is never
//! ambiguous between them:
//!
//! * [Part 1: Types](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html)
//!   — the type system this crate implements.
//! * [Part 2: Transport](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)
//!   — frames, performatives, sessions, links, credit, settlement.
//! * [Part 3: Messaging](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html)
//!   — message sections, delivery states, termini.
//! * [Part 4: Transactions](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html)
//!   — the coordinator and the control link.
//! * [Part 5: Security](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html)
//!   — the TLS and SASL layers.

#![warn(missing_docs)]

pub mod codes;
pub mod decode;
pub mod encode;
pub mod error;
pub mod fields;
pub mod frame;
pub mod limits;
pub mod message;
pub mod performative;
pub mod protocol_header;
pub mod sasl;
pub mod state;
pub mod types;
pub mod value;

pub use error::{DecodeError, EncodeError};
pub use fields::Fields;
pub use frame::{Frame, FrameHeader, FrameKind};
pub use limits::Limits;
pub use message::{Body, Header, Message, MessageId, Properties};
pub use performative::Performative;
pub use protocol_header::{ProtocolHeader, ProtocolId};
pub use sasl::{SaslCode, SaslFrame};
pub use state::DeliveryState;
pub use types::{AmqpError, Multiple, ReceiverSettleMode, Role, SenderSettleMode};
pub use value::{Array, Described, Descriptor, ElementKind, Value};
