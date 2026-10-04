//! CBOR frame headers.
//!
//! The codecs are written by hand rather than derived. Three reasons:
//!
//! * the strictness rules (duplicate-key rejection, non-uint key rejection,
//!   per-field string caps, depth-limited skipping) are protocol requirements,
//!   not serialization defaults;
//! * the exact byte layout is normative — see the golden vectors in
//!   `docs/PROTOCOL.md` §8 — and a derive macro's field ordering is not a
//!   contract we control;
//! * every decoder here is a hostile-input boundary, so its allocation
//!   behaviour has to be readable.
//!
//! All decode failures are protocol violations that close the connection.
//!
//! **No field of a DATA header is required by the decoder.** A header no longer
//! carries the stream's role, so the decoder cannot know which fields the
//! context demands; `endpoint`-on-initiating-streams is enforced by the
//! transport's dispatch, which does know (`docs/PROTOCOL.md` §6.2).

use std::convert::Infallible;

use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use weida_core::Error;

use crate::varint::{VarintError, decode_varint, encode_varint};

/// Decoder limits. The string caps are normative
/// (`docs/PROTOCOL.md` §6); the list and depth caps are defensive
/// implementation limits documented in the same section.
pub mod limits {
    /// Cap for the DATA `endpoint` field.
    pub const MAX_ENDPOINT_BYTES: usize = 512;
    /// Cap for the DATA `content_type` field.
    pub const MAX_CONTENT_TYPE_BYTES: usize = 256;
    /// Cap for the DATA `traceparent` field.
    pub const MAX_TRACEPARENT_BYTES: usize = 128;
    /// Cap for the DATA `tracestate` field.
    pub const MAX_TRACESTATE_BYTES: usize = 512;
    /// Cap for the DATA `topic` field.
    pub const MAX_TOPIC_BYTES: usize = 256;
    /// Length of the DATA `producer` field: a raw 32-byte digest.
    ///
    /// Both a cap and an exact length. `docs/PROTOCOL.md` §6.2 defines the
    /// value as "the raw 32-byte digest", so a longer one is a framing
    /// violation and a shorter one names nothing this specification defines.
    pub const PRODUCER_BYTES: usize = 32;
    /// Cap for the SUBSCRIBE/UNSUBSCRIBE `filter` field.
    pub const MAX_FILTER_BYTES: usize = 256;
    /// Cap for the ERROR `message` field.
    pub const MAX_MESSAGE_BYTES: usize = 1024;
    /// Cap on the number of items in a HELLO list field.
    ///
    /// Without it, a hostile peer could pin `max_concurrent_uni_streams`
    /// worth of large `Vec<u64>`s by opening many HELLO streams.
    pub const MAX_LIST_ITEMS: usize = 64;
    /// Cap on the number of levels a DATA header may order a report for
    /// (`docs/PROTOCOL.md` §6.2, key `10`).
    ///
    /// A report order is a remote-controlled list, so it needs a cap for the
    /// same reason [`MAX_LIST_ITEMS`] exists; 16 is more levels than the level
    /// space defines below the application floor, so it constrains nothing a
    /// sender legitimately wants.
    pub const MAX_REPORT_LEVELS: usize = 16;
    /// Nesting depth allowed when skipping an unknown field.
    pub const MAX_SKIP_DEPTH: usize = 8;
}

/// HELLO keys.
mod hello_key {
    pub const VERSIONS: u64 = 0;
    pub const MAX_HEADER_BYTES: u64 = 1;
    pub const MAX_TRANSFERS: u64 = 2;
    pub const CAPABILITIES: u64 = 3;
    pub const REQUIRED_CAPABILITIES: u64 = 4;
    pub const GUARANTEES_OFFERED: u64 = 5;
    pub const GUARANTEES_REQUIRED: u64 = 6;
}

/// DATA keys.
mod data_key {
    pub const ENDPOINT: u64 = 0;
    pub const CONTENT_LEN: u64 = 1;
    pub const CONTENT_TYPE: u64 = 2;
    pub const TRACEPARENT: u64 = 3;
    pub const TRACESTATE: u64 = 4;
    pub const TOPIC: u64 = 5;
    pub const SEQUENCE: u64 = 6;
    pub const PRODUCER: u64 = 7;
    pub const ACHIEVED: u64 = 8;
    pub const REPORT_ID: u64 = 9;
    pub const REPORT: u64 = 10;
    pub const REPORT_MODE: u64 = 11;
    // Key 12 (`delivery_attempt`) is reserved in docs/PROTOCOL.md §6.2 and
    // coded by the slice that writes it.
    pub const SEGMENT: u64 = 13;
}

/// ERROR keys.
mod error_key {
    pub const CODE: u64 = 0;
    pub const MESSAGE: u64 = 1;
}

/// SUBSCRIBE and UNSUBSCRIBE keys.
mod subscription_key {
    pub const ENDPOINT: u64 = 0;
    pub const FILTER: u64 = 1;
    pub const MAX_AGE_MS: u64 = 2;
}

/// CREDIT keys.
mod credit_key {
    pub const ENDPOINT: u64 = 0;
    pub const FILTER: u64 = 1;
    pub const LIMIT: u64 = 2;
}

/// CURSOR head-frame keys.
mod cursor_key {
    pub const REPORT_ID: u64 = 0;
}

/// FLOW keys (`docs/PROTOCOL.md` §6.8).
mod flow_key {
    pub const ENDPOINT: u64 = 0;
    pub const FLOW: u64 = 1;
    pub const CONTENT_TYPE: u64 = 2;
    pub const TRACEPARENT: u64 = 3;
    pub const TRACESTATE: u64 = 4;
    pub const TOPIC: u64 = 5;
}

/// The topic filter grammar of `docs/PROTOCOL.md` §6.4.
///
/// A topic and a filter are byte strings split on [`filter::SEPARATOR`] into
/// segments. The two wildcards are whole-segment tokens, and everything else
/// is literal: there is no escape character, no normalization and no case
/// folding ([decisions/0007](../../../docs/decisions/0007-topic-namespace.md)
/// §4.2). Both halves of the grammar live here: [`filter::validate`], the rule
/// that says which filters may exist at all — so an illegal one is refused at
/// the codec boundary rather than reaching a matcher that would have to cope
/// with it — and [`filter::matches`], the matcher itself. The matcher used to
/// sit with the fan-out it served, in `weida::pubsub`, and moved when a second
/// layer needed it: an L2 queue selects a consumer with the same grammar
/// (B-202), and a second implementation of a wildcard language is exactly the
/// kind of drift one definition exists to prevent.
pub mod filter {
    use super::HeaderError;

    /// Segment separator: `.`, one byte.
    pub const SEPARATOR: char = '.';
    /// Matches exactly one whole segment.
    pub const ONE_SEGMENT: &str = "*";
    /// Matches zero or more trailing segments; legal only as the last segment.
    pub const REST: &str = "#";

    /// Does `topic` match `filter`?
    ///
    /// The segmented grammar of `docs/PROTOCOL.md` §6.4: segments split on `.`,
    /// `*` for exactly one whole segment, a trailing `#` for zero or more, every
    /// other byte literal, and the empty filter matching everything.
    ///
    /// The objection this function used to carry was that treating `*` as a
    /// wildcard "would make topics with a literal `*` unaddressable and would put
    /// a matching language in the hot path". Both halves were true and both are
    /// accepted deliberately
    /// ([decisions/0007](../../../../docs/decisions/0007-topic-namespace.md) §4.6): a
    /// filter can no longer select a segment containing `.`, `*` or `#`
    /// literally — there is no escape character, and no sheet reports a use for
    /// one — while a byte prefix could not express a boundary at all, so
    /// `sensors.temp` also selected `sensors.temperature`. The hot-path half is
    /// answered by the shape rather than by the choice: `#` is legal only as the
    /// final segment, so this is one left-to-right walk with no backtracking, no
    /// allocation and work bounded by the 256 B filter cap.
    ///
    /// A `topic` is never a pattern: `*` and `#` in a published topic are literal
    /// bytes here, exactly like any other.
    pub fn matches(topic: &str, filter: &str) -> bool {
        if filter.is_empty() {
            return true;
        }
        let mut topic_segments = topic.split(SEPARATOR);
        let mut filter_segments = filter.split(SEPARATOR);
        loop {
            let Some(pattern) = filter_segments.next() else {
                // The filter is spent: it matches only if the topic is too.
                return topic_segments.next().is_none();
            };
            // Only ever the final segment — `filter::validate` rejects anything
            // else at the codec boundary — so everything left over matches.
            if pattern == REST {
                return true;
            }
            let Some(segment) = topic_segments.next() else {
                return false;
            };
            if pattern != ONE_SEGMENT && pattern != segment {
                return false;
            }
        }
    }

    /// Checks `filter` against the grammar.
    ///
    /// The empty filter is legal and matches every topic. One pass, no
    /// allocation.
    pub fn validate(filter: &str) -> Result<(), HeaderError> {
        let mut segments = filter.split(SEPARATOR).peekable();
        while let Some(segment) = segments.next() {
            let is_last = segments.peek().is_none();
            if segment.contains(ONE_SEGMENT) && segment != ONE_SEGMENT {
                return Err(HeaderError::InvalidFilter(
                    "`*` must occupy a whole segment",
                ));
            }
            if segment.contains(REST) {
                if segment != REST {
                    return Err(HeaderError::InvalidFilter(
                        "`#` must occupy a whole segment",
                    ));
                }
                if !is_last {
                    return Err(HeaderError::InvalidFilter("`#` must be the final segment"));
                }
            }
        }
        Ok(())
    }
}

/// Guarantee set keys (`docs/PROTOCOL.md` §6.5).
mod guarantee_key {
    pub const DELIVERY: u64 = 0;
    pub const ACKNOWLEDGEMENT: u64 = 1;
    pub const DURABILITY: u64 = 2;
    pub const REPLICAS: u64 = 3;
    pub const ORDERING: u64 = 4;
    pub const DEDUPLICATION: u64 = 5;
    pub const DEDUP_WINDOW_MS: u64 = 6;
    pub const BACKPRESSURE: u64 = 7;
    pub const PRODUCER_NAMING: u64 = 8;
    pub const CONTROL_ISOLATED: u64 = 9;
}

/// Declares an enum whose wire form is a small `uint`, with the `core` level
/// first so that `Default` and "absent means core" agree by construction.
///
/// The derived `Ord` ranks by declaration order while `to_wire` reads
/// explicit literals, and the ladder comparisons rest on the two agreeing —
/// `GuaranteeSet::intersect` picks the weaker level with `.min()`,
/// `GuaranteeSet::reaches` compares with `<`, and `CursorLevel`'s derived
/// order inherits the same coincidence — so the macro asserts the agreement
/// at compile time rather than letting a variant inserted mid-block with a
/// higher literal silently rank a stronger guarantee below a weaker one.
macro_rules! wire_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident = $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)+
        }

        impl $name {
            /// The wire value of `docs/PROTOCOL.md` §6.5.
            pub fn to_wire(self) -> u64 {
                match self {
                    $($name::$variant => $value,)+
                }
            }

            /// The level a wire value names, or `None` if the value is not
            /// one this version defines.
            pub fn from_wire(value: u64) -> Option<$name> {
                match value {
                    $($value => Some($name::$variant),)+
                    _ => None,
                }
            }
        }

        // The invariant the ladder comparisons depend on. A build failure is
        // the only acceptable outcome: at run time the mis-ranking is
        // invisible — every value still encodes and decodes — and shows up
        // only as a negotiated guarantee weaker than the one reported.
        const _: () = {
            let values = [$($value as u64),+];
            let mut i = 1;
            while i < values.len() {
                assert!(
                    values[i - 1] < values[i],
                    concat!(
                        stringify!($name),
                        ": wire values must ascend with declaration order, ",
                        "because the derived Ord is the ladder"
                    )
                );
                i += 1;
            }
        };
    };
}

wire_enum! {
    /// Delivery dimension ([`GUARANTEES.md`] §3). A ladder: later is stronger.
    ///
    /// [`GUARANTEES.md`]: https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/GUARANTEES.md
    Delivery {
        /// v0: no retries, losses reported.
        #[default]
        BestEffort = 0,
        /// Reserved.
        AtMostOnce = 1,
        /// Reserved.
        AtLeastOnce = 2,
    }
}

wire_enum! {
    /// Acknowledgement/completion dimension. A ladder; the durability axes of
    /// [`Durability`] and `replicas` are *not* part of it.
    Acknowledgement {
        /// Nothing is reported.
        None = 0,
        /// v0: QUIC's fin-acknowledgement.
        #[default]
        TransportReceipt = 1,
        /// Reserved for the L2 broker.
        Accepted = 2,
        /// Reserved for the L2 broker.
        Stored = 3,
        /// Reserved for the L2 broker.
        Replicated = 4,
        /// Reserved for the L2 broker.
        Processed = 5,
    }
}

wire_enum! {
    /// Persistence axis of `Stored`/`Replicated`
    /// ([decisions/0004](../../../docs/decisions/0004-durability-levels.md) §4.1).
    Durability {
        /// Survives the broker process.
        #[default]
        Written = 0,
        /// Survives loss of power on that node.
        Flushed = 1,
    }
}

wire_enum! {
    /// Ordering dimension. A ladder: later is stronger.
    OrderingMode {
        /// v0.
        #[default]
        None = 0,
        /// Report gaps, deliver as messages arrive.
        PerProducerDetect = 1,
        /// Hold messages back up to a bounded buffer.
        PerProducerReassemble = 2,
        /// L2 only.
        PerKey = 3,
        /// Reserved.
        Total = 4,
    }
}

wire_enum! {
    /// Deduplication dimension. A ladder: later is stronger.
    Deduplication {
        /// v0.
        #[default]
        None = 0,
        /// Suppressed within a time window.
        Bounded = 1,
        /// L2 only.
        Durable = 2,
    }
}

wire_enum! {
    /// Backpressure dimension. **Not ordered**: these are behaviours, not
    /// strengths, so two peers state the same one or fail to negotiate.
    Backpressure {
        /// v0 for Req/Rep and Push/Pull.
        #[default]
        Block = 0,
        /// Refuse past a cap.
        Reject = 1,
        /// v0 for fan-out.
        Drop = 2,
        /// Reserved.
        Spill = 3,
        /// Reserved.
        Coalesce = 4,
    }
}

wire_enum! {
    /// How a producer is named for the sequence field of
    /// [decisions/0001](../../../docs/decisions/0001-sequence-field.md) §7.3.
    /// **Not ordered**: two peers state the same one or fail.
    ProducerNaming {
        /// The proved connection fingerprint; the counter restarts with the
        /// connection (v0 default,
        /// [decisions/0008](../../../docs/decisions/0008-session-identity.md) §4.3).
        #[default]
        Fingerprint = 0,
        /// A name supplied above L0, carried in DATA key `7`.
        Stable = 1,
    }
}

wire_enum! {
    /// How often a reporter emits a record (`docs/PROTOCOL.md` §6.2, key
    /// `11`).
    ///
    /// **Not ordered**: these are two shapes of the same report, not two
    /// strengths. A cursor is never load-bearing, so neither mode is a
    /// guarantee and neither is negotiated
    /// ([decisions/0023](../../../docs/decisions/0023-completion-is-a-cursor.md)
    /// §4.5).
    ReportMode {
        /// Records as the level advances, coalesced at the reporter's own
        /// granularity.
        #[default]
        Progress = 0,
        /// One record per level, at the end.
        FinalOnly = 1,
    }
}

/// A level a cursor can name: one weida defines, or one the application does.
///
/// The level space is **open**
/// ([decisions/0023](../../../docs/decisions/0023-completion-is-a-cursor.md)
/// §4.4): values below [`CursorLevel::APPLICATION_FLOOR`] are weida's own
/// ladder, [`Acknowledgement`], and everything at or above it is an
/// application stage weida carries and orders but never interprets. An
/// undefined value *below* the floor is a protocol violation rather than an
/// application level, because the reserved range is where a later version of
/// this specification will put its own stages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CursorLevel {
    /// A level this version of the protocol defines.
    Known(Acknowledgement),
    /// An application stage, at or above the floor.
    Application(u64),
}

impl CursorLevel {
    /// First wire value an application may name.
    pub const APPLICATION_FLOOR: u64 = 16;

    /// The wire value.
    pub fn to_wire(self) -> u64 {
        match self {
            CursorLevel::Known(level) => level.to_wire(),
            CursorLevel::Application(value) => value,
        }
    }

    /// Interprets a wire value, or `None` if it is an undefined value in the
    /// reserved range.
    pub fn from_wire(value: u64) -> Option<CursorLevel> {
        if value >= CursorLevel::APPLICATION_FLOOR {
            Some(CursorLevel::Application(value))
        } else {
            Acknowledgement::from_wire(value).map(CursorLevel::Known)
        }
    }

    /// An application stage, or `None` below the floor: the reserved range is
    /// not an application's to name.
    pub fn application(value: u64) -> Option<CursorLevel> {
        (value >= CursorLevel::APPLICATION_FLOOR).then_some(CursorLevel::Application(value))
    }
}

/// One level per guarantee dimension, as declared in HELLO keys `5` and `6`
/// (`docs/PROTOCOL.md` §6.5).
///
/// [`GuaranteeSet::CORE`] is the default set and is exactly what v0 does, so
/// an absent HELLO key, an empty map and `CORE` are the same statement
/// ([decisions/0006](../../../docs/decisions/0006-guarantee-sets.md) §4.2).
/// Every field is a small `Copy` value: a set costs no allocation, which is
/// what lets it ride a header a peer controls.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GuaranteeSet {
    /// Delivery dimension.
    pub delivery: Delivery,
    /// Acknowledgement/completion dimension.
    pub acknowledgement: Acknowledgement,
    /// Persistence axis; legal only with `Stored` or `Replicated`.
    pub durability: Option<Durability>,
    /// Replica count, leader included; legal only with `Replicated`, and ≥ 2.
    pub replicas: Option<u64>,
    /// Ordering dimension.
    pub ordering: OrderingMode,
    /// Deduplication dimension.
    pub deduplication: Deduplication,
    /// Dedup window; required with `Bounded`, forbidden otherwise.
    pub dedup_window_ms: Option<u64>,
    /// Backpressure behaviour. Not ordered.
    pub backpressure: Backpressure,
    /// How the producer of a sequenced transfer is named. Not ordered.
    pub producer_naming: ProducerNaming,
    /// Whether control traffic is isolated from bulk traffic
    /// ([decisions/0002](../../../docs/decisions/0002-control-and-bulk-separation.md)
    /// §6.1). Ordered: `true` is strictly stronger.
    pub control_isolated: bool,
}

impl GuaranteeSet {
    /// The default set: what v0 offers and requires.
    pub const CORE: GuaranteeSet = GuaranteeSet {
        delivery: Delivery::BestEffort,
        acknowledgement: Acknowledgement::TransportReceipt,
        durability: None,
        replicas: None,
        ordering: OrderingMode::None,
        deduplication: Deduplication::None,
        dedup_window_ms: None,
        backpressure: Backpressure::Block,
        producer_naming: ProducerNaming::Fingerprint,
        control_isolated: false,
    };

    /// Is this the default set? A `core` declaration is never written: it is
    /// what an absent key already means.
    pub fn is_core(&self) -> bool {
        *self == GuaranteeSet::CORE
    }

    /// Checks the dimension combinations §6.5 forbids.
    fn validate(&self) -> Result<(), HeaderError> {
        let stored_or_replicated = matches!(
            self.acknowledgement,
            Acknowledgement::Stored | Acknowledgement::Replicated
        );
        if self.durability.is_some() && !stored_or_replicated {
            return Err(HeaderError::InvalidGuarantees(
                "durability without Stored or Replicated",
            ));
        }
        match self.replicas {
            Some(_) if self.acknowledgement != Acknowledgement::Replicated => {
                return Err(HeaderError::InvalidGuarantees(
                    "replicas without Replicated",
                ));
            }
            Some(n) if n < 2 => {
                return Err(HeaderError::InvalidGuarantees(
                    "a replica count below 2 is not a replication",
                ));
            }
            _ => {}
        }
        match (self.deduplication, self.dedup_window_ms) {
            (Deduplication::Bounded, None) => {
                return Err(HeaderError::InvalidGuarantees(
                    "Bounded deduplication without a window",
                ));
            }
            (level, Some(_)) if level != Deduplication::Bounded => {
                return Err(HeaderError::InvalidGuarantees(
                    "a dedup window without Bounded deduplication",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    /// Writes the set as a CBOR map, omitting every dimension left at `core`.
    fn encode_into(
        &self,
        e: &mut Encoder<Vec<u8>>,
    ) -> Result<(), minicbor::encode::Error<Infallible>> {
        let core = GuaranteeSet::CORE;
        let count = u64::from(self.delivery != core.delivery)
            + u64::from(self.acknowledgement != core.acknowledgement)
            + u64::from(self.durability.is_some())
            + u64::from(self.replicas.is_some())
            + u64::from(self.ordering != core.ordering)
            + u64::from(self.deduplication != core.deduplication)
            + u64::from(self.dedup_window_ms.is_some())
            + u64::from(self.backpressure != core.backpressure)
            + u64::from(self.producer_naming != core.producer_naming)
            + u64::from(self.control_isolated != core.control_isolated);
        e.map(count)?;
        if self.delivery != core.delivery {
            e.u64(guarantee_key::DELIVERY)?
                .u64(self.delivery.to_wire())?;
        }
        if self.acknowledgement != core.acknowledgement {
            e.u64(guarantee_key::ACKNOWLEDGEMENT)?
                .u64(self.acknowledgement.to_wire())?;
        }
        if let Some(durability) = self.durability {
            e.u64(guarantee_key::DURABILITY)?
                .u64(durability.to_wire())?;
        }
        if let Some(replicas) = self.replicas {
            e.u64(guarantee_key::REPLICAS)?.u64(replicas)?;
        }
        if self.ordering != core.ordering {
            e.u64(guarantee_key::ORDERING)?
                .u64(self.ordering.to_wire())?;
        }
        if self.deduplication != core.deduplication {
            e.u64(guarantee_key::DEDUPLICATION)?
                .u64(self.deduplication.to_wire())?;
        }
        if let Some(window) = self.dedup_window_ms {
            e.u64(guarantee_key::DEDUP_WINDOW_MS)?.u64(window)?;
        }
        if self.backpressure != core.backpressure {
            e.u64(guarantee_key::BACKPRESSURE)?
                .u64(self.backpressure.to_wire())?;
        }
        if self.producer_naming != core.producer_naming {
            e.u64(guarantee_key::PRODUCER_NAMING)?
                .u64(self.producer_naming.to_wire())?;
        }
        if self.control_isolated != core.control_isolated {
            e.u64(guarantee_key::CONTROL_ISOLATED)?
                .u64(u64::from(self.control_isolated))?;
        }
        Ok(())
    }

    /// Reads a set from the nested map at the decoder's position.
    ///
    /// The nesting is one level deep by specification (§5), and an unknown
    /// dimension is skipped exactly like an unknown top-level key.
    fn decode_from(m: &mut MapReader<'_, '_>) -> Result<GuaranteeSet, HeaderError> {
        let mut set = GuaranteeSet::CORE;
        let mut inner = MapReader::new(m.d)?;
        while let Some(key) = inner.next_key()? {
            match key {
                guarantee_key::DELIVERY => set.delivery = level(inner.u64()?, "delivery")?,
                guarantee_key::ACKNOWLEDGEMENT => {
                    set.acknowledgement = level(inner.u64()?, "acknowledgement")?;
                }
                guarantee_key::DURABILITY => {
                    set.durability = Some(level(inner.u64()?, "durability")?);
                }
                guarantee_key::REPLICAS => set.replicas = Some(inner.u64()?),
                guarantee_key::ORDERING => set.ordering = level(inner.u64()?, "ordering")?,
                guarantee_key::DEDUPLICATION => {
                    set.deduplication = level(inner.u64()?, "deduplication")?;
                }
                guarantee_key::DEDUP_WINDOW_MS => set.dedup_window_ms = Some(inner.u64()?),
                guarantee_key::BACKPRESSURE => {
                    set.backpressure = level(inner.u64()?, "backpressure")?;
                }
                guarantee_key::PRODUCER_NAMING => {
                    set.producer_naming = level(inner.u64()?, "producer naming")?;
                }
                guarantee_key::CONTROL_ISOLATED => {
                    set.control_isolated = match inner.u64()? {
                        0 => false,
                        1 => true,
                        _ => {
                            return Err(HeaderError::InvalidGuarantees(
                                "control_isolated is 0 or 1",
                            ));
                        }
                    };
                }
                _ => inner.skip()?,
            }
        }
        set.validate()?;
        Ok(set)
    }

    /// The weaker of two offered sets, dimension by dimension
    /// (`docs/PROTOCOL.md` §2.3 step 5).
    ///
    /// Ladders take the minimum. Dimensions that are **not** ordered —
    /// backpressure, producer naming, and the two independent axes of a
    /// durability level — have no "weaker", so the two declarations must be
    /// equal; the name of the dimension comes back as the error so a peer can
    /// be told which one disagreed.
    pub fn intersect(&self, other: &GuaranteeSet) -> Result<GuaranteeSet, &'static str> {
        if self.backpressure != other.backpressure {
            return Err("backpressure");
        }
        if self.producer_naming != other.producer_naming {
            return Err("producer naming");
        }
        if self.durability.is_some()
            && other.durability.is_some()
            && self.durability != other.durability
        {
            return Err("durability");
        }
        if self.replicas.is_some() && other.replicas.is_some() && self.replicas != other.replicas {
            return Err("replicas");
        }

        let acknowledgement = self.acknowledgement.min(other.acknowledgement);
        let keeps_durability = matches!(
            acknowledgement,
            Acknowledgement::Stored | Acknowledgement::Replicated
        );
        let deduplication = self.deduplication.min(other.deduplication);
        let mut merged = GuaranteeSet {
            delivery: self.delivery.min(other.delivery),
            acknowledgement,
            // A dimension the weakened acknowledgement can no longer carry is
            // dropped rather than kept: dropping is what "weaker" means here,
            // and keeping it would produce a set §6.5 forbids.
            durability: keeps_durability
                .then_some(self.durability.or(other.durability))
                .flatten(),
            replicas: (acknowledgement == Acknowledgement::Replicated)
                .then_some(self.replicas.or(other.replicas))
                .flatten(),
            ordering: self.ordering.min(other.ordering),
            deduplication,
            // A shorter window is the weaker promise.
            dedup_window_ms: None,
            backpressure: self.backpressure,
            producer_naming: self.producer_naming,
            control_isolated: self.control_isolated && other.control_isolated,
        };
        if deduplication == Deduplication::Bounded {
            merged.dedup_window_ms = match (self.dedup_window_ms, other.dedup_window_ms) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
        }
        Ok(merged)
    }

    /// Does this set reach `required` on every dimension?
    ///
    /// Ladders compare by level, the durability axes compare per axis, and the
    /// unordered dimensions must match exactly. A longer dedup window is the
    /// stronger promise.
    pub fn reaches(&self, required: &GuaranteeSet) -> bool {
        if self.delivery < required.delivery
            || self.acknowledgement < required.acknowledgement
            || self.ordering < required.ordering
            || self.deduplication < required.deduplication
        {
            return false;
        }
        if self.backpressure != required.backpressure
            || self.producer_naming != required.producer_naming
        {
            return false;
        }
        if !self.control_isolated && required.control_isolated {
            return false;
        }
        match (self.durability, required.durability) {
            (_, None) => {}
            (Some(have), Some(want)) if have >= want => {}
            _ => return false,
        }
        match (self.replicas, required.replicas) {
            (_, None) => {}
            (Some(have), Some(want)) if have >= want => {}
            _ => return false,
        }
        match (self.dedup_window_ms, required.dedup_window_ms) {
            (_, None) => {}
            (Some(have), Some(want)) if have >= want => {}
            _ => return false,
        }
        true
    }
}

/// Maps a wire value to a level, naming the dimension when it is unknown.
fn level<T: WireLevel>(value: u64, dimension: &'static str) -> Result<T, HeaderError> {
    T::from_wire_value(value).ok_or(HeaderError::UnknownLevel { dimension, value })
}

/// Lets [`level`] work for every dimension enum without a macro per call.
trait WireLevel: Sized {
    fn from_wire_value(value: u64) -> Option<Self>;
}

macro_rules! impl_wire_level {
    ($($name:ident),+ $(,)?) => {
        $(impl WireLevel for $name {
            fn from_wire_value(value: u64) -> Option<$name> {
                $name::from_wire(value)
            }
        })+
    };
}

impl_wire_level!(
    Delivery,
    Acknowledgement,
    Durability,
    OrderingMode,
    Deduplication,
    Backpressure,
    ProducerNaming,
    ReportMode,
);

/// Why a header was rejected. Every variant is a protocol violation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeaderError {
    /// The bytes are not well-formed CBOR, or a value had the wrong type.
    Malformed(&'static str),
    /// An indefinite-length item was used where the protocol forbids it.
    Indefinite,
    /// A map key appeared twice.
    DuplicateKey(u64),
    /// A map key was not greater than the preceding one; keys must ascend.
    UnorderedKey(u64),
    /// A map key was not an unsigned integer.
    NonUintKey,
    /// A required key was absent.
    MissingKey(u64),
    /// A text field exceeded its cap.
    StringTooLong {
        /// Key of the offending field.
        key: u64,
        /// Length found.
        len: usize,
        /// Cap for this field.
        max: usize,
    },
    /// A list field declared more items than the decoder accepts.
    ListTooLong {
        /// Key of the offending field.
        key: u64,
        /// Declared item count.
        len: u64,
        /// Cap for list fields.
        max: usize,
    },
    /// An unknown field nested deeper than [`limits::MAX_SKIP_DEPTH`].
    DepthExceeded,
    /// Bytes remained after the header map.
    TrailingBytes,
    /// A guarantee set, or a DATA header's achieved level, named a level this
    /// version does not define.
    UnknownLevel {
        /// Dimension whose value was unknown.
        dimension: &'static str,
        /// The value found.
        value: u64,
    },
    /// A guarantee set's dimension combination is one §6.5 forbids, or a
    /// HELLO requires more than it offers (§6.1).
    InvalidGuarantees(&'static str),
    /// A topic filter violated the grammar of `docs/PROTOCOL.md` §6.4.
    InvalidFilter(&'static str),
    /// A DATA header's report order is malformed: the levels do not ascend,
    /// there are too many of them, or the order and its id disagree
    /// (`docs/PROTOCOL.md` §6.2, keys `9`-`11`).
    InvalidReport(&'static str),
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderError::Malformed(what) => write!(f, "malformed header: {what}"),
            HeaderError::Indefinite => f.write_str("indefinite-length items are not allowed"),
            HeaderError::DuplicateKey(k) => write!(f, "duplicate header key {k}"),
            HeaderError::UnorderedKey(k) => {
                write!(f, "header key {k} is out of ascending order")
            }
            HeaderError::NonUintKey => f.write_str("header key is not an unsigned integer"),
            HeaderError::MissingKey(k) => write!(f, "required header key {k} is missing"),
            HeaderError::StringTooLong { key, len, max } => {
                write!(
                    f,
                    "key {key}: text of {len} bytes exceeds the {max} byte cap"
                )
            }
            HeaderError::ListTooLong { key, len, max } => {
                write!(
                    f,
                    "key {key}: list of {len} items exceeds the {max} item cap"
                )
            }
            HeaderError::DepthExceeded => f.write_str("unknown field nested too deeply"),
            HeaderError::TrailingBytes => f.write_str("trailing bytes after the header"),
            HeaderError::UnknownLevel { dimension, value } => {
                write!(f, "unknown {dimension} level {value}")
            }
            HeaderError::InvalidGuarantees(why) => write!(f, "invalid guarantee set: {why}"),
            HeaderError::InvalidFilter(why) => write!(f, "invalid topic filter: {why}"),
            HeaderError::InvalidReport(reason) => write!(f, "invalid report: {reason}"),
        }
    }
}

impl std::error::Error for HeaderError {}

impl From<HeaderError> for Error {
    fn from(e: HeaderError) -> Error {
        Error::Protocol(e.to_string())
    }
}

/// Appends an encoded header to a buffer the caller owns.
///
/// Every `encode` in this file is a thin wrapper over an `encode_into` that
/// goes through here, so a hot send path can reuse one buffer and the
/// canonical form has exactly one implementation — the property that matters,
/// because two encoders that can disagree about key order would be a wire
/// divergence rather than an optimisation (B-250).
fn encode_into_with(
    out: &mut Vec<u8>,
    f: impl FnOnce(&mut Encoder<Vec<u8>>) -> Result<(), minicbor::encode::Error<Infallible>>,
) {
    // `Encoder` owns its writer, so the buffer is handed over and taken back.
    // `std::mem::take` keeps the caller's allocation: the `Vec` that comes
    // back is the same one, grown at most by this header.
    let mut e = Encoder::new(std::mem::take(out));
    f(&mut e).expect("encoding into a Vec is infallible");
    *out = e.into_writer();
}

/// Skips one CBOR value iteratively, refusing to recurse and refusing to nest
/// deeper than `max_depth`.
///
/// `minicbor` has its own `skip`, but the protocol requires a specific,
/// auditable bound on hostile nesting; a value that nests deeper is rejected
/// rather than tolerated.
fn skip_value(d: &mut Decoder<'_>, max_depth: usize) -> Result<(), HeaderError> {
    // `stack` holds the outstanding item counts of enclosing containers; its
    // length is the current nesting depth and is bounded by `max_depth`.
    let mut stack: Vec<u64> = Vec::new();
    let mut remaining: u64 = 1;

    loop {
        if remaining == 0 {
            match stack.pop() {
                Some(outer) => {
                    remaining = outer;
                    continue;
                }
                None => return Ok(()),
            }
        }
        remaining -= 1;

        let ty = d
            .datatype()
            .map_err(|_| HeaderError::Malformed("truncated value"))?;
        let nested = match ty {
            Type::Bool => {
                d.bool().map_err(|_| HeaderError::Malformed("bool"))?;
                None
            }
            Type::Null => {
                d.null().map_err(|_| HeaderError::Malformed("null"))?;
                None
            }
            Type::Undefined => {
                d.undefined()
                    .map_err(|_| HeaderError::Malformed("undefined"))?;
                None
            }
            Type::U8
            | Type::U16
            | Type::U32
            | Type::U64
            | Type::I8
            | Type::I16
            | Type::I32
            | Type::I64
            | Type::Int => {
                d.int().map_err(|_| HeaderError::Malformed("integer"))?;
                None
            }
            Type::F32 | Type::F64 => {
                d.f64().map_err(|_| HeaderError::Malformed("float"))?;
                None
            }
            Type::Bytes => {
                d.bytes()
                    .map_err(|_| HeaderError::Malformed("byte string"))?;
                None
            }
            Type::String => {
                d.str().map_err(|_| HeaderError::Malformed("text string"))?;
                None
            }
            Type::Array => Some(
                d.array()
                    .map_err(|_| HeaderError::Malformed("array"))?
                    .ok_or(HeaderError::Indefinite)?,
            ),
            Type::Map => {
                let pairs = d
                    .map()
                    .map_err(|_| HeaderError::Malformed("map"))?
                    .ok_or(HeaderError::Indefinite)?;
                Some(
                    pairs
                        .checked_mul(2)
                        .ok_or(HeaderError::Malformed("map length overflow"))?,
                )
            }
            Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef => {
                return Err(HeaderError::Indefinite);
            }
            Type::Break => return Err(HeaderError::Malformed("unexpected break")),
            // Tags, half-floats and other simple values carry no meaning in
            // weida headers; extensions must use plain data items.
            Type::Tag => return Err(HeaderError::Malformed("tags are not allowed")),
            Type::F16 => return Err(HeaderError::Malformed("half floats are not allowed")),
            Type::Simple => return Err(HeaderError::Malformed("simple values are not allowed")),
            Type::Unknown(_) => return Err(HeaderError::Malformed("unknown major type")),
        };

        if let Some(count) = nested
            && count > 0
        {
            if stack.len() >= max_depth {
                return Err(HeaderError::DepthExceeded);
            }
            stack.push(remaining);
            remaining = count;
        }
    }
}

/// Reader for one strict header map.
struct MapReader<'a, 'b> {
    d: &'a mut Decoder<'b>,
    remaining: u64,
    /// Bitmask of seen keys `0..=63`, for the required-key checks. The
    /// specification reserves that range, so presence needs no allocation.
    seen: u64,
    /// Previously read key.
    ///
    /// Keys are required to ascend strictly, which makes duplicate detection
    /// complete for *every* key — including extension keys the decoder skips —
    /// in constant space. A set of seen extension keys would be exactly the
    /// remote-controlled allocation the invariants forbid.
    last: Option<u64>,
}

impl<'a, 'b> MapReader<'a, 'b> {
    fn new(d: &'a mut Decoder<'b>) -> Result<MapReader<'a, 'b>, HeaderError> {
        let len = d
            .map()
            .map_err(|_| HeaderError::Malformed("header is not a map"))?
            .ok_or(HeaderError::Indefinite)?;
        Ok(MapReader {
            d,
            remaining: len,
            seen: 0,
            last: None,
        })
    }

    fn next_key(&mut self) -> Result<Option<u64>, HeaderError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        match self.d.datatype() {
            Ok(Type::U8 | Type::U16 | Type::U32 | Type::U64) => {}
            Ok(_) => return Err(HeaderError::NonUintKey),
            Err(_) => return Err(HeaderError::Malformed("truncated key")),
        }
        let key = self.d.u64().map_err(|_| HeaderError::NonUintKey)?;
        if let Some(prev) = self.last {
            if key == prev {
                return Err(HeaderError::DuplicateKey(key));
            }
            if key < prev {
                return Err(HeaderError::UnorderedKey(key));
            }
        }
        self.last = Some(key);
        if key < 64 {
            self.seen |= 1u64 << key;
        }
        Ok(Some(key))
    }

    fn saw(&self, key: u64) -> bool {
        key < 64 && self.seen & (1u64 << key) != 0
    }

    fn require(&self, key: u64) -> Result<(), HeaderError> {
        if self.saw(key) {
            Ok(())
        } else {
            Err(HeaderError::MissingKey(key))
        }
    }

    fn u64(&mut self) -> Result<u64, HeaderError> {
        self.d
            .u64()
            .map_err(|_| HeaderError::Malformed("expected an unsigned integer"))
    }

    fn text(&mut self, key: u64, max: usize) -> Result<String, HeaderError> {
        let s = self
            .d
            .str()
            .map_err(|_| HeaderError::Malformed("expected a text string"))?;
        if s.len() > max {
            return Err(HeaderError::StringTooLong {
                key,
                len: s.len(),
                max,
            });
        }
        Ok(s.to_owned())
    }

    /// Reads a byte string of exactly `N` bytes.
    ///
    /// The cap is checked before the length is trusted for anything, and a
    /// shorter value is rejected rather than padded: `docs/PROTOCOL.md` §6.2
    /// defines the one field that uses this as a raw 32-byte digest, and half
    /// a digest identifies nobody.
    fn byte_array<const N: usize>(&mut self, key: u64) -> Result<[u8; N], HeaderError> {
        let bytes = self
            .d
            .bytes()
            .map_err(|_| HeaderError::Malformed("expected a byte string"))?;
        if bytes.len() > N {
            return Err(HeaderError::StringTooLong {
                key,
                len: bytes.len(),
                max: N,
            });
        }
        bytes
            .try_into()
            .map_err(|_| HeaderError::Malformed("byte string has the wrong length"))
    }

    fn uint_list(&mut self, key: u64) -> Result<Vec<u64>, HeaderError> {
        let len = self
            .d
            .array()
            .map_err(|_| HeaderError::Malformed("expected an array"))?
            .ok_or(HeaderError::Indefinite)?;
        if len > limits::MAX_LIST_ITEMS as u64 {
            return Err(HeaderError::ListTooLong {
                key,
                len,
                max: limits::MAX_LIST_ITEMS,
            });
        }
        // `len` is now bounded by MAX_LIST_ITEMS, so reserving is safe.
        let mut out = Vec::with_capacity(len as usize);
        for _ in 0..len {
            out.push(self.u64()?);
        }
        Ok(out)
    }

    /// Reads a report order: a definite-length array of strictly ascending
    /// cursor levels, capped at [`limits::MAX_REPORT_LEVELS`].
    ///
    /// Ascent is checked here rather than after the fact for the same reason
    /// map keys are: it makes duplicate detection complete in constant space,
    /// and it makes the wire form canonical, so two peers ordering the same
    /// levels send the same bytes.
    fn report_levels(&mut self) -> Result<Vec<CursorLevel>, HeaderError> {
        let len = self
            .d
            .array()
            .map_err(|_| HeaderError::Malformed("expected an array"))?
            .ok_or(HeaderError::Indefinite)?;
        if len > limits::MAX_REPORT_LEVELS as u64 {
            return Err(HeaderError::InvalidReport("too many report levels"));
        }
        // `len` is now bounded by MAX_REPORT_LEVELS, so reserving is safe.
        let mut out: Vec<CursorLevel> = Vec::with_capacity(len as usize);
        let mut last: Option<u64> = None;
        for _ in 0..len {
            let value = self.u64()?;
            if let Some(prev) = last
                && value <= prev
            {
                return Err(HeaderError::InvalidReport("report levels must ascend"));
            }
            last = Some(value);
            out.push(
                CursorLevel::from_wire(value).ok_or(HeaderError::UnknownLevel {
                    dimension: "report",
                    value,
                })?,
            );
        }
        Ok(out)
    }

    fn skip(&mut self) -> Result<(), HeaderError> {
        skip_value(self.d, limits::MAX_SKIP_DEPTH)
    }
}

/// Rejects trailing bytes after a header map.
fn finish(d: &Decoder<'_>) -> Result<(), HeaderError> {
    if d.position() == d.input().len() {
        Ok(())
    } else {
        Err(HeaderError::TrailingBytes)
    }
}

/// HELLO header: connection negotiation input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    /// Wire protocol versions the sender supports.
    pub versions: Vec<u64>,
    /// Largest header the sender is willing to receive.
    pub max_header_bytes: u64,
    /// Advisory concurrent inbound transfer count.
    pub max_transfers: u64,
    /// Capability codes the sender supports.
    pub capabilities: Vec<u64>,
    /// Capability codes the sender requires the peer to support.
    pub required_capabilities: Vec<u64>,
    /// Guarantee set the sender can honour (key `5`).
    ///
    /// `None` means the default set: an absent key and
    /// [`GuaranteeSet::CORE`] are the same declaration, which is why a v0
    /// HELLO is unchanged on the wire.
    pub guarantees_offered: Option<GuaranteeSet>,
    /// Guarantee set the sender requires of the peer (key `6`).
    ///
    /// MUST be reachable by `guarantees_offered` on every dimension: requiring
    /// what you cannot honour yourself is a configuration error
    /// (`docs/PROTOCOL.md` §6.1), and a decoder rejects it.
    pub guarantees_required: Option<GuaranteeSet>,
}

impl Hello {
    /// The HELLO a v0 implementation sends: no guarantee declarations, so
    /// `core` offered and `core` required.
    pub fn v0(max_header_bytes: u64, max_transfers: u64) -> Hello {
        Hello {
            versions: vec![crate::VERSION],
            max_header_bytes,
            max_transfers,
            capabilities: Vec::new(),
            required_capabilities: Vec::new(),
            guarantees_offered: None,
            guarantees_required: None,
        }
    }

    /// The set this HELLO offers; an absent declaration means `core`.
    pub fn offered(&self) -> GuaranteeSet {
        self.guarantees_offered.unwrap_or(GuaranteeSet::CORE)
    }

    /// The set this HELLO requires; an absent declaration means `core`.
    pub fn required(&self) -> GuaranteeSet {
        self.guarantees_required.unwrap_or(GuaranteeSet::CORE)
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`, for a send path that reuses a
    /// buffer (B-250). The canonical form has one implementation and this is
    /// it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        encode_into_with(out, |e| {
            // A `core` declaration is never written: an absent key already
            // says it, and a v0 HELLO must stay byte-identical (§6.1).
            let offered = self.guarantees_offered.filter(|s| !s.is_core());
            let required = self.guarantees_required.filter(|s| !s.is_core());
            e.map(5 + u64::from(offered.is_some()) + u64::from(required.is_some()))?;
            e.u64(hello_key::VERSIONS)?
                .array(self.versions.len() as u64)?;
            for v in &self.versions {
                e.u64(*v)?;
            }
            e.u64(hello_key::MAX_HEADER_BYTES)?
                .u64(self.max_header_bytes)?;
            e.u64(hello_key::MAX_TRANSFERS)?.u64(self.max_transfers)?;
            e.u64(hello_key::CAPABILITIES)?
                .array(self.capabilities.len() as u64)?;
            for c in &self.capabilities {
                e.u64(*c)?;
            }
            e.u64(hello_key::REQUIRED_CAPABILITIES)?
                .array(self.required_capabilities.len() as u64)?;
            for c in &self.required_capabilities {
                e.u64(*c)?;
            }
            if let Some(set) = offered {
                e.u64(hello_key::GUARANTEES_OFFERED)?;
                set.encode_into(e)?;
            }
            if let Some(set) = required {
                e.u64(hello_key::GUARANTEES_REQUIRED)?;
                set.encode_into(e)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<Hello, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut versions = Vec::new();
        let mut max_header_bytes = 0;
        let mut max_transfers = 0;
        let mut capabilities = Vec::new();
        let mut required_capabilities = Vec::new();
        let mut guarantees_offered = None;
        let mut guarantees_required = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    hello_key::VERSIONS => versions = m.uint_list(key)?,
                    hello_key::MAX_HEADER_BYTES => max_header_bytes = m.u64()?,
                    hello_key::MAX_TRANSFERS => max_transfers = m.u64()?,
                    hello_key::CAPABILITIES => capabilities = m.uint_list(key)?,
                    hello_key::REQUIRED_CAPABILITIES => required_capabilities = m.uint_list(key)?,
                    hello_key::GUARANTEES_OFFERED => {
                        guarantees_offered = Some(GuaranteeSet::decode_from(&mut m)?);
                    }
                    hello_key::GUARANTEES_REQUIRED => {
                        guarantees_required = Some(GuaranteeSet::decode_from(&mut m)?);
                    }
                    _ => m.skip()?,
                }
            }
            for key in [
                hello_key::VERSIONS,
                hello_key::MAX_HEADER_BYTES,
                hello_key::MAX_TRANSFERS,
                hello_key::CAPABILITIES,
                hello_key::REQUIRED_CAPABILITIES,
            ] {
                m.require(key)?;
            }
        }
        finish(&d)?;
        let hello = Hello {
            versions,
            max_header_bytes,
            max_transfers,
            capabilities,
            required_capabilities,
            guarantees_offered,
            guarantees_required,
        };
        // §6.1: requiring more than you offer is a configuration error, and
        // one a decoder can see in a single header.
        if !hello.offered().reaches(&hello.required()) {
            return Err(HeaderError::InvalidGuarantees(
                "guarantees_required is not covered by guarantees_offered",
            ));
        }
        Ok(hello)
    }
}

/// DATA header: one transfer.
///
/// Every field is optional at the decoder. Which of them the *context*
/// requires is a dispatch question: an initiating stream must name an endpoint
/// and the reply half of an exchange must not, but the decoder sees bytes, not
/// streams (`docs/PROTOCOL.md` §6.2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataHeader {
    /// Endpoint path. Required on an initiating stream, ignored on a reply.
    pub endpoint: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// W3C `traceparent`.
    pub traceparent: Option<String>,
    /// W3C `tracestate`, opaque passthrough.
    pub tracestate: Option<String>,
    /// Pub/Sub topic; opaque bytes, selected by the filter grammar of
    /// `docs/PROTOCOL.md` §6.4. Only meaningful on transfers fanned out by a
    /// publisher.
    pub topic: Option<String>,
    /// Per-producer sequence number, for ordering and gap detection
    /// (`docs/PROTOCOL.md` §6.2, key `6`).
    ///
    /// Written by a publisher whose connection negotiated `PerProducer`
    /// ordering, and by nothing under `core`: the number is assigned once per
    /// published message, before fan-out, so a copy a subscriber lost shows up
    /// as a hole in its own sequence. It is not a transfer identifier and
    /// correlates nothing — an exchange is correlated by its stream.
    pub sequence: Option<u64>,
    /// Producer identity: the raw 32-byte digest (`docs/PROTOCOL.md` §6.2,
    /// key `7`).
    ///
    /// **Specified ahead of code**, and absent in the default case by design:
    /// the receiver already knows the sending peer's proved fingerprint from
    /// the handshake, so this names a producer only where it is *not* the
    /// connection peer — a relay, or a name an L2 subscription supplies
    /// ([decisions/0008](../../../docs/decisions/0008-session-identity.md)
    /// §4.4). The `sha256:<64 hex>` spelling is presentation only and never
    /// goes on the wire.
    pub producer: Option<[u8; limits::PRODUCER_BYTES]>,
    /// The completion level the sender **achieved** for the message it is
    /// answering (`docs/PROTOCOL.md` §6.2, key `8`).
    ///
    /// This is the L2 confirm, and it is a statement about one hop: a broker
    /// that has taken responsibility for a message in memory writes
    /// [`Acknowledgement::Accepted`] on the reply half of the producer's
    /// exchange, which is what makes the reply a publisher confirm without a
    /// frame kind of its own
    /// ([decisions/0018](../../../docs/decisions/0018-minimal-broker.md)
    /// §4.6). It is *achieved*, never requested — a level a peer wants is
    /// negotiated in HELLO and refused there if it cannot be reached
    /// ([0006](../../../docs/decisions/0006-guarantee-sets.md) §4.4) — and it
    /// is never relayed: the producer's confirm says nothing about what a
    /// consumer later does with the message
    /// ([GUARANTEES.md] §2).
    ///
    /// A v0 sender leaves it absent, and an absent key is not
    /// `Acknowledgement::None`: it says this hop makes no claim beyond the
    /// transport receipt QUIC already gave.
    ///
    /// [GUARANTEES.md]: https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/GUARANTEES.md
    pub achieved: Option<Acknowledgement>,
    /// Identifier the sender assigns to the report it orders (key `9`).
    ///
    /// Present exactly when [`DataHeader::report`] is non-empty. It names the
    /// CURSOR stream that will report on *this* transfer, and it is scoped to
    /// the connection and to the direction that allocated it: a peer reports
    /// only on transfers it received, so the two directions cannot collide.
    pub report_id: Option<u64>,
    /// Levels the sender asks to be reported, strictly ascending (key `10`).
    ///
    /// An **order**, not a guarantee: a receiver that cannot reach a level
    /// simply does not report it, and the transfer does not fail for it. A
    /// level a peer must reach is the negotiated `acknowledgement` dimension
    /// of HELLO instead
    /// ([decisions/0006](../../../docs/decisions/0006-guarantee-sets.md)
    /// §4.4).
    pub report: Vec<CursorLevel>,
    /// How often the reporter should emit a record (key `11`).
    ///
    /// [`ReportMode::Progress`] is the default and is never written.
    pub report_mode: ReportMode,
    /// Segment number per `(sender, path, topic)`, from 0 (key `13`).
    ///
    /// Written by a radio, or by a dialling peer's `Peer::segment` toward a
    /// bound path
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.6). It is not a `PerProducer` sequence: it is written whatever
    /// ordering was negotiated, and a dish uses it to discard a segment older
    /// than the newest it delivered.
    pub segment: Option<u64>,
}

impl DataHeader {
    /// A header addressing `endpoint`, for the initiating half of a stream.
    pub fn addressed(endpoint: impl Into<String>) -> DataHeader {
        DataHeader {
            endpoint: Some(endpoint.into()),
            ..DataHeader::default()
        }
    }

    /// A header for the reply half of an exchange: no endpoint, no topic.
    ///
    /// The stream is the correlation, so a reply carries no identifier of the
    /// request it answers.
    pub fn reply() -> DataHeader {
        DataHeader::default()
    }

    /// Encodes the header.
    ///
    /// Key `10` goes out in the canonical form §6.2 makes normative —
    /// strictly ascending by wire value, no repeats — whatever order
    /// [`DataHeader::report`] happens to hold. That rule is enforced here
    /// because encoding cannot fail: a vector in any other order would
    /// otherwise produce bytes that close the connection at every conformant
    /// peer, and there would be no way to tell the caller so.
    ///
    /// The report's other two rules stay the caller's for exactly that
    /// reason — both need an error, and this function has none to give. At
    /// most [`limits::MAX_REPORT_LEVELS`] distinct levels, and key `9`
    /// present exactly when key `10` is: `weida`'s `data_header` refuses an
    /// oversized order with `Error::LimitExceeded` and allocates the report
    /// id alongside the order, so no caller reaches this encoder with either
    /// mistake.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`, for a send path that reuses a
    /// buffer (B-250). The canonical form has one implementation and this is
    /// it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        // Sorted and deduplicated by wire value, not by the enum's derived
        // order, because the wire value is what ascends on the wire.
        let mut report: Vec<u64> = self.report.iter().map(|level| level.to_wire()).collect();
        report.sort_unstable();
        report.dedup();
        let count = u64::from(self.endpoint.is_some())
            + u64::from(self.content_len.is_some())
            + u64::from(self.content_type.is_some())
            + u64::from(self.traceparent.is_some())
            + u64::from(self.tracestate.is_some())
            + u64::from(self.topic.is_some())
            + u64::from(self.sequence.is_some())
            + u64::from(self.producer.is_some())
            + u64::from(self.achieved.is_some())
            + u64::from(self.report_id.is_some())
            + u64::from(!report.is_empty())
            + u64::from(self.report_mode != ReportMode::default())
            + u64::from(self.segment.is_some());
        encode_into_with(out, |e| {
            e.map(count)?;
            if let Some(endpoint) = &self.endpoint {
                e.u64(data_key::ENDPOINT)?.str(endpoint)?;
            }
            if let Some(len) = self.content_len {
                e.u64(data_key::CONTENT_LEN)?.u64(len)?;
            }
            if let Some(ct) = &self.content_type {
                e.u64(data_key::CONTENT_TYPE)?.str(ct)?;
            }
            if let Some(tp) = &self.traceparent {
                e.u64(data_key::TRACEPARENT)?.str(tp)?;
            }
            if let Some(ts) = &self.tracestate {
                e.u64(data_key::TRACESTATE)?.str(ts)?;
            }
            if let Some(topic) = &self.topic {
                e.u64(data_key::TOPIC)?.str(topic)?;
            }
            // Keys 6 and 7 are written only when set, which for every v0
            // sender means never: nothing in `weida` populates them yet.
            if let Some(sequence) = self.sequence {
                e.u64(data_key::SEQUENCE)?.u64(sequence)?;
            }
            if let Some(producer) = &self.producer {
                e.u64(data_key::PRODUCER)?.bytes(producer)?;
            }
            if let Some(achieved) = self.achieved {
                e.u64(data_key::ACHIEVED)?.u64(achieved.to_wire())?;
            }
            if let Some(report_id) = self.report_id {
                e.u64(data_key::REPORT_ID)?.u64(report_id)?;
            }
            if !report.is_empty() {
                e.u64(data_key::REPORT)?.array(report.len() as u64)?;
                for value in &report {
                    e.u64(*value)?;
                }
            }
            // `Progress` is never written: an absent key already says it, so
            // a header that orders a report in the default mode stays as
            // short as the mode is uninteresting (§6.5's rule for levels).
            if self.report_mode != ReportMode::default() {
                e.u64(data_key::REPORT_MODE)?
                    .u64(self.report_mode.to_wire())?;
            }
            if let Some(segment) = self.segment {
                e.u64(data_key::SEGMENT)?.u64(segment)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<DataHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut header = DataHeader::default();
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    data_key::ENDPOINT => {
                        header.endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?)
                    }
                    data_key::CONTENT_LEN => header.content_len = Some(m.u64()?),
                    data_key::CONTENT_TYPE => {
                        header.content_type = Some(m.text(key, limits::MAX_CONTENT_TYPE_BYTES)?)
                    }
                    data_key::TRACEPARENT => {
                        header.traceparent = Some(m.text(key, limits::MAX_TRACEPARENT_BYTES)?)
                    }
                    data_key::TRACESTATE => {
                        header.tracestate = Some(m.text(key, limits::MAX_TRACESTATE_BYTES)?)
                    }
                    data_key::TOPIC => header.topic = Some(m.text(key, limits::MAX_TOPIC_BYTES)?),
                    data_key::SEQUENCE => header.sequence = Some(m.u64()?),
                    data_key::PRODUCER => header.producer = Some(m.byte_array(key)?),
                    // An unknown level is not a level: a peer naming one this
                    // version does not define is refused rather than silently
                    // read as the weakest, because the value decides what a
                    // producer believes about its message.
                    data_key::ACHIEVED => {
                        let value = m.u64()?;
                        header.achieved = Some(Acknowledgement::from_wire(value).ok_or(
                            HeaderError::UnknownLevel {
                                dimension: "achieved",
                                value,
                            },
                        )?);
                    }
                    data_key::REPORT_ID => header.report_id = Some(m.u64()?),
                    data_key::REPORT => header.report = m.report_levels()?,
                    data_key::REPORT_MODE => {
                        header.report_mode = level(m.u64()?, "report_mode")?;
                    }
                    data_key::SEGMENT => header.segment = Some(m.u64()?),
                    _ => m.skip()?,
                }
            }
        }
        finish(&d)?;
        // Keys 9 and 10 are one statement in two halves: an order with no
        // stream to report on, or a stream with nothing to report, names a
        // report nobody can serve.
        if !header.report.is_empty() && header.report_id.is_none() {
            return Err(HeaderError::InvalidReport("report without report_id"));
        }
        if header.report_id.is_some() && header.report.is_empty() {
            return Err(HeaderError::InvalidReport("report_id without report"));
        }
        Ok(header)
    }
}

/// ERROR header.
///
/// Legal only on the reply half of a bidirectional stream: an ERROR is the
/// alternative to a reply, so it needs no reference to what it answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorHeader {
    /// Raw error code.
    pub code: u64,
    /// Human-readable detail; never machine-interpreted.
    pub message: Option<String>,
}

impl ErrorHeader {
    /// Builds a header for a known error code.
    pub fn new(code: weida_core::ErrorCode) -> ErrorHeader {
        ErrorHeader {
            code: code.to_wire(),
            message: None,
        }
    }

    /// The error code, or `None` for an unknown one.
    pub fn error_code(&self) -> Option<weida_core::ErrorCode> {
        weida_core::ErrorCode::from_wire(self.code)
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`, for a send path that reuses a
    /// buffer (B-250). The canonical form has one implementation and this is
    /// it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let count = 1 + u64::from(self.message.is_some());
        encode_into_with(out, |e| {
            e.map(count)?;
            e.u64(error_key::CODE)?.u64(self.code)?;
            if let Some(msg) = &self.message {
                e.u64(error_key::MESSAGE)?.str(msg)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<ErrorHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut code = 0;
        let mut message = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    error_key::CODE => code = m.u64()?,
                    error_key::MESSAGE => message = Some(m.text(key, limits::MAX_MESSAGE_BYTES)?),
                    _ => m.skip()?,
                }
            }
            m.require(error_key::CODE)?;
        }
        finish(&d)?;
        Ok(ErrorHeader { code, message })
    }
}

/// SUBSCRIBE and UNSUBSCRIBE header.
///
/// Both frames carry the same two keys: the publisher path to (un)subscribe on
/// and the topic filter. The filter is a segmented pattern, not a byte prefix
/// ([`filter`]): the empty filter matches every topic, `*` matches one whole
/// segment and a trailing `#` matches zero or more.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionHeader {
    /// Publisher endpoint path.
    pub endpoint: String,
    /// Topic filter; the empty string matches everything. A decoded header's
    /// filter has passed [`filter::validate`].
    pub filter: String,
    /// The dish's latency budget in milliseconds (key `2`), meaningful on a
    /// RADIO path only
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.6). Encoded only when present.
    pub max_age_ms: Option<u64>,
}

impl SubscriptionHeader {
    /// A header for `endpoint` and `filter`.
    pub fn new(endpoint: impl Into<String>, filter: impl Into<String>) -> SubscriptionHeader {
        SubscriptionHeader {
            endpoint: endpoint.into(),
            filter: filter.into(),
            max_age_ms: None,
        }
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`, for a send path that reuses a
    /// buffer (B-250). The canonical form has one implementation and this is
    /// it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        // Keys 0 and 1 are required, so neither is elided: an absent filter
        // and an empty filter would otherwise be indistinguishable on the
        // wire, and the empty filter is the "everything" subscription. Key 2
        // is optional and written only when set.
        encode_into_with(out, |e| {
            e.map(2 + u64::from(self.max_age_ms.is_some()))?;
            e.u64(subscription_key::ENDPOINT)?.str(&self.endpoint)?;
            e.u64(subscription_key::FILTER)?.str(&self.filter)?;
            if let Some(max_age_ms) = self.max_age_ms {
                e.u64(subscription_key::MAX_AGE_MS)?.u64(max_age_ms)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<SubscriptionHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut endpoint = None;
        let mut filter = None;
        let mut max_age_ms = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    subscription_key::ENDPOINT => {
                        endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?)
                    }
                    subscription_key::FILTER => {
                        filter = Some(m.text(key, limits::MAX_FILTER_BYTES)?)
                    }
                    subscription_key::MAX_AGE_MS => max_age_ms = Some(m.u64()?),
                    _ => m.skip()?,
                }
            }
            m.require(subscription_key::ENDPOINT)?;
            m.require(subscription_key::FILTER)?;
        }
        // The grammar is checked here, at the codec boundary, so no matcher
        // ever sees a filter it would have to interpret twice; an illegal one
        // closes the connection with `PROTOCOL_VIOLATION`
        // (`docs/PROTOCOL.md` §6.4).
        let filter = filter.expect("presence checked above");
        filter::validate(&filter)?;
        finish(&d)?;
        Ok(SubscriptionHeader {
            endpoint: endpoint.expect("presence checked above"),
            filter,
            max_age_ms,
        })
    }
}

/// FLOW header (kind `7`).
///
/// The registration of a datagram flow: the stream it opens is the flow's
/// lifetime, and the flow id prefixes every datagram the flow carries
/// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.2).
/// The optional keys reuse DATA's meaning and caps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowHeader {
    /// Endpoint path the flow is addressed to (key `0`, required).
    pub endpoint: String,
    /// Flow id, chosen by the sender and unique per connection and
    /// direction (key `1`, required).
    pub flow: u64,
    /// Opaque media type label (key `2`).
    pub content_type: Option<String>,
    /// W3C Trace Context `traceparent` (key `3`).
    pub traceparent: Option<String>,
    /// W3C Trace Context `tracestate` (key `4`).
    pub tracestate: Option<String>,
    /// Topic the flow carries (key `5`).
    pub topic: Option<String>,
}

impl FlowHeader {
    /// A header for flow `flow` addressed to `endpoint`, with no optional key.
    pub fn new(endpoint: impl Into<String>, flow: u64) -> FlowHeader {
        FlowHeader {
            endpoint: endpoint.into(),
            flow,
            content_type: None,
            traceparent: None,
            tracestate: None,
            topic: None,
        }
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`. The canonical form has one
    /// implementation and this is it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let count = 2
            + u64::from(self.content_type.is_some())
            + u64::from(self.traceparent.is_some())
            + u64::from(self.tracestate.is_some())
            + u64::from(self.topic.is_some());
        encode_into_with(out, |e| {
            e.map(count)?;
            e.u64(flow_key::ENDPOINT)?.str(&self.endpoint)?;
            e.u64(flow_key::FLOW)?.u64(self.flow)?;
            if let Some(ct) = &self.content_type {
                e.u64(flow_key::CONTENT_TYPE)?.str(ct)?;
            }
            if let Some(tp) = &self.traceparent {
                e.u64(flow_key::TRACEPARENT)?.str(tp)?;
            }
            if let Some(ts) = &self.tracestate {
                e.u64(flow_key::TRACESTATE)?.str(ts)?;
            }
            if let Some(topic) = &self.topic {
                e.u64(flow_key::TOPIC)?.str(topic)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<FlowHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut endpoint = None;
        let mut flow = None;
        let mut content_type = None;
        let mut traceparent = None;
        let mut tracestate = None;
        let mut topic = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    flow_key::ENDPOINT => endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?),
                    flow_key::FLOW => flow = Some(m.u64()?),
                    flow_key::CONTENT_TYPE => {
                        content_type = Some(m.text(key, limits::MAX_CONTENT_TYPE_BYTES)?)
                    }
                    flow_key::TRACEPARENT => {
                        traceparent = Some(m.text(key, limits::MAX_TRACEPARENT_BYTES)?)
                    }
                    flow_key::TRACESTATE => {
                        tracestate = Some(m.text(key, limits::MAX_TRACESTATE_BYTES)?)
                    }
                    flow_key::TOPIC => topic = Some(m.text(key, limits::MAX_TOPIC_BYTES)?),
                    _ => m.skip()?,
                }
            }
            m.require(flow_key::ENDPOINT)?;
            m.require(flow_key::FLOW)?;
        }
        finish(&d)?;
        Ok(FlowHeader {
            endpoint: endpoint.expect("presence checked above"),
            flow: flow.expect("presence checked above"),
            content_type,
            traceparent,
            tracestate,
            topic,
        })
    }
}

/// CREDIT header (kind `5`).
///
/// The L2 credit of
/// [decisions/0003](../../../docs/decisions/0003-credit-unit.md) §4.2-§4.3:
/// which subscription, and how many messages that subscription will accept in
/// total. Three keys, all required — a subscription is `(endpoint, filter)`
/// on the connection the frame arrives on, and an absent limit would be
/// indistinguishable from a limit of zero, which is the pause.
///
/// **The limit is absolute and cumulative, not a delta.** It counts messages
/// delivered on that subscription since it was created, so a lost frame costs
/// nothing and a duplicated one changes nothing. It is also **monotone at the
/// receiver**: a broker keeps the highest limit it has seen, which is what
/// makes a reordered frame harmless on a transport that does not order the
/// streams control frames ride. Monotone is the whole rule: a receiver
/// ignores any limit that is not strictly greater than the standing one, so
/// restating a number already delivered changes nothing unless the
/// subscription had exhausted its credit anyway. **v0 offers no way to lower
/// a standing limit.** The only pause is the `0` a fresh subscription starts
/// at, so a consumer that wants to stay in control grants in increments it
/// is willing to receive. AMQP 1.0 can shrink `link-credit` against an
/// absolute baseline [amqp10 §5.1]; this frame cannot, and a peer that reads
/// it as if it could would wait for a stop no broker can deliver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreditHeader {
    /// Endpoint path of the queue the subscription is on.
    pub endpoint: String,
    /// Topic filter of the subscription. A decoded header's filter has passed
    /// [`filter::validate`].
    pub filter: String,
    /// Messages this subscription will accept in total, counted from its
    /// creation.
    pub limit: u64,
}

impl CreditHeader {
    /// A header granting `limit` to the subscription `(endpoint, filter)`.
    pub fn new(endpoint: impl Into<String>, filter: impl Into<String>, limit: u64) -> CreditHeader {
        CreditHeader {
            endpoint: endpoint.into(),
            filter: filter.into(),
            limit,
        }
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`, for a send path that reuses a
    /// buffer (B-250). The canonical form has one implementation and this is
    /// it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        encode_into_with(out, |e| {
            e.map(3)?;
            e.u64(credit_key::ENDPOINT)?.str(&self.endpoint)?;
            e.u64(credit_key::FILTER)?.str(&self.filter)?;
            e.u64(credit_key::LIMIT)?.u64(self.limit)?;
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<CreditHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut endpoint = None;
        let mut filter = None;
        let mut limit = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    credit_key::ENDPOINT => {
                        endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?)
                    }
                    credit_key::FILTER => filter = Some(m.text(key, limits::MAX_FILTER_BYTES)?),
                    credit_key::LIMIT => limit = Some(m.u64()?),
                    _ => m.skip()?,
                }
            }
            m.require(credit_key::ENDPOINT)?;
            m.require(credit_key::FILTER)?;
            m.require(credit_key::LIMIT)?;
        }
        // Same boundary as SUBSCRIBE: a filter that does not name a
        // subscription cannot grant credit to one.
        let filter = filter.expect("presence checked above");
        filter::validate(&filter)?;
        finish(&d)?;
        Ok(CreditHeader {
            endpoint: endpoint.expect("presence checked above"),
            filter,
            limit: limit.expect("presence checked above"),
        })
    }
}

/// CURSOR head frame (kind `6`).
///
/// One key, required: which report this stream carries. The stream then
/// carries `(level, offset)` records until FIN and **never any payload**
/// ([decisions/0024](../../../docs/decisions/0024-three-families-one-back-channel.md)
/// §4.4) — which is why the head frame needs no length, no endpoint and no
/// correlation beyond the id the DATA header allocated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorHeader {
    /// The `report_id` of the DATA header that ordered this report.
    pub report_id: u64,
}

impl CursorHeader {
    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends the encoded header to `out`, for a send path that reuses a
    /// buffer (B-250). The canonical form has one implementation and this is
    /// it; [`Self::encode`] is a wrapper.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        encode_into_with(out, |e| {
            e.map(1)?;
            e.u64(cursor_key::REPORT_ID)?.u64(self.report_id)?;
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<CursorHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut report_id = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    cursor_key::REPORT_ID => report_id = Some(m.u64()?),
                    _ => m.skip()?,
                }
            }
            m.require(cursor_key::REPORT_ID)?;
        }
        finish(&d)?;
        Ok(CursorHeader {
            report_id: report_id.expect("presence checked above"),
        })
    }
}

/// Longest possible cursor record: two 8-byte QUIC varints.
pub const MAX_CURSOR_RECORD_LEN: usize = 2 * crate::varint::MAX_ENCODED_LEN;

/// Appends one `(level, offset)` record to `out`.
///
/// Records are QUIC varint pairs rather than CBOR: a record is hot-path and
/// self-delimiting, and a CBOR map per record would cost a map header per
/// reported byte range for no gain — the head frame already carries every
/// field a record could need to name.
pub fn encode_cursor_record(
    level: CursorLevel,
    offset: u64,
    out: &mut Vec<u8>,
) -> Result<(), VarintError> {
    encode_varint(level.to_wire(), out)?;
    encode_varint(offset, out)
}

/// Decodes one record from the front of `input`.
///
/// `Ok(None)` means the input ends inside a record: read more bytes and
/// retry. That is **not** a violation — a reader sees whatever slice the
/// transport handed it, and a record is at most
/// [`MAX_CURSOR_RECORD_LEN`] bytes, so the retry is bounded. An undefined
/// level in the reserved range *is* a violation: the value decides what the
/// receiver believes about its own transfer.
pub fn decode_cursor_record(
    input: &[u8],
) -> Result<Option<(CursorLevel, u64, usize)>, HeaderError> {
    // `decode_varint` accepts the whole representable range, so its only
    // failure is a value the input ended inside of.
    let Ok((raw_level, level_len)) = decode_varint(input) else {
        return Ok(None);
    };
    let Ok((offset, offset_len)) = decode_varint(&input[level_len..]) else {
        return Ok(None);
    };
    let level = CursorLevel::from_wire(raw_level).ok_or(HeaderError::UnknownLevel {
        dimension: "cursor",
        value: raw_level,
    })?;
    Ok(Some((level, offset, level_len + offset_len)))
}

#[cfg(test)]
mod tests {
    /// The tests build headers by hand; production code goes through each
    /// type's `encode_into`.
    fn encode_with(
        f: impl FnOnce(
            &mut minicbor::Encoder<Vec<u8>>,
        ) -> Result<(), minicbor::encode::Error<std::convert::Infallible>>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        super::encode_into_with(&mut out, f);
        out
    }

    use super::*;
    use weida_core::ErrorCode;

    // --- golden vectors, docs/PROTOCOL.md §8 ------------------------------

    #[test]
    fn golden_data_request_header() {
        let h = DataHeader::addressed("/t");
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA1, 0x00, 0x62, 0x2F, 0x74]);
        assert_eq!(bytes.len(), 0x05);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_data_reply_header() {
        // The stream is the correlation, so a reply header is an empty map.
        let h = DataHeader::reply();
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA0]);
        assert_eq!(bytes.len(), 0x01);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_hello_header() {
        let h = Hello::v0(16384, 1024);
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA5, 0x00, 0x81, 0x00, 0x01, 0x19, 0x40, 0x00, 0x02, 0x19, 0x04, 0x00, 0x03, 0x80,
                0x04, 0x80
            ]
        );
        assert_eq!(bytes.len(), 0x10);
        assert_eq!(Hello::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_error_header() {
        let h = ErrorHeader::new(ErrorCode::NoReply);
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA1, 0x00, 0x05]);
        assert_eq!(bytes.len(), 0x03);
        assert_eq!(ErrorHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_pub_copy_data_header() {
        let mut h = DataHeader::addressed("/md");
        h.topic = Some("px.eur".into());
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x05, 0x66, 0x70, 0x78, 0x2E, 0x65, 0x75, 0x72
            ]
        );
        assert_eq!(bytes.len(), 0x0E);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    /// The digest of the §8 vectors: SHA-256 of `"test"`, the value the
    /// address examples in `docs/PROTOCOL.md` already use.
    const VECTOR_PRODUCER: [u8; limits::PRODUCER_BYTES] = [
        0x9F, 0x86, 0xD0, 0x81, 0x88, 0x4C, 0x7D, 0x65, 0x9A, 0x2F, 0xEA, 0xA0, 0xC5, 0x5A, 0xD0,
        0x15, 0xA3, 0xBF, 0x4F, 0x1B, 0x2B, 0x0B, 0x82, 0x2C, 0xD1, 0x5D, 0x6C, 0x15, 0xB0, 0xF0,
        0x0A, 0x08,
    ];

    #[test]
    fn golden_sequenced_data_header() {
        let mut h = DataHeader::addressed("/t");
        h.sequence = Some(1);
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA2, 0x00, 0x62, 0x2F, 0x74, 0x06, 0x01]);
        assert_eq!(bytes.len(), 0x07);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_relayed_data_header() {
        let mut h = DataHeader::addressed("/t");
        h.sequence = Some(1);
        h.producer = Some(VECTOR_PRODUCER);
        let bytes = h.encode();
        let mut expected = vec![0xA3, 0x00, 0x62, 0x2F, 0x74, 0x06, 0x01, 0x07, 0x58, 0x20];
        expected.extend_from_slice(&VECTOR_PRODUCER);
        assert_eq!(bytes, expected);
        assert_eq!(bytes.len(), 0x2A);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn a_producer_longer_than_the_cap_is_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::PRODUCER)?
                .bytes(&[0u8; limits::PRODUCER_BYTES + 1])?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes),
            Err(HeaderError::StringTooLong {
                key: data_key::PRODUCER,
                len: limits::PRODUCER_BYTES + 1,
                max: limits::PRODUCER_BYTES,
            })
        );
    }

    #[test]
    fn a_producer_shorter_than_a_digest_is_rejected() {
        // Half a digest identifies nobody, so it is a framing violation
        // rather than a value to carry (`docs/PROTOCOL.md` §6.2).
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::PRODUCER)?.bytes(&[0u8; 16])?;
            Ok(())
        });
        assert!(matches!(
            DataHeader::decode(&bytes),
            Err(HeaderError::Malformed(_))
        ));
    }

    #[test]
    fn the_new_keys_reject_the_wrong_cbor_type() {
        let sequence_as_text = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::SEQUENCE)?.str("7")?;
            Ok(())
        });
        assert!(DataHeader::decode(&sequence_as_text).is_err());

        let producer_as_text = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::PRODUCER)?.str("sha256:…")?;
            Ok(())
        });
        assert!(DataHeader::decode(&producer_as_text).is_err());
    }

    #[test]
    fn a_v0_header_carries_neither_new_key() {
        // What the runtime writes on a `core` connection: neither key 6 —
        // which needs negotiated `PerProducer` ordering — nor key 7, which
        // nothing in this repository sets.
        let mut h = DataHeader::addressed("/t");
        h.traceparent = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into());
        let bytes = h.encode();
        let mut d = Decoder::new(&bytes);
        let pairs = d.map().unwrap().unwrap();
        let keys: Vec<u64> = (0..pairs)
            .map(|_| {
                let key = d.u64().unwrap();
                d.skip().unwrap();
                key
            })
            .collect();
        assert_eq!(keys, vec![data_key::ENDPOINT, data_key::TRACEPARENT]);
    }

    #[test]
    fn golden_subscription_headers() {
        let h = SubscriptionHeader::new("/md", "px.");
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x63, 0x70, 0x78, 0x2E
            ]
        );
        assert_eq!(bytes.len(), 0x0B);
        // One header layout serves both kinds; only the kind byte differs, and
        // that byte belongs to the preamble (see `tests/golden_vectors.rs`).
        assert_eq!(SubscriptionHeader::decode(&bytes).unwrap(), h);
    }

    // --- roundtrips -------------------------------------------------------

    #[test]
    fn data_header_roundtrip_with_every_field() {
        let h = DataHeader {
            endpoint: Some("/transform".into()),
            content_len: Some(1 << 40),
            content_type: Some("application/octet-stream".into()),
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=value".into()),
            topic: Some("px.eur".into()),
            sequence: Some(u64::MAX),
            producer: Some([0x5A; limits::PRODUCER_BYTES]),
            achieved: Some(Acknowledgement::Processed),
            report_id: Some(7),
            report: vec![
                CursorLevel::Known(Acknowledgement::Accepted),
                CursorLevel::Known(Acknowledgement::Processed),
                CursorLevel::Application(CursorLevel::APPLICATION_FLOOR),
            ],
            report_mode: ReportMode::FinalOnly,
            segment: Some(u64::MAX),
        };
        assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
    }

    #[test]
    fn error_header_roundtrip_with_and_without_message() {
        let bare = ErrorHeader::new(ErrorCode::UnknownEndpoint);
        assert_eq!(ErrorHeader::decode(&bare.encode()).unwrap(), bare);
        assert_eq!(bare.error_code(), Some(ErrorCode::UnknownEndpoint));

        let with_msg = ErrorHeader {
            code: 4,
            message: Some("handler panicked".into()),
        };
        assert_eq!(ErrorHeader::decode(&with_msg.encode()).unwrap(), with_msg);
    }

    #[test]
    fn keys_are_emitted_in_ascending_order() {
        let h = DataHeader {
            endpoint: Some("/x".into()),
            content_len: Some(1),
            content_type: Some("t".into()),
            traceparent: Some("p".into()),
            tracestate: Some("s".into()),
            topic: Some("k".into()),
            sequence: Some(9),
            producer: Some([0u8; limits::PRODUCER_BYTES]),
            achieved: Some(Acknowledgement::Accepted),
            report_id: Some(1),
            report: vec![CursorLevel::Known(Acknowledgement::Stored)],
            report_mode: ReportMode::FinalOnly,
            segment: Some(3),
        };
        let bytes = h.encode();
        let mut d = Decoder::new(&bytes);
        let n = d.map().unwrap().unwrap();
        let mut last = None;
        for _ in 0..n {
            let key = d.u64().unwrap();
            if let Some(prev) = last {
                assert!(key > prev, "keys must ascend: {prev} then {key}");
            }
            last = Some(key);
            d.skip().unwrap();
        }
    }

    #[test]
    fn an_unsorted_report_with_repeats_is_emitted_as_the_canonical_ascending_set() {
        // A caller hands over the levels in the order it thought of them.
        // Decoding is the proof: the decoder refuses a non-ascending or
        // repeated array, so a header that survives its own encoder was
        // canonicalized on the way out.
        let h = DataHeader {
            report_id: Some(1),
            report: vec![
                CursorLevel::Application(CursorLevel::APPLICATION_FLOOR),
                CursorLevel::Known(Acknowledgement::Stored),
                CursorLevel::Application(CursorLevel::APPLICATION_FLOOR),
                CursorLevel::Known(Acknowledgement::Accepted),
                CursorLevel::Known(Acknowledgement::Stored),
            ],
            ..DataHeader::reply()
        };
        let decoded = DataHeader::decode(&h.encode()).expect("encoder emits the canonical form");
        assert_eq!(
            decoded.report,
            vec![
                CursorLevel::Known(Acknowledgement::Accepted),
                CursorLevel::Known(Acknowledgement::Stored),
                CursorLevel::Application(CursorLevel::APPLICATION_FLOOR),
            ]
        );
    }

    // --- optional fields --------------------------------------------------

    #[test]
    fn every_data_field_is_optional_at_the_decoder() {
        // The endpoint requirement lives in dispatch, not here: a reply half
        // legitimately carries none, and the decoder cannot tell the halves
        // apart.
        assert_eq!(DataHeader::decode(&[0xA0]).unwrap(), DataHeader::default());

        let only_topic = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::TOPIC)?.str("px.eur")?;
            Ok(())
        });
        let h = DataHeader::decode(&only_topic).unwrap();
        assert_eq!(h.topic.as_deref(), Some("px.eur"));
        assert_eq!(h.endpoint, None);
    }

    #[test]
    fn absent_fields_are_omitted_by_the_encoder() {
        let h = DataHeader::addressed("/t");
        assert_eq!(h.encode(), vec![0xA1, 0x00, 0x62, 0x2F, 0x74]);
    }

    // --- forward compatibility -------------------------------------------

    #[test]
    fn unknown_keys_are_skipped() {
        // Re-encode the golden DATA header with an extra key 63 holding a
        // nested structure, and check it still decodes to the same value.
        let h = DataHeader::addressed("/t");
        let extended = encode_with(|e| {
            e.map(2)?;
            e.u64(0)?.str("/t")?;
            e.u64(63)?.array(2)?.u64(7)?.map(1)?.u64(1)?.bool(true)?;
            Ok(())
        });
        assert_eq!(DataHeader::decode(&extended).unwrap(), h);
    }

    #[test]
    fn unknown_keys_above_the_reserved_range_are_skipped() {
        let extended = encode_with(|e| {
            e.map(2)?;
            e.u64(1)?.u64(5)?;
            e.u64(1000)?.str("future")?;
            Ok(())
        });
        let h = DataHeader::decode(&extended).unwrap();
        assert_eq!(h.content_len, Some(5));
    }

    #[test]
    fn skipping_tolerates_nesting_up_to_the_depth_limit() {
        for depth in [1usize, limits::MAX_SKIP_DEPTH] {
            let bytes = encode_with(|e| {
                e.map(2)?;
                e.u64(data_key::CONTENT_LEN)?.u64(1)?;
                e.u64(50)?;
                for _ in 0..depth {
                    e.array(1)?;
                }
                e.u64(1)?;
                Ok(())
            });
            let h = DataHeader::decode(&bytes).unwrap_or_else(|e| panic!("depth {depth}: {e}"));
            assert_eq!(h.content_len, Some(1), "depth {depth}");
        }
    }

    #[test]
    fn skipping_rejects_nesting_beyond_the_depth_limit() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(50)?;
            for _ in 0..(limits::MAX_SKIP_DEPTH + 1) {
                e.array(1)?;
            }
            e.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::DepthExceeded
        );
    }

    #[test]
    fn skipping_a_wide_shallow_structure_is_fine() {
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(data_key::CONTENT_LEN)?.u64(1)?;
            e.u64(40)?.array(64)?;
            for i in 0..64u64 {
                e.u64(i)?;
            }
            Ok(())
        });
        assert_eq!(DataHeader::decode(&bytes).unwrap().content_len, Some(1));
    }

    // --- strictness -------------------------------------------------------

    #[test]
    fn duplicate_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(1)?.u64(1)?;
            e.u64(1)?.u64(2)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::DuplicateKey(1)
        );
    }

    #[test]
    fn non_uint_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.str("endpoint")?.str("/t")?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::NonUintKey
        );

        let negative = encode_with(|e| {
            e.map(1)?;
            e.i64(-1)?.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&negative).unwrap_err(),
            HeaderError::NonUintKey
        );
    }

    #[test]
    fn indefinite_maps_are_rejected() {
        let bytes = encode_with(|e| {
            e.begin_map()?;
            e.u64(1)?.u64(1)?;
            e.end()?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Indefinite
        );
    }

    #[test]
    fn indefinite_arrays_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(5)?;
            e.u64(0)?.begin_array()?.u64(0)?.end()?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(1)?;
            e.u64(3)?.array(0)?;
            e.u64(4)?.array(0)?;
            Ok(())
        });
        assert_eq!(Hello::decode(&bytes).unwrap_err(), HeaderError::Indefinite);
    }

    #[test]
    fn value_type_mismatches_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::CONTENT_LEN)?.str("not a number")?;
            Ok(())
        });
        assert!(matches!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Malformed(_)
        ));
    }

    #[test]
    fn missing_required_keys_are_rejected() {
        // ERROR without a code.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(error_key::MESSAGE)?.str("why")?;
            Ok(())
        });
        assert_eq!(
            ErrorHeader::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(error_key::CODE)
        );

        // HELLO missing capabilities.
        let bytes = encode_with(|e| {
            e.map(4)?;
            e.u64(0)?.array(1)?.u64(0)?;
            e.u64(1)?.u64(16384)?;
            e.u64(2)?.u64(16)?;
            e.u64(4)?.array(0)?;
            Ok(())
        });
        assert_eq!(
            Hello::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(hello_key::CAPABILITIES)
        );
    }

    // --- subscription headers ---------------------------------------------

    #[test]
    fn an_empty_filter_is_legal_and_survives_the_roundtrip() {
        let h = SubscriptionHeader::new("/md", "");
        let bytes = h.encode();
        assert_eq!(SubscriptionHeader::decode(&bytes).unwrap(), h);
        // The key is written even though the value is empty: absent and empty
        // must stay distinguishable, and empty means "every topic".
        assert!(
            bytes.contains(&0x60),
            "the empty filter is encoded: {bytes:?}"
        );
    }

    #[test]
    fn the_filter_grammar_accepts_what_docs_protocol_6_4_permits() {
        for ok in [
            "",
            "#",
            "px",
            "px.eur",
            "px.*",
            "*.eur",
            "sensors.*.temp",
            "px.#",
            "px.",
            "a..b",
        ] {
            assert_eq!(filter::validate(ok), Ok(()), "{ok:?} must be legal");
        }
    }

    #[test]
    fn the_filter_grammar_rejects_partial_and_misplaced_wildcards() {
        for bad in [
            "px*", "*px", "p*x.eur", "px.e*ur", "#.px", "px.#.eur", "px#",
        ] {
            assert!(
                matches!(filter::validate(bad), Err(HeaderError::InvalidFilter(_))),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn an_illegal_filter_is_rejected_at_the_codec_boundary() {
        // Encoding does not validate — a test may build any bytes — but
        // decoding does, which is what makes the grammar enforceable against
        // a peer (`docs/PROTOCOL.md` §6.4).
        let bytes = SubscriptionHeader::new("/md", "px.#.eur").encode();
        assert!(matches!(
            SubscriptionHeader::decode(&bytes),
            Err(HeaderError::InvalidFilter(_))
        ));
        let e: Error = HeaderError::InvalidFilter("`#` must be the final segment").into();
        assert!(e.to_string().contains("invalid topic filter"));
    }

    #[test]
    fn subscription_strings_are_capped() {
        for (key, max) in [
            (subscription_key::ENDPOINT, limits::MAX_ENDPOINT_BYTES),
            (subscription_key::FILTER, limits::MAX_FILTER_BYTES),
        ] {
            let build = |len: usize| {
                let text = "a".repeat(len);
                let mut h = SubscriptionHeader::new("/md", "px.");
                if key == subscription_key::ENDPOINT {
                    h.endpoint = text;
                } else {
                    h.filter = text;
                }
                h.encode()
            };
            assert_eq!(
                SubscriptionHeader::decode(&build(max + 1)).unwrap_err(),
                HeaderError::StringTooLong {
                    key,
                    len: max + 1,
                    max
                },
                "key {key}"
            );
            assert!(
                SubscriptionHeader::decode(&build(max)).is_ok(),
                "key {key} at cap"
            );
        }
    }

    #[test]
    fn subscription_headers_require_both_keys() {
        let only = |key: u64| {
            encode_with(|e| {
                e.map(1)?;
                e.u64(key)?.str("/md")?;
                Ok(())
            })
        };
        assert_eq!(
            SubscriptionHeader::decode(&only(subscription_key::ENDPOINT)).unwrap_err(),
            HeaderError::MissingKey(subscription_key::FILTER)
        );
        assert_eq!(
            SubscriptionHeader::decode(&only(subscription_key::FILTER)).unwrap_err(),
            HeaderError::MissingKey(subscription_key::ENDPOINT)
        );
    }

    #[test]
    fn subscription_headers_reject_malformed_input() {
        assert!(SubscriptionHeader::decode(&[]).is_err());
        // Trailing bytes.
        let mut bytes = SubscriptionHeader::new("/md", "px.").encode();
        bytes.push(0xff);
        assert_eq!(
            SubscriptionHeader::decode(&bytes).unwrap_err(),
            HeaderError::TrailingBytes
        );
        // Unknown keys are skipped, like every other header.
        let extended = encode_with(|e| {
            e.map(3)?;
            e.u64(0)?.str("/md")?;
            e.u64(1)?.str("px.")?;
            e.u64(40)?.array(2)?.u64(1)?.u64(2)?;
            Ok(())
        });
        assert_eq!(
            SubscriptionHeader::decode(&extended).unwrap(),
            SubscriptionHeader::new("/md", "px.")
        );
    }

    #[test]
    fn oversized_strings_are_rejected_per_field() {
        // Built through the encoder, which emits keys in ascending order.
        let with_text = |key: u64, text: String| -> Vec<u8> {
            let mut h = DataHeader::reply();
            match key {
                data_key::ENDPOINT => h.endpoint = Some(text),
                data_key::CONTENT_TYPE => h.content_type = Some(text),
                data_key::TRACEPARENT => h.traceparent = Some(text),
                data_key::TRACESTATE => h.tracestate = Some(text),
                data_key::TOPIC => h.topic = Some(text),
                other => panic!("key {other} is not a text field"),
            }
            h.encode()
        };
        let cases: [(u64, usize); 5] = [
            (data_key::ENDPOINT, limits::MAX_ENDPOINT_BYTES),
            (data_key::CONTENT_TYPE, limits::MAX_CONTENT_TYPE_BYTES),
            (data_key::TRACEPARENT, limits::MAX_TRACEPARENT_BYTES),
            (data_key::TRACESTATE, limits::MAX_TRACESTATE_BYTES),
            (data_key::TOPIC, limits::MAX_TOPIC_BYTES),
        ];
        for (key, max) in cases {
            assert_eq!(
                DataHeader::decode(&with_text(key, "a".repeat(max + 1))).unwrap_err(),
                HeaderError::StringTooLong {
                    key,
                    len: max + 1,
                    max
                },
                "key {key}"
            );
            assert!(
                DataHeader::decode(&with_text(key, "a".repeat(max))).is_ok(),
                "key {key} at cap"
            );
        }
    }

    #[test]
    fn unordered_keys_are_rejected() {
        // Descending keys break the ascending-order rule, which is what makes
        // duplicate detection complete for extension keys.
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(2)?.str("t")?;
            e.u64(1)?.u64(1)?;
            e.u64(3)?.str("p")?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::UnorderedKey(1)
        );
    }

    #[test]
    fn duplicate_extension_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(1)?.u64(1)?;
            e.u64(1000)?.u64(1)?;
            e.u64(1000)?.u64(2)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::DuplicateKey(1000)
        );
    }

    #[test]
    fn oversized_error_messages_are_rejected() {
        let big = "m".repeat(limits::MAX_MESSAGE_BYTES + 1);
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(error_key::CODE)?.u64(2)?;
            e.u64(error_key::MESSAGE)?.str(&big)?;
            Ok(())
        });
        assert_eq!(
            ErrorHeader::decode(&bytes).unwrap_err(),
            HeaderError::StringTooLong {
                key: error_key::MESSAGE,
                len: limits::MAX_MESSAGE_BYTES + 1,
                max: limits::MAX_MESSAGE_BYTES
            }
        );
    }

    #[test]
    fn oversized_lists_are_rejected_without_allocating() {
        // A one-byte array header claiming 2^32 items must not reserve memory.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(0)?.array(u64::from(u32::MAX))?;
            Ok(())
        });
        assert_eq!(
            Hello::decode(&bytes).unwrap_err(),
            HeaderError::ListTooLong {
                key: hello_key::VERSIONS,
                len: u64::from(u32::MAX),
                max: limits::MAX_LIST_ITEMS
            }
        );
    }

    #[test]
    fn lists_exactly_at_the_cap_are_accepted() {
        let bytes = encode_with(|e| {
            e.map(5)?;
            e.u64(0)?.array(limits::MAX_LIST_ITEMS as u64)?;
            for i in 0..limits::MAX_LIST_ITEMS as u64 {
                e.u64(i)?;
            }
            e.u64(1)?.u64(16384)?;
            e.u64(2)?.u64(16)?;
            e.u64(3)?.array(0)?;
            e.u64(4)?.array(0)?;
            Ok(())
        });
        assert_eq!(
            Hello::decode(&bytes).unwrap().versions.len(),
            limits::MAX_LIST_ITEMS
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = ErrorHeader::new(ErrorCode::Rejected).encode();
        bytes.push(0xff);
        assert_eq!(
            ErrorHeader::decode(&bytes).unwrap_err(),
            HeaderError::TrailingBytes
        );
    }

    #[test]
    fn truncated_headers_are_rejected() {
        let full = DataHeader::addressed("/t").encode();
        for cut in 0..full.len() {
            assert!(
                DataHeader::decode(&full[..cut]).is_err(),
                "prefix of {cut} bytes must not decode"
            );
        }
    }

    #[test]
    fn empty_input_is_rejected_for_every_header() {
        assert!(Hello::decode(&[]).is_err());
        assert!(DataHeader::decode(&[]).is_err());
        assert!(ErrorHeader::decode(&[]).is_err());
        assert!(SubscriptionHeader::decode(&[]).is_err());
    }

    #[test]
    fn tags_are_rejected() {
        // Key 50 is unknown, so the value goes through `skip_value`, which is
        // where the tag rule lives.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(50)?.tag(minicbor::data::IanaTag::Cbor)?.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Malformed("tags are not allowed")
        );
    }

    // --- reserved value passthrough ---------------------------------------

    #[test]
    fn unknown_error_codes_survive_decoding() {
        let err = ErrorHeader {
            code: 99,
            message: None,
        };
        let decoded = ErrorHeader::decode(&err.encode()).unwrap();
        assert_eq!(decoded.code, 99);
        assert_eq!(decoded.error_code(), None);
    }

    #[test]
    fn header_errors_become_protocol_errors() {
        let e: Error = HeaderError::DuplicateKey(3).into();
        assert!(matches!(e, Error::Protocol(_)));
        assert!(e.to_string().contains("duplicate header key 3"));
    }
}
