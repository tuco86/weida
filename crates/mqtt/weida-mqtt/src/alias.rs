//! Topic Aliases: two tables, one per direction, both dying with the
//! connection.
//!
//! A Topic Alias replaces a Topic Name with a two-byte integer for the rest of
//! the connection (3.3.2.3.4) [mqtt5 §3]. Four rules decide everything here,
//! and each one is a place an implementation goes wrong:
//!
//! 1. **Per direction.** "Topic Alias mappings exist only within a Network
//!    Connection and Session, and are **not** shared between directions"
//!    ([MQTT-3.3.2-8]). A server declaring `Topic Alias Maximum` 10 in CONNACK
//!    has said what *it* will accept from us; what we will accept from it is
//!    the number *we* declared in CONNECT. So there are two tables with two
//!    different bounds, and one shared table would silently accept an alias
//!    the peer was never allowed to send.
//! 2. **Per connection.** "Topic Alias mappings MUST NOT be carried across
//!    Network Connections" ([MQTT-3.3.2-7]). Both tables are constructed with
//!    the connection and dropped with it; [`crate::StoredPublish`] has no
//!    alias field at all, so a retransmitted PUBLISH goes out with its full
//!    Topic Name on the new connection.
//! 3. **Established by a non-zero-length Topic Name.** A PUBLISH carrying an
//!    alias *and* a topic sets or overwrites the mapping; one carrying an
//!    alias and a zero-length topic uses it ([MQTT-3.3.4-6]). There is no
//!    other way to establish one - no negotiation packet, no handshake.
//! 4. **Never zero.** "A Topic Alias of 0 is a Protocol Error"
//!    ([MQTT-3.3.2-8]), and one above the receiver's declared maximum is
//!    too - both earning 0x94 (Topic Alias invalid).
//!
//! # Where the bound is
//!
//! Both tables are bounded in entries by a number the *receiver* declared, so
//! neither can be grown by the peer: the outbound table by the server's `Topic
//! Alias Maximum` from CONNACK, the inbound table by
//! [`crate::Limits::topic_alias_maximum`] from our own CONNECT. A peer that
//! sends alias 40,000 against our declared 10 does not allocate a 40,000-entry
//! map; it earns a Protocol Error.
//!
//! **An entry count is not a memory bound**, because an entry holds a Topic
//! Name of up to 65,535 bytes: ten declared aliases of near-64 KiB names are
//! 640 KiB, and a thousand are 64 MiB for the life of the connection. So each
//! table also holds a budget of bytes, [`crate::Limits::max_alias_bytes`],
//! charged per entry with a fixed overhead so that a table of tiny names is
//! bounded as well. A peer cannot be made to forget an alias it established,
//! so an inbound one that would pass the budget is refused outright; an
//! outbound topic that would pass it is only sent without an alias, which
//! costs a few bytes and no correctness.

use std::collections::HashMap;

use crate::error::{Error, Feature, Result};

/// What an alias entry costs beyond the bytes of its Topic Name: the map slot,
/// the `String`'s own header and the table's bookkeeping. Without it a peer
/// could fill the table's *count* with names of one byte each and be charged
/// next to nothing for it.
const ENTRY_OVERHEAD: usize = 64;

/// The aliases this client has handed out on this connection.
///
/// Bounded in entries by the server's `Topic Alias Maximum`, which is also
/// the highest alias value it will accept (3.2.2.3.8): aliases are allocated
/// 1..=max, so the table's size and the protocol's ceiling are the same
/// number. The server's number is the peer's, though, and an entry may be a
/// 65,535-byte Topic Name an application took from remote input, so the table
/// is also bounded in bytes.
///
/// **No eviction.** Once `max` topics have aliases, further topics are
/// published with their full Topic Name and no alias. Replacing a mapping is
/// legal - a PUBLISH with an established alias and a new Topic Name
/// overwrites it ([MQTT-3.3.4-6]) - and deliberately not done, because the
/// only policy that could beat "keep the first `max`" needs to know which
/// topic this client will publish to next, and a library that guessed would
/// spend a full Topic Name on every publish in the worst case instead of
/// saving one in the common case. A caller that knows its hot topics
/// publishes to them first.
#[derive(Debug)]
pub struct OutboundAliases {
    max: u16,
    max_bytes: usize,
    bytes: usize,
    assigned: HashMap<String, u16>,
}

/// What to put on the wire for one publish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Aliased {
    /// Send the Topic Name in full and no alias: either the server declined
    /// aliases entirely, or the table is full.
    Full,
    /// Send the Topic Name in full **and** this alias, establishing it
    /// ([MQTT-3.3.4-6]).
    Establish(u16),
    /// Send a zero-length Topic Name and this alias. The only case where a
    /// zero-length Topic Name is legal (3.3.2.1).
    Use(u16),
}

impl OutboundAliases {
    /// A table bounded by the server's `Topic Alias Maximum` and by
    /// `max_bytes` of Topic Names. A maximum of zero, which is the protocol's
    /// default, means the server accepts no aliases ([MQTT-3.2.2-18]), so
    /// nothing is ever aliased.
    #[must_use]
    pub fn new(maximum: u16, max_bytes: usize) -> OutboundAliases {
        OutboundAliases {
            max: maximum,
            max_bytes,
            bytes: 0,
            assigned: HashMap::new(),
        }
    }

    /// How many aliases are in use.
    #[must_use]
    pub fn len(&self) -> usize {
        self.assigned.len()
    }

    /// Whether none are.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.assigned.is_empty()
    }

    /// What this publish should carry for `topic`.
    pub fn publish(&mut self, topic: &str) -> Aliased {
        if self.max == 0 || topic.is_empty() {
            return Aliased::Full;
        }
        if let Some(alias) = self.assigned.get(topic) {
            return Aliased::Use(*alias);
        }
        let next = u16::try_from(self.assigned.len()).unwrap_or(u16::MAX);
        if next >= self.max {
            return Aliased::Full;
        }
        // The table's own budget: a topic that would not fit is sent in full,
        // which is what every topic past the server's maximum does.
        let cost = topic.len() + ENTRY_OVERHEAD;
        if self.bytes.saturating_add(cost) > self.max_bytes {
            return Aliased::Full;
        }
        // 1..=max: "Topic Alias 0 is a Protocol Error" ([MQTT-3.3.2-8]).
        let alias = next + 1;
        self.bytes += cost;
        self.assigned.insert(topic.to_owned(), alias);
        Aliased::Establish(alias)
    }

    /// Takes back what [`OutboundAliases::publish`] decided for `topic` when
    /// the packet it was for never reached the wire.
    ///
    /// An alias is established by the PUBLISH that carries it, so one whose
    /// PUBLISH was refused locally — it did not fit the server's `Maximum
    /// Packet Size` — was never established, and a later publish that used it
    /// would carry a zero-length Topic Name the server has no mapping for.
    /// Only an [`Aliased::Establish`] leaves anything to take back, and only
    /// the **latest** one may be: aliases are handed out in order, so this is
    /// called straight after the failed send and before the next publish.
    pub fn revoke(&mut self, decision: Aliased, topic: &str) {
        if let Aliased::Establish(alias) = decision
            && self.assigned.get(topic) == Some(&alias)
        {
            self.assigned.remove(topic);
            self.bytes -= topic.len() + ENTRY_OVERHEAD;
        }
    }
}

/// The aliases the server has established toward this client on this
/// connection.
///
/// Bounded by the `Topic Alias Maximum` this client declared in CONNECT, which
/// is the only number the server is permitted to use ([MQTT-3.2.2-18] in the
/// other direction, 3.1.2.11.5): an alias outside 1..=max is refused before
/// the map is touched, so the map never holds more than `max` entries however
/// hostile the peer.
#[derive(Debug)]
pub struct InboundAliases {
    max: u16,
    max_bytes: usize,
    bytes: usize,
    known: HashMap<u16, String>,
}

impl InboundAliases {
    /// A table bounded by what this client declared it would accept, and by
    /// `max_bytes` of Topic Names.
    #[must_use]
    pub fn new(maximum: u16, max_bytes: usize) -> InboundAliases {
        InboundAliases {
            max: maximum,
            max_bytes,
            bytes: 0,
            known: HashMap::new(),
        }
    }

    /// How many aliases the server has established.
    #[must_use]
    pub fn len(&self) -> usize {
        self.known.len()
    }

    /// Whether none are.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.known.is_empty()
    }

    /// The Topic Name a delivery is really about.
    ///
    /// Three cases, all of 3.3.4's rules:
    ///
    /// * no alias: the Topic Name as sent, which must not be zero-length;
    /// * an alias with a Topic Name: the mapping is set or overwritten, and
    ///   the Topic Name is the answer ([MQTT-3.3.4-6]);
    /// * an alias with a zero-length Topic Name: the mapping is used.
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] with 0x94 (Topic Alias invalid) for alias 0, for
    /// an alias above the maximum this client declared, for a zero-length
    /// Topic Name whose alias was never established, and for a mapping that
    /// would take the table past its byte budget. The third is not
    /// pedantry - it is the difference between reporting a Protocol Error and
    /// delivering a message on an unknown topic.
    pub fn resolve(&mut self, alias: Option<u16>, topic: &str) -> Result<String> {
        let Some(alias) = alias else {
            if topic.is_empty() {
                return Err(Error::InvalidTopic {
                    topic: String::new(),
                    reason: "a zero-length Topic Name is legal only with a Topic Alias \
                             (3.3.2.1)",
                });
            }
            return Ok(topic.to_owned());
        };
        if alias == 0 || alias > self.max {
            // "A Topic Alias of 0 is a Protocol Error", and one above the
            // declared maximum is too ([MQTT-3.3.2-8]).
            return Err(Error::Unavailable {
                feature: Feature::TopicAlias,
                reason_code: Feature::TopicAlias.reason_code(),
            });
        }
        if topic.is_empty() {
            return self.known.get(&alias).cloned().ok_or(Error::Unavailable {
                feature: Feature::TopicAlias,
                reason_code: Feature::TopicAlias.reason_code(),
            });
        }
        // A new or replaced mapping is charged, and what it replaces is given
        // back, before anything is stored.
        let cost = topic.len() + ENTRY_OVERHEAD;
        let replaced = self
            .known
            .get(&alias)
            .map_or(0, |old| old.len() + ENTRY_OVERHEAD);
        let after = self.bytes - replaced + cost;
        if after > self.max_bytes {
            return Err(Error::Unavailable {
                feature: Feature::TopicAlias,
                reason_code: Feature::TopicAlias.reason_code(),
            });
        }
        self.bytes = after;
        self.known.insert(alias, topic.to_owned());
        Ok(topic.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A budget no test here comes near, for the tests that are not about it.
    const ROOMY: usize = 1 << 20;

    /// An alias is established by a full Topic Name and then used by a
    /// zero-length one, which is the whole mechanism ([MQTT-3.3.4-6]).
    #[test]
    fn an_alias_is_established_once_and_used_after() {
        let mut aliases = OutboundAliases::new(4, ROOMY);
        assert_eq!(aliases.publish("a/b"), Aliased::Establish(1));
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1));
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1));
        assert_eq!(aliases.publish("c/d"), Aliased::Establish(2));
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1), "still its own");
        assert_eq!(aliases.len(), 2);
    }

    /// The first alias is 1, never 0 ([MQTT-3.3.2-8]), and the last is the
    /// maximum the server declared.
    #[test]
    fn aliases_run_from_one_to_the_servers_maximum() {
        let mut aliases = OutboundAliases::new(3, ROOMY);
        let handed: Vec<Aliased> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|topic| aliases.publish(topic))
            .collect();
        assert_eq!(
            handed,
            [
                Aliased::Establish(1),
                Aliased::Establish(2),
                Aliased::Establish(3),
                // Full, and the topics keep going out in full rather than
                // stealing an alias from a topic that is still in use.
                Aliased::Full,
                Aliased::Full,
            ]
        );
        assert_eq!(aliases.len(), 3, "never more than the declared maximum");
        // And the established ones still work, which is what no eviction buys.
        assert_eq!(aliases.publish("a"), Aliased::Use(1));
    }

    /// `Topic Alias Maximum` 0 - the protocol's own default - means the peer
    /// accepts none ([MQTT-3.2.2-18]), so nothing is ever aliased.
    #[test]
    fn a_maximum_of_zero_aliases_nothing() {
        let mut aliases = OutboundAliases::new(0, ROOMY);
        assert_eq!(aliases.publish("a/b"), Aliased::Full);
        assert_eq!(aliases.publish("a/b"), Aliased::Full);
        assert!(aliases.is_empty());
    }

    /// The inbound half: set by a full Topic Name, read by a zero-length one.
    #[test]
    fn an_inbound_alias_resolves_what_it_was_set_to() {
        let mut aliases = InboundAliases::new(4, ROOMY);
        assert_eq!(aliases.resolve(Some(1), "a/b").unwrap(), "a/b");
        assert_eq!(aliases.resolve(Some(1), "").unwrap(), "a/b");
        assert_eq!(aliases.resolve(Some(1), "").unwrap(), "a/b");

        // "A sender can modify the Topic Alias mapping by sending another
        // PUBLISH with the same alias and a different Topic Name"
        // ([MQTT-3.3.4-6]).
        assert_eq!(aliases.resolve(Some(1), "c/d").unwrap(), "c/d");
        assert_eq!(aliases.resolve(Some(1), "").unwrap(), "c/d");
        assert_eq!(aliases.len(), 1, "overwritten, not added");

        // No alias at all is the ordinary case.
        assert_eq!(aliases.resolve(None, "e/f").unwrap(), "e/f");
        assert_eq!(aliases.len(), 1);
    }

    /// Every way an alias can be invalid, each earning 0x94.
    #[test]
    fn an_invalid_inbound_alias_is_a_protocol_error() {
        let mut aliases = InboundAliases::new(2, ROOMY);

        // Zero.
        assert_eq!(
            aliases.resolve(Some(0), "a").unwrap_err().reason_code(),
            Some(0x94)
        );
        // Above what this client declared - the hostile case, and the map is
        // not grown by it.
        assert_eq!(
            aliases
                .resolve(Some(40_000), "a")
                .unwrap_err()
                .reason_code(),
            Some(0x94)
        );
        assert!(aliases.is_empty(), "a refused alias allocates nothing");
        // Never established, so there is nothing to resolve.
        assert_eq!(
            aliases.resolve(Some(2), "").unwrap_err().reason_code(),
            Some(0x94)
        );
        // A zero-length Topic Name with no alias at all.
        assert!(aliases.resolve(None, "").is_err());
    }

    /// A client that declared `Topic Alias Maximum` 0 - which is what
    /// [`crate::Limits`] does not default to, but a caller may choose - accepts
    /// no alias whatever, because 1 is already above 0.
    #[test]
    fn declaring_zero_refuses_every_alias() {
        let mut aliases = InboundAliases::new(0, ROOMY);
        assert_eq!(
            aliases.resolve(Some(1), "a").unwrap_err().reason_code(),
            Some(0x94)
        );
        assert_eq!(aliases.resolve(None, "a").unwrap(), "a");
    }

    /// The two directions are independent ([MQTT-3.3.2-8]): the same number in
    /// each is a different permission, and alias 3 inbound has nothing to do
    /// with alias 3 outbound.
    #[test]
    fn the_two_directions_do_not_share_a_table() {
        let mut outbound = OutboundAliases::new(1, ROOMY);
        let mut inbound = InboundAliases::new(4, ROOMY);

        assert_eq!(outbound.publish("ours"), Aliased::Establish(1));
        assert_eq!(outbound.publish("second"), Aliased::Full, "our bound is 1");

        // The server's own alias 1 is a different mapping, and its 2, 3 and 4
        // are permitted because *we* declared 4.
        assert_eq!(inbound.resolve(Some(1), "theirs").unwrap(), "theirs");
        assert_eq!(inbound.resolve(Some(4), "far").unwrap(), "far");
        assert_eq!(inbound.resolve(Some(1), "").unwrap(), "theirs");
        assert_eq!(outbound.publish("ours"), Aliased::Use(1));
    }

    /// A table of declared entries is not a table of bounded memory: each may
    /// be a name of nearly 64 KiB. Two such names do not fit a 64 KiB budget,
    /// and the second is refused rather than stored, with the table left as it
    /// was.
    #[test]
    fn an_inbound_table_is_bounded_in_bytes_not_only_in_entries() {
        let long = "t".repeat(40 * 1024);
        let mut aliases = InboundAliases::new(8, 64 * 1024);
        assert_eq!(aliases.resolve(Some(1), &long).unwrap(), long);
        assert_eq!(
            aliases.resolve(Some(2), &long).unwrap_err().reason_code(),
            Some(0x94)
        );
        assert_eq!(aliases.len(), 1, "the refused one stored nothing");
        assert_eq!(
            aliases.resolve(Some(1), "").unwrap(),
            long,
            "the first stands"
        );

        // Replacing a mapping gives its bytes back, so a table at its budget
        // is not stuck at it.
        assert_eq!(aliases.resolve(Some(1), "short").unwrap(), "short");
        assert_eq!(aliases.resolve(Some(2), &long).unwrap(), long);
    }

    /// The per-entry overhead is what bounds a table of tiny names: with it,
    /// a budget is also a ceiling on the count.
    #[test]
    fn tiny_names_are_charged_too() {
        let mut aliases = InboundAliases::new(1000, 10 * ENTRY_OVERHEAD);
        let stored = (1..=1000u16)
            .take_while(|alias| aliases.resolve(Some(*alias), "x").is_ok())
            .count();
        assert!((1..=10).contains(&stored), "{stored} entries of one byte");
    }

    /// An outbound topic that does not fit the budget is sent in full, which
    /// costs a few bytes and nothing else.
    #[test]
    fn an_outbound_topic_past_the_budget_goes_out_without_an_alias() {
        let long = "t".repeat(40 * 1024);
        let mut aliases = OutboundAliases::new(8, 64 * 1024);
        assert_eq!(aliases.publish(&long), Aliased::Establish(1));
        let other = "u".repeat(40 * 1024);
        assert_eq!(aliases.publish(&other), Aliased::Full);
        assert_eq!(aliases.publish(&long), Aliased::Use(1), "the first stands");
        assert_eq!(aliases.len(), 1);
    }

    /// An alias whose establishing PUBLISH was refused locally was never
    /// established, so taking it back leaves the next publish of that topic to
    /// establish it again, under the same number.
    #[test]
    fn a_revoked_alias_is_established_again() {
        let mut aliases = OutboundAliases::new(4, ROOMY);
        let decision = aliases.publish("a/b");
        assert_eq!(decision, Aliased::Establish(1));
        aliases.revoke(decision, "a/b");
        assert!(aliases.is_empty());
        assert_eq!(aliases.publish("a/b"), Aliased::Establish(1));

        // Taking back a use, or a full topic, takes back nothing.
        aliases.revoke(Aliased::Use(1), "a/b");
        aliases.revoke(Aliased::Full, "a/b");
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1));
    }
}
