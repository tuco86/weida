//! Topic Names, Topic Filters, and the matcher the specification's own worked
//! examples define.
//!
//! `/` separates levels, `+` matches exactly one level and MUST occupy a whole
//! level ([MQTT-4.7.1-2]), and `#` matches the parent level and any number of
//! child levels and MUST be last and alone in its level ([MQTT-4.7.1-1])
//! [mqtt5 §4.1]. Matching is "character for character with no normalization"
//! ([MQTT-4.7.3-4]) [mqtt5 §3].
//!
//! # Why a client needs a matcher at all
//!
//! Matching is the *server's* job, so a client that only publishes and
//! subscribes never needs one. Two things make it load-bearing here:
//!
//! * **A filter is refused before it reaches the wire.** A malformed filter
//!   earns a SUBACK 0x8F or a DISCONNECT, and refusing it locally is both
//!   cheaper and more specific than reading a reason code back.
//! * **A client has to tell which of its own subscriptions caused a
//!   delivery.** `Subscription Identifier` answers that when the server sends
//!   one, and it is optional and refusable — `Subscription Identifiers
//!   Available` 0 (3.2.2.3.12) [mqtt5 §11] — so against such a server the
//!   only answer is to match the delivered Topic Name against the filters this
//!   client subscribed with. [`Subscriptions::matching`] is that, and the
//!   matcher is why it can exist.
//!
//! # The `$` rule, which is the one a matcher most easily gets wrong
//!
//! "A server MUST NOT match a filter beginning with a wildcard against a
//! Topic Name beginning with `$`" ([MQTT-4.7.2-1]) [mqtt5 §4.1]. So `#`
//! receives nothing under `$` while `$SYS/#` does, and a client that wants
//! both must subscribe to both. The rule is about the *filter's first
//! character*, not about wildcards anywhere in it: `$SYS/+` is fine.
//!
//! # What this is not
//!
//! It is not weida's matcher, and the two genuinely disagree: weida's
//! separator is `.` rather than `/`, and weida reserves no topic prefix, so
//! its `#` matches `$`-prefixed topics where MQTT's does not. Similar
//! separators and wildcards do not make two grammars equal
//! ([0007](../../../docs/decisions/0007-topic-namespace.md) §5). Nothing in
//! this module translates anything; it implements MQTT's rules, and any
//! conversion between the two namespaces is the application's to configure
//! and to refuse.

use weida_mqtt_codec::{QoS, RetainHandling, SubackReasonCode, SubscriptionOptions};

use crate::error::{Error, Feature, Result};
use crate::limits::ServerLimits;

/// The largest a Topic Name or Filter may encode to, from the two-byte length
/// prefix of a UTF-8 Encoded String ([MQTT-4.7.3-3]) [mqtt5 §3].
pub const MAX_TOPIC_BYTES: usize = u16::MAX as usize;

/// The prefix that makes a filter a Shared Subscription ([MQTT-4.8.2-1])
/// [mqtt5 §4.2].
pub const SHARE_PREFIX: &str = "$share/";

/// Checks a Topic **Name**, which a PUBLISH carries.
///
/// A Topic Name "MUST NOT contain wildcard characters" ([MQTT-3.3.2-2])
/// [mqtt5 §3] — it is a name, not a pattern — and must be at least one
/// character ([MQTT-4.7.3-1]).
///
/// The one exception is a zero-length Topic Name, which is legal **only**
/// with an established Topic Alias (3.3.2.1) [mqtt5 §3]; that is the alias
/// table's business, so `allow_empty` is the caller's statement about it.
///
/// # Errors
///
/// [`Error::InvalidTopic`] for an empty name where one is required, a name
/// above [`MAX_TOPIC_BYTES`], or one containing `+` or `#`.
pub fn check_topic_name(topic: &str, allow_empty: bool) -> Result<()> {
    if topic.is_empty() {
        if allow_empty {
            return Ok(());
        }
        return Err(Error::InvalidTopic {
            topic: topic.to_owned(),
            reason: "a Topic Name must be at least one character ([MQTT-4.7.3-1]); a zero-length \
                     one is legal only with an established Topic Alias (3.3.2.1)",
        });
    }
    if topic.len() > MAX_TOPIC_BYTES {
        return Err(Error::InvalidTopic {
            topic: topic.to_owned(),
            reason: "a Topic Name must not encode to more than 65,535 bytes ([MQTT-4.7.3-3])",
        });
    }
    if topic.contains(['+', '#']) {
        return Err(Error::InvalidTopic {
            topic: topic.to_owned(),
            reason: "a Topic Name must not contain the wildcards + or # ([MQTT-3.3.2-2]): it is \
                     a name, not a filter",
        });
    }
    Ok(())
}

/// A Shared Subscription's parts: the ShareName and the filter it shares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shared<'a> {
    /// The ShareName: at least one character and containing none of `/`, `+`
    /// or `#` ([MQTT-4.8.2-2]) [mqtt5 §4.2].
    pub name: &'a str,
    /// The filter itself, which is what matching uses: "neither `$share` nor
    /// the ShareName is considered when matching publications"
    /// [mqtt5 §4.2].
    pub filter: &'a str,
}

/// Splits `$share/{ShareName}/{filter}`, or `None` where `filter` is not
/// shared.
///
/// # Errors
///
/// [`Error::InvalidTopic`] for a `$share/` filter whose ShareName is empty or
/// contains `/`, `+` or `#`, or whose filter half is empty.
pub fn split_shared(filter: &str) -> Result<Option<Shared<'_>>> {
    let Some(rest) = filter.strip_prefix(SHARE_PREFIX) else {
        return Ok(None);
    };
    let invalid = |reason: &'static str| Error::InvalidTopic {
        topic: filter.to_owned(),
        reason,
    };
    let (name, inner) = rest.split_once('/').ok_or_else(|| {
        invalid("a shared subscription is $share/{ShareName}/{filter} ([MQTT-4.8.2-1])")
    })?;
    if name.is_empty() {
        return Err(invalid(
            "a ShareName must be at least one character ([MQTT-4.8.2-2])",
        ));
    }
    if name.contains(['+', '#']) {
        return Err(invalid(
            "a ShareName must not contain + or # ([MQTT-4.8.2-2])",
        ));
    }
    if inner.is_empty() {
        return Err(invalid(
            "a shared subscription's filter must be at least one character ([MQTT-4.7.3-1])",
        ));
    }
    Ok(Some(Shared {
        name,
        filter: inner,
    }))
}

/// Checks a Topic **Filter**, which SUBSCRIBE and UNSUBSCRIBE carry.
///
/// The whole grammar of 4.7.1 [mqtt5 §4.1]: at least one character, at most
/// 65,535 bytes, `+` occupying a whole level, `#` last and alone in its level.
/// A `$share/` prefix is split off first, because the rules apply to the
/// filter and not to the ShareName.
///
/// # Errors
///
/// [`Error::InvalidTopic`] naming which rule was broken.
pub fn check_topic_filter(filter: &str) -> Result<()> {
    let invalid = |reason: &'static str| Error::InvalidTopic {
        topic: filter.to_owned(),
        reason,
    };
    if filter.is_empty() {
        return Err(invalid(
            "a Topic Filter must be at least one character ([MQTT-4.7.3-1])",
        ));
    }
    if filter.len() > MAX_TOPIC_BYTES {
        return Err(invalid(
            "a Topic Filter must not encode to more than 65,535 bytes ([MQTT-4.7.3-3])",
        ));
    }

    let body = match split_shared(filter)? {
        Some(shared) => shared.filter,
        None => filter,
    };

    let levels: Vec<&str> = body.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        let last = index + 1 == levels.len();
        if level.contains('+') && *level != "+" {
            return Err(invalid(
                "+ must occupy an entire level: sport/+ is legal, sport+ is not \
                 ([MQTT-4.7.1-2])",
            ));
        }
        if level.contains('#') {
            if *level != "#" {
                return Err(invalid(
                    "# must be alone in its level: sport/# is legal, sport/tennis# is not \
                     ([MQTT-4.7.1-1])",
                ));
            }
            if !last {
                return Err(invalid(
                    "# must be the last level of a filter: sport/#/ranking is not \
                     ([MQTT-4.7.1-1])",
                ));
            }
        }
    }
    Ok(())
}

/// Whether `filter` selects `topic`.
///
/// MQTT's matcher, and the specification's own worked examples are its
/// definition (4.7.1, 4.7.2) [mqtt5 §4.1]:
///
/// * `sport/tennis/player1/#` matches `sport/tennis/player1` **itself** as
///   well as its children — `#` includes the parent level;
/// * `sport/+` matches `sport/` but **not** `sport`;
/// * `/finance` matches `+/+` and `/+` but **not** `+`;
/// * a filter beginning with a wildcard never matches a `$` topic
///   ([MQTT-4.7.2-1]).
///
/// A `$share/` prefix is ignored, because "neither `$share` nor the ShareName
/// is considered when matching publications" [mqtt5 §4.2].
///
/// Allocation-free: one left-to-right walk over both strings.
#[must_use]
pub fn matches(filter: &str, topic: &str) -> bool {
    let body = match filter.strip_prefix(SHARE_PREFIX) {
        Some(rest) => match rest.split_once('/') {
            Some((_name, inner)) => inner,
            None => return false,
        },
        None => filter,
    };

    // [MQTT-4.7.2-1]: the exclusion is about the filter's first character.
    if topic.starts_with('$') && (body.starts_with('+') || body.starts_with('#')) {
        return false;
    }

    let mut filter_levels = body.split('/');
    let mut topic_levels = topic.split('/');

    loop {
        match (filter_levels.next(), topic_levels.next()) {
            // `#` matches this level and every level below it, including the
            // case where there is nothing below: `sport/#` matches `sport`.
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(expected), Some(actual)) if expected == actual => {}
            (Some(_), Some(_)) => return false,
            // The filter ran out before the topic, or the other way round.
            (None, None) => return true,
            (None, Some(_)) => return false,
            // `sport/tennis/player1/#` against `sport/tennis/player1`: the
            // topic ran out and the only filter level left is `#`, which the
            // first arm has already handled. Anything else is a miss.
            (Some(_), None) => return false,
        }
    }
}

/// A filter to subscribe with, and its options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    /// The Topic Filter, possibly `$share/{name}/{filter}`.
    pub filter: String,
    /// Maximum QoS, No Local, Retain As Published and Retain Handling
    /// (3.8.3.1) [mqtt5 §4.4].
    pub options: SubscriptionOptions,
}

impl Subscription {
    /// A subscription to `filter` at `maximum_qos`, defaults elsewhere.
    #[must_use]
    pub fn new(filter: impl Into<String>, maximum_qos: QoS) -> Subscription {
        Subscription {
            filter: filter.into(),
            options: SubscriptionOptions::new(maximum_qos),
        }
    }

    /// The same with No Local set: "messages MUST NOT be forwarded to a
    /// connection whose ClientID equals the publishing connection's"
    /// ([MQTT-3.8.3-3]), one of the two options "primarily defined to allow
    /// for message bridge applications" [mqtt5 §4.6].
    #[must_use]
    pub fn no_local(mut self) -> Subscription {
        self.options.no_local = true;
        self
    }

    /// The same with Retain As Published set, the other bridge option: the
    /// server forwards the RETAIN flag as published ([MQTT-3.3.1-13]) rather
    /// than clearing it ([MQTT-3.3.1-12]) [mqtt5 §4.4].
    #[must_use]
    pub fn retain_as_published(mut self) -> Subscription {
        self.options.retain_as_published = true;
        self
    }

    /// The same with a Retain Handling.
    #[must_use]
    pub fn retain_handling(mut self, handling: RetainHandling) -> Subscription {
        self.options.retain_handling = handling;
        self
    }

    /// Whether this is a Shared Subscription.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        self.filter.starts_with(SHARE_PREFIX)
    }

    /// Refuses what the grammar and the server's declarations forbid, before
    /// the packet reaches the wire.
    ///
    /// Four checks, and each has its own verdict on the wire:
    ///
    /// * the grammar of 4.7.1, which is [`check_topic_filter`];
    /// * `Wildcard Subscription Available` 0 against a filter containing `+`
    ///   or `#`, which is 0xA2 (3.2.2.3.11);
    /// * `Shared Subscription Available` 0 against a `$share/` filter, which
    ///   is 0x9E (3.2.2.3.13);
    /// * No Local on a Shared Subscription, which "is a Protocol Error"
    ///   ([MQTT-3.8.3-4]) [mqtt5 §4.2] — refused here as well as by the codec,
    ///   because refusing it at configuration time names the filter.
    ///
    /// Retain Handling 3 cannot reach this function: it is not a value
    /// [`RetainHandling`] has, so the codec's own refusal (3.8.3.1) is the
    /// only way to meet one and it comes off the wire.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidTopic`] or [`Error::Unavailable`] carrying the code the
    /// server would have sent.
    pub fn check(&self, limits: &ServerLimits) -> Result<()> {
        check_topic_filter(&self.filter)?;

        let shared = split_shared(&self.filter)?;
        let body = shared.map_or(self.filter.as_str(), |shared| shared.filter);
        if body.contains(['+', '#']) {
            limits.require(Feature::WildcardSubscription)?;
        }
        if shared.is_some() {
            limits.require(Feature::SharedSubscription)?;
            if self.options.no_local {
                return Err(Error::InvalidTopic {
                    topic: self.filter.clone(),
                    reason: "No Local must not be set on a Shared Subscription \
                             ([MQTT-3.8.3-4])",
                });
            }
        }
        Ok(())
    }
}

/// One subscription as the client last saw it acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionRecord {
    /// The filter and its options, as sent.
    pub subscription: Subscription,
    /// The `Subscription Identifier` sent with it, if any. The server reports
    /// it back "on every delivery it caused" ([MQTT-3.3.4-4]) [mqtt5 §4.1],
    /// which is how a client tells which filter matched.
    pub identifier: Option<u32>,
    /// What the SUBACK granted, which "MUST be the minimum of the QoS of the
    /// originally published message and the Maximum QoS granted"
    /// ([MQTT-3.8.4-8]) [mqtt5 §6] — so it may be less than was asked for,
    /// and a failure code for one filter leaves the others alone (3.9.3).
    pub granted: SubackReasonCode,
}

/// The client's mirror of its own subscriptions, keyed by filter.
///
/// "A non-shared subscription belongs to exactly one session, and a session
/// cannot hold two with the same filter, so the filter is the key" (4.8.1)
/// [mqtt5 §2]. [`Subscriptions::record`] therefore replaces rather than
/// appends, which is the client-side shape of "re-subscribing the same filter
/// MUST replace the subscription without losing messages" ([MQTT-3.8.4-3])
/// [mqtt5 §2].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Subscriptions {
    entries: Vec<SubscriptionRecord>,
}

impl Subscriptions {
    /// No subscriptions.
    #[must_use]
    pub fn new() -> Subscriptions {
        Subscriptions::default()
    }

    /// How many filters are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether none are.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every subscription, in the order it was first recorded.
    pub fn iter(&self) -> impl Iterator<Item = &SubscriptionRecord> {
        self.entries.iter()
    }

    /// The record for `filter`, if any.
    #[must_use]
    pub fn get(&self, filter: &str) -> Option<&SubscriptionRecord> {
        self.entries
            .iter()
            .find(|entry| entry.subscription.filter == filter)
    }

    /// Records a subscription, **replacing** any entry with the same filter.
    ///
    /// A failure code is not recorded: "SUBACK 0x87 or 0x8F for that filter
    /// only; other filters in the same SUBSCRIBE may succeed" (3.9.3)
    /// [mqtt5 §8], so a refused filter is not a subscription and keeping it
    /// would make the mirror claim one the server never made.
    pub fn record(
        &mut self,
        subscription: Subscription,
        identifier: Option<u32>,
        granted: SubackReasonCode,
    ) {
        if granted.is_error() {
            self.remove(&subscription.filter);
            return;
        }
        let record = SubscriptionRecord {
            subscription,
            identifier,
            granted,
        };
        match self
            .entries
            .iter_mut()
            .find(|entry| entry.subscription.filter == record.subscription.filter)
        {
            Some(existing) => *existing = record,
            None => self.entries.push(record),
        }
    }

    /// Forgets `filter`. Returns whether it was held.
    pub fn remove(&mut self, filter: &str) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.subscription.filter != filter);
        self.entries.len() != before
    }

    /// Forgets everything, for Clean Start 1: "subscriptions do not survive
    /// Clean Start 1" [mqtt5 §1].
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// The subscriptions whose filters select `topic`.
    ///
    /// This is what a client uses when the server declined `Subscription
    /// Identifiers` (3.2.2.3.12) [mqtt5 §11] and a delivery therefore carries
    /// none. More than one may match: "when one client's own subscriptions
    /// overlap, the server MUST deliver respecting the maximum QoS of all
    /// matching subscriptions and MAY additionally send one copy per matching
    /// subscription" ([MQTT-3.3.4-2]) [mqtt5 §4.1], so overlapping filters
    /// are permitted and this returns all of them.
    pub fn matching(&self, topic: &str) -> impl Iterator<Item = &SubscriptionRecord> {
        self.entries
            .iter()
            .filter(move |entry| matches(&entry.subscription.filter, topic))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// The specification's own worked examples, verbatim from 4.7.1 and
    /// 4.7.2. Every one of these is a case a plausible matcher gets wrong in
    /// the opposite direction, which is why they are the specification's
    /// examples and not ours.
    #[test]
    fn the_specifications_worked_examples() {
        // "sport/tennis/player1/# matches sport/tennis/player1 itself as well
        // as .../ranking and .../score/wimbledon" — # includes the parent.
        assert!(matches("sport/tennis/player1/#", "sport/tennis/player1"));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/ranking"
        ));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/score/wimbledon"
        ));

        // "sport/# matches bare sport"; "# receives everything".
        assert!(matches("sport/#", "sport"));
        assert!(matches("#", "anything/at/all"));
        assert!(matches("#", "a"));

        // "sport/tennis/+ matches sport/tennis/player1 but not
        // sport/tennis/player1/ranking".
        assert!(matches("sport/tennis/+", "sport/tennis/player1"));
        assert!(!matches("sport/tennis/+", "sport/tennis/player1/ranking"));

        // "sport/+ does not match sport but does match sport/" — the empty
        // level is a level.
        assert!(!matches("sport/+", "sport"));
        assert!(matches("sport/+", "sport/"));

        // "/finance matches +/+ and /+ but not +".
        assert!(matches("+/+", "/finance"));
        assert!(matches("/+", "/finance"));
        assert!(!matches("+", "/finance"));
    }

    /// [MQTT-4.7.2-1]: a filter beginning with a wildcard never matches a `$`
    /// topic, so "`#` receives nothing under `$` while `$SYS/#` does, and a
    /// client wanting both must subscribe to `#` *and* `$SYS/#`"
    /// [mqtt5 §4.1].
    #[test]
    fn a_leading_wildcard_never_matches_a_dollar_topic() {
        assert!(!matches("#", "$SYS/broker/uptime"));
        assert!(!matches("+/broker/uptime", "$SYS/broker/uptime"));
        assert!(!matches("+", "$SYS"));

        // A filter that names the `$` level explicitly does match, and a
        // wildcard after the first level is fine — the rule is about the
        // filter's first character.
        assert!(matches("$SYS/#", "$SYS/broker/uptime"));
        assert!(matches("$SYS/+/uptime", "$SYS/broker/uptime"));
        assert!(matches("$SYS/broker/uptime", "$SYS/broker/uptime"));

        // And the exclusion applies only to `$` topics: the same filters
        // match everything else.
        assert!(matches("#", "SYS/broker/uptime"));
    }

    /// The grammar of 4.7.1, each rule with the specification's own
    /// counter-example.
    #[test]
    fn the_filter_grammar_refuses_what_4_7_1_forbids() {
        for legal in [
            "sport",
            "sport/tennis/#",
            "sport/+/player1",
            "+",
            "#",
            "/",
            "/finance",
            "$SYS/#",
            "sport/",
        ] {
            assert!(check_topic_filter(legal).is_ok(), "{legal} is legal");
        }

        // "sport/tennis# and sport/tennis/#/ranking" are the specification's
        // own illegal examples, plus `+` not alone in its level.
        for illegal in ["sport/tennis#", "sport/#/ranking", "sport+", "sp+rt", ""] {
            assert!(
                check_topic_filter(illegal).is_err(),
                "{illegal:?} is illegal"
            );
        }
    }

    /// A Topic Name is a name and not a pattern ([MQTT-3.3.2-2]), and the
    /// zero-length case is legal only with an alias (3.3.2.1).
    #[test]
    fn a_topic_name_refuses_wildcards_and_emptiness() {
        assert!(check_topic_name("a/b", false).is_ok());
        assert!(check_topic_name("$SYS/x", false).is_ok());
        assert!(check_topic_name("a/+", false).is_err());
        assert!(check_topic_name("a/#", false).is_err());
        assert!(check_topic_name("", false).is_err());
        // Only with an alias, which is the caller's statement.
        assert!(check_topic_name("", true).is_ok());
    }

    /// `$share/{ShareName}/{filter}`, with the three rules [MQTT-4.8.2-2]
    /// gives the ShareName and the matching rule that ignores it.
    #[test]
    fn a_shared_subscription_splits_and_matches_on_its_filter_alone() {
        let shared = split_shared("$share/consumer1/sport/tennis/+")
            .expect("valid")
            .expect("shared");
        assert_eq!(shared.name, "consumer1");
        assert_eq!(shared.filter, "sport/tennis/+");

        // "Neither $share nor the ShareName is considered when matching."
        assert!(matches(
            "$share/consumer1/sport/tennis/+",
            "sport/tennis/player1"
        ));
        assert!(!matches("$share/consumer1/sport/tennis/+", "other/topic"));

        // A non-shared filter is not shared.
        assert_eq!(split_shared("sport/+").expect("valid"), None);

        // The ShareName's own rules.
        for illegal in [
            "$share/",
            "$share//sport",
            "$share/name",
            "$share/na+me/sport",
            "$share/na#me/sport",
            "$share/name/",
        ] {
            assert!(split_shared(illegal).is_err(), "{illegal:?} is illegal");
            assert!(check_topic_filter(illegal).is_err(), "{illegal:?}");
        }
    }

    /// The three availability flags a subscription can trip, each refused with
    /// the code the server would have sent.
    #[test]
    fn a_subscription_is_refused_against_what_the_server_declined() {
        let permissive = ServerLimits::default();
        assert!(
            Subscription::new("sport/+", QoS::AtLeastOnce)
                .check(&permissive)
                .is_ok()
        );
        assert!(
            Subscription::new("$share/g/sport/+", QoS::AtLeastOnce)
                .check(&permissive)
                .is_ok()
        );

        let no_wildcards = ServerLimits {
            wildcard_subscription_available: false,
            ..ServerLimits::default()
        };
        assert_eq!(
            Subscription::new("sport/+", QoS::AtLeastOnce)
                .check(&no_wildcards)
                .unwrap_err()
                .reason_code(),
            Some(0xA2)
        );
        // A literal filter is still fine against such a server.
        assert!(
            Subscription::new("sport/tennis", QoS::AtLeastOnce)
                .check(&no_wildcards)
                .is_ok()
        );

        let no_shared = ServerLimits {
            shared_subscription_available: false,
            ..ServerLimits::default()
        };
        assert_eq!(
            Subscription::new("$share/g/sport", QoS::AtLeastOnce)
                .check(&no_shared)
                .unwrap_err()
                .reason_code(),
            Some(0x9E)
        );
    }

    /// [MQTT-3.8.3-4]: No Local on a Shared Subscription is a Protocol Error,
    /// refused at configuration time so the message names the filter.
    #[test]
    fn no_local_on_a_shared_subscription_is_refused() {
        let limits = ServerLimits::default();
        let shared = Subscription::new("$share/g/sport/+", QoS::AtLeastOnce).no_local();
        let error = shared.check(&limits).expect_err("refused");
        assert!(matches!(error, Error::InvalidTopic { .. }), "{error}");
        assert!(error.to_string().contains("$share/g/sport/+"));

        // No Local on a non-shared filter is exactly what it is for: a bridge
        // that must not receive its own traffic back [mqtt5 §4.6].
        assert!(
            Subscription::new("sport/+", QoS::AtLeastOnce)
                .no_local()
                .check(&limits)
                .is_ok()
        );
    }

    /// The builders set the two bridge options and the Retain Handling, and
    /// the options byte round-trips through the codec — which is where Retain
    /// Handling 3 is refused, because it is not a value this type has.
    #[test]
    fn the_options_builders_reach_the_wire_byte() {
        let subscription = Subscription::new("a/+", QoS::ExactlyOnce)
            .no_local()
            .retain_as_published()
            .retain_handling(RetainHandling::DoNotSend);
        let byte = subscription.options.as_byte();
        assert_eq!(
            SubscriptionOptions::from_byte(byte),
            Ok(subscription.options)
        );
        assert_eq!(byte & 0b1100_0000, 0, "the reserved bits stay clear");

        // Retain Handling 3 is unrepresentable here and refused by the codec
        // when it comes off the wire (3.8.3.1).
        assert!(SubscriptionOptions::from_byte(0b0011_0000).is_err());
    }

    /// An empty level is a level, which is the case `split('/')` gets right
    /// and a hand-rolled tokenizer that skips empties gets wrong.
    #[test]
    fn empty_levels_are_levels() {
        assert!(matches("+/+", "/a"));
        assert!(matches("a//b", "a//b"));
        assert!(!matches("a/b", "a//b"));
        assert!(matches("a/+/b", "a//b"));
        assert!(matches("/", "/"));
        assert!(!matches("/", ""));
    }

    /// The filter is never longer than the field that carries it
    /// ([MQTT-4.7.3-3]).
    #[test]
    fn an_oversized_filter_is_refused() {
        let long = "a".repeat(MAX_TOPIC_BYTES);
        assert!(check_topic_filter(&long).is_ok());
        let too_long = "a".repeat(MAX_TOPIC_BYTES + 1);
        assert!(check_topic_filter(&too_long).is_err());
        assert!(check_topic_name(&too_long, false).is_err());
    }
}
