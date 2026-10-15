//! What the server refuses and keeps the connection after.
//!
//! # `-ERR` is not always the end
//!
//! "The `-ERR` message is used by the server to indicate a protocol,
//! authorization, or other runtime connection error to the client. Most of
//! these errors result in the server closing the connection", and the
//! reference's table of them has a `Recoverable` column in which `Invalid
//! Subject` and both `Permissions Violation` rows say yes. Those are the
//! server saying no to one operation and carrying on. Ending the connection
//! over one — which is what any other `-ERR` earns — would throw away every
//! subscription and every pending request on a healthy connection because of
//! one publish outside a user's permissions.
//!
//! So an `-ERR` is sorted by its text, as `nats.go` sorts it, and the ones
//! that leave the connection alone are a [`Refusal`]:
//!
//! | Text | Kind | Where it is established |
//! | --- | --- | --- |
//! | `Permissions Violation for Publish to <subject>` and `... with Reply of <subject>` | [`RefusalKind::PermissionViolation`] | the reference's table, and `nats.go`'s `processTransientError` |
//! | `Permissions Violation for Subscription to <subject>`, with ` using queue <q>`, ` (sid <n>)` or `, too many tokens` after it | [`RefusalKind::PermissionViolation`] | the same |
//! | `Invalid Subject`, and `Invalid Publish Subject` in `pedantic` mode | [`RefusalKind::InvalidSubject`] | the reference's table; `nats-server` sends the second and goes on reading |
//! | `Maximum Subscriptions Exceeded` | [`RefusalKind::SubscriptionLimit`] | `nats-server`'s `parseSub` ("we don't return an error here to not close the connection as a parsing error") and `nats.go` |
//!
//! Everything else — `Authorization Violation`, `Stale Connection`,
//! `Maximum Payload Violation`, `Maximum Control Line Exceeded`, `Unknown
//! Protocol Operation`, `Slow Consumer`, and any sentence this client has not
//! heard of — ends the connection as [`Error::Server`](crate::Error::Server),
//! because the server either closes behind it or has said nothing to suggest
//! it will not. An unknown sentence is not guessed at.
//!
//! # Who is told
//!
//! The server names a subject and never a publish, so a refusal cannot be
//! given back to the `publish` call that caused it: that call returned long
//! ago, and Core NATS has no acknowledgement to return it in. Every refusal is
//! therefore readable on the connection ([`Connection::refusals`]), and the
//! one kind that names the subscription it ended — a refused `SUB` — is given
//! to that [`Subscription`](crate::Subscription) as well, whose
//! [`next`](crate::Subscription::next) then ends and whose
//! [`refusal`](crate::Subscription::refusal) says why. A subscription the
//! server has refused will never be sent a message, and one that went on
//! waiting for one would be a silence where an answer was given.
//!
//! What the connection keeps for a reader that does not come is bounded by
//! [`ConnectionOptions::refusal_queue`](crate::options::ConnectionOptions::refusal_queue):
//! the server decides how many refusals there are, and an overload of them
//! costs the oldest ones, counted in [`Refusals::lost`], rather than memory.
//!
//! [`Connection::refusals`]: crate::Connection::refusals

use tokio::sync::broadcast;

/// What a recoverable `-ERR` refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefusalKind {
    /// The user the connection authenticated as may not do this: publish to a
    /// subject, publish with that reply subject, or subscribe to a subject.
    /// The operation did not happen, and a refused subscription never will.
    PermissionViolation,
    /// The server found a subject malformed. The operation did not happen.
    InvalidSubject,
    /// The connection already holds as many subscriptions as the server
    /// allows. The `SUB` was not taken, and the server does not say which one.
    SubscriptionLimit,
}

/// An `-ERR` the server sent and kept the connection open after.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Refusal {
    /// What was refused.
    pub kind: RefusalKind,
    /// The subject the server named, where its sentence names one this client
    /// can read back: the subject of a refused publish or subscription, or
    /// the reply subject of a publish whose reply it would not allow.
    pub subject: Option<String>,
    /// The server's own sentence, without the quotes that delimit it on the
    /// wire. The explanation is the server's, and folding it into a category
    /// would throw it away.
    pub reason: String,
}

/// The subscription a refusal says the server has ended, as the server named
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Named {
    pub(crate) subject: Vec<u8>,
    pub(crate) queue_group: Option<Vec<u8>>,
    /// Where the sentence carries one, which only a subscription the server
    /// removed does. The only exact way to say which one.
    pub(crate) sid: Option<u64>,
}

/// Sorts an `-ERR` reason: `Some` for the ones that leave the connection
/// alone, with the subscription it ended where it names one, and `None` for
/// every other, which ends it.
pub(crate) fn classify(reason: &str) -> Option<(Refusal, Option<Named>)> {
    let kind = if has_prefix(reason, "permissions violation") {
        RefusalKind::PermissionViolation
    } else if reason.eq_ignore_ascii_case("invalid subject")
        || reason.eq_ignore_ascii_case("invalid publish subject")
    {
        RefusalKind::InvalidSubject
    } else if has_prefix(reason, "maximum subscriptions exceeded") {
        RefusalKind::SubscriptionLimit
    } else {
        return None;
    };
    let named = (kind == RefusalKind::PermissionViolation)
        .then(|| named_subscription(reason))
        .flatten();
    let subject = match kind {
        RefusalKind::PermissionViolation => reason
            .find('"')
            .and_then(|at| quoted(&reason[at..]))
            .map(|(subject, _)| subject),
        RefusalKind::InvalidSubject | RefusalKind::SubscriptionLimit => None,
    };
    Some((
        Refusal {
            kind,
            subject,
            reason: reason.to_owned(),
        },
        named,
    ))
}

/// Whether `text` starts with `prefix`, ignoring ASCII case: `nats.go` lowers
/// the sentence before it compares, and the server's capitalisation is not
/// part of the protocol.
fn has_prefix(text: &str, prefix: &str) -> bool {
    text.as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// What `Permissions Violation for Subscription to "<subject>" [using queue
/// "<q>"] [(sid "<n>")]` names, and `None` for any other sentence or one whose
/// quoting this does not read.
///
/// The server writes each name with Go's `%q`. Only the two escapes a subject
/// can plausibly carry are undone; any other escape means the name cannot be
/// matched against what this client sent, and a refusal that cannot be
/// matched is reported on the connection and ends no subscription.
fn named_subscription(reason: &str) -> Option<Named> {
    let rest = reason.strip_prefix("Permissions Violation for Subscription to ")?;
    let (subject, mut rest) = quoted(rest)?;
    let mut queue_group = None;
    let mut sid = None;
    loop {
        if let Some(after) = rest.strip_prefix(" using queue ") {
            let (queue, after) = quoted(after)?;
            queue_group = Some(queue.into_bytes());
            rest = after;
        } else if let Some(after) = rest.strip_prefix(" (sid ") {
            let (text, after) = quoted(after)?;
            sid = Some(text.parse::<u64>().ok()?);
            rest = after.strip_prefix(')')?;
        } else {
            break;
        }
    }
    Some(Named {
        subject: subject.into_bytes(),
        queue_group,
        sid,
    })
}

/// A Go-quoted string at the start of `text`, unescaped, and what follows its
/// closing quote.
fn quoted(text: &str) -> Option<(String, &str)> {
    let body = text.strip_prefix('"')?;
    let mut out = String::new();
    let mut chars = body.char_indices();
    while let Some((at, c)) = chars.next() {
        match c {
            '"' => return Some((out, &body[at + 1..])),
            '\\' => match chars.next()? {
                (_, escaped @ ('"' | '\\')) => out.push(escaped),
                _ => return None,
            },
            plain => out.push(plain),
        }
    }
    None
}

/// The refusals sent after a [`Connection::refusals`] reader was made, in the
/// order the server sent them.
///
/// Bounded: a reader that falls more than
/// [`ConnectionOptions::refusal_queue`](crate::options::ConnectionOptions::refusal_queue)
/// behind loses the oldest, and [`Refusals::lost`] says how many.
///
/// [`Connection::refusals`]: crate::Connection::refusals
#[derive(Debug)]
pub struct Refusals {
    rx: broadcast::Receiver<Refusal>,
    lost: u64,
}

impl Refusals {
    pub(crate) const fn new(rx: broadcast::Receiver<Refusal>) -> Self {
        Self { rx, lost: 0 }
    }

    /// The next refusal, or `None` once the connection has ended and every
    /// refusal sent before it has been read.
    pub async fn next(&mut self) -> Option<Refusal> {
        loop {
            match self.rx.recv().await {
                Ok(refusal) => return Some(refusal),
                Err(broadcast::error::RecvError::Lagged(missed)) => self.lost += missed,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// The next refusal if one is already waiting.
    pub fn try_next(&mut self) -> Option<Refusal> {
        loop {
            match self.rx.try_recv() {
                Ok(refusal) => return Some(refusal),
                Err(broadcast::error::TryRecvError::Lagged(missed)) => self.lost += missed,
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => return None,
            }
        }
    }

    /// How many refusals this reader missed because it was too far behind to
    /// have them kept for it.
    #[must_use]
    pub const fn lost(&self) -> u64 {
        self.lost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind(reason: &str) -> Option<RefusalKind> {
        classify(reason).map(|(refusal, _)| refusal.kind)
    }

    /// The rows of the reference's table that say `Recoverable: Yes`, and the
    /// two `nats-server` sends beside them without ending the connection.
    #[test]
    fn the_recoverable_sentences_are_refusals() {
        for reason in [
            r#"Permissions Violation for Publish to "orders.created""#,
            r#"Permissions Violation for Publish with Reply of "_INBOX.x""#,
            r#"Permissions Violation for Subscription to "secret.>""#,
            r#"Permissions Violation for Subscription to "jobs" using queue "workers""#,
            r#"Permissions Violation for Subscription to "jobs" (sid "7")"#,
            r#"Permissions Violation for Subscription to "a.b.c", too many tokens"#,
        ] {
            assert_eq!(
                kind(reason),
                Some(RefusalKind::PermissionViolation),
                "{reason}"
            );
        }
        for reason in ["Invalid Subject", "Invalid Publish Subject"] {
            assert_eq!(kind(reason), Some(RefusalKind::InvalidSubject), "{reason}");
        }
        assert_eq!(
            kind("Maximum Subscriptions Exceeded"),
            Some(RefusalKind::SubscriptionLimit)
        );
    }

    /// `nats.go` lowers the sentence before it compares, so a server that
    /// capitalised differently is still understood.
    #[test]
    fn the_sentence_is_compared_without_regard_to_case() {
        assert_eq!(
            kind("permissions violation for publish to \"x\""),
            Some(RefusalKind::PermissionViolation)
        );
        assert_eq!(kind("INVALID SUBJECT"), Some(RefusalKind::InvalidSubject));
    }

    /// Every other `-ERR` in the reference's table ends the connection, and so
    /// does a sentence nobody has heard of.
    #[test]
    fn every_other_sentence_ends_the_connection() {
        for reason in [
            "Unknown Protocol Operation",
            "Attempted To Connect To Route Port",
            "Authorization Violation",
            "Authorization Timeout",
            "Invalid Client Protocol",
            "Maximum Control Line Exceeded",
            "Parser Error",
            "Secure Connection - TLS Required",
            "Stale Connection",
            "Maximum Connections Exceeded",
            "Slow Consumer",
            "Maximum Payload Violation",
            "User Authentication Expired",
            "Something Nobody Has Heard Of",
            "Invalid Subject, and then some",
            "",
        ] {
            assert_eq!(kind(reason), None, "{reason:?}");
        }
    }

    /// The subject the server names is read back, so a caller can tell which
    /// publish it was.
    #[test]
    fn the_named_subject_is_read_back() {
        let (refusal, named) =
            classify(r#"Permissions Violation for Publish to "orders.created""#).unwrap();
        assert_eq!(refusal.subject.as_deref(), Some("orders.created"));
        assert_eq!(named, None, "a publish names no subscription");
        let (refusal, _) = classify("Invalid Subject").unwrap();
        assert_eq!(refusal.subject, None);
        assert_eq!(refusal.reason, "Invalid Subject");
    }

    /// A refused subscription is named by subject, optionally by queue group,
    /// and by `sid` where the server removed it rather than refused it.
    #[test]
    fn a_refused_subscription_names_what_the_server_ended() {
        let (_, named) =
            classify(r#"Permissions Violation for Subscription to "secret.>""#).unwrap();
        assert_eq!(
            named,
            Some(Named {
                subject: b"secret.>".to_vec(),
                queue_group: None,
                sid: None,
            })
        );
        let (_, named) = classify(
            r#"Permissions Violation for Subscription to "jobs" using queue "workers" (sid "12")"#,
        )
        .unwrap();
        assert_eq!(
            named,
            Some(Named {
                subject: b"jobs".to_vec(),
                queue_group: Some(b"workers".to_vec()),
                sid: Some(12),
            })
        );
        let (_, named) =
            classify(r#"Permissions Violation for Subscription to "a.b.c", too many tokens"#)
                .unwrap();
        assert_eq!(named.map(|named| named.subject), Some(b"a.b.c".to_vec()));
    }

    /// A name that cannot be matched against what this client sent ends no
    /// subscription, but is still a refusal the connection reports.
    #[test]
    fn a_name_this_client_cannot_read_is_reported_and_attributes_nothing() {
        for reason in [
            r#"Permissions Violation for Subscription to "caf\u00e9""#,
            r#"Permissions Violation for Subscription to "unterminated"#,
            r#"Permissions Violation for Subscription to "jobs" (sid "not a number")"#,
        ] {
            let (refusal, named) = classify(reason).unwrap();
            assert_eq!(refusal.kind, RefusalKind::PermissionViolation, "{reason}");
            assert_eq!(named, None, "{reason}");
        }
    }

    #[test]
    fn the_two_escapes_a_subject_can_carry_are_undone() {
        assert_eq!(
            quoted(r#""a\"b\\c" tail"#),
            Some((r#"a"b\c"#.to_owned(), " tail"))
        );
        assert_eq!(quoted(r#""a\nb""#), None);
        assert_eq!(quoted("no quote"), None);
    }
}
