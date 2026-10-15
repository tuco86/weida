//! Request-reply: the inbox, the mandatory window, and the two ways a
//! request can end without an answer.
//!
//! # There is no correlation field
//!
//! > A requester subscribes to a unique inbox and publishes a request whose
//! > reply-to field is that inbox. A responder receives the reply subject
//! > alongside the request and publishes its response to that subject. The
//! > reply is therefore routed by ordinary subject interest, not by a hidden
//! > correlation field. (`docs/research/nats.md` §4)
//!
//! So there is no correlation identifier anywhere in this crate — not on the
//! wire, not in a header, not in the message type. The reply subject *is* the
//! correlation: it is unique per request, the responder can only have learned
//! it from that request, and the server routes the answer by subject interest
//! like any other publication. A client that added an id would be adding a
//! second, weaker mechanism beside the one the protocol has.
//!
//! # One inbox subscription, not one per request
//!
//! The connection subscribes once to `<prefix><token>.>` and gives each
//! request a reply subject `<prefix><token>.<n>`. The alternative — a `SUB`
//! and an `UNSUB` around every request — costs two control lines per round
//! trip and makes the server do two table edits for each one.
//!
//! What that buys is one client-local map from reply subject to the waiter,
//! which is still subject routing: the wildcard subscription delivers
//! everything under the connection's inbox and the exact subject picks the
//! request. The map is remote-influenced in size — one entry per outstanding
//! request — so it has a named bound of ours,
//! [`ConnectionOptions::max_pending_requests`](crate::options::ConnectionOptions::max_pending_requests),
//! because the protocol bounds it nowhere. A request whose caller stopped
//! waiting — its future dropped, a timeout of the caller's own — leaves its
//! entry behind until the table is full; the table then sweeps the entries
//! whose waiter has gone before it refuses anyone, so abandoned requests cost a
//! slot for a while and never the ability to make the next one.
//!
//! # The inbox token is unguessable
//!
//! `_INBOX.` plus "something unique" is all the protocol asks for, and
//! uniqueness is all correctness needs: two requests must never share a reply
//! subject. But the reply subject is also the only thing that decides who may
//! answer. Anyone who can publish to this client's inbox can make their
//! payload the reply to its request, and in the default account — where
//! nothing restricts who may publish where — that is every other client. So
//! the token is 128 bits from the operating system's random number generator,
//! hex encoded: finding the subject is a search over 2^128, and a connection
//! that cannot get the bits does not open. The counter after it only keeps one
//! connection's reply subjects apart from each other.
//!
//! Where an application wants its inboxes under a prefix of its own — so that a
//! server permission can be written against it — it sets
//! [`ConnectionOptions::inbox_prefix`](crate::options::ConnectionOptions::inbox_prefix);
//! the unguessable token goes after it.
//!
//! # The window is the caller's and is mandatory
//!
//! Every request takes a `Duration` as a plain argument, not an
//! `Option<Duration>` with a default. "Client request APIs create and manage
//! an `_INBOX` reply subscription and impose a caller-selected timeout"
//! (§4) — and a request API that *can* hang is the failure this module
//! exists to prevent. Core NATS has no delivery receipt of any kind, so a
//! request with no deadline is a task that may never be scheduled again.
//!
//! # Two outcomes, never confused
//!
//! * `NATS/1.0 503` on the inbox — [`Error::NoResponders`] — means the
//!   server found no interest in the request's subject *at the moment it was
//!   published*. It arrives in one round trip, and only where `headers` and
//!   `no_responders` were both negotiated in `CONNECT`.
//! * The window elapsing — [`Error::RequestTimeout`] — means somebody may
//!   well have been listening and did not answer in time.
//!
//! Reporting the first as the second turns a missing service into a slow one
//! and costs the caller the whole window to find out.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rand::TryRngCore;
use rand::rngs::OsRng;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::queue::{Budget, Queued};

/// How many octets of the operating system's randomness go into an inbox
/// stem: 128 bits, which is out of reach of a search and past the 96 that
/// nothing less than a collision-free identifier should have.
const ENTROPY_BYTES: usize = 16;

/// The inbox of one connection: an unguessable stem and a counter under it.
#[derive(Debug)]
pub(crate) struct Inbox {
    /// `<prefix><token>`, without a trailing dot.
    stem: String,
    /// The next reply subject's last token.
    next: AtomicU64,
}

impl Inbox {
    /// Builds the inbox stem for a new connection.
    ///
    /// `prefix` ends in a dot, which
    /// [`ConnectionOptions::validate`](crate::options::ConnectionOptions::validate)
    /// has already checked, so the stem is `prefix` plus a token of hex
    /// digits — no dot, no space, nothing a control line would split on.
    ///
    /// Fails where the operating system has no random bits to give: an inbox
    /// that anyone can guess is not one this client will subscribe to.
    pub(crate) fn new(prefix: &str) -> Result<Self> {
        let mut entropy = [0u8; ENTROPY_BYTES];
        OsRng.try_fill_bytes(&mut entropy).map_err(|error| {
            Error::Runtime(weida_core::Error::Runtime(format!(
                "the operating system's random number generator failed, and an \
                 inbox that could be guessed is not one this client opens: {error}"
            )))
        })?;
        Ok(Self::from_entropy(prefix, &entropy))
    }

    /// The stem for `entropy`, hex encoded under `prefix`.
    fn from_entropy(prefix: &str, entropy: &[u8]) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut stem = String::with_capacity(prefix.len() + 2 * entropy.len());
        stem.push_str(prefix);
        for byte in entropy {
            stem.push(char::from(HEX[usize::from(byte >> 4)]));
            stem.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        Self {
            stem,
            next: AtomicU64::new(0),
        }
    }

    /// The subject the inbox subscription asks for: everything under the
    /// stem.
    ///
    /// `>` rather than `*` so that a nested reply subject — which JetStream's
    /// own conventions use — still arrives.
    pub(crate) fn pattern(&self) -> String {
        format!("{}.>", self.stem)
    }

    /// A reply subject no other request on this connection will be given.
    pub(crate) fn next_reply_subject(&self) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        format!("{}.{n}", self.stem)
    }
}

/// The requests waiting for a reply, keyed by their reply subject.
///
/// Owned by the driver, like the subscription table, and bounded by a number
/// of ours.
#[derive(Debug)]
pub(crate) struct Pending {
    entries: HashMap<Vec<u8>, mpsc::Sender<Queued>>,
    max: usize,
}

impl Pending {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max,
        }
    }

    /// The bound, for the error that reports it.
    pub(crate) const fn max(&self) -> usize {
        self.max
    }

    /// Registers a waiter. `false` where the map is full.
    ///
    /// "Full" is counted after the entries whose waiter has gone are swept
    /// out. A request whose caller dropped the future, or timed out on its own
    /// clock, never says so: its receiver is closed and its entry stays. If
    /// those were counted, enough abandoned requests would make every later
    /// one fail for the life of the connection.
    pub(crate) fn insert(&mut self, reply_subject: Vec<u8>, tx: mpsc::Sender<Queued>) -> bool {
        if self.entries.len() >= self.max {
            self.entries.retain(|_, waiter| !waiter.is_closed());
            if self.entries.len() >= self.max {
                return false;
            }
        }
        self.entries.insert(reply_subject, tx);
        true
    }

    pub(crate) fn remove(&mut self, reply_subject: &[u8]) {
        self.entries.remove(reply_subject);
    }

    /// Hands a reply to the request whose reply subject it arrived on.
    ///
    /// The subject is the only key. A reply that arrives for a request that
    /// has already given up is dropped, which is the honest answer: the
    /// caller's window is over and Core NATS keeps nothing for replay.
    ///
    /// The reply is charged against the connection's byte budget like any
    /// other queued message, and is dropped where it does not fit.
    pub(crate) fn dispatch(&mut self, message: Message, budget: &Arc<Budget>) -> bool {
        let Some(tx) = self.entries.get(&message.subject[..]) else {
            return false;
        };
        let Some(charge) = budget.try_charge(message.queued_len()) else {
            return false;
        };
        // Bounded and non-blocking: the waiter's channel is sized for the
        // responses it asked for, and a request that got its answers does not
        // get to stall the driver with the ones that came late.
        tx.try_send(Queued::new(message, charge)).is_ok()
    }

    /// Ends every outstanding request, which is what the end of the
    /// connection does to them.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Turns the first message on an inbox into the outcome it represents.
///
/// The 503 check is here rather than at the call sites so that the single and
/// the scatter-gather form cannot disagree about it.
pub(crate) fn outcome(message: Message) -> Result<Message> {
    if message.is_no_responders() {
        return Err(Error::NoResponders);
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::message::{NO_RESPONDERS, OwnedHeaders};

    fn roomy() -> Arc<Budget> {
        Budget::new(usize::MAX / 2)
    }

    /// Two connections in one process must not share an inbox stem, or one
    /// connection's replies would arrive on the other's wildcard
    /// subscription.
    #[test]
    fn two_inboxes_in_one_process_differ() {
        let first = Inbox::new("_INBOX.").expect("an inbox");
        let second = Inbox::new("_INBOX.").expect("an inbox");
        assert_ne!(first.stem, second.stem);
        assert!(first.stem.starts_with("_INBOX."));
        assert!(
            first.pattern().ends_with(".>"),
            "the inbox subscription takes everything under the stem"
        );
    }

    /// The token must be a single subject token: a dot or a space in it would
    /// silently change the shape of every reply subject and of the wildcard
    /// that collects them.
    #[test]
    fn the_inbox_token_is_one_subject_token() {
        let inbox = Inbox::new("_INBOX.").expect("an inbox");
        let token = inbox.stem.strip_prefix("_INBOX.").expect("the prefix");
        assert!(
            token.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{token:?}"
        );
        crate::subject::check_subscribe_subject(inbox.pattern().as_bytes())
            .expect("the inbox pattern is a subject this client would send");
        crate::subject::check_publish_subject(inbox.next_reply_subject().as_bytes())
            .expect("a reply subject is a literal subject");
    }

    /// The token is not something a neighbour in the account can enumerate: it
    /// carries at least 96 bits, and they are not the pid or the clock — every
    /// hex digit of it varies from one connection to the next.
    #[test]
    fn the_inbox_token_is_random() {
        let stems: Vec<String> = (0..256)
            .map(|_| Inbox::new("_INBOX.").expect("an inbox").stem)
            .collect();
        let tokens: Vec<&str> = stems
            .iter()
            .map(|stem| stem.strip_prefix("_INBOX.").expect("the prefix"))
            .collect();
        let width = tokens[0].len();
        assert!(width >= 24, "{width} hex digits is under 96 bits");
        assert!(tokens.iter().all(|token| token.len() == width));

        let distinct: std::collections::HashSet<&&str> = tokens.iter().collect();
        assert_eq!(distinct.len(), tokens.len(), "no two inboxes alike");

        // A digit position that came from a constant, a pid or a clock's high
        // half would hold one value across all of them. Random digits hold
        // most of the sixteen over 256 draws.
        for position in 0..width {
            let seen: std::collections::HashSet<u8> = tokens
                .iter()
                .map(|token| token.as_bytes()[position])
                .collect();
            assert!(
                seen.len() >= 8,
                "digit {position} took only {} values over 256 inboxes",
                seen.len()
            );
        }
    }

    /// The stem is the entropy in hex and nothing else, under the prefix the
    /// caller chose.
    #[test]
    fn the_stem_is_the_prefix_and_the_entropy() {
        let inbox = Inbox::from_entropy("app.replies.", &[0x00, 0xab, 0xff, 0x10]);
        assert_eq!(inbox.stem, "app.replies.00abff10");
        assert_eq!(inbox.pattern(), "app.replies.00abff10.>");
        assert_eq!(inbox.next_reply_subject(), "app.replies.00abff10.0");
        assert_eq!(inbox.next_reply_subject(), "app.replies.00abff10.1");
    }

    /// Each request gets its own reply subject, because that subject is the
    /// whole of the correlation.
    #[test]
    fn every_request_gets_its_own_reply_subject() {
        let inbox = Inbox::new("_INBOX.").expect("an inbox");
        let first = inbox.next_reply_subject();
        let second = inbox.next_reply_subject();
        assert_ne!(first, second);
        assert!(first.starts_with(&inbox.stem));
        assert!(crate::subject::matches(
            inbox.pattern().as_bytes(),
            second.as_bytes()
        ));
    }

    /// A reply is routed by its subject and nothing else, and one that has no
    /// waiter is dropped rather than delivered somewhere.
    #[test]
    fn a_reply_goes_to_the_subject_that_asked() {
        let budget = roomy();
        let mut pending = Pending::new(4);
        let (tx, mut rx) = mpsc::channel(1);
        assert!(pending.insert(b"_INBOX.a.1".to_vec(), tx));

        let mine = Message {
            subject: Bytes::from_static(b"_INBOX.a.1"),
            payload: Bytes::from_static(b"answer"),
            ..Message::default()
        };
        assert!(pending.dispatch(mine, &budget));
        assert_eq!(
            rx.try_recv().expect("delivered").into_message().payload,
            &b"answer"[..]
        );

        let stray = Message {
            subject: Bytes::from_static(b"_INBOX.a.2"),
            ..Message::default()
        };
        assert!(!pending.dispatch(stray, &budget), "no waiter, no delivery");

        pending.remove(b"_INBOX.a.1");
        let late = Message {
            subject: Bytes::from_static(b"_INBOX.a.1"),
            ..Message::default()
        };
        assert!(
            !pending.dispatch(late, &budget),
            "a reply that arrives after the caller gave up has nowhere to go"
        );
    }

    /// A reply is queued under the same byte budget as every other message,
    /// and is dropped where it does not fit.
    #[test]
    fn a_reply_over_the_byte_budget_is_dropped() {
        let reply = Message {
            subject: Bytes::from_static(b"_INBOX.a.1"),
            ..Message::default()
        };
        let budget = Budget::new(reply.queued_len());
        let mut pending = Pending::new(4);
        let (tx, mut rx) = mpsc::channel(4);
        assert!(pending.insert(b"_INBOX.a.1".to_vec(), tx));

        assert!(pending.dispatch(reply.clone(), &budget));
        assert!(
            !pending.dispatch(reply.clone(), &budget),
            "the budget holds one"
        );
        drop(rx.try_recv().expect("the first reply"));
        assert!(pending.dispatch(reply, &budget), "and room came back");
    }

    /// The map is bounded, because every entry is a request that may never be
    /// answered.
    #[test]
    fn the_pending_map_is_bounded() {
        let mut pending = Pending::new(1);
        let (first, _rx1) = mpsc::channel(1);
        let (second, _rx2) = mpsc::channel(1);
        assert!(pending.insert(b"_INBOX.a.1".to_vec(), first));
        assert!(!pending.insert(b"_INBOX.a.2".to_vec(), second));
        assert_eq!(pending.max(), 1);
    }

    /// A request whose caller stopped waiting — the receiver is gone — does
    /// not count against the bound once the map is full: it is swept, and the
    /// next request gets its slot. Abandoned requests must not accumulate into
    /// a connection that refuses every request for the rest of its life.
    #[test]
    fn abandoned_requests_are_swept_before_the_map_refuses() {
        let mut pending = Pending::new(2);
        let (live_tx, _live_rx) = mpsc::channel(1);
        let (gone_tx, gone_rx) = mpsc::channel(1);
        assert!(pending.insert(b"_INBOX.a.live".to_vec(), live_tx));
        assert!(pending.insert(b"_INBOX.a.gone".to_vec(), gone_tx));

        let (tx, _rx) = mpsc::channel(1);
        assert!(
            !pending.insert(b"_INBOX.a.new".to_vec(), tx.clone()),
            "two waiters are both still there"
        );

        // The caller of one gives up — the future is dropped — and says
        // nothing.
        drop(gone_rx);
        assert!(
            pending.insert(b"_INBOX.a.new".to_vec(), tx),
            "the abandoned entry is swept to make room"
        );
        let budget = roomy();
        let reply = |subject: &'static [u8]| Message {
            subject: Bytes::from_static(subject),
            ..Message::default()
        };
        assert!(pending.dispatch(reply(b"_INBOX.a.live"), &budget));
        assert!(
            !pending.dispatch(reply(b"_INBOX.a.gone"), &budget),
            "the abandoned entry is gone"
        );
    }

    /// A 503 is not an answer, and the conversion says so in one place so the
    /// two request forms cannot disagree.
    #[test]
    fn a_503_is_the_no_responder_error_and_not_a_reply() {
        let mut headers = OwnedHeaders::new();
        headers.status = Some(NO_RESPONDERS);
        let message = Message {
            headers: Some(headers),
            ..Message::default()
        };
        assert!(matches!(outcome(message), Err(Error::NoResponders)));

        let ordinary = Message {
            payload: Bytes::from_static(b"answer"),
            ..Message::default()
        };
        assert_eq!(
            outcome(ordinary).expect("a reply").payload,
            &b"answer"[..],
            "an ordinary reply passes through untouched"
        );
    }
}
