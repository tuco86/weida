//! Subscriptions: the publisher-side table, its reference counts, and the two
//! wire forms.
//!
//! **Filtering is the publisher's job.** 37/ZMTP: filtering "SHALL happen at
//! the publisher side (the PUB or XPUB socket)", and "a subscription of 'A'
//! SHALL match all messages starting with 'A'. An empty subscription SHALL
//! match all messages" (`docs/research/zeromq.md` §4.3). So a publisher keeps
//! one of these per subscriber and compares each message's **first frame**
//! against it.
//!
//! **Subscriptions are additive and not idempotent**, which is a counting
//! rule and not a set rule: "subscribing to 'A' and 'A' counts as two
//! subscriptions, and would require two CANCEL commands to undo" (§2). A set
//! would lose that, so this is a multiset — and the count is exactly why the
//! table needs a ceiling, because a peer can open N of them with N commands.
//!
//! **Matching is a walk down a compressed prefix tree, not a scan.** A peer
//! chooses the table's contents, up to `max_subscriptions` prefixes of
//! `max_subscription_bytes` each, and a publisher matches every message
//! against every peer's table. A scan costs one comparison per prefix, which
//! would let the peer set the publisher's cost. A radix tree follows at most
//! one edge per topic byte whatever the table holds, and it is kept in a
//! canonical shape so its size stays proportional to the stored bytes.
//!
//! **Two wire forms exist and a widely interoperable implementation accepts
//! both.** The 3.x form is the `SUBSCRIBE`/`CANCEL` commands; the ZMTP 2.0
//! form is a one-frame *message* whose first octet is `%x01` for subscribe or
//! `%x00` for unsubscribe. Which one a 3.x peer sends "is not settled by the
//! specification", and `zeromq` 0.6 — the pure-Rust implementation the interop
//! bench uses — announces 3.0 and speaks only the 2.0 form (§4.3). They are
//! distinguishable on the wire, since a command frame carries the COMMAND
//! flag, so this library **accepts both** and lets the sent form be chosen.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

/// `%x01`: the first octet of a subscribe in the message form.
pub const SUBSCRIBE_PREFIX: u8 = 1;

/// `%x00`: the first octet of an unsubscribe in the message form.
pub const CANCEL_PREFIX: u8 = 0;

/// Subscriptions one socket may hold for one peer, by default.
///
/// **Not a libzmq option.** 37/ZMTP bounds a subscription neither in length
/// nor in count, and subscriptions are non-idempotent, so "N repeated
/// `SUBSCRIBE` commands cost N entries" is a memory cost a peer chooses
/// (`docs/research/zeromq.md` §11), and `docs/INVARIANTS.md` allows no remote
/// input that causes unbounded allocation. 1024 is far above any real topic
/// set — the zguide's examples subscribe to one or a handful.
///
/// **A count alone is not a bound**: with the length unbounded, 1024 prefixes
/// of a megabyte each are a megabyte times 1024 per peer. Both dimensions
/// are capped, so the product
/// is `DEFAULT_MAX_SUBSCRIPTIONS × DEFAULT_MAX_SUBSCRIPTION_BYTES` per peer
/// — 256 KiB — times `max_peers` peers.
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 1024;

/// Longest subscription prefix, in bytes, by default.
///
/// **Also not a libzmq option**, and 37/ZMTP's grammar is explicitly
/// unbounded here — "subscription = *OCTET" (`docs/research/zeromq.md` §11)
/// — so the number is ours to choose and to state. 256 B is
/// `docs/PROTOCOL.md` §10's filter bound, and it is far above any real topic:
/// the longest prefix in the zguide's examples is a handful of characters,
/// and weida's own filters live under the same ceiling.
pub const DEFAULT_MAX_SUBSCRIPTION_BYTES: usize = 256;

/// Which wire form a socket **sends** its subscriptions in.
///
/// Receiving accepts both, always: a publisher that read only one form would
/// silently have no subscribers from an implementation that sends the other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubscriptionForm {
    /// The 3.x `SUBSCRIBE`/`CANCEL` commands. The default, because this
    /// library speaks ZMTP 3.1.
    #[default]
    Commands,
    /// ZMTP 2.0's one-frame message, `%x01`/`%x00` then the prefix. What a
    /// 3.0 peer such as `zeromq` 0.6 reads.
    LegacyMessage,
}

/// One peer's subscriptions: a bounded multiset of byte prefixes.
///
/// Shared between the session that reads the peer's commands and the socket
/// that matches messages against them, so cloning the `Arc` is how the two
/// meet.
pub struct Subscriptions {
    max: usize,
    max_bytes: usize,
    state: Mutex<State>,
}

impl fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The prefixes are left out: a peer chooses them, up to
        // `max × max_bytes` bytes, and a derived Debug would copy all of that
        // into whatever logs the peer's session.
        let state = self.lock();
        f.debug_struct("Subscriptions")
            .field("max", &self.max)
            .field("max_bytes", &self.max_bytes)
            .field("distinct", &state.distinct)
            .field("refused", &state.refused)
            .finish_non_exhaustive()
    }
}

/// Index of the root node in [`State::nodes`]; it spells the empty prefix.
const ROOT: usize = 0;

/// The table: a compressed prefix tree in an arena.
///
/// Every stored prefix is the path from the root to a node with a non-zero
/// count. The tree is kept canonical after every cancel (see
/// [`State::prune`]): a non-root node without a count has at least two
/// children, so there are at most two nodes per distinct prefix, and sibling
/// edges start with different bytes, so a child is found by one binary
/// search. The arena keeps the tree flat, so dropping it is not recursive
/// however deep a peer makes it.
struct State {
    nodes: Vec<Node>,
    /// Slots of released nodes, reused before the arena grows, so subscribe
    /// and cancel churn cannot grow the arena past the peak table size.
    free: Vec<usize>,
    /// Distinct prefixes, which is what the ceiling counts.
    distinct: usize,
    refused: u64,
}

#[derive(Default)]
struct Node {
    /// Subscriptions to the prefix this node spells; zero for an interior
    /// node. The count is the non-idempotence rule.
    count: usize,
    /// Sorted by the first byte of each label.
    edges: Vec<Edge>,
}

struct Edge {
    /// The bytes between the parent and `child`. Never empty: an empty label
    /// would make two nodes spell the same path.
    label: Box<[u8]>,
    child: usize,
}

impl Node {
    /// The edge whose label starts with `byte`, or the position it would take.
    fn edge_for(&self, byte: u8) -> Result<usize, usize> {
        self.edges.binary_search_by_key(&byte, |edge| edge.label[0])
    }
}

impl State {
    fn new() -> State {
        State {
            nodes: vec![Node::default()],
            free: Vec::new(),
            distinct: 0,
            refused: 0,
        }
    }

    fn alloc(&mut self, node: Node) -> usize {
        match self.free.pop() {
            Some(index) => {
                self.nodes[index] = node;
                index
            }
            None => {
                self.nodes.push(node);
                self.nodes.len() - 1
            }
        }
    }

    fn release(&mut self, index: usize) {
        self.nodes[index] = Node::default();
        self.free.push(index);
    }

    /// The node that spells `prefix`, if one exists, and the (parent, edge
    /// index) pairs leading to it, appended to `path`.
    fn find(&self, prefix: &[u8], path: &mut Vec<(usize, usize)>) -> Option<usize> {
        let mut node = ROOT;
        let mut rest = prefix;
        while let Some(&byte) = rest.first() {
            let at = self.nodes[node].edge_for(byte).ok()?;
            let edge = &self.nodes[node].edges[at];
            // A label that is longer than what is left, or that differs from
            // it, means no node spells `prefix`.
            rest = rest.strip_prefix(&*edge.label)?;
            path.push((node, at));
            node = edge.child;
        }
        Some(node)
    }

    /// Creates the nodes for `prefix`, which the caller knows is not held,
    /// and returns the node that spells it. The caller sets its count.
    ///
    /// An edge is split only where a new prefix diverges from it, so every
    /// interior node created here has at least two children once the
    /// caller's leaf is in.
    fn insert(&mut self, prefix: &[u8]) -> usize {
        let mut node = ROOT;
        let mut rest = prefix;
        loop {
            let Some(&byte) = rest.first() else {
                return node;
            };
            match self.nodes[node].edge_for(byte) {
                Err(at) => {
                    let child = self.alloc(Node::default());
                    self.nodes[node].edges.insert(
                        at,
                        Edge {
                            label: rest.into(),
                            child,
                        },
                    );
                    return child;
                }
                Ok(at) => {
                    let edge = &self.nodes[node].edges[at];
                    let common = common_len(&edge.label, rest);
                    if common == edge.label.len() {
                        node = edge.child;
                    } else {
                        // The new prefix leaves the edge partway: the edge is
                        // cut at `common`, and the old child hangs below a
                        // node that now sits at the cut.
                        let head: Box<[u8]> = edge.label[..common].into();
                        let tail: Box<[u8]> = edge.label[common..].into();
                        let child = edge.child;
                        let mid = self.alloc(Node {
                            count: 0,
                            edges: vec![Edge { label: tail, child }],
                        });
                        self.nodes[node].edges[at] = Edge {
                            label: head,
                            child: mid,
                        };
                        node = mid;
                    }
                    rest = &rest[common..];
                }
            }
        }
    }

    /// Restores the canonical shape after `node` lost its count. An interior
    /// node needs two children, so a childless one is removed and an only
    /// child is joined to the edge above it. Removing a leaf can leave its
    /// parent short as well, so this walks up and stops at the first node
    /// that needs nothing.
    fn prune(&mut self, mut node: usize, path: &[(usize, usize)]) {
        let mut depth = path.len();
        while node != ROOT && self.nodes[node].count == 0 {
            let (parent, at) = path[depth - 1];
            match self.nodes[node].edges.len() {
                0 => {
                    self.nodes[parent].edges.remove(at);
                    self.release(node);
                    node = parent;
                    depth -= 1;
                }
                1 => {
                    let below = self.nodes[node]
                        .edges
                        .pop()
                        .expect("one edge was just counted");
                    let mut label =
                        std::mem::take(&mut self.nodes[parent].edges[at].label).into_vec();
                    label.extend_from_slice(&below.label);
                    self.nodes[parent].edges[at] = Edge {
                        label: label.into_boxed_slice(),
                        child: below.child,
                    };
                    self.release(node);
                    return;
                }
                _ => return,
            }
        }
    }

    /// Calls `visit` with each held prefix and its count.
    ///
    /// Iterative: an explicit stack of frames plus one buffer holding the path
    /// of the top frame, so a deep table cannot overflow the thread's stack.
    fn walk(&self, mut visit: impl FnMut(&[u8], usize)) {
        struct Frame {
            node: usize,
            next: usize,
            /// Length of the path buffer when this frame was entered.
            base: usize,
        }
        let mut path: Vec<u8> = Vec::new();
        let root = &self.nodes[ROOT];
        if root.count > 0 {
            visit(&path, root.count);
        }
        let mut stack = vec![Frame {
            node: ROOT,
            next: 0,
            base: 0,
        }];
        while let Some(top) = stack.last_mut() {
            let node = &self.nodes[top.node];
            if let Some(edge) = node.edges.get(top.next) {
                top.next += 1;
                let base = path.len();
                path.extend_from_slice(&edge.label);
                let child = &self.nodes[edge.child];
                if child.count > 0 {
                    visit(&path, child.count);
                }
                stack.push(Frame {
                    node: edge.child,
                    next: 0,
                    base,
                });
            } else {
                path.truncate(top.base);
                stack.pop();
            }
        }
    }
}

fn common_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

impl Subscriptions {
    /// An empty table holding at most `max` distinct prefixes, each at most
    /// `max_bytes` long.
    ///
    /// Both bounds are needed: a count without a length lets one peer hold
    /// `max` prefixes of any size at all.
    pub fn new(max: usize, max_bytes: usize) -> Subscriptions {
        Subscriptions {
            max,
            max_bytes,
            state: Mutex::new(State::new()),
        }
    }

    /// The longest prefix this table accepts.
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Adds one subscription to `prefix`.
    ///
    /// Returns whether this is the **first** subscription to that prefix,
    /// which is what a deduplicating XPUB reports to its application, and
    /// `None` when the subscription was refused — either because the table
    /// is at its count ceiling or because the prefix is longer than
    /// [`Subscriptions::max_bytes`].
    pub fn subscribe(&self, prefix: &[u8]) -> Option<bool> {
        let mut state = self.lock();
        if prefix.len() > self.max_bytes {
            // A prefix's length is as much a peer's choice as its count, and
            // 37/ZMTP bounds neither.
            state.refused += 1;
            return None;
        }
        if let Some(node) = state.find(prefix, &mut Vec::new()) {
            // A repeat of a held prefix takes no new slot, so it is never
            // refused by the ceiling.
            let count = &mut state.nodes[node].count;
            if *count > 0 {
                *count += 1;
                return Some(false);
            }
        }
        if state.distinct >= self.max {
            state.refused += 1;
            return None;
        }
        let node = state.insert(prefix);
        state.nodes[node].count = 1;
        state.distinct += 1;
        Some(true)
    }

    /// Removes one subscription to `prefix`.
    ///
    /// Returns whether that was the **last** one, so the prefix is gone —
    /// two `SUBSCRIBE`s need two `CANCEL`s, and only the second changes what
    /// a publisher sends. `None` means there was no such subscription.
    pub fn cancel(&self, prefix: &[u8]) -> Option<bool> {
        let mut state = self.lock();
        let mut path = Vec::new();
        let node = state.find(prefix, &mut path)?;
        let count = &mut state.nodes[node].count;
        if *count == 0 {
            // An interior node of the tree, not a held prefix.
            return None;
        }
        *count -= 1;
        if *count > 0 {
            return Some(false);
        }
        state.distinct -= 1;
        state.prune(node, &path);
        Some(true)
    }

    /// Whether `first_frame` matches any subscription.
    ///
    /// "A binary comparison of the subscription against the start of the
    /// first frame of the message", and an empty subscription matches
    /// everything. A table with nothing in it matches nothing, which is what
    /// makes a fresh SUB silent.
    ///
    /// The walk follows one edge per step and stops at the first terminal, so
    /// its cost is bounded by the topic's length, whatever the table holds.
    /// It allocates nothing.
    pub fn matches(&self, first_frame: &[u8]) -> bool {
        let state = self.lock();
        let mut node = &state.nodes[ROOT];
        let mut rest = first_frame;
        loop {
            // Reaching a terminal means its whole path is a prefix of the
            // topic: the walk only follows labels that the topic starts with.
            if node.count > 0 {
                return true;
            }
            let Some(&byte) = rest.first() else {
                // The topic ends inside the table: every prefix below is longer.
                return false;
            };
            let Ok(at) = node.edge_for(byte) else {
                return false;
            };
            let edge = &node.edges[at];
            let Some(tail) = rest.strip_prefix(&*edge.label) else {
                return false;
            };
            rest = tail;
            node = &state.nodes[edge.child];
        }
    }

    /// Distinct prefixes held.
    pub fn len(&self) -> usize {
        self.lock().distinct
    }

    /// Whether nothing is subscribed — a fresh SUB, which "shall filter out
    /// all incoming messages".
    pub fn is_empty(&self) -> bool {
        self.lock().distinct == 0
    }

    /// How many subscriptions were refused at the ceiling.
    pub fn refused(&self) -> u64 {
        self.lock().refused
    }

    /// The prefixes held, each once, in no particular order.
    ///
    /// What a socket re-sends to a publisher it has just (re)connected to:
    /// the *set*, not the counts, because the counts are the subscriber's own
    /// bookkeeping and the publisher only needs to know what to match.
    pub fn prefixes(&self) -> Vec<Vec<u8>> {
        let state = self.lock();
        let mut out = Vec::with_capacity(state.distinct);
        state.walk(|prefix, _| out.push(prefix.to_vec()));
        out
    }

    /// Every subscription, with its count, for a socket that has to unwind
    /// them — XSUB "SHOULD send unsubscribe requests for all subscriptions"
    /// when it closes a connection to a publisher.
    pub fn counted(&self) -> Vec<(Vec<u8>, usize)> {
        let state = self.lock();
        let mut out = Vec::with_capacity(state.distinct);
        state.walk(|prefix, count| out.push((prefix.to_vec(), count)));
        out
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("subscription table poisoned")
    }
}

/// Reads the message form of a subscription: `%x01`/`%x00` and a prefix.
///
/// `None` for anything else. "Messages without a sub/unsub prefix are also
/// received, but have no effect on subscription status", which is why this
/// answers rather than refuses.
pub fn read_message_form(frame: &[u8]) -> Option<(bool, &[u8])> {
    match frame.first() {
        Some(&SUBSCRIBE_PREFIX) => Some((true, &frame[1..])),
        Some(&CANCEL_PREFIX) => Some((false, &frame[1..])),
        _ => None,
    }
}

/// Writes the message form: `%x01`/`%x00` then the prefix.
pub fn write_message_form(subscribe: bool, prefix: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(prefix.len() + 1);
    frame.push(if subscribe {
        SUBSCRIBE_PREFIX
    } else {
        CANCEL_PREFIX
    });
    frame.extend_from_slice(prefix);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    /// Claim: the table counts rather than sets — two subscriptions to one
    /// prefix need two cancellations, and only the second changes what a
    /// publisher matches. 37/ZMTP's non-idempotence, which a `HashSet` would
    /// have lost.
    #[test]
    fn subscriptions_are_additive_and_not_idempotent() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        assert_eq!(subs.subscribe(b"topic"), Some(true), "the first is new");
        assert_eq!(subs.subscribe(b"topic"), Some(false), "the second is not");
        assert_eq!(subs.len(), 1, "one prefix, twice");

        assert_eq!(subs.cancel(b"topic"), Some(false), "still subscribed");
        assert!(subs.matches(b"topic/a"));
        assert_eq!(subs.cancel(b"topic"), Some(true), "now it is gone");
        assert!(!subs.matches(b"topic/a"));
        assert_eq!(subs.cancel(b"topic"), None, "and there is nothing left");
    }

    /// Claim: the match is a binary prefix comparison against the first
    /// frame; an empty subscription takes everything and an empty table
    /// takes nothing.
    #[test]
    fn the_match_is_a_binary_prefix() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        assert!(
            !subs.matches(b"anything"),
            "a fresh subscriber filters everything out"
        );

        subs.subscribe(b"weather.uk").expect("room");
        assert!(subs.matches(b"weather.uk.london"));
        assert!(subs.matches(b"weather.uk"));
        assert!(!subs.matches(b"weather.us"));
        // Bytes, not segments: a prefix may end mid-word, which is the loss
        // weida's own segmented filters exist to avoid.
        assert!(subs.matches(b"weather.uk-ish"));

        subs.subscribe(b"").expect("room");
        assert!(subs.matches(b"anything at all"));
        assert!(subs.matches(b""));
    }

    /// Claim: the table has a ceiling, and a peer past it is refused rather
    /// than served — N non-idempotent subscriptions cost N entries, and the
    /// count is the peer's choice.
    #[test]
    fn the_table_has_a_ceiling() {
        let subs = Subscriptions::new(2, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        assert_eq!(subs.subscribe(b"a"), Some(true));
        assert_eq!(subs.subscribe(b"b"), Some(true));
        assert_eq!(subs.subscribe(b"c"), None, "past the ceiling");
        assert_eq!(subs.refused(), 1);
        assert_eq!(subs.len(), 2);

        // A repeat of one already held still counts, because it allocates
        // nothing new.
        assert_eq!(subs.subscribe(b"a"), Some(false));
        assert!(!subs.matches(b"c"));
    }

    /// Claim: **a prefix is bounded in length as well as in count.** One at
    /// the ceiling is accepted and still filters; one octet more is refused,
    /// because 37/ZMTP's `subscription = *OCTET` bounds nothing and a
    /// count-only ceiling would let one peer hold `max` prefixes of any size.
    #[test]
    fn a_prefix_is_bounded_in_length() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, 8);
        assert_eq!(subs.max_bytes(), 8);

        let longest = vec![b'x'; 8];
        assert_eq!(subs.subscribe(&longest), Some(true), "at the ceiling");
        let mut matching = longest.clone();
        matching.extend_from_slice(b" and more");
        assert!(
            subs.matches(&matching),
            "a prefix at the ceiling must still filter"
        );

        let over = vec![b'x'; 9];
        assert_eq!(subs.subscribe(&over), None, "one octet too many");
        assert_eq!(subs.refused(), 1);
        assert_eq!(subs.len(), 1, "and nothing was stored for it");
        assert!(!subs.matches(&[b'y'; 9]));
    }

    /// Claim: the message form round-trips, and anything else is not a
    /// subscription at all.
    #[test]
    fn the_message_form_round_trips() {
        let framed = write_message_form(true, b"topic");
        assert_eq!(framed, vec![1, b't', b'o', b'p', b'i', b'c']);
        assert_eq!(
            read_message_form(&framed),
            Some((true, b"topic".as_slice()))
        );

        let framed = write_message_form(false, b"topic");
        assert_eq!(
            read_message_form(&framed),
            Some((false, b"topic".as_slice()))
        );

        // An empty subscription is legal in both directions.
        assert_eq!(read_message_form(&[1]), Some((true, b"".as_slice())));
        // And a payload that is not a subscription says so.
        assert_eq!(read_message_form(b"hello"), None);
        assert_eq!(read_message_form(&[]), None);
    }

    /// The naive table the tree must agree with: a count per prefix, and a
    /// scan on every match, with the same ceilings.
    struct Model {
        counts: HashMap<Vec<u8>, usize>,
        max: usize,
        max_bytes: usize,
        refused: u64,
    }

    impl Model {
        fn new(max: usize, max_bytes: usize) -> Model {
            Model {
                counts: HashMap::new(),
                max,
                max_bytes,
                refused: 0,
            }
        }

        fn subscribe(&mut self, prefix: &[u8]) -> Option<bool> {
            if prefix.len() > self.max_bytes {
                self.refused += 1;
                return None;
            }
            if let Some(count) = self.counts.get_mut(prefix) {
                *count += 1;
                return Some(false);
            }
            if self.counts.len() >= self.max {
                self.refused += 1;
                return None;
            }
            self.counts.insert(prefix.to_vec(), 1);
            Some(true)
        }

        fn cancel(&mut self, prefix: &[u8]) -> Option<bool> {
            let count = self.counts.get_mut(prefix)?;
            *count -= 1;
            if *count == 0 {
                self.counts.remove(prefix);
                Some(true)
            } else {
                Some(false)
            }
        }

        fn matches(&self, topic: &[u8]) -> bool {
            self.counts.keys().any(|prefix| topic.starts_with(prefix))
        }

        fn sorted_prefixes(&self) -> Vec<Vec<u8>> {
            let mut prefixes: Vec<Vec<u8>> = self.counts.keys().cloned().collect();
            prefixes.sort();
            prefixes
        }

        fn sorted_counted(&self) -> Vec<(Vec<u8>, usize)> {
            let mut counted: Vec<(Vec<u8>, usize)> = self
                .counts
                .iter()
                .map(|(prefix, count)| (prefix.clone(), *count))
                .collect();
            counted.sort();
            counted
        }
    }

    /// Deterministic xorshift64, so the operation mix is reproducible and no
    /// dependency is needed. The seed must be non-zero.
    struct XorShift(u64);

    impl XorShift {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        /// A value in `0..n`; `n` must be non-zero.
        fn below(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// A random byte string up to `max_len` long over `a`, `b`, and the two
    /// bytes that sort at the ends, so that nested prefixes are common.
    fn random_bytes(rng: &mut XorShift, max_len: usize) -> Vec<u8> {
        let len = rng.below(max_len + 1);
        (0..len)
            .map(|_| match rng.below(16) {
                0 => 0x00,
                1 => 0xff,
                r => b"ab"[r % 2],
            })
            .collect()
    }

    /// Every topic of length `0..=max_len` over `a`, `b`, `0x00`, `0xff`.
    fn all_topics(max_len: usize) -> Vec<Vec<u8>> {
        const ALPHABET: [u8; 4] = [b'a', b'b', 0x00, 0xff];
        let mut all = vec![Vec::new()];
        let mut level: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..max_len {
            level = level
                .iter()
                .flat_map(|topic| {
                    ALPHABET.iter().map(move |&byte| {
                        let mut next = topic.clone();
                        next.push(byte);
                        next
                    })
                })
                .collect();
            all.extend(level.clone());
        }
        all
    }

    /// The shape invariants, checked by walking the arena: every slot is the
    /// root, reachable from it, or free, and in exactly one of those; an
    /// interior node branches; sibling labels are non-empty and start with
    /// different bytes, in order; terminals number `distinct`.
    fn assert_canonical(subs: &Subscriptions) {
        let state = subs.lock();
        let mut owned = vec![false; state.nodes.len()];
        owned[ROOT] = true;
        let mut terminals = 0;
        let mut stack = vec![ROOT];
        while let Some(index) = stack.pop() {
            let node = &state.nodes[index];
            if index != ROOT && node.count == 0 {
                assert!(
                    node.edges.len() >= 2,
                    "interior node {index} has {} children",
                    node.edges.len()
                );
            }
            if node.count > 0 {
                terminals += 1;
            }
            let mut previous: Option<u8> = None;
            for edge in &node.edges {
                assert!(!edge.label.is_empty(), "node {index} has an empty label");
                assert!(
                    previous < Some(edge.label[0]),
                    "node {index} has unsorted or duplicate edges"
                );
                previous = Some(edge.label[0]);
                assert!(!owned[edge.child], "node {} reached twice", edge.child);
                owned[edge.child] = true;
                stack.push(edge.child);
            }
        }
        for &index in &state.free {
            assert!(!owned[index], "free slot {index} is still in use");
            assert!(
                state.nodes[index].count == 0 && state.nodes[index].edges.is_empty(),
                "free slot {index} was not cleared"
            );
            owned[index] = true;
        }
        assert!(
            owned.iter().all(|&o| o),
            "an arena slot is neither reachable nor free"
        );
        assert_eq!(terminals, state.distinct, "terminal count");
    }

    fn assert_agrees(subs: &Subscriptions, model: &Model, ctx: &str) {
        assert_eq!(subs.len(), model.counts.len(), "{ctx}: len");
        assert_eq!(subs.is_empty(), model.counts.is_empty(), "{ctx}: is_empty");
        assert_eq!(subs.refused(), model.refused, "{ctx}: refused");
        let mut prefixes = subs.prefixes();
        prefixes.sort();
        assert_eq!(prefixes, model.sorted_prefixes(), "{ctx}: prefixes");
        let mut counted = subs.counted();
        counted.sort();
        assert_eq!(counted, model.sorted_counted(), "{ctx}: counted");
        assert_canonical(subs);
    }

    /// Runs `ops` random operations against the tree and the model in step,
    /// comparing everything the public API exposes after each one.
    fn model_check(seed: u64, max: usize, max_bytes: usize, ops: usize) {
        let subs = Subscriptions::new(max, max_bytes);
        let mut model = Model::new(max, max_bytes);
        let mut rng = XorShift(seed);
        let topics = all_topics(6);
        for step in 0..ops {
            let ctx = format!("seed {seed:#x} op {step}");
            match rng.below(10) {
                0..=3 => {
                    let prefix = random_bytes(&mut rng, 12);
                    assert_eq!(
                        subs.subscribe(&prefix),
                        model.subscribe(&prefix),
                        "{ctx}: subscribe {prefix:?}"
                    );
                }
                4..=6 => {
                    // Mostly a held prefix, so cancels reach the prune paths.
                    let prefix = if model.counts.is_empty() || rng.below(4) == 0 {
                        random_bytes(&mut rng, 12)
                    } else {
                        let held = model.sorted_prefixes();
                        held[rng.below(held.len())].clone()
                    };
                    assert_eq!(
                        subs.cancel(&prefix),
                        model.cancel(&prefix),
                        "{ctx}: cancel {prefix:?}"
                    );
                }
                7 => {
                    let prefix = random_bytes(&mut rng, 12);
                    assert_eq!(
                        subs.cancel(&prefix),
                        model.cancel(&prefix),
                        "{ctx}: cancel {prefix:?}"
                    );
                }
                _ => {
                    let topic = random_bytes(&mut rng, 12);
                    assert_eq!(
                        subs.matches(&topic),
                        model.matches(&topic),
                        "{ctx}: matches {topic:?}"
                    );
                }
            }
            assert_agrees(&subs, &model, &ctx);
            if step % 250 == 0 {
                for topic in &topics {
                    assert_eq!(
                        subs.matches(topic),
                        model.matches(topic),
                        "{ctx}: topic {topic:?}"
                    );
                }
            }
        }
    }

    /// Claim: the same agreement holds when both ceilings are small enough to
    /// be hit constantly, so refusal and the repeat-takes-no-slot rule are
    /// exercised on every few operations.
    #[test]
    fn the_tree_agrees_with_a_naive_table_at_small_ceilings() {
        for seed in [0x1357_9BDF_2468_ACE1, 0xF00D_F00D_F00D_F00D] {
            model_check(seed, 40, 8, 2000);
            model_check(seed ^ 0x55, 6, 3, 2000);
        }
    }

    /// Claim: a table that is fully cancelled is the empty table again. No
    /// node is left behind, the tree is canonical after every step, and it
    /// takes a new subscription afterwards.
    #[test]
    fn cancelling_everything_leaves_no_nodes_behind() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        let mut prefixes: Vec<Vec<u8>> = (0..=12).map(|n| vec![b'a'; n]).collect();
        prefixes.extend([
            b"ab".to_vec(),
            b"aab".to_vec(),
            b"aba".to_vec(),
            b"b".to_vec(),
            b"ba".to_vec(),
        ]);
        for prefix in &prefixes {
            assert_eq!(subs.subscribe(prefix), Some(true));
            assert_eq!(subs.subscribe(prefix), Some(false));
            assert_canonical(&subs);
        }
        assert_eq!(subs.len(), prefixes.len());

        // Each prefix is held twice, so two shuffled rounds of cancels: the
        // first only decrements, the second removes.
        let mut rng = XorShift(0x00C0_FFEE);
        for round in 0..2 {
            let mut order = prefixes.clone();
            for i in (1..order.len()).rev() {
                order.swap(i, rng.below(i + 1));
            }
            for prefix in &order {
                assert_eq!(
                    subs.cancel(prefix),
                    Some(round == 1),
                    "round {round}: {prefix:?}"
                );
                assert_canonical(&subs);
            }
        }

        assert!(subs.is_empty());
        assert_eq!(subs.len(), 0);
        assert!(subs.prefixes().is_empty());
        for topic in [&b""[..], b"a", b"aaaa", b"abb", b"zzz"] {
            assert!(!subs.matches(topic), "nothing matches an empty table");
        }
        assert_eq!(subs.cancel(b"a"), None);
        {
            let state = subs.lock();
            assert!(state.nodes[ROOT].edges.is_empty());
            assert_eq!(
                state.free.len() + 1,
                state.nodes.len(),
                "every node but the root was released"
            );
        }

        assert_eq!(subs.subscribe(b"ab"), Some(true));
        assert!(subs.matches(b"abc"));
        assert_canonical(&subs);
    }

    /// Claim: the Debug form never prints a prefix. A peer chooses the
    /// prefixes, so a derived Debug would copy up to `max × max_bytes` bytes of
    /// peer input into logs whenever a session is logged.
    #[test]
    fn debug_does_not_print_the_prefixes() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        subs.subscribe(b"peer-chosen-topic").expect("room");
        let text = format!("{subs:?}");
        assert!(!text.contains("peer-chosen"), "{text}");
    }

    /// The fastest of several batches of `matches` calls. Scheduler noise only
    /// ever adds time, so the minimum is the cost of the work itself.
    fn best_batch(subs: &Subscriptions, topic: &[u8]) -> Duration {
        (0..7)
            .map(|_| {
                let start = Instant::now();
                for _ in 0..4000 {
                    std::hint::black_box(subs.matches(std::hint::black_box(topic)));
                }
                start.elapsed()
            })
            .min()
            .expect("seven batches")
    }

    /// Claim: matching cost does not grow with the table. A peer chooses the
    /// table, so a scan would let it multiply every publish's cost by up to
    /// `max_subscriptions`. The adversarial table shares a long stem with the
    /// topic and matches nothing, and it is compared with a table holding one
    /// prefix of the same stem.
    #[test]
    fn matching_cost_does_not_grow_with_the_table() {
        let stem = vec![b'A'; 200];
        let topic = [stem.as_slice(), b"x"].concat();

        let full = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        for n in 0..DEFAULT_MAX_SUBSCRIPTIONS {
            let prefix = [stem.as_slice(), format!("{n:04}").as_bytes()].concat();
            assert_eq!(full.subscribe(&prefix), Some(true));
        }
        let single = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        single
            .subscribe(&[stem.as_slice(), b"0000"].concat())
            .expect("room");
        assert!(!full.matches(&topic));
        assert!(!single.matches(&topic));

        let full_cost = best_batch(&full, &topic);
        let single_cost = best_batch(&single, &topic);
        assert!(
            full_cost < single_cost * 20,
            "1024 prefixes cost {full_cost:?} per batch, one prefix {single_cost:?}"
        );
    }

    /// Claim: a chain of 1024 nested prefixes is built, matched, cancelled and
    /// dropped without recursion, so no operation on it can overflow the stack
    /// whatever lengths a peer picks. Half of it is still held at the drop.
    #[test]
    fn a_deep_chain_builds_queries_cancels_and_drops() {
        const DEPTH: usize = 1024;
        let subs = Subscriptions::new(2048, 4096);
        for len in 1..=DEPTH {
            assert_eq!(subs.subscribe(&vec![b'a'; len]), Some(true));
        }
        assert_eq!(subs.len(), DEPTH);
        assert!(subs.matches(&vec![b'a'; DEPTH + 476]));
        assert!(!subs.matches(b"b"));
        assert_eq!(subs.prefixes().len(), DEPTH);
        assert_eq!(subs.counted().len(), DEPTH);

        for len in 1..=DEPTH / 2 {
            assert_eq!(subs.cancel(&vec![b'a'; len]), Some(true));
        }
        assert_eq!(subs.len(), DEPTH / 2);
        assert!(subs.matches(&vec![b'a'; DEPTH]));
        assert!(!subs.matches(&vec![b'a'; DEPTH / 2 - 1]));
        // The rest is dropped with the table still holding them.
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: the tree is observably the naive table — the same return values,
        /// counts, refusals, key sets and matches — over random operation mixes
        /// that hit the table ceiling, the length ceiling, and the prune paths.
        #[test]
        fn the_tree_agrees_with_a_naive_table() {
            for seed in [
                0x9E37_79B9_7F4A_7C15,
                0x0123_4567_89AB_CDEF,
                0xDEAD_BEEF_CAFE_F00D,
                0x5DEE_CE66_D000_0001,
            ] {
                model_check(
                    seed,
                    DEFAULT_MAX_SUBSCRIPTIONS,
                    DEFAULT_MAX_SUBSCRIPTION_BYTES,
                    2000,
                );
            }
        }
    }
}
