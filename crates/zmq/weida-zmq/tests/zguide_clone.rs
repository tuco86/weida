//! The zguide's chapter 5 state and pub-sub recipes - Clone (12/CHP), Binary
//! Star and the two proxy recipes - driven and **asserted against the RFC's
//! and the guide's own claims**, 0013 §4.7 clause 5.
//!
//! The examples are included as modules, so the code under test is the file a
//! reader runs.
//!
//! The claims, each named on its test:
//!
//! * CHP: `ICANHAZ?` is "answered by zero or more `KVSYNC` and then
//!   `KTHXBAI` carrying the highest sequence", and the client that subscribed
//!   first "discards those at or below the snapshot sequence" - the
//!   strict-increment rule, set up so that every raced update *must* be
//!   discarded rather than left to a race between two publishers.
//! * CHP: the server "centralizes every change and imposes one sequence in
//!   arrival order"; an update newer than the snapshot is applied; "an empty
//!   value meaning delete"; and `HUGZ` is a beat and not state.
//! * Binary Star: "at most one active server"; the three-way rule, "server
//!   will not become active until it receives application connection requests
//!   **and** cannot see peer", asserted in both halves; and "two actives
//!   would mean split-brain", which is the one case a peer can detect.
//! * Espresso: "all bridged traffic, control and data, is observable" - the
//!   subscriptions, the data and the unsubscriptions, in order.
//! * Last Value Caching: "immediate catch-up to the cached last value per
//!   subscribed topic, **not** the full stream".

use std::time::{Duration, Instant};

use weida_zmq::{Context, ContextConfig, Result};

#[path = "../examples/binary_star.rs"]
#[allow(dead_code)]
pub(crate) mod binary_star;
#[path = "../examples/clone.rs"]
#[allow(dead_code)]
pub(crate) mod clone;
#[path = "../examples/espresso.rs"]
#[allow(dead_code)]
pub(crate) mod espresso;
#[path = "../examples/last_value_cache.rs"]
#[allow(dead_code)]
pub(crate) mod last_value_cache;

use binary_star::{BinaryStar, Event, State, Verdict};
use clone::CloneClient;

/// Fast enough that a test is quick, slow enough that a loaded machine is not
/// a failure.
pub(crate) const BEAT: Duration = Duration::from_millis(100);
/// How long a wait runs before it reports; it never fails a test.
pub(crate) const PATIENCE: Duration = Duration::from_secs(5);

/// The patience handed to an example call whose expiry is not the claim: it
/// cannot fire in practice, and [`patient`] reports a slow call instead.
pub(crate) const FOREVER: Duration = Duration::from_secs(3600);

/// Awaits an example call that was given [`FOREVER`], reporting past
/// `PATIENCE` instead of failing.
pub(crate) async fn patient<T>(what: &str, call: impl Future<Output = T>) -> T {
    weida_testkit::within(what, PATIENCE, call).await
}

/// Long enough that "nothing more arrived" means it.
pub(crate) const QUIET: Duration = Duration::from_millis(300);

pub(crate) fn context() -> Result<Context> {
    Context::new(ContextConfig::default())
}

/// Claim, Binary Star: **the three-way rule** - "server will not become
/// active until it receives application connection requests **and** cannot
/// see peer". Both halves, and the third case the C asserts: a server that
/// has never seen its peer has no evidence of silence and stays put.
#[test]
fn a_passive_server_needs_both_the_silence_and_the_vote() {
    let now = Instant::now();
    //  Never met the peer: a request is not a vote, however long ago the
    //  server started - `bstar.c` asserts `peer_expiry > 0` here.
    let fresh = BinaryStar::new(State::Backup, BEAT);
    assert_eq!(
        fresh.classify(now + BEAT * 100),
        Event::ClientRequest,
        "no peer ever seen is not evidence of a dead peer"
    );

    //  Peer visible: a request is a request, and a passive server refuses it.
    let mut passive = BinaryStar::new(State::Backup, BEAT);
    passive.peer_seen(now);
    assert_eq!(
        passive.event(Event::Peer(State::Active)).unwrap(),
        Verdict::Noted
    );
    assert_eq!(passive.state(), State::Passive);
    assert_eq!(passive.classify(now + BEAT), Event::ClientRequest);
    assert_eq!(
        passive.event(Event::ClientRequest).unwrap(),
        Verdict::Reject,
        "the peer is doing the work"
    );
    assert_eq!(passive.state(), State::Passive, "and nothing changed");

    //  Two heartbeats of silence: the same request is now a vote, and the
    //  vote is what promotes.
    assert_eq!(
        passive.classify(now + BEAT * 2),
        Event::ClientVote,
        "two heartbeats of silence make a request a vote"
    );
    assert_eq!(passive.event(Event::ClientVote).unwrap(), Verdict::Serve);
    assert_eq!(passive.state(), State::Active, "and only then it is active");
}

/// Claim, Binary Star: **"two actives would mean split-brain"** - the one
/// case a peer can detect from the inside, reported as `EFSM` rather than
/// swallowed.
#[test]
fn an_active_server_told_its_peer_is_active_reports_a_split_brain() {
    let mut star = BinaryStar::new(State::Primary, BEAT);
    star.event(Event::Peer(State::Backup))
        .expect("a backup peer");
    assert_eq!(star.state(), State::Active);
    let error = star
        .event(Event::Peer(State::Active))
        .expect_err("a second active is fatal");
    assert_eq!(error.errno(), "EFSM", "{error:?}");
    assert!(
        error.cause().contains("dual actives"),
        "the C's own words: {error:?}"
    );
}

/// Waits for a condition, reporting past `PATIENCE` instead of failing.
pub(crate) async fn until(ready: impl FnMut() -> bool) {
    weida_testkit::until("the condition", PATIENCE, ready).await;
}

/// The same, for a condition that needs the client to read its socket first.
pub(crate) async fn until_async(
    mut ready: impl FnMut(&mut CloneClient) -> bool,
    client: &mut CloneClient,
) -> () {
    weida_testkit::within("the condition", PATIENCE, async {
        while !ready(client) {
            client
                .apply_pending(Duration::from_millis(50))
                .await
                .expect("reading the update stream");
        }
    })
    .await;
}
