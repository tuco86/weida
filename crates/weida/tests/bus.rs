//! BUS: n members, each bound and connected.
//!
//! The property that distinguishes a bus from a fan-out is the one nobody can
//! infer from the types: a message reaches every **other** member and never
//! the sender. Here it is structural — `send` writes to the peers a member
//! dialled, and a member does not dial itself — so the test is a check on the
//! structure rather than on a filter.
//!
//! There is **no relay**: a bus of *n* members is *n* × (*n* − 1) deliveries.
//! `every_member_sees_every_other_members_message` is what that costs, made
//! visible.

use std::time::Duration;

use weida::{
    BusMember, Error, Identity, Limits, Listener, Runtime, RuntimeConfig, ServerTls, Trust,
};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// One bus member on a loopback port, with everything it needs held alive.
struct Member {
    member: BusMember,
    url: String,
    _runtime: Runtime,
    _listener: Listener,
    _binding: weida::Binding,
}

/// Starts `n` members, each trusting the others by pinned fingerprint.
async fn members(n: usize) -> Vec<Member> {
    members_with(n, RuntimeConfig::default()).await
}

/// Like [`members`], with an explicit runtime configuration for each member.
async fn members_with(n: usize, config: RuntimeConfig) -> Vec<Member> {
    let mut out = Vec::new();
    let mut fingerprints = Vec::new();
    let mut parts = Vec::new();
    for _ in 0..n {
        let runtime = Runtime::new(config.clone()).expect("runtime");
        let listener = runtime.listener();
        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback"),
                ServerTls::new(identity),
            )
            .await
            .expect("bind");
        let url = format!("weida://127.0.0.1:{}/bus", binding.local_addr().port());
        fingerprints.push(fingerprint);
        parts.push((runtime, listener, binding, url));
    }
    // Every member trusts every fingerprint on the bus: joining is a dial, so
    // trust is the only thing membership needs.
    let trust = fingerprints
        .into_iter()
        .fold(Trust::default(), Trust::and_pin);
    for (runtime, listener, binding, url) in parts {
        let member = listener
            .bus("/bus", weida::ClientTls::new(trust.clone()))
            .expect("bus member");
        out.push(Member {
            member,
            url,
            _runtime: runtime,
            _listener: listener,
            _binding: binding,
        });
    }
    out
}

/// Joins every member to every other one.
async fn join_all(members: &[Member]) {
    for (i, member) in members.iter().enumerate() {
        for (j, other) in members.iter().enumerate() {
            if i != j {
                within(member.member.connect(&other.url))
                    .await
                    .expect("join");
            }
        }
    }
}

#[tokio::test]
async fn every_member_sees_every_other_members_message() {
    let bus = members(3).await;
    join_all(&bus).await;
    assert_eq!(bus[0].member.peer_count(), 2);

    // n × (n − 1): three members, one message each, six deliveries.
    for (i, member) in bus.iter().enumerate() {
        let body = format!("from {i}");
        let reached = within(member.member.send(body.as_bytes()))
            .await
            .expect("send");
        assert_eq!(reached, 2, "a message reaches every other member");
    }

    for (i, member) in bus.iter().enumerate() {
        let mut seen = Vec::new();
        for _ in 0..2 {
            let transfer = within(member.member.recv()).await.expect("recv");
            assert_eq!(transfer.meta().endpoint.as_deref(), Some("/bus"));
            seen.push(within(transfer.collect(64)).await.expect("collect"));
        }
        seen.sort();
        let expected: Vec<Vec<u8>> = (0..3)
            .filter(|j| *j != i)
            .map(|j| format!("from {j}").into_bytes())
            .collect();
        assert_eq!(seen, expected, "member {i} saw the wrong set");
    }
}

#[tokio::test]
async fn a_sender_never_receives_its_own_message() {
    let bus = members(2).await;
    join_all(&bus).await;

    within(bus[0].member.send(b"mine")).await.expect("send");
    let seen = within(bus[1].member.recv())
        .await
        .expect("the other member");
    assert_eq!(within(seen.collect(64)).await.expect("collect"), b"mine");

    // The sender's own queue stays empty: nothing filters the copy out,
    // because no copy was ever addressed to it.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), bus[0].member.recv())
            .await
            .is_err(),
        "a bus member never receives its own message"
    );
}

#[tokio::test]
async fn a_dead_member_is_dropped_and_the_others_continue() {
    let mut bus = members(3).await;
    join_all(&bus).await;

    // One member leaves, which is an ordinary disconnect: its runtime goes
    // away and with it every connection it held.
    let leaving = bus.pop().expect("three members");
    leaving._runtime.shutdown().await;

    // The survivors notice by the peer count falling.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while bus[0].member.peer_count() > 1 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        bus[0].member.peer_count(),
        1,
        "a member that died is dropped from the set"
    );

    // One send after the death. A member's writer now ends on **either** of
    // two events — its connection closing, or a write failing — so which one
    // won decides whether a copy was made for the dead member at all. The
    // invariant that holds across both is what this asserts: a copy is never
    // silently lost, so a copy accepted for a member that is gone shows up in
    // `dropped()`, and a member pruned before the send is simply not reached.
    let reached = within(bus[0].member.send(b"first after"))
        .await
        .expect("send");
    assert!(
        (1..=2).contains(&reached),
        "one live member, and at most one copy for the member that died: {reached}"
    );
    if reached == 2 {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while bus[0].member.dropped() == 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            bus[0].member.dropped(),
            1,
            "a copy accepted for the dead member is counted, not lost"
        );
    }

    // And the survivors continue: the dead member's writer is pruned, so the
    // next send reaches exactly the members that are there.
    let reached = within(bus[0].member.send(b"still here"))
        .await
        .expect("send");
    assert_eq!(reached, 1);
    let first = within(bus[1].member.recv()).await.expect("recv");
    assert_eq!(
        within(first.collect(64)).await.expect("collect"),
        b"first after"
    );
    let second = within(bus[1].member.recv()).await.expect("recv");
    assert_eq!(
        within(second.collect(64)).await.expect("collect"),
        b"still here"
    );
}

#[tokio::test]
async fn a_slow_member_is_dropped_and_counted_rather_than_blocking() {
    // A member that never reads stalls its own writer, and past that queue a
    // copy is **counted** rather than retried — the same rule as Pub/Sub's
    // drops (`docs/GUARANTEES.md` §6). One writer queue slot per member
    // makes the boundary observable rather than theoretical.
    let bus = members_with(
        2,
        RuntimeConfig {
            endpoint_queue: 1,
            ..RuntimeConfig::default()
        },
    )
    .await;
    join_all(&bus).await;

    // Payloads large enough that a member which never reads stops draining
    // them: flow control stalls its writer, the writer's one slot fills, and
    // the next copy has nowhere to go.
    let payload = vec![0x5au8; 1024 * 1024];
    let mut reached = 0usize;
    for _ in 0..16u32 {
        match tokio::time::timeout(Duration::from_secs(2), bus[0].member.send(&payload)).await {
            Ok(Ok(n)) => reached += n,
            Ok(Err(e)) => panic!("a bus send failed: {e:?}"),
            // A send that cannot return inside two seconds is exactly the
            // blocking this test forbids: the whole point of a writer per
            // member is that a slow one costs its own copies, not the
            // sender's time.
            Err(_) => panic!("a slow member blocked the sender"),
        }
    }

    let dropped = bus[0].member.dropped();
    assert_eq!(
        reached + dropped as usize,
        16,
        "every copy is either accepted for delivery or counted: \
         {reached} reached, {dropped} dropped"
    );
    assert!(
        dropped > 0,
        "a member that never reads must cost copies rather than the sender"
    );
}

#[tokio::test]
async fn a_body_above_the_fan_out_budget_is_refused_rather_than_dropped_for_everyone() {
    // The same rule `Publisher::publish` follows: a message that could not be
    // enqueued for *anybody* is reported to the sender instead of being
    // dropped once per member, which would look like a delivered send with a
    // rising drop counter.
    let bus = members_with(
        2,
        RuntimeConfig {
            limits: Limits {
                subscriber_buffer_bytes: 64 * 1024,
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        },
    )
    .await;
    join_all(&bus).await;

    let refused = within(bus[0].member.send(&vec![0x5au8; 64 * 1024 + 1]))
        .await
        .expect_err("a body above the budget is refused");
    assert!(
        matches!(refused, Error::LimitExceeded),
        "expected LIMIT_EXCEEDED, got {refused:?}"
    );

    // The refusal costs the message, not the member: a body inside the
    // budget still goes out.
    assert_eq!(
        within(bus[0].member.send(b"inside the budget"))
            .await
            .expect("send"),
        1
    );
}

#[tokio::test]
async fn a_slow_member_is_bounded_in_bytes_before_its_message_count() {
    // A writer queue counted only in messages bounds nothing: a bus send
    // costs `endpoint_queue × body.len()` per member and multiplies by the
    // members, and `body.len()` is the application's number. With 256
    // message slots and a byte budget of four payloads, the byte budget is
    // what has to stop the fan-out — the same second dimension Pub/Sub
    // charges per subscriber (`docs/INVARIANTS.md`).
    let bus = members_with(
        2,
        RuntimeConfig {
            limits: Limits {
                subscriber_buffer_bytes: 128 * 1024,
                // Small enough that a member which never reads stalls its
                // writer after the first copy or two, so the budget is
                // reached rather than merely configured.
                stream_receive_window: 16 * 1024,
                connection_receive_window: 32 * 1024,
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        },
    )
    .await;
    join_all(&bus).await;

    let payload = vec![0x5au8; 32 * 1024];
    let mut reached = 0usize;
    for _ in 0..16u32 {
        match tokio::time::timeout(Duration::from_secs(2), bus[0].member.send(&payload)).await {
            Ok(Ok(n)) => reached += n,
            Ok(Err(e)) => panic!("a bus send failed: {e:?}"),
            Err(_) => panic!("a slow member blocked the sender"),
        }
    }

    let dropped = bus[0].member.dropped();
    assert_eq!(
        reached + dropped as usize,
        16,
        "every copy is either accepted for delivery or counted: \
         {reached} reached, {dropped} dropped"
    );
    assert!(
        reached <= 5,
        "the byte budget holds at most four payloads plus the one in flight, \
         but {reached} copies were accepted — the message count is bounding this, \
         not the bytes"
    );
}
