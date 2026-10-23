//! The tests of `tests/guide.rs` that need the network, a process or real time.

use crate::guide::guide_late_is_lost::{layered, sfu, supersession};
use crate::guide::guide_many_peers::{
    ceilings, selection, slow_reader, survey_with_a_silent_peer, width,
};
use crate::guide::guide_one_transfer::{
    Outcome, STREAMED, gigabyte, hello, how_far, outcome, receipt_cost,
};
use std::time::Duration;

use crate::guide::*;

/// Claim §1.1: an address and a trust decision are the whole setup.
#[tokio::test]
async fn an_address_and_a_trust_decision_are_the_whole_setup() {
    let answered = within(hello()).await.expect("the exchange");
    assert_eq!(answered.reply, b"hello, world");
    assert!(
        answered.peer_was_named,
        "the address carries the peer's fingerprint, which is what `Trust::by_address` trusts"
    );
}
/// Claim §1.2: the payload never has to exist anywhere, and a cap is a
/// refusal rather than a bigger buffer — on both sides.
#[tokio::test]
async fn the_same_calls_carry_a_payload_no_cap_would_admit() {
    let streamed = within(gigabyte()).await.expect("the transfers");
    assert_eq!(
        streamed.bytes, STREAMED as u64,
        "every byte arrived, through one 64 KiB buffer"
    );
    assert!(
        streamed.collect_refused,
        "`collect` under a 1 MiB cap must refuse a 64 MiB payload rather than grow to it"
    );
    let told = streamed
        .sender_was_told
        .expect("the sender learns of a refused payload rather than succeeding into nothing");
    assert!(
        told.contains("reject") || told.contains("cancel"),
        "the refusal reaches the sender as a refusal: {told}"
    );
}
/// Claim §1.3: the outcome is a value, and which case a sender can be sure
/// of is decided by the pattern.
#[tokio::test]
async fn what_a_sender_can_be_sure_of_is_decided_by_the_pattern() {
    let outcomes = within(outcome()).await.expect("the senders");
    assert_eq!(
        outcomes.push_served,
        Outcome::Delivered,
        "a transfer to a path somebody serves resolves on the peer's transport receipt"
    );

    // The race of 0005, asserted as a race: either answer is correct, and
    // neither is `Indeterminate` — nothing here loses a connection.
    assert!(
        matches!(
            outcomes.push_unserved,
            Outcome::Delivered | Outcome::Refused(_)
        ),
        "a misrouted push is delivered or refused depending on whether the refusal \
         overtakes the acknowledgement, never indeterminate: {:?}",
        outcomes.push_unserved
    );

    match outcomes.request_unserved {
        Outcome::Refused(reason) => assert!(
            reason.contains("endpoint"),
            "an exchange to an unserved path is refused by name: {reason}"
        ),
        other => panic!("an exchange has a reply half, so the refusal is definite: {other:?}"),
    }
}
/// Claim §1.4: "did it arrive" and "how far did the far end get" are two
/// questions, and both are answered separately.
#[tokio::test]
async fn the_receipt_and_the_cursor_are_two_different_facts() {
    let far = within(how_far()).await.expect("the transfer");
    assert!(far.delivered, "the transport receipt resolved");
    assert_eq!(
        far.reported,
        Some(far.sent),
        "the receiving application reported an absolute offset, and it consumed the whole payload"
    );
}
/// Claim §1.5: the receipt is a round trip. What is asserted is that both
/// shapes deliver; the numbers are printed by the example, because a timing
/// assertion on a shared machine pins a load rather than a protocol.
#[tokio::test]
async fn both_send_shapes_deliver_and_one_of_them_waits() {
    let cost = within(receipt_cost(4)).await.expect("both shapes");
    assert_eq!(cost.rounds, 4);
    assert!(
        cost.with > Duration::ZERO && cost.without > Duration::ZERO,
        "both shapes completed and were measured"
    );
}
/// Claim §2.1: the pattern chooses the peer set; the call does not change.
///
/// Every number here is structural rather than timed: six pushes over three
/// pullers is two each because the selection cursor is one `fetch_add` per
/// send, a publish reaches every matching subscriber, and a bus member reaches
/// every other member and never itself.
#[tokio::test]
async fn the_pattern_chooses_the_peer_set() {
    let selected = within(selection()).await.expect("the three fan-outs");
    assert_eq!(
        selected.push_per_peer,
        vec![2, 2, 2],
        "round-robin over three peers divides six messages evenly"
    );
    assert_eq!(
        selected.fanout_per_subscriber,
        vec![2, 2, 2],
        "a publish reaches every subscriber whose filter matches"
    );
    assert_eq!(
        selected.bus_per_member,
        vec![2, 2, 2],
        "three members, one send each, and nobody hears itself"
    );
}
/// Claim §2.2: a subscriber that stops reading costs its neighbour nothing,
/// and costs itself nothing until the publisher outruns its transport.
///
/// The mechanism is asserted, the rate is printed. How many messages the
/// silent subscriber absorbs before the first loss depends on the connection's
/// in-flight capacity on the day, so pinning it would pin a machine.
#[tokio::test]
async fn a_silent_subscriber_costs_its_neighbour_nothing() {
    let slow = within(slow_reader()).await.expect("the two phases");
    assert_eq!(
        slow.healthy_received, slow.published,
        "the reading subscriber lost nothing while its neighbour read nothing at all"
    );
    assert_eq!(
        slow.reached_both, slow.published,
        "the silent subscriber never left: every copy was enqueued for both"
    );
    assert_eq!(
        slow.dropped_while_paced, 0,
        "a paced publisher loses nothing, however far behind a subscriber is"
    );
    assert!(
        slow.absorbed > 0,
        "the unpaced phase published at least one message"
    );
    assert!(
        slow.dropped > 0 && slow.cause != "nothing was dropped",
        "an unpaced publisher outruns a silent subscriber and the loss is counted by cause: \
         {slow:?}"
    );
    println!(
        "§2.2 paced {} of {} received, then {} absorbed before the first drop on {}",
        slow.healthy_received, slow.published, slow.absorbed, slow.cause
    );
}
/// Claim §2.3: the two ceilings are distinguishable, and which one refuses
/// follows from the budget rather than from the payload.
///
/// Structural, not timed: a budget an order of magnitude below the
/// connection's in-flight capacity must be exhausted while QUIC is still
/// accepting, and one an order of magnitude above it cannot be, so the queue
/// is what backs up. Both directions are asserted because a change that broke
/// either would make `dropped_on` lie.
#[tokio::test]
async fn each_ceiling_names_itself_when_it_is_the_one_that_binds() {
    let found = within(ceilings()).await.expect("both budgets");
    assert_eq!(found.len(), 2, "one row per budget");
    assert_eq!(
        found[0].bound, "the byte budget",
        "a 64 KiB budget is spent before QUIC stops accepting: {:?}",
        found[0]
    );
    assert_eq!(
        found[1].bound, "the queue",
        "an 8 MiB budget outlasts the connection's capacity, so the queue fills: {:?}",
        found[1]
    );
    assert!(
        found[0].bytes >= found[0].budget,
        "the budget was actually reached: {:?}",
        found[0]
    );
    println!(
        "§2.3 {} KiB budget -> {} after {} KiB; {} KiB budget -> {} after {} KiB",
        found[0].budget >> 10,
        found[0].bound,
        found[0].bytes >> 10,
        found[1].budget >> 10,
        found[1].bound,
        found[1].bytes >> 10
    );
}
/// Claim §2.4: the publisher's cost per message grows with the width, and
/// every copy still arrives.
///
/// The arrival count and the drop count are asserted; the timing is printed.
/// One timing comparison is safe enough to assert because it spans a factor of
/// five and is structurally necessary — a publish to 32 subscribers does 32
/// times the enqueueing of a publish to one.
#[tokio::test]
async fn width_costs_the_publisher_and_loses_nothing() {
    let widths = within(width(&[1, 32], 16)).await.expect("both widths");
    assert_eq!(widths.len(), 2);
    for measured in &widths {
        assert_eq!(
            measured.copies,
            measured.subscribers * 16,
            "every subscriber received every message: {measured:?}"
        );
        assert_eq!(measured.dropped, 0, "nobody was starved: {measured:?}");
    }
    assert!(
        widths[1].per_publish > widths[0].per_publish,
        "a publish to 32 subscribers costs more than a publish to one: {widths:?}"
    );
    println!(
        "§2.4 {:?} at width {}, {:?} at width {}",
        widths[0].per_publish, widths[0].subscribers, widths[1].per_publish, widths[1].subscribers
    );
}
/// Claim §2.5: a survey is a fan-out with a deadline, so a silent peer is a
/// number rather than a hang.
#[tokio::test]
async fn a_silent_respondent_is_a_number() {
    // The silent respondent is the claim, so the window must elapse; it is
    // wide enough that only a stall of seconds makes an answer late.
    let surveyed = within(survey_with_a_silent_peer(std::time::Duration::from_secs(5)))
        .await
        .expect("the survey");
    assert_eq!(surveyed.asked, 3);
    assert_eq!(
        surveyed.answered, 2,
        "the two answering respondents answered inside the deadline"
    );
    assert_eq!(
        surveyed.missing, 1,
        "the respondent that accepted the question and never answered is a count, not a stall"
    );
}
/// Claim §2.6: a dish that falls behind loses the old segment, never the new
/// one, and never at another dish's expense.
#[tokio::test]
async fn a_dish_that_falls_behind_loses_the_old_segment() {
    let seen = within(supersession()).await.expect("the broadcast");
    assert_eq!(
        seen.fast_received,
        (0..10).collect::<Vec<u64>>(),
        "the dish that keeps up receives every segment, whole and in order"
    );
    assert!(
        seen.stalled_superseded >= 1,
        "the stalled dish lost old segments to their successors"
    );
}
/// Claim §2.7: an SFU and a relay are loops over opaque payload.
#[tokio::test]
async fn an_sfu_and_a_relay_are_loops_over_opaque_payload() {
    let forwarded = within(sfu()).await.expect("the SFU and the relay");
    assert!(
        forwarded.forwarded >= 90,
        "{} of 100 frames reached both listeners",
        forwarded.forwarded
    );
    assert_eq!(forwarded.relayed_segments, 5);
    assert!(
        forwarded.payload_opaque,
        "every byte that arrived is a byte that was sent"
    );
}
/// Claim §2.8: a viewer's quality is a cap it states, not a second encoding.
#[tokio::test]
async fn quality_is_a_cap_per_viewer_not_a_second_encoding() {
    let seen = within(layered()).await.expect("the layered relay");
    assert_eq!(
        seen.capped,
        (0..5).map(|n| (n, 0)).collect::<Vec<(u64, u8)>>(),
        "the capped viewer receives layer 0 of every segment"
    );
    let mut uncapped = seen.uncapped.clone();
    uncapped.sort_unstable();
    assert_eq!(
        uncapped,
        (0..5)
            .flat_map(|n| (0..3).map(move |l| (n, l)))
            .collect::<Vec<(u64, u8)>>(),
        "the uncapped viewer receives every layer of every segment"
    );
    assert!(
        !seen.capped_heard_more,
        "the capped viewer hears nothing else"
    );
    assert!(
        seen.payload_whole,
        "every layer arrives as the sharer wrote it"
    );
}
/// The guide's own rule, checked against the guide: every claim a chapter
/// makes is asserted somewhere in this workspace.
///
/// Not a test of prose — a test of the **count**. Each chapter numbers its
/// claims `§N.1` upward, and each is a `pub` entry point in that chapter's
/// example. A claim added to the document without a program and a test fails
/// here, which is the only mechanism that keeps a guide from drifting into
/// prose nobody runs.
///
/// The count lives here beside the assertions for every written chapter.
#[test]
fn every_claim_the_chapters_make_has_a_program_and_a_test() {
    let guide = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/GUIDE.md")
            .canonicalize()
            .expect("the guide is in the tree"),
    )
    .expect("read the guide");

    // One row per written chapter.
    for (chapter, asserted) in [(1usize, 5usize), (2, 8)] {
        let prefix = format!("**Claim §{chapter}.");
        let claims: Vec<&str> = guide
            .lines()
            .filter(|line| line.starts_with(&prefix))
            .collect();
        assert_eq!(
            claims.len(),
            asserted,
            "chapter {chapter} states these claims, and this file asserts {asserted} of them: \
             {claims:?}"
        );
        for (n, claim) in claims.iter().enumerate() {
            let expected = format!("**Claim §{chapter}.{}", n + 1);
            assert!(
                claim.starts_with(&expected),
                "the claims are numbered in order; expected {expected}, found {claim}"
            );
        }
    }
}
