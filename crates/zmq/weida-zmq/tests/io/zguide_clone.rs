//! The tests of `tests/zguide_clone.rs` that need the network, a process or real time.

use crate::zguide_clone::binary_star::{State, Verdict, bind_state_pub, start_star};
use crate::zguide_clone::clone::{CloneClient, clone_server};
use crate::zguide_clone::espresso::{espresso, render};
use crate::zguide_clone::last_value_cache::last_value_cache;
use std::sync::atomic::Ordering;
use weida_zmq::{Message, Multipart, PubSocket, Result, SubSocket};

use crate::zguide_clone::*;

/// Claim, 12/CHP: **the snapshot dialog carries the whole state and the
/// sequence it was taken at**, and **every update that raced the snapshot is
/// discarded by the strict-increment rule.**
///
/// Deterministic by construction, not by racing: the reader subscribes and
/// waits for a `HUGZ`, so its subscription is live *before* any update is
/// written. Every one of the three updates therefore reaches its SUB queue,
/// and every one carries a sequence at or below the snapshot's - so the
/// discard count is exactly three, and a rule that applied them instead would
/// not merely be slow to notice, it would fail here.
#[tokio::test]
async fn a_snapshot_carries_the_state_and_the_raced_updates_are_discarded() -> Result<()> {
    let context = context()?;
    let server = clone_server(&context, BEAT).await?;
    let mut writer = CloneClient::connect(&context, &server.ports, "/client/")?;
    let mut reader = CloneClient::connect(&context, &server.ports, "/client/")?;
    //  The subscription is live once a beat has arrived over it.
    patient("the first beat", reader.wait_for_beat(FOREVER)).await?;

    for (key, value) in [("/client/a", "1"), ("/client/b", "2"), ("/client/a", "3")] {
        patient("the update", writer.set(key, value.as_bytes(), FOREVER)).await?;
    }
    //  Wait for the server to have numbered all three before asking, so the
    //  snapshot is the one that contains them.
    until(|| server.sequence.load(Ordering::Relaxed) == 3).await;

    patient("the snapshot", reader.snapshot(FOREVER)).await?;
    assert_eq!(reader.sequence, 3, "KTHXBAI carried the highest sequence");
    assert_eq!(reader.map.len(), 2, "two keys, one of them written twice");
    assert_eq!(
        reader.map.get("/client/a").map(Vec::as_slice),
        Some(b"3".as_slice()),
        "the snapshot has the later value"
    );

    //  The three updates are sitting in the SUB queue, all at or below the
    //  snapshot sequence.
    until_async(
        |reader: &mut CloneClient| reader.discarded == 3,
        &mut reader,
    )
    .await;
    reader.apply_pending(QUIET).await?;
    assert_eq!(
        reader.discarded, 3,
        "every update that raced the snapshot was discarded"
    );
    assert_eq!(reader.sequence, 3, "and none of them moved the sequence");
    Ok(())
}
/// Claim, 12/CHP: **an update newer than the snapshot is applied, an empty
/// value deletes, and `HUGZ` is a beat rather than state.** The other side of
/// the discard rule: strictly greater is the test, so the first update after
/// the snapshot must land.
#[tokio::test]
async fn an_update_after_the_snapshot_is_applied_and_an_empty_value_deletes() -> Result<()> {
    let context = context()?;
    let server = clone_server(&context, BEAT).await?;
    let mut writer = CloneClient::connect(&context, &server.ports, "/client/")?;
    let mut reader = CloneClient::connect(&context, &server.ports, "/client/")?;
    patient("the first beat", reader.wait_for_beat(FOREVER)).await?;
    patient("the update", writer.set("/client/a", b"1", FOREVER)).await?;
    until(|| server.sequence.load(Ordering::Relaxed) == 1).await;
    patient("the snapshot", reader.snapshot(FOREVER)).await?;
    reader.apply_pending(QUIET).await?;
    let beats_after_snapshot = reader.beats;

    //  Newer than the snapshot: applied.
    patient("the update", writer.set("/client/b", b"2", FOREVER)).await?;
    until_async(
        |reader: &mut CloneClient| reader.map.contains_key("/client/b"),
        &mut reader,
    )
    .await;
    assert_eq!(reader.sequence, 2, "the sequence moved with it");

    //  "an empty value meaning delete"
    patient("the delete", writer.set("/client/a", b"", FOREVER)).await?;
    until_async(
        |reader: &mut CloneClient| !reader.map.contains_key("/client/a"),
        &mut reader,
    )
    .await;
    assert_eq!(
        reader.map.keys().collect::<Vec<_>>(),
        vec!["/client/b"],
        "the delete removed the key rather than storing an empty value"
    );

    //  And the beats kept coming without touching the map.
    let keys_before = reader.map.len();
    until_async(
        |reader: &mut CloneClient| reader.beats > beats_after_snapshot,
        &mut reader,
    )
    .await;
    assert!(
        reader.beats > beats_after_snapshot,
        "HUGZ kept arriving: {} then {}",
        beats_after_snapshot,
        reader.beats
    );
    assert_eq!(reader.map.len(), keys_before, "and changed no state");
    Ok(())
}
/// Claim, Binary Star: **at most one active server.** A primary and a backup
/// that can see each other settle into exactly one active and one passive,
/// and the passive refuses client requests - "the passive does no work".
#[tokio::test]
async fn a_pair_settles_on_exactly_one_active() -> Result<()> {
    let context = context()?;
    let (primary_pub, primary_endpoint) = bind_state_pub(&context).await?;
    let (backup_pub, backup_endpoint) = bind_state_pub(&context).await?;
    let primary = start_star(
        &context,
        State::Primary,
        primary_pub,
        &backup_endpoint,
        BEAT,
    )?;
    let backup = start_star(&context, State::Backup, backup_pub, &primary_endpoint, BEAT)?;

    until(|| primary.state() == State::Active && backup.state() == State::Passive).await;
    //  Both halves of "at most one": the active serves, the passive refuses.
    assert_eq!(primary.client_request()?, Verdict::Serve, "the active one");
    assert_eq!(backup.client_request()?, Verdict::Reject, "the passive one");
    assert_eq!(
        backup.rejected.load(Ordering::Relaxed),
        1,
        "and the refusal is counted, which is the client's cue to try the other address"
    );
    Ok(())
}
/// Claim, Binary Star: **failover happens "reliably when needed, and only
/// when needed"** - a live pair where the primary vanishes. Silence alone
/// leaves the backup passive for as long as nobody asks; the first client
/// request after it promotes it.
#[tokio::test]
async fn a_backup_fails_over_only_when_a_client_asks() -> Result<()> {
    let context = context()?;
    let (primary_pub, primary_endpoint) = bind_state_pub(&context).await?;
    let (backup_pub, backup_endpoint) = bind_state_pub(&context).await?;
    let primary = start_star(
        &context,
        State::Primary,
        primary_pub,
        &backup_endpoint,
        BEAT,
    )?;
    let backup = start_star(&context, State::Backup, backup_pub, &primary_endpoint, BEAT)?;
    until(|| backup.state() == State::Passive).await;

    //  The active server disappears without a word.
    primary.task.abort();
    tokio::time::sleep(BEAT * 6).await;
    assert_eq!(
        backup.state(),
        State::Passive,
        "silence alone does not promote: nobody asked for the service"
    );

    //  Now a client asks, and the silence makes it a vote.
    assert_eq!(backup.client_request()?, Verdict::Serve, "the vote carried");
    assert_eq!(backup.state(), State::Active);
    Ok(())
}
/// Claim, Espresso: **all bridged traffic, control and data, is observable**
/// on the capture socket - the subscriptions, the message, and the
/// unsubscriptions, which is exactly the trace the guide prints.
#[tokio::test]
async fn the_capture_socket_shows_subscriptions_data_and_unsubscriptions() -> Result<()> {
    let context = context()?;
    let mut publisher = PubSocket::new(&context)?;
    let publisher_endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    let mut trace = espresso(&context, &publisher_endpoint).await?;

    let mut subscriber = SubSocket::new(&context)?;
    subscriber.connect(&trace.subscriber_endpoint)?;
    subscriber.subscribe("A")?;
    subscriber.subscribe("B")?;
    let message = Multipart::new(vec![
        Message::from(b"A".to_vec()),
        Message::from(b"hello".to_vec()),
    ])
    .expect("two frames");
    //  Published until it crosses, which is the slow joiner and not a fault.
    weida_testkit::until("the subscription to cross", PATIENCE, || {
        publisher.publish(message.clone()).delivered != 0
    })
    .await;
    weida_testkit::within("the published message", PATIENCE, subscriber.recv()).await?;
    subscriber.unsubscribe("A")?;
    subscriber.unsubscribe("B")?;

    //  Wait for all five, however long that takes, then keep a short window
    //  open only to see that nothing more follows.
    let mut captured = Vec::new();
    while captured.len() < 5 {
        let message =
            weida_testkit::within("a captured message", PATIENCE, trace.capture.recv()).await?;
        captured.push(render(&message));
    }
    while let Ok(message) = trace.capture.recv_timeout(QUIET).await {
        captured.push(render(&message));
    }
    captured.sort();
    assert_eq!(
        captured,
        vec!["00A", "00B", "01A", "01B", "A hello"],
        "control frames both ways and the data, all of it"
    );
    Ok(())
}
/// Claim, Last Value Caching: **immediate catch-up to the cached last value
/// per subscribed topic, not the full stream.** Three things at once: the
/// late subscriber gets a message with nothing published after it arrived, it
/// is the *last* value for that topic rather than the first, and it is only
/// the topic it subscribed to.
#[tokio::test]
async fn a_late_subscriber_catches_up_to_the_last_value_and_no_more() -> Result<()> {
    let context = context()?;
    let mut publisher = PubSocket::new(&context)?;
    let publisher_endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    let cache = last_value_cache(&context, &publisher_endpoint).await?;

    //  Somebody has to be subscribed for anything to cross the cache: a
    //  publisher with no subscriber downstream delivers nothing at all.
    let mut early = SubSocket::new(&context)?;
    early.connect(&cache.subscriber_endpoint)?;
    early.subscribe("A")?;
    early.subscribe("B")?;
    for (topic, body) in [("A", "one"), ("B", "one"), ("A", "two")] {
        let message = Multipart::new(vec![
            Message::from(topic.as_bytes().to_vec()),
            Message::from(body.as_bytes().to_vec()),
        ])
        .expect("two frames");
        weida_testkit::until("the subscription to cross", PATIENCE, || {
            publisher.publish(message.clone()).delivered != 0
        })
        .await;
        weida_testkit::within("the published message", PATIENCE, early.recv()).await?;
    }

    //  A new subscriber, and **nothing published after it**: without the
    //  cache it would wait for the next update, which is the 500 seconds the
    //  guide complains about.
    let mut late = SubSocket::new(&context)?;
    late.connect(&cache.subscriber_endpoint)?;
    late.subscribe("A")?;
    let caught_up = weida_testkit::within("the cached value", PATIENCE, late.recv()).await?;
    assert_eq!(
        (
            caught_up.frames()[0].as_slice(),
            caught_up.frames()[1].as_slice()
        ),
        (b"A".as_slice(), b"two".as_slice()),
        "the last value for A, not the first"
    );
    //  "not the full stream": one message, and nothing for a topic this
    //  subscriber never asked about.
    assert!(
        late.recv_timeout(QUIET).await.is_err(),
        "the cache replayed one message per subscribed topic and stopped"
    );
    assert_eq!(
        cache.served_from_cache.load(Ordering::Relaxed),
        1,
        "one cached topic matched the subscription"
    );
    Ok(())
}
