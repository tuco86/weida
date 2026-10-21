//! The zguide's chapter 4 service recipes - Majordomo (18/MDP 0.2) and
//! Freelance (10/FLP) - driven and **asserted against the RFCs' own stated
//! guarantees**, 0013 §4.7 clause 5.
//!
//! The examples are included as modules, so what is under test is the file a
//! reader runs (`cargo run --example majordomo`, `--example freelance`) and
//! not a second copy written to be testable.
//!
//! The claims, each named on its test:
//!
//! * MDP: "route by abstract service name" - a request carrying a service
//!   name reaches a worker that registered for it, and the reply comes back
//!   through the broker.
//! * MDP: "one request queue and one worker queue per service" plus "a least
//!   recently used pattern for task distribution" - two workers on one
//!   service share the work.
//! * MDP: `mmi.service` "answers 200 if workers are registered and 404
//!   otherwise" - asked through the same broker for a service that exists
//!   and one that does not.
//! * MDP: on `DISCONNECT` a worker "MUST close its socket and reconnect on a
//!   new one", the mechanism that "allows workers to re-register after a
//!   broker failure and recovery" - and a worker that keeps beating is never
//!   disconnected at all.
//! * FLP: a client "SHALL discard replies that do not carry the sequence
//!   number of the pending request", and model one "tries each endpoint in
//!   turn".
//!
//! The intervals are milliseconds here and the guide's seconds in `main`, so
//! that an eviction and a re-registration can be asserted without a test
//! that takes ten seconds.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use weida_zmq::{Context, ContextConfig, Result};

#[path = "../examples/freelance.rs"]
#[allow(dead_code)]
mod freelance;
#[path = "../examples/majordomo.rs"]
#[allow(dead_code)]
mod majordomo;

use freelance::{FreelanceClient, freelance_model_one, freelance_server};
use majordomo::{majordomo_broker, majordomo_request, majordomo_worker};

/// Fast enough that a lifetime is 150 ms, slow enough that a loaded machine
/// is not a failure.
const BEAT: Duration = Duration::from_millis(50);
/// The guide's `HEARTBEAT_LIVENESS`.
const LIVENESS: u32 = 3;
/// A reply this library cannot produce in this long is a bug, not a slow
/// machine.
const PATIENCE: Duration = Duration::from_secs(5);

fn context() -> Result<Context> {
    Context::new(ContextConfig::default())
}

/// Claim, 18/MDP: **a request addressed to an abstract service name reaches a
/// worker registered for that name.** The client never learns of the worker
/// and the worker never learns of the client; the service string is the whole
/// address.
#[tokio::test]
async fn a_request_reaches_the_worker_that_registered_the_service() -> Result<()> {
    let context = context()?;
    let broker = majordomo_broker(&context, BEAT, LIVENESS).await?;
    let echo = majordomo_worker(&context, &broker.endpoint, "echo", BEAT)?;
    //  A second service, so that "the name routed it" is not indistinguishable
    //  from "there was only one worker".
    let other = majordomo_worker(&context, &broker.endpoint, "reverse", BEAT)?;
    until(|| broker.ready_seen.load(Ordering::Relaxed) >= 2).await;

    let reply = majordomo_request(
        &context,
        &broker.endpoint,
        "echo",
        "Hello world",
        PATIENCE,
        3,
    )
    .await?;
    assert_eq!(
        reply.as_deref(),
        Some("Hello world"),
        "the service answered"
    );
    assert_eq!(echo.served.load(Ordering::Relaxed), 1, "the echo worker");
    assert_eq!(
        other.served.load(Ordering::Relaxed),
        0,
        "the worker of another service saw nothing"
    );
    Ok(())
}

/// Claim, 18/MDP: **the broker keeps one worker queue per service and hands
/// work out least recently used first.** Two workers, two requests, one each -
/// the second request cannot go to the worker that just answered, because
/// that one went to the back of the queue.
#[tokio::test]
async fn two_workers_of_one_service_share_the_work_least_recently_used() -> Result<()> {
    let context = context()?;
    let broker = majordomo_broker(&context, BEAT, LIVENESS).await?;
    let first = majordomo_worker(&context, &broker.endpoint, "echo", BEAT)?;
    until(|| broker.ready_seen.load(Ordering::Relaxed) >= 1).await;
    let second = majordomo_worker(&context, &broker.endpoint, "echo", BEAT)?;
    until(|| broker.ready_seen.load(Ordering::Relaxed) >= 2).await;

    for request in ["one", "two"] {
        let reply =
            majordomo_request(&context, &broker.endpoint, "echo", request, PATIENCE, 3).await?;
        assert_eq!(reply.as_deref(), Some(request), "each request answered");
    }
    assert_eq!(
        (
            first.served.load(Ordering::Relaxed),
            second.served.load(Ordering::Relaxed)
        ),
        (1, 1),
        "one request each, not two to the same worker"
    );
    Ok(())
}

/// Claim, 18/MDP MMI: **`mmi.service` answers 200 for a service with
/// registered workers and 404 for one without**, through the same broker and
/// the same client code as an ordinary request - MMI rides on MDP without
/// changing it.
#[tokio::test]
async fn mmi_service_answers_two_hundred_for_a_live_service_and_four_oh_four_otherwise()
-> Result<()> {
    let context = context()?;
    let broker = majordomo_broker(&context, BEAT, LIVENESS).await?;
    let _worker = majordomo_worker(&context, &broker.endpoint, "echo", BEAT)?;
    until(|| broker.ready_seen.load(Ordering::Relaxed) >= 1).await;

    let live = majordomo_request(
        &context,
        &broker.endpoint,
        "mmi.service",
        "echo",
        PATIENCE,
        3,
    )
    .await?;
    let absent = majordomo_request(
        &context,
        &broker.endpoint,
        "mmi.service",
        "nosuchservice",
        PATIENCE,
        3,
    )
    .await?;
    assert_eq!(live.as_deref(), Some("200"), "a registered service");
    assert_eq!(absent.as_deref(), Some("404"), "an unknown service");
    Ok(())
}

/// Claim, 18/MDP: **a worker that stops speaking is sent `DISCONNECT`, and on
/// `DISCONNECT` it closes its socket and reconnects on a new one** - the
/// mechanism the RFC keeps for broker restarts. Here the worker's heartbeat
/// interval is longer than the broker's patience, so it falls silent without
/// dying; the evidence is a `DISCONNECT` sent, a re-registration, and a
/// request served *after* the reconnect.
#[tokio::test]
async fn a_silent_worker_is_disconnected_and_registers_again() -> Result<()> {
    let context = context()?;
    let broker = majordomo_broker(&context, BEAT, LIVENESS).await?;
    //  A worker that will not beat within the broker's lifetime.
    let worker = majordomo_worker(&context, &broker.endpoint, "echo", Duration::from_secs(30))?;
    until(|| broker.ready_seen.load(Ordering::Relaxed) >= 1).await;

    until(|| broker.disconnects_sent.load(Ordering::Relaxed) >= 1).await;
    until(|| worker.disconnected.load(Ordering::Relaxed) >= 1).await;
    until(|| worker.registrations.load(Ordering::Relaxed) >= 2).await;
    //  A fresh socket, a fresh registration - and still able to work, which
    //  is the point of re-registering rather than exiting.
    let reply = majordomo_request(&context, &broker.endpoint, "echo", "after", PATIENCE, 5).await?;
    assert_eq!(reply.as_deref(), Some("after"), "served after reconnecting");
    Ok(())
}

/// Claim, 18/MDP: **"any received command except `DISCONNECT` acts as a
/// heartbeat"** - a worker that beats faster than the broker's liveness
/// window is never disconnected, however long nothing happens.
#[tokio::test]
async fn a_beating_worker_is_never_disconnected() -> Result<()> {
    let context = context()?;
    let broker = majordomo_broker(&context, BEAT, LIVENESS).await?;
    let worker = majordomo_worker(&context, &broker.endpoint, "echo", BEAT)?;
    until(|| broker.ready_seen.load(Ordering::Relaxed) >= 1).await;

    //  Four lifetimes with no work at all.
    tokio::time::sleep(BEAT * LIVENESS * 4).await;
    assert_eq!(
        broker.disconnects_sent.load(Ordering::Relaxed),
        0,
        "the beats kept it registered"
    );
    assert_eq!(
        worker.registrations.load(Ordering::Relaxed),
        1,
        "it never had to register twice"
    );
    let reply = majordomo_request(&context, &broker.endpoint, "echo", "still", PATIENCE, 3).await?;
    assert_eq!(reply.as_deref(), Some("still"), "and it still works");
    Ok(())
}

/// Claim, 10/FLP model one: **the client tries each endpoint in turn until
/// one answers.** A dead endpoint in front of a live one costs one timeout
/// and the answer still arrives - and `attempts` proves the dead one was
/// really tried rather than skipped.
#[tokio::test]
async fn freelance_model_one_walks_past_a_dead_endpoint() -> Result<()> {
    let context = context()?;
    let live = freelance_server(&context, "live", None).await?;
    let endpoints = vec!["tcp://127.0.0.1:1".to_owned(), live.endpoint.clone()];

    let reply = freelance_model_one(&context, &endpoints, "name", Duration::from_millis(300))
        .await?
        .expect("the live server answered");
    assert_eq!(reply.body, "name live", "the live server's echo");
    assert_eq!(reply.attempts, 2, "the dead endpoint was tried first");

    //  And the model's failure mode: no server in the list, no answer.
    let nothing = freelance_model_one(
        &context,
        &["tcp://127.0.0.1:1".to_owned()],
        "name",
        Duration::from_millis(200),
    )
    .await?;
    assert!(nothing.is_none(), "it gives up rather than blocking");
    Ok(())
}

/// Claim, 10/FLP model two: **a client SHALL discard replies that do not
/// carry the sequence number of the pending request.** Both halves: a
/// request the shotgun answers normally, and then a server slow enough that
/// its answer to request one arrives while request two is pending - the
/// stale reply is thrown away and the wait for the right one continues.
#[tokio::test]
async fn freelance_model_two_discards_a_stale_reply() -> Result<()> {
    let context = context()?;
    let quick = freelance_server(&context, "quick", None).await?;
    let mut client = FreelanceClient::new(&context, std::slice::from_ref(&quick.endpoint))?;
    let answered = client
        .request("one", PATIENCE)
        .await?
        .expect("the quick server answered");
    assert_eq!(answered.body, "one quick", "the ordinary case first");
    assert_eq!(answered.stale_discarded, 0, "nothing stale here");

    //  A server that takes longer over every request than the client is
    //  willing to wait for the first one. Its answers are therefore always
    //  one request behind, which is the situation the sequence number is for.
    let slow = freelance_server(
        &context,
        "slow",
        Some((Duration::from_millis(250), usize::MAX)),
    )
    .await?;
    let mut client = FreelanceClient::new(&context, std::slice::from_ref(&slow.endpoint))?;
    let abandoned = client.request("one", Duration::from_millis(100)).await?;
    assert!(abandoned.is_none(), "request one timed out, as arranged");

    //  Request two: the reply to request one arrives first and is not it.
    let second = client
        .request("two", PATIENCE)
        .await?
        .expect("request two was answered");
    assert_eq!(
        second.body, "two slow",
        "the reply carries request two's body, not request one's"
    );
    assert_eq!(
        second.stale_discarded, 1,
        "the sequence-one reply was discarded, not returned"
    );
    Ok(())
}

/// Waits for a condition, failing the test rather than hanging the suite.
async fn until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !ready() {
        assert!(Instant::now() < deadline, "the condition never held");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
