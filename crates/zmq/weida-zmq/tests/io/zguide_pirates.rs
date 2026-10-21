#![cfg(feature = "blocking")]
//! The zguide's chapter 4 pirates, driven and **asserted against the guide's
//! own claims** — 0013 §4.7 clause 5: "the canonical patterns run unchanged
//! in spirit, each asserting the guarantee the guide claims".
//!
//! Each test drives the example file itself, included as a module, so what is
//! asserted is the recipe a reader runs and not a second copy of it written
//! to be testable:
//!
//! * `examples/lazy_pirate.rs` — "the client gets an in-order reply or
//!   abandons, never blocking indefinitely".
//! * `examples/simple_pirate.rs` — "workers may crash and restart repeatedly
//!   while the queue runs; client retries recover from a dead worker".
//! * `examples/paranoid_pirate.rs` — "the queue evicts lost workers instead
//!   of discovering them through a failed request".
//!
//! The file needs the `blocking` feature because two of the three recipes are
//! synchronous in their C originals and use the facade:
//!
//! ```text
//! cargo test -p weida-zmq --features blocking --test zguide_pirates
//! ```
//!
//! The intervals here are milliseconds where the guide's are seconds. That is
//! the only liberty: `HEARTBEAT_INTERVAL` is 1000 ms and `REQUEST_TIMEOUT`
//! 2500 ms in the C, and both are arguments in the examples precisely so that
//! a claim about a timeout can be asserted without a test that takes a
//! minute.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use weida_zmq::blocking::BlockingContext;
use weida_zmq::{Context, ContextConfig};
#[path = "../examples/paranoid_pirate.rs"]
#[allow(dead_code)]
mod paranoid_pirate;
#[path = "../examples/simple_pirate.rs"]
#[allow(dead_code)]
mod simple_pirate;

// Lazy Pirate is reached **through** Simple Pirate, because that is how the
// recipe is composed: `examples/simple_pirate.rs` includes the client file
// unchanged and this test uses the same module rather than a second copy of
// it.
use simple_pirate::lazy_pirate;
use simple_pirate::lazy_pirate::Outcome;

/// Claim, Lazy Pirate: **an in-order reply or abandonment, never an
/// indefinite block.** Both halves, on the same code path: a server that
/// crashes once still answers the retry, and a server that is not there at
/// all is abandoned after `REQUEST_RETRIES` attempts.
#[test]
fn lazy_pirate_gets_an_in_order_reply_or_abandons() {
    let context = BlockingContext::new().expect("context");
    let server = lazy_pirate::lazy_pirate_server(&context, 1).expect("server");

    let outcome = lazy_pirate::lazy_pirate_request(
        &context,
        &server.endpoint,
        7,
        Duration::from_millis(300),
        3,
    )
    .expect("the exchange");
    match outcome {
        Outcome::Reply {
            body,
            attempts,
            out_of_sequence,
        } => {
            // In order: the reply carries the request's own sequence number,
            // which is what the C checks with `atoi (reply) == sequence`.
            assert_eq!(body, "7");
            assert!(attempts >= 2, "the crash cost at least one attempt");
            assert_eq!(out_of_sequence, 0, "nothing out of sequence was kept");
        }
        Outcome::Abandoned { attempts } => {
            panic!("abandoned after {attempts} attempts although the server came back")
        }
    }

    // The other half: nobody is listening on port 1, and the client must
    // come back rather than hang. Bounded by construction — three attempts
    // of 150 ms — and asserted as elapsed time, because "never blocking
    // indefinitely" is a claim about the clock.
    let started = Instant::now();
    let outcome = lazy_pirate::lazy_pirate_request(
        &context,
        "tcp://127.0.0.1:1",
        8,
        Duration::from_millis(150),
        3,
    )
    .expect("the exchange");
    assert_eq!(outcome, Outcome::Abandoned { attempts: 3 });
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the client blocked for {:?}",
        started.elapsed()
    );
}

/// Claim, Simple Pirate: **a worker may crash and restart while the queue
/// runs, and the client's retry recovers.** The queue is never restarted and
/// never told what happened; the only thing that changes is which worker is
/// alive.
#[tokio::test]
async fn a_worker_may_crash_and_restart_while_the_queue_runs() {
    let context = Context::new(ContextConfig::default()).expect("context");
    let queue = simple_pirate::load_balancing_broker(&context)
        .await
        .expect("the queue");
    let blocking = BlockingContext::new().expect("blocking context");

    // A worker that dies on its first request.
    let _doomed =
        simple_pirate::simple_pirate_worker(&blocking, &queue.backend, Some(0)).expect("worker");
    wait_for(|| queue.ready_seen.load(Ordering::Relaxed) >= 1).await;

    // The replacement, started while the queue keeps running.
    let _healthy =
        simple_pirate::simple_pirate_worker(&blocking, &queue.backend, None).expect("worker");
    wait_for(|| queue.ready_seen.load(Ordering::Relaxed) >= 2).await;

    let frontend = queue.frontend.clone();
    let first = {
        let blocking = blocking.clone();
        let frontend = frontend.clone();
        tokio::task::spawn_blocking(move || {
            lazy_pirate::lazy_pirate_request(&blocking, &frontend, 1, Duration::from_millis(300), 5)
        })
        .await
        .expect("the client thread")
        .expect("the exchange")
    };
    let Outcome::Reply { body, attempts, .. } = first else {
        panic!("the client abandoned although a worker was alive: {first:?}");
    };
    assert_eq!(body, "1");
    assert!(
        attempts >= 2,
        "the crash was never exercised: {attempts} attempt(s)"
    );

    // And the queue is still the same queue: the next request needs no
    // retry at all.
    let second = tokio::task::spawn_blocking(move || {
        lazy_pirate::lazy_pirate_request(&blocking, &frontend, 2, Duration::from_millis(300), 5)
    })
    .await
    .expect("the client thread")
    .expect("the exchange");
    assert_eq!(
        second,
        Outcome::Reply {
            body: "2".to_owned(),
            attempts: 1,
            out_of_sequence: 0
        },
        "the queue survived the crash"
    );
    assert!(queue.routed.load(Ordering::Relaxed) >= 2);
}

/// Claim, Paranoid Pirate: **the queue evicts a lost worker instead of
/// discovering it through a failed request.**
///
/// The test sends no request at all. A worker registers, then stops beating
/// without a word; after `HEARTBEAT_LIVENESS` intervals the queue has taken
/// it off its list and counted the eviction, which is the difference from
/// Simple Pirate, whose queue "does not notice a worker that dies while idle
/// until work is sent to it".
#[tokio::test]
async fn the_queue_evicts_a_lost_worker_without_sending_it_work() {
    let interval = Duration::from_millis(80);
    let liveness = 3;
    let context = Context::new(ContextConfig::default()).expect("context");
    let queue = paranoid_pirate::paranoid_pirate_queue(&context, interval, liveness)
        .await
        .expect("the queue");

    let worker =
        paranoid_pirate::paranoid_pirate_worker(&context, &queue.backend, interval, liveness)
            .expect("worker");
    wait_for(|| queue.live_workers.load(Ordering::Relaxed) == 1).await;
    assert_eq!(queue.evicted.load(Ordering::Relaxed), 0);

    // The worker vanishes: no DISCONNECT, no error, no request to fail on.
    worker.abort();
    wait_for(|| queue.evicted.load(Ordering::Relaxed) >= 1).await;
    assert_eq!(
        queue.live_workers.load(Ordering::Relaxed),
        0,
        "the evicted worker is off the list"
    );
}

/// Polls `ready` until it holds, with a bound that makes a hang a failure.
async fn wait_for(ready: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "the condition never held");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
