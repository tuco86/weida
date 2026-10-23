//! PUSH and PULL over `inproc://`, where nothing but the pipe's own buffer
//! sits between them, so what the engine decides is all a test sees.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use weida_nng::{Context, ContextConfig, PullSocket, PushSocket, SocketOptions};

use crate::local_transports::options;

/// Counts what `socket` receives into `into`, until the socket closes.
fn count(socket: &PullSocket, into: &Arc<AtomicUsize>) -> tokio::task::JoinHandle<()> {
    let socket = socket.clone();
    let into = Arc::clone(into);
    tokio::spawn(async move {
        while socket.recv().await.is_ok() {
            into.fetch_add(1, Ordering::SeqCst);
        }
    })
}

/// Claim: a PUSH whose puller cannot take a message skips it and hands the
/// work to the one that can, rather than waiting for the stalled one —
/// "unavailable peers are excluded by flow control" (§4) — and loses
/// nothing on the way.
///
/// Over `inproc://` the stalled puller can absorb only its queue of one,
/// the message its session holds for that queue, what the 64 KiB pipe
/// buffers (two 32 KiB jobs, the second in part) and the one its writer is
/// in the middle of: five at most (four measured), where a kernel's socket
/// buffers absorb a platform's guess — on darwin more than half of the work.
#[tokio::test(start_paused = true)]
async fn a_puller_that_cannot_accept_is_skipped_rather_than_waited_for() {
    const JOBS: usize = 200;
    const STALLED_ABSORBS: usize = 5;

    let ctx = Context::new(ContextConfig::default()).expect("context");
    // The send timeout is the oracle of this test and runs on the paused
    // clock, so a slow machine cannot trigger it: only a send that would wait
    // for ever does.
    let push = PushSocket::with_options(
        &ctx,
        SocketOptions {
            send_timeout: Some(Duration::from_secs(5)),
            ..options()
        },
    )
    .expect("push");
    push.listen("inproc://work").await.expect("listen");
    let stalled = PullSocket::with_options(
        &ctx,
        SocketOptions {
            recv_depth: Some(1),
            ..options()
        },
    )
    .expect("pull");
    let working = PullSocket::with_options(&ctx, options()).expect("pull");
    stalled.dial("inproc://work").await.expect("dial");
    working.dial("inproc://work").await.expect("dial");
    while push.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let worked = Arc::new(AtomicUsize::new(0));
    let reader = count(&working, &worked);

    // A send that waited for the stalled puller would wait for ever, and the
    // paused clock turns that into `send_timeout` at once.
    let job = vec![0xABu8; 32 * 1024];
    for _ in 0..JOBS {
        push.send(job.clone())
            .await
            .expect("a send waited although a worker was free");
    }

    // Every send has been handed to a pipe; the stalled puller now reads
    // what it absorbed, and the two together account for every job.
    let absorbed = Arc::new(AtomicUsize::new(0));
    let late = count(&stalled, &absorbed);
    let settled = tokio::time::timeout(Duration::from_secs(10), async {
        while worked.load(Ordering::SeqCst) + absorbed.load(Ordering::SeqCst) < JOBS {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let (worked, absorbed) = (
        worked.load(Ordering::SeqCst),
        absorbed.load(Ordering::SeqCst),
    );
    assert!(
        settled.is_ok(),
        "{worked} + {absorbed} of {JOBS} jobs arrived"
    );
    assert!(
        absorbed <= STALLED_ABSORBS,
        "the stalled puller took {absorbed} jobs; it was not skipped"
    );
    reader.abort();
    late.abort();
}
