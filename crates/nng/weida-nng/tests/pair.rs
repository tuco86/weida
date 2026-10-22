//! PAIR over `inproc://`: the one-peer rule, with nothing but the engine
//! between the sockets.

use std::time::Duration;

use weida_nng::{Context, ContextConfig, Pair0Socket};

use crate::local_transports::options;

/// Waits, on the paused clock, until `socket` holds `pipes` pipes.
async fn until_pipes(socket: &Pair0Socket, pipes: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while socket.pipe_count() != pipes {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} pipes, not {pipes}", socket.pipe_count()));
}

/// Claim: a paired socket refuses a second peer while one is live, keeps
/// the first one working, and takes a peer again once the first has gone
/// (§4).
///
/// The refusal is a close after both protocol headers have crossed (§6),
/// so the intruder's dial may report success for the moment before the
/// close arrives, and then redials into the same refusal; what it never
/// gets is the pair.
#[tokio::test(start_paused = true)]
async fn a_paired_socket_admits_one_peer_at_a_time() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    server.listen("inproc://pair").await.expect("listen");

    let first = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    first.dial("inproc://pair").await.expect("the first peer");
    until_pipes(&server, 1).await;

    let intruder = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    let _ = intruder.dial("inproc://pair").await;
    // Long enough for several refusals and redials.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.pipe_count(), 1, "the first pairing is untouched");

    // The pair still works, which is the point of refusing the intruder.
    first.send(b"still here".to_vec()).await.expect("send");
    assert_eq!(server.recv().await.expect("recv").body(), b"still here");

    // Once the first peer goes, the slot is free again.
    intruder.close();
    first.close();
    until_pipes(&server, 0).await;
    let third = Pair0Socket::with_options(&ctx, options()).expect("pair0");
    third.dial("inproc://pair").await.expect("dial");
    until_pipes(&server, 1).await;
    third.send(b"third".to_vec()).await.expect("send");
    assert_eq!(server.recv().await.expect("recv").body(), b"third");
}
