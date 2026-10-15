//! How QUIC stream mechanics show through the weida API.
//!
//! Every test here defends one statement about flow control, stream budgets,
//! cancellation or connection liveness that an application can rely on — or
//! must not. The setups use deliberately tiny `Limits` so the phenomena appear
//! within tens of kilobytes and milliseconds instead of megabytes and seconds.
//!
//! Two conventions run through the file:
//!
//! * a stall is asserted as the *absence* of a progress marker on a channel
//!   within [`STALL`], never as elapsed wall-clock time;
//! * a spawned sender reports its progress with [`Stage`] markers, so a task
//!   that fails instead of blocking closes the channel and fails the assertion
//!   rather than looking like a stall.

use crate::common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::{Certs, Server};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::time::timeout;
use weida::{Binding, Error, Limits, Listener, LossCause, Runtime, RuntimeConfig, TransferMeta};

/// Generous ceiling: everything that must settle settles in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

/// Window used to assert that a future does *not* resolve. Long enough that a
/// loopback round trip and a Tokio wakeup have happened many times over, short
/// enough to keep the suite fast.
const STALL: Duration = Duration::from_millis(300);

async fn within<F: Future>(f: F) -> F::Output {
    timeout(DEADLINE, f).await.expect("operation timed out")
}

/// Progress markers reported by a spawned sender.
///
/// Which marker is missing identifies the call that blocked, which is the point
/// of several probes below: QUIC backpressure lands on `open`, on `write_all`
/// or on the receipt, and the three are not interchangeable.
#[derive(Debug, PartialEq, Eq)]
enum Stage {
    Opened,
    Wrote,
    Delivered,
}

/// A server built by hand, so several servers can share one identity and one
/// can carry a non-default [`RuntimeConfig`].
///
/// `common::Server` generates a fresh certificate per server and only takes
/// `Limits`; a restart probe needs the *same* certificate on a new port, and
/// an idle-timeout probe needs a server-side `idle_timeout`.
struct ManualServer {
    _runtime: Runtime,
    listener: Listener,
    binding: Binding,
    addr: SocketAddr,
}

impl ManualServer {
    async fn start(certs: &Certs, config: RuntimeConfig) -> ManualServer {
        let runtime = Runtime::new(config).expect("runtime");
        let listener = runtime.listener();
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let addr = binding.local_addr();
        ManualServer {
            _runtime: runtime,
            listener,
            binding,
            addr,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }
}

// --- 1. finish() is a commitment ------------------------------------------

/// Claim: `finish()` hands the payload to QUIC for good. Once the FIN is
/// queued, no local handle is needed for the bytes to arrive — the transfer,
/// its `Delivery` and even the `Pusher` may all be gone.
///
/// The setup proves it by dropping every one of them inside a scope that ends
/// *before* the puller calls `recv` for the first time. Only the runtime's
/// pooled connection survives, and the whole 64 KiB still arrives byte for
/// byte.
#[tokio::test]
async fn a_finished_transfer_needs_no_local_handle_to_arrive() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();

    let payload = vec![0xa5u8; 64 * 1024];
    {
        let pusher = client.pusher(server.trust());
        within(pusher.connect(&server.url("/jobs")))
            .await
            .expect("connect");
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open");
        within(transfer.write_all(&payload)).await.expect("write");
        // `finish` consumes the transfer; dropping the receipt is the
        // fire-and-forget path. The pusher goes out of scope with them.
        drop(transfer.finish().expect("finish"));
    }

    let inbound = within(puller.recv()).await.expect("recv");
    let body = within(inbound.collect(128 * 1024)).await.expect("collect");
    assert_eq!(body.len(), payload.len());
    assert_eq!(body, payload);

    client.shutdown().await;
}

// --- 2. the receipt and the flow-control window ---------------------------

/// Claim: a delivery receipt for a payload larger than the peer's stream
/// receive window cannot resolve until the peer's *application* has consumed at
/// least `payload - window` bytes. Beyond one window the receipt stops being a
/// pure transport signal and starts implying application progress.
///
/// The setup pushes two and a half 64 KiB windows and reports the sender's
/// progress on a channel: nothing settles while the puller reads nothing, and
/// once it reads, the byte count at which the receipt appears is recorded and
/// checked against the window bound. `pushpull::push_delivery_receipt` covers
/// the converse (a payload inside the window resolves before any `recv`).
#[tokio::test]
async fn a_receipt_beyond_the_window_implies_the_reader_consumed() {
    const WINDOW: usize = 64 * 1024;
    const TOTAL: usize = WINDOW * 5 / 2;
    const CHUNK: usize = 8 * 1024;

    let server = Server::start_with(Limits {
        stream_receive_window: WINDOW as u64,
        connection_receive_window: 1024 * 1024,
        ..Limits::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();
    let pusher = Arc::new(client.pusher(server.trust()));
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let sender = {
        let pusher = Arc::clone(&pusher);
        tokio::spawn(async move {
            let mut transfer = pusher.open(TransferMeta::default()).await.expect("open");
            let _ = tx.send(Stage::Opened);
            transfer
                .write_all(&vec![0x7eu8; TOTAL])
                .await
                .expect("write");
            let _ = tx.send(Stage::Wrote);
            transfer
                .finish()
                .expect("finish")
                .delivered()
                .await
                .expect("delivered");
            let _ = tx.send(Stage::Delivered);
        })
    };

    // The stream is addressed and open, so `open` reports at once...
    assert_eq!(within(rx.recv()).await, Some(Stage::Opened));
    // ...but the payload is two and a half windows: with nothing read, the
    // write cannot finish and the receipt cannot exist.
    assert!(
        timeout(STALL, rx.recv()).await.is_err(),
        "a payload past the window must stall while the reader reads nothing"
    );

    let mut inbound = within(puller.recv()).await.expect("recv");
    let mut chunk = vec![0u8; CHUNK];
    let mut consumed = 0usize;
    let mut wrote_at = None;
    let mut delivered_at = None;
    while consumed < TOTAL {
        within(inbound.read_exact(&mut chunk)).await.expect("read");
        consumed += CHUNK;
        while let Ok(stage) = rx.try_recv() {
            match stage {
                Stage::Wrote => wrote_at = Some(consumed),
                Stage::Delivered => delivered_at = Some(consumed),
                Stage::Opened => unreachable!("already observed"),
            }
        }
    }
    // The last markers may still be in flight once the body ends.
    while delivered_at.is_none() {
        match within(rx.recv()).await.expect("sender reported") {
            Stage::Wrote => wrote_at = Some(consumed),
            Stage::Delivered => delivered_at = Some(consumed),
            Stage::Opened => unreachable!("already observed"),
        }
    }

    let wrote_at = wrote_at.expect("the write completed");
    let delivered_at = delivered_at.expect("the receipt resolved");
    // Flow control accounts header plus payload, and the server consumed the
    // header before queueing the transfer, so the sender may be at most one
    // window ahead of the reader. Both the write and the receipt therefore
    // require the reader to have taken `TOTAL - WINDOW` bytes.
    assert!(
        wrote_at >= TOTAL - WINDOW,
        "the write completed after {wrote_at} bytes were consumed, less than the {} the window allows",
        TOTAL - WINDOW
    );
    assert!(
        delivered_at >= TOTAL - WINDOW,
        "the receipt resolved after {delivered_at} bytes were consumed, less than the {} the window allows",
        TOTAL - WINDOW
    );
    // Observed on loopback: the write returns at 128 KiB consumed, the receipt
    // at 160 KiB — both comfortably past the 96 KiB the window forces.

    within(sender).await.expect("sender task");
    client.shutdown().await;
}

/// Claim: the stream receive window is not a payload budget. The DATA header
/// shares it with the payload, and quinn only raises the window once the
/// reader has consumed an eighth of it, so a payload of *exactly*
/// `stream_receive_window` bytes cannot be written until the application
/// starts reading — however small the shortfall is.
///
/// The setup writes exactly one 64 KiB window. The write stalls even though
/// the server has already parsed the header, because parsing a few dozen
/// bytes is far below the eighth-of-a-window update threshold. Reading one
/// eighth releases it. This is the boundary case behind the test above, and
/// the reason payload sizing must leave the header room.
#[tokio::test]
async fn a_payload_the_size_of_the_stream_window_waits_for_the_reader() {
    const WINDOW: usize = 64 * 1024;

    let server = Server::start_with(Limits {
        stream_receive_window: WINDOW as u64,
        connection_receive_window: 1024 * 1024,
        ..Limits::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();
    let pusher = Arc::new(client.pusher(server.trust()));
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let sender = {
        let pusher = Arc::clone(&pusher);
        tokio::spawn(async move {
            let mut transfer = pusher.open(TransferMeta::default()).await.expect("open");
            let _ = tx.send(Stage::Opened);
            transfer
                .write_all(&vec![0x11u8; WINDOW])
                .await
                .expect("write");
            let _ = tx.send(Stage::Wrote);
        })
    };
    assert_eq!(within(rx.recv()).await, Some(Stage::Opened));
    assert!(
        timeout(STALL, rx.recv()).await.is_err(),
        "the header shares the window, so a full-window payload cannot complete unread"
    );

    let mut inbound = within(puller.recv()).await.expect("recv");
    let mut eighth = vec![0u8; WINDOW / 8];
    within(inbound.read_exact(&mut eighth)).await.expect("read");
    assert_eq!(
        within(rx.recv()).await,
        Some(Stage::Wrote),
        "one eighth of the window is the update threshold, so the write resumes"
    );

    within(sender).await.expect("sender task");
    client.shutdown().await;
}

// --- 3. per-stream isolation, shared connection window --------------------

/// Claim: per-stream flow control isolates streams — one unread stream does not
/// delay its siblings — while the *connection* window is a shared resource that
/// one slow reader can exhaust for everyone.
///
/// The setup parks stream A with 32 KiB unread inside a 64 KiB stream window,
/// so nothing about A is blocked by its own window, then pushes two small
/// messages that arrive normally. It then opens further unread 32 KiB streams
/// until a write stalls, which must happen no later than the 256 KiB
/// connection window being spent (8 x 32 KiB). Reading A's bytes releases
/// connection credit and the stalled write completes.
#[tokio::test]
async fn a_stalled_stream_does_not_block_its_siblings() {
    const STREAM_WINDOW: usize = 64 * 1024;
    const PAYLOAD: usize = 32 * 1024;
    const CONN_WINDOW: usize = 256 * 1024;
    const CAPACITY: usize = CONN_WINDOW / PAYLOAD;

    let server = Server::start_with(Limits {
        stream_receive_window: STREAM_WINDOW as u64,
        connection_receive_window: CONN_WINDOW as u64,
        ..Limits::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();
    let pusher = Arc::new(client.pusher(server.trust()));
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // A: half a stream window, never finished, never read.
    let mut a = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open A");
    within(a.write_all(&vec![0xaau8; PAYLOAD]))
        .await
        .expect("write A");
    let mut inbound_a = within(puller.recv()).await.expect("recv A");

    // B and C: fresh streams, unaffected by A sitting unread.
    within(pusher.send(b"B")).await.expect("send B");
    within(pusher.send(b"C")).await.expect("send C");
    let b = within(puller.recv()).await.expect("recv B");
    assert_eq!(within(b.collect(64)).await.expect("collect B"), b"B");
    let c = within(puller.recv()).await.expect("recv C");
    assert_eq!(within(c.collect(64)).await.expect("collect C"), b"C");

    // Now spend the shared connection window with more unread streams. Each
    // writer parks after reporting, so its stream stays open and unfinished.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut writers = Vec::new();
    let mut blocked_at = None;
    for i in 0..CAPACITY + 4 {
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open filler");
        let tx = tx.clone();
        writers.push(tokio::spawn(async move {
            transfer
                .write_all(&vec![0xbbu8; PAYLOAD])
                .await
                .expect("write filler");
            let _ = tx.send(i);
            std::future::pending::<()>().await;
        }));
        match timeout(STALL, rx.recv()).await {
            Ok(Some(done)) => assert_eq!(done, i, "writers report in order"),
            Ok(None) => unreachable!("a writer holds the sender"),
            Err(_) => {
                blocked_at = Some(i);
                break;
            }
        }
    }

    let blocked_at = blocked_at.expect("the connection window must run out");
    // A plus the fillers that got through must not exceed what the connection
    // window can hold unread.
    let unread = 1 + blocked_at;
    assert!(
        unread <= CAPACITY,
        "{unread} unread {PAYLOAD}-byte streams fit in a {CONN_WINDOW}-byte connection window"
    );
    // Observed on loopback: A plus six fillers, 224 KiB of the 256 KiB window,
    // and the seventh filler stalls part-written.

    // Consuming A's payload releases connection credit — 32 KiB is exactly the
    // eighth of the window at which quinn announces a `MAX_DATA` update — and
    // the stalled write is waiting for precisely that.
    let mut drained = vec![0u8; PAYLOAD];
    within(inbound_a.read_exact(&mut drained))
        .await
        .expect("read A");
    assert!(
        drained.iter().all(|&b| b == 0xaa),
        "a stream parked behind flow control loses nothing"
    );
    assert_eq!(
        within(rx.recv()).await,
        Some(blocked_at),
        "the stalled write must complete once the slow reader consumes"
    );

    client.shutdown().await;
}

/// Claim: **two endpoint paths cannot stall each other**, because they are two
/// connections and therefore two receive windows
/// ([0002](../../../docs/decisions/0002-control-and-bulk-separation.md) §6.2).
/// This is the isolation the previous test cannot give: there, siblings share
/// one connection window and one slow reader exhausts it for everyone.
///
/// The setup exhausts `/slow`'s connection window with unread streams until a
/// write stalls, then sends on `/fast` from the same runtime and the same
/// endpoint handle. Before one connection per path, that send would have been
/// behind the very window the filler just spent.
#[tokio::test]
async fn a_stalled_path_does_not_stall_another_path() {
    const STREAM_WINDOW: usize = 64 * 1024;
    const PAYLOAD: usize = 32 * 1024;
    const CONN_WINDOW: usize = 256 * 1024;
    const CAPACITY: usize = CONN_WINDOW / PAYLOAD;

    let server = Server::start_with(Limits {
        stream_receive_window: STREAM_WINDOW as u64,
        connection_receive_window: CONN_WINDOW as u64,
        ..Limits::default()
    })
    .await;
    let slow = server.listener.puller("/slow").expect("slow puller");
    let fast = server.listener.puller("/fast").expect("fast puller");
    let client = server.client_runtime();

    // One endpoint handle per path, because a handle round-robins over the
    // peers it has: a pusher connected to both would alternate, and this probe
    // needs to fill one path and then send on the other.
    let slow_only = client.pusher(server.trust());
    within(slow_only.connect(&server.url("/slow")))
        .await
        .expect("connect slow");
    let fast_only = client.pusher(server.trust());
    within(fast_only.connect(&server.url("/fast")))
        .await
        .expect("connect fast");

    // Spend `/slow`'s connection window with streams nobody reads, until a
    // write parks.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut writers = Vec::new();
    let mut blocked_at = None;
    for i in 0..CAPACITY + 4 {
        let mut transfer = within(slow_only.open(TransferMeta::default()))
            .await
            .expect("open filler");
        let tx = tx.clone();
        writers.push(tokio::spawn(async move {
            transfer
                .write_all(&vec![0xccu8; PAYLOAD])
                .await
                .expect("write filler");
            let _ = tx.send(i);
            std::future::pending::<()>().await;
        }));
        match timeout(STALL, rx.recv()).await {
            Ok(Some(done)) => assert_eq!(done, i, "writers report in order"),
            Ok(None) => unreachable!("a writer holds the sender"),
            Err(_) => {
                blocked_at = Some(i);
                break;
            }
        }
    }
    let blocked_at = blocked_at.expect("the slow path's connection window must run out");
    assert!(blocked_at <= CAPACITY, "{blocked_at} unread streams");

    // The other path, right now, while `/slow` is parked mid-write.
    within(fast_only.send(b"fast"))
        .await
        .expect("send on /fast");
    let arrived = within(fast.recv()).await.expect("recv on /fast");
    assert_eq!(
        within(arrived.collect(64)).await.expect("collect"),
        b"fast",
        "a path with its own connection is unaffected by another path's stall"
    );

    // And the stall was real: reading one filler releases credit on the slow
    // path's own connection and its parked write completes.
    let mut inbound = within(slow.recv()).await.expect("recv on /slow");
    let mut drained = vec![0u8; PAYLOAD];
    within(inbound.read_exact(&mut drained))
        .await
        .expect("read the slow path");
    assert_eq!(
        within(rx.recv()).await,
        Some(blocked_at),
        "the stalled write completes once its own path's reader consumes"
    );

    client.shutdown().await;
}

// --- 4. the concurrent-stream budget --------------------------------------

/// Claim: exhausting the peer's `max_concurrent_uni_streams` is backpressure,
/// not an error. The sender waits for stream credit inside `open` and no call
/// fails.
///
/// The setup gives the server a budget of two and a puller that reads nothing,
/// so both streams stay open and the third has no credit. The third transfer
/// runs in a task that reports each stage: no stage is reported within
/// [`STALL`], and a failure would close the channel instead. Reading one
/// transfer to EOF closes its stream and the third proceeds.
#[tokio::test]
async fn the_stream_budget_is_backpressure_not_an_error() {
    let server = Server::start_with_config(RuntimeConfig {
        limits: Limits {
            max_concurrent_uni_streams: 2,
            ..Limits::default()
        },
        endpoint_queue: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();
    let pusher = Arc::new(client.pusher(server.trust()));
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    within(pusher.send(b"one")).await.expect("send one");
    within(pusher.send(b"two")).await.expect("send two");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let third = {
        let pusher = Arc::clone(&pusher);
        tokio::spawn(async move {
            let mut transfer = pusher.open(TransferMeta::default()).await.expect("open");
            let _ = tx.send(Stage::Opened);
            transfer.write_all(b"three").await.expect("write");
            let _ = tx.send(Stage::Wrote);
            transfer
                .finish()
                .expect("finish")
                .delivered()
                .await
                .expect("delivered");
            let _ = tx.send(Stage::Delivered);
        })
    };

    // Not even `Opened`: `open_uni` is where the sender waits for credit. A
    // panic or an error in the task would drop the sender and yield
    // `Ok(None)` here, so this really does assert "blocked, not failed".
    assert!(
        timeout(STALL, rx.recv()).await.is_err(),
        "a spent stream budget must block the third transfer, not fail it"
    );

    // Reading a transfer to EOF ends its stream, which returns the credit.
    let first = within(puller.recv()).await.expect("recv one");
    assert_eq!(within(first.collect(64)).await.expect("collect"), b"one");

    assert_eq!(within(rx.recv()).await, Some(Stage::Opened));
    assert_eq!(within(rx.recv()).await, Some(Stage::Wrote));
    assert_eq!(within(rx.recv()).await, Some(Stage::Delivered));
    within(third).await.expect("third task");

    client.shutdown().await;
}

/// Claim: `endpoint_queue` does not raise the stream budget. A transfer parked
/// in the bounded accept queue still owns its QUIC stream, so a queue deeper
/// than `max_concurrent_uni_streams` buys the sender nothing.
///
/// Same shape as the test above with an eight-deep queue and the same budget of
/// two: the third transfer still cannot open.
#[tokio::test]
async fn a_deeper_endpoint_queue_does_not_raise_the_stream_budget() {
    let server = Server::start_with_config(RuntimeConfig {
        limits: Limits {
            max_concurrent_uni_streams: 2,
            ..Limits::default()
        },
        endpoint_queue: 8,
        ..RuntimeConfig::default()
    })
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();
    let pusher = Arc::new(client.pusher(server.trust()));
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    within(pusher.send(b"one")).await.expect("send one");
    within(pusher.send(b"two")).await.expect("send two");

    let (tx, mut rx) = mpsc::unbounded_channel();
    {
        let pusher = Arc::clone(&pusher);
        tokio::spawn(async move {
            let mut transfer = pusher.open(TransferMeta::default()).await.expect("open");
            let _ = tx.send(Stage::Opened);
            transfer.write_all(b"three").await.expect("write");
            let _ = tx.send(Stage::Wrote);
        });
    }

    assert!(
        timeout(STALL, rx.recv()).await.is_err(),
        "queue depth is not stream credit: the third transfer must still block"
    );

    let first = within(puller.recv()).await.expect("recv one");
    assert_eq!(within(first.collect(64)).await.expect("collect"), b"one");
    assert_eq!(within(rx.recv()).await, Some(Stage::Opened));
    assert_eq!(within(rx.recv()).await, Some(Stage::Wrote));

    client.shutdown().await;
}

// --- 5. cancellation is per-stream and retroactive only for unread bytes ---

/// Claim: `cancel()` resets the stream. The reader keeps every byte it already
/// took, may still be served bytes its transport had buffered when the reset
/// arrived, and then observes a reset — never EOF. A canceled transfer can
/// therefore never be mistaken for a complete one, but "cancel" is not a
/// promise that the tail was unread.
///
/// The setup writes 8 KiB, reads exactly the first 4 KiB, and only then
/// cancels, so the head is provably unaffected. Reads continue until they
/// fail: through `AsyncRead` the failure is `io::ErrorKind::ConnectionReset`,
/// and on a second transfer `read_capped` reports `Error::Canceled`.
#[tokio::test]
async fn cancel_discards_unread_bytes_and_keeps_read_ones() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let payload: Vec<u8> = (0..8 * 1024u32).map(|i| (i % 251) as u8).collect();

    // The `AsyncRead` view.
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(&payload)).await.expect("write");
    let mut inbound = within(puller.recv()).await.expect("recv");
    let mut head = vec![0u8; 4 * 1024];
    within(inbound.read_exact(&mut head)).await.expect("read");
    assert_eq!(head, payload[..4 * 1024]);

    transfer.cancel();

    // Read on until the reset lands. Bytes already in the peer's assembler are
    // still delivered — the reset only discards what is left — so the count is
    // an observation, not a guarantee; the *failure* is the guarantee.
    let mut scratch = vec![0u8; 1024];
    let mut after_cancel = 0usize;
    let err = loop {
        match within(inbound.read(&mut scratch)).await {
            Ok(0) => panic!("a canceled transfer must never surface as EOF"),
            Ok(n) => after_cancel += n,
            Err(e) => break e,
        }
    };
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset,
        "quinn maps RESET_STREAM to ConnectionReset: {err:?}"
    );
    assert!(
        after_cancel <= 4 * 1024,
        "the reset cannot deliver more than the payload's remainder"
    );
    // Observed on loopback: all 4096 buffered bytes still arrive, then the
    // reset. Which side of that race a reader lands on is not a guarantee;
    // "the read fails and never reports EOF" is.
    // Still exactly the bytes we took before the reset.
    assert_eq!(head, payload[..4 * 1024]);

    // The same event through the typed convenience API.
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open again");
    within(transfer.write_all(&payload)).await.expect("write");
    let mut inbound = within(puller.recv()).await.expect("recv again");
    let mut head = vec![0u8; 4 * 1024];
    within(inbound.read_exact(&mut head)).await.expect("read");
    transfer.cancel();
    let err = within(inbound.read_capped(64 * 1024))
        .await
        .expect_err("a reset stream must not read as complete");
    assert!(matches!(err, Error::Canceled), "{err:?}");

    // The connection is untouched by a per-stream reset.
    within(pusher.send(b"next")).await.expect("send next");
    let next = within(puller.recv()).await.expect("recv next");
    assert_eq!(within(next.collect(64)).await.expect("collect"), b"next");

    client.shutdown().await;
}

// --- 6. a lost peer is reported, and a redial is the runtime's -------------

/// Claim: when a server goes away the loss is definite and named —
/// `ConnectionLost(PeerClosed)` — and `peer_count` stops counting the dead
/// peer. What happens next is the redial of `tests/reconnect.rs`; here the
/// policy is `never`, so the first send after the close is the one that
/// reports it, exactly as before 0031.
#[tokio::test]
async fn a_server_that_goes_away_is_reported_as_connection_lost() {
    let certs = Certs::generate();
    let first = ManualServer::start(&certs, RuntimeConfig::default()).await;
    let puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        reconnect: weida::ReconnectPolicy::never(),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    within(pusher.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"before")).await.expect("send before");
    let inbound = within(puller.recv()).await.expect("recv before");
    assert_eq!(
        within(inbound.collect(64)).await.expect("collect"),
        b"before"
    );

    first.binding.close().await;
    drop(puller);
    drop(first);

    // `close()` waits for the sockets to go idle, so the client has seen the
    // CONNECTION_CLOSE by now; the loop only guards against the close racing
    // the very next `open_uni`.
    let mut attempts = 0usize;
    let err = within(async {
        loop {
            attempts += 1;
            match pusher.send(b"orphan").await {
                Err(e) => return e,
                Ok(()) => tokio::task::yield_now().await,
            }
        }
    })
    .await;
    assert!(
        matches!(err, Error::ConnectionLost(_)),
        "a closed peer must be reported as ConnectionLost, got {err:?}"
    );
    assert_eq!(attempts, 1, "the first send after the close already fails");
    assert_eq!(pusher.peer_count(), 0, "a dead peer does not count");

    // A new server, same identity, new port: a new address is a new slot,
    // and the dead one no longer counts.
    let second = ManualServer::start(&certs, RuntimeConfig::default()).await;
    let puller = second.listener.puller("/jobs").expect("puller");
    within(pusher.connect(&second.url("/jobs")))
        .await
        .expect("connect again");
    within(pusher.send(b"after")).await.expect("send after");
    let inbound = within(puller.recv()).await.expect("recv after");
    assert_eq!(
        within(inbound.collect(64)).await.expect("collect"),
        b"after"
    );
    assert_eq!(pusher.peer_count(), 1, "only the live peer counts");

    client.shutdown().await;
}

// --- 7. idle timeout ------------------------------------------------------

/// Claim: a silent connection is declared dead after `idle_timeout`, and the
/// keep-alives that prevent that are sent only by the dialling side. A server
/// with a short idle timeout therefore drops idle clients whose keep-alive
/// interval is longer than that timeout.
///
/// The setup gives the server a 500 ms idle timeout — QUIC uses the smaller of
/// the two advertised values, so it governs both sides — against a client with
/// the default 10 s keep-alive. One exchange succeeds, then the connection is
/// left silent for 1.5 s (the single deliberate sleep in this file: the
/// phenomenon *is* elapsed time), and the next request fails.
#[tokio::test]
async fn idle_timeout_reports_loss_within_the_window() {
    let certs = Certs::generate();
    let server = ManualServer::start(
        &certs,
        RuntimeConfig {
            limits: Limits {
                idle_timeout: Duration::from_millis(500),
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        },
    )
    .await;
    let replier = server.listener.replier("/rpc").expect("replier");
    let replier = std::sync::Arc::new(replier);
    let handler = tokio::spawn({
        let replier = std::sync::Arc::clone(&replier);
        async move {
            let request = replier.accept().await.expect("accept");
            let mut out = request
                .reply(TransferMeta::default())
                .await
                .expect("open reply");
            out.write_all(b"pong").await.expect("write reply");
            out.finish().expect("finish reply");
        }
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    assert!(
        client.config().limits.keep_alive > Duration::from_millis(500),
        "the client's keep-alive must be too slow to save this connection"
    );
    let requester = client.requester(certs.client_tls());
    let mut events = requester.events();
    within(requester.connect(&server.url("/rpc")))
        .await
        .expect("connect");
    assert!(matches!(
        within(events.recv()).await,
        Some(weida::PeerEvent::Connected { .. })
    ));
    let reply = within(requester.request(b"ping")).await.expect("request");
    assert_eq!(within(reply.collect(64)).await.expect("collect"), b"pong");
    within(handler).await.expect("handler");

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // The cause, not merely the outcome: an application deciding what an
    // outage means must be able to tell an idle timeout from a peer that
    // closed deliberately. The cause is on the event stream
    // (0031), and the loss itself is the runtime's to repair: the server is
    // still there, so the next request rides the redialled connection.
    let lost = within(events.recv()).await;
    assert!(
        matches!(
            lost,
            Some(weida::PeerEvent::Lost {
                cause: LossCause::IdleTimeout,
                ..
            })
        ),
        "a timed-out peer must surface as an idle timeout, got {lost:?}"
    );
    let handler = tokio::spawn(async move {
        let request = replier.accept().await.expect("accept");
        let mut out = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        out.write_all(b"pong").await.expect("write reply");
        out.finish().expect("finish reply");
    });
    let reply = within(requester.request(b"ping again"))
        .await
        .expect("the redialled connection serves the request");
    assert_eq!(within(reply.collect(64)).await.expect("collect"), b"pong");
    within(handler).await.expect("handler");

    client.shutdown().await;
}

// --- 8. a replier that stops accepting ------------------------------------

/// Claim: a replier that stops calling `accept` stalls its requesters instead
/// of failing them or growing a queue. The binding constraint is the
/// bidirectional stream budget, not the accept queue: a request parked in the
/// queue still owns its stream.
///
/// The setup registers a replier and never accepts, with a one-deep queue and a
/// budget of two exchanges. Exchanges are opened, written and finished without
/// awaiting a reply; the number that completes is recorded, the next one is
/// shown to block rather than error, and accepting a single request unblocks
/// it.
#[tokio::test]
async fn a_replier_that_stops_accepting_stalls_requesters_after_the_queue_fills() {
    const BUDGET: usize = 2;

    let server = Server::start_with_config(RuntimeConfig {
        limits: Limits {
            max_concurrent_bidi_streams: BUDGET as u32,
            ..Limits::default()
        },
        endpoint_queue: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let replier = server.listener.replier("/rpc").expect("replier");
    let client = server.client_runtime();
    let requester = Arc::new(client.requester(server.trust()));
    within(requester.connect(&server.url("/rpc")))
        .await
        .expect("connect");

    // Reply halves are kept alive: dropping one would cancel its exchange.
    let mut replies = Vec::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut blocked_at = None;
    for i in 0..BUDGET + 2 {
        let requester = Arc::clone(&requester);
        let tx = tx.clone();
        let started = tokio::spawn(async move {
            let (mut transfer, reply) =
                requester.open(TransferMeta::default()).await.expect("open");
            transfer.write_all(b"req").await.expect("write");
            transfer.finish().expect("finish");
            let _ = tx.send(i);
            reply
        });
        match timeout(STALL, rx.recv()).await {
            Ok(Some(done)) => {
                assert_eq!(done, i, "exchanges report in order");
                replies.push(within(started).await.expect("exchange task"));
            }
            Ok(None) => unreachable!("a task holds the sender"),
            Err(_) => {
                blocked_at = Some((i, started));
                break;
            }
        }
    }

    let (blocked_at, blocked_task) = blocked_at.expect("the stream budget must run out");
    assert_eq!(
        blocked_at, BUDGET,
        "exactly the bidi budget worth of exchanges gets through; the one-deep \
         accept queue does not add a slot, because a queued request still owns \
         its stream"
    );
    assert_eq!(replies.len(), BUDGET);

    // One accept, and the dropped request closes both halves of its stream:
    // the returned credit is what the blocked exchange is waiting for.
    let accepted = within(replier.accept()).await.expect("accept");
    assert_eq!(accepted.meta().endpoint.as_deref(), Some("/rpc"));
    drop(accepted);

    assert_eq!(
        within(rx.recv()).await,
        Some(blocked_at),
        "the blocked exchange must proceed once a request is accepted"
    );
    replies.push(within(blocked_task).await.expect("blocked task"));

    client.shutdown().await;
}

// --- 9. reordering across streams, and what a reorder buffer costs --------

/// What one arrival order cost an application that wants dispatch order back.
struct Reorder {
    /// Payload sequence numbers in the order their transfers completed.
    arrivals: Vec<u64>,
    /// Arrivals that did not land at their dispatch position.
    out_of_order: usize,
    /// Most transfers an application-side reorder buffer held at once.
    peak_held: usize,
    /// Dispatch position at which the arrival order first diverges, if it does.
    first_divergence: Option<usize>,
}

/// How the reverse-order FINs are released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Finish {
    /// All FINs queued back to back, which is what an application does.
    Batched,
    /// Each FIN awaited to its transport receipt before the next is queued, so
    /// the peer provably processes them one at a time, in reverse order.
    Sequenced,
}

/// Claim: nothing orders transfers against each other, so an application that
/// needs dispatch order has to buffer, and the buffer is bounded only by how
/// many transfers are in flight. `Ordering = None` is therefore a cost the
/// receiving application pays, not a gap the transport might close.
///
/// The setup opens `n` transfers, writes a dispatch sequence into each, and
/// only then finishes them from the last to the first. A task per accepted
/// transfer reports the sequence it read, so what is observed is *completion*
/// order rather than the order a single-threaded reader would impose on
/// itself.
///
/// Asserted is the invariant, never the order: every dispatched transfer
/// arrives exactly once and the reorder buffer drains empty. The two numbers
/// — arrivals out of dispatch position, and the peak the buffer held — are
/// recorded in `docs/IMPLEMENTATION.md`, because QUIC promises no ordering
/// across streams and a test that pinned one would pin an accident.
async fn reorder_probe(n: usize, finish: Finish) -> Reorder {
    let server = Server::start_with_config(RuntimeConfig {
        endpoint_queue: n + 1,
        ..RuntimeConfig::default()
    })
    .await;
    let puller = server.listener.puller("/reorder").expect("puller");
    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/reorder")))
        .await
        .expect("connect");

    // One task per accepted transfer, so a transfer that finishes early is
    // reported early instead of waiting behind an unfinished sibling.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        for _ in 0..n {
            let transfer = puller.recv().await.expect("recv");
            let tx = tx.clone();
            tokio::spawn(async move {
                let body = transfer.collect(64).await.expect("collect");
                let bytes: [u8; 8] = body[..].try_into().expect("8-byte sequence");
                let seq = u64::from_le_bytes(bytes);
                let _ = tx.send(seq);
            });
        }
        puller
    });

    let mut transfers = Vec::with_capacity(n);
    for seq in 0..n as u64 {
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open");
        within(transfer.write_all(&seq.to_le_bytes()))
            .await
            .expect("write");
        transfers.push(transfer);
    }
    // Reverse order: the transfer dispatched first is the last to be finished,
    // so its FIN is the last to arrive.
    for transfer in transfers.into_iter().rev() {
        let delivery = transfer.finish().expect("finish");
        if finish == Finish::Sequenced {
            within(delivery.delivered()).await.expect("delivered");
        }
    }

    let mut arrivals = Vec::with_capacity(n);
    for _ in 0..n {
        arrivals.push(within(rx.recv()).await.expect("arrival"));
    }
    let _puller = within(reader).await.expect("reader task");

    // An application-side reorder buffer keyed on the payload sequence: hold
    // what cannot be released yet, release as soon as the next one is there.
    let mut held = std::collections::BTreeSet::new();
    let mut next_expected = 0u64;
    let mut peak_held = 0usize;
    for &seq in &arrivals {
        held.insert(seq);
        while held.remove(&next_expected) {
            next_expected += 1;
        }
        peak_held = peak_held.max(held.len());
    }
    assert!(
        held.is_empty() && next_expected == n as u64,
        "the reorder buffer must drain: {} held, next_expected {next_expected}",
        held.len()
    );

    let diverged: Vec<usize> = arrivals
        .iter()
        .enumerate()
        .filter(|(position, seq)| **seq != *position as u64)
        .map(|(position, _)| position)
        .collect();

    client.shutdown().await;
    Reorder {
        arrivals,
        out_of_order: diverged.len(),
        peak_held,
        first_divergence: diverged.first().copied(),
    }
}

#[tokio::test]
async fn reverse_order_completion_measures_the_reorder_buffer() {
    // `Sequenced` awaits a transport receipt per transfer, so it is run at the
    // small size only: at 256 transfers it would spend a quarter of a second
    // per hundred on QUIC's delayed acknowledgements and measure the same
    // bound.
    let cases = [
        (16usize, Finish::Batched),
        (256, Finish::Batched),
        (16, Finish::Sequenced),
    ];

    for (n, finish) in cases {
        let probe = within(reorder_probe(n, finish)).await;

        assert_eq!(probe.arrivals.len(), n, "every transfer must arrive");
        let mut seen = probe.arrivals.clone();
        seen.sort_unstable();
        assert!(
            seen.iter().copied().eq(0..n as u64),
            "each dispatched transfer must arrive exactly once"
        );
        assert!(probe.peak_held < n, "the buffer cannot hold every transfer");

        eprintln!(
            "reorder n={n} {finish:?}: {} of {n} arrived out of dispatch position, peak reorder \
             buffer {} transfers, first divergence at position {:?}, first five arrivals {:?}",
            probe.out_of_order,
            probe.peak_held,
            probe.first_divergence,
            &probe.arrivals[..probe.arrivals.len().min(5)],
        );
    }
}

// --- expiry ----------------------------------------------------------------

/// Claim: a transfer given a deadline is reset with `CANCELED` when its bytes
/// are not acknowledged by then (0034 §4.3). The sender's blocked write fails
/// with `Expired`, the reader sees `Canceled` rather than EOF, and the runtime
/// counts the expiry apart from a cancel.
///
/// The reader holds its transfer unread, so a 64 KiB stream window stalls a
/// 1 MiB write long before the deadline.
#[tokio::test]
async fn a_transfer_past_its_deadline_is_reset() {
    let server = Server::start_with(Limits {
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    })
    .await;
    let acceptor = server.listener.acceptor("/slow").expect("acceptor");
    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/slow")))
        .await
        .expect("connect");

    let mut transfer = within(peer.open(TransferMeta::default()))
        .await
        .expect("open");
    let started = std::time::Instant::now();
    transfer.expire_at(started + Duration::from_millis(300));
    let held = match within(acceptor.accept()).await.expect("accept") {
        weida::Incoming::Stream(held) => held,
        other => panic!("expected a stream, got {other:?}"),
    };

    let failed = within(transfer.write_all(&vec![0x5a; 1 << 20]))
        .await
        .expect_err("the window stalls the write past its deadline");
    let took = started.elapsed();
    assert!(matches!(failed, Error::Expired), "{failed:?}");
    assert!(
        took >= Duration::from_millis(300) && took < Duration::from_secs(3),
        "expired after {took:?}"
    );
    assert!(!failed.is_definite_failure());

    let read = within(held.collect(2 << 20)).await;
    assert!(matches!(read, Err(Error::Canceled)), "{read:?}");
    assert_eq!(client.expired_transfers(), 1);
    client.shutdown().await;
}
