//! What per-subscriber fan-out costs, at a width worth the name.
//!
//! weida gives every subscriber its own unidirectional stream per message, so
//! a stream nobody reads does not delay its siblings ([`PATTERNS.md`] §1.3).
//! That isolation is the argument for the design and nothing in this
//! repository had priced it: the largest fan-out ever measured here was
//! **eight** subscribers, in `patterns.rs`. The guide's C8B arithmetic rests
//! on the width one node holds ([`GUIDE.md`] §0.1), which makes this the one
//! number the whole depth argument stands on.
//!
//! Three things are measured, at widths 1, 16 and 256:
//!
//! 1. **What the publisher pays per message.** `publish` is synchronous and
//!    never waits on a subscriber, so its cost is `width` matcher walks,
//!    `width` queue pushes and `width` budget reservations — linear in the
//!    width by construction, and the question is the constant.
//! 2. **What a subscriber waits.** Median and tail of the delay between the
//!    `publish` call and the payload arriving in a subscriber's
//!    `IncomingTransfer`, over every copy of every message.
//! 3. **Which of the two bounds binds first.** A fan-out node is limited by
//!    per-connection transport state (`IMPLEMENTATION.md` §4.8 measures
//!    750-850 KiB per connection, both ends together) and by
//!    `Limits::subscriber_buffer_bytes`, the 8 MiB a slow subscriber may hold.
//!    The second half of this bench stalls every subscriber and publishes
//!    until the drop counter moves, which is the only way to see what those
//!    8 MiB per subscriber actually cost in resident memory.
//!
//! One client runtime per subscriber, as in `connections.rs`: a subscriber
//! claims its path in its connection's namespace, so subscribers cannot share
//! a pooled connection, and both ends of every connection live in this
//! process. Loopback QUIC, `RuntimeConfig::default()`, 1 KiB payload. The
//! machine and the numbers are in [`IMPLEMENTATION.md`] — read them as one
//! desktop's shape, not as a portable constant.
//!
//! [`PATTERNS.md`]: https://github.com/tuco86/weida/blob/main/docs/PATTERNS.md
//! [`GUIDE.md`]: https://github.com/tuco86/weida/blob/main/docs/GUIDE.md
//! [`IMPLEMENTATION.md`]: https://github.com/tuco86/weida/blob/main/docs/IMPLEMENTATION.md

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{Identity, Listener, Publisher, Runtime, RuntimeConfig, Subscriber, Trust};

/// Fan-out widths reported. 1 is the floor every per-subscriber cost is
/// measured against, 16 is a service fan-out, 256 is the largest width this
/// process can hold as one runtime per subscriber without the measurement
/// becoming a memory benchmark of tokio.
const WIDTHS: [usize; 3] = [1, 16, 256];

/// Payload size, matching `patterns.rs` so the two fan-out figures compare.
const PAYLOAD: usize = 1024;

/// Messages per width for the latency distribution. 64 messages at width 256
/// is 16 384 copies, which is enough for a p99 and short enough that the whole
/// bench stays inside a minute.
const MESSAGES: usize = 64;

/// Width for the stalled-subscriber half, where every copy is held rather than
/// read. Small on purpose: the interesting quantity is bytes per subscriber,
/// and 16 subscribers holding 8 MiB each is already 128 MiB of accounted
/// budget.
const STALLED_WIDTH: usize = 16;

/// Payload sizes for the stalled half, one on each side of the crossover
/// between the two ceilings: at the defaults the queue holds 256 messages and
/// the budget 8 MiB, so the message size at which they meet is 32 KiB. 1 KiB
/// must reach the queue first and 64 KiB the budget — which is a prediction
/// this bench either confirms or refutes.
const STALLED_PAYLOADS: [usize; 2] = [1024, 64 * 1024];

/// A subscriber and the client runtime that owns its connection.
type Sub = (Runtime, Arc<Subscriber>);

struct Harness {
    runtime: Runtime,
    trust: Trust,
    addr: SocketAddr,
    listener: Listener,
    _binding: weida::Binding,
}

impl Harness {
    fn client(&self) -> Runtime {
        Runtime::new(RuntimeConfig::default()).expect("client runtime")
    }

    fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }
}

async fn harness() -> Harness {
    let identity = Identity::generate().expect("identity");
    let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
        .await
        .expect("bind");
    let addr = binding.local_addr();

    Harness {
        runtime,
        trust,
        addr,
        listener,
        _binding: binding,
    }
}

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Resident set size of this process in bytes, on Linux.
///
/// `VmRSS` and not `VmHWM`, for the reason `connections.rs` gives: what a set
/// of live subscribers holds *now* is the quantity a fan-out node is sized by.
fn current_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib * 1024);
        }
    }
    None
}

/// A subscriber set of `width`, each on its own client runtime, all subscribed
/// to everything on the publisher's path and visible to `publisher`.
async fn subscribers(harness: &Harness, publisher: &Publisher, width: usize) -> Vec<Sub> {
    let url = harness.url(publisher.path());
    let mut set = Vec::with_capacity(width);
    for _ in 0..width {
        let client = harness.client();
        let sub = client.subscriber(harness.trust.clone());
        sub.connect(&url).await.expect("connect");
        sub.subscribe("").await.expect("subscribe");
        set.push((client, Arc::new(sub)));
    }
    // A SUBSCRIBE travels on its own stream, so the publisher learns of it
    // asynchronously; measuring before it has them all would report a
    // narrower fan-out than the one named.
    while publisher.filter_count() != width {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    set
}

/// `[median, p99, max]` of `samples`, which is sorted in place.
fn quantiles(samples: &mut [u64]) -> [Duration; 3] {
    samples.sort_unstable();
    let at = |q: f64| {
        let last = samples.len().saturating_sub(1);
        let index = ((samples.len() as f64 * q) as usize).min(last);
        Duration::from_nanos(samples.get(index).copied().unwrap_or(0))
    };
    [
        at(0.5),
        at(0.99),
        Duration::from_nanos(samples.last().copied().unwrap_or(0)),
    ]
}

/// Per-subscriber latency, publisher cost and memory, at each width.
///
/// The latency clock is one `Instant` epoch shared by both ends, which is
/// sound because both ends are this process: the publisher writes the epoch
/// offset into the first eight bytes of the payload and the receiving task
/// subtracts it from its own offset. That measures the whole path — matcher
/// walk, queue, stream open, QUIC loopback, frame decode — and nothing else.
///
/// **Two phases, because they answer different questions and the first run of
/// this bench conflated them.** In the *idle* phase the publisher waits until
/// every copy of a message has arrived before publishing the next, so each
/// sample is one message's path with nothing queued ahead of it — the
/// per-subscriber latency `IMPLEMENTATION.md` §4.11 asks for. In the *burst*
/// phase it publishes `MESSAGES` back to back without waiting, so the samples include the
/// queueing behind the burst: that number is a backlog, and at width 256 it
/// is two orders of magnitude larger than the idle one. Reporting only the
/// burst figure as "latency" would be the kind of number this repository
/// refuses.
fn report_fanout(rt: &tokio::runtime::Runtime, harness: &Harness) {
    for width in WIDTHS {
        let baseline = current_rss();
        let publisher = harness
            .listener
            .publisher(&format!("/fan{width}"))
            .expect("publisher");

        rt.block_on(async {
            let set = subscribers(harness, &publisher, width).await;
            let connected = current_rss();

            let epoch = Instant::now();
            let received = Arc::new(AtomicUsize::new(0));
            let idle = Arc::new(std::sync::Mutex::new(Vec::with_capacity(width * MESSAGES)));
            let burst = Arc::new(std::sync::Mutex::new(Vec::with_capacity(width * MESSAGES)));
            let mut drains = Vec::with_capacity(width);
            for (_, sub) in &set {
                let sub = Arc::clone(sub);
                let received = Arc::clone(&received);
                let idle = Arc::clone(&idle);
                let burst = Arc::clone(&burst);
                drains.push(tokio::spawn(async move {
                    let mut mine = Vec::with_capacity(2 * MESSAGES);
                    for _ in 0..2 * MESSAGES {
                        let Ok(transfer) = sub.recv().await else {
                            break;
                        };
                        let Ok(body) = transfer.collect(64 * 1024).await else {
                            break;
                        };
                        let sent = u64::from_le_bytes(
                            body[..8].try_into().expect("eight bytes of timestamp"),
                        );
                        mine.push(epoch.elapsed().as_nanos() as u64 - sent);
                        // Ordered per subscriber, so the count is the phase:
                        // published one at a time first, then in a burst.
                        received.fetch_add(1, Ordering::Release);
                    }
                    let (first, second) = mine.split_at(MESSAGES.min(mine.len()));
                    idle.lock().expect("idle poisoned").extend_from_slice(first);
                    burst
                        .lock()
                        .expect("burst poisoned")
                        .extend_from_slice(second);
                }));
            }

            let mut payload = vec![0x7au8; PAYLOAD];
            let mut calls = Vec::with_capacity(2 * MESSAGES);
            let stamp = |payload: &mut [u8]| {
                payload[..8].copy_from_slice(&(epoch.elapsed().as_nanos() as u64).to_le_bytes());
            };

            // Idle phase: one message in flight at a time.
            for message in 0..MESSAGES {
                stamp(&mut payload);
                let call = Instant::now();
                let sent = publisher
                    .publish("px.eur", payload.clone())
                    .expect("publish");
                calls.push(call.elapsed().as_nanos() as u64);
                assert_eq!(sent, width, "a subscriber did not take the message");
                let done = (message + 1) * width;
                while received.load(Ordering::Acquire) < done {
                    tokio::task::yield_now().await;
                }
            }

            // Burst phase: nothing waits.
            let start = Instant::now();
            for _ in 0..MESSAGES {
                stamp(&mut payload);
                let call = Instant::now();
                let sent = publisher
                    .publish("px.eur", payload.clone())
                    .expect("publish");
                calls.push(call.elapsed().as_nanos() as u64);
                assert_eq!(sent, width, "a subscriber did not take the message");
            }
            let enqueued = start.elapsed();

            // Every drain ends after its own `2 * MESSAGES`, so joining them
            // is the drain time rather than a timeout.
            for drain in drains {
                drain.await.expect("drain task");
            }
            let drained = start.elapsed();

            let [call_median, call_p99, call_max] = quantiles(&mut calls);
            let mut idle = std::mem::take(&mut *idle.lock().expect("idle poisoned"));
            let mut burst = std::mem::take(&mut *burst.lock().expect("burst poisoned"));
            let copies = idle.len() + burst.len();
            let [idle_median, idle_p99, idle_max] = quantiles(&mut idle);
            let [burst_median, burst_p99, burst_max] = quantiles(&mut burst);

            let memory = match (baseline, connected) {
                (Some(before), Some(after)) => format!(
                    "{:.1} KiB per subscriber ({:.2} MiB for {width})",
                    after.saturating_sub(before) as f64 / 1024.0 / width as f64,
                    after.saturating_sub(before) as f64 / (1024.0 * 1024.0),
                ),
                _ => "RSS unavailable on this platform".to_owned(),
            };
            eprintln!(
                "fanout width={width}: publish call {call_median:.2?} median, {call_p99:.2?} p99, \
                 {call_max:.2?} max; idle latency {idle_median:.2?} median, {idle_p99:.2?} p99, \
                 {idle_max:.2?} max; burst of {MESSAGES} enqueued in {enqueued:.2?}, \
                 {} copies drained in {drained:.2?} ({:.1} Kcopies/s), \
                 burst latency {burst_median:.2?} median, {burst_p99:.2?} p99, \
                 {burst_max:.2?} max; {} drops; {memory}",
                burst.len(),
                burst.len() as f64 / drained.as_secs_f64() / 1000.0,
                publisher.dropped(),
            );

            assert_eq!(copies, 2 * width * MESSAGES, "a copy went missing");
            assert_eq!(
                publisher.dropped(),
                0,
                "a keeping-up subscriber lost a copy"
            );
            drop(set);
        });
    }
}

/// Which bound binds first when every subscriber stalls.
///
/// This is the question the guide's §0.1 derivation needs answered. A fan-out
/// node has three ceilings and they are not the same size:
///
/// * per-connection **transport state**, which `report_fanout` measures;
/// * the per-subscriber **queue**, `RuntimeConfig::endpoint_queue`, 256
///   messages deep by default;
/// * the per-subscriber **byte budget**, `Limits::subscriber_buffer_bytes`,
///   8 MiB by default — 8192 messages at a 1 KiB payload, 128 at 64 KiB.
///
/// So the derivation that sized a node by `width * 8 MiB` assumed the budget
/// binds, and whether it does depends on the payload: the two ceilings cross
/// where `subscriber_buffer_bytes / endpoint_queue` is the message size, which
/// is **32 KiB** at the defaults. Both sides of that crossover are measured
/// here rather than derived, and the bound is reported **by name** from
/// `dropped_on`: nobody reads, the publisher runs until `dropped()` moves, and
/// the counters say whether the queue or the budget refused the copy.
///
/// The resident memory at that point is the second half of the answer, because
/// the payload of one `publish` is **one** `Bytes` allocation that every copy
/// shares: the budget is an accounting bound, and what a stalled fan-out holds
/// is one payload per distinct message rather than one per copy.
fn report_stalled_budget(rt: &tokio::runtime::Runtime, harness: &Harness) {
    for payload_len in STALLED_PAYLOADS {
        let publisher = harness
            .listener
            .publisher(&format!("/stalled{payload_len}"))
            .expect("publisher");
        rt.block_on(async {
            let set = subscribers(harness, &publisher, STALLED_WIDTH).await;
            let payload = vec![0x7au8; payload_len];

            let held = current_rss();
            let mut accepted = 0usize;
            // A ceiling rather than a `while true`: a bound must be reached
            // within `subscriber_buffer_bytes / payload_len` messages plus the
            // queue depth, and a bench that cannot end is worse than one that
            // reports a bound it did not reach.
            let ceiling = 64 * 1024;
            while publisher.dropped() == 0 && accepted < ceiling {
                publisher
                    .publish("px.eur", payload.clone())
                    .expect("publish");
                accepted += 1;
                // The per-subscriber writer tasks need the executor to move a
                // copy from the queue onto a stream; without a yield this loop
                // starves them and measures the queue of a publisher nobody
                // serves.
                if accepted.is_multiple_of(256) {
                    tokio::task::yield_now().await;
                }
            }
            let after = current_rss();

            let drops = publisher
                .dropped_on("px.eur")
                .expect("the topic that lost a copy");
            let bound = if drops.subscriber_budget > 0 {
                "the byte budget"
            } else if drops.subscriber_queue > 0 {
                "the queue"
            } else {
                "neither: no parked connection"
            };
            let resident = match (held, after) {
                (Some(before), Some(after)) => format!(
                    "{:.2} MiB resident",
                    after.saturating_sub(before) as f64 / (1024.0 * 1024.0)
                ),
                _ => "RSS unavailable on this platform".to_owned(),
            };
            eprintln!(
                "stalled width={STALLED_WIDTH} payload={payload_len}: {accepted} messages \
                 accepted before the first drop, so {:.2} MiB per subscriber of the 8 MiB \
                 budget and {:.2} MiB accounted over the fan-out; {resident}; \
                 first bound reached: {bound} \
                 (budget {}, queue {}, no parked connection {})",
                (accepted * payload_len) as f64 / (1024.0 * 1024.0),
                (accepted * payload_len * STALLED_WIDTH) as f64 / (1024.0 * 1024.0),
                drops.subscriber_budget,
                drops.subscriber_queue,
                drops.no_parked_connection,
            );
            assert!(
                accepted < ceiling,
                "nothing bound: {accepted} messages of {payload_len} B went into stalled \
                 subscribers"
            );
            drop(set);
        });
    }
}

/// The publisher's own cost per message, as a criterion distribution.
///
/// `report_fanout` above measures a set and prints it; this measures the one
/// operation a fan-out node repeats, at each width, with every subscriber
/// draining. It is the number that decides whether a node can serve a width at
/// a message rate: at width `w` a publish is `w` matcher walks, `w` queue
/// pushes and `w` semaphore acquisitions.
fn bench_publish(c: &mut Criterion) {
    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    report_fanout(&rt, &harness);
    report_stalled_budget(&rt, &harness);

    let mut group = c.benchmark_group("fanout_publish");
    group.sample_size(20);
    for width in WIDTHS {
        let publisher = harness
            .listener
            .publisher(&format!("/bench{width}"))
            .expect("publisher");
        let set = rt.block_on(subscribers(&harness, &publisher, width));
        // Every subscriber drains forever: the timed section is the publish
        // call, and a stalled subscriber would turn it into a drop counter.
        for (_, sub) in &set {
            let sub = Arc::clone(sub);
            rt.spawn(async move {
                while let Ok(transfer) = sub.recv().await {
                    let _ = transfer.collect(64 * 1024).await;
                }
            });
        }

        let payload = vec![0x7au8; PAYLOAD];
        group.throughput(Throughput::Bytes((width * PAYLOAD) as u64));
        group.bench_function(format!("publish_1kib_{width}_subscribers"), |b| {
            b.iter(|| {
                let sent = publisher
                    .publish("px.eur", black_box(payload.clone()))
                    .expect("publish");
                black_box(sent)
            })
        });
        drop(set);
    }
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);
