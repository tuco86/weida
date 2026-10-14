//! Push/Pull and Pub/Sub benchmarks over loopback QUIC.
//!
//! Two profiles matching what the patterns are for: a one-way send where the
//! acknowledgement mode is the only variable, and a fan-out where the cost
//! scales with subscriber count. As in `echo.rs`, the server is built once per
//! group so the measured work is the transfer, not TLS handshakes.

use std::net::SocketAddr;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{Identity, Listener, Publisher, Puller, Runtime, RuntimeConfig, TransferMeta, Trust};
use weida_protocol::{DataHeader, FrameKind, ReportMode, encode_frame};

const PAYLOAD: usize = 1024;

struct Harness {
    runtime: Runtime,
    /// The server's fingerprint. Nothing is written to disk: `criterion`
    /// exits the process when the run ends, so `Drop` would not fire and a
    /// key file would survive the benchmark.
    trust: Trust,
    addr: SocketAddr,
    listener: Listener,
    _binding: weida::Binding,
}

impl Harness {
    /// A client runtime and an endpoint that trusts this server.
    fn client(&self) -> Runtime {
        Runtime::new(RuntimeConfig::default()).expect("client runtime")
    }

    fn trust(&self) -> Trust {
        self.trust.clone()
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

/// Drains a puller forever, so the sender never stalls on a full queue.
fn drain(puller: Puller) {
    tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(64 * 1024).await;
        }
    });
}

fn bench_push(c: &mut Criterion) {
    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    let (best_effort, delivered) = rt.block_on(async {
        drain(harness.listener.puller("/best").expect("puller best"));
        drain(
            harness
                .listener
                .puller("/delivered")
                .expect("puller delivered"),
        );

        let client = harness.client();
        let best_effort = client.pusher(harness.trust());
        best_effort
            .connect(&harness.url("/best"))
            .await
            .expect("connect best");
        let delivered = client.pusher(harness.trust());
        delivered
            .connect(&harness.url("/delivered"))
            .await
            .expect("connect delivered");
        (best_effort, delivered)
    });

    let payload = vec![0x61u8; PAYLOAD];
    let mut group = c.benchmark_group("push");
    group.throughput(Throughput::Bytes(PAYLOAD as u64));

    // Best effort settles at FIN: no round trip, so this measures the send
    // path alone.
    group.bench_function("push_1kib_best_effort", |b| {
        b.to_async(&rt).iter(|| async {
            best_effort.send(black_box(&payload)).await.expect("send");
        })
    });

    // Awaiting the receipt waits for the peer's transport acknowledgement:
    // one network round trip, and no application involvement at all.
    group.bench_function("push_1kib_delivered", |b| {
        b.to_async(&rt).iter(|| async {
            let mut transfer = delivered.open(TransferMeta::default()).await.expect("open");
            transfer
                .write_all(black_box(&payload))
                .await
                .expect("write");
            transfer
                .finish()
                .expect("finish")
                .delivered()
                .await
                .expect("delivered");
        })
    });
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

fn bench_fanout(c: &mut Criterion) {
    const SUBSCRIBERS: usize = 8;

    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    let (publisher, mut subscribers) = rt.block_on(async {
        let publisher: Publisher = harness.listener.publisher("/md").expect("publisher");
        let mut subscribers = Vec::with_capacity(SUBSCRIBERS);
        for _ in 0..SUBSCRIBERS {
            // One runtime per subscriber: a subscriber claims its path in its
            // connection's namespace, so they cannot share a pooled connection.
            let client = harness.client();
            let sub = client.subscriber(harness.trust());
            sub.connect(&harness.url("/md")).await.expect("connect");
            sub.subscribe("").await.expect("subscribe");
            subscribers.push((client, sub));
        }
        // Wait until the publisher can see all of them, or the first iteration
        // would measure a partial fan-out.
        while publisher.filter_count() != SUBSCRIBERS {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        (publisher, subscribers)
    });

    let payload = vec![0x7au8; PAYLOAD];
    let mut group = c.benchmark_group("fanout");
    group.throughput(Throughput::Bytes((SUBSCRIBERS * PAYLOAD) as u64));
    group.sample_size(10);

    group.bench_function("pub_1kib_8_subscribers", |b| {
        b.to_async(&rt).iter(|| async {
            let sent = publisher
                .publish("px.eur", black_box(payload.clone()))
                .expect("publish");
            assert_eq!(sent, SUBSCRIBERS, "a subscriber dropped the message");
            // Draining every subscriber is part of the measured work: the
            // publish itself only enqueues.
            for (_, sub) in &subscribers {
                let transfer = sub.recv().await.expect("recv");
                let body = transfer.collect(64 * 1024).await.expect("collect");
                debug_assert_eq!(body.len(), PAYLOAD);
            }
            black_box(sent)
        })
    });
    group.finish();

    subscribers.clear();
    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

/// What the segmented matcher costs in the publisher's fan-out path
/// (`docs/IMPLEMENTATION.md` §4.10).
///
/// The objection that decision 0007 §4.6 accepted was that a matching language
/// belongs nowhere near a hot path. This measures the price it actually asked
/// for.
///
/// The trick is a subscriber whose filters all **fail**: one connection holds
/// `FILTERS` of them, the published topic matches none, so `publish` returns 0
/// and the measurement is `FILTERS` matcher calls plus the publish frame — no
/// enqueue, no fan-out, no drain. The shapes then differ only in how much work
/// one call is:
///
/// * `literal` — a mismatch in the first segment, which is the cheapest walk
///   and the floor a byte prefix would also have to pay;
/// * `one_segment` — `*` in the middle, so the walk reaches the third segment
///   before failing;
/// * `rest` — a trailing `#`, so the walk fails on a literal segment before
///   the wildcard can accept anything.
///
/// `prefix_reference` is the byte-prefix matcher the walk is measured against,
/// `topic.starts_with`, over the same strings. It is a pure function here
/// because the library has no such matcher: it is the scale, not a switchable
/// implementation.
/// The matched, drained fan-out is the `fanout` group above.
fn bench_filters(c: &mut Criterion) {
    /// Filters held by one connection; `max_subscriptions` defaults to 256.
    const FILTERS: usize = 64;
    /// Matches none of the filters below, in the first segment.
    const TOPIC: &str = "px.eur.spot";

    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    // The last field is how many of that shape one connection holds. The pair
    // at 1 and at 64 is what isolates the matcher: the difference divided by
    // 63 is the cost of one more filter, with every fixed cost of `publish`
    // subtracted out.
    let shapes: [(&str, usize); 5] = [
        ("literal_x1", 1),
        ("literal_x64", FILTERS),
        ("one_segment_x1", 1),
        ("one_segment_x64", FILTERS),
        ("rest_x64", FILTERS),
    ];

    let mut group = c.benchmark_group("filters");

    for (name, count) in shapes {
        let filters: Vec<String> = (0..count)
            .map(|i| {
                if name.starts_with("literal") {
                    format!("zz{i}.eur.spot")
                } else if name.starts_with("one_segment") {
                    format!("zz{i}.*.spot")
                } else {
                    format!("zz{i}.eur.#")
                }
            })
            .collect();
        let path = format!("/md-{name}");
        let (publisher, _client, _sub) = rt.block_on(async {
            let publisher: Publisher = harness.listener.publisher(&path).expect("publisher");
            let client = harness.client();
            let sub = client.subscriber(harness.trust());
            sub.connect(&harness.url(&path)).await.expect("connect");
            for filter in &filters {
                sub.subscribe(filter).await.expect("subscribe");
            }
            while publisher.filter_count() != count {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            (publisher, client, sub)
        });

        group.bench_function(name, |b| {
            b.iter(|| {
                let sent = publisher
                    .publish(black_box(TOPIC), &b"x"[..])
                    .expect("publish");
                // Zero is the point: every filter was walked and every walk
                // said no, so nothing was enqueued or copied.
                assert_eq!(sent, 0, "the bench topic must match no filter");
                sent
            })
        });
    }

    let prefix_filters: Vec<String> = (0..FILTERS).map(|i| format!("zz{i}.eur.spot")).collect();

    // The byte-prefix matcher, for scale: 64 byte-prefix comparisons over the
    // same strings, with no publish around them.
    group.bench_function("prefix_reference", |b| {
        b.iter(|| {
            let mut matched = 0usize;
            for filter in &prefix_filters {
                if black_box(TOPIC).starts_with(filter.as_str()) {
                    matched += 1;
                }
            }
            matched
        })
    });
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

/// What the two DATA keys of decision 0001 will cost at a high message rate.
///
/// Neither key exists on the wire yet, so each is simulated by the key with its
/// exact wire shape: `content_len` is a `uint` key with a `uint` value, the
/// shape of the sequence number, and a fingerprint-shaped `content_type` is a
/// `uint` key with a 71-byte `tstr`, the shape of the producer identity of
/// decision 0008. The two endpoint paths are the same length, so the only
/// difference between the two measurements is the two keys.
fn bench_header_cost(c: &mut Criterion) {
    /// Small enough that the header is a visible fraction of the message.
    const SMALL: usize = 64;
    /// A five-byte CBOR `uint`: what a long-lived producer's counter reaches.
    const SEQUENCE: u64 = 1 << 20;
    /// `sha256:` plus 64 hex digits — the producer name of decision 0008.
    const PRODUCER: &str =
        "sha256:22ed30a800000000000000000000000000000000000000000000000000009f25";
    assert_eq!(PRODUCER.len(), 71, "the producer key is a 71-byte tstr");

    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    let (lean_pusher, rich_pusher, traced_pusher) = rt.block_on(async {
        drain(harness.listener.puller("/keys0").expect("puller keys0"));
        drain(harness.listener.puller("/keys2").expect("puller keys2"));
        drain(harness.listener.puller("/traced").expect("puller traced"));

        let client = harness.client();
        let lean = client.pusher(harness.trust());
        lean.connect(&harness.url("/keys0"))
            .await
            .expect("connect keys0");
        let rich = client.pusher(harness.trust());
        rich.connect(&harness.url("/keys2"))
            .await
            .expect("connect keys2");
        let traced = client.pusher(harness.trust());
        traced
            .connect(&harness.url("/traced"))
            .await
            .expect("connect traced");
        (lean, rich, traced)
    });

    let payload = vec![0x61u8; SMALL];
    let lean_meta = TransferMeta::default();
    let rich_meta = TransferMeta::default()
        .with_content_len(SEQUENCE)
        .with_content_type(PRODUCER);
    // A caller-supplied trace context: what key `3` costs when the runtime
    // writes it only on propagation
    // ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)),
    // and `lean` below is what a message costs without it.
    let traced_meta = TransferMeta::default().with_trace(weida::new_trace());

    // Criterion reports messages per second; the bytes per message are
    // deterministic, so they are computed rather than measured.
    let lean_bytes = wire_bytes("/keys0", &lean_meta, SMALL);
    let rich_bytes = wire_bytes("/keys2", &rich_meta, SMALL);
    let traced_bytes = wire_bytes("/traced", &traced_meta, SMALL);
    eprintln!(
        "header cost: {lean_bytes} B/message minimal, {rich_bytes} B/message with two extra keys \
         (+{} B, +{:.1} % over a {SMALL}-byte payload), {traced_bytes} B/message with a \
         caller-supplied traceparent (+{} B)",
        rich_bytes - lean_bytes,
        (rich_bytes - lean_bytes) as f64 * 100.0 / lean_bytes as f64,
        traced_bytes - lean_bytes,
    );

    let mut group = c.benchmark_group("header");
    group.throughput(Throughput::Elements(1));

    for (name, meta, pusher) in [
        ("push_64b_keys0", &lean_meta, &lean_pusher),
        ("push_64b_keys2", &rich_meta, &rich_pusher),
        ("push_64b_traced", &traced_meta, &traced_pusher),
    ] {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async {
                let mut transfer = pusher.open(meta.clone()).await.expect("open");
                transfer
                    .write_all(black_box(&payload))
                    .await
                    .expect("write");
                // Best effort: the receipt is dropped, so this is the send path
                // and nothing else.
                transfer.finish().expect("finish");
            })
        });
    }
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

/// Bytes one DATA frame puts on the wire: preamble, CBOR header, payload.
///
/// **The header is the one the runtime writes**, not a reconstruction of it.
/// A header rebuilt by hand with a hardcoded 55-byte `traceparent` would go
/// wrong whenever `data_header` stops minting one
/// ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md),
/// `docs/IMPLEMENTATION.md` §4.5). A measurement that can disagree with the
/// code is worse than none, so this builds the header the same way the send
/// path does: same fields, same optional keys, and a `traceparent` exactly
/// when `meta` carries a context.
fn wire_bytes(endpoint: &str, meta: &TransferMeta, payload: usize) -> usize {
    let header = DataHeader {
        endpoint: Some(endpoint.to_owned()),
        content_len: meta.content_len,
        content_type: meta.content_type.clone(),
        traceparent: meta.trace.map(|t| t.to_traceparent()),
        tracestate: None,
        topic: None,
        // The patterns benched here write neither key `6` nor key `7` (their
        // cost is measured in `docs/IMPLEMENTATION.md` §4.4), and key `8` is
        // an L2 confirm no pattern here sends.
        sequence: None,
        producer: None,
        achieved: meta.achieved,
        report_id: None,
        report: Vec::new(),
        report_mode: ReportMode::default(),
        segment: None,
        layer: None,
    };
    encode_frame(FrameKind::Data, &header.encode()).len() + payload
}

/// What the dedup key costs per call (`docs/IMPLEMENTATION.md` §4.12).
///
/// `DedupWindow::is_duplicate` builds its lookup key with
/// `scope: scope.into()`, which allocates a `Box<str>` on **every** call —
/// including the two that never insert anything: a duplicate that is found,
/// and a miss whose entry is then written from the same allocation. The
/// question is whether that is worth restructuring the key for.
///
/// It cannot be measured through the public surface. `DedupWindow` is
/// `pub(crate)` and its only public path is a negotiated connection receiving
/// DATA, where a loopback message costs microseconds and would bury the
/// answer — so this measures the two *shapes* as pure functions, the way
/// `bench_filters` measures the byte prefix. What the numbers price is
/// the difference between the shapes, not weida's code:
///
/// * **owned** — the library's shape: one flat `HashMap<Identity, _>` whose key
///   owns its scope, so every probe allocates.
/// * **borrowed** — the candidate: `HashMap<Box<str>, HashMap<(producer,
///   sequence), _>>`, where the outer lookup borrows `&str` (`Box<str>`
///   borrows as `str`) and the inner key is `Copy`, so nothing allocates
///   except a scope seen for the first time.
fn bench_dedup_key(c: &mut Criterion) {
    use std::collections::HashMap;

    /// The count cap `Limits::max_dedup_entries` ships with, so the tables are
    /// as full as they are ever allowed to get.
    const ENTRIES: usize = 4096;
    /// Scopes a receiver is plausibly tracking at once: a handful of paths or
    /// topics, each with many sequence numbers behind it.
    const SCOPES: usize = 8;

    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Owned {
        producer: Option<[u8; 32]>,
        scope: Box<str>,
        sequence: u64,
    }

    let scopes: Vec<String> = (0..SCOPES).map(|i| format!("/md/instrument-{i}")).collect();
    let producer = Some([7u8; 32]);

    /// The candidate's inner table: producer and sequence are both `Copy`, so
    /// a probe borrows everything and allocates nothing.
    type BySequence = HashMap<(Option<[u8; 32]>, u64), u64>;

    let mut owned: HashMap<Owned, u64> = HashMap::new();
    let mut borrowed: HashMap<Box<str>, BySequence> = HashMap::new();
    for i in 0..ENTRIES {
        let scope = &scopes[i % SCOPES];
        let sequence = i as u64;
        owned.insert(
            Owned {
                producer,
                scope: scope.as_str().into(),
                sequence,
            },
            sequence,
        );
        borrowed
            .entry(scope.as_str().into())
            .or_default()
            .insert((producer, sequence), sequence);
    }

    // A number that is present and one that is not: the hit path and the miss
    // path differ, and the allocation is paid on both.
    let present = (ENTRIES / 2) as u64;
    let absent = ENTRIES as u64 * 3;
    let scope = scopes[(present as usize) % SCOPES].as_str();

    let mut group = c.benchmark_group("dedup_key");
    group.bench_function("owned_hit", |b| {
        b.iter(|| {
            let key = Owned {
                producer,
                scope: black_box(scope).into(),
                sequence: black_box(present),
            };
            black_box(owned.contains_key(&key))
        })
    });
    group.bench_function("owned_miss", |b| {
        b.iter(|| {
            let key = Owned {
                producer,
                scope: black_box(scope).into(),
                sequence: black_box(absent),
            };
            black_box(owned.contains_key(&key))
        })
    });
    // The allocation on its own: the same flat table probed with a key built
    // once. The difference against `owned_*` is what `scope.into()` costs and
    // nothing else.
    let prebuilt_hit = Owned {
        producer,
        scope: scope.into(),
        sequence: present,
    };
    let prebuilt_miss = Owned {
        producer,
        scope: scope.into(),
        sequence: absent,
    };
    group.bench_function("prebuilt_hit", |b| {
        b.iter(|| black_box(owned.contains_key(black_box(&prebuilt_hit))))
    });
    group.bench_function("prebuilt_miss", |b| {
        b.iter(|| black_box(owned.contains_key(black_box(&prebuilt_miss))))
    });
    group.bench_function("borrowed_hit", |b| {
        b.iter(|| {
            black_box(borrowed.get(black_box(scope)).is_some_and(|by_sequence| {
                by_sequence.contains_key(&(producer, black_box(present)))
            }))
        })
    });
    group.bench_function("borrowed_miss", |b| {
        b.iter(|| {
            black_box(borrowed.get(black_box(scope)).is_some_and(|by_sequence| {
                by_sequence.contains_key(&(producer, black_box(absent)))
            }))
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_push,
    bench_fanout,
    bench_filters,
    bench_header_cost,
    bench_dedup_key
);
criterion_main!(benches);
