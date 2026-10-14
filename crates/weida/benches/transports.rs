//! What the three transports cost: the same patterns over QUIC, inproc and `AF_UNIX`.
//!
//! [Decision 0010](../../../docs/decisions/0010-local-transport.md) §4.2 chose
//! "the OS connection **is** the stream, one per transfer" on a structural
//! argument — a multiplexer would reimplement QUIC's stream layer in the one
//! place the project set out not to — and put no number beside it. This bench
//! is that number: what a local transfer costs against a loopback QUIC one, at
//! a payload where the overhead dominates and at one where the copies do, plus
//! what a local connection costs in memory against the ~995 KiB per QUIC
//! connection that `docs/IMPLEMENTATION.md` §4.9 measures.
//!
//! The honest row is the **round trip**. A one-way send returns when the
//! transport has taken the bytes, which locally means a kernel buffer or a
//! channel slot, so a one-way number compares enqueue rates and not delivery
//! (the same reading applies to the ZMTP bench). Req/Rep is
//! therefore the comparison, and Push/Pull is reported beside it with that
//! caveat rather than left out.

#[cfg(unix)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{
    ClientTls, Identity, Listener, Puller, Runtime, RuntimeConfig, ServerTls, TransferMeta, Trust,
};

/// The two payload sizes: one where the transport's overhead is the whole
/// figure, one where moving the bytes is.
const SIZES: [usize; 2] = [1024, 1024 * 1024];

/// Local connections held live for the memory figure. Just under
/// `max_local_streams` (255), since every live transfer is one of them, and
/// as close to it as possible because the delta is a handful of KiB against
/// a page-granular RSS.
const LOCAL_CONNECTIONS: usize = 200;

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Resident set size in bytes, on Linux. `VmRSS`, as in `connections.rs`:
/// what a set of live connections holds *now*.
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

/// Which transport a row runs over.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Quic,
    Inproc,
    #[cfg(unix)]
    Unix,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Quic => "quic",
            Kind::Inproc => "inproc",
            #[cfg(unix)]
            Kind::Unix => "unix",
        }
    }
}

/// A server on one transport, plus what a client needs to reach it.
struct Server {
    runtime: Runtime,
    listener: Listener,
    url_base: String,
    trust: Trust,
    _quic: Option<weida::Binding>,
    _inproc: Option<weida::LocalBinding>,
    #[cfg(unix)]
    _unix: Option<(weida::UnixBinding, PathBuf)>,
}

impl Server {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.url_base, path)
    }

    /// A client runtime; the trust terms come from the server.
    fn client(&self) -> Runtime {
        Runtime::new(RuntimeConfig::default()).expect("client runtime")
    }
}

/// A unique local name per bench run, so repeated runs in one process do not
/// collide on a bus name or a socket path.
fn local_name(prefix: &str) -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

async fn server(kind: Kind) -> Server {
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    match kind {
        Kind::Quic => {
            let identity = Identity::generate().expect("identity");
            let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));
            let binding = listener
                .bind_quic(
                    "127.0.0.1:0".parse().expect("loopback"),
                    ServerTls::new(identity),
                )
                .await
                .expect("bind quic");
            let url_base = format!("weida://127.0.0.1:{}", binding.local_addr().port());
            Server {
                runtime,
                listener,
                url_base,
                trust,
                _quic: Some(binding),
                _inproc: None,
                #[cfg(unix)]
                _unix: None,
            }
        }
        Kind::Inproc => {
            let bus = local_name("weida-bench");
            let binding = listener.bind_inproc(&bus).expect("bind inproc");
            Server {
                runtime,
                listener,
                url_base: format!("weida+inproc://{bus}"),
                trust: Trust::by_address(),
                _quic: None,
                _inproc: Some(binding),
                #[cfg(unix)]
                _unix: None,
            }
        }
        #[cfg(unix)]
        Kind::Unix => {
            let path = std::env::temp_dir().join(local_name("weida-bench.sock"));
            let binding = listener.bind_unix(&path).expect("bind unix");
            // The path goes into the URL percent-encoded, which is what the
            // address form requires of anything containing a separator.
            let encoded: String = path
                .to_str()
                .expect("utf-8 path")
                .bytes()
                .map(|b| match b {
                    b'/' => "%2F".to_string(),
                    b => (b as char).to_string(),
                })
                .collect();
            Server {
                runtime,
                listener,
                url_base: format!("weida+unix://{encoded}"),
                trust: Trust::by_address(),
                _quic: None,
                _inproc: None,
                _unix: Some((binding, path)),
            }
        }
    }
}

/// One exchange, echoed: the responder every Req/Rep row measures against.
fn echo(server: &Server) {
    let replier = server.listener.replier("/echo").expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = match request.take_body().collect(8 * 1024 * 1024).await {
                Ok(body) => body,
                Err(_) => continue,
            };
            let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                continue;
            };
            if reply.write_all(&body).await.is_ok() {
                let _ = reply.finish();
            }
        }
    });
}

fn drain(puller: Puller) {
    tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(8 * 1024 * 1024).await;
        }
    });
}

/// Req/Rep round trip over each transport: the comparison that means
/// something, because it is bounded by the far side reading and answering.
fn bench_round_trip(c: &mut Criterion) {
    let rt = tokio_runtime();
    let mut group = c.benchmark_group("transport_req_rep");

    let kinds: &[Kind] = &[
        Kind::Quic,
        Kind::Inproc,
        #[cfg(unix)]
        Kind::Unix,
    ];

    for &kind in kinds {
        let (server, requester) = rt.block_on(async {
            let server = server(kind).await;
            echo(&server);
            let client = server.client();
            let requester = client.requester(ClientTls::new(server.trust.clone()));
            requester
                .connect(&server.url("/echo"))
                .await
                .expect("connect");
            // One exchange before measuring: the QUIC row would otherwise
            // include a TLS handshake in its first sample.
            let reply = requester.request(b"warm").await.expect("warm");
            reply.collect(64).await.expect("warm body");
            (server, (client, requester))
        });
        let (client, requester) = requester;

        for size in SIZES {
            let payload = vec![0x61u8; size];
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_function(format!("{}_{size}", kind.name()), |b| {
                b.to_async(&rt).iter(|| async {
                    let reply = requester
                        .request(black_box(&payload))
                        .await
                        .expect("request");
                    reply.collect(2 * 1024 * 1024).await.expect("reply body");
                })
            });
        }

        rt.block_on(async {
            client.shutdown().await;
            server.runtime.clone().shutdown().await;
        });
    }
    group.finish();
}

/// Push one-way over each transport. Reported with the caveat in the module
/// docs: what returns is the transport taking the bytes, not a peer reading
/// them, so this compares enqueue rates.
fn bench_one_way(c: &mut Criterion) {
    let rt = tokio_runtime();
    let mut group = c.benchmark_group("transport_push");

    let kinds: &[Kind] = &[
        Kind::Quic,
        Kind::Inproc,
        #[cfg(unix)]
        Kind::Unix,
    ];

    for &kind in kinds {
        let (server, pair) = rt.block_on(async {
            let server = server(kind).await;
            drain(server.listener.puller("/sink").expect("puller"));
            let client = server.client();
            let pusher = client.pusher(ClientTls::new(server.trust.clone()));
            pusher.connect(&server.url("/sink")).await.expect("connect");
            pusher.send(b"warm").await.expect("warm");
            (server, (client, pusher))
        });
        let (client, pusher) = pair;

        for size in SIZES {
            let payload = vec![0x61u8; size];
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_function(format!("{}_{size}", kind.name()), |b| {
                b.to_async(&rt)
                    .iter(|| async { pusher.send(black_box(&payload)).await.expect("send") })
            });
        }

        rt.block_on(async {
            client.shutdown().await;
            server.runtime.clone().shutdown().await;
        });
    }
    group.finish();
}

/// What a *live local connection* costs, against the ~995 KiB per QUIC
/// connection of `docs/IMPLEMENTATION.md` §4.9.
///
/// Locally a connection is not a pool entry a client reuses: one transfer is
/// one connection ([0010 §4.2]), so the figure that matters is what holding N
/// of them costs — which is exactly what a local peer with N transfers in
/// flight pays. Both ends are in this process, as in `connections.rs`, so the
/// delta covers the accepting side too.
///
/// Not a criterion row: this prints a report, because a memory figure has no
/// per-iteration meaning.
fn report_memory(_c: &mut Criterion) {
    let Some(_) = current_rss() else {
        eprintln!("transport memory: /proc unavailable, skipped");
        return;
    };
    let rt = tokio_runtime();

    let kinds: &[Kind] = &[
        Kind::Inproc,
        #[cfg(unix)]
        Kind::Unix,
    ];

    println!("\n== live local connections, RSS delta over {LOCAL_CONNECTIONS} ==");
    for &kind in kinds {
        rt.block_on(async {
            let server = server(kind).await;
            // A replier that accepts and holds: the request stays open, so the
            // connection behind it stays live for the measurement.
            let replier = server.listener.replier("/hold").expect("replier");
            let held = tokio::spawn(async move {
                let mut requests = Vec::new();
                while let Ok(request) = replier.accept().await {
                    requests.push(request);
                }
                requests.len()
            });

            let client = server.client();
            let requester = client.requester(ClientTls::new(server.trust.clone()));
            requester
                .connect(&server.url("/hold"))
                .await
                .expect("connect");

            let before = current_rss().expect("rss");
            let started = Instant::now();
            let mut open = Vec::new();
            for _ in 0..LOCAL_CONNECTIONS {
                let (mut request, reply) = requester
                    .open(TransferMeta::default())
                    .await
                    .expect("open a transfer");
                request.write_all(b"hold").await.expect("write");
                // Deliberately unfinished: the transfer, and therefore the
                // local connection under it, stays live.
                open.push((request, reply));
            }
            let elapsed = started.elapsed();
            let after = current_rss().expect("rss");

            let delta = after.saturating_sub(before);
            println!(
                "{:>6}: {:>8} B per live transfer connection ({} held, {} B in total, \
                 {:?} to open them, {:.3} ms each)",
                kind.name(),
                delta / LOCAL_CONNECTIONS as u64,
                LOCAL_CONNECTIONS,
                delta,
                elapsed,
                elapsed.as_secs_f64() * 1000.0 / LOCAL_CONNECTIONS as f64,
            );

            drop(open);
            client.shutdown().await;
            server.runtime.clone().shutdown().await;
            // Aborted rather than awaited: the accept loop ends when its
            // binding goes, and this one's outlives the runtime in `server`.
            held.abort();
        });
    }
}

criterion_group!(benches, bench_round_trip, bench_one_way, report_memory);
criterion_main!(benches);
