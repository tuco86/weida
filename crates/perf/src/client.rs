//! `weida-perf client`: runs the cases against a server and prints the
//! tables.

use std::error::Error;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, timeout};
use weida::{
    Bits, Blob, BlobId, ConnectionStats, FetchOptions, JoinTerms, PieceSize, Received, Runtime,
    RuntimeConfig, SegmentTerms, TransferMeta, Trust,
};

use crate::frames::{Parser, encode};
use crate::stats::percentile;
use crate::{MIB, PORT_QUINN, PORT_SWARM_EXTRA, PORT_TCP, PORT_WEIDA, cpu_times, now_us, pattern};

type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Each case's deadline.
const CASE_DEADLINE: Duration = Duration::from_secs(120);

/// Video frames per second and frames per GOP.
const FPS: u64 = 60;
const GOP: u64 = 60;

/// The `max_age` of the video segments and of the viewer's join.
const VIDEO_MAX_AGE: Duration = Duration::from_secs(3);

pub struct Options {
    pub server: String,
    pub only: Option<Vec<String>>,
    pub mib: u64,
    pub swarm_depth: Option<(usize, usize)>,
    pub video_mbit: Vec<u64>,
    pub video_secs: u64,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            server: String::new(),
            only: None,
            mib: 1024,
            swarm_depth: None,
            video_mbit: vec![8, 25, 50],
            video_secs: 20,
        }
    }
}

impl Options {
    fn selected(&self, case: &str) -> bool {
        self.only
            .as_ref()
            .is_none_or(|only| only.iter().any(|c| c == case))
    }

    fn url(&self, port: u16, path: &str) -> String {
        format!(
            "weida://{}@{}:{port}{path}",
            crate::fingerprint(),
            self.server
        )
    }

    async fn addr(&self, port: u16) -> Res<SocketAddr> {
        tokio::net::lookup_host((self.server.as_str(), port))
            .await?
            .next()
            .ok_or_else(|| format!("{} does not resolve", self.server).into())
    }

    fn bytes(&self) -> u64 {
        self.mib * MIB as u64
    }
}

/// What a throughput case measured.
struct Outcome {
    bytes: u64,
    secs: f64,
    rtt: Option<Duration>,
    lost: Option<u64>,
}

impl Outcome {
    fn with_stats(bytes: u64, secs: f64, stats: &[ConnectionStats]) -> Outcome {
        let first = stats.first();
        let transport = first.and_then(|s| s.transport.as_ref());
        Outcome {
            bytes,
            secs,
            rtt: transport.map(|t| t.path.rtt),
            lost: transport.map(|t| {
                t.path.lost_packets
                    + first
                        .and_then(|s| s.remote.as_ref())
                        .map_or(0, |r| r.lost_packets)
            }),
        }
    }
}

fn trust() -> Trust {
    Trust::by_address()
}

fn fmt_opt<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_owned(), |v| v.to_string())
}

pub async fn run(options: Arc<Options>) -> Result<(), Box<dyn Error>> {
    println!("| case | MiB | s | MB/s | rtt ms | lost pkts | user s | sys s |");
    println!("|---|---|---|---|---|---|---|---|");
    let cases: [&str; 10] = [
        "tcp-up",
        "tcp-down",
        "quinn-up",
        "quinn-down",
        "push-1",
        "push-8",
        "pull-1",
        "pull-8",
        "swarm-1",
        "swarm-4",
    ];
    for case in cases {
        if !options.selected(case) {
            continue;
        }
        let before = cpu_times();
        let result = timeout(CASE_DEADLINE, throughput(&options, case)).await;
        let cpu = before.zip(cpu_times()).map(|(b, a)| (a.0 - b.0, a.1 - b.1));
        let (user, sys) = match cpu {
            Some((u, s)) => (format!("{u:.2}"), format!("{s:.2}")),
            None => ("-".into(), "-".into()),
        };
        match result {
            Ok(Ok(o)) => println!(
                "| {case} | {:.0} | {:.2} | {:.1} | {} | {} | {user} | {sys} |",
                o.bytes as f64 / MIB as f64,
                o.secs,
                o.bytes as f64 / 1e6 / o.secs,
                fmt_opt(o.rtt.map(|r| format!("{:.2}", r.as_secs_f64() * 1e3))),
                fmt_opt(o.lost),
            ),
            Ok(Err(e)) => println!("| {case} | ERROR {e} | | | | | {user} | {sys} |"),
            Err(_) => println!("| {case} | TIMEOUT | | | | | {user} | {sys} |"),
        }
        let _ = std::io::stdout().flush();
    }

    let rows = [
        ("video", Competitor::None),
        ("video+push-same", Competitor::PushSame),
        ("video+pull-other", Competitor::PullOther),
    ];
    if rows.iter().any(|(name, _)| options.selected(name)) {
        println!();
        println!(
            "| row | Mbit | frames sent | frames recv | p50 ms | p95 ms | p99 ms | max ms | stale | overflow | uplink drops |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|---|");
    }
    for (name, competitor) in rows {
        if !options.selected(name) {
            continue;
        }
        for &mbit in &options.video_mbit {
            let deadline = CASE_DEADLINE + Duration::from_secs(options.video_secs);
            match timeout(deadline, video(&options, mbit, competitor)).await {
                Ok(Ok(r)) => println!(
                    "| {name} | {mbit} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {} | {} | {} |",
                    r.sent,
                    r.received,
                    r.p50 as f64 / 1e3,
                    r.p95 as f64 / 1e3,
                    r.p99 as f64 / 1e3,
                    r.max as f64 / 1e3,
                    r.stale,
                    r.overflow,
                    r.drops,
                ),
                Ok(Err(e)) => println!("| {name} | {mbit} | ERROR {e} | | | | | | | | |"),
                Err(_) => println!("| {name} | {mbit} | TIMEOUT | | | | | | | | |"),
            }
            let _ = std::io::stdout().flush();
        }
    }
    Ok(())
}

async fn throughput(o: &Options, case: &str) -> Res<Outcome> {
    let len = o.bytes();
    match case {
        "tcp-up" | "tcp-down" => {
            let addr = o.addr(PORT_TCP).await?;
            let started = Instant::now();
            crate::raw::tcp_transfer(addr, case == "tcp-up", len).await?;
            Ok(Outcome {
                bytes: len,
                secs: started.elapsed().as_secs_f64(),
                rtt: None,
                lost: None,
            })
        }
        "quinn-up" | "quinn-down" => {
            let addr = o.addr(PORT_QUINN).await?;
            let started = Instant::now();
            let m = crate::raw::quinn_transfer(addr, case == "quinn-up", len).await?;
            Ok(Outcome {
                bytes: len,
                secs: started.elapsed().as_secs_f64(),
                rtt: m.rtt,
                lost: m.lost,
            })
        }
        "push-1" | "push-8" | "pull-1" | "pull-8" | "swarm-1" | "swarm-4" => {
            // Every weida case on a runtime of its own, so cases share no
            // pooled connection.
            let runtime = Runtime::new(RuntimeConfig::default())?;
            let result = match case {
                "push-1" => push(&runtime, o, 1).await,
                "push-8" => push(&runtime, o, 8).await,
                "pull-1" => pull(&runtime, o, 1).await,
                "pull-8" => pull(&runtime, o, 8).await,
                "swarm-1" => swarm(&runtime, o, 1).await,
                _ => swarm(&runtime, o, 4).await,
            };
            runtime.shutdown().await;
            result
        }
        _ => Err(format!("unknown case {case}").into()),
    }
}

/// Pushes `o.mib` MiB in `n` concurrent transfers over one pusher; the time
/// runs from the first open to the last delivery.
async fn push(rt: &Runtime, o: &Options, n: u64) -> Res<Outcome> {
    let pusher = Arc::new(rt.pusher(trust()));
    pusher.connect(&o.url(PORT_WEIDA, "/sink")).await?;
    let per = o.mib / n;
    let started = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..n {
        let pusher = Arc::clone(&pusher);
        set.spawn(async move {
            let mut transfer = pusher.open(TransferMeta::default()).await?;
            let chunk = pattern();
            for _ in 0..per {
                transfer.write_all(&chunk).await?;
            }
            transfer.finish()?.delivered().await?;
            Ok::<_, weida::Error>(())
        });
    }
    while let Some(done) = set.join_next().await {
        done??;
    }
    let secs = started.elapsed().as_secs_f64();
    Ok(Outcome::with_stats(
        per * n * MIB as u64,
        secs,
        &pusher.connection_stats(),
    ))
}

/// Requests `o.mib` MiB from `/source` in `n` concurrent requests.
async fn pull(rt: &Runtime, o: &Options, n: u64) -> Res<Outcome> {
    let requester = Arc::new(rt.requester(trust()));
    requester.connect(&o.url(PORT_WEIDA, "/source")).await?;
    let per = o.mib / n * MIB as u64;
    let started = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..n {
        let requester = Arc::clone(&requester);
        set.spawn(async move {
            let mut reply = requester.request(&per.to_be_bytes()).await?;
            let mut buf = Vec::with_capacity(MIB);
            let mut got = 0u64;
            loop {
                buf.clear();
                match reply.read_buf(&mut buf).await? {
                    0 => break,
                    n => got += n as u64,
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(got)
        });
    }
    let mut total = 0;
    while let Some(got) = set.join_next().await {
        total += got??;
    }
    let secs = started.elapsed().as_secs_f64();
    if total != per * n {
        return Err(format!("pulled {total} of {} bytes", per * n).into());
    }
    Ok(Outcome::with_stats(
        total,
        secs,
        &requester.connection_stats(),
    ))
}

/// Fetches the server's blob from `seeders` of its four SWARM bindings.
async fn swarm(rt: &Runtime, o: &Options, seeders: u16) -> Res<Outcome> {
    let requester = rt.requester(trust());
    requester.connect(&o.url(PORT_WEIDA, "/blob")).await?;
    let mut reply = requester.request(&[]).await?;
    let mut described = Vec::new();
    reply.read_to_end(&mut described).await?;
    if described.len() != 41 {
        return Err(format!("/blob answered {} bytes", described.len()).into());
    }
    let root = BlobId::from_bytes(described[..32].try_into()?);
    let size = u64::from_be_bytes(described[32..40].try_into()?);
    let piece = PieceSize::new(1u64 << described[40]).ok_or("bad piece size")?;
    let blob = Blob::new(root, size, piece).ok_or("bad blob")?;

    let swarm = rt.swarm(trust());
    swarm.connect(&o.url(PORT_WEIDA, "/swarm")).await?;
    for i in 1..seeders {
        swarm
            .connect(&o.url(PORT_SWARM_EXTRA + i - 1, "/swarm"))
            .await?;
    }
    let mut options = FetchOptions::default();
    if let Some((per_peer, max_inflight)) = o.swarm_depth {
        options.per_peer = per_peer;
        options.max_inflight = max_inflight;
    }
    let started = Instant::now();
    let mut fetch = swarm.fetch(blob, Bits::new(blob.count()), options);
    let mut total = 0u64;
    while let Some(piece) = fetch.next().await {
        total += piece?.bytes.len() as u64;
    }
    let secs = started.elapsed().as_secs_f64();
    if total != size {
        return Err(format!("fetched {total} of {size} bytes").into());
    }
    Ok(Outcome::with_stats(total, secs, &swarm.connection_stats()))
}

#[derive(Clone, Copy)]
enum Competitor {
    None,
    /// `push-1` on the uplink's runtime, sharing its pooled connection.
    PushSame,
    /// `pull-1` on a third runtime, beside the viewer's downlink.
    PullOther,
}

struct VideoRow {
    sent: u64,
    received: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    stale: u64,
    overflow: u64,
    drops: u64,
}

/// What the viewer's readers collect.
#[derive(Default)]
struct Seen {
    received: AtomicU64,
    latencies: Mutex<Vec<u64>>,
}

/// One video row at `mbit`: GOP segments from an uplink peer through the
/// server's relay to a dish in this process.
async fn video(o: &Options, mbit: u64, competitor: Competitor) -> Res<VideoRow> {
    let uplink = Runtime::new(RuntimeConfig::default())?;
    let peer = uplink.peer(trust());
    peer.connect(&o.url(PORT_WEIDA, "/up")).await?;

    let viewer = Runtime::new(RuntimeConfig::default())?;
    let dish = Arc::new(viewer.dish(trust()));
    dish.join("v", JoinTerms::default().with_max_age(VIDEO_MAX_AGE))
        .await?;
    dish.connect(&o.url(PORT_WEIDA, "/live")).await?;

    // Frames sent at or after this time count: the first GOP is excluded.
    let threshold = Arc::new(AtomicU64::new(u64::MAX));
    let seen = Arc::new(Seen::default());
    let receiver = {
        let dish = Arc::clone(&dish);
        let threshold = Arc::clone(&threshold);
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok(received) = dish.recv().await {
                let Received::Segment(mut transfer) = received else {
                    continue;
                };
                let threshold = Arc::clone(&threshold);
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let mut parser = Parser::default();
                    let mut buf = Vec::with_capacity(64 * 1024);
                    loop {
                        buf.clear();
                        match transfer.read_buf(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        let now = now_us();
                        let from = threshold.load(Ordering::Relaxed);
                        parser.feed(&buf, |send_us, _| {
                            if send_us >= from {
                                seen.received.fetch_add(1, Ordering::Relaxed);
                                seen.latencies
                                    .lock()
                                    .expect("latencies")
                                    .push(now.saturating_sub(send_us));
                            }
                        });
                    }
                });
            }
        })
    };

    let third = match competitor {
        Competitor::PullOther => Some(Runtime::new(RuntimeConfig::default())?),
        _ => None,
    };
    let load = match competitor {
        Competitor::None => None,
        Competitor::PushSame => {
            let pusher = Arc::new(uplink.pusher(trust()));
            pusher.connect(&o.url(PORT_WEIDA, "/sink")).await?;
            let mib = o.mib;
            Some(tokio::spawn(async move {
                let mut transfer = pusher.open(TransferMeta::default()).await?;
                let chunk = pattern();
                for _ in 0..mib {
                    transfer.write_all(&chunk).await?;
                }
                transfer.finish()?.delivered().await?;
                Ok::<_, weida::Error>(())
            }))
        }
        Competitor::PullOther => {
            let requester = Arc::new(third.as_ref().expect("third runtime").requester(trust()));
            requester.connect(&o.url(PORT_WEIDA, "/source")).await?;
            let len = o.bytes();
            Some(tokio::spawn(async move {
                let mut reply = requester.request(&len.to_be_bytes()).await?;
                let mut buf = Vec::with_capacity(MIB);
                loop {
                    buf.clear();
                    if reply.read_buf(&mut buf).await.map_err(weida::Error::from)? == 0 {
                        return Ok(());
                    }
                }
            }))
        }
    };

    // Bytes per GOP, a P-frame, and the I-frame that opens each GOP.
    let gop_bytes = mbit * 1_000_000 / 8;
    let p_frame = (gop_bytes / 67) as usize;
    let i_frame = 8 * p_frame;
    let frames = o.video_secs * FPS;
    let mut tick = interval(Duration::from_micros(1_000_000 / FPS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
    let mut segment = None;
    let mut sent = 0u64;
    let mut frame = Vec::with_capacity(i_frame);
    for f in 0..frames {
        tick.tick().await;
        let first = f % GOP == 0;
        if first {
            if let Some(previous) = segment.take() {
                weida::Segment::finish(previous);
            }
            if f == GOP {
                threshold.store(now_us(), Ordering::Relaxed);
            }
            let terms = SegmentTerms::default().with_max_age(VIDEO_MAX_AGE);
            segment = Some(peer.segment("v", terms).await?);
        }
        frame.clear();
        encode(now_us(), if first { i_frame } else { p_frame }, &mut frame);
        if f >= GOP {
            sent += 1;
        }
        if let Some(segment) = segment.as_mut() {
            segment.write(Bytes::copy_from_slice(&frame))?;
        }
    }
    if let Some(last) = segment.take() {
        last.finish();
    }

    // Let the last frames arrive: until every counted frame did, or the
    // segments' max age passed.
    let waited = Instant::now();
    while seen.received.load(Ordering::Relaxed) < sent && waited.elapsed() < VIDEO_MAX_AGE {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(load) = load {
        load.abort();
    }
    receiver.abort();

    let mut latencies = std::mem::take(&mut *seen.latencies.lock().expect("latencies"));
    latencies.sort_unstable();
    let row = VideoRow {
        sent,
        received: seen.received.load(Ordering::Relaxed),
        p50: percentile(&latencies, 50),
        p95: percentile(&latencies, 95),
        p99: percentile(&latencies, 99),
        max: latencies.last().copied().unwrap_or(0),
        stale: dish.stale(),
        overflow: dish.overflow(),
        drops: peer.segment_drops("v").map_or(0, |d| d.total()),
    };
    drop(peer);
    drop(dish);
    uplink.shutdown().await;
    viewer.shutdown().await;
    if let Some(third) = third {
        third.shutdown().await;
    }
    Ok(row)
}
