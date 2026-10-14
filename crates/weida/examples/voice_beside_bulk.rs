//! What a bulk upload to the same host costs a voice flow, by path and
//! by congestion controller
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §6).
//!
//! An in-process UDP shaper sits between one client runtime and one server:
//! client to server is a token bucket at 20 Mbit/s feeding a drop-tail queue
//! of 64 KiB, server to client is not shaped — an upload over a slow uplink,
//! the case griasdi's voice meets at home. Through it run a voice flow of
//! 200-byte datagrams at 50 Hz and back-to-back 1 MiB bulk transfers, in four
//! configurations: voice and bulk on separate paths (two connections, two
//! controllers) or on one path (one connection), each with the client's bulk
//! and voice connections on CUBIC or on BBR. Every voice datagram carries its
//! send time, so the receiver measures one-way latency on one clock.
//!
//! ```sh
//! cargo run --release -p weida --example voice_beside_bulk
//! ```

use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify};
use weida::{
    Congestion, DEFAULT_DATAGRAM_RECEIVE_BYTES, FlowMeta, Identity, Incoming, Limits, Runtime,
    RuntimeConfig, ServerTls, TransferMeta, Trust,
};

/// Client-to-server rate of the bottleneck.
const RATE_BITS: u64 = 20_000_000;
/// Drop-tail queue in front of it.
const QUEUE_BYTES: usize = 64 * 1024;
/// How long each configuration runs.
const RUN: Duration = Duration::from_secs(20);
/// Voice: one 200-byte datagram every 20 ms.
const VOICE_BYTES: usize = 200;
const VOICE_EVERY: Duration = Duration::from_millis(20);
/// One bulk transfer.
const BULK_BYTES: usize = 1 << 20;
/// Queued packets with the client each came from, and their byte total.
type Queue = (VecDeque<(SocketAddr, Vec<u8>)>, usize);

/// The client-to-server queue of the shaper.
struct Bottleneck {
    queue: Mutex<Queue>,
    ready: Notify,
    dropped: AtomicU64,
}

/// Starts the shaper in front of `server`; returns the address clients dial.
async fn shaper(server: SocketAddr) -> std::io::Result<(SocketAddr, Arc<Bottleneck>)> {
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let front_addr = front.local_addr()?;
    let bottleneck = Arc::new(Bottleneck {
        queue: Mutex::new((VecDeque::new(), 0)),
        ready: Notify::new(),
        dropped: AtomicU64::new(0),
    });
    let backs: Arc<Mutex<HashMap<SocketAddr, Arc<UdpSocket>>>> = Arc::default();

    // Ingress: every client packet joins the queue, or is dropped at its tail.
    {
        let (front, bottleneck) = (Arc::clone(&front), Arc::clone(&bottleneck));
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            while let Ok((n, from)) = front.recv_from(&mut buf).await {
                let mut queue = bottleneck.queue.lock().await;
                if queue.1 + n > QUEUE_BYTES {
                    bottleneck.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                queue.1 += n;
                queue.0.push_back((from, buf[..n].to_vec()));
                drop(queue);
                bottleneck.ready.notify_one();
            }
        });
    }
    // Egress: the token bucket, one packet at a time at the link rate. Each
    // client address gets its own socket toward the server, and the server's
    // answers go back unshaped.
    {
        let (front, bottleneck) = (Arc::clone(&front), Arc::clone(&bottleneck));
        tokio::spawn(async move {
            let mut next = tokio::time::Instant::now();
            loop {
                let item = bottleneck.queue.lock().await.0.pop_front();
                let Some((from, packet)) = item else {
                    bottleneck.ready.notified().await;
                    continue;
                };
                bottleneck.queue.lock().await.1 -= packet.len();
                let now = tokio::time::Instant::now();
                if next < now {
                    next = now;
                }
                tokio::time::sleep_until(next).await;
                next += Duration::from_nanos(packet.len() as u64 * 8 * 1_000_000_000 / RATE_BITS);
                let back = {
                    let mut backs = backs.lock().await;
                    match backs.get(&from) {
                        Some(back) => Arc::clone(back),
                        None => {
                            let back =
                                Arc::new(UdpSocket::bind("127.0.0.1:0").await.expect("bind"));
                            back.connect(server).await.expect("connect");
                            backs.insert(from, Arc::clone(&back));
                            let (front, reply) = (Arc::clone(&front), Arc::clone(&back));
                            tokio::spawn(async move {
                                let mut buf = vec![0u8; 65_536];
                                while let Ok(n) = reply.recv(&mut buf).await {
                                    let _ = front.send_to(&buf[..n], from).await;
                                }
                            });
                            back
                        }
                    }
                };
                let _ = back.send(&packet).await;
            }
        });
    }
    Ok((front_addr, bottleneck))
}

/// One configuration's outcome.
struct Row {
    shape: &'static str,
    congestion: Congestion,
    latencies: Vec<Duration>,
    sent: u64,
    bulk_mib: u64,
    queue_drops: u64,
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

async fn run(shared_path: bool, congestion: Congestion) -> weida::Result<Row> {
    let flows = Limits {
        datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
        ..Limits::default()
    };
    let server = Runtime::new(RuntimeConfig {
        limits: flows,
        ..RuntimeConfig::default()
    })?;
    let listener = server.listener();
    let identity = Identity::generate_for(["localhost", "127.0.0.1"])?;
    let fingerprint = identity.fingerprint()?;
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity),
        )
        .await?;
    let (front, bottleneck) = shaper(binding.local_addr()).await?;
    let url = |path: &str| format!("weida://{fingerprint}@{front}{path}");
    let (voice_path, bulk_path) = if shared_path {
        ("/mixed", "/mixed")
    } else {
        ("/voice", "/bulk")
    };

    // The server side: every flow's datagrams are timed, every stream is
    // drained and counted.
    let start = Instant::now();
    let latencies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let bulk_bytes = Arc::new(AtomicU64::new(0));
    for path in if shared_path {
        vec!["/mixed"]
    } else {
        vec!["/voice", "/bulk"]
    } {
        let acceptor = listener.acceptor(path)?;
        let (latencies, bulk_bytes) = (Arc::clone(&latencies), Arc::clone(&bulk_bytes));
        tokio::spawn(async move {
            while let Ok(incoming) = acceptor.accept().await {
                match incoming {
                    Incoming::Flow(flow) => {
                        let latencies = Arc::clone(&latencies);
                        tokio::spawn(async move {
                            while let Some(datagram) = flow.recv().await {
                                let sent = u64::from_be_bytes(
                                    datagram[..8].try_into().expect("timestamp"),
                                );
                                let now = start.elapsed().as_nanos() as u64;
                                latencies
                                    .lock()
                                    .expect("latencies")
                                    .push(Duration::from_nanos(now.saturating_sub(sent)));
                            }
                        });
                    }
                    Incoming::Stream(transfer) => {
                        let bulk_bytes = Arc::clone(&bulk_bytes);
                        tokio::spawn(async move {
                            if let Ok(body) = transfer.collect(2 * BULK_BYTES).await {
                                bulk_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                            }
                        });
                    }
                    _ => {}
                }
            }
        });
    }

    let client = Runtime::new(RuntimeConfig {
        limits: Limits {
            congestion,
            ..flows
        },
        ..RuntimeConfig::default()
    })?;
    let peer = client.peer(Trust::by_address());
    peer.connect(&url(voice_path)).await?;
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&url(bulk_path)).await?;
    let flow = peer.open_flow(FlowMeta::default()).await?;

    let bulk = tokio::spawn(async move {
        let body = vec![0x5a; BULK_BYTES];
        while start.elapsed() < RUN {
            let Ok(mut transfer) = pusher.open(TransferMeta::default()).await else {
                break;
            };
            if transfer.write_all(&body).await.is_err() || transfer.finish().is_err() {
                break;
            }
        }
    });
    let mut sent = 0u64;
    let mut tick = tokio::time::interval(VOICE_EVERY);
    // Let the bulk upload fill the bottleneck before voice is measured.
    tokio::time::sleep(Duration::from_secs(2)).await;
    while start.elapsed() < RUN {
        tick.tick().await;
        let mut frame = vec![0u8; VOICE_BYTES];
        frame[..8].copy_from_slice(&(start.elapsed().as_nanos() as u64).to_be_bytes());
        if flow.send(frame).is_ok() {
            sent += 1;
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    bulk.abort();
    let mut latencies = latencies.lock().expect("latencies").clone();
    latencies.sort_unstable();
    let row = Row {
        shape: if shared_path {
            "one path"
        } else {
            "separate paths"
        },
        congestion,
        latencies,
        sent,
        bulk_mib: bulk_bytes.load(Ordering::Relaxed) >> 20,
        queue_drops: bottleneck.dropped.load(Ordering::Relaxed),
    };
    client.shutdown().await;
    server.shutdown().await;
    Ok(row)
}

#[tokio::main]
async fn main() -> weida::Result<()> {
    println!(
        "bottleneck {} Mbit/s client to server, drop-tail {} KiB; voice {VOICE_BYTES} B every {} ms; bulk {} MiB transfers back to back; {} s per run",
        RATE_BITS / 1_000_000,
        QUEUE_BYTES / 1024,
        VOICE_EVERY.as_millis(),
        BULK_BYTES >> 20,
        RUN.as_secs(),
    );
    println!(
        "| paths | controller | voice p50 | p95 | p99 | voice loss | bulk MiB | queue drops |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- |");
    for shared_path in [false, true] {
        for congestion in [Congestion::Cubic, Congestion::Bbr] {
            let row = run(shared_path, congestion).await?;
            let received = row.latencies.len() as u64;
            let loss = 100.0 * row.sent.saturating_sub(received) as f64 / row.sent.max(1) as f64;
            println!(
                "| {} | {:?} | {:.1} ms | {:.1} ms | {:.1} ms | {:.2} % | {} | {} |",
                row.shape,
                row.congestion,
                percentile(&row.latencies, 50.0).as_secs_f64() * 1e3,
                percentile(&row.latencies, 95.0).as_secs_f64() * 1e3,
                percentile(&row.latencies, 99.0).as_secs_f64() * 1e3,
                loss,
                row.bulk_mib,
                row.queue_drops,
            );
        }
    }
    Ok(())
}
