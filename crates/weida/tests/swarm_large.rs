//! One seeder, one fetch, 256 MiB through a 27 ms path
//! ([decisions/0041](../../../docs/decisions/0041-swarm-sessions.md) §4.4),
//! ignored by default.
//!
//! ```text
//! cargo test -p weida --features swarm --release --test swarm_large -- --ignored --nocapture
//! ```
//!
//! A swarm fetch from one seeder should behave like a normal download, only
//! with its pieces in any order. So the baseline is a normal download of the
//! same 256 MiB on the same path: one reply, streamed, read by a requester
//! whose receive windows are the ones a swarm dials with. The fetch must
//! reach 0.7 of it.
//!
//! Neither side ever holds the blob: the seeder generates each piece when it
//! is ordered, the piece list comes from one streaming pass over the
//! generator, and the fetching caller only counts what it is handed. Peak
//! RSS far below the payload is the evidence that the credit bounds what is
//! in flight. Its own process, because it measures the process's peak RSS.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Server, Xorshift};
use tokio::io::AsyncReadExt;
use weida::{Ask, Bits, FetchOptions, Limits, PieceList, PieceSize, TransferMeta, TreeHasher};

const PIECE: usize = 1 << 20;
const COUNT: u32 = 256;
const TOTAL: u64 = PIECE as u64 * COUNT as u64;
/// 13 ms each way: the 27 ms ping of the path griasdi measured on.
const ONE_WAY: Duration = Duration::from_millis(13);
/// Half the payload: a fetch that buffered the blob would pass it.
const RSS_LIMIT: u64 = 128 << 20;

/// Peak resident set size in bytes, on Linux.
fn peak_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Piece `index` of the blob, generated on demand.
fn piece(index: u32) -> Vec<u8> {
    let mut bytes = vec![0; PIECE];
    Xorshift::new(u64::from(index) + 1).fill(&mut bytes);
    bytes
}

/// The blob's piece list, from one pass over the generator.
fn piece_list() -> PieceList {
    let mut hasher = TreeHasher::new(PieceSize::DEFAULT);
    for index in 0..COUNT {
        hasher.update(&piece(index));
    }
    hasher.finish().expect("256 pieces fit a piece list")
}

/// MB/s (10^6 bytes per second) of `bytes` in `elapsed`.
fn mb_per_s(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / 1e6 / elapsed.as_secs_f64()
}

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "moves 512 MiB through a 27 ms path; run with --release -- --ignored --nocapture"]
    async fn a_session_fetch_keeps_up_with_a_normal_download() {
        let list = Arc::new(piece_list());
        let blob = list.blob();
        // On the OS network: the 27 ms path is `delay_proxy`, a real UDP
        // forwarder, which a server on a simulated network never answers.
        let server = Server::start_os().await;

        let seeder = server.listener.seeder("/swarm").expect("seeder");
        tokio::spawn(async move {
            while let Ok(mut session) = seeder.accept().await {
                let list = Arc::clone(&list);
                tokio::spawn(async move {
                    if session.blob() != list.blob() {
                        session.decline().await;
                        return;
                    }
                    while let Ok(Some(ask)) = session.next().await {
                        let answered = match ask {
                            Ask::List => session.answer_list(&list).await,
                            Ask::Have => session.answer_have(&Bits::full(COUNT)).await,
                            Ask::Piece(index) => session.answer_piece(&piece(index)).await,
                        };
                        if answered.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let replier = server.listener.replier("/download").expect("replier");
        tokio::spawn(async move {
            while let Ok(request) = replier.accept().await {
                let Ok(mut reply) = request
                    .reply(TransferMeta::default().with_content_len(TOTAL))
                    .await
                else {
                    continue;
                };
                for index in 0..COUNT {
                    if reply.write_all(&piece(index)).await.is_err() {
                        break;
                    }
                }
                let _ = reply.finish();
            }
        });

        let front = common::delay_proxy(server.addr, ONE_WAY).await;
        let url = |path: &str| format!("weida://127.0.0.1:{}{path}", front.port());

        // The normal download, with the windows a swarm dials with.
        let download_client = server.client_runtime_with(Limits {
            stream_receive_window: 32 << 20,
            connection_receive_window: 64 << 20,
            ..Limits::default()
        });
        let requester = download_client.requester(server.trust());
        requester.connect(&url("/download")).await.expect("connect");
        let started = Instant::now();
        let mut body = requester.request(b"").await.expect("the download");
        let mut chunk = vec![0; PIECE];
        let mut downloaded = 0u64;
        loop {
            let n = body.read(&mut chunk).await.expect("read the download");
            if n == 0 {
                break;
            }
            downloaded += n as u64;
        }
        let baseline = mb_per_s(downloaded, started.elapsed());
        assert_eq!(downloaded, TOTAL);

        // The swarm fetch, on the runtime's default limits: it raises its own
        // windows. No list given, so LIST is ordered too.
        let swarm_client = server.client_runtime();
        let swarm = swarm_client.swarm(server.trust());
        swarm.connect(&url("/swarm")).await.expect("connect");
        let options = FetchOptions {
            per_peer: 16,
            max_inflight: 32,
            ..FetchOptions::default()
        };
        let started = Instant::now();
        let mut fetch = swarm.fetch(blob, Bits::new(COUNT), options);
        let mut fetched = 0u64;
        while let Some(piece) = fetch.next().await {
            fetched += piece.expect("a verified piece").bytes.len() as u64;
        }
        let swarm_rate = mb_per_s(fetched, started.elapsed());

        eprintln!(
            "256 MiB over {} ms RTT: download {baseline:.1} MB/s, swarm {swarm_rate:.1} MB/s ({:.2}x)",
            2 * ONE_WAY.as_millis(),
            swarm_rate / baseline
        );
        assert!(fetch.delivered().all());
        assert_eq!(fetched, TOTAL);
        assert!(
            swarm_rate >= 0.7 * baseline,
            "the swarm fetched at {swarm_rate:.1} MB/s, below 0.7 of the download's {baseline:.1} MB/s"
        );
        match peak_rss() {
            Some(rss) => {
                eprintln!("peak_rss={:.1} MiB", rss as f64 / f64::from(1 << 20));
                assert!(
                    rss < RSS_LIMIT,
                    "peak RSS {rss} bytes over {RSS_LIMIT}: something held the blob"
                );
            }
            None => eprintln!("peak_rss unavailable on this platform"),
        }
    }
}
