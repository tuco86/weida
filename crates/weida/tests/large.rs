//! Bounded-memory test (master doc §83 requirement 16), ignored by default.
//!
//! ```text
//! cargo test -p weida --test large -- --ignored --nocapture
//! ```
//!
//! Echoes 1 GiB through an in-process client/server pair over real QUIC and
//! asserts the byte count, the checksum, and that the peak resident set stayed
//! far below the payload size. Materializing either direction would need more
//! than 2 GiB, so a passing run is evidence that nothing along the path
//! buffers a whole transfer.
//!
//! Ignored by default because it moves a gigabyte; run it in release mode.

mod common;

use common::{Fnv, Server, Xorshift};
use tokio::io::AsyncReadExt;
use weida::TransferMeta;

const CHUNK: usize = 1024 * 1024;
const TOTAL: u64 = 1024 * 1024 * 1024;
/// Ceiling for peak RSS. The payload is 1 GiB in each direction, so anything
/// under this proves no full-transfer buffering.
const RSS_LIMIT: u64 = 512 * 1024 * 1024;

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

/// Tests that touch the filesystem, network, processes or real time: the
/// integration tier (`.config/nextest.toml`).
mod io {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "moves 1 GiB; run explicitly with --ignored"]
    async fn large_stream_bounded_memory() {
        let server = Server::start().await;
        let replier = server.listener.replier("/echo").expect("replier");

        let handler = tokio::spawn(async move {
            let mut request = replier.accept().await.expect("accept");
            // Detach the request half so both directions of the one bidirectional
            // stream stay live at once.
            let mut body = request.take_body();
            let mut out = request
                .reply(TransferMeta::default())
                .await
                .expect("open reply");
            let mut chunk = vec![0u8; CHUNK];
            let mut echoed = 0u64;
            loop {
                let n = body.read(&mut chunk).await.expect("read body");
                if n == 0 {
                    break;
                }
                out.write_all(&chunk[..n]).await.expect("write reply");
                echoed += n as u64;
            }
            out.finish().expect("finish reply");
            echoed
        });

        let client = server.client_runtime();
        let requester = client.requester(server.trust());
        requester
            .connect(&server.url("/echo"))
            .await
            .expect("connect");

        let (mut transfer, reply) = requester
            .open(TransferMeta::default().with_content_len(TOTAL))
            .await
            .expect("open");

        // Fold the echo away as it arrives; buffering it would defeat the point.
        let reader = tokio::spawn(async move {
            let mut reply = reply.recv().await.expect("recv reply");
            let mut digest = Fnv::default();
            let mut chunk = vec![0u8; CHUNK];
            let mut received = 0u64;
            loop {
                let n = reply.read(&mut chunk).await.expect("read reply");
                if n == 0 {
                    break;
                }
                digest.update(&chunk[..n]);
                received += n as u64;
            }
            (received, digest.finish())
        });

        let started = std::time::Instant::now();
        let mut rng = Xorshift::new(0x2545_f491_4f6c_dd1d);
        let mut sent_digest = Fnv::default();
        let mut chunk = vec![0u8; CHUNK];
        let mut sent = 0u64;
        while sent < TOTAL {
            let n = std::cmp::min(CHUNK as u64, TOTAL - sent) as usize;
            rng.fill(&mut chunk[..n]);
            sent_digest.update(&chunk[..n]);
            transfer.write_all(&chunk[..n]).await.expect("write");
            sent += n as u64;
        }
        transfer.finish().expect("finish");

        let (received, received_digest) = reader.await.expect("reader task");
        let echoed = handler.await.expect("handler");
        let elapsed = started.elapsed();

        assert_eq!(sent, TOTAL);
        assert_eq!(echoed, TOTAL, "the server must have seen every byte");
        assert_eq!(received, TOTAL, "the client must have read every byte back");
        assert_eq!(
            received_digest,
            sent_digest.finish(),
            "the echo does not match what was sent"
        );

        let throughput = (sent + received) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        println!(
            "echoed {} MiB each way in {:.2}s ({throughput:.1} MiB/s both directions)",
            TOTAL / (1024 * 1024),
            elapsed.as_secs_f64()
        );

        if let Some(rss) = peak_rss() {
            println!("peak_rss={:.1} MiB", rss as f64 / (1024.0 * 1024.0));
            assert!(
                rss < RSS_LIMIT,
                "peak RSS {rss} bytes exceeds the {RSS_LIMIT} byte ceiling: something buffered a whole transfer"
            );
        } else {
            println!(
                "peak_rss unavailable on this platform; byte and checksum checks still applied"
            );
        }

        client.shutdown().await;
    }
}
