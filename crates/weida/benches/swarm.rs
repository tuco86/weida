//! SWARM throughput over loopback QUIC: one seeder, one fetch of 64 MiB in
//! 1 MiB pieces through a session
//! ([decisions/0041](../../../docs/decisions/0041-swarm-sessions.md) §4.4).
//!
//! ```text
//! cargo bench -p weida --features swarm --bench swarm -- --warm-up-time 1 --measurement-time 3
//! ```
//!
//! The seeder serves from memory and the fetch's caller only counts what it
//! is handed, so the figure is the session wire and the piece check, not a
//! disk. The connection is dialled once, outside the measurement.

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{
    Ask, Bits, FetchOptions, Identity, PieceList, PieceSize, Runtime, RuntimeConfig, Trust,
};

const PIECE: usize = 1 << 20;
const COUNT: u32 = 64;
const TOTAL: usize = PIECE * COUNT as usize;

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// What keeps the seeder alive for the whole run.
struct Seeding {
    _runtime: Runtime,
    _listener: weida::Listener,
    _binding: weida::Binding,
    url: String,
    trust: Trust,
}

/// Binds a seeder that answers every order from `data`.
async fn seeding(data: Arc<Vec<u8>>, list: Arc<PieceList>) -> Seeding {
    let identity = Identity::generate().expect("identity");
    let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
        .await
        .expect("bind");
    let url = format!("weida://127.0.0.1:{}/swarm", binding.local_addr().port());
    let seeder = listener.seeder("/swarm").expect("seeder");
    tokio::spawn(async move {
        while let Ok(mut session) = seeder.accept().await {
            let data = Arc::clone(&data);
            let list = Arc::clone(&list);
            tokio::spawn(async move {
                let blob = list.blob();
                if session.blob() != blob {
                    session.decline().await;
                    return;
                }
                while let Ok(Some(ask)) = session.next().await {
                    let answered = match ask {
                        Ask::List => session.answer_list(&list).await,
                        Ask::Have => session.answer_have(&Bits::full(COUNT)).await,
                        Ask::Piece(index) => {
                            let span = blob.span(index).expect("a piece of the blob");
                            session
                                .answer_piece(&data[span.start as usize..span.end as usize])
                                .await
                        }
                    };
                    if answered.is_err() {
                        return;
                    }
                }
            });
        }
    });
    Seeding {
        _runtime: runtime,
        _listener: listener,
        _binding: binding,
        url,
        trust,
    }
}

fn bench_fetch(c: &mut Criterion) {
    let rt = tokio_runtime();
    let data: Arc<Vec<u8>> = Arc::new(
        (0..TOTAL)
            .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_be_bytes()[0])
            .collect(),
    );
    let list = Arc::new(PieceList::of(&data, PieceSize::DEFAULT).expect("list"));
    let blob = list.blob();
    let seeding = rt.block_on(seeding(Arc::clone(&data), Arc::clone(&list)));
    let client = rt.block_on(async { Runtime::new(RuntimeConfig::default()).expect("client") });
    let swarm = client.swarm(seeding.trust.clone());
    rt.block_on(swarm.connect(&seeding.url)).expect("connect");

    let mut group = c.benchmark_group("swarm");
    group.throughput(Throughput::Bytes(TOTAL as u64));
    group.sample_size(10);
    group.bench_function("fetch_64mib_one_seeder", |b| {
        b.to_async(&rt).iter(|| async {
            let options = FetchOptions {
                list: Some((*list).clone()),
                per_peer: 16,
                max_inflight: 32,
                ..FetchOptions::default()
            };
            let mut fetch = swarm.fetch(blob, Bits::new(COUNT), options);
            let mut fetched = 0;
            while let Some(piece) = fetch.next().await {
                fetched += piece.expect("a verified piece").bytes.len();
            }
            assert_eq!(fetched, TOTAL);
            black_box(fetched)
        });
    });
    group.finish();
}

criterion_group!(benches, bench_fetch);
criterion_main!(benches);
