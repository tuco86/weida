//! `weida-perf serve`: every endpoint the client measures, on fixed ports.

use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use tokio::io::AsyncReadExt;
use weida::{
    Ask, Bits, IncomingTransfer, PieceList, PieceSize, Radio, Runtime, RuntimeConfig, Seeder,
    SegmentTerms, TransferMeta, TreeHasher,
};

use crate::{MIB, Ports, cpu_times, identity, pattern};

/// Pieces of the SWARM blob: 1 GiB of the 1 MiB pattern.
pub const SWARM_PIECES: u32 = 1024;

/// The relay's read size, as griasdi's screen share reads.
const READ_CHUNK: usize = 64 * 1024;

pub async fn run(
    bind: IpAddr,
    ports: &Ports,
    lifetime: Option<std::time::Duration>,
    congestion: weida::Congestion,
) -> Result<(), Box<dyn Error>> {
    let mut config = RuntimeConfig::default();
    config.limits.congestion = congestion;
    let runtime = Runtime::new(config)?;
    let main = runtime.listener();
    let _main_binding = main
        .bind_quic(SocketAddr::new(bind, ports.main), identity())
        .await?;

    let list = Arc::new(swarm_list());

    let sink = main.puller("/sink")?;
    tokio::spawn(async move {
        while let Ok(transfer) = sink.recv().await {
            tokio::spawn(drain(transfer));
        }
    });

    let source = main.replier("/source")?;
    tokio::spawn(async move {
        while let Ok(mut request) = source.accept().await {
            tokio::spawn(async move {
                let mut len = [0u8; 8];
                if request.body().read_exact(&mut len).await.is_err() {
                    return;
                }
                let mut left = u64::from_be_bytes(len);
                let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                    return;
                };
                let chunk = pattern();
                while left > 0 {
                    let n = left.min(chunk.len() as u64) as usize;
                    if reply.write_all(&chunk[..n]).await.is_err() {
                        return;
                    }
                    left -= n as u64;
                }
                if let Ok(delivery) = reply.finish() {
                    let _ = delivery.delivered().await;
                }
            });
        }
    });

    let blob = main.replier("/blob")?;
    let described = list.blob();
    tokio::spawn(async move {
        while let Ok(request) = blob.accept().await {
            let mut body = Vec::with_capacity(41);
            body.extend_from_slice(described.root().as_bytes());
            body.extend_from_slice(&described.size().to_be_bytes());
            body.push(described.piece().bytes().trailing_zeros() as u8);
            tokio::spawn(async move {
                if let Ok(mut reply) = request.reply(TransferMeta::default()).await
                    && reply.write_all(&body).await.is_ok()
                    && let Ok(delivery) = reply.finish()
                {
                    let _ = delivery.delivered().await;
                }
            });
        }
    });

    tokio::spawn(seed(main.seeder("/swarm")?, Arc::clone(&list)));

    let up = main.puller("/up")?;
    let radio = Arc::new(main.radio("/live")?);
    tokio::spawn(async move {
        while let Ok(transfer) = up.recv().await {
            tokio::spawn(relay(Arc::clone(&radio), transfer));
        }
    });

    // More bindings, each its own listener with only `/swarm`, so a fetch
    // sees one seeder per binding on a connection of its own.
    let mut extra = Vec::new();
    for &port in &ports.swarm {
        let listener = runtime.listener();
        let binding = listener
            .bind_quic(SocketAddr::new(bind, port), identity())
            .await?;
        tokio::spawn(seed(listener.seeder("/swarm")?, Arc::clone(&list)));
        extra.push((listener, binding));
    }

    crate::raw::serve_quinn(SocketAddr::new(bind, ports.quinn), congestion)
        .await
        .map_err(|e| e.to_string())?;
    crate::raw::serve_tcp(SocketAddr::new(bind, ports.tcp))
        .await
        .map_err(|e| e.to_string())?;

    println!("READY {ports:?}");
    match lifetime {
        Some(lifetime) => tokio::time::sleep(lifetime).await,
        None => std::future::pending::<()>().await,
    }
    drop((main, extra));
    runtime.shutdown().await;
    Ok(())
}

/// The piece list of [`SWARM_PIECES`] copies of the pattern.
fn swarm_list() -> PieceList {
    let piece = PieceSize::new(MIB as u64).expect("1 MiB is a piece size");
    let mut hasher = TreeHasher::new(piece);
    let chunk = pattern();
    for _ in 0..SWARM_PIECES {
        hasher.update(&chunk);
    }
    hasher.finish().expect("1024 pieces fit")
}

/// Reads one pushed transfer to its end and reports its rate.
async fn drain(mut transfer: IncomingTransfer) {
    let started = Instant::now();
    let mut buf = Vec::with_capacity(MIB);
    let mut total = 0u64;
    loop {
        buf.clear();
        match transfer.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(n) => total += n as u64,
            Err(e) => {
                eprintln!("sink: {e}");
                return;
            }
        }
    }
    let secs = started.elapsed().as_secs_f64();
    let (user, sys) = cpu_times().unwrap_or((f64::NAN, f64::NAN));
    println!(
        "SINK {total} {secs:.3} {:.1} user={user:.2} sys={sys:.2}",
        total as f64 / 1e6 / secs
    );
}

/// Answers every session for the perf blob from the pattern.
async fn seed(seeder: Seeder, list: Arc<PieceList>) {
    while let Ok(mut session) = seeder.accept().await {
        let list = Arc::clone(&list);
        tokio::spawn(async move {
            let blob = list.blob();
            if session.blob() != blob {
                session.decline().await;
                return;
            }
            let chunk = pattern();
            while let Ok(Some(ask)) = session.next().await {
                let answered = match ask {
                    Ask::List => session.answer_list(&list).await,
                    Ask::Have => session.answer_have(&Bits::full(blob.count())).await,
                    Ask::Piece(_) => session.answer_piece(&chunk).await,
                };
                if answered.is_err() {
                    return;
                }
            }
        });
    }
}

/// Copies an uplink segment into a radio segment that follows its upstream,
/// as griasdi's `screen::relay_chunks` does, without its size and rate charge.
async fn relay(radio: Arc<Radio>, mut transfer: IncomingTransfer) {
    let topic = transfer.meta().topic.clone().unwrap_or_else(|| "v".into());
    let terms = SegmentTerms::default().with_follows_upstream(true);
    let Ok(mut segment) = radio.segment(&topic, terms) else {
        return;
    };
    let mut scratch = Vec::with_capacity(READ_CHUNK);
    loop {
        scratch.clear();
        let n = match transfer.read_buf(&mut scratch).await {
            Ok(0) => {
                segment.finish();
                return;
            }
            Ok(n) => n,
            Err(_) => return,
        };
        // No viewer copy left is fine here: the viewer may join late.
        if segment.write(scratch[..n].to_vec()).is_err() {
            return;
        }
    }
}
