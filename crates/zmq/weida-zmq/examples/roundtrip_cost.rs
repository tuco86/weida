//! What a REQ/REP round trip costs in Rust, for the comparison with Python.
//!
//! The Python binding's round trip is measured by
//! `crates/zmq/weida-zmq-py/roundtrip_cost.py` with the same shape — one
//! request out, one reply back, over `inproc://` and over loopback `tcp://`,
//! at an empty payload and at 1 KiB — and the two numbers go into
//! `docs/IMPLEMENTATION.md §4.15`. This is the Rust half: the same library
//! without a language boundary in the middle.
//!
//! Not a criterion bench on purpose: what is wanted is one median against
//! another median measured the same way, and the Python side cannot be a
//! criterion bench. An example keeps both halves the same shape.
//!
//! ```text
//! cargo run -p weida-zmq --release --example roundtrip_cost
//! ```

use std::time::Instant;

use weida_zmq::{Context, ContextConfig, Multipart, RepSocket, ReqSocket, Result};

/// Round trips per measurement. Enough for a stable median at the low
/// microseconds, short enough to run in the gate's patience.
const ROUNDS: usize = 2000;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    for (transport, endpoint) in [("inproc", "inproc://cost"), ("tcp", "tcp://127.0.0.1:0")] {
        for size in [0usize, 1024] {
            let median = round_trips(endpoint, size).await?;
            println!("{transport}/{size}: {median:?}");
        }
    }
    Ok(())
}

/// The median of [`ROUNDS`] round trips, in wall-clock time.
async fn round_trips(endpoint: &str, size: usize) -> Result<std::time::Duration> {
    let context = Context::new(ContextConfig::default())?;
    let mut server = RepSocket::new(&context)?;
    let mut client = ReqSocket::new(&context)?;
    let bound = server.bind(endpoint).await?;
    client.connect(&bound.to_string())?;

    let payload = vec![0x5au8; size];
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        client.send(Multipart::single(payload.clone())).await?;
        let request = server.recv().await?;
        server.send(request).await?;
        let _reply = client.recv().await?;
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
}
