//! The loopback broker every integration test in this crate drives.
//!
//! Each test file is its own crate and adds the producers and consumers it
//! needs in its own `impl Harness` block.

use std::time::Duration;

use weida::{Binding, ClientTls, Identity, Listener, Runtime, RuntimeConfig, ServerTls, Trust};
use weida_broker::{Broker, BrokerConfig};

/// Generous ceiling: every assertion settles in milliseconds.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// How long "nothing arrives" is given before it counts as nothing. Long
/// enough that a delivery in flight would have landed on loopback, short
/// enough to keep the suite quick.
#[allow(dead_code)]
pub const SILENCE: Duration = Duration::from_millis(300);

/// A broker on a loopback port, and what a peer needs to reach it.
pub struct Harness {
    pub runtime: Runtime,
    /// Held: dropping the listener stops serving.
    pub _listener: Listener,
    /// Held: dropping the binding closes the port.
    pub _binding: Binding,
    pub broker: Broker,
    pub client: ClientTls,
    pub port: u16,
}

impl Harness {
    /// Starts a broker serving `config`'s queues.
    pub async fn start(config: BrokerConfig) -> Harness {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let listener = runtime.listener();
        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                ServerTls::new(identity),
            )
            .await
            .expect("bind");
        let port = binding.local_addr().port();
        let broker = Broker::new(&listener, config).expect("broker");
        let serving = broker.clone();
        tokio::spawn(async move { serving.serve().await });
        Harness {
            runtime,
            _listener: listener,
            _binding: binding,
            broker,
            client: ClientTls::new(Trust::pin(fingerprint)),
            port,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.port, path)
    }
}
