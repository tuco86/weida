//! `weida-perf`: weida's streaming paths between two hosts, beside raw quinn
//! and plain TCP on the same path (B-390).
//!
//! The chain, from the repository root on tuco-pc:
//!
//! 1. Open the firewall for the client host:
//!    `sudo -n ufw allow from 10.0.0.13 to any port 47300:47305 proto udp`
//!    and the same with `proto tcp`.
//! 2. `cargo build --release -p weida-perf`, then `target/release/weida-perf serve`.
//! 3. The LAN client: `run_jobs perf-windows-amd64` (wohnzimmer).
//! 4. Loopback: `target/release/weida-perf client --server 127.0.0.1`.
//! 5. Close the firewall: `sudo -n ufw delete allow ...` for both lines.
//!
//! The certificate and key in `perf-identity.pem` are a perf-only identity,
//! trusted by nothing but weida-perf; never use it elsewhere. Both sides
//! carry it, so the client pins it without a parameter from the server.
//!
//! Ports on the server: 47300/udp the main weida binding (`/sink`,
//! `/source`, `/blob`, `/swarm`, `/up`, `/live`), 47301-47303/udp three more
//! bindings with only `/swarm`, 47304/udp raw quinn, 47305/tcp plain TCP.

mod client;
mod frames;
mod raw;
mod serve;
mod stats;

use std::net::IpAddr;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use bytes::Bytes;
use weida::{Fingerprint, Identity};

/// The perf-only identity: certificate, then key.
const PERF_PEM: &str = include_str!("../perf-identity.pem");

/// The ports both sides use; `--ports main=P,swarm=P+P,quinn=P,tcp=P`
/// overrides any of them, the same on both sides.
#[derive(Clone, Debug)]
pub struct Ports {
    /// The main weida binding: every endpoint, `/swarm` included.
    pub main: u16,
    /// Further bindings with only `/swarm`; may be empty.
    pub swarm: Vec<u16>,
    /// Raw quinn (UDP).
    pub quinn: u16,
    /// Plain TCP; may equal `quinn`, they differ in protocol.
    pub tcp: u16,
}

impl Default for Ports {
    fn default() -> Ports {
        Ports {
            main: 47300,
            swarm: vec![47301, 47302, 47303],
            quinn: 47304,
            tcp: 47305,
        }
    }
}

impl Ports {
    fn parse(text: &str) -> Ports {
        let mut ports = Ports::default();
        for part in text.split(',') {
            let (key, value) = part.split_once('=').unwrap_or_else(|| usage());
            match key {
                "main" => ports.main = parsed(value, "--ports main"),
                "swarm" if value.is_empty() => ports.swarm.clear(),
                "swarm" => {
                    ports.swarm = value
                        .split('+')
                        .map(|p| parsed(p, "--ports swarm"))
                        .collect();
                }
                "quinn" => ports.quinn = parsed(value, "--ports quinn"),
                "tcp" => ports.tcp = parsed(value, "--ports tcp"),
                _ => usage(),
            }
        }
        ports
    }
}

/// The size of the shared payload chunk and of a SWARM piece.
pub const MIB: usize = 1 << 20;

/// The perf identity.
pub fn identity() -> Identity {
    let split = PERF_PEM
        .find("-----BEGIN PRIVATE KEY-----")
        .expect("the perf identity holds a key");
    Identity::from_pem(&PERF_PEM.as_bytes()[..split], &PERF_PEM.as_bytes()[split..])
}

/// The PEM text of the perf identity, for raw quinn.
pub fn identity_pem() -> &'static [u8] {
    PERF_PEM.as_bytes()
}

/// The perf identity's fingerprint, which every client URL pins.
pub fn fingerprint() -> Fingerprint {
    identity().fingerprint().expect("the perf identity parses")
}

/// One MiB of xorshift64 (seed 7), shared by every sender and SWARM piece.
static PATTERN: LazyLock<Bytes> = LazyLock::new(|| {
    let mut x: u64 = 7;
    let mut out = Vec::with_capacity(MIB);
    while out.len() < MIB {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    Bytes::from(out)
});

/// The process's start, the video rows' clock.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// The shared 1 MiB payload chunk.
pub fn pattern() -> Bytes {
    PATTERN.clone()
}

/// Microseconds since the process started.
pub fn now_us() -> u64 {
    u64::try_from(START.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// The process's user and system CPU seconds, on Linux.
pub fn cpu_times() -> Option<(f64, f64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the command name, which may contain spaces, start at
    // field 3; utime and stime are fields 14 and 15, in clock ticks of
    // 1/100 s on Linux.
    let rest = &stat[stat.rfind(')')? + 1..];
    let mut fields = rest.split_whitespace().skip(11);
    let user: f64 = fields.next()?.parse().ok()?;
    let sys: f64 = fields.next()?.parse().ok()?;
    Some((user / 100.0, sys / 100.0))
}

fn usage() -> ! {
    eprintln!(
        "usage: weida-perf serve [--bind IP] [--ports SPEC] [--for SECS] [--congestion cubic|newreno|bbr]\n       \
         weida-perf client --server HOST [--ports SPEC] [--only a,b] [--mib N] \
         [--swarm-depth P/I] [--video-mbit 8,25,50] [--video-secs S]\n\
         SPEC: main=P,swarm=P+P+P,quinn=P,tcp=P (any subset; swarm= for none)"
    );
    std::process::exit(2);
}

fn value(args: &mut impl Iterator<Item = String>, name: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("{name} needs a value");
        usage()
    })
}

fn parsed<T: std::str::FromStr>(text: &str, name: &str) -> T
where
    T::Err: std::fmt::Display,
{
    text.parse().unwrap_or_else(|e| {
        eprintln!("{name}: {e}");
        usage()
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    now_us();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("serve") => {
            let mut bind: IpAddr = [0, 0, 0, 0].into();
            let mut ports = Ports::default();
            let mut lifetime = None;
            let mut congestion = weida::Congestion::default();
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--bind" => bind = parsed(&value(&mut args, "--bind"), "--bind"),
                    "--ports" => ports = Ports::parse(&value(&mut args, "--ports")),
                    "--congestion" => {
                        congestion = match value(&mut args, "--congestion").as_str() {
                            "cubic" => weida::Congestion::Cubic,
                            "newreno" => weida::Congestion::NewReno,
                            "bbr" => weida::Congestion::Bbr,
                            _ => usage(),
                        };
                    }
                    "--for" => {
                        lifetime = Some(std::time::Duration::from_secs(parsed(
                            &value(&mut args, "--for"),
                            "--for",
                        )));
                    }
                    _ => usage(),
                }
            }
            serve::run(bind, &ports, lifetime, congestion).await
        }
        Some("client") => {
            let mut options = client::Options::default();
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--server" => options.server = value(&mut args, "--server"),
                    "--ports" => options.ports = Ports::parse(&value(&mut args, "--ports")),
                    "--only" => {
                        options.only = Some(
                            value(&mut args, "--only")
                                .split(',')
                                .map(str::to_owned)
                                .collect(),
                        );
                    }
                    "--mib" => options.mib = parsed(&value(&mut args, "--mib"), "--mib"),
                    "--swarm-depth" => {
                        let text = value(&mut args, "--swarm-depth");
                        let (p, i) = text.split_once('/').unwrap_or_else(|| usage());
                        options.swarm_depth =
                            Some((parsed(p, "--swarm-depth"), parsed(i, "--swarm-depth")));
                    }
                    "--video-mbit" => {
                        options.video_mbit = value(&mut args, "--video-mbit")
                            .split(',')
                            .map(|m| parsed(m, "--video-mbit"))
                            .collect();
                    }
                    "--video-secs" => {
                        options.video_secs =
                            parsed(&value(&mut args, "--video-secs"), "--video-secs");
                    }
                    _ => usage(),
                }
            }
            if options.server.is_empty() {
                usage();
            }
            client::run(Arc::new(options)).await
        }
        _ => usage(),
    }
}
