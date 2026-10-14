//! `weida` — exercise an endpoint from a shell.
//!
//! Every example in this repository is a Rust file, so the shortest way to
//! find out whether an endpoint is reachable would be to write and compile a
//! program. This binary is four verbs
//! ([IMPLEMENTATION.md](../../../docs/IMPLEMENTATION.md) §1) over the same
//! library the examples use, with the payload on stdin and stdout so it
//! composes with everything else in a pipeline.
//!
//! ```text
//! weida serve --echo   weida://0.0.0.0:7443/echo [--identity PATH] [--cert-out PATH]
//! weida serve --sink   weida://0.0.0.0:7443/sink [--identity PATH]
//! weida serve --pub    weida://0.0.0.0:7443/md   [--identity PATH]
//! weida request        weida://sha256:…@host:7443/echo [FILE]
//! weida send           weida://sha256:…@host:7443/sink [FILE]
//! weida sub            weida://sha256:…@host:7443/md [--filter F] [--count N]
//! ```
//!
//! **What goes where.** The payload — the request body, the reply, a published
//! message — is the only thing on stdout, so `weida request … | sha256sum`
//! means what it looks like. Addresses, receipts, topics and diagnostics go to
//! stderr. `serve` is the one exception: the addresses it prints *are* its
//! output, in the sense that a client needs nothing else, so they go to stdout
//! exactly as `transform_server` prints them.
//!
//! **Trust.** A `weida://sha256:…@host:port/path` address names the key that
//! must answer and needs no file (`Trust::by_address`); `--ca PATH` trusts a
//! certificate as an anchor instead and then a plain address is verified
//! against its names. A local transport (`weida+unix://`, `weida+pipe://`,
//! `weida+inproc://`) has no key at all: the kernel proves the peer, and an
//! address carrying a fingerprint is refused rather than quietly ignored
//! ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.8).
//!
//! **Exit codes**, so a script can branch on the error vocabulary rather than
//! on a message:
//!
//! | Code | Meaning |
//! | --- | --- |
//! | `0` | the operation completed |
//! | `1` | any other failure, including `Indeterminate` |
//! | `2` | usage |
//! | `3` | the peer refused the payload (`Rejected`) |
//! | `4` | no endpoint is registered under that path (`UnknownEndpoint`) |
//! | `5` | the request was accepted and no reply will exist (`NoReply`) |
//! | `6` | the key that answered is not trusted (`Untrusted`) |
//! | `7` | the connection was lost, or was never established |
//!
//! `1` rather than a code of its own for `Indeterminate` is deliberate: a
//! script must not be able to treat "I do not know whether it arrived" as a
//! definite outcome by matching a number
//! ([FAILURE_MODEL.md](../../../docs/FAILURE_MODEL.md)).

use std::path::PathBuf;
use std::process::ExitCode;

use std::io::{Read as _, Write as _};

use tokio::io::AsyncReadExt;
use weida::{
    Address, Error, Identity, IncomingRequest, Publisher, Puller, Replier, Runtime, RuntimeConfig,
    TransferMeta, Trust,
};

/// Read and write in 64 KiB pieces, the size the examples use: large enough
/// that a megabyte is sixteen syscalls, small enough to stream.
const CHUNK: usize = 64 * 1024;

/// How `sub` separates one payload from the next on stdout.
///
/// The default is a newline, because a subscriber's output is usually a log to
/// look at — and a newline is *not framing*: a payload that contains one is
/// indistinguishable from two payloads. So the choice is the caller's, stated
/// rather than assumed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// A newline after each payload. Unambiguous only for payloads that
    /// contain none.
    Line,
    /// Nothing at all: the payload is the whole of stdout. For `--count 1`,
    /// where the process boundary is the frame.
    Raw,
    /// A NUL after each payload — unambiguous for text, and what `xargs -0`
    /// reads.
    Nul,
    /// An eight-byte big-endian length before each payload. Unambiguous for
    /// anything, and the only form a script can parse without knowing the
    /// payload.
    Length,
}

impl Framing {
    fn parse(name: &str) -> Framing {
        match name {
            "line" => Framing::Line,
            "raw" => Framing::Raw,
            "nul" => Framing::Nul,
            "length" => Framing::Length,
            other => fail(format!(
                "--framing: {other:?} is not one of line, raw, nul, length"
            )),
        }
    }

    /// What precedes a payload of `len` bytes.
    fn prefix(self, len: u64) -> Option<[u8; 8]> {
        match self {
            Framing::Length => Some(len.to_be_bytes()),
            _ => None,
        }
    }

    /// What follows it.
    fn suffix(self) -> Option<u8> {
        match self {
            Framing::Line => Some(b'\n'),
            Framing::Nul => Some(0),
            Framing::Raw | Framing::Length => None,
        }
    }
}

/// The usage text, printed for `--help` and for a usage error alike.
const USAGE: &str = "usage:
  weida send    [--ca PATH] URL [FILE]        one-way transfer; payload from FILE or stdin
  weida request [--ca PATH] URL [FILE]        exchange; the reply goes to stdout
  weida sub     [--ca PATH] [--filter F] [--count N] [--framing FORM] URL
  weida serve   (--echo | --sink | --pub) URL [--identity PATH] [--cert-out PATH]

options:
  --ca PATH        trust this certificate as an anchor instead of the address's key
  --identity PATH  load or create the server identity here, so the address is stable
  --cert-out PATH  write the server certificate, for a client that uses --ca
  --filter F       subscribe to this topic filter; the default takes every topic
  --count N        exit after N messages; without it, sub runs until stopped
  --framing FORM   how sub separates payloads on stdout: line (default), raw, nul, length
  --version        print the version
  --help           print this

URL is weida://[sha256:HEX@]HOST:PORT/PATH, weida+unix://SOCKET/PATH with the
socket path percent-encoded (weida+unix://%2Ftmp%2Fs.sock/echo), or
weida+pipe://NAME/PATH. weida+inproc:// is in-process only and cannot be
served from a shell.

Payload on stdin and stdout; addresses and diagnostics on stderr.
Exit codes: 2 usage, 3 refused, 4 unknown endpoint, 5 no reply, 6 untrusted,
7 connection lost, 1 anything else.";

/// A usage *error*: the text on stderr and exit `2`.
fn usage() -> ! {
    eprintln!("{USAGE}");
    std::process::exit(2);
}

/// A help *request*: the text on stdout and exit `0`, because asking for help
/// is not a mistake.
fn help() -> ! {
    println!("{USAGE}");
    std::process::exit(0);
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("weida: {message}");
    std::process::exit(2);
}

/// What `serve` binds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A replier that returns the request payload unchanged.
    Echo,
    /// A puller that writes every payload it receives to stdout.
    Sink,
    /// A publisher that fans stdin out, one message per line.
    Publish,
}

enum Command {
    Send {
        url: String,
        file: Option<PathBuf>,
        ca: Option<PathBuf>,
    },
    Request {
        url: String,
        file: Option<PathBuf>,
        ca: Option<PathBuf>,
    },
    Sub {
        url: String,
        filter: String,
        count: Option<u64>,
        framing: Framing,
        ca: Option<PathBuf>,
    },
    Serve {
        url: String,
        role: Role,
        identity: Option<PathBuf>,
        cert_out: Option<PathBuf>,
    },
}

fn parse_args() -> Command {
    let mut args = std::env::args().skip(1);
    let verb = args.next().unwrap_or_else(|| usage());
    // Before anything else, because these two answer without a verb.
    match verb.as_str() {
        "--help" | "-h" | "help" => help(),
        "--version" | "-V" => {
            println!("weida {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        _ => {}
    }
    let mut url = None;
    let mut file = None;
    let mut ca = None;
    let mut identity = None;
    let mut cert_out = None;
    let mut filter = None;
    let mut count = None;
    let mut framing = Framing::Line;
    let mut role = None;

    let next = |args: &mut dyn Iterator<Item = String>, what: &str| -> String {
        args.next()
            .unwrap_or_else(|| fail(format!("{what} needs a value")))
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ca" => ca = Some(PathBuf::from(next(&mut args, "--ca"))),
            "--identity" => identity = Some(PathBuf::from(next(&mut args, "--identity"))),
            "--cert-out" => cert_out = Some(PathBuf::from(next(&mut args, "--cert-out"))),
            "--filter" => filter = Some(next(&mut args, "--filter")),
            "--count" => {
                let value = next(&mut args, "--count");
                count = Some(
                    value
                        .parse::<u64>()
                        .unwrap_or_else(|e| fail(format!("--count: {e}"))),
                );
            }
            "--framing" => framing = Framing::parse(&next(&mut args, "--framing")),
            "--echo" => role = Some(Role::Echo),
            "--sink" => role = Some(Role::Sink),
            "--pub" => role = Some(Role::Publish),
            "--help" | "-h" => help(),
            "--version" | "-V" => {
                println!("weida {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other if other.starts_with("--") => fail(format!("unexpected option: {other}")),
            other if url.is_none() => url = Some(other.to_owned()),
            other if file.is_none() => file = Some(PathBuf::from(other)),
            other => fail(format!("unexpected argument: {other}")),
        }
    }
    let url = url.unwrap_or_else(|| usage());

    match verb.as_str() {
        "send" => Command::Send { url, file, ca },
        "request" => Command::Request { url, file, ca },
        "sub" => Command::Sub {
            url,
            // The empty filter takes every topic, which is what a caller who
            // did not name one is asking for.
            filter: filter.unwrap_or_default(),
            count,
            framing,
            ca,
        },
        "serve" => Command::Serve {
            url,
            role: role
                .unwrap_or_else(|| fail("serve needs one of --echo, --sink or --pub".to_owned())),
            identity,
            cert_out,
        },
        other => fail(format!("unknown command: {other}")),
    }
}

fn trust(ca: Option<&PathBuf>) -> Trust {
    match ca {
        Some(path) => Trust::anchor_file(path),
        None => Trust::by_address(),
    }
}

/// The exit code for one library error.
fn code_of(error: &Error) -> u8 {
    match error {
        Error::Rejected => 3,
        Error::UnknownEndpoint => 4,
        Error::NoReply => 5,
        Error::Untrusted(_) => 6,
        Error::ConnectionLost(_) | Error::NotConnected => 7,
        _ => 1,
    }
}

fn report(error: Error) -> ExitCode {
    match &error {
        // Worth its own sentence: the operator can check this fingerprint out
        // of band and paste it into the address.
        Error::Untrusted(presented) => {
            eprintln!("weida: the peer presented {presented}, which is not trusted");
        }
        other => eprintln!("weida: {other}"),
    }
    ExitCode::from(code_of(&error))
}

/// Reads the whole payload a client is to send.
///
/// Whole, because a one-way transfer and a request both want a `content_len`
/// where one is knowable, and because a CLI payload is a file or a pipe that
/// has already been produced. A payload too large to hold is what the library
/// API is for.
///
/// Standard input and output are used **blocking**, through `std`, rather than
/// through tokio's `io-std` and `fs` features. Those features are not in the
/// library's dependency set and adding them for a binary would widen what
/// every downstream crate compiles; a terminal is not a peer, so the write
/// that blocks here blocks on a pipe the operator controls and never on the
/// network.
fn payload(file: Option<&PathBuf>) -> std::io::Result<Vec<u8>> {
    match file {
        Some(path) => std::fs::read(path),
        None => {
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf)?;
            Ok(buf)
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let command = parse_args();
    let runtime = match Runtime::new(RuntimeConfig::default()) {
        Ok(runtime) => runtime,
        Err(e) => return report(e),
    };

    let outcome = match &command {
        Command::Send { url, file, ca } => send(&runtime, url, file.as_ref(), ca.as_ref()).await,
        Command::Request { url, file, ca } => {
            request(&runtime, url, file.as_ref(), ca.as_ref()).await
        }
        Command::Sub {
            url,
            filter,
            count,
            framing,
            ca,
        } => subscribe(&runtime, url, filter, *count, *framing, ca.as_ref()).await,
        Command::Serve {
            url,
            role,
            identity,
            cert_out,
        } => serve(&runtime, url, *role, identity.as_ref(), cert_out.as_ref()).await,
    };

    // A drain rather than a shutdown: a `send` whose receipt nobody awaited
    // has its transfer parked on the connection, and abandoning the process
    // would discard the bytes the command was for
    // (`docs/decisions/0009-drain.md` §4.2). The deadline is this program's
    // own, because the drain requires one.
    let drained = runtime.drain(std::time::Duration::from_secs(5)).await;
    // Only worth saying when the command otherwise succeeded: a refused
    // request leaves its own transfer unacknowledged, and reporting that
    // beside the refusal would read as a second, unrelated failure.
    if outcome.is_ok() && drained.outstanding > 0 {
        eprintln!(
            "weida: {} transfer(s) unacknowledged at the drain deadline",
            drained.outstanding
        );
    }

    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

async fn send(
    runtime: &Runtime,
    url: &str,
    file: Option<&PathBuf>,
    ca: Option<&PathBuf>,
) -> Result<(), Error> {
    let body = payload(file).map_err(Error::Io)?;
    let pusher = runtime.pusher(trust(ca));
    pusher.connect(url).await?;
    let mut transfer = pusher
        .open(TransferMeta::default().with_content_len(body.len() as u64))
        .await?;
    transfer.write_all(&body).await?;
    let delivery = transfer.finish()?;
    // The receipt is QUIC's transport acknowledgement and not an application
    // ack, which is why it says "delivered" and not "processed"
    // (`docs/GUARANTEES.md` §1).
    match delivery.delivered().await {
        Ok(()) => eprintln!("delivered bytes={}", body.len()),
        Err(e) => {
            eprintln!("undelivered bytes={}: {e}", body.len());
            return Err(e);
        }
    }
    Ok(())
}

async fn request(
    runtime: &Runtime,
    url: &str,
    file: Option<&PathBuf>,
    ca: Option<&PathBuf>,
) -> Result<(), Error> {
    let body = payload(file).map_err(Error::Io)?;
    let requester = runtime.requester(trust(ca));
    requester.connect(url).await?;
    let mut reply = requester
        .request_with(
            TransferMeta::default().with_content_len(body.len() as u64),
            &body,
        )
        .await?;

    let mut stdout = std::io::stdout();
    let mut chunk = vec![0u8; CHUNK];
    let mut total = 0u64;
    loop {
        let n = reply.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        stdout.write_all(&chunk[..n]).map_err(Error::Io)?;
    }
    stdout.flush().map_err(Error::Io)?;
    eprintln!("reply bytes={total}");
    Ok(())
}

async fn subscribe(
    runtime: &Runtime,
    url: &str,
    filter: &str,
    count: Option<u64>,
    framing: Framing,
    ca: Option<&PathBuf>,
) -> Result<(), Error> {
    let subscriber = runtime.subscriber(trust(ca));
    subscriber.connect(url).await?;
    subscriber.subscribe(filter).await?;
    eprintln!("subscribed filter={filter:?}");

    let mut stdout = std::io::stdout();
    let mut seen = 0u64;
    let mut chunk = vec![0u8; CHUNK];
    // Without `--count` this runs until the process is stopped: a subscriber
    // has no end of its own, and inventing one would be a timeout nobody
    // asked for.
    while count.is_none_or(|limit| seen < limit) {
        let mut transfer = subscriber.recv().await?;
        let topic = transfer.meta().topic.clone().unwrap_or_default();
        // `length` framing has to know the size before the bytes, so it holds
        // the payload; every other form streams. The hold is bounded by the
        // publisher's own `max_message_size` for a whole publish, and by
        // nothing for a streamed one — which is why it is not the default and
        // why the help text says what each form costs.
        let mut held = Vec::new();
        let mut bytes = 0u64;
        loop {
            let n = transfer.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            bytes += n as u64;
            if framing == Framing::Length {
                held.extend_from_slice(&chunk[..n]);
            } else {
                stdout.write_all(&chunk[..n]).map_err(Error::Io)?;
            }
        }
        if let Some(prefix) = framing.prefix(bytes) {
            stdout.write_all(&prefix).map_err(Error::Io)?;
            stdout.write_all(&held).map_err(Error::Io)?;
        }
        if let Some(suffix) = framing.suffix() {
            stdout.write_all(&[suffix]).map_err(Error::Io)?;
        }
        stdout.flush().map_err(Error::Io)?;
        // The gap is the whole reason a subscriber can trust what it got: a
        // fan-out drop is silent unless `PerProducer` is negotiated, and then
        // it is a number.
        match transfer.meta().gap.as_ref() {
            Some(gap) => eprintln!("topic={topic} bytes={bytes} missed={}", gap.missed()),
            None => eprintln!("topic={topic} bytes={bytes}"),
        }
        seen += 1;
    }
    Ok(())
}

async fn serve(
    runtime: &Runtime,
    url: &str,
    role: Role,
    identity: Option<&PathBuf>,
    cert_out: Option<&PathBuf>,
) -> Result<(), Error> {
    let listener = runtime.listener();

    // A **bind** address is not the string a client dials: its port may be
    // `0` — let the kernel choose, which is what a script wants — and the
    // fingerprint is this process's output rather than its input. So the QUIC
    // form is parsed here rather than through `Address::parse`, which refuses
    // both, rightly, for a dialling address. The local schemes have neither
    // problem and go through the library's parser.
    let (path, printed) = match url.strip_prefix("weida://") {
        Some(rest) => {
            let Some((authority, tail)) = rest.split_once('/') else {
                return Err(Error::InvalidAddress(format!(
                    "missing endpoint path: {url:?}"
                )));
            };
            if authority.contains('@') {
                return Err(Error::InvalidAddress(
                    "a bind address carries no fingerprint; serve prints the one it used"
                        .to_owned(),
                ));
            }
            let socket: std::net::SocketAddr = authority
                .parse()
                .map_err(|e| Error::InvalidAddress(format!("{authority:?}: {e}")))?;
            let identity = match identity {
                Some(path) => load_or_create_identity(path)?,
                None => Identity::generate_for(["localhost", "127.0.0.1", "::1"])?,
            };
            if let Some(path) = cert_out {
                std::fs::write(path, identity.certificate_pem()?).map_err(Error::Io)?;
                eprintln!("wrote the certificate to {}", path.display());
            }
            let fingerprint = identity.fingerprint()?;
            let binding = listener.bind_quic(socket, identity).await?;
            let local = binding.local_addr();
            // Leaked on purpose: the binding must outlive this scope, and the
            // process exists to serve until it is stopped.
            std::mem::forget(binding);
            let path = format!("/{tail}");
            let printed = format!(
                "weida://{fingerprint}@{}:{}{path}",
                local.ip(),
                local.port()
            );
            (path, printed)
        }
        None => bind_local(&listener, url)?,
    };

    // stdout, because this address is what a client needs and nothing else.
    println!("{printed}");

    match role {
        Role::Echo => echo(listener.replier(&path)?).await,
        Role::Sink => sink(listener.puller(&path)?).await,
        Role::Publish => publish(listener.publisher(&path)?).await,
    }
}

/// Binds one of the local transports, and returns its endpoint path and the
/// address a client dials.
///
/// No identity anywhere: a local transport has no key to pin, because the
/// kernel proves the peer
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.8).
fn bind_local(listener: &weida::Listener, url: &str) -> Result<(String, String), Error> {
    let address = Address::parse(url)?;
    let path = address.path().to_owned();
    match &address {
        Address::Quic(_) => Err(Error::InvalidAddress(
            "a weida:// bind address is handled above".to_owned(),
        )),
        // Honest rather than convenient: an in-process bus cannot be reached
        // from another process, so serving one from a shell could only ever
        // talk to itself.
        Address::Inproc(addr) => Err(Error::InvalidAddress(format!(
            "weida+inproc://{} is reachable only inside one process; \
             use weida+unix://, weida+pipe:// or weida://",
            addr.bus
        ))),
        #[cfg(unix)]
        Address::Unix(addr) => {
            let binding = listener.bind_unix(&addr.socket)?;
            std::mem::forget(binding);
            // `Display` rather than the decoded path: the socket path shares
            // its separator with the endpoint path, so what a client can dial
            // is the percent-encoded form.
            Ok((path, addr.to_string()))
        }
        #[cfg(not(unix))]
        Address::Unix(_) => Err(Error::InvalidAddress(
            "weida+unix:// needs a Unix host".to_owned(),
        )),
        #[cfg(windows)]
        Address::Pipe(addr) => {
            let binding = listener.bind_pipe(&addr.name)?;
            std::mem::forget(binding);
            Ok((path, addr.to_string()))
        }
        #[cfg(not(windows))]
        Address::Pipe(_) => Err(Error::InvalidAddress(
            "weida+pipe:// needs a Windows host".to_owned(),
        )),
    }
}

async fn echo(replier: Replier) -> Result<(), Error> {
    loop {
        let request = replier.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = echo_one(request).await {
                eprintln!("weida: request failed: {e}");
            }
        });
    }
}

async fn echo_one(mut request: IncomingRequest) -> Result<(), Error> {
    let mut body = request.take_body();
    let mut out = request.reply(TransferMeta::default()).await?;
    let mut chunk = vec![0u8; CHUNK];
    let mut total = 0u64;
    loop {
        let n = body.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        out.write_all(&chunk[..n]).await?;
    }
    out.finish()?;
    eprintln!("echoed bytes={total}");
    Ok(())
}

async fn sink(puller: Puller) -> Result<(), Error> {
    let mut stdout = std::io::stdout();
    let mut chunk = vec![0u8; CHUNK];
    loop {
        let mut transfer = puller.recv().await?;
        let mut total = 0u64;
        loop {
            let n = transfer.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            total += n as u64;
            stdout.write_all(&chunk[..n]).map_err(Error::Io)?;
        }
        stdout.flush().map_err(Error::Io)?;
        eprintln!("received bytes={total}");
    }
}

async fn publish(publisher: Publisher) -> Result<(), Error> {
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        let read = std::io::BufRead::read_line(&mut stdin.lock(), &mut line).map_err(Error::Io)?;
        if read == 0 {
            return Ok(());
        }
        // `topic body` per line, or just a body on the empty topic: a
        // publisher's topic is part of the message, so it has to come from
        // somewhere, and a line is what a shell can produce.
        let (topic, body) = match line.trim_end_matches('\n').split_once(' ') {
            Some((topic, body)) => (topic, body),
            None => ("", line.trim_end_matches('\n')),
        };
        let reached = publisher.publish(topic, body.as_bytes().to_vec())?;
        eprintln!("published topic={topic} subscribers={reached}");
    }
}

/// Loads the identity at `path`, or generates one and stores it there, so a
/// served address is stable across restarts.
fn load_or_create_identity(path: &PathBuf) -> Result<Identity, Error> {
    if path.exists() {
        let identity = Identity::from_pem_file(path);
        // Read it now, so a corrupt file fails here with the path in hand.
        identity.fingerprint()?;
        return Ok(identity);
    }
    let identity = Identity::generate_for(["localhost", "127.0.0.1", "::1"])?;
    write_private(path, identity.to_pem()?.as_bytes()).map_err(Error::Io)?;
    eprintln!("generated an identity in {}", path.display());
    Ok(identity)
}

/// Writes `bytes` to `path`, readable by the owner only.
fn write_private(path: &PathBuf, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)
}
