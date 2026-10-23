//! The `weida` binary, driven the way a user drives it.
//!
//! These tests assert what a *caller* observes — across four verbs, two
//! transports and four exit codes — and nothing about how the binary is
//! built: the address `serve` prints, the payload on stdout with nothing else
//! on it, and the exit codes a script branches on. The binary's path comes
//! from `CARGO_BIN_EXE_weida`, so there is no extra dependency and no
//! assumption about the target directory.
//!
//! Every child is killed when its handle drops and every wait is bounded, so
//! a binary that hangs fails these tests instead of stalling the suite.
//!
//! The whole file is behind `generate`, because `serve` mints a self-signed
//! identity and the binary therefore carries `required-features =
//! ["generate"]`: without the feature there is no binary to exercise.
#![cfg(feature = "generate")]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The expected time of one step: long enough for a QUIC handshake on a
/// loaded machine. Past it a step reports and keeps waiting; a hang ends with
/// the job's budget.
const DEADLINE: Duration = Duration::from_secs(20);

const BIN: &str = env!("CARGO_BIN_EXE_weida");

/// A child process that is killed when this goes out of scope, however the
/// test ends.
struct Served {
    child: Child,
    /// The address it printed on stdout, which is the whole client
    /// configuration.
    address: String,
    /// What is left of its stdout after the address line — a `--sink` writes
    /// every payload it receives there. Handed back by the reader thread, so
    /// the buffered bytes are not lost with it; `None` once a test has taken
    /// it. Read by the `--sink` tests on both platforms, each through its
    /// own local transport.
    stdout: Option<BufReader<std::process::ChildStdout>>,
    /// What a `--pub` reads its messages from. Held open, so that the test
    /// decides when each message goes out.
    stdin: Option<ChildStdin>,
}

impl Served {
    /// Starts `weida serve` with `args` and waits for the address line.
    fn start(args: &[&str]) -> Served {
        let mut child = Command::new(BIN)
            .arg("serve")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the binary");
        let stdout = child.stdout.take().expect("piped stdout");
        let stdin = child.stdin.take();
        // On a thread, because a `serve` that never binds would otherwise
        // block this one with nothing to report while it waits.
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let read = reader.read_line(&mut line);
            let _ = tx.send(read.map(|_| (line, reader)));
        });
        let (address, stdout) = match weida_testkit::recv(&rx, "the served address", DEADLINE) {
            Ok(Ok((line, reader))) => (line.trim().to_owned(), reader),
            Ok(Err(e)) => panic!("reading the served address: {e}"),
            Err(_) => panic!("the binary exited without printing an address"),
        };
        assert!(
            !address.is_empty(),
            "serve must print the address a client dials"
        );
        Served {
            child,
            address,
            stdout: Some(stdout),
            stdin,
        }
    }

    /// Writes one line to the served process's stdin: what a `--pub` reads as
    /// one message per line.
    fn say(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("a piped stdin");
        writeln!(stdin, "{line}").expect("write a message");
        stdin.flush().expect("flush a message");
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Runs one client verb with `payload` on stdin, and returns
/// `(exit code, stdout)`.
fn run(args: &[&str], payload: &[u8]) -> (i32, Vec<u8>) {
    let mut child = Command::new(BIN)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the binary");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(payload)
        .expect("write the payload");
    let mut stdout = child.stdout.take().expect("piped stdout");
    // The child is waited for on a thread for the same reason `start` reads
    // on one: a client that hangs is reported while it is waited for.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let read = stdout.read_to_end(&mut out);
        let status = child.wait();
        let _ = tx.send(read.and(status).map(|s| (s.code().unwrap_or(-1), out)));
    });
    match weida_testkit::recv(&rx, &format!("{args:?} to finish"), DEADLINE) {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => panic!("running {args:?}: {e}"),
        Err(_) => panic!("{args:?} ended without a result"),
    }
}

/// An address on the same host and port with a fingerprint that is not the
/// one that will answer.
fn wrong_key(address: &str) -> String {
    let at = address.find('@').expect("a served address names its key");
    format!("weida://sha256:{}{}", "0".repeat(64), &address[at..])
}

#[test]
fn a_request_round_trips_through_the_echo_server() {
    let server = Served::start(&["--echo", "weida://127.0.0.1:0/echo"]);
    let (code, out) = run(&["request", &server.address], b"hello from a test");
    assert_eq!(code, 0, "stdout was {:?}", String::from_utf8_lossy(&out));
    // The payload and nothing else: the receipt and the byte count are on
    // stderr, which is what makes `weida request … | sha256sum` meaningful.
    assert_eq!(out, b"hello from a test");
}

#[test]
fn the_exit_code_names_the_error_a_script_can_act_on() {
    let server = Served::start(&["--echo", "weida://127.0.0.1:0/echo"]);

    // A path nothing is registered under: `UnknownEndpoint`.
    let unknown = server.address.replace("/echo", "/nowhere");
    let (code, out) = run(&["request", &unknown], b"x");
    assert_eq!(code, 4, "unknown endpoint");
    assert!(out.is_empty(), "nothing may reach stdout on a failure");

    // The right host and port, the wrong key: `Untrusted`.
    let (code, out) = run(&["request", &wrong_key(&server.address)], b"x");
    assert_eq!(code, 6, "untrusted key");
    assert!(out.is_empty());

    // Not a command at all: usage.
    let (code, _) = run(&["frobnicate"], b"");
    assert_eq!(code, 2, "usage");
}

#[test]
fn help_and_version_are_answers_rather_than_errors() {
    let (code, out) = run(&["--version"], b"");
    assert_eq!(code, 0);
    let printed = String::from_utf8(out).expect("utf-8");
    assert!(
        printed.starts_with("weida ") && printed.contains(env!("CARGO_PKG_VERSION")),
        "{printed:?}"
    );

    let (code, out) = run(&["--help"], b"");
    assert_eq!(code, 0, "a help request is not a usage error");
    let printed = String::from_utf8(out).expect("utf-8");
    for verb in ["send", "request", "sub", "serve"] {
        assert!(printed.contains(verb), "usage must name {verb}");
    }
}

/// Reads `want` bytes from a running server's own stdout.
fn sunk(server: &mut Served, want: usize) -> Vec<u8> {
    let mut stdout = server.stdout.take().expect("the served stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = vec![0u8; want];
        let read = stdout.read_exact(&mut buf);
        let _ = tx.send(read.map(|()| buf));
    });
    match weida_testkit::recv(&rx, "the sink's stdout", DEADLINE) {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => panic!("reading the sink's stdout: {e}"),
        Err(_) => panic!("the sink's stdout ended without a result"),
    }
}

#[cfg(unix)]
#[test]
fn a_send_reaches_a_sink_over_a_unix_socket() {
    // A socket path the test owns, short enough for `sun_path`.
    let socket = std::env::temp_dir().join(format!("weida-cli-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);
    let encoded: String = socket
        .to_str()
        .expect("utf-8 path")
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect();
    let url = format!("weida+unix://{encoded}/sink");

    let mut server = Served::start(&["--sink", &url]);
    assert_eq!(server.address, url, "a local address needs no fingerprint");

    let (code, _) = run(&["send", &url], b"over a unix socket");
    assert_eq!(code, 0);

    // The sink writes what it received to its own stdout. It is still
    // running, so read exactly as many bytes as were sent.
    assert_eq!(
        sunk(&mut server, b"over a unix socket".len()),
        b"over a unix socket"
    );
    let _ = std::fs::remove_file(&socket);
}

/// The same claim on the platform whose local transport is a named pipe: the
/// verbs, the address and the framing are the transport's business and not
/// the CLI's, which is the property this asserts.
#[cfg(windows)]
#[test]
fn a_send_reaches_a_sink_over_a_named_pipe() {
    let url = format!("weida+pipe://weida-cli-{}/sink", std::process::id());
    let mut server = Served::start(&["--sink", &url]);
    assert_eq!(server.address, url, "a local address needs no fingerprint");

    let (code, _) = run(&["send", &url], b"over a named pipe");
    assert_eq!(code, 0);
    assert_eq!(
        sunk(&mut server, b"over a named pipe".len()),
        b"over a named pipe"
    );
}

/// A child killed when this goes out of scope, however the test ends.
struct Stopped(Child);

impl Drop for Stopped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts `sub` with `args`, its output piped so that a test can read it.
fn spawn_sub(args: &[&str]) -> Stopped {
    Stopped(
        Command::new(BIN)
            .arg("sub")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the subscriber"),
    )
}

/// Publishes `line` until `sub` exits, and returns its exit code with what it
/// wrote to stdout and stderr. A publish that lands before the subscription
/// is registered is dropped by design, so the line is repeated.
fn publish_until_exit(
    publisher: &mut Served,
    line: &str,
    sub: &mut Stopped,
) -> (i32, Vec<u8>, Vec<u8>) {
    let started = Instant::now();
    let mut reported = 0u32;
    loop {
        publisher.say(line);
        if let Some(status) = sub.0.try_wait().expect("poll the subscriber") {
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            sub.0
                .stdout
                .take()
                .expect("piped stdout")
                .read_to_end(&mut stdout)
                .expect("read the subscriber's stdout");
            sub.0
                .stderr
                .take()
                .expect("piped stderr")
                .read_to_end(&mut stderr)
                .expect("read the subscriber's stderr");
            return (status.code().unwrap_or(-1), stdout, stderr);
        }
        if started.elapsed() > DEADLINE * (reported + 1) {
            reported += 1;
            eprintln!("still waiting for the subscriber to finish: past {reported} x {DEADLINE:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `--max-bytes` is the ceiling on what `sub --framing length` holds: a
/// payload above it is refused, the cap is named, and nothing is framed out.
#[test]
fn a_length_framed_payload_above_max_bytes_is_refused() {
    let mut publisher = Served::start(&["--pub", "weida://127.0.0.1:0/md"]);
    let mut sub = spawn_sub(&[
        &publisher.address,
        "--framing",
        "length",
        "--max-bytes",
        "16",
        "--count",
        "1",
    ]);
    let message = format!("t {}", "x".repeat(64));
    let (code, stdout, stderr) = publish_until_exit(&mut publisher, &message, &mut sub);
    let stderr = String::from_utf8_lossy(&stderr);
    assert_eq!(code, 1, "stderr was {stderr:?}");
    assert!(
        stdout.is_empty(),
        "a refused payload must not be framed out"
    );
    assert!(stderr.contains("--max-bytes 16"), "stderr was {stderr:?}");
}

/// A topic is any UTF-8 a publisher sends, escape sequences included, so the
/// operator's terminal is given it escaped: no raw ESC byte reaches stderr.
#[test]
fn a_topic_with_escape_sequences_is_printed_escaped() {
    let mut publisher = Served::start(&["--pub", "weida://127.0.0.1:0/md"]);
    let mut sub = spawn_sub(&[&publisher.address, "--count", "1"]);
    let (code, _, stderr) = publish_until_exit(&mut publisher, "\u{1b}[2J hello", &mut sub);
    let text = String::from_utf8_lossy(&stderr);
    assert_eq!(code, 0, "stderr was {text:?}");
    assert!(!stderr.contains(&0x1b), "a raw ESC byte reached stderr");
    assert!(text.contains(r"\u{1b}[2J"), "stderr was {text:?}");
}
