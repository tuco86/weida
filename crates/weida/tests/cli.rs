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
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// Long enough for a QUIC handshake on a loaded machine, short enough that a
/// hang is a failure rather than a coffee break.
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
}

impl Served {
    /// Starts `weida serve` with `args` and waits for the address line.
    fn start(args: &[&str]) -> Served {
        let mut child = Command::new(BIN)
            .arg("serve")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the binary");
        let stdout = child.stdout.take().expect("piped stdout");
        // On a thread, because a `serve` that never binds must time out here
        // rather than block the test for ever.
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let read = reader.read_line(&mut line);
            let _ = tx.send(read.map(|_| (line, reader)));
        });
        let (address, stdout) = match rx.recv_timeout(DEADLINE) {
            Ok(Ok((line, reader))) => (line.trim().to_owned(), reader),
            Ok(Err(e)) => panic!("reading the served address: {e}"),
            Err(_) => panic!("the binary printed no address within {DEADLINE:?}"),
        };
        assert!(
            !address.is_empty(),
            "serve must print the address a client dials"
        );
        Served {
            child,
            address,
            stdout: Some(stdout),
        }
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
    // on one: a client that hangs must fail this test, not own it.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let read = stdout.read_to_end(&mut out);
        let status = child.wait();
        let _ = tx.send(read.and(status).map(|s| (s.code().unwrap_or(-1), out)));
    });
    match rx.recv_timeout(DEADLINE) {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => panic!("running {args:?}: {e}"),
        Err(_) => panic!("{args:?} did not finish within {DEADLINE:?}"),
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

/// Reads `want` bytes from a running server's own stdout, bounded.
fn sunk(server: &mut Served, want: usize) -> Vec<u8> {
    let mut stdout = server.stdout.take().expect("the served stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = vec![0u8; want];
        let read = stdout.read_exact(&mut buf);
        let _ = tx.send(read.map(|()| buf));
    });
    match rx.recv_timeout(DEADLINE) {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => panic!("reading the sink's stdout: {e}"),
        Err(_) => panic!("the sink printed nothing within {DEADLINE:?}"),
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
