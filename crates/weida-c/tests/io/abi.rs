//! Round trips through the C ABI's exports, called the way a C program
//! calls them: raw pointers, status codes, out-parameters and descriptors.

use std::ffi::{CStr, CString, c_char, c_int};
use std::ptr;
use std::sync::mpsc;
use std::time::Duration;

use weida::*;

/// The expected time of one wait: past it a wait reports and goes on.
const WAIT: Duration = Duration::from_secs(10);

/// A pointer moved to another thread; the ABI makes every handle usable
/// from any thread (R5).
struct Sent<T>(*mut T);
unsafe impl<T> Send for Sent<T> {}

fn last_message() -> String {
    // SAFETY: read on the thread that failed, before its next failure.
    let text = unsafe { weida_last_error_message() };
    if text.is_null() {
        return "(no error recorded)".to_owned();
    }
    // SAFETY: a non-NULL message is a NUL-terminated string.
    unsafe { CStr::from_ptr(text) }
        .to_string_lossy()
        .into_owned()
}

#[track_caller]
fn ok(rc: c_int) {
    assert_eq!(rc, 0, "failed with {rc}: {}", last_message());
}

/// Calls `attempt` with a short timeout until it stops answering
/// `-WEIDA_EAGAIN`, reporting past the expected time.
fn settle(what: &str, mut attempt: impl FnMut(i64) -> c_int) -> c_int {
    let mut rc = -WEIDA_EAGAIN;
    weida_testkit::poll_until(what, WAIT, || {
        rc = attempt(20);
        rc != -WEIDA_EAGAIN
    });
    rc
}

fn c(text: &str) -> CString {
    CString::new(text).expect("no NUL")
}

/// A string getter's result as an owned string; `None` for NULL.
fn text(ptr: *const c_char, len: usize) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: a getter's non-NULL result points to `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    Some(String::from_utf8(bytes.to_vec()).expect("UTF-8"))
}

fn runtime() -> *mut weida_runtime {
    let mut rt = ptr::null_mut();
    // SAFETY: NULL configuration and a writable out-parameter.
    ok(unsafe { weida_runtime_owned(ptr::null(), &mut rt) });
    rt
}

fn listener(rt: *mut weida_runtime) -> *mut weida_listener {
    let mut l = ptr::null_mut();
    // SAFETY: a live runtime and a writable out-parameter.
    ok(unsafe { weida_runtime_listener(rt, &mut l) });
    l
}

fn replier(l: *mut weida_listener, path: &str) -> *mut weida_replier {
    let mut r = ptr::null_mut();
    let path = c(path);
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_replier(l, path.as_ptr(), &mut r) });
    r
}

fn trust_by_address() -> *mut weida_trust {
    let mut t = ptr::null_mut();
    // SAFETY: a writable out-parameter.
    ok(unsafe { weida_trust_new(&mut t) });
    t
}

fn requester(rt: *mut weida_runtime, trust: *mut weida_trust) -> *mut weida_requester {
    let mut tls = ptr::null_mut();
    let mut r = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_client_tls_new(trust, &mut tls));
        ok(weida_runtime_requester(rt, tls, &mut r));
        weida_client_tls_drop(tls);
    }
    r
}

/// Dials `url` and waits for the dial: the status of the connect op.
fn connect(r: *mut weida_requester, url: &str) -> c_int {
    let url = c(url);
    let mut op = ptr::null_mut();
    // SAFETY: a live requester, a C string and a writable out-parameter.
    ok(unsafe { weida_requester_connect(r, url.as_ptr(), &mut op) });
    // SAFETY: a live op.
    let rc = settle("the dial", |ms| unsafe {
        weida_requester_connect_op_wait(op, ms)
    });
    // SAFETY: the op is dropped once.
    unsafe { weida_requester_connect_op_drop(op) };
    rc
}

fn request(r: *mut weida_requester, body: &[u8]) -> *mut weida_requester_request_op {
    let mut op = ptr::null_mut();
    // SAFETY: a live requester, `body.len()` readable bytes and a writable
    // out-parameter.
    ok(unsafe { weida_requester_request(r, body.as_ptr(), body.len(), &mut op) });
    op
}

fn accept(r: *mut weida_replier) -> *mut weida_incoming_request {
    let mut q = ptr::null_mut();
    // SAFETY: a live replier and a writable out-parameter.
    ok(settle("a request", |ms| unsafe {
        weida_replier_accept(r, ms, &mut q)
    }));
    q
}

fn take_body(q: *mut weida_incoming_request) -> *mut weida_incoming_transfer {
    let mut body = ptr::null_mut();
    // SAFETY: a live request and a writable out-parameter.
    ok(unsafe { weida_incoming_request_take_body(q, &mut body) });
    body
}

/// Reads `t` to its end in pieces of at most `cap` bytes.
fn read_all(t: *mut weida_incoming_transfer, cap: usize) -> Result<Vec<u8>, c_int> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; cap];
    loop {
        let mut n = 0usize;
        // SAFETY: a live transfer, `cap` writable bytes and a writable count.
        let rc = settle("payload bytes", |ms| unsafe {
            weida_incoming_transfer_read(t, buf.as_mut_ptr(), cap, ms, &mut n)
        });
        if rc != 0 {
            return Err(rc);
        }
        if n == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&buf[..n]);
    }
}

fn write_all(t: *mut weida_outgoing_transfer, mut data: &[u8]) {
    while !data.is_empty() {
        let mut n = 0usize;
        // SAFETY: a live transfer, `data.len()` readable bytes and a
        // writable count.
        ok(settle("flow control", |ms| unsafe {
            weida_outgoing_transfer_write(t, data.as_ptr(), data.len(), ms, &mut n)
        }));
        data = &data[n..];
    }
}

/// Opens the reply to `q`, consuming it, with `content_type` when given.
fn reply(
    q: *mut weida_incoming_request,
    content_type: Option<&str>,
) -> *mut weida_outgoing_transfer {
    let mut meta = ptr::null_mut();
    let mut op = ptr::null_mut();
    let mut out = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters; `q` is consumed.
    unsafe {
        if let Some(content_type) = content_type {
            let content_type = c(content_type);
            ok(weida_transfer_meta_new(&mut meta));
            ok(weida_transfer_meta_set_content_type(
                meta,
                content_type.as_ptr(),
            ));
        }
        ok(weida_incoming_request_reply(q, meta, &mut op));
        weida_transfer_meta_drop(meta);
        ok(settle("the reply stream", |ms| {
            weida_incoming_request_reply_op_wait(op, ms, &mut out)
        }));
        weida_incoming_request_reply_op_drop(op);
    }
    out
}

/// Collects `t` whole under `max`, consuming it: the message or the code.
fn collect(t: *mut weida_incoming_transfer, max: u64) -> Result<*mut weida_msg, c_int> {
    let mut op = ptr::null_mut();
    let mut msg = ptr::null_mut();
    // SAFETY: a live transfer, consumed, and writable out-parameters.
    unsafe {
        ok(weida_incoming_transfer_collect(t, max, &mut op));
        let rc = settle("the collected body", |ms| {
            weida_incoming_transfer_collect_op_wait(op, ms, &mut msg)
        });
        weida_incoming_transfer_collect_op_drop(op);
        if rc == 0 { Ok(msg) } else { Err(rc) }
    }
}

fn content_type(meta: *mut weida_incoming_meta) -> Option<String> {
    let mut len = 0usize;
    // SAFETY: live metadata and a writable length.
    let ptr = unsafe { weida_incoming_meta_content_type(meta, &mut len) };
    text(ptr, len)
}

fn shutdown(rt: *mut weida_runtime) {
    let mut op = ptr::null_mut();
    // SAFETY: a live runtime, consumed, and a writable out-parameter.
    unsafe {
        ok(weida_runtime_shutdown(rt, &mut op));
        ok(settle("the shutdown", |ms| {
            weida_runtime_shutdown_op_wait(op, ms)
        }));
        weida_runtime_shutdown_op_drop(op);
    }
}

#[test]
fn a_streamed_round_trip_over_inproc() {
    let rt = runtime();
    let l = listener(rt);
    let mut lb = ptr::null_mut();
    let bus = c("abi-round-trip");
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_bind_inproc(l, bus.as_ptr(), &mut lb) });
    let rep = replier(l, "/echo");
    let trust = trust_by_address();
    let req = requester(rt, trust);
    ok(connect(req, "weida+inproc://abi-round-trip/echo"));

    let pending = request(req, b"ping");
    let q = accept(rep);

    let mut meta = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: a live request and writable out-parameters.
    unsafe {
        ok(weida_incoming_request_meta(q, &mut meta));
        let endpoint = weida_incoming_meta_endpoint(meta, &mut len);
        assert_eq!(text(endpoint, len).as_deref(), Some("/echo"));
        weida_incoming_meta_drop(meta);
    }

    let body = take_body(q);
    assert_eq!(read_all(body, 3).expect("the request body"), b"ping");
    // SAFETY: the body is dropped once.
    unsafe { weida_incoming_transfer_drop(body) };

    let out = reply(q, Some("text/plain"));
    write_all(out, b"pong");
    let mut delivery = ptr::null_mut();
    let mut delivered = ptr::null_mut();
    // SAFETY: live handles, each consumed or dropped once.
    unsafe {
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        ok(weida_delivery_delivered(delivery, &mut delivered));
        ok(settle("the delivery receipt", |ms| {
            weida_delivery_delivered_op_wait(delivered, ms)
        }));
        weida_delivery_delivered_op_drop(delivered);
    }

    let mut answer = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(settle("the reply", |ms| {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        }));
        weida_requester_request_op_drop(pending);
        let mut meta = ptr::null_mut();
        ok(weida_incoming_transfer_meta(answer, &mut meta));
        assert_eq!(content_type(meta).as_deref(), Some("text/plain"));
        weida_incoming_meta_drop(meta);
    }
    let msg = collect(answer, 1024).expect("the reply body");
    // SAFETY: a live message, dropped once.
    unsafe {
        let data = std::slice::from_raw_parts(weida_msg_data(msg), weida_msg_len(msg));
        assert_eq!(data, b"pong");
        let mut meta = ptr::null_mut();
        ok(weida_msg_meta(msg, &mut meta));
        assert_eq!(content_type(meta).as_deref(), Some("text/plain"));
        weida_incoming_meta_drop(meta);
        weida_msg_drop(msg);

        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

#[test]
fn an_accept_with_nothing_pending_answers_eagain() {
    let rt = runtime();
    let l = listener(rt);
    let rep = replier(l, "/idle");
    let mut q = ptr::null_mut();
    // SAFETY: live handles and a writable out-parameter.
    unsafe {
        assert_eq!(weida_replier_accept(rep, 0, &mut q), -WEIDA_EAGAIN);
        assert!(q.is_null(), "an output is written only on success");
        weida_replier_drop(rep);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

fn readable(fd: i64, timeout_ms: i64) -> bool {
    let mut item = weida_poll_item {
        fd,
        events: WEIDA_POLLIN,
        revents: 0,
    };
    // SAFETY: one writable item whose descriptor belongs to a live handle.
    let n = unsafe { weida_poll(&mut item, 1, timeout_ms) };
    assert!(n >= 0, "weida_poll failed with {n}: {}", last_message());
    item.revents & WEIDA_POLLIN != 0
}

#[test]
fn a_repliers_descriptor_follows_its_queue() {
    let rt = runtime();
    let l = listener(rt);
    let mut lb = ptr::null_mut();
    let bus = c("abi-readiness");
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_bind_inproc(l, bus.as_ptr(), &mut lb) });
    let rep = replier(l, "/ready");
    let mut fd = -1i64;
    let mut q = ptr::null_mut();
    // SAFETY: a live replier and writable out-parameters.
    unsafe {
        ok(weida_replier_fd(rep, &mut fd));
        assert_eq!(weida_replier_accept(rep, 0, &mut q), -WEIDA_EAGAIN);
    }
    assert!(!readable(fd, 0), "nothing has arrived yet");

    let trust = trust_by_address();
    let req = requester(rt, trust);
    ok(connect(req, "weida+inproc://abi-readiness/ready"));
    let pending = request(req, b"knock");
    weida_testkit::poll_until("the replier's descriptor", WAIT, || readable(fd, 20));

    // SAFETY: a live replier and a writable out-parameter.
    unsafe {
        ok(weida_replier_accept(rep, 0, &mut q));
        assert_eq!(weida_replier_accept(rep, 0, &mut q), -WEIDA_EAGAIN);
    }
    assert!(
        !readable(fd, 0),
        "an EAGAIN leaves the descriptor unreadable"
    );

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_request_drop(q);
        weida_requester_request_op_drop(pending);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

/// An inproc pair: a runtime with a bound bus, a replier at `/p` and a
/// connected requester.
struct Pair {
    rt: *mut weida_runtime,
    l: *mut weida_listener,
    lb: *mut weida_local_binding,
    rep: *mut weida_replier,
    trust: *mut weida_trust,
    req: *mut weida_requester,
}

impl Pair {
    fn new(bus: &str) -> Pair {
        let rt = runtime();
        let l = listener(rt);
        let mut lb = ptr::null_mut();
        let name = c(bus);
        // SAFETY: a live listener, a C string and a writable out-parameter.
        ok(unsafe { weida_listener_bind_inproc(l, name.as_ptr(), &mut lb) });
        let rep = replier(l, "/p");
        let trust = trust_by_address();
        let req = requester(rt, trust);
        ok(connect(req, &format!("weida+inproc://{bus}/p")));
        Pair {
            rt,
            l,
            lb,
            rep,
            trust,
            req,
        }
    }

    fn close(self) {
        // SAFETY: every handle is dropped once.
        unsafe {
            weida_requester_drop(self.req);
            weida_trust_drop(self.trust);
            weida_replier_drop(self.rep);
            weida_local_binding_drop(self.lb);
            weida_listener_drop(self.l);
        }
        shutdown(self.rt);
    }
}

#[test]
fn a_collect_over_its_ceiling_fails_with_limit_exceeded() {
    let pair = Pair::new("abi-ceiling");
    let pending = request(pair.req, b"four");
    let q = accept(pair.rep);
    let body = take_body(q);
    assert_eq!(collect(body, 2).err(), Some(-WEIDA_ELIMIT_EXCEEDED));
    // SAFETY: each handle is dropped once.
    unsafe {
        weida_incoming_request_drop(q);
        weida_requester_request_op_drop(pending);
    }
    pair.close();
}

#[test]
fn a_reply_dropped_unfinished_reads_as_canceled() {
    let pair = Pair::new("abi-canceled");
    let pending = request(pair.req, b"go");
    let q = accept(pair.rep);
    let out = reply(q, None);
    write_all(out, b"ha");
    // SAFETY: dropped once, unfinished: a reset with CANCELED.
    unsafe { weida_outgoing_transfer_drop(out) };

    let mut answer = ptr::null_mut();
    // SAFETY: a live op and a writable out-parameter.
    unsafe {
        ok(settle("the reply", |ms| {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        }));
        weida_requester_request_op_drop(pending);
    }
    assert_eq!(read_all(answer, 64).err(), Some(-WEIDA_ECANCELED));
    // SAFETY: dropped once.
    unsafe { weida_incoming_transfer_drop(answer) };
    pair.close();
}

#[test]
fn dropping_a_replier_ends_a_blocked_accept_with_eclosed() {
    let pair = Pair::new("abi-drop-wakes");
    let rep = Sent(pair.rep);
    let (tx, rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let rep = rep;
        let mut q = ptr::null_mut();
        // SAFETY: the replier is live when the call starts; the test drops
        // it only while this call holds its own reference.
        let rc = unsafe { weida_replier_accept(rep.0, -1, &mut q) };
        tx.send(rc).expect("the test is listening");
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "an accept with nothing pending blocks"
    );
    // SAFETY: dropped once; the blocked call keeps its own reference.
    unsafe { weida_replier_drop(pair.rep) };
    let rc = weida_testkit::recv(&rx, "the blocked accept", WAIT).expect("a result");
    assert_eq!(rc, -WEIDA_ECLOSED);
    waiter.join().expect("the waiter");
    // SAFETY: every other handle is dropped once.
    unsafe {
        weida_requester_drop(pair.req);
        weida_trust_drop(pair.trust);
        weida_local_binding_drop(pair.lb);
        weida_listener_drop(pair.l);
    }
    shutdown(pair.rt);
}

#[test]
fn a_shutdown_finishes_with_an_exchange_abandoned() {
    let pair = Pair::new("abi-shutdown");
    let pending = request(pair.req, b"never answered");
    // SAFETY: dropped once, unfinished: the exchange is abandoned.
    unsafe { weida_requester_request_op_drop(pending) };
    pair.close();
}

#[test]
fn a_malformed_pin_is_an_invalid_fingerprint() {
    let trust = trust_by_address();
    let bad = c("nope");
    // SAFETY: a live trust and a C string.
    unsafe {
        assert_eq!(
            weida_trust_add_pin(trust, bad.as_ptr()),
            -WEIDA_EINVALID_FINGERPRINT
        );
        assert!(!weida_last_error_message().is_null());
        weida_trust_drop(trust);
    }
}

fn identity() -> (*mut weida_identity, String) {
    let mut id = ptr::null_mut();
    let mut fp = ptr::null();
    // SAFETY: writable out-parameters; the fingerprint lives with `id`.
    unsafe {
        ok(weida_identity_generate(&mut id));
        ok(weida_identity_fingerprint(id, &mut fp));
        (id, CStr::from_ptr(fp).to_str().expect("UTF-8").to_owned())
    }
}

#[test]
fn a_server_outside_the_pins_is_untrusted() {
    let rt = runtime();
    let l = listener(rt);
    let (server, server_fp) = identity();
    let (other, other_fp) = identity();
    let mut tls = ptr::null_mut();
    let mut bind = ptr::null_mut();
    let mut binding = ptr::null_mut();
    let addr = c("127.0.0.1:0");
    let mut len = 0usize;
    // SAFETY: live handles and writable out-parameters.
    let local = unsafe {
        ok(weida_server_tls_new(server, &mut tls));
        ok(weida_listener_bind_quic(l, addr.as_ptr(), tls, &mut bind));
        ok(settle("the bind", |ms| {
            weida_listener_bind_quic_op_wait(bind, ms, &mut binding)
        }));
        weida_listener_bind_quic_op_drop(bind);
        weida_server_tls_drop(tls);
        text(weida_binding_local_addr(binding, &mut len), len).expect("an address")
    };
    let rep = replier(l, "/echo");

    let trust = trust_by_address();
    let pin = c(&other_fp);
    // SAFETY: a live trust and a C string.
    ok(unsafe { weida_trust_add_pin(trust, pin.as_ptr()) });
    let req = requester(rt, trust);
    let mut rc = connect(req, &format!("weida://{local}/echo"));
    if rc == 0 {
        let pending = request(req, b"hello");
        let mut answer = ptr::null_mut();
        // SAFETY: a live op and a writable out-parameter.
        rc = settle("the refused exchange", |ms| unsafe {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        });
        // SAFETY: dropped once.
        unsafe { weida_requester_request_op_drop(pending) };
    }
    assert_eq!(rc, -WEIDA_EUNTRUSTED, "{}", last_message());
    // SAFETY: read on the thread that failed, before its next failure.
    let presented = unsafe { weida_last_error_fingerprint() };
    assert!(!presented.is_null());
    // SAFETY: a non-NULL fingerprint is a NUL-terminated string.
    let presented = unsafe { CStr::from_ptr(presented) }
        .to_str()
        .expect("UTF-8");
    assert_eq!(presented, server_fp);

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_binding_drop(binding);
        weida_identity_drop(server);
        weida_identity_drop(other);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

#[cfg(unix)]
#[test]
fn a_signal_ends_a_blocked_accept_with_eintr() {
    use nix::sys::pthread::{Pthread, pthread_kill, pthread_self};
    use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal, sigaction};

    extern "C" fn ignore(_: c_int) {}
    let action = SigAction::new(
        SigHandler::Handler(ignore),
        SaFlags::empty(),
        SigSet::empty(),
    );
    // SAFETY: the handler does nothing, which is async-signal-safe, and no
    // `SA_RESTART` is set, so a blocked `poll` returns `EINTR`.
    unsafe { sigaction(Signal::SIGUSR1, &action) }.expect("install the handler");

    let pair = Pair::new("abi-eintr");
    let rep = Sent(pair.rep);
    let (thread_tx, thread_rx) = mpsc::channel::<Pthread>();
    let (tx, rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let rep = rep;
        thread_tx
            .send(pthread_self())
            .expect("the test is listening");
        let mut q = ptr::null_mut();
        // SAFETY: the replier stays live until this thread is joined.
        let rc = unsafe { weida_replier_accept(rep.0, -1, &mut q) };
        // SAFETY: the same replier, on the same thread.
        let again = unsafe { weida_replier_accept(rep.0, 0, &mut q) };
        tx.send((rc, again)).expect("the test is listening");
    });
    let thread = weida_testkit::recv(&thread_rx, "the waiter's id", WAIT).expect("an id");
    // A signal sent before the waiter blocks interrupts nothing, so it is
    // sent again until the waiter answers.
    let (rc, again) = loop {
        pthread_kill(thread, Signal::SIGUSR1).expect("signal the waiter");
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(answer) => break answer,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(e) => panic!("the waiter is gone: {e}"),
        }
    };
    assert_eq!(rc, -WEIDA_EINTR);
    assert_eq!(again, -WEIDA_EAGAIN, "the interrupted accept lost nothing");
    waiter.join().expect("the waiter");
    pair.close();
}
