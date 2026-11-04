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
    runtime_with(ptr::null())
}

/// A runtime on the configuration `cfg` holds, or the defaults for NULL.
fn runtime_with(cfg: *const weida_runtime_config) -> *mut weida_runtime {
    let mut rt = ptr::null_mut();
    // SAFETY: a live or NULL configuration and a writable out-parameter.
    ok(unsafe { weida_runtime_owned(cfg, &mut rt) });
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

/// Binds the inproc bus `bus` on a new listener of `rt` and registers a
/// replier at `/echo`.
fn inproc(
    rt: *mut weida_runtime,
    bus: &str,
) -> (
    *mut weida_listener,
    *mut weida_local_binding,
    *mut weida_replier,
) {
    let l = listener(rt);
    let mut lb = ptr::null_mut();
    let name = c(bus);
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_bind_inproc(l, name.as_ptr(), &mut lb) });
    (l, lb, replier(l, "/echo"))
}

fn trust_by_address() -> *mut weida_trust {
    let mut t = ptr::null_mut();
    // SAFETY: a writable out-parameter.
    ok(unsafe { weida_trust_new(&mut t) });
    t
}

fn requester(rt: *mut weida_runtime, trust: *mut weida_trust) -> *mut weida_requester {
    requester_with(rt, trust, None)
}

/// A requester under `trust` that presents `identity` when one is given.
fn requester_with(
    rt: *mut weida_runtime,
    trust: *mut weida_trust,
    identity: Option<*mut weida_identity>,
) -> *mut weida_requester {
    let mut tls = ptr::null_mut();
    let mut r = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_client_tls_new(trust, &mut tls));
        if let Some(identity) = identity {
            ok(weida_client_tls_set_identity(tls, identity));
        }
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

/// Opens an exchange on `r`, with `content_type` when given: the request
/// body and the reply stream.
fn open_exchange(
    r: *mut weida_requester,
    content_type: Option<&str>,
) -> (*mut weida_outgoing_transfer, *mut weida_reply_stream) {
    let mut meta = ptr::null_mut();
    let mut op = ptr::null_mut();
    let mut body = ptr::null_mut();
    let mut stream = ptr::null_mut();
    // SAFETY: live handles, each dropped once, and writable out-parameters.
    unsafe {
        if let Some(content_type) = content_type {
            let content_type = c(content_type);
            ok(weida_transfer_meta_new(&mut meta));
            ok(weida_transfer_meta_set_content_type(
                meta,
                content_type.as_ptr(),
            ));
        }
        ok(weida_requester_open(r, meta, &mut op));
        weida_transfer_meta_drop(meta);
        ok(settle("the exchange", |ms| {
            weida_requester_open_op_wait(op, ms, &mut body, &mut stream)
        }));
        weida_requester_open_op_drop(op);
    }
    (body, stream)
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

/// An hour: a drain that waited for its deadline would hang the test
/// rather than pass it.
const DRAIN_DEADLINE_MS: u64 = 3_600_000;

/// Drains `rt`, consuming it, and waits for the report:
/// `(delivered, outstanding)`.
fn drain(rt: *mut weida_runtime, deadline_ms: u64) -> (u64, u64) {
    let mut op = ptr::null_mut();
    let mut report = ptr::null_mut();
    // SAFETY: a live runtime, consumed; then a live op and a live report,
    // each dropped once.
    unsafe {
        ok(weida_runtime_drain(rt, deadline_ms, &mut op));
        ok(settle("the drain", |ms| {
            weida_runtime_drain_op_wait(op, ms, &mut report)
        }));
        weida_runtime_drain_op_drop(op);
        let counts = (
            weida_drained_delivered(report),
            weida_drained_outstanding(report),
        );
        weida_drained_drop(report);
        counts
    }
}

/// One number getter of a stats snapshot: `Some` when present.
fn stat(
    s: *mut weida_connection_stats,
    index: usize,
    get: unsafe extern "C" fn(*const weida_connection_stats, usize, *mut u64) -> c_int,
) -> Option<u64> {
    let mut n = 0u64;
    // SAFETY: a live snapshot and a writable out-parameter.
    (unsafe { get(s, index, &mut n) } == 1).then_some(n)
}

/// The requester's stats snapshot; the caller drops it.
fn connection_stats(r: *mut weida_requester) -> *mut weida_connection_stats {
    let mut s = ptr::null_mut();
    // SAFETY: a live requester and a writable out-parameter.
    ok(unsafe { weida_requester_connection_stats(r, &mut s) });
    s
}

/// The next peer event of `events`, waiting for it; never the end.
fn next_event(events: *mut weida_peer_events) -> *mut weida_peer_event {
    let mut event = ptr::null_mut();
    // SAFETY: a live reader and a writable out-parameter.
    ok(settle("a peer event", |ms| unsafe {
        weida_peer_events_recv(events, ms, &mut event)
    }));
    assert!(!event.is_null(), "the event stream ended");
    event
}

#[test]
fn a_streamed_round_trip_over_inproc() {
    let rt = runtime();
    let (l, lb, rep) = inproc(rt, "abi-round-trip");
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
fn a_refused_request_fails_the_requester_with_its_codes_error() {
    let pair = Pair::new("abi-refusals-codes");
    for (code, expected) in [
        (WEIDA_ERROR_CODE_REJECTED, -WEIDA_EREJECTED),
        (WEIDA_ERROR_CODE_UNKNOWN_ENDPOINT, -WEIDA_EUNKNOWN_ENDPOINT),
        (WEIDA_ERROR_CODE_UNSUPPORTED, -WEIDA_EUNSUPPORTED),
        (WEIDA_ERROR_CODE_INTERNAL, -WEIDA_ETRANSPORT),
        (WEIDA_ERROR_CODE_NO_REPLY, -WEIDA_ENO_REPLY),
    ] {
        let pending = request(pair.req, b"ask");
        let q = accept(pair.rep);
        let mut op = ptr::null_mut();
        let mut answer = ptr::null_mut();
        // SAFETY: a live request, consumed, live ops and writable
        // out-parameters; each op is dropped once.
        unsafe {
            ok(weida_incoming_request_refuse(q, code, &mut op));
            ok(settle("the refusal", |ms| {
                weida_incoming_request_refuse_op_wait(op, ms)
            }));
            weida_incoming_request_refuse_op_drop(op);
            assert_eq!(
                settle("the refused request", |ms| {
                    weida_requester_request_op_wait(pending, ms, &mut answer)
                }),
                expected,
                "{code}"
            );
            weida_requester_request_op_drop(pending);
        }
    }

    let pending = request(pair.req, b"ask");
    let q = accept(pair.rep);
    let mut op = ptr::null_mut();
    let mut answer = ptr::null_mut();
    // SAFETY: a live request, consumed even by the failing call, a live op
    // dropped once and writable out-parameters.
    unsafe {
        assert_eq!(weida_incoming_request_refuse(q, 6, &mut op), -WEIDA_EINVAL);
        assert!(op.is_null(), "an invalid code starts no refusal");
        assert_eq!(
            settle("the dropped request", |ms| {
                weida_requester_request_op_wait(pending, ms, &mut answer)
            }),
            -WEIDA_ENO_REPLY
        );
        weida_requester_request_op_drop(pending);
    }
    pair.close();
}

#[test]
fn canceled_settles_once_the_requester_drops_its_request() {
    let pair = Pair::new("abi-refusals-canceled");
    let pending = request(pair.req, b"wait");
    let q = accept(pair.rep);
    let mut canceled = ptr::null_mut();
    // SAFETY: a live request and op, each dropped once, and a writable
    // out-parameter. `q` stays live until the wait settled: its NO_REPLY
    // would finish the reply half for another reason.
    unsafe {
        ok(weida_incoming_request_canceled(q, &mut canceled));
        assert_eq!(
            weida_incoming_request_canceled_op_wait(canceled, 0),
            -WEIDA_EAGAIN,
            "the requester still waits"
        );
        weida_requester_request_op_drop(pending);
        ok(settle("the cancellation", |ms| {
            weida_incoming_request_canceled_op_wait(canceled, ms)
        }));
        weida_incoming_request_canceled_op_drop(canceled);
        weida_incoming_request_drop(q);
    }
    pair.close();
}

#[test]
fn an_opened_exchange_reads_its_reply_while_the_request_is_still_open() {
    let pair = Pair::new("abi-streams-open");
    let (out, stream) = open_exchange(pair.req, Some("text/plain"));
    for piece in [&b"one"[..], b"two", b"three"] {
        write_all(out, piece);
    }
    let q = accept(pair.rep);
    let mut meta = ptr::null_mut();
    // SAFETY: a live request and a writable out-parameter; dropped once.
    unsafe {
        ok(weida_incoming_request_meta(q, &mut meta));
        assert_eq!(content_type(meta).as_deref(), Some("text/plain"));
        weida_incoming_meta_drop(meta);
    }
    let body = take_body(q);
    let answer = reply(q, None);
    write_all(answer, b"ack");
    let mut delivery = ptr::null_mut();
    let mut op = ptr::null_mut();
    let mut incoming = ptr::null_mut();
    // SAFETY: live handles, each consumed or dropped once, and writable
    // out-parameters.
    unsafe {
        ok(weida_outgoing_transfer_finish(answer, &mut delivery));
        weida_delivery_drop(delivery);
        ok(weida_reply_stream_recv(stream, &mut op));
        ok(settle("the reply", |ms| {
            weida_reply_stream_recv_op_wait(op, ms, &mut incoming)
        }));
        weida_reply_stream_recv_op_drop(op);
    }
    assert_eq!(read_all(incoming, 64).expect("the reply"), b"ack");
    // SAFETY: each handle is consumed or dropped once; the request body is
    // finished only after the reply was read to its end.
    unsafe {
        weida_incoming_transfer_drop(incoming);
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        weida_delivery_drop(delivery);
    }
    assert_eq!(read_all(body, 4).expect("the request body"), b"onetwothree");
    // SAFETY: dropped once.
    unsafe { weida_incoming_transfer_drop(body) };
    pair.close();
}

#[test]
fn dropping_a_reply_stream_cancels_the_exchange() {
    let pair = Pair::new("abi-streams-drop");
    let (out, stream) = open_exchange(pair.req, None);
    let q = accept(pair.rep);
    let mut canceled = ptr::null_mut();
    // SAFETY: live handles, each dropped once, and a writable
    // out-parameter. `q` stays live until the wait settled: its NO_REPLY
    // would end the reply half for another reason.
    unsafe {
        ok(weida_incoming_request_canceled(q, &mut canceled));
        assert_eq!(
            weida_incoming_request_canceled_op_wait(canceled, 0),
            -WEIDA_EAGAIN,
            "the requester still listens"
        );
        weida_reply_stream_drop(stream);
        ok(settle("the cancellation", |ms| {
            weida_incoming_request_canceled_op_wait(canceled, ms)
        }));
        weida_incoming_request_canceled_op_drop(canceled);
        weida_incoming_request_drop(q);
        weida_outgoing_transfer_drop(out);
    }
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

/// The fingerprint text of `id`.
fn fingerprint_of(id: *mut weida_identity) -> String {
    let mut fp = ptr::null();
    // SAFETY: a live identity and a writable out-parameter; the
    // fingerprint lives with `id`.
    unsafe {
        ok(weida_identity_fingerprint(id, &mut fp));
        CStr::from_ptr(fp).to_str().expect("UTF-8").to_owned()
    }
}

fn identity() -> (*mut weida_identity, String) {
    let mut id = ptr::null_mut();
    // SAFETY: a writable out-parameter.
    ok(unsafe { weida_identity_generate(&mut id) });
    (id, fingerprint_of(id))
}

/// A generated identity whose certificate names `names`.
fn identity_for(names: &[&str]) -> (*mut weida_identity, String) {
    let names: Vec<CString> = names.iter().map(|n| c(n)).collect();
    let ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
    let mut id = ptr::null_mut();
    // SAFETY: `ptrs.len()` C strings and a writable out-parameter.
    ok(unsafe { weida_identity_generate_for(ptrs.as_ptr(), ptrs.len(), &mut id) });
    (id, fingerprint_of(id))
}

/// The bytes of a PEM handle; drops the handle.
fn pem_of(p: *mut weida_pem) -> Vec<u8> {
    // SAFETY: a live PEM whose data has `weida_pem_len` bytes, dropped once.
    unsafe {
        let bytes = std::slice::from_raw_parts(weida_pem_data(p), weida_pem_len(p)).to_vec();
        weida_pem_drop(p);
        bytes
    }
}

fn certificate_pem(id: *mut weida_identity) -> Vec<u8> {
    let mut p = ptr::null_mut();
    // SAFETY: a live identity and a writable out-parameter.
    ok(unsafe { weida_identity_certificate_pem(id, &mut p) });
    pem_of(p)
}

fn to_pem(id: *mut weida_identity) -> Vec<u8> {
    let mut p = ptr::null_mut();
    // SAFETY: a live identity and a writable out-parameter.
    ok(unsafe { weida_identity_to_pem(id, &mut p) });
    pem_of(p)
}

/// A trust holding `pin`, beside what each address names.
fn trust_pinning(pin: &str) -> *mut weida_trust {
    let trust = trust_by_address();
    let pin = c(pin);
    // SAFETY: a live trust and a C string.
    ok(unsafe { weida_trust_add_pin(trust, pin.as_ptr()) });
    trust
}

/// Binds `tls` on `127.0.0.1:0`: the binding and its local address, or the
/// status the bind's op ended with.
fn try_bind_quic(
    l: *mut weida_listener,
    tls: *mut weida_server_tls,
) -> Result<(*mut weida_binding, String), c_int> {
    let mut bind = ptr::null_mut();
    let mut binding = ptr::null_mut();
    let addr = c("127.0.0.1:0");
    let mut len = 0usize;
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_listener_bind_quic(l, addr.as_ptr(), tls, &mut bind));
        let rc = settle("the bind", |ms| {
            weida_listener_bind_quic_op_wait(bind, ms, &mut binding)
        });
        weida_listener_bind_quic_op_drop(bind);
        if rc != 0 {
            return Err(rc);
        }
        let local = text(weida_binding_local_addr(binding, &mut len), len).expect("an address");
        Ok((binding, local))
    }
}

fn bind_quic(l: *mut weida_listener, tls: *mut weida_server_tls) -> (*mut weida_binding, String) {
    try_bind_quic(l, tls)
        .unwrap_or_else(|rc| panic!("the bind failed with {rc}: {}", last_message()))
}

fn server_tls(identity: *mut weida_identity) -> *mut weida_server_tls {
    let mut tls = ptr::null_mut();
    // SAFETY: a live identity and a writable out-parameter.
    ok(unsafe { weida_server_tls_new(identity, &mut tls) });
    tls
}

/// Dials `url` and sends one request nobody answers: the connect op's
/// status, or, when that is 0, the request's. A refused handshake shows at
/// either one, depending on when the alert arrives.
fn exchange_status(req: *mut weida_requester, url: &str) -> c_int {
    let rc = connect(req, url);
    if rc != 0 {
        return rc;
    }
    let pending = request(req, b"hello");
    let mut answer = ptr::null_mut();
    // SAFETY: a live op and a writable out-parameter.
    let rc = settle("the refused exchange", |ms| unsafe {
        weida_requester_request_op_wait(pending, ms, &mut answer)
    });
    // SAFETY: dropped once.
    unsafe { weida_requester_request_op_drop(pending) };
    rc
}

/// Dials `url`, sends one request and answers it: the metadata of the
/// request as the replier accepted it.
fn round_trip(
    req: *mut weida_requester,
    rep: *mut weida_replier,
    url: &str,
) -> *mut weida_incoming_meta {
    ok(connect(req, url));
    exchange(req, rep)
}

/// Sends one request on the connection `req` holds and answers it at
/// `rep`: the metadata of the request as the replier accepted it.
fn exchange(req: *mut weida_requester, rep: *mut weida_replier) -> *mut weida_incoming_meta {
    let pending = request(req, b"hello");
    let q = accept(rep);
    let mut meta = ptr::null_mut();
    // SAFETY: a live request and a writable out-parameter.
    ok(unsafe { weida_incoming_request_meta(q, &mut meta) });
    let out = reply(q, None);
    write_all(out, b"hi");
    let mut delivery = ptr::null_mut();
    let mut delivered = ptr::null_mut();
    let mut answer = ptr::null_mut();
    // SAFETY: live handles, each consumed or dropped once.
    unsafe {
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        ok(weida_delivery_delivered(delivery, &mut delivered));
        ok(settle("the delivery receipt", |ms| {
            weida_delivery_delivered_op_wait(delivered, ms)
        }));
        weida_delivery_delivered_op_drop(delivered);
        ok(settle("the reply", |ms| {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        }));
        weida_requester_request_op_drop(pending);
    }
    assert_eq!(read_all(answer, 64).expect("the reply body"), b"hi");
    // SAFETY: dropped once.
    unsafe { weida_incoming_transfer_drop(answer) };
    meta
}

/// A configuration whose limits time a connection out after `ms` idle
/// milliseconds, its handshake included. The caller drops it.
fn config_idle_timeout(ms: u64) -> *mut weida_runtime_config {
    let mut cfg = ptr::null_mut();
    let mut limits = ptr::null_mut();
    // SAFETY: writable out-parameters and live handles; `limits` is dropped
    // once.
    unsafe {
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_limits_new(&mut limits));
        ok(weida_limits_set_idle_timeout_ms(limits, ms));
        ok(weida_runtime_config_set_limits(cfg, limits));
        weida_limits_drop(limits);
    }
    cfg
}

/// Closes `b` and waits until its port is free; the handle stays live.
fn close_binding(b: *mut weida_binding) {
    let mut op = ptr::null_mut();
    // SAFETY: a live binding and a writable out-parameter; the op is dropped
    // once.
    unsafe {
        ok(weida_binding_close(b, &mut op));
        ok(settle("the close", |ms| {
            weida_binding_close_op_wait(op, ms)
        }));
        weida_binding_close_op_drop(op);
    }
}

/// The next event of `events`, which must be a loss: its cause.
fn lost_cause(events: *mut weida_peer_events) -> c_int {
    let event = next_event(events);
    let mut cause: c_int = 0;
    // SAFETY: a live event and a writable out-parameter; dropped once.
    unsafe {
        assert_eq!(weida_peer_event_kind(event), WEIDA_PEER_EVENT_LOST);
        assert_eq!(weida_peer_event_cause(event, &mut cause), 1);
        weida_peer_event_drop(event);
    }
    cause
}

/// Reads `events` up to and including the next `CONNECTED`.
fn until_connected(events: *mut weida_peer_events) {
    loop {
        let event = next_event(events);
        // SAFETY: a live event, dropped once.
        let kind = unsafe { weida_peer_event_kind(event) };
        // SAFETY: as above.
        unsafe { weida_peer_event_drop(event) };
        if kind == WEIDA_PEER_EVENT_CONNECTED {
            return;
        }
    }
}

/// The next request at the relay `r`: the request, the key of its asker
/// and its target.
fn relay_accept(r: *mut weida_relay) -> (*mut weida_relay_request, Option<String>, Option<String>) {
    let mut q = ptr::null_mut();
    // SAFETY: a live relay and a writable out-parameter.
    ok(settle("a relay request", |ms| unsafe {
        weida_relay_accept(r, ms, &mut q)
    }));
    let mut len = 0usize;
    // SAFETY: a live request and a writable length.
    let peer = text(
        unsafe { weida_relay_request_peer_fingerprint(q, &mut len) },
        len,
    );
    // SAFETY: as above.
    let target = text(unsafe { weida_relay_request_target(q, &mut len) }, len);
    (q, peer, target)
}

/// Admits `q`, consuming it, and waits for the admission.
fn admit(q: *mut weida_relay_request) {
    let mut op = ptr::null_mut();
    // SAFETY: a live request, consumed; the op is dropped once.
    unsafe {
        ok(weida_relay_request_admit(q, &mut op));
        ok(settle("the admission", |ms| {
            weida_relay_request_admit_op_wait(op, ms)
        }));
        weida_relay_request_admit_op_drop(op);
    }
}

/// Refuses `q`, consuming it, and waits for the refusal.
fn refuse_relay(q: *mut weida_relay_request) {
    let mut op = ptr::null_mut();
    // SAFETY: a live request, consumed; the op is dropped once.
    unsafe {
        ok(weida_relay_request_refuse(q, &mut op));
        ok(settle("the refusal", |ms| {
            weida_relay_request_refuse_op_wait(op, ms)
        }));
        weida_relay_request_refuse_op_drop(op);
    }
}

/// Starts serving `l` through the relay at `relay_url`, registered under
/// `id`'s key and serving on `tls`: the op, not waited for.
fn start_bind_relay(
    l: *mut weida_listener,
    relay_url: &str,
    id: *mut weida_identity,
    tls: *mut weida_server_tls,
) -> *mut weida_listener_bind_relay_op {
    let trust = trust_by_address();
    let url = c(relay_url);
    let mut via = ptr::null_mut();
    let mut op = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters; the start copied
    // `via` and the trust, each dropped once.
    unsafe {
        ok(weida_client_tls_new(trust, &mut via));
        ok(weida_client_tls_set_identity(via, id));
        ok(weida_listener_bind_relay(
            l,
            url.as_ptr(),
            via,
            tls,
            &mut op,
        ));
        weida_client_tls_drop(via);
        weida_trust_drop(trust);
    }
    op
}

/// Waits for a relay bind op and drops it: the relayed binding or the
/// status.
fn finish_bind_relay(op: *mut weida_listener_bind_relay_op) -> Result<*mut weida_binding, c_int> {
    let mut binding = ptr::null_mut();
    // SAFETY: a live op and a writable out-parameter; dropped once.
    let rc = settle("the relay registration", |ms| unsafe {
        weida_listener_bind_relay_op_wait(op, ms, &mut binding)
    });
    // SAFETY: dropped once.
    unsafe { weida_listener_bind_relay_op_drop(op) };
    if rc == 0 { Ok(binding) } else { Err(rc) }
}

fn peer_fingerprint(meta: *mut weida_incoming_meta) -> Option<String> {
    let mut len = 0usize;
    // SAFETY: live metadata and a writable length.
    let ptr = unsafe { weida_incoming_meta_peer_fingerprint(meta, &mut len) };
    text(ptr, len)
}

/// The last failure's presented fingerprint.
fn last_fingerprint() -> Option<String> {
    // SAFETY: read on the thread that failed, before its next failure.
    let presented = unsafe { weida_last_error_fingerprint() };
    if presented.is_null() {
        return None;
    }
    // SAFETY: a non-NULL fingerprint is a NUL-terminated string.
    Some(
        unsafe { CStr::from_ptr(presented) }
            .to_str()
            .expect("UTF-8")
            .to_owned(),
    )
}

#[test]
fn a_server_outside_the_pins_is_untrusted() {
    let rt = runtime();
    let l = listener(rt);
    let (server, server_fp) = identity();
    let (other, other_fp) = identity();
    let tls = server_tls(server);
    let (binding, local) = bind_quic(l, tls);
    // SAFETY: dropped once.
    unsafe { weida_server_tls_drop(tls) };
    let rep = replier(l, "/echo");

    let trust = trust_pinning(&other_fp);
    let req = requester(rt, trust);
    let rc = exchange_status(req, &format!("weida://{local}/echo"));
    assert_eq!(rc, -WEIDA_EUNTRUSTED, "{}", last_message());
    assert_eq!(last_fingerprint().as_deref(), Some(server_fp.as_str()));

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

#[test]
fn a_trust_anchor_checks_the_name_the_dial_used() {
    let rt = runtime();
    let l = listener(rt);
    let rep = replier(l, "/echo");
    let mut anchors = Vec::new();
    let mut bindings = Vec::new();
    let mut identities = Vec::new();
    let mut outcomes = Vec::new();
    for name in ["127.0.0.1", "localhost"] {
        let (server, server_fp) = identity_for(&[name]);
        let tls = server_tls(server);
        let (binding, local) = bind_quic(l, tls);
        // SAFETY: dropped once.
        unsafe { weida_server_tls_drop(tls) };
        let trust = trust_by_address();
        let pem = certificate_pem(server);
        // SAFETY: a live trust and `pem.len()` readable bytes.
        ok(unsafe { weida_trust_add_anchor(trust, pem.as_ptr(), pem.len()) });
        let req = requester(rt, trust);
        let url = format!("weida://{local}/echo");
        outcomes.push((req, url, server_fp));
        anchors.push(trust);
        bindings.push(binding);
        identities.push(server);
    }

    let (named, named_url, _) = &outcomes[0];
    let meta = round_trip(*named, rep, named_url);
    assert_eq!(
        peer_fingerprint(meta),
        None,
        "the client dialled anonymously"
    );
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };

    let (unnamed, unnamed_url, unnamed_fp) = &outcomes[1];
    let rc = exchange_status(*unnamed, unnamed_url);
    assert_eq!(rc, -WEIDA_EUNTRUSTED, "{}", last_message());
    assert_eq!(last_fingerprint().as_deref(), Some(unnamed_fp.as_str()));

    // SAFETY: every handle is dropped once.
    unsafe {
        for (req, _, _) in outcomes {
            weida_requester_drop(req);
        }
        anchors.into_iter().for_each(|t| weida_trust_drop(t));
        bindings.into_iter().for_each(|b| weida_binding_drop(b));
        identities
            .into_iter()
            .for_each(|id| weida_identity_drop(id));
        weida_replier_drop(rep);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

#[test]
fn a_binding_that_requires_a_pinned_client_refuses_anonymous_and_strangers() {
    let rt = runtime();
    let l = listener(rt);
    let rep = replier(l, "/echo");
    let (server, server_fp) = identity();
    let (client, client_fp) = identity();
    let (stranger, _) = identity();
    let tls = server_tls(server);

    let empty = trust_by_address();
    // SAFETY: live handles.
    ok(unsafe { weida_server_tls_require_client(tls, empty) });
    let refused = try_bind_quic(l, tls).err();
    assert_eq!(refused, Some(-WEIDA_ETLS), "{}", last_message());

    let clients = trust_pinning(&client_fp);
    // SAFETY: live handles; the last requirement wins.
    ok(unsafe { weida_server_tls_require_client(tls, clients) });
    let (binding, local) = bind_quic(l, tls);
    let url = format!("weida://{local}/echo");

    let trust = trust_pinning(&server_fp);
    let anonymous = requester(rt, trust);
    let rc = exchange_status(anonymous, &url);
    assert_eq!(rc, -WEIDA_ETLS, "anonymous: {}", last_message());
    let unknown = requester_with(rt, trust, Some(stranger));
    let rc = exchange_status(unknown, &url);
    assert_eq!(rc, -WEIDA_ETLS, "a stranger: {}", last_message());
    let mut q = ptr::null_mut();
    // SAFETY: a live replier and a writable out-parameter.
    let rc = unsafe { weida_replier_accept(rep, 0, &mut q) };
    assert_eq!(rc, -WEIDA_EAGAIN, "nothing reached the server");

    let pinned = requester_with(rt, trust, Some(client));
    let meta = round_trip(pinned, rep, &url);
    assert_eq!(peer_fingerprint(meta), Some(client_fp));

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_drop(pinned);
        weida_requester_drop(unknown);
        weida_requester_drop(anonymous);
        weida_trust_drop(trust);
        weida_trust_drop(clients);
        weida_trust_drop(empty);
        weida_server_tls_drop(tls);
        weida_binding_drop(binding);
        weida_identity_drop(stranger);
        weida_identity_drop(client);
        weida_identity_drop(server);
        weida_replier_drop(rep);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

/// The DER of certificate `index` of the metadata's peer chain.
fn chain_cert(meta: *mut weida_incoming_meta, index: usize) -> Option<Vec<u8>> {
    let mut len = 0usize;
    // SAFETY: live metadata and a writable length.
    let der = unsafe { weida_incoming_meta_peer_chain_cert(meta, index, &mut len) };
    if der.is_null() {
        return None;
    }
    // SAFETY: a non-NULL certificate points to `len` bytes.
    Some(unsafe { std::slice::from_raw_parts(der, len) }.to_vec())
}

#[test]
fn a_binding_that_requires_any_key_exposes_the_fingerprint_and_chain() {
    // A device key certified by a user key, presented as `[device, user]`.
    let user_key = rcgen::KeyPair::generate().expect("user key");
    let user_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("user params");
    let user_cert = user_params.self_signed(&user_key).expect("user cert");
    let device_key = rcgen::KeyPair::generate().expect("device key");
    let device_cert = rcgen::CertificateParams::new(vec!["device".to_owned()])
        .expect("device params")
        .signed_by(&device_key, &rcgen::Issuer::new(user_params, &user_key))
        .expect("device cert");
    let chain_pem = device_cert.pem() + &user_cert.pem();
    let key_pem = device_key.serialize_pem();
    let mut client = ptr::null_mut();
    // SAFETY: readable buffers and a writable out-parameter.
    ok(unsafe {
        weida_identity_from_pem(
            chain_pem.as_ptr(),
            chain_pem.len(),
            key_pem.as_ptr(),
            key_pem.len(),
            &mut client,
        )
    });
    let client_fp = fingerprint_of(client);

    let rt = runtime();
    let l = listener(rt);
    let rep = replier(l, "/echo");
    let (server, server_fp) = identity();
    let tls = server_tls(server);
    // SAFETY: live server terms.
    ok(unsafe { weida_server_tls_require_any_key(tls) });
    let (binding, local) = bind_quic(l, tls);
    let url = format!("weida://{local}/echo");
    let trust = trust_pinning(&server_fp);

    let anonymous = requester(rt, trust);
    let rc = exchange_status(anonymous, &url);
    assert_eq!(rc, -WEIDA_ETLS, "anonymous: {}", last_message());

    let keyed = requester_with(rt, trust, Some(client));
    let meta = round_trip(keyed, rep, &url);
    let mut n = 0usize;
    // SAFETY: live metadata and a writable count.
    assert_eq!(
        unsafe { weida_incoming_meta_peer_chain_len(meta, &mut n) },
        1
    );
    assert_eq!(n, 2);
    assert_eq!(
        chain_cert(meta, 0).as_deref(),
        Some(device_cert.der().as_ref())
    );
    assert_eq!(
        chain_cert(meta, 1).as_deref(),
        Some(user_cert.der().as_ref())
    );
    assert_eq!(chain_cert(meta, 2), None);
    assert_eq!(peer_fingerprint(meta), Some(client_fp));

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_drop(keyed);
        weida_requester_drop(anonymous);
        weida_trust_drop(trust);
        weida_server_tls_drop(tls);
        weida_binding_drop(binding);
        weida_identity_drop(server);
        weida_identity_drop(client);
        weida_replier_drop(rep);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

#[test]
fn an_identity_written_to_a_file_reads_back_with_its_fingerprint() {
    let dir = std::env::temp_dir().join(format!("weida-c-abi-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the directory");
    let (id, fp) = identity();
    let pem = to_pem(id);
    let path = |name: &str| c(dir.join(name).to_str().expect("a UTF-8 path"));

    std::fs::write(dir.join("id.pem"), &pem).expect("id.pem");
    let mut one = ptr::null_mut();
    // SAFETY: a C string and a writable out-parameter.
    ok(unsafe { weida_identity_from_pem_file(path("id.pem").as_ptr(), &mut one) });
    assert_eq!(fingerprint_of(one), fp);

    std::fs::write(dir.join("cert.pem"), certificate_pem(id)).expect("cert.pem");
    std::fs::write(dir.join("key.pem"), &pem).expect("key.pem");
    let mut two = ptr::null_mut();
    // SAFETY: C strings and a writable out-parameter.
    ok(unsafe {
        weida_identity_from_pem_files(
            path("cert.pem").as_ptr(),
            path("key.pem").as_ptr(),
            &mut two,
        )
    });
    assert_eq!(fingerprint_of(two), fp);

    let mut missing = ptr::null_mut();
    let mut text = ptr::null();
    // SAFETY: a C string and writable out-parameters.
    unsafe {
        ok(weida_identity_from_pem_file(
            path("missing.pem").as_ptr(),
            &mut missing,
        ));
        assert_eq!(weida_identity_fingerprint(missing, &mut text), -WEIDA_ETLS);
    }

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_identity_drop(missing);
        weida_identity_drop(two);
        weida_identity_drop(one);
        weida_identity_drop(id);
    }
    std::fs::remove_dir_all(&dir).expect("remove the directory");
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

/// A configuration whose guarantee set asks for per-producer gap detection.
fn config_detecting_gaps() -> *mut weida_runtime_config {
    let mut cfg = ptr::null_mut();
    let mut g = ptr::null_mut();
    // SAFETY: writable out-parameters and live handles; `g` is dropped once.
    unsafe {
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_guarantee_set_new(&mut g));
        ok(weida_guarantee_set_set_ordering(
            g,
            WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT,
        ));
        ok(weida_runtime_config_set_guarantees(cfg, g));
        weida_guarantee_set_drop(g);
    }
    cfg
}

#[test]
fn a_guarantee_the_dialler_does_not_offer_fails_negotiation() {
    let cfg = config_detecting_gaps();
    let server = runtime_with(cfg);
    let (l, lb, rep) = inproc(server, "abi-config-guarantee");
    let url = "weida+inproc://abi-config-guarantee/echo";

    let plain = runtime();
    let trust = trust_by_address();
    let refused = requester(plain, trust);
    let rc = exchange_status(refused, url);
    assert_eq!(rc, -WEIDA_ENEGOTIATION, "{}", last_message());

    let alike = runtime_with(cfg);
    let req = requester(alike, trust);
    let meta = round_trip(req, rep, url);
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_drop(req);
        weida_requester_drop(refused);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
        weida_runtime_config_drop(cfg);
    }
    shutdown(alike);
    shutdown(plain);
    shutdown(server);
}

/// A fresh traceparent from `weida_new_trace`.
fn new_trace() -> CString {
    let mut buf = [0 as c_char; WEIDA_TRACEPARENT_LEN as usize + 1];
    // SAFETY: `buf` holds `buf.len()` writable bytes.
    ok(unsafe { weida_new_trace(buf.as_mut_ptr(), buf.len()) });
    // SAFETY: on success `buf` holds a NUL-terminated string.
    unsafe { CStr::from_ptr(buf.as_ptr()) }.to_owned()
}

fn meta_text(
    meta: *mut weida_incoming_meta,
    get: unsafe extern "C" fn(*const weida_incoming_meta, *mut usize) -> *const c_char,
) -> Option<String> {
    let mut len = 0usize;
    // SAFETY: live metadata and a writable length.
    let ptr = unsafe { get(meta, &mut len) };
    text(ptr, len)
}

#[test]
fn metadata_travels_with_a_request_and_its_reply() {
    let rt = runtime();
    let (l, lb, rep) = inproc(rt, "abi-config-meta");
    let trust = trust_by_address();
    let req = requester(rt, trust);
    ok(connect(req, "weida+inproc://abi-config-meta/echo"));
    let trace = new_trace();
    let t = trace.to_str().expect("UTF-8").to_owned();
    let topic = c("jobs");

    let mut meta = ptr::null_mut();
    let mut pending = ptr::null_mut();
    // SAFETY: live handles, C strings and writable out-parameters; `meta`
    // is copied by the request and dropped once.
    unsafe {
        ok(weida_transfer_meta_new(&mut meta));
        ok(weida_transfer_meta_set_content_len(meta, 4));
        ok(weida_transfer_meta_set_topic(meta, topic.as_ptr()));
        ok(weida_transfer_meta_set_trace(meta, trace.as_ptr()));
        ok(weida_requester_request_with(
            req,
            meta,
            b"ping".as_ptr(),
            4,
            &mut pending,
        ));
        weida_transfer_meta_drop(meta);
    }

    let q = accept(rep);
    let mut seen = ptr::null_mut();
    let mut len = 0u64;
    // SAFETY: a live request and writable out-parameters.
    unsafe {
        ok(weida_incoming_request_meta(q, &mut seen));
        assert_eq!(weida_incoming_meta_content_len(seen, &mut len), 1);
    }
    assert_eq!(len, 4);
    assert_eq!(
        meta_text(seen, weida_incoming_meta_endpoint).as_deref(),
        Some("/echo")
    );
    assert_eq!(
        meta_text(seen, weida_incoming_meta_topic).as_deref(),
        Some("jobs")
    );
    assert_eq!(meta_text(seen, weida_incoming_meta_trace), Some(t.clone()));
    assert_eq!(meta_text(seen, weida_incoming_meta_tracestate), None);
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(seen) };

    let mut answer_meta = ptr::null_mut();
    let mut op = ptr::null_mut();
    let mut out = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters; `q` is consumed.
    unsafe {
        ok(weida_transfer_meta_new(&mut answer_meta));
        ok(weida_transfer_meta_set_achieved(
            answer_meta,
            WEIDA_ACKNOWLEDGEMENT_ACCEPTED,
        ));
        ok(weida_incoming_request_reply(q, answer_meta, &mut op));
        weida_transfer_meta_drop(answer_meta);
        ok(settle("the reply stream", |ms| {
            weida_incoming_request_reply_op_wait(op, ms, &mut out)
        }));
        weida_incoming_request_reply_op_drop(op);
        ok(weida_outgoing_transfer_set_priority(out, 5));
        let mut len = 0usize;
        let inherited = weida_outgoing_transfer_trace(out, &mut len);
        assert_eq!(
            text(inherited, len),
            Some(t.clone()),
            "a reply inherits the trace"
        );
    }
    write_all(out, b"pong");
    let mut delivery = ptr::null_mut();
    let mut answer = ptr::null_mut();
    let mut level = 0;
    // SAFETY: live handles, each consumed or dropped once.
    unsafe {
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        weida_delivery_drop(delivery);
        ok(settle("the reply", |ms| {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        }));
        weida_requester_request_op_drop(pending);
        let mut got = ptr::null_mut();
        ok(weida_incoming_transfer_meta(answer, &mut got));
        assert_eq!(meta_text(got, weida_incoming_meta_trace), Some(t));
        assert_eq!(weida_incoming_meta_achieved(got, &mut level), 1);
        weida_incoming_meta_drop(got);
    }
    assert_eq!(level, WEIDA_ACKNOWLEDGEMENT_ACCEPTED);
    let msg = collect(answer, 64).expect("the reply body");
    // SAFETY: live handles, each dropped once.
    unsafe {
        let data = std::slice::from_raw_parts(weida_msg_data(msg), weida_msg_len(msg));
        assert_eq!(data, b"pong");
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
fn an_expired_reply_fails_its_finish_and_cancels_the_reader() {
    let rt = runtime();
    // SAFETY: a live runtime.
    assert_eq!(unsafe { weida_runtime_expired_transfers(rt) }, 0);
    let (l, lb, rep) = inproc(rt, "abi-config-expired");
    let trust = trust_by_address();
    let req = requester(rt, trust);
    ok(connect(req, "weida+inproc://abi-config-expired/echo"));
    let pending = request(req, b"go");
    let q = accept(rep);
    let out = reply(q, None);
    let mut delivery = ptr::null_mut();
    let mut answer = ptr::null_mut();
    // SAFETY: live handles; `out` is consumed by the finish.
    unsafe {
        ok(weida_outgoing_transfer_expire_after_ms(out, 0));
        assert_eq!(
            weida_outgoing_transfer_finish(out, &mut delivery),
            -WEIDA_EEXPIRED
        );
        assert!(delivery.is_null());
        assert_eq!(
            weida_runtime_expired_transfers(rt),
            1,
            "the expiry is counted once"
        );
        ok(settle("the reply", |ms| {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        }));
        weida_requester_request_op_drop(pending);
    }
    assert_eq!(collect(answer, 64).err(), Some(-WEIDA_ECANCELED));
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

#[test]
fn a_send_timeout_ends_a_request_to_a_peer_that_is_gone() {
    let server = runtime();
    let (l, lb, rep) = inproc(server, "abi-config-send-timeout");
    let mut cfg = ptr::null_mut();
    // SAFETY: a writable out-parameter, then a live configuration.
    unsafe {
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_runtime_config_set_send_timeout_ms(cfg, 100));
    }
    let client = runtime_with(cfg);
    let trust = trust_by_address();
    let req = requester(client, trust);
    ok(connect(req, "weida+inproc://abi-config-send-timeout/echo"));
    // SAFETY: every handle of the server is dropped once.
    unsafe {
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
        weida_runtime_config_drop(cfg);
    }
    shutdown(server);

    let pending = request(req, b"anyone?");
    let mut answer = ptr::null_mut();
    // SAFETY: a live op and a writable out-parameter.
    let rc = settle("the timed-out request", |ms| unsafe {
        weida_requester_request_op_wait(pending, ms, &mut answer)
    });
    assert_eq!(rc, -WEIDA_ECONNECTION_LOST, "{}", last_message());
    // SAFETY: read on the thread that failed, before its next failure.
    assert_ne!(unsafe { weida_last_error_loss_cause() }, 0);
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_requester_request_op_drop(pending);
        weida_requester_drop(req);
        weida_trust_drop(trust);
    }
    shutdown(client);
}

#[test]
fn limits_reach_the_quic_transport() {
    let mut cfg = ptr::null_mut();
    let mut limits = ptr::null_mut();
    // SAFETY: writable out-parameters and live handles, each dropped once.
    unsafe {
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_limits_new(&mut limits));
        ok(weida_limits_set_idle_timeout_ms(limits, u64::MAX));
        ok(weida_runtime_config_set_limits(cfg, limits));
        weida_limits_drop(limits);
    }
    let rt = runtime_with(cfg);
    let l = listener(rt);
    let (id, _) = identity();
    let tls = server_tls(id);
    let rc = try_bind_quic(l, tls).err();
    assert_eq!(rc, Some(-WEIDA_ERUNTIME));
    let message = last_message();
    assert!(message.contains("idle_timeout out of range"), "{message}");
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_server_tls_drop(tls);
        weida_identity_drop(id);
        weida_listener_drop(l);
        weida_runtime_config_drop(cfg);
    }
    shutdown(rt);
}

#[test]
fn an_inproc_connection_is_reported_with_its_url_and_no_transport() {
    let pair = Pair::new("abi-stats-inproc");
    let pending = request(pair.req, b"ping");
    let q = accept(pair.rep);
    let out = reply(q, None);
    write_all(out, b"pong");
    let mut delivery = ptr::null_mut();
    let mut answer = ptr::null_mut();
    // SAFETY: live handles, each consumed or dropped once.
    unsafe {
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        weida_delivery_drop(delivery);
        ok(settle("the reply", |ms| {
            weida_requester_request_op_wait(pending, ms, &mut answer)
        }));
        weida_requester_request_op_drop(pending);
    }
    assert_eq!(read_all(answer, 64).expect("the reply"), b"pong");
    // SAFETY: dropped once.
    unsafe { weida_incoming_transfer_drop(answer) };

    let s = connection_stats(pair.req);
    let mut len = 0usize;
    // SAFETY: a live snapshot and writable out-parameters.
    unsafe {
        assert_eq!(weida_connection_stats_len(s), 1);
        assert_eq!(
            text(weida_connection_stats_url(s, 0, &mut len), len).as_deref(),
            Some("weida+inproc://abi-stats-inproc/p")
        );
    }
    assert_eq!(stat(s, 0, weida_connection_stats_redials), Some(0));
    assert!(stat(s, 0, weida_connection_stats_age_us).is_some());
    assert_eq!(
        stat(s, 0, weida_connection_stats_transport_path_rtt_us),
        None,
        "in process there is no path"
    );
    assert_eq!(stat(s, 0, weida_connection_stats_remote_rtt_us), None);
    // SAFETY: a live snapshot, then dropped once.
    unsafe {
        assert!(weida_connection_stats_url(s, 1, ptr::null_mut()).is_null());
    }
    assert_eq!(stat(s, 1, weida_connection_stats_age_us), None);
    // SAFETY: each handle is dropped once; the runtime is live.
    unsafe {
        weida_connection_stats_drop(s);
        assert_eq!(weida_runtime_suppressed_duplicates(pair.rt), 0);
    }
    pair.close();
}

#[test]
fn a_request_and_its_body_name_the_same_connection() {
    let pair = Pair::new("abi-stats-connection");
    let first = request(pair.req, b"one");
    let q = accept(pair.rep);
    let mut id = 0u64;
    let mut body_id = 0u64;
    // SAFETY: a live request and a writable out-parameter.
    ok(unsafe { weida_incoming_request_connection(q, &mut id) });
    let body = take_body(q);
    // SAFETY: a live transfer and a writable out-parameter.
    ok(unsafe { weida_incoming_transfer_connection(body, &mut body_id) });
    assert_eq!(id, body_id);

    let other_rt = runtime();
    let other_trust = trust_by_address();
    let other = requester(other_rt, other_trust);
    ok(connect(other, "weida+inproc://abi-stats-connection/p"));
    let second = request(other, b"two");
    let q2 = accept(pair.rep);
    let mut other_id = 0u64;
    // SAFETY: a live request and a writable out-parameter.
    ok(unsafe { weida_incoming_request_connection(q2, &mut other_id) });
    assert_ne!(id, other_id, "another runtime dials another connection");

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_request_drop(q2);
        weida_requester_request_op_drop(second);
        weida_incoming_transfer_drop(body);
        weida_incoming_request_drop(q);
        weida_requester_request_op_drop(first);
        weida_requester_drop(other);
        weida_trust_drop(other_trust);
    }
    shutdown(other_rt);
    pair.close();
}

#[test]
fn a_quic_connection_reports_its_transport() {
    let rt = runtime();
    let l = listener(rt);
    let (server, server_fp) = identity();
    let tls = server_tls(server);
    let (binding, local) = bind_quic(l, tls);
    // SAFETY: dropped once.
    unsafe { weida_server_tls_drop(tls) };
    let rep = replier(l, "/echo");
    let trust = trust_pinning(&server_fp);
    let req = requester(rt, trust);
    let url = format!("weida://{local}/echo");
    let meta = round_trip(req, rep, &url);
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };

    let s = connection_stats(req);
    let mut len = 0usize;
    // SAFETY: a live snapshot and a writable length.
    unsafe {
        assert_eq!(weida_connection_stats_len(s), 1);
        assert_eq!(
            text(weida_connection_stats_url(s, 0, &mut len), len),
            Some(url)
        );
    }
    assert!(stat(s, 0, weida_connection_stats_transport_path_rtt_us).is_some());
    for (what, get) in [
        (
            "sent packets",
            weida_connection_stats_transport_path_sent_packets
                as unsafe extern "C" fn(*const weida_connection_stats, usize, *mut u64) -> c_int,
        ),
        (
            "datagrams sent",
            weida_connection_stats_transport_tx_datagrams,
        ),
        ("bytes received", weida_connection_stats_transport_rx_bytes),
    ] {
        let n = stat(s, 0, get).unwrap_or_else(|| panic!("{what} are present"));
        assert!(n > 0, "{what}: {n}");
    }
    let mtu = stat(s, 0, weida_connection_stats_transport_path_current_mtu).expect("the MTU");
    assert!(mtu >= 1200, "the MTU: {mtu}");
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_connection_stats_drop(s);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_binding_drop(binding);
        weida_identity_drop(server);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

#[test]
fn a_replier_runtime_shut_down_reports_lost_then_retrying() {
    let server = runtime();
    let (l, lb, rep) = inproc(server, "abi-stats-events");
    let client = runtime();
    let trust = trust_by_address();
    let req = requester(client, trust);
    let url = "weida+inproc://abi-stats-events/echo";
    ok(connect(req, url));
    let mut events = ptr::null_mut();
    let mut event = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters; the server's handles
    // are dropped once.
    unsafe {
        ok(weida_requester_events(req, &mut events));
        assert_eq!(
            weida_peer_events_recv(events, 0, &mut event),
            -WEIDA_EAGAIN,
            "nothing happened since the reader was made"
        );
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(server);

    let mut len = 0usize;
    let mut cause: c_int = 0;
    let mut attempt = 0u32;
    let mut delay = 0u64;
    let lost = next_event(events);
    // SAFETY: live events and writable out-parameters; each event is dropped
    // once.
    unsafe {
        assert_eq!(weida_peer_event_kind(lost), WEIDA_PEER_EVENT_LOST);
        assert_eq!(
            text(weida_peer_event_url(lost, &mut len), len).as_deref(),
            Some(url)
        );
        assert_eq!(weida_peer_event_cause(lost, &mut cause), 1);
        assert!(
            (WEIDA_LOSS_IDLE_TIMEOUT..=WEIDA_LOSS_TRANSPORT_ERROR).contains(&cause),
            "{cause}"
        );
        weida_peer_event_drop(lost);
    }
    let retrying = next_event(events);
    // SAFETY: as above.
    unsafe {
        assert_eq!(weida_peer_event_kind(retrying), WEIDA_PEER_EVENT_RETRYING);
        assert_eq!(weida_peer_event_attempt(retrying, &mut attempt), 1);
        assert_eq!(attempt, 1);
        assert_eq!(weida_peer_event_delay_us(retrying, &mut delay), 1);
        assert_eq!(
            weida_peer_event_cause(retrying, &mut cause),
            0,
            "a field of another kind is absent"
        );
        weida_peer_event_drop(retrying);
    }

    // SAFETY: dropped once.
    unsafe { weida_requester_drop(req) };
    let mut end = ptr::null_mut();
    // SAFETY: a live reader and a writable out-parameter.
    ok(settle("the end of the event stream", |ms| unsafe {
        weida_peer_events_recv(events, ms, &mut end)
    }));
    assert!(end.is_null(), "a requester that is gone ends its events");
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_peer_events_drop(events);
        weida_trust_drop(trust);
    }
    shutdown(client);
}

#[test]
fn a_drain_counts_a_reply_finished_before_it_as_delivered() {
    let server = runtime();
    let (l, lb, rep) = inproc(server, "abi-drain-reply");
    let client = runtime();
    let trust = trust_by_address();
    let req = requester(client, trust);
    ok(connect(req, "weida+inproc://abi-drain-reply/echo"));
    let pending = request(req, b"ping");
    let q = accept(rep);
    let out = reply(q, None);
    write_all(out, b"pong");
    let mut delivery = ptr::null_mut();
    // SAFETY: a live transfer, consumed; the receipt is dropped unwaited,
    // which parks it where the drain finds it.
    unsafe {
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        weida_delivery_drop(delivery);
    }
    // The requester has not read the reply: the exchange is in flight.
    assert_eq!(
        drain(server, DRAIN_DEADLINE_MS),
        (1, 0),
        "the reply's parked receipt is what the drain reports"
    );
    // SAFETY: every handle is dropped once; the server runtime is consumed.
    unsafe {
        weida_requester_request_op_drop(pending);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(client);
}

#[test]
fn a_drain_with_nothing_pending_returns_at_once() {
    let pair = Pair::new("abi-drain-idle");
    let mut op = ptr::null_mut();
    // SAFETY: a NULL runtime and a writable out-parameter.
    assert_eq!(
        unsafe { weida_runtime_drain(ptr::null_mut(), 0, &mut op) },
        -WEIDA_EINVAL
    );
    assert!(op.is_null());
    assert_eq!(
        drain(pair.rt, DRAIN_DEADLINE_MS),
        (0, 0),
        "an idle connection leaves nothing to wait for"
    );
    // SAFETY: NULL reports read 0.
    unsafe {
        assert_eq!(weida_drained_delivered(ptr::null()), 0);
        assert_eq!(weida_drained_outstanding(ptr::null()), 0);
    }
    // SAFETY: every handle is dropped once; the runtime is consumed.
    unsafe {
        weida_requester_drop(pair.req);
        weida_trust_drop(pair.trust);
        weida_replier_drop(pair.rep);
        weida_local_binding_drop(pair.lb);
        weida_listener_drop(pair.l);
    }
}

#[test]
fn a_closed_binding_refuses_the_next_dial() {
    let rt = runtime();
    let l = listener(rt);
    let (server, server_fp) = identity();
    let tls = server_tls(server);
    let (binding, local) = bind_quic(l, tls);
    let rep = replier(l, "/echo");
    let url = format!("weida://{server_fp}@{local}/echo");
    let client = runtime();
    let trust = trust_by_address();
    let req = requester(client, trust);
    let meta = round_trip(req, rep, &url);
    let mut events = ptr::null_mut();
    // SAFETY: live handles and a writable out-parameter; the meta is
    // dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        ok(weida_requester_events(req, &mut events));
    }

    close_binding(binding);
    assert_eq!(lost_cause(events), WEIDA_LOSS_PEER_CLOSED);

    let cfg = config_idle_timeout(1_000);
    let late = runtime_with(cfg);
    let late_req = requester(late, trust);
    // Nobody answers on the closed port; the handshake times out.
    assert_eq!(
        connect(late_req, &url),
        -WEIDA_ECONNECTION_LOST,
        "{}",
        last_message()
    );
    // SAFETY: read on the thread that failed, before its next failure.
    assert_eq!(
        unsafe { weida_last_error_loss_cause() },
        WEIDA_LOSS_IDLE_TIMEOUT
    );

    let mut op = ptr::null_mut();
    // SAFETY: a live binding, consumed, and a writable out-parameter.
    assert_eq!(
        unsafe { weida_binding_hand_over(binding, &mut op) },
        -WEIDA_ECLOSED
    );
    assert!(op.is_null());
    // SAFETY: every handle is dropped once; the binding was consumed.
    unsafe {
        weida_peer_events_drop(events);
        weida_requester_drop(late_req);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_runtime_config_drop(cfg);
        weida_replier_drop(rep);
        weida_server_tls_drop(tls);
        weida_identity_drop(server);
        weida_listener_drop(l);
    }
    shutdown(late);
    shutdown(client);
    shutdown(rt);
}

#[test]
fn a_binding_disconnects_a_peer_by_its_key() {
    let rt = runtime();
    let l = listener(rt);
    let (server, server_fp) = identity();
    let tls = server_tls(server);
    // SAFETY: live server terms.
    ok(unsafe { weida_server_tls_require_any_key(tls) });
    let (binding, local) = bind_quic(l, tls);
    let rep = replier(l, "/echo");
    let client = runtime();
    let trust = trust_by_address();
    let (client_id, client_fp) = identity();
    let req = requester_with(client, trust, Some(client_id));
    let meta = round_trip(req, rep, &format!("weida://{server_fp}@{local}/echo"));
    let mut events = ptr::null_mut();
    // SAFETY: live handles and a writable out-parameter; the meta is
    // dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        ok(weida_requester_events(req, &mut events));
    }

    let (stranger, stranger_fp) = identity();
    let mut n = usize::MAX;
    let stranger_key = c(&stranger_fp);
    let nope = c("nope");
    let client_key = c(&client_fp);
    // SAFETY: a live binding, C strings and a writable out-parameter.
    unsafe {
        ok(weida_binding_disconnect(
            binding,
            stranger_key.as_ptr(),
            &mut n,
        ));
        assert_eq!(n, 0, "nobody of that key is connected");
        assert_eq!(
            weida_binding_disconnect(binding, nope.as_ptr(), &mut n),
            -WEIDA_EINVALID_FINGERPRINT
        );
        ok(weida_binding_disconnect(
            binding,
            client_key.as_ptr(),
            &mut n,
        ));
    }
    assert_eq!(n, 1);
    assert_eq!(lost_cause(events), WEIDA_LOSS_PEER_CLOSED);
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_peer_events_drop(events);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_identity_drop(client_id);
        weida_identity_drop(stranger);
        weida_replier_drop(rep);
        weida_binding_drop(binding);
        weida_server_tls_drop(tls);
        weida_identity_drop(server);
        weida_listener_drop(l);
    }
    shutdown(client);
    shutdown(rt);
}

#[test]
fn a_hand_over_keeps_a_requesters_connection() {
    let old = runtime();
    let l = listener(old);
    let (server, server_fp) = identity();
    let tls = server_tls(server);
    let (binding, local) = bind_quic(l, tls);
    let rep = replier(l, "/echo");
    let client = runtime();
    let trust = trust_by_address();
    let req = requester(client, trust);
    let meta = round_trip(req, rep, &format!("weida://{server_fp}@{local}/echo"));
    let mut events = ptr::null_mut();
    // SAFETY: live handles and a writable out-parameter; the meta and the
    // old replier are dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        ok(weida_requester_events(req, &mut events));
        weida_replier_drop(rep);
    }

    let mut op = ptr::null_mut();
    let mut handoff = ptr::null_mut();
    let mut socket = -1i64;
    let mut again = -1i64;
    let mut len = 0usize;
    // SAFETY: a live binding, consumed; then a live op and a live handoff,
    // each dropped once; the held bytes are copied before the handoff goes.
    let held = unsafe {
        ok(weida_binding_hand_over(binding, &mut op));
        ok(settle("the hand-over", |ms| {
            weida_binding_hand_over_op_wait(op, ms, &mut handoff)
        }));
        weida_binding_hand_over_op_drop(op);
        assert!(
            weida_handoff_held(handoff) >= 1,
            "the client's close is held"
        );
        assert_eq!(
            text(weida_handoff_local_addr(handoff, &mut len), len).as_deref(),
            Some(local.as_str())
        );
        ok(weida_handoff_take_socket(handoff, &mut socket));
        assert_eq!(
            weida_handoff_take_socket(handoff, &mut again),
            -WEIDA_ECLOSED
        );
        let data = weida_handoff_bytes(handoff, &mut len);
        let held = std::slice::from_raw_parts(data, len).to_vec();
        weida_handoff_drop(handoff);
        held
    };

    let new = runtime();
    let l2 = listener(new);
    let rep2 = replier(l2, "/echo");
    let mut bind = ptr::null_mut();
    let mut binding2 = ptr::null_mut();
    let mut take = ptr::null_mut();
    let mut sent = 0usize;
    // SAFETY: a live listener, the socket handed over to it, live terms and
    // writable out-parameters; each op is dropped once.
    unsafe {
        ok(weida_listener_bind_quic_socket(l2, socket, tls, &mut bind));
        ok(settle("the socket bind", |ms| {
            weida_listener_bind_quic_socket_op_wait(bind, ms, &mut binding2)
        }));
        weida_listener_bind_quic_socket_op_drop(bind);
        assert_eq!(
            text(weida_binding_local_addr(binding2, &mut len), len).as_deref(),
            Some(local.as_str())
        );
        ok(weida_binding_take_over(
            binding2,
            held.as_ptr(),
            held.len(),
            &mut take,
        ));
        ok(settle("the take-over", |ms| {
            weida_binding_take_over_op_wait(take, ms, &mut sent)
        }));
        weida_binding_take_over_op_drop(take);
    }
    assert!(sent >= 1, "sent {sent}");

    assert_eq!(lost_cause(events), WEIDA_LOSS_PEER_RESTARTING);
    until_connected(events);
    // The requester kept its address: no second `connect`.
    let meta = exchange(req, rep2);
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_peer_events_drop(events);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep2);
        weida_binding_drop(binding2);
        weida_listener_drop(l2);
        weida_server_tls_drop(tls);
        weida_identity_drop(server);
        weida_listener_drop(l);
    }
    shutdown(new);
    shutdown(client);
    // The predecessor's runtime lives to the end, as a predecessor process
    // would.
    shutdown(old);
}

#[test]
fn a_dial_only_replier_answers_through_a_relay() {
    // The relay R.
    let r_rt = runtime();
    let r_l = listener(r_rt);
    let (r_id, r_fp) = identity();
    let r_tls = server_tls(r_id);
    // A registration is addressed by its key, so the relay asks every peer
    // for one.
    // SAFETY: live server terms.
    ok(unsafe { weida_server_tls_require_any_key(r_tls) });
    let (r_binding, r_local) = bind_quic(r_l, r_tls);
    let mut relay = ptr::null_mut();
    let mut second = ptr::null_mut();
    let mut ev = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_listener_relay(r_l, &mut relay));
        assert_eq!(
            weida_listener_relay(r_l, &mut second),
            -WEIDA_EALREADY_REGISTERED
        );
        assert_eq!(
            weida_binding_relay_events(r_binding, &mut ev),
            0,
            "a binding that is not relayed has no relay events"
        );
    }
    let relay_url = format!("weida://{r_fp}@{r_local}/.weida/relay");

    // A registration the relay refuses.
    let h_rt = runtime();
    let h_l = listener(h_rt);
    let (stranger, stranger_fp) = identity();
    let stranger_tls = server_tls(stranger);
    let op = start_bind_relay(h_l, &relay_url, stranger, stranger_tls);
    let (q, peer, target) = relay_accept(relay);
    assert_eq!(peer, Some(stranger_fp));
    assert_eq!(target, None);
    refuse_relay(q);
    assert_eq!(finish_bind_relay(op), Err(-WEIDA_EREJECTED));

    // The hidden peer H.
    let (h_id, h_fp) = identity();
    let h_tls = server_tls(h_id);
    // SAFETY: live server terms.
    ok(unsafe { weida_server_tls_require_any_key(h_tls) });
    let op = start_bind_relay(h_l, &relay_url, h_id, h_tls);
    let (q, peer, target) = relay_accept(relay);
    assert_eq!(peer.as_deref(), Some(h_fp.as_str()));
    assert_eq!(target, None);
    admit(q);
    let h_binding = finish_bind_relay(op)
        .unwrap_or_else(|rc| panic!("the registration failed with {rc}: {}", last_message()));
    let h_rep = replier(h_l, "/echo");

    // The dialler D.
    let d_rt = runtime();
    let trust = trust_by_address();
    let (d_id, d_fp) = identity();
    let d_req = requester_with(d_rt, trust, Some(d_id));
    let url = c(&format!("weida+relay://{h_fp}@{r_fp}@{r_local}/echo"));
    let mut dial = ptr::null_mut();
    // SAFETY: a live requester, a C string and a writable out-parameter.
    ok(unsafe { weida_requester_connect(d_req, url.as_ptr(), &mut dial) });
    let (q, peer, target) = relay_accept(relay);
    assert_eq!(peer.as_deref(), Some(d_fp.as_str()));
    assert_eq!(target.as_deref(), Some(h_fp.as_str()));
    admit(q);
    // SAFETY: a live op, dropped once.
    unsafe {
        ok(settle("the relayed dial", |ms| {
            weida_requester_connect_op_wait(dial, ms)
        }));
        weida_requester_connect_op_drop(dial);
    }

    let meta = exchange(d_req, h_rep);
    assert_eq!(
        peer_fingerprint(meta),
        Some(d_fp),
        "the replier sees the dialler's key, not the relay's"
    );

    let mut event = ptr::null_mut();
    // SAFETY: a live binding, a live reader and writable out-parameters.
    unsafe {
        weida_incoming_meta_drop(meta);
        assert_eq!(weida_binding_relay_events(h_binding, &mut ev), 1);
        assert_eq!(
            weida_peer_events_recv(ev, 0, &mut event),
            -WEIDA_EAGAIN,
            "nothing happened since the reader was made"
        );
        weida_relay_drop(relay);
    }
    assert_eq!(lost_cause(ev), WEIDA_LOSS_PEER_CLOSED);

    let mut op = ptr::null_mut();
    // SAFETY: a live binding, consumed, and a writable out-parameter.
    assert_eq!(
        unsafe { weida_binding_hand_over(h_binding, &mut op) },
        -WEIDA_EUNSUPPORTED
    );
    assert!(op.is_null());
    // SAFETY: every handle is dropped once; `h_binding` was consumed.
    unsafe {
        weida_peer_events_drop(ev);
        weida_requester_drop(d_req);
        weida_identity_drop(d_id);
        weida_trust_drop(trust);
        weida_replier_drop(h_rep);
        weida_server_tls_drop(h_tls);
        weida_identity_drop(h_id);
        weida_server_tls_drop(stranger_tls);
        weida_identity_drop(stranger);
        weida_listener_drop(h_l);
        weida_binding_drop(r_binding);
        weida_server_tls_drop(r_tls);
        weida_identity_drop(r_id);
        weida_listener_drop(r_l);
    }
    shutdown(d_rt);
    shutdown(h_rt);
    shutdown(r_rt);
}

/// One number getter of the metadata: `Some` when present.
fn meta_u32(
    meta: *mut weida_incoming_meta,
    get: unsafe extern "C" fn(*const weida_incoming_meta, *mut u32) -> c_int,
) -> Option<u32> {
    let mut out = 0u32;
    // SAFETY: live metadata and a writable out-parameter.
    (unsafe { get(meta, &mut out) } == 1).then_some(out)
}

#[cfg(unix)]
#[test]
fn a_round_trip_over_unix_names_the_clients_uid() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = std::env::temp_dir().join(format!("weida-c-abi-unix-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the directory");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).expect("mode 0700");
    let socket = dir.join("s.sock");
    let socket_text = socket.to_str().expect("a UTF-8 path").to_owned();

    let rt = runtime();
    let l = listener(rt);
    let mut ub = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: a live listener, a C string and a writable out-parameter; the
    // path lives with `ub`.
    unsafe {
        ok(weida_listener_bind_unix(
            l,
            c(&socket_text).as_ptr(),
            &mut ub,
        ));
        assert_eq!(
            text(weida_unix_binding_path(ub, &mut len), len).as_deref(),
            Some(socket_text.as_str())
        );
    }
    let rep = replier(l, "/echo");
    let trust = trust_by_address();
    let req = requester(rt, trust);
    let url = weida_rs::UnixAddr {
        socket: socket_text.clone(),
        path: "/echo".to_owned(),
    }
    .to_string();
    let meta = round_trip(req, rep, &url);

    // The node's owner is this process, which is the client too.
    let owner = std::fs::metadata(&socket).expect("the socket").uid();
    assert_eq!(meta_u32(meta, weida_incoming_meta_peer_uid), Some(owner));
    // On macOS a new file takes its directory's group, so only presence.
    assert!(meta_u32(meta, weida_incoming_meta_peer_gid).is_some());
    if let Some(pid) = meta_u32(meta, weida_incoming_meta_peer_pid) {
        assert_eq!(pid, std::process::id());
    }
    assert_eq!(meta_text(meta, weida_incoming_meta_peer_sid), None);
    assert_eq!(peer_fingerprint(meta), None);

    let mut pb = ptr::null_mut();
    // SAFETY: a live listener, a C string and a writable out-parameter.
    assert_eq!(
        unsafe { weida_listener_bind_pipe(l, c("x").as_ptr(), &mut pb) },
        -WEIDA_EUNSUPPORTED
    );
    assert!(pb.is_null());

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_unix_binding_drop(ub);
    }
    assert!(!socket.exists(), "the drop removes the socket file");
    // SAFETY: dropped once.
    unsafe { weida_listener_drop(l) };
    shutdown(rt);
    std::fs::remove_dir_all(&dir).expect("remove the directory");
}

#[cfg(windows)]
#[test]
fn a_round_trip_over_a_pipe_names_the_clients_sid() {
    let name = format!("weida-c-abi-{}", std::process::id());
    let rt = runtime();
    let l = listener(rt);
    let mut pb = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: a live listener, a C string and a writable out-parameter; the
    // name lives with `pb`.
    unsafe {
        ok(weida_listener_bind_pipe(l, c(&name).as_ptr(), &mut pb));
        assert_eq!(
            text(weida_pipe_binding_name(pb, &mut len), len).as_deref(),
            Some(name.as_str())
        );
    }
    let rep = replier(l, "/echo");
    let trust = trust_by_address();
    let req = requester(rt, trust);
    let meta = round_trip(req, rep, &format!("weida+pipe://{name}/echo"));

    let sid = meta_text(meta, weida_incoming_meta_peer_sid).expect("the SID");
    assert!(sid.starts_with("S-1-"), "{sid}");
    assert_eq!(
        meta_u32(meta, weida_incoming_meta_peer_pid),
        Some(std::process::id())
    );
    assert_eq!(meta_u32(meta, weida_incoming_meta_peer_uid), None);
    assert_eq!(peer_fingerprint(meta), None);

    let mut ub = ptr::null_mut();
    // SAFETY: a live listener, a C string and a writable out-parameter.
    assert_eq!(
        unsafe { weida_listener_bind_unix(l, c("x").as_ptr(), &mut ub) },
        -WEIDA_EUNSUPPORTED
    );
    assert!(ub.is_null());

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_pipe_binding_drop(pb);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

/// The next identity event of `events`, waiting for it; never the end.
fn next_identity_event(events: *mut weida_identity_events) -> *mut weida_identity_event {
    let mut event = ptr::null_mut();
    // SAFETY: a live reader and a writable out-parameter.
    ok(settle("an identity event", |ms| unsafe {
        weida_identity_events_recv(events, ms, &mut event)
    }));
    assert!(!event.is_null(), "the event stream ended");
    event
}

/// The current fingerprint of an identity source.
fn source_fingerprint(src: *mut weida_identity_source) -> String {
    let mut buf = [0 as c_char; WEIDA_FINGERPRINT_LEN as usize + 1];
    // SAFETY: a live source and `buf.len()` writable bytes; on success
    // `buf` holds a NUL-terminated string.
    unsafe {
        ok(weida_identity_source_fingerprint(
            src,
            buf.as_mut_ptr(),
            buf.len(),
        ));
        CStr::from_ptr(buf.as_ptr())
            .to_str()
            .expect("UTF-8")
            .to_owned()
    }
}

#[test]
fn a_files_identity_source_rotates_a_bindings_certificate() {
    let dir = std::env::temp_dir().join(format!("weida-c-abi-files-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let rt = runtime();
    let l = listener(rt);
    let mut opts = ptr::null_mut();
    let mut src = ptr::null_mut();
    let mut events = ptr::null_mut();
    let mut tls = ptr::null_mut();
    let names = [c("127.0.0.1")];
    let ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
    let path = c(dir.to_str().expect("a UTF-8 path"));
    // SAFETY: live handles, C strings and writable out-parameters; the
    // options are dropped once.
    unsafe {
        ok(weida_files_options_new(&mut opts));
        ok(weida_files_options_set_names(
            opts,
            ptrs.as_ptr(),
            ptrs.len(),
        ));
        ok(weida_files_options_set_poll_ms(opts, 0));
        ok(weida_identity_source_files(path.as_ptr(), opts, &mut src));
        weida_files_options_drop(opts);
    }
    assert!(dir.join("cert.pem").exists() && dir.join("key.pem").exists());
    let old = source_fingerprint(src);
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_identity_source_events(src, &mut events));
        ok(weida_server_tls_new_source(src, &mut tls));
    }
    let (binding, local) = bind_quic(l, tls);
    // SAFETY: dropped once; the binding holds its copy of the terms.
    unsafe { weida_server_tls_drop(tls) };
    let rep = replier(l, "/echo");
    let url = format!("weida://{local}/echo");

    let old_trust = trust_pinning(&old);
    let first = requester(rt, old_trust);
    let meta = round_trip(first, rep, &url);
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };

    // An agent rewrites the files: the certificate first, the key second.
    let (fresh, new) = identity();
    std::fs::write(dir.join("cert.pem"), certificate_pem(fresh)).expect("cert.pem");
    std::fs::write(dir.join("key.pem"), to_pem(fresh)).expect("key.pem");

    let new_trust = trust_pinning(&new);
    let second = requester(rt, new_trust);
    let meta_second = round_trip(second, rep, &url);
    // The connection made before the rotation still carries requests.
    let meta_first = exchange(first, rep);

    let event = next_identity_event(events);
    let mut len = 0usize;
    // SAFETY: a live event and a writable length; the texts live with it.
    unsafe {
        assert_eq!(
            weida_identity_event_kind(event),
            WEIDA_IDENTITY_EVENT_KEY_CHANGED
        );
        assert_eq!(
            text(weida_identity_event_from(event, &mut len), len),
            Some(old)
        );
        assert_eq!(
            text(weida_identity_event_to(event, &mut len), len),
            Some(new.clone())
        );
    }
    assert_eq!(source_fingerprint(src), new);
    // SAFETY: a live source; the handshake already loaded the change.
    assert_eq!(unsafe { weida_identity_source_reload(src) }, 0);

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_identity_event_drop(event);
        weida_identity_events_drop(events);
        weida_incoming_meta_drop(meta_first);
        weida_incoming_meta_drop(meta_second);
        weida_requester_drop(second);
        weida_requester_drop(first);
        weida_trust_drop(new_trust);
        weida_trust_drop(old_trust);
        weida_replier_drop(rep);
        weida_binding_drop(binding);
        weida_identity_drop(fresh);
        weida_identity_source_drop(src);
        weida_listener_drop(l);
    }
    shutdown(rt);
    std::fs::remove_dir_all(&dir).expect("the directory");
}

#[test]
fn a_mismatched_alpn_fails_the_dial_with_etls() {
    let rt = runtime();
    let l = listener(rt);
    let (server, server_fp) = identity();
    let (client, client_fp) = identity();
    let client_pin = trust_pinning(&client_fp);
    let server_pin = trust_pinning(&server_fp);
    let mut client_trust = ptr::null_mut();
    let mut server_trust = ptr::null_mut();
    let mut client_id = ptr::null_mut();
    let tls = server_tls(server);
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_trust_source_from_trust(client_pin, &mut client_trust));
        ok(weida_trust_source_from_trust(server_pin, &mut server_trust));
        ok(weida_identity_source_from_identity(client, &mut client_id));
        ok(weida_server_tls_set_alpn(tls, WEIDA_ALPN_RAFT));
        ok(weida_server_tls_require_client_source(tls, client_trust));
    }
    let (binding, local) = bind_quic(l, tls);
    let rep = replier(l, "/echo");
    let url = format!("weida://{local}/echo");

    let mut plain_tls = ptr::null_mut();
    let mut plain = ptr::null_mut();
    let mut raft_tls = ptr::null_mut();
    let mut raft = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters.
    unsafe {
        ok(weida_client_tls_new(server_pin, &mut plain_tls));
        ok(weida_client_tls_set_identity_source(plain_tls, client_id));
        ok(weida_runtime_requester(rt, plain_tls, &mut plain));
        ok(weida_client_tls_new_source(server_trust, &mut raft_tls));
        ok(weida_client_tls_set_identity_source(raft_tls, client_id));
        ok(weida_client_tls_set_alpn(raft_tls, WEIDA_ALPN_RAFT));
        ok(weida_runtime_requester(rt, raft_tls, &mut raft));
    }
    let rc = exchange_status(plain, &url);
    assert_eq!(
        rc,
        -WEIDA_ETLS,
        "weida/0 against weida-raft/0: {}",
        last_message()
    );
    let meta = round_trip(raft, rep, &url);
    assert_eq!(peer_fingerprint(meta), Some(client_fp));

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_drop(raft);
        weida_requester_drop(plain);
        weida_client_tls_drop(raft_tls);
        weida_client_tls_drop(plain_tls);
        weida_replier_drop(rep);
        weida_binding_drop(binding);
        weida_server_tls_drop(tls);
        weida_identity_source_drop(client_id);
        weida_trust_source_drop(server_trust);
        weida_trust_source_drop(client_trust);
        weida_trust_drop(server_pin);
        weida_trust_drop(client_pin);
        weida_identity_drop(client);
        weida_identity_drop(server);
        weida_listener_drop(l);
    }
    shutdown(rt);
}

/// A runtime whose dials resolve through a new resolver's queue, and that
/// resolver.
fn resolving_runtime() -> (*mut weida_runtime, *mut weida_resolver) {
    let mut r = ptr::null_mut();
    let mut cfg = ptr::null_mut();
    // SAFETY: writable out-parameters and live handles; `cfg` is dropped once.
    unsafe {
        ok(weida_resolver_new(&mut r));
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_runtime_config_set_resolver(cfg, r));
    }
    let rt = runtime_with(cfg);
    // SAFETY: dropped once; the runtime holds the queue.
    unsafe { weida_runtime_config_drop(cfg) };
    (rt, r)
}

/// The next lookup of `r`, waiting for it; never the end.
fn next_lookup(r: *mut weida_resolver) -> *mut weida_resolved {
    let mut q = ptr::null_mut();
    // SAFETY: a live resolver and a writable out-parameter.
    ok(settle("a lookup", |ms| unsafe {
        weida_resolver_recv(r, ms, &mut q)
    }));
    assert!(!q.is_null(), "the resolver's queue ended");
    q
}

/// Starts dialling `url`: the connect op, not waited for.
fn start_connect(r: *mut weida_requester, url: &str) -> *mut weida_requester_connect_op {
    let url = c(url);
    let mut op = ptr::null_mut();
    // SAFETY: a live requester, a C string and a writable out-parameter.
    ok(unsafe { weida_requester_connect(r, url.as_ptr(), &mut op) });
    op
}

/// The host and port a lookup asks for.
fn lookup_of(q: *mut weida_resolved) -> (Option<String>, Option<u16>) {
    let mut len = 0usize;
    let mut port = 0u16;
    // SAFETY: a live lookup and writable out-parameters.
    unsafe {
        let name = text(weida_resolved_name(q, &mut len), len);
        let port = (weida_resolved_port(q, &mut port) == 1).then_some(port);
        (name, port)
    }
}

#[test]
fn a_name_only_the_resolver_knows_is_dialled_through_it() {
    let server = runtime();
    let l = listener(server);
    let (id, server_fp) = identity_for(&["svc.weida.test"]);
    let tls = server_tls(id);
    let (binding, local) = bind_quic(l, tls);
    let rep = replier(l, "/echo");
    let port = local.rsplit(':').next().expect("a port");

    let (client, r) = resolving_runtime();
    let trust = trust_pinning(&server_fp);
    let req = requester(client, trust);
    let op = start_connect(req, "weida://svc.weida.test/echo");
    let q = next_lookup(r);
    assert_eq!(lookup_of(q), (Some("svc.weida.test".to_owned()), None));
    // SAFETY: a live lookup.
    assert_eq!(unsafe { weida_resolved_max_addresses(q) }, 8);
    let addr = c(&format!("127.0.0.1:{port}"));
    let addrs = [addr.as_ptr()];
    // SAFETY: a live lookup, consumed, and one readable pointer.
    ok(unsafe { weida_resolved_answer(q, addrs.as_ptr(), addrs.len()) });
    // SAFETY: a live op.
    ok(settle("the dial", |ms| unsafe {
        weida_requester_connect_op_wait(op, ms)
    }));
    let meta = exchange(req, rep);

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_requester_connect_op_drop(op);
        weida_requester_drop(req);
        weida_trust_drop(trust);
        weida_replier_drop(rep);
        weida_binding_drop(binding);
        weida_server_tls_drop(tls);
        weida_identity_drop(id);
        weida_listener_drop(l);
    }
    shutdown(client);
    // SAFETY: dropped once.
    unsafe { weida_resolver_drop(r) };
    shutdown(server);
}

#[test]
fn a_refused_lookup_fails_the_connect_with_einvalid_address() {
    let (client, r) = resolving_runtime();
    // A pin, since an empty trust refuses the dial before it resolves.
    let trust = trust_pinning(&format!("sha256:{}", "0".repeat(64)));
    let req = requester(client, trust);
    let op = start_connect(req, "weida://nowhere.weida.test:7443/echo");
    let q = next_lookup(r);
    assert_eq!(
        lookup_of(q),
        (Some("nowhere.weida.test".to_owned()), Some(7443))
    );
    let reason = c("no such service");
    // SAFETY: a live lookup, consumed, and a C string.
    ok(unsafe { weida_resolved_refuse(q, reason.as_ptr()) });
    // SAFETY: a live op.
    let rc = settle("the dial", |ms| unsafe {
        weida_requester_connect_op_wait(op, ms)
    });
    assert_eq!(rc, -WEIDA_EINVALID_ADDRESS, "{}", last_message());
    assert!(
        last_message().contains("no such service"),
        "{}",
        last_message()
    );

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_requester_connect_op_drop(op);
        weida_requester_drop(req);
        weida_trust_drop(trust);
    }
    shutdown(client);
    // SAFETY: dropped once.
    unsafe { weida_resolver_drop(r) };
}

// --- patterns: Push/Pull and Pub/Sub ----------------------------------------

type LimitSetter = unsafe extern "C" fn(*const weida_limits, u64) -> c_int;

/// A configuration whose guarantee set asks for `ordering` and whose limits
/// have each setter applied. The caller drops it.
fn pattern_config(ordering: c_int, limits: &[(LimitSetter, u64)]) -> *mut weida_runtime_config {
    let mut cfg = ptr::null_mut();
    let mut g = ptr::null_mut();
    let mut l = ptr::null_mut();
    // SAFETY: writable out-parameters and live handles; `g` and `l` are
    // dropped once.
    unsafe {
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_guarantee_set_new(&mut g));
        ok(weida_guarantee_set_set_ordering(g, ordering));
        ok(weida_runtime_config_set_guarantees(cfg, g));
        weida_guarantee_set_drop(g);
        ok(weida_limits_new(&mut l));
        for (set, value) in limits {
            ok(set(l, *value));
        }
        ok(weida_runtime_config_set_limits(cfg, l));
        weida_limits_drop(l);
    }
    cfg
}

/// A new listener of `rt` bound to the inproc bus `bus`.
fn bound(rt: *mut weida_runtime, bus: &str) -> (*mut weida_listener, *mut weida_local_binding) {
    let l = listener(rt);
    let mut lb = ptr::null_mut();
    let name = c(bus);
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_bind_inproc(l, name.as_ptr(), &mut lb) });
    (l, lb)
}

/// Client terms that trust each address; the caller drops them.
fn client_tls() -> *mut weida_client_tls {
    let trust = trust_by_address();
    let mut tls = ptr::null_mut();
    // SAFETY: a live trust, dropped once, and a writable out-parameter.
    unsafe {
        ok(weida_client_tls_new(trust, &mut tls));
        weida_trust_drop(trust);
    }
    tls
}

fn pusher(rt: *mut weida_runtime) -> *mut weida_pusher {
    let tls = client_tls();
    let mut p = ptr::null_mut();
    // SAFETY: live handles, `tls` dropped once, and a writable out-parameter.
    unsafe {
        ok(weida_runtime_pusher(rt, tls, &mut p));
        weida_client_tls_drop(tls);
    }
    p
}

fn subscriber(rt: *mut weida_runtime) -> *mut weida_subscriber {
    let tls = client_tls();
    let mut s = ptr::null_mut();
    // SAFETY: live handles, `tls` dropped once, and a writable out-parameter.
    unsafe {
        ok(weida_runtime_subscriber(rt, tls, &mut s));
        weida_client_tls_drop(tls);
    }
    s
}

/// Starts an op with `start`, settles its wait and drops it: the status the
/// wait ended with.
fn run_op<O>(
    what: &str,
    start: impl FnOnce(*mut *mut O) -> c_int,
    wait: unsafe extern "C" fn(*const O, i64) -> c_int,
    drop: unsafe extern "C" fn(*mut O),
) -> c_int {
    let mut op = ptr::null_mut();
    ok(start(&mut op));
    // SAFETY: a live op.
    let rc = settle(what, |ms| unsafe { wait(op, ms) });
    // SAFETY: the op is dropped once.
    unsafe { drop(op) };
    rc
}

fn pusher_connect(p: *mut weida_pusher, url: &str) -> c_int {
    let url = c(url);
    run_op(
        "the dial",
        // SAFETY: a live pusher, a C string and a writable out-parameter.
        |op| unsafe { weida_pusher_connect(p, url.as_ptr(), op) },
        weida_pusher_connect_op_wait,
        weida_pusher_connect_op_drop,
    )
}

fn subscriber_connect(s: *mut weida_subscriber, url: &str) -> c_int {
    let url = c(url);
    run_op(
        "the dial",
        // SAFETY: a live subscriber, a C string and a writable out-parameter.
        |op| unsafe { weida_subscriber_connect(s, url.as_ptr(), op) },
        weida_subscriber_connect_op_wait,
        weida_subscriber_connect_op_drop,
    )
}

fn subscribe(s: *mut weida_subscriber, filter: &str) -> c_int {
    let filter = c(filter);
    run_op(
        "the subscription",
        // SAFETY: a live subscriber, a C string and a writable out-parameter.
        |op| unsafe { weida_subscriber_subscribe(s, filter.as_ptr(), op) },
        weida_subscriber_subscribe_op_wait,
        weida_subscriber_subscribe_op_drop,
    )
}

fn send(p: *mut weida_pusher, body: &[u8]) {
    ok(run_op(
        "the send",
        // SAFETY: a live pusher, `body.len()` readable bytes and a writable
        // out-parameter.
        |op| unsafe { weida_pusher_send(p, body.as_ptr(), body.len(), op) },
        weida_pusher_send_op_wait,
        weida_pusher_send_op_drop,
    ));
}

fn pull(p: *mut weida_puller) -> *mut weida_incoming_transfer {
    let mut t = ptr::null_mut();
    // SAFETY: a live puller and a writable out-parameter.
    ok(settle("a pushed transfer", |ms| unsafe {
        weida_puller_recv(p, ms, &mut t)
    }));
    assert!(!t.is_null());
    t
}

fn next_copy(s: *mut weida_subscriber) -> *mut weida_incoming_transfer {
    let mut t = ptr::null_mut();
    // SAFETY: a live subscriber and a writable out-parameter.
    ok(settle("a published copy", |ms| unsafe {
        weida_subscriber_recv(s, ms, &mut t)
    }));
    assert!(!t.is_null());
    t
}

/// Publishes `data` on `topic`: how many subscribers it was enqueued to.
fn publish(p: *mut weida_publisher, topic: &str, data: &[u8]) -> usize {
    let topic = c(topic);
    let mut n = usize::MAX;
    // SAFETY: a live publisher, a C string, `data.len()` readable bytes and
    // a writable out-parameter.
    ok(unsafe { weida_publisher_publish(p, topic.as_ptr(), data.as_ptr(), data.len(), &mut n) });
    n
}

/// The whole body of `t`, consuming it.
fn body(t: *mut weida_incoming_transfer) -> Vec<u8> {
    let msg = collect(t, 1 << 20).expect("the body");
    // SAFETY: a live message, dropped once.
    unsafe {
        let data = std::slice::from_raw_parts(weida_msg_data(msg), weida_msg_len(msg)).to_vec();
        weida_msg_drop(msg);
        data
    }
}

/// The metadata of `t`; the caller drops it.
fn meta_of(t: *mut weida_incoming_transfer) -> *mut weida_incoming_meta {
    let mut m = ptr::null_mut();
    // SAFETY: a live transfer and a writable out-parameter.
    ok(unsafe { weida_incoming_transfer_meta(t, &mut m) });
    m
}

fn gap(meta: *mut weida_incoming_meta) -> Option<(u64, u64)> {
    let (mut expected, mut seen) = (0u64, 0u64);
    // SAFETY: live metadata and writable out-parameters.
    (unsafe { weida_incoming_meta_gap(meta, &mut expected, &mut seen) } == 1)
        .then_some((expected, seen))
}

fn sequence(meta: *mut weida_incoming_meta) -> Option<u64> {
    let mut n = 0u64;
    // SAFETY: live metadata and a writable out-parameter.
    (unsafe { weida_incoming_meta_sequence(meta, &mut n) } == 1).then_some(n)
}

/// Waits until `p` holds `n` filters: the subscriptions arrived.
fn await_filters(p: *mut weida_publisher, n: usize) {
    weida_testkit::poll_until("the subscriptions", WAIT, || {
        // SAFETY: a live publisher.
        unsafe { weida_publisher_filter_count(p) == n }
    });
}

/// One count of a drops snapshot: `Some` when present.
fn drop_count(
    d: *mut weida_topic_drops,
    index: usize,
    get: unsafe extern "C" fn(*const weida_topic_drops, usize, *mut u64) -> c_int,
) -> Option<u64> {
    let mut n = 0u64;
    // SAFETY: a live snapshot and a writable out-parameter.
    (unsafe { get(d, index, &mut n) } == 1).then_some(n)
}

#[test]
fn push_to_pull_over_inproc_arrives_in_order() {
    let cfg = pattern_config(WEIDA_ORDERING_MODE_PER_PRODUCER_REASSEMBLE, &[]);
    let server = runtime_with(cfg);
    let client = runtime_with(cfg);
    let (l, lb) = bound(server, "abi-patterns-push");
    let mut pull_end = ptr::null_mut();
    let path = c("/jobs");
    let mut len = 0usize;
    // SAFETY: a live listener, a C string and writable out-parameters.
    unsafe {
        ok(weida_listener_puller(l, path.as_ptr(), &mut pull_end));
        let at = weida_puller_path(pull_end, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("/jobs"));
    }
    let push = pusher(client);
    let mut events = ptr::null_mut();
    // SAFETY: a live pusher and a writable out-parameter.
    ok(unsafe { weida_pusher_events(push, &mut events) });
    let url = "weida+inproc://abi-patterns-push/jobs";
    ok(pusher_connect(push, url));
    until_connected(events);
    // SAFETY: a live pusher; the reader is dropped once.
    unsafe {
        assert_eq!(weida_pusher_peer_count(push), 1);
        weida_peer_events_drop(events);
    }

    send(push, b"one");
    send(push, b"two");
    let mut meta = ptr::null_mut();
    let json = c("application/json");
    // SAFETY: live handles, a C string and writable out-parameters; `meta`
    // is copied by the send and dropped once.
    unsafe {
        ok(weida_transfer_meta_new(&mut meta));
        ok(weida_transfer_meta_set_content_type(meta, json.as_ptr()));
    }
    ok(run_op(
        "the send",
        // SAFETY: live handles, 5 readable bytes and a writable out-parameter.
        |op| unsafe { weida_pusher_send_with(push, meta, b"three".as_ptr(), 5, op) },
        weida_pusher_send_op_wait,
        weida_pusher_send_op_drop,
    ));
    // SAFETY: dropped once.
    unsafe { weida_transfer_meta_drop(meta) };
    for (n, (expected, labelled)) in [
        (&b"one"[..], None),
        (b"two", None),
        (b"three", Some("application/json")),
    ]
    .into_iter()
    .enumerate()
    {
        let t = pull(pull_end);
        let meta = meta_of(t);
        assert_eq!(sequence(meta), Some(n as u64), "arrival order");
        assert_eq!(gap(meta), None);
        assert_eq!(content_type(meta).as_deref(), labelled);
        // SAFETY: dropped once.
        unsafe { weida_incoming_meta_drop(meta) };
        assert_eq!(body(t), expected);
    }

    let mut meta = ptr::null_mut();
    let mut out = ptr::null_mut();
    let mut delivery = ptr::null_mut();
    let content_type_text = c("text/plain");
    // SAFETY: live handles, each dropped or consumed once, and writable
    // out-parameters.
    unsafe {
        ok(weida_transfer_meta_new(&mut meta));
        ok(weida_transfer_meta_set_content_type(
            meta,
            content_type_text.as_ptr(),
        ));
        let mut op = ptr::null_mut();
        ok(weida_pusher_open(push, meta, &mut op));
        weida_transfer_meta_drop(meta);
        ok(settle("the open", |ms| {
            weida_pusher_open_op_wait(op, ms, &mut out)
        }));
        weida_pusher_open_op_drop(op);
    }
    write_all(out, b"four");
    // SAFETY: a live transfer, consumed, and a writable out-parameter.
    ok(unsafe { weida_outgoing_transfer_finish(out, &mut delivery) });
    let fourth = pull(pull_end);
    let meta = meta_of(fourth);
    assert_eq!(content_type(meta).as_deref(), Some("text/plain"));
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };
    assert_eq!(body(fourth), b"four");
    ok(run_op(
        "the delivery",
        // SAFETY: a live receipt, consumed, and a writable out-parameter.
        |op| unsafe { weida_delivery_delivered(delivery, op) },
        weida_delivery_delivered_op_wait,
        weida_delivery_delivered_op_drop,
    ));

    let mut stats = ptr::null_mut();
    let url_c = c(url);
    // SAFETY: live handles and writable out-parameters; `stats` is dropped
    // once.
    unsafe {
        ok(weida_pusher_connection_stats(push, &mut stats));
        assert_eq!(weida_connection_stats_len(stats), 1);
        let at = weida_connection_stats_url(stats, 0, &mut len);
        assert_eq!(text(at, len).as_deref(), Some(url));
        weida_connection_stats_drop(stats);
        assert_eq!(weida_pusher_dropped(push), 0);
        assert_eq!(weida_pusher_disconnect(push, url_c.as_ptr()), 1);
        assert_eq!(weida_pusher_disconnect(push, url_c.as_ptr()), 0);
        assert_eq!(weida_pusher_peer_count(push), 0);

        weida_pusher_drop(push);
        weida_puller_drop(pull_end);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
        weida_runtime_config_drop(cfg);
    }
    shutdown(client);
    shutdown(server);
}

#[test]
fn a_subscriber_receives_only_its_topic() {
    let server = runtime();
    let (l, lb) = bound(server, "abi-patterns-topics");
    let mut p = ptr::null_mut();
    let path = c("/md");
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_publisher(l, path.as_ptr(), &mut p) });
    let url = "weida+inproc://abi-patterns-topics/md";
    // One runtime per subscriber: two on one runtime share the pooled
    // connection and collide on the path.
    let (c1, c2) = (runtime(), runtime());
    let (s1, s2) = (subscriber(c1), subscriber(c2));
    let mut events = ptr::null_mut();
    // SAFETY: a live subscriber and a writable out-parameter.
    ok(unsafe { weida_subscriber_events(s1, &mut events) });
    ok(subscriber_connect(s1, url));
    ok(subscriber_connect(s2, url));
    until_connected(events);
    // SAFETY: the reader is dropped once.
    unsafe { weida_peer_events_drop(events) };
    ok(subscribe(s1, "px.#"));
    ok(subscribe(s2, "fx.#"));
    assert_eq!(
        subscribe(s1, "#.px"),
        -WEIDA_EPROTOCOL,
        "{}",
        last_message()
    );
    await_filters(p, 2);
    // SAFETY: a live publisher.
    assert_eq!(unsafe { weida_publisher_subscriber_count(p) }, 2);

    assert_eq!(publish(p, "px.eur", b"price"), 1);
    let trace = new_trace();
    let topic = c("fx.usd");
    let mut n = 0usize;
    // SAFETY: a live publisher, C strings, 4 readable bytes and a writable
    // out-parameter.
    ok(unsafe {
        weida_publisher_publish_with_trace(
            p,
            topic.as_ptr(),
            b"rate".as_ptr(),
            4,
            trace.as_ptr(),
            &mut n,
        )
    });
    assert_eq!(n, 1);

    let t1 = next_copy(s1);
    let m1 = meta_of(t1);
    assert_eq!(
        meta_text(m1, weida_incoming_meta_topic).as_deref(),
        Some("px.eur")
    );
    assert_eq!(body(t1), b"price");
    let t2 = next_copy(s2);
    let m2 = meta_of(t2);
    assert_eq!(
        meta_text(m2, weida_incoming_meta_topic).as_deref(),
        Some("fx.usd")
    );
    assert_eq!(
        meta_text(m2, weida_incoming_meta_trace).as_deref(),
        trace.to_str().ok()
    );
    assert_eq!(body(t2), b"rate");
    for s in [s1, s2] {
        let mut t = ptr::null_mut();
        // SAFETY: a live subscriber and a writable out-parameter.
        let rc = unsafe { weida_subscriber_recv(s, 200, &mut t) };
        assert_eq!(rc, -WEIDA_EAGAIN, "only the subscribed topic arrives");
    }

    let filter = c("px.#");
    ok(run_op(
        "the grant",
        // SAFETY: a live subscriber, a C string and a writable out-parameter.
        |op| unsafe { weida_subscriber_grant(s1, filter.as_ptr(), 10, op) },
        weida_subscriber_grant_op_wait,
        weida_subscriber_grant_op_drop,
    ));
    ok(run_op(
        "the withdrawal",
        // SAFETY: a live subscriber, a C string and a writable out-parameter.
        |op| unsafe { weida_subscriber_unsubscribe(s1, filter.as_ptr(), op) },
        weida_subscriber_unsubscribe_op_wait,
        weida_subscriber_unsubscribe_op_drop,
    ));
    // SAFETY: a live subscriber.
    assert_eq!(unsafe { weida_subscriber_filter_count(s1) }, 0);
    await_filters(p, 1);
    assert_eq!(publish(p, "px.eur", b"unheard"), 0);

    let mut stats = ptr::null_mut();
    let mut len = 0usize;
    let url_c = c(url);
    // SAFETY: live handles and writable out-parameters; `stats` is dropped
    // once.
    unsafe {
        assert_eq!(weida_subscriber_peer_count(s2), 1);
        ok(weida_subscriber_connection_stats(s2, &mut stats));
        assert_eq!(weida_connection_stats_len(stats), 1);
        let at = weida_connection_stats_url(stats, 0, &mut len);
        assert_eq!(text(at, len).as_deref(), Some(url));
        weida_connection_stats_drop(stats);
        assert_eq!(weida_subscriber_disconnect(s2, url_c.as_ptr()), 1);
        assert_eq!(weida_subscriber_disconnect(s2, url_c.as_ptr()), 0);
        assert_eq!(weida_subscriber_peer_count(s2), 0);
    }
    await_filters(p, 0);

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(m1);
        weida_incoming_meta_drop(m2);
        weida_subscriber_drop(s1);
        weida_subscriber_drop(s2);
        weida_publisher_drop(p);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(c1);
    shutdown(c2);
    shutdown(server);
}

#[test]
fn a_skipped_sequence_reads_as_a_gap_under_detect() {
    const DETECT: c_int = WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT;
    const MSG: usize = 32 * 1024;
    // A budget of two copies on the publisher, and a stream window on the
    // subscriber that the first copy overflows: its writer stalls, the
    // second copy fills the budget and the third is dropped.
    let server_cfg = pattern_config(
        DETECT,
        &[(weida_limits_set_subscriber_buffer_bytes, 64 * 1024)],
    );
    let client_cfg = pattern_config(DETECT, &[(weida_limits_set_stream_receive_window, 8192)]);
    let server = runtime_with(server_cfg);
    let client = runtime_with(client_cfg);
    let (l, lb) = bound(server, "abi-patterns-gap");
    let mut p = ptr::null_mut();
    let path = c("/md");
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_publisher(l, path.as_ptr(), &mut p) });
    let s = subscriber(client);
    ok(subscriber_connect(s, "weida+inproc://abi-patterns-gap/md"));
    ok(subscribe(s, "px.#"));
    await_filters(p, 1);

    let payload = vec![0u8; MSG];
    let mut published = 0u64;
    weida_testkit::poll_until("a dropped copy", WAIT, || {
        publish(p, "px.eur", &payload);
        published += 1;
        // SAFETY: a live publisher.
        unsafe { weida_publisher_dropped(p) > 0 }
    });
    // SAFETY: a live publisher.
    let lost = unsafe { weida_publisher_dropped(p) };
    assert!(published > lost, "the first copy was deliverable");

    let mut d = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: a live publisher and a writable out-parameter.
    ok(unsafe { weida_publisher_drops(p, &mut d) });
    // SAFETY: a live snapshot and a writable length.
    unsafe {
        assert_eq!(weida_topic_drops_len(d), 1);
        let at = weida_topic_drops_topic(d, 0, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("px.eur"));
        assert!(weida_topic_drops_topic(d, 1, &mut len).is_null());
    }
    assert_eq!(
        drop_count(d, 0, weida_topic_drops_subscriber_budget),
        Some(lost)
    );
    for other in [
        weida_topic_drops_subscriber_queue,
        weida_topic_drops_no_parked_connection,
        weida_topic_drops_superseded,
        weida_topic_drops_expired,
        weida_topic_drops_too_large,
        weida_topic_drops_no_datagrams,
        weida_topic_drops_layers_cut,
    ] {
        assert_eq!(drop_count(d, 0, other), Some(0));
    }
    assert_eq!(drop_count(d, 1, weida_topic_drops_subscriber_budget), None);
    // SAFETY: dropped once.
    unsafe { weida_topic_drops_drop(d) };
    for (topic, records) in [("px.eur", 1), ("fx.none", 0)] {
        let topic = c(topic);
        let mut d = ptr::null_mut();
        // SAFETY: a live publisher, a C string and a writable
        // out-parameter; the snapshot is dropped once.
        unsafe {
            ok(weida_publisher_dropped_on(p, topic.as_ptr(), &mut d));
            assert_eq!(weida_topic_drops_len(d), records);
            weida_topic_drops_drop(d);
        }
    }

    // What arrived carries no gap yet: the lost copies were the last ones.
    for _ in 0..published - lost {
        let t = next_copy(s);
        let meta = meta_of(t);
        assert!(sequence(meta).is_some(), "detect numbers every copy");
        assert_eq!(gap(meta), None);
        // SAFETY: dropped once.
        unsafe { weida_incoming_meta_drop(meta) };
        assert_eq!(body(t).len(), MSG);
    }

    // The budget is free again: the next copy arrives and names the hole.
    let mut t = ptr::null_mut();
    weida_testkit::poll_until("a copy after the hole", WAIT, || {
        publish(p, "px.eur", &payload);
        // SAFETY: a live subscriber and a writable out-parameter.
        let rc = unsafe { weida_subscriber_recv(s, 100, &mut t) };
        assert!(rc == 0 || rc == -WEIDA_EAGAIN, "recv failed with {rc}");
        rc == 0
    });
    let meta = meta_of(t);
    let (expected, seen) = gap(meta).expect("the copy after the hole carries the gap");
    // SAFETY: a live publisher.
    let dropped = unsafe { weida_publisher_dropped(p) };
    assert!(
        (lost..=dropped).contains(&(seen - expected)),
        "missed {} of {lost}..={dropped}",
        seen - expected
    );

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_incoming_meta_drop(meta);
        weida_incoming_transfer_drop(t);
        weida_subscriber_drop(s);
        weida_publisher_drop(p);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
        weida_runtime_config_drop(server_cfg);
        weida_runtime_config_drop(client_cfg);
    }
    shutdown(client);
    shutdown(server);
}

/// Writes `chunk` to `f` without waiting: the subscribers left.
fn write_now(f: *mut weida_fan_out, chunk: &[u8]) -> usize {
    let mut n = usize::MAX;
    // SAFETY: a live fan-out, `chunk.len()` readable bytes and a writable
    // out-parameter.
    ok(unsafe { weida_fan_out_write_now(f, chunk.as_ptr(), chunk.len(), &mut n) });
    n
}

/// Finishes `f`, consuming it: the subscribers that received all of it.
fn finish(f: *mut weida_fan_out) -> usize {
    let mut n = usize::MAX;
    // SAFETY: a live fan-out, consumed, and a writable out-parameter.
    ok(unsafe { weida_fan_out_finish(f, &mut n) });
    n
}

#[test]
fn a_fan_out_streams_one_payload_to_every_subscriber() {
    let server = runtime();
    let (l, lb) = bound(server, "abi-patterns-fan-out");
    let mut p = ptr::null_mut();
    let path = c("/md");
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_publisher(l, path.as_ptr(), &mut p) });
    let client = runtime();
    let s = subscriber(client);
    ok(subscriber_connect(
        s,
        "weida+inproc://abi-patterns-fan-out/md",
    ));
    ok(subscribe(s, "px.#"));
    await_filters(p, 1);

    let topic = c("px.frame");
    let mut f = ptr::null_mut();
    let mut len = 0usize;
    let mut left = usize::MAX;
    // SAFETY: live handles, C strings and writable out-parameters.
    unsafe {
        ok(weida_publisher_open(p, topic.as_ptr(), &mut f));
        let at = weida_fan_out_topic(f, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("px.frame"));
        ok(weida_fan_out_subscribers(f, &mut left));
    }
    assert_eq!(left, 1);
    let mut op = ptr::null_mut();
    // SAFETY: a live fan-out, 3 readable bytes and a writable out-parameter;
    // the op is dropped once.
    unsafe {
        ok(weida_fan_out_write_within(
            f,
            b"abc".as_ptr(),
            3,
            1000,
            &mut op,
        ));
        ok(settle("the bounded write", |ms| {
            weida_fan_out_write_within_op_wait(op, ms, &mut left)
        }));
        weida_fan_out_write_within_op_drop(op);
    }
    assert_eq!(left, 1);
    assert_eq!(write_now(f, b"def"), 1);
    assert_eq!(finish(f), 1);
    let t = next_copy(s);
    let meta = meta_of(t);
    assert_eq!(
        meta_text(meta, weida_incoming_meta_topic).as_deref(),
        Some("px.frame")
    );
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };
    assert_eq!(body(t), b"abcdef");

    let bad = c("nope");
    let trace = new_trace();
    // SAFETY: a live publisher, C strings and a writable out-parameter.
    unsafe {
        let rc = weida_publisher_open_with_trace(p, topic.as_ptr(), bad.as_ptr(), &mut f);
        assert_eq!(rc, -WEIDA_EINVAL);
        ok(weida_publisher_open_with_trace(
            p,
            topic.as_ptr(),
            trace.as_ptr(),
            &mut f,
        ));
    }
    assert_eq!(write_now(f, b"x"), 1);
    assert_eq!(finish(f), 1);
    let t = next_copy(s);
    let meta = meta_of(t);
    assert_eq!(
        meta_text(meta, weida_incoming_meta_trace).as_deref(),
        trace.to_str().ok()
    );
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };
    assert_eq!(body(t), b"x");

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_subscriber_drop(s);
        weida_publisher_drop(p);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(client);
    shutdown(server);
}

// --- patterns: PAIR, SURVEY and BUS -----------------------------------------

fn paired(rt: *mut weida_runtime) -> *mut weida_paired {
    let tls = client_tls();
    let mut p = ptr::null_mut();
    // SAFETY: live handles, `tls` dropped once, and a writable out-parameter.
    unsafe {
        ok(weida_runtime_pair(rt, tls, &mut p));
        weida_client_tls_drop(tls);
    }
    p
}

fn surveyor(rt: *mut weida_runtime) -> *mut weida_surveyor {
    let tls = client_tls();
    let mut s = ptr::null_mut();
    // SAFETY: live handles, `tls` dropped once, and a writable out-parameter.
    unsafe {
        ok(weida_runtime_surveyor(rt, tls, &mut s));
        weida_client_tls_drop(tls);
    }
    s
}

fn paired_connect(p: *mut weida_paired, url: &str) -> c_int {
    let url = c(url);
    run_op(
        "the dial",
        // SAFETY: a live pair, a C string and a writable out-parameter.
        |op| unsafe { weida_paired_connect(p, url.as_ptr(), op) },
        weida_paired_connect_op_wait,
        weida_paired_connect_op_drop,
    )
}

fn surveyor_connect(s: *mut weida_surveyor, url: &str) -> c_int {
    let url = c(url);
    run_op(
        "the dial",
        // SAFETY: a live surveyor, a C string and a writable out-parameter.
        |op| unsafe { weida_surveyor_connect(s, url.as_ptr(), op) },
        weida_surveyor_connect_op_wait,
        weida_surveyor_connect_op_drop,
    )
}

fn bus_connect(m: *mut weida_bus_member, url: &str) -> c_int {
    let url = c(url);
    run_op(
        "the join",
        // SAFETY: a live member, a C string and a writable out-parameter.
        |op| unsafe { weida_bus_member_connect(m, url.as_ptr(), op) },
        weida_bus_member_connect_op_wait,
        weida_bus_member_connect_op_drop,
    )
}

fn paired_send(p: *mut weida_paired, body: &[u8]) {
    ok(run_op(
        "the send",
        // SAFETY: a live pair, `body.len()` readable bytes and a writable
        // out-parameter.
        |op| unsafe { weida_paired_send(p, body.as_ptr(), body.len(), op) },
        weida_paired_send_op_wait,
        weida_paired_send_op_drop,
    ));
}

fn paired_recv(p: *mut weida_paired) -> *mut weida_incoming_transfer {
    let mut t = ptr::null_mut();
    // SAFETY: a live pair and a writable out-parameter.
    ok(settle("a transfer from the peer", |ms| unsafe {
        weida_paired_recv(p, ms, &mut t)
    }));
    assert!(!t.is_null());
    t
}

fn bus_recv(m: *mut weida_bus_member) -> *mut weida_incoming_transfer {
    let mut t = ptr::null_mut();
    // SAFETY: a live member and a writable out-parameter.
    ok(settle("a message from another member", |ms| unsafe {
        weida_bus_member_recv(m, ms, &mut t)
    }));
    assert!(!t.is_null());
    t
}

/// Metadata carrying `content_type`; the caller drops it.
fn meta_with(content_type: &str) -> *mut weida_transfer_meta {
    let content_type = c(content_type);
    let mut meta = ptr::null_mut();
    // SAFETY: a writable out-parameter, a live handle and a C string.
    unsafe {
        ok(weida_transfer_meta_new(&mut meta));
        ok(weida_transfer_meta_set_content_type(
            meta,
            content_type.as_ptr(),
        ));
    }
    meta
}

/// Sends `body` from `m` with `meta`: how many members it reached.
fn bus_send(m: *mut weida_bus_member, meta: *const weida_transfer_meta, body: &[u8]) -> usize {
    let mut op = ptr::null_mut();
    let mut n = usize::MAX;
    // SAFETY: live handles, `body.len()` readable bytes and writable
    // out-parameters; the op is dropped once.
    unsafe {
        ok(weida_bus_member_send_with(
            m,
            meta,
            body.as_ptr(),
            body.len(),
            &mut op,
        ));
        ok(settle("the bus send", |ms| {
            weida_bus_member_send_op_wait(op, ms, &mut n)
        }));
        weida_bus_member_send_op_drop(op);
    }
    n
}

/// Answers the next question on `r` with `with`: the question and its
/// content type.
fn answer(r: *mut weida_respondent, with: &[u8]) -> (Vec<u8>, Option<String>) {
    let mut q = ptr::null_mut();
    // SAFETY: a live respondent and a writable out-parameter.
    ok(settle("a survey question", |ms| unsafe {
        weida_respondent_accept(r, ms, &mut q)
    }));
    let mut meta = ptr::null_mut();
    // SAFETY: a live request and a writable out-parameter.
    ok(unsafe { weida_incoming_request_meta(q, &mut meta) });
    let labelled = content_type(meta);
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };
    let question = body(take_body(q));
    let out = reply(q, None);
    write_all(out, with);
    let mut delivery = ptr::null_mut();
    // SAFETY: a live transfer, consumed, and a writable out-parameter; the
    // receipt is dropped once.
    unsafe {
        ok(weida_outgoing_transfer_finish(out, &mut delivery));
        weida_delivery_drop(delivery);
    }
    (question, labelled)
}

/// The next answer of `r`: `None` once the run ended.
fn next_answer(r: *mut weida_survey_run) -> Option<Vec<u8>> {
    let mut a = ptr::null_mut();
    // SAFETY: a live run and a writable out-parameter.
    ok(settle("an answer", |ms| unsafe {
        weida_survey_run_next(r, 1024, ms, &mut a)
    }));
    if a.is_null() {
        return None;
    }
    // SAFETY: a live answer, dropped once.
    unsafe {
        let data =
            std::slice::from_raw_parts(weida_survey_answer_data(a), weida_survey_answer_len(a))
                .to_vec();
        weida_survey_answer_drop(a);
        Some(data)
    }
}

#[test]
fn a_pair_exchanges_in_both_directions() {
    let server = runtime();
    let (l, lb) = bound(server, "abi-patterns-pair");
    let mut near = ptr::null_mut();
    let path = c("/link");
    let url = "weida+inproc://abi-patterns-pair/link";
    let mut len = usize::MAX;
    let mut events = ptr::null_mut();
    // SAFETY: a live listener, a C string and writable out-parameters.
    unsafe {
        ok(weida_listener_pair(l, path.as_ptr(), &mut near));
        let at = weida_paired_path(near, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("/link"));
    }
    assert_eq!(paired_connect(near, url), -WEIDA_EUNSUPPORTED);
    // SAFETY: a live pair and a writable out-parameter.
    unsafe {
        assert_eq!(weida_paired_events(near, &mut events), 0);
        assert!(events.is_null(), "a bound pair reports nothing");
        assert_eq!(weida_paired_peer_count(near), 0);
    }

    let client = runtime();
    let far = paired(client);
    // SAFETY: a live pair and writable out-parameters; the reader is
    // dropped once.
    unsafe {
        let at = weida_paired_path(far, &mut len);
        assert_eq!(text(at, len).as_deref(), Some(""));
        assert_eq!(weida_paired_events(far, &mut events), 1);
        assert!(!events.is_null());
        weida_peer_events_drop(events);
    }
    ok(paired_connect(far, url));
    // SAFETY: a live pair.
    assert_eq!(unsafe { weida_paired_peer_count(far) }, 1);
    assert_eq!(paired_connect(far, url), -WEIDA_ELIMIT_EXCEEDED);

    paired_send(far, b"ping");
    assert_eq!(body(paired_recv(near)), b"ping");

    let meta = meta_with("text/plain");
    ok(run_op(
        "the send",
        // SAFETY: live handles, 4 readable bytes and a writable out-parameter.
        |op| unsafe { weida_paired_send_with(near, meta, b"pong".as_ptr(), 4, op) },
        weida_paired_send_op_wait,
        weida_paired_send_op_drop,
    ));
    // SAFETY: dropped once.
    unsafe { weida_transfer_meta_drop(meta) };
    let pong = paired_recv(far);
    let pong_meta = meta_of(pong);
    assert_eq!(content_type(pong_meta).as_deref(), Some("text/plain"));
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(pong_meta) };
    assert_eq!(body(pong), b"pong");

    let mut out = ptr::null_mut();
    let mut delivery = ptr::null_mut();
    // SAFETY: a live pair and writable out-parameters; the op is dropped
    // once.
    unsafe {
        let mut op = ptr::null_mut();
        ok(weida_paired_open(far, ptr::null(), &mut op));
        ok(settle("the open", |ms| {
            weida_paired_open_op_wait(op, ms, &mut out)
        }));
        weida_paired_open_op_drop(op);
    }
    write_all(out, b"again");
    // SAFETY: a live transfer, consumed, and a writable out-parameter.
    ok(unsafe { weida_outgoing_transfer_finish(out, &mut delivery) });
    assert_eq!(body(paired_recv(near)), b"again");
    ok(run_op(
        "the delivery",
        // SAFETY: a live receipt, consumed, and a writable out-parameter.
        |op| unsafe { weida_delivery_delivered(delivery, op) },
        weida_delivery_delivered_op_wait,
        weida_delivery_delivered_op_drop,
    ));

    let mut stats = ptr::null_mut();
    // SAFETY: live handles and writable out-parameters; each snapshot is
    // dropped once.
    unsafe {
        ok(weida_paired_connection_stats(far, &mut stats));
        assert_eq!(weida_connection_stats_len(stats), 1);
        let at = weida_connection_stats_url(stats, 0, &mut len);
        assert_eq!(text(at, len).as_deref(), Some(url));
        weida_connection_stats_drop(stats);
        ok(weida_paired_connection_stats(near, &mut stats));
        assert_eq!(weida_connection_stats_len(stats), 0);
        weida_connection_stats_drop(stats);
        assert_eq!(weida_paired_dropped(far), 0);
    }

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_paired_drop(far);
        weida_paired_drop(near);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(client);
    shutdown(server);
}

#[test]
fn a_survey_collects_two_respondents_answers_within_its_window() {
    let server = runtime();
    let (la, lba) = bound(server, "abi-patterns-survey-a");
    let (lb, lbb) = bound(server, "abi-patterns-survey-b");
    let path = c("/poll");
    let (mut ra, mut rb, mut again) = (ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
    let mut len = usize::MAX;
    // SAFETY: live listeners, a C string and writable out-parameters.
    unsafe {
        ok(weida_listener_respondent(la, path.as_ptr(), &mut ra));
        ok(weida_listener_respondent(lb, path.as_ptr(), &mut rb));
        let at = weida_respondent_path(ra, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("/poll"));
        assert_eq!(
            weida_listener_respondent(la, path.as_ptr(), &mut again),
            -WEIDA_EALREADY_REGISTERED
        );
    }

    let client = runtime();
    let s = surveyor(client);
    let mut op = ptr::null_mut();
    let mut run = ptr::null_mut();
    // Nobody to ask: an empty run, not an error.
    // SAFETY: a live surveyor, 7 readable bytes and writable
    // out-parameters; the op and the run are dropped once.
    unsafe {
        ok(weida_surveyor_survey(
            s,
            b"anyone?".as_ptr(),
            7,
            5000,
            &mut op,
        ));
        ok(settle("the empty survey", |ms| {
            weida_surveyor_survey_op_wait(op, ms, &mut run)
        }));
        weida_surveyor_survey_op_drop(op);
        assert_eq!(weida_survey_run_respondents(run), 0);
    }
    assert_eq!(next_answer(run), None);
    // SAFETY: a live run, dropped once.
    unsafe {
        assert_eq!(weida_survey_run_late(run), 0);
        weida_survey_run_drop(run);
    }

    ok(surveyor_connect(
        s,
        "weida+inproc://abi-patterns-survey-a/poll",
    ));
    ok(surveyor_connect(
        s,
        "weida+inproc://abi-patterns-survey-b/poll",
    ));
    let mut stats = ptr::null_mut();
    // SAFETY: a live surveyor and a writable out-parameter; the snapshot is
    // dropped once.
    unsafe {
        assert_eq!(weida_surveyor_peer_count(s), 2);
        ok(weida_surveyor_connection_stats(s, &mut stats));
        assert_eq!(weida_connection_stats_len(stats), 2);
        weida_connection_stats_drop(stats);
    }

    let meta = meta_with("text/plain");
    // SAFETY: live handles, 12 readable bytes and a writable out-parameter;
    // `meta` is copied by the call and dropped once.
    unsafe {
        ok(weida_surveyor_survey_with(
            s,
            meta,
            b"who is there".as_ptr(),
            12,
            60_000,
            &mut op,
        ));
        weida_transfer_meta_drop(meta);
    }
    for (r, with) in [(ra, &b"a"[..]), (rb, b"b")] {
        let (question, labelled) = answer(r, with);
        assert_eq!(question, b"who is there");
        assert_eq!(labelled.as_deref(), Some("text/plain"));
    }
    // SAFETY: a live op, dropped once, and a writable out-parameter.
    unsafe {
        ok(settle("the survey", |ms| {
            weida_surveyor_survey_op_wait(op, ms, &mut run)
        }));
        weida_surveyor_survey_op_drop(op);
        assert_eq!(weida_survey_run_respondents(run), 2);
    }
    let mut answers = [next_answer(run), next_answer(run)].map(|a| a.expect("an answer"));
    answers.sort();
    assert_eq!(answers, [b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(next_answer(run), None, "the run ends once both answered");

    // SAFETY: every handle is dropped once.
    unsafe {
        assert_eq!(weida_survey_run_late(run), 0);
        weida_survey_run_drop(run);
        weida_surveyor_drop(s);
        weida_respondent_drop(ra);
        weida_respondent_drop(rb);
        weida_local_binding_drop(lba);
        weida_local_binding_drop(lbb);
        weida_listener_drop(la);
        weida_listener_drop(lb);
    }
    shutdown(client);
    shutdown(server);
}

#[test]
fn a_bus_message_reaches_every_other_member() {
    let rts = [runtime(), runtime(), runtime()];
    let path = c("/bus");
    let mut bindings = Vec::new();
    let mut members = Vec::new();
    for (i, rt) in rts.iter().enumerate() {
        let (l, lb) = bound(*rt, &format!("abi-patterns-bus-{i}"));
        let tls = client_tls();
        let mut m = ptr::null_mut();
        let mut len = usize::MAX;
        // SAFETY: live handles, a C string and writable out-parameters;
        // `tls` is dropped once.
        unsafe {
            ok(weida_listener_bus(l, path.as_ptr(), tls, &mut m));
            let at = weida_bus_member_path(m, &mut len);
            assert_eq!(text(at, len).as_deref(), Some("/bus"));
            if i == 0 {
                let mut again = ptr::null_mut();
                assert_eq!(
                    weida_listener_bus(l, path.as_ptr(), tls, &mut again),
                    -WEIDA_EALREADY_REGISTERED
                );
            }
            weida_client_tls_drop(tls);
        }
        bindings.push((l, lb));
        members.push(m);
    }
    for (i, m) in members.iter().enumerate() {
        for j in (0..members.len()).filter(|j| *j != i) {
            ok(bus_connect(
                *m,
                &format!("weida+inproc://abi-patterns-bus-{j}/bus"),
            ));
        }
        let mut stats = ptr::null_mut();
        // SAFETY: a live member and a writable out-parameter; the snapshot
        // is dropped once.
        unsafe {
            assert_eq!(weida_bus_member_peer_count(*m), 2);
            ok(weida_bus_member_connection_stats(*m, &mut stats));
            assert_eq!(weida_connection_stats_len(stats), 2);
            weida_connection_stats_drop(stats);
        }
    }

    let meta = meta_with("text/plain");
    assert_eq!(bus_send(members[0], meta, b"from 0"), 2);
    // SAFETY: dropped once.
    unsafe { weida_transfer_meta_drop(meta) };
    assert_eq!(bus_send(members[1], ptr::null(), b"from 1"), 2);
    assert_eq!(bus_send(members[2], ptr::null(), b"from 2"), 2);

    for (i, m) in members.iter().enumerate() {
        let mut seen = Vec::new();
        for _ in 0..2 {
            let t = bus_recv(*m);
            let meta = meta_of(t);
            let labelled = content_type(meta);
            // SAFETY: dropped once.
            unsafe { weida_incoming_meta_drop(meta) };
            let data = body(t);
            let expected = (data == b"from 0").then_some("text/plain");
            assert_eq!(labelled.as_deref(), expected, "member {i}");
            seen.push(data);
        }
        seen.sort();
        let others: Vec<Vec<u8>> = (0..members.len())
            .filter(|j| *j != i)
            .map(|j| format!("from {j}").into_bytes())
            .collect();
        assert_eq!(seen, others, "member {i}");
    }
    for m in &members {
        let mut t = ptr::null_mut();
        // SAFETY: a live member and a writable out-parameter.
        unsafe {
            let rc = weida_bus_member_recv(*m, 200, &mut t);
            assert_eq!(rc, -WEIDA_EAGAIN, "nobody gets its own message");
            assert_eq!(weida_bus_member_dropped(*m), 0);
        }
    }

    // SAFETY: every handle is dropped once.
    unsafe {
        for m in members {
            weida_bus_member_drop(m);
        }
        for (l, lb) in bindings {
            weida_local_binding_drop(lb);
            weida_listener_drop(l);
        }
    }
    for rt in rts {
        shutdown(rt);
    }
}

// --- patterns: RADIO/DISH ---------------------------------------------------

/// A configuration whose limits carry datagrams
/// (`WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES`); its guarantee set is the
/// default, so a dish on the default configuration still negotiates with
/// it. The caller drops it.
fn datagram_config() -> *mut weida_runtime_config {
    let mut cfg = ptr::null_mut();
    let mut l = ptr::null_mut();
    // SAFETY: writable out-parameters and live handles; `l` is dropped once.
    unsafe {
        ok(weida_runtime_config_new(&mut cfg));
        ok(weida_limits_new(&mut l));
        ok(weida_limits_set_datagram_receive_bytes(
            l,
            WEIDA_DEFAULT_DATAGRAM_RECEIVE_BYTES as u64,
        ));
        ok(weida_runtime_config_set_limits(cfg, l));
        weida_limits_drop(l);
    }
    cfg
}

/// A dish of `rt` dialling on `tls`, which is dropped.
fn dish(rt: *mut weida_runtime, tls: *mut weida_client_tls) -> *mut weida_dish {
    let mut d = ptr::null_mut();
    // SAFETY: live handles, `tls` dropped once, and a writable out-parameter.
    unsafe {
        ok(weida_runtime_dish(rt, tls, &mut d));
        weida_client_tls_drop(tls);
    }
    d
}

fn dish_connect(d: *mut weida_dish, url: &str) -> c_int {
    let url = c(url);
    run_op(
        "the dial",
        // SAFETY: a live dish, a C string and a writable out-parameter.
        |op| unsafe { weida_dish_connect(d, url.as_ptr(), op) },
        weida_dish_connect_op_wait,
        weida_dish_connect_op_drop,
    )
}

fn dish_join(d: *mut weida_dish, filter: &str, terms: *const weida_join_terms) -> c_int {
    let filter = c(filter);
    run_op(
        "the join",
        // SAFETY: a live dish, a C string, live or NULL terms and a writable
        // out-parameter.
        |op| unsafe { weida_dish_join(d, filter.as_ptr(), terms, op) },
        weida_dish_join_op_wait,
        weida_dish_join_op_drop,
    )
}

/// Waits until `n` dishes joined `r`.
fn await_dishes(r: *mut weida_radio, n: usize) {
    weida_testkit::poll_until("the joins", WAIT, || {
        // SAFETY: a live radio.
        unsafe { weida_radio_dish_count(r) == n }
    });
}

fn next_received(d: *mut weida_dish) -> *mut weida_received {
    let mut x = ptr::null_mut();
    // SAFETY: a live dish and a writable out-parameter.
    ok(settle("a segment", |ms| unsafe {
        weida_dish_recv(d, ms, &mut x)
    }));
    assert!(!x.is_null());
    x
}

/// The transfer of a stream segment, taken once; drops `x`.
fn take_segment(x: *mut weida_received) -> *mut weida_incoming_transfer {
    let mut t = ptr::null_mut();
    let mut again = ptr::null_mut();
    // SAFETY: a live received segment, dropped once, and writable
    // out-parameters.
    unsafe {
        ok(weida_received_take_segment(x, &mut t));
        assert_eq!(
            weida_received_take_segment(x, &mut again),
            -WEIDA_ECLOSED,
            "a transfer is handed over once"
        );
        weida_received_drop(x);
    }
    t
}

fn segment_of(meta: *mut weida_incoming_meta) -> (Option<u64>, Option<u8>) {
    let (mut number, mut layer) = (0u64, 0u8);
    // SAFETY: live metadata and writable out-parameters.
    unsafe {
        (
            (weida_incoming_meta_segment(meta, &mut number) == 1).then_some(number),
            (weida_incoming_meta_layer(meta, &mut layer) == 1).then_some(layer),
        )
    }
}

/// The next thing `d` received, which must be a stream segment on `topic`:
/// its transfer with its segment number and layer.
fn stream_segment(d: *mut weida_dish, topic: &str) -> (*mut weida_incoming_transfer, u64, u8) {
    let x = next_received(d);
    let mut len = usize::MAX;
    let mut n = u64::MAX;
    // SAFETY: a live received segment and writable out-parameters.
    unsafe {
        assert_eq!(weida_received_kind(x), WEIDA_RECEIVED_SEGMENT);
        assert!(weida_received_topic(x, &mut len).is_null());
        assert_eq!(weida_received_segment(x, &mut n), 0);
        assert!(weida_received_payload(x, &mut len).is_null());
    }
    let t = take_segment(x);
    let meta = meta_of(t);
    assert_eq!(
        meta_text(meta, weida_incoming_meta_topic).as_deref(),
        Some(topic)
    );
    let (number, layer) = segment_of(meta);
    // SAFETY: dropped once.
    unsafe { weida_incoming_meta_drop(meta) };
    (
        t,
        number.expect("a segment number"),
        layer.expect("a layer"),
    )
}

/// Opens a segment of `r` on `topic`.
fn open_segment(
    r: *mut weida_radio,
    topic: &str,
    terms: *const weida_segment_terms,
) -> *mut weida_segment {
    let topic = c(topic);
    let mut s = ptr::null_mut();
    // SAFETY: a live radio, a C string, live or NULL terms and a writable
    // out-parameter.
    ok(unsafe { weida_radio_segment(r, topic.as_ptr(), terms, &mut s) });
    s
}

/// Writes `chunk` to `layer` of `s`: how many copies took it.
fn write_layer(s: *mut weida_segment, layer: u8, chunk: &[u8]) -> usize {
    let mut n = usize::MAX;
    // SAFETY: a live segment, `chunk.len()` readable bytes and a writable
    // out-parameter.
    ok(unsafe { weida_segment_write_layer(s, layer, chunk.as_ptr(), chunk.len(), &mut n) });
    n
}

/// Finishes `s`, consuming it: how many copies still hold layer 0.
fn finish_segment(s: *mut weida_segment) -> usize {
    let mut n = usize::MAX;
    // SAFETY: a live segment, consumed, and a writable out-parameter.
    ok(unsafe { weida_segment_finish(s, &mut n) });
    n
}

/// One count of a dish-drops snapshot: `Some` when present.
fn dish_drop_count(
    d: *mut weida_dish_drops,
    index: usize,
    get: unsafe extern "C" fn(*const weida_dish_drops, usize, *mut u64) -> c_int,
) -> Option<u64> {
    let mut n = 0u64;
    // SAFETY: a live snapshot and a writable out-parameter.
    (unsafe { get(d, index, &mut n) } == 1).then_some(n)
}

/// The radio's dish-drops snapshot; the caller drops it.
fn dish_drops_of(r: *mut weida_radio) -> *mut weida_dish_drops {
    let mut d = ptr::null_mut();
    // SAFETY: a live radio and a writable out-parameter.
    ok(unsafe { weida_radio_dish_drops(r, &mut d) });
    d
}

/// A radio at `/r` on a new listener of `rt` bound to the inproc bus `bus`.
fn radio_on(
    rt: *mut weida_runtime,
    bus: &str,
) -> (
    *mut weida_listener,
    *mut weida_local_binding,
    *mut weida_radio,
) {
    let (l, lb) = bound(rt, bus);
    let path = c("/r");
    let mut r = ptr::null_mut();
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_radio(l, path.as_ptr(), &mut r) });
    (l, lb, r)
}

#[test]
fn a_dish_receives_the_radios_segments_with_their_numbers_and_layers() {
    let server = runtime();
    let (l, lb, r) = radio_on(server, "abi-patterns-radio-layers");
    let path = c("/r");
    let mut again = ptr::null_mut();
    let mut len = usize::MAX;
    // SAFETY: live handles, a C string and writable out-parameters.
    unsafe {
        let at = weida_radio_path(r, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("/r"));
        assert_eq!(
            weida_listener_radio(l, path.as_ptr(), &mut again),
            -WEIDA_EALREADY_REGISTERED
        );
    }

    let mut t = ptr::null_mut();
    // SAFETY: a writable out-parameter and live terms.
    unsafe {
        ok(weida_join_terms_new(&mut t));
        assert_eq!(weida_join_terms_set_max_layer(t, 256), -WEIDA_EINVAL);
        ok(weida_join_terms_set_max_layer(t, 16));
    }
    let client = runtime();
    let d = dish(client, client_tls());
    assert_eq!(dish_join(d, "v", t), -WEIDA_ELIMIT_EXCEEDED);
    // SAFETY: live terms, dropped once after the join copied them.
    ok(unsafe { weida_join_terms_set_max_layer(t, 1) });
    ok(dish_join(d, "v", t));
    // SAFETY: dropped once.
    unsafe { weida_join_terms_drop(t) };

    ok(dish_connect(
        d,
        "weida+inproc://abi-patterns-radio-layers/r",
    ));
    await_dishes(r, 1);
    let mut stats = ptr::null_mut();
    let mut events = ptr::null_mut();
    // SAFETY: a live dish and writable out-parameters; each handle is
    // dropped once.
    unsafe {
        assert_eq!(weida_dish_peer_count(d), 1);
        ok(weida_dish_connection_stats(d, &mut stats));
        assert_eq!(weida_connection_stats_len(stats), 1);
        weida_connection_stats_drop(stats);
        ok(weida_dish_events(d, &mut events));
        assert!(!events.is_null());
        weida_peer_events_drop(events);
    }

    let long = c(&"t".repeat(257));
    let mut refused = ptr::null_mut();
    // SAFETY: a live radio, a C string and a writable out-parameter.
    assert_eq!(
        unsafe { weida_radio_segment(r, long.as_ptr(), ptr::null(), &mut refused) },
        -WEIDA_ELIMIT_EXCEEDED
    );

    let s0 = open_segment(r, "v", ptr::null());
    // SAFETY: a live segment and a writable length.
    unsafe {
        assert_eq!(weida_segment_number(s0), 0);
        let at = weida_segment_topic(s0, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("v"));
    }
    assert_eq!(write_layer(s0, 0, b"a0"), 1);
    assert_eq!(write_layer(s0, 1, b"a1"), 1);
    assert_eq!(
        write_layer(s0, 2, b"a2"),
        0,
        "the copy is capped at layer 1"
    );
    let mut n = usize::MAX;
    // SAFETY: a live segment, 2 readable bytes and a writable out-parameter.
    unsafe {
        assert_eq!(
            weida_segment_write_layer(s0, 16, b"xx".as_ptr(), 2, &mut n),
            -WEIDA_ELIMIT_EXCEEDED
        );
        ok(weida_segment_finish_layer(s0, 0));
        assert_eq!(
            weida_segment_write(s0, b"xx".as_ptr(), 2, &mut n),
            -WEIDA_ERUNTIME
        );
    }
    assert_eq!(finish_segment(s0), 1);

    let mut seen = Vec::new();
    for _ in 0..2 {
        let (t, number, layer) = stream_segment(d, "v");
        seen.push((number, layer, body(t)));
    }

    let mut st = ptr::null_mut();
    // SAFETY: a writable out-parameter and live terms.
    unsafe {
        ok(weida_segment_terms_new(&mut st));
        assert_eq!(weida_segment_terms_set_priority(st, 40_000), -WEIDA_EINVAL);
        ok(weida_segment_terms_set_priority(st, 1));
        ok(weida_segment_terms_set_max_age_ms(st, 60_000));
    }
    let s1 = open_segment(r, "v", st);
    // SAFETY: live handles, 2 readable bytes and a writable out-parameter;
    // the terms are dropped once.
    unsafe {
        weida_segment_terms_drop(st);
        assert_eq!(weida_segment_number(s1), 1);
        ok(weida_segment_write(s1, b"b0".as_ptr(), 2, &mut n));
    }
    assert_eq!(n, 1);
    assert_eq!(finish_segment(s1), 1);
    let (t, number, layer) = stream_segment(d, "v");
    seen.push((number, layer, body(t)));
    seen.sort();
    assert_eq!(
        seen,
        [
            (0, 0, b"a0".to_vec()),
            (0, 1, b"a1".to_vec()),
            (1, 0, b"b0".to_vec())
        ]
    );

    let mut drops = ptr::null_mut();
    // SAFETY: live handles and a writable out-parameter; each snapshot is
    // dropped once.
    unsafe {
        assert_eq!(weida_radio_dropped(r), 0);
        ok(weida_radio_drops(r, &mut drops));
        assert_eq!(weida_topic_drops_len(drops), 0);
        weida_topic_drops_drop(drops);
    }
    let dd = dish_drops_of(r);
    // SAFETY: a live snapshot.
    assert_eq!(unsafe { weida_dish_drops_len(dd) }, 1);
    for get in [
        weida_dish_drops_subscriber_budget,
        weida_dish_drops_subscriber_queue,
        weida_dish_drops_superseded,
        weida_dish_drops_expired,
        weida_dish_drops_too_large,
        weida_dish_drops_no_datagrams,
        weida_dish_drops_layers_cut,
    ] {
        assert_eq!(dish_drop_count(dd, 0, get), Some(0));
        assert_eq!(dish_drop_count(dd, 1, get), None);
    }

    // SAFETY: every handle is dropped once.
    unsafe {
        assert_eq!(weida_dish_stale(d), 0);
        assert_eq!(weida_dish_overflow(d), 0);
        weida_dish_drops_drop(dd);
        weida_dish_drop(d);
        weida_radio_drop(r);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(client);
    shutdown(server);
}

#[test]
fn a_superseded_segment_counts_in_dish_drops() {
    let server = runtime();
    let (l, lb, r) = radio_on(server, "abi-patterns-radio-supersede");
    let client = runtime();
    let d = dish(client, client_tls());
    ok(dish_join(d, "v", ptr::null()));
    ok(dish_connect(
        d,
        "weida+inproc://abi-patterns-radio-supersede/r",
    ));
    await_dishes(r, 1);

    let first = open_segment(r, "v", ptr::null());
    assert_eq!(write_layer(first, 0, b"first"), 1);
    let second = open_segment(r, "v", ptr::null());
    assert_eq!(write_layer(second, 0, b"second"), 1);
    assert_eq!(finish_segment(second), 1);

    weida_testkit::poll_until("the supersession", WAIT, || {
        let dd = dish_drops_of(r);
        let superseded = dish_drop_count(dd, 0, weida_dish_drops_superseded);
        // SAFETY: dropped once.
        unsafe { weida_dish_drops_drop(dd) };
        superseded == Some(1)
    });
    let topic = c("v");
    let mut drops = ptr::null_mut();
    // SAFETY: a live radio, a C string and a writable out-parameter; the
    // snapshot is dropped once.
    unsafe {
        ok(weida_radio_dropped_on(r, topic.as_ptr(), &mut drops));
        assert_eq!(weida_topic_drops_len(drops), 1);
        assert_eq!(drop_count(drops, 0, weida_topic_drops_superseded), Some(1));
        weida_topic_drops_drop(drops);
        assert_eq!(weida_radio_dropped(r), 1);
    }
    assert_eq!(finish_segment(first), 0, "the copy was reset");

    loop {
        let (t, number, layer) = stream_segment(d, "v");
        assert_eq!(layer, 0);
        let read = read_all(t, 1024);
        // SAFETY: dropped once.
        unsafe { weida_incoming_transfer_drop(t) };
        if number == 1 {
            assert_eq!(read.expect("segment 1"), b"second");
            break;
        }
        assert!(read.is_err(), "segment 0 was reset, never finished");
    }

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_dish_drop(d);
        weida_radio_drop(r);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(client);
    shutdown(server);
}

#[test]
fn a_radio_datagram_reaches_a_dish_that_carries_datagrams() {
    let cfg = datagram_config();
    let server = runtime_with(cfg);
    let near = runtime_with(cfg);
    let far = runtime();
    // SAFETY: dropped once; the runtimes copied it.
    unsafe { weida_runtime_config_drop(cfg) };
    let (l, lb, r) = radio_on(server, "abi-patterns-radio-datagram");
    let a = dish(near, client_tls());
    let b = dish(far, client_tls());
    for d in [a, b] {
        ok(dish_join(d, "voice", ptr::null()));
        ok(dish_connect(
            d,
            "weida+inproc://abi-patterns-radio-datagram/r",
        ));
    }
    await_dishes(r, 2);

    let topic = c("voice");
    let mut n = usize::MAX;
    // SAFETY: a live radio, a C string, 5 readable bytes and a writable
    // out-parameter.
    ok(unsafe { weida_radio_datagram(r, topic.as_ptr(), b"hello".as_ptr(), 5, &mut n) });
    assert_eq!(n, 1, "only one dish carries datagrams");

    let x = next_received(a);
    let mut len = usize::MAX;
    let mut number = u64::MAX;
    let mut t = ptr::null_mut();
    // SAFETY: a live received segment, dropped once, and writable
    // out-parameters; the payload lives with it.
    unsafe {
        assert_eq!(weida_received_kind(x), WEIDA_RECEIVED_DATAGRAM);
        let at = weida_received_topic(x, &mut len);
        assert_eq!(text(at, len).as_deref(), Some("voice"));
        assert_eq!(weida_received_segment(x, &mut number), 1);
        assert_eq!(number, 0);
        let payload = weida_received_payload(x, &mut len);
        assert_eq!(std::slice::from_raw_parts(payload, len), b"hello");
        assert_eq!(weida_received_take_segment(x, &mut t), -WEIDA_EINVAL);
        weida_received_drop(x);
    }

    let mut drops = ptr::null_mut();
    // SAFETY: a live radio, a C string and a writable out-parameter; the
    // snapshot is dropped once.
    unsafe {
        ok(weida_radio_dropped_on(r, topic.as_ptr(), &mut drops));
        assert_eq!(
            drop_count(drops, 0, weida_topic_drops_no_datagrams),
            Some(1)
        );
        weida_topic_drops_drop(drops);
    }
    let dd = dish_drops_of(r);
    // SAFETY: a live snapshot.
    assert_eq!(unsafe { weida_dish_drops_len(dd) }, 2);
    let mut counts: Vec<Option<u64>> = (0..2)
        .map(|i| dish_drop_count(dd, i, weida_dish_drops_no_datagrams))
        .collect();
    counts.sort();
    assert_eq!(counts, [Some(0), Some(1)]);

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_dish_drops_drop(dd);
        weida_dish_drop(a);
        weida_dish_drop(b);
        weida_radio_drop(r);
        weida_local_binding_drop(lb);
        weida_listener_drop(l);
    }
    shutdown(near);
    shutdown(far);
    shutdown(server);
}

#[test]
fn a_radio_evicts_a_dish_by_its_key() {
    let server = runtime();
    let l = listener(server);
    let (server_id, server_fp) = identity();
    let tls = server_tls(server_id);
    // SAFETY: live server terms.
    ok(unsafe { weida_server_tls_require_any_key(tls) });
    let (binding, local) = bind_quic(l, tls);
    let path = c("/r");
    let mut r = ptr::null_mut();
    // SAFETY: a live listener, a C string and a writable out-parameter.
    ok(unsafe { weida_listener_radio(l, path.as_ptr(), &mut r) });

    let client = runtime();
    let (client_id, client_fp) = identity();
    let trust = trust_by_address();
    let mut dish_tls = ptr::null_mut();
    // SAFETY: live handles and a writable out-parameter; `trust` is dropped
    // once.
    unsafe {
        ok(weida_client_tls_new(trust, &mut dish_tls));
        ok(weida_client_tls_set_identity(dish_tls, client_id));
        weida_trust_drop(trust);
    }
    let d = dish(client, dish_tls);
    ok(dish_join(d, "v", ptr::null()));
    ok(dish_connect(d, &format!("weida://{server_fp}@{local}/r")));
    await_dishes(r, 1);

    let dd = dish_drops_of(r);
    let mut len = usize::MAX;
    // SAFETY: a live snapshot, dropped once, and a writable length.
    unsafe {
        let at = weida_dish_drops_peer_fingerprint(dd, 0, &mut len);
        assert_eq!(text(at, len), Some(client_fp.clone()));
        assert!(weida_dish_drops_peer_fingerprint(dd, 1, &mut len).is_null());
        weida_dish_drops_drop(dd);
    }

    let (stranger, stranger_fp) = identity();
    let stranger_key = c(&stranger_fp);
    let nope = c("nope");
    let client_key = c(&client_fp);
    let filter = c("v");
    let mut n = usize::MAX;
    // SAFETY: a live radio, C strings and a writable out-parameter.
    unsafe {
        ok(weida_radio_evict(
            r,
            stranger_key.as_ptr(),
            filter.as_ptr(),
            &mut n,
        ));
        assert_eq!(n, 0, "nobody of that key joined");
        assert_eq!(
            weida_radio_evict(r, nope.as_ptr(), filter.as_ptr(), &mut n),
            -WEIDA_EINVALID_FINGERPRINT
        );
        ok(weida_radio_evict(
            r,
            client_key.as_ptr(),
            filter.as_ptr(),
            &mut n,
        ));
    }
    assert_eq!(n, 1);

    let s = open_segment(r, "v", ptr::null());
    assert_eq!(write_layer(s, 0, b"x"), 0, "the evicted dish gets no copy");
    assert_eq!(finish_segment(s), 0);

    // SAFETY: every handle is dropped once.
    unsafe {
        weida_dish_drop(d);
        weida_identity_drop(client_id);
        weida_identity_drop(stranger);
        weida_radio_drop(r);
        weida_binding_drop(binding);
        weida_server_tls_drop(tls);
        weida_identity_drop(server_id);
        weida_listener_drop(l);
    }
    shutdown(client);
    shutdown(server);
}

// --- cursors ----------------------------------------------------------------

const ACCEPTED: u64 = WEIDA_ACKNOWLEDGEMENT_ACCEPTED as u64;
const STORED: u64 = WEIDA_ACKNOWLEDGEMENT_STORED as u64;

/// Metadata that orders `levels` reported in `mode`; the caller drops it.
fn report_meta(levels: &[u64], mode: c_int) -> *mut weida_transfer_meta {
    let mut m = ptr::null_mut();
    // SAFETY: a writable out-parameter, then live metadata and
    // `levels.len()` readable values.
    unsafe {
        ok(weida_transfer_meta_new(&mut m));
        ok(weida_transfer_meta_set_report(
            m,
            levels.as_ptr(),
            levels.len(),
        ));
        ok(weida_transfer_meta_set_report_mode(m, mode));
    }
    m
}

/// Opens an exchange on `r` with `meta`: the request body and the reply
/// stream, or the code the open failed with.
fn open_with(
    r: *mut weida_requester,
    meta: *const weida_transfer_meta,
) -> Result<(*mut weida_outgoing_transfer, *mut weida_reply_stream), c_int> {
    let mut op = ptr::null_mut();
    let mut body = ptr::null_mut();
    let mut stream = ptr::null_mut();
    // SAFETY: live handles, the op dropped once, and writable
    // out-parameters.
    let rc = unsafe {
        ok(weida_requester_open(r, meta, &mut op));
        let rc = settle("the exchange", |ms| {
            weida_requester_open_op_wait(op, ms, &mut body, &mut stream)
        });
        weida_requester_open_op_drop(op);
        rc
    };
    if rc == 0 { Ok((body, stream)) } else { Err(rc) }
}

/// The cursor queue of `t`, which hands it out once.
fn cursors_of(t: *mut weida_outgoing_transfer) -> *mut weida_cursors {
    let mut c = ptr::null_mut();
    let mut again = ptr::null_mut();
    // SAFETY: a live transfer and writable out-parameters.
    unsafe {
        assert_eq!(weida_outgoing_transfer_cursors(t, &mut c), 1);
        assert_eq!(weida_outgoing_transfer_cursors(t, &mut again), 0);
    }
    assert!(!c.is_null());
    assert!(again.is_null(), "the queue is handed out once");
    c
}

fn reporter_of(t: *mut weida_incoming_transfer) -> *mut weida_reporter {
    let mut r = ptr::null_mut();
    // SAFETY: a live transfer and a writable out-parameter.
    assert_eq!(unsafe { weida_incoming_transfer_reporter(t, &mut r) }, 1);
    r
}

/// The next cursor set of `c`, NULL at the end.
fn next_set(c: *mut weida_cursors) -> *mut weida_cursor_set {
    let mut s = ptr::null_mut();
    // SAFETY: a live queue and a writable out-parameter.
    ok(settle("a cursor set", |ms| unsafe {
        weida_cursors_recv(c, ms, &mut s)
    }));
    s
}

/// Every `(level, offset)` of `s` in index order; drops `s`.
fn entries(s: *mut weida_cursor_set) -> Vec<(u64, u64)> {
    // SAFETY: a live set, dropped once, and writable out-parameters.
    unsafe {
        let out = (0..weida_cursor_set_len(s))
            .map(|i| {
                let (mut level, mut offset) = (0, 0);
                assert_eq!(weida_cursor_set_level(s, i, &mut level, &mut offset), 1);
                (level, offset)
            })
            .collect();
        weida_cursor_set_drop(s);
        out
    }
}

fn report(r: *mut weida_reporter, level: u64, offset: u64) -> c_int {
    run_op(
        "the report",
        // SAFETY: a live reporter and a writable out-parameter.
        |op| unsafe { weida_reporter_report(r, level, offset, op) },
        weida_reporter_report_op_wait,
        weida_reporter_report_op_drop,
    )
}

/// Finishes `r`, consuming it.
fn finish_reporter(r: *mut weida_reporter) -> c_int {
    run_op(
        "the flush",
        // SAFETY: a live reporter, consumed, and a writable out-parameter.
        |op| unsafe { weida_reporter_finish(r, op) },
        weida_reporter_finish_op_wait,
        weida_reporter_finish_op_drop,
    )
}

/// A level array a getter handed out; empty for NULL.
fn levels_of(p: *const u64, len: usize) -> Vec<u64> {
    if p.is_null() {
        return Vec::new();
    }
    // SAFETY: a getter's non-NULL result points to `len` values.
    unsafe { std::slice::from_raw_parts(p, len) }.to_vec()
}

/// The report order the replier sees on `q`: levels and mode.
fn ordered(q: *mut weida_incoming_request) -> (Vec<u64>, c_int, c_int) {
    let mut meta = ptr::null_mut();
    let mut len = usize::MAX;
    let mut id = u64::MAX;
    // SAFETY: a live request, the metadata dropped once, and writable
    // out-parameters.
    unsafe {
        ok(weida_incoming_request_meta(q, &mut meta));
        let levels = levels_of(weida_incoming_meta_report(meta, &mut len), len);
        let mode = weida_incoming_meta_report_mode(meta);
        let has_id = weida_incoming_meta_report_id(meta, &mut id);
        weida_incoming_meta_drop(meta);
        (levels, mode, has_id)
    }
}

fn finish_body(t: *mut weida_outgoing_transfer) {
    let mut delivery = ptr::null_mut();
    // SAFETY: a live transfer, consumed, and the receipt dropped once.
    unsafe {
        ok(weida_outgoing_transfer_finish(t, &mut delivery));
        weida_delivery_drop(delivery);
    }
}

#[test]
fn a_request_that_orders_accepted_receives_the_reported_offsets_in_order() {
    let pair = Pair::new("abi-cursors-progress");
    let mut m = ptr::null_mut();
    let reserved = [7u64];
    let twice = [ACCEPTED, ACCEPTED];
    // SAFETY: a writable out-parameter, live metadata and readable arrays.
    unsafe {
        ok(weida_transfer_meta_new(&mut m));
        assert_eq!(
            weida_transfer_meta_set_report(m, reserved.as_ptr(), 1),
            -WEIDA_EINVAL
        );
        assert_eq!(
            weida_transfer_meta_set_report(m, ptr::null(), 1),
            -WEIDA_EINVAL
        );
        assert_eq!(weida_transfer_meta_set_report_mode(m, 2), -WEIDA_EINVAL);
        ok(weida_transfer_meta_set_report(m, twice.as_ptr(), 2));
        ok(weida_transfer_meta_set_report_mode(
            m,
            WEIDA_REPORT_MODE_PROGRESS,
        ));
    }
    let (body, stream) = open_with(pair.req, m).expect("the exchange");
    // SAFETY: dropped once.
    unsafe { weida_transfer_meta_drop(m) };
    let c = cursors_of(body);
    let mut s = ptr::null_mut();
    // SAFETY: a live queue and a writable out-parameter.
    assert_eq!(
        unsafe { weida_cursors_recv(c, 0, &mut s) },
        -WEIDA_EAGAIN,
        "nothing reported yet"
    );
    write_all(body, b"aaaabbbbcccc");
    finish_body(body);

    let q = accept(pair.rep);
    assert_eq!(ordered(q), (vec![ACCEPTED], WEIDA_REPORT_MODE_PROGRESS, 1));
    let t = take_body(q);
    let rep = reporter_of(t);
    let mut len = usize::MAX;
    // SAFETY: a live reporter and a writable length.
    unsafe {
        let levels = weida_reporter_levels(rep, &mut len);
        assert_eq!(levels_of(levels, len), [ACCEPTED]);
        assert_eq!(weida_reporter_mode(rep), WEIDA_REPORT_MODE_PROGRESS);
        ok(weida_reporter_set_granularity(rep, 0, 0));
    }
    assert_eq!(read_all(t, 64).expect("the body"), b"aaaabbbbcccc");
    let mut op = ptr::null_mut();
    // SAFETY: a live reporter and a writable out-parameter.
    assert_eq!(
        unsafe { weida_reporter_report(rep, 7, 1, &mut op) },
        -WEIDA_EINVAL
    );
    assert_eq!(report(rep, STORED, 12), 0, "a level not ordered is ignored");

    for offset in [4, 8] {
        assert_eq!(report(rep, ACCEPTED, offset), 0);
        assert_eq!(entries(next_set(c)), [(ACCEPTED, offset)]);
    }
    assert_eq!(report(rep, ACCEPTED, 12), 0);
    assert_eq!(finish_reporter(rep), 0);
    let s = next_set(c);
    let mut offset = 0;
    let (mut level, mut at) = (u64::MAX, u64::MAX);
    // SAFETY: a live set and writable out-parameters.
    unsafe {
        assert_eq!(weida_cursor_set_offset(s, ACCEPTED, &mut offset), 1);
        assert_eq!(weida_cursor_set_offset(s, STORED, &mut offset), 0);
        assert_eq!(weida_cursor_set_offset(s, 7, &mut offset), -WEIDA_EINVAL);
        assert_eq!(weida_cursor_set_level(s, 1, &mut level, &mut at), 0);
    }
    assert_eq!(offset, 12);
    assert_eq!(entries(s), [(ACCEPTED, 12)]);
    assert!(next_set(c).is_null(), "the reporter finished");
    assert!(next_set(c).is_null(), "and the end stays the end");

    let answer = reply(q, None);
    let mut none = ptr::null_mut();
    // SAFETY: a live transfer and a writable out-parameter.
    assert_eq!(
        unsafe { weida_outgoing_transfer_cursors(answer, &mut none) },
        0
    );
    assert!(none.is_null(), "the reply ordered nothing");
    write_all(answer, b"ok");
    finish_body(answer);
    let mut recv = ptr::null_mut();
    let mut incoming = ptr::null_mut();
    let mut no_reporter = ptr::null_mut();
    // SAFETY: live handles, each consumed or dropped once (the recv consumes
    // the stream), and writable out-parameters.
    unsafe {
        ok(weida_reply_stream_recv(stream, &mut recv));
        ok(settle("the reply", |ms| {
            weida_reply_stream_recv_op_wait(recv, ms, &mut incoming)
        }));
        weida_reply_stream_recv_op_drop(recv);
        assert_eq!(
            weida_incoming_transfer_reporter(incoming, &mut no_reporter),
            0
        );
    }
    assert!(no_reporter.is_null());
    assert_eq!(read_all(incoming, 64).expect("the reply"), b"ok");
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_cursors_drop(c);
        weida_incoming_transfer_drop(t);
        weida_incoming_transfer_drop(incoming);
    }
    pair.close();
}

#[test]
fn final_only_reports_one_record_per_level_at_the_end() {
    let pair = Pair::new("abi-cursors-final-only");
    let too_many = report_meta(&(16..=32).collect::<Vec<u64>>(), WEIDA_REPORT_MODE_PROGRESS);
    assert_eq!(
        open_with(pair.req, too_many).err(),
        Some(-WEIDA_ELIMIT_EXCEEDED)
    );
    let stage = WEIDA_CURSOR_LEVEL_APPLICATION_FLOOR as u64 + 1;
    let m = report_meta(&[stage, ACCEPTED], WEIDA_REPORT_MODE_FINAL_ONLY);
    let (body, stream) = open_with(pair.req, m).expect("the exchange");
    // SAFETY: each metadata is dropped once.
    unsafe {
        weida_transfer_meta_drop(too_many);
        weida_transfer_meta_drop(m);
    }
    let c = cursors_of(body);
    write_all(body, b"0123456789");
    finish_body(body);

    let q = accept(pair.rep);
    assert_eq!(
        ordered(q),
        (vec![ACCEPTED, stage], WEIDA_REPORT_MODE_FINAL_ONLY, 1)
    );
    let t = take_body(q);
    let rep = reporter_of(t);
    let mut len = usize::MAX;
    // SAFETY: a live reporter and a writable length.
    unsafe {
        let levels = weida_reporter_levels(rep, &mut len);
        assert_eq!(levels_of(levels, len), [ACCEPTED, stage]);
        assert_eq!(weida_reporter_mode(rep), WEIDA_REPORT_MODE_FINAL_ONLY);
    }
    assert_eq!(read_all(t, 64).expect("the body"), b"0123456789");
    for offset in [5, 10] {
        for level in [ACCEPTED, stage] {
            assert_eq!(report(rep, level, offset), 0);
        }
    }
    let mut s = ptr::null_mut();
    // SAFETY: a live queue and a writable out-parameter.
    assert_eq!(
        unsafe { weida_cursors_recv(c, 200, &mut s) },
        -WEIDA_EAGAIN,
        "final-only writes nothing before finish"
    );

    assert_eq!(finish_reporter(rep), 0);
    let mut seen = std::collections::BTreeSet::new();
    let mut last = Vec::new();
    loop {
        let s = next_set(c);
        if s.is_null() {
            break;
        }
        last = entries(s);
        seen.extend(last.iter().copied());
    }
    assert_eq!(
        seen,
        std::collections::BTreeSet::from([(ACCEPTED, 10), (stage, 10)])
    );
    assert_eq!(last, [(ACCEPTED, 10), (stage, 10)]);
    // SAFETY: every handle is dropped once.
    unsafe {
        weida_reply_stream_drop(stream);
        weida_incoming_request_drop(q);
        weida_incoming_transfer_drop(t);
        weida_cursors_drop(c);
    }
    pair.close();
}
