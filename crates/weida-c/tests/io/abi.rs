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
