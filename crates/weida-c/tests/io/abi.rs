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
