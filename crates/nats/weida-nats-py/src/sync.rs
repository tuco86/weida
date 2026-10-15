//! `weida_nats.sync`: Core NATS for a Python program with no event loop.
//!
//! ```python
//! from weida_nats import sync
//!
//! nats = sync.connect("127.0.0.1", 4222)
//! orders = nats.subscribe("orders.>")
//! nats.publish("orders.created", b"{}")
//! print(orders.next_msg(2.0).payload)
//! reply = nats.request("service.echo", b"ping", 2.0)
//! nats.close()
//! ```
//!
//! # It implements nothing, and the `block_on` is here because it has to be
//!
//! `weida-zmq` has a `blocking` module of its own, so `weida_zmq.sync` is
//! that module with argument conversion around it. `weida-nats` has no such
//! facade — it is an asynchronous client and nothing else — so
//! the `block_on` lives here instead. That is the only difference. Every
//! method below drives **the same future the coroutine surface awaits**:
//! subject validation, `INFO.max_payload`, the inbox, the mandatory request
//! window, the 503-versus-timeout distinction, queue groups and every bound
//! are decided once, in `weida-nats`, and this module cannot disagree with
//! `weida_nats` because it does not implement them.
//!
//! # The calling thread drives, and the GIL is released while it does
//!
//! [`futures::executor::block_on`], not the reactor's `block_on`: the
//! reactor's threads are busy driving the connection, and a caller that
//! parked one of them would be waiting for the worker that has to drive what
//! it is waiting for. `weida-runtime`'s contract is that "the caller may
//! drive the returned futures on any executor —
//! `futures::executor::block_on` included", and this is that sentence used:
//! the operation's future is polled on the thread that asked, while the
//! socket and the timers it waits on run on the reactor the connection owns.
//!
//! The one thing [`drive`] does beyond polling is enter the runtime context
//! for the duration, which is not the same as running on it: `tokio::net`
//! and `tokio::time` look for the ambient runtime of the thread that
//! *creates* a socket or a timer, and `weida-nats` creates both inside the
//! future this drives.
//!
//! Each of those calls happens inside [`Python::detach`], so other Python
//! threads run while one is parked in `next_msg`. A blocking binding that
//! held the GIL would make a second thread pointless, and the usual shape of
//! a synchronous messaging program — one thread per subscription — would be
//! a program that runs one subscription at a time.
//!
//! No asyncio loop exists in such a process and none is needed: the reactor
//! belongs to the connection object.
//!
//! # What has no synchronous form
//!
//! `__aiter__` and `__anext__`, because they are asyncio's spelling of a
//! loop; a synchronous caller writes `while (message := sub.next_msg(1.0))`.
//! Everything else on `weida_nats.Connection` and
//! `weida_nats.Subscription` is here under the same name, minus the `await`.

use std::sync::Arc;

use futures::executor::block_on;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};
use weida_nats::{Connection, Error, Message, Subscription};
use weida_py_core::{Slot, interruptible, payload_of, py_bytes};

use crate::connection::Reactor;
use crate::errors::raise;
use crate::options;
use crate::subscription::next_of;
use crate::values::{PyMessage, PyRemoteInfo, PyState, headers_of, subject_of};

/// Drives one of the library's futures to completion on the **calling**
/// thread.
///
/// The runtime context is entered first and that is not the same as running
/// on the reactor: `weida-nats` builds its socket with `tokio::net` and its
/// timers with `tokio::time`, and both look for the ambient runtime of the
/// thread that *creates* them. Entering costs one thread-local write and
/// moves no work — the future is still polled here, and the reactor's own
/// threads still drive the I/O it waits on.
fn drive<F: Future>(reactor: &Reactor, future: F) -> F::Output {
    let _entered = reactor.exec().enter();
    block_on(future)
}

/// `weida_nats.sync.Connection`: one connection, driven by its caller.
#[pyclass(frozen, name = "Connection", module = "weida_nats.sync")]
pub struct SyncConnection {
    inner: Connection,
    reactor: Reactor,
}

/// `sync.connect("127.0.0.1", 4222)`: a connection that owns its reactor.
///
/// The same keywords as `weida_nats.connect`, read by the same function, so
/// the two surfaces cannot come to disagree about what an option means.
#[pyfunction]
#[pyo3(signature = (host, port, *, worker_threads=1, **options))]
pub fn connect(
    py: Python<'_>,
    host: &str,
    port: u16,
    worker_threads: usize,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<SyncConnection> {
    let configured = options::from_kwargs(py, options)?;
    let reactor = Reactor::owned(py, worker_threads)?;
    let connected = py.detach(|| {
        drive(
            &reactor,
            Connection::connect(reactor.exec(), host, port, configured),
        )
    });
    Ok(SyncConnection {
        inner: raise(py, connected)?,
        reactor,
    })
}

/// A second connection on `other`'s reactor: one thread pool, two
/// connections.
#[pyfunction]
#[pyo3(signature = (other, host, port, **options))]
pub fn connect_sharing(
    py: Python<'_>,
    other: &SyncConnection,
    host: &str,
    port: u16,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<SyncConnection> {
    let configured = options::from_kwargs(py, options)?;
    let reactor = other.reactor.clone();
    let connected = py.detach(|| {
        drive(
            &reactor,
            Connection::connect(reactor.exec(), host, port, configured),
        )
    });
    Ok(SyncConnection {
        inner: raise(py, connected)?,
        reactor,
    })
}

#[pymethods]
impl SyncConnection {
    /// The server's `INFO`, as it now stands.
    fn info(&self) -> PyRemoteInfo {
        PyRemoteInfo::of(self.inner.info())
    }

    /// `INFO.max_payload`: the largest publication this server accepts. The
    /// check against it is the library's.
    fn max_payload(&self) -> u64 {
        self.inner.max_payload()
    }

    /// Whether headers were negotiated, which is what `HPUB` needs.
    fn headers_supported(&self) -> bool {
        self.inner.headers_supported()
    }

    /// Whether the server has said it is draining (`INFO.ldm`).
    fn is_lame_duck(&self) -> bool {
        self.inner.is_lame_duck()
    }

    /// Waits for the server to enter Lame Duck Mode: `True` for the notice,
    /// `False` when the connection ended first.
    ///
    /// Unbounded, and finite all the same: a connection that never drains
    /// ends, and its end is the other answer.
    fn lame_duck_notice(&self, py: Python<'_>) -> PyResult<bool> {
        let connection = self.inner.clone();
        interruptible(py, self.reactor.exec(), move || async move {
            connection.lame_duck_notice().await
        })
    }

    /// Where this connection is, now.
    fn state(&self) -> PyState {
        PyState::of(self.inner.state())
    }

    /// Waits for the connection to stop being usable, and says why.
    fn closed(&self, py: Python<'_>) -> PyResult<PyState> {
        let connection = self.inner.clone();
        let state = interruptible(py, self.reactor.exec(), move || async move {
            connection.closed().await
        })?;
        Ok(PyState::of(state))
    }

    /// `PUB <subject> <#bytes>`. Queued for the driver, which is all Core
    /// NATS offers: there is no publish acknowledgement.
    fn publish(
        &self,
        py: Python<'_>,
        subject: &Bound<'_, PyAny>,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let subject = subject_of(subject)?;
        let payload = payload_of(payload)?;
        let published = py.detach(|| drive(&self.reactor, self.inner.publish(subject, payload)));
        raise(py, published)
    }

    /// `PUB` or `HPUB`, with an optional reply subject and header block.
    #[pyo3(signature = (subject, *, reply_to=None, headers=None, payload=None))]
    fn publish_with(
        &self,
        py: Python<'_>,
        subject: &Bound<'_, PyAny>,
        reply_to: Option<&Bound<'_, PyAny>>,
        headers: Option<&Bound<'_, PyAny>>,
        payload: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let subject = subject_of(subject)?;
        let reply_to = reply_to.map(subject_of).transpose()?;
        let headers = headers.map(headers_of).transpose()?;
        let payload = payload.map(payload_of).transpose()?.unwrap_or_default();
        let published = py.detach(|| {
            drive(
                &self.reactor,
                self.inner
                    .publish_with(subject, reply_to.as_deref(), headers.as_ref(), payload),
            )
        });
        raise(py, published)
    }

    /// `SUB <subject> <sid>`: one copy of every matching publication.
    fn subscribe(&self, py: Python<'_>, subject: &Bound<'_, PyAny>) -> PyResult<SyncSubscription> {
        let subject = subject_of(subject)?;
        let subscribed = py.detach(|| drive(&self.reactor, self.inner.subscribe(subject)));
        Ok(SyncSubscription::of(
            raise(py, subscribed)?,
            self.reactor.clone(),
        ))
    }

    /// `SUB <subject> <queue group> <sid>`: one copy per publication between
    /// all members of the group.
    fn subscribe_with_queue_group(
        &self,
        py: Python<'_>,
        subject: &Bound<'_, PyAny>,
        queue_group: &Bound<'_, PyAny>,
    ) -> PyResult<SyncSubscription> {
        let subject = subject_of(subject)?;
        let queue_group = subject_of(queue_group)?;
        let subscribed = py.detach(|| {
            drive(
                &self.reactor,
                self.inner.subscribe_with_queue_group(subject, queue_group),
            )
        });
        Ok(SyncSubscription::of(
            raise(py, subscribed)?,
            self.reactor.clone(),
        ))
    }

    /// Publishes a request and waits at most `timeout` seconds for one
    /// reply.
    ///
    /// The request-reply of the asyncio surface, unchanged, in synchronous
    /// form: the same three outcomes and the same three classes, because it
    /// is the same call.
    fn request(
        &self,
        py: Python<'_>,
        subject: &Bound<'_, PyAny>,
        payload: &Bound<'_, PyAny>,
        timeout: f64,
    ) -> PyResult<PyMessage> {
        let subject = subject_of(subject)?;
        let payload = payload_of(payload)?;
        let window = options::window(py, "timeout", timeout)?;
        let connection = self.inner.clone();
        let replied = interruptible(py, self.reactor.exec(), move || async move {
            connection.request(subject, payload, window).await
        })?;
        raise(py, replied).map(PyMessage::of)
    }

    /// Publishes a request and collects every reply that arrives inside
    /// `timeout` seconds, up to `max_replies`.
    fn request_many(
        &self,
        py: Python<'_>,
        subject: &Bound<'_, PyAny>,
        payload: &Bound<'_, PyAny>,
        timeout: f64,
        max_replies: usize,
    ) -> PyResult<Vec<PyMessage>> {
        let subject = subject_of(subject)?;
        let payload = payload_of(payload)?;
        let window = options::window(py, "timeout", timeout)?;
        let max_replies = options::bounded(py, "max_replies", max_replies)?;
        let connection = self.inner.clone();
        let replies = interruptible(py, self.reactor.exec(), move || async move {
            connection
                .request_many(subject, payload, window, max_replies)
                .await
        })?;
        raise(py, replies).map(|replies| replies.into_iter().map(PyMessage::of).collect())
    }

    /// `PING`, answered when the `PONG` comes back: the only way to learn
    /// that the server has read what was written before it.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let connection = self.inner.clone();
        let flushed = interruptible(py, self.reactor.exec(), move || async move {
            connection.flush().await
        })?;
        raise(py, flushed)
    }

    /// Ends the connection. Idempotent.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let closed = py.detach(|| drive(&self.reactor, self.inner.close()));
        raise(py, closed)
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nats.sync.Connection {} max_payload={}>",
            match self.inner.state() {
                weida_nats::State::Connected => "connected".to_owned(),
                weida_nats::State::Closed => "closed".to_owned(),
                weida_nats::State::Failed(why) => format!("failed: {why}"),
            },
            self.inner.max_payload()
        )
    }
}

/// `weida_nats.sync.Subscription`.
#[pyclass(frozen, name = "Subscription", module = "weida_nats.sync")]
pub struct SyncSubscription {
    /// A [`Slot`] rather than a mutex: a receive waits inside a future that
    /// Ctrl-C may drop, and a lock guard cannot travel into that future. One
    /// thread reads at a time; a second waits for the first with the GIL
    /// released. Sync reads never restore, because a future that Ctrl-C drops
    /// has not produced its message yet.
    held: Arc<Slot<Subscription, Message>>,
    subject: Vec<u8>,
    queue_group: Option<Vec<u8>>,
    sid: u64,
    /// The connection's reactor, held for the same reason the asynchronous
    /// subscription holds one: messages must keep arriving after the
    /// connection object goes out of scope.
    reactor: Reactor,
}

impl SyncSubscription {
    /// Wraps a live subscription.
    fn of(subscription: Subscription, reactor: Reactor) -> SyncSubscription {
        SyncSubscription {
            subject: subscription.subject().to_vec(),
            queue_group: subscription.queue_group().map(<[u8]>::to_vec),
            sid: subscription.sid(),
            held: Slot::new(subscription),
            reactor,
        }
    }
}

#[pymethods]
impl SyncSubscription {
    /// The subject or pattern this subscription asked for, as octets.
    fn subject<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        py_bytes(py, &self.subject)
    }

    /// The queue group this subscription joined, where it joined one.
    fn queue_group<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.queue_group.as_deref().map(|group| py_bytes(py, group))
    }

    /// The `sid` this client chose.
    fn sid(&self) -> u64 {
        self.sid
    }

    /// The next message, waiting at most `timeout` seconds for one.
    ///
    /// `None` once the subscription has ended — unsubscribed, its
    /// auto-unsubscribe count reached, or its connection gone — and
    /// `weida_nats.RequestTimeout` when the window closed with the
    /// subscription still live. The two are different facts and a caller
    /// that retries needs to tell them apart, which is why the end is a
    /// value and the timeout is not.
    ///
    /// The timeout is mandatory for the reason the library makes a request's
    /// window mandatory: a blocking drain that can hang forever is the
    /// failure this argument exists to prevent.
    fn next_msg(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<PyMessage>> {
        let window = options::window(py, "timeout", timeout)?;
        let slot = Arc::clone(&self.held);
        let within = self.reactor.exec().clone();
        // A Ctrl-C drops this future where it stands, which is what cancelling
        // the coroutine does to the same receive; see `weida_py_core::interruptible`.
        let received = interruptible(py, self.reactor.exec(), move || async move {
            within
                .within(window, async move { next_of(&slot).await })
                .await
        })?;
        match received {
            Some(message) => Ok(message.map(PyMessage::of)),
            // The library's own error value rather than an invented one, so that
            // `next_msg` and `request` raise one class for one fact: the caller's
            // window elapsed.
            None => raise(py, Err(Error::RequestTimeout { after: window })),
        }
    }

    /// The next message if one is already queued, and `None` if not.
    ///
    /// It waits only while another thread is in `next_msg` on this subscription,
    /// never for a message.
    fn try_next(&self, py: Python<'_>) -> Option<PyMessage> {
        py.detach(|| block_on(async { self.held.acquire().await.try_next() }))
            .map(PyMessage::of)
    }

    /// `UNSUB <sid>`: removes the subscription now. Messages the server had
    /// already written are still readable.
    fn unsubscribe(&self, py: Python<'_>) {
        py.detach(|| {
            drive(&self.reactor, async {
                self.held.acquire().await.unsubscribe().await
            })
        });
    }

    /// `UNSUB <sid> <max_msgs>`: removes the subscription once it has
    /// received `max_msgs` messages in total.
    fn unsubscribe_after(&self, py: Python<'_>, max_msgs: u64) {
        py.detach(|| {
            drive(&self.reactor, async {
                self.held.acquire().await.unsubscribe_after(max_msgs).await
            })
        });
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nats.sync.Subscription {:?} sid={}{}>",
            String::from_utf8_lossy(&self.subject),
            self.sid,
            self.queue_group
                .as_deref()
                .map_or_else(String::new, |group| {
                    format!(" queue_group={:?}", String::from_utf8_lossy(group))
                })
        )
    }
}

/// Builds `weida_nats.sync` and registers it so that `import
/// weida_nats.sync` works as well as `from weida_nats import sync`.
pub fn install(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let sync = PyModule::new(py, "sync")?;
    sync.add_class::<SyncConnection>()?;
    sync.add_class::<SyncSubscription>()?;
    sync.add_function(pyo3::wrap_pyfunction!(connect, &sync)?)?;
    sync.add_function(pyo3::wrap_pyfunction!(connect_sharing, &sync)?)?;
    parent.add("sync", &sync)?;
    // A submodule built in Rust is an attribute of its parent but not an
    // entry in `sys.modules`, and `import weida_nats.sync` reads the latter.
    py.import("sys")?
        .getattr("modules")?
        .set_item("weida_nats.sync", &sync)?;
    Ok(())
}
