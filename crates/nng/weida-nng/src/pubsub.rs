//! PUB and SUB: broadcast, and a filter that runs at the receiver.
//!
//! "PUB broadcasts every message to every connected SUB; each SUB filters
//! locally by prefix subscription, so subscriptions do not reduce link
//! bandwidth" (`docs/research/nanomsg-nng.md` §4). Those two clauses are
//! the whole pattern, and the second one is the expensive one.
//!
//! **Where the filter is, and what it costs.** "A SUB socket that has
//! subscribed to nothing is still sent every publication and discards each
//! one after inspecting its prefix, and a SUB that subscribes to one prefix
//! receives the others too. A publisher's egress is therefore the message
//! size times the number of connected subscribers regardless of what they
//! asked for, and a subscription is a receiver-side cost saving only" (§4).
//! [`SubSocket::discarded`] is that sentence made countable: it is the
//! number of publications this socket was sent and threw away, and a test
//! asserts it is not zero.
//!
//! **The publisher tests nothing.** [`PubSocket::send`] offers a copy to
//! every pipe without looking at any subscription, because there is nothing
//! to look at: a subscription never leaves the subscriber, and no frame,
//! field or command carries one (§3, §4).
//!
//! **A subscription is a byte prefix of the body**, not a topic field:
//! "PUB/SUB uses the initial bytes of the body as a topic; they are neither
//! a separate wire field nor typed metadata" (§3). An empty subscription
//! admits everything (§4).
//!
//! **A full subscriber queue drops, and which end is a choice.**
//! "`SUB_PREFNEW=true`" — the default — "drops the oldest message by
//! default … or rejects the new one when false" (§4). The choice is
//! [`SocketOptions::sub_prefer_new`](crate::SocketOptions::sub_prefer_new),
//! and it applies to the queue **after** the filter, because a queue that
//! dropped a subscribed message to make room for an unsubscribed one would
//! be applying the policy to traffic the application never asked for.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use tokio::task::JoinHandle;
use weida_sp::EndpointType;

use crate::context::Context;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::{FullAction, Queue, QueueConfig};
use crate::socket::{Broadcast, SocketCore, socket_endpoints, within};

/// A PUB socket: send only, to every connected subscriber.
#[derive(Clone, Debug)]
pub struct PubSocket {
    core: Arc<SocketCore>,
}

impl PubSocket {
    /// A PUB socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<PubSocket> {
        PubSocket::with_options(context, SocketOptions::default())
    }

    /// A PUB socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<PubSocket> {
        Ok(PubSocket {
            core: Arc::new(SocketCore::new(context, EndpointType::Pub, options)?),
        })
    }

    /// Offers `body` to every connected subscriber.
    ///
    /// Never waits and never fails for a subscriber that cannot take its
    /// copy: PUB "broadcasts best-effort" (§5) and a lost copy is the
    /// subscriber's loss with no receipt anywhere (§4). What comes back is
    /// how many copies were queued and how many were dropped, which is the
    /// only trace there will be.
    ///
    /// Fails with `NNG_ECLOSED` once the socket is closed.
    pub fn send(&self, body: impl Into<Vec<u8>>) -> Result<Broadcast> {
        self.core.ensure_open()?;
        let body: Vec<u8> = body.into();
        Ok(self
            .core
            .send_to_all_except(&Message::from_body(body), None))
    }
}

socket_endpoints!(PubSocket);

/// The prefixes one SUB socket admits.
///
/// A list rather than a tree: "SUB topics are arbitrary-size byte arrays
/// and are maintained locally; the manual gives no subscription-count
/// limit" (§11), and nothing a peer sends can add one — a subscription
/// never crosses the wire, so this structure has no remote input to bound
/// (`docs/INVARIANTS.md`).
#[derive(Debug, Default)]
struct Subscriptions {
    prefixes: Vec<Vec<u8>>,
}

impl Subscriptions {
    /// Whether `body` matches any subscription. An empty prefix admits
    /// everything, which is what an empty subscription means (§4).
    fn admits(&self, body: &[u8]) -> bool {
        self.prefixes
            .iter()
            .any(|prefix| body.starts_with(prefix.as_slice()))
    }
}

/// A SUB socket: receive only, filtered locally by byte prefix.
#[derive(Clone, Debug)]
pub struct SubSocket {
    core: Arc<SocketCore>,
    shared: Arc<SubShared>,
}

#[derive(Debug)]
struct SubShared {
    subscriptions: std::sync::Mutex<Subscriptions>,
    /// The queue the application reads from: filtered messages only, with
    /// `SUB_PREFNEW`'s policy at its bound.
    admitted: Queue,
    /// Publications received and thrown away because no subscription
    /// matched. Counted because it is the cost of receiver-side filtering,
    /// and a number nobody can see is a cost nobody can measure.
    discarded: AtomicU64,
    pump: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for SubShared {
    fn drop(&mut self) {
        if let Some(pump) = self.pump.lock().expect("sub pump poisoned").take() {
            pump.abort();
        }
    }
}

impl SubSocket {
    /// A SUB socket on `context`, with NNG's defaults — which subscribe to
    /// nothing, so nothing is delivered until a prefix is added.
    pub fn new(context: &Context) -> Result<SubSocket> {
        SubSocket::with_options(context, SocketOptions::default())
    }

    /// A SUB socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<SubSocket> {
        let prefer_new = options.sub_prefer_new;
        let depth = options.recv_depth.unwrap_or(crate::DEFAULT_RECV_DEPTH);
        let core = Arc::new(SocketCore::new(context, EndpointType::Sub, options)?);
        let shared = Arc::new(SubShared {
            subscriptions: std::sync::Mutex::new(Subscriptions::default()),
            admitted: Queue::new(QueueConfig {
                depth,
                max_bytes: crate::DEFAULT_QUEUE_BYTES,
                full: if prefer_new {
                    FullAction::DropOldest
                } else {
                    FullAction::RejectNewest
                },
            }),
            discarded: AtomicU64::new(0),
            pump: std::sync::Mutex::new(None),
        });
        // The pump holds the socket weakly, for the reason the resend clock
        // of REQ does: a pump that owned it would keep a socket nobody can
        // reach alive for as long as it ran.
        let task = core.exec().spawn(filter_loop(
            Arc::downgrade(&core),
            Arc::downgrade(&shared),
            core.engine().clone(),
        ));
        *shared.pump.lock().expect("sub pump poisoned") = Some(task);
        Ok(SubSocket { core, shared })
    }

    /// `NNG_OPT_SUB_SUBSCRIBE`: admit every publication whose body begins
    /// with `prefix`.
    ///
    /// An empty prefix admits everything. Several subscriptions may be held
    /// at once and a message matching any of them is admitted. Subscribing
    /// to a prefix already held changes nothing and is not an error, which
    /// is NNG's behaviour and deliberately **not** ZMTP's reference
    /// counting — SP has no subscription on the wire to count.
    pub fn subscribe(&self, prefix: impl Into<Vec<u8>>) {
        let prefix = prefix.into();
        let mut subscriptions = self.shared.subscriptions.lock().expect("subs poisoned");
        if !subscriptions.prefixes.contains(&prefix) {
            subscriptions.prefixes.push(prefix);
        }
    }

    /// `NNG_OPT_SUB_UNSUBSCRIBE`: stop admitting `prefix`.
    ///
    /// `NNG_ENOENT` for a prefix this socket does not hold, which is what
    /// NNG reports for unsubscribing from something never subscribed to.
    pub fn unsubscribe(&self, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        let mut subscriptions = self.shared.subscriptions.lock().expect("subs poisoned");
        match subscriptions
            .prefixes
            .iter()
            .position(|held| held.as_slice() == prefix)
        {
            Some(at) => {
                subscriptions.prefixes.remove(at);
                Ok(())
            }
            None => Err(Error::ENOENT(
                "this socket holds no such subscription".into(),
            )),
        }
    }

    /// The prefixes this socket currently admits.
    pub fn subscriptions(&self) -> Vec<Vec<u8>> {
        self.shared
            .subscriptions
            .lock()
            .expect("subs poisoned")
            .prefixes
            .clone()
    }

    /// Publications this socket was sent and threw away because no
    /// subscription matched.
    ///
    /// The measurable form of "a subscription is a receiver-side cost
    /// saving only" (§4): a publisher sends everything to everybody, and
    /// this counts what that cost this subscriber.
    pub fn discarded(&self) -> u64 {
        self.shared.discarded.load(Ordering::Relaxed)
    }

    /// Receives the next publication that matched a subscription.
    ///
    /// Bounded by `NNG_OPT_RECVTIMEO`.
    pub async fn recv(&self) -> Result<Message> {
        let limit = self.core.options().recv_timeout;
        within(self.core.exec(), limit, self.shared.admitted.recv()).await
    }

    /// The non-blocking form: `NNG_EAGAIN` when nothing matching has
    /// arrived.
    pub fn try_recv(&self) -> Result<Message> {
        self.shared.admitted.try_recv()
    }
}

socket_endpoints!(SubSocket);

/// Takes everything the publishers send, tests it against the
/// subscriptions, and admits what matches.
///
/// This is the locus the sheet describes: the message has already crossed
/// the link and been read before anything is tested, so the filter saves
/// the application work and saves the network nothing (§4).
async fn filter_loop(core: Weak<SocketCore>, shared: Weak<SubShared>, engine: Engine) {
    loop {
        // Created before the look, so an arrival while the pump is taking
        // what is there ends the wait instead of being missed by it.
        let arrived = engine.signals().arrived();
        {
            let (Some(core), Some(shared)) = (core.upgrade(), shared.upgrade()) else {
                return;
            };
            while let Some((_, message)) = core.try_take_any() {
                let admits = shared
                    .subscriptions
                    .lock()
                    .expect("subs poisoned")
                    .admits(message.body());
                if !admits {
                    shared.discarded.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if shared.admitted.send(message).await.is_err() {
                    return;
                }
            }
            if engine.is_closed() {
                shared.admitted.close();
                return;
            }
        }
        arrived.await;
    }
}
