//! Transparent redial: the policy a dialling endpoint redials under, and the
//! events it reports while doing so
//! ([decisions/0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)).
//!
//! A dialled address outlives its connection. The slot [`crate::Peer`]
//! records for it is redialled by a runtime task under a [`ReconnectPolicy`],
//! and every transition of that slot is reported as a [`PeerEvent`] on the
//! endpoint's event stream. Nothing in here touches a payload: what a redial
//! restores is the transport and, for a subscriber, the filters it holds
//! itself; the peer kept nothing ([0008](../../../docs/decisions/0008-session-identity.md)
//! §4.5).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use weida_core::{Fingerprint, LossCause, PeerIdentity};

/// How a dialling endpoint redials an address it has lost.
///
/// ZeroMQ's `ZMQ_RECONNECT_IVL` family with better defaults: exponential
/// rather than libzmq's constant interval, jittered so a fleet that lost one
/// server does not redial it in lockstep, and bounded by `max` rather than
/// growing without limit.
///
/// Two losses never redial whatever the policy says: a connection this side
/// closed (`Runtime::shutdown`, a dropped runtime), because the application
/// already decided; and a redial that reached a peer with a different key,
/// because that is not the peer the address named (0031 §4.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconnectPolicy {
    /// Delay before the first redial. Doubles on every failed attempt, and on
    /// every connection that did not stay up (see [`ReconnectPolicy::max`]).
    pub initial: Duration,
    /// Ceiling on the delay between attempts.
    ///
    /// It is also what "stayed up" means: a connection that lived at least
    /// this long earns a fresh schedule, and one that closed sooner — a server
    /// that accepts, says HELLO and hangs up, or a crash loop — continues the
    /// schedule where it stopped. Such a peer is redialled ever more slowly
    /// instead of at [`ReconnectPolicy::initial`] for as long as it keeps
    /// doing it.
    pub max: Duration,
    /// Whether each delay is drawn uniformly from the upper half of its
    /// computed value rather than used exactly.
    pub jitter: bool,
    /// Attempts since the last connection that stayed up (see
    /// [`ReconnectPolicy::max`]) before the slot is given up; `None` retries
    /// for as long as the endpoint lives.
    pub max_attempts: Option<u32>,
    /// Give the slot up when the peer closed the connection deliberately
    /// (`LossCause::PeerClosed`) rather than redialling it: ZeroMQ's
    /// `ZMQ_RECONNECT_STOP_AFTER_DISCONNECT`. A restart that says so
    /// (`LossCause::PeerRestarting`, sent for a binding handed over with
    /// `Binding::hand_over`) is redialled regardless. Off by default,
    /// because a server that restarts by closing and binding again still
    /// looks like a deliberate close.
    pub stop_on_peer_closed: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        ReconnectPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            jitter: true,
            max_attempts: None,
            stop_on_peer_closed: false,
        }
    }
}

impl ReconnectPolicy {
    /// No redial at all: the behaviour before 0031. A lost peer is reported
    /// as `GaveUp` at once and the next operation on it fails with the
    /// `LossCause`.
    #[must_use]
    pub fn never() -> Self {
        ReconnectPolicy {
            max_attempts: Some(0),
            ..ReconnectPolicy::default()
        }
    }

    /// Whether a lost slot is redialled at all under this policy.
    #[must_use]
    pub fn redials(&self) -> bool {
        self.max_attempts != Some(0)
    }

    /// Whether a loss with `cause` is redialled at all under this policy.
    ///
    /// A connection this side closed never is: the application, or its
    /// runtime's shutdown, already decided.
    pub(crate) fn redials_after(&self, cause: LossCause) -> bool {
        self.redials()
            && cause != LossCause::LocallyClosed
            && !(cause == LossCause::PeerClosed && self.stop_on_peer_closed)
    }

    /// Whether `attempt` (1-based) is still allowed.
    pub(crate) fn allows(&self, attempt: u32) -> bool {
        self.max_attempts.is_none_or(|max| attempt <= max)
    }

    /// The delay before `attempt` (1-based): `initial * 2^(attempt-1)`, capped
    /// at `max`, and with `jitter` drawn from the upper half of that.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        let doublings = attempt.saturating_sub(1).min(32);
        let base = self
            .initial
            .checked_mul(1u32 << doublings.min(31))
            .unwrap_or(self.max)
            .min(self.max);
        if !self.jitter || base.is_zero() {
            return base;
        }
        let half = base / 2;
        half + rand::random_range(Duration::ZERO..=half)
    }

    /// The delay before `attempt` (1-based) after a loss with `cause`. A
    /// restart skips the first wait and only that one: the successor is
    /// expected to serve already, and if it does not, the schedule continues
    /// from its own first step.
    pub(crate) fn delay_after(&self, cause: LossCause, attempt: u32) -> Duration {
        match cause {
            LossCause::PeerRestarting if attempt <= 1 => Duration::ZERO,
            LossCause::PeerRestarting => self.delay(attempt - 1),
            _ => self.delay(attempt),
        }
    }
}

/// What a `send` does when the outbox is at its bound
/// ([`crate::RuntimeConfig::outbox_messages`]).
///
/// The names are the backpressure vocabulary of `docs/GUARANTEES.md` §3,
/// but the setting is the sender's alone: an outbox is sender-local state,
/// and what a puller declares has no bearing on how its pusher waits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutboxFull {
    /// Wait for room: ZeroMQ's `PUSH` at its high-water mark.
    #[default]
    Block,
    /// Discard the body and count it in `dropped()`.
    Drop,
    /// Fail the send with `Error::LimitExceeded`.
    Reject,
}

/// Why a slot stopped redialling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GiveUp {
    /// The policy ran out: `max_attempts` reached, `stop_on_peer_closed`
    /// matched, or the connection was closed by this side.
    Policy {
        /// Redials attempted after the loss before giving up.
        attempts: u32,
    },
    /// The redial reached a peer that proved a different key than the one
    /// the slot proved first — a different server behind the same address,
    /// which is not a reconnect (0031 §4.7). Carries what answered, or
    /// `None` when it proved nothing.
    PeerChanged {
        /// The fingerprint the replacement presented.
        presented: Option<Fingerprint>,
    },
    /// The redial connected but could not be used: negotiation or the TLS
    /// handshake failed, or the endpoint's own registration on the new
    /// connection was refused. Redialling again would not change it.
    Failed(String),
}

/// One transition of one dialled address, reported on
/// [`PeerEvents`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerEvent {
    /// The address has a live connection: after `connect`, and after every
    /// successful redial.
    Connected {
        /// The address as it was dialled.
        url: Arc<str>,
        /// What the peer proved, exactly as `IncomingMeta::peer` reports it.
        peer: Option<PeerIdentity>,
    },
    /// The connection is gone, and why. The slot is down until `Connected`
    /// or `GaveUp` follows.
    Lost {
        /// The address as it was dialled.
        url: Arc<str>,
        /// Why, as the next operation would have reported it.
        cause: LossCause,
    },
    /// A redial is scheduled.
    Retrying {
        /// The address as it was dialled.
        url: Arc<str>,
        /// Which attempt since the last connection that stayed up, from 1.
        attempt: u32,
        /// How long the runtime waits before it. On an in-process address
        /// this is the policy's number, but the wait ends as soon as the bus
        /// is bound again.
        delay: Duration,
    },
    /// The slot will not be redialled again. The next operation that would
    /// have used it fails with the loss cause.
    GaveUp {
        /// The address as it was dialled.
        url: Arc<str>,
        /// Why.
        why: GiveUp,
    },
    /// The reader fell behind by this many events, which were discarded.
    ///
    /// The stream is bounded, so a reader that does not keep up learns how
    /// much it missed rather than growing a queue.
    Missed(u64),
}

/// Depth of an endpoint's event stream: how many events a reader may fall
/// behind before it starts missing them.
pub(crate) const EVENT_QUEUE: usize = 64;

/// The event stream of one dialling endpoint.
///
/// Obtained from the endpoint's `events()`; each call gives an independent
/// reader that sees every event from that moment on. An endpoint with no
/// reader pays nothing for its events.
pub struct PeerEvents {
    rx: broadcast::Receiver<PeerEvent>,
}

impl PeerEvents {
    pub(crate) fn new(rx: broadcast::Receiver<PeerEvent>) -> PeerEvents {
        PeerEvents { rx }
    }

    /// The next event, or `None` once the endpoint is gone.
    pub async fn recv(&mut self) -> Option<PeerEvent> {
        match self.rx.recv().await {
            Ok(event) => Some(event),
            Err(broadcast::error::RecvError::Lagged(n)) => Some(PeerEvent::Missed(n)),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

impl std::fmt::Debug for PeerEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerEvents").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_delay_doubles_and_is_capped() {
        let policy = ReconnectPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(350),
            jitter: false,
            ..ReconnectPolicy::default()
        };
        assert_eq!(policy.delay(1), Duration::from_millis(100));
        assert_eq!(policy.delay(2), Duration::from_millis(200));
        assert_eq!(policy.delay(3), Duration::from_millis(350));
        assert_eq!(policy.delay(40), Duration::from_millis(350));
    }

    #[test]
    fn jitter_stays_in_the_upper_half() {
        let policy = ReconnectPolicy {
            initial: Duration::from_millis(100),
            ..ReconnectPolicy::default()
        };
        for _ in 0..1000 {
            let d = policy.delay(2);
            assert!(
                d >= Duration::from_millis(100) && d <= Duration::from_millis(200),
                "{d:?}"
            );
        }
    }

    #[test]
    fn never_means_no_attempt() {
        assert!(!ReconnectPolicy::never().redials());
        assert!(!ReconnectPolicy::never().allows(1));
        assert!(ReconnectPolicy::default().allows(1_000_000));
        assert!(
            ReconnectPolicy {
                max_attempts: Some(3),
                ..ReconnectPolicy::default()
            }
            .allows(3)
        );
        assert!(
            !ReconnectPolicy {
                max_attempts: Some(3),
                ..ReconnectPolicy::default()
            }
            .allows(4)
        );
    }

    #[test]
    fn a_restart_skips_the_first_wait_and_only_that() {
        let policy = ReconnectPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(1),
            jitter: false,
            ..ReconnectPolicy::default()
        };
        assert_eq!(
            policy.delay_after(LossCause::PeerRestarting, 1),
            Duration::ZERO
        );
        assert_eq!(
            policy.delay_after(LossCause::PeerRestarting, 2),
            Duration::from_millis(100)
        );
        assert_eq!(
            policy.delay_after(LossCause::PeerRestarting, 3),
            Duration::from_millis(200)
        );
        assert_eq!(
            policy.delay_after(LossCause::PeerClosed, 1),
            Duration::from_millis(100)
        );

        // A restart is redialled even where a deliberate close is not, and
        // a policy that never redials still does not.
        let stopping = ReconnectPolicy {
            stop_on_peer_closed: true,
            ..ReconnectPolicy::default()
        };
        assert!(stopping.redials_after(LossCause::PeerRestarting));
        assert!(!stopping.redials_after(LossCause::PeerClosed));
        assert!(!ReconnectPolicy::never().redials_after(LossCause::PeerRestarting));
    }
}
