//! PUSH and PULL: the pipeline, 30/PIPELINE.
//!
//! "Intended for task distribution, typically in a multi-stage pipeline where
//! one or a few nodes push work to many workers, and they in turn push
//! results to one or a few collectors. The pattern is mostly reliable insofar
//! as it will not discard messages unless a node disconnects unexpectedly"
//! (`docs/research/zeromq.md` §4.4).
//!
//! - **PUSH** keeps one outgoing queue per peer, treats a peer as available
//!   "only when its queue is not full", round-robins over the available
//!   ones, blocks or errors when none is, and **never discards**. That last
//!   clause is what makes the pattern "mostly reliable", and it is why
//!   `zmq_socket(3)`'s mute row for PUSH is *block*.
//! - **PULL** fair-queues its peers and receives only.
//!
//! **The slow joiner is the pattern's, not ours.** "The first PULL socket to
//! connect will grab an unfair share of messages. The accurate rotation of
//! messages only happens when all PULL sockets are successfully connected"
//! (§4.4) — so an even split is a property of a steady state, and a test
//! that wants one waits for the peers to arrive first.

use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::MuteAction;
use crate::socket::{SocketCore, socket_endpoints};

/// A PUSH socket: send only, round-robin, never discarding.
#[derive(Debug)]
pub struct PushSocket {
    core: SocketCore,
}

impl PushSocket {
    /// A PUSH socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<PushSocket> {
        PushSocket::with_options(context, SocketOptions::default())
    }

    /// A PUSH socket with `options`.
    ///
    /// The mute action is `zmq_socket(3)`'s row for PUSH — block — and is not
    /// a setting: "never discard" is the sentence that makes the pipeline
    /// reliable, and a PUSH that dropped would be a PUB with the wrong name.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<PushSocket> {
        options.pipe.outgoing.mute = MuteAction::Block;
        Ok(PushSocket {
            core: SocketCore::new(context, SocketType::Push, options)?,
        })
    }

    /// Sends a message to the next available peer.
    ///
    /// Round-robin over the peers whose queue has room. Blocks when every
    /// queue is full **and** when there is no peer at all — "block or error
    /// when none" — bounded by `ZMQ_SNDTIMEO`, and never discards.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        let limit = self.core.options().send_timeout;
        let exec = self.core.exec().clone();
        let message = message.into();
        let delivered = match limit {
            None => self.core.send_round_robin(message).await?,
            Some(limit) => match exec
                .within(limit, self.core.send_round_robin(message))
                .await
            {
                Some(result) => result?,
                None => {
                    return Err(Error::EAGAIN(
                        format!("no worker took the task within {limit:?} (ZMQ_SNDTIMEO)").into(),
                    ));
                }
            },
        };
        if delivered.peer.is_none() {
            return Err(Error::EAGAIN(
                "no worker took the task; a PUSH socket never discards one".into(),
            ));
        }
        Ok(())
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` rather than a wait. The message is
    /// still not discarded — the caller keeps it.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// Sends under an explicit wall-clock bound.
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        let exec = self.core.exec().clone();
        match exec
            .within(limit, self.core.send_round_robin(message.into()))
            .await
        {
            Some(result) => result.map(|_| ()),
            None => Err(Error::EAGAIN(
                format!("no worker took the task within {limit:?}").into(),
            )),
        }
    }
}

socket_endpoints!(PushSocket);

/// A PULL socket: receive only, fair-queued.
#[derive(Debug)]
pub struct PullSocket {
    core: SocketCore,
}

impl PullSocket {
    /// A PULL socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<PullSocket> {
        PullSocket::with_options(context, SocketOptions::default())
    }

    /// A PULL socket with `options`.
    ///
    /// PULL cannot send, so there is no outgoing mute action to choose;
    /// `zmq_socket(3)` gives PULL the block row, which is the receive side's
    /// backpressure — a PULL that stops reading stops its PUSH.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<PullSocket> {
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(PullSocket {
            core: SocketCore::new(context, SocketType::Pull, options)?,
        })
    }

    /// Receives the next task from any peer, fair-queued, bounded by
    /// `ZMQ_RCVTIMEO`.
    pub async fn recv(&mut self) -> Result<Multipart> {
        let limit = self.core.options().recv_timeout;
        let exec = self.core.exec().clone();
        match limit {
            None => self.core.recv_fair().await.map(|(_, message)| message),
            Some(limit) => match exec.within(limit, self.core.recv_fair()).await {
                Some(result) => result.map(|(_, message)| message),
                None => Err(Error::EAGAIN(
                    format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.core.recv_fair()).await {
            Some(result) => result.map(|(_, message)| message),
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?}").into(),
            )),
        }
    }
}

socket_endpoints!(PullSocket);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn text(message: &Multipart) -> String {
        String::from_utf8_lossy(message.frames()[0].as_slice()).into_owned()
    }

    async fn wait_for(done: impl FnMut() -> bool) {
        weida_testkit::until("the condition", weida_testkit::EXPECTED, done).await;
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: PUSH rotates over its workers, so nine tasks and three workers
        /// are three each — the load balancing the pattern is for. The wait for
        /// all three to arrive is the pattern's own slow joiner, not a fudge:
        /// "the accurate rotation of messages only happens when all PULL sockets
        /// are successfully connected".
        #[tokio::test]
        async fn push_round_robins_over_its_workers() {
            let ctx = context();
            let mut ventilator = PushSocket::new(&ctx).expect("push");
            let endpoint = ventilator.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut workers = Vec::new();
            for _ in 0..3 {
                let worker = PullSocket::new(&ctx).expect("pull");
                worker.connect(&endpoint.to_string()).expect("connect");
                workers.push(worker);
            }
            wait_for(|| ventilator.peer_count() == 3).await;

            for n in 0..9 {
                ventilator.send(format!("task {n}")).await.expect("send");
            }

            for worker in &mut workers {
                let mut taken = Vec::new();
                for _ in 0..3 {
                    let task =
                        weida_testkit::within("a task", weida_testkit::EXPECTED, worker.recv())
                            .await
                            .expect("task");
                    taken.push(text(&task));
                }
                assert_eq!(taken.len(), 3, "each worker takes its third: {taken:?}");
            }
        }
        /// Claim: with nowhere to send, PUSH **waits** — it does not discard and
        /// it does not error. The task goes out when a worker appears, which is
        /// 30/PIPELINE's "block or error when none" and "never discard" in one.
        #[tokio::test]
        async fn push_blocks_rather_than_discarding_when_there_is_no_worker() {
            let ctx = context();
            let mut ventilator = PushSocket::new(&ctx).expect("push");
            let endpoint = ventilator.bind("tcp://127.0.0.1:0").await.expect("bind");
            assert_eq!(ventilator.peer_count(), 0);

            let sending = tokio::spawn(async move {
                ventilator.send("work").await.expect("send");
                ventilator
            });

            // Margin: 50 ms for a send that must *not* complete; a send that
            // wrongly dropped the task would have returned in microseconds.
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!sending.is_finished(), "a PUSH with no worker must wait");

            let mut worker = PullSocket::new(&ctx).expect("pull");
            worker.connect(&endpoint.to_string()).expect("connect");
            let task = weida_testkit::within("the task", weida_testkit::EXPECTED, worker.recv())
                .await
                .expect("task");
            assert_eq!(text(&task), "work");
            let ventilator = sending.await.expect("the sender completed");
            assert_eq!(ventilator.peer_count(), 1);
        }
        /// Claim: PULL fair-queues, so one busy sender cannot starve another —
        /// the first two messages come from two different senders even though
        /// each has a queue full of its own.
        #[tokio::test]
        async fn pull_fair_queues_its_peers() {
            let ctx = context();
            let mut sink = PullSocket::new(&ctx).expect("pull");
            let endpoint = sink.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut first = PushSocket::new(&ctx).expect("push one");
            let mut second = PushSocket::new(&ctx).expect("push two");
            first.connect(&endpoint.to_string()).expect("connect one");
            second.connect(&endpoint.to_string()).expect("connect two");
            wait_for(|| sink.peer_count() == 2).await;

            for n in 0..3 {
                first.send(format!("a{n}")).await.expect("send");
                second.send(format!("b{n}")).await.expect("send");
            }
            // Both senders' queues are full before the sink reads, so the order
            // is the sink's rotation rather than the network's.
            wait_for(|| {
                sink.core
                    .engine()
                    .peers()
                    .iter()
                    .all(|peer| !peer.pipe.incoming().is_empty())
            })
            .await;

            let first_two = [
                text(&sink.recv().await.expect("one")),
                text(&sink.recv().await.expect("two")),
            ];
            assert_ne!(
                first_two[0].as_bytes()[0],
                first_two[1].as_bytes()[0],
                "fair-queueing must alternate rather than drain one peer: {first_two:?}"
            );

            let mut all = first_two.to_vec();
            for _ in 0..4 {
                all.push(text(&sink.recv().await.expect("rest")));
            }
            all.sort();
            assert_eq!(all, vec!["a0", "a1", "a2", "b0", "b1", "b2"]);
        }
    }
}
