//! The serving side of the transport: every exchange on [`crate::RAFT_PATH`]
//! is one RPC, checked against the key its sender proved before the engine
//! sees it.
//!
//! **Admission.** Once a node's effective membership is non-empty, an RPC
//! whose proved key is not in it is refused with `Rejected` before a byte of
//! it is read; a node with an empty membership accepts engine RPCs from any
//! key, because that is how a node being added learns its group. A forwarded
//! write or read barrier needs a member, and a forwarded membership change
//! needs a **voter**: a learner replicates the group's log and is not one of
//! the nodes that decides who belongs to it.
//!
//! **What a stranger can cost.** The binding admits any key that proves
//! possession of one, so who is *in* the group is only known after the engine
//! has been asked, and what a key outside it may hold is bounded before that.
//! A request is classified first, under [`MAX_UNADMITTED`] and the election
//! deadline, and only then takes one of the `max_concurrent_rpcs` handler
//! permits; so a stranger cannot spend a member's permit, and an outsider's
//! refusal — which waits on the outsider's own flow-control credit — runs in
//! a task of its own under [`MAX_REFUSALS`] and is *dropped* when none is
//! free. Nothing the accept loop does waits on a peer or on the engine.
//!
//! **Deadlines and the caps.** The length prefix, the envelope and its end
//! are read under [`crate::ConsensusConfig::election_timeout_max`], a snapshot
//! body under `install_snapshot_timeout` counted from admission, a forwarded
//! operation under `forward_timeout`, and every refusal and the reply under
//! `election_timeout_max` again, so no peer can hold a handler by going
//! quiet. At most `max_concurrent_rpcs` handlers run; one more is refused. At
//! most [`MAX_SNAPSHOT_BODIES`] snapshot bodies are buffered at once, and one
//! whose vote the engine would refuse is answered before any of it is read.

use std::sync::Arc;

use openraft::Snapshot;
use openraft::raft::SnapshotResponse;
use tokio::io::AsyncReadExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout, timeout_at};
use weida::{ErrorCode, IncomingRequest, PeerIdentity, Replier, TransferMeta};

use crate::codec::{self, DecodeError, Rpc};
use crate::consensus::{Admission, Ops};
use crate::network::ForwardOp;
use crate::{ConsensusConfig, NodeId};

/// How much of an envelope is reserved before its bytes arrive: a claimed
/// length costs nothing until its bytes arrive.
const HEADER_PREALLOC: usize = 64 << 10;

/// Requests the server has accepted and not yet classified.
///
/// A request is held here from its accept until the engine has said whether
/// its key is in the membership, which is a round trip to the engine's own
/// task. The cap is what keeps a flood of requests from keys that have never
/// been heard of from queueing that many tasks on the engine, and it is held
/// for nothing a peer controls: classification waits on the engine alone.
const MAX_UNADMITTED: usize = 256;

/// Refusals in flight at once.
///
/// A refusal is an ERROR frame written to the peer's own flow-control credit,
/// and a peer that withholds credit holds the write until its deadline. The
/// refusals of strangers are the ones a stranger can prolong at will, so they
/// share a small pool of their own instead of the handler permits; one more
/// than the pool holds is dropped, which the peer sees as an exchange nobody
/// answered.
const MAX_REFUSALS: usize = 16;

/// Snapshot bodies buffered at once.
///
/// A snapshot body is held in memory up to
/// [`ConsensusConfig::max_snapshot_bytes`] — many times what an envelope costs
/// — before the engine can look at it. A leader sends one snapshot at a time
/// to each follower, so a node receives one from its leader and, briefly across
/// a change of leader, a second.
const MAX_SNAPSHOT_BODIES: usize = 2;

/// What the accept loop and the handlers it starts share.
struct Server {
    ops: Arc<Ops>,
    config: Arc<ConsensusConfig>,
    /// See [`MAX_UNADMITTED`].
    unadmitted: Arc<Semaphore>,
    /// See [`MAX_REFUSALS`].
    refusals: Arc<Semaphore>,
    /// One permit per admitted request being served.
    handlers: Arc<Semaphore>,
    /// See [`MAX_SNAPSHOT_BODIES`].
    bodies: Semaphore,
}

/// Serves RPCs until the listener closes.
///
/// One task per request, so a snapshot being received never delays a
/// heartbeat. `tokio::spawn` lands on the ambient runtime, which is the
/// runtime's own `Exec` by the construction-site rule of the crate docs.
///
/// At most [`ConsensusConfig::max_concurrent_rpcs`] admitted requests run at
/// once. A request over a cap is refused without the loop waiting for it:
/// the refusal is a task of its own with its own small cap, and past that the
/// request is dropped.
pub(crate) async fn serve(replier: Replier, ops: Arc<Ops>, config: Arc<ConsensusConfig>) {
    let server = Arc::new(Server {
        handlers: Arc::new(Semaphore::new(config.max_concurrent_rpcs)),
        unadmitted: Arc::new(Semaphore::new(MAX_UNADMITTED)),
        refusals: Arc::new(Semaphore::new(MAX_REFUSALS)),
        bodies: Semaphore::new(MAX_SNAPSHOT_BODIES),
        ops,
        config,
    });
    while let Ok(request) = replier.accept().await {
        match Arc::clone(&server.unadmitted).try_acquire_owned() {
            Ok(pending) => {
                let server = Arc::clone(&server);
                tokio::spawn(async move { server.admit(request, pending).await });
            }
            Err(_) => {
                tracing::debug!(cap = MAX_UNADMITTED, "raft RPC over the pre-admission cap");
                server.refuse_detached(request, ErrorCode::Rejected);
            }
        }
    }
}

impl Server {
    /// Classifies a request, and either refuses it or serves it.
    async fn admit(self: Arc<Self>, request: IncomingRequest, pending: OwnedSemaphorePermit) {
        // The binding requires client keys, so this is defensive.
        let Some(proved) = request
            .meta()
            .peer
            .as_ref()
            .and_then(PeerIdentity::key)
            .map(NodeId::from)
        else {
            drop(pending);
            return self.refuse_detached(request, ErrorCode::Rejected);
        };

        let classified =
            timeout(self.config.election_timeout_max, self.ops.admission(proved)).await;
        drop(pending);
        let admission = match classified {
            Ok(Ok(admission)) => admission,
            Ok(Err(fatal)) => {
                tracing::debug!(error = %fatal, "raft engine refused an admission check");
                return self.refuse_detached(request, ErrorCode::Internal);
            }
            Err(_) => {
                tracing::debug!(%proved, "raft engine did not classify a key in time");
                return self.refuse_detached(request, ErrorCode::Internal);
            }
        };
        if admission == Admission::Outsider {
            // Debug: the volume is chosen by whoever sends.
            tracing::debug!(%proved, "raft RPC from a key outside the membership");
            return self.refuse_detached(request, ErrorCode::Rejected);
        }

        let Ok(permit) = Arc::clone(&self.handlers).try_acquire_owned() else {
            tracing::debug!(
                cap = self.config.max_concurrent_rpcs,
                "raft RPC over the cap"
            );
            return self.refuse_detached(request, ErrorCode::Rejected);
        };
        self.handle(request, proved, admission).await;
        drop(permit);
    }

    /// Refuses a request in a task of its own, or drops it when the pool of
    /// refusals is spent.
    ///
    /// For the requests that hold no handler permit: the refusal waits on the
    /// peer, and what waits on a peer must not hold anything a member needs.
    fn refuse_detached(&self, request: IncomingRequest, code: ErrorCode) {
        let Ok(permit) = Arc::clone(&self.refusals).try_acquire_owned() else {
            tracing::debug!(
                cap = MAX_REFUSALS,
                "raft refusals over the cap; dropping a request"
            );
            return;
        };
        let deadline = self.config.election_timeout_max;
        tokio::spawn(async move {
            let _ = timeout(deadline, request.refuse(code)).await;
            drop(permit);
        });
    }

    /// Refuses a request the handler already holds a permit for, for as long
    /// as the peer lets it.
    async fn refuse(&self, request: IncomingRequest, code: ErrorCode) {
        let refused = timeout(self.config.election_timeout_max, request.refuse(code)).await;
        if refused.is_err() {
            tracing::debug!("a raft refusal was not taken in time");
        }
    }

    /// The engine failed or stopped: no answer, an `INTERNAL` refusal.
    async fn internal(&self, request: IncomingRequest, e: &(dyn std::fmt::Display + Sync)) {
        tracing::debug!(error = %e, "raft engine refused an RPC");
        self.refuse(request, ErrorCode::Internal).await;
    }

    async fn handle(&self, mut request: IncomingRequest, proved: NodeId, admission: Admission) {
        let config = &self.config;
        let ops = &self.ops;
        let started = Instant::now();

        let rpc = match timeout(
            config.election_timeout_max,
            read_rpc(&mut request, proved, config.max_rpc_bytes),
        )
        .await
        {
            Ok(Ok(rpc)) => rpc,
            Ok(Err(code)) => return self.refuse(request, code).await,
            Err(_) => {
                tracing::debug!(%proved, "raft RPC envelope not received in time");
                return self.refuse(request, ErrorCode::Rejected).await;
            }
        };

        // The identity rule (0020 §4.4): an engine message names its sender in
        // its vote, and that sender must be the key that proved itself in the
        // handshake. A forwarded client operation must come from a member, and
        // one that changes the membership from a voter.
        let permitted = match &rpc {
            Rpc::Vote(rpc) => claim(proved, rpc.vote.leader_id.node_id),
            Rpc::AppendEntries(rpc) => claim(proved, rpc.vote.leader_id.node_id),
            Rpc::Snapshot { vote, .. } => claim(proved, vote.leader_id.node_id),
            Rpc::Forward(op) => {
                let allowed = match op {
                    ForwardOp::AddNode(_) | ForwardOp::RemoveNode(_) => {
                        admission == Admission::Voter
                    }
                    ForwardOp::Write(_) | ForwardOp::ReadIndex => admission.is_member(),
                };
                if !allowed {
                    tracing::warn!(
                        %proved,
                        ?admission,
                        "forwarded operation from a node that may not send it"
                    );
                }
                allowed
            }
        };
        if !permitted {
            return self.refuse(request, ErrorCode::Rejected).await;
        }
        let reply = match rpc {
            Rpc::Vote(rpc) => match ops.raft.vote(rpc).await {
                Ok(response) => codec::encode_vote_response(&response),
                Err(e) => return self.internal(request, &e).await,
            },
            Rpc::AppendEntries(rpc) => match ops.raft.append_entries(rpc).await {
                Ok(response) => codec::encode_append_response(&response),
                Err(e) => return self.internal(request, &e).await,
            },
            Rpc::Snapshot { vote, meta } => {
                // Held until the snapshot is installed: the buffer is what the
                // cap is about.
                let Ok(_body) = self.bodies.try_acquire() else {
                    tracing::debug!(%proved, "raft snapshot while others are being received");
                    return self.refuse(request, ErrorCode::Rejected).await;
                };
                // The engine's own first check, made before anything is
                // buffered: a vote lower than the one this node holds is
                // answered with that vote, which is what tells a stale leader
                // to step down, and nothing of the body is read.
                let current = match ops.raft.with_raft_state(|st| *st.vote_ref()).await {
                    Ok(current) => current,
                    Err(e) => return self.internal(request, &e).await,
                };
                if vote < current {
                    tracing::debug!(%proved, "raft snapshot under a stale vote");
                    codec::encode_snapshot_response(&SnapshotResponse { vote: current })
                } else {
                    let mut data = match ops.raft.begin_receiving_snapshot().await {
                        Ok(data) => data,
                        Err(e) => return self.internal(request, &e).await,
                    };
                    let cap = config.max_snapshot_bytes;
                    let mut body = request.body().take(cap as u64 + 1);
                    match timeout_at(
                        started + config.install_snapshot_timeout,
                        body.read_to_end(data.get_mut()),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(_)) => return self.refuse(request, ErrorCode::Rejected).await,
                        Err(_) => {
                            tracing::debug!(%proved, "snapshot not received in time");
                            return self.refuse(request, ErrorCode::Rejected).await;
                        }
                    }
                    if data.get_ref().len() > cap {
                        tracing::warn!(%proved, cap, "snapshot over the size limit");
                        return self.refuse(request, ErrorCode::Rejected).await;
                    }
                    data.set_position(0);
                    let snapshot = Snapshot {
                        meta,
                        snapshot: data,
                    };
                    match ops.raft.install_full_snapshot(vote, snapshot).await {
                        Ok(response) => codec::encode_snapshot_response(&response),
                        Err(e) => return self.internal(request, &e).await,
                    }
                }
            }
            // The forwarder has given up by then, and the permit must not stay
            // held by an operation that hangs without a quorum. The operation is
            // moved into the engine, not copied: nothing here needs it again.
            Rpc::Forward(op) => match timeout(config.forward_timeout, ops.execute(op)).await {
                Ok(outcome) => codec::encode_forward_outcome(&outcome),
                Err(_) => {
                    return self
                        .internal(request, &"forwarded operation timed out")
                        .await;
                }
            },
        };

        // Bounded, so a peer that stops reading cannot hold the handler.
        let sent = timeout(config.election_timeout_max, async {
            match request.reply(TransferMeta::default()).await {
                Ok(mut out) => {
                    if let Err(e) = out.write_all(&reply).await {
                        tracing::debug!(error = %e, "raft reply not written");
                    } else if let Err(e) = out.finish() {
                        tracing::debug!(error = %e, "raft reply not finished");
                    }
                }
                Err(e) => tracing::debug!(error = %e, "raft reply not opened"),
            }
        })
        .await;
        if sent.is_err() {
            tracing::debug!(%proved, "raft reply not taken in time");
        }
    }
}

/// Does an engine message's claimed sender match the key that proved itself?
fn claim(proved: NodeId, claimed: NodeId) -> bool {
    if claimed == proved {
        return true;
    }
    tracing::warn!(%proved, %claimed, "raft RPC claims a sender other than the proved key");
    false
}

/// Reads one RPC's length prefix and envelope, and for every kind but a
/// snapshot its end; a snapshot's body follows and is the caller's.
///
/// The envelope is allocated as it arrives, not as claimed. The caller
/// bounds the whole read with a deadline.
async fn read_rpc(
    request: &mut IncomingRequest,
    proved: NodeId,
    max: usize,
) -> Result<Rpc, ErrorCode> {
    let mut len = [0u8; 4];
    if request.body().read_exact(&mut len).await.is_err() {
        return Err(ErrorCode::Rejected);
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > max {
        tracing::debug!(%proved, len, "raft RPC over the size limit");
        return Err(ErrorCode::Rejected);
    }
    let mut header = Vec::with_capacity(len.min(HEADER_PREALLOC));
    if request
        .body()
        .take(len as u64)
        .read_to_end(&mut header)
        .await
        .is_err()
        || header.len() != len
    {
        return Err(ErrorCode::Rejected);
    }
    let rpc = match codec::decode_rpc(&header) {
        Ok(rpc) => rpc,
        Err(DecodeError::UnknownKind(kind)) => {
            tracing::debug!(%proved, kind, "raft RPC of an unknown kind");
            return Err(ErrorCode::Unsupported);
        }
        Err(e) => {
            tracing::debug!(%proved, error = %e, "undecodable raft RPC");
            return Err(ErrorCode::Rejected);
        }
    };
    drop(header);

    // Everything but a snapshot ends with its envelope.
    if !matches!(rpc, Rpc::Snapshot { .. }) {
        let mut probe = [0u8; 1];
        if !matches!(request.body().read(&mut probe).await, Ok(0)) {
            return Err(ErrorCode::Rejected);
        }
    }
    Ok(rpc)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use openraft::raft::{AppendEntriesRequest, VoteRequest};
    use openraft::{CommittedLeaderId, LogId, SnapshotMeta, Vote};
    use weida::{Alpn, ClientTls, Identity, Runtime, RuntimeConfig, TransferMeta, Trust};

    use crate::codec::{self, Rpc};
    use crate::{Consensus, ConsensusConfig, MemoryStore, NodeId, Request};

    /// Sends one RPC and returns the reply.
    async fn send(requester: &weida::Requester, rpc: &Rpc) -> Result<Vec<u8>, weida::Error> {
        let header = codec::encode_rpc(rpc);
        let (mut transfer, reply) = requester.open(TransferMeta::default()).await?;
        transfer
            .write_all(&(header.len() as u32).to_be_bytes())
            .await?;
        transfer.write_all(&header).await?;
        transfer.finish()?;
        reply.recv().await?.collect(1 << 16).await
    }

    fn vote_by(claimed: NodeId) -> Rpc {
        Rpc::Vote(VoteRequest {
            vote: Vote::new(1, claimed),
            last_log_id: None,
        })
    }

    async fn start(runtime: &Runtime, config: ConsensusConfig) -> Consensus {
        let store = MemoryStore::default();
        Consensus::start(
            runtime,
            Identity::generate().expect("identity"),
            store.clone(),
            store,
            config,
        )
        .await
        .expect("start")
    }

    fn loopback() -> ConsensusConfig {
        ConsensusConfig::new("127.0.0.1:0".parse().expect("loopback address"))
    }

    async fn leads(consensus: &Consensus) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while consensus.leader() != Some(consensus.id()) {
            assert!(Instant::now() < deadline, "the node never led");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A stranger with a fresh key: its id and a requester connected to
    /// `consensus` on the raft ALPN.
    async fn stranger(runtime: &Runtime, consensus: &Consensus) -> (NodeId, weida::Requester) {
        let x = Identity::generate().expect("identity X");
        let x_id = NodeId::from(x.fingerprint().expect("fingerprint"));
        let requester = runtime.requester(
            ClientTls::new(Trust::by_address())
                .with_identity(x)
                .with_alpn(Alpn::Raft),
        );
        requester
            .connect(&consensus.node().url)
            .await
            .expect("connect");
        (x_id, requester)
    }

    /// The identity rule: an engine message whose vote names a sender other
    /// than the key proved in the handshake is refused before openraft sees
    /// it; the same message naming the proved key is answered. Run against
    /// an uninitialized node, which admits every key.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_rpc_claiming_another_sender_is_refused() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let consensus = start(&runtime, loopback()).await;
        let (x_id, requester) = stranger(&runtime, &consensus).await;

        let y_id = NodeId([9; 32]);
        assert!(
            matches!(
                send(&requester, &vote_by(y_id)).await,
                Err(weida::Error::Rejected)
            ),
            "a vote naming Y, sent by X, must be refused"
        );
        let reply = send(&requester, &vote_by(x_id))
            .await
            .expect("a vote naming X");
        codec::decode_vote_response(&reply).expect("a VoteResponse");

        consensus.shutdown().await;
    }

    /// Admission: once a node is in a group, a key outside it can neither
    /// unseat its leader with a high-term vote nor append as a leader.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_outsider_cannot_unseat_or_write_to_an_initialized_node() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let consensus = start(&runtime, loopback()).await;
        consensus.bootstrap().await.expect("bootstrap");
        leads(&consensus).await;
        let (x_id, requester) = stranger(&runtime, &consensus).await;

        let vote = Rpc::Vote(VoteRequest {
            vote: Vote::new(1_000, x_id),
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1_000, x_id), 1_000)),
        });
        assert!(
            matches!(send(&requester, &vote).await, Err(weida::Error::Rejected)),
            "an outsider's vote must be refused"
        );
        let append = Rpc::AppendEntries(AppendEntriesRequest {
            vote: Vote::new_committed(1_000, x_id),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        });
        assert!(
            matches!(send(&requester, &append).await, Err(weida::Error::Rejected)),
            "an outsider's AppendEntries must be refused"
        );

        assert_eq!(consensus.leader(), Some(consensus.id()));
        consensus
            .write(Request(b"after".to_vec()))
            .await
            .expect("the node still takes writes");

        consensus.shutdown().await;
    }

    /// A peer that claims a large envelope and sends none of it holds a
    /// handler only until the deadline, and over the cap the next request is
    /// refused at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_envelope_ends_at_its_deadline_and_the_cap_refuses_more() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let mut config = loopback();
        config.heartbeat_interval = Duration::from_millis(100);
        config.election_timeout_min = Duration::from_millis(300);
        config.election_timeout_max = Duration::from_millis(600);
        config.max_concurrent_rpcs = 2;
        let consensus = start(&runtime, config).await;
        let (x_id, requester) = stranger(&runtime, &consensus).await;

        let mut stalled = Vec::new();
        for _ in 0..2 {
            let (mut transfer, reply) =
                requester.open(TransferMeta::default()).await.expect("open");
            transfer
                .write_all(&(1u32 << 20).to_be_bytes())
                .await
                .expect("the length prefix");
            stalled.push((Instant::now(), transfer, reply));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let sent = Instant::now();
        assert!(
            matches!(
                send(&requester, &vote_by(x_id)).await,
                Err(weida::Error::Rejected)
            ),
            "a request over the cap must be refused"
        );
        assert!(
            sent.elapsed() < Duration::from_millis(500),
            "the refusal took {:?}",
            sent.elapsed()
        );

        for (opened, transfer, reply) in stalled {
            let result = async { reply.recv().await?.collect(1 << 16).await }.await;
            assert!(
                matches!(result, Err(weida::Error::Rejected)),
                "a stalled envelope must end Rejected, got {result:?}"
            );
            let waited = opened.elapsed();
            assert!(
                waited >= Duration::from_millis(500) && waited < Duration::from_secs(5),
                "the stalled exchange ended after {waited:?}"
            );
            drop(transfer);
        }

        // The permits are back.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let reply = send(&requester, &vote_by(x_id))
            .await
            .expect("a complete vote after the stalled ones ended");
        codec::decode_vote_response(&reply).expect("a VoteResponse");

        consensus.shutdown().await;
    }

    /// Opens a snapshot RPC the way a leader does — length prefix, then the
    /// envelope — and returns the handles without sending the body or the
    /// end, so the receiver is left waiting for bytes that do not come.
    async fn open_snapshot(
        requester: &weida::Requester,
        vote: Vote<NodeId>,
    ) -> (weida::OutgoingTransfer, weida::ReplyStream) {
        let header = codec::encode_rpc(&Rpc::Snapshot {
            vote,
            meta: SnapshotMeta::default(),
        });
        let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
        transfer
            .write_all(&(header.len() as u32).to_be_bytes())
            .await
            .expect("the length prefix");
        transfer.write_all(&header).await.expect("the envelope");
        (transfer, reply)
    }

    /// Snapshot bodies are buffered up to `max_snapshot_bytes` each, so only
    /// two are held at once: a third, however it is sent, is refused before
    /// the engine or a buffer is involved.
    #[tokio::test(flavor = "multi_thread")]
    async fn only_two_snapshot_bodies_are_buffered_at_once() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let consensus = start(&runtime, loopback()).await;
        let (x_id, requester) = stranger(&runtime, &consensus).await;
        let vote = Vote::new_committed(1, x_id);

        let first = open_snapshot(&requester, vote).await;
        let second = open_snapshot(&requester, vote).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let started = Instant::now();
        let third = send(
            &requester,
            &Rpc::Snapshot {
                vote,
                meta: SnapshotMeta::default(),
            },
        )
        .await;
        assert!(
            matches!(third, Err(weida::Error::Rejected)),
            "a third snapshot must be refused, got {third:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));

        drop((first, second));
        consensus.shutdown().await;
    }

    /// A snapshot under a vote the engine would refuse is answered with the
    /// vote the node holds, before any of its body is read: the sender never
    /// sends one here, and the answer still comes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_snapshot_under_a_stale_vote_is_answered_without_its_body() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let consensus = start(&runtime, loopback()).await;
        let (x_id, requester) = stranger(&runtime, &consensus).await;

        // The node takes a vote at a high term.
        let reply = send(
            &requester,
            &Rpc::Vote(VoteRequest {
                vote: Vote::new(5, x_id),
                last_log_id: None,
            }),
        )
        .await
        .expect("a vote");
        let held = codec::decode_vote_response(&reply)
            .expect("a VoteResponse")
            .vote;
        assert_eq!(held.leader_id.term, 5);

        let (_transfer, reply) = open_snapshot(&requester, Vote::new_committed(1, x_id)).await;
        let answered = tokio::time::timeout(Duration::from_secs(2), async {
            reply.recv().await?.collect(1 << 16).await
        })
        .await
        .expect("answered while the body is still to come")
        .expect("a reply");
        let response = codec::decode_snapshot_response(&answered).expect("a SnapshotResponse");
        assert_eq!(
            response.vote, held,
            "the answer carries the vote the node holds"
        );

        consensus.shutdown().await;
    }
}
