//! The serving side of the transport: every exchange on [`crate::RAFT_PATH`]
//! is one RPC, checked against the key its sender proved before the engine
//! sees it.
//!
//! **Admission.** Once a node's effective membership is non-empty, an RPC
//! whose proved key is not in it is refused with `Rejected` before a byte of
//! it is read; a node with an empty membership accepts engine RPCs from any
//! key, because that is how a node being added learns its group.
//!
//! **Deadlines and the cap.** The length prefix, the envelope and its end
//! are read under [`crate::ConsensusConfig::election_timeout_max`], a snapshot
//! body under `install_snapshot_timeout` counted from admission, a forwarded
//! operation under `forward_timeout`, and the reply under
//! `election_timeout_max` again, so no peer can hold a handler by going
//! quiet. At most `max_concurrent_rpcs` handlers run; one more is refused.

use std::sync::Arc;

use openraft::Snapshot;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout, timeout_at};
use weida::{ErrorCode, IncomingRequest, PeerIdentity, Replier, TransferMeta};

use crate::codec::{self, DecodeError, Rpc};
use crate::consensus::{Admission, Ops};
use crate::{ConsensusConfig, NodeId};

/// How much of an envelope is reserved before its bytes arrive: a claimed
/// length costs nothing until its bytes arrive.
const HEADER_PREALLOC: usize = 64 << 10;

/// Serves RPCs until the listener closes.
///
/// One task per request, so a snapshot being received never delays a
/// heartbeat. `tokio::spawn` lands on the ambient runtime, which is the
/// runtime's own `Exec` by the construction-site rule of the crate docs.
///
/// At most [`ConsensusConfig::max_concurrent_rpcs`] run at once. A request
/// over the cap is refused here, inline, without a task: an ERROR frame fits
/// a fresh stream's initial credit, and the timeout covers a peer that
/// withholds even that.
pub(crate) async fn serve(replier: Replier, ops: Arc<Ops>, config: Arc<ConsensusConfig>) {
    let permits = Arc::new(Semaphore::new(config.max_concurrent_rpcs));
    while let Ok(request) = replier.accept().await {
        match Arc::clone(&permits).try_acquire_owned() {
            Ok(permit) => {
                let ops = Arc::clone(&ops);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    handle(request, ops, config).await;
                    drop(permit);
                });
            }
            Err(_) => {
                tracing::debug!(cap = config.max_concurrent_rpcs, "raft RPC over the cap");
                let _ = timeout(
                    config.heartbeat_interval,
                    request.refuse(ErrorCode::Rejected),
                )
                .await;
            }
        }
    }
}

async fn handle(mut request: IncomingRequest, ops: Arc<Ops>, config: Arc<ConsensusConfig>) {
    // The binding requires client keys, so this is defensive.
    let Some(proved) = request
        .meta()
        .peer
        .as_ref()
        .and_then(PeerIdentity::key)
        .map(NodeId::from)
    else {
        return request.refuse(ErrorCode::Rejected).await;
    };

    let started = Instant::now();
    let admission = match ops.admission(proved).await {
        Ok(admission) => admission,
        Err(fatal) => return internal(request, &fatal).await,
    };
    if admission == Admission::Outsider {
        // Debug: the volume is chosen by whoever sends.
        tracing::debug!(%proved, "raft RPC from a key outside the membership");
        return request.refuse(ErrorCode::Rejected).await;
    }

    let rpc = match timeout(
        config.election_timeout_max,
        read_rpc(&mut request, proved, config.max_rpc_bytes),
    )
    .await
    {
        Ok(Ok(rpc)) => rpc,
        Ok(Err(code)) => return request.refuse(code).await,
        Err(_) => {
            tracing::debug!(%proved, "raft RPC envelope not received in time");
            return request.refuse(ErrorCode::Rejected).await;
        }
    };

    // The identity rule (0020 §4.4): an engine message names its sender in
    // its vote, and that sender must be the key that proved itself in the
    // handshake. A forwarded client operation must come from a member.
    let claimed = match &rpc {
        Rpc::Vote(rpc) => Some(rpc.vote.leader_id.node_id),
        Rpc::AppendEntries(rpc) => Some(rpc.vote.leader_id.node_id),
        Rpc::Snapshot { vote, .. } => Some(vote.leader_id.node_id),
        Rpc::Forward(_) => None,
    };
    match claimed {
        Some(claimed) if claimed != proved => {
            tracing::warn!(%proved, %claimed, "raft RPC claims a sender other than the proved key");
            return request.refuse(ErrorCode::Rejected).await;
        }
        None if admission != Admission::Member => {
            tracing::warn!(%proved, "forwarded operation from a non-member");
            return request.refuse(ErrorCode::Rejected).await;
        }
        _ => {}
    }

    let reply = match rpc {
        Rpc::Vote(rpc) => match ops.raft.vote(rpc).await {
            Ok(response) => codec::encode_vote_response(&response),
            Err(e) => return internal(request, &e).await,
        },
        Rpc::AppendEntries(rpc) => match ops.raft.append_entries(rpc).await {
            Ok(response) => codec::encode_append_response(&response),
            Err(e) => return internal(request, &e).await,
        },
        Rpc::Snapshot { vote, meta } => {
            let mut data = match ops.raft.begin_receiving_snapshot().await {
                Ok(data) => data,
                Err(e) => return internal(request, &e).await,
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
                Ok(Err(_)) => return request.refuse(ErrorCode::Rejected).await,
                Err(_) => {
                    tracing::debug!(%proved, "snapshot not received in time");
                    return request.refuse(ErrorCode::Rejected).await;
                }
            }
            if data.get_ref().len() > cap {
                tracing::warn!(%proved, cap, "snapshot over the size limit");
                return request.refuse(ErrorCode::Rejected).await;
            }
            data.set_position(0);
            let snapshot = Snapshot {
                meta,
                snapshot: data,
            };
            match ops.raft.install_full_snapshot(vote, snapshot).await {
                Ok(response) => codec::encode_snapshot_response(&response),
                Err(e) => return internal(request, &e).await,
            }
        }
        // The forwarder has given up by then, and the permit must not stay
        // held by an operation that hangs without a quorum.
        Rpc::Forward(op) => match timeout(config.forward_timeout, ops.execute_local(&op)).await {
            Ok(outcome) => codec::encode_forward_outcome(&outcome),
            Err(_) => return internal(request, &"forwarded operation timed out").await,
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

/// The engine failed or stopped: no answer, an `INTERNAL` refusal.
async fn internal(request: IncomingRequest, e: &(dyn std::fmt::Display + Sync)) {
    tracing::debug!(error = %e, "raft engine refused an RPC");
    request.refuse(ErrorCode::Internal).await;
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use openraft::raft::{AppendEntriesRequest, VoteRequest};
    use openraft::{CommittedLeaderId, LogId, Vote};
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
}
