//! The serving side of the transport: every exchange on [`crate::RAFT_PATH`]
//! is one RPC, checked against the key its sender proved before the engine
//! sees it.

use std::sync::Arc;

use openraft::Snapshot;
use tokio::io::AsyncReadExt;
use weida::{ErrorCode, IncomingRequest, PeerIdentity, Replier, TransferMeta};

use crate::codec::{self, DecodeError, Rpc};
use crate::consensus::Ops;
use crate::{ConsensusConfig, NodeId};

/// Serves RPCs until the listener closes.
///
/// One task per request, so a snapshot being received never delays a
/// heartbeat. `tokio::spawn` lands on the ambient runtime, which is the
/// runtime's own `Exec` by the construction-site rule of the crate docs.
pub(crate) async fn serve(replier: Replier, ops: Arc<Ops>, config: Arc<ConsensusConfig>) {
    while let Ok(request) = replier.accept().await {
        tokio::spawn(handle(request, Arc::clone(&ops), Arc::clone(&config)));
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

    let mut len = [0u8; 4];
    if request.body().read_exact(&mut len).await.is_err() {
        return request.refuse(ErrorCode::Rejected).await;
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > config.max_rpc_bytes {
        tracing::debug!(%proved, len, "raft RPC over the size limit");
        return request.refuse(ErrorCode::Rejected).await;
    }
    let mut header = vec![0u8; len];
    if request.body().read_exact(&mut header).await.is_err() {
        return request.refuse(ErrorCode::Rejected).await;
    }
    let rpc = match codec::decode_rpc(&header) {
        Ok(rpc) => rpc,
        Err(DecodeError::UnknownKind(kind)) => {
            tracing::debug!(%proved, kind, "raft RPC of an unknown kind");
            return request.refuse(ErrorCode::Unsupported).await;
        }
        Err(e) => {
            tracing::debug!(%proved, error = %e, "undecodable raft RPC");
            return request.refuse(ErrorCode::Rejected).await;
        }
    };
    drop(header);

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
        None if !ops.is_member(&proved) => {
            tracing::warn!(%proved, "forwarded operation from a non-member");
            return request.refuse(ErrorCode::Rejected).await;
        }
        _ => {}
    }

    // Everything but a snapshot ends with its envelope.
    if !matches!(rpc, Rpc::Snapshot { .. }) {
        let mut probe = [0u8; 1];
        if !matches!(request.body().read(&mut probe).await, Ok(0)) {
            return request.refuse(ErrorCode::Rejected).await;
        }
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
            if body.read_to_end(data.get_mut()).await.is_err() {
                return request.refuse(ErrorCode::Rejected).await;
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
        Rpc::Forward(op) => codec::encode_forward_outcome(&ops.execute_local(&op).await),
    };

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
}

/// The engine failed or stopped: no answer, an `INTERNAL` refusal.
async fn internal(request: IncomingRequest, e: &(dyn std::fmt::Display + Sync)) {
    tracing::debug!(error = %e, "raft engine refused an RPC");
    request.refuse(ErrorCode::Internal).await;
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use openraft::Vote;
    use openraft::raft::VoteRequest;
    use weida::{Alpn, ClientTls, Identity, Runtime, RuntimeConfig, TransferMeta, Trust};

    use crate::codec::{self, Rpc};
    use crate::{Consensus, ConsensusConfig, MemoryStore, NodeId};

    /// Sends one Vote RPC whose vote names `claimed` and returns the reply.
    async fn vote_as(
        requester: &weida::Requester,
        claimed: NodeId,
    ) -> Result<Vec<u8>, weida::Error> {
        let header = codec::encode_rpc(&Rpc::Vote(VoteRequest {
            vote: Vote::new(1, claimed),
            last_log_id: None,
        }));
        let (mut transfer, reply) = requester.open(TransferMeta::default()).await?;
        transfer
            .write_all(&(header.len() as u32).to_be_bytes())
            .await?;
        transfer.write_all(&header).await?;
        transfer.finish()?;
        reply.recv().await?.collect(1 << 16).await
    }

    /// The identity rule: an engine message whose vote names a sender other
    /// than the key proved in the handshake is refused before openraft sees
    /// it; the same message naming the proved key is answered.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_rpc_claiming_another_sender_is_refused() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let store = MemoryStore::default();
        let consensus = Consensus::start(
            &runtime,
            Identity::generate().expect("identity"),
            store.clone(),
            store,
            ConsensusConfig::new("127.0.0.1:0".parse().expect("loopback address")),
        )
        .await
        .expect("start");
        consensus.bootstrap().await.expect("bootstrap");
        let deadline = Instant::now() + Duration::from_secs(10);
        while consensus.leader() != Some(consensus.id()) {
            assert!(Instant::now() < deadline, "the node never led");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

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

        let y_id = NodeId([9; 32]);
        assert!(
            matches!(vote_as(&requester, y_id).await, Err(weida::Error::Rejected)),
            "a vote naming Y, sent by X, must be refused"
        );
        let reply = vote_as(&requester, x_id).await.expect("a vote naming X");
        codec::decode_vote_response(&reply).expect("a VoteResponse");

        consensus.shutdown().await;
    }
}
