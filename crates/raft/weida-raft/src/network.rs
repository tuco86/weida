//! The client side of the transport: openraft's `RaftNetwork` as one weida
//! exchange per RPC, and the forwarding of client operations to the leader.
//!
//! An exchange's request body is a 4-byte big-endian length, the envelope
//! (`codec::encode_rpc`), and — for a snapshot only — the raw snapshot bytes
//! to the end of the stream. The reply body is the reply message alone. A
//! peer that will not answer refuses the exchange with a typed error instead.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use openraft::error::{
    Fatal, NetworkError, PayloadTooLarge, RPCError, RaftError, ReplicationClosed, StreamingError,
    Timeout, Unreachable,
};
use openraft::metrics::RaftMetrics;
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{LogId, OptionalSend, Snapshot, Vote};
use tokio::sync::watch;
use weida::{ClientTls, Requester, Runtime, TransferMeta};

use crate::codec::{self, Rpc};
use crate::{ConsensusConfig, Error, Node, NodeId, Request, Response, Role, TypeConfig};

/// A client operation that only the leader can carry out.
#[derive(Clone, Debug)]
pub(crate) enum ForwardOp {
    Write(Request),
    ReadIndex,
    AddNode(Node),
    RemoveNode(NodeId),
}

/// What carrying out a [`ForwardOp`] came to.
#[derive(Debug)]
pub(crate) enum ForwardOutcome {
    Done(Done),
    /// This node does not lead; the leader it knows of, if any.
    ForwardToLeader(Option<Node>),
    Failed(Error),
}

/// A carried-out [`ForwardOp`].
#[derive(Debug)]
pub(crate) enum Done {
    Written(Response),
    ReadIndex(Option<LogId<NodeId>>),
    Added(Role),
    Removed,
}

/// The dialling half of one node: a requester per peer, shared by every
/// replication stream and every forwarded operation toward it.
#[derive(Clone)]
pub(crate) struct Network {
    inner: Arc<NetworkInner>,
}

struct NetworkInner {
    id: NodeId,
    runtime: Runtime,
    tls: ClientTls,
    config: Arc<ConsensusConfig>,
    targets: Mutex<HashMap<NodeId, Arc<Target>>>,
    /// The engine's metrics, once the engine exists: what says which nodes the
    /// group still lists, and so which dialled targets are still wanted.
    metrics: OnceLock<watch::Receiver<RaftMetrics<NodeId, Node>>>,
}

/// One peer, dialled at one URL.
struct Target {
    url: String,
    requester: Requester,
    /// Set once the first connect succeeded; weida redials on its own after
    /// that. A failed first connect leaves it empty, so the next RPC retries.
    connected: tokio::sync::OnceCell<()>,
}

impl Network {
    /// `tls` must ask for `weida-raft/0` and present this node's identity.
    pub(crate) fn new(
        id: NodeId,
        runtime: Runtime,
        tls: ClientTls,
        config: Arc<ConsensusConfig>,
    ) -> Network {
        Network {
            inner: Arc::new(NetworkInner {
                id,
                runtime,
                tls,
                config,
                targets: Mutex::new(HashMap::new()),
                metrics: OnceLock::new(),
            }),
        }
    }

    /// Hands the network the engine's metrics, which the engine's own
    /// construction needs a network to produce.
    pub(crate) fn attach(&self, metrics: watch::Receiver<RaftMetrics<NodeId, Node>>) {
        // Set once: the engine is built once.
        let _ = self.inner.metrics.set(metrics);
    }

    /// The requester toward `id` at `node`, built on first use and rebuilt
    /// when the node's URL changed.
    fn target(&self, id: NodeId, node: &Node) -> Result<Arc<Target>, Error> {
        if node.id()? != id {
            return Err(Error::InvalidNode(
                "node URL names a different key than its id".to_owned(),
            ));
        }
        let mut targets = self
            .inner
            .targets
            .lock()
            .expect("the target map is never poisoned");
        if let Some(target) = targets.get(&id)
            && target.url == node.url
        {
            return Ok(Arc::clone(target));
        }
        // Making a target is the one moment the map grows, so it is the moment
        // to drop what the group no longer lists: a learner that came and went
        // would otherwise leave a requester and its connection behind for the
        // life of the process, and how many nodes a group has ever listed is
        // not bounded by how many it lists.
        if let Some(metrics) = self.inner.metrics.get() {
            let metrics = metrics.borrow();
            let membership = metrics.membership_config.membership();
            targets.retain(|other, old| {
                let wanted = *other == id || membership.get_node(other).is_some();
                if !wanted {
                    old.requester.disconnect(&old.url);
                }
                wanted
            });
        }
        let target = Arc::new(Target {
            url: node.url.clone(),
            requester: self.inner.runtime.requester(self.inner.tls.clone()),
            connected: tokio::sync::OnceCell::new(),
        });
        if let Some(old) = targets.insert(id, Arc::clone(&target)) {
            old.requester.disconnect(&old.url);
        }
        Ok(target)
    }

    /// Sends `header` (an encoded `Rpc::Forward`) to `leader` and decodes
    /// what it answered, within [`ConsensusConfig::forward_timeout`].
    pub(crate) async fn forward(
        &self,
        leader: &Node,
        header: &[u8],
    ) -> Result<ForwardOutcome, Error> {
        let target = self.target(leader.id()?, leader)?;
        let config = &self.inner.config;
        let reply = tokio::time::timeout(
            config.forward_timeout,
            target.call(header, None, config.max_rpc_bytes),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(codec::decode_forward_outcome(&reply)?)
    }
}

impl Target {
    async fn call(
        &self,
        header: &[u8],
        snapshot: Option<&[u8]>,
        max_reply: usize,
    ) -> Result<Vec<u8>, weida::Error> {
        self.connected
            .get_or_try_init(|| self.requester.connect(&self.url))
            .await?;
        let (mut transfer, reply) = self.requester.open(TransferMeta::default()).await?;
        let len = u32::try_from(header.len()).map_err(|_| weida::Error::LimitExceeded)?;
        transfer.write_all(&len.to_be_bytes()).await?;
        transfer.write_all(header).await?;
        if let Some(data) = snapshot {
            transfer.write_all(data).await?;
        }
        transfer.finish()?;
        reply.recv().await?.collect(max_reply).await
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Peer {
        // Validation happens per RPC, so a bad URL surfaces as an RPC error
        // openraft backs off from, not as a panic here.
        Peer {
            network: self.clone(),
            target,
            node: node.clone(),
        }
    }
}

/// The RPC channel toward one peer.
pub(crate) struct Peer {
    network: Network,
    target: NodeId,
    node: Node,
}

impl Peer {
    async fn call(&self, header: &[u8], snapshot: Option<&[u8]>) -> Result<Vec<u8>, Error> {
        let target = self.network.target(self.target, &self.node)?;
        let max_reply = self.network.inner.config.max_rpc_bytes;
        Ok(target.call(header, snapshot, max_reply).await?)
    }

    /// One call under openraft's hard TTL, its failure classified.
    async fn exchange(
        &self,
        action: RPCTypes,
        header: &[u8],
        snapshot: Option<&[u8]>,
        ttl: Duration,
    ) -> Result<Vec<u8>, Failure> {
        match tokio::time::timeout(ttl, self.call(header, snapshot)).await {
            Err(_) => Err(Failure::Timeout(Timeout {
                action,
                id: self.network.inner.id,
                target: self.target,
                timeout: ttl,
            })),
            Ok(Err(e)) if is_unreachable(&e) => Err(Failure::Unreachable(Unreachable::new(&e))),
            Ok(Err(e)) => Err(Failure::Network(NetworkError::new(&e))),
            Ok(Ok(reply)) => Ok(reply),
        }
    }
}

/// Why an exchange produced no reply, in the terms both of openraft's error
/// types share.
enum Failure {
    Timeout(Timeout<NodeId>),
    /// Back off before retrying.
    Unreachable(Unreachable),
    /// Retry at once.
    Network(NetworkError),
}

impl Failure {
    fn decode(e: codec::DecodeError) -> Failure {
        Failure::Network(NetworkError::new(&Error::Decode(e)))
    }
}

impl<E: std::error::Error> From<Failure> for RPCError<NodeId, Node, E> {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Timeout(e) => RPCError::Timeout(e),
            Failure::Unreachable(e) => RPCError::Unreachable(e),
            Failure::Network(e) => RPCError::Network(e),
        }
    }
}

impl From<Failure> for StreamingError<TypeConfig, Fatal<NodeId>> {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Timeout(e) => StreamingError::Timeout(e),
            Failure::Unreachable(e) => StreamingError::Unreachable(e),
            Failure::Network(e) => StreamingError::Network(e),
        }
    }
}

/// Failures openraft should back off from rather than retry at once: the
/// peer cannot be reached, or its address is unusable.
fn is_unreachable(e: &Error) -> bool {
    match e {
        Error::Weida(e) => matches!(
            e,
            weida::Error::NotConnected
                | weida::Error::ConnectionLost(_)
                | weida::Error::Tls(_)
                | weida::Error::Untrusted(_)
                | weida::Error::Io(_)
                | weida::Error::Transport(_)
        ),
        Error::InvalidNode(_) => true,
        _ => false,
    }
}

impl RaftNetwork<TypeConfig> for Peer {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let entries = rpc.entries.len() as u64;
        let header = codec::encode_rpc(&Rpc::AppendEntries(rpc));
        // The peer refuses a message over its cap; asking openraft for a
        // smaller batch is the only way such a batch ever gets through.
        if header.len() > self.network.inner.config.max_rpc_bytes && entries > 1 {
            return Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint(entries / 2),
            ));
        }
        let reply = self
            .exchange(RPCTypes::AppendEntries, &header, None, option.hard_ttl())
            .await?;
        Ok(codec::decode_append_response(&reply).map_err(Failure::decode)?)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let header = codec::encode_rpc(&Rpc::Vote(rpc));
        let reply = self
            .exchange(RPCTypes::Vote, &header, None, option.hard_ttl())
            .await?;
        Ok(codec::decode_vote_response(&reply).map_err(Failure::decode)?)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        let Snapshot { meta, snapshot } = snapshot;
        let header = codec::encode_rpc(&Rpc::Snapshot { vote, meta });
        // The whole buffer, whatever position the cursor was left at.
        let data = snapshot.get_ref().as_slice();
        let call = self.exchange(
            RPCTypes::InstallSnapshot,
            &header,
            Some(data),
            option.hard_ttl(),
        );
        let reply = tokio::select! {
            closed = cancel => return Err(StreamingError::Closed(closed)),
            outcome = call => outcome?,
        };
        Ok(codec::decode_snapshot_response(&reply).map_err(Failure::decode)?)
    }
}
