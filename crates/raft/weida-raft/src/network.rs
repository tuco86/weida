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
        let write = async {
            transfer.write_all(&len.to_be_bytes()).await?;
            transfer.write_all(header).await?;
            if let Some(data) = snapshot {
                transfer.write_all(data).await?;
            }
            transfer.finish()
        };
        let read = async { reply.recv().await?.collect(max_reply).await };
        tokio::pin!(write, read);
        // The peer may answer before it has read the request: a snapshot
        // under a vote it would refuse is answered at once, and it stops the
        // request half as it does, which fails this write. The answer is
        // what the exchange came to, so it is read while the write is still
        // going and not after it, and a write that failed leaves the reply
        // to say why.
        let mut writing = true;
        loop {
            tokio::select! {
                reply = &mut read => return reply,
                _ = &mut write, if writing => writing = false,
            }
        }
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
        let rpc = Rpc::AppendEntries(rpc);
        // What the peer measures its cap against, counted without building
        // it: a batch of entries that each weigh almost the cap is not worth
        // a second copy of itself to be told it is too large.
        let size = codec::rpc_len(&rpc);
        let max = self.network.inner.config.max_rpc_bytes;
        // The peer refuses a message over its cap; asking openraft for a
        // smaller batch is the only way such a batch ever gets through.
        if size > max && entries > 1 {
            return Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint(entries_that_fit(entries, size, max)),
            ));
        }
        let header = codec::encode_rpc(&rpc);
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
        // The whole buffer, whatever position the cursor was left at.
        let data = snapshot.get_ref().as_slice();
        // The follower reads up to its cap and refuses what is over, so a
        // snapshot over the cap is sent for nothing, and again at every
        // retry. The follower's cap is not known here; the group's is taken
        // to be this node's own (see `ConsensusConfig::max_snapshot_bytes`).
        let max = self.network.inner.config.max_snapshot_bytes;
        if data.len() > max {
            let refused = Error::Config(format!(
                "a snapshot of {} bytes is over max_snapshot_bytes ({max})",
                data.len()
            ));
            return Err(Failure::Unreachable(Unreachable::new(&refused)).into());
        }
        let header = codec::encode_rpc(&Rpc::Snapshot { vote, meta });
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

/// How many of `entries` entries fit under `max` bytes, judging by what the
/// whole batch of them weighs.
///
/// Derived from the bytes and not halved from the count: one retry usually
/// fits where halving takes several, each of them a batch loaded and
/// measured. At least one, because a single entry is sent whatever it
/// weighs, and never the whole batch, which is the one that was too large.
fn entries_that_fit(entries: u64, size: usize, max: usize) -> u64 {
    let fit = u128::from(entries) * max as u128 / size.max(1) as u128;
    u64::try_from(fit)
        .unwrap_or(u64::MAX)
        .clamp(1, entries.saturating_sub(1).max(1))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Cursor;
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::metrics::RaftMetrics;
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::VoteRequest;
    use openraft::{Membership, Snapshot, SnapshotMeta, StoredMembership, Vote};
    use tokio::sync::watch;
    use weida::{Alpn, ClientTls, Fingerprint, Identity, Runtime, RuntimeConfig, Trust};

    use super::{Network, entries_that_fit};
    use crate::{Consensus, ConsensusConfig, MemoryStore, Node, NodeId, TypeConfig};

    fn loopback() -> ConsensusConfig {
        ConsensusConfig::new("127.0.0.1:0".parse().expect("loopback address"))
    }

    /// A node that is named and not there.
    fn node(n: u8) -> Node {
        Node::new(format!(
            "weida://{}@127.0.0.1:9/raft",
            Fingerprint::from_bytes([n; 32])
        ))
        .expect("a well-formed node")
    }

    /// A dialling half with a fresh identity, and the metrics it is told.
    fn network(
        runtime: &Runtime,
        config: ConsensusConfig,
    ) -> (Network, NodeId, watch::Sender<RaftMetrics<NodeId, Node>>) {
        let identity = Identity::generate().expect("identity");
        let id = NodeId::from(identity.fingerprint().expect("fingerprint"));
        let network = Network::new(
            id,
            runtime.clone(),
            ClientTls::new(Trust::by_address())
                .with_identity(identity)
                .with_alpn(Alpn::Raft),
            Arc::new(config),
        );
        let (metrics, receiver) = watch::channel(RaftMetrics::new_initial(id));
        network.attach(receiver);
        (network, id, metrics)
    }

    /// `metrics` as they are when the group lists exactly `listed`.
    fn listing(metrics: &watch::Sender<RaftMetrics<NodeId, Node>>, listed: &[u8]) {
        let nodes: BTreeMap<NodeId, Node> = listed
            .iter()
            .map(|n| (NodeId([*n; 32]), node(*n)))
            .collect();
        let voters: BTreeSet<NodeId> = nodes.keys().copied().collect();
        metrics.send_modify(|m| {
            m.membership_config = Arc::new(StoredMembership::new(
                None,
                Membership::new(vec![voters], nodes),
            ));
        });
    }

    fn targets(network: &Network) -> BTreeSet<NodeId> {
        network
            .inner
            .targets
            .lock()
            .expect("the target map is never poisoned")
            .keys()
            .copied()
            .collect()
    }

    fn snapshot_of(bytes: usize) -> Snapshot<TypeConfig> {
        Snapshot {
            meta: SnapshotMeta::default(),
            snapshot: Box::new(Cursor::new(vec![0; bytes])),
        }
    }

    /// A learner that came and went must not keep a requester and its
    /// connection for the life of the process: making the next target drops
    /// the ones the group no longer lists, and leaves the ones it does.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_target_the_group_no_longer_lists_is_dropped_when_the_next_is_made() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let (network, _, metrics) = network(&runtime, loopback());
        listing(&metrics, &[1, 2]);

        let one = network.target(NodeId([1; 32]), &node(1)).expect("target 1");
        network.target(NodeId([2; 32]), &node(2)).expect("target 2");
        assert_eq!(
            targets(&network),
            BTreeSet::from([NodeId([1; 32]), NodeId([2; 32])])
        );

        listing(&metrics, &[1]);
        network.target(NodeId([3; 32]), &node(3)).expect("target 3");
        assert_eq!(
            targets(&network),
            BTreeSet::from([NodeId([1; 32]), NodeId([3; 32])]),
            "2 left the group and is dropped; 1 is listed and kept; 3 is the one being made"
        );
        let again = network.target(NodeId([1; 32]), &node(1)).expect("target 1");
        assert!(
            Arc::ptr_eq(&one, &again),
            "a listed target keeps its requester"
        );
    }

    /// A snapshot over the cap is not streamed: the follower would read up to
    /// its cap and refuse it, at every retry. It is refused where it is
    /// offered, before the peer is even dialled, as a failure openraft backs
    /// off from.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_snapshot_over_the_cap_is_not_streamed() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let mut config = loopback();
        config.max_snapshot_bytes = 1024;
        let (mut network, id, _metrics) = network(&runtime, config);
        let mut peer = network.new_client(NodeId([1; 32]), &node(1)).await;
        let option = RPCOption::new(Duration::from_secs(5));

        let refused = peer
            .full_snapshot(
                Vote::new_committed(1, id),
                snapshot_of(1025),
                std::future::pending(),
                option.clone(),
            )
            .await;
        match refused {
            Err(openraft::error::StreamingError::Unreachable(e)) => {
                assert!(e.to_string().contains("max_snapshot_bytes"), "{e}");
            }
            other => panic!("expected a refusal to back off from, got {other:?}"),
        }
        assert!(targets(&network).is_empty(), "the peer was dialled");

        // At the cap it is sent, which here means the peer is dialled (and is
        // not there).
        let sent = peer
            .full_snapshot(
                Vote::new_committed(1, id),
                snapshot_of(1024),
                std::future::pending(),
                option,
            )
            .await;
        assert!(sent.is_err());
        assert_eq!(targets(&network).len(), 1, "a snapshot at the cap is sent");
    }

    /// The answer a follower gives a stale leader without reading the body of
    /// its snapshot is the one thing that tells that leader to step down, and
    /// the follower stops the request half as it gives it. A snapshot larger
    /// than the stream window is still being written then, so the answer is
    /// read while the write goes on, not after it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_vote_answer_reaches_a_leader_still_writing_the_body() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let store = MemoryStore::default();
        let follower = Consensus::start(
            &runtime,
            Identity::generate().expect("identity"),
            store.clone(),
            store,
            loopback(),
        )
        .await
        .expect("start the follower");
        let (mut network, id, _metrics) = network(&runtime, loopback());
        let mut peer = network.new_client(follower.id(), follower.node()).await;
        let option = RPCOption::new(Duration::from_secs(10));

        // The follower holds a vote at term 5.
        let held = peer
            .vote(
                VoteRequest {
                    vote: Vote::new(5, id),
                    last_log_id: None,
                },
                option.clone(),
            )
            .await
            .expect("a vote")
            .vote;
        assert_eq!(held.leader_id.term, 5);

        // A snapshot under term 1, four times the default stream window.
        let answered = tokio::time::timeout(
            Duration::from_secs(10),
            peer.full_snapshot(
                Vote::new_committed(1, id),
                snapshot_of(4 << 20),
                std::future::pending(),
                option,
            ),
        )
        .await
        .expect("the exchange ended")
        .expect("the stale-vote answer was read");
        assert_eq!(answered.vote, held);

        follower.shutdown().await;
    }

    #[test]
    fn the_hint_follows_the_bytes() {
        assert_eq!(
            entries_that_fit(10, 40, 16),
            4,
            "a batch of 40 bytes into 16"
        );
        assert_eq!(entries_that_fit(64, 128, 64), 32);
    }

    #[test]
    fn the_hint_is_never_zero_and_never_the_whole_batch() {
        assert_eq!(
            entries_that_fit(10, 1_000_000, 16),
            1,
            "one entry is always sent"
        );
        assert_eq!(
            entries_that_fit(10, 17, 16),
            9,
            "the batch that was too large is not offered again"
        );
        assert_eq!(entries_that_fit(2, 17, 16), 1);
    }
}
