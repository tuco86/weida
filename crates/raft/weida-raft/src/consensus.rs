//! One node of a consensus group: the engine, its raft binding, and the
//! client operations that follow the leader.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::metrics::WaitError;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{ChangeMembers, Membership, Raft, SnapshotPolicy};
use tokio::sync::Semaphore;
use weida::{
    Alpn, Binding, ClientTls, ClientTrust, Fingerprint, IdentitySource, Listener, Runtime,
    ServerTls, Trust,
};

use crate::codec::{self, Rpc};
use crate::network::{Done, ForwardOp, ForwardOutcome, Network};
use crate::{
    ConsensusConfig, Error, Node, NodeId, RAFT_PATH, Request, Response, Role, TypeConfig, server,
};

/// How often a client operation follows a leader that moved before it gives
/// up with [`Error::NoLeader`].
const MAX_FORWARD_HOPS: usize = 3;

/// One node of a consensus group.
///
/// Built with [`Consensus::start`] **inside the runtime's own `Exec`** (the
/// construction-site rule of the crate docs). Every client operation can be
/// called on any node: a follower forwards it to the leader over the raft
/// binding and returns the leader's answer.
pub struct Consensus {
    ops: Arc<Ops>,
    node: Node,
    network: Network,
    config: Arc<ConsensusConfig>,
    _listener: Listener,
    binding: Binding,
    server: tokio::task::JoinHandle<()>,
}

impl fmt::Debug for Consensus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consensus")
            .field("id", &self.ops.id)
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl Consensus {
    /// Binds the raft port and starts the engine over the application's log
    /// store and state machine.
    ///
    /// The node's id is the fingerprint of `identity`; its URL is
    /// `weida://<id>@<authority>/raft`, the authority being
    /// [`ConsensusConfig::advertise`] or the bound address. A fresh node is
    /// a member of nothing until it calls [`Consensus::bootstrap`] or a
    /// member adds it with [`Consensus::add_node`]; a restarted one resumes
    /// from what its stores hold.
    ///
    /// **Call this inside the runtime's own `Exec`**: openraft spawns its
    /// tasks on the ambient runtime.
    pub async fn start<LS, SM>(
        runtime: &Runtime,
        identity: impl Into<IdentitySource>,
        log: LS,
        state: SM,
        config: ConsensusConfig,
    ) -> Result<Consensus, Error>
    where
        LS: RaftLogStorage<TypeConfig>,
        SM: RaftStateMachine<TypeConfig>,
    {
        let identity = identity.into();
        let id = NodeId::from(identity.fingerprint()?);
        if id.is_zero() {
            return Err(Error::InvalidNode(
                "the identity's fingerprint is all-zero".to_owned(),
            ));
        }
        let raft_config = engine_config(&config)?;
        let config = Arc::new(config);

        let listener = runtime.listener();
        let replier = listener.replier(RAFT_PATH)?;
        // `AnyKey` proves possession; the server admits by membership plus
        // the identity rule.
        let binding = listener
            .bind_quic(
                config.listen,
                ServerTls::new(identity.clone())
                    .require_client(ClientTrust::AnyKey)
                    .with_alpn(Alpn::Raft),
            )
            .await?;
        let bound = binding.local_addr();
        let authority = match &config.advertise {
            Some(authority) => authority.clone(),
            None if bound.ip().is_unspecified() => {
                return Err(Error::Config(
                    "listen address is unspecified; set ConsensusConfig::advertise".to_owned(),
                ));
            }
            None => bound.to_string(),
        };
        let node = Node::new(format!(
            "weida://{}@{authority}{RAFT_PATH}",
            Fingerprint::from(id)
        ))?;

        let network = Network::new(
            id,
            runtime.clone(),
            ClientTls::new(Trust::by_address())
                .with_identity(identity)
                .with_alpn(Alpn::Raft),
            Arc::clone(&config),
        );
        let raft = Raft::new(id, Arc::new(raft_config), network.clone(), log, state)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        let ops = Arc::new(Ops { raft, id });
        let server = runtime.exec().spawn(server::serve(
            replier,
            Arc::clone(&ops),
            Arc::clone(&config),
        ));
        Ok(Consensus {
            ops,
            node,
            network,
            config,
            _listener: listener,
            binding,
            server,
        })
    }

    /// Makes this node a one-member group, if it is not a member of one yet.
    ///
    /// Idempotent: a node that is already initialized — a restarted one, or
    /// one another member added — answers `Ok(())` and changes nothing.
    ///
    /// A node that is to join an existing group must not bootstrap: once it
    /// is a group of its own it refuses every other group's leader.
    pub async fn bootstrap(&self) -> Result<(), Error> {
        let raft = &self.ops.raft;
        if raft.is_initialized().await.map_err(raft_error)? {
            return Ok(());
        }
        match raft
            .initialize(BTreeMap::from([(self.ops.id, self.node.clone())]))
            .await
        {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(e) => Err(raft_error(e)),
        }
    }

    /// Commits `request` and returns what the state machine answered.
    pub async fn write(&self, request: Request) -> Result<Response, Error> {
        match self.on_leader(ForwardOp::Write(request)).await? {
            Done::Written(response) => Ok(response),
            other => Err(unexpected(other)),
        }
    }

    /// Returns once this node's state machine has applied everything the
    /// leader had committed when the barrier was asked for: a read after it
    /// sees every write acknowledged before it.
    pub async fn read_barrier(&self) -> Result<(), Error> {
        let read = match self.on_leader(ForwardOp::ReadIndex).await? {
            Done::ReadIndex(read) => read,
            other => return Err(unexpected(other)),
        };
        match self
            .ops
            .raft
            .wait(Some(self.config.forward_timeout))
            .applied_index_at_least(read.map(|log_id| log_id.index), "read_barrier")
            .await
        {
            Ok(_) => Ok(()),
            Err(WaitError::Timeout(..)) => Err(Error::Timeout),
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Adds `node` to the group.
    ///
    /// It becomes a learner first and catches up; then every member is made
    /// a voter, unless that would make exactly two voters — a group that
    /// tolerates no failure and doubles the quorum — in which case it stays
    /// a [`Role::Learner`] until a third member arrives.
    pub async fn add_node(&self, node: Node) -> Result<Role, Error> {
        match self.on_leader(ForwardOp::AddNode(node)).await? {
            Done::Added(role) => Ok(role),
            other => Err(unexpected(other)),
        }
    }

    /// Removes the node `id` from the group.
    ///
    /// Refused with [`Error::TwoVoters`] when exactly two voters would be
    /// left, and with [`Error::LastVoter`] for the last one. Removing a node
    /// that is not a member succeeds.
    pub async fn remove_node(&self, id: NodeId) -> Result<(), Error> {
        match self.on_leader(ForwardOp::RemoveNode(id)).await? {
            Done::Removed => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// The leader this node knows of.
    pub fn leader(&self) -> Option<NodeId> {
        self.ops.raft.metrics().borrow().current_leader
    }

    /// Resolves once the engine has stopped on its own: a storage error openraft
    /// treats as fatal, or a panic in its core task. Never resolves while it runs;
    /// `shutdown` consumes the handle, so a caller only sees an engine that ended
    /// unasked. Select it against the application's work and exit on it, so a
    /// supervisor restarts the process.
    pub async fn stopped(&self) -> Error {
        let mut metrics = self.ops.raft.metrics();
        loop {
            if let Err(fatal) = &metrics.borrow_and_update().running_state {
                return Error::Raft(fatal.to_string());
            }
            if metrics.changed().await.is_err() {
                return Error::Raft(
                    "the engine's core task ended without reporting why".to_owned(),
                );
            }
        }
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.ops.id
    }

    /// This node's address, as another member adds it.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Stops the engine and closes the raft binding.
    pub async fn shutdown(self) {
        if let Err(e) = self.ops.raft.shutdown().await {
            tracing::warn!(error = %e, "the consensus engine did not stop cleanly");
        }
        self.server.abort();
        self.binding.close().await;
    }

    /// Carries `op` out where the leader is: here, or forwarded.
    async fn on_leader(&self, op: ForwardOp) -> Result<Done, Error> {
        let mut outcome = self.ops.execute_local(&op).await;
        // Encoded once, on the first forward.
        let mut header = None;
        for _ in 0..MAX_FORWARD_HOPS {
            let leader = match outcome {
                ForwardOutcome::Done(done) => return Ok(done),
                ForwardOutcome::Failed(e) => return Err(e),
                ForwardOutcome::ForwardToLeader(None) => return Err(Error::NoLeader),
                ForwardOutcome::ForwardToLeader(Some(leader)) => leader,
            };
            outcome = if leader.id()? == self.ops.id {
                self.ops.execute_local(&op).await
            } else {
                let header =
                    header.get_or_insert_with(|| codec::encode_rpc(&Rpc::Forward(op.clone())));
                self.network.forward(&leader, header).await?
            };
        }
        match outcome {
            ForwardOutcome::Done(done) => Ok(done),
            ForwardOutcome::Failed(e) => Err(e),
            ForwardOutcome::ForwardToLeader(_) => Err(Error::NoLeader),
        }
    }
}

/// Where a proved key stands with this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// In the effective membership, as a voter or a learner.
    Member,
    /// Not in it, and the membership is not empty.
    Outsider,
    /// The membership is empty: this node is in no group yet.
    Uninitialized,
}

/// What the server task and the [`Consensus`] handle share.
pub(crate) struct Ops {
    pub(crate) raft: Raft<TypeConfig>,
    id: NodeId,
}

impl Ops {
    /// Where `id` stands with this node, read from the engine's own state.
    ///
    /// Not from the metrics channel: on a restarted node it starts with
    /// `RaftMetrics::new_initial`, an empty membership, until the core first
    /// reports, so a metrics-based check would admit everyone in that window.
    /// And not openraft's own notion of "initialized": `RaftState::is_initialized`
    /// is crate-private and also counts a node that merely saw a vote, which
    /// would make a fresh joiner refuse the leader that adds it. Here a node
    /// is uninitialized exactly while its effective membership is empty.
    pub(crate) async fn admission(&self, id: NodeId) -> Result<Admission, Box<Fatal<NodeId>>> {
        self.raft
            .with_raft_state(move |st| {
                let membership = st.membership_state.effective();
                if membership.get_node(&id).is_some() {
                    Admission::Member
                } else if membership.nodes().next().is_none() {
                    Admission::Uninitialized
                } else {
                    Admission::Outsider
                }
            })
            .await
            .map_err(Box::new)
    }

    fn membership(&self) -> Membership<NodeId, Node> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .clone()
    }

    /// `Some(redirect)` unless this node leads.
    fn not_leader(&self) -> Option<ForwardOutcome> {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        match metrics.current_leader {
            Some(leader) if leader == self.id => None,
            Some(leader) => Some(ForwardOutcome::ForwardToLeader(
                metrics
                    .membership_config
                    .membership()
                    .get_node(&leader)
                    .cloned(),
            )),
            None => Some(ForwardOutcome::ForwardToLeader(None)),
        }
    }

    /// The leader's node, from what openraft said or from the membership.
    fn leader_node(&self, id: Option<NodeId>, node: Option<Node>) -> ForwardOutcome {
        ForwardOutcome::ForwardToLeader(
            node.or_else(|| id.and_then(|id| self.membership().get_node(&id).cloned())),
        )
    }

    fn write_failed(&self, e: RaftError<NodeId, ClientWriteError<NodeId, Node>>) -> ForwardOutcome {
        match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
                self.leader_node(f.leader_id, f.leader_node)
            }
            e => ForwardOutcome::Failed(raft_error(e)),
        }
    }

    /// Carries `op` out on this node, which must lead; never forwards.
    pub(crate) async fn execute_local(&self, op: &ForwardOp) -> ForwardOutcome {
        match op {
            ForwardOp::Write(request) => match self.raft.client_write(request.clone()).await {
                Ok(written) => ForwardOutcome::Done(Done::Written(written.data)),
                Err(e) => self.write_failed(e),
            },
            ForwardOp::ReadIndex => match self.raft.get_read_log_id().await {
                Ok((read, _applied)) => ForwardOutcome::Done(Done::ReadIndex(read)),
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                    self.leader_node(f.leader_id, f.leader_node)
                }
                Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {
                    ForwardOutcome::Failed(Error::NoLeader)
                }
                Err(e) => ForwardOutcome::Failed(raft_error(e)),
            },
            ForwardOp::AddNode(node) => self.add_node(node).await,
            ForwardOp::RemoveNode(id) => self.remove_node(id).await,
        }
    }

    async fn add_node(&self, node: &Node) -> ForwardOutcome {
        if let Some(redirect) = self.not_leader() {
            return redirect;
        }
        let id = match node.id() {
            Ok(id) => id,
            Err(e) => return ForwardOutcome::Failed(e),
        };
        let membership = self.membership();
        if membership.voter_ids().any(|voter| voter == id) {
            return ForwardOutcome::Done(Done::Added(Role::Voter));
        }
        let mut members: BTreeSet<NodeId> = membership.nodes().map(|(id, _)| *id).collect();
        members.insert(id);
        if let Err(e) = self.raft.add_learner(id, node.clone(), true).await {
            return self.write_failed(e);
        }
        // Two voters tolerate no failure and need both for a quorum: the
        // second member is parked as a learner until a third arrives.
        if members.len() == 2 {
            return ForwardOutcome::Done(Done::Added(Role::Learner));
        }
        match self
            .raft
            .change_membership(ChangeMembers::ReplaceAllVoters(members), false)
            .await
        {
            Ok(_) => ForwardOutcome::Done(Done::Added(Role::Voter)),
            Err(e) => self.write_failed(e),
        }
    }

    async fn remove_node(&self, id: &NodeId) -> ForwardOutcome {
        if let Some(redirect) = self.not_leader() {
            return redirect;
        }
        let membership = self.membership();
        let mut voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let change = if voters.remove(id) {
            if voters.is_empty() {
                return ForwardOutcome::Failed(Error::LastVoter);
            }
            if voters.len() == 2 {
                return ForwardOutcome::Failed(Error::TwoVoters);
            }
            ChangeMembers::ReplaceAllVoters(voters)
        } else if membership.get_node(id).is_some() {
            ChangeMembers::RemoveNodes(BTreeSet::from([*id]))
        } else {
            return ForwardOutcome::Done(Done::Removed);
        };
        match self.raft.change_membership(change, false).await {
            Ok(_) => ForwardOutcome::Done(Done::Removed),
            Err(e) => self.write_failed(e),
        }
    }
}

/// openraft's configuration from ours.
fn engine_config(config: &ConsensusConfig) -> Result<openraft::Config, Error> {
    if config.max_payload_entries > codec::MAX_ENTRIES {
        return Err(Error::Config(format!(
            "max_payload_entries {} exceeds the codec's limit of {}",
            config.max_payload_entries,
            codec::MAX_ENTRIES
        )));
    }
    if config.max_concurrent_rpcs == 0 || config.max_concurrent_rpcs > Semaphore::MAX_PERMITS {
        return Err(Error::Config(format!(
            "max_concurrent_rpcs must be between 1 and {}",
            Semaphore::MAX_PERMITS
        )));
    }
    openraft::Config {
        cluster_name: config.cluster_name.clone(),
        heartbeat_interval: millis(config.heartbeat_interval)?,
        election_timeout_min: millis(config.election_timeout_min)?,
        election_timeout_max: millis(config.election_timeout_max)?,
        install_snapshot_timeout: millis(config.install_snapshot_timeout)?,
        max_payload_entries: config.max_payload_entries,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last),
        max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
        ..Default::default()
    }
    .validate()
    .map_err(|e| Error::Config(e.to_string()))
}

fn millis(d: Duration) -> Result<u64, Error> {
    u64::try_from(d.as_millis()).map_err(|_| Error::Config(format!("{d:?} is too long")))
}

fn raft_error(e: impl fmt::Display) -> Error {
    Error::Raft(e.to_string())
}

fn unexpected(done: Done) -> Error {
    Error::Raft(format!("unexpected answer from the leader: {done:?}"))
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::ops::RangeBounds;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
    use openraft::{Entry, LogId, RaftLogReader, StorageError, StorageIOError, Vote};
    use tokio::time::timeout;
    use weida::{Identity, Runtime, RuntimeConfig};

    use crate::{Consensus, ConsensusConfig, Error, MemoryStore, NodeId, Request, TypeConfig};

    /// A log that fails every `append` once `fail` is set.
    #[derive(Clone)]
    struct FailingLog {
        store: MemoryStore,
        fail: Arc<AtomicBool>,
    }

    impl RaftLogReader<TypeConfig> for FailingLog {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
            &mut self,
            range: RB,
        ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
            self.store.try_get_log_entries(range).await
        }
    }

    impl RaftLogStorage<TypeConfig> for FailingLog {
        type LogReader = MemoryStore;

        async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
            self.store.get_log_state().await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.store.get_log_reader().await
        }

        async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
            self.store.save_vote(vote).await
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
            self.store.read_vote().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogId<NodeId>>,
        ) -> Result<(), StorageError<NodeId>> {
            self.store.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
            self.store.read_committed().await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: LogFlushed<TypeConfig>,
        ) -> Result<(), StorageError<NodeId>>
        where
            I: IntoIterator<Item = Entry<TypeConfig>> + Send,
            I::IntoIter: Send,
        {
            if self.fail.load(Ordering::SeqCst) {
                return Err(StorageError::from(StorageIOError::write_logs(
                    openraft::AnyError::new(&std::io::Error::other("injected")),
                )));
            }
            self.store.append(entries, callback).await
        }

        async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
            self.store.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
            self.store.purge(log_id).await
        }
    }

    /// `stopped` stays pending while the engine runs and fires with
    /// `Error::Raft` once a storage failure ends it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_store_failure_stops_the_engine_and_stopped_fires() {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let store = MemoryStore::default();
        let fail = Arc::new(AtomicBool::new(false));
        let log = FailingLog {
            store: store.clone(),
            fail: Arc::clone(&fail),
        };
        let consensus = Consensus::start(
            &runtime,
            Identity::generate().expect("identity"),
            log,
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
        consensus
            .write(Request(b"before".to_vec()))
            .await
            .expect("a write on a healthy engine");
        assert!(
            timeout(Duration::from_millis(200), consensus.stopped())
                .await
                .is_err(),
            "stopped fired while the engine was healthy"
        );

        fail.store(true, Ordering::SeqCst);
        let _ = timeout(
            Duration::from_secs(5),
            consensus.write(Request(b"doomed".to_vec())),
        )
        .await;
        let stopped = timeout(Duration::from_secs(10), consensus.stopped())
            .await
            .expect("stopped must fire after a store failure");
        assert!(matches!(stopped, Error::Raft(_)), "got {stopped:?}");

        consensus.shutdown().await;
    }
}
