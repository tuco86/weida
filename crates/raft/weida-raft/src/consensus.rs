//! One node of a consensus group: the engine, its raft binding, and the
//! client operations that follow the leader.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::metrics::{RaftMetrics, WaitError};
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
        network.attach(raft.metrics());
        let ops = Arc::new(Ops {
            raft,
            id,
            max_rpc_bytes: config.max_rpc_bytes,
            catch_up: config.forward_timeout / 2,
            membership_lock: Arc::new(tokio::sync::Mutex::new(())),
        });
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
    ///
    /// Refused with [`Error::RequestTooLarge`] when the request, as one log
    /// entry in an AppendEntries, would not fit
    /// [`ConsensusConfig::max_rpc_bytes`](crate::ConsensusConfig::max_rpc_bytes):
    /// the followers refuse such an entry, so a leader that appended it could
    /// never commit it or anything behind it. A write whose answer would not
    /// fit one reply under the same limit commits and is refused with
    /// [`Error::ResponseTooLarge`], on every node alike.
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
    /// It becomes a learner first and catches up, for at most half of
    /// [`ConsensusConfig::forward_timeout`](crate::ConsensusConfig::forward_timeout).
    /// Then every learner that is replicating — this one, and any parked
    /// earlier — is made a voter, unless that would make exactly two voters, a
    /// group that tolerates no failure and doubles the quorum. The answer is
    /// the role this node ended in: [`Role::Learner`] means it was not
    /// promoted, because of that rule or because it had not caught up yet, and
    /// calling `add_node` again for the same node promotes it once it has.
    ///
    /// A learner that never answers is never made a voter: a voter that is
    /// not there raises the quorum the group needs without lowering what it
    /// can reach.
    ///
    /// Refused with [`Error::TooManyNodes`] when the group already lists
    /// [`codec::MAX_MEMBERS`] nodes, and, on the leader, only a voter may ask.
    ///
    /// Once begun, the change runs to its end even if this future is dropped
    /// (a timeout around it, say): a change of voters is two committed
    /// entries, and abandoning it between them would leave the group in a
    /// joint configuration. Changes take turns, and one that finds an
    /// earlier change left unfinished finishes it first.
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
            let id = leader.id()?;
            outcome = if id == self.ops.id {
                self.ops.execute_local(&op).await
            } else {
                // A redirect names a node, and the peer that sent it is not
                // the authority on where that node lives: dialling the URL it
                // wrote would send this operation — a client's request bytes —
                // to any host a member cared to name. The address is the one
                // this node's own membership holds, and a node it does not
                // list is not a leader this node follows.
                let Some(leader) = self.ops.member(&id) else {
                    return Err(Error::NoLeader);
                };
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
    /// In the effective membership as a voter: one of the nodes that decide
    /// who belongs to the group.
    Voter,
    /// In it as a learner: it replicates the log and votes on nothing.
    Learner,
    /// Not in it, and the membership is not empty.
    Outsider,
    /// The membership is empty: this node is in no group yet.
    Uninitialized,
}

impl Admission {
    /// Voter or learner.
    pub(crate) fn is_member(self) -> bool {
        matches!(self, Admission::Voter | Admission::Learner)
    }
}

/// What the server task and the [`Consensus`] handle share.
pub(crate) struct Ops {
    pub(crate) raft: Raft<TypeConfig>,
    id: NodeId,
    /// [`ConsensusConfig::max_rpc_bytes`]: what a request may weigh as one
    /// replicated entry, less the envelope that carries it.
    max_rpc_bytes: usize,
    /// How long `add_node` waits for a node it just added to catch up before
    /// it answers that the node is still a learner.
    catch_up: Duration,
    /// Held by a change of membership from its first look at the membership
    /// to its last step, so that changes take turns. What a change checks —
    /// that the group has room for another node — and what it does are then
    /// one step to every other change, and what an earlier change left
    /// behind is seen by the next.
    membership_lock: Arc<tokio::sync::Mutex<()>>,
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
                if membership.voter_ids().any(|voter| voter == id) {
                    Admission::Voter
                } else if membership.get_node(&id).is_some() {
                    Admission::Learner
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

    /// The address this node's own membership lists for `id`.
    ///
    /// What a redirect to a leader is resolved through: a peer that says
    /// "the leader is at this URL" is naming a node, and the address this
    /// node dials for it is the one the group agreed on, not the one the peer
    /// wrote.
    pub(crate) fn member(&self, id: &NodeId) -> Option<Node> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_node(id)
            .cloned()
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
    ///
    /// Copies the operation to keep the caller's: a client operation that may
    /// still have to be forwarded afterwards needs it again. The server's
    /// path, which does not, uses [`Ops::execute`].
    pub(crate) async fn execute_local(self: &Arc<Self>, op: &ForwardOp) -> ForwardOutcome {
        self.execute(op.clone()).await
    }

    /// Carries `op` out on this node, which must lead; never forwards, and
    /// consumes the operation, so a payload of megabytes is moved into the
    /// log instead of copied.
    pub(crate) async fn execute(self: &Arc<Self>, op: ForwardOp) -> ForwardOutcome {
        match op {
            ForwardOp::Write(request) => {
                // The followers refuse an envelope over their own cap, and a
                // leader that appended such an entry would retry it for ever:
                // the entry could not commit and nothing behind it could.
                if request.0.len() + codec::ENTRY_ENVELOPE_OVERHEAD > self.max_rpc_bytes {
                    return ForwardOutcome::Failed(Error::RequestTooLarge);
                }
                match self.raft.client_write(request).await {
                    // The forwarder collects the reply under the same cap, so
                    // the leader judges it the same way: an answer a forwarder
                    // could not take is not handed to the leader's own caller
                    // either, and a write has one outcome on every node.
                    Ok(written)
                        if written.data.0.len() + codec::REPLY_ENVELOPE_OVERHEAD
                            > self.max_rpc_bytes =>
                    {
                        ForwardOutcome::Failed(Error::ResponseTooLarge)
                    }
                    Ok(written) => ForwardOutcome::Done(Done::Written(written.data)),
                    Err(e) => self.write_failed(e),
                }
            }
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
            ForwardOp::AddNode(node) => {
                self.membership_change(move |ops| async move { ops.add_node(&node).await })
                    .await
            }
            ForwardOp::RemoveNode(id) => {
                self.membership_change(move |ops| async move { ops.remove_node(&id).await })
                    .await
            }
        }
    }

    /// Carries a membership change out **to its end**, whether or not whoever
    /// asked keeps waiting.
    ///
    /// A change of voters is two committed entries, the joint configuration
    /// and then the uniform one, and openraft leaves the second to its
    /// caller: a future dropped between them — the forwarder's timeout, an
    /// application's — leaves the group in the joint configuration, where every
    /// commit needs a majority of both. So the change runs in a task of its
    /// own, and a caller that stops waiting abandons the answer and not the
    /// change. The turn is waited for here, in the caller's own future, so a
    /// caller that gives up while it waits starts nothing; once it has the
    /// turn it is the only change there is, and so the only task there can
    /// be.
    async fn membership_change<F, Fut>(self: &Arc<Self>, change: F) -> ForwardOutcome
    where
        F: FnOnce(Arc<Ops>) -> Fut + Send + 'static,
        Fut: Future<Output = ForwardOutcome> + Send + 'static,
    {
        let turn = Arc::clone(&self.membership_lock).lock_owned().await;
        let ops = Arc::clone(self);
        let task = tokio::spawn(async move {
            let _turn = turn;
            change(ops).await
        });
        match task.await {
            Ok(outcome) => outcome,
            Err(e) => ForwardOutcome::Failed(Error::Raft(format!(
                "a membership change ended without an answer: {e}"
            ))),
        }
    }

    /// The effective membership, read from the engine's own state.
    ///
    /// What a change has to start from. The metrics channel trails the
    /// engine, so a change decided on what it last said can start from a
    /// membership that has already moved.
    async fn effective(&self) -> Result<Membership<NodeId, Node>, ForwardOutcome> {
        self.raft
            .with_raft_state(|st| st.membership_state.effective().membership().clone())
            .await
            .map_err(|e| ForwardOutcome::Failed(raft_error(e)))
    }

    /// The effective membership once any change of voters that was begun has
    /// been ended.
    ///
    /// Under a joint configuration the group looks uniform — a node is a
    /// voter if either configuration lists it — and every commit quietly
    /// needs a majority of both. A change that was begun and never ended, by
    /// a leader that was replaced or a process that stopped between the two
    /// entries, would stay that way until some unrelated change moved it on,
    /// so whoever finds it finishes it first.
    async fn settled(&self) -> Result<Membership<NodeId, Node>, ForwardOutcome> {
        let membership = self.effective().await?;
        let configs = membership.get_joint_config();
        if configs.len() < 2 {
            return Ok(membership);
        }
        let last = configs.last().cloned().unwrap_or_default();
        tracing::info!("finishing a membership change that was begun and not ended");
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(last), false)
            .await
            .map_err(|e| self.write_failed(e))?;
        self.effective().await
    }

    async fn add_node(&self, node: &Node) -> ForwardOutcome {
        if let Some(redirect) = self.not_leader() {
            return redirect;
        }
        let id = match node.id() {
            Ok(id) => id,
            Err(e) => return ForwardOutcome::Failed(e),
        };
        let membership = match self.settled().await {
            Ok(membership) => membership,
            Err(outcome) => return outcome,
        };
        if membership.voter_ids().any(|voter| voter == id) {
            return ForwardOutcome::Done(Done::Added(Role::Voter));
        }
        // A follower decodes at most `MAX_MEMBERS` nodes in a membership. A
        // leader that appended a larger one would have it refused by every
        // follower for ever, and could not read it back from its own store
        // after a restart. The count is the engine's own and the turn is
        // held, so no other change adds a node between this look and the
        // `add_learner` below.
        if membership.get_node(&id).is_none() && membership.nodes().count() >= codec::MAX_MEMBERS {
            return ForwardOutcome::Failed(Error::TooManyNodes);
        }
        if let Err(e) = self.raft.add_learner(id, node.clone(), false).await {
            return self.write_failed(e);
        }
        // The wait is this one and not `add_learner`'s own blocking mode,
        // which gives up after half a second and returns as if the node had
        // caught up: whether it is replicating is a question for the engine's
        // own metrics, asked for as long as `catch_up` allows.
        let threshold = self.raft.config().replication_lag_threshold;
        let _ = self
            .raft
            .wait(Some(self.catch_up))
            .metrics(
                |metrics| replicating(metrics, &id, threshold),
                "a new learner replicates",
            )
            .await;

        // Only nodes that answer may vote. A learner that never caught up is a
        // node that was named and is not there, and a voter that is not there
        // raises the quorum the group has to reach without lowering what it
        // can: with enough of them no write commits and no change completes.
        let membership = match self.effective().await {
            Ok(membership) => membership,
            Err(outcome) => return outcome,
        };
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
        let metrics = self.raft.metrics().borrow().clone();
        let Some(voters) = voters_after_add(&voters, &learners, |learner| {
            replicating(&metrics, learner, threshold)
        }) else {
            return ForwardOutcome::Done(Done::Added(Role::Learner));
        };
        let role = if voters.contains(&id) {
            Role::Voter
        } else {
            Role::Learner
        };
        match self
            .raft
            .change_membership(ChangeMembers::ReplaceAllVoters(voters), false)
            .await
        {
            Ok(_) => ForwardOutcome::Done(Done::Added(role)),
            Err(e) => self.write_failed(e),
        }
    }

    async fn remove_node(&self, id: &NodeId) -> ForwardOutcome {
        if let Some(redirect) = self.not_leader() {
            return redirect;
        }
        let membership = match self.settled().await {
            Ok(membership) => membership,
            Err(outcome) => return outcome,
        };
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

/// Is the leader replicating to `id` closely enough for it to vote?
///
/// The leader's view of what the node has matched is within `threshold`
/// entries of the end of the log — the distance openraft's own blocking
/// `add_learner` calls "ready to join the cluster, as a voter" — **and the
/// node has matched something**. The second half is what the first leaves out:
/// on a log shorter than the threshold every node is "close", a node that was
/// named and never reached included, so a distance alone would promote it.
fn replicating(metrics: &RaftMetrics<NodeId, Node>, id: &NodeId, threshold: u64) -> bool {
    // Entries are counted from one, so a log of one entry is one behind a node
    // that has matched none.
    fn next_index(index: Option<u64>) -> u64 {
        index.map_or(0, |index| index + 1)
    }
    let Some(replication) = &metrics.replication else {
        return false;
    };
    let Some(Some(matched)) = replication.get(id) else {
        return false;
    };
    let matched = next_index(Some(matched.index));
    next_index(metrics.last_log_index).saturating_sub(matched) <= threshold
}

/// The voters of the group after one node was added, or `None` when the
/// membership should stay as it is.
///
/// The current voters, plus every learner that is replicating — the node just
/// added and any that were parked behind the two-voter rule, which is how a
/// third member makes the second a voter. Unchanged when no learner is ready,
/// and when the result would be exactly two voters: a group that tolerates no
/// failure and doubles the quorum, so the second member stays a learner until
/// a third arrives.
fn voters_after_add(
    voters: &BTreeSet<NodeId>,
    learners: &BTreeSet<NodeId>,
    replicating: impl Fn(&NodeId) -> bool,
) -> Option<BTreeSet<NodeId>> {
    let mut after = voters.clone();
    after.extend(learners.iter().filter(|learner| replicating(learner)));
    (after.len() != voters.len() && after.len() != 2).then_some(after)
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
    if config.max_rpc_bytes < codec::MIN_RPC_BYTES {
        return Err(Error::Config(format!(
            "max_rpc_bytes {} is below the {} a membership of {} nodes needs to be replicated",
            config.max_rpc_bytes,
            codec::MIN_RPC_BYTES,
            codec::MAX_MEMBERS
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
    use std::collections::BTreeSet;
    use std::fmt::Debug;
    use std::ops::RangeBounds;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine};
    use openraft::{
        ChangeMembers, Entry, EntryPayload, LogId, RaftLogReader, Snapshot, SnapshotMeta,
        StorageError, StorageIOError, StoredMembership, Vote,
    };
    use tokio::time::timeout;
    use weida::{Fingerprint, Identity, Runtime, RuntimeConfig};

    use crate::{
        Consensus, ConsensusConfig, Error, MemoryStore, Node, NodeId, Request, Response, TypeConfig,
    };

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
    #[tokio::test(start_paused = true)]
    async fn a_store_failure_stops_the_engine_and_stopped_fires() {
        let net = weida::SimNet::new();
        let runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(&net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while consensus.leader() != Some(consensus.id()) {
            assert!(tokio::time::Instant::now() < deadline, "the node never led");
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

    fn quick(listen: &str) -> ConsensusConfig {
        let mut config = ConsensusConfig::new(listen.parse().expect("loopback address"));
        config.heartbeat_interval = Duration::from_millis(100);
        config.election_timeout_min = Duration::from_millis(300);
        config.election_timeout_max = Duration::from_millis(600);
        config.forward_timeout = Duration::from_secs(1);
        config
    }

    fn listen_of(node: &Node) -> String {
        let authority = node.url.split('@').nth(1).expect("an authority");
        authority.split('/').next().expect("a host").to_owned()
    }

    /// How many configs the node's effective membership holds: two while a
    /// change of voters is between its entries.
    async fn configs(consensus: &Consensus) -> usize {
        consensus
            .ops
            .raft
            .with_raft_state(|st| {
                st.membership_state
                    .effective()
                    .membership()
                    .get_joint_config()
                    .len()
            })
            .await
            .expect("the engine runs")
    }

    /// A one-member group whose only member leads.
    async fn leader(runtime: &Runtime) -> Consensus {
        let store = MemoryStore::default();
        let a = Consensus::start(
            runtime,
            Identity::generate().expect("identity"),
            store.clone(),
            store,
            quick("127.0.0.1:0"),
        )
        .await
        .expect("start");
        a.bootstrap().await.expect("bootstrap");
        let deadline = Instant::now() + Duration::from_secs(10);
        while a.leader() != Some(a.id()) {
            assert!(Instant::now() < deadline, "the node never led");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        a
    }

    /// What a stopped node needs to come back as it was.
    struct Gone {
        identity: Identity,
        store: MemoryStore,
        listen: String,
    }

    /// A learner of `leader`, added without the promotion `add_node` would
    /// make, that replicates, and is then stopped. The leader still counts it
    /// as replicating: what it knows is what the learner last matched.
    async fn a_learner_that_replicated_and_left(net: &weida::SimNet, leader: &Consensus) -> Gone {
        let identity = Identity::generate().expect("identity");
        let store = MemoryStore::default();
        let node_runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let node = Consensus::start(
            &node_runtime,
            identity.clone(),
            store.clone(),
            store.clone(),
            quick("127.0.0.1:0"),
        )
        .await
        .expect("start");
        leader
            .ops
            .raft
            .add_learner(node.id(), node.node().clone(), false)
            .await
            .expect("added as a learner");
        let id = node.id();
        leader
            .ops
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| super::replicating(metrics, &id, 1000),
                "a learner replicates",
            )
            .await
            .expect("the learner replicates");
        let listen = listen_of(node.node());
        node.shutdown().await;
        node_runtime.shutdown().await;
        Gone {
            identity,
            store,
            listen,
        }
    }

    /// The node that stopped, started again on its port.
    async fn comes_back(gone: &Gone, runtime: &Runtime) -> Consensus {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            // The port of the node that stopped may take a moment to be free.
            match Consensus::start(
                runtime,
                gone.identity.clone(),
                gone.store.clone(),
                gone.store.clone(),
                quick(&gone.listen),
            )
            .await
            {
                Ok(back) => return back,
                Err(e) => {
                    assert!(Instant::now() < deadline, "the node never came back: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// A change of voters is two committed entries, and a caller that stops
    /// waiting between them must not leave the group in the joint
    /// configuration. Here the new voters cannot commit the first entry —
    /// two learners that were replicating are gone — and the caller gives up;
    /// once they are back the change must end uniform without anybody asking
    /// again.
    #[tokio::test(start_paused = true)]
    async fn a_change_of_voters_whose_caller_gave_up_still_ends_uniform() {
        let net = weida::SimNet::new();
        let runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(&net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let a = leader(&runtime).await;

        let first = a_learner_that_replicated_and_left(&net, &a).await;
        let second = a_learner_that_replicated_and_left(&net, &a).await;

        // A third node that never answers, so the call has something to add:
        // it promotes the two that are gone and waits for the first entry to
        // commit, which it cannot.
        let never = Node::new(format!(
            "weida://{}@127.0.0.1:9/raft",
            Fingerprint::from_bytes([9; 32])
        ))
        .expect("a well-formed node");
        let asked = timeout(Duration::from_millis(2500), a.add_node(never)).await;
        assert!(
            asked.is_err(),
            "the change cannot commit, so it was still waiting"
        );
        assert_eq!(configs(&a).await, 2, "the joint configuration is appended");

        // They come back; nobody asks again.
        let back_runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(&net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let back = comes_back(&first, &back_runtime).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while configs(&a).await != 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the change that its caller gave up on was never ended"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        drop(second);
        back.shutdown().await;
        a.shutdown().await;
    }

    /// A group left in a joint configuration — by a leader that was replaced
    /// or a process that stopped between the two entries of a change, or by
    /// anything that drove the engine directly — looks uniform to every
    /// reader and quietly needs a majority of both configs for every commit.
    /// The next membership change finds it and ends it first: here a
    /// removal of a node that is not a member, which changes nothing of its
    /// own.
    #[tokio::test(start_paused = true)]
    async fn the_next_change_ends_a_joint_configuration_it_finds() {
        let net = weida::SimNet::new();
        let runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(&net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let a = leader(&runtime).await;
        let gone = a_learner_that_replicated_and_left(&net, &a).await;

        // The first entry of a change that makes the learner a voter; it
        // cannot commit with the learner away, and the caller leaves before
        // the second entry is proposed, as openraft's own API allows.
        let gone_id = NodeId::from(gone.identity.fingerprint().expect("fingerprint"));
        let both = BTreeSet::from([a.id(), gone_id]);
        let left = timeout(
            Duration::from_secs(1),
            a.ops
                .raft
                .change_membership(ChangeMembers::ReplaceAllVoters(both), false),
        )
        .await;
        assert!(left.is_err(), "the first entry cannot commit yet");
        assert_eq!(configs(&a).await, 2, "the joint configuration is appended");

        // The learner is back and the first entry commits; nothing proposes
        // the second.
        let back_runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(&net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let back = comes_back(&gone, &back_runtime).await;
        let joint = a.ops.raft.metrics().borrow().last_log_index;
        a.ops
            .raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(joint, "the joint configuration is committed")
            .await
            .expect("the first entry commits once the learner is back");
        assert_eq!(configs(&a).await, 2, "nothing but the next change ends it");

        timeout(Duration::from_secs(10), a.remove_node(NodeId([77; 32])))
            .await
            .expect("the removal finished")
            .expect("removing a node that is not a member succeeds");
        assert_eq!(configs(&a).await, 1, "the joint configuration was ended");
        let voters = a
            .ops
            .raft
            .with_raft_state(|st| {
                st.membership_state
                    .effective()
                    .voter_ids()
                    .collect::<BTreeSet<_>>()
            })
            .await
            .expect("the engine runs");
        assert_eq!(
            voters.len(),
            2,
            "the change that was begun is the one ended"
        );

        back.shutdown().await;
        a.shutdown().await;
    }

    /// [`MemoryStore`] as a state machine whose answer to the request `big`
    /// is `answer` bytes long.
    #[derive(Clone)]
    struct Chatty {
        store: MemoryStore,
        answer: usize,
    }

    impl RaftStateMachine<TypeConfig> for Chatty {
        type SnapshotBuilder = MemoryStore;

        async fn applied_state(
            &mut self,
        ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>>
        {
            self.store.applied_state().await
        }

        async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
        where
            I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        {
            let entries: Vec<_> = entries.into_iter().collect();
            let big: Vec<bool> = entries
                .iter()
                .map(|entry| matches!(&entry.payload, EntryPayload::Normal(r) if r.0 == b"big"))
                .collect();
            let mut answers = self.store.apply(entries).await?;
            for (answer, big) in answers.iter_mut().zip(big) {
                if big {
                    answer.0 = vec![7; self.answer];
                }
            }
            Ok(answers)
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            self.store.get_snapshot_builder().await
        }

        async fn begin_receiving_snapshot(
            &mut self,
        ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
            self.store.begin_receiving_snapshot().await
        }

        async fn install_snapshot(
            &mut self,
            meta: &SnapshotMeta<NodeId, Node>,
            snapshot: Box<std::io::Cursor<Vec<u8>>>,
        ) -> Result<(), StorageError<NodeId>> {
            self.store.install_snapshot(meta, snapshot).await
        }

        async fn get_current_snapshot(
            &mut self,
        ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
            self.store.get_current_snapshot().await
        }
    }

    /// A write whose answer would not fit the reply the forwarder collects
    /// has one outcome on every node: the leader judges the answer by the
    /// forwarder's cap, so its own caller is refused as a forwarded one is,
    /// where it used to return the answer to itself and fail the follower
    /// with an error that read as a transport failure.
    #[tokio::test(start_paused = true)]
    async fn an_answer_a_forwarder_could_not_take_is_refused_on_every_node() {
        let net = weida::SimNet::new();
        let runtime = Runtime::new(RuntimeConfig {
            network: weida::Network::sim(&net),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let node = |runtime: &Runtime| {
            let runtime = runtime.clone();
            async move {
                let mut config = quick("127.0.0.1:0");
                config.max_rpc_bytes = crate::codec::MIN_RPC_BYTES;
                let store = MemoryStore::default();
                let state = Chatty {
                    store: store.clone(),
                    answer: crate::codec::MIN_RPC_BYTES + 1,
                };
                Consensus::start(
                    &runtime,
                    Identity::generate().expect("identity"),
                    store,
                    state,
                    config,
                )
                .await
                .expect("start")
            }
        };
        let a = node(&runtime).await;
        a.bootstrap().await.expect("bootstrap");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while a.leader() != Some(a.id()) {
            assert!(tokio::time::Instant::now() < deadline, "the node never led");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let b = node(&runtime).await;
        a.add_node(b.node().clone()).await.expect("add the learner");
        while b.leader() != Some(a.id()) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the learner never saw the leader"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        for (name, node) in [("the leader", &a), ("a forwarder", &b)] {
            let refused = node.write(Request(b"big".to_vec())).await;
            assert!(
                matches!(refused, Err(Error::ResponseTooLarge)),
                "{name}: {refused:?}"
            );
            node.write(Request(b"small".to_vec()))
                .await
                .unwrap_or_else(|e| panic!("{name}: the group still answers: {e}"));
        }

        b.shutdown().await;
        a.shutdown().await;
    }
}
