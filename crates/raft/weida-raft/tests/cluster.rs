//! Consensus groups on loopback over the real raft transport: one weida
//! runtime, one identity and one QUIC binding per node.

use std::future::Future;
use std::time::{Duration, Instant};

use weida::{ClientTls, Identity, Runtime, RuntimeConfig, Trust};
use weida_raft::openraft::storage::RaftLogStorage;
use weida_raft::{Consensus, ConsensusConfig, Error, MemoryStore, NodeId, Request, Response, Role};

const DEADLINE: Duration = Duration::from_secs(10);

fn config() -> ConsensusConfig {
    let mut config = ConsensusConfig::new("127.0.0.1:0".parse().expect("loopback address"));
    config.heartbeat_interval = Duration::from_millis(100);
    config.election_timeout_min = Duration::from_millis(300);
    config.election_timeout_max = Duration::from_millis(600);
    config
}

/// One node: its consensus handle, its store, and what it needs to restart.
struct TestNode {
    consensus: Consensus,
    store: MemoryStore,
    identity: Identity,
    runtime: Runtime,
}

impl TestNode {
    async fn start(config: ConsensusConfig) -> TestNode {
        TestNode::restart(
            Identity::generate().expect("identity"),
            MemoryStore::default(),
            config,
        )
        .await
    }

    async fn restart(identity: Identity, store: MemoryStore, config: ConsensusConfig) -> TestNode {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let consensus = Consensus::start(
            &runtime,
            identity.clone(),
            store.clone(),
            store.clone(),
            config,
        )
        .await
        .expect("start");
        TestNode {
            consensus,
            store,
            identity,
            runtime,
        }
    }

    fn id(&self) -> NodeId {
        self.consensus.id()
    }
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + DEADLINE;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn count(n: u64) -> Response {
    Response(n.to_be_bytes().to_vec())
}

fn requests(items: &[&[u8]]) -> Vec<Request> {
    items.iter().map(|r| Request(r.to_vec())).collect()
}

/// The single leader every node in `nodes` agrees on.
async fn agreed_leader(nodes: &[&TestNode]) -> NodeId {
    eventually("every node reports the same leader", || async {
        let first = nodes[0].consensus.leader();
        first.is_some() && nodes.iter().all(|n| n.consensus.leader() == first)
    })
    .await;
    nodes[0].consensus.leader().expect("a leader")
}

async fn bootstrapped() -> TestNode {
    let node = TestNode::start(config()).await;
    node.consensus.bootstrap().await.expect("bootstrap");
    let id = node.id();
    eventually("the node leads itself", || async {
        node.consensus.leader() == Some(id)
    })
    .await;
    node
}

/// A bootstrapped A, then B (parked as a learner) and C (all voters).
async fn three_nodes() -> [TestNode; 3] {
    let a = bootstrapped().await;
    let b = TestNode::start(config()).await;
    let c = TestNode::start(config()).await;
    assert_eq!(
        a.consensus
            .add_node(b.consensus.node().clone())
            .await
            .expect("add B"),
        Role::Learner,
        "a second member is parked, not made the second voter"
    );
    assert_eq!(
        a.consensus
            .add_node(c.consensus.node().clone())
            .await
            .expect("add C"),
        Role::Voter
    );
    [a, b, c]
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_node_commits_a_write() {
    let node = bootstrapped().await;
    assert_eq!(
        node.consensus
            .write(Request(b"a".to_vec()))
            .await
            .expect("write"),
        count(1)
    );
    assert_eq!(node.store.requests().await, requests(&[b"a"]));
    node.consensus.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_node_resumes_from_its_stores() {
    let node = bootstrapped().await;
    node.consensus
        .write(Request(b"a".to_vec()))
        .await
        .expect("write");
    let TestNode {
        consensus,
        store,
        identity,
        runtime: _runtime,
    } = node;
    consensus.shutdown().await;

    let node = TestNode::restart(identity, store, config()).await;
    node.consensus
        .bootstrap()
        .await
        .expect("bootstrap is idempotent");
    let id = node.id();
    eventually("the restarted node leads again", || async {
        node.consensus.leader() == Some(id)
    })
    .await;
    assert_eq!(node.store.requests().await, requests(&[b"a"]));
    assert_eq!(
        node.consensus
            .write(Request(b"b".to_vec()))
            .await
            .expect("write after restart"),
        count(2)
    );
    assert_eq!(node.store.requests().await, requests(&[b"a", b"b"]));
    node.consensus.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_write_reaches_every_node_and_two_voters_are_refused() {
    let nodes = three_nodes().await;
    let leader = agreed_leader(&[&nodes[0], &nodes[1], &nodes[2]]).await;
    let l = nodes.iter().find(|n| n.id() == leader).expect("leader");
    let f = nodes.iter().find(|n| n.id() != leader).expect("follower");

    assert_eq!(
        f.consensus
            .write(Request(b"x".to_vec()))
            .await
            .expect("follower write"),
        count(1),
        "the leader's state machine answers a forwarded write"
    );
    for node in &nodes {
        eventually("every store holds the write", || async {
            node.store.requests().await == requests(&[b"x"])
        })
        .await;
    }

    l.consensus
        .write(Request(b"y".to_vec()))
        .await
        .expect("leader write");
    f.consensus.read_barrier().await.expect("read barrier");
    assert!(
        f.store.requests().await.contains(&Request(b"y".to_vec())),
        "a read barrier returns only after the follower applied the leader's commit"
    );

    let c = nodes[2].id();
    assert!(
        matches!(l.consensus.remove_node(c).await, Err(Error::TwoVoters)),
        "removing a voter of three is refused"
    );

    for node in nodes {
        node.consensus.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_group_survives_its_leader_being_killed() {
    let nodes = three_nodes().await;
    let old = agreed_leader(&[&nodes[0], &nodes[1], &nodes[2]]).await;
    let (dead, survivors): (Vec<TestNode>, Vec<TestNode>) =
        nodes.into_iter().partition(|n| n.id() == old);
    for node in dead {
        node.consensus.shutdown().await;
    }

    eventually("the survivors agree on a new leader", || async {
        let first = survivors[0].consensus.leader();
        first.is_some_and(|l| l != old) && survivors[1].consensus.leader() == first
    })
    .await;
    let leader = survivors[0].consensus.leader().expect("a leader");
    let l = survivors.iter().find(|n| n.id() == leader).expect("leader");
    l.consensus
        .write(Request(b"after".to_vec()))
        .await
        .expect("write under the new leader");
    for node in &survivors {
        eventually("both survivors hold the write", || async {
            node.store
                .requests()
                .await
                .contains(&Request(b"after".to_vec()))
        })
        .await;
    }

    for node in survivors {
        node.consensus.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_member_catches_up_through_a_streamed_snapshot() {
    let mut snapshotting = config();
    snapshotting.snapshot_logs_since_last = 3;
    snapshotting.max_in_snapshot_log_to_keep = 0;
    let a = TestNode::start(snapshotting).await;
    a.consensus.bootstrap().await.expect("bootstrap");
    let id = a.id();
    eventually("A leads itself", || async {
        a.consensus.leader() == Some(id)
    })
    .await;

    let written: Vec<Request> = (0u8..10).map(|i| Request(vec![i])).collect();
    for request in &written {
        a.consensus.write(request.clone()).await.expect("write");
    }
    // Only a purged prefix forces the snapshot path: entries still in the
    // log would be replicated as entries.
    eventually("A purged its log prefix", || async {
        let mut store = a.store.clone();
        store
            .get_log_state()
            .await
            .expect("log state")
            .last_purged_log_id
            .is_some()
    })
    .await;

    let b = TestNode::start(config()).await;
    assert_eq!(
        a.consensus
            .add_node(b.consensus.node().clone())
            .await
            .expect("add B"),
        Role::Learner
    );
    eventually("B holds every request", || async {
        b.store.requests().await == written
    })
    .await;

    a.consensus.shutdown().await;
    b.consensus.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_application_dial_never_reaches_the_raft_binding() {
    let node = bootstrapped().await;
    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let plain = client.requester(
        ClientTls::new(Trust::by_address())
            .with_identity(Identity::generate().expect("client identity")),
    );
    let outcome = tokio::time::timeout(DEADLINE, plain.connect(&node.consensus.node().url))
        .await
        .expect("the handshake ends within the deadline");
    assert!(
        outcome.is_err(),
        "a weida/0 dial reached the weida-raft/0 binding"
    );
    client.shutdown().await;
    node.consensus.shutdown().await;
}

/// The construction-site invariant of the crate docs: the engine is built
/// and driven inside the runtime's own `Exec`, so the tasks openraft spawns
/// land on that runtime. There is no ambient runtime at this call site — a
/// plain `#[test]` — so if the engine did not inherit weida's, `spawn` would
/// panic and the channel would never receive.
#[test]
fn the_engine_runs_on_the_runtime_that_built_it() {
    let runtime = Runtime::owned(RuntimeConfig::default()).expect("weida runtime");
    let (tx, rx) = std::sync::mpsc::channel();
    let inner = runtime.clone();
    runtime.exec().spawn(async move {
        let outcome = async {
            let identity = Identity::generate().map_err(|e| e.to_string())?;
            let store = MemoryStore::default();
            let consensus = Consensus::start(&inner, identity, store.clone(), store, config())
                .await
                .map_err(|e| e.to_string())?;
            consensus.bootstrap().await.map_err(|e| e.to_string())?;
            let id = consensus.id();
            let deadline = Instant::now() + DEADLINE;
            while consensus.leader() != Some(id) {
                if Instant::now() >= deadline {
                    return Err("never elected".to_owned());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let response = consensus
                .write(Request(b"a".to_vec()))
                .await
                .map_err(|e| e.to_string())?;
            consensus.shutdown().await;
            let bytes: [u8; 8] = response
                .0
                .try_into()
                .map_err(|_| "a response of the wrong length".to_owned())?;
            Ok(u64::from_be_bytes(bytes))
        }
        .await;
        let _ = tx.send(outcome);
    });

    let outcome = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("the engine reported nothing: it never ran");
    assert_eq!(outcome, Ok(1));
}
