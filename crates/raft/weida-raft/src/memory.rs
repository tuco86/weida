//! The reference in-memory log store and state machine.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};

use crate::{Node, NodeId, Request, Response, TypeConfig};

/// An in-memory log and state machine: the reference store of this crate and
/// the fixture of its tests.
///
/// An application supplies its own [`RaftLogStorage`] and [`RaftStateMachine`]
/// to [`Consensus::start`](crate::Consensus::start); this is what such a pair
/// has to match. The requirements of 0021 §4.3 are the v2 traits
/// `RaftLogStorage` — `get_log_state`, `get_log_reader`,
/// `save_vote`/`read_vote`,
/// `save_committed`/`read_committed`, `append` **with a flush callback**,
/// `truncate` backwards and `purge` forwards — plus `RaftStateMachine`:
/// `applied_state`, `apply`, `get_snapshot_builder`,
/// `begin_receiving_snapshot`, `install_snapshot`, `get_current_snapshot`. The
/// flush callback is the persist-before-send rule of the Raft thesis made
/// explicit in a signature, which is the part a hand-rolled store gets wrong.
///
/// The store is shared by clone: all clones see one state. That is what lets
/// a restarted node reuse the store of the node it replaces, and what lets a
/// test read the state machine of a running node.
#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<tokio::sync::Mutex<StoreState>>,
}

#[derive(Default)]
struct StoreState {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, Node>,
    requests: Vec<Request>,
    snapshot: Option<(SnapshotMeta<NodeId, Node>, Vec<u8>)>,
    snapshot_index: u64,
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

impl MemoryStore {
    /// Every request applied so far, in log order.
    pub async fn requests(&self) -> Vec<Request> {
        self.inner.lock().await.requests.clone()
    }

    /// Seeds entries without the flush callback `append` requires.
    ///
    /// Test-only, and it exists because `LogFlushed::new` is `pub(crate)` in
    /// openraft: a third-party store cannot construct the callback its own
    /// `append` is handed.
    #[cfg(test)]
    async fn seed(&self, indexes: std::ops::RangeInclusive<u64>) {
        let mut inner = self.inner.lock().await;
        for index in indexes {
            inner.log.insert(
                index,
                Entry {
                    log_id: LogId::new(openraft::CommittedLeaderId::new(1, NodeId([1; 32])), index),
                    payload: EntryPayload::Normal(Request(Vec::new())),
                },
            );
        }
    }
}

/// The snapshot data: a definite-length CBOR array of byte strings, one per
/// request.
fn encode_requests(requests: &[Request]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut e = minicbor::Encoder::new(&mut out);
    e.array(requests.len() as u64)
        .expect("encoding into a Vec is infallible");
    for request in requests {
        e.bytes(&request.0)
            .expect("encoding into a Vec is infallible");
    }
    out
}

fn decode_requests(data: &[u8]) -> Result<Vec<Request>, minicbor::decode::Error> {
    let mut d = minicbor::Decoder::new(data);
    let len = d
        .array()?
        .ok_or_else(|| minicbor::decode::Error::message("indefinite snapshot array"))?;
    // Every element takes at least one byte, so a longer claim is a lie.
    if len > data.len() as u64 {
        return Err(minicbor::decode::Error::message(
            "snapshot array longer than its data",
        ));
    }
    let mut requests = Vec::with_capacity(len as usize);
    for _ in 0..len {
        requests.push(Request(d.bytes()?.to_vec()));
    }
    if d.position() != data.len() {
        return Err(minicbor::decode::Error::message(
            "trailing bytes after snapshot",
        ));
    }
    Ok(requests)
}

impl RaftLogReader<TypeConfig> for MemoryStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemoryStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last = inner.log.iter().next_back().map(|(_, e)| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last.or(inner.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                inner.log.insert(entry.get_log_id().index, entry);
            }
        }
        // In memory the write is already durable by the time this returns, so
        // the callback fires immediately. A real store fires it **after** the
        // bytes are on the medium, and nothing may be sent to a peer before
        // that: this callback is where that ordering lives.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemoryStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.snapshot_index += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.applied,
            last_membership: inner.membership.clone(),
            snapshot_id: format!("{}", inner.snapshot_index),
        };
        let data = encode_requests(&inner.requests);
        inner.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for MemoryStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok((inner.applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.inner.lock().await;
        let mut answers = Vec::new();
        for entry in entries {
            inner.applied = Some(*entry.get_log_id());
            match entry.payload {
                EntryPayload::Blank => answers.push(Response::default()),
                EntryPayload::Normal(request) => {
                    inner.requests.push(request);
                    let count = inner.requests.len() as u64;
                    answers.push(Response(count.to_be_bytes().to_vec()));
                }
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembership::new(inner.applied, membership);
                    answers.push(Response::default());
                }
            }
        }
        Ok(answers)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let requests = decode_requests(&data).map_err(|e| {
            StorageError::from(StorageIOError::read_snapshot(
                Some(meta.signature()),
                openraft::AnyError::new(&e),
            ))
        })?;
        let mut inner = self.inner.lock().await;
        inner.requests = requests;
        inner.applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `truncate` discards a suffix and `purge` a prefix — the two operations
    /// an append-only store cannot do.
    ///
    /// The entries are seeded directly rather than through
    /// [`RaftLogStorage::append`]: `LogFlushed::new` is `pub(crate)` in
    /// openraft, so a third-party store cannot construct the flush callback
    /// its own `append` receives.
    #[tokio::test]
    async fn the_log_truncates_backwards_and_purges_forwards() {
        let mut store = MemoryStore::default();
        store.seed(1..=5).await;
        assert_eq!(last_index(&mut store).await, Some(5));

        store.truncate(log_id(4)).await.expect("truncate");
        assert_eq!(
            last_index(&mut store).await,
            Some(3),
            "truncate discards the suffix from the given index on"
        );

        store.purge(log_id(2)).await.expect("purge");
        let left = store.try_get_log_entries(..).await.expect("read");
        assert_eq!(left.len(), 1, "1 and 2 are gone, 3 remains");
        assert_eq!(
            store
                .get_log_state()
                .await
                .expect("state")
                .last_purged_log_id
                .map(|id| id.index),
            Some(2),
            "a purged prefix is remembered, or a restart would replay it"
        );
    }

    /// A purged log still reports the last log id it had, which is what lets a
    /// restarted node answer a vote request about entries it no longer holds.
    #[tokio::test]
    async fn a_fully_purged_log_still_knows_where_it_ended() {
        let mut store = MemoryStore::default();
        store.seed(1..=3).await;
        store.purge(log_id(3)).await.expect("purge everything");

        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id.map(|id| id.index), Some(3));
        assert_eq!(
            state.last_log_id.map(|id| id.index),
            Some(3),
            "an empty log whose prefix was purged is not an empty history"
        );
    }

    /// An undecodable snapshot is refused and the state machine is untouched.
    #[tokio::test]
    async fn a_garbage_snapshot_is_refused_and_changes_nothing() {
        let mut store = MemoryStore::default();
        store
            .apply(normal_entries(&[b"a", b"b"]))
            .await
            .expect("apply");
        let before = store.applied_state().await.expect("state");

        let meta = SnapshotMeta {
            last_log_id: Some(log_id(9)),
            last_membership: StoredMembership::default(),
            snapshot_id: "garbage".to_owned(),
        };
        let result = store
            .install_snapshot(&meta, Box::new(Cursor::new(vec![0xff, 0x00, 0x13])))
            .await;
        assert!(result.is_err(), "garbage must not install");

        assert_eq!(
            store.requests().await,
            vec![Request(b"a".to_vec()), Request(b"b".to_vec())]
        );
        let after = store.applied_state().await.expect("state");
        assert_eq!(after.0, before.0);
        assert_eq!(after.1, before.1);
    }

    /// A built snapshot installs into a fresh store and reproduces the state.
    #[tokio::test]
    async fn a_built_snapshot_installs_into_a_fresh_store() {
        let mut source = MemoryStore::default();
        source
            .apply(normal_entries(&[b"x", b"yy", b""]))
            .await
            .expect("apply");
        let snapshot = source.build_snapshot().await.expect("build");

        let mut target = MemoryStore::default();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");

        assert_eq!(target.requests().await, source.requests().await);
        assert_eq!(
            target.applied_state().await.expect("state").0,
            source.applied_state().await.expect("state").0
        );
    }

    fn normal_entries(payloads: &[&[u8]]) -> Vec<Entry<TypeConfig>> {
        payloads
            .iter()
            .enumerate()
            .map(|(i, p)| Entry {
                log_id: log_id(i as u64 + 1),
                payload: EntryPayload::Normal(Request(p.to_vec())),
            })
            .collect()
    }

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(openraft::CommittedLeaderId::new(1, NodeId([1; 32])), index)
    }

    async fn last_index(store: &mut MemoryStore) -> Option<u64> {
        store
            .get_log_state()
            .await
            .expect("state")
            .last_log_id
            .map(|id| id.index)
    }
}
