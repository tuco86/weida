# Proposal from griasdi: who may talk to a weida-raft node

Status: proposal from griasdi, for tuco86 to take over into weida.
Read against: the uncommitted B-224 work in this checkout (`crates/raft/weida-raft/src/{server,consensus}.rs`,
2026-10-13), openraft 0.9.25.

## 1. What griasdi found

griasdi-august runs `weida_raft::Consensus` as a single node. A review of the raft binding found
that it admits engine traffic from **any** key:

- `Consensus::start` binds with `ClientTrust::AnyKey` (`consensus.rs`, `bind_quic`).
- `server::handle` checks that the sender named in a Vote, AppendEntries or Snapshot equals the
  proved key, and checks membership **only** for `Rpc::Forward` (`server.rs`, the identity rule).
- openraft 0.9 grants a vote when the candidate's `last_log_id` is at least its own, and that
  value is supplied by the candidate; outside the leader lease it also adopts any higher term.
  `append_entries` carries no membership check, and a vote response tells the asker the node's
  `last_log_id`, so a matching `prev_log_id` is easy to construct.

Consequences for a node whose raft port is reachable:

1. Anyone with a fresh key sends a Vote with a large term about once a second: the node keeps
   stepping down, and every `write` ends in `Error::NoLeader`.
2. A forged AppendEntries as "leader" can append and commit entries, including a membership change
   that removes the node from its own voters, or application entries the state machine accepts.
3. `server::handle` allocates the RPC header (`vec![0u8; len]`, up to `max_rpc_bytes`) before a
   deadline and spawns one task per request without a cap, so memory grows with the sender's
   bandwidth.

griasdi works around it for now by binding august's raft port on loopback, which is only possible
while the group has one node.

## 2. Proposed behavior

- **Admission by membership.** Once a node is initialized, a Vote, AppendEntries or Snapshot whose
  proved sender is not in the node's current membership (voters and learners, joint configs
  included) is refused with `Rejected` before openraft sees it. An uninitialized node keeps
  accepting them, because that is how a node being added learns the group.
  (openraft's `Raft::metrics()` carries the membership config already used by `Ops::is_member`.)
- **Bounded reads.** The length prefix and the header are read under a deadline (for example the
  heartbeat interval times a small factor for AppendEntries, `install_snapshot_timeout` for the
  snapshot body), and `server::serve` caps concurrent handlers (a semaphore; a full one refuses).
- **Engine end is observable.** `Consensus` exposes when the engine stopped on its own (fatal
  storage error, panic in a core task): for example `pub async fn stopped(&self) -> Error` or a
  `watch::Receiver<bool>`, so an application can exit and let its supervisor restart it.
  griasdi-august currently cannot tell that its engine died and keeps serving stale reads.

## 3. Why this belongs in weida-raft

The transport is the only place that knows the proved key of an RPC's sender, and "the node id is
the proved fingerprint" (0020 §4.4) only protects a group if a key outside the group cannot act as
a member. An application cannot add this check: the RPCs never reach it.

## 4. The griasdi code that uses it

`crates/griasdi-august/src/consensus.rs` (`Consensus::open`), and the default `raft_listen` in
`crates/griasdi-august/src/config.rs`, which goes back to the public address once admission by
membership exists, so a second august node can join.

## 5. Acceptance griasdi needs

1. A key outside the membership of an initialized single node sends Vote (any term) and
   AppendEntries; both are refused, the node stays leader, and a `write` succeeds.
2. A node being added (uninitialized) still receives the leader's AppendEntries and snapshot.
3. A sender that opens many RPC streams and sends only a length prefix does not grow the node's
   memory beyond the concurrency cap times the header limit, and its streams end at the deadline.
4. Killing the engine (for example a store that fails every call) makes the proposed "stopped"
   signal fire.
