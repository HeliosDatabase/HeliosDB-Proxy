# D-01 — Control authority and data path separation (design spike)

**Status:** design spike, 2026-09-12. Input to H-01's provider interface (already
partially shipped) and H-02's fencing work; implementation is staged and optional.
**Sources:** `docs/internal/audit-2026-09/IMPROVEMENTS.md` (D-01), issue [#54],
`docs/internal/audit-2026-09/README.md` (HA findings), H-01 (`3a09d23`) and H-02
(`d860787`) increments.
**Related:** D-02 (durable replay/outcome journal), D-03 (backend capabilities and
versioned shard map), H-04 (lag and causal positions), C-02/C-05/C-06 (cache
correctness).

---

## 1. Problem

The daemon's current topology model has three layers of authority, added in order:

1. **Static:** configured `[[nodes]]` roles + the health checker (default).
2. **Provider-observed:** `[topology] provider = "postgres"` polls
   `pg_is_in_recovery()`; its leader is authoritative while a **lease** is valid
   (H-02). An unreachable provider stops writes at the lease boundary.
3. **Process epoch:** `PrimaryTracker` increments an in-process authority `epoch`
   on every observed leader change; `/topology` exposes it.

What is still missing for a global/sharded deployment is a *contract*: where does
authority come from when there are several proxies and several shards? Today each
proxy observes a database and keeps its own epoch. Two proxies can disagree during a
partition; a shard map has no owner; cache versions have no shared ordering. D-01
defines the contract — not a full implementation.

The proxy cannot become a consensus system by itself: a proxy-side epoch cannot
fence a client that connects around the proxy, and it cannot atomically commit
PostgreSQL effects and its own state (see D-02). The control plane in this design
owns **decisions**, not data-path durability.

## 2. Goals and non-goals

**Goals**

- One **immutable snapshot** contract that owns: cluster/shard identity, leader
  identity, authority epoch, membership, tenant placement, policy versions, and the
  commit position watermark.
- Region-local proxies consume snapshots from a small control authority; the **read
  path never touches the consensus path** (a cached snapshot is read on every
  request).
- Writes **fail closed** when authority expires; there is a defined expiry and a
  defined reintegration path for a returning old primary.
- Cache and routing versions are derived from `(cluster, shard, authority_epoch,
  commit_position)`, never from wall clocks or last-writer-wins.

**Non-goals**

- Making the proxy a witness or a database. Split-brain prevention for clients that
  bypass the proxy is enforced by the database (synchronous replication, promotion
  fencing, watchdog) or by exclusive network/backend control.
- Cross-shard SQL execution with distributed transactions. Work that cannot be
  pinned to one shard is rejected or delegated to a database coordinator (D-03).
- Cross-proxy session continuity across a process death (D-02 explicitly defers this).

## 3. The snapshot contract

An immutable, versioned value; every routing/cache decision that needs authority
reads one local copy.

```rust
pub struct ControlSnapshot {
    /// Monotonic control-plane version. Increases with every published change;
    /// a consumer that sees a lower version than it already applied must
    /// discard the update (stale/duplicate delivery).
    pub version: u64,
    pub cluster_id: String,
    pub shards: Vec<ShardAuthority>,

    /// The control authority's own lease. A snapshot older than
    /// `lease_timeout` is not authoritative: writes fail closed.
    pub published_at: SystemTime,
    pub lease_timeout: Duration,
}

pub struct ShardAuthority {
    pub shard_id: String,
    /// Monotonic per shard. A transaction is pinned to one epoch for its whole
    /// life; routing a transaction across epochs is a correctness error.
    pub authority_epoch: u64,
    pub leader: Option<NodeIdentity>,   // node_id + client address
    /// The leader's confirmed commit position (WAL LSN or backend-specific
    /// equivalent), as observed by the control authority — the provenance for
    /// causal reads (H-04).
    pub leader_commit_position: Option<u64>,
    /// Replica positions the authority is willing to attest, with sample time.
    pub replicas: Vec<ReplicaPosition>,
    /// Membership + roles as the authority sees them (does not replace
    /// `[[nodes]]`; it must agree with it or the proxy refuses the snapshot).
    pub members: Vec<MemberIdentity>,
    /// Policy version for cache identity/freshness (C-05/C-06).
    pub policy_version: u64,
}
```

**Identity rules**

- `NodeIdentity` is the configured `[[nodes]]` address plus a stable node id; a
  snapshot member that is not an enabled configured node is **rejected**, not
  routed to (keeps the H-01 property).
- `authority_epoch` may only increase for a shard. A snapshot that decreases it is
  discarded and logged; the same applies to `ControlSnapshot.version`.
- `leader_commit_position` is advisory for causality, **never** a durability
  claim: the proxy does not know whether an async replica has received the last
  acknowledged commit. RPO statements stay with the database.

**Reads vs writes**

- Reads use the newest locally cached snapshot they have; per-table freshness
  policy decides whether a read may be served from a replica, must wait, or must go
  to the leader (C-05).
- Writes resolve one shard and one `authority_epoch`; if the local snapshot's lease
  has expired, they fail closed with the same semantics H-02 introduced
  (`NoHealthyNodes` / bounded wait). Reads may continue under the last snapshot only
  if their freshness policy still permits it, otherwise they bypass the cache or
  fail.

## 4. Control plane shapes (staged)

The contract above is deliberately source-agnostic. Three staged providers, each
implementing the same snapshot contract:

- **D-01a — External authority adapter (no new consensus).** A small adapter reads
  an existing authority — Patroni's REST API, a Kubernetes `Lease` object, etcd, or
  Consul — and produces `ControlSnapshot`. This is what H-01/H-02 already do for
  single-shard Postgres polling; D-01a extends it to arbitrary controllers and adds
  cluster/shard/version fields. No new failure domain: if the authority is
  unreachable, leases expire and writes fail closed.
- **D-01b — Control plane as a service (optional).** A small consensus-backed
  service (Raft, reusing the patterns surveyed from Full's `raft_storage.rs`)
  owns shard maps, writer epochs, membership, tenant placement and policy
  versions. Region-local proxies cache snapshots; the service is on the **write
  authority** path and on config changes, never on reads.
- **D-01c — Multi-region.** Region-local control replicas, asynchronous
  cross-region replication of the snapshot log, and explicit per-region write
  authority. Cross-region synchronous durability remains a separate, named
  latency/RPO choice (D-02).

**Control traffic is separate from data traffic**: control connections use their own
credentials (`[topology]` today; a control token later), their own bounded poll
budget, and never share pooled data connections. Control-path partitions must not
be able to stall the data path beyond the lease boundary.

## 5. Consumer integration

| Consumer | Today | With D-01 |
|---|---|---|
| Write routing (`select_primary_until`) | Provider leader + lease (H-02) | Leader + epoch from `ShardAuthority`; transaction pinned to one epoch |
| Read routing | Configured standbys + health | Freshness policy against attested replica positions (H-04/C-05) |
| Admin `/topology` | `authoritative {address, epoch, valid, leaseRemainingMs}` | Adds `clusterId`, `shardId`, snapshot `version`, per-shard blocks |
| Cache identity | query/db/user/branch (C-06 pending) | Adds `(cluster, shard, authority_epoch, commit_position, policy_version)` |
| Pool eviction | Local pool health | Evicts connections to nodes that left the authoritative membership |
| Failover/replay | In-session replay + provider tracker | Recovery refuses to cross an epoch boundary; replay targets the epoch's leader |

## 6. Failure semantics (what must be true)

- **Partition between proxy and control authority:** lease expires, writes fail
  closed, reads follow their freshness policy or fail. No fallback to configured
  roles (H-02 property, kept).
- **Partition between two proxies:** both may believe different epochs. Writes to
  the same shard must be serialized by the *database* (synchronous replication,
  promotion fencing) or by exclusive leader reachability; the proxy cannot prevent
  two direct clients from both writing. This is a documented limit, not a bug to
  fix in the proxy.
- **Old primary returns:** its epoch is stale. The proxy must never route to it
  under the new epoch, and a returning node must rejoin via the database's
  reintegration path (`pg_rewind`/rebase) before the authority attests it.
- **Delayed/duplicated control messages:** discarded by the monotonic
  `version`/`authority_epoch` rules.
- **Witness loss:** if the authority cannot form quorum, it stops publishing new
  snapshots; proxies fail closed at lease expiry. Correctness over availability.
- **Rolling config changes:** a config reload that changes `[[nodes]]` must not
  change the authority epoch; a config that *removes* the authoritative leader is
  rejected by validation or the snapshot is refused.

## 7. Staged implementation plan

| Stage | Deliverable | Depends on |
|---|---|---|
| D-01a | `ControlSnapshot` type + monotonic version/epoch validation + external adapter over the existing `[topology]` provider; admin exposure. | H-01/H-02 (done) |
| D-01b | Shard map + per-shard authority epochs in the snapshot; transaction pinning in routing; cache version derivation. | D-01a, D-03, C-06 |
| D-01c | Optional consensus-backed control service; region-local snapshot caches. | D-01b, D-02 |
| D-01d | Multi-region authority and explicit sync/async RPO choices. | D-01c, H-04 |

Acceptance (from IMPROVEMENTS.md): **rolling config changes and partition healing
cannot route a transaction across epochs.** Each stage must ship with an
acceptance test that holds a transaction pinned across a snapshot change and proves
it never dispatches on a different epoch.

## 8. Open decisions (need owner input before D-01b)

1. **Where does the first external adapter point?** Patroni REST (single cluster,
   already a de facto standard) vs Kubernetes `Lease` (better multi-tenant story)
   vs etcd/Consul (more general). Recommendation: Patroni first, because it answers
   the actual promotion question; the contract is identical for the others.
2. **Is D-01b in scope for 1.x at all?** The audit calls it optional; the proxy is
   useful without it. Recommendation: keep D-01a/D-01b as optional components and
   do not put consensus in the default deployment.
3. **Epoch ownership per shard vs per cluster.** Per-shard is more precise but
   complicates cache identity; recommendation: per-shard, with a cluster-level
   snapshot version as the outer ordering.
4. **How much of H-04's watermark is in D-01a?** `leader_commit_position` can be
   filled by the adapter (e.g., `pg_current_wal_lsn()`), but replica attestation
   needs real measurement; recommendation: ship the field as `Option`, never
   fabricate it.

## 9. What this fixes, and what it does not

**Fixes:** one place decides leadership/epoch per shard; consumers read one
immutable value; stale/duplicate control messages cannot reorder decisions; cache
versions have a causal basis; fail-closed behavior is uniform across routing,
recovery and cache.

**Does not fix:** direct-client split brain (database/network enforcement), atomic
proxy-journal + database commit (D-02), cross-proxy socket continuity (D-02), and
sharded SQL execution (D-03). Any claim of "zero downtime", "no data loss" or
"global consistency" that reads this design as delivering those is wrong.
