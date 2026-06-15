# BATCH E — Reported-but-unverified findings (verify-then-fix)

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** 32 findings from the audit lost their adversarial verifier to a rate limit. Each item below must be VERIFIED against the code first (read the cited file, confirm the claim, check it is on a path that matters) and only then fixed. Two further findings were REFUTED (listed at the end — do not fix). Three low-severity extras are appended.

**Parallel-execution compatibility:** PARALLEL-friendly: items are grouped by subsystem; each group can go to a separate agent. Within a group items are independent unless they share a file.

**Files touched:** various (see each item)

**Conflicts with:** Mostly disjoint from A–D; check each item's file list. Items in `src/server.rs` defer until A/B/C land.

**Acceptance criteria:**

- For each item: a written verify verdict (real/not-real/matters) before any code change.
- Fixes follow the same acceptance style as BATCH A (build + test + targeted benchmark).

---

## Unverified findings by subsystem (full evidence in audit-result.json where available)

### Group `routing-rewrite-analytics` — assignable to one agent

- **Query fingerprinting runs 6 sequential regex passes and ~9 full-string allocations per query** — `src/analytics/fingerprinter.rs` — VERIFY FIRST, then fix.
- **Rewriter compiles a new Regex on every transformation invocation** — `src/rewriter/transformer.rs` — VERIFY FIRST, then fix.
- **StatisticsStore eviction does an O(n) full DashMap scan on every new fingerprint at capacity** — `src/analytics/statistics.rs` — VERIFY FIRST, then fix.
- **RoutingMetrics spawns a no-op tokio task per routed query and uses SeqCst atomics** — `src/routing/metrics.rs` — VERIFY FIRST, then fix.
- **Read/write and intent classification allocate full case-converted copies of every query** — `src/routing/query_router.rs` — VERIFY FIRST, then fix.
- **Rewriter parses every query eagerly and computes the fingerprint twice via to_uppercase+hash** — `src/rewriter/parser.rs` — VERIFY FIRST, then fix.
- **WorkflowTracer builds a new QueryClassifier (14 String allocs) per step and active workflows grow unbounded** — `src/analytics/intent.rs` — VERIFY FIRST, then fix.
- **SchemaAwareRouter clones entire NodeInfo vectors (and TableSchema per lookup) on every routing decision** — `src/schema_routing/router.rs` — VERIFY FIRST, then fix.

### Group `ha-failover-replay` — assignable to one agent

- **Global tokio RwLock health map (plus lb_state write lock) acquired on every client message** — `src/server.rs` — VERIFY FIRST, then fix.
- **Per-message full-buffer clone and double-decode in backend response loop (O(n²) in response size)** — `src/server.rs` — VERIFY FIRST, then fix.
- **TransactionJournal: single global RwLock per statement plus O(n) total_size() re-scan → O(n²) per transaction** — `src/transaction_journal.rs` — VERIFY FIRST, then fix.
- **HA-TR data plane is unwired: journal never populated and switchover buffer never consulted on the query path** — `src/server.rs` — VERIFY FIRST, then fix.
- **Replay and session migration open a fresh backend connection per statement** — `src/failover_replay.rs` — VERIFY FIRST, then fix.
- **execute_replay holds the global active_replays write lock across the entire replay including 200 ms WAL-sync polling** — `src/failover_replay.rs` — VERIFY FIRST, then fix.
- **Failover detection latency floor: TCP-connect-only health probes run serially, and blocked writers poll at 500 ms** — `src/server.rs` — VERIFY FIRST, then fix.
- **PostgresTopologyProvider opens a new TLS backend connection to every node on every 2 s poll, sequentially** — `src/primary_tracker.rs` — VERIFY FIRST, then fix.

### Group `caching-tiers` — assignable to one agent

- **Full regex normalization pipeline runs twice per query (get + put), even when L2/L3 are disabled** — `src/cache/mod.rs` — VERIFY FIRST, then fix.
- **L1 hot cache takes a write lock and does an O(n) Vec scan plus full query String allocation on every hit** — `src/cache/l1_hot.rs` — VERIFY FIRST, then fix.
- **WAL invalidator does blocking std::net::TcpStream reads inside async fn while holding a tokio RwLock write guard** — `src/distribcache/invalidator.rs` — VERIFY FIRST, then fix.
- **L3 distributed cache opens a brand-new TCP connection per get/insert/invalidate and replicates sequentially** — `src/distribcache/tiers/l3_distributed.rs` — VERIFY FIRST, then fix.
- **Edge cache hit path serializes all readers on a write lock with O(n) VecDeque scan and deep-copies the full wire response** — `src/edge/cache.rs` — VERIFY FIRST, then fix.
- **DistribCache lookup path pays discarded workload classification plus a global heatmap write lock and unbounded per-fingerprint stats** — `src/distribcache/heatmap.rs` — VERIFY FIRST, then fix.
- **DistribCache CacheEntry stores results as Vec<u8>; every tier hit and promotion deep-copies the payload, and L1 hits take a shard write lock** — `src/distribcache/tiers/l1_hot.rs` — VERIFY FIRST, then fix.
- **L2 'SSD' warm tier actually stores compressed blobs in RAM, zstd/bincode run synchronously on the async path, and LZ4 mode does no compression** — `src/distribcache/tiers/l2_warm.rs` — VERIFY FIRST, then fix.

### Group `tenancy-auth-guards` — assignable to one agent

- **Anomaly rate-spike detector serializes every query through one global write lock and recomputes O(window) statistics inside it** — `src/anomaly/mod.rs` — VERIFY FIRST, then fix.
- **Novel-query detector takes a global write lock per query even on the common already-seen path, and the fingerprint set grows without bound** — `src/anomaly/mod.rs` — VERIFY FIRST, then fix.
- **Rate-limiter sliding window preallocates 480KB per limiter key, hardcodes a 60k/min limit ignoring config, and stores one timestamp per event** — `src/rate_limit/limiter.rs` — VERIFY FIRST, then fix.
- **Tenant row-isolation transformer re-uppercases the full query once per end-marker inside a filter_map, plus 3 more full-string case conversions per query** — `src/multi_tenancy/transformer.rs` — VERIFY FIRST, then fix.
- **transform_query deep-clones the entire TenantConfig out of the DashMap on every query** — `src/multi_tenancy/mod.rs` — VERIFY FIRST, then fix.
- **API-key authentication does an O(n) scan over all registered keys, re-hashing the presented key once per stored entry** — `src/auth/handler.rs` — VERIFY FIRST, then fix.
- **ApiKeyManager::validate write-locks the entire key store on every validation just to update last_used_at, then deep-clones the ApiKey** — `src/auth/api_keys.rs` — VERIFY FIRST, then fix.
- **Per-query anomaly observation eagerly allocates an RFC3339 timestamp string and three owned Strings even when no detector fires** — `src/server.rs` — VERIFY FIRST, then fix.

## REFUTED — do not fix (kept for the record)

### Single global RwLock<HashMap<NodeId, NodePool>> write-locked on every acquire and release across all nodes
- `src/connection_pool.rs` (pooling-balancing)

The quoted lock acquisitions exist (src/connection_pool.rs:264, 337-342, 351, 367 verified), but the headline claim — 'all acquires/releases for ALL nodes serialize on one lock' — is false in production wiring. The only production consumer, ConnectionPoolManager (src/pool/manager.rs:23), stores DashMap<NodeId, ConnectionPool>: one separate ConnectionPool instance per node, each created in add_node (manager.rs:88-90) with exactly one node in its inner HashMap. So the RwLock at line 264 is already per-node; the finding's suggested sharding is effectively implemented one layer up. Worse for the finding's impact claim: the pool is not on the data path at all. Client sessions connect to backends via direct TcpStream::connect (src/server.rs:995-1001) with raw byte forwarding; ConnectionPoolManager::acquire/acquire_with_mode has zero non-test callers (only doc comment pool/mod.rs:25 and unit tests manager.rs:420/440), and the server only uses the pool manager for evict_idle, get_stats, add_node, and a has_active_lease log line at session close (server.rs:710). The write-lock critical sections also contain no .await and are bounded to one node's <=max_connections (default 10) traffic, and the second lock for total_created only runs on the new-connection path, dominated by the backend TCP+auth handshake. Fixing it would not measurably change production latency, throughput, or scalability today.

### DISCARD ALL reset query executes a full backend round trip on every lease release in Transaction and Statement modes
- `src/pool/manager.rs` (pooling-balancing)

The evidence excerpt exists verbatim (src/pool/manager.rs:211-231) and the default reset_query is DISCARD ALL (src/pool/config.rs:81-83), but the headline claim — 'a full backend round trip on every lease release' — is false in the current codebase for two independent reasons. (1) Dead code in production: ConnectionPoolManager::acquire/release are never called outside unit tests (src/pool/manager.rs:420-445) and a doc example (src/pool/mod.rs:25-27). The real data path, client_loop in src/server.rs:729+, forwards bytes over a raw TcpStream to the backend; the only pool-manager interactions in server.rs are has_active_lease (line 710, log-only disconnect cleanup), evict_idle, close_all, get_stats, metrics, default_mode, and add_node — all cold path. The finding's own impact text hedges with 'once pool-modes is wired into the data path', i.e., it describes a hypothetical future cost. (2) Even if release were called, no round trip would occur: run_reset_query (src/connection_pool.rs:469-481) only executes when conn.client is Some, but ConnectionPoolManager::add_node (src/pool/manager.rs:78-93) builds ConnectionPool::new without with_backend_template and uses the skeleton add_node (src/connection_pool.rs:216-219), so create_connection (src/connection_pool.rs:395-411) always yields client: None. The comment at manager.rs:206-210 explicitly documents this skeleton behavior ('record the reset as if it ran'). Therefore fixing it now would not improve any production latency/throughput metric. The suggestion (dirtiness tracking, cheaper default, pipelined reset) is sound PgBouncer-style design advice for whenever pool-modes is actually wired to live backend clients, but it addresses code that currently performs zero I/O.

## Low-severity extras (unverified, fix opportunistically)

### Auth relay polls the client with a fixed 100ms timeout per round

- **Location:** `src/server.rs:1108`
- **Severity / category:** low / async
- **Found by:** `hot-path-protocol` auditor

**Evidence (verbatim from code at audit time):**

```
let n = tokio::time::timeout(Duration::from_millis(100), client_stream.read(&mut read_buf))
                .await;
```

**Impact:** proxy_authentication alternates blocking reads on the backend with a 100ms-timeout read on the client instead of selecting over both directions. Multi-step auth (SCRAM is 2+ client messages) eats up to 100ms of dead time per step when the timing misaligns, and the loop silently drops the case where the client message arrives split across the 100ms boundary. Connection-setup only, not per-query, but adds avoidable connect latency and timer churn under connection churn.

**Fix:** Use tokio::select! over client.read and backend.read in the auth phase (no timers), exiting on ReadyForQuery/ErrorResponse. Removes up to ~100-200ms from SCRAM/MD5 connection establishment and the per-iteration timer.


### Hook registry clones a Vec<String> of plugin names under the lock for every hook stage of every query

- **Location:** `src/plugins/mod.rs:464`
- **Severity / category:** low / locking
- **Found by:** `plugins-wasm` auditor

**Evidence (verbatim from code at audit time):**

```
let hooks = self.hooks.read();
        let plugin_names = hooks.get(&HookType::PreQuery).cloned().unwrap_or_default();
```

**Impact:** execute_pre_query, execute_post_query, execute_route, and execute_authenticate each take the hooks RwLock and deep-clone the Vec<String> (one Vec allocation plus one String allocation per registered plugin) on every invocation — i.e. up to 3 Vec+String clone sets per query — followed by a DashMap name lookup per plugin. The registry only changes on load/unload/reload, which is rare.

**Fix:** Store the per-hook plugin list as Arc<[Arc<LoadedPlugin>]> (resolve the DashMap lookup at registration time) behind an arc_swap::ArcSwap or RwLock<Arc<...>>; the per-query cost becomes a single Arc clone with no allocation and no per-plugin map lookup. Benefit: removes 3 Vec + 3N String allocations and N DashMap lookups per query.


### Plugins only hook the simple-query protocol: extended-protocol traffic (Parse/Bind/Execute) bypasses all hooks, and the WASM boundary is JSON-over-double-copy

- **Location:** `src/server.rs:1665`
- **Severity / category:** low / protocol
- **Found by:** `plugins-wasm` auditor

**Evidence (verbatim from code at audit time):**

```
/// Only simple-query (`MessageType::Query`) messages are inspected today.
    /// Extended-protocol messages (`Parse`/`Bind`/`Execute`) are passed
    /// through unchanged — a future task wires them in.
```

**Impact:** Two architectural ceilings observed: (1) any driver using prepared statements (JDBC, asyncpg, pgx defaults) sends extended-protocol messages that skip pre-query/route/post-query hooks entirely — so plugin policy (blocking, masking, routing) silently does not apply to most production ORM traffic, and there is no cheaper code path to win back. (2) The hook ABI marshals everything as JSON: serialize ctx → alloc in guest → memory.write copy in → guest runs → read_memory copies result out into a fresh Vec (runtime.rs:681 `let mut out = vec![0u8; len as usize]`) → serde_json::from_slice. That is two full payload copies plus JSON encode/decode per hook per plugin, which grows linearly with query text size.

**Fix:** Wire Parse-message inspection into the same hook pipeline (the SQL is available in the Parse payload) so plugin policy covers extended protocol; longer term consider a compact binary context encoding (bincode is already a dependency) or a flat fixed-layout struct in linear memory to eliminate JSON costs. Benefit: correctness of plugin enforcement for prepared-statement clients and lower per-hook marshalling cost on large queries.

