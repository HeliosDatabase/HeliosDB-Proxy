# BATCH C — Wire the connection pool into the data path

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

---
## IMPLEMENTATION STATUS (2026-06-13)

**Delivered: per-session multi-node connection cache.** `client_loop` now holds a
`HashMap<node_addr, TcpStream>` of authenticated backend connections instead of a
single stream. A read/write route switch (or a forced-route / failover switch)
reuses the already-authenticated connection to each node rather than dropping the
socket and paying a fresh TCP connect + startup + SCRAM handshake every time
(audit finding #3, the per-switch re-auth cost). Broken connections are evicted
and redialed lazily. Verified: 1307 unit tests + live battery green on PostgreSQL
18.4 (single-node AND 2-node read/write-split) and HeliosDB-Nano 3.57 — zero
regression.

**Deferred to after BATCH F (proxy-side backend auth) — the headline finding:**
the pool's *cross-client* multiplexing (many client connections sharing a small
set of backend connections, the "decouple client count from backend count" claim)
is **hard-blocked on proxy-owned backend credentials.** Auth is pass-through
(`proxy_authentication`), so every backend connection is authenticated as one
specific client and cannot be safely handed to another client. True transaction
pooling therefore requires SCRAM-verifier / `auth_query` proxy-side auth (BATCH F)
first; once that lands, the connection cache here becomes the per-(user,database)
shared pool. This matches the verifier's own correction in finding #3 below.

**Also deferred (correctness-sensitive, belongs with lag-routing):** aggressively
routing reads to standbys to un-idle replicas. Under the current sticky routing a
session stays on its initial node, so the cache rarely switches today; making
reads offload to standbys needs read-your-writes / replica-lag awareness
(`lag-routing`) to avoid serving stale reads, so it is not done here.

The latent pool-manager defects below (DashMap guard across `.await`, dead
`LoadBalancer`, unused `min_connections`/`test_on_acquire`) remain on the unwired
`ConnectionPoolManager`; they become live work when BATCH F wires real pooling.
---

**Goal:** Make the shipped `ConnectionPoolManager` actually serve backend connections in `route_and_forward`, decoupling client count from backend count and removing per-switch TCP+auth round trips. This is the change that makes the pooler claim real.

**Parallel-execution compatibility:** SOLO only. Highest-risk batch; do not parallelize.

**Prerequisites:** BATCH A + B merged. IMPORTANT investigation first: determine what credentials the data path uses for backend auth (client passthrough via `proxy_authentication` vs node-configured). Pooling requires proxy-owned credentials per (node, database, user) — if client credentials are passed through, pool keying must include the client role, or pooled mode must require auth-proxy configuration.

**Files touched:** `src/server.rs` (`route_and_forward`, session lifecycle), `src/pool/manager.rs`, `src/connection_pool.rs`, `src/load_balancer.rs` (or delete its dead path)

**Conflicts with:** Conflicts with A and B (same function) — execute AFTER both. Compatible in parallel with D/E/F/G/H.

**Acceptance criteria:**

- Two concurrent psql sessions doing alternating reads share ≤ pool-size backend connections (verify via `pg_stat_activity` count on the backend).
- A read→write route switch reuses a pooled, pre-authenticated connection (no new backend connection in `pg_stat_activity`, latency of switch ≈ 0 RTT).
- Transaction/statement pool modes honor lease semantics incl. reset; session mode pins.
- Failover demo (`demos/chaos-failover`) still passes.
- `cargo test` green incl. pool unit tests; no DashMap guard held across `.await` (finding idx 9).

---

### Backend connections dialed and fully re-authenticated per client session/switch; pool never used on data path

- **Location:** `src/server.rs:1211`
- **Severity / category:** high / architecture
- **Found by:** `hot-path-protocol` auditor; independently confirmed by `pooling-balancing` (idx 8) and `infra-build-observability` (idx 27)
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
// Close old connection if any
            drop(backend_stream);

            // Connect to new backend
            let new_backend = tokio::time::timeout(
                config.pool.acquire_timeout(),
                TcpStream::connect(&target_node),
```

**Impact:** Every new client connection dials a fresh backend TCP connection and proxies a full auth handshake (connect_and_authenticate, lines 995-1011), and every routing switch (e.g. write after reads on a standby) drops the old socket and performs TCP connect + startup + complete_backend_auth — multiple RTTs including SCRAM. The ConnectionPoolManager built at startup (line 437) is never consulted in route_and_forward, so client connection storms pass 1:1 to PostgreSQL, defeating the main purpose of a PG proxy. Additionally, since need_switch is false whenever the current node is healthy and the query is a read, sessions that land on the primary never rebalance to standbys, so read replicas sit idle.

**Fix:** Acquire pre-authenticated backend connections from the pool manager in route_and_forward (lease per query/transaction per pool mode) and return them instead of dropping; keep a small per-node warm pool with session-parameter reset. Cuts switch cost from ~3-5 RTTs to ~0 and decouples client connection count from backend connection count.

**Verifier correction/nuance:** Two details need adjustment. (a) The claim that switches perform 'complete' auth 'including SCRAM' is wrong in an interesting way: complete_backend_auth (server.rs:1452-1510) only reads until ReadyForQuery and never writes a response to any auth challenge, so the silent backend switch only succeeds against trust-auth backends; with md5/SCRAM it hangs for the 5-10s timeouts and fails the query — a correctness bug layered on the perf issue, not extra RTTs. (b) The suggestion ('acquire pre-authenticated connections from the pool manager') is directionally right but understates the work: client auth is pass-through (proxy_authentication, 1049-1121), so the proxy holds no backend credentials; pooling requires proxy-side credentials (BackendConfig template, auth_query/auth_file style) and pools keyed per (user, database), plus wiring with_backend_template/add_node_with_endpoint, since the currently-constructed pool is skeleton-mode and cannot produce live connections. Also, the 'write after reads on a standby' switch scenario is rarer than implied because select_node pins new sessions to the primary; the dominant costs are the per-new-client backend dial+auth and the idle read replicas.

<details><summary>Verifier reasoning</summary>

Could not refute; every load-bearing claim checks out in src/server.rs. (1) Evidence excerpt exists verbatim at lines 1209-1222 in route_and_forward. (2) This is the hot data path: client_loop (lines 729-877) routes EVERY decoded client message through route_and_forward (line 837); main.rs:135 runs this ProxyServer and no alternate splice/forwarding path exists (backend/client.rs:7 explicitly says its path is distinct). (3) Initial connections: connect_and_authenticate (977-1022) dials TcpStream::connect directly (995-1001) per client, so client connection count maps 1:1 to backend connections — no multiplexing. (4) The ConnectionPoolManager built at server.rs:437 is provably never consulted on the data path: grep shows pool_manager used only for construction (406-455), idle eviction (2094-2115), a comment-only lease check on disconnect (708-718), stats (2133-2136), and add_node (2167-2175); pool/manager.rs:111 acquire() is never called from server.rs. Worse than the finding states, the pool as wired is skeleton-mode: ConnectionPoolManager::add_node creates ConnectionPool::new without with_backend_template/add_node_with_endpoint, so get_connection would return PooledConnection{client: None} bookkeeping objects (connection_pool.rs:195-217, 380-411). (5) Replica idling confirmed: select_node (1965-2018) pins new sessions to the healthy primary, and need_switch (1170-1195) is false for reads on a healthy node, so in a healthy cluster standbys receive no reads. This is per-connection-at-scale and per-switch cost (multiple network RTTs + backend fork/auth in PostgreSQL), so fixing it would materially improve latency, backend load, and connection-storm resilience — the canonical value of a PG proxy.

</details>


### DashMap shard guard held across .await (up to acquire_timeout of 30s) in ConnectionPoolManager acquire and release

- **Location:** `src/pool/manager.rs:137`
- **Severity / category:** high / async
- **Found by:** `pooling-balancing` auditor
- **Adversarial verification:** isReal=True matters=False confidence=high

**Evidence (verbatim from code at audit time):**

```
let pool = self
    .pools
    .get(node_id)
    .ok_or_else(|| ProxyError::PoolExhausted(format!("Node {:?} not in pool", node_id)))?;
```

**Impact:** The dashmap::Ref returned by self.pools.get() holds a synchronous shard RwLock read guard, and it stays alive across `pool.get_connection(node_id)` awaited under a 30s timeout (lines 144-148). release() does the same: the guard from `self.pools.get(&info.node_id)` (line 203) is held across run_reset_query().await and return_connection().await (lines 213-234). Any concurrent pools.insert/remove on the same shard (add_node/remove_node) then blocks a tokio worker thread on a sync lock for up to the full acquire wait — worker starvation and a classic dashmap-across-await deadlock hazard. Also note acquire wraps pool.get_connection in a second tokio::time::timeout although get_connection already applies config acquire_timeout internally (connection_pool.rs:306), registering two timers per acquire.

**Fix:** Store DashMap<NodeId, Arc<ConnectionPool>>, clone the Arc, and drop the guard before any .await; remove the redundant outer timeout in acquire_with_mode and rely on the pool's internal acquire_timeout. Expected benefit: eliminates worker-thread blocking/deadlock risk and halves timer registrations per acquire.

**Verifier correction/nuance:** Severity "high" is wrong — this is a latent defect on an unwired feature path, not a live hot-path hazard. The "deadlock hazard" framing is also overstated: pool.get_connection never write-locks the manager's DashMap, so there is no same-task re-entrancy deadlock; worst case on the default multi-threaded runtime is one worker thread blocking in add_node/remove_node (cold topology operations) for up to the acquire wait while a guard-holding task is parked. The suggested fix (DashMap<NodeId, Arc<ConnectionPool>>, clone+drop guard before awaiting, drop the redundant outer timeout since the inner pool applies the same acquire_timeout at connection_pool.rs:306) is technically correct and would not break any invariant — the guard protects only map membership, and remove_node already handles concurrent removal by closing the pool it extracts. It is worth applying only if/when pool-modes leasing is actually wired into the server data path.

<details><summary>Verifier reasoning</summary>

The code does exactly what the finding claims. At src/pool/manager.rs:23 the map is DashMap<NodeId, ConnectionPool> (dashmap 5, no Arc), so self.pools.get(node_id) at lines 137-140 yields a dashmap::Ref holding a synchronous shard read guard, and that guard is held across the awaited tokio::time::timeout(acquire_timeout, pool.get_connection(node_id)) at lines 144-148 (guard lives to line 189). release() likewise holds the guard from line 203 across run_reset_query().await (line 213 — a genuine backend network round trip per connection_pool.rs:474-478), close_connection().await (227), and return_connection().await (234). The redundant double timeout is also real: manager.rs:144 wraps get_connection in acquire_timeout while connection_pool.rs:306-310 applies the same acquire_timeout internally (manager.rs:84 copies the identical duration into PoolConfig). However, the finding does NOT matter for production: ConnectionPoolManager::acquire/acquire_with_mode/release/release_and_close have zero production callers — grep across src/, tests/, and benches/ shows the only invocations are the unit tests inside manager.rs (lines 411-449) and doc comments in src/pool/mod.rs. server.rs merely constructs the manager (line 437), add_node at startup (2175), background evict_idle (2107), close_all on shutdown (2115), get_stats for an admin endpoint (2134), and a has_active_lease check on disconnect (710) whose adjacent comment (715-716) confirms the lease path is not wired in. The actual data path opens a direct per-session TcpStream (connect_to_backend, server.rs:994-1021) and never goes through this manager, and benches/pooling.rs benchmarks the inner ConnectionPool, not the manager. The only writers that could block (add_node/remove_node, lines 90/97) are cold-path startup/topology operations. So this is a latent defect in dead scaffolding, not a production latency/throughput issue today.

</details>


### LoadBalancer node selection calls Handle::block_on inside the runtime and does per-call Vec collect + sort + endpoint clone; it is also dead code on the hot path

- **Location:** `src/load_balancer.rs:178`
- **Severity / category:** medium / async
- **Found by:** `pooling-balancing` auditor
- **Adversarial verification:** isReal=True matters=False confidence=high

**Evidence (verbatim from code at audit time):**

```
Ok(handle) => {
    handle.block_on(async { self.nodes.read().await })
}
```

**Impact:** select_for_read/select_for_write (lines 171-245) would panic with 'Cannot block the current thread from within a runtime' the first time they are called from a tokio worker, and otherwise block a thread on an async RwLock. Per selection they also build a Vec of all eligible nodes, sort it (eligible.sort_by_key, line 211 — O(n log n) per query), and clone the NodeEndpoint (heap Strings) at line 219. add_node/remove_node spawn fire-and-forget tasks (`tokio::spawn(async move { nodes.write().await.insert(...) })`, line 156), so registration is racy. In practice grep shows no production call sites — server.rs re-implements selection (select_node/select_primary_with_timeout) with its own linear scans of config.nodes under state.health RwLock per query — so this whole component is an unused parallel implementation.

**Fix:** Either delete LoadBalancer or make it the single async selection API: async fn select(...) with no block_on, snapshot node states into an ArcSwap'd immutable Vec rebuilt on health change so per-query selection is a lock-free indexed pick (round-robin atomic counter) with no sort/clone. Expected benefit: removes a panic landmine and gives O(1) lock-free selection instead of duplicated per-query scans.

**Verifier correction/nuance:** The code defects are real but the severity/category are wrong: this is dead code (confirmed — no callers of select_for_read/select_for_write anywhere), so it is a maintenance/landmine issue, not a medium async-performance issue. The correct action is deletion. The performance-relevant half of the suggestion (ArcSwap snapshot, lock-free indexed pick) targets the actual hot path in src/server.rs::select_node (line 1965), which is a separate finding against different code; that path scans only the handful of configured nodes per query under a read lock, so even there the win would be modest.

<details><summary>Verifier reasoning</summary>

Every factual claim checks out in src/load_balancer.rs: Handle::try_current()+handle.block_on(self.nodes.read()) at lines 175-184 (select_for_read) and 226-234 (select_for_write), which would panic 'Cannot block the current thread from within a runtime' on a tokio worker thread; per-call Vec collect (187-196), sort_by_key (211), endpoint.clone() (219, 242); fire-and-forget tokio::spawn writes in add_node/remove_node (156-158, 165-167). The dead-code claim is also correct and is precisely why this does not matter: select_for_read/select_for_write have zero call sites in src/, tests/, or benches/; the only LoadBalancer::new outside the module is in a #[cfg(test)] test at src/lib.rs:362 that merely constructs the struct. Production selection is implemented independently in src/server.rs (select_node at 1965, select_primary_with_timeout at 1357, using the separate LoadBalancerState at 210 and config.rs LoadBalancerConfig at 473). Because no production path executes this module, the panic cannot fire and the per-selection sort/clone costs are never paid — fixing or deleting it yields zero latency/throughput/scalability improvement. It is a legitimate code-hygiene finding (delete the module or migrate server.rs onto it), but as filed it is a performance non-issue.

</details>


### min_connections/min_idle is configured but never used: no connection warmup or replenishment after idle eviction

- **Location:** `src/pool/manager.rs:80`
- **Severity / category:** medium / architecture
- **Found by:** `pooling-balancing` auditor
- **Adversarial verification:** isReal=True matters=False confidence=high

**Evidence (verbatim from code at audit time):**

```
min_connections: self.config.min_idle as usize,
```

**Impact:** PoolConfig.min_connections is plumbed from config (default 2) into ConnectionPool but no code path reads it afterwards (grep: only config validation and this assignment). add_node never pre-opens connections, and the 30s eviction loop (server.rs:2099-2108 + connection_pool.rs evict_idle) drains idle connections to zero with nothing refilling them. After any quiet period every client pays the full backend connect + auth latency on first acquire; under bursty load this causes cold-start latency spikes and thundering-herd reconnects.

**Fix:** Add a warmup/replenish task: on add_node pre-create min_connections, and after evict_idle (or on a timer) top the idle set back up to min_idle per node, also have evict_idle retain at least min_connections. Expected benefit: steady-state P99 acquire latency drops from connect+auth cost (ms) to an idle-pop (µs) after idle periods.

**Verifier correction/nuance:** The finding is correct that min_connections/min_idle is dead config with no warmup or post-eviction replenishment (verified at manager.rs:80, connection_pool.rs:19/217-226/496-518, server.rs:2098-2110). But the impact analysis is wrong: the pool-modes ConnectionPoolManager is not on any data path (acquire is only called by tests), and its pools run in skeleton mode (no backend template/endpoint, so connections carry client: None and cost no connect/auth). The actual per-client backend connection is a per-session raw TcpStream opened in connect_and_authenticate (server.rs:995), which is unpooled and unaffected by the eviction loop. This is a dead-code/config-hygiene issue (config option silently ignored), not a latency issue; severity should be low and category dead-config rather than a performance defect.

<details><summary>Verifier reasoning</summary>

The code-level claim is confirmed: src/pool/manager.rs:80 assigns min_idle into PoolConfig.min_connections, and connection_pool.rs never reads it (add_node at 217-226 pre-creates nothing; evict_idle at 496-518 retains only by idle_timeout with no min floor; get_connection at 257-345 is purely lazy). The 30s eviction loop at server.rs:2098-2110 can indeed drain idle to zero. However, the performance impact is refuted on two grounds. (1) No production path acquires from this pool: ConnectionPoolManager::acquire/acquire_with_mode is called only from its own unit tests (manager.rs:420,440) and a doc example (pool/mod.rs:25). All pool_manager uses in server.rs are cold-path (stats 2133, add_node 2167-2175, eviction 2106, shutdown 2114, disconnect log 708-716). The real query path (connect_and_authenticate, server.rs:977-1022) opens a raw per-session TcpStream (line 995) and proxies auth directly (line 1011) — connection-per-session passthrough, never touching this pool. (2) Even if acquired, the pool is in skeleton mode: manager.rs:88-89 builds ConnectionPool::new without with_backend_template and uses add_node without an endpoint, so create_connection (connection_pool.rs:383-411) yields client: None — no TCP connect, no auth, just a struct allocation. The claimed "full backend connect + auth latency on first acquire" and "thundering-herd reconnects" cannot occur. Adding warmup/replenishment would pre-create empty client:None structs with zero latency benefit; real benefit requires first wiring the pool into the data path, a far larger change than suggested.

</details>


### test_on_acquire is ignored: validate_connection is a no-op that always returns true and is never called from get_connection

- **Location:** `src/connection_pool.rs:461`
- **Severity / category:** medium / io
- **Found by:** `pooling-balancing` auditor
- **Adversarial verification:** isReal=True matters=False confidence=high

**Evidence (verbatim from code at audit time):**

```
// `run_reset_query` / `ping_mut`. Returning `true` here
// matches the skeleton contract: "this handle looks alive."
let _ = client; // acknowledged present
```

**Impact:** PoolConfig.test_on_acquire defaults to true, but get_connection never invokes validate_connection, and validate_connection itself admits it cannot ping (needs &mut) and unconditionally returns Ok(true) for any non-Closed state. Connections silently killed by backend idle timeouts, restarts, or network drops are handed back to clients and fail on first use — after any failover or network blip every stale pooled connection turns into a client-visible error + retry, a latency/error cliff exactly when the system is already degraded.

**Fix:** On acquire of an idle connection older than some last-verified threshold, do a cheap liveness check (zero-byte read readiness on the socket, or an actual `SELECT 1` via client.as_mut() since get_connection owns the conn) and discard+retry on failure; honor test_on_acquire. Expected benefit: stale connections are recycled inside the pool instead of surfacing as query errors, with near-zero cost for recently-used connections.

**Verifier correction/nuance:** The finding correctly identifies that test_on_acquire is ignored and validate_connection is a no-op, but the impact analysis is wrong: this pool (src/connection_pool.rs + src/pool/manager.rs) is dormant scaffolding. Pooled connections in production always have client: None (no backend template or endpoint is ever wired in outside unit tests), and the pool's acquire path is never invoked by the server — real client queries flow over per-session TcpStreams opened in server.rs connect_and_authenticate (line 997). The 'stale pooled connections fail on first use after failover' cliff cannot happen via this code. It is at most a latent correctness gap (dead config + misleading comment referencing a nonexistent ping_mut) that would only matter if/when the pool-modes lease path is actually connected to the data path.

<details><summary>Verifier reasoning</summary>

The mechanical claims are accurate: src/connection_pool.rs:444-464 shows validate_connection returns Ok(true) for any non-Closed state, with the exact 'let _ = client; // acknowledged present' no-op at line 461 (and the comment's referenced 'ping_mut' method does not exist anywhere in the codebase). get_connection (lines 257-345) never calls validate_connection and never reads test_on_acquire; the only acquire-time check is age vs max_lifetime (line 287). test_on_acquire defaults to true (line 40) and is plumbed through config (src/config.rs:438, src/server.rs:432, src/pool/manager.rs:85) but is dead config. However, the claimed production impact is false. (1) The query-serving data path never touches this pool: client_loop (src/server.rs:729-769) proxies traffic over a per-session TcpStream opened directly via TcpStream::connect in connect_and_authenticate (src/server.rs:997). (2) ConnectionPoolManager::acquire/acquire_with_mode (src/pool/manager.rs:111-189) is never called from any production code — only from its own unit tests (lines 420, 440) and a doc-comment example; server.rs uses pool_manager only for add_node/evict_idle/close_all/get_stats and a no-op has_active_lease check (server.rs:708-718). (3) Even on the unused acquire path, ConnectionPoolManager::add_node (manager.rs:88-89) builds ConnectionPool::new() without with_backend_template and registers nodes without endpoints, so create_connection always produces client: None skeleton tokens — there is no live backend socket in any pooled connection that could go stale. with_backend_template/add_node_with_endpoint appear only in connection_pool.rs's own tests (lines 684, 688, 714). Therefore no stale-socket-handed-to-client failure mode exists, and implementing test_on_acquire would be exercised by nothing in production; it cannot improve latency, throughput, or scalability today.

</details>


## Refuted findings relevant here (do NOT "fix" these)
Two pool findings were adversarially REFUTED — their premises only hold for dead code paths:
- "Single global RwLock<HashMap<NodeId, NodePool>> serializes all nodes" — refuted: `ConnectionPoolManager` already keeps one `ConnectionPool` per node in a DashMap; the inner lock is per-node.
- "DISCARD ALL costs a round trip on every lease release" — refuted today because acquire/release are never called in production; once C wires them in, re-evaluate: consider piggybacking reset or using `DISCARD ALL` only when session state was touched.
