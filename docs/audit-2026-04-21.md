# HeliosProxy — Code & Performance Audit

**Date:** 2026-04-21
**Scope:** `src/` tree (37 modules, ~80 000 LOC). 1 096 tests passing at HEAD `8c43de9`.
**Method:** Explore-agent sweep across all modules, then direct verification of every finding flagged HIGH/MED by reading the source. Findings 20-21 rely on the agent's line references and have not been independently line-checked.

---

## Executive summary

HeliosProxy compiles clean, tests pass, and the wire path is solid enough to run the TR demos. Beneath that surface there are **seven correctness defects** and a cluster of hot-path allocations / lock-contention spots that together cap throughput well below what the design implies. Fixing the top 5 items is ~8 hours of work and should produce **2-4× throughput** on a mixed read/write workload while eliminating one latent panic and one silent-overshoot bug in `max_connections`.

Three independent pooling implementations coexist in the tree; this audit treats that as a structural issue (§22-25) rather than a blocker for the perf work below.

---

## 🔴 Correctness bugs (fix first — defects, not tuning)

### 1. Semaphore permit released before connection is handed out
**File:** `src/connection_pool.rs:188-216`
**Problem:** `Ok(Ok(_permit)) => { … return Ok(conn); }` drops `_permit` at the match-arm boundary, before the `PooledConnection` reaches the caller. `max_connections` is only enforced against concurrent calls to `get_connection()` itself, not against actual in-use connections.
**Impact:** Pool can overshoot `max_connections` without bound under sustained load.
**Fix:** Store the `OwnedSemaphorePermit` inside `PooledConnection`; release it in `return_connection`/`close_connection`.

### 2. `RwLock` held across `.await` in pool checkout hot path
**File:** `src/connection_pool.rs:175-241`
**Problem:** `self.pools.write().await` is held while awaiting `semaphore.acquire_owned()` and `create_connection()`. All node pools share one lock, so concurrent checkouts on different nodes serialize through this mutex.
**Impact:** tokio-level stall under load; latency spikes; throughput cap.
**Fix:** Hold the lock only long enough to clone an `Arc<Semaphore>` and read idle-conn metadata, then drop before `.await`. Better: per-node `Arc<NodePool>` with its own `parking_lot::Mutex<Vec<_>>`.

### 3. `handle.block_on()` inside functions callable from async context
**File:** `src/load_balancer.rs:187, 239`
**Problem:** `select_for_read()` and `select_for_write()` call `tokio::runtime::Handle::try_current()` then `block_on` on the RwLock. Calling `block_on` from within the same runtime panics: *"Cannot start a runtime from within a runtime"*.
**Impact:** Latent panic. Not wired into `server.rs` today (grep confirms), but anyone adopting the `LoadBalancer` API will hit it.
**Fix:** Make both methods `async fn` and await the read lock directly. Drop `Handle::try_current()` entirely.

### 4. Fire-and-forget `tokio::spawn` for node add/remove
**File:** `src/load_balancer.rs:157-174, 178-184`
**Problem:** `add_node`/`remove_node` are `sync` but spawn a task to modify state. After `add_node(x)` returns, `select_for_read` may still not see `x`.
**Impact:** Flaky tests, non-deterministic startup behavior.
**Fix:** Make them `async fn` and await the write directly.

### 5. `Random` strategy is not random under burst load
**File:** `src/load_balancer.rs:318-325`
**Problem:** Seeds from `SystemTime::now().nanos` per call. Two rapid calls on the same nanosecond pick the same node. Under high QPS, "random" degrades to "last-chosen repeatedly".
**Fix:** `rand::thread_rng().gen_range(0..nodes.len())`.

### 6. Timed-out concurrency waiters leak a slot
**File:** `src/rate_limit/concurrency.rs:158-180`
**Problem:** When `acquire_timeout` times out, the stale `Sender` stays in the waiters queue. The next `release()` pops it and `send()` silently fails — but `active` was decremented for nothing.
**Impact:** Under heavy timeout churn, `active` drifts below true utilization; metrics become wrong; eventually a queued waiter may never be woken.
**Fix:** On timeout, remove your sender from the queue before returning the error (use a shared cancellation flag or re-scan the queue).

### 7. Three separate connection-pool implementations
- `src/connection_pool.rs` — top-level "skeleton" with `// TODO: Implement actual connection creation` at line 274.
- `src/pool/` — feature-gated `pool-modes`, used by `server.rs`.
- `src/server.rs:59` — internal `NodePool` struct.
**Impact:** The top-level `connection_pool.rs` is imported by nothing critical yet lives in the public API and contributes the bugs above. Maintenance tax + ambiguity for users.
**Fix:** Either delete `connection_pool.rs` or consolidate into a single real implementation.

---

## 🟠 High-impact performance (hot path — directly hurts throughput)

### 8. `read_cstring` allocates a `Vec<u8>` per protocol string
**File:** `src/protocol.rs:326-340`
**Called from:** Query, Parse (name + query), Bind (portal + statement), Execute (portal), ErrorResponse fields, CommandComplete tag, StartupMessage params.
**Fix:** Scan for `\0` offset, `payload.split_to(pos)` → `Bytes` (zero-copy), then `std::str::from_utf8(&bytes)`. Change return type to `Bytes` or `String` only where truly needed.

### 9. `BindMessage::parse` copies every parameter value with `.to_vec()`
**File:** `src/protocol.rs:441`
**Problem:** `payload.split_to(len as usize).to_vec()` — new `Vec<u8>` per parameter, per prepared-statement execution. 10-param prepared statement @ 10k qps ⇒ 100k allocs/s of raw data immediately discarded.
**Fix:** Change `param_values: Vec<Option<Vec<u8>>>` → `Vec<Option<Bytes>>`; use `.freeze()` instead of `.to_vec()`. `Bytes` is ref-counted — no copy.

### 10. `L1HotCache::get` takes a **write** lock on every lookup
**File:** `src/cache/l1_hot.rs:41-64`
**Problem:** Uses `std::sync::RwLock` (not parking_lot) and `entries.write()` for every `get()`, even on a hit, because it calls `entry.touch()` and `update_lru()`. Cache hits are serialized through a write lock — defeats L1's purpose.
**Fix:**
- `std::sync::RwLock` → `parking_lot::RwLock` (no poisoning, ~2× faster uncontended).
- Split: read-lock peek → if hit-and-fresh, clone result and release; optionally schedule LRU update asynchronously. For strict LRU, use DashMap + per-entry `AtomicU64` sequence counter.

### 11. `query.trim().to_uppercase()` on every routing decision
**File:** `src/routing/query_router.rs:146` (also `src/rewriter/mod.rs` `evaluate_condition`)
**Problem:** Allocates a new `String` per query to match a 19-variant enum prefix.
**Fix:** Byte-level match with `eq_ignore_ascii_case`:
```rust
fn starts_with_i(q: &str, kw: &str) -> bool {
    let q = q.trim_start().as_bytes();
    q.len() >= kw.len() && q[..kw.len()].eq_ignore_ascii_case(kw.as_bytes())
}
```

### 12. `ConnectionPool` metrics use `RwLock<PoolMetrics>` — 15 `.write().await` in hot path
**File:** `src/connection_pool.rs:171, 205, 228, 235, 261, 287, 299, …`
**Fix:** Replace `PoolMetrics` fields with `AtomicU64`; `metrics()` snapshots to a plain struct on demand.

### 13. `CommandComplete::rows_affected` collects to `Vec<&str>` just to get last element
**File:** `src/protocol.rs:590-597`
**Fix:** `self.tag.split_whitespace().last().and_then(|s| s.parse().ok())`.

### 14. `Vec::remove(idx)` inside pool connection scan
**File:** `src/connection_pool.rs:195`
**Problem:** `Vec::remove` shifts all subsequent elements (O(n)). 100 conns/node ⇒ ~100 memmoves per checkout.
**Fix:** `Vec::swap_remove` if order doesn't matter, or use `VecDeque`.

---

## 🟡 Medium impact

### 15. `std::sync::RwLock` where `parking_lot::RwLock` is already a dep
`src/cache/l1_hot.rs` (and likely elsewhere). parking_lot is ~2× faster uncontended, no poisoning, smaller. Already used in `concurrency.rs` and `rewriter/mod.rs`.
**Fix:** Codemod `std::sync::RwLock` → `parking_lot::RwLock` where the `.unwrap()` / `.ok()?` only guards against poisoning.

### 16. `println!` in production
**File:** `src/rewriter/mod.rs:184-187` — bypasses tracing. Lost or uncorrelated in containers.
**Fix:** `tracing::info!`.

### 17. Unbounded `Clone` derivations on message types
**File:** `src/protocol.rs:349 (QueryMessage), 370 (ParseMessage), 411 (BindMessage), 464 (ExecuteMessage), 488 (ErrorResponse), 570 (CommandComplete)`.
Today clones deep-copy `Vec<u8>` param values. Once #9 lands, clones become cheap — but `Clone` still invites accidental copies.
**Fix:** Remove `Clone` from types that aren't meant to be cloned; audit callers.

### 18. Missing `#[inline]` on tiny hot matchers
**File:** `src/protocol.rs:84-118 (MessageType::from_tag)`, `121-155 (to_tag)`, `550-556 (TransactionStatus::from_byte)`. Cross-module, called per-message. LLVM may not inline.
**Fix:** `#[inline]` annotations.

### 19. Encode paths use `BytesMut::new()` with no capacity hint
**File:** `src/protocol.rs:363, 399, 479, 526, 583`. Each growth reallocates.
**Fix:** `BytesMut::with_capacity(estimated_size)` where the size is predictable (Query: `payload.len() + 1`; Parse: `name.len() + query.len() + 2 + 2 + 4*param_types.len()`; etc.).

### 20. `rewriter::get_rules()` clones the entire `Vec<RewriteRule>` *(not line-verified)*
**Fix:** Return `Arc<Vec<RewriteRule>>`.

### 21. `rate_limit/limiter.rs` clones the config per check *(not line-verified)*
**Fix:** Borrow the guard or wrap in `Arc`.

---

## 🟢 Structural / clean-up

### 22. Swallowed errors (`let _ = …`)
`src/pool/manager.rs:108, 227, 254` — silent failures on close/return. At minimum log with `tracing::warn!`.

### 23. Dead `savepoints: Vec<String>` field
`src/server.rs:152-164` — unused in `TransactionState`. Remove or wire up.

### 24. `Arc<RwLock<Vec<_>>>` where scans dominate
`src/routing/query_router.rs:21, 38` — `nodes: Arc<RwLock<Vec<NodeInfo>>>`. For large N a DashMap keyed by name is faster for lookups and avoids full-vector scans on `remove_node`.

### 25. Feature-flag smoke test
`cargo check --no-default-features` and `cargo check --no-default-features --features <each>` per feature — easy to flag modules that silently pull in dead deps. Worth adding as a CI matrix job.

---

## 📊 Top 5 wins (impact ÷ effort)

| # | Change | Files | Effort | Why it wins |
|---|---|---|---|---|
| 1 | Fix semaphore permit leak | `connection_pool.rs:188-216` | 1 h | Correctness — `max_connections` actually enforces the limit |
| 2 | Don't hold `pools.write().await` across `.await` | `connection_pool.rs:175-241` | 2-3 h | Removes serialization of all pool checkouts across all nodes |
| 3 | Zero-copy `read_cstring` + `BindMessage` params as `Bytes` | `protocol.rs:326-340, 416, 441` | 2 h | Eliminates allocation per protocol string/param |
| 4 | Pool metrics → `AtomicU64` | `connection_pool.rs` (15 call sites) | 1 h | Removes RwLock.await just to bump counters |
| 5 | L1 cache: `parking_lot::RwLock` + read-path on hit | `cache/l1_hot.rs` | 1.5 h | Cache hits become sub-µs again |

**Combined estimate:** ~8 h for all 5 → ballpark **2-4×** throughput on mixed read/write workload, one correctness bug eliminated, one latent panic removed.

---

## 🏃 <30-min quick wins

- `CommandComplete::rows_affected` → iterator (`protocol.rs:590`)
- Replace `println!` → `tracing::info!` (`rewriter/mod.rs:184-187`)
- Add `#[inline]` to `MessageType::from_tag/to_tag` (`protocol.rs:84, 121`)
- Replace `SystemTime::now().nanos` RNG → `rand::thread_rng()` (`load_balancer.rs:318-325`)
- Delete dead `savepoints` field (`server.rs:152-164`)
- Add `with_capacity` hints to all `encode()` buffers (`protocol.rs:363, 399, …`)

---

## Modules not personally re-verified

Read directly: `protocol.rs`, `connection_pool.rs`, `cache/l1_hot.rs`, `load_balancer.rs`, `rate_limit/concurrency.rs`, `routing/query_router.rs`, top of `server.rs`.

Unexplored in depth (where further audits may yield more): `pool/` (the real pool manager used by `server.rs`), `distribcache/`, `failover_replay.rs`, `transaction_journal.rs`, `plugins/`, `graphql/`, `schema_routing/`, `multi_tenancy/`.

---

## Planned implementation order

Start with correctness (#1), then unblock the hot path (#2 and #4 together — both live in `connection_pool.rs`), then allocations (#3), then cache (#5). Before committing #1-2, verify whether `src/connection_pool.rs` is truly dead code vs. a seed for a production pool (§22) — if dead, delete it and apply the fixes inside `src/pool/` instead.

Each of the Top 5 is a self-contained commit. Benchmark before/after with the existing Criterion harness in `benches/pooling.rs` and `benches/routing.rs`.
