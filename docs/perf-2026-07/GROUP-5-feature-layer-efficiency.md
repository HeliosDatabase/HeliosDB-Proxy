# Group 5 — Feature-layer efficiency & leak fixes (all-features builds)

These layers are compiled into the `all-features` bundle most crates.io users
enable. The default build (`pool-modes` only) is unaffected by most of this,
so it ships **after** G1-G4. Two classes: (a) work done per query even when
the layer's config is off, (b) unbounded growth / global-lock chokepoints
when it is on.

## Highest impact

### 5.1 ha-tr journal: unbounded leak of every write + global lock, ON BY DEFAULT (HIGH)
`tr_enabled` defaults to **true** (config.rs:867). With `ha-tr` compiled,
every successful write calls `journal_write`: a fresh UUIDv4 keys a new
journal entry holding the full SQL, under **two** acquisitions of one global
`RwLock<HashMap>` (transaction_journal.rs:319/339). `commit`/`rollback` (the
only removal paths) are never called from the data path → the map grows one
entry per write forever; `/api/replay` then scans all of them O(total writes).
**Fix:** time-windowed / ring-buffered retention with a reaper; one lock
acquisition per `journal_write`; reconsider defaulting `tr_enabled` to false.

### 5.2 query-cache L1 leak per session (HIGH)
`l1_caches: DashMap<u64, Arc<L1HotCache>>` keyed by session-unique
`connection_id` (cache/mod.rs:190); `remove_l1_cache` is **never called**
(server.rs). Every session that runs one cacheable SELECT permanently leaks
its L1 cache (up to 500 entries × up to 1 MiB). Connection-churn → unbounded.
**Fix:** call `remove_l1_cache` on session teardown at the end of `client_loop`.

### 5.3 anomaly-detection: no off switch, ~4 allocs + shard-lock per query (HIGH)
The detector is constructed unconditionally (server.rs:939) — there is **no
`[anomaly] enabled` config** — so on any `anomaly-detection` build every Query
and routed Parse pays: `variables.try_read` + tenant clone, `anomaly_fingerprint`
(full scan + String), `query.to_string()`, a DashMap **entry (write)** lock on
`rate_windows`, plus a 60-element `Vec` alloc + mean/variance inside
`observe_and_score`; `sql_injection::scan` lowercases the whole SQL **twice**.
`rate_windows` (keyed by tenant→client-IP fallback) never evicts.
**Fix:** add `[anomaly] enabled` (checked before any allocation); O(1) running
sum/sum-sq in `RateWindow` (no per-query Vec); `get_mut` fast path before
`entry`; lowercase once; periodic `retain()` reaper on `rate_windows`.

### 5.4 Per-query global write locks at high concurrency (MED, several layers)
The chokepoints under 64+ clients when the owning layer is enabled:
- `rate_limit::metrics::record_decision` — global `decision_times_us`
  `RwLock<Vec>` write per query (metrics.rs:65). → atomic histogram / shard.
- `analytics::metrics::record` — global `recent: RwLock<Vec>` write +
  `remove(0)` O(100) memmove per query (metrics.rs:262). → `VecDeque` ring.
- `analytics::patterns::maybe_cleanup` — global `last_cleanup` write lock per
  query just to compare timestamps (patterns.rs:370). → atomic read-check first.
- `rate_limit::limiter::check_with_priority` — two DashMap `entry` (write)
  locks + two key clones per query (limiter.rs:426). → `get()` fast path.

## Disabled-cost (compiled-but-off) waste
- **N1** analytics: `query_text(...).to_string()` captured before the
  `state.analytics` Option check (server.rs:3034/3220) — 1 wasted full-SQL
  alloc/query on every analytics-compiled build. → `is_some().then(||…)`.

## Enabled-cost redundant allocations (MED/LOW, quality)
- **N2/N6** analytics fingerprint+intent: ~10 full-SQL allocs (multiple
  `to_uppercase`/`to_lowercase`, `Cow`→`String` on no-match regex). Uppercase
  once, chain Cows.
- **W1** rewriter parse: ~7 full-SQL allocs per query *even when no rule
  matches* (3× `to_uppercase`, redundant `normalize`). Uppercase once; skip
  normalize unless a fingerprint rule exists.
- **T1/T3** multi-tenancy: full `variables` HashMap clone per query
  (transformer `extract_tables` does 10× tail-`to_uppercase` inside a
  `filter_map`, plus per-word `to_uppercase`). Resolve tenant once per session
  (startup params are immutable); uppercase once and pass `&str` down.
- **C2/C3/C6/C7** cache: O(500) LRU `Vec::retain` + alloc per L1 hit (store
  last-access in the entry, drop the LRU Vec); double normalize on miss
  (`get` then `put`) — reuse the normalized query; `captured` buffer keeps
  appending past `max_result_size` (stop + mark uncacheable at the cap).

## Unbounded growth (MED, the leaks besides 5.1/5.2)
- **R3/R4** rate-limit: `sliding_window` stores one `u64` per event
  (≤480 KB/key) and `cleanup()` has no callers → per-key/per-IP leak.
  Bucketized window (60×1s counters) + an idle-key reaper.
- **C6** cache invalidation `table_keys`/`key_tables` never unregister on L2
  eviction. Unregister on evict.

## Risk & tests
- Risk: LOW-MED, but touches many modules → land as **sub-milestones** by
  layer (5.1+5.2 leaks first, then 5.3 anomaly, then 5.4 locks, then the
  redundant-alloc cleanups), each gated independently on its feature's
  regress script (`analytics-test.sh`, `cache-test.sh`, `rate-limit-test.sh`,
  `tenant-test.sh`, `rewrite-test.sh`, `tr-replay-test.sh`).
- New: a leak assertion (run N short cacheable-SELECT sessions, assert L1 map
  size returns to ~0) and a soak counter check on the journal map.
- Gate: full milestone protocol on `all-features`.

## Expected outcome
No unbounded memory on ha-tr / query-cache / anomaly / rate-limit; global
per-query lock contention removed at 64+ clients on enabled layers; disabled
layers cost ~0. Default build unchanged.
