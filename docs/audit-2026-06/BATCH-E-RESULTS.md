# BATCH E — Verify-then-fix: results (2026-06-13)

All 35 previously-unverified audit findings were re-verified against the
**current** (post-A–D) code by a 35-agent workflow. Full per-finding verdicts:
`batch-e-verdicts.json`. Headline: only **6 were actionable**; **1 was already
fixed by A–D**; **28 are real but do not matter** because the cited module is
not wired into any runtime data path (dead code) or is feature-gated and off the
wired path. Optimizing dead code yields no runtime benefit, so those are
deferred until their module is actually wired in.

## Fixed in Batch E

| # | Finding | Where | Notes |
|---|---------|-------|-------|
| 8 | Health map read takes a tokio `RwLock` + `.await` on **every query** | `server.rs` | `health: RwLock<HashMap>` → `ArcSwap<HashMap>`. Each route decision's health lookup is now one lock-free atomic load (`load_full()`), no await, no semaphore. The periodic checker publishes a new snapshot via `store`. The lb_state half was already fixed in A (atomic rr_counter). |
| 14 | Failover detection floor: serial health probes + 500ms writer poll | `server.rs` | Health sweep now probes all nodes concurrently via `JoinSet` (one slow node no longer delays detection on the others); the blocked-writer failover poll dropped 500ms → 100ms. Lowers failover detection + recovery latency. |
| 24 | Anomaly rate-spike detector serializes every query on one global write lock | `anomaly/mod.rs` | `rate_windows`/`auth_windows` `RwLock<HashMap>` → `DashMap` (per-key shard). |
| 25 | Novel-query detector write-locks per query + **unbounded fingerprint set (memory leak)** | `anomaly/mod.rs` | Common already-seen path now takes a shard read; the set is bounded at 100k fingerprints (clears on overflow — informational detector). Fixes a real slow leak on high-cardinality SQL. |

Verified: 1308 unit tests green; live battery green on PostgreSQL 18.4 (single +
2-node read/write-split) and HeliosDB-Nano 3.57. (#24/#25 are behind the
`anomaly-detection` feature; exercised in all-features builds.)

## Already fixed by Batches A–D
- #9 — per-message full-buffer clone + double-decode in the response loop → removed by the Batch A/B streaming relay.

## Deferred (real, but verified NOT worth doing now)

**Delicate / pairs with a later batch:**
- #32 — `proxy_authentication` polls the client with a fixed 100ms timeout per round. On the default per-connection path; costs up to ~100ms of one-time connection-setup latency under split reads / multi-round SCRAM. Fix is a `select!`-based event-driven auth relay — delicate, and it pairs naturally with **Batch F's SCRAM work**, so deferred there.
- #31 — anomaly observation copies the full SQL (`sql.to_string()`) per query even when no detector fires. The RFC3339 half was already fixed in A. The remaining copy needs a borrowing `QueryObservation<'a>` (Cow/lifetime) refactor; feature-gated; deferred.

**Real but dead code — 28 findings (do NOT optimize until the module is wired):**
The verifiers confirmed each of these modules has **no production caller on the
data path**, so the per-query cost the finding describes is never actually paid:
- routing/rewrite/analytics: #0 fingerprinter, #1 rewriter regex, #2 statistics eviction, #3 routing metrics task, #4 query_router classification, #5 rewriter parser, #6 WorkflowTracer, #7 schema_routing — none invoked from `route_and_forward`.
- HA/replay: #10 transaction_journal, #11 journal unwired, #12/#13 failover_replay, #15 topology provider — the journal/replay plane is inert (becomes live with **G2**, which wires the journal).
- caching tiers: #16 cache/mod, #17 l1_hot, #18 invalidator blocking-IO, #19 l3 per-op TCP, #20 edge cache, #21 heatmap, #22 distribcache l1, #23 l2 warm — cache stacks not consulted on the wired path.
- tenancy/auth: #26 rate_limiter, #27 tenant transformer, #28 transform_query clone, #29 api-key O(n) scan, #30 ApiKeyManager lock — `multi_tenancy`/`auth` not invoked per-query/connection.
- plugins: #33 hook-name Vec clone (guarded by the Batch-A `has_hook` fast path, so only paid when a plugin is actually subscribed), #34 extended-protocol bypasses plugin hooks (a **coverage** gap, not a perf issue — wiring it ADDS work; belongs with a plugin-coverage task).

When any of these subsystems is wired into the data path (e.g. the journal via
G2, or pooling/auth via F), re-open the corresponding finding — the proposed fix
is recorded per-item in `batch-e-verdicts.json`.
