# Group D — Edge-proxy: two-region cache with SSE invalidation push

**Goal:** turn the `edge-proxy` scaffolding (present but non-functional
end-to-end) into a working two-region result cache. A `role=home` proxy caches
reads, invalidates on writes, and **pushes** invalidations to subscribed edges
over Server-Sent Events; a `role=edge` proxy serves reads from a local cache,
forwards misses and writes to the home over PG-wire, and drops cached entries as
the home's SSE invalidations arrive. Coherence = last-write-wins on a
**home-authoritative** version clock with bounded staleness (SSE delivery lag +
TTL).

**Starting state (audit Milestone D):** `EdgeCache`/`EdgeRegistry` were
constructed but never consulted on the query path (**H6**); the admin `register`
handler dropped the `mpsc::Receiver` immediately so the first broadcast pruned
every edge (**H5**); `EdgeConfig` was never wired into `ProxyConfig` (**L5**);
the registry never GC'd (**M10**); the cache did an O(n) LRU scan per hit
(**M11**); versions were per-process and unsynchronized (**M12**); there was no
edge-role client at all.

Default-off and fully `#[cfg(feature = "edge-proxy")]`-gated: a default build
carries only the (always-present, unused) `[edge]` config section, so the hot
path is byte-for-byte unchanged when the feature is off.

## Architecture

Two channels between an edge and its home:

- **Data plane — PG-wire.** The edge lists the **home proxy's client port as its
  backend node**. Read misses and all writes flow through the *existing*
  `forward_simple_query` path to the home, and the captured PG-wire response
  bytes are what the edge caches. No JSON↔PG-wire re-encoding; the edge cache is
  a byte-exact replay cache.
- **Control plane — HTTP/SSE.** The edge holds a long-lived
  `GET /api/edge/subscribe` connection to the home admin and applies each
  `event: invalidate` frame to its local cache. `reqwest` (already a non-optional
  dep) drives the client; the home writes raw SSE frames from the per-edge
  `mpsc::Receiver` returned by `EdgeRegistry::register`.

Both roles run the same cache wiring in `forward_simple_query`; `role` decides
the invalidation source. The **home** mints versions and broadcasts; the
**edge** stamps entries in the home's clock domain (see below) and never
broadcasts — it only receives.

## Implementation (phases)

- **P0 `EdgeCache` rewrite.** One `Mutex<lru::LruCache<CacheKey, Arc<CacheEntry>>>`
  (O(1) get/insert, `Arc` entries — no `Vec<u8>` clone per hit) with a
  `by_table` reverse index so table-targeted invalidation walks only the
  affected keys instead of the whole map. `lru` is an optional dep pulled in
  only by `edge-proxy = ["dep:lru"]`.
- **P1 config.** `EdgeConfig` gains `enabled` (default false), `region`,
  `edge_id`, `liveness_window_secs`, `subscribe_gc_secs`, `allow_insecure_home_url`,
  and `default_ttl_secs` (integer seconds, matching every other `*_secs` knob).
  `ProxyConfig` carries an always-present `#[serde(default)] edge` section;
  `validate()` enforces the role invariants (edge needs `home_url` + a node;
  https bearer safety; feature-compiled).
- **P2 query path.** New self-contained `edge::fingerprint::analyze(sql, db, user)`
  (literal-folded fingerprint, tenant-scoped `params_hash`, table extraction).
  In `forward_simple_query`: edge read-lookup before the query-cache lookup;
  miss mints a version and captures the response via the shared
  `stream_until_ready_capture`; write invalidates locally and (home only)
  broadcasts. Simple and extended protocols and COPY FROM all invalidate.
- **P3 SSE + GC.** `handle_edge_subscribe` in the raw admin server (behind the
  same bearer gate, before the one-shot router) holds the connection open,
  streams invalidations, and heartbeats every 15s. `spawn_edge_maintenance`
  GC-prunes stale registrations; `broadcast` uses non-blocking `try_send` so one
  slow edge cannot stall the fan-out.
- **P4 edge client.** `EdgeClient::spawn` reconnect-loops the SSE subscription
  (capped backoff, idle-timeout), applies invalidations, and tracks the home
  clock via `observe_home_version`.

## Correctness — adversarial review

The diff went through a multi-lens adversarial review (finder lenses over
coherence, PG-wire replay, security, build/config, concurrency/perf; every
finding independently refuted + judged before fixing). **19 findings were
confirmed and fixed**; one (a stored-XSS in the admin dashboard) was verified as
a *pre-existing* `main` defect the branch neither introduces nor worsens and was
left for a separate hardening pass.

The load-bearing correctness fixes:

- **Home-authoritative clock (the core coherence bug).** The plan's original
  design stamped edge entries from the edge's *local* counter, which
  `observe_home_version` keeps ahead of the home clock — so home invalidations
  (`entry.version <= up_to_version`) could never match edge entries on a warm
  edge, silently degrading invalidation to TTL-only. Fixed: edge entries are
  stamped with the last **observed home version**; the store gate is an
  **invalidation-epoch** snapshot re-checked under the map lock (closing the
  read-after-invalidate TOCTOU on both roles).
- **Home-restart epoch.** `InvalidationEvent` carries a per-boot epoch; a
  reconnecting edge gets a `hello` frame and flushes + re-syncs its clock when
  the home's epoch changes, so a restarted home (version counter reset to 1)
  cannot no-op invalidations.
- **Session-context safety.** Any statement that alters session state
  (`SET search_path`, `SET ROLE`, RLS GUCs, …) makes the session permanently
  edge-ineligible, and session vars fold into the cache key — no cross-session
  or cross-tenant result bleed. Multi-statement simple queries, `SELECT … INTO`,
  and responses carrying async frames (Notification/Notice/ParameterStatus) are
  never cached.
- **Coverage.** Extended-protocol (Parse/Bind/Execute) writes and COPY FROM now
  invalidate; bare transaction-control verbs no longer trigger a fleet-wide
  flush.
- **Availability.** SSE writes are timeout-bounded (a wedged subscriber can't
  pin an admin permit); heartbeats refresh registry liveness (no idle-home churn
  loop); SIGHUP keeps the running `[edge]` config; the admin bearer is refused
  over plain http without an explicit opt-out.
- **Performance.** Table-targeted invalidation via the reverse index (no
  per-write O(entries) scan under the hot lock); the write path skips the
  fingerprint regex and the session-vars lock when only the table list is
  needed; the capture buffer is shared as refcounted `Bytes` (no per-miss copy).

A **second adversarial pass** targeted the (large) fix diff itself — the
extended-protocol/COPY invalidation state machine, query-cache regressions from
the shared-helper changes, and the post-epoch coherence logic. It confirmed and
fixed a further batch:

- **G1 (high, regression the first pass introduced):** the Close-drain removed a
  statement's invalidation metadata at Flush boundaries and after same-batch
  name reuse, silently disabling invalidation for a live DML statement under
  Npgsql's prepared-statement-replacement pattern. Fixed: the metadata is pruned
  only at a Sync and only for names not re-Parsed in the batch (the non-edge
  drain path is left byte-identical).
- **G2 / G3:** extended-protocol `COMMIT`/`END` and SQL-level `EXECUTE`/`CALL`/
  `DO` now trigger the conservative wildcard flush (they were invisible to both
  invalidation classifiers); the simple-path `END` COMMIT-synonym hole is
  closed.
- **G5 / G10 / G12:** a lost-backend COPY abort clears its deferred-invalidation
  stash; the manual `/api/edge/invalidate` version is clamped to the home clock
  (an oversized value would otherwise poison every edge's observed-home stamp);
  the https scheme check is case-insensitive (RFC 3986).
- **G7 (query-cache, safe-direction):** the shared read gate now over-rejects a
  SELECT whose *text* contains an interior `;` or the word `into` (even inside a
  string literal). This is a deliberate hit-rate-only trade-off — the raw scan
  is the only defense against multi-statement replay fabrication, and a
  literal-stripping pre-pass would reopen that hole under
  `standard_conforming_strings`. Pinned by tests; noted here as a query-cache
  behaviour change.

### Known limitations / tracked follow-ups

- **Commit-time flush is a fleet-wide wildcard.** Both the simple and extended
  paths full-flush every edge on each `COMMIT`/`END`/`EXECUTE`/`CALL`/`DO` — safe
  and conservative, but it craters edge hit-rate on write-heavy transactional
  (JDBC/ORM) workloads. Follow-up: track the union of tables written while a
  session's transaction is open and flush only that set at commit.
- **Invalidation fires on success paths.** A write whose response relay errors
  *after* the statement reached the backend does not invalidate (bounded by TTL;
  over-invalidation-safe to add later). The identical Ok-arm-only placement
  exists for the shipped query-cache and ha-tr journal on `main` — a shared
  follow-up, not introduced here.
- **Never-Sync Flush-only pipelines** accumulate referenced-table state until
  their Sync (bounded by the pending-bytes cap), and **two COPY FROMs in one
  extended batch** share a single deferred-invalidation slot (rare; the second
  may fall back to TTL).
- **query-cache multi-statement replay (G9)** and **notice-suppression (G8)** are
  pre-existing `main` behaviours surfaced by this review, out of scope for this
  branch's release.

## Verification

- **Unit tests** across `edge::{cache,fingerprint,registry,client}`, config
  validation, and the admin SSE handler. Full gate green: `fmt`, `clippy
  -D warnings` on all four CI feature sets, `check --features msrv-features`,
  and `test --lib` on default (282) and all-features (1481). Query-cache
  feature tests stay green (380) — no regression from the shared-helper changes.
- **Live two-process end-to-end** (`scripts/regress/edge-proxy-test.sh`): a
  `role=home` and a `role=edge` proxy against real PostgreSQL, exercising the
  full loop. **12/12 assertions pass:**
  - two-hop SCRAM relay (edge → home → PG) authenticates transparently;
  - the edge's SSE subscription registers at the home;
  - a repeated read is served from the edge cache with no backend round-trip
    (the edge returns `1` while the value mutated directly on PG reads `2`);
  - a write **through the home** pushes an SSE invalidation the edge applies in
    under 5s — well under the 60s TTL, so push (not expiry) drove the refresh;
  - a write **through the edge** forwards over PG-wire and lands on PG.
- **Regression + scalability gate** (candidate branch vs `main` baseline, both
  all-features release, `scripts/regress/bench-scalability.sh`, pgbench `-S`
  scale 20). Since edge-proxy is default-off, the candidate must match the
  baseline — it does, within run-to-run noise (proxy/session c64 62.3k vs 62.2k
  tps; proxy/transaction c64 41.3k vs 42.1k tps; backend-connection efficiency
  identical at 33/35 for 32 bursty clients; direct-PG itself varied ±4% between
  runs, the noise floor). The `cache-test` (query-cache, 4/4) and `admin-auth`
  (5/5) batteries pass on the candidate.
