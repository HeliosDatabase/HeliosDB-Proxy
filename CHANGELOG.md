# Changelog

All notable changes to HeliosProxy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`[limits]` config section** — ten operational safety bounds that were
  hardcoded `const`s in `src/server.rs` are now tunable via `proxy.toml`. Each
  default reproduces the prior constant exactly, resolved once at startup, so a
  config without a `[limits]` block is byte-for-byte unchanged. Keys (default):
  `max_cancel_keys` (100000), `startup_timeout_secs` (30),
  `backend_write_timeout_secs` (30), `backend_read_timeout_secs` (30),
  `client_write_timeout_secs` (60), `reprepare_timeout_secs` (15),
  `max_prepared_statements` (8192), `max_prepared_bytes` (67108864 / 64 MiB),
  `max_pending_bytes` (67108864 / 64 MiB),
  `max_total_idle_backend_conns` (8192, pool-modes), and
  `pool_reap_interval_secs` (30). `validate()` rejects a `0` for any of these
  (a safety bound, not "unbounded") with a key-named error.
- **`[anomaly]` config section** — the in-process anomaly detector previously
  ran on a hardcoded `AnomalyConfig::default()` plus a `MAX_SEEN_FINGERPRINTS`
  module const with no way to tune it. Its eight tunables are now exposed via
  `proxy.toml`, defaults reproducing the prior behavior exactly: `rate_window_secs`
  (60), `spike_z_threshold` (3.0), `auth_window_secs` (60), `auth_critical_count`
  (10), `auth_warning_count` (5), `event_buffer_size` (1024), `emit_novel_queries`
  (true), and `max_seen_fingerprints` (100000). `validate()` rejects degenerate
  values (windows/buffer/fingerprint-cap `> 0`, `spike_z_threshold` finite and
  `> 0`, `auth_critical_count >= 1`, `auth_warning_count <= auth_critical_count`).
  The detector is built once at startup, so changing `[anomaly]` requires a
  restart (a SIGHUP reload does not rebuild it).
- **`/admin/kv/<plugin>/<key>` admin endpoints** — the per-plugin KV store
  (`KvBackend`, read by plugins through their `kv_get`/`kv_set` host imports)
  can now be read, written, listed, and deleted from outside the WASM sandbox,
  so operators can push a plugin's runtime config (budgets, region maps, mask
  rules, allowlists) without a restart. `GET /admin/kv/<plugin>/<key>` returns
  `{"plugin","key","value"}` (404 if absent), `GET /admin/kv/<plugin>/`
  (trailing slash) lists the namespace as `{"plugin","keys":[...]}`,
  `PUT` sets a value (UTF-8 body via `from_utf8_lossy`), and `DELETE` removes one
  (idempotent 200). A trailing-slash list accepts an optional `?prefix=` filter;
  any query string is stripped before the plugin/key split, so `?…` never leaks
  into a stored key, and an empty `<plugin>` segment (`/admin/kv//<key>`) is
  rejected `400`. All four sit behind the normal admin bearer gate; the build
  returns `501` without `--features wasm-plugins` and `503` when no plugin
  manager is attached. Four `[plugins]` caps bound writes and are tunable
  (`0` = unlimited): `kv_max_value_bytes` (default 65536, now bounds a single
  key OR value), `kv_max_keys_per_plugin` (default 1024), `kv_max_plugins`
  (default 256, bounds how many `<plugin>` namespaces can exist so a token-holder
  cannot exhaust memory by writing to unboundedly-many namespace names), and
  `kv_max_total_bytes` (default 67108864 / 64 MiB) — a total-footprint backstop
  that sums each entry's key + value bytes plus each live namespace's name bytes
  and keeps the whole store within a survivable ceiling regardless of the
  per-axis product (which could otherwise retain tens of GiB). A PUT
  past a cap returns `413` (and the in-WASM `kv_set` returns `-1`); an oversized
  body is rejected before it is copied. Overwriting an existing key never trips
  the key-count cap, writing to an existing namespace never trips the namespace
  cap, and deleting a namespace's last key frees its slot (the reclaimed bytes
  are subtracted from the total-footprint counter too).
  Keys must not contain `?`: a query string is stripped before the plugin/key
  split (so `?prefix=` can filter a listing), which means a plugin-created key
  containing `?` is listable but not addressable via GET/DELETE over the admin
  surface.
- **`benches/protocol.rs`** — a Criterion benchmark covering the PG-wire
  per-query hot path that every client frame and backend response flows
  through, previously uncovered by the pool/routing benches (so a regression
  there was invisible to quality gate 3). Three groups —
  `protocol/decode_message`, `protocol/encode_message`, and
  `protocol/query_text` — each run over three payload sizes (a trivial
  `SELECT 1`, a ~60-char `WHERE` query, and a deterministically-built ~1 KiB
  `IN (...)` statement) with `Throughput::Bytes` so a regression surfaces as
  both a per-call delta and a bytes/sec change. Feature-free: it exercises only
  the always-public `protocol` API, so it compiles under every feature set.

### Changed

- **CI now lints test code (`cargo clippy --tests`)** — both clippy invocations
  in `.github/workflows/ci.yml` gained `--tests`, so `#[cfg(test)]` modules and
  the `tests/` integration crate are held to the same `-D warnings` bar as the
  library. Pre-existing `clippy::field_reassign_with_default` warnings in test
  code (converted `let mut x = T::default(); x.field = …;` sequences into
  struct-literal `T { field: …, ..Default::default() }` initializers, behavior
  unchanged) were cleared so the gate starts green.
- **Rewrote `docs/transaction-replay.md` and `docs/topology-providers.md` against
  the current code.** Both documents were conceptually dated. The TR deep dive is now
  grounded in `src/transaction_journal.rs`, `src/failover_replay.rs`,
  `src/failover_controller.rs`, `src/switchover_buffer.rs`, `src/replay/mod.rs`, and the
  `tr_enabled` / `tr_mode` / `write_timeout_secs` keys — correcting invented keys
  (`tr_max_journal_bytes`, `switchover_drain_timeout_secs` never existed), the
  `write_timeout_secs` default (30, not 15), the default `tr_mode` (`session`), the
  text-format (not binary) replay parameter path, and the fact that session-state/cursor
  migration exists only as unwired library modules (`src/cursor_restore.rs`,
  `src/session_migrate.rs`) not reachable from the replay path, while
  `FailoverController`/`PrimaryTracker` are library components rather than
  daemon-wired. The topology doc now separates the daemon's
  static-role-plus-health primary tracking (surfaced at `/topology`) from the
  `TopologyProvider` library abstraction, and notes the PostgreSQL provider is constructed
  programmatically (not from `[[nodes]]`). Each doc carries a "last verified against"
  commit line.

### Fixed

- **`/healthz`, `/livez`, `/readyz` admin routes** — these three
  Kubernetes-style probe paths were already token-exempt in the admin auth gate
  but had no handler, so they fell through to the catch-all and returned `404`.
  They now route to the same handlers as their slash-form twins (`/healthz` →
  `/health`, `/livez` → `/health/live`, `/readyz` → `/health/ready`), returning
  byte-for-byte identical responses. Because they are token-exempt, orchestrators
  can use `/livez` and `/readyz` for unauthenticated liveness/readiness probes
  even when `admin_token` is set (the slash-form `/health/live` and
  `/health/ready` remain token-gated, unchanged).
- **Embedded admin dashboard usable with `admin_token` set** — v1.4.0 made
  token-gating the recommended posture, but the embedded web UI
  (`src/admin_ui.html`, served at `/` and `/ui`) sent no `Authorization`
  header on any of its `fetch()` calls, so with a token set every panel
  `401`ed — the secure configuration broke the dashboard. The UI now wraps
  `window.fetch` once to inject `Authorization: Bearer <token>` (from the tab's
  `sessionStorage`, key `helios_admin_token`) into every request; on a `401`
  it prompts once per page load for the token and reloads. A **token** button
  in the header bar clears the saved token so a wrong one can be re-entered.
  The static shell (`GET /`, `/ui`) is now token-exempt so the page can load
  and prompt — it carries no privileged data, and every API call it makes is
  still individually gated. Without a token, behavior is unchanged (no prompt).

## [1.4.0] - 2026-07-09

Minor release — data-path performance, relay robustness, security hardening
across the admin API and the HTTP/MCP/GraphQL gateways, an edge/geo result-cache
mode, and config-loader correctness. Covers the 2026-07 performance & stability
program (PRs #26–#37) plus a security/stability follow-up sweep.

### ⚠️ Breaking / behavior changes — read before upgrading

- **The admin API now binds loopback by default.** The `--admin` CLI default and
  the `admin_address` default changed from `0.0.0.0:9090` to `127.0.0.1:9090`,
  and binding the admin API to a **non-loopback** address without an
  `admin_token` is now **refused at startup**. To keep an off-loopback admin
  bind, set `admin_token` (recommended) or `admin_allow_insecure = true`.
- **`[health] check_interval_secs = 0` is now rejected at startup.** Previously
  it started and silently disabled the health-check task; set a positive value.
- **Config files now undergo `${VAR}` / `${VAR:-default}` substitution at load.**
  A `${VAR}` with no default and an unset environment variable now fails fast
  (naming the variable) instead of being passed through literally.

### Added

- **Edge / geo result-cache mode** (`edge-proxy` feature, `[edge]` config section):
  a two-region result cache with a home-authoritative clock, SSE invalidation
  push, and a PG-wire data plane. Admin surface: `GET /api/edge/subscribe` (SSE
  stream), `POST /api/edge/register`, `POST /api/edge/invalidate`.
- **`[pool_mode] skip_clean_reset`** — skip `DISCARD ALL` for provably
  session-neutral pooled connections (opt-in; default off).
- **`mcp.auth_token`** — bearer authentication for the MCP agent gateway, which
  was previously open by default.
- **`admin_allow_insecure`** — opt in to an anonymous non-loopback admin bind
  (see the breaking-changes note above).
- **Environment-variable substitution in config files** — `${NAME}` and
  `${NAME:-default}` are expanded by `ProxyConfig::from_file` (env lookup only,
  no shell). Unrecognized top-level TOML keys/sections are now logged at startup
  (`tracing::warn!`) instead of being silently ignored.

### Changed

- **LISTEN/NOTIFY notifications are delivered to idle sessions immediately**
  (previously deferred to the session's next query); idle-relay `Flush`
  round-trip latency dropped from ~202 ms to ~0.8 ms.
- Pooled-connection reset is now conditional for clean connections (see
  `skip_clean_reset`).

### Fixed

- **MCP `read_only` guardrail bypass.** The gateway inspected only a statement's
  leading verb, so a multi-statement batch (`SELECT 1; DROP TABLE t`) or a
  data-modifying CTE (`WITH x AS (INSERT … RETURNING *) SELECT …`) slipped past
  it — and multi-statement input also defeated agent-contract allow-lists. It is
  now closed by a comment/quote/dollar-quote-aware lexical guard (reject
  multi-statement, scan for data-modifying CTEs) **and** a backend
  `default_transaction_read_only` backstop on the fresh per-call connection.
  (Also listed under Security.)
- **Underflow panics on the live query path.** `Instant − Duration` panics when a
  configured window exceeds process uptime (the monotonic clock starts near zero
  at boot); ten sites across the circuit breaker (`sliding_counter`, `adaptive`)
  and analytics (`patterns`), plus a wall-clock age in the distribcache heatmap,
  now use `checked_sub` / `saturating_sub`.
- **Auth relay** slow-client deadlock and `ErrorResponse` blindness (the auth
  relay is now event-driven).
- **Connection pool** COPY-hang, poisoned-park, and pool-key identity leakage.
- **Session leaks** — RAII session guard and a DashMap-backed session table;
  the health-check loop is now supervised.
- **`ha-tr` transaction journal** is bounded (50 000 journals, oldest-first
  eviction at capacity) — previously an unbounded leak; note the replay-window
  implication for `/api/replay`. Per-session L1 caches are reclaimed.
- **The shipped example configs now load.** `${VAR:-default}` was documented but
  unimplemented and several files were invalid TOML; the loader now substitutes,
  and a CI test loads every `config/*.toml` through the real `from_file` path.

### Security

- **Gateway request hardening (HTTP SQL / MCP / GraphQL):** an 8 MiB body cap
  (rejected with 413 before allocation), a 15 s request deadline, 100-header /
  64 KiB header caps, constant-time bearer comparison, and the new
  `mcp.auth_token`.
- **Admin-API exposure hardening:** loopback-by-default, refusal of anonymous
  non-loopback binds, a `MAX_ADMIN_CONNS = 256` connection cap, a 15 s admin
  read timeout, an 8 MiB admin body cap, and header caps.
- **Pre-auth protocol crash guards:** undersized startup / message frames
  (`len < 8` / `len < 4`) that previously panicked the pre-auth handler are
  rejected; a 30 s pre-auth startup timeout bounds the handshake; per-session
  resource caps bound the prepared-statement registry (count and retained
  bytes), the un-flushed extended-protocol buffer, and a single inbound message.

### Performance

- **Hot-path per-query allocation elimination** — the relay previously allocated
  two fresh 16 KiB buffers per query (~1 GB/s of churn at 64 clients); buffers
  are now reused.
- **Conditional reset** — transaction-pooling throughput +48 % at 16 clients and
  +31 % at 64 clients versus always-reset (`pgbench -S`, evidence host).
- **Idle relay** — `Flush` stall reduced from ~202 ms to ~0.8 ms.

## [1.3.1] - 2026-06-28

Patch release — feature-gating fix.

### Fixed

- **`query-analytics` now compiles standalone.** `parse_limit_query` (in
  `src/admin.rs`) was gated `#[cfg(feature = "anomaly-detection")]`, but the
  `#[cfg(feature = "query-analytics")]` `/api/analytics` handler also calls it.
  Because the two features are declared independently, building with
  `--features query-analytics` alone failed to compile. The helper (and its unit
  test) are now gated `any(feature = "anomaly-detection", feature =
  "query-analytics")`, so it is available whenever either consumer is compiled —
  without coupling the two features.

## [1.3.0] - 2026-06-27

Minor release — reliability hardening across the data path, the pool, and
observability.

### Added

- **In-band failure detection.** A query that fails against a backend now
  demotes that node's health *immediately* — both on the forward path and at
  connection establishment — instead of waiting up to a full health-check
  interval. The demotion is narrowly scoped: a client-side error, or a merely
  slow-but-healthy query (a backend read-timeout, indistinguishable from a large
  sort / lock wait / bulk DML), never takes a healthy backend out of rotation;
  only genuine backend faults do. Concurrent health writers are serialized so a
  demotion can never be lost to a racing update. Live-verified: the node is
  marked unhealthy within ~1 query, far ahead of the periodic checker.
- **Protocol-level health probe.** The health check now performs a PostgreSQL
  `SSLRequest` handshake (auth-free, un-logged) instead of a bare TCP connect,
  so a wedged-but-connectable backend (stuck postmaster, out of slots) is
  detected.
- **`/api/circuit` admin endpoint** reporting live per-node circuit-breaker
  state (closed / open / half-open).
- **Idle-connection reaper + global ceiling** for the data-path backend pool:
  parked connections older than the idle timeout are reaped, and a hard global
  cap (enforced via an atomic reservation, so concurrent check-ins cannot
  overshoot) bounds total file descriptors across all `(node,user,db)`
  identities. An `idle_timeout_secs` of `0` disables the TTL reaper (PgBouncer
  convention) rather than reaping every connection each cycle.

### Changed

- **Timeout coverage** added to backend forward writes, client streaming
  writes, and the prepared-statement re-prepare exchange — a hung backend or
  wedged client can no longer pin a session indefinitely (backend reads were
  already bounded).
- **Per-session resource caps** bound `stmt_registry` (prepared statements, by
  both count *and* aggregate retained bytes), the un-flushed extended-protocol
  `pending` buffer, and a single inbound message — a misbehaving client can no
  longer grow proxy memory without limit.
- **Cancel-key map** now FIFO-evicts its oldest entries at capacity instead of
  clearing the whole map, so a busy proxy no longer loses every in-flight
  cancel registration at once.

### Notes

- New live test `scripts/regress/reliability-test.sh` (5/5 vs PostgreSQL 18.4):
  `/api/circuit` reporting, in-band demotion on backend death, fast clean-error
  (no hang), and recovery after the backend returns.
- Transparent *mid-query* failover remains out of scope for a streaming proxy
  (a partially-streamed response cannot be retried); the proxy surfaces a clean
  error and reroutes the next query. Single-primary-without-standby still
  requires an external orchestrator.

## [1.2.0] - 2026-06-26

Minor release — real LDAP authentication.

### Added

- **LDAP authentication (`ldap-auth` feature).** A new optional feature (off by
  default; implies `auth-proxy`) implements the standard search-then-bind flow:
  service-bind + search to resolve the user DN, then bind as that DN with the
  supplied password to verify it. Group memberships become identity groups;
  StartTLS / ldaps are supported. Previously `authenticate_ldap` denied by
  default. Without the feature it still denies — never a fabricated success.

### Notes

- Live-verified against a real OpenLDAP container
  (`scripts/regress/ldap-test.sh`, local-only): correct credentials
  authenticate, wrong password and unknown user are denied.
- All implemented auth methods are now genuinely real: JWT (HS256), API key,
  OAuth RFC 7662 introspection, and **LDAP**.

## [1.1.0] - 2026-06-26

Minor release — `pool-modes` now does real work on the data path.

### Added

- **Transaction / Statement connection pooling (`pool-modes`).** Previously the
  pool manager was never exercised on the query path — every client stayed 1:1
  session-pinned regardless of the configured mode. A new raw-stream
  `BackendIdlePool`, keyed by `(node, user, database)` identity, now backs the
  data path: `ensure_conn` leases a parked, identity-matched connection before
  dialing fresh; at each idle transaction boundary the connection is
  `DISCARD ALL`-reset and parked for reuse; on disconnect idle connections are
  retained (not dropped) for cross-session reuse. Session mode and feature-off
  builds are unchanged.

### Notes

- Live-verified vs PostgreSQL 18.4 (`scripts/regress/pool-modes-test.sh`):
  connection reuse fires on every post-first query, the `DISCARD ALL` reset is
  proven by a temp table vanishing between pooled statements, results are
  correct, and connections are parked on disconnect.
- Bounded scope (reported, not faded): connection reuse + a shared identity-keyed
  idle pool with correct reset are delivered. The first connection of each
  session still authenticates through the startup path, so concurrent backend
  connections are not yet reduced below the concurrent-client count; true N:M
  startup-level multiplexing needs proxy-terminated backend auth and is the
  documented next increment.

## [1.0.0] - 2026-06-26

First stable release. Every feature flag now ships genuinely-working, tested
functionality — no scaffolding, no stubs that fake success. Where a capability
is intentionally bounded, it is **documented here**, not hidden.

This release is the culmination of the 0.7.0–0.19.0 wiring series: each proxy
feature was taken from "compiles but inert" to wired onto the real data path (or,
for standalone subsystems, to genuinely functional internals), with a unit test
plus — wherever a backend was reachable — a live verification against
PostgreSQL 18.4.

### What is real and verified

- **Routing & resilience** — routing hints (`/*+ route= */`), replica lag-aware
  routing + read-your-writes, rate limiting, circuit breaker, schema-aware
  (OLAP) routing. All on the per-query path; live-verified via the proxy log.
- **Caching** — query cache (L1/L2/L3) on the path; DistribCache subsystem with
  real LZ4 (`lz4_flex`) + zstd compression and a working L3 peer server
  (replication/remote-reads over TCP).
- **Query handling** — query rewriting (rules engine on the path), query
  analytics / slow-query / N+1 detection, GraphQL-to-SQL gateway (real engine
  execution + HTTP listener).
- **Multi-tenancy** — tenant isolation + per-tenant query transformation on the
  path.
- **HA / Transaction Replay (`ha-tr`)** — failover replay, cursor restore,
  session migration, and the zero-downtime major-version upgrade orchestrator
  (real logical-replication setup, row-count parity validation, `pg_promote`
  cutover, artefact cleanup).
- **Auth (`auth-proxy`)** — JWT (HS256, constant-time verify), API keys
  (SHA-256, constant-time compare), and OAuth RFC 7662 token introspection.
- **Plugins, anomaly detection, edge proxy, observability** — as documented in
  their modules.

### Documented limitations (reported, not faded)

- **LDAP auth** is deny-by-default: `authenticate_ldap` returns an error rather
  than faking acceptance. A real bind needs an `ldap3` client and a live
  directory.
- **DistribCache** and the **INSERT batcher** are genuinely functional but are
  standalone subsystems, not yet mounted on the proxy's per-query data path
  (the `query-cache` feature is the data-path cache).
- **Pool modes** (Session/Transaction/Statement) and the **auth connection
  path** have working components; full data-path integration is staged for a
  later minor.
- The GraphQL generator keys the FROM table off the (pluralized) query field
  name and assumes an `id` primary key; nested-relationship shaping and
  mutations are follow-ons.
- `/api/pools` reports live active/idle/total per node; per-node
  pending/created/closed counters are not yet tracked.

### Engineering

- CI gate clean: `cargo clippy -D warnings` on `""`, `ha-tr`, `all-features`,
  `all-features,postgres-topology` (and `--no-default-features`). 1391
  all-features lib tests pass.

## [0.19.0] - 2026-06-26

Minor release — OAuth token introspection is now real.

### Added

- **OAuth RFC 7662 introspection (`auth-proxy`).** `authenticate_oauth`
  previously denied every request as "not implemented". It now POSTs the bearer
  token to the configured introspection endpoint (HTTP Basic client auth),
  requires an explicit `"active": true`, enforces optional issuer / audience /
  required-scope checks, and mints an `Identity` (subject + scopes→roles +
  expiry). Results are cached. A token is never trusted without the round-trip.
- `reqwest` now enables `rustls-tls`, so HTTPS identity providers work without
  OpenSSL (also benefits the L3 semantic cache).

### Notes

- Implemented + real auth methods: **JWT (HS256)**, **API key** (SHA-256 +
  constant-time compare), **OAuth introspection**. **LDAP** remains
  deny-by-default — `authenticate_ldap` returns an error rather than faking
  acceptance; a real bind needs an `ldap3` client + a live directory.

## [0.18.0] - 2026-06-26

Minor release — three orphan stubs made real.

### Changed

- **INSERT batcher executes for real (`batch.rs`).** The flush built the
  combined bulk INSERT, discarded it, and reported a fabricated success to every
  waiter. It now executes the combined statement against an attached backend
  (`with_backend`) and reports the true outcome — an honest failure (no backend
  / connect / SQL error) instead of a silent success. Live-verified: batched
  rows actually land in PostgreSQL.
- **`PreferLocal` routing is real (`load_balancer.rs`).** Was "return the first
  node". Now prefers the least-loaded node co-located with the proxy (loopback /
  localhost), falling back to least-connections overall when none is local.
- **`/api/pools` returns real pool stats (`admin.rs`).** `get_pool_stats`
  returned an empty list; `AdminState` now holds the `ConnectionPoolManager`
  (wired at startup) and reports real per-node active/idle/total connections.

### Notes

- The `InsertBatcher` is a standalone utility (not yet on the proxy's per-query
  path); these changes make its execution real and honest. Per-node
  pending/created/closed pool counters aren't tracked separately, so those
  `/api/pools` fields are 0 while active/idle/total are live.

## [0.17.0] - 2026-06-26

Minor release — fourth of the 1.0.0 wiring set: the upgrade orchestrator's
validation stage is now real.

### Changed

- **Upgrade-orchestrator stage 3 validation (`ha-tr`).** The validation stage
  advertised a source≡target sample comparison but only slept and advanced. It
  now lists a deterministic, bounded sample of the source's user tables and
  compares `count(*)` on each between source and target over the backend
  client; a mismatch fails the job, and the rows checked are recorded in
  `validated_rows`. Stages 1/2/4/6 were already real. The stale module
  doc-comment (which called every stage a stub) is corrected.

### Notes

- Row-count parity is the portable PG 14–18 check; a per-row checksum on top is
  a follow-on. The validation SQL path is live-verified via self-compare
  (`HELIOS_LIVE_PG`-gated test); a full two-major-version replication run needs
  a two-node harness.

## [0.16.0] - 2026-06-26

Minor release — third of the 1.0.0 wiring set: the DistribCache internals are
now real.

### Added

- **L3 distributed-cache peer server (`distribcache`).** `DistributedCache`
  now serves the GET/PUT/PING/INVALIDATE wire protocol it already spoke as a
  client (`serve` / `serve_on`). Replication PUTs and remote GETs from peers
  previously reached nothing — the mesh can now actually replicate and serve
  cross-node reads.

### Changed

- **L2 LZ4 compression is real.** The `Lz4` tier compressor (the default) was a
  no-op that copied bytes behind a marker; it now uses `lz4_flex` block
  compression. Round-trip + measurable shrink are covered by tests.

### Notes

- These make the DistribCache subsystem genuinely functional. It is not yet
  mounted on the proxy's per-query data path — the `query-cache` feature is the
  data-path cache; wiring DistribCache in as an optional proxy backend is a
  follow-on.

## [0.15.0] - 2026-06-26

Minor release — second of the 1.0.0 wiring set: the GraphQL gateway is now real.

### Added

- **GraphQL-to-SQL gateway (`graphql-gateway`).** `[graphql_gateway]` config
  (off by default; `listen_address`, `backend_*`, `tables`). A new HTTP listener
  accepts `POST {"query": "<graphql>"}`, generates SQL from the configured
  schema, executes it over the backend PG-wire client, and returns a real
  GraphQL response. The engine previously returned `null`/empty for every field
  and had no listener at all.

### Notes

- Real SQL execution + flat top-level response shaping is delivered and
  live-verified. Nested-relationship shaping (joins), mutations, and the SQL
  generator's table-name derivation + `id` primary-key assumption are follow-ons.

## [0.14.0] - 2026-06-26

Minor release — first of the 1.0.0 wiring set: schema/workload-aware routing.

### Added

- **Schema/workload-aware routing (`schema-routing`).** `[schema_routing]`
  config (off by default): a read classified as analytical (OLAP — aggregations,
  GROUP BY, window functions) is routed to the configured `analytics_node`,
  offloading analytics from the primary. Other queries use default routing.
  Decisions log to the `helios::schema` target.

### Notes

- This wires the real (query-shape) workload classification. Table-level schema
  discovery (data-temperature routing from `pg_catalog`/`information_schema`
  introspection) needs a configured introspection credential and is a follow-on.

## [0.13.0] - 2026-06-26

Minor release — platform-tier wiring wave 6: Transaction Replay journaling.

### Added

- **Transaction Replay journaling (`ha-tr`).** With `tr_enabled`, write
  statements are now journaled on the query path (they previously weren't, so
  the replay engine had nothing to replay). The existing replay machinery —
  `POST /api/replay` and the failover coordinator — can now re-apply journaled
  writes onto a promoted primary or a staging target.

### Notes

- The journal-population half and the replay **mechanism** are live-verified
  (writes journaled → table emptied → `/api/replay` re-applies them → rows
  reappear). The failover-**triggered** auto-replay is coordinated by the
  failover controller but is not live-verified here (no real standby in the dev
  env). Follow-ons: extended-protocol write journaling, explicit-transaction
  grouping, journal retention, array bind params in replay, cursor reposition.

## [0.12.0] - 2026-06-26

Minor release — platform-tier wiring wave 5: multi-tenancy.

### Added

- **Multi-tenancy row isolation (`multi-tenancy`).** `[multi_tenancy]` config
  (off by default): `identify_by` (a startup parameter such as
  `application_name`/`user`, or `database`) selects the tenant; queries against
  the configured `tenant_tables` get a `WHERE <tenant_column> = '<tenant>'`
  filter injected so each tenant sees only its own rows. The filter is applied
  before the cache lookup, so cached results are tenant-scoped (no cross-tenant
  leakage). Injection logs to the `helios::tenant` target.

## [0.11.0] - 2026-06-26

**Security release** — the auth subsystem (`auth-proxy`) no longer accepts
forged or arbitrary credentials.

### Security

- **JWT signatures are now actually verified.** `verify_signature` previously
  returned success for *every* token. It now performs a constant-time HS256
  (HMAC-SHA256) verification against a symmetric key, and **rejects any other
  algorithm** (RS256/ES256 etc.) rather than trusting it — so a forged or
  tampered JWT no longer validates.
- **OAuth / LDAP / HTTP-Basic deny by default.** These previously synthesized an
  accepted identity for any input (any password, any bearer token). They now
  deny; real OIDC introspection / LDAP bind / user-store verification are
  follow-ons requiring external infrastructure.
- **API keys** are hashed with SHA-256 and compared in constant time (was the
  non-cryptographic `DefaultHasher` with a `==` compare).

### Breaking

- `auth::jwt::Jwk` gains a `k` (symmetric key) field, and JWT validation now
  rejects algorithms it cannot verify.

### Notes

- This closes the client-auth **crypto** holes (the deep-audit's #1 finding),
  verified by unit tests. Wiring proxy-terminated client auth into the
  connection path (a `[auth] mode` that verifies a client-presented JWT on
  connect), RS256/ES256, and real OAuth/LDAP remain follow-ons. That
  connection-path work is also the prerequisite for `pool-modes`.

## [0.10.0] - 2026-06-26

Minor release — platform-tier wiring wave 4: SQL query rewriting.

### Added

- **SQL query rewriting (`query-rewriting`).** `[query_rewrite]` config (off by
  default) with a list of rules — each `{ match_table | match_regex }` selects
  which queries it applies to and `{ replace_table_with | append_where |
  add_limit }` is the transformation. Matching simple queries are rewritten on
  the path before forwarding; the cache and backend both see the rewritten SQL.
  Rewrites log to the `helios::rewrite` target.

### Notes

- Rewriting covers the simple-query path and the real string-based
  transformations. The extended-protocol (Parse) path and the
  AST-reserialization transform are follow-ons.
- `pool-modes` has been resequenced to **after** `auth-proxy`: real connection
  multiplexing requires proxy-held backend credentials (which `auth-proxy`
  provides — passthrough auth cannot share a backend connection across clients)
  plus a raw-connection transaction-pool on the data path.

## [0.9.0] - 2026-06-26

Minor release — platform-tier wiring wave 3: query-result caching.

### Added

- **Query-result cache (`query-cache`).** `[cache]` config (off by default;
  `ttl_secs`, `max_result_bytes`). A cacheable read (a plain, deterministic,
  non-transactional `SELECT` — not `WITH`/`FOR UPDATE`/volatile) is served from
  an in-memory L1 hot (per-connection) + L2 warm (shared, normalized) cache with
  no backend round-trip; on a miss the response is captured while streaming and
  stored. A write invalidates cached reads referencing its tables. Cache hits
  log to the `helios::cache` target.

### Notes

- L1 hot + L2 warm tiers are live-verified (a read served the stale cached value
  while the backend held a newer one; a write invalidated it). The L3 semantic
  (vector-similarity) tier remains opt-in via the `/*helios:cache=semantic*/`
  hint and is not yet live-verified — it needs an embedding source, tracked as a
  follow-on.

## [0.8.0] - 2026-06-26

Minor release — platform-tier wiring wave 2: observability + read-scaling. Two
more feature flags that previously did nothing on the query path are now active.

### Added

- **Query analytics (`query-analytics`) records every forwarded query.**
  `[analytics]` config (off by default). Each query is fingerprinted +
  normalized (literal-only differences collapse to one `select ?` fingerprint),
  accumulating per-fingerprint call/latency/p99/error stats with a slow-query
  log. New `GET /api/analytics` returns the top queries + slow-query count. The
  real query normalizer now runs on the live path (it was previously a
  stand-in).
- **Read-your-writes (`lag-routing`).** `[lag_routing]` config (off by default).
  Within `ryw_window_ms` after a write, a session's reads are pinned to the
  primary so the client always observes its own writes despite replica lag. A
  lag-aware read-exclusion filter (`max_lag_bytes`) drops standbys lagging
  beyond a threshold.

### Notes

- Read-your-writes is live-verified. The lag-**exclusion** decision logic is
  unit-tested and in place (safe-by-default, `max_lag_bytes=0`), but populating
  per-node replication lag requires a background monitor with configured backend
  credentials + a real standby — tracked as a follow-on.

## [0.7.0] - 2026-06-26

Minor release — the first wave of **platform-tier wiring**. Three feature flags
that previously compiled but did nothing on the query path are now genuinely
enforced, each live-verified against PostgreSQL 18.4. This release also formally
owns two prior unreleased source changes (an MSRV bump and one breaking
public-API rename).

### Added

- **SQL-comment routing hints (`routing-hints`) are now honored.**
  `[routing_hints]` config (off by default). `/*helios:route=primary|standby|…*/`,
  `/*helios:node=<name>*/`, and `consistency=strong` steer routing on both the
  simple- and extended-query paths, taking precedence over default verb routing
  (but never over a plugin `Block`). A `node=<name>` hint resolves the node name
  to its address; the hint comment is optionally stripped before forwarding.
- **Rate limiting (`rate-limiting`) is now enforced.** `[rate_limit]` config
  (off by default; token bucket + concurrency, keyed per user / client-ip /
  database / global). Over-limit queries receive a clean `ErrorResponse`
  (SQLSTATE 53400); throttle/queue verdicts apply real backpressure; the
  connection is not dropped.
- **Per-node circuit breaker (`circuit-breaker`) is now active.**
  `[circuit_breaker]` config (off by default). Repeated backend failures trip a
  node's circuit; while open the proxy fast-fails with `ErrorResponse` SQLSTATE
  08006 instead of retrying the dead node, and read selection routes around it.
  A half-open probe closes the circuit on recovery.

### Fixed

- **Write detection is no longer fooled by a leading hint comment.**
  `is_write_query` is evaluated on the hint-stripped SQL, so
  `/*helios:…*/ INSERT …` is correctly classified as a write. The `node=<name>`
  route path (hint and plugin) now resolves a node name to its address instead
  of dialing a literal hostname.

### Changed

- **MSRV is now Rust 1.86** (was 1.75), to match the transitive dependency floor.

### Breaking

- `rewriter::RewriteRule::new` (the builder entry point, under the
  `query-rewriting` feature) was renamed to `RewriteRule::build`. Callers using
  `RewriteRule::new(id)` must switch to `RewriteRule::build(id)`.

## [0.6.1] - 2026-06-15

Patch release — demo infrastructure fixes. No proxy code or runtime behavior changes.

### Fixed

- **All 5 legacy demos now work end-to-end with the published Docker image.**
  They previously assumed a local `build: context` that no longer exists.
  - Replaced `build: context: ../../` with `image: ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.6.1`
  - Fixed `pg_isready` healthcheck to probe TCP (`-h 127.0.0.1`) so standbys
    don't start `pg_basebackup` before the primary TCP listener is ready.
  - Added `init-hba.sh` to each demo's `initdb.d` to allow Docker bridge-network
    replication connections (pg_hba `host replication all all trust`).
  - Added `user: postgres` to standby containers (postgres refuses root execution).
  - Added `chmod 750` after `pg_basebackup` to satisfy PostgreSQL permissions check.
  - Removed `synchronous_standby_names` from primary command blocks — it blocks
    `CREATE DATABASE` indefinitely when no standbys exist at init time.
  - Updated `demo.sh` and `run-compare.sh` to drop `--build`; updated READMEs.
  - Rewrote `proxy.toml` in bank-ledger and vs-pgbouncer from stale PgBouncer
    format to current HeliosProxy format.

## [0.6.0] - 2026-06-15

Maintenance release — operational/packaging tooling and docs. No proxy code or
runtime behavior changes.

### Fixed

- **Docker image publishing now works.** The `docker.yml` workflow had three
  bugs that failed every build: a non-existent action name, an uppercase image
  name ghcr rejects, and the wrong Dockerfile path. It now builds and pushes
  `ghcr.io/heliosdatabase/hdb-heliosdb-proxy` (linux/amd64).

### Changed

- Demo / skill / IaC examples reference the current `:0.6.0` image (the prior
  `:0.4.x` tags were never built).
- Build linux/amd64 only (dropped the slow/flaky emulated linux/arm64 leg).
- README uses stable "Connection-Routing Tier" / "Platform Tier" wording instead
  of version-pinned module counts.
- Internal working docs consolidated under `docs/internal/` (excluded from the
  published crate).

## [0.5.1] - 2026-06-14

Docs/branding patch. No code or behavior changes.

### Changed

- Rebrand the project org from `Dimensigon` to `HeliosDatabase` across the
  README, embedded operator skills, demos, operator/Terraform guides, and the
  LICENSE copyright line. Registry/k8s/npm references use lowercase
  `heliosdatabase` where required (`ghcr.io` image refs, the k8s API group, the
  npm scope, `*.github.io` / `charts.*` hosts, the Terraform provider source);
  `github.com` URLs keep the `HeliosDatabase` display case.

### Added

- `AGENTS.md` — contributor guide (build/test commands, style, testing, and
  commit conventions).
- A README "Ecosystem" section linking the HeliosDatabase repositories.

## [0.5.0] - 2026-06-14

Minor release: the 2026-06 deep-audit batches (A–H + G2). Table-stakes
security/protocol features, an AI/agent data-plane, a continuous PostgreSQL→
HeliosDB-Nano migration mirror, zero-downtime operations, and hot-path
performance work — all live-verified against PostgreSQL 18.4 and HeliosDB-Nano.

### Added

- **Client-facing TLS termination + mTLS** — `[tls]` config; rustls server
  handshake, optional client-certificate verification.
- **Proxy-terminated SCRAM-SHA-256** — `[auth] mode = "scram"` with a pgbouncer-
  style auth file; the proxy authenticates clients itself.
- **pg_hba-style admission rules** — `[[hba]]` allow/reject by user / database /
  IP-CIDR, evaluated before any backend connection.
- **Query cancellation forwarding** — `CancelRequest` routed to the backend that
  owns the session's `BackendKeyData`.
- **Prepared statements survive backend switches** — named statements are
  transparently re-prepared on a new connection after a failover / redial.
- **Native MCP agent gateway** — JSON-RPC 2.0 over HTTP (`query` / `list_tables`
  / `explain`), backend-agnostic, read-only by default.
- **Per-agent scoped grants + SQL contract validator** — verb/table/predicate/
  row-limit policy with machine-readable repair hints.
- **Neon-serverless-compatible HTTP SQL gateway** — `POST /sql`.
- **Continuous traffic mirroring** and a **PostgreSQL→Nano migration mirror**:
  snapshot bootstrap (COPY-based bulk load with INSERT fallback + a non-empty-
  target idempotency fence), `migration_ready` status, and transparent cutover
  with rollback.
- **Instant branch databases** — `CREATE DATABASE … TEMPLATE` provisioning via
  `POST/GET/DELETE /api/branch`.
- **Admin API Bearer-token authentication** — closes the unauthenticated-admin
  release blocker.
- **Zero-downtime operations** — `SIGHUP` config reload (nodes / pools / limits /
  hba without dropping connections) and a `SIGUSR2` binary handoff over
  SO_REUSEPORT with graceful drain (client + admin listeners).
- **Plugin registry + `helios-plugin` CLI** — `install` (from a local/`file://`/
  `http://` index, SHA-256 + Ed25519 verified), `list`, `new`, `verify`.

### Changed

- **Extended-protocol streaming relay** — large result sets stream frame-by-frame
  with bounded proxy memory (≈100 MB result → flat RSS).
- **Per-session multi-node backend connection cache** — reuses authenticated
  connections across read/write routing switches.
- **WASM plugin runtime** — `InstancePre` reuse, epoch-based timeout enforcement,
  sharded metrics.
- **Lock-free hot path** — node-health map and live config behind `ArcSwap`;
  parallel health sweep.
- **Unnamed-`Parse` promotion** — an identical re-`Parse` is not re-forwarded to a
  backend that already holds that unnamed statement (fewer backend round trips).

### Fixed

- Wire-protocol tag mapping: `'R'` → `AuthRequest`, and server→client tag-collision
  remapping in the backend client (fixes management/replay queries against real
  backends).

## [0.4.2] - 2026-05-01

Patch release: ship the operator skill bundle inside the binary so
`cargo install heliosdb-proxy` users (no repo clone) can deploy
the bundle to Claude Code / Codex with one subcommand.

### Added

- **`heliosdb-proxy install skills`** — new subcommand that deploys
  the embedded `.claude/skills/` bundle (22 skills + index + template)
  into `~/.claude/skills` and `~/.codex/skills`.
- Flags: `--target claude|codex|both` (default `both`),
  `--symlink` (use a symlink into `~/.local/share/heliosdb-proxy/skills/`
  so re-running after a binary upgrade refreshes the bundle in place),
  `--force` (overwrite pre-existing entries), `--dry-run` (show
  planned actions without writing).
- New module `src/skills.rs` with `EMBEDDED_SKILLS` (a `Dir<'_>`
  populated by `include_dir!`), `install_skills()`, and a
  test-friendly `install_skills_at(home, …)` overload.
- Dependency: `include_dir = "0.7"` (~80 KiB binary growth).

### Changed

- `src/main.rs` now uses clap subcommands. The daemon path
  (no subcommand + flags) is unchanged for backward compatibility
  with v0.4.1 invocations.
- Package size up from 188 files / 3.0 MiB to 214 files / 3.2 MiB
  (compressed: 620 KiB → 673 KiB) — entirely from the embedded
  skill bundle.

## [0.4.1] - 2026-05-01

Patch release: internal cleanup + first tag-driven crates.io publish.
No API surface changes; default-feature lib lint count drops from 41
to 0. All 1 502 tests still green.

### Build & release

- Tag-driven `cargo publish` workflow (`.github/workflows/crates-io.yml`).
  Pushing a `vX.Y.Z` tag matching `Cargo.toml`'s version uploads the
  crate. `workflow_dispatch` retries against an existing tag.
- Cargo metadata for crates.io: `rust-version = "1.75"`, `homepage`,
  `documentation`, `exclude` (drops demos/docs/operator/terraform/docker
  from the `.crate` — 320 → 188 files), and
  `[package.metadata.docs.rs] all-features = true` so docs.rs renders
  the full feature surface.

### Internal — dead-code retirement

- Removed legacy connection-pool path in `server.rs` that
  `ConnectionPoolManager` superseded: `state.pools` field, `NodePool`
  and `BackendConnection` structs, `get_connection`, `cleanup_pools`,
  and `process_statement_for_pool_mode` methods. ~200 lines deleted.
- Removed unused fields across `pool/manager.rs`,
  `connection_pool.rs`, `health_checker.rs`, `failover_controller.rs`,
  `switchover_buffer.rs`, `pipeline.rs`, `batch.rs`, `load_balancer.rs`,
  `admin.rs` (and the now-empty `with_flush_channel` constructor on
  `InsertBatcher`).
- Cfg-gated imports / locals that only live under `wasm-plugins`,
  `ha-tr`, or `#[cfg(test)]` (chrono `DateTime`/`Utc` in admin.rs,
  `NodeRole` in failover_controller.rs, `forward_start` in server.rs).
- `MessageType::from_tag` had four unreachable server-side arms
  (`DataRow`/`ErrorResponse`/`CommandComplete`/`ParameterStatus`)
  that collide with client-side `Describe`/`Execute`/`Close`/`Sync`
  byte tags. Removed and documented that the function is
  direction-agnostic and resolves collisions to the client-side variant.
- Collapsed the `let mut backend_stream = None; ... match { ... =
  Some(...) }` pattern in `handle_client` to a single
  `let (mut backend_stream, mut backend_node) = match { ... }`.

### Licensing

- License changed from AGPL-3.0-only to Apache-2.0. (Already shipped
  in the 0.4.0 crates.io upload — recorded here for the changelog
  trail.)

## [0.4.0] - 2026-04-25

Major feature release: full delivery of the T1 / T2 / T3 roadmap shipped
in v0.3.1's audit, plus a critical correctness fix in the WASM plugin
hook serialization. 31 PRs across five repositories; 1 296 lib tests +
5 end-to-end WASM tests + 57 plugin tests + 17 operator tests, all green.

### ⚠ Critical fix
- **`QueryContext` serializer was dropping `hook_context`.** Every WASM
  plugin shipping in v0.3.1 received a context without the per-request
  attributes the proxy carefully populated (`tenant_id`, `agent_id`,
  `application_name`, `helios.region`, etc). All five attribute-driven
  plugins (cost-governor, token-budget, llm-guardrail, ai-classifier,
  residency-router) silently no-op'd their gates as a result. Fixed by
  adding `hook_context` to the custom `Serialize` impl and deriving
  `Serialize` on `HookContext`. Covered by the new
  `tests/wasm_plugin_e2e.rs` end-to-end tests that load real .wasm
  plugins through the production runtime.

### Added — admin REST API
- `GET /topology` — `{ currentPrimary, healthyNodes, unhealthyNodes,
  totalNodes, lastFailoverAt }`. Joins config (role) with node_health
  (healthy) so external controllers populate cluster topology in one
  poll. Field names match `HeliosProxyStatus` directly (camelCase).
- `POST /api/replay` + `GET /api/replay` — time-travel replay of a
  journal window against a target backend. Per-call credential
  overrides via `target_user` / `target_password` / `target_database`.
  503 when `ha-tr` feature off.
- `POST /api/shadow` — runs a query against source AND shadow backends,
  diffs results, returns `{ row_count_match, row_hash_match, is_clean,
  primary_elapsed_us, shadow_elapsed_us, primary_error, shadow_error }`.
  Order-independent row-set hash so non-deterministic orderings tolerate.
- `POST /api/chaos` — controlled fault injection: `force_unhealthy`,
  `restore`, `reset` actions. `GET /api/chaos` lists active overrides.
- `GET /plugins` — loaded WASM plugin list with name, version, hooks,
  state, invocation + error counts.
- `GET /anomalies?limit=N` — recent anomaly events (rate spikes,
  credential bursts, SQLi patterns, novel queries).
- `GET /api/edge` + `POST /api/edge/register` + `POST /api/edge/invalidate`
  — edge-mode cache stats, edge-node registration, manual invalidation
  broadcast.

### Added — admin Web UI (`/` + `/ui`)
Single embedded HTML file. Auto-refresh every 5 s. Panels:
Nodes · Topology · Plugins · Anomalies · Edge Mode · Chaos Mode ·
Shadow Execution · Time-Travel Replay · SQL Console · Traffic · Cluster.

### Added — WASM plugin ecosystem
- **Plugin host KV bridge.** `env.kv_get` / `env.kv_set` /
  `env.kv_delete` wasmtime imports. Per-plugin namespaced
  `Arc<RwLock<HashMap<plugin, HashMap<key, value>>>>`. Plugins can
  persist state across hook invocations.
- **`env.sha256_hex` host import.** Real SHA-256 via the audited
  `sha2` crate; plugins no longer embed their own (now ~25 KiB
  smaller). Replaces the FNV placeholder `audit-chain` shipped with
  in v0.3.1.
- **`PluginLoader.SignatureVerifier`.** Optional Ed25519 trust root
  (directory of `*.pub` files, base64 raw 32-byte keys). Every
  loaded `.wasm` requires a sidecar `.sig`. Wire-compatible with
  `openssl pkeyutl -sign` and `signify`.
- **OCI-style artefact loader.** `PluginLoader.load` now accepts
  `.tar.gz` artefacts produced by the new `helios-plugin pack` CLI:
  `manifest.json` + `plugin.wasm` + optional `plugin.sig`. Validates
  wasm SHA-256 against the manifest, verifies signature against trust
  root if attached.
- **`RouteResult::Block { reason }` ABI variant.** Plugin-side route
  rejection synthesises a PG `ErrorResponse` + `ReadyForQuery` —
  same wire shape as `PreQueryResult::Block`.
- **`trust_root` config knob** — plugins.trust_root in TOML wires the
  signature verifier automatically.

### Added — anomaly detection (T3.1)
- **`anomaly-detection` feature.** In-process sliding-window
  detector. Four families:
  1. **Rate spike** — z-score on per-tenant queries-per-second vs
     rolling EWMA baseline (default 60 s window, 3σ threshold).
  2. **Credential stuffing** — failed-auth bursts per (user, ip);
     Warning at 5, Critical at 10. Successful auth resets.
  3. **SQL injection** — six pattern classes (classic OR, UNION
     SELECT, comment escape, stacked queries, time-based blind,
     information_schema probe).
  4. **Novel query** — first-seen fingerprint, informational.
- Wired into the query path via `record_anomaly_observation`. Tenant
  identity from session vars / fallback to client IP.

### Added — edge / geo proxy mode (T3.2)
- **`edge-proxy` feature.** Cache-first edge mode with last-write-wins
  TTL coherence. `EdgeRole::Edge` terminates reads against a local
  LRU+TTL+version cache; `EdgeRole::Home` is authoritative and
  broadcasts invalidations. Pull-on-miss + invalidation push,
  no consensus, no central registrar.
- `EdgeCache` (LRU + TTL + monotonic version + per-table tags).
- `EdgeRegistry` (home-side fanout, bounded mpsc per edge,
  back-pressure on slow edges, prune-stale).
- Explicit "eventual consistency with bounded staleness via TTL"
  contract.

### Added — chaos engineering (T3.3)
- **`POST /api/chaos`.** `force_unhealthy` / `restore` / `reset`
  actions. Triggers the failover path the same way a real probe
  failure would.

### Added — query shadow execution (T3.4)
- **`POST /api/shadow`.** Built on the existing `src/shadow_execute/`
  module. Order-independent row-set hash for non-deterministic
  orderings. SLO: HTTP 500 only on source-connect failures; shadow
  failures land in the report.

### Added — Kubernetes operator (`HDB-HeliosDB-Proxy-Operator`)
- Reconciler renders ConfigMap + Deployment + Service from
  HeliosProxy CR. Owned objects use `SetControllerReference` so
  `kubectl delete` reaps the stack.
- `RefMissing` condition surfaces unresolved PoolProfile /
  RoutingRule / AuditPolicy / TenantQuota refs.
- Status polling: reconciler hits the proxy's `/topology` and
  populates `currentPrimary` / `healthyNodes` / `unhealthyNodes` /
  `lastFailover`.
- 17 Go unit tests (render helpers + condition merge + config hash).

### Added — Terraform + Pulumi providers
- `terraform-provider-HDB-HeliosDB-Proxy` — five resources
  (`heliosproxy_instance`, `_pool_profile`, `_routing_rule`,
  `_audit_policy`, `_tenant_quota`). Schema mirrors operator CRDs
  via local replace.
- `pulumi-HDB-HeliosDB-Proxy` — terraform-bridge wrapper, Node.js /
  Python / Go / .NET SDK metadata. Both providers wrap the same
  operator CRDs.

### Added — first-party plugins (`HDB-HeliosDB-Proxy-Plugins`)
All seven plugins on the new shared `helios-plugin-abi` crate +
host KV bridge:
- `cost-governor` — per-tenant query cost budgets (minute / hour /
  day windows). Real `kv_get` / `kv_set` for usage tracking.
- `ai-classifier` — detects LLM-generated SQL via
  `application_name` keywords, generated-by markers, opt-in.
  Best-effort `agent_id` + `model_id` extraction.
- `token-budget` — per-(agent, model) cost gating.
- `llm-guardrail` — blocks DROP/TRUNCATE in AI traffic, DELETE/UPDATE
  without WHERE, SELECT without LIMIT against large tables, missing
  tenant_id filter.
- `pgvector-router` — detects pgvector top-K (`<->`, `<#>`, `<=>`),
  routes to a topology-tagged vector replica via
  `RouteResult::Node`.
- `column-mask` — rewrites bare column refs to
  `mask_<fn>(<col>) AS <col>` based on user roles.
- `audit-chain` — hash-chained tamper-evident log. **Now uses real
  SHA-256** via the `env.sha256_hex` host import (was an FNV
  placeholder in v0.3.1).
- `residency-router` — routes by `helios.region` attribute; uses
  `RouteResult::Block` for cross-region rejections (was a sentinel
  hack in v0.3.1).

### Added — `helios-plugin` CLI
Pack / inspect / verify WASM plugin artefacts as portable `.tar.gz`:

```sh
helios-plugin pack    --wasm <path> --name X --version 1.0 \
                      --hooks pre_query,post_query [--sig <path>] \
                      --out <path>
helios-plugin inspect <artefact.tar.gz>
helios-plugin verify  <artefact.tar.gz> --trust-root <dir>
```

Uses the same Ed25519 + base64 trust-root format as the proxy's
loader so a single key directory works for both artefact
verification AND in-proxy signature checking.

### Changed
- New feature flags: `anomaly-detection`, `edge-proxy`. Both added
  to the `all-features` bundle.
- `RouteResult` deserialiser accepts the new `block` action variant.
- `PluginLoader.allowed_extensions` now accepts `gz` (for
  `.tar.gz` artefacts) in addition to `wasm`.

### Tests
- 1 296 lib tests pass with `--features all-features` (+58
  regression tests covering the additions above).
- 5 new end-to-end WASM tests load real plugin .wasm artefacts
  through the production runtime.
- All 7 plugins compile to wasm32 (~120-150 KiB each).
- All three feature configurations build clean: default,
  `--no-default-features`, `--features all-features`.

## [0.3.1] - 2026-04-22

### Fixed
- **Connection pool: `max_connections` now enforced while connections are in use.**
  The semaphore permit acquired during `get_connection` was previously dropped
  before the connection reached the caller, so the limit only gated concurrent
  calls to `get_connection` itself. The permit is now attached to the
  `PooledConnection` and released when the connection is dropped or closed,
  so `max_connections` bounds total (idle + in-use) connections as documented.
  Covered by `test_max_connections_enforced_while_in_use`.
- **Protocol parser: unterminated C-strings now return a protocol error.**
  The previous `read_cstring` loop silently consumed the entire remaining
  buffer on missing null terminators. It now returns
  `ProxyError::Protocol(...)` and leaves the buffer untouched. Covered by
  `test_read_cstring_unterminated`.

### Performance
- **Connection-pool checkout no longer serialises across nodes.**
  `ConnectionPool::get_connection` previously held `pools.write().await`
  across `semaphore.acquire_owned().await` and `create_connection().await`,
  serialising all checkouts through a single lock. The lock is now taken
  only to pop an idle connection and clone the per-node `Arc<Semaphore>`;
  both awaits happen after the lock is released.
- **Pool metrics are now lock-free.** Seven `self.metrics.write().await`
  call sites replaced with `fetch_add(1, Relaxed)` on a
  `PoolMetricsCounters` struct of `AtomicU64` fields. `pool.metrics()`
  snapshots the counters on demand; no lock, no `.await`.
- **Zero-copy protocol parsing.**
  - `read_cstring` scans for the null terminator in a single pass
    (`iter().position(|&b| b == 0)`) and hands the split-off `BytesMut`
    directly to `String` (zero-copy conversion when uniquely owned)
    instead of growing a `Vec<u8>` byte-by-byte via `get_u8()`.
  - `BindMessage::param_values` is now `Vec<Option<Bytes>>`; parse uses
    `split_to(len).freeze()` instead of `.to_vec()`, so parameter values
    are reference-counted slices into the original buffer rather than
    per-parameter heap allocations.
- **L1 hot cache: hits take only a read lock.**
  `L1Entry::access_count` is now an `AtomicU64`, so `touch()` takes `&self`
  and can run under a read lock on the entries map. The cache itself
  switches from `std::sync::RwLock` to `parking_lot::RwLock` (no
  poisoning, faster uncontended). Only expired entries escalate to a
  write lock for eviction. Covered by `test_concurrent_hits_read_lock_only`
  (16 threads × 500 iters on the same key).

### Changed
- `L1Entry` no longer derives `Clone` (atomics aren't trivially `Clone`).
  No external caller was cloning it; only the contained `CachedResult` is
  cloned on return from `get()`. `access_count()` accessor added for
  read-only consumers.

### Tests
- 1 102 tests pass with `--features all-features` (+6 regression tests
  covering the fixes above).

### Benchmarks
Criterion `--quick` on the `pooling` bench suite, comparing baseline
(v0.3.0, commit `8c43de9`) against v0.3.1 on the same machine:

| Benchmark                                  | v0.3.0     | v0.3.1     | Change       |
|--------------------------------------------|------------|------------|--------------|
| `pool/acquire_release/single`              | 471.81 ns  | 329.83 ns  | **−30 %**    |
| `pool/throughput/sequential_acquire/1`     | 490.36 ns  | 353.88 ns  | **−28 %**    |
| `pool/throughput/sequential_acquire/10`    | 5.354 µs   | 3.351 µs   | **−37 %**    |
| `pool/throughput/sequential_acquire/50`    | 25.36 µs   | 16.95 µs   | **−33 %**    |
| `pool/metrics/read_metrics`                | 35.90 ns   | 11.51 ns   | **−68 %** (3.1× faster) |

Single-threaded numbers — the concurrency gains from dropping
`RwLock<pools>` across `.await` and from lock-free metric reads
compound under contention and are not captured by this bench.

## [0.3.0] - 2026-03-26

### Added
- **24 feature modules** migrated to standalone, feature-gated architecture:
  - `pool-modes` - Session, Transaction, and Statement connection pooling
  - `ha-tr` - Transaction Replay with failover replay, cursor restore, session migration
  - `query-cache` - Multi-tier caching (L1 hot / L2 warm / L3 semantic)
  - `routing-hints` - SQL comment-based query routing (`/*helios:route=primary*/`)
  - `lag-routing` - Replica lag-aware routing with read-your-writes consistency
  - `rate-limiting` - Token bucket, sliding window, and concurrency limiters
  - `circuit-breaker` - Adaptive circuit breaker with sliding counter
  - `query-analytics` - Slow query log, N+1 detection, query fingerprinting
  - `multi-tenancy` - Tenant isolation with per-tenant connection pools
  - `auth-proxy` - JWT, OAuth, and API key authentication
  - `query-rewriting` - SQL rewriting rules engine
  - `wasm-plugins` - Hot-reload WASM plugin system (sandboxed)
  - `graphql-gateway` - GraphQL-to-SQL translation with introspection
  - `schema-routing` - Data temperature and workload classification routing
  - `distribcache` - Distributed intelligent caching with AI-driven prefetch
  - `observability` - Prometheus metrics and OpenTelemetry tracing
- **TopologyProvider trait** with two implementations:
  - `postgres-topology` - PostgreSQL discovery via `pg_is_in_recovery()` polling
  - `heliosdb-topology` - HeliosDB-Lite internal topology
- **`all-features` bundle** for enabling all proxy features at once
- PostgreSQL wire protocol forwarding with Transparent Write Routing (TWR)
- Request pipelining support (Parse/Bind/Execute pipeline, FIFO ordering)
- Batch INSERT coalescing for bulk write optimization
- Switchover buffer for zero-downtime planned failover
- Primary tracker with standalone and integrated modes
- Admin REST API with HTTP SQL endpoint for load-balanced queries
- Criterion benchmarks for pooling and routing performance
- Integration test skeleton for end-to-end verification
- CI workflow with feature matrix (default, ha-tr, all-features, all-features+postgres-topology)
- MSRV verification at Rust 1.75
- Release workflow for cross-platform binary builds (linux/macOS, x86_64/aarch64)
- Docker workflow with multi-arch images (amd64/arm64) pushed to GHCR

### Changed
- Restructured from embedded library to standalone binary + library crate
- All feature modules are now independently toggleable via Cargo feature flags
- Connection pool modes (`session`, `transaction`, `statement`) moved behind `pool-modes` feature

## [0.2.0] - 2026-02-15

### Added
- Connection pooling with Session, Transaction, and Statement modes
- Pool hardening: transaction leak detection, stale lease cleanup, exhaustion monitoring
- Prepared statement tracking across pool mode transitions
- Connection reset executor for clean state between leases
- Load balancer with round-robin, least-connections, and latency-based strategies
- Health checker with configurable check queries and failure thresholds
- Failover controller with sync-standby preference and automatic promotion
- Transaction journal for write tracking and replay

### Changed
- Upgraded to tokio 1.x full runtime
- Improved protocol codec for better PostgreSQL compatibility

## [0.1.0] - 2026-01-27

### Added
- Initial release of HeliosProxy as a standalone connection router
- PostgreSQL wire protocol support (startup, simple query, extended query)
- Basic connection pooling with configurable min/max connections
- Read/write splitting with automatic query classification
- Node health monitoring with configurable intervals and thresholds
- Admin API for runtime management and metrics
- Configuration via TOML file or command-line arguments
- Benchmark suite: HeliosProxy vs PgBouncer scalability comparison
- Docker support for containerized deployment

[0.3.1]: https://github.com/HeliosDatabase/HeliosDB-Proxy/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/HeliosDatabase/HeliosDB-Proxy/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/HeliosDatabase/HeliosDB-Proxy/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/HeliosDatabase/HeliosDB-Proxy/releases/tag/v0.1.0
