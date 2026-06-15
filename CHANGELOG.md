# Changelog

All notable changes to HeliosProxy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
