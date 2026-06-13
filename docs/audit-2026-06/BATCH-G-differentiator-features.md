# BATCH G — Differentiator features

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** Features no competing PG proxy ships; each compounds existing HeliosProxy modules into a moat. Judge-ranked.

**Parallel-execution compatibility:** PARALLEL-friendly: one agent per item.

**Files touched:** new modules; minimal hot-path coupling

**Conflicts with:** Independent of A–E. G items are mutually independent.

**Acceptance criteria:**

- Each feature ships with a demo, docs, integration tests; admin REST surfaces follow the v2 conventions.

---

### [90] MCP Agent Gateway with Policy-Chained, Sandboxable Sessions

- **Effort:** L  |  **Lens:** ai-data-plane (merged with competitive)
- **Pitch:** First PG proxy with a native MCP server: agents call query/schema/explain tools and every call inherits ai-classifier, token-budget, llm-guardrail, cost-governor, and audit-chain with zero new policy code.
- **Why it wins:** Highest-differentiation move available: in 2026 agents speak MCP first, and no pooler (PgBouncer, PgCat, Supavisor, RDS Proxy, Neon) enforces anything about agent traffic — while HeliosProxy already owns the entire enforcement layer as shipped, signed plugins whose hook_context carries agent_id/model_id. The admin HTTP server makes SSE/streamable-HTTP incremental; sandboxed auto-rollback sessions (competitive lens) and a preflight EXPLAIN/cost tool (ai-data-plane lens) fold in as phase 2. Turns the existing AI plugin suite from features into a product category.
- **Builds on:** src/admin.rs HTTP surface, src/plugins/ hook chain + first-party AI plugins, src/auth/{api_keys,jwt}.rs, src/pool/ + src/backend/client.rs session mapping, ha-tr transaction journal for sandbox rollback, src/rate_limit/cost_estimator.rs + src/analytics for the explain/preflight tool

### [86] Continuous Traffic Mirroring + Blue/Green Cutover GA

- **Effort:** L  |  **Lens:** competitive + enterprise-ops (merged)
- **Pitch:** Mirror a sampled percentage of live traffic to a second cluster with always-on row-hash diffing, and use it to finish the stubbed upgrade orchestrator into a real one-call PG-12-to-17 cutover with parity evidence and rollback.
- **Why it wins:** One merged feature closes a competitive gap (PgCat/pgDog mirror), leapfrogs it (nobody diffs the mirror), and repairs a verified credibility risk: the README advertises zero-downtime upgrades while src/upgrade_orchestrator/mod.rs transitions are stubs that log and advance state. Every hard component is already shipped and tested separately — shadow_execute's order-independent diffing, transaction journal, replay engine, switchover buffer — so this is the highest-ROI glue work in the repo, and parity reports turn major-version upgrades from a top-3 bank pain into a buying trigger.
- **Builds on:** src/shadow_execute/ diff engine, src/transaction_journal.rs + src/replay/, src/switchover_buffer.rs, src/upgrade_orchestrator/ state machine, admin UI panels, query-analytics fingerprinting, chaos API for rehearsal

### [83] HTTP/WebSocket SQL Gateway (Neon-Compatible Serverless Driver)

- **Effort:** M  |  **Lens:** competitive
- **Pitch:** A Neon-serverless-driver-compatible HTTP one-shot and WebSocket session endpoint, so Cloudflare Workers, Vercel Edge, and Lambda reach any Postgres through HeliosProxy with a one-line connection-string change.
- **Why it wins:** Neon proved the demand but only works against Neon; no self-hostable OSS pooler offers an HTTP/WS SQL surface, making this a whole-new-buyer unlock (the edge/serverless ecosystem) rather than a parity move. HeliosProxy is uniquely close: an HTTP-to-SQL path already exists in the admin SQL Console, auth-proxy supplies JWT/API-key validation for stateless requests, query-cache gives sub-millisecond cached one-shots, and edge-proxy turns each PoP into a regional endpoint. Should share its session-mapping and auth layer with the MCP gateway.
- **Builds on:** src/admin.rs HTTP stack + SQL Console, src/auth/ (JWT/API keys), query-cache L1/L2/L3, edge-proxy module, rate-limiting

### [82] Per-Agent Scoped Grants + SQL Contract Validator with Repair Hints

- **Effort:** L  |  **Lens:** ai-data-plane (two proposals merged)
- **Pitch:** Proxy-minted, TTL-bound agent credentials whose table/column/verb/row-filter manifests compile into a wire-level allowlist — and every block returns a machine-readable repair hint so agents self-correct in one round trip.
- **Why it wins:** Agent IAM without touching PG roles is a category nobody serves: today every agent run shares the app's DB user with whole-schema blast radius. The two ai-data-plane proposals are one feature — the grant manifest IS the contract, enforced by one interceptor — and the pieces exist (role_mapper, api_keys, tenant identifier, rewriter filter injection, RouteResult::Block ABI that already synthesizes ErrorResponse). Structured DETAIL payloads make LLM-generated SQL converge instead of flail, and grant-scoped cache keys fix a latent privilege-leak in query-cache. Pairs natively with the MCP gateway.
- **Builds on:** src/auth/{role_mapper,api_keys}.rs, src/multi_tenancy/identifier.rs, src/rewriter/ (filter injection), RouteResult::Block ABI in src/plugins/runtime.rs, plugin KV, query-cache key derivation, llm-guardrail plugin

### [81] Audit-Grade Evidence Pipeline + Admin Authentication

- **Effort:** M  |  **Lens:** enterprise-ops (absorbs ai-data-plane flight recorder)
- **Pitch:** Durable, hash-chained audit stream to WORM/S3/syslog with explicit backpressure semantics, one-call signed compliance evidence bundles, per-(agent_id, run_id) queryable timelines — and authn on the currently wide-open admin surface.
- **Why it wins:** Verified: every audit artifact today is volatile (hash chain in plugin KV memory, anomalies in a ring buffer) and src/admin.rs has zero auth on any endpoint including the SQL console — independently a release blocker for regulated deployment that this feature forces. Banks buy 'prove it was masked for 90 days', not 'we can mask'; the Ed25519 signing machinery for chain anchoring is already in-tree. The ai-data-plane Agent Flight Recorder folds in as run-keyed views over the same stream, ticking the 'agent audit trail' checkbox enterprise buyers now ask for by name.
- **Builds on:** audit-chain plugin + env.sha256_hex, anomaly event ring buffer + GET /anomalies, analytics/slow_log.rs, transaction_journal.rs retention, Ed25519 key handling from plugin signing, ai-classifier agent_id extraction, src/admin.rs

### [80] Instant Branch Databases with Agent Sandbox Mode

- **Effort:** L  |  **Lens:** dx-ecosystem (merged with ai-data-plane)
- **Pitch:** Neon-style copy-on-write branches for vanilla Postgres, provisioned and routed entirely through the proxy — and `sandbox=true` agent sessions whose writes land on a branch with diff, discard, or replay-to-promote.
- **Why it wins:** Branching is the single biggest bottom-up adoption driver in modern DB DX, and HeliosProxy can deliver it without Neon's storage layer: POST /api/replay with target overrides already does most of branch hydration, template databases give cheap copies, routing maps branch names from startup params, and Nano branches natively. The same substrate is the marquee agent-safety demo (the ai-data-plane sandbox proposal), with shadow_execute providing branch-vs-prod diffing; promote-conflict semantics are the deferred XL tail. No competitor proxy offers anything like it.
- **Builds on:** src/replay/ + transaction_journal.rs, src/shadow_execute/ diff, src/routing/ + schema-routing, multi-tenancy per-pool isolation, upgrade_orchestrator state-machine pattern, HeliosDB-Nano native branching

## Backlog — remaining ideas from all four lenses (not in top 12)

- **Client-Side TLS Termination** (competitive, leverage 10, effort M): Accept SSLRequest and terminate rustls TLS on the client listener with SNI, hot cert reload, and per-listener policy — instead of rejecting encryption outright.
- **SCRAM Verifier Auth + pg_hba Rules + auth_query** (competitive, leverage 9, effort M): Proxy-terminated SCRAM-SHA-256 client authentication with verifiers fetched via auth_query, plus pg_hba-style host/db/user access rules — so pooling works against real production auth instead of passthrough.
- **Continuous Traffic Mirroring with Sampled Diffing** (competitive, leverage 8, effort M): Mirror a configurable percentage of live traffic to a second cluster and continuously diff results — PgCat/pgDog can mirror, but nobody can tell you the mirror disagrees.
- **Tenant-Aware Sharding Router (Direct-to-Shard)** (competitive, leverage 8, effort XL): List/range/hash shard routing with automatic shard-key extraction and per-shard pools — the multi-tenant SaaS sharding use case pgDog is winning, without waiting for full scatter/gather.
- **HTTP/WebSocket SQL Gateway (Self-Hosted Serverless Driver)** (competitive, leverage 9, effort M): A Neon-compatible HTTP one-shot-query and WebSocket session endpoint on the proxy, so edge functions (Cloudflare Workers, Vercel Edge, Lambda) reach any Postgres through HeliosProxy without TCP.
- **MCP Agent Gateway with Sandboxed Agent Sessions** (competitive, leverage 8, effort M): First proxy with a native MCP server: agents get scoped, guardrailed, auto-rollback SQL sessions with per-agent token budgets and a tamper-evident audit trail — turning the existing AI plugin suite into a product category.
- **MCP Gateway (`mcp-gateway` feature)** (ai-data-plane, leverage 10, effort L): Native MCP server on the proxy: agents call query/schema/explain tools over MCP, and every call inherits the full plugin policy chain.
- **Per-Agent Identities with Scoped Grants** (ai-data-plane, leverage 9, effort L): Proxy-minted, short-lived agent credentials whose table/column/operation scopes are enforced at the wire level — agent IAM without touching PG roles.
- **Query-Cost Preflight (dry-run API for agents)** (ai-data-plane, leverage 8, effort M): Agents ask 'what will this query cost?' before running it; the proxy answers from EXPLAIN-on-replica plus its own fingerprint history, and gates expensive queries behind a confirm token.
- **Sandbox Branch Sessions for Agent Experimentation** (ai-data-plane, leverage 9, effort XL): An agent connection flagged `sandbox=true` gets its writes routed to a copy-on-write branch; the proxy journals everything and offers diff, discard, or replay-to-promote.
- **SQL Contract Validator with Machine-Readable Repair Hints** (ai-data-plane, leverage 8, effort M): Blocked AI queries return structured JSON repair hints ('missing tenant filter: add WHERE tenant_id = $1') instead of opaque errors, so agents self-correct in one round trip.
- **Agent Flight Recorder (per-run audit timeline)** (ai-data-plane, leverage 8, effort M): Every agent run gets a queryable, hash-chained timeline — each query with fingerprint, cost, rows touched, plugin verdicts — exported as OTel spans keyed by agent_id/run_id.
- **Result-Set PII Egress Guard** (ai-data-plane, leverage 9, effort L): Wire-level DLP on DataRow messages flowing back to agents: detect, redact, or block PII in results regardless of how the query was phrased, and alarm on bulk-extraction patterns.
- **Vector Workload Optimizer** (ai-data-plane, leverage 7, effort M): 10x pgvector-router: adaptive ef_search/probes tuning, similarity-aware reuse of top-K results via the existing L3 cache, and ANN-vs-exact routing to tagged replicas.
- **Client-Facing TLS, mTLS, and SPIFFE Workload Identity (+ FIPS posture)** (enterprise-ops, leverage 10, effort L): Terminate TLS from clients, authenticate apps by mTLS/SPIFFE SVID instead of passwords, and offer a FIPS-capable crypto build — the table-stakes gate for bank and healthcare procurement.
- **Zero-Downtime Reload: SIGHUP Config Apply + SO_REUSEPORT Binary Handoff** (enterprise-ops, leverage 9, effort L): Change pools, nodes, limits, and even the binary version without dropping a single client connection.
- **Per-Query Distributed Tracing with traceparent Propagation into PostgreSQL** (enterprise-ops, leverage 9, effort M): W3C traceparent flows from the app through the proxy into PG as a SQL comment + application_name, with a proxy span per query showing pool wait, cache tier, route decision, and backend time.
- **Helios Fleet: Control Plane for Many Proxies** (enterprise-ops, leverage 9, effort XL): One control plane where every proxy registers, pulls signed config/policy/plugin bundles, and reports topology + analytics — manage 200 sidecars like one.
- **Blue/Green Database Cutover GA (finish the Upgrade Orchestrator)** (enterprise-ops, leverage 8, effort L): One API call migrates a live workload PG-12->17 or onto new hardware: replicate, shadow-verify, buffered cutover, replay, retire — with drift metrics and one-command rollback.
- **Connection Storm Protection and Post-Failover Slow-Start** (enterprise-ops, leverage 8, effort M): Survive the reconnect avalanche: handshake admission control, a fair client wait-queue with backoff hints, pause/resume per database, and ramped backend reconnects after failover.
- **Audit-Grade Logging Pipeline + Compliance Evidence Export** (enterprise-ops, leverage 8, effort M): Guaranteed-delivery, hash-chained audit stream to WORM/S3/syslog with backpressure semantics, plus one-call signed evidence bundles mapped to SOC 2 / PCI-DSS-10 / HIPAA controls.
- **SLO-Based Adaptive Routing with Capacity Forecasting** (enterprise-ops, leverage 7, effort M): Declare p99/error SLOs per route class; the proxy shifts reads, sheds to cache, and brownouts low-priority traffic to defend the budget — and forecasts when you'll run out of headroom.
- **Instant Branch Databases (helios branch)** (dx-ecosystem, leverage 10, effort L): Neon/PlanetScale-style copy-on-write database branches provisioned and routed entirely through the proxy — `helios branch create feature-x` and connect with `?options=branch=feature-x`.
- **Migration Safety Gate (ddl-gate feature)** (dx-ecosystem, leverage 9, effort M): Lint every DDL statement on the wire, auto-inject lock_timeout/statement_timeout before it runs, and block table-rewrite footguns (CREATE INDEX without CONCURRENTLY, volatile defaults, NOT NULL without validation) with a proper PG error and fix suggestion.
- **Query Flight Recorder with Shareable Replay Bundles** (dx-ecosystem, leverage 8, effort M): `helios record --window 5m` captures fingerprints, params, timings, and plans into a signed portable .tar.gz; anyone replays it against any backend with `helios replay bundle.tgz` and gets a shadow-diff report.
- **ORM-Native Insights (Prisma/Drizzle/SQLAlchemy/ActiveRecord)** (dx-ecosystem, leverage 8, effort M): Detect the ORM from application_name and query shape, then surface N+1 and slow-query findings where devs actually live: dev-mode PostgreSQL NOTICE messages that appear inline in the ORM's own log, plus marginalia-comment parsing to pin findings to file:line.
- **SQL-Level Feature Flags (flag-routing)** (dx-ecosystem, leverage 7, effort M): Declare flags in the admin KV and gate rewriter rules on them per tenant/user/percentage: dual-write to a new schema for 5% of tenants, kill-switch a pathological query pattern in prod without a deploy, A/B a rewritten query.
- **Typed Schema Diff + Live Drift Guard (helios schema diff)** (dx-ecosystem, leverage 7, effort M): Snapshot a typed schema catalog through the proxy, diff between branches/environments, and uniquely cross-reference against live query analytics: 'dropping users.ssn breaks 3 query fingerprints seen 1,200x in the last 24h'.
- **helios dev: One-Command Local Stack + TUI Console** (dx-ecosystem, leverage 7, effort M): `helios dev up` starts the proxy plus an ephemeral Postgres/Nano backend with fixtures/*.sql seeded through the proxy, then drops into a ratatui console streaming live queries, latency histograms, N+1 alerts, topology, and plugin state.