# BATCH H — Ops & DX quick wins

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** Small-effort, high-visibility wins.

**Parallel-execution compatibility:** PARALLEL-friendly.

**Files touched:** new modules + `src/config.rs` + helios-plugin CLI

**Conflicts with:** Independent of everything else.

**Acceptance criteria:**

- Demo + docs per repo convention.

---

### [84] Zero-Downtime Reload: SIGHUP Config Apply + Binary Handoff

- **Effort:** L  |  **Lens:** enterprise-ops
- **Pitch:** Change pools, nodes, limits — or the binary itself via SO_REUSEPORT handoff with session adoption — without dropping a single client connection.
- **Why it wins:** Restart-to-reconfigure is the #1 operational objection to standardizing on any proxy (PgBouncer RELOAD and Envoy hot-restart set the expectation), and it is an awkward story that WASM plugins hot-reload while proxy.toml does not. HeliosProxy uniquely owns the two hard parts already: SwitchoverBuffer holds queries mid-flight and SessionMigrate serializes full session state including prepared statements — so a new process can adopt sessions, not just sockets, which no competitor does. Table-stakes entry, differentiated exit.
- **Builds on:** src/switchover_buffer.rs, src/session_migrate.rs, src/config.rs, src/server.rs listener, plugins/hot_reload.rs file-watch pattern

### [78] Plugin Registry + One-Command Install

- **Effort:** S  |  **Lens:** dx-ecosystem
- **Pitch:** `helios-plugin install column-mask` resolves a signed artefact from a static HTTPS registry index, verifies against the trust root, and hot-reload picks it up live; `helios-plugin new` scaffolds plugins from a template.
- **Why it wins:** The entire trust pipeline — Ed25519 signatures, SHA-256 manifests, OCI tarballs, hot reload, a CLI with pack/inspect/verify — is already built; distribution is the only missing verb, and a JSON index over GitHub Releases is days of work. Marketplaces are how proxies become platforms (Envoy, Kong): the 8 first-party plugins are a launch catalog, third-party plugins become free marketing, and an ecosystem is the one moat competitors cannot fast-follow. Second quick win in the portfolio with genuine network-effect leverage.
- **Builds on:** helios-plugin CLI, src/plugins/loader.rs (OCI + SHA-256 + Ed25519 SignatureVerifier), src/plugins/hot_reload.rs, 8 first-party plugins

<details><summary>Full original rationale (dx-ecosystem lens)</summary>

The entire trust pipeline is built — signed artefacts, manifest hashes, trust roots, hot reload — but distribution is still 'clone the plugins repo and cargo build'. A static registry index (JSON over HTTPS, artefacts on GitHub Releases) plus an `install` subcommand is days of work and converts the WASM platform from a feature checkbox into an ecosystem with network effects. Marketplaces are how proxies become platforms (cf. Envoy/Kong); third-party plugins are also free marketing and the moat competitors can't fast-follow.

</details>


## Judge notes on merges/overlaps

Verification: no proposal duplicates shipped functionality — confirmed in-source that client TLS is rejected (server.rs:905-908 answers 'N'), CancelRequest just closes (server.rs:936), backend is text-format-only (src/backend/mod.rs:17), the observability feature has zero cfg-gates in src/, no MCP code exists, upgrade_orchestrator transitions are stubs, and admin.rs has no auth. Merges: (1) Client TLS proposed by both competitive and enterprise-ops — merged, with mTLS/SPIFFE/FIPS as the enterprise tail. (2) MCP gateway proposed by competitive (sandboxed agent sessions) and ai-data-plane — merged; the ai-data-plane Query-Cost Preflight folds in as the MCP explain/preflight tool, and sandboxed sessions share substrate with branching. (3) dx-ecosystem Instant Branch Databases and ai-data-plane Sandbox Branch Sessions are the same journal+replay+clone substrate — merged with sandbox as a mode. (4) competitive Continuous Traffic Mirroring and enterprise-ops Blue/Green Cutover GA both exist to finish the stubbed upgrade orchestrator via shadow_execute diffing — merged. (5) ai-data-plane Per-Agent Identities and SQL Contract Validator share one manifest store and enforcement interceptor — merged. (6) The observability cluster (enterprise-ops distributed tracing, ai-data-plane Agent Flight Recorder, dx-ecosystem Query Flight Recorder bundles) all sit on the fingerprint/journal/audit substrate; the Audit Evidence Pipeline winner absorbs the flight recorder's (agent_id, run_id) timelines, while traceparent tracing and shareable replay bundles are the top runners-up. (7) HTTP/WS gateway and MCP gateway share the admin HTTP + auth + session-mapping layer — kept as separate products for different buyers but should be built on one shared surface. Notable cuts despite merit: Connection Storm Protection / PAUSE-RESUME (first runner-up, partially mitigated by rate-limiting + circuit-breaker today), per-query distributed tracing, Result-Set PII Egress Guard, DDL Migration Safety Gate, ORM-Native Insights, Tenant-Aware Sharding (XL, strategically loud but premature before TLS/SCRAM/prepared-statement table stakes land), Helios Fleet control plane (XL phase-2 after zero-downtime reload), SLO routing, vector optimizer, SQL feature flags, schema diff, and helios dev TUI.
