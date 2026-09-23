# HeliosDB Proxy

HeliosProxy (`heliosdb-proxy`, Rust 2021, MSRV 1.86) is a PostgreSQL-wire-protocol
connection router: pooling (session/transaction/statement), automatic failover with
transaction replay, lag-aware query routing, caching, auth proxy, rate limiting, WASM
plugins, GraphQL/HTTP/MCP gateways. Works against PostgreSQL, HeliosDB-Lite/Nano/Full,
and any PG-wire backend. Repo conventions live in `AGENTS.md` — read it; this file adds
the mandatory quality gates and does not duplicate it. Where `AGENTS.md` conflicts
with this file, `Cargo.toml`, or `.github/workflows/ci.yml`, those win.

## Build & Test

- Build: `cargo build` (default = `pool-modes`); full: `cargo build --features all-features`
- Full test suite: `cargo test --features all-features`
- CI feature matrix (all must pass): `cargo test`, `cargo test --features ha-tr`,
  `cargo test --features all-features`, `cargo test --features all-features,postgres-topology`
- Backend-dependent integration tests (need a live PG/Nano, see `.github/workflows/ci.yml` env vars):
  `cargo test --test integration --features all-features,postgres-topology -- --include-ignored`
- Live regression battery: `BK=pg scripts/regress/run.sh <proxy-binary>` (or `BK=nano`).
  Needs Docker for the psql/pgbench client tooling AND an already-running backend — the
  script does not start one (BK=pg expects PG 18.4 at `127.0.0.1:25433`, user `bench`;
  BK=nano expects Nano at `100.64.0.2:54320`)
- MSRV: `cargo check --locked --features msrv-features` on Rust 1.86

## Quality Gates (mandatory — every change must pass ALL before commit/merge)

1. **Full test suite passes**: `cargo test --features all-features` (plus the CI feature
   matrix above for changes touching feature-gated code). No skipped/`#[ignore]`d tests
   without a written justification in the commit message.
2. **No regression**: any behavior change must keep all existing tests green; add tests for
   every new code path (`#[cfg(test)]` unit tests next to the code; `tests/integration/` when
   behavior crosses network/backend/process boundaries).
3. **Benchmarks**: `cargo bench --features all-features` (Criterion: `benches/pooling.rs`,
   `benches/routing.rs`; CI only compile-checks with `--no-run`). All available benchmarks
   must be non-negative vs the recorded baseline, and CUMULATIVE performance degradation
   across a work session must stay under 3% vs baseline. **No Criterion baseline is recorded
   yet** — the FIRST task of any implementation session is to record one into
   `/home/gpc/HDB/Proxy/benches/BASELINE.md` (bench name, metric, value, date, host, features,
   commit). The proxy-path scalability harness is `scripts/regress/bench-scalability.sh
   <proxy-binary>` (Dockerized pgbench against an already-running PG 18.4 backend at
   `127.0.0.1:25433`; heavy — see Resource Constraints); its evidence
   baseline tables live in `docs/perf-2026-07/README.md` and are re-measured back-to-back
   with the candidate, never compared across days.
   **User-path gate (mandatory for write-path changes and releases)**: Criterion runs one
   task per lock and cannot see scheduler convoys under concurrent write load — on
   2026-09-21 it passed a TR-07 candidate at +2.97% while the live-backend user-path harness
   showed -40% committed TPS at 16 clients (async-lock convoy, fixed in 8f412dc). For any
   change touching `src/server.rs` relay paths, `src/journal_capture.rs`,
   `src/transaction_journal.rs`, or `src/pool/`, and before every release, run
   `scripts/perf-gate-userpath.sh <base-binary> <candidate-binary> <label>`. It wraps
   `scripts/regress/bench-usertp.sh` for 3 interleaved base/candidate passes
   (`CLIENTS="16 64" DUR=12 PGOPTIONS="-c synchronous_commit=off" MODES="session transaction"`)
   and gates on `scripts/regress/compare-usertp.py`, which exits 1 if any proxy-mode
   `committed_write` cell regresses beyond a 3% budget (`BUDGET_PCT`) in TPS or p99 (medians
   over passes). Like every Docker/pgbench harness here it needs a live PostgreSQL backend
   and explicit owner approval per run (see Resource Constraints) — record that approval in
   the GATE-RECORD.
4. **Lint gates** (exact CI commands):
   - `cargo fmt --check`
   - `cargo clippy --features all-features -- -D warnings` (CI also runs the matrix: default,
     `ha-tr`, `all-features,postgres-topology`)
   - `cargo check --locked --features msrv-features` (Rust 1.86)
   - No `clippy.toml` / `deny.toml` exists; do not add suppressions — fix warnings.
5. **Interface coverage**: every new or changed function/feature must be reachable through at
   least one user-facing interface below (CLI flag, `proxy.toml` parameter, PG-wire behavior,
   admin HTTP endpoint, or gateway) and be tunable — no new hardcoded magic numbers; expose
   thresholds/sizes/timeouts as config parameters or CLI flags with documented defaults.

## Interfaces

- **CLI `heliosdb-proxy`** (src/main.rs, clap):
  - Daemon flags: `-c/--config <file>`, `-l/--listen` (default `0.0.0.0:5432`),
    `--admin` (default `127.0.0.1:9090`), `--primary <host:port>`, `--standby <host:port>`
    (repeatable), `--tr` (default true), `--log-level`, `--json-logs`
  - Subcommand: `install skills [--target claude|codex|both] [--symlink] [--force] [--dry-run]`
  - Signals: SIGHUP = live config reload; SIGUSR2 = graceful drain for zero-downtime
    binary handoff (bounded by `shutdown_drain_timeout_secs`, env override
    `HELIOS_DRAIN_TIMEOUT_SECS`). There is NO SIGTERM/ctrl-c handler — SIGTERM kills
    the process immediately
- **CLI `helios-plugin`** (src/bin/helios-plugin.rs): `install`, `list`, `verify`, `new`
  (plugin registry / Ed25519 signature tooling)
- **Config file** `proxy.toml` — examples in `config/proxy.example.toml`,
  `config/proxy.full.toml`, `config/proxy.postgres.toml`; the working configs under
  `scripts/regress/*.toml` are the most current examples. Authoritative parser:
  `ProxyConfig` in `src/config.rs` (~1850 lines).
  Top-level keys: `listen_address`, `admin_address`, `admin_token`,
  `admin_allow_insecure`, `tr_enabled`, `tr_mode`, `write_timeout_secs`,
  `optimize_unnamed_parse`, `shutdown_drain_timeout_secs`.
  Sections: `[pool]`, `[pool_mode]`, `[load_balancer]`, `[health]`, `[[nodes]]`, `[tls]`,
  `[cache]`, `[routing_hints]`, `[lag_routing]`, `[rate_limit]`, `[circuit_breaker]`,
  `[limits]`, `[analytics]`, `[anomaly]`, `[multi_tenancy]`, `[auth]` (only `mode = "passthrough"|"scram"` +
  `auth_file`), `[[hba]]`, `[query_rewrite]` (+`[[query_rewrite.rules]]`), `[plugins]`,
  `[graphql_gateway]` (+`[[graphql_gateway.tables]]`), `[schema_routing]`, `[mcp]`,
  `[[agent_contracts]]`, `[http_gateway]`, `[mirror]`, `[edge]`, `[branch]`.
  CAUTION: unknown TOML sections are silently ignored (plain serde). The commented
  `[routing.*]`, `[lag]`, `[rewriter]`, `[graphql]`, `[auth.jwt]`-style, `[cache.l1]`-style
  blocks in `config/proxy.full.toml` — and its uncommented `[ha]`, `[logging]`, `[metrics]`
  sections — do NOT exist in `ProxyConfig` and are ignored. There is no `[distribcache]`
  section (distribcache is a library module, not proxy.toml-wired). NOTE: as of the
  1.4.1 config batch, anomaly detection IS proxy.toml-wired via the `[anomaly]` section
  (its defaults reproduce the old hardcoded `AnomalyConfig::default()` +
  `MAX_SEEN_FINGERPRINTS`, so an absent section is unchanged), and operational limits
  via `[limits]`
- **PG-wire listener** (default `:5432`): any PostgreSQL client; routing hints via
  SQL comments, e.g. `/*helios:route=primary*/` (syntax is `/*helios:key=value,...*/` —
  see `src/routing/hint_parser.rs`; requires the `routing-hints` feature AND
  `[routing_hints] enabled = true`)
- **Admin HTTP API** (default `127.0.0.1:9090`; token-gated off loopback), src/admin.rs:
  `/health`, `/health/live`, `/health/ready` (plus token-exempt Kubernetes-style
  aliases `/healthz`→`/health`, `/livez`→`/health/live`, `/readyz`→`/health/ready`,
  routed and identical to their slash-form twins), `/metrics`,
  `/metrics/prometheus`, `/version`, `/config`, `/topology`, `/nodes`, `/nodes/{addr}`,
  `/nodes/{addr}/enable|disable`, `/sessions`, `/pools`, `/plugins`, `/analytics`,
  `/anomalies`, `/branch`, `/api/sql`, `/api/chaos`, `/api/replay`, `/api/shadow`,
  `/api/branch`, `/api/circuit`, `/api/analytics`,
  `/api/edge` (+`/register`, `/subscribe`, `/invalidate`),
  `/api/migration/{snapshot,cutover,cutover/rollback,status}`
- **Web UI**: embedded admin dashboard at `GET /` and `/ui` on the admin port
  (src/admin_ui.html)
- **HTTP SQL gateway** (`[http_gateway] enabled = true`): Neon-serverless-compatible
  `POST /sql` (src/http_gateway.rs)
- **GraphQL gateway** (`[graphql_gateway] enabled = true`): separate HTTP listener
  (default `0.0.0.0:9091`), `POST` body `{"query": "..."}` → `{"data": ...}`
  (src/graphql_gateway.rs; tables exposed via `[[graphql_gateway.tables]]`)
- **MCP agent gateway** (`[mcp] enabled = true`): JSON-RPC 2.0 over HTTP POST; tools
  `query`, `list_tables`, `explain` (src/mcp.rs)

## Resource Constraints

This host crashed ~16h ago (suspected OOM) and runs production-like services. Therefore:

- Run at most ONE heavy build/test/benchmark at a time — never in parallel; no
  `cargo build & cargo test &`, no parallel feature-matrix runs. Prefer sequential
  matrix runs and let one finish before starting the next.
- Cap benchmark dataset sizes and client counts to what the recorded baselines used
  (`bench-scalability.sh` defaults: `CLIENTS="1 16 64"`, `DUR=8`; the 2026-07-04
  evidence baseline in `docs/perf-2026-07/README.md` was run with `CLIENTS="1 4 16 64"`,
  `DUR=10` — match whichever baseline you compare against); never scale them up
  to "stress" the host.
- NEVER touch `/home/gpc/heliosdb-ada-data`, any `/data` directories, or running
  heliosdb services/containers. Docker-backed harnesses vary: `tests/docker/*` compose up
  their own cluster and `scripts/regress/ldap-test.sh` starts its own LDAP container, but
  `scripts/regress/run.sh`, `scripts/regress/bench-scalability.sh`, and
  `benchmarks/bench-engines.sh` use Docker only for the psql/pgbench client tooling and
  REQUIRE an already-running backend (PG 18.4 at `127.0.0.1:25433` — the
  `codex-pg184-bench` container — or a live Nano). Only run any of them with explicit
  user approval, one at a time, and clean up afterwards.

### Bounded benchmark invocation (mandatory — root cause of the 2026-07-08 host crash)
A runaway benchmark (38 GiB RSS) livelocked this host for 16h. Run ANY heavy benchmark or load-generating process in a bounded scope so it dies alone instead of taking the host down:
```bash
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 <bench command>
```
Full incident report: /home/gpc/HDB/sprint/status/incident-2026-07-08.md
