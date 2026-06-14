# HeliosProxy — 2026-06 Audit Implementation Close-Out

Branch: `audit-2026-06-perf` (merged to `main`, **not pushed** — local only).
Verification backends throughout: **PostgreSQL 18.4** (`codex-pg184-bench`, 127.0.0.1:25433) and **HeliosDB-Nano** (3.37 / 3.57). Client tooling: psql + pgbench 18.4 via `docker run --network host postgres:18.4-bookworm`. Per-batch gate: `BK=pg|nano ./scripts/regress/run.sh <binary>` plus a feature-specific live test.

## Status: A–H delivered; F, G, G2 complete; H.1 shipped (binary-handoff + plugin-registry leftovers deferred by decision)

| Batch | What shipped | Commit | Live proof |
|------|--------------|--------|-----------|
| **A** | Hot-path quick wins | `9da6936` | regression battery green |
| **B** | Extended protocol + streaming relay | `9da6936` | pgbench -M extended/prepared abort→working; large-result RSS 113MB→7MB |
| **C** | Per-session multi-node backend connection cache | `8da7132` | PG single + 2-node read/write-split + Nano |
| **D** | WASM runtime: InstancePre, enforced timeout, sharded metrics | `fd4a2e5` | `test_call_hook_enforces_timeout` + battery |
| **E** | Verify-then-fix 35 findings (6 actionable, 28 dead-code) | `a27a164` | health→ArcSwap, parallel sweep, anomaly→DashMap; `batch-e-verdicts.json` |
| **F.1** | Query cancellation forwarding | `bb2196c` | `cancel-test.sh` (pg_sleep cancelled) |
| **F.2** | Client TLS termination + mTLS | `82766d4` | `tls-test.sh` (TLSv1.3 + plaintext) |
| **F.3a** | pg_hba-style admission rules | `c623bd4` | `hba-test.sh` (28000 reject) |
| **F.3b** | Proxy-terminated SCRAM-SHA-256 | `d3452f9` | `scram-test.sh` (correct admit / wrong reject) |
| **F.4** | Prepared statements survive backend switches | `3902d1e` | `prepared-stmt-test.sh` 4/4 + 5 unit tests |
| **G** | MCP agent gateway | `0f9186c` | `mcp-test.sh` 6/6 (+ fixed 2 latent protocol bugs) |
| **G** | Per-agent scoped grants + SQL contract validator | `9cbef11` | `mcp-test.sh` 9/9 w/ repair hints |
| **G** | Neon-compatible HTTP SQL gateway | `74ea049` | `http-gw-test.sh` 6/6 |
| **G** | Continuous traffic mirroring | `487015c` | `mirror-test.sh` PG→Nano writes propagate |
| **G** | Instant branch databases (CREATE DATABASE TEMPLATE) | `4bd95a7` | `branch-test.sh` 6/6 (clone + isolation + drop) |
| **G** | Admin API Bearer-token auth | `2d69df6` | `admin-auth-test.sh` (release blocker closed) |
| **G2** | Migration status + `migration_ready` | `c09ceec` | `mirror-test.sh` 5/5 |
| **G2** | Snapshot bootstrap of existing data | `bac104b` | `snapshot-test.sh` (PG rows → Nano) |
| **G2** | Transparent cutover + rollback | `54b6384` | `cutover-test.sh` 5/5 (`version()` flips PG↔Nano on one client) |
| **H.1** | Zero-downtime SIGHUP config reload | `f78cd86` | `reload-test.sh` 6/6 (in-flight conn survives; new conn sees reload; bad config rejected) |
| — | 2×2 scalability matrix (2 proxy × 2 Nano) | `9b9e2f5` | `SCALABILITY-MATRIX.md` + Nano v3.57 recs |

## Deferred by decision (2026-06-14)

- **Item 84 — binary handoff** (SO_REUSEPORT + session adoption via `switchover_buffer`/`session_migrate`): Effort L. H.1 landed the SIGHUP config-apply half; the live-binary-swap half remains.
- **Item 78 — plugin registry + `helios-plugin install`**: audit rated S but presumes a `helios-plugin` CLI that does not exist in this repo (only `heliosdb-proxy` builds here; `loader.rs` has the `SignatureVerifier`/Ed25519 trust-root + `PluginManifest`, no CLI). A testable offline slice = `install` from a `file://` registry index reusing `SignatureVerifier` + a `new` scaffold.

## Cross-team finding (HeliosDB-Nano)

While validating the COPY relay against Nano v3.58.0, found COPY FROM STDIN **deadlocks on the wire** (reproduces direct via psql, control-proven against PG): `handle_copy` sends `CopyInResponse` through its `BufWriter` then blocks on `read_message` for CopyData without flushing. One-line fix (`self.flush().await?;`) verified end-to-end through the proxy (500 rows, count+sum exact, single-RFQ correct). Patch handed to the Nano team; their tree left pristine.

## Release posture

`main` is local-only (not pushed); crates.io publish is the outward-facing gate and remains **unperformed pending explicit confirmation**. The user's pre-existing uncommitted working-tree changes (skills, READMEs, demos, LICENSE, AGENTS.md, website-briefs) were never staged.
