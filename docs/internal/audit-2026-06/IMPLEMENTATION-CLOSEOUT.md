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
| **H.2** | Plugin registry + `helios-plugin install` | `6f4c524` | `plugin-install-test.sh` 7/7 (signed install verified, sha256/untrusted-signer rejected) + 7 unit tests |
| **H.3** | Zero-downtime binary handoff (SO_REUSEPORT + drain) | `3091ea9` | `handoff-test.sh` 6/6 (B binds shared port; 8/8 new conns served during handoff; in-flight survives; A drains+exits) |
| — | 2×2 scalability matrix (2 proxy × 2 Nano) | `9b9e2f5` | `SCALABILITY-MATRIX.md` + Nano v3.57 recs |

## Roadmap complete

Every batch from the 2026-06 audit (A–H) is delivered and verified on PG 18.4 + Nano. Audit item 84 is delivered as SIGHUP config reload (H.1) + SO_REUSEPORT binary handoff with graceful drain (H.3) — the zero-downtime upgrade promise for new connections plus clean draining of old ones.

Documented frontiers (beyond the audit scope, optional):
- **Live session adoption**: passing in-flight client FDs to the new process via `SCM_RIGHTS` so a *mid-query* connection migrates between proxy processes (`session_migrate`/`switchover_buffer` serialize the session-state half; FD passing + mid-protocol resumption is the hard remainder). The SO_REUSEPORT+drain handoff covers the real-world upgrade case without it.
- Item 78 **`https://` artefact fetch** (public registry over GitHub Releases): a thin follow-on at the install fetch step; the offline `file://` slice with full SHA-256 + Ed25519 verification shipped.
- Admin listener SO_REUSEPORT (the client listener handles the connection-critical path today).

## Cross-team finding (HeliosDB-Nano)

While validating the COPY relay against Nano v3.58.0, found COPY FROM STDIN **deadlocks on the wire** (reproduces direct via psql, control-proven against PG): `handle_copy` sends `CopyInResponse` through its `BufWriter` then blocks on `read_message` for CopyData without flushing. One-line fix (`self.flush().await?;`) verified end-to-end through the proxy (500 rows, count+sum exact, single-RFQ correct). Patch handed to the Nano team; their tree left pristine.

## Release posture

`main` is local-only (not pushed); crates.io publish is the outward-facing gate and remains **unperformed pending explicit confirmation**. The user's pre-existing uncommitted working-tree changes (skills, READMEs, demos, LICENSE, AGENTS.md, website-briefs) were never staged.
