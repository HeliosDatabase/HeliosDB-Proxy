# HeliosProxy deep audit — 2026-06-10 — execution plan

Source: 77-agent analysis workflow (8 subsystem auditors, adversarial verification per finding,
4 feature-ideation lenses + judge panel). 29 findings verified high-confidence; 32 reported
findings await verification (BATCH E); 12 features judge-ranked from 30 proposals.
Raw machine-readable results: `audit-result.json`.

## Batches

| Batch | File | Theme | Risk | Parallel? | Depends on |
|-------|------|-------|------|-----------|------------|
| A | `BATCH-A-perf-quick-wins.md` | Hot-path perf quick wins | Low | solo (first) | — |
| B | `BATCH-B-extended-protocol-streaming.md` | Extended protocol + streaming relay | High | solo | A |
| C | `BATCH-C-pool-data-path.md` | Pool on the data path | High | solo | A, B |
| D | `BATCH-D-plugin-runtime.md` | WASM runtime perf | Medium | parallel-ok | (hook sites after A) |
| E | `BATCH-E-verify-then-fix.md` | Verify-then-fix backlog (32 items) | Low–Med | parallel by group | server.rs items after A–C |
| F | `BATCH-F-table-stakes-features.md` | TLS/mTLS, SCRAM, cancel, prepared stmts | Med–High | parallel by item | F4 after B |
| G | `BATCH-G-differentiator-features.md` | MCP, mirroring, HTTP gateway, branches… | Varies | parallel by item | — |
| G2 | `BATCH-G2-pg-to-nano-migration-mirror.md` | Continuous PG→HeliosDB-Nano migration mirror (user-requested) | XL | solo | B, C, G |
| H | `BATCH-H-ops-dx-quick-wins.md` | Reload, plugin registry | Low | parallel | — |

## Execution protocol (per batch) — 2026-06-13 revision

1. Read the batch file fully; read `SUBSYSTEM-MAPS.md` for the relevant subsystem.
2. Re-verify each finding's evidence against current code (line numbers drift after each batch).
3. Implement; keep each work item an independently revertable commit.
4. Run `cargo test` (unit/integration regression) — must stay green.
5. Run the **live regression battery** against BOTH backends:
   `BK=pg ./scripts/regress/run.sh <binary>` and `BK=nano ./scripts/regress/run.sh <binary>`
   (PostgreSQL 18.4 on :25433, HeliosDB-Nano 3.57 on 100.64.0.2:54320). No regression vs prior batch.
6. If a regression appears, fix it before moving on. Then continue to the next batch.
7. Release (crates.io publish via tag) is an explicit, outward-facing gate — commit locally per
   green batch; publish only on user confirmation.

## Sequencing for the 2026-06 implementation pass

A ✅ → B → C → D → E → F → G → G2 → H. Each gated by `cargo test` + the live battery on PG 18.4 and Nano.
