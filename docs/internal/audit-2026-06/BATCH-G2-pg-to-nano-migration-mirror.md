# BATCH G2 — Continuous PostgreSQL→HeliosDB-Nano Migration Mirror

> Queued by user request (2026-06-13), to execute **after BATCH G**. This is a
> concrete, heterogeneous-migration specialization of BATCH-G's "Continuous
> Traffic Mirroring + Blue/Green Cutover GA" (judge rank #6). Where BATCH-G
> mirrors PG→PG for blue/green, **G2 mirrors a PostgreSQL primary into a
> HeliosDB-Nano secondary so a migration target is always warm, verified, and
> ready to cut over at any moment.**

**Goal:** Turn HeliosProxy into a live PG→Nano migration appliance: every write
that lands on the PostgreSQL primary is continuously replayed into a HeliosDB-Nano
instance, with always-on result diffing proving the Nano copy matches, and a
one-call cutover that promotes Nano to primary. "Migration is ready at any time"
becomes a literal, observable proxy state — not a maintenance-window project.

**Effort:** XL. **Parallel-execution compatibility:** SOLO (touches the data
path + replay + shadow + orchestrator). **Depends on:** BATCH B (extended-protocol
relay — the journal must capture Parse/Bind/Execute, not just simple Query),
BATCH C (pooled, proxy-authenticated backend connections — the mirror writer
needs its own pooled Nano connections), and ideally BATCH-G's mirroring core.

---

## Why this is mostly assembly, not invention

The deep audit confirmed the primitives already exist in-tree but are **unwired**:

1. **Time-travel replay** (`src/replay/`, `POST /api/replay`) replays a window of
   the transaction journal onto a target backend and already supports
   `target_user` / `target_database` overrides — so it can re-run captured
   statements against a *different* backend (Nano). **Gap:** it is window-based and
   on-demand, and the audit found the **transaction journal is never populated from
   the live data path** (`begin_transaction`/`log_statement` have no non-test
   callers). The journal it would replay from is empty today.
2. **Shadow execution** (`src/shadow_execute/`, `POST /api/shadow`) runs a query
   against two backends in parallel and diffs results with order-independent
   row-hashing — exactly the "is the secondary correct?" verification half.
3. **Upgrade orchestrator** (`src/upgrade_orchestrator/`) is a 6-state blue/green
   cutover state machine (stand-up → shadow-verify → cutover → drain) — but the
   audit confirmed **its transitions are stubs that log and advance**.

So G2 = wire the journal to the data path (a hard dependency it shares with
HA-TR), add a continuous replay *tail* (vs on-demand windows) targeting Nano,
fold shadow-diff in as the always-on correctness monitor, and make the
orchestrator's transitions real for the PG→Nano cutover.

## Heterogeneous-specific challenges (the real work beyond BATCH-G)

PG→Nano is **not** PG→PG. The mirror must bridge dialect/semantic gaps:

- **SQL dialect translation layer.** PG-specific DDL/types/functions
  (`SERIAL`, `JSONB`, `gen_random_uuid()`, arrays, `ON CONFLICT`, `RETURNING`,
  `COPY`) must be rewritten to Nano's accepted grammar. Reuse `src/rewriter/`
  (rules engine) as the translation stage; ship a `pg-to-nano` rule pack.
  Statements that cannot be faithfully translated are logged to a
  **migration-gap report** rather than silently dropped (no silent caps).
- **Type fidelity.** Map PG type OIDs → Nano types; verify via shadow-diff that
  round-tripped values match (the diff already row-hashes, so mismatches surface).
- **Write-only replay.** Mirror only state-changing statements
  (INSERT/UPDATE/DELETE/DDL/COPY) — reads stay on PG. The Batch-A
  `is_write_query` classifier already identifies these allocation-free.
- **Ordering & transactions.** Replay must preserve per-connection statement
  order and transaction boundaries (BEGIN/COMMIT/ROLLBACK, savepoints). The
  journal already records `TransactionEvent`; the tail must apply them in order
  per source transaction, serialized into Nano.
- **Backfill + tail handoff.** Initial bulk copy of existing data (snapshot via
  `COPY`/`pg_dump`-style export → Nano import), then switch to the live tail at
  the snapshot LSN/journal offset with no gap and no double-apply (idempotency
  fence on journal sequence number).
- **Lag & health surfacing.** Expose mirror lag (journal offset applied vs
  produced), last-verified-diff timestamp, and a `migration_ready` boolean on
  `/api/migration/status` and Prometheus.

## Deliverables

1. **Journal wiring (shared with HA-TR):** populate `TransactionJournal` from
   `route_and_forward` on every write statement (post-Batch-B, so extended-protocol
   writes are captured as their reconstructed SQL or Parse+Bind params).
2. **Mirror writer:** a background task that tails the journal and applies
   translated write statements to Nano over a dedicated pooled connection set,
   preserving per-source-connection order and transaction framing.
3. **`pg-to-nano` rewriter rule pack** + a typed dialect/type map, with an
   untranslatable-statement gap report.
4. **Always-on shadow-diff monitor:** sample N% of reads (or periodic canary
   queries) executed against both PG and Nano, row-hashed; expose drift count.
5. **Snapshot+tail bootstrap:** `POST /api/migration/start` does snapshot backfill
   then attaches the tail at the correct fence.
6. **Cutover:** make the orchestrator's transitions real — `POST /api/migration/cutover`
   drains PG writes via `SwitchoverBuffer`, flushes the mirror tail to zero lag,
   verifies a clean shadow-diff, then promotes Nano to primary in the routing table.
7. **Status surface:** `GET /api/migration/status` → `{state, lag, last_diff_ok,
   migration_ready, gaps[]}`; admin UI panel; Prometheus metrics.

## Acceptance criteria

- Writes (INSERT/UPDATE/DELETE/DDL) issued through the proxy to PG 18.4 appear in
  Nano within a bounded lag; `migration_ready` flips true once lag≈0 and the
  latest shadow-diff is clean.
- A representative PG schema (serial PKs, JSONB, arrays, `ON CONFLICT`,
  `RETURNING`) replicates into Nano with a row-hash-clean shadow-diff, or every
  unsupported construct appears in the gap report (nothing silently dropped).
- Snapshot+tail bootstrap of a non-empty PG database yields a Nano copy whose
  full-table row-hashes match PG (validated by `/api/shadow` over each table).
- `POST /api/migration/cutover` promotes Nano with zero lost writes (verified by a
  continuous writer during cutover) and clients see no dropped connections
  (reuses `SwitchoverBuffer` + Batch-C pooling).
- Regression battery (live, PG 18.4 + Nano) stays green; new migration tests added.

## Regression / live tests to add

- `migration_write_propagation` — write through proxy to PG, assert it lands in Nano.
- `migration_txn_atomicity` — a rolled-back PG txn must NOT appear in Nano.
- `migration_dialect_pack` — each `pg-to-nano` rule verified by shadow-diff or gap report.
- `migration_snapshot_tail` — bootstrap then continuous writes, full-table hash match.
- `migration_cutover_zero_loss` — concurrent writer across cutover, no lost/dup rows.
