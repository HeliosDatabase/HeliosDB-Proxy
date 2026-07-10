# Transaction Replay (TR) — Deep Dive

Transaction Replay is HeliosProxy's failover-continuity subsystem: a per-write
transaction journal plus a replay engine that can re-execute journaled statements on a
new backend after a primary change, so that a failover looks to the client like a slow
query rather than a dropped connection.

This document is grounded in the code. Every concrete claim below — feature flag, config
key, default, mode name, behavior — is verifiable in `src/transaction_journal.rs`,
`src/failover_replay.rs`, `src/failover_controller.rs`, `src/switchover_buffer.rs`,
`src/replay/mod.rs`, and the TR fields of `ProxyConfig` in `src/config.rs`. Where the
narrative describes intent rather than shipped runtime behavior, it says so explicitly.

**Last verified against commit `9c5ff9b`.**

---

## Why Transaction Replay Matters

In a plain PostgreSQL HA setup, when the primary fails:

1. The connection pool detects the failure and drops active connections.
2. In-flight transactions receive an error (connection reset, server closed the
   connection).
3. The application must detect the error, reconnect, and retry the transaction from
   scratch.
4. Many applications do not implement retry logic correctly (or at all).

Transaction Replay targets that failure mode. The proxy journals every write, and after a
primary change the replay engine can re-apply the journaled statements against the new
primary. Where full replay is not in play, the proxy still buffers writes for a bounded
window (`write_timeout_secs`) so that a fast failover resumes writes without surfacing an
error to the client.

### Industry Comparison

Oracle Database has offered comparable capabilities for years:

- **Oracle TAF (Transparent Application Failover)** — reconnects sessions and optionally
  re-executes SELECT statements after failover, but does not replay DML transactions.
- **Oracle TAC (Transparent Application Continuity)** — full transaction replay including
  DML, introduced in Oracle 12c.

HeliosProxy's `ha-tr` feature is aimed at the same problem space for PostgreSQL-wire
backends, built on an in-memory journal rather than a driver-side capture buffer.

---

## Feature Gating

Transaction Replay lives behind the **`ha-tr`** cargo feature
(`Cargo.toml`: `ha-tr = []`; described there as "Transaction Replay (TR) — failover
replay, cursor restore, session migrate"). It is included in `all-features` and in the
CI feature matrix (`cargo test --features ha-tr`).

What the feature actually gates:

- The **write-path journaling hook** in `src/server.rs` (`if is_write &&
  config.tr_enabled { journal_write(...) }`) is `#[cfg(feature = "ha-tr")]`.
- The **coordinated post-failover replay** on `FailoverController`
  (`coordinate_failover_replay`, `wait_for_lsn_catchup`, `CoordinatedReplayResult`) is
  `#[cfg(feature = "ha-tr")]`.

The supporting types — `TransactionJournal`, `FailoverReplay`, `FailoverController`,
`SwitchoverBuffer` — are compiled unconditionally; only the journaling hook and the
coordinated-replay orchestration are behind the flag.

---

## Configuration

Transaction Replay is configured through three top-level `proxy.toml` keys, all fields of
`ProxyConfig` in `src/config.rs`:

```toml
# Enable Transaction Replay journaling / failover write-buffering.
tr_enabled = true

# TR policy: "none" | "session" | "select" | "transaction"
tr_mode = "session"

# Seconds to wait for a healthy primary during failover before a write
# returns an error. Also the ceiling on the write-buffering window.
write_timeout_secs = 30
```

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `tr_enabled` | bool | `true` | Enables the write-path journaling hook. Required in a config file (no serde default; the in-code `Default` is `true`). |
| `tr_mode` | enum | `session` | Selects the replay policy (see below). Stored on each session and surfaced at `/config`. |
| `write_timeout_secs` | u64 | `30` | `default_write_timeout_secs()` = 30. Exposed as `ProxyConfig::write_timeout()` → `Duration`; consumed by `select_primary_with_timeout` (`src/server.rs`). |

> **No `tr_max_journal_bytes` or `switchover_drain_timeout_secs` key exists.** Earlier
> revisions of this document invented both. Journal size caps are code constants (see
> [Transaction Journal](#1-transaction-journal)); the SIGUSR2 drain window is
> `shutdown_drain_timeout_secs`, a separate binary-handoff setting documented in
> [configuration.md](configuration.md).

### TR Modes (`tr_mode`)

`TrMode` (`src/config.rs`) is a four-variant enum; the doc-comments on each variant are
the authoritative one-line semantics:

| Mode | Behavior (from `TrMode`) |
|------|--------------------------|
| `none` | No transaction replay. In-flight transactions are aborted on failover. |
| `session` | Re-establish session only. *(Default.)* |
| `select` | Re-execute SELECT queries. |
| `transaction` | Full transaction replay. |

**Honest caveat on `tr_mode`.** As of `9c5ff9b`, `tr_mode` is parsed, stored on the
`ClientSession` (`session.tr_mode = config.tr_mode`), and reported through `/config`, but
the live write-path journaling branches only on `tr_enabled` — it does not yet select
different journaling behavior per mode. Mode-specific replay (session-only vs. re-run
SELECTs vs. full DML replay) is expressed by the **replay engine's** `ReplayConfig`
(e.g. `skip_read_only`), not by a `tr_mode` switch in the server hot path. Treat `tr_mode`
as the declared policy that the replay/coordination layer honors, not as a runtime toggle
on the forwarding path.

---

## What Happens on the Live Write Path

Two mechanisms cover a primary change in the running daemon:

**1. Write journaling** (`src/server.rs`, `journal_write`, `#[cfg(feature = "ha-tr")]`).
When `tr_enabled` and the statement is a write, the proxy records it in the shared
`TransactionJournal`. Each write is journaled as its own auto-commit transaction — a fresh
`tx_id`, `begin_transaction` then `log_statement`, never committed. Because these journals
never commit, the journal manager bounds the map with a global cap and evicts the oldest
entries (see below). The live hook records the **SQL text**; the parameter, checksum, and
row-count fields of a journal entry exist in the data model but are passed as
`None`/empty by this hook today.

**2. Failover write-buffering** (`src/server.rs`, `select_primary_with_timeout`).
When a write needs the primary and the configured-primary node is not healthy, the proxy
does **not** immediately error. It polls node health every 100 ms for up to
`write_timeout_secs`, and as soon as a node with `role = "primary"` is enabled and healthy,
the write proceeds against it. If the window elapses with no healthy primary, the proxy
increments the `failovers` metric and returns `NoHealthyNodes`.

In the standalone daemon the "current primary" is the configured `[[nodes]]` entry whose
`role = "primary"` and whose health check is passing (see
[topology-providers.md](topology-providers.md) for how the primary is determined and how
`/topology` reports it). The `FailoverController` and `PrimaryTracker` types are library
components (exercised by tests, available for embedded/programmatic use) and are not wired
into the daemon's forwarding loop.

---

## Components

### 1. Transaction Journal

`src/transaction_journal.rs`. An in-memory, per-transaction log of statements.

A `TransactionJournalEntry` holds: `tx_id`, `session_id`, `node_id`, `started_at`,
`start_lsn` (for WAL anchoring), the ordered `entries`, `current_sequence`, `active`,
`has_mutations`, and `savepoints`. Each `JournalEntry` captures:

- `sequence` — monotonically increasing within the transaction.
- `statement` — the SQL text.
- `parameters: Vec<JournalValue>` — `Null` / `Bool` / `Int64` / `Float64` / `Text` /
  `Bytes` / `Array`.
- `result_checksum: Option<u64>` and `rows_affected: Option<u64>` — for verification.
- `timestamp`, `statement_type`, `duration_ms`.

`StatementType::from_sql` classifies by leading keyword into `Select`, `Insert`, `Update`,
`Delete`, `Ddl`, `Transaction` (BEGIN/COMMIT/ROLLBACK/SAVEPOINT), `Set`, or `Other`.
`is_read_only()` is true only for `Select`; `is_mutation()` covers Insert/Update/Delete/Ddl.

**Savepoints.** `create_savepoint` records the sequence at the savepoint;
`rollback_to_savepoint` truncates journal entries after that sequence and drops later
savepoints — so a replay reflects the post-rollback statement set.

**Memory bounds (code constants, not `proxy.toml` keys).** `TransactionJournal::new()`
sets `max_entries = 10_000`, `max_size = 64 MiB` per journal, and a global
`max_journals = 50_000`. `log_statement` rejects entries past `max_entries` or `max_size`.
When the global cap is reached, `begin_transaction` evicts the oldest journals (by
`started_at`) down to 90% of the cap in one pass — this is the leak guard for the
never-committed auto-commit journals produced by the write path. `commit_transaction` /
`rollback_transaction` remove a transaction's journal immediately.

### 2. Replay Engine

`src/failover_replay.rs`. `FailoverReplay` drives replay of a `TransactionJournalEntry`
against a target node, governed by `ReplayConfig`:

| `ReplayConfig` field | Default | Effect |
|----------------------|---------|--------|
| `verify_results` | `true` | Compare replay outcome to recorded metadata. |
| `statement_timeout_ms` | `30000` | Per-statement / WAL-wait timeout bound. |
| `retry_on_error` | `true` | Retry a failed statement. |
| `max_retries` | `3` | Retry ceiling (100 ms backoff between tries). |
| `skip_read_only` | `false` | Skip `SELECT` entries during replay. |
| `wait_for_wal_sync` | `true` | Wait for the target's WAL to reach `start_lsn` first. |
| `max_wal_lag_bytes` | `0` | `0` = wait for full sync. |

Replay proceeds through the `ReplayState` machine — `Pending` → `WaitingForWal` →
`Replaying` → (`Completed` | `Failed`):

1. **WAL wait** (if `wait_for_wal_sync`): connect to the target and poll
   `SELECT pg_last_wal_replay_lsn()::text` every 200 ms until it is `>= start_lsn`,
   bounded by `statement_timeout_ms`. LSNs are parsed from PG's `hi/lo` hex text form via
   `pg_lsn_to_u64`.
2. **Statement replay**, in strict `sequence` order. Read-only statements are skipped when
   `skip_read_only` is set; `Transaction`-control statements are always skipped (BEGIN/
   COMMIT/ROLLBACK are handled by the surrounding flow, not replayed verbatim).
3. **Execution** via `crate::backend::BackendClient` — `simple_query` when there are no
   parameters, `query_with_params` otherwise. Parameters are converted by
   `journal_value_to_param` into **text-format** `ParamValue`s (`Bytes` → a `\x…` hex
   literal; `Array` is not yet supported and degrades to `NULL`, which surfaces as a
   `rows_matched = false` mismatch).
4. **Verification**: `rows_affected` is compared against the recorded count when one was
   captured. Checksum matching is best-effort — the engine does not recompute a
   server-side hash, so an entry with no recorded checksum counts as matched.
5. **Retry**: on failure, up to `max_retries` with a 100 ms pause.

`FailoverReplay` keeps `active_replays`, a bounded `completed_replays` history (last 100),
and exposes `get_state`, `get_progress`, `cancel_replay`, `history`, and `stats`.

> **Skeleton path.** When no backend template/endpoint is attached to the
> `FailoverReplay` (the unit-test configuration), the backend-touching calls short-circuit
> to success without opening a connection. Real replay requires `with_backend_template`
> plus `register_endpoint`.

### 3. Failover Coordination

`src/failover_controller.rs`. `FailoverController` is the orchestration layer.
`FailoverConfig` defaults: `detection_time = 10 s`, `failover_timeout = 60 s`,
`auto_failover = true`, `prefer_sync_standby = true`, `max_lag_bytes = 16 MiB`,
`retry_failed = true`, `max_retries = 3`.

- **Candidate selection** (`select_best_candidate`): sort standbys by sync status (sync
  preferred when `prefer_sync_standby`), then by replication lag, then by priority.
- **Sync wait** (`wait_for_sync`): poll `pg_last_wal_replay_lsn()` at 200 ms cadence; two
  consecutive equal LSNs mean "caught up as far as it can" (the dead primary is producing
  no new WAL). Bounded by `failover_timeout`.
- **Promotion** (`promote_standby`): `SELECT pg_promote(true, N)` with `N` clamped to
  10–300 s, then verify on a fresh connection that `pg_is_in_recovery()` is now `false`.
- **Split-brain guard** (`on_old_primary_recovered`): deliberately read-only. PostgreSQL
  has no in-place "demote" — rejoining a recovered old primary needs `pg_rewind` /
  `pg_basebackup` out of band — so the controller only probes and emits
  `OldPrimaryRecovered`, logging loudly if the recovered node still reports itself primary.

**Coordinated replay** (`coordinate_failover_replay`, `ha-tr`): collect the failed node's
active transactions (`get_transactions_for_node`), compute their maximum `start_lsn`,
`wait_for_lsn_catchup` on the new primary, then run `FailoverReplay` (with
`wait_for_wal_sync = false`, since the wait already happened) over each transaction. The
result is a `CoordinatedReplayResult` with `total_transactions`, `successful_replays`,
`failed_replays`, per-transaction `ReplayResult`s, and `all_successful()` / `success_rate()`
helpers.

### 4. Switchover Buffer

`src/switchover_buffer.rs`. For a **planned** switchover, `SwitchoverBuffer` queues write
queries during the brief promotion window and replays them to the new primary once it is
ready. `BufferConfig` defaults: `buffer_timeout = 5 s`, `max_buffered_queries = 10_000`,
`max_buffer_memory = 100 MiB`, `allow_queries_during_drain = true`. It moves through
`Passthrough` → `Buffering` → `Draining`; queries that outlast `buffer_timeout` complete
with `BufferResult::Timeout` rather than blocking forever. Like the failover controller,
this is a library component with its own unit tests.

---

## Time-Travel Replay (related)

The same journal powers **time-travel replay** (`src/replay/mod.rs`), surfaced by the
admin endpoint `POST /api/replay`. Given a `[from, to]` timestamp window and a target
host/port, the engine pulls `entries_in_window` from the shared `TransactionJournal`,
sorts them by timestamp, and re-executes them against a target backend (typically staging)
via `BackendClient`, returning a `ReplaySummary` (`statements_replayed`, `failures`,
`elapsed_ms`, the window, and the first error). This is the "re-run yesterday 10:00–11:00
against staging" path, distinct from post-failover replay but built on the same journal.
See [admin-api.md](admin-api.md) for the request/response shape.

---

## Limitations and Trade-offs

### Journal is in-memory and best-effort on the hot path
The journal lives in process memory and is bounded by the code constants above; it is not
persisted. The live write-path hook records SQL text only — parameter values, result
checksums, and row counts are part of the journal model but are not populated by the
forwarding path today, so hot-path replay verification degrades to "did the statement
run" rather than "did it produce identical results".

### Session state is not migrated
The replay engine re-executes journaled SQL statements. It does **not** re-establish
`SET`/GUC parameters, re-`PREPARE` named statements, restore cursor positions, re-create
session temp tables, or re-acquire advisory locks on the new primary. (The `ha-tr` feature
line mentions "cursor restore, session migrate" as intent; those paths are not implemented
in `failover_replay.rs` as of `9c5ff9b`.) Applications that depend on pre-transaction
session state surviving a failover need application-level coordination.

### Non-deterministic functions and sequences
`random()`, `clock_timestamp()`, `txid_current()`, and `nextval()` produce different
values on replay. Row-count verification will flag DML whose affected-row count diverges;
sequence values are not synchronized. This is inherent to statement-level replay.

### Array parameters
`JournalValue::Array` is not yet supported by `journal_value_to_param` and degrades to
`NULL`, which the verification path surfaces as a row-count mismatch.

### Coordination is a library capability, not an auto-wired daemon loop
`FailoverController`, `PrimaryTracker`, and `SwitchoverBuffer` are unit-tested library
components intended for embedded/programmatic use (and for the HeliosDB-workspace build).
The standalone daemon's failover handling is the health-driven
`select_primary_with_timeout` write-buffering described above; it does not drive the
controller/tracker automatically.

---

## Comparison

| Capability | HeliosProxy TR (`ha-tr`) | Oracle TAF | Oracle TAC | PgBouncer |
|---|---|---|---|---|
| Failover write-buffering (bounded by `write_timeout_secs`) | Yes | N/A | N/A | No |
| DML statement replay (journal-driven) | Yes (replay engine) | No | Yes | No |
| SELECT re-execution | Yes (`skip_read_only = false`) | Yes (read-only) | Yes | No |
| WAL-LSN wait before replay | Yes (`pg_last_wal_replay_lsn()`) | N/A | Yes | No |
| Row-count / checksum verification | Row count; checksum best-effort | Basic | Full | N/A |
| Cursor / session-state migration | Not implemented | Yes | Yes | No |
| Planned-switchover buffering | Yes (`SwitchoverBuffer`) | No | Yes | No |
| Persisted journal | No (in-memory) | N/A | N/A | N/A |
| Open source | Yes | No | No | Yes |
| Proxy-level (no app driver) | Yes | Requires OCI | Requires JDBC | Yes (no TR) |

---

## See Also

- [Configuration Reference](configuration.md) — the `tr_*` keys and `write_timeout_secs`.
- [Topology Providers](topology-providers.md) — how the current primary is determined.
- [Admin API Reference](admin-api.md) — `/topology`, `/api/replay`, `/api/chaos`.
- [Architecture](architecture.md) — system overview and module map.
</content>
</invoke>
