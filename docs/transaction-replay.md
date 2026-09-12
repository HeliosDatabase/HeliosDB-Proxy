# Transaction Replay (TR) — Deep Dive

> **How to read this guide.** [In-session replay](#in-session-replay-the-core-path)
> describes the recovery path a live client actually takes. Transaction Replay ships in
> the default build; `tr_enabled = false` (or `--tr=false`) turns it off at runtime.
> Everything about the transaction journal, the replay engine and time-travel replay
> describes the operator tooling behind `POST /api/replay`, which is not on that
> recovery path. The [2026-09 audit](internal/audit-2026-09/README.md) records what was
> fixed and what remains.

Transaction Replay is HeliosProxy's failover-continuity subsystem: a per-write
transaction journal plus a replay engine that can re-execute journaled statements on a
new backend after a primary change, so that a failover looks to the client like a slow
query rather than a dropped connection.

This document is grounded in the code. Every concrete claim below — config key, default,
mode name, behavior — is verifiable in `src/transaction_journal.rs`,
`src/failover_replay.rs`, `src/failover_controller.rs`, `src/switchover_buffer.rs`,
`src/replay/mod.rs`, and the TR fields of `ProxyConfig` in `src/config.rs`. Where the
narrative describes intent rather than shipped runtime behavior, it says so explicitly.

**Last verified against the post-1.7.0 tree that moved TR into the default build.** The
in-session recovery path described in [In-session replay](#in-session-replay-the-core-path)
is the current behavior; the journal and administrative replay sections below describe
the operator tooling, which is compiled into every build and disabled at runtime by
`tr_enabled = false`.

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

HeliosProxy's Transaction Replay is aimed at the same problem space for PostgreSQL-wire
backends, built on an in-memory journal rather than a driver-side capture buffer.

---

## Build and Runtime Gating

Transaction Replay is **in the default build** — there is no cargo feature to enable.
`ha-tr` is retained as a deprecated no-op (`Cargo.toml`: `ha-tr = []`) so downstream
feature mappings keep resolving, but it gates nothing.

`tr_enabled` is the runtime master switch (`proxy.toml`, or `--tr` on the CLI). Setting
it to `false`:

- **stops write journaling** — the write-path `journal_write` hook is skipped, so no new
  entries are recorded;
- **disables in-session recovery** — `effective_tr_mode()` returns `TrMode::None`
  regardless of `tr_mode`, so a backend fault gets the plain one-error-then-close
  behavior instead of replay;
- **makes `POST /api/replay` return `503`** with
  `{"error": "transaction replay disabled (tr_enabled = false)"}`.

The modules (`transaction_journal`, `failover_replay`, `replay`, `cursor_restore`,
`session_migrate`, `upgrade_orchestrator`, `shadow_execute`), the `ServerState` journal
field and the `FailoverController` coordinated replay are always compiled. `POST
/api/shadow` stays available with `tr_enabled = false`: it is an explicit operator
validation tool, not part of failover recovery.

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
| `tr_enabled` | bool | `true` | Master switch for Transaction Replay: journaling, in-session replay (forces `tr_mode = none` when off) and `POST /api/replay`. Required in a config file (no serde default; the in-code `Default` is `true`). |
| `tr_mode` | enum | `session` | Selects the replay policy (see below). Stored on each session and surfaced at `/config`. Also required in a config file (no serde default — the `#[default]` on `TrMode` only feeds `ProxyConfig::default()`, so omitting `tr_mode` from `proxy.toml` fails deserialization with "missing field `tr_mode`"). |
| `write_timeout_secs` | u64 | `30` | `default_write_timeout_secs()` = 30. Exposed as `ProxyConfig::write_timeout()` → `Duration`. Since 1.7.0 it is **one deadline for the whole recovery** — waiting for a primary, connect/auth, session restore and replay all share it, rather than each having its own timeout. |
| `tr_read_functions` | list | `[]` | Extra function names to treat as side-effect-free when deciding whether an interrupted read may be re-executed. Plain identifiers only. |
| `[limits] tr_max_observation_bytes` | usize | `1048576` | Per-statement budget for the response digest used to verify a replay. A response beyond it makes the transaction non-replayable. |
| `[limits] tr_max_session_set_statements` | usize | see `configuration.md` | Cap on **distinct** tracked `SET` variables. Exceeding it refuses failover with `08006` rather than re-homing with incomplete state. |
| `[limits] max_backend_frame_bytes` | usize | `104857600` | Ceiling on any single backend protocol frame on every streaming relay. |
| `[limits] backend_response_timeout_secs` | u64 | `0` (off) | Whole-response deadline; bounds a backend that drips bytes inside one response, which the per-read timeout cannot. |

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

**`tr_mode` is live.** Since 1.6.0 the mode is enforced on the session's own recovery
path, not only reported: `none` aborts, `session` re-establishes the connection and its
tracked session state, `select` additionally re-executes an interrupted read when that
read is provably side-effect-free, and `transaction` replays the recorded transaction.
The modes are cumulative — each does everything the one before it does.

---

## In-session replay (the core path)

This is what a client actually experiences when its backend dies mid-session. It is
independent of the journal and administrative replay modules documented further down;
both ship in the default build and both are turned off together by `tr_enabled = false`.

**What is recorded.** While a session is inside a transaction under `tr_mode = select` or
`transaction`, the proxy records each statement, the session state it changed, and a
bounded digest of the response frames the client was shown (`[limits]
tr_max_observation_bytes`, default 1 MiB). Autocommit traffic records nothing.

**What happens on a backend fault.** The proxy classifies how far the statement got. If
the outcome is *not delivered*, recovery is safe. If the outcome is *unknown* — the
statement may have committed — it is never re-executed; the client receives `08007` with
instructions to verify. Once any part of a response has reached the client, the session is
closed rather than have a second result appended to the first.

**What recovery does.** Under one deadline (`write_timeout_secs`) it waits for a healthy
primary, connects and authenticates, restores the session's tracked `SET` state, and
replays the recorded transaction. Each replayed statement's response is hashed and
compared with what the client originally saw; any divergence rolls the replay back and
returns `40001` rather than continuing on top of rows the client never observed.

**What it refuses.** Recovery is declined, conservatively, when:

- the transaction ran at `SERIALIZABLE` or `REPEATABLE READ` — no replay can reproduce
  that snapshot;
- a response was larger than `tr_max_observation_bytes`, so it cannot be verified;
- a read calls anything that is not a known side-effect-free built-in and is not listed in
  `tr_read_functions` (a user-defined function, `nextval`, `pg_notify`, `set_config`, an
  advisory lock);
- the number of distinct tracked `SET` variables exceeded
  `[limits] tr_max_session_set_statements`, so the session cannot be restored completely
  (`08006`);
- a commit boundary cannot be established lexically, or an `Execute` refers to a `Parse`
  or `Bind` from an earlier completed protocol cycle.

---

## What happens on the live write path (the journal hook)

Beyond the in-session recovery above, two mechanisms observe a primary change
in the running daemon:

**1. Write journaling** (`src/server.rs`, `journal_write`). When `tr_enabled` and the
statement is a write, the proxy records it in the shared
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
   literal; `Array` is not yet supported and degrades to `NULL`). That `NULL` surfaces as
   a `rows_matched = false` mismatch **only** for entries that captured a `rows_affected`
   count; hot-path journals record no count (`rows_affected = None`), so there the
   degradation is silent.
4. **Verification**: `rows_affected` is compared against the recorded count when one was
   captured. Checksum matching is best-effort — the engine does not recompute a
   server-side hash, so an entry with no recorded checksum counts as matched.
5. **Retry**: on failure, up to `max_retries` with a 100 ms pause.

`FailoverReplay` keeps `active_replays`, a bounded `completed_replays` history (last 100),
and exposes `get_state`, `get_progress`, `cancel_replay`, `history`, and `stats`.

> **Skeleton path.** When no backend template/endpoint is attached to the
> `FailoverReplay`, the backend-touching calls short-circuit to success without opening a
> connection (`execute_statement` returns `(true, true, true, None)`). Real replay
> requires `with_backend_template` plus `register_endpoint`. Nothing in the daemon calls
> those, so this is not merely the unit-test configuration — it is the only configuration
> reachable through `coordinate_failover_replay` (see
> [Failover Coordination](#3-failover-coordination)).

### 3. Failover Coordination

`src/failover_controller.rs`. `FailoverController` is the orchestration layer.
`FailoverConfig` defaults: `detection_time = 10 s`, `failover_timeout = 60 s`,
`auto_failover = true`, `prefer_sync_standby = true`, `max_lag_bytes = 16 MiB`,
`retry_failed = true`, `max_retries = 3`.

- **Candidate selection** (`select_best_candidate`): sort standbys by sync status (sync
  preferred when `prefer_sync_standby`), then by replication lag, then by priority.
- **Sync wait** (`wait_for_sync`): poll `pg_last_wal_replay_lsn()` at 200 ms cadence; the
  same LSN must be observed across three consecutive polls (`stable_polls >= 2`, i.e. two
  repeat observations) before the standby is treated as "caught up as far as it can" (the
  dead primary is producing no new WAL). Bounded by `failover_timeout`.
- **Promotion** (`promote_standby`): `SELECT pg_promote(true, N)` with `N` clamped to
  10–300 s, then verify on a fresh connection that `pg_is_in_recovery()` is now `false`.
- **Split-brain guard** (`on_old_primary_recovered`): deliberately read-only. PostgreSQL
  has no in-place "demote" — rejoining a recovered old primary needs `pg_rewind` /
  `pg_basebackup` out of band — so the controller only probes and emits
  `OldPrimaryRecovered`, logging loudly if the recovered node still reports itself primary.

**Coordinated replay** (`coordinate_failover_replay`): collect the failed node's
active transactions (`get_transactions_for_node`), compute their maximum `start_lsn`,
`wait_for_lsn_catchup` on the new primary, then run `FailoverReplay` (with
`wait_for_wal_sync = false`, since the wait already happened) over each transaction. The
result is a `CoordinatedReplayResult` with `total_transactions`, `successful_replays`,
`failed_replays`, per-transaction `ReplayResult`s, and `all_successful()` / `success_rate()`
helpers.

> **What this actually does today.** `coordinate_failover_replay` builds its
> `FailoverReplay` via `FailoverReplay::new(ReplayConfig { .. })` and never calls
> `with_backend_template` or `register_endpoint` on it — there is no API to attach a
> backend to the instance it constructs. Every statement therefore takes the skeleton
> path above (`execute_statement` returns `(true, true, true, None)` without opening a
> connection), so **no statement is executed against the new primary** and each
> per-transaction result reports success unconditionally. Likewise `wait_for_lsn_catchup`
> does **not** query `pg_last_wal_replay_lsn()`; it polls the tracked candidate's
> in-memory `lag_bytes` (code comment: "In a real implementation, we'd query the node's
> current LSN") and returns immediately when `target_lsn == 0` — always the case for
> hot-path journals, since `journal_write` records `start_lsn = 0`. Coordinated replay is
> wired end-to-end, but its executing half is a no-op until a backend is attached to the
> `FailoverReplay` it creates.

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

Like the journal it reads from, this endpoint is controlled by `tr_enabled`: with
`tr_enabled = false` the route still exists, but the handler returns
`503 {"error": "transaction replay disabled (tr_enabled = false)"}` (`src/admin.rs`).

---

## Limitations and Trade-offs

### Journal is in-memory and best-effort on the hot path
The journal lives in process memory and is bounded by the code constants above; it is not
persisted. The live write-path hook records SQL text only — parameter values and row
counts are part of the journal model but are not populated by the forwarding path today.
This applies to the write journal, **not** to in-session recovery: that path records its
own per-statement response digest and verifies it during replay (see
[In-session replay](#in-session-replay-the-core-path)).

### Session state: `SET` is restored, the rest is not
The in-session path tracks `SET`/`RESET` transactionally — a `SET` inside a transaction is
restored only if that transaction commits, `ROLLBACK TO SAVEPOINT` discards the ones made
after the savepoint — and replays the resulting state onto the replacement backend, up to
`[limits] tr_max_session_set_statements` distinct variables. Beyond that cap the failover
is refused (`08006`) rather than completed with incomplete state.

Not restored: named `PREPARE`d statements, cursor positions, session temp tables and
advisory locks. `SET`s issued through the extended protocol are also not tracked. The
library modules `src/cursor_restore.rs` and `src/session_migrate.rs` implement
parts of this but remain unwired from the recovery path. Applications depending on that
state surviving a failover still need application-level coordination.

### Snapshot-pinned transactions are not replayed
A transaction at `SERIALIZABLE` or `REPEATABLE READ` — set on `BEGIN`, through
`SET TRANSACTION`, or inherited from `default_transaction_isolation` — is marked
non-replayable. Its reads were taken against a snapshot that no replacement backend can
reproduce, so recovery reports the failure instead of silently continuing against a
different snapshot. The same applies to a response too large to have been digested within
`[limits] tr_max_observation_bytes`.

### Non-deterministic functions and sequences
`random()`, `clock_timestamp()`, `txid_current()`, and `nextval()` produce different
values on replay. Row-count verification will flag DML whose affected-row count diverges;
sequence values are not synchronized. This is inherent to statement-level replay.

### Array parameters
`JournalValue::Array` is not yet supported by `journal_value_to_param` and degrades to
`NULL`. The verification path surfaces that as a row-count mismatch only when the journal
entry captured a `rows_affected` count; for the hot-path journals produced by the live
write path (which record no counts) the degradation is silent.

### Coordination is a library capability, not an auto-wired daemon loop
`FailoverController`, `PrimaryTracker`, and `SwitchoverBuffer` are unit-tested library
components intended for embedded/programmatic use (and for the HeliosDB-workspace build).
The standalone daemon's failover handling is the health-driven
`select_primary_with_timeout` write-buffering described above; it does not drive the
controller/tracker automatically.

---

## Comparison

| Capability | HeliosProxy TR | Oracle TAF | Oracle TAC | PgBouncer |
|---|---|---|---|---|
| Failover write-buffering (bounded by `write_timeout_secs`) | Yes | N/A | N/A | No |
| DML statement replay (journal-driven) | Yes (replay engine) | No | Yes | No |
| SELECT re-execution | Yes (`skip_read_only = false`) | Yes (read-only) | Yes | No |
| WAL-LSN wait before replay | Yes (`pg_last_wal_replay_lsn()`) | N/A | Yes | No |
| Result verification on replay | Ordered response digest, verified; divergence returns `40001` | Basic | Full | N/A |
| Session-state migration | `SET` state restored transactionally; cursors/`PREPARE`/temp tables not | Yes | Yes | No |
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
