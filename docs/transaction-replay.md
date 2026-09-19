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

**Last verified against the post-1.8.0 tree with the TR-07 recovery-journal slice.** The
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

- **stops write journaling** — the write-path capture hooks are skipped, so no new
  transactions are recorded;
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

**1. Write journaling** (`src/server.rs` capture hooks + `src/journal_capture.rs`).
When `tr_enabled`, the proxy journals **real transactions** as the backend reports them
(TR-07). The forward path registers what it sends — a simple-query string, or the
`Parse`/`Bind`/`Execute`/`Close` messages of an extended batch including every bound
parameter value byte for byte with its text/binary format and the declared type OIDs —
and the relay observes what the backend answers: each `CommandComplete` tag,
`PortalSuspended`, `EmptyQueryResponse`, the first `ErrorResponse`, and the
`ReadyForQuery` status byte. A per-session state machine reconciles the two:

- `BEGIN` opens an active journal; each data-changing statement the backend completed is
  appended with its command tag (and the rows it reported); `SAVEPOINT` / `ROLLBACK TO`
  are applied structurally (a rollback to a savepoint truncates the entries after it);
  the transaction moves into **committed history** only when the backend's closing tag
  was `COMMIT` — a `COMMIT` in an aborted transaction answers `ROLLBACK` and is treated
  as one. Auto-commit statements and implicit multi-statement transactions become one
  committed transaction each.
- A statement the backend rejected is never journaled; a rolled-back transaction, a
  session that ends mid-transaction and a two-phase `PREPARE TRANSACTION` leave no
  committed history.
- Each committed transaction carries its source identity (client address, user,
  database, backend node, tenant) and a **global commit sequence** assigned in the order
  this proxy observed the commit responses.
- Reads are not journaled (a read with side effects — `nextval`, `set_config`, a volatile
  function — is not reproduced). A `COPY … FROM STDIN`, an `EXECUTE` of a session-scoped
  prepared statement, a statement over `[journal] max_statement_bytes` or a
  per-transaction cap marks the transaction *incomplete*: it stays in the journal, but
  committed-history replay refuses it rather than apply it partially.
- Only writes arm the relay's capture; an auto-commit read pays no capture work.

Committed transactions are kept in a bounded in-memory store and, when
`[journal] dir` is set, appended to a segmented on-disk journal and reloaded at startup
(see [Transaction Journal](#1-transaction-journal)). `POST /api/replay` reports what
backs every run in its `coverage` block.

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

`src/transaction_journal.rs`. The per-transaction log the capture feeds, plus the
committed store and the durable sink.

A `TransactionJournalEntry` holds: `tx_id`, `session_id`, `node_id` (derived from the
backend address), `started_at`, `start_lsn`, the ordered `entries`, `current_sequence`,
`active`, `has_mutations`, `savepoints`, the `source` identity, and — once committed —
`commit_seq`, `committed_at`, `commit_tag`, and `incomplete_reason` when some effect could
not be captured. Each `JournalEntry` captures:

- `sequence` — monotonically increasing within the transaction.
- `statement` — the SQL text (a simple-query string may hold several statements).
- `parameters: Vec<JournalValue>` — `Text` / `TextRaw` / `Binary` for captured `Bind`
  values (format preserved), plus `Null` / `Bool` / `Int64` / `Float64` / `Bytes` /
  `Array` for library callers; `param_types` — the OIDs declared in `Parse`.
- `outcome` — `Succeeded { tag }` / `Failed { sqlstate, message }` / `Unobserved`;
  `rows_affected` parsed from the tag; `protocol` (`Simple` / `Extended`).
- `result_checksum`, `timestamp`, `statement_type`, `duration_ms`.

`StatementType::from_sql` classifies by leading keyword into `Select`, `Insert`, `Update`,
`Delete`, `Ddl`, `Transaction` (BEGIN/COMMIT/ROLLBACK/SAVEPOINT), `Set`, or `Other`.

**Commit order.** `commit_transaction` moves an active journal into the committed store
and assigns the next `commit_seq`; `record_committed` does the same for an auto-commit
transaction; `rollback_transaction` drops it. `committed_in_window(from, to)` returns
committed transactions in commit order; `entries_in_window` (time-window replay) reads
the committed store's statements in timestamp order. Empty transactions (nothing to
replay) are not retained.

**Bounds (`[journal]`, see [configuration.md](configuration.md#recovery-journal-journal)).**
`max_active_transactions` caps open journals (oldest evicted), `max_entries_per_transaction`
/ `max_bytes_per_transaction` cap one transaction (beyond them it is marked incomplete),
`max_committed_transactions` / `max_committed_bytes` cap the committed store (oldest
evicted), `max_statement_bytes` caps one statement's captured text.

**Durability (`src/journal_store.rs`).** With `[journal] dir` set, every committed
transaction is handed to a `JournalSink` backed by a writer thread that appends
CRC-checked records to `journal-<first-commit-seq>.log` segments (rotated at
`segment_bytes`, oldest segments retired beyond `retain_bytes`), fsync'ed per the
`fsync` policy. The data path never waits on disk: the hand-off is a bounded queue, and
a full queue drops the record and counts it (`coverage.dropped_transactions`). At startup
the segments are read back, a torn or corrupt tail is truncated at the last intact record,
the newest `max_committed_transactions` are reloaded and numbering continues after the
highest recovered `commit_seq`. The record is written **after** the backend reported the
commit — the proxy is not a participant in the backend's commit — so a crash in between
loses that record: the store is a faithful log of what this proxy observed, not a WAL the
backend waits on (the D-02 boundary). The segments contain statement text and bound parameter
values in clear; protect the directory like the database's own data files.

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
3. **Execution** on **one connection inside one `BEGIN … COMMIT`** (TR-07): each entry
   goes through `BackendClient::execute_journaled`, the extended protocol with the
   declared type OIDs and every parameter re-sent in its captured text/binary format
   (typed library values are rendered as text; arrays as PostgreSQL array literals). The
   first failure — a backend error, or a `rows_affected` mismatch under `verify_results`
   — rolls the transaction back and stops; the target never holds a partial transaction.
4. **Verification**: `rows_affected` is compared against the recorded count when one was
   captured. Checksum matching is best-effort — the engine does not recompute a
   server-side hash, so an entry with no recorded checksum counts as matched.
5. **Retry**: `max_retries` (100 ms apart) applies to the connection attempt only; a
   statement cannot be retried inside an aborted transaction.

`FailoverReplay` keeps `active_replays`, a bounded `completed_replays` history (last 100),
and exposes `get_state`, `get_progress`, `cancel_replay`, `history`, and `stats`.

> **No-backend path.** Without an attached backend template/endpoint, the
> backend-touching calls now fail explicitly instead of reporting a synthetic success:
> `execute_statement` refuses when no template or endpoint is attached,
> `wait_for_wal_sync` refuses a nonzero LSN it cannot verify, and `/api/replay` rejects a
> blank target — so a coordinated replay reports `successful_replays: 0`. Real replay
> requires `with_backend_template` plus `register_endpoint` (see
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
> backend to the instance it constructs. Since TR-07 every statement therefore returns a
> **failure** ("no backend configured … refusing to report a replay that never executed")
> instead of the old synthetic success, so `successful_replays` is `0` and the coordinated
> result is honestly failed. Likewise `wait_for_lsn_catchup` does **not** query
> `pg_last_wal_replay_lsn()`; it polls the tracked candidate's in-memory `lag_bytes` (code
> comment: "In a real implementation, we'd query the node's current LSN") and returns
> immediately when `target_lsn == 0` — always the case for hot-path journals, since
> the capture records `start_lsn = 0`. Coordinated replay is wired end-to-end, but its
> executing half is inert until a backend is attached to the `FailoverReplay` it creates;
> until then it fails rather than claims success.

### 4. Switchover Buffer

`src/switchover_buffer.rs`. For a **planned** switchover, `SwitchoverBuffer` queues write
queries during the brief promotion window and replays them to the new primary once it is
ready. `BufferConfig` defaults: `buffer_timeout = 5 s`, `max_buffered_queries = 10_000`,
`max_buffer_memory = 100 MiB`, `allow_queries_during_drain = true`. It moves through
`Passthrough` → `Buffering` → `Draining`; queries that outlast `buffer_timeout` complete
with `BufferResult::Timeout` rather than blocking forever. Like the failover controller,
this is a library component with its own unit tests.

---

## Operator replay: time window and committed history

The same journal powers the admin endpoint `POST /api/replay` (`src/replay/mod.rs`), which
offers two modes (see [admin-api.md](admin-api.md#post-apireplay) for the request/response
shape):

- **`time_window`** (best effort): pulls the statements of every committed transaction in a
  `[from, to]` timestamp window (`entries_in_window`), flattens them across transactions in
  timestamp order and re-executes each independently on one target connection; failures
  are counted, not fatal (`partial: true`). "Re-run yesterday 10:00–11:00 against staging."
- **`committed_history`** (recovery grade, TR-07): selects the transactions whose *commit*
  was observed in the window (`committed_in_window`, optionally after a resume point
  `after_commit_seq`), and applies them in commit order, one at a time, each inside its own
  `BEGIN … COMMIT` on one connection, with every parameter re-sent in its captured format
  through the extended protocol. The first failure rolls that transaction back and stops
  the run (`partial: true`, `stopped_at`), so the target holds exactly the transactions
  up to `last_commit_seq` and nothing partial; a transaction marked incomplete is refused
  before it starts. The overall `[limits] replay_deadline_secs` is honoured between and
  inside transactions (a deadline mid-transaction rolls it back).

Both modes carry the `coverage` block (retained active/committed counts and bytes, the
caps, `commit_seq_high`, `dropped_transactions`, and the structural guarantees
`transaction_boundaries` / `parameter_values` / `outcomes` — now `true` — plus
`survives_restart`, `true` when `[journal] dir` is configured). Like the journal, the
endpoint is controlled by `tr_enabled`: with `tr_enabled = false` the handler returns
`503 {"error": "transaction replay disabled (tr_enabled = false)"}` (`src/admin.rs`).

---

## Limitations and Trade-offs

### What the journal does and does not capture
The journal records what the backend confirmed: completed data-changing statements with
their outcomes, real transaction boundaries, savepoint structure, bound parameter values
and a proxy-observed commit order. It does **not** record reads (so a read with side
effects — `nextval`, `set_config`, a volatile function — is not reproduced), the data of
a `COPY … FROM STDIN`, or the effect of an `EXECUTE` of a session-scoped prepared
statement; those mark the transaction incomplete and committed-history replay refuses it.
Sequence values, `now()`, `random()` and other non-deterministic expressions inside a
journaled statement produce fresh values on replay — replay re-executes SQL, it does not
copy rows.

**Retention is bounded and documented (TR-07).** Without `[journal] dir` the journal is
per-process memory bounded by `[journal] max_committed_*`: nothing survives a restart.
With `dir` set, committed transactions survive a restart up to `retain_bytes` on disk and
`max_committed_transactions` reloaded, subject to the fsync policy — a record is written
after the backend reported the commit, so a crash between the two loses that record.
The commit order is the order this proxy observed commits: a total order over everything
routed through it, not necessarily the backend's WAL order for commits that raced on
different sessions, and transactions that bypassed the proxy are not in it. This applies
to the write journal, **not** to in-session recovery: that path records its own
per-statement response digest and verifies it during replay (see
[In-session replay](#in-session-replay-the-core-path)).

**Time-window replay is not committed-history replay.** `time_window` flattens
interleaved transactions into timestamp order and continues past failures; it is a
best-effort tool for hydrating a staging copy. `committed_history` is the ledger-safe
mode: commit order, one transaction at a time, stop on first failure. Both say which one
they are (`mode`) and what backed them (`coverage`).

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

### Array and binary parameters
Captured `Bind` values are replayed in their original format, so arrays and
binary-encoded values reproduce exactly under `committed_history` and in the library
replay. Library-recorded `JournalValue::Array` values render as PostgreSQL array
literals. The text-interpolating `time_window` path cannot render a binary-format value
and sends `NULL` for it; use `committed_history` for parameterised extended-protocol
writes.

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
| Persisted journal | Optional (`[journal] dir`, segmented, CRC-checked) | N/A | N/A | N/A |
| Open source | Yes | No | No | Yes |
| Proxy-level (no app driver) | Yes | Requires OCI | Requires JDBC | Yes (no TR) |

---

## See Also

- [Configuration Reference](configuration.md) — the `tr_*` keys and `write_timeout_secs`.
- [Topology Providers](topology-providers.md) — how the current primary is determined.
- [Admin API Reference](admin-api.md) — `/topology`, `/api/replay`, `/api/chaos`.
- [Architecture](architecture.md) — system overview and module map.
