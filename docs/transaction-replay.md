# Transaction Replay (TR) — Deep Dive

Transaction Replay is HeliosProxy's flagship feature: the ability to transparently replay in-flight transactions on a new primary after a failover, so that client applications experience zero errors and zero data loss during both planned switchovers and unplanned failures.

## Why Transaction Replay Matters

In a traditional PostgreSQL HA setup, when the primary fails:

1. The connection pool detects the failure and drops active connections.
2. In-flight transactions receive an error (connection reset, server closed the connection).
3. The application must detect the error, reconnect, and retry the transaction from scratch.
4. Many applications do not implement retry logic correctly (or at all).

Transaction Replay eliminates this entire failure mode. The proxy captures every statement in the active transaction, detects the failover, connects to the new primary, replays the journal, and returns the result to the client as if nothing happened.

### Industry Comparison

Oracle Database has offered similar capabilities for years:

- **Oracle TAF (Transparent Application Failover)** — reconnects sessions and optionally re-executes SELECT statements after failover, but does not replay DML transactions.
- **Oracle TAC (Transparent Application Continuity)** — full transaction replay including DML, introduced in Oracle 12c. The gold standard.

HeliosProxy brings TAC-equivalent capabilities to PostgreSQL for the first time.

## Architecture

```
Client                    HeliosProxy                     PostgreSQL
  │                           │                               │
  │── BEGIN ────────────────▶ │── BEGIN ─────────────────────▶│ primary
  │                           │   [journal: BEGIN]            │
  │── INSERT ... ───────────▶ │── INSERT ... ────────────────▶│
  │                           │   [journal: INSERT ...]       │
  │── UPDATE ... ───────────▶ │── UPDATE ... ────────────────▶│
  │                           │   [journal: UPDATE ...]       │
  │                           │                               │ ✗ primary dies
  │                           │   [detect failure]            │
  │                           │   [connect to new primary]    │
  │                           │── BEGIN ─────────────────────▶│ new primary
  │                           │── INSERT ... ────────────────▶│
  │                           │── UPDATE ... ────────────────▶│
  │                           │   [verify consistency]        │
  │── COMMIT ───────────────▶ │── COMMIT ───────────────────▶│
  │◀─ COMMIT OK ──────────── │◀─ COMMIT OK ──────────────── │
  │                           │                               │
```

The client never sees the failover. The COMMIT returns successfully.

## Components

### 1. Transaction Journal

The journal is an ordered, in-memory log of every statement executed within the current transaction.

**What is captured:**
- SQL statement text
- Bind parameters (serialized with type OIDs)
- Statement sequence number (monotonically increasing)
- Execution timestamp
- Result metadata (row count, column types) for verification
- WAL LSN at time of execution (for consistency anchoring)

**Memory management:**
- Journals are per-session, allocated from a pool
- Configurable maximum journal size (default: 16 MB per session)
- Transactions exceeding the journal limit fall back to error-on-failover behavior
- Journals are freed immediately on COMMIT/ROLLBACK

### 2. Failover Detection

The proxy detects backend failure through multiple signals:
- TCP connection reset or timeout
- Health check failure (configurable interval, default 2s)
- WAL receiver disconnection (monitored via `pg_stat_wal_receiver`)

Detection triggers the replay pipeline rather than returning an error to the client.

### 3. Replay Engine

Once a new primary is available, the replay engine:

1. Opens a new backend connection to the new primary.
2. Migrates session state (see Session Migration below).
3. Replays each journal entry in sequence order.
4. After each statement, verifies the result matches the original (row counts, error codes).
5. If verification passes, the transaction continues normally on the new primary.
6. If verification fails, the transaction is aborted and the client receives an error (this is a safety guard for non-deterministic transactions).

### 4. Verification

Replay verification ensures the replayed transaction is consistent with what the client observed:

- **Row count matching:** INSERT/UPDATE/DELETE must affect the same number of rows.
- **SELECT result comparison:** For TR mode `select`, SELECT results are hashed and compared.
- **Error code consistency:** If the original statement returned an error, the replay must produce the same error.

Verification failures are rare but possible with non-deterministic functions (`random()`, `now()`, `clock_timestamp()`). The proxy logs a warning and aborts the replayed transaction.

## Transaction Journal Internals

### Statement Capture

Every statement passing through the proxy in an active transaction is intercepted at the protocol level:

```
Parse → Bind → Describe → Execute
```

The proxy captures the complete Parse/Bind cycle, including:
- The SQL text from Parse
- Parameter values and type OIDs from Bind
- The requested result format from Describe

### Parameter Serialization

Bind parameters are stored in their wire-protocol binary format to ensure exact reproduction during replay. This avoids any text-to-binary conversion ambiguity.

### LSN Anchoring

Each journal entry records the WAL LSN returned by the primary after execution. During replay, the proxy waits for the new primary to have replayed past this LSN before beginning the journal replay, ensuring the new primary's state is at least as fresh as what the transaction previously observed.

## Replay Ordering

Journal entries are replayed in strict sequence-number order. The sequence number is assigned at capture time and is monotonically increasing within a session.

**WAL synchronization:** Before replay begins, the proxy queries the new primary's `pg_last_wal_replay_lsn()` and compares it against the journal's maximum LSN. If the new primary has not yet replayed to that point, the proxy waits (with a configurable timeout, default 15s) for it to catch up.

## Cursor Restore

Cursors declared within the transaction are part of the journal and are re-declared during replay. Cursor position tracking works as follows:

1. `DECLARE cursor_name CURSOR FOR ...` is journaled.
2. Each `FETCH` records the number of rows fetched and the cumulative position.
3. During replay, the cursor is re-declared and a `MOVE FORWARD n` is issued to restore position.
4. The next FETCH from the client operates at the correct position.

This approach avoids re-fetching and re-sending rows the client has already received.

## Session Migration

When the proxy connects to the new primary for replay, it must restore the session environment:

### SET Parameters
All `SET` commands issued in the session (before and during the transaction) are captured and replayed:
- `SET search_path`, `SET timezone`, `SET statement_timeout`, etc.
- `SET LOCAL` parameters (transaction-scoped) are replayed within the transaction.

### Prepared Statements
Named prepared statements are re-prepared on the new connection before replay begins. The proxy tracks all `PREPARE` / `DEALLOCATE` calls.

### Temp Tables
Temporary tables created **within the transaction** are part of the journal and are re-created during replay. Temp tables created in prior transactions (before the current one) cannot be migrated — this is a known limitation.

### Advisory Locks
Advisory locks held at the session level cannot be transparently migrated. The proxy logs a warning if advisory locks are detected during replay.

## Switchover Buffer

During a **planned switchover** (e.g., maintenance, upgrades), HeliosProxy provides a zero-dropped-query guarantee:

1. The admin API receives a switchover command.
2. The proxy stops sending **new** transactions to the current primary.
3. In-flight transactions are allowed to complete (with a configurable drain timeout).
4. The switchover proceeds (promote standby, demote old primary).
5. New transactions are routed to the new primary.
6. Any transactions that were in the "drain" phase and whose backend connection dropped are replayed on the new primary.

This means planned maintenance causes **zero client errors** — not even a retry.

## Configuration

### TR Modes

| Mode | Behavior |
|------|----------|
| `none` | Transaction Replay disabled. Failover returns errors to clients. |
| `session` | Replays session state (SET parameters, prepared statements) but not transaction statements. Clients get an error for in-flight transactions but reconnect seamlessly. |
| `select` | Replays full transactions and verifies SELECT results match. Most conservative — catches non-deterministic queries. |
| `transaction` | Replays full transactions, verifies row counts for DML but does not compare SELECT result sets. Best balance of safety and compatibility. |

### Configuration Keys

```toml
# Enable Transaction Replay
tr_enabled = true

# TR mode: "none", "session", "select", "transaction"
tr_mode = "transaction"

# Max journal size per session (bytes). Transactions exceeding this
# fall back to error-on-failover.
tr_max_journal_bytes = 16777216  # 16 MB

# Time to wait for new primary to be WAL-consistent before replay
write_timeout_secs = 15

# Switchover drain timeout
switchover_drain_timeout_secs = 30
```

## Limitations and Trade-offs

### Journal Memory
Each active transaction consumes memory proportional to its journal size. For workloads with many concurrent large transactions, memory usage can be significant. The `tr_max_journal_bytes` setting provides a safety cap.

### Replay Latency
Replay adds latency to the COMMIT that spans the failover. The client experiences the failover as a slow COMMIT rather than an error. Typical replay latency: 50-500ms depending on journal size and new primary readiness.

### Non-deterministic Functions
Transactions using `random()`, `clock_timestamp()`, `txid_current()`, or similar non-deterministic functions may fail verification during replay. `now()` and `current_timestamp` are safe because they are fixed at transaction start. Use TR mode `transaction` (not `select`) to avoid SELECT verification issues with non-deterministic queries.

### Sequence Gaps
Sequences (`nextval()`) will produce different values on the new primary. This is acceptable for surrogate keys but may cause issues if the application depends on specific sequence values. The proxy does not attempt to synchronize sequence state.

### Temp Tables from Prior Transactions
Temporary tables created before the current transaction cannot be migrated. If the replayed transaction references a pre-existing temp table, replay will fail with a "relation does not exist" error.

### Advisory Locks
Session-level advisory locks (`pg_advisory_lock()`) are not migrated. If the replayed transaction depends on an advisory lock held before the transaction began, application-level coordination is needed.

## Comparison

| Capability | HeliosProxy TR | Oracle TAF | Oracle TAC | MySQL MaxScale | PgBouncer |
|---|---|---|---|---|---|
| **Session reconnect** | Yes | Yes | Yes | Yes | No |
| **SELECT replay** | Yes | Yes (read-only) | Yes | No | No |
| **DML transaction replay** | Yes | No | Yes | No | No |
| **Bind parameter preservation** | Yes | N/A (OCI) | Yes | No | No |
| **Cursor position restore** | Yes | Yes | Yes | No | No |
| **Planned switchover (zero-drop)** | Yes | No | Yes | Partial | No |
| **Verification / safety checks** | Row count + optional SELECT hash | Basic | Full (logical comparison) | N/A | N/A |
| **Non-deterministic detection** | Yes (configurable strictness) | N/A | Yes (mutable function list) | N/A | N/A |
| **Max journal size cap** | Configurable | N/A | Configurable | N/A | N/A |
| **Open source** | Yes (AGPL-3.0) | No | No | Yes (BSL) | Yes (ISC) |
| **PostgreSQL support** | Native | No | No | No | Yes (no TR) |
| **Proxy-level (no app changes)** | Yes | Requires OCI driver | Requires JDBC driver | Yes | Yes (no TR) |
