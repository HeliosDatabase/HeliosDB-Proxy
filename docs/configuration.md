# HeliosProxy Configuration Reference

Complete reference for HeliosProxy (`heliosdb-proxy`) configuration. HeliosProxy uses
TOML for its configuration file. The authoritative parser is `ProxyConfig` (and its
section structs) in `src/config.rs`; every key documented here is a real field of that
type. Keys, sections, and defaults are kept in sync with the code — if a section is not
listed here, it is not part of `ProxyConfig` and is silently ignored on load (see
[Unknown Keys](#unknown-keys-are-warned-not-rejected)).

---

## Usage

```bash
# Start with a configuration file
heliosdb-proxy --config /etc/heliosproxy/config.toml

# Start with command-line arguments (no config file)
heliosdb-proxy \
  --listen 0.0.0.0:6432 \
  --admin 127.0.0.1:9090 \
  --primary db-primary:5432 \
  --standby db-standby-1:5432 \
  --standby db-standby-2:5432

# Override log level
heliosdb-proxy --config config.toml --log-level debug

# Emit JSON-structured logs
heliosdb-proxy --config config.toml --json-logs
```

---

## Command-Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--config`, `-c` | *(none)* | Path to TOML configuration file. |
| `--listen`, `-l` | `0.0.0.0:5432` | Client (PostgreSQL-wire) listen address. |
| `--admin` | `127.0.0.1:9090` | Admin API listen address. Loopback by default — see [Admin API Security](#admin-api-security). |
| `--admin-token` | *(none)* | Admin bearer token in CLI mode. Prefer a protected config file with `${VAR}` substitution when shell history/process arguments must not contain secrets. |
| `--admin-allow-insecure` | `false` | Explicitly permit an unauthenticated admin listener in CLI mode. Development only; exposes privileged operations. |
| `--primary` | *(none)* | Primary node `host:port`. |
| `--standby` | *(none)* | Standby node `host:port` (repeatable). |
| `--tr` | `true` | Enable Transaction Replay. Takes an optional value: `--tr false` or `--tr=false` turns it off; a bare `--tr` means `true`. |
| `--log-level` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error`. |
| `--json-logs` | `false` | Emit logs in JSON format. |

There is also an `install skills` subcommand (`--target claude|codex|both`,
`--symlink`, `--force`, `--dry-run`) for installing the bundled agent skills.

When `--config` is provided, the file drives the whole configuration. Command-line node
arguments (`--primary`, `--standby`) build an in-memory config only when no config file
is supplied. The two admin security flags also apply only in CLI mode; combining them with
`--config` is rejected rather than silently ignoring authentication settings.

### Signals

- **SIGHUP** — live configuration reload.
- **SIGUSR2** — graceful drain for a zero-downtime binary handoff, bounded by
  `shutdown_drain_timeout_secs` (env override `HELIOS_DRAIN_TIMEOUT_SECS`).
- There is **no** SIGTERM / Ctrl-C handler: SIGTERM terminates the process immediately.

---

## Environment-Variable Substitution

Before the file is parsed as TOML, HeliosProxy expands environment-variable references
in the raw text (`ProxyConfig::from_file` → `substitute_env`):

| Syntax | Meaning |
|--------|---------|
| `${NAME}` | Replaced with the value of environment variable `NAME`. If `NAME` is **unset**, startup fails fast with an error naming the variable — the literal is never left in the output. |
| `${NAME:-default}` | Replaced with `NAME` if set, otherwise the literal `default` text (which may be empty; it runs up to the first `}`). |

```toml
listen_address = "${HELIOS_LISTEN:-0.0.0.0:5432}"
admin_token    = "${ADMIN_TOKEN}"          # fails at startup if ADMIN_TOKEN is unset

[branch]
admin_password = "${PGPASSWORD}"

# max_connections = ${POOL_MAX:-100}       # unquoted numeric substitution is valid TOML
```

Rules and limits:

- `NAME` must match `[A-Za-z_][A-Za-z0-9_]*`.
- Substitution is **in place**, so an unquoted `${POOL_MAX:-100}` becomes the bare
  token `100` (valid TOML) and a quoted `"${X:-y}"` becomes `"y"`.
- **Env lookup only.** No shell is spawned and nothing is evaluated — only the env
  lookup plus the `:-` default operator run.
- **Comment handling is line-level.** A line whose first non-whitespace byte is `#` (a
  full-line comment) is copied verbatim and never substituted, so commented `${VAR}`
  examples in the shipped reference configs do not trigger the unset-variable error. A
  trailing `#` comment on a value line is **not** treated as a comment.
- **Trust boundary.** Substitution is textual and runs *before* the TOML parse, so a
  substituted value containing a quote or newline can inject arbitrary TOML structure.
  Environment variables and the config file are operator-controlled, which makes this
  acceptable — but do **not** feed an untrusted-party-controlled environment variable
  into a `${...}` reference.

### Real environment variables

There is **no** generic `HELIOS_PROXY_*` "override any value" system. Only one runtime
environment variable is consulted directly by the proxy:

| Variable | Effect |
|----------|--------|
| `HELIOS_DRAIN_TIMEOUT_SECS` | Overrides `shutdown_drain_timeout_secs` at runtime (SIGUSR2 drain bound). |

Everything else is configured through the file (optionally via `${VAR}` substitution
above) or the command-line arguments.

---

## Unknown Keys Are Warned, Not Rejected (By Default)

`ProxyConfig` does **not** use `deny_unknown_fields`. Except for the rejected `[ha]`
section described below, any TOML key that is not a
recognized `ProxyConfig` field — top-level *or nested inside a known section* — is
parsed-and-ignored, and each one is logged once at startup as a warning, with its full
dotted path:

```
WARN unknown config key '<key>' ignored (not part of ProxyConfig)
WARN unknown config key 'cache.enabed' ignored (not part of ProxyConfig); did you mean 'cache.enabled'?
WARN unknown config key 'rate_limit.global_qps' ignored (not part of ProxyConfig); did you mean 'rate_limit.default_qps'?
```

Each warning names the key most likely meant: a known rename or never-existed key
(table below), else the closest key of the same section when the typo is small. The
same hint appears in the `strict_config` error. The list of ignored keys also stays
available after startup, so a monitor can catch a policy that silently ran at its
defaults: `GET /config` returns `ignored_config_keys`, `GET /health` returns their
count, and `/metrics/prometheus` exposes the gauge `heliosdb_proxy_config_ignored_keys`
(all follow a SIGHUP reload).

Renamed or non-existent keys operators reach for:

| Written | Reality |
|---|---|
| `[rate_limit] global_qps`, `global_burst` | `default_qps`, `default_burst` (per bucket, see `key_by`). With the wrong names the limiter runs at its defaults, 1000 qps / burst 2000. |
| `[pool_mode] validation_query` | Never a key. `[pool] test_on_acquire` checks that a pooled socket is alive before handing it out (no query); `[health] check_query` probes nodes. |
| `[pool_mode] queue_timeout_secs`, `max_queue_size` | Never keys: the pool does not queue acquisitions; `acquire_timeout_secs` bounds the wait. |
| `[ha]` | Rejected (below). |
| `[logging]`, `[metrics]` | `--log-level` / `--json-logs`; `/metrics` and `/metrics/prometheus` are always on. |
| `[routing]`, `[lag]`, `[rewriter]`, `[graphql]`, `[[tenants]]`, `[[schema_routes]]`, `[auth.jwt]`, `[cache.l1]` | `[load_balancer]` / `[routing_hints]` / `[lag_routing]`, `[query_rewrite]`, `[graphql_gateway]`, `[multi_tenancy]`, `[schema_routing]`, `[auth] mode`, flat `[cache]` keys. |
| `[ai_cache]` (`semantic_cache_enabled`, `similarity_threshold`, `rag_chunk_cache_size`, …) | Never a section. The semantic tier is configured under `[cache]`: `semantic_enabled` (was `semantic_cache_enabled`) and `semantic_similarity_threshold` (was `similarity_threshold`), used by statements that carry `/*helios:cache=semantic*/` ([Semantic tier](#semantic-tier-l3)). There are no conversation or RAG-chunk caches: the proxy caches SQL results (`distribcache` is a library module the daemon does not configure). |
| `[cache.query]` (`max_result_size`, `exclude_patterns`) | `[cache]` with `max_result_bytes`. There is no exclusion pattern: only single-statement `SELECT` reads are cached and writes always invalidate; keep a table out of the cache with `[cache] exclude_tables` or `[[cache.tables]] exclude = true`. |
| `[cache.semantic]`, `[cache.l3]`, `[cache] similarity_threshold`, `semantic_cache_enabled` | The semantic keys are flat under `[cache]`: `semantic_enabled`, `semantic_similarity_threshold`, `semantic_*`. |
| `[cache] exclude_patterns`, `excluded_tables` | `exclude_tables` (whole table names; writes are never cached). |
| `[[cache.tables]] ttl`, `table`, `exclude_columns`; `[cache] exclude_columns` | `ttl_secs`, `name`. `exclude_columns` is **refused at load** in any spelling under `[cache]` (flat, per table, as a table, or inside any other table such as `[cache.query]` or `[cache.l3]`, at any depth and in any case), even without `strict_config`: column-level exclusion cannot be enforced (a `SELECT *`, a view or a function hides which columns a read returns); set `exclude = true` for the table, or list it in `exclude_tables`. |
| `[tenancy]`, `[[tenancy.tenant]]` | `[multi_tenancy]` (`identify_by`, `tenant_column`, `tenant_tables`, `tenants`, `max_clients_per_tenant`, `tenant_max_clients`, `max_active_queries_per_tenant`, `tenant_max_active_queries`, `active_query_wait_ms`, `idle_permit_timeout_ms`, `max_replay_bytes_per_tenant`, `tenant_max_replay_bytes`). |
| `[rate_limit] global_connections`, `[[rate_limit.tenant]]` | Rate limits apply per bucket (`key_by`); there are no per-tenant rate-limit tables. Per-tenant concurrency is `[multi_tenancy] max_active_queries_per_tenant`. |
| `[circuit_breaker] reset_timeout`, `half_open_requests` | `open_secs`, `success_threshold`. |
| `[auth] enabled`, `[auth.api_key]`, `[auth.ldap]` | `[auth] mode = "passthrough" \| "scram"` plus `auth_file`; admission rules are `[[hba]]`. |
| `[transparent_write_routing]`, `[upgrade]`, `[[upgrade.stages]]` | Never sections: writes always go to the primary (`tr_mode`, `write_timeout_secs`, `[load_balancer] read_write_split`); the upgrade orchestrator is a library module. |
| `[[plugins]]` (array of `name`, `source`, `hooks`, `verify_with`) | `[plugins]` is one table (`enabled`, `plugin_dir`, `trust_root`, limits); an array where a table is expected is a parse error, not an ignored key. |

This makes silent doc-drift and typos visible without breaking pre-existing configs.
Historical example blocks such as `[logging]`, `[metrics]`, `[routing]`,
`[distribcache]`, `[graphql]`, `[[tenants]]`, and `[[schema_routes]]` are **not** part of
`ProxyConfig` — they will be warned and ignored. Their real counterparts are documented
below (`lag_routing`, `multi_tenancy`, `graphql_gateway`, `schema_routing`, …).

**Exception: `[ha]` is rejected even with `strict_config = false`.** Silently
ignoring `auto_failover` would leave an operator expecting a promotion loop that
the daemon does not run. Remove that section, configure database promotion and
fencing externally, and select a compatible `[topology]` provider to discover
the promoted leader. See [topology providers](topology-providers.md#setting-up-automatic-routing-after-promotion).

Detection covers every nesting depth (built on `serde_ignored`, not a hand-maintained
whitelist), so a typo inside an otherwise-valid section — e.g. `[cache] l1_max_byts = 1`
instead of `max_result_bytes`, or `[journal] fsyncc = "commit"` instead of `fsync` — is
caught exactly like an unknown top-level section. Set `strict_config = true` to turn
every one of these warnings into a hard startup failure instead (see below).

---

## Top-Level Options

```toml
listen_address = "0.0.0.0:5432"
admin_address  = "127.0.0.1:9090"
# admin_token          = "..."     # bearer token for the admin API (see below)
# admin_allow_insecure = false
# strict_config        = false     # reject settings this build cannot deliver (D-05)
tr_enabled     = true
tr_mode        = "session"
tr_read_functions = []
write_timeout_secs        = 30
optimize_unnamed_parse    = true
shutdown_drain_timeout_secs = 60
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `listen_address` | string | `"0.0.0.0:5432"` | Address/port for PostgreSQL client connections. |
| `admin_address` | string | `"127.0.0.1:9090"` | Address/port for the admin HTTP API. Loopback by default. |
| `admin_token` | string | *(none)* | Bearer token required on every admin endpoint except liveness probes. See [Admin API Security](#admin-api-security). |
| `admin_allow_insecure` | bool | `false` | Explicit opt-in to expose the admin API on a non-loopback address **without** a token. |
| `strict_config` | bool | `false` | Opt-in strict configuration (D-05): startup fails when an enabled subsystem was not compiled into the binary (e.g. `[cache] enabled = true` without the `query-cache` feature) or when an unknown config key is present, at any nesting depth (an unknown top-level section, or a typo'd key inside a known section such as `[cache] l1_max_byts`). Default `false` keeps the historical warn-and-continue behaviour. `GET /capabilities` reports the per-subsystem compiled/enabled/wired state. |
| `tr_enabled` | bool | `true` | Master switch for Transaction Replay, which ships in the default build. When `false`: the write journal stops recording, `tr_mode` is forced to `none`, and `POST /api/replay` returns `503`. |
| `tr_mode` | string | `"session"` | Transaction Replay mode: `none`, `session`, `select`, `transaction`. |
| `tr_read_functions` | array of string | `[]` | TR-03 read re-execution policy extension. In-session replay re-executes an interrupted read on an unknown outcome only when every function it calls is a PostgreSQL built-in known to be side-effect-free; list additional provably pure functions (unqualified names, case-insensitive) here. Reads calling anything else, quoted-identifier calls, `SELECT … INTO`, and sequence functions are classified as opaque and never re-executed. |
| `write_timeout_secs` | u64 | `30` | Seconds to buffer writes during failover before returning an error — and, since 1.7.0, the **single deadline for a whole session recovery**: waiting for a primary, connect/auth, session-state restore and replay all draw on it, instead of each having its own timeout. |
| `optimize_unnamed_parse` | bool | `true` | Skip re-forwarding an identical unnamed extended-protocol `Parse` a backend already holds, synthesizing `ParseComplete` locally. A kill-switch for drivers that depend on the redundant round trip. |
| `shutdown_drain_timeout_secs` | u64 | `60` | How long a SIGUSR2 binary-handoff drain keeps serving in-flight connections before dropping them. Runtime override: `HELIOS_DRAIN_TIMEOUT_SECS`. |

Only backend identity is required: configure at least one `[[nodes]]` with `host`
and `role`, including a primary. Node ports default to 5432, weight to 100, and
`enabled` to true. Omitted top-level settings and omitted fields in `[pool]`,
`[pool_mode]`, `[load_balancer]`, and `[health]` use their documented defaults.
Explicit values still undergo validation; admin authentication is still required
for non-loopback listeners. For example, this is a complete minimal file:

```toml
listen_address = "127.0.0.1:6432"

[[nodes]]
host = "127.0.0.1"
role = "primary"
```

The explicit proxy port avoids conflicting with a local database on port 5432.
Without this override, the proxy listener defaults to `0.0.0.0:5432`.

See `config/proxy.minimal.toml` for a copyable minimal configuration and
`config/proxy.example.toml` for a commented starter with tuning options.

### Transaction Replay Modes (`tr_mode`)

`tr_mode` governs **in-session failover**: what a live client session experiences
when its backend connection fails while a request is in flight (write error /
timeout, read error, EOF, reset) or dies while idle. It needs no cargo feature —
Transaction Replay is in the default build — but `tr_enabled = false` forces the
effective mode to `none` and disables the operator-driven `POST /api/replay`
engine at the same time. Two fault phases are distinguished:
**not-delivered** (the request never reached the backend — it certainly did not
run) and **outcome-unknown** (written, then the connection failed before
`ReadyForQuery`).

| Mode | Behaviour on a backend fault |
|------|-------------|
| `none` | One `ErrorResponse` (SQLSTATE `57P01`, naming the failed node) + `ReadyForQuery`, then the client connection is closed. |
| `session` (default) | The client connection stays open. The proxy waits for a healthy primary (`write_timeout_secs`), reconnects (startup parameters re-sent), replays the session's tracked `SET`/`RESET` statements, and re-prepares named prepared statements lazily. A not-delivered statement issued **outside** an explicit transaction is re-executed transparently. Anything else gets ONE error — `57P01` (not delivered inside a transaction) or `08007 transaction_resolution_unknown` — the client-visible transaction is aborted (`ROLLBACK` and retry; other statements get `25P02` until then) and the session continues on the new primary. |
| `select` | `session`, plus: an outcome-unknown **read** (`SELECT`/`SHOW`/`VALUES`/read-only `WITH`/`COPY … TO`) is re-executed transparently outside a transaction, **but only when every function it calls is known side-effect-free** — a PostgreSQL built-in on the allowlist or a name you listed in `tr_read_functions`. A read calling a user-defined function, `nextval`, `pg_notify`, `set_config`, an advisory lock, a quoted-identifier call, or using `SELECT … INTO` is opaque: it returns `08007` and is never run twice. A **read-only** explicit transaction is replayed from its `BEGIN` first and the interrupted statement re-run inside it. |
| `transaction` | `select`, plus: an **uncommitted** explicit transaction is replayed from its `BEGIN` on the new primary (recorded simple-protocol text / raw extended-protocol batches) and the in-flight statement is re-executed inside it. Replay is **verified**: each statement's original response frames were digested as the client saw them (within `[limits] tr_max_observation_bytes`) and the replacement backend's responses are digested the same way — any divergence rolls the replay back and returns `40001`, so a replay that would silently continue on different rows fails loudly instead. A failed replayed statement produces the same `40001`. A `COMMIT`/`END`/`PREPARE TRANSACTION`/`COMMIT PREPARED` whose outcome is unknown is **never** retried (`08007`). Still opt-in: verification catches divergent *results*, it does not make `now()` or a `RETURNING` serial reproduce its first value. |

Hard rules in every mode: a write whose outcome is unknown is never re-executed
except as part of `transaction` mode's replay of an uncommitted transaction (the
original died uncommitted with the old backend); a `COMMIT` with unknown outcome
is never retried; `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` are ordinary replayed
statements; a `COPY` in progress at the fault → `08006` and the connection is
closed. A transaction is marked non-replayable (so `transaction` degrades to
`session` for it) when it exceeds `[limits] tr_max_replay_statements` /
`tr_max_replay_bytes` or its tenant's `[multi_tenancy]` replay-byte budget,
enters the failed state, contains a `COPY`, or one of
its statements was transformed by query-rewrite / multi-tenancy, runs at
`SERIALIZABLE` or `REPEATABLE READ` (explicitly, via `SET TRANSACTION`, or
inherited from a tracked `default_transaction_isolation` — no replay can
reproduce that snapshot), or produced a response too large to have been digested
within `[limits] tr_max_observation_bytes`.

Session `SET` tracking is transactional: a `SET` inside a transaction takes effect
in the restore set only when that transaction commits, `ROLLBACK TO SAVEPOINT`
discards the ones made after the savepoint, and `RESET`/`RESET ALL`/`DISCARD ALL`
inside a transaction are deferred to commit. Variables are keyed by name, so
repeating a `SET` reuses its slot; the cap `tr_max_session_set_statements` counts
**distinct variables**, and exceeding it refuses the failover with `08006` rather
than re-homing a session with incomplete state. It covers simple-protocol
queries and extended-protocol cycles. A multi-statement query's or a cycle's
statements are read in order, the way PostgreSQL runs them in one implicit
transaction. That covers a pool reset such as `SET SESSION AUTHORIZATION
DEFAULT;RESET ALL;...`, a JDBC `BEGIN` in front of a transaction's first
statement, pgjdbc `autosave` savepoints and batches. Extended-protocol
`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`/`ROLLBACK TO` (and the `AND CHAIN` forms)
settle deferred settings like simple-protocol ones. Leading comments do not hide a
statement, and `SET LOCAL` is not a session setting.

The proxy counts the statements of each request that completed (`CommandComplete`,
`EmptyQueryResponse`, `PortalSuspended`, output answering a `Flush` included) and notes
any error in its responses, so a failed cycle is read exactly: the statement after the
last one that completed failed. Only when that count is not whole (a backend fault or
re-home during the request, a `[query_rewrite]` rule that changed its text, or a count
that does not fit the request) are all the places the error could have come from
considered. When a failed cycle leaves it open which of its statements ran, or a cycle
cannot be read statement by statement, the session's settings become unknown (inside a
transaction, once it commits). Examples: a statement the proxy has no record of, an
Execute of a portal bound in another cycle, a call of `set_config(..., false)`, a `SET`
before a `CALL` or `DO` in the same cycle or query, or more statements than
`tr_max_cycle_statements`. A failover is then refused with `08006`
(`tr_session_set_untracked_total`) until `DISCARD ALL`. `SET SESSION AUTHORIZATION`
ends a tracked `SET ROLE`, as in PostgreSQL. At most `tr_max_tracked_savepoints`
savepoints per transaction are tracked. `SET`s run inside functions, procedures or `DO`
blocks are not seen. SQL-level `PREPARE`, session temp tables and cursors are not
restored.

**Backend credentials.** Re-homing a session means opening a *fresh* backend
connection. In pass-through auth mode the proxy never sees the client's
password (SCRAM is designed for that), so a fresh connection can only be
completed against a backend that does not challenge (trust); a challenging
backend fails the recovery immediately with `08006` ("the proxy could not
authenticate to the replacement primary"). To fail over onto password-protected
(SCRAM-SHA-256 / MD5 / cleartext) backends, make the proxy the auth boundary:
`[auth] mode = "scram"` with a **plaintext** `auth_file` entry for the user —
the proxy then authenticates the client itself and uses the same secret as a
SCRAM client towards every backend connection it opens (this also removes the
former "scram mode needs a trust backend" restriction on the first connection).

Counters on `/metrics` and `/metrics/prometheus`: `tr_failovers_total`,
`tr_statements_reexecuted_total`, `tr_transactions_replayed_total`,
`tr_replay_failures_total`, `tr_unknown_outcome_errors_total`,
`tr_replay_cap_exceeded_total`, `tr_session_set_cap_exceeded_total`,
`tr_session_set_untracked_total`.

---

## Admin API Security

The admin API runs privileged operations (arbitrary SQL via `/api/sql`, forced failover
via `/api/chaos`, migration cutover, branch `CREATE`/`DROP DATABASE`, replay/shadow
against operator-chosen targets), so its exposure is guarded at startup:

- **Default bind is loopback** (`127.0.0.1:9090`). A fresh install is safe.
- If `admin_address` parses to a **non-loopback** IP **and** `admin_token` is unset
  **and** `admin_allow_insecure` is `false`, the proxy **refuses to start** with a
  descriptive error. Fix it by setting `admin_token`, binding to `127.0.0.1`, or
  setting `admin_allow_insecure = true` (only when you front the admin port with your
  own authenticating proxy / network policy).
- When `admin_token` is set, every admin endpoint requires
  `Authorization: Bearer <token>` except the liveness probes.

Startup validation also rejects `health.check_interval_secs = 0` (a zero interval would
panic the health-check timer and silently stop probing).

---

## Pool Mode (`[pool_mode]`)

Controls Session/Transaction/Statement pooling.

```toml
[pool_mode]
mode = "transaction"
max_pool_size = 100
min_idle = 10
idle_timeout_secs = 600
max_lifetime_secs = 3600
acquire_timeout_secs = 5
reset_query = "DISCARD ALL"
prepared_statement_mode = "track"
skip_clean_reset = false
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | string | `"session"` | Pooling mode: `session`, `transaction`, `statement`. |
| `max_pool_size` | u32 | `100` | Maximum backend connections per node. |
| `min_idle` | u32 | `10` | Minimum idle connections to maintain. |
| `idle_timeout_secs` | u64 | `600` | Close idle connections after this many seconds. |
| `max_lifetime_secs` | u64 | `3600` | Recycle connections after this many seconds. |
| `acquire_timeout_secs` | u64 | `5` | Max seconds to wait when acquiring from the pool. Also bounds how long a new backend connection keeps retrying when PostgreSQL refuses it at `max_connections` (SQLSTATE 53300): each retry first closes one idle pooled connection to that node, and only after this timeout does the client receive the 53300. Size the backend's `max_connections` for the pool plus the expected burst of new client logins, since every pass-through login opens its own backend connection. |
| `reset_query` | string | `"DISCARD ALL"` | SQL run when a connection returns to the pool. |
| `prepared_statement_mode` | string | `"disable"` | Prepared-statement handling: `disable`, `track`, `named`. |
| `skip_clean_reset` | bool | `false` | Transaction/Statement pooling only: park a connection that provably touched **no** session state (no `SET`/GUC, temp table, prepared statement, `LISTEN`, advisory lock, …) *without* running `reset_query`, saving a round-trip per clean transaction. Classification is conservative — a misclassification only ever costs an unnecessary reset, never leaks state. Intended for autocommit / simple-protocol workloads. |

### Pooling Modes

| Mode | Returns To Pool | Best For |
|------|-----------------|----------|
| `session` | When the client disconnects (1:1 client↔backend). | Prepared statements, long-running sessions, legacy apps. |
| `transaction` | After `COMMIT`/`ROLLBACK`. | Web apps, microservices, connection-starved environments. |
| `statement` | After each statement. | Simple read-heavy workloads without multi-statement transactions. |

### Prepared Statement Modes

| Mode | Behavior |
|------|----------|
| `disable` | Not tracked. Safest for transaction/statement pooling. |
| `track` | Track PREPARE/DEALLOCATE and recreate on a new backend connection. |
| `named` | Protocol-level named statements. Compatible with session pooling. |

---

## Connection Pool (`[pool]`)

Core per-node connection pool. *(Required section.)*

```toml
[pool]
min_connections = 2
max_connections = 100
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 30
test_on_acquire = true
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `min_connections` | usize | `2` | Minimum connections per node. |
| `max_connections` | usize | `100` | Maximum connections per node. Must be ≥ `min_connections`. |
| `idle_timeout_secs` | u64 | `300` | Close connections idle longer than this. |
| `max_lifetime_secs` | u64 | `1800` | Maximum connection lifetime before recycling. |
| `acquire_timeout_secs` | u64 | `30` | Max wait for a connection from the pool. |
| `test_on_acquire` | bool | `true` | Health-check a connection before handing it out. |

---

## Load Balancer (`[load_balancer]`)

*(Required section.)*

```toml
[load_balancer]
read_strategy = "round_robin"
read_write_split = true
standby_reads = "async"     # "async" | "sync" | "primary"
read_your_writes_ms = 1000
latency_threshold_ms = 100
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `read_strategy` | string | `"round_robin"` | Read routing strategy (see below). |
| `read_write_split` | bool | `true` | Route writes to the primary and, per `standby_reads`, reads to standby/replica nodes. `false`: every statement stays on the session's node (the primary). |
| `standby_reads` | string | `"async"` | Which standbys serve reads: `async` — any healthy standby or read replica, which may lag the primary; `sync` — only synchronous standbys (`[[nodes]] synchronous = true`, or Patroni's `sync_standby` member with `[topology] provider = "patroni"`), reads stay on the primary while there is none; `primary` — reads stay on the primary. |
| `read_your_writes_ms` | u64 | `1000` | For this long after a session sends a write — or `SET`, a transaction statement, or any statement the proxy cannot show is a side-effect-free read (`CALL`, `DO`, a function that may write) — its reads stay on the primary, so it reads back what it just wrote despite replica lag. `0` disables. A `/*helios:route=standby*/` hint overrides it. |
| `latency_threshold_ms` | u64 | `100` | Latency above which a node is treated as unhealthy for routing. |

### Read offload

With `read_write_split = true` and `standby_reads` other than `primary`, a statement runs on
a standby only when the proxy can show it is a side-effect-free read:

- one `SELECT`, `VALUES`, `TABLE` or `WITH` statement that transaction replay classifies as a
  read: every function it calls is a side-effect-free built-in or listed in
  `tr_read_functions`, it has no `INTO`, and a `WITH` contains no `INSERT`, `UPDATE`,
  `DELETE` or `MERGE`;
- without a row-locking clause (`FOR UPDATE`, `FOR SHARE`, …), and without a function that
  answers about the primary or fails during recovery (`pg_is_in_recovery()`, the WAL-position,
  replay-status, transaction-id and snapshot functions);
- outside an explicit transaction. `BEGIN` goes to the primary and the whole transaction
  stays there;
- for the extended protocol, every statement the `Sync`-terminated batch executes must
  qualify (a batch split by `Flush` stays on the primary). A `Bind` of a named statement
  prepared earlier is classified from that statement's SQL, and is re-prepared on the
  standby when needed;
- not part of a cycle a backend already holds open. Everything the client sends after a
  `Flush` forwarded part of a cycle, or after an extended-protocol `COPY` ended, up to and
  including the `Sync` or `Query` that settles it, goes to the connection holding that
  cycle, whatever a hint, read offload or the node's health would pick for a new request:
  only that backend can finish the cycle. If that connection is gone, the request fails
  like any statement whose backend failed (`tr_mode`), with an unknown outcome: part of
  its cycle ran there, so it is never re-run alone on a replacement connection. After a
  `Flush`-forwarded part the session closes; a `Sync` or `Query` continuing an ended
  extended `COPY` (whose rows died uncommitted with the connection) gets `08007` and the
  session goes on (`tr_mode = none`: `57P01`, and the session closes).

Everything else (writes, `SET`, `SHOW`, `COPY`, `EXPLAIN`, functions that may write, a
`/*helios:route=primary*/` hint) runs on the primary. A `/*helios:route=standby*/` hint
sends a statement the proxy cannot classify to a standby, but never one it classifies as
a write or transaction control; `/*helios:node=<name>*/` still targets that node.

A session's offloaded reads stay on one standby while it remains eligible (healthy, circuit
closed, within `[lag_routing] max_lag_bytes`, synchronous for `sync`), so its reads do not
alternate between standbys at different replay positions. With no eligible standby the
primary serves the read. A result a standby served is never stored in the query cache or
the edge cache: it may predate a write the cache was just invalidated for. Counters:
`heliosdb_proxy_reads_offloaded_total`, `heliosdb_proxy_read_mirror_failures_total`.

**Session state.** Session-level `SET`/`RESET` go to the primary and are mirrored onto a
standby connection before its next read (`RESET ALL`, then the session's settings, once per
change; session pooling only, since transaction and statement pooling reset connections
anyway). That covers the simple protocol and extended-protocol `SET`/`RESET`/`DISCARD
ALL`, alone or batched, as JDBC, Npgsql and tokio-postgres send them; inside a
transaction a `SET` applies when the transaction commits, in either protocol. Comments,
such as sqlcommenter tags, do not hide a statement from this tracking or from read
classification. `RESET ALL` keeps `SET ROLE` and `SET SESSION AUTHORIZATION`, as in
PostgreSQL; `DISCARD ALL` resets them. While the session's default isolation level is
SERIALIZABLE its reads stay on the primary (a hot standby refuses them). A session that
creates state only its primary connection holds keeps its reads on the primary for the
rest of its life: temporary tables (`CREATE TEMP …`, `SELECT … INTO`), cursors
(`DECLARE`), SQL `PREPARE`, `LISTEN`, `LOAD`, `set_config()`, a multi-statement simple
query, an extended-protocol cycle whose settings the proxy cannot tell (see Transaction
Replay Modes above), more tracked `SET`s than
`[limits] tr_max_session_set_statements`, or a `SET` the standby refuses.

**Authentication.** A standby connection is opened by the proxy itself. With the default
pass-through auth (`[auth] mode = "passthrough"`) the proxy holds no password for that, so a
standby that requires one refuses it: the read is served by the primary, the session keeps
its reads there, and later sessions of the same user and database without a credential skip
that standby (one warning per standby, user and database; other logins keep offloading, and
`heliosdb_proxy_read_mirror_failures_total` counts the fallbacks). A SIGHUP reload lets them
try again. To offload with password-protected standbys, use `[auth] mode = "scram"` with an
`auth_file` holding the users' passwords, or let the standbys trust the proxy's address.

**Connections.** In session pooling a session that has offloaded a read keeps one standby
connection besides its primary connection for its lifetime: size the standbys'
`max_connections` (and `[[nodes]] max_connections`) for the session count. The idle
session's primary connection is the one watched for backend loss; a standby connection
that died while idle is replaced before its next read.

**Consistency.** A read on an asynchronous standby can miss a write committed just before
it. `read_your_writes_ms` keeps a session's reads on the primary for a while after its own
writes; it does not cover other sessions' writes. For stronger guarantees bound the lag with
`[lag_routing] max_lag_bytes`, or use
`standby_reads = "sync"` with the primary's `synchronous_commit = remote_apply` (with `on`,
a synchronous standby has the commit flushed but not necessarily applied yet).

### Routing Strategies

| Strategy | Description |
|----------|-------------|
| `round_robin` | Rotate through nodes equally. |
| `weighted_round_robin` | Rotate proportionally to each node's `weight`. |
| `least_connections` | Route to the node with the fewest live sessions currently attached to it. |
| `latency_based` | Route to the node with the lowest measured health-probe latency. |
| `random` | Pick a node at random. |
| `power_of_two` | Sample two eligible nodes and pick the one with fewer attached sessions, breaking ties on measured latency (H-03). Cheaper and more herd-resistant than global least-connections. |

All strategies are applied to the healthy, enabled standbys after circuit-breaker and
lag exclusion; a strategy is only ever asked to choose among eligible nodes. The daemon
honors `[load_balancer] read_strategy` on the read path (the pre-1.8.1 daemon hardcoded
round-robin).

---

## Health Checks (`[health]`)

*(Required section.)*

```toml
[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `check_interval_secs` | u64 | `5` | Interval between probes. **Must be ≥ 1** (0 is rejected at startup). |
| `check_timeout_secs` | u64 | `3` | Max wait for a probe response. |
| `failure_threshold` | u32 | `3` | Consecutive failures before marking a node unhealthy. |
| `success_threshold` | u32 | `2` | Consecutive successes before a node marked unhealthy is considered healthy again. |
| `check_query` | string | `"SELECT 1"` | Health-check query. Used by the daemon only when `[health] user` is set; the default credential-less probe is a PostgreSQL `SSLRequest` liveness check. |
| `user` / `password` / `database` | string / string / string | *(none)* | Optional credentials for the daemon's health and lag probes (H-03/H-04). When set, the daemon connects and runs `check_query` (and, for standbys, WAL-position lag probes) instead of the credential-less protocol probe. |

---

## Topology (`[topology]`)

Authoritative primary tracking (H-01). Optional; absent = `provider = "static"`,
which preserves the historical behaviour (the configured `role = "primary"` node whose
health check passes is the primary).

```toml
[topology]
provider = "static"          # static | postgres | patroni
poll_interval_secs = 2
lease_timeout_secs = 10
user = "postgres"
# password = "${HELIOS_PROXY_TOPOLOGY_PASSWORD}"
database = "postgres"
# patroni_endpoints = ["http://10.0.0.1:8008", "http://10.0.0.2:8008"]
# patroni_request_timeout_ms = 2000
# patroni_confirm_leader_lock = true
# patroni_ttl_secs = 30
# patroni_loop_wait_secs = 10
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `provider` | string | `"static"` | `static`: configured roles + the daemon health checker decide the primary. `postgres`: poll `pg_is_in_recovery()` on every configured node and treat the non-recovering node as the primary. `patroni`: poll the Patroni REST API (`GET /cluster`) and treat the cluster's running leader as the primary. `postgres` and `patroni` require the `postgres-topology` cargo feature — a config that requests them on a build without that feature is rejected at startup. |
| `poll_interval_secs` | u64 | `2` | Seconds between provider polls. **Must be ≥ 1** (0 is rejected at startup). |
| `lease_timeout_secs` | u64 | `10` | How long a provider observation stays authoritative without a refresh (H-02). When the provider cannot be reached, the write path stops after at most this long instead of proceeding on stale knowledge. **Must be ≥ 1**. |
| `user` | string | `"postgres"` | User for the provider's own probe connections. |
| `password` | string | *(none)* | Password for the probe user, if required. |
| `database` | string | `"postgres"` | Database the probe connects to. |
| `patroni_endpoints` | list of URLs | `[]` | Patroni REST API base URLs (`http(s)://host:8008`), tried in order each poll; the first that answers is used. **Required** for `provider = "patroni"`; each must start with `http://` or `https://`. |
| `patroni_request_timeout_ms` | u64 | `2000` | Timeout for one Patroni REST request. **Must be ≥ 1**. |
| `patroni_confirm_leader_lock` | bool | `true` | *(patroni)* Count the leader `GET /cluster` names only while its own REST API answers `GET /primary` with 200, i.e. it holds Patroni's DCS leader lock (H-02). |
| `patroni_ttl_secs` | u64 | `30` | *(patroni)* The cluster's Patroni `ttl`. `lease_timeout_secs` must be below `patroni_ttl_secs - patroni_loop_wait_secs`, or startup fails. |
| `patroni_loop_wait_secs` | u64 | `10` | *(patroni)* The cluster's Patroni `loop_wait`. |

When a provider is configured it is **authoritative for the write path**: writes go only
to the provider's leader (which must be an enabled `[[nodes]]` entry), the answer waits
rather than falling back to a configured role while no leader is known, and
`GET /topology` reports the leader plus an `authoritative` block with its authority
epoch (incremented on every observed leader change), `valid`, and the remaining lease.
This is how a real promotion moves the write destination without hand-editing
`proxy.toml` roles.

The proxy does not initiate database promotion. A healthy `standby` is not a
writable primary: if the primary fails and no replacement is authorized,
`currentPrimary` becomes null and writes fail after `write_timeout_secs`.
Configure promotion and fencing in the database HA manager, then use a compatible
`postgres` or `patroni` provider to discover its result. `[ha] auto_failover = true`
is not a supported proxy configuration option, and enabling `ha-tr` does not add
a promotion loop. See [the failover setup guide](topology-providers.md#setting-up-automatic-routing-after-promotion).

**Fencing limits (H-02).** A proxy-side epoch/lease cannot fence a client that connects
directly to a database node: the proxy can only refuse to route, and it fails closed once
the lease expires (`valid: false`, writes wait). Preventing two sides of a partition from
both accepting writes requires enforcement by the database (synchronous replication and
promotion fencing) or by exclusive network/backend control. The proxy never advertises
zero-loss (RPO 0) for an asynchronous replica that has not acknowledged the last commit —
configure synchronous replication at the database if that is your requirement.

---

## Automatic promotion (`[auto_promote]`)

Opt-in and **off by default**. Requires `[topology] provider = "postgres"` (so the
proxy rediscovers the promoted node) and at least one enabled `role = "standby"`
node. **Single-proxy only**: two proxies with this enabled over one cluster can
promote different standbys. With Patroni or another HA manager, leave it off.

Promotion happens only when the topology has had no primary for `down_after_secs`,
a fresh probe of every node finds no writable primary, the standby with the highest
replay LSN has a known lag within `max_lag_bytes`, and `fence_command` (if set)
exits 0. The old primary is never demoted: use `fence_command` to stop or isolate it.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Master switch. |
| `down_after_secs` | `30` | Primary must be absent this long. |
| `check_interval_secs` | `5` | Decision-loop period. |
| `max_lag_bytes` | `16777216` | Max last-sampled lag of the chosen standby. |
| `allow_unknown_lag` | `false` | Promote without a lag sample (needs no `[health] user`). |
| `cooldown_secs` | `300` | Minimum gap between promotions. |
| `max_attempts` | `3` | Failed attempts per outage before giving up. |
| `promote_timeout_secs` | `60` | Bound on promote and on verification. |
| `method` | `"pg_promote"` | `pg_promote`, `command` or `http`. |
| `promote_command` | `[]` | argv for `command`; env `HELIOS_PROMOTE_TARGET`, `HELIOS_PROMOTE_OLD_PRIMARY`. |
| `promote_url` | none | URL for `http`; POSTs `{"target","old_primary"}`. |
| `fence_command` | `[]` | argv run before promotion; non-zero exit aborts. |
| `fence_timeout_secs` | `30` | Bound on `fence_command`. |

Probes and `pg_promote` use `[topology] user/password/database`. Status:
`GET /api/promotion`. Nano has no fenced promote of its own; use `method = "command"`
with your own tooling.

---

## Nodes (`[[nodes]]`)

One entry per backend. At least one node with `role = "primary"` is required.

```toml
[[nodes]]
host = "db-primary.internal"
port = 5432
http_port = 8080
role = "primary"
weight = 100
enabled = true
name = "primary-1"

[[nodes]]
host = "db-standby-1.internal"
port = 5432
role = "standby"
weight = 100
enabled = true
name = "standby-1"
```

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `host` | string | — | Yes | Backend hostname or IP. |
| `port` | u16 | — | Yes | PostgreSQL-protocol port. |
| `http_port` | u16 | `8080` | No | HTTP API port on the backend node (SQL API forwarding). |
| `role` | string | — | Yes | `primary`, `standby`, or `replica`. |
| `weight` | u32 | — | Yes* | Load-balancing weight. |
| `synchronous` | bool | `false` | No | The standby replicates synchronously (it is listed in the primary's `synchronous_standby_names`). Only such standbys serve reads with `[load_balancer] standby_reads = "sync"`. |
| `max_connections` | usize | `0` | No | Most backend connections this proxy keeps to the node at once, in use and parked idle alike (H-05); `0` = unlimited. A full node first gives up one idle pooled connection, then waits up to `[limits] backend_slot_wait_ms`, then refuses the request with SQLSTATE 53300 (the node is not marked unhealthy). Health and topology probes are not counted: keep the backend's own `max_connections` above the sum of every proxy's caps plus probes and admins. |
| `enabled` | bool | — | Yes* | Whether the node is routable. Toggleable at runtime via the admin API. |
| `name` | string | *(none)* | No | Human-readable name for logs/metrics/admin. |

\* `weight` and `enabled` have no serde default — supply them explicitly per node.

### Node Roles

| Role | Description |
|------|-------------|
| `primary` | Read/write node. All writes and transaction-control statements route here. At least one required. |
| `standby` | Receives side-effect-free reads when `read_write_split` is on (see [Read offload](#read-offload) and `standby_reads`). Becomes a write destination only after external promotion and discovery by an authoritative provider, or an operator updates its configured role to `primary`. The proxy does not promote it. |
| `replica` | Read-only replica. Not promotable; receives reads only. |

---

## TLS (`[tls]`)

Optional TLS termination for client connections. Omit the whole section to disable.
A `SIGHUP` reloads this section and the files it names: to rotate the certificate,
replace `cert_path` / `key_path` on disk and send `SIGHUP`; every new connection then
handshakes with the new certificate, established sessions keep the one they started
with, and a certificate or key that fails to load keeps the running TLS configuration
(logged as an error) rather than dropping to plaintext. Enabling or disabling the
section on reload takes effect for new connections the same way.

```toml
[tls]
enabled = true
cert_path = "/etc/heliosproxy/server.crt"
key_path = "/etc/heliosproxy/server.key"
ca_path = "/etc/heliosproxy/ca.crt"
require_client_cert = false
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | — | Enable TLS for client-facing connections. |
| `cert_path` | string | — | PEM server certificate path. |
| `key_path` | string | — | PEM private key path. |
| `ca_path` | string | *(none)* | CA cert for client-certificate verification. |
| `require_client_cert` | bool | — | Require a valid client certificate. |

---

## Query Cache (`[cache]`)

In-process query-result cache. Only active with the `query-cache` feature and
`enabled = true`. Two tiers are always on: L1 (hot; per connection, keyed by the exact
text) and L2 (warm; shared, keyed by the statement's fingerprint — comments and
whitespace removed, literals kept in order, every word kept as written: HeliosDB-Nano
labels an output column as written, so `SELECT Name …` and `SELECT name …` return
different column names and never share an entry, and in a `client_encoding` such as SJIS
or BIG5 a letter byte can belong to a multi-byte character). Where HeliosDB-Nano reads a
statement otherwise than PostgreSQL, the fingerprint keeps the text as written: every
space and comment of a statement whose `AS OF` / `VERSIONS BETWEEN`, `SECURITY DEFINER` / `SECURITY INVOKER` or
`AS IDENTITY (…)` rewrite depends on them (`SELECT v AS of FROM t` loses its `FROM t`
there, `SELECT v AS  of FROM t` does not; `FROM xsecurity definer` reads the table `x`,
`FROM xsecurity  definer` the table `xsecurity`), of one with a string literal, dollar
quote, quoted identifier or comment holding one of those texts (HeliosDB-Nano's planner
and HeliosDB-Lite search the raw statement for `AS OF` and cut the clause up to a keyword
spelled with one space; `/* was offline */` holds `AS OF`), and of one naming a catalog
HeliosDB-Nano answers by matching the raw text (any `pg_…` or `information_schema` text:
its `pg_settings` and `pg_tables` read `name IN ('x')` and `name IN  ('x')`
differently). Whitespace is PostgreSQL 16's: `\v` included. **On a HeliosDB backend
the L2 key keeps every run of whitespace and every comment of every statement** (only
literal values are folded, and a dollar quote keeps its delimiters, so two texts share a
key only when they differ in literals alone), and so does the `[edge]` key: once a backend connection's startup reports a
`server_version` naming HeliosDB (HeliosDB-Nano sends `16.0 (HeliosDB Nano x.y.z)`,
HeliosDB-Lite `17.0 (HeliosDB-Lite 2.0)`), a process-wide sticky flag switches both keys
and retires every entry stored before; it is logged once. Those engines read comments,
spacing and case before they parse: HeliosDB-Lite's `SIMILAR TO` and `COLLATE` rewrites
take a quote inside a comment for the start of a literal, HeliosDB-Nano's point-lookup
fast path declines any statement holding a comment and looks its table up as written
while its planner folds the name, and its empty-projection rewrite matches `select from `
with single spaces only. The flag is set before the connection answers its first read,
so no entry from such a backend is keyed the PostgreSQL way. The L3 semantic tier is
opt-in, per key and per statement ([below](#semantic-tier-l3)).

What is cached: single-statement reads that **start with** `SELECT` (a leading comment
makes a read uncacheable; see [Cache hints](#cache-hints) for leading hints), outside an
explicit transaction, without `FOR UPDATE`/`FOR SHARE`, `SELECT … INTO` or volatile
functions (`now()`, `random()`, `nextval()`, `set_config()`, …). `VALUES`, `TABLE` and
`WITH … SELECT` are not cached. Writes and DDL invalidate. Only the **simple query
protocol** is served from or stored in the cache: extended-protocol reads (prepared
statements; asyncpg; psycopg 3 with parameters) always reach the backend. A result a
standby served is never stored.

Never looked up, captured or stored by **any tier** (L1, L2, L3 and `[edge]`):

- a read that may name a **system catalog** relation: a word starting with `pg_` or
  `information_schema`, in any case, anywhere in the text — the schema `pg_catalog`, an
  unqualified `pg_settings` or `pg_stat_activity`, `'pg_class'::regclass`, a quoted
  `"pg_class"`, a comment mentioning one, one after a Unicode space (U+00A0, U+3000),
  which HeliosDB-Nano and HeliosDB-Lite read as whitespace — and any read with a
  Unicode-escaped quoted identifier (`U&"…"`, which can spell `pg_settings` without its
  bytes: `U&"\0070g_settings"`). Fail-closed: a function such as `pg_backend_pid()` or
  a column named `pg_x` only costs the hit. Such reads depend on
  the session (`pg_settings` reflects `SET`), change with DDL no write through the proxy
  announces, and HeliosDB-Nano answers several from a router that matches the raw text.
  Counted in `heliosdb_proxy_cache_catalog_skipped_total`. **HeliosDB-Nano's own live
  system views** are not PostgreSQL catalogs and are not recognised:
  `helios_topology`, `helios_ha_status`, `helios_node_aliases`, `helios_sync_status`,
  `helios_query_history`, `helios_table_memory_stats`, `helios_transaction_stats`,
  `heliosdb_config`, `heliosdb_compression_stats`, `heliosdb_compression_events`,
  `heliosdb_pattern_stats`, `heliosdb_art_indexes`, `heliosdb_simd_capabilities`. No write
  through the proxy changes them (a failover or the engine does), so a cached read of
  one would be served stale until its TTL: on HeliosDB-Nano, list the ones your clients
  read in `exclude_tables`. Nor are its code-graph and knowledge-graph tables, which the
  engine fills itself and reads under two names (`_hdb_code_symbols` and
  `_hdb_code.symbols`): list the names of both spellings, `_hdb_code_symbols` and
  `symbols` ([Exclusions and per-table TTL](#exclusions-and-per-table-ttl)), and expect a
  write through one spelling not to retire a cached read through the other;
- a read with a **comment holding `AS OF` or `VERSIONS BETWEEN`**, with any run of
  whitespace between the words, in any ASCII case (`/* as  of now */`, `-- was offline`):
  the keys leave comments out, while HeliosDB-Nano and HeliosDB-Lite read a time-travel
  clause in one (Lite cuts it out of the raw text across the end of the comment). Counted
  in `heliosdb_proxy_cache_time_travel_skipped_total`;
- a read whose text **PostgreSQL and HeliosDB-Nano/Lite read differently**: a backslash
  right before a quote in a plain `'…'` literal (`'C:\'`, `E'\''`: the server's
  `standard_conforming_strings`, which the proxy does not track, ends it in different
  places), a nested `/*` in a block comment (PostgreSQL nests them, HeliosDB-Nano ends
  one at its first `*/`), a `--` comment ended by a lone `\r` (PostgreSQL ends it there,
  HeliosDB-Nano at `\n`), a dollar quote holding a quote or a comment marker
  (HeliosDB-Nano strips comments without knowing dollar quotes), a `$` where
  HeliosDB-Nano and HeliosDB-Lite open a dollar quote and PostgreSQL does not (inside a
  name, `a$$` or `a$b$`, or before a tag starting with a digit, `$1$`: PostgreSQL reads
  a name or a parameter) or the reverse (a tag holding a non-ASCII byte, `$é$`, which
  HeliosDB-Nano's statement splitter does not know), a comment holding a `;` or a `$`, or
  leaving a quote open (`-- it's`: a `'` or `"` that no later one of its kind closes
  inside the comment), when a `;` other than the text's final one comes after its start
  (HeliosDB-Lite's statement splitter knows no comments: it runs the DELETE of
  `SELECT v FROM a -- ; DELETE FROM b` and of `SELECT 1 -- it's` newline
  `, 'a; DELETE FROM b; --'`; a comment whose quotes all close, such as a sqlcommenter tag
  `/*controller='users',action='index'*/`, leaves it where PostgreSQL is), an
  unterminated literal, identifier or comment, or a quoted literal where the table
  extraction reads a table name (below): `SELECT v FROM public.'kv'` (or `FROM 'kv'`)
  reads `kv` on HeliosDB-Nano and HeliosDB-Lite, whose parser takes a `'…'` string
  there for a name, and fails on PostgreSQL. The proxy does not tell a function's `FROM`
  from a table's, so a literal right after `FROM` inside a function call counts too
  (`substring(s FROM 'x')`, `EXTRACT(year FROM '2024-01-01'::date)`,
  `trim(BOTH FROM ' x ')`; a column or a `$n` parameter there does not), and neither
  the `FROM` of `IS [NOT] DISTINCT FROM` (HeliosDB-Nano and HeliosDB-Lite may take `is`
  for a column and `DISTINCT` for its alias: `SELECT kv.is DISTINCT FROM 'kv'` reads
  `kv`), so a literal after it counts too (`WHERE status IS DISTINCT FROM 'done'`); so
  does one right after a number running into a word, which they read as a number and a
  word (`SELECT 1from 'kv'` reads `kv`). Such text is rare, and the proxy does not try to read it every way: it is
  never cached, counted in
  `heliosdb_proxy_cache_ambiguous_text_skipped_total` (JSON `cache_ambiguous_skipped`),
  and as a write it invalidates the whole cache (below).

```toml
[cache]
enabled = true
ttl_secs = 300
max_result_bytes = 1048576
# keep these tables out of both result caches (this one and [edge])
exclude_tables = ["payroll"]
# opt-in semantic tier; "verbatim" = the same statement text only
semantic_enabled = false
semantic_match = "verbatim"
semantic_similarity_threshold = 1.0

[[cache.tables]]
name = "fx_rates"
ttl_secs = 10
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Serve read SELECT results from the L1/L2 cache. |
| `ttl_secs` | u64 | `300` | Time-to-live for cached results. A client `cache_ttl` hint can only shorten it; a `[[cache.tables]]` TTL can shorten or lengthen it (below). |
| `max_result_bytes` | usize | `1048576` | Largest single result to cache; larger results bypass. |
| `max_cacheable_response_bytes` | usize | `4194304` | Ceiling on the response the proxy buffers while streaming a cacheable read to the client, shared by this cache and `[edge]`; past it the capture is dropped (the client still receives everything) and `heliosdb_proxy_cache_capture_oversize_total` counts it. Applies even with `enabled = false` while `[edge]` caches reads. Must be ≥ 1. |
| `invalidation_source` | string | `"proxy"` | `proxy`: only writes through this proxy invalidate (below). `logical`: also every committed change decoded from a `pgoutput` logical replication slot on the primary — writes from other proxies, direct connections, triggers and cron jobs (C-02 slice 2). |
| `logical_dsn` | string | `""` | *(logical)* libpq-style `host=... port=... user=... password=... dbname=...` of a role with `REPLICATION` on the primary; its own connection, separate from data-path credentials. Required for `logical`. |
| `logical_slot` | string | `"heliosdb_proxy_cache"` | *(logical)* Slot name; created with the `pgoutput` plugin if missing. Lower-case identifier. **One slot per proxy instance**: reading a slot consumes its changes, so two proxies sharing one would each see only part of them. |
| `logical_publication` | string | `"heliosdb_proxy_cache"` | *(logical)* Publication the slot decodes. Must exist: `CREATE PUBLICATION heliosdb_proxy_cache FOR ALL TABLES`. |
| `logical_feedback_ms` | u64 | `1000` | *(logical)* Milliseconds between polls of the slot: how long a write that bypassed the proxy can still be served from cache. |
| `logical_max_changes_per_poll` | u64 | `10000` | *(logical)* Changes read per poll (whole transactions); a full batch is followed by an immediate next poll. |
| `logical_max_slot_lag_bytes` | u64 | `1073741824` | *(logical)* A slot lagging more than this behind the primary's WAL invalidates the whole cache and logs an error (an unread slot also retains WAL on the primary). |
| `l2_shed_batch` | usize | `1024` | When L2 is full and writes have retired entries, one eviction pass sheds them on the `cache-reclaim` thread, this many per step (`0` = all at once). |
| `l2_shed_pause_us` | u64 | `2000` | Pause between those steps, microseconds (`0` = none). After a write retires a whole working set (10^5-10^6 entries) the shed then contends with concurrent readers at a lower rate, for longer, instead of at full rate for 3-5 s. |
| `exclude_tables` | list of strings | `[]` | Tables whose reads neither result cache (this one and `[edge]`) looks up, captures or stores. Bare identifiers (`[a-z_][a-z0-9_]*`, case-insensitive, at most 63 bytes, no schema or quotes). On HeliosDB-Nano, list its live system views that clients read (`helios_ha_status`, `helios_topology`, …, above). See [Exclusions and per-table TTL](#exclusions-and-per-table-ttl). |
| `[[cache.tables]]` `name`, `ttl_secs`, `exclude` | table array | none | Per-table settings: `name` (required, same rule, unique) and exactly one of `ttl_secs` (≥ 1) or `exclude = true` (same as listing the table in `exclude_tables`). `exclude_columns` is refused at load, here or flat under `[cache]`. |
| `semantic_enabled` | bool | `false` | Turn on the L3 semantic tier (with `enabled = true`; alone it only logs a warning). Used only by statements that carry `/*helios:cache=semantic*/` (also `semantic`, `cache=l3`, `cache=vector`), after L1 and L2 miss, and only by single-table SELECTs of its [grammar](#semantic-tier-l3); any other statement uses L1/L2. Under `strict_config`, a build without `query-cache` refuses it. |
| `semantic_match` | string | `"verbatim"` | How the semantic tier matches a read. `"verbatim"`: an entry stored for the same statement text as the backend receives it, hint comments included (still only statements of the [grammar](#semantic-tier-l3)); sound on every backend. `"canonical"`: the canonical form (hint comments, whitespace, keyword case, `!=`/`<>`, the relation alias and `AS` fold; names keep their case), **PostgreSQL semantics only**: once a backend reports a HeliosDB `server_version` the tier neither stores nor serves (counted in `semantic_ineligible`) and one warning names this key and the node. Any other value is a startup error. |
| `semantic_similarity_threshold` | f64 | `1.0` | `1.0`: exact matches only; the embedding endpoint is **never contacted**. Below 1.0 (only with `semantic_match = "canonical"`; with `"verbatim"` a startup error names both keys), a bare select-list column of a statement without `DISTINCT` or `GROUP BY` may be served from an entry listing another column at the same position whose name embeds at or above this cosine (an approximate hit). **Caution: such a hit returns the values of a different column at that position** (the stored statement's rows, never other rows), under the stored column's name. Must be finite, > 0 and ≤ 1. |
| `semantic_max_entries` | usize | `5000` | Maximum semantic entries (the whole tier), and remembered column-name embeddings; entries are evicted oldest-inserted first within each of the tier's shards, so only approximately in insertion order across the tier ([bounds and eviction](#semantic-tier-l3)). It also sets the shard count: `semantic_max_entries / 32` rounded down to a power of two, 1 to 64 (64 at the default). ≥ 1. |
| `semantic_max_variants` | usize | `16` | Maximum entries of one statement shape that differ only in their bare select-list columns (a full shape replaces its oldest). Bounds the cost of a lookup. 1–1024. |
| `semantic_max_bytes` | usize | `67108864` | Byte budget for semantic entries plus remembered embeddings. Embeddings take at most a quarter of it and yield first (least recently used first); entries are evicted oldest first (within a shard, as for `semantic_max_entries`) only while they alone exceed it, never to make room for embeddings. A result whose entry alone exceeds it (the result plus the entry's statement and bookkeeping) is not stored in the tier — it uses L1/L2 only, nothing is evicted for it, and it is counted in `semantic_too_large`; startup logs a warning when this is below `max_result_bytes`. `0` = no byte budget (`semantic_max_entries` still applies to both). |
| `semantic_ttl_secs` | u64 | `3600` | Upper bound on a semantic entry's lifetime: min(hint TTL, table TTL or `ttl_secs`, this). ≥ 1. |
| `semantic_embedding_endpoint` | string | `"http://localhost:11434"` | Base URL of an Ollama-compatible embedding service (`POST {url}/api/embeddings` with `{"model","prompt"}`). http or https with a host, no user, password, query or fragment, no leading or trailing whitespace and no control characters; a trailing `/` is ignored; system HTTP proxies are not used. It only ever receives column names, in the background. |
| `semantic_embedding_model` | string | `"all-minilm"` | Model name sent with each request. Non-empty, no control characters. |
| `semantic_embedding_dim` | usize | `384` | Expected embedding length. A response of another length, or larger than `dim × 32 + 4096` bytes, is an endpoint failure (it is never padded). 1–65536. |
| `semantic_embedding_timeout_ms` | u64 | `5000` | Total time limit (DNS, connect, body) of one background embedding call; it bounds how long a permit is held, reads never wait on it. 1–60000. |
| `semantic_embedding_concurrency` | usize | `4` | Background embedding calls in flight at most. When none is free the call is skipped (`semantic_embed_skipped`), nothing waits. 1–256. |
| `semantic_embedding_retry_secs` | u64 | `30` | After an endpoint failure (error, timeout, non-2xx, bad JSON, wrong dimension, oversized body) no embedding call is made for this long; then exactly one call probes the endpoint. Exact canonical hits continue meanwhile. 1–86400. |
| `semantic_max_statement_bytes` | usize | `16384` | Longest statement the semantic tier's grammar is parsed for. A longer one is not parsed at all (parsing takes about a hundred times the statement's size, transiently): it never uses the tier (counted in `semantic_ineligible` when hinted) and a `[[cache.tables]]` TTL longer than `ttl_secs` never applies to it (a shorter one still does). Applies whether or not the tier is enabled. Range 1..=1048576 (1 MiB): a statement at the bound parses in about 100 MiB on the connection's task, so the bound stays far below `[limits] max_pending_bytes`. |

Every key is validated at startup whether or not its tier is enabled, and an error names
the key. **`[cache]` is applied at startup only:** a SIGHUP that changes it keeps the
running section and logs a warning (restart to apply) — except
`max_cacheable_response_bytes`, which every read takes from the reloaded config, on
connections opened before the reload too (both the capture ceiling and the `[edge]`
cache's per-entry ceiling; entries already stored stay).

**Invalidation is commit-aware on both protocols (C-02).** Every cached result records
the write generation of the tables it read, taken before its backend fetch, and every
hit — L1, L2 or L3 — is served only while no write to those tables has happened since.
Writes are recognised from what the backend actually executed (the same capture that
feeds the transaction journal), on the simple and the extended protocol alike:

- a successful autocommit write moves its tables' generation before the
  `ReadyForQuery` that acknowledges it reaches the client, so no read that starts
  afterwards can hit an older entry;
- inside an explicit transaction the tables are staged (other sessions cannot see the
  rows yet, and a session in a transaction is never served from the cache) and moved
  when the backend reports the commit, before the client sees it, so an entry a
  concurrent reader refilled from pre-commit data is refused; a rollback drops the
  stage;
- a successful `COMMIT PREPARED` invalidates the whole cache before its
  completion reaches the client, even when another session prepared the
  transaction or the client uses Execute/Flush without Sync; `PREPARE
  TRANSACTION` and `ROLLBACK PREPARED` do not publish changes;
- a result whose tables were written while it was being fetched is not stored;
- a write whose tables cannot be read from its text — DDL, `TRUNCATE`, `COPY`,
  `EXECUTE`, `CALL`, `DO`, a `MERGE` other than PostgreSQL's `MERGE INTO`
  (HeliosDB-Nano's `MERGE [DATABASE] BRANCH src INTO dst` changes every table of the
  branch it merges into; `[edge]` flushes on it too), an `INSERT` without `INTO`
  (HeliosDB-Nano and HeliosDB-Lite accept `INSERT orders SELECT …` and write `orders`;
  `[edge]` flushes on it too), a write whose text PostgreSQL and HeliosDB-Nano/Lite read
  differently (the backend-ambiguous text listed above, anywhere in the statement: after
  a leading `-- v2` and a lone `\r`, PostgreSQL runs the `CREATE OR REPLACE VIEW …`
  HeliosDB-Nano reads as a comment; `[edge]` flushes on it too), a statement, read or
  write, whose first word follows a leading comment the backends end in different places
  (`/* purge /tmp/* entries */ DELETE FROM sessions …` is an unterminated comment for
  PostgreSQL and a DELETE for HeliosDB-Nano: the capture journals it as a statement it
  cannot reproduce, so committed-history replay refuses its transaction; `[edge]`
  flushes on it too), a statement whose first word holds a non-ASCII character other
  than a space (HeliosDB-Lite matches keywords upper-cased the Unicode way and runs
  `ınsert ınto kv …`, with a dotless `ı`, as an INSERT: the capture journals it as a
  statement it cannot reproduce; `[edge]` flushes on it too), a write naming, where the
  table extraction reads a table name — after `FROM`, `JOIN`, `INTO`, `UPDATE`, `TABLE`,
  `COPY`, `INSERT` or `ONLY` (or a `(` after one), or after the dot of a name there, but
  not after the `FROM` of `IS [NOT] DISTINCT FROM`, which starts an expression — a
  quoted identifier (`INSERT INTO "app_book" …`, as Django and Rails write every name;
  `UPDATE s."t" …`; a `FROM "t"` in a subquery counts too, since a data-modifying CTE's
  `DELETE FROM "t"` reads the same) or a name with a `$` or a non-ASCII character
  other than a space (`t$x`, `café`), which the table extraction cannot read whole, a
  quoted literal where it reads the name of a table the write may change — after
  `UPDATE`, `INTO`, `TABLE`, `COPY`, `INSERT`, `ONLY` or `JOIN`, after a number running
  into a word (`1into`, which HeliosDB-Nano and HeliosDB-Lite read as `1` and `INTO`),
  or after any `FROM` once a `DELETE` came before it (`UPDATE public.'kv' …`, a write of
  `kv` on HeliosDB-Nano and HeliosDB-Lite; `DELETE x FROM 'kv' …`, which they run as a
  delete from `kv`, ignoring the names between), a Unicode-escaped identifier (`U&"t"`)
  anywhere, or anywhere a non-ASCII word whose upper case (the Unicode way) is one of
  the keywords of sqlparser 0.53, which HeliosDB-Lite then reads as that keyword
  (`ınsert ınto kv` in a CTE, `ſet`, `ﬁlter` with a `ﬁ` ligature), or, for a word
  starting with a digit, any ASCII text (`1ınsert` is `1` and `INSERT` there)
  (`[edge]` flushes on all of these too; a quoted or non-ASCII column,
  conflict target or alias, as in `UPDATE accounts SET "lastLogin" = now() …`,
  `UPDATE kunden SET größe = 3 …` or `UPDATE kunden SET straße = 'x' …`, and a literal
  value name no table and leave the write's tables read: so does a literal after any
  other `FROM` in a write without a `DELETE`, a table the write only reads, as in
  `UPDATE t SET s = substring(s FROM 'x')`, and one after the `FROM` of
  `IS [NOT] DISTINCT FROM` in any write, as in
  `UPDATE jobs SET status = 'done' WHERE … AND status IS DISTINCT FROM 'done'`), an `UPDATE` with
  sqlparser's conflict clause or an `INSERT` with Hive's `LOCAL` after `INTO`
  (`UPDATE OR IGNORE kv …`, `UPDATE REPLACE kv …`, `INSERT INTO LOCAL kv …`, which
  HeliosDB-Nano and HeliosDB-Lite run as writes of `kv`; a table really named `replace`
  or `local` after those keywords counts too; `[edge]` flushes on them as well), and a
  write of more than one statement (`UPDATE a …; TRUNCATE b`; `[edge]` flushes on it
  too) — invalidates the whole cache. Statement boundaries are the proxy lexer's: a `;`
  outside literals, quoted identifiers, dollar quotes and comments. A `;` inside one
  ends nothing and no literal's or comment's text is read as SQL, so
  `UPDATE posts SET body = 'Hi; do you know?' WHERE id = 1` retires only `posts`.
  An `INSERT` names its table whether or not `INTO`
  follows (`OR REPLACE`, `REPLACE` and `OVERWRITE` are skipped), and a Unicode space
  (U+00A0, U+3000, …) before a keyword or around a dotted name's dot separates words,
  as HeliosDB-Nano and HeliosDB-Lite read it;
- a simple-query string, read or write, with a `;` inside backend-ambiguous text
  (above) may run a statement the proxy's capture
  does not see (`SELECT E'\'' ; UPDATE b …` runs the UPDATE with
  `standard_conforming_strings` on; HeliosDB-Nano runs the DELETE of
  `SELECT 1 /* a /* b */ ; DELETE FROM b */` and of `SELECT 1 AS a$$ -- $$; DELETE FROM
  b`, HeliosDB-Lite the UPDATE of `SELECT 1 -- ; UPDATE b SET v = 2`): it invalidates
  the whole cache before its `ReadyForQuery` reaches the client, and again at the commit
  when it leaves a transaction open, counted in
  `heliosdb_proxy_cache_unseen_write_invalidations_total` (JSON
  `unseen_write_invalidations`). Nothing else does: a `;` inside a plain literal or a
  quoted identifier invalidates nothing, nor does a comment holding a quote with no `;`
  after it (`SELECT * FROM products WHERE name ILIKE '%a;b%' /*controller='search'*/`)
  or whose quotes all close (`/*application='app'*/ UPDATE t SET note = 'a;b' …` retires
  only `t`), and a `$`, a backslash or a comment marker alone is no ambiguity. The capture splits a
  string as PostgreSQL does: a `$` that continues an identifier is part of it
  (`SELECT 1 AS a$b$ ; UPDATE b …` is the write it is), a dollar tag may hold non-ASCII
  bytes and never starts with a digit, and a `--` comment ends at a lone `\r`.

Not covered with `invalidation_source = "proxy"` (the default): writes that do not pass
through this proxy (another proxy, a direct connection, a trigger or function writing a
table other than the one named in the statement). A `COMMIT PREPARED` sent through this
proxy is covered even if the transaction was prepared elsewhere; a commit bypassing this
proxy is not. Such changes become visible when the entry's `ttl_secs` expires; keep the
TTL short, or leave the cache off, for tables written that way.

**`invalidation_source = "logical"`** closes that gap. The proxy polls a `pgoutput`
logical replication slot (`pg_logical_slot_get_binary_changes`, an ordinary connection,
every `logical_feedback_ms`) and, for each committed transaction, moves the generation of
every table it wrote, whoever wrote it. Requirements: `wal_level = logical` on the
primary, the publication, and a `REPLICATION` role in `logical_dsn`; at startup the proxy
checks the publication and creates the slot if needed, and refuses to start if it cannot.
It invalidates the whole cache when it cannot account for a change: a relation it has no
name for, a lost connection or slot (changes may have been consumed and not applied; the
next successful attach invalidates everything once), or a slot lagging beyond
`logical_max_slot_lag_bytes`. Staleness from a write that bypasses the proxy is then
bounded by about one `logical_feedback_ms` plus the poll, not by `ttl_secs`. Limits: the
slot lives on the node `logical_dsn` names — after a failover point it at the new
primary (until then the source keeps reattaching and every reattach invalidates
everything); and an unread slot retains WAL, so remove it (`SELECT
pg_drop_replication_slot('heliosdb_proxy_cache')`) when you turn the source off.
Status: `/metrics` (`query_cache.logical_source`) and
`heliosdb_proxy_cache_logical_*` on `/metrics/prometheus`.

**Cache identity is the SQL, the database and the user — not session state.** This holds
for L1, L2 and L3 alike (the daemon never sets a branch). Two sessions of the same user
that differ only in `SET ROLE`, `search_path`, or a custom setting read by
row-level-security policies (for example `app.tenant_id` set per request on a shared
connection user) can be served each other's cached result. Leave the cache off for
workloads whose results depend on such session state. The proxy's own `[multi_tenancy]`
filtering rewrites the SQL per tenant before the cache lookup, so tenants do not share
entries, provided the filter lands in the statement: write cache hints as block comments
(see "Cache hints" below). (Tracked as audit item C-06.)

### Semantic tier (L3)

Opt-in twice: `semantic_enabled = true` turns the tier on, and only reads that carry a
`cache=semantic` hint use it, after L1 and L2 miss. The tier accepts only statements of
one small grammar — an allow-list, not a list of refused shapes. **A statement outside it
simply uses L1/L2** (exact text, and the fingerprint with literals in place); it is counted
in `semantic_ineligible`, never an error. The accepted statements read one table:

```text
SELECT [ALL | DISTINCT] <select list> FROM <relation> [WHERE <condition>]
  [GROUP BY <column>, …] [ORDER BY <column> [ASC | DESC] [NULLS FIRST | LAST], …]
  [LIMIT <digits>] [OFFSET <digits>] [;]
```

- **select list**: `*`, or items that are each a column or one of `count(*)`,
  `count|sum|min|max|avg([DISTINCT] <column>)`, with an optional output alias
  (`[AS] <name>`);
- **relation**: `<table>` or `<schema>.<table>`, with an optional alias (`[AS] <alias>`);
- **column**: `<name>` or `<qualifier>.<name>`, where the qualifier is the relation's
  alias (or, without an alias, the table's own name);
- **condition**: comparisons joined by `AND`/`OR`, each optionally preceded by `NOT`, in
  parentheses as needed; a comparison is `<column> <op> <literal or column>` (`=`, `<>`,
  `!=`, `<`, `<=`, `>`, `>=`), `<column> IS [NOT] NULL|TRUE|FALSE`, `<column> [NOT] IN
  (<literal>, …)`, `<column> [NOT] BETWEEN <literal> AND <literal>`, or `<column> [NOT]
  LIKE|ILIKE '<pattern>'`;
- **literal**: a number (`42`, `-1.5`, `.5`, `1e3`; no `_` separators), a plain `'…'`
  string (no `E`/`B`/`X`/`U&`/`N` prefix, no backslash), `TRUE`, `FALSE`, `NULL`;
- **names**: unquoted, ASCII (a name with a non-ASCII byte is refused: the proxy does not
  know the session's `client_encoding`, in which case folding could change a multi-byte
  character), at most 63 bytes, and not a PostgreSQL reserved word or a word of this
  grammar (`count`, `first`, `nulls`, …); a qualifier is spelled exactly like the alias
  (or the table) in case too;
- **comments**: only `helios:` hints made of words and `key=value` pieces
  (`/*helios:cache=semantic,cache_ttl=30*/`); any other comment makes the statement
  ineligible;
- **length**: at most `semantic_max_statement_bytes` (16 KiB by default); a longer
  statement is not parsed.

Refused, among others: quoted identifiers, `::` casts, any function other than the five
aggregates, `CASE`, arithmetic and string operators, `$n` parameters, dollar quotes,
prefixed strings, subqueries, joins and comma lists in `FROM`, `ONLY`, `TABLESAMPLE`,
`LATERAL`, `UNION`/`INTERSECT`/`EXCEPT`, `HAVING`, `WINDOW`, `FETCH`, `FOR UPDATE`/`SHARE`,
`GROUP BY ALL`/`DISTINCT`, ordinals in `ORDER BY`/`GROUP BY` (`ORDER BY 1`), `COLLATE`,
`OPERATOR(…)`, three-part names (`s.t.c`), a column named like the table or its alias
(PostgreSQL reads that as the whole row), an unqualified `GROUP BY` or `ORDER BY` name
equal to an output alias (in any case), a qualifier spelled otherwise than the alias or
the table (`SELECT T.id FROM kv t`), and a relation the table extraction did not find (a
write could not invalidate it). Also refused, because HeliosDB-Nano refuses or rewrites
them while their canonical form is that of a statement it runs: a relation alias without
`AS` that its SQL parser reserves (`FROM kv sort`; `sort`, `view`, `set`, `start`,
`format`, `global`, `partition`, `top`, `explain`, … — with `AS` any name works), the
names `exists`, `interval`, `struct`, `trim` and `top` (it cannot read `trim.val`), a
comment it ends elsewhere than PostgreSQL (a nested block comment, a `--` comment a lone
`\r` ends), and a statement one of its spacing-exact text rewrites may fire on: its
`AS OF` / `VERSIONS BETWEEN` rewrite, its removal of the first `SECURITY DEFINER` /
`SECURITY INVOKER` (`FROM xsecurity definer` reads the table `x` there) and its removal of
the list after `AS IDENTITY`. That is two words that end in `as` and start with `of` or
`identity`, end in `versions` and start with `between`, or end in `security` and start
with `definer` or `invoker`, or a string literal holding `AS OF`, `VERSIONS BETWEEN`,
`SECURITY DEFINER`, `SECURITY INVOKER` or `AS IDENTITY` in any case. A statement naming
a catalog it emulates — any `pg_…` or `information_schema` text, a table such as
`pg_settings`, `pg_tables` or `pg_catalog.pg_class`, or that text in a literal — is
refused (and a read naming a system catalog is cached by no tier at all, above): it answers those from a router that matches the raw text, spacing included
(`WHERE name='work_mem'` keeps every `pg_settings` row, `WHERE name = 'work_mem'` one;
`tablename='kv'` answers `pg_tables` with eight columns, `tablename = 'kv'` with five).
And a comment other than a plain hint is refused because HeliosDB-Nano and
HeliosDB-Lite read comments as text: Nano's planner searches the raw statement for
`AS OF` (`/* was offline */` holds it, and the statement fails), Lite cuts an `AS OF`
clause out of the raw text across the end of a comment (`-- as of now`, a newline,
`WHERE id = 1` loses its `WHERE`), and Lite's `SIMILAR TO` and `COLLATE` rewrites track
quotes without knowing comments (a quote in one flips what they take for a literal).

**How a read matches an entry is `semantic_match`**; neither mode needs an embedding
service.

- **`"verbatim"` (the default)**: the entry stored for the same statement text as the
  backend receives it, byte for byte, hint comments included (removing a hint would glue
  the words around it — `FROM t/*helios:…*/x` reads the table `t` aliased `x`, `FROM tx`
  another table — and HeliosDB-Nano reads a statement with a comment through another
  path than the same text without one). Case, whitespace, aliases, literals and the
  hint's own text all count. The grammar above still decides eligibility, so every
  entry reads one relation that writes retire exactly. Approximate substitution is not
  available (a threshold below 1.0 is a startup error). Sound on every backend.
- **`"canonical"` (PostgreSQL semantics only)**: the canonical form below. HeliosDB-Nano
  and HeliosDB-Lite re-read the raw text, so statements PostgreSQL reads alike may read
  differently there (an alias decides between Nano's point-lookup fast path and its
  planner, which fold names differently; Lite's `SIMILAR TO` rewrite reads `dissimilar
  tolerance` and `dissimilar  tolerance` differently). Once a backend connection's
  startup reports a HeliosDB `server_version`, the tier neither stores nor serves (every
  hinted read counts in `semantic_ineligible` and uses L1/L2), every entry stored before
  is retired, and one warning per process names `cache.semantic_match` and the node.

With `"canonical"`, a read is served from an entry with the same **canonical form**. Hint
comments, whitespace, keyword case, `!=` vs `<>`, the relation's alias and `AS` in
`FROM`, and which alias a qualifier uses do not matter:
`select val from kv where id = 1` and `SELECT val FROM kv AS t WHERE id = 1` are one
entry, and so are `SELECT t.val FROM kv AS t WHERE t.id = 1` and
`SELECT kv.val FROM kv WHERE kv.id = 1`. **Names keep their case**: the table, its
schema, every column and every output alias is compared as written. PostgreSQL folds
unquoted names, but HeliosDB-Lite never does (`Orders` and `orders` are two tables
there, `Name` and `name` two columns), HeliosDB-Nano's point lookups find a table as
written, and Nano reports an output column's label (its alias, or else its column's
name) as written in the RowDescription. So `SELECT val FROM KV …` and
`SELECT val FROM kv …`, `SELECT VAL …` and `SELECT val …`, or `… AS Total` and
`… AS total` never serve each other exactly; case variants only cost hit rate (an
aggregate is labelled by its function's name, which every backend folds: `COUNT(*)` and
`count(*)` are one entry). Below a threshold of 1.0 a bare column spelled in another
case is at most an approximate hit, with the stored label (see below).
**Whether a column is qualified does matter**:
PostgreSQL reads `t.f` as the column `f` when the table has one and otherwise as the
function call `f(t)` (attribute notation, the "computed field" pattern), while a bare `f`
is then an error, so `t.val` and `val` are never one entry. Everything else is kept as
written: the relation with its schema, the output aliases and a select-list `AS`, every
literal in place, parentheses, `ASC`/`DESC`/`NULLS` (no defaults filled in), `LIMIT` and
`OFFSET`. `id = 1` and `id = 2`, `<` and `>`, `ASC` and `DESC`, `s1.kv` and `s2.kv` never
share an entry. **`ORDER BY` and `GROUP BY` keys are compared as written**, never by what
they resolve to, since backends resolve a lone name differently: PostgreSQL sorts
`SELECT price_eur AS price FROM items ORDER BY price` by the output column `price`
(`price_eur`), HeliosDB-Nano by the table's own `price` column. Such a statement, a lone
`ORDER BY` or `GROUP BY` name equal to an output alias, is refused (it uses L1/L2);
`ORDER BY price_eur` and `ORDER BY items.price` are accepted, and `ORDER BY a` and
`ORDER BY t.a` are never one entry.

**Approximate column substitution** (only with `semantic_match = "canonical"` and
`semantic_similarity_threshold` below 1.0). In a statement without `DISTINCT` (in the select list or an aggregate) and without
`GROUP BY`, an unqualified select-list column without an output alias may be served from
an entry that lists another unqualified column at the same position, when the two column
names' embeddings have a cosine at or above the threshold,
position by position (names are compared as written: a column in another case is
another name, matched only through its embedding). A qualified column (`t.f`) is never substituted: it may
be the function call `f(t)`, which can return another number of rows (a set-returning
function) or fail where the stored column did not. Aggregates are always
compared exactly. The client then receives the **stored** statement's rows — the same
rows by construction — with the **stored column's values and name** in its
RowDescription. That is the only way the tier is approximate; approximate hits are counted
in `semantic_approximate_hits`. `1.0` (the default) switches it off entirely.

**What leaves the proxy.** Only the column names of stored results are sent to the
embedding endpoint, in clear text over http (use https, or keep the endpoint on
loopback), never literals, operators or table names. A name is embedded once a statement
listing it has returned a result and been stored; a lookup never sends the names its own
statement lists, which no backend has answered yet. So an approximate hit needs a stored
statement for each of the two names (the client's column is embedded after its first
miss, or after any other stored row that lists it). Embeddings are computed in the
background, at most
`semantic_embedding_concurrency` at a time, once per name in flight, and remembered in a
bounded LRU (at most `semantic_max_entries` names, and at most a quarter of
`semantic_max_bytes`, which the entries take first: an embedding never evicts an entry);
no lookup or store waits on the endpoint. A name is sent only when the memo has room for
every name it must be remembered with (a stored statement's columns; for a lookup, which
only fetches a stored entry's name it lacks, both names of each column it would
substitute, next to those of newer candidates): else it could not be kept, or would
evict the one it is compared with. While the entries leave the
memo too little room, no such name is sent (counted in `semantic_embed_skipped`):
**approximate matching pauses until the entries shrink**
(they expire, or writes retire them), while exact canonical hits continue. Size
`semantic_max_bytes` for the entries plus the embeddings you want remembered. A failure (error, timeout, non-2xx,
bad JSON, wrong dimension, oversized body) marks the endpoint down: no call is made for
`semantic_embedding_retry_secs`, then exactly one call at a time probes it (a probe that
outlasts the window keeps every other call off until it ends). Exact canonical hits
continue while it is down; only approximate matching pauses. At startup the daemon probes
it once (only when the threshold is below 1.0). Watch
`heliosdb_proxy_cache_semantic_endpoint_state`, `…_embed_errors_total` and
`…_embed_seconds` ([Admin API](admin-api.md)).

**Ollama** unloads an idle model; a cold load can exceed `semantic_embedding_timeout_ms`
and mark the endpoint down for a retry window. Set `OLLAMA_KEEP_ALIVE` (e.g. `-1`) on the
Ollama server, or raise the timeout.

**Bounds and eviction.** The tier's entries are split into shards by statement shape and
session context (`semantic_max_entries / 32` rounded down to a power of two, from 1 to 64;
64 at the default 5000 entries, about 78 each). A lookup or a store locks only its own
shard, and a store copies and sizes everything it keeps before it takes that lock, so
hinted reads of different statements do not wait on each other (the miss path of a
random-key workload stores on every read). `semantic_max_entries` and
`semantic_max_bytes` bound the **whole tier**: a store that takes it over either bound
evicts the oldest entries of its own shard, and only when that shard holds nothing older
than the new entry the oldest of the following shards, in turn. Eviction is therefore
oldest-inserted first within a shard and only approximately across the tier: an entry
can outlive an older one of a shard that received fewer stores (a tier below 64 entries
is one shard, an exact FIFO). The bounds hold whenever no store is in progress; while
stores run concurrently each can hold the tier one entry over them (or evict one more than
needed) for the moment it takes. With `semantic_similarity_threshold` at 1.0 a store never
touches the embedding memo.

**Cost of a hinted miss.** A hinted read that L1 and L2 miss is parsed once for the
tier's grammar, looked up, and stored in the tier when the backend's result arrives, on
top of the L1/L2 miss path. In the single-threaded `cache/l3/*` cases of
`benches/BASELINE.md` that work is about 3 µs for a short point read, most of it the
parse; with 8 threads missing and storing at once, a round takes about three times one
thread's lookup and store (`store_lookup_contended_8`). A hit saves a backend round trip,
so the hint pays off on reads that repeat or are paraphrased; on reads that never repeat
it is only overhead. End to end, on pgbench point reads of random keys, where every hinted
read missed and stored (2026-10-29: 16 and 64 clients, session and transaction pooling,
`semantic_match = "canonical"`, threshold 0.92), hinted reads ran 5–10 % fewer per second
than the same reads unhinted, and the concurrent writes stayed within the harness's noise.
Hint the reads that repeat, not every read.

Other limits: simple query protocol only; a standby-served result is never stored, so
with read offload (`[load_balancer] standby_reads`) hinted reads that a standby serves
never fill the tier; and HeliosDB-Nano through 4.41 answers a statement that starts with
a block comment with no rows — leading hints therefore only work through the
`[routing_hints]` strip, which forwards the statement without the comment.

### Exclusions and per-table TTL

`exclude_tables` and `[[cache.tables]] exclude = true` are enforced in the relay, ahead
of **both** result caches (this one and `[edge]`): a cacheable read that names an
excluded table is neither looked up, captured nor stored, and counts once in
`heliosdb_proxy_cache_excluded_total`. The check is **fail-closed**: it searches the raw
statement text, comments and literals included, for each name as a whole identifier
(case-insensitive), so `public.payroll`, `"Payroll"`, `FROM a, payroll` and
`FROM/**/payroll` are all caught. It is one pass over the statement whatever the number
of names: its cost grows with the statement's length, not with the length of the list. PostgreSQL truncates an identifier longer than 63 bytes
(clipping a multi-byte character), so a name of 60 bytes or more is also matched as the
start of a longer identifier. A name next to a character other than an ASCII letter
or digit, `_`, `$` or a letter (a Unicode space, U+00A0 or U+3000, which HeliosDB-Nano
and HeliosDB-Lite read as whitespace) is a whole identifier: `FROM`, a U+00A0 and
`payroll` is caught, `payrollé` is not. It also over-excludes: a column, an alias, a literal, a
comment or a keyword with the same name excludes the read too (a table named like an
unreserved keyword, `values` or `name`, matches every use of that keyword), and so does
any Unicode-escaped quoted identifier (`U&"…"`, which can spell a name without its
bytes). A PostgreSQL reserved or type/function-name keyword (`order`, `user`, `left`,
`table`, …) is **refused as a name at load**, in `exclude_tables` and `[[cache.tables]]`
alike: a table so named is only ever written quoted (`"order"`) or after a schema, while
the scan would match the keyword in nearly every statement (`ORDER BY`), excluding almost
every read or giving it that table's TTL. **Views and functions**
over an excluded table are not detected: list them as well. So are **partitions and
inheritance children**: a read of a parent returns its children's rows, and a read of a
child its parent's data, without naming the other; list the parent and every partition
or child. A `[[cache.tables]]` `ttl_secs` entry for a table that `exclude_tables` lists is
refused at load. **HeliosDB-Nano's `_hdb_code_<t>` and `_hdb_graph_<t>` tables** (its
code graph and knowledge graph, written by the engine itself, never by a statement
through the proxy) are also read as `_hdb_code.<t>` and `_hdb_graph.<t>`, the spelling
its `pg_tables` lists: a name may only be listed bare, and the scan matches whole
identifiers, so excluding such a table, or giving it a shorter TTL, takes both its
storage name and the bare `<t>` (`exclude_tables = ["_hdb_code_symbols", "symbols"]`;
the bare name also matches user tables called `symbols`).

`[[cache.tables]] ttl_secs` sets a table's TTL. A read of several tables uses the
shortest of their TTLs (`ttl_secs` for tables without one). A table TTL **longer** than
`ttl_secs` applies only to a read of the [semantic tier's grammar](#semantic-tier-l3)
(one relation, which the table extraction found, spelled in lower case like the
configured name — `FROM Orders` is another table than `orders` on a backend that does not
fold names — and none of the shapes it refuses, such as a lone `ORDER BY` name equal to
an output alias, a comment other than a hint, or a `pg_…` catalog) without a qualified column (`t.f` may
call a function `f(t)` that reads other tables) — whether or not the read is hinted for
the tier, and whether or not the tier is on; any other read (a join, a comma list, a
subquery, a quoted or function relation, a qualified column, a statement over
`semantic_max_statement_bytes`, …) is capped at `ttl_secs`. That check is syntactic: it
sees that the statement names one relation, not what that relation reads. **Writes that
reach a table without naming it do not retire its entries** with
`invalidation_source = "proxy"` (the default): a write to a partition or an inheritance
child (a read of the parent returns its rows), a foreign key's `ON DELETE`/`ON UPDATE`
`CASCADE`/`SET NULL`/`SET DEFAULT`, a trigger, or a **function called from a `SELECT`**
(`SELECT place_order(42, 3)`, as RPC-style applications write), which the proxy treats
as a read and never invalidates on. Likewise a **view** (a security-barrier
view, or a relation a rule rewrites, alike) returns rows that change with writes to its
base tables, and a table with a **row-level security policy** that reads other tables
(`USING (EXISTS (SELECT 1 FROM acl …))`) returns rows that change with writes to those;
a read of either never names them. Such cached reads are served until their TTL ends, so
never give such a table or view a longer TTL (with `"logical"`, cascades, triggers and a
function's writes are decoded as changes of the table itself, so give a table written
through functions a longer TTL only with `invalidation_source = "logical"`; but a
partition's change still names the partition, or with `publish_via_partition_root` the
root, never both, and a view's or a policy's tables only ever name themselves). A shorter TTL on a table does not reach a
view over it either: give the view its own `[[cache.tables]]` entry, as exclusions list
views too. A **shorter** table TTL holds fail-closed: for a
read outside that grammar the statement text is also scanned, the way exclusions are,
and every configured name it mentions anywhere (as a whole identifier, comments and
literals included, a name of 60 bytes or more also as the start of a longer identifier,
which PostgreSQL truncates to 63 bytes) applies its TTL; a Unicode-escaped identifier
(`U&"…"`) applies the shortest. L1 admits TTLs up to the longest configured. A table name
applies to that table in every schema. A client `cache_ttl` hint can only shorten the
result. **Table TTLs bind `[edge]` too**, as a cap: an edge entry lives
`[edge] default_ttl_secs`, or less when its read names a table whose `ttl_secs` is
shorter (found by the same fail-closed text scan, comments and literals included): then
that table's TTL. A longer table TTL never extends an edge entry. This holds whether or
not `[cache]` itself is enabled.

### Cache hints

Hints are SQL block comments `/*helios:key=value,…*/`, read only outside string literals
and quoted identifiers; one comment may carry several, comma-separated
(`/*helios:cache=semantic,cache_ttl=60*/`), also together with routing hints. **Do not put
cache hints next to `route=primary` or `consistency=strong`**: with `[routing_hints]` on,
those make the proxy treat the read as a write — it runs on the primary and bypasses
every cache tier, neither looked up nor stored. A result a standby serves
(`route=standby`) is never stored either. The parser
also reads a `-- helios:key=value` line comment, but do not use that form with
`[multi_tenancy]`: a line comment runs to the end of the line, so it also comments out
the tenant filter the proxy appends to a SELECT without `WHERE`. The cache acts on:

| Hint | Effect |
|---|---|
| `cache=skip` (also `bypass`, `none`, `no`, `off`, `false`, `disable`) | Neither look up nor store this read. `bypass` and `none` are the routing parser's words for it too, which strips them (below). |
| `cache_ttl=N` / `ttl=N` (seconds, or `500ms`, `5m`, `1h`, `1d`) | A shorter TTL for this result; a longer one is capped at the configured TTL. It also bounds what the read is served: an entry older than `N` (stored by an unhinted read, or under a longer hint) is a miss on every tier, and the result is fetched and stored with the shorter TTL. A zero TTL (`0`, `0ms`) means "always fresh" and acts as `cache=skip` (nothing looked up, captured or stored, so the entry unhinted reads share stays). A value too large or unparsable is ignored. |
| `cache=semantic` (also `semantic`, `cache=l3`, `cache=vector`) | Use the semantic tier (with `semantic_enabled`). |

`cache_tables`, `cache_refresh` and `cache=l1|l2|all` are parsed and have no effect.
Placement: a hint **after** the statement always works (`SELECT … /*helios:cache=skip*/`).
A **leading** hint works when `[routing_hints] enabled` (with the default `strip_hints`)
and the key is one the routing parser knows (`cache=…`, `cache_ttl=<seconds>`): the
strip forwards the statement without the comment, and the cache reads the hints from the
client's own text. Any other leading comment keeps the read uncacheable. `[edge]` ignores
cache hints.

---

## Lag-Aware Routing (`[lag_routing]`)

Replica-lag-aware routing + read-your-writes. Only enforced with the `lag-routing`
feature and `enabled = true`.

```toml
[lag_routing]
enabled = true
ryw_window_ms = 500
max_lag_bytes = 0
require_known_lag = false
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable lag-aware read routing + read-your-writes. |
| `ryw_window_ms` | u64 | `500` | Reads within this many ms of a write in the same session pin to the primary (read-your-writes). 0 disables the window. |
| `max_lag_bytes` | u64 | `0` | Exclude a standby when its measured replication lag exceeds this many bytes. 0 = no lag-based exclusion. |
| `require_known_lag` | bool | `false` | Strict freshness: when `true`, a standby whose lag was never measured is excluded instead of being treated as fresh. Lag is measured only when `[health] user` is configured and this feature is enabled. |

---

## Routing Hints (`[routing_hints]`)

SQL-comment routing hints (`/*helios:route=primary*/`). Only honored with the
`routing-hints` feature and `enabled = true`.

```toml
[routing_hints]
enabled = true
strip_hints = true
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Parse and honor `/*helios:...*/` hints; an applied hint overrides default verb routing (but never a plugin `Block`). |
| `strip_hints` | bool | `true` | Remove the hint comment from SQL before forwarding to the backend. |

---

## Rate Limiting (`[rate_limit]`)

Token-bucket + concurrency limiting. Only enforced with the `rate-limiting` feature and
`enabled = true`.

```toml
[rate_limit]
enabled = true
default_qps = 1000
default_burst = 2000
max_concurrent = 0
key_by = "user"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enforce rate limits. |
| `default_qps` | u32 | `1000` | Sustained queries/sec per bucket. |
| `default_burst` | u32 | `2000` | Token-bucket depth (burst) per bucket. |
| `max_concurrent` | u32 | `0` | Not enforced: nothing on the data path checks it (see `[multi_tenancy] max_active_queries_per_tenant` for a per-tenant concurrency cap). |
| `key_by` | string | `"user"` | Bucket key: `user`, `client_ip`, `database`, `global`. |

> The keys are `default_qps` / `default_burst`. A file that says `global_qps` /
> `global_burst` loads with a warning naming the right key and the limiter runs at its
> defaults; `strict_config = true` refuses it instead.

A refused statement (53400) never reaches the backend. Inside a transaction its
`ReadyForQuery` reports the transaction's real status (`T`, or `E` in a failed one), and unlike an error from
PostgreSQL the refusal does not abort the transaction: the backend transaction stays open,
so a `COMMIT` sent after it commits the statements that ran before the refused one. A
client that carries on after an error inside a transaction (a psql script without
`ON_ERROR_STOP`, an application that logs the error and continues) must roll back itself.
The circuit breaker's fast-fail and a plugin `Block` behave the same way.

A message that continues an extended-protocol cycle the backend already holds open —
anything the client sends after a `Flush` forwarded part of the cycle (more
`Bind`/`Execute`/`Flush` batches of a pipeline, up to the `Sync`), or after an
extended-protocol `COPY` ended, and the `Sync` or `Query` that settles it — is never
refused, by the limiter or by an open circuit, and never answered from the query or edge
cache: it belongs to a request admitted at the cycle's first forwarded batch, and only the
backend's own reply settles the cycle. (A refusal there would tell the client that the
cycle failed while the backend kept it open, to skip the next cycle or to commit the
flushed part with it.) Such a message is still charged: one that runs statements of its
own (a batch with an `Execute`, or a `Query`) takes a token like any request, and with the
bucket empty it waits for the token to refill (the limiter's retry delay, at most 5 s,
the same cap as any backpressure sleep) instead of being refused, then runs. A client
pipelining `Execute`s behind `Flush`es without a `Sync` therefore runs at the bucket's
rate. A `Sync`, `Flush`, `Describe` or `Close` alone runs no statement and costs nothing.
A bare `Flush` with no extended cycle open never reaches the limiter (or an open circuit)
at all: the proxy consumes it, as PostgreSQL answers nothing to one, so it is never
refused and never discards the request the client sends after it.

A refused `Flush`-terminated batch that starts an extended cycle is that cycle's error. As
PostgreSQL does after an error, everything the client sends after it up to the `Sync`
(more `Bind`/`Execute`/`Flush` batches, a `Query`) is discarded and never reaches the
backend, even once the bucket has refilled, so no later part of the cycle runs, or is
committed by the `Sync`, behind the error; the `Sync` is answered with `ReadyForQuery`
(the transaction's status) and is not charged. An open circuit's fast-fail of such a batch,
and the error a failover answers one with when it does not re-run it, end the cycle the
same way.

---

## Circuit Breaker (`[circuit_breaker]`)

Per-node circuit breaker. Only enforced with the `circuit-breaker` feature and
`enabled = true`.

```toml
[circuit_breaker]
enabled = true
failure_threshold = 5
open_secs = 10
success_threshold = 3
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Trip failing backends out of rotation. |
| `failure_threshold` | u32 | `5` | Consecutive failures that open a node's circuit. |
| `open_secs` | u64 | `10` | How long a circuit stays open before a half-open probe. |
| `success_threshold` | u32 | `3` | Successful probes required to close a half-open circuit. |

A fast-fail (08006 `circuit open for node ...`) never reaches the backend. Inside a
transaction it reports the transaction's status (`T`) and does not abort the transaction: a
later `COMMIT` commits the statements before it (see Rate Limiting above). A message that
continues a cycle a node already holds open (anything after a `Flush` forwarded part of the
cycle up to its `Sync`, see Rate Limiting above) is never fast-failed, executing batches of
a `Flush`-chained pipeline included: it goes to that node's connection, whose reply or
failure settles the cycle. The circuit applies again from the next request. A fast-fail of
a `Flush`-terminated batch that starts a cycle discards the rest of that cycle up to its
`Sync` (see Rate Limiting above).

---

## Operational Limits (`[limits]`)

Session/protocol safety bounds and relay timeouts. Each key was previously a
compiled-in constant; the defaults reproduce those constants exactly, so an
absent `[limits]` block is byte-for-byte unchanged. All values are resolved once
at startup. `validate()` rejects `0` for any of these (a `0` disables a safety
bound rather than meaning anything useful).

```toml
[limits]
max_cancel_keys = 100000
startup_timeout_secs = 30
backend_write_timeout_secs = 30
backend_read_timeout_secs = 30
client_write_timeout_secs = 60
reprepare_timeout_secs = 15
max_prepared_statements = 8192
max_prepared_bytes = 67108864
max_pending_bytes = 67108864
max_backend_frame_bytes = 104857600
backend_response_timeout_secs = 0
backend_cancel_timeout_ms = 5000
tr_max_observation_bytes = 1048576
max_total_idle_backend_conns = 8192
pool_reap_interval_secs = 30
replay_deadline_secs = 300
max_client_connections = 0
client_idle_timeout_secs = 0
client_admission_wait_secs = 0
client_tcp_keepalive_secs = 60
gateway_pool_max_idle = 16
tr_max_replay_statements = 1000
tr_max_replay_bytes = 4194304
tr_max_session_set_statements = 256
tr_max_tracked_savepoints = 1024
tr_max_cycle_statements = 16
latency_buckets_ms = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
tr_buffer_small_reads_bytes = 0
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_cancel_keys` | usize | `100000` | Capacity of the query-cancellation key map (`BackendKeyData` → backend address); at capacity the oldest entries are FIFO-evicted. |
| `startup_timeout_secs` | u64 | `30` | Deadline for the pre-auth startup exchange (TLS negotiation + startup/authentication); bounds slow-loris handshakes. |
| `backend_write_timeout_secs` | u64 | `30` | Timeout for a single backend write on the forward path. |
| `backend_read_timeout_secs` | u64 | `30` | Timeout for a single backend read on the relay path (paired with `backend_write_timeout_secs`; a slow-but-healthy read is not itself a fault). |
| `client_write_timeout_secs` | u64 | `60` | Timeout for a single client write, so a wedged client cannot pin a proxy task (and its backend connection) forever. |
| `reprepare_timeout_secs` | u64 | `15` | Timeout for the out-of-band re-prepare exchange performed on a backend connection switch. |
| `max_prepared_statements` | usize | `8192` | Per-session cap on distinct named prepared statements. |
| `max_prepared_bytes` | usize | `67108864` | Per-session cap on aggregate bytes retained in the statement registry (64 MiB). |
| `max_pending_bytes` | usize | `67108864` | Per-session cap on the un-flushed extended-protocol `pending` buffer (64 MiB). |
| `max_backend_frame_bytes` | usize | `104857600` | Cap on the declared length of one backend response frame on every streaming relay; a header above it or below the 4-byte minimum closes the backend as malformed (100 MiB, the frontend message cap). Bounds a frame, not a result set. |
| `backend_response_timeout_secs` | u64 | `0` | Whole-response deadline for one backend response on the streaming relays, first read to `ReadyForQuery`. `backend_read_timeout_secs` re-arms per read and cannot catch a slow drip inside one response; this can. `0` disables (prior behaviour). |
| `backend_cancel_timeout_ms` | u64 | `5000` | Bound on sending one CancelRequest to a backend: the TCP connect, the request, and the backend closing that connection once it has signalled the statement (as libpq's `PQcancel` waits for it). Covers a client's CancelRequest forwarded to the backend running its statement, and the one the proxy sends itself before it drops a backend connection on which a request it delivered may still be running: a `backend_read_timeout_secs` or `backend_response_timeout_secs` expiry, a client gone mid-response, another fault that leaves the socket open, or a session that ends (client gone, `Terminate`, an idle timeout) with an extended cycle a `Flush` dispatched, or an extended COPY's end, still open there. PostgreSQL notices a closed client socket only when it next writes to it (`client_connection_check_interval` is off by default), so without the cancel such a statement runs on with its locks for its whole duration, and outside `[multi_tenancy] max_active_queries_per_tenant`, whose permit the session returns only after the cancel was sent. The cancel stops the statement only on a backend that implements CancelRequest (PostgreSQL; PgBouncer forwards it); one that does not (HeliosDB-Nano ignores it) runs the statement to completion. Only a session whose tenant has an active-query cap waits for that cancel (up to this bound, after the fault was recorded against the node); every other session (no `[multi_tenancy]`, an unknown tenant, or one without an active-query cap) sends it in the background, so its failover, re-execution or next message never waits for a backend host that does not answer. A backend that already closed or reset the connection gets none; one that does not close the cancel connection in time is assumed to have taken the request. Must be 1..=31536000000 (1 year). Applied at startup. |
| `tr_max_observation_bytes` | usize | `1048576` | In-session TR: response bytes hashed per recorded statement into an observation digest, re-verified when the transaction is replayed; divergence rolls the replay back with `40001`. A response over the cap has no verifiable digest, so its transaction becomes non-replayable. |
| `max_total_idle_backend_conns` | usize | `8192` | Global ceiling on idle backend-pool connections across all `(node,user,db)` identities. Only consumed with the `pool-modes` feature; parsed-and-ignored otherwise. |
| `pool_reap_interval_secs` | u64 | `30` | How often the idle-connection reaper runs. |
| `replay_deadline_secs` | u64 | `300` | Overall wall-clock deadline for one operator time-window replay (`POST /api/replay`). `query_timeout` bounds each statement; this bounds the run. On expiry the replay stops at the current statement and reports `deadline_exceeded` + partial progress. `0` disables the overall deadline. Max `31536000`. |
| `max_client_connections` | usize | `0` | Ceiling on concurrently-served client connections on the PG-wire listener. `0` = unlimited. Excess connections are refused with SQLSTATE 53300 (see `client_admission_wait_secs`). Applied at startup; a SIGHUP change is ignored with a warning. |
| `client_idle_timeout_secs` | u64 | `0` | Close an authenticated idle session after this many seconds. `0` = wait forever (historical behaviour). Applied at startup. |
| `client_admission_wait_secs` | u64 | `0` | Bounded fair-admission queue (H-05): when `max_client_connections` is reached, a new connection waits up to this many seconds for a permit before being refused with 53300. `0` refuses immediately. Health/admin traffic is unaffected (health probes hold no client slot; admin is a separate listener). Applied at startup. |
| `client_tcp_keepalive_secs` | u64 | `60` | TCP keepalive on every accepted PG-wire client socket: after this many seconds without traffic from the client the kernel probes it, repeating the probe at the same interval (Linux, macOS, FreeBSD, NetBSD, Android, Windows; elsewhere the OS's interval), and once the OS's probe count (`net.ipv4.tcp_keepalive_probes`, 9 on Linux) goes unanswered the connection fails. A client that vanished without a FIN or RST (a NAT or load-balancer flow dropped, a client host frozen or powered off) then ends its session, about ten times this long after its last traffic, and what the session held returns: its `[multi_tenancy]` active-query permit (an idle transaction, advisory locks, a stalled COPY), its client slot, its backend connections. A live client's kernel answers the probes, so they never end an idle session, and they keep idle flows open through NAT and load balancers whose idle timeout is longer than this value. Unlike the keys above it has no compiled-in predecessor: before it the proxy enabled no keepalive, and such a session waited on its client until the proxy restarted; `0` restores that. Must be ≤ 32767 (the largest keepalive time and interval Linux accepts). Applied at startup; a SIGHUP change is ignored with a warning. |
| `gateway_pool_max_idle` | usize | `16` | Idle authenticated backend connections kept per identity by each non-PG-wire gateway (HTTP SQL, MCP, GraphQL) for reuse (H-06). `0` disables pooling (each gateway request dials, the pre-H-06 behaviour). Gateways have separate pools so a session policy (e.g. MCP read-only) cannot leak across interfaces. Applied at startup. |
| `tr_max_replay_statements` | usize | `1000` | In-session TR (`tr_mode = select\|transaction`): cap on statements recorded per explicit transaction for failover replay. Over the cap the transaction is marked non-replayable (`transaction` degrades to `session` for it; `tr_replay_cap_exceeded_total`). |
| `tr_max_replay_bytes` | usize | `4194304` | In-session TR: cap on bytes (statement text / raw extended-protocol frames) recorded per explicit transaction (4 MiB). Same degradation as above. |
| `backend_slot_wait_ms` | u64 | `5000` | How long a new backend connection waits for a slot on a node at its `[[nodes]] max_connections`, or for a `max_concurrent_reconnects_per_node` permit, before the request is refused with 53300 (H-05). Must be ≥ 1. Applied at startup. |
| `max_concurrent_reconnects_per_node` | usize | `16` | Most backend connections the proxy opens at once to one node on a session's behalf after its startup connection — a transaction-replay re-home, a reconnect after a lost backend, a standby connection for offloaded reads — from TCP connect through authentication (H-05). Meters the reconnect wave a failover sends to the new primary; client startup connections are bounded by `max_client_connections` and `[[nodes]] max_connections` instead. Over the limit a dial waits up to `backend_slot_wait_ms`, then the request is refused with 53300 (the node is not marked unhealthy); a transaction-replay re-home keeps retrying until its recovery deadline. A wave of N such connections takes about N × (connect + authentication time) / the limit: 1000 re-homes at 20 ms each take about 1.25 s at the default. `0` = unlimited. Applied at startup. |
| `latency_buckets_ms` | list of numbers | `[1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]` | Bucket upper bounds, in milliseconds, of every latency histogram on `/metrics` and `/metrics/prometheus` (failover reconnect, transaction replay, admission wait, journal apply, operator replay). 1 to 32 values, strictly increasing, each at least `0.001`; an implicit `+Inf` bucket follows. Applied at startup. |
| `tr_buffer_small_reads_bytes` | usize | `0` | In-session TR (`tr_mode = select\|transaction`, TR-09): hold the response of a read the proxy may re-execute until its `ReadyForQuery`, up to this many bytes, so a backend fault before then has shown the client nothing and the read is retried transparently instead of the session being closed. A response that outgrows the bound is published at that point and streams on as usual. Costs the client its time to first row (the whole held response arrives at once) and up to this many bytes per session. `0` = off (stream every response). Applied at startup. |
| `tr_max_session_set_statements` | usize | `256` | In-session TR (`tr_mode != none`): cap on DISTINCT session variables whose latest `SET` is replayed onto the replacement backend (a later `SET` of the same variable replaces the earlier one; `RESET name` removes it; `DISCARD ALL` clears all; `RESET ALL` clears all but `SET ROLE` / `SET SESSION AUTHORIZATION`, which it keeps as PostgreSQL does and which are tracked even past the cap). Over the cap tracking stops (`tr_session_set_cap_exceeded_total`) and a subsequent failover is refused with `08006` rather than re-homing the session with incomplete state. |
| `tr_max_tracked_savepoints` | usize | `1024` | In-session TR and read offload: cap on the savepoints of one transaction whose position among its deferred `SET`s is tracked, so `ROLLBACK TO SAVEPOINT` undoes the right ones (simple or extended protocol). Past the cap the records start over; a later `ROLLBACK TO` one that was dropped while `SET`s were deferred makes the session's settings unknown once the transaction commits (reads stay home, a failover is refused with `08006`). 1024 covers a JDBC `autosave` transaction of that many statements, at most about 72 KiB per session. Must be ≥ 1. Applied at startup. |
| `tr_max_cycle_statements` | usize | `16` | Session-settings tracking (TR and read offload): statements of one extended-protocol cycle (Parse/Bind/Execute up to a Sync) or multi-statement simple query that are read one by one, in order, to follow its `SET`s and transaction control; consecutive statements that touch neither count once. A longer one that carries a `SET` or a transaction end makes the session's settings unknown (reads stay home, a failover is refused with `08006`). 16 covers the longest shapes drivers send (JDBC `BEGIN` + `autosave` `SAVEPOINT` + statement; pool resets take at most 8). 1 to 256: reading a failed cycle replays it once per place the error could have come from, so its cost grows with the square of this limit. Applied at startup. |

---

## Recovery Journal (`[journal]`)

Retention and durability of the Transaction Replay recovery journal (TR-07) — the
committed-transaction log behind `POST /api/replay`. Journaling itself follows the
top-level `tr_enabled`; this section only bounds what is kept and where. Every key
defaults to the value it had as a compiled-in constant, and an absent `[journal]`
block keeps the journal in process memory only. `validate()` rejects `0` for every
cap (a `0` would disable a safety bound), an unknown `fsync` value, and
`fsync_interval_ms = 0` under `fsync = "interval"`.

```toml
[journal]
max_active_transactions = 50000
max_entries_per_transaction = 10000
max_bytes_per_transaction = 67108864
max_committed_transactions = 50000
max_committed_bytes = 268435456
max_statement_bytes = 1048576
dir = ""
segment_bytes = 67108864
retain_bytes = 1073741824
fsync = "interval"
fsync_interval_ms = 100
writer_queue = 4096
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_active_transactions` | usize | `50000` | Cap on **open** (uncommitted) transaction journals across all sessions; the oldest are evicted at the cap. Only bites when many sessions hold long transactions. |
| `max_entries_per_transaction` | usize | `10000` | Statements journaled per transaction. Beyond it the transaction is kept but marked *incomplete* (committed-history replay refuses it rather than apply it partially). |
| `max_bytes_per_transaction` | usize | `67108864` | Bytes (statement text + parameters) journaled per transaction (64 MiB). Same incomplete marking. |
| `max_committed_transactions` | usize | `50000` | **Committed** transactions retained in memory for replay, oldest evicted first. Also the number reloaded from `dir` at startup. |
| `max_committed_bytes` | usize | `268435456` | Bytes of committed transactions retained in memory (256 MiB). |
| `max_statement_bytes` | usize | `1048576` | A single statement longer than this (1 MiB) is journaled without its text and marks its transaction incomplete — bounds per-statement memory on the capture path. |
| `dir` | string | `""` | Directory of the durable segment store. Empty = memory only: **nothing survives a restart**. When set, every committed transaction is appended to `journal-<first-commit-seq>.log` segments there and the newest `max_committed_transactions` are reloaded at startup with commit numbering continuing after the highest recovered sequence. An unwritable directory fails startup. The segments hold statement text and bound parameter values of committed writes, unencrypted: protect the directory like the database's own data files. Records are CRC-checked and postcard-encoded (record format `HJ02`); a directory written by a pre-release 1.9.0 build (format `HJ01`) is refused at startup with an explicit error rather than truncated. |
| `segment_bytes` | u64 | `67108864` | Bytes after which the current segment is closed and a new one started (64 MiB). |
| `retain_bytes` | u64 | `1073741824` | Total on-disk bytes to keep; the oldest whole segments beyond it are deleted (1 GiB). |
| `fsync` | string | `"interval"` | When appended records reach stable storage: `"commit"` = `fdatasync` after every record, `"interval"` = group `fdatasync` every `fsync_interval_ms`, `"none"` = leave it to the OS page cache. The data path never waits on disk under any policy (see `writer_queue`). |
| `fsync_interval_ms` | u64 | `100` | Group-`fsync` period for `fsync = "interval"`. |
| `writer_queue` | usize | `4096` | Bounded queue between the data path and the segment writer thread. A full queue never stalls a client: the record is dropped and counted (`heliosdb_proxy_journal_dropped_total` is reported as `coverage.dropped_transactions` on replay responses). |

What the journal records is described in [transaction-replay.md](transaction-replay.md#what-happens-on-the-live-write-path-the-journal-hook).

---

## Query Analytics (`[analytics]`)

Fingerprinting, per-query stats, slow-query log, pattern detection. Only active with the
`query-analytics` feature and `enabled = true`.

```toml
[analytics]
enabled = true
slow_query_ms = 1000
max_fingerprints = 10000
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Record per-query statistics and slow-query log. |
| `slow_query_ms` | u64 | `1000` | Queries slower than this are added to the slow-query log. |
| `max_fingerprints` | u32 | `10000` | Maximum distinct query fingerprints to track. |

---

## Anomaly Detection (`[anomaly]`)

Tunables for the in-process anomaly detector (SQL-injection patterns, failed-auth
bursts, per-tenant rate spikes, novel-query shapes). The section is parsed on
every build for config round-tripping, but the detector is only active with the
`anomaly-detection` feature. The defaults reproduce the prior hardcoded behavior
exactly, so an absent `[anomaly]` block changes nothing.

The detector is built **once at startup**, so changing `[anomaly]` requires a
restart — a SIGHUP config reload does not rebuild it.

```toml
[anomaly]
rate_window_secs = 60
spike_z_threshold = 3.0
auth_window_secs = 60
auth_critical_count = 10
auth_warning_count = 5
event_buffer_size = 1024
emit_novel_queries = true
max_seen_fingerprints = 100000
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `rate_window_secs` | u64 | `60` | Rolling window for the per-tenant rate EWMA, seconds. Must be `>= 1`. |
| `spike_z_threshold` | f64 | `3.0` | Minimum z-score before a rate spike fires. Must be finite and `> 0`. |
| `auth_window_secs` | u64 | `60` | Window for failed-auth (credential-stuffing) bursts, seconds. Must be `>= 1`. |
| `auth_critical_count` | u32 | `10` | Failures inside the auth window that escalate to Critical. Must be `>= 1`. |
| `auth_warning_count` | u32 | `5` | Failures inside the auth window that escalate to Warning. Must be `<= auth_critical_count`. |
| `event_buffer_size` | usize | `1024` | Maximum events kept in the in-memory ring buffer. Must be `>= 1`. |
| `emit_novel_queries` | bool | `true` | Emit first-seen query fingerprints as informational events; set `false` on high-churn workloads. |
| `max_seen_fingerprints` | usize | `100000` | Upper bound on the novel-query fingerprint set before it is cleared (bounds memory on high-cardinality SQL). Must be `>= 1`. |

---

## Query Rewriting (`[query_rewrite]`)

Rules-engine SQL rewriting. Only active with the `query-rewriting` feature and
`enabled = true`.

```toml
[query_rewrite]
enabled = true

[[query_rewrite.rules]]
match_table = "orders"
append_where = "deleted_at IS NULL"

[[query_rewrite.rules]]
match_regex = "^SELECT \\* FROM events"
add_limit = 1000
```

`[query_rewrite]` keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Apply the rewrite rules on the query path. |
| `rules` | array | `[]` | Ordered rewrite rules (`[[query_rewrite.rules]]`). |

Each `[[query_rewrite.rules]]` entry (first matching transformation is applied):

| Key | Type | Description |
|-----|------|-------------|
| `match_table` | string | Apply to queries referencing this table. |
| `match_regex` | string | Apply to queries matching this regex. |
| `replace_table_with` | string | Rewrite `match_table` → this table name. |
| `append_where` | string | Append `AND <expr>` to the WHERE clause. |
| `add_limit` | u32 | Add `LIMIT n` to an unbounded query. |

---

## Multi-Tenancy (`[multi_tenancy]`)

Per-tenant row isolation via injected predicates, per-tenant client caps, and per-tenant
request budgets. Only active with the `multi-tenancy` feature and `enabled = true` (a build
without the feature logs a startup warning; under `strict_config` it is a startup error).

The whole section is applied at startup; a SIGHUP that changes it logs a warning and keeps
the running settings. Change caps with a restart or the SIGUSR2 handoff.

```toml
[multi_tenancy]
enabled = true
identify_by = "application_name"
tenant_column = "tenant_id"
tenant_tables = ["orders", "invoices"]
tenants = ["acme", "globex"]
max_clients_per_tenant = 50
max_active_queries_per_tenant = 20
active_query_wait_ms = 5000
idle_permit_timeout_ms = 60000
max_replay_bytes_per_tenant = 67108864

[multi_tenancy.tenant_max_clients]
acme = 200

[multi_tenancy.tenant_max_active_queries]
acme = 50
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable multi-tenancy: per-tenant row isolation (with `tenants` / `tenant_tables`) and the per-tenant client caps and request budgets below. Needs the `multi-tenancy` feature. |
| `identify_by` | string | `"application_name"` | Connection attribute naming the tenant: a startup parameter name (e.g. `application_name`, `user`) or the literal `database`. |
| `tenant_column` | string | `"tenant_id"` | The row-level tenant column injected into queries. |
| `tenant_tables` | array | `[]` | Tables that get the tenant filter injected; others pass through. |
| `tenants` | array | `[]` | Known tenant ids. |
| `max_clients_per_tenant` | usize | `0` | Most client sessions one known tenant may hold on this proxy at once (H-05). The tenant is read from the startup packet (`identify_by`; `database` falls back to `user` as PostgreSQL does). A tenant at its cap waits up to `[limits] client_admission_wait_secs` for one of its own sessions to end, then is refused with SQLSTATE 53300 naming the tenant; other tenants are not affected. The tenant slot is taken before the process-wide `[limits] max_client_connections` slot, so connections queued at their tenant's cap never hold process-wide capacity. Sessions naming no known tenant count only against `max_client_connections`. To guarantee a small tenant its share, keep the sum of tenant caps at or below `max_client_connections`. `0` = no per-tenant cap. Must be ≤ `Semaphore::MAX_PERMITS`. |
| `tenant_max_clients` | table | `{}` | Per-tenant overrides of `max_clients_per_tenant` (`0` = no cap for that tenant). Every key must be in `tenants` (startup error otherwise); each value ≤ `Semaphore::MAX_PERMITS`. |
| `max_active_queries_per_tenant` | usize | `0` | Most requests one known tenant may have in progress through this proxy at once, on PG-wire sessions (the HTTP, MCP and GraphQL gateways are not covered). A request — a simple `Query`, or an extended cycle from its first `Parse`/`Bind`/`Describe`/`Execute`/`Close` to its `Sync` — that starts while its session is idle takes one of the tenant's permits and holds it until the session is idle again: an explicit transaction holds one permit from `BEGIN` to its end and its statements are never refused, a partly received extended cycle (or one a `Query` interrupts) keeps it, and under session pooling a session holding session-level advisory locks (which outlive `ReadyForQuery`; a statement takes them when its text names `pg_advisory_lock`, `pg_advisory_lock_shared`, `pg_try_advisory_lock` or `pg_try_advisory_lock_shared` as a whole word, in any case and anywhere in it, string literals and comments included, so a `DO` block's body counts, while other text containing `advisory_lock`, such as a table `job_advisory_locks` or a literal `'advisory_lock'`, does not) keeps it until a simple-protocol `pg_advisory_unlock_all()` (a call outside string literals and comments, in a statement without a backslash) or `DISCARD ALL` succeeds on the backend connection that took them, or until that connection ends (one that never reaches it, refused by the rate limiter, an open circuit or a plugin or answered from a cache, released nothing and keeps the permit held; a lock statement is seen at its `Parse` and at each `Bind` of a named statement that takes one, and counts once its request reaches a backend: one a failover answers with an error without re-running it took none; the same unlock statements over the extended protocol do not clear it, so a session that uses only the extended protocol keeps its permit until it ends: bound it with `idle_permit_timeout_ms`). Locks end with their backend connection: when it is lost, or the session moves to a replacement after a failover, they no longer keep the permit. A connection lost while its session idles returns the permit at once, without waiting for the client's next message (unless a transaction, or a cycle the client is still in, keeps it: the client's next request replays a lost transaction where its `tr_mode` allows, and is told it was lost otherwise); a session moved during a request returns it at that request's end. So a session idle inside a transaction keeps its permit for as long as its client stays connected, also after the backend ended that transaction (`idle_in_transaction_session_timeout`, `pg_terminate_backend`): bound such sessions with `idle_permit_timeout_ms`. A client that vanishes without closing its connection is detected by `[limits] client_tcp_keepalive_secs` (on by default), and its session then ends and returns the permit. With keepalive off, such a session keeps its permit until the proxy restarts unless a timeout ends it: `idle_permit_timeout_ms` ends only an idle permit holder (inside a transaction, holding advisory locks), `[limits] client_idle_timeout_secs` also one in the middle of an extended cycle (after a `Flush`, owing its `Sync`), and nothing ends one in a stalled `COPY FROM STDIN` or with a message partly received; a warning says so at startup when keepalive is off and a tenant has an active-query cap. A lock statement a transaction replay runs again on the replacement keeps it there. At the cap a request waits in FIFO order up to `active_query_wait_ms`, then is refused with SQLSTATE 53300 `too many active queries for tenant "X" (limit N)`; the session stays usable. On the simple protocol the refusal is followed by `ReadyForQuery`; on the extended protocol the error is sent at once and everything up to `Sync` is discarded (Flush and Query included), as PostgreSQL does after an error; the backend never sees a refused request. A CancelRequest for a waiting session ends the wait with 57014 `canceling statement due to user request` (a key that several live sessions were relayed, from a backend that issues colliding BackendKeyData, ends no wait, since the cancel may come from a session running a statement: it is forwarded to the backend as before); a client that disconnects while waiting abandons its request. A message that continues a cycle a backend holds open (after a `Flush`, or the end of an extended-protocol `COPY`, up to the `Sync` or `Query` that settles it) goes to the connection holding that cycle and is never refused by the rate limiter (which paces it) or an open circuit nor answered from a cache (see `[rate_limit]`), so that backend's reply settles the cycle and the permit returns then. Only a plugin `Block` of such a `Query` leaves the cycle open on the backend: the session keeps its permit (and, under transaction/statement pooling, its backend connection) until a later request reaches the backend and settles it, and meanwhile counts as an idle permit holder (`idle_permit_timeout_ms`). When that connection is lost meanwhile, the cycle dies with it (the backend rolls it back) and the permit returns at once; the client's next request is answered as after any loss of an open cycle. A message other than COPY data that arrives while a `COPY ... FROM STDIN` is in copy-in ends that COPY (PostgreSQL fails it, 08P01, without running the message) and goes to the COPY's connection the same way; that backend's ReadyForQuery ends the session's COPY and leaves it idle (its permit returns), and a plugin `Block` of such a `Query` leaves the COPY open there and the session an idle permit holder. When the connection holding a cycle its client is still in (and may be waiting on: the rest of a `Flush`-dispatched cycle's results, which only the idle backend watch relays, or the answer to an extended `COPY`'s end) is lost while the session waits for that client, the session ends at once with FATAL 08006 `backend ... connection lost while an extended-protocol cycle was open on it (its outcome is unknown)`, and its permit returns: no backend will answer that client. A session that ends with a cycle still open on its backend (its client gone, a `Terminate`, an idle or idle-permit timeout) closes that connection, never resets or parks it, so the backend rolls the cycle back; it first sends that backend a CancelRequest, so on a backend that implements one (PostgreSQL; PgBouncer forwards it) a flushed `Execute` (or the end of an extended COPY) still running there stops before the permit returns. Likewise, a statement the relay abandons while its backend may still run it (silent past `[limits] backend_read_timeout_secs`, past `backend_response_timeout_secs`, or its client gone mid-response) gets a CancelRequest on its connection (bounded by `[limits] backend_cancel_timeout_ms`) before the connection closes and the permit returns, so on such a backend it does not run on outside the cap (only a capped session waits for that cancel). A backend that does not implement CancelRequest (HeliosDB-Nano ignores it) runs such a statement to completion, outside the cap, after its permit returned. With read offload, while a cycle its client is still in is open on a standby (a `route=standby` or `node=` hint sent it there), the session watches that standby's connection rather than its home's, so the rest of the cycle's results reach the waiting client and the loss of that connection ends the session as above. Under transaction/statement pooling a waiting or refused session first parks its idle backend connections, so it holds no backend connection while it waits; the cap does not bound backend connections otherwise. Query-cache and edge-cache hits, rate-limit refusals and the in-request primary wait of a failover all count as active. A single-statement simple-protocol `COMMIT PREPARED` / `ROLLBACK PREPARED` runs without a permit (it can only release locks that permit holders wait on); 2PC through the extended protocol and locks taken inside functions or by an SQL `EXECUTE` of a `PREPARE`d statement are not seen, so set `lock_timeout` as the backstop for lock-wait/permit-wait cycles. A `FunctionCall` message is not gated. `0` = no cap. Must be ≤ `Semaphore::MAX_PERMITS`. |
| `tenant_max_active_queries` | table | `{}` | Per-tenant overrides of `max_active_queries_per_tenant` (`0` = no cap for that tenant). Every key must be in `tenants`; each value ≤ `Semaphore::MAX_PERMITS`. |
| `active_query_wait_ms` | u64 | `5000` | How long a request waits for one of its tenant's active-query permits before it is refused with 53300. `0` = refuse at once. Must be ≤ 31536000000 (1 year). |
| `idle_permit_timeout_ms` | u64 | `0` | A session that sits idle (waiting for its client) while holding one of its tenant's active-query permits — inside a transaction, or holding session-level advisory locks — is terminated after this long: FATAL 25P03 `terminating connection due to idle-in-transaction timeout` inside a transaction, 57P05 otherwise. The permit returns. A session whose `Query` a plugin blocked while its backend held an extended cycle, or a COPY in copy-in, open (see `max_active_queries_per_tenant`) is idle too: it has been answered `ReadyForQuery`. A bare `Flush` with no extended cycle open, or sent after such a blocked `Query`, has nothing to flush for its client (PostgreSQL answers nothing to an idle one): the proxy consumes it (in every session, capped or not), so it never makes an idle session look busy, and nothing refuses it. Never applies to a session without a permit, nor to one in the middle of an extended cycle (after a `Flush`, before the `Sync` the client still owes, also when that `Flush` was answered with an error before reaching a backend, such as a rate-limit or open-circuit refusal or the error a failover answers it with when it does not re-run it; `[limits] client_idle_timeout_secs` bounds that); when `[limits] client_idle_timeout_secs` is also armed, the earlier deadline wins. A stalled `COPY FROM STDIN` is not idle (both idle timeouts are disarmed during COPY), so it holds its permit (and, under transaction or statement pooling, its backend connection) until its client sends the rest of the COPY or closes its socket. A client host that vanishes without a FIN or RST is detected by `[limits] client_tcp_keepalive_secs` (on by default, about ten times its value after the client's last traffic), which ends the session and returns the permit; with keepalive off it is never detected. During copy-in the proxy reads only the client, so a backend that ends the COPY (or is terminated with `pg_terminate_backend`) goes unnoticed while the client stays connected: only that client, or restarting the proxy, ends such a session; size the cap with headroom for it. `0` = never. Must be ≤ 31536000000 (1 year). |
| `max_replay_bytes_per_tenant` | usize | `0` | Most transaction-replay bytes (`[limits] tr_max_replay_bytes`'s measure: statement text and raw extended-protocol frames) all sessions of one known tenant may hold at once. Applies to sessions whose `tr_mode` is `select` or `transaction` (`tr_mode` follows a SIGHUP for new sessions; a warning is logged at startup when no recording mode is configured, and on a SIGHUP that changes it). A `select`-mode transaction with writes is never replayed, so it is not charged, and a `Flush` chunk that may write is not checked against the budget either (its write is recorded only at the cycle's `Sync`). The statement whose recording would take the tenant over marks its transaction non-replayable, the same degradation as `tr_max_replay_bytes` (P-03 refusal reason `cap_exceeded`; `heliosdb_proxy_tr_replay_cap_exceeded_total` keeps counting only the `[limits]` caps, and a transaction one of them degrades is not counted as a tenant refusal as well). Inside a transaction, chunks of an open Flush-terminated cycle are checked against the budget and charged when the `Sync` records the cycle; an autocommit chunk holds no budget bytes (a cycle that opens a transaction is charged, or refused, at its `Sync`). The bytes return at transaction end and at session end. `0` = no budget. |
| `tenant_max_replay_bytes` | table | `{}` | Per-tenant overrides of `max_replay_bytes_per_tenant` (`0` = no budget for that tenant). Every key must be in `tenants`. |

`/metrics` reports `tenant_slots` (`tenant`, `limit`, `in_use`, `waiting`, `waits`,
`refusals`) for capped tenants; Prometheus has `heliosdb_proxy_tenant_clients`,
`heliosdb_proxy_tenant_client_limit`, `heliosdb_proxy_tenant_admission_queue_depth`,
`heliosdb_proxy_tenant_admission_waits_total` and
`heliosdb_proxy_tenant_admission_refusals_total`, labelled by `tenant`, and
`heliosdb_proxy_admission_queue_depth` (all connections waiting for admission now).
The caps bound client sessions; with session pooling that also bounds a tenant's backend
connections, while transaction pooling shares backend connections across tenants.

For tenants with a request budget, `/metrics` reports `tenant_budgets` (`tenant`,
`query_limit`, `queries_active`, `queries_idle`, `queries_waiting`, `queries_admitted`,
`query_waits`, `query_refusals`, `query_wait_cancels`, `idle_permit_timeouts`,
`replay_byte_limit`, `replay_bytes`, `replay_refusals`). Prometheus has, labelled by
`tenant`, for tenants with an active-query cap: `heliosdb_proxy_tenant_active_queries`,
`heliosdb_proxy_tenant_active_query_limit`, `heliosdb_proxy_tenant_active_queries_idle`
(permit holders idle in a transaction, holding locks, or behind a plugin-blocked `Query` that left
an extended cycle open), `heliosdb_proxy_tenant_query_queue_depth`,
`heliosdb_proxy_tenant_queries_admitted_total`, `heliosdb_proxy_tenant_query_waits_total`,
`heliosdb_proxy_tenant_query_refusals_total`, `heliosdb_proxy_tenant_query_wait_cancels_total`
and `heliosdb_proxy_tenant_idle_permit_timeouts_total`; for tenants with a replay-byte
budget: `heliosdb_proxy_tenant_replay_bytes`, `heliosdb_proxy_tenant_replay_byte_limit`
and `heliosdb_proxy_tenant_replay_budget_refusals_total`. The histogram
`heliosdb_proxy_tenant_query_wait_seconds` records the wait of every request that waited
for a permit, however the wait ended (admitted, refused with 53300 at its end, cancelled, or
abandoned by its client). It is one series for all tenants, with no `tenant` label: its
count matches the sum of `heliosdb_proxy_tenant_query_waits_total` over all tenants, and its
buckets mix every tenant's waits, so it cannot show one tenant's wait latency (the
per-tenant counters show which tenants wait).

---

## Schema Routing (`[schema_routing]`)

Route analytical (OLAP) queries to a dedicated node. Only active with the
`schema-routing` feature and `enabled = true`.

```toml
[schema_routing]
enabled = true
analytics_node = "analytics-1"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Route aggregations / GROUP BY / window-function queries to a node. |
| `analytics_node` | string | `""` | `name` of the node analytical queries route to. |

---

## Authentication (`[auth]`) and HBA Rules (`[[hba]]`)

Client-side authentication mode and pg_hba-style admission.

```toml
[auth]
mode = "scram"
auth_file = "/etc/heliosproxy/userlist.txt"

[[hba]]
action = "allow"
user = "all"
database = "all"
address = "10.0.0.0/8"

[[hba]]
action = "reject"
user = "all"
database = "all"
address = "all"        # trailing rule = default-deny
```

`[auth]`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | string | `"passthrough"` | `passthrough` relays client auth to the backend; `scram` makes the proxy terminate SCRAM-SHA-256 against `auth_file`. |
| `auth_file` | string | *(none)* | Path to a pgbouncer-style user list (`user:secret`, secret = plaintext or a `SCRAM-SHA-256$...` verifier). Required when `mode = "scram"`. |

`[[hba]]` rules are evaluated in order; the first rule whose `user`, `database`, and
`address` all match decides the outcome. If **no** rule matches, the connection is
admitted (add a trailing `reject … all/all/all` for default-deny):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `action` | string | — | `allow` or `reject`. |
| `user` | string | `"all"` | Matching PostgreSQL user, or `all`. |
| `database` | string | `"all"` | Matching database, or `all`. |
| `address` | string | `"all"` | `all`, a bare IP, or a CIDR (e.g. `10.0.0.0/8`, `::1/128`). |

---

## WASM Plugins (`[plugins]`)

Plugin subsystem (a single `[plugins]` table, **not** an array of `[[plugins]]`). Only
consumed with the `wasm-plugins` feature; strictly opt-in.

```toml
[plugins]
enabled = true
plugin_dir = "/etc/heliosproxy/plugins"
hot_reload = false
memory_limit_mb = 64
timeout_ms = 100
max_plugins = 20
fuel_metering = true
fuel_limit = 1000000
kv_max_value_bytes = 65536
kv_max_keys_per_plugin = 1024
kv_max_plugins = 256
kv_max_total_bytes = 67108864
# trust_root = "/etc/heliosproxy/plugin-keys"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable the plugin subsystem. |
| `plugin_dir` | string | `"/etc/heliosproxy/plugins"` | Directory scanned for `.wasm` plugins at startup. |
| `hot_reload` | bool | `false` | Watch `plugin_dir` and reload plugins on change. |
| `memory_limit_mb` | usize | `64` | Memory limit per plugin instance. |
| `timeout_ms` | u64 | `100` | Execution timeout per hook call. |
| `max_plugins` | usize | `20` | Maximum concurrently-loaded plugins. |
| `fuel_metering` | bool | `true` | Enable per-call CPU-cycle (fuel) metering. |
| `fuel_limit` | u64 | `1000000` | Fuel units allowed per hook call when metering is on. |
| `kv_max_value_bytes` | usize | `65536` | Max bytes for a single plugin-KV key OR value (via `kv_set` or `PUT /admin/kv/<plugin>/<key>`); `0` = unlimited. A write past this cap is rejected (`kv_set` returns `-1`; the admin endpoint returns `413`). |
| `kv_max_keys_per_plugin` | usize | `1024` | Max distinct keys per plugin KV namespace; `0` = unlimited. Overwriting an existing key never trips the cap. |
| `kv_max_plugins` | usize | `256` | Max distinct plugin KV namespaces that may exist at once; `0` = unlimited. Bounds how many `<plugin>` namespaces `PUT /admin/kv/<plugin>/<key>` can create, so a token-holder cannot exhaust memory by writing to unboundedly-many namespace names. Writing to an already-present namespace never trips the cap; deleting a namespace's last key frees its slot. |
| `kv_max_total_bytes` | usize | `67108864` (64 MiB) | Max TOTAL retained bytes across ALL plugin KV namespaces (each entry's key + value bytes plus each live namespace's name bytes); `0` = unlimited. The single backstop that bounds the whole KV footprint regardless of the per-axis product `kv_max_plugins × kv_max_keys_per_plugin × kv_max_value_bytes` (which can otherwise reach tens of GiB), so a token-holding `PUT /admin/kv/<plugin>/<key>` caller cannot drive the proxy to an OOM. A write past this cap is rejected (`kv_set` returns `-1`; the admin endpoint returns `413`). |
| `trust_root` | string | *(none)* | Ed25519 trust-root directory. When set, every `.wasm` requires a sidecar `.sig` verifying against a `*.pub` in this directory; when omitted, signatures are not checked. |

A statement a plugin blocks (42000) never reaches the backend. Inside a transaction the
reply reports the transaction's status (`T`) and does not abort the transaction: a later
`COMMIT` commits the statements before it (see Rate Limiting above).

---

## MCP Agent Gateway (`[mcp]`) and Agent Contracts (`[[agent_contracts]]`)

Native MCP server exposing `query` / `list_tables` / `explain` tools. Disabled by
default.

```toml
[mcp]
enabled = true
listen_address = "127.0.0.1:9092"
backend_host = "127.0.0.1"
backend_port = 5432
backend_user = "postgres"
# backend_password = "..."
# backend_database = "app"
read_only = true
# contract = "reporting-agent"
auth_token = "${MCP_TOKEN}"
query_timeout_ms = 30000
max_concurrent_requests = 64

[[agent_contracts]]
id = "reporting-agent"
read_only = true
allowed_verbs = ["SELECT"]
allowed_tables = ["orders", "invoices"]
require_limit = true
max_rows = 1000
```

`[mcp]`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Serve the MCP JSON-RPC endpoint. |
| `listen_address` | string | `"127.0.0.1:9092"` | HTTP listen address for MCP. |
| `backend_host` | string | `"127.0.0.1"` | Backend the tool SQL runs against. |
| `backend_port` | u16 | `5432` | Backend port. |
| `backend_user` | string | `"postgres"` | Backend user. |
| `backend_password` | string | *(none)* | Backend password. |
| `backend_database` | string | *(none)* | Backend database. |
| `read_only` | bool | `true` | Refuse write/DDL — agents get a read-only surface. |
| `contract` | string | *(none)* | Name of an `[[agent_contracts]]` entry to enforce on every tool call. |
| `auth_token` | string | *(none)* | Bearer token required on every MCP request. Absent = open, so **set this for any non-loopback deployment** — MCP exposes SQL and must not be anonymous off localhost. |
| `query_timeout_ms` | u64 | `30000` | Per-`tools/call` backend execution timeout, milliseconds. Was a hardcoded 30 s. Must be > 0. |
| `max_concurrent_requests` | usize | `64` | Maximum `tools/call` requests admitted concurrently; beyond this the gateway answers a JSON-RPC error (code `-32000`) instead of queuing indefinitely. `initialize`/`ping`/`tools/list` and notifications are never gated. Must be > 0. |
| `tenant_header` | string | `""` | With `[multi_tenancy]` (H-06): the request header naming the tenant (e.g. `X-Helios-Tenant`). Every request must name a known tenant in it (403 otherwise), and its statements get that tenant's row filter, as a PG-wire session does. Trusted as sent: put the gateway behind a front end that authenticates the caller and sets the header. Empty = no per-request tenant, which `[multi_tenancy]` refuses under `strict_config` (warns otherwise). |

Each `[[agent_contracts]]` entry (scoped grants, referenced by `id` from `[mcp] contract`):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `id` | string | — | Identifier matched against the agent. |
| `read_only` | bool | `true` | Reject write/DDL statements. |
| `allowed_verbs` | array | *(none)* | If set, only these SQL verbs are allowed (upper-case). |
| `allowed_tables` | array | *(none)* | If set, only these tables may be referenced. |
| `denied_tables` | array | `[]` | Tables that may never be referenced (takes precedence over allow). |
| `require_predicate_on` | array | `[]` | Predicates that must be present when a named table is touched. |
| `require_limit` | bool | `false` | Require a LIMIT on SELECTs. |
| `max_rows` | u64 | *(none)* | Suggested/enforced row cap. |

---

## HTTP SQL Gateway (`[http_gateway]`)

> **Request policies (H-06), all three gateways (HTTP, GraphQL, MCP):** `[rate_limit]`
> applies per request (keyed by `key_by`; HTTP 429 on denial), `[query_rewrite]` rules
> rewrite every statement a gateway sends, and `[circuit_breaker]` fast-fails a gateway
> (HTTP 503) while its backend's circuit is open. `[multi_tenancy]` filtering does **not**
> reach the gateways (no per-request tenant identity); `strict_config = true` refuses
> that combination at startup.

Neon-serverless-driver-compatible `POST /sql` endpoint. Disabled by default.

```toml
[http_gateway]
enabled = true
listen_address = "127.0.0.1:9093"
backend_host = "127.0.0.1"
backend_port = 5432
backend_user = "postgres"
# backend_password = "..."
# backend_database = "app"
auth_token = "${HTTP_GW_TOKEN}"
query_timeout_ms = 30000
max_concurrent_requests = 64
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Serve the HTTP SQL gateway. |
| `listen_address` | string | `"127.0.0.1:9093"` | HTTP listen address. |
| `backend_host` | string | `"127.0.0.1"` | Backend host. |
| `backend_port` | u16 | `5432` | Backend port. |
| `backend_user` | string | `"postgres"` | Backend user. |
| `backend_password` | string | *(none)* | Backend password. |
| `backend_database` | string | *(none)* | Backend database. |
| `auth_token` | string | *(none)* | Optional Bearer token required on requests. |
| `query_timeout_ms` | u64 | `30000` | Per-request backend execution timeout, milliseconds. Was a hardcoded 30 s. Must be > 0. |
| `max_concurrent_requests` | usize | `64` | Maximum `/sql` requests admitted concurrently; beyond this the gateway answers `503 Service Unavailable` with a `Retry-After` header instead of queuing indefinitely (`/health` is never gated). Must be > 0. |
| `tenant_header` | string | `""` | With `[multi_tenancy]` (H-06): the request header naming the tenant (e.g. `X-Helios-Tenant`). Every request must name a known tenant in it (403 otherwise), and its statements get that tenant's row filter, as a PG-wire session does. Trusted as sent: put the gateway behind a front end that authenticates the caller and sets the header. Empty = no per-request tenant, which `[multi_tenancy]` refuses under `strict_config` (warns otherwise). |

---

## GraphQL Gateway (`[graphql_gateway]`)

GraphQL-to-SQL gateway on a separate HTTP listener. Only active with the
`graphql-gateway` feature and `enabled = true`.

```toml
[graphql_gateway]
enabled = true
listen_address = "0.0.0.0:9091"
backend_host = "127.0.0.1"
backend_port = 5432
backend_user = "postgres"
# backend_password = "..."
# backend_database = "app"
# auth_token = "..."
query_timeout_ms = 30000
max_concurrent_requests = 64

[[graphql_gateway.tables]]
name = "orders"
columns = ["id", "customer_id", "total", "created_at"]
```

`[graphql_gateway]`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Serve the GraphQL gateway. |
| `listen_address` | string | `"0.0.0.0:9091"` | HTTP listen address. |
| `backend_host` | string | `"127.0.0.1"` | Backend host. |
| `backend_port` | u16 | `5432` | Backend port. |
| `backend_user` | string | `"postgres"` | Backend user. |
| `backend_password` | string | *(none)* | Backend password. |
| `backend_database` | string | *(none)* | Backend database. |
| `auth_token` | string | *(none)* | Optional Bearer token required on requests. |
| `tables` | array | `[]` | Tables exposed as GraphQL types (`[[graphql_gateway.tables]]`). |
| `query_timeout_ms` | u64 | `30000` | Per-request backend execution timeout, milliseconds. Was a hardcoded 30 s. Must be > 0. |
| `max_concurrent_requests` | usize | `64` | Maximum GraphQL requests admitted concurrently; beyond this the gateway answers `503 Service Unavailable` with a `Retry-After` header instead of queuing indefinitely (`/health` is never gated). Must be > 0. |
| `tenant_header` | string | `""` | With `[multi_tenancy]` (H-06): the request header naming the tenant (e.g. `X-Helios-Tenant`). Every request must name a known tenant in it (403 otherwise), and its statements get that tenant's row filter, as a PG-wire session does. Trusted as sent: put the gateway behind a front end that authenticates the caller and sets the header. Empty = no per-request tenant, which `[multi_tenancy]` refuses under `strict_config` (warns otherwise). |

Each `[[graphql_gateway.tables]]`: `name` (string) and `columns` (array of strings).

---

## Traffic Mirror (`[mirror]`)

Continuously mirror a sampled share of live (simple-query) writes to a secondary
backend, off the client hot path. Disabled by default; the on-ramp to a PG→Nano
migration mirror.

```toml
[mirror]
enabled = true
sample_rate = 1.0
writes_only = true
queue_size = 10000
backend_host = "127.0.0.1"
backend_port = 5432
backend_user = "postgres"
# backend_password = "..."
# backend_database = "app"
# source_host / source_port / source_user / source_password / source_database
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Mirror eligible statements to the secondary. |
| `sample_rate` | f64 | `1.0` | Fraction of eligible statements to mirror (`0.0`–`1.0`). |
| `writes_only` | bool | `true` | Mirror only write/DDL statements; `false` mirrors all simple queries. |
| `queue_size` | usize | `10000` | Bounded queue depth; when full, statements are dropped (and counted) rather than blocking. |
| `backend_host` | string | `"127.0.0.1"` | Mirror-target host. |
| `backend_port` | u16 | `5432` | Mirror-target port. |
| `backend_user` | string | `"postgres"` | Mirror-target user. |
| `backend_password` | string | *(none)* | Mirror-target password. |
| `backend_database` | string | *(none)* | Mirror-target database. |
| `source_host` / `source_port` / `source_user` / `source_password` / `source_database` | — | localhost / `5432` / `postgres` / none / none | Source (primary) connection used by `POST /api/migration/snapshot` to bootstrap the secondary. |

---

## Edge / Geo Proxy (`[edge]`)

Two-region result caching. A `home`-role proxy is authoritative (routes writes, caches
reads, broadcasts SSE invalidations); an `edge`-role proxy serves reads from a local
cache and forwards misses/writes to the home. Disabled by default. Parsed on every
build, but `enabled = true` requires the `edge-proxy` compile-time feature (validation
rejects it otherwise).

```toml
[edge]
enabled = true
role = "edge"                       # "home" (default) or "edge"
home_url = "https://home-proxy:9090"  # edge: home admin base URL
auth_token = "${EDGE_HOME_TOKEN}"     # edge: home admin bearer
allow_insecure_home_url = false
default_ttl_secs = 60
max_entries = 10000
max_total_bytes = 268435456
max_edges = 32
liveness_window_secs = 120
subscribe_gc_secs = 30
region = "eu-west"
edge_id = "edge-a"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Master switch; requires the `edge-proxy` feature to enable. |
| `role` | string | `"home"` | `home` (authoritative) or `edge` (cache-first). |
| `home_url` | string | `""` | *(edge)* Home proxy admin base URL the edge subscribes to. Required for `role = "edge"`. |
| `auth_token` | string | `""` | *(edge)* Home admin bearer for the invalidation subscription. When set, `home_url` must be `https://` unless `allow_insecure_home_url = true`. |
| `allow_insecure_home_url` | bool | `false` | *(edge)* Allow presenting `auth_token` to a plain-http `home_url` (private links only). |
| `default_ttl_secs` | u64 | `60` | Default TTL for cache entries when the home supplies none. Must be ≥ 1 when edge is enabled. A shorter `[[cache.tables]] ttl_secs` of a table the read names caps it ([Exclusions and per-table TTL](#exclusions-and-per-table-ttl)). |
| `max_entries` | usize | `10000` | Cache entries before LRU eviction. |
| `max_total_bytes` | usize | `268435456` | Aggregate retained-payload budget (C-01): response bodies plus per-entry overhead. On insert the cache evicts LRU entries until the retained payload fits. `0` disables the budget (entry count x per-entry ceiling then bounds memory). |
| `max_edges` | usize | `32` | *(home)* Maximum simultaneously-registered edges. |
| `liveness_window_secs` | u64 | `120` | *(home)* Edges not seen within this window are GC-pruned. Keep comfortably above ~45s. Must be ≥ 1 when edge is enabled. |
| `subscribe_gc_secs` | u64 | `30` | *(home)* Registry GC sweep cadence. Must be ≥ 1 when edge is enabled. |
| `region` | string | `""` | *(edge)* Region label reported when subscribing. |
| `edge_id` | string | `""` | *(edge)* Stable registration id (empty → `edge-<pid>`). |

An `edge`-role proxy also requires at least one `[[nodes]]` entry pointing at the home's
PG-wire listener (its data plane), and cannot be combined with `[cache] enabled = true`
(the query-result cache does not receive edge invalidations).

A write through the home invalidates the edge entries of the tables its text names (the
query cache's table extraction, [C-02](#query-cache-cache)), or every entry when its
scope is unknown: DDL, `REFRESH MATERIALIZED VIEW`, `COMMENT`, `SECURITY LABEL`,
`IMPORT FOREIGN SCHEMA`, `REASSIGN OWNED`, `EXECUTE`, `CALL` and `DO` (a view, a function
or a `TRUNCATE … CASCADE` changes reads its text does not name), a `MERGE` other than
`MERGE INTO`, an `INSERT` without `INTO`, a write naming a quoted identifier or a name
with a `$` or a non-ASCII character where a table name is read (or a `U&"…"` one
anywhere) or a quoted literal where the name of a table it may write is read (a literal
after the `FROM` of an `UPDATE`, of a subquery or of a function in a write without a
`DELETE`, after `COPY t FROM`, or after `IS DISTINCT FROM` names no written table:
`COPY kv FROM '/tmp/kv.csv'` flushes `kv` on `[edge]`, while the query cache
invalidates everything on any `COPY`; after a `DELETE` any `FROM` counts, since
`DELETE x FROM 'kv'` deletes from `kv`), backend-ambiguous text and a string of more than one statement holding a write
or a statement not known to change no rows. Such a string is read one statement at a
time, split where the proxy's lexer finds a `;` outside literals, quoted identifiers and
comments: a string of reads (`SELECT`, `VALUES`, `TABLE`, `SHOW`, a `WITH` without a
data-modifying verb, an `EXPLAIN` without `ANALYZE`) and bare `BEGIN`, `START`,
`SAVEPOINT`, `RELEASE` and `ROLLBACK` alone (`SELECT a FROM t1; SELECT b FROM t2`)
invalidates nothing; any other statement in it (`REFRESH MATERIALIZED VIEW a;
REFRESH MATERIALIZED VIEW b`, a `LOCK`) flushes every entry, and so does a string the
backends may split differently (a `;` inside backend-ambiguous text). A string the lexer
finds one statement in is read as that statement, however many `;` its literals and
quoted identifiers hold (`COPY t TO STDOUT (DELIMITER ';')` and `NOTIFY c, 'a;b'`
invalidate nothing). An
`EXPLAIN ANALYZE`, which runs the statement it explains, invalidates the tables its text
names. A
simple-protocol statement is read from its first word past its leading comments
(`/* job */ UPDATE …` and `MERGE INTO … USING …` are writes), and one whose leading
comment the backends end in different places, or whose first word holds a non-ASCII
character other than a space (HeliosDB-Lite reads `ınsert` as `INSERT`), counts as a
write of unknown scope (so does a data-modifying CTE verb HeliosDB-Lite reads upper-cased
the Unicode way). A `WITH` statement is a write when a word outside its literals, quoted
identifiers and comments is `INSERT`, `UPDATE`, `DELETE` or `MERGE`:
`WITH r AS (SELECT … WHERE op = 'delete') SELECT …` is a read.

---

## Instant Branch Databases (`[branch]`)

Provision `CREATE DATABASE <branch> TEMPLATE <base>` clones through the proxy. Disabled
by default.

```toml
[branch]
enabled = true
backend_host = "127.0.0.1"
backend_port = 5432
admin_user = "postgres"          # a role with CREATEDB
admin_password = "${PGPASSWORD}"
admin_database = "postgres"      # maintenance DB for CREATE/DROP DATABASE
base_database = "postgres"       # default template when a request omits `base`
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable branch-database provisioning. |
| `backend_host` | string | `"127.0.0.1"` | Backend host. |
| `backend_port` | u16 | `5432` | Backend port. |
| `admin_user` | string | `"postgres"` | Role with CREATEDB privilege. |
| `admin_password` | string | *(none)* | Password for `admin_user`. |
| `admin_database` | string | `"postgres"` | Maintenance database to issue CREATE/DROP DATABASE against. |
| `base_database` | string | `"postgres"` | Default template database when a request omits `base`. |

---

## Configuration Validation

At startup the proxy validates the loaded config and refuses to start if:

1. No backend nodes are configured.
2. No node has `role = "primary"`.
3. `pool.max_connections < pool.min_connections`.
4. `health.check_interval_secs = 0`.
5. `admin_address` is non-loopback but `admin_token` is unset and
   `admin_allow_insecure = false` (see [Admin API Security](#admin-api-security)).
6. `edge.enabled = true` without the `edge-proxy` feature, or an `edge` role missing its
   `home_url`, or its zero-value timing knobs, or combined with `[cache]`.

Invalid configurations produce a descriptive error and a non-zero exit code. Unknown
top-level keys are **warned**, not rejected (see
[Unknown Keys](#unknown-keys-are-warned-not-rejected)).

---

## Complete Example

Ready-to-use examples live in `config/proxy.example.toml`, `config/proxy.full.toml`,
`config/proxy.postgres.toml`, and the working `scripts/regress/*.toml` files.

```toml
# HeliosProxy configuration example.

listen_address = "0.0.0.0:6432"
admin_address  = "127.0.0.1:9090"
# admin_token  = "${ADMIN_TOKEN}"
tr_enabled     = true
tr_mode        = "session"
write_timeout_secs = 30

[pool_mode]
mode = "transaction"
max_pool_size = 100
prepared_statement_mode = "track"
skip_clean_reset = true

[pool]
min_connections = 5
max_connections = 100
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 30
test_on_acquire = true

[load_balancer]
read_strategy = "least_connections"
read_write_split = true
latency_threshold_ms = 50

[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"

[[nodes]]
host = "db-primary.internal"
port = 5432
role = "primary"
weight = 100
enabled = true
name = "primary"

[[nodes]]
host = "db-standby-1.internal"
port = 5432
role = "standby"
weight = 100
enabled = true
name = "standby-1"

[tls]
enabled = false
cert_path = "/etc/heliosproxy/server.crt"
key_path = "/etc/heliosproxy/server.key"
require_client_cert = false
```

---

## See Also

- [Architecture](architecture.md)
- [Feature Flags](feature-flags.md)
- [Admin API Reference](admin-api.md)


## Transaction Replay: the re-executable read subset (TR-03)

`tr_mode = select` and `transaction` re-execute an interrupted statement on the
replacement backend only when its outcome on the failed backend cannot have had a
side effect. The proxy decides lexically, without a catalog round trip:

- Statement shape: `SELECT`, `VALUES`, `TABLE`, `SHOW`, `EXPLAIN` (without
  `ANALYZE`), `COPY … TO`, or a `WITH` whose CTEs contain no `INSERT`/`UPDATE`/
  `DELETE`/`MERGE`.
- No `INTO` (creates a table).
- Every syntactic function call — an identifier followed by `(` outside strings,
  quoted identifiers, dollar quotes and comments — must be a PostgreSQL built-in
  from the proxy's side-effect-free list (aggregates, string/math/date/JSON/array
  functions, `pg_sleep`, `random`, `now`, `current_setting`, size/introspection
  helpers) or listed in `tr_read_functions`. Schema qualification is ignored
  (`pg_catalog.lower(...)` is `lower`).
- A call through a quoted identifier (`"MyFn"(...)`) is never eligible: its
  case is opaque to the policy.

Everything else — user-defined functions, `nextval`/`setval`/`currval`/`lastval`,
`pg_notify`, `set_config`, `pg_advisory_*`, `txid_current`, `setseed`, large-object
and replication functions — makes the statement opaque: on an unknown outcome the
client receives SQLSTATE `08007` and the proxy does not run it again. Inside an
explicit transaction an opaque statement also marks the transaction as containing
a possible write, so `select` mode will not replay it. Nondeterministic but
side-effect-free calls are allowed because an unknown-outcome autocommit read
published nothing to the client before the fault.

### Session state across a failover (TR-04)

`SET`/`RESET` issued through the simple query protocol, or alone through the extended
protocol outside a transaction, are tracked per session and replayed, in order, onto the
replacement backend when the session re-homes. The
tracking is transactional, mirroring PostgreSQL: a `SET` inside an explicit
transaction takes effect for restore only when that transaction commits; `ROLLBACK`
discards it; `ROLLBACK TO SAVEPOINT` discards the `SET`s made after the savepoint
(`RELEASE` keeps them); `RESET name`, `RESET ALL` and `DISCARD ALL` inside a
transaction are likewise deferred until commit. `SET LOCAL`, `SET TRANSACTION` and
`SET CONSTRAINTS` are transaction-scoped and never tracked. Variables are identified
by name (`SET TIME ZONE` is `timezone`, `SET SCHEMA` is `search_path`, `SET NAMES`
is `client_encoding`), so repeated `SET`s of one variable cost one slot; the role and the
session authorization always fit, because `RESET ALL` keeps them (as in PostgreSQL) while
`DISCARD ALL` resets them. Extended-protocol `SET`s inside a transaction or batched with
other statements, `SQL PREPARE`, temporary tables and cursors are not restored.

### Replay verification and the recovery deadline (TR-06)

While a transaction is recorded for `tr_mode = transaction` replay, the proxy hashes
each statement's response frames (`RowDescription`, `DataRow`, `CommandComplete`,
`EmptyQueryResponse`) up to `tr_max_observation_bytes`. When the transaction is
replayed on the replacement backend, each response is hashed the same way; if any
digest differs — the data the client already saw is not what the new backend would
have returned — the replay is rolled back and the client receives `40001` for the
interrupted statement. Transactions opened at `SERIALIZABLE` or `REPEATABLE READ`
(explicitly, or through `default_transaction_isolation`) are never replayed: a
replay cannot reproduce the old snapshot.

One deadline, `write_timeout_secs`, bounds the whole recovery: waiting for a
healthy primary, connecting and authenticating, restoring session `SET`s, replaying
the transaction and re-executing the interrupted statement.
