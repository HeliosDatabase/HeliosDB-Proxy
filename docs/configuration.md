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
| `--primary` | *(none)* | Primary node `host:port`. |
| `--standby` | *(none)* | Standby node `host:port` (repeatable). |
| `--tr` | `true` | Enable Transaction Replay. |
| `--log-level` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error`. |
| `--json-logs` | `false` | Emit logs in JSON format. |

There is also an `install skills` subcommand (`--target claude|codex|both`,
`--symlink`, `--force`, `--dry-run`) for installing the bundled agent skills.

When `--config` is provided, the file drives the whole configuration. Command-line node
arguments (`--primary`, `--standby`) build an in-memory config only when no config file
is supplied.

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

## Unknown Keys Are Warned, Not Rejected

`ProxyConfig` does **not** use `deny_unknown_fields`. Any top-level TOML key that is not
a recognized `ProxyConfig` field is parsed-and-ignored, and each one is logged once at
startup as a warning:

```
WARN unknown config section/key '<key>' ignored (not part of ProxyConfig)
```

This makes silent doc-drift visible without breaking pre-existing configs. Historical
example blocks such as `[ha]`, `[logging]`, `[metrics]`, `[routing]`, `[distribcache]`,
`[graphql]`, `[[tenants]]`, and `[[schema_routes]]` are **not** part of `ProxyConfig` —
they will be warned and ignored. Their real counterparts are documented below
(`lag_routing`, `multi_tenancy`, `graphql_gateway`, `schema_routing`, …). Note that
detection is top-level only: a nested unknown key (e.g. `[cache.l1]`) is not reported.

---

## Top-Level Options

```toml
listen_address = "0.0.0.0:5432"
admin_address  = "127.0.0.1:9090"
# admin_token          = "..."     # bearer token for the admin API (see below)
# admin_allow_insecure = false
tr_enabled     = true
tr_mode        = "session"
write_timeout_secs        = 30
optimize_unnamed_parse    = true
shutdown_drain_timeout_secs = 60
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `listen_address` | string | `"0.0.0.0:5432"` | Address/port for PostgreSQL client connections. *(Required in a config file.)* |
| `admin_address` | string | `"127.0.0.1:9090"` | Address/port for the admin HTTP API. Loopback by default. *(Required in a config file.)* |
| `admin_token` | string | *(none)* | Bearer token required on every admin endpoint except liveness probes. See [Admin API Security](#admin-api-security). |
| `admin_allow_insecure` | bool | `false` | Explicit opt-in to expose the admin API on a non-loopback address **without** a token. |
| `tr_enabled` | bool | `true` | Enable Transaction Replay. *(Required in a config file.)* |
| `tr_mode` | string | `"session"` | Transaction Replay mode: `none`, `session`, `select`, `transaction`. *(Required in a config file.)* |
| `write_timeout_secs` | u64 | `30` | Seconds to buffer writes during failover before returning an error. |
| `optimize_unnamed_parse` | bool | `true` | Skip re-forwarding an identical unnamed extended-protocol `Parse` a backend already holds, synthesizing `ParseComplete` locally. A kill-switch for drivers that depend on the redundant round trip. |
| `shutdown_drain_timeout_secs` | u64 | `60` | How long a SIGUSR2 binary-handoff drain keeps serving in-flight connections before dropping them. Runtime override: `HELIOS_DRAIN_TIMEOUT_SECS`. |

The sections `[pool]`, `[load_balancer]`, `[health]`, and at least one `[[nodes]]` entry
are also required in a config file (they have no serde defaults). Every other section
listed below is optional and defaults to disabled/off.

### Transaction Replay Modes (`tr_mode`)

| Mode | Description |
|------|-------------|
| `none` | Transaction Replay disabled. In-flight transactions are aborted on failover. |
| `session` | Re-establish session state (SET parameters, prepared statements) on the new primary. Transactions are not replayed. |
| `select` | Restore session state and re-execute SELECT queries. Write transactions are not replayed. |
| `transaction` | Full transaction replay — all journaled statements re-executed on the new primary. Strongest failover guarantee. |

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
| `acquire_timeout_secs` | u64 | `5` | Max seconds to wait when acquiring from the pool. |
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
latency_threshold_ms = 100
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `read_strategy` | string | `"round_robin"` | Read routing strategy (see below). |
| `read_write_split` | bool | `true` | Route writes to the primary, reads to standby/replica nodes. |
| `latency_threshold_ms` | u64 | `100` | Latency above which a node is treated as unhealthy for routing. |

### Routing Strategies

| Strategy | Description |
|----------|-------------|
| `round_robin` | Rotate through nodes equally. |
| `weighted_round_robin` | Rotate proportionally to each node's `weight`. |
| `least_connections` | Route to the node with the fewest active connections. |
| `latency_based` | Route to the lowest-latency node. |
| `random` | Pick a node at random. |

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
| `success_threshold` | u32 | `2` | Consecutive successes before marking a node healthy again. |
| `check_query` | string | `"SELECT 1"` | Health-check query. |

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
| `enabled` | bool | — | Yes* | Whether the node is routable. Toggleable at runtime via the admin API. |
| `name` | string | *(none)* | No | Human-readable name for logs/metrics/admin. |

\* `weight` and `enabled` have no serde default — supply them explicitly per node.

### Node Roles

| Role | Description |
|------|-------------|
| `primary` | Read/write node. All writes and transaction-control statements route here. At least one required. |
| `standby` | Promotable standby. Eligible for failover; receives reads when `read_write_split` is on. |
| `replica` | Read-only replica. Not promotable; receives reads only. |

---

## TLS (`[tls]`)

Optional TLS termination for client connections. Omit the whole section to disable.

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
`enabled = true`.

```toml
[cache]
enabled = true
ttl_secs = 300
max_result_bytes = 1048576
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Serve read SELECT results from the L1/L2 cache. |
| `ttl_secs` | u64 | `300` | Time-to-live for cached results. |
| `max_result_bytes` | usize | `1048576` | Largest single result to cache; larger results bypass. |

---

## Lag-Aware Routing (`[lag_routing]`)

Replica-lag-aware routing + read-your-writes. Only enforced with the `lag-routing`
feature and `enabled = true`.

```toml
[lag_routing]
enabled = true
ryw_window_ms = 500
max_lag_bytes = 0
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable lag-aware read routing + read-your-writes. |
| `ryw_window_ms` | u64 | `500` | Reads within this many ms of a write in the same session pin to the primary (read-your-writes). 0 disables the window. |
| `max_lag_bytes` | u64 | `0` | Exclude a standby when its replication lag exceeds this many bytes. 0 = no lag-based exclusion. |

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
| `max_concurrent` | u32 | `0` | Max concurrent in-flight queries per bucket (0 = engine default). |
| `key_by` | string | `"user"` | Bucket key: `user`, `client_ip`, `database`, `global`. |

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
max_total_idle_backend_conns = 8192
pool_reap_interval_secs = 30
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
| `max_total_idle_backend_conns` | usize | `8192` | Global ceiling on idle backend-pool connections across all `(node,user,db)` identities. Only consumed with the `pool-modes` feature; parsed-and-ignored otherwise. |
| `pool_reap_interval_secs` | u64 | `30` | How often the idle-connection reaper runs. |

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

Per-tenant row isolation via injected predicates. Only active with the `multi-tenancy`
feature and `enabled = true`.

```toml
[multi_tenancy]
enabled = true
identify_by = "application_name"
tenant_column = "tenant_id"
tenant_tables = ["orders", "invoices"]
tenants = ["acme", "globex"]
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enforce per-tenant row isolation. |
| `identify_by` | string | `"application_name"` | Connection attribute naming the tenant: a startup parameter name (e.g. `application_name`, `user`) or the literal `database`. |
| `tenant_column` | string | `"tenant_id"` | The row-level tenant column injected into queries. |
| `tenant_tables` | array | `[]` | Tables that get the tenant filter injected; others pass through. |
| `tenants` | array | `[]` | Known tenant ids. |

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
| `trust_root` | string | *(none)* | Ed25519 trust-root directory. When set, every `.wasm` requires a sidecar `.sig` verifying against a `*.pub` in this directory; when omitted, signatures are not checked. |

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
| `default_ttl_secs` | u64 | `60` | Default TTL for cache entries when the home supplies none. Must be ≥ 1 when edge is enabled. |
| `max_entries` | usize | `10000` | Cache entries before LRU eviction. |
| `max_edges` | usize | `32` | *(home)* Maximum simultaneously-registered edges. |
| `liveness_window_secs` | u64 | `120` | *(home)* Edges not seen within this window are GC-pruned. Keep comfortably above ~45s. Must be ≥ 1 when edge is enabled. |
| `subscribe_gc_secs` | u64 | `30` | *(home)* Registry GC sweep cadence. Must be ≥ 1 when edge is enabled. |
| `region` | string | `""` | *(edge)* Region label reported when subscribing. |
| `edge_id` | string | `""` | *(edge)* Stable registration id (empty → `edge-<pid>`). |

An `edge`-role proxy also requires at least one `[[nodes]]` entry pointing at the home's
PG-wire listener (its data plane), and cannot be combined with `[cache] enabled = true`
(the query-result cache does not receive edge invalidations).

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
