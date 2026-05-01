---
name: heliosproxy-connect
description: Connect a PostgreSQL client to HeliosProxy. psql / asyncpg / jdbc / Rust tokio-postgres / Go pgx. Sanity-check `SELECT 1`. Force routing with `/*+ route=primary */` hints. Use when the user says "connect to the proxy", "psql -h proxy", "first SQL round-trip", "force read to primary".
allowed-tools: Bash(psql *), Bash(curl *), Read
related: [heliosproxy-overview, heliosproxy-topology, heliosproxy-health]
---

# Connect to HeliosProxy

The proxy speaks PostgreSQL wire protocol — any PG client connects
to it the same way it would to a real PG server. Default port: `6432`.

## When to use

- First SQL round-trip after `heliosproxy-start`
- Verifying TLS / auth path before app rollout
- Forcing a query to a specific role (primary / standby) via hint
- Checking which node a query was routed to (via `/api/sql`)

🔵 Read-only (the SELECT is) — but a real client opens a session
that counts in `/sessions` until close.

## Surfaces

| Client | Notes |
|---|---|
| `psql`               | The reference client. Should always work. |
| `pgx` (Go)            | Set `prefer_simple_protocol=true` for early demos |
| `asyncpg` (Python)   | Pass the proxy host:port like any PG server |
| `tokio-postgres`     | Same |
| JDBC                  | `jdbc:postgresql://proxy:6432/db?...` |
| `POST /api/sql`      | Admin REST: returns `{routed_to, node_role}` for visibility |

## Recipes

### Recipe 1: Round-trip with psql

```bash
psql -h localhost -p 6432 -U postgres -d demo -c "SELECT 1"
# server version + ' 1' on the value line
```

If it hangs: probably PG listen-port mismatch. `curl /version` to
confirm proxy is up; check `[server] listen_address` in `proxy.toml`.

### Recipe 2: Pin to primary with a route hint

```sql
/*+ route=primary */ SELECT current_setting('server_version');
/*+ route=standby */ SELECT * FROM read_only_view LIMIT 10;
/*+ route=node:pg-replica-2:5432 */ SELECT 1;
```

Hints survive a `psql -c` invocation if quoted carefully:

```bash
psql -h localhost -p 6432 -U postgres -d demo \
  -c "/*+ route=primary */ SELECT pg_is_in_recovery()"
```

The `routing-hints` feature must be enabled in the build (see
`heliosproxy-install`).

### Recipe 3: Inspect routing with the admin SQL endpoint

```bash
curl -s -X POST http://localhost:9090/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1"}' | jq .
```

```json
{
  "query_type": "read",
  "routed_to":  "pg-replica-1:5432",
  "node_role":  "standby",
  "result":     {"rows":[[1]],"rowCount":1}
}
```

Useful in CI to assert read-vs-write classification without parsing
proxy logs. Switch to `INSERT INTO …` and you'll see
`"query_type":"write","routed_to":"pg-primary:5432"`.

### Recipe 4: TLS to the proxy

```bash
PGPASSWORD=secret psql \
  -h proxy.example.com -p 6432 -U app -d app_db \
  "sslmode=verify-full sslrootcert=/etc/ssl/certs/proxy-ca.pem"
```

Configure server-side TLS in `proxy.toml`'s `[tls]` block; see
`heliosproxy-config`.

### Recipe 5: Application drivers — connection-string form

```ini
# Python (asyncpg, psycopg)
postgres://app:secret@proxy:6432/app_db?sslmode=require

# Go (pgx)
postgres://app:secret@proxy:6432/app_db?sslmode=require&pool_max_conns=20

# JDBC
jdbc:postgresql://proxy:6432/app_db?user=app&password=secret&sslmode=require

# Rust tokio-postgres
"host=proxy port=6432 user=app password=secret dbname=app_db sslmode=require"
```

Apps should treat the proxy as a regular PG server. If your driver
needs `prepared_statements=disable` against PgBouncer Transaction
mode, the same applies to HeliosProxy in Transaction mode (see
`heliosproxy-config` `[pool_mode]`).

### Recipe 6: Smoke-test all backends through the proxy

```bash
for query in \
  "SELECT pg_is_in_recovery() AS standby" \
  "/*+ route=primary */ SELECT pg_is_in_recovery() AS primary"
do
  psql -h localhost -p 6432 -U postgres -d demo -At -c "$query"
done
# false   ← primary
# true    ← standby (correctly routed to a replica)
```

## Pitfalls

- **`FATAL: SSL required`** when proxy is in TLS mode but driver isn't —
  check `[tls.client]` in `proxy.toml` and pass `sslmode=require` (or
  stronger) on the connection string.
- **`unsupported startup parameter: …`** — proxy passes through most
  parameters, but a few (`replication`, `client_encoding` overrides
  on some drivers) need explicit allow-list. Check the proxy log.
- **PgBouncer-Transaction-mode caveats apply** to HeliosProxy in
  Transaction or Statement modes: prepared statements break unless
  the driver disables them or uses Named mode. See `[pool_mode]`.
- **A sticky session pinned by route hint stays sticky** for the
  duration of the transaction. `BEGIN; /*+ route=standby */ SELECT
  …` keeps subsequent statements on that standby until COMMIT.
- **`/api/sql` is admin-grade** — it executes with the proxy's
  credentials, not the caller's. Don't expose it externally; it's
  for ops/CI on the admin port.
- **No connection-pooling on the admin /api/sql path.** Every call
  opens a fresh backend connection. Fine for spot checks; bad for
  benchmarks.

## See also

- `heliosproxy-topology` — see which backend you're talking to
- `heliosproxy-health` — confirm the proxy + backends before debugging connects
- `heliosproxy-config` — `[tls]`, `[pool_mode]`, route-hint feature gate
- Demo: [`demos/v0.4.0/10-admin-rest/`](../../demos/v0.4.0/10-admin-rest/) — `/api/sql` tour
- Code: [`src/server.rs`](../../src/server.rs) — wire-protocol handler
- Code: [`src/admin.rs`](../../src/admin.rs) — `/api/sql` implementation
