# HeliosProxy Admin API Reference

The Admin API provides REST endpoints for monitoring, management, HA/migration control, and SQL routing. It runs on a dedicated TCP listener, separate from the PostgreSQL client port.

**Default address:** `127.0.0.1:9090` (loopback). Configurable via `admin_address` in `proxy.toml` or `--admin` on the command line.

All responses use `Content-Type: application/json` unless otherwise noted. The two exceptions are the embedded web UI (`GET /` and `/ui`, served as `text/html`) and the edge SSE stream (`GET /api/edge/subscribe`, `text/event-stream`).

Source of truth for everything below is the dispatch `match` in `src/admin.rs`.

---

## Security Model

### Bind default

The admin API is privileged (it can execute SQL, disable nodes, force chaos faults, cut over migrations). It therefore defaults to loopback (`127.0.0.1:9090`). If you set a **non-loopback** `admin_address` (e.g. `0.0.0.0:9090`), the proxy **refuses to start** unless one of the following is also true:

- `admin_token` is set (bearer-token auth is enabled), **or**
- `admin_allow_insecure = true` is set (explicit opt-in to an unauthenticated non-loopback bind).

This guard lives in `ProxyConfig::validate` (`src/config.rs`) and only fires for non-loopback binds; a loopback bind with no token is allowed.

### Bearer-token authentication

When `admin_token` is set, **every route requires** `Authorization: Bearer <token>`, verified with a constant-time compare, **except** the token-exempt liveness paths (below) and the static web-UI shell (`GET /`, `/ui`) — the shell holds no privileged data and injects the token into its own API calls client-side (see [Web UI](#web-ui)). Requests without a valid token get `401 Unauthorized` with body `{"error":"missing or invalid admin bearer token"}`.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/nodes
```

When `admin_token` is **unset**, no route requires a token (rely on the loopback bind for protection).

### Token-exempt health paths

The auth gate exempts a fixed set of `GET` liveness paths so orchestrators can probe without the token:

```
/health   /healthz   /livez   /readyz
```

The static web-UI shell (`GET /`, `/ui`) is also served without a token (it holds no privileged data); see [Web UI](#web-ui).

All four are routed and token-exempt. The z-suffixed paths are Kubernetes-style aliases of the slash-form health routes and return byte-for-byte the same responses:

| Path | Alias of | Response |
|------|----------|----------|
| `/health` | — | 200 `{"status":"ok"}` |
| `/healthz` | `/health` | 200 `{"status":"ok"}` |
| `/livez` | `/health/live` | 200 `{"alive":true}` |
| `/readyz` | `/health/ready` | 200 if ≥1 healthy backend, else 503 |

The slash-form `/health/live` and `/health/ready` routes are **not** token-exempt, so when `admin_token` is set they require the bearer token like any other route — use the z-suffixed aliases (`/livez`, `/readyz`) for unauthenticated probes.

**Recommendation for Kubernetes probes:** point the liveness probe at `/healthz` (or `/livez`) and the readiness probe at `/readyz` — all three are open (no token) and always routed.

### Connection & request caps

The admin listener is hardened against slow-loris and oversized-request abuse with hard caps (constants in `src/admin.rs`):

| Cap | Value | Meaning |
|-----|-------|---------|
| `MAX_ADMIN_CONNS` | 256 | Max concurrent admin connections (semaphore-bounded). |
| `ADMIN_READ_TIMEOUT` | 15 s | Bounds the request-line/header/body read phase; a stalled reader is dropped. |
| `MAX_ADMIN_HEADERS` | 100 | Max header lines per request. |
| `MAX_ADMIN_HEADER_BYTES` | 64 KiB | Max total header bytes. |
| `MAX_ADMIN_BODY_BYTES` | 8 MiB | Max request body; larger `Content-Length` is rejected. |
| `ADMIN_SSE_WRITE_TIMEOUT` | 30 s | Per-write timeout on the long-lived edge SSE stream (paired with a 15 s heartbeat) so a wedged subscriber can't hold a connection permit forever. |

---

## Feature gating

The default build (`default = ["pool-modes"]`) compiles in only a subset of routes. Feature-gated routes are **always present in the dispatch table**, but return **`503 Service Unavailable`** with an explanatory `error` message when their cargo feature is not compiled in (a full build uses `--features all-features`). A handful of routes are always routed but return `503` when the corresponding subsystem is not configured at runtime (mirror/migration, branch databases, plugin manager).

---

## Endpoint Summary

Auth column: **token** = requires bearer token when `admin_token` is set; **open** = token-exempt.

| Method | Path | Purpose | Feature gate | Auth |
|--------|------|---------|--------------|------|
| `GET` | `/health` | Liveness (`{"status":"ok"}`) | — | open |
| `GET` | `/healthz` | Liveness — alias of `/health` | — | open |
| `GET` | `/health/live` | Liveness (`{"alive":true}`) | — | token |
| `GET` | `/livez` | Liveness — alias of `/health/live` | — | open |
| `GET` | `/health/ready` | Readiness — 200 if ≥1 healthy backend, else 503 | — | token |
| `GET` | `/readyz` | Readiness — alias of `/health/ready` | — | open |
| `GET` | `/metrics` | Server metrics (JSON) | — | token |
| `GET` | `/metrics/prometheus` | Server metrics (Prometheus text, wrapped in JSON `text`) | — | token |
| `GET` | `/version` | Proxy version | — | token |
| `GET` | `/config` | Current configuration snapshot | — | token |
| `GET` | `/topology` | Primary + healthy/unhealthy node sets in one call | — | token |
| `GET` | `/nodes` | All backend nodes with health | — | token |
| `GET` | `/nodes/{addr}` | Single node health (404 if unknown) | — | token |
| `POST` | `/nodes/{addr}/enable` | Re-enable a node into routing | — | token |
| `POST` | `/nodes/{addr}/disable` | Remove a node from routing | — | token |
| `GET` | `/sessions` | Active client session count | — | token |
| `GET` | `/pools` | Per-node connection pool stats | — | token |
| `POST` | `/api/sql` | Execute SQL with transparent write routing | — | token |
| `GET` | `/plugins` | Loaded WASM plugins (503 if manager not attached) | `wasm-plugins` | token |
| `GET` | `/admin/kv/{plugin}/{key}` | Read a plugin KV value (`{"plugin","key","value"}`; 404 if absent) | `wasm-plugins` | token |
| `GET` | `/admin/kv/{plugin}/` | List a plugin's KV keys (trailing slash) | `wasm-plugins` | token |
| `PUT` | `/admin/kv/{plugin}/{key}` | Set a plugin KV value (UTF-8 body; 413 on cap breach) | `wasm-plugins` | token |
| `DELETE` | `/admin/kv/{plugin}/{key}` | Delete a plugin KV value (idempotent 200) | `wasm-plugins` | token |
| `GET` | `/anomalies` | Anomaly-detector recent events (`?limit=N`) | `anomaly-detection` | token |
| `GET` | `/analytics`, `/api/analytics` | Top queries + slow-query log (`?limit=N`) | `query-analytics` | token |
| `GET` | `/api/chaos` | Read current chaos overrides | — | token |
| `POST` | `/api/chaos` | Inject/clear a fault (`force_unhealthy`/`restore`/`reset`) | — | token |
| `POST` | `/api/replay` | Replay a journal window against a target backend | `ha-tr` | token |
| `POST` | `/api/shadow` | Dual-execute a query and diff the results | `ha-tr` | token |
| `GET` | `/api/circuit` | Per-node circuit-breaker state | `circuit-breaker` | token |
| `GET` | `/api/edge` | Edge/geo cache + registered-edge stats | `edge-proxy` | token |
| `POST` | `/api/edge/register` | Register an edge with the home proxy | `edge-proxy` | token |
| `POST` | `/api/edge/invalidate` | Broadcast a table-level invalidation | `edge-proxy` | token |
| `GET` | `/api/edge/subscribe` | Long-lived SSE invalidation stream (`?edge_id=…`) | `edge-proxy` | token |
| `GET` | `/api/migration/status` | Traffic-mirror / migration status (503 if mirror off) | — | token |
| `POST` | `/api/migration/snapshot` | Snapshot-bootstrap named tables into the mirror | — | token |
| `POST` | `/api/migration/cutover` | Promote the mirror target to primary | — | token |
| `POST` | `/api/migration/cutover/rollback` | Revert a cutover to the original primary | — | token |
| `GET` | `/api/branch`, `/branch` | List branch databases (503 if branching off) | — | token |
| `POST` | `/api/branch`, `/branch` | Create a branch database | — | token |
| `DELETE` | `/api/branch`, `/branch` | Drop a branch database (`?name=…`) | — | token |
| `GET` | `/`, `/ui` | Embedded admin web UI (HTML) — static shell only; its API calls are gated | — | open |

> The `/api/migration/*` and `/api/branch` routes also accept the same paths without the `/api` prefix (e.g. `/migration/cutover`, `/branch`) — both spellings are wired.

Any path/method not in this table returns `404 {"error":"Not found"}`.

---

## Health Endpoints

### GET /health, GET /healthz

Basic liveness. Always 200 while the process is running. **Token-exempt** — both spellings are routed and open, so either is a correct target for an unauthenticated liveness probe (`/healthz` is the Kubernetes-conventional alias).

```bash
curl http://localhost:9090/health
curl http://localhost:9090/healthz
```

```json
{ "status": "ok" }
```

### GET /health/live, GET /livez

Simple alive indicator. Always 200. `/health/live` **requires the bearer token** when `admin_token` is set (it is *not* in the token-exempt list); its `/livez` alias is **token-exempt** — prefer `/livez` for unauthenticated probes.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/health/live
curl http://localhost:9090/livez
```

```json
{ "alive": true }
```

### GET /health/ready, GET /readyz

Readiness. Returns 200 if at least one backend node is healthy, `503` otherwise. `/health/ready` **requires the bearer token** when `admin_token` is set; its `/readyz` alias is **token-exempt** — prefer `/readyz` for unauthenticated probes.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/health/ready
curl http://localhost:9090/readyz

```json
{ "ready": true, "message": "Proxy is ready" }
```

```json
{ "ready": false, "message": "Proxy is not ready" }
```

---

## Topology & Node Management

### GET /topology

Joins the config (node roles) with live health so a controller can read the current primary and the healthy/unhealthy node sets in a single, non-blocking round-trip. Designed for polling.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/topology
```

### GET /nodes

List all configured backend nodes with their current health status.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/nodes
```

```json
[
  {
    "address": "db-primary.internal:5432",
    "healthy": true,
    "last_check": "2026-07-08T10:15:30.123Z",
    "failure_count": 0,
    "last_error": null,
    "latency_ms": 0.5,
    "replication_lag_bytes": null
  }
]
```

| Field | Type | Description |
|-------|------|-------------|
| `address` | string | Node address in `host:port` format. |
| `healthy` | bool | Whether the node is currently passing health checks. |
| `last_check` | string | ISO 8601 timestamp of the most recent health check. |
| `failure_count` | u32 | Consecutive health-check failures; resets to 0 on success. |
| `last_error` | string \| null | Error from the most recent failed health check. |
| `latency_ms` | f64 | Round-trip latency of the most recent successful check. |
| `replication_lag_bytes` | u64 \| null | Replication lag in bytes (standby/replica only). |

### GET /nodes/{address}

Single node health. `{address}` is the node's `host:port` string. `404 {"error":"Node not found"}` if unknown.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://localhost:9090/nodes/db-primary.internal:5432
```

### POST /nodes/{address}/enable — POST /nodes/{address}/disable

Enable re-admits a node into routing; disable removes it (new queries stop routing to it; in-flight work is allowed to finish). Useful for draining a **single backend** for maintenance.

```bash
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://localhost:9090/nodes/db-replica-1.internal:5432/disable
# → {"status":"disabled"}

curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://localhost:9090/nodes/db-replica-1.internal:5432/enable
# → {"status":"enabled"}
```

> Draining the **whole proxy** is a different operation: send `SIGUSR2` for a graceful drain (bounded by `shutdown_drain_timeout_secs`). There is no `/drain` route.

---

## Failover & Chaos

Failover between backends is **automatic** (health-driven, with transaction replay when `ha-tr` is enabled). There is **no `/failover` endpoint.** To *force* a failover for testing, mark a node unhealthy via the chaos API.

### GET /api/chaos

Read the current chaos overrides — "what is broken on purpose right now".

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/api/chaos
```

### POST /api/chaos

Inject or clear a controlled fault. Supported actions: `force_unhealthy`, `restore`, `reset`.

```bash
# Force the primary unhealthy → triggers automatic failover to a standby
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action":"force_unhealthy","target_node":"db-primary.internal:5432"}' \
  http://localhost:9090/api/chaos

# Restore that node
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action":"restore","target_node":"db-primary.internal:5432"}' \
  http://localhost:9090/api/chaos

# Clear all chaos overrides at once
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action":"reset"}' \
  http://localhost:9090/api/chaos
```

### GET /api/circuit

Per-node circuit-breaker state (`closed` / `open` / `half-open`), so an operator can see which backends the breaker has tripped. **`503` unless built with `--features circuit-breaker`.**

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/api/circuit
```

---

## Metrics, Sessions, Pools, Version, Config

### GET /metrics

Server metrics in JSON.

```json
{
  "connections_accepted": 15234,
  "connections_closed": 15100,
  "connections_active": 134,
  "queries_processed": 892451,
  "bytes_received": 45623891,
  "bytes_sent": 189234567,
  "failovers": 1
}
```

`connections_active` is computed as `accepted − closed`.

### GET /metrics/prometheus

Prometheus text exposition format, wrapped in a JSON `text` field:

```json
{ "text": "# HELP heliosdb_proxy_connections_total ...\n..." }
```

> This is not directly scrapable as raw Prometheus text — the payload is JSON with the exposition string in `text`, so a scraper must unwrap the `text` field. This endpoint is served unconditionally; there is no separate build flag that changes its format (the `observability` feature only pulls in the `prometheus`/`opentelemetry` crates and wires no exporter — see the feature-flags reference).

### GET /sessions

```json
{ "active_sessions": 42 }
```

### GET /pools

Per-node connection pool statistics (active/idle/pending, lifetime created/closed). Useful for watching a single-node drain complete.

### GET /version

```json
{ "version": "1.5.0", "build_time": "1.5.0" }
```

> Both fields derive from the crate version (`CARGO_PKG_VERSION`); `build_time` is the version string, not a wall-clock timestamp.

### GET /config

Returns a snapshot of the running configuration (secrets such as `admin_token` and TLS material that serialize as `None` are omitted).

---

## SQL Execution API

### POST /api/sql

Execute a SQL query through the proxy with transparent write routing (TWR): writes go to the primary, reads are load-balanced across healthy standby/replica nodes. The proxy forwards to the backend's HTTP SQL API and returns the result plus routing metadata.

```bash
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}' \
  http://localhost:9090/api/sql
```

```json
{
  "query_type": "read",
  "routed_to": "db-standby-1.internal:5432",
  "node_role": "standby",
  "result": { "columns": ["id","name"], "rows": [[1,"Alice"]], "row_count": 1 }
}
```

**Request body**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | yes | SQL to execute. |
| `params` | array | no | Parameters (reserved for prepared-statement support). |

**Write classification** — routed to the primary when the statement starts with: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `UPSERT`, `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `GRANT`, `REVOKE`, `VACUUM`, `REINDEX`, `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`. Everything else (chiefly `SELECT`) is a read. `node_role` is reported as `primary` / `standby` / `replica`.

**Error responses** include `{"error":"No healthy primary node available"}` and `{"error":"Empty query"}`.

---

## Observability & Diagnostics (feature-gated)

### GET /plugins

Loaded WASM plugins — name, version, description, hooks, state, invocation count. Returns `503 {"error":"plugin manager not attached"}` when the proxy runs without a plugin manager, and `503 {"error":"wasm-plugins feature not compiled in"}` in a build without `--features wasm-plugins`.

### /admin/kv/{plugin}/{key} — plugin runtime KV

Read, write, delete, and list a loaded plugin's key-value state — the same per-plugin namespace the plugin sees through its `kv_get` / `kv_set` host imports. Operators use it to push runtime config (budgets, region maps, mask rules, allowlists) without restarting the proxy. All four verbs sit behind the normal admin bearer gate.

| Method | Path | Behavior |
|--------|------|----------|
| `GET` | `/admin/kv/{plugin}/{key}` | `200 {"plugin","key","value"}`, or `404 {"error":"key not found"}` |
| `GET` | `/admin/kv/{plugin}/` | `200 {"plugin","keys":[...]}` — the trailing slash lists the namespace; an optional `?prefix=` filters the listing |
| `PUT` | `/admin/kv/{plugin}/{key}` | `200 {"ok":true}`, or `413` when a cap is exceeded |
| `DELETE` | `/admin/kv/{plugin}/{key}` | `200 {"ok":true}` — idempotent (200 even when the key is absent); deleting a namespace's last key frees its slot |

- **`{key}` may contain `/`.** The first path segment after `/admin/kv/` is the plugin name; everything after it is the key (e.g. `budget/tenant-a`). Any query string is stripped before the split, so `?…` never leaks into a key or plugin name; on a list request `?prefix=<p>` (percent-decoded) filters the returned keys.
- **Values are UTF-8 text.** PUT bodies are decoded with `String::from_utf8_lossy` (the admin body limit still applies), and GET returns the value as a JSON string. Store binary blobs base64-encoded.
- **Caps** guard against runaway writes; all four are tunable in `[plugins]` and `0` means unlimited. `kv_max_value_bytes` (default 65536) bounds a single key's OR value's length; `kv_max_keys_per_plugin` (default 1024) bounds the distinct keys per namespace; `kv_max_plugins` (default 256) bounds how many `<plugin>` namespaces can exist at once (so a token-holder cannot exhaust memory by writing to unboundedly-many namespace names); and `kv_max_total_bytes` (default 67108864 / 64 MiB) bounds the TOTAL retained size across all namespaces (each entry's key + value bytes plus each live namespace's name bytes) — the single backstop that keeps the whole store within a survivable ceiling regardless of the per-axis product `kv_max_plugins × kv_max_keys_per_plugin × kv_max_value_bytes` (which can otherwise reach tens of GiB). Overwriting an existing key never trips the key-count cap, and writing to an already-present namespace never trips the namespace cap. A PUT that would exceed a cap returns `413` (`{"error":"kv_max_value_bytes exceeded"}` for an oversized body, or `{"error":"kv_max_value_bytes, kv_max_keys_per_plugin, kv_max_plugins, or kv_max_total_bytes exceeded"}`).
- **Keys containing `?` are not addressable here.** The query string is stripped before the plugin/key split (that is what lets `?prefix=` filter a listing), so a key that itself contains `?` — a plugin can create one through `kv_set` — cannot be read or deleted through this endpoint: the strip eats everything from the first `?`, and path segments are not percent-decoded, so `%3F` does not reach it either. Such a key still appears in the trailing-slash listing. Avoid `?` in KV keys.
- **`400`** on a malformed path (`/admin/kv/{plugin}` with no key segment) or an empty `{plugin}` segment (`/admin/kv//{key}`).
- **`405`** on an unsupported method.
- **`503 {"error":"plugin runtime not enabled"}`** when no plugin manager is attached (plugins disabled in config).
- **`501 {"error":"proxy built without the wasm-plugins feature"}`** in a build without `--features wasm-plugins` — note this is `501`, not the `503` other feature-gated routes use, because the entire KV subsystem is absent from the binary.

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-cost-governor/budget/tenant-a \
  --data-raw '{"queries_per_minute":1000}'
curl -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://localhost:9090/admin/kv/helios-plugin-cost-governor/budget/tenant-a
# {"plugin":"helios-plugin-cost-governor","key":"budget/tenant-a","value":"{\"queries_per_minute\":1000}"}
```

### GET /anomalies

Recent events from the in-process anomaly detector (SQL-injection heuristics, auth bursts, rate spikes, novel query shapes), newest-first. Optional `?limit=N` clamps the response (default 100). **`503` unless built with `--features anomaly-detection`.**

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" "http://localhost:9090/anomalies?limit=20"
```

### GET /analytics (alias /api/analytics)

Top queries by call count plus the slow-query log. Optional `?limit=N` (default 50). **`503` unless built with `--features query-analytics`.**

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" "http://localhost:9090/api/analytics?limit=50"
```

---

## HA / Time-Travel (feature `ha-tr`)

### POST /api/replay

Replay a window of the transaction journal against a target backend (typically a staging DB) — for failover validation, hydrating staging from prod, or forensics. Body is a `ReplayRequestBody`. **`503 {"error":"ha-tr feature not compiled in"}`** without the feature.

### POST /api/shadow

Run a query against a source **and** a shadow backend in parallel and diff the results — used for major-version-upgrade validation, schema-migration canaries, and replica-drift detection. Body is a `ShadowRequestBody`. **`503`** without the `ha-tr` feature.

---

## Edge / Geo Mode (feature `edge-proxy`)

All edge routes return `503 {"error":"edge-proxy feature not compiled in"}` in a build without `--features edge-proxy`.

### GET /api/edge

Home-side stats: registered edges and cross-region cache stats.

### POST /api/edge/register

Register an edge with the home proxy (ack-only compatibility path; the long-lived stream is `/subscribe`).

### POST /api/edge/invalidate

Broadcast a table-level invalidation to subscribed edges (last-write-wins TTL coherence). Handy for ops drills.

### GET /api/edge/subscribe

Long-lived **Server-Sent Events** stream of invalidations. Requires an `?edge_id=<id>` query parameter (optional `region`, `base_url`); a missing `edge_id` gets `400`. This route is intercepted before the normal one-shot dispatch (a `Content-Length`-framed JSON response cannot hold a stream open), but the bearer-token gate still applies — an unauthenticated subscribe gets the same `401`. Each write is bounded by `ADMIN_SSE_WRITE_TIMEOUT` (30 s) and a 15 s heartbeat keeps the connection permit from leaking.

```bash
curl -N -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:9090/api/edge/subscribe?edge_id=eu-west-1"
```

---

## Migration / Traffic Mirror

These routes are always compiled in but return `503 {"error":"traffic mirroring not enabled"}` unless a `[mirror]` migration is configured. Each path also accepts the non-`/api` spelling.

### GET /api/migration/status

Mirror lag/backlog/drop counters plus a `cutover_active` flag.

### POST /api/migration/snapshot

Snapshot-bootstrap named tables from the source into the mirror. Body: `{"tables":["orders","users"]}` — an empty/missing `tables` array returns `400`. Response reports per-table rows copied and a `rows_copied` total.

### POST /api/migration/cutover

Promote the mirror target to primary — new connections route there. If the mirror is not `migration_ready` (backlog/drops present) the call returns `409` with the current status; pass `force=true` (query string or `{"force":true}` body) to override.

```bash
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:9090/api/migration/cutover?force=true"
```

### POST /api/migration/cutover/rollback

Revert a cutover so new connections route to the original primary again.

---

## Branch Databases

Always compiled in; each returns `503 {"error":"branch databases not enabled"}` unless `[branch]` is configured.

```bash
# List
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/api/branch

# Create (base optional; defaults to the configured base database)
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"feature-x","base":"main"}' \
  http://localhost:9090/api/branch

# Drop
curl -X DELETE -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:9090/api/branch?name=feature-x"
```

Creating without a `name` returns `400 {"error":"provide 'name'"}`; dropping without `?name=` returns `400 {"error":"provide ?name=<branch>"}`.

---

## Web UI

`GET /` and `GET /ui` serve an embedded admin dashboard (single HTML file compiled into the binary). The static shell is **token-exempt** — it is served even when `admin_token` is set, because the HTML itself carries no privileged data; every API call the dashboard makes (`/nodes`, `/metrics`, `/api/sql`, …) still goes through the bearer gate.

When `admin_token` is set, the dashboard handles auth entirely client-side:

- On first load, the first API call returns `401` and the page prompts **once** for the admin token (the same value as `admin_token` in `proxy.toml`).
- The token is kept in the tab's `sessionStorage` under the key `helios_admin_token` — it dies with the tab, is never written to disk, and never appears in the URL. A wrapped `window.fetch` injects `Authorization: Bearer <token>` into every request (a caller-supplied `Authorization` header still wins).
- A **token** button in the header bar clears the saved token and reloads, so a wrong token can be re-entered without closing the tab.

When `admin_token` is unset, the dashboard works with no prompt (rely on the loopback bind for protection).

---

## Error Handling

All JSON errors share the shape `{"error":"<description>"}`.

| Status | Meaning |
|--------|---------|
| `200` | Success. |
| `400` | Bad request (malformed input, missing required field/param). |
| `401` | Missing/invalid admin bearer token (only when `admin_token` is set). |
| `404` | Unknown node address, or an unrouted path. |
| `409` | Migration cutover blocked (mirror not `migration_ready`); retry with `force=true`. |
| `500` | Internal server error. |
| `503` | Not ready / no healthy backends, **or** a feature/subsystem is not compiled in / not enabled. |

---

## Usage Examples

### Liveness / readiness in a script

```bash
# Liveness — open path, no token needed
curl -sf http://localhost:9090/health >/dev/null && echo "alive"

# Readiness — token required when admin_token is set
if curl -sf -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://localhost:9090/health/ready >/dev/null; then
  echo "ready"
else
  echo "NOT ready" >&2; exit 1
fi
```

### Drain a single backend for maintenance

```bash
NODE=db-standby-1.internal:5432
AUTH="Authorization: Bearer $ADMIN_TOKEN"

curl -X POST -H "$AUTH" "http://localhost:9090/nodes/$NODE/disable"

while true; do
  active=$(curl -s -H "$AUTH" http://localhost:9090/pools | \
    jq --arg n "$NODE" '.[] | select(.node == $n) | .active_connections')
  [ "$active" = "0" ] && break
  echo "waiting for $active connections to drain..."; sleep 5
done

# ...maintenance...
curl -X POST -H "$AUTH" "http://localhost:9090/nodes/$NODE/enable"
```

### Force a failover drill

```bash
AUTH="Authorization: Bearer $ADMIN_TOKEN"
# Mark the primary unhealthy → automatic failover kicks in
curl -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"action":"force_unhealthy","target_node":"db-primary.internal:5432"}' \
  http://localhost:9090/api/chaos

curl -s -H "$AUTH" http://localhost:9090/topology | jq '.currentPrimary'

# Restore when done
curl -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"action":"reset"}' http://localhost:9090/api/chaos
```

### Prometheus scrape

```yaml
scrape_configs:
  - job_name: heliosproxy
    metrics_path: /metrics/prometheus
    static_configs:
      - targets: ["heliosproxy:9090"]
```

Note: `/metrics/prometheus` wraps the exposition text in a JSON `text` field, so the scrape job must unwrap `text` before parsing. There is no build flag that emits raw Prometheus text (the `observability` feature adds the `prometheus`/`opentelemetry` crates but wires no exporter). If `admin_token` is set, add the bearer token to the scrape job's `authorization` config.

---

## See Also

- [Architecture](architecture.md)
- [Configuration Reference](configuration.md)
- [Deployment Guides](deployment/)
</content>
</invoke>
