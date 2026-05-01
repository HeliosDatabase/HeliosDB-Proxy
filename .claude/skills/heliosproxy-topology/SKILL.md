---
name: heliosproxy-topology
description: Inspect cluster topology — current primary, healthy/unhealthy nodes, per-node detail, raw config. Enable / disable a node. Use when the user says "what's the primary", "topology", "/topology", "which nodes are up", "disable that replica", or "currentPrimary is null".
allowed-tools: Bash(curl *), Read
related: [heliosproxy-overview, heliosproxy-health, heliosproxy-switchover, heliosproxy-chaos]
---

# Topology & node management

The proxy maintains live state for every backend node: role
(primary / standby / read-replica), health, replication lag,
last failure. `GET /topology` joins config + health into one
response designed to feed an external controller or operator.

## When to use

- Confirming which node is the current primary
- Counting healthy vs unhealthy backends
- Disabling a node (cordoning) without removing it from config
- Reading replication lag per replica
- Diagnosing "currentPrimary: null" after a failover

🔵 Read for `/topology`, `/nodes`, `/config`
🟠 Mutating for `/nodes/{addr}/{enable,disable}`

## Surfaces

| Endpoint | Returns / Effect |
|---|---|
| `GET /topology`             | `{currentPrimary, healthyNodes, unhealthyNodes, totalNodes, lastFailoverAt, nodes:[…]}` |
| `GET /nodes`                | array of `NodeHealthResponse` |
| `GET /nodes/{addr}`         | one node's detail or 404 |
| `GET /config`               | redacted config snapshot — listen, admin, nodes |
| `POST /nodes/{addr}/enable` | restore routing to a node previously disabled |
| `POST /nodes/{addr}/disable`| stop routing to the node immediately |

## Recipes

### Recipe 1: One-shot topology view

```bash
curl -s http://localhost:9090/topology | jq .
```

```json
{
  "currentPrimary":  "pg-primary:5432",
  "healthyNodes":    2,
  "unhealthyNodes":  0,
  "totalNodes":      2,
  "lastFailoverAt":  null,
  "nodes": [
    {"address":"pg-primary:5432","role":"primary","healthy":true,"replication_lag_bytes":null,"latency_ms":1.2},
    {"address":"pg-replica-1:5432","role":"standby","healthy":true,"replication_lag_bytes":4096,"latency_ms":1.4}
  ]
}
```

Field names match the operator's `HeliosProxyStatus` CRD field
names (camelCase) — designed for direct CR status patching.

### Recipe 2: Per-node detail

```bash
curl -s http://localhost:9090/nodes/pg-replica-1:5432 | jq .
```

```json
{
  "address":               "pg-replica-1:5432",
  "healthy":               true,
  "last_check":            "2026-05-01T13:25:01Z",
  "failure_count":         0,
  "last_error":            null,
  "latency_ms":            1.4,
  "replication_lag_bytes": 4096
}
```

`failure_count` resets to 0 the first time a check passes after a
failure.

### Recipe 3: List all nodes

```bash
curl -s http://localhost:9090/nodes | jq '.[] | {address, role, healthy, latency_ms}'
```

`role` reflects the **configured** role from `proxy.toml`. After a
failover, the configured role and the live role can diverge — check
`/topology.currentPrimary` for the live primary.

### Recipe 4: Cordon a node (disable routing)

```bash
curl -s -X POST http://localhost:9090/nodes/pg-replica-2:5432/disable
# {"status":"disabled"}
```

The node stays in config but receives no new connections. Active
sessions on that node are not killed — they finish naturally.
Health checks continue, so the node's `healthy` field still updates.

To restore:

```bash
curl -s -X POST http://localhost:9090/nodes/pg-replica-2:5432/enable
# {"status":"enabled"}
```

Both endpoints are idempotent.

### Recipe 5: Watch topology during a chaos drill

```bash
# Terminal 1 — watch
watch -n 1 'curl -s http://localhost:9090/topology | jq "{currentPrimary, healthyNodes, unhealthyNodes}"'

# Terminal 2 — induce failure
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"force_unhealthy","target_node":"pg-primary:5432"}'

# Watch the primary flip and `lastFailoverAt` populate
```

See `heliosproxy-chaos` and `heliosproxy-switchover` for the full
fail-over walk-through.

### Recipe 6: Dump the redacted config

```bash
curl -s http://localhost:9090/config | jq .
```

```json
{
  "listen_address": "0.0.0.0:6432",
  "admin_address":  "0.0.0.0:9090",
  "nodes": [
    {"address":"pg-primary:5432","role":"primary","weight":1,"enabled":true},
    {"address":"pg-replica-1:5432","role":"standby","weight":1,"enabled":true}
  ]
}
```

Secrets, plugin trust roots, and TLS material are stripped.

## Pitfalls

- **`currentPrimary: null` is normal during a failover** — the
  primary-tracker is mid-decision. Wait a health-check cycle
  (`[health].check_interval_secs`, default 5 s). If it stays null
  for >30 s, see `heliosproxy-switchover`.
- **`role: "primary"` plus `healthy: false`** → the node is the
  configured primary but unreachable. The proxy may have promoted a
  standby; check `/topology.currentPrimary` for the live one.
- **Disabling the only primary** disables the entire write path
  immediately. The proxy doesn't auto-promote a standby just because
  you cordoned a primary. Use chaos `force_unhealthy` instead — it
  triggers the failover machinery (see `heliosproxy-chaos`).
- **`replication_lag_bytes: null` on a standby** means the proxy
  hasn't been able to read the lag yet. Common during the first
  health-check cycle after start. Persistent null = check
  `[health]` config and the standby's `pg_stat_wal_receiver`.
- **`/config` is redacted** — don't grep it for passwords, they're
  not there. Look at the source `proxy.toml` instead.

## See also

- `heliosproxy-health` — `/health`, `/metrics`, `/version` for liveness
- `heliosproxy-chaos` — induce failure to test topology behaviour
- `heliosproxy-switchover` — what happens during failover, end-to-end
- Code: [`src/admin.rs`](../../src/admin.rs) — `/topology`, `/nodes` impl
- Code: [`src/primary_tracker.rs`](../../src/primary_tracker.rs) — primary detection
- Demo: [`demos/v0.4.0/10-admin-rest/`](../../demos/v0.4.0/10-admin-rest/) — full curl tour
