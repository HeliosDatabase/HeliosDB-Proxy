---
name: heliosproxy-health
description: Liveness + readiness + metrics + sessions + pools + version. Every read-only observability endpoint. Use when the user says "is the proxy up", "Prometheus", "metrics", "what to alert on", "active sessions", "pool stats", or wires up a health-check probe.
allowed-tools: Bash(curl *), Read
related: [heliosproxy-overview, heliosproxy-topology, heliosproxy-shutdown]
---

# Health, metrics, sessions, pools

Every read-only endpoint an operator (or their alerting / load
balancer) calls to know whether the proxy is alive, ready, and
performing. All on the admin port (default `9090`).

## When to use

- Liveness / readiness probe wiring (k8s, docker-compose healthcheck)
- Prometheus scrape config
- Capacity / pool-utilization checks
- "Why is the proxy slow" first-pass triage
- Confirming a build's version + features

🔵 Read-only

## Endpoint cheatsheet

| Endpoint | Code | Purpose |
|---|---|---|
| `GET /health`           | 200 | always returns `{"status":"ok"}` if the admin loop is running |
| `GET /health/live`      | 200 | "alive" — same semantics as `/health`, separated for k8s style |
| `GET /health/ready`     | 200/503 | 200 once ≥1 backend is healthy and not draining |
| `GET /version`          | 200 | `{version, build_time}` (and `features` where built in) |
| `GET /metrics`          | 200 | JSON metrics — connections, queries, bytes, failovers |
| `GET /metrics/prometheus` | 200 | text/plain Prometheus exposition (via `observability` feature) |
| `GET /sessions`         | 200 | `{active_sessions: N}` |
| `GET /pools`            | 200 | per-node pool stats (active, idle, pending) |

## Recipes

### Recipe 1: Liveness + readiness probes (k8s)

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health/ready
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

Liveness must NOT use `/health/ready` — during a clean drain,
ready flips to 503 but the process is still healthy. K8s would
restart it instead of letting it drain.

### Recipe 2: Quick triage one-liner

```bash
curl -s http://localhost:9090/version | jq .
curl -s http://localhost:9090/metrics | jq .
curl -s http://localhost:9090/sessions
curl -s http://localhost:9090/pools | jq .
```

A typical healthy snapshot:

```json
{"version":"0.4.1","build_time":"2026-05-01T13:20:05Z"}
{"connections_accepted":17234,"connections_closed":17190,"connections_active":44,"queries_processed":982041,"bytes_received":108112334,"bytes_sent":2014223891,"failovers":0}
{"active_sessions":44}
[{"node":"pg-primary:5432","active":12,"idle":8,"pending":0},{"node":"pg-replica-1:5432","active":31,"idle":4,"pending":0}]
```

### Recipe 3: Prometheus scrape

```yaml
# prometheus.yml
scrape_configs:
  - job_name: heliosproxy
    static_configs:
      - targets: ['proxy:9090']
    metrics_path: /metrics/prometheus
    scrape_interval: 15s
```

Sample exported metrics:

```
# HELP heliosproxy_connections_active Active client connections
# TYPE heliosproxy_connections_active gauge
heliosproxy_connections_active 44
# HELP heliosproxy_queries_total Queries processed since start
# TYPE heliosproxy_queries_total counter
heliosproxy_queries_total 982041
# HELP heliosproxy_failovers_total Failovers since start
# TYPE heliosproxy_failovers_total counter
heliosproxy_failovers_total 0
```

The `observability` feature adds `prometheus` + `opentelemetry`
deps — `cargo install --features observability`.

### Recipe 4: What to alert on

| Symptom | Query / metric | Severity |
|---|---|---|
| `/health/ready` 503 for >2 min | probe failure | critical |
| `failovers_total` increased | `rate(heliosproxy_failovers_total[5m]) > 0` | high |
| `connections_active` near `[pool] max_connections` | gauge ratio > 0.9 | warning |
| `pool.pending > 0` for >30 s | `/pools` poll | warning |
| `queries_processed` rate ≈ 0 with non-zero `connections_active` | drop in ingest | high |
| 5xx from `/metrics` itself | proxy admin loop wedged | critical |

### Recipe 5: Pool utilization snapshot

```bash
curl -s http://localhost:9090/pools | jq '
  .[] | {node, used: .active, idle, pending,
         pct: (.active * 100 / (.active + .idle))}'
```

```
{"node":"pg-primary:5432","used":12,"idle":8,"pending":0,"pct":60}
{"node":"pg-replica-1:5432","used":31,"idle":4,"pending":0,"pct":89}
```

`pct: 89` on a single replica suggests adding a replica, raising
`[pool] max_connections`, or routing more traffic to other replicas
(weight changes — see `heliosproxy-config`).

### Recipe 6: Confirm features compiled in

```bash
curl -s http://localhost:9090/version | jq .
```

If the response includes a `features` array, it lists the compiled-in
feature flags. (Older versions don't expose this — fall back to
checking `Cargo.toml` of the build.)

If a feature-gated endpoint returns 503 (e.g. `/api/replay`), the
feature is OFF in the build — see `heliosproxy-install` to rebuild
with `--features ha-tr`.

## Pitfalls

- **`/health` always 200, even during drain.** Use `/health/ready`
  for traffic-routing decisions; `/health` is just "the admin loop
  is alive."
- **`/metrics/prometheus` requires the `observability` feature.**
  Without it, the endpoint returns 404. Use `/metrics` (JSON) or
  rebuild.
- **`active_sessions ≠ connections_active`.** Sessions count
  client-facing connections; pool active counts backend-side
  connections. They diverge under multi-statement transactions or
  pool-mode='session' with idle clients.
- **Pool stats need the `pool-modes` feature** (default-on). Without
  it, `/pools` returns an empty array, not 503.
- **Don't poll faster than `[health].check_interval_secs`.**
  `/topology` updates only on health-check ticks; faster polling
  burns CPU without new data.

## See also

- `heliosproxy-topology` — node-level health detail
- `heliosproxy-shutdown` — `/health/ready` flips during drain
- `heliosproxy-anomaly` — separate `/anomalies` endpoint, not metrics
- Code: [`src/admin.rs`](../../src/admin.rs) — endpoint impl
- Code: [`src/server.rs`](../../src/server.rs) — metrics counters
- Demo: [`demos/v0.4.0/09-admin-ui/`](../../demos/v0.4.0/09-admin-ui/) — dashboard polls all of the above
