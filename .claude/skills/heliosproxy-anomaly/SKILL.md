---
name: heliosproxy-anomaly
description: Read the in-process anomaly detector — SQL injection patterns, auth-burst credential stuffing, rate-spike z-score, novel-query shapes. Use when the user says "/anomalies", "anomaly", "SQL injection", "auth burst", "credential stuffing", "rate spike", or wires up an alerting hook.
allowed-tools: Bash(curl *), Bash(jq *), Bash(psql *)
related: [heliosproxy-overview, heliosproxy-health, heliosproxy-plugin-catalog]
---

# Anomaly detection

The `anomaly-detection` feature runs four families of detectors
in-process on every request the proxy sees. Detections land in a
ring buffer (default 5000 events) readable via `GET /anomalies`.
No external store, no extra deps — sliding-window math in Rust.

Requires `--features anomaly-detection` at build time.

## When to use

- Wiring up Slack / PagerDuty alerts on detection events
- Investigating "why did the proxy log a sql_injection event"
- Demoing SOC-style detection without buying a SIEM
- Validating that a known-bad payload is caught (CI test)

🔵 Read-only

## Detection families

| Kind | What it catches | Tunable in `proxy.toml` |
|---|---|---|
| `sql_injection`     | `OR 1=1`, UNION SELECT, comment escapes (`-- …`), stacked queries (`;DROP …`), time-based blind (`pg_sleep`), `information_schema` probes | `[anomaly].sql_injection = true` |
| `auth_burst`        | N failed auths from one user/IP within T seconds — credential stuffing | `auth_burst_threshold`, `auth_burst_window` |
| `rate_spike`        | per-tenant query rate exceeds EWMA + z-score threshold | `rate_spike_z_score` |
| `novel_query`       | query fingerprint never seen before (low-frequency, high-novelty) | `novel_query_threshold` |

## Surfaces

| Endpoint | Returns |
|---|---|
| `GET /anomalies` | most recent N events (default 100, max 1024) |
| `GET /anomalies?limit=50` | limited |

## Recipes

### Recipe 1: Trigger a known-bad payload + verify detection

```bash
# Trigger SQL injection
psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT * FROM users WHERE id = 1 OR 1=1 --"

# Read the detection
curl -s "http://localhost:9090/anomalies?limit=5" | jq '.events[] | select(.kind=="sql_injection")'
```

```json
{
  "kind":             "sql_injection",
  "severity":         "high",
  "timestamp":        "2026-05-01T13:42:11Z",
  "patterns_matched": ["classic_or", "comment_escape"],
  "sql_excerpt":      "SELECT * FROM users WHERE id = 1 OR 1=1 --"
}
```

The `patterns_matched` array names the specific heuristics that
fired. Six patterns total: `classic_or`, `union_select`,
`comment_escape`, `stacked_queries`, `time_based_blind`,
`information_schema_probe`.

### Recipe 2: Auth burst (credential stuffing)

```bash
# 12 failed logins in 30 s with default threshold of 10
for i in $(seq 1 12); do
  PGPASSWORD=wrong psql -h localhost -p 6432 -U attacker -d demo -c 'SELECT 1' 2>/dev/null
done

curl -s "http://localhost:9090/anomalies?limit=5" | jq '.events[] | select(.kind=="auth_burst")'
```

```json
{
  "kind":      "auth_burst",
  "severity":  "high",
  "timestamp": "2026-05-01T13:43:55Z",
  "user":      "attacker",
  "client_ip": "127.0.0.1",
  "failures":  12,
  "window_secs": 30
}
```

Tune in `[anomaly]`:

```toml
[anomaly]
auth_burst_threshold = 5
auth_burst_window    = "10s"
```

### Recipe 3: Rate spike (noisy-neighbour tenant)

```bash
# Run a high-rate workload from one tenant
( while sleep 0.001; do
    psql -h localhost -p 6432 -U postgres -d demo \
      -c "SET application_name = 'tenant-a'; SELECT 1" 2>/dev/null
  done ) &

curl -s "http://localhost:9090/anomalies?limit=5" \
  | jq '.events[] | select(.kind=="rate_spike")'
```

Spike detection is z-score based: rate per tenant must exceed the
EWMA by `rate_spike_z_score` standard deviations. Default 3.0
(99.7th percentile); lower for more sensitivity.

### Recipe 4: Continuous polling for alerting

```bash
# Poll every 10 s, forward new high-severity events to a webhook
LAST_TS=$(date -u +%FT%TZ)
while true; do
  curl -s "http://localhost:9090/anomalies?limit=200" \
    | jq -c --arg since "$LAST_TS" \
        '.events[] | select(.timestamp > $since and .severity == "high")' \
    | while read -r ev; do
        echo "ALERT $ev"
        # POST to Slack / PagerDuty here
      done
  LAST_TS=$(date -u +%FT%TZ)
  sleep 10
done
```

The `/anomalies` endpoint is read-only and cheap (in-memory ring).
Polling at 10 s is fine; you can go to 1 s if you want.

### Recipe 5: Read the buffer header

```bash
curl -s "http://localhost:9090/anomalies?limit=1" | jq '{count, limit, buffer_total}'
# {"count":1,"limit":1,"buffer_total":5000}
```

`buffer_total` is the in-process ring capacity. When the ring fills
the oldest event drops; alert pollers must keep up or they'll miss
events. Increase `[anomaly].buffer_size` if you can't poll often
enough.

### Recipe 6: Filter by kind, severity, time

```bash
# All sql_injection in last hour
curl -s "http://localhost:9090/anomalies?limit=1024" | jq --arg since \
  "$(date -u -d '1 hour ago' +%FT%TZ)" \
  '.events[] | select(.kind=="sql_injection" and .timestamp >= $since)'
```

## Pitfalls

- **503 from `/anomalies` = feature off.** Rebuild with
  `--features anomaly-detection`.
- **Zero events with traffic flowing** — the detector is on but
  not actually running on the request path. In v0.4.x the detector
  hooks into PG-message reception for SQL traffic and into the
  auth-handler for auth events. If you bypass via `/api/sql`, only
  the SQL-text detectors fire (no auth-burst).
- **`patterns_matched: []` is impossible** for `sql_injection` —
  if you see it, file a bug. Other kinds don't have that field.
- **Heuristic detectors have false positives.** A query like
  `SELECT 1 WHERE 1=1` triggers `classic_or` even though it's
  legitimate. Don't auto-block based on detections; alert and
  triage.
- **The buffer is in-process.** A proxy restart wipes it. For
  durable history, ship events to an external store (Loki, BigQuery,
  etc.) by polling.
- **`rate_spike` needs at least N seconds of warmup** to
  build its EWMA baseline. Right after start, no rate_spike fires.
- **`novel_query` is noisy in dev.** Production traffic patterns
  stabilise quickly; dev traffic is always novel. Default
  `novel_query_threshold = 0.05` is for prod; raise it for dev.

## See also

- `heliosproxy-config` — `[anomaly]` block tunables
- `heliosproxy-plugin-catalog` — for prevention-grade plugins
  (`llm-guardrail`, `cost-governor`)
- Demo: [`demos/v0.4.0/01-anomaly-detection/`](../../demos/v0.4.0/01-anomaly-detection/) — runnable end-to-end
- Code: [`src/anomaly/`](../../src/anomaly/) — detector implementations
- Code: [`src/anomaly/sql_injection.rs`](../../src/anomaly/sql_injection.rs) — pattern definitions
- Code: [`src/admin.rs`](../../src/admin.rs) — `/anomalies` impl
