---
name: heliosproxy-chaos
description: Inject controlled faults via `POST /api/chaos`. Force a node unhealthy, restore it, or reset all overrides at once. Use when the user says "chaos drill", "force the primary down", "fault injection", "test failover", "restore the node", or wants to validate the failover path.
allowed-tools: Bash(curl *), Read
related: [heliosproxy-overview, heliosproxy-topology, heliosproxy-switchover]
---

# Chaos: controlled fault injection

The chaos engine is built into the admin server (no feature flag).
It overrides the proxy's own view of node health, independent of
the actual backend. Use it to test failover, drain behaviour, and
client-side retry logic without touching the real database.

## When to use

- Validating failover end-to-end before going to prod
- Demonstrating HA in a sales / on-call drill
- Reproducing an incident locally (force the same node unhealthy)
- Stress-testing app-side retry logic

🟠 Mutating — flips routing decisions immediately; production-safe
only if you understand the blast radius.

## Surfaces

| Surface | Effect |
|---|---|
| `POST /api/chaos {action: "force_unhealthy", target_node: "host:port"}` | Mark node unhealthy regardless of real check |
| `POST /api/chaos {action: "restore", target_node: "host:port"}`         | Clear that node's override |
| `POST /api/chaos {action: "reset"}`                                       | Clear ALL overrides |
| `GET /api/chaos`                                                          | List active overrides |

The override layer is independent of real health checks. A chaos-
forced-unhealthy node may still be real-healthy in `/nodes`; the
proxy uses the OR — any unhealthy signal wins.

## Recipes

### Recipe 1: Force the primary unhealthy + watch failover

```bash
# 1. confirm starting state
curl -s http://localhost:9090/topology | jq '{currentPrimary, healthyNodes}'
# {"currentPrimary":"pg-primary:5432","healthyNodes":2}

# 2. inject failure
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"force_unhealthy","target_node":"pg-primary:5432"}'
# {"applied":"force_unhealthy","target_node":"pg-primary:5432"}

# 3. observe — within `[health].check_interval_secs` the proxy
#    detects "unhealthy primary" and promotes a standby (when
#    postgres-topology / heliosdb-topology is configured)
sleep 6
curl -s http://localhost:9090/topology | jq '{currentPrimary, lastFailoverAt}'
# {"currentPrimary":"pg-replica-1:5432","lastFailoverAt":"2026-05-01T13:30:14Z"}
```

After enough confidence, restore:

```bash
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"restore","target_node":"pg-primary:5432"}'
# {"restored":"pg-primary:5432"}
```

The node returns to the "healthy" pool on the next health-check
tick. The promoted standby remains the primary unless you trigger
another failover.

### Recipe 2: List active chaos state

```bash
curl -s http://localhost:9090/api/chaos | jq .
```

```json
[
  {"target_node":"pg-replica-2:5432","kind":"force_unhealthy","since":"2026-05-01T13:28:00Z","note":null}
]
```

Empty array = no overrides active. The dashboard at `:9090/`
visualises this.

### Recipe 3: Reset everything

```bash
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"reset"}'
# {"reset":true,"restored":["pg-primary:5432","pg-replica-2:5432"]}
```

Idempotent. Returns `restored: []` if nothing was overridden.
Always safe at the end of a test or demo.

### Recipe 4: Combined drill — chaos + topology watch + workload

```bash
# Run a small write workload on the proxy
( while sleep 0.2; do
    psql -h localhost -p 6432 -U postgres -d demo -c "INSERT INTO ping (t) VALUES (now())" 2>&1 \
    | tail -n 1
  done ) &
WL=$!

# Knock the primary down
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"force_unhealthy","target_node":"pg-primary:5432"}'

# Observe how many writes the workload missed during the failover
sleep 30
kill $WL

# Cleanup
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"reset"}'
```

Expect: a brief gap of failed writes during the switchover window
(see `heliosproxy-switchover` for what's actually happening). With
the `ha-tr` feature, the in-flight writes can be replayed onto the
new primary — see `heliosproxy-time-travel`.

### Recipe 5: Chaos against a standby (drain pattern)

```bash
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"force_unhealthy","target_node":"pg-replica-2:5432"}'
# … perform maintenance on pg-replica-2 …
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d '{"action":"restore","target_node":"pg-replica-2:5432"}'
```

This is the closest thing v0.4.x has to a "drain a single replica"
operation. Active sessions on the chaos-targeted node are NOT
killed — they finish naturally; new connections route elsewhere.

## Pitfalls

- **`force_unhealthy` does not promote a standby by itself.**
  Promotion requires the topology provider (`postgres-topology` or
  `heliosdb-topology`) to be configured AND `[ha].auto_failover =
  true`. Without those, the primary stays "down" and writes fail.
- **Chaos overrides survive across restarts? No** — they live in
  process memory. A restart clears them automatically. Treat that
  as a feature: stuck overrides → restart fixes it.
- **`target_node` must match the configured address exactly.**
  `pg-primary:5432` ≠ `127.0.0.1:5432`. Look up the canonical form
  via `/topology` first.
- **`/api/chaos` is not auth-protected.** If admin port is exposed
  to the world, an attacker can DoS your cluster. Always behind a
  firewall / auth proxy in production.
- **Don't run chaos and time-travel-replay back-to-back without
  thinking.** Replay re-applies writes from the journal; if you
  fail-over mid-replay, you can apply the same writes twice. Reset
  chaos before invoking replay.
- **`reset` doesn't undo a failover.** It clears the overrides; the
  promoted standby remains the primary. To "fail back" you must
  drive a second failover the other direction (or restart the
  proxy and let the topology provider re-elect).

## See also

- `heliosproxy-topology` — observe state during/after chaos
- `heliosproxy-switchover` — what failover actually does
- `heliosproxy-time-travel` — replay writes that were lost during failover
- Demo: [`demos/v0.4.0/10-admin-rest/`](../../demos/v0.4.0/10-admin-rest/) — chaos in the curl tour
- Code: [`src/admin.rs`](../../src/admin.rs) — `/api/chaos` impl
- Code: [`src/server.rs`](../../src/server.rs) — chaos override application
