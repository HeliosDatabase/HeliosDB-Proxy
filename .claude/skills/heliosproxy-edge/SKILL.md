---
name: heliosproxy-edge
description: Geo / edge proxy mode. Register edges with the home proxy, push table-level invalidation broadcasts, watch cache hits across regions. Use when the user says "edge proxy", "geo cache", "/api/edge", "register edge", "invalidate", "last-write-wins TTL".
allowed-tools: Bash(curl *), Bash(jq *), Bash(psql *)
related: [heliosproxy-overview, heliosproxy-config]
---

# Edge / geo proxy mode

A "home" proxy and one or more "edge" proxies, with a table-versioned
LRU+TTL cache at each edge and last-write-wins invalidation
broadcasts. Edges register with home; on writes, home pushes
invalidations to every edge.

Requires `--features edge-proxy` at build time.

## When to use

- Read-heavy workloads with strong locality (catalog / lookups)
- Multi-region apps where round-tripping to the home DB hurts
- Cache-coherence demos (the `02-edge-proxy` demo shows ~40×
  speedup with a single edge)

🟠 Mutating — `register` and `invalidate` modify shared state.
🔵 `/api/edge` (status) is read-only.

## Surfaces

| Endpoint | Method | Purpose |
|---|---|---|
| `GET /api/edge`               | list of registered edges + cache stats |
| `POST /api/edge/register`     | edge announces itself to home |
| `POST /api/edge/invalidate`   | invalidate one or more tables across all edges |

`proxy.toml` chooses the role:

```toml
[edge]
mode = "home"               # this proxy is the home
# OR
mode = "edge"               # this proxy is an edge of <home_url>
edge_id  = "edge-eu-west"
region   = "eu-west"
home_url = "https://heliosproxy-home.example.com"
```

## Recipes

### Recipe 1: Stand up a home + one edge

```bash
# proxy-home.toml
cat > proxy-home.toml <<'EOF'
[server]
listen_address = "0.0.0.0:6432"
admin_address  = "0.0.0.0:9090"

[[nodes]]
address = "pg-primary:5432"
role    = "primary"

[edge]
mode               = "home"
registry_capacity  = 32
edge_prune_after   = "120s"
EOF

# proxy-edge.toml
cat > proxy-edge.toml <<'EOF'
[server]
listen_address = "0.0.0.0:6432"
admin_address  = "0.0.0.0:9091"

[[nodes]]
address = "pg-primary:5432"   # edge falls back to home's primary on miss
role    = "primary"

[edge]
mode      = "edge"
edge_id   = "edge-eu-west"
region    = "eu-west"
home_url  = "http://heliosproxy-home:9090"
cache_size     = 10_000
cache_ttl_secs = 60
EOF
```

Start home, then start edge — the edge auto-registers on boot.

### Recipe 2: Manually register an edge

```bash
curl -s -X POST http://heliosproxy-home:9090/api/edge/register \
  -H 'Content-Type: application/json' \
  -d '{
    "edge_id":  "edge-eu-west",
    "region":   "eu-west",
    "base_url": "http://heliosproxy-edge-eu-west:9091"
  }' | jq .
```

```json
{
  "edge_id":      "edge-eu-west",
  "region":       "eu-west",
  "base_url":     "http://heliosproxy-edge-eu-west:9091",
  "registered_at":"2026-05-01T13:50:00Z"
}
```

Re-registering the same `edge_id` is idempotent — last call wins.

### Recipe 3: List edges + cache stats

```bash
curl -s http://heliosproxy-home:9090/api/edge | jq .
```

```json
{
  "cache":      {"size":0,"hits":0,"misses":0},
  "registered": [
    {"edge_id":"edge-eu-west","region":"eu-west","base_url":"http://heliosproxy-edge-eu-west:9091","registered_at":"2026-05-01T13:50:00Z"}
  ],
  "edge_count": 1
}
```

The `cache` stats are the home's own cache view (typically empty
in home mode); for the edge's cache, query the edge's `/api/edge`.

### Recipe 4: Invalidate tables across all edges

```bash
curl -s -X POST http://heliosproxy-home:9090/api/edge/invalidate \
  -H 'Content-Type: application/json' \
  -d '{"tables":["users","orders"]}' | jq .
```

```json
{
  "invalidated_tables": ["users","orders"],
  "broadcast_sent":     true
}
```

The home pushes the invalidation message to every registered edge.
Edges drop matching cache entries on receipt. Optionally, scope by
`up_to_version`:

```bash
-d '{"tables":["users"],"up_to_version":12345}'
```

— invalidate only entries with version ≤ 12345 (last-write-wins
semantics).

### Recipe 5: Observe a cache speedup

Against the `02-edge-proxy` demo:

```bash
cd demos/v0.4.0/02-edge-proxy && ./demo.sh up
PGPASSWORD=postgres

# Cold hit on edge — pulls from home
time psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT count(*) FROM users"   # ~50 ms

# Warm hit on edge — served from cache
time psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT count(*) FROM users"   # ~1 ms

# Mutate at home — invalidation pushed
psql -h homeproxy -p 6432 -U postgres -d demo \
  -c "INSERT INTO users (name) VALUES ('alice')"

# Next read at edge is a miss again — re-pulls fresh
time psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT count(*) FROM users"   # ~50 ms
```

## Pitfalls

- **`registry_capacity` (home) is bounded.** With more edges than
  capacity, the home rejects new registrations with 503. Tune for
  expected fan-out.
- **`edge_prune_after` evicts edges that haven't heartbeat'd.**
  Edges send a heartbeat with their `/api/edge/register` periodically;
  if an edge dies without unregistering, home prunes it.
- **TTL is per-table, version-scoped.** A cached entry stays valid
  until either the TTL expires OR an invalidation arrives for the
  table-version pair. There's no manual "evict by query" — only by
  table.
- **`/api/edge/invalidate` is fire-and-forget at the home.** If
  some edges are unreachable, home doesn't retry. Consider
  re-broadcasting from a sidecar if reliability matters.
- **The edge cache is in-memory only.** Restart loses the cache;
  the first reads after restart are cold.
- **503 on `POST /api/edge/*`** = `edge-proxy` feature off in the
  build. Rebuild with `--features edge-proxy`.

## See also

- `heliosproxy-config` — `[edge]` block schema
- Demo: [`demos/v0.4.0/02-edge-proxy/`](../../demos/v0.4.0/02-edge-proxy/) — runnable end-to-end
- Code: [`src/edge/cache.rs`](../../src/edge/cache.rs) — cache impl
- Code: [`src/edge/registry.rs`](../../src/edge/registry.rs) — home-side registry + broadcast
- Code: [`src/admin.rs`](../../src/admin.rs) — `/api/edge*` impl
