# Demo 2 — Edge / Geo Proxy (T3.2)

**Feature flag:** `edge-proxy`
**Module brief:** [website-brief-v0.4.0.md §Module 2](../../../docs/website-brief-v0.4.0.md)

## UVP

> PostgreSQL at the edge — without rewriting your app. Local cache
> on every region, last-write-wins TTL coherence, no consensus.

## Use cases

- **Read-heavy SaaS with global users.** Product catalog, pricing,
  feature flags, settings — anything where bounded staleness is an
  acceptable trade for sub-region latency.
- **Analytics dashboards.** Per-region cache means a Grafana panel
  loads in ~5 ms locally instead of ~80 ms across the Atlantic.
- **Multi-tenant SaaS where regions matter.** Pair with the
  `residency-router` plugin (Demo 18) for "user data only ever
  served from in-region replicas".

## What this demo shows

Two HeliosProxy instances on a shared Docker network:

- **`proxy-home`** — authoritative, fronts `pg-primary`. Receives
  writes; broadcasts invalidations.
- **`proxy-edge`** — registered with home. Caches reads locally
  with a 60-second TTL.

Walkthrough:

1. Edge connects to `pg-primary` via home (cache cold). Read
   takes the round-trip latency.
2. Same query repeated at edge — served from local cache. Latency
   drops by ~20×.
3. Write at home: `UPDATE users SET name = 'CHANGED' WHERE id = 1`.
4. Home broadcasts `Invalidate { tables: ["users"], up_to_version: N }`.
5. Edge drops the cached entry.
6. Next read at edge: cache miss → fetch from home → fresh data.

## Run it

```bash
cd demos/v0.4.0/02-edge-proxy
./demo.sh
```

Expected output:

```text
=== Edge Proxy Demo ===
[1/5] Starting home + edge + Postgres...
[2/5] Registering edge with home...
   {"edge_id":"edge-eu-west","registered_at":"2026-04-26T...Z"}
[3/5] First read at edge (cache cold):
   query took 12 ms
[4/5] Second read at edge (cache hit):
   query took  0.3 ms     ← 40× faster
[5/5] Write at home + invalidate broadcast:
   {"version":42,"tables":["users"],"dropped_local":1,"edges_notified":1}
[6/5] Read at edge again (cache miss → fresh data):
   value = "CHANGED"
   query took 11 ms
```

## Try it yourself

After `./demo.sh up`:

```bash
# Inspect cache stats live
watch -n 1 'curl -s http://localhost:9091/api/edge | jq .cache'

# Manual invalidation (force drop anything tagged "users")
curl -s -X POST http://localhost:9090/api/edge/invalidate \
  -H 'Content-Type: application/json' \
  -d '{"tables":["users"]}' | jq .
```

## HeliosDB compatibility

Edge mode is proxy-side; the backend is unchanged. Works with
`postgres:17-alpine` (this demo) or HeliosDB-Lite. The cache key is
`(query_fingerprint, params_hash)` — no backend coordination needed.
