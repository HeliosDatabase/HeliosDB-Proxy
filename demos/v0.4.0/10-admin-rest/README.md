# Demo 10 — Admin REST API expansion

**Module brief:** [§Module 10](../../../docs/website-brief-v0.4.0.md)

## UVP

> Eight new endpoints surface every v0.4.0 capability over JSON.
> Same API consumed by the operator's status loop AND the admin
> Web UI — one source of truth for both machines and humans.

## Use cases

- **Kubernetes operator status sync.** `/topology` populates
  `HeliosProxyStatus.currentPrimary` (Demo 20).
- **Custom dashboards.** Pipe `/anomalies` into your existing
  observability stack.
- **CI smoke tests.** `/api/shadow` after every schema migration
  to validate against staging.

## What this demo shows

A `curl` tour of all 8 new v0.4.0 endpoints. Each command returns
real JSON; the script pretty-prints with `jq`.

```bash
cd demos/v0.4.0/10-admin-rest
./demo.sh
```

Output (abbreviated):

```text
=== Admin REST tour ===

GET /topology
   { "currentPrimary": "pg-primary:5432",
     "healthyNodes": 1, "unhealthyNodes": 0, "totalNodes": 1,
     "lastFailoverAt": null }

GET /plugins
   []   (no plugins loaded in this demo)

GET /anomalies?limit=5
   { "count": 0, "limit": 5, "buffer_total": 0, "events": [] }

GET /api/edge
   { "cache":      { "hits": 0, "misses": 0, "current_entries": 0, ... },
     "registered": [], "edge_count": 0 }

POST /api/edge/register
     {"edge_id":"e1","region":"us-east","base_url":"http://e1"}
   { "edge_id": "e1", "region": "us-east", "registered_at": "..." }

POST /api/edge/invalidate
     {"tables":["users"]}
   { "version": 2, "tables": ["users"], "dropped_local": 0,
     "edges_notified": 0, "edges_pruned": 1 }
   (the e1 edge from above had no live receiver in this script,
    so it was pruned)

GET /api/chaos    → {}   (no overrides yet)

POST /api/chaos
     {"action":"force_unhealthy","target_node":"pg-primary:5432"}
   { "applied": "force_unhealthy", "target_node": "pg-primary:5432" }

POST /api/chaos
     {"action":"reset"}
   { "reset": true, "restored": ["pg-primary:5432"] }

POST /api/replay
     {"from":"2026-04-26T10:00:00Z","to":"2026-04-26T11:00:00Z",
      "target_host":"pg-primary","target_port":5432}
   { "statements_replayed": 0, "failures": 0, "elapsed_ms": 12,
     "from": "...", "to": "...", "first_error": null }
   (no journal entries in this demo's window — replay engine
    connects to the target and confirms there's nothing to apply)

POST /api/shadow
     {"sql":"SELECT 1","source_host":"pg-primary","source_port":5432,
      "shadow_host":"pg-primary","shadow_port":5432}
   { "is_clean": true, "row_count_match": true, "row_hash_match": true,
     "primary_elapsed_us": 1342, "shadow_elapsed_us": 1287,
     "both_succeeded": true, "primary_error": null, "shadow_error": null }
```

## Implementation pointer

`src/admin.rs::route_request` — single `match` over `(method,
path)`. Per-handler tests in `src/admin.rs::tests` cover happy
path + 503/400/404/500 failure modes for every endpoint.

## HeliosDB compatibility

Backend-agnostic.
