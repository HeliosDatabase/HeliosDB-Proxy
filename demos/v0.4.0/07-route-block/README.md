# Demo 7 — `RouteResult::Block` plugin ABI

**Module brief:** [§Module 7](../../../docs/website-brief-v0.4.0.md)

## UVP

> A `route` hook plugin can refuse a query with a clean PostgreSQL
> `ErrorResponse` — same wire shape as `PreQueryResult::Block`,
> consistent client experience.

## Use cases

- **Residency enforcement** (Demo 18 builds on this): "user from
  EU cannot read US-region replicas, even via SET replica = ..."
- **Per-environment guards.** "Production cluster refuses queries
  from a dev-environment session token."
- **Capability-restricted clients.** Embedded analytics tool only
  allowed to read from `analytics_*` tables.

## What this demo shows

The `residency-router` plugin (which uses `RouteResult::Block` for
its rejection path — see Demo 18) connecting users from a region
the proxy doesn't have a tagged replica for.

```bash
PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -c "SET helios.region = 'antarctica'; SELECT * FROM users LIMIT 1"

# ERROR:  Query blocked by route plugin: no in-region replica for user
```

The client sees a proper `ERROR` response with severity `ERROR`,
SQLSTATE `42000`, and a human-readable message — same wire shape
as `PreQueryResult::Block`. The session stays alive; the next
query proceeds normally:

```sql
SET helios.region = 'us-east';   -- works, configured below
SELECT * FROM users LIMIT 1;     -- routed to us-east replica
```

## Run it

This demo runs the same docker-compose as Demo 18 (residency-router).
See `18-residency-router/` for the fully-runnable instance.

## Implementation pointer

- ABI: `heliosdb-proxy-plugins/abi/src/lib.rs::RouteResult::Block`
  variant + the wire-format `{ "action": "block", "reason": "..." }`.
- Proxy plumbing: `src/server.rs::route_and_forward` — Block
  short-circuits before backend selection, synthesises
  `ErrorResponse` + `ReadyForQuery`.
- Plugin user: `residency-router/src/lib.rs::route` returns
  `RouteResult::Block { reason }` when the user's region has no
  in-region node and `enforce` is true.

## HeliosDB compatibility

Backend-agnostic.
