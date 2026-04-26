# Demo 11 — `cost-governor` plugin

**Module brief:** [§Module 11](../../../docs/website-brief-v0.4.0.md)

## UVP

> Per-tenant query cost budgets with auto-block at the proxy.
> Stops a runaway tenant from eating the shared database without
> killing legit traffic.

## Use cases

- **SaaS noisy-neighbour control.** Free-tier tenant runs a
  Cartesian join — gets blocked at minute-budget without taking
  down paid tenants.
- **Fair-share for analytics workloads.** Each tenant gets a
  daily compute budget; runaway dashboards stop when they exhaust it.

## What this demo shows

1. Seed `acme` with a tight minute-budget (1 cost-unit).
2. Run 5 quick queries → no problem.
3. Run a `SELECT * FROM events` (~6.4 MB response, ≈ 6.4 cost-units).
4. The 6th query → `ERROR: Query blocked by plugin: tenant exceeded
   minute budget (6.40/1.00) (retry in 60s)`.
5. Wait 60s → next query succeeds.

## Run it

```bash
cd demos/v0.4.0/11-cost-governor
./demo.sh
```

Sample run:

```text
=== cost-governor demo ===
[1/5] Starting proxy + Postgres + cost-governor.wasm
[2/5] Seeding acme budget: minute=1.0, hour=10.0, day=100.0
[3/5] Running 5 small queries — all succeed
[4/5] Running 1 large query (SELECT * FROM events) — exhausts budget
   ✓ usage now: minute=6.4, hour=6.4, day=6.4
[5/5] Running 6th query — blocked
   ERROR:  Query blocked by plugin: tenant exceeded minute budget
           (6.40/1.00) (retry in 60s)
```

## Implementation pointer

Plugin source: `HDB-HeliosDB-Proxy-Plugins/cost-governor/src/lib.rs`.
Hook entry points: `pre_query` (decides) + `post_query` (records).
KV layout documented at top of the file.

## HeliosDB compatibility

Backend-agnostic.
