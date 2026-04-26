# Demo 18 — `residency-router` plugin

**Module brief:** [§Module 18](../../../docs/website-brief-v0.4.0.md)

## UVP

> Per-user data-residency routing. EU users only ever see EU
> replicas; US users only ever see US replicas. Cross-region
> reads return a clean PG `ErrorResponse` (not a confusing
> "node not found").

## Use cases

- **GDPR / India DPDP / China PIPL.** Hard regulatory rules around
  data movement; this plugin enforces them at the proxy.
- **Schrems II** ("EU data must not transit US-controlled
  systems"). Block the routing path entirely; can't accidentally
  query a US replica.
- **Tenant locality.** SaaS customers pay extra for "data stays
  in our region"; the plugin makes that operationally enforceable.

## What this demo shows

Two PG 17 backends representing regional replicas:

- `pg-eu-west` (tagged `region: eu-west` in proxy.toml)
- `pg-us-east` (tagged `region: us-east`)

The plugin reads `hook_context.attributes["helios.region"]` (set
by the user's auth claim or session var). Three outcomes:

```bash
# 1. EU user — routes to EU replica
PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -c "SET helios.region = 'eu-west'; SELECT current_setting('cluster_node')"
#  → 'pg-eu-west'

# 2. US user — routes to US replica
PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -c "SET helios.region = 'us-east'; SELECT current_setting('cluster_node')"
#  → 'pg-us-east'

# 3. Region with no in-region replica + enforce=true
PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -c "SET helios.region = 'antarctica'; SELECT 1"
#  ERROR: Query blocked by route plugin: no in-region replica for user
```

## Run it

```bash
cd demos/v0.4.0/18-residency-router
./demo.sh
```

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/residency-router/src/lib.rs`. Pure
function `decide(user_region, ResidencyConfig)` returns
`ResidencyDecision::Node(name)`, `::Block(reason)`, or
`::NoRequirement`. Block uses the new `RouteResult::Block` ABI
variant from Demo 7.

## HeliosDB compatibility

Backend-agnostic.
