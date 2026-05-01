# HeliosProxy feature matrix

Which feature flags must be enabled (in `Cargo.toml`) for each skill's
recipes to work. Default features are `pool-modes` only — every
heavier skill below requires explicit feature enablement at build
time.

## Default build (`cargo install heliosdb-proxy`)

Default enables `pool-modes` only. The following skills work out of the box:

| Skill | Why |
|---|---|
| `heliosproxy-overview` | Pure documentation |
| `heliosproxy-install` | Pure documentation |
| `heliosproxy-start` | Lifecycle; needs no feature |
| `heliosproxy-config` | Documentation; covers feature-gated blocks too |
| `heliosproxy-shutdown` | Lifecycle |
| `heliosproxy-connect` | PG wire is always-on |
| `heliosproxy-topology` | `/topology`, `/nodes` always-on |
| `heliosproxy-health` | `/health`, `/metrics`, `/sessions`, `/pools` always-on |
| `heliosproxy-chaos` | Chaos engine always-on |
| `heliosproxy-demo-up` / `heliosproxy-demo-down` | Demo scripts |
| `heliosproxy-release` | Build/CI workflow |

## Per-feature requirements

| Feature flag | Unlocks skills |
|---|---|
| `ha-tr` | `heliosproxy-time-travel`, `heliosproxy-shadow-execute`, parts of `heliosproxy-switchover` |
| `wasm-plugins` | `heliosproxy-plugin-pack`, `heliosproxy-plugin-load`, `heliosproxy-plugin-kv`, `heliosproxy-plugin-catalog` |
| `anomaly-detection` | `heliosproxy-anomaly` |
| `edge-proxy` | `heliosproxy-edge` |
| `pool-modes` (default) | `heliosproxy-health` `/pools` data |
| `postgres-topology` *or* `heliosdb-topology` | `heliosproxy-switchover` automatic detection |
| `observability` | `heliosproxy-health` Prometheus output |

## All features at once

```bash
cargo install heliosdb-proxy --features all-features
```

`all-features` enables: `pool-modes`, `ha-tr`, `query-cache`,
`routing-hints`, `lag-routing`, `rate-limiting`, `circuit-breaker`,
`query-analytics`, `multi-tenancy`, `auth-proxy`, `query-rewriting`,
`wasm-plugins`, `graphql-gateway`, `schema-routing`, `distribcache`,
`anomaly-detection`, `edge-proxy`. (Topology providers are still
exclusive: pick one of `postgres-topology` / `heliosdb-topology`.)

## How to check at runtime

```bash
curl -s http://localhost:9090/version | jq .
# {"version": "0.4.1", "build_time": "...", "features": [...]}
```

The `features` list (where exposed) reflects what was compiled in.
A 503 from a feature-gated endpoint (`/api/replay`, `/anomalies`,
`/api/edge*`, `/plugins`) means the feature is **off** at build time
— rebuild with the right flag.

## IaC skills

`heliosproxy-iac` is documentation-only and references three sibling
repositories (operator, terraform-provider, pulumi). It has no
proxy-side feature requirement, but the deployed proxy must be built
with whatever features the operator's `HeliosProxy` CRD spec assumes.
