# HeliosProxy verb map

Every administrative operation, sorted by what an operator actually
types or thinks. Each row points to the skill that owns the recipe.

If your verb isn't here, start with [`heliosproxy-overview`](../heliosproxy-overview/SKILL.md).

## Lifecycle

| Verb / phrase | Skill |
|---|---|
| install, build, `cargo install`, system requirements | `heliosproxy-install` |
| start, run, daemon, `--config`, systemd, foreground | `heliosproxy-start` |
| configure, `proxy.toml`, feature flags, TLS, nodes block | `heliosproxy-config` |
| stop, shutdown, drain, SIGTERM | `heliosproxy-shutdown` |
| reload config | `heliosproxy-shutdown` (drain) + `heliosproxy-start` (re-launch) |

## Connectivity & inspection

| Verb / phrase | Skill |
|---|---|
| connect, psql, asyncpg, jdbc, `SELECT 1` round-trip | `heliosproxy-connect` |
| route hint, `/*+ route=primary */`, force primary | `heliosproxy-connect` |
| topology, current primary, `/topology`, `/nodes`, `/config` | `heliosproxy-topology` |
| enable / disable a node | `heliosproxy-topology` |
| health, `/health`, `/health/ready`, `/health/live`, `/version` | `heliosproxy-health` |
| metrics, Prometheus, `/metrics`, `/sessions`, `/pools` | `heliosproxy-health` |

## Failure handling

| Verb / phrase | Skill |
|---|---|
| chaos, force unhealthy, fault injection, `/api/chaos` | `heliosproxy-chaos` |
| restore, reset chaos | `heliosproxy-chaos` |
| switchover, failover, primary promotion | `heliosproxy-switchover` |
| read failover log, observe failover | `heliosproxy-switchover` |

## Time-travel & shadow

| Verb / phrase | Skill |
|---|---|
| replay journal, `/api/replay`, time-travel | `heliosproxy-time-travel` |
| replay window onto staging | `heliosproxy-time-travel` |
| shadow, dual-execute, `/api/shadow`, diff results | `heliosproxy-shadow-execute` |
| validate PG upgrade with shadow | `heliosproxy-shadow-execute` |

## Anomaly detection

| Verb / phrase | Skill |
|---|---|
| anomalies, `/anomalies`, SQL injection detection | `heliosproxy-anomaly` |
| auth burst, rate spike, novel query | `heliosproxy-anomaly` |

## Plugins (WASM)

| Verb / phrase | Skill |
|---|---|
| pack, sign, inspect, verify (`helios-plugin` CLI) | `heliosproxy-plugin-pack` |
| load plugin, drop into `plugins/`, hot-reload | `heliosproxy-plugin-load` |
| trust root, Ed25519 keys, signature verify | `heliosproxy-plugin-pack` |
| KV, `/admin/kv/<plugin>/<key>`, configure plugin runtime | `heliosproxy-plugin-kv` |
| which plugin does X, plugin catalog | `heliosproxy-plugin-catalog` |
| cost-governor, ai-classifier, token-budget, llm-guardrail, pgvector-router, column-mask, audit-chain, residency-router | `heliosproxy-plugin-catalog` |

## Edge proxy

| Verb / phrase | Skill |
|---|---|
| edge, geo cache, register edge, invalidate, `/api/edge` | `heliosproxy-edge` |

## Demos & dev loop

| Verb / phrase | Skill |
|---|---|
| run a demo, bring up demo, `demo.sh up` | `heliosproxy-demo-up` |
| tear down demo, `demo.sh down`, port conflict, volume cleanup | `heliosproxy-demo-down` |

## Release & IaC

| Verb / phrase | Skill |
|---|---|
| release, cut version, `cargo publish`, tag-driven | `heliosproxy-release` |
| Kubernetes, operator, CRD, `HeliosProxy` resource | `heliosproxy-iac` |
| Terraform, `terraform-provider-HDB-HeliosDB-Proxy` | `heliosproxy-iac` |
| Pulumi | `heliosproxy-iac` |

## Direct REST endpoint → skill

| Endpoint | Skill |
|---|---|
| `GET /health`, `/health/ready`, `/health/live` | `heliosproxy-health` |
| `GET /version`, `/sessions`, `/pools` | `heliosproxy-health` |
| `GET /metrics`, `/metrics/prometheus` | `heliosproxy-health` |
| `GET /topology`, `/nodes`, `/nodes/{addr}`, `/config` | `heliosproxy-topology` |
| `POST /nodes/{addr}/{enable,disable}` | `heliosproxy-topology` |
| `GET /api/chaos`, `POST /api/chaos` | `heliosproxy-chaos` |
| `POST /api/replay` | `heliosproxy-time-travel` |
| `POST /api/shadow` | `heliosproxy-shadow-execute` |
| `GET /anomalies` | `heliosproxy-anomaly` |
| `GET /plugins` | `heliosproxy-plugin-load` |
| `PUT/GET/DELETE /admin/kv/<plugin>/<key>` | `heliosproxy-plugin-kv` |
| `GET /api/edge`, `POST /api/edge/register`, `POST /api/edge/invalidate` | `heliosproxy-edge` |
| `POST /api/sql` | `heliosproxy-connect` |
