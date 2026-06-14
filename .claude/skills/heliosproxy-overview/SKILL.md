---
name: heliosproxy-overview
description: Top-level navigation for HeliosProxy. Auto-loads when the user mentions "heliosproxy", "helios proxy", or pastes its admin REST output / config / log lines. Routes to one of 21 domain skills (install, start, config, shutdown, connect, topology, health, chaos, switchover, time-travel, shadow-execute, anomaly, plugin-{pack,load,kv,catalog}, edge, demo-{up,down}, release, iac). Use this skill to find the right skill before going deep.
allowed-tools: Bash(curl *), Bash(psql *), Read, Grep, Glob
related: [heliosproxy-install, heliosproxy-start, heliosproxy-config]
---

# HeliosProxy — Operational Overview

## When to use

Any task involving HeliosProxy. This skill is the index — it answers
"which skill should I read?" and gives a one-shot orientation.
After picking a domain skill, follow that skill's recipes.

🔵 Read-only (just navigation)

## What is HeliosProxy

PostgreSQL-wire-protocol-compatible connection router and intelligent
query proxy. Sits between an application and a PG-compatible cluster
(PostgreSQL ≥12, HeliosDB, CockroachDB, YugabyteDB, AlloyDB,
TimescaleDB, Citus). Crate name: `heliosdb-proxy`. Binary name:
`heliosdb-proxy`. Repo: `HeliosDatabase/HDB-HeliosDB-Proxy`. Default
listen ports: PG `6432`, admin `9090`.

24+ feature-gated modules including pool modes (Session/Transaction/
Statement), HA + Transaction Replay, multi-tier query cache,
multi-tenancy, WASM plugins, GraphQL gateway, schema-aware routing,
DistribCache, anomaly detection, edge/geo proxy.

## Pick a skill

| If the task is about… | Read |
|---|---|
| Installing the binary, building from source, picking feature flags | `heliosproxy-install` |
| Daemonizing, CLI flags, env vars, systemd | `heliosproxy-start` |
| `proxy.toml` syntax, nodes block, feature blocks, TLS | `heliosproxy-config` |
| SIGTERM, drain semantics, observing shutdown | `heliosproxy-shutdown` |
| `psql` round-trip, route hints, sanity-check the proxy | `heliosproxy-connect` |
| `/topology`, `/nodes`, current primary, enable/disable nodes | `heliosproxy-topology` |
| `/health`, `/metrics`, `/sessions`, `/pools`, `/version`, alerting | `heliosproxy-health` |
| Chaos: force_unhealthy, restore, reset, watch failover | `heliosproxy-chaos` |
| Switchover/failover semantics, promotion, primary-tracker | `heliosproxy-switchover` |
| Replay journal window onto staging (`POST /api/replay`) | `heliosproxy-time-travel` |
| Dual-execute + diff (`POST /api/shadow`), PG-upgrade validation | `heliosproxy-shadow-execute` |
| `GET /anomalies`: SQL-injection / auth-burst / rate-spike / novel-query | `heliosproxy-anomaly` |
| `helios-plugin pack/inspect/verify`, OCI artefacts, Ed25519 sigs | `heliosproxy-plugin-pack` |
| Loading a `.wasm` plugin, hot-reload, trust-root, `/plugins` | `heliosproxy-plugin-load` |
| `/admin/kv/<plugin>/<key>`: per-plugin runtime config | `heliosproxy-plugin-kv` |
| First-party plugins (cost-governor / ai-classifier / token-budget / llm-guardrail / pgvector-router / column-mask / audit-chain / residency-router) | `heliosproxy-plugin-catalog` |
| Edge mode, `/api/edge/{register,invalidate}`, last-write-wins TTL | `heliosproxy-edge` |
| `demos/v0.4.0/<n>/demo.sh up`, picking a demo to run | `heliosproxy-demo-up` |
| `demo.sh down`, port-conflict, volume cleanup | `heliosproxy-demo-down` |
| Cutting a release: bump → tag → push → workflow | `heliosproxy-release` |
| K8s operator + CRD, Terraform provider, Pulumi provider | `heliosproxy-iac` |

## Sanity-check the install (one-liners)

```bash
heliosdb-proxy --version
# heliosdb-proxy 0.4.1
```

```bash
curl -s http://localhost:9090/health
# {"status":"ok"}

curl -s http://localhost:9090/topology | jq .
# {"currentPrimary":"pg-primary:5432","healthyNodes":2,"unhealthyNodes":0,...}
```

If `--version` fails: see `heliosproxy-install`.
If `/health` returns nothing: see `heliosproxy-start`.
If `/topology` shows `currentPrimary: null`: see `heliosproxy-topology`
(it's normal during failover; after — read `heliosproxy-switchover`).

## Map of operational verbs

See [`_index/verb-map.md`](../_index/verb-map.md) for a full
verb → skill table. See [`_index/feature-matrix.md`](../_index/feature-matrix.md)
for which feature flags unlock which skills.

## When in doubt

The 22 demos under `demos/v0.4.0/` are runnable end-to-end on any
laptop with Docker + Compose. They're the fastest way to see a feature
work before reading the corresponding skill.

```bash
cd demos/v0.4.0/10-admin-rest && ./demo.sh        # curl tour of every admin endpoint
cd demos/v0.4.0/01-anomaly-detection && ./demo.sh # SQL injection + auth burst detection
cd demos/v0.4.0/02-edge-proxy && ./demo.sh        # home + edge proxy with cache coherence
```

See `heliosproxy-demo-up` for the full demo index.

## See also

- `heliosproxy-install` — start here on a fresh machine
- `heliosproxy-start` — start here on a running build
- [`README.md`](../../README.md) — repo-level overview
- [`CHANGELOG.md`](../../CHANGELOG.md) — release notes
- [`docs/`](../../docs/) — design docs and RFCs
