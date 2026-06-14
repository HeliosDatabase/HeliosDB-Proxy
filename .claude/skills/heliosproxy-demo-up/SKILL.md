---
name: heliosproxy-demo-up
description: Bring up any of the 22 v0.4.0 demos under `demos/v0.4.0/<n>/`. Each ships a `demo.sh up\|run\|down` and a self-contained `docker-compose.yml`. Use when the user says "run a demo", "demo.sh", "show me X working", "anomaly detection demo", or asks for a runnable example.
allowed-tools: Bash(./demo.sh *), Bash(docker compose *), Bash(cd *), Bash(curl *), Bash(psql *), Read
related: [heliosproxy-overview, heliosproxy-demo-down]
---

# Bring up a demo

Every v0.4.0 feature has a runnable demo at
`demos/v0.4.0/<NN-name>/`. Each demo:

- has a **`README.md`** with the UVP, use cases, walkthrough,
- ships a **`docker-compose.yml`** (proxy + a real PostgreSQL 17 backend),
- and a **`demo.sh`** with `up` / `run` / `down` subcommands.

Default backend is `postgres:17-alpine` so it works on any laptop
with Docker. Wire-protocol-identical with HeliosDB-Lite — swap the
`image:` line if you want HeliosDB; demos behave identically.

🟠 Mutating — spins up containers, opens ports.

## Prerequisites

- Docker + Docker Compose v2
- `psql` (PostgreSQL 14+ client)
- `curl` and `jq`
- For plugin demos: a Rust toolchain with `wasm32-unknown-unknown`
  target, **OR** pre-built `.wasm` artefacts dropped into the
  per-demo `plugins/` directory.

## Demo index

| # | Demo | Module | What it proves |
|---|---|---|---|
| [1](../../demos/v0.4.0/01-anomaly-detection/) | Anomaly Detection | `anomaly-detection` | SQLi + auth burst + novel query — three families fire concurrently |
| [2](../../demos/v0.4.0/02-edge-proxy/) | Edge / Geo Proxy | `edge-proxy` | Two proxies (home + edge), invalidation broadcast on write, ~40× cache speedup |
| [3](../../demos/v0.4.0/03-plugin-kv/) | Plugin host KV bridge | `wasm-plugins` | Per-plugin namespaced state survives across hook invocations |
| [4](../../demos/v0.4.0/04-plugin-crypto/) | Plugin host crypto | `wasm-plugins` | RFC 6234 SHA-256 vector check + audit-chain producing real digests |
| [5](../../demos/v0.4.0/05-plugin-signatures/) | Ed25519 signatures | `wasm-plugins` | openssl-signed `.wasm` loads; tampered or unsigned refuses |
| [6](../../demos/v0.4.0/06-plugin-oci/) | Plugin OCI loader | `wasm-plugins` | `helios-plugin pack` → drop tarball → proxy validates SHA-256 |
| [7](../../demos/v0.4.0/07-route-block/) | `RouteResult::Block` | `wasm-plugins` | Route hook produces clean PG `ErrorResponse` on rejection |
| [8](../../demos/v0.4.0/08-trust-root/) | `trust_root` knob | `wasm-plugins` | Same proxy binary, different TOML → permissive vs enforced |
| [9](../../demos/v0.4.0/09-admin-ui/) | Admin Web UI | (always-on) | 10-panel dashboard at `:9090/`, auto-refresh |
| [10](../../demos/v0.4.0/10-admin-rest/) | Admin REST tour | (always-on) | `curl` tour of all 8 new endpoints |
| [11](../../demos/v0.4.0/11-cost-governor/) | cost-governor | T2.3 | Per-tenant budget exhaustion → block; recovery after window resets |
| [12](../../demos/v0.4.0/12-ai-classifier/) | ai-classifier | T2.2 | LLM detection from `application_name` keywords + generated-by markers |
| [13](../../demos/v0.4.0/13-token-budget/) | token-budget | T2.2 | Per-`(agent, model)` cost gate for AI traffic |
| [14](../../demos/v0.4.0/14-llm-guardrail/) | llm-guardrail | T2.2 | DROP, missing WHERE, missing tenant_id all refused for AI traffic |
| [15](../../demos/v0.4.0/15-pgvector-router/) | pgvector-router | T2.2 | Vector top-K → pg-vector replica; non-vector → pg-primary |
| [16](../../demos/v0.4.0/16-column-mask/) | column-mask | T2.4 | Same query, different roles, masked vs raw PII |
| [17](../../demos/v0.4.0/17-audit-chain/) | audit-chain | T2.4 | Hash-chained tamper-evident log; verify_chain catches mutation |
| [18](../../demos/v0.4.0/18-residency-router/) | residency-router | T2.4 | EU users → EU replica; US users → US replica; unknown region → block |
| [19](../../demos/v0.4.0/19-helios-plugin-cli/) | `helios-plugin` CLI | (build tool) | Pack/inspect/verify with openssl-generated key |
| [20](../../demos/v0.4.0/20-k8s-operator/) | Kubernetes operator | T1.1 | One CR brings up ConfigMap + Deployment + Service; status flips Pending → Ready |
| [21](../../demos/v0.4.0/21-terraform/) | Terraform provider | T1.3 | Five resources via `main.tf`; Terraform-tracked state |
| [22](../../demos/v0.4.0/22-pulumi/) | Pulumi provider | T1.3 | Same five resources via TypeScript |

## Recipes

### Recipe 1: Run any demo end-to-end

```bash
cd demos/v0.4.0/01-anomaly-detection
./demo.sh           # interactive walkthrough — up, run, narrate
```

`./demo.sh` (no arg) defaults to `run`, which calls `up` first if
needed. Output streams to your terminal.

### Recipe 2: Just bring services up, leave them running

```bash
cd demos/v0.4.0/02-edge-proxy
./demo.sh up
# … containers start, services listen
docker compose ps      # confirm healthy
```

Useful when you want to poke at the running proxy yourself
(`curl http://localhost:9090/...`, `psql -h localhost -p 6432`).
See `heliosproxy-demo-down` to tear down.

### Recipe 3: Run the demo against an already-up cluster

```bash
cd demos/v0.4.0/01-anomaly-detection
./demo.sh up           # if not already up
./demo.sh run          # repeatable; doesn't require fresh state
```

Most demos are idempotent on `run`. Anomaly demo's "rate spike"
might saturate the buffer if run many times in quick succession;
restart with `down` + `up` to reset state.

### Recipe 4: Plugin demos — pre-built wasm vs build-on-demand

The plugin demos (11–18) need the relevant `.wasm` artefact in
the demo's `plugins/` directory. Two options:

**Build on demand** (requires Rust toolchain + wasm target):

```bash
rustup target add wasm32-unknown-unknown
cd demos/v0.4.0/11-cost-governor
./demo.sh up      # the shared `plugin-demo.sh` builds the .wasm if missing
```

**Pre-built** (drop into per-demo `plugins/` dir):

```bash
cd demos/v0.4.0/11-cost-governor/plugins
cp /path/to/helios_plugin_cost_governor.wasm .
./demo.sh up
```

The shared bring-up script
[`demos/v0.4.0/_shared/plugin-demo.sh`](../../demos/v0.4.0/_shared/plugin-demo.sh)
handles both paths.

### Recipe 5: Demo 9 / 10 — the universal smoke-tests

```bash
# 9: open the dashboard in a browser
cd demos/v0.4.0/09-admin-ui && ./demo.sh up
xdg-open http://localhost:9090/

# 10: curl tour of every admin endpoint
cd demos/v0.4.0/10-admin-rest && ./demo.sh
```

If you only have time to run two demos, run these. They exercise
every endpoint the other 20 demos use.

### Recipe 6: HeliosDB instead of PostgreSQL

Edit the demo's `docker-compose.yml`:

```yaml
# Before
image: postgres:17-alpine
# After
image: heliosdatabase/heliosdb-lite:latest
```

The wire protocol is identical; the rest of the demo runs unchanged.
See [`demos/v0.4.0/_shared/README.md`](../../demos/v0.4.0/_shared/README.md)
for the swap recipe.

## Pitfalls

- **Port conflicts on 6432, 9090, or 5432.** Another instance is
  bound. Find with `lsof -i :6432`. The demo's `docker-compose.yml`
  is the source of port mappings — edit if needed.
- **`docker compose up` builds the proxy from a `build:` context** —
  in the released demos, the compose files reference
  `ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.4.1`. If you're testing
  an unreleased build, swap `image:` for `build: ../../..`.
- **Plugin builds are slow first time.** The wasm target downloads
  + compiles wasmtime deps. Subsequent demos reuse the cargo cache.
- **`./demo.sh run` against a stale `up`.** State accumulates
  across runs (cache, journal, anomaly buffer). For a clean state,
  `./demo.sh down` first.
- **The 09-admin-ui demo serves a real HTML dashboard at the
  root of the admin port.** `curl http://localhost:9090/` returns
  HTML, not JSON. Use `/health`, `/topology`, etc. for JSON.

## See also

- `heliosproxy-demo-down` — clean tear-down
- `heliosproxy-overview` — pick the right skill before running a demo
- [`demos/v0.4.0/README.md`](../../demos/v0.4.0/README.md) — top-level index
- [`demos/v0.4.0/_shared/`](../../demos/v0.4.0/_shared/) — shared scaffolding
- Container image: `ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.4.1`
