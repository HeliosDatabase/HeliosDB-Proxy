# HeliosProxy v0.4.0 demos — 22 hands-on showcases

One self-contained demo per feature added in v0.4.0. Each demo:

- has a **README.md** with the UVP, use cases, and a step-by-step
  walkthrough,
- ships a **`docker-compose.yml`** with a working PostgreSQL 17
  backend (HeliosDB swap notes inline),
- and a **`demo.sh`** entry point with `up` / `run` / `down`
  subcommands.

```bash
cd <demo-dir>
./demo.sh           # interactive walkthrough
./demo.sh up        # just bring up services + leave them running
./demo.sh down      # tear everything down + remove volumes
```

## Prerequisites

- Docker + Docker Compose v2
- `psql` (PostgreSQL 14+ client)
- `curl` and `jq`
- For plugin demos: a Rust toolchain with `wasm32-unknown-unknown`
  target, **OR** pre-built `.wasm` artefacts dropped into the
  per-demo `plugins/` directory.

## Demo index

| # | Demo | Module | Runnable | What it proves |
|---|---|---|---|---|
| [1](01-anomaly-detection/) | Anomaly Detection | `anomaly-detection` | yes | SQLi + auth burst + novel query — three families fire concurrently against a single backend |
| [2](02-edge-proxy/) | Edge / Geo Proxy | `edge-proxy` | yes | Two proxies (home + edge), invalidation broadcast on write, ~40× cache speedup |
| [3](03-plugin-kv/) | Plugin host KV bridge | `wasm-plugins` | yes (via Demo 11) | Per-plugin namespaced state survives across hook invocations |
| [4](04-plugin-crypto/) | Plugin host crypto | `wasm-plugins` | walkthrough | RFC 6234 SHA-256 vector check + audit-chain producing real digests |
| [5](05-plugin-signatures/) | Plugin Ed25519 signatures | `wasm-plugins` | yes | openssl-signed `.wasm` loads; tampered or unsigned refuses |
| [6](06-plugin-oci/) | Plugin OCI artefact loader | `wasm-plugins` | yes | `helios-plugin pack` → drop tarball → proxy loads + validates SHA-256 |
| [7](07-route-block/) | `RouteResult::Block` | `wasm-plugins` | yes (via Demo 18) | Route hook produces clean PG `ErrorResponse` on rejection |
| [8](08-trust-root/) | `trust_root` config knob | `wasm-plugins` | walkthrough | Same proxy binary, different TOML → permissive vs enforced |
| [9](09-admin-ui/) | Admin Web UI | (always-on) | yes | 10-panel dashboard at `http://localhost:9090/`, auto-refresh |
| [10](10-admin-rest/) | Admin REST API tour | (always-on) | yes | `curl` tour of all 8 new endpoints |
| [11](11-cost-governor/) | Plugin: cost-governor | T2.3 | yes | Per-tenant budget exhaustion → block; recovery after window resets |
| [12](12-ai-classifier/) | Plugin: ai-classifier | T2.2 | yes | LLM detection from `application_name` keywords + generated-by markers |
| [13](13-token-budget/) | Plugin: token-budget | T2.2 | yes | Per-`(agent, model)` cost gate for AI traffic |
| [14](14-llm-guardrail/) | Plugin: llm-guardrail | T2.2 | yes | DROP, missing WHERE, missing tenant_id all refused for AI traffic |
| [15](15-pgvector-router/) | Plugin: pgvector-router | T2.2 | yes | Vector top-K → pg-vector replica; non-vector → pg-primary |
| [16](16-column-mask/) | Plugin: column-mask | T2.4 | yes | Same query, different roles, masked vs raw PII |
| [17](17-audit-chain/) | Plugin: audit-chain | T2.4 | yes | Hash-chained tamper-evident log; verify_chain catches mutation |
| [18](18-residency-router/) | Plugin: residency-router | T2.4 | yes | EU users → EU replica; US users → US replica; unknown region → block |
| [19](19-helios-plugin-cli/) | `helios-plugin` CLI | (build tool) | walkthrough | Pack/inspect/verify with openssl-generated key |
| [20](20-k8s-operator/) | Kubernetes operator | T1.1 | yes (kind cluster) | One CR brings up ConfigMap + Deployment + Service; status flips Pending → Ready |
| [21](21-terraform/) | Terraform provider | T1.3 | walkthrough (needs Demo 20) | Five resources via `main.tf`; Terraform-tracked state |
| [22](22-pulumi/) | Pulumi provider | T1.3 | walkthrough (needs Demo 20) | Same five resources via TypeScript |

## Shared assets

[`_shared/`](_shared/) contains files multiple demos depend on:

- `proxy.base.toml` — minimal proxy config
- `init.sql` — sample schema (users + orders + events) with mask
  functions and roles
- `wait-for.sh` — TCP-port poller used by every `demo.sh`
- `plugin-demo.sh` — shared bring-up logic sourced by per-plugin demos

## HeliosDB compatibility

Every demo uses **`postgres:17-alpine`** as the default backend so
`docker compose up` works on any laptop. The wire protocol is
identical between PG and HeliosDB; swap the `image:` line for
`dimensigon/heliosdb-lite:latest` (or your local build) and every
demo behaves the same. See [_shared/README.md](_shared/README.md)
for the swap recipe.

## Container image

All demos pull `ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.0`. The
image is published by the docker workflow on tag pushes; if you're
running against an unreleased build, swap the `image:` line for
`build:` pointing at your local proxy repo.

## Where to file improvements

[github.com/dimensigon/HDB-HeliosDB-Proxy/issues](https://github.com/dimensigon/HDB-HeliosDB-Proxy/issues)
— one issue per demo, please. Pull requests welcome; new demos
follow the layout described at the top of this file.
