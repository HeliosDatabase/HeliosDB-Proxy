# HeliosProxy 46-Module Demo Map

Use this as the on-stage checklist. "Proof" is the fastest local evidence to
show: an endpoint, an existing demo, or a code path.

| # | Tier | Module | Flag | Proof |
|---|------|--------|------|-------|
| 1 | v0.3 | Connection Pooling | `pool-modes` | `src/pool/`, OLTP race |
| 2 | v0.3 | Load Balancer | core | `GET /nodes`, `src/load_balancer.rs` |
| 3 | v0.3 | Health Checker | core | `GET /health/ready`, `src/health_checker.rs` |
| 4 | v0.3 | Pipeline | core | `src/pipeline.rs` |
| 5 | v0.3 | Batch Operations | core | `src/batch.rs` |
| 6 | v0.3 | Failover Controller | core | `run-main-demos.sh switchover` |
| 7 | v0.3 | Transaction Replay | `ha-tr` | `POST /api/replay` |
| 8 | v0.3 | Session Migration | `ha-tr` | `src/session_migrate.rs` |
| 9 | v0.3 | Cursor Restore | `ha-tr` | `src/cursor_restore.rs` |
| 10 | v0.3 | Switchover Buffer | core | `src/switchover_buffer.rs` |
| 11 | v0.3 | Primary Tracker | core | `src/primary_tracker.rs`, `/topology` |
| 12 | v0.3 | Transaction Journal | `ha-tr` | `src/transaction_journal.rs` |
| 13 | v0.3 | Query Cache | `query-cache` | `src/cache/` |
| 14 | v0.3 | Query Routing | `routing-hints` | `src/routing/` |
| 15 | v0.3 | Lag-Aware Routing | `lag-routing` | `demos/lag-aware-routing/` |
| 16 | v0.3 | Query Rewriter | `query-rewriting` | `src/rewriter/` |
| 17 | v0.3 | Query Analytics | `query-analytics` | `src/analytics/` |
| 18 | v0.3 | Schema Routing | `schema-routing` | `src/schema_routing/` |
| 19 | v0.3 | Auth Proxy | `auth-proxy` | `src/auth/` |
| 20 | v0.3 | Rate Limiter | `rate-limiting` | `src/rate_limit/` |
| 21 | v0.3 | Circuit Breaker | `circuit-breaker` | `src/circuit_breaker/` |
| 22 | v0.3 | Multi-Tenancy | `multi-tenancy` | `examples/multi-tenant/` |
| 23 | v0.3 | WASM Plugins | `wasm-plugins` | `GET /plugins`, demos 11-18 |
| 24 | v0.3 | GraphQL Gateway | `graphql-gateway` | `src/graphql/` |
| 25 | v0.4 | Anomaly Detection | `anomaly-detection` | `run-main-demos.sh anomaly` |
| 26 | v0.4 | Edge Mode | `edge-proxy` | `demos/v0.4.0/02-edge-proxy/` |
| 27 | v0.4 | Plugin Host KV | `wasm-plugins` | `demos/v0.4.0/03-plugin-kv/` |
| 28 | v0.4 | Plugin Host Crypto | `wasm-plugins` | `demos/v0.4.0/04-plugin-crypto/` |
| 29 | v0.4 | Plugin Signatures | `wasm-plugins` | `demos/v0.4.0/05-plugin-signatures/` |
| 30 | v0.4 | OCI Plugin Artifacts | `wasm-plugins` | `demos/v0.4.0/06-plugin-oci/` |
| 31 | v0.4 | Plugin Route-Block | `wasm-plugins` | `demos/v0.4.0/07-route-block/` |
| 32 | v0.4 | Plugin Trust Root Config | `wasm-plugins` | `demos/v0.4.0/08-trust-root/` |
| 33 | v0.4 | Admin Web UI | core | `http://localhost:9090/` |
| 34 | v0.4 | Admin REST v2 | core | `demos/v0.4.0/10-admin-rest/` |
| 35 | v0.4 | cost-governor | plugin | `demos/v0.4.0/11-cost-governor/` |
| 36 | v0.4 | ai-classifier | plugin | `demos/v0.4.0/12-ai-classifier/` |
| 37 | v0.4 | token-budget | plugin | `demos/v0.4.0/13-token-budget/` |
| 38 | v0.4 | llm-guardrail | plugin | `demos/v0.4.0/14-llm-guardrail/` |
| 39 | v0.4 | pgvector-router | plugin | `demos/v0.4.0/15-pgvector-router/` |
| 40 | v0.4 | column-mask | plugin | `demos/v0.4.0/16-column-mask/` |
| 41 | v0.4 | audit-chain | plugin | `demos/v0.4.0/17-audit-chain/` |
| 42 | v0.4 | residency-router | plugin | `demos/v0.4.0/18-residency-router/` |
| 43 | v0.4 | helios-plugin CLI | companion | `demos/v0.4.0/19-helios-plugin-cli/` |
| 44 | v0.4 | Kubernetes Operator | companion | `demos/v0.4.0/20-k8s-operator/` |
| 45 | v0.4 | Terraform Provider | companion | `demos/v0.4.0/21-terraform/` |
| 46 | v0.4 | Pulumi Provider | companion | `demos/v0.4.0/22-pulumi/` |

