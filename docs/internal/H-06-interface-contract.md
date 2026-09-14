# H-06 — interface policy contract

**Status:** first slice shipped 2026-09-14 (shared gateway connection pools,
`[limits] gateway_pool_max_idle`). This file records the contract each interface must
honor so "the same request" behaves the same through the PG-wire listener, the HTTP SQL
gateway, the GraphQL gateway and the MCP agent gateway.

## Interfaces

| Interface | Listener | Policy source | Connection model |
|---|---|---|---|
| PG-wire | `listen_address` (default `:5432`) | `[limits]`, `[rate_limit]`, `[circuit_breaker]`, `[multi_tenancy]`, `[query_rewrite]`, TR | Session holds backend connections (session/txn/statement pooling); H-05 admission applies |
| HTTP SQL (`POST /sql`) | `[http_gateway] listen_address` | gateway config + `[limits] gateway_pool_max_idle`; **not** the PG-wire rate limiter/circuit breaker today | Per-request checkout from its own idle pool (H-06); session-neutral statements return the connection |
| GraphQL | `[graphql_gateway] listen_address` | gateway config + same pool rule | Engine checks out per generated query, releases when the generated SQL is session-neutral |
| MCP | `[mcp] listen_address` | gateway config (+ optional agent contract) + same pool rule | Per-tool-call checkout; read-only mode re-asserts `default_transaction_read_only` on every checkout and retains those connections |

## Contract

- **Transaction scope.** One gateway request is one autocommit unit unless its SQL
  contains explicit transaction control. A connection is returned to a gateway pool only
  when `gateway_pool::statement_is_session_neutral` accepts the SQL (no `BEGIN/START/
  COMMIT/ROLLBACK/SET/RESET/DISCARD/DECLARE/LISTEN/PREPARE`, no `CREATE TEMP*`, no
  interior `;`). Everything else is discarded, so a request can never inherit another
  request's transaction or session state.
- **Pools are per gateway.** HTTP, MCP and GraphQL each own a pool; a connection that
  carries one gateway's session policy (MCP read-only GUC) is never handed to another.
  Retention is bounded by `gateway_pool_max_idle` per identity; a dead idle client is
  detected on checkout and dropped.
- **Auth / tenant identity.** PG-wire identity comes from the startup packet and `[auth]`;
  the gateways authenticate with their own bearer tokens/gateway config. The gateway
  backend user is fixed by gateway config (`backend_user`/`backend_database`) — there is
  no per-request tenant identity on the gateways yet, and therefore no RLS/GUC tenant
  injection. This is a known gap for full H-06 parity.
- **Admission.** The H-05 client cap (`max_client_connections`) and its bounded queue
  apply to the PG-wire listener only; gateways are bounded by their pools. Reserved
  capacity for health/admin is structural: health probes hold no client slot and the
  admin API is a separate listener.
- **Retries.** None of the interfaces retry a statement automatically. A failed gateway
  request returns an error; the caller retries. PG-wire transaction replay (TR) is the
  only automatic re-execution path and follows its own safety contract.

## Known gaps (tracked in #54 / H-06)

- Gateway requests do not consult the PG-wire rate limiter, circuit breaker, rewrite
  rules or tenant manager. The next slice should route gateway execution through a shared
  request-executor that applies those policies, or explicitly reject unsupported
  combinations at startup.
- `[mcp] read_only=true` is a backstop GUC plus a lexical guard, not a role-level
  enforcement; volatile functions with side effects are still possible (documented in
  `src/mcp.rs`).
- Pooling metrics (hit/reuse/discard counters) and per-gateway admission caps are not
  yet exposed; they belong with P-03 observability.
