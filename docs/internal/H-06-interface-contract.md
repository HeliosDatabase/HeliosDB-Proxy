# H-06 — interface policy contract

**Status:** first slice shipped 2026-09-14 (shared gateway connection pools,
`[limits] gateway_pool_max_idle`). This file records the contract each interface must
honor so "the same request" behaves the same through the PG-wire listener, the HTTP SQL
gateway, the GraphQL gateway and the MCP agent gateway.

## Interfaces

| Interface | Listener | Policy source | Connection model |
|---|---|---|---|
| PG-wire | `listen_address` (default `:5432`) | `[limits]`, `[rate_limit]`, `[circuit_breaker]`, `[multi_tenancy]`, `[query_rewrite]`, TR | Session holds backend connections (session/txn/statement pooling); H-05 admission applies |
| HTTP SQL (`POST /sql`) | `[http_gateway] listen_address` | gateway config + `[limits] gateway_pool_max_idle` + the shared gateway policy (`[rate_limit]`, `[query_rewrite]`, `[circuit_breaker]`) | Per-request checkout from its own idle pool (H-06); session-neutral statements return the connection |
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
  backend user is fixed by gateway config (`backend_user`/`backend_database`). With
  `[multi_tenancy]`, a gateway configured with `tenant_header` takes each request's tenant
  from that header (a known tenant is required, 403 otherwise) and applies the same row
  filter to every statement as the PG-wire listener does for a session; the header is
  trusted as sent, so an authenticating front end must set it.
- **Admission.** The H-05 client cap (`max_client_connections`) and its bounded queue
  apply to the PG-wire listener only; gateways are bounded by their pools. Reserved
  capacity for health/admin is structural: health probes hold no client slot and the
  admin API is a separate listener.
- **Retries.** None of the interfaces retry a statement automatically. A failed gateway
  request returns an error; the caller retries. PG-wire transaction replay (TR) is the
  only automatic re-execution path and follows its own safety contract.

## Shared request policy (slice 2, 2026-10-12)

`src/gateway_policy.rs` (`GatewayPolicy`) carries the daemon's own rate limiter, query
rewriter and circuit breaker into the three gateways:

- `[rate_limit]`: one check per gateway request (HTTP `POST /sql`, GraphQL request, MCP
  tool call), keyed by `[rate_limit] key_by` (`client_ip` = the HTTP peer, `user` /
  `database` = the gateway's backend user / database, `global`); a throttle verdict waits
  (capped at 5 s, as on PG-wire), a denial answers HTTP 429 (MCP: an in-band tool error);
- `[query_rewrite]`: every statement a gateway sends (HTTP, MCP, each GraphQL-generated
  statement) is rewritten by the same rules;
- `[circuit_breaker]`: a gateway fast-fails while its backend's circuit is open (HTTP 503)
  and records connect outcomes on it (an SQL error is the backend answering, not a fault);
- `[multi_tenancy]`: with a gateway's `tenant_header`, every statement gets the request
  tenant's row filter after the rewrite rules, as on PG-wire; a request without a known
  tenant is refused (HTTP 403; MCP answers the HTTP request with 403).

## Known gaps (tracked in #54 / H-06)

- A gateway without `tenant_header` has no per-request tenant identity, so
  `[multi_tenancy]` filtering does not reach it. The daemon refuses that combination
  under `strict_config` and warns otherwise. The tenant header is trusted as sent (no
  JWT/claims verification yet).
- `[mcp] read_only=true` is a backstop GUC plus a lexical guard; volatile functions with
  side effects are still possible. At startup the MCP gateway now probes whether its
  backend role can INSERT/UPDATE/DELETE/TRUNCATE any user table: `strict_config` refuses
  to start then, otherwise it warns. A role without write privileges is the real fix.
- Per-gateway pool metrics are exported (P-03); per-request tenant identity is a trusted
  header (`tenant_header`); JWT-verified identities are not implemented.
