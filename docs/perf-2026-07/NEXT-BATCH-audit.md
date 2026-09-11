# Next-batch audit (2026-07-05, Fable 5 investigation)

**Status: CLOSED 2026-09-11.** Every item below was re-checked against the code at
`67b4ec3`. What shipped is recorded in the first table; what is still true was
re-filed into the active backlog, `docs/internal/audit-2026-09/IMPROVEMENTS.md`,
under its original identifier. This file is now history — do not add work to it.

Independent deep sweep of surfaces not covered by the 2026-07 milestones
(gateways, edge, mirror/migration, branch/replay/shadow/upgrade, admin routes,
server accept path, TLS). Full raw findings preserved in the session; the
milestone plan below drove the work.

**Confirmed clean:** the M3b idle backend-watch `select!` is cancel-safe for
`ClientStream::Tls` (rustls owns the reassembly buffers; dropping the losing
`read_buf` future loses nothing). Release profile (thin LTO, codegen-units=1,
panic=unwind) correct. `branch.rs`/`mirror.rs`/`replay`/`shadow` injection
escaping correct. `client_tls.rs` correct.

## Closed — verified in the code, 2026-09-11

| Item | Where it landed |
|---|---|
| H1 admin default-open | `admin_address` defaults to `127.0.0.1:9090`, `admin_allow_insecure` is the explicit escape hatch, and `validate()` refuses a non-loopback bind without a token (`src/config.rs:1338`, `:1846`) |
| H2 MCP gateway unauthenticated | `McpConfig.auth_token`, enforced before dispatch (`src/mcp.rs:87`) |
| H3 uncapped body allocation | All three gateways reject `content_length > http_util::MAX_HTTP_BODY_BYTES` before allocating and read through the bounded `http_util::read_body` |
| H4 read timeout, header loop | One overall request deadline plus header count and byte caps in `http_util` (`src/http_gateway.rs:72`). The connection cap is the one part that did not land — carried forward |
| H5/H6 edge invalidation and query path | Delivered by the G-D edge build-out (PR #36) |
| M3 MCP DML-in-CTE bypass | `has_data_modifying_cte` in `check_policy`, with `effective_read_only` and the backend `READ ONLY` backstop (`src/mcp.rs:472`) |
| M4 per-connection `ProxyConfig` deep clone | `Arc<ProxyConfig>` through the accept path (`src/server.rs:2779`) |
| M7 upgrade-orchestrator conninfo injection | `backend_for` parses `host:port` into a typed `BackendConfig`; no conninfo string is assembled (`src/upgrade_orchestrator/mod.rs:565`) |
| M8 admin connection semaphore | `MAX_ADMIN_CONNS` with `try_acquire_owned` per accept (`src/admin.rs:202`) |
| M9 Bearer compared with `==` | All three gateways use `http_util::constant_time_eq_str` |
| M10 edge registry never GCs | Idle-prune window in `EdgeConfig`, swept by the home registry |
| M11 per-hit global LRU scan | `edge/cache.rs` uses `lru::LruCache` (O(1) touch) |
| M12 LWW versions unsynchronized | Superseded by the home-authoritative clock; the remaining design question is D-01 in the active backlog |
| L5 edge config unwired | `[edge]` is parsed and wired into the edge client and cache |

## Carried forward — re-filed in IMPROVEMENTS.md, 2026-09-11

These were re-confirmed as still present. They keep their original identifiers in
the active backlog: M1, M2, M5, M6, M13/F0, L1, L7, L8, L9, L13, and the gateway
connection cap left over from H4.

## Original findings (2026-07-05)

Item status is in the tables above; the text below is the original plan as written.

## Milestone A — close the HTTP control surfaces (SECURITY; do first)
- **H1** admin default-open: `admin_address = 0.0.0.0:9090` + `admin_token = None`
  → anonymous `POST /api/sql|chaos|migration/cutover|branch|replay|shadow`.
  Fix: default admin to loopback; refuse non-loopback bind without a token
  (behind an explicit `admin_allow_insecure` escape hatch to avoid silently
  breaking existing deploys — decide policy).
- **H2** MCP gateway has no auth field at all (`McpConfig` lacks `auth_token`).
- **H3** gateways `vec![0u8; content_length]` uncapped (http_gateway.rs:126,
  graphql_gateway.rs:162, mcp.rs:101) — the admin OOM fix never reached them.
- **H4** gateways: no read timeout, unbounded header loop, no conn cap (slowloris).
- **M8** admin: no concurrent-connection semaphore. **M9** gateways compare
  Bearer with `==` not the existing `constant_time_eq_str`.
- Gate: startup refuses non-loopback admin w/o token; each mutating route 401
  w/o token; `Content-Length: 10^10` → 413 RSS-flat on every gateway; drip
  header dropped within timeout; MCP tool call 401 w/o token.

## Milestone B — make the gateways real data-plane paths
- **M1** gateways bypass rate-limit/anomaly/plugins/tenancy/cache/HA entirely.
- **M2** gateways dial a fresh backend + full auth PER REQUEST (GraphQL: per
  top-level field). No pooling despite `backend_pool` existing.
- **M3** MCP `is_write_sql` first-keyword only → DML-in-CTE bypasses `read_only`.
- **F0/M13** `BackendClient::run_query` hardcodes 30s, ignoring `query_timeout`
  (branch clone, replay, upgrade all mis-timeout). Fix globally.

## Milestone C — migration/cutover safety
- **M5** `migration_ready` ignores apply `errors` → false "safe to cut over"
  (mirror.rs:75 → add `&& errors == 0`).
- **M6** `shadow_execute` buffers full result sets from both backends unbounded.
- **M7** `upgrade_orchestrator` conninfo injection via `from_address` host.
- **L7** `replay entries_in_window` holds journal lock across clone+sort;
  **L8** replay no overall deadline; **L9** shadow runs sequentially.

## Milestone D — edge: finish or fence (decision)
Edge is non-functional end-to-end: **H5** invalidation reaches zero edges
(receiver dropped at registration; first broadcast prunes all); **H6** cache/
registry never invoked on the query path; **M10** registry never GCs → wedges at
capacity; **M11** per-hit global LRU O(n) scan; **M12** LWW versions per-process
unsynchronized; **L5** edge config entirely unwired. Either wire it end-to-end
or gate `edge-proxy` experimental and stop constructing it.

## Fold-in perf (next G-series)
- **M4** `handle_client` deep-clones the whole `ProxyConfig` per accepted
  connection (server.rs accept path) — change to `Arc<ProxyConfig>`, one refcount
  bump. Highest-value clean win under connection churn.
- **L1** extended-batch `batch_refs`/`batch_defines` Vecs cleared only on Sync
  (O(n²) reprepare for a never-Sync client). **L13** `create_error_response`
  HashMap+4 Strings per error frame (cold path).
