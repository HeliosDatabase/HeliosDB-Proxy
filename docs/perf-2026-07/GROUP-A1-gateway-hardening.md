# Group A-1 — HTTP gateway request hardening

First slice of the security milestone from `NEXT-BATCH-audit.md`. The HTTP
`/sql`, MCP, and GraphQL gateways read attacker-controllable requests but never
received the request-parsing bounds the admin server got (H3/H4), and MCP had no
authentication at all (H2).

## Changes
- **New `src/http_util.rs`** — shared bounds for all HTTP-facing listeners:
  `HTTP_READ_TIMEOUT` (15s overall deadline), `MAX_HTTP_HEADERS` (100),
  `MAX_HTTP_HEADER_BYTES` (64 KiB), `MAX_HTTP_BODY_BYTES` (8 MiB), a
  `constant_time_eq_str`, and `read_head` / `read_body` helpers that enforce the
  deadline + caps. Unit-tested.
- **All three gateways** now read requests through `read_head`/`read_body`:
  - **H3** — `content_length > MAX_HTTP_BODY_BYTES` → **413 before allocating**
    (was `vec![0u8; content_length]` with no cap → a `Content-Length: 9e9` OOM).
  - **H4** — the whole request read is under a 15s `timeout_at` deadline with
    header count/byte caps (was an unbounded, untimed `read_line` loop →
    slow-loris pinned the handler task forever).
  - **M9** — Bearer comparison is now `constant_time_eq_str` (was `v == format!(
    "Bearer {}", tok)` — a short-circuiting `==` oracle plus a per-request alloc).
- **H2** — `McpConfig` gains `auth_token: Option<String>`; the MCP handler
  refuses a request without a matching Bearer (was: no auth field, no check —
  anonymous SQL to anyone who could reach the port).

Out of scope (behaviour-preserving): the gateways still dial a fresh backend +
auth per request and still bypass the wire-path pipeline — those are Milestone B.

## Gate
- `gateway-hardening-test.sh` 8/8: oversized `Content-Length` → 413 on all three
  gateways (proxy survives); MCP without token → 401, with token → works;
  slow-loris drip dropped at the read timeout; normal request works after abuse.
- No functional regression: `http-gw-test` 6/6, `mcp-test` 9/9, `graphql-test`
  4/4. clippy `-D warnings` ×4; 275 default + 1412 all-features lib tests
  (new `http_util` unit tests included).

## Next (Milestone A-2)
Admin default-open (H1: `0.0.0.0:9090` + `admin_token = None`) — default admin
to loopback, refuse a non-loopback bind without a token (with an explicit
opt-in), and cap concurrent admin connections (M8).
