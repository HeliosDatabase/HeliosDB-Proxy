# Group 4 — Stability hardening (control plane + pre-auth surface)

Fixes from the control-plane stability sweep. These are small, high-confidence,
and largely on **unauthenticated or default-exposed** paths — highest safety
value per line changed.

## Delivery split

- **M4 (this milestone):** 4.3 admin body/header caps + read timeout, 4.5
  health-interval validation + clamp, 4.6 pre-auth startup timeout. All on the
  unauthenticated / default-open surface, all independent and low-risk.
- **M4b (follow-up):** 4.2 session RAII guard (needs the `sessions` map moved
  off a tokio `RwLock` so `Drop` can deregister synchronously), 4.6 global
  connection semaphore, 4.7 task supervision, 4.8 typed `is_backend_fault`.
  Deferred because M1 already removed the main panic triggers (4.1) and these
  touch broader surfaces.

## Findings & fixes

### 4.1 Protocol length underflow → panic (HIGH, unauth remote crash)
- `decode_message` (protocol.rs:303) does `src.split_to(len - 4)` after only
  an upper-bound check. A frame with declared `len < 4` (e.g. `Q 00 00 00 00`)
  underflows `usize` → `split_to` panics. On the hot query path AND backend
  responses.
- `decode_startup` (protocol.rs:246,262): a startup frame with `len` in 4..=7
  panics via `get_u32` (nothing left) or `remaining = len - 8` underflow.
  **Pre-auth**: a 4-byte `00 00 00 04` reliably crashes the handler task.
- **Fix:** `decode_message` → reject `len < 4` with `ProxyError::Protocol`
  before `split_to`. `decode_startup` → require `len >= 8` before reading
  version/params. Unit tests for each malformed frame.

### 4.2 Session leak on connection-task panic (HIGH)
`handle_client` (server.rs:1490-1533) inserts the session into
`state.sessions` before `negotiate_client_tls`/`client_loop` and removes it
after — no unwind guard. Any panic (incl. 4.1) leaks the `ClientSession`
Arc, inflates the active-session gauge, and makes `drain_connections` never
reach 0 → every `SIGUSR2` drain waits the full timeout.
**Fix:** RAII guard whose `Drop` removes the session from the map, so cleanup
runs on unwind too. (4.1 removes the main trigger; this bounds the blast
radius of any future panic.)

### 4.3 Admin unbounded body/ header allocation (HIGH, default-open DoS)
`handle_connection` (admin.rs:303-309) reads `Content-Length`
(`parse().unwrap_or(0)`, no cap) then `vec![0u8; content_length]` — a
`Content-Length: 99999999999` forces a multi-GB zero-fill → OOM/abort of the
whole process. Admin defaults to `0.0.0.0:9090` with `admin_token = None`
(open). Header loop (admin.rs:241-262) is likewise unbounded. Response body
from a backend (admin.rs:1000-1006) is uncapped too.
**Fix:** cap request body (reject > 8 MiB with 413) and header count/size
before allocating; cap the backend-response body. (Binding admin to loopback
by default is a separate config-policy call — noted, not bundled.)

### 4.4 Admin holds health/config read locks across an un-timed backend forward (MED)
`handle_sql_request` (admin.rs:789-823) keeps `proxy_config.read()` and
`node_health.read()` guards across `forward_sql_request().await`, which has
no connect/read timeout. A slow backend pins the `node_health` read lock,
blocking the 1 s health-sync writer (server.rs:1408) and all admin readers.
**Fix:** clone the target address, drop both guards before forwarding, wrap
the backend I/O in `tokio::time::timeout`.

### 4.5 Health interval = 0 panics the checker (MED)
`spawn_health_checker` (server.rs:4860) builds
`interval(Duration::from_secs(check_interval_secs))`; tokio `interval` panics
on a zero period, and `ProxyConfig::validate` never checks it. A config (or
SIGHUP reload) with `check_interval_secs = 0` kills health checking silently
→ the proxy routes to dead backends forever (initial health seeds all-healthy).
**Fix:** validate `check_interval_secs >= 1` in `ProxyConfig::validate`;
clamp to a 1 s floor in `spawn_health_checker` as belt-and-suspenders.

### 4.6 No pre-auth timeout or connection cap (MED, slowloris)
The startup read loops (server.rs:2058/2118) and `acceptor.accept` (2076) can
block forever; the session is already registered (4.2), so a stalled
unauthenticated client parks a task + session-map entry indefinitely, and
there is no accept-side concurrency limit.
**Fix:** wrap `negotiate_client_tls` + `handle_startup` (incl. TLS accept) in
a startup deadline (configurable, default ~10 s); add a global connection
`Semaphore` on the accept loop (configurable cap, default high e.g. 10 000)
that sheds load with a clean error instead of unbounded task growth.

### 4.7 Background services unsupervised (MED)
The health task handle is only `.abort()`ed at shutdown, never watched — a
panic in `check_all_nodes` ends health checking permanently with no restart
and no structured log. Mirror/MCP/HTTP/GraphQL workers are fire-and-forget.
**Fix:** guard `check_all_nodes`'s per-tick body so a probe panic can't kill
the loop (the JoinSet tasks are already isolated; the concern is the reducer),
and log-fatal + restart the health loop if its task exits unexpectedly.

### 4.8 `is_backend_fault` classifies by Display substring (LOW, brittle)
`!err.contains("Client") && !err.contains("Backend read timeout")`
(server.rs:3851) is the single gate for in-band demotion AND circuit
recording. A real backend error whose text contains "Client" never demotes.
**Fix:** thread a typed discriminant (a small `FaultKind` enum, or match on
`ProxyError` variant / `io::ErrorKind`) from the call sites instead of
re-parsing the formatted string. Keep the existing behaviour for the two
excluded classes; only the *classification mechanism* changes.

## Explicitly deferred (documented, not in this group)
- Mid-session redial against md5/SCRAM backends (needs proxy-held backend
  creds; the long-standing Batch-C blocker) — fail-fast at startup when
  rw-split/pool-modes is configured against a non-trust backend is a smaller
  follow-up.
- `load_balancer.rs` / `connection_pool.rs` / `pool/manager.rs` /
  `pool/hardening.rs` dead scaffolding (`Handle::block_on` landmine, stub
  validators, lease-counter drift) — latent, unwired; either wire loudly or
  delete. Tracked for a later cleanup pass.
- `failover_controller.rs` / `primary_tracker.rs` unbounded `history` Vec and
  busy-poll — dead at runtime in this binary (topology features unwired).

## Risk & tests
- Risk: LOW. Each fix is local and independently revertable.
- New unit tests: malformed-frame rejection (4.1), zero-interval validation
  (4.5), oversized Content-Length rejection (4.3), session-guard Drop (4.2).
- New live test `malformed-input-test.sh`: raw sockets sending short/zero-len
  frames + oversized admin Content-Length; assert the proxy stays up and
  keeps serving; assert active-session gauge returns to 0 after the abusive
  conns close (4.2).
- Gate: full milestone protocol + `admin-auth-test.sh`, `reload-test.sh`,
  `drain-timeout-test.sh`, `handoff-test.sh`.
