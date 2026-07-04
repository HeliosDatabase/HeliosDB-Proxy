# Group 3 — Event-driven relays (kill the polling stalls)

**Goal:** remove the three places the relay substitutes a timer poll for
actual readiness, each of which is a measurable latency cliff or a hang.

## Delivery split

- **M3 (this milestone) — auth relay:** 3.A (poll deadlock) + 3.A.2
  (`ErrorResponse`-blind auth loops). Both `proxy_authentication` (passthrough)
  and `complete_backend_auth` (redial) rewritten. Self-contained to the
  startup/auth path; proven by a new `slow-auth-test.sh` that fails on the
  pre-fix binary.
- **M3b (follow-up) — data-path relays:** 3.B (Flush 200 ms stall) + 3.C (idle
  LISTEN/NOTIFY + idle backend-death detection). Deferred because both need a
  client-readability probe on the `ClientStream` (Plain/Tls) enum, which is a
  larger, riskier change on the query hot path.

## Findings

### 3.A Auth relay: 100 ms client poll + unbounded backend read (BUG)
`proxy_authentication` (server.rs:2546-2639) relays startup auth by looping:
read backend (NO timeout) → forward → poll client with a **100 ms** timeout →
forward. A client that takes >100 ms to answer a challenge (slow SCRAM
client, high-RTT link, loaded host) misses its window; the loop re-enters
the untimed backend read while the backend itself is waiting for the client
→ **startup deadlocks** until the client gives up. Even the happy path eats
up to 100 ms of dead time per challenge round when the client answer lands
just after a poll expires.

**Fix:** replace the poll loop with a `tokio::select!` relay over both
directions (forward frames as they arrive, watch for RFQ / ErrorResponse on
the backend leg) under one overall auth deadline (30 s). No per-direction
timers.

### 3.B Flush relay: 200 ms idle sniff per Flush (LATENCY)
`stream_flush` (server.rs:3665-3696) returns only after the backend has been
silent for **200 ms**. Drivers that use `Flush` during statement preparation
(node-postgres et al.) pay up to +200 ms per prepare, and the session loop
cannot see the client's already-sent next messages while sniffing.

**Fix:** select on backend-read vs client-readability: relay backend bytes
as they come; return as soon as the client has data to process (its next
frames are the real signal that the driver got what it waited for) or on a
much shorter quiet period as fallback. `ClientStream` needs a
`poll_read_ready`-style probe (TCP: `TcpStream::readable()`; TLS: readable
on the underlying socket OR buffered plaintext remaining).

### 3.A.2 Auth loops are blind to backend ErrorResponse (BUG, agent-verified)
`MessageType::from_tag` maps backend tag `'E'` to `Execute` (client-side
variant), so the `MessageType::ErrorResponse` match arms in
`proxy_authentication` (server.rs:2609) and `complete_backend_auth`
(server.rs:4233) are **dead code**. A backend auth failure (wrong password,
unknown database) is treated as "continue reading" and surfaces as a
misleading timeout/"Backend closed during auth" instead of the real error.
**Fix:** disambiguate on the raw tag byte in these direction-aware loops
(as the relay scanners at server.rs:3514/3595 already do).

### 3.C Idle sessions are deaf (NOTIFY / async messages / dead backends)
While parked on `stream.read()` waiting for the *client* (client_loop:1625),
the proxy never reads the backend socket. Consequences: `LISTEN/NOTIFY`
notifications, `NoticeResponse`, and `ParameterStatus` sit in the backend
buffer until the next query completes (LISTEN through the proxy is
effectively broken for idle clients); a backend that dies while the session
is idle goes unnoticed until the next forward fails.

**Fix:** at the idle point, `select!` between client read and a read on the
*current* backend conn. Backend bytes while idle are relayed frame-whole to
the client (they can only be async frames in that state); backend EOF/error
while idle drops the cached conn (next query redials) without killing the
client session unless in transaction. Applies only to the session-pinned
current conn; pooled-away conns are not watched (they are not attached).

## Ordering & scope
3.A is self-contained (startup path only). 3.B and 3.C share the
"client-readability probe" mechanism — implement together. All three are
behaviour-preserving for well-behaved fast peers; they change *waiting*, not
framing.

## Risk & tests
- Risk: MED-HIGH (main-loop state machine). Mitigations: frame-whole
  relaying only (partial frames stay buffered), no change to the
  in-response path (`stream_until_ready` untouched), feature-flag-free but
  each fix independently revertable.
- New live tests:
  - `notify-test.sh`: psql LISTEN via proxy; NOTIFY from a second direct
    conn; assert the notification arrives while the listener is idle (today
    it demonstrably does not) — plus notification during a query gap.
  - `flush-latency-test.sh`: time a Parse+Describe+Flush→Bind+Execute+Sync
    sequence (psql `\parse`/`\bind_named`); assert wall time < 100 ms
    (today ≥ 200 ms).
  - Slow-auth test: scripted client answering the SCRAM challenge after
    300 ms; assert startup succeeds (today: hang/failure).
- Gate: full milestone protocol; auth changes additionally re-run
  `scram-test.sh`, `tls-test.sh`, `hba-test.sh`, `ldap-test.sh` (auth-adjacent).

## Expected outcome
No 200 ms Flush cliff; no 100 ms auth poll floor or slow-client deadlock;
LISTEN/NOTIFY works through idle sessions; idle backend death detected
immediately. Throughput unchanged (these are latency/correctness wins).
