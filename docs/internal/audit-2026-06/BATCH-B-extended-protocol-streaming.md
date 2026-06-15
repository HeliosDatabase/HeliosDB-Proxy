# BATCH B — Extended-protocol support + streaming relay (route_and_forward rewrite)

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** Fix the verified correctness bug that makes extended-protocol drivers (asyncpg, psycopg3, JDBC, node-postgres prepared statements) stall 30s per message, and replace full store-and-forward response buffering with a streaming relay. These two share one rewrite of the relay core.

**Parallel-execution compatibility:** SOLO only (single coherent rewrite). Do not split across agents.

**Prerequisites:** BATCH A merged (it reshapes the same loops).

**Files touched:** `src/server.rs` (`client_loop`, `route_and_forward` and helpers), possibly `src/protocol.rs` (frame-tag peek helper)

**Conflicts with:** Conflicts with BATCH A and C (same function). Execute AFTER A; C builds on B's relay shape. Compatible in parallel with D/E/F/G/H.

**Acceptance criteria:**

- asyncpg (or psycopg3) connect + prepared-statement round-trip works through the proxy (this fails/stalls today).
- `pgbench -M extended` completes through the proxy.
- Time-to-first-byte for a large SELECT ≈ backend TTFB (not full-transfer time); proxy RSS stays flat during a 1GB result relay.
- Simple-protocol path (psql, `pgbench -M simple`) unchanged and green.
- Plugin pre/route/post hooks and anomaly observation still fire per client message.

---

## Design (from audit + verifier guidance)

1. **Protocol-phase tracking in `client_loop`:** classify client frames. `Query`/`FunctionCall` keep the existing request→response shape. Extended frames (`Parse`,`Bind`,`Describe`,`Execute`,`Close`) are appended to a pending batch (routing decided from the first `Parse`/`Query` content in the batch); nothing is awaited per-frame.
2. **Barriers:** on `Sync` → forward batch+Sync, then relay backend→client until `ReadyForQuery`. On `Flush` → forward batch+Flush, then relay backend→client until the client socket becomes readable again (the backend sends everything pre-Flush and then goes quiet; client activity is the deadlock-free exit signal). Implement with `tokio::select!` over both sockets.
3. **Streaming:** during the relay phase, write frames (or raw chunks with a frame-boundary tail tracker) to the client as they arrive. Only the 5-byte frame header needs decoding to detect `ReadyForQuery`/`ErrorResponse` — payloads forward verbatim. Keep a transaction-status byte from RFQ for `session.tx_state`.
4. **Error semantics:** after backend `ErrorResponse` mid-batch, PG discards until `Sync` — the relay already terminates on RFQ after Sync, so no special casing beyond not double-forwarding.
5. **Pipelining:** if client data arrives while awaiting RFQ (pipelined drivers), buffer it; process it as the next batch after RFQ. Do not interleave routing decisions mid-pipeline.

### Extended query protocol (Parse/Bind/Execute) stalls 30s per message and kills the session

- **Location:** `src/server.rs:1276`
- **Severity / category:** high / protocol
- **Found by:** `hot-path-protocol` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
if resp_msg.msg_type == MessageType::ReadyForQuery {
```

**Impact:** client_loop forwards every decoded client message individually through route_and_forward, whose read loop only returns on ReadyForQuery (or EOF) under a 30s timeout (line 1262). PostgreSQL does not emit ReadyForQuery for Parse/Bind/Execute until Sync, so a Parse is forwarded alone, the backend buffers it silently, the proxy times out after 30s ('Backend read timeout'), the error propagates and the whole client connection is torn down. Every extended-protocol client (JDBC, asyncpg, npgsql, pgx with prepared statements) is effectively unusable; this is also the architectural ceiling that prevents any pipelining.

**Fix:** Track protocol phase: for extended-protocol messages, buffer/forward frames without awaiting a response and only enter the response-relay loop after forwarding Sync (or Flush, terminating on the matching response set). Better: replace the per-message request/response RPC shape with a bidirectional relay (tokio::select! over both sockets) that tracks Sync/ReadyForQuery boundaries only for routing decisions. Restores extended-protocol support and enables pipelining.

**Verifier correction/nuance:** Minor: the session stalls 30s on the FIRST extended-protocol message and is then torn down, so it is one 30s stall per connection attempt rather than literally "30s per message" — the connection never survives to a second Parse/Bind/Execute. All other claims are accurate as stated.

<details><summary>Verifier reasoning</summary>

Verified in /home/gpc/HDB/Proxy/src/server.rs. (1) Evidence is verbatim: line 1276 is `if resp_msg.msg_type == MessageType::ReadyForQuery {` and line 1262 is the 30s `tokio::time::timeout(Duration::from_secs(30), backend.read(...))` producing "Backend read timeout". (2) client_loop (line 729) decodes one frame at a time (line 773) and calls route_and_forward per frame (line 837); route_and_forward writes exactly one frame (lines 1249-1253) then loops reading until ReadyForQuery (1276-1283) or EOF (1267). There is no handling of Sync/Flush anywhere on the data path (MessageType::Sync appears in server.rs only in a test at line 2354); is_write_message (line 1303) inspects Parse only for routing classification. PostgreSQL buffers ParseComplete/BindComplete until Sync or Flush, so a lone forwarded Parse returns zero bytes, the read times out at 30s, the error propagates via `forward_result?` (line 854) out of client_loop, and handle_connection (line 698) destroys the session — the client's Sync is never forwarded. (3) Hot path: this is the sole per-message data path for every client connection; route_and_forward's only caller is client_loop. JDBC/asyncpg/npgsql/pgx all use extended protocol by default, so the proxy is unusable for them, and the per-message request/response shape is the structural pipelining blocker. (4) No mitigating alternate path exists: src/pipeline.rs (README line 63 advertises "extended query protocol pipelining") is an unwired queue used only by a test at src/lib.rs:599; pool-modes adds only lease bookkeeping; src/backend/mod.rs explicitly scopes the internal client to simple protocol. (5) The suggested fix (buffer extended frames until Sync/Flush before entering the response relay, or a select!-based bidirectional relay tracking ReadyForQuery boundaries) is the standard pgbouncer/pgcat design, is not implemented anywhere in the repo, and does not break routing correctness since routing can be decided at Sync boundaries. Fixing it restores extended-protocol clients (currently 30s stall + connection kill) and removes the architectural ceiling on pipelining, so it plausibly improves production latency and throughput.

</details>


### Full store-and-forward of every backend response with triple copy per byte

- **Location:** `src/server.rs:1271`
- **Severity / category:** high / io
- **Found by:** `hot-path-protocol` auditor; independently confirmed by `infra-build-observability` (idx 23)
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
response.extend_from_slice(&read_buf[..n]);
            response_buffer.extend_from_slice(&read_buf[..n]);
```

**Impact:** route_and_forward accumulates the entire result set into a Vec (line 1256: `let mut response = Vec::new();`) before a single byte is written to the client (write_all at line 861). Every response byte is copied from the kernel into read_buf, then into `response`, then again into `response_buffer` for framing. Memory per in-flight query is unbounded (a multi-GB SELECT is fully buffered in proxy RAM), and time-to-first-row equals time-to-last-row, destroying streaming latency for large results.

**Fix:** Stream frames to the client as soon as they are complete (write each decoded frame, or write raw chunks and keep only the unframed tail to detect ReadyForQuery). Eliminates two of three copies, caps per-query memory at one read buffer, and gives clients first rows immediately.

**Verifier correction/nuance:** Finding is accurate but slightly understates the cost: line 1275 additionally deep-copies the framing buffer (`response_buffer.clone()`, BytesMut deep copy) once per decoded backend message just to peek for ReadyForQuery — a fourth copy, though bounded (~one 8KB read chunk) since the real buffer is drained at line 1285. Also read_buf is a freshly zeroed vec![0u8; 8192] allocated every loop iteration (line 1261).

<details><summary>Verifier reasoning</summary>

Verified in src/server.rs: line 1256 allocates `let mut response = Vec::new()`; lines 1271-1272 contain the quoted double extend_from_slice; route_and_forward returns only after ReadyForQuery (line 1283) or EOF, and the caller writes the whole buffered response to the client in one write_all at lines 861-864. So every response byte is copied kernel->read_buf->response->response_buffer, memory per in-flight query is unbounded, and the client receives nothing until the last backend byte arrives — exactly as claimed. This is the sole production hot path: accept loop (line 511) -> handle_client (667) -> client_loop (729) -> route_and_forward (837) per client message; grep of all of src/ shows no copy_bidirectional or alternate streaming handler. The suggestion is valid and not already implemented: the only consumers of the buffered bytes are the client write (861) and the post-query plugin hook, which uses only resp.len() (lines 1944-1948), so streaming frames while keeping the unframed tail for ReadyForQuery detection (lines 1276-1282 already work per-frame) breaks nothing; error-path atomicity is unchanged because the caller kills the connection on error anyway (line 854). Fixing it would improve time-to-first-row for large result sets, cut 2 of 3 copies plus a per-iteration zeroed 8KB allocation (line 1261), and cap per-query proxy memory, so it plausibly improves production latency, throughput, and scalability.

</details>

