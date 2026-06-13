# BATCH A — Hot-path performance quick wins (no architecture changes)

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** Remove verified per-query waste from the relay loop and build configuration. Every fix here is local, low-risk, and independently revertable. Expected combined effect: significant p50/p99 latency reduction and CPU/query reduction.

**Parallel-execution compatibility:** SOLO-friendly. Items A1–A7 are independent of each other and can even be split across agents (different functions), but they are small enough for one agent in one pass.

**Files touched:** `src/server.rs` (relay loop, hooks, classification), `Cargo.toml` (profile), `src/pool/mode.rs` (keyword detection), `src/admin.rs` (optional)

**Conflicts with:** BATCH B and BATCH C also edit `src/server.rs::route_and_forward` — execute A before B/C (A is the baseline; B/C rebase on it). A does NOT conflict with D (plugins runtime internals), E, F, G, H.

**Acceptance criteria:**

- `cargo build --release --all-features` clean; `cargo test` green.
- pgbench (simple protocol) through the proxy shows improved tps / p99 vs baseline binary built from pre-batch HEAD.
- A large `SELECT` (e.g. 500k rows) through the proxy completes with linear CPU (no quadratic blow-up) — compare `time psql -c` before/after.
- With `plugins.enabled=true` and an empty `plugin_dir`, per-query profile shows no payload clone / UUID / parse attributable to hook stages.

---

## Work items (ordered, each independently committable)

### A1. Set `TCP_NODELAY` on every data-path socket
Add `stream.set_nodelay(true).ok();` (or log on error) at:
- the accept loop, right after `listener.accept()` returns (`src/server.rs:503-516`)
- every backend `TcpStream::connect` on the data path (`src/server.rs:997`, `src/server.rs:1216`)
- the admin SQL-console forward connection (`src/admin.rs:661` area), low priority.
Do NOT bother with the health-probe connect (`src/server.rs:2081`) — it sends nothing.

### A2. Add `[profile.release]` to Cargo.toml
```toml
[profile.release]
lto = "thin"
codegen-units = 1
strip = true
```
Deliberately NOT setting `panic = "abort"`: a panicking per-connection tokio task currently kills only that session; abort would kill the whole proxy. Revisit only with a panic-isolation strategy.
Optional follow-up (separate commit, feature-gated): jemalloc/mimalloc global allocator; measure before adopting.

### O(n^2) deep-copy + double-decode of backend response buffer per relayed message

- **Location:** `src/server.rs:1275`
- **Severity / category:** high / algorithmic
- **Found by:** `hot-path-protocol` auditor; independently confirmed by `infra-build-observability` (idx 21)
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
while let Some(resp_msg) = codec.decode_message(&mut response_buffer.clone())? {
```

**Impact:** BytesMut::clone is a deep copy of all bytes still in response_buffer, performed once per decoded message just to peek at the type, after which decode_message is run a second time on the real buffer (line 1285) to advance it. For a response of M frames the copying is O(M * remaining) — quadratic in response size — and every frame is parsed twice. Runs on every single query response. The identical clone-then-redecode pattern is in proxy_authentication (line 1078) and complete_backend_auth (line 1482).

**Fix:** Decode each message exactly once from the real buffer and act on the returned Message (decode_message already consumes the frame). If only the tag is needed, peek tag+length bytes without splitting at all. Removes one full-buffer memcpy and one parse per frame; turns quadratic relay cost into linear.

**Verifier correction/nuance:** The "O(M * remaining) — quadratic in response size" characterization is overstated for the common case: the inner loop drains complete frames after each 8KB socket read (read_buf at server.rs:1261), so the cloned remainder is bounded by ~8KB plus any partial frame. For multi-frame responses streamed across reads the cost is linear in total response size with a large constant (~40x memcpy amplification for small DataRows plus one alloc/free per frame, and one extra full clone per read for the final None check). True quadratic behavior occurs only when a single frame exceeds 8KB (each read clones the entire growing buffer before decode returns None: O(F^2/8KB) bytes copied for a frame of size F) or when many frames accumulate before draining. Also, "every frame is parsed twice" is literally true but parsing is cheap (O(1) split_to); the dominant cost is the clone's alloc+memcpy, not the second decode. Sites 1078 and 1482 are per-connection/per-backend-switch, not per-query.

<details><summary>Verifier reasoning</summary>

Verified in /home/gpc/HDB/Proxy/src/server.rs: line 1275 clones the BytesMut response buffer (deep copy: alloc + memcpy of all remaining bytes) on every iteration of the frame-drain loop just to peek at the message type, then line 1285 re-decodes the same frame from the real buffer. protocol.rs:279-305 confirms decode_message is safe to call directly on the real buffer (returns None without consuming on partial frames), so the clone is pure waste; server.rs:773 already uses the single-decode pattern. The containing function route_and_forward is the per-query relay path, called from the main client loop at server.rs:837 for every client message (confirmed by backend/mod.rs docs stating this is the live forwarding path). Sites 1078 (proxy_authentication, per-connection startup) and 1482 (complete_backend_auth, only on backend switch) exhibit the identical pattern but are colder. Fixing it removes one full-buffer memcpy+alloc and one redundant parse per relayed frame on every query response, plus eliminates genuinely quadratic O(F^2/8KB) copying for any frame larger than the 8KB read size — a plausible production latency/throughput win, especially for row-heavy or large-value responses.

</details>


### Fresh 8KB zeroed Vec allocated per read syscall on both client and backend loops

- **Location:** `src/server.rs:758`
- **Severity / category:** medium / allocation
- **Found by:** `hot-path-protocol` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let mut read_buf = vec![0u8; 8192];
```

**Impact:** The client loop allocates and zeroes a new 8KB Vec on every read iteration, then copies the bytes into the persistent BytesMut (line 769). The same pattern repeats inside the backend response loop (line 1261, per chunk of every response), proxy_authentication (line 1059), complete_backend_auth (line 1463), and backend/client.rs read_one (line 469, 4KB). That is one heap alloc + zeroing + an extra memcpy per read syscall on the hottest loops in the proxy.

**Fix:** Read directly into the accumulation buffer with AsyncReadExt::read_buf(&mut buffer) (BytesMut implements BufMut; reserve() amortizes growth), or hoist a single reusable read buffer outside each loop. Removes one allocation and one copy per read syscall.

**Verifier correction/nuance:** Finding is accurate; two refinements. (1) It understates line 1261: each backend chunk is copied into two buffers (response at 1271, response_buffer at 1272) and line 1275 additionally does response_buffer.clone() per decode pass, making the decode loop O(n^2) per large response — a bigger win than the read_buf change at that site. (2) complete_backend_auth (line 1463) runs on every routing-induced backend switch via route_and_forward line 1234, not only during failover, so it is warmer than a typical auth path. One uncited instance also exists at server.rs:889 (handle_startup, 1KB).

<details><summary>Verifier reasoning</summary>

Verified every cited location in the code. src/server.rs:758 is inside client_loop's main query loop (loop at line 756): a fresh `vec![0u8; 8192]` is allocated per read iteration, `stream.read` fills it, and line 769 copies it into the persistent BytesMut — exactly as claimed, one heap alloc + zeroing + extra memcpy per client read syscall. src/server.rs:1261 is inside route_and_forward's per-query backend response loop (line 1260); same pattern per response chunk, and the finding actually understates it: each chunk is copied twice (lines 1271 and 1272 into `response` and `response_buffer`), and line 1275 clones the whole response_buffer per decode pass. src/server.rs:1059 (proxy_authentication, per-connection) and 1463 (complete_backend_auth, called from route_and_forward line 1234 on every routing-induced backend switch, not just failover) and src/backend/client.rs:469 (read_one, 4KB) all match the claim. Lines 758 and 1261 are the two innermost data-plane loops of the proxy — per read syscall and per response chunk for every query, hot path at scale. The suggestion is valid: tokio is built with `full` so AsyncReadExt::read_buf into BytesMut (implements BufMut) works; grep confirms no call site uses read_buf today. No correctness risk — no locks or freshness invariants involved. Minor caveat: proxy_authentication forwards the raw chunk to the client (line 1073) and route_and_forward accumulates into two buffers, so direct read_buf there needs the newly-appended range tracked via pre-read buffer length, or a hoisted reusable buffer as the drop-in variant. Impact is real but modest per iteration (alloc+memset+memcpy of 8KB is ~hundreds of ns vs ~µs per syscall), yet it multiplies across every read on every connection and adds allocator pressure under concurrency, so fixing it plausibly improves throughput/CPU at scale; medium severity is fair.

</details>


### TCP_NODELAY never set on client or backend data-path sockets

- **Location:** `src/server.rs:1216`
- **Severity / category:** medium / io
- **Found by:** `hot-path-protocol` auditor; independently confirmed by `infra-build-observability` (idx 22)
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
TcpStream::connect(&target_node),
```

**Impact:** grep shows set_nodelay only in src/distribcache/invalidator.rs; the accepted client socket (line 505), the startup backend connect (line 997), and the route_and_forward backend connect (line 1216), as well as backend/client.rs:84, all leave Nagle enabled. Multi-write turns (send_block_response writes error then ReadyForQuery as two small write_alls, lines 1771-1778; the auth relay does many small writes) can stall up to 40ms on Nagle/delayed-ACK interaction, and any future pipelining/streaming relay would be throttled on every small frame.

**Fix:** Call stream.set_nodelay(true) on every accepted client socket and every dialed backend socket (and in BackendClient::connect_inner). One-line change per site; removes worst-case 40ms tail latencies on small-message turns.

**Verifier correction/nuance:** Minor overstatement of breadth: the steady-state per-query path is strict ping-pong with one coalesced write_all per direction per turn (route_and_forward buffers the entire backend response before a single client write at server.rs:862; the query goes to the backend in a single write at server.rs:1251), so Nagle rarely delays the common case since there is usually no unacked data outstanding when the single small write is issued. The up-to-40ms stalls are concentrated in multi-write turns (send_block_response on the plugin-block path, multi-chunk auth relay during connection establishment), in the final sub-MSS runt of responses sized just over n*MSS (tail latency), and in any future pipelining/streaming relay. It is a real medium-severity tail-latency fix, not a uniform per-query 40ms penalty.

<details><summary>Verifier reasoning</summary>

Every factual claim checks out. Repo-wide grep shows set_nodelay only in src/distribcache/invalidator.rs:113,187. Verified no socket2/TcpSocket builder or other socket-option path exists. The accepted client socket (src/server.rs:505), the startup backend connect (src/server.rs:995-998), the route_and_forward backend connect (src/server.rs:1214-1217, evidence excerpt matches line 1216 verbatim), and BackendClient::connect_inner (src/backend/client.rs:84) all leave Nagle enabled. These are data-path sockets: the client socket carries every response (write_all at server.rs:862) and the backend socket carries every forwarded query (write_all at server.rs:1251). send_block_response (server.rs:1762-1785) really does two consecutive small write_alls (error then ReadyForQuery), the classic write-write-read pattern that stalls ~40ms under Nagle + delayed ACK; the auth relay (proxy_authentication, server.rs:1057-1120) relays chunk-by-chunk and can issue multiple small client writes per connection setup. The suggestion is standard practice (PostgreSQL itself, pgbouncer, odyssey, pgcat all set TCP_NODELAY), trivially safe, and not implemented elsewhere. Fixing it plausibly removes 40ms tails on blocked-query turns, connection setup, and the trailing sub-MSS runt of responses, and is necessary before any streaming/pipelined relay.

</details>


### No [profile.release] section: missing LTO, codegen-units, panic=abort; default allocator

- **Location:** `Cargo.toml:1`
- **Severity / category:** high / build
- **Found by:** `infra-build-observability` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
[package]
name = "heliosdb-proxy"
```

**Impact:** The manifest ends at the [[test]] section (line 232) with no [profile.release] anywhere, so release builds use defaults: lto=false (not even thin), codegen-units=16, panic=unwind, no strip. For an ~86k LoC binary with hot cross-crate calls (bytes/tokio/codec) this typically leaves 5-15% throughput on the table, on every query. The allocation-heavy hot path (String per message, Vec per read) also runs on the default glibc allocator. tokio uses features=["full"] (line 119), inflating compile time though not runtime.

**Fix:** Add: [profile.release] lto = "thin" (or "fat"), codegen-units = 1, panic = "abort", strip = true. Add an optional jemalloc/mimalloc global allocator (feature-gated, e.g. tikv-jemallocator) — multithreaded allocation-heavy proxies commonly see 5-20% throughput gain. Trim tokio features to the ones used (rt-multi-thread, net, io-util, time, sync, signal, macros) to cut build time.

**Verifier correction/nuance:** Two parts of the suggestion need adjustment. (1) panic="abort" is an availability tradeoff, not a pure perf win: the proxy has 19 tokio::spawn sites and currently relies on tokio's task-boundary panic isolation, so a panic in one connection task kills only that session; with panic=abort a single panic aborts the whole proxy and drops every client connection. Recommend lto="thin" + codegen-units=1 + optional allocator, but treat panic=abort as a deliberate policy decision (or omit it). (2) strip=true affects binary size only, not runtime performance. (3) The 5-15% throughput estimate for LTO/CGU alone is optimistic — bytes/tokio hot generics are monomorphized into the consuming crate already, so realistic codegen gains are low single digits to ~10%; the allocator swap is the larger, better-supported win for this allocation-heavy multithreaded workload. (4) Trimming tokio "full" is compile-time only, as the finding itself concedes; the catch_unwind uses in src/anomaly/ewma.rs and src/edge/cache.rs are test-only and unaffected (cargo ignores panic settings for test targets).

<details><summary>Verifier reasoning</summary>

Verified in /home/gpc/HDB/Proxy/Cargo.toml: no [profile.release] section exists (manifest ends line 235, [[test]] at line 232 as the finding said), and tokio uses features=["full"] at line 119. Crucially, I confirmed no override path neutralizes this: the crate is not in a workspace (no [workspace] key, no parent manifest), there is no .cargo/config.toml, and no CARGO_PROFILE_RELEASE_*/RUSTFLAGS/lto settings appear in .github/workflows/, docker/, or scripts/. All production artifacts inherit stock release defaults: docker/Dockerfile (~line 25) and .github/workflows/release.yml:55 run plain `cargo build --release`, and crates.io `cargo install` uses this same manifest — so lto=false (no cross-crate LTO), codegen-units=16, panic=unwind, no strip, glibc allocator (no #[global_allocator] anywhere in src/). The path it affects is hot: per-message/per-row allocations confirmed at src/protocol.rs:344 (String::from_utf8 per cstring), 390-460 (Vecs per Parse/Bind), 656-660 (payload.to_vec() per CopyData), and src/backend/client.rs:361-448 (String::from_utf8_lossy(...).into_owned() per column name and per data value, Vec per row). For a multithreaded tokio proxy at ~91.6k LoC this is exactly the workload where thin LTO + codegen-units=1 and a jemalloc/mimalloc swap plausibly improve throughput/latency in production, so matters=true. The finding is real, with corrections to two suggestion items (see correction field).

</details>


### Same Query payload deep-cloned and re-parsed up to 5 times per query, plus full to_uppercase of SQL

- **Location:** `src/server.rs:1318`
- **Severity / category:** medium / allocation
- **Found by:** `hot-path-protocol` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let upper = sql.trim().to_uppercase();
```

**Impact:** Per simple-query message, `QueryMessage::parse(msg.payload.clone())` (a deep BytesMut copy plus a String allocation) is executed independently in record_anomaly_observation (line 1721), apply_pre_query_hook (line 1683), apply_route_hook (line 1893), is_write_message (line 1297), and fire_post_query_hook (line 1938). is_write_query then allocates an uppercased copy of the entire SQL just to prefix-match a dozen verbs, and build_query_context (line 1798) does two more to_string() copies. With wasm-plugins + anomaly-detection enabled that is ~7 full-query-length allocations/copies on every query.

**Fix:** Parse the Query payload once at the top of the per-message loop into a borrowed &str (or small context struct) and pass it to all hooks; replace to_uppercase with case-insensitive prefix checks (eq_ignore_ascii_case on the trimmed head). Saves several allocations and memcpys per query, more for large SQL texts.

**Verifier correction/nuance:** Two refinements: (a) the three plugin-hook parses require a plugin manager to actually be attached at runtime (state.plugin_manager.is_some(), checked before the parse at server.rs:1674, 1886, 1931), not just the wasm-plugins feature flag; the anomaly parse (line 1721) runs whenever the feature is compiled, with no runtime gate. (b) The "~7 allocations" figure is an undercount for the maximal config: 5 deep BytesMut clones + 5 String allocations + 4 full-SQL to_uppercase calls (is_write_message once, plus build_query_context at lines 1688/1897/1942 each re-running is_write_query) + 6 to_string copies (2 per build_query_context). Also, a parse-once refactor must re-derive the SQL after PreQueryResult::Rewrite (server.rs:1693-1696) replaces the message.

<details><summary>Verifier reasoning</summary>

Every specific claim verified in code. (1) Evidence exact: src/server.rs:1318 has `let upper = sql.trim().to_uppercase();` followed only by starts_with prefix checks (lines 1321-1351) — a full-query-length Unicode uppercase allocation to match a dozen ASCII verbs. (2) Message.payload is BytesMut (protocol.rs:165) whose clone() is a deep memcpy, and QueryMessage::parse (protocol.rs:362) allocates a String via read_cstring (protocol.rs:333-346). (3) All five independent parse+clone sites confirmed at server.rs:1297, 1683, 1721, 1893, 1938, and all are invoked from the per-message hot loop in handle_session (lines 785, 789, 837->1133/1137, 847). (4) build_query_context (server.rs:1793-1804) additionally does two query.to_string() copies AND re-runs is_write_query (another to_uppercase) per hook — so with plugins attached the full uppercase runs 4x per query and total full-query-length allocations exceed the finding's ~7 estimate (closer to 15-20 in the maximal config); the finding is conservative, not inflated. (5) Suggestion is valid and not already implemented: parse once before the pre-query hook, refresh on PreQueryResult::Rewrite (lines 1693-1696, new SQL already available there), and use eq_ignore_ascii_case prefix checks — SQL verbs are ASCII so this is semantically equivalent, including the SET TRANSACTION READ ONLY exclusion. Minor caveats: the three plugin-hook parses are gated on plugin_manager.is_some() (early return before parse), not merely the compile feature; baseline build still pays 1 clone + 1 String + 1 to_uppercase per Query via is_write_message at line 1133. This is the proxy's per-query fast path, so eliminating several allocations and O(query-length) memcpys per query plausibly improves CPU per query and throughput at high QPS, especially for large SQL texts (batch INSERTs), even though per-query absolute savings are microsecond-scale next to a backend round-trip.

</details>


### Full-SQL to_uppercase() allocation per query in read/write routing and transaction-boundary detection

- **Location:** `src/server.rs:1318`
- **Severity / category:** high / allocation
- **Found by:** `pooling-balancing` auditor; independently confirmed by `hot-path-protocol` (idx 4 overlaps)
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
fn is_write_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
```

**Impact:** is_write_query runs for every Query and Parse message on the live data path (called from is_write_message in route_and_forward) and allocates an uppercased copy of the entire SQL text — for a multi-megabyte bulk INSERT that is a multi-megabyte allocation + copy per query just to inspect the first keyword. The identical pattern exists in the pooling subsystem at src/pool/mode.rs:161 (`let upper = sql.trim().to_uppercase();` in TransactionEvent::detect, invoked per statement by ConnectionLease::on_statement_complete), which additionally does whole-string `upper.contains("TRANSACTION")`/`contains(" TO ")` scans. is_write_message also clones the message payload (`QueryMessage::parse(msg.payload.clone())`, line 1297).

**Fix:** Match only the first token case-insensitively: slice the first word after trim and use eq_ignore_ascii_case (or a small match on the first 1-2 keywords), no allocation; for ROLLBACK TO / START TRANSACTION inspect only the next token. Expected benefit: removes an O(query-length) allocation+copy from every query in both the router and the lease state machine.

**Verifier correction/nuance:** The secondary claim about the pooling subsystem is technically accurate but overstated as live cost: src/pool/mode.rs:161 does have the identical to_uppercase pattern plus whole-string contains() scans (lines 170, 180), and ConnectionLease::on_statement_complete (src/pool/lease.rs:123) calls TransactionEvent::detect — but that lease state machine is not wired into the live data path. server.rs only calls pool_manager.{has_active_lease, evict_idle, close_all, get_stats, metrics, default_mode, add_node}; neither acquire nor on_statement_complete is ever called from the server's message loop, so the pool/mode.rs instance is currently exercised only by unit tests and unwired handler scaffolding. It is worth fixing for when pool-modes leasing is integrated, but today only the server.rs:1318 routing-path instance carries production cost.

<details><summary>Verifier reasoning</summary>

Verified against the code. (1) Evidence is verbatim: src/server.rs:1317-1318 has `let upper = sql.trim().to_uppercase()` (Unicode-aware, full-string allocation) followed by ~15 starts_with checks on the leading keyword only (lines 1321-1351). (2) Hot path confirmed: is_write_message (line 1293) is the first call in route_and_forward (line 1133), which is invoked per decoded client message in the main query loop (server.rs line 837, inside `while let Some(msg) = codec.decode_message` in client_loop around line 774). For every Query/Parse message the code performs three O(query-length) copies just to classify the verb: BytesMut deep-copy via `msg.payload.clone()` (lines 1297/1305 — Message.payload is BytesMut, protocol.rs:165, whose Clone deep-copies), the String built by read_cstring in QueryMessage::parse (protocol.rs:363), and the to_uppercase allocation. is_write_query is also re-invoked at line 1794 (wasm-plugins build_query_context). For multi-MB bulk INSERTs this is multiple multi-MB allocations+copies per statement on the data path; for short OLTP queries it is a smaller but per-query cost in a latency-sensitive proxy. (3) Suggestion is valid and safe: first-token eq_ignore_ascii_case (with 1-2 token lookahead for SET TRANSACTION READ ONLY / ROLLBACK TO) preserves semantics — keywords are ASCII and the current starts_with has no word boundary anyway; nothing equivalent is already implemented. So the primary claim is real and fixing it plausibly improves production latency/throughput.

</details>


### Plugin hook stages are not zero-cost when zero plugins are loaded: per-query payload clone, SQL parse, double SQL String copy, and UUIDv4 generation happen before the empty-registry check

- **Location:** `src/server.rs:1683`
- **Severity / category:** high / allocation
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return (msg, PreQueryAction::Forward),
            };

            let ctx = Self::build_query_context(&query_msg.query, session);
```

**Impact:** When plugins are enabled in proxy.toml (PluginRuntimeConfig default enabled: true) the manager exists even with an empty plugin_dir, and apply_pre_query_hook, apply_route_hook, and fire_post_query_hook each run for every Query message. Each one clones msg.payload, parses the SQL, and calls build_query_context (server.rs:1798: `query: query.to_string(), normalized: query.to_string()`) plus HookContext::default() which does `uuid::Uuid::new_v4().to_string()` (mod.rs:209) and allocates a HashMap — only for execute_* to find an empty hook list and return. Net: per query with zero plugins loaded ≈ 3 payload clones, 3 parses, 6 full SQL String copies, 3 UUID generations (getrandom) and 3 String/HashMap allocations. The doc comment at server.rs:1660 claims this path is a 'zero-cost passthrough' when 'the plugin manager has no loaded plugins' — the code does not implement that.

**Fix:** Add a cheap PluginManager::has_hooks(HookType) (atomic bitmask or per-hook AtomicUsize counter updated on load/unload) and early-return in all three apply_*/fire_* helpers before any parsing or context construction. Replace the eager UUID with a lazily-generated request id (or a per-session counter). Benefit: restores the documented zero-cost no-plugin path; removes ~9 allocations + 3 RNG calls per query in plugin-enabled deployments.

**Verifier correction/nuance:** Three quibbles: (a) the parenthetical 'PluginRuntimeConfig default enabled: true' is misleading — plugins/config.rs:62 does default enabled:true, but the manager is created from the TOML PluginToml whose default is enabled=false (config.rs ~705-711 test asserts !config.plugins.enabled), so the issue is opt-in, not on-by-default; (b) HashMap::new() in HookContext::default does not allocate, so '3 HashMap allocations' is wrong — though total allocations are if anything undercounted (parse String and session.id.to_string per hook are omitted); (c) only simple-query (MessageType::Query) messages pay the cost — extended-protocol Parse/Bind/Execute are skipped early (server.rs:1679, 1890, 1935). Severity 'high' is arguably 'medium' given it requires the wasm-plugins compile feature plus an explicit [plugins].enabled=true.

<details><summary>Verifier reasoning</summary>

Verified in code. (1) Evidence exists verbatim at src/server.rs:1683-1688; the doc comment at 1658-1662 promises zero-cost passthrough when 'the plugin manager has no loaded plugins', but the code only checks plugin_manager.is_some() (1674-1677), never the loaded count. (2) Hot path confirmed: apply_pre_query_hook (server.rs:789), apply_route_hook (server.rs:837→1137), and fire_post_query_hook (server.rs:847) run per client message inside the decode loop; for every simple-query message each does msg.payload.clone() (payload is BytesMut, protocol.rs:165 — full copy), QueryMessage::parse (protocol.rs:362-365, allocates the SQL String), and build_query_context (server.rs:1793-1804: query.to_string() twice, session.id.to_string(), HookContext::default() with uuid::Uuid::new_v4().to_string() at plugins/mod.rs:209; Cargo.toml:141 confirms uuid v4 feature). (3) init_plugin_manager (server.rs:325-381) returns Some(pm) when [plugins].enabled=true even if plugin_dir is missing/empty, so the enabled-but-zero-plugins scenario is real. (4) execute_pre_query/execute_post_query/execute_route (plugins/mod.rs:462, 511, 606) do RwLock read + empty-vec clone + zero-iteration loop — all expensive work happened in the caller for nothing. (5) Suggestion is valid and not implemented anywhere (no has_hooks/hook_count in src/plugins/); hooks mutate only on load/unload so an atomic counter early-exit is sound. Matters=true: per-query overhead (3 payload copies, 3 SQL parses, ~12-15 allocations, 3 getrandom-backed UUIDs) in a microsecond-budget proxy plausibly improves CPU/throughput for plugin-enabled deployments at scale, and the fix also helps deployments WITH plugins for stages that have no registered hooks.

</details>


### Per-query observability tax: SQL parsed/cloned 3x, eager RFC3339 timestamp, full-string case conversions

- **Location:** `src/server.rs:1743`
- **Severity / category:** medium / allocation
- **Found by:** `infra-build-observability` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let fingerprint = anomaly_fingerprint(&query_msg.query);
        let now = std::time::Instant::now();
        let iso = chrono::Utc::now().to_rfc3339();
```

**Impact:** For every Query message the hot loop does: record_anomaly_observation -> QueryMessage::parse(msg.payload.clone()) (line 1721), apply_pre_query_hook -> parse(msg.payload.clone()) again (line 1683), and is_write_message -> parse(msg.payload.clone()) a third time (line 1297). Each clone copies the full SQL payload. chrono::Utc::now().to_rfc3339() heap-formats an ISO string per query even when zero anomalies fire (only used in event payloads). is_write_query does sql.trim().to_uppercase() (line 1318) and sql_injection::scan does sql.to_lowercase() (sql_injection.rs:23) — two more full-SQL-length allocations per query. ~6 avoidable allocations + 3 redundant parses per query.

**Fix:** Parse the Query payload once per message in client_loop and pass &str to the anomaly hook, plugin hook, and is_write_message. Build the ISO timestamp lazily inside the (rare) event-emission branches. Replace to_uppercase/to_lowercase prefix checks with case-insensitive byte comparison (eq_ignore_ascii_case on the first token) and use memchr-style case-folded substring search in the SQLi scanner. Removes all per-query transient allocations from the observability path.

**Verifier correction/nuance:** Two refinements: (1) The "3 redundant parses" only fully materializes when the anomaly-detection and wasm-plugins features are compiled AND a plugin manager is configured; with default features (pool-modes only, Cargo.toml:32) the per-query cost is just one parse+clone (is_write_message, server.rs:1297) plus one to_uppercase. Official release builds use all-features so the finding holds for shipped binaries. (2) The single-parse suggestion needs a small correctness caveat: the pre-query plugin hook can rewrite the SQL (server.rs:1693-1696), so is_write_message/route decisions must re-derive from the post-rewrite query on the Rewrite path. Also, when plugins are configured there is a 4th parse (apply_route_hook, server.rs:1893) the finding missed, and record_query additionally takes a global parking_lot write lock per query (anomaly/mod.rs:165) — arguably a larger scalability issue than the allocations.

<details><summary>Verifier reasoning</summary>

Every specific claim checks out at the cited lines. server.rs:1741-1743 matches the evidence verbatim, and the iso_timestamp is consumed only inside detection branches (anomaly/mod.rs:182,198,219), so the eager rfc3339 String is wasted on the no-anomaly common path. The three redundant parses exist exactly as cited: server.rs:1721 (record_anomaly_observation), 1683 (apply_pre_query_hook), 1297 (is_write_message via route_and_forward:1133), each deep-copying the BytesMut payload (protocol.rs:165) and allocating a String (protocol.rs:362). is_write_query's full-SQL trim().to_uppercase() is at server.rs:1318 and sql_injection::scan's to_lowercase() at sql_injection.rs:23, run per Query with no runtime off-switch (AnomalyDetector built unconditionally at server.rs:463; AnomalyConfig has no enabled flag). This is the per-message hot loop (server.rs:773-789), not a cold path. The finding even undercounts: apply_route_hook (server.rs:1893) is a 4th parse+clone and build_query_context adds two query.to_string() copies plus another to_uppercase per hook invocation when plugins are configured. matters=true because release binaries ship with --features all-features (release.yml:55, includes anomaly-detection and wasm-plugins), making this ~6-10 SQL-length allocations plus redundant parsing per query of proxy CPU; eliminating it plausibly improves proxy throughput and added latency at high QPS, though gains are modest relative to backend RTT (hence medium severity is fair, not high).

</details>


### Global round-robin counter behind an async RwLock write taken on the read-routing path

- **Location:** `src/server.rs:1436`
- **Severity / category:** medium / locking
- **Found by:** `hot-path-protocol` auditor
- **Adversarial verification:** isReal=True matters=False confidence=high

**Evidence (verbatim from code at audit time):**

```
let mut lb_state = state.lb_state.write().await;
            let index = lb_state.rr_counter as usize % healthy_standbys.len();
```

**Impact:** select_read_node serializes all sessions through a single tokio::sync::RwLock write just to bump a u64. The per-query path also takes state.health.read().await once or twice per message (lines 1171/1178 plus inside select_* helpers) and session.tx_state.write().await per response (line 1280). Each is an async lock acquisition with waker bookkeeping on every query; lb_state is a genuine cross-session serialization point under load.

**Fix:** Replace rr_counter with an AtomicU64 fetch_add (no lock); publish node health as an ArcSwap'd immutable snapshot (or per-node AtomicBool) so the per-query path is lock-free reads. Removes all cross-session lock contention from routing.

**Verifier correction/nuance:** The code excerpt is accurate, but the lock is on the backend-switch path, not the per-query read path. Sticky routing (src/server.rs:1177-1192) keeps reads on the session's current healthy node, so select_read_node (and the lb_state write lock at 1436) runs only when a session has no node or its node went unhealthy — at most ~once per session and during failover — and each call is followed by a TCP connect + full backend auth handshake (1209-1246) that dominates cost. The per-message health lock (1171/1178) is a read lock with no reader-reader serialization, and tx_state (1280) is per-session with no cross-session contention. A side observation: because of the same sticky logic, sessions that start on the primary never distribute reads to standbys at all — arguably a load-distribution bug that matters more than the lock.

<details><summary>Verifier reasoning</summary>

The evidence is real: src/server.rs:1436-1438 takes a tokio::sync::RwLock write on global state.lb_state solely to bump a u64 round-robin counter (lb_state declared line 145, rr_counter line 212, tokio RwLock import line 23), and the guard is even held across the session.current_node.write().await await point at line 1441. However, the impact claim is wrong: select_read_node is NOT on the per-query read-routing path. route_and_forward (the per-message loop body, called from line 837) only invokes select_read_node when need_switch is true (lines 1197-1204), and the sticky-session logic at lines 1177-1192 sets need_switch=false for any read whose current node is healthy — reads never rebalance off their current node (even off the primary). Every session enters the loop with a backend already selected at startup (client_loop lines 739-753; connect_and_authenticate line 992 -> select_node line 1999 prefers primary). So select_read_node fires roughly once per session at most, and in practice mainly on failover when the current node is unhealthy — and every invocation is immediately followed by a fresh TcpStream::connect plus a full PostgreSQL auth handshake (lines 1209-1246), milliseconds of work that dwarfs a sub-microsecond lock by 4-5 orders of magnitude. The bundled per-message claims also overreach: state.health.read().await at 1171/1178 is per-message but is a READ lock — tokio readers run concurrently and only contend with the periodic health checker, so 'serializes all sessions' does not apply; session.tx_state.write().await at 1280 is a per-session lock touched only by that session's own task, with zero cross-session contention. The same per-message path performs far heavier work that buries lock costs: msg.payload.clone() (1297/1305), full-SQL to_uppercase() (1318), response_buffer.clone() per decoded message per read chunk (1275), and fresh 8KB Vec allocations per read (758, 1261). The suggestion (AtomicU64 fetch_add; ArcSwap'd health snapshot) is technically valid and would not break correctness — rr_counter guards no invariant beyond itself — but fixing it would not plausibly improve production latency, throughput, or scalability given the lock sits on the backend-switch/failover path, not the per-query path.

</details>


### Anomaly detector: two global exclusive locks per query and unbounded fingerprint/rate-window maps

- **Location:** `src/anomaly/mod.rs:192`
- **Severity / category:** medium / locking
- **Found by:** `infra-build-observability` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let mut seen = self.seen_fingerprints.write();
            if !seen.contains_key(&ctx.fingerprint) {
```

**Impact:** record_query (called from the per-message loop for every Query) takes rate_windows.write() (line 165) and seen_fingerprints.write() (line 192) — both global parking_lot exclusive locks — serializing all client connections through the detector under concurrency. The write lock on seen_fingerprints is taken even in the steady-state case where the fingerprint is already known. Both seen_fingerprints (HashMap<String, ()>) and rate_windows (keyed by tenant or client IP, server.rs:1738 falls back to client IP) grow without any eviction — unbounded memory under diverse query shapes or many client IPs.

**Fix:** Use DashMap (already a dependency) or read-lock fast path + write-lock upgrade only on novel fingerprints; shard rate windows. Cap seen_fingerprints with an LRU or probabilistic set (e.g. a sized bloom/cuckoo filter) and expire idle rate/auth windows. Eliminates a cross-connection serialization point and the leak.

**Verifier correction/nuance:** Finding is accurate with three refinements: (a) the `anomaly-detection` feature is NOT in the default feature set (Cargo.toml: default = ["pool-modes"]), so only builds compiled with it (or all-features) are affected — but within such builds there is no runtime off-switch for record_query; the detector is constructed with hardcoded `AnomalyConfig::default()` and invoked for every Query message. (b) The rate_windows lock is heavier than implied: it is held across an O(window_secs)=O(60) variance computation with a per-call Vec allocation (ewma.rs:86-105), not just a map insert; that lock, not seen_fingerprints, is the dominant serialization cost. (c) auth_windows (record_auth) also leaks map entries for (user, ip) pairs that never authenticate successfully — only its inner VecDeque is time-evicted — so the suggested idle-window expiry should cover it too.

<details><summary>Verifier reasoning</summary>

Every element of the finding checks out in the code. (1) Evidence exists verbatim: src/anomaly/mod.rs:192-194 takes `self.seen_fingerprints.write()` BEFORE the `contains_key` check, so the exclusive lock is acquired even in the steady state where the fingerprint is already known; `emit_novel_queries` defaults to true (mod.rs:130) and the server constructs the detector with `AnomalyConfig::default()` unconditionally (server.rs:462-466) — grep of src/config.rs shows no proxy.toml wiring to disable it at runtime. (2) `rate_windows.write()` at mod.rs:165 is also confirmed, and it is worse than the finding states: the global write lock is held across `observe_and_score` (src/anomaly/ewma.rs:82-118), which does an O(window_secs)=O(60) scan plus a Vec allocation per query. (3) Hot path confirmed: server.rs:784-785 calls `record_anomaly_observation` inside the inner per-message decode loop of the main query loop for every Query message, and the AnomalyDetector is a single Arc in ServerState (server.rs:163) shared by all client connections — so both locks are genuine cross-connection serialization points. (4) Unbounded growth confirmed: no eviction/retain/clear anywhere in mod.rs for seen_fingerprints or rate_windows (only auth_windows entries are removed, and only on successful auth). The tenant key falls back to client IP exactly as claimed (server.rs:1733-1740, `unwrap_or_else(|| session.client_addr.ip().to_string())`, plus the try_read-contention branch), so rate_windows grows with distinct client IPs (~480 bytes/window per ewma.rs doc plus map overhead), and seen_fingerprints stores full normalized-fingerprint Strings forever; the lightweight normalizer (server.rs:89-130) collapses literals but not shape diversity (e.g. variable-arity IN lists yield distinct fingerprints), so "diverse query shapes" growth is realistic. (5) The suggestion is valid and not already implemented: dashmap 5 is already a dependency; a read-lock fast path on seen_fingerprints is trivially correct, sharding rate windows preserves per-key semantics, and LRU/bloom capping only affects an informational (Severity::Info) detector's re-fire behavior. Caveats for the correction field: the impact is feature-gated and off by default, and parking_lot short critical sections mean the seen_fingerprints lock alone is a modest cost — the rate_windows lock holding the O(60) scoring loop is the bigger serialization term. matters=true because for builds shipping anomaly-detection (a marketed feature with its own demo, demos/v0.4.0/01-anomaly-detection) this is an always-on per-query global lock pair plus a slow memory leak; fixing it plausibly improves throughput under concurrency and removes unbounded memory growth.

</details>


## Notes for the executor
- A3 = findings "O(n^2) deep-copy + double-decode" — the tactical fix (decode once from `response_buffer`, no clone) is enough for this batch; full streaming is BATCH B.
- A4 = "Fresh 8KB zeroed Vec per read" — hoist buffers out of all four read loops (`src/server.rs:758, 889, 1059, 1261, 1463`).
- A5 = the two `to_uppercase`/payload-clone findings — introduce one cheap first-token classifier (`eq_ignore_ascii_case`) used by `is_write_query` (`src/server.rs:1318`) and `TransactionEvent::detect` (`src/pool/mode.rs:161`); thread one parsed `QueryMessage` through hook sites instead of re-parsing (target: parse once per message).
- A6 = plugin hooks zero-cost early-return: add `PluginManager::has_hooks(HookType)` (atomic counter maintained on load/unload) checked before any clone/parse/UUID in `apply_pre_query_hook` / `apply_route_hook` / `fire_post_query_hook` (`src/server.rs:1683` area).
- A7 (optional, verified matters=false but trivial): replace round-robin `RwLock` counter with `AtomicUsize::fetch_add` (`src/server.rs:1436`).
- A8 (optional, medium): anomaly detector lock split + map bounds (`src/anomaly/mod.rs:192`) — only if time permits; otherwise leave for BATCH E follow-up.
