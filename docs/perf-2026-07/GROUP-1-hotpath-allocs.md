# Group 1 — Per-query allocation & copy elimination (hot path)

**Goal:** cut user-side CPU and allocator churn per forwarded query without
changing any wire behaviour. Everything here is protocol-equivalent
byte-for-byte; only buffer ownership and copy counts change.

**Evidence:** 48.9 µs CPU/query at c=64 (25% user / 75% sys);
`stream_until_ready` allocates a fresh `BytesMut(16 KiB)` + zeroed
`vec![0u8; 16 KiB]` **per query** (server.rs:3493-3494) ≈ 1 GB/s allocator
churn at 64k qps; every forwarded message pays `Message::encode()` — a fresh
`BytesMut` + full payload copy (protocol.rs:185-198) — even when forwarded
verbatim; the client-read path copies `read_buf → buffer`
(server.rs:1625-1635) before decoding.

## Changes

### 1.1 Hoist relay scratch buffers to session scope
- `client_loop` owns one `RelayScratch { buf: BytesMut, read_buf: Vec<u8> }`
  (or two locals) created once per session, passed as `&mut` into
  `stream_until_ready` / `stream_until_ready_capture`.
- Buffers are `.clear()`ed at entry; capacity is retained across queries.
  Cap retained capacity (shrink if > 256 KiB after a huge result) so one
  monster row doesn't pin memory for the session's lifetime.

### 1.2 Zero-copy frame forwarding (kill `encode()` on the data path)
- `ProtocolCodec::decode_frame(&mut BytesMut) -> Result<Option<Frame>>` where
  `Frame { msg_type: MessageType, raw: BytesMut }` and `raw` is
  `src.split_to(5 + payload_len)` — O(1) split, no copy; payload accessor
  `fn payload(&self) -> &[u8] { &self.raw[5..] }`.
- `client_loop` switches from `decode_message` to `decode_frame`:
  - simple Query: forward `frame.raw` directly (today: `msg.encode()` alloc+copy);
  - extended messages: `pending.extend_from_slice(&frame.raw)` (one copy into
    the batch buffer, no intermediate encode alloc);
  - COPY / passthrough arms: write `frame.raw` directly.
- All payload readers on the path (`query_text`, `parse_stmt_name`,
  `bind_stmt_ref`, `stmt_kind_name`, `is_write_message`, anomaly/mirror
  offer, stmt_registry insert) take `frame.payload()`.
- `stmt_registry` stores `frame.raw.clone().freeze()` (the registry needs an
  owned copy anyway — unchanged semantics, one copy as today).
- `Message`/`decode_message` stay for control paths (auth relays, backend
  client, admin) — no churn outside the hot loop.

### 1.3 Read directly into the accumulation buffer
- Replace `stream.read(&mut read_buf)` + `buffer.extend_from_slice` with
  `buffer.reserve(16384)` + `stream.read_buf(&mut buffer)` in `client_loop`
  (and the same pattern for the backend side inside `stream_until_ready*`).
  Removes one full copy of every inbound byte in each direction and the
  per-session 16 KiB read scratch entirely.

### 1.4 Transaction-state flag → atomic
- `ClientSession.in_transaction: AtomicBool` (set in `stream_until_ready*`
  from the RFQ status byte; read in `release_to_pool_if_idle`,
  `select_read_node`, `cacheable_read_ctx`).
- The `RwLock<TransactionState>` stays for the rich TR/journal state, but the
  per-query write lock acquisition disappears from the relay.

### 1.5 ArcSwap guard loads on per-query reads
- `choose_target_node` / `select_read_node` / cutover check: use `.load()`
  (Guard) instead of `.load_full()` (Arc clone → 2 refcount RMWs) for
  short-lived reads. `load_full` stays where the value outlives the guard
  scope legitimately.

## Explicitly out of scope
Response-side re-chunking, io_uring, vectored writes, worker pinning —
syscall floor is addressed separately if at all; this group is pure
user-space waste removal.

## Risk & tests
- Risk: LOW-MED. Mechanical, but touches the frame pump — the regression
  battery exercises simple/extended/prepared/COPY/large-stream on both
  backends, which is exactly the blast radius.
- New unit tests: `decode_frame` framing (partial header, partial body,
  boundary at Z/G/W, `raw` equals original bytes), atomic tx flag transitions.
- Gate: full milestone protocol (README) + `unnamed-parse-test.sh`,
  `prepared-stmt-test.sh`, `copy-test.sh`, `cancel-test.sh` targeted runs.

## Expected outcome
User CPU/query down 30-50%; allocator churn per query → ~0 steady-state;
session-mode tps at c=16/64 up double-digit %; zero wire-behaviour change.
