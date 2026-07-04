# Group 2 — Pool-modes correctness + release off the critical path

**Goal:** fix the pooling release path's correctness holes (COPY hang, dirty
parks, identity leakage), then make Transaction/Statement pooling cost
~nothing on the client's critical path, so transaction mode stops being
*slower* than session mode.

## Delivery split

G2 ships in two gated milestones to keep the connection-lifecycle risk isolated:
- **M2 (this milestone) — correctness only:** 2.0.a COPY-hang, 2.0.b
  poisoned-park, 2.0.c pool-key startup params. Reset stays **synchronous**
  (unchanged critical-path cost). Self-contained, high-confidence.
- **M2b (follow-up) — async release perf:** 2.1/2.2 spawn the reset off the
  client path with a bounded semaphore. Gated separately.

## Correctness findings (agent-verified, fix FIRST)

### 2.0.a COPY FROM STDIN hangs under transaction/statement mode (HIGH)
`stream_until_ready` yields on CopyInResponse (`G`/`W`) *without* updating
`tx_state` (server.rs:3519-3547); the main loop then unconditionally calls
`release_to_pool_if_idle` (server.rs:1742-1750), which sees a stale
`in_transaction=false`, removes the conn from the session cache, and sends
`DISCARD ALL` into a backend that is mid-COPY (protocol violation, aborts
the COPY). The client's subsequent CopyData/CopyDone frames find no conn
(server.rs:1954-1985) and are **silently dropped** → client hang.
**Fix:** a session `copy_in_progress` flag set on the CopyIn yield and
cleared after the CopyDone/CopyFail drain; release is skipped while set;
missing-conn COPY frames get an ErrorResponse instead of silence.

### 2.0.b `reset_backend` parks poisoned connections (MED)
The reset drain returns Ok on the first ReadyForQuery regardless of a
preceding ErrorResponse or the RFQ status byte (server.rs:2811-2815) — a
failed `DISCARD ALL` (e.g. 2.0.a's copy-abort, or a bad custom
`reset_query`) parks a dirty conn. The reset write is also the only backend
write on the path without `BACKEND_WRITE_TIMEOUT`.
**Fix:** park only when no ErrorResponse was seen AND RFQ status == `'I'`;
timeout the write.

### 2.0.c Pool key ignores startup parameters (MED)
`pool_key` = `(node,user,db)` only (backend_pool.rs:40-42) but `DISCARD ALL`
resets GUCs to the *lender's* startup values — borrowers with a different
`client_encoding` / `DateStyle` / `options` inherit the lender's settings
(silent corruption for non-UTF8 clients).
**Fix:** fold a hash of the routing-relevant startup params into the key.

### 2.0.d Minor pool hygiene
Per-op `key.to_string()`/`format!` allocs (cache the key on the session);
empty per-identity Vec entries never removed from the DashMap (evict on
empty in checkout/reap).

**Evidence:** baseline scalability — proxy/transaction c=64 = 26 067 tps vs
proxy/session 48 767; latency 2.455 ms vs 1.312 ms. Cause (server.rs:2836-2866
`release_to_pool_if_idle` → `reset_backend`): at **every** idle transaction
boundary the session loop synchronously round-trips `DISCARD ALL` to the
backend and drains its response **before reading the client's next message**.
For autocommit workloads that inserts a full backend RTT + reset execution
into every query→query gap, roughly doubling effective per-query time.
Additionally axis B shows transaction mode peaking at **35** backend conns for
32 clients (worse than session's 33): reset-in-flight overlap forces the next
query to dial/checkout another conn, churning extras.

## Changes

### 2.1 Asynchronous park (reset off the client path)
- `release_to_pool_if_idle` no longer awaits the reset. It removes the conn
  from the session cache and hands `(stream, pool_key, reset_query)` to a
  detached task: `tokio::spawn(reset_and_park(...))` which runs today's
  `reset_backend` logic and then `pool.checkin(...)`; on reset failure the
  conn is dropped (unchanged semantics).
- The session loop returns to reading the client immediately — the reset RTT
  overlaps client think time instead of adding to it.

### 2.2 Bound the reset fleet
- Global `Arc<Semaphore>` (e.g. 512 permits) owned by the pool: a release
  that cannot get a permit **drops** the connection instead of queuing
  (bounded memory/FDs, graceful under storm; a dropped conn just means a
  future dial). `try_acquire_owned` — never blocks the session loop.
- Metric counters on the pool (`resets_inflight`, `resets_dropped`) surfaced
  through the existing `/api/pools` stats so the behaviour is observable.

### 2.3 Reuse-first ordering stays intact
- `ensure_conn` already checks the pool before dialing; unchanged. A conn
  mid-reset is simply not yet in the pool — the borrower dials fresh, same
  as today's behaviour during the synchronous reset window.

### 2.4 Shutdown correctness
- Detached reset tasks hold only their own stream + an Arc to the pool; on
  proxy shutdown they finish or die with the process (no join needed — same
  as today's mid-reset drop). Drain (`SIGUSR2`) is unaffected: session map
  gates the drain, parked conns are not sessions.

## Alternatives considered
- *Skip reset when the same session re-leases its own conn*: breaks the
  cross-client reuse guarantee unless leases are tracked per-owner; more
  state for less win than async-park. Rejected.
- *Pipelining reset with the park (send DISCARD ALL, park immediately, drain
  on checkout)*: parks dirty conns and moves the drain cost onto the
  borrower's critical path. Rejected.

## Risk & tests
- Risk: MED — connection lifecycle moves across tasks. Failure modes:
  double-checkin (prevented — the conn is owned by exactly one reset task),
  fd leak on task panic (reset task is panic-free by construction: pure
  awaits + drop semantics), pool overshoot (2.2 bounds it).
- Tests: `pool-modes-test.sh` (reuse, reset proven via temp-table vanish,
  park-on-disconnect) must stay 4/4; new unit test for permit-exhaustion →
  drop; axis A transaction sweep expected ≈ session; axis B peak conns
  must be ≤ baseline's 35.
- **New `copy-poolmode-test.sh` (required):** the existing `copy-test.sh` runs
  in session mode (`proxy-pg.toml` has no `[pool_mode]`), so it cannot catch
  2.0.a. The new test runs `COPY _t FROM STDIN` **through a
  `pool_mode.mode = "transaction"` proxy** and asserts the row count lands
  and a follow-up query on the same session succeeds (today: client hang).
- Gate: full milestone protocol.

## Expected outcome
Transaction-mode c=64 tps ≈ session mode (≥ 45k vs today's 26k); latency
delta vs session ≈ 0; axis B peak conns no worse; session mode untouched
(code path gated on `backend_pool.is_some()`).
