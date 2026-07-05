# Group 4B — Stability completion (session RAII guard, health supervision)

The deferred stability items from Group 4.

## 4.2 — session-map RAII guard (panic-safe teardown)

`handle_client` registered the session in `state.sessions` and removed it after
the query loop, with no unwind guard. A panic anywhere in
negotiation/startup/the loop leaked the `ClientSession` entry forever —
inflating the active-session gauge and stalling every graceful drain to its full
timeout (the drain gates on `sessions.len()`).

**Fix.**
- `state.sessions` is now a `DashMap<Uuid, Arc<ClientSession>>` instead of a
  `tokio::RwLock<HashMap>` — register/deregister are synchronous and
  lock-free-sharded (also removes a per-connection async write-lock from the
  accept and teardown paths).
- A `SessionGuard` created right after registration owns the ENTIRE
  per-connection teardown in its `Drop`: deregister, bump `connections_closed`,
  and reclaim the L1 query cache. `Drop` runs on a normal return AND on a panic
  unwind, so nothing leaks. (Verified by `session_guard_deregisters_on_panic`,
  which `catch_unwind`s a panic holding the guard and asserts the map is empty.)

## 4.7 — health-loop panic supervision

A panic in `check_all_nodes` would end the health task silently, freezing health
at its last snapshot forever (the proxy would keep routing to whatever it last
believed). The per-tick sweep now runs in a child task whose `JoinError` (from a
panic) is logged, and the health loop continues. Probe tasks were already
isolated via `JoinSet`; this covers the reducer.

## Results
- Unit: `session_guard_deregisters_on_drop`, `session_guard_deregisters_on_panic`.
- Live (exercise the session map + health loop): regression 9/9; drain-timeout
  4/4; handoff 6/6; reload 6/6; notify 2/2 (teardown path changed).
- clippy `-D warnings` ×4; 272 default + 1409 all-features lib tests.

## Deferred (still)
- Accept-side connection `Semaphore` (the pre-auth `STARTUP_TIMEOUT` already
  bounds stalled connections).
- Typed `is_backend_fault` (the Display-substring classifier works for the
  actual error strings; the refactor touches failover-critical code — low ROI).
- Gateway/mirror task supervision (see the next-batch security milestone).
