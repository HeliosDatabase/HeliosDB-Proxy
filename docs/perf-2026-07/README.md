# HeliosProxy performance & stability program — 2026-07

Follow-up to the 2026-06 audit (all batches A–H shipped, v1.3.1). This round
targets the **data-path efficiency gap** and **relay robustness** found by a
fresh code review + live measurement of v1.3.1 (`main` @ 1b2b5ab).

## Evidence baseline (2026-07-04, this host, PG 18.4 @ 127.0.0.1:25433)

Binary: `/tmp/perf-2026-07/heliosdb-proxy-baseline-main` (v1.3.1, default
features = `pool-modes`). Full logs: `/tmp/perf-2026-07/baseline-full.log`.

Regression battery: **PG 9/9 PASS, Nano 7/7 PASS (2 skip)**.

Scalability (pgbench -S, DUR=10, CLIENTS=1/4/16/64):

| tps            | c=1   | c=4    | c=16   | c=64   |
|----------------|-------|--------|--------|--------|
| direct         | 4 613 | 22 251 | 58 557 | 97 422 |
| proxy/session  | 4 973 | 11 806 | 20 484 | 48 767 |
| proxy/transaction | 3 779 | 11 440 | 28 085 | 26 067 |

Backend-conn efficiency (32 bursty clients): session 33, transaction **35**
(transaction mode currently buys *nothing* on conns and costs ~half the tps).

CPU accounting under c=64 load (63.9k tps run): **48.9 µs CPU/query,
25% user / 75% sys** — syscall + allocator dominated. The relay allocates
**two fresh 16 KiB buffers per query** (~1 GB/s churn at this rate) and
re-serializes every forwarded message.

## Modification groups

| Group | File | Theme | Risk | Affects |
|-------|------|-------|------|---------|
| G1 (M1) | [GROUP-1-hotpath-allocs.md](GROUP-1-hotpath-allocs.md) | Per-query allocation & copy elimination | Low-Med | all builds |
| G2 (M2) | [GROUP-2-pool-release.md](GROUP-2-pool-release.md) | Pool-modes correctness (COPY/poison/key) + release off the critical path | Med | default (`pool-modes`) |
| G3 (M3) | [GROUP-3-relay-eventing.md](GROUP-3-relay-eventing.md) | Event-driven relays: auth deadlock/ErrorResponse-blindness, Flush 200 ms stall, idle NOTIFY | Med-High | all builds |
| G4 (M4) | [GROUP-4-stability-hardening.md](GROUP-4-stability-hardening.md) | Pre-auth panic/DoS + control-plane stability | Low | all builds |
| G5 (M5) | [GROUP-5-feature-layer-efficiency.md](GROUP-5-feature-layer-efficiency.md) | Feature-layer leaks + per-query waste + global-lock chokepoints | Low-Med | `all-features` |

Each group = one milestone = one branch = one PR, gated by the protocol below.
Order rationale: G1 first (universal perf, low risk), G4 early-priority (unauth
crash/DoS safety) but sequenced after the perf work so the two big data-path
milestones (G1/G2) land on a stable base; G5 last (largest surface, all-features
only). G4's protocol-panic fixes (4.1) may be pulled forward if desired — they
are self-contained.

## Milestone gate protocol (user-approved)

At the end of each milestone:
1. `cargo fmt --check`, `clippy -D warnings` (default + all-features), full
   lib tests (default + all-features).
2. **Offloaded to a Sonnet-5 sub-agent**: live regression battery
   `BK=pg` + `BK=nano scripts/regress/run.sh <candidate>` and scalability
   bench `scripts/regress/bench-scalability.sh` for **baseline binary and
   candidate back-to-back** (same host window), plus the feature regress
   scripts touched by the group.
3. Compare candidate vs baseline **and vs previous milestone**: no regression
   battery failures; tps within noise or better (judged on proxy/direct ratio
   per run to cancel host variance); RSS flat on large_stream.
4. Pass → PR to `main`, merge. Fail → fix or revert the offending change.

Baseline numbers live in `/tmp/perf-2026-07/` and are re-measured next to the
candidate at every gate (never compared across days).
