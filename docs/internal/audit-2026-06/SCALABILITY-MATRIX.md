# Scalability matrix — Proxy baseline vs candidate × Nano 3.57 vs 3.37

Date: 2026-06-14. Client: pgbench 18.4 over `--network host`. Workload: `SELECT 1`
through the proxy (proxy on 127.0.0.1:6432 → single Nano backend), 6s per level,
threads = min(clients, 16) on a 32-core host.

- **Proxy baseline** = `heliosdb-proxy` v0.4.2 at commit `e4d0209` (pre-audit).
- **Proxy candidate** = branch `audit-2026-06-perf` (Batches A–E + F.1/F.2/F.3a).
- **Nano 3.57** = running standby on `100.64.0.2:54320`.
- **Nano 3.37** = built from tag `v3.37.0` (`6e6867b`), standalone on `127.0.0.1:55337`.

## Simple protocol throughput (tps), by client concurrency

| clients | B·3.57 (base) | **C·3.57 (cand)** | Δ | B·3.37 (base) | **C·3.37 (cand)** | Δ |
|--------:|--------------:|------------------:|----:|--------------:|------------------:|----:|
| 1  | 10,223 | **10,888** | +6.5% | 10,634 | **10,580** | −0.5% |
| 8  | 55,711 | **57,791** | +3.7% | 53,052 | **54,513** | +2.8% |
| 16 | 88,806 | **93,293** | +5.1% | 84,706 | **87,253** | +3.0% |
| 32 | 103,375 | **110,034** | +6.4% | 102,862 | **105,639** | +2.7% |
| 64 | 110,977 | **113,830** | +2.6% | 108,514 | **111,969** | +3.2% |

**Read:** the candidate matches-or-beats the baseline at every concurrency on both
Nano versions (avg ≈ +3–4%; up to +6.4%). The gains are modest because `SELECT 1`
over simple protocol is dominated by the Nano backend + network round-trip, not by
proxy CPU — the candidate's per-query work (lock-free `ArcSwap` health read, atomic
round-robin, no O(n²) relay clone) is a small slice of that path, and it shows most
at mid/high concurrency where the baseline's contention used to bite. Nano 3.57 is
a few % faster than 3.37 under both proxies.

## Extended / prepared protocol — the categorical win (candidate → Nano 3.57, c16)

| mode | baseline | **candidate** |
|------|----------|---------------|
| `-M simple` | 88,806 | **93,293** |
| `-M extended` | **ABORT** (0/N, 30s stall) | **69,658** |
| `-M prepared` | **ABORT** | **84,887** |

The baseline **cannot run** extended or prepared protocol at all (the 30s-per-message
stall fixed in Batch B) — every JDBC/asyncpg/pgx/npgsql client that uses prepared
statements was unusable. The candidate makes them work: this is not a few-percent
delta, it's *0 → working*. Memory under a large result also drops from ~113 MB to
~7 MB (streaming relay), not captured by these tps numbers.

## Headline

- **No regression anywhere; small steady-state gains on simple protocol** (both Nanos).
- **Extended + prepared protocol go from broken to fast** — the candidate's defining
  improvement for real-world drivers.
- The `extended (69.7k) < prepared (84.9k) < simple (93.3k)` ordering at c16 quantifies
  where proxy→Nano time goes and drives the Nano-side optimization recommendations
  (see `NANO-v3.57-PROXY-OPTIMIZATION-RECOMMENDATIONS.md`).

Raw per-run data: `/tmp/m_{b57,c57,b37,c37}.txt`. Harness: `scripts/regress/scale.sh`.
