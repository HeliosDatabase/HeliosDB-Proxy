# P-01 — user-path performance measurement plan

**Status:** harness shipped 2026-09-17 (`scripts/regress/bench-usertp.sh`). This plan
defines what "better" means for the user path, so a microbenchmark mean cannot hide a
replay/pool regression.

## Deployments compared (identical everything else)

Run each deployment against the **same backend capacity, TLS mode, durability settings,
timeouts and hardware**, in the same time window (back-to-back, never across days):

1. **direct PostgreSQL 18.4** — the floor.
2. **HeliosProxy (session pool)** — `[pool_mode] mode = "session"`.
3. **HeliosProxy (transaction pool)** — `[pool_mode] mode = "transaction"`.
4. **HAProxy + Patroni** — the credible alternative; label the exact version/config in
   the report.
5. **PgBouncer** — optional and **labelled as a separate combination** (it changes the
   session semantics), never folded into the proxy numbers.

## Metrics (per client count, per workload)

- **Committed TPS** (`pgbench -b simple-update`): transactions that actually committed.
- **Read TPS + latency tails** (`pgbench -S`): p50 / p95 / p99 / p999 from the
  per-transaction log, not just the mean.
- **First-row latency**: one-row `SELECT ... LIMIT 1` round-trip, averaged over 20 runs
  (proxy of time-to-first-row; a true streaming measurement is a follow-up).
- **Client error / unknown rates**: pgbench failed transactions plus the proxy's
  `08007`/`08006` events in its log.
- **Proxy cost**: RSS (KB) and CPU% sampled during the run.
- **WAN bytes**: local harness does not measure WAN; cross-region runs must report bytes
  at the NIC (documented as out of scope for `bench-usertp.sh`).

## Gate policy

- Baseline BEFORE (unchanged tree) and candidate AFTER (changed tree) in the same
  window; both stored under `/home/gpc/HDB/sprint/baselines/proxy/<label>/`.
- **Cumulative budget 3%** on the user path (committed TPS and p99 read latency).
- **Critical paths gate individually**: a replay, pool or relay regression fails even if
  an unrelated microbenchmark improved enough to keep the mean flat.
- Failover, burst and recovery runs need the same correctness counters as normal load:
  a run that reports success while dropping/failing writes is a failure regardless of
  its TPS.

## Running

```sh
# Backend prerequisite: PG 18.4 at 127.0.0.1:25433 with pgbench tables initialized.
CLIENTS="1 16 64" DUR=10 \
  scripts/regress/bench-usertp.sh /path/to/heliosdb-proxy baseline-1.8.0
```

Results land in `/tmp/bench-usertp/<label>-results.json` (per mode × client count ×
workload: TPS, failed txns, p50/p95/p99/p999, RSS, CPU, first-row ms) plus per-mode
proxy logs. Copy the JSON into the sprint baseline directory as evidence.

## Remaining P-01 work

- Add the HAProxy+Patroni and PgBouncer stacks to the harness (today it covers direct
  PG + HeliosProxy session/transaction).
- Automate the baseline/candidate delta report (same-window comparison) and tie it to
  the Criterion gate so one release report carries both.
- True first-row/time-to-first-byte measurement on a large streaming result.
