# HeliosProxy Live Talk Demo Kit

This directory contains the shareable assets for the three-demo talk:

1. Lossless switchover with transaction journal and `POST /api/replay`
2. Shadow execution for PG major-version upgrades with `POST /api/shadow`
3. Wire-edge anomaly detection with `GET /anomalies`
4. Sixty-second WASM plugin tour across the first-party plugin demos

Local state, logs, and presentation scratch files live outside the repo in
`~/HDB/Proxy-Demogrounds`.

## One-Time Setup

```bash
cd ~/HDB/Proxy
./demos/live-talk/bin/prepare-demoground.sh
```

This creates the demoground layout, writes a local `.env`, and copies the
launcher scripts and workload assets into `~/HDB/Proxy-Demogrounds`.

## Side-by-Side OLTP Comparison

Use this as the opening visual: PostgreSQL on the left, HeliosDB-Nano on the
right, both driven by the same PostgreSQL-wire `pgbench` custom script.

```bash
~/HDB/Proxy-Demogrounds/bin/oltp-race.sh up
~/HDB/Proxy-Demogrounds/bin/oltp-race.sh init
~/HDB/Proxy-Demogrounds/bin/launch-tmux.sh
```

Defaults:

- PostgreSQL container: `127.0.0.1:55432`
- HeliosDB-Nano: `127.0.0.1:16432`
- Nano binary: `~/HDB/Nano/target/release/heliosdb-nano`
- Logs: `~/HDB/Proxy-Demogrounds/logs/`

Tune `CLIENTS`, `JOBS`, `DURATION`, and ports in
`~/HDB/Proxy-Demogrounds/.env`.

If Nano is no longer reachable after a benchmark run, restart only the local
services with `~/HDB/Proxy-Demogrounds/bin/oltp-race.sh up`. The benchmark
summary and logs are still valid; some Nano builds exit after the last client
disconnects.

## Main Demo Runner

```bash
~/HDB/Proxy-Demogrounds/bin/run-main-demos.sh switchover
~/HDB/Proxy-Demogrounds/bin/run-main-demos.sh shadow
~/HDB/Proxy-Demogrounds/bin/run-main-demos.sh anomaly
~/HDB/Proxy-Demogrounds/bin/run-main-demos.sh plugins
```

`switchover` uses `tests/docker/cluster.yml`; `shadow` uses
`tests/docker/upgrade-matrix.yml`; `anomaly` delegates to
`demos/v0.4.0/01-anomaly-detection`; `plugins` prints the hot-reload plugin tour
and points to runnable demos 11-18.

## 46-Module Tour

Use `module-map.md` as the on-stage checklist. It maps every README module to
the fastest local evidence: code path, existing demo, or endpoint.
