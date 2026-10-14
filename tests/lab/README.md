# HA/chaos laboratory (V-02)

A disposable PostgreSQL 18.4 cluster behind **two** HeliosProxy instances, driven by a
ledger client that records the outcome the client saw for every transfer. A checker then
compares those outcomes with the final database. Results of recorded runs:
[docs/ha-lab-results.md](../../docs/ha-lab-results.md).

```
ledger --+--> proxy-a :27710 --+--> pg-a :27701  primary (synchronous_standby_names = pg_b)
         +--> proxy-b :27711 --+--> pg-b :27702  synchronous standby  (promotion target)
                               +--> pg-c :27703  asynchronous standby
```

Both proxies run the same topology config (`proxy-a.toml`, `proxy-b.toml`; they differ
only in their listen and admin ports): `[topology] provider = "postgres"` (they follow
whichever node is not in recovery), `tr_mode = "transaction"`. The proxies never promote;
the driver promotes `pg-b` with `pg_promote()`, standing in for an HA manager.

## Run

```sh
tests/lab/run-lab.sh <heliosdb-proxy binary> <scenario> [outdir]
```

The binary must be built with `postgres-topology`, for example
`cargo build --release --features all-features,postgres-topology`. It is bind-mounted into
the `postgres:18.4-bookworm` image, so a glibc build from the host runs as is.

Every container uses the **host network** and binds 127.0.0.1 only: PostgreSQL on
27701-27703, the proxies on 27710/27711 with admin APIs on 27790/27791, and the edge
drill's relay on 27792, all below the Linux ephemeral port range (32768-60999) so that no
outgoing connection on the host can hold one of them (the lab used 357xx until a relay
collided with one, 2026-10-13). This host cannot create Docker bridge networks and its published
ports do not work, so the lab does not use either. A run refuses to start while any of
these ports is in use.

| Scenario | What happens |
|---|---|
| `promote-under-load` | The ledger runs through both proxies; after 10 s `pg-a` is killed (`SIGKILL`) and `pg-b` promoted. |
| `stop-old-primary` | After 10 s `pg-a` is frozen (`docker compose pause`, every process stopped) and `pg-b` promoted; 15 s later `pg-a` returns **unfenced** and still writable while the ledger runs; it is stopped (fenced) at the end. |
| `cache-logical` | C-02 slice 2, no ledger: `proxy-a` caches reads with `[cache] invalidation_source = "logical"` (`proxy-cache.toml`; pg-a runs `wal_level = logical` and has the publication). A write through `proxy-b`, a direct write on `pg-a`, and a trigger's side effect on another table must each stop `proxy-a` serving the old cached value within the poll interval. Needs a binary with that feature. |
| `edge-resume` | C-03 drill, no ledger: `proxy-a` runs as an `[edge]` home (`proxy-home.toml`), `proxy-b` as its edge (`proxy-edge.toml`). The edge reaches the home's admin API through a TCP relay (`scripts/regress/tcp-relay.py`, 27792 → 27790). The edge's cache is warmed, the relay is killed (the edge's invalidation stream drops at once) while a write goes through the home, then restarted: expected **warm** resume (missed invalidation replayed, the cached entry of another table kept; edge invalidation is per table). Then the home is restarted: expected **gap** (the edge flushes). |

Tunables (environment): `LEDGER_SECS` (default 60), `LEDGER_WORKERS` (4; even workers
use `proxy-a`, odd ones `proxy-b`), `STEP_TIMEOUT` (180 s per step).

Output (default `/home/gpc/HDB/sprint/baselines/proxy/ha-lab-<UTC>-<scenario>/`):
`driver.log`, `compose.log`, `outcomes.tsv` (one line per transfer: id, outcome,
SQLSTATE, proxy, start/end ms), `check.json`, `topology-proxy-*.json` and
`metrics-prometheus-proxy-*.prom` (each proxy's `/topology` and `/metrics/prometheus` at
the end of the run), `verdict.txt`. `cache-logical` and `edge-resume` write
`cache-drill.json` / `edge-drill.json` and admin snapshots instead of a ledger check.

## What is checked

Each transfer is one simple-query batch `BEGIN; debit; credit; INSERT transfers(id);
COMMIT;`. The client-visible outcome is `committed`, `error` (an `ERROR` with a SQLSTATE:
the proxy or server says it rolled back), `unknown` (`08007`, a `FATAL`, or the connection
broke after sending) or `not_sent` (no connection). `verdict.py` then requires:

| Property | Meaning |
|---|---|
| `balance_conserved` | `sum(balance)` on the new primary equals the starting total. |
| `no_acknowledged_loss` | Every transfer the client saw commit is on the new primary. |
| `no_phantom_write` | No transfer the client saw fail (`error`, `not_sent`) is on the new primary. |
| `writes_resume_proxy-a/b` | Both proxies committed transfers that started after the promotion, without a config edit. |
| `no_split_brain_ack` (`stop-old-primary`) | The returned former primary holds no acknowledged transfer the new primary lacks. |

`unknown` outcomes may resolve either way; their split is reported (`unknown_present`,
`unknown_absent`).

`cache-logical` checks: the cache served hits, the logical source is attached and applied
transactions, and every read after a bypassing write returns the new value.

`edge-resume` checks instead: the edge cache was warm, the reconnect logged a warm resume,
the missed invalidation was applied (fresh value), another cached entry still hit and no
flush was counted, and the home restart flushed the edge (next read misses).

## Safety

- `run-lab.sh` re-executes itself under the fleet build lock and a
  `systemd-run --scope -p MemoryMax=24G -p MemorySwapMax=0` scope; every step has a
  timeout.
- Compose project `helios-lab`, host network, every listener on 127.0.0.1; the driver
  runs its clients inside the `ledger` container (`docker compose exec`) and reads the
  admin APIs with `curl` on 127.0.0.1.
- All data directories are tmpfs, so no volumes are created. The EXIT trap runs
  `docker compose -p helios-lab down --remove-orphans` (never `-v`; there is nothing to
  remove) and reports any object still carrying the project label. A run refuses to start
  while such objects exist.
- Only objects labelled `com.docker.compose.project=helios-lab` are touched.

## Not yet covered

Network partitions and `tc netem` (with host networking a partition needs a relay per
link, as the edge drill does, or a separate network namespace), proxy drain (`SIGUSR2`)
and restart, SIGHUP with a stale config, and TLS rotation.
