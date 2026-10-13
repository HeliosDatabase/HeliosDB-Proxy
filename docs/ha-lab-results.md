# HA lab results

Recorded runs of the V-02 HA/chaos lab ([tests/lab/](../tests/lab/README.md)): a disposable
PostgreSQL 18.4 cluster (pg-a primary with pg-b as its synchronous standby and pg-c
asynchronous) behind two HeliosProxy instances. A ledger client runs four workers, two
through each proxy, for 60 s. Each transfer is one `BEGIN; debit; credit;
INSERT transfers; COMMIT` batch, and the client records the outcome it saw. `verdict.py`
then compares those outcomes with the database on the new primary.

The proxies never promote anything. The driver plays the HA manager, promoting pg-b with
`pg_promote()`. `[topology] provider = "postgres"` lets both proxies follow whichever node
is not in recovery, with no config edit and no restart.

## 2026-10-13, local main (cf78f8a)

Binary sha256 `605d3ac6…1cfc08` (`--features all-features,postgres-topology`, release).
Host gpc001ca, every container on the host network, 127.0.0.1 ports 35701-35792. Evidence:
`/home/gpc/HDB/sprint/baselines/proxy/stack-20261012/lab-main-*`.

### promote-under-load: PASS

pg-a is killed with `SIGKILL` 10 s into the ledger run, and pg-b is promoted 2 s later.

| | |
|---|---|
| transfers attempted / committed | 2447 / 2445 |
| outcomes | 2445 committed, 2 `not_sent` (the connection attempt failed; the batch never reached a server) |
| balance on pg-b | 1,000,000 = expected |
| acknowledged but missing | 0 |
| failed but present (phantom) | 0 |
| unknown outcomes | 0 |
| commits after promotion | proxy-a 1000, proxy-b 1003 |
| write gap | 3.5 s: the last acknowledgement came 2.4 s before the promotion and the first 1.1 s after it, with pg-a killed at −2 s |

Not one transfer ended `unknown`. Transactions open when pg-a died were replayed by
in-session transaction replay (`tr_mode = "transaction"`) on pg-b, or reported as not
sent. Every commit the client saw is on pg-b, and nothing it saw fail is there.

### stop-old-primary: PASS

pg-a is frozen (`docker compose pause`) and pg-b promoted. 15 s later pg-a returns
**unfenced and writable** (`pg_is_in_recovery() = f` on both nodes) while the ledger keeps
running, and it is stopped at the end.

| | |
|---|---|
| transfers attempted / committed | 2342 / 2338 |
| outcomes | 2338 committed, 4 `not_sent` |
| balance on pg-b | 1,000,000 = expected |
| acknowledged but missing / phantom / unknown | 0 / 0 / 0 |
| transfers only on the returned pg-a | 0, none of them acknowledged |
| commits after promotion | proxy-a 956, proxy-b 955 |
| write gap | 5.1 s |

Both proxies stayed on pg-b after pg-a came back as a second writable node. No
acknowledged write went to pg-a. The proxy refuses to route rather than fence the
database: fencing remains the HA manager's job (see
[topology-providers.md](topology-providers.md)).

### edge-resume (C-03): PASS

`proxy-a` is the edge home and `proxy-b` its edge; the edge reaches the home's admin API
through a TCP relay. With the edge cache warm, the relay is killed, a write goes through
the home, and the relay is restarted. Then the home is restarted.

| check | result |
|---|---|
| edge cache warm before the cut | ok |
| reconnect after the cut resumes **warm** | ok |
| missed invalidation replayed (next read returns the new value) | ok |
| a cached entry of another table is still served | ok |
| no flush counted on the warm resume | ok |
| home restart: the edge sees the new epoch and flushes (**gap**) | ok |
| next read after the flush misses | ok |

The first run failed the "kept other entries" check: the drill's other entry was in the
same table as the missed write, and edge invalidation is per table, so dropping it was
correct. The drill now uses a second table (46f6a11).

## 2026-10-13, 2026-10-12 stack tip (0020dd3)

The local branch stack: routing snapshot, P-03, H-02, C-02 slice 2, H-06, H-05, TR-08,
TR-09, L2 shed pacing and the dependency bump. Binary `tip3`, same build features.
Evidence: `/home/gpc/HDB/sprint/baselines/proxy/stack-20261012/window-M/lab-tip2-*`.

| scenario | result | committed / attempted | not_sent | unknown | lost / phantom | write gap |
|---|---|---|---|---|---|---|
| promote-under-load | PASS | 2452 / 2454 | 2 | 0 | 0 / 0 | about 3.5 s |
| stop-old-primary | PASS (no split-brain acknowledgement) | 2367 / 2371 | 4 | 0 | 0 / 0 | about 5 s |
| edge-resume | PASS, 7/7 checks | | | | | |
| cache-logical (C-02 slice 2) | PASS | | | | | |

**cache-logical:** `proxy-a` caches reads with `[cache] invalidation_source = "logical"`.
A write through `proxy-b`, a direct write on pg-a, and a trigger's side effect on a table
`proxy-a` never wrote each stopped `proxy-a` from serving the old cached value within one
poll interval. Logical source: attached, 4 transactions applied, 0 lag, 0 reconnects, 0
full invalidations.

## Not covered yet

Network partitions and `tc netem` delay, proxy drain (`SIGUSR2`) and restart, SIGHUP with
a stale config, TLS rotation, and loss of a DCS member. The Patroni example
(`examples/patroni-failover`) drills the Patroni leader lock and a frozen control path
(H-02).
