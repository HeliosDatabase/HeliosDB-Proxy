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

## 2026-10-13, local main (7218448)

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
correct. The drill now uses a second table (11ffcad).

## 2026-10-13, 2026-10-12 stack tip (bfa7e2b)

The local branch stack: routing snapshot, P-03, H-02, C-02 slice 2, H-06, H-05, TR-08,
TR-09, L2 shed pacing and the dependency bump. Binary `tip3`, same build features.
Evidence: `/home/gpc/HDB/sprint/baselines/proxy/stack-20261012/window-M/lab-tip2-*`.

| scenario | result | committed / attempted | not_sent | unknown | lost / phantom | write gap |
|---|---|---|---|---|---|---|
| promote-under-load | PASS | 2452 / 2454 | 2 | 0 | 0 / 0 | 2.7 s |
| stop-old-primary | PASS (no split-brain acknowledgement) | 2367 / 2371 | 4 | 0 | 0 / 0 | 5.1 s |
| edge-resume | PASS, 7/7 checks | | | | | |
| cache-logical (C-02 slice 2) | PASS | | | | | |

**cache-logical:** `proxy-a` caches reads with `[cache] invalidation_source = "logical"`.
A write through `proxy-b`, a direct write on pg-a, and a trigger's side effect on a table
`proxy-a` never wrote each stopped `proxy-a` from serving the old cached value within one
poll interval. Logical source: attached, 4 transactions applied, 0 lag, 0 reconnects, 0
full invalidations.

## 2026-10-14, reconnect wave (H-05), main 24416af + reconnect limit

Binary `wave2` (sha256 in the evidence), same build features, lab ports 27701-27792.
Evidence: `/home/gpc/HDB/sprint/baselines/proxy/wave-20261014/lab-reconnect-wave` and
`lab-wave-control`.

`proxy-a` runs `proxy-wave.toml`. Two tenants drive it through pgbench, named by
`application_name`: `big` with 48 tpcb-like clients and `small` with 2 select-only
clients, for 60 s, all with transaction replay on. pg-a is killed at 15.9 s and pg-b
promoted at 17.9 s.

| run | max reconnects in progress | permit waits / refusals | TR re-homes | big tenant back to ½ throughput | small tenant | proxy RSS before kill → end |
|---|---|---|---|---|---|---|
| `max_concurrent_reconnects_per_node = 4` | 4 | 749 / 0 | 47 | 1.1 s after promotion | served every second except the 2 s of the kill and promotion (17-18 s); 0 aborted | 16.5 MiB → 17.3 MiB, flat after |
| control, limit 1000 | 0 sampled (11 in an earlier run; dials finish within the 250 ms sampling) | 0 / 0 | 47 | 1.2 s | same | 16.3 MiB → 17.2 MiB |

The limit meters the reconnect wave at no cost to recovery time. The big tenant lost 6
clients (control: 4) to `outcome unknown` errors. Those were in-flight COMMITs at the
moment pg-a was killed: replay cannot know whether they landed and says so instead of
guessing. In an earlier run with the default committed-history ring (50,000
transactions), the proxy's RSS grew about 3 MiB/s until the ring was full and then held
flat. The drill therefore uses a 2,000-transaction ring, so it measures the failover
rather than the ring filling.

## 2026-10-14, reconnect wave at the default limit (16), main 0c73797

Binary `rdef2` (sha256 in the evidence), same drill. Evidence:
`/home/gpc/HDB/sprint/baselines/proxy/rdef2-20261014/lab-reconnect-wave` and
`lab-wave-default`. `max_concurrent_reconnects_per_node` now defaults to 16. The drill
reads the new `heliosdb_proxy_backend_reconnects_peak` gauge, the most connections
opened at once to one node, instead of sampling the in-progress gauge every 250 ms:
dials finish within one sample, so the sampled maximum was 0 in both runs.

| run | peak reconnects to one node | permit waits / refusals | TR re-homes | big tenant back to ½ throughput | small tenant | proxy RSS before kill → end |
|---|---|---|---|---|---|---|
| `max_concurrent_reconnects_per_node = 4` | 4 | 618 / 0 | 45 | 1.2 s after promotion | 0 aborted, served outside the kill and promotion seconds | 15.8 MiB → 16.5 MiB |
| default, 16 | 16 | 48 / 0 | 47 | 1.1 s | same | 15.9 MiB → 16.5 MiB |

The default limit is reached by a 48-client wave and holds it at 16 without refusals or a
slower recovery.

## 2026-10-21, admin node controls (b251750f1d54), branch fix/admin-node-controls-20261020 (4a5a975)

`POST /api/chaos force_unhealthy` and `POST /nodes/{addr}/disable` now reach routing (before
this fix they only changed the admin API's display copy). Binary sha256 `bf8cf1ac…9f539b`
(`--features all-features,postgres-topology`, release). Evidence:
`/home/gpc/HDB/sprint/baselines/proxy/b251750f1d54-20261020/ha-lab-*`.

| scenario | result | committed / attempted | not_sent | unknown | lost / phantom | write gap |
|---|---|---|---|---|---|---|
| chaos-force-leader (new) | PASS (no split-brain acknowledgement) | 2137 / 2137 | 0 | 0 | 0 / 0 | 9.4 s |
| promote-under-load | PASS | 2403 / 2405 | 2 | 0 | 0 / 0 | 3.6 s |
| stop-old-primary | PASS (no split-brain acknowledgement) | 2359 / 2363 | 4 | 0 | 0 / 0 | 5.1 s |
| reconnect-wave (H-05) | PASS, 6/6 checks | | | | | |
| cache-logical (C-02 slice 2) | PASS, 6/6 checks | | | | | |
| edge-resume (C-03) | PASS, 7/7 checks | | | | | |

**chaos-force-leader:** 10 s into the ledger run, pg-a is held out through each proxy's
`POST /api/chaos force_unhealthy`; no process is touched. Both proxies answered
`{"applied":"force_unhealthy"}` and, read back in the same second, `/topology` named no
primary (`currentPrimary: null`, the provider's authority still valid) and `/nodes` showed
pg-a `healthy: false` with `override.kind = "force_unhealthy"`. Writes waited. pg-b was
promoted 2 s later while pg-a stayed up, unfenced and writable, for the rest of the run;
`[topology] provider = "postgres"` then saw two nodes not in recovery and chose pg-b once
its post-promotion checkpoint raised its timeline, which is why the write gap (9.4 s) is
longer than after a kill (3.6 s): the ledger's writes were held, not failed, and every one
of the 2137 transfers committed (863 through proxy-a and 845 through proxy-b after the
promotion). pg-a holds no acknowledged transfer pg-b lacks. `reset` on each proxy cleared
the override (`restored: ["127.0.0.1:27701"]`) and pg-a was stopped (fenced) at the end.

## 2026-10-22, remaining chaos verbs (Sprinter 967e1db11c29), branch feat/lab-chaos-verbs-20261022

Binary labverbs-heliosdb-proxy (sha256 `d9269655…9248a8`, `--features all-features,postgres-topology`,
release), main c85b0bf plus the reloadable client TLS acceptor. Evidence:
`/home/gpc/HDB/sprint/baselines/proxy/lab-verbs-20261022/ha-lab-*`.

| scenario | result | committed / attempted | not_sent | unknown | lost / phantom | proxy-a write gap |
|---|---|---|---|---|---|---|
| drain-restart (new) | PASS, 7/7 | 2525 / 2548 | 23 | 0 | 0 / 0 | 0.1 s |
| stale-sighup (new) | PASS, 7/7 | 2558 / 2558 | 0 | 0 | 0 / 0 | 0.07 s |
| tls-rotation (new) | PASS, 6/6 | 2435 / 2435 | 0 | 0 | 0 / 0 | 0.1 s |

**drain-restart:** 10 s into the ledger run proxy-a received `SIGUSR2`. It closed its listener,
let the in-flight sessions finish and exited 531 ms later (`drain complete` in its log); the
driver started it again and it served 911 ms after exiting. The ledger workers on proxy-a saw 23
refused connections in that window (`not_sent`, never `unknown`: a drain finishes what is in
flight) and committed again at once; proxy-b was untouched. No cluster change: the check ran
against pg-a.

**stale-sighup:** proxy-a read a copy of its config the driver rewrote. First `SIGHUP` with a
file that does not parse: refused (`SIGHUP reload failed to parse — keeping current config`),
`/nodes` kept 3 nodes. Then `SIGHUP` with a stale file that predates pg-c: applied
(`SIGHUP: node removed from config`), `/nodes` dropped to 2, the provider's leader stayed pg-a
and every one of the 2558 transfers committed through both proxies.

**tls-rotation:** proxy-a terminated client TLS (`proxy-a-tls.toml`); the ledger's `psql`
negotiated it with its default `sslmode=prefer`. A new self-signed certificate was written over
the old files and `SIGHUP` sent: the fingerprint a new client saw changed from the old
certificate's to the new file's (`SIGHUP: client TLS certificate reloaded`), established
sessions kept going, and all 2435 transfers committed. Before this change the acceptor was
built once at startup, so a rotation needed the `SIGUSR2` drain and a restart.

### Patroni demo, same day (`examples/patroni-failover`, three-member etcd)

Evidence: `/home/gpc/HDB/sprint/baselines/proxy/lab-verbs-20261022/patroni-*.log`.

**`dcs` (DCS member loss): PASS.** Under three writers (one fresh session per `INSERT`) and
a `/topology` sampler: `etcd1` stopped, 20 s later `etcd2` (quorum lost), 38 s later both
started again. With one member down 241 writes committed and the proxy never lost its write
target; once the quorum had been lost for Patroni's `ttl` (15 s) plus the proxy's lease
(8 s), no write completed until the heal (28 samples without a write target, writes refused
with `08006` or a dropped connection), and 262 committed after it. Patroni re-elected
`pg-b`; nothing the client saw commit is missing on it.

**`delay` (control-path latency), 1 s: lease flaps.** The relay delays each chunk in both
directions, so 1 s is a round trip of about 2 s, exactly `patroni_request_timeout_ms` (2 s).
Polls then timed out intermittently and the proxy's lease lapsed and returned repeatedly: 25
of the samples in that phase had no write target, while 61 writes still committed whenever
the lease was valid; the 5 s phase and the heal behaved as expected (no write completed after
the lapse, 200 committed after the heal, nothing acknowledged missing). This is the designed
fail-closed behaviour, not a defect: a control path whose round trip sits at the request
timeout is indistinguishable from a partition. Operators with a slow Patroni REST path should
raise `patroni_request_timeout_ms` (and keep `lease_timeout_secs` below Patroni's `ttl` minus
`loop_wait`). The drill's benign phase was recalibrated to 0.4 s (round trip 0.8 s).

**`delay`, recalibrated (0.4 s then 5 s): PASS.** Under the 0.4 s delay (round trip 0.8 s)
200 writes committed and the write target never vanished; under 5 s every poll timed out, the
lease lapsed (26 samples without a write target) and no write that started after the lapse
completed before the latency was removed; 200 committed after the heal; nothing acknowledged
is missing on the leader. 566 writes in all, 0 errors, 0 unknown.

## Not covered yet

Kernel-level network faults (`tc netem` delay and loss, `iptables` partitions): this host
cannot create Docker bridge networks, so every lab container shares the host network
namespace and such tooling would hit the host itself. Control-path delay and black-holing
are covered in user space instead (`scripts/regress/tcp-relay.py --control`, the Patroni
demo's `delay` mode and the H-02 `control-partition` drill); data-path partitions are not.
