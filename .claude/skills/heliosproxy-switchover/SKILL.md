---
name: heliosproxy-switchover
description: Failover and switchover semantics — automatic primary detection, switchover buffer, manual triggering via chaos. What happens between "primary down" and "promoted standby is now serving". Use when the user says "failover", "switchover", "promote", "what happens during failover", "currentPrimary stuck null", "lastFailoverAt".
allowed-tools: Bash(curl *), Bash(grep *), Read
related: [heliosproxy-overview, heliosproxy-chaos, heliosproxy-topology, heliosproxy-time-travel]
---

# Switchover & failover

There is no `POST /api/switchover` endpoint in v0.4.x — switchover
is **automatic**, driven by the primary tracker watching the topology
provider. Manual control happens by inducing health changes
(chaos / cordon a node) and letting the tracker react.

## When to use

- Reading what just happened during an incident (`lastFailoverAt`)
- Running a planned maintenance switchover
- Diagnosing "currentPrimary went null and stayed null"
- Understanding the role of `SwitchoverBuffer` in keeping clients
  alive during the cutover

🔵 Read-only for observation; 🟠 mutating when you trigger via chaos

## How it actually works

1. **Topology provider** (`postgres-topology` or `heliosdb-topology`,
   built into Cargo features) polls each node periodically.
   `postgres-topology` runs `SELECT pg_is_in_recovery()` to identify
   primaries. Default poll interval: `[health].check_interval_secs`.
2. **PrimaryTracker** observes the topology provider's stream and
   updates the proxy's `current_primary` on change.
3. **FailoverController** (`src/failover_controller.rs`) handles
   the transition: drains in-flight writes from the old primary,
   waits for the standby to catch up (sync mode only), promotes
   it, and broadcasts a `FailoverEvent` to subscribers.
4. **SwitchoverBuffer** (`src/switchover_buffer.rs`) holds incoming
   writes briefly during the cutover so clients see latency, not
   errors — buffer size is `[ha].switchover_buffer_capacity`.
5. Once promotion completes, `lastFailoverAt` is set in `/topology`
   and traffic resumes.

This whole sequence completes in <10 s on a clean cluster. With
`ha-tr` enabled, in-flight writes that didn't make it to the old
primary's journal are replayed onto the new primary (see
`heliosproxy-time-travel`).

## Surfaces

| Action | How |
|---|---|
| Trigger by inducing primary failure | `POST /api/chaos {force_unhealthy, target_node: <primary>}` |
| Trigger by stopping the primary process | kill on the actual PG node |
| Trigger by network partition | `iptables -A OUTPUT -d primary -j DROP` |
| Observe transition | `GET /topology` polled every 1 s |
| Read failover events | `journalctl -u heliosproxy \| grep -i failover` |

## Recipes

### Recipe 1: Planned switchover (no DB-side downtime)

```bash
# 1. snapshot starting state
curl -s http://localhost:9090/topology | jq .

# 2. mark current primary unhealthy via chaos (triggers failover)
PRIMARY=$(curl -s http://localhost:9090/topology | jq -r .currentPrimary)
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d "{\"action\":\"force_unhealthy\",\"target_node\":\"$PRIMARY\"}"

# 3. wait for the new primary to settle
for i in 1 2 3 4 5 6 7 8 9 10; do
  cur=$(curl -s http://localhost:9090/topology | jq -r .currentPrimary)
  [ "$cur" != "$PRIMARY" ] && [ "$cur" != "null" ] && { echo "promoted: $cur"; break; }
  sleep 1
done

# 4. clear the chaos override
curl -s -X POST http://localhost:9090/api/chaos \
  -H 'Content-Type: application/json' \
  -d "{\"action\":\"restore\",\"target_node\":\"$PRIMARY\"}"
```

The old primary doesn't auto-demote — it'll be reachable but
flagged as `role: primary` (configured) while `currentPrimary` is
the new one (live). Use `proxy.toml` to update configured roles
on next restart, or rely on the operator (`heliosproxy-iac`) to
do it.

### Recipe 2: Observe the failover log

```bash
journalctl -u heliosproxy -n 200 -f | grep -E 'failover|primary|switchover'
```

Sample log:

```
INFO  PrimaryTracker: detected pg_is_in_recovery=true on pg-primary:5432
INFO  FailoverController: state Normal -> PrimaryFailed
INFO  SwitchoverBuffer: buffering enabled (capacity=128)
INFO  FailoverController: candidate pg-replica-1:5432 sync_lag=0 selected
INFO  FailoverController: promoting pg-replica-1:5432 (pg_promote)
INFO  FailoverController: state PrimaryFailed -> Completed in 3147ms
INFO  PrimaryTracker: current_primary now pg-replica-1:5432
INFO  SwitchoverBuffer: drained 7 buffered writes
```

`drained N buffered writes` is the count of client requests that
the buffer absorbed during the cutover and replayed onto the new
primary. If `N` is unexpectedly high (>1000), the cutover took too
long — check standby lag.

### Recipe 3: Diagnose stuck `currentPrimary: null`

```bash
curl -s http://localhost:9090/topology | jq '{currentPrimary, nodes: [.nodes[] | {address, role, healthy}]}'
```

If `currentPrimary` stays `null` for more than ~30 s with at least
one `role: primary` node listed:

1. **No topology provider configured?** — `postgres-topology` or
   `heliosdb-topology` feature compiled-in but `[ha]` block is
   missing or `[ha].mode = "manual"`.
2. **All primaries unhealthy** and `auto_failover = false`. Set
   `auto_failover = true` in `[ha]`, restart, or manually edit
   the topology by enabling/disabling nodes.
3. **Standby lag too large** for promotion (`max_lag_bytes`
   exceeded). Cluster is in split-brain risk; the proxy
   intentionally refuses to promote. Investigate the standby's
   replication state directly on the DB.

### Recipe 4: Read post-failover state

```bash
curl -s http://localhost:9090/topology | jq '{
  currentPrimary,
  lastFailoverAt,
  failoverDurationMs: ((.lastFailoverAt | fromdateiso8601) - (.lastFailoverPrimaryDownAt // .lastFailoverAt | fromdateiso8601)) * 1000
}'
```

`lastFailoverAt` is null until the first failover after start.
After it fires, every subsequent failover overwrites it. Persist
to your alerting / audit log.

### Recipe 5: Verify replication caught up before drain ended

```bash
# on the new primary
psql -h $NEW_PRIMARY -U postgres -d postgres \
  -c "SELECT * FROM pg_stat_replication"
```

If the failover used `[ha].prefer_sync_standby = true`, the chosen
standby had `sync_state = 'sync'`. With async, expect a small data
gap — that's where `ha-tr` replay kicks in.

## Pitfalls

- **Switchover ≠ a 1-click button.** It's a state machine. Watch
  it with `/topology` polling and the log; don't expect
  instantaneous results.
- **Chaos `restore` doesn't fail back.** Once promoted, the new
  primary stays. To return to the original primary, induce a
  second failover by chaosing the new one.
- **`prefer_sync_standby` requires sync replication configured at
  the DB layer** (`synchronous_standby_names` in postgresql.conf).
  Without it, the proxy can't tell sync from async standbys; it
  picks lowest-lag.
- **`max_lag_bytes` is a refusal threshold, not a delay.** If lag
  exceeds it, no standby gets promoted and the cluster sits with
  `currentPrimary: null` until something changes. Don't set it too
  tight in async-replication setups.
- **`SwitchoverBuffer` capacity is bounded.** During a multi-second
  cutover under high write load, the buffer fills and incoming
  writes are rejected with a 503-ish PG error. Tune
  `[ha].switchover_buffer_capacity` for peak QPS × cutover seconds.
- **`pg_promote()` fails on standbys not configured for promotion.**
  In that case the failover gives up; the log will say
  `pg_promote: cannot promote during recovery: ...`. Fix the standby
  config and retry.

## See also

- `heliosproxy-chaos` — induce failover for testing
- `heliosproxy-topology` — observe `/topology` and `/nodes` during
- `heliosproxy-time-travel` — replay writes that lost the cutover race
- `heliosproxy-config` — `[ha]` block tuning
- Code: [`src/failover_controller.rs`](../../src/failover_controller.rs)
- Code: [`src/primary_tracker.rs`](../../src/primary_tracker.rs)
- Code: [`src/switchover_buffer.rs`](../../src/switchover_buffer.rs)
- Demo: [`demos/v0.4.0/10-admin-rest/`](../../demos/v0.4.0/10-admin-rest/)
