# Patroni failover demo

Patroni (with etcd) elects, promotes and fences. HeliosProxy only follows the leader Patroni
reports (`[topology] provider = "patroni"`) and keeps a client session alive through the change
with Transaction Replay (`tr_mode = "transaction"`). The proxy does not promote or fence anything here.

## Run it

```bash
cargo build --release --features all-features,postgres-topology   # the prebuilt release binary lacks the Patroni provider
cd examples/patroni-failover
./run-demo.sh ../../target/release/heliosdb-proxy
```

Needs Docker with Compose, `python3` with `psycopg2`, and `curl`. It builds a small Patroni image
(`hp-patroni-demo:local`, PostgreSQL 18.4 + `pip install patroni[etcd3]`), starts a three-member etcd and two
members on loopback ports 35601-35625, starts the proxy on 35600 (admin 35690) and runs three scenarios. Everything it
creates is named `hp-patroni-demo-*` and is removed when the script exits (exact names; it never uses
`down -v` and never touches other containers). Demo only: loopback `trust` authentication.

### H-02 drills (`./run-demo.sh <binary> h02`)

Same cluster, but the proxy polls Patroni through two killable TCP relays
(`scripts/regress/tcp-relay.py`, 35631/35632 -> 35611/35612), and two drills run under
load (three writers, one fresh session per `INSERT`, plus a `/topology` sampler):

- `lock-drill`: the leader container is frozen (`docker pause`); Patroni moves the leader
  lock after its `ttl`; the old leader is unpaused 10 s later. Passes when no write the
  client saw commit is missing on the new leader, none exists only on the returned old
  leader, the proxy's routing never names the old leader once the lease has run out after
  the move, and writes resume on the new leader.
- `control-partition`: only the relays are frozen for 25 s (Patroni and the data path are
  untouched). Passes when, once the lease (8 s) has run out, the proxy has no write target
  and no write that started after that completes before the heal, and writes resume after
  it with nothing acknowledged missing.

### DCS and control-path drills (`./run-demo.sh <binary> dcs`, `./run-demo.sh <binary> delay`)

Same cluster and load as the H-02 drills:

- `dcs`: `etcd1` is stopped (two of three members remain: quorum kept), 20 s later `etcd2`
  (quorum lost: Patroni cannot renew the leader key and demotes after `ttl`; the proxy's
  lease lapses), then both are started again. Passes when writes flowed with one member
  down and the proxy never lost its write target then, no write that started once the
  quorum had been lost for `ttl` plus the lease completed before the heal, the proxy had
  no write target during that time, writes resumed after the heal, and nothing
  acknowledged is missing on the final leader.
- `delay`: the relays carrying the proxy's Patroni polls get latency through their
  control file (`tcp-relay.py --control`; the relay delays each chunk both ways, so a
  delay of D is a round trip of about 2D): 0.4 s for 20 s (round trip 0.8 s, inside
  `patroni_request_timeout_ms` = 2 s: no effect expected), then 5 s (every request times
  out: the lease lapses and writes wait), then none. Passes when writes flowed and
  the write target never vanished under the 0.4 s delay, no write that started after the
  lapse completed under the 5 s delay, the proxy had no write target then, writes resumed
  after the heal, and nothing acknowledged is missing on the leader.

## What `demo.py` does

1. Opens ONE client connection through the proxy and probes which backend it is on.
2. Starts a transaction and writes row 1 (not committed).
3. Has Patroni change leader: `switchover` (planned, REST) or `failover` (the leader container is stopped).
4. Continues the same transaction: writes row 2 and commits, on the same client connection.
5. Reads the new leader directly (bypassing the proxy) and passes only if the new leader is out of
   recovery and holds exactly rows 1 and 2.

## Observed result (2026-10-12, PostgreSQL 18.4, proxy from branch fix/authoritative-new-session-20261012)

Full output: [evidence-2026-10-12-after-fix.log](evidence-2026-10-12-after-fix.log). All three scenarios passed in one run:

| Scenario | Leader | Client outcome | Rows on the leader |
|---|---|---|---|
| 1. planned switchover, transaction open | -> new member | transaction continued and committed on the same connection | 1 and 2, once each |
| 2. brand-new session after the promotion | current leader | connected to the leader, write succeeded | row 3 present |
| 3. leader container stopped, transaction open | -> other member | transaction continued and committed | 1 and 2, once each |

Scenario 2 fails with the proxy built from `main` before the fix (`cannot execute ... in a read-only transaction`;
[before-fix log](evidence-2026-10-12-before-fix-base.log): in that run Patroni happened to elect `pg-b`, the node
configured as standby, so even the first setup write was refused).

## What this does and does not show

- One session with an open transaction surviving a Patroni leader change, with the transaction replayed on the
  new leader, and a new session reaching the new leader. One run on one host, not a statistical or load test.
- The statements inside the transaction must replay to the same results: Transaction Replay verifies this
  and rolls back with SQLSTATE `40001` otherwise. In an earlier attempt of this demo, a
  `SELECT inet_server_port()` placed inside the transaction differed on the new backend and the proxy
  correctly refused to continue. That is why the script probes the backend outside the transaction.
- No zero-RPO claim: Patroni's `maximum_lag_on_failover` here is 1 MiB, and a failover can lose
  asynchronously replicated commits. The demo only commits after the leader change.
- Read routing is checked only through the proxy's published routing snapshot
  (`GET /topology` → `routing`: after the switchover it names the new leader as the write
  target and the demoted former leader as the read replica). The demo's client sessions
  stay on the node they started on, so no client read is routed to a replica here.
