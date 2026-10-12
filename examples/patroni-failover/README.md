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
(`hp-patroni-demo:local`, PostgreSQL 18.4 + `pip install patroni[etcd3]`), starts etcd and two members on
loopback ports 35601-35621, starts the proxy on 35600 (admin 35690) and runs two scenarios. Everything it
creates is named `hp-patroni-demo-*` and is removed when the script exits (exact names; it never uses
`down -v` and never touches other containers). Demo only: loopback `trust` authentication.

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
- Reads are still routed by configured node role, not by the provider's leader.
