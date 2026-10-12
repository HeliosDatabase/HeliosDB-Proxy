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

## Observed result (2026-10-12, proxy 2.0.0 + this tree, PostgreSQL 18.4)

Full output: [evidence-2026-10-12.log](evidence-2026-10-12.log). Both scenarios passed in the same run:

| Scenario | Leader | Client outcome | Rows on new leader |
|---|---|---|---|
| planned switchover | pg-a -> pg-b | open transaction continued and committed; session moved from backend port 35601 to 35602 | 1 and 2, once each |
| leader container stopped | pg-a -> pg-b | same | 1 and 2, once each |

## What this does and does not show

- It shows one session with an open transaction surviving a Patroni leader change, with the
  transaction replayed on the new leader. It is one run on one host, not a statistical or load test.
- The statements inside the transaction must replay to the same results: Transaction Replay verifies this
  and rolls back with SQLSTATE `40001` otherwise. In an earlier attempt of this demo, a
  `SELECT inet_server_port()` placed inside the transaction differed on the new backend and the proxy
  correctly refused to continue. That is why the script probes the backend outside the transaction.
- No zero-RPO claim: Patroni's `maximum_lag_on_failover` here is 1 MiB, and a failover can lose
  asynchronously replicated commits. The demo only commits after the leader change.

## Known limitation (found while building this demo)

A NEW client session opened after the leader changed starts on the node configured with
`role = "primary"`, which Patroni has by then demoted, and its first write fails with
`cannot execute ... in a read-only transaction`. Sessions that were already open are re-homed correctly.
Until that is fixed, keep the configured roles aligned with the Patroni leader (the script switches the leader
back to pg-a before each scenario for this reason). Tracked in Sprinter.
