# HeliosProxy integration-test cluster

docker-compose harness for exercising the Transaction-Replay and
load-balancing code paths against a real PostgreSQL streaming-replication
cluster.

## Topology

```
   pgbench-chaos --> helios-proxy --+--> pg-primary
                                    +--> pg-standby-sync   (synchronous)
                                    +--> pg-standby-async  (asynchronous)
```

The `pgbench-chaos` container generates a continuous write+read workload
through the proxy. The other four containers make up the cluster under
test. Fault-injection scenarios (`pgbench-chaos.sh scenario ...`) send
SIGKILL / SIGSTOP / `tc netem` commands to the target container via
`docker compose exec`.

## Files

| Path | Purpose |
|---|---|
| `cluster.yml` | Compose file wiring primary + 2 standbys + proxy + pgbench driver. |
| `primary-init.sh` | Creates the `repl` role + opens pg_hba for replication and client traffic. |
| `standby-init.sh` | `pg_basebackup` the primary, set `primary_conninfo` with the right `application_name`, start as hot standby. |
| `proxy.toml` | HeliosProxy config pointing at the three service-names in the compose network. |
| `pgbench-chaos.sh` | Init / run / scenario commands. |

## Prereqs

- Docker Engine 20.10+ with Compose V2 (`docker compose ...`).
- Rust toolchain for the proxy image build (the compose file builds from
  `../../docker/Dockerfile`).
- For network-partition / netem scenarios: the postgres image ships
  without `iproute2`; install it on-the-fly or switch to `postgres:16`
  (not `-alpine`) if you need those scenarios reliably.

## Usage

```sh
# Build + bring up the cluster (waits until all health checks pass).
docker compose -f tests/docker/cluster.yml up --build --wait

# Create the pgbench schema on the primary via the proxy.
tests/docker/pgbench-chaos.sh init

# Run a 5-minute load test in the background.
tests/docker/pgbench-chaos.sh run 300 &
LOAD_PID=$!

# Inject a primary-kill failover mid-load.
sleep 30
tests/docker/pgbench-chaos.sh scenario primary-sigkill

# Wait for the load test to finish.
wait $LOAD_PID

# Inspect.
tests/docker/pgbench-chaos.sh status
cat /tmp/pgbench-chaos.log | tail -30
```

## Success criteria

For a functional-correctness pass (T0-IT1 exit):

- `docker compose up --wait` returns 0 within 30 s.
- `pgbench-chaos.sh init` completes without error.
- `pgbench-chaos.sh run 60` returns with **zero TPS gaps > 10 s**
  (read from pgbench's `progress` output) when no scenario fires.

For the TR / failover exit criteria (T0-IT2 + T0-IT3 take over the
richer assertions):

- `pgbench-chaos.sh scenario primary-sigkill` during load: pgbench sees
  at most a bounded burst of transient errors while the proxy detects
  the dead primary and promotes a standby; after that, TPS recovers.
  The exact SLA is the subject of T0-IT2.

## Teardown

```sh
docker compose -f tests/docker/cluster.yml down -v
```

`-v` removes the data volumes — always use it between runs to guarantee
a clean primary.
