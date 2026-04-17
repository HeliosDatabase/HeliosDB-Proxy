# Impossible Query Demo

A 60-second marketing demo that proves HeliosProxy can survive a primary database
failure **mid-transaction** with zero errors and zero data loss.

## What This Proves

- **Transaction Replay (TR)** buffers in-flight transactions and replays them on a
  new primary after failover.
- The client sees a successful `COMMIT` even though the original primary was killed
  with `SIGKILL` while the transaction was open.
- Failover detection + promotion + replay happens in under 15 seconds.

## Prerequisites

- Docker and Docker Compose
- `psql` (PostgreSQL client) installed locally
- `curl` and `python3` (for admin API output formatting)

## How to Run

```bash
# Interactive mode (pauses between steps for live demos)
./demo.sh

# Automatic mode (runs straight through)
./demo.sh --auto
```

## What Happens

1. A 2-node PostgreSQL cluster starts (1 primary + 1 streaming standby)
2. HeliosProxy connects to both nodes with Transaction Replay enabled
3. A client opens a transaction through the proxy: `BEGIN`, `INSERT`, `UPDATE`
4. The primary is killed with `docker kill` (simulating hardware failure)
5. The client issues `COMMIT` — HeliosProxy detects the failure, promotes the
   standby, and replays the buffered transaction on the new primary
6. `COMMIT` succeeds. A `SELECT` confirms the data exists.

## Expected Output

- Steps 1-3: Cluster starts, transaction opens normally
- Step 4: Primary is killed
- Step 6: `COMMIT` succeeds (typically within 2-10 seconds)
- Step 7: `SELECT` returns the inserted order and updated inventory
- Step 9: Summary shows zero errors, zero data loss

## What to Look For

- The **commit latency** — how long the client waited for the replay to complete
- The **proxy logs** (`docker compose logs heliosproxy`) show the failover detection,
  standby promotion, and transaction replay in real-time
- After failover, the admin API (`/nodes`) shows the former standby as the new primary

## Cleanup

The demo cleans up automatically on exit. To clean up manually:

```bash
docker compose down -v --remove-orphans
```
