# Impossible Query Demo

A legacy marketing scenario that kills a primary while issuing SQL through the
proxy. **This script does not prove transaction recovery or zero data loss.**

## Current limitations

- `run_sql` opens a new `psql` session for every call. `BEGIN`, the writes, and
  `COMMIT` therefore do not belong to one persistent transaction.
- Several command failures are masked by fallback output. Treat output as
  illustrative; verify SQL responses and persisted state independently.
- The daemon does not promote standbys. This setup has no external promotion
  authority, so primary failure can produce write timeouts until recovery.
- A `COMMIT` with an unknown outcome is never blindly replayed. Replay modes and
  their limits are described in [transaction replay](../../docs/transaction-replay.md).

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
3. Separate client sessions issue `BEGIN`, `INSERT`, and `UPDATE`
4. The primary is killed with `docker kill` (simulating hardware failure)
5. A new client session attempts `COMMIT`; no database promotion is initiated
6. The script prints query responses for inspection

## Expected Output

- Steps 1-3: Cluster startup and separate SQL requests are attempted
- Step 4: Primary is killed
- Later SQL may fail while no primary is available. A successful standalone
  `COMMIT` would not prove recovery of the earlier sessions.
- The summary reports elapsed time, not a verified recovery or durability result.

## What to Look For

- Inspect raw SQL errors, persisted rows, and `GET /topology` rather than the
  presentation text. `/nodes` roles alone do not establish database promotion.
- A real recovery test needs one persistent client session, verified replication,
  external promotion/fencing, and observation of the new leader through a
  [compatible topology provider](../../docs/topology-providers.md).

## Cleanup

The demo cleans up automatically on exit. To clean up manually:

```bash
docker compose down -v --remove-orphans
```
