# HeliosProxy vs PgBouncer — Failover Comparison Results

**Date:** {{DATE}}
**Concurrency:** {{CONCURRENCY}} workers per proxy

## Scenario

Two identical PostgreSQL 16 clusters (primary + synchronous standby), one
fronted by HeliosProxy (with Transaction Replay), one fronted by PgBouncer
(transaction pooling). Both primaries killed simultaneously during active
workload.

## Results

| Metric              | PgBouncer      | HeliosProxy    |
|---------------------|----------------|----------------|
| Queries attempted   | {{PB_TOTAL}}   | {{HP_TOTAL}}   |
| Successful queries  | {{PB_OK}}      | {{HP_OK}}      |
| Client errors       | {{PB_ERRORS}}  | {{HP_ERRORS}}  |
| Rows in database    | {{PB_ROWS}}    | {{HP_ROWS}}    |
| Rows lost           | {{PB_LOST}}    | {{HP_LOST}}    |
| Max client downtime | {{PB_DOWNTIME}} | {{HP_DOWNTIME}} |

## Analysis

Describe the actual database authority, promotion/fencing procedure, topology
configuration, and measured client results here. Neither proxy initiates a
database promotion in this demo. With static topology and no replacement
primary, HeliosProxy writes can fail until the original primary recovers.
With an external promotion and a compatible provider, it can route to the new
leader and replay eligible work. Unknown `COMMIT` outcomes remain errors.

## Key Takeaway

Connection routing and transaction replay depend on database promotion, fencing,
replication, and replay eligibility. Draw conclusions from the recorded results;
this template does not establish zero downtime or zero data loss.
