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

PgBouncer is a connection pooler -- it has no awareness of PostgreSQL
replication topology and cannot fail over to a standby. When the primary
dies, all in-flight transactions fail and clients see errors until the
primary is manually restored.

HeliosProxy detects the primary failure, promotes the standby, and replays
any in-flight transactions. Clients experience a brief pause but see zero
(or near-zero) errors.

## Key Takeaway

Connection pooling alone does not provide high availability. Transaction
Replay is what makes the difference between "errors during failover" and
"zero downtime failover."
