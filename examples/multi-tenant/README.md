# Multi-tenant row filtering and admission

This example identifies two known tenants from the PostgreSQL startup parameter
`application_name`, filters their reads and writes on `orders` and `products`,
and applies per-tenant session, active-query and replay-byte limits.

Build with `cargo build --features multi-tenancy` or `all-features`, prepare a
PostgreSQL backend at `127.0.0.1:5432`, then start:

```bash
heliosdb-proxy --config examples/multi-tenant/proxy.toml
psql "host=127.0.0.1 port=6432 user=appuser dbname=shared_db application_name=acme"
psql "host=127.0.0.1 port=6432 user=appuser dbname=shared_db application_name=widgetco"
```

Authenticate the tenant at a trusted application boundary before selecting its
startup parameter. Clients can choose `application_name`; it is an identifier,
not proof of identity. Use database privileges and PostgreSQL RLS as the security
boundary. Proxy row filtering has a deliberately restricted SQL grammar and
refuses statements it cannot safely transform.

The backend tables need a `tenant_id` column, with values `acme` or `widgetco`.
Register every name under which a tenant table can be reached, including schema
qualifications, partitions and inheritance children. The proxy leaves unrelated
tables unchanged. With `unknown_tenant = "refuse"`, statements on tenant tables
from an unidentified session are refused.

| Limit | Acme | WidgetCo |
|---|---:|---:|
| Client sessions | 50 | 20 |
| Active requests | 8 | 4 |
| Active-query wait | 5 s | 5 s |
| Idle permit timeout | 60 s | 60 s |
| Replay bytes | 8 MiB | 8 MiB |

Transactions retain their active-query permit until they finish. A request that
cannot obtain a permit within the configured wait receives SQLSTATE `53300`.
`idle_permit_timeout_ms` terminates an idle transaction or advisory-lock holder;
it does not terminate a stalled COPY. Replay budgets bound replayable transaction
state, not the recovery-grade journal.

The daemon matches the configured startup parameter exactly. `identify_by =
"database"` instead identifies by the database name (the base database for Nano's
`db@branch` form). This example does not select schemas or rewrite usernames.
Per-tenant QPS, DDL permissions and dedicated pools are not configuration keys;
use the documented global rate-limit buckets and backend privileges as needed.

Inspect aggregate metrics through the loopback admin listener:

```bash
curl http://127.0.0.1:9090/metrics
curl http://127.0.0.1:9090/metrics/prometheus
```

Change `tenants` and its per-tenant override maps together. Multi-tenancy settings
are applied at startup: restart or use a SIGUSR2 handoff after editing them. The
exception is the size of a cap or byte budget a tenant already has: a SIGHUP applies
that to the running proxy (a lowered cap ends nothing; the tenant admits nobody new
until it is back under it). Any other change makes SIGHUP keep the running
multi-tenancy settings and log a warning.
See [configuration](../../docs/configuration.md) for the full key reference.
