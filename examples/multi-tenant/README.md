# Multi-Tenant Routing

Demonstrates how HeliosProxy isolates multiple tenants behind a single proxy endpoint, each with independent connection pools, rate limits, and query permissions.

## How Tenant Identification Works

When a client connects, the proxy extracts a tenant identifier from the connection. Several methods are supported:

| Method | Example Connection | Tenant ID |
|--------|-------------------|-----------|
| **Username prefix** | `psql -U acme.appuser` | `acme` |
| **HTTP header** | `X-Tenant-Id: acme` | `acme` |
| **JWT claim** | JWT with `{"tenant_id": "acme"}` | `acme` |
| **Database name** | `psql -d acme_db` | `acme` |

This example uses **username prefix** with `.` as the separator.

## Isolation Strategies

### Schema Isolation (Tenant: Acme Corp)

All tenant data lives in a shared database under a dedicated schema.

```
Database: shared_db
  Schema: acme      <-- Acme Corp's tables
  Schema: public    <-- shared/system tables
```

The proxy sets `search_path = acme` before forwarding queries, so the application does not need to qualify table names.

```bash
# Connect as Acme Corp
PGPASSWORD=apppass psql -h localhost -p 6432 -U acme.appuser -d shared_db

# These queries hit the "acme" schema automatically:
# SELECT * FROM orders;        -->  SELECT * FROM acme.orders;
# INSERT INTO products (...)   -->  INSERT INTO acme.products (...)
```

### Database Isolation (Tenant: WidgetCo)

Each tenant gets a completely separate database. The proxy routes the connection to the correct database based on the tenant's configuration.

```bash
# Connect as WidgetCo
PGPASSWORD=apppass psql -h localhost -p 6432 -U widgetco.appuser -d widgetco_db
```

## Per-Tenant Resource Limits

Each tenant has independent guardrails:

| Resource | Acme Corp | WidgetCo |
|----------|-----------|----------|
| Max connections | 50 | 20 |
| Queries/second | 1000 | 200 |
| Max query duration | 60s | 30s |
| Max result size | 100 MB | 50 MB |
| DDL allowed | Yes | No |
| Burst multiplier | 2.0x | 1.5x |
| Dedicated pool | No (shared) | Yes |

When a tenant exceeds its QPS limit, the proxy returns an error rather than queueing the request. The burst multiplier allows short spikes above the steady-state limit.

## Connection Pool Behavior

- **Shared pool** (`dedicated_pool = false`): Acme connections share the global pool. Efficient for many tenants with moderate load.
- **Dedicated pool** (`dedicated_pool = true`): WidgetCo gets its own pool. Guarantees connection availability but uses more backend resources.

## Permissions

The proxy enforces query-level permissions before forwarding to the backend:

```
Acme Corp:
  SELECT, INSERT, UPDATE, DELETE -- allowed
  CREATE TABLE, ALTER, DROP      -- allowed (allow_ddl = true)
  EXPLAIN ANALYZE                -- allowed

WidgetCo:
  SELECT, INSERT, UPDATE, DELETE -- allowed
  CREATE TABLE, ALTER, DROP      -- BLOCKED (allow_ddl = false)
  EXPLAIN ANALYZE                -- allowed
```

Blocked operations return an error at the proxy layer without reaching the database.

## Admin API

```bash
# List all configured tenants
curl http://localhost:9090/tenants | jq .

# View a specific tenant's current metrics
curl http://localhost:9090/tenants/acme/metrics | jq .

# View global metrics (aggregated across tenants)
curl http://localhost:9090/metrics | jq .
```

## Adding a New Tenant

Add a new `[[multi_tenancy.tenants]]` block to `proxy.toml` and reload the proxy:

```toml
[[multi_tenancy.tenants]]
id   = "newclient"
name = "New Client Inc"

[multi_tenancy.tenants.isolation]
strategy      = "schema"
database_name = "shared_db"
schema_name   = "newclient"

[multi_tenancy.tenants.pool]
max_connections = 10
min_idle        = 1
dedicated_pool  = false

[multi_tenancy.tenants.rate_limits]
qps_limit      = 100
max_connections = 10

[multi_tenancy.tenants.permissions]
allowed_operations = ["SELECT", "INSERT", "UPDATE", "DELETE"]
read_only          = false
allow_ddl          = false
```

## Configuration Reference

See `proxy.toml` in this directory for the full annotated configuration. Key sections:

- `[multi_tenancy]` -- global multi-tenancy settings
- `[multi_tenancy.identification]` -- how tenant IDs are extracted
- `[[multi_tenancy.tenants]]` -- per-tenant configuration blocks
- `[multi_tenancy.tenants.isolation]` -- data isolation strategy
- `[multi_tenancy.tenants.pool]` -- per-tenant connection pool
- `[multi_tenancy.tenants.rate_limits]` -- per-tenant rate limits
- `[multi_tenancy.tenants.permissions]` -- per-tenant query permissions
