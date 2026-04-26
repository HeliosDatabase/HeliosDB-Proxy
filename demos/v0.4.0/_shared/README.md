# Shared assets — v0.4.0 demos

These files are pulled into multiple per-feature demos to avoid
duplication. Each demo's `docker-compose.yml` references them via
relative paths.

| file | what |
|---|---|
| `proxy.base.toml` | Minimal proxy config with admin port 9090, listen 6432, one PostgreSQL 17 backend at `pg-primary:5432`. Per-demo configs `include` (or override) this. |
| `init.sql` | Sample schema (`users` + `orders` + `events`) + 1 000 rows, used by every demo that needs a non-trivial workload. |
| `wait-for.sh` | Polls a TCP port until it accepts connections — used by demo scripts to wait for proxy/PG to come up. |

## PostgreSQL vs HeliosDB

Every demo uses **`postgres:17-alpine`** as the default backend so
`docker compose up` works on a fresh laptop with no extra build
steps. To run any demo against **HeliosDB-Lite** instead, swap the
backend service in `docker-compose.yml`:

```yaml
pg-primary:
  # was: image: postgres:17-alpine
  build:
    context: ../../../../Lite          # path to your HeliosDB-Lite checkout
    dockerfile: tests/docker/Dockerfile.ha
  environment:
    HELIOSDB_ROLE: primary
```

The wire protocol is identical — HeliosProxy speaks PostgreSQL on
both sides — so no proxy or plugin changes are needed.
