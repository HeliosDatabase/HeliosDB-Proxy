#!/bin/sh
# Runs under the primary container's `docker-entrypoint-initdb.d`
# hook, after initdb but before the server accepts client traffic.
# Creates a replication role + matching pg_hba entries so the standbys
# can connect, and a small benchmark schema pgbench will populate.

set -eu

# Replication role (distinct from the application role so we can revoke
# later without losing app access).
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-SQL
  CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD 'repl';
SQL

# Append pg_hba entries AFTER the defaults so we can accept replication
# traffic and plain connections from any host in the test network.
cat >> "${PGDATA}/pg_hba.conf" <<-HBA
  # HeliosProxy integration-test cluster
  host    replication     repl        0.0.0.0/0       md5
  host    all             all         0.0.0.0/0       md5
HBA

# pgbench pre-creates its own tables on --initialize, so nothing more to
# do schema-wise. Reload so the hba change takes effect.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_reload_conf()"
