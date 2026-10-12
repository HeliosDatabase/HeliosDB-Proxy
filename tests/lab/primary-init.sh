#!/bin/bash
# docker-entrypoint-initdb.d hook on pg-a: allow physical replication from the
# lab network. Client access is already `trust` (POSTGRES_HOST_AUTH_METHOD);
# `all` in pg_hba does not match replication connections, so add that line.
set -eu
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_reload_conf()"
# For the cache-logical scenario (C-02 slice 2): the publication the proxy's
# logical invalidation source decodes. Harmless for the other scenarios.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE PUBLICATION heliosdb_proxy_cache FOR ALL TABLES"
