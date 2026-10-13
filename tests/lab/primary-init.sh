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
# pg-b is the synchronous standby. Set here, not on the command line: the
# image's init phase starts its temporary server with the command's -c
# options, and every commit there (CREATE DATABASE included) would wait
# forever for a standby that does not exist yet. ALTER SYSTEM takes effect
# when the real server starts after init.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "ALTER SYSTEM SET synchronous_standby_names = 'pg_b'"
