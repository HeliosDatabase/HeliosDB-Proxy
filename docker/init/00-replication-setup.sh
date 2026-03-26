#!/bin/bash
set -e

# Create replication user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
EOSQL

# Allow replication connections from the Docker network
echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"
echo "host all all all md5" >> "$PGDATA/pg_hba.conf"
