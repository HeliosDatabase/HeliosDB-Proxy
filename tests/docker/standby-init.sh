#!/bin/sh
# Bring up a PostgreSQL standby via pg_basebackup from $PRIMARY_HOST.
# Runs in place of the default postgres entrypoint so we can set up
# replication BEFORE the server accepts connections on this node.

set -eu

PGDATA=/var/lib/postgresql/data
export PGPASSWORD=repl

# Fresh data dir — this container is not meant to survive; all state
# lives in the primary.
rm -rf "$PGDATA"
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"

# Wait for primary to accept connections (depends_on: healthy should
# cover it, but add a retry loop defensively).
until su postgres -c "pg_isready -h $PRIMARY_HOST -U repl" >/dev/null 2>&1; do
  echo "waiting for $PRIMARY_HOST ..."
  sleep 1
done

# Base backup streaming replication setup.
su postgres -c "
  pg_basebackup \
    --host='$PRIMARY_HOST' \
    --username=repl \
    --pgdata='$PGDATA' \
    --wal-method=stream \
    --write-recovery-conf \
    --no-password \
    --progress \
    --verbose
"

# Tell the standby how to identify itself to the primary (this becomes
# the `application_name` on the replication slot).
cat >> "$PGDATA/postgresql.auto.conf" <<-CONF
  primary_conninfo = 'host=$PRIMARY_HOST port=5432 user=repl password=repl application_name=$STANDBY_APPLICATION_NAME'
  hot_standby = on
  listen_addresses = '*'
CONF

# Re-populate pg_hba from the primary's hba already — pg_basebackup
# replicates the hba file — so plain client connections work from any
# host in the test network.

exec su postgres -c "postgres -D $PGDATA"
