#!/bin/bash
# Entrypoint of pg-b / pg-c: clone the primary (127.0.0.1:$PRIMARY_PORT) with
# pg_basebackup and start as a hot standby on 127.0.0.1:$PORT whose
# application_name is $STANDBY_NAME (pg-a lists pg_b as its synchronous
# standby). The data directory lives on tmpfs and dies with the container.
set -eu
mkdir -p "$PGDATA"
chown -R postgres:postgres /var/lib/postgresql
chmod 700 "$PGDATA"
until gosu postgres pg_isready -h 127.0.0.1 -p "$PRIMARY_PORT" -U lab -d lab >/dev/null 2>&1; do
  sleep 1
done
# Two standbys clone at once from a fresh primary; a clone that loses a race
# with WAL recycling is retried from scratch (bounded).
for attempt in 1 2 3 4 5; do
  if gosu postgres pg_basebackup --host=127.0.0.1 --port="$PRIMARY_PORT" --username=lab \
    --pgdata="$PGDATA" --wal-method=stream --write-recovery-conf --no-password; then
    break
  fi
  [ "$attempt" = 5 ] && exit 1
  echo "pg_basebackup attempt $attempt failed; retrying"
  find "$PGDATA" -mindepth 1 -delete
  sleep 2
done
# The base backup carries pg-a's postgresql.auto.conf. A standby must not keep
# its synchronous_standby_names (a promoted pg-b would wait forever for a
# standby named after itself), and hot standby needs connection/sender limits
# at least as high as the primary's (pg-a runs max_connections=200).
cat >> "$PGDATA/postgresql.auto.conf" <<CONF
primary_conninfo = 'host=127.0.0.1 port=$PRIMARY_PORT user=lab application_name=$STANDBY_NAME'
hot_standby = on
listen_addresses = '127.0.0.1'
port = $PORT
synchronous_standby_names = ''
max_connections = 200
max_wal_senders = 10
max_replication_slots = 10
wal_level = logical
CONF
exec gosu postgres postgres -D "$PGDATA"
