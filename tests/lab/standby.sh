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
gosu postgres pg_basebackup --host=127.0.0.1 --port="$PRIMARY_PORT" --username=lab \
  --pgdata="$PGDATA" --wal-method=stream --write-recovery-conf --no-password
cat >> "$PGDATA/postgresql.auto.conf" <<CONF
primary_conninfo = 'host=127.0.0.1 port=$PRIMARY_PORT user=lab application_name=$STANDBY_NAME'
hot_standby = on
listen_addresses = '127.0.0.1'
port = $PORT
CONF
exec gosu postgres postgres -D "$PGDATA"
