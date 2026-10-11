#!/bin/bash
# Live [auto_promote] test. Starts its OWN disposable PG 18.4 primary+standby on
# host net (ports 35501/35502; proxy 35500, admin 35590) and removes them on exit.
# Never touches the shared backend at 25433. Needs Docker. Scenarios: primary alive
# (no action), fence fails (no promotion), fence ok (promoted + write via proxy).
set -u
BIN=${1:?usage: auto-promote-test.sh <proxy-binary> (built with --features postgres-topology)}
W=$(mktemp -d); IMG=postgres:18.4-bookworm; LBL=hdb-autopromote-test
PP=35501; SP=35502; LP=35500; AP=35590
PX=""
killpx(){ [ -n "$PX" ] && kill $PX 2>/dev/null; PX=""; }
cleanup(){ killpx; docker rm -f ap-primary ap-standby >/dev/null 2>&1; }
trap cleanup EXIT
cleanup; sleep 1
psqlc(){ docker run --rm --network host -e PGPASSWORD= $IMG psql -h 127.0.0.1 -U postgres -tA "$@"; }
docker run -d --name ap-primary --label $LBL --network host -e POSTGRES_HOST_AUTH_METHOD=trust $IMG \
  postgres -p $PP -c wal_level=replica -c max_wal_senders=5 -c listen_addresses=127.0.0.1 >/dev/null
for i in $(seq 30); do psqlc -p $PP -c 'select 1' >/dev/null 2>&1 && break; sleep 1; done
docker exec ap-primary bash -c 'echo "host replication all 127.0.0.1/32 trust" >> /var/lib/postgresql/18/docker/pg_hba.conf 2>/dev/null || echo "host replication all 127.0.0.1/32 trust" >> $PGDATA/pg_hba.conf; psql -p '$PP' -U postgres -c "select pg_reload_conf()"' >/dev/null
docker run -d --user postgres --name ap-standby --label $LBL --network host -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/tmp/sb $IMG \
  bash -c "pg_basebackup -h 127.0.0.1 -p $PP -U postgres -D /tmp/sb -R -X stream && chmod 700 /tmp/sb && exec postgres -D /tmp/sb -p $SP -c listen_addresses=127.0.0.1" >/dev/null
for i in $(seq 40); do [ "$(psqlc -p $SP -c 'select pg_is_in_recovery()' 2>/dev/null)" = t ] && break; sleep 1; done
echo "standby in recovery: $(psqlc -p $SP -c 'select pg_is_in_recovery()')"
psqlc -p $PP -c "create table t(i int); insert into t values (1)" >/dev/null
sleep 2; echo "standby sees row: $(psqlc -p $SP -c 'select count(*) from t')"

cfg(){ cat > $W/proxy-$1.toml <<EOT
listen_address = "127.0.0.1:$LP"
admin_address = "127.0.0.1:$AP"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 5
[health]
check_interval_secs = 1
check_timeout_secs = 2
failure_threshold = 2
success_threshold = 1
[topology]
provider = "postgres"
poll_interval_secs = 1
lease_timeout_secs = 3
[auto_promote]
enabled = true
down_after_secs = 4
check_interval_secs = 1
allow_unknown_lag = true
promote_timeout_secs = 20
max_attempts = 1
$2
[[nodes]]
host = "127.0.0.1"
port = $PP
role = "primary"
[[nodes]]
host = "127.0.0.1"
port = $SP
role = "standby"
EOT
}
startproxy(){ killpx; sleep 1; $BIN -c $W/proxy-$1.toml >$W/proxy-$1.log 2>&1 & PX=$!; sleep 3; }
promo(){ curl -s http://127.0.0.1:$AP/api/promotion; echo; }

# Scenario 0: primary alive, nothing may happen.
cfg alive 'fence_command = ["true"]'; startproxy alive; sleep 8
echo "[S0 primary alive] $(promo)"; echo "standby recovery: $(psqlc -p $SP -c 'select pg_is_in_recovery()')"

# Scenario B: fence fails => no promotion.
cfg fencefail 'fence_command = ["false"]'; startproxy fencefail
docker stop -t 2 ap-primary >/dev/null; sleep 14
echo "[B fence fails] $(promo)"; echo "standby recovery: $(psqlc -p $SP -c 'select pg_is_in_recovery()')"

# Scenario A: fence ok => promoted, writes via proxy work.
rm -f $W/fenced; cfg ok "fence_command = [\"touch\", \"$W/fenced\"]"; startproxy ok; sleep 14
echo "[A fence ok] $(promo)"; echo "standby recovery: $(psqlc -p $SP -c 'select pg_is_in_recovery()')"; ls $W/fenced
for i in 1 2 3 4 5; do
  out=$(docker run --rm --network host $IMG psql -h 127.0.0.1 -p $LP -U postgres -tA -c "insert into t values (2) returning i" 2>&1) && break; sleep 2
done
echo "[A write via proxy] $out"
echo "rows on promoted: $(psqlc -p $SP -c 'select count(*) from t')"
echo "---- proxy log (auto_promote)"; grep -h auto_promote $W/proxy-*.log | cut -c1-260

[ "$(psqlc -p $SP -c "select pg_is_in_recovery()")" = f ] && echo "RESULT: PASS" || { echo "RESULT: FAIL"; exit 1; }
