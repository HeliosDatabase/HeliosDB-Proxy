#!/usr/bin/env bash
# HeliosProxy live test — LDAP authentication (feature: ldap-auth).
#
# Stands up a real OpenLDAP directory in a throwaway container, seeds an OU and
# a user, then runs the gated Rust integration test `ldap_live_search_and_bind`,
# which performs the real search + bind flow through the proxy's auth handler:
#   - correct username/password authenticates (yields an `ldap` identity),
#   - a wrong password is denied (InvalidCredentials),
#   - an unknown user is denied.
#
# Local-only: the directory lives entirely in a container (port-mapped to
# 127.0.0.1) and is removed on exit. No external services.
#
# Usage:  ./ldap-test.sh        (builds + runs the gated test with cargo)
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
NAME=helios-ldap-test
IMG="${LDAP_IMAGE:-osixia/openldap:1.5.0}"
PORT=1389                       # host port -> container 389
ROOT="dc=example,dc=org"
ADMIN_DN="cn=admin,$ROOT"
ADMIN_PW=adminpw
USER=alice
USER_PW=alicepw

PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

cleanup(){ docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "== ldap-auth live test  image=$IMG =="
docker run -d --name "$NAME" -p 127.0.0.1:$PORT:389 \
  -e LDAP_ORGANISATION="Helios Test" \
  -e LDAP_DOMAIN="example.org" \
  -e LDAP_ADMIN_PASSWORD="$ADMIN_PW" \
  "$IMG" >/dev/null || { echo "failed to start LDAP container"; exit 1; }

# Wait for slapd to answer the base DN.
ready=0
for _ in $(seq 1 60); do
  if docker exec "$NAME" ldapsearch -x -LLL -H ldap://127.0.0.1:389 \
        -D "$ADMIN_DN" -w "$ADMIN_PW" -b "$ROOT" -s base dn >/dev/null 2>&1; then ready=1; break; fi
  if ! docker ps --format '{{.Names}}' | grep -q "^$NAME$"; then
    echo "LDAP container exited:"; docker logs "$NAME" 2>&1 | tail -20; exit 1; fi
  sleep 1
done
[ "$ready" = 1 ] || { echo "LDAP never became ready"; docker logs "$NAME" 2>&1 | tail -20; exit 1; }

# Seed an OU + user (deterministic, post-boot ldapadd).
docker exec -i "$NAME" ldapadd -x -H ldap://127.0.0.1:389 -D "$ADMIN_DN" -w "$ADMIN_PW" >/dev/null 2>&1 <<LDIF
dn: ou=users,$ROOT
objectClass: organizationalUnit
ou: users

dn: uid=$USER,ou=users,$ROOT
objectClass: inetOrgPerson
cn: $USER
sn: $USER
uid: $USER
userPassword: $USER_PW
LDIF

if docker exec "$NAME" ldapsearch -x -LLL -H ldap://127.0.0.1:389 -D "$ADMIN_DN" -w "$ADMIN_PW" \
      -b "ou=users,$ROOT" "(uid=$USER)" dn 2>/dev/null | grep -qi "uid=$USER"; then
  ok ldap_directory_ready "(user $USER seeded)"
else
  bad ldap_directory_ready "user not seeded"; docker logs "$NAME" 2>&1 | tail -10; exit 1
fi

export HELIOS_LDAP_URL="ldap://127.0.0.1:$PORT"
export HELIOS_LDAP_BIND_DN="$ADMIN_DN"
export HELIOS_LDAP_BIND_PW="$ADMIN_PW"
export HELIOS_LDAP_BASE="ou=users,$ROOT"
export HELIOS_LDAP_FILTER="(uid={0})"
export HELIOS_LDAP_USER="$USER"
export HELIOS_LDAP_PASS="$USER_PW"

echo "-- running gated Rust test ldap_live_search_and_bind --"
if (cd "$REPO" && cargo test --features ldap-auth --lib ldap_live_search_and_bind -- --nocapture) 2>&1 \
     | tee /tmp/ldap-test-out.log | grep -qE 'test result: ok\. 1 passed'; then
  ok ldap_search_and_bind_authenticates
else
  bad ldap_search_and_bind_authenticates "see /tmp/ldap-test-out.log"; tail -20 /tmp/ldap-test-out.log
fi

echo "== ldap-auth: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
