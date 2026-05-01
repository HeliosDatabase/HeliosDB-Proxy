---
name: heliosproxy-time-travel
description: Replay a window of the transaction journal onto a target backend via `POST /api/replay`. Validate failover, hydrate staging from prod, debug "what happened yesterday at 14:32". Use when the user says "replay", "time-travel", "/api/replay", "hydrate staging", "rerun the last hour against staging".
allowed-tools: Bash(curl *), Bash(date *), Bash(jq *)
related: [heliosproxy-overview, heliosproxy-shadow-execute, heliosproxy-switchover]
---

# Time-Travel Replay

Replay any window of the proxy's transaction journal against a target
backend. Use it to hydrate a staging database from prod, validate
that failover replay worked, or reproduce an incident that landed
between two timestamps.

Requires the `ha-tr` feature compiled in. The journal is enabled
when `[ha]` is configured (see `heliosproxy-config`).

## When to use

- Hydrating staging with the last N hours of prod writes
- Re-applying writes that were buffered during a failover but not
  delivered (rare — the failover machinery normally handles this)
- "What did the system do between 14:30 and 14:35 yesterday?"
- Validating a PG-version migration end-to-end (replay last week of
  writes onto a new-version standby)

🟠 Mutating against the **target** — production-safe only against
a target you control (typically staging).

## Surfaces

| Endpoint | Method | Purpose |
|---|---|---|
| `POST /api/replay` | trigger replay of `[from, to]` window onto a target |

## Request shape

```json
{
  "from":            "2026-05-01T13:00:00Z",
  "to":              "2026-05-01T14:00:00Z",
  "target_host":     "staging-pg.internal",
  "target_port":     5432,
  "target_user":     "postgres",
  "target_password": "...",
  "target_database": "demo"
}
```

Timestamps are RFC 3339, UTC recommended. The window is inclusive
on both ends. Target credentials default to the proxy's startup
credentials if omitted (rarely useful).

## Recipes

### Recipe 1: Replay the last hour onto staging

```bash
curl -s -X POST http://localhost:9090/api/replay \
  -H 'Content-Type: application/json' \
  -d "{
    \"from\":            \"$(date -u -d '1 hour ago' +%FT%TZ)\",
    \"to\":              \"$(date -u +%FT%TZ)\",
    \"target_host\":     \"staging-pg.internal\",
    \"target_port\":     5432,
    \"target_user\":     \"postgres\",
    \"target_password\": \"$STAGING_PG_PASS\",
    \"target_database\": \"demo\"
  }" | jq .
```

```json
{
  "window":           {"from":"2026-05-01T13:00:00Z","to":"2026-05-01T14:00:00Z"},
  "target":           "staging-pg.internal:5432/demo",
  "statements_total": 14237,
  "statements_ok":    14237,
  "statements_error": 0,
  "duration_ms":      28411,
  "first_xid":        "0/A1F4001",
  "last_xid":         "0/A23FE08"
}
```

`statements_error > 0` indicates the target schema diverged from
the source's at some point in the window — you'll need to run the
relevant DDL on the target first.

### Recipe 2: Replay a precisely-bounded incident

```bash
curl -s -X POST http://localhost:9090/api/replay \
  -H 'Content-Type: application/json' \
  -d '{
    "from":            "2026-05-01T14:32:00Z",
    "to":              "2026-05-01T14:34:00Z",
    "target_host":     "scratch-pg.internal",
    "target_port":     5432,
    "target_user":     "postgres",
    "target_password": "scratch",
    "target_database":"reproduce_incident"
  }'
```

Common pattern: `pg_dump` prod at the timestamp matching `from`,
restore into a scratch DB, then replay onto it. The replay fast-
forwards the scratch DB to exactly `to`'s state.

### Recipe 3: Replay a longer window in chunks

For windows exceeding the journal retention or memory budget, break
into chunks:

```bash
START=2026-05-01T00:00:00Z
END=2026-05-01T06:00:00Z
HOURS=6

for h in $(seq 0 $((HOURS-1))); do
  from=$(date -u -d "$START +${h}hours" +%FT%TZ)
  to=$(date -u   -d "$START +$((h+1))hours" +%FT%TZ)
  echo "replay [$from, $to]"
  curl -s -X POST http://localhost:9090/api/replay \
    -H 'Content-Type: application/json' \
    -d "{\"from\":\"$from\",\"to\":\"$to\",\"target_host\":\"staging\",
         \"target_port\":5432,\"target_user\":\"pg\",\"target_password\":\"x\",
         \"target_database\":\"demo\"}" \
    | jq '{window, statements_total, statements_error, duration_ms}'
done
```

Replays are sequential at the proxy — they don't share threads.
Run chunks serially.

### Recipe 4: Validate a PG-version migration

```bash
# 1. stand up a new-version standby
# 2. snapshot the source DB at the journal's start LSN
# 3. replay last N hours onto the new-version standby
curl -s -X POST http://localhost:9090/api/replay \
  -H 'Content-Type: application/json' \
  -d "{...target_host = pg17-standby...}"
# 4. for ongoing diff validation, see heliosproxy-shadow-execute
```

## Pitfalls

- **Replay is NOT idempotent.** Running the same window twice on
  the same target will apply each INSERT/UPDATE/DELETE twice (the
  journal stores statement-level entries, not idempotent CRDTs).
  If a replay fails partway, snapshot-restore the target before
  retrying.
- **Schema must match the source's state at `from`.** If the
  source ran a DDL inside the window, that DDL is in the journal
  and replay applies it. If the source ran DDL **before** `from`
  that the target is missing, the replay errors out on the first
  affected statement.
- **`target_password` is sent in the JSON body in cleartext.**
  Always TLS-terminate the admin port, or run replay only over a
  trusted network. Don't log the request body.
- **Replay window must fit in journal retention.** Default
  `[ha].journal_retention = "24h"`. Older windows return 404 with
  "journal window evicted." Increase retention or rely on an
  external WAL archive.
- **503 means the feature is off.** `ha-tr` not compiled in =
  `/api/replay` unavailable. Rebuild with `--features ha-tr`.
- **Replay against the live primary is supported but rarely what
  you want.** It re-applies writes — almost always against staging
  / scratch is correct.

## See also

- `heliosproxy-shadow-execute` — for ongoing dual-execute + diff
- `heliosproxy-switchover` — replay's role during cutover
- `heliosproxy-config` — `[ha]` journal retention
- Demo: [`demos/v0.4.0/10-admin-rest/`](../../demos/v0.4.0/10-admin-rest/) — `/api/replay` curl
- Code: [`src/admin.rs`](../../src/admin.rs) — endpoint impl
- Code: [`src/replay/`](../../src/replay/) — replay engine
- Code: [`src/transaction_journal.rs`](../../src/transaction_journal.rs)
