# Demo 1 — Anomaly Detection (T3.1)

**Feature flag:** `anomaly-detection`
**Module brief:** [website-brief-v0.4.0.md §Module 1](../../../docs/website-brief-v0.4.0.md)

## UVP

> Catch SQL injection attempts, credential-stuffing bursts, and rate
> spikes from the same vantage point that already sees every query —
> the proxy. No SIEM in front of your DB.

## Use cases

- **Audit + SOC integration.** Stream `/anomalies` to your SIEM of
  record. Detections carry the SQL excerpt + matched patterns + the
  source tenant — enough for triage.
- **Day-zero defence in depth.** Even with a WAF in front of your
  app, proxy-level detection catches injection that reaches the DB
  via pickled cookies, server-side template renders, or anything
  the WAF didn't normalise.
- **Production-shape observability.** Novel-query events fingerprint
  every new query shape — useful when you want to know what new
  workload an app deploy just introduced.

## What this demo shows

Three detector families fire against a single PostgreSQL backend:

1. **SQL injection** — a classic `OR 1=1` payload + a stacked-query
   `; DROP TABLE` payload + a `pg_sleep` time-based blind probe.
   Each lands in `/anomalies` with its matched pattern labels.
2. **Credential stuffing** — a 12-failure burst from the same
   client IP triggers Critical severity at the 10-failure
   threshold.
3. **Novel query** — every new query fingerprint generates an
   informational event. Useful as a regression signal.

## Run it

```bash
cd demos/v0.4.0/01-anomaly-detection
./demo.sh
```

The script:

1. Brings up `postgres:17-alpine` + the proxy with
   `--features anomaly-detection`.
2. Fires the three attack payloads + the auth burst via `psql`.
3. Polls `GET /anomalies?limit=50` and pretty-prints the events.
4. Tears everything down on exit.

Expected output:

```text
=== Anomaly Detection Demo ===
[1/4] Starting services...
[2/4] Waiting for proxy admin port (9090)...
[3/4] Firing attack payloads...
   - classic OR injection
   - stacked-query injection
   - time-based blind probe
   - 12 failed-auth attempts from 10.0.0.99
[4/4] Polling /anomalies:

  ┌─ critical  sql_injection           classic_or_payload, comment_escape
  │  excerpt: SELECT * FROM users WHERE id = 1 OR 1=1 --
  ├─ critical  sql_injection           stacked_queries
  │  excerpt: SELECT 1; DROP TABLE events; --
  ├─ warning   sql_injection           time_based_blind
  │  excerpt: SELECT * FROM users WHERE id = pg_sleep(5)
  ├─ critical  auth_burst              user=alice ip=10.0.0.99 failures=12
  └─ info      novel_query             SELECT 1
```

## Try it yourself

After `./demo.sh up`, the proxy stays running. Probe it with your
own payloads:

```bash
psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT * FROM users WHERE id = 1 UNION SELECT NULL,NULL,NULL,NULL,NULL,NULL"
curl -s http://localhost:9090/anomalies?limit=10 | jq .
```

Tear down with `./demo.sh down`.

## HeliosDB compatibility

Swap `postgres:17-alpine` for HeliosDB-Lite (see
[`_shared/README.md`](../_shared/README.md)). Detector behaviour is
identical — it operates on the SQL the proxy sees, not on the
backend's response.
