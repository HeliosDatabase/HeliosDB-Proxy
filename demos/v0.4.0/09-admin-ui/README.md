# Demo 9 — Admin Web UI

**Module brief:** [§Module 9](../../../docs/website-brief-v0.4.0.md)

## UVP

> A working dashboard at `http://<proxy>:9090/` — single embedded
> HTML file, no build step, ten panels covering every v0.4.0
> capability.

## Use cases

- **On-call.** Open the dashboard, see "current primary" + "healthy
  nodes" + "active anomalies" in one view. No Grafana to set up.
- **Post-incident review.** The Anomalies panel + Replay panel
  together let you see what happened *and* replay traffic from
  before the incident against a copy of the DB.
- **Ad-hoc investigation.** SQL panel sends queries through the
  proxy's load-balancer; useful for sanity checks without a
  separate psql session.

## What this demo shows

```bash
cd demos/v0.4.0/09-admin-ui
./demo.sh
```

The script brings up the proxy + PG, then opens the dashboard
in your browser (or prints the URL if no browser is detected):

```text
=== Admin UI Demo ===
[1/2] Starting proxy + Postgres
[2/2] Dashboard at http://localhost:9090/

   Open in your browser. The page auto-refreshes every 5 seconds
   so you can leave it open while exercising the proxy elsewhere.
```

### Panels you'll see

1. **Nodes** — health, failure count, latency, last check
2. **Topology** — current primary, healthy/unhealthy counts,
   last failover (red badge when no primary is healthy)
3. **Plugins** — name, version, hooks, state, invocation count,
   error count (red when > 0)
4. **Anomalies** — recent detections from the anomaly module
5. **Edge Mode** — cache stats, registered edges, manual invalidate
6. **Chaos Mode** — force-unhealthy buttons + active overrides
7. **Shadow Execution** — diff a query across two backends
8. **Time-Travel Replay** — replay a journal window
9. **SQL Console** — ad-hoc query box
10. **Traffic / Cluster** — counters (queries, bytes, sessions)

Try this once the dashboard is open:

```bash
# Generate some load — the Traffic panel should tick up
for i in $(seq 1 200); do
  PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
    -c "SELECT 1" >/dev/null 2>&1 &
done
wait
```

Refresh the dashboard — `Queries processed` should jump to ~200,
`Active sessions` will spike then settle.

## Implementation pointer

`src/admin_ui.html` — the whole dashboard, ~1 200 lines of vanilla
HTML/CSS/JS. Served by `src/admin.rs::handle_connection` at
`GET /` and `GET /ui` via `include_str!`.

## HeliosDB compatibility

Backend-agnostic.
