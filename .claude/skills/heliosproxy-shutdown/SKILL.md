---
name: heliosproxy-shutdown
description: Stop the proxy gracefully. SIGTERM triggers a drain — wait for active sessions to close, then exit. Use when the user says "stop the proxy", "shutdown", "drain", "restart for config change", or asks why a `kill` left clients hanging.
allowed-tools: Bash(kill *), Bash(systemctl *), Bash(curl *), Bash(docker *)
related: [heliosproxy-overview, heliosproxy-start, heliosproxy-config, heliosproxy-health]
---

# Shutdown HeliosProxy

The proxy treats SIGTERM (and SIGINT, used by Ctrl-C) as a graceful
drain signal. New connections are refused, existing sessions are
allowed to finish. Read this before you `kill -9` something.

## When to use

- Clean stop (any planned shutdown)
- Restart-for-config-reload (there's no SIGHUP — see `heliosproxy-config`)
- Killing a stuck dev instance
- Tearing down a demo / test fixture

🟠 Mutating — closes active client sessions

## Drain semantics

1. SIGTERM (or SIGINT) received.
2. Listener stops accepting new client connections (PG port).
3. Active sessions are allowed to complete their current
   transaction. Sessions in `Idle` state get an immediate close.
4. Once `/sessions` count reaches zero (or the configured
   `shutdown_grace_secs` elapses), the process exits.

The admin port is the last thing closed — `/health` keeps responding
during drain so a load balancer can de-register the node before
clients see refused connections.

## Surfaces

| Surface | When to use |
|---|---|
| `kill -TERM <pid>` | Manual / scripts |
| `systemctl stop heliosproxy` | systemd — uses SIGTERM by default |
| `docker stop <container>` | Container — sends SIGTERM, then SIGKILL after 10 s |
| Ctrl-C (foreground) | Dev / interactive runs |

## Recipes

### Recipe 1: Graceful stop (typical)

```bash
sudo systemctl stop heliosproxy
```

Or, manually:

```bash
pkill -TERM heliosdb-proxy
# observe the drain
watch -n 1 'curl -s http://localhost:9090/sessions'
# … active_sessions: 12 → 8 → 3 → 0 → connection refused
```

When `/sessions` returns connection-refused (admin port closed),
the process has fully exited.

### Recipe 2: Wait-bounded stop

```bash
timeout 30s pkill -TERM heliosdb-proxy
# if still running after 30s, escalate
pgrep -x heliosdb-proxy && pkill -KILL heliosdb-proxy
```

systemd does the same with `TimeoutStopSec=30s` — see
`heliosproxy-start`.

### Recipe 3: Restart for config change

```bash
sudo systemctl restart heliosproxy
journalctl -u heliosproxy -n 50 -f
```

systemd issues SIGTERM, waits for drain (or `TimeoutStopSec`),
then re-launches with the new config. New sessions hit the new
config; in-flight sessions see the old config until they exit.

### Recipe 4: Force-stop a stuck dev instance

```bash
pgrep -x heliosdb-proxy            # find PIDs
kill -KILL $(pgrep -x heliosdb-proxy)   # nuclear, no drain
```

Use only when graceful failed (typically `kill -TERM` already sat
for `>2× shutdown_grace_secs`). Force-kill drops in-flight queries
on the floor — clients see "server closed the connection" mid-result.

### Recipe 5: Docker / Compose

```bash
docker stop heliosproxy        # SIGTERM, then SIGKILL after 10s default
docker stop -t 30 heliosproxy  # extend grace to 30s
docker compose down             # stops + removes containers
docker compose down -v          # also remove volumes (data loss!)
```

For demos with state worth keeping, prefer `docker compose stop`
over `down`.

### Recipe 6: Verify it's actually gone

```bash
pgrep -x heliosdb-proxy && echo "still running" || echo "stopped"
curl -sf http://localhost:9090/health || echo "admin port closed"
ss -tnlp | grep -E ':6432|:9090'    # Linux: confirm no listeners
```

## Pitfalls

- **`kill -9` skips drain.** In-flight queries die with `connection
  reset`; the application sees that as a backend crash even though
  the backend was fine. Almost always wrong outside dev.
- **Long-running idle sessions stretch the drain indefinitely.**
  If your app pool keeps idle sessions for hours, set
  `[server] shutdown_grace_secs = 60` to cap drain. Anything still
  active after that gets closed forcibly.
- **`systemctl reload` does nothing.** There's no SIGHUP handler.
  Use `restart`. If you typed `reload` and nothing changed, that's
  why.
- **Docker default grace is 10 s.** Production deploys should pass
  `--stop-timeout 30` or set it in the compose file:
  `stop_grace_period: 30s`. Without it, you'll see corrupted
  failover-replay journals if `ha-tr` is on and a flush was in
  flight.
- **The admin port is open during drain.** That's intentional —
  load balancers need `/health` to flip to "ready: false" before
  the listener actually closes. Don't interpret a successful
  `/health` mid-drain as "still serving traffic"; check `/health/ready`
  which returns 503 once draining.

## See also

- `heliosproxy-start` — what comes after a stop
- `heliosproxy-config` — there's no live reload; restart is the answer
- `heliosproxy-health` — `/health/ready` flips during drain
- Code: [`src/server.rs:shutdown`](../../src/server.rs) — drain implementation
- Code: [`src/main.rs`](../../src/main.rs) — signal handlers
