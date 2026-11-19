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
- Restart for a change a `SIGHUP` reload does not apply (see `heliosproxy-config`)
- Killing a stuck dev instance
- Tearing down a demo / test fixture

🟠 Mutating — closes active client sessions

## Drain semantics

1. SIGTERM (or SIGINT) received. SIGUSR2 (binary handoff) runs the
   same drain.
2. Listeners stop accepting: the PG port and the HTTP SQL, MCP and
   GraphQL gateway ports. `/readyz` answers 503 `Proxy is draining`.
3. Active sessions complete their current transaction, extended
   cycle or COPY, then get FATAL `57P01`. Idle sessions get it at
   once. Gateway requests already accepted are answered.
4. Once no session and no gateway request is left (or the configured
   `shutdown_drain_timeout_secs` elapses), the recovery journal is
   flushed and the process exits 0. A second SIGTERM / SIGINT exits
   at once.

The admin port is the last thing closed — `/health` keeps responding
during drain so a load balancer can de-register the node before
clients see refused connections.

## Surfaces

| Surface | When to use |
|---|---|
| `kill -TERM <pid>` | Manual / scripts |
| `systemctl stop heliosproxy` | systemd — uses SIGTERM by default |
| `docker stop <container>` | Container — sends SIGTERM, then SIGKILL after 10 s (raise with `-t`) |
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

systemd does the same with `TimeoutStopSec` — keep it above
`shutdown_drain_timeout_secs` (default 60); see `heliosproxy-start`.

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
for `>2× shutdown_drain_timeout_secs`). Force-kill drops in-flight queries
on the floor — clients see "server closed the connection" mid-result.

### Recipe 5: Docker / Compose

```bash
docker stop heliosproxy        # SIGTERM, then SIGKILL after 10s default
docker stop -t 70 heliosproxy  # grace longer than the 60 s default drain
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
- **A transaction left open stretches the drain to its timeout.**
  Idle sessions are closed at once (FATAL `57P01`), but a session
  inside a transaction is waited for. The top-level key
  `shutdown_drain_timeout_secs` (default 60, env override
  `HELIOS_DRAIN_TIMEOUT_SECS`) caps the drain. Anything still
  active after that gets closed forcibly.
- **`SIGHUP` reloads the config file; it does not restart.** Keys
  that are read at startup only are reported in the log and need a
  `restart`.
- **Docker default grace is 10 s, Kubernetes 30 s; the drain
  allows 60 s.** Pass `--stop-timeout 70` (Compose:
  `stop_grace_period: 70s`; Kubernetes:
  `terminationGracePeriodSeconds: 75`), or lower
  `shutdown_drain_timeout_secs`. Otherwise sessions still in a
  transaction are killed mid-drain and the recovery journal may not
  be flushed.
- **The admin port is open during drain.** That's intentional —
  load balancers need `/health` to flip to "ready: false" before
  the listener actually closes. Don't interpret a successful
  `/health` mid-drain as "still serving traffic"; check `/health/ready`
  which returns 503 once draining.

## See also

- `heliosproxy-start` — what comes after a stop
- `heliosproxy-config` — SIGHUP reloads what it can; startup-only keys need a restart
- `heliosproxy-health` — `/health/ready` flips during drain
- Code: [`src/server/`](../../src/server/) and [`src/gateway_drain.rs`](../../src/gateway_drain.rs) — drain implementation
- Code: [`src/main.rs`](../../src/main.rs) — signal handlers
