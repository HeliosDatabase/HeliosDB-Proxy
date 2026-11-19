---
name: heliosproxy-start
description: Start the HeliosProxy daemon — `heliosdb-proxy --config proxy.toml` or all-CLI-args mode. Use when the user says "start the proxy", "run heliosproxy", "set up the daemon", "systemd unit", or hits "connection refused" against the admin port.
allowed-tools: Bash(heliosdb-proxy *), Bash(systemctl *), Bash(curl *), Bash(docker *), Read
related: [heliosproxy-overview, heliosproxy-config, heliosproxy-shutdown, heliosproxy-health]
---

# Start HeliosProxy

Daemonize the proxy. Two invocation styles: TOML config file (preferred)
or CLI flags (good for one-liner demos and CI).

## When to use

- First-time bring-up after `cargo install`
- Restarting after a config change that SIGHUP does not apply (see Pitfalls)
- Running the proxy under systemd / Docker / Kubernetes
- Background-launching as part of a test fixture

🟠 Mutating — opens listen ports, mutates network state on the host

## Surfaces

| Surface | When to use |
|---|---|
| `heliosdb-proxy --config X.toml`     | Production / repeatable |
| `heliosdb-proxy --primary host:port` | Quick demo / smoke-test |
| systemd unit                          | Long-lived host deploy |
| Docker `entrypoint`                  | Container deploy |
| `demo.sh up`                          | Demo fixtures |

## Recipes

### Recipe 1: Foreground with a config file

```bash
heliosdb-proxy --config proxy.toml
```

The proxy logs to stdout and listens on the addresses defined in
`proxy.toml` (default PG `6432`, admin `9090`). Ctrl-C to stop —
SIGINT triggers the same drain as SIGTERM (see `heliosproxy-shutdown`).

### Recipe 2: Background (no config file, all CLI flags)

```bash
heliosdb-proxy \
  --listen 0.0.0.0:6432 \
  --admin 0.0.0.0:9090 \
  --primary pg-primary:5432 \
  --standby pg-replica-1:5432,pg-replica-2:5432 \
  --log-level info \
  >/var/log/heliosproxy.log 2>&1 &
```

Useful for one-shot CI fixtures. For more than smoke-testing, use a
config file (Recipe 1) — CLI flags can't express feature blocks
(plugins, anomaly thresholds, edge config, etc.).

### Recipe 3: systemd unit

```ini
# /etc/systemd/system/heliosproxy.service
[Unit]
Description=HeliosProxy
After=network.target

[Service]
ExecStart=/usr/local/bin/heliosdb-proxy --config /etc/heliosproxy/proxy.toml
Restart=on-failure
User=heliosproxy
Group=heliosproxy
TimeoutStopSec=75s

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now heliosproxy
sudo systemctl status heliosproxy
journalctl -u heliosproxy -f
```

`TimeoutStopSec=75s` gives the drain (SIGTERM → each session ends
with FATAL `57P01` once nothing of it is in flight, at most
`shutdown_drain_timeout_secs`, default 60) time to finish before
systemd hard-kills. Add `ExecReload=/bin/kill -HUP $MAINPID` for
`systemctl reload`.

### Recipe 4: Docker / Compose

```yaml
# docker-compose.yml
services:
  heliosproxy:
    image: ghcr.io/heliosdatabase/hdb-heliosdb-proxy:3.0.0
    stop_grace_period: 70s   # longer than shutdown_drain_timeout_secs
    volumes:
      - ./proxy.toml:/etc/heliosproxy/proxy.toml:ro
    ports:
      - "6432:6432"   # PG
      - "9090:9090"   # admin
    command: ["--config", "/etc/heliosproxy/proxy.toml"]
    depends_on: [pg-primary]
```

```bash
docker compose up -d heliosproxy
docker compose logs -f heliosproxy
```

### Recipe 5: Verify it's listening

```bash
curl -s http://localhost:9090/health        # {"status":"ok"}
curl -s http://localhost:9090/version | jq  # {"version":"3.0.0",...}
psql -h localhost -p 6432 -U postgres -c "SELECT 1"
```

If admin returns 200 but PG returns "connection refused" — the
proxy is up but the PG listener didn't bind. Check the log for
`bind failed` lines and confirm `--listen` / the top-level `listen_address`.

### Recipe 6: Environment variables

There is no generic env-var override. `proxy.toml` values can reference
the environment (`${NAME}`, `${NAME:-default}`, expanded before parsing),
`RUST_LOG` controls logging, and `HELIOS_DRAIN_TIMEOUT_SECS` overrides
`shutdown_drain_timeout_secs`:

```bash
ADMIN_TOKEN=... RUST_LOG=heliosdb_proxy=debug \
heliosdb-proxy --config proxy.toml   # proxy.toml: admin_token = "${ADMIN_TOKEN}"
```

## Pitfalls

- **SIGHUP reloads `proxy.toml`, but not every section.** Keys read
  only at startup are kept (the log names them) and need a restart:
  SIGTERM (drain), then start with the new config, or a SIGUSR2
  handoff to a new process. See `heliosproxy-shutdown`.
- **`Address already in use` on 6432 or 9090.** Another process
  is bound. `lsof -i :6432` to identify. Common culprit: a previous
  `heliosdb-proxy` that didn't fully exit (kill -9 it after
  graceful timeout).
- **`--admin 127.0.0.1:9090` (the default) makes admin local-only.**
  A non-loopback admin address without `admin_token` (or
  `--admin-token`) is refused at startup unless
  `admin_allow_insecure = true`.
- **`--primary` / `--standby` apply only without `--config`.** With a
  config file, `[[nodes]]` is the whole node list.
- **Ctrl-C (SIGINT) drains.** Idle sessions end at once; a session
  inside a transaction is waited for, up to
  `shutdown_drain_timeout_secs`. Press Ctrl-C twice to exit at once.

## See also

- `heliosproxy-config` — the TOML schema your `--config` points at
- `heliosproxy-shutdown` — graceful stop semantics
- `heliosproxy-health` — verify the daemon responds
- `heliosproxy-connect` — first SQL round-trip after start
- Code: [`src/main.rs`](../../src/main.rs) — CLI parsing
- Code: [`src/server.rs:start`](../../src/server.rs) — runtime entry
- Demo: any of [`demos/v0.4.0/`](../../demos/v0.4.0/) — every demo's
  `demo.sh up` shows a real start flow
