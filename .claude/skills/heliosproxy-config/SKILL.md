---
name: heliosproxy-config
description: Author or edit `proxy.toml`. Walk through every block (server, nodes, pool, ha, plugins, anomaly, edge, multi-tenancy, etc.). Use when the user asks "how do I configure X", "where do I put Y in proxy.toml", or after pasting a TOML snippet that doesn't load.
allowed-tools: Read, Grep, Glob, Bash(heliosdb-proxy --check-config *), Bash(toml *)
related: [heliosproxy-overview, heliosproxy-start, heliosproxy-install]
---

# Configure HeliosProxy

`proxy.toml` is the single source of truth. Two reference configs
ship in the repo: minimal (`config/proxy.example.toml`) and exhaustive
(`config/proxy.full.toml`). Start from the minimal, copy in only the
blocks you need.

## When to use

- Authoring a new deployment's TOML
- Adding a feature (plugins, edge, anomaly, ha-tr) to an existing one
- Diagnosing "feature returns 503" → usually a missing block
- Migrating a CLI-flags-only setup to a TOML file

🔵 Read-only — editing a file; effect requires a restart

## Top-level blocks

| Block | Purpose | Required |
|---|---|---|
| `[server]`        | listen addresses, log level | ✓ |
| `[[nodes]]`       | each backend (primary/standby/replica) | ✓ (≥1) |
| `[pool]`          | connection-pool sizing, timeouts | ✓ |
| `[pool_mode]`     | Session/Transaction/Statement | optional |
| `[health]`        | check interval, failure threshold | optional |
| `[tls]`           | client + backend TLS | optional |
| `[ha]`            | failover, switchover, TR | feat: `ha-tr` |
| `[plugins]`       | WASM plugin dir, trust root | feat: `wasm-plugins` |
| `[anomaly]`       | rate spike / SQLi / auth-burst thresholds | feat: `anomaly-detection` |
| `[edge]`          | mode = home / edge, registry | feat: `edge-proxy` |
| `[multi_tenancy]` | per-tenant pools and quotas | feat: `multi-tenancy` |
| `[query_cache]`   | L1/L2/L3 cache tiers | feat: `query-cache` |
| `[rate_limit]`    | token bucket / sliding window | feat: `rate-limiting` |

## Recipes

### Recipe 1: Minimal HA config

```toml
[server]
listen_address = "0.0.0.0:6432"
admin_address  = "0.0.0.0:9090"
log_level      = "info"

[[nodes]]
address = "pg-primary:5432"
role    = "primary"
weight  = 1

[[nodes]]
address = "pg-replica-1:5432"
role    = "standby"
weight  = 1

[pool]
max_connections      = 100
idle_timeout_secs    = 600
acquire_timeout_secs = 30

[health]
check_interval_secs = 5
failure_threshold   = 3
```

Sanity-check the file before launching:

```bash
heliosdb-proxy --config proxy.toml --check-config
# OK: 2 nodes, pool=100, health=5s
```

### Recipe 2: Add WASM plugins

```toml
[plugins]
plugin_dir = "/etc/heliosproxy/plugins"
trust_root = "/etc/heliosproxy/keys/plugin-publisher.pub"
hot_reload = true

[plugins.config.helios-plugin-cost-governor]
default_budget_per_second = 1000
```

Drop signed `.wasm` artefacts into `plugin_dir`. See
`heliosproxy-plugin-load`.

### Recipe 3: Time-Travel Replay (HA-TR)

```toml
[ha]
mode               = "active-passive"
switchover_buffer  = true
journal_max_bytes  = 1_000_000_000  # 1 GB
journal_retention  = "24h"
replay_concurrency = 4
```

Replay is invoked via `POST /api/replay` — see `heliosproxy-time-travel`.

### Recipe 4: Anomaly detection

```toml
[anomaly]
buffer_size           = 5000
rate_spike_z_score    = 3.0
auth_burst_threshold  = 10
auth_burst_window     = "30s"
sql_injection         = true
novel_query_threshold = 0.05
```

### Recipe 5: Edge-mode proxy

```toml
[edge]
mode      = "edge"           # or "home"
edge_id   = "edge-eu-west"
region    = "eu-west"
home_url  = "https://heliosproxy.example.com"
cache_size       = 10_000
cache_ttl_secs   = 60
```

Home-mode pairs:

```toml
[edge]
mode               = "home"
registry_capacity  = 32
edge_prune_after   = "120s"
```

See `heliosproxy-edge`.

### Recipe 6: TLS to backend

```toml
[tls.backend]
mode             = "verify-full"   # off | prefer | verify-ca | verify-full
ca_cert_path     = "/etc/heliosproxy/certs/backend-ca.pem"
client_cert_path = "/etc/heliosproxy/certs/proxy-client.pem"
client_key_path  = "/etc/heliosproxy/certs/proxy-client.key"
```

### Recipe 7: Reload config (= restart)

There is no `SIGHUP` reload in v0.4.x. To apply config changes:

```bash
sudo systemctl reload heliosproxy   # WRONG — does nothing
sudo systemctl restart heliosproxy  # Correct
```

systemd `restart` issues SIGTERM (drain) then re-launches. New
sessions hit the old config until drain completes; new connections
arriving after the restart hit the new config.

## Pitfalls

- **`failed to parse proxy.toml: invalid feature block`** — usually
  means the feature isn't compiled in. Check `[features]` in
  `Cargo.toml` and reinstall with the right `--features`.
- **`role = "replica"` is invalid.** Use `"standby"` or `"read_replica"`.
- **`weight = 0`** disables a node from routing without removing it
  from health checks. Useful for cordoning, confusing if unintended.
- **CLI flags override TOML silently.** If you `--primary` and have
  `[[nodes]]` with role=primary, the CLI wins. Pick one.
- **Don't put secrets in `proxy.toml`** — there's no secret-resolution
  layer. Use env vars (`${PG_PASSWORD}`) and let your process manager
  inject them, or mount a separate credentials file.
- **Path resolution is relative to CWD,** not the config file's
  directory. Prefer absolute paths for `plugin_dir`, `trust_root`,
  cert paths.
- **Default features are minimal.** A `[plugins]` block in TOML
  without the `wasm-plugins` feature compiled in causes a parse
  warning and the block is ignored. `/plugins` returns 503.

## See also

- [`config/proxy.example.toml`](../../config/proxy.example.toml) — minimal template
- [`config/proxy.full.toml`](../../config/proxy.full.toml) — every option, commented
- [`config/proxy.postgres.toml`](../../config/proxy.postgres.toml) — PG-specific tuning
- `heliosproxy-start` — how to launch with the config you wrote
- `heliosproxy-shutdown` — what restart-for-reload actually does
- Code: [`src/config.rs`](../../src/config.rs) — TOML schema
