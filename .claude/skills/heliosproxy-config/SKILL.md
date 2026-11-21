---
name: heliosproxy-config
description: Author or edit `proxy.toml`. Walk through every block (top-level keys, nodes, pool, pool_mode, cache, plugins, anomaly, edge, multi-tenancy, etc.). Use when the user asks "how do I configure X", "where do I put Y in proxy.toml", or after pasting a TOML snippet that doesn't load.
allowed-tools: Read, Grep, Glob, Bash(heliosdb-proxy *), Bash(curl *)
related: [heliosproxy-overview, heliosproxy-start, heliosproxy-install]
---

# Configure HeliosProxy

`proxy.toml` is the single source of truth. Reference configs ship in
the repo: `config/proxy.minimal.toml` (smallest valid file),
`config/proxy.example.toml` (commented starter) and
`config/proxy.full.toml` (every section, commented). The authoritative
key reference is `docs/configuration.md`; the parser is `ProxyConfig` in
`src/config.rs`.

## When to use

- Authoring a new deployment's TOML
- Adding a feature (plugins, edge, anomaly, cache, tenants) to an existing one
- Diagnosing "feature returns 503" → the feature is not compiled in, or its section is off
- Migrating a CLI-flags-only setup to a TOML file

🔵 Read-only — editing a file; SIGHUP applies what it can, the rest needs a restart

## Top-level keys and sections

There is **no `[server]` table**: `listen_address`, `admin_address`,
`admin_token`, `admin_allow_insecure`, `tr_enabled`, `tr_mode`,
`write_timeout_secs`, `optimize_unnamed_parse`, `function_call`,
`shutdown_drain_timeout_secs` and `strict_config` sit at the document root.
Logging is `--log-level` / `--json-logs` / `RUST_LOG`, not TOML.

| Section | Purpose | Needs |
|---|---|---|
| `[[nodes]]`       | each backend: `host`, `port`, `role` (`primary` / `standby` / `replica`), `weight`, `enabled`, `name` | ≥ 1, with a primary |
| `[pool]`, `[pool_mode]` | pool sizing; `mode` = session / transaction / statement | optional (defaults) |
| `[load_balancer]`, `[health]` | read strategy, read/write split; health probes | optional |
| `[topology]`      | `static` (default), `postgres`, `patroni` | `postgres-topology` for the last two |
| `[tls]`           | client TLS termination | optional |
| `[auth]`, `[[hba]]` | `mode = "passthrough" \| "scram"` + `auth_file`; admission rules | optional |
| `[cache]`         | query cache (`trust_backend_read_effects`, `proof_ttl_ms`, tiers, semantic L3) | `query-cache` |
| `[plugins]`       | WASM plugin dir, trust root, hot reload | `wasm-plugins` |
| `[anomaly]`       | detector thresholds | `anomaly-detection` |
| `[edge]`          | `role` = home / edge | `edge-proxy` |
| `[multi_tenancy]` | tenant identity, row filter, per-tenant caps and budgets | `multi-tenancy` |
| `[rate_limit]`, `[circuit_breaker]` | token bucket / window / in-flight cap; per-node breaker | `rate-limiting`, `circuit-breaker` |
| `[branch]`        | branch databases and branch routing (`route_enabled`) | — |
| `[mcp]`, `[http_gateway]`, `[graphql_gateway]` | gateways | `graphql-gateway` for GraphQL |
| `[limits]`, `[journal]`, `[admin]` | operational bounds, recovery journal, `/api/stats` | — |

`[ha]` is **rejected** at startup (promotion is external, or the opt-in
`[auto_promote]`). Any other unknown key is warned and ignored;
`strict_config = true` refuses it.

## Recipes

### Recipe 1: Minimal config with a standby

```toml
listen_address = "0.0.0.0:6432"
admin_address  = "127.0.0.1:9090"   # non-loopback needs admin_token

[pool_mode]
mode = "transaction"

[[nodes]]
host = "pg-primary"
port = 5432
role = "primary"

[[nodes]]
host = "pg-replica-1"
port = 5432
role = "standby"

[health]
check_interval_secs = 5
failure_threshold   = 3
```

Check it loads: start it and read `GET /config` — `ignored_config_keys`
lists every key the proxy did not recognise (also in the startup log).

### Recipe 2: Add WASM plugins

```toml
[plugins]
enabled    = true
plugin_dir = "/etc/heliosproxy/plugins"
trust_root = "/etc/heliosproxy/plugin-keys"   # dir of *.pub, one key file, or 64 hex digits
hot_reload = true
```

Drop signed `.tar.gz` artefacts (or `.wasm` modules with a `.sig`) into
`plugin_dir`. There are no per-plugin tables: push a plugin's settings at
runtime with `PUT /admin/kv/<plugin>/<key>` (`heliosproxy-plugin-kv`). See
`heliosproxy-plugin-load`.

### Recipe 3: Transaction Replay

```toml
tr_enabled = true
tr_mode    = "session"     # none | session | select | transaction
write_timeout_secs = 30

[journal]
# dir = "/var/lib/heliosproxy/journal"   # optional durable recovery journal
```

Replay of a journal window is `POST /api/replay` — see
`heliosproxy-time-travel`.

### Recipe 4: Anomaly detection

```toml
[anomaly]
rate_window_secs    = 60
spike_z_threshold   = 3.0
auth_window_secs    = 60
auth_warning_count  = 5
auth_critical_count = 10
event_buffer_size   = 1024
max_auth_windows    = 10000
```

### Recipe 5: Edge-mode proxy

```toml
[edge]
enabled  = true
role     = "edge"                          # "home" (default) or "edge"
home_url = "https://home-proxy:9090"
auth_token = "${EDGE_HOME_TOKEN}"
edge_id  = "edge-eu-west"
region   = "eu-west"
default_ttl_secs = 60
max_entries = 10000
```

See `heliosproxy-edge`.

### Recipe 6: Client TLS

```toml
[tls]
enabled   = true
cert_path = "/etc/heliosproxy/server.crt"
key_path  = "/etc/heliosproxy/server.key"
```

### Recipe 7: Apply config changes

```bash
sudo systemctl reload heliosproxy   # if the unit has ExecReload=/bin/kill -HUP $MAINPID
kill -HUP $(pgrep -x heliosdb-proxy)
```

SIGHUP reloads what can change live (the node set, `[tls]` and its
certificate files, `tr_mode` for new sessions, the sizes of per-tenant
caps and budgets, the plugin directory's files) and warns about what it
keeps for a restart (`listen_address`, `admin_address`, `[edge]`,
`[cache]`, `[limits]`, `[plugins]` settings, other `[multi_tenancy]`
changes, `[anomaly]`). For those, `systemctl restart` (SIGTERM drain, then start)
or a SIGUSR2 handoff to a new process.

## Pitfalls

- **`[server]`, `[ha]`, `address = "host:port"` in `[[nodes]]`** are
  not ProxyConfig: use top-level keys, `host` + `port`, and see above
  for `[ha]`.
- **A feature section without its cargo feature** is parsed but does
  nothing; `strict_config = true` makes it a startup error and
  `GET /capabilities` shows compiled / enabled / wired per subsystem.
- **`--primary` / `--standby` apply only without `--config`.**
- **Secrets:** reference the environment (`admin_token =
  "${ADMIN_TOKEN}"`, `${VAR:-default}`); an unset `${VAR}` fails startup.
- **Path resolution is relative to CWD,** not the config file's
  directory. Prefer absolute paths for `plugin_dir`, `trust_root`,
  cert paths.
- **The query cache caches only `SELECT <integer>` until you set
  `[cache] trust_backend_read_effects = true`** — read its contract in
  `docs/configuration.md` first.

## See also

- [`config/proxy.minimal.toml`](../../config/proxy.minimal.toml) — smallest valid file
- [`config/proxy.example.toml`](../../config/proxy.example.toml) — commented starter
- [`config/proxy.full.toml`](../../config/proxy.full.toml) — every section, commented
- [`docs/configuration.md`](../../docs/configuration.md) — every key with its default
- `heliosproxy-start` — how to launch with the config you wrote
- `heliosproxy-shutdown` — drain semantics of a restart
- Code: [`src/config.rs`](../../src/config.rs) — TOML schema
