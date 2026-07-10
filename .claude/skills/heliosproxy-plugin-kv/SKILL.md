---
name: heliosproxy-plugin-kv
description: Configure a running plugin's behaviour without restarting via `PUT/GET/DELETE /admin/kv/<plugin>/<key>`. Per-plugin namespaced state. Use when the user says "set the budget", "configure the residency map", "/admin/kv", "plugin runtime config", or wants to push a new mask rule live.
allowed-tools: Bash(curl *), Bash(jq *)
related: [heliosproxy-overview, heliosproxy-plugin-load, heliosproxy-plugin-catalog]
---

# Plugin KV — runtime configuration

Every loaded plugin gets a namespaced KV bucket
(`/admin/kv/<plugin-name>/<key>`) that the plugin reads through the
host import `kv_get`. This is how operators push runtime config
(budget caps, region maps, mask rules, allowlists) without restarting.

Requires `--features wasm-plugins` and at least one loaded plugin.
The bucket is in-memory; values don't survive a proxy restart.

## When to use

- Configuring a plugin you just loaded for the first time
- Updating a budget / threshold without restart
- Pushing a region map, mask rule, allowlist live
- Debugging "the plugin says no but it should say yes" — check
  what's in the KV

🟠 Mutating — `PUT` and `DELETE` change plugin behaviour
immediately on the next request that hits the relevant hook.

## Surfaces

| Verb | Path | Body / Result |
|---|---|---|
| `PUT`    | `/admin/kv/<plugin-name>/<key>` | UTF-8 body (text or JSON); `200 {"ok":true}`, or `413` on a cap breach |
| `GET`    | `/admin/kv/<plugin-name>/<key>` | `200 {"plugin","key","value"}` (value as a JSON string), or `404 {"error":"key not found"}` |
| `DELETE` | `/admin/kv/<plugin-name>/<key>` | `200 {"ok":true}` — idempotent (200 even when the key is absent) |
| `GET`    | `/admin/kv/<plugin-name>/`      | `200 {"plugin","keys":[...]}` — the trailing slash lists the namespace |

Keys are arbitrary UTF-8; the value is returned inside a JSON envelope
(`{"plugin","key","value"}`), with `value` as a UTF-8 string — pull it
out with `jq -r .value`. The `<key>` segment may itself contain `/`.
PUT bodies are decoded with `String::from_utf8_lossy`, so store binary
blobs base64-encoded.

## Recipes

### Recipe 1: Set a region map for `helios-plugin-residency-router`

```bash
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-residency-router/region_map \
  --data-raw '[["eu-west","pg-eu-west:5432"],["us-east","pg-us-east:5432"]]'
```

```bash
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-residency-router/enforce \
  --data-raw 'true'
```

The plugin reads `region_map` on every Route hook invocation. New
map values take effect on the next query, no restart needed.

### Recipe 2: Set a per-tenant budget for `helios-plugin-cost-governor`

```bash
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-cost-governor/budget/tenant-a \
  --data-raw '{"queries_per_minute":1000,"cost_units_per_hour":50000}'
```

The cost-governor reads `budget/<tenant>` on every PreQuery hook;
mutating one tenant's budget doesn't disturb others.

### Recipe 3: Read a value back

```bash
curl -s http://localhost:9090/admin/kv/helios-plugin-residency-router/region_map
# {"plugin":"helios-plugin-residency-router","key":"region_map","value":"[[\"eu-west\",\"pg-eu-west:5432\"],[\"us-east\",\"pg-us-east:5432\"]]"}

# The stored string lives under .value — unwrap it with jq -r:
curl -s http://localhost:9090/admin/kv/helios-plugin-residency-router/region_map | jq -r .value | jq .
# [["eu-west","pg-eu-west:5432"],["us-east","pg-us-east:5432"]]
```

A 404 means the key has never been set (or was DELETEd). Plugins
generally fall back to a hardcoded default in that case.

### Recipe 4: Delete a value (revert to default)

```bash
curl -s -X DELETE \
  http://localhost:9090/admin/kv/helios-plugin-cost-governor/budget/tenant-a
# {"ok":true}   (idempotent — 200 even if the key was already gone)
```

### Recipe 5: Bulk-load configuration from a file

```bash
# config-rules.json — produced by your config repo / GitOps
cat config-rules.json
# {
#   "/admin/kv/helios-plugin-column-mask/rules":     "...",
#   "/admin/kv/helios-plugin-residency-router/region_map": "...",
#   "/admin/kv/helios-plugin-cost-governor/budget/tenant-a": "..."
# }

jq -r 'to_entries[] | "\(.key) \(.value | @base64)"' config-rules.json \
  | while read -r path b64; do
      echo "$b64" | base64 -d | curl -s -X PUT --data-binary @- "http://localhost:9090$path"
    done
```

Pattern: keep all per-plugin runtime config in version control; on
deploy, replay the file against the proxy.

### Recipe 6: Verify a plugin honoured your new config

After PUT, immediately make a request that exercises the plugin and
inspect the result. For residency-router:

```bash
psql -h localhost -p 6432 -U postgres -d demo \
  -c "SET helios.region='eu-west'; SELECT 1"
# routed to pg-eu-west:5432 (visible in proxy logs)

psql -h localhost -p 6432 -U postgres -d demo \
  -c "SET helios.region='antarctica'; SELECT 1"
# rejected if `enforce=true` was set
```

For cost-governor: run a workload at >budget rate and watch
`/anomalies` (or the plugin's per-request errors).

## Pitfalls

- **Values are bytes, not typed.** The plugin parses them. Send
  the right shape — JSON for plugins that expect JSON, raw strings
  otherwise. Wrong shape = the plugin's KV-decode fails on the
  next invocation; the plugin logs an error and typically falls
  back to default behaviour.
- **No schema validation at the proxy layer.** A malformed JSON in
  KV won't error on PUT — it errors on first read by the plugin.
  Validate with `jq` before sending.
- **Values don't persist across restart.** The KV is in-process
  memory. Re-PUT after restart, or invoke the proxy via a config
  agent that does this on boot.
- **List with a trailing slash.** `GET /admin/kv/<plugin>/` (trailing
  slash, empty key) returns `{"plugin","keys":[...]}` for any loaded
  plugin — this is framework-provided, not plugin-implemented. An
  optional `?prefix=<p>` filters the returned keys. A path with no
  key segment at all (`/admin/kv/<plugin>`, no trailing slash) is
  malformed and returns `400`, as is an empty `<plugin>` segment
  (`/admin/kv//<key>`). Any query string is stripped before the
  plugin/key split, so `?…` never leaks into a stored key.
- **Size caps.** A key OR value larger than `kv_max_value_bytes`
  (default 65536), a new key past `kv_max_keys_per_plugin` (default
  1024), or a new namespace past `kv_max_plugins` (default 256) is
  rejected with `413`; overwriting an existing key never trips the
  key-count cap, and writing to an already-present namespace never
  trips the namespace cap. Deleting a namespace's last key frees its
  slot. All three are set in `[plugins]` (`0` = unlimited).
- **`/admin/kv` follows the admin bearer gate.** When `admin_token`
  is set, every `/admin/kv` call needs `Authorization: Bearer
  <token>` (the recipes above omit it for brevity); a proxy with no
  token configured leaves the admin port open, so firewall it and set
  a token regardless.
- **Concurrent PUTs to the same key** are last-writer-wins, no
  CAS. If two operators race, one update vanishes silently.

## See also

- `heliosproxy-plugin-load` — load the plugin first
- `heliosproxy-plugin-catalog` — what KV keys each first-party
  plugin reads
- Demo: [`demos/v0.4.0/03-plugin-kv/`](../../demos/v0.4.0/03-plugin-kv/)
- Demo: [`demos/v0.4.0/18-residency-router/`](../../demos/v0.4.0/18-residency-router/) — KV for region map
- Code: [`src/plugins/host_imports.rs`](../../src/plugins/host_imports.rs) — `kv_get` / `kv_set` host imports
- Code: [`src/admin.rs`](../../src/admin.rs) — `/admin/kv/...` impl
