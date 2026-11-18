---
name: heliosproxy-plugin-load
description: Get a packed/signed `.tar.gz` or bare `.wasm` plugin into the running proxy. Drop into `plugin_dir`, rescan (hot reload, SIGHUP or `POST /plugins/reload`), verify via `GET /plugins` and `GET /plugins/files`. Configure `trust_root`. Use when the user says "load a plugin", "drop the wasm", "/plugins is empty", "trust root", "hot reload".
allowed-tools: Bash(curl *), Bash(cp *), Bash(ls *), Read
related: [heliosproxy-overview, heliosproxy-plugin-pack, heliosproxy-plugin-kv, heliosproxy-plugin-catalog]
---

# Load a plugin into the proxy

The plugin directory (`[plugins].plugin_dir`) is the source of truth. The
proxy reconciles it at startup, on SIGHUP, on `POST /plugins/reload`, and
with `hot_reload = true` every `hot_reload_interval_ms`: it verifies each
`.tar.gz` / `.wasm` (signature, if `trust_root` is set), compiles and links
it, checks its exports, and registers its hooks. `GET /plugins` lists what
is loaded; `GET /plugins/files` says why a file is not.

Requires `--features wasm-plugins` at build time.

## When to use

- First-time bring-up of any first-party plugin (cost-governor, etc.)
- Updating a plugin to a new version (replace the file)
- Disabling / removing a plugin (delete the file)
- Verifying that the proxy actually accepted a plugin you packed

🟠 Mutating — adding a plugin changes hook behaviour for ALL
client traffic. Validate in dev or under chaos before enabling
in prod.

## Surfaces

| Verb | How |
|---|---|
| Add plugin       | `cp my-plugin.tar.gz $PLUGIN_DIR/`, then rescan |
| Remove plugin    | `rm $PLUGIN_DIR/my-plugin.tar.gz`, then rescan |
| Rescan now       | `POST /plugins/reload`, or `kill -HUP <pid>` |
| Hot reload       | `[plugins].hot_reload = true` (rescans every `hot_reload_interval_ms`) |
| List loaded      | `GET /plugins` |
| Per-file state   | `GET /plugins/files` |
| Trust root       | `[plugins].trust_root = "/path/to/keys/"` (dir of `*.pub`), a key file, or a hex key |
| Runtime settings | `PUT /admin/kv/<plugin>/<key>` (see `heliosproxy-plugin-kv`) |

## Recipes

### Recipe 1: Configure plugin loading in proxy.toml

```toml
[plugins]
enabled    = true
plugin_dir = "/etc/heliosproxy/plugins"
trust_root = "/etc/heliosproxy/keys"     # dir of *.pub, a .pub/.pem file, or 64 hex digits
hot_reload = true                        # rescan every hot_reload_interval_ms (1000)
# require_signed_manifest = true         # only artefacts whose manifest is signed too
```

A key file is a SubjectPublicKeyInfo PEM (`openssl pkey -pubout`), base64
of the 32 raw bytes (`helios-plugin keygen` writes this), or 64 hex digits.
If `trust_root` is unset, signatures are **not** checked and startup logs a
warning: treat that as a misconfiguration in production. There are no
per-plugin `[plugins.<name>]` tables: plugins read their settings from
their KV namespace (`PUT /admin/kv/<plugin>/<key>`).

### Recipe 2: Drop a packed artefact and watch it load

```bash
PLUGIN_DIR=/etc/heliosproxy/plugins
sudo cp cost-governor-0.1.0.tar.gz $PLUGIN_DIR/
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/plugins/reload | jq .
```

```json
{"loaded": ["helios-plugin-cost-governor"], "updated": [], "unloaded": [], "failed": []}
```

The log shows `Plugin loaded` with the plugin's name, version, hooks,
file, module SHA-256 and the key that signed it. Then:

```bash
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/plugins | jq .
```

```json
[
  {
    "name":        "helios-plugin-cost-governor",
    "version":     "0.1.0",
    "description": "Per-tenant cost / budget gate",
    "path":        "/etc/heliosproxy/plugins/cost-governor-0.1.0.tar.gz",
    "sha256":      "8a3b1f...",
    "hooks":       ["pre_query", "post_query"],
    "state":       "Running",
    "invocations": 0,
    "errors":      0
  }
]
```

### Recipe 3: Update a plugin

```bash
sudo cp cost-governor-0.1.1.tar.gz $PLUGIN_DIR/
sudo rm $PLUGIN_DIR/cost-governor-0.1.0.tar.gz
```

One file provides each plugin name (a `.tar.gz` before a bare `.wasm`, then
the first file name in sort order), so while both files exist
`cost-governor-0.1.0.tar.gz` keeps providing it and the new file shows as
`shadowed`; once the old file is removed the new version takes over at the
next rescan (within ~1 s with hot reload). Replacing a file in place works
the same way. The new version replaces the old one only after it has
loaded: if it fails (bad signature, bad module) the old version keeps
running and `GET /plugins/files` shows the file as `stale` with the error.
Calls in flight finish on the version they started with. Invocation and
error counters are per plugin name and carry over.

### Recipe 4: Disable a plugin

Move or delete its file and rescan:

```bash
sudo mv $PLUGIN_DIR/cost-governor-0.1.0.tar.gz /var/lib/heliosproxy/disabled/
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/plugins/reload
```

Some first-party plugins also read an on/off switch from their KV
namespace (see `heliosproxy-plugin-catalog`). Removing a key from the trust
root unloads every plugin it signed at the next rescan.

### Recipe 5: See why a file did not load

```bash
helios-plugin pack --wasm my.wasm --name my-plugin --version 0.0.1 \
  --hooks pre_query --output my-plugin-unsigned.tar.gz
sudo cp my-plugin-unsigned.tar.gz $PLUGIN_DIR/
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9090/plugins/files | jq .
```

```json
[{"path": ".../my-plugin-unsigned.tar.gz", "state": "failed",
  "error": "Signature verification failed: plugin.sig: the artefact has no plugin.sig"}]
```

The file stays on disk and is retried when it, a sidecar or the trust root
changes. Other states: `loaded`, `stale`, `shadowed`, `pending` (changed
within `hot_reload_debounce_ms`).

### Recipe 6: Bare `.wasm` for development

```bash
sudo cp my-plugin.wasm my-plugin.json $PLUGIN_DIR/    # manifest sidecar
```

A bare module takes its manifest from a `.yaml` or `.json` sidecar, else
from a `helios_manifest` custom section; with none, it loads under its
file name (version `0.0.0-dev`) with the hooks it exports. With
`trust_root` set it needs a `.sig` sidecar (`helios-plugin sign`); with
`require_signed_manifest` bare modules are refused. Pack for production.

## Pitfalls

- **503 from `/plugins`** — `wasm-plugins` feature not compiled in, or
  `[plugins] enabled = false`.
- **A plugin that would never run is refused**: a hook the module does not
  export with the ABI signature, a hook the proxy never calls (it calls
  `pre_query`, `route`, `post_query`, `authenticate`), or no hooks. The
  reason is in `GET /plugins/files` and the log.
- **`max_plugins`** (default 20) bounds the loaded set; a plugin over it
  waits (state `failed`, "max_plugins") until another is unloaded.
- **Symlinks in `plugin_dir` are skipped.** Place real files there.
- **Changing `[plugins]` itself needs a restart**; a SIGHUP keeps the
  running settings (logged) and only rescans the directory.

## See also

- `heliosproxy-plugin-pack` — produce the artefact you load
- `heliosproxy-plugin-kv` — runtime config of a loaded plugin
- `heliosproxy-plugin-catalog` — what's available to load
- `heliosproxy-config` — `[plugins]` block
- Demo: [`demos/v0.4.0/06-plugin-oci/`](../../demos/v0.4.0/06-plugin-oci/) — pack-and-load end-to-end
- Demo: [`demos/v0.4.0/08-trust-root/`](../../demos/v0.4.0/08-trust-root/) — unsigned vs enforced
- Docs: "Loading, reloading and the plugin directory" in `docs/configuration.md`
- Code: [`src/plugins/mod.rs`](../../src/plugins/mod.rs) (`PluginManager::rescan`)
