---
name: heliosproxy-plugin-load
description: Get a packed/signed `.wasm` or `.tar.gz` plugin into the running proxy. Drop into `plugin_dir`, hot-reload, verify via `GET /plugins`. Configure `trust_root`. Use when the user says "load a plugin", "drop the wasm", "/plugins is empty", "trust root", "hot reload".
allowed-tools: Bash(curl *), Bash(cp *), Bash(ls *), Read
related: [heliosproxy-overview, heliosproxy-plugin-pack, heliosproxy-plugin-kv, heliosproxy-plugin-catalog]
---

# Load a plugin into the proxy

The proxy scans `[plugins].plugin_dir` at startup and on the hot-
reload watch. Drop a `.wasm` or `.tar.gz` artefact into the directory;
the proxy validates the signature (if `trust_root` is set), instantiates
the WASM module, and registers its hooks. Status appears in
`GET /plugins`.

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
| Add plugin       | `cp my-plugin.tar.gz $PLUGIN_DIR/` (and reload) |
| Remove plugin    | `rm $PLUGIN_DIR/my-plugin.tar.gz` (and reload) |
| Hot-reload       | `[plugins].hot_reload = true` (FS watcher); else restart |
| List loaded      | `GET /plugins` |
| Trust root       | `[plugins].trust_root = "/path/to/pubkey.pem"` |

## Recipes

### Recipe 1: Configure plugin loading in proxy.toml

```toml
[plugins]
plugin_dir = "/etc/heliosproxy/plugins"
trust_root = "/etc/heliosproxy/keys/plugin-publisher.pub"
hot_reload = true       # watch plugin_dir for changes

# optional — per-plugin runtime config (passed via env-style map)
[plugins.config.helios-plugin-cost-governor]
default_budget_per_second = 1000
```

`trust_root` accepts:
- a PEM file path,
- a raw hex string of the 32-byte public key,
- or `"permissive"` to disable signature requirement (dev only).

If `trust_root` is unset, signature is **not** required, but a
warning is logged. Treat that as a misconfiguration in production.

### Recipe 2: Drop a packed artefact and watch it load

```bash
PLUGIN_DIR=/etc/heliosproxy/plugins
sudo cp cost-governor-0.1.0.tar.gz $PLUGIN_DIR/
journalctl -u heliosproxy -f | grep -E 'plugin|wasm'
```

Expected log lines:

```
INFO  PluginLoader: scanning /etc/heliosproxy/plugins
INFO  PluginLoader: validating cost-governor-0.1.0.tar.gz manifest
INFO  PluginLoader: signature OK (Ed25519)
INFO  PluginLoader: instantiating helios-plugin-cost-governor 0.1.0
INFO  PluginLoader: hooks pre_query, post_query registered
INFO  PluginLoader: 1 plugin loaded
```

Then verify via the admin API:

```bash
curl -s http://localhost:9090/plugins | jq .
```

```json
[
  {
    "name":        "helios-plugin-cost-governor",
    "version":     "0.1.0",
    "description": "Per-tenant cost / budget gate",
    "hooks":       ["pre_query", "post_query"],
    "state":       "Running",
    "invocations": 0,
    "errors":      0
  }
]
```

### Recipe 3: Hot-reload an updated plugin

```bash
sudo cp cost-governor-0.1.1.tar.gz $PLUGIN_DIR/
sudo rm $PLUGIN_DIR/cost-governor-0.1.0.tar.gz
```

With `[plugins].hot_reload = true`, the FS watcher picks up the
change within ~1 s. The proxy unloads the old version, loads the
new one, and resets per-plugin invocation/error counters.

In-flight requests being handled by the old version finish on the
old version; new requests use the new.

### Recipe 4: Disable a plugin without removing the file

The proxy doesn't have a runtime "disable" toggle in v0.4.x. Three
options:
1. Move the file out of `plugin_dir`:
   ```bash
   sudo mv $PLUGIN_DIR/cost-governor-0.1.0.tar.gz /tmp/
   ```
2. Set `[plugins.config.<plugin-name>].enabled = false` if the
   plugin honours that key (most first-party plugins do — see
   `heliosproxy-plugin-catalog`).
3. Remove the trust root (effectively unsigns all artefacts) —
   nuclear, affects every plugin.

### Recipe 5: Verify trust-root rejection of an unsigned artefact

```bash
# Pack without signing (see heliosproxy-plugin-pack)
helios-plugin pack --wasm my.wasm --name my-plugin --version 0 \
  --hooks pre_query --output my-plugin-unsigned.tar.gz

sudo cp my-plugin-unsigned.tar.gz $PLUGIN_DIR/

journalctl -u heliosproxy -f | grep -i 'plugin\|signature'
# WARN  PluginLoader: my-plugin-unsigned.tar.gz: signature absent and trust_root requires it; rejected
```

The artefact stays on disk but isn't loaded. `GET /plugins`
doesn't list it.

### Recipe 6: Bare `.wasm` without packaging

For local development, a bare `.wasm` is allowed when
`trust_root = "permissive"` or unset:

```bash
sudo cp my-plugin.wasm $PLUGIN_DIR/
```

The loader synthesises a default manifest (`name = filename`,
`version = "0.0.0-dev"`, hooks discovered from exports). Useful for
iteration; never for production.

## Pitfalls

- **503 from `/plugins`** — `wasm-plugins` feature not compiled in.
  Rebuild with `--features wasm-plugins`.
- **Plugin loads but never fires** — `manifest.hooks` may not match
  the actual `.wasm` exports. The proxy registers what's declared,
  not what's exported. Inspect via `helios-plugin inspect` and
  `wasm-objdump`.
- **`state: Error(...)`** in `/plugins` — instantiation failed
  (out-of-memory at allocation, bad ABI, missing import). Read the
  error string; usually points at a missing host import the plugin
  expected.
- **Hot-reload can race** if you replace a file while the previous
  version is in the middle of a hook invocation. Inflight requests
  use the old plugin; future requests use the new. Don't expect a
  hard barrier.
- **`trust_root = "permissive"` in prod is a CVE in waiting.** Any
  attacker who can write to `plugin_dir` runs WASM with full
  host-import access. Set the trust root and treat the public key
  like a code-signing cert.
- **Symlinks in `plugin_dir` are not followed.** Place real files
  there (or use copy-on-write at the package-manager level).

## See also

- `heliosproxy-plugin-pack` — produce the artefact you load
- `heliosproxy-plugin-kv` — runtime config of a loaded plugin
- `heliosproxy-plugin-catalog` — what's available to load
- `heliosproxy-config` — `[plugins]` block
- Demo: [`demos/v0.4.0/06-plugin-oci/`](../../demos/v0.4.0/06-plugin-oci/) — pack-and-load end-to-end
- Demo: [`demos/v0.4.0/08-trust-root/`](../../demos/v0.4.0/08-trust-root/) — permissive vs enforced
- Code: [`src/plugins/loader.rs`](../../src/plugins/loader.rs)
- Code: [`src/plugins/runtime.rs`](../../src/plugins/runtime.rs)
