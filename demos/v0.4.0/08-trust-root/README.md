# Demo 8 — `trust_root` config knob

**Module brief:** [§Module 8](../../../docs/website-brief-v0.4.0.md)

## UVP

> One TOML knob — `[plugins].trust_root` — flips the proxy from
> permissive (load any `.wasm`) to enforced (only signed plugins
> from the trust root). Same binary; deploy to dev or prod with
> different config.

## Use cases

- **Dev → prod promotion.** Local dev box has `trust_root` unset;
  CI pipeline sets it to a sealed key directory; same binary, same
  config layout.
- **Compliance gate.** SOC 2 evidence is "this prod proxy started
  with trust_root configured." Operators can grep `proxy.toml` to
  prove it.

## What this demo shows

Two `proxy.toml` files in this directory:

- `proxy.permissive.toml` — no `trust_root`. Loads anything.
- `proxy.enforced.toml`   — `trust_root = "/etc/helios/keys"`.
  Only loads signed plugins.

```bash
# Permissive: drop in an unsigned cost-governor.wasm; loads.
docker compose --env-file=permissive.env up -d
docker compose logs proxy | grep "loaded plugin"
# → INFO loaded plugin helios-plugin-cost-governor v0.1.0

# Enforced: same .wasm without a signature → refuses to load.
docker compose down
docker compose --env-file=enforced.env up -d
docker compose logs proxy | grep -i "signature"
# → WARN plugin cost-governor.wasm requires a sidecar .sig file
#        (trust root active)

# Sign it + restart → loads.
openssl pkeyutl -sign -inkey keys/signing.pem -rawin \
  -in plugins/cost-governor.wasm | base64 -w 0 > plugins/cost-governor.sig
docker compose restart proxy
docker compose logs proxy | grep "signed_by"
# → INFO plugin signature verified, signed_by=release-key
```

## Implementation pointer

- TOML field: `src/config.rs::PluginToml.trust_root: Option<String>`
- Runtime config: `src/plugins/config.rs::PluginRuntimeConfig.trust_root: Option<PathBuf>`
- Activation: `src/plugins/mod.rs::PluginManager::load_plugin` reads
  `runtime.config().trust_root` and conditionally attaches the
  `SignatureVerifier`.

## HeliosDB compatibility

Backend-agnostic.
