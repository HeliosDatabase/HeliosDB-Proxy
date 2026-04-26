# Demo 6 — Plugin OCI artefact loader

**Module brief:** [§Module 6](../../../docs/website-brief-v0.4.0.md)

## UVP

> Distribute plugins like containers — `.tar.gz` with manifest +
> wasm + signature. The proxy ingests them directly; no extraction
> step.

## Use cases

- **Plugin marketplaces.** A registry serves
  `<name>-<version>.tar.gz` artefacts; operators `curl | tar -tzf`
  to inspect, drop in plugin dir, the proxy validates.
- **Reproducible builds.** Manifest's `wasm_sha256` lets CI verify
  the same bytes shipped to every environment.
- **Signed releases.** The `plugin.sig` inside the artefact uses
  the same Ed25519 trust root as Demo 5.

## What this demo shows

```bash
# 1. Build the plugin (one-time, cached)
cd ../../../../HDB-HeliosDB-Proxy-Plugins
cargo build -p helios-plugin-cost-governor \
  --target wasm32-unknown-unknown --release

# 2. Pack it
helios-plugin pack \
  --wasm target/wasm32-unknown-unknown/release/helios_plugin_cost_governor.wasm \
  --name helios-plugin-cost-governor \
  --version 0.1.0 \
  --hooks pre_query,post_query \
  --out cost-governor-0.1.0.tar.gz

# 3. Inspect — proves the manifest survived the round-trip
helios-plugin inspect cost-governor-0.1.0.tar.gz
#   {
#     "schema_version": "1.0",
#     "name": "helios-plugin-cost-governor",
#     "version": "0.1.0",
#     "hooks": ["pre_query", "post_query"],
#     "wasm_sha256": "09889579082ab18f72955a8754b63143afd694e97cf7684061ba7f53d6f13e4c",
#     "packed_at": "2026-04-26T..."
#   }

# 4. Drop in plugin dir — proxy loads directly
cp cost-governor-0.1.0.tar.gz demos/v0.4.0/06-plugin-oci/plugins/
cd demos/v0.4.0/06-plugin-oci
./demo.sh
```

The proxy log shows the artefact loading:

```text
INFO loaded plugin helios-plugin-cost-governor v0.1.0 from cost-governor-0.1.0.tar.gz
INFO   wasm_sha256 verified: 09889579082ab18f72955a8754b63143afd694e97cf7684061ba7f53d6f13e4c
```

Tamper proof — change one byte in the tarball and the loader
refuses:

```bash
# Flip a byte in the middle of the tarball
printf '\xff' | dd of=plugins/cost-governor-0.1.0.tar.gz bs=1 count=1 \
  conv=notrunc seek=5000
docker compose restart proxy
# → "wasm sha256 mismatch: manifest claims X, actual Y"
```

## Implementation pointer

`src/plugins/loader.rs::load_tar_gz` — detects `.gz` extension,
unpacks via `tar` + `flate2`, validates SHA-256, optionally
verifies signature via the same `SignatureVerifier` from Demo 5.
CLI side at `HDB-HeliosDB-Proxy-Plugins/cli/src/artefact.rs`.

## HeliosDB compatibility

Backend-agnostic — artefact handling is pure proxy-side.
