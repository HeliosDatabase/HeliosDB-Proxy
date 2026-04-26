# Demo 19 — `helios-plugin` CLI

**Module brief:** [§Module 19](../../../docs/website-brief-v0.4.0.md)

## UVP

> `helios-plugin pack` is to plugin distribution what `docker
> build` is to container distribution. Three commands, no extra
> infrastructure.

## Use cases

- **CI plugin pipeline.** `cargo build` → `helios-plugin pack` →
  `helios-plugin verify` → upload to artefact store. Same pattern
  every team uses for libraries.
- **Air-gapped delivery.** Sign at HQ, ship `.tar.gz` on physical
  media to the deployment site, verify against the local trust
  root before loading.
- **Plugin registry.** Drop tarballs into a static S3 bucket;
  consumers `curl + verify` before installing.

## What this demo shows

End-to-end plugin distribution loop using only `openssl` and
`helios-plugin`:

```bash
cd demos/v0.4.0/19-helios-plugin-cli
./demo.sh
```

What the script does:

```bash
# 0. Build the plugin (one-time)
cd ../../../../HDB-HeliosDB-Proxy-Plugins
cargo build -p helios-plugin-cost-governor \
  --target wasm32-unknown-unknown --release

# 1. Generate Ed25519 release key
openssl genpkey -algorithm Ed25519 -out keys/release.pem
openssl pkey -in keys/release.pem -pubout -outform DER | tail -c 32 \
  | base64 > trust/release.pub

# 2. Sign + pack
openssl pkeyutl -sign -inkey keys/release.pem -rawin \
  -in target/wasm32-unknown-unknown/release/helios_plugin_cost_governor.wasm \
  | base64 -w 0 > /tmp/cg.sig
helios-plugin pack \
  --wasm target/wasm32-unknown-unknown/release/helios_plugin_cost_governor.wasm \
  --name helios-plugin-cost-governor \
  --version 0.1.0 \
  --hooks pre_query,post_query \
  --sig /tmp/cg.sig \
  --out cost-governor-0.1.0.tar.gz

# 3. Inspect (proves manifest survived)
helios-plugin inspect cost-governor-0.1.0.tar.gz | jq .

# 4. Verify against the trust root
helios-plugin verify cost-governor-0.1.0.tar.gz --trust-root trust/
# → "OK — signed by release"

# 5. Tamper test — flip a byte in the wasm, re-pack, verify
# Note: this re-signs with the modified bytes. Real tamper test is
# to mutate the tarball directly:
gunzip -c cost-governor-0.1.0.tar.gz \
  | sed 's/h/H/' \
  | gzip > tampered.tar.gz
helios-plugin verify tampered.tar.gz --trust-root trust/
# → "wasm sha256 mismatch" or "signature did not match"
```

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/cli/src/main.rs` — `clap` subcommands.
`cli/src/artefact.rs` — pack/unpack/verify (11 unit tests).
`cli/src/manifest.rs` — versioned manifest schema.

## HeliosDB compatibility

CLI is a host binary; doesn't touch any database. Operates on
`.wasm` + `.sig` + tarball files only.
