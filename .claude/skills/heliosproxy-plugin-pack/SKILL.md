---
name: heliosproxy-plugin-pack
description: Use the `helios-plugin` CLI to generate keys and to pack, sign, inspect, and verify WASM plugin artefacts. OCI-style `.tar.gz` (manifest + .wasm + signatures). Ed25519 keys from `helios-plugin keygen` or openssl. Use when the user says "pack a plugin", "sign WASM", "trust root", "helios-plugin", "OCI artefact".
allowed-tools: Bash(helios-plugin *), Bash(openssl *), Bash(tar *), Bash(sha256sum *), Bash(cargo *), Read
related: [heliosproxy-overview, heliosproxy-plugin-load, heliosproxy-plugin-catalog]
---

# Pack, sign, inspect plugins

Plugins ship as OCI-style `.tar.gz` artefacts: `manifest.json`, the
`plugin.wasm` module, and optionally `plugin.sig` (Ed25519 over the
module) and `manifest.sig` (Ed25519 over `manifest.json`, which pins the
module's SHA-256). The `helios-plugin` CLI builds, signs and checks them.

## When to use

- Building a fresh plugin from a Rust crate compiled to wasm32
- Signing a module or an artefact so the proxy's trust root accepts it
- Inspecting a third-party `.tar.gz` before loading it
- Verifying signatures against a trust root in CI

🟠 Mutating on disk (writes the artefact or the key files); 🔵 read-only
on the running proxy until the artefact is loaded (see
`heliosproxy-plugin-load`).

## Surfaces

| Verb | Effect |
|---|---|
| `helios-plugin keygen`  | Ed25519 key pair: `<name>.key` (PKCS#8 PEM, mode 0600) and `<name>.pub` (trust-root format) |
| `helios-plugin pack`    | Build a `.tar.gz` from a module plus manifest fields or a `plugin.yaml`; `--sign-with` signs module and manifest |
| `helios-plugin sign`    | Write a module's `.sig` sidecar, or re-sign an artefact (in place or `--output`) |
| `helios-plugin inspect` | Manifest, entries, sizes, SHA-256s, signature status (`--json`); for a module, its exported hooks |
| `helios-plugin verify`  | Integrity, and signatures against a trust root (`--trust-root` / `--pubkey`) |
| `helios-plugin new`     | Scaffold a plugin crate that builds and loads |

The CLI ships with the proxy crate:

```bash
cargo install heliosdb-proxy      # installs heliosdb-proxy and helios-plugin
helios-plugin --version
```

Keys: a signing key is a PKCS#8 PEM (`keygen`, `openssl genpkey -algorithm
Ed25519`), or base64 / 64 hex digits of the 32-byte seed. A public key is a
SubjectPublicKeyInfo PEM (`openssl pkey -pubout`), or base64 / 64 hex digits
of the 32 raw bytes. A trust root (`--trust-root`, and the proxy's
`[plugins] trust_root`) is a directory of `*.pub` files, one key file, or
one key inline as 64 hex digits.

## Recipes

### Recipe 1: Generate an Ed25519 keypair

```bash
helios-plugin keygen --out-dir keys --name plugin-publisher
# signing key: keys/plugin-publisher.key (keep it secret)
# public key:  keys/plugin-publisher.pub (put it in the proxy's trust root)
```

Or with openssl — both files are accepted as they are:

```bash
openssl genpkey -algorithm Ed25519 -out plugin-publisher.key
openssl pkey -in plugin-publisher.key -pubout -out plugin-publisher.pub
```

### Recipe 2: Build a plugin from source (Rust → wasm)

```bash
helios-plugin new my-plugin && cd my-plugin    # or a first-party plugin:
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown
ls target/wasm32-unknown-unknown/release/      # my_plugin.wasm
```

Cargo emits underscore names (`my_plugin.wasm` for crate `my-plugin`); the
plugin's name comes from the manifest, not the file.

### Recipe 3: Pack + sign in one step

```bash
helios-plugin pack \
  --wasm        target/wasm32-unknown-unknown/release/helios_plugin_cost_governor.wasm \
  --name        helios-plugin-cost-governor \
  --version     0.1.0 \
  --description "Per-tenant cost / budget gate" \
  --hooks       pre_query,post_query \
  --license     Apache-2.0 \
  --sign-with   keys/plugin-publisher.key \
  --output      cost-governor-0.1.0.tar.gz
```

Or take the fields from the scaffold's manifest: `helios-plugin pack
--manifest plugin.yaml --wasm ... --sign-with ... --output ...` (flags
override it). Layout:

```
manifest.json    — schema 1.1: name, version, description, license, hooks, wasm_sha256, signature_sha256
plugin.wasm      — the compiled module
plugin.sig       — Ed25519 signature over plugin.wasm
manifest.sig     — Ed25519 signature over manifest.json
```

`--sig <file>` embeds a signature over the module made elsewhere (no
`manifest.sig` then). Without either, the artefact is unsigned: a proxy
with `trust_root` set refuses it; without `trust_root` it loads.

### Recipe 4: Inspect a foreign artefact before loading

```bash
helios-plugin inspect cost-governor-0.1.0.tar.gz
```

```
helios-plugin-cost-governor 0.1.0
  description: Per-tenant cost / budget gate
  license:     Apache-2.0
  hooks:       pre_query, post_query
  schema:      1.1
  packed at:   2026-11-17T21:00:00Z
  signatures:  module signed, manifest signed
  files:
    manifest.json        412 bytes  sha256=...
    plugin.wasm       124904 bytes  sha256=8a3b1f...
    plugin.sig            88 bytes  sha256=...
    manifest.sig          88 bytes  sha256=...
```

`inspect` (like the proxy) refuses an artefact holding anything but those
four entries, an entry twice, or a module whose SHA-256 differs from the
manifest's.

### Recipe 5: Verify signatures

```bash
helios-plugin verify cost-governor-0.1.0.tar.gz --trust-root keys/
#   OK: module signed by 'plugin-publisher', manifest signed by 'plugin-publisher'
helios-plugin verify cost-governor-0.1.0.tar.gz --pubkey plugin-publisher.pub --require-signed-manifest
```

Exit 0 when valid, non-zero (with the reason) otherwise. A bare module is
verified against its `.sig` sidecar (or `--sig`).

### Recipe 6: Sign later

```bash
helios-plugin sign my-plugin.tar.gz --key keys/plugin-publisher.key   # re-signs in place
helios-plugin sign my-plugin.wasm   --key keys/plugin-publisher.key   # writes my-plugin.sig
```

## Pitfalls

- **An artefact holds only `manifest.json`, `plugin.wasm`, `plugin.sig`
  and `manifest.sig`.** Don't `tar -czf` a directory: extra entries
  (`.DS_Store`, `Cargo.lock`, a `./` entry) make the proxy refuse it.
  Use `helios-plugin pack`.
- **`plugin.sig` covers the module only**, so it survives a re-pack; a
  re-pack under another name is caught by `manifest.sig`. Set
  `[plugins] require_signed_manifest = true` to load only artefacts that
  carry it.
- **Don't reuse signing keys across environments.** One key per publisher
  per environment (dev, staging, prod). Compromise of one key doesn't
  taint others; removing a key from the trust root unloads what it signed.
- **`--hooks` must match the module's exports.** The proxy refuses a
  plugin that does not export a declared hook with the ABI signature, or
  declares a hook it never calls; `helios-plugin inspect <module.wasm>`
  lists the hooks a module exports.

## See also

- `heliosproxy-plugin-load` — drop the artefact into the proxy
- `heliosproxy-plugin-catalog` — what each first-party plugin does
- `heliosproxy-config` — `[plugins]` block
- Demo: [`demos/v0.4.0/05-plugin-signatures/`](../../demos/v0.4.0/05-plugin-signatures/) — sig demo
- Demo: [`demos/v0.4.0/06-plugin-oci/`](../../demos/v0.4.0/06-plugin-oci/) — pack-and-load
- Demo: [`demos/v0.4.0/19-helios-plugin-cli/`](../../demos/v0.4.0/19-helios-plugin-cli/) — CLI walkthrough
- Code: [`src/plugin_artefact.rs`](../../src/plugin_artefact.rs) — format, keys, trust roots
- Code: [`src/bin/helios-plugin.rs`](../../src/bin/helios-plugin.rs) — the CLI
