---
name: heliosproxy-plugin-pack
description: Use the `helios-plugin` CLI to pack, sign, inspect, and verify WASM plugin artefacts. OCI-style `.tar.gz` (manifest + .wasm + .sig). Generate Ed25519 keys with openssl. Use when the user says "pack a plugin", "sign WASM", "trust root", "helios-plugin", "OCI artefact".
allowed-tools: Bash(helios-plugin *), Bash(openssl *), Bash(tar *), Bash(sha256sum *), Bash(cargo *), Read
related: [heliosproxy-overview, heliosproxy-plugin-load, heliosproxy-plugin-catalog]
---

# Pack, sign, inspect plugins

Plugins ship as OCI-style `.tar.gz` artefacts containing a
`manifest.json`, the `plugin.wasm` binary, and an optional
`plugin.sig` (Ed25519). The `helios-plugin` CLI is the canonical
tool for building, signing, and validating those artefacts.

## When to use

- Building a fresh plugin from a Rust crate compiled to wasm32
- Signing an existing `.wasm` so the proxy's trust-root accepts it
- Inspecting a third-party `.tar.gz` before loading it
- Verifying a signature against a public key in CI

🟠 Mutating on disk (writes the artefact); 🔵 read-only on running
proxy until the artefact is loaded (see `heliosproxy-plugin-load`).

## Surfaces

| Verb | Effect |
|---|---|
| `helios-plugin pack`    | Build `.tar.gz` from `.wasm` + manifest fields |
| `helios-plugin inspect` | Print manifest, file list, SHA256s |
| `helios-plugin verify`  | Check signature against a public key |
| `openssl genpkey` / `openssl pkey` | Generate / convert Ed25519 keys |

The CLI binary lives in the sibling repo
[`HDB-HeliosDB-Proxy-Plugins`](https://github.com/dimensigon/HDB-HeliosDB-Proxy-Plugins)
and installs alongside the proxy:

```bash
cargo install helios-plugin
helios-plugin --version
```

## Recipes

### Recipe 1: Generate an Ed25519 keypair

```bash
# Private key (keep secret)
openssl genpkey -algorithm Ed25519 -out plugin-publisher.key

# Public key (ship to operators as the trust root)
openssl pkey -in plugin-publisher.key -pubout -out plugin-publisher.pub

# View the public key as raw bytes (32) for embedding
openssl pkey -in plugin-publisher.pub -pubin -outform DER \
  | tail -c 32 | xxd -p -c 32
```

Both PEM and the trailing-32-bytes form are accepted by the proxy's
`[plugins].trust_root` config — the loader auto-detects.

### Recipe 2: Build a plugin from source (Rust → wasm)

```bash
git clone git@github.com:dimensigon/HDB-HeliosDB-Proxy-Plugins.git
cd HDB-HeliosDB-Proxy-Plugins
cargo build -p helios-plugin-cost-governor \
  --target wasm32-unknown-unknown --release

ls target/wasm32-unknown-unknown/release/
# helios_plugin_cost_governor.wasm   (≈120 KiB)
```

Note the underscore-vs-hyphen in the artefact name: cargo emits
`helios_plugin_cost_governor.wasm` for crate
`helios-plugin-cost-governor`.

### Recipe 3: Pack + sign in one step

```bash
helios-plugin pack \
  --wasm   target/wasm32-unknown-unknown/release/helios_plugin_cost_governor.wasm \
  --name        helios-plugin-cost-governor \
  --version     0.1.0 \
  --description "Per-tenant cost / budget gate" \
  --hooks       pre_query,post_query \
  --license     Apache-2.0 \
  --sign-with   plugin-publisher.key \
  --output      cost-governor-0.1.0.tar.gz
```

Resulting `.tar.gz` layout:

```
manifest.json    — name, version, description, license, hooks, wasm_sha256, signature_sha256
plugin.wasm      — the compiled module
plugin.sig       — Ed25519 detached signature over plugin.wasm
```

Without `--sign-with`, the artefact has no `plugin.sig` and the
proxy's `trust_root` policy decides whether to load (rejects by
default; accepts when `trust_root = "permissive"`).

### Recipe 4: Inspect a foreign artefact before loading

```bash
helios-plugin inspect cost-governor-0.1.0.tar.gz
```

```
helios-plugin-cost-governor 0.1.0
  description: Per-tenant cost / budget gate
  license:     Apache-2.0
  hooks:       pre_query, post_query
  files:
    plugin.wasm  124904 bytes  sha256=8a3b1f...
    plugin.sig       64 bytes  sha256=f12a09...
```

The output is what the proxy logs at load time. If the file list
includes anything beyond `manifest.json`, `plugin.wasm`, and
`plugin.sig`, the loader rejects it.

### Recipe 5: Verify a signature manually

```bash
helios-plugin verify cost-governor-0.1.0.tar.gz \
  --pubkey plugin-publisher.pub
# OK: signature valid for plugin-publisher.pub
```

Returns 0 on valid signature, non-zero on missing / mismatched.
Useful in CI gates before publishing artefacts to a registry.

### Recipe 6: Pack without signing (dev)

```bash
helios-plugin pack \
  --wasm  target/wasm32-unknown-unknown/release/my_plugin.wasm \
  --name  my-plugin \
  --version 0.0.1 \
  --hooks pre_query \
  --output my-plugin-dev.tar.gz
```

The proxy will refuse to load this unless `[plugins].trust_root =
"permissive"` (dev-only) or `trust_root = ""` (no signature
required).

## Pitfalls

- **`tar.gz` MUST contain only `manifest.json`, `plugin.wasm`,
  and optionally `plugin.sig`.** Extra files cause the loader to
  reject. Don't `tar -czf` a directory with `.DS_Store`,
  `Cargo.lock`, etc.
- **Signature is over `plugin.wasm` bytes only**, not the full
  `.tar.gz`. So a signature is portable across re-packs as long as
  the .wasm doesn't change.
- **PEM vs raw bytes** for keys: the CLI handles both. The proxy's
  `trust_root` config accepts a path to a PEM file or a 64-char hex
  string. `openssl pkey -in foo.pub -pubin -outform DER | tail -c 32 | xxd -p -c 32`
  produces the hex.
- **Don't reuse signing keys across environments.** One key per
  publisher per environment (dev, staging, prod). Compromise of one
  key doesn't taint others.
- **Cargo emits underscore names**, but plugin names in
  `manifest.json` should be hyphenated. Match the crate's
  declared `name`.
- **`--hooks` must include exactly the export names the .wasm
  actually defines.** Mismatch = the proxy loads but never invokes
  the hook. Verify with `wasm-objdump -j Export your.wasm`.

## See also

- `heliosproxy-plugin-load` — drop the artefact into the proxy
- `heliosproxy-plugin-catalog` — what each first-party plugin does
- `heliosproxy-config` — `[plugins].trust_root` config
- Demo: [`demos/v0.4.0/05-plugin-signatures/`](../../demos/v0.4.0/05-plugin-signatures/) — sig demo
- Demo: [`demos/v0.4.0/06-plugin-oci/`](../../demos/v0.4.0/06-plugin-oci/) — pack-and-load
- Demo: [`demos/v0.4.0/19-helios-plugin-cli/`](../../demos/v0.4.0/19-helios-plugin-cli/) — CLI walkthrough
- Sibling repo: <https://github.com/dimensigon/HDB-HeliosDB-Proxy-Plugins>
- Code: [`src/plugins/loader.rs`](../../src/plugins/loader.rs) — verifier
