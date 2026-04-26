# Demo 5 — Plugin Ed25519 signature verification

**Module brief:** [§Module 5](../../../docs/website-brief-v0.4.0.md)

## UVP

> Plugins are code with database-level privileges. Treat them like
> code: only load what's signed by a trusted key.

## Use cases

- **Multi-publisher plugin ecosystems.** First-party + community +
  customer-private plugins coexist; trust root decides what loads.
- **Air-gapped environments.** No registry, no internet — operators
  ship a `.wasm` + `.sig` pair on an SD card.
- **Compliance.** Auditors want "we cryptographically verified
  every plugin that ran" in the SOC 2 evidence pile. Signatures
  give them that, traceable to the signer label in the loader logs.

## What this demo shows

End-to-end pipeline using only `openssl` and the proxy:

1. Generate Ed25519 keypair (operator's release key).
2. Sign `cost-governor.wasm` with it.
3. Configure the proxy with `trust_root = /etc/helios/keys`.
4. Drop the signed `.wasm` + `.sig` in the plugin dir → loads,
   logs `signed_by=release-key`.
5. Drop a *tampered* `.wasm` (one byte flipped) → load refuses
   with `SignatureInvalid`.
6. Drop an unsigned `.wasm` → load refuses with
   `requires a sidecar .sig file`.

## Run it

```bash
cd demos/v0.4.0/05-plugin-signatures
./demo.sh
```

Sample sequence (the script automates this):

```bash
# 1. Generate key + write trust root
openssl genpkey -algorithm Ed25519 -out signing.pem
openssl pkey -in signing.pem -pubout -outform DER | tail -c 32 \
  | base64 > keys/release-key.pub

# 2. Sign the plugin
openssl pkeyutl -sign -inkey signing.pem -rawin -in plugins/cost-governor.wasm \
  | base64 -w 0 > plugins/cost-governor.sig

# 3. Start proxy with trust_root configured
docker compose up -d

# 4. Watch logs — should see "signed_by=release-key"
docker compose logs proxy | grep "signature verified"
```

Negative-path test (also in `demo.sh`):

```bash
# Tamper with the .wasm
printf '\xff' | dd of=plugins/cost-governor.wasm bs=1 count=1 conv=notrunc \
  seek=$(($(stat -c%s plugins/cost-governor.wasm) - 1))
docker compose restart proxy
docker compose logs proxy | grep -i "signature"
# → "Signature verification failed: signature did not match any trusted key"
```

## Implementation pointer

`src/plugins/loader.rs::SignatureVerifier` (~80 lines of
trust-root parsing + verify). Wired through
`PluginManager::load_plugin` when `[plugins].trust_root` is set in
TOML.

## HeliosDB compatibility

Backend-agnostic — signature verification is pure proxy-side.
