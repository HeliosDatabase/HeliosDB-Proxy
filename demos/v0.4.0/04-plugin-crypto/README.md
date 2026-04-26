# Demo 4 — Plugin host crypto (`env.sha256_hex`)

**Module brief:** [§Module 4](../../../docs/website-brief-v0.4.0.md)

## UVP

> One audited SHA-256 implementation in the host, available to every
> plugin via `env.sha256_hex` — saves ~25 KiB per `.wasm` and gives
> reviewers a single attack surface to audit.

## Use cases

- Audit-chain hash linking (Demo 17 builds on this).
- Per-request signatures the proxy emits to a downstream system.
- Idempotency-key hashing in payment plugins.

## What this demo shows

Two parts:

1. **RFC 6234 vector check** — the proxy's host import is byte-
   for-byte equivalent to OpenSSL's SHA-256:

   ```bash
   echo -n "abc" | sha256sum
   # 67e2... wait, that's md5. SHA-256 of "abc" =
   # ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
   ```

   The proxy ships a unit test (`test_host_sha256_import_matches_rfc_6234_vector`)
   that loads a WAT module exercising the import and asserts the
   canonical digest.

2. **Audit-chain producing real digests** — load `audit-chain.wasm`,
   run a few queries, dump the chain, observe SHA-256 hex digests
   (not the v0.3.x FNV placeholder).

## Run it

```bash
cd demos/v0.4.0/04-plugin-crypto
./demo.sh
```

Output:

```text
=== Plugin Crypto Demo ===
[1/3] Verifying SHA-256 of "abc" against RFC 6234 vector
   expected: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
   produced: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad   ✓

[2/3] Loading audit-chain.wasm + running 3 queries:
   Q1: SELECT 1
   Q2: SELECT 2
   Q3: SELECT 3

[3/3] Dumping chain from KV:
   seq=0  prev=GENESIS  hash=cab07e7b1e... (real SHA-256, not FNV)
   seq=1  prev=cab07e7b1e...  hash=58df...
   seq=2  prev=58df...  hash=a1c9...
```

## Implementation pointer

`src/plugins/host_imports.rs::register_crypto_imports` —
`func_wrap("env", "sha256_hex", ...)` calls into the production
`sha2` crate. Plugin-side wrapper at
`heliosdb-proxy-plugins/abi/src/lib.rs::sha256_digest_hex`.

## HeliosDB compatibility

Backend-agnostic.
