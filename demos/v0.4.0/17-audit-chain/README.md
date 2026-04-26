# Demo 17 — `audit-chain` plugin

**Module brief:** [§Module 17](../../../docs/website-brief-v0.4.0.md)

## UVP

> Hash-chained tamper-evident audit log. Every record embeds the
> SHA-256 of the previous; modifying any entry breaks the chain.
> Real cryptography via `env.sha256_hex` (was a placeholder in
> v0.3.x).

## Use cases

- **SOC 2 / ISO 27001 evidence.** Auditor walks the chain;
  unbroken = no tampering since boot.
- **Forensic incident response.** When a breach is suspected,
  chain breakage points to which records were modified and when.
- **Regulatory query log.** PCI / HIPAA require immutable query
  records; hash chain + S3 object lock satisfies both.

## What this demo shows

1. Run 5 queries — each appends to the chain.
2. Dump the chain via `/admin/kv/audit-chain/record:N`.
3. Walk the chain — every `prev_hash` matches the previous
   record's SHA-256.
4. Tamper with `record:2` (modify `elapsed_us`).
5. Re-run the verifier → reports broken link at `record:3`
   because its `prev_hash` no longer matches the (modified)
   `record:2`.

## Run it

```bash
cd demos/v0.4.0/17-audit-chain
./demo.sh
```

Output:

```text
=== audit-chain demo ===
[1/5] Running 5 queries
[2/5] Dumping chain:
   seq=0  prev=GENESIS         hash=cab07e7b...
   seq=1  prev=cab07e7b...     hash=58df8a91...
   seq=2  prev=58df8a91...     hash=a1c93e7f...
   seq=3  prev=a1c93e7f...     hash=2e8b7d44...
   seq=4  prev=2e8b7d44...     hash=ff301aa2...
[3/5] verify_chain → OK (5 records, no broken links)
[4/5] Tampering with seq=2 (elapsed_us 100 → 999)
[5/5] verify_chain → FAILED at index 3 (prev_hash mismatch)
```

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/audit-chain/src/lib.rs`. `record_hash`
+ `verify_chain` are pure functions; `build_record` shapes the
record from a `PostQueryEnvelope`. The `sha256_hex` helper
delegates to `env.sha256_hex` on `wasm32` (production) and falls
back to a deterministic FNV mixer on host targets (so unit tests
don't need wasmtime).

## HeliosDB compatibility

Backend-agnostic.
