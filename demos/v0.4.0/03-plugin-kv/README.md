# Demo 3 — Plugin host KV bridge

**Module brief:** [§Module 3](../../../docs/website-brief-v0.4.0.md)

## UVP

> WASM plugins can persist state across hook invocations via three
> wasmtime imports (`env.kv_get` / `env.kv_set` / `env.kv_delete`).
> Per-plugin namespacing — plugins cannot read each other's state.

## Use cases

- Token-bucket counters (rate limit per tenant).
- Sliding-window deduplication (drop repeat queries within N seconds).
- Per-request signature buffers (audit-chain's hash chain).

## What this demo shows

A trivial WASM plugin (built inline) that maintains a per-tenant
counter via `env.kv_set` / `env.kv_get`. After 100 queries to
tenant `acme`, `helios.kv()` shows 100. Tenant `globex` separately
shows its own count.

## Run it

```bash
cd demos/v0.4.0/03-plugin-kv
./demo.sh
```

The demo loads the existing `cost-governor.wasm` (which uses the
KV bridge for budget tracking) and proves namespace isolation by
seeding two tenants with separate budgets.

```bash
# Seed acme with budget+usage; seed globex with same budget but no usage
curl -s http://localhost:9090/admin/kv/cost-governor/tenant:acme:budget \
  -X PUT --data-binary '{"minute":1.0,"hour":10.0,"day":100.0}'
curl -s http://localhost:9090/admin/kv/cost-governor/tenant:globex:budget \
  -X PUT --data-binary '{"minute":1.0,"hour":10.0,"day":100.0}'

# Run 50 queries as acme, 5 as globex
for i in $(seq 1 50); do
  PGUSER=postgres psql -h localhost -p 6432 -d demo \
    -c "SET helios.tenant_id = 'acme'; SELECT 1" >/dev/null
done
for i in $(seq 1 5); do
  PGUSER=postgres psql -h localhost -p 6432 -d demo \
    -c "SET helios.tenant_id = 'globex'; SELECT 1" >/dev/null
done

# Inspect each tenant's usage — proves namespacing
curl -s http://localhost:9090/admin/kv/cost-governor/tenant:acme:usage
curl -s http://localhost:9090/admin/kv/cost-governor/tenant:globex:usage
```

## Implementation pointer

KV imports live in `src/plugins/host_imports.rs` (proxy) +
`abi/src/lib.rs` (plugin-side wrappers `kv_read`, `kv_write`,
`kv_remove`). Per-plugin namespacing is the key in
`KvBackend.inner: HashMap<plugin_name, HashMap<key, value>>`.

## HeliosDB compatibility

Backend-agnostic — the KV is in-process on the proxy. Works
identically against PG 17 or HeliosDB.
