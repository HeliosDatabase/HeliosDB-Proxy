# Website brief — v0.4.0 modules

**Audience:** the team updating https://www.heliosdb.com/heliosproxy.html and
the blog at https://www.heliosdb.com/blog-post.html?id=XX

**Window covered:** v0.3.0 (2026-03-26) → v0.4.0 (2026-04-25). The v0.3.1
maintenance release in between contained only fixes; no new modules.

**Module count delta:** v0.3.0 listed **24 modules** on the website. v0.4.0
adds **22 modules** at the same granularity, bringing the total to **46**.
The two new top-level Cargo features (`anomaly-detection`, `edge-proxy`)
are joined by 6 plugin-platform extensions, 2 admin-surface modules,
8 first-party plugins, and 4 companion projects. See breakdown below.

---

## How to use this document

For each of the 22 modules:

- **Card copy** — three lines for the heliosproxy.html module grid (matches
  the card style of existing entries like `wasm-plugins`, `query-cache`).
  Drop straight into the page.
- **Blog draft** — 2–4 paragraphs of technical writing. Suitable for a
  blog-post body; the headline and lede are also provided.
- **Key files** — paths inside the renamed repos so authors can pull
  code excerpts or screenshot the implementation.

Repository naming after the v0.4.0 rename:

| canonical name | role |
|---|---|
| `dimensigon/HDB-HeliosDB-Proxy` | core proxy (this is where most files live) |
| `dimensigon/HDB-HeliosDB-Proxy-Operator` | Kubernetes operator |
| `dimensigon/HDB-HeliosDB-Proxy-Plugins` | first-party WASM plugins + CLI |
| `dimensigon/terraform-provider-HDB-HeliosDB-Proxy` | Terraform provider |
| `dimensigon/pulumi-HDB-HeliosDB-Proxy` | Pulumi provider |

Container image: `ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.0`.

---

## Module 1 — Anomaly Detection (`anomaly-detection`)

### Card copy

> **Anomaly Detection** — In-process detector for rate spikes (z-score
> against rolling EWMA), credential-stuffing bursts, six classes of SQL
> injection patterns, and novel query shapes. No external data store;
> events stream over `/anomalies`.

### Blog draft — *"Catching abuse without a SIEM in front of your DB"*

Production database proxies sit on the perfect vantage point to spot
abuse: every query, every auth attempt, every tenant identity flows
through them. v0.4.0 turns that vantage into a working detector
without bolting on a separate analytics stack.

Four families fire concurrently per query: a **rate-spike** detector
keeps a 60-second sliding-bucket window per tenant and fires when the
current second's count is more than 3σ above the 59-second baseline; a
**credential-stuffing** detector keeps per-`(user, ip)` failure
counts that reset on first successful auth; a **SQL-injection**
scanner pattern-matches six well-known payload classes (classic OR
tautology, `UNION SELECT`, comment escape, stacked queries, time-based
blind, `information_schema` probe); and a **novel-query** detector
emits informational events the first time a query fingerprint appears
on the proxy.

The "why statistical, not trained" answer is honest: production
classifiers want labelled data — feedback loops from analyst-marked
false positives — and that loop doesn't exist on a fresh deployment.
The `AnomalyEvent` ring buffer makes future-proofing easy: events are
exactly what a learned classifier would need as input. Statistical
detectors produce signal today; the same shape can feed a model
tomorrow.

The detector runs *before* the WASM plugin pre-query hook, so a
detection lands in the audit trail even when a plugin later blocks
the query. Tenant identity comes from `session.variables.tenant_id`
with a fallback to `session.variables.user` and finally the client IP
— no plugin required.

### Key files

| file | what to look at |
|---|---|
| `src/anomaly/mod.rs` | `AnomalyDetector::record_query` — the one-call entry point used by the server's hot path. `AnomalyEvent` enum at the top is the wire shape for `/anomalies`. |
| `src/anomaly/ewma.rs` | `RateWindow::observe_and_score` — sliding-bucket z-score implementation. ~50 LoC, no external deps. |
| `src/anomaly/sql_injection.rs` | `scan` returns labels for every pattern that matched; one function per pattern class. |
| `src/server.rs` | `record_anomaly_observation` — wiring into `client_loop`. |
| `src/admin.rs` | `handle_anomalies_list` — the `/anomalies?limit=N` endpoint. |

---

## Module 2 — Edge / Geo Proxy (`edge-proxy`)

### Card copy

> **Edge Mode** — Cache-first proxy for geo-distributed deployments.
> Local LRU+TTL+version cache on every edge; home broadcasts table-
> scoped invalidations on writes. Last-write-wins; no consensus.

### Blog draft — *"PostgreSQL at the edge without rewriting your app"*

Geo-distributed PostgreSQL traditionally means choosing between
synchronous replication (slow), asynchronous read replicas (consistent
only at the source), or sharding (rewrite your app). v0.4.0 ships a
fourth option: HeliosProxy in **edge mode**, which terminates reads
against a per-region cache and pulls misses from a designated home
proxy.

The coherence model is intentionally simple. Every cache entry carries
a monotonic logical version assigned by the home at write commit time,
plus the set of tables the write touched. When the home commits a
write it broadcasts an `Invalidate { up_to_version, tables }` event to
every registered edge over a per-edge mpsc channel; each edge drops
cached entries whose version ≤ the bound and whose table set
intersects. Late writes — including those caused by cross-region clock
skew — cannot resurrect stale data because the version bound is
monotonic.

The contract this gives you is "eventual consistency with bounded
staleness": readers in any region may see TTL-window-stale data after
a write to another region, but never older than the configured TTL.
For sites where bounded staleness is the explicit deal — analytics
dashboards, product catalogs, anything where the latency win matters
more than per-record freshness — this is the simplest model that
actually works without distributed consensus.

There is no central registrar. Edges register with the home over HTTP
(`POST /api/edge/register`), the home pushes invalidations on the same
channel, and edges that disconnect get pruned on the next broadcast.
Liveness window prunes silent edges separately. No vector clocks, no
gossip, no Raft.

### Key files

| file | what to look at |
|---|---|
| `src/edge/mod.rs` | `EdgeConfig`, `EdgeRole` (Home/Edge), top-level docs explaining the coherence contract. |
| `src/edge/cache.rs` | `EdgeCache::invalidate` — version + table-scoped sweep, the heart of the coherence story. LRU eviction in `insert`; lazy expiry in `get`. |
| `src/edge/registry.rs` | `EdgeRegistry::broadcast` — bounded mpsc per edge, prunes closed channels in the same call. `prune_stale` for liveness. |
| `src/admin.rs` | `handle_edge_status`, `handle_edge_register`, `handle_edge_invalidate` — three endpoints under `/api/edge*`. |

---

## Module 3 — Plugin Host KV Bridge

### Card copy

> **Plugin KV** — `env.kv_get`/`kv_set`/`kv_delete` wasmtime imports.
> Per-plugin namespaced state that survives across hook invocations.
> Lets plugins persist counters, budgets, and signatures without
> per-call data round-trips.

### Blog draft — *"Why we punted on stateful WASM and bridged the kitchen sink instead"*

WASM plugins in v0.3.0 were pure functions: every hook invocation got
a fresh `wasmtime::Store`, ran, and dropped state on the way out. That
was fine for query-shape rewrites, hopeless for anything that needed
to *count*. Token budgets, audit chains, anomaly-shaped detectors —
none of them work without persistence between calls.

v0.4.0 adds three host imports under the `env` module:
`env.kv_get(key_ptr, key_len, val_out_ptr, val_max_len) -> i32`,
`env.kv_set(key_ptr, key_len, val_ptr, val_len)`, and
`env.kv_delete(key_ptr, key_len)`. Return-value conventions follow the
"length or negative error" pattern Postgres clients are used to:
positive = bytes written; -1 = missing; -2 = caller buffer too small.

The crucial design decision is **per-plugin namespacing**. The
backend is `Arc<RwLock<HashMap<plugin_name, HashMap<key, value>>>>`;
each plugin sees only its own namespace, looked up via `caller.data()`
on the wasmtime `StoreCtx`. Plugin A cannot read plugin B's state
even with a malicious key. Operators can also seed a plugin's
namespace from the host side via `WasmPluginRuntime::kv()`, which is
how the Kubernetes operator injects per-tenant budgets at reconcile
time without going through WASM.

The `helios-plugin-abi` crate provides the safe Rust wrappers
(`kv_read`, `kv_write`, `kv_remove`) so plugin authors never touch
raw pointers. `cost-governor`, `audit-chain`, `token-budget`, and
`ai-classifier` use this directly to persist sliding-window counters,
hash chains, agent-cost ledgers, and per-request classification
results.

### Key files

| file | what to look at |
|---|---|
| `src/plugins/host_imports.rs` | `KvBackend` (the in-memory store) + `register_kv_imports` (wasmtime linker bindings). `StoreCtx` is the per-call state carried into host functions. |
| `src/plugins/runtime.rs` | `call_hook` builds the StoreCtx and registers the imports before instantiating each plugin. |
| Plugins repo: `abi/src/lib.rs` | `kv_read` / `kv_write` / `kv_remove` — the plugin-side wrappers around the imports. |
| Plugins repo: `cost-governor/src/lib.rs` | First real consumer — `decide_pre_query` reads tenant budget+usage; `observe_post_query` increments. |

---

## Module 4 — Plugin Host Crypto (`env.sha256_hex`)

### Card copy

> **Plugin Crypto** — `env.sha256_hex` host import backed by the
> audited `sha2` crate. Plugins compute real SHA-256 without
> embedding the algorithm (≈25 KiB saved per `.wasm`).

### Blog draft — *"Audit chains need real cryptography"*

The v0.3.0 `audit-chain` plugin shipped with a placeholder hash —
an FNV-flavoured mixer that produced a deterministic 256-bit digest
suitable for chain-integrity tests but explicitly not cryptographic.
The docs said so. Reviewers noticed.

v0.4.0 fixes that with a single host import:
`env.sha256_hex(in_ptr, in_len, out_ptr) -> i32`. The host computes
the digest using the production `sha2` crate, hex-encodes into a
fixed 64-byte stack buffer, and writes it back to the supplied
pointer. Returns `64` on success or `-1` on memory error.

Why a host import rather than a Rust-WASM `sha2` dep in every
plugin? Three reasons. **Size**: pulling `sha2` into a `cdylib`
target adds ~25 KiB to each `.wasm`; with seven plugins that's a
real savings. **Auditability**: a single host implementation is
easier to review than N copies sharing a transitive dep. **Constant-
time guarantees**: the host's Rust build can statically opt into
the constant-time codepath; the wasm build target can't promise
the same.

The audit-chain plugin now produces real SHA-256 digests in
production while keeping the FNV fallback gated on
`#[cfg(not(target_arch = "wasm32"))]` so unit tests stay
reproducible without wiring a wasmtime instance into `cargo test`.

### Key files

| file | what to look at |
|---|---|
| `src/plugins/host_imports.rs` | `register_crypto_imports` — the wasmtime binding. Hex encoding is inline + zero-allocation. |
| Plugins repo: `abi/src/lib.rs` | `sha256_digest_hex` — the safe wrapper. |
| Plugins repo: `audit-chain/src/lib.rs` | `sha256_hex` — the dual-mode helper (wasm32 calls the import, host build falls back to the FNV mixer). |
| `src/plugins/runtime.rs` (test) | `test_host_sha256_import_matches_rfc_6234_vector` — proves the import returns the canonical SHA-256("abc"). |

---

## Module 5 — Plugin Ed25519 Signature Verification

### Card copy

> **Plugin Signatures** — Optional Ed25519 trust root: drop `*.pub`
> files in a directory and every loaded `.wasm` requires a verifying
> `.sig` sidecar. Interoperates with `openssl`/`signify`.

### Blog draft — *"Distributing third-party plugins without a CA"*

Once plugins can come from anywhere (the next module covers the OCI
artefact format), you need a way to know what to trust. v0.4.0
ships an Ed25519 trust-root model: a directory of `*.pub` files
(base64-encoded raw 32-byte public keys) and a per-`.wasm` `.sig`
sidecar (base64-encoded raw 64-byte signature over the wasm bytes).

The format is plain on purpose. No PEM, no X.509 chain, no JSON
envelope, no JWS. Operators sign plugins with whatever Ed25519 tool
they already have:

```sh
openssl genpkey -algorithm Ed25519 -out signing.pem
openssl pkey -in signing.pem -pubout -outform DER | tail -c 32 \
  | base64 > /etc/helios/plugin-keys/official.pub
openssl pkeyutl -sign -inkey signing.pem -rawin -in plugin.wasm \
  | base64 -w 0 > plugin.sig
```

Drop `plugin.wasm` + `plugin.sig` in the plugin directory and set
`plugins.trust_root = "/etc/helios/plugin-keys"` in `proxy.toml`. On
load, `PluginLoader::SignatureVerifier::verify` walks the trust
root, finds the matching public key, logs the signer label
(`plugin signature verified, signed_by: official`), and instantiates.
A missing `.sig` or a tampered `.wasm` returns
`PluginLoadError::SignatureInvalid` and the plugin doesn't load.

### Key files

| file | what to look at |
|---|---|
| `src/plugins/loader.rs` | `SignatureVerifier::from_trust_root` (key-set load) and `SignatureVerifier::verify` (per-`.wasm` check). `PluginLoader::with_signature_verifier` is the attach point. |
| `src/plugins/config.rs` | `PluginRuntimeConfig.trust_root: Option<PathBuf>` — the config knob. |
| `src/config.rs` | `PluginToml.trust_root` — the TOML field. |
| `src/plugins/loader.rs` (tests) | Six tests cover happy path, tampered bytes, untrusted signer, malformed pubkey, multi-key trust root, and missing-sig rejection. |

---

## Module 6 — Plugin OCI Artefact Loader

### Card copy

> **OCI Plugin Artefacts** — `.tar.gz` distribution format with
> `manifest.json` + `plugin.wasm` + optional `plugin.sig`. The
> proxy loader detects `.gz` extension and ingests directly.

### Blog draft — *"Containerising plugins without containers"*

A `.wasm` file alone can't tell the loader what hooks it implements,
which version it claims, who signed it, or whether the bytes have
been tampered with. v0.4.0 wraps these into a portable `.tar.gz`
artefact:

```text
my-plugin-0.1.0.tar.gz
  ├─ manifest.json     # name, version, hooks, wasm_sha256, signature_*
  ├─ plugin.wasm
  └─ plugin.sig        # optional, base64 Ed25519
```

Manifests are versioned (`schema_version: "1.0"`); incompatible
majors fail to load. The wasm SHA-256 is recorded in the manifest at
pack time and recomputed at load time — a tampered tarball fails
integrity check before any plugin code runs. Signatures use the same
Ed25519 trust-root format as the loader itself, so one key directory
serves both `helios-plugin verify` (CLI side) and the proxy's runtime
loader.

The `helios-plugin` CLI (next module) packs and verifies; the proxy
loader detects `.tar.gz` extension via `PluginLoader::load_tar_gz`
and pulls the same three artefacts back out. End-to-end the chain
works: `cargo build --target wasm32-unknown-unknown --release` →
`helios-plugin pack` → drop in plugin dir → proxy loads, verifies,
instantiates.

### Key files

| file | what to look at |
|---|---|
| `src/plugins/loader.rs` | `load_tar_gz` — the loader-side implementation. Five tests cover round-trip, tampered wasm, schema-version mismatch, signed acceptance, missing-sig rejection. |
| Plugins repo: `cli/src/manifest.rs` | `Manifest` type definition. |
| Plugins repo: `cli/src/artefact.rs` | `pack` + `unpack` + `verify`. |
| `tests/wasm_plugin_e2e.rs` | `proxy_loads_packed_tar_gz_artefact` — full chain regression test that builds a tarball and loads it through the production runtime. |

---

## Module 7 — `RouteResult::Block` Plugin ABI

### Card copy

> **Plugin Route-Block** — `RouteResult::Block { reason }` ABI variant
> for hard-rejecting queries from a Route-hook plugin. Wire-compatible
> with `PreQueryResult::Block` — clients see one consistent error
> format.

### Blog draft — *"Two block paths, one client experience"*

Plugin authors had only one rejection signal in v0.3.0:
`PreQueryResult::Block` from the pre-query hook, which produces a
PostgreSQL `ErrorResponse` (SQLSTATE 42000) followed by
`ReadyForQuery`. But the `route` hook — which fires later in the
pipeline, after backend selection — had no equivalent. The
residency-router plugin worked around this by routing rejections to a
sentinel node name (`__residency_block__`) and letting the proxy fail
to resolve it; users got a confusing "node not found" error instead
of a clean access denial.

v0.4.0 adds `RouteResult::Block(String)` to the plugin ABI and
plumbs `RouteOverride::Block(reason)` through `route_and_forward` in
the server. A blocking route plugin now produces the same
`ErrorResponse` + `ReadyForQuery` pair as a blocking pre-query
plugin. Wire shape on the JSON side:
`{"action":"block","reason":"cross-region read forbidden"}`. The
deserialiser tolerates a missing `reason` and supplies "blocked by
plugin" so misconfigured plugins still produce a clear-enough error.

### Key files

| file | what to look at |
|---|---|
| `src/plugins/mod.rs` | `RouteResult::Block(String)` enum variant. |
| `src/plugins/runtime.rs` | The custom `Deserialize` impl on `RouteResult` — handles the new `block` action with separate `reason` field. |
| `src/server.rs` | `RouteOverride::Block` enum + `route_and_forward` short-circuit before backend selection. |
| Plugins repo: `residency-router/src/lib.rs` | First consumer — `ResidencyDecision::Block` now maps cleanly to `RouteResult::Block`. |

---

## Module 8 — `trust_root` Plugin Config Knob

### Card copy

> **Plugin Trust Root Config** — `[plugins].trust_root = "/path/to/keys"`
> in `proxy.toml`. Auto-attaches the SignatureVerifier when set;
> defaults to permissive when unset (dev-loop ergonomics preserved).

### Blog draft — *"One TOML knob to flip plugins from dev to prod"*

The signature verifier (Module 5) shipped as a programmatic API in the
loader. v0.4.0 wires it through to the runtime config so operators
flip a single TOML knob to require signed plugins:

```toml
[plugins]
enabled    = true
plugin_dir = "/var/lib/helios/plugins"
trust_root = "/etc/helios/plugin-keys"   # ← new in v0.4.0
```

When `trust_root` is present, `PluginManager::load_plugin` builds a
`SignatureVerifier::from_trust_root(...)` and attaches it to a fresh
`PluginLoader`. Every `.wasm` (or `.tar.gz` artefact) loaded
afterwards requires a matching signature. When `trust_root` is
absent, the loader is permissive — preserving the dev-loop where you
drop an unsigned `.wasm` and reload.

### Key files

| file | what to look at |
|---|---|
| `src/config.rs` | `PluginToml.trust_root: Option<String>` — the TOML field. |
| `src/plugins/config.rs` | `PluginRuntimeConfig.trust_root: Option<PathBuf>` + the `From<&PluginToml>` impl. |
| `src/plugins/mod.rs` | `PluginManager::load_plugin` — reads `runtime.config().trust_root` and conditionally attaches the verifier. |

---

## Module 9 — Admin Web UI

### Card copy

> **Admin Web UI** — Single embedded HTML dashboard at `/` and `/ui`
> on the admin port. Ten panels (Nodes, Topology, Plugins, Anomalies,
> Edge Mode, Chaos Mode, Shadow Execution, Time-Travel Replay, SQL,
> Traffic). Auto-refreshes every 5 s. No build step.

### Blog draft — *"A real dashboard in 1,200 lines of HTML"*

Most database proxies ship a stats endpoint and a wiki page.
HeliosProxy v0.4.0 ships a working dashboard — but it's a single
HTML file compiled into the binary via `include_str!`. No webpack,
no build pipeline, no separate container. Open `http://<proxy>:9090/`
and the page loads from the same admin port that serves
`/topology` and `/api/sql`.

Ten panels, all powered by `Promise.all` over the existing JSON
endpoints with a 5-second refresh tick:

- **Nodes** — health, failure count, latency, last check
- **Topology** — current primary, healthy/unhealthy counts,
  last failover (red badge when no primary is healthy)
- **Plugins** — name, version, hooks, state, invocation count,
  error count (red when > 0)
- **Anomalies** — recent detections from the new anomaly module
- **Edge Mode** — cache stats, registered edges, manual invalidate
- **Chaos Mode** — force-unhealthy buttons + active-overrides list
- **Shadow Execution** — diff a query across two backends
- **Time-Travel Replay** — replay a journal window onto a target
- **SQL** — ad-hoc query box with rendered result
- **Traffic** + **Cluster** — counters

Each panel is feature-gated: when the underlying endpoint returns 503
(feature not compiled in or component not attached), the panel shows
a friendly "X not attached" message rather than spinning.

### Key files

| file | what to look at |
|---|---|
| `src/admin_ui.html` | The whole dashboard — 1,200 lines of vanilla HTML+CSS+JS. Copy a panel block as a starting point for new ones. |
| `src/admin.rs` | `handle_connection` short-circuits `GET /` and `GET /ui` to serve the embedded HTML; everything else is JSON. |

---

## Module 10 — Admin REST API expansion

### Card copy

> **Admin REST v2** — Eight new endpoints surface every v0.4.0
> capability: `/topology`, `/plugins`, `/anomalies`, `/api/edge*`,
> `/api/chaos`, `/api/shadow`, `/api/replay`. Operator + UI consume
> the same JSON.

### Blog draft — *"One API surface for both humans and the operator"*

The Kubernetes operator polls `/topology` to populate
`HeliosProxyStatus.currentPrimary`. The admin Web UI calls the same
endpoint to render the Topology card. Same for `/plugins` →
operator-side health checks AND UI plugin panel. v0.4.0 deliberately
treats the admin REST surface as one API consumed by two clients.

The eight new endpoints:

| route | method | purpose |
|---|---|---|
| `/topology` | GET | currentPrimary, healthyNodes, unhealthyNodes, totalNodes, lastFailoverAt — the operator's status feed |
| `/plugins` | GET | loaded plugin list with hooks, state, invocations, errors |
| `/anomalies?limit=N` | GET | recent anomaly detections (newest first) |
| `/api/edge` | GET | edge cache stats + registered edges |
| `/api/edge/register` | POST | edge announces itself to home |
| `/api/edge/invalidate` | POST | manual invalidation broadcast |
| `/api/chaos` | GET / POST | active overrides / force-unhealthy / restore / reset |
| `/api/shadow` | POST | diff a query across two backends |
| `/api/replay` | POST | replay a journal window against a target backend |

Status codes are honest. `503` when the corresponding feature isn't
compiled in. `400` on malformed bodies. `404` when the named target
node is unknown. `500` only when the proxy itself fails (e.g. the
shadow source connect refused).

### Key files

| file | what to look at |
|---|---|
| `src/admin.rs` | `route_request` — single `match` over `(method, path)` covering every endpoint. Each handler is a separate `async fn handle_X`. |
| `src/admin.rs` (tests) | 30+ admin tests cover happy path + 503/400/404/500 failure modes for every new endpoint. |

---

## Module 11 — Plugin: `cost-governor`

### Card copy

> **Plugin: cost-governor** — Per-tenant query cost budgets (minute
> / hour / day windows). Sliding window stored in the plugin's KV
> namespace; configurable thresholds per tenant via the operator's
> `TenantQuota` CRD.

### Blog draft — *"Budget the database the way you budget the cloud bill"*

`cost-governor` is the first plugin migrated off the v0.3.x
attribute-injection workaround and onto the new host KV bridge
(Module 3). It enforces a three-window budget per tenant — minute,
hour, day — using a deterministic cost model:

```text
cost = α × rows_scanned + β × wall_time_ms
α = 1e-6, β = 1e-3
```

`pre_query` reads `tenant:<id>:budget` and `tenant:<id>:usage` from
KV, runs `check_budget` (day overrides hour overrides minute), and
returns `PreQueryResult::Block { reason }` with a structured
"retry in Ns" message when exhausted. `post_query` increments the
sliding-window counter and writes back via `kv_write`.

The Kubernetes operator's `TenantQuota` reconciler seeds the
budget side of the namespace through the host-side
`WasmPluginRuntime::kv()` accessor; the plugin never has to
contact the control plane. That seam keeps tenant provisioning out
of the data path entirely.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `cost-governor/src/lib.rs` | `decide_pre_query` + `observe_post_query` + `check_budget` (day-precedence logic). 8 unit tests cover every threshold + window. |
| `tests/wasm_plugin_e2e.rs` | `cost_governor_blocks_when_budget_exhausted_via_kv` — end-to-end test that seeds an over-quota tenant and asserts Block. |

---

## Module 12 — Plugin: `ai-classifier`

### Card copy

> **Plugin: ai-classifier** — Detects LLM-generated SQL via
> `application_name` keywords, generated-by markers, opt-in
> attributes. Best-effort `agent_id` + `model_id` extraction.
> Persists per-request tags into KV for downstream plugins.

### Blog draft — *"Telling agent traffic apart from human traffic"*

LLM agents now write a meaningful slice of SQL hitting production
databases. `ai-classifier` runs as a `pre_query` hook and tags
those requests so downstream plugins (token-budget, llm-guardrail)
can apply different policy.

Three signals fire in priority order: `application_name` containing
LLM keywords (`gpt`, `claude`, `gemini`, `chatbot`, `agent`,
`openai`, `anthropic`); explicit opt-in via session attribute
`helios.ai_traffic = true`; or a `/* generated by GPT-4 */`-style
marker in the SQL itself. On positive match the plugin best-
effort extracts `agent_id` (from `application_name`) and `model_id`
(from a substring check against known model names), then writes
three keys to its KV namespace:

```
req:<request_id>:ai_traffic = "true"
req:<request_id>:agent_id   = "<extracted>"
req:<request_id>:model_id   = "<guessed>"
```

Downstream plugins read these via the same `request_id`. The
plugin always returns `PreQueryResult::Continue` — its job is
labelling, not gating.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `ai-classifier/src/lib.rs` | `classify` returns `ClassifyResult { ai_traffic, agent_id, model_id }`; `guess_model` is the LLM-name substring match. 6 unit tests. |
| `tests/wasm_plugin_e2e.rs` | `ai_classifier_writes_request_keys_into_kv` — end-to-end test that fires the plugin against a `/* generated by GPT-4 */` query and asserts the three KV keys. |

---

## Module 13 — Plugin: `token-budget`

### Card copy

> **Plugin: token-budget** — Per-`(agent, model)` cost gating for
> AI traffic. Sliding-window (minute + day) tracked in KV; blocks
> when budget exhausted. Cost model assumes ≈ 4 bytes per token.

### Blog draft — *"Putting a meter on the LLM ↔ database loop"*

LLM agents loop. They call your database, read the result, decide
their next call, run that, decide the next... and a runaway agent
can cost more than the entire human workforce. `token-budget`
reads the `agent_id` + `model_id` tags `ai-classifier` set, looks
up the per-`(agent, model)` budget from KV, and blocks when
exhausted.

The cost estimator approximates LLM token cost from response size:
`tokens ≈ response_bytes / 4`. That's deliberately rough — exact
token counts require the model's tokeniser, which the proxy
doesn't have. The approximation is good enough for budget
enforcement at minute/day granularity; the proxy's job is to stop
runaways, not to precisely bill.

`pre_query` returns `Block` when the rolling window exceeds the
budget. `post_query` adds the just-completed query's estimated
cost to the running total via `kv_write`. Untagged queries (no
`agent_id` in the context) pass through untouched — you only pay
the policy cost on AI-flagged traffic.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `token-budget/src/lib.rs` | `decide_pre_query` + `estimate_token_cost`. Day budget overrides minute (same precedence as cost-governor). 7 unit tests. |
| Plugins repo: `ai-classifier/src/lib.rs` | Upstream that sets `agent_id`/`model_id`. |

---

## Module 14 — Plugin: `llm-guardrail`

### Card copy

> **Plugin: llm-guardrail** — Refuses dangerous SQL from AI traffic:
> DROP/TRUNCATE, DELETE/UPDATE without WHERE, SELECT without LIMIT
> against large tables, missing tenant_id filter on tenant-scoped
> tables. Self-contained heuristic — useful even without
> ai-classifier deployed.

### Blog draft — *"Defaults that survive a hallucinating agent"*

LLM agents hallucinate. They write `DROP TABLE users` when they
meant to write `DELETE FROM temp_users WHERE id = $1`. They write
`SELECT * FROM events` when they meant to write the same with a
`LIMIT 100`. They forget the multi-tenancy filter the application
layer normally adds.

`llm-guardrail` is the safety net. For queries flagged as AI
traffic — either via `ai-classifier`'s tags or by the plugin's own
self-contained heuristic on `application_name` — it scans the SQL
and refuses four payload classes:

| pattern | rejection |
|---|---|
| `DROP` or `TRUNCATE` (any form) | "DROP/TRUNCATE forbidden in LLM-tagged traffic" |
| `DELETE` / `UPDATE` without ` WHERE ` | "DELETE/UPDATE without WHERE forbidden" |
| `SELECT` against `large_tables` without ` LIMIT ` | "SELECT without LIMIT against large table" |
| `SELECT` against `tenant_scoped_tables` without `tenant_id` | "missing tenant_id filter on tenant-scoped table" |

Tables are configurable lists at the plugin level. Returns
`PreQueryResult::Block { reason: "llm-guardrail: <reason>" }` —
the reason flows back to the client via the standard PG
`ErrorResponse`.

The "self-contained heuristic" matters because it means the
guardrail works on day one. You don't have to deploy
ai-classifier first; the plugin can decide on its own that
`application_name=claude-bot` is AI traffic and apply the rules.
Once ai-classifier is deployed, the guardrail benefits from its
richer signal, but it doesn't depend on it.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `llm-guardrail/src/lib.rs` | `evaluate` — the four rule checks; `is_ai_traffic` — the self-contained classifier. 8 unit tests. |
| Plugins repo: `ai-classifier/src/lib.rs` | Optional upstream that improves classification quality. |

---

## Module 15 — Plugin: `pgvector-router`

### Card copy

> **Plugin: pgvector-router** — Routes pgvector top-K queries
> (`<->`, `<#>`, `<=>` in `ORDER BY`) to a topology-tagged
> vector replica. Falls back to default routing when no tagged
> node exists.

### Blog draft — *"Sending vector queries where the index is hot"*

pgvector's HNSW + IVF indexes are large, in-memory, and slow to
warm. Spreading vector queries across every replica defeats the
caching the index relies on. `pgvector-router` consolidates them
on a designated replica.

The detector is two checks: (1) the SQL contains one of the three
distance operators (`<->` L2, `<#>` inner product, `<=>` cosine);
(2) the SQL contains `ORDER BY` (operator without ORDER BY is
usually incidental — comparing distances in a CTE doesn't need
the special replica). When both fire, the plugin returns
`RouteResult::Node { target: <vector_node> }`.

The `<vector_node>` value comes from the request's
`hook_context.attributes["helios.vector_node"]`, which the
operator's `RoutingRule` reconciler can populate from a `vector:`
node tag in the topology. When the attribute is absent, the
plugin returns `RouteResult::Default` and the proxy's standard
read routing applies. Best-effort, no breakage on misconfigured
topologies.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `pgvector-router/src/lib.rs` | `classify` is the pure-function detector. 6 unit tests including `no_order_by_returns_default` and `empty_vector_node_falls_back_to_default`. |
| `tests/wasm_plugin_e2e.rs` | `pgvector_router_returns_node_for_top_k_query` — end-to-end test against the production runtime. |

---

## Module 16 — Plugin: `column-mask`

### Card copy

> **Plugin: column-mask** — Per-role column masking via SQL
> rewriting. `SELECT ssn` becomes `SELECT mask_ssn(ssn) AS ssn`
> when the user lacks the `pii_reader` role. Rules stored in KV;
> idempotent on re-application.

### Blog draft — *"Per-role PII masking without schema changes"*

The traditional way to mask PII per-role is to install database
views, grant on the views, revoke on the underlying tables, and
hope the application layer never bypasses the views. That's a
schema-change story with an audit-trail requirement; small teams
default to "everyone can read everything" because the alternative
is too much work.

`column-mask` puts the masking at the proxy. Mask rules are stored
in the plugin's KV namespace as JSON-serialised
`Vec<MaskRule { table, column, mask_function, unmask_role }>`,
seeded by the operator's `AuditPolicy` reconciler. On every
`pre_query`:

1. Look up the user's roles from `hook_context.attributes`.
2. For every rule whose `table` appears in the SQL and whose
   `unmask_role` is *not* in the user's roles…
3. …rewrite bare `<column>` references to
   `<mask_function>(<column>) AS <column>`.

The mask functions (`mask_ssn`, `mask_email`, etc.) are SQL-side
helpers the operator installs in the database (one-time DDL, kept
out of the audit chain). The plugin returns
`PreQueryResult::Rewrite { sql }` when at least one rule fired.

Rewriting is intentionally substring-based, not full SQL parsing
— a parser big enough to be correct is also too big for a
production WASM module. The whole-word matcher avoids the
obvious false positives (`_ssn`, `ssna`), and the plugin is
idempotent: a second pass over already-rewritten SQL is a no-op
(it skips columns whose `mask_function(` already appears).

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `column-mask/src/lib.rs` | `apply_rules` — the rewrite loop; `find_word` — whole-word matcher; `mask_for` — pure-function decision. 8 unit tests including idempotence. |

---

## Module 17 — Plugin: `audit-chain`

### Card copy

> **Plugin: audit-chain** — Hash-chained tamper-evident audit log.
> Every query record embeds SHA-256 of the previous; modifying any
> entry breaks the chain. Real cryptography via `env.sha256_hex`
> (was a placeholder in v0.3.x).

### Blog draft — *"Audit logs that detect tampering, not just record events"*

A normal audit log is a database table. A normal database admin can
edit the table. A normal incident response team has to trust the
table. Hash-chained audit logs invert this: every record carries
the SHA-256 of the previous record's bytes; auditors recompute the
chain and any modified record breaks the link to the next.

`audit-chain` runs as a `post_query` hook. Each completed query
appends an `AuditRecord { seq, timestamp, prev_hash, client_ip,
identity, query_fingerprint, target_node, elapsed_us, success,
error }` to the chain. The plugin's KV namespace stores three
keys:

```
seq               # u64 LE — next sequence number
tail_hash         # hex SHA-256 of the most recent record
record:<seq>      # JSON of AuditRecord
```

`verify_chain` walks the records and returns the index of the first
broken link. Production deployments would also flush records to S3
or RFC-3161 timestamping for off-host immutability — that's the
companion `[backend]` config in the `AuditPolicy` CRD.

The cryptography is now real. v0.3.x shipped a deterministic FNV
mixer with a doc note saying "placeholder." v0.4.0's `sha256_hex`
helper delegates to the proxy's `env.sha256_hex` host import on
`wasm32` (production) and falls back to the FNV mixer on the host
target so unit tests stay reproducible.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `audit-chain/src/lib.rs` | `record_hash` + `verify_chain` (auditor side) + `build_record` (writer side). The `sha256_hex` helper is the cfg-gated dispatch. 5 unit tests cover chain build, tamper detection, normalised-vs-raw fingerprint. |

---

## Module 18 — Plugin: `residency-router`

### Card copy

> **Plugin: residency-router** — Per-user data-residency routing.
> Reads user region from `helios.region` attribute; routes to a
> tagged in-region replica or returns `RouteResult::Block` with a
> proper PG ErrorResponse when no in-region node exists.

### Blog draft — *"Compliance routing for users who never leave their region"*

GDPR, India's DPDP, China's PIPL, Schrems II — every regulator
has slightly different data-residency rules, and "data only ever
touches replicas in the user's region" is the safest answer to
all of them. `residency-router` enforces this at routing time.

The plugin reads two pieces of state: the user's region (from
`hook_context.attributes["helios.region"]`, set by an
`Authenticate` plugin upstream) and a region → node map plus
`enforce` flag stored in KV. It returns one of three decisions:

| condition | result |
|---|---|
| no region attribute | `RouteResult::Default` (tenant didn't opt in) |
| region matches a tagged node | `RouteResult::Node { target }` |
| region unknown AND `enforce` | `RouteResult::Block { reason: "no in-region replica for user" }` |
| region unknown AND `!enforce` | `RouteResult::Default` (pre-prod ergonomics) |

The Block path uses the new `RouteResult::Block` ABI variant
(Module 7) — clients see a clean PostgreSQL `ErrorResponse`
instead of the v0.3.x sentinel-target hack.

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `residency-router/src/lib.rs` | `decide` — pure-function decision over `(user_region, ResidencyConfig)`. 6 unit tests. |

---

## Module 19 — `helios-plugin` CLI

### Card copy

> **`helios-plugin` CLI** — Pack, inspect, and verify WASM plugin
> artefacts as portable `.tar.gz`. Same Ed25519 trust-root format
> as the proxy loader. Interoperates with `openssl`/`signify`.

### Blog draft — *"`helios-plugin pack` is what `docker build` is for containers"*

Plugin authors compile their `.wasm`, sign it, and pack it into a
distribution artefact. Three commands:

```sh
helios-plugin pack    --wasm <path> --name X --version 1.0 \
                      --hooks pre_query,post_query [--sig <path>] \
                      --out <path>
helios-plugin inspect <artefact.tar.gz>
helios-plugin verify  <artefact.tar.gz> --trust-root <dir>
```

`pack` reads the `.wasm`, computes its SHA-256, embeds the manifest
(name, version, hooks, license, optional signature metadata,
RFC 3339 packed-at), and writes the gzipped tarball. `inspect`
prints the manifest. `verify` recomputes the SHA-256, base64-decodes
the signature, and walks every `*.pub` in the trust root looking
for one that verifies — printing `OK — signed by <label>` on
success.

The CLI uses the same trust-root + signature format as the proxy
loader, so one `keys/` directory works for both build-time
verification and run-time loading. Operators can include `verify`
in CI:

```sh
helios-plugin verify build/my-plugin-1.0.0.tar.gz --trust-root release-keys/
```

### Key files

| file | what to look at |
|---|---|
| Plugins repo: `cli/src/main.rs` | `clap` subcommand definitions + the three-call dispatch. |
| Plugins repo: `cli/src/artefact.rs` | `pack` + `unpack` + `verify` — 11 unit tests including tampered-wasm rejection and verify-against-untrusted-signer. |
| Plugins repo: `cli/src/manifest.rs` | `Manifest` schema (versioned). |

---

## Module 20 — Kubernetes Operator (`HDB-HeliosDB-Proxy-Operator`)

### Card copy

> **Kubernetes Operator** — CRDs for HeliosProxy, PoolProfile,
> RoutingRule, AuditPolicy, TenantQuota. Reconciler renders
> ConfigMap + Deployment + Service per CR; polls `/topology` to
> populate status. Owned objects auto-clean on `kubectl delete`.

### Blog draft — *"One CR, one full HeliosProxy stack"*

`heliosproxy.dev/v1alpha1` declares five CRDs:

```yaml
apiVersion: heliosproxy.dev/v1alpha1
kind: HeliosProxy           # the proxy instance + node list
kind: PoolProfile           # per-instance pool tuning
kind: RoutingRule           # routing-hints
kind: AuditPolicy           # audit-chain + masking config
kind: TenantQuota           # per-tenant limits + cost budgets
```

Apply a `HeliosProxy` and the reconciler:

1. **Resolves refs.** Looks up referenced sub-CRDs; surfaces a
   `RefMissing` condition for each unresolved ref but proceeds
   (the proxy can boot from inline-spec values).
2. **Renders proxy.toml** from the merged spec.
3. **Owns three objects**: a `ConfigMap` with the rendered TOML,
   a `Deployment` with the right replicas + image + ports + CM
   mount + liveness/readiness probes, and a ClusterIP `Service`
   exposing the postgres + admin ports. All have
   `OwnerReference`s so `kubectl delete heliosproxy <name>`
   cleans up the stack.
4. **Drives a config-hash annotation** on the pod template — when
   the rendered TOML changes the deployment rolls automatically.
5. **Polls `/topology`** on a 5-second cadence and updates
   `status.currentPrimary` / `healthyNodes` / `unhealthyNodes`.

Status transitions are honest: `Pending` until any pod is ready,
`Degraded` while `ReadyReplicas < spec.Replicas`, `Ready` when
they match. The polling client hard-fails over after 3 seconds so
a hung proxy doesn't block reconcile.

### Key files

| file | what to look at |
|---|---|
| Operator repo: `api/v1alpha1/*.go` | The five CRD type definitions + kubebuilder markers. |
| Operator repo: `internal/controller/render.go` | `renderConfig` (TOML), `renderConfigMap`, `renderDeployment`, `renderService`. 12 unit tests. |
| Operator repo: `internal/controller/heliosproxy_controller.go` | The full reconcile loop including ref resolution + status writeback. |
| Operator repo: `internal/controller/topology.go` | The `/topology` polling client. |

---

## Module 21 — Terraform Provider (`terraform-provider-HDB-HeliosDB-Proxy`)

### Card copy

> **Terraform Provider** — Five resources mirror the operator
> CRDs: `heliosproxy_instance`, `_pool_profile`, `_routing_rule`,
> `_audit_policy`, `_tenant_quota`. Schema generated from the
> operator's Go types via local `replace`.

### Blog draft — *"Declare HeliosProxy from `main.tf`"*

The Terraform provider wraps the operator: each resource calls
`controller-runtime/client` to apply the corresponding CRD against
the cluster. Five resources mirror the five CRDs one-for-one.

```hcl
provider "heliosproxy" {
  namespace = "data"
}

resource "heliosproxy_pool_profile" "default" {
  name = "default-pool"
  mode = "transaction"
  max_pool_size = 200
}

resource "heliosproxy_instance" "analytics" {
  name     = "analytics"
  replicas = 2
  image    = "ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.0"
  pool_profile_ref = heliosproxy_pool_profile.default.name
  nodes = [
    { host = "pg-primary.db.svc",   port = 5432, role = "primary", weight = 100 },
    { host = "pg-standby.db.svc",   port = 5432, role = "standby", weight = 100 },
  ]
}
```

The schema doesn't drift from the operator because it imports the
operator's Go types directly via a local `replace` directive.
Adding a field to the operator's CRD makes it available in the
provider on the next `make tidy && go build`.

### Key files

| file | what to look at |
|---|---|
| Terraform repo: `internal/provider/provider.go` | `New` factory + `Configure` (kubeconfig loading + namespace default). |
| Terraform repo: `internal/provider/instance_resource.go` | Largest of the five — composes nested blocks for nodes, pool, plugins, refs. |
| Terraform repo: `examples/full/main.tf` | End-to-end example wiring all five resources together. |

---

## Module 22 — Pulumi Provider (`pulumi-HDB-HeliosDB-Proxy`)

### Card copy

> **Pulumi Provider** — Wraps the Terraform provider via
> pulumi-terraform-bridge. Same five resources surfaced as
> first-class Pulumi types in TypeScript / Python / Go / .NET.

### Blog draft — *"Pulumi without rewriting the resource layer"*

Building a Pulumi provider from scratch is weeks of work. Building
one *on top of an existing Terraform provider* is hours.
`pulumi-HDB-HeliosDB-Proxy` does the latter via
`pulumi-terraform-bridge`'s plugin-framework variant (`pf`).

The bridge consumes the Terraform provider's schema (via the
`pkg/provider.New` re-export the Terraform repo added in v0.4.0)
and emits Pulumi-shaped SDKs in four languages: TypeScript,
Python, Go, .NET. The five resources `heliosproxy.Instance`,
`heliosproxy.PoolProfile`, `heliosproxy.RoutingRule`,
`heliosproxy.AuditPolicy`, `heliosproxy.TenantQuota` come over
1:1.

Two cmd binaries: `pulumi-tfgen-heliosproxy` generates the
Pulumi schema; `pulumi-resource-heliosproxy` is the RPC server
`pulumi up` launches and embeds the schema via `//go:embed`.
`make build` produces both binaries plus the SDKs. Operators
who already use Pulumi don't need to context-switch to HCL.

### Key files

| file | what to look at |
|---|---|
| Pulumi repo: `provider/resources.go` | `Provider()` returns `tfbridge.ProviderInfo` — the bridge config (token mappings, SDK package names, doc URLs). |
| Pulumi repo: `provider/cmd/pulumi-resource-heliosproxy/main.go` | RPC server entry point. |
| Pulumi repo: `provider/cmd/pulumi-tfgen-heliosproxy/main.go` | Schema generator. |
| Pulumi repo: `examples/typescript/index.ts` | TypeScript program mirroring the Terraform `examples/full/main.tf`. |

---

## Cross-cutting note for the website team

Two repository naming choices are worth surfacing on the page so
external contributors land in the right place:

1. **The new repo names follow `HDB-HeliosDB-Proxy*` for in-house
   code**, but `terraform-provider-` and `pulumi-` keep their
   ecosystem-conventional prefixes
   (`terraform-provider-HDB-HeliosDB-Proxy`,
   `pulumi-HDB-HeliosDB-Proxy`) because Terraform Registry and
   Pulumi expect those prefixes for discovery.
2. **The container image is lowercase** (`hdb-heliosdb-proxy`)
   because GHCR forces lowercase package names. The GitHub repo
   keeps the mixed-case `HDB-HeliosDB-Proxy` form.

Both are minor surprises if you assume one canonical name; the
above mapping prevents tickets from contributors who can't find
the repo or pull the image.
