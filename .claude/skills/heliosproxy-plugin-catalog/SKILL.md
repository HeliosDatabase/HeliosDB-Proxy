---
name: heliosproxy-plugin-catalog
description: Catalog of the eight first-party plugins (cost-governor, ai-classifier, token-budget, llm-guardrail, pgvector-router, column-mask, audit-chain, residency-router). What each fires on, what it gates, what KV keys it reads. Use when the user says "which plugin does X", "I need to mask PII", "I need to gate AI traffic", "guardrails", "how does residency-router decide".
allowed-tools: Read, Bash(curl *)
related: [heliosproxy-overview, heliosproxy-plugin-pack, heliosproxy-plugin-load, heliosproxy-plugin-kv]
---

# First-party plugin catalog

Eight plugins ship in the
[`HDB-HeliosDB-Proxy-Plugins`](https://github.com/dimensigon/HDB-HeliosDB-Proxy-Plugins)
sibling repo. Each is independently versioned and packed as a
standalone OCI artefact. This skill is the at-a-glance "which
plugin do I want" reference.

🔵 Read-only navigation. Loading a plugin and configuring it are
separate skills (`heliosproxy-plugin-load`, `heliosproxy-plugin-kv`).

## Quick chooser

| If you need to … | Plugin |
|---|---|
| Cap per-tenant query cost / rate / cumulative spend | `helios-plugin-cost-governor` |
| Detect LLM-generated traffic (User-Agent / app_name / SQL shape) | `helios-plugin-ai-classifier` |
| Gate per-(agent, model) token / cost spend | `helios-plugin-token-budget` |
| Refuse dangerous LLM-sourced SQL (DROP, missing WHERE, full scan) | `helios-plugin-llm-guardrail` |
| Route HNSW vector queries to a vector replica | `helios-plugin-pgvector-router` |
| Mask PII columns at proxy (rewrite `SELECT ssn` → `mask_ssn(ssn)`) | `helios-plugin-column-mask` |
| Tamper-evident audit log (hash-chained entries) | `helios-plugin-audit-chain` |
| Per-region routing + cross-region read refusal | `helios-plugin-residency-router` |

## Per-plugin reference

### `helios-plugin-cost-governor`

| Aspect | Detail |
|---|---|
| Hooks fired   | `pre_query` (gate), `post_query` (charge) |
| Scope         | Per-tenant (read from `app_name=tenant:<id>` or session var) |
| KV keys read  | `budget/<tenant>` (JSON: `{queries_per_minute, cost_units_per_hour}`) |
| What it does  | Refuses queries when the tenant exceeds budget; recovers when window resets |
| Demo          | [`demos/v0.4.0/11-cost-governor/`](../../demos/v0.4.0/11-cost-governor/) |

### `helios-plugin-ai-classifier`

| Aspect | Detail |
|---|---|
| Hooks fired   | `pre_query` |
| Scope         | Per-query, tags `hook_context.is_ai = true` |
| KV keys read  | `agents` (allowlist of known UA / app_name patterns) |
| What it does  | Sets a flag downstream plugins (token-budget, llm-guardrail) read |
| Demo          | [`demos/v0.4.0/12-ai-classifier/`](../../demos/v0.4.0/12-ai-classifier/) |

### `helios-plugin-token-budget`

| Aspect | Detail |
|---|---|
| Hooks fired   | `pre_query` (gate), `post_query` (charge) |
| Scope         | Per-(agent, model) where agent comes from ai-classifier |
| KV keys read  | `budget/<agent>/<model>` (JSON: `{tokens_per_hour, cost_usd_per_hour}`) |
| Pairs with    | `helios-plugin-ai-classifier` (must run first) |
| Demo          | [`demos/v0.4.0/13-token-budget/`](../../demos/v0.4.0/13-token-budget/) |

### `helios-plugin-llm-guardrail`

| Aspect | Detail |
|---|---|
| Hooks fired   | `pre_query` |
| Scope         | Only fires when `hook_context.is_ai = true` |
| KV keys read  | `rules` (JSON list of refusal patterns) |
| What it does  | Refuses DROP, missing WHERE on UPDATE/DELETE, missing tenant_id on multi-tenant tables, full-table scans on configured tables |
| Pairs with    | `helios-plugin-ai-classifier` |
| Demo          | [`demos/v0.4.0/14-llm-guardrail/`](../../demos/v0.4.0/14-llm-guardrail/) |

### `helios-plugin-pgvector-router`

| Aspect | Detail |
|---|---|
| Hooks fired   | `route` |
| Scope         | Queries with `<=>` distance operator AND `ORDER BY ... LIMIT N` |
| KV keys read  | `vector_node` (target replica address) |
| What it does  | Forces top-K vector queries onto the configured replica; lets other queries flow normally |
| Demo          | [`demos/v0.4.0/15-pgvector-router/`](../../demos/v0.4.0/15-pgvector-router/) |

### `helios-plugin-column-mask`

| Aspect | Detail |
|---|---|
| Hooks fired   | `pre_query` (rewrite) |
| Scope         | Per-role; mask rules vary by `current_user` |
| KV keys read  | `rules/<role>` (JSON: `{table.column: mask_function}`) |
| What it does  | Rewrites `SELECT ssn` → `SELECT mask_ssn(ssn)` for restricted roles |
| Demo          | [`demos/v0.4.0/16-column-mask/`](../../demos/v0.4.0/16-column-mask/) |

### `helios-plugin-audit-chain`

| Aspect | Detail |
|---|---|
| Hooks fired   | `post_query` |
| Scope         | Every successful query (read or write) |
| KV keys read  | `prev_hash` (state — last hash in the chain) |
| What it does  | Writes a hash-chained audit entry per query: `H(prev_hash || query || timestamp)`. Detect tampering with `verify_chain`. |
| Demo          | [`demos/v0.4.0/17-audit-chain/`](../../demos/v0.4.0/17-audit-chain/) |

### `helios-plugin-residency-router`

| Aspect | Detail |
|---|---|
| Hooks fired   | `route` |
| Scope         | Per-session (reads `helios.region` session variable) |
| KV keys read  | `region_map` (`[[region, node:port], ...]`), `enforce` (bool) |
| What it does  | Routes the query to the in-region replica; with `enforce=true`, refuses queries from regions with no mapping |
| Demo          | [`demos/v0.4.0/18-residency-router/`](../../demos/v0.4.0/18-residency-router/) |

## How they compose

Multiple plugins can hook the same query. Order is determined by
the order plugins are loaded (filename-sorted under `plugin_dir`).
Typical AI/RAG stack:

```
1. helios-plugin-ai-classifier   (sets is_ai)
2. helios-plugin-llm-guardrail   (refuses dangerous AI queries)
3. helios-plugin-token-budget    (gates AI spend)
4. helios-plugin-cost-governor   (gates per-tenant spend)
5. helios-plugin-pgvector-router (route hint for vector queries)
```

Compliance stack:

```
1. helios-plugin-residency-router (route)
2. helios-plugin-column-mask      (rewrite for masking)
3. helios-plugin-audit-chain      (post-query log)
```

The plugins don't communicate directly — they share the
`hook_context` carrier on the request, and they each read what they
need from KV.

## Pitfalls

- **Order matters.** `llm-guardrail` runs only when `is_ai`, set
  by `ai-classifier`. Load classifier first.
- **All plugins are advisory unless gated.** A `pre_query` hook
  must explicitly `Block` the query for it to be refused. A
  classifier that just sets a flag doesn't reject anything.
- **`helios-plugin-column-mask` requires SQL functions in the
  database** (`mask_ssn`, `mask_email` etc — see `_shared/init.sql`
  in the demos). The plugin only rewrites; the masking happens at
  the DB.
- **`audit-chain` doesn't ship the audit log anywhere.** It writes
  per-entry to KV (or stdout — depends on plugin config). For
  compliance, ship from KV to S3 / GCS via a sidecar.
- **Plugin pairs share `hook_context` only within one request.**
  The carrier is per-message, not per-session.

## See also

- `heliosproxy-plugin-pack` — build / sign these
- `heliosproxy-plugin-load` — drop them into the proxy
- `heliosproxy-plugin-kv` — runtime config knobs
- Sibling repo: <https://github.com/dimensigon/HDB-HeliosDB-Proxy-Plugins>
- Demos: [`demos/v0.4.0/{11..18}-*`](../../demos/v0.4.0/) — one demo per plugin
