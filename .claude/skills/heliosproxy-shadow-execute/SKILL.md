---
name: heliosproxy-shadow-execute
description: Run a query against two backends in parallel and diff the results. Validates that a new-version replica matches the source. Use when the user says "shadow", "/api/shadow", "diff results", "validate migration", "PG-version upgrade test".
allowed-tools: Bash(curl *), Bash(jq *)
related: [heliosproxy-overview, heliosproxy-time-travel]
---

# Shadow execute

`POST /api/shadow` runs a single SQL statement against a "source"
backend and a "shadow" backend, captures both results, and reports
whether they match. Order-independent row-set hash, so non-
deterministic ORDER BY tolerant.

Requires the `ha-tr` feature compiled in.

## When to use

- Validating that a new-PG-version replica produces identical
  results to the old primary
- Testing a query rewriter / plugin transformation: original SQL
  vs rewritten on the same data should match
- Schema-migration safety check: same query before / after a
  schema change should match for the unchanged subset

🟠 Mutating against shadow if the query is a write — *but* writes
land on both sides and may diverge transactional state. Use only
for SELECT-class queries unless you understand the consequences.

## Request shape

```json
{
  "sql":              "SELECT id, email FROM users WHERE id < 100",
  "params":           [],
  "source_host":      "pg-primary",
  "source_port":      5432,
  "source_user":      "postgres",
  "source_password":  "...",
  "source_database":  "demo",
  "shadow_host":      "pg17-staging",
  "shadow_port":      5432,
  "shadow_user":      "postgres",
  "shadow_password":  "...",
  "shadow_database":  "demo"
}
```

## Response shape

```json
{
  "both_succeeded":     true,
  "row_count_match":    true,
  "row_hash_match":     true,
  "is_clean":           true,
  "primary_elapsed_us": 2341,
  "shadow_elapsed_us":  2802,
  "primary_error":      null,
  "shadow_error":       null
}
```

`is_clean = both_succeeded && row_count_match && row_hash_match`.

## Recipes

### Recipe 1: One-shot shadow query

```bash
curl -s -X POST http://localhost:9090/api/shadow \
  -H 'Content-Type: application/json' \
  -d '{
    "sql":             "SELECT count(*) FROM events WHERE created_at > now() - interval ''1 day''",
    "source_host":     "pg-primary", "source_port": 5432,
    "source_user":     "postgres",   "source_password":"x",
    "source_database": "demo",
    "shadow_host":     "pg17-staging","shadow_port": 5432,
    "shadow_user":     "postgres",   "shadow_password":"x",
    "shadow_database": "demo"
  }' | jq .
```

`is_clean: true` is your green-light. Anything else: read the
specific match flags.

### Recipe 2: Sweep a regression suite

```bash
queries=(
  "SELECT count(*) FROM users"
  "SELECT id, email FROM users ORDER BY id LIMIT 10"
  "SELECT category, count(*) FROM products GROUP BY category"
  "SELECT * FROM orders WHERE status = 'pending'"
)

for q in "${queries[@]}"; do
  result=$(curl -s -X POST http://localhost:9090/api/shadow \
    -H 'Content-Type: application/json' \
    -d "$(jq -nR --arg sql "$q" '{
      sql: $sql,
      source_host:"pg-primary",  source_port:5432,
      source_user:"postgres",    source_password:"x",  source_database:"demo",
      shadow_host:"pg17-staging",shadow_port:5432,
      shadow_user:"postgres",    shadow_password:"x",  shadow_database:"demo"}')" \
    | jq -r .is_clean)
  echo "${result}  ${q}"
done
# true   SELECT count(*) FROM users
# true   SELECT id, email FROM users ORDER BY id LIMIT 10
# false  SELECT category, count(*) FROM products GROUP BY category
# true   SELECT * FROM orders WHERE status = 'pending'
```

The one `false` flags a divergence on `products` — drill into it
with the per-flag fields:

```bash
curl ... | jq '{both_succeeded, row_count_match, row_hash_match, primary_error, shadow_error}'
```

### Recipe 3: Validate a query-rewriter plugin

Run the same SQL twice — once with the plugin enabled (proxy port),
once with it disabled (direct PG):

```bash
# rewriter on
psql -h proxy   -p 6432 -U postgres -d demo -c "SELECT * FROM users WHERE email = 'a@b.com'"

# rewriter off — direct
psql -h pg-primary -p 5432 -U postgres -d demo -c "SELECT * FROM users WHERE email = 'a@b.com'"
```

Or, structurally, use `/api/shadow` with both endpoints pointing at
the same DB but routed through the proxy vs direct.

### Recipe 4: Compare row counts only (faster)

The endpoint always computes both row count and row hash. To skip
the hash on huge result sets, scope the query with a hash-friendly
sample:

```sql
SELECT id, email FROM users TABLESAMPLE BERNOULLI(1) WHERE id < 1000000
```

The 1% sample keeps memory bounded while still giving a high-
confidence match signal across versions.

## Pitfalls

- **Order matters for hash, even though it shouldn't.** The hash
  is order-independent (computed over a row-set, not a row-list)
  but only when the rows are emitted in the SAME format on both
  sides. PG version upgrades sometimes change `regclass` text
  formatting, JSON key ordering, or numeric trailing zeros.
  Cast suspicious columns explicitly.
- **`both_succeeded: false` with one error and not the other** is
  the more common failure mode and the one you want to investigate
  first — typically a missing extension, role, or permission on
  the shadow side.
- **Writes on shadow side really write.** A `INSERT … RETURNING …`
  is a write. The endpoint warns but doesn't block. Use a scratch
  DB for shadow targets when running anything but pure SELECTs.
- **Shadow elapsed > primary elapsed by >2× consistently** = the
  shadow is slower. In a PG-upgrade test that's noteworthy
  (regression). In a same-version cross-region test it's expected.
- **Credentials are sent in the JSON body** (same as
  `/api/replay`). TLS-terminate the admin port and don't log
  bodies.
- **503 = feature off.** `ha-tr` not compiled in.

## See also

- `heliosproxy-time-travel` — replay (writes the journal back)
  vs shadow (executes one query both ways)
- `heliosproxy-config` — `[ha]` configures both `/api/replay` and
  `/api/shadow`
- Demo: [`demos/v0.4.0/10-admin-rest/`](../../demos/v0.4.0/10-admin-rest/)
- Code: [`src/admin.rs`](../../src/admin.rs) — `/api/shadow` impl
