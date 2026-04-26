# Demo 15 — `pgvector-router` plugin

**Module brief:** [§Module 15](../../../docs/website-brief-v0.4.0.md)

## UVP

> Routes pgvector top-K queries (`<->`, `<#>`, `<=>` in `ORDER BY`)
> to a designated vector-tagged replica. Keeps the HNSW index
> hot; non-vector reads stay on cheaper replicas.

## Use cases

- **Semantic search at scale.** Vector index is large + slow to
  warm; consolidate vector queries on one or two replicas with
  enough RAM.
- **Mixed workload separation.** OLTP traffic + vector search on
  the same DB cluster; the vector plugin auto-segregates.

## What this demo shows

Two PG 17 backends, one with `vector` extension. Plugin routes
queries with `ORDER BY embedding <=> $1 LIMIT N` to the
vector-tagged node. Same query without `ORDER BY` (just a
distance comparison) → default routing.

## Run it

```bash
cd demos/v0.4.0/15-pgvector-router
./demo.sh
```

The compose has two PG 17 services:

- `pg-primary` — standard, no vector extension
- `pg-vector` — `pgvector/pgvector:pg17` image, has `embedding`
  column with HNSW index

Tagged in proxy.toml as `vector: pg-vector`. The plugin reads
`hook_context.attributes["helios.vector_node"]` (which the
operator's `RoutingRule` reconciler can set; for this demo we
inject via `application_name=helios.vector_node:pg-vector`).

```bash
psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT id FROM docs ORDER BY embedding <=> '[1,2,3]' LIMIT 5"
# routed to pg-vector — observable via /metrics or RUST_LOG=debug

psql -h localhost -p 6432 -U postgres -d demo \
  -c "SELECT id FROM users LIMIT 5"
# routed to pg-primary (default; no vector op)
```

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/pgvector-router/src/lib.rs`. Pure
function `classify(sql, vector_node)` returns
`VectorRoute::Node(name)` or `VectorRoute::Default`. Six unit
tests including the "no ORDER BY → default" guard.

## HeliosDB compatibility

HeliosDB doesn't ship pgvector today; this demo uses
`pgvector/pgvector:pg17`. Plugin behaviour is identical — the
detector is text-pattern matching, not backend-specific.
