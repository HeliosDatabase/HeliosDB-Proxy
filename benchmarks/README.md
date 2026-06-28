# Benchmarks

Reproducible performance comparisons for the HeliosDB ecosystem.

| Report | Harness | Raw data |
|---|---|---|
| [HeliosDB-Nano vs PostgreSQL (2026-06-28)](heliosdb-nano-vs-postgresql-2026-06-28.md) | [`bench-engines.sh`](bench-engines.sh) | [`results/`](results/) |

`bench-engines.sh` measures `SELECT 1`, indexed point-read, bulk-load `COPY`, and
`DROP TABLE` for PostgreSQL and any supplied HeliosDB-Nano binaries (each run as a
native host binary; PostgreSQL containerized; `pgbench` as the client). See the
script header for usage and how to build the Nano binaries from version tags.

The proxy-path scalability harness lives at
[`../scripts/regress/bench-scalability.sh`](../scripts/regress/bench-scalability.sh)
(direct vs proxy session/transaction pool modes).
