# Changelog

All notable changes to HeliosProxy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-04-22

### Fixed
- **Connection pool: `max_connections` now enforced while connections are in use.**
  The semaphore permit acquired during `get_connection` was previously dropped
  before the connection reached the caller, so the limit only gated concurrent
  calls to `get_connection` itself. The permit is now attached to the
  `PooledConnection` and released when the connection is dropped or closed,
  so `max_connections` bounds total (idle + in-use) connections as documented.
  Covered by `test_max_connections_enforced_while_in_use`.
- **Protocol parser: unterminated C-strings now return a protocol error.**
  The previous `read_cstring` loop silently consumed the entire remaining
  buffer on missing null terminators. It now returns
  `ProxyError::Protocol(...)` and leaves the buffer untouched. Covered by
  `test_read_cstring_unterminated`.

### Performance
- **Connection-pool checkout no longer serialises across nodes.**
  `ConnectionPool::get_connection` previously held `pools.write().await`
  across `semaphore.acquire_owned().await` and `create_connection().await`,
  serialising all checkouts through a single lock. The lock is now taken
  only to pop an idle connection and clone the per-node `Arc<Semaphore>`;
  both awaits happen after the lock is released.
- **Pool metrics are now lock-free.** Seven `self.metrics.write().await`
  call sites replaced with `fetch_add(1, Relaxed)` on a
  `PoolMetricsCounters` struct of `AtomicU64` fields. `pool.metrics()`
  snapshots the counters on demand; no lock, no `.await`.
- **Zero-copy protocol parsing.**
  - `read_cstring` scans for the null terminator in a single pass
    (`iter().position(|&b| b == 0)`) and hands the split-off `BytesMut`
    directly to `String` (zero-copy conversion when uniquely owned)
    instead of growing a `Vec<u8>` byte-by-byte via `get_u8()`.
  - `BindMessage::param_values` is now `Vec<Option<Bytes>>`; parse uses
    `split_to(len).freeze()` instead of `.to_vec()`, so parameter values
    are reference-counted slices into the original buffer rather than
    per-parameter heap allocations.
- **L1 hot cache: hits take only a read lock.**
  `L1Entry::access_count` is now an `AtomicU64`, so `touch()` takes `&self`
  and can run under a read lock on the entries map. The cache itself
  switches from `std::sync::RwLock` to `parking_lot::RwLock` (no
  poisoning, faster uncontended). Only expired entries escalate to a
  write lock for eviction. Covered by `test_concurrent_hits_read_lock_only`
  (16 threads × 500 iters on the same key).

### Changed
- `L1Entry` no longer derives `Clone` (atomics aren't trivially `Clone`).
  No external caller was cloning it; only the contained `CachedResult` is
  cloned on return from `get()`. `access_count()` accessor added for
  read-only consumers.

### Tests
- 1 102 tests pass with `--features all-features` (+6 regression tests
  covering the fixes above).

### Benchmarks
Criterion `--quick` on the `pooling` bench suite, comparing baseline
(v0.3.0, commit `8c43de9`) against v0.3.1 on the same machine:

| Benchmark                                  | v0.3.0     | v0.3.1     | Change       |
|--------------------------------------------|------------|------------|--------------|
| `pool/acquire_release/single`              | 471.81 ns  | 329.83 ns  | **−30 %**    |
| `pool/throughput/sequential_acquire/1`     | 490.36 ns  | 353.88 ns  | **−28 %**    |
| `pool/throughput/sequential_acquire/10`    | 5.354 µs   | 3.351 µs   | **−37 %**    |
| `pool/throughput/sequential_acquire/50`    | 25.36 µs   | 16.95 µs   | **−33 %**    |
| `pool/metrics/read_metrics`                | 35.90 ns   | 11.51 ns   | **−68 %** (3.1× faster) |

Single-threaded numbers — the concurrency gains from dropping
`RwLock<pools>` across `.await` and from lock-free metric reads
compound under contention and are not captured by this bench.

## [0.3.0] - 2026-03-26

### Added
- **24 feature modules** migrated to standalone, feature-gated architecture:
  - `pool-modes` - Session, Transaction, and Statement connection pooling
  - `ha-tr` - Transaction Replay with failover replay, cursor restore, session migration
  - `query-cache` - Multi-tier caching (L1 hot / L2 warm / L3 semantic)
  - `routing-hints` - SQL comment-based query routing (`/*helios:route=primary*/`)
  - `lag-routing` - Replica lag-aware routing with read-your-writes consistency
  - `rate-limiting` - Token bucket, sliding window, and concurrency limiters
  - `circuit-breaker` - Adaptive circuit breaker with sliding counter
  - `query-analytics` - Slow query log, N+1 detection, query fingerprinting
  - `multi-tenancy` - Tenant isolation with per-tenant connection pools
  - `auth-proxy` - JWT, OAuth, and API key authentication
  - `query-rewriting` - SQL rewriting rules engine
  - `wasm-plugins` - Hot-reload WASM plugin system (sandboxed)
  - `graphql-gateway` - GraphQL-to-SQL translation with introspection
  - `schema-routing` - Data temperature and workload classification routing
  - `distribcache` - Distributed intelligent caching with AI-driven prefetch
  - `observability` - Prometheus metrics and OpenTelemetry tracing
- **TopologyProvider trait** with two implementations:
  - `postgres-topology` - PostgreSQL discovery via `pg_is_in_recovery()` polling
  - `heliosdb-topology` - HeliosDB-Lite internal topology
- **`all-features` bundle** for enabling all proxy features at once
- PostgreSQL wire protocol forwarding with Transparent Write Routing (TWR)
- Request pipelining support (Parse/Bind/Execute pipeline, FIFO ordering)
- Batch INSERT coalescing for bulk write optimization
- Switchover buffer for zero-downtime planned failover
- Primary tracker with standalone and integrated modes
- Admin REST API with HTTP SQL endpoint for load-balanced queries
- Criterion benchmarks for pooling and routing performance
- Integration test skeleton for end-to-end verification
- CI workflow with feature matrix (default, ha-tr, all-features, all-features+postgres-topology)
- MSRV verification at Rust 1.75
- Release workflow for cross-platform binary builds (linux/macOS, x86_64/aarch64)
- Docker workflow with multi-arch images (amd64/arm64) pushed to GHCR

### Changed
- Restructured from embedded library to standalone binary + library crate
- All feature modules are now independently toggleable via Cargo feature flags
- Connection pool modes (`session`, `transaction`, `statement`) moved behind `pool-modes` feature

## [0.2.0] - 2026-02-15

### Added
- Connection pooling with Session, Transaction, and Statement modes
- Pool hardening: transaction leak detection, stale lease cleanup, exhaustion monitoring
- Prepared statement tracking across pool mode transitions
- Connection reset executor for clean state between leases
- Load balancer with round-robin, least-connections, and latency-based strategies
- Health checker with configurable check queries and failure thresholds
- Failover controller with sync-standby preference and automatic promotion
- Transaction journal for write tracking and replay

### Changed
- Upgraded to tokio 1.x full runtime
- Improved protocol codec for better PostgreSQL compatibility

## [0.1.0] - 2026-01-27

### Added
- Initial release of HeliosProxy as a standalone connection router
- PostgreSQL wire protocol support (startup, simple query, extended query)
- Basic connection pooling with configurable min/max connections
- Read/write splitting with automatic query classification
- Node health monitoring with configurable intervals and thresholds
- Admin API for runtime management and metrics
- Configuration via TOML file or command-line arguments
- Benchmark suite: HeliosProxy vs PgBouncer scalability comparison
- Docker support for containerized deployment

[0.3.1]: https://github.com/dimensigon/heliosdb-proxy/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/dimensigon/heliosdb-proxy/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dimensigon/heliosdb-proxy/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dimensigon/heliosdb-proxy/releases/tag/v0.1.0
