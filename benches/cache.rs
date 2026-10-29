//! Query-cache, L2 memory/file-spill and edge-fingerprint benchmarks.
//!
//! `cache/l2/*` measure the L2 tier directly. `cache/query/*` go through the
//! public `QueryCache` API (hint parse, normalization, tier lookups) the relay
//! calls for every cacheable read, and `cache/edge/*` the edge-cache key the
//! relay computes for every cacheable read when `[edge]` is on.

use criterion::{criterion_group, criterion_main, Criterion};
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
use std::hint::black_box;

/// One 1 KiB result: an L2 memory-hit control and a real file read/decode/promotion.
/// The spill file is seeded once outside the timed loop, and shedding memory
/// forces the disk fallback each iteration without growing the file. This
/// measures warm OS-page-cache reads, including shed/runtime overhead.
#[cfg(feature = "query-cache")]
fn bench_l2_cache(c: &mut Criterion) {
    use bytes::Bytes;
    use heliosdb_proxy::cache::{CacheKey, CachedResult, L2Config, L2WarmCache, StorageBackend};
    use std::time::Duration;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let key = CacheKey::from_parts(1, "bench".to_string(), None, None);
    let result = CachedResult::new(
        Bytes::from(vec![b'x'; 1024]),
        1,
        Duration::from_secs(3600),
        vec!["bench_table".to_string()],
        Duration::ZERO,
    );
    let memory = L2WarmCache::new(L2Config::default());
    rt.block_on(memory.put(key.clone(), result.clone()));

    let dir = tempfile::tempdir().unwrap();
    let spill = L2WarmCache::new(L2Config {
        storage: StorageBackend::Mmap,
        mmap_path: Some(dir.path().join("spill")),
        size_mb: 1,
        ..L2Config::default()
    });
    rt.block_on(spill.put(key.clone(), result));
    assert_eq!(spill.flush_to_disk().unwrap(), 1);
    let hash = key.hash_value();

    let mut group = c.benchmark_group("cache/l2");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("memory_hit_1k", |b| {
        b.iter(|| black_box(rt.block_on(memory.get(black_box(&key))).unwrap()));
    });
    group.bench_function("mmap_read_promote_1k", |b| {
        b.iter(|| {
            spill.shed(&[hash]);
            black_box(rt.block_on(spill.get(black_box(&key))).unwrap())
        });
    });
    group.finish();
}

#[cfg(not(feature = "query-cache"))]
fn bench_l2_cache(_c: &mut Criterion) {}

/// Number of distinct statements `miss_store_1k` rotates through: each store
/// replaces the entry its literal stored one rotation earlier, so L2 stays
/// bounded however long Criterion runs.
#[cfg(feature = "query-cache")]
const ROTATION: usize = 4096;

/// The `QueryCache` read path with a 1 KiB result, through the public API the
/// relay uses: an L1 hit (same connection), an L2 hit (no connection, so no L1
/// and no promotion), the same L2 hit with a trailing cache hint (the hint
/// parse), and a miss followed by a store (two normalizations, an L2 miss and
/// a replacing L2 store).
#[cfg(feature = "query-cache")]
fn bench_query_cache(c: &mut Criterion) {
    use bytes::Bytes;
    use heliosdb_proxy::cache::{CacheConfig, CacheContext, CacheLookup, QueryCache};
    use std::time::Duration;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let data = Bytes::from(vec![b'x'; 1024]);
    let conn = CacheContext {
        database: "bench".to_string(),
        user: Some("bench".to_string()),
        branch: None,
        connection_id: Some(1),
    };
    let shared = CacheContext {
        connection_id: None,
        ..conn.clone()
    };
    let point = "SELECT id, name, email FROM accounts WHERE id = 42 AND status = 'active'";
    let hinted = "SELECT id, name, email FROM accounts WHERE id = 42 AND status = 'active' \
                  /*helios:cache_ttl=60*/";

    let cache = QueryCache::new(CacheConfig::default());
    rt.block_on(async {
        cache
            .put(point, &conn, data.clone(), 1, Duration::ZERO)
            .await;
        cache
            .put(point, &shared, data.clone(), 1, Duration::ZERO)
            .await;
        cache
            .put(hinted, &shared, data.clone(), 1, Duration::ZERO)
            .await;
    });
    let hit = |sql: &str, ctx: &CacheContext| {
        let lookup = rt.block_on(cache.get(black_box(sql), black_box(ctx)));
        assert!(matches!(lookup, CacheLookup::Hit { .. }), "{sql} must hit");
        black_box(lookup)
    };

    let mut group = c.benchmark_group("cache/query");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("l1_hit_1k", |b| b.iter(|| hit(point, &conn)));
    group.bench_function("l2_hit_1k", |b| b.iter(|| hit(point, &shared)));
    group.bench_function("hinted_l2_hit_1k", |b| b.iter(|| hit(hinted, &shared)));

    // Every lookup misses (its database never stores anything); every store
    // replaces the entry of the same literal one rotation back.
    let missing = CacheContext {
        database: "bench_miss".to_string(),
        ..shared.clone()
    };
    let statements: Vec<String> = (0..ROTATION)
        .map(|i| {
            format!("SELECT id, name, email FROM accounts WHERE id = {i} AND status = 'active'")
        })
        .collect();
    let mut next = 0usize;
    group.bench_function("miss_store_1k", |b| {
        b.iter(|| {
            let sql = statements[next].as_str();
            next = (next + 1) % ROTATION;
            rt.block_on(async {
                let lookup = cache.get(black_box(sql), &missing).await;
                assert!(matches!(lookup, CacheLookup::Miss));
                cache
                    .put(black_box(sql), &shared, data.clone(), 1, Duration::ZERO)
                    .await;
            })
        });
    });
    group.finish();
}

#[cfg(not(feature = "query-cache"))]
fn bench_query_cache(_c: &mut Criterion) {}

/// The edge-cache key of one short read: comment stripping, literal folding
/// and the tenant-scoped params hash.
#[cfg(feature = "edge-proxy")]
fn bench_edge_fingerprint(c: &mut Criterion) {
    use heliosdb_proxy::edge::fingerprint::analyze;
    use std::time::Duration;
    let sql = "SELECT id, name FROM users /* dashboard */ WHERE email = 'a@example.com' \
               AND org_id = 42 ORDER BY id";
    let vars = vec![("TimeZone".to_string(), "UTC".to_string())];
    let mut group = c.benchmark_group("cache/edge");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("fingerprint_short", |b| {
        b.iter(|| black_box(analyze(black_box(sql), "bench", "bench", &vars)))
    });
    group.finish();
}

#[cfg(not(feature = "edge-proxy"))]
fn bench_edge_fingerprint(_c: &mut Criterion) {}

criterion_group!(
    benches,
    bench_l2_cache,
    bench_query_cache,
    bench_edge_fingerprint
);
criterion_main!(benches);
