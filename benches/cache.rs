//! L2 cache memory and file-spill benchmarks.

use criterion::{criterion_group, criterion_main, Criterion};
#[cfg(feature = "query-cache")]
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

criterion_group!(benches, bench_l2_cache);
criterion_main!(benches);
