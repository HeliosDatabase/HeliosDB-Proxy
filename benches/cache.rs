//! Query-cache, L2 memory/file-spill and edge-fingerprint benchmarks.
//!
//! `cache/l2/*` measure the L2 tier directly. `cache/query/*` go through the
//! public `QueryCache` API (hint parse, normalization, tier lookups) the relay
//! calls for every cacheable read, and its `write_tables` for every journaled
//! write; `cache/edge/*` the edge-cache key the relay computes for every
//! cacheable read when `[edge]` is on, and the edge invalidation's table set.
//! `cache/l3/*` measure the semantic tier through its public API (canonical
//! form included; `store_lookup_contended_8` with eight threads at once) and
//! `cache/excl/*` the relay's `[cache] exclude_tables` scan.

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

/// A multi-row `INSERT INTO events …` of at least `bytes` bytes.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
fn bulk_insert(bytes: usize) -> String {
    use std::fmt::Write;
    let mut sql = String::with_capacity(bytes + 64);
    sql.push_str("INSERT INTO events (id, kind, payload) VALUES ");
    let mut i = 0u32;
    while sql.len() < bytes {
        if i > 0 {
            sql.push(',');
        }
        let _ = write!(sql, "({i}, 'click', 'row {i} of the batch')");
        i += 1;
    }
    sql
}

/// The `QueryCache` read path with a 1 KiB result, through the public API the
/// relay uses: an L1 hit (same connection), an L2 hit (no connection, so no L1
/// and no promotion), the same L2 hit with a trailing cache hint (the hint
/// parse), and a miss followed by a store (two normalizations, an L2 miss and
/// a replacing L2 store). Then the C-02 write path: the tables of a journaled
/// write (`QueryCache::write_tables`), read before its `ReadyForQuery`
/// reaches the client, for a short UPDATE and for a bulk INSERT as long as
/// `[journal] max_statement_bytes` (its default), the longest statement the
/// capture journals whole.
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

    let update = "UPDATE accounts SET status = 'closed', note = 'moved' WHERE id = 42";
    let insert = bulk_insert(
        heliosdb_proxy::config::ProxyConfig::default()
            .journal
            .max_statement_bytes,
    );
    assert_eq!(
        cache.write_tables(update),
        Some(vec!["accounts".to_string()])
    );
    assert_eq!(
        cache.write_tables(&insert),
        Some(vec!["events".to_string()])
    );
    group.bench_function("write_tables_short", |b| {
        b.iter(|| black_box(cache.write_tables(black_box(update))))
    });
    group.bench_function("write_tables_insert_1m", |b| {
        b.iter(|| black_box(cache.write_tables(black_box(&insert))))
    });
    group.finish();
}

#[cfg(not(feature = "query-cache"))]
fn bench_query_cache(_c: &mut Criterion) {}

/// The edge-cache key of one short read: comment stripping, literal folding
/// and the tenant-scoped params hash. Then the edge invalidation's table set
/// of a bulk INSERT as long as `[journal] max_statement_bytes` (its default;
/// `edge::fingerprint::write_tables`, on the forward path of every write the
/// edge cache invalidates for).
#[cfg(feature = "edge-proxy")]
fn bench_edge_fingerprint(c: &mut Criterion) {
    use heliosdb_proxy::edge::fingerprint::{analyze, write_tables};
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
    let insert = bulk_insert(
        heliosdb_proxy::config::ProxyConfig::default()
            .journal
            .max_statement_bytes,
    );
    assert_eq!(write_tables(&insert), ["events"]);
    group.bench_function("write_tables_insert_1m", |b| {
        b.iter(|| black_box(write_tables(black_box(&insert))))
    });
    group.finish();
}

#[cfg(not(feature = "edge-proxy"))]
fn bench_edge_fingerprint(_c: &mut Criterion) {}

/// A blocking stand-in for an Ollama-compatible embedding endpoint: one
/// deterministic vector per prompt, except that `alt` embeds like `c15`.
#[cfg(feature = "query-cache")]
fn embedding_stub(dim: usize) -> String {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(mut sock) = sock else { continue };
            let mut reader = BufReader::new(sock.try_clone().unwrap());
            let mut length = 0usize;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                if let Some((k, v)) = line.split_once(':') {
                    if k.eq_ignore_ascii_case("content-length") {
                        length = v.trim().parse().unwrap_or(0);
                    }
                }
            }
            let mut body = vec![0u8; length];
            if reader.read_exact(&mut body).is_err() {
                continue;
            }
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let prompt = match request["prompt"].as_str().unwrap_or("") {
                "alt" => "c15",
                other => other,
            };
            let mut x = prompt.bytes().fold(0x9e37_79b9_7f4a_7c15u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
            }) | 1;
            let vector: Vec<f64> = (0..dim)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x % 2001) as f64 / 1000.0 - 1.0
                })
                .collect();
            let payload = serde_json::json!({ "embedding": vector }).to_string();
            let _ = write!(
                sock,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{payload}",
                payload.len()
            );
        }
    });
    url
}

/// Threads of `cache/l3/store_lookup_contended_8`.
#[cfg(feature = "query-cache")]
const CONTENDED_THREADS: usize = 8;

/// The semantic tier through its public API (normalization and canonical
/// form on every call): an exact hit among 5000 entries, an approximate hit
/// scoring 16 variants of one statement shape (embeddings from an in-bench
/// stub, remembered before timing), a store into a full tier (FIFO
/// eviction on every call), and `CONTENDED_THREADS` threads at once each
/// missing and then storing keys of their own against a full tier (the miss
/// path of hinted reads of distinct keys, which a tier-wide lock serialised).
#[cfg(feature = "query-cache")]
fn bench_semantic_cache(c: &mut Criterion) {
    use bytes::Bytes;
    use heliosdb_proxy::cache::{CacheContext, CachedResult, L3Config, L3SemanticCache};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = CacheContext {
        database: "bench".to_string(),
        user: Some("bench".to_string()),
        branch: None,
        connection_id: None,
    };
    let result = CachedResult::new(
        Bytes::from(vec![b'x'; 1024]),
        1,
        Duration::from_secs(3600),
        vec!["kv".to_string()],
        Duration::ZERO,
    );
    let full = L3SemanticCache::new(L3Config {
        enabled: true,
        similarity_threshold: 1.0,
        ..L3Config::default()
    });
    rt.block_on(async {
        for i in 0..5000 {
            full.put(
                &format!("SELECT val FROM kv WHERE id = {i}"),
                &ctx,
                result.clone(),
            )
            .await;
        }
    });
    assert_eq!(full.len(), 5000);

    let dim = 384;
    let approx = L3SemanticCache::new(L3Config {
        enabled: true,
        similarity_threshold: 0.9,
        embedding_endpoint: embedding_stub(dim),
        embedding_dim: dim,
        embedding_concurrency: 32,
        ..L3Config::default()
    });
    let wanted = "SELECT alt FROM kv AS k WHERE id = 1";
    rt.block_on(async {
        for j in 0..16 {
            approx
                .put(
                    &format!("SELECT c{j} FROM kv WHERE id = 1"),
                    &ctx,
                    result.clone(),
                )
                .await;
        }
        // A lookup only fetches stored names: `alt` is embedded through a
        // stored statement of its own (another row); each miss schedules the
        // stored names still without an embedding.
        approx
            .put("SELECT alt FROM kv WHERE id = 2", &ctx, result.clone())
            .await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while approx.health().embeddings < 17 {
            assert!(std::time::Instant::now() < deadline, "stub embeddings");
            let _ = approx.get(wanted, &ctx).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    assert!(rt.block_on(approx.get(wanted, &ctx)).is_some());

    let mut group = c.benchmark_group("cache/l3");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    // A paraphrase: keyword case and the alias differ (names keep their case).
    let exact = "select val from kv as t where id = 2500";
    group.bench_function("lookup_exact_5000", |b| {
        b.iter(|| black_box(rt.block_on(full.get(black_box(exact), &ctx)).unwrap()))
    });
    group.bench_function("lookup_approx_16", |b| {
        b.iter(|| black_box(rt.block_on(approx.get(black_box(wanted), &ctx)).unwrap()))
    });
    let mut next = 5000u64;
    group.bench_function("store_full_5000", |b| {
        b.iter(|| {
            let sql = format!("SELECT val FROM kv WHERE id = {next}");
            next += 1;
            rt.block_on(full.put(black_box(&sql), &ctx, result.clone()))
        })
    });

    // One iteration: every thread misses and then stores one key of its own
    // (never stored before, so every store evicts), all at once; the time is
    // the wall time from a common start (a barrier, after the threads and
    // their runtimes are up) until the last thread is done, per round.
    let contended = Arc::new(L3SemanticCache::new(L3Config {
        enabled: true,
        similarity_threshold: 1.0,
        ..L3Config::default()
    }));
    rt.block_on(async {
        for i in 0..5000 {
            contended
                .put(
                    &format!("SELECT val FROM kv WHERE id = {i}"),
                    &ctx,
                    result.clone(),
                )
                .await;
        }
    });
    assert_eq!(contended.len(), 5000);
    let next_id = AtomicU64::new(5000);
    group.bench_function("store_lookup_contended_8", |b| {
        b.iter_custom(|iters| {
            let start = Arc::new(Barrier::new(CONTENDED_THREADS + 1));
            let workers: Vec<_> = (0..CONTENDED_THREADS)
                .map(|_| {
                    let first = next_id.fetch_add(iters, Ordering::Relaxed);
                    let (cache, start) = (contended.clone(), start.clone());
                    let (ctx, result) = (ctx.clone(), result.clone());
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        start.wait();
                        for id in first..first + iters {
                            let sql = format!("SELECT val FROM kv WHERE id = {id}");
                            rt.block_on(async {
                                assert!(cache.get(black_box(&sql), &ctx).await.is_none());
                                cache.put(black_box(&sql), &ctx, result.clone()).await;
                            });
                        }
                    })
                })
                .collect();
            start.wait();
            let began = std::time::Instant::now();
            for worker in workers {
                worker.join().unwrap();
            }
            began.elapsed()
        })
    });
    // Concurrent stores may evict one more than needed each: at most full.
    assert!(contended.len() <= 5000);
    group.finish();
}

#[cfg(not(feature = "query-cache"))]
fn bench_semantic_cache(_c: &mut Criterion) {}

/// The relay's `[cache] exclude_tables` scan of a ~1 KiB read that names
/// none of the excluded tables (the cost every cacheable read pays when
/// exclusions are configured): three tables, and 1100 (a daily-partitioned
/// table over three years, every partition listed, as the docs require).
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
fn bench_exclusions(c: &mut Criterion) {
    use heliosdb_proxy::sql_text::TableExclusions;
    use std::time::Duration;
    let exclusions = TableExclusions::new(["secrets", "audit_log", "payroll"]);
    let mut sql = String::from("SELECT o.id, o.total, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id IN (");
    let mut i = 0u32;
    while sql.len() < 1024 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&i.to_string());
        i += 1;
    }
    sql.push(')');
    assert!(!exclusions.matches(&sql));
    let partitions: Vec<String> = (0..1100).map(|d| format!("events_p{d:04}")).collect();
    let many = TableExclusions::new(&partitions);
    assert!(!many.matches(&sql));
    let mut group = c.benchmark_group("cache/excl");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("scan_3_names_1k", |b| {
        b.iter(|| black_box(exclusions.matches(black_box(&sql))))
    });
    group.bench_function("scan_1100_names_1k", |b| {
        b.iter(|| black_box(many.matches(black_box(&sql))))
    });
    // The system-catalog and comment/quoting (time-travel comment,
    // backend-ambiguous text) scans every cacheable read pays while a result
    // cache runs, on the same read with a trailing hint comment (none
    // refuses it).
    let hinted = format!("{sql} /*helios:cache_ttl=60*/");
    let refuses = |text: &str| {
        heliosdb_proxy::sql_text::names_system_catalog(text.as_bytes())
            || heliosdb_proxy::sql_text::uncacheable_text(text).is_some()
    };
    assert!(!refuses(&hinted));
    group.bench_function("catalog_time_travel_scan_1k", |b| {
        b.iter(|| black_box(refuses(black_box(&hinted))))
    });
    group.finish();
}

#[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
fn bench_exclusions(_c: &mut Criterion) {}

criterion_group!(
    benches,
    bench_l2_cache,
    bench_query_cache,
    bench_edge_fingerprint,
    bench_semantic_cache,
    bench_exclusions
);
criterion_main!(benches);
