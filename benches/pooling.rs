//! Connection Pool Benchmarks
//!
//! Measures pool construction, configuration, and acquire/release
//! latency for the HeliosProxy connection pool.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use heliosdb_proxy::connection_pool::{ConnectionPool, PoolConfig};
use heliosdb_proxy::{NodeEndpoint, NodeId, NodeRole};
use std::time::Duration;

/// Benchmark pool creation with various max-connection sizes.
fn bench_pool_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool/create");

    for pool_size in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::from_parameter(pool_size),
            &pool_size,
            |b, &size| {
                b.iter(|| {
                    let config = PoolConfig {
                        min_connections: 2,
                        max_connections: size,
                        ..Default::default()
                    };
                    black_box(ConnectionPool::new(config));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark pool configuration construction.
fn bench_pool_config(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool/config");

    group.bench_function("default", |b| {
        b.iter(|| {
            black_box(PoolConfig::default());
        });
    });

    group.bench_function("custom", |b| {
        b.iter(|| {
            black_box(PoolConfig {
                min_connections: 5,
                max_connections: 100,
                idle_timeout: Duration::from_secs(600),
                max_lifetime: Duration::from_secs(3600),
                acquire_timeout: Duration::from_secs(10),
                test_on_acquire: true,
            });
        });
    });

    group.finish();
}

/// Benchmark async acquire/release cycle.
fn bench_acquire_release(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pool/acquire_release");
    group.throughput(Throughput::Elements(1));

    let pool = rt.block_on(async {
        let pool = ConnectionPool::new(PoolConfig {
            min_connections: 2,
            max_connections: 100,
            ..Default::default()
        });
        let node_id = NodeId::new();
        pool.add_node(node_id).await;
        (pool, node_id)
    });

    group.bench_function("single", |b| {
        let (ref pool, node_id) = pool;
        b.to_async(&rt).iter(|| async {
            let conn = pool.get_connection(&node_id).await.unwrap();
            pool.return_connection(conn).await;
        });
    });

    group.finish();
}

/// Benchmark acquire throughput under contention (sequential acquires).
fn bench_pool_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pool/throughput");

    for batch_size in [1u64, 10, 50] {
        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::new("sequential_acquire", batch_size),
            &batch_size,
            |b, &n| {
                let (pool, node_id) = rt.block_on(async {
                    let pool = ConnectionPool::new(PoolConfig {
                        min_connections: 2,
                        max_connections: 200,
                        ..Default::default()
                    });
                    let node_id = NodeId::new();
                    pool.add_node(node_id).await;
                    (pool, node_id)
                });

                b.to_async(&rt).iter(|| {
                    let pool = &pool;
                    async move {
                        let mut conns = Vec::with_capacity(n as usize);
                        for _ in 0..n {
                            conns.push(pool.get_connection(&node_id).await.unwrap());
                        }
                        for conn in conns {
                            pool.return_connection(conn).await;
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark the acquire/release cycle under real concurrency.
///
/// Extends `pool/acquire_release/single` onto the contended path: an
/// `Arc`-shared skeleton pool is hit by K tasks on the multi-thread tokio
/// runtime, each looping acquire+return, so the `RwLock<HashMap>` write lock and
/// the per-node `Semaphore` are exercised under genuine parallelism rather than
/// sequentially. No backend is involved (skeleton mode, `add_node` only).
fn bench_acquire_release_contention(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pool/acquire_release/contention");

    let (pool, node_id) = rt.block_on(async {
        let pool = std::sync::Arc::new(ConnectionPool::new(PoolConfig {
            min_connections: 2,
            max_connections: 256,
            ..Default::default()
        }));
        let node_id = NodeId::new();
        pool.add_node(node_id).await;
        (pool, node_id)
    });

    // Each task performs a small fixed number of acquire+return cycles per
    // iteration, so the reported throughput reflects total pool operations.
    const OPS_PER_TASK: usize = 4;
    for concurrency in [2usize, 8, 32] {
        group.throughput(Throughput::Elements((concurrency * OPS_PER_TASK) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &k| {
                let pool = pool.clone();
                b.to_async(&rt).iter(|| {
                    let pool = pool.clone();
                    async move {
                        let mut handles = Vec::with_capacity(k);
                        for _ in 0..k {
                            let pool = pool.clone();
                            handles.push(tokio::spawn(async move {
                                for _ in 0..OPS_PER_TASK {
                                    let conn = pool.get_connection(&node_id).await.unwrap();
                                    pool.return_connection(conn).await;
                                }
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark node endpoint construction (used during pool operations).
fn bench_node_endpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool/node_endpoint");

    group.bench_function("create", |b| {
        b.iter(|| {
            black_box(
                NodeEndpoint::new("pg-primary.example.com", 5432)
                    .with_role(NodeRole::Primary)
                    .with_weight(100),
            );
        });
    });

    group.bench_function("address", |b| {
        let endpoint =
            NodeEndpoint::new("pg-primary.example.com", 5432).with_role(NodeRole::Primary);
        b.iter(|| {
            black_box(endpoint.address());
        });
    });

    group.bench_function("node_id", |b| {
        b.iter(|| {
            black_box(NodeId::new());
        });
    });

    group.finish();
}

/// Benchmark pool metrics retrieval.
fn bench_pool_metrics(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pool/metrics");

    let pool = rt.block_on(async {
        let pool = ConnectionPool::new(PoolConfig::default());
        let node_id = NodeId::new();
        pool.add_node(node_id).await;
        // Do a few acquire/release cycles to populate metrics
        for _ in 0..5 {
            let conn = pool.get_connection(&node_id).await.unwrap();
            pool.return_connection(conn).await;
        }
        pool
    });

    group.bench_function("read_metrics", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(pool.metrics().await);
        });
    });

    group.finish();
}

/// Per-tenant request budgets (ac858d61400b): the active-query permit a
/// capped tenant's request takes and returns (one CAS, then the release
/// through the semaphore's waiter mutex), the FIFO handoff when requests wait
/// at the cap, and the replay-byte charge each recorded statement makes.
fn bench_tenant_budget(c: &mut Criterion) {
    use heliosdb_proxy::config::ProxyConfig;
    use heliosdb_proxy::metrics_hist::{default_latency_buckets_ms, LatencyHistogram};
    use heliosdb_proxy::tenant_slots::{TenantBudget, TenantCharge, TenantSlots};
    use std::sync::Arc;

    let slots_of = |query_cap: usize| {
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.tenants = vec!["t".to_string()];
        config.multi_tenancy.max_active_queries_per_tenant = query_cap;
        config.multi_tenancy.max_replay_bytes_per_tenant = 1 << 30;
        TenantSlots::from_config(&config)
    };
    let budget_in =
        |slots: &TenantSlots| Arc::clone(slots.budget("t").expect("tenant t has a budget"));
    let mut group = c.benchmark_group("tenant_budget");

    let wide = budget_in(&slots_of(1024));
    group.throughput(Throughput::Elements(1));
    group.bench_function("query_permit/acquire_release", |b| {
        b.iter(|| drop(black_box(wide.try_acquire_query())));
    });

    // 8 tasks share a cap of 4 on one thread: the first 4 take a permit and
    // hold it across a yield, so the other 4 find the cap full, queue (FIFO)
    // and are each handed a permit by a release, the path a saturated tenant
    // takes; each wait is recorded in the wait histogram, as the proxy does.
    // That every iteration queues exactly 4 is checked outside the timing.
    const TASKS: usize = 8;
    const CAP: usize = 4;
    async fn handoff(budget: &Arc<TenantBudget>, waits: &Arc<LatencyHistogram>) {
        let tasks: Vec<_> = (0..TASKS)
            .map(|_| {
                let budget = Arc::clone(budget);
                let waits = Arc::clone(waits);
                tokio::spawn(async move {
                    let permit = budget
                        .wait_query(&waits)
                        .await
                        .expect("admitted within the wait");
                    tokio::task::yield_now().await;
                    drop(permit);
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
    }
    let narrow_slots = slots_of(CAP);
    let narrow = budget_in(&narrow_slots);
    let waits = Arc::new(LatencyHistogram::new(&default_latency_buckets_ms()));
    let one_thread = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    group.throughput(Throughput::Elements(TASKS as u64));
    group.bench_function("query_permit/handoff_8x4", |b| {
        b.to_async(&one_thread).iter_custom(|iters| {
            let (slots, budget, waits) = (&narrow_slots, &narrow, &waits);
            async move {
                let queued = || slots.budget_stats()[0].query_waits;
                let (before, recorded) = (queued(), waits.snapshot().count());
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    handoff(budget, waits).await;
                }
                let elapsed = start.elapsed();
                let expected = (TASKS - CAP) as u64 * iters;
                assert_eq!(queued() - before, expected, "waits queued at the cap");
                assert_eq!(
                    waits.snapshot().count() - recorded,
                    expected,
                    "waits recorded"
                );
                elapsed
            }
        });
    });

    let charge = TenantCharge::new(Arc::clone(&wide));
    group.throughput(Throughput::Elements(1));
    group.bench_function("replay_charge/reserve_settle", |b| {
        b.iter(|| {
            charge.replay_reserve(black_box(4096));
            charge.replay_settle(0);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pool_creation,
    bench_pool_config,
    bench_acquire_release,
    bench_pool_throughput,
    bench_acquire_release_contention,
    bench_node_endpoint,
    bench_pool_metrics,
    bench_tenant_budget,
);
criterion_main!(benches);
