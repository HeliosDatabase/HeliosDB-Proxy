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

criterion_group!(
    benches,
    bench_pool_creation,
    bench_pool_config,
    bench_acquire_release,
    bench_pool_throughput,
    bench_node_endpoint,
    bench_pool_metrics,
);
criterion_main!(benches);
