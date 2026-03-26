//! Query Routing Benchmarks
//!
//! Measures hint parsing, read/write classification, and node selection
//! performance for the HeliosProxy routing engine.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[cfg(feature = "routing-hints")]
mod routing_benches {
    use super::*;
    use heliosdb_proxy::routing::{
        HintParser, NodeInfo, QueryRouter, RoutingConfig, SyncMode,
    };

    // ── Hint parsing ─────────────────────────────────────────────────

    pub fn bench_hint_parsing(c: &mut Criterion) {
        let mut group = c.benchmark_group("routing/hint_parse");
        let parser = HintParser::new();

        group.bench_function("no_hints", |b| {
            b.iter(|| {
                black_box(parser.parse("SELECT * FROM users WHERE id = 42"));
            });
        });

        group.bench_function("single_hint", |b| {
            b.iter(|| {
                black_box(parser.parse(
                    "/*helios:route=primary*/ SELECT * FROM users WHERE id = 42",
                ));
            });
        });

        group.bench_function("multiple_hints", |b| {
            b.iter(|| {
                black_box(parser.parse(
                    "/*helios:route=standby,consistency=eventual,timeout=5s,priority=low,cache=skip*/ SELECT * FROM products",
                ));
            });
        });

        group.bench_function("complex_query", |b| {
            b.iter(|| {
                black_box(parser.parse(
                    "/*helios:route=async,lag=100ms,priority=low*/ \
                     SELECT p.id, p.name, c.name AS category, AVG(r.rating) \
                     FROM products p \
                     JOIN categories c ON p.category_id = c.id \
                     LEFT JOIN reviews r ON r.product_id = p.id \
                     WHERE p.active = true \
                     GROUP BY p.id, p.name, c.name \
                     ORDER BY AVG(r.rating) DESC NULLS LAST \
                     LIMIT 50",
                ));
            });
        });

        group.finish();
    }

    // ── Hint stripping ───────────────────────────────────────────────

    pub fn bench_hint_stripping(c: &mut Criterion) {
        let mut group = c.benchmark_group("routing/hint_strip");
        let parser = HintParser::new();

        group.bench_function("no_hints", |b| {
            b.iter(|| {
                black_box(parser.strip("SELECT * FROM users WHERE id = 42"));
            });
        });

        group.bench_function("single_hint", |b| {
            b.iter(|| {
                black_box(parser.strip(
                    "/*helios:route=primary*/ SELECT * FROM users WHERE id = 42",
                ));
            });
        });

        group.bench_function("two_hints", |b| {
            b.iter(|| {
                black_box(parser.strip(
                    "/*helios:route=standby*/ SELECT * /*helios:cache=skip*/ FROM users",
                ));
            });
        });

        group.finish();
    }

    // ── Read/write classification ────────────────────────────────────

    pub fn bench_write_detection(c: &mut Criterion) {
        let mut group = c.benchmark_group("routing/write_detect");
        let router = QueryRouter::new(RoutingConfig::default());

        let queries = vec![
            ("select", "SELECT * FROM users WHERE id = 1"),
            ("insert", "INSERT INTO users (name, email) VALUES ('test', 'test@test.com')"),
            ("update", "UPDATE users SET name = 'new' WHERE id = 1"),
            ("delete", "DELETE FROM users WHERE id = 1"),
            ("begin", "BEGIN"),
            ("create_table", "CREATE TABLE test (id INT PRIMARY KEY, name TEXT)"),
            ("with_cte", "WITH cte AS (SELECT 1) SELECT * FROM cte"),
        ];

        for (name, query) in queries {
            group.bench_with_input(
                BenchmarkId::from_parameter(name),
                &query,
                |b, q| {
                    b.iter(|| {
                        black_box(router.is_write_query(q));
                    });
                },
            );
        }

        group.finish();
    }

    // ── Full routing decision ────────────────────────────────────────

    pub fn bench_route_decision(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("routing/route");
        group.throughput(Throughput::Elements(1));

        let router = rt.block_on(async {
            let router = QueryRouter::new(RoutingConfig::default());
            router
                .add_node(NodeInfo::primary("primary"))
                .await;
            router
                .add_node(NodeInfo::standby("standby-sync-1", SyncMode::Sync))
                .await;
            router
                .add_node(NodeInfo::standby("standby-async-1", SyncMode::Async).with_lag(50))
                .await;
            router
                .add_node(NodeInfo::standby("standby-async-2", SyncMode::Async).with_lag(100))
                .await;
            router
        });

        group.bench_function("read_no_hints", |b| {
            b.to_async(&rt).iter(|| async {
                black_box(router.route("SELECT * FROM users WHERE id = 1").await);
            });
        });

        group.bench_function("read_with_hint", |b| {
            b.to_async(&rt).iter(|| async {
                black_box(
                    router
                        .route("/*helios:route=standby*/ SELECT * FROM users WHERE id = 1")
                        .await,
                );
            });
        });

        group.bench_function("write", |b| {
            b.to_async(&rt).iter(|| async {
                black_box(
                    router
                        .route("INSERT INTO users (name) VALUES ('test')")
                        .await,
                );
            });
        });

        group.bench_function("complex_hints", |b| {
            b.to_async(&rt).iter(|| async {
                black_box(
                    router
                        .route("/*helios:route=async,lag=200ms,priority=low*/ SELECT COUNT(*) FROM events")
                        .await,
                );
            });
        });

        group.finish();
    }

    // ── Node selection throughput ─────────────────────────────────────

    pub fn bench_node_selection(c: &mut Criterion) {
        let mut group = c.benchmark_group("routing/node_select");
        let parser = HintParser::new();

        let queries_per_iter: u64 = 100;
        group.throughput(Throughput::Elements(queries_per_iter));

        let queries = vec![
            "SELECT * FROM users",
            "/*helios:route=primary*/ SELECT * FROM accounts",
            "INSERT INTO logs VALUES (1)",
            "/*helios:route=async,lag=5s*/ SELECT COUNT(*) FROM events",
            "/*helios:route=sync*/ SELECT balance FROM accounts WHERE id = 1",
        ];

        group.bench_function("batch_parse", |b| {
            b.iter(|| {
                for _ in 0..queries_per_iter {
                    for q in &queries {
                        black_box(parser.parse(q));
                    }
                }
            });
        });

        group.finish();
    }
}

// When routing-hints feature is not enabled, provide a no-op benchmark
// so the binary still compiles.
#[cfg(not(feature = "routing-hints"))]
mod routing_benches {
    use super::*;

    pub fn bench_hint_parsing(c: &mut Criterion) {
        c.bench_function("routing/noop_requires_routing_hints_feature", |b| {
            b.iter(|| black_box(42));
        });
    }

    pub fn bench_hint_stripping(_c: &mut Criterion) {}
    pub fn bench_write_detection(_c: &mut Criterion) {}
    pub fn bench_route_decision(_c: &mut Criterion) {}
    pub fn bench_node_selection(_c: &mut Criterion) {}
}

criterion_group!(
    benches,
    routing_benches::bench_hint_parsing,
    routing_benches::bench_hint_stripping,
    routing_benches::bench_write_detection,
    routing_benches::bench_route_decision,
    routing_benches::bench_node_selection,
);
criterion_main!(benches);
