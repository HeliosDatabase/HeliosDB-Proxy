//! Relay / Failover / Pool-Contention Benchmarks
//!
//! Extends the in-process benchmark coverage (CLAUDE.md quality gate 3)
//! onto the paths that carry the proxy's HA and mode-aware pooling work
//! but that never touch a live backend:
//!
//! - **Switchover buffer** (`switchover_buffer`, always-on): the synchronous
//!   enqueue hot path (`buffer_query`) and the replay-drain (`drain`) with an
//!   in-process no-op executor.
//! - **Transaction journal** (`transaction_journal`, `ha-tr`): per-statement
//!   classification, the size walk enforced on every `log_statement`, in-memory
//!   journal mutation (`add_entry` / `rollback_to_savepoint`), the async
//!   begin+log+commit journaling lifecycle (single-threaded and under write-lock
//!   contention), and the time-travel `entries_in_window` scan.
//! - **Mode-aware pooling** (`pool`, `pool-modes`): the allocation-free SQL
//!   classifiers (`TransactionEvent::detect`, `pool_key`, statement-mode safety,
//!   PREPARE/DEALLOCATE parsers) and the `ConnectionPoolManager` acquire/release
//!   lease cycle under real client concurrency plus the per-statement
//!   `on_statement_complete` decision.
//!
//! # Feature gating
//!
//! This bench MUST compile under every feature set (it carries no
//! `required-features`). The `ha-tr` work lives behind `#[cfg(feature = "ha-tr")]`
//! and the `pool-modes` work behind `#[cfg(feature = "pool-modes")]`; each has a
//! no-op fallback module so `criterion_group!`/`criterion_main!` always resolve
//! and `cargo bench --no-run` succeeds regardless of enabled features. The
//! switchover-buffer group is feature-free.

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};

// ─────────────────────────────────────────────────────────────────────
// Switchover buffer (always-on, feature-free)
// ─────────────────────────────────────────────────────────────────────

use heliosdb_proxy::switchover_buffer::{BufferConfig, SwitchoverBuffer};

/// A fresh buffer already in the `Buffering` state, with a very high query cap
/// so a micro-bench never trips the capacity guard.
fn started_buffer(max_queries: usize) -> SwitchoverBuffer {
    let buffer = SwitchoverBuffer::new(BufferConfig {
        max_buffered_queries: max_queries,
        ..Default::default()
    });
    buffer.start_buffering();
    buffer
}

/// Synchronous enqueue hot path: `is_buffering`/timeout/capacity/memory checks +
/// oneshot allocation + `VecDeque` push under the `parking_lot` mutex. A fresh
/// started buffer is minted per iteration (untimed setup) so the capacity guard
/// is never the thing being measured; the returned receiver is dropped, exactly
/// as `test_buffer_limits` does.
fn bench_switchover_buffer_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("switchover/buffer_query");
    group.throughput(Throughput::Elements(1));
    group.bench_function("enqueue_one", |b| {
        b.iter_batched(
            || started_buffer(1_000_000),
            |buffer| {
                let rx = buffer
                    .buffer_query(
                        black_box("INSERT INTO orders VALUES (1)".to_string()),
                        black_box(Vec::new()),
                        1,
                    )
                    .unwrap();
                black_box(rx);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Replay-drain path with an in-process no-op executor (no backend, exactly like
/// `test_pg_switchover_buffer`): drains the `VecDeque`, runs the per-query
/// timeout check, and sends each result. `iter_batched` rebuilds a fresh buffer
/// pre-loaded with `n` queries per iteration since `drain` empties it.
fn bench_switchover_drain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("switchover/drain");
    for n in [1usize, 16, 64] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let buffer = started_buffer(1_000_000);
                    for i in 0..n {
                        // Receivers are intentionally dropped; `drain` tolerates
                        // a closed receiver (send returns Err, which it ignores).
                        // Explicit `drop` (not `let _ =`) so clippy's
                        // let_underscore_future does not fire on the returned
                        // oneshot receiver (a Future we deliberately discard).
                        let receiver = buffer
                            .buffer_query(
                                format!("INSERT INTO t VALUES ({})", i),
                                Vec::new(),
                                i as u64,
                            )
                            .unwrap();
                        drop(receiver);
                    }
                    buffer
                },
                |buffer| {
                    rt.block_on(buffer.drain(|_sql, _params| async { Ok(()) }));
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────
// Transaction journal
// ─────────────────────────────────────────────────────────────────────

mod journal_benches {
    use super::*;
    use heliosdb_proxy::transaction_journal::{
        JournalEntry, JournalValue, StatementType, TransactionJournal, TransactionJournalEntry,
    };
    use heliosdb_proxy::NodeId;
    use std::sync::Arc;
    use uuid::Uuid;

    /// A deterministic journal entry with a mixed parameter set.
    fn sample_entry(sequence: u64) -> JournalEntry {
        JournalEntry {
            sequence,
            statement: "UPDATE accounts SET balance = balance - $1 WHERE id = $2".to_string(),
            parameters: vec![
                JournalValue::Float64(25.0),
                JournalValue::Int64(1),
                JournalValue::Text("memo".to_string()),
                JournalValue::Bytes(vec![0xAB; 16]),
            ],
            result_checksum: Some(0xDEAD_BEEF),
            rows_affected: Some(1),
            timestamp: chrono::Utc::now(),
            statement_type: StatementType::Update,
            duration_ms: 3,
        }
    }

    /// Per-statement journal classification (`trim` + `to_uppercase` + prefix
    /// match) run on every `log_statement`.
    pub fn bench_statement_type_from_sql(c: &mut Criterion) {
        let mut group = c.benchmark_group("journal/statement_type");
        let cases = [
            ("select", "SELECT * FROM users WHERE id = 1"),
            ("insert", "INSERT INTO users (name) VALUES ('x')"),
            ("update", "UPDATE users SET name = 'y' WHERE id = 1"),
            ("delete", "DELETE FROM users WHERE id = 1"),
            ("ddl", "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)"),
            ("txn", "BEGIN"),
            ("set", "SET search_path = public"),
            ("other", "EXPLAIN ANALYZE SELECT 1"),
        ];
        for (name, sql) in cases {
            group.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, s| {
                b.iter(|| black_box(StatementType::from_sql(black_box(s))));
            });
        }
        group.finish();
    }

    /// The size walk enforced on every `log_statement` (sums statement lengths +
    /// recurses `estimate_params_size`).
    pub fn bench_total_size(c: &mut Criterion) {
        let mut group = c.benchmark_group("journal/total_size");
        for n in [1usize, 16, 128] {
            let mut entry =
                TransactionJournalEntry::new(Uuid::new_v4(), Uuid::new_v4(), NodeId::new(), 0);
            for i in 0..n {
                entry.add_entry(sample_entry(i as u64 + 1));
            }
            group.throughput(Throughput::Elements(n as u64));
            group.bench_with_input(BenchmarkId::from_parameter(n), &entry, |b, e| {
                b.iter(|| black_box(e.total_size()));
            });
        }
        group.finish();
    }

    /// In-memory journal mutation: `add_entry` (mutation-flag bump + `Vec` push).
    pub fn bench_add_entry(c: &mut Criterion) {
        let mut group = c.benchmark_group("journal/add_entry");
        group.throughput(Throughput::Elements(1));
        group.bench_function("push", |b| {
            b.iter_batched(
                || {
                    (
                        TransactionJournalEntry::new(
                            Uuid::new_v4(),
                            Uuid::new_v4(),
                            NodeId::new(),
                            0,
                        ),
                        sample_entry(1),
                    )
                },
                |(mut container, entry)| {
                    container.add_entry(black_box(entry));
                    black_box(container.entries.len());
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    /// In-memory journal mutation: `rollback_to_savepoint` (`retain` + `truncate`).
    pub fn bench_rollback_to_savepoint(c: &mut Criterion) {
        let mut group = c.benchmark_group("journal/rollback_to_savepoint");
        for n in [16usize, 128] {
            group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                b.iter_batched(
                    || {
                        let mut entry = TransactionJournalEntry::new(
                            Uuid::new_v4(),
                            Uuid::new_v4(),
                            NodeId::new(),
                            0,
                        );
                        for i in 0..n {
                            entry.add_entry(sample_entry(i as u64 + 1));
                            if i == n / 2 {
                                entry.create_savepoint("sp".to_string());
                            }
                        }
                        entry
                    },
                    |mut entry| {
                        black_box(entry.rollback_to_savepoint(black_box("sp")));
                    },
                    BatchSize::SmallInput,
                );
            });
        }
        group.finish();
    }

    /// The TR journaling lifecycle (Arc<RwLock<HashMap>> only, no backend):
    /// begin_transaction -> log_statement -> commit_transaction. Re-uses a fixed
    /// tx id and commits each iteration so the map stays bounded and the input is
    /// deterministic.
    pub fn bench_journal_manager(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("journal/manager");
        group.throughput(Throughput::Elements(1));

        let journal = Arc::new(TransactionJournal::new());
        let tx_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();

        group.bench_function("begin_log_commit", |b| {
            b.to_async(&rt).iter(|| {
                let journal = journal.clone();
                async move {
                    journal
                        .begin_transaction(tx_id, session_id, node_id, 0)
                        .await
                        .unwrap();
                    journal
                        .log_statement(
                            tx_id,
                            "INSERT INTO t VALUES ($1)".to_string(),
                            vec![JournalValue::Int64(1)],
                            None,
                            Some(1),
                            1,
                        )
                        .await
                        .unwrap();
                    journal.commit_transaction(tx_id).await.unwrap();
                }
            });
        });
        group.finish();
    }

    /// The same journaling lifecycle under write-lock contention: K concurrent
    /// tasks, each with its own fixed tx id, hammer the single
    /// `Arc<RwLock<HashMap>>` write lock on a multi-thread runtime.
    pub fn bench_journal_contention(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("journal/manager_contention");
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();

        for k in [2usize, 8, 32] {
            let journal = Arc::new(TransactionJournal::new());
            let tx_ids: Arc<Vec<Uuid>> = Arc::new((0..k).map(|_| Uuid::new_v4()).collect());
            group.throughput(Throughput::Elements(k as u64));
            group.bench_with_input(BenchmarkId::from_parameter(k), &k, |b, &k| {
                let journal = journal.clone();
                let tx_ids = tx_ids.clone();
                b.to_async(&rt).iter(|| {
                    let journal = journal.clone();
                    let tx_ids = tx_ids.clone();
                    async move {
                        let mut handles = Vec::with_capacity(k);
                        for i in 0..k {
                            let journal = journal.clone();
                            let tx = tx_ids[i];
                            handles.push(tokio::spawn(async move {
                                journal
                                    .begin_transaction(tx, session_id, node_id, 0)
                                    .await
                                    .unwrap();
                                journal
                                    .log_statement(
                                        tx,
                                        "INSERT INTO t VALUES (1)".to_string(),
                                        Vec::new(),
                                        None,
                                        Some(1),
                                        1,
                                    )
                                    .await
                                    .unwrap();
                                journal.commit_transaction(tx).await.unwrap();
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                    }
                });
            });
        }
        group.finish();
    }

    /// Time-travel replay windowing: read-lock scan + timestamp filter + sort
    /// over a pre-populated journal (feeds `ReplayEngine::replay_window`).
    pub fn bench_entries_in_window(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("journal/entries_in_window");

        let journal = TransactionJournal::new();
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();
        rt.block_on(async {
            for _ in 0..20u64 {
                let tx = Uuid::new_v4();
                journal
                    .begin_transaction(tx, session_id, node_id, 0)
                    .await
                    .unwrap();
                for _ in 0..25 {
                    journal
                        .log_statement(
                            tx,
                            "UPDATE t SET x = 1 WHERE id = 2".to_string(),
                            Vec::new(),
                            None,
                            Some(1),
                            1,
                        )
                        .await
                        .unwrap();
                }
            }
        });
        // A window that spans every entry (500 entries across 20 transactions).
        let from = chrono::Utc::now() - chrono::Duration::days(1);
        let to = chrono::Utc::now() + chrono::Duration::days(1);

        group.throughput(Throughput::Elements(500));
        group.bench_function("scan_500", |b| {
            b.to_async(&rt).iter(|| async {
                black_box(journal.entries_in_window(from, to).await);
            });
        });
        group.finish();
    }
}

// ─────────────────────────────────────────────────────────────────────
// Mode-aware pooling (pool-modes)
// ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "pool-modes")]
mod pool_mode_benches {
    use super::*;
    use heliosdb_proxy::pool::lease::ClientId;
    use heliosdb_proxy::pool::prepared::{parse_deallocate_statement, parse_prepare_statement};
    use heliosdb_proxy::pool::{
        pool_key, ConnectionPoolManager, PoolModeConfig, StatementModeHandler, TransactionEvent,
    };
    use heliosdb_proxy::NodeEndpoint;
    use std::sync::Arc;

    /// Allocation-free SQL transaction-boundary classifier (called on every
    /// statement in transaction/statement pooling).
    pub fn bench_transaction_event_detect(c: &mut Criterion) {
        let mut group = c.benchmark_group("pool_mode/txn_event_detect");
        let cases = [
            ("begin", "BEGIN"),
            (
                "start_txn",
                "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            ),
            ("commit", "COMMIT"),
            ("rollback", "ROLLBACK"),
            ("rollback_to", "ROLLBACK TO SAVEPOINT sp1"),
            ("savepoint", "SAVEPOINT sp1"),
            ("release", "RELEASE SAVEPOINT sp1"),
            ("statement", "SELECT * FROM users WHERE id = 42"),
        ];
        for (name, sql) in cases {
            group.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, s| {
                b.iter(|| black_box(TransactionEvent::detect(black_box(s))));
            });
        }
        group.finish();
    }

    /// NUL-delimited identity-key builder, called on every data-path
    /// checkout/checkin.
    pub fn bench_pool_key(c: &mut Criterion) {
        c.bench_function("pool_mode/pool_key", |b| {
            b.iter(|| {
                black_box(pool_key(
                    black_box("10.0.0.1:5432"),
                    black_box("bench_user"),
                    black_box("app_db"),
                ))
            });
        });
    }

    /// Per-statement statement-mode safety classifier.
    pub fn bench_statement_safety(c: &mut Criterion) {
        let mut group = c.benchmark_group("pool_mode/statement_safety");
        let handler = StatementModeHandler::new();
        let cases = [
            ("safe_select", "SELECT * FROM users WHERE id = 1"),
            ("unsafe_listen", "LISTEN channel_events"),
            ("unsafe_prepare", "PREPARE p AS SELECT $1"),
            ("unsafe_set", "SET search_path = public"),
            ("safe_set_local", "SET LOCAL work_mem = '64MB'"),
        ];
        for (name, sql) in cases {
            group.bench_with_input(BenchmarkId::new("is_safe", name), &sql, |b, s| {
                b.iter(|| black_box(handler.is_safe_query(black_box(s))));
            });
            group.bench_with_input(BenchmarkId::new("warning", name), &sql, |b, s| {
                b.iter(|| black_box(handler.get_query_warning(black_box(s))));
            });
        }
        group.finish();
    }

    /// Pure PREPARE/DEALLOCATE SQL parsers used by transaction-mode
    /// prepared-statement tracking.
    pub fn bench_prepared_parse(c: &mut Criterion) {
        let mut group = c.benchmark_group("pool_mode/prepared_parse");
        let prepare_cases = [
            (
                "named",
                "PREPARE getuser AS SELECT * FROM users WHERE id = $1",
            ),
            (
                "typed",
                "PREPARE ins (int, text) AS INSERT INTO t (id, name) VALUES ($1, $2)",
            ),
        ];
        for (name, sql) in prepare_cases {
            group.bench_with_input(BenchmarkId::new("prepare", name), &sql, |b, s| {
                b.iter(|| black_box(parse_prepare_statement(black_box(s))));
            });
        }
        let deallocate_cases = [("named", "DEALLOCATE getuser"), ("all", "DEALLOCATE ALL")];
        for (name, sql) in deallocate_cases {
            group.bench_with_input(BenchmarkId::new("deallocate", name), &sql, |b, s| {
                b.iter(|| black_box(parse_deallocate_statement(black_box(s))));
            });
        }
        group.finish();
    }

    /// Mode-aware pooling hot path under real client concurrency: K tasks, each
    /// with its own client id, run acquire -> release against a skeleton-mode
    /// manager (DashMap lease insert/remove + underlying skeleton pool acquire +
    /// the transaction-mode reset bookkeeping). No backend needed.
    pub fn bench_manager_contention(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (manager, node_id) = rt.block_on(async {
            let config = PoolModeConfig {
                max_pool_size: 128,
                ..PoolModeConfig::transaction_mode()
            };
            let manager = Arc::new(ConnectionPoolManager::new(config));
            let node = NodeEndpoint::new("127.0.0.1", 5432);
            let node_id = node.id;
            manager.add_node(&node).await;
            (manager, node_id)
        });

        let mut group = c.benchmark_group("pool_mode/manager_acquire_release");
        for k in [2usize, 8, 32] {
            let client_ids: Arc<Vec<ClientId>> =
                Arc::new((0..k).map(|_| ClientId::new()).collect());
            group.throughput(Throughput::Elements(k as u64));
            group.bench_with_input(BenchmarkId::from_parameter(k), &k, |b, &k| {
                let manager = manager.clone();
                let client_ids = client_ids.clone();
                b.to_async(&rt).iter(|| {
                    let manager = manager.clone();
                    let client_ids = client_ids.clone();
                    async move {
                        let mut handles = Vec::with_capacity(k);
                        for i in 0..k {
                            let manager = manager.clone();
                            let cid = client_ids[i];
                            handles.push(tokio::spawn(async move {
                                let lease = manager.acquire(cid, &node_id).await.unwrap();
                                manager.release(lease).await;
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                    }
                });
            });
        }
        group.finish();
    }

    /// The synchronous per-statement decision: `TransactionEvent::detect` + the
    /// lease state machine + the DashMap statement-counter bump, driven over a
    /// BEGIN..COMMIT sequence against one skeleton lease held across iterations.
    pub fn bench_manager_on_statement_complete(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (manager, mut lease) = rt.block_on(async {
            let config = PoolModeConfig {
                max_pool_size: 16,
                ..PoolModeConfig::transaction_mode()
            };
            let manager = ConnectionPoolManager::new(config);
            let node = NodeEndpoint::new("127.0.0.1", 5432);
            manager.add_node(&node).await;
            let cid = ClientId::new();
            let lease = manager.acquire(cid, &node.id).await.unwrap();
            (manager, lease)
        });

        let sqls = [
            "BEGIN",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 2 WHERE id = 1",
            "SELECT * FROM t WHERE id = 1",
            "COMMIT",
        ];

        let mut group = c.benchmark_group("pool_mode/on_statement_complete");
        group.throughput(Throughput::Elements(sqls.len() as u64));
        group.bench_function("txn_sequence", |b| {
            b.iter(|| {
                for &sql in &sqls {
                    black_box(manager.on_statement_complete(&mut lease, black_box(sql)));
                }
            });
        });
        group.finish();
    }
}

// When `pool-modes` is disabled, provide no-op stand-ins.
#[cfg(not(feature = "pool-modes"))]
mod pool_mode_benches {
    use super::*;

    pub fn bench_transaction_event_detect(c: &mut Criterion) {
        c.bench_function("pool_mode/noop_requires_pool_modes_feature", |b| {
            b.iter(|| black_box(42));
        });
    }
    pub fn bench_pool_key(_c: &mut Criterion) {}
    pub fn bench_statement_safety(_c: &mut Criterion) {}
    pub fn bench_prepared_parse(_c: &mut Criterion) {}
    pub fn bench_manager_contention(_c: &mut Criterion) {}
    pub fn bench_manager_on_statement_complete(_c: &mut Criterion) {}
}

criterion_group!(
    switchover,
    bench_switchover_buffer_query,
    bench_switchover_drain,
);
criterion_group!(
    journal,
    journal_benches::bench_statement_type_from_sql,
    journal_benches::bench_total_size,
    journal_benches::bench_add_entry,
    journal_benches::bench_rollback_to_savepoint,
    journal_benches::bench_journal_manager,
    journal_benches::bench_journal_contention,
    journal_benches::bench_entries_in_window,
);
criterion_group!(
    pool_mode,
    pool_mode_benches::bench_transaction_event_detect,
    pool_mode_benches::bench_pool_key,
    pool_mode_benches::bench_statement_safety,
    pool_mode_benches::bench_prepared_parse,
    pool_mode_benches::bench_manager_contention,
    pool_mode_benches::bench_manager_on_statement_complete,
);
criterion_main!(switchover, journal, pool_mode);
