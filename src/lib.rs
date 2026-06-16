//! HeliosDB Proxy - Standalone Connection Router
//!
//! A standalone proxy for HeliosDB-Lite providing:
//! - Connection pooling
//! - Load balancing (read/write splitting)
//! - Health monitoring
//! - Transaction Replay (TR)
//!
//! # Deployment Options
//!
//! - **Standalone binary**: Run as a separate process
//! - **Kubernetes sidecar**: Deploy alongside your application
//! - **Embedded library**: Use as a library in your application
//!
//! # Quick Start
//!
//! ```bash
//! # Start with config file
//! heliosdb-proxy --config /etc/heliosdb/proxy.toml
//!
//! # Start with command line options
//! heliosdb-proxy \
//!   --listen 0.0.0.0:5432 \
//!   --primary db-primary:5432 \
//!   --standby db-standby-1:5432 \
//!   --standby db-standby-2:5432
//! ```
//!
//! # Configuration Example
//!
//! ```toml
//! [proxy]
//! listen_address = "0.0.0.0:5432"
//! admin_address = "0.0.0.0:9090"
//!
//! [pool]
//! min_connections = 5
//! max_connections = 100
//! idle_timeout_secs = 300
//!
//! [load_balancer]
//! strategy = "round_robin"  # or "least_connections", "latency_based"
//! read_write_split = true
//!
//! [health]
//! check_interval_secs = 5
//! failure_threshold = 3
//!
//! [[nodes]]
//! host = "db-primary"
//! port = 5432
//! role = "primary"
//!
//! [[nodes]]
//! host = "db-standby-1"
//! port = 5432
//! role = "standby"
//! ```

// ── Core modules (always available) ──────────────────────────────────
pub mod admin;
pub mod agent_contract;
pub mod auth_scram;
pub mod backend;
pub mod batch;
pub mod branch;
pub mod client_tls;
pub mod config;
pub mod connection_pool;
pub mod failover_controller;
pub mod health_checker;
pub mod http_gateway;
pub mod load_balancer;
pub mod mcp;
pub mod mirror;
pub mod pipeline;
pub mod plugin_registry;
pub mod primary_tracker;
pub mod protocol;
pub mod request;
pub mod server;
pub mod switchover_buffer;

// ── Connection pooling modes (Session/Transaction/Statement) ─────────
#[cfg(feature = "pool-modes")]
pub mod pool;

// ── TR (Transaction Replay) modules ─────────────────────────────────
#[cfg(feature = "ha-tr")]
pub mod cursor_restore;
#[cfg(feature = "ha-tr")]
pub mod failover_replay;
#[cfg(feature = "ha-tr")]
pub mod replay;
#[cfg(feature = "ha-tr")]
pub mod session_migrate;
#[cfg(feature = "ha-tr")]
pub mod transaction_journal;

// ── Zero-downtime PG major-version upgrade orchestrator (T2.1) ─────
#[cfg(feature = "ha-tr")]
pub mod upgrade_orchestrator;

// ── R&D: shadow execution (T3.4) ────────────────────────────────────
#[cfg(feature = "ha-tr")]
pub mod shadow_execute;

// ── Query caching (L1/L2/L3 multi-tier cache) ──────────────────────
#[cfg(feature = "query-cache")]
pub mod cache;

// ── Query routing hints ─────────────────────────────────────────────
#[cfg(feature = "routing-hints")]
pub mod routing;

// ── Replica lag-aware routing ───────────────────────────────────────
#[cfg(feature = "lag-routing")]
pub mod lag;

// ── Rate limiting and query throttling ──────────────────────────────
#[cfg(feature = "rate-limiting")]
pub mod rate_limit;

// ── Circuit breaker pattern ─────────────────────────────────────────
#[cfg(feature = "circuit-breaker")]
pub mod circuit_breaker;

// ── Query analytics and slow query log ──────────────────────────────
#[cfg(feature = "query-analytics")]
pub mod analytics;

// ── Anomaly detection (T3.1) — rate spikes, credential stuffing,
// SQL injection heuristics, novel query shapes ─────────────────────
#[cfg(feature = "anomaly-detection")]
pub mod anomaly;

// ── Edge / geo proxy mode (T3.2) ───────────────────────────────────
#[cfg(feature = "edge-proxy")]
pub mod edge;

// ── Multi-tenancy support ───────────────────────────────────────────
#[cfg(feature = "multi-tenancy")]
pub mod multi_tenancy;

// ── Authentication proxy ────────────────────────────────────────────
#[cfg(feature = "auth-proxy")]
pub mod auth;

// ── Query rewriting ─────────────────────────────────────────────────
#[cfg(feature = "query-rewriting")]
pub mod rewriter;

// ── WASM plugin system ──────────────────────────────────────────────
#[cfg(feature = "wasm-plugins")]
pub mod plugins;

// ── GraphQL-to-SQL gateway ──────────────────────────────────────────
#[cfg(feature = "graphql-gateway")]
pub mod graphql;

// ── Schema-aware routing ────────────────────────────────────────────
#[cfg(feature = "schema-routing")]
pub mod schema_routing;

// ── Distributed intelligent caching (DistribCache) ──────────────────
#[cfg(feature = "distribcache")]
pub mod distribcache;

// ── Embedded skill-bundle deployer ──────────────────────────────────
//
// Always-on: the `heliosdb-proxy install skills` subcommand calls
// into this. Adds ~80 KiB to the binary (the `.claude/skills/`
// bundle, embedded by `include_dir!`).
pub mod skills;

use thiserror::Error;
use uuid::Uuid;

/// Proxy error types
#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Pool error: {0}")]
    Pool(String),

    #[error("Health check error: {0}")]
    HealthCheck(String),

    #[error("Failover error: {0}")]
    Failover(String),

    #[error("Failover failed: {0}")]
    FailoverFailed(String),

    #[error("Transaction replay failed: {0}")]
    ReplayFailed(String),

    #[error("Session migration failed: {0}")]
    SessionMigration(String),

    #[error("Cursor restore failed: {0}")]
    CursorRestore(String),

    #[error("Routing error: {0}")]
    Routing(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Pool exhausted: {0}")]
    PoolExhausted(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("No healthy nodes available")]
    NoHealthyNodes,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ProxyError>;

/// Proxy version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default listen port
pub const DEFAULT_PORT: u16 = 5432;

/// Default admin port
pub const DEFAULT_ADMIN_PORT: u16 = 9090;

/// Node identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Primary node (accepts writes)
    Primary,
    /// Standby node (read-only, can be promoted)
    Standby,
    /// Read replica (read-only, cannot be promoted)
    ReadReplica,
    /// Unknown role (during discovery)
    Unknown,
}

/// Node endpoint information
#[derive(Debug, Clone)]
pub struct NodeEndpoint {
    /// Node identifier
    pub id: NodeId,
    /// Host address
    pub host: String,
    /// Port
    pub port: u16,
    /// Node role
    pub role: NodeRole,
    /// Weight for load balancing (higher = more traffic)
    pub weight: u32,
    /// Whether this node is enabled
    pub enabled: bool,
}

impl NodeEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            id: NodeId::new(),
            host: host.into(),
            port,
            role: NodeRole::Unknown,
            weight: 100,
            enabled: true,
        }
    }

    pub fn with_role(mut self, role: NodeRole) -> Self {
        self.role = role;
        self
    }

    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

// ── PostgreSQL compatibility integration test ───────────────────────
//
// Verifies that all feature modules can be instantiated and used in a
// PostgreSQL-only context (no HeliosDB dependencies required).

#[cfg(test)]
mod postgresql_compat_tests {
    use super::*;

    /// All core types work for PostgreSQL endpoints.
    #[test]
    fn test_pg_node_endpoints() {
        let primary =
            NodeEndpoint::new("pg-primary.example.com", 5432).with_role(NodeRole::Primary);
        let standby =
            NodeEndpoint::new("pg-standby-1.example.com", 5432).with_role(NodeRole::Standby);
        let replica = NodeEndpoint::new("pg-replica-1.example.com", 5433)
            .with_role(NodeRole::ReadReplica)
            .with_weight(50);

        assert_eq!(primary.role, NodeRole::Primary);
        assert_eq!(standby.role, NodeRole::Standby);
        assert_eq!(replica.weight, 50);
        assert_eq!(replica.address(), "pg-replica-1.example.com:5433");
    }

    /// Load balancer config works for PostgreSQL cluster with read/write splitting.
    #[test]
    fn test_pg_load_balancer_config() {
        use load_balancer::*;

        let config = LoadBalancerConfig {
            read_write_split: true,
            read_strategy: RoutingStrategy::RoundRobin,
            write_strategy: RoutingStrategy::PrimaryOnly,
            ..Default::default()
        };

        assert!(config.read_write_split);
        assert_eq!(config.read_strategy, RoutingStrategy::RoundRobin);
        assert_eq!(config.write_strategy, RoutingStrategy::PrimaryOnly);

        // Verify the LB can be constructed
        let _lb = LoadBalancer::new(config);
    }

    /// Health checker works with standard PostgreSQL `SELECT 1` checks.
    #[test]
    fn test_pg_health_config() {
        use health_checker::*;

        let config = HealthConfig {
            check_query: "SELECT 1".to_string(),
            detailed_checks: true,
            ..Default::default()
        };

        assert_eq!(config.check_query, "SELECT 1");
        assert!(config.detailed_checks);
    }

    /// Failover controller works for PostgreSQL streaming replication.
    #[tokio::test]
    async fn test_pg_failover() {
        use failover_controller::*;

        let controller = FailoverController::new(FailoverConfig {
            auto_failover: true,
            prefer_sync_standby: true,
            ..Default::default()
        });

        let primary = NodeId::new();
        controller.set_primary(primary).await;
        assert_eq!(controller.get_primary().await, Some(primary));

        // Register candidates
        let sync_standby = NodeId::new();
        controller
            .register_candidate(FailoverCandidate {
                node_id: sync_standby,
                endpoint: NodeEndpoint::new("pg-sync", 5432).with_role(NodeRole::Standby),
                is_sync: true,
                lag_bytes: 0,
                priority: 1,
                last_heartbeat: None,
            })
            .await;

        let async_standby = NodeId::new();
        controller
            .register_candidate(FailoverCandidate {
                node_id: async_standby,
                endpoint: NodeEndpoint::new("pg-async", 5432).with_role(NodeRole::Standby),
                is_sync: false,
                lag_bytes: 1024,
                priority: 2,
                last_heartbeat: None,
            })
            .await;

        // Verify state
        assert_eq!(controller.state().await, FailoverState::Normal);
        assert_eq!(controller.failover_count(), 0);
    }

    /// Connection pool config works for PostgreSQL connections.
    #[test]
    fn test_pg_connection_pool() {
        use connection_pool::*;

        let config = PoolConfig {
            min_connections: 2,
            max_connections: 20,
            test_on_acquire: true,
            ..Default::default()
        };

        assert_eq!(config.min_connections, 2);
        assert_eq!(config.max_connections, 20);
        assert!(config.test_on_acquire);

        let _pool = ConnectionPool::new(config);
    }

    /// Switchover buffer works for PostgreSQL planned switchover.
    #[tokio::test]
    async fn test_pg_switchover_buffer() {
        use switchover_buffer::*;

        let buffer = SwitchoverBuffer::new(BufferConfig {
            buffer_timeout: std::time::Duration::from_secs(5),
            max_buffered_queries: 1000,
            ..Default::default()
        });

        assert_eq!(buffer.state(), BufferState::Passthrough);
        assert!(!buffer.is_buffering());

        // Simulate pg_ctl promote workflow
        buffer.start_buffering();
        assert!(buffer.is_buffering());

        let rx = buffer
            .buffer_query("INSERT INTO orders VALUES (1)".to_string(), vec![], 1)
            .unwrap();

        // Simulate promotion complete
        buffer.stop_buffering();
        buffer.drain(|_sql, _params| async { Ok(()) }).await;

        let result = rx.await.unwrap();
        assert!(matches!(result, BufferResult::Success));
    }

    /// Primary tracker works with the standalone mode for PostgreSQL.
    #[test]
    fn test_pg_primary_tracker_standalone() {
        use primary_tracker::*;

        let tracker = PrimaryTracker::new_standalone();

        // Simulate discovering primary via pg_is_in_recovery()
        let pg_primary = uuid::Uuid::new_v4();
        tracker.set_primary(pg_primary, "pg-primary.local:5432".to_string());
        tracker.confirm_primary();

        assert!(tracker.has_primary());
        assert!(tracker.get_primary().unwrap().is_confirmed);

        // Simulate failover detected
        tracker.clear_primary();
        assert!(!tracker.has_primary());

        // New primary
        let pg_new_primary = uuid::Uuid::new_v4();
        tracker.set_primary(pg_new_primary, "pg-standby.local:5432".to_string());
        tracker.confirm_primary();
        assert_eq!(
            tracker.get_primary_address(),
            Some("pg-standby.local:5432".to_string())
        );
    }

    /// Transaction journal works for PostgreSQL transaction replay.
    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_pg_transaction_replay() {
        use transaction_journal::*;

        let journal = TransactionJournal::new();
        let tx_id = uuid::Uuid::new_v4();
        let session_id = uuid::Uuid::new_v4();
        let node = NodeId::new();

        // Journal a PostgreSQL transaction
        journal
            .begin_transaction(tx_id, session_id, node, 0)
            .await
            .unwrap();
        journal
            .log_statement(tx_id, "BEGIN".to_string(), vec![], None, None, 1)
            .await
            .unwrap();
        journal
            .log_statement(
                tx_id,
                "INSERT INTO accounts (id, balance) VALUES ($1, $2)".to_string(),
                vec![JournalValue::Int64(1), JournalValue::Float64(100.0)],
                Some(12345),
                Some(1),
                5,
            )
            .await
            .unwrap();
        journal
            .log_statement(
                tx_id,
                "UPDATE accounts SET balance = balance - $1 WHERE id = $2".to_string(),
                vec![JournalValue::Float64(25.0), JournalValue::Int64(1)],
                Some(67890),
                Some(1),
                3,
            )
            .await
            .unwrap();

        let j = journal.get_journal(&tx_id).await.unwrap();
        assert_eq!(j.entries.len(), 3);
        assert!(j.has_mutations);

        // Verify statement types
        assert_eq!(j.entries[0].statement_type, StatementType::Transaction);
        assert_eq!(j.entries[1].statement_type, StatementType::Insert);
        assert_eq!(j.entries[2].statement_type, StatementType::Update);

        // Commit clears journal
        journal.commit_transaction(tx_id).await.unwrap();
        assert!(journal.get_journal(&tx_id).await.is_none());
    }

    /// Session migration works for PostgreSQL session parameters.
    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_pg_session_migration() {
        use session_migrate::*;

        let migrate = SessionMigrate::new();
        let session_id = uuid::Uuid::new_v4();
        let node = NodeId::new();

        let mut state =
            SessionState::new(session_id, "postgres".to_string(), "mydb".to_string(), node);

        // Set PostgreSQL-specific session parameters
        state.set_parameter("timezone".to_string(), "America/New_York".to_string());
        state.set_parameter("search_path".to_string(), "public, app_schema".to_string());
        state.set_parameter("statement_timeout".to_string(), "30000".to_string());
        state.set_parameter("work_mem".to_string(), "256MB".to_string());

        // Add a prepared statement
        state.add_prepared_statement(PreparedStatementInfo {
            name: "get_user".to_string(),
            query: "SELECT * FROM users WHERE id = $1".to_string(),
            param_types: vec!["integer".to_string()],
            created_at: chrono::Utc::now(),
        });

        migrate.register_session(state).await.unwrap();

        // Generate SET statements for replay on new primary
        let session = migrate.get_session(&session_id).await.unwrap();
        let restore_stmts = session.generate_restore_statements();

        assert!(restore_stmts.iter().any(|s| s.contains("America/New_York")));
        assert!(restore_stmts.iter().any(|s| s.contains("search_path")));
        assert!(restore_stmts
            .iter()
            .any(|s| s.contains("statement_timeout")));
        assert!(restore_stmts.iter().any(|s| s.contains("PREPARE get_user")));
    }

    /// Pipeline supports PostgreSQL extended query protocol pipelining.
    #[tokio::test]
    async fn test_pg_pipelining() {
        use pipeline::*;

        let pipeline = RequestPipeline::new(PipelineConfig {
            max_depth: 16,
            enabled: true,
            ..Default::default()
        });

        let conn_id = 1;

        // Simulate pipelined Parse/Bind/Execute sequence
        let t1 = pipeline
            .submit(conn_id, b"Parse: SELECT $1::int".to_vec())
            .unwrap();
        let t2 = pipeline.submit(conn_id, b"Bind: [42]".to_vec()).unwrap();
        let t3 = pipeline.submit(conn_id, b"Execute".to_vec()).unwrap();

        assert_eq!(pipeline.depth(conn_id), 3);

        // Complete in order (FIFO — matches PG protocol)
        pipeline.complete_next(conn_id, b"ParseComplete".to_vec(), true, None);
        pipeline.complete_next(conn_id, b"BindComplete".to_vec(), true, None);
        pipeline.complete_next(conn_id, b"DataRow: 42".to_vec(), true, None);

        assert_eq!(pipeline.depth(conn_id), 0);

        let r1 = t1.wait().await.unwrap();
        assert!(r1.success);
    }

    /// Batch INSERT works for PostgreSQL bulk inserts.
    #[tokio::test]
    async fn test_pg_batch_insert() {
        use batch::*;

        let config = BatchConfig {
            max_batch_size: 3,
            ..Default::default()
        };
        let batcher = InsertBatcher::new(config);

        batcher
            .add(
                "orders".to_string(),
                vec!["id".to_string(), "total".to_string()],
                vec![vec!["1".to_string(), "99.99".to_string()]],
                "INSERT INTO orders (id, total) VALUES (1, 99.99)".to_string(),
            )
            .unwrap();

        assert_eq!(batcher.batch_size("orders"), 1);

        let stats = batcher.stats();
        assert_eq!(stats.inserts_received, 1);
        assert_eq!(stats.rows_received, 1);
    }
}
