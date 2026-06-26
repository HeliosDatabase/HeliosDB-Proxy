//! Connection Pooling Modes - HeliosProxy
//!
//! Implements three connection pooling modes for optimized backend connection sharing:
//!
//! - **Session Mode**: 1:1 client-to-backend mapping (current default behavior)
//! - **Transaction Mode**: Return connection to pool after COMMIT/ROLLBACK
//! - **Statement Mode**: Return connection after each statement (most aggressive)
//!
//! # Feature Flag
//!
//! This module requires the `pool-modes` feature flag.
//!
//! # Usage
//!
//! ```rust,ignore
//! use heliosdb_proxy::pool::{PoolingMode, ConnectionPoolManager, PoolModeConfig};
//!
//! let config = PoolModeConfig {
//!     default_mode: PoolingMode::Transaction,
//!     max_pool_size: 100,
//!     ..Default::default()
//! };
//!
//! let manager = ConnectionPoolManager::new(config);
//! let lease = manager.acquire(client_id, &node).await?;
//! // Use connection...
//! manager.release(lease).await;
//! ```

pub mod backend_pool;
pub mod config;
pub mod hardening;
pub mod lease;
pub mod manager;
pub mod metrics;
pub mod mode;
pub mod prepared;
pub mod reset;
pub mod session;
pub mod statement;
pub mod transaction;

// Re-exports
pub use backend_pool::{pool_key, BackendIdlePool};
pub use config::PoolModeConfig;
pub use hardening::{
    ConnectionHealthValidator, HardeningStats, PoolExhaustionMonitor, PoolHardening,
    StaleLeaseCleaner, TransactionLeakDetector,
};
pub use lease::{ConnectionLease, LeaseAction};
pub use manager::ConnectionPoolManager;
pub use metrics::PoolModeMetrics;
pub use mode::{PoolingMode, PreparedStatementMode, TransactionEvent};
pub use prepared::PreparedStatementTracker;
pub use reset::ConnectionResetExecutor;
pub use session::SessionModeHandler;
pub use statement::StatementModeHandler;
pub use transaction::TransactionModeHandler;
