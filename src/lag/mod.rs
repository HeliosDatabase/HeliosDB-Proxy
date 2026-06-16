//! Replica Lag-Aware Routing Module
//!
//! This module provides lag-aware query routing for HeliosProxy, ensuring
//! queries are routed to replicas that meet freshness requirements.
//!
//! # Features
//!
//! - Continuous lag monitoring via WAL position tracking
//! - Read-your-writes consistency guarantees
//! - Configurable lag thresholds per sync mode
//! - Integration with routing hints (`/*helios:lag=X*/`)
//!
//! # Architecture
//!
//! ```text
//! Query + Lag Hint
//!       │
//!       ▼
//! ┌─────────────────┐
//! │  LagAwareRouter │──────► LagMonitor
//! │                 │        (node lag data)
//! │  - Extract hint │
//! │  - Check RYW    │──────► RYWTracker
//! │  - Filter nodes │        (session LSNs)
//! └─────────────────┘
//!       │
//!       ▼
//!   Eligible Nodes
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::lag::{LagMonitor, LagAwareRouter, LagRoutingConfig};
//!
//! let config = LagRoutingConfig::default();
//! let monitor = LagMonitor::new(config.poll_interval);
//! let router = LagAwareRouter::new(monitor, config);
//!
//! // Route with freshness requirement
//! let decision = router.route(query, session_id, Some(Duration::from_millis(100)));
//! ```

pub mod config;
pub mod metrics;
pub mod monitor;
pub mod router;
pub mod ryw;

// Re-exports for convenience
pub use config::{LagCalculation, LagRoutingConfig, SyncModeLagConfig};
pub use metrics::{LagMetrics, LagStatsSnapshot, NodeLagStats};
pub use monitor::{LagInfo, LagMonitor, LagTrend, NodeLagData};
pub use router::{LagAwareRouter, LagRoutingDecision, LagRoutingReason};
pub use ryw::{ReadYourWritesTracker, RywSession, WorkflowConsistency, WorkflowTracker};

/// SyncMode enum for replica synchronization classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SyncMode {
    /// Synchronous replication - zero data loss guarantee
    Sync,
    /// Semi-synchronous - bounded lag guarantee
    SemiSync,
    /// Asynchronous - eventual consistency
    Async,
    /// Unknown or unclassified
    #[default]
    Unknown,
}

impl std::fmt::Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncMode::Sync => write!(f, "sync"),
            SyncMode::SemiSync => write!(f, "semisync"),
            SyncMode::Async => write!(f, "async"),
            SyncMode::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::str::FromStr for SyncMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sync" | "synchronous" => Ok(SyncMode::Sync),
            "semisync" | "semi-sync" | "semi_sync" => Ok(SyncMode::SemiSync),
            "async" | "asynchronous" => Ok(SyncMode::Async),
            "unknown" => Ok(SyncMode::Unknown),
            _ => Err(format!("Unknown sync mode: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_mode_display() {
        assert_eq!(SyncMode::Sync.to_string(), "sync");
        assert_eq!(SyncMode::SemiSync.to_string(), "semisync");
        assert_eq!(SyncMode::Async.to_string(), "async");
        assert_eq!(SyncMode::Unknown.to_string(), "unknown");
    }

    #[test]
    fn test_sync_mode_from_str() {
        assert_eq!("sync".parse::<SyncMode>().unwrap(), SyncMode::Sync);
        assert_eq!("synchronous".parse::<SyncMode>().unwrap(), SyncMode::Sync);
        assert_eq!("semisync".parse::<SyncMode>().unwrap(), SyncMode::SemiSync);
        assert_eq!("semi-sync".parse::<SyncMode>().unwrap(), SyncMode::SemiSync);
        assert_eq!("async".parse::<SyncMode>().unwrap(), SyncMode::Async);
        assert_eq!("asynchronous".parse::<SyncMode>().unwrap(), SyncMode::Async);
        assert!("invalid".parse::<SyncMode>().is_err());
    }
}
