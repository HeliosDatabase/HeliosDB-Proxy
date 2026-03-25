//! Query Routing Hints - HeliosProxy Feature 03
//!
//! SQL comment-based routing hints give applications fine-grained control
//! over query routing. Hints take precedence over default routing policies.
//!
//! # Supported Hints
//!
//! | Hint | Values | Description |
//! |------|--------|-------------|
//! | `route` | `primary`, `standby`, `sync`, `semisync`, `async`, `any`, `local` | Target node type |
//! | `node` | Node name | Route to specific node |
//! | `consistency` | `strong`, `bounded`, `eventual` | Consistency requirement |
//! | `pool` | `session`, `transaction`, `statement` | Pooling mode |
//! | `cache` | `skip`, `refresh`, `semantic` | Cache behavior |
//! | `timeout` | Duration (e.g., `5s`, `100ms`) | Query timeout |
//! | `priority` | `low`, `normal`, `high`, `critical` | Scheduling priority |
//! | `lag` | Duration (e.g., `100ms`, `1s`) | Maximum acceptable lag |
//!
//! # Examples
//!
//! ```sql
//! -- Route critical reads to primary
//! /*helios:route=primary*/
//! SELECT balance FROM accounts WHERE id = $1 FOR UPDATE;
//!
//! -- Route analytics to async replica
//! /*helios:route=async,lag=10s,priority=low*/
//! SELECT DATE(created_at), COUNT(*) FROM orders GROUP BY 1;
//!
//! -- Route to specific node
//! /*helios:node=standby-sync-1*/
//! SELECT * FROM debug_logs;
//! ```

pub mod hint_parser;
pub mod query_router;
pub mod node_filter;
pub mod metrics;
pub mod config;

pub use hint_parser::{
    HintParser, RoutingHint, RouteTarget, ConsistencyLevel,
    QueryPriority, CacheBehavior, ParsedHints,
};
pub use query_router::{QueryRouter, RoutingDecision, RoutingReason};
pub use node_filter::{NodeFilter, NodeCriteria, FilterResult, NodeInfo, NodeRole, SyncMode};
pub use metrics::{RoutingMetrics, RoutingStats, HintUsageStats};
pub use config::{RoutingConfig, HintConfig, ConsistencyConfig, AliasConfig};

use thiserror::Error;
use std::time::Duration;

/// Routing errors
#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("No nodes match routing hints: {0}")]
    NoMatchingNodes(String),

    #[error("Invalid hint combination: {0}")]
    InvalidHintCombination(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Hint not allowed: {0}")]
    HintNotAllowed(String),

    #[error("Parse error: {0}")]
    ParseError(String),
}

pub type Result<T> = std::result::Result<T, RoutingError>;

/// Parse duration from string (e.g., "5s", "100ms", "1m")
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();

    if let Some(num) = s.strip_suffix("ms") {
        num.parse::<u64>().ok().map(Duration::from_millis)
    } else if let Some(num) = s.strip_suffix('s') {
        num.parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(num) = s.strip_suffix('m') {
        num.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60))
    } else if let Some(num) = s.strip_suffix('h') {
        num.parse::<u64>().ok().map(|h| Duration::from_secs(h * 3600))
    } else {
        // Try parsing as milliseconds by default
        s.parse::<u64>().ok().map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("100ms"), Some(Duration::from_millis(100)));
        assert_eq!(parse_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("500"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("invalid"), None);
    }
}
