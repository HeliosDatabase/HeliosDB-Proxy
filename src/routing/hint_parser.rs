//! SQL Hint Parser
//!
//! Parses routing hints from SQL comments.
//! Supports both single hints and comma-separated multiple hints.
//!
//! # Format
//!
//! ```text
//! /*helios:key=value*/
//! /*helios:key1=value1,key2=value2*/
//! ```

use super::{parse_duration, RoutingError, Result};
use regex::Regex;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

#[cfg(feature = "pool-modes")]
use crate::pool::PoolingMode;

/// Compiled regex for hint parsing
static HINT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"/\*\s*helios:([^*]+)\*/").expect("Invalid hint regex")
});

/// Key-value pair regex
static KV_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\w+)\s*=\s*([^,\s]+)").expect("Invalid key-value regex")
});

/// Hint parser for SQL routing hints
#[derive(Debug, Clone, Default)]
pub struct HintParser {
    /// Whether to strip hints from query before sending to backend
    pub strip_hints: bool,
}

impl HintParser {
    /// Create a new hint parser
    pub fn new() -> Self {
        Self { strip_hints: true }
    }

    /// Create parser with hint stripping disabled
    pub fn without_stripping() -> Self {
        Self { strip_hints: false }
    }

    /// Parse all routing hints from a SQL query
    pub fn parse(&self, query: &str) -> ParsedHints {
        let mut hints = ParsedHints::default();

        for cap in HINT_REGEX.captures_iter(query) {
            let hint_content = cap.get(1).map(|m| m.as_str()).unwrap_or("");

            // Parse key-value pairs
            for kv in KV_REGEX.captures_iter(hint_content) {
                let key = kv.get(1).map(|m| m.as_str()).unwrap_or("");
                let value = kv.get(2).map(|m| m.as_str()).unwrap_or("");

                if let Some(hint) = self.parse_hint(key, value) {
                    hints.add(hint);
                }
            }
        }

        hints
    }

    /// Parse a single hint from key-value pair
    fn parse_hint(&self, key: &str, value: &str) -> Option<RoutingHint> {
        match key.to_lowercase().as_str() {
            "route" => RouteTarget::from_str(value).ok().map(RoutingHint::Route),
            "node" => Some(RoutingHint::Node(value.to_string())),
            "consistency" => ConsistencyLevel::from_str(value).ok().map(RoutingHint::Consistency),
            "pool" => PoolingModeHint::from_str(value).ok().map(RoutingHint::Pool),
            "cache" => CacheBehavior::from_str(value).ok().map(RoutingHint::Cache),
            "timeout" => parse_duration(value).map(RoutingHint::Timeout),
            "priority" => QueryPriority::from_str(value).ok().map(RoutingHint::Priority),
            "lag" => parse_duration(value).map(RoutingHint::MaxLag),
            "retry" => self.parse_retry(value).map(RoutingHint::Retry),
            "branch" => Some(RoutingHint::Branch(value.to_string())),
            "twr" => value.parse::<bool>().ok().map(RoutingHint::TransparentWriteRouting),
            "tool" => Some(RoutingHint::AgentTool(value.to_string())),
            "workflow" => Some(RoutingHint::WorkflowStep(value.to_string())),
            "prefetch" => value.parse::<bool>().ok().map(RoutingHint::Prefetch),
            "cache_ttl" => value.parse::<u64>().ok().map(|s| RoutingHint::CacheTtl(Duration::from_secs(s))),
            _ => None,
        }
    }

    /// Parse retry hint value
    fn parse_retry(&self, value: &str) -> Option<RetryBehavior> {
        match value.to_lowercase().as_str() {
            "true" | "yes" => Some(RetryBehavior::Auto),
            "false" | "no" => Some(RetryBehavior::None),
            _ => value.parse::<u32>().ok().map(RetryBehavior::Count),
        }
    }

    /// Strip hints from query for backend execution
    pub fn strip(&self, query: &str) -> String {
        HINT_REGEX.replace_all(query, "").trim().to_string()
    }

    /// Extract raw hint string from query (for logging)
    pub fn extract_raw(&self, query: &str) -> Vec<String> {
        HINT_REGEX
            .captures_iter(query)
            .filter_map(|cap| cap.get(0).map(|m| m.as_str().to_string()))
            .collect()
    }
}

/// Parsed hints collection
#[derive(Debug, Clone, Default)]
pub struct ParsedHints {
    /// All parsed hints
    hints: Vec<RoutingHint>,
    /// Route target (if specified)
    pub route: Option<RouteTarget>,
    /// Specific node (if specified)
    pub node: Option<String>,
    /// Consistency level (if specified)
    pub consistency: Option<ConsistencyLevel>,
    /// Pool mode (if specified)
    pub pool: Option<PoolingModeHint>,
    /// Cache behavior (if specified)
    pub cache: Option<CacheBehavior>,
    /// Query timeout (if specified)
    pub timeout: Option<Duration>,
    /// Query priority (if specified)
    pub priority: Option<QueryPriority>,
    /// Maximum acceptable lag (if specified)
    pub max_lag: Option<Duration>,
    /// Retry behavior (if specified)
    pub retry: Option<RetryBehavior>,
    /// Branch name (if specified)
    pub branch: Option<String>,
    /// Transparent Write Routing (if specified)
    pub twr: Option<bool>,
    /// Cache TTL override (if specified)
    pub cache_ttl: Option<Duration>,
}

impl ParsedHints {
    /// Add a hint to the collection
    pub fn add(&mut self, hint: RoutingHint) {
        match &hint {
            RoutingHint::Route(target) => self.route = Some(*target),
            RoutingHint::Node(name) => self.node = Some(name.clone()),
            RoutingHint::Consistency(level) => self.consistency = Some(*level),
            RoutingHint::Pool(mode) => self.pool = Some(*mode),
            RoutingHint::Cache(behavior) => self.cache = Some(*behavior),
            RoutingHint::Timeout(dur) => self.timeout = Some(*dur),
            RoutingHint::Priority(pri) => self.priority = Some(*pri),
            RoutingHint::MaxLag(dur) => self.max_lag = Some(*dur),
            RoutingHint::Retry(retry) => self.retry = Some(retry.clone()),
            RoutingHint::Branch(name) => self.branch = Some(name.clone()),
            RoutingHint::TransparentWriteRouting(enabled) => self.twr = Some(*enabled),
            RoutingHint::CacheTtl(dur) => self.cache_ttl = Some(*dur),
            _ => {}
        }
        self.hints.push(hint);
    }

    /// Check if any hints were parsed
    pub fn is_empty(&self) -> bool {
        self.hints.is_empty()
    }

    /// Get number of hints
    pub fn len(&self) -> usize {
        self.hints.len()
    }

    /// Get all hints
    pub fn hints(&self) -> &[RoutingHint] {
        &self.hints
    }

    /// Check if route=primary is specified
    pub fn is_primary_route(&self) -> bool {
        matches!(self.route, Some(RouteTarget::Primary))
    }

    /// Check if any standby route is specified
    pub fn is_standby_route(&self) -> bool {
        matches!(
            self.route,
            Some(RouteTarget::Standby) | Some(RouteTarget::Sync) |
            Some(RouteTarget::SemiSync) | Some(RouteTarget::Async)
        )
    }

    /// Validate hint combinations
    pub fn validate(&self) -> Result<()> {
        // Check for conflicting hints
        if let (Some(RouteTarget::Async), Some(ConsistencyLevel::Strong)) =
            (self.route, self.consistency)
        {
            return Err(RoutingError::InvalidHintCombination(
                "route=async and consistency=strong are incompatible".to_string(),
            ));
        }

        // Bounded consistency requires lag specification for proper enforcement
        if self.consistency == Some(ConsistencyLevel::Bounded) && self.max_lag.is_none() {
            // Not an error, just a warning - use default lag
        }

        Ok(())
    }
}

/// Individual routing hint
#[derive(Debug, Clone, PartialEq)]
pub enum RoutingHint {
    /// Target node type
    Route(RouteTarget),

    /// Specific node by name
    Node(String),

    /// Consistency level requirement
    Consistency(ConsistencyLevel),

    /// Connection pool mode
    Pool(PoolingModeHint),

    /// Cache behavior
    Cache(CacheBehavior),

    /// Query timeout override
    Timeout(Duration),

    /// Query priority for scheduling
    Priority(QueryPriority),

    /// Maximum acceptable replication lag
    MaxLag(Duration),

    /// Retry behavior on failure
    Retry(RetryBehavior),

    /// Branch name for branch-aware routing
    Branch(String),

    /// Enable Transparent Write Routing
    TransparentWriteRouting(bool),

    /// Agent tool identifier
    AgentTool(String),

    /// Workflow step identifier
    WorkflowStep(String),

    /// Prefetch hint for context retrieval
    Prefetch(bool),

    /// Cache TTL override
    CacheTtl(Duration),
}

/// Route target types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouteTarget {
    /// Primary node (for writes or critical reads)
    Primary,
    /// Any standby (read scaling)
    Standby,
    /// Synchronous standby only
    Sync,
    /// Semi-synchronous standby
    SemiSync,
    /// Asynchronous standby (eventual consistency)
    Async,
    /// Any available node
    Any,
    /// Prefer local/closest node
    Local,
    /// Vector-optimized node
    Vector,
}

impl FromStr for RouteTarget {
    type Err = RoutingError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "primary" | "master" | "leader" => Ok(RouteTarget::Primary),
            "standby" | "replica" | "secondary" => Ok(RouteTarget::Standby),
            "sync" | "synchronous" => Ok(RouteTarget::Sync),
            "semisync" | "semi-sync" | "semi_sync" => Ok(RouteTarget::SemiSync),
            "async" | "asynchronous" => Ok(RouteTarget::Async),
            "any" | "all" => Ok(RouteTarget::Any),
            "local" | "nearest" => Ok(RouteTarget::Local),
            "vector" => Ok(RouteTarget::Vector),
            _ => Err(RoutingError::ParseError(format!("Unknown route target: {}", s))),
        }
    }
}

impl std::fmt::Display for RouteTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteTarget::Primary => write!(f, "primary"),
            RouteTarget::Standby => write!(f, "standby"),
            RouteTarget::Sync => write!(f, "sync"),
            RouteTarget::SemiSync => write!(f, "semisync"),
            RouteTarget::Async => write!(f, "async"),
            RouteTarget::Any => write!(f, "any"),
            RouteTarget::Local => write!(f, "local"),
            RouteTarget::Vector => write!(f, "vector"),
        }
    }
}

/// Consistency levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsistencyLevel {
    /// Must read from primary or sync standby
    Strong,
    /// Allow semi-sync with bounded lag
    Bounded,
    /// Allow any replica (eventual consistency)
    Eventual,
}

impl FromStr for ConsistencyLevel {
    type Err = RoutingError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "strong" | "strict" | "linearizable" => Ok(ConsistencyLevel::Strong),
            "bounded" | "session" | "read-your-writes" => Ok(ConsistencyLevel::Bounded),
            "eventual" | "weak" => Ok(ConsistencyLevel::Eventual),
            _ => Err(RoutingError::ParseError(format!("Unknown consistency level: {}", s))),
        }
    }
}

impl std::fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyLevel::Strong => write!(f, "strong"),
            ConsistencyLevel::Bounded => write!(f, "bounded"),
            ConsistencyLevel::Eventual => write!(f, "eventual"),
        }
    }
}

/// Pooling mode hint (mirrors pool::PoolingMode)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolingModeHint {
    Session,
    Transaction,
    Statement,
}

impl FromStr for PoolingModeHint {
    type Err = RoutingError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "session" => Ok(PoolingModeHint::Session),
            "transaction" | "tx" => Ok(PoolingModeHint::Transaction),
            "statement" | "stmt" | "query" => Ok(PoolingModeHint::Statement),
            _ => Err(RoutingError::ParseError(format!("Unknown pool mode: {}", s))),
        }
    }
}

#[cfg(feature = "pool-modes")]
impl From<PoolingModeHint> for PoolingMode {
    fn from(hint: PoolingModeHint) -> Self {
        match hint {
            PoolingModeHint::Session => PoolingMode::Session,
            PoolingModeHint::Transaction => PoolingMode::Transaction,
            PoolingModeHint::Statement => PoolingMode::Statement,
        }
    }
}

/// Cache behavior hints
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheBehavior {
    /// Use normal caching
    Normal,
    /// Skip cache entirely
    Skip,
    /// Refresh cache (bypass read, update on response)
    Refresh,
    /// Use semantic (L3) cache
    Semantic,
    /// Only use L1 cache
    L1Only,
    /// Only use L2 cache
    L2Only,
}

impl FromStr for CacheBehavior {
    type Err = RoutingError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "normal" | "default" => Ok(CacheBehavior::Normal),
            "skip" | "bypass" | "none" => Ok(CacheBehavior::Skip),
            "refresh" | "force" | "update" => Ok(CacheBehavior::Refresh),
            "semantic" | "l3" | "vector" => Ok(CacheBehavior::Semantic),
            "l1" | "hot" => Ok(CacheBehavior::L1Only),
            "l2" | "warm" => Ok(CacheBehavior::L2Only),
            _ => Err(RoutingError::ParseError(format!("Unknown cache behavior: {}", s))),
        }
    }
}

/// Query priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl FromStr for QueryPriority {
    type Err = RoutingError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "low" | "background" => Ok(QueryPriority::Low),
            "normal" | "default" => Ok(QueryPriority::Normal),
            "high" | "elevated" => Ok(QueryPriority::High),
            "critical" | "urgent" | "realtime" => Ok(QueryPriority::Critical),
            _ => Err(RoutingError::ParseError(format!("Unknown priority: {}", s))),
        }
    }
}

impl Default for QueryPriority {
    fn default() -> Self {
        QueryPriority::Normal
    }
}

/// Retry behavior
#[derive(Debug, Clone, PartialEq)]
pub enum RetryBehavior {
    /// No retry
    None,
    /// Automatic retry with default count
    Auto,
    /// Retry specific number of times
    Count(u32),
}

impl Default for RetryBehavior {
    fn default() -> Self {
        RetryBehavior::Auto
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:route=primary*/ SELECT * FROM users");

        assert!(!hints.is_empty());
        assert_eq!(hints.route, Some(RouteTarget::Primary));
    }

    #[test]
    fn test_parse_multiple_hints() {
        let parser = HintParser::new();
        let hints = parser.parse(
            "/*helios:route=standby,consistency=eventual,timeout=5s*/ SELECT * FROM products"
        );

        assert_eq!(hints.len(), 3);
        assert_eq!(hints.route, Some(RouteTarget::Standby));
        assert_eq!(hints.consistency, Some(ConsistencyLevel::Eventual));
        assert_eq!(hints.timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_parse_node_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:node=standby-sync-1*/ SELECT * FROM logs");

        assert_eq!(hints.node, Some("standby-sync-1".to_string()));
    }

    #[test]
    fn test_parse_lag_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:route=async,lag=10s*/ SELECT COUNT(*) FROM events");

        assert_eq!(hints.route, Some(RouteTarget::Async));
        assert_eq!(hints.max_lag, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_parse_priority_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:priority=critical*/ SELECT balance FROM accounts");

        assert_eq!(hints.priority, Some(QueryPriority::Critical));
    }

    #[test]
    fn test_parse_cache_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:cache=skip*/ SELECT now()");

        assert_eq!(hints.cache, Some(CacheBehavior::Skip));
    }

    #[test]
    fn test_parse_pool_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:pool=transaction*/ BEGIN");

        assert_eq!(hints.pool, Some(PoolingModeHint::Transaction));
    }

    #[test]
    fn test_strip_hints() {
        let parser = HintParser::new();
        let query = "/*helios:route=primary*/ SELECT * FROM users WHERE id = 1";
        let stripped = parser.strip(query);

        assert_eq!(stripped, "SELECT * FROM users WHERE id = 1");
    }

    #[test]
    fn test_strip_multiple_hints() {
        let parser = HintParser::new();
        let query = "/*helios:route=standby*/ SELECT * /*helios:cache=skip*/ FROM users";
        let stripped = parser.strip(query);

        assert_eq!(stripped, "SELECT *  FROM users");
    }

    #[test]
    fn test_validate_conflicting_hints() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:route=async,consistency=strong*/ SELECT * FROM users");

        let result = hints.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_route_target_parsing() {
        assert_eq!(RouteTarget::from_str("primary").unwrap(), RouteTarget::Primary);
        assert_eq!(RouteTarget::from_str("master").unwrap(), RouteTarget::Primary);
        assert_eq!(RouteTarget::from_str("standby").unwrap(), RouteTarget::Standby);
        assert_eq!(RouteTarget::from_str("replica").unwrap(), RouteTarget::Standby);
        assert_eq!(RouteTarget::from_str("sync").unwrap(), RouteTarget::Sync);
        assert_eq!(RouteTarget::from_str("async").unwrap(), RouteTarget::Async);
        assert_eq!(RouteTarget::from_str("local").unwrap(), RouteTarget::Local);
    }

    #[test]
    fn test_consistency_level_parsing() {
        assert_eq!(ConsistencyLevel::from_str("strong").unwrap(), ConsistencyLevel::Strong);
        assert_eq!(ConsistencyLevel::from_str("bounded").unwrap(), ConsistencyLevel::Bounded);
        assert_eq!(ConsistencyLevel::from_str("eventual").unwrap(), ConsistencyLevel::Eventual);
    }

    #[test]
    fn test_query_priority_ordering() {
        assert!(QueryPriority::Critical > QueryPriority::High);
        assert!(QueryPriority::High > QueryPriority::Normal);
        assert!(QueryPriority::Normal > QueryPriority::Low);
    }

    #[test]
    fn test_ai_workflow_hints() {
        let parser = HintParser::new();
        let hints = parser.parse(
            "/*helios:route=async,tool=knowledge_search,workflow=planning*/ SELECT content FROM docs"
        );

        assert!(!hints.is_empty());
        assert_eq!(hints.route, Some(RouteTarget::Async));

        // Check for tool and workflow hints in the list
        let has_tool = hints.hints().iter().any(|h| matches!(h, RoutingHint::AgentTool(t) if t == "knowledge_search"));
        let has_workflow = hints.hints().iter().any(|h| matches!(h, RoutingHint::WorkflowStep(w) if w == "planning"));

        assert!(has_tool);
        assert!(has_workflow);
    }

    #[test]
    fn test_branch_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:branch=analytics,route=local*/ SELECT * FROM reports");

        assert_eq!(hints.branch, Some("analytics".to_string()));
        assert_eq!(hints.route, Some(RouteTarget::Local));
    }

    #[test]
    fn test_twr_hint() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:route=sync,twr=true*/ INSERT INTO logs VALUES (1)");

        assert_eq!(hints.route, Some(RouteTarget::Sync));
        assert_eq!(hints.twr, Some(true));
    }

    #[test]
    fn test_empty_query() {
        let parser = HintParser::new();
        let hints = parser.parse("SELECT * FROM users");

        assert!(hints.is_empty());
    }

    #[test]
    fn test_extract_raw() {
        let parser = HintParser::new();
        let raw = parser.extract_raw("/*helios:route=primary*/ SELECT /*helios:cache=skip*/ 1");

        assert_eq!(raw.len(), 2);
        assert!(raw[0].contains("route=primary"));
        assert!(raw[1].contains("cache=skip"));
    }
}
