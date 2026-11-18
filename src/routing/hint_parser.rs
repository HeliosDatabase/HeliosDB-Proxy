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
//!
//! Hints are read only from comments outside string literals, dollar-quoted
//! literals and quoted identifiers ([`crate::sql_text`]). Text such as
//! `/*helios:route=standby*/` inside a literal is data: it is neither parsed
//! nor stripped.

use super::{parse_duration, Result, RoutingError};
use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::Cow;
use std::str::FromStr;
use std::time::Duration;

#[cfg(feature = "pool-modes")]
use crate::pool::PoolingMode;

/// Text every hint contains; a query without it has no hint to find.
const HINT_MARKER: &str = "helios:";

/// Finds [`HINT_MARKER`] in a query, built once: a forward (vectorised)
/// search, so a statement without a hint costs one fast pass, as
/// `str::contains` did.
static HINT_MARKER_FINDER: Lazy<memchr::memmem::Finder<'static>> =
    Lazy::new(|| memchr::memmem::Finder::new(HINT_MARKER.as_bytes()));

/// Compiled regex for hint parsing, applied to one comment at a time: a
/// hint is a match of it inside one comment. Block comments are matched by
/// [`block_hint`], which finds the same match without a regex search.
static HINT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/\*\s*helios:([^*]+)\*/").expect("Invalid hint regex"));

/// Key-value pair regex (an empty value is read for `branch=` only).
static KV_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(\w+)\s*=\s*([^,\s]*)").expect("Invalid key-value regex"));

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

    /// Parse all routing hints from a SQL query. Only hint comments outside
    /// string literals, dollar quotes and quoted identifiers are read.
    pub fn parse(&self, query: &str) -> ParsedHints {
        let mut hints = ParsedHints::default();

        for span in hint_spans(query) {
            // Parse key-value pairs
            for kv in KV_REGEX.captures_iter(span.body) {
                let key = kv.get(1).map(|m| m.as_str()).unwrap_or("");
                let value = kv.get(2).map(|m| m.as_str()).unwrap_or("");

                if let Some(hint) = self.parse_hint(key, value) {
                    hints.add(hint);
                }
            }
        }

        hints
    }

    /// Parse a single hint from key-value pair. A key with no value is
    /// ignored, except `branch=`, which asks for the session's own database.
    fn parse_hint(&self, key: &str, value: &str) -> Option<RoutingHint> {
        if value.is_empty() && !key.eq_ignore_ascii_case("branch") {
            return None;
        }
        match key.to_lowercase().as_str() {
            "route" => RouteTarget::from_str(value).ok().map(RoutingHint::Route),
            "node" => Some(RoutingHint::Node(value.to_string())),
            "consistency" => ConsistencyLevel::from_str(value)
                .ok()
                .map(RoutingHint::Consistency),
            "pool" => PoolingModeHint::from_str(value).ok().map(RoutingHint::Pool),
            "cache" => CacheBehavior::from_str(value).ok().map(RoutingHint::Cache),
            "timeout" => parse_duration(value).map(RoutingHint::Timeout),
            "priority" => QueryPriority::from_str(value)
                .ok()
                .map(RoutingHint::Priority),
            "lag" => parse_duration(value).map(RoutingHint::MaxLag),
            "retry" => self.parse_retry(value).map(RoutingHint::Retry),
            "branch" => Some(RoutingHint::Branch(value.to_string())),
            "twr" => value
                .parse::<bool>()
                .ok()
                .map(RoutingHint::TransparentWriteRouting),
            "tool" => Some(RoutingHint::AgentTool(value.to_string())),
            "workflow" => Some(RoutingHint::WorkflowStep(value.to_string())),
            "prefetch" => value.parse::<bool>().ok().map(RoutingHint::Prefetch),
            "cache_ttl" => value
                .parse::<u64>()
                .ok()
                .map(|s| RoutingHint::CacheTtl(Duration::from_secs(s))),
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

    /// Strip hints from query for backend execution. Removes only the hint
    /// comments [`parse`](Self::parse) reads and trims the ends, as before.
    /// Bytes inside literals and quoted identifiers are never changed. A
    /// query without a hint is returned borrowed (trimmed), with no copy.
    pub fn strip<'a>(&self, query: &'a str) -> Cow<'a, str> {
        let spans = hint_spans(query);
        if spans.is_empty() {
            return Cow::Borrowed(query.trim());
        }
        let mut out = String::with_capacity(query.len());
        let mut last = 0;
        for span in &spans {
            out.push_str(&query[last..span.start]);
            last = span.end;
        }
        out.push_str(&query[last..]);
        // Trimmed in place: one allocation.
        out.truncate(out.trim_end().len());
        let lead = out.len() - out.trim_start().len();
        out.drain(..lead);
        Cow::Owned(out)
    }

    /// Extract raw hint string from query (for logging)
    pub fn extract_raw(&self, query: &str) -> Vec<String> {
        hint_spans(query)
            .into_iter()
            .map(|span| query[span.start..span.end].to_string())
            .collect()
    }
}

/// A `/*helios:…*/` hint comment outside literals and quoted identifiers.
#[derive(Debug, Clone, Copy)]
struct HintSpan<'a> {
    /// Byte offset of the hint's first byte in the query.
    start: usize,
    /// Byte offset just past the hint's last byte in the query.
    end: usize,
    /// The text after `helios:` up to the closing `*/`.
    body: &'a str,
}

/// Every hint in `query`, in order. A hint counts only inside a comment the
/// quote-aware lexer ([`crate::sql_lex::comments`]) reads outside literals,
/// dollar quotes and quoted identifiers, so `/*helios:…*/` inside `'…'` is
/// data. A query without `helios:` is never lexed and yields no spans, and
/// nothing after its last `helios:` is read. A query without a quote or a
/// `$` has no literal or quoted identifier, and its comments are found
/// without the lexer ([`crate::sql_lex::quote_free_comments`], the same
/// comments).
fn hint_spans(query: &str) -> Vec<HintSpan<'_>> {
    // The last `helios:` (one cannot overlap another, so the iterator sees
    // every one).
    let Some(last) = HINT_MARKER_FINDER.find_iter(query.as_bytes()).last() else {
        return Vec::new();
    };
    let mut spans = Vec::new();
    if crate::sql_lex::quote_free(query) {
        push_hint_spans(&mut spans, crate::sql_lex::quote_free_comments(query, last));
    } else {
        push_hint_spans(&mut spans, crate::sql_lex::comments(query, last));
    }
    spans
}

/// Push the hints of each comment (offset, text) to `spans`: the matches of
/// [`HINT_REGEX`] in it.
fn push_hint_spans<'a>(
    spans: &mut Vec<HintSpan<'a>>,
    comments: impl Iterator<Item = (usize, &'a str)>,
) {
    for (at, comment) in comments {
        if comment.starts_with("/*") {
            if let Some(body) = block_hint(comment) {
                spans.push(HintSpan {
                    start: at,
                    end: at + comment.len(),
                    body,
                });
            }
        } else if comment.contains(HINT_MARKER) {
            for found in HINT_REGEX.find_iter(comment) {
                let hint = found.as_str();
                // The regex's group 1: past the first `helios:` (the `\s*`
                // before it holds none), up to the closing `*/`.
                if let Some(marker) = hint.find(HINT_MARKER) {
                    spans.push(HintSpan {
                        start: at + found.start(),
                        end: at + found.end(),
                        body: &hint[marker + HINT_MARKER.len()..hint.len() - 2],
                    });
                }
            }
        }
    }
}

/// The body of the hint a block comment the lexer read (`/*…*/`, with no
/// other `/*` in it, ending at its first `*/`) holds, or `None`: the match of
/// [`HINT_REGEX`] in it, which can only start at its `/*` and so is the
/// whole comment or nothing. `\s*` is Unicode whitespace in both; the body
/// (`[^*]+`) runs to the first `*`, which must close the comment.
fn block_hint(comment: &str) -> Option<&str> {
    let rest = comment
        .strip_prefix("/*")?
        .trim_start_matches(char::is_whitespace)
        .strip_prefix(HINT_MARKER)?;
    let (body, close) = rest.split_at(rest.find('*')?);
    (!body.is_empty() && close == "*/").then_some(body)
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
            Some(RouteTarget::Standby)
                | Some(RouteTarget::Sync)
                | Some(RouteTarget::SemiSync)
                | Some(RouteTarget::Async)
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
            _ => Err(RoutingError::ParseError(format!(
                "Unknown route target: {}",
                s
            ))),
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
            _ => Err(RoutingError::ParseError(format!(
                "Unknown consistency level: {}",
                s
            ))),
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
            _ => Err(RoutingError::ParseError(format!(
                "Unknown pool mode: {}",
                s
            ))),
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
            _ => Err(RoutingError::ParseError(format!(
                "Unknown cache behavior: {}",
                s
            ))),
        }
    }
}

/// Query priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum QueryPriority {
    Low = 0,
    #[default]
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

/// Retry behavior
#[derive(Debug, Clone, PartialEq, Default)]
pub enum RetryBehavior {
    /// No retry
    None,
    /// Automatic retry with default count
    #[default]
    Auto,
    /// Retry specific number of times
    Count(u32),
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
            "/*helios:route=standby,consistency=eventual,timeout=5s*/ SELECT * FROM products",
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
        assert_eq!(
            RouteTarget::from_str("primary").unwrap(),
            RouteTarget::Primary
        );
        assert_eq!(
            RouteTarget::from_str("master").unwrap(),
            RouteTarget::Primary
        );
        assert_eq!(
            RouteTarget::from_str("standby").unwrap(),
            RouteTarget::Standby
        );
        assert_eq!(
            RouteTarget::from_str("replica").unwrap(),
            RouteTarget::Standby
        );
        assert_eq!(RouteTarget::from_str("sync").unwrap(), RouteTarget::Sync);
        assert_eq!(RouteTarget::from_str("async").unwrap(), RouteTarget::Async);
        assert_eq!(RouteTarget::from_str("local").unwrap(), RouteTarget::Local);
    }

    #[test]
    fn test_consistency_level_parsing() {
        assert_eq!(
            ConsistencyLevel::from_str("strong").unwrap(),
            ConsistencyLevel::Strong
        );
        assert_eq!(
            ConsistencyLevel::from_str("bounded").unwrap(),
            ConsistencyLevel::Bounded
        );
        assert_eq!(
            ConsistencyLevel::from_str("eventual").unwrap(),
            ConsistencyLevel::Eventual
        );
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
        let has_tool = hints
            .hints()
            .iter()
            .any(|h| matches!(h, RoutingHint::AgentTool(t) if t == "knowledge_search"));
        let has_workflow = hints
            .hints()
            .iter()
            .any(|h| matches!(h, RoutingHint::WorkflowStep(w) if w == "planning"));

        assert!(has_tool);
        assert!(has_workflow);
    }

    #[test]
    fn an_empty_branch_hint_asks_for_the_sessions_own_database() {
        let parser = HintParser::new();
        let hints = parser.parse("/*helios:branch=,route=primary*/ SELECT 1");
        assert_eq!(hints.branch.as_deref(), Some(""));
        assert_eq!(hints.route, Some(RouteTarget::Primary));
        // Other keys without a value are ignored, as before.
        let hints = parser.parse("/*helios:node=*/ SELECT 1");
        assert!(hints.node.is_none());
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

    // Hints are read only from comments outside string literals, dollar
    // quotes and quoted identifiers. Text inside them is data: it is neither
    // parsed (no routing) nor stripped (no byte changes).

    #[test]
    fn hint_text_in_single_quoted_literal_is_not_parsed_or_stripped() {
        let parser = HintParser::new();
        let query = "INSERT INTO notes(body) VALUES ('see /*helios:route=standby*/ here')";

        assert!(parser.parse(query).is_empty());
        assert_eq!(parser.parse(query).route, None);
        assert_eq!(parser.strip(query), query);
        assert!(parser.extract_raw(query).is_empty());
    }

    #[test]
    fn hint_text_in_escape_string_literal_is_not_parsed_or_stripped() {
        let parser = HintParser::new();
        // A doubled quote, and a backslash-escaped quote (which the lexer
        // leaves ambiguous, so nothing after it is read).
        for query in [
            "INSERT INTO notes(body) VALUES (E'it''s /*helios:route=standby*/ here')",
            "INSERT INTO notes(body) VALUES (E'it\\'s /*helios:route=standby*/ here')",
        ] {
            assert!(parser.parse(query).is_empty(), "parsed hint in {query}");
            assert_eq!(parser.strip(query), query, "stripped {query}");
        }
    }

    #[test]
    fn hint_text_in_dollar_quoted_literal_is_not_parsed_or_stripped() {
        let parser = HintParser::new();
        for query in [
            "SELECT $$see /*helios:route=standby*/ here$$",
            "SELECT $tag$see /*helios:route=standby*/ here$tag$",
        ] {
            assert!(parser.parse(query).is_empty(), "parsed hint in {query}");
            assert_eq!(parser.strip(query), query, "stripped {query}");
        }
    }

    #[test]
    fn hint_text_in_quoted_identifier_is_not_parsed_or_stripped() {
        let parser = HintParser::new();
        let query = r#"SELECT "col/*helios:route=standby*/" FROM t"#;

        assert!(parser.parse(query).is_empty());
        assert_eq!(parser.strip(query), query);
    }

    #[test]
    fn literal_containing_comment_close_is_kept_byte_identical() {
        let parser = HintParser::new();
        let query = "SELECT 'a */ /*helios:route=standby*/ b */ FROM t";

        assert!(parser.parse(query).is_empty());
        assert_eq!(parser.strip(query), query);
    }

    #[test]
    fn real_hints_around_literal_are_parsed_and_stripped_literal_unchanged() {
        let parser = HintParser::new();
        let query = "/*helios:route=primary*/ INSERT INTO notes(body) VALUES \
                     ('see /*helios:route=standby*/ */ here') /*helios:cache=skip*/";

        let hints = parser.parse(query);
        assert_eq!(hints.route, Some(RouteTarget::Primary));
        assert_eq!(hints.cache, Some(CacheBehavior::Skip));
        assert_eq!(hints.len(), 2);
        assert_eq!(
            parser.strip(query),
            "INSERT INTO notes(body) VALUES ('see /*helios:route=standby*/ */ here')"
        );
    }

    #[test]
    fn real_hint_after_escape_string_with_doubled_quote_is_read() {
        let parser = HintParser::new();
        let query = "SELECT E'it''s /*helios:route=standby*/' , 1 /*helios:consistency=eventual*/";

        let hints = parser.parse(query);
        assert_eq!(hints.route, None);
        assert_eq!(hints.consistency, Some(ConsistencyLevel::Eventual));
        assert_eq!(
            parser.strip(query),
            "SELECT E'it''s /*helios:route=standby*/' , 1"
        );
    }

    #[test]
    fn adjacent_hint_comments_are_both_parsed_and_stripped() {
        let parser = HintParser::new();
        let query = "/*helios:route=standby*//*helios:consistency=eventual*/ SELECT 1";

        let hints = parser.parse(query);
        assert_eq!(hints.route, Some(RouteTarget::Standby));
        assert_eq!(hints.consistency, Some(ConsistencyLevel::Eventual));
        assert_eq!(parser.strip(query), "SELECT 1");
    }

    #[test]
    fn hint_inside_nested_comment_is_not_read() {
        // A `/*` before the first `*/` makes the comment ambiguous between
        // PostgreSQL (nests) and HeliosDB-Nano (does not), so nothing after
        // it is read.
        let parser = HintParser::new();
        let query = "/* note /*helios:route=primary*/ SELECT 1";

        assert!(parser.parse(query).is_empty());
        assert_eq!(parser.strip(query), query);
    }

    #[test]
    fn line_comment_hint_is_still_parsed_and_stripped() {
        let parser = HintParser::new();
        let query = "-- /*helios:route=primary*/\nSELECT 1";

        assert_eq!(parser.parse(query).route, Some(RouteTarget::Primary));
        assert_eq!(parser.strip(query), "-- \nSELECT 1");
    }

    #[test]
    fn strip_without_hint_marker_borrows_the_input() {
        let parser = HintParser::new();
        let query = "SELECT '*/ /* x' FROM t";

        let out = parser.strip(query);
        assert_eq!(&*out, query);
        assert!(std::ptr::eq(out.as_ptr(), query.as_ptr()));
    }

    #[test]
    fn strip_without_hint_marker_still_trims_whitespace() {
        let parser = HintParser::new();
        assert_eq!(parser.strip("  SELECT 1 \n"), "SELECT 1");
    }

    /// The hint reader as it was before its fast paths: every comment the
    /// lexer reads (no limit), the regex with its capture group on each.
    fn lexer_hint_spans(query: &str) -> Vec<(usize, usize, &str)> {
        if !query.contains(HINT_MARKER) {
            return Vec::new();
        }
        let mut spans = Vec::new();
        for (at, comment) in crate::sql_lex::comments(query, usize::MAX) {
            if !comment.contains(HINT_MARKER) {
                continue;
            }
            for cap in HINT_REGEX.captures_iter(comment) {
                if let (Some(whole), Some(body)) = (cap.get(0), cap.get(1)) {
                    spans.push((at + whole.start(), at + whole.end(), body.as_str()));
                }
            }
        }
        spans
    }

    /// `strip` as it was, on [`lexer_hint_spans`].
    fn lexer_strip(query: &str) -> String {
        let mut out = String::new();
        let mut last = 0;
        for (start, end, _) in lexer_hint_spans(query) {
            out.push_str(&query[last..start]);
            last = end;
        }
        out.push_str(&query[last..]);
        out.trim().to_string()
    }

    /// The fast paths (the quote-free comment scanner, the stop after the
    /// last `helios:`, the block-comment match, the in-place trim) read the
    /// same hints as the lexer and the regex on generated statements, with
    /// and without literals, quoted identifiers and dollar quotes: the same
    /// spans and bodies, so the same `parse`, `strip` and `extract_raw`.
    #[test]
    fn fast_paths_read_the_hints_the_lexer_reads() {
        let parser = HintParser::new();
        let (mut quote_free, mut lexed, mut hinted) = (0, 0, 0);
        for query in crate::sql_lex::generated_comment_texts(0xC0DE_D00D_5EED_0042, 30_000, true) {
            let fast: Vec<_> = hint_spans(&query)
                .iter()
                .map(|s| (s.start, s.end, s.body))
                .collect();
            let want = lexer_hint_spans(&query);
            assert_eq!(fast, want, "{query:?}");
            assert_eq!(parser.strip(&query), lexer_strip(&query), "{query:?}");
            assert_eq!(
                parser.extract_raw(&query),
                want.iter()
                    .map(|&(s, e, _)| query[s..e].to_string())
                    .collect::<Vec<_>>(),
                "{query:?}"
            );
            if crate::sql_lex::quote_free(&query) {
                quote_free += 1;
            } else {
                lexed += 1;
            }
            if !want.is_empty() {
                hinted += 1;
            }
        }
        assert!(
            quote_free > 5_000 && lexed > 5_000 && hinted > 2_000,
            "quote-free {quote_free}, lexed {lexed}, with hints {hinted}"
        );
    }

    #[test]
    fn block_hint_is_the_regex_match_of_a_block_comment() {
        for comment in [
            "/*helios:route=primary*/",
            "/* helios:cache=skip */",
            "/*\u{a0}\nhelios:route=standby\n*/",
            "/*helios:*/",
            "/*helios:a=1**/",
            "/*helios:a=1 * b*/",
            "/**helios:route=primary*/",
            "/* x helios:route=primary*/",
            "/*helios:route=primary /*/",
        ] {
            let want = HINT_REGEX
                .captures(comment)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str());
            assert_eq!(block_hint(comment), want, "{comment:?}");
        }
    }

    #[test]
    fn hints_after_the_last_marker_are_not_read_past_and_quotes_take_the_lexer() {
        let parser = HintParser::new();
        // The lexer reads up to the last `helios:` only; a literal after it
        // (with an unterminated quote) changes nothing.
        let query = "/*helios:route=primary*/ SELECT 'it''s', $1 FROM t WHERE a = 'x";
        assert_eq!(parser.parse(query).route, Some(RouteTarget::Primary));
        assert_eq!(
            parser.strip(query),
            "SELECT 'it''s', $1 FROM t WHERE a = 'x"
        );
        // Quote-free: hints at the start and at the end are both read.
        let query = "/*helios:route=standby*/ SELECT a - b / c FROM t -- x\n/*helios:cache=skip*/";
        let hints = parser.parse(query);
        assert_eq!(hints.route, Some(RouteTarget::Standby));
        assert_eq!(hints.cache, Some(CacheBehavior::Skip));
        assert_eq!(parser.strip(query), "SELECT a - b / c FROM t -- x");
    }
}
