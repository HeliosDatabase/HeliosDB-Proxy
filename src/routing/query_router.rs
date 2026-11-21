//! Query Router
//!
//! Routes queries to appropriate nodes based on hints and policies.

use super::{
    FilterResult, HintParser, NodeCriteria, NodeFilter, NodeInfo, ParsedHints, Result, RouteTarget,
    RoutingConfig, RoutingError, RoutingMetrics,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// The longest word [`is_write_word`] knows (`DEALLOCATE`): a longer first
/// word is not one of them.
const LONGEST_WRITE_WORD: usize = 10;

/// Whether `word`, upper-cased, starts a write or a transaction-control
/// statement.
fn is_write_word(word: &[u8]) -> bool {
    matches!(
        word,
        b"INSERT"
            | b"UPDATE"
            | b"DELETE"
            | b"CREATE"
            | b"ALTER"
            | b"DROP"
            | b"TRUNCATE"
            | b"GRANT"
            | b"REVOKE"
            | b"MERGE"
            | b"UPSERT"
            | b"BEGIN"
            | b"START"
            | b"COMMIT"
            | b"ROLLBACK"
            | b"SAVEPOINT"
            | b"LOCK"
            | b"PREPARE"
            | b"EXECUTE"
            | b"DEALLOCATE"
    )
}

/// The first whitespace-delimited word of `b` (empty when there is none),
/// when every byte up to its end is ASCII; `None` when a non-ASCII byte comes
/// before that, where Unicode whitespace and case rules decide. Whitespace
/// is `char::is_whitespace`'s ASCII part: tab to carriage return, and space.
fn ascii_first_word(b: &[u8]) -> Option<&[u8]> {
    let is_space = |c: u8| matches!(c, b'\t'..=b'\r' | b' ');
    let start = b.iter().position(|&c| !is_space(c)).unwrap_or(b.len());
    let rest = &b[start..];
    let end = rest
        .iter()
        .position(|&c| is_space(c) || !c.is_ascii())
        .unwrap_or(rest.len());
    match rest.get(end) {
        Some(c) if !c.is_ascii() => None,
        _ => Some(&rest[..end]),
    }
}

/// Query router - routes queries to appropriate nodes
pub struct QueryRouter {
    /// Hint parser
    parser: HintParser,
    /// Node filter
    filter: NodeFilter,
    /// Available nodes
    nodes: Arc<RwLock<Vec<NodeInfo>>>,
    /// Routing metrics
    metrics: Arc<RoutingMetrics>,
    /// Configuration
    config: RoutingConfig,
    /// Round-robin counter for load balancing
    rr_counter: std::sync::atomic::AtomicU64,
}

impl QueryRouter {
    /// Create a new query router
    pub fn new(config: RoutingConfig) -> Self {
        let filter = NodeFilter::new(config.clone());

        Self {
            parser: HintParser::new(),
            filter,
            nodes: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(RoutingMetrics::new()),
            config,
            rr_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Route a query
    pub async fn route(&self, query: &str) -> RoutingDecision {
        let start = Instant::now();

        // Parse hints
        let hints = self.parser.parse(query);

        // Validate hints
        if let Err(e) = hints.validate() {
            self.metrics.record_invalid_hints();
            return RoutingDecision::error(e.to_string());
        }

        // Determine if this is a write query
        let is_write = self.is_write_query(query);

        // Build criteria from hints
        let mut criteria = if !hints.is_empty() {
            NodeCriteria::from_hints(&hints)
        } else if is_write {
            self.filter.default_criteria_for_write()
        } else {
            self.filter.default_criteria_for_read()
        };

        // For writes without explicit routing, force primary
        if is_write && criteria.route.is_none() {
            criteria.route = Some(RouteTarget::Primary);
        }

        // Get nodes and filter
        let nodes = self.nodes.read().await;
        let filter_result = self.filter.filter(&nodes, &criteria);

        // Build decision
        let decision = if filter_result.has_matches() {
            let selected = self.select_node(&filter_result);
            self.metrics
                .record_routing(criteria.route, !hints.is_empty(), start.elapsed());

            RoutingDecision {
                target_node: Some(selected.name.clone()),
                hints: hints.clone(),
                reason: RoutingReason::Routed {
                    target: criteria.route,
                    filters_applied: filter_result.reasons.clone(),
                },
                elapsed: start.elapsed(),
                is_write,
            }
        } else {
            // No matching nodes - try fallback
            let fallback = self.try_fallback(&nodes, is_write);

            if let Some(node) = fallback {
                self.metrics.record_fallback();
                RoutingDecision {
                    target_node: Some(node.name.clone()),
                    hints: hints.clone(),
                    reason: RoutingReason::Fallback {
                        original_filters: filter_result.reasons.clone(),
                    },
                    elapsed: start.elapsed(),
                    is_write,
                }
            } else {
                self.metrics.record_no_nodes();
                RoutingDecision {
                    target_node: None,
                    hints: hints.clone(),
                    reason: RoutingReason::NoNodes {
                        filters: filter_result.reasons.clone(),
                    },
                    elapsed: start.elapsed(),
                    is_write,
                }
            }
        };

        decision
    }

    /// Route with explicit hints (for use by other modules)
    pub async fn route_with_criteria(&self, criteria: &NodeCriteria) -> Result<String> {
        let nodes = self.nodes.read().await;
        let filter_result = self.filter.filter(&nodes, criteria);

        filter_result
            .require_match("routing")
            .map(|n| n.name.clone())
    }

    /// Check if query is a write operation: its first word, in any case, is
    /// one of the write and transaction-control words.
    ///
    /// The word is read without allocating when everything up to its end is
    /// ASCII. Otherwise the whole text is upper-cased by Unicode rules and
    /// split at Unicode whitespace, as it always was (`begın` upper-cases to
    /// `BEGIN`; a no-break space ends a word).
    pub fn is_write_query(&self, query: &str) -> bool {
        if !self.config.default.auto_detect_writes {
            return false;
        }
        match ascii_first_word(query.as_bytes()) {
            Some(word) => {
                let mut upper = [0u8; LONGEST_WRITE_WORD];
                let Some(upper) = upper.get_mut(..word.len()) else {
                    return false;
                };
                upper.copy_from_slice(word);
                upper.make_ascii_uppercase();
                is_write_word(upper)
            }
            None => {
                let upper = query.trim().to_uppercase();
                is_write_word(upper.split_whitespace().next().unwrap_or("").as_bytes())
            }
        }
    }

    /// Select a node from eligible nodes using load balancing
    fn select_node<'a>(&self, result: &FilterResult<'a>) -> &'a NodeInfo {
        if result.eligible.is_empty() {
            panic!("select_node called with no eligible nodes");
        }

        if result.eligible.len() == 1 {
            return result.eligible[0];
        }

        // Simple round-robin for now
        let idx = self
            .rr_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let selected_idx = (idx as usize) % result.eligible.len();
        result.eligible[selected_idx]
    }

    /// Try to find a fallback node
    fn try_fallback<'a>(&self, nodes: &'a [NodeInfo], is_write: bool) -> Option<&'a NodeInfo> {
        if is_write {
            // For writes, only primary is acceptable
            nodes
                .iter()
                .find(|n| n.role == super::node_filter::NodeRole::Primary && n.healthy)
        } else {
            // For reads, try any healthy node
            nodes.iter().find(|n| n.healthy && n.enabled)
        }
    }

    /// Strip hints from query for backend execution
    pub fn strip_hints(&self, query: &str) -> String {
        if self.config.hints.strip_hints {
            self.parser.strip(query).into_owned()
        } else {
            query.to_string()
        }
    }

    /// Parse hints from query (for external use)
    pub fn parse_hints(&self, query: &str) -> ParsedHints {
        self.parser.parse(query)
    }

    /// Add a node
    pub async fn add_node(&self, node: NodeInfo) {
        self.nodes.write().await.push(node);
    }

    /// Remove a node by name
    pub async fn remove_node(&self, name: &str) {
        self.nodes.write().await.retain(|n| n.name != name);
    }

    /// Update node state
    pub async fn update_node<F>(&self, name: &str, f: F)
    where
        F: FnOnce(&mut NodeInfo),
    {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.iter_mut().find(|n| n.name == name) {
            f(node);
        }
    }

    /// Get metrics
    pub fn metrics(&self) -> &RoutingMetrics {
        &self.metrics
    }

    /// Get configuration
    pub fn config(&self) -> &RoutingConfig {
        &self.config
    }
}

/// Routing decision result
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Target node name (None if no node available)
    pub target_node: Option<String>,
    /// Parsed hints from query
    pub hints: ParsedHints,
    /// Reason for routing decision
    pub reason: RoutingReason,
    /// Time taken to make decision
    pub elapsed: Duration,
    /// Whether this is a write query
    pub is_write: bool,
}

impl RoutingDecision {
    /// Create an error decision
    pub fn error(message: String) -> Self {
        Self {
            target_node: None,
            hints: ParsedHints::default(),
            reason: RoutingReason::Error { message },
            elapsed: Duration::ZERO,
            is_write: false,
        }
    }

    /// Check if routing succeeded
    pub fn is_success(&self) -> bool {
        self.target_node.is_some()
    }

    /// Get the target node or error
    pub fn require_target(&self) -> Result<&str> {
        self.target_node
            .as_deref()
            .ok_or_else(|| RoutingError::NoMatchingNodes(self.reason.to_string()))
    }

    /// Get a summary string
    pub fn summary(&self) -> String {
        match &self.reason {
            RoutingReason::Routed { target, .. } => {
                format!(
                    "Routed to {} ({:?}) in {:?}",
                    self.target_node.as_deref().unwrap_or("unknown"),
                    target,
                    self.elapsed
                )
            }
            RoutingReason::Fallback { .. } => {
                format!(
                    "Fallback to {} in {:?}",
                    self.target_node.as_deref().unwrap_or("unknown"),
                    self.elapsed
                )
            }
            RoutingReason::NoNodes { filters } => {
                format!("No nodes available (filters: {:?})", filters)
            }
            RoutingReason::Error { message } => {
                format!("Error: {}", message)
            }
        }
    }
}

/// Reason for routing decision
#[derive(Debug, Clone)]
pub enum RoutingReason {
    /// Successfully routed
    Routed {
        target: Option<RouteTarget>,
        filters_applied: Vec<String>,
    },
    /// Fallback used due to no matches
    Fallback { original_filters: Vec<String> },
    /// No nodes available
    NoNodes { filters: Vec<String> },
    /// Error occurred
    Error { message: String },
}

impl std::fmt::Display for RoutingReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingReason::Routed { target, .. } => {
                write!(f, "routed to {:?}", target)
            }
            RoutingReason::Fallback { .. } => {
                write!(f, "fallback")
            }
            RoutingReason::NoNodes { filters } => {
                write!(f, "no nodes ({})", filters.join(", "))
            }
            RoutingReason::Error { message } => {
                write!(f, "error: {}", message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::node_filter::SyncMode;
    use super::*;

    async fn setup_router() -> QueryRouter {
        let router = QueryRouter::new(RoutingConfig::default());

        // Add test nodes
        router.add_node(NodeInfo::primary("primary")).await;
        router
            .add_node(NodeInfo::standby("standby-sync-1", SyncMode::Sync))
            .await;
        router
            .add_node(NodeInfo::standby("standby-async-1", SyncMode::Async).with_lag(100))
            .await;
        router
            .add_node(NodeInfo::standby("standby-async-2", SyncMode::Async).with_lag(200))
            .await;

        router
    }

    #[tokio::test]
    async fn test_route_read_query() {
        let router = setup_router().await;

        let decision = router.route("SELECT * FROM users").await;

        assert!(decision.is_success());
        assert!(!decision.is_write);
    }

    #[tokio::test]
    async fn test_route_write_query() {
        let router = setup_router().await;

        let decision = router
            .route("INSERT INTO users (name) VALUES ('test')")
            .await;

        assert!(decision.is_success());
        assert!(decision.is_write);
        assert_eq!(decision.target_node.as_deref(), Some("primary"));
    }

    #[tokio::test]
    async fn test_route_with_primary_hint() {
        let router = setup_router().await;

        let decision = router
            .route("/*helios:route=primary*/ SELECT * FROM users")
            .await;

        assert!(decision.is_success());
        assert_eq!(decision.target_node.as_deref(), Some("primary"));
    }

    #[tokio::test]
    async fn test_route_with_sync_hint() {
        let router = setup_router().await;

        let decision = router
            .route("/*helios:route=sync*/ SELECT * FROM users")
            .await;

        assert!(decision.is_success());
        assert_eq!(decision.target_node.as_deref(), Some("standby-sync-1"));
    }

    #[tokio::test]
    async fn test_route_with_node_hint() {
        let router = setup_router().await;

        let decision = router
            .route("/*helios:node=standby-async-1*/ SELECT * FROM users")
            .await;

        assert!(decision.is_success());
        assert_eq!(decision.target_node.as_deref(), Some("standby-async-1"));
    }

    #[tokio::test]
    async fn test_route_with_lag_hint() {
        let router = setup_router().await;

        let decision = router
            .route("/*helios:route=async,lag=150ms*/ SELECT * FROM users")
            .await;

        assert!(decision.is_success());
        // Should only match standby-async-1 (100ms lag)
        assert_eq!(decision.target_node.as_deref(), Some("standby-async-1"));
    }

    #[tokio::test]
    async fn test_route_no_matching_nodes() {
        let router = setup_router().await;

        let decision = router
            .route("/*helios:node=nonexistent*/ SELECT * FROM users")
            .await;

        // Should fallback
        assert!(decision.is_success()); // Fallback finds a node
    }

    #[tokio::test]
    async fn test_is_write_query() {
        let router = QueryRouter::new(RoutingConfig::default());

        assert!(router.is_write_query("INSERT INTO users VALUES (1)"));
        assert!(router.is_write_query("UPDATE users SET name = 'test'"));
        assert!(router.is_write_query("DELETE FROM users"));
        assert!(router.is_write_query("CREATE TABLE test (id INT)"));
        assert!(router.is_write_query("BEGIN"));
        assert!(router.is_write_query("COMMIT"));

        assert!(!router.is_write_query("SELECT * FROM users"));
        assert!(!router.is_write_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
    }

    /// `is_write_query` as it was before it stopped allocating: the whole
    /// text upper-cased by Unicode rules, its first Unicode-whitespace word.
    fn is_write_query_by_upper_casing(query: &str) -> bool {
        let upper = query.trim().to_uppercase();
        is_write_word(upper.split_whitespace().next().unwrap_or("").as_bytes())
    }

    /// Every write word in any case, behind every kind of leading whitespace
    /// and before every kind of delimiter, plus look-alikes that Unicode
    /// case and whitespace rules decide, classifies as upper-casing the text
    /// did.
    #[test]
    fn is_write_query_matches_the_upper_casing_form() {
        const WORDS: [&str; 20] = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "ALTER",
            "DROP",
            "TRUNCATE",
            "GRANT",
            "REVOKE",
            "MERGE",
            "UPSERT",
            "BEGIN",
            "START",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "LOCK",
            "PREPARE",
            "EXECUTE",
            "DEALLOCATE",
        ];
        const OTHERS: [&str; 12] = [
            "SELECT",
            "WITH",
            "SHOW",
            "INSERTS",
            "DEALLOCATES",
            "BEGI",
            "BEGIN;",
            "",
            "b\u{131}g\u{131}n",
            "beg\u{131}n",
            "\u{130}NSERT",
            "lo\u{17f}k",
        ];
        const LEADS: [&str; 8] = [
            "",
            " ",
            "\t\n",
            "\r \u{b}\u{c}",
            "\u{a0}",
            "\u{2003} ",
            "\u{1f}",
            "\u{85}",
        ];
        const TAILS: [&str; 8] = [
            "",
            " x",
            "\tx",
            "\nx",
            "\u{b}x",
            "\u{a0}x",
            "\u{3000}x",
            "\u{e9}",
        ];
        let router = QueryRouter::new(RoutingConfig::default());
        let mixed = |word: &str| -> String {
            word.chars()
                .enumerate()
                .map(|(i, c)| {
                    if i % 2 == 0 {
                        c.to_ascii_lowercase()
                    } else {
                        c
                    }
                })
                .collect()
        };
        let (mut writes, mut reads) = (0, 0);
        for word in WORDS.iter().chain(&OTHERS) {
            for form in [word.to_string(), word.to_lowercase(), mixed(word)] {
                for lead in LEADS {
                    for tail in TAILS {
                        let query = format!("{lead}{form}{tail}");
                        let want = is_write_query_by_upper_casing(&query);
                        assert_eq!(router.is_write_query(&query), want, "{query:?}");
                        if want {
                            writes += 1;
                        } else {
                            reads += 1;
                        }
                    }
                }
            }
        }
        assert!(
            writes > 1_000 && reads > 1_000,
            "{writes} writes, {reads} reads"
        );
        for word in WORDS {
            assert!(word.len() <= LONGEST_WRITE_WORD, "{word}");
            assert!(router.is_write_query(word), "{word}");
            assert!(router.is_write_query(&format!("  {} x", word.to_lowercase())));
        }
        // Unicode rules still decide where ASCII ones cannot.
        assert!(router.is_write_query("beg\u{131}n"));
        assert!(router.is_write_query("insert\u{a0}into t values (1)"));
        assert!(!router.is_write_query("\u{1f}insert"));
    }

    #[tokio::test]
    async fn test_strip_hints() {
        let router = QueryRouter::new(RoutingConfig::default());

        let stripped = router.strip_hints("/*helios:route=primary*/ SELECT * FROM users");
        assert_eq!(stripped, "SELECT * FROM users");
    }

    #[tokio::test]
    async fn test_invalid_hint_combination() {
        let router = setup_router().await;

        let decision = router
            .route("/*helios:route=async,consistency=strong*/ SELECT * FROM users")
            .await;

        // Should return error due to invalid combination
        assert!(!decision.is_success());
    }

    #[tokio::test]
    async fn test_metrics_tracking() {
        let router = setup_router().await;

        // Make some routing decisions
        router.route("SELECT * FROM users").await;
        router
            .route("/*helios:route=primary*/ SELECT * FROM accounts")
            .await;
        router.route("INSERT INTO users VALUES (1)").await;

        let stats = router.metrics().snapshot();
        assert!(stats.total_routed >= 3);
        assert!(stats.with_hints >= 1);
    }
}
