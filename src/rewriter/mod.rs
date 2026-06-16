//! Query Rewriting Module
//!
//! Transparent query rewriting at the proxy layer for optimization,
//! compatibility, and security enforcement.
//!
//! # Features
//!
//! - **Pattern Matching**: Match queries by fingerprint, regex, AST, or table
//! - **Transformations**: Index hints, SELECT * expansion, LIMIT addition
//! - **Rule Engine**: Priority-based rule application
//! - **AI Safety**: Agent query limits and forbidden table enforcement
//!
//! # Architecture
//!
//! ```text
//!   Original Query → Parse → Match Rules → Apply Transformations → Rewritten Query
//!                      │         │                  │
//!                      │         │                  ├── Replace
//!                      │         │                  ├── AddIndexHint
//!                      │         ├── Fingerprint    ├── ExpandSelectStar
//!                      │         ├── Regex          ├── AddLimit
//!                      │         ├── AST            ├── AddWhereClause
//!                      │         └── Table          └── ReplaceTable
//!                      │
//!                   SQL AST
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::rewriter::{QueryRewriter, RewriteRule, QueryPattern, Transformation};
//!
//! let mut rewriter = QueryRewriter::builder()
//!     .rule(RewriteRule::build("expand_star")
//!         .pattern(QueryPattern::table("users"))
//!         .transform(Transformation::ExpandSelectStar {
//!             columns: vec!["id", "name", "email"]
//!         }))
//!     .rule(RewriteRule::build("add_limit")
//!         .pattern(QueryPattern::all())
//!         .transform(Transformation::AddLimit(1000)))
//!     .build();
//!
//! let result = rewriter.rewrite("SELECT * FROM users")?;
//! // Result: SELECT id, name, email FROM users LIMIT 1000
//! ```

pub mod config;
pub mod matcher;
pub mod metrics;
pub mod parser;
pub mod rules;
pub mod transformer;

// Re-export main types
pub use config::{RewriterConfig, RewriterConfigBuilder};
pub use matcher::{MatchResult, RuleMatcher};
pub use metrics::{RewriteMetrics, RewriteStats, RuleStats};
pub use parser::{ParsedQuery, SqlParser, SqlStatement};
pub use rules::{
    AstPattern, Condition, QueryPattern, RewriteRule, RewriteRuleBuilder, Transformation,
};
pub use transformer::{TransformError, TransformationEngine};

use parking_lot::RwLock;
use std::sync::Arc;

/// Query rewriter
///
/// Main entry point for query rewriting operations.
pub struct QueryRewriter {
    /// Configuration
    config: RewriterConfig,

    /// SQL parser
    parser: SqlParser,

    /// Rewrite rules
    rules: Arc<RwLock<Vec<RewriteRule>>>,

    /// Rule matcher
    matcher: Arc<RwLock<RuleMatcher>>,

    /// Transformation engine
    transformer: TransformationEngine,

    /// Metrics
    metrics: Arc<RewriteMetrics>,
}

impl QueryRewriter {
    /// Create a new query rewriter
    pub fn new(config: RewriterConfig) -> Self {
        let rules = Arc::new(RwLock::new(config.rules.clone()));
        let matcher = Arc::new(RwLock::new(RuleMatcher::new(&config.rules)));
        let parser = SqlParser::new();
        let transformer = TransformationEngine::new();
        let metrics = Arc::new(RewriteMetrics::new());

        Self {
            config,
            parser,
            rules,
            matcher,
            transformer,
            metrics,
        }
    }

    /// Create a builder
    pub fn builder() -> QueryRewriterBuilder {
        QueryRewriterBuilder::new()
    }

    /// Check if rewriter is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Rewrite a query
    ///
    /// Returns the rewritten query and list of applied rules.
    pub fn rewrite(&self, query: &str) -> Result<RewriteResult, RewriteError> {
        if !self.config.enabled {
            return Ok(RewriteResult::unchanged(query));
        }

        let start = std::time::Instant::now();

        // Parse query to get fingerprint
        let parsed = self.parser.parse(query)?;
        let fingerprint = parsed.fingerprint();

        // Check cache for previously rewritten query
        // (In production, add caching by fingerprint)

        // Match rules
        let rules = self.rules.read();
        let matcher = self.matcher.read();
        let matched = matcher.match_query(&parsed, &rules);

        if matched.is_empty() {
            self.metrics.record_no_match(start.elapsed());
            return Ok(RewriteResult::unchanged(query));
        }

        // Apply transformations
        let mut current_query = query.to_string();
        let mut applied_rules = Vec::new();

        for rule in matched {
            if !rule.enabled {
                continue;
            }

            // Check condition
            if let Some(ref condition) = rule.condition {
                if !self.evaluate_condition(condition, &current_query) {
                    continue;
                }
            }

            // Apply transformation
            match self.transformer.apply(&current_query, &rule.transformation) {
                Ok(rewritten) => {
                    current_query = rewritten;
                    applied_rules.push(rule.id.clone());
                    self.metrics.record_rule_match(&rule.id);
                }
                Err(e) => {
                    if self.config.log_errors {
                        eprintln!("Rewrite error for rule {}: {}", rule.id, e);
                    }
                    // Continue with other rules
                }
            }
        }

        let duration = start.elapsed();
        self.metrics
            .record_rewrite(duration, !applied_rules.is_empty());

        if applied_rules.is_empty() {
            Ok(RewriteResult::unchanged(query))
        } else {
            if self.config.log_rewrites {
                println!("Rewritten query:");
                println!("  Original: {}", query);
                println!("  Rewritten: {}", current_query);
                println!("  Rules: {:?}", applied_rules);
            }

            Ok(RewriteResult {
                original: query.to_string(),
                rewritten: current_query,
                rules_applied: applied_rules,
                fingerprint,
                duration,
            })
        }
    }

    /// Test rewrite without metrics recording
    pub fn test_rewrite(&self, query: &str) -> Result<RewriteResult, RewriteError> {
        let parsed = self.parser.parse(query)?;
        let fingerprint = parsed.fingerprint();

        let rules = self.rules.read();
        let matcher = self.matcher.read();
        let matched = matcher.match_query(&parsed, &rules);

        let mut current_query = query.to_string();
        let mut applied_rules = Vec::new();

        for rule in matched {
            if !rule.enabled {
                continue;
            }

            if let Some(ref condition) = rule.condition {
                if !self.evaluate_condition(condition, &current_query) {
                    continue;
                }
            }

            if let Ok(rewritten) = self.transformer.apply(&current_query, &rule.transformation) {
                current_query = rewritten;
                applied_rules.push(rule.id.clone());
            }
        }

        Ok(RewriteResult {
            original: query.to_string(),
            rewritten: current_query,
            rules_applied: applied_rules,
            fingerprint,
            duration: std::time::Duration::ZERO,
        })
    }

    /// Add a new rule
    pub fn add_rule(&self, rule: impl Into<RewriteRule>) {
        let mut rules = self.rules.write();
        rules.push(rule.into());

        // Rebuild matcher
        let mut matcher = self.matcher.write();
        *matcher = RuleMatcher::new(&rules);
    }

    /// Remove a rule by ID
    pub fn remove_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.rules.write();
        let initial_len = rules.len();
        rules.retain(|r| r.id != rule_id);

        if rules.len() != initial_len {
            let mut matcher = self.matcher.write();
            *matcher = RuleMatcher::new(&rules);
            true
        } else {
            false
        }
    }

    /// Update a rule
    pub fn update_rule(&self, rule_id: &str, update: impl FnOnce(&mut RewriteRule)) -> bool {
        let mut rules = self.rules.write();
        if let Some(rule) = rules.iter_mut().find(|r| r.id == rule_id) {
            update(rule);

            let mut matcher = self.matcher.write();
            *matcher = RuleMatcher::new(&rules);
            true
        } else {
            false
        }
    }

    /// Enable/disable a rule
    pub fn set_rule_enabled(&self, rule_id: &str, enabled: bool) -> bool {
        self.update_rule(rule_id, |r| r.enabled = enabled)
    }

    /// Get all rules
    pub fn get_rules(&self) -> Vec<RewriteRule> {
        self.rules.read().clone()
    }

    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<RewriteRule> {
        self.rules.read().iter().find(|r| r.id == rule_id).cloned()
    }

    /// Get statistics
    pub fn stats(&self) -> RewriteStats {
        self.metrics.stats()
    }

    /// Evaluate a condition
    fn evaluate_condition(&self, condition: &Condition, query: &str) -> bool {
        match condition {
            Condition::NoExistingLimit => !query.to_uppercase().contains("LIMIT"),
            Condition::NoExistingOrderBy => !query.to_uppercase().contains("ORDER BY"),
            Condition::HasSelectStar => {
                let upper = query.to_uppercase();
                upper.contains("SELECT *") || upper.contains("SELECT  *")
            }
            Condition::SessionVar { name: _, exists } => {
                // In production, check session variables
                // For now, always return true if exists is expected
                *exists
            }
            Condition::ClientType { client_type: _ } => {
                // In production, check client metadata
                true
            }
            Condition::TableExists { table: _ } => {
                // In production, check schema cache
                true
            }
            Condition::And(conditions) => {
                conditions.iter().all(|c| self.evaluate_condition(c, query))
            }
            Condition::Or(conditions) => {
                conditions.iter().any(|c| self.evaluate_condition(c, query))
            }
            Condition::Not(condition) => !self.evaluate_condition(condition, query),
        }
    }
}

/// Query rewriter builder
pub struct QueryRewriterBuilder {
    config: RewriterConfig,
}

impl QueryRewriterBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: RewriterConfig::default(),
        }
    }

    /// Enable the rewriter
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Log rewrites
    pub fn log_rewrites(mut self, log: bool) -> Self {
        self.config.log_rewrites = log;
        self
    }

    /// Log errors
    pub fn log_errors(mut self, log: bool) -> Self {
        self.config.log_errors = log;
        self
    }

    /// Add a rule
    pub fn rule(mut self, rule: impl Into<RewriteRule>) -> Self {
        self.config.rules.push(rule.into());
        self
    }

    /// Add multiple rules
    pub fn rules(mut self, rules: Vec<RewriteRule>) -> Self {
        self.config.rules.extend(rules);
        self
    }

    /// Enable SELECT * expansion
    pub fn expand_select_star(mut self, enabled: bool) -> Self {
        self.config.expand_select_star = enabled;
        self
    }

    /// Add default LIMIT to queries
    pub fn add_default_limit(mut self, enabled: bool) -> Self {
        self.config.add_default_limit = enabled;
        self
    }

    /// Set default LIMIT value
    pub fn default_limit(mut self, limit: u32) -> Self {
        self.config.default_limit = limit;
        self
    }

    /// Build the rewriter
    pub fn build(self) -> QueryRewriter {
        QueryRewriter::new(self.config)
    }
}

impl Default for QueryRewriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a rewrite operation
#[derive(Debug, Clone)]
pub struct RewriteResult {
    /// Original query
    pub original: String,

    /// Rewritten query (same as original if no changes)
    pub rewritten: String,

    /// IDs of rules that were applied
    pub rules_applied: Vec<String>,

    /// Query fingerprint
    pub fingerprint: u64,

    /// Time taken to rewrite
    pub duration: std::time::Duration,
}

impl RewriteResult {
    /// Create an unchanged result
    pub fn unchanged(query: &str) -> Self {
        Self {
            original: query.to_string(),
            rewritten: query.to_string(),
            rules_applied: Vec::new(),
            fingerprint: 0,
            duration: std::time::Duration::ZERO,
        }
    }

    /// Check if query was modified
    pub fn was_rewritten(&self) -> bool {
        !self.rules_applied.is_empty()
    }

    /// Get the final query (rewritten or original)
    pub fn query(&self) -> &str {
        &self.rewritten
    }
}

/// Rewrite error
#[derive(Debug, Clone)]
pub enum RewriteError {
    /// Failed to parse query
    ParseError(String),

    /// Transformation failed
    TransformError(String),

    /// Rule not found
    RuleNotFound(String),

    /// Forbidden table access
    ForbiddenTable(String),

    /// Configuration error
    ConfigError(String),
}

impl std::fmt::Display for RewriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::TransformError(msg) => write!(f, "Transform error: {}", msg),
            Self::RuleNotFound(id) => write!(f, "Rule not found: {}", id),
            Self::ForbiddenTable(table) => write!(f, "Forbidden table: {}", table),
            Self::ConfigError(msg) => write!(f, "Config error: {}", msg),
        }
    }
}

impl std::error::Error for RewriteError {}

impl From<TransformError> for RewriteError {
    fn from(e: TransformError) -> Self {
        Self::TransformError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewriter_disabled() {
        let rewriter = QueryRewriter::builder().enabled(false).build();

        let result = rewriter.rewrite("SELECT * FROM users").unwrap();
        assert!(!result.was_rewritten());
        assert_eq!(result.query(), "SELECT * FROM users");
    }

    #[test]
    fn test_rewriter_add_limit() {
        let rewriter = QueryRewriter::builder()
            .enabled(true)
            .rule(
                RewriteRule::build("add_limit")
                    .pattern(QueryPattern::All)
                    .transform(Transformation::AddLimit(100))
                    .condition(Condition::NoExistingLimit),
            )
            .build();

        let result = rewriter.rewrite("SELECT * FROM users").unwrap();
        assert!(result.was_rewritten());
        assert!(result.rewritten.contains("LIMIT 100"));
    }

    #[test]
    fn test_rewriter_skip_existing_limit() {
        let rewriter = QueryRewriter::builder()
            .enabled(true)
            .rule(
                RewriteRule::build("add_limit")
                    .pattern(QueryPattern::All)
                    .transform(Transformation::AddLimit(100))
                    .condition(Condition::NoExistingLimit),
            )
            .build();

        let result = rewriter.rewrite("SELECT * FROM users LIMIT 50").unwrap();
        assert!(!result.was_rewritten());
    }

    #[test]
    fn test_rewriter_replace_query() {
        let rewriter = QueryRewriter::builder()
            .enabled(true)
            .rule(
                RewriteRule::build("replace")
                    .pattern(QueryPattern::Fingerprint(12345))
                    .transform(Transformation::Replace("SELECT 1".to_string())),
            )
            .build();

        // This won't match because fingerprint doesn't match
        let result = rewriter.rewrite("SELECT * FROM users").unwrap();
        assert!(!result.was_rewritten());
    }

    #[test]
    fn test_add_remove_rule() {
        let rewriter = QueryRewriter::builder().enabled(true).build();

        assert!(rewriter.get_rules().is_empty());

        rewriter.add_rule(
            RewriteRule::build("test")
                .pattern(QueryPattern::All)
                .transform(Transformation::AddLimit(100)),
        );

        assert_eq!(rewriter.get_rules().len(), 1);

        assert!(rewriter.remove_rule("test"));
        assert!(rewriter.get_rules().is_empty());
    }

    #[test]
    fn test_update_rule() {
        let rewriter = QueryRewriter::builder()
            .enabled(true)
            .rule(
                RewriteRule::build("test")
                    .pattern(QueryPattern::All)
                    .transform(Transformation::AddLimit(100)),
            )
            .build();

        assert!(rewriter.get_rule("test").unwrap().enabled);

        rewriter.set_rule_enabled("test", false);

        assert!(!rewriter.get_rule("test").unwrap().enabled);
    }
}
