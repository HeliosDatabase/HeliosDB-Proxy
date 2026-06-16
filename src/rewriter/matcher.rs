//! Rule Matcher
//!
//! Efficient matching of queries against rewrite rules.

use super::parser::ParsedQuery;
use super::rules::{AstPattern, QueryPattern, RewriteRule};
use regex::Regex;
use std::collections::HashMap;

/// Rule matcher for efficient query matching
pub struct RuleMatcher {
    /// Fingerprint index for fast lookup
    fingerprint_index: HashMap<u64, Vec<usize>>,

    /// Compiled regex patterns
    regex_patterns: Vec<(Regex, usize)>,

    /// Table index
    table_index: HashMap<String, Vec<usize>>,

    /// Rules that match all queries
    all_rules: Vec<usize>,

    /// AST pattern rules
    ast_rules: Vec<usize>,
}

impl RuleMatcher {
    /// Create a new matcher from rules
    pub fn new(rules: &[RewriteRule]) -> Self {
        let mut fingerprint_index: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut regex_patterns: Vec<(Regex, usize)> = Vec::new();
        let mut table_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut all_rules: Vec<usize> = Vec::new();
        let mut ast_rules: Vec<usize> = Vec::new();

        for (idx, rule) in rules.iter().enumerate() {
            if !rule.enabled {
                continue;
            }

            match &rule.pattern {
                QueryPattern::Fingerprint(fp) => {
                    fingerprint_index.entry(*fp).or_default().push(idx);
                }
                QueryPattern::Regex(pattern) => {
                    if let Ok(re) = Regex::new(pattern) {
                        regex_patterns.push((re, idx));
                    }
                }
                QueryPattern::Table(table) => {
                    table_index.entry(table.clone()).or_default().push(idx);
                }
                QueryPattern::TableAny(tables) => {
                    for table in tables {
                        table_index.entry(table.clone()).or_default().push(idx);
                    }
                }
                QueryPattern::Ast(_) => {
                    ast_rules.push(idx);
                }
                QueryPattern::All => {
                    all_rules.push(idx);
                }
            }
        }

        Self {
            fingerprint_index,
            regex_patterns,
            table_index,
            all_rules,
            ast_rules,
        }
    }

    /// Match a query against rules
    pub fn match_query<'a>(
        &self,
        parsed: &ParsedQuery,
        rules: &'a [RewriteRule],
    ) -> Vec<&'a RewriteRule> {
        let mut matched_indices: Vec<usize> = Vec::new();

        // Check fingerprint matches (fast path)
        if let Some(indices) = self.fingerprint_index.get(&parsed.fingerprint()) {
            matched_indices.extend(indices);
        }

        // Check regex matches
        for (regex, idx) in &self.regex_patterns {
            if regex.is_match(&parsed.original) {
                matched_indices.push(*idx);
            }
        }

        // Check table matches
        for table in &parsed.tables {
            if let Some(indices) = self.table_index.get(table) {
                matched_indices.extend(indices);
            }
        }

        // Check AST pattern matches
        for &idx in &self.ast_rules {
            if let Some(rule) = rules.get(idx) {
                if self.matches_ast_pattern(&rule.pattern, parsed) {
                    matched_indices.push(idx);
                }
            }
        }

        // Add all-matching rules
        matched_indices.extend(&self.all_rules);

        // Deduplicate and sort by priority
        matched_indices.sort_unstable();
        matched_indices.dedup();

        let mut matched: Vec<&RewriteRule> = matched_indices
            .into_iter()
            .filter_map(|idx| rules.get(idx))
            .filter(|r| r.enabled)
            .collect();

        // Sort by priority (highest first)
        matched.sort_by_key(|r| -r.priority);

        matched
    }

    /// Check if query matches AST pattern
    fn matches_ast_pattern(&self, pattern: &QueryPattern, parsed: &ParsedQuery) -> bool {
        match pattern {
            QueryPattern::Ast(ast_pattern) => self.matches_ast(ast_pattern, parsed),
            _ => false,
        }
    }

    /// Match AST pattern against parsed query
    fn matches_ast(&self, pattern: &AstPattern, parsed: &ParsedQuery) -> bool {
        match pattern {
            AstPattern::SelectStar => parsed.has_select_star,
            AstPattern::SelectFrom { table } => parsed.is_select && parsed.tables.contains(table),
            AstPattern::NoLimit => !parsed.has_limit,
            AstPattern::NoWhere => !parsed.has_where,
            AstPattern::Insert => parsed.is_insert,
            AstPattern::Update => parsed.is_update,
            AstPattern::Delete => parsed.is_delete,
            AstPattern::Ddl => parsed.is_ddl,
            AstPattern::NPlusOne { table } => {
                // N+1 detection: SELECT ... WHERE id = $1 in loop
                // Simplified: just check if table is accessed
                parsed.tables.contains(table) && !parsed.has_limit
            }
            AstPattern::FullTableScan => parsed.is_select && !parsed.has_where,
            AstPattern::And(patterns) => patterns.iter().all(|p| self.matches_ast(p, parsed)),
            AstPattern::Or(patterns) => patterns.iter().any(|p| self.matches_ast(p, parsed)),
        }
    }

    /// Get statistics about the matcher
    pub fn stats(&self) -> MatcherStats {
        MatcherStats {
            fingerprint_rules: self.fingerprint_index.values().map(|v| v.len()).sum(),
            regex_rules: self.regex_patterns.len(),
            table_rules: self.table_index.values().map(|v| v.len()).sum(),
            all_rules: self.all_rules.len(),
            ast_rules: self.ast_rules.len(),
        }
    }
}

/// Match result
#[derive(Debug, Clone)]
pub struct MatchResult {
    /// Matched rule IDs
    pub rule_ids: Vec<String>,

    /// Query fingerprint
    pub fingerprint: u64,

    /// Tables referenced
    pub tables: Vec<String>,
}

/// Matcher statistics
#[derive(Debug, Clone)]
pub struct MatcherStats {
    /// Number of fingerprint-indexed rules
    pub fingerprint_rules: usize,

    /// Number of regex rules
    pub regex_rules: usize,

    /// Number of table-indexed rules
    pub table_rules: usize,

    /// Number of all-matching rules
    pub all_rules: usize,

    /// Number of AST pattern rules
    pub ast_rules: usize,
}

impl MatcherStats {
    /// Total rules indexed
    pub fn total(&self) -> usize {
        self.fingerprint_rules
            + self.regex_rules
            + self.table_rules
            + self.all_rules
            + self.ast_rules
    }
}

#[cfg(test)]
mod tests {
    use super::super::rules::{RewriteRule, Transformation};
    use super::*;

    fn test_rules() -> Vec<RewriteRule> {
        vec![
            RewriteRule::build("fp_match")
                .pattern(QueryPattern::Fingerprint(12345))
                .transform(Transformation::NoOp)
                .priority(100)
                .build(),
            RewriteRule::build("regex_match")
                .pattern(QueryPattern::Regex(r"SELECT .* FROM users".to_string()))
                .transform(Transformation::NoOp)
                .priority(50)
                .build(),
            RewriteRule::build("table_match")
                .pattern(QueryPattern::Table("orders".to_string()))
                .transform(Transformation::NoOp)
                .priority(75)
                .build(),
            RewriteRule::build("all_match")
                .pattern(QueryPattern::All)
                .transform(Transformation::AddLimit(1000))
                .priority(10)
                .build(),
            RewriteRule::build("ast_match")
                .pattern(QueryPattern::Ast(AstPattern::SelectStar))
                .transform(Transformation::NoOp)
                .priority(60)
                .build(),
        ]
    }

    #[test]
    fn test_matcher_creation() {
        let rules = test_rules();
        let matcher = RuleMatcher::new(&rules);

        let stats = matcher.stats();
        assert_eq!(stats.fingerprint_rules, 1);
        assert_eq!(stats.regex_rules, 1);
        assert_eq!(stats.table_rules, 1);
        assert_eq!(stats.all_rules, 1);
        assert_eq!(stats.ast_rules, 1);
    }

    #[test]
    fn test_matcher_all_rules() {
        let rules = test_rules();
        let matcher = RuleMatcher::new(&rules);

        let parsed = ParsedQuery {
            original: "SELECT 1".to_string(),
            normalized: "SELECT ?".to_string(),
            tables: vec![],
            has_select_star: false,
            has_limit: false,
            has_where: false,
            is_select: true,
            is_insert: false,
            is_update: false,
            is_delete: false,
            is_ddl: false,
        };

        let matched = matcher.match_query(&parsed, &rules);
        assert!(matched.iter().any(|r| r.id == "all_match"));
    }

    #[test]
    fn test_matcher_regex() {
        let rules = test_rules();
        let matcher = RuleMatcher::new(&rules);

        let parsed = ParsedQuery {
            original: "SELECT id, name FROM users WHERE id = 1".to_string(),
            normalized: "SELECT id, name FROM users WHERE id = ?".to_string(),
            tables: vec!["users".to_string()],
            has_select_star: false,
            has_limit: false,
            has_where: true,
            is_select: true,
            is_insert: false,
            is_update: false,
            is_delete: false,
            is_ddl: false,
        };

        let matched = matcher.match_query(&parsed, &rules);
        assert!(matched.iter().any(|r| r.id == "regex_match"));
    }

    #[test]
    fn test_matcher_table() {
        let rules = test_rules();
        let matcher = RuleMatcher::new(&rules);

        let parsed = ParsedQuery {
            original: "SELECT * FROM orders".to_string(),
            normalized: "SELECT * FROM orders".to_string(),
            tables: vec!["orders".to_string()],
            has_select_star: true,
            has_limit: false,
            has_where: false,
            is_select: true,
            is_insert: false,
            is_update: false,
            is_delete: false,
            is_ddl: false,
        };

        let matched = matcher.match_query(&parsed, &rules);
        assert!(matched.iter().any(|r| r.id == "table_match"));
        assert!(matched.iter().any(|r| r.id == "ast_match")); // SELECT *
    }

    #[test]
    fn test_matcher_priority_ordering() {
        let rules = test_rules();
        let matcher = RuleMatcher::new(&rules);

        let parsed = ParsedQuery {
            original: "SELECT * FROM orders".to_string(),
            normalized: "SELECT * FROM orders".to_string(),
            tables: vec!["orders".to_string()],
            has_select_star: true,
            has_limit: false,
            has_where: false,
            is_select: true,
            is_insert: false,
            is_update: false,
            is_delete: false,
            is_ddl: false,
        };

        let matched = matcher.match_query(&parsed, &rules);
        // Should be ordered by priority: table_match (75), ast_match (60), all_match (10)
        assert!(matched.len() >= 3);
        assert!(matched[0].priority >= matched[1].priority);
    }
}
