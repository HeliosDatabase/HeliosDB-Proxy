//! Rewrite Rules
//!
//! Rule definitions for query rewriting.

use std::collections::HashSet;

/// A rewrite rule
#[derive(Debug, Clone)]
pub struct RewriteRule {
    /// Rule identifier
    pub id: String,

    /// Human-readable description
    pub description: String,

    /// Pattern to match
    pub pattern: QueryPattern,

    /// Transformation to apply
    pub transformation: Transformation,

    /// Condition for applying rule
    pub condition: Option<Condition>,

    /// Priority (higher = applied first)
    pub priority: i32,

    /// Enabled/disabled
    pub enabled: bool,

    /// Rule tags for grouping
    pub tags: HashSet<String>,
}

impl RewriteRule {
    /// Create a new rule
    pub fn new(id: impl Into<String>) -> RewriteRuleBuilder {
        RewriteRuleBuilder::new(id)
    }

    /// Check if rule matches query pattern
    pub fn matches(&self, fingerprint: u64, query: &str, tables: &[String]) -> bool {
        if !self.enabled {
            return false;
        }

        match &self.pattern {
            QueryPattern::Fingerprint(fp) => *fp == fingerprint,
            QueryPattern::Regex(pattern) => {
                regex::Regex::new(pattern)
                    .map(|re| re.is_match(query))
                    .unwrap_or(false)
            }
            QueryPattern::Table(table) => tables.contains(table),
            QueryPattern::TableAny(table_patterns) => {
                tables.iter().any(|t| table_patterns.contains(t))
            }
            QueryPattern::Ast(ast_pattern) => {
                // AST matching is done by the matcher
                false
            }
            QueryPattern::All => true,
        }
    }
}

/// Builder for RewriteRule
pub struct RewriteRuleBuilder {
    rule: RewriteRule,
}

impl RewriteRuleBuilder {
    /// Create a new builder
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            rule: RewriteRule {
                id: id.into(),
                description: String::new(),
                pattern: QueryPattern::All,
                transformation: Transformation::NoOp,
                condition: None,
                priority: 0,
                enabled: true,
                tags: HashSet::new(),
            },
        }
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.rule.description = desc.into();
        self
    }

    /// Set pattern
    pub fn pattern(mut self, pattern: QueryPattern) -> Self {
        self.rule.pattern = pattern;
        self
    }

    /// Set transformation
    pub fn transform(mut self, transformation: Transformation) -> Self {
        self.rule.transformation = transformation;
        self
    }

    /// Set condition
    pub fn condition(mut self, condition: Condition) -> Self {
        self.rule.condition = Some(condition);
        self
    }

    /// Set priority
    pub fn priority(mut self, priority: i32) -> Self {
        self.rule.priority = priority;
        self
    }

    /// Enable/disable
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.rule.enabled = enabled;
        self
    }

    /// Add a tag
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.rule.tags.insert(tag.into());
        self
    }

    /// Build the rule
    pub fn build(self) -> RewriteRule {
        self.rule
    }
}

impl From<RewriteRuleBuilder> for RewriteRule {
    fn from(builder: RewriteRuleBuilder) -> Self {
        builder.build()
    }
}

/// Query pattern for matching
#[derive(Debug, Clone)]
pub enum QueryPattern {
    /// Match by fingerprint hash
    Fingerprint(u64),

    /// Match by SQL pattern (regex)
    Regex(String),

    /// Match by table name
    Table(String),

    /// Match any of these tables
    TableAny(HashSet<String>),

    /// Match by AST pattern
    Ast(AstPattern),

    /// Match all queries
    All,
}

impl QueryPattern {
    /// Create a fingerprint pattern
    pub fn fingerprint(fp: u64) -> Self {
        Self::Fingerprint(fp)
    }

    /// Create a regex pattern
    pub fn regex(pattern: impl Into<String>) -> Self {
        Self::Regex(pattern.into())
    }

    /// Create a table pattern
    pub fn table(table: impl Into<String>) -> Self {
        Self::Table(table.into())
    }

    /// Create a table-any pattern
    pub fn table_any(tables: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::TableAny(tables.into_iter().map(Into::into).collect())
    }

    /// Create an AST pattern
    pub fn ast(pattern: AstPattern) -> Self {
        Self::Ast(pattern)
    }

    /// Create an all pattern
    pub fn all() -> Self {
        Self::All
    }
}

/// AST-level pattern matching
#[derive(Debug, Clone)]
pub enum AstPattern {
    /// SELECT * query
    SelectStar,

    /// SELECT with specific table
    SelectFrom { table: String },

    /// Query without LIMIT
    NoLimit,

    /// Query without WHERE
    NoWhere,

    /// INSERT statement
    Insert,

    /// UPDATE statement
    Update,

    /// DELETE statement
    Delete,

    /// DDL statement (CREATE, ALTER, DROP)
    Ddl,

    /// N+1 query pattern
    NPlusOne { table: String },

    /// Full table scan
    FullTableScan,

    /// Compound pattern
    And(Vec<AstPattern>),

    /// Any of patterns
    Or(Vec<AstPattern>),
}

impl AstPattern {
    /// Create a SELECT * pattern
    pub fn select_star() -> Self {
        Self::SelectStar
    }

    /// Create a no-limit pattern
    pub fn no_limit() -> Self {
        Self::NoLimit
    }

    /// Create a no-where pattern
    pub fn no_where() -> Self {
        Self::NoWhere
    }
}

/// Query transformation
#[derive(Debug, Clone)]
pub enum Transformation {
    /// No operation (pass through)
    NoOp,

    /// Replace entire query
    Replace(String),

    /// Add index hint
    AddIndexHint {
        table: String,
        index: String,
    },

    /// Rewrite SELECT * to specific columns
    ExpandSelectStar {
        columns: Vec<String>,
    },

    /// Add LIMIT clause
    AddLimit(u32),

    /// Add WHERE condition
    AddWhereClause(String),

    /// Append to WHERE clause with AND
    AppendWhereAnd(String),

    /// Replace table name
    ReplaceTable {
        from: String,
        to: String,
    },

    /// Add ORDER BY clause
    AddOrderBy {
        column: String,
        descending: bool,
    },

    /// Add query hint comment
    AddHint(String),

    /// Add branch routing hint
    AddBranchHint(String),

    /// Add timeout hint
    AddTimeout(std::time::Duration),

    /// Custom transformation by name
    Custom(String),

    /// Chain multiple transformations
    Chain(Vec<Transformation>),
}

impl Transformation {
    /// Create a replace transformation
    pub fn replace(query: impl Into<String>) -> Self {
        Self::Replace(query.into())
    }

    /// Create an add-limit transformation
    pub fn add_limit(limit: u32) -> Self {
        Self::AddLimit(limit)
    }

    /// Create an add-where transformation
    pub fn add_where(condition: impl Into<String>) -> Self {
        Self::AddWhereClause(condition.into())
    }

    /// Create a replace-table transformation
    pub fn replace_table(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self::ReplaceTable {
            from: from.into(),
            to: to.into(),
        }
    }

    /// Create an expand-select-star transformation
    pub fn expand_select_star(columns: Vec<impl Into<String>>) -> Self {
        Self::ExpandSelectStar {
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }

    /// Create an add-index-hint transformation
    pub fn add_index_hint(table: impl Into<String>, index: impl Into<String>) -> Self {
        Self::AddIndexHint {
            table: table.into(),
            index: index.into(),
        }
    }

    /// Create a chain transformation
    pub fn chain(transformations: Vec<Transformation>) -> Self {
        Self::Chain(transformations)
    }
}

/// Condition for rule application
#[derive(Debug, Clone)]
pub enum Condition {
    /// Query has no LIMIT clause
    NoExistingLimit,

    /// Query has no ORDER BY clause
    NoExistingOrderBy,

    /// Query has SELECT *
    HasSelectStar,

    /// Session variable check
    SessionVar {
        name: String,
        exists: bool,
    },

    /// Client type check
    ClientType {
        client_type: String,
    },

    /// Table exists in schema
    TableExists {
        table: String,
    },

    /// All conditions must match
    And(Vec<Condition>),

    /// Any condition must match
    Or(Vec<Condition>),

    /// Negate condition
    Not(Box<Condition>),
}

impl Condition {
    /// No existing LIMIT
    pub fn no_limit() -> Self {
        Self::NoExistingLimit
    }

    /// No existing ORDER BY
    pub fn no_order_by() -> Self {
        Self::NoExistingOrderBy
    }

    /// Has SELECT *
    pub fn has_select_star() -> Self {
        Self::HasSelectStar
    }

    /// Session variable exists
    pub fn session_var(name: impl Into<String>) -> Self {
        Self::SessionVar {
            name: name.into(),
            exists: true,
        }
    }

    /// Client type matches
    pub fn client_type(client_type: impl Into<String>) -> Self {
        Self::ClientType {
            client_type: client_type.into(),
        }
    }

    /// AND conditions
    pub fn and(conditions: Vec<Condition>) -> Self {
        Self::And(conditions)
    }

    /// OR conditions
    pub fn or(conditions: Vec<Condition>) -> Self {
        Self::Or(conditions)
    }

    /// NOT condition
    pub fn not(condition: Condition) -> Self {
        Self::Not(Box::new(condition))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_builder() {
        let rule = RewriteRule::new("test")
            .description("Test rule")
            .pattern(QueryPattern::All)
            .transform(Transformation::AddLimit(100))
            .priority(50)
            .tag("safety")
            .build();

        assert_eq!(rule.id, "test");
        assert_eq!(rule.description, "Test rule");
        assert_eq!(rule.priority, 50);
        assert!(rule.enabled);
        assert!(rule.tags.contains("safety"));
    }

    #[test]
    fn test_query_pattern_table() {
        let pattern = QueryPattern::table("users");

        match pattern {
            QueryPattern::Table(t) => assert_eq!(t, "users"),
            _ => panic!("Expected Table pattern"),
        }
    }

    #[test]
    fn test_transformation_chain() {
        let transform = Transformation::chain(vec![
            Transformation::AddLimit(100),
            Transformation::AddOrderBy {
                column: "id".to_string(),
                descending: true,
            },
        ]);

        match transform {
            Transformation::Chain(t) => assert_eq!(t.len(), 2),
            _ => panic!("Expected Chain"),
        }
    }

    #[test]
    fn test_condition_and() {
        let condition = Condition::and(vec![
            Condition::NoExistingLimit,
            Condition::HasSelectStar,
        ]);

        match condition {
            Condition::And(c) => assert_eq!(c.len(), 2),
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_rule_matches() {
        let rule = RewriteRule::new("test")
            .pattern(QueryPattern::Table("users".to_string()))
            .transform(Transformation::AddLimit(100))
            .build();

        assert!(rule.matches(0, "", &["users".to_string()]));
        assert!(!rule.matches(0, "", &["orders".to_string()]));
    }
}
