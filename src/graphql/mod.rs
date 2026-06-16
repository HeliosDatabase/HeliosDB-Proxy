//! GraphQL-to-SQL Gateway
//!
//! Feature 12 of the HeliosProxy roadmap.
//!
//! This module provides a GraphQL gateway that automatically generates efficient SQL
//! queries from GraphQL requests. It includes:
//!
//! - Automatic schema introspection from database tables
//! - Efficient SQL generation with JOIN optimization
//! - N+1 query prevention via DataLoader pattern
//! - Query complexity analysis and limits
//! - Branch-aware and time-travel queries (HeliosDB-Lite integration)
//!
//! # Architecture
//!
//! ```text
//! GraphQL Query → Parse → Validate → Plan → Generate SQL → Execute → Shape Response
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::graphql::{GraphQLEngine, GraphQLConfig};
//!
//! let config = GraphQLConfig::builder()
//!     .endpoint("/graphql")
//!     .playground(true)
//!     .max_depth(10)
//!     .build();
//!
//! let engine = GraphQLEngine::new(config, db_pool).await?;
//!
//! let response = engine.execute(GraphQLRequest {
//!     query: "query { users { id name } }".to_string(),
//!     variables: None,
//!     operation_name: None,
//! }).await?;
//! ```

pub mod config;
pub mod engine;
pub mod introspector;
pub mod sql_generator;
pub mod dataloader;
pub mod resolver;
pub mod validation;
pub mod metrics;

pub use config::{GraphQLConfig, GraphQLConfigBuilder, TableConfig, RelationshipConfig};
pub use engine::{GraphQLEngine, GraphQLRequest, GraphQLResponse, GraphQLError};
pub use introspector::{SchemaIntrospector, GraphQLSchema, GraphQLType, GraphQLField};
pub use sql_generator::{SqlGenerator, SqlQuery, QueryPlan, Selection, Filter};
pub use dataloader::{DataLoader, DataLoaderConfig, BatchResult};
pub use resolver::{FieldResolver, ResolverContext, ResolverResult};
pub use validation::{QueryValidator, ValidationError, ComplexityResult};
pub use metrics::{GraphQLMetrics, QueryStats, OperationMetrics};

use std::collections::HashMap;

/// GraphQL operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// Query (read-only)
    Query,
    /// Mutation (write)
    Mutation,
    /// Subscription (real-time)
    Subscription,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Query => write!(f, "query"),
            OperationType::Mutation => write!(f, "mutation"),
            OperationType::Subscription => write!(f, "subscription"),
        }
    }
}

/// Relationship type between tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationType {
    /// One-to-one relationship
    OneToOne,
    /// One-to-many relationship
    OneToMany,
    /// Many-to-one relationship
    ManyToOne,
    /// Many-to-many relationship
    ManyToMany,
}

impl RelationType {
    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "one_to_one" | "onetoone" | "1:1" => Some(RelationType::OneToOne),
            "one_to_many" | "onetomany" | "1:n" => Some(RelationType::OneToMany),
            "many_to_one" | "manytoone" | "n:1" => Some(RelationType::ManyToOne),
            "many_to_many" | "manytomany" | "n:n" => Some(RelationType::ManyToMany),
            _ => None,
        }
    }

    /// Returns true if this relationship returns multiple records
    pub fn is_list(&self) -> bool {
        matches!(self, RelationType::OneToMany | RelationType::ManyToMany)
    }
}

/// GraphQL scalar types mapped from SQL
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GraphQLScalar {
    /// GraphQL ID type
    ID,
    /// GraphQL String type
    String,
    /// GraphQL Int type
    Int,
    /// GraphQL Float type
    Float,
    /// GraphQL Boolean type
    Boolean,
    /// GraphQL DateTime type (custom scalar)
    DateTime,
    /// GraphQL Date type (custom scalar)
    Date,
    /// GraphQL Time type (custom scalar)
    Time,
    /// GraphQL JSON type (custom scalar)
    JSON,
    /// GraphQL Decimal type (custom scalar)
    Decimal,
    /// GraphQL BigInt type (custom scalar)
    BigInt,
    /// Custom scalar type
    Custom(String),
}

impl GraphQLScalar {
    /// Convert SQL type to GraphQL scalar
    pub fn from_sql_type(sql_type: &str) -> Self {
        let lower = sql_type.to_lowercase();

        if lower.contains("serial") || lower == "uuid" {
            GraphQLScalar::ID
        } else if lower.contains("int") || lower == "smallint" {
            if lower.contains("big") {
                GraphQLScalar::BigInt
            } else {
                GraphQLScalar::Int
            }
        } else if lower.contains("float") || lower.contains("double") || lower == "real" {
            GraphQLScalar::Float
        } else if lower.contains("numeric") || lower.contains("decimal") {
            GraphQLScalar::Decimal
        } else if lower == "boolean" || lower == "bool" {
            GraphQLScalar::Boolean
        } else if lower.contains("timestamp") || lower == "datetime" {
            GraphQLScalar::DateTime
        } else if lower == "date" {
            GraphQLScalar::Date
        } else if lower == "time" {
            GraphQLScalar::Time
        } else if lower == "json" || lower == "jsonb" {
            GraphQLScalar::JSON
        } else {
            GraphQLScalar::String
        }
    }

    /// Get the GraphQL SDL representation
    pub fn to_sdl(&self) -> &str {
        match self {
            GraphQLScalar::ID => "ID",
            GraphQLScalar::String => "String",
            GraphQLScalar::Int => "Int",
            GraphQLScalar::Float => "Float",
            GraphQLScalar::Boolean => "Boolean",
            GraphQLScalar::DateTime => "DateTime",
            GraphQLScalar::Date => "Date",
            GraphQLScalar::Time => "Time",
            GraphQLScalar::JSON => "JSON",
            GraphQLScalar::Decimal => "Decimal",
            GraphQLScalar::BigInt => "BigInt",
            GraphQLScalar::Custom(name) => name,
        }
    }
}

/// Consistency level for GraphQL queries (HeliosDB integration)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsistencyLevel {
    /// Strong consistency - read from primary
    Strong,
    /// Eventual consistency - can read from replicas
    #[default]
    Eventual,
    /// Bounded staleness - read within time window
    Bounded,
}

/// Distance metric for vector searches
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Cosine similarity
    #[default]
    Cosine,
    /// Euclidean distance
    Euclidean,
    /// Dot product
    DotProduct,
}

/// Branch context for HeliosDB branch-aware queries
#[derive(Debug, Clone)]
pub struct BranchContext {
    /// Branch name
    pub name: String,
    /// As-of timestamp (for time-travel)
    pub as_of: Option<std::time::SystemTime>,
}

impl Default for BranchContext {
    fn default() -> Self {
        Self {
            name: "main".to_string(),
            as_of: None,
        }
    }
}

/// GraphQL execution context
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct ExecutionContext {
    /// User identity (if authenticated)
    pub user_id: Option<String>,
    /// Roles for authorization
    pub roles: Vec<String>,
    /// Branch context
    pub branch: BranchContext,
    /// Consistency level
    pub consistency: ConsistencyLevel,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}


impl ExecutionContext {
    /// Create a new execution context
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the user ID
    pub fn with_user(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Add a role
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    /// Set the branch
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch.name = branch.into();
        self
    }

    /// Set the as-of timestamp for time-travel
    pub fn with_as_of(mut self, timestamp: std::time::SystemTime) -> Self {
        self.branch.as_of = Some(timestamp);
        self
    }

    /// Set the consistency level
    pub fn with_consistency(mut self, level: ConsistencyLevel) -> Self {
        self.consistency = level;
        self
    }

    /// Check if the user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Check if the context is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.user_id.is_some()
    }
}

/// GraphQL error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// Parse error
    ParseError,
    /// Validation error
    ValidationError,
    /// Authorization error
    Unauthorized,
    /// Forbidden
    Forbidden,
    /// Not found
    NotFound,
    /// Internal server error
    InternalError,
    /// Query too complex
    QueryTooComplex,
    /// Rate limited
    RateLimited,
    /// Timeout
    Timeout,
}

impl ErrorCode {
    /// Get the HTTP status code equivalent
    pub fn http_status(&self) -> u16 {
        match self {
            ErrorCode::ParseError | ErrorCode::ValidationError => 400,
            ErrorCode::Unauthorized => 401,
            ErrorCode::Forbidden => 403,
            ErrorCode::NotFound => 404,
            ErrorCode::QueryTooComplex | ErrorCode::RateLimited => 429,
            ErrorCode::Timeout => 408,
            ErrorCode::InternalError => 500,
        }
    }
}

/// Convert string to PascalCase
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Convert string to camelCase
pub fn to_camel_case(s: &str) -> String {
    let pascal = to_pascal_case(s);
    let mut chars = pascal.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_lowercase().chain(chars).collect(),
    }
}

/// Convert string to snake_case
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    let mut prev_was_upper = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && !prev_was_upper {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
            prev_was_upper = true;
        } else {
            result.push(c);
            prev_was_upper = false;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relation_type_from_str() {
        assert_eq!(RelationType::from_str("one_to_one"), Some(RelationType::OneToOne));
        assert_eq!(RelationType::from_str("1:n"), Some(RelationType::OneToMany));
        assert_eq!(RelationType::from_str("n:1"), Some(RelationType::ManyToOne));
        assert_eq!(RelationType::from_str("many_to_many"), Some(RelationType::ManyToMany));
        assert_eq!(RelationType::from_str("invalid"), None);
    }

    #[test]
    fn test_relation_type_is_list() {
        assert!(!RelationType::OneToOne.is_list());
        assert!(!RelationType::ManyToOne.is_list());
        assert!(RelationType::OneToMany.is_list());
        assert!(RelationType::ManyToMany.is_list());
    }

    #[test]
    fn test_graphql_scalar_from_sql_type() {
        assert_eq!(GraphQLScalar::from_sql_type("serial"), GraphQLScalar::ID);
        assert_eq!(GraphQLScalar::from_sql_type("UUID"), GraphQLScalar::ID);
        assert_eq!(GraphQLScalar::from_sql_type("INTEGER"), GraphQLScalar::Int);
        assert_eq!(GraphQLScalar::from_sql_type("BIGINT"), GraphQLScalar::BigInt);
        assert_eq!(GraphQLScalar::from_sql_type("FLOAT"), GraphQLScalar::Float);
        assert_eq!(GraphQLScalar::from_sql_type("BOOLEAN"), GraphQLScalar::Boolean);
        assert_eq!(GraphQLScalar::from_sql_type("TIMESTAMP"), GraphQLScalar::DateTime);
        assert_eq!(GraphQLScalar::from_sql_type("JSONB"), GraphQLScalar::JSON);
        assert_eq!(GraphQLScalar::from_sql_type("VARCHAR"), GraphQLScalar::String);
    }

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("user_name"), "UserName");
        assert_eq!(to_pascal_case("users"), "Users");
        assert_eq!(to_pascal_case("post_comments"), "PostComments");
    }

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("user_name"), "userName");
        assert_eq!(to_camel_case("Users"), "users");
        assert_eq!(to_camel_case("post_comments"), "postComments");
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("UserName"), "user_name");
        assert_eq!(to_snake_case("postComments"), "post_comments");
        assert_eq!(to_snake_case("ID"), "id");
    }

    #[test]
    fn test_execution_context() {
        let ctx = ExecutionContext::new()
            .with_user("user123")
            .with_role("admin")
            .with_role("reader")
            .with_branch("development")
            .with_consistency(ConsistencyLevel::Strong);

        assert_eq!(ctx.user_id, Some("user123".to_string()));
        assert!(ctx.is_authenticated());
        assert!(ctx.has_role("admin"));
        assert!(ctx.has_role("reader"));
        assert!(!ctx.has_role("writer"));
        assert_eq!(ctx.branch.name, "development");
        assert_eq!(ctx.consistency, ConsistencyLevel::Strong);
    }

    #[test]
    fn test_error_code_http_status() {
        assert_eq!(ErrorCode::ParseError.http_status(), 400);
        assert_eq!(ErrorCode::Unauthorized.http_status(), 401);
        assert_eq!(ErrorCode::Forbidden.http_status(), 403);
        assert_eq!(ErrorCode::NotFound.http_status(), 404);
        assert_eq!(ErrorCode::InternalError.http_status(), 500);
    }
}
