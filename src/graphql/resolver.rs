//! Field Resolver
//!
//! Resolves GraphQL fields to database values.

use std::collections::HashMap;
use std::sync::Arc;

use super::{GraphQLSchema, ExecutionContext};

/// Resolver context passed to field resolvers
#[derive(Debug, Clone)]
pub struct ResolverContext {
    /// Execution context
    pub execution: ExecutionContext,
    /// Parent value (for nested resolvers)
    pub parent: Option<serde_json::Value>,
    /// Field arguments
    pub arguments: HashMap<String, serde_json::Value>,
    /// Field path
    pub path: Vec<String>,
    /// Schema reference
    pub schema: Arc<GraphQLSchema>,
}

impl ResolverContext {
    /// Create a new resolver context
    pub fn new(schema: Arc<GraphQLSchema>, execution: ExecutionContext) -> Self {
        Self {
            execution,
            parent: None,
            schema,
            arguments: HashMap::new(),
            path: Vec::new(),
        }
    }

    /// Set the parent value
    pub fn with_parent(mut self, parent: serde_json::Value) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Set arguments
    pub fn with_arguments(mut self, arguments: HashMap<String, serde_json::Value>) -> Self {
        self.arguments = arguments;
        self
    }

    /// Add to path
    pub fn push_path(&mut self, segment: impl Into<String>) {
        self.path.push(segment.into());
    }

    /// Get an argument value
    pub fn arg<T: serde::de::DeserializeOwned>(&self, name: &str) -> Option<T> {
        self.arguments
            .get(name)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Get a required argument
    pub fn required_arg<T: serde::de::DeserializeOwned>(&self, name: &str) -> Result<T, ResolverError> {
        self.arg(name)
            .ok_or_else(|| ResolverError::MissingArgument(name.to_string()))
    }

    /// Get a field from the parent
    pub fn parent_field(&self, name: &str) -> Option<&serde_json::Value> {
        self.parent.as_ref()?.get(name)
    }

    /// Check if user has a role
    pub fn has_role(&self, role: &str) -> bool {
        self.execution.has_role(role)
    }

    /// Get the current user ID
    pub fn user_id(&self) -> Option<&str> {
        self.execution.user_id.as_deref()
    }
}

/// Resolver result
#[derive(Debug, Clone)]
pub enum ResolverResult {
    /// Resolved value
    Value(serde_json::Value),
    /// Null value
    Null,
    /// Error occurred
    Error(ResolverError),
    /// Deferred to DataLoader
    Deferred(String),
}

impl ResolverResult {
    /// Create a value result
    pub fn value(val: impl Into<serde_json::Value>) -> Self {
        ResolverResult::Value(val.into())
    }

    /// Create a null result
    pub fn null() -> Self {
        ResolverResult::Null
    }

    /// Create an error result
    pub fn error(err: impl Into<ResolverError>) -> Self {
        ResolverResult::Error(err.into())
    }

    /// Check if result is an error
    pub fn is_error(&self) -> bool {
        matches!(self, ResolverResult::Error(_))
    }

    /// Convert to JSON value
    pub fn to_json(self) -> serde_json::Value {
        match self {
            ResolverResult::Value(v) => v,
            ResolverResult::Null | ResolverResult::Deferred(_) => serde_json::Value::Null,
            ResolverResult::Error(_) => serde_json::Value::Null,
        }
    }
}

impl From<serde_json::Value> for ResolverResult {
    fn from(val: serde_json::Value) -> Self {
        ResolverResult::Value(val)
    }
}

impl<T: Into<serde_json::Value>> From<Option<T>> for ResolverResult {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => ResolverResult::Value(v.into()),
            None => ResolverResult::Null,
        }
    }
}

/// Resolver error
#[derive(Debug, Clone)]
pub enum ResolverError {
    /// Missing required argument
    MissingArgument(String),
    /// Invalid argument value
    InvalidArgument(String, String),
    /// Field not found
    FieldNotFound(String),
    /// Authorization failed
    Unauthorized(String),
    /// Database error
    DatabaseError(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for ResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolverError::MissingArgument(name) => write!(f, "Missing required argument: {}", name),
            ResolverError::InvalidArgument(name, msg) => {
                write!(f, "Invalid argument '{}': {}", name, msg)
            }
            ResolverError::FieldNotFound(name) => write!(f, "Field not found: {}", name),
            ResolverError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ResolverError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            ResolverError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ResolverError {}

impl From<String> for ResolverError {
    fn from(s: String) -> Self {
        ResolverError::Internal(s)
    }
}

impl From<&str> for ResolverError {
    fn from(s: &str) -> Self {
        ResolverError::Internal(s.to_string())
    }
}

/// Field resolver trait
pub trait FieldResolver: Send + Sync {
    /// Resolve the field
    fn resolve(&self, ctx: &ResolverContext) -> ResolverResult;

    /// Get the field name
    fn field_name(&self) -> &str;

    /// Get the type name this resolver belongs to
    fn type_name(&self) -> &str;
}

/// Default field resolver (extracts from parent)
#[derive(Debug)]
pub struct DefaultResolver {
    /// Type name
    type_name: String,
    /// Field name
    field_name: String,
    /// Column name in database
    column_name: String,
}

impl DefaultResolver {
    /// Create a new default resolver
    pub fn new(
        type_name: impl Into<String>,
        field_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            field_name: field_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl FieldResolver for DefaultResolver {
    fn resolve(&self, ctx: &ResolverContext) -> ResolverResult {
        match &ctx.parent {
            Some(parent) => {
                parent.get(&self.column_name).cloned().into()
            }
            None => ResolverResult::Null,
        }
    }

    fn field_name(&self) -> &str {
        &self.field_name
    }

    fn type_name(&self) -> &str {
        &self.type_name
    }
}

/// Computed field resolver
pub struct ComputedResolver<F>
where
    F: Fn(&ResolverContext) -> ResolverResult + Send + Sync,
{
    type_name: String,
    field_name: String,
    resolver_fn: F,
}

impl<F> ComputedResolver<F>
where
    F: Fn(&ResolverContext) -> ResolverResult + Send + Sync,
{
    /// Create a computed resolver
    pub fn new(
        type_name: impl Into<String>,
        field_name: impl Into<String>,
        resolver_fn: F,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            field_name: field_name.into(),
            resolver_fn,
        }
    }
}

impl<F> FieldResolver for ComputedResolver<F>
where
    F: Fn(&ResolverContext) -> ResolverResult + Send + Sync,
{
    fn resolve(&self, ctx: &ResolverContext) -> ResolverResult {
        (self.resolver_fn)(ctx)
    }

    fn field_name(&self) -> &str {
        &self.field_name
    }

    fn type_name(&self) -> &str {
        &self.type_name
    }
}

impl<F> std::fmt::Debug for ComputedResolver<F>
where
    F: Fn(&ResolverContext) -> ResolverResult + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputedResolver")
            .field("type_name", &self.type_name)
            .field("field_name", &self.field_name)
            .finish()
    }
}

/// Resolver registry
#[derive(Default)]
pub struct ResolverRegistry {
    /// Resolvers by type.field
    resolvers: HashMap<String, Arc<dyn FieldResolver>>,
}

impl std::fmt::Debug for ResolverRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolverRegistry")
            .field("resolvers_count", &self.resolvers.len())
            .finish()
    }
}

impl ResolverRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resolver
    pub fn register(&mut self, resolver: impl FieldResolver + 'static) {
        let key = format!("{}.{}", resolver.type_name(), resolver.field_name());
        self.resolvers.insert(key, Arc::new(resolver));
    }

    /// Get a resolver
    pub fn get(&self, type_name: &str, field_name: &str) -> Option<Arc<dyn FieldResolver>> {
        let key = format!("{}.{}", type_name, field_name);
        self.resolvers.get(&key).cloned()
    }

    /// Check if a resolver exists
    pub fn has(&self, type_name: &str, field_name: &str) -> bool {
        let key = format!("{}.{}", type_name, field_name);
        self.resolvers.contains_key(&key)
    }

    /// Get all resolvers for a type
    pub fn resolvers_for(&self, type_name: &str) -> Vec<Arc<dyn FieldResolver>> {
        let prefix = format!("{}.", type_name);
        self.resolvers
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }
}

/// Resolver chain for applying multiple resolvers
pub struct ResolverChain {
    /// Resolvers in order
    resolvers: Vec<Arc<dyn FieldResolver>>,
}

impl std::fmt::Debug for ResolverChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolverChain")
            .field("resolvers_count", &self.resolvers.len())
            .finish()
    }
}

impl ResolverChain {
    /// Create a new chain
    pub fn new() -> Self {
        Self {
            resolvers: Vec::new(),
        }
    }

    /// Add a resolver to the chain
    pub fn add(mut self, resolver: impl FieldResolver + 'static) -> Self {
        self.resolvers.push(Arc::new(resolver));
        self
    }

    /// Resolve through the chain
    pub fn resolve(&self, ctx: &ResolverContext) -> ResolverResult {
        for resolver in &self.resolvers {
            let result = resolver.resolve(ctx);
            if !matches!(result, ResolverResult::Null) {
                return result;
            }
        }
        ResolverResult::Null
    }
}

impl Default for ResolverChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::introspector::GraphQLSchema;

    fn create_test_context() -> ResolverContext {
        let schema = Arc::new(GraphQLSchema::new());
        let execution = ExecutionContext::default();
        ResolverContext::new(schema, execution)
    }

    #[test]
    fn test_resolver_context_args() {
        let mut args = HashMap::new();
        args.insert("limit".to_string(), serde_json::json!(10));
        args.insert("name".to_string(), serde_json::json!("test"));

        let ctx = create_test_context().with_arguments(args);

        assert_eq!(ctx.arg::<i32>("limit"), Some(10));
        assert_eq!(ctx.arg::<String>("name"), Some("test".to_string()));
        assert_eq!(ctx.arg::<i32>("missing"), None);
    }

    #[test]
    fn test_resolver_context_required_arg() {
        let mut args = HashMap::new();
        args.insert("id".to_string(), serde_json::json!("123"));

        let ctx = create_test_context().with_arguments(args);

        assert!(ctx.required_arg::<String>("id").is_ok());
        assert!(ctx.required_arg::<String>("missing").is_err());
    }

    #[test]
    fn test_resolver_context_parent() {
        let parent = serde_json::json!({
            "id": "123",
            "name": "Test"
        });

        let ctx = create_test_context().with_parent(parent);

        assert_eq!(ctx.parent_field("id"), Some(&serde_json::json!("123")));
        assert_eq!(ctx.parent_field("missing"), None);
    }

    #[test]
    fn test_default_resolver() {
        let resolver = DefaultResolver::new("User", "name", "name");

        let parent = serde_json::json!({
            "id": "123",
            "name": "John"
        });

        let ctx = create_test_context().with_parent(parent);
        let result = resolver.resolve(&ctx);

        match result {
            ResolverResult::Value(v) => assert_eq!(v, serde_json::json!("John")),
            _ => panic!("Expected value"),
        }
    }

    #[test]
    fn test_computed_resolver() {
        let resolver = ComputedResolver::new("User", "fullName", |ctx| {
            let first = ctx.parent_field("firstName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let last = ctx.parent_field("lastName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            ResolverResult::value(format!("{} {}", first, last))
        });

        let parent = serde_json::json!({
            "firstName": "John",
            "lastName": "Doe"
        });

        let ctx = create_test_context().with_parent(parent);
        let result = resolver.resolve(&ctx);

        match result {
            ResolverResult::Value(v) => assert_eq!(v, serde_json::json!("John Doe")),
            _ => panic!("Expected value"),
        }
    }

    #[test]
    fn test_resolver_registry() {
        let mut registry = ResolverRegistry::new();

        registry.register(DefaultResolver::new("User", "id", "id"));
        registry.register(DefaultResolver::new("User", "name", "name"));
        registry.register(DefaultResolver::new("Post", "title", "title"));

        assert!(registry.has("User", "id"));
        assert!(registry.has("User", "name"));
        assert!(registry.has("Post", "title"));
        assert!(!registry.has("User", "email"));

        let user_resolvers = registry.resolvers_for("User");
        assert_eq!(user_resolvers.len(), 2);
    }

    #[test]
    fn test_resolver_result_conversions() {
        let value_result: ResolverResult = serde_json::json!("test").into();
        assert!(!value_result.is_error());

        let some_result: ResolverResult = Some("test").into();
        assert!(matches!(some_result, ResolverResult::Value(_)));

        let none_result: ResolverResult = Option::<String>::None.into();
        assert!(matches!(none_result, ResolverResult::Null));
    }

    #[test]
    fn test_resolver_chain() {
        let chain = ResolverChain::new()
            .add(DefaultResolver::new("User", "displayName", "display_name"))
            .add(DefaultResolver::new("User", "displayName", "name"))
            .add(DefaultResolver::new("User", "displayName", "email"));

        // First resolver returns null, second returns value
        let parent = serde_json::json!({
            "name": "John"
        });

        let ctx = create_test_context().with_parent(parent);
        let result = chain.resolve(&ctx);

        match result {
            ResolverResult::Value(v) => assert_eq!(v, serde_json::json!("John")),
            _ => panic!("Expected value from second resolver"),
        }
    }

    #[test]
    fn test_resolver_error_display() {
        let err = ResolverError::MissingArgument("id".to_string());
        assert!(err.to_string().contains("id"));

        let err = ResolverError::Unauthorized("Not authenticated".to_string());
        assert!(err.to_string().contains("Not authenticated"));
    }

    #[test]
    fn test_resolver_context_roles() {
        let schema = Arc::new(GraphQLSchema::new());
        let execution = ExecutionContext::default()
            .with_user("user1")
            .with_role("admin");

        let ctx = ResolverContext::new(schema, execution);

        assert!(ctx.has_role("admin"));
        assert!(!ctx.has_role("superuser"));
        assert_eq!(ctx.user_id(), Some("user1"));
    }

    #[test]
    fn test_resolver_result_to_json() {
        assert_eq!(
            ResolverResult::value("test").to_json(),
            serde_json::json!("test")
        );
        assert_eq!(
            ResolverResult::null().to_json(),
            serde_json::Value::Null
        );
        assert_eq!(
            ResolverResult::error("err").to_json(),
            serde_json::Value::Null
        );
    }
}
