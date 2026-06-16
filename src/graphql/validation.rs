//! Query Validation
//!
//! Validates GraphQL queries against schema and complexity limits.

use std::sync::Arc;

use super::{
    engine::ParsedDocument, engine::ParsedSelection, ErrorCode, GraphQLConfig, GraphQLError,
    GraphQLSchema,
};

/// Query validator
#[derive(Debug)]
pub struct QueryValidator {
    /// Configuration
    config: Arc<GraphQLConfig>,
}

impl QueryValidator {
    /// Create a new validator
    pub fn new(config: Arc<GraphQLConfig>) -> Self {
        Self { config }
    }

    /// Validate a parsed document
    #[allow(clippy::result_large_err)]
    pub fn validate(
        &self,
        document: &ParsedDocument,
        schema: &GraphQLSchema,
    ) -> Result<(), GraphQLError> {
        // Check complexity
        let complexity = self.calculate_complexity(document, schema)?;
        if complexity.total > self.config.limits.max_complexity {
            return Err(GraphQLError::new(
                format!(
                    "Query complexity {} exceeds maximum allowed {}",
                    complexity.total, self.config.limits.max_complexity
                ),
                ErrorCode::QueryTooComplex,
            ));
        }

        // Check depth
        if complexity.max_depth > self.config.limits.max_depth {
            return Err(GraphQLError::new(
                format!(
                    "Query depth {} exceeds maximum allowed {}",
                    complexity.max_depth, self.config.limits.max_depth
                ),
                ErrorCode::QueryTooComplex,
            ));
        }

        // Check alias count
        if complexity.alias_count > self.config.limits.max_aliases {
            return Err(GraphQLError::new(
                format!(
                    "Query has {} aliases, maximum allowed is {}",
                    complexity.alias_count, self.config.limits.max_aliases
                ),
                ErrorCode::QueryTooComplex,
            ));
        }

        // Check root fields
        if document.selections.len() > self.config.limits.max_root_fields as usize {
            return Err(GraphQLError::new(
                format!(
                    "Query has {} root fields, maximum allowed is {}",
                    document.selections.len(),
                    self.config.limits.max_root_fields
                ),
                ErrorCode::QueryTooComplex,
            ));
        }

        // Validate fields against schema
        self.validate_fields(&document.selections, "Query", schema)?;

        Ok(())
    }

    /// Calculate query complexity
    #[allow(clippy::result_large_err)]
    pub fn calculate_complexity(
        &self,
        document: &ParsedDocument,
        _schema: &GraphQLSchema,
    ) -> Result<ComplexityResult, GraphQLError> {
        let mut result = ComplexityResult::default();

        for selection in &document.selections {
            self.calculate_selection_complexity(selection, 1, &mut result)?;
        }

        Ok(result)
    }

    /// Calculate complexity for a selection
    #[allow(clippy::result_large_err)]
    fn calculate_selection_complexity(
        &self,
        selection: &ParsedSelection,
        depth: u32,
        result: &mut ComplexityResult,
    ) -> Result<(), GraphQLError> {
        // Update max depth
        result.max_depth = result.max_depth.max(depth);

        // Base cost per field
        let mut field_cost = 1u32;

        // Count alias
        if selection.alias.is_some() {
            result.alias_count += 1;
        }

        // Multiplier for list fields with limit
        if let Some(limit) = selection.arguments.get("limit") {
            if let Some(l) = limit.as_u64() {
                field_cost = field_cost.saturating_mul(l.min(100) as u32);
            }
        }

        // Add to total
        result.total = result.total.saturating_add(field_cost);
        result.field_count += 1;

        // Recurse into nested selections
        for nested in &selection.selections {
            self.calculate_selection_complexity(nested, depth + 1, result)?;
        }

        Ok(())
    }

    /// Validate fields against schema
    #[allow(clippy::result_large_err)]
    fn validate_fields(
        &self,
        selections: &[ParsedSelection],
        type_name: &str,
        schema: &GraphQLSchema,
    ) -> Result<(), GraphQLError> {
        // Get type from schema
        let type_def = schema.get_type(type_name);

        for selection in selections {
            // Skip __typename and introspection fields
            if selection.name.starts_with("__") {
                continue;
            }

            // Check if field exists (for non-Query types)
            if type_name != "Query" && type_name != "Mutation" {
                if let Some(type_def) = type_def {
                    let field_exists = type_def.get_field(&selection.name).is_some();
                    let rel_exists = schema
                        .get_relationships_for(type_name)
                        .iter()
                        .any(|r| r.field_name == selection.name);

                    if !field_exists && !rel_exists {
                        return Err(GraphQLError::validation_error(format!(
                            "Field '{}' does not exist on type '{}'",
                            selection.name, type_name
                        )));
                    }
                }
            }

            // Validate nested selections
            if !selection.selections.is_empty() {
                // Determine the type of this field
                let nested_type = self.get_field_type(&selection.name, type_name, schema);
                if let Some(nested_type) = nested_type {
                    self.validate_fields(&selection.selections, &nested_type, schema)?;
                }
            }
        }

        Ok(())
    }

    /// Get the type of a field
    fn get_field_type(
        &self,
        field_name: &str,
        parent_type: &str,
        schema: &GraphQLSchema,
    ) -> Option<String> {
        // Check direct fields
        if let Some(type_def) = schema.get_type(parent_type) {
            if let Some(field) = type_def.get_field(field_name) {
                return Some(self.extract_type_name(&field.graphql_type));
            }
        }

        // Check relationships
        for rel in schema.get_relationships_for(parent_type) {
            if rel.field_name == field_name {
                return Some(rel.to_type.clone());
            }
        }

        // For queries, the field name is often the type name
        Some(super::to_pascal_case(field_name))
    }

    /// Extract the base type name from a field type
    fn extract_type_name(&self, field_type: &super::introspector::FieldType) -> String {
        use super::introspector::FieldType;

        match field_type {
            FieldType::Scalar(s) => s.to_sdl().to_string(),
            FieldType::Object(name) => name.clone(),
            FieldType::List(inner) => self.extract_type_name(inner),
            FieldType::NonNull(inner) => self.extract_type_name(inner),
        }
    }
}

/// Complexity calculation result
#[derive(Debug, Clone, Default)]
pub struct ComplexityResult {
    /// Total complexity score
    pub total: u32,
    /// Maximum depth reached
    pub max_depth: u32,
    /// Number of aliases used
    pub alias_count: u32,
    /// Total field count
    pub field_count: u32,
}

impl ComplexityResult {
    /// Check if complexity is within limits
    pub fn is_within_limits(&self, config: &GraphQLConfig) -> bool {
        self.total <= config.limits.max_complexity
            && self.max_depth <= config.limits.max_depth
            && self.alias_count <= config.limits.max_aliases
    }
}

/// Validation error
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error message
    pub message: String,
    /// Error locations
    pub locations: Vec<(u32, u32)>,
    /// Validation rule that failed
    pub rule: ValidationRule,
}

impl ValidationError {
    /// Create a new validation error
    pub fn new(message: impl Into<String>, rule: ValidationRule) -> Self {
        Self {
            message: message.into(),
            locations: Vec::new(),
            rule,
        }
    }

    /// Add a location
    pub fn at(mut self, line: u32, column: u32) -> Self {
        self.locations.push((line, column));
        self
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ValidationError {}

/// Validation rules
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationRule {
    /// Unknown field
    UnknownField,
    /// Unknown type
    UnknownType,
    /// Unknown argument
    UnknownArgument,
    /// Missing required argument
    MissingArgument,
    /// Invalid argument type
    InvalidArgumentType,
    /// Query too complex
    QueryTooComplex,
    /// Query too deep
    QueryTooDeep,
    /// Too many aliases
    TooManyAliases,
    /// Duplicate field
    DuplicateField,
    /// Fragment cycle
    FragmentCycle,
    /// Unknown fragment
    UnknownFragment,
    /// Invalid fragment spread
    InvalidFragmentSpread,
}

/// Rule validator trait
pub trait RuleValidator: Send + Sync {
    /// Validate the rule
    fn validate(
        &self,
        document: &ParsedDocument,
        schema: &GraphQLSchema,
    ) -> Result<(), ValidationError>;

    /// Get the rule type
    fn rule(&self) -> ValidationRule;
}

/// Unknown field validator
pub struct UnknownFieldValidator;

impl RuleValidator for UnknownFieldValidator {
    fn validate(
        &self,
        document: &ParsedDocument,
        schema: &GraphQLSchema,
    ) -> Result<(), ValidationError> {
        fn check_selections(
            selections: &[ParsedSelection],
            type_name: &str,
            schema: &GraphQLSchema,
        ) -> Result<(), ValidationError> {
            for selection in selections {
                if selection.name.starts_with("__") {
                    continue;
                }

                if type_name != "Query" && type_name != "Mutation" {
                    if let Some(type_def) = schema.get_type(type_name) {
                        if type_def.get_field(&selection.name).is_none() {
                            return Err(ValidationError::new(
                                format!(
                                    "Unknown field '{}' on type '{}'",
                                    selection.name, type_name
                                ),
                                ValidationRule::UnknownField,
                            ));
                        }
                    }
                }

                // Recurse (would need proper type resolution)
                if !selection.selections.is_empty() {
                    check_selections(&selection.selections, &selection.name, schema)?;
                }
            }
            Ok(())
        }

        check_selections(&document.selections, "Query", schema)
    }

    fn rule(&self) -> ValidationRule {
        ValidationRule::UnknownField
    }
}

/// Depth validator
pub struct DepthValidator {
    max_depth: u32,
}

impl DepthValidator {
    pub fn new(max_depth: u32) -> Self {
        Self { max_depth }
    }
}

impl RuleValidator for DepthValidator {
    fn validate(
        &self,
        document: &ParsedDocument,
        _schema: &GraphQLSchema,
    ) -> Result<(), ValidationError> {
        fn check_depth(
            selections: &[ParsedSelection],
            current_depth: u32,
            max_depth: u32,
        ) -> Result<(), ValidationError> {
            if current_depth > max_depth {
                return Err(ValidationError::new(
                    format!(
                        "Query depth {} exceeds maximum {}",
                        current_depth, max_depth
                    ),
                    ValidationRule::QueryTooDeep,
                ));
            }

            for selection in selections {
                check_depth(&selection.selections, current_depth + 1, max_depth)?;
            }

            Ok(())
        }

        check_depth(&document.selections, 1, self.max_depth)
    }

    fn rule(&self) -> ValidationRule {
        ValidationRule::QueryTooDeep
    }
}

/// Alias count validator
pub struct AliasValidator {
    max_aliases: u32,
}

impl AliasValidator {
    pub fn new(max_aliases: u32) -> Self {
        Self { max_aliases }
    }
}

impl RuleValidator for AliasValidator {
    fn validate(
        &self,
        document: &ParsedDocument,
        _schema: &GraphQLSchema,
    ) -> Result<(), ValidationError> {
        fn count_aliases(selections: &[ParsedSelection]) -> u32 {
            let mut count = 0;
            for selection in selections {
                if selection.alias.is_some() {
                    count += 1;
                }
                count += count_aliases(&selection.selections);
            }
            count
        }

        let alias_count = count_aliases(&document.selections);
        if alias_count > self.max_aliases {
            return Err(ValidationError::new(
                format!(
                    "Query has {} aliases, maximum is {}",
                    alias_count, self.max_aliases
                ),
                ValidationRule::TooManyAliases,
            ));
        }

        Ok(())
    }

    fn rule(&self) -> ValidationRule {
        ValidationRule::TooManyAliases
    }
}

/// Validation pipeline
#[derive(Default)]
pub struct ValidationPipeline {
    validators: Vec<Box<dyn RuleValidator>>,
}

impl ValidationPipeline {
    /// Create a new pipeline
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a validator
    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, validator: impl RuleValidator + 'static) -> Self {
        self.validators.push(Box::new(validator));
        self
    }

    /// Create default validation pipeline
    pub fn default_pipeline(config: &GraphQLConfig) -> Self {
        Self::new()
            .add(UnknownFieldValidator)
            .add(DepthValidator::new(config.limits.max_depth))
            .add(AliasValidator::new(config.limits.max_aliases))
    }

    /// Run all validators
    pub fn validate(
        &self,
        document: &ParsedDocument,
        schema: &GraphQLSchema,
    ) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        for validator in &self.validators {
            if let Err(e) = validator.validate(document, schema) {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::{introspector::*, OperationType};

    fn create_test_schema() -> GraphQLSchema {
        let mut schema = GraphQLSchema::new();

        let mut user_type = GraphQLType::new("User");
        user_type.add_field(GraphQLField::new(
            "id",
            FieldType::scalar(crate::graphql::GraphQLScalar::ID),
        ));
        user_type.add_field(GraphQLField::new(
            "name",
            FieldType::scalar(crate::graphql::GraphQLScalar::String),
        ));
        schema.add_type(user_type);

        schema
    }

    fn create_test_document(selections: Vec<ParsedSelection>) -> ParsedDocument {
        ParsedDocument {
            operation_type: OperationType::Query,
            operation_name: None,
            selections,
            variable_definitions: Vec::new(),
            fragments: HashMap::new(),
        }
    }

    #[test]
    fn test_complexity_calculation() {
        let config = Arc::new(GraphQLConfig::default());
        let validator = QueryValidator::new(config);
        let schema = create_test_schema();

        let document = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: None,
            arguments: HashMap::new(),
            selections: vec![
                ParsedSelection {
                    name: "id".to_string(),
                    alias: None,
                    arguments: HashMap::new(),
                    selections: vec![],
                    directives: vec![],
                },
                ParsedSelection {
                    name: "name".to_string(),
                    alias: None,
                    arguments: HashMap::new(),
                    selections: vec![],
                    directives: vec![],
                },
            ],
            directives: vec![],
        }]);

        let result = validator.calculate_complexity(&document, &schema).unwrap();

        assert_eq!(result.field_count, 3); // users, id, name
        assert_eq!(result.max_depth, 2);
        assert_eq!(result.alias_count, 0);
    }

    #[test]
    fn test_complexity_with_limit() {
        let config = Arc::new(GraphQLConfig::default());
        let validator = QueryValidator::new(config);
        let schema = create_test_schema();

        let mut args = HashMap::new();
        args.insert("limit".to_string(), serde_json::json!(10));

        let document = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: None,
            arguments: args,
            selections: vec![],
            directives: vec![],
        }]);

        let result = validator.calculate_complexity(&document, &schema).unwrap();

        // With limit of 10, complexity should be multiplied
        assert_eq!(result.total, 10);
    }

    #[test]
    fn test_alias_counting() {
        let config = Arc::new(GraphQLConfig::default());
        let validator = QueryValidator::new(config);
        let schema = create_test_schema();

        let document = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: Some("allUsers".to_string()),
            arguments: HashMap::new(),
            selections: vec![ParsedSelection {
                name: "id".to_string(),
                alias: Some("userId".to_string()),
                arguments: HashMap::new(),
                selections: vec![],
                directives: vec![],
            }],
            directives: vec![],
        }]);

        let result = validator.calculate_complexity(&document, &schema).unwrap();

        assert_eq!(result.alias_count, 2);
    }

    #[test]
    fn test_depth_validator() {
        let validator = DepthValidator::new(2);
        let schema = create_test_schema();

        // Depth 1 - should pass
        let shallow = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: None,
            arguments: HashMap::new(),
            selections: vec![],
            directives: vec![],
        }]);
        assert!(validator.validate(&shallow, &schema).is_ok());

        // Depth 3 - should fail
        let deep = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: None,
            arguments: HashMap::new(),
            selections: vec![ParsedSelection {
                name: "posts".to_string(),
                alias: None,
                arguments: HashMap::new(),
                selections: vec![ParsedSelection {
                    name: "comments".to_string(),
                    alias: None,
                    arguments: HashMap::new(),
                    selections: vec![],
                    directives: vec![],
                }],
                directives: vec![],
            }],
            directives: vec![],
        }]);
        assert!(validator.validate(&deep, &schema).is_err());
    }

    #[test]
    fn test_alias_validator() {
        let validator = AliasValidator::new(2);
        let schema = create_test_schema();

        // 2 aliases - should pass
        let within_limit = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: Some("a1".to_string()),
            arguments: HashMap::new(),
            selections: vec![ParsedSelection {
                name: "id".to_string(),
                alias: Some("a2".to_string()),
                arguments: HashMap::new(),
                selections: vec![],
                directives: vec![],
            }],
            directives: vec![],
        }]);
        assert!(validator.validate(&within_limit, &schema).is_ok());

        // 3 aliases - should fail
        let exceeds_limit = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: Some("a1".to_string()),
            arguments: HashMap::new(),
            selections: vec![
                ParsedSelection {
                    name: "id".to_string(),
                    alias: Some("a2".to_string()),
                    arguments: HashMap::new(),
                    selections: vec![],
                    directives: vec![],
                },
                ParsedSelection {
                    name: "name".to_string(),
                    alias: Some("a3".to_string()),
                    arguments: HashMap::new(),
                    selections: vec![],
                    directives: vec![],
                },
            ],
            directives: vec![],
        }]);
        assert!(validator.validate(&exceeds_limit, &schema).is_err());
    }

    #[test]
    fn test_validation_pipeline() {
        let config = GraphQLConfig::default();
        let pipeline = ValidationPipeline::default_pipeline(&config);
        let schema = create_test_schema();

        let document = create_test_document(vec![ParsedSelection {
            name: "users".to_string(),
            alias: None,
            arguments: HashMap::new(),
            selections: vec![],
            directives: vec![],
        }]);

        assert!(pipeline.validate(&document, &schema).is_ok());
    }

    #[test]
    fn test_complexity_result_within_limits() {
        let config = GraphQLConfig::default();

        let within = ComplexityResult {
            total: 100,
            max_depth: 5,
            alias_count: 2,
            field_count: 10,
        };
        assert!(within.is_within_limits(&config));

        let exceeds = ComplexityResult {
            total: 10000,
            max_depth: 5,
            alias_count: 2,
            field_count: 10,
        };
        assert!(!exceeds.is_within_limits(&config));
    }

    #[test]
    fn test_validation_error() {
        let err = ValidationError::new("Test error", ValidationRule::UnknownField)
            .at(1, 10)
            .at(2, 5);

        assert_eq!(err.message, "Test error");
        assert_eq!(err.locations.len(), 2);
        assert_eq!(err.rule, ValidationRule::UnknownField);
    }
}
