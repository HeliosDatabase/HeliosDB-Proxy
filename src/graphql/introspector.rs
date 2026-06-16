//! Schema Introspector
//!
//! Introspects database schema and generates GraphQL schema.

use std::collections::HashMap;

use super::{GraphQLScalar, RelationType, to_pascal_case, to_camel_case};

/// GraphQL schema generated from database introspection
#[derive(Debug, Clone)]
pub struct GraphQLSchema {
    /// Object types
    pub types: Vec<GraphQLType>,
    /// Query type fields
    pub queries: Vec<QueryDefinition>,
    /// Mutation type fields
    pub mutations: Vec<MutationDefinition>,
    /// Relationships between types
    pub relationships: Vec<Relationship>,
    /// Input types
    pub input_types: Vec<GraphQLInputType>,
    /// Enum types
    pub enum_types: Vec<GraphQLEnumType>,
}

impl GraphQLSchema {
    /// Create an empty schema
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
            queries: Vec::new(),
            mutations: Vec::new(),
            relationships: Vec::new(),
            input_types: Vec::new(),
            enum_types: Vec::new(),
        }
    }

    /// Add a type to the schema
    pub fn add_type(&mut self, type_def: GraphQLType) {
        self.types.push(type_def);
    }

    /// Add a query to the schema
    pub fn add_query(&mut self, query: QueryDefinition) {
        self.queries.push(query);
    }

    /// Add a mutation to the schema
    pub fn add_mutation(&mut self, mutation: MutationDefinition) {
        self.mutations.push(mutation);
    }

    /// Add a relationship
    pub fn add_relationship(&mut self, relationship: Relationship) {
        self.relationships.push(relationship);
    }

    /// Get a type by name
    pub fn get_type(&self, name: &str) -> Option<&GraphQLType> {
        self.types.iter().find(|t| t.name == name)
    }

    /// Get relationships for a type
    pub fn get_relationships_for(&self, type_name: &str) -> Vec<&Relationship> {
        self.relationships.iter()
            .filter(|r| r.from_type == type_name)
            .collect()
    }

    /// Convert to SDL (Schema Definition Language)
    pub fn to_sdl(&self) -> String {
        let mut sdl = String::new();

        // Custom scalars
        sdl.push_str("# Custom Scalars\n");
        sdl.push_str("scalar DateTime\n");
        sdl.push_str("scalar Date\n");
        sdl.push_str("scalar Time\n");
        sdl.push_str("scalar JSON\n");
        sdl.push_str("scalar Decimal\n");
        sdl.push_str("scalar BigInt\n");
        sdl.push('\n');

        // Enum types
        for enum_type in &self.enum_types {
            sdl.push_str(&format!("enum {} {{\n", enum_type.name));
            for value in &enum_type.values {
                sdl.push_str(&format!("  {}\n", value));
            }
            sdl.push_str("}\n\n");
        }

        // Object types
        for type_def in &self.types {
            if let Some(ref desc) = type_def.description {
                sdl.push_str(&format!("\"\"\"{}\"\"\"\n", desc));
            }
            sdl.push_str(&format!("type {} {{\n", type_def.name));

            for field in &type_def.fields {
                if let Some(ref desc) = field.description {
                    sdl.push_str(&format!("  \"\"\"{}\"\"\"\n", desc));
                }

                let type_str = if field.nullable {
                    field.graphql_type.to_string()
                } else {
                    format!("{}!", field.graphql_type)
                };

                sdl.push_str(&format!("  {}: {}\n", field.name, type_str));
            }

            // Add relationship fields
            for rel in self.get_relationships_for(&type_def.name) {
                let type_str = if rel.relation_type.is_list() {
                    format!("[{}!]!", rel.to_type)
                } else {
                    format!("{}!", rel.to_type)
                };
                sdl.push_str(&format!("  {}: {}\n", rel.field_name, type_str));
            }

            sdl.push_str("}\n\n");
        }

        // Input types
        for input_type in &self.input_types {
            sdl.push_str(&format!("input {} {{\n", input_type.name));
            for field in &input_type.fields {
                let type_str = if field.nullable {
                    field.graphql_type.to_string()
                } else {
                    format!("{}!", field.graphql_type)
                };
                sdl.push_str(&format!("  {}: {}\n", field.name, type_str));
            }
            sdl.push_str("}\n\n");
        }

        // Query type
        sdl.push_str("type Query {\n");
        for query in &self.queries {
            let args: Vec<String> = query.arguments.iter()
                .map(|a| {
                    let type_str = if a.nullable {
                        a.graphql_type.to_string()
                    } else {
                        format!("{}!", a.graphql_type)
                    };
                    format!("{}: {}", a.name, type_str)
                })
                .collect();

            let args_str = if args.is_empty() {
                String::new()
            } else {
                format!("({})", args.join(", "))
            };

            let return_type = if query.returns_list {
                format!("[{}!]!", query.return_type)
            } else {
                query.return_type.clone()
            };

            sdl.push_str(&format!("  {}{}: {}\n", query.name, args_str, return_type));
        }
        sdl.push_str("}\n\n");

        // Mutation type
        if !self.mutations.is_empty() {
            sdl.push_str("type Mutation {\n");
            for mutation in &self.mutations {
                let args: Vec<String> = mutation.arguments.iter()
                    .map(|a| {
                        let type_str = if a.nullable {
                            a.graphql_type.to_string()
                        } else {
                            format!("{}!", a.graphql_type)
                        };
                        format!("{}: {}", a.name, type_str)
                    })
                    .collect();

                let args_str = if args.is_empty() {
                    String::new()
                } else {
                    format!("({})", args.join(", "))
                };

                sdl.push_str(&format!("  {}{}: {}\n", mutation.name, args_str, mutation.return_type));
            }
            sdl.push_str("}\n");
        }

        sdl
    }
}

impl Default for GraphQLSchema {
    fn default() -> Self {
        Self::new()
    }
}

/// GraphQL object type
#[derive(Debug, Clone)]
pub struct GraphQLType {
    /// Type name (PascalCase)
    pub name: String,
    /// Fields
    pub fields: Vec<GraphQLField>,
    /// Description
    pub description: Option<String>,
    /// Source table name
    pub table_name: Option<String>,
}

impl GraphQLType {
    /// Create a new type
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
            description: None,
            table_name: None,
        }
    }

    /// Add a field
    pub fn add_field(&mut self, field: GraphQLField) {
        self.fields.push(field);
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set source table name
    pub fn from_table(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Get a field by name
    pub fn get_field(&self, name: &str) -> Option<&GraphQLField> {
        self.fields.iter().find(|f| f.name == name)
    }
}

/// GraphQL field
#[derive(Debug, Clone)]
pub struct GraphQLField {
    /// Field name (camelCase)
    pub name: String,
    /// Field type
    pub graphql_type: FieldType,
    /// Is nullable
    pub nullable: bool,
    /// Description
    pub description: Option<String>,
    /// Source column name
    pub column_name: Option<String>,
    /// Is deprecated
    pub deprecated: bool,
    /// Deprecation reason
    pub deprecation_reason: Option<String>,
}

impl GraphQLField {
    /// Create a new field
    pub fn new(name: impl Into<String>, graphql_type: FieldType) -> Self {
        Self {
            name: name.into(),
            graphql_type,
            nullable: true,
            description: None,
            column_name: None,
            deprecated: false,
            deprecation_reason: None,
        }
    }

    /// Set nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set source column name
    pub fn from_column(mut self, column_name: impl Into<String>) -> Self {
        self.column_name = Some(column_name.into());
        self
    }

    /// Mark as deprecated
    pub fn deprecated(mut self, reason: impl Into<String>) -> Self {
        self.deprecated = true;
        self.deprecation_reason = Some(reason.into());
        self
    }
}

/// Field type representation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// Scalar type
    Scalar(GraphQLScalar),
    /// Object type reference
    Object(String),
    /// List of type
    List(Box<FieldType>),
    /// Non-null wrapper
    NonNull(Box<FieldType>),
}

impl FieldType {
    /// Create a scalar field type
    pub fn scalar(scalar: GraphQLScalar) -> Self {
        FieldType::Scalar(scalar)
    }

    /// Create an object reference field type
    pub fn object(name: impl Into<String>) -> Self {
        FieldType::Object(name.into())
    }

    /// Create a list field type
    pub fn list(inner: FieldType) -> Self {
        FieldType::List(Box::new(inner))
    }

    /// Create a non-null field type
    pub fn non_null(inner: FieldType) -> Self {
        FieldType::NonNull(Box::new(inner))
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::Scalar(s) => write!(f, "{}", s.to_sdl()),
            FieldType::Object(name) => write!(f, "{}", name),
            FieldType::List(inner) => write!(f, "[{}]", inner),
            FieldType::NonNull(inner) => write!(f, "{}!", inner),
        }
    }
}

/// GraphQL input type
#[derive(Debug, Clone)]
pub struct GraphQLInputType {
    /// Type name
    pub name: String,
    /// Fields
    pub fields: Vec<GraphQLField>,
}

/// GraphQL enum type
#[derive(Debug, Clone)]
pub struct GraphQLEnumType {
    /// Enum name
    pub name: String,
    /// Values
    pub values: Vec<String>,
}

/// Query definition
#[derive(Debug, Clone)]
pub struct QueryDefinition {
    /// Query name
    pub name: String,
    /// Arguments
    pub arguments: Vec<ArgumentDefinition>,
    /// Return type
    pub return_type: String,
    /// Returns list
    pub returns_list: bool,
    /// Source table
    pub table_name: Option<String>,
}

impl QueryDefinition {
    /// Create a new query definition
    pub fn new(name: impl Into<String>, return_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            arguments: Vec::new(),
            return_type: return_type.into(),
            returns_list: false,
            table_name: None,
        }
    }

    /// Add an argument
    pub fn arg(mut self, arg: ArgumentDefinition) -> Self {
        self.arguments.push(arg);
        self
    }

    /// Set returns list
    pub fn returns_list(mut self, list: bool) -> Self {
        self.returns_list = list;
        self
    }

    /// Set source table
    pub fn from_table(mut self, table: impl Into<String>) -> Self {
        self.table_name = Some(table.into());
        self
    }
}

/// Mutation definition
#[derive(Debug, Clone)]
pub struct MutationDefinition {
    /// Mutation name
    pub name: String,
    /// Arguments
    pub arguments: Vec<ArgumentDefinition>,
    /// Return type
    pub return_type: String,
    /// Source table
    pub table_name: Option<String>,
    /// Mutation kind
    pub kind: MutationKind,
}

impl MutationDefinition {
    /// Create a new mutation definition
    pub fn new(name: impl Into<String>, return_type: impl Into<String>, kind: MutationKind) -> Self {
        Self {
            name: name.into(),
            arguments: Vec::new(),
            return_type: return_type.into(),
            table_name: None,
            kind,
        }
    }

    /// Add an argument
    pub fn arg(mut self, arg: ArgumentDefinition) -> Self {
        self.arguments.push(arg);
        self
    }

    /// Set source table
    pub fn from_table(mut self, table: impl Into<String>) -> Self {
        self.table_name = Some(table.into());
        self
    }
}

/// Mutation kind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    /// Create operation
    Create,
    /// Update operation
    Update,
    /// Delete operation
    Delete,
}

/// Argument definition
#[derive(Debug, Clone)]
pub struct ArgumentDefinition {
    /// Argument name
    pub name: String,
    /// Argument type
    pub graphql_type: FieldType,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default_value: Option<serde_json::Value>,
}

impl ArgumentDefinition {
    /// Create a new argument
    pub fn new(name: impl Into<String>, graphql_type: FieldType) -> Self {
        Self {
            name: name.into(),
            graphql_type,
            nullable: true,
            default_value: None,
        }
    }

    /// Set nullable
    pub fn required(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set default value
    pub fn default(mut self, value: serde_json::Value) -> Self {
        self.default_value = Some(value);
        self
    }
}

/// Relationship between types
#[derive(Debug, Clone)]
pub struct Relationship {
    /// Relationship name
    pub name: String,
    /// Source type
    pub from_type: String,
    /// Target type
    pub to_type: String,
    /// Source column
    pub from_column: String,
    /// Target column
    pub to_column: String,
    /// Relationship type
    pub relation_type: RelationType,
    /// Field name in GraphQL
    pub field_name: String,
}

impl Relationship {
    /// Create a new relationship
    pub fn new(
        name: impl Into<String>,
        from_type: impl Into<String>,
        to_type: impl Into<String>,
        relation_type: RelationType,
    ) -> Self {
        let name = name.into();
        let field_name = to_camel_case(&name);

        Self {
            name: name.clone(),
            from_type: from_type.into(),
            to_type: to_type.into(),
            from_column: "id".to_string(),
            to_column: "id".to_string(),
            relation_type,
            field_name,
        }
    }

    /// Set columns
    pub fn columns(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.from_column = from.into();
        self.to_column = to.into();
        self
    }

    /// Set field name
    pub fn field(mut self, name: impl Into<String>) -> Self {
        self.field_name = name.into();
        self
    }
}

/// Schema introspector - generates GraphQL schema from database
#[derive(Debug)]
pub struct SchemaIntrospector {
    /// Excluded tables
    excluded_tables: Vec<String>,
    /// Excluded columns by table
    excluded_columns: HashMap<String, Vec<String>>,
    /// Type name overrides
    type_names: HashMap<String, String>,
}

impl SchemaIntrospector {
    /// Create a new introspector
    pub fn new() -> Self {
        Self {
            excluded_tables: vec![
                "pg_catalog".to_string(),
                "information_schema".to_string(),
            ],
            excluded_columns: HashMap::new(),
            type_names: HashMap::new(),
        }
    }

    /// Exclude a table
    pub fn exclude_table(&mut self, table: impl Into<String>) {
        self.excluded_tables.push(table.into());
    }

    /// Exclude a column
    pub fn exclude_column(&mut self, table: impl Into<String>, column: impl Into<String>) {
        self.excluded_columns
            .entry(table.into())
            .or_default()
            .push(column.into());
    }

    /// Override type name
    pub fn set_type_name(&mut self, table: impl Into<String>, type_name: impl Into<String>) {
        self.type_names.insert(table.into(), type_name.into());
    }

    /// Build schema from table definitions
    pub fn build_schema(&self, tables: &[TableDefinition]) -> GraphQLSchema {
        let mut schema = GraphQLSchema::new();

        for table in tables {
            if self.excluded_tables.contains(&table.name) {
                continue;
            }

            // Generate type
            let type_def = self.generate_type(table);
            let type_name = type_def.name.clone();
            schema.add_type(type_def);

            // Generate queries
            schema.add_query(
                QueryDefinition::new(to_camel_case(&table.name), &type_name)
                    .arg(ArgumentDefinition::new("id", FieldType::scalar(GraphQLScalar::ID)).required())
                    .from_table(&table.name)
            );

            schema.add_query(
                QueryDefinition::new(format!("{}s", to_camel_case(&table.name)), &type_name)
                    .arg(ArgumentDefinition::new("limit", FieldType::scalar(GraphQLScalar::Int)))
                    .arg(ArgumentDefinition::new("offset", FieldType::scalar(GraphQLScalar::Int)))
                    .arg(ArgumentDefinition::new("where", FieldType::object(format!("{}Filter", type_name))))
                    .returns_list(true)
                    .from_table(&table.name)
            );

            // Generate mutations
            schema.add_mutation(
                MutationDefinition::new(format!("create{}", type_name), &type_name, MutationKind::Create)
                    .arg(ArgumentDefinition::new("input", FieldType::object(format!("Create{}Input", type_name))).required())
                    .from_table(&table.name)
            );

            schema.add_mutation(
                MutationDefinition::new(format!("update{}", type_name), &type_name, MutationKind::Update)
                    .arg(ArgumentDefinition::new("id", FieldType::scalar(GraphQLScalar::ID)).required())
                    .arg(ArgumentDefinition::new("input", FieldType::object(format!("Update{}Input", type_name))).required())
                    .from_table(&table.name)
            );

            schema.add_mutation(
                MutationDefinition::new(format!("delete{}", type_name), "Boolean".to_string(), MutationKind::Delete)
                    .arg(ArgumentDefinition::new("id", FieldType::scalar(GraphQLScalar::ID)).required())
                    .from_table(&table.name)
            );

            // Generate filter input type
            let filter_type = self.generate_filter_type(table);
            schema.input_types.push(filter_type);

            // Generate create input type
            let create_input = self.generate_create_input(table, &type_name);
            schema.input_types.push(create_input);

            // Generate update input type
            let update_input = self.generate_update_input(table, &type_name);
            schema.input_types.push(update_input);
        }

        // Generate relationships from foreign keys
        for table in tables {
            for fk in &table.foreign_keys {
                let from_type = self.get_type_name(&table.name);
                let to_type = self.get_type_name(&fk.referenced_table);

                // Many-to-one relationship
                schema.add_relationship(
                    Relationship::new(&fk.name, &from_type, &to_type, RelationType::ManyToOne)
                        .columns(&fk.column, &fk.referenced_column)
                        .field(to_camel_case(&fk.name))
                );

                // Reverse one-to-many relationship
                let reverse_name = format!("{}s", to_camel_case(&table.name));
                schema.add_relationship(
                    Relationship::new(&reverse_name, &to_type, &from_type, RelationType::OneToMany)
                        .columns(&fk.referenced_column, &fk.column)
                        .field(&reverse_name)
                );
            }
        }

        schema
    }

    /// Generate GraphQL type from table
    fn generate_type(&self, table: &TableDefinition) -> GraphQLType {
        let type_name = self.get_type_name(&table.name);
        let mut type_def = GraphQLType::new(&type_name)
            .from_table(&table.name);

        let excluded = self.excluded_columns.get(&table.name);

        for column in &table.columns {
            if let Some(excluded) = excluded {
                if excluded.contains(&column.name) {
                    continue;
                }
            }

            let scalar = GraphQLScalar::from_sql_type(&column.data_type);
            let field = GraphQLField::new(
                to_camel_case(&column.name),
                FieldType::scalar(scalar),
            )
            .nullable(column.nullable)
            .from_column(&column.name);

            type_def.add_field(field);
        }

        type_def
    }

    /// Generate filter input type
    fn generate_filter_type(&self, table: &TableDefinition) -> GraphQLInputType {
        let type_name = self.get_type_name(&table.name);
        let mut input = GraphQLInputType {
            name: format!("{}Filter", type_name),
            fields: Vec::new(),
        };

        for column in &table.columns {
            let scalar = GraphQLScalar::from_sql_type(&column.data_type);
            let filter_type_name = format!("{}Filter", scalar.to_sdl());

            input.fields.push(GraphQLField::new(
                to_camel_case(&column.name),
                FieldType::object(filter_type_name),
            ));
        }

        // Add AND/OR
        input.fields.push(GraphQLField::new(
            "AND",
            FieldType::list(FieldType::object(format!("{}Filter", type_name))),
        ));
        input.fields.push(GraphQLField::new(
            "OR",
            FieldType::list(FieldType::object(format!("{}Filter", type_name))),
        ));

        input
    }

    /// Generate create input type
    fn generate_create_input(&self, table: &TableDefinition, type_name: &str) -> GraphQLInputType {
        let mut input = GraphQLInputType {
            name: format!("Create{}Input", type_name),
            fields: Vec::new(),
        };

        for column in &table.columns {
            // Skip auto-generated columns
            if column.is_primary_key && column.data_type.to_lowercase().contains("serial") {
                continue;
            }

            let scalar = GraphQLScalar::from_sql_type(&column.data_type);
            input.fields.push(GraphQLField::new(
                to_camel_case(&column.name),
                FieldType::scalar(scalar),
            ).nullable(column.nullable || column.has_default));
        }

        input
    }

    /// Generate update input type
    fn generate_update_input(&self, table: &TableDefinition, type_name: &str) -> GraphQLInputType {
        let mut input = GraphQLInputType {
            name: format!("Update{}Input", type_name),
            fields: Vec::new(),
        };

        for column in &table.columns {
            // Skip primary key
            if column.is_primary_key {
                continue;
            }

            let scalar = GraphQLScalar::from_sql_type(&column.data_type);
            input.fields.push(GraphQLField::new(
                to_camel_case(&column.name),
                FieldType::scalar(scalar),
            ));
        }

        input
    }

    /// Get type name for a table
    fn get_type_name(&self, table_name: &str) -> String {
        self.type_names
            .get(table_name)
            .cloned()
            .unwrap_or_else(|| to_pascal_case(table_name))
    }
}

impl Default for SchemaIntrospector {
    fn default() -> Self {
        Self::new()
    }
}

/// Table definition from database
#[derive(Debug, Clone)]
pub struct TableDefinition {
    /// Table name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Columns
    pub columns: Vec<ColumnDefinition>,
    /// Foreign keys
    pub foreign_keys: Vec<ForeignKeyDefinition>,
}

impl TableDefinition {
    /// Create a new table definition
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: "public".to_string(),
            columns: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Add a column
    pub fn column(mut self, column: ColumnDefinition) -> Self {
        self.columns.push(column);
        self
    }

    /// Add a foreign key
    pub fn foreign_key(mut self, fk: ForeignKeyDefinition) -> Self {
        self.foreign_keys.push(fk);
        self
    }
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Is nullable
    pub nullable: bool,
    /// Is primary key
    pub is_primary_key: bool,
    /// Has default value
    pub has_default: bool,
}

impl ColumnDefinition {
    /// Create a new column definition
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable: true,
            is_primary_key: false,
            has_default: false,
        }
    }

    /// Set nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Mark as primary key
    pub fn primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self.nullable = false;
        self
    }

    /// Mark as having default
    pub fn with_default(mut self) -> Self {
        self.has_default = true;
        self
    }
}

/// Foreign key definition
#[derive(Debug, Clone)]
pub struct ForeignKeyDefinition {
    /// Constraint name
    pub name: String,
    /// Source column
    pub column: String,
    /// Referenced table
    pub referenced_table: String,
    /// Referenced column
    pub referenced_column: String,
}

impl ForeignKeyDefinition {
    /// Create a new foreign key
    pub fn new(
        name: impl Into<String>,
        column: impl Into<String>,
        referenced_table: impl Into<String>,
        referenced_column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            column: column.into(),
            referenced_table: referenced_table.into(),
            referenced_column: referenced_column.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_tables() -> Vec<TableDefinition> {
        vec![
            TableDefinition::new("users")
                .column(ColumnDefinition::new("id", "serial").primary_key())
                .column(ColumnDefinition::new("name", "varchar(255)").nullable(false))
                .column(ColumnDefinition::new("email", "varchar(255)").nullable(false))
                .column(ColumnDefinition::new("created_at", "timestamp").with_default()),
            TableDefinition::new("posts")
                .column(ColumnDefinition::new("id", "serial").primary_key())
                .column(ColumnDefinition::new("title", "varchar(255)").nullable(false))
                .column(ColumnDefinition::new("content", "text"))
                .column(ColumnDefinition::new("user_id", "integer").nullable(false))
                .foreign_key(ForeignKeyDefinition::new("author", "user_id", "users", "id")),
        ]
    }

    #[test]
    fn test_introspector_build_schema() {
        let introspector = SchemaIntrospector::new();
        let tables = create_test_tables();
        let schema = introspector.build_schema(&tables);

        assert_eq!(schema.types.len(), 2);
        assert!(schema.get_type("Users").is_some());
        assert!(schema.get_type("Posts").is_some());
    }

    #[test]
    fn test_schema_to_sdl() {
        let introspector = SchemaIntrospector::new();
        let tables = create_test_tables();
        let schema = introspector.build_schema(&tables);

        let sdl = schema.to_sdl();

        assert!(sdl.contains("type Users"));
        assert!(sdl.contains("type Posts"));
        assert!(sdl.contains("type Query"));
        assert!(sdl.contains("type Mutation"));
    }

    #[test]
    fn test_type_generation() {
        let introspector = SchemaIntrospector::new();
        let table = TableDefinition::new("users")
            .column(ColumnDefinition::new("id", "serial").primary_key())
            .column(ColumnDefinition::new("name", "varchar").nullable(false));

        let type_def = introspector.generate_type(&table);

        assert_eq!(type_def.name, "Users");
        assert_eq!(type_def.fields.len(), 2);
        assert_eq!(type_def.fields[0].name, "id");
        assert_eq!(type_def.fields[1].name, "name");
    }

    #[test]
    fn test_relationship_generation() {
        let introspector = SchemaIntrospector::new();
        let tables = create_test_tables();
        let schema = introspector.build_schema(&tables);

        let post_relationships = schema.get_relationships_for("Posts");
        assert_eq!(post_relationships.len(), 1);
        assert_eq!(post_relationships[0].to_type, "Users");
        assert_eq!(post_relationships[0].relation_type, RelationType::ManyToOne);

        let user_relationships = schema.get_relationships_for("Users");
        assert_eq!(user_relationships.len(), 1);
        assert_eq!(user_relationships[0].to_type, "Posts");
        assert_eq!(user_relationships[0].relation_type, RelationType::OneToMany);
    }

    #[test]
    fn test_excluded_columns() {
        let mut introspector = SchemaIntrospector::new();
        introspector.exclude_column("users", "password_hash");

        let table = TableDefinition::new("users")
            .column(ColumnDefinition::new("id", "serial").primary_key())
            .column(ColumnDefinition::new("password_hash", "varchar"));

        let type_def = introspector.generate_type(&table);

        assert_eq!(type_def.fields.len(), 1);
        assert!(type_def.get_field("passwordHash").is_none());
    }

    #[test]
    fn test_type_name_override() {
        let mut introspector = SchemaIntrospector::new();
        introspector.set_type_name("users", "User");

        let table = TableDefinition::new("users")
            .column(ColumnDefinition::new("id", "serial").primary_key());

        let type_def = introspector.generate_type(&table);

        assert_eq!(type_def.name, "User");
    }

    #[test]
    fn test_field_type_display() {
        assert_eq!(FieldType::scalar(GraphQLScalar::String).to_string(), "String");
        assert_eq!(FieldType::object("User").to_string(), "User");
        assert_eq!(FieldType::list(FieldType::object("User")).to_string(), "[User]");
        assert_eq!(
            FieldType::non_null(FieldType::list(FieldType::object("User"))).to_string(),
            "[User]!"
        );
    }

    #[test]
    fn test_graphql_schema_default() {
        let schema = GraphQLSchema::default();
        assert!(schema.types.is_empty());
        assert!(schema.queries.is_empty());
        assert!(schema.mutations.is_empty());
    }
}
