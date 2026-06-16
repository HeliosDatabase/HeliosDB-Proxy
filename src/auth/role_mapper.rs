//! Role Mapping
//!
//! Maps authenticated identities to database roles and permissions.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::config::{Identity, RoleMappingCondition, RoleMappingRule};

/// Role mapper
pub struct RoleMapper {
    /// Mapping rules
    rules: Vec<RoleMappingRule>,

    /// Static role assignments (user_id -> roles)
    static_roles: Arc<RwLock<HashMap<String, Vec<String>>>>,

    /// Group to role mappings
    group_roles: HashMap<String, Vec<String>>,

    /// Default roles for authenticated users
    default_roles: Vec<String>,

    /// Default role for anonymous users
    anonymous_role: Option<String>,
}

impl RoleMapper {
    /// Create a new role mapper
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            static_roles: Arc::new(RwLock::new(HashMap::new())),
            group_roles: HashMap::new(),
            default_roles: Vec::new(),
            anonymous_role: None,
        }
    }

    /// Create a builder
    pub fn builder() -> RoleMapperBuilder {
        RoleMapperBuilder::new()
    }

    /// Map an identity to database roles
    pub fn map_roles(&self, identity: &Identity) -> Vec<String> {
        let mut roles = Vec::new();

        // Add roles from identity
        roles.extend(identity.roles.clone());

        // Add static roles
        if let Some(static_roles) = self.static_roles.read().get(&identity.user_id) {
            roles.extend(static_roles.clone());
        }

        // Add group-based roles
        for group in &identity.groups {
            if let Some(group_roles) = self.group_roles.get(group) {
                roles.extend(group_roles.clone());
            }
        }

        // Apply mapping rules
        for rule in &self.rules {
            if self.evaluate_rule(rule, identity) {
                roles.extend(rule.assign_roles.clone());
            }
        }

        // Add default roles if no roles assigned
        if roles.is_empty() {
            roles.extend(self.default_roles.clone());
        }

        // Deduplicate
        roles.sort();
        roles.dedup();

        roles
    }

    /// Map an identity to a single database role (primary)
    pub fn map_primary_role(&self, identity: &Identity) -> Option<String> {
        let roles = self.map_roles(identity);
        roles.into_iter().next()
    }

    /// Check if identity has a specific permission
    pub fn has_permission(&self, identity: &Identity, permission: &str) -> bool {
        let roles = self.map_roles(identity);

        for rule in &self.rules {
            if roles.iter().any(|r| rule.assign_roles.contains(r))
                && rule.permissions.contains(&permission.to_string())
            {
                return true;
            }
        }

        false
    }

    /// Get all permissions for an identity
    pub fn get_permissions(&self, identity: &Identity) -> Vec<String> {
        let roles = self.map_roles(identity);
        let mut permissions = Vec::new();

        for rule in &self.rules {
            if roles.iter().any(|r| rule.assign_roles.contains(r)) {
                permissions.extend(rule.permissions.clone());
            }
        }

        permissions.sort();
        permissions.dedup();
        permissions
    }

    /// Add a static role assignment
    pub fn assign_static_role(&self, user_id: impl Into<String>, role: impl Into<String>) {
        let user_id = user_id.into();
        let role = role.into();

        let mut static_roles = self.static_roles.write();
        static_roles.entry(user_id).or_default().push(role);
    }

    /// Remove a static role assignment
    pub fn remove_static_role(&self, user_id: &str, role: &str) {
        let mut static_roles = self.static_roles.write();
        if let Some(roles) = static_roles.get_mut(user_id) {
            roles.retain(|r| r != role);
        }
    }

    /// Get anonymous role
    pub fn anonymous_role(&self) -> Option<&String> {
        self.anonymous_role.as_ref()
    }

    /// Evaluate a mapping rule against an identity
    fn evaluate_rule(&self, rule: &RoleMappingRule, identity: &Identity) -> bool {
        // All conditions must match
        for condition in &rule.conditions {
            if !self.evaluate_condition(condition, identity) {
                return false;
            }
        }
        true
    }

    /// Evaluate a single condition
    fn evaluate_condition(&self, condition: &RoleMappingCondition, identity: &Identity) -> bool {
        match condition {
            RoleMappingCondition::HasClaim { claim, value } => {
                match identity.claims.get(claim) {
                    Some(claim_value) => {
                        if let Some(expected) = value {
                            claim_value.as_str() == Some(expected.as_str())
                        } else {
                            true // Just check claim exists
                        }
                    }
                    None => false,
                }
            }

            RoleMappingCondition::InGroup { group } => identity.groups.contains(group),

            RoleMappingCondition::HasRole { role } => identity.roles.contains(role),

            RoleMappingCondition::FromTenant { tenant_id } => {
                identity.tenant_id.as_ref() == Some(tenant_id)
            }

            RoleMappingCondition::AuthMethod { method } => &identity.auth_method == method,

            RoleMappingCondition::EmailDomain { domain } => identity
                .email
                .as_ref()
                .map(|e| e.ends_with(&format!("@{}", domain)))
                .unwrap_or(false),

            RoleMappingCondition::UsernamePattern { pattern } => {
                self.match_pattern(&identity.user_id, pattern)
            }

            RoleMappingCondition::And { conditions } => conditions
                .iter()
                .all(|c| self.evaluate_condition(c, identity)),

            RoleMappingCondition::Or { conditions } => conditions
                .iter()
                .any(|c| self.evaluate_condition(c, identity)),

            RoleMappingCondition::Not { condition } => {
                !self.evaluate_condition(condition, identity)
            }
        }
    }

    /// Simple pattern matching (supports * wildcard)
    fn match_pattern(&self, value: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if let Some(prefix) = pattern.strip_suffix('*') {
            return value.starts_with(prefix);
        }

        if let Some(suffix) = pattern.strip_prefix('*') {
            return value.ends_with(suffix);
        }

        value == pattern
    }
}

impl Default for RoleMapper {
    fn default() -> Self {
        Self::new()
    }
}

/// Role mapper builder
pub struct RoleMapperBuilder {
    rules: Vec<RoleMappingRule>,
    group_roles: HashMap<String, Vec<String>>,
    default_roles: Vec<String>,
    anonymous_role: Option<String>,
}

impl RoleMapperBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            group_roles: HashMap::new(),
            default_roles: Vec::new(),
            anonymous_role: None,
        }
    }

    /// Add a mapping rule
    pub fn rule(mut self, rule: RoleMappingRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Add a group to role mapping
    pub fn group_role(mut self, group: impl Into<String>, role: impl Into<String>) -> Self {
        let group = group.into();
        let role = role.into();

        self.group_roles.entry(group).or_default().push(role);
        self
    }

    /// Add a default role
    pub fn default_role(mut self, role: impl Into<String>) -> Self {
        self.default_roles.push(role.into());
        self
    }

    /// Set anonymous role
    pub fn anonymous_role(mut self, role: impl Into<String>) -> Self {
        self.anonymous_role = Some(role.into());
        self
    }

    /// Build the role mapper
    pub fn build(self) -> RoleMapper {
        RoleMapper {
            rules: self.rules,
            static_roles: Arc::new(RwLock::new(HashMap::new())),
            group_roles: self.group_roles,
            default_roles: self.default_roles,
            anonymous_role: self.anonymous_role,
        }
    }
}

impl Default for RoleMapperBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Permission set for authorization
#[derive(Debug, Clone)]
pub struct PermissionSet {
    /// Allowed databases
    pub databases: Vec<String>,

    /// Allowed schemas
    pub schemas: Vec<String>,

    /// Allowed tables (database.schema.table patterns)
    pub tables: Vec<String>,

    /// Allowed operations
    pub operations: Vec<Operation>,

    /// Row-level security predicates
    pub row_predicates: HashMap<String, String>,

    /// Column restrictions
    pub column_restrictions: HashMap<String, Vec<String>>,
}

/// Database operation types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Grant,
    Execute,
    All,
}

impl Operation {
    /// Parse operation from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Self::Select),
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            "CREATE" => Some(Self::Create),
            "DROP" => Some(Self::Drop),
            "ALTER" => Some(Self::Alter),
            "GRANT" => Some(Self::Grant),
            "EXECUTE" => Some(Self::Execute),
            "ALL" => Some(Self::All),
            _ => None,
        }
    }
}

impl PermissionSet {
    /// Create an empty permission set
    pub fn empty() -> Self {
        Self {
            databases: Vec::new(),
            schemas: Vec::new(),
            tables: Vec::new(),
            operations: Vec::new(),
            row_predicates: HashMap::new(),
            column_restrictions: HashMap::new(),
        }
    }

    /// Create a full access permission set
    pub fn full_access() -> Self {
        Self {
            databases: vec!["*".to_string()],
            schemas: vec!["*".to_string()],
            tables: vec!["*".to_string()],
            operations: vec![Operation::All],
            row_predicates: HashMap::new(),
            column_restrictions: HashMap::new(),
        }
    }

    /// Check if operation is allowed on table
    pub fn is_operation_allowed(&self, operation: &Operation, table: &str) -> bool {
        // Check operation
        if !self.operations.contains(&Operation::All) && !self.operations.contains(operation) {
            return false;
        }

        // Check table access
        if self.tables.is_empty() {
            return true;
        }

        for pattern in &self.tables {
            if pattern == "*" || pattern == table {
                return true;
            }

            // Simple wildcard matching
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if table.starts_with(prefix) {
                    return true;
                }
            }
        }

        false
    }

    /// Get row predicate for a table
    pub fn row_predicate(&self, table: &str) -> Option<&String> {
        self.row_predicates.get(table)
    }

    /// Get allowed columns for a table
    pub fn allowed_columns(&self, table: &str) -> Option<&Vec<String>> {
        self.column_restrictions.get(table)
    }
}

/// Authorization context
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    /// User identity
    pub identity: Identity,

    /// Mapped roles
    pub roles: Vec<String>,

    /// Permission set
    pub permissions: PermissionSet,

    /// Session start time
    pub session_start: chrono::DateTime<chrono::Utc>,

    /// Additional context
    pub context: HashMap<String, String>,
}

impl AuthorizationContext {
    /// Create a new authorization context
    pub fn new(identity: Identity, roles: Vec<String>, permissions: PermissionSet) -> Self {
        Self {
            identity,
            roles,
            permissions,
            session_start: chrono::Utc::now(),
            context: HashMap::new(),
        }
    }

    /// Check if operation is allowed
    pub fn is_allowed(&self, operation: &Operation, table: &str) -> bool {
        self.permissions.is_operation_allowed(operation, table)
    }

    /// Add context value
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity() -> Identity {
        Identity {
            user_id: "user123".to_string(),
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            roles: vec!["user".to_string()],
            groups: vec!["developers".to_string()],
            tenant_id: Some("tenant1".to_string()),
            claims: {
                let mut claims = HashMap::new();
                claims.insert("department".to_string(), serde_json::json!("engineering"));
                claims
            },
            auth_method: "jwt".to_string(),
            authenticated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_basic_role_mapping() {
        let mapper = RoleMapper::builder()
            .group_role("developers", "db_developer")
            .default_role("db_user")
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"user".to_string())); // From identity
        assert!(roles.contains(&"db_developer".to_string())); // From group
    }

    #[test]
    fn test_rule_based_mapping() {
        let mapper = RoleMapper::builder()
            .rule(RoleMappingRule {
                name: "admin_from_claim".to_string(),
                condition: RoleCondition::Always,
                db_role: String::new(),
                conditions: vec![RoleMappingCondition::HasClaim {
                    claim: "department".to_string(),
                    value: Some("engineering".to_string()),
                }],
                assign_roles: vec!["db_admin".to_string()],
                permissions: vec!["read".to_string(), "write".to_string()],
                priority: 1,
            })
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"db_admin".to_string()));
    }

    #[test]
    fn test_tenant_condition() {
        let mapper = RoleMapper::builder()
            .rule(RoleMappingRule {
                name: "tenant1_role".to_string(),
                condition: RoleCondition::Always,
                db_role: String::new(),
                conditions: vec![RoleMappingCondition::FromTenant {
                    tenant_id: "tenant1".to_string(),
                }],
                assign_roles: vec!["tenant1_user".to_string()],
                permissions: Vec::new(),
                priority: 1,
            })
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"tenant1_user".to_string()));
    }

    #[test]
    fn test_email_domain_condition() {
        let mapper = RoleMapper::builder()
            .rule(RoleMappingRule {
                name: "example_domain".to_string(),
                condition: RoleCondition::Always,
                db_role: String::new(),
                conditions: vec![RoleMappingCondition::EmailDomain {
                    domain: "example.com".to_string(),
                }],
                assign_roles: vec!["internal_user".to_string()],
                permissions: Vec::new(),
                priority: 1,
            })
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"internal_user".to_string()));
    }

    #[test]
    fn test_and_condition() {
        let mapper = RoleMapper::builder()
            .rule(RoleMappingRule {
                name: "combined".to_string(),
                condition: RoleCondition::Always,
                db_role: String::new(),
                conditions: vec![RoleMappingCondition::And {
                    conditions: vec![
                        RoleMappingCondition::HasRole {
                            role: "user".to_string(),
                        },
                        RoleMappingCondition::InGroup {
                            group: "developers".to_string(),
                        },
                    ],
                }],
                assign_roles: vec!["power_user".to_string()],
                permissions: Vec::new(),
                priority: 1,
            })
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"power_user".to_string()));
    }

    #[test]
    fn test_or_condition() {
        let mapper = RoleMapper::builder()
            .rule(RoleMappingRule {
                name: "either".to_string(),
                condition: RoleCondition::Always,
                db_role: String::new(),
                conditions: vec![RoleMappingCondition::Or {
                    conditions: vec![
                        RoleMappingCondition::HasRole {
                            role: "admin".to_string(),
                        },
                        RoleMappingCondition::HasRole {
                            role: "user".to_string(),
                        },
                    ],
                }],
                assign_roles: vec!["authenticated".to_string()],
                permissions: Vec::new(),
                priority: 1,
            })
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"authenticated".to_string()));
    }

    #[test]
    fn test_not_condition() {
        let mapper = RoleMapper::builder()
            .rule(RoleMappingRule {
                name: "not_admin".to_string(),
                condition: RoleCondition::Always,
                db_role: String::new(),
                conditions: vec![RoleMappingCondition::Not {
                    condition: Box::new(RoleMappingCondition::HasRole {
                        role: "admin".to_string(),
                    }),
                }],
                assign_roles: vec!["regular_user".to_string()],
                permissions: Vec::new(),
                priority: 1,
            })
            .build();

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"regular_user".to_string()));
    }

    #[test]
    fn test_static_role_assignment() {
        let mapper = RoleMapper::new();
        mapper.assign_static_role("user123", "special_role");

        let identity = test_identity();
        let roles = mapper.map_roles(&identity);

        assert!(roles.contains(&"special_role".to_string()));
    }

    #[test]
    fn test_default_roles() {
        let mapper = RoleMapper::builder().default_role("guest").build();

        // Empty identity with no roles
        let identity = Identity {
            user_id: "empty".to_string(),
            name: None,
            email: None,
            roles: Vec::new(),
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "none".to_string(),
            authenticated_at: chrono::Utc::now(),
        };

        let roles = mapper.map_roles(&identity);
        assert!(roles.contains(&"guest".to_string()));
    }

    #[test]
    fn test_permission_set() {
        let permissions = PermissionSet {
            databases: vec!["mydb".to_string()],
            schemas: vec!["public".to_string()],
            tables: vec!["users".to_string(), "orders*".to_string()],
            operations: vec![Operation::Select, Operation::Insert],
            row_predicates: {
                let mut p = HashMap::new();
                p.insert("users".to_string(), "id = current_user_id()".to_string());
                p
            },
            column_restrictions: HashMap::new(),
        };

        assert!(permissions.is_operation_allowed(&Operation::Select, "users"));
        assert!(permissions.is_operation_allowed(&Operation::Insert, "orders_2024"));
        assert!(!permissions.is_operation_allowed(&Operation::Delete, "users"));
        assert!(!permissions.is_operation_allowed(&Operation::Select, "secrets"));

        assert_eq!(
            permissions.row_predicate("users"),
            Some(&"id = current_user_id()".to_string())
        );
    }

    #[test]
    fn test_pattern_matching() {
        let mapper = RoleMapper::new();

        assert!(mapper.match_pattern("admin_user", "admin*"));
        assert!(mapper.match_pattern("user_admin", "*admin"));
        assert!(mapper.match_pattern("anything", "*"));
        assert!(mapper.match_pattern("exact", "exact"));
        assert!(!mapper.match_pattern("mismatch", "exact"));
    }

    #[test]
    fn test_authorization_context() {
        let identity = test_identity();
        let permissions = PermissionSet::full_access();
        let roles = vec!["admin".to_string()];

        let ctx = AuthorizationContext::new(identity, roles, permissions)
            .with_context("client_ip", "192.168.1.1");

        assert!(ctx.is_allowed(&Operation::Select, "any_table"));
        assert_eq!(
            ctx.context.get("client_ip"),
            Some(&"192.168.1.1".to_string())
        );
    }
}
