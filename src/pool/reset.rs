//! Connection Reset Executor
//!
//! Handles resetting connection state when returning connections to the pool.

use crate::{ProxyError, Result};

/// Connection reset executor
///
/// Executes reset queries to clear session state before returning connections to the pool.
pub struct ConnectionResetExecutor {
    /// SQL to execute for reset
    reset_query: String,
    /// Whether to use DISCARD ALL
    use_discard_all: bool,
    /// Custom reset commands
    custom_commands: Vec<String>,
}

impl Default for ConnectionResetExecutor {
    fn default() -> Self {
        Self::new("DISCARD ALL")
    }
}

impl ConnectionResetExecutor {
    /// Create a new reset executor with the given query
    pub fn new(reset_query: impl Into<String>) -> Self {
        let query = reset_query.into();
        let use_discard_all = query.to_uppercase().contains("DISCARD ALL");

        Self {
            reset_query: query,
            use_discard_all,
            custom_commands: Vec::new(),
        }
    }

    /// Create a reset executor with multiple commands
    pub fn with_commands(commands: Vec<String>) -> Self {
        Self {
            reset_query: String::new(),
            use_discard_all: false,
            custom_commands: commands,
        }
    }

    /// Add a custom reset command
    pub fn add_command(&mut self, command: impl Into<String>) {
        self.custom_commands.push(command.into());
    }

    /// Get the reset query (or queries)
    pub fn reset_queries(&self) -> Vec<&str> {
        if !self.custom_commands.is_empty() {
            self.custom_commands.iter().map(|s| s.as_str()).collect()
        } else {
            vec![&self.reset_query]
        }
    }

    /// Check if using DISCARD ALL
    pub fn uses_discard_all(&self) -> bool {
        self.use_discard_all
    }

    /// Build the complete reset SQL (for protocols that support multi-statement)
    pub fn build_reset_sql(&self) -> String {
        if !self.custom_commands.is_empty() {
            self.custom_commands.join("; ")
        } else {
            self.reset_query.clone()
        }
    }

    /// Validate that reset queries are safe
    ///
    /// Returns an error if the reset queries contain potentially dangerous statements.
    pub fn validate(&self) -> Result<()> {
        let queries = self.reset_queries();

        for query in queries {
            let upper = query.to_uppercase();

            // Disallow data modification
            if upper.contains("INSERT")
                || upper.contains("UPDATE")
                || upper.contains("DELETE")
                || upper.contains("DROP")
                || upper.contains("CREATE")
                || upper.contains("ALTER")
                || upper.contains("TRUNCATE")
            {
                return Err(ProxyError::Configuration(format!(
                    "Reset query cannot contain data modification: {}",
                    query
                )));
            }

            // Disallow transaction control
            if upper.contains("BEGIN") || upper.contains("COMMIT") || upper.contains("ROLLBACK") {
                return Err(ProxyError::Configuration(format!(
                    "Reset query cannot contain transaction control: {}",
                    query
                )));
            }
        }

        Ok(())
    }
}

/// What DISCARD ALL resets in PostgreSQL:
///
/// - Prepared statements (DEALLOCATE ALL)
/// - Temporary tables (unlisted)
/// - Session variables (RESET ALL)
/// - Session-local advisory locks (pg_advisory_unlock_all)
/// - Sequences (not reset)
///
/// Equivalent to:
/// ```sql
/// CLOSE ALL;
/// DEALLOCATE ALL;
/// UNLISTEN *;
/// SELECT pg_advisory_unlock_all();
/// DISCARD PLANS;
/// DISCARD SEQUENCES;
/// DISCARD TEMP;
/// RESET ALL;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetLevel {
    /// Full reset (DISCARD ALL)
    Full,
    /// Reset prepared statements only (DEALLOCATE ALL)
    PreparedStatements,
    /// Reset session variables only (RESET ALL)
    SessionVariables,
    /// Minimal reset (just advisory locks)
    Minimal,
    /// No reset
    None,
}

impl ResetLevel {
    /// Get the SQL for this reset level
    pub fn sql(&self) -> Option<&'static str> {
        match self {
            ResetLevel::Full => Some("DISCARD ALL"),
            ResetLevel::PreparedStatements => Some("DEALLOCATE ALL"),
            ResetLevel::SessionVariables => Some("RESET ALL"),
            ResetLevel::Minimal => Some("SELECT pg_advisory_unlock_all()"),
            ResetLevel::None => None,
        }
    }

    /// Create an executor for this reset level
    pub fn executor(&self) -> ConnectionResetExecutor {
        match self.sql() {
            Some(sql) => ConnectionResetExecutor::new(sql),
            None => ConnectionResetExecutor {
                reset_query: String::new(),
                use_discard_all: false,
                custom_commands: Vec::new(),
            },
        }
    }
}

/// Builder for customizing reset behavior
pub struct ResetBuilder {
    commands: Vec<String>,
}

impl Default for ResetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ResetBuilder {
    /// Create a new reset builder
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
        }
    }

    /// Add DEALLOCATE ALL (clear prepared statements)
    pub fn deallocate_all(mut self) -> Self {
        self.commands.push("DEALLOCATE ALL".to_string());
        self
    }

    /// Add CLOSE ALL (close cursors)
    pub fn close_cursors(mut self) -> Self {
        self.commands.push("CLOSE ALL".to_string());
        self
    }

    /// Add UNLISTEN * (stop listening for notifications)
    pub fn unlisten_all(mut self) -> Self {
        self.commands.push("UNLISTEN *".to_string());
        self
    }

    /// Add RESET ALL (reset session variables)
    pub fn reset_all(mut self) -> Self {
        self.commands.push("RESET ALL".to_string());
        self
    }

    /// Add advisory lock release
    pub fn release_advisory_locks(mut self) -> Self {
        self.commands
            .push("SELECT pg_advisory_unlock_all()".to_string());
        self
    }

    /// Add DISCARD PLANS (clear cached query plans)
    pub fn discard_plans(mut self) -> Self {
        self.commands.push("DISCARD PLANS".to_string());
        self
    }

    /// Add DISCARD TEMP (drop temporary tables)
    pub fn discard_temp(mut self) -> Self {
        self.commands.push("DISCARD TEMP".to_string());
        self
    }

    /// Add a custom command
    pub fn custom(mut self, command: impl Into<String>) -> Self {
        self.commands.push(command.into());
        self
    }

    /// Build the reset executor
    pub fn build(self) -> ConnectionResetExecutor {
        if self.commands.is_empty() {
            ConnectionResetExecutor::default()
        } else {
            ConnectionResetExecutor::with_commands(self.commands)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_reset() {
        let executor = ConnectionResetExecutor::default();
        assert!(executor.uses_discard_all());
        assert_eq!(executor.reset_queries(), vec!["DISCARD ALL"]);
    }

    #[test]
    fn test_custom_reset() {
        let executor = ConnectionResetExecutor::new("RESET ALL");
        assert!(!executor.uses_discard_all());
        assert_eq!(executor.reset_queries(), vec!["RESET ALL"]);
    }

    #[test]
    fn test_multiple_commands() {
        let executor = ConnectionResetExecutor::with_commands(vec![
            "DEALLOCATE ALL".to_string(),
            "RESET ALL".to_string(),
        ]);
        assert_eq!(
            executor.reset_queries(),
            vec!["DEALLOCATE ALL", "RESET ALL"]
        );
        assert_eq!(executor.build_reset_sql(), "DEALLOCATE ALL; RESET ALL");
    }

    #[test]
    fn test_validation_success() {
        let executor = ConnectionResetExecutor::default();
        assert!(executor.validate().is_ok());

        let executor = ConnectionResetExecutor::new("RESET ALL");
        assert!(executor.validate().is_ok());
    }

    #[test]
    fn test_validation_failure() {
        let executor = ConnectionResetExecutor::new("DROP TABLE users");
        assert!(executor.validate().is_err());

        let executor = ConnectionResetExecutor::new("INSERT INTO log VALUES (1)");
        assert!(executor.validate().is_err());

        let executor = ConnectionResetExecutor::new("BEGIN; RESET ALL; COMMIT");
        assert!(executor.validate().is_err());
    }

    #[test]
    fn test_reset_level() {
        assert_eq!(ResetLevel::Full.sql(), Some("DISCARD ALL"));
        assert_eq!(ResetLevel::PreparedStatements.sql(), Some("DEALLOCATE ALL"));
        assert_eq!(ResetLevel::None.sql(), None);
    }

    #[test]
    fn test_reset_builder() {
        let executor = ResetBuilder::new()
            .deallocate_all()
            .close_cursors()
            .reset_all()
            .build();

        let queries = executor.reset_queries();
        assert_eq!(queries.len(), 3);
        assert!(queries.contains(&"DEALLOCATE ALL"));
        assert!(queries.contains(&"CLOSE ALL"));
        assert!(queries.contains(&"RESET ALL"));
    }
}
