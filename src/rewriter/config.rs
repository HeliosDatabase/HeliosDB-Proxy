//! Query Rewriter Configuration
//!
//! Configuration types for the query rewriting system.

use super::rules::RewriteRule;

/// Query rewriter configuration
#[derive(Debug, Clone)]
pub struct RewriterConfig {
    /// Enable query rewriting
    pub enabled: bool,

    /// Log rewrite operations
    pub log_rewrites: bool,

    /// Log rewrite errors
    pub log_errors: bool,

    /// Rewrite rules
    pub rules: Vec<RewriteRule>,
}

impl Default for RewriterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_rewrites: false,
            log_errors: true,
            rules: Vec::new(),
        }
    }
}

impl RewriterConfig {
    /// Create a new enabled config
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a builder
    pub fn builder() -> RewriterConfigBuilder {
        RewriterConfigBuilder::new()
    }
}

/// Builder for RewriterConfig
#[derive(Default)]
pub struct RewriterConfigBuilder {
    config: RewriterConfig,
}

impl RewriterConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: RewriterConfig {
                enabled: true,
                ..Default::default()
            },
        }
    }

    /// Enable/disable rewriting
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
    pub fn rule(mut self, rule: RewriteRule) -> Self {
        self.config.rules.push(rule);
        self
    }

    /// Add multiple rules
    pub fn rules(mut self, rules: Vec<RewriteRule>) -> Self {
        self.config.rules.extend(rules);
        self
    }

    /// Build the config
    pub fn build(self) -> RewriterConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RewriterConfig::default();
        assert!(!config.enabled);
        assert!(config.rules.is_empty());
    }

    #[test]
    fn test_config_builder() {
        let config = RewriterConfig::builder()
            .enabled(true)
            .log_rewrites(true)
            .build();

        assert!(config.enabled);
        assert!(config.log_rewrites);
    }
}
