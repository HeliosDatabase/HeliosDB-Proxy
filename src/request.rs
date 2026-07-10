//! Per-Request View
//!
//! A read-only bridge trait that modules can consume instead of depending
//! on a concrete per-request carrier type. Today per-request state is
//! scattered across `plugins::QueryContext`, `multi_tenancy::RequestContext`,
//! `analytics::QueryExecution`, and `auth::role_mapper::AuthorizationContext`;
//! merging them into a single struct would churn every consumer. The trait
//! here lets new code accept `&dyn RequestView` (or `impl RequestView`)
//! and work against any of them, while existing code keeps its native
//! concrete types.
//!
//! This is the foundation for T0-d. As new plugins and modules are added
//! (T2.2–T2.4 especially), they should accept `impl RequestView` rather
//! than reach for a module-specific carrier.

/// Read-only view of per-request metadata.
///
/// Every method returns `Option` where appropriate so implementations can
/// return `None` when the underlying carrier doesn't track that field.
/// Consumers that need a field unconditionally should guard with
/// `.ok_or(Error::…)` at the call site.
pub trait RequestView {
    /// SQL text of the query being processed, if this request carries a
    /// query. Returns `None` for non-SQL protocol messages
    /// (e.g. `Terminate`, `Sync`) or for contexts that don't carry SQL.
    fn query(&self) -> Option<&str>;

    /// Whether the query is a read-only operation (SELECT / SHOW / …).
    /// Defaults to `false` for safety — an implementation that cannot
    /// classify a query should leave the default.
    fn is_read_only(&self) -> bool {
        false
    }

    /// Target database from the client's startup message.
    fn database(&self) -> Option<&str> {
        None
    }

    /// Stable client session identifier.
    fn client_id(&self) -> Option<&str> {
        None
    }

    /// Tenant identifier, for multi-tenant deployments.
    fn tenant_id(&self) -> Option<&str> {
        None
    }
}

// --- Implementation for plugins::QueryContext -----------------------------
//
// Feature-gated because QueryContext is only compiled when `wasm-plugins`
// is enabled. Other modules can add their own impls beside their carriers
// as they adopt the trait.

#[cfg(feature = "wasm-plugins")]
impl RequestView for crate::plugins::QueryContext {
    fn query(&self) -> Option<&str> {
        Some(self.query.as_str())
    }

    fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    fn database(&self) -> Option<&str> {
        self.hook_context.database.as_deref()
    }

    fn client_id(&self) -> Option<&str> {
        self.hook_context.client_id.as_deref()
    }

    fn tenant_id(&self) -> Option<&str> {
        // HookContext carries a free-form attribute map; tenants land there
        // once multi-tenancy populates it.
        self.hook_context
            .attributes
            .get("tenant_id")
            .map(String::as_str)
    }
}

#[cfg(all(test, feature = "wasm-plugins"))]
mod tests {
    use super::*;
    use crate::plugins::{HookContext, QueryContext};
    use std::collections::HashMap;

    fn make_ctx(sql: &str, is_read_only: bool) -> QueryContext {
        let mut hc = HookContext {
            client_id: Some("session-abc".to_string()),
            database: Some("app".to_string()),
            ..Default::default()
        };
        hc.attributes
            .insert("tenant_id".to_string(), "acme".to_string());

        QueryContext {
            query: sql.to_string(),
            normalized: sql.to_string(),
            tables: Vec::new(),
            is_read_only,
            hook_context: hc,
        }
    }

    /// Demonstrate the trait abstraction: a generic function that only
    /// sees `RequestView` can read every field regardless of the
    /// concrete carrier.
    fn summarise<V: RequestView>(v: &V) -> String {
        format!(
            "sql={:?} ro={} db={:?} client={:?} tenant={:?}",
            v.query(),
            v.is_read_only(),
            v.database(),
            v.client_id(),
            v.tenant_id(),
        )
    }

    #[test]
    fn test_request_view_for_query_context() {
        let ctx = make_ctx("SELECT 1", true);
        assert_eq!(ctx.query(), Some("SELECT 1"));
        assert!(ctx.is_read_only());
        assert_eq!(ctx.database(), Some("app"));
        assert_eq!(ctx.client_id(), Some("session-abc"));
        assert_eq!(ctx.tenant_id(), Some("acme"));
    }

    #[test]
    fn test_request_view_for_query_context_write() {
        let ctx = make_ctx("INSERT INTO orders VALUES (1)", false);
        assert_eq!(ctx.query(), Some("INSERT INTO orders VALUES (1)"));
        assert!(!ctx.is_read_only());
    }

    /// A missing tenant attribute yields `None`, not a panic.
    #[test]
    fn test_request_view_tenant_missing() {
        let hc = HookContext {
            client_id: None,
            database: None,
            attributes: HashMap::new(),
            ..HookContext::default()
        };
        let ctx = QueryContext {
            query: "SELECT 1".to_string(),
            normalized: "SELECT 1".to_string(),
            tables: Vec::new(),
            is_read_only: true,
            hook_context: hc,
        };
        assert_eq!(ctx.tenant_id(), None);
    }

    /// Generic consumer works against a concrete type — proves the
    /// dispatch pattern future plugin code will rely on.
    #[test]
    fn test_generic_consumer_over_trait() {
        let ctx = make_ctx("SELECT 42", true);
        let summary = summarise(&ctx);
        assert!(summary.contains("sql=Some(\"SELECT 42\")"));
        assert!(summary.contains("ro=true"));
        assert!(summary.contains("tenant=Some(\"acme\")"));
    }
}
