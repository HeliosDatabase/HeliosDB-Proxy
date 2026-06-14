//! Instant branch databases.
//!
//! Provisions copy-on-write-ish database branches through the proxy:
//! `CREATE DATABASE <branch> TEMPLATE <base>` (vanilla PostgreSQL's fast
//! file-clone — the proxy-layer approximation of Neon/PlanetScale branching;
//! a Nano backend can branch natively). The proxy issues the maintenance
//! statements over its backend PG-wire client; clients then connect to the
//! branch simply by database name (the proxy relays the startup unchanged), so
//! routing is native — branching here is the *provisioning + lifecycle* layer.

use std::time::Duration;

use crate::backend::types::TextValue;
use crate::backend::{tls::default_client_config, BackendClient, BackendConfig, TlsMode};
use crate::config::BranchConfig;

fn admin_cfg(cfg: &BranchConfig) -> BackendConfig {
    BackendConfig {
        host: cfg.backend_host.clone(),
        port: cfg.backend_port,
        user: cfg.admin_user.clone(),
        password: cfg.admin_password.clone(),
        database: Some(cfg.admin_database.clone()),
        application_name: Some("heliosproxy-branch".to_string()),
        tls_mode: TlsMode::Disable,
        connect_timeout: Duration::from_secs(5),
        query_timeout: Duration::from_secs(120),
        tls_config: default_client_config(),
    }
}

/// A database identifier must be a simple `[A-Za-z_][A-Za-z0-9_]*` name. We
/// validate (rather than escape) because `CREATE DATABASE` cannot be
/// parameterised, so a strict allowlist is the safe path.
fn valid_ident(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Create a branch database from `base` (or the configured default).
pub async fn create(cfg: &BranchConfig, name: &str, base: Option<&str>) -> Result<(), String> {
    let base = base.unwrap_or(&cfg.base_database);
    if !valid_ident(name) {
        return Err(format!("invalid branch name '{}' (use [A-Za-z_][A-Za-z0-9_]*)", name));
    }
    if !valid_ident(base) {
        return Err(format!("invalid base name '{}'", base));
    }
    let mut c = BackendClient::connect(&admin_cfg(cfg))
        .await
        .map_err(|e| format!("admin connect: {}", e))?;
    let sql = format!("CREATE DATABASE \"{}\" TEMPLATE \"{}\"", name, base);
    let r = c.execute(&sql).await.map_err(|e| format!("create branch: {}", e));
    c.close().await;
    r.map(|_| ())
}

/// Drop a branch database.
pub async fn drop(cfg: &BranchConfig, name: &str) -> Result<(), String> {
    if !valid_ident(name) {
        return Err(format!("invalid branch name '{}'", name));
    }
    let mut c = BackendClient::connect(&admin_cfg(cfg))
        .await
        .map_err(|e| format!("admin connect: {}", e))?;
    let r = c
        .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", name))
        .await
        .map_err(|e| format!("drop branch: {}", e));
    c.close().await;
    r.map(|_| ())
}

/// List user databases (excludes templates + the built-in maintenance DBs).
pub async fn list(cfg: &BranchConfig) -> Result<Vec<String>, String> {
    let mut c = BackendClient::connect(&admin_cfg(cfg))
        .await
        .map_err(|e| format!("admin connect: {}", e))?;
    let res = c
        .simple_query(
            "SELECT datname FROM pg_database \
             WHERE datistemplate = false AND datname NOT IN ('postgres','template0','template1') \
             ORDER BY datname",
        )
        .await
        .map_err(|e| format!("list branches: {}", e));
    c.close().await;
    let res = res?;
    Ok(res
        .rows
        .into_iter()
        .filter_map(|row| match row.into_iter().next() {
            Some(TextValue::Text(s)) => Some(s),
            _ => None,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_validation() {
        assert!(valid_ident("feature_x"));
        assert!(valid_ident("_b1"));
        assert!(!valid_ident("1bad"));
        assert!(!valid_ident("drop;table"));
        assert!(!valid_ident("a b"));
        assert!(!valid_ident(""));
        assert!(!valid_ident("\"inject\""));
    }
}
