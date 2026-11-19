//! Schema Auto-Discovery
//!
//! Discovery configuration and inference of table properties (temperature,
//! access pattern, workload) from table statistics.
//!
//! Discovery against a backend database is not supported in this release:
//! the backend-reading methods of [`SchemaDiscovery`] fail with
//! [`DiscoveryError::BackendDiscoveryUnsupported`] rather than return
//! fabricated schemas.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use super::{AccessPattern, DataTemperature, IndexSchema, Relationship, TableSchema, WorkloadType};

/// Configuration for schema discovery
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Enable automatic discovery
    pub enabled: bool,
    /// Discovery refresh interval
    pub refresh_interval: Duration,
    /// Schemas to discover (empty = all)
    pub schemas: Vec<String>,
    /// Tables to exclude from discovery
    pub exclude_tables: Vec<String>,
    /// Include system tables
    pub include_system_tables: bool,
    /// Discover foreign key relationships
    pub discover_relationships: bool,
    /// Discover indexes
    pub discover_indexes: bool,
    /// Infer access patterns from index usage
    pub infer_access_patterns: bool,
    /// Sample table statistics for temperature inference
    pub sample_statistics: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval: Duration::from_secs(300),
            schemas: vec!["public".to_string()],
            exclude_tables: Vec::new(),
            include_system_tables: false,
            discover_relationships: true,
            discover_indexes: true,
            infer_access_patterns: true,
            sample_statistics: true,
        }
    }
}

/// Schema discovery from database metadata
///
/// This release has no backend connection for discovery: the methods that
/// would read the catalogs of a live database ([`SchemaDiscovery::discover`],
/// [`SchemaDiscovery::discover_table`], [`SchemaDiscovery::discover_indexes`],
/// [`SchemaDiscovery::discover_relationships`],
/// [`SchemaDiscovery::get_statistics`]) fail with
/// [`DiscoveryError::BackendDiscoveryUnsupported`] instead of returning
/// fabricated schemas. Register tables explicitly with
/// `SchemaRegistry::register_table`. The pure inference helpers
/// (`infer_temperature`, `infer_access_pattern`, `infer_workload`) work on
/// caller-supplied [`TableStatistics`].
pub struct SchemaDiscovery {
    config: DiscoveryConfig,
    /// Last successful refresh time
    last_refresh: Arc<RwLock<Option<std::time::Instant>>>,
}

/// Table statistics from database
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Table name
    pub table_name: String,
    /// Estimated row count
    pub row_count: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Index size in bytes
    pub index_size_bytes: u64,
    /// Sequential scan count
    pub seq_scan_count: u64,
    /// Index scan count
    pub idx_scan_count: u64,
    /// Rows inserted since last vacuum
    pub n_tup_ins: u64,
    /// Rows updated since last vacuum
    pub n_tup_upd: u64,
    /// Rows deleted since last vacuum
    pub n_tup_del: u64,
    /// Last vacuum time
    pub last_vacuum: Option<String>,
    /// Last analyze time
    pub last_analyze: Option<String>,
}

impl SchemaDiscovery {
    /// Create a new schema discovery instance
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            last_refresh: Arc::new(RwLock::new(None)),
        }
    }

    /// Discover all table schemas from the backend.
    ///
    /// Always fails with [`DiscoveryError::BackendDiscoveryUnsupported`]:
    /// discovery against a backend is not supported in this release.
    pub async fn discover(&self) -> Result<Vec<TableSchema>, DiscoveryError> {
        Err(DiscoveryError::BackendDiscoveryUnsupported("table schemas"))
    }

    /// Discover a specific table's schema from the backend.
    ///
    /// Always fails with [`DiscoveryError::BackendDiscoveryUnsupported`].
    pub async fn discover_table(&self, _table_name: &str) -> Result<TableSchema, DiscoveryError> {
        Err(DiscoveryError::BackendDiscoveryUnsupported("table schema"))
    }

    /// Discover indexes for a table from the backend.
    ///
    /// Returns an empty list when index discovery is disabled in the
    /// configuration; otherwise fails with
    /// [`DiscoveryError::BackendDiscoveryUnsupported`].
    pub async fn discover_indexes(
        &self,
        _table_name: &str,
    ) -> Result<Vec<IndexSchema>, DiscoveryError> {
        if !self.config.discover_indexes {
            return Ok(Vec::new());
        }
        Err(DiscoveryError::BackendDiscoveryUnsupported("indexes"))
    }

    /// Discover foreign key relationships from the backend.
    ///
    /// Returns an empty list when relationship discovery is disabled in the
    /// configuration; otherwise fails with
    /// [`DiscoveryError::BackendDiscoveryUnsupported`].
    pub async fn discover_relationships(&self) -> Result<Vec<Relationship>, DiscoveryError> {
        if !self.config.discover_relationships {
            return Ok(Vec::new());
        }
        Err(DiscoveryError::BackendDiscoveryUnsupported(
            "foreign key relationships",
        ))
    }

    /// Get table statistics from the backend for temperature inference.
    ///
    /// Fails with [`DiscoveryError::StatisticsDisabled`] when statistics
    /// sampling is disabled, otherwise with
    /// [`DiscoveryError::BackendDiscoveryUnsupported`].
    pub async fn get_statistics(
        &self,
        _table_name: &str,
    ) -> Result<TableStatistics, DiscoveryError> {
        if !self.config.sample_statistics {
            return Err(DiscoveryError::StatisticsDisabled);
        }
        Err(DiscoveryError::BackendDiscoveryUnsupported(
            "table statistics",
        ))
    }

    /// Infer data temperature from statistics
    pub fn infer_temperature(&self, stats: &TableStatistics) -> DataTemperature {
        // Calculate access frequency
        let total_scans = stats.seq_scan_count + stats.idx_scan_count;
        let write_rate = stats.n_tup_ins + stats.n_tup_upd + stats.n_tup_del;

        // Hot: High access rate, recent modifications
        if total_scans > 10_000 && write_rate > 100 {
            return DataTemperature::Hot;
        }

        // Warm: Moderate access rate
        if total_scans > 1_000 || write_rate > 10 {
            return DataTemperature::Warm;
        }

        // Cold: Low access rate
        if total_scans > 100 {
            return DataTemperature::Cold;
        }

        // Frozen: Rarely or never accessed
        DataTemperature::Frozen
    }

    /// Infer access pattern from index usage
    pub fn infer_access_pattern(&self, stats: &TableStatistics) -> AccessPattern {
        let total_scans = stats.seq_scan_count + stats.idx_scan_count;

        if total_scans == 0 {
            return AccessPattern::Mixed;
        }

        let index_ratio = stats.idx_scan_count as f64 / total_scans as f64;

        // High index usage suggests point lookups
        if index_ratio > 0.9 {
            return AccessPattern::PointLookup;
        }

        // Moderate index usage suggests range scans
        if index_ratio > 0.5 {
            return AccessPattern::RangeScan;
        }

        // Low index usage suggests full scans (OLAP)
        if index_ratio < 0.1 {
            return AccessPattern::FullScan;
        }

        AccessPattern::Mixed
    }

    /// Infer workload type from statistics
    pub fn infer_workload(&self, stats: &TableStatistics) -> WorkloadType {
        let total_scans = stats.seq_scan_count + stats.idx_scan_count;
        let write_rate = stats.n_tup_ins + stats.n_tup_upd + stats.n_tup_del;

        // High write rate with index usage = OLTP
        if write_rate > 100 && stats.idx_scan_count > stats.seq_scan_count {
            return WorkloadType::OLTP;
        }

        // High read rate with sequential scans = OLAP
        if total_scans > 1000 && stats.seq_scan_count > stats.idx_scan_count * 2 {
            return WorkloadType::OLAP;
        }

        // Balanced read/write = HTAP
        if write_rate > 50 && total_scans > 500 {
            return WorkloadType::HTAP;
        }

        WorkloadType::Mixed
    }

    /// Check if a refresh is due (always true until a refresh has succeeded)
    pub async fn needs_refresh(&self) -> bool {
        let last_refresh = self.last_refresh.read().await;
        match *last_refresh {
            None => true,
            Some(time) => time.elapsed() > self.config.refresh_interval,
        }
    }

    /// Refresh discovered schemas from the backend.
    ///
    /// Fails with [`DiscoveryError::BackendDiscoveryUnsupported`] (see
    /// [`SchemaDiscovery::discover`]).
    pub async fn refresh(&self) -> Result<(), DiscoveryError> {
        self.discover().await?;
        if self.config.discover_relationships {
            self.discover_relationships().await?;
        }
        *self.last_refresh.write().await = Some(std::time::Instant::now());
        Ok(())
    }

    /// Forget the last refresh time
    pub async fn clear_cache(&self) {
        *self.last_refresh.write().await = None;
    }
}

/// Discovery errors
#[derive(Debug, Clone)]
pub enum DiscoveryError {
    /// Table not found
    TableNotFound(String),
    /// Connection error
    ConnectionError(String),
    /// Query error
    QueryError(String),
    /// Statistics collection disabled
    StatisticsDisabled,
    /// Cache refresh failed
    RefreshFailed(String),
    /// Reading this information from a backend database is not supported in
    /// this release (no fabricated data is returned)
    BackendDiscoveryUnsupported(&'static str),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
            Self::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            Self::QueryError(msg) => write!(f, "Query error: {}", msg),
            Self::StatisticsDisabled => write!(f, "Statistics collection is disabled"),
            Self::RefreshFailed(msg) => write!(f, "Cache refresh failed: {}", msg),
            Self::BackendDiscoveryUnsupported(what) => write!(
                f,
                "schema discovery of {} against a backend is not supported in this release; \
                 register tables explicitly",
                what
            ),
        }
    }
}

impl std::error::Error for DiscoveryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discovery_config_default() {
        let config = DiscoveryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.schemas, vec!["public"]);
        assert!(config.discover_relationships);
        assert!(config.discover_indexes);
    }

    #[tokio::test]
    async fn test_schema_discovery_new() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);
        assert!(discovery.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_backend_discovery_fails_closed() {
        let discovery = SchemaDiscovery::new(DiscoveryConfig::default());

        assert!(matches!(
            discovery.discover().await,
            Err(DiscoveryError::BackendDiscoveryUnsupported(_))
        ));
        assert!(matches!(
            discovery.discover_table("users").await,
            Err(DiscoveryError::BackendDiscoveryUnsupported(_))
        ));
        assert!(matches!(
            discovery.discover_indexes("users").await,
            Err(DiscoveryError::BackendDiscoveryUnsupported(_))
        ));
        assert!(matches!(
            discovery.discover_relationships().await,
            Err(DiscoveryError::BackendDiscoveryUnsupported(_))
        ));
        assert!(matches!(
            discovery.get_statistics("users").await,
            Err(DiscoveryError::BackendDiscoveryUnsupported(_))
        ));
        assert!(matches!(
            discovery.refresh().await,
            Err(DiscoveryError::BackendDiscoveryUnsupported(_))
        ));
        // A failed refresh never marks the schemas as fresh.
        assert!(discovery.needs_refresh().await);

        let msg = discovery.discover().await.unwrap_err().to_string();
        assert!(msg.contains("not supported in this release"), "{msg}");
    }

    #[tokio::test]
    async fn test_disabled_discovery_returns_empty() {
        let config = DiscoveryConfig {
            discover_indexes: false,
            discover_relationships: false,
            ..Default::default()
        };
        let discovery = SchemaDiscovery::new(config);
        assert!(discovery
            .discover_indexes("users")
            .await
            .unwrap()
            .is_empty());
        assert!(discovery.discover_relationships().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_infer_temperature() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        // Hot table
        let hot_stats = TableStatistics {
            table_name: "active_sessions".to_string(),
            row_count: 10000,
            size_bytes: 1_000_000,
            index_size_bytes: 100_000,
            seq_scan_count: 1000,
            idx_scan_count: 50000,
            n_tup_ins: 500,
            n_tup_upd: 200,
            n_tup_del: 100,
            last_vacuum: None,
            last_analyze: None,
        };
        assert_eq!(
            discovery.infer_temperature(&hot_stats),
            DataTemperature::Hot
        );

        // Cold table
        let cold_stats = TableStatistics {
            table_name: "audit_logs".to_string(),
            row_count: 1_000_000,
            size_bytes: 100_000_000,
            index_size_bytes: 10_000_000,
            seq_scan_count: 50,
            idx_scan_count: 100,
            n_tup_ins: 5,
            n_tup_upd: 0,
            n_tup_del: 0,
            last_vacuum: None,
            last_analyze: None,
        };
        assert_eq!(
            discovery.infer_temperature(&cold_stats),
            DataTemperature::Cold
        );
    }

    #[tokio::test]
    async fn test_infer_access_pattern() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        // Point lookup pattern
        let point_stats = TableStatistics {
            table_name: "users".to_string(),
            row_count: 100000,
            size_bytes: 10_000_000,
            index_size_bytes: 1_000_000,
            seq_scan_count: 10,
            idx_scan_count: 10000,
            n_tup_ins: 0,
            n_tup_upd: 0,
            n_tup_del: 0,
            last_vacuum: None,
            last_analyze: None,
        };
        assert_eq!(
            discovery.infer_access_pattern(&point_stats),
            AccessPattern::PointLookup
        );

        // Full scan pattern
        let scan_stats = TableStatistics {
            table_name: "reports".to_string(),
            row_count: 100000,
            size_bytes: 10_000_000,
            index_size_bytes: 1_000_000,
            seq_scan_count: 1000,
            idx_scan_count: 50,
            n_tup_ins: 0,
            n_tup_upd: 0,
            n_tup_del: 0,
            last_vacuum: None,
            last_analyze: None,
        };
        assert_eq!(
            discovery.infer_access_pattern(&scan_stats),
            AccessPattern::FullScan
        );
    }

    #[tokio::test]
    async fn test_infer_workload() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        // OLTP workload
        let oltp_stats = TableStatistics {
            table_name: "orders".to_string(),
            row_count: 100000,
            size_bytes: 10_000_000,
            index_size_bytes: 1_000_000,
            seq_scan_count: 100,
            idx_scan_count: 5000,
            n_tup_ins: 200,
            n_tup_upd: 50,
            n_tup_del: 10,
            last_vacuum: None,
            last_analyze: None,
        };
        assert_eq!(discovery.infer_workload(&oltp_stats), WorkloadType::OLTP);

        // OLAP workload
        let olap_stats = TableStatistics {
            table_name: "sales_history".to_string(),
            row_count: 10_000_000,
            size_bytes: 1_000_000_000,
            index_size_bytes: 100_000_000,
            seq_scan_count: 5000,
            idx_scan_count: 100,
            n_tup_ins: 10,
            n_tup_upd: 0,
            n_tup_del: 0,
            last_vacuum: None,
            last_analyze: None,
        };
        assert_eq!(discovery.infer_workload(&olap_stats), WorkloadType::OLAP);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let discovery = SchemaDiscovery::new(DiscoveryConfig::default());
        discovery.clear_cache().await;
        assert!(discovery.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_statistics_disabled() {
        let config = DiscoveryConfig {
            sample_statistics: false,
            ..Default::default()
        };
        let discovery = SchemaDiscovery::new(config);

        let result = discovery.get_statistics("users").await;
        assert!(matches!(result, Err(DiscoveryError::StatisticsDisabled)));
    }

    #[test]
    fn test_discovery_error_display() {
        let err = DiscoveryError::TableNotFound("users".to_string());
        assert_eq!(err.to_string(), "Table not found: users");

        let err = DiscoveryError::ConnectionError("timeout".to_string());
        assert_eq!(err.to_string(), "Connection error: timeout");
    }
}
