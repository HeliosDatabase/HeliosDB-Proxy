//! Schema Auto-Discovery
//!
//! Automatically discovers table schemas from database metadata.
//! Supports PostgreSQL information_schema and system catalogs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use super::{
    TableSchema, ColumnSchema, IndexSchema, AccessPattern,
    DataTemperature, WorkloadType, Relationship, PartitionKey,
};
use super::registry::{StorageType, IndexType, RelationshipType};

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
pub struct SchemaDiscovery {
    config: DiscoveryConfig,
    /// Cached discovered schemas
    cache: Arc<RwLock<DiscoveryCache>>,
    /// Last refresh time
    last_refresh: Arc<RwLock<Option<std::time::Instant>>>,
}

/// Cache for discovered schema information
#[derive(Debug, Default)]
struct DiscoveryCache {
    tables: HashMap<String, TableSchema>,
    indexes: HashMap<String, Vec<IndexSchema>>,
    relationships: Vec<Relationship>,
    statistics: HashMap<String, TableStatistics>,
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
            cache: Arc::new(RwLock::new(DiscoveryCache::default())),
            last_refresh: Arc::new(RwLock::new(None)),
        }
    }

    /// Discover all table schemas
    ///
    /// Returns discovered tables with inferred properties
    pub async fn discover(&self) -> Result<Vec<TableSchema>, DiscoveryError> {
        // Build discovery queries
        let queries = self.build_discovery_queries();

        // In a real implementation, these would execute against the database
        // For now, we return mock data for testing
        let mut tables = Vec::new();

        // Example discovered table
        let users_table = TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    is_indexed: true,
                    storage_type: StorageType::Row,
                },
                ColumnSchema {
                    name: "email".to_string(),
                    data_type: "varchar(255)".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    is_indexed: true,
                    storage_type: StorageType::Row,
                },
                ColumnSchema {
                    name: "created_at".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    is_indexed: true,
                    storage_type: StorageType::Row,
                },
            ],
            access_pattern: AccessPattern::PointLookup,
            temperature: DataTemperature::Hot,
            workload: WorkloadType::OLTP,
            primary_key: vec!["id".to_string()],
            shard_key: Some("id".to_string()),
            estimated_rows: 1_000_000,
            avg_row_size: 100,
            partition_key: None,
            preferred_nodes: Vec::new(),
        };
        tables.push(users_table);

        // Cache the results
        let mut cache = self.cache.write().await;
        for table in &tables {
            cache.tables.insert(table.name.clone(), table.clone());
        }

        // Update refresh time
        let mut last_refresh = self.last_refresh.write().await;
        *last_refresh = Some(std::time::Instant::now());

        Ok(tables)
    }

    /// Discover a specific table's schema
    pub async fn discover_table(&self, table_name: &str) -> Result<TableSchema, DiscoveryError> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(table) = cache.tables.get(table_name) {
                return Ok(table.clone());
            }
        }

        // Query for specific table
        let query = self.build_table_query(table_name);

        // Mock implementation
        Err(DiscoveryError::TableNotFound(table_name.to_string()))
    }

    /// Discover indexes for a table
    pub async fn discover_indexes(&self, table_name: &str) -> Result<Vec<IndexSchema>, DiscoveryError> {
        if !self.config.discover_indexes {
            return Ok(Vec::new());
        }

        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(indexes) = cache.indexes.get(table_name) {
                return Ok(indexes.clone());
            }
        }

        // Mock implementation - return sample indexes
        let indexes = vec![
            IndexSchema {
                name: format!("{}_pkey", table_name),
                table: table_name.to_string(),
                columns: vec!["id".to_string()],
                is_unique: true,
                index_type: IndexType::BTree,
            },
            IndexSchema {
                name: format!("{}_email_idx", table_name),
                table: table_name.to_string(),
                columns: vec!["email".to_string()],
                is_unique: true,
                index_type: IndexType::BTree,
            },
        ];

        // Cache the results
        let mut cache = self.cache.write().await;
        cache.indexes.insert(table_name.to_string(), indexes.clone());

        Ok(indexes)
    }

    /// Discover foreign key relationships
    pub async fn discover_relationships(&self) -> Result<Vec<Relationship>, DiscoveryError> {
        if !self.config.discover_relationships {
            return Ok(Vec::new());
        }

        // Check cache first
        {
            let cache = self.cache.read().await;
            if !cache.relationships.is_empty() {
                return Ok(cache.relationships.clone());
            }
        }

        // Mock implementation
        let relationships = vec![
            Relationship {
                from_table: "orders".to_string(),
                from_column: "user_id".to_string(),
                to_table: "users".to_string(),
                to_column: "id".to_string(),
                relationship_type: RelationshipType::ManyToOne,
            },
            Relationship {
                from_table: "order_items".to_string(),
                from_column: "order_id".to_string(),
                to_table: "orders".to_string(),
                to_column: "id".to_string(),
                relationship_type: RelationshipType::ManyToOne,
            },
        ];

        // Cache the results
        let mut cache = self.cache.write().await;
        cache.relationships = relationships.clone();

        Ok(relationships)
    }

    /// Get table statistics for temperature inference
    pub async fn get_statistics(&self, table_name: &str) -> Result<TableStatistics, DiscoveryError> {
        if !self.config.sample_statistics {
            return Err(DiscoveryError::StatisticsDisabled);
        }

        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(stats) = cache.statistics.get(table_name) {
                return Ok(stats.clone());
            }
        }

        // Mock implementation
        let stats = TableStatistics {
            table_name: table_name.to_string(),
            row_count: 1_000_000,
            size_bytes: 100_000_000,
            index_size_bytes: 20_000_000,
            seq_scan_count: 100,
            idx_scan_count: 50_000,
            n_tup_ins: 1000,
            n_tup_upd: 500,
            n_tup_del: 100,
            last_vacuum: Some("2024-01-15 10:00:00".to_string()),
            last_analyze: Some("2024-01-15 10:00:00".to_string()),
        };

        // Cache the results
        let mut cache = self.cache.write().await;
        cache.statistics.insert(table_name.to_string(), stats.clone());

        Ok(stats)
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

    /// Check if cache needs refresh
    pub async fn needs_refresh(&self) -> bool {
        let last_refresh = self.last_refresh.read().await;
        match *last_refresh {
            None => true,
            Some(time) => time.elapsed() > self.config.refresh_interval,
        }
    }

    /// Refresh the discovery cache
    pub async fn refresh(&self) -> Result<(), DiscoveryError> {
        self.discover().await?;
        if self.config.discover_relationships {
            self.discover_relationships().await?;
        }
        Ok(())
    }

    /// Clear the discovery cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        *cache = DiscoveryCache::default();

        let mut last_refresh = self.last_refresh.write().await;
        *last_refresh = None;
    }

    /// Build SQL queries for discovery
    fn build_discovery_queries(&self) -> Vec<String> {
        let mut queries = Vec::new();

        // Tables query
        let schemas_filter = if self.config.schemas.is_empty() {
            String::new()
        } else {
            let schemas = self.config.schemas.iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");
            format!("AND table_schema IN ({})", schemas)
        };

        queries.push(format!(
            r#"
            SELECT
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            {}
            ORDER BY table_schema, table_name
            "#,
            schemas_filter
        ));

        // Columns query
        queries.push(
            r#"
            SELECT
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            ORDER BY table_schema, table_name, ordinal_position
            "#.to_string()
        );

        // Indexes query (PostgreSQL specific)
        queries.push(
            r#"
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            ORDER BY schemaname, tablename, indexname
            "#.to_string()
        );

        // Foreign keys query
        if self.config.discover_relationships {
            queries.push(
                r#"
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                "#.to_string()
            );
        }

        // Statistics query (PostgreSQL specific)
        if self.config.sample_statistics {
            queries.push(
                r#"
                SELECT
                    schemaname,
                    relname as tablename,
                    n_live_tup as row_count,
                    seq_scan,
                    idx_scan,
                    n_tup_ins,
                    n_tup_upd,
                    n_tup_del,
                    last_vacuum,
                    last_analyze
                FROM pg_stat_user_tables
                "#.to_string()
            );
        }

        queries
    }

    /// Build query for a specific table
    fn build_table_query(&self, table_name: &str) -> String {
        format!(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_name = '{}'
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_name = '{}'
            ORDER BY c.ordinal_position
            "#,
            table_name, table_name
        )
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
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
            Self::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            Self::QueryError(msg) => write!(f, "Query error: {}", msg),
            Self::StatisticsDisabled => write!(f, "Statistics collection is disabled"),
            Self::RefreshFailed(msg) => write!(f, "Cache refresh failed: {}", msg),
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
    async fn test_discover_tables() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        let tables = discovery.discover().await.unwrap();
        assert!(!tables.is_empty());

        let users = tables.iter().find(|t| t.name == "users").unwrap();
        assert_eq!(users.temperature, DataTemperature::Hot);
        assert_eq!(users.workload, WorkloadType::OLTP);
    }

    #[tokio::test]
    async fn test_discover_indexes() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        let indexes = discovery.discover_indexes("users").await.unwrap();
        assert!(!indexes.is_empty());

        let pkey = indexes.iter().find(|i| i.name.ends_with("_pkey")).unwrap();
        assert!(pkey.is_unique);
    }

    #[tokio::test]
    async fn test_discover_relationships() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        let rels = discovery.discover_relationships().await.unwrap();
        assert!(!rels.is_empty());

        let order_user = rels.iter()
            .find(|r| r.from_table == "orders" && r.to_table == "users")
            .unwrap();
        assert_eq!(order_user.relationship_type, RelationshipType::ManyToOne);
    }

    #[tokio::test]
    async fn test_get_statistics() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        let stats = discovery.get_statistics("users").await.unwrap();
        assert_eq!(stats.table_name, "users");
        assert!(stats.row_count > 0);
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
        assert_eq!(discovery.infer_temperature(&hot_stats), DataTemperature::Hot);

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
        assert_eq!(discovery.infer_temperature(&cold_stats), DataTemperature::Cold);
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
        assert_eq!(discovery.infer_access_pattern(&point_stats), AccessPattern::PointLookup);

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
        assert_eq!(discovery.infer_access_pattern(&scan_stats), AccessPattern::FullScan);
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
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        // Populate cache
        discovery.discover().await.unwrap();
        assert!(!discovery.needs_refresh().await);

        // Clear cache
        discovery.clear_cache().await;
        assert!(discovery.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_table_not_found() {
        let config = DiscoveryConfig::default();
        let discovery = SchemaDiscovery::new(config);

        let result = discovery.discover_table("nonexistent_table").await;
        assert!(matches!(result, Err(DiscoveryError::TableNotFound(_))));
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
