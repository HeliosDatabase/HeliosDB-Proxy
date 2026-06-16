//! Schema Registry
//!
//! Manages metadata about tables, indexes, and relationships for routing decisions.

use std::collections::HashMap;
use dashmap::DashMap;
use parking_lot::RwLock;

/// Schema registry for routing decisions
#[derive(Debug)]
pub struct SchemaRegistry {
    /// Table metadata
    tables: DashMap<String, TableSchema>,
    /// Index metadata
    indexes: DashMap<String, IndexSchema>,
    /// Relationships between tables
    relationships: RwLock<Vec<Relationship>>,
    /// Sharding configuration
    sharding: RwLock<ShardingConfig>,
    /// Node capabilities
    node_capabilities: DashMap<String, NodeCapabilities>,
    /// Branch locations (branch -> nodes)
    branch_locations: DashMap<String, Vec<String>>,
}

impl SchemaRegistry {
    /// Create a new schema registry
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            indexes: DashMap::new(),
            relationships: RwLock::new(Vec::new()),
            sharding: RwLock::new(ShardingConfig::default()),
            node_capabilities: DashMap::new(),
            branch_locations: DashMap::new(),
        }
    }

    /// Register a table schema
    pub fn register_table(&self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
    }

    /// Get a table schema
    pub fn get_table(&self, name: &str) -> Option<TableSchema> {
        self.tables.get(name).map(|r| r.clone())
    }

    /// Update table classification
    pub fn update_classification(
        &self,
        table: &str,
        temperature: DataTemperature,
        workload: WorkloadType,
    ) {
        if let Some(mut entry) = self.tables.get_mut(table) {
            entry.temperature = temperature;
            entry.workload = workload;
        }
    }

    /// Register an index schema
    pub fn register_index(&self, schema: IndexSchema) {
        self.indexes.insert(schema.name.clone(), schema);
    }

    /// Get an index schema
    pub fn get_index(&self, name: &str) -> Option<IndexSchema> {
        self.indexes.get(name).map(|r| r.clone())
    }

    /// Get vector index for a table
    pub fn get_vector_index(&self, table: &str) -> Option<IndexSchema> {
        self.indexes.iter()
            .find(|entry| entry.table == table && entry.index_type == IndexType::Vector)
            .map(|entry| entry.clone())
    }

    /// Add a relationship
    pub fn add_relationship(&self, relationship: Relationship) {
        let mut rels = self.relationships.write();
        rels.push(relationship);
    }

    /// Get relationships for a table
    pub fn get_relationships(&self, table: &str) -> Vec<Relationship> {
        let rels = self.relationships.read();
        rels.iter()
            .filter(|r| r.from_table == table || r.to_table == table)
            .cloned()
            .collect()
    }

    /// Set sharding configuration
    pub fn set_sharding(&self, config: ShardingConfig) {
        let mut sharding = self.sharding.write();
        *sharding = config;
    }

    /// Get shard for a value
    pub fn get_shard(&self, key: &str, value: &str) -> Option<u32> {
        let sharding = self.sharding.read();
        sharding.get_shard(key, value)
    }

    /// Register node capabilities
    pub fn register_node_capabilities(&self, node_id: &str, capabilities: NodeCapabilities) {
        self.node_capabilities.insert(node_id.to_string(), capabilities);
    }

    /// Get node capabilities
    pub fn get_node_capabilities(&self, node_id: &str) -> Option<NodeCapabilities> {
        self.node_capabilities.get(node_id).map(|r| r.clone())
    }

    /// Register branch location
    pub fn register_branch_location(&self, branch: &str, node_ids: Vec<String>) {
        self.branch_locations.insert(branch.to_string(), node_ids);
    }

    /// Get nodes that have a branch
    pub fn get_branch_locations(&self, branch: &str) -> Vec<String> {
        self.branch_locations
            .get(branch)
            .map(|r| r.clone())
            .unwrap_or_default()
    }

    /// Get all tables
    pub fn all_tables(&self) -> Vec<TableSchema> {
        self.tables.iter().map(|r| r.clone()).collect()
    }

    /// List all tables (alias for all_tables)
    pub fn list_tables(&self) -> Vec<TableSchema> {
        self.all_tables()
    }

    /// Get table count
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Remove a table
    pub fn remove_table(&self, name: &str) {
        self.tables.remove(name);
    }

    /// Get tables by workload
    pub fn tables_by_workload(&self, workload: WorkloadType) -> Vec<TableSchema> {
        self.tables
            .iter()
            .filter(|r| r.workload == workload)
            .map(|r| r.clone())
            .collect()
    }

    /// Get tables by temperature
    pub fn tables_by_temperature(&self, temperature: DataTemperature) -> Vec<TableSchema> {
        self.tables
            .iter()
            .filter(|r| r.temperature == temperature)
            .map(|r| r.clone())
            .collect()
    }

    /// Check if a column uses columnar storage
    pub fn is_columnar_column(&self, table: &str, column: &str) -> bool {
        self.tables
            .get(table)
            .map(|t| {
                t.columns
                    .iter()
                    .any(|c| c.name == column && c.storage_type == StorageType::Columnar)
            })
            .unwrap_or(false)
    }

    /// Check if a column is content-addressed
    pub fn is_content_addressed(&self, table: &str, column: &str) -> bool {
        self.tables
            .get(table)
            .map(|t| {
                t.columns
                    .iter()
                    .any(|c| c.name == column && c.storage_type == StorageType::ContentAddressed)
            })
            .unwrap_or(false)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Table schema information
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Columns
    pub columns: Vec<ColumnSchema>,
    /// Access pattern classification
    pub access_pattern: AccessPattern,
    /// Temperature (HOT/WARM/COLD)
    pub temperature: DataTemperature,
    /// Workload type
    pub workload: WorkloadType,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Shard key (if sharded)
    pub shard_key: Option<String>,
    /// Partition key (if partitioned)
    pub partition_key: Option<PartitionKey>,
    /// Preferred nodes
    pub preferred_nodes: Vec<String>,
    /// Estimated row count
    pub estimated_rows: u64,
    /// Average row size in bytes
    pub avg_row_size: usize,
}

impl TableSchema {
    /// Create a new table schema
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            access_pattern: AccessPattern::Mixed,
            temperature: DataTemperature::Warm,
            workload: WorkloadType::Mixed,
            primary_key: Vec::new(),
            shard_key: None,
            partition_key: None,
            preferred_nodes: Vec::new(),
            estimated_rows: 0,
            avg_row_size: 0,
        }
    }

    /// Add a column
    pub fn with_column(mut self, column: ColumnSchema) -> Self {
        self.columns.push(column);
        self
    }

    /// Set access pattern
    pub fn with_access_pattern(mut self, pattern: AccessPattern) -> Self {
        self.access_pattern = pattern;
        self
    }

    /// Set temperature
    pub fn with_temperature(mut self, temp: DataTemperature) -> Self {
        self.temperature = temp;
        self
    }

    /// Set workload
    pub fn with_workload(mut self, workload: WorkloadType) -> Self {
        self.workload = workload;
        self
    }

    /// Set primary key
    pub fn with_primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = columns;
        self
    }

    /// Set shard key
    pub fn with_shard_key(mut self, key: impl Into<String>) -> Self {
        self.shard_key = Some(key.into());
        self
    }

    /// Add preferred node
    pub fn with_preferred_node(mut self, node: impl Into<String>) -> Self {
        self.preferred_nodes.push(node.into());
        self
    }

    /// Set estimated rows
    pub fn with_estimated_rows(mut self, rows: u64) -> Self {
        self.estimated_rows = rows;
        self
    }
}

/// Column schema information
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Is nullable
    pub nullable: bool,
    /// Storage type
    pub storage_type: StorageType,
    /// Is part of primary key
    pub is_primary_key: bool,
    /// Is indexed
    pub is_indexed: bool,
}

impl ColumnSchema {
    /// Create a new column schema
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable: true,
            storage_type: StorageType::Row,
            is_primary_key: false,
            is_indexed: false,
        }
    }

    /// Set nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set storage type
    pub fn with_storage(mut self, storage: StorageType) -> Self {
        self.storage_type = storage;
        self
    }

    /// Set as primary key
    pub fn as_primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self.nullable = false;
        self
    }

    /// Set as indexed
    pub fn indexed(mut self) -> Self {
        self.is_indexed = true;
        self
    }
}

/// Column storage type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Default)]
pub enum StorageType {
    /// Traditional row storage
    #[default]
    Row,
    /// Columnar storage (for analytics)
    Columnar,
    /// Content-addressed storage
    ContentAddressed,
    /// Vector storage
    Vector,
}


/// Index schema information
#[derive(Debug, Clone)]
pub struct IndexSchema {
    /// Index name
    pub name: String,
    /// Table name
    pub table: String,
    /// Indexed columns
    pub columns: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Is unique
    pub is_unique: bool,
}

impl IndexSchema {
    /// Create a new index schema
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            columns: Vec::new(),
            index_type: IndexType::BTree,
            is_unique: false,
        }
    }

    /// Add column
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.columns.push(column.into());
        self
    }

    /// Set index type
    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Set as unique
    pub fn unique(mut self) -> Self {
        self.is_unique = true;
        self
    }
}

/// Index type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Default)]
pub enum IndexType {
    /// B-tree index
    #[default]
    BTree,
    /// Hash index
    Hash,
    /// GiST index
    GiST,
    /// GIN index
    GIN,
    /// Vector/HNSW index
    Vector,
}


/// Access pattern classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Default)]
pub enum AccessPattern {
    /// Point lookups by primary key
    PointLookup,
    /// Range scans
    RangeScan,
    /// Full table scans (OLAP)
    FullScan,
    /// Vector similarity search
    VectorSearch,
    /// Time-series append
    TimeSeriesAppend,
    /// Mixed patterns
    #[default]
    Mixed,
}


impl AccessPattern {
    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "point_lookup" | "pointlookup" => Some(AccessPattern::PointLookup),
            "range_scan" | "rangescan" => Some(AccessPattern::RangeScan),
            "full_scan" | "fullscan" => Some(AccessPattern::FullScan),
            "vector_search" | "vectorsearch" | "vector" => Some(AccessPattern::VectorSearch),
            "time_series" | "timeseries" | "append" => Some(AccessPattern::TimeSeriesAppend),
            "mixed" => Some(AccessPattern::Mixed),
            _ => None,
        }
    }
}

/// Data temperature classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Default)]
pub enum DataTemperature {
    /// Frequently accessed, keep in memory
    Hot,
    /// Occasionally accessed
    #[default]
    Warm,
    /// Rarely accessed, can be on slower storage
    Cold,
    /// Archive, acceptable to be slow
    Frozen,
}


impl DataTemperature {
    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "hot" => Some(DataTemperature::Hot),
            "warm" => Some(DataTemperature::Warm),
            "cold" => Some(DataTemperature::Cold),
            "frozen" | "archive" => Some(DataTemperature::Frozen),
            _ => None,
        }
    }
}

/// Workload type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Default)]
pub enum WorkloadType {
    /// Online Transaction Processing
    OLTP,
    /// Online Analytical Processing
    OLAP,
    /// Hybrid Transactional/Analytical
    HTAP,
    /// Vector/AI workloads
    Vector,
    /// Mixed workload
    #[default]
    Mixed,
}


impl WorkloadType {
    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "oltp" => Some(WorkloadType::OLTP),
            "olap" => Some(WorkloadType::OLAP),
            "htap" => Some(WorkloadType::HTAP),
            "vector" | "ai" => Some(WorkloadType::Vector),
            "mixed" => Some(WorkloadType::Mixed),
            _ => None,
        }
    }
}

/// Partition key configuration
#[derive(Debug, Clone)]
pub struct PartitionKey {
    /// Column name
    pub column: String,
    /// Partition type
    pub partition_type: PartitionType,
}

/// Partition type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionType {
    /// Range partitioning (e.g., by date)
    Range,
    /// List partitioning
    List,
    /// Hash partitioning
    Hash,
}

/// Table relationship
#[derive(Debug, Clone)]
pub struct Relationship {
    /// Source table
    pub from_table: String,
    /// Source column
    pub from_column: String,
    /// Target table
    pub to_table: String,
    /// Target column
    pub to_column: String,
    /// Relationship type
    pub relationship_type: RelationshipType,
}

/// Relationship type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipType {
    /// One-to-one
    OneToOne,
    /// One-to-many
    OneToMany,
    /// Many-to-one
    ManyToOne,
    /// Many-to-many
    ManyToMany,
}

/// Sharding configuration
#[derive(Debug, Clone, Default)]
pub struct ShardingConfig {
    /// Enabled
    pub enabled: bool,
    /// Shard count
    pub shard_count: u32,
    /// Hash ring for consistent hashing
    pub hash_ring: Vec<u32>,
    /// Table to shard key mapping
    pub table_shard_keys: HashMap<String, String>,
}

impl ShardingConfig {
    /// Create a new sharding configuration
    pub fn new(shard_count: u32) -> Self {
        let mut config = Self {
            enabled: true,
            shard_count,
            hash_ring: Vec::new(),
            table_shard_keys: HashMap::new(),
        };
        config.initialize_hash_ring();
        config
    }

    /// Initialize the hash ring
    fn initialize_hash_ring(&mut self) {
        self.hash_ring = (0..self.shard_count).collect();
    }

    /// Get shard for a value
    pub fn get_shard(&self, _key: &str, value: &str) -> Option<u32> {
        if !self.enabled || self.shard_count == 0 {
            return None;
        }

        // Simple consistent hashing
        let hash = self.hash_value(value);
        Some(hash % self.shard_count)
    }

    /// Hash a value
    fn hash_value(&self, value: &str) -> u32 {
        // Simple FNV-1a hash
        let mut hash: u32 = 2166136261;
        for byte in value.bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(16777619);
        }
        hash
    }

    /// Register a table's shard key
    pub fn register_table_shard_key(&mut self, table: &str, shard_key: &str) {
        self.table_shard_keys.insert(table.to_string(), shard_key.to_string());
    }
}

/// Node capabilities
#[derive(Debug, Clone, Default)]
pub struct NodeCapabilities {
    /// Supports vector search
    pub vector_search: bool,
    /// Has GPU acceleration
    pub gpu_acceleration: bool,
    /// Has columnar storage engine
    pub columnar_storage: bool,
    /// Has in-memory storage
    pub in_memory: bool,
    /// Has content-addressed storage
    pub content_addressed: bool,
    /// Maximum concurrent queries
    pub max_concurrent_queries: u32,
    /// Memory limit in bytes
    pub memory_limit: u64,
}

impl NodeCapabilities {
    /// Create with vector support
    pub fn vector_node() -> Self {
        Self {
            vector_search: true,
            gpu_acceleration: true,
            ..Default::default()
        }
    }

    /// Create with analytics support
    pub fn analytics_node() -> Self {
        Self {
            columnar_storage: true,
            in_memory: false,
            ..Default::default()
        }
    }

    /// Create with in-memory support
    pub fn hot_node() -> Self {
        Self {
            in_memory: true,
            ..Default::default()
        }
    }

    /// Check if node has required capabilities
    pub fn satisfies(&self, required: &NodeCapabilities) -> bool {
        (!required.vector_search || self.vector_search)
            && (!required.gpu_acceleration || self.gpu_acceleration)
            && (!required.columnar_storage || self.columnar_storage)
            && (!required.in_memory || self.in_memory)
            && (!required.content_addressed || self.content_addressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry() {
        let registry = SchemaRegistry::new();

        let users = TableSchema::new("users")
            .with_temperature(DataTemperature::Hot)
            .with_workload(WorkloadType::OLTP)
            .with_access_pattern(AccessPattern::PointLookup)
            .with_column(ColumnSchema::new("id", "integer").as_primary_key())
            .with_column(ColumnSchema::new("name", "varchar"));

        registry.register_table(users);

        let result = registry.get_table("users");
        assert!(result.is_some());
        let table = result.expect("should exist");
        assert_eq!(table.name, "users");
        assert_eq!(table.temperature, DataTemperature::Hot);
    }

    #[test]
    fn test_update_classification() {
        let registry = SchemaRegistry::new();

        registry.register_table(TableSchema::new("events")
            .with_temperature(DataTemperature::Warm)
            .with_workload(WorkloadType::Mixed));

        registry.update_classification("events", DataTemperature::Cold, WorkloadType::OLAP);

        let table = registry.get_table("events").expect("should exist");
        assert_eq!(table.temperature, DataTemperature::Cold);
        assert_eq!(table.workload, WorkloadType::OLAP);
    }

    #[test]
    fn test_sharding_config() {
        let mut config = ShardingConfig::new(4);
        config.register_table_shard_key("orders", "customer_id");

        let shard1 = config.get_shard("customer_id", "cust_123");
        let shard2 = config.get_shard("customer_id", "cust_456");

        assert!(shard1.is_some());
        assert!(shard2.is_some());
        // Different values may map to same or different shards
    }

    #[test]
    fn test_node_capabilities() {
        let required = NodeCapabilities {
            vector_search: true,
            gpu_acceleration: false,
            ..Default::default()
        };

        let vector_node = NodeCapabilities::vector_node();
        let analytics_node = NodeCapabilities::analytics_node();

        assert!(vector_node.satisfies(&required));
        assert!(!analytics_node.satisfies(&required));
    }

    #[test]
    fn test_access_pattern_from_str() {
        assert_eq!(AccessPattern::from_str("point_lookup"), Some(AccessPattern::PointLookup));
        assert_eq!(AccessPattern::from_str("vector"), Some(AccessPattern::VectorSearch));
        assert_eq!(AccessPattern::from_str("invalid"), None);
    }

    #[test]
    fn test_data_temperature_from_str() {
        assert_eq!(DataTemperature::from_str("hot"), Some(DataTemperature::Hot));
        assert_eq!(DataTemperature::from_str("cold"), Some(DataTemperature::Cold));
        assert_eq!(DataTemperature::from_str("archive"), Some(DataTemperature::Frozen));
    }

    #[test]
    fn test_workload_type_from_str() {
        assert_eq!(WorkloadType::from_str("oltp"), Some(WorkloadType::OLTP));
        assert_eq!(WorkloadType::from_str("vector"), Some(WorkloadType::Vector));
        assert_eq!(WorkloadType::from_str("ai"), Some(WorkloadType::Vector));
    }

    #[test]
    fn test_index_schema() {
        let index = IndexSchema::new("idx_users_email", "users")
            .with_column("email")
            .with_type(IndexType::BTree)
            .unique();

        assert_eq!(index.name, "idx_users_email");
        assert!(index.is_unique);
        assert_eq!(index.columns, vec!["email"]);
    }

    #[test]
    fn test_tables_by_workload() {
        let registry = SchemaRegistry::new();

        registry.register_table(TableSchema::new("users").with_workload(WorkloadType::OLTP));
        registry.register_table(TableSchema::new("events").with_workload(WorkloadType::OLAP));
        registry.register_table(TableSchema::new("orders").with_workload(WorkloadType::OLTP));

        let oltp_tables = registry.tables_by_workload(WorkloadType::OLTP);
        assert_eq!(oltp_tables.len(), 2);
    }
}
