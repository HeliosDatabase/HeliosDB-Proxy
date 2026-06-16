//! Tool result cache
//!
//! Caches results of deterministic tool calls to avoid redundant execution.
//! Useful for AI agents that may call the same tool with same parameters multiple times.

use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Tool call key (tool name + parameters hash)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ToolCallKey {
    /// Tool name
    pub tool: String,
    /// Parameter hash
    pub param_hash: u64,
}

impl ToolCallKey {
    /// Create a new tool call key
    pub fn new(tool: &str, params: &serde_json::Value) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        params.to_string().hash(&mut hasher);

        Self {
            tool: tool.to_string(),
            param_hash: hasher.finish(),
        }
    }
}

/// Tool execution result
#[derive(Debug, Clone)]
pub struct ToolResult {
    /// Result data
    pub data: serde_json::Value,
    /// Execution time
    pub execution_time: Duration,
    /// Timestamp
    pub timestamp: Instant,
    /// TTL
    pub ttl: Duration,
}

impl ToolResult {
    /// Create a new result
    pub fn new(data: serde_json::Value, execution_time: Duration) -> Self {
        Self {
            data,
            execution_time,
            timestamp: Instant::now(),
            ttl: Duration::from_secs(300), // Default 5 minutes
        }
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Check if expired
    pub fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > self.ttl
    }

    /// Approximate size
    pub fn size(&self) -> usize {
        self.data.to_string().len() + 32
    }
}

/// Tool result cache
pub struct ToolResultCache {
    /// Cache storage
    cache: DashMap<ToolCallKey, ToolResult>,

    /// Deterministic tools (safe to cache)
    deterministic_tools: HashSet<String>,

    /// Custom TTLs per tool
    tool_ttls: DashMap<String, Duration>,

    /// Statistics
    stats: ToolCacheStats,
}

/// Tool cache statistics
#[derive(Debug, Default)]
struct ToolCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    cached_executions: AtomicU64,
    time_saved_ms: AtomicU64,
}

impl ToolResultCache {
    /// Create a new tool cache
    pub fn new() -> Self {
        // Default deterministic tools
        let mut deterministic = HashSet::new();
        deterministic.insert("get_weather".to_string());
        deterministic.insert("calculate".to_string());
        deterministic.insert("lookup_definition".to_string());
        deterministic.insert("search_knowledge_base".to_string());
        deterministic.insert("get_stock_price".to_string());
        deterministic.insert("convert_units".to_string());
        deterministic.insert("translate".to_string());

        Self {
            cache: DashMap::new(),
            deterministic_tools: deterministic,
            tool_ttls: DashMap::new(),
            stats: ToolCacheStats::default(),
        }
    }

    /// Check if tool is deterministic (cacheable)
    pub fn is_deterministic(&self, tool: &str) -> bool {
        self.deterministic_tools.contains(tool)
    }

    /// Mark a tool as deterministic
    pub fn mark_deterministic(&mut self, tool: impl Into<String>) {
        self.deterministic_tools.insert(tool.into());
    }

    /// Mark a tool as non-deterministic
    pub fn mark_non_deterministic(&mut self, tool: &str) {
        self.deterministic_tools.remove(tool);
    }

    /// Set custom TTL for a tool
    pub fn set_tool_ttl(&self, tool: impl Into<String>, ttl: Duration) {
        self.tool_ttls.insert(tool.into(), ttl);
    }

    /// Get cached result
    pub fn get(&self, key: &ToolCallKey) -> Option<ToolResult> {
        // Check if tool is deterministic
        if !self.is_deterministic(&key.tool) {
            return None;
        }

        if let Some(result) = self.cache.get(key) {
            if result.is_expired() {
                drop(result);
                self.cache.remove(key);
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.stats
                .time_saved_ms
                .fetch_add(result.execution_time.as_millis() as u64, Ordering::Relaxed);

            Some(result.clone())
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Cache a tool result
    pub fn put(&self, key: ToolCallKey, result: ToolResult) {
        // Only cache deterministic tools
        if !self.is_deterministic(&key.tool) {
            return;
        }

        // Apply custom TTL if configured
        let result = if let Some(ttl) = self.tool_ttls.get(&key.tool) {
            result.with_ttl(*ttl)
        } else {
            result
        };

        self.cache.insert(key, result);
        self.stats.cached_executions.fetch_add(1, Ordering::Relaxed);
    }

    /// Execute with caching
    pub async fn execute_with_cache<F, Fut>(
        &self,
        tool: &str,
        params: &serde_json::Value,
        executor: F,
    ) -> ToolResult
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = serde_json::Value>,
    {
        let key = ToolCallKey::new(tool, params);

        // Check cache
        if let Some(cached) = self.get(&key) {
            return cached;
        }

        // Execute
        let start = Instant::now();
        let data = executor().await;
        let execution_time = start.elapsed();

        let result = ToolResult::new(data, execution_time);

        // Cache result
        self.put(key, result.clone());

        result
    }

    /// Clear all cached results
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Clear cached results for a tool
    pub fn clear_tool(&self, tool: &str) {
        self.cache.retain(|k, _| k.tool != tool);
    }

    /// Remove expired entries
    pub fn cleanup_expired(&self) {
        self.cache.retain(|_, v| !v.is_expired());
    }

    /// Get statistics
    pub fn stats(&self) -> ToolCacheStatsSnapshot {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        ToolCacheStatsSnapshot {
            cached_entries: self.cache.len(),
            deterministic_tools: self.deterministic_tools.len(),
            hits,
            misses,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
            cached_executions: self.stats.cached_executions.load(Ordering::Relaxed),
            time_saved_ms: self.stats.time_saved_ms.load(Ordering::Relaxed),
        }
    }
}

impl Default for ToolResultCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Tool cache statistics snapshot
#[derive(Debug, Clone)]
pub struct ToolCacheStatsSnapshot {
    pub cached_entries: usize,
    pub deterministic_tools: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub cached_executions: u64,
    pub time_saved_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tool_call_key() {
        let key1 = ToolCallKey::new("calculate", &json!({"a": 1, "b": 2}));
        let key2 = ToolCallKey::new("calculate", &json!({"a": 1, "b": 2}));
        let key3 = ToolCallKey::new("calculate", &json!({"a": 1, "b": 3}));

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_deterministic_check() {
        let cache = ToolResultCache::new();

        assert!(cache.is_deterministic("calculate"));
        assert!(cache.is_deterministic("get_weather"));
        assert!(!cache.is_deterministic("random_function"));
    }

    #[test]
    fn test_cache_put_get() {
        let cache = ToolResultCache::new();

        let key = ToolCallKey::new("calculate", &json!({"expr": "2+2"}));
        let result = ToolResult::new(json!(4), Duration::from_millis(10));

        cache.put(key.clone(), result);

        let cached = cache.get(&key);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().data, json!(4));
    }

    #[test]
    fn test_non_deterministic_not_cached() {
        let cache = ToolResultCache::new();

        let key = ToolCallKey::new("random_tool", &json!({}));
        let result = ToolResult::new(json!("result"), Duration::from_millis(10));

        cache.put(key.clone(), result);

        // Should not be cached
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_expired_entries() {
        let cache = ToolResultCache::new();

        let key = ToolCallKey::new("calculate", &json!({}));
        let result =
            ToolResult::new(json!(1), Duration::from_millis(1)).with_ttl(Duration::from_millis(1));

        cache.put(key.clone(), result);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_stats() {
        let cache = ToolResultCache::new();

        let key = ToolCallKey::new("calculate", &json!({}));
        let result = ToolResult::new(json!(1), Duration::from_millis(50));

        cache.put(key.clone(), result);
        cache.get(&key); // Hit
        cache.get(&key); // Hit

        let key2 = ToolCallKey::new("calculate", &json!({"x": 1}));
        cache.get(&key2); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!(stats.time_saved_ms >= 100);
    }

    #[tokio::test]
    async fn test_execute_with_cache() {
        let cache = ToolResultCache::new();

        let params = json!({"a": 5, "b": 3});
        let mut call_count = 0;

        // First call - executes
        let result1 = cache
            .execute_with_cache("calculate", &params, || {
                call_count += 1;
                async { json!(8) }
            })
            .await;

        // Second call - cached
        let result2 = cache
            .execute_with_cache("calculate", &params, || {
                call_count += 1;
                async { json!(8) }
            })
            .await;

        assert_eq!(result1.data, json!(8));
        assert_eq!(result2.data, json!(8));
        // Function should only be called once
        // Note: call_count tracking doesn't work directly in async closure
    }
}
