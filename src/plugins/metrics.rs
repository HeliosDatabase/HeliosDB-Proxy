//! Plugin Metrics
//!
//! Metrics collection and reporting for the plugin system.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use super::HookType;

/// Plugin metrics collector
pub struct PluginMetrics {
    /// Per-plugin statistics. DashMap so concurrent hook calls for
    /// different plugins land on different shards instead of serializing
    /// on one global write lock.
    plugin_stats: DashMap<String, PluginStatsInner>,

    /// Global counters
    global: GlobalMetrics,

    /// Hook latency histograms (sharded per hook type).
    hook_latencies: DashMap<HookType, LatencyHistogram>,

    /// Creation time
    created_at: Instant,
}

impl PluginMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            plugin_stats: DashMap::new(),
            global: GlobalMetrics::new(),
            hook_latencies: DashMap::new(),
            created_at: Instant::now(),
        }
    }

    /// Record a hook call
    pub fn record_hook_call(
        &self,
        plugin_name: &str,
        hook: HookType,
        latency: Duration,
        success: bool,
    ) {
        // Update global counters
        self.global.total_calls.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.global.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        // Update plugin-specific stats
        {
            let mut entry = self
                .plugin_stats
                .entry(plugin_name.to_string())
                .or_insert_with(PluginStatsInner::new);

            entry.total_calls += 1;
            if success {
                entry.successful_calls += 1;
            } else {
                entry.failed_calls += 1;
            }
            entry.total_latency += latency;

            if latency > entry.max_latency {
                entry.max_latency = latency;
            }
            if entry.min_latency == Duration::ZERO || latency < entry.min_latency {
                entry.min_latency = latency;
            }

            // Update per-hook stats
            let hook_entry = entry
                .hook_stats
                .entry(hook)
                .or_insert_with(HookStatsInner::new);
            hook_entry.calls += 1;
            hook_entry.latency += latency;
            if !success {
                hook_entry.errors += 1;
            }
        }

        // Update hook latency histogram
        {
            let mut histogram = self
                .hook_latencies
                .entry(hook)
                .or_insert_with(LatencyHistogram::new);
            histogram.record(latency);
        }
    }

    /// Record a plugin load
    pub fn record_plugin_load(&self, plugin_name: &str) {
        self.global.plugins_loaded.fetch_add(1, Ordering::Relaxed);

        let mut entry = self
            .plugin_stats
            .entry(plugin_name.to_string())
            .or_insert_with(PluginStatsInner::new);
        entry.loaded_at = Some(Instant::now());
    }

    /// Record a plugin unload
    pub fn record_plugin_unload(&self, plugin_name: &str) {
        self.global.plugins_unloaded.fetch_add(1, Ordering::Relaxed);

        if let Some(mut entry) = self.plugin_stats.get_mut(plugin_name) {
            entry.unloaded_at = Some(Instant::now());
        }
    }

    /// Record a plugin error
    pub fn record_plugin_error(&self, plugin_name: &str, _error: &str) {
        self.global.total_errors.fetch_add(1, Ordering::Relaxed);

        let mut entry = self
            .plugin_stats
            .entry(plugin_name.to_string())
            .or_insert_with(PluginStatsInner::new);
        entry.error_count += 1;
    }

    /// Get plugin statistics
    pub fn get_plugin_stats(&self, plugin_name: &str) -> PluginStats {
        self.plugin_stats
            .get(plugin_name)
            .map(|s| s.to_public())
            .unwrap_or_default()
    }

    /// Get all plugin statistics
    pub fn get_all_stats(&self) -> HashMap<String, PluginStats> {
        self.plugin_stats
            .iter()
            .map(|e| (e.key().clone(), e.value().to_public()))
            .collect()
    }

    /// Get total calls
    pub fn total_calls(&self) -> u64 {
        self.global.total_calls.load(Ordering::Relaxed)
    }

    /// Get total errors
    pub fn total_errors(&self) -> u64 {
        self.global.total_errors.load(Ordering::Relaxed)
    }

    /// Get average latency across all plugins
    pub fn avg_latency(&self) -> Duration {
        let mut total_latency = Duration::ZERO;
        let mut total_calls = 0u64;

        for s in self.plugin_stats.iter() {
            total_latency += s.total_latency;
            total_calls += s.total_calls;
        }

        if total_calls == 0 {
            Duration::ZERO
        } else {
            total_latency / total_calls as u32
        }
    }

    /// Get hook latency
    pub fn get_hook_latency(&self, hook: HookType) -> HookLatency {
        self.hook_latencies
            .get(&hook)
            .map(|h| h.to_latency())
            .unwrap_or_default()
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.global.total_calls.store(0, Ordering::Relaxed);
        self.global.total_errors.store(0, Ordering::Relaxed);
        self.plugin_stats.clear();
        self.hook_latencies.clear();
    }
}

impl Default for PluginMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global metrics
struct GlobalMetrics {
    total_calls: AtomicU64,
    total_errors: AtomicU64,
    plugins_loaded: AtomicU64,
    plugins_unloaded: AtomicU64,
}

impl GlobalMetrics {
    fn new() -> Self {
        Self {
            total_calls: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            plugins_loaded: AtomicU64::new(0),
            plugins_unloaded: AtomicU64::new(0),
        }
    }
}

/// Internal plugin statistics
struct PluginStatsInner {
    total_calls: u64,
    successful_calls: u64,
    failed_calls: u64,
    error_count: u64,
    total_latency: Duration,
    min_latency: Duration,
    max_latency: Duration,
    hook_stats: HashMap<HookType, HookStatsInner>,
    loaded_at: Option<Instant>,
    unloaded_at: Option<Instant>,
}

impl PluginStatsInner {
    fn new() -> Self {
        Self {
            total_calls: 0,
            successful_calls: 0,
            failed_calls: 0,
            error_count: 0,
            total_latency: Duration::ZERO,
            min_latency: Duration::ZERO,
            max_latency: Duration::ZERO,
            hook_stats: HashMap::new(),
            loaded_at: None,
            unloaded_at: None,
        }
    }

    fn to_public(&self) -> PluginStats {
        PluginStats {
            total_calls: self.total_calls,
            successful_calls: self.successful_calls,
            failed_calls: self.failed_calls,
            error_count: self.error_count,
            avg_latency: if self.total_calls > 0 {
                self.total_latency / self.total_calls as u32
            } else {
                Duration::ZERO
            },
            min_latency: self.min_latency,
            max_latency: self.max_latency,
            uptime: self.loaded_at.map(|t| t.elapsed()),
        }
    }
}

/// Internal hook statistics
struct HookStatsInner {
    calls: u64,
    errors: u64,
    latency: Duration,
}

impl HookStatsInner {
    fn new() -> Self {
        Self {
            calls: 0,
            errors: 0,
            latency: Duration::ZERO,
        }
    }
}

/// Public plugin statistics
#[derive(Debug, Clone, Default)]
pub struct PluginStats {
    /// Total calls
    pub total_calls: u64,

    /// Successful calls
    pub successful_calls: u64,

    /// Failed calls
    pub failed_calls: u64,

    /// Error count
    pub error_count: u64,

    /// Average latency
    pub avg_latency: Duration,

    /// Minimum latency
    pub min_latency: Duration,

    /// Maximum latency
    pub max_latency: Duration,

    /// Plugin uptime
    pub uptime: Option<Duration>,
}

impl PluginStats {
    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_calls == 0 {
            1.0
        } else {
            self.successful_calls as f64 / self.total_calls as f64
        }
    }
}

/// Hook latency statistics
#[derive(Debug, Clone, Default)]
pub struct HookLatency {
    /// Total calls
    pub count: u64,

    /// Average latency
    pub avg: Duration,

    /// P50 latency
    pub p50: Duration,

    /// P90 latency
    pub p90: Duration,

    /// P99 latency
    pub p99: Duration,

    /// Maximum latency
    pub max: Duration,
}

/// Latency histogram
struct LatencyHistogram {
    /// Recorded latencies (sorted)
    latencies: Vec<Duration>,

    /// Maximum latency
    max: Duration,

    /// Sum for average
    sum: Duration,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            latencies: Vec::new(),
            max: Duration::ZERO,
            sum: Duration::ZERO,
        }
    }

    fn record(&mut self, latency: Duration) {
        self.latencies.push(latency);
        self.sum += latency;
        if latency > self.max {
            self.max = latency;
        }

        // Keep sorted for percentile calculations
        // In production, would use a more efficient data structure
        self.latencies.sort();

        // Limit size to prevent memory growth
        if self.latencies.len() > 10000 {
            self.latencies.drain(0..5000);
        }
    }

    fn percentile(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((self.latencies.len() as f64) * p / 100.0) as usize;
        let idx = idx.min(self.latencies.len() - 1);
        self.latencies[idx]
    }

    fn to_latency(&self) -> HookLatency {
        HookLatency {
            count: self.latencies.len() as u64,
            avg: if self.latencies.is_empty() {
                Duration::ZERO
            } else {
                self.sum / self.latencies.len() as u32
            },
            p50: self.percentile(50.0),
            p90: self.percentile(90.0),
            p99: self.percentile(99.0),
            max: self.max,
        }
    }
}

/// Metrics exporter for Prometheus format
pub struct MetricsExporter {
    metrics: std::sync::Arc<PluginMetrics>,
    prefix: String,
}

impl MetricsExporter {
    /// Create a new exporter
    pub fn new(metrics: std::sync::Arc<PluginMetrics>, prefix: &str) -> Self {
        Self {
            metrics,
            prefix: prefix.to_string(),
        }
    }

    /// Export metrics in Prometheus format
    pub fn export(&self) -> String {
        let mut output = String::new();

        // Global metrics
        output.push_str(&format!(
            "# HELP {}_total_calls Total hook calls\n",
            self.prefix
        ));
        output.push_str(&format!("# TYPE {}_total_calls counter\n", self.prefix));
        output.push_str(&format!(
            "{}_total_calls {}\n",
            self.prefix,
            self.metrics.total_calls()
        ));

        output.push_str(&format!(
            "# HELP {}_total_errors Total errors\n",
            self.prefix
        ));
        output.push_str(&format!("# TYPE {}_total_errors counter\n", self.prefix));
        output.push_str(&format!(
            "{}_total_errors {}\n",
            self.prefix,
            self.metrics.total_errors()
        ));

        // Per-plugin metrics
        let all_stats = self.metrics.get_all_stats();
        for (name, stats) in all_stats {
            let name_label = name.replace('-', "_");

            output.push_str(&format!(
                "{}_plugin_calls{{plugin=\"{}\"}} {}\n",
                self.prefix, name_label, stats.total_calls
            ));

            output.push_str(&format!(
                "{}_plugin_errors{{plugin=\"{}\"}} {}\n",
                self.prefix, name_label, stats.error_count
            ));

            output.push_str(&format!(
                "{}_plugin_latency_avg_us{{plugin=\"{}\"}} {}\n",
                self.prefix,
                name_label,
                stats.avg_latency.as_micros()
            ));
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_metrics_new() {
        let metrics = PluginMetrics::new();
        assert_eq!(metrics.total_calls(), 0);
        assert_eq!(metrics.total_errors(), 0);
    }

    #[test]
    fn test_record_hook_call() {
        let metrics = PluginMetrics::new();

        metrics.record_hook_call(
            "test-plugin",
            HookType::PreQuery,
            Duration::from_micros(50),
            true,
        );

        assert_eq!(metrics.total_calls(), 1);
        assert_eq!(metrics.total_errors(), 0);

        let stats = metrics.get_plugin_stats("test-plugin");
        assert_eq!(stats.total_calls, 1);
        assert_eq!(stats.successful_calls, 1);
    }

    #[test]
    fn test_record_hook_call_error() {
        let metrics = PluginMetrics::new();

        metrics.record_hook_call(
            "test-plugin",
            HookType::PreQuery,
            Duration::from_micros(50),
            false,
        );

        assert_eq!(metrics.total_calls(), 1);
        assert_eq!(metrics.total_errors(), 1);

        let stats = metrics.get_plugin_stats("test-plugin");
        assert_eq!(stats.failed_calls, 1);
    }

    #[test]
    fn test_plugin_stats_success_rate() {
        let stats = PluginStats {
            total_calls: 100,
            successful_calls: 90,
            failed_calls: 10,
            ..Default::default()
        };

        assert!((stats.success_rate() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_plugin_stats_default() {
        let stats = PluginStats::default();
        assert_eq!(stats.total_calls, 0);
        assert_eq!(stats.success_rate(), 1.0);
    }

    #[test]
    fn test_latency_histogram() {
        let mut histogram = LatencyHistogram::new();

        for i in 1..=100 {
            histogram.record(Duration::from_micros(i));
        }

        let latency = histogram.to_latency();
        assert_eq!(latency.count, 100);
        assert!(latency.p50 >= Duration::from_micros(50));
        assert!(latency.p99 >= Duration::from_micros(99));
    }

    #[test]
    fn test_get_hook_latency() {
        let metrics = PluginMetrics::new();

        for i in 1..=10 {
            metrics.record_hook_call(
                "test",
                HookType::PreQuery,
                Duration::from_micros(i * 10),
                true,
            );
        }

        let latency = metrics.get_hook_latency(HookType::PreQuery);
        assert_eq!(latency.count, 10);
        assert!(latency.avg > Duration::ZERO);
    }

    #[test]
    fn test_avg_latency() {
        let metrics = PluginMetrics::new();

        metrics.record_hook_call("p1", HookType::PreQuery, Duration::from_micros(100), true);
        metrics.record_hook_call("p1", HookType::PreQuery, Duration::from_micros(200), true);

        let avg = metrics.avg_latency();
        assert_eq!(avg, Duration::from_micros(150));
    }

    #[test]
    fn test_reset() {
        let metrics = PluginMetrics::new();

        metrics.record_hook_call("test", HookType::PreQuery, Duration::from_micros(50), true);
        assert_eq!(metrics.total_calls(), 1);

        metrics.reset();
        assert_eq!(metrics.total_calls(), 0);
    }

    #[test]
    fn test_metrics_exporter() {
        let metrics = std::sync::Arc::new(PluginMetrics::new());

        metrics.record_hook_call("test", HookType::PreQuery, Duration::from_micros(50), true);

        let exporter = MetricsExporter::new(metrics, "helios_plugin");
        let output = exporter.export();

        assert!(output.contains("helios_plugin_total_calls"));
        assert!(output.contains("helios_plugin_plugin_calls"));
    }

    #[test]
    fn test_record_plugin_load_unload() {
        let metrics = PluginMetrics::new();

        metrics.record_plugin_load("test-plugin");

        let stats = metrics.get_plugin_stats("test-plugin");
        assert!(stats.uptime.is_some());

        metrics.record_plugin_unload("test-plugin");
    }
}
