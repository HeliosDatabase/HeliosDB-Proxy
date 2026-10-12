use super::*;

impl ProxyServer {
    /// Start the query cache's logical-decoding invalidation source (C-02
    /// slice 2) when `[cache] invalidation_source = "logical"`: attach (slot
    /// and publication) before serving, so a misconfigured source refuses
    /// startup instead of silently serving writes it cannot see, then poll in
    /// the background. `None` when the cache or the source is off.
    #[cfg(feature = "query-cache")]
    pub(super) async fn spawn_cache_logical_source(
        &self,
    ) -> Result<Option<tokio::task::JoinHandle<()>>> {
        use crate::cache::logical_source::{
            LogicalDsn, LogicalSettings, LogicalSource, LogicalStats,
        };
        let c = &self.config.cache;
        let Some(cache) = self.state.query_cache.clone() else {
            return Ok(None);
        };
        if c.invalidation_source != crate::config::CacheInvalidationSource::Logical {
            return Ok(None);
        }
        let dsn = LogicalDsn::parse(&c.logical_dsn)
            .map_err(|e| ProxyError::Config(format!("cache.logical_dsn: {e}")))?;
        let settings = LogicalSettings {
            dsn,
            slot: c.logical_slot.clone(),
            publication: c.logical_publication.clone(),
            feedback: Duration::from_millis(c.logical_feedback_ms),
            max_slot_lag_bytes: c.logical_max_slot_lag_bytes,
            max_changes_per_poll: c.logical_max_changes_per_poll,
            connect_timeout: Duration::from_secs(self.config.health.check_timeout_secs.max(1)),
            query_timeout: self.state.limits.backend_read_timeout,
        };
        let stats = Arc::new(LogicalStats::default());
        let mut source = LogicalSource::new(settings, stats.clone());
        source.attach().await.map_err(|e| {
            ProxyError::Config(format!(
                "cache.invalidation_source = \"logical\": cannot attach the logical source: {e}"
            ))
        })?;
        cache.set_logical_stats(stats);
        tracing::info!(
            slot = %c.logical_slot,
            publication = %c.logical_publication,
            feedback_ms = c.logical_feedback_ms,
            "query cache: logical-decoding invalidation source attached"
        );
        let shutdown = self.shutdown_tx.subscribe();
        Ok(Some(tokio::spawn(source.run(cache, shutdown))))
    }
}
