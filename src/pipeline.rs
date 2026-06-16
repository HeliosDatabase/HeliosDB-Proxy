//! Request Pipeline for HeliosProxy
//!
//! Provides request pipelining to reduce latency by sending multiple requests
//! without waiting for responses. Supports PostgreSQL protocol pipelining.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Connection ID type
pub type ConnectionId = u64;

/// Request ID for tracking pipelined requests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl RequestId {
    fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Maximum depth of the pipeline per connection
    pub max_depth: usize,
    /// Enable request pipelining
    pub enabled: bool,
    /// Timeout for individual requests (ms)
    pub request_timeout_ms: u64,
    /// Enable auto-flushing when idle
    pub auto_flush: bool,
    /// Auto-flush interval (ms)
    pub auto_flush_interval_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_depth: 16,
            enabled: true,
            request_timeout_ms: 30_000,
            auto_flush: true,
            auto_flush_interval_ms: 10,
        }
    }
}

/// A pending request in the pipeline
#[derive(Debug)]
pub struct PendingRequest {
    /// Request ID
    pub id: RequestId,
    /// Request data (SQL query or command)
    pub data: Vec<u8>,
    /// Submission timestamp
    pub submitted_at: Instant,
    /// Response channel
    response_tx: Option<oneshot::Sender<PipelineResponse>>,
}

/// Pipeline response
#[derive(Debug)]
pub struct PipelineResponse {
    /// Request ID
    pub request_id: RequestId,
    /// Response data
    pub data: Vec<u8>,
    /// Response time (from submission to completion)
    pub response_time: Duration,
    /// Whether the request succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}

/// Ticket for awaiting a pipelined response
pub struct Ticket {
    rx: oneshot::Receiver<PipelineResponse>,
}

impl Ticket {
    /// Wait for the response
    pub async fn wait(self) -> Result<PipelineResponse, PipelineError> {
        self.rx.await.map_err(|_| PipelineError::ChannelClosed)
    }

    /// Wait with timeout
    pub async fn wait_timeout(self, timeout: Duration) -> Result<PipelineResponse, PipelineError> {
        tokio::time::timeout(timeout, self.rx)
            .await
            .map_err(|_| PipelineError::Timeout)?
            .map_err(|_| PipelineError::ChannelClosed)
    }
}

/// Pipeline error types
#[derive(Debug, Clone)]
pub enum PipelineError {
    /// Pipeline is full
    PipelineFull,
    /// Pipeline is disabled
    Disabled,
    /// Request timeout
    Timeout,
    /// Channel closed unexpectedly
    ChannelClosed,
    /// Connection error
    ConnectionError(String),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PipelineFull => write!(f, "Pipeline is full"),
            Self::Disabled => write!(f, "Pipeline is disabled"),
            Self::Timeout => write!(f, "Request timeout"),
            Self::ChannelClosed => write!(f, "Channel closed"),
            Self::ConnectionError(e) => write!(f, "Connection error: {}", e),
        }
    }
}

impl std::error::Error for PipelineError {}

/// Pipeline statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Total requests submitted
    pub requests_submitted: u64,
    /// Total requests completed
    pub requests_completed: u64,
    /// Requests that timed out
    pub requests_timeout: u64,
    /// Requests rejected (pipeline full)
    pub requests_rejected: u64,
    /// Average pipeline depth
    pub avg_pipeline_depth: f64,
    /// Peak pipeline depth
    pub peak_pipeline_depth: usize,
    /// Average response time (ms)
    pub avg_response_time_ms: f64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
}

/// Request Pipeline for a single connection
struct ConnectionPipeline {
    /// Pending requests
    pending: VecDeque<PendingRequest>,
    /// Peak depth for this connection
    peak_depth: usize,
}

impl Default for ConnectionPipeline {
    fn default() -> Self {
        Self {
            pending: VecDeque::with_capacity(16),
            peak_depth: 0,
        }
    }
}

/// Request Pipeline Manager
///
/// Manages pipelined requests across multiple connections.
pub struct RequestPipeline {
    /// Configuration
    config: PipelineConfig,
    /// Pending requests per connection
    connections: DashMap<ConnectionId, ConnectionPipeline>,
    /// Next request ID
    next_request_id: AtomicU64,
    /// Statistics
    stats: Arc<parking_lot::RwLock<PipelineStats>>,
    /// Shutdown flag
    shutdown: AtomicBool,
}

impl RequestPipeline {
    /// Create a new request pipeline
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            connections: DashMap::new(),
            next_request_id: AtomicU64::new(1),
            stats: Arc::new(parking_lot::RwLock::new(PipelineStats::default())),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Submit a request to the pipeline
    pub fn submit(&self, conn_id: ConnectionId, data: Vec<u8>) -> Result<Ticket, PipelineError> {
        if !self.config.enabled {
            return Err(PipelineError::Disabled);
        }

        if self.shutdown.load(Ordering::Relaxed) {
            return Err(PipelineError::ConnectionError(
                "Pipeline shutdown".to_string(),
            ));
        }

        let request_id = RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();

        let pending = PendingRequest {
            id: request_id,
            data,
            submitted_at: Instant::now(),
            response_tx: Some(tx),
        };

        // Get or create pipeline for connection
        let mut pipeline = self.connections.entry(conn_id).or_default();

        // Check pipeline depth
        if pipeline.pending.len() >= self.config.max_depth {
            self.stats.write().requests_rejected += 1;
            return Err(PipelineError::PipelineFull);
        }

        // Track statistics
        {
            let mut stats = self.stats.write();
            stats.requests_submitted += 1;
            stats.bytes_sent += pending.data.len() as u64;
        }

        // Update peak depth
        let current_depth = pipeline.pending.len() + 1;
        if current_depth > pipeline.peak_depth {
            pipeline.peak_depth = current_depth;
        }

        pipeline.pending.push_back(pending);

        Ok(Ticket { rx })
    }

    /// Complete a request with a response
    pub fn complete(
        &self,
        conn_id: ConnectionId,
        request_id: RequestId,
        data: Vec<u8>,
        success: bool,
        error: Option<String>,
    ) {
        if let Some(mut pipeline) = self.connections.get_mut(&conn_id) {
            // Find and remove the matching request
            if let Some(pos) = pipeline.pending.iter().position(|r| r.id == request_id) {
                if let Some(mut req) = pipeline.pending.remove(pos) {
                    let response_time = req.submitted_at.elapsed();

                    // Update statistics
                    {
                        let mut stats = self.stats.write();
                        stats.requests_completed += 1;
                        stats.bytes_received += data.len() as u64;

                        // Update average response time (exponential moving average)
                        let ms = response_time.as_millis() as f64;
                        if stats.avg_response_time_ms == 0.0 {
                            stats.avg_response_time_ms = ms;
                        } else {
                            stats.avg_response_time_ms =
                                stats.avg_response_time_ms * 0.9 + ms * 0.1;
                        }
                    }

                    // Send response
                    if let Some(tx) = req.response_tx.take() {
                        let _ = tx.send(PipelineResponse {
                            request_id,
                            data,
                            response_time,
                            success,
                            error,
                        });
                    }
                }
            }
        }
    }

    /// Complete the next pending request in order (FIFO)
    pub fn complete_next(
        &self,
        conn_id: ConnectionId,
        data: Vec<u8>,
        success: bool,
        error: Option<String>,
    ) {
        if let Some(mut pipeline) = self.connections.get_mut(&conn_id) {
            if let Some(mut req) = pipeline.pending.pop_front() {
                let response_time = req.submitted_at.elapsed();

                // Update statistics
                {
                    let mut stats = self.stats.write();
                    stats.requests_completed += 1;
                    stats.bytes_received += data.len() as u64;

                    let ms = response_time.as_millis() as f64;
                    if stats.avg_response_time_ms == 0.0 {
                        stats.avg_response_time_ms = ms;
                    } else {
                        stats.avg_response_time_ms = stats.avg_response_time_ms * 0.9 + ms * 0.1;
                    }
                }

                if let Some(tx) = req.response_tx.take() {
                    let _ = tx.send(PipelineResponse {
                        request_id: req.id,
                        data,
                        response_time,
                        success,
                        error,
                    });
                }
            }
        }
    }

    /// Get current pipeline depth for a connection
    pub fn depth(&self, conn_id: ConnectionId) -> usize {
        self.connections
            .get(&conn_id)
            .map(|p| p.pending.len())
            .unwrap_or(0)
    }

    /// Check if pipeline is empty for a connection
    pub fn is_empty(&self, conn_id: ConnectionId) -> bool {
        self.depth(conn_id) == 0
    }

    /// Clear pipeline for a connection (e.g., on connection close)
    pub fn clear(&self, conn_id: ConnectionId) {
        self.connections.remove(&conn_id);
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> PipelineStats {
        let mut stats = self.stats.read().clone();

        // Calculate peak pipeline depth across all connections
        stats.peak_pipeline_depth = self
            .connections
            .iter()
            .map(|p| p.peak_depth)
            .max()
            .unwrap_or(0);

        // Calculate average pipeline depth
        let total_depth: usize = self.connections.iter().map(|p| p.pending.len()).sum();
        let conn_count = self.connections.len();
        stats.avg_pipeline_depth = if conn_count > 0 {
            total_depth as f64 / conn_count as f64
        } else {
            0.0
        };

        stats
    }

    /// Shutdown the pipeline
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.connections.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_submit() {
        let pipeline = RequestPipeline::new(PipelineConfig::default());
        let conn_id = 1;

        let ticket = pipeline.submit(conn_id, b"SELECT 1".to_vec()).unwrap();
        assert_eq!(pipeline.depth(conn_id), 1);

        // Complete the request
        pipeline.complete_next(conn_id, b"1".to_vec(), true, None);
        assert_eq!(pipeline.depth(conn_id), 0);

        // Verify response
        let response = ticket.wait().await.unwrap();
        assert!(response.success);
    }

    #[tokio::test]
    async fn test_pipeline_full() {
        let config = PipelineConfig {
            max_depth: 2,
            ..Default::default()
        };
        let pipeline = RequestPipeline::new(config);
        let conn_id = 1;

        // Submit up to max depth
        pipeline.submit(conn_id, b"SELECT 1".to_vec()).unwrap();
        pipeline.submit(conn_id, b"SELECT 2".to_vec()).unwrap();

        // Third should fail
        let result = pipeline.submit(conn_id, b"SELECT 3".to_vec());
        assert!(matches!(result, Err(PipelineError::PipelineFull)));
    }

    #[test]
    fn test_pipeline_stats() {
        let pipeline = RequestPipeline::new(PipelineConfig::default());
        let conn_id = 1;

        pipeline.submit(conn_id, b"SELECT 1".to_vec()).unwrap();
        pipeline.submit(conn_id, b"SELECT 2".to_vec()).unwrap();

        let stats = pipeline.stats();
        assert_eq!(stats.requests_submitted, 2);
    }

    #[test]
    fn test_pipeline_disabled() {
        let config = PipelineConfig {
            enabled: false,
            ..Default::default()
        };
        let pipeline = RequestPipeline::new(config);

        let result = pipeline.submit(1, b"SELECT 1".to_vec());
        assert!(matches!(result, Err(PipelineError::Disabled)));
    }
}
