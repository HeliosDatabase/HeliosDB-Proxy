//! Workload scheduler for cache resource allocation
//!
//! Schedules cache operations based on workload type and priority.
//! Supports multiple scheduling policies.

use chrono::Timelike;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use super::classifier::WorkloadType;
use super::config::SchedulingPolicy;
use super::DistribCacheConfig;

/// Scheduled query
#[derive(Debug, Clone)]
pub struct ScheduledQuery {
    /// Query identifier
    pub id: u64,
    /// Workload type
    pub workload_type: WorkloadType,
    /// Request timestamp
    pub timestamp: std::time::Instant,
}

/// Schedule result
#[derive(Debug, Clone)]
pub enum ScheduleResult {
    /// Execute immediately
    Execute { priority: QueryPriority },
    /// Queue for later execution
    Queued { position: usize },
    /// Reject due to resource constraints
    Rejected { reason: String },
}

/// Query priority
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryPriority {
    High,
    Normal,
    Low,
}

/// Workload distribution snapshot
#[derive(Debug, Clone)]
pub struct WorkloadDistribution {
    /// OLTP percentage
    pub oltp: WorkloadSlot,
    /// OLAP percentage
    pub olap: WorkloadSlot,
    /// Vector percentage
    pub vector: WorkloadSlot,
    /// AI Agent percentage
    pub ai_agent: WorkloadSlot,
    /// RAG percentage
    pub rag: WorkloadSlot,
}

/// Workload slot information
#[derive(Debug, Clone)]
pub struct WorkloadSlot {
    /// Current percentage
    pub current_pct: f64,
    /// Target percentage
    pub target_pct: f64,
    /// Currently queued
    pub queued: u32,
    /// Currently active
    pub active: u32,
}

/// Per-workload queue
struct WorkloadQueue {
    /// Pending queries
    pending: std::collections::VecDeque<ScheduledQuery>,
    /// Active count
    active: AtomicU32,
    /// Total processed
    total_processed: AtomicU64,
}

impl WorkloadQueue {
    fn new() -> Self {
        Self {
            pending: std::collections::VecDeque::new(),
            active: AtomicU32::new(0),
            total_processed: AtomicU64::new(0),
        }
    }
}

/// Workload scheduler
pub struct WorkloadScheduler {
    /// Configuration
    #[allow(dead_code)]
    config: DistribCacheConfig,

    /// Queues per workload type
    queues: DashMap<WorkloadType, RwLock<WorkloadQueue>>,

    /// Resource limits per workload
    limits: HashMap<WorkloadType, ResourceLimit>,

    /// Scheduling policy
    policy: SchedulingPolicy,

    /// Statistics
    stats: SchedulerStats,
}

/// Resource limits for a workload
#[derive(Debug, Clone)]
pub struct ResourceLimit {
    /// Maximum concurrent queries
    pub max_concurrent: u32,
    /// Maximum cache memory in MB
    pub max_cache_mb: usize,
    /// Priority weight (0.0 - 1.0)
    pub priority_weight: f64,
}

impl Default for ResourceLimit {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            max_cache_mb: 64,
            priority_weight: 0.5,
        }
    }
}

/// Scheduler statistics
#[derive(Debug, Default)]
struct SchedulerStats {
    total_scheduled: AtomicU64,
    total_queued: AtomicU64,
    total_rejected: AtomicU64,
    current_active: AtomicU32,
}

impl WorkloadScheduler {
    /// Create a new scheduler
    pub fn new(config: DistribCacheConfig) -> Self {
        let mut limits = HashMap::new();

        limits.insert(
            WorkloadType::OLTP,
            ResourceLimit {
                max_concurrent: config.max_concurrent_oltp,
                max_cache_mb: 64,
                priority_weight: config.oltp_priority,
            },
        );

        limits.insert(
            WorkloadType::OLAP,
            ResourceLimit {
                max_concurrent: config.max_concurrent_olap,
                max_cache_mb: 128,
                priority_weight: config.olap_priority,
            },
        );

        limits.insert(
            WorkloadType::Vector,
            ResourceLimit {
                max_concurrent: config.max_concurrent_vector,
                max_cache_mb: 96,
                priority_weight: config.vector_priority,
            },
        );

        limits.insert(
            WorkloadType::AIAgent,
            ResourceLimit {
                max_concurrent: config.max_concurrent_ai,
                max_cache_mb: 64,
                priority_weight: config.ai_agent_priority,
            },
        );

        limits.insert(
            WorkloadType::RAG,
            ResourceLimit {
                max_concurrent: config.max_concurrent_ai,
                max_cache_mb: 64,
                priority_weight: config.ai_agent_priority,
            },
        );

        limits.insert(WorkloadType::Mixed, ResourceLimit::default());

        let queues = DashMap::new();
        for wt in [
            WorkloadType::OLTP,
            WorkloadType::OLAP,
            WorkloadType::Vector,
            WorkloadType::AIAgent,
            WorkloadType::RAG,
            WorkloadType::Mixed,
        ] {
            queues.insert(wt, RwLock::new(WorkloadQueue::new()));
        }

        Self {
            policy: config.scheduling_policy,
            config,
            queues,
            limits,
            stats: SchedulerStats::default(),
        }
    }

    /// Schedule a query
    pub fn schedule(&self, query: ScheduledQuery) -> ScheduleResult {
        self.stats.total_scheduled.fetch_add(1, Ordering::Relaxed);

        let workload = query.workload_type;
        let default_limit = ResourceLimit::default();
        let limit = self.limits.get(&workload).unwrap_or(&default_limit);

        // Check current concurrency
        let current = self.get_current_concurrency(&workload);
        if current >= limit.max_concurrent {
            // Queue the request
            self.enqueue(query.clone());
            self.stats.total_queued.fetch_add(1, Ordering::Relaxed);
            return ScheduleResult::Queued {
                position: self.queue_position(&workload),
            };
        }

        // Apply scheduling policy
        match self.policy {
            SchedulingPolicy::StrictPriority => self.schedule_strict_priority(query),
            SchedulingPolicy::WeightedFair => self.schedule_weighted_fair(query),
            SchedulingPolicy::TimeBased => self.schedule_time_based(query),
            SchedulingPolicy::Adaptive => self.schedule_adaptive(query),
        }
    }

    /// Strict priority scheduling (OLTP always first)
    fn schedule_strict_priority(&self, query: ScheduledQuery) -> ScheduleResult {
        let priority = match query.workload_type {
            WorkloadType::OLTP => QueryPriority::High,
            WorkloadType::AIAgent | WorkloadType::RAG => QueryPriority::Normal,
            WorkloadType::Vector => QueryPriority::Normal,
            WorkloadType::OLAP => QueryPriority::Low,
            WorkloadType::Mixed => QueryPriority::Normal,
        };

        self.mark_active(&query.workload_type);
        ScheduleResult::Execute { priority }
    }

    /// Weighted fair scheduling
    fn schedule_weighted_fair(&self, query: ScheduledQuery) -> ScheduleResult {
        let limit = self.limits.get(&query.workload_type).unwrap();
        let weight = limit.priority_weight;

        let priority = if weight >= 0.8 {
            QueryPriority::High
        } else if weight >= 0.4 {
            QueryPriority::Normal
        } else {
            QueryPriority::Low
        };

        self.mark_active(&query.workload_type);
        ScheduleResult::Execute { priority }
    }

    /// Time-based scheduling
    fn schedule_time_based(&self, query: ScheduledQuery) -> ScheduleResult {
        let hour = chrono::Utc::now().hour();

        // Business hours (9-18): prioritize OLTP
        let priority = if (9..18).contains(&hour) {
            match query.workload_type {
                WorkloadType::OLTP | WorkloadType::AIAgent => QueryPriority::High,
                WorkloadType::OLAP => QueryPriority::Low,
                _ => QueryPriority::Normal,
            }
        } else {
            // Off-hours: prioritize OLAP
            match query.workload_type {
                WorkloadType::OLAP => QueryPriority::High,
                WorkloadType::OLTP => QueryPriority::Normal,
                _ => QueryPriority::Normal,
            }
        };

        self.mark_active(&query.workload_type);
        ScheduleResult::Execute { priority }
    }

    /// Adaptive scheduling (learns optimal distribution)
    fn schedule_adaptive(&self, query: ScheduledQuery) -> ScheduleResult {
        // Get current and ideal distribution
        let distribution = self.get_distribution();
        let workload = query.workload_type;

        let slot = match workload {
            WorkloadType::OLTP => &distribution.oltp,
            WorkloadType::OLAP => &distribution.olap,
            WorkloadType::Vector => &distribution.vector,
            WorkloadType::AIAgent => &distribution.ai_agent,
            WorkloadType::RAG => &distribution.rag,
            WorkloadType::Mixed => &distribution.oltp, // Default to OLTP behavior
        };

        let priority = if slot.current_pct < slot.target_pct {
            QueryPriority::High // Below target, prioritize
        } else if slot.current_pct > slot.target_pct * 1.2 {
            QueryPriority::Low // Above target, deprioritize
        } else {
            QueryPriority::Normal
        };

        self.mark_active(&query.workload_type);
        ScheduleResult::Execute { priority }
    }

    /// Get current concurrency for a workload
    fn get_current_concurrency(&self, workload: &WorkloadType) -> u32 {
        self.queues
            .get(workload)
            .map(|q| q.read().unwrap().active.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get queue position
    fn queue_position(&self, workload: &WorkloadType) -> usize {
        self.queues
            .get(workload)
            .map(|q| q.read().unwrap().pending.len())
            .unwrap_or(0)
    }

    /// Enqueue a query
    fn enqueue(&self, query: ScheduledQuery) {
        if let Some(queue) = self.queues.get(&query.workload_type) {
            queue.write().unwrap().pending.push_back(query);
        }
    }

    /// Mark a query as active
    fn mark_active(&self, workload: &WorkloadType) {
        if let Some(queue) = self.queues.get(workload) {
            queue.read().unwrap().active.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.current_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark a query as complete
    pub fn mark_complete(&self, workload: WorkloadType) {
        if let Some(queue) = self.queues.get(&workload) {
            let q = queue.read().unwrap();
            q.active.fetch_sub(1, Ordering::Relaxed);
            q.total_processed.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.current_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get workload distribution
    pub fn get_distribution(&self) -> WorkloadDistribution {
        let total_active = self.stats.current_active.load(Ordering::Relaxed) as f64;

        let get_slot = |wt: WorkloadType| -> WorkloadSlot {
            let queue = self.queues.get(&wt).unwrap();
            let q = queue.read().unwrap();
            let active = q.active.load(Ordering::Relaxed);
            let limit = self.limits.get(&wt).unwrap();

            WorkloadSlot {
                current_pct: if total_active > 0.0 {
                    active as f64 / total_active * 100.0
                } else {
                    0.0
                },
                target_pct: limit.priority_weight * 100.0 / 2.5, // Normalize
                queued: q.pending.len() as u32,
                active,
            }
        };

        WorkloadDistribution {
            oltp: get_slot(WorkloadType::OLTP),
            olap: get_slot(WorkloadType::OLAP),
            vector: get_slot(WorkloadType::Vector),
            ai_agent: get_slot(WorkloadType::AIAgent),
            rag: get_slot(WorkloadType::RAG),
        }
    }

    /// Get scheduler statistics
    pub fn stats(&self) -> SchedulerStatsSnapshot {
        SchedulerStatsSnapshot {
            total_scheduled: self.stats.total_scheduled.load(Ordering::Relaxed),
            total_queued: self.stats.total_queued.load(Ordering::Relaxed),
            total_rejected: self.stats.total_rejected.load(Ordering::Relaxed),
            current_active: self.stats.current_active.load(Ordering::Relaxed),
            policy: self.policy,
        }
    }
}

/// Scheduler statistics snapshot
#[derive(Debug, Clone)]
pub struct SchedulerStatsSnapshot {
    pub total_scheduled: u64,
    pub total_queued: u64,
    pub total_rejected: u64,
    pub current_active: u32,
    pub policy: SchedulingPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_oltp() {
        let config = DistribCacheConfig::default();
        let scheduler = WorkloadScheduler::new(config);

        let query = ScheduledQuery {
            id: 1,
            workload_type: WorkloadType::OLTP,
            timestamp: std::time::Instant::now(),
        };

        let result = scheduler.schedule(query);
        assert!(matches!(result, ScheduleResult::Execute { .. }));
    }

    #[test]
    fn test_schedule_with_concurrency_limit() {
        let mut config = DistribCacheConfig::default();
        config.max_concurrent_oltp = 1;

        let scheduler = WorkloadScheduler::new(config);

        // First query should execute
        let query1 = ScheduledQuery {
            id: 1,
            workload_type: WorkloadType::OLTP,
            timestamp: std::time::Instant::now(),
        };
        let result1 = scheduler.schedule(query1);
        assert!(matches!(result1, ScheduleResult::Execute { .. }));

        // Second query should be queued (max concurrent = 1)
        let query2 = ScheduledQuery {
            id: 2,
            workload_type: WorkloadType::OLTP,
            timestamp: std::time::Instant::now(),
        };
        let result2 = scheduler.schedule(query2);
        assert!(matches!(result2, ScheduleResult::Queued { .. }));
    }

    #[test]
    fn test_mark_complete() {
        let config = DistribCacheConfig::default();
        let scheduler = WorkloadScheduler::new(config);

        let query = ScheduledQuery {
            id: 1,
            workload_type: WorkloadType::OLTP,
            timestamp: std::time::Instant::now(),
        };

        scheduler.schedule(query);
        assert_eq!(scheduler.stats().current_active, 1);

        scheduler.mark_complete(WorkloadType::OLTP);
        assert_eq!(scheduler.stats().current_active, 0);
    }

    #[test]
    fn test_get_distribution() {
        let config = DistribCacheConfig::default();
        let scheduler = WorkloadScheduler::new(config);

        // Schedule some queries
        for i in 0..5 {
            let query = ScheduledQuery {
                id: i,
                workload_type: WorkloadType::OLTP,
                timestamp: std::time::Instant::now(),
            };
            scheduler.schedule(query);
        }

        for i in 0..3 {
            let query = ScheduledQuery {
                id: i + 10,
                workload_type: WorkloadType::OLAP,
                timestamp: std::time::Instant::now(),
            };
            scheduler.schedule(query);
        }

        let dist = scheduler.get_distribution();
        assert!(dist.oltp.active > 0);
        assert!(dist.olap.active > 0);
    }
}
