//! Agent Token Budget & Workflow Quotas
//!
//! AI/Agent-specific rate limiting features:
//! - Token budgets (daily/hourly allocations)
//! - Workflow quotas (limit multi-step operations)
//! - LLM-friendly error messages

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

/// Agent token budget
///
/// AI agents get token budgets instead of simple rate limits.
/// This allows for burst operations while enforcing daily/hourly limits.
#[derive(Debug)]
pub struct AgentTokenBudget {
    /// Budget identifier
    agent_id: String,

    /// Total token allocation for the period
    total_tokens: u64,

    /// Used tokens in current period
    used_tokens: AtomicU64,

    /// Token cost per operation type
    operation_costs: HashMap<String, u64>,

    /// Budget period (reset interval)
    period: Duration,

    /// Last reset time
    last_reset: RwLock<Instant>,

    /// Warning threshold (percentage)
    warning_threshold: f64,

    /// Hard limit enabled
    hard_limit: bool,
}

impl AgentTokenBudget {
    /// Create a new daily token budget
    pub fn daily(agent_id: impl Into<String>, tokens: u64) -> Self {
        Self::new(agent_id, tokens, Duration::from_secs(86400))
    }

    /// Create a new hourly token budget
    pub fn hourly(agent_id: impl Into<String>, tokens: u64) -> Self {
        Self::new(agent_id, tokens, Duration::from_secs(3600))
    }

    /// Create a new token budget with custom period
    pub fn new(agent_id: impl Into<String>, tokens: u64, period: Duration) -> Self {
        Self {
            agent_id: agent_id.into(),
            total_tokens: tokens,
            used_tokens: AtomicU64::new(0),
            operation_costs: Self::default_operation_costs(),
            period,
            last_reset: RwLock::new(Instant::now()),
            warning_threshold: 0.8,
            hard_limit: true,
        }
    }

    /// Set warning threshold (0.0 - 1.0)
    pub fn with_warning_threshold(mut self, threshold: f64) -> Self {
        self.warning_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set hard limit behavior
    pub fn with_hard_limit(mut self, hard: bool) -> Self {
        self.hard_limit = hard;
        self
    }

    /// Set operation costs
    pub fn with_operation_costs(mut self, costs: HashMap<String, u64>) -> Self {
        self.operation_costs = costs;
        self
    }

    /// Add operation cost
    pub fn add_operation_cost(&mut self, operation: impl Into<String>, cost: u64) {
        self.operation_costs.insert(operation.into(), cost);
    }

    /// Consume tokens for an operation
    pub fn consume(&self, operation: &str, estimated_tokens: u64) -> Result<(), BudgetExceeded> {
        self.maybe_reset();

        let cost = self.operation_costs.get(operation).copied().unwrap_or(1);
        let total_cost = cost.saturating_mul(estimated_tokens);

        let used = self.used_tokens.fetch_add(total_cost, Ordering::SeqCst);

        if self.hard_limit && used + total_cost > self.total_tokens {
            // Rollback
            self.used_tokens.fetch_sub(total_cost, Ordering::SeqCst);

            return Err(BudgetExceeded {
                agent_id: self.agent_id.clone(),
                requested: total_cost,
                remaining: self.total_tokens.saturating_sub(used),
                total: self.total_tokens,
                resets_in: self.time_until_reset(),
            });
        }

        Ok(())
    }

    /// Check if budget is available (without consuming)
    pub fn check(&self, operation: &str, estimated_tokens: u64) -> Result<(), BudgetExceeded> {
        self.maybe_reset();

        let cost = self.operation_costs.get(operation).copied().unwrap_or(1);
        let total_cost = cost.saturating_mul(estimated_tokens);
        let used = self.used_tokens.load(Ordering::SeqCst);

        if used + total_cost > self.total_tokens {
            return Err(BudgetExceeded {
                agent_id: self.agent_id.clone(),
                requested: total_cost,
                remaining: self.total_tokens.saturating_sub(used),
                total: self.total_tokens,
                resets_in: self.time_until_reset(),
            });
        }

        Ok(())
    }

    /// Get remaining tokens
    pub fn remaining(&self) -> u64 {
        self.maybe_reset();
        let used = self.used_tokens.load(Ordering::SeqCst);
        self.total_tokens.saturating_sub(used)
    }

    /// Get used tokens
    pub fn used(&self) -> u64 {
        self.maybe_reset();
        self.used_tokens.load(Ordering::SeqCst)
    }

    /// Get usage percentage (0.0 - 1.0)
    pub fn usage_percentage(&self) -> f64 {
        self.maybe_reset();
        let used = self.used_tokens.load(Ordering::SeqCst);
        used as f64 / self.total_tokens as f64
    }

    /// Check if over warning threshold
    pub fn is_warning(&self) -> bool {
        self.usage_percentage() >= self.warning_threshold
    }

    /// Get time until reset
    pub fn time_until_reset(&self) -> Duration {
        let last = *self.last_reset.read();
        let elapsed = last.elapsed();

        if elapsed >= self.period {
            Duration::ZERO
        } else {
            self.period - elapsed
        }
    }

    /// Force reset
    pub fn reset(&self) {
        self.used_tokens.store(0, Ordering::SeqCst);
        *self.last_reset.write() = Instant::now();
    }

    /// Maybe reset if period elapsed
    fn maybe_reset(&self) {
        let last = *self.last_reset.read();
        if last.elapsed() >= self.period {
            self.reset();
        }
    }

    fn default_operation_costs() -> HashMap<String, u64> {
        let mut costs = HashMap::new();
        costs.insert("query".to_string(), 1);
        costs.insert("embedding".to_string(), 5);
        costs.insert("vector_search".to_string(), 10);
        costs.insert("write".to_string(), 2);
        costs.insert("transaction".to_string(), 3);
        costs
    }
}

impl Clone for AgentTokenBudget {
    fn clone(&self) -> Self {
        Self {
            agent_id: self.agent_id.clone(),
            total_tokens: self.total_tokens,
            used_tokens: AtomicU64::new(self.used_tokens.load(Ordering::Relaxed)),
            operation_costs: self.operation_costs.clone(),
            period: self.period,
            last_reset: RwLock::new(*self.last_reset.read()),
            warning_threshold: self.warning_threshold,
            hard_limit: self.hard_limit,
        }
    }
}

/// Budget exceeded error
#[derive(Debug, Clone)]
pub struct BudgetExceeded {
    /// Agent ID
    pub agent_id: String,

    /// Tokens requested
    pub requested: u64,

    /// Tokens remaining
    pub remaining: u64,

    /// Total budget
    pub total: u64,

    /// Time until budget resets
    pub resets_in: Duration,
}

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Token budget exceeded for agent '{}': requested {} tokens, {} remaining of {} total, resets in {}s",
            self.agent_id,
            self.requested,
            self.remaining,
            self.total,
            self.resets_in.as_secs()
        )
    }
}

impl std::error::Error for BudgetExceeded {}

impl BudgetExceeded {
    /// Get LLM-friendly error message
    pub fn to_llm_message(&self) -> String {
        format!(
            "{{\"error\": \"budget_exceeded\", \"message\": \"Token budget exceeded\", \
             \"details\": {{\"agent_id\": \"{}\", \"requested\": {}, \"remaining\": {}, \
             \"total\": {}, \"resets_in_seconds\": {}}}, \
             \"suggestion\": \"Wait for budget reset or request a higher allocation\"}}",
            self.agent_id,
            self.requested,
            self.remaining,
            self.total,
            self.resets_in.as_secs()
        )
    }
}

/// Workflow quota
///
/// Tracks and limits agent workflow executions (multi-step operations).
#[derive(Debug)]
pub struct WorkflowQuota {
    /// Maximum workflows per period
    max_workflows: u32,

    /// Maximum steps per workflow
    max_steps: u32,

    /// Current period's workflow count
    workflow_count: AtomicU32,

    /// Quota period
    period: Duration,

    /// Last reset
    last_reset: RwLock<Instant>,

    /// Active workflows
    active_workflows: DashMap<String, WorkflowToken>,
}

impl WorkflowQuota {
    /// Create a new hourly workflow quota
    pub fn hourly(max_workflows: u32, max_steps: u32) -> Self {
        Self::new(max_workflows, max_steps, Duration::from_secs(3600))
    }

    /// Create a new workflow quota
    pub fn new(max_workflows: u32, max_steps: u32, period: Duration) -> Self {
        Self {
            max_workflows,
            max_steps,
            workflow_count: AtomicU32::new(0),
            period,
            last_reset: RwLock::new(Instant::now()),
            active_workflows: DashMap::new(),
        }
    }

    /// Begin a new workflow
    pub fn begin_workflow(
        &self,
        workflow_id: impl Into<String>,
    ) -> Result<WorkflowToken, QuotaExceeded> {
        self.maybe_reset();

        let count = self.workflow_count.fetch_add(1, Ordering::SeqCst);
        if count >= self.max_workflows {
            self.workflow_count.fetch_sub(1, Ordering::SeqCst);
            return Err(QuotaExceeded::HourlyLimit {
                current: count,
                limit: self.max_workflows,
                resets_in: self.time_until_reset(),
            });
        }

        let id = workflow_id.into();
        let token = WorkflowToken::new(id.clone(), self.max_steps);
        self.active_workflows.insert(id, token.clone());

        Ok(token)
    }

    /// End a workflow
    pub fn end_workflow(&self, workflow_id: &str) {
        self.active_workflows.remove(workflow_id);
    }

    /// Get active workflow count
    pub fn active_count(&self) -> usize {
        self.active_workflows.len()
    }

    /// Get workflow count in period
    pub fn period_count(&self) -> u32 {
        self.maybe_reset();
        self.workflow_count.load(Ordering::SeqCst)
    }

    /// Get remaining workflows
    pub fn remaining(&self) -> u32 {
        self.maybe_reset();
        let count = self.workflow_count.load(Ordering::SeqCst);
        self.max_workflows.saturating_sub(count)
    }

    /// Get time until reset
    pub fn time_until_reset(&self) -> Duration {
        let last = *self.last_reset.read();
        let elapsed = last.elapsed();

        if elapsed >= self.period {
            Duration::ZERO
        } else {
            self.period - elapsed
        }
    }

    /// Force reset
    pub fn reset(&self) {
        self.workflow_count.store(0, Ordering::SeqCst);
        *self.last_reset.write() = Instant::now();
    }

    fn maybe_reset(&self) {
        let last = *self.last_reset.read();
        if last.elapsed() >= self.period {
            self.reset();
        }
    }
}

impl Clone for WorkflowQuota {
    fn clone(&self) -> Self {
        Self {
            max_workflows: self.max_workflows,
            max_steps: self.max_steps,
            workflow_count: AtomicU32::new(self.workflow_count.load(Ordering::Relaxed)),
            period: self.period,
            last_reset: RwLock::new(*self.last_reset.read()),
            active_workflows: DashMap::new(),
        }
    }
}

/// Workflow token
///
/// Tracks a single workflow's step usage.
#[derive(Debug)]
pub struct WorkflowToken {
    /// Workflow ID
    pub id: String,

    /// Remaining steps
    remaining_steps: AtomicU32,

    /// Total steps allowed
    max_steps: u32,

    /// Steps executed
    steps_executed: AtomicU32,

    /// Created at
    created_at: Instant,
}

impl Clone for WorkflowToken {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            remaining_steps: AtomicU32::new(self.remaining_steps.load(Ordering::Relaxed)),
            max_steps: self.max_steps,
            steps_executed: AtomicU32::new(self.steps_executed.load(Ordering::Relaxed)),
            created_at: self.created_at,
        }
    }
}

impl WorkflowToken {
    fn new(id: String, max_steps: u32) -> Self {
        Self {
            id,
            remaining_steps: AtomicU32::new(max_steps),
            max_steps,
            steps_executed: AtomicU32::new(0),
            created_at: Instant::now(),
        }
    }

    /// Execute a step
    pub fn execute_step(&self) -> Result<(), QuotaExceeded> {
        let remaining = self.remaining_steps.fetch_sub(1, Ordering::SeqCst);

        if remaining == 0 {
            self.remaining_steps.fetch_add(1, Ordering::SeqCst); // Rollback
            return Err(QuotaExceeded::StepLimit {
                workflow_id: self.id.clone(),
                steps_executed: self.steps_executed.load(Ordering::SeqCst),
                max_steps: self.max_steps,
            });
        }

        self.steps_executed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Get remaining steps
    pub fn remaining_steps(&self) -> u32 {
        self.remaining_steps.load(Ordering::SeqCst)
    }

    /// Get executed steps
    pub fn steps_executed(&self) -> u32 {
        self.steps_executed.load(Ordering::SeqCst)
    }

    /// Get workflow duration
    pub fn duration(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Check if can execute more steps
    pub fn can_continue(&self) -> bool {
        self.remaining_steps.load(Ordering::SeqCst) > 0
    }
}

/// Quota exceeded error
#[derive(Debug, Clone)]
pub enum QuotaExceeded {
    /// Hourly workflow limit reached
    HourlyLimit {
        current: u32,
        limit: u32,
        resets_in: Duration,
    },

    /// Step limit for workflow reached
    StepLimit {
        workflow_id: String,
        steps_executed: u32,
        max_steps: u32,
    },
}

impl std::fmt::Display for QuotaExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaExceeded::HourlyLimit {
                current,
                limit,
                resets_in,
            } => {
                write!(
                    f,
                    "Hourly workflow limit exceeded: {}/{} workflows, resets in {}s",
                    current,
                    limit,
                    resets_in.as_secs()
                )
            }
            QuotaExceeded::StepLimit {
                workflow_id,
                steps_executed,
                max_steps,
            } => {
                write!(
                    f,
                    "Workflow '{}' step limit exceeded: {}/{} steps",
                    workflow_id, steps_executed, max_steps
                )
            }
        }
    }
}

impl std::error::Error for QuotaExceeded {}

impl QuotaExceeded {
    /// Get LLM-friendly error message
    pub fn to_llm_message(&self) -> String {
        match self {
            QuotaExceeded::HourlyLimit {
                current,
                limit,
                resets_in,
            } => {
                format!(
                    "{{\"error\": \"workflow_quota_exceeded\", \"type\": \"hourly_limit\", \
                     \"current\": {}, \"limit\": {}, \"resets_in_seconds\": {}, \
                     \"suggestion\": \"Wait for quota reset or optimize workflow count\"}}",
                    current,
                    limit,
                    resets_in.as_secs()
                )
            }
            QuotaExceeded::StepLimit {
                workflow_id,
                steps_executed,
                max_steps,
            } => {
                format!(
                    "{{\"error\": \"workflow_quota_exceeded\", \"type\": \"step_limit\", \
                     \"workflow_id\": \"{}\", \"steps_executed\": {}, \"max_steps\": {}, \
                     \"suggestion\": \"Complete current workflow before starting more steps\"}}",
                    workflow_id, steps_executed, max_steps
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_budget_creation() {
        let budget = AgentTokenBudget::daily("agent-1", 10000);
        assert_eq!(budget.remaining(), 10000);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn test_token_budget_consume() {
        let budget = AgentTokenBudget::daily("agent-1", 100);

        assert!(budget.consume("query", 10).is_ok());
        assert_eq!(budget.used(), 10);
        assert_eq!(budget.remaining(), 90);
    }

    #[test]
    fn test_token_budget_exceeded() {
        let budget = AgentTokenBudget::daily("agent-1", 10);

        assert!(budget.consume("query", 5).is_ok());
        assert!(budget.consume("query", 5).is_ok());

        let result = budget.consume("query", 1);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.agent_id, "agent-1");
        assert_eq!(err.remaining, 0);
    }

    #[test]
    fn test_token_budget_operation_costs() {
        let budget = AgentTokenBudget::daily("agent-1", 1000);

        // Default: embedding costs 5x
        assert!(budget.consume("embedding", 10).is_ok());
        assert_eq!(budget.used(), 50); // 5 * 10
    }

    #[test]
    fn test_token_budget_warning() {
        let budget = AgentTokenBudget::daily("agent-1", 100).with_warning_threshold(0.8);

        assert!(!budget.is_warning());

        assert!(budget.consume("query", 85).is_ok());
        assert!(budget.is_warning());
    }

    #[test]
    fn test_token_budget_reset() {
        let budget = AgentTokenBudget::new("agent-1", 100, Duration::from_millis(50));

        assert!(budget.consume("query", 100).is_ok());
        assert_eq!(budget.remaining(), 0);

        std::thread::sleep(Duration::from_millis(60));

        // Should auto-reset
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_budget_exceeded_llm_message() {
        let err = BudgetExceeded {
            agent_id: "agent-1".to_string(),
            requested: 100,
            remaining: 50,
            total: 1000,
            resets_in: Duration::from_secs(3600),
        };

        let msg = err.to_llm_message();
        assert!(msg.contains("budget_exceeded"));
        assert!(msg.contains("agent-1"));
    }

    #[test]
    fn test_workflow_quota_creation() {
        let quota = WorkflowQuota::hourly(10, 100);
        assert_eq!(quota.remaining(), 10);
    }

    #[test]
    fn test_workflow_quota_begin() {
        let quota = WorkflowQuota::hourly(10, 100);

        let token = quota.begin_workflow("wf-1").unwrap();
        assert_eq!(token.remaining_steps(), 100);
        assert_eq!(quota.remaining(), 9);
    }

    #[test]
    fn test_workflow_quota_exceeded() {
        let quota = WorkflowQuota::hourly(2, 100);

        assert!(quota.begin_workflow("wf-1").is_ok());
        assert!(quota.begin_workflow("wf-2").is_ok());

        let result = quota.begin_workflow("wf-3");
        assert!(result.is_err());
    }

    #[test]
    fn test_workflow_token_steps() {
        let quota = WorkflowQuota::hourly(10, 5);
        let token = quota.begin_workflow("wf-1").unwrap();

        for _ in 0..5 {
            assert!(token.execute_step().is_ok());
        }

        let result = token.execute_step();
        assert!(result.is_err());
    }

    #[test]
    fn test_workflow_token_can_continue() {
        let quota = WorkflowQuota::hourly(10, 2);
        let token = quota.begin_workflow("wf-1").unwrap();

        assert!(token.can_continue());

        assert!(token.execute_step().is_ok());
        assert!(token.can_continue());

        assert!(token.execute_step().is_ok());
        assert!(!token.can_continue());
    }

    #[test]
    fn test_quota_exceeded_llm_message() {
        let err = QuotaExceeded::HourlyLimit {
            current: 10,
            limit: 10,
            resets_in: Duration::from_secs(1800),
        };

        let msg = err.to_llm_message();
        assert!(msg.contains("workflow_quota_exceeded"));
        assert!(msg.contains("hourly_limit"));

        let err2 = QuotaExceeded::StepLimit {
            workflow_id: "wf-1".to_string(),
            steps_executed: 100,
            max_steps: 100,
        };

        let msg2 = err2.to_llm_message();
        assert!(msg2.contains("step_limit"));
    }

    #[test]
    fn test_workflow_end() {
        let quota = WorkflowQuota::hourly(10, 100);

        let _token = quota.begin_workflow("wf-1").unwrap();
        assert_eq!(quota.active_count(), 1);

        quota.end_workflow("wf-1");
        assert_eq!(quota.active_count(), 0);
    }
}
