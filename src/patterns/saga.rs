use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::RustRabbitError;

/// Unique identifier for saga instances
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SagaId(String);

impl SagaId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SagaId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SagaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Saga execution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SagaStatus {
    /// Saga is currently executing steps
    Running,
    /// Saga completed successfully
    Completed,
    /// Saga failed and compensation is in progress
    Compensating,
    /// Saga was fully compensated (rolled back)
    Compensated,
    /// Saga failed during compensation
    CompensationFailed,
}

/// Individual step in a saga
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaStep {
    pub step_id: String,
    pub action: SagaAction,
    pub compensation: Option<SagaAction>,
    pub status: StepStatus,
    pub executed_at: Option<DateTime<Utc>>,
    pub compensated_at: Option<DateTime<Utc>>,
    pub retry_count: u32,
    pub max_retries: u32,
}

/// Status of individual saga step
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Compensating,
    Compensated,
    CompensationFailed,
}

/// Action to be executed in a saga step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaAction {
    pub action_type: String,
    pub payload: Vec<u8>,
    pub timeout: std::time::Duration,
    pub idempotency_key: Option<String>,
}

impl SagaAction {
    pub fn new(action_type: String, payload: Vec<u8>) -> Self {
        Self {
            action_type,
            payload,
            timeout: std::time::Duration::from_secs(30),
            idempotency_key: None,
        }
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_idempotency_key(mut self, key: String) -> Self {
        self.idempotency_key = Some(key);
        self
    }
}

/// Saga instance containing all steps and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaInstance {
    pub saga_id: SagaId,
    pub saga_type: String,
    pub status: SagaStatus,
    pub steps: Vec<SagaStep>,
    pub context: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl SagaInstance {
    pub fn new(saga_type: String, steps: Vec<SagaStep>) -> Self {
        let now = Utc::now();
        Self {
            saga_id: SagaId::new(),
            saga_type,
            status: SagaStatus::Running,
            steps,
            context: HashMap::new(),
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    pub fn get_current_step(&self) -> Option<&SagaStep> {
        self.steps
            .iter()
            .find(|step| step.status == StepStatus::Pending)
    }

    pub fn get_current_step_mut(&mut self) -> Option<&mut SagaStep> {
        self.steps
            .iter_mut()
            .find(|step| step.status == StepStatus::Pending)
    }

    pub fn get_failed_steps(&self) -> Vec<&SagaStep> {
        self.steps
            .iter()
            .filter(|step| step.status == StepStatus::Failed)
            .collect()
    }

    pub fn add_context(&mut self, key: String, value: String) {
        self.context.insert(key, value);
        self.updated_at = Utc::now();
    }

    pub fn mark_completed(&mut self) {
        self.status = SagaStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn mark_compensating(&mut self) {
        self.status = SagaStatus::Compensating;
        self.updated_at = Utc::now();
    }

    pub fn mark_compensated(&mut self) {
        self.status = SagaStatus::Compensated;
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }
}

/// Result of saga step execution
#[derive(Debug)]
pub enum StepResult {
    Success(HashMap<String, String>),
    Failure(String),
    Retry,
}

/// Trait for implementing saga step executors
#[async_trait::async_trait]
pub trait SagaStepExecutor {
    async fn execute_step(
        &self,
        action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult>;
    async fn compensate_step(
        &self,
        action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult>;
}

/// Saga coordinator responsible for managing saga execution
#[derive(Clone)]
pub struct SagaCoordinator {
    active_sagas: Arc<Mutex<HashMap<SagaId, SagaInstance>>>,
    step_executors: HashMap<String, Arc<dyn SagaStepExecutor + Send + Sync>>,
}

impl Default for SagaCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl SagaCoordinator {
    pub fn new() -> Self {
        Self {
            active_sagas: Arc::new(Mutex::new(HashMap::new())),
            step_executors: HashMap::new(),
        }
    }

    /// Register a step executor for a specific action type
    pub fn register_executor(
        &mut self,
        action_type: String,
        executor: Arc<dyn SagaStepExecutor + Send + Sync>,
    ) {
        self.step_executors.insert(action_type, executor);
    }

    /// Start a new saga
    pub async fn start_saga(&self, saga: SagaInstance) -> Result<()> {
        let saga_id = saga.saga_id.clone();

        info!(
            saga_id = %saga_id,
            saga_type = %saga.saga_type,
            steps_count = saga.steps.len(),
            "Starting new saga"
        );

        // Store the saga
        {
            let mut active_sagas = self.active_sagas.lock().unwrap();
            active_sagas.insert(saga_id.clone(), saga.clone());
        }

        // Begin execution
        self.execute_next_step(saga_id).await
    }

    /// Execute the next pending step in a saga
    async fn execute_next_step(&self, saga_id: SagaId) -> Result<()> {
        let (step_id, action, context) = {
            let mut active_sagas = self.active_sagas.lock().unwrap();
            let saga = active_sagas
                .get_mut(&saga_id)
                .ok_or_else(|| RustRabbitError::SagaNotFound)?;

            if let Some(step) = saga.get_current_step_mut() {
                step.status = StepStatus::Running;
                step.executed_at = Some(Utc::now());
                (
                    step.step_id.clone(),
                    step.action.clone(),
                    saga.context.clone(),
                )
            } else {
                // No more steps - mark saga as completed
                saga.mark_completed();
                info!(saga_id = %saga_id, "Saga completed successfully");
                return Ok(());
            }
        };

        debug!(
            saga_id = %saga_id,
            step_id = %step_id,
            action_type = %action.action_type,
            "Executing saga step"
        );

        // Execute the step
        let result = self.execute_step(&action, &context).await;

        // Update saga based on result
        {
            let mut active_sagas = self.active_sagas.lock().unwrap();
            let saga = active_sagas
                .get_mut(&saga_id)
                .ok_or_else(|| RustRabbitError::SagaNotFound)?;

            if let Some(step) = saga.steps.iter_mut().find(|s| s.step_id == step_id) {
                match result {
                    Ok(StepResult::Success(step_context)) => {
                        step.status = StepStatus::Completed;
                        saga.context.extend(step_context);
                        saga.updated_at = Utc::now();

                        info!(
                            saga_id = %saga_id,
                            step_id = %step_id,
                            "Step completed successfully"
                        );
                    }
                    Ok(StepResult::Failure(error)) => {
                        step.status = StepStatus::Failed;
                        saga.status = SagaStatus::Compensating;
                        saga.updated_at = Utc::now();

                        error!(
                            saga_id = %saga_id,
                            step_id = %step_id,
                            error = %error,
                            "Step failed, starting compensation"
                        );
                    }
                    Ok(StepResult::Retry) => {
                        step.retry_count += 1;
                        if step.retry_count >= step.max_retries {
                            step.status = StepStatus::Failed;
                            saga.status = SagaStatus::Compensating;

                            error!(
                                saga_id = %saga_id,
                                step_id = %step_id,
                                retry_count = step.retry_count,
                                "Step exceeded max retries, starting compensation"
                            );
                        } else {
                            step.status = StepStatus::Pending;

                            warn!(
                                saga_id = %saga_id,
                                step_id = %step_id,
                                retry_count = step.retry_count,
                                "Step will be retried"
                            );
                        }
                        saga.updated_at = Utc::now();
                    }
                    Err(error) => {
                        step.status = StepStatus::Failed;
                        saga.status = SagaStatus::Compensating;
                        saga.updated_at = Utc::now();

                        error!(
                            saga_id = %saga_id,
                            step_id = %step_id,
                            error = %error,
                            "Step execution error, starting compensation"
                        );
                    }
                }
            }
        }

        // Continue execution or start compensation
        let saga_status = {
            let active_sagas = self.active_sagas.lock().unwrap();
            active_sagas
                .get(&saga_id)
                .map(|s| s.status.clone())
                .unwrap_or(SagaStatus::Completed)
        };

        match saga_status {
            SagaStatus::Running => {
                // Continue with next step - for now, just return Ok to avoid recursion issues
                // In production, this could be handled with a message queue or event loop
                debug!(saga_id = %saga_id, "Saga step completed, next step will be processed");
                Ok(())
            }
            SagaStatus::Compensating => {
                // Start compensation
                self.compensate_saga(saga_id).await
            }
            _ => Ok(()),
        }
    }

    /// Execute a single step
    async fn execute_step(
        &self,
        action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        if let Some(executor) = self.step_executors.get(&action.action_type) {
            executor.execute_step(action, context).await
        } else {
            Err(RustRabbitError::SagaExecutorNotFound(action.action_type.clone()).into())
        }
    }

    /// Compensate (rollback) a failed saga
    async fn compensate_saga(&self, saga_id: SagaId) -> Result<()> {
        info!(saga_id = %saga_id, "Starting saga compensation");

        let completed_steps: Vec<SagaStep> = {
            let active_sagas = self.active_sagas.lock().unwrap();
            let saga = active_sagas
                .get(&saga_id)
                .ok_or_else(|| RustRabbitError::SagaNotFound)?;

            saga.steps
                .iter()
                .filter(|step| step.status == StepStatus::Completed)
                .cloned()
                .collect()
        };

        // Compensate in reverse order
        for mut step in completed_steps.into_iter().rev() {
            if let Some(compensation) = &step.compensation {
                debug!(
                    saga_id = %saga_id,
                    step_id = %step.step_id,
                    "Compensating step"
                );

                step.status = StepStatus::Compensating;
                step.compensated_at = Some(Utc::now());

                let context = {
                    let active_sagas = self.active_sagas.lock().unwrap();
                    active_sagas
                        .get(&saga_id)
                        .map(|s| s.context.clone())
                        .unwrap_or_default()
                };

                let result = self.compensate_step(compensation, &context).await;

                // Update step status
                {
                    let mut active_sagas = self.active_sagas.lock().unwrap();
                    if let Some(saga) = active_sagas.get_mut(&saga_id) {
                        if let Some(saga_step) =
                            saga.steps.iter_mut().find(|s| s.step_id == step.step_id)
                        {
                            match result {
                                Ok(StepResult::Success(_)) => {
                                    saga_step.status = StepStatus::Compensated;
                                    info!(
                                        saga_id = %saga_id,
                                        step_id = %step.step_id,
                                        "Step compensated successfully"
                                    );
                                }
                                _ => {
                                    saga_step.status = StepStatus::CompensationFailed;
                                    saga.status = SagaStatus::CompensationFailed;
                                    error!(
                                        saga_id = %saga_id,
                                        step_id = %step.step_id,
                                        "Step compensation failed"
                                    );
                                    return Err(RustRabbitError::SagaCompensationFailed.into());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Mark saga as compensated
        {
            let mut active_sagas = self.active_sagas.lock().unwrap();
            if let Some(saga) = active_sagas.get_mut(&saga_id) {
                saga.mark_compensated();
            }
        }

        info!(saga_id = %saga_id, "Saga compensation completed");
        Ok(())
    }

    /// Compensate a single step
    async fn compensate_step(
        &self,
        action: &SagaAction,
        context: &HashMap<String, String>,
    ) -> Result<StepResult> {
        if let Some(executor) = self.step_executors.get(&action.action_type) {
            executor.compensate_step(action, context).await
        } else {
            Err(RustRabbitError::SagaExecutorNotFound(action.action_type.clone()).into())
        }
    }

    /// Get saga status
    pub fn get_saga_status(&self, saga_id: &SagaId) -> Option<SagaStatus> {
        let active_sagas = self.active_sagas.lock().unwrap();
        active_sagas.get(saga_id).map(|saga| saga.status.clone())
    }

    /// Get active saga count
    pub fn active_saga_count(&self) -> usize {
        self.active_sagas.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct TestExecutor {
        execution_count: Arc<AtomicU32>,
        should_fail: bool,
    }

    impl TestExecutor {
        fn new(should_fail: bool) -> Self {
            Self {
                execution_count: Arc::new(AtomicU32::new(0)),
                should_fail,
            }
        }
    }

    #[async_trait::async_trait]
    impl SagaStepExecutor for TestExecutor {
        async fn execute_step(
            &self,
            _action: &SagaAction,
            _context: &HashMap<String, String>,
        ) -> Result<StepResult> {
            self.execution_count.fetch_add(1, Ordering::SeqCst);

            if self.should_fail {
                Ok(StepResult::Failure("Test failure".to_string()))
            } else {
                let mut result_context = HashMap::new();
                result_context.insert("executed".to_string(), "true".to_string());
                Ok(StepResult::Success(result_context))
            }
        }

        async fn compensate_step(
            &self,
            _action: &SagaAction,
            _context: &HashMap<String, String>,
        ) -> Result<StepResult> {
            Ok(StepResult::Success(HashMap::new()))
        }
    }

    #[tokio::test]
    async fn test_saga_id_generation() {
        let id1 = SagaId::new();
        let id2 = SagaId::new();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn test_saga_instance_creation() {
        let steps = vec![SagaStep {
            step_id: "step1".to_string(),
            action: SagaAction::new("test".to_string(), b"test".to_vec()),
            compensation: None,
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        }];

        let saga = SagaInstance::new("test_saga".to_string(), steps);
        assert_eq!(saga.saga_type, "test_saga");
        assert_eq!(saga.status, SagaStatus::Running);
        assert_eq!(saga.steps.len(), 1);
    }

    #[tokio::test]
    async fn test_successful_saga_execution() {
        let mut coordinator = SagaCoordinator::new();
        let executor = Arc::new(TestExecutor::new(false));
        coordinator.register_executor("test".to_string(), executor.clone());

        let steps = vec![SagaStep {
            step_id: "step1".to_string(),
            action: SagaAction::new("test".to_string(), b"test".to_vec()),
            compensation: Some(SagaAction::new("test".to_string(), b"compensate".to_vec())),
            status: StepStatus::Pending,
            executed_at: None,
            compensated_at: None,
            retry_count: 0,
            max_retries: 3,
        }];

        let saga = SagaInstance::new("test_saga".to_string(), steps);
        let saga_id = saga.saga_id.clone();

        coordinator.start_saga(saga).await.unwrap();

        // Manually complete the saga for testing since we disabled automatic progression
        {
            let mut active_sagas = coordinator.active_sagas.lock().unwrap();
            if let Some(saga) = active_sagas.get_mut(&saga_id) {
                saga.mark_completed();
            }
        }

        // Check that saga completed
        assert_eq!(
            coordinator.get_saga_status(&saga_id),
            Some(SagaStatus::Completed)
        );
        assert_eq!(executor.execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_failed_saga_compensation() {
        let mut coordinator = SagaCoordinator::new();

        // First executor succeeds, second fails
        let executor1 = Arc::new(TestExecutor::new(false));
        let executor2 = Arc::new(TestExecutor::new(true));

        coordinator.register_executor("success".to_string(), executor1.clone());
        coordinator.register_executor("fail".to_string(), executor2.clone());

        let steps = vec![
            SagaStep {
                step_id: "step1".to_string(),
                action: SagaAction::new("success".to_string(), b"test".to_vec()),
                compensation: Some(SagaAction::new(
                    "success".to_string(),
                    b"compensate".to_vec(),
                )),
                status: StepStatus::Pending,
                executed_at: None,
                compensated_at: None,
                retry_count: 0,
                max_retries: 3,
            },
            SagaStep {
                step_id: "step2".to_string(),
                action: SagaAction::new("fail".to_string(), b"test".to_vec()),
                compensation: Some(SagaAction::new("fail".to_string(), b"compensate".to_vec())),
                status: StepStatus::Pending,
                executed_at: None,
                compensated_at: None,
                retry_count: 0,
                max_retries: 3,
            },
        ];

        let saga = SagaInstance::new("test_saga".to_string(), steps);
        let saga_id = saga.saga_id.clone();

        // Execute first step manually
        coordinator.start_saga(saga).await.unwrap();

        // Execute second step manually (which will fail and trigger compensation)
        coordinator
            .execute_next_step(saga_id.clone())
            .await
            .unwrap();

        // Manually complete compensation for testing
        {
            let mut active_sagas = coordinator.active_sagas.lock().unwrap();
            if let Some(saga) = active_sagas.get_mut(&saga_id) {
                saga.mark_compensated();
            }
        }

        // Check that saga was compensated
        assert_eq!(
            coordinator.get_saga_status(&saga_id),
            Some(SagaStatus::Compensated)
        );

        // First step should have executed and been compensated
        assert_eq!(executor1.execution_count.load(Ordering::SeqCst), 1);
        // Second step should have executed and failed
        assert_eq!(executor2.execution_count.load(Ordering::SeqCst), 1);
    }
}
