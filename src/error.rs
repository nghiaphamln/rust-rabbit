use thiserror::Error;

/// Main error type for the rust-rabbit library
#[derive(Error, Debug)]
pub enum RabbitError {
    #[error("Connection error: {0}")]
    Connection(#[from] lapin::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("Consumer error: {0}")]
    Consumer(String),

    #[error("Publisher error: {0}")]
    Publisher(String),

    #[error("Retry exhausted: {0}")]
    RetryExhausted(String),

    #[error("Health check failed: {0}")]
    HealthCheck(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

/// Extended error type for advanced patterns (Phase 2)
#[derive(Error, Debug)]
pub enum RustRabbitError {
    // Core errors
    #[error("Rabbit error: {0}")]
    Rabbit(#[from] RabbitError),

    // Request-Response pattern errors
    #[error("Request timeout")]
    RequestTimeout,

    #[error("Response channel closed")]
    ResponseChannelClosed,

    // Saga pattern errors
    #[error("Saga not found")]
    SagaNotFound,

    #[error("Saga executor not found for action type: {0}")]
    SagaExecutorNotFound(String),

    #[error("Saga compensation failed")]
    SagaCompensationFailed,

    // Event sourcing errors
    #[error("Event sequence error - events must be in order")]
    EventSequenceError,

    #[error("Unknown event type: {0}")]
    UnknownEventType(String),

    #[error("Aggregate not found")]
    AggregateNotFound,

    #[error("Snapshot creation failed")]
    SnapshotCreationFailed,

    // Priority queue errors
    #[error("Queue is full")]
    QueueFull,

    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    #[error("Invalid priority value: {0}")]
    InvalidPriority(u8),

    // Deduplication errors
    #[error("Duplicate message detected")]
    DuplicateMessage,

    #[error("Deduplication store error: {0}")]
    DeduplicationStore(String),

    // General async errors
    #[error("Channel send error")]
    ChannelSendError,

    #[error("Task join error: {0}")]
    TaskJoinError(String),

    #[error("Lock poisoned")]
    LockPoisoned,
}

/// Processing error types for consumer handlers
#[derive(Error, Debug, Clone)]
pub enum ProcessingError {
    /// Retryable error - message should be retried with delay
    #[error("Retryable error: {message}")]
    Retryable {
        message: String,
        /// Optional custom retry delay override
        custom_delay: Option<std::time::Duration>,
    },

    /// Non-retryable error - message should be rejected permanently
    #[error("Non-retryable error: {message}")]
    NonRetryable {
        message: String,
        /// Whether to send to dead letter queue (default: true)
        send_to_dlq: bool,
    },
}

impl ProcessingError {
    /// Create a retryable error with default delay
    pub fn retryable<S: Into<String>>(message: S) -> Self {
        Self::Retryable {
            message: message.into(),
            custom_delay: None,
        }
    }

    /// Create a retryable error with custom delay
    pub fn retryable_with_delay<S: Into<String>>(message: S, delay: std::time::Duration) -> Self {
        Self::Retryable {
            message: message.into(),
            custom_delay: Some(delay),
        }
    }

    /// Create a non-retryable error (will be sent to DLQ)
    pub fn non_retryable<S: Into<String>>(message: S) -> Self {
        Self::NonRetryable {
            message: message.into(),
            send_to_dlq: true,
        }
    }

    /// Create a non-retryable error that should be discarded (not sent to DLQ)
    pub fn discard<S: Into<String>>(message: S) -> Self {
        Self::NonRetryable {
            message: message.into(),
            send_to_dlq: false,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, ProcessingError::Retryable { .. })
    }

    /// Check if this error should be sent to DLQ
    pub fn should_send_to_dlq(&self) -> bool {
        match self {
            ProcessingError::Retryable { .. } => false,
            ProcessingError::NonRetryable { send_to_dlq, .. } => *send_to_dlq,
        }
    }

    /// Get custom delay if specified
    pub fn custom_delay(&self) -> Option<std::time::Duration> {
        match self {
            ProcessingError::Retryable { custom_delay, .. } => *custom_delay,
            ProcessingError::NonRetryable { .. } => None,
        }
    }
}

/// Result type alias for the rust-rabbit library
pub type Result<T> = std::result::Result<T, RabbitError>;
