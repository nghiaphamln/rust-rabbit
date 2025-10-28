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

/// Result type alias for the rust-rabbit library
pub type Result<T> = std::result::Result<T, RabbitError>;
