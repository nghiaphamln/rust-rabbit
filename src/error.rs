use thiserror::Error;

/// Main error type for the rust-rabbit library
#[derive(Error, Debug)]
pub enum RabbitError {
    #[error("Connection error: {0}")]
    Connection(#[from] lapin::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Configuration(String),

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

/// Result type alias for the rust-rabbit library
pub type Result<T> = std::result::Result<T, RabbitError>;