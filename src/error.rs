//! Simplified Error Handling for rust-rabbit
//!
//! Basic error types for core RabbitMQ operations without complex pattern-specific errors.

use thiserror::Error;

/// Main error type for rust-rabbit library
#[derive(Error, Debug)]
pub enum RustRabbitError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("RabbitMQ protocol error: {0}")]
    Protocol(#[from] lapin::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Consumer error: {0}")]
    Consumer(String),

    #[error("Publisher error: {0}")]
    Publisher(String),

    #[error("Retry error: {0}")]
    Retry(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, RustRabbitError>;

impl RustRabbitError {
    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            // Connection errors are usually retryable
            RustRabbitError::Connection(_) => true,
            RustRabbitError::Protocol(lapin_error) => {
                // Check if it's a temporary protocol error
                matches!(
                    lapin_error,
                    lapin::Error::IOError(_) | lapin::Error::ProtocolError(_)
                )
            }
            // Serialization and configuration errors are not retryable
            RustRabbitError::Serialization(_) => false,
            RustRabbitError::Configuration(_) => false,
            // Consumer/Publisher errors might be retryable depending on context
            RustRabbitError::Consumer(_) => true,
            RustRabbitError::Publisher(_) => true,
            // Retry errors are not retryable (to avoid infinite loops)
            RustRabbitError::Retry(_) => false,
            // IO errors might be retryable
            RustRabbitError::Io(_) => true,
        }
    }

    /// Check if the error indicates a connection issue
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            RustRabbitError::Connection(_) | RustRabbitError::Protocol(_) | RustRabbitError::Io(_)
        )
    }

    /// Get a user-friendly error message
    pub fn user_message(&self) -> String {
        match self {
            RustRabbitError::Connection(_) => {
                "Failed to connect to RabbitMQ. Please check your connection settings.".to_string()
            }
            RustRabbitError::Protocol(_) => {
                "RabbitMQ protocol error. The connection might be unstable.".to_string()
            }
            RustRabbitError::Serialization(_) => {
                "Failed to serialize/deserialize message. Please check your message format."
                    .to_string()
            }
            RustRabbitError::Configuration(_) => {
                "Configuration error. Please check your settings.".to_string()
            }
            RustRabbitError::Consumer(_) => {
                "Consumer error. Failed to process message.".to_string()
            }
            RustRabbitError::Publisher(_) => "Publisher error. Failed to send message.".to_string(),
            RustRabbitError::Retry(_) => {
                "Retry mechanism failed. Message processing could not be completed.".to_string()
            }
            RustRabbitError::Io(_) => {
                "IO error occurred. This might be a temporary issue.".to_string()
            }
        }
    }
}

/// Convert serde_json errors to RustRabbitError
impl From<serde_json::Error> for RustRabbitError {
    fn from(err: serde_json::Error) -> Self {
        RustRabbitError::Serialization(err.to_string())
    }
}

/// Convert URL parsing errors to RustRabbitError
impl From<url::ParseError> for RustRabbitError {
    fn from(err: url::ParseError) -> Self {
        RustRabbitError::Configuration(format!("Invalid URL: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_retryable() {
        let conn_error = RustRabbitError::Connection("test".to_string());
        assert!(conn_error.is_retryable());

        let serialization_error = RustRabbitError::Serialization("test".to_string());
        assert!(!serialization_error.is_retryable());

        let config_error = RustRabbitError::Configuration("test".to_string());
        assert!(!config_error.is_retryable());

        let retry_error = RustRabbitError::Retry("test".to_string());
        assert!(!retry_error.is_retryable());
    }

    #[test]
    fn test_connection_error_detection() {
        let conn_error = RustRabbitError::Connection("test".to_string());
        assert!(conn_error.is_connection_error());

        let serialization_error = RustRabbitError::Serialization("test".to_string());
        assert!(!serialization_error.is_connection_error());
    }

    #[test]
    fn test_user_messages() {
        let errors = vec![
            RustRabbitError::Connection("test".to_string()),
            RustRabbitError::Serialization("test".to_string()),
            RustRabbitError::Configuration("test".to_string()),
        ];

        for error in errors {
            let message = error.user_message();
            assert!(!message.is_empty());
            assert!(!message.contains("test")); // User message should not contain internal details
        }
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_error = serde_json::from_str::<i32>("invalid json").unwrap_err();
        let rabbit_error = RustRabbitError::from(json_error);

        matches!(rabbit_error, RustRabbitError::Serialization(_));
    }
}
