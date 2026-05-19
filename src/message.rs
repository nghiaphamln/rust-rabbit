//! Message envelope with metadata for retry tracking and error history
//!
//! This module provides a standardized message format that includes:
//! - Original payload data
//! - Retry tracking (attempt count, max retries)
//! - Error history for debugging failed messages
//! - Timestamps for monitoring and debugging
//! - MassTransit integration support

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Simple wire message format for basic publish/consume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage<T> {
    pub data: T,
    pub retry_attempt: u32,
}

/// MassTransit message envelope format (C# camelCase JSON)
///
/// This structure matches MassTransit's message envelope format for integration
/// with C# services using MassTransit's IBus.
///
/// MassTransit wraps messages in this format when publishing via IBus.
/// The actual message payload is in the `message` field.
///
/// According to MassTransit documentation, messageType should be an array of URNs
/// in the format: "urn:message:Namespace:TypeName"
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MassTransitEnvelope {
    /// Auto-generated message ID (Guid in C#)
    #[serde(default)]
    pub message_id: Option<String>,

    /// Correlation ID for tracking operations (Guid? in C#)
    #[serde(default)]
    pub correlation_id: Option<String>,

    /// Source address (Uri in C#)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_address: Option<String>,

    /// Destination address (Uri in C#)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_address: Option<String>,

    /// Sent time (DateTime? in C#)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_time: Option<DateTime<Utc>>,

    /// Message type array - array of URNs in format "urn:message:Namespace:TypeName"
    /// This is required for MassTransit to properly route messages
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type: Option<Vec<String>>,

    /// Headers (Dictionary<string, object> in C#)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, serde_json::Value>>,

    /// The actual message payload (this is what we care about)
    /// MassTransit wraps the actual message in this field
    pub message: serde_json::Value,
}

impl MassTransitEnvelope {
    /// Create a new MassTransit envelope with a message payload
    /// This automatically generates a message ID (Guid format) and sets sent_time
    pub fn new<T>(message: &T) -> Result<Self, serde_json::Error>
    where
        T: Serialize,
    {
        let message_json = serde_json::to_value(message)?;

        Ok(Self {
            message_id: Some(uuid::Uuid::new_v4().to_string()),
            correlation_id: None,
            source_address: None,
            destination_address: None,
            sent_time: Some(Utc::now()),
            message_type: None,
            headers: None,
            message: message_json,
        })
    }

    /// Convert message type string to URN format if needed
    /// Accepts "Namespace:TypeName" or "urn:message:Namespace:TypeName"
    fn normalize_message_type(message_type: &str) -> String {
        if message_type.starts_with("urn:message:") {
            message_type.to_string()
        } else {
            format!("urn:message:{}", message_type)
        }
    }

    /// Create a MassTransit envelope with message type for proper routing
    /// MassTransit uses message type names for routing (e.g., "YourNamespace:YourMessageType")
    /// The message type will be added as an array in the envelope body AND in headers for full compatibility
    ///
    /// Message type can be in format:
    /// - "Namespace:TypeName" (will be converted to "urn:message:Namespace:TypeName")
    /// - "urn:message:Namespace:TypeName" (used as-is)
    pub fn with_message_type<T>(message: &T, message_type: &str) -> Result<Self, serde_json::Error>
    where
        T: Serialize,
    {
        let mut envelope = Self::new(message)?;

        // Normalize message type to URN format
        let urn_type = Self::normalize_message_type(message_type);

        // Add message type to envelope body (required by MassTransit)
        envelope.message_type = Some(vec![urn_type.clone()]);

        // Also add to headers for full compatibility (MT-Host-MessageType header)
        let mut headers = HashMap::new();
        let message_type_array = vec![serde_json::Value::String(urn_type)];
        headers.insert(
            "MT-Host-MessageType".to_string(),
            serde_json::Value::Array(message_type_array),
        );
        envelope.headers = Some(headers);

        Ok(envelope)
    }

    /// Set correlation ID for tracking operations
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Set source address (typically "rabbitmq://host/exchange")
    pub fn with_source_address(mut self, source_address: impl Into<String>) -> Self {
        self.source_address = Some(source_address.into());
        self
    }

    /// Set destination address (typically "rabbitmq://host/queue")
    pub fn with_destination_address(mut self, destination_address: impl Into<String>) -> Self {
        self.destination_address = Some(destination_address.into());
        self
    }

    /// Add custom header
    pub fn with_header(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        if self.headers.is_none() {
            self.headers = Some(HashMap::new());
        }
        if let Some(ref mut headers) = self.headers {
            headers.insert(key.into(), value);
        }
        self
    }

    /// Set message type in envelope body and headers (required for MassTransit routing)
    /// MassTransit expects message types as an array in both envelope body and MT-Host-MessageType header
    /// Message type can be in format "Namespace:TypeName" or "urn:message:Namespace:TypeName"
    pub fn with_message_type_header(mut self, message_type: &str) -> Self {
        // Normalize to URN format
        let urn_type = Self::normalize_message_type(message_type);

        // Add to envelope body
        self.message_type = Some(vec![urn_type.clone()]);

        // Add to headers
        if self.headers.is_none() {
            self.headers = Some(HashMap::new());
        }
        if let Some(ref mut headers) = self.headers {
            let message_type_array = vec![serde_json::Value::String(urn_type)];
            headers.insert(
                "MT-Host-MessageType".to_string(),
                serde_json::Value::Array(message_type_array),
            );
        }
        self
    }

    /// Extract the actual message payload as the specified type
    pub fn extract_message<T>(&self) -> Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_value(self.message.clone())
    }

    /// Get correlation ID if present
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// Get message ID if present
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    /// Try to deserialize a MassTransit envelope from JSON bytes
    pub fn from_slice(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Message envelope that wraps the actual payload with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope<T> {
    /// The actual message payload
    pub payload: T,

    /// Message metadata
    pub metadata: MessageMetadata,
}

/// Metadata associated with a message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    /// Unique message ID for tracking
    pub message_id: String,

    /// Current retry attempt (0 = first attempt, 1 = first retry, etc.)
    pub retry_attempt: u32,

    /// Maximum number of retry attempts allowed
    pub max_retries: u32,

    /// When the message was first created
    pub created_at: DateTime<Utc>,

    /// When the message was last processed (updated on each retry)
    pub last_processed_at: DateTime<Utc>,

    /// History of errors from previous attempts
    pub error_history: Vec<ErrorRecord>,

    /// Custom headers for additional metadata
    pub headers: HashMap<String, String>,

    /// Source information (queue, exchange, routing key where message originated)
    pub source: MessageSource,
}

/// Record of an error that occurred during message processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRecord {
    /// Which attempt this error occurred on
    pub attempt: u32,

    /// Error message
    pub error: String,

    /// When the error occurred
    pub occurred_at: DateTime<Utc>,

    /// Error category for classification
    pub error_type: ErrorType,

    /// Additional context about the error
    pub context: Option<String>,
}

/// Classification of error types for better handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorType {
    /// Temporary errors that might succeed on retry (network, timeout, etc.)
    Transient,

    /// Permanent errors that won't succeed on retry (validation, auth, etc.)
    Permanent,

    /// Resource errors (rate limit, quota exceeded, etc.)
    Resource,

    /// Unknown error type
    Unknown,
}

/// Information about where the message came from
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSource {
    /// Original queue name
    pub queue: String,

    /// Exchange name (if any)
    pub exchange: Option<String>,

    /// Routing key used
    pub routing_key: Option<String>,

    /// Application or service that published the message
    pub publisher: Option<String>,
}

impl<T> MessageEnvelope<T> {
    /// Create a new message envelope with the given payload
    pub fn new(payload: T, source_queue: &str) -> Self {
        let now = Utc::now();

        Self {
            payload,
            metadata: MessageMetadata {
                message_id: uuid::Uuid::new_v4().to_string(),
                retry_attempt: 0,
                max_retries: 0, // Will be set by retry config
                created_at: now,
                last_processed_at: now,
                error_history: Vec::new(),
                headers: HashMap::new(),
                source: MessageSource {
                    queue: source_queue.to_string(),
                    exchange: None,
                    routing_key: None,
                    publisher: None,
                },
            },
        }
    }

    /// Create envelope with source details
    pub fn with_source(
        payload: T,
        queue: &str,
        exchange: Option<&str>,
        routing_key: Option<&str>,
        publisher: Option<&str>,
    ) -> Self {
        let mut envelope = Self::new(payload, queue);
        envelope.metadata.source.exchange = exchange.map(|s| s.to_string());
        envelope.metadata.source.routing_key = routing_key.map(|s| s.to_string());
        envelope.metadata.source.publisher = publisher.map(|s| s.to_string());
        envelope
    }

    /// Set the maximum number of retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.metadata.max_retries = max_retries;
        self
    }

    /// Add a custom header
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.metadata
            .headers
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Check if this message has exceeded its retry limit
    ///
    /// `retry_attempt` tracks the number of retries already scheduled/performed.
    /// A message is exhausted only after it has gone beyond `max_retries`.
    pub fn is_retry_exhausted(&self) -> bool {
        self.metadata.retry_attempt > self.metadata.max_retries
    }

    /// Check if this is the first attempt (not a retry)
    pub fn is_first_attempt(&self) -> bool {
        self.metadata.retry_attempt == 0
    }

    /// Get the next retry attempt number
    pub fn next_retry_attempt(&self) -> u32 {
        self.metadata.retry_attempt + 1
    }

    /// Record an error and create a new envelope for retry
    pub fn with_error(mut self, error: &str, error_type: ErrorType, context: Option<&str>) -> Self {
        // Record the error
        let error_record = ErrorRecord {
            attempt: self.metadata.retry_attempt,
            error: error.to_string(),
            occurred_at: Utc::now(),
            error_type,
            context: context.map(|s| s.to_string()),
        };

        self.metadata.error_history.push(error_record);

        // Increment retry attempt
        self.metadata.retry_attempt += 1;
        self.metadata.last_processed_at = Utc::now();

        self
    }

    /// Get the last error if any
    pub fn last_error(&self) -> Option<&ErrorRecord> {
        self.metadata.error_history.last()
    }

    /// Get all errors of a specific type
    pub fn errors_by_type(&self, error_type: &ErrorType) -> Vec<&ErrorRecord> {
        self.metadata
            .error_history
            .iter()
            .filter(|e| std::mem::discriminant(&e.error_type) == std::mem::discriminant(error_type))
            .collect()
    }

    /// Get a summary string for dead letter analysis
    pub fn get_failure_summary(&self) -> String {
        let total_errors = self.metadata.error_history.len();
        let last_error = self.last_error();

        match last_error {
            Some(error) => {
                format!(
                    "Message {} failed after {} attempts. Last error (attempt {}): {} [{}]",
                    self.metadata.message_id,
                    total_errors,
                    error.attempt + 1,
                    error.error,
                    match error.error_type {
                        ErrorType::Transient => "TRANSIENT",
                        ErrorType::Permanent => "PERMANENT",
                        ErrorType::Resource => "RESOURCE",
                        ErrorType::Unknown => "UNKNOWN",
                    }
                )
            }
            None => format!("Message {} has no error history", self.metadata.message_id),
        }
    }

    /// Convert to JSON for debugging
    pub fn to_debug_json(&self) -> Result<String, serde_json::Error>
    where
        T: Serialize,
    {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        id: u32,
        name: String,
    }

    #[test]
    fn test_message_envelope_creation() {
        let payload = TestPayload {
            id: 123,
            name: "test".to_string(),
        };

        let envelope = MessageEnvelope::new(payload.clone(), "test_queue").with_max_retries(3); // Set max retries so it's not exhausted initially

        assert_eq!(envelope.payload, payload);
        assert_eq!(envelope.metadata.retry_attempt, 0);
        assert_eq!(envelope.metadata.source.queue, "test_queue");
        assert!(envelope.is_first_attempt());
        assert!(!envelope.is_retry_exhausted());
    }

    #[test]
    fn test_error_tracking() {
        let payload = TestPayload {
            id: 123,
            name: "test".to_string(),
        };

        let envelope = MessageEnvelope::new(payload, "test_queue")
            .with_max_retries(3)
            .with_error("First error", ErrorType::Transient, Some("Network timeout"))
            .with_error("Second error", ErrorType::Resource, Some("Rate limited"));

        assert_eq!(envelope.metadata.retry_attempt, 2);
        assert_eq!(envelope.metadata.error_history.len(), 2);
        assert!(!envelope.is_retry_exhausted());

        let last_error = envelope.last_error().unwrap();
        assert_eq!(last_error.error, "Second error");
        assert_eq!(last_error.attempt, 1);
    }

    #[test]
    fn test_retry_exhaustion() {
        let payload = TestPayload {
            id: 123,
            name: "test".to_string(),
        };

        let envelope = MessageEnvelope::new(payload, "test_queue")
            .with_max_retries(2)
            .with_error("Error 1", ErrorType::Transient, None)
            .with_error("Error 2", ErrorType::Transient, None)
            .with_error("Error 3", ErrorType::Permanent, None);

        assert!(envelope.is_retry_exhausted());
        assert_eq!(envelope.next_retry_attempt(), 4);
    }

    #[test]
    fn test_failure_summary() {
        let payload = TestPayload {
            id: 123,
            name: "test".to_string(),
        };

        let envelope = MessageEnvelope::new(payload, "test_queue")
            .with_max_retries(2)
            .with_error(
                "Database connection failed",
                ErrorType::Transient,
                Some("Timeout after 5s"),
            )
            .with_error("Invalid data format", ErrorType::Permanent, None);

        let summary = envelope.get_failure_summary();
        assert!(summary.contains("failed after 2 attempts"));
        assert!(summary.contains("Invalid data format"));
        assert!(summary.contains("PERMANENT"));
    }

    #[test]
    fn test_masstransit_envelope_deserialization() {
        // Simulate MassTransit JSON format (camelCase)
        let masstransit_json = r#"{
            "messageId": "123e4567-e89b-12d3-a456-426614174000",
            "correlationId": "987fcdeb-51a2-43d7-b890-123456789abc",
            "sourceAddress": "rabbitmq://localhost/test",
            "destinationAddress": "rabbitmq://localhost/queue",
            "message": {
                "id": 123,
                "name": "test message"
            }
        }"#;

        let envelope: MassTransitEnvelope = serde_json::from_str(masstransit_json).unwrap();

        assert_eq!(
            envelope.message_id,
            Some("123e4567-e89b-12d3-a456-426614174000".to_string())
        );
        assert_eq!(
            envelope.correlation_id,
            Some("987fcdeb-51a2-43d7-b890-123456789abc".to_string())
        );

        // Extract the actual message
        let payload: TestPayload = envelope.extract_message().unwrap();
        assert_eq!(payload.id, 123);
        assert_eq!(payload.name, "test message");
    }

    #[test]
    fn test_masstransit_envelope_minimal() {
        // Minimal MassTransit envelope (only message field)
        let minimal_json = r#"{
            "message": {
                "id": 456,
                "name": "minimal test"
            }
        }"#;

        let envelope: MassTransitEnvelope = serde_json::from_str(minimal_json).unwrap();
        assert_eq!(envelope.message_id, None);
        assert_eq!(envelope.correlation_id, None);

        let payload: TestPayload = envelope.extract_message().unwrap();
        assert_eq!(payload.id, 456);
        assert_eq!(payload.name, "minimal test");
    }

    #[test]
    fn test_masstransit_correlation_id_extraction() {
        let json = r#"{
            "correlationId": "test-correlation-id",
            "message": {"id": 1, "name": "test"}
        }"#;

        let envelope: MassTransitEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.correlation_id(), Some("test-correlation-id"));
        assert_eq!(envelope.message_id(), None);
    }
}
