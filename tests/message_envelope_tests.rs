use rust_rabbit::{ErrorType, MassTransitEnvelope, MessageEnvelope};
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

    let envelope = MessageEnvelope::new(payload.clone(), "test_queue").with_max_retries(3);

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

    let payload: TestPayload = envelope.extract_message().unwrap();
    assert_eq!(payload.id, 123);
    assert_eq!(payload.name, "test message");
}

#[test]
fn test_masstransit_envelope_minimal() {
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
