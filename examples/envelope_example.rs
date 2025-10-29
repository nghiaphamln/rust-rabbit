//! Example demonstrating MessageEnvelope usage with retry tracking
//!
//! This example shows how to use the new MessageEnvelope system for
//! publishing and consuming messages with built-in retry metadata.

use rust_rabbit::{Connection, Consumer, MessageEnvelope, Publisher, RetryConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u32,
    customer_id: u32,
    amount: f64,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    println!("🚀 Starting MessageEnvelope example");

    // Connect to RabbitMQ
    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

    // Example 1: Publishing with envelope
    println!("\n📤 Publishing messages with envelopes...");

    let publisher = Publisher::new(connection.clone());

    let order = Order {
        id: 1001,
        customer_id: 123,
        amount: 99.99,
        status: "pending".to_string(),
    };

    // Publish with envelope (includes retry metadata)
    publisher
        .publish_with_envelope_to_queue("order_queue", &order, 5, None)
        .await?;

    println!(
        "✅ Published order {} with envelope (max 5 retries)",
        order.id
    );

    // Example 2: Manual envelope creation
    let envelope = MessageEnvelope::new(order.clone(), "order_queue")
        .with_max_retries(3)
        .with_header("source", "api-server")
        .with_header("priority", "high");

    publisher
        .publish_envelope_to_queue("priority_orders", &envelope, None)
        .await?;

    println!("✅ Published priority order with custom envelope");

    // Example 3: Consuming with envelope support
    println!("\n📥 Starting envelope consumer...");

    let consumer = Consumer::builder(connection, "order_queue")
        .with_retry(RetryConfig::exponential(
            3,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(30),
        ))
        .with_prefetch(5)
        .build();

    // Consume envelopes (includes retry tracking)
    consumer
        .consume_envelopes(|envelope: MessageEnvelope<Order>| async move {
            let order = &envelope.payload;

            println!(
                "🔄 Processing order {} (attempt {}/{}, id: {})",
                order.id,
                envelope.metadata.retry_attempt + 1,
                envelope.metadata.max_retries + 1,
                envelope.metadata.message_id
            );

            // Simulate processing logic with potential errors
            match order.status.as_str() {
                "pending" => {
                    println!("✅ Order {} processed successfully", order.id);
                    Ok(())
                }
                "invalid" => {
                    // Permanent error - don't retry
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid order data - validation failed",
                    )
                    .into())
                }
                "network_error" => {
                    // Transient error - will retry
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Network timeout - external service unavailable",
                    )
                    .into())
                }
                _ => {
                    println!(
                        "✅ Order {} processed with status: {}",
                        order.id, order.status
                    );
                    Ok(())
                }
            }
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_rabbit::{ErrorRecord, MessageMetadata, MessageSource};

    #[test]
    fn test_envelope_creation() {
        let order = Order {
            id: 123,
            customer_id: 456,
            amount: 99.99,
            status: "pending".to_string(),
        };

        let envelope = MessageEnvelope::new(order.clone(), "test_queue")
            .with_max_retries(5)
            .with_header("test", "value");

        assert_eq!(envelope.payload.id, 123);
        assert_eq!(envelope.metadata.max_retries, 5);
        assert_eq!(envelope.metadata.retry_attempt, 0);
        assert!(envelope.metadata.headers.contains_key("test"));
        assert!(!envelope.is_retry_exhausted());
        assert!(envelope.is_first_attempt());
    }

    #[test]
    fn test_error_tracking() {
        let order = Order {
            id: 123,
            customer_id: 456,
            amount: 99.99,
            status: "pending".to_string(),
        };

        let envelope = MessageEnvelope::new(order, "test_queue")
            .with_max_retries(3)
            .with_error("First error", ErrorType::Transient, Some("Network timeout"))
            .with_error("Second error", ErrorType::Resource, Some("Rate limited"));

        assert_eq!(envelope.metadata.retry_attempt, 2);
        assert_eq!(envelope.metadata.error_history.len(), 2);

        let last_error = envelope.last_error().unwrap();
        assert_eq!(last_error.error, "Second error");
        assert_eq!(last_error.attempt, 1);

        let failure_summary = envelope.get_failure_summary();
        assert!(failure_summary.contains("failed after 2 attempts"));
        assert!(failure_summary.contains("Second error"));
    }

    #[test]
    fn test_retry_exhaustion() {
        let order = Order {
            id: 123,
            customer_id: 456,
            amount: 99.99,
            status: "pending".to_string(),
        };

        let envelope = MessageEnvelope::new(order, "test_queue")
            .with_max_retries(2)
            .with_error("Error 1", ErrorType::Transient, None)
            .with_error("Error 2", ErrorType::Transient, None)
            .with_error("Error 3", ErrorType::Permanent, None);

        assert!(envelope.is_retry_exhausted());
        assert_eq!(envelope.next_retry_attempt(), 4);
    }
}
