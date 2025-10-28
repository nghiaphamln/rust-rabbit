//! Basic Publisher Example
//!
//! This example demonstrates how to publish messages using rust-rabbit.
//! Shows both exchange-based and direct queue publishing.

use rust_rabbit::{Connection, PublishOptions, Publisher};
use serde::Serialize;
use std::time::Duration;
use tracing::{info, Level};

#[derive(Serialize)]
struct Order {
    id: u32,
    customer_id: u32,
    amount: f64,
    status: String,
}

#[derive(Serialize)]
struct Notification {
    recipient: String,
    subject: String,
    body: String,
    priority: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting basic publisher example");

    // Connect to RabbitMQ
    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    let publisher = Publisher::new(connection);

    // Example 1: Publish directly to a queue (simple)
    info!("Publishing messages directly to queue...");

    let order = Order {
        id: 1001,
        customer_id: 123,
        amount: 299.99,
        status: "pending".to_string(),
    };

    // Simple publish with default options
    publisher
        .publish_to_queue("order_queue", &order, None)
        .await?;
    info!("Order {} published to queue", order.id);

    // Example 2: Publish with custom options
    let priority_order = Order {
        id: 1002,
        customer_id: 456,
        amount: 999.99,
        status: "urgent".to_string(),
    };

    let priority_options = PublishOptions::new()
        .with_priority(9) // High priority (0-255)
        .with_expiration("300000"); // 5 minutes TTL in milliseconds

    publisher
        .publish_to_queue("order_queue", &priority_order, Some(priority_options))
        .await?;
    info!("Priority order {} published to queue", priority_order.id);

    // Example 3: Publish to exchange with routing (advanced)
    info!("Publishing messages to exchange...");

    let notification = Notification {
        recipient: "customer@example.com".to_string(),
        subject: "Order Confirmation".to_string(),
        body: "Your order has been received".to_string(),
        priority: 5,
    };

    // Publish to topic exchange with routing key
    publisher
        .publish_to_exchange("notifications", "order.confirmation", &notification, None)
        .await?;
    info!("Notification published to exchange");

    // Example 4: Different message types to different routes
    let urgent_notification = Notification {
        recipient: "admin@example.com".to_string(),
        subject: "High Value Order Alert".to_string(),
        body: "Order over $999 received".to_string(),
        priority: 9,
    };

    let urgent_options = PublishOptions::new()
        .with_priority(9);

    publisher
        .publish_to_exchange(
            "notifications",
            "alert.urgent",
            &urgent_notification,
            Some(urgent_options),
        )
        .await?;
    info!("Urgent notification published to exchange");

    // Example 5: Batch publishing
    info!("Publishing batch of messages...");

    for i in 1..=10 {
        let batch_order = Order {
            id: 2000 + i,
            customer_id: 100 + i,
            amount: 50.0 + (i as f64 * 10.0),
            status: "batch".to_string(),
        };

        publisher
            .publish_to_queue("batch_orders", &batch_order, None)
            .await?;
    }
    info!("Batch of 10 orders published");

    // Example 6: Error handling
    info!("Demonstrating error handling...");

    #[derive(Serialize)]
    struct InvalidMessage {
        // This will serialize fine, but shows error handling pattern
        data: String,
    }

    let invalid_msg = InvalidMessage {
        data: "test message".to_string(),
    };

    match publisher
        .publish_to_queue("test_queue", &invalid_msg, None)
        .await
    {
        Ok(_) => info!("Message published successfully"),
        Err(e) => {
            if e.is_retryable() {
                info!("Retryable error: {}", e);
                // Could implement retry logic here
            } else {
                info!("Permanent error: {}", e);
                // Handle permanent error
            }
        }
    }

    info!("Basic publisher example completed successfully!");
    Ok(())
}

// Helper function to demonstrate retry logic
async fn publish_with_retry(
    publisher: &Publisher,
    queue: &str,
    message: &impl serde::Serialize,
    max_retries: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut attempts = 0;

    loop {
        match publisher.publish_to_queue(queue, message, None).await {
            Ok(_) => {
                if attempts > 0 {
                    info!("Message published successfully after {} retries", attempts);
                }
                return Ok(());
            }
            Err(e) if e.is_retryable() && attempts < max_retries => {
                attempts += 1;
                info!("Publish attempt {} failed: {}", attempts, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let order = Order {
            id: 123,
            customer_id: 456,
            amount: 99.99,
            status: "test".to_string(),
        };

        let json = serde_json::to_string(&order).unwrap();
        assert!(json.contains("123"));
        assert!(json.contains("99.99"));
    }

    #[test]
    fn test_publish_options() {
        let options = PublishOptions::new()
            .persistent(true)
            .priority(5)
            .header("test", "value");

        assert!(options.persistent);
        assert_eq!(options.priority, Some(5));
        assert_eq!(options.headers.get("test"), Some(&"value".to_string()));
    }
}
