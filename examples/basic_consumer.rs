//! Basic Consumer Example
//!
//! This example demonstrates how to consume messages using rust-rabbit.
//! Shows basic consumption, error handling, and retry configuration.

use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::Deserialize;
use std::time::Duration;
use tracing::{error, info, warn, Level};

#[derive(Deserialize, Debug)]
struct Order {
    id: u32,
    customer_id: u32,
    amount: f64,
    status: String,
}

#[derive(Deserialize, Debug)]
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

    info!("Starting basic consumer example");

    // Connect to RabbitMQ
    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

    // Example 1: Simple consumer without retry
    info!("Starting simple consumer...");

    let simple_consumer = Consumer::builder(connection.clone(), "simple_queue")
        .concurrency(5) // Process up to 5 messages in parallel
        .build()
        .await?;

    // Spawn simple consumer in background
    let simple_handle = tokio::spawn(async move {
        simple_consumer
            .consume(|order: Order| async move {
                info!(
                    "Processing simple order: {} for customer {} (${:.2})",
                    order.id, order.customer_id, order.amount
                );

                // Simulate processing time
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Simple validation
                if order.amount < 0.0 {
                    error!("Invalid order amount: {}", order.amount);
                    return Err("Invalid amount".into());
                }

                info!("Order {} processed successfully", order.id);
                Ok(()) // ACK the message
            })
            .await
    });

    // Example 2: Consumer with retry configuration
    info!("Starting consumer with retry...");

    let retry_consumer = Consumer::builder(connection.clone(), "order_queue")
        .retry(RetryConfig::exponential_default()) // 1s->2s->4s->8s->16s (5 retries)
        .concurrency(3)
        .build()
        .await?;

    let retry_handle = tokio::spawn(async move {
        retry_consumer
            .consume(|order: Order| async move {
                info!(
                    "Processing order with retry: {} (${:.2})",
                    order.id, order.amount
                );

                // Simulate different types of errors
                match order.status.as_str() {
                    "invalid" => {
                        error!("Invalid order status - not retrying");
                        Ok(()) // Don't retry invalid data
                    }
                    "network_error" => {
                        warn!("Simulating network error - will retry");
                        Err("Network temporarily unavailable".into()) // Will retry
                    }
                    "rate_limited" => {
                        warn!("Simulating rate limit - will retry");
                        Err("Rate limit exceeded".into()) // Will retry
                    }
                    _ => {
                        // Normal processing
                        match process_order(order).await {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e),
                        }
                    }
                }
            })
            .await
    });

    // Example 3: Consumer with exchange binding
    info!("Starting consumer with exchange binding...");

    let exchange_consumer = Consumer::builder(connection.clone(), "notification_queue")
        .bind_to_exchange("notifications")
        .routing_key("order.*") // Receive all order-related notifications
        .retry(RetryConfig::linear(3, Duration::from_secs(5))) // 3 retries, 5s each
        .concurrency(10)
        .build()
        .await?;

    let exchange_handle = tokio::spawn(async move {
        exchange_consumer
            .consume(|notification: Notification| async move {
                info!(
                    "Processing notification: {} (priority: {})",
                    notification.subject, notification.priority
                );

                // Simulate notification processing
                match send_notification(&notification).await {
                    Ok(_) => {
                        info!("Notification sent to {}", notification.recipient);
                        Ok(())
                    }
                    Err(e) => {
                        if notification.priority >= 8 {
                            // High priority notifications - retry
                            warn!("High priority notification failed, will retry: {}", e);
                            Err(e)
                        } else {
                            // Low priority - don't retry
                            warn!("Low priority notification failed, not retrying: {}", e);
                            Ok(())
                        }
                    }
                }
            })
            .await
    });

    // Example 4: Consumer with custom retry pattern
    info!("Starting consumer with custom retry...");

    let custom_retry = RetryConfig::custom(vec![
        Duration::from_secs(1),    // First retry after 1 second
        Duration::from_secs(5),    // Second retry after 5 seconds
        Duration::from_secs(30),   // Third retry after 30 seconds
        Duration::from_minutes(5), // Fourth retry after 5 minutes
    ]);

    let custom_consumer = Consumer::builder(connection.clone(), "api_calls")
        .retry(custom_retry)
        .concurrency(2)
        .build()
        .await?;

    let custom_handle = tokio::spawn(async move {
        custom_consumer
            .consume(|request: serde_json::Value| async move {
                info!("Processing API request: {:?}", request);

                // Simulate API call processing
                match external_api_call(&request).await {
                    Ok(response) => {
                        info!("API call successful: {:?}", response);
                        Ok(())
                    }
                    Err(ApiError::RateLimit) => {
                        warn!("API rate limited - will retry with custom delays");
                        Err("API rate limit exceeded".into())
                    }
                    Err(ApiError::ServerError) => {
                        warn!("API server error - will retry");
                        Err("API server error".into())
                    }
                    Err(ApiError::BadRequest) => {
                        error!("Bad API request - not retrying");
                        Ok(()) // Don't retry client errors
                    }
                }
            })
            .await
    });

    // Example 5: Consumer for batch processing
    info!("Starting batch consumer...");

    let batch_consumer = Consumer::builder(connection.clone(), "batch_orders")
        .retry(RetryConfig::exponential(
            3,
            Duration::from_millis(500),
            Duration::from_secs(30),
        ))
        .concurrency(1) // Process one batch at a time
        .build()
        .await?;

    let batch_handle = tokio::spawn(async move {
        batch_consumer
            .consume(|order: Order| async move {
                info!("Processing batch order: {}", order.id);

                // Simulate batch processing
                match process_batch_order(order).await {
                    Ok(_) => Ok(()),
                    Err(e) if is_retryable_error(&e) => {
                        warn!("Retryable batch error: {}", e);
                        Err(e)
                    }
                    Err(e) => {
                        error!("Non-retryable batch error: {}", e);
                        Ok(()) // Don't retry
                    }
                }
            })
            .await
    });

    info!("All consumers started. Press Ctrl+C to stop...");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, stopping consumers...");

    // Gracefully stop consumers
    simple_handle.abort();
    retry_handle.abort();
    exchange_handle.abort();
    custom_handle.abort();
    batch_handle.abort();

    info!("Basic consumer example completed!");
    Ok(())
}

// Simulate order processing
async fn process_order(order: Order) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simulate processing time
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Business logic validation
    if order.amount <= 0.0 {
        return Err("Invalid order amount".into());
    }

    if order.customer_id == 0 {
        return Err("Invalid customer ID".into());
    }

    // Simulate occasional transient errors
    if order.id % 10 == 0 {
        return Err("Database temporarily unavailable".into());
    }

    info!("Order {} processed: ${:.2}", order.id, order.amount);
    Ok(())
}

// Simulate notification sending
async fn send_notification(
    notification: &Notification,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Simulate occasional failures
    if notification.recipient.contains("invalid") {
        return Err("Invalid email address".into());
    }

    if notification.priority >= 9 {
        // Simulate rate limiting for high priority
        if fastrand::f32() < 0.3 {
            return Err("Rate limit exceeded".into());
        }
    }

    Ok(format!("Notification sent to {}", notification.recipient))
}

// Simulate external API call
#[derive(Debug)]
enum ApiError {
    RateLimit,
    ServerError,
    BadRequest,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::RateLimit => write!(f, "Rate limit exceeded"),
            ApiError::ServerError => write!(f, "Server error"),
            ApiError::BadRequest => write!(f, "Bad request"),
        }
    }
}

impl std::error::Error for ApiError {}

async fn external_api_call(request: &serde_json::Value) -> Result<serde_json::Value, ApiError> {
    // Simulate API delay
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Simulate different types of API errors
    let random = fastrand::f32();

    if random < 0.1 {
        Err(ApiError::RateLimit)
    } else if random < 0.2 {
        Err(ApiError::ServerError)
    } else if random < 0.25 {
        Err(ApiError::BadRequest)
    } else {
        Ok(serde_json::json!({
            "status": "success",
            "request_id": fastrand::u64(..),
            "response": "API call processed successfully"
        }))
    }
}

// Simulate batch processing
async fn process_batch_order(order: Order) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting batch processing for order {}", order.id);

    // Simulate longer processing time for batches
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Simulate batch-specific errors
    if order.amount > 500.0 {
        // Large orders need special handling
        if fastrand::f32() < 0.2 {
            return Err("Large order processing failed - retry".into());
        }
    }

    info!("Batch order {} completed successfully", order.id);
    Ok(())
}

// Helper function to classify errors
fn is_retryable_error(error: &Box<dyn std::error::Error + Send + Sync>) -> bool {
    let error_msg = error.to_string().to_lowercase();

    // Classify errors as retryable or not
    error_msg.contains("timeout")
        || error_msg.contains("network")
        || error_msg.contains("temporarily")
        || error_msg.contains("rate limit")
        || error_msg.contains("server error")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_process_order() {
        let valid_order = Order {
            id: 1,
            customer_id: 123,
            amount: 99.99,
            status: "pending".to_string(),
        };

        let result = process_order(valid_order).await;
        assert!(result.is_ok());

        let invalid_order = Order {
            id: 2,
            customer_id: 0,
            amount: -10.0,
            status: "pending".to_string(),
        };

        let result = process_order(invalid_order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_notification() {
        let valid_notification = Notification {
            recipient: "test@example.com".to_string(),
            subject: "Test".to_string(),
            body: "Test body".to_string(),
            priority: 5,
        };

        let result = send_notification(&valid_notification).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_error_classification() {
        let retryable_error: Box<dyn std::error::Error + Send + Sync> = "Network timeout".into();
        assert!(is_retryable_error(&retryable_error));

        let permanent_error: Box<dyn std::error::Error + Send + Sync> =
            "Invalid data format".into();
        assert!(!is_retryable_error(&permanent_error));
    }
}
