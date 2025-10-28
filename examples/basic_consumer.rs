//! Basic Consumer Example (Simplified)
//!
//! This example demonstrates core Consumer functionality:
//! - Simple consumption
//! - Retry configuration  
//! - Exchange binding
//! - Error handling patterns

use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Deserialize, Serialize, Debug, Clone)]
struct Order {
    id: u32,
    customer_id: u32,
    amount: f64,
    status: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct Notification {
    recipient: String,
    subject: String,
    priority: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();
    info!("🚀 Starting basic consumer examples");

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

    // Start all consumer types
    let _handles = vec![
        start_simple_consumer(connection.clone()),
        start_retry_consumer(connection.clone()),
        start_exchange_consumer(connection.clone()),
        start_manual_ack_consumer(connection.clone()),
    ];

    info!("✅ All consumers started. Press Ctrl+C to stop.");

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal, stopping consumers...");
    
    // In a real application, you would gracefully shutdown the consumers here
    Ok(())
}

// Example 1: Simple consumer without retry
fn start_simple_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "simple_orders")
            .concurrency(5)
            .build();

        consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
            let order = &msg.data;
            info!("🔹 Simple - Processing order {} (${:.2})", order.id, order.amount);
            
            // Basic validation
            if order.amount <= 0.0 {
                error!("Invalid amount for order {}", order.id);
                return Err("Invalid amount".into());
            }
            
            // Simulate processing
            tokio::time::sleep(Duration::from_millis(100)).await;
            info!("✅ Order {} processed", order.id);
            Ok(())
        }).await.map_err(|e| e.into())
    })
}

// Example 2: Consumer with retry configuration
fn start_retry_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "retry_orders")
            .with_retry(RetryConfig::exponential_default()) // 1s→2s→4s→8s→16s
            .concurrency(3)
            .build();

        consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
            let order = &msg.data;
            info!("🔄 Retry - Processing order {} (attempt {})", order.id, msg.retry_attempt + 1);
            
            // Simulate different error scenarios
            match order.status.as_str() {
                "invalid" => {
                    error!("Invalid order - not retrying");
                    Ok(()) // Don't retry invalid data
                }
                "network_error" => {
                    warn!("Network error - will retry");
                    Err("Network temporarily unavailable".into())
                }
                _ => {
                    // Normal processing
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    info!("✅ Order {} processed successfully", order.id);
                    Ok(())
                }
            }
        }).await.map_err(|e| e.into())
    })
}

// Example 3: Consumer with exchange binding
fn start_exchange_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "notifications")
            .bind_to_exchange("notifications", "order.*")
            .with_retry(RetryConfig::linear(2, Duration::from_secs(5)))
            .concurrency(10)
            .build();

        consumer.consume(|msg: rust_rabbit::Message<Notification>| async move {
            let notification = &msg.data;
            info!("📧 Exchange - Processing notification for {}", notification.recipient);
            
            // Priority-based processing
            let processing_time = match notification.priority {
                9..=10 => Duration::from_millis(50),  // Critical - fast
                6..=8 => Duration::from_millis(100),  // High priority
                _ => Duration::from_millis(200),      // Normal
            };
            
            tokio::time::sleep(processing_time).await;
            
            // Simulate sending notification
            if notification.recipient.contains("invalid") {
                warn!("Invalid recipient - will retry");
                return Err("Invalid recipient".into());
            }
            
            info!("✅ Notification sent to {}", notification.recipient);
            Ok(())
        }).await.map_err(|e| e.into())
    })
}

// Example 4: Manual ACK consumer
fn start_manual_ack_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "manual_orders")
            .manual_ack() // Disable auto-acknowledge
            .concurrency(2)
            .build();

        consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
            let order = &msg.data;
            info!("🤲 Manual ACK - Processing order {}", order.id);
            
            // Simulate processing
            tokio::time::sleep(Duration::from_millis(300)).await;
            
            if order.amount > 1000.0 {
                info!("High value order {} - manual verification needed", order.id);
                // In real scenario, you might want to defer ACK until manual verification
                
                // For demo, we'll ACK it
                msg.ack().await.map_err(|e| {
                    error!("Failed to ACK message: {}", e);
                    e
                })?;
                
                info!("✅ High value order {} manually verified and ACKed", order.id);
            } else {
                // Normal processing - ACK immediately
                msg.ack().await.map_err(|e| {
                    error!("Failed to ACK message: {}", e);
                    e
                })?;
                
                info!("✅ Order {} processed and ACKed", order.id);
            }
            
            Ok(())
        }).await.map_err(|e| e.into())
    })
}

// Helper function to publish test messages (run separately)
#[allow(dead_code)]
async fn publish_test_messages() -> Result<(), Box<dyn std::error::Error>> {
    use rust_rabbit::Publisher;
    
    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    let publisher = Publisher::new(connection);
    
    // Publish to different queues
    let test_orders = vec![
        ("simple_orders", Order { id: 1, customer_id: 100, amount: 99.99, status: "pending".to_string() }),
        ("retry_orders", Order { id: 2, customer_id: 101, amount: 199.99, status: "network_error".to_string() }),
        ("manual_orders", Order { id: 3, customer_id: 102, amount: 1999.99, status: "pending".to_string() }),
    ];
    
    for (queue, order) in test_orders {
        publisher.publish_to_queue(queue, &order, None).await?;
        info!("📤 Published order {} to {}", order.id, queue);
    }
    
    // Publish notification to exchange
    let notification = Notification {
        recipient: "customer@example.com".to_string(),
        subject: "Order confirmation".to_string(),
        priority: 7,
    };
    
    publisher.publish_to_exchange("notifications", "order.created", &notification, None).await?;
    info!("📤 Published notification to exchange");
    
    Ok(())
}