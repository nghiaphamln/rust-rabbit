//! Production Setup Example (Final Simplified)
//!
//! This example demonstrates production-ready patterns:
//! - Multiple consumers with different retry strategies
//! - Error handling and metrics
//! - Graceful shutdown
//! - Message publishing patterns

use anyhow::Result;
use rust_rabbit::{Connection, Consumer, PublishOptions, Publisher, RetryConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderEvent {
    order_id: String,
    user_id: String,
    amount: f64,
    event_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct NotificationTask {
    recipient: String,
    message: String,
    priority: u8, // 1-10
}

#[derive(Default, Debug)]
struct Stats {
    orders_processed: u64,
    notifications_sent: u64,
    errors: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    info!("🚀 Starting production setup");

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    let stats = Arc::new(RwLock::new(Stats::default()));

    // Start background tasks
    let _order_handle = tokio::spawn(start_order_processor(connection.clone(), stats.clone()));
    let _notification_handle = tokio::spawn(start_notification_processor(
        connection.clone(),
        stats.clone(),
    ));
    let _metrics_handle = tokio::spawn(start_metrics_reporter(stats.clone()));
    let _producer_handle = tokio::spawn(start_message_producer(connection.clone()));

    info!("✅ All services started. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("🛑 Shutdown signal received");

    // Wait a bit for graceful shutdown
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("✅ Application shutdown complete");
    Ok(())
}

// Order processing with exponential retry
async fn start_order_processor(
    connection: Arc<Connection>,
    stats: Arc<RwLock<Stats>>,
) -> Result<()> {
    let consumer = Consumer::builder(connection, "orders")
        .with_retry(RetryConfig::exponential_default())
        .concurrency(10)
        .build();

    consumer
        .consume(move |msg: rust_rabbit::Message<OrderEvent>| {
            let stats = Arc::clone(&stats);
            async move {
                let order = &msg.data;
                info!("Processing order {}: ${}", order.order_id, order.amount);

                // Simulate processing
                if order.amount > 1000.0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                // Simulate occasional failures
                if order.order_id.ends_with("999") {
                    return Err("Processing failed - will retry".into());
                }

                // Update stats
                {
                    let mut s = stats.write().await;
                    s.orders_processed += 1;
                }

                info!("✅ Order {} processed successfully", order.order_id);
                Ok(())
            }
        })
        .await
        .map_err(|e| e.into())
}

// Notification processing with priority handling
async fn start_notification_processor(
    connection: Arc<Connection>,
    stats: Arc<RwLock<Stats>>,
) -> Result<()> {
    let consumer = Consumer::builder(connection, "notifications")
        .bind_to_exchange("notifications", "notification.*")
        .with_retry(RetryConfig::linear(3, Duration::from_secs(5)))
        .concurrency(15)
        .build();

    consumer
        .consume(move |msg: rust_rabbit::Message<NotificationTask>| {
            let stats = Arc::clone(&stats);
            async move {
                let notification = &msg.data;
                info!("Sending notification to {}", notification.recipient);

                // Priority-based processing
                let processing_time = match notification.priority {
                    9..=10 => Duration::from_millis(100), // Critical
                    6..=8 => Duration::from_millis(200),  // High
                    _ => Duration::from_millis(300),      // Normal
                };

                tokio::time::sleep(processing_time).await;

                // Update stats
                {
                    let mut s = stats.write().await;
                    s.notifications_sent += 1;
                }

                info!("✅ Notification sent to {}", notification.recipient);
                Ok(())
            }
        })
        .await
        .map_err(|e| e.into())
}

// Metrics reporting
async fn start_metrics_reporter(stats: Arc<RwLock<Stats>>) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;
        let s = stats.read().await;
        info!(
            "📊 Stats - Orders: {}, Notifications: {}, Errors: {}",
            s.orders_processed, s.notifications_sent, s.errors
        );
    }
}

// Message producer for testing
async fn start_message_producer(connection: Arc<Connection>) -> Result<()> {
    let publisher = Publisher::new(connection);
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    let mut counter = 1;

    loop {
        interval.tick().await;

        // Publish test order
        let order = OrderEvent {
            order_id: format!("ORD-{:06}", counter),
            user_id: format!("user-{}", counter % 100),
            amount: (counter as f64) * 99.99,
            event_type: "created".to_string(),
        };

        if let Err(e) = publisher.publish_to_queue("orders", &order, None).await {
            warn!("Failed to publish order: {}", e);
        }

        // Publish test notification
        let notification = NotificationTask {
            recipient: format!("user{}@example.com", counter % 50),
            message: format!("Order {} has been created", order.order_id),
            priority: if counter % 10 == 0 { 9 } else { 5 },
        };

        let options = if notification.priority >= 9 {
            Some(PublishOptions::new().priority(9))
        } else {
            None
        };

        if let Err(e) = publisher
            .publish_to_exchange(
                "notifications",
                "notification.order",
                &notification,
                options,
            )
            .await
        {
            warn!("Failed to publish notification: {}", e);
        }

        counter += 1;
    }
}
