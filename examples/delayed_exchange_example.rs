//! Example demonstrating RabbitMQ delayed message exchange plugin for retry delays
//!
//! This example shows how to use the DelayedExchange strategy for retrying failed messages.
//!
//! **Requirements**: This example requires the `rabbitmq_delayed_message_exchange` plugin:
//! - Download: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases
//! - Install: Place the .ez file in RabbitMQ plugins directory and enable
//! - RabbitMQ CLI: `rabbitmq-plugins enable rabbitmq_delayed_message_exchange`
//!
//! **Flow**:
//! 1. Consumer receives message from queue
//! 2. Handler processes message (may fail)
//! 3. On error, message is published to delay exchange with x-delay header
//! 4. Delay exchange automatically routes message back to original queue after delay
//! 5. Consumer retries processing the message
//! 6. After max retries exceeded, message goes to DLQ

use rust_rabbit::{Connection, Consumer, DelayStrategy, Message, Publisher, RetryConfig};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Task {
    id: u32,
    name: String,
    priority: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("=== RabbitMQ Delayed Message Exchange Retry Example ===");
    info!("This example requires the rabbitmq_delayed_message_exchange plugin");

    // Connection
    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    info!("✓ Connected to RabbitMQ");

    // Setup publisher to publish test messages
    let publisher = Publisher::new(connection.clone());

    // Publish some test messages
    info!("\n📤 Publishing test messages...");
    for i in 1..=3 {
        let task = Task {
            id: i,
            name: format!("Task {}", i),
            priority: (i % 3 + 1) as u8,
        };
        publisher
            .publish_to_queue("task_queue", &task, None)
            .await?;
        info!("  Published: {:?}", task);
    }

    // Consumer setup with DelayedExchange strategy
    // This will use the delayed message exchange plugin for retry delays
    let retry_config = RetryConfig::exponential(3, Duration::from_secs(2), Duration::from_secs(30))
        .with_delay_strategy(DelayStrategy::DelayedExchange); // Use delayed exchange!

    info!("\n⚙️  Consumer Configuration:");
    info!("  - Strategy: DelayedExchange (rabbitmq_delayed_message_exchange plugin)");
    info!("  - Max retries: {}", retry_config.max_retries);
    info!("  - Backoff: Exponential (2s base, 30s max)");

    let consumer = Consumer::builder(connection.clone(), "task_queue")
        .with_retry(retry_config)
        .build();

    // Track how many times each message is processed
    let attempt_counter = Arc::new(AtomicU32::new(0));

    info!("\n🔄 Starting consumer with delayed exchange retry strategy...\n");

    let counter = attempt_counter.clone();
    consumer
        .consume(move |msg: Message<Task>| {
            let counter = counter.clone();
            async move {
                let _attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                info!(
                    "Processing task {} (attempt {}): {}",
                    msg.data.id,
                    msg.retry_attempt + 1,
                    msg.data.name
                );

                // Simulate processing with occasional failures
                if msg.data.id.is_multiple_of(2) && msg.retry_attempt < 2 {
                    // Fail even-numbered tasks on first 2 attempts
                    warn!("  ❌ Processing failed for task {}", msg.data.id);
                    return Err("Processing error".into());
                }

                if msg.data.id == 3 && msg.retry_attempt == 0 {
                    // Fail task 3 on first attempt
                    warn!("  ❌ Processing failed for task 3");
                    return Err("Processing error".into());
                }

                // Success
                info!(
                    "  ✓ Task {} processed successfully after {} attempt(s)",
                    msg.data.id,
                    msg.retry_attempt + 1
                );
                Ok(())
            }
        })
        .await?;

    Ok(())
}

/// Example flow for different scenarios:
///
/// **Scenario 1: Immediate Success**
/// - Task arrives in queue
/// - Handler processes successfully
/// - Message acknowledged
/// - ✓ Done (no retries needed)
///
/// **Scenario 2: Success After Retry**
/// - Task arrives in queue
/// - Handler fails (attempt 1)
/// - Message published to delay exchange with 2s delay
/// - After 2s, message requeued to task_queue
/// - Handler processes successfully (attempt 2)
/// - ✓ Done (1 retry used)
///
/// **Scenario 3: Exhausted Retries**
/// - Task arrives in queue
/// - Handler fails (attempts 1, 2, 3)
/// - After attempt 3 failure, max retries exceeded
/// - Message sent to Dead Letter Queue (task_queue.dlq)
/// - ⚠️ Manual intervention needed
///
/// **Advantages of DelayedExchange over TTL:**
/// - More precise timing (RabbitMQ manages delays server-side)
/// - Cleaner architecture (single delay exchange vs multiple retry queues)
/// - Better for high-volume scenarios
/// - Built-in reliability with delayed exchange plugin
///
/// **Disadvantages:**
/// - Requires external plugin installation
/// - Plugin adds complexity to RabbitMQ setup
/// - Slightly higher memory footprint on RabbitMQ
#[allow(dead_code)]
fn example_scenarios() {
    // This function serves as documentation anchor for the example flow above
}
