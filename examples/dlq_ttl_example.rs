//! Example: Dead Letter Queue with Auto-Cleanup TTL
//!
//! This example shows how to configure DLQ (Dead Letter Queue) with automatic cleanup.
//! Messages that exhaust all retries are sent to DLQ and automatically removed after TTL expires.
//!
//! **Benefits:**
//! - Automatic cleanup of failed messages (prevents DLQ bloat)
//! - Configurable retention period (can set per-consumer)
//! - No manual intervention needed for old failed messages
//! - Useful for high-volume systems

use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Task {
    id: u32,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("=== Dead Letter Queue with Auto-Cleanup Example ===");

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    info!("✓ Connected to RabbitMQ");

    // Configure retry with DLQ TTL
    let retry_config = RetryConfig::exponential_default().with_dlq_ttl(Duration::from_secs(86400)); // 1 day auto-cleanup

    info!("\n⚙️  Configuration:");
    info!("  - Max retries: {}", retry_config.max_retries);
    info!("  - DLQ TTL: 1 day (auto-cleanup)");
    info!("  - Dead Letter Queue: task_queue.dlq");
    info!("    Messages in DLQ expire after 1 day and are removed");

    // Create consumer with DLQ TTL
    let consumer = Consumer::builder(connection.clone(), "task_queue")
        .with_retry(retry_config)
        .build();

    info!("\n🔄 Starting consumer...\n");

    // Consume messages
    consumer
        .consume(|msg: rust_rabbit::Message<Task>| async move {
            info!(
                "Processing task {} (attempt {}): {}",
                msg.data.id,
                msg.retry_attempt + 1,
                msg.data.name
            );

            // Simulate some tasks succeeding and some failing
            if msg.data.id.is_multiple_of(3) {
                info!("  ✓ Task {} succeeded", msg.data.id);
                Ok(())
            } else {
                // This task fails and will be retried
                info!("  ❌ Task {} failed", msg.data.id);
                Err("Processing failed".into())
            }
        })
        .await?;

    Ok(())
}

/// ## Detailed Flow
///
/// **Message Lifecycle with DLQ TTL:**
///
/// ```text
/// Task arrives in queue
///     ↓
/// Handler processes message
///     ├─ SUCCESS? → ACK message (done) ✓
///     └─ FAIL? → Retry logic triggered
///         ├─ Retry attempt < max_retries?
///         │   └─ YES → Send to retry queue (exponential backoff: 1s→2s→4s→8s→16s)
///         │       ↓
///         │   After delay, message returns to original queue
///         │       ↓
///         │   Try processing again
///         │
///         └─ NO (retries exhausted)
///             └─ Send to DLQ (task_queue.dlq)
///                 ├─ Message stored in DLQ
///                 ├─ TTL: 86400 seconds (1 day)
///                 ├─ After 1 day: message auto-deleted by RabbitMQ
///                 └─ ⚠️ No manual cleanup needed!
/// ```
///
/// ## DLQ TTL Options
///
/// **1 Hour (Fresh Failed Messages):**
/// ```ignore
/// .with_dlq_ttl(Duration::from_secs(3600))
/// ```
///
/// **1 Day (Default for most systems):**
/// ```ignore
/// .with_dlq_ttl(Duration::from_secs(86400))
/// ```
///
/// **1 Week (Long retention for analysis):**
/// ```ignore
/// .with_dlq_ttl(Duration::from_secs(604800))
/// ```
///
/// **No TTL (Manual cleanup only):**
/// ```ignore
/// // Don't call .with_dlq_ttl() - DLQ messages persist indefinitely
/// ```
///
/// ## RabbitMQ Management Console
///
/// You can monitor DLQ and verify TTL:
///
/// 1. Go to http://localhost:15672
/// 2. Login (default: guest/guest)
/// 3. Go to "Queues"
/// 4. Find "task_queue.dlq" queue
/// 5. Check "x-message-ttl" in queue details
/// 6. See "Message count" to monitor failed messages
/// 7. Watch as messages auto-expire after TTL
#[allow(dead_code)]
fn dlq_monitoring_guide() {
    // This function serves as documentation for DLQ monitoring
}
