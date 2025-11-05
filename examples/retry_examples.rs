//! Retry Examples (Simplified)
//!
//! This example demonstrates different retry configurations available in rust-rabbit.
//! Shows exponential, linear, custom, and no-retry patterns.

use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Deserialize, Serialize, Debug, Clone)]
struct Task {
    id: u32,
    task_type: String,
    difficulty: u8, // 1-10, affects failure probability
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();
    info!("🚀 Starting retry examples");

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

    // Start all retry pattern consumers
    let _handles = [
        start_exponential_consumer(connection.clone()),
        start_linear_consumer(connection.clone()),
        start_custom_consumer(connection.clone()),
        start_no_retry_consumer(connection.clone()),
    ];

    info!("✅ All consumers started. Press Ctrl+C to stop.");

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal, stopping consumers...");

    Ok(())
}

// Exponential retry: 1s → 2s → 4s → 8s → 16s (5 retries)
fn start_exponential_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "exponential_tasks")
            .with_retry(RetryConfig::exponential_default())
            .with_prefetch(3)
            .build();

        consumer
            .consume(|msg: Task| async move {
                info!("📈 Exponential - Processing task {}", msg.id);

                simulate_work(&msg, "exponential").await
            })
            .await
            .map_err(|e| e.into())
    })
}

// Linear retry: 10s → 10s → 10s (3 retries)
fn start_linear_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "linear_tasks")
            .with_retry(RetryConfig::linear(3, Duration::from_secs(10)))
            .with_prefetch(2)
            .build();

        consumer
            .consume(|msg: Task| async move {
                info!("📊 Linear - Processing task {}", msg.id);

                simulate_work(&msg, "linear").await
            })
            .await
            .map_err(|e| e.into())
    })
}

// Custom retry: 1s → 5s → 30s → 120s
fn start_custom_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let custom_retry = RetryConfig::custom(vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_secs(120),
        ]);

        let consumer = Consumer::builder(connection, "custom_tasks")
            .with_retry(custom_retry)
            .with_prefetch(2)
            .build();

        consumer
            .consume(|msg: Task| async move {
                info!("🎯 Custom - Processing task {}", msg.id);

                simulate_work(&msg, "custom").await
            })
            .await
            .map_err(|e| e.into())
    })
}

// No retry: fail immediately
fn start_no_retry_consumer(
    connection: std::sync::Arc<Connection>,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let consumer = Consumer::builder(connection, "no_retry_tasks")
            .with_retry(RetryConfig::no_retry())
            .with_prefetch(5)
            .build();

        consumer
            .consume(|msg: Task| async move {
                info!("🚫 No Retry - Processing task {}", msg.id);

                simulate_work(&msg, "no-retry").await
            })
            .await
            .map_err(|e| e.into())
    })
}

// Simulate task processing with controlled failure rates
async fn simulate_work(
    task: &Task,
    retry_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simulate processing time
    let processing_time = Duration::from_millis(100 + (task.difficulty as u64 * 50));
    tokio::time::sleep(processing_time).await;

    // Simulate failures based on task difficulty and type
    let failure_rate = match task.task_type.as_str() {
        "easy" => 0.1,       // 10% failure rate
        "medium" => 0.3,     // 30% failure rate
        "hard" => 0.6,       // 60% failure rate
        "impossible" => 1.0, // Always fails
        _ => 0.2,            // 20% default failure rate
    };

    // Add some randomness
    let random_factor = (task.id % 100) as f64 / 100.0;

    if random_factor < failure_rate {
        let error_msg = match task.difficulty {
            1..=3 => "Network timeout",
            4..=6 => "Database connection failed",
            7..=8 => "Rate limit exceeded",
            _ => "Service unavailable",
        };

        warn!(
            "❌ {} task {} failed: {} (difficulty: {})",
            retry_type, task.id, error_msg, task.difficulty
        );

        return Err(error_msg.into());
    }

    info!(
        "✅ {} task {} completed successfully (difficulty: {})",
        retry_type, task.id, task.difficulty
    );

    Ok(())
}

// Helper to simulate publishing test tasks (run this separately)
#[allow(dead_code)]
async fn publish_test_tasks() -> Result<(), Box<dyn std::error::Error>> {
    use rust_rabbit::Publisher;

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    let publisher = Publisher::new(connection);

    let test_tasks = vec![
        (
            "exponential_tasks",
            Task {
                id: 1,
                task_type: "easy".to_string(),
                difficulty: 2,
            },
        ),
        (
            "linear_tasks",
            Task {
                id: 2,
                task_type: "medium".to_string(),
                difficulty: 5,
            },
        ),
        (
            "custom_tasks",
            Task {
                id: 3,
                task_type: "hard".to_string(),
                difficulty: 8,
            },
        ),
        (
            "no_retry_tasks",
            Task {
                id: 4,
                task_type: "impossible".to_string(),
                difficulty: 10,
            },
        ),
    ];

    for (queue, task) in test_tasks {
        publisher.publish_to_queue(queue, &task, None).await?;
        info!("📤 Published task {} to {}", task.id, queue);
    }

    Ok(())
}
