use rust_rabbit::{
    RustRabbit, RabbitConfig, retry::DelayedMessageExchange,
    connection::ConnectionManager, retry::RetryPolicy,
};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
struct TaskMessage {
    task_id: String,
    task_type: String,
    payload: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::init();

    // Create configuration
    let config = RabbitConfig {
        connection_string: "amqp://localhost:5672".to_string(),
        virtual_host: Some("/".to_string()),
        ..Default::default()
    };

    // Create connection manager
    let connection_manager = ConnectionManager::new(config).await?;

    // Configure retry policy
    let retry_policy = RetryPolicy {
        max_retries: 5,
        initial_delay: Duration::from_millis(2000),
        max_delay: Duration::from_secs(120),
        backoff_multiplier: 2.0,
        jitter: 0.15,
        dead_letter_exchange: Some("failed-tasks".to_string()),
        dead_letter_queue: Some("failed-tasks-queue".to_string()),
        ..Default::default()
    };

    // Create delayed message exchange
    let delayed_exchange = DelayedMessageExchange::new(
        connection_manager,
        "task-retry-exchange".to_string(),
        retry_policy,
    );

    // Setup the delayed message exchange infrastructure
    delayed_exchange.setup().await?;
    info!("Delayed message exchange setup completed");

    // Setup retry queues for specific task queue
    delayed_exchange.setup_retry_queues("task-queue").await?;
    info!("Retry queues setup completed for task-queue");

    // Create sample task message
    let task = TaskMessage {
        task_id: "TASK-001".to_string(),
        task_type: "data-processing".to_string(),
        payload: serde_json::json!({
            "input_file": "data.csv",
            "output_format": "json",
            "filters": ["active", "verified"]
        }),
    };

    // Simulate retry scenarios
    for retry_attempt in 0..3 {
        info!("Publishing retry attempt {} for task: {}", retry_attempt, task.task_id);
        
        delayed_exchange
            .publish_with_retry(
                "task-queue",
                &task,
                retry_attempt,
                None, // No original headers
            )
            .await?;

        // Wait a bit before next retry
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    info!("Retry example completed");

    Ok(())
}